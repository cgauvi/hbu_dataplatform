"""What a lot yields, and which lots the roll says are like it.

Two halves, matching the two modules. `urban_rag.comparables` is arithmetic
over frames and is tested directly - the CUBF classification, the per-lot
aggregation, the income, the metric and the value bases. `lot_assessment_
comparables` is tested by materializing it over a hand-built borough written
to a `ParquetStore`, the same way `test_role.py` exercises the assets it sits
behind: the upstream parquet is written by hand rather than produced by
running the roll, because what is under test here is this asset's own reading
of those four files and not the chain that wrote them.

The borough is five lots, chosen so that each is the only one exercising the
case it stands for - see `borough_lots`.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import Point, box

from asset_helpers import materialization_metadata, stub_publish
from urban_rag import comparables, comparables_assets
from urban_rag.cubf import USE_DESCRIPTION_COLUMN
from urban_rag.cmhc_assets import (
    AVERAGE_RENTS_FILE,
    VACANCY_FILE,
    average_rents,
    vacancy_rates,
)
from urban_rag.comparables import (
    ComparableWeights,
    IncomeAssumptions,
    aggregate_units_by_lot,
    annual_income,
    cap_rate_pct,
    estimate_value,
    income_class_of,
    nearest_comparables,
    summarise_comparables,
    use_class_of,
)
from urban_rag.comparables_assets import (
    LOT_COMPARABLES_FILE,
    lot_assessment_comparables,
)
from urban_rag.rent_assets import COMMERCIAL_RENTS_FILE, commercial_rents
from urban_rag.frames import write_frame
from urban_rag.program import (
    COMMERCIAL_REVENUE_PER_SQFT_CAD,
    COMMERCIAL_VACANCY_PCT,
    M2_PER_SQFT,
    MAX_MAINTENANCE_PREMIUM,
    MONTHS_PER_YEAR,
)
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.role_assets import (
    ASSESSMENT_UNITS_FILE,
    CADASTRE_FILE,
    LOT_VALUES_FILE,
    assessment_units,
    lot_assessed_values,
    property_assessment_roll,
)
from urban_rag.role_foncier import (
    DWELLINGS_COLUMN,
    FLOOR_AREA_COLUMN,
    JOIN_KEY,
    LAND_AREA_COLUMN,
    NONRESIDENTIAL_UNITS_COLUMN,
    RENTAL_ROOMS_COLUMN,
    ROLL_LOT_COLUMN,
    ROLL_LOT_SUFFIX_COLUMN,
    STOREYS_COLUMN,
    USE_CODE_COLUMN,
    VALUE_COLUMN,
    YEAR_BUILT_COLUMN,
)
from urban_rag.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
ROLL_YEAR = 2026


# -- the classification -----------------------------------------------------


@pytest.mark.parametrize(
    "code, expected_class, expected_income",
    [
        ("1000", "habitation", "residential"),
        ("1932", "habitation", "residential"),
        ("2311", "industrie_manufacturiere", "industrial"),
        # Manufacturing spans 2 and 3, so both digits are one category.
        ("3190", "industrie_manufacturiere", "industrial"),
        # 4611 is a parking garage, which the manual files under
        # transport and utilities rather than under commerce.
        ("4611", "transport_communication_services_publics", "industrial"),
        # 5811 is a restaurant - the 5000s are where commerce actually is.
        ("5811", "commerciale", "commercial"),
        ("6512", "services", "commercial"),
        ("7412", "culturelle_recreative_loisirs", "commercial"),
        ("8110", "production_extraction_ressources", "industrial"),
        ("9100", "immeubles_non_exploites_etendues_eau", "none"),
    ],
)
def test_the_leading_digit_is_the_class(code, expected_class, expected_income):
    assert use_class_of(code) == expected_class
    assert income_class_of(code) == expected_income


@pytest.mark.parametrize("code", [None, "", "abcd", "100", "10000", float("nan")])
def test_a_code_that_is_not_four_digits_is_not_placed(code):
    """Guessed rather than unknown is how a lot ends up in the wrong class."""
    assert use_class_of(code) == comparables.UNKNOWN_USE_CLASS
    assert income_class_of(code) == comparables.UNKNOWN_INCOME_CLASS


def test_a_use_code_is_never_read_as_a_magnitude():
    """1000 and 4000 are residential and commercial, not three thousand apart."""
    assert use_class_of("1000") != use_class_of("4000")
    assert use_class_of("1999") == use_class_of("1000")


@pytest.mark.parametrize("code", ["4500", "4510", "4550", "4562", "4599"])
def test_the_road_group_is_the_whole_of_45(code):
    assert comparables.is_road_use_code(code)


@pytest.mark.parametrize(
    "code", ["4000", "4499", "4600", "1000", "5000", None, "", "abcd", "455",
             float("nan")]
)
def test_nothing_outside_45_is_a_road(code):
    assert not comparables.is_road_use_code(code)


def test_a_road_code_survives_a_parquet_round_trip_as_a_float():
    """`4550.0` is what a nullable column hands back, and it is still a road."""
    assert comparables.is_road_use_code(4550.0)
    assert comparables.is_road_use_code("4550.0")


def test_a_road_is_still_filed_under_the_category_the_manual_gives_it():
    """The two readings sit side by side rather than one replacing the other.

    Rubric 45 is VOIE PUBLIQUE and the manual puts it inside category 4,
    TRANSPORTS, COMMUNICATIONS ET SERVICES PUBLICS - which is where a public
    road belongs. `use_class_of` says so, and `is_road_use_code` is the finer
    reading kept beside it.
    """
    assert use_class_of("4550") == "transport_communication_services_publics"
    assert comparables.is_road_use_code("4550")


# -- the aggregation --------------------------------------------------------


def units_frame(rows: list[dict]) -> pd.DataFrame:
    """`assessment_units` as this asset reads it, defaults filled in."""
    defaults = {
        USE_CODE_COLUMN: "1000",
        LAND_AREA_COLUMN: 300.0,
        STOREYS_COLUMN: 2,
        YEAR_BUILT_COLUMN: "1950",
        FLOOR_AREA_COLUMN: 200.0,
        DWELLINGS_COLUMN: 2,
        NONRESIDENTIAL_UNITS_COLUMN: 0,
        RENTAL_ROOMS_COLUMN: 0,
        VALUE_COLUMN: 500_000.0,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_a_lots_characteristics_are_summed_over_the_units_on_it():
    units = units_frame(
        [
            {JOIN_KEY: "A", DWELLINGS_COLUMN: 2, FLOOR_AREA_COLUMN: 200.0},
            {JOIN_KEY: "B", DWELLINGS_COLUMN: 3, FLOOR_AREA_COLUMN: 300.0},
        ]
    )
    pairs = pd.DataFrame({JOIN_KEY: ["A", "B"], "NO_LOT": ["L1", "L1"]})

    aggregated = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    )

    assert aggregated.loc["L1", "num_assessment_units"] == 2
    assert aggregated.loc["L1", "num_dwellings"] == 5
    assert aggregated.loc["L1", "floor_area_m2"] == 500.0


def test_the_floor_is_split_by_each_units_own_use_code():
    """A triplex over a depanneur earns both, because its two units say so."""
    units = units_frame(
        [
            {JOIN_KEY: "A", USE_CODE_COLUMN: "1000", FLOOR_AREA_COLUMN: 200.0},
            {JOIN_KEY: "B", USE_CODE_COLUMN: "5413", FLOOR_AREA_COLUMN: 80.0},
            {JOIN_KEY: "C", USE_CODE_COLUMN: "2311", FLOOR_AREA_COLUMN: 900.0},
        ]
    )
    pairs = pd.DataFrame({JOIN_KEY: ["A", "B", "C"], "NO_LOT": ["L1", "L1", "L1"]})

    row = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    ).loc["L1"]

    assert row["residential_floor_area_m2"] == 200.0
    assert row["commercial_floor_area_m2"] == 80.0
    assert row["industrial_floor_area_m2"] == 900.0
    assert row["floor_area_m2"] == 1180.0


def test_commerce_splits_into_retail_and_office_and_adds_back_up():
    """CUBF 5000 is retail and 6000/7000 are services and culture. The two are
    priced at different surveyed rents and reported as one column."""
    units = units_frame(
        [
            {JOIN_KEY: "A", USE_CODE_COLUMN: "5811", FLOOR_AREA_COLUMN: 95.0},
            {JOIN_KEY: "B", USE_CODE_COLUMN: "6512", FLOOR_AREA_COLUMN: 120.0},
            {JOIN_KEY: "C", USE_CODE_COLUMN: "7412", FLOOR_AREA_COLUMN: 40.0},
        ]
    )
    pairs = pd.DataFrame({JOIN_KEY: ["A", "B", "C"], "NO_LOT": ["L1"] * 3})

    row = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    ).loc["L1"]

    assert row["retail_floor_area_m2"] == 95.0
    assert row["office_floor_area_m2"] == 160.0
    assert row["commercial_floor_area_m2"] == pytest.approx(
        row["retail_floor_area_m2"] + row["office_floor_area_m2"]
    )


@pytest.mark.parametrize(
    "code, rent_class",
    [
        ("1000", "residential"),
        # The 5000s are the shops, and they are charged a retail rent.
        ("5811", "retail"),
        ("6512", "office"),
        ("7412", "office"),
        ("2311", "industrial"),
        # A parking garage is transport infrastructure, priced as
        # industrial floor rather than as a shop.
        ("4611", "industrial"),
        ("9100", "none"),
        (None, "none"),
    ],
)
def test_the_rent_class_splits_commerce_where_the_income_class_does_not(
    code, rent_class
):
    assert comparables.rent_class_of(code) == rent_class


def test_floor_the_cubf_cannot_place_lands_in_no_class():
    """And so is priced at nothing, rather than at the average of a guess."""
    units = units_frame(
        [{JOIN_KEY: "A", USE_CODE_COLUMN: None, FLOOR_AREA_COLUMN: 500.0}]
    )
    pairs = pd.DataFrame({JOIN_KEY: ["A"], "NO_LOT": ["L1"]})

    row = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    ).loc["L1"]

    assert row["floor_area_m2"] == 500.0
    assert pd.isna(row["residential_floor_area_m2"])
    assert pd.isna(row["commercial_floor_area_m2"])
    assert pd.isna(row["industrial_floor_area_m2"])


def test_a_unit_on_several_lots_is_counted_whole_on_each():
    """The same basis `total_assessed_value` uses, so the ratio between them
    is a yield rather than a yield times a fraction."""
    units = units_frame([{JOIN_KEY: "A", DWELLINGS_COLUMN: 3}])
    pairs = pd.DataFrame({JOIN_KEY: ["A", "A"], "NO_LOT": ["L1", "L2"]})

    aggregated = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    )

    assert aggregated.loc["L1", "num_dwellings"] == 3
    assert aggregated.loc["L2", "num_dwellings"] == 3


def test_the_dominant_unit_is_the_one_carrying_most_of_the_value():
    units = units_frame(
        [
            {
                JOIN_KEY: "A",
                USE_CODE_COLUMN: "1000",
                VALUE_COLUMN: 200_000.0,
                YEAR_BUILT_COLUMN: "1920",
            },
            {
                JOIN_KEY: "B",
                USE_CODE_COLUMN: "4611",
                VALUE_COLUMN: 900_000.0,
                YEAR_BUILT_COLUMN: "1985",
                STOREYS_COLUMN: 4,
            },
        ]
    )
    pairs = pd.DataFrame({JOIN_KEY: ["A", "B"], "NO_LOT": ["L1", "L1"]})

    row = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    ).loc["L1"]

    assert row["dominant_use_code"] == "4611"
    assert row["dominant_use_class"] == (
        "transport_communication_services_publics"
    )
    assert row["dominant_income_class"] == "industrial"
    assert row["year_built"] == 1985
    assert row["num_storeys"] == 4


def test_the_dominant_units_description_is_read_off_that_same_unit():
    """What the lot *is*, in words, from the row the code and year came from.

    Carried across from `silver.assessment_units` rather than looked up here,
    so this module needs no second copy of the manual and the words beside a
    code are the ones that partition was described with.
    """
    units = units_frame(
        [
            {
                JOIN_KEY: "A",
                USE_CODE_COLUMN: "1000",
                USE_DESCRIPTION_COLUMN: "Logement",
                VALUE_COLUMN: 200_000.0,
            },
            {
                JOIN_KEY: "B",
                USE_CODE_COLUMN: "4611",
                USE_DESCRIPTION_COLUMN: "Garage de stationnement pour automobiles",
                VALUE_COLUMN: 900_000.0,
            },
        ]
    )
    pairs = pd.DataFrame({JOIN_KEY: ["A", "B"], "NO_LOT": ["L1", "L1"]})

    row = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    ).loc["L1"]

    assert row["dominant_use_code"] == "4611"
    assert row["dominant_use_description"] == (
        "Garage de stationnement pour automobiles"
    )


def test_a_partition_with_no_description_column_still_aggregates():
    """A silver partition written before the codebook existed carries none.

    It must cost the column and nothing else - the floor split, the dominant
    code and every sum are computed from the roll's own fields.
    """
    units = units_frame(
        [{JOIN_KEY: "A", USE_CODE_COLUMN: "4611", VALUE_COLUMN: 900_000.0}]
    )
    pairs = pd.DataFrame({JOIN_KEY: ["A"], "NO_LOT": ["L1"]})

    aggregated = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    )

    assert aggregated.loc["L1", "dominant_use_code"] == "4611"
    assert "dominant_use_description" not in aggregated.columns


def test_a_lot_whose_units_state_no_floor_reports_null_and_not_zero():
    """`missing_penalty` and "identical to every empty warehouse" are what the
    two would mean to the neighbour search."""
    units = units_frame([{JOIN_KEY: "A", FLOOR_AREA_COLUMN: None}])
    pairs = pd.DataFrame({JOIN_KEY: ["A"], "NO_LOT": ["L1"]})

    row = aggregate_units_by_lot(
        pairs, units, lot_column="NO_LOT", join_key=JOIN_KEY
    ).loc["L1"]

    assert pd.isna(row["floor_area_m2"])


def test_a_partition_that_placed_no_unit_still_has_the_shape():
    empty = aggregate_units_by_lot(
        pd.DataFrame({JOIN_KEY: [], "NO_LOT": []}),
        units_frame([{JOIN_KEY: "A"}]),
        lot_column="NO_LOT",
        join_key=JOIN_KEY,
    )

    assert empty.empty
    assert "num_dwellings" in empty.columns
    assert "dominant_use_code" in empty.columns


# -- the income -------------------------------------------------------------


def test_residential_income_is_rent_a_month_less_the_surveyed_vacancy():
    frame = pd.DataFrame({"num_dwellings": [4.0]})
    assumptions = IncomeAssumptions(average_rent_cad=1000.0, vacancy_rate_pct=2.0)

    income = annual_income(frame, assumptions)

    assert income["residential_income_cad"].iloc[0] == pytest.approx(
        4 * 1000.0 * MONTHS_PER_YEAR * 0.98
    )


def test_a_vacancy_rate_is_read_as_the_percentage_it_is_published_as():
    """0.2 is two tenths of a percent, not twenty percent - the error that
    produces a confident answer rather than an exception."""
    frame = pd.DataFrame({"num_dwellings": [1.0]})

    income = annual_income(
        frame, IncomeAssumptions(average_rent_cad=1000.0, vacancy_rate_pct=0.2)
    )

    assert income["residential_income_cad"].iloc[0] == pytest.approx(
        1000.0 * 12 * 0.998
    )


def test_non_residential_income_is_annual_and_per_square_foot():
    frame = pd.DataFrame({"num_dwellings": [0.0], "retail_floor_area_m2": [1000.0]})

    income = annual_income(frame, IncomeAssumptions(average_rent_cad=900.0))

    assert income["retail_income_cad"].iloc[0] == pytest.approx(
        1000.0 / M2_PER_SQFT
        * COMMERCIAL_REVENUE_PER_SQFT_CAD
        * (1 - COMMERCIAL_VACANCY_PCT / 100)
    )


def test_retail_and_office_are_priced_apart_and_reported_together():
    """The whole point of the rent-class split: C&W survey an office rent and
    nobody surveys a retail one, and the two are dollars apart."""
    frame = pd.DataFrame(
        {
            "num_dwellings": [0.0],
            "retail_floor_area_m2": [100.0],
            "office_floor_area_m2": [100.0],
        }
    )
    assumptions = IncomeAssumptions(
        retail_rent_per_sqft_cad=26.0, office_rent_per_sqft_cad=22.39
    )

    income = annual_income(frame, assumptions)

    assert income["retail_income_cad"].iloc[0] > income["office_income_cad"].iloc[0]
    # `commercial_income_cad` keeps the meaning every reader downstream has:
    # the two halves added back together.
    assert income["commercial_income_cad"].iloc[0] == pytest.approx(
        income["retail_income_cad"].iloc[0] + income["office_income_cad"].iloc[0]
    )


def test_a_lot_with_only_retail_floor_still_earns_a_commercial_income():
    """`min_count=1` on the commerce sum: no office floor is not a null lot."""
    frame = pd.DataFrame({"num_dwellings": [0.0], "retail_floor_area_m2": [100.0]})

    income = annual_income(frame, IncomeAssumptions(retail_rent_per_sqft_cad=26.0))

    assert pd.isna(income["office_income_cad"].iloc[0])
    assert income["commercial_income_cad"].iloc[0] == pytest.approx(
        income["retail_income_cad"].iloc[0]
    )


def test_the_classes_are_added_rather_than_chosen_between():
    frame = pd.DataFrame(
        {
            "num_dwellings": [3.0],
            "retail_floor_area_m2": [100.0],
            "industrial_floor_area_m2": [50.0],
        }
    )

    income = annual_income(frame, IncomeAssumptions(average_rent_cad=1000.0))

    assert income["gross_income_cad"].iloc[0] == pytest.approx(
        income["residential_income_cad"].iloc[0]
        + income["commercial_income_cad"].iloc[0]
        + income["industrial_income_cad"].iloc[0]
    )


def test_a_borough_cmhc_suppressed_gets_a_null_residential_income():
    """Null, not zero: the dwellings are there, the rent is not published."""
    frame = pd.DataFrame({"num_dwellings": [4.0]})

    income = annual_income(frame, IncomeAssumptions(average_rent_cad=None))

    assert pd.isna(income["residential_income_cad"].iloc[0])
    assert pd.isna(income["gross_income_cad"].iloc[0])
    assert pd.isna(income["net_operating_income_cad"].iloc[0])


def test_a_lot_nobody_could_price_is_null_and_one_priced_at_zero_is_zero():
    frame = pd.DataFrame(
        {
            "num_dwellings": [0.0, np.nan],
            "commercial_floor_area_m2": [np.nan, np.nan],
        }
    )

    income = annual_income(frame, IncomeAssumptions(average_rent_cad=1000.0))

    assert income["gross_income_cad"].iloc[0] == 0.0
    assert pd.isna(income["gross_income_cad"].iloc[1])


def test_the_expense_ratio_is_applied_once_and_after_vacancy():
    frame = pd.DataFrame({"num_dwellings": [1.0]})
    assumptions = IncomeAssumptions(
        average_rent_cad=1000.0, vacancy_rate_pct=10.0, operating_expense_ratio=0.4
    )

    income = annual_income(frame, assumptions)

    assert income["net_operating_income_cad"].iloc[0] == pytest.approx(
        1000.0 * 12 * 0.9 * 0.6
    )


def test_a_cap_rate_is_a_percentage_and_a_value_of_zero_is_not_infinite():
    rates = cap_rate_pct(
        pd.Series([50_000.0, 50_000.0, np.nan]),
        pd.Series([1_000_000.0, 0.0, 1_000_000.0]),
    )

    assert rates.iloc[0] == pytest.approx(5.0)
    assert pd.isna(rates.iloc[1])
    assert pd.isna(rates.iloc[2])


def test_the_market_value_factor_scales_the_denominator():
    on_the_roll = cap_rate_pct(pd.Series([50_000.0]), pd.Series([1_000_000.0]))
    on_the_market = cap_rate_pct(
        pd.Series([50_000.0]), pd.Series([1_000_000.0]), market_value_factor=1.25
    )

    assert on_the_market.iloc[0] == pytest.approx(on_the_roll.iloc[0] / 1.25)


@pytest.mark.parametrize("ratio", [-0.1, 1.0, 1.5])
def test_an_expense_ratio_outside_the_unit_interval_is_refused(ratio):
    with pytest.raises(ValueError, match="operating_expense_ratio"):
        IncomeAssumptions(operating_expense_ratio=ratio)


# -- the neighbour metric ---------------------------------------------------


def comparable_lots(rows: list[dict]) -> pd.DataFrame:
    """The narrow frame `nearest_comparables` reads, defaults filled in."""
    defaults = {
        "x_m": 0.0,
        "y_m": 0.0,
        "lot_area_m2": 300.0,
        "floor_area_m2": 200.0,
        "num_dwellings": 2.0,
        "use_code": "1000",
        "total_assessed_value": 500_000.0,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_a_lot_is_never_its_own_comparable():
    lots = comparable_lots(
        [{"lot_number": "L1"}, {"lot_number": "L2", "x_m": 50.0}]
    )

    neighbours = nearest_comparables(lots, k=5)

    assert [entry["lot_number"] for entry in neighbours[0]] == ["L2"]
    assert [entry["lot_number"] for entry in neighbours[1]] == ["L1"]


def test_the_same_use_code_beats_a_nearer_lot_of_another_class():
    """The claim `use_weight` makes: a comparable of the wrong kind is not a
    comparable at any distance."""
    lots = comparable_lots(
        [
            {"lot_number": "SUBJECT", "use_code": "1000"},
            {"lot_number": "NEAR_SHOP", "x_m": 20.0, "use_code": "5413"},
            {"lot_number": "FAR_TRIPLEX", "x_m": 300.0, "use_code": "1000"},
        ]
    )

    neighbours = nearest_comparables(lots, k=2)

    assert neighbours[0][0]["lot_number"] == "FAR_TRIPLEX"


def test_a_lot_beyond_the_radius_is_not_a_comparable_however_alike():
    lots = comparable_lots(
        [{"lot_number": "L1"}, {"lot_number": "L2", "x_m": 5_000.0}]
    )

    assert nearest_comparables(lots, k=5, max_distance_m=2_000.0)[0] == []
    assert len(nearest_comparables(lots, k=5, max_distance_m=6_000.0)[0]) == 1


def test_size_is_a_ratio_rather_than_a_difference():
    """A factor of two is one unit apart whatever the absolute figures, which
    is what stops every small lot being equidistant from every other.

    Both come out just under 1.0 rather than exactly on it, and the gap is
    `log1p`: `log(201/101)` is a shade under `log(2)` where `log(4001/2001)` is
    nearer it. That +1 is what lets a lot with no floor sit at the origin of
    the axis instead of at a negative infinity on it, and the error it costs
    shrinks with size - 0.7 percent at 100 m2, 0.04 at 2,000 - which is the
    opposite end of the scale from where a size comparison matters.
    """
    weights = ComparableWeights(
        distance_weight=0.0, lot_area_weight=0.0, dwellings_weight=0.0, use_weight=0.0
    )
    lots = comparable_lots(
        [
            {"lot_number": "SMALL", "floor_area_m2": 100.0},
            {"lot_number": "DOUBLE_SMALL", "floor_area_m2": 200.0},
            {"lot_number": "BIG", "floor_area_m2": 2_000.0},
            {"lot_number": "DOUBLE_BIG", "floor_area_m2": 4_000.0},
        ]
    )

    neighbours = nearest_comparables(lots, k=3, weights=weights)
    by_name = {row[0]["lot_number"]: row[0]["distance"] for row in neighbours}

    assert by_name["DOUBLE_SMALL"] == pytest.approx(1.0, rel=1e-2)
    assert by_name["DOUBLE_BIG"] == pytest.approx(1.0, rel=1e-2)
    # A linear scale would have put these two 100 and 2,000 units apart.
    assert by_name["DOUBLE_SMALL"] == pytest.approx(by_name["DOUBLE_BIG"], rel=1e-2)


def test_a_feature_neither_side_can_state_costs_the_missing_penalty():
    """So an unassessed lane is not the nearest neighbour of every other one."""
    weights = ComparableWeights(
        distance_weight=0.0, lot_area_weight=0.0, dwellings_weight=0.0, use_weight=0.0
    )
    lots = comparable_lots(
        [
            {"lot_number": "BLANK", "floor_area_m2": None},
            {"lot_number": "KNOWN", "floor_area_m2": 200.0},
        ]
    )

    neighbours = nearest_comparables(lots, k=1, weights=weights)

    assert neighbours[0][0]["distance"] == pytest.approx(
        math.sqrt(weights.floor_area_weight) * weights.missing_penalty
    )


def test_only_a_valued_lot_can_be_a_comparable():
    """Which is what lets a vacant parcel be valued off the built ones."""
    lots = comparable_lots(
        [
            {"lot_number": "LANE", "total_assessed_value": None},
            {"lot_number": "TRIPLEX", "x_m": 40.0},
        ]
    )

    neighbours = nearest_comparables(lots, k=5)

    assert [entry["lot_number"] for entry in neighbours[0]] == ["TRIPLEX"]
    # The lane is a subject and never a candidate.
    assert neighbours[1] == []


def test_a_partition_with_no_valued_lot_gives_every_row_an_empty_list():
    lots = comparable_lots(
        [
            {"lot_number": "L1", "total_assessed_value": None},
            {"lot_number": "L2", "total_assessed_value": None},
        ]
    )

    assert nearest_comparables(lots, k=5) == [[], []]


def test_neighbours_come_back_nearest_first():
    lots = comparable_lots(
        [
            {"lot_number": "SUBJECT"},
            {"lot_number": "NEAR", "x_m": 10.0},
            {"lot_number": "MID", "x_m": 100.0},
            {"lot_number": "FAR", "x_m": 400.0},
        ]
    )

    names = [entry["lot_number"] for entry in nearest_comparables(lots, k=3)[0]]

    assert names == ["NEAR", "MID", "FAR"]


def test_the_search_crosses_a_chunk_boundary_intact():
    """`_CHUNK_ROWS` is an implementation detail and must not be a seam: the
    511th and 513th lots have to get the same answer as the first."""
    count = comparables._CHUNK_ROWS * 2 + 7
    lots = comparable_lots(
        [{"lot_number": f"L{i}", "x_m": float(i)} for i in range(count)]
    )

    neighbours = nearest_comparables(lots, k=2, max_distance_m=10_000.0)

    assert len(neighbours) == count
    assert all(len(entries) == 2 for entries in neighbours)
    # Every interior lot's two nearest are the ones either side of it.
    middle = comparables._CHUNK_ROWS
    assert {entry["lot_number"] for entry in neighbours[middle]} == {
        f"L{middle - 1}",
        f"L{middle + 1}",
    }


@pytest.mark.parametrize(
    "kwargs", [{"size_ratio_scale": 1.0}, {"distance_scale_m": 0.0}]
)
def test_a_scale_that_cannot_be_a_scale_is_refused(kwargs):
    with pytest.raises(ValueError):
        ComparableWeights(**kwargs)


def test_weights_that_are_all_zero_are_refused():
    """Every distance would be 0 and the neighbour list would be whatever the
    sort left first - computed-looking, and not computed."""
    with pytest.raises(ValueError, match="at least one weight"):
        ComparableWeights(
            distance_weight=0.0,
            lot_area_weight=0.0,
            floor_area_weight=0.0,
            dwellings_weight=0.0,
            use_weight=0.0,
        )


# -- the value estimate -----------------------------------------------------


def test_the_median_ratio_is_taken_over_the_neighbours_that_have_it():
    """A commercial comparable with no dwellings sits out the per-dwelling
    median and counts in the other two."""
    neighbours = [
        [
            {"distance": 0.1, "distance_m": 10.0, "value_per_dwelling_cad": 300_000.0,
             "value_per_floor_m2_cad": 3_000.0, "value_per_land_m2_cad": 2_000.0},
            {"distance": 0.2, "distance_m": 20.0, "value_per_dwelling_cad": None,
             "value_per_floor_m2_cad": 5_000.0, "value_per_land_m2_cad": 4_000.0},
        ]
    ]

    summary = summarise_comparables(neighbours, index=pd.Index([0]))

    assert summary["num_comparables"].iloc[0] == 2
    assert summary["comparable_value_per_dwelling_cad"].iloc[0] == 300_000.0
    assert summary["comparable_value_per_floor_m2_cad"].iloc[0] == 4_000.0


def test_a_median_ignores_the_condominium_tower_a_mean_would_not():
    neighbours = [
        [
            {"distance": 0.1, "distance_m": 1.0, "value_per_dwelling_cad": v}
            for v in (300_000.0, 310_000.0, 320_000.0, 50_000_000.0)
        ]
    ]

    summary = summarise_comparables(neighbours, index=pd.Index([0]))

    assert summary["comparable_value_per_dwelling_cad"].iloc[0] == 315_000.0


def test_the_value_basis_falls_through_to_what_the_lot_actually_has():
    frame = pd.DataFrame(
        {
            "num_dwellings": [3.0, 0.0, 0.0],
            "floor_area_m2": [200.0, 500.0, np.nan],
            "lot_area_m2": [300.0, 400.0, 250.0],
            "comparable_value_per_dwelling_cad": [200_000.0, 200_000.0, 200_000.0],
            "comparable_value_per_floor_m2_cad": [3_000.0, 3_000.0, 3_000.0],
            "comparable_value_per_land_m2_cad": [2_000.0, 2_000.0, 2_000.0],
        }
    )

    estimated = estimate_value(frame)

    assert estimated["estimated_value_basis"].tolist() == [
        "per_dwelling",
        "per_floor_area",
        "per_land_area",
    ]
    assert estimated["estimated_value_cad"].tolist() == [
        600_000.0,
        1_500_000.0,
        500_000.0,
    ]


def test_a_lot_with_no_comparable_at_all_is_estimated_at_nothing():
    frame = pd.DataFrame(
        {
            "num_dwellings": [2.0],
            "floor_area_m2": [200.0],
            "lot_area_m2": [300.0],
            "comparable_value_per_dwelling_cad": [np.nan],
            "comparable_value_per_floor_m2_cad": [np.nan],
            "comparable_value_per_land_m2_cad": [np.nan],
        }
    )

    estimated = estimate_value(frame)

    assert pd.isna(estimated["estimated_value_cad"].iloc[0])
    assert estimated["estimated_value_basis"].iloc[0] == "none"


# -- the asset --------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture(autouse=True)
def published(monkeypatch):
    """The table needs a database; the upsert is recorded instead."""
    return stub_publish(monkeypatch, comparables_assets)


#: The five lots the asset fixtures are built from, each the only one
#: exercising its case:
#:
#: * `LOT_TRIPLEX_A` / `LOT_TRIPLEX_B` - two ordinary residential lots a few
#:   dozen metres apart. Each is the other's obvious comparable.
#: * `LOT_SHOP` - a commercial lot between them, which the use weight has to
#:   keep out of their neighbour lists' first place.
#: * `LOT_MIXED` - a triplex over a depanneur: two units, two classes, and the
#:   only lot whose income has more than one term.
#: * `LOT_LANE` - nothing assessed on it. A subject and never a candidate,
#:   valued on ground area alone.
LOT_TRIPLEX_A, LOT_TRIPLEX_B = "1 000 001", "1 000 002"
LOT_SHOP, LOT_MIXED, LOT_LANE = "1 000 003", "1 000 004", "1 000 005"

#: Roughly 40 m apart at this latitude, so every lot is inside the default
#: 2 km radius and the ordering below is about the metric rather than the map.
_LOTS = {
    LOT_TRIPLEX_A: (-73.6000, 45.5000),
    LOT_TRIPLEX_B: (-73.5995, 45.5000),
    LOT_SHOP: (-73.5990, 45.5000),
    LOT_MIXED: (-73.5985, 45.5000),
    LOT_LANE: (-73.5980, 45.5000),
}

#: (unit, lot, use code, floor m2, dwellings, non-residential units, value).
#: 5413 is *Dépanneur (sans vente d'essence)* - a real 5000-series retail
#: code, so the shop lots are charged the surveyed retail rent rather than
#: an office one.
_UNITS = [
    ("U_A", LOT_TRIPLEX_A, "1000", 240.0, 3, 0, 900_000.0),
    ("U_B", LOT_TRIPLEX_B, "1000", 260.0, 3, 0, 1_000_000.0),
    ("U_SHOP", LOT_SHOP, "5413", 300.0, 0, 1, 1_200_000.0),
    ("U_MIX_H", LOT_MIXED, "1000", 200.0, 3, 0, 800_000.0),
    ("U_MIX_C", LOT_MIXED, "5413", 90.0, 0, 1, 400_000.0),
]


def _cell(lot: str) -> box:
    """A small square around the lot's point, so its area is a real number."""
    x, y = _LOTS[lot]
    return box(x - 0.0001, y - 0.0001, x + 0.0001, y + 0.0001)


def write_lot_values(store, *, lots=None) -> None:
    """`lot_assessed_values` as this asset reads it: geometry and two totals."""
    lots = lots or list(_LOTS)
    totals = {}
    for _, lot, _, _, _, _, value in _UNITS:
        totals[lot] = totals.get(lot, 0.0) + value
    frame = gpd.GeoDataFrame(
        {
            "NO_LOT": lots,
            "num_assessment_units": [
                sum(1 for u in _UNITS if u[1] == lot) for lot in lots
            ],
            "num_shared_units": [0] * len(lots),
            "num_units_by_point": [0] * len(lots),
            "total_assessed_value": [totals.get(lot) for lot in lots],
            "total_assessed_value_apportioned": [totals.get(lot) for lot in lots],
            "roll_year": [ROLL_YEAR] * len(lots),
        },
        geometry=[_cell(lot) for lot in lots],
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(
                lot_assessed_values.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_VALUES_FILE,
        ),
    )


def write_units(store) -> None:
    """`assessment_units` as this asset reads it: point plus characteristics."""
    frame = gpd.GeoDataFrame(
        {
            JOIN_KEY: [unit for unit, *_ in _UNITS],
            USE_CODE_COLUMN: [use for _, _, use, *_ in _UNITS],
            FLOOR_AREA_COLUMN: [floor for *_, floor, _, _, _ in _UNITS],
            DWELLINGS_COLUMN: [d for *_, d, _, _ in _UNITS],
            NONRESIDENTIAL_UNITS_COLUMN: [n for *_, n, _ in _UNITS],
            RENTAL_ROOMS_COLUMN: [0] * len(_UNITS),
            LAND_AREA_COLUMN: [300.0] * len(_UNITS),
            STOREYS_COLUMN: [2] * len(_UNITS),
            YEAR_BUILT_COLUMN: ["1950"] * len(_UNITS),
            VALUE_COLUMN: [value for *_, value in _UNITS],
            "roll_year": [ROLL_YEAR] * len(_UNITS),
        },
        geometry=[Point(*_LOTS[lot]) for _, lot, *_ in _UNITS],
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(assessment_units.key.path[-1], DATE),
            ASSESSMENT_UNITS_FILE,
        ),
    )


def write_crosswalk(store) -> None:
    """`b05v_lot_cadst`, spelling lot numbers the way the roll does."""
    frame = pd.DataFrame(
        {
            JOIN_KEY: [unit for unit, *_ in _UNITS],
            ROLL_LOT_COLUMN: [lot.replace(" ", "") for _, lot, *_ in _UNITS],
            ROLL_LOT_SUFFIX_COLUMN: [None] * len(_UNITS),
        }
    )
    write_frame(
        frame,
        join(
            store.partition_dir(property_assessment_roll.key.path[-1], DATE),
            CADASTRE_FILE,
        ),
    )


def write_cmhc(store, *, rent: float | None = 1_200.0, vacancy: float | None = 2.0):
    """The borough's two surveyed figures, as the CMHC silver assets write them."""
    write_frame(
        pd.DataFrame(
            {
                "bedroom_type": ["all", "2_bedroom"],
                "average_rent_cad": [rent, 1_400.0],
                "num_quartiers": [3, 2],
                "survey_year": [2070, 2070],
                "survey_period": ["October 2070"] * 2,
            }
        ),
        join(
            store.partition_dir(average_rents.key.path[-1], DATE, NEIGHBORHOOD),
            AVERAGE_RENTS_FILE,
        ),
    )
    write_frame(
        pd.DataFrame(
            {
                "dwelling_type": ["all", "row"],
                "bedroom_type": ["all", "all"],
                "vacancy_rate_pct": [vacancy, 1.0],
                "num_quartiers": [3, 1],
                "survey_year": [2023, 2023],
                "survey_period": ["octobre 2023"] * 2,
            }
        ),
        join(
            store.partition_dir(vacancy_rates.key.path[-1], DATE, NEIGHBORHOOD),
            VACANCY_FILE,
        ),
    )


#: The three surveyed rents the fixture prices commercial floor at, near what
#: the Q2 2026 reports actually publish for VSMPE's Midtown North submarket:
#: office $22.39 gross, industrial $12.98 net + $4.09 additional, and the
#: stated retail base.
RETAIL_RENT, OFFICE_RENT, INDUSTRIAL_RENT = 26.0, 22.39, 17.07


def write_commercial_rents(store, **overrides) -> None:
    """`silver/commercial_rents` as `lot_assessment_comparables` reads it."""
    rents = {
        "retail": RETAIL_RENT,
        "office": OFFICE_RENT,
        "industrial": INDUSTRIAL_RENT,
        **overrides,
    }
    write_frame(
        pd.DataFrame(
            {
                "rent_class": list(rents),
                "rent_psf_cad": list(rents.values()),
                "submarket": ["Midtown North"] * len(rents),
                "is_submarket_rate": [True] * len(rents),
                "source": ["cushman_wakefield_marketbeat"] * len(rents),
                "source_period": ["2026-Q2"] * len(rents),
                "rent_basis": ["measured"] * len(rents),
                "index_period": ["2026-04"] * len(rents),
            }
        ),
        join(
            store.partition_dir(
                commercial_rents.key.path[-1], DATE, NEIGHBORHOOD
            ),
            COMMERCIAL_RENTS_FILE,
        ),
    )


@pytest.fixture
def borough(store):
    """Every upstream this asset reads, for one borough-day."""
    write_lot_values(store)
    write_units(store)
    write_crosswalk(store)
    write_cmhc(store)
    write_commercial_rents(store)
    return store


def run(store, **config):
    return materialize(
        [lot_assessment_comparables],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        run_config=(
            {"ops": {"silver__lot_assessment_comparables": {"config": config}}}
            if config
            else None
        ),
    )


def written(store) -> gpd.GeoDataFrame:
    return gpd.read_parquet(
        Path(
            store.partition_dir(
                lot_assessment_comparables.key.path[-1], DATE, NEIGHBORHOOD
            )
        )
        / LOT_COMPARABLES_FILE
    ).set_index("NO_LOT")


def test_every_lot_keeps_its_row(borough):
    result = run(borough)

    assert result.success
    frame = written(borough)
    assert sorted(frame.index) == sorted(_LOTS)


def test_the_roll_is_summed_onto_the_lot_the_crosswalk_names(borough):
    run(borough)

    frame = written(borough)
    assert frame.loc[LOT_TRIPLEX_A, "num_dwellings"] == 3
    assert frame.loc[LOT_MIXED, "num_dwellings"] == 3
    assert frame.loc[LOT_MIXED, "residential_floor_area_m2"] == 200.0
    assert frame.loc[LOT_MIXED, "commercial_floor_area_m2"] == 90.0


def test_the_value_columns_are_carried_and_not_recomputed(borough):
    """The two silver tables must not be able to disagree about what a lot is
    worth, so this asset carries that answer rather than arriving at its own."""
    run(borough)

    frame = written(borough)
    assert frame.loc[LOT_MIXED, "total_assessed_value"] == 1_200_000.0
    assert frame.loc[LOT_TRIPLEX_A, "total_assessed_value"] == 900_000.0


def test_a_mixed_lot_earns_from_both_of_its_units(borough):
    run(borough)

    row = written(borough).loc[LOT_MIXED]
    assert row["residential_income_cad"] > 0
    assert row["commercial_income_cad"] > 0
    assert row["gross_income_cad"] == pytest.approx(
        row["residential_income_cad"] + row["commercial_income_cad"]
    )


def test_the_cap_rate_is_the_income_over_the_carried_value(borough):
    run(borough)

    row = written(borough).loc[LOT_TRIPLEX_A]
    assert row["cap_rate_pct"] == pytest.approx(
        100.0 * row["net_operating_income_cad"] / row["total_assessed_value"]
    )


def test_a_lot_nothing_is_assessed_on_has_no_cap_rate_and_still_has_a_value(borough):
    """A lane earns nothing the roll knows about, and is still worth the ground
    its neighbours sit on."""
    run(borough)

    row = written(borough).loc[LOT_LANE]
    assert pd.isna(row["total_assessed_value"])
    assert pd.isna(row["cap_rate_pct"])
    assert row["estimated_value_basis"] == "per_land_area"
    assert row["estimated_value_cad"] > 0


def test_a_residential_lots_first_comparable_is_the_other_triplex(borough):
    """The shop sits between them and is nearer to neither."""
    run(borough)

    payload = json.loads(written(borough).loc[LOT_TRIPLEX_A, "comparables"])
    entries = payload["neighbors"]
    assert entries[0]["lot_number"] == LOT_TRIPLEX_B


def test_the_comparables_carry_the_metric_that_produced_them(borough):
    """A neighbour list means nothing without the weights it was chosen under -
    the rule every stated assumption in this platform follows."""
    run(borough, k=3, distance_scale_m=250.0, use_weight=2.0)

    payload = json.loads(written(borough).loc[LOT_TRIPLEX_A, "comparables"])
    assert payload["k"] == 3
    assert payload["scales"]["distance_m"] == 250.0
    assert payload["weights"]["use_code"] == 2.0
    assert payload["num_candidates"] == 4


def test_every_row_records_the_income_assumptions_behind_its_rate(borough):
    run(borough, operating_expense_ratio=0.4, market_value_factor=1.2)

    payload = json.loads(written(borough).loc[LOT_TRIPLEX_A, "income_assumptions"])
    assert payload["operating_expense_ratio"] == 0.4
    assert payload["market_value_factor"] == 1.2
    assert payload["average_rent_cad"] == 1_200.0
    assert payload["vacancy_rate_pct"] == 2.0


def test_k_bounds_the_neighbour_list(borough):
    run(borough, k=2)

    frame = written(borough)
    assert frame.loc[LOT_TRIPLEX_A, "num_comparables"] == 2
    assert len(json.loads(frame.loc[LOT_TRIPLEX_A, "comparables"])["neighbors"]) == 2


def test_a_suppressed_rent_leaves_the_comparables_intact(borough, store):
    """CMHC publishing nothing is a fact about the survey. Half of this asset
    does not need a rent, and that half still has to run."""
    write_cmhc(store, rent=None, vacancy=None)

    result = run(borough)

    assert result.success
    frame = written(borough)
    assert pd.isna(frame.loc[LOT_TRIPLEX_A, "cap_rate_pct"])
    assert (frame.loc[[LOT_TRIPLEX_A, LOT_TRIPLEX_B], "num_comparables"] > 0).all()
    assert (frame["estimated_value_cad"] > 0).all()


def test_a_commercial_lot_keeps_its_rate_when_the_rent_is_suppressed(borough, store):
    """The point of pricing the non-residential floor at a stated rate rather
    than leaving it out: a shop's income never depended on CMHC, so a borough
    CMHC suppressed still has a cap rate on every lot with a shop in it."""
    write_cmhc(store, rent=None, vacancy=None)

    run(borough)

    frame = written(borough)
    assert frame.loc[LOT_SHOP, "cap_rate_pct"] > 0
    # The mixed lot keeps a rate too, and it is now its commercial half alone.
    assert pd.isna(frame.loc[LOT_MIXED, "residential_income_cad"])
    assert frame.loc[LOT_MIXED, "gross_income_cad"] == pytest.approx(
        frame.loc[LOT_MIXED, "commercial_income_cad"]
    )


def test_the_frame_that_was_written_is_the_frame_that_is_published(borough, published):
    run(borough)

    assert published["calls"] == 1
    assert published["partition"] == (NEIGHBORHOOD, DATE)
    assert set(published["datasets"]) == {"lot_assessment_comparables"}
    assert len(published["datasets"]["lot_assessment_comparables"]) == len(_LOTS)


def test_a_missing_upstream_names_the_asset_to_materialize(store):
    write_lot_values(store)

    with pytest.raises(Failure, match="materialize assessment_units"):
        run(store)


def test_the_run_reports_the_pool_its_neighbours_came_from(borough):
    result = run(borough)

    metadata = materialization_metadata(result, lot_assessment_comparables)
    # Four of the five lots carry a value; the lane is a subject only.
    assert metadata["num_candidates"].value == 4
    assert metadata["num_lots"].value == len(_LOTS)
    # The cross-check: this asset re-derives the placement and has to land on
    # the same unit counts the totals it carries were summed over.
    assert metadata["num_units_disagreeing"].value == 0


# -- maintenance: an old building keeps less of its rent --------------------


def _triplex(year_built) -> pd.DataFrame:
    """One residential lot, its year built the only thing that varies."""
    return pd.DataFrame(
        {
            "num_dwellings": [3.0],
            "year_built": [year_built],
            "residential_floor_area_m2": [200.0],
            "commercial_floor_area_m2": [0.0],
            "industrial_floor_area_m2": [0.0],
            "retail_floor_area_m2": [0.0],
            "office_floor_area_m2": [0.0],
        }
    )


def _rented(**kwargs) -> IncomeAssumptions:
    return IncomeAssumptions(
        average_rent_cad=1200.0, vacancy_rate_pct=1.0, **kwargs
    )


def test_two_identical_triplexes_of_different_ages_earn_the_same_and_keep_different():
    # The whole point of the change: the gross is a function of the dwellings
    # and the rent, and only what the owner keeps depends on the roof.
    old = annual_income(_triplex(1920), _rented(income_reference_year=2026))
    new = annual_income(_triplex(2024), _rented(income_reference_year=2026))

    assert old["gross_income_cad"].iloc[0] == new["gross_income_cad"].iloc[0]
    assert (
        old["net_operating_income_cad"].iloc[0]
        < new["net_operating_income_cad"].iloc[0]
    )


def test_a_new_building_is_charged_the_base_ratio_and_nothing_more():
    income = annual_income(
        _triplex(2026), _rented(income_reference_year=2026, operating_expense_ratio=0.35)
    )

    assert income["maintenance_premium"].iloc[0] == 0.0
    assert income["effective_operating_expense_ratio"].iloc[0] == pytest.approx(0.35)


def test_the_premium_is_capped_however_old_the_building():
    income = annual_income(_triplex(1850), _rented(income_reference_year=2026))

    assert income["maintenance_premium"].iloc[0] == pytest.approx(
        MAX_MAINTENANCE_PREMIUM
    )


def test_a_lot_with_no_year_built_is_charged_the_assumed_age():
    # Not the new-build ratio: an unstated year is likelier to be old stock.
    income = annual_income(
        _triplex(None),
        _rented(income_reference_year=2026, assumed_building_age_years=50.0),
    )
    stated = annual_income(_triplex(1976), _rented(income_reference_year=2026))

    assert income["building_age_years"].iloc[0] == 50.0
    assert income["maintenance_premium"].iloc[0] == pytest.approx(
        stated["maintenance_premium"].iloc[0]
    )


def test_without_a_reference_year_every_lot_is_charged_the_base():
    # No year, no age - and inventing one from the wall clock would make a
    # partition's cap rates depend on the day it was materialized.
    income = annual_income(_triplex(1920), _rented(operating_expense_ratio=0.35))

    assert pd.isna(income["building_age_years"].iloc[0])
    assert income["maintenance_premium"].iloc[0] == 0.0
    assert income["effective_operating_expense_ratio"].iloc[0] == pytest.approx(0.35)
    assert income["net_operating_income_cad"].iloc[0] == pytest.approx(
        income["gross_income_cad"].iloc[0] * 0.65
    )


def test_a_zero_per_year_curve_reproduces_the_flat_ratio():
    # The escape hatch: the behaviour this asset had before the curve existed
    # is still reachable, and is one setting rather than a code path.
    flat = annual_income(
        _triplex(1920),
        _rented(income_reference_year=2026, maintenance_premium_per_year=0.0),
    )

    assert flat["effective_operating_expense_ratio"].iloc[0] == pytest.approx(0.35)


def test_the_curve_and_the_age_travel_in_the_assumptions():
    # A ratio with no curve beside it cannot be read against another run's:
    # 0.43 is a 1955 building on the default curve and a 1990 one on a steeper.
    assumptions = _rented(income_reference_year=2026)
    payload = assumptions.as_metadata()

    assert payload["income_reference_year"] == 2026
    assert payload["maintenance_premium_per_year"] > 0
    assert payload["max_maintenance_premium"] == MAX_MAINTENANCE_PREMIUM
    assert payload["assumed_building_age_years"] > 0


def test_a_negative_premium_is_refused_by_the_assumptions():
    with pytest.raises(ValueError, match="maintenance_premium_per_year"):
        IncomeAssumptions(maintenance_premium_per_year=-0.01)
