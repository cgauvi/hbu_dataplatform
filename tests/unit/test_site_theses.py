"""The second axis of `lot_investment_opportunities`: why a site is acquirable.

`urban_rag.opportunities`'s site functions are arithmetic over a frame and are
tested directly - which thesis fires on what, what the heritage rows screen,
what each thesis's denominator carries, and how the improvement is sized. The
asset is then materialized over hand-written gap, HBU, comparables and zone
partitions, the way `test_opportunities.py` materializes it over the gap alone.

The fixture borough is one lot per case: a gas station, an obsolete plex under
a six-storey grid, the same plex in a heritage sector, a parking lot, a plex
that can take a storey, one the solver never reached, and one built out.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
from dagster import MultiPartitionKey, materialize

from asset_helpers import materialization_metadata, stub_publish
from urban_rag import opportunity_assets
from urban_rag.comparables_assets import (
    LOT_COMPARABLES_FILE,
    lot_assessment_comparables,
)
from urban_rag.envelope_assets import ZONE_COLUMNS_FILE, zoning_grid_columns
from urban_rag.frames import write_frame
from urban_rag.hbu_assets import (
    LOT_GAP_FILE,
    LOT_HBU_FILE,
    lot_highest_best_use,
    lot_redevelopment_gap,
)
from urban_rag.opportunities import (
    BROWNFIELD,
    IMPROVEMENT,
    INFILL,
    NO_SITE_THESIS,
    SITE_THESES,
    TEARDOWN,
    SiteRules,
    assign_site_thesis,
    heritage_flags,
    improvement_program,
    rank_site_opportunities,
    site_thesis_summary,
    site_yield_on_cost_pct,
)
from urban_rag.opportunity_assets import (
    LOT_OPPORTUNITIES_FILE,
    lot_investment_opportunities,
)
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import join

DATE = "2026-09-01"
NEIGHBORHOOD = "VSMPE"


def lot(**overrides) -> dict:
    """One lot as the joined frame the site functions read, defaults filled.

    A 1950 two-storey plex of 240 m2 on a 300 m2 lot, under a grid the solver
    filled to six storeys and 1 000 m2, redeveloping at a gain.
    """
    row = {
        "lot_uid": 1,
        "lot_number": "1 000 000",
        "lot_area_m2": 300.0,
        "is_underbuilt": True,
        "existing_floor_area_m2": 240.0,
        "existing_num_dwellings": 2,
        "existing_year_built": 1950,
        "existing_num_storeys": 2,
        "existing_dominant_use_code": "1000",
        "existing_dominant_income_class": "residential",
        "existing_total_assessed_value": 400_000.0,
        "hbu_floor_area_m2": 1_000.0,
        "hbu_residential_floor_area_m2": 1_000.0,
        "hbu_commercial_floor_area_m2": 0.0,
        "hbu_industrial_floor_area_m2": 0.0,
        "hbu_floors": 6,
        "hbu_footprint_m2": 180.0,
        "hbu_annual_stabilised_noi_cad": 120_000.0,
        "hbu_total_capital_cost_cad": 1_500_000.0,
        "floor_area_gap_m2": 760.0,
        "redevelopment_npv_gain_cad": 500_000.0,
        "heritage_sector": None,
        "piia_sector": None,
    }
    row.update(overrides)
    return row


def frame(*rows: dict) -> pd.DataFrame:
    built = pd.DataFrame([lot(**row) for row in rows])
    built["lot_uid"] = range(1, len(built) + 1)
    return built


# -- which thesis fires -----------------------------------------------------


def test_an_obsolete_plex_under_an_unused_envelope_is_a_teardown():
    assigned = assign_site_thesis(frame({}))
    assert assigned.loc[0, "site_thesis"] == TEARDOWN
    assert assigned.loc[0, "is_teardown_site"]
    assert assigned.loc[0, "storey_headroom"] == 4
    assert assigned.loc[0, "built_share"] == pytest.approx(0.24)


@pytest.mark.parametrize(
    "overrides",
    [
        # Too young to be presumed obsolete.
        {"existing_year_built": 1975},
        # Fills too much of the envelope.
        {"existing_floor_area_m2": 600.0},
        # Only one storey of headroom: a like-for-like replacement.
        {"hbu_floors": 3},
        # Already built to its envelope.
        {"is_underbuilt": False},
        # The roll states no year, so obsolescence cannot be presumed.
        {"existing_year_built": None},
    ],
)
def test_the_teardown_screen_needs_every_condition(overrides):
    assigned = assign_site_thesis(frame(overrides))
    assert not assigned.loc[0, "is_teardown_site"]


def test_a_gas_station_is_a_brownfield_whatever_else_holds():
    """Precedence: the use decides, and it is not screened on is_underbuilt -
    what changes on a service station is the use, not the floor."""
    assigned = assign_site_thesis(
        frame({"existing_dominant_use_code": "5533", "is_underbuilt": False})
    )
    assert assigned.loc[0, "site_thesis"] == BROWNFIELD
    assert assigned.loc[0, "is_brownfield_use"]
    assert not assigned.loc[0, "is_teardown_site"]


@pytest.mark.parametrize(
    "code, expected",
    [
        ("3280", True),  # atelier d'usinage - manufacturing
        ("4299", True),  # transport yard
        ("4879", True),  # salvage
        ("6411", True),  # garage
        ("6379", True),  # warehousing
        ("6231", True),  # dry cleaning
        ("5010", False),  # immeuble commercial
        ("1000", False),  # dwelling
        ("4621", False),  # a parking lot is infill, not a risk activity
        ("", False),
    ],
)
def test_the_brownfield_prefixes_are_the_regimes_families(code, expected):
    assigned = assign_site_thesis(frame({"existing_dominant_use_code": code}))
    assert bool(assigned.loc[0, "is_brownfield_use"]) is expected


def test_a_use_code_that_came_back_as_a_float_still_matches():
    assigned = assign_site_thesis(frame({"existing_dominant_use_code": 6411.0}))
    assert assigned.loc[0, "is_brownfield_use"]


def test_a_parking_lot_with_nothing_on_it_is_infill():
    assigned = assign_site_thesis(
        frame(
            {
                "existing_floor_area_m2": 0.0,
                "existing_num_dwellings": 0,
                "existing_year_built": None,
                "existing_num_storeys": None,
                "existing_dominant_use_code": "4621",
            }
        )
    )
    assert assigned.loc[0, "site_thesis"] == INFILL
    assert assigned.loc[0, "is_infill_site"]
    assert not assigned.loc[0, "is_improvement_site"]


def test_a_recent_plex_with_a_storey_of_headroom_is_an_improvement():
    """Not old enough to tear down; the building stays and gains a floor."""
    assigned = assign_site_thesis(frame({"existing_year_built": 1985}))
    assert assigned.loc[0, "site_thesis"] == IMPROVEMENT
    assert assigned.loc[0, "is_improvement_site"]


def test_a_teardown_is_also_an_improvement_and_the_flag_says_so():
    """Precedence names the lot; the booleans keep the rest."""
    assigned = assign_site_thesis(frame({}))
    assert assigned.loc[0, "site_thesis"] == TEARDOWN
    assert assigned.loc[0, "is_improvement_site"]


def test_a_lot_the_solver_never_reached_has_no_site_thesis():
    assigned = assign_site_thesis(
        frame(
            {
                "hbu_floor_area_m2": np.nan,
                "hbu_floors": None,
                "hbu_footprint_m2": None,
                "existing_dominant_use_code": "5533",
            }
        )
    )
    assert assigned.loc[0, "site_thesis"] == NO_SITE_THESIS
    assert not assigned.loc[0, "is_brownfield_site"]
    # The use is still a risk use - that is a fact about the roll, not the
    # solver - so the boolean holds while the thesis does not.
    assert assigned.loc[0, "is_brownfield_use"]


# -- heritage ---------------------------------------------------------------


def test_a_heritage_sector_keeps_a_lot_out_of_the_theses_that_demolish():
    assigned = assign_site_thesis(frame({"heritage_sector": "Oui"}))
    assert assigned.loc[0, "is_heritage_sector"]
    assert assigned.loc[0, "is_demolition_restricted"]
    assert not assigned.loc[0, "is_teardown_site"]
    # The building can still gain a storey: a heritage sector reviews what is
    # built, it does not forbid an addition.
    assert assigned.loc[0, "site_thesis"] == IMPROVEMENT


def test_a_heritage_sector_keeps_a_brownfield_out_too():
    assigned = assign_site_thesis(
        frame({"heritage_sector": "Oui", "existing_dominant_use_code": "6411"})
    )
    assert assigned.loc[0, "is_brownfield_use"]
    assert not assigned.loc[0, "is_brownfield_site"]


def test_a_dash_in_the_heritage_row_is_no_sector():
    flags = heritage_flags(frame({"heritage_sector": "-"}))
    assert not flags.loc[0, "is_heritage_sector"]


def test_a_piia_sector_is_flagged_and_not_screened_by_default():
    assigned = assign_site_thesis(frame({"piia_sector": "2"}))
    assert assigned.loc[0, "has_piia_review"]
    assert not assigned.loc[0, "is_demolition_restricted"]
    assert assigned.loc[0, "site_thesis"] == TEARDOWN


def test_the_piia_screen_is_a_switch():
    rules = SiteRules(exclude_piia_sectors=True)
    assigned = assign_site_thesis(frame({"piia_sector": "2"}), rules)
    assert assigned.loc[0, "is_demolition_restricted"]
    assert not assigned.loc[0, "is_teardown_site"]


def test_a_pre_1940_building_needs_a_demolition_review_and_is_flagged():
    """The Loi sur le patrimoine culturel's floor for a demolition by-law."""
    flags = heritage_flags(frame({"existing_year_built": 1925}))
    assert flags.loc[0, "demolition_review_required"]
    assert not flags.loc[0, "is_demolition_restricted"]

    strict = heritage_flags(
        frame({"existing_year_built": 1925}),
        SiteRules(exclude_demolition_review=True),
    )
    assert strict.loc[0, "is_demolition_restricted"]


def test_the_heritage_switch_can_be_turned_off():
    rules = SiteRules(exclude_heritage_sectors=False)
    assigned = assign_site_thesis(frame({"heritage_sector": "Oui"}), rules)
    assert assigned.loc[0, "is_heritage_sector"]
    assert assigned.loc[0, "is_teardown_site"]


# -- the improvement --------------------------------------------------------


def test_the_added_storey_is_the_standing_footprint():
    """240 m2 over two storeys is a 120 m2 plate; one more storey is 120 m2,
    plus a 60 m2 annex on the ground the solver covers and the plex does not,
    raised two storeys."""
    program = improvement_program(frame({}))
    assert program.loc[0, "existing_footprint_m2"] == pytest.approx(120.0)
    assert program.loc[0, "improvement_added_storeys"] == 1
    assert program.loc[0, "improvement_floor_m2"] == pytest.approx(120.0 + 60.0 * 2)


def test_the_improvement_is_capped_at_the_floor_gap():
    program = improvement_program(frame({"floor_area_gap_m2": 100.0}))
    assert program.loc[0, "improvement_floor_m2"] == pytest.approx(100.0)


def test_two_storeys_may_be_added_when_the_rules_allow():
    program = improvement_program(
        frame({"hbu_footprint_m2": 120.0}),
        SiteRules(improvement_max_added_storeys=2),
    )
    assert program.loc[0, "improvement_added_storeys"] == 2
    assert program.loc[0, "improvement_floor_m2"] == pytest.approx(240.0)


def test_no_storey_count_means_no_invented_footprint():
    program = improvement_program(frame({"existing_num_storeys": None}))
    assert pd.isna(program.loc[0, "improvement_floor_m2"])
    assigned = assign_site_thesis(frame({"existing_num_storeys": None}))
    assert not assigned.loc[0, "is_improvement_site"]


def test_the_addition_earns_and_costs_the_solvers_per_square_metre_rates():
    """$120 of NOI and $1 500 of capital per m2 on this lot; the addition of
    240 m2 earns 240 x 120 and costs 240 x 1 500 x 1.25."""
    program = improvement_program(frame({}))
    assert program.loc[0, "improvement_noi_cad"] == pytest.approx(240.0 * 120.0)
    assert program.loc[0, "improvement_cost_cad"] == pytest.approx(
        240.0 * 1_500.0 * 1.5
    )
    assert program.loc[0, "improvement_yield_pct"] == pytest.approx(
        100.0 * 120.0 / (1_500.0 * 1.5), abs=1e-3
    )


# -- the denominators -------------------------------------------------------


def test_a_teardown_pays_to_demolish_what_stands():
    assigned = assign_site_thesis(frame({}))
    economics = site_yield_on_cost_pct(frame({}), assigned)
    assert economics.loc[0, "demolition_cost_cad"] == pytest.approx(240.0 * 150.0)
    assert economics.loc[0, "remediation_cost_cad"] == 0.0
    assert economics.loc[0, "site_total_project_cost_cad"] == pytest.approx(
        1_500_000.0 + 400_000.0 + 36_000.0
    )
    assert economics.loc[0, "site_yield_on_cost_pct"] == pytest.approx(
        100.0 * 120_000.0 / 1_936_000.0, abs=1e-3
    )


def test_a_brownfield_pays_to_characterise_and_clean_to_the_housing_criterion():
    rows = frame({"existing_dominant_use_code": "5533"})
    assigned = assign_site_thesis(rows)
    economics = site_yield_on_cost_pct(rows, assigned)
    assert economics.loc[0, "site_assessment_cost_cad"] == 12_000.0
    assert economics.loc[0, "remediation_cost_cad"] == pytest.approx(300.0 * 150.0)


def test_a_brownfield_rebuilt_as_industry_cleans_to_the_lower_bar():
    rows = frame(
        {
            "existing_dominant_use_code": "3280",
            "existing_dominant_income_class": "industrial",
            "hbu_residential_floor_area_m2": 0.0,
            "hbu_industrial_floor_area_m2": 1_000.0,
        }
    )
    assigned = assign_site_thesis(rows)
    economics = site_yield_on_cost_pct(rows, assigned)
    assert economics.loc[0, "remediation_cost_cad"] == pytest.approx(300.0 * 75.0)


def test_a_commercial_shell_comes_down_at_the_non_residential_rate():
    rows = frame(
        {
            "existing_dominant_use_code": "5010",
            "existing_dominant_income_class": "commercial",
        }
    )
    assigned = assign_site_thesis(rows)
    economics = site_yield_on_cost_pct(rows, assigned)
    assert economics.loc[0, "demolition_cost_cad"] == pytest.approx(240.0 * 250.0)


def test_an_improvement_yields_on_the_addition_alone():
    rows = frame({"existing_year_built": 1985})
    assigned = assign_site_thesis(rows)
    economics = site_yield_on_cost_pct(rows, assigned)
    assert economics.loc[0, "site_total_project_cost_cad"] == pytest.approx(
        assigned.loc[0, "improvement_cost_cad"]
    )
    assert economics.loc[0, "site_yield_on_cost_pct"] == pytest.approx(
        assigned.loc[0, "improvement_yield_pct"]
    )


def test_the_land_factor_scales_the_land_here_too():
    rows = frame({})
    assigned = assign_site_thesis(rows)
    economics = site_yield_on_cost_pct(rows, assigned, market_value_factor=1.5)
    assert economics.loc[0, "site_total_project_cost_cad"] == pytest.approx(
        1_500_000.0 + 600_000.0 + 36_000.0
    )


# -- the rank ---------------------------------------------------------------


def test_rank_is_within_the_site_thesis():
    rows = frame(
        {},
        {"existing_dominant_use_code": "6411"},
        {"hbu_annual_stabilised_noi_cad": 200_000.0},
    )
    ranked = rank_site_opportunities(rows)
    assert ranked["site_thesis"].tolist() == [TEARDOWN, BROWNFIELD, TEARDOWN]
    assert ranked["site_thesis_rank"].tolist() == [2, 1, 1]
    assert ranked["num_ranked_in_site_thesis"].tolist() == [2, 1, 2]


def test_a_site_that_does_not_pay_keeps_its_thesis_and_loses_its_rank():
    rows = frame({"redevelopment_npv_gain_cad": -10_000.0})
    ranked = rank_site_opportunities(rows)
    assert ranked.loc[0, "site_thesis"] == TEARDOWN
    assert pd.isna(ranked.loc[0, "site_thesis_rank"])

    lenient = rank_site_opportunities(rows, rules=SiteRules(require_positive_npv=False))
    assert lenient.loc[0, "site_thesis_rank"] == 1


def test_an_industrial_zone_that_never_pencils_is_still_filed():
    """The point of the switch: a pure-I grid whose every program loses money
    still gets its teardowns ordered when asked."""
    rows = frame(
        {
            "hbu_residential_floor_area_m2": 0.0,
            "hbu_industrial_floor_area_m2": 1_000.0,
            "redevelopment_npv_gain_cad": -250_000.0,
            "existing_dominant_use_code": "3999",
        }
    )
    ranked = rank_site_opportunities(rows, rules=SiteRules(require_positive_npv=False))
    assert ranked.loc[0, "site_thesis"] == BROWNFIELD
    assert ranked.loc[0, "site_thesis_rank"] == 1


def test_the_improvement_rank_needs_the_addition_to_earn():
    rows = frame({"existing_year_built": 1985, "hbu_annual_stabilised_noi_cad": 0.0})
    ranked = rank_site_opportunities(rows)
    assert ranked.loc[0, "site_thesis"] == IMPROVEMENT
    assert pd.isna(ranked.loc[0, "site_thesis_rank"])


def test_top_n_marks_a_flag_over_the_rank():
    rows = frame(
        *({"hbu_annual_stabilised_noi_cad": noi} for noi in (300e3, 200e3, 100e3))
    )
    ranked = rank_site_opportunities(rows, top_n=2)
    assert ranked["is_top_site_opportunity"].tolist() == [True, True, False]


def test_every_site_thesis_gets_a_summary_row():
    rows = frame({})
    summary = site_thesis_summary(pd.concat([rows, rank_site_opportunities(rows)], axis=1))
    assert summary["site_thesis"].tolist() == list(SITE_THESES)
    teardown = summary.set_index("site_thesis").loc[TEARDOWN]
    assert teardown["num_lots"] == 1
    # The owner's gain over holding, the demolition in: 500k less 240 m2 at $150.
    assert teardown["total_verdict_cad"] == pytest.approx(500_000.0 - 36_000.0)


def test_rules_that_make_no_sense_are_refused():
    with pytest.raises(ValueError):
        SiteRules(teardown_max_built_share=0.0)
    with pytest.raises(ValueError):
        SiteRules(addition_cost_premium=0.0)
    with pytest.raises(ValueError):
        SiteRules(brownfield_use_prefixes=())


# -- the asset --------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture(autouse=True)
def published(monkeypatch):
    return stub_publish(monkeypatch, opportunity_assets)


def _gap_rows() -> pd.DataFrame:
    rows = frame(
        {"lot_number": "1 000 001"},  # teardown
        {"lot_number": "1 000 002", "existing_dominant_use_code": "5533"},  # brownfield
        {"lot_number": "1 000 003"},  # in a heritage zone -> improvement
        {"lot_number": "1 000 004", "existing_year_built": 1985},  # improvement
        {"lot_number": "1 000 005", "is_underbuilt": False, "existing_year_built": 1985},
    )
    gap = rows.drop(
        columns=[
            "existing_year_built",
            "existing_num_storeys",
            "hbu_floors",
            "hbu_footprint_m2",
            "heritage_sector",
            "piia_sector",
        ]
    )
    gap["hbu_status"] = "solved"
    gap["primary_frontage_m"] = 10.0
    gap["existing_dominant_income_class"] = "residential"
    gap["existing_dominant_use_description"] = "Logement"
    gap["existing_cap_rate_pct"] = 3.5
    gap["existing_annual_stabilised_noi_cad"] = 20_000.0
    gap["hbu_num_dwellings"] = 10
    gap["dwelling_gap"] = 8
    gap["annual_stabilised_noi_gap_cad"] = 100_000.0
    gap["operating_expense_ratio"] = 0.35
    gap["neighborhood"] = NEIGHBORHOOD
    gap["scrape_date"] = DATE
    return gap


@pytest.fixture
def borough(store):
    gap = _gap_rows()
    write_frame(
        gap,
        join(
            store.partition_dir(lot_redevelopment_gap.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_GAP_FILE,
        ),
    )
    hbu = pd.DataFrame(
        {
            "lot_uid": gap["lot_uid"],
            "lot_number": gap["lot_number"],
            "floors": 6,
            "footprint_m2": 180.0,
            "grid_zone": ["H01-001", "C01-002", "H01-003", "H01-001", "H01-001"],
            "feature_id": ["H01-001", "C01-002", "H01-003", "H01-001", "H01-001"],
            "source_table": "ZONAGE",
        }
    )
    write_frame(
        hbu,
        join(
            store.partition_dir(lot_highest_best_use.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_HBU_FILE,
        ),
    )
    comparables = pd.DataFrame(
        {
            "lot_number": gap["lot_number"],
            "year_built": [1950, 1962, 1950, 1985, 1985],
            "num_storeys": 2,
        }
    )
    write_frame(
        comparables,
        join(
            store.partition_dir(
                lot_assessment_comparables.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_COMPARABLES_FILE,
        ),
    )
    zones = pd.DataFrame(
        {
            "source_table": "ZONAGE",
            "feature_id": ["H01-001", "H01-001", "C01-002", "H01-003"],
            "column_index": [0, 1, 0, 0],
            "heritage_sector": [None, None, None, "Oui"],
            "piia_sector": [None, None, "2", None],
        }
    )
    write_frame(
        zones,
        join(
            store.partition_dir(zoning_grid_columns.key.path[-1], DATE, NEIGHBORHOOD),
            ZONE_COLUMNS_FILE,
        ),
    )
    return store


def _materialize(store):
    return materialize(
        [lot_investment_opportunities],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource(dsn="postgresql://x")},
    )


def test_the_asset_joins_the_three_inputs_and_files_every_lot(borough):
    result = _materialize(borough)
    assert result.success
    frame_out = pd.read_parquet(
        join(
            borough.partition_dir(
                lot_investment_opportunities.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_OPPORTUNITIES_FILE,
        )
    ).set_index("lot_number")

    assert frame_out.loc["1 000 001", "site_thesis"] == TEARDOWN
    assert frame_out.loc["1 000 002", "site_thesis"] == BROWNFIELD
    assert frame_out.loc["1 000 002", "has_piia_review"]
    assert frame_out.loc["1 000 003", "is_heritage_sector"]
    assert frame_out.loc["1 000 003", "site_thesis"] == IMPROVEMENT
    assert frame_out.loc["1 000 004", "site_thesis"] == IMPROVEMENT
    assert frame_out.loc["1 000 005", "site_thesis"] == NO_SITE_THESIS

    # What the screen read travels with the row.
    assert frame_out.loc["1 000 001", "existing_year_built"] == 1950
    assert frame_out.loc["1 000 001", "hbu_floors"] == 6
    assert frame_out.loc["1 000 001", "grid_zone"] == "H01-001"
    assert frame_out.loc["1 000 003", "heritage_sector"] == "Oui"
    # The join keys to the zone do not.
    assert "feature_id" not in frame_out.columns

    assumptions = json.loads(frame_out.iloc[0]["screen_assumptions"])
    assert assumptions["demolition_cost_cad_per_m2"] == 150.0
    assert assumptions["demolition_cost_cad_per_m2_nonresidential"] == 250.0
    assert assumptions["addition_cost_premium"] == 1.5
    assert assumptions["heritage_source"] == "zoning_grid_columns"
    assert assumptions["site_top_n"] == 25

    metadata = materialization_metadata(result, lot_investment_opportunities)
    assert metadata["num_teardown_sites"].value == 1
    assert metadata["num_brownfield_sites"].value == 1
    assert metadata["num_improvement_sites"].value == 2
    assert metadata["num_heritage_sector_lots"].value == 1
    assert metadata["num_piia_review_lots"].value == 1
    # Every lot's zone had a grid row, and a grid printing '-' against the
    # heritage row is an answer rather than an unknown.
    assert metadata["num_lots_heritage_unknown"].value == 0


def test_the_asset_carries_the_config_into_the_rules(borough):
    result = materialize(
        [lot_investment_opportunities],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": borough, "postgis": PostgisResource(dsn="postgresql://x")},
        run_config={
            "ops": {
                "gold__lot_investment_opportunities": {
                    "config": {
                        "exclude_heritage_sectors": False,
                        "demolition_cost_cad_per_m2": 200.0,
                        "site_top_n": 1,
                    }
                }
            }
        },
    )
    assert result.success
    frame_out = pd.read_parquet(
        join(
            borough.partition_dir(
                lot_investment_opportunities.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_OPPORTUNITIES_FILE,
        )
    ).set_index("lot_number")
    # The heritage lot is a teardown once the switch is off.
    assert frame_out.loc["1 000 003", "site_thesis"] == TEARDOWN
    assert frame_out.loc["1 000 001", "demolition_cost_cad"] == pytest.approx(
        240.0 * 200.0
    )
    assert int(frame_out["is_top_site_opportunity"].sum()) == len(SITE_THESES) - 1


def test_a_zone_file_without_the_heritage_rows_screens_nothing_and_says_so(store):
    gap = _gap_rows()
    write_frame(
        gap,
        join(
            store.partition_dir(lot_redevelopment_gap.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_GAP_FILE,
        ),
    )
    write_frame(
        pd.DataFrame(
            {
                "lot_uid": gap["lot_uid"],
                "floors": 6,
                "footprint_m2": 180.0,
                "grid_zone": "H01-001",
                "feature_id": "H01-001",
                "source_table": "ZONAGE",
            }
        ),
        join(
            store.partition_dir(lot_highest_best_use.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_HBU_FILE,
        ),
    )
    write_frame(
        pd.DataFrame(
            {"lot_number": gap["lot_number"], "year_built": 1950, "num_storeys": 2}
        ),
        join(
            store.partition_dir(
                lot_assessment_comparables.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_COMPARABLES_FILE,
        ),
    )
    # A zone columns file parsed before the Patrimoine rows were read.
    write_frame(
        pd.DataFrame(
            {"source_table": ["ZONAGE"], "feature_id": ["H01-001"], "column_index": [0]}
        ),
        join(
            store.partition_dir(zoning_grid_columns.key.path[-1], DATE, NEIGHBORHOOD),
            ZONE_COLUMNS_FILE,
        ),
    )
    result = _materialize(store)
    assert result.success
    metadata = materialization_metadata(result, lot_investment_opportunities)
    assert metadata["num_lots_heritage_unknown"].value == len(gap)
    assert metadata["num_heritage_sector_lots"].value == 0
    frame_out = pd.read_parquet(
        join(
            store.partition_dir(
                lot_investment_opportunities.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_OPPORTUNITIES_FILE,
        )
    )
    assert json.loads(frame_out.iloc[0]["screen_assumptions"])["heritage_source"] == "absent"


# -- the three futures, for the owner and for a buyer ---------------------------


from urban_rag.opportunities import futures_economics, site_costs  # noqa: E402


def _with_futures(**overrides) -> pd.DataFrame:
    """A gap row that carries the enhancement solve and the three values."""
    return frame(
        {
            "existing_present_value_cad": 400_000.0,
            "hold_value_cad": 400_000.0,
            "hbu_npv_cad": 900_000.0,
            "rebuild_value_cad": 900_000.0,
            "enhance_solved": True,
            "enhance_status": "OPTIMAL",
            "enhance_added_storeys": 1,
            "enhance_added_floor_area_m2": 200.0,
            "enhance_capital_cost_cad": 450_000.0,
            "enhance_added_annual_stabilised_noi_cad": 30_000.0,
            "enhance_gain_cad": 120_000.0,
            "enhance_value_cad": 520_000.0,
            "existing_annual_stabilised_noi_cad": 20_000.0,
            **overrides,
        }
    )


def test_the_owner_sees_the_three_futures_with_the_site_costs_on_the_rebuild():
    rows = _with_futures()
    costs = site_costs(rows, pd.Series([False]), SiteRules())
    futures = futures_economics(rows, costs)
    assert futures.loc[0, "owner_hold_value_cad"] == 400_000.0
    assert futures.loc[0, "owner_enhance_value_cad"] == 520_000.0
    # 900k less 240 m2 of demolition at $150.
    assert futures.loc[0, "owner_rebuild_value_cad"] == pytest.approx(864_000.0)
    assert futures.loc[0, "owner_gain_enhance_cad"] == 120_000.0
    assert futures.loc[0, "owner_gain_rebuild_cad"] == pytest.approx(464_000.0)
    assert futures.loc[0, "owner_best_future"] == "rebuild"


def test_the_buyer_pays_the_larger_of_the_roll_and_the_income():
    rows = _with_futures(existing_total_assessed_value=300_000.0)
    costs = site_costs(rows, pd.Series([False]), SiteRules())
    futures = futures_economics(rows, costs, market_value_factor=1.2)
    # 300k x 1.2 = 360k is below the 400k the standing income is worth.
    assert futures.loc[0, "acquisition_cost_cad"] == 400_000.0
    assert futures.loc[0, "buyer_npv_hold_cad"] == 0.0
    assert futures.loc[0, "buyer_npv_enhance_cad"] == 120_000.0
    assert futures.loc[0, "buyer_npv_rebuild_cad"] == pytest.approx(464_000.0)
    assert futures.loc[0, "residual_price_rebuild_cad"] == pytest.approx(864_000.0)
    assert futures.loc[0, "buyer_best_future"] == "rebuild"

    dear = futures_economics(rows, costs, market_value_factor=2.0)
    assert dear.loc[0, "acquisition_cost_cad"] == 600_000.0
    assert dear.loc[0, "buyer_npv_hold_cad"] == -200_000.0


def test_the_buyers_yields_put_everything_paid_in_the_denominator():
    rows = _with_futures()
    costs = site_costs(rows, pd.Series([False]), SiteRules())
    futures = futures_economics(rows, costs)
    assert futures.loc[0, "buyer_yield_hold_pct"] == pytest.approx(100 * 20_000 / 400_000, abs=1e-3)
    assert futures.loc[0, "buyer_yield_enhance_pct"] == pytest.approx(
        100 * 50_000 / (400_000 + 450_000), abs=1e-3
    )
    assert futures.loc[0, "buyer_yield_rebuild_pct"] == pytest.approx(
        100 * 120_000 / (400_000 + 1_500_000 + 36_000), abs=1e-3
    )


def test_a_lot_the_roll_never_priced_has_no_buyer_columns():
    rows = _with_futures(existing_total_assessed_value=None)
    futures = futures_economics(rows, site_costs(rows, pd.Series([False]), SiteRules()))
    assert pd.isna(futures.loc[0, "acquisition_cost_cad"])
    assert pd.isna(futures.loc[0, "buyer_best_future"])
    # The owner's answer does not need a price.
    assert futures.loc[0, "owner_best_future"] == "rebuild"


def test_a_lot_without_an_enhancement_has_no_enhance_future():
    rows = _with_futures(enhance_solved=False, enhance_gain_cad=None, enhance_value_cad=None)
    futures = futures_economics(rows, site_costs(rows, pd.Series([False]), SiteRules()))
    assert pd.isna(futures.loc[0, "owner_enhance_value_cad"])
    assert futures.loc[0, "owner_best_future"] == "rebuild"


def test_holding_wins_where_neither_play_pays():
    rows = _with_futures(hbu_npv_cad=350_000.0, rebuild_value_cad=350_000.0, enhance_gain_cad=-5_000.0)
    futures = futures_economics(rows, site_costs(rows, pd.Series([False]), SiteRules()))
    assert futures.loc[0, "owner_best_future"] == "hold"
    # The price is the income's worth, so buying and keeping is exactly zero.
    assert futures.loc[0, "buyer_best_future"] == "hold"


def test_a_buyer_walks_where_no_future_clears_the_price():
    """The roll prices the property above its income and above what either
    play is worth; the least-bad loss is not a best."""
    rows = _with_futures(existing_total_assessed_value=2_000_000.0)
    futures = futures_economics(rows, site_costs(rows, pd.Series([False]), SiteRules()))
    assert futures.loc[0, "acquisition_cost_cad"] == 2_000_000.0
    assert futures.loc[0, "buyer_npv_rebuild_cad"] < 0
    assert futures.loc[0, "buyer_best_future"] == "none"
    assert futures.loc[0, "owner_best_future"] == "rebuild"


def test_the_improvement_reads_the_solve_where_the_gap_carries_one():
    program = improvement_program(_with_futures())
    assert program.loc[0, "improvement_source"] == "solve"
    assert program.loc[0, "improvement_floor_m2"] == 200.0
    assert program.loc[0, "improvement_cost_cad"] == 450_000.0
    assert program.loc[0, "improvement_noi_cad"] == 30_000.0
    assert program.loc[0, "improvement_yield_pct"] == pytest.approx(100 * 30_000 / 450_000, abs=1e-3)
    assert improvement_program(frame({})).loc[0, "improvement_source"] == "estimate"


def test_a_teardown_defers_to_the_enhancement_when_it_pays_more():
    """An old under-built plex where adding a storey beats rebuilding is an
    improvement, however well it meets the age and headroom screens."""
    rows = _with_futures(enhance_gain_cad=600_000.0, enhance_value_cad=1_000_000.0)
    ranked = rank_site_opportunities(rows)
    assert not ranked.loc[0, "is_teardown_site"]
    assert ranked.loc[0, "site_thesis"] == IMPROVEMENT
    assert ranked.loc[0, "owner_best_future"] == "enhance"
    assert ranked.loc[0, "site_verdict_cad"] == 600_000.0

    ranked = rank_site_opportunities(_with_futures())
    assert ranked.loc[0, "site_thesis"] == TEARDOWN
    assert ranked.loc[0, "site_verdict_cad"] == pytest.approx(464_000.0)


def test_the_rank_screens_on_the_owners_gain_with_the_site_costs_in():
    """A rebuild whose gain is eaten by the demolition is filed and unranked."""
    rows = _with_futures(hbu_npv_cad=420_000.0, rebuild_value_cad=420_000.0,
                         enhance_solved=False, enhance_gain_cad=None, enhance_value_cad=None)
    ranked = rank_site_opportunities(rows)
    assert ranked.loc[0, "site_thesis"] == TEARDOWN
    assert ranked.loc[0, "owner_gain_rebuild_cad"] == pytest.approx(-16_000.0)
    assert pd.isna(ranked.loc[0, "site_thesis_rank"])
