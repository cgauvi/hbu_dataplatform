"""Offline tests for `urban_rag.hbu` and the three assets over it.

Nothing here is stubbed except the upsert. The real `solve_program` runs on
every envelope these build, so what they cover includes the CP-SAT model - the
point being that this module's job is to hand that solver the right inputs and
put its answer beside the right things, and both halves are only checkable
against real answers.

The two arithmetic traps the module exists to avoid get a test each and are the
reason for most of the rest: `solve_program` returns income a *month* and
`comparables` returns it a *year*, and the two NOIs net out different things.
`test_gap_annualises_the_program` and `test_gap_nets_both_sides_the_same_way`
are those.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest
from asset_helpers import materialization_metadata, stub_publish as stub_publish_into
from dagster import Failure, MultiPartitionKey, materialize

from urban_rag import hbu, hbu_assets
from urban_rag.cmhc_assets import (
    AVERAGE_RENTS_FILE,
    VACANCY_FILE,
    average_rents,
    vacancy_rates,
)
from urban_rag.comparables_assets import (
    LOT_COMPARABLES_FILE,
    lot_assessment_comparables,
)
from urban_rag.envelope_assets import LOT_ENVELOPES_FILE, lot_zoning_envelopes
from urban_rag.frames import write_frame
from urban_rag.hbu_assets import (
    LOT_GAP_FILE,
    LOT_HBU_FILE,
    LOT_PROGRAMS_FILE,
    lot_development_programs,
    lot_highest_best_use,
    lot_redevelopment_gap,
)
from urban_rag.program import M2_PER_SQFT, MONTHS_PER_YEAR
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.setback_assets import LOT_SETBACKS_FILE, lot_buildable_setbacks
from urban_rag.storage import join

DATE = "2026-08-24"
NEIGHBORHOOD = "VSMPE"
ZONE_TABLE = "Reglement_urbanisme__VSP_REG_ZONE"

#: One residential envelope of a real shape: six storeys, a density of 3, a
#: coverage of 70% and a 500 m2 parcel, which is a Villeray mid-block lot under
#: a mixed-use column. Big enough that the solver has choices to make, small
#: enough that every cap can be reached by moving one number.
ENVELOPE = {
    "lot_uid": 1,
    "lot_number": "2 216 001",
    "neighborhood": NEIGHBORHOOD,
    "scrape_date": DATE,
    "feature_id": "C01-001",
    "source_table": ZONE_TABLE,
    "column_index": 0,
    "grid_zone": "C01-001",
    "pct_of_lot": 100.0,
    "overlap_area_m2": 500.0,
    "usages": json.dumps(["H.2", "C.2"]),
    "usage_habitation": "H.2",
    "usage_commerce": "C.2",
    "permits_residential": True,
    "permits_commercial": True,
    "permits_industrial": False,
    "governs_residential": True,
    "meets_min_lot_width": True,
    "solver_ready": True,
    "solver_error": None,
    "levels": json.dumps(["tous_les_niveaux"]),
    "residential_floors": 6,
    "lot_area_m2": 500.0,
    "primary_frontage_m": 20.0,
    "floors_min": 2,
    "floors_max": 6,
    "height_min_m": None,
    "height_max_m": 20.0,
    "min_lot_width_m": None,
    "max_dwellings": None,
    "density_min": None,
    "density_max": 3.0,
    "site_coverage_min_pct": None,
    "site_coverage_max_pct": 70.0,
}


def envelope(**overrides) -> dict:
    return {**ENVELOPE, **overrides}


def envelopes(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows) or [ENVELOPE])


def rent_frame(**overrides) -> pd.DataFrame:
    """CMHC's rent grid for the borough, as `average_rents` writes it."""
    rents = {
        "studio": 900.0,
        "1_bedroom": 1100.0,
        "2_bedroom": 1400.0,
        "3_bedroom_plus": 1800.0,
        "all": 1200.0,
        **overrides,
    }
    return pd.DataFrame(
        {
            "neighborhood": [NEIGHBORHOOD] * len(rents),
            "scrape_date": [DATE] * len(rents),
            "bedroom_type": list(rents),
            "average_rent_cad": list(rents.values()),
            "survey_year": [2025] * len(rents),
            "survey_period": ["October 2025"] * len(rents),
        }
    )


def vacancy_frame() -> pd.DataFrame:
    """CMHC's vacancy grid, both structures, as `vacancy_rates` writes it."""
    bedrooms = ["studio", "1_bedroom", "2_bedroom", "3_bedroom_plus", "all"]
    rates = [2.0, 1.5, 1.0, 0.8, 1.2]
    return pd.DataFrame(
        {
            "neighborhood": [NEIGHBORHOOD] * 10,
            "scrape_date": [DATE] * 10,
            "dwelling_type": ["all"] * 5 + ["row"] * 5,
            "bedroom_type": bedrooms * 2,
            # The `row` rows are deliberately absurd: nothing may read them.
            "vacancy_rate_pct": rates + [90.0] * 5,
        }
    )


def comparables_frame(**overrides) -> pd.DataFrame:
    """`lot_assessment_comparables` at the columns the gap reads."""
    row = {
        "NO_LOT": "2 216 001",
        "neighborhood": NEIGHBORHOOD,
        "scrape_date": DATE,
        "residential_floor_area_m2": 300.0,
        "commercial_floor_area_m2": 0.0,
        "industrial_floor_area_m2": 0.0,
        "num_dwellings": 3,
        "num_assessment_units": 1,
        "gross_income_cad": 50_000.0,
        "net_operating_income_cad": 32_500.0,
        "total_assessed_value": 900_000.0,
        "cap_rate_pct": 3.6,
        "dominant_use_code": "1000",
        "dominant_income_class": "residential",
        "income_assumptions": json.dumps({"operating_expense_ratio": 0.35}),
        **overrides,
    }
    return pd.DataFrame([row])


@pytest.fixture
def economics():
    return hbu.unit_economics(rent_frame(), vacancy_frame())[0]


# --------------------------------------------------------------------------
# the inputs
# --------------------------------------------------------------------------


def test_unit_economics_drops_the_all_total():
    """`all` is CMHC's total, not a fifth kind of dwelling to build."""
    economics, suppressed = hbu.unit_economics(rent_frame(), vacancy_frame())
    assert set(economics.average_rent_cad) == set(hbu.PRICED_BEDROOM_TYPES)
    assert "all" not in economics.average_rent_cad
    assert suppressed == ()


def test_unit_economics_reads_vacancy_off_the_all_structure_row():
    """A storey of a mid-rise is neither a row house nor CMHC's apartments."""
    economics, _ = hbu.unit_economics(rent_frame(), vacancy_frame())
    assert economics.vacancy_rate_pct["studio"] == 2.0


def test_unit_economics_reports_a_suppressed_class():
    """A cell CMHC will not publish has no key, and is named rather than zeroed."""
    rents = rent_frame()
    rents.loc[rents["bedroom_type"] == "3_bedroom_plus", "average_rent_cad"] = None
    economics, suppressed = hbu.unit_economics(rents, vacancy_frame())
    assert suppressed == ("3_bedroom_plus",)
    assert "3_bedroom_plus" not in economics.average_rent_cad
    assert economics.monthly_revenue("3_bedroom_plus") is None


def test_zone_column_of_round_trips_every_norm():
    """A row of the table is turned back into the object it was written from."""
    column = hbu.zone_column_of(ENVELOPE)
    assert column.usages == ("H.2", "C.2")
    assert (column.floors_min, column.floors_max) == (2, 6)
    assert column.density_max == 3.0
    assert column.site_coverage_max_pct == 70.0
    assert column.height_max_m == 20.0
    assert column.min_lot_width_m is None
    assert column.permits_residential and column.permits_commercial
    assert column.permitted_floors_count == 6


def test_lot_of_reads_a_missing_frontage_as_zero():
    """The reading `meets_min_lot_width` gives it: measured nothing, so no width."""
    lot = hbu.lot_of(envelope(primary_frontage_m=None))
    assert lot.frontage_m == 0.0
    assert lot.buildable_area_m2 is None


# --------------------------------------------------------------------------
# solving
# --------------------------------------------------------------------------


def test_solve_envelopes_answers_a_real_envelope(economics):
    programs = hbu.solve_envelopes(envelopes(), economics)
    assert len(programs) == 1
    row = programs.iloc[0]
    assert row["status"] == "OPTIMAL"
    assert row["solved"]
    assert row["num_dwellings"] > 0
    assert row["footprint_m2"] <= 350.0  # 70% of 500
    assert row["gross_floor_area_m2"] <= 1500.0 + 1e-6  # density 3 x 500
    assert json.loads(row["binding"])


def test_solve_envelopes_drops_a_column_that_authorises_no_dwelling(economics):
    """A Commerce column is not a candidate, and is not a failure either."""
    programs = hbu.solve_envelopes(
        envelopes(
            envelope(usages=json.dumps(["C.2"]), permits_residential=False),
            envelope(lot_uid=2, column_index=1),
        ),
        economics,
    )
    assert list(programs["lot_uid"]) == [2]


def test_solve_envelopes_drops_a_column_the_parser_could_not_read(economics):
    programs = hbu.solve_envelopes(
        envelopes(envelope(solver_ready=False, floors_max=None)), economics
    )
    assert programs.empty
    assert list(programs.columns)[: len(hbu.CANDIDATE_COLUMNS)] == list(
        hbu.CANDIDATE_COLUMNS
    )


def test_a_failed_solve_costs_its_row_and_not_the_frame(economics):
    """A parcel of no area cannot be a `Lot`; the borough is still answered."""
    programs = hbu.solve_envelopes(
        envelopes(
            envelope(lot_uid=1, lot_area_m2=0.0),
            envelope(lot_uid=2),
        ),
        economics,
    )
    failed = programs[programs["lot_uid"] == 1].iloc[0]
    assert failed["status"] == "ERROR"
    assert not failed["solved"]
    assert "area" in failed["solve_error"]
    assert programs[programs["lot_uid"] == 2].iloc[0]["solved"]


def test_the_buildable_area_caps_the_footprint(economics):
    """Margins are the second cap, and a shallow parcel is where it bites.

    A bare Habitation column, so that what stops the building is the envelope
    rather than commerce outbidding the housing for a storey - which is a real
    answer on the mixed-use column above and a different one. `setbacks` in
    `binding` is `program._footprint_cap_norm` saying the margins produced the
    envelope, not *Taux d'implantation*.
    """
    residential = {"usages": json.dumps(["H.2"]), "permits_commercial": False}
    without = hbu.solve_envelopes(envelopes(envelope(**residential)), economics).iloc[0]
    with_margins = hbu.solve_envelopes(
        envelopes(envelope(buildable_area_m2=120.0, **residential)), economics
    ).iloc[0]
    assert with_margins["footprint_m2"] <= 120.0
    assert with_margins["footprint_m2"] < without["footprint_m2"]
    assert with_margins["num_dwellings"] < without["num_dwellings"]
    assert "setbacks" in json.loads(with_margins["binding"])
    assert "setbacks" not in json.loads(without["binding"])


def test_a_suppressed_class_is_reported_and_not_built(economics):
    """The solver names what it was not allowed to price."""
    rents = rent_frame()
    rents.loc[rents["bedroom_type"] == "studio", "average_rent_cad"] = None
    thin, _ = hbu.unit_economics(rents, vacancy_frame())
    row = hbu.solve_envelopes(envelopes(), thin).iloc[0]
    assert json.loads(row["unpriced_types"]) == ["studio"]
    assert "studio" not in json.loads(row["units"])


def test_program_row_annualises_the_objective(economics):
    """The objective is a month; the assessment side of this platform is a year."""
    row = hbu.solve_envelopes(envelopes(), economics).iloc[0]
    assert row["annual_net_operating_income_cad"] == pytest.approx(
        row["monthly_net_operating_income_cad"] * MONTHS_PER_YEAR
    )
    assert row["annual_gross_revenue_cad"] == pytest.approx(
        row["monthly_gross_revenue_cad"] * MONTHS_PER_YEAR
    )


def test_residential_area_is_the_plate_and_not_the_unit_schedule(economics):
    """`footprint x residential_floors` is what compares with the roll's floor."""
    row = hbu.solve_envelopes(envelopes(), economics).iloc[0]
    assert row["residential_area_m2"] == pytest.approx(
        row["footprint_m2"] * row["residential_floors"]
    )
    assert row["unit_area_m2"] <= row["residential_area_m2"] + 1e-6


# --------------------------------------------------------------------------
# choosing
# --------------------------------------------------------------------------


def test_the_governing_column_wins_even_when_it_earns_less(economics):
    """The grid picks the column, not the income - see the module docstring."""
    frame = envelopes(
        envelope(column_index=0, governs_residential=True, floors_max=3),
        envelope(column_index=1, governs_residential=False, floors_max=6),
    )
    programs = hbu.solve_envelopes(frame, economics)
    richer = programs.sort_values("monthly_net_operating_income_cad").iloc[-1]
    assert not richer["governs_residential"]

    chosen = hbu.select_highest_best_use(programs, frame)
    assert len(chosen) == 1
    row = chosen.iloc[0]
    assert row["column_index"] == 0
    assert row["hbu_status"] == "solved"
    assert row["num_candidates"] == 2
    assert row["num_governing_candidates"] == 1


def test_the_zone_covering_most_of_the_lot_decides(economics):
    """A boundary sliver is a mapping disagreement, not a choice of rules."""
    frame = envelopes(
        envelope(feature_id="C01-001", pct_of_lot=3.0, floors_max=6),
        envelope(feature_id="H02-002", pct_of_lot=97.0, floors_max=2),
    )
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    row = chosen.iloc[0]
    assert row["feature_id"] == "H02-002"
    assert row["pct_of_lot"] == 97.0
    assert row["num_zones"] == 2


def test_a_lot_with_no_residential_column_keeps_its_row(economics):
    frame = envelopes(envelope(usages=json.dumps(["C.2"]), permits_residential=False))
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    row = chosen.iloc[0]
    assert row["hbu_status"] == "no_residential_column"
    assert row["num_candidates"] == 0
    assert pd.isna(row["status"])


def test_a_lot_with_no_governing_column_says_so(economics):
    """A parcel too narrow for every column of its own grid."""
    frame = envelopes(envelope(governs_residential=False))
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    assert chosen.iloc[0]["hbu_status"] == "no_governing_column"


def test_an_infeasible_governing_column_is_distinguished_from_an_absent_one(
    economics,
):
    """*Densite min* the parcel cannot meet is an answer about the parcel."""
    frame = envelopes(envelope(density_min=50.0, density_max=50.0))
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    assert chosen.iloc[0]["hbu_status"] == "infeasible"


def test_a_governing_column_that_raised_says_solver_error(economics):
    frame = envelopes(envelope(lot_area_m2=0.0))
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    assert chosen.iloc[0]["hbu_status"] == "solver_error"


def test_every_status_is_declared():
    """`HBU_STATUSES` is what the table's CHECK constraint is written from."""
    assert set(HBU_STATUS_SAMPLES) <= set(hbu.HBU_STATUSES)


HBU_STATUS_SAMPLES = (
    "solved",
    "no_residential_column",
    "no_governing_column",
    "infeasible",
    "solver_error",
)


# --------------------------------------------------------------------------
# comparing
# --------------------------------------------------------------------------


def gap_of(economics, *rows, existing=None):
    frame = envelopes(*rows)
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    existing = comparables_frame() if existing is None else existing
    return hbu.use_gap(
        chosen,
        existing,
        operating_expense_ratio=hbu.operating_expense_ratio_of(existing),
    )


def test_gap_annualises_the_program(economics):
    """A monthly objective against an annual roll is the first way to be wrong."""
    row = gap_of(economics).iloc[0]
    assert row["hbu_annual_gross_income_cad"] == pytest.approx(
        row["monthly_gross_revenue_cad"] * MONTHS_PER_YEAR
    )


def test_gap_nets_both_sides_the_same_way(economics):
    """One definition of NOI, stated here, applied to both sides.

    The solver's own objective nets the amortised build and no operating
    expense; the roll's nets an expense ratio and no build. Neither is wrong and
    they are not subtractable, so the stabilised pair is what the gap is taken
    on and the development margin is carried separately.
    """
    row = gap_of(economics).iloc[0]
    assert row["operating_expense_ratio"] == 0.35
    assert row["hbu_annual_stabilised_noi_cad"] == pytest.approx(
        row["hbu_annual_gross_income_cad"] * 0.65
    )
    assert row["existing_annual_stabilised_noi_cad"] == 32_500.0
    assert row["annual_stabilised_noi_gap_cad"] == pytest.approx(
        row["hbu_annual_stabilised_noi_cad"] - 32_500.0
    )
    # The solver's own number is kept, under a name that says what it nets.
    assert row["hbu_annual_noi_after_construction_cad"] == pytest.approx(
        row["annual_net_operating_income_cad"]
    )
    assert row["hbu_total_capital_cost_cad"] > 0


def test_the_expense_ratio_is_the_comparables_own(economics):
    """Read off the upstream's rows, so the two sides cannot be netted twice over."""
    existing = comparables_frame(
        income_assumptions=json.dumps({"operating_expense_ratio": 0.5})
    )
    assert hbu.operating_expense_ratio_of(existing) == 0.5
    row = gap_of(economics, existing=existing).iloc[0]
    assert row["hbu_annual_stabilised_noi_cad"] == pytest.approx(
        row["hbu_annual_gross_income_cad"] * 0.5
    )


def test_the_expense_ratio_falls_back_to_the_published_default():
    assert hbu.operating_expense_ratio_of(pd.DataFrame()) == 0.35


def test_the_proposal_is_charged_the_new_build_ratio_not_the_standing_ones(economics):
    """The asymmetry the maintenance premium exists to create.

    The standing building has already been charged for its age upstream; the
    building this module proposes has no age yet. Netting the proposal at the
    old building's ratio would make a new tower pay for a century-old
    triplex's roof, and it is the error that quietly makes redevelopment look
    not worth doing.
    """
    existing = comparables_frame(
        # A 1920 walk-up, as `comparables` would have written it: the base is
        # what a new building costs, the effective is what this one does.
        income_assumptions=json.dumps({"operating_expense_ratio": 0.35}),
        building_age_years=106.0,
        maintenance_premium=0.10,
        effective_operating_expense_ratio=0.45,
        net_operating_income_cad=27_500.0,
    )
    row = gap_of(economics, existing=existing).iloc[0]

    # The proposal pays the base and no premium.
    assert row["hbu_operating_expense_ratio"] == 0.35
    assert row["hbu_annual_stabilised_noi_cad"] == pytest.approx(
        row["hbu_annual_gross_income_cad"] * 0.65
    )
    # The standing building's own ratio travels beside it, so a reader sees
    # that the two NOIs were netted differently and by how much.
    assert row["existing_effective_operating_expense_ratio"] == 0.45
    assert row["existing_building_age_years"] == 106.0
    assert row["existing_annual_stabilised_noi_cad"] == 27_500.0


def test_an_older_building_widens_the_gap_the_redevelopment_closes(economics):
    """Same rent, same envelope, older building - a larger case for building."""
    new_ish = comparables_frame(
        maintenance_premium=0.0,
        effective_operating_expense_ratio=0.35,
        net_operating_income_cad=32_500.0,
    )
    ageing = comparables_frame(
        maintenance_premium=0.10,
        effective_operating_expense_ratio=0.45,
        net_operating_income_cad=27_500.0,
    )

    tighter = gap_of(economics, existing=new_ish).iloc[0]
    wider = gap_of(economics, existing=ageing).iloc[0]

    assert (
        wider["annual_stabilised_noi_gap_cad"]
        > tighter["annual_stabilised_noi_gap_cad"]
    )


def test_the_maintenance_penalty_prices_the_age_premium_on_its_own(economics):
    """What the standing building loses to age alone, in dollars a year."""
    existing = comparables_frame(
        gross_income_cad=50_000.0,
        maintenance_premium=0.10,
        effective_operating_expense_ratio=0.45,
    )
    row = gap_of(economics, existing=existing).iloc[0]

    assert row["existing_maintenance_penalty_cad"] == pytest.approx(5_000.0)


def test_a_building_the_curve_found_new_is_penalised_nothing(economics):
    existing = comparables_frame(
        gross_income_cad=50_000.0,
        maintenance_premium=0.0,
        effective_operating_expense_ratio=0.35,
    )
    row = gap_of(economics, existing=existing).iloc[0]

    assert row["existing_maintenance_penalty_cad"] == 0.0


def test_gap_is_per_class_in_both_units(economics):
    """The question this asset was asked: square feet per residential and
    commercial class, and the NOI beside them."""
    row = gap_of(economics).iloc[0]
    assert row["existing_residential_floor_area_m2"] == 300.0
    assert row["residential_floor_area_gap_m2"] == pytest.approx(
        row["hbu_residential_floor_area_m2"] - 300.0
    )
    for class_name in ("residential", "commercial", "industrial"):
        assert row[f"{class_name}_floor_area_gap_sqft"] == pytest.approx(
            row[f"{class_name}_floor_area_gap_m2"] / M2_PER_SQFT
        )
    assert row["floor_area_gap_m2"] == pytest.approx(
        sum(
            row[f"{class_name}_floor_area_gap_m2"]
            for class_name in ("residential", "commercial", "industrial")
        )
    )
    assert row["dwelling_gap"] == row["hbu_num_dwellings"] - 3


def test_a_lot_the_roll_never_reached_gets_nulls_and_not_zeros(economics):
    """A lane is not a lot with no floor on it - except for `is_underbuilt`."""
    row = gap_of(
        economics, envelope(lot_number="9 999 999"), existing=comparables_frame()
    ).iloc[0]
    assert not row["has_assessment"]
    assert pd.isna(row["existing_residential_floor_area_m2"])
    assert pd.isna(row["residential_floor_area_gap_m2"])
    assert pd.isna(row["existing_annual_stabilised_noi_cad"])
    # The one deliberate exception: a parcel with an envelope and nothing
    # assessed on it is exactly what this column is for.
    assert row["is_underbuilt"]


def test_a_lot_with_no_program_is_not_underbuilt(economics):
    """Unanswered is not the same as built out, and neither is it under-built."""
    row = gap_of(economics, envelope(governs_residential=False)).iloc[0]
    assert row["hbu_status"] == "no_governing_column"
    assert not row["is_underbuilt"]
    assert pd.isna(row["hbu_floor_area_m2"])


# --------------------------------------------------------------------------
# the assets
# --------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture(autouse=True)
def stub_publish(monkeypatch):
    """The upsert into the three tables, recorded rather than run."""
    return stub_publish_into(monkeypatch, hbu_assets)


def write_upstreams(store, *, rows=None, setbacks=True, comparables=None):
    """Every parquet partition the three assets read."""
    write_frame(
        pd.DataFrame(rows or [ENVELOPE]),
        join(
            store.partition_dir(
                lot_zoning_envelopes.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_ENVELOPES_FILE,
        ),
    )
    write_frame(
        rent_frame(),
        join(
            store.partition_dir(average_rents.key.path[-1], DATE, NEIGHBORHOOD),
            AVERAGE_RENTS_FILE,
        ),
    )
    write_frame(
        vacancy_frame(),
        join(
            store.partition_dir(vacancy_rates.key.path[-1], DATE, NEIGHBORHOOD),
            VACANCY_FILE,
        ),
    )
    if setbacks:
        write_frame(
            pd.DataFrame(
                [
                    {
                        "lot_uid": row["lot_uid"],
                        "feature_id": row["feature_id"],
                        "column_index": row["column_index"],
                        "buildable_area_m2": 240.0,
                    }
                    for row in (rows or [ENVELOPE])
                ]
            ),
            join(
                store.partition_dir(
                    lot_buildable_setbacks.key.path[-1], DATE, NEIGHBORHOOD
                ),
                LOT_SETBACKS_FILE,
            ),
        )
    write_frame(
        comparables_frame() if comparables is None else comparables,
        join(
            store.partition_dir(
                lot_assessment_comparables.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_COMPARABLES_FILE,
        ),
    )


def run(store, asset_def, run_config=None):
    return materialize(
        [asset_def],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[asset_def],
        run_config=run_config,
    )


def test_programs_asset_writes_and_publishes(store, stub_publish):
    write_upstreams(store)
    result = run(store, lot_development_programs)
    assert result.success

    frame = pd.read_parquet(
        join(
            store.partition_dir(
                lot_development_programs.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_PROGRAMS_FILE,
        )
    )
    assert len(frame) == 1
    assert frame.iloc[0]["solved"]
    # The margins reached the solve: 240 m2 is under the 350 the coverage allows.
    assert frame.iloc[0]["buildable_area_m2"] == 240.0
    assert frame.iloc[0]["footprint_m2"] <= 240.0
    assert json.loads(frame.iloc[0]["program_assumptions"])["stalls_per_dwelling"] == 0.5

    assert stub_publish["datasets"].keys() == {"lot_development_programs"}
    assert stub_publish["partition"] == (NEIGHBORHOOD, DATE)

    metadata = materialization_metadata(result, lot_development_programs)
    assert metadata["num_candidates"].value == 1
    assert metadata["num_solved"].value == 1
    assert metadata["num_with_buildable_area"].value == 1


def test_programs_asset_without_setbacks_warns_and_still_solves(store):
    """That asset has no schedule; a partition without it is ordinary."""
    write_upstreams(store, setbacks=False)
    result = run(store, lot_development_programs)
    assert result.success
    metadata = materialization_metadata(result, lot_development_programs)
    assert metadata["num_without_buildable_area"].value == 1
    frame = pd.read_parquet(
        join(
            store.partition_dir(
                lot_development_programs.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_PROGRAMS_FILE,
        )
    )
    # Capped on Taux d'implantation alone: 70% of 500.
    assert frame.iloc[0]["footprint_m2"] <= 350.0
    assert frame.iloc[0]["footprint_m2"] > 240.0


def test_programs_asset_config_reaches_the_solver(store):
    """The building is config; a stall nobody has to build changes the answer."""
    write_upstreams(store)
    run(store, lot_development_programs)
    priced = pd.read_parquet(
        join(
            store.partition_dir(
                lot_development_programs.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_PROGRAMS_FILE,
        )
    ).iloc[0]

    run(
        store,
        lot_development_programs,
        run_config={
            "ops": {
                "silver__lot_development_programs": {
                    "config": {"stalls_per_dwelling": 0.0, "stalls_per_1000_sqft": 0.0}
                }
            }
        },
    )
    free = pd.read_parquet(
        join(
            store.partition_dir(
                lot_development_programs.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_PROGRAMS_FILE,
        )
    ).iloc[0]
    assert free["total_stalls"] == 0
    assert free["parking_cost_cad"] == 0.0
    assert (
        free["monthly_net_operating_income_cad"]
        > priced["monthly_net_operating_income_cad"]
    )
    assert json.loads(free["program_assumptions"])["stalls_per_dwelling"] == 0.0


def test_programs_asset_fails_naming_a_missing_upstream(store):
    with pytest.raises(Failure, match="materialize lot_zoning_envelopes"):
        run(store, lot_development_programs)


def test_hbu_asset_answers_every_lot(store, stub_publish):
    rows = [
        envelope(lot_uid=1, lot_number="2 216 001"),
        envelope(lot_uid=2, lot_number="2 216 002", governs_residential=False),
        envelope(
            lot_uid=3,
            lot_number="2 216 003",
            usages=json.dumps(["C.2"]),
            permits_residential=False,
            governs_residential=False,
        ),
    ]
    write_upstreams(store, rows=rows)
    assert run(store, lot_development_programs).success
    result = run(store, lot_highest_best_use)
    assert result.success

    frame = pd.read_parquet(
        join(
            store.partition_dir(lot_highest_best_use.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_HBU_FILE,
        )
    )
    assert len(frame) == 3
    assert dict(zip(frame["lot_uid"], frame["hbu_status"])) == {
        1: "solved",
        2: "no_governing_column",
        3: "no_residential_column",
    }
    metadata = materialization_metadata(result, lot_highest_best_use)
    assert metadata["num_lots"].value == 3
    assert metadata["num_answered"].value == 1
    assert stub_publish["datasets"].keys() == {"lot_highest_best_use"}


def test_gap_asset_puts_the_two_buildings_side_by_side(store, stub_publish):
    write_upstreams(store)
    assert run(store, lot_development_programs).success
    assert run(store, lot_highest_best_use).success
    result = run(store, lot_redevelopment_gap)
    assert result.success

    frame = pd.read_parquet(
        join(
            store.partition_dir(lot_redevelopment_gap.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_GAP_FILE,
        )
    )
    row = frame.iloc[0]
    assert row["has_assessment"]
    assert row["is_underbuilt"]
    assert row["existing_residential_floor_area_m2"] == 300.0
    assert row["residential_floor_area_gap_sqft"] == pytest.approx(
        row["residential_floor_area_gap_m2"] / M2_PER_SQFT
    )
    assert row["annual_stabilised_noi_gap_cad"] == pytest.approx(
        row["hbu_annual_stabilised_noi_cad"] - 32_500.0
    )

    metadata = materialization_metadata(result, lot_redevelopment_gap)
    assert metadata["num_lots"].value == 1
    assert metadata["num_with_assessment"].value == 1
    assert metadata["num_underbuilt"].value == 1
    assert metadata["operating_expense_ratio"].value == 0.35
    assert stub_publish["datasets"].keys() == {"lot_redevelopment_gap"}


def test_gap_asset_keeps_a_lot_the_roll_never_reached(store):
    write_upstreams(store, comparables=comparables_frame(NO_LOT="9 999 999"))
    assert run(store, lot_development_programs).success
    assert run(store, lot_highest_best_use).success
    result = run(store, lot_redevelopment_gap)
    assert result.success
    metadata = materialization_metadata(result, lot_redevelopment_gap)
    assert metadata["num_with_assessment"].value == 0
    assert metadata["num_without_assessment"].value == 1
