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

from urban_rag import hbu, hbu_assets, opportunity_assets
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
from urban_rag.frontage_assets import ROAD_LOTS_FILE, lot_frontage
from urban_rag.hbu_assets import (
    LOT_GAP_FILE,
    LOT_HBU_FILE,
    LOT_PROGRAMS_FILE,
    lot_development_programs,
    lot_highest_best_use,
    lot_redevelopment_gap,
)
from urban_rag.opportunity_assets import (
    LOT_OPPORTUNITIES_FILE,
    lot_investment_opportunities,
)
from urban_rag.program import (
    M2_PER_SQFT,
    MONTHS_PER_YEAR,
    UNDISCOUNTED_INVESTMENT,
)
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.setback_assets import LOT_SETBACKS_FILE, lot_buildable_setbacks
from urban_rag.storage import join

DATE = "2026-08-01"
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
    "usages": json.dumps(["H", "C.2"]),
    "usage_habitation": "H",
    "usage_commerce": "C.2",
    "permits_residential": True,
    "permits_commercial": True,
    "permits_industrial": False,
    "governs_residential": True,
    "governs_commercial": True,
    "governs_industrial": False,
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


#: The two parcels that are avenue Querbes between Ball and Saint-Roch. Real
#: numbers, from `bronze/neighborhood_lots` VSMPE 2026-08-26: two strips of
#: 3,319 m2 and 3,298 m2, each some 9 m wide and a block long, each carrying
#: about 365 m of geobase double street line, and *neither on the assessment
#: roll*. Under the CUBF gate alone they were development sites - the zoning of
#: the blocks either side put an apartment building on each of them, and they
#: reached the redevelopment gap and the investment shortlist from there.
#:
#: Named rather than invented so this is a regression test on the two parcels
#: the failure was reported for; the geometry that actually identifies them is
#: `tests/integration/test_street_parcels.py`.
QUERBES_LOTS = ("2 249 179", "2 249 339")


def road_lots_frame(*lot_numbers, **overrides) -> pd.DataFrame:
    """`lot_frontage`'s `road_lots.parquet` at the columns the gate reads.

    Not marginal by default: 365 m of street line inside a parcel is a block
    of roadway, and `near_cutoff` False is what says the roll gets no vote on
    it. Pass ``near_cutoff=True`` for the ambiguous end of the range.
    """
    return pd.DataFrame(
        [
            {
                "lot_uid": index,
                "lot_number": number,
                "street_m_inside": 365.0,
                "num_street_sides": 16,
                hbu.ROAD_LOT_FLAG_COLUMN: False,
                "neighborhood": NEIGHBORHOOD,
                "scrape_date": DATE,
                **overrides,
            }
            for index, number in enumerate(lot_numbers or QUERBES_LOTS, start=1)
        ]
    )


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
    assert column.usages == ("H", "C.2")
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
    assert row["gross_floor_area_m2"] > 0
    assert row["footprint_m2"] <= 350.0  # 70% of 500
    assert row["gross_floor_area_m2"] <= 1500.0 + 1e-6  # density 3 x 500
    assert json.loads(row["binding"])
    # At the module's $80 retail rent the commerce outearns the housing by a
    # wide margin on this parcel, and nothing in the envelope holds a storey
    # back for the dwellings, so it takes the whole building. That the *mix*
    # is what was solved for rather than chosen in advance is what
    # `test_retail_at_grade_carries_housing_above` pins.
    assert row["commercial_area_m2"] > 0


def test_solve_envelopes_solves_a_pure_commerce_column(economics):
    """A Commerce column is a candidate now - the solver prices all three
    families - and its program is commercial floor and no dwellings."""
    programs = hbu.solve_envelopes(
        envelopes(
            envelope(usages=json.dumps(["C.2"]), permits_residential=False),
            envelope(lot_uid=2, column_index=1),
        ),
        economics,
    )
    assert sorted(programs["lot_uid"]) == [1, 2]
    commerce = programs[programs["lot_uid"] == 1].iloc[0]
    assert commerce["solved"]
    assert commerce["num_dwellings"] == 0
    assert commerce["commercial_area_m2"] > 0


def test_solve_envelopes_drops_a_column_that_authorises_no_priced_use(economics):
    """An Equipements collectifs column is not a candidate, and not a failure:
    a school is not something a proforma rents by the square foot."""
    programs = hbu.solve_envelopes(
        envelopes(
            envelope(
                usages=json.dumps(["E.1"]),
                permits_residential=False,
                permits_commercial=False,
            ),
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
    residential = {"usages": json.dumps(["H"]), "permits_commercial": False}
    # The undiscounted stance, so what stops the last dwelling is the envelope
    # and not the price of its stall - under the default discounting a stall
    # can cost more than the dwelling that owes it earns, and `binding` then
    # rightly reports no printed norm at all.
    assumptions = hbu.ProgramAssumptions(investment=UNDISCOUNTED_INVESTMENT)
    without = hbu.solve_envelopes(
        envelopes(envelope(**residential)), economics, assumptions=assumptions
    ).iloc[0]
    with_margins = hbu.solve_envelopes(
        envelopes(envelope(buildable_area_m2=120.0, **residential)),
        economics,
        assumptions=assumptions,
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


def test_the_floor_stack_travels_as_json_beside_the_counts(economics):
    """The storeys re-cut by level, on the row that already counts them."""
    row = hbu.solve_envelopes(envelopes(), economics).iloc[0]
    stack = json.loads(row["floor_stack"])
    above = [entry for entry in stack if entry["position"] == "above_grade"]
    below = [entry for entry in stack if entry["position"] == "below_grade"]
    assert sum(entry["floors"] for entry in above) == row["floors"]
    assert sum(entry["floors"] for entry in below) == row["underground_levels"]
    assert sum(entry["floor_area_m2"] for entry in above) == pytest.approx(
        row["gross_floor_area_m2"], abs=0.05
    )
    # The stack is storeys, and a surface stall stands in none - it is on the
    # yard the footprint leaves. Everything parked *in* the building does
    # reconcile, garage bays included: they ride on the residential run because
    # they are floor area inside it rather than a storey of their own.
    assert sum(entry["stalls"] for entry in stack) == (
        row["underground_stalls"] + row["above_grade_stalls"] + row["garage_stalls"]
    )
    assert row["total_stalls"] == (
        sum(entry["stalls"] for entry in stack) + row["surface_stalls"]
    )
    assert sum(entry["dwellings"] for entry in stack) == row["num_dwellings"]


def test_the_chosen_lot_keeps_the_stack_of_the_envelope_that_won(economics):
    """`lot_highest_best_use` carries it through the choice, unchanged."""
    frame = envelopes()
    programs = hbu.solve_envelopes(frame, economics)
    chosen = hbu.select_highest_best_use(programs, frame).iloc[0]
    assert "floor_stack" in hbu.HBU_COLUMNS
    assert chosen["floor_stack"] == programs.iloc[0]["floor_stack"]
    assert json.loads(chosen["floor_stack"])


# --------------------------------------------------------------------------
# choosing
# --------------------------------------------------------------------------


def test_the_governing_column_wins_even_when_it_earns_less(economics):
    """The grid picks each family's column, not the income - see the module
    docstring. The richer column of the *same* families governs neither, so
    the envelope is built without it and the parcel is held to three storeys
    rather than the six the richer column would have allowed."""
    frame = envelopes(
        envelope(column_index=0, governs_residential=True, floors_max=3),
        envelope(
            column_index=1,
            governs_residential=False,
            governs_commercial=False,
            floors_max=6,
        ),
    )
    programs = hbu.solve_envelopes(frame, economics)
    # One row per zone now, not one per column: the columns are a single
    # envelope and the non-governing one contributes nothing to it.
    assert len(programs) == 1
    row = programs.iloc[0]
    assert row["column_index"] == 0
    assert row["floors"] == 3

    chosen = hbu.select_highest_best_use(programs, frame)
    assert len(chosen) == 1
    row = chosen.iloc[0]
    assert row["column_index"] == 0
    assert row["hbu_status"] == "solved"
    assert row["num_candidates"] == 2
    assert row["num_governing_candidates"] == 1


def test_an_h_column_and_a_c_column_are_one_building(economics):
    """A zone writing an H column and a C column beside it authorises *both*
    in one building, and that is a mix rather than a choice between two pure
    programs. The two columns become one envelope and one solve, and the floor
    area between the families is what the model decides."""
    frame = envelopes(
        envelope(
            column_index=0,
            usages=json.dumps(["H"]),
            permits_commercial=False,
            governs_commercial=False,
        ),
        envelope(
            column_index=1,
            usages=json.dumps(["C.2"]),
            permits_residential=False,
            governs_residential=False,
        ),
    )
    programs = hbu.solve_envelopes(frame, economics)
    assert len(programs) == 1
    row = programs.iloc[0]
    # Both families reached the model: the row governs each of them, and its
    # usages are the two columns' put together.
    assert row["governs_residential"] and row["governs_commercial"]
    assert set(json.loads(row["usages"])) == {"H", "C.2"}

    # And the answer is the same one the single mixed column of `ENVELOPE`
    # gives, which is the point: whether the zone prints its two usages in one
    # column or in two is a fact about the PDF, not about what may be built.
    together = hbu.solve_envelopes(envelopes(), economics).iloc[0]
    assert row["npv_cad"] == pytest.approx(float(together["npv_cad"]))
    assert row["floors"] == together["floors"]

    chosen = hbu.select_highest_best_use(programs, frame)
    assert chosen.iloc[0]["hbu_status"] == "solved"


def test_retail_at_grade_carries_housing_above(economics):
    """The building the old per-column solve could not propose at all.

    The C column is marked *Rez-de-chaussee* and the H column *Tous les
    niveaux*, so the commerce may take one storey and the housing may take
    any. Solved apart, the answer was the better of "six storeys of housing"
    and "one storey of retail"; solved together it is the one the zone
    actually describes - retail at grade with dwellings over it.
    """
    frame = envelopes(
        envelope(
            column_index=0,
            usages=json.dumps(["H"]),
            levels=json.dumps(["tous_les_niveaux"]),
            permits_commercial=False,
            governs_commercial=False,
        ),
        envelope(
            column_index=1,
            usages=json.dumps(["C.2"]),
            levels=json.dumps(["rez_de_chaussee"]),
            permits_residential=False,
            governs_residential=False,
        ),
    )
    row = hbu.solve_envelopes(frame, economics).iloc[0]
    assert row["solved"]
    # The level rows are read per column, so the commerce is held to the one
    # storey its own column allows while the housing fills what is left.
    assert row["commercial_floors"] == 1
    assert row["residential_floors"] > 0
    assert row["num_dwellings"] > 0
    assert row["floors"] == row["commercial_floors"] + row["residential_floors"]


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


def test_a_lot_with_no_priced_use_keeps_its_row(economics):
    """A pure Equipements collectifs zone has no proforma and says which."""
    frame = envelopes(
        envelope(
            usages=json.dumps(["E.1"]),
            permits_residential=False,
            permits_commercial=False,
            governs_residential=False,
            governs_commercial=False,
        )
    )
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    row = chosen.iloc[0]
    assert row["hbu_status"] == "equipment_zone"
    assert row["num_candidates"] == 0
    assert pd.isna(row["status"])


def test_a_column_with_no_usage_at_all_is_not_an_equipment_zone(economics):
    """The distinction `equipment_zone` exists to draw: a park is a use this
    module will not price, an unreadable usage row is a grid it could not
    read, and the two are different things to tell a reader."""
    frame = envelopes(
        envelope(
            usages=json.dumps([]),
            permits_residential=False,
            permits_commercial=False,
            governs_residential=False,
            governs_commercial=False,
        )
    )
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    assert chosen.iloc[0]["hbu_status"] == "no_candidate_column"


def test_an_equipment_parcel_is_not_answered_by_the_zone_next_door(economics):
    """Parc Jarry, in miniature: a parcel almost wholly inside an Equipements
    zone clips a residential one, and used to be reported as a development
    site on the strength of the sliver."""
    frame = envelopes(
        envelope(
            feature_id="E04-019",
            pct_of_lot=98.3,
            usages=json.dumps(["E.1"]),
            permits_residential=False,
            permits_commercial=False,
            governs_residential=False,
            governs_commercial=False,
        ),
        envelope(feature_id="H02-002", pct_of_lot=1.7, column_index=1),
    )
    programs = hbu.solve_envelopes(frame, economics)
    assert programs.empty
    row = hbu.select_highest_best_use(programs, frame).iloc[0]
    assert row["hbu_status"] == "equipment_zone"
    assert row["num_candidates"] == 0
    # The other zone is still on the record - it covers part of the parcel and
    # the table says so - it just does not get to answer for it.
    assert row["num_zones"] == 2


def test_the_governing_zone_still_answers_where_it_has_a_program(economics):
    """The gate is about which zone speaks, not about excluding slivers: a lot
    whose dominant zone permits housing is answered by that zone as before."""
    frame = envelopes(
        envelope(feature_id="H02-002", pct_of_lot=97.0, floors_max=2),
        envelope(feature_id="C01-001", pct_of_lot=3.0, column_index=1, floors_max=6),
    )
    row = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame).iloc[0]
    assert row["hbu_status"] == "solved"
    assert row["feature_id"] == "H02-002"


def test_a_pure_commerce_zone_is_solved_not_skipped(economics):
    """The case the memory of this platform used to call no_residential_column."""
    frame = envelopes(
        envelope(
            usages=json.dumps(["C.2"]),
            permits_residential=False,
            governs_residential=False,
        )
    )
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    row = chosen.iloc[0]
    assert row["hbu_status"] == "solved"
    assert row["hbu_dominant_use"] == "commercial"
    assert row["num_dwellings"] == 0
    assert row["commercial_area_m2"] > 0


def test_a_lot_with_no_governing_column_says_so(economics):
    """A parcel too narrow for every column of its own grid."""
    frame = envelopes(
        envelope(governs_residential=False, governs_commercial=False)
    )
    chosen = hbu.select_highest_best_use(hbu.solve_envelopes(frame, economics), frame)
    assert chosen.iloc[0]["hbu_status"] == "no_governing_column"


def test_a_road_parcel_loses_the_program_its_zoning_would_have_allowed(economics):
    """A lane inside a residential zone. The grid permits a building on the
    block; the roll says this particular parcel is the roadway."""
    frame = envelopes()
    programs = hbu.solve_envelopes(frame, economics)
    # The zoning alone would have built here, which is the point.
    assert programs.iloc[0]["solved"]

    chosen = hbu.select_highest_best_use(
        programs,
        frame,
        assessments=comparables_frame(dominant_use_code="4550"),
    )
    row = chosen.iloc[0]
    assert row["hbu_status"] == "road_parcel"
    assert pd.isna(row["status"])
    assert pd.isna(row["num_dwellings"])
    # The lot keeps its row and its own facts - this is an inventory, and a
    # street is a thing the borough has.
    assert row["lot_area_m2"] == 500.0


def test_the_roll_overrules_the_zoning_only_for_the_road_codes(economics):
    """4550 is a street; 4551 would be too; 1000 is a triplex and builds."""
    frame = envelopes()
    programs = hbu.solve_envelopes(frame, economics)
    for code, expected in (("4550", "road_parcel"), ("4599", "road_parcel"),
                           ("4500", "road_parcel"), ("1000", "solved"),
                           ("4611", "solved"), ("4000", "solved")):
        chosen = hbu.select_highest_best_use(
            programs, frame, assessments=comparables_frame(dominant_use_code=code)
        )
        assert chosen.iloc[0]["hbu_status"] == expected, code


def test_a_lot_the_roll_never_reached_is_not_a_road(economics):
    """No assessment unit is not evidence of anything. The 3,090 VSMPE lots
    the roll never reached keep whatever their zoning says."""
    frame = envelopes()
    programs = hbu.solve_envelopes(frame, economics)
    chosen = hbu.select_highest_best_use(
        programs, frame, assessments=comparables_frame(NO_LOT="9 999 999")
    )
    assert chosen.iloc[0]["hbu_status"] == "solved"


def test_a_street_parcel_the_roll_never_reached_is_still_a_road(economics):
    """The failure this gate was added for.

    Lot 2 249 179 *is* avenue Querbes. Montreal never entered it on the roll,
    so `road_parcel_lots` cannot see it and the zoning of the blocks either
    side of it - which is what the zone polygon over a roadway describes -
    built an apartment block on the street.
    """
    frame = envelopes(envelope(lot_number=QUERBES_LOTS[0]))
    programs = hbu.solve_envelopes(frame, economics)
    # The zoning alone builds here, which is exactly the problem.
    assert programs.iloc[0]["solved"]

    chosen = hbu.select_highest_best_use(
        programs,
        frame,
        # The roll reaches this parcel with nothing to say about it, which is
        # the state the borough is actually in.
        assessments=comparables_frame(NO_LOT=QUERBES_LOTS[0], dominant_use_code=None),
        road_lots=road_lots_frame(),
    )
    row = chosen.iloc[0]
    assert row["hbu_status"] == "road_parcel"
    assert pd.isna(row["status"])
    assert pd.isna(row["num_dwellings"])
    assert pd.isna(row["npv_cad"])
    # It keeps its row and its own facts. The borough has a street here, and
    # this table is an inventory of the borough.
    assert row["lot_number"] == QUERBES_LOTS[0]
    assert row["lot_area_m2"] == 500.0


def test_the_two_road_predicates_are_unioned_and_neither_contains_the_other(
    economics,
):
    """Each reaches parcels the other cannot, so both have to be consulted.

    The roll knows a right of way it assessed; the cadastre knows every street
    Montreal never put on the roll. A lot either of them calls a street is not
    a development site.
    """
    frame = envelopes(
        envelope(lot_uid=1, lot_number="2 216 001"),
        # On the roll as a road, and not a parcel the street network reaches -
        # a right of way the city did assess.
        envelope(lot_uid=2, lot_number="2 216 002"),
        # The other way round: avenue Querbes, which the roll never saw.
        envelope(lot_uid=3, lot_number=QUERBES_LOTS[0]),
    )
    programs = hbu.solve_envelopes(frame, economics)
    chosen = hbu.select_highest_best_use(
        programs,
        frame,
        assessments=comparables_frame(NO_LOT="2 216 002", dominant_use_code="4550"),
        road_lots=road_lots_frame(QUERBES_LOTS[0]),
    )
    assert dict(zip(chosen["lot_uid"], chosen["hbu_status"])) == {
        1: "solved",
        2: "road_parcel",
        3: "road_parcel",
    }


def test_a_parcel_the_cadastre_does_not_call_a_road_still_builds(economics):
    """The gate is a membership test, not a blanket over the partition."""
    frame = envelopes(envelope(lot_number="2 216 001"))
    programs = hbu.solve_envelopes(frame, economics)
    chosen = hbu.select_highest_best_use(
        programs, frame, road_lots=road_lots_frame()
    )
    assert chosen.iloc[0]["hbu_status"] == "solved"


def test_no_road_lots_at_all_leaves_the_answer_as_it_was(economics):
    """`road_lots` is optional, the way `assessments` is: a partition where
    `lot_frontage` has not run answers as it did before rather than failing."""
    frame = envelopes(envelope(lot_number=QUERBES_LOTS[0]))
    programs = hbu.solve_envelopes(frame, economics)
    for road_lots in (None, pd.DataFrame(), road_lots_frame("9 999 999")):
        chosen = hbu.select_highest_best_use(programs, frame, road_lots=road_lots)
        assert chosen.iloc[0]["hbu_status"] == "solved"


def test_the_roll_overturns_a_marginal_call_but_not_a_clear_one(economics):
    """The roll's veto, and the two halves of why it is this narrow.

    A parcel caught by 1.5 m of street line is the one case the geometry
    cannot read - a short stub of roadway and a geobase side clipping a corner
    look identical there - so a roll saying *Logement* is better evidence and
    wins. A parcel with 365 m of street line down the inside of it is the
    street whatever the roll calls it, and VSMPE has ten such parcels coded
    *Logement*, so there the roll gets no vote at all.
    """
    frame = envelopes(
        envelope(lot_uid=1, lot_number="2 216 001"),
        envelope(lot_uid=2, lot_number=QUERBES_LOTS[0]),
    )
    programs = hbu.solve_envelopes(frame, economics)
    assessments = pd.concat(
        [
            comparables_frame(NO_LOT="2 216 001", dominant_use_code="1000"),
            comparables_frame(NO_LOT=QUERBES_LOTS[0], dominant_use_code="1000"),
        ],
        ignore_index=True,
    )
    road_lots = pd.concat(
        [
            # A corner clip: barely over the cutoff, and assessed as housing.
            road_lots_frame(
                "2 216 001", street_m_inside=1.5, **{hbu.ROAD_LOT_FLAG_COLUMN: True}
            ),
            # A block of avenue Querbes, also assessed as housing.
            road_lots_frame(QUERBES_LOTS[0]),
        ],
        ignore_index=True,
    )
    chosen = hbu.select_highest_best_use(
        programs, frame, assessments=assessments, road_lots=road_lots
    )
    assert dict(zip(chosen["lot_uid"], chosen["hbu_status"])) == {
        1: "solved",
        2: "road_parcel",
    }


def test_a_lot_the_roll_never_reached_is_not_rescued(economics):
    """The asymmetry the whole design turns on: absence of a use code is not
    evidence of anything.

    Read as a whitelist - keep only what the roll files as non-road - the same
    column would drop the 2,509 VSMPE parcels the roll never reached, of which
    1,245 are not roads and 1,053 had solved programs. So a marginal parcel
    with no code stays a road; only a stated non-road use overturns one.
    """
    frame = envelopes(envelope(lot_number="2 216 001"))
    programs = hbu.solve_envelopes(frame, economics)
    chosen = hbu.select_highest_best_use(
        programs,
        frame,
        assessments=comparables_frame(NO_LOT="2 216 001", dominant_use_code=None),
        road_lots=road_lots_frame(
            "2 216 001", street_m_inside=1.5, **{hbu.ROAD_LOT_FLAG_COLUMN: True}
        ),
    )
    assert chosen.iloc[0]["hbu_status"] == "road_parcel"


def test_a_marginal_parcel_the_roll_calls_a_road_stays_one(economics):
    """Both predicates agreeing is not a case the rescue may reach."""
    frame = envelopes(envelope(lot_number="2 216 001"))
    programs = hbu.solve_envelopes(frame, economics)
    chosen = hbu.select_highest_best_use(
        programs,
        frame,
        assessments=comparables_frame(NO_LOT="2 216 001", dominant_use_code="4550"),
        road_lots=road_lots_frame(
            "2 216 001", street_m_inside=1.5, **{hbu.ROAD_LOT_FLAG_COLUMN: True}
        ),
    )
    assert chosen.iloc[0]["hbu_status"] == "road_parcel"


def test_road_lots_written_before_the_flag_existed_keep_their_answer(economics):
    """A parquet with no `near_cutoff` column is read as nothing being
    marginal, so the roll gets no vote and the partition answers as it did."""
    frame = envelopes(envelope(lot_number=QUERBES_LOTS[0]))
    programs = hbu.solve_envelopes(frame, economics)
    old_file = road_lots_frame(QUERBES_LOTS[0]).drop(
        columns=[hbu.ROAD_LOT_FLAG_COLUMN]
    )
    chosen = hbu.select_highest_best_use(
        programs,
        frame,
        assessments=comparables_frame(
            NO_LOT=QUERBES_LOTS[0], dominant_use_code="1000"
        ),
        road_lots=old_file,
    )
    assert chosen.iloc[0]["hbu_status"] == "road_parcel"


def test_no_roll_at_all_leaves_the_answer_as_it_was(economics):
    """`assessments` is optional, and omitting it is not the same as an empty
    one - both leave the zoning to answer alone."""
    frame = envelopes()
    programs = hbu.solve_envelopes(frame, economics)
    without = hbu.select_highest_best_use(programs, frame)
    empty = hbu.select_highest_best_use(
        programs, frame, assessments=pd.DataFrame()
    )
    assert without.iloc[0]["hbu_status"] == "solved"
    assert empty.iloc[0]["hbu_status"] == "solved"


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
    "no_candidate_column",
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
    row = gap_of(
        economics,
        envelope(governs_residential=False, governs_commercial=False),
    ).iloc[0]
    assert row["hbu_status"] == "no_governing_column"
    assert not row["is_underbuilt"]
    assert pd.isna(row["hbu_floor_area_m2"])
    # And the verdict columns stay null with it: no programme, no npv, and a
    # gain of nothing rather than a gain of zero.
    assert pd.isna(row["hbu_npv_cad"])
    assert pd.isna(row["redevelopment_npv_gain_cad"])


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


def write_upstreams(
    store, *, rows=None, setbacks=True, comparables=None, road_lots=None
):
    """Every parquet partition the three assets read.

    ``road_lots`` defaults to an empty `road_lots.parquet` rather than to no
    file at all, because that is the shape of a real partition: `lot_frontage`
    writes the file whether or not the borough has a roadway in it, and a test
    that omitted it would be exercising the degraded path by accident.
    """
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
    if road_lots is not False:
        write_frame(
            road_lots_frame().iloc[:0] if road_lots is None else road_lots,
            join(
                store.partition_dir(lot_frontage.key.path[-1], DATE, NEIGHBORHOOD),
                ROAD_LOTS_FILE,
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
        envelope(
            lot_uid=2,
            lot_number="2 216 002",
            governs_residential=False,
            governs_commercial=False,
        ),
        # A pure Commerce zone - solved now, where it used to be the
        # no_residential_column gap.
        envelope(
            lot_uid=3,
            lot_number="2 216 003",
            usages=json.dumps(["C.2"]),
            permits_residential=False,
            governs_residential=False,
        ),
        # A pure Equipements zone - a park, a school, a cemetery.
        envelope(
            lot_uid=4,
            lot_number="2 216 004",
            usages=json.dumps(["E.1"]),
            permits_residential=False,
            permits_commercial=False,
            governs_residential=False,
            governs_commercial=False,
        ),
        # A lane under the same residential grid as lot 1, which the roll
        # files as roadway below.
        envelope(lot_uid=5, lot_number="2 216 005"),
    ]
    write_upstreams(
        store,
        rows=rows,
        comparables=pd.concat(
            [
                comparables_frame(),
                comparables_frame(NO_LOT="2 216 005", dominant_use_code="4550"),
            ],
            ignore_index=True,
        ),
    )
    assert run(store, lot_development_programs).success
    result = run(store, lot_highest_best_use)
    assert result.success

    frame = pd.read_parquet(
        join(
            store.partition_dir(lot_highest_best_use.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_HBU_FILE,
        )
    )
    assert len(frame) == 5
    assert dict(zip(frame["lot_uid"], frame["hbu_status"])) == {
        1: "solved",
        2: "no_governing_column",
        3: "solved",
        4: "equipment_zone",
        5: "road_parcel",
    }
    by_lot = frame.set_index("lot_uid")
    assert by_lot.loc[3, "hbu_dominant_use"] == "commercial"
    assert by_lot.loc[3, "num_dwellings"] == 0
    metadata = materialization_metadata(result, lot_highest_best_use)
    assert metadata["num_lots"].value == 5
    assert metadata["num_answered"].value == 2
    assert metadata["num_equipment_zone"].value == 1
    assert metadata["num_road_parcel"].value == 1
    # The count worth watching: lot 5 solved and was then not chosen, which is
    # the answer this gate actually took away.
    assert metadata["num_road_programs_withheld"].value == 1
    assert stub_publish["datasets"].keys() == {"lot_highest_best_use"}


def test_hbu_asset_gates_the_street_parcels_the_roll_never_reached(
    store, stub_publish
):
    """Avenue Querbes, end to end, at the grain the complaint was made in.

    Lots 2 249 179 and 2 249 339 are the roadway. Nothing in the assessment
    roll says so - Montreal does not enter its streets on it - so before the
    cadastral predicate the zoning of the blocks either side answered for them
    and the borough's HBU table proposed a building on each.
    """
    rows = [
        envelope(lot_uid=1, lot_number="2 216 001"),
        envelope(lot_uid=2, lot_number=QUERBES_LOTS[0]),
        envelope(lot_uid=3, lot_number=QUERBES_LOTS[1]),
    ]
    write_upstreams(
        store,
        rows=rows,
        # The roll reaches the ordinary parcel and neither street.
        comparables=comparables_frame(),
        road_lots=road_lots_frame(),
    )
    assert run(store, lot_development_programs).success
    result = run(store, lot_highest_best_use)
    assert result.success

    frame = pd.read_parquet(
        join(
            store.partition_dir(lot_highest_best_use.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_HBU_FILE,
        )
    ).set_index("lot_number")
    assert frame.loc["2 216 001", "hbu_status"] == "solved"
    for number in QUERBES_LOTS:
        assert frame.loc[number, "hbu_status"] == "road_parcel", number
        assert pd.isna(frame.loc[number, "num_dwellings"]), number
        assert pd.isna(frame.loc[number, "npv_cad"]), number

    metadata = materialization_metadata(result, lot_highest_best_use)
    assert metadata["num_road_parcel"].value == 2
    # The count that says the gate did work rather than merely labelled: both
    # streets had a solved program, and both lost it.
    assert metadata["num_road_programs_withheld"].value == 2
    # And which predicate found them, kept apart on purpose - the roll found
    # neither, which is the whole reason the second one exists.
    assert metadata["num_road_parcels_on_the_roll"].value == 0
    assert metadata["num_road_parcels_in_the_cadastre"].value == 2


def test_a_cadastral_street_parcel_reaches_neither_the_gap_nor_the_shortlist(
    store, stub_publish, monkeypatch
):
    """The gate is applied once, at the choice, and the rest of gold inherits
    it: no program means no gap, and no gap means no investment thesis."""
    write_upstreams(
        store,
        rows=[envelope(lot_number=QUERBES_LOTS[0])],
        comparables=comparables_frame(NO_LOT=QUERBES_LOTS[0], dominant_use_code=None),
        road_lots=road_lots_frame(QUERBES_LOTS[0]),
    )
    assert run(store, lot_development_programs).success
    assert run(store, lot_highest_best_use).success
    assert run(store, lot_redevelopment_gap).success

    gap = pd.read_parquet(
        join(
            store.partition_dir(lot_redevelopment_gap.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_GAP_FILE,
        )
    ).iloc[0]
    assert gap["hbu_status"] == "road_parcel"
    assert not gap["is_underbuilt"]
    assert pd.isna(gap["hbu_residential_floor_area_m2"])

    # The shortlist is a fourth asset and a fourth upsert, so it needs its own
    # module patched out - `stub_publish` above only covers `hbu_assets`.
    stub_publish_into(monkeypatch, opportunity_assets)
    assert run(store, lot_investment_opportunities).success
    shortlist = pd.read_parquet(
        join(
            store.partition_dir(
                lot_investment_opportunities.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_OPPORTUNITIES_FILE,
        )
    ).iloc[0]
    # It keeps its row - this is an inventory - and it is not an opportunity.
    assert shortlist["lot_number"] == QUERBES_LOTS[0]
    assert shortlist["investment_thesis"] == "none"
    assert pd.isna(shortlist["thesis_rank"])
    assert not shortlist["is_top_opportunity"]


def test_hbu_asset_without_the_road_lots_file_warns_and_falls_back_to_the_roll(
    store, stub_publish
):
    """`lot_frontage` has no schedule and reads a relation hbu_infra creates,
    so a partition without it must answer as it did before rather than fail -
    loudly, because what it costs is a borough of streets read as sites."""
    write_upstreams(
        store, rows=[envelope(lot_number=QUERBES_LOTS[0])], road_lots=False
    )
    assert run(store, lot_development_programs).success
    result = run(store, lot_highest_best_use)
    assert result.success

    frame = pd.read_parquet(
        join(
            store.partition_dir(lot_highest_best_use.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_HBU_FILE,
        )
    )
    assert frame.iloc[0]["hbu_status"] == "solved"
    metadata = materialization_metadata(result, lot_highest_best_use)
    assert metadata["num_road_parcels_in_the_cadastre"].value == 0


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


def test_a_road_parcel_is_never_under_built(store):
    """The gate is applied once, at the choice, and the tables downstream
    inherit it: no program means no gap, and no gap means no shortlist."""
    write_upstreams(store, comparables=comparables_frame(dominant_use_code="4550"))
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
    assert row["hbu_status"] == "road_parcel"
    assert not row["is_underbuilt"]
    assert pd.isna(row["hbu_residential_floor_area_m2"])
    # It keeps its row and the roll's side of it - the borough still has a
    # street here, and the table is an inventory.
    assert row["has_assessment"]
    metadata = materialization_metadata(result, lot_redevelopment_gap)
    assert metadata["num_underbuilt"].value == 0
