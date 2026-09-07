"""The second future - the building stays and grows - and the three side by side.

`program.solve_program` with a `RetainedBuilding` is the enhancement: the
standing plate and storeys are a floor under the model, nothing is dug or
decked, and the money is the addition's own. `hbu.solve_enhancements` runs it
per lot off the gap's own inputs, and `hbu.three_futures` puts hold, enhance
and rebuild on one footing. What these pin: the retained building cannot be
shrunk, the addition pays its premium and its delay, the hold is valued
without one, and the verdict picks the largest with holding on a tie.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from urban_rag import hbu
from urban_rag.hbu import (
    ENHANCEMENT_COLUMNS,
    EnhancementRules,
    ProgramAssumptions,
    program_assumptions_of,
    retained_building_of,
    solve_enhancements,
    three_futures,
)
from urban_rag.program import (
    BuildingLevel,
    ConstructionCosts,
    InvestmentAssumptions,
    Lot,
    ProgramError,
    RetainedBuilding,
    UnitEconomics,
    ZoneColumn,
    solve_program,
)

ECONOMICS = UnitEconomics(
    average_rent_cad={
        "studio": 1000.0, "1_bedroom": 1300.0, "2_bedroom": 1600.0, "3_bedroom_plus": 2000.0,
    },
    vacancy_rate_pct={
        "studio": 2.0, "1_bedroom": 1.5, "2_bedroom": 1.0, "3_bedroom_plus": 0.8,
    },
)
COLUMN = ZoneColumn(
    usages=("H",),
    floors_max=6,
    floors_min=2,
    levels=frozenset({BuildingLevel.ALL}),
    site_coverage_max_pct=70.0,
    density_max=4.5,
)
LOT = Lot(area_m2=300.0, frontage_m=10.0)
PLEX = RetainedBuilding(
    footprint_m2=120.0,
    storeys=2,
    residential_floor_area_m2=240.0,
    dwellings=2,
    monthly_gross_revenue_cad=3000.0,
    max_added_storeys=1,
    addition_cost_premium=1.5,
)


# -- the timing ---------------------------------------------------------------


def test_the_delay_pushes_the_whole_stream_out():
    now = InvestmentAssumptions()
    later = InvestmentAssumptions(construction_months=18, lease_up_months=6)
    assert now.delay_years == 0.0
    assert now.annual_pv_factor == now.hold_pv_factor
    # 18 months plus half of 6: 21 months.
    assert later.delay_years == pytest.approx(21.0 / 12.0)
    assert later.delay_factor == pytest.approx(1.05 ** -(21.0 / 12.0))
    assert later.hold_pv_factor == now.hold_pv_factor
    assert later.annual_pv_factor == pytest.approx(now.hold_pv_factor * later.delay_factor)


def test_negative_months_are_refused():
    with pytest.raises(ProgramError):
        InvestmentAssumptions(construction_months=-1)


def test_a_rebuild_is_worth_less_the_longer_it_takes():
    now = solve_program(COLUMN, LOT, ECONOMICS)
    later = solve_program(
        COLUMN, LOT, ECONOMICS,
        investment=InvestmentAssumptions(construction_months=18, lease_up_months=6),
    )
    assert now.status == later.status == "OPTIMAL"
    assert later.present_value_cad < now.present_value_cad
    assert later.npv_cad < now.npv_cad


# -- the retained building in the model ----------------------------------------


def test_the_addition_keeps_the_plate_and_the_storeys_and_adds_one():
    program = solve_program(COLUMN, LOT, ECONOMICS, retained=PLEX)
    assert program.status == "OPTIMAL"
    assert program.is_enhancement
    assert program.footprint_m2 >= PLEX.footprint_m2
    assert program.floors == PLEX.storeys + 1
    assert program.added_floor_area_m2 == pytest.approx(
        program.gross_floor_area_m2 - PLEX.floor_area_m2
    )
    assert program.added_floor_area_m2 > 0
    # The money is the addition's: dwellings added, retained ones carried apart.
    assert program.total_dwellings > 0
    assert program.retained_dwellings == 2
    assert program.retained_monthly_gross_cad == 3000.0


def test_nothing_is_dug_or_decked_under_or_over_a_standing_building():
    program = solve_program(COLUMN, LOT, ECONOMICS, retained=PLEX)
    assert program.underground_levels == 0
    assert program.above_grade_parking_floors == 0
    assert program.garage_stalls == 0
    assert program.basement_levels == 0
    assert program.total_stalls == program.surface_stalls


def test_the_addition_pays_the_premium():
    """The same addition at a 1.0 premium costs less and is worth more."""
    dear = solve_program(COLUMN, LOT, ECONOMICS, retained=PLEX)
    cheap = solve_program(
        COLUMN, LOT, ECONOMICS,
        retained=RetainedBuilding(
            footprint_m2=120.0, storeys=2, residential_floor_area_m2=240.0,
            dwellings=2, addition_cost_premium=1.0,
        ),
    )
    assert cheap.npv_cad > dear.npv_cad


def test_the_structure_caps_the_storeys_added():
    two = solve_program(
        COLUMN, LOT, ECONOMICS,
        retained=RetainedBuilding(
            footprint_m2=120.0, storeys=2, residential_floor_area_m2=240.0,
            dwellings=2, max_added_storeys=4,
        ),
    )
    assert two.floors <= COLUMN.floors_max
    assert two.floors > PLEX.storeys + 1


def test_a_plate_the_grid_cannot_hold_is_refused_by_name():
    program = solve_program(
        ZoneColumn(
            usages=("H",), floors_max=3, levels=frozenset({BuildingLevel.ALL}),
            site_coverage_max_pct=30.0,
        ),
        LOT,
        ECONOMICS,
        retained=PLEX,
    )
    assert program.status == "INFEASIBLE"
    assert program.binding == ("retained_footprint_exceeds_cap",)


def test_an_addition_nobody_would_pay_for_adds_nothing():
    """At a premium no dwelling clears, the model prices nothing new - and
    must then report the standing building, not a plate it never built."""
    dear = RetainedBuilding(
        footprint_m2=120.0, storeys=2, residential_floor_area_m2=240.0,
        dwellings=2, addition_cost_premium=4.0,
    )
    program = solve_program(COLUMN, LOT, ECONOMICS, retained=dear)
    assert program.status == "OPTIMAL"
    assert program.total_dwellings == 0
    assert program.added_floor_area_m2 == 0.0
    assert program.footprint_m2 == pytest.approx(120.0)
    assert program.floors == 2
    assert program.surface_stalls == 0
    assert program.npv_cad == 0.0


def test_no_headroom_means_nothing_added_rather_than_an_error():
    program = solve_program(
        ZoneColumn(
            usages=("H",), floors_max=2, levels=frozenset({BuildingLevel.ALL}),
            site_coverage_max_pct=40.0,
        ),
        LOT,
        ECONOMICS,
        retained=PLEX,
    )
    assert program.status == "OPTIMAL"
    assert program.added_floor_area_m2 == 0.0
    assert program.npv_cad == 0.0


def test_the_dwelling_ceiling_counts_the_ones_that_stand():
    capped = solve_program(
        ZoneColumn(
            usages=("H",), floors_max=6, levels=frozenset({BuildingLevel.ALL}),
            site_coverage_max_pct=70.0, max_dwellings=3,
        ),
        LOT,
        ECONOMICS,
        retained=PLEX,
    )
    assert capped.total_dwellings <= 1


def test_a_retained_building_needs_a_plate_and_a_storey():
    with pytest.raises(ProgramError):
        RetainedBuilding(footprint_m2=0.0, storeys=2)
    with pytest.raises(ProgramError):
        RetainedBuilding(footprint_m2=100.0, storeys=0)


def test_scaled_costs_multiply_the_rates_and_nothing_else():
    costs = ConstructionCosts().scaled(1.5)
    assert costs.residential_cost_per_sqft == pytest.approx(
        ConstructionCosts().residential_cost_per_sqft * 1.5
    )
    assert costs.below_grade_premium == ConstructionCosts().below_grade_premium
    with pytest.raises(ProgramError):
        ConstructionCosts().scaled(0.0)


# -- the roll's building as the block ----------------------------------------------


def test_the_roll_row_becomes_the_retained_block():
    block = retained_building_of(
        {
            "num_storeys": 3,
            "residential_floor_area_m2": 300.0,
            "commercial_floor_area_m2": 60.0,
            "num_dwellings": 3,
            "gross_income_cad": 48_000.0,
        },
        EnhancementRules(max_added_storeys=2, addition_cost_premium=1.4),
    )
    assert block.footprint_m2 == pytest.approx(120.0)
    assert block.storeys == 3
    assert block.floor_area_m2 == 360.0
    assert block.monthly_gross_revenue_cad == pytest.approx(4000.0)
    assert block.max_added_storeys == 2
    assert block.addition_cost_premium == 1.4


@pytest.mark.parametrize(
    "row",
    [
        {"num_storeys": None, "residential_floor_area_m2": 300.0},
        {"num_storeys": 2, "residential_floor_area_m2": 0.0},
        {"num_storeys": 0, "residential_floor_area_m2": 300.0},
    ],
)
def test_no_building_to_retain_is_none(row):
    assert retained_building_of(row) is None


# -- the assumptions read back --------------------------------------------------------


def test_program_assumptions_round_trip():
    stated = ProgramAssumptions(
        investment=InvestmentAssumptions(
            discount_rate_pct=6.0, construction_months=12, lease_up_months=4
        ),
        construction=ConstructionCosts(residential_cost_per_sqft=300.0),
    )
    hbu_frame = pd.DataFrame({"program_assumptions": [json.dumps(stated.as_metadata())]})
    back = program_assumptions_of(hbu_frame)
    assert back.investment.discount_rate_pct == 6.0
    assert back.investment.construction_months == 12
    assert back.investment.lease_up_months == 4
    assert back.construction.residential_cost_per_sqft == 300.0
    assert back.parking.stalls_per_dwelling == stated.parking.stalls_per_dwelling


def test_an_older_payload_falls_back_field_by_field():
    hbu_frame = pd.DataFrame({"program_assumptions": [json.dumps({"discount_rate_pct": 7.0})]})
    back = program_assumptions_of(hbu_frame)
    assert back.investment.discount_rate_pct == 7.0
    assert back.investment.construction_months == 0
    assert program_assumptions_of(pd.DataFrame()).max_seconds == ProgramAssumptions().max_seconds


# -- per lot, off the gap's inputs ---------------------------------------------------------


def _hbu_row(**overrides) -> dict:
    row = {
        "lot_uid": 1,
        "lot_number": "1 000 001",
        "feature_id": "H01-001",
        "source_table": "ZONAGE",
        "hbu_status": "solved",
        "solved": True,
        "gross_floor_area_m2": 1000.0,
        "lot_area_m2": 300.0,
        "primary_frontage_m": 10.0,
        "buildable_area_m2": None,
        "parkable_area_m2": None,
        "program_assumptions": json.dumps(ProgramAssumptions().as_metadata()),
    }
    row.update(overrides)
    return row


def _envelope_row(**overrides) -> dict:
    row = {
        "lot_uid": 1,
        "feature_id": "H01-001",
        "column_index": 0,
        "usages": json.dumps(["H"]),
        "levels": json.dumps(["tous_les_niveaux"]),
        "floors_min": 2,
        "floors_max": 6,
        "height_min_m": None,
        "height_max_m": None,
        "min_lot_width_m": None,
        "max_dwellings": None,
        "density_min": None,
        "density_max": 4.5,
        "site_coverage_min_pct": None,
        "site_coverage_max_pct": 70.0,
        "permits_residential": True,
        "permits_commercial": False,
        "permits_industrial": False,
        "governs_residential": True,
        "governs_commercial": False,
        "governs_industrial": False,
        "lot_area_m2": 300.0,
        "primary_frontage_m": 10.0,
    }
    row.update(overrides)
    return row


def _existing_row(**overrides) -> dict:
    row = {
        "lot_number": "1 000 001",
        "num_storeys": 2,
        "residential_floor_area_m2": 240.0,
        "commercial_floor_area_m2": 0.0,
        "industrial_floor_area_m2": 0.0,
        "num_dwellings": 2,
        "gross_income_cad": 36_000.0,
        "net_operating_income_cad": 23_400.0,
    }
    row.update(overrides)
    return row


def test_solve_enhancements_solves_a_plex_with_room_above_it():
    frame = solve_enhancements(
        pd.DataFrame([_hbu_row()]),
        pd.DataFrame([_existing_row()]),
        pd.DataFrame([_envelope_row()]),
        ECONOMICS,
        rules=EnhancementRules(construction_months=9, disruption_share=0.25),
    )
    assert list(frame.columns) == list(ENHANCEMENT_COLUMNS)
    row = frame.iloc[0]
    assert row["enhance_status"] == "OPTIMAL"
    assert row["enhance_solved"]
    assert row["enhance_added_storeys"] == 1
    assert row["enhance_added_floor_area_m2"] > 0
    assert row["enhance_added_dwellings"] > 0
    assert row["enhance_num_dwellings"] == row["enhance_added_dwellings"] + 2
    # A quarter of the standing NOI for nine months.
    assert row["enhance_disruption_cad"] == pytest.approx(23_400.0 * 0.25 * 9 / 12)
    assert row["enhance_gain_cad"] == pytest.approx(
        row["enhance_npv_cad"] - row["enhance_disruption_cad"]
    )


@pytest.mark.parametrize(
    "hbu_overrides, existing_overrides, expected",
    [
        ({"hbu_status": "road_parcel", "solved": False}, {}, "no_program"),
        ({}, {"num_storeys": None}, "no_building"),
        ({}, {"residential_floor_area_m2": 0.0}, "no_building"),
        ({"gross_floor_area_m2": 200.0}, {}, "not_underbuilt"),
        ({"feature_id": "H09-999"}, {}, "no_envelope"),
    ],
)
def test_a_lot_with_nothing_to_grow_says_why(hbu_overrides, existing_overrides, expected):
    frame = solve_enhancements(
        pd.DataFrame([_hbu_row(**hbu_overrides)]),
        pd.DataFrame([_existing_row(**existing_overrides)]),
        pd.DataFrame([_envelope_row()]),
        ECONOMICS,
    )
    row = frame.iloc[0]
    assert row["enhance_status"] == expected
    assert not row["enhance_solved"]
    assert pd.isna(row["enhance_gain_cad"])


# -- the three futures ----------------------------------------------------------------------


def test_the_futures_stand_on_one_footing():
    frame = pd.DataFrame(
        {
            "existing_present_value_cad": [400_000.0, 400_000.0, 400_000.0, None],
            "enhance_gain_cad": [150_000.0, -20_000.0, None, None],
            "hbu_npv_cad": [100_000.0, 500_000.0, 300_000.0, 250_000.0],
        }
    )
    futures = three_futures(frame)
    assert futures["hold_value_cad"].tolist() == [400_000.0, 400_000.0, 400_000.0, 0.0]
    assert futures.loc[0, "enhance_value_cad"] == 550_000.0
    assert futures.loc[0, "rebuild_value_cad"] == 100_000.0
    assert futures["best_future"].tolist() == ["enhance", "rebuild", "hold", "rebuild"]


def test_a_tie_goes_to_holding():
    frame = pd.DataFrame(
        {
            "existing_present_value_cad": [400_000.0],
            "enhance_gain_cad": [0.0],
            "hbu_npv_cad": [400_000.0],
        }
    )
    assert three_futures(frame)["best_future"].tolist() == ["hold"]


def test_the_gap_values_the_hold_without_a_delay(monkeypatch):
    """The rebuild's income is pushed out by its build; the standing building's
    is not. The gap must read the two factors apart or the delay cancels."""
    investment = InvestmentAssumptions(construction_months=18, lease_up_months=6)
    hbu_frame = pd.DataFrame(
        [
            {
                "lot_uid": 1, "lot_number": "1 000 001", "npv_cad": 100_000.0,
                "present_value_cad": 900_000.0, "annual_gross_revenue_cad": 90_000.0,
                "num_dwellings": 6, "residential_area_m2": 600.0,
                "commercial_area_m2": 0.0, "industrial_area_m2": 0.0,
                "unit_area_m2": 500.0, "annual_net_operating_income_cad": 50_000.0,
                "total_capital_cost_cad": 800_000.0,
            }
        ]
    )
    gap = hbu.use_gap(
        hbu_frame,
        pd.DataFrame([_existing_row()]),
        operating_expense_ratio=0.35,
        investment=investment,
    )
    assert gap.loc[0, "existing_present_value_cad"] == pytest.approx(
        23_400.0 * investment.hold_pv_factor
    )
    assert gap.loc[0, "existing_num_storeys"] == 2
