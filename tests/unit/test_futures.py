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
from dataclasses import replace

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
    DevelopmentProgram,
    InvestmentAssumptions,
    Lot,
    NO_PARKING,
    ParkingRules,
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


# -- the ground floor an enhancement already has ------------------------------


def test_an_enhancement_keeps_a_ground_floor_the_grid_would_not_grant_it():
    """The *Niveaux* placement rule is a rebuild's, not an addition's.

    A rebuild whose ``H`` column is marked *Tous sauf le RDC* must give its
    ground floor to something else, and where the zone authorises nothing else
    it may not be built at all. An enhancement is a building that already
    stands on a ground floor already occupied and already lawful - as of right
    or as a droit acquis - and the storeys this solve chooses go on **top** of
    it, above the RDC by construction. Holding the addition to the rule would
    ask the owner of a plex to put a shop under it before adding a storey.
    """
    off_the_rdc = replace(
        COLUMN, levels=frozenset({BuildingLevel.ALL_EXCEPT_GROUND})
    )
    # As a rebuild, the same column is refused outright: nothing may stand at
    # grade, and a building of storeys has a ground floor.
    rebuild = solve_program(off_the_rdc, LOT, ECONOMICS, parking=NO_PARKING)
    assert rebuild.status == "INFEASIBLE"
    assert rebuild.binding == ("no_usage_permitted_on_ground_floor",)

    # As an enhancement it solves, keeps its two standing storeys of housing,
    # and owes no commerce for the privilege.
    enhancement = solve_program(
        off_the_rdc, LOT, ECONOMICS, parking=NO_PARKING, retained=PLEX
    )
    assert enhancement.solved
    assert enhancement.commercial_floors == 0
    assert enhancement.industrial_floors == 0
    assert enhancement.residential_floors >= PLEX.storeys
    assert "ground_floor_excluded" not in enhancement.binding
    # And it is the same answer the column gives when the rows never excluded
    # the RDC at all, which is the point: the addition was never the storey
    # the grid was talking about.
    unrestricted = solve_program(
        COLUMN, LOT, ECONOMICS, parking=NO_PARKING, retained=PLEX
    )
    assert enhancement.residential_floors == unrestricted.residential_floors
    assert enhancement.npv_cad == pytest.approx(unrestricted.npv_cad)


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


def test_nothing_is_dug_under_or_bayed_into_a_standing_building():
    program = solve_program(COLUMN, LOT, ECONOMICS, retained=PLEX)
    assert program.underground_levels == 0
    assert program.underground_stalls == 0
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


# 240 sits on the solver's 0.01 m² grid; 139.3 and 153.3 are the roll's own
# figures and do not (139.3 x 100 is 13930.000000000002 in a float, and its
# ceiling is a centimetre of floor nobody built). Both must read as nothing.
@pytest.mark.parametrize("standing_floor", [240.0, 139.3, 153.3])
def test_an_enhancement_that_adds_nothing_is_worth_exactly_holding(standing_floor):
    """No storey and no annex pays at a premium this steep, so the solver's
    answer is the standing building - and works nobody does disturb no rent.
    The gain is 0, not minus a quarter of the NOI for nine months."""
    frame = solve_enhancements(
        pd.DataFrame([_hbu_row()]),
        pd.DataFrame([_existing_row(residential_floor_area_m2=standing_floor)]),
        pd.DataFrame([_envelope_row()]),
        ECONOMICS,
        rules=EnhancementRules(
            construction_months=9, disruption_share=0.25, addition_cost_premium=50.0
        ),
    )
    row = frame.iloc[0]
    assert row["enhance_solved"]
    assert "nothing_pencils" in json.loads(row["enhance_binding"])
    assert row["enhance_added_floor_area_m2"] == 0.0
    assert row["enhance_added_storeys"] == 0
    assert row["enhance_capital_cost_cad"] == 0.0
    assert row["enhance_disruption_cad"] == 0.0
    assert row["enhance_gain_cad"] == 0.0
    futures = three_futures(
        pd.concat(
            [pd.DataFrame([{"existing_present_value_cad": 165_873.22}]), frame], axis=1
        )
    )
    assert futures.loc[0, "enhance_value_cad"] == futures.loc[0, "hold_value_cad"]
    assert futures.loc[0, "best_future"] == "hold"


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


# -- the parking waived ---------------------------------------------------------------

#: A plex with a shop under it. The shop's floor owes stalls - the demand is
#: on the whole building, the standing part included - an enhancement parks
#: on the yard or not at all, and `NO_YARD` is a lot with no yard to speak of.
SHOP_PLEX = RetainedBuilding(
    footprint_m2=200.0,
    storeys=2,
    residential_floor_area_m2=200.0,
    commercial_floor_area_m2=200.0,
    dwellings=2,
    max_added_storeys=1,
)
MIXED = ZoneColumn(
    usages=("H", "C.2"),
    floors_max=6,
    floors_min=2,
    levels=frozenset({BuildingLevel.ALL}),
    site_coverage_max_pct=70.0,
    density_max=4.5,
)
NO_YARD = ParkingRules(max_surface_stalls=0)


def test_a_standing_shop_with_no_yard_has_no_enhancement_until_the_parking_is_waived():
    refused = solve_program(MIXED, LOT, ECONOMICS, retained=SHOP_PLEX, parking=NO_YARD)
    assert refused.status == "INFEASIBLE"
    assert refused.binding == ()

    waived = solve_program(
        MIXED, LOT, ECONOMICS, retained=SHOP_PLEX, parking=NO_YARD,
        waive_parking_if_empty=True,
    )
    assert waived.solved
    assert waived.is_enhancement
    assert waived.parking_waived
    assert waived.total_stalls == 0
    assert waived.added_floor_area_m2 > 0
    # Owed on the whole building - the standing shop's floor included, since
    # that is what the demand constraint read.
    assert waived.waived_stalls == NO_YARD.stalls_owed(
        dwellings=waived.total_dwellings,
        non_residential_area_sqft=(
            waived.commercial_area_sqft + waived.basement_commercial_area_sqft
        ),
    )
    assert waived.waived_stalls > 0


def test_solve_enhancements_carries_the_waiver():
    existing = _existing_row(
        residential_floor_area_m2=200.0, commercial_floor_area_m2=200.0
    )
    envelope = _envelope_row(
        usages=json.dumps(["H", "C.2"]),
        permits_commercial=True,
        governs_commercial=True,
    )
    frame = solve_enhancements(
        pd.DataFrame([_hbu_row()]),
        pd.DataFrame([existing]),
        pd.DataFrame([envelope]),
        ECONOMICS,
        assumptions=ProgramAssumptions(parking=NO_YARD),
    )
    assert list(frame.columns) == list(ENHANCEMENT_COLUMNS)
    row = frame.iloc[0]
    assert row["enhance_solved"]
    assert row["enhance_parking_waived"]
    assert row["enhance_waived_stalls"] > 0
    assert row["enhance_surface_stalls"] == 0

    strict = solve_enhancements(
        pd.DataFrame([_hbu_row()]),
        pd.DataFrame([existing]),
        pd.DataFrame([envelope]),
        ECONOMICS,
        assumptions=ProgramAssumptions(parking=NO_YARD, waive_parking_if_empty=False),
    )
    assert strict.iloc[0]["enhance_status"] == "INFEASIBLE"
    assert not strict.iloc[0]["enhance_solved"]
    assert not strict.iloc[0]["enhance_parking_waived"]
    assert strict.iloc[0]["enhance_waived_stalls"] == 0


# -- the parking rent in the income split -------------------------------------------


def test_the_income_shares_spread_the_parking_rent_over_the_families():
    """The parking rent is inside the gross and belongs to no family, so the
    shares are taken over the three family rents and still sum to one."""
    program = DevelopmentProgram(
        units={}, floors=0, footprint_m2=0.0, gross_floor_area_m2=0.0,
        unit_area_m2=0.0, net_operating_income=0.0, status="OPTIMAL",
        gross_revenue_cad=1_100.0,
        residential_gross_revenue_cad=800.0,
        commercial_gross_revenue_cad=200.0,
        parking_gross_revenue_cad=100.0,
    )
    shares = hbu._income_shares(program)
    assert shares == pytest.approx(
        {"residential": 0.8, "commercial": 0.2, "industrial": 0.0}
    )
    assert sum(shares.values()) == pytest.approx(1.0)


def test_use_gap_keeps_the_family_noi_lines_summing_with_parking_rent_inside():
    hbu_frame = pd.DataFrame(
        [
            {
                "lot_uid": 1, "lot_number": "1 000 001", "npv_cad": 100_000.0,
                "present_value_cad": 900_000.0,
                "annual_gross_revenue_cad": 99_000.0,
                "annual_residential_gross_revenue_cad": 90_000.0,
                "annual_commercial_gross_revenue_cad": 0.0,
                "annual_industrial_gross_revenue_cad": 0.0,
                "annual_parking_gross_revenue_cad": 9_000.0,
                "num_dwellings": 6, "residential_area_m2": 600.0,
                "commercial_area_m2": 0.0, "industrial_area_m2": 0.0,
                "unit_area_m2": 500.0, "annual_net_operating_income_cad": 50_000.0,
                "total_capital_cost_cad": 800_000.0,
            }
        ]
    )
    gap = hbu.use_gap(
        hbu_frame, pd.DataFrame([_existing_row()]), operating_expense_ratio=0.35
    )
    total = 99_000.0 * 0.65
    assert gap.loc[0, "hbu_annual_stabilised_noi_cad"] == pytest.approx(total)
    # All of it on the housing, parking rent included: the shop and the
    # workshop earn nothing here and get nothing.
    assert gap.loc[0, "hbu_residential_noi_cad"] == pytest.approx(total)
    assert gap.loc[0, "hbu_commercial_noi_cad"] == 0.0
    assert gap.loc[0, "hbu_industrial_noi_cad"] == 0.0
