"""Offline tests for the CP-SAT development program.

Three things are worth testing separately here, and they fail in different
ways. The level arithmetic and the column choice are pure lookups over a grid's
own vocabulary, so they are tested against the rows a real grid prints. The
solver is tested by making one cap binding at a time and checking the answer
sits exactly against it - an optimiser that is merely *plausible* is the hard
kind of wrong, so every expected number below is one that can be arrived at by
hand.

The envelopes are the ones cached under `data/cache/pdf/`: zone C01-001 of the
Villeray-Saint-Michel-Parc-Extension zoning by-law (01-283) authorises `H` on
*Tous sauf le RDC* with *En étage* 2/6, *Taux d'implantation* 50/70 and
*Densité* 0/4,5.

The parking tests are the fourth group, and they are about one asymmetry: an
underground stall is bigger (400 sq ft against 300) and dearer ($60 300
against $48 125) and is still the right answer whenever *Densité* binds,
because article 38 1° of 01-283 keeps it out of the *superficie de plancher*
entirely. Each of them makes exactly one of the two options impossible or
uneconomic and checks the solver picks the other for the stated reason. The
tests that are about something else - the rent arithmetic, the vacancy
factor - pass `NO_PARKING` and `NO_CONSTRUCTION_COST`, so a change to a
published rate cannot move a number those tests exist to pin.

The objective is net operating income, and the fifth group is about the one
thing that makes it more than the rent roll with a subtraction on the end.
Per dwelling, per month, at the module's own constants:

=================  =========  =========  =========  ==========
class              revenue    build      net        net/sq ft
=================  =========  =========  =========  ==========
studio               784.00     429.17     354.83      0.7097
1 bedroom            985.00     515.00     470.00      0.7833
2 bedroom          1 386.00     772.50     613.50      0.6817
3 bedroom plus     1 691.50   1 030.00     661.50      0.5513
=================  =========  =========  =========  ==========

Rent per square foot falls as dwellings get bigger and construction cost per
square foot does not, so the last two columns rank the classes in opposite
orders. Which one wins is entirely a question of which cap binds - a dwelling
ceiling buys the best dwelling, an envelope buys the best square foot - and
that is what the two tests at the head of the solver group are checking.
"""

from __future__ import annotations

import math

import pytest

from urban_rag.program import (
    ABOVE_GRADE_STALL_AREA_SQFT,
    ABOVE_GRADE_STALL_COST_CAD,
    AMORTIZATION_MONTHS,
    COMMERCIAL_COST_PER_SQFT_CAD,
    COMMERCIAL_REVENUE_PER_SQFT_CAD,
    COMMERCIAL_VACANCY_PCT,
    INDUSTRIAL_COST_PER_SQFT_CAD,
    INDUSTRIAL_REVENUE_PER_SQFT_CAD,
    INDUSTRIAL_VACANCY_PCT,
    M2_PER_SQFT,
    MONEY_SCALE,
    MONTHS_PER_YEAR,
    NO_CONSTRUCTION_COST,
    NO_NON_RESIDENTIAL,
    NO_PARKING,
    RESIDENTIAL_COST_PER_SQFT_CAD,
    STALLS_PER_1000_SQFT,
    STALLS_PER_DWELLING,
    UNDERGROUND_STALL_AREA_SQFT,
    UNDERGROUND_STALL_COST_CAD,
    UNIT_AREAS_SQFT,
    BuildingLevel,
    ConstructionCosts,
    Lot,
    NonResidentialEconomics,
    ParkingRules,
    ProgramError,
    UnitEconomics,
    ZoneColumn,
    is_commercial_usage,
    is_industrial_usage,
    is_residential_usage,
    permitted_floors,
    select_residential_column,
    solve_program,
)

#: Rents and vacancies in the shape `average_rents` / `vacancy_rates` write
#: them: dollars a month, and a rate **in percent as published**.
RENTS = {
    "studio": 800.0,
    "1_bedroom": 1000.0,
    "2_bedroom": 1400.0,
    "3_bedroom_plus": 1700.0,
}
VACANCY = {
    "studio": 2.0,
    "1_bedroom": 1.5,
    "2_bedroom": 1.0,
    "3_bedroom_plus": 0.5,
}
ECONOMICS = UnitEconomics(average_rent_cad=RENTS, vacancy_rate_pct=VACANCY)


def column(**overrides) -> ZoneColumn:
    """Zone C01-001's Habitation column, with fields overridden per test."""
    base = {
        "usages": ("H",),
        "levels": frozenset({BuildingLevel.ALL_EXCEPT_GROUND}),
        "floors_max": 6,
        "floors_min": 2,
        "site_coverage_min_pct": 50.0,
        "site_coverage_max_pct": 70.0,
        "density_max": 4.5,
        "density_min": 0.0,
        "zone": "C01-001",
    }
    return ZoneColumn(**{**base, **overrides})


# -- the usage codes --------------------------------------------------------


@pytest.mark.parametrize("usage", ["H", "H.1", "H.7", "H.7A", "H.2(9)", " H.3 "])
def test_residential_usages_are_recognised(usage):
    assert is_residential_usage(usage)


@pytest.mark.parametrize("usage", ["C.4", "I.2", "E.1(2)", "", "HOTEL", "CH.1"])
def test_other_usages_are_not_residential(usage):
    assert not is_residential_usage(usage)


# -- the level rows ---------------------------------------------------------


@pytest.mark.parametrize(
    ("levels", "expected"),
    [
        ({BuildingLevel.ALL}, 6),
        ({BuildingLevel.ALL_EXCEPT_GROUND}, 5),
        ({BuildingLevel.GROUND}, 1),
        ({BuildingLevel.BELOW_GROUND}, 1),
        ({BuildingLevel.SECOND}, 1),
        (set(), 0),
    ],
)
def test_each_level_row_contributes_its_own_storeys(levels, expected):
    assert permitted_floors(levels, total_floors=6) == expected


def test_level_rows_add_up():
    # The grid states two authorisations; together they are 1 + 5.
    assert (
        permitted_floors(
            {BuildingLevel.GROUND, BuildingLevel.ALL_EXCEPT_GROUND}, total_floors=6
        )
        == 6
    )


def test_added_rows_never_exceed_the_building():
    # Ground + second + all-but-ground is 1 + 1 + 5 = 7 storeys of authorisation
    # over a six-storey building. The building wins.
    assert (
        permitted_floors(
            {
                BuildingLevel.GROUND,
                BuildingLevel.SECOND,
                BuildingLevel.ALL_EXCEPT_GROUND,
            },
            total_floors=6,
        )
        == 6
    )


def test_all_except_ground_of_a_single_storey_building_is_nothing():
    assert permitted_floors({BuildingLevel.ALL_EXCEPT_GROUND}, total_floors=1) == 0


# -- choosing the column ----------------------------------------------------


def test_widest_minimum_the_lot_still_meets_wins():
    narrow = column(usages=("H.1",), min_lot_width_m=5.0)
    middle = column(usages=("H.2",), min_lot_width_m=10.0)
    wide = column(usages=("H.6",), min_lot_width_m=20.0)
    chosen = select_residential_column([narrow, middle, wide], frontage_m=15.0)
    assert chosen is middle


def test_a_lot_exactly_as_wide_as_the_minimum_meets_it():
    # A minimum is a floor: 10 m of frontage satisfies "largeur du terrain
    # min 10 m". Testing it strictly would drop the lot to the column below.
    exact = column(usages=("H.2",), min_lot_width_m=10.0)
    fallback = column(usages=("H.1",), min_lot_width_m=None)
    assert select_residential_column([fallback, exact], frontage_m=10.0) is exact


def test_a_column_with_no_minimum_is_the_fallback():
    dash = column(usages=("H",), min_lot_width_m=None)
    wide = column(usages=("H.6",), min_lot_width_m=25.0)
    assert select_residential_column([dash, wide], frontage_m=8.0) is dash


def test_non_residential_columns_are_never_chosen():
    commerce = column(usages=("C.4",), min_lot_width_m=None)
    industry = column(usages=("I.2",), min_lot_width_m=None)
    assert select_residential_column([commerce, industry], frontage_m=30.0) is None


def test_too_narrow_for_every_residential_column():
    assert select_residential_column([column(min_lot_width_m=20.0)], 12.0) is None


# -- the solver -------------------------------------------------------------


def test_the_dwelling_ceiling_binds():
    # Twelve dwellings on a lot roomy enough for far more: the count is what
    # stops it, and the mix is then the most valuable class only.
    program = solve_program(
        column(max_dwellings=12, density_max=None),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
    )
    assert program.solved
    assert program.total_dwellings == 12
    assert program.units == {"3_bedroom_plus": 12}
    assert "max_dwellings" in program.binding


def test_the_most_valuable_class_wins_when_only_the_count_is_capped():
    # One dwelling allowed and a lot with room for hundreds, so the question
    # is only which class earns most *per dwelling* - the fourth column of the
    # module docstring's table, where the three-bedroom wins at 661.50:
    #   1 700 x 0.995 - 1 200 x 257.50 / 300 = 1 691.50 - 1 030.00
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.units == {"3_bedroom_plus": 1}
    assert program.net_operating_income == pytest.approx(661.50)


def test_the_best_square_foot_wins_when_the_envelope_is_what_binds():
    # The same rents and the same rates, with the cap moved from the count to
    # the envelope: 1 400 m2 of dwellings and no ceiling on how many they are
    # divided into. Now the ranking is the *last* column, and the class that
    # won the test above comes last. Twenty-five 55.74 m2 one-bedrooms fill
    # 1 393.5 of the 1 400.
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.units == {"1_bedroom": 25}
    assert program.net_operating_income == pytest.approx(25 * 470.00)


def test_the_density_cap_binds_when_it_is_tighter_than_the_envelope():
    # C01-001's own 4,5 never binds on a 400 m2 lot - 70% coverage over five
    # storeys is 1 400 m2, well under 400 x 4,5 = 1 800 - so this uses a grid
    # printing 2,0: 800 m2 of floor area, and the mix is then whatever packs
    # the most net income into it. Fourteen dwellings do, and the last of the
    # 800 goes to a studio and a two-bedroom rather than to a fifteenth
    # one-bedroom there is no room for.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(column(density_max=2.0), lot, ECONOMICS, parking=NO_PARKING)
    assert program.solved
    assert program.gross_floor_area_m2 <= 800.0 + 1e-9
    assert program.unit_area_m2 <= program.gross_floor_area_m2 + 1e-9
    assert program.total_dwellings == 14
    assert "density_max" in program.binding


def test_site_coverage_and_storeys_bound_the_envelope_without_a_density_cap():
    # No Densité row, so the dwellings' envelope is footprint x storeys alone:
    # 400 m2 x 70% = 280 m2 per floor, over the five storeys "Tous sauf le RDC"
    # allows of a six-storey building = 1 400 m2, which is twenty-five 55.74 m2
    # one-bedrooms with 6.5 m2 to spare.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(
        column(density_max=None), lot, ECONOMICS, parking=NO_PARKING
    )
    assert program.solved
    assert program.residential_floors == 5
    assert program.units == {"1_bedroom": 25}
    assert program.unit_area_m2 == pytest.approx(1393.5)
    # The footprint is only pinned from below: any storey plate big enough to
    # hold the mix is equally optimal, since nothing rewards a larger one.
    assert 278.7 - 1e-9 <= program.footprint_m2 <= 280.0 + 1e-9
    assert program.footprint_m2 * program.residential_floors <= 1400.0 + 1e-9
    assert {"site_coverage_max", "floors"} <= set(program.binding)


def test_fewer_storeys_when_the_level_rows_say_ground_floor_only():
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(
        column(
            levels=frozenset({BuildingLevel.GROUND}),
            floors_min=0,
            density_max=None,
        ),
        lot,
        ECONOMICS,
    )
    assert program.residential_floors == 1
    # One storey of at most 70% of 400 m2: five 55.74 m2 one-bedrooms, 278.7 m2
    # inside the 280 m2 plate.
    assert program.units == {"1_bedroom": 5}
    assert program.unit_area_m2 <= 280.0 + 1e-9
    # A single residential storey, so the plate holds the whole mix at once -
    # pinned from below by the mix and from above by the coverage cap.
    assert 278.7 - 1e-9 <= program.footprint_m2 <= 280.0 + 1e-9
    # The level rows bound the dwellings, not the building: the five of them
    # owe three stalls, and a second storey is where those go.
    assert program.floors == 2
    assert program.above_grade_parking_floors == 1


def test_a_storey_minimum_the_level_rows_cannot_reach_is_infeasible():
    # "En étage min 2" over a usage authorised only on the ground floor. A real
    # contradiction in the grid, named rather than returned as a bare status.
    program = solve_program(
        column(levels=frozenset({BuildingLevel.GROUND}), floors_min=2),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
    )
    assert not program.solved
    assert program.status == "INFEASIBLE"
    assert program.binding == ("floors_min_exceeds_permitted_levels",)


def test_a_suppressed_rent_removes_that_class_without_failing():
    # CMHC suppresses heavily at quartier geography - 31 of 45 VSMPE cells in
    # the 2023 survey. A class with no published rent is not buildable, and is
    # reported rather than silently priced at zero.
    economics = UnitEconomics(
        average_rent_cad={"studio": 800.0, "1_bedroom": 1000.0},
        vacancy_rate_pct={"studio": 2.0, "1_bedroom": 1.5},
    )
    program = solve_program(
        column(max_dwellings=4, density_max=None),
        Lot(area_m2=2000.0, frontage_m=25.0),
        economics,
    )
    assert program.solved
    assert set(program.units) <= {"studio", "1_bedroom"}
    assert program.unpriced_types == ("2_bedroom", "3_bedroom_plus")


def test_no_priced_class_at_all_is_infeasible():
    program = solve_program(
        column(),
        Lot(area_m2=400.0, frontage_m=12.0),
        UnitEconomics(average_rent_cad={}),
    )
    assert not program.solved
    assert program.binding == ("no_priced_unit_type",)


def test_vacancy_is_read_as_a_percentage():
    # 10.0 means ten percent, not ten times over. One dwelling, so the profit
    # is exactly one unit's value and the factor is checkable by eye.
    economics = UnitEconomics(
        average_rent_cad={"studio": 1000.0}, vacancy_rate_pct={"studio": 10.0}
    )
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=1000.0, frontage_m=20.0),
        economics,
        parking=NO_PARKING,
        construction=NO_CONSTRUCTION_COST,
    )
    assert program.net_operating_income == pytest.approx(1000 * 0.9)


def test_a_missing_vacancy_is_treated_as_full_occupancy():
    economics = UnitEconomics(average_rent_cad={"studio": 1000.0})
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=1000.0, frontage_m=20.0),
        economics,
        parking=NO_PARKING,
        construction=NO_CONSTRUCTION_COST,
    )
    assert program.net_operating_income == pytest.approx(1000)


def test_units_fit_inside_the_envelope_they_are_placed_in():
    lot = Lot(area_m2=650.0, frontage_m=18.0)
    program = solve_program(column(), lot, ECONOMICS)
    assert program.solved
    assert program.unit_area_m2 <= program.gross_floor_area_m2 + 1e-9
    assert program.footprint_m2 * program.floors == pytest.approx(
        program.gross_floor_area_m2
    )
    assert program.footprint_m2 <= 650.0 * 0.70 + 1e-9
    assert program.footprint_m2 >= 650.0 * 0.50 - 1e-9


def test_the_answer_is_reported_against_the_zone_and_lot_it_was_asked_about():
    program = solve_program(
        column(), Lot(area_m2=400.0, frontage_m=12.0, lot_number="1425926"), ECONOMICS
    )
    assert program.zone == "C01-001"
    assert program.lot_number == "1425926"


def test_a_column_authorising_no_dwelling_is_refused():
    with pytest.raises(ProgramError, match="authorises no dwelling"):
        solve_program(
            column(usages=("C.4",)), Lot(area_m2=400.0, frontage_m=12.0), ECONOMICS
        )


def test_a_percentage_outside_zero_to_a_hundred_is_refused():
    with pytest.raises(ProgramError, match="percentage"):
        column(site_coverage_max_pct=140.0)


# -- the parking ------------------------------------------------------------


def test_the_parking_constants_are_the_ones_the_program_was_specified_with():
    assert STALLS_PER_DWELLING == 0.5
    assert UNDERGROUND_STALL_AREA_SQFT == 400.0
    assert ABOVE_GRADE_STALL_AREA_SQFT == 300.0
    # The Altus Group guide's Montreal midpoints, as `urban_rag.estimator`
    # publishes them: `parkade_ug` [51 925, 68 675] and `parkade_ag`
    # [38 500, 57 750], both flagged `perStall`.
    assert UNDERGROUND_STALL_COST_CAD == pytest.approx((51_925 + 68_675) / 2)
    assert ABOVE_GRADE_STALL_COST_CAD == pytest.approx((38_500 + 57_750) / 2)
    # Underground is worse on both counts. Only the by-law makes it worth it.
    assert UNDERGROUND_STALL_COST_CAD > ABOVE_GRADE_STALL_COST_CAD
    assert UNDERGROUND_STALL_AREA_SQFT > ABOVE_GRADE_STALL_AREA_SQFT


def test_half_a_stall_a_dwelling_rounds_up():
    # Eleven dwellings owe 5,5 stalls, and half a stall parks nobody.
    program = solve_program(
        column(max_dwellings=11, density_max=None),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
    )
    assert program.total_dwellings == 11
    assert program.total_stalls == 6


def test_a_slack_envelope_parks_above_grade_where_it_is_cheaper():
    # $48 125 a stall against $60 300, and on a 2 000 m2 lot with no Densité
    # row there is a whole spare storey to put them on, so the cheaper option
    # is also the feasible one. Nothing is dug.
    program = solve_program(
        column(max_dwellings=12, density_max=None),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
    )
    assert program.total_dwellings == 12
    assert program.above_grade_stalls == 6
    assert program.underground_stalls == 0
    assert program.underground_levels == 0
    assert program.above_grade_parking_floors == 1
    assert program.parking_cost_cad == pytest.approx(6 * ABOVE_GRADE_STALL_COST_CAD)


def test_a_tight_density_cap_drives_the_parking_underground():
    # ISP 2,0 on 400 m2 is 800 m2 of superficie de plancher, and the mix fills
    # 798.94 of it. An above-grade stall would have to come out of the 1.06
    # left; an underground one comes out of area the index cannot see, so all
    # seven are dug rather than one dwelling being given up for them.
    program = solve_program(
        column(density_max=2.0), Lot(area_m2=400.0, frontage_m=12.0), ECONOMICS
    )
    assert program.total_dwellings == 14
    assert program.underground_stalls == 7
    assert program.above_grade_stalls == 0
    assert program.above_grade_parking_floors == 0
    # Seven stalls of 37.16 m2 is 260.13 m2 of excavation, and *how many
    # levels* that is, is not something the model decides: once the mix is
    # fixed, any footprint x floors product big enough to hold it is equally
    # optimal, and the levels follow the footprint. 266.67 x 3 buys one level
    # deep enough; 200 x 4 needs two. Both are optimal and CP-SAT returns
    # whichever it reaches first, so what is asserted is the excavation - see
    # `_binding_caps` on why the caps are read rather than the solution.
    assert program.underground_levels >= 1
    assert program.underground_area_m2 >= 7 * UNDERGROUND_STALL_AREA_SQFT * M2_PER_SQFT
    assert program.parking_cost_cad == pytest.approx(7 * UNDERGROUND_STALL_COST_CAD)


def test_underground_parking_is_neither_floor_area_nor_a_storey():
    # Article 38 1° of 01-283, which is the whole reason the two options are
    # not substitutes: seven stalls are built and paid for, and both the
    # density index and the storey count are unmoved by them.
    program = solve_program(
        column(density_max=2.0), Lot(area_m2=400.0, frontage_m=12.0), ECONOMICS
    )
    assert program.underground_area_m2 > 0.0
    assert program.gross_floor_area_m2 <= 800.0 + 1e-9
    assert program.gross_floor_area_m2 == pytest.approx(
        program.footprint_m2 * program.floors
    )
    assert program.floors == program.residential_floors


def test_above_grade_parking_is_paid_for_in_dwellings():
    # The same lot with the excavator taken away. Three storeys of dwellings
    # and one of stalls inside the same 800 m2: ten dwellings rather than
    # fourteen, and the report names the parking as what took the other four.
    program = solve_program(
        column(density_max=2.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=ParkingRules(max_underground_levels=0),
    )
    assert program.total_dwellings == 10
    assert program.above_grade_stalls == 5
    assert program.underground_stalls == 0
    assert program.above_grade_parking_floors == 1
    assert {"density_max", "above_grade_parking"} <= set(program.binding)


def test_more_stalls_a_dwelling_stack_more_underground_levels():
    # Two stalls a dwelling rather than a half, so the parking is four times
    # the burden and the mix shifts to fewer, larger dwellings to carry fewer
    # stalls: nine of them owing eighteen. At 37.16 m2 each that is 668.9 m2,
    # and a 200 m2 plate takes four levels to hold it.
    program = solve_program(
        column(density_max=2.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=ParkingRules(stalls_per_dwelling=2.0),
    )
    assert program.total_dwellings == 9
    assert program.underground_stalls == 18
    assert program.underground_levels == 4


def test_the_footprint_holds_whichever_floor_is_hungriest():
    # One plate for every storey, so it is at least what a residential floor
    # needs and at least what a parking floor needs.
    program = solve_program(column(), Lot(area_m2=650.0, frontage_m=18.0), ECONOMICS)
    assert program.solved
    assert program.footprint_m2 >= (
        program.unit_area_m2 / program.residential_floors - 1e-9
    )
    if program.above_grade_parking_floors:
        stall_area_m2 = ABOVE_GRADE_STALL_AREA_SQFT * M2_PER_SQFT
        assert program.footprint_m2 >= (
            program.above_grade_stalls
            * stall_area_m2
            / program.above_grade_parking_floors
            - 1e-9
        )
    if program.underground_levels:
        stall_area_m2 = UNDERGROUND_STALL_AREA_SQFT * M2_PER_SQFT
        assert program.footprint_m2 >= (
            program.underground_stalls * stall_area_m2 / program.underground_levels
            - 1e-9
        )


def test_no_parking_asks_the_envelope_question_on_its_own():
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.total_stalls == 0
    assert program.parking_cost_cad == 0.0
    assert program.underground_levels == 0
    assert program.above_grade_parking_floors == 0
    assert program.floors == program.residential_floors == 5
    assert program.units == {"1_bedroom": 25}


@pytest.mark.parametrize(
    "overrides",
    [
        {"stalls_per_dwelling": -0.5},
        {"underground_area_sqft": -1.0},
        {"above_grade_cost_cad": -1.0},
        {"amortization_months": 0},
        {"max_underground_levels": -1},
    ],
)
def test_a_parking_rule_that_cannot_be_priced_is_refused(overrides):
    with pytest.raises(ProgramError):
        ParkingRules(**overrides)


# -- net operating income ---------------------------------------------------


def test_the_construction_rate_is_the_guides_montreal_wood_frame_midpoint():
    # `condo_wood`, "Wood Frame Condo (Up to 6 Storeys)", mtl [225, 290] - the
    # band `estimator.LOW_RISE_CONDO_TYPE_ID` names, and the construction a
    # borough zoned two-to-six storeys actually builds.
    assert RESIDENTIAL_COST_PER_SQFT_CAD == pytest.approx((225 + 290) / 2)
    assert AMORTIZATION_MONTHS == 300


def test_the_objective_nets_the_build_off_the_rent():
    # One three-bedroom, no stalls: 1 700 x 0.995 collected against
    # 1 200 sq ft x $257.50 spread over 300 months.
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.gross_revenue_cad == pytest.approx(1700 * 0.995)
    assert program.construction_cost_cad == pytest.approx(1200 * 257.5)
    assert program.net_operating_income == pytest.approx(
        1700 * 0.995 - 1200 * 257.5 / 300
    )
    # `profit` is the same number under the name it used to carry.
    assert program.profit == program.net_operating_income


def test_the_stalls_are_charged_on_the_same_footing_as_the_dwellings():
    # The same dwelling, with its half-stall rounded up to one. Both costs are
    # amortised over the same horizon, and the objective's coefficients are
    # rounded to the `MONEY_SCALE` grain it is held in - which is finer than a
    # cent, because a square metre of non-residential floor is worth cents.
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        ECONOMICS,
    )
    assert program.above_grade_stalls == 1
    dwelling = round((1700 * 0.995 - 1200 * 257.5 / 300) * MONEY_SCALE) / MONEY_SCALE
    stall = (
        round(ABOVE_GRADE_STALL_COST_CAD / AMORTIZATION_MONTHS * MONEY_SCALE)
        / MONEY_SCALE
    )
    assert program.net_operating_income == pytest.approx(dwelling - stall)
    # The capital figures are reported unamortised, and separately.
    assert program.parking_cost_cad == pytest.approx(ABOVE_GRADE_STALL_COST_CAD)
    assert program.construction_cost_cad == pytest.approx(1200 * 257.5)
    assert program.total_capital_cost_cad == pytest.approx(
        1200 * 257.5 + ABOVE_GRADE_STALL_COST_CAD
    )


def test_construction_is_priced_off_the_unit_schedule():
    # Not off the gross floor area: the corridors and lobbies the difference
    # would pay for are not costed at all, which is the assumption this pins.
    program = solve_program(column(), Lot(area_m2=650.0, frontage_m=18.0), ECONOMICS)
    assert program.solved
    expected = sum(
        quantity * UNIT_AREAS_SQFT[unit_type] * RESIDENTIAL_COST_PER_SQFT_CAD
        for unit_type, quantity in program.units.items()
    )
    assert program.construction_cost_cad == pytest.approx(expected)
    priced_area_m2 = sum(
        quantity * UNIT_AREAS_SQFT[unit_type] * M2_PER_SQFT
        for unit_type, quantity in program.units.items()
    )
    assert priced_area_m2 < program.gross_floor_area_m2


def test_rents_that_cannot_cover_the_build_produce_no_dwellings():
    # Every class at half the rent is under water - a one-bedroom collects
    # 492.50 against 515.00 of amortised construction - so the answer is that
    # nothing pencils, rather than a building that loses money.
    program = solve_program(
        column(),
        Lot(area_m2=650.0, frontage_m=18.0),
        UnitEconomics(
            average_rent_cad={name: rent / 2 for name, rent in RENTS.items()},
            vacancy_rate_pct=VACANCY,
        ),
    )
    assert program.solved
    assert program.units == {}
    assert program.total_stalls == 0
    assert program.net_operating_income == pytest.approx(0.0)


def test_a_cheaper_structure_is_worth_more_on_the_same_envelope():
    # The one lever that moves the answer without touching the by-law: the
    # same mix on the same lot, costed as wood frame against `condo_12`'s
    # mtl midpoint of $305. Same building, less of it spent.
    lot = Lot(area_m2=650.0, frontage_m=18.0)
    wood = solve_program(column(), lot, ECONOMICS)
    concrete = solve_program(
        column(),
        lot,
        ECONOMICS,
        construction=ConstructionCosts(residential_cost_per_sqft=305.0),
    )
    assert wood.net_operating_income > concrete.net_operating_income
    assert wood.construction_cost_cad < concrete.construction_cost_cad


def test_no_construction_cost_leaves_the_revenue_alone():
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        ECONOMICS,
        parking=NO_PARKING,
        construction=NO_CONSTRUCTION_COST,
    )
    assert program.construction_cost_cad == 0.0
    assert program.net_operating_income == pytest.approx(program.gross_revenue_cad)


@pytest.mark.parametrize(
    "overrides",
    [{"residential_cost_per_sqft": -1.0}, {"amortization_months": 0}],
)
def test_a_construction_cost_that_cannot_be_priced_is_refused(overrides):
    with pytest.raises(ProgramError):
        ConstructionCosts(**overrides)


def test_unit_areas_convert_to_metres_once():
    # The schedule is imperial and every constraint is metric; this is the
    # single conversion the whole module depends on.
    assert UNIT_AREAS_SQFT["2_bedroom"] * M2_PER_SQFT == pytest.approx(
        83.6127, abs=1e-4
    )


# -- the usage codes beside Habitation ---------------------------------------


@pytest.mark.parametrize("usage", ["C", "C.1", "C.7A", "C.3(9)", " C.2 "])
def test_commercial_usages_are_recognised(usage):
    assert is_commercial_usage(usage)
    assert not is_industrial_usage(usage)


@pytest.mark.parametrize("usage", ["I", "I.1", "I.4B", "I.2(9)", " I.3 "])
def test_industrial_usages_are_recognised(usage):
    assert is_industrial_usage(usage)
    assert not is_commercial_usage(usage)


@pytest.mark.parametrize("usage", ["H.2", "E.1", "", "CH.1", "Commerce", "IND"])
def test_the_other_families_are_neither(usage):
    # Anchored the whole way, so a letter inside a word is not the code.
    assert not is_commercial_usage(usage)
    assert not is_industrial_usage(usage)


# -- the commerce and the industry -------------------------------------------


def mixed(*extra_usages, **overrides) -> ZoneColumn:
    """C01-001's column with more usages at its head, and every level allowed.

    "Tous les niveaux" rather than the base column's "Tous sauf le RDC", so
    the storeys the level rows allow equal the storeys *En etage* allows and a
    test about who wins a storey is not also a test about how many there are.
    """
    return column(
        usages=("H.2", *extra_usages),
        levels=frozenset({BuildingLevel.ALL}),
        **overrides,
    )


def test_a_column_that_authorises_no_commerce_builds_none():
    # The base column is bare "H", and nothing about its answer changes.
    program = solve_program(
        column(levels=frozenset({BuildingLevel.ALL})),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.commercial_floors == 0
    assert program.industrial_floors == 0
    assert program.commercial_area_m2 == 0.0
    assert program.commercial_cost_cad == 0.0
    assert program.units == {"1_bedroom": 30}


def test_commerce_outbids_housing_for_every_storey_the_grid_will_spare():
    # 80/12 x 0.93 - 300/300 = $5.20 a square foot a month, against a
    # one-bedroom's $470 over 600 square feet, or $0.78. With no stalls owed,
    # commerce takes every storey that is not held back by *En etage min*,
    # which reserves two for the dwellings.
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.solved
    assert program.commercial_floors == 4
    assert program.residential_floors == 2
    assert program.floors == 6
    assert program.industrial_floors == 0
    # The storeys the dwellings did not get are the answer to why there are
    # only ten of them, and no printed norm binds at all here.
    assert program.binding == ("commercial_floor_area",)


def test_industry_takes_the_storeys_where_no_commerce_is_authorised():
    # 30/12 x 0.93 - 200/300 = $1.66 a square foot a month: worse than
    # commerce and still more than twice a one-bedroom's $0.78, so an I column
    # fills the same way a C column does.
    program = solve_program(
        mixed("I.1"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.industrial_floors == 4
    assert program.commercial_floors == 0
    assert program.binding == ("industrial_floor_area",)


def test_commerce_beats_industry_where_both_are_authorised():
    program = solve_program(
        mixed("C.2", "I.1"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.commercial_floors == 4
    assert program.industrial_floors == 0


def test_the_non_residential_storeys_are_floor_area_the_density_cap_sees():
    # Unlike an underground stall, a retail floor is *superficie de plancher*:
    # inside `gross_floor_area_m2`, and what the cap is tested against.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(mixed("C.2"), lot, ECONOMICS, parking=NO_PARKING)
    assert program.gross_floor_area_m2 <= 400.0 * 4.5 + 1e-9
    assert program.commercial_area_m2 > 0.0
    assert program.gross_floor_area_m2 == pytest.approx(
        program.footprint_m2 * program.floors
    )
    assert program.non_residential_area_m2 == pytest.approx(
        program.footprint_m2 * program.commercial_floors
    )


def test_the_square_footage_is_the_area_reported_in_the_other_unit():
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.commercial_area_sqft == pytest.approx(
        program.commercial_area_m2 / M2_PER_SQFT
    )
    assert program.industrial_area_sqft == 0.0


def test_the_commercial_floors_are_priced_and_rented_by_the_square_foot():
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    sqft = program.commercial_area_sqft
    assert program.commercial_cost_cad == pytest.approx(
        sqft * COMMERCIAL_COST_PER_SQFT_CAD
    )
    # The rate is annual per square foot, the objective is a month, and the
    # vacancy comes off it exactly as a dwelling's does.
    commercial_rent = (
        sqft
        * COMMERCIAL_REVENUE_PER_SQFT_CAD
        / MONTHS_PER_YEAR
        * (1 - COMMERCIAL_VACANCY_PCT / 100.0)
    )
    dwelling_rent = sum(
        RENTS[unit_type] * (1 - VACANCY[unit_type] / 100.0) * quantity
        for unit_type, quantity in program.units.items()
    )
    assert program.gross_revenue_cad == pytest.approx(commercial_rent + dwelling_rent)
    assert program.total_capital_cost_cad == pytest.approx(
        program.construction_cost_cad + program.commercial_cost_cad
    )


def test_the_industrial_floors_are_priced_the_same_way():
    program = solve_program(
        mixed("I.1"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.industrial_cost_cad == pytest.approx(
        program.industrial_area_sqft * INDUSTRIAL_COST_PER_SQFT_CAD
    )
    assert program.commercial_cost_cad == 0.0


def test_space_that_earns_nothing_is_not_built():
    # The same column, with the rates zeroed rather than the zoning changed.
    # The variables exist and the solver declines them, because at no rent
    # every square foot of them is a build cost and nothing else.
    program = solve_program(
        mixed("C.2", "I.1"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        non_residential=NO_NON_RESIDENTIAL,
    )
    assert program.commercial_floors == 0
    assert program.industrial_floors == 0
    assert program.units == {"1_bedroom": 30}


def test_a_rent_that_does_not_cover_the_build_is_declined():
    # $6 a square foot a year is $0.47 a month after vacancy, against $1.00 of
    # amortised build - so the coefficient is negative and no storey is raised.
    thin = NonResidentialEconomics(commercial_per_sqft_year=6.0)
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        non_residential=thin,
    )
    assert program.commercial_floors == 0


def test_the_storey_ceiling_is_shared_with_the_dwellings():
    # *En etage max* is six and *En etage min* two, so four are what commerce
    # can take at most - a column printing a tighter maximum spares fewer.
    program = solve_program(
        mixed("C.2", floors_max=3),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.floors == 3
    assert program.residential_floors == 2
    assert program.commercial_floors == 1


def test_the_level_rows_bound_the_commerce_as_they_bound_the_dwellings():
    # "Tous sauf le RDC" on a six-storey column allows five, and the rows are
    # the column's rather than any one usage's - so the six-storey stack of
    # the tests above cannot be built by anything the column authorises.
    program = solve_program(
        column(usages=("H.2", "C.2")),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.floors == 5
    assert program.commercial_floors == 3
    assert program.residential_floors == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"commercial_per_sqft_year": -1.0},
        {"industrial_per_sqft_year": -1.0},
        {"months_per_year": 0},
    ],
)
def test_a_non_residential_rate_that_cannot_be_priced_is_refused(overrides):
    with pytest.raises(ProgramError):
        NonResidentialEconomics(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [{"commercial_cost_per_sqft": -1.0}, {"industrial_cost_per_sqft": -1.0}],
)
def test_a_non_residential_build_cost_that_is_negative_is_refused(overrides):
    with pytest.raises(ProgramError):
        ConstructionCosts(**overrides)


def test_the_non_residential_rates_are_the_ones_the_docstring_quotes():
    # Pinned because the module docstring quotes all four in its arithmetic,
    # and a rate changed without the prose beside it is how that prose ages.
    assert COMMERCIAL_COST_PER_SQFT_CAD == 300.0
    assert INDUSTRIAL_COST_PER_SQFT_CAD == 200.0
    assert COMMERCIAL_REVENUE_PER_SQFT_CAD == 80.0
    assert INDUSTRIAL_REVENUE_PER_SQFT_CAD == 30.0
    assert COMMERCIAL_VACANCY_PCT == 7.0
    assert INDUSTRIAL_VACANCY_PCT == 7.0
    assert MONTHS_PER_YEAR == 12


# -- the non-residential vacancy ---------------------------------------------


def test_the_vacancy_comes_off_the_asking_rent():
    rates = NonResidentialEconomics()
    # $80 a year asking is $6.67 a month asking and $6.20 a month earning.
    assert rates.commercial_per_sqft_month == pytest.approx(80.0 / 12)
    assert rates.commercial_effective_per_sqft_month == pytest.approx(
        80.0 / 12 * 0.93
    )
    assert rates.industrial_per_sqft_month == pytest.approx(30.0 / 12)
    assert rates.industrial_effective_per_sqft_month == pytest.approx(
        30.0 / 12 * 0.93
    )
    # And the revenue a floor collects is the effective one, per square foot,
    # exactly as a dwelling's is per dwelling.
    assert rates.commercial_monthly_revenue(1000.0) == pytest.approx(
        1000.0 * 80.0 / 12 * 0.93
    )
    assert rates.industrial_monthly_revenue(1000.0) == pytest.approx(
        1000.0 * 30.0 / 12 * 0.93
    )


def test_the_reported_revenue_is_net_of_the_vacancy():
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    dwellings = sum(
        RENTS[unit_type] * (1 - VACANCY[unit_type] / 100.0) * quantity
        for unit_type, quantity in program.units.items()
    )
    commerce = (
        program.commercial_area_sqft
        * COMMERCIAL_REVENUE_PER_SQFT_CAD
        / MONTHS_PER_YEAR
        * (1 - COMMERCIAL_VACANCY_PCT / 100.0)
    )
    assert program.gross_revenue_cad == pytest.approx(dwellings + commerce)


def test_a_softer_market_is_a_higher_vacancy_and_not_a_lower_rent():
    # The same asking rent with the floor mostly empty. The coefficient goes
    # negative and the storeys are not raised, which is the vacancy doing the
    # job the rate would otherwise have to be falsified to do.
    empty = NonResidentialEconomics(commercial_vacancy_pct=95.0)
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        non_residential=empty,
    )
    assert program.commercial_floors == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"commercial_vacancy_pct": -1.0},
        {"industrial_vacancy_pct": 101.0},
    ],
)
def test_a_vacancy_that_is_not_a_percentage_is_refused(overrides):
    with pytest.raises(ProgramError):
        NonResidentialEconomics(**overrides)


# -- the stalls the commerce owes --------------------------------------------


def test_the_non_residential_floor_owes_stalls_by_the_thousand_square_feet():
    # Three per thousand, on the gross area of the storeys - the same area the
    # rent and the build cost are charged on.
    rules = ParkingRules(stalls_per_dwelling=0.0)
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=rules,
    )
    assert program.commercial_area_sqft > 0.0
    owed = math.ceil(STALLS_PER_1000_SQFT * program.commercial_area_sqft / 1000.0)
    assert program.total_stalls == owed


def test_the_industry_owes_them_at_the_same_rate():
    rules = ParkingRules(stalls_per_dwelling=0.0)
    program = solve_program(
        mixed("I.1"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=rules,
    )
    owed = math.ceil(STALLS_PER_1000_SQFT * program.industrial_area_sqft / 1000.0)
    assert program.total_stalls == owed


def test_the_dwellings_and_the_floors_owe_one_number_of_stalls_between_them():
    # Not two ceilings added together: a half stall owed by the housing and a
    # fraction owed by the retail are one stall, and rounding each separately
    # would buy two.
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
    )
    demand = (
        STALLS_PER_DWELLING * program.total_dwellings
        + STALLS_PER_1000_SQFT * program.commercial_area_sqft / 1000.0
    )
    assert program.total_stalls == math.ceil(demand)


def test_no_parking_owes_nothing_on_a_column_that_authorises_commerce():
    program = solve_program(
        mixed("C.2", "I.1"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.total_stalls == 0
    assert program.above_grade_parking_floors == 0
    assert program.underground_levels == 0


def test_the_stalls_the_commerce_owes_cost_it_something():
    # The same column and the same rates, with only the parking turned on.
    # Stalls appear, they are paid for, and the income falls by what they cost.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    free = solve_program(mixed("C.2"), lot, ECONOMICS, parking=NO_PARKING)
    charged = solve_program(
        mixed("C.2"), lot, ECONOMICS, parking=ParkingRules(stalls_per_dwelling=0.0)
    )
    assert free.total_stalls == 0
    assert charged.total_stalls > 0
    assert charged.parking_cost_cad > 0.0
    assert charged.net_operating_income < free.net_operating_income


def test_the_commerce_digs_rather_than_spend_the_density_cap_on_its_parking():
    # Three stalls per thousand square feet against a stall's own 300 is very
    # nearly a parking plate for every retail plate, and an above-grade plate
    # is floor area *Densite* counts while an underground one is not - article
    # 38 1°. So the stalls a retail storey owes go under the building.
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=ParkingRules(stalls_per_dwelling=0.0),
    )
    assert program.underground_stalls > 0
    assert program.above_grade_stalls == 0
    assert program.above_grade_parking_floors == 0


def test_parking_is_required_by_either_ratio_alone():
    # A column that authorises only commerce owes stalls without a dwelling
    # anywhere in the program.
    assert ParkingRules(stalls_per_dwelling=0.0).required
    assert ParkingRules(stalls_per_1000_sqft=0.0).required
    assert not NO_PARKING.required


def test_a_stall_ratio_that_cannot_be_applied_is_refused():
    with pytest.raises(ProgramError):
        ParkingRules(stalls_per_1000_sqft=-1.0)


def test_the_stall_ratios_are_the_ones_the_docstring_quotes():
    assert STALLS_PER_DWELLING == 0.5
    assert STALLS_PER_1000_SQFT == 3.0
