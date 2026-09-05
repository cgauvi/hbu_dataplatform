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

That asymmetry is between the two *structured* options, and it is only ever
reached once the yard is spent: a surface stall is $6 105 against $48 125 and
$60 300, so the solver parks on the ground the footprint leaves and buys
structure only when the coverage cap has left none. So the tests in that group pass
`STRUCTURED_ONLY`, which is `max_surface_stalls=0` - the same knob
`max_underground_levels=0` already was, pointed at the other option - and stay
about the dig-against-deck question they were written for. The group after
them is about the yard itself, and about the parcel that made it necessary:
lot 2 166 060, whose zoning permits one house and which came back with
nothing at all while a parkade was the only stall the model knew how to buy.

The objective is discounted net profit; the tables below are computed under
`UNDISCOUNTED_INVESTMENT`, which reprices it back to the old monthly NOI
exactly (see that constant), and the discounting itself has its own group at
the bottom of the file. Per dwelling, per month, at the module's constants:

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

import numpy as np
import pytest

from urban_rag.program import (
    ABOVE_GRADE_PARKING_STOREY_HEIGHT_M,
    BASEMENT_LEVELS,
    BASEMENT_LEVELS_ALLOWED,
    BASEMENT_STACK_ORDER,
    BELOW_GRADE_COST_PREMIUM,
    BELOW_GRADE_RENT_DISCOUNT_PCT,
    NO_BASEMENT,
    permitted_basement_levels,
    ABOVE_GRADE_STALL_AREA_SQFT,
    ABOVE_GRADE_STALL_COST_CAD,
    AMORTIZATION_MONTHS,
    ASSUMED_BUILDING_AGE_YEARS,
    DEFAULT_INVESTMENT,
    DEFAULT_NON_RESIDENTIAL,
    InvestmentAssumptions,
    UNDISCOUNTED_INVESTMENT,
    BuildingLevel,
    COMMERCIAL_COST_PER_SQFT_CAD,
    COMMERCIAL_REVENUE_PER_SQFT_CAD,
    COMMERCIAL_STOREY_HEIGHT_M,
    COMMERCIAL_VACANCY_PCT,
    ConstructionCosts,
    DEFAULT_STOREY_HEIGHTS,
    DevelopmentProgram,
    FLOOR_STACK_ORDER,
    INDUSTRIAL_COST_PER_SQFT_CAD,
    INDUSTRIAL_REVENUE_PER_SQFT_CAD,
    INDUSTRIAL_STOREY_HEIGHT_M,
    INDUSTRIAL_VACANCY_PCT,
    Lot,
    M2_PER_SQFT,
    MAINTENANCE_PREMIUM_PER_YEAR,
    MAX_MAINTENANCE_PREMIUM,
    MONEY_SCALE,
    MONTHS_PER_YEAR,
    NO_CONSTRUCTION_COST,
    NO_NON_RESIDENTIAL,
    NO_PARKING,
    NonResidentialEconomics,
    ParkingRules,
    ProgramError,
    RESIDENTIAL_COST_PER_SQFT_CAD,
    RESIDENTIAL_STOREY_HEIGHT_M,
    STALLS_PER_1000_SQFT,
    STALLS_PER_DWELLING,
    SURFACE_STALL_AREA_SQFT,
    SURFACE_STALL_COST_CAD,
    GARAGE_SHELL_FRACTION,
    GARAGE_STALL_AREA_SQFT,
    GARAGE_STALL_COST_CAD,
    StoreyHeights,
    UNDERGROUND_LEVEL_HEIGHT_M,
    UNDERGROUND_STALL_AREA_SQFT,
    UNDERGROUND_STALL_COST_CAD,
    UNIT_AREAS_SQFT,
    UnitEconomics,
    ZoneColumn,
    ZoneEnvelope,
    building_age_years,
    class_max_dwellings,
    effective_operating_expense_ratio,
    floor_stack,
    is_commercial_usage,
    is_industrial_usage,
    is_residential_usage,
    maintenance_premium,
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

#: The parking program with both cheap provisions taken away, so the only
#: stalls left are the two that cost a whole storey or a hole. The counterpart
#: of `ParkingRules(max_underground_levels=0)`, which the group below already
#: used to take the excavator away, and what every test about the
#: dig-against-deck trade passes: a stall on the yard is $6 105 and a bay in
#: the ground floor $15 450 against the deck's $48 125, so on any lot with
#: either to spare the solver takes it and the trade those tests are about
#: never comes up.
STRUCTURED_ONLY = ParkingRules(max_surface_stalls=0, max_garage_stalls=0)

#: The objective the docstring tables at the head of this file were computed
#: under: the old undiscounted amortisation, no expenses, no premium. The
#: tests that pin a *mix* or a hand-computed dollar figure pass it, so they
#: stay about the caps and the rent arithmetic rather than about the price of
#: money - which has its own test group at the bottom of the file.
UNDISCOUNTED = UNDISCOUNTED_INVESTMENT


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


# -- the Habitation class ceiling -------------------------------------------


@pytest.mark.parametrize(
    ("usages", "expected"),
    [
        (("H.1",), 1),
        (("H.2",), 2),
        (("H.3",), 3),
        (("H.4",), 8),
        (("H.5",), 12),
        (("H.6",), 36),
        # H.7 is "37 and up", which is no ceiling the model can hold.
        (("H.7",), None),
        # Bare H is the whole category and bounds nothing.
        (("H",), None),
        # The most permissive of several, not the least: a column headed both
        # authorises either building.
        (("H.2", "H.4"), 8),
        # A trailing letter distinguishes two readings of one class.
        (("H.7A",), None),
        (("H.4A",), 8),
        # A footnote qualifies a usage rather than renaming it.
        (("H.2(9)",), 2),
        # Non-residential codes neither add a ceiling nor lift one.
        (("C.4",), None),
        (("H.2", "C.4"), 2),
        ((), None),
    ],
)
def test_the_class_ceiling_is_read_off_the_usage_codes(usages, expected):
    assert class_max_dwellings(usages) == expected


@pytest.mark.parametrize(
    ("usages", "printed", "expected"),
    [
        # The grid prints nothing for H.2 - the code has already said two.
        (("H.2",), None, 2),
        # Both stated: a building answers to whichever is stricter.
        (("H.4",), 4, 4),
        (("H.4",), 12, 8),
        # Neither stated.
        (("H",), None, None),
        # Only the grid.
        (("H",), 5, 5),
    ],
)
def test_the_effective_ceiling_is_the_stricter_of_the_two(usages, printed, expected):
    assert (
        column(usages=usages, max_dwellings=printed).effective_max_dwellings
        == expected
    )


def test_the_class_ceiling_binds_a_column_the_grid_left_blank():
    """The 498 H.1/H.2/H.3 columns of VSMPE, which print no dwelling maximum.

    Three storeys at 60% of a 600 m2 lot is 1 080 m2 of floor and about twenty
    dwellings, and an H.2 column authorises a duplex. Without the class the
    solver built the twenty.
    """
    duplex = column(
        usages=("H.2",),
        levels=frozenset({BuildingLevel.ALL}),
        floors_max=3,
        site_coverage_max_pct=60.0,
        density_max=None,
        max_dwellings=None,
    )
    lot = Lot(area_m2=600.0, frontage_m=15.0)
    program = solve_program(duplex, lot, ECONOMICS, parking=NO_PARKING)
    assert sum(program.units.values()) == 2
    assert "max_dwellings" in program.binding

    # The identical envelope read as bare H has no ceiling and fills up.
    from dataclasses import replace

    unclassed = solve_program(
        replace(duplex, usages=("H",)), lot, ECONOMICS, parking=NO_PARKING
    )
    assert sum(unclassed.units.values()) > 2
    assert "max_dwellings" not in unclassed.binding


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
        # A cellar is not a storey: *Inferieurs au RDC* authorises a level and
        # `permitted_basement_levels` is what counts it. See the group below.
        ({BuildingLevel.BELOW_GROUND}, 0),
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
        investment=UNDISCOUNTED,
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
        investment=UNDISCOUNTED,
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
        investment=UNDISCOUNTED,
        basement_levels_allowed=NO_BASEMENT,
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


# -- the margins, as the second cap on a footprint -------------------------
#
# `lot_buildable_setbacks` subtracts the grid's four margins from the parcel
# and hands the result in as `Lot.buildable_area_m2`. It is independent of
# *Taux d'implantation*: coverage says what share of the lot may be built on,
# the margins say where, and a building satisfies both. Every number below is
# against the same 400 m2 lot the coverage tests use, where 70 per cent is a
# 280 m2 plate - so a buildable area under 280 binds and one over it does not.


def test_the_margins_bind_when_they_leave_less_than_the_coverage_allows():
    # 240 m2 left after the margins against the 280 m2 coverage allows, over
    # the five storeys "Tous sauf le RDC" permits = 1 200 m2 - against 1 400
    # for the same lot with no margins, which is twenty-five one-bedrooms.
    #
    # The 200 m2 that goes is worth more than the four one-bedrooms it looks
    # like: twenty 55.74 m2 one-bedrooms and one 83.61 m2 two-bedroom is
    # 1 198.45 m2 and $10 013.50 a month, where twenty-one one-bedrooms is
    # 1 170.58 m2 and $9 870. At 1 400 the mono-mix wins because 25 of them
    # leave only 6.5 m2 spare; at 1 200 there is room to spend the remainder
    # on the better dwelling instead of the better square foot, which is the
    # trade the module docstring's last two columns describe.
    lot = Lot(area_m2=400.0, frontage_m=12.0, buildable_area_m2=240.0)
    program = solve_program(
        column(density_max=None),
        lot,
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
        basement_levels_allowed=NO_BASEMENT,
    )
    assert program.solved
    assert program.footprint_m2 <= 240.0 + 1e-9
    assert program.footprint_m2 * program.residential_floors <= 1200.0 + 1e-9
    assert program.units == {"1_bedroom": 20, "2_bedroom": 1}
    assert program.net_operating_income == pytest.approx(20 * 470.00 + 613.50)


def test_the_binding_norm_says_margins_and_not_coverage():
    """Which of the two ceilings produced the envelope is the useful half of
    the answer: a coverage cap is argued at the plan, a margin at the lot
    line."""
    lot = Lot(area_m2=400.0, frontage_m=12.0, buildable_area_m2=240.0)
    program = solve_program(
        column(density_max=None), lot, ECONOMICS, parking=NO_PARKING
    )
    assert "setbacks" in program.binding
    assert "site_coverage_max" not in program.binding


def test_a_buildable_area_the_coverage_already_undercuts_changes_nothing():
    """Margins leaving more room than *Taux d'implantation* allows are not the
    binding norm, and must not be reported as one."""
    lot = Lot(area_m2=400.0, frontage_m=12.0, buildable_area_m2=390.0)
    program = solve_program(
        column(density_max=None), lot, ECONOMICS, parking=NO_PARKING
    )
    assert program.units == {"1_bedroom": 25}
    assert "site_coverage_max" in program.binding
    assert "setbacks" not in program.binding


def test_a_lot_with_no_buildable_area_passed_solves_as_it_always_did():
    """The field is optional because it has to be - it is computed from a
    zoning column, so a caller solving a column whose grid states no margins
    has nothing to pass. `None` must leave the answer exactly as it was before
    the setback asset existed."""
    without = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert without.units == {"1_bedroom": 25}
    assert "site_coverage_max" in without.binding
    assert "setbacks" not in without.binding


def test_a_parcel_with_nowhere_to_build_returns_an_empty_program():
    """0 is a real answer - a lot narrower than twice its side margin - and
    the solver should report an empty program for it rather than refuse the
    lot. Against a column with no coverage *minimum*, so what is being tested
    is the empty envelope and not the contradiction below."""
    lot = Lot(area_m2=400.0, frontage_m=12.0, buildable_area_m2=0.0)
    program = solve_program(
        column(density_max=None, site_coverage_min_pct=0.0),
        lot,
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.total_dwellings == 0
    assert program.footprint_m2 == 0.0


def test_margins_leaving_less_than_the_coverage_minimum_demands_are_infeasible():
    """C01-001 requires at least 50 per cent coverage - 200 m2 on this lot -
    and margins leaving 150 make that impossible. Named apart from
    `site_coverage_range` because the column is coherent and it is the parcel
    that cannot satisfy it, which is a different thing to tell a reader."""
    lot = Lot(area_m2=400.0, frontage_m=12.0, buildable_area_m2=150.0)
    program = solve_program(
        column(density_max=None), lot, ECONOMICS, parking=NO_PARKING
    )
    assert not program.solved
    assert program.status == "INFEASIBLE"
    assert program.binding == ("buildable_area_below_site_coverage_min",)


def test_a_negative_buildable_area_is_refused_and_zero_is_not():
    with pytest.raises(ProgramError, match="buildable area"):
        Lot(area_m2=400.0, frontage_m=12.0, buildable_area_m2=-1.0)

    assert Lot(area_m2=400.0, frontage_m=12.0, buildable_area_m2=0.0)


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
        parking=STRUCTURED_ONLY,
        investment=UNDISCOUNTED,
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
    # owe three stalls, and with the yard taken away a second storey is where
    # those go. `STRUCTURED_ONLY` is what makes that the answer - 400 m2 of lot
    # under a 280 m2 plate leaves 120 m2 of yard, four surface stalls, and the
    # solver would otherwise never raise the storey this test is about.
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
        investment=UNDISCOUNTED,
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
        investment=UNDISCOUNTED,
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


def test_a_column_authorising_no_priced_use_is_refused():
    # A pure Commerce or Industrie column solves now; only a column with none
    # of the three priced families - Équipements collectifs, in practice -
    # has no proforma to state.
    with pytest.raises(ProgramError, match="authorises none"):
        solve_program(
            column(usages=("E.1",)), Lot(area_m2=400.0, frontage_m=12.0), ECONOMICS
        )


def test_a_pure_commerce_column_solves_without_a_dwelling():
    # The former no_residential_column gap: a C column is the same model with
    # the dwelling counts pinned at zero. The commerce fills the storeys the
    # level rows spare it, pays its build cost, and owes its stalls.
    program = solve_program(
        column(usages=("C.4",), levels=frozenset({BuildingLevel.ALL}), floors_min=0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.solved
    assert program.total_dwellings == 0
    assert program.residential_floors == 0
    assert program.commercial_floors > 0
    assert program.commercial_area_m2 > 0
    assert program.npv_cad > 0
    # The cellar is in the answer as well: a below-grade shop is floor area
    # *Densite* counts and no storey *En etage* does, so a column whose storeys
    # are spent digs for one more plate. Its rent is the same rate a level
    # down - see `BELOW_GRADE_RENT_DISCOUNT_PCT`.
    assert program.basement_commercial_levels == BASEMENT_LEVELS_ALLOWED
    below_grade = 1.0 - BELOW_GRADE_RENT_DISCOUNT_PCT / 100.0
    assert program.gross_revenue_cad == pytest.approx(
        DEFAULT_NON_RESIDENTIAL.commercial_monthly_revenue(
            program.commercial_area_sqft
        )
        + DEFAULT_NON_RESIDENTIAL.commercial_monthly_revenue(
            program.basement_commercial_area_sqft
        )
        * below_grade
    )


def test_a_pure_industry_column_owes_en_etage_min_from_its_own_storeys():
    # *En étage min* is met by usage storeys on a column with no Habitation at
    # its head - the parking is not allowed to pay it there either.
    program = solve_program(
        column(usages=("I.1",), levels=frozenset({BuildingLevel.ALL}), floors_min=2),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.solved
    assert program.industrial_floors >= 2
    assert program.residential_floors == 0


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
        parking=STRUCTURED_ONLY,
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
        column(density_max=2.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=STRUCTURED_ONLY,
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
        column(density_max=2.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=STRUCTURED_ONLY,
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
        parking=ParkingRules(
            max_underground_levels=0, max_surface_stalls=0, max_garage_stalls=0
        ),
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
        parking=ParkingRules(
            stalls_per_dwelling=2.0, max_surface_stalls=0, max_garage_stalls=0
        ),
        investment=UNDISCOUNTED,
    )
    assert program.total_dwellings == 9
    assert program.underground_stalls == 18
    # However many levels that plate takes: the footprint is not pinned by the
    # objective - any plate big enough to hold the mix is equally optimal - so
    # what is true of the answer is that the hole is exactly deep enough.
    stall_area_m2 = UNDERGROUND_STALL_AREA_SQFT * M2_PER_SQFT
    assert program.underground_levels == math.ceil(
        program.underground_stalls * stall_area_m2 / program.footprint_m2 - 1e-9
    )
    assert program.underground_levels <= 4


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
        {"surface_area_sqft": -1.0},
        {"surface_cost_cad": -1.0},
        {"max_surface_stalls": -1},
        {"garage_area_sqft": -1.0},
        {"garage_cost_cad": -1.0},
        {"max_garage_stalls": -1},
    ],
)
def test_a_parking_rule_that_cannot_be_priced_is_refused(overrides):
    with pytest.raises(ProgramError):
        ParkingRules(**overrides)


# -- the two cheap provisions: the yard and the ground floor ------------------
#
# Everything above is about the two that cost a whole storey or a hole. These
# are about the two that cost neither - a stall on the ground the building does
# not cover, and a bay inside the ground floor it does - about the different
# things that ration them (the parcel, and one plate), about the different caps
# they answer to (neither, and both *Densite* and *Taux d'implantation*), and
# about lot 2 166 060: the parcel that came back with nothing at all while a
# parkade was the only stall the model knew how to buy.


def test_the_surface_stall_is_the_guides_third_montreal_midpoint():
    # Read the same way as the two beside it: `surface_lot`'s mtl
    # [3 960, 8 250], flagged `perStall`, from the Altus Group guide
    # `urban_rag.estimator` ingests. Eight to ten times under either parkade,
    # which is the whole reason a house can afford the stall it owes itself.
    assert SURFACE_STALL_COST_CAD == pytest.approx((3_960 + 8_250) / 2)
    assert UNDERGROUND_STALL_COST_CAD / SURFACE_STALL_COST_CAD > 9.0
    assert ABOVE_GRADE_STALL_COST_CAD / SURFACE_STALL_COST_CAD > 7.0
    # The same rectangle and the same aisle as an above-grade stall. What
    # differs is that nothing is built over it.
    assert SURFACE_STALL_AREA_SQFT == ABOVE_GRADE_STALL_AREA_SQFT


def test_a_lot_with_a_yard_parks_on_it_rather_than_paying_for_structure():
    # The envelope of `test_a_slack_envelope_parks_above_grade_where_it_is_
    # cheaper`, with the yard put back. 70% of 2 000 m2 leaves 600 m2 of
    # ground, twenty-one stalls' worth, so the six the twelve dwellings owe
    # never touch a deck or an excavator - which is that test's answer changed
    # by exactly the option this group is about.
    program = solve_program(
        column(max_dwellings=12, density_max=None),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
    )
    assert program.total_dwellings == 12
    assert program.surface_stalls == 6
    assert program.above_grade_stalls == 0
    assert program.underground_stalls == 0
    assert program.above_grade_parking_floors == 0
    assert program.underground_levels == 0
    assert program.parking_cost_cad == pytest.approx(6 * SURFACE_STALL_COST_CAD)
    # Not a storey and not floor area: the building is dwellings all the way up.
    assert program.floors == program.residential_floors
    assert program.gross_floor_area_m2 == pytest.approx(
        program.footprint_m2 * program.floors
    )


def test_the_yard_runs_out_and_the_garage_takes_over():
    # The same lot with *Taux d'implantation* printing 100/100, which is the
    # one thing that actually takes the ground away: a coverage *maximum* of
    # 100 only permits the whole parcel to be covered, and the solver would
    # still choose a smaller plate and park on what is left. A minimum of 100
    # obliges it. With no yard the next-cheapest provision is the ground floor
    # itself, so the twelve dwellings get bays rather than a deck - $15 450
    # against $48 125, and no storey spent either way.
    program = solve_program(
        column(
            max_dwellings=12,
            density_max=None,
            site_coverage_min_pct=100.0,
            site_coverage_max_pct=100.0,
        ),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
    )
    assert program.total_dwellings == 12
    assert program.footprint_m2 == pytest.approx(2000.0)
    assert program.surface_stalls == 0
    assert program.garage_stalls == 6
    assert program.above_grade_stalls == 0
    assert program.underground_stalls == 0
    # A garage is not a storey: the building is dwellings all the way up.
    assert program.above_grade_parking_floors == 0
    assert program.floors == program.residential_floors
    assert program.parking_cost_cad == pytest.approx(6 * GARAGE_STALL_COST_CAD)


def test_and_when_the_ground_floor_is_spoken_for_too_the_deck_takes_over():
    # The last rung. Same envelope, same parcel, and both cheap provisions
    # closed: what is left is the storey of stalls the module has always had.
    program = solve_program(
        column(
            max_dwellings=12,
            density_max=None,
            site_coverage_min_pct=100.0,
            site_coverage_max_pct=100.0,
        ),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
        parking=STRUCTURED_ONLY,
    )
    assert program.total_dwellings == 12
    assert program.surface_stalls == 0
    assert program.garage_stalls == 0
    assert program.above_grade_stalls == 6
    assert program.above_grade_parking_floors == 1


def test_the_yard_is_the_parcel_less_the_plate_and_nothing_else():
    # The land rations it, so the count is arithmetic rather than a preference.
    # *Taux d'implantation* printing 60/60 pins the plate at 180 m2 of a 300 m2
    # lot, so the yard is exactly 120 m2 and a stall is 300 sq ft - 27.87 m2 -
    # so four fit and the fifth does not. Two stalls a dwelling asks for ten
    # against a five-dwelling ceiling, so the yard is filled first and the
    # other six go into structure rather than being left unprovided.
    lot = Lot(area_m2=300.0, frontage_m=12.0)
    program = solve_program(
        column(
            max_dwellings=5,
            density_max=None,
            site_coverage_min_pct=60.0,
            site_coverage_max_pct=60.0,
        ),
        lot,
        ECONOMICS,
        parking=ParkingRules(stalls_per_dwelling=2.0),
        investment=UNDISCOUNTED,
    )
    assert program.solved
    assert program.footprint_m2 == pytest.approx(180.0)
    stall_m2 = SURFACE_STALL_AREA_SQFT * M2_PER_SQFT
    assert program.surface_stalls == 4
    assert program.surface_stalls * stall_m2 <= 120.0
    assert 5 * stall_m2 > 120.0
    # Every stall the dwellings owe is still provided; the yard only decides
    # how many of them were cheap.
    assert program.total_stalls == 2 * program.total_dwellings
    assert program.total_stalls > program.surface_stalls


def test_no_parking_owes_no_surface_stalls_either():
    program = solve_program(
        column(max_dwellings=12, density_max=None),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.surface_stalls == 0
    assert program.garage_stalls == 0
    assert program.total_stalls == 0
    assert program.parking_cost_cad == 0.0


def test_the_garage_rate_is_derived_from_the_modules_own_build_rate():
    # The one parking rate the guide does not publish. A parkade is a parking
    # *structure*; a bay in a house being built anyway is a shell, and the
    # module prices it as one - `GARAGE_SHELL_FRACTION` of finished residential
    # space over a stall's area. Pinned so a change to the guide's residential
    # rate cannot quietly leave this one stale.
    assert GARAGE_STALL_COST_CAD == pytest.approx(
        RESIDENTIAL_COST_PER_SQFT_CAD * GARAGE_SHELL_FRACTION * GARAGE_STALL_AREA_SQFT
    )
    # And where it sits is what decides the answers: dearer than asphalt, well
    # under either structure.
    assert (
        SURFACE_STALL_COST_CAD
        < GARAGE_STALL_COST_CAD
        < ABOVE_GRADE_STALL_COST_CAD
        < UNDERGROUND_STALL_COST_CAD
    )


def test_a_bay_is_floor_area_the_density_cap_counts():
    # The difference between a garage and a stall on the yard, in one envelope.
    # *Densité* 1,0 on a 600 m2 lot is 600 m2 of superficie de plancher, and
    # the garage is inside it: the bays and the dwellings come out of the same
    # 600, so a bay is a square metre of housing given up. The stall on the
    # yard is not in the cap at all.
    lot = Lot(area_m2=600.0, frontage_m=18.0)
    grid = column(density_max=1.0, density_min=0.0, site_coverage_max_pct=100.0)

    garage = solve_program(
        grid, lot, ECONOMICS,
        parking=ParkingRules(max_surface_stalls=0, max_underground_levels=0),
    )
    assert garage.garage_stalls > 0
    # Every square metre of it is inside the density cap and outside the
    # dwellings: the plates hold the units *and* the bays, exactly.
    assert garage.gross_floor_area_m2 <= 600.0 + 1e-9
    assert garage.unit_area_m2 + garage.garage_area_m2 <= (
        garage.footprint_m2 * garage.residential_floors + 1e-9
    )
    # At the hundredth of a square metre the model holds areas in - a 300 sq ft
    # bay is 27.870912 m2 nominally and 27.87 m2 as the solver reserved it.
    assert garage.garage_area_m2 == pytest.approx(
        garage.garage_stalls * GARAGE_STALL_AREA_SQFT * M2_PER_SQFT, abs=0.01
    )


def test_a_bay_is_never_more_than_the_ground_floor_holds():
    # A garage is one storey of bays. Past that it is the deck the module
    # already had, so the cap is a plate - and a building needing more takes
    # both rather than a taller garage.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(
        column(density_max=None),
        lot,
        ECONOMICS,
        parking=ParkingRules(stalls_per_dwelling=2.0, max_surface_stalls=0),
        investment=UNDISCOUNTED,
    )
    assert program.solved
    assert program.garage_stalls > 0
    assert program.garage_area_m2 <= program.footprint_m2 + 1e-9


def test_the_four_provisions_combine_to_meet_one_demand():
    # They are not alternatives: the building owes a single number of stalls
    # and fills it from wherever is cheapest at the margin, so a tight envelope
    # with a heavy ratio uses more than one of them at once. Two stalls a
    # dwelling on a 60%-covered lot spends the yard first, then the ground
    # floor, then whatever structure is left.
    lot = Lot(area_m2=900.0, frontage_m=25.0)
    program = solve_program(
        column(
            density_max=None,
            site_coverage_min_pct=60.0,
            site_coverage_max_pct=60.0,
        ),
        lot,
        ECONOMICS,
        parking=ParkingRules(stalls_per_dwelling=2.0),
        investment=UNDISCOUNTED,
    )
    assert program.solved
    kinds = [
        program.surface_stalls,
        program.garage_stalls,
        program.above_grade_stalls,
        program.underground_stalls,
    ]
    assert sum(kinds) == program.total_stalls
    assert program.total_stalls == 2 * program.total_dwellings
    # More than one kind is in use, which is the point of the word "combined".
    assert sum(1 for n in kinds if n > 0) >= 2
    # And each is inside the space that rations it.
    assert program.surface_stalls * SURFACE_STALL_AREA_SQFT * M2_PER_SQFT <= (
        lot.area_m2 - program.footprint_m2 + 1e-9
    )
    assert program.garage_area_m2 <= program.footprint_m2 + 1e-9


# -- lot 2 166 060 -----------------------------------------------------------


#: Zone H03-085 of Villeray-Saint-Michel-Parc-Extension, as
#: `silver.lot_zoning_envelopes` holds it: one *Habitation* column headed
#: ``H.1``, marked *Tous les niveaux*, *En étage* 1/1, *Hauteur en mètre* 0/9,
#: *Taux d'implantation* 0/35, and no *Densité* and no *Nombre de logements
#: maximal* printed - H.1 is the single-family class and the by-law has already
#: fixed the number at one, which is why the row is blank.
H03_085 = ZoneColumn(
    usages=("H.1",),
    levels=frozenset({BuildingLevel.ALL}),
    floors_min=1,
    floors_max=1,
    height_min_m=0.0,
    height_max_m=9.0,
    min_lot_width_m=15.0,
    site_coverage_min_pct=0.0,
    site_coverage_max_pct=35.0,
    zone="H03-085",
)

#: The parcel itself, from the same table: 741.52 m2 with 17.6 m on 8e Avenue,
#: comfortably over the column's 15 m minimum, and 486.95 m2 left once the
#: grid's margins (5 m front, 1.5 m side, 3 m rear) are taken off.
LOT_2166060 = Lot(
    area_m2=741.5225112438202,
    frontage_m=17.605170585545242,
    lot_number="2 166 060",
    buildable_area_m2=486.9537979756411,
)


def test_a_single_family_envelope_is_not_an_empty_one():
    """Lot 2 166 060: one house, not "zoning permits nothing".

    The regression this group exists for. H.1 caps the parcel at one dwelling
    and *En étage* 1/1 at one storey, and *En étage min* is owed by the usage
    storeys, so the single permitted storey is the dwelling's own and an
    above-grade deck has nowhere to stand. That left digging as the only
    provision the model knew: one dwelling is worth about $50 000 net and an
    underground stall costs $60 300, so the maximum of the objective was to
    build nothing, and a parcel whose grid plainly permits a house came back
    as 0 m2, 0 dwellings, `OPTIMAL`, and an empty `binding` - which downstream
    is indistinguishable from a column that permits nothing at all.

    The stall it actually owes is a driveway. 35% of 741.52 m2 is a 259.53 m2
    plate and the rest of the parcel is yard, so there is room for it several
    times over.
    """
    program = solve_program(H03_085, LOT_2166060, ECONOMICS)

    assert program.status == "OPTIMAL"
    assert program.total_dwellings == 1
    assert program.gross_floor_area_m2 > 0.0
    assert program.npv_cad > 0.0

    # One storey, and the height row leaves room for three - so it is *En
    # étage*, not *Hauteur*, that made this a bungalow.
    assert program.floors == 1
    assert program.residential_floors == 1
    assert program.height_m == pytest.approx(3.0)

    # The stall is on the ground, which on a 741 m2 parcel covered to 35% is
    # the cheapest of the four and the one a house on a lot this size uses.
    assert program.surface_stalls == 1
    assert program.garage_stalls == 0
    assert program.underground_stalls == 0
    assert program.above_grade_stalls == 0
    assert program.underground_levels == 0
    assert program.above_grade_parking_floors == 0
    assert program.parking_cost_cad == pytest.approx(SURFACE_STALL_COST_CAD)

    # The plate stays inside both ceilings: *Taux d'implantation* on the
    # parcel, and the margins on where it may sit.
    assert program.footprint_m2 <= 741.5225112438202 * 0.35 + 1e-9
    assert program.footprint_m2 <= 486.9537979756411 + 1e-9

    # And the reason it is one dwelling is the class, which is the answer the
    # report should give: H.1 is the single-family class.
    assert program.binding == ("max_dwellings",)


def test_the_same_house_puts_the_bay_indoors_when_the_yard_is_taken_away():
    """The second rung, on the parcel that needed the first.

    With no yard the house does what a house on a tight lot does: it puts the
    garage in its own ground floor and pays for it twice - $15 450, and the
    floor area the bay takes out of the plate. The plate grows by exactly a
    bay to keep the dwelling whole, which is the arithmetic of "counts towards
    the floor space ratio and the implantation": both caps see it, and here
    both have room.
    """
    program = solve_program(
        H03_085,
        LOT_2166060,
        ECONOMICS,
        parking=ParkingRules(max_surface_stalls=0),
    )
    assert program.total_dwellings == 1
    assert program.garage_stalls == 1
    assert program.surface_stalls == 0
    assert program.underground_stalls == 0
    assert program.above_grade_stalls == 0
    # Not a storey: still the single storey *En étage* allows.
    assert program.floors == 1
    assert program.above_grade_parking_floors == 0
    # Floor area, though - the plate carries the dwelling and the bay both,
    # and is bigger than the yard answer's by exactly one bay.
    # Compared at `AREA_SCALE`'s hundredth: the bay is 27.870912 m2 nominally
    # and 27.87 m2 as the solver reserved it.
    bay_m2 = GARAGE_STALL_AREA_SQFT * M2_PER_SQFT
    assert program.garage_area_m2 == pytest.approx(bay_m2, abs=0.01)
    assert program.footprint_m2 == pytest.approx(
        program.unit_area_m2 + program.garage_area_m2
    )
    assert program.gross_floor_area_m2 == pytest.approx(program.footprint_m2)
    # And both caps still hold.
    assert program.footprint_m2 <= 741.5225112438202 * 0.35 + 1e-9
    assert program.parking_cost_cad == pytest.approx(GARAGE_STALL_COST_CAD)


def test_the_same_envelope_builds_nothing_with_only_the_structured_options():
    """Which is the whole of what the two cheap provisions changed.

    Same column, same parcel, `STRUCTURED_ONLY`: no yard and no bays, so the
    building has nowhere to put the stall it owes but under itself, and one
    dwelling does not earn back a $60 300 parkade. This is the answer the
    module gave before, kept as a test so the diagnosis stays attached to the
    fix - the envelope was never empty, the two provisions a house actually
    uses were simply missing from it.
    """
    program = solve_program(
        H03_085, LOT_2166060, ECONOMICS, parking=STRUCTURED_ONLY
    )
    assert program.status == "OPTIMAL"
    assert program.total_dwellings == 0
    assert program.gross_floor_area_m2 == 0.0

    # And it says so, rather than leaving a reader to read the zeros as a
    # zoning column that permits nothing.
    assert program.binding == ("nothing_pencils",)


def test_an_optimal_solve_that_builds_nothing_says_so():
    # The same reporting rule reached the other way: rents too low to cover the
    # build on an envelope with room to spare. No printed cap is stopping this,
    # so naming one would name the wrong culprit.
    program = solve_program(
        column(),
        Lot(area_m2=650.0, frontage_m=18.0),
        UnitEconomics(average_rent_cad={"1_bedroom": 50.0}, vacancy_rate_pct={}),
        parking=NO_PARKING,
    )
    assert program.solved
    assert program.units == {}
    assert program.binding == ("nothing_pencils",)


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
        investment=UNDISCOUNTED,
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
        parking=STRUCTURED_ONLY,
        investment=UNDISCOUNTED,
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
    wood = solve_program(column(), lot, ECONOMICS, investment=UNDISCOUNTED)
    concrete = solve_program(
        column(),
        lot,
        ECONOMICS,
        construction=ConstructionCosts(residential_cost_per_sqft=305.0),
        investment=UNDISCOUNTED,
    )
    assert wood.net_operating_income > concrete.net_operating_income
    # Per square foot of dwelling rather than in total, because the two are no
    # longer the same building: at the wood rate the sous-sol pays for itself
    # and the answer takes a sixth plate below grade, at the concrete rate it
    # does not and the answer buys a parking deck instead. What the rate moved
    # is the price of a square foot, so that is what is compared.
    assert wood.construction_cost_cad / wood.unit_area_m2 < (
        concrete.construction_cost_cad / concrete.unit_area_m2
    )


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

    Bare ``H`` rather than a numbered class for the same reason, one norm
    over: ``H.2`` carries a ceiling of two dwellings
    (`RESIDENTIAL_CLASS_MAX_DWELLINGS`), and a test about which usage takes a
    storey should not also be a test about how many dwellings fit on it. The
    class ceiling has its own tests below.
    """
    return column(
        usages=("H", *extra_usages),
        levels=frozenset({BuildingLevel.ALL}),
        **overrides,
    )


def test_two_columns_of_a_zone_are_one_building():
    """The whole point of `ZoneEnvelope`: a zone printing its housing in one
    column and its commerce in another authorises both in one building, and
    the answer must be the same as if the grid had printed them together."""
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    together = solve_program(
        mixed("C.2"), lot, ECONOMICS, parking=NO_PARKING
    )
    apart = solve_program(
        ZoneEnvelope.of(
            [
                column(usages=("H",), levels=frozenset({BuildingLevel.ALL})),
                column(usages=("C.2",), levels=frozenset({BuildingLevel.ALL})),
            ],
            12.0,
        ),
        lot,
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert apart.solved
    assert apart.commercial_floors == together.commercial_floors
    assert apart.residential_floors == together.residential_floors
    assert apart.npv_cad == pytest.approx(together.npv_cad)


def test_a_mixed_building_meets_the_stricter_of_the_two_columns():
    """The reading this module takes where two columns disagree: each one's
    norms bind the building while the family it heads is built, so a mix
    answers to the intersection and a pure program answers to its own column
    alone."""
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    housing = column(
        usages=("H",), levels=frozenset({BuildingLevel.ALL}), floors_max=3
    )
    commerce = column(
        usages=("C.2",), levels=frozenset({BuildingLevel.ALL}), floors_max=6
    )
    envelope = ZoneEnvelope.of([housing, commerce], 12.0)

    # Commerce outearns housing here, so the solver takes the six storeys its
    # own column allows and builds no dwelling - the H column's three do not
    # bind a building with no housing in it.
    program = solve_program(envelope, lot, ECONOMICS, parking=NO_PARKING)
    assert program.residential_floors == 0
    assert program.floors == 6

    # Force the mix by making the commerce worthless above the ground floor:
    # the C column may then take one storey, the H column's cap of three binds
    # the whole building, and two storeys of housing sit on the shop.
    grade_only = column(
        usages=("C.2",), levels=frozenset({BuildingLevel.GROUND}), floors_max=6
    )
    mixed_program = solve_program(
        ZoneEnvelope.of([housing, grade_only], 12.0),
        lot,
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert mixed_program.commercial_floors == 1
    assert mixed_program.residential_floors == 2
    # Three, not six: the housing is built, so the H column's ceiling applies.
    assert mixed_program.floors == 3


def test_an_envelope_that_authorises_nothing_is_refused():
    with pytest.raises(ProgramError, match="Equipements"):
        solve_program(
            ZoneEnvelope(), Lot(area_m2=400.0, frontage_m=12.0), ECONOMICS
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
    # commerce takes every storey *En etage max* allows - all six.
    #
    # *En etage min* holds none of them back for the dwellings: the minimum is
    # owed by the usage storeys between them and six of commerce pay it, which
    # is what "the mix is a decision" means. The module reserved two for the
    # housing before envelopes existed, and that reservation was the model
    # choosing a mix rather than solving for one.
    program = solve_program(
        mixed("C.2"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.solved
    assert program.commercial_floors == 6
    assert program.residential_floors == 0
    assert program.floors == 6
    assert program.industrial_floors == 0
    # And the cellar under them, which *En etage max* does not ration: six
    # storeys is every storey the grid allows and the seventh plate is below
    # grade. `density_max` is then what stops the seventh from being an
    # eighth - 400 x 4,5 = 1 800 m2 over seven plates is a 257,14 m2 footprint.
    assert program.basement_commercial_levels == BASEMENT_LEVELS_ALLOWED
    assert program.density_floor_area_m2 == pytest.approx(1800.0, abs=0.05)
    assert set(program.binding) == {
        "density_max",
        "commercial_floor_area",
        "basement_levels",
    }


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
    assert program.industrial_floors == 6
    assert program.commercial_floors == 0
    assert program.binding == ("industrial_floor_area",)


def test_commerce_beats_industry_where_both_are_authorised():
    program = solve_program(
        mixed("C.2", "I.1"),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.commercial_floors == 6
    assert program.industrial_floors == 0


def test_the_non_residential_storeys_are_floor_area_the_density_cap_sees():
    # Unlike an underground stall, a retail floor is *superficie de plancher* -
    # and so is a retail *cellar*, which is the half of article 38 1° that is
    # about what the level holds rather than how deep it is. So the cap is
    # tested against `density_floor_area_m2`, and `gross_floor_area_m2` beside
    # it stays the above-grade figure a massing extrudes.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(mixed("C.2"), lot, ECONOMICS, parking=NO_PARKING)
    assert program.density_floor_area_m2 <= 400.0 * 4.5 + 1e-9
    assert program.commercial_area_m2 > 0.0
    assert program.gross_floor_area_m2 == pytest.approx(
        program.footprint_m2 * program.floors
    )
    assert program.density_floor_area_m2 == pytest.approx(
        program.footprint_m2 * (program.floors + program.basement_levels)
    )
    assert program.non_residential_area_m2 == pytest.approx(
        program.footprint_m2
        * (program.commercial_floors + program.basement_commercial_levels)
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
        investment=UNDISCOUNTED,
    )
    sqft = program.commercial_area_sqft
    cellar_sqft = program.basement_commercial_area_sqft
    # The cellar is the same rate with the premium on it, and it is charged to
    # the same column: `commercial_cost_cad` is the commerce in this building,
    # wherever in it that commerce stands.
    assert program.commercial_cost_cad == pytest.approx(
        sqft * COMMERCIAL_COST_PER_SQFT_CAD
        + cellar_sqft * COMMERCIAL_COST_PER_SQFT_CAD * (1 + BELOW_GRADE_COST_PREMIUM)
    )
    # The rate is annual per square foot, the objective is a month, and the
    # vacancy comes off it exactly as a dwelling's does. One level down the
    # rent comes off again, at `BELOW_GRADE_RENT_DISCOUNT_PCT`.
    per_sqft_month = (
        COMMERCIAL_REVENUE_PER_SQFT_CAD
        / MONTHS_PER_YEAR
        * (1 - COMMERCIAL_VACANCY_PCT / 100.0)
    )
    commercial_rent = per_sqft_month * (
        sqft + cellar_sqft * (1 - BELOW_GRADE_RENT_DISCOUNT_PCT / 100.0)
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
    # *En etage max* is the ceiling on the stack whatever fills it, so a column
    # printing a tighter maximum spares the commerce fewer storeys - three
    # here rather than the six of the tests above.
    program = solve_program(
        mixed("C.2", floors_max=3),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.floors == 3
    assert program.residential_floors == 0
    assert program.commercial_floors == 3


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
    assert program.commercial_floors == 5
    assert program.residential_floors == 0


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
        investment=UNDISCOUNTED,
    )
    dwellings = sum(
        RENTS[unit_type] * (1 - VACANCY[unit_type] / 100.0) * quantity
        for unit_type, quantity in program.units.items()
    )
    commerce = (
        (
            program.commercial_area_sqft
            + program.basement_commercial_area_sqft
            * (1 - BELOW_GRADE_RENT_DISCOUNT_PCT / 100.0)
        )
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
    # The cellar owes them too: what generates a trip is a shop, not a storey,
    # and the by-law exclusion that spares a below-grade *stall* has nothing to
    # say about the floor area above it.
    owed = math.ceil(
        STALLS_PER_1000_SQFT
        * (program.commercial_area_sqft + program.basement_commercial_area_sqft)
        / 1000.0
    )
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
        + STALLS_PER_1000_SQFT
        * (program.commercial_area_sqft + program.basement_commercial_area_sqft)
        / 1000.0
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
    # Three storeys, so the stalls the retail owes stay inside what six
    # underground levels can hold: 3 x 280 m2 is 9 042 sq ft and 28 stalls
    # against a capacity of 45. Asked for more commerce than the digging can
    # serve the answer is a mix of both, which is a different question - this
    # one is about which the solver reaches for first.
    program = solve_program(
        mixed("C.2", floors_max=3),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=ParkingRules(stalls_per_dwelling=0.0),
    )
    assert program.commercial_floors == 3
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


# -- the metric cap ---------------------------------------------------------
#
# *En etage* counts storeys and *Hauteur en metre* measures them, so a grid
# printing both states two ceilings on one stack. The heights are stated
# assumptions rather than norms - 3 m a dwelling storey, 4 m a commercial or
# industrial one, 3 m an above-grade deck, nothing at all below grade - which
# is why every number below is arrived at by multiplying one of them by a
# storey count the test also asserts.


def test_the_storey_heights_are_the_ones_the_module_was_specified_with():
    assert RESIDENTIAL_STOREY_HEIGHT_M == 3.0
    assert COMMERCIAL_STOREY_HEIGHT_M == 4.0
    assert INDUSTRIAL_STOREY_HEIGHT_M == 4.0
    assert ABOVE_GRADE_PARKING_STOREY_HEIGHT_M == 3.0
    assert UNDERGROUND_LEVEL_HEIGHT_M == 0.0
    assert DEFAULT_STOREY_HEIGHTS.residential_m == RESIDENTIAL_STOREY_HEIGHT_M
    assert DEFAULT_STOREY_HEIGHTS.commercial_m == COMMERCIAL_STOREY_HEIGHT_M


def test_a_column_printing_no_metric_cap_is_unchanged_by_one():
    # The row is optional like every other, and a grid printing "-" for it must
    # give back exactly the answer this module gave before the row existed.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(column(), lot, ECONOMICS, parking=NO_PARKING)
    assert program.residential_floors == 5
    assert program.units == {"1_bedroom": 25}
    assert "height_max" not in program.binding
    # Reported all the same: five storeys of housing stand fifteen metres.
    assert program.height_m == pytest.approx(15.0)


def test_a_metric_cap_is_read_as_the_storeys_it_leaves_room_for():
    # 11 m at three metres a dwelling storey is three of them and not the five
    # "Tous sauf le RDC" allows of a six-storey building - 12 m would be four.
    # Three storeys of 280 m2 is 840 m2, which is fifteen 55.74 m2
    # one-bedrooms with 3.9 m2 left over.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(
        column(height_max_m=11.0), lot, ECONOMICS, parking=NO_PARKING
    )
    assert program.residential_floors == 3
    assert program.height_m == pytest.approx(9.0)
    assert program.units == {"1_bedroom": 15}
    # Both ceilings are named: the envelope the dwellings filled was built from
    # the storeys *Hauteur* left room for, not the ones *En etage* prints.
    assert {"site_coverage_max", "floors", "height_max"} <= set(program.binding)


def test_the_slacker_of_the_two_ceilings_is_not_the_one_that_binds():
    # 18 m is six storeys of housing and the level rows allow five, so the
    # metric cap is not what stopped the building and does not claim to be.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    program = solve_program(
        column(height_max_m=18.0), lot, ECONOMICS, parking=NO_PARKING
    )
    assert program.residential_floors == 5
    assert program.units == {"1_bedroom": 25}
    assert "height_max" not in program.binding


def test_a_commercial_storey_spends_four_metres_of_the_cap_and_housing_three():
    # The whole of what a metric cap adds to a storey cap: the same metres buy
    # different numbers of storeys depending on what fills them. The commerce
    # outbids the housing for every storey here, so a metric cap is spent at
    # four metres a plate - six of them need 24 m, and an 18 m limit buys four
    # while a 14 m limit buys three. Neither is a number *En etage* could have
    # produced, which is the point.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    expected = {None: (6, 24.0), 18.0: (4, 16.0), 14.0: (3, 12.0)}
    for cap, (commercial_floors, height_m) in expected.items():
        program = solve_program(
            mixed("C.2", height_max_m=cap), lot, ECONOMICS, parking=NO_PARKING
        )
        assert program.commercial_floors == commercial_floors
        assert program.height_m == pytest.approx(height_m)
        if cap is not None:
            assert program.height_m <= cap + 1e-9
        assert program.floors == commercial_floors

    # And the other half of the comparison, on the identical caps: a column
    # with no commerce at its head spends the same metres three at a time, so
    # 18 m stands six storeys of housing where it stood four of retail.
    housing = solve_program(
        column(levels=frozenset({BuildingLevel.ALL}), height_max_m=18.0),
        lot,
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert housing.residential_floors == 6
    assert housing.height_m == pytest.approx(18.0)


def test_an_underground_level_stands_no_metres():
    # The other half of article 38 1o's bargain, arriving from a different
    # article: height is measured from grade up, so what is dug is outside the
    # measurement exactly as it is outside the *superficie de plancher*. The
    # levels are not asserted - see the parking tests on why they are a tie -
    # but whatever they are, they are worth nothing in metres.
    program = solve_program(
        column(density_max=2.0, height_max_m=18.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=STRUCTURED_ONLY,
    )
    assert program.underground_levels >= 1
    assert program.underground_area_m2 > 0.0
    assert program.above_grade_parking_floors == 0
    assert program.height_m == pytest.approx(3.0 * program.residential_floors)


def test_an_above_grade_deck_is_measured_and_a_dug_one_is_not():
    # The same lot with the excavator taken away: the stalls go on a storey of
    # their own, and that storey is three metres of the building's height where
    # the levels dug beside it were none.
    program = solve_program(
        column(density_max=2.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=ParkingRules(
            max_underground_levels=0, max_surface_stalls=0, max_garage_stalls=0
        ),
    )
    assert program.above_grade_parking_floors == 1
    assert program.underground_levels == 0
    assert program.height_m == pytest.approx(3.0 * program.floors)


def test_the_reported_height_is_the_storey_split_priced():
    program = solve_program(
        mixed("C.2", "I.1", height_max_m=20.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
    )
    assert program.solved
    assert program.height_m == pytest.approx(
        DEFAULT_STOREY_HEIGHTS.height_m(
            residential=program.residential_floors,
            above_grade_parking=program.above_grade_parking_floors,
            commercial=program.commercial_floors,
            industrial=program.industrial_floors,
        )
    )
    assert program.height_m <= 20.0 + 1e-9


def test_taller_storeys_get_fewer_of_them_out_of_the_same_metres():
    # The heights are assumptions, and a building known to be built otherwise
    # says so at the call site. 18 m is six three-metre storeys and three
    # five-metre ones, and the level rows allow five either way.
    program = solve_program(
        column(height_max_m=18.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        heights=StoreyHeights(residential_m=5.0),
    )
    assert program.residential_floors == 3
    assert program.height_m == pytest.approx(15.0)
    assert "height_max" in program.binding


def test_a_metric_minimum_above_the_metric_maximum_is_infeasible():
    # Two rows of one column contradicting each other, named rather than
    # returned as a bare status - the treatment `site_coverage_range` gets.
    program = solve_program(
        column(height_min_m=12.0, height_max_m=6.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
    )
    assert not program.solved
    assert program.binding == ("height_range",)


def test_a_storey_minimum_the_metric_cap_cannot_reach_is_infeasible():
    # *En etage min 2* is six metres of housing, and this column allows five.
    # Named apart from the level-row contradiction because it is a different
    # pair of rows disagreeing, and because a shorter storey would resolve it.
    program = solve_program(
        column(height_max_m=5.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
    )
    assert not program.solved
    assert program.binding == ("height_max_below_floors_min",)


def test_a_metric_minimum_no_permitted_stack_reaches_is_infeasible():
    # Five storeys of housing is fifteen metres and this column authorises
    # nothing else, so a twenty-metre minimum has no program. A real answer
    # about the column, like a *Densite min* nothing satisfies.
    program = solve_program(
        column(height_min_m=20.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert not program.solved
    assert program.status == "INFEASIBLE"


def test_a_metric_minimum_the_envelope_can_reach_is_met():
    # A minimum is a floor, so a building exactly as tall as one meets it.
    program = solve_program(
        column(height_min_m=15.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.solved
    assert program.height_m >= 15.0 - 1e-9
    assert program.residential_floors == 5


@pytest.mark.parametrize(
    "overrides",
    [
        {"residential_m": 0.0},
        {"commercial_m": -3.0},
        {"industrial_m": 0.0},
        {"above_grade_parking_m": 0.0},
    ],
)
def test_a_storey_of_no_height_is_refused(overrides):
    # A storey the metric cap never charges for is one the solver would stack
    # without limit under any *Hauteur* at all. Taking the cap off is what a
    # column printing no maximum is for.
    with pytest.raises(ProgramError):
        StoreyHeights(**overrides)


@pytest.mark.parametrize("norm", ["height_min_m", "height_max_m"])
def test_a_negative_metric_norm_is_refused(norm):
    with pytest.raises(ProgramError):
        column(**{norm: -1.0})


# -- the cellar --------------------------------------------------------------
#
# A sous-sol is the one plate in the model that answers to a single cap. *En
# etage* counts storeys and a below-grade level is not one; *Hauteur en metre*
# is measured from grade up and a below-grade level stands under it; *Densite*
# is computed on the *superficie de plancher*, from which article 38 1 removes
# a below-grade **stall and its ramp** and nothing else. So a cellar of
# dwellings is floor area and the parkade beneath it is not, and the two are
# the same hole - which is what this group is about, one cap at a time.
#
# The footprint is not a second decision: the basement is flat under the
# building, so *Taux d'implantation* has already said everything it has to say
# about it. `BASEMENT_LEVELS_ALLOWED` is one cellar, and
# `basement_levels_allowed=NO_BASEMENT` is how the tests above ask for the
# building this module solved before the cellar was in it.


@pytest.mark.parametrize(
    ("levels", "expected"),
    [
        # The row that names the level outright.
        ({BuildingLevel.BELOW_GROUND}, BASEMENT_LEVELS_ALLOWED),
        # And the two blanket rows, read as covering it: "every level" and
        # "every level but the RDC" both include the one below.
        ({BuildingLevel.ALL}, BASEMENT_LEVELS_ALLOWED),
        ({BuildingLevel.ALL_EXCEPT_GROUND}, BASEMENT_LEVELS_ALLOWED),
        # The rows naming a level above grade authorise nothing under it.
        ({BuildingLevel.GROUND}, 0),
        ({BuildingLevel.SECOND}, 0),
        (set(), 0),
        # Two rows that both reach the basement are one cellar and not two -
        # the same "covers the building exactly once" that caps
        # `permitted_floors`.
        ({BuildingLevel.GROUND, BuildingLevel.BELOW_GROUND}, BASEMENT_LEVELS_ALLOWED),
    ],
)
def test_the_level_rows_that_authorise_a_cellar(levels, expected):
    assert permitted_basement_levels(levels) == expected


def test_a_column_and_a_zone_report_the_cellar_they_allow():
    """The accessors beside `permitted_floors_count`, read at the same grain:
    per column, and loosest across a zone's governing columns."""
    # Two families, because `ZoneEnvelope.of` governs one column per family -
    # the housing has no cellar and the commerce beside it does.
    housing = column(levels=frozenset({BuildingLevel.GROUND}))
    shop = column(usages=("C.2",), levels=frozenset({BuildingLevel.ALL}))
    assert shop.permitted_basement_levels == BASEMENT_LEVELS_ALLOWED
    assert housing.permitted_basement_levels == 0
    assert ZoneEnvelope.single(housing).permitted_basement_levels == 0
    # The loosest across the governing columns: it sizes a domain, and what
    # binds a family is its own column's allowance.
    assert (
        ZoneEnvelope.of([housing, shop], frontage_m=12.0).permitted_basement_levels
        == BASEMENT_LEVELS_ALLOWED
    )


def test_the_rows_that_reach_the_basement_are_the_documented_set():
    """`BASEMENT_LEVELS` is the reading, in one place, so it can be narrowed."""
    assert BASEMENT_LEVELS == {
        BuildingLevel.BELOW_GROUND,
        BuildingLevel.ALL,
        BuildingLevel.ALL_EXCEPT_GROUND,
    }


def test_a_cellar_is_not_a_storey_and_stands_no_metres():
    # The whole of the difference, on one envelope: the same column solved with
    # the cellar and without it. Both storey caps see the same building either
    # way, and *Densite* sees one plate more.
    lot = Lot(area_m2=400.0, frontage_m=12.0)
    above_only = solve_program(
        column(density_max=None),
        lot,
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
        basement_levels_allowed=NO_BASEMENT,
    )
    with_cellar = solve_program(
        column(density_max=None),
        lot,
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
    )
    assert with_cellar.basement_residential_levels == BASEMENT_LEVELS_ALLOWED
    # Neither storey cap moved.
    assert with_cellar.floors == above_only.floors
    assert with_cellar.residential_floors == above_only.residential_floors
    assert with_cellar.height_m == pytest.approx(above_only.height_m)
    # `gross_floor_area_m2` is above grade and did not move either; what grew
    # is the floor area the density index is computed on.
    assert with_cellar.gross_floor_area_m2 == pytest.approx(
        above_only.gross_floor_area_m2
    )
    assert with_cellar.density_floor_area_m2 == pytest.approx(
        with_cellar.gross_floor_area_m2 + with_cellar.footprint_m2
    )
    assert with_cellar.total_dwellings > above_only.total_dwellings


def test_the_cellar_is_the_plate_above_it_and_moves_no_footprint():
    """Flat under the rest of the building: one footprint, and *Taux
    d'implantation* has nothing further to say about the basement."""
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
    )
    assert program.footprint_m2 <= 400.0 * 0.70 + 1e-9
    assert program.basement_residential_area_m2 == pytest.approx(
        program.footprint_m2 * program.basement_residential_levels
    )
    assert program.basement_area_m2 == pytest.approx(
        program.basement_residential_area_m2
    )


def test_the_density_cap_counts_the_cellar_and_not_the_parkade_under_it():
    # The two halves of article 38 1 in one answer: a dug *stall* is outside
    # the *superficie de plancher* and a dug *dwelling* is inside it. Both are
    # below grade, and only one of them is charged.
    program = solve_program(
        column(density_max=2.0),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=STRUCTURED_ONLY,
        investment=UNDISCOUNTED,
    )
    assert program.solved
    assert program.underground_stalls > 0
    assert program.underground_area_m2 > 0.0
    assert program.density_floor_area_m2 <= 800.0 + 1e-9
    assert program.density_floor_area_m2 == pytest.approx(
        program.gross_floor_area_m2 + program.basement_area_m2
    )


def test_a_column_that_authorises_no_level_below_the_rdc_digs_no_cellar():
    program = solve_program(
        column(
            levels=frozenset({BuildingLevel.GROUND}),
            floors_min=0,
            density_max=None,
        ),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
    )
    assert program.residential_floors == 1
    assert program.basement_levels == 0
    assert program.basement_area_m2 == 0.0
    assert program.density_floor_area_m2 == pytest.approx(program.gross_floor_area_m2)


def test_the_rdc_and_its_cellar_are_one_storey_and_one_level_below():
    # The 91 Villeray columns that enumerate their levels rather than saying
    # "tous": *Inferieurs au RDC* used to buy a second above-grade storey and
    # be charged three metres for it. Now it buys the level it names.
    program = solve_program(
        column(
            levels=frozenset({BuildingLevel.GROUND, BuildingLevel.BELOW_GROUND}),
            floors_min=0,
            density_max=None,
        ),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
    )
    assert program.residential_floors == 1
    assert program.basement_residential_levels == 1
    assert program.height_m == pytest.approx(RESIDENTIAL_STOREY_HEIGHT_M)
    assert program.density_floor_area_m2 == pytest.approx(2 * program.footprint_m2)


def test_a_cellar_dwelling_is_dearer_to_build_and_leases_for_less():
    # The one place the model says which level a dwelling is on, and the reason
    # it has to: the two rates differ. Every figure below is the program's own
    # split, priced at the two module constants.
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
    )
    assert program.basement_dwellings > 0
    assert program.above_grade_dwellings > 0
    assert program.total_dwellings == (
        program.above_grade_dwellings + program.basement_dwellings
    )
    premium = 1 + BELOW_GRADE_COST_PREMIUM
    discount = 1 - BELOW_GRADE_RENT_DISCOUNT_PCT / 100.0
    assert program.construction_cost_cad == pytest.approx(
        sum(
            UNIT_AREAS_SQFT[unit_type] * RESIDENTIAL_COST_PER_SQFT_CAD * quantity
            for unit_type, quantity in program.above_grade_units.items()
        )
        + sum(
            UNIT_AREAS_SQFT[unit_type]
            * RESIDENTIAL_COST_PER_SQFT_CAD
            * premium
            * quantity
            for unit_type, quantity in program.basement_units.items()
        )
    )
    assert program.gross_revenue_cad == pytest.approx(
        sum(
            RENTS[unit_type] * (1 - VACANCY[unit_type] / 100.0) * quantity
            for unit_type, quantity in program.above_grade_units.items()
        )
        + sum(
            RENTS[unit_type] * (1 - VACANCY[unit_type] / 100.0) * discount * quantity
            for unit_type, quantity in program.basement_units.items()
        )
    )


def test_at_the_module_rates_a_cellar_dwelling_does_not_pay_for_itself():
    """The default proforma's own answer, and worth pinning: the discount and
    the premium together put a sous-sol unit under water at every class CMHC
    prices, so the discounted objective builds none. It is a close call -
    `BELOW_GRADE_RENT_DISCOUNT_PCT` has the break-evens - and a change to a
    published rate that quietly flipped it should be visible here."""
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.solved
    assert program.basement_dwellings == 0
    assert "basement_unbuilt" in program.binding


def test_a_cellar_that_is_spent_is_reported_as_the_cap_it_is():
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
    )
    assert program.basement_levels == BASEMENT_LEVELS_ALLOWED
    assert "basement_levels" in program.binding
    assert "basement_unbuilt" not in program.binding


@pytest.mark.parametrize("investment", [DEFAULT_INVESTMENT, UNDISCOUNTED])
def test_an_empty_cellar_is_never_reported(investment):
    """Nothing in the objective charges for the *plate* - a dwelling's
    coefficient is on its count - so without a guard a parcel with density to
    spare would report a sous-sol with nothing in it."""
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=investment,
    )
    if program.basement_residential_levels:
        assert program.basement_dwellings > 0
    else:
        assert program.basement_residential_area_m2 == 0.0


def test_no_basement_solves_what_the_module_solved_before_the_cellar():
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
        basement_levels_allowed=NO_BASEMENT,
    )
    assert program.units == {"1_bedroom": 25}
    assert program.basement_levels == 0
    assert program.density_floor_area_m2 == pytest.approx(program.gross_floor_area_m2)
    assert "basement_unbuilt" not in program.binding


def test_the_cellar_is_stacked_under_the_rdc_and_over_the_parkade():
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=STRUCTURED_ONLY,
        investment=UNDISCOUNTED,
    )
    assert program.basement_residential_levels == 1
    assert program.underground_levels > 0
    stack = floor_stack(program)
    cellar = [
        entry
        for entry in stack
        if entry["position"] == "below_grade" and entry["use"] == "residential"
    ]
    assert len(cellar) == 1
    # Immediately under the rez-de-chaussee, with the parking below it.
    assert cellar[0]["to_level"] == -1
    assert cellar[0]["dwellings"] == program.basement_dwellings
    # Floor area the index counts, standing no metres - the one entry in the
    # stack of which both are true.
    assert cellar[0]["counts_as_floor_area"] is True
    assert cellar[0]["storey_height_m"] == 0.0
    parkade = [
        entry
        for entry in stack
        if entry["position"] == "below_grade" and entry["use"] == "parking"
    ]
    assert parkade[0]["to_level"] < cellar[0]["from_level"]
    assert parkade[0]["counts_as_floor_area"] is False


def test_a_shop_at_grade_with_its_cellar_and_housing_over_it():
    """The shape 90 of Villeray's 91 basement-marked columns are actually in.

    A zone printing its commerce on *RDC* + *Inferieurs au RDC* and its housing
    on *Tous les niveaux* is two columns and one building. The commerce gets
    the storey and the cellar its rows name; the housing gets the rest, and
    supplies the *En etage min* the commerce column could never pay on its
    own. Before the cellar stopped being a storey the commerce could take two
    above-grade storeys here, which is not what those rows say.
    """
    shop = ZoneColumn(
        usages=("C.2",),
        levels=frozenset({BuildingLevel.GROUND, BuildingLevel.BELOW_GROUND}),
        floors_min=2,
        floors_max=3,
        site_coverage_max_pct=70.0,
        density_max=4.5,
        zone="C02-113",
    )
    housing = ZoneColumn(
        usages=("H.7",),
        levels=frozenset({BuildingLevel.ALL}),
        floors_min=2,
        floors_max=3,
        site_coverage_max_pct=70.0,
        density_max=4.5,
        zone="C02-113",
    )
    program = solve_program(
        ZoneEnvelope.of([shop, housing], frontage_m=15.0),
        Lot(area_m2=500.0, frontage_m=15.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=UNDISCOUNTED,
    )
    assert program.solved
    # One above-grade storey of commerce - the RDC its rows name - and its
    # cellar under it. Not two storeys, which is what the level rows bought
    # while a cellar still counted as one.
    assert program.commercial_floors == 1
    assert program.basement_commercial_levels == 1
    # The housing supplies the storeys the shop column's own minimum demands.
    assert program.residential_floors >= 1
    assert program.commercial_floors + program.residential_floors >= 2


def test_a_column_whose_only_storey_is_the_rdc_cannot_meet_a_two_storey_minimum():
    """Zone C02-113's lone column, and the one place the correction bites.

    *RDC* and *Inferieurs au RDC* is one above-grade storey and one cellar,
    and *En etage min* 2 asks for a second storey no usage at this column's
    head may occupy. Named rather than returned as a bare INFEASIBLE, because
    it is a contradiction between two rows of one column - which is what the
    grid prints, and what counting the cellar as the storey used to hide.
    """
    program = solve_program(
        ZoneColumn(
            usages=("C.2",),
            levels=frozenset({BuildingLevel.GROUND, BuildingLevel.BELOW_GROUND}),
            floors_min=2,
            floors_max=3,
            site_coverage_max_pct=70.0,
            density_max=4.5,
            zone="C02-113",
        ),
        Lot(area_m2=500.0, frontage_m=15.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert not program.solved
    assert program.binding == ("floors_min_exceeds_permitted_levels",)


def test_the_below_grade_order_is_the_documented_one():
    assert BASEMENT_STACK_ORDER == ("commercial", "industrial", "residential")


def test_a_negative_below_grade_premium_is_refused():
    with pytest.raises(ProgramError):
        ConstructionCosts(below_grade_premium=-0.1)


@pytest.mark.parametrize("discount", [-1.0, 101.0])
def test_a_below_grade_discount_that_is_not_a_share_is_refused(discount):
    with pytest.raises(ProgramError):
        InvestmentAssumptions(below_grade_rent_discount_pct=discount)


def test_a_negative_basement_allowance_is_refused():
    with pytest.raises(ProgramError):
        permitted_basement_levels({BuildingLevel.ALL}, allowed=-1)


# -- maintenance: what age adds to an operating expense ratio ---------------


def test_age_is_the_reference_year_less_the_year_built():
    assert float(building_age_years(1920, reference_year=2026)) == 106.0


def test_an_unstated_year_is_charged_the_assumed_age_not_zero():
    # Reading a missing year as new would hand the least-documented buildings
    # in the borough the cheapest maintenance in it.
    assert float(
        building_age_years(float("nan"), reference_year=2026, assumed_age_years=50.0)
    ) == 50.0


def test_a_year_in_the_future_is_a_new_building_rather_than_a_negative_age():
    # The roll carries units permitted but not finished; a building cannot be
    # newer than new.
    assert float(building_age_years(2030, reference_year=2026)) == 0.0


def test_the_premium_is_zero_for_a_new_building():
    assert float(maintenance_premium(0)) == 0.0


def test_the_premium_rises_with_age_and_then_stops():
    at_50 = float(maintenance_premium(50))
    at_100 = float(maintenance_premium(100))
    at_200 = float(maintenance_premium(200))

    assert 0 < at_50 < at_100
    # Past the cap an older building is not charged more: an owner who stops
    # spending stops collecting, and the curve is fitted to nothing that old.
    assert at_100 == pytest.approx(MAX_MAINTENANCE_PREMIUM)
    assert at_200 == pytest.approx(MAX_MAINTENANCE_PREMIUM)


def test_the_premium_is_linear_in_age_below_the_cap():
    assert float(maintenance_premium(10)) == pytest.approx(
        10 * MAINTENANCE_PREMIUM_PER_YEAR
    )
    assert float(maintenance_premium(20)) == pytest.approx(
        2 * float(maintenance_premium(10))
    )


def test_a_whole_borough_is_charged_in_one_pass():
    # The array form is what `comparables` uses; it must agree with the scalar.
    ages = np.array([0.0, 25.0, 100.0])
    charged = maintenance_premium(ages)

    assert charged.shape == (3,)
    assert list(charged) == [float(maintenance_premium(a)) for a in ages]


def test_the_effective_ratio_is_the_base_plus_the_premium():
    assert float(effective_operating_expense_ratio(0, base_ratio=0.35)) == 0.35
    assert float(
        effective_operating_expense_ratio(100, base_ratio=0.35)
    ) == pytest.approx(0.35 + MAX_MAINTENANCE_PREMIUM)


def test_the_effective_ratio_stays_below_one_so_an_noi_stays_positive():
    # A ratio of 1 is a building whose whole rent leaves again; past it the
    # income is negative, which is not what a high maintenance bill means.
    charged = float(
        effective_operating_expense_ratio(
            200, base_ratio=0.95, per_year=0.01, cap=0.5
        )
    )
    assert charged < 1.0


def test_a_base_ratio_that_is_not_a_share_is_refused_rather_than_clipped():
    with pytest.raises(ProgramError, match="base operating expense ratio"):
        effective_operating_expense_ratio(10, base_ratio=1.0)


def test_a_negative_premium_is_refused():
    # This is a premium, not an adjustment: a building cannot be cheaper to
    # run than new.
    with pytest.raises(ProgramError, match="cannot be negative"):
        maintenance_premium(10, per_year=-0.01)


# -- discounted net profit: what a dollar of rent is worth --------------------
#
# The objective's price list. Every figure below is arrived at by hand from
# the proforma the InvestmentAssumptions docstring states: a dollar a month of
# gross becomes 12 x (1 - opex) dollars a year of stabilised NOI, collected
# over the hold at the discount rate, plus the sale at the terminal cap.


def test_the_pv_factor_is_the_annuity_plus_the_discounted_reversion():
    stance = InvestmentAssumptions(
        discount_rate_pct=5.0,
        hold_years=25,
        terminal_cap_rate_pct=4.5,
        operating_expense_ratio=0.35,
    )
    annuity = (1 - 1.05**-25) / 0.05
    reversion = (100 / 4.5) / 1.05**25
    assert stance.annual_pv_factor == pytest.approx(annuity + reversion)
    assert stance.pv_per_monthly_gross == pytest.approx(
        12 * 0.65 * (annuity + reversion)
    )


def test_no_terminal_cap_values_the_income_stream_alone():
    with_sale = InvestmentAssumptions(terminal_cap_rate_pct=4.5)
    without = InvestmentAssumptions(terminal_cap_rate_pct=None)
    assert without.annual_pv_factor < with_sale.annual_pv_factor
    rate = without.discount_rate_pct / 100.0
    assert without.annual_pv_factor == pytest.approx(
        (1 - (1 + rate) ** -without.hold_years) / rate
    )


def test_the_undiscounted_stance_reprices_the_old_objective_exactly():
    # Zero discount, zero expenses, no sale, no premium, held for the
    # amortisation's own 25 years: a dollar a month is worth the 300 dollars
    # the straight-line amortisation always implied, so the argmax is what it
    # always was.
    assert UNDISCOUNTED_INVESTMENT.pv_per_monthly_gross == pytest.approx(300.0)
    assert UNDISCOUNTED_INVESTMENT.rent_premium_factor == 1.0


def test_the_objective_is_the_present_value_less_the_capital():
    # One dwelling, no parking, so every identity is a single multiplication:
    # the objective is the proforma rent through the PV multiplier, less the
    # build in full - not amortised.
    stance = InvestmentAssumptions(new_build_rent_premium_pct=0.0)
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=stance,
    )
    assert program.total_dwellings == 1
    assert program.present_value_cad == pytest.approx(
        program.gross_revenue_cad * stance.pv_per_monthly_gross
    )
    assert program.npv_cad == pytest.approx(
        program.present_value_cad - program.total_capital_cost_cad
    )
    assert program.annual_stabilised_noi_cad == pytest.approx(
        program.gross_revenue_cad * 12 * (1 - stance.operating_expense_ratio)
    )
    # And the legacy monthly figure is restated beside it, unchanged in
    # meaning: gross less the amortised capital.
    assert program.net_operating_income == pytest.approx(
        program.gross_revenue_cad
        - program.total_capital_cost_cad / AMORTIZATION_MONTHS
    )


def test_the_rent_premium_reaches_the_dwelling_revenue_and_nothing_else():
    # 30 percent over the survey, dwellings only. One priced class, so the
    # premium cannot also move the mix and the comparison is one
    # multiplication: the reported gross is the proforma rent, because every
    # dollar figure on the program is built from it.
    one_class = UnitEconomics(
        average_rent_cad={"1_bedroom": 1000.0},
        vacancy_rate_pct={"1_bedroom": 1.5},
    )
    plain = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        one_class,
        parking=NO_PARKING,
        investment=InvestmentAssumptions(new_build_rent_premium_pct=0.0),
    )
    boosted = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        one_class,
        parking=NO_PARKING,
        investment=InvestmentAssumptions(new_build_rent_premium_pct=30.0),
    )
    assert plain.units == boosted.units == {"1_bedroom": 1}
    assert boosted.gross_revenue_cad == pytest.approx(
        plain.gross_revenue_cad * 1.3
    )


def test_discounting_reprices_the_classes_against_their_build_cost():
    # The docstring table's two rankings are the undiscounted ones. Under the
    # default stance the capital weighs roughly twice as much against the
    # rent, and the per-dwelling winner moves: the three-bedroom's extra rent
    # no longer pays for its extra square feet.
    program = solve_program(
        column(max_dwellings=1, density_max=None),
        Lot(area_m2=5000.0, frontage_m=40.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=DEFAULT_INVESTMENT,
    )
    assert program.total_dwellings == 1
    assert program.units == {"2_bedroom": 1}


def test_a_program_that_does_not_pencil_discounted_is_not_built():
    # Every class under water once the discounting weighs the capital: no
    # premium, no reversion, and a short hold. The solver declines to build
    # rather than reporting a loss - the same posture the halved-rent test
    # pins for the undiscounted objective.
    thin = InvestmentAssumptions(
        discount_rate_pct=8.0,
        hold_years=10,
        terminal_cap_rate_pct=None,
        new_build_rent_premium_pct=0.0,
    )
    program = solve_program(
        column(density_max=None),
        Lot(area_m2=400.0, frontage_m=12.0),
        ECONOMICS,
        parking=NO_PARKING,
        investment=thin,
    )
    assert program.solved
    assert program.units == {}
    assert program.npv_cad == 0.0


@pytest.mark.parametrize(
    "overrides",
    [
        {"discount_rate_pct": -1.0},
        {"hold_years": 0},
        {"terminal_cap_rate_pct": 0.0},
        {"operating_expense_ratio": 1.0},
        {"new_build_rent_premium_pct": -5.0},
    ],
)
def test_an_investment_stance_that_cannot_be_priced_is_refused(overrides):
    with pytest.raises(ProgramError):
        InvestmentAssumptions(**overrides)


# -- the floor stack --------------------------------------------------------
#
# `floor_stack` decides nothing the solver decided: it re-cuts storey counts
# the solver produced into runs of identical levels, and the order it stacks
# them in is `FLOOR_STACK_ORDER`'s stated convention rather than an answer.
# So these are of two kinds - arithmetic over a program written out by hand,
# where every level number can be counted on fingers, and one reconciliation
# against a real solve, which is the only thing that can catch the stack and
# the columns beside it drifting apart.

#: Every key an entry carries, whatever it is filled with. Named here because
#: the promise is the *stable shape* - a reader unnesting these in SQL should
#: never have to branch on the use to know a key is there.
STACK_KEYS = {
    "use",
    "position",
    "from_level",
    "to_level",
    "floors",
    "floor_plate_m2",
    "floor_area_m2",
    "counts_as_floor_area",
    "storey_height_m",
    "height_m",
    "stalls",
    "dwellings",
    "units",
}


def mixed_program(**overrides) -> DevelopmentProgram:
    """Two dug levels, retail at grade, a workshop, a deck and five of housing.

    Written out rather than solved: no envelope produces all four above-grade
    uses at once, and the point of the fixture is to have every branch of the
    stack present in one answer.
    """
    base = {
        "units": {"1_bedroom": 8, "2_bedroom": 4},
        "floors": 9,
        "footprint_m2": 200.0,
        "gross_floor_area_m2": 1800.0,
        "unit_area_m2": 900.0,
        "net_operating_income": 0.0,
        "status": "OPTIMAL",
        "residential_floors": 5,
        "commercial_floors": 1,
        "industrial_floors": 1,
        "above_grade_parking_floors": 2,
        "underground_levels": 2,
        "underground_stalls": 7,
        "above_grade_stalls": 5,
        "underground_area_m2": 400.0,
        "commercial_area_m2": 200.0,
        "industrial_area_m2": 200.0,
    }
    return DevelopmentProgram(**{**base, **overrides})


def test_a_run_of_identical_storeys_is_one_entry():
    """Five identical plates are one entry and not five rows to read."""
    stack = floor_stack(
        mixed_program(
            floors=5,
            residential_floors=5,
            commercial_floors=0,
            industrial_floors=0,
            above_grade_parking_floors=0,
            underground_levels=0,
            underground_stalls=0,
            above_grade_stalls=0,
            underground_area_m2=0.0,
            commercial_area_m2=0.0,
            industrial_area_m2=0.0,
            gross_floor_area_m2=1000.0,
        )
    )
    assert len(stack) == 1
    entry = stack[0]
    assert entry["use"] == "residential"
    assert (entry["from_level"], entry["to_level"], entry["floors"]) == (1, 5, 5)
    assert entry["floor_plate_m2"] == pytest.approx(200.0)
    assert entry["floor_area_m2"] == pytest.approx(1000.0)


def test_the_uses_stack_in_the_stated_order():
    """Dug levels, then `FLOOR_STACK_ORDER` from grade up, with no gaps."""
    stack = floor_stack(mixed_program())
    assert [entry["use"] for entry in stack] == [
        "parking",
        "commercial",
        "industrial",
        "parking",
        "residential",
    ]
    assert [(entry["from_level"], entry["to_level"]) for entry in stack] == [
        (-2, -1),
        (1, 1),
        (2, 2),
        (3, 4),
        (5, 9),
    ]
    above = [entry["use"] for entry in stack if entry["position"] == "above_grade"]
    assert above == list(FLOOR_STACK_ORDER)


def test_the_dug_levels_are_numbered_below_grade_and_are_not_floor_area():
    """Article 38 1 arriving in the one place the stack can show it."""
    stack = floor_stack(mixed_program())
    dug = stack[0]
    assert dug["position"] == "below_grade"
    # -2 to -1: there is no level 0, and the ground floor is 1.
    assert (dug["from_level"], dug["to_level"]) == (-2, -1)
    assert dug["counts_as_floor_area"] is False
    assert dug["storey_height_m"] == UNDERGROUND_LEVEL_HEIGHT_M
    assert dug["height_m"] == 0.0
    assert all(entry["counts_as_floor_area"] for entry in stack[1:])


def test_the_stalls_are_reported_where_they_were_put():
    stack = floor_stack(mixed_program())
    parked = {
        entry["position"]: entry["stalls"]
        for entry in stack
        if entry["use"] == "parking"
    }
    assert parked == {"below_grade": 7, "above_grade": 5}
    assert sum(entry["stalls"] for entry in stack) == 12
    assert all(entry["stalls"] == 0 for entry in stack if entry["use"] != "parking")


def test_the_mix_sits_on_the_residential_run_whole():
    """The solver chose a mix for the building rather than for a plate, and
    dividing it by the storeys would be inventing the part it did not choose."""
    stack = floor_stack(mixed_program())
    housing = [entry for entry in stack if entry["use"] == "residential"]
    assert len(housing) == 1
    assert housing[0]["units"] == {"1_bedroom": 8, "2_bedroom": 4}
    assert housing[0]["dwellings"] == 12
    assert all(
        entry["units"] == {} and entry["dwellings"] == 0
        for entry in stack
        if entry["use"] != "residential"
    )


def test_every_entry_carries_every_key():
    """The shape is stable so unnesting one of these needs no branch."""
    assert all(set(entry) == STACK_KEYS for entry in floor_stack(mixed_program()))


def test_each_run_is_priced_at_its_own_storey_height():
    expected = {
        ("commercial", "above_grade"): COMMERCIAL_STOREY_HEIGHT_M,
        ("industrial", "above_grade"): INDUSTRIAL_STOREY_HEIGHT_M,
        ("parking", "above_grade"): ABOVE_GRADE_PARKING_STOREY_HEIGHT_M,
        ("residential", "above_grade"): RESIDENTIAL_STOREY_HEIGHT_M,
        ("parking", "below_grade"): UNDERGROUND_LEVEL_HEIGHT_M,
    }
    for entry in floor_stack(mixed_program()):
        height = expected[(entry["use"], entry["position"])]
        assert entry["storey_height_m"] == pytest.approx(height)
        assert entry["height_m"] == pytest.approx(height * entry["floors"])


def test_a_stated_storey_height_reaches_the_stack():
    """The heights are an assumption, and a run is priced at the one the
    program was solved with rather than at the module's default."""
    tall = StoreyHeights(residential_m=4.0)
    stack = floor_stack(mixed_program(), heights=tall)
    housing = next(entry for entry in stack if entry["use"] == "residential")
    assert housing["storey_height_m"] == pytest.approx(4.0)
    assert housing["height_m"] == pytest.approx(20.0)


def test_a_program_with_nothing_built_has_no_stack():
    empty = floor_stack(
        mixed_program(
            floors=0,
            residential_floors=0,
            commercial_floors=0,
            industrial_floors=0,
            above_grade_parking_floors=0,
            underground_levels=0,
            gross_floor_area_m2=0.0,
            underground_area_m2=0.0,
        )
    )
    assert empty == []


def test_the_stack_reconciles_with_the_columns_beside_it():
    """Every number in a stack is one already on the program, re-cut by level."""
    solved = solve_program(
        column(),
        Lot(area_m2=500.0, frontage_m=15.0),
        ECONOMICS,
        investment=UNDISCOUNTED,
    )
    assert solved.solved
    stack = floor_stack(solved)
    above = [entry for entry in stack if entry["position"] == "above_grade"]
    below = [entry for entry in stack if entry["position"] == "below_grade"]
    assert sum(entry["floors"] for entry in above) == solved.floors
    assert sum(entry["floors"] for entry in below) == (
        solved.underground_levels + solved.basement_levels
    )
    assert sum(entry["floor_area_m2"] for entry in above) == pytest.approx(
        solved.gross_floor_area_m2, abs=0.05
    )
    assert sum(entry["floor_area_m2"] for entry in below) == pytest.approx(
        solved.underground_area_m2 + solved.basement_area_m2, abs=0.05
    )
    # And the below-grade entries are not all the same kind of area: the usage
    # levels are *superficie de plancher* and the parking under them is not, so
    # `counts_as_floor_area` splits them and the two sums are the two columns
    # on the program.
    assert sum(
        entry["floor_area_m2"] for entry in below if entry["counts_as_floor_area"]
    ) == pytest.approx(solved.basement_area_m2, abs=0.05)
    assert sum(
        entry["floor_area_m2"] for entry in stack if entry["counts_as_floor_area"]
    ) == pytest.approx(solved.density_floor_area_m2, abs=0.05)
    # The dug levels stand no metres, so the whole stack is the reported height.
    assert sum(entry["height_m"] for entry in stack) == pytest.approx(
        solved.height_m, abs=0.05
    )
    # The stack is the building, and a surface stall is not in it - it stands
    # on the yard the footprint leaves, which is no run and no level. What the
    # stack accounts for is everything parked indoors, the garage bays
    # included: those ride on the residential run, being floor area inside it
    # rather than a storey of their own. `total_stalls` is larger by exactly
    # the stalls that are outdoors.
    assert sum(entry["stalls"] for entry in stack) == (
        solved.underground_stalls
        + solved.above_grade_stalls
        + solved.garage_stalls
    )
    assert solved.total_stalls == (
        sum(entry["stalls"] for entry in stack) + solved.surface_stalls
    )
    assert sum(entry["dwellings"] for entry in stack) == solved.total_dwellings


# --------------------------------------------------------------------------
# the yard has a shape, not only an area
# --------------------------------------------------------------------------
#
# `surface_stall_area x stalls + footprint <= lot area` is an area against an
# area, and every test above satisfies it on parcels whose shape nobody asked
# about. `Lot.parkable_area_m2` is the second ceiling - the largest
# parking-shaped rectangle the parcel actually holds, measured off the cadastre
# by `massing.parking_capacity_m2` - and these are what it changes.


def _yard_test_column(**overrides):
    """A column that parks on the ground unless something stops it.

    60/60 coverage on a 300 m2 lot pins the plate at 180 m2 and leaves 120 m2
    of yard, which is the same envelope
    `test_the_yard_is_spent_before_the_structure` uses - so what differs
    between these tests and that one is the parcel's shape and nothing else.
    """
    return column(
        max_dwellings=5,
        density_max=None,
        site_coverage_min_pct=60.0,
        site_coverage_max_pct=60.0,
        **overrides,
    )


def _yard_program(lot, **overrides):
    return solve_program(
        _yard_test_column(),
        lot,
        ECONOMICS,
        parking=ParkingRules(stalls_per_dwelling=2.0),
        investment=UNDISCOUNTED,
        **overrides,
    )


def test_an_unmeasured_yard_is_bounded_on_area_alone():
    """`None` is what every caller passed before this existed, and still works."""
    program = _yard_program(Lot(area_m2=300.0, frontage_m=12.0))
    assert program.surface_stalls == 4
    assert program.solved


def test_a_parcel_that_holds_no_parking_puts_the_stalls_in_structure():
    """0.0 is a measurement, not an absence - and a sharp one.

    The same 300 m2 lot and the same 120 m2 of yard, on a parcel four metres
    wide. Nothing about the *area* has changed and the area constraint is as
    satisfied as it was; what has changed is that no car can stand on it. Every
    stall the dwellings owe is still provided, because the by-law asks for them
    however awkward the parcel is - they are just provided at eight to ten
    times the price.
    """
    program = _yard_program(Lot(area_m2=300.0, frontage_m=12.0, parkable_area_m2=0.0))
    assert program.solved
    assert program.surface_stalls == 0
    assert program.total_stalls == 2 * program.total_dwellings
    assert program.total_stalls == (
        program.underground_stalls + program.above_grade_stalls + program.garage_stalls
    )


def test_the_yard_shape_is_reported_as_the_binding_cap():
    """No printed norm says "your lot is the wrong shape", so this column does."""
    shaped = _yard_program(Lot(area_m2=300.0, frontage_m=12.0, parkable_area_m2=0.0))
    assert "surface_parking_shape" in shaped.binding

    # And it is not reported where the yard's shape is not what stopped it: a
    # parcel measured to hold far more parking than the program wants is bound
    # by something else, whatever that is.
    roomy = _yard_program(
        Lot(area_m2=300.0, frontage_m=12.0, parkable_area_m2=5_000.0)
    )
    assert "surface_parking_shape" not in roomy.binding


def test_a_measured_yard_rations_the_stalls_between_the_two_bounds():
    """Whichever of shape and area is tighter is the one that binds."""
    stall_m2 = SURFACE_STALL_AREA_SQFT * M2_PER_SQFT
    # Room for two stalls in the shape, against four in the area.
    program = _yard_program(
        Lot(area_m2=300.0, frontage_m=12.0, parkable_area_m2=2.5 * stall_m2)
    )
    assert program.surface_stalls == 2
    assert program.total_stalls == 2 * program.total_dwellings


def test_a_generous_shape_never_buys_more_than_the_area_allows():
    """The new bound only ever tightens - it cannot conjure yard."""
    program = _yard_program(
        Lot(area_m2=300.0, frontage_m=12.0, parkable_area_m2=10_000.0)
    )
    assert program.surface_stalls == 4
    assert program.surface_stalls * SURFACE_STALL_AREA_SQFT * M2_PER_SQFT <= 120.0


def test_surface_area_is_the_ground_the_model_reserved():
    """What `lot_building_massing` draws, rather than the nominal product.

    The area the model actually held, so a rectangle of exactly it can be drawn
    on the parcel. It is floor area of no kind: not in the gross, not in the
    footprint, and not under the building either.
    """
    program = _yard_program(Lot(area_m2=300.0, frontage_m=12.0))
    stall_m2 = SURFACE_STALL_AREA_SQFT * M2_PER_SQFT
    assert program.surface_area_m2 == pytest.approx(
        program.surface_stalls * stall_m2, abs=0.05
    )
    assert program.surface_area_m2 > 0
    assert program.surface_area_m2 + program.footprint_m2 <= 300.0
    # Not floor area, and not part of the plate.
    assert program.gross_floor_area_m2 == pytest.approx(
        program.footprint_m2 * program.floors
    )


def test_a_program_with_no_surface_stalls_reserves_no_ground():
    program = solve_program(
        column(max_dwellings=12, density_max=None),
        Lot(area_m2=2000.0, frontage_m=25.0),
        ECONOMICS,
        parking=NO_PARKING,
    )
    assert program.surface_stalls == 0
    assert program.surface_area_m2 == 0.0


@pytest.mark.parametrize("parkable", [-1.0, -0.01])
def test_a_negative_parkable_area_is_refused(parkable):
    with pytest.raises(ProgramError, match="parkable area must not be negative"):
        Lot(area_m2=300.0, frontage_m=12.0, parkable_area_m2=parkable)
