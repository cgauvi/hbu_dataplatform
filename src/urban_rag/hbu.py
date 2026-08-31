"""What a lot should be built as, what it is built as, and the gap between.

`urban_rag.program` answers one lot's question and stops there: hand it a
`ZoneColumn`, a `Lot` and a rent grid and it returns the mix of dwellings,
commerce, industry and parking worth the most under that envelope. Nothing
between it and the pipeline turned a borough's worth of envelopes into a
borough's worth of answers - `lot_zoning_envelopes` builds the inputs and
`solve_program` had no caller outside its own tests. This module is that
caller, plus the two questions that only become askable once it has run:
which of a lot's candidate envelopes is *the* answer, and how far the building
standing on the lot today is from it.

Deliberately free of Dagster imports, the posture `urban_rag.program`,
`urban_rag.comparables` and `urban_rag.role_foncier` take: everything here is
arithmetic over frames, and `urban_rag.hbu_assets` is what reads and writes the
partitions.

----------------------------------------------------------------------------
Solving
----------------------------------------------------------------------------

`solve_envelopes` is one `solve_program` call per row of
`lot_zoning_envelopes`, and the interesting part is which rows. A row is solved
when it `permits_residential` and is `solver_ready` - the first because
`solve_program` refuses a column authorising no dwelling, the second because
that flag is the solver's own constructor having already accepted the column.
Neither is re-decided here: both are written by `envelope_assets` from
`ZoneColumn.__post_init__`, and a second copy of that rule would be the copy
that goes stale.

**A failed solve costs its row, not the borough.** A `ProgramError` - a lot of
zero area, a grid whose two coverage rows contradict each other - is recorded
in `solve_error` and the row comes back unsolved. A borough is tens of
thousands of these and the ones that fail are the interesting ones; failing the
partition would hide them all behind the first.

**The envelope is the grid's, the footprint cap is the parcel's.**
`Lot.buildable_area_m2` is what `lot_buildable_setbacks` computes for *this*
column - the parcel less that column's own four margins - and it is passed per
row rather than per lot, because two columns of one grid legitimately state
different margins. Where the setbacks asset has not run it is `None`, which
leaves the footprint capped on *Taux d'implantation* alone. That is the same
fallback every caller had before that asset existed, and the asset's
`num_with_buildable_area` is what says how many rows got the better cap.

----------------------------------------------------------------------------
Choosing
----------------------------------------------------------------------------

`select_highest_best_use` collapses the candidates onto one row per lot, and
what it does *not* do is take the largest number.

A lot can carry several envelope rows for two unrelated reasons, and only one
of them is a choice. Within one zone, a grid authorises dwellings in more than
one column and distinguishes them by *Largeur du terrain min*: the column a
parcel of this width is written for is `select_residential_column`'s to pick,
`envelope_assets` already marks it `governs_residential`, and taking the
higher-earning column instead would be reporting a program under rules the
parcel may not build to. Across zones, a lot on a boundary picks up a sliver of
its neighbour's zoning because two publishers drew two lines - that is not two
sets of rules the owner may choose between, it is one set and a mapping
disagreement, and `pct_of_lot` is what says which is the real one.

So the answer is: **the governing column of the zone that covers most of the
lot**. Income breaks a tie between two zones covering it equally and the column
index breaks a tie after that, so the choice is deterministic. Every candidate
that lost keeps its own row in `lot_development_programs`, which is where "why
not the other column" is answered.

The maximisation this table is named for is not here at all - it is inside
`solve_program`, over the mix of dwellings and floor the envelope can hold.
What is chosen here is only *which envelope*, and the grid is what chooses it.

A lot with candidates but no governing one keeps its row and says so in
`hbu_status`: `no_governing_column` is almost always a parcel with no measured
frontage under a grid that states a width minimum, which reads as 0 m and
qualifies for nothing. That is a gap in `lot_frontage` rather than in the grid,
and it is worth being able to count.

----------------------------------------------------------------------------
Comparing
----------------------------------------------------------------------------

`use_gap` puts the building the roll describes beside the building the solver
chose. Three things have to be reconciled before the subtraction means
anything, and each of them is a way to be confidently wrong:

**The period.** `solve_program` returns income a *month* - CMHC surveys a
monthly rent, so that is the unit the objective is built in. `comparables`
returns income a *year*, because commercial leasing is quoted annually and a
roll is read against annual figures. `MONTHS_PER_YEAR` is the whole of the
conversion, and every money column here carries `monthly_` or `annual_` in its
name rather than leaving a reader to remember which is which.

**The definition of NOI.** The two upstreams net out different things and
neither is wrong. `comparables.annual_income` takes an
`operating_expense_ratio` off the gross - taxes, insurance, management,
maintenance - and charges nothing for the building, because the building is
already standing. `program.solve_program` charges the amortised cost of
*putting the building up* and takes no operating expenses off, because that
module says in as many words that it nets income less the cost of building the
thing that earns it and no further. Subtracting one from the other compares a
stabilised income against a development margin and produces a number that is
neither.

This module therefore states one definition and computes both sides under it.
`annual_stabilised_noi_cad` is `gross x (1 - operating_expense_ratio)` on both
sides, and the gap is taken between those two. The ratio is not re-invented
here: it is read off the `income_assumptions` the comparables asset wrote onto
every one of its rows, so the existing side of the subtraction is the number
that table already published rather than a second computation of it.

**The two sides are charged different ratios, and that asymmetry is the
point.** What is read off `income_assumptions` is the *base* - what a building
costs to run when it is new - and the building this module proposes is new by
construction, so the base is the whole of its ratio. The existing building is
not: `comparables` has already added a maintenance premium for its age, and
`existing_annual_stabilised_noi_cad` arrives here net of it. Charging the
proposal the standing building's ratio would make a new tower pay for a
century-old triplex's roof; charging the triplex the proposal's would be the
error the premium exists to correct, and it is the one that quietly makes
redevelopment look not worth doing. `existing_effective_operating_expense_ratio`
travels beside `hbu_operating_expense_ratio` so the two are readable against
each other rather than inferred from a gap.

What the development cost buys is not thrown away - it is
`hbu_annual_noi_after_construction_cad`, the solver's own objective annualised,
with `hbu_total_capital_cost_cad` beside it. A reader asking "is this worth
doing" needs both: the first says the redeveloped building earns more, the
second says what it costs to get there, and this module deliberately stops
before the discount rate that would turn the pair into a verdict.

**Gross floor area against a unit schedule.** The roll's `rl0308a` is the
building's floor area, corridors and cores included. `DevelopmentProgram`
carries two candidates for the residential side of that: `unit_area_m2`, the
rentable schedule in `UNIT_AREAS_SQFT`, and the plate the storeys actually
occupy - `footprint x residential_floors`. The second is the like-for-like
comparison and is what the gap is taken on; the first travels beside it as
`hbu_unit_area_m2` because it is what the revenue was computed from, and the
difference between the two is the corridors the residential rate quietly
leaves unpriced.

**And the classes are the same three on both sides.** `comparables` splits the
roll's floor by each unit's own CUBF code into residential, commercial and
industrial; `solve_program` fills an envelope with the same three. That is not
a coincidence - the non-residential rates in both are `urban_rag.program`'s,
imported rather than restated - and it is what makes a per-class subtraction
mean anything at all.

**Square feet as well as square metres, for the gaps only.** Everything
upstream is metric and the levels stay that way; a gap is what gets read out
loud, and floor area is quoted in square feet by everyone who would read one.
`M2_PER_SQFT` is the conversion, the same constant the solver's own unit
schedule is converted with.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Mapping, Sequence

import pandas as pd

from urban_rag.comparables import INCOME_CLASSES, IncomeAssumptions
from urban_rag.program import (
    DEFAULT_CONSTRUCTION,
    DEFAULT_NON_RESIDENTIAL,
    DEFAULT_PARKING,
    DEFAULT_STOREY_HEIGHTS,
    M2_PER_SQFT,
    MONTHS_PER_YEAR,
    BuildingLevel,
    ConstructionCosts,
    DevelopmentProgram,
    Lot,
    NonResidentialEconomics,
    ParkingRules,
    ProgramError,
    StoreyHeights,
    UnitEconomics,
    ZoneColumn,
    solve_program,
)

#: The CMHC bedroom classes the solver prices a dwelling in, in the order a
#: unit schedule is read. `program.UNIT_AREAS_SQFT` is keyed by exactly these
#: and `cmhc.BEDROOM_TYPES` is where the spellings come from; the fifth key
#: that survey publishes - ``all`` - is a total rather than a class and is
#: deliberately absent, because a solver handed it would build the average of
#: the four beside the four themselves.
PRICED_BEDROOM_TYPES: tuple[str, ...] = (
    "studio",
    "1_bedroom",
    "2_bedroom",
    "3_bedroom_plus",
)

#: The `dwelling_type` row of `vacancy_rates` a bedroom class's vacancy is read
#: from. CMHC publishes the grid by structure as well as by bedroom count, and
#: the solver's dwellings have no structure - a storey of a mid-rise is neither
#: ``row`` nor exactly ``apartment_other`` - so the total is the honest cell.
_ALL_DWELLINGS = "all"

#: Why a lot has the row it has. One value per row of `lot_highest_best_use`,
#: so "the borough has four hundred unanswered lots" is a `GROUP BY` rather
#: than a set of nulls to interpret.
HBU_STATUSES: tuple[str, ...] = (
    # A governing envelope was solved, and the row carries its program.
    "solved",
    # Every envelope covering the lot authorises something other than housing.
    # `solve_program` refuses such a column by design - it prices dwellings,
    # commerce and industry but only ever inside a residential envelope - so
    # this is a limit of the model rather than a fault in the data, and a pure
    # `C` or `I` zone is where it shows.
    "no_residential_column",
    # Residential columns exist and none governs this parcel. Nearly always a
    # lot with no measured frontage under a grid stating *Largeur du terrain
    # min*: a missing frontage reads as 0 m and qualifies for nothing.
    "no_governing_column",
    # The governing column was solved and has no feasible program - a minimum
    # the parcel cannot meet, or stalls it has nowhere to put.
    "infeasible",
    # The governing column could not be turned into a model at all;
    # `solve_error` carries what it said.
    "solver_error",
)

#: What the solver's answer is called on a row, in reading order: the status,
#: then the money, then the envelope it fills, then what it cost to put up.
#: Named once so the frame builder and the warehouse table cannot disagree
#: about the schema, and shared by both assets because a chosen program is the
#: same shape as a candidate one.
PROGRAM_COLUMNS: tuple[str, ...] = (
    "status",
    "solved",
    "solve_error",
    "monthly_net_operating_income_cad",
    "annual_net_operating_income_cad",
    "monthly_gross_revenue_cad",
    "annual_gross_revenue_cad",
    "num_dwellings",
    "units",
    "floors",
    "height_m",
    "footprint_m2",
    "gross_floor_area_m2",
    "residential_area_m2",
    "unit_area_m2",
    "commercial_area_m2",
    "industrial_area_m2",
    "underground_area_m2",
    "residential_floors",
    "commercial_floors",
    "industrial_floors",
    "above_grade_parking_floors",
    "underground_levels",
    "underground_stalls",
    "above_grade_stalls",
    "total_stalls",
    "construction_cost_cad",
    "commercial_cost_cad",
    "industrial_cost_cad",
    "parking_cost_cad",
    "total_capital_cost_cad",
    "binding",
    "unpriced_types",
)

#: The money columns of a program, for the zero-fill a raised solve gets. Named
#: rather than matched on a suffix so a column added above is a decision here
#: too, and `height_m` is not mistaken for a length by a `_m` test.
_PROGRAM_FLOATS: tuple[str, ...] = (
    "monthly_net_operating_income_cad",
    "annual_net_operating_income_cad",
    "monthly_gross_revenue_cad",
    "annual_gross_revenue_cad",
    "height_m",
    "footprint_m2",
    "gross_floor_area_m2",
    "residential_area_m2",
    "unit_area_m2",
    "commercial_area_m2",
    "industrial_area_m2",
    "underground_area_m2",
    "construction_cost_cad",
    "commercial_cost_cad",
    "industrial_cost_cad",
    "parking_cost_cad",
    "total_capital_cost_cad",
)

#: The counts of a program, same purpose as `_PROGRAM_FLOATS`.
_PROGRAM_COUNTS: tuple[str, ...] = (
    "num_dwellings",
    "floors",
    "residential_floors",
    "commercial_floors",
    "industrial_floors",
    "above_grade_parking_floors",
    "underground_levels",
    "underground_stalls",
    "above_grade_stalls",
    "total_stalls",
)

#: What a candidate row carries about the parcel and the column *before* the
#: program, so a reader holding one row knows what was solved without going
#: back to `lot_zoning_envelopes`.
CANDIDATE_COLUMNS: tuple[str, ...] = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "feature_id",
    "source_table",
    "column_index",
    "grid_zone",
    "pct_of_lot",
    "usages",
    "permits_commercial",
    "permits_industrial",
    "governs_residential",
    "lot_area_m2",
    "primary_frontage_m",
    "buildable_area_m2",
)

#: The columns a chosen program brings across from its candidate row. The lot's
#: own facts are not among them: those come from `_lot_index`, which has a row
#: for every lot including the ones no candidate was chosen for.
_CHOSEN_COLUMNS: tuple[str, ...] = (
    "feature_id",
    "source_table",
    "column_index",
    "grid_zone",
    "pct_of_lot",
    "usages",
    "permits_commercial",
    "permits_industrial",
    "buildable_area_m2",
    *PROGRAM_COLUMNS,
)

#: What a `lot_highest_best_use` row says about the parcel before the envelope
#: chosen for it and the program that fills it.
_HBU_LOT_COLUMNS: tuple[str, ...] = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "lot_area_m2",
    "primary_frontage_m",
    "num_candidates",
    "num_governing_candidates",
    "num_zones",
    "hbu_status",
)

#: The whole of a `lot_highest_best_use` row, in reading order.
HBU_COLUMNS: tuple[str, ...] = (*_HBU_LOT_COLUMNS, *_CHOSEN_COLUMNS)


@dataclass(frozen=True)
class ProgramAssumptions:
    """Everything `solve_program` is handed that is not the grid or the parcel.

    One object rather than five keyword arguments threaded through three
    functions, and frozen, because every one of these is a *stated assumption*
    and the rule this platform follows for those is that the row records the
    value that produced it - the same rule `comparables.IncomeAssumptions` and
    `LotProfilesConfig.max_built_area_m2` follow. `as_metadata` is what travels
    into the parquet and the jsonb.

    The defaults are `urban_rag.program`'s own module constants, so a run that
    passes nothing gets exactly the program that module documents: half a stall
    a dwelling, the Altus midpoints, twenty-five years straight line, three
    metres a dwelling storey and four a commercial one.
    """

    parking: ParkingRules = DEFAULT_PARKING
    construction: ConstructionCosts = DEFAULT_CONSTRUCTION
    non_residential: NonResidentialEconomics = DEFAULT_NON_RESIDENTIAL
    heights: StoreyHeights = DEFAULT_STOREY_HEIGHTS
    #: Seconds CP-SAT may spend on one envelope. A borough is tens of thousands
    #: of models of fifteen variables each and nearly all of them are solved in
    #: milliseconds; this bounds the handful that are not. A model that runs out
    #: comes back `FEASIBLE` or `UNKNOWN` rather than `OPTIMAL`, and the asset
    #: counts both, so a limit set too low is visible rather than silent.
    max_seconds: float = 10.0

    def as_metadata(self) -> dict[str, object]:
        """The object every row carries, so a program can be read back."""
        return {
            "stalls_per_dwelling": self.parking.stalls_per_dwelling,
            "stalls_per_1000_sqft": self.parking.stalls_per_1000_sqft,
            "underground_stall_area_sqft": self.parking.underground_area_sqft,
            "above_grade_stall_area_sqft": self.parking.above_grade_area_sqft,
            "underground_stall_cost_cad": self.parking.underground_cost_cad,
            "above_grade_stall_cost_cad": self.parking.above_grade_cost_cad,
            "max_underground_levels": self.parking.max_underground_levels,
            "residential_cost_per_sqft_cad": (
                self.construction.residential_cost_per_sqft
            ),
            "commercial_cost_per_sqft_cad": self.construction.commercial_cost_per_sqft,
            "industrial_cost_per_sqft_cad": self.construction.industrial_cost_per_sqft,
            "amortization_months": self.construction.amortization_months,
            "commercial_rent_per_sqft_year_cad": (
                self.non_residential.commercial_per_sqft_year
            ),
            "industrial_rent_per_sqft_year_cad": (
                self.non_residential.industrial_per_sqft_year
            ),
            "commercial_vacancy_pct": self.non_residential.commercial_vacancy_pct,
            "industrial_vacancy_pct": self.non_residential.industrial_vacancy_pct,
            "residential_storey_height_m": self.heights.residential_m,
            "commercial_storey_height_m": self.heights.commercial_m,
            "industrial_storey_height_m": self.heights.industrial_m,
            "above_grade_parking_storey_height_m": self.heights.above_grade_parking_m,
            "months_per_year": MONTHS_PER_YEAR,
            "max_seconds": self.max_seconds,
        }


# --------------------------------------------------------------------------
# the inputs
# --------------------------------------------------------------------------


def unit_economics(
    rents: pd.DataFrame, vacancy: pd.DataFrame
) -> tuple[UnitEconomics, tuple[str, ...]]:
    """CMHC's two borough grids, as the rent list `solve_program` reads.

    Returns the economics and the classes CMHC suppressed, which are two halves
    of the same answer: a class with no published rent has no key in the map,
    `solve_program` reports it in `unpriced_types` and declines to build it, and
    a borough where all four are suppressed produces an empty program on every
    lot. That last is a fact about the survey rather than about the borough's
    parcels, and returning the list is what lets an asset say so once instead of
    leaving it to be inferred from forty thousand empty rows.

    ``all`` is not read. It is CMHC's total across the four classes, and a
    solver handed it would treat it as a fifth kind of dwelling to build - at
    the average rent of the others, and at whatever area a schedule with no
    entry for it would refuse to price.

    Vacancy is taken from the ``all`` *structure* row of each bedroom class: the
    survey splits by structure as well, and a storey of the mid-rise the solver
    is designing is neither a row house nor exactly CMHC's "appartements et
    autres". A class the vacancy grid suppresses but the rent grid publishes is
    fully occupied rather than fully empty - the rent is what was measured, and
    inventing a vacancy for it would move the answer further than assuming
    none, which is the choice `IncomeAssumptions` makes for the same reason.
    """
    rent_by_type = _cells(rents, "average_rent_cad", key="bedroom_type")
    if "dwelling_type" in vacancy.columns:
        vacancy = vacancy[vacancy["dwelling_type"] == _ALL_DWELLINGS]
    vacancy_by_type = _cells(vacancy, "vacancy_rate_pct", key="bedroom_type")
    priced = {
        bedroom: rent_by_type[bedroom]
        for bedroom in PRICED_BEDROOM_TYPES
        if bedroom in rent_by_type
    }
    rates = {
        bedroom: vacancy_by_type[bedroom]
        for bedroom in PRICED_BEDROOM_TYPES
        if bedroom in vacancy_by_type
    }
    suppressed = tuple(
        bedroom for bedroom in PRICED_BEDROOM_TYPES if bedroom not in priced
    )
    return UnitEconomics(average_rent_cad=priced, vacancy_rate_pct=rates), suppressed


def zone_column_of(row: Mapping) -> ZoneColumn:
    """One row of `lot_zoning_envelopes`, back as the solver's input.

    The public inverse of `envelope_assets._column_row`, and the reason that
    asset writes every norm as its own column rather than as a blob: a row is
    turned back into the object it came from by name, so a norm added to
    `envelope_assets.NORM_FIELDS` reaches the solver by being added there and
    nowhere else.

    Raises `ProgramError` on a row that cannot be one - which is what
    `solver_ready` already promises will not happen, and is caught per row
    anyway, so a promise broken by a stale parquet costs one lot.
    """
    floors_max = _int_or_none(row.get("floors_max"))
    if floors_max is None:
        raise ProgramError(
            "no storey maximum (En etage), so the envelope has no ceiling"
        )
    return ZoneColumn(
        usages=tuple(str(usage) for usage in _json_list(row.get("usages"))),
        floors_max=floors_max,
        levels=frozenset(
            BuildingLevel(value) for value in _json_list(row.get("levels"))
        ),
        floors_min=_int_or_none(row.get("floors_min")) or 0,
        height_min_m=_float_or_none(row.get("height_min_m")),
        height_max_m=_float_or_none(row.get("height_max_m")),
        min_lot_width_m=_float_or_none(row.get("min_lot_width_m")),
        max_dwellings=_int_or_none(row.get("max_dwellings")),
        density_min=_float_or_none(row.get("density_min")),
        density_max=_float_or_none(row.get("density_max")),
        site_coverage_min_pct=_float_or_none(row.get("site_coverage_min_pct")),
        site_coverage_max_pct=_float_or_none(row.get("site_coverage_max_pct")),
        zone=_text_or_none(row.get("feature_id")),
    )


def lot_of(row: Mapping) -> Lot:
    """One row of `lot_zoning_envelopes`, back as the parcel the solver sizes.

    A missing frontage is 0 m rather than an error, which is the reading
    `envelope_assets.meets_min_lot_width` gives it too: a lot nothing measured
    satisfies no width minimum, and excluding it is the conservative answer.
    `buildable_area_m2` is absent on a partition where `lot_buildable_setbacks`
    has not run, and `Lot` reads `None` as "no margin cap known" rather than as
    no buildable area at all.
    """
    return Lot(
        area_m2=float(row["lot_area_m2"]),
        frontage_m=_float_or_none(row.get("primary_frontage_m")) or 0.0,
        lot_number=_text_or_none(row.get("lot_number")),
        buildable_area_m2=_float_or_none(row.get("buildable_area_m2")),
    )


# --------------------------------------------------------------------------
# solving
# --------------------------------------------------------------------------


def solve_envelopes(
    envelopes: pd.DataFrame,
    economics: UnitEconomics,
    *,
    assumptions: ProgramAssumptions | None = None,
) -> pd.DataFrame:
    """One `solve_program` call per solvable envelope row.

    ``envelopes`` is `lot_zoning_envelopes` for one partition, optionally with
    `buildable_area_m2` merged on from `lot_buildable_setbacks` at the same
    (lot, zone, column) grain. Rows authorising no dwelling, and rows
    `solver_ready` marks unreadable, are dropped rather than carried as
    failures: they are not candidates, and counting them is the asset's job.

    Returns one row per surviving candidate - `CANDIDATE_COLUMNS` then
    `PROGRAM_COLUMNS` - sorted by (lot, zone, column). An empty input yields an
    empty frame with those columns, so a partition whose grids all failed to
    parse writes a readable file rather than nothing.
    """
    assumptions = assumptions or ProgramAssumptions()
    candidates = candidate_envelopes(envelopes)
    rows = [
        {
            **{name: row.get(name) for name in CANDIDATE_COLUMNS},
            **_program_row(row, economics, assumptions),
        }
        for row in candidates.to_dict("records")
    ]
    frame = pd.DataFrame(rows, columns=[*CANDIDATE_COLUMNS, *PROGRAM_COLUMNS])
    return frame.sort_values(
        ["lot_uid", "feature_id", "column_index"], kind="stable"
    ).reset_index(drop=True)


def candidate_envelopes(envelopes: pd.DataFrame) -> pd.DataFrame:
    """The rows `solve_program` can be asked about.

    Both filters are upstream flags read rather than re-derived - see the module
    docstring. A frame missing either column is treated as though every row
    passed it, so a hand-built frame in a test need not carry columns the test
    is not about.
    """
    mask = pd.Series(True, index=envelopes.index)
    for flag in ("permits_residential", "solver_ready"):
        if flag in envelopes.columns:
            mask &= envelopes[flag].fillna(False).astype(bool)
    return envelopes[mask]


def program_row(program: DevelopmentProgram) -> dict:
    """A `DevelopmentProgram` flattened to the columns of the table.

    Public because a chosen program is the same shape as a candidate one, and
    two flatteners would be two schemas. The period is in every money column's
    name for the reason the module docstring gives: the objective is a month,
    the assessment side is a year, and a column called
    `net_operating_income_cad` would be an invitation to subtract one from the
    other.

    `residential_area_m2` is not on `DevelopmentProgram` and is derived here.
    `footprint x residential_floors` is the plate the dwellings stand on, and it
    is the like-for-like counterpart of the roll's floor area in a way
    `unit_area_m2` - a schedule of rentable areas - is not.
    """
    return {
        "status": program.status,
        "solved": program.solved,
        "solve_error": None,
        "monthly_net_operating_income_cad": program.net_operating_income,
        "annual_net_operating_income_cad": (
            program.net_operating_income * MONTHS_PER_YEAR
        ),
        "monthly_gross_revenue_cad": program.gross_revenue_cad,
        "annual_gross_revenue_cad": program.gross_revenue_cad * MONTHS_PER_YEAR,
        "num_dwellings": program.total_dwellings,
        "units": json.dumps(dict(program.units), ensure_ascii=False),
        "floors": program.floors,
        "height_m": program.height_m,
        "footprint_m2": program.footprint_m2,
        "gross_floor_area_m2": program.gross_floor_area_m2,
        "residential_area_m2": program.footprint_m2 * program.residential_floors,
        "unit_area_m2": program.unit_area_m2,
        "commercial_area_m2": program.commercial_area_m2,
        "industrial_area_m2": program.industrial_area_m2,
        "underground_area_m2": program.underground_area_m2,
        "residential_floors": program.residential_floors,
        "commercial_floors": program.commercial_floors,
        "industrial_floors": program.industrial_floors,
        "above_grade_parking_floors": program.above_grade_parking_floors,
        "underground_levels": program.underground_levels,
        "underground_stalls": program.underground_stalls,
        "above_grade_stalls": program.above_grade_stalls,
        "total_stalls": program.total_stalls,
        "construction_cost_cad": program.construction_cost_cad,
        "commercial_cost_cad": program.commercial_cost_cad,
        "industrial_cost_cad": program.industrial_cost_cad,
        "parking_cost_cad": program.parking_cost_cad,
        "total_capital_cost_cad": program.total_capital_cost_cad,
        "binding": json.dumps(list(program.binding), ensure_ascii=False),
        "unpriced_types": json.dumps(list(program.unpriced_types), ensure_ascii=False),
    }


def _program_row(
    row: Mapping, economics: UnitEconomics, assumptions: ProgramAssumptions
) -> dict:
    """One candidate's answer, or the reason it has none."""
    try:
        program = solve_program(
            zone_column_of(row),
            lot_of(row),
            economics,
            parking=assumptions.parking,
            construction=assumptions.construction,
            non_residential=assumptions.non_residential,
            heights=assumptions.heights,
            max_seconds=assumptions.max_seconds,
        )
    except (ProgramError, ValueError, KeyError) as exc:
        # `ValueError` and `KeyError` beside `ProgramError` on purpose: a stale
        # parquet can hand this a `levels` value the enum no longer has, or a
        # row with no `lot_area_m2` at all, and neither is worth a borough.
        return {**_ERROR_PROGRAM_ROW, "solve_error": str(exc)}
    return program_row(program)


#: Every program column at nothing, for a candidate whose model could not be
#: built. `ERROR` is not one of CP-SAT's statuses precisely so it cannot be
#: mistaken for one: `INFEASIBLE` is the solver's answer about a parcel, this
#: is the absence of an answer.
_ERROR_PROGRAM_ROW: dict = {
    **{name: 0.0 for name in _PROGRAM_FLOATS},
    **{name: 0 for name in _PROGRAM_COUNTS},
    "status": "ERROR",
    "solved": False,
    "solve_error": None,
    "units": "{}",
    "binding": "[]",
    "unpriced_types": "[]",
}


# --------------------------------------------------------------------------
# choosing
# --------------------------------------------------------------------------


def select_highest_best_use(
    programs: pd.DataFrame, envelopes: pd.DataFrame
) -> pd.DataFrame:
    """One row per lot: the program of the envelope that governs it.

    ``programs`` is `solve_envelopes`' output and ``envelopes`` is the frame it
    was built from - both, because a lot whose every envelope authorises
    commerce has no candidate row at all and would otherwise vanish from a table
    that is meant to be an inventory. Every lot the envelopes reach keeps a row,
    and `hbu_status` says which of `HBU_STATUSES` it is.

    The pick is the module docstring's: among the rows `governs_residential`
    marks, the one whose zone covers most of the lot, ties broken by income and
    then by column index. `num_candidates` and `num_zones` travel with it, so a
    lot where the choice was real is distinguishable from one where it was not.
    """
    lots = _lot_index(envelopes)
    if lots.empty:
        return pd.DataFrame(columns=list(HBU_COLUMNS))

    chosen = _chosen(programs)
    frame = lots.join(chosen, how="left")
    frame["hbu_status"] = _hbu_status(frame, programs)
    return frame.reset_index()[list(HBU_COLUMNS)]


def _chosen(programs: pd.DataFrame) -> pd.DataFrame:
    """The winning candidate of each lot, indexed by `lot_uid`."""
    if programs.empty:
        return pd.DataFrame(columns=list(_CHOSEN_COLUMNS)).rename_axis("lot_uid")
    solved = programs[
        programs["governs_residential"].fillna(False).astype(bool)
        & programs["solved"].fillna(False).astype(bool)
    ]
    return (
        solved.sort_values(
            [
                "lot_uid",
                "pct_of_lot",
                "monthly_net_operating_income_cad",
                "column_index",
            ],
            # Coverage first and descending: the zone that actually covers the
            # lot decides, and the income is only what separates two that cover
            # it equally.
            ascending=[True, False, False, True],
            kind="stable",
        )
        .drop_duplicates("lot_uid")
        .set_index("lot_uid")[list(_CHOSEN_COLUMNS)]
    )


def _lot_index(envelopes: pd.DataFrame) -> pd.DataFrame:
    """One row per lot the envelopes reach, with what it is and how many.

    `num_candidates` counts *candidates* rather than envelope rows: a lot under
    a grid with one Habitation column and three Commerce ones had one choice,
    not four, and reporting four would make the Habitation column look like
    three failed parses.
    """
    if envelopes.empty:
        empty = pd.DataFrame(
            columns=[name for name in _HBU_LOT_COLUMNS if name != "lot_uid"]
        )
        empty.index.name = "lot_uid"
        return empty
    candidates = candidate_envelopes(envelopes)
    per_lot = envelopes.groupby("lot_uid", sort=False).agg(
        lot_number=("lot_number", "first"),
        neighborhood=("neighborhood", "first"),
        scrape_date=("scrape_date", "first"),
        lot_area_m2=("lot_area_m2", "first"),
        primary_frontage_m=("primary_frontage_m", "first"),
        num_zones=("feature_id", "nunique"),
    )
    per_lot["num_candidates"] = _count_by_lot(candidates, per_lot.index)
    per_lot["num_governing_candidates"] = _count_by_lot(
        candidates[candidates["governs_residential"].fillna(False).astype(bool)]
        if "governs_residential" in candidates.columns
        else candidates.iloc[0:0],
        per_lot.index,
    )
    return per_lot


def _count_by_lot(frame: pd.DataFrame, index: pd.Index) -> pd.Series:
    """How many rows of ``frame`` each lot of ``index`` has - 0, not null."""
    if frame.empty:
        return pd.Series(0, index=index, dtype="int64")
    return (
        frame.groupby("lot_uid", sort=False)
        .size()
        .reindex(index)
        .fillna(0)
        .astype("int64")
    )


def _hbu_status(frame: pd.DataFrame, programs: pd.DataFrame) -> pd.Series:
    """Why each lot has the row it has - one of `HBU_STATUSES`.

    Written from the answer outwards, so what is reported is the *furthest* a
    lot got: a lot with a program is `solved` whatever else is true of it, and a
    lot without one is described by whether it had no candidates at all,
    candidates but none governing, a governing candidate the solver refused, or
    one it could not build a model from.
    """
    status = pd.Series("solved", index=frame.index, dtype="object")
    unsolved = frame["status"].isna()
    status[unsolved & (frame["num_candidates"] == 0)] = "no_residential_column"
    status[unsolved & (frame["num_candidates"] > 0)] = "no_governing_column"
    if programs.empty:
        return status
    governing = programs[programs["governs_residential"].fillna(False).astype(bool)]
    lots = frame.index.to_series()
    infeasible = set(
        governing.loc[
            ~governing["solved"].fillna(False).astype(bool)
            & (governing["status"] != "ERROR"),
            "lot_uid",
        ]
    )
    status[unsolved & lots.isin(infeasible)] = "infeasible"
    # Last, so a lot with one column that raised and another that was merely
    # infeasible is reported as the harder failure - the one that has a message
    # to read.
    errored = set(governing.loc[governing["status"] == "ERROR", "lot_uid"])
    status[unsolved & lots.isin(errored)] = "solver_error"
    return status


# --------------------------------------------------------------------------
# comparing
# --------------------------------------------------------------------------

#: The floor-area columns the roll's side of the gap is read from, keyed by the
#: income class both sides split on. `comparables._SUMMED_COLUMNS` is where they
#: are written and `comparables.INCOME_CLASSES` is the order.
_EXISTING_AREA_COLUMNS: Mapping[str, str] = {
    "residential": "residential_floor_area_m2",
    "commercial": "commercial_floor_area_m2",
    "industrial": "industrial_floor_area_m2",
}

#: The same three on the solver's side. Residential is the *plate*, not the unit
#: schedule - see the module docstring on gross floor area.
_HBU_AREA_COLUMNS: Mapping[str, str] = {
    "residential": "residential_area_m2",
    "commercial": "commercial_area_m2",
    "industrial": "industrial_area_m2",
}

#: What the roll's side of a lot contributes, where
#: `lot_assessment_comparables` has a row for it. Narrowed rather than joined
#: whole, so a column that table gains does not silently arrive here under a
#: name this one already uses.
_EXISTING_COLUMNS: tuple[str, ...] = (
    *_EXISTING_AREA_COLUMNS.values(),
    "num_dwellings",
    "num_assessment_units",
    "gross_income_cad",
    "net_operating_income_cad",
    "total_assessed_value",
    "cap_rate_pct",
    "dominant_use_code",
    "dominant_income_class",
    # What age cost the standing building. `net_operating_income_cad` above is
    # already net of it; these three are what say by how much, and are what
    # make the two sides' differing ratios readable off one row.
    "building_age_years",
    "maintenance_premium",
    "effective_operating_expense_ratio",
)


def use_gap(
    hbu: pd.DataFrame,
    existing: pd.DataFrame,
    *,
    operating_expense_ratio: float,
) -> pd.DataFrame:
    """The building that stands, the building that could, and the difference.

    ``hbu`` is `select_highest_best_use`' output and ``existing`` is
    `lot_assessment_comparables`, joined on the lot number. One row per lot of
    ``hbu`` and no more: this is a table about zoning envelopes, and a lot the
    grids do not reach has nothing to compare against however well the roll
    describes it.

    ``operating_expense_ratio`` is the one the comparables asset used, read off
    its own rows by `operating_expense_ratio_of` rather than restated - see the
    module docstring. It is applied to the *solver's* gross to put the two NOIs
    on one definition; the existing NOI is carried through as that asset
    published it.

    Every gap is ``hbu - existing`` and is null where either side is, because a
    lot the roll never reached is not a lot with no floor on it. The one
    exception is deliberate and named: `is_underbuilt` reads a missing existing
    area as nothing standing, since a parcel with an envelope and no assessed
    building is exactly the case that column exists to find.
    """
    frame = hbu.copy()
    joined = _join_existing(frame, existing)

    for class_name in INCOME_CLASSES:
        built = _numeric(joined, _EXISTING_AREA_COLUMNS[class_name])
        proposed = _numeric(frame, _HBU_AREA_COLUMNS[class_name])
        frame[f"existing_{class_name}_floor_area_m2"] = built
        frame[f"hbu_{class_name}_floor_area_m2"] = proposed
        gap = proposed - built
        frame[f"{class_name}_floor_area_gap_m2"] = gap
        frame[f"{class_name}_floor_area_gap_sqft"] = gap / M2_PER_SQFT

    existing_total = _sum_classes(frame, "existing_{}_floor_area_m2")
    hbu_total = _sum_classes(frame, "hbu_{}_floor_area_m2")
    frame["existing_floor_area_m2"] = existing_total
    frame["hbu_floor_area_m2"] = hbu_total
    frame["floor_area_gap_m2"] = hbu_total - existing_total
    frame["floor_area_gap_sqft"] = (hbu_total - existing_total) / M2_PER_SQFT

    # The unit schedule beside the plate, so the corridors the residential rate
    # leaves unpriced are visible rather than only implied by the two differing.
    frame["hbu_unit_area_m2"] = _numeric(frame, "unit_area_m2")

    frame["existing_num_dwellings"] = _numeric(joined, "num_dwellings")
    frame["hbu_num_dwellings"] = _numeric(frame, "num_dwellings")
    frame["dwelling_gap"] = frame["hbu_num_dwellings"] - frame["existing_num_dwellings"]

    frame["existing_annual_gross_income_cad"] = _numeric(joined, "gross_income_cad")
    frame["hbu_annual_gross_income_cad"] = _numeric(frame, "annual_gross_revenue_cad")
    frame["annual_gross_income_gap_cad"] = (
        frame["hbu_annual_gross_income_cad"] - frame["existing_annual_gross_income_cad"]
    )

    # One definition, both sides. The existing figure is `comparables`' own -
    # carried, not recomputed, so the two tables cannot disagree about what a
    # standing building earns.
    frame["existing_annual_stabilised_noi_cad"] = _numeric(
        joined, "net_operating_income_cad"
    )
    # The proposal is a new building, so it is charged the base ratio and no
    # maintenance premium - see the module docstring. The existing side is
    # already net of its own age and is carried, not recomputed.
    frame["hbu_annual_stabilised_noi_cad"] = frame["hbu_annual_gross_income_cad"] * (
        1.0 - operating_expense_ratio
    )
    frame["annual_stabilised_noi_gap_cad"] = (
        frame["hbu_annual_stabilised_noi_cad"]
        - frame["existing_annual_stabilised_noi_cad"]
    )
    # And the solver's own objective, which is a different thing and is kept
    # under a name that says so: income after the amortised cost of building it,
    # before a dollar of operating expense.
    frame["hbu_annual_noi_after_construction_cad"] = _numeric(
        frame, "annual_net_operating_income_cad"
    )
    frame["hbu_total_capital_cost_cad"] = _numeric(frame, "total_capital_cost_cad")
    frame["operating_expense_ratio"] = operating_expense_ratio
    # The same number under the name that says which building it applies to,
    # and the standing building's beside it. A reader comparing two NOIs needs
    # to see that they were netted differently and by how much, rather than
    # discovering it from the gap.
    frame["hbu_operating_expense_ratio"] = operating_expense_ratio
    frame["existing_effective_operating_expense_ratio"] = _numeric(
        joined, "effective_operating_expense_ratio"
    )
    frame["existing_building_age_years"] = _numeric(joined, "building_age_years")
    frame["existing_maintenance_premium"] = _numeric(joined, "maintenance_premium")
    # What the age premium alone is worth to a redevelopment, in dollars a
    # year: the standing building's gross charged at the new-build ratio,
    # against the same gross charged at its own. Null where either is, and
    # zero on a building the curve found new - which is the honest answer for
    # a parcel whose maintenance redevelopment would not improve.
    frame["existing_maintenance_penalty_cad"] = frame[
        "existing_annual_gross_income_cad"
    ] * frame["existing_maintenance_premium"]

    frame["existing_num_assessment_units"] = _numeric(joined, "num_assessment_units")
    frame["existing_total_assessed_value"] = _numeric(joined, "total_assessed_value")
    frame["existing_cap_rate_pct"] = _numeric(joined, "cap_rate_pct")
    frame["existing_dominant_use_code"] = _column_or_null(joined, "dominant_use_code")
    frame["existing_dominant_income_class"] = _column_or_null(
        joined, "dominant_income_class"
    )
    # Read against 0 rather than against null on the existing side only: a
    # parcel with an envelope and nothing assessed on it is the case this
    # column is for. Null on the *hbu* side stays null - a lot with no program
    # is not a lot that is built out.
    frame["is_underbuilt"] = frame["hbu_floor_area_m2"].notna() & (
        frame["hbu_floor_area_m2"] > frame["existing_floor_area_m2"].fillna(0.0)
    )
    frame["has_assessment"] = joined.notna().any(axis=1) if len(joined.columns) else False
    return frame


def operating_expense_ratio_of(existing: pd.DataFrame) -> float:
    """The **base** ratio `lot_assessment_comparables` ran with.

    Read off the `income_assumptions` object that asset writes onto every row,
    so the two sides of `use_gap` are netted with one number rather than with a
    config here and a config there that drift apart. Falls back to
    `IncomeAssumptions`' default where the column is absent - an older parquet,
    or a hand-built frame in a test - which is the same 0.35 that asset would
    itself have used.

    The base and not the effective ratio, and the distinction is load-bearing:
    `operating_expense_ratio` in that object is what a *new* building costs to
    run, and the per-lot `effective_operating_expense_ratio` column is that
    plus the standing building's age premium. This is the ratio for the
    building `use_gap` proposes, which has no age yet - so the base is the
    right one, and a partition written before the premium existed carried the
    same number under the same key and still reads correctly here.
    """
    default = IncomeAssumptions().operating_expense_ratio
    if existing.empty or "income_assumptions" not in existing.columns:
        return default
    values = existing["income_assumptions"].dropna()
    if not len(values):
        return default
    try:
        payload = json.loads(values.iloc[0])
    except (TypeError, ValueError):
        return default
    ratio = payload.get("operating_expense_ratio")
    return float(ratio) if isinstance(ratio, (int, float)) else default


def _join_existing(hbu: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    """The roll's side of every lot, aligned to ``hbu``'s rows.

    A left join on the lot number and nothing cleverer: both sides carry
    Infolot's own spelling of it - `lot_zoning_envelopes` from the cadastre it
    was joined against, `lot_assessment_comparables` from the same polygons - so
    the normalisation `role_assets.lot_key` exists for is a layer upstream of
    here and already done.

    Duplicates on the right are dropped rather than allowed to multiply the
    left: that table is one row per lot number by construction, and a stale
    partition that is not should cost the extra rows rather than leave this one
    with two answers for a lot.
    """
    empty = pd.DataFrame(index=hbu.index)
    if existing.empty or "lot_number" not in hbu.columns:
        return empty
    key = "NO_LOT" if "NO_LOT" in existing.columns else "lot_number"
    if key not in existing.columns:
        return empty
    wanted = [name for name in _EXISTING_COLUMNS if name in existing.columns]
    if not wanted:
        return empty
    right = existing.drop_duplicates(key).set_index(key)[wanted]
    aligned = right.reindex(hbu["lot_number"].to_numpy())
    aligned.index = hbu.index
    return aligned


# --------------------------------------------------------------------------
# small conversions
# --------------------------------------------------------------------------


def _cells(frame: pd.DataFrame, column: str, *, key: str) -> dict[str, float]:
    """One CMHC grid as a dict, suppressed cells left out rather than zeroed."""
    if frame.empty or column not in frame.columns or key not in frame.columns:
        return {}
    values = pd.to_numeric(frame[column], errors="coerce")
    return {
        str(name): float(value)
        for name, value in zip(frame[key], values)
        if pd.notna(value)
    }


def _sum_classes(frame: pd.DataFrame, template: str) -> pd.Series:
    """The three income classes added, null only where none of them was stated."""
    parts = [frame[template.format(name)] for name in INCOME_CLASSES]
    return pd.concat(parts, axis=1).sum(axis=1, min_count=1)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    """``frame[column]`` as float64, or all-null where it is not there."""
    if column not in frame.columns:
        return pd.Series(float("nan"), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").astype("float64")


def _column_or_null(frame: pd.DataFrame, column: str) -> pd.Series:
    """``frame[column]`` as it is, or all-null where it is not there."""
    if column not in frame.columns:
        return pd.Series(None, index=frame.index, dtype="object")
    return frame[column]


def _json_list(value) -> Sequence:
    """A JSON array written by `envelope_assets`, back as a list."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return []
    return decoded if isinstance(decoded, list) else []


def _float_or_none(value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _int_or_none(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _text_or_none(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)
