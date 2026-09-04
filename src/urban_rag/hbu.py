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
`lot_zoning_envelopes`, and the interesting part is which rows. A row is
solved when it authorises at least one of the three families the solver
prices - `permits_residential`, `permits_commercial` or `permits_industrial`
- and is `solver_ready`, the latter because that flag is the solver's own
constructor having already accepted the column. None of this is re-decided
here: the flags are written by `envelope_assets`, and `ensure_use_flags` is
the one documented fallback - a partition written before the commercial and
industrial flags existed gets them recomputed from its own `usages`, by the
same functions the asset calls, so an old parquet solves without a re-parse
of the grids.

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
the shape of the choice matters: what may be chosen is the *use*, never the
rules.

A lot can carry several envelope rows for three reasons, and only one of them
is anybody's choice. Within one zone and one usage family, a grid authorises
dwellings in more than one column and distinguishes them by *Largeur du
terrain min*: the column a parcel of this width is written for is
`select_governing_column`'s to pick, `envelope_assets` marks it
`governs_residential` - and `governs_commercial`, `governs_industrial` for
the families beside it - and taking a higher-earning column of the *same*
family instead would be reporting a program under rules the parcel may not
build to. Across zones, a lot on a boundary picks up a sliver of its
neighbour's zoning because two publishers drew two lines - that is not two
sets of rules the owner may choose between, it is one set and a mapping
disagreement, and `pct_of_lot` is what says which is the real one.

Across *families*, there is no choice to make, because there is nothing to
choose between. A zone whose grid writes an ``H.2`` column and a ``C.4``
column beside it authorises **both in one building** - retail at grade with
dwellings above is the ordinary form of a Montreal commercial street - and
picking the better of two pure programs cannot propose it. The governing
columns of a zone are therefore assembled into one `program.ZoneEnvelope` and
solved **together**, with the floor area split between housing, commerce and
industry a decision the model makes. `lot_development_programs` carries one
row per (lot, zone) rather than one per column, and `hbu_dominant_use` says in
one word what kind of building the mix came out as - `mixed` where no class
holds `DOMINANT_USE_SHARE` of the usage floor.

The pure programs are not lost by this: each is a feasible point of the same
model - a column's norms bind only while the family it heads is built - so the
answer is never worse than the best single-family building, and is better
exactly where a mix pays. Across the 425 solvable zones of
Villeray-Saint-Michel-Parc-Extension it is unchanged on 373 and higher on 48,
by a median of $136 000.

The maximisation over the *mix* is inside `solve_program`, over the dwellings
and the floor one envelope can hold - now including the split between the
families. What is left to maximise here is only which *zone*, on a lot two of
them overlap, and `pct_of_lot` settles that before profit is consulted at all.

A lot with candidates but no governing one keeps its row and says so in
`hbu_status`: `no_governing_column` is almost always a parcel with no measured
frontage under a grid that states a width minimum, which reads as 0 m and
qualifies for nothing. That is a gap in `lot_frontage` rather than in the grid,
and it is worth being able to count.

----------------------------------------------------------------------------
What is not a development site
----------------------------------------------------------------------------

Two kinds of parcel get no program at all, and neither is a failure to answer:
they are the answer. Both are named in `hbu_status` rather than dropped,
because a table that is meant to be an inventory of a borough should be able to
say *why* the park is not on the shortlist.

**A parcel that is the road.** The zoning grid says nothing useful about one -
the zone polygon over a roadway states what may be built on the *block it
serves*, and a solver handed the roadway's own area will happily propose eleven
dwellings on it. This overrides the zoning entirely, which is why `_hbu_status`
applies it last.

Two predicates answer it and both are needed, because they are two different
publishers looking at the same ground. `road_parcel_lots` reads the CUBF: every
code from 4510 to 4599 is a piece of the public way, and the roll carries them
at a nominal hundred dollars with no floor, no storeys and no dwellings.
`cadastral_road_lots` reads the geometry instead - the parcels a *géobase
double* side runs inside, which `lot_frontage` already identifies because it
cannot measure a frontage without them.

**The roll alone was not enough, and the gap was not marginal.** Montreal does
not enter its own roadways on the assessment roll, so the CUBF gate found 48 of
Villeray-Saint-Michel-Parc-Extension's street parcels where the cadastre and the
street network together find some 1,400. Avenue Querbes between Ball and
Saint-Roch is lots 2 249 179 and 2 249 339 - two 3,300 m² strips, each carrying
some 365 m of geobase street line and neither on the roll - and under the zoning
of the blocks either side of it the solver built on both, put them in the
redevelopment gap, and ranked them among the borough's investment
opportunities. The union of the two predicates is what closes that; the sets
overlap little and neither is a superset of the other.

**A parcel whose zone authorises only Équipements collectifs.** A park, a
school, a hospital, a cemetery. `program` has never priced the ``E`` family -
a school is not something a proforma rents by the square foot - so those
columns were never candidates, and what made these parcels development sites
anyway was the *other* zone: a lot on a zone boundary picks up a sliver of its
neighbour's, and `_chosen` resolved between zones only among the rows that had
produced a program, so a zone that produced none dropped silently out of the
contest and a 1.7% sliver of the block next door answered for the whole parcel.
`governing_zone` closes that by deciding which zone speaks for a lot *before*
the candidates are read, on the same coverage rule `_chosen` always used. On
the 2026 Villeray-Saint-Michel-Parc-Extension partition it removes 343 such
answers, 206 of them on Équipements parcels, Parc Jarry among them, and takes
the borough's candidate rows from 75,007 to 64,493 - which is that many CP-SAT
models not run on parcels nobody may build on.

The road gate and the equipment gate are independent of each other, and of the
two road predicates the roll is much the narrower: it reaches 21,862 of the
borough's 24,952 parcels, and among those it calls 48 a road. The cadastral
predicate is what reaches the rest, and it is a different kind of claim - it
infers the street from where the street network runs rather than from what a
publisher wrote down - which is why it lives in `lot_frontage`, where the same
inference is already load-bearing and already measured, rather than being
invented here. `postgis.DEFAULT_ROAD_LOT_MIN_STREET_M` is the whole of its
judgement and is argued there.

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
`hbu_annual_noi_after_construction_cad`, the legacy monthly figure annualised,
with `hbu_total_capital_cost_cad` beside it. And the verdict this module used
to stop short of is now stated, because the solve carries a discount rate of
its own: `hbu_npv_cad` prices redeveloping, `existing_present_value_cad`
prices keeping the standing building at the same `InvestmentAssumptions`, and
`redevelopment_npv_gain_cad` is the difference - land in neither side, since
the owner holds it in both futures.

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
import math
from dataclasses import dataclass
from typing import Mapping, Sequence

import pandas as pd

from urban_rag.comparables import (
    INCOME_CLASSES,
    IncomeAssumptions,
    is_road_use_code,
)
from urban_rag.program import (
    DEFAULT_CONSTRUCTION,
    DEFAULT_INVESTMENT,
    DEFAULT_NON_RESIDENTIAL,
    DEFAULT_PARKING,
    DEFAULT_STOREY_HEIGHTS,
    M2_PER_SQFT,
    MONTHS_PER_YEAR,
    BuildingLevel,
    ConstructionCosts,
    DevelopmentProgram,
    InvestmentAssumptions,
    Lot,
    NonResidentialEconomics,
    ParkingRules,
    ProgramError,
    StoreyHeights,
    UnitEconomics,
    ZoneColumn,
    ZoneEnvelope,
    floor_stack,
    is_commercial_usage,
    is_equipment_usage,
    is_industrial_usage,
    is_residential_usage,
    select_governing_column,
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

#: The column of `lot_frontage`'s `road_lots.parquet` that says the geometry
#: was a close call - see `cadastral_road_lots`, which is the only reader.
#:
#: Declared here rather than beside the rest of that file's schema in
#: `urban_rag.postgis`, because it is the one column of it this module
#: *interprets* rather than passes through, and because a name defined there
#: could only reach this one through an import of the I/O layer that this
#: module deliberately does not take. `frontage_assets` writes it and imports
#: the spelling from here.
ROAD_LOT_FLAG_COLUMN = "near_cutoff"

#: Why a lot has the row it has. One value per row of `lot_highest_best_use`,
#: so "the borough has four hundred unanswered lots" is a `GROUP BY` rather
#: than a set of nulls to interpret.
HBU_STATUSES: tuple[str, ...] = (
    # A governing envelope was solved, and the row carries its program.
    "solved",
    # This parcel is a street, a lane, a highway or a right of way - either
    # because the roll files it under a CUBF road code or because a geobase
    # double side runs down the inside of it. Reported before the zoning is
    # consulted at all, because the zone polygon over a lane says what may be
    # built on the *block*, not on the roadway, and a parcel nobody may build
    # on is not a development site whatever the grid permits.
    "road_parcel",
    # The zone governing the lot authorises *Équipements collectifs et
    # institutionnels* and nothing this module prices: a park, a school, a
    # cemetery, a hospital. Split out of `no_candidate_column` because the two
    # are different facts - this is a grid that was read and says the parcel is
    # not for sale as floor area, that one is a grid that named no usage at all.
    "equipment_zone",
    # Every envelope covering the lot authorises none of the usages the
    # solver prices - Habitation, Commerce or Industrie - and none of them is
    # an Équipements column either: a grid with no usage row this parser
    # recognised. Pure `C` and `I` zones no longer land here: they solve like
    # everything else now.
    "no_candidate_column",
    # Candidate columns exist and none governs this parcel. Nearly always a
    # lot with no measured frontage under a grid stating *Largeur du terrain
    # min*: a missing frontage reads as 0 m and qualifies for nothing.
    "no_governing_column",
    # A governing column was solved and none has a feasible program - a
    # minimum the parcel cannot meet, or stalls it has nowhere to put.
    "infeasible",
    # A governing column could not be turned into a model at all;
    # `solve_error` carries what it said.
    "solver_error",
)

#: The three usage families the solver prices, in the order the flags are
#: read: the `permits_*` and `governs_*` column suffixes, and the classes a
#: chosen program's floor is split into.
USE_FAMILIES: tuple[str, ...] = ("residential", "commercial", "industrial")

#: The `governs_*` flag columns, one per family. `envelope_assets` writes all
#: three; `ensure_use_flags` recomputes any a pre-existing parquet lacks.
GOVERNS_COLUMNS: tuple[str, ...] = tuple(
    f"governs_{family}" for family in USE_FAMILIES
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
    "npv_cad",
    "present_value_cad",
    "annual_stabilised_noi_cad",
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
    "floor_stack",
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
    "npv_cad",
    "present_value_cad",
    "annual_stabilised_noi_cad",
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
    "permits_residential",
    "permits_commercial",
    "permits_industrial",
    "governs_residential",
    "governs_commercial",
    "governs_industrial",
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
    "permits_residential",
    "permits_commercial",
    "permits_industrial",
    "governs_residential",
    "governs_commercial",
    "governs_industrial",
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
#: `hbu_dominant_use` is the one column computed here rather than carried: a
#: reader's first question about a chosen program is *which kind of building
#: it is*, and answering it should not take four area columns and a rule.
HBU_COLUMNS: tuple[str, ...] = (
    *_HBU_LOT_COLUMNS,
    *_CHOSEN_COLUMNS,
    "hbu_dominant_use",
)


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
    investment: InvestmentAssumptions = DEFAULT_INVESTMENT
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
            "discount_rate_pct": self.investment.discount_rate_pct,
            "hold_years": self.investment.hold_years,
            "terminal_cap_rate_pct": self.investment.terminal_cap_rate_pct,
            "operating_expense_ratio": self.investment.operating_expense_ratio,
            "new_build_rent_premium_pct": (
                self.investment.new_build_rent_premium_pct
            ),
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

    Returns one row per surviving **(lot, zone)** - `CANDIDATE_COLUMNS` then
    `PROGRAM_COLUMNS` - sorted by (lot, zone, column). An empty input yields an
    empty frame with those columns, so a partition whose grids all failed to
    parse writes a readable file rather than nothing.

    **One solve per zone, not per column.** A grid states one usage family per
    column in most boroughs, so a zone that permits housing and commerce prints
    two columns and never one. Solving each apart and keeping the better answer
    can only return the better *pure* building, and the building the zone
    actually permits - retail at grade with dwellings above - is neither of
    them. The columns governing a lot are assembled into one `ZoneEnvelope`
    and solved together, with the floor area split between the families a
    decision the model makes rather than one made for it. The row carries the
    identity of the lowest-indexed governing column and the usages of all of
    them, because the answer is the zone's rather than any one column's.
    """
    assumptions = assumptions or ProgramAssumptions()
    envelopes = ensure_use_flags(envelopes)
    candidates = candidate_envelopes(envelopes)
    if candidates.empty:
        return pd.DataFrame(columns=[*CANDIDATE_COLUMNS, *PROGRAM_COLUMNS])
    rows = [
        _envelope_row(group, economics, assumptions)
        for _, group in candidates.groupby(["lot_uid", "feature_id"], sort=False)
    ]
    frame = pd.DataFrame(rows, columns=[*CANDIDATE_COLUMNS, *PROGRAM_COLUMNS])
    return frame.sort_values(
        ["lot_uid", "feature_id", "column_index"], kind="stable"
    ).reset_index(drop=True)


def _envelope_row(
    group: pd.DataFrame, economics: UnitEconomics, assumptions: ProgramAssumptions
) -> dict:
    """One zone's columns, solved together as the single building they permit.

    The identity columns come from the lowest-indexed row that governs any
    family - an arbitrary but stable pick among rows that agree on everything
    the lot's identity is made of - and the usage flags come from the envelope,
    which is what the program was actually solved against.
    """
    parsed: list[tuple[dict, ZoneColumn]] = []
    for record in group.to_dict("records"):
        try:
            parsed.append((record, zone_column_of(record)))
        except (ProgramError, ValueError, KeyError):
            # A row `solver_ready` promised would parse and did not. It costs
            # this column rather than the zone: the others still describe a
            # building, and dropping the zone would lose them too.
            continue
    if not parsed:
        return {
            **_identity_of([record for record, _ in group.to_dict("records")] or []),
            **_ERROR_PROGRAM_ROW,
            "solve_error": "no column in this zone parsed into a solver input",
        }

    # The asset's own `governs_*` flags decide which column speaks for each
    # family, not a fresh `select_governing_column` pass over these rows. The
    # module docstring's rule - the flags are upstream columns read rather than
    # re-derived - and `ensure_use_flags` is what fills them in on a parquet
    # written before they existed. Recomputing here would quietly overrule a
    # writer that knows things this frame does not carry.
    governing: dict[str, ZoneColumn | None] = {}
    for family in USE_FAMILIES:
        governing[family] = next(
            (
                zone_column
                for record, zone_column in parsed
                if _flag(record, f"governs_{family}")
                and _flag(record, f"permits_{family}")
            ),
            None,
        )
    envelope = ZoneEnvelope(
        residential=governing["residential"],
        commercial=governing["commercial"],
        industrial=governing["industrial"],
    )

    governed = [
        record
        for record, zone_column in parsed
        if any(zone_column is chosen for chosen in envelope.columns)
    ]
    identity = _identity_of(governed or [record for record, _ in parsed])
    identity["usages"] = json.dumps(
        list(envelope.usages if not envelope.is_empty else _all_usages(parsed)),
        ensure_ascii=False,
    )
    for family in USE_FAMILIES:
        identity[f"permits_{family}"] = any(
            _flag(record, f"permits_{family}") for record, _ in parsed
        )
        # One row now stands for the whole zone, so it governs a family exactly
        # when some column of the zone did. `_chosen` reads these to decide the
        # row is a building the owner may put up, and a zone where nothing
        # governs still reports `no_governing_column` through them.
        identity[f"governs_{family}"] = governing[family] is not None

    if envelope.is_empty:
        # Nothing governs this parcel - every column states a *Largeur du
        # terrain min* the frontage does not meet. The program is still solved
        # and reported, from the columns that merely *permit*, so the row says
        # what the zone would allow a wider lot; the governs flags above are
        # what keep `select_highest_best_use` from choosing it.
        envelope = ZoneEnvelope.of([zone_column for _, zone_column in parsed], 0.0)
        if envelope.is_empty:
            envelope = ZoneEnvelope.single(parsed[0][1])
    return {
        **identity,
        **_program_row(parsed[0][0], economics, assumptions, envelope),
    }


def _all_usages(parsed: Sequence[tuple[Mapping, ZoneColumn]]) -> tuple[str, ...]:
    """Every usage code the zone's parsed columns carry, deduplicated."""
    codes: list[str] = []
    for _, zone_column in parsed:
        for usage in zone_column.usages:
            if usage not in codes:
                codes.append(usage)
    return tuple(codes)


def _flag(record: Mapping, name: str) -> bool:
    """One boolean column of a row, with a null read as False."""
    value = record.get(name)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return False
    return bool(value)


def _identity_of(records: Sequence[Mapping]) -> dict:
    """`CANDIDATE_COLUMNS` for a zone, from its lowest-indexed row."""
    first = min(records, key=lambda record: _column_index_of(record))
    return {name: first.get(name) for name in CANDIDATE_COLUMNS}


def _column_index_of(record: Mapping) -> int:
    index = _int_or_none(record.get("column_index"))
    return 0 if index is None else index


def _frontage_of(record: Mapping) -> float:
    """The lot's primary frontage, as `select_governing_column` reads it.

    Absent frontage is 0 m, which qualifies for the columns printing no
    *Largeur du terrain min* and for no others - the same reading
    `envelope_assets` gives it, and the reason `no_governing_column` is
    nearly always a lot the frontage asset could not measure.
    """
    frontage = _float_or_none(record.get("primary_frontage_m"))
    return 0.0 if frontage is None else frontage


def governing_zone(envelopes: pd.DataFrame) -> pd.Series:
    """The one zone that speaks for each lot, keyed by `lot_uid`.

    The zone covering most of the parcel, which is the rule `_chosen` has
    always applied between two solved programs and the module docstring has
    always stated: two zones on one lot are two publishers' lines disagreeing,
    not a menu the owner may order from.

    **Applied before the candidates are read, which is the point.** Deciding
    between zones only among the rows that produced a program lets a zone that
    produced none drop silently out of the contest, and the parcels whose zone
    produces none are exactly the ones this is about - a park, a school, a
    cemetery. On the 2026 Villeray-Saint-Michel-Parc-Extension partition, 343
    lots were answered by a zone that is not theirs, 206 of them parcels whose
    own zoning is *Équipements collectifs*. Parc Jarry is the case to picture:
    1.59 km2 carrying 31 envelope rows over 14 zones, its own E04-019 covering
    98.3% of it and the other thirteen between 1.7% and 0.000022% - a few
    square centimetres of a residential zone at the corner of the park - and
    any one of those slivers was enough to report it as a development site.

    **It ranks the zones the envelopes carry, not the zones on the map.**
    `lot_zoning_envelopes` inner-joins the grid columns, so a zone whose PDF
    did not parse contributes no row and cannot be picked here - 36 of the 633
    VSMPE zones. A lot whose true dominant zone is one of those is governed by
    the best-covered zone that *did* parse, which is the same answer the rest
    of this module has always given and is worth knowing when reading a
    surprising one: `zoning_grid_columns`' `num_documents_failed` is where the
    coverage is reported.

    Ties are broken on `feature_id` so a lot split exactly evenly answers to
    the same zone on every run rather than to whichever the join happened to
    place first. A frame carrying no `pct_of_lot` - a hand-built one in a test
    - leaves every zone in, since there is nothing to rank them by.

    **The smallest of those slivers no longer reach here.**
    `EnvelopeConfig.min_overlap_m2` drops a zone clipping under a square metre
    of a lot before `lot_zoning_envelopes` writes a row for it - the few square
    centimetres at the corner of the park above is one of them. That changes
    the counts quoted here, which were measured before it: this still decides
    every case, and now has fewer to decide. Nothing about the rule changes,
    since the rows removed are the ones it was already ranking last.
    """
    if "lot_uid" not in envelopes.columns or "feature_id" not in envelopes.columns:
        return pd.Series(dtype="object")
    if "pct_of_lot" not in envelopes.columns:
        return pd.Series(dtype="object")
    ranked = envelopes.sort_values(
        ["lot_uid", "pct_of_lot", "feature_id"],
        ascending=[True, False, True],
        kind="stable",
    ).drop_duplicates("lot_uid", keep="first")
    return ranked.set_index("lot_uid")["feature_id"]


def candidate_envelopes(envelopes: pd.DataFrame) -> pd.DataFrame:
    """The rows `solve_program` can be asked about.

    A candidate sits in the zone that governs its lot, authorises at least one
    of the three priced families and parses into a solver input. The flags are
    upstream columns read rather than re-derived - see the module docstring -
    with `ensure_use_flags` as the documented fallback for a parquet written
    before the commercial and industrial ones existed. A frame missing every
    permits flag is treated as though each row passed, so a hand-built frame in
    a test need not carry columns the test is not about.

    `governing_zone` is why an Équipements parcel now stays one: its columns
    were never candidates, and what used to make it a development site anyway
    was a sliver of the zone next door.
    """
    envelopes = ensure_use_flags(envelopes)
    permits = [
        envelopes[f"permits_{family}"].fillna(False).astype(bool)
        for family in USE_FAMILIES
        if f"permits_{family}" in envelopes.columns
    ]
    if permits:
        mask = permits[0]
        for flag in permits[1:]:
            mask |= flag
    else:
        mask = pd.Series(True, index=envelopes.index)
    if "solver_ready" in envelopes.columns:
        mask &= envelopes["solver_ready"].fillna(False).astype(bool)
    zones = governing_zone(envelopes)
    if not zones.empty:
        mask &= envelopes["feature_id"].eq(
            envelopes["lot_uid"].map(zones)
        )
    return envelopes[mask]


def ensure_use_flags(envelopes: pd.DataFrame) -> pd.DataFrame:
    """The envelopes, with every permits and governs flag present.

    `envelope_assets` writes all six; a partition written before the
    commercial and industrial ones existed carries only the residential pair,
    and this recomputes the missing four from columns every partition has -
    `usages` for the permits flags, `min_lot_width_m` and the frontage for
    the governs ones, by the same `select_governing_column` rule the asset
    itself calls. The same posture `operating_expense_ratio_of` takes to an
    older parquet: the writer is upstream, the fallback is here and says so.

    Columns already present are left exactly as written - this fills gaps,
    it does not audit the writer.
    """
    frame = envelopes.copy()
    if frame.empty:
        for name in (
            "permits_residential",
            "permits_commercial",
            "permits_industrial",
            *GOVERNS_COLUMNS,
        ):
            if name not in frame.columns:
                frame[name] = pd.Series(dtype="bool")
        return frame

    if "usages" not in frame.columns:
        # A hand-built frame with no usage codes states nothing to derive
        # flags from; the flags it does carry are read as written and the
        # rest stay absent, which `candidate_envelopes` reads as passing.
        return frame

    usages = [_json_list(value) for value in frame["usages"]]
    for family, matches in (
        ("residential", is_residential_usage),
        ("commercial", is_commercial_usage),
        ("industrial", is_industrial_usage),
    ):
        name = f"permits_{family}"
        if name in frame.columns:
            continue
        frame[name] = [
            any(matches(str(usage)) for usage in row_usages) for row_usages in usages
        ]

    missing_governs = [name for name in GOVERNS_COLUMNS if name not in frame.columns]
    if missing_governs and not {"lot_uid", "feature_id"} <= set(frame.columns):
        # A hand-built frame with no grain to group on: nothing can govern,
        # and saying so beats a KeyError inside a groupby.
        for name in missing_governs:
            frame[name] = False
        missing_governs = []
    if missing_governs:
        for name in missing_governs:
            frame[name] = False
        eligible = frame
        if "solver_ready" in frame.columns:
            eligible = frame[frame["solver_ready"].fillna(False).astype(bool)]
        for _, group in eligible.groupby(["lot_uid", "feature_id"], sort=False):
            frontage = 0.0
            if "primary_frontage_m" in group.columns:
                frontage = float(
                    pd.to_numeric(group["primary_frontage_m"], errors="coerce")
                    .fillna(0.0)
                    .iloc[0]
                )
            columns = {index: _governing_column_of(row) for index, row in group.iterrows()}
            for name in missing_governs:
                family = name.removeprefix("governs_")
                permits = {
                    "residential": lambda c: c.permits_residential,
                    "commercial": lambda c: c.permits_commercial,
                    "industrial": lambda c: c.permits_industrial,
                }[family]
                chosen = select_governing_column(
                    list(columns.values()), frontage, permits=permits
                )
                if chosen is None:
                    continue
                for index, candidate in columns.items():
                    # Identity, not equality - two columns of one grid can
                    # state identical norms, and matching on value would mark
                    # both. The same rule `envelope_assets._governing` states.
                    if candidate is chosen:
                        frame.loc[index, name] = True
                        break
    return frame


def _governing_column_of(row: pd.Series) -> ZoneColumn:
    """The two fields `select_governing_column` reads, as a `ZoneColumn`.

    The private counterpart of `envelope_assets._as_zone_column`, here so an
    older parquet can be read without that asset re-running. `floors_max`
    falls back to 0 on a row that never parsed - such a row is not
    `solver_ready` and is filtered before this is called, but a hand-built
    test frame should not have to state a ceiling to ask about governance.
    """
    floors_max = _int_or_none(row.get("floors_max")) or 0
    return ZoneColumn(
        usages=tuple(str(usage) for usage in _json_list(row.get("usages"))),
        floors_max=floors_max,
        min_lot_width_m=_float_or_none(row.get("min_lot_width_m")),
        zone=_text_or_none(row.get("feature_id")),
    )


def program_row(
    program: DevelopmentProgram,
    *,
    heights: StoreyHeights = DEFAULT_STOREY_HEIGHTS,
) -> dict:
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

    `floor_stack` is the storey counts beside it re-cut by level: one entry
    per run of identical storeys, bottom upwards, with the stalls and the
    dwelling mix on the runs that hold them. `program.floor_stack` is where
    the shape is, and where the order it stacks the uses in is written down
    as the reporting convention it is - the solver counts storeys by type
    and never places one. It travels as json for the same reason `units`
    does, and `heights` is on this signature only so it can price a run: no
    other column here reads a storey height.
    """
    return {
        "status": program.status,
        "solved": program.solved,
        "solve_error": None,
        "npv_cad": program.npv_cad,
        "present_value_cad": program.present_value_cad,
        "annual_stabilised_noi_cad": program.annual_stabilised_noi_cad,
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
        "floor_stack": json.dumps(
            floor_stack(program, heights=heights), ensure_ascii=False
        ),
        "construction_cost_cad": program.construction_cost_cad,
        "commercial_cost_cad": program.commercial_cost_cad,
        "industrial_cost_cad": program.industrial_cost_cad,
        "parking_cost_cad": program.parking_cost_cad,
        "total_capital_cost_cad": program.total_capital_cost_cad,
        "binding": json.dumps(list(program.binding), ensure_ascii=False),
        "unpriced_types": json.dumps(list(program.unpriced_types), ensure_ascii=False),
    }


def _program_row(
    row: Mapping,
    economics: UnitEconomics,
    assumptions: ProgramAssumptions,
    envelope: ZoneEnvelope | None = None,
) -> dict:
    """One candidate's answer, or the reason it has none.

    ``envelope`` is the zone's columns solved together; without one the row's
    own column is solved alone, which is what a caller holding a single
    candidate row still wants.
    """
    try:
        program = solve_program(
            envelope if envelope is not None else zone_column_of(row),
            lot_of(row),
            economics,
            parking=assumptions.parking,
            construction=assumptions.construction,
            non_residential=assumptions.non_residential,
            heights=assumptions.heights,
            investment=assumptions.investment,
            max_seconds=assumptions.max_seconds,
        )
    except (ProgramError, ValueError, KeyError) as exc:
        # `ValueError` and `KeyError` beside `ProgramError` on purpose: a stale
        # parquet can hand this a `levels` value the enum no longer has, or a
        # row with no `lot_area_m2` at all, and neither is worth a borough.
        return {**_ERROR_PROGRAM_ROW, "solve_error": str(exc)}
    return program_row(program, heights=assumptions.heights)


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
    "floor_stack": "[]",
    "binding": "[]",
    "unpriced_types": "[]",
}


# --------------------------------------------------------------------------
# choosing
# --------------------------------------------------------------------------


def select_highest_best_use(
    programs: pd.DataFrame,
    envelopes: pd.DataFrame,
    *,
    assessments: pd.DataFrame | None = None,
    road_lots: pd.DataFrame | None = None,
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

    ``assessments`` is `lot_assessment_comparables`, and the one thing read out
    of it is `dominant_use_code`: a lot the roll files under a CUBF road code
    keeps its row, loses its program and reports `road_parcel`. Optional
    because it is the only input here that is not the zoning - a caller with no
    roll, and every test that is not about this, passes none and gets the
    behaviour it had - and dropped rather than never solved because the solve
    is `lot_development_programs`' lineage and the roll is not in it.

    ``road_lots`` is `frontage_assets`' `road_lots.parquet`, and is the same
    gate read off the geometry instead of the roll - the parcels a geobase
    double side runs inside, which is what Montreal's own street lots are and
    what the roll never records. The two sets are unioned: a parcel either of
    them calls a street gets `road_parcel`. Optional on the same terms, and
    for a second reason - `lot_frontage` has no schedule and reads a relation
    hbu_infra has to create, so a partition without it must answer as it did
    before rather than fail.

    The roll is then consulted a *second* time, in the other direction: where
    the geometry was a close call and the roll says the parcel is primarily
    something else, the parcel is not a road. That is the whole of the roll's
    veto and it is deliberately narrow - see `cadastral_road_lots`, which is
    also where the arithmetic on why it cannot be widened into a whitelist is.
    """
    envelopes = ensure_use_flags(envelopes)
    lots = _lot_index(envelopes)
    if lots.empty:
        return pd.DataFrame(columns=list(HBU_COLUMNS))

    # Both road predicates speak lot numbers and everything from here on speaks
    # `lot_uid`, so the translation happens once, here, off the lot index that
    # already holds both. Unioned before the translation: they are two ways of
    # learning the same fact and they reach different parcels - the roll knows
    # a right of way it assessed, the cadastre knows every street Montreal
    # never put on the roll.
    road_numbers = (
        road_parcel_lots(assessments) if assessments is not None else frozenset()
    ) | cadastral_road_lots(road_lots, assessments)
    road_uids = frozenset(
        lots.index[lots["lot_number"].isin(road_numbers)]
        if road_numbers and "lot_number" in lots.columns
        else ()
    )
    chosen = _chosen(programs)
    # Before the join, so a road parcel's row is the same shape as any other
    # lot without a program: nulls across the envelope and the program, and
    # `hbu_status` carrying the reason.
    chosen = chosen[~chosen.index.isin(road_uids)]
    frame = lots.join(chosen, how="left")
    frame["hbu_status"] = _hbu_status(
        frame,
        programs,
        equipment_lots=equipment_zone_lots(envelopes),
        road_lots=road_uids,
    )
    frame["hbu_dominant_use"] = _dominant_use(frame)
    return frame.reset_index()[list(HBU_COLUMNS)]


def _governs_any(programs: pd.DataFrame) -> pd.Series:
    """Whether each candidate row governs its lot for *some* family.

    Any of the three flags: a row that is the grid's own pick for the
    housing, the commerce or the industry is one the developer may build to.
    Falls back to the residential flag alone on a frame written before the
    other two existed, which is exactly what selection did then.
    """
    governs = pd.Series(False, index=programs.index)
    found = False
    for name in GOVERNS_COLUMNS:
        if name in programs.columns:
            governs |= programs[name].fillna(False).astype(bool)
            found = True
    if not found and "governs_residential" in programs.columns:
        governs = programs["governs_residential"].fillna(False).astype(bool)
    return governs


def _chosen(programs: pd.DataFrame) -> pd.DataFrame:
    """The winning candidate of each lot, indexed by `lot_uid`.

    One row per (lot, zone) reaches this now, each already the best building
    its zone permits across all three families - the mix is `solve_program`'s
    to decide, not this function's. What is left is a lot that two zones
    overlap, and coverage settles it: two zones on one lot are a mapping
    disagreement rather than a menu, so `pct_of_lot` decides and the profit
    only breaks a tie between zones covering the lot equally.
    """
    if programs.empty:
        return pd.DataFrame(columns=list(_CHOSEN_COLUMNS)).rename_axis("lot_uid")
    solved = programs[
        _governs_any(programs) & programs["solved"].fillna(False).astype(bool)
    ]
    value = "npv_cad" if "npv_cad" in solved.columns else "monthly_net_operating_income_cad"
    ranked = solved.sort_values(
        ["lot_uid", "pct_of_lot", value, "column_index"],
        # Coverage first and descending: the zone that actually covers the
        # lot decides, and the profit only ranks the envelopes within it.
        ascending=[True, False, False, True],
        kind="stable",
    ).drop_duplicates("lot_uid")
    wanted = [name for name in _CHOSEN_COLUMNS if name in ranked.columns]
    return ranked.set_index("lot_uid")[wanted].reindex(
        columns=list(_CHOSEN_COLUMNS)
    )


#: Share of the proposed usage floor one class must hold for the program to
#: be called by its name rather than `mixed`. A stated reporting threshold,
#: not an economics input: nothing in the solve reads it.
DOMINANT_USE_SHARE = 0.7


def _dominant_use(frame: pd.DataFrame) -> pd.Series:
    """What kind of building each chosen program is, in one word.

    ``residential`` / ``commercial`` / ``industrial`` when one class holds at
    least `DOMINANT_USE_SHARE` of the proposed usage floor, ``mixed`` when
    none does, ``none`` for a solved program with no floor at all - nothing
    pencils - and null wherever there is no program to describe. Parking is
    not usage floor and is not in the denominator.
    """
    areas = {
        "residential": _numeric(frame, "residential_area_m2"),
        "commercial": _numeric(frame, "commercial_area_m2"),
        "industrial": _numeric(frame, "industrial_area_m2"),
    }
    total = sum(series.fillna(0.0) for series in areas.values())
    result = pd.Series(None, index=frame.index, dtype="object")
    solved = frame["solved"].fillna(False).astype(bool) if "solved" in frame.columns else pd.Series(False, index=frame.index)
    result[solved] = "none"
    built = solved & (total > 0)
    for family, series in areas.items():
        share = series.fillna(0.0) / total.where(total > 0)
        result[built & (share >= DOMINANT_USE_SHARE)] = family
    result[built & result.isin([None, "none"])] = "mixed"
    return result


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
        candidates[_governs_any(candidates)], per_lot.index
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


def _permits_equipment(envelopes: pd.DataFrame) -> pd.Series:
    """Whether each envelope row's column heads an *Équipements* usage.

    `envelope_assets` writes the category out as `usage_equipements`, and that
    is read where it is there. A parquet written before it existed, or a
    hand-built frame, falls back to the `usages` list and the same anchored
    matcher the solver's own families use - the posture `ensure_use_flags`
    takes, and for the same reason.
    """
    if "usage_equipements" in envelopes.columns:
        codes = envelopes["usage_equipements"]
        return codes.notna() & codes.astype("string").str.strip().ne("")
    if "usages" not in envelopes.columns:
        return pd.Series(False, index=envelopes.index)
    return pd.Series(
        [
            any(is_equipment_usage(str(usage)) for usage in _json_list(value))
            for value in envelopes["usages"]
        ],
        index=envelopes.index,
    )


def equipment_zone_lots(envelopes: pd.DataFrame) -> frozenset:
    """The lots whose governing zone authorises *Équipements collectifs*.

    A park, a school, a hospital, a cemetery, a fire station. Read off the
    governing zone alone - `governing_zone`'s - so a lot properly zoned for
    housing that clips the corner of the school's zone is not one of these.

    Membership here is not on its own a reason to have no program: a grid that
    prints an ``E`` column beside an ``H`` one authorises both, and that lot
    solves like any other. It is what tells `_hbu_status` *why* a lot with no
    candidate has none - a use this module deliberately does not price, rather
    than a grid whose usage row it failed to read.
    """
    if envelopes.empty or "lot_uid" not in envelopes.columns:
        return frozenset()
    zones = governing_zone(envelopes)
    governing = envelopes
    if not zones.empty:
        governing = envelopes[
            envelopes["feature_id"].eq(envelopes["lot_uid"].map(zones))
        ]
    if governing.empty:
        return frozenset()
    permits = _permits_equipment(governing)
    return frozenset(governing.loc[permits, "lot_uid"])


def cadastral_road_lots(
    road_lots: pd.DataFrame | None,
    assessments: pd.DataFrame | None = None,
) -> frozenset:
    """The **lot numbers** the cadastre and the street network agree are road.

    ``road_lots`` is `frontage_assets`' `road_lots.parquet` for one partition:
    the parcels a `silver.neighborhood_streets` side runs at least
    `postgis.DEFAULT_ROAD_LOT_MIN_STREET_M` inside. In Quebec's renewed
    cadastre the street is a lot like any other - avenue Querbes is 2 249 179
    and 2 249 339, some 9 m wide and a block long - and a geobase double side
    is drawn along the roadway, so it runs *within* the parcel that is the
    roadway and enters no other.

    **This is the predicate `road_parcel_lots` cannot be.** That one asks the
    assessment roll, which is a record of tenure: Montreal does not enter its
    own roadways on it, so the roll names 48 of this borough's streets where
    the geometry names some fourteen hundred. The two are unioned rather than
    chosen between - each reaches parcels the other misses, and a parcel either
    of them calls a street is not a development site.

    Keyed on the lot *number*, like `road_parcel_lots` and for its reason:
    `lot_uid` is a bigserial that means nothing outside the partition that
    minted it.

    **``assessments`` overturns the marginal calls, and only those.** A parcel
    caught by a metre or two of street line is the one case this geometry
    cannot read - a short stub of roadway and a geobase side clipping the
    corner of an ordinary lot look identical at that end of the range - and
    `lot_frontage` marks exactly those rows `near_cutoff`. Where the roll says
    such a parcel is primarily something else, that is better evidence than two
    metres of line, and it is dropped from the road set. On the 2026
    Villeray-Saint-Michel-Parc-Extension partition this rescues 14 parcels, ten
    of which the solver had a building for, at a median 738 m2 - three
    *Logement*, three *Espace de terrain non aménagé*, an office building, a
    local shopping centre.

    **It is a rescue and not a gate, and the difference is the whole of the
    argument.** Read as a gate - keep only the parcels the roll files as
    something other than a road - the same column drops every lot the roll
    never reached, which is 2,509 of that borough's 24,952: 1,245 of them are
    not roads at all and 1,053 had solved programs, 3,074 dwellings over 40 ha
    of ordinary house lots and unassessed vacant land. It would also still miss
    93 real street parcels, because the roll files them as parking, railway,
    vacant land, ten as *Logement* and six as *Abribus*. Absence of a code is
    not evidence, and a non-road code does not outweigh 300 m of street line
    running down the inside of a parcel. Restricting the roll's vote to the
    ambiguous band is what keeps it worth having.

    Old parquet has no `near_cutoff` column, and is read as no row being
    marginal - so a partition written before this existed keeps the answer it
    had, which is the safe direction.
    """
    if road_lots is None or road_lots.empty:
        return frozenset()
    key = next(
        (name for name in ("lot_number", "NO_LOT") if name in road_lots.columns),
        None,
    )
    if key is None:
        return frozenset()
    numbers = frozenset(road_lots[key].dropna())
    if assessments is None or ROAD_LOT_FLAG_COLUMN not in road_lots.columns:
        return numbers
    marginal = road_lots.loc[
        road_lots[ROAD_LOT_FLAG_COLUMN].fillna(False).astype(bool), key
    ]
    return numbers - (frozenset(marginal.dropna()) & _non_road_use_lots(assessments))


def _non_road_use_lots(assessments: pd.DataFrame) -> frozenset:
    """Lot numbers the roll files under a use code that is *not* a road.

    The complement of `road_parcel_lots` over the parcels the roll reached, and
    deliberately not over the ones it did not: a lot with no `dominant_use_code`
    is absent here rather than counted as non-road. That asymmetry is the point
    - see `cadastral_road_lots` on why the same column read as a whitelist
    deletes an eighth of a borough.
    """
    if assessments is None or assessments.empty:
        return frozenset()
    if "dominant_use_code" not in assessments.columns:
        return frozenset()
    key = next(
        (name for name in ("NO_LOT", "lot_number") if name in assessments.columns),
        None,
    )
    if key is None:
        return frozenset()
    codes = assessments["dominant_use_code"]
    stated = codes.notna() & codes.astype("string").str.strip().ne("")
    not_road = ~codes.map(is_road_use_code).fillna(False)
    return frozenset(assessments.loc[stated & not_road, key].dropna())


def road_parcel_lots(assessments: pd.DataFrame) -> frozenset:
    """The **lot numbers** the assessment roll files under a CUBF road code.

    ``assessments`` is `lot_assessment_comparables` for one partition, whose
    `dominant_use_code` is the `rl0105a` of the most valuable assessment unit
    standing on the lot. On a street, a lane or a right of way that is the only
    unit there is, carried at the nominal hundred dollars the roll gives the
    public way - so "most valuable" and "the one" are the same row.

    Keyed on the lot *number* and not on `lot_uid`, which is `_existing_side`'s
    key and for its reason: that table is one row per Infolot lot number, and
    `lot_uid` is a bigserial that means nothing outside the partition that
    minted it. `NO_LOT` is the spelling the comparables carry and `lot_number`
    the one everything else does; both are accepted, as there.

    **What this does not reach.** A parcel the roll never assessed has no code
    to read and is not here: on the 2026 Villeray-Saint-Michel-Parc-Extension
    partition 3,090 of 24,952 lots carry no assessment unit at all, and most of
    the ruelles are among them - of the 337 parcels in that borough shaped like
    a lane, the roll reaches 63 and calls 12 of them roads. This says what the
    roll says, which is a fact about tenure and not a guess about shape. The
    parcels it never reached are `cadastral_road_lots`' to answer for, off the
    geometry, and the two are unioned - see `select_highest_best_use`.
    """
    if assessments.empty or "dominant_use_code" not in assessments.columns:
        return frozenset()
    key = next(
        (name for name in ("NO_LOT", "lot_number") if name in assessments.columns),
        None,
    )
    if key is None:
        return frozenset()
    roads = assessments["dominant_use_code"].map(is_road_use_code)
    return frozenset(assessments.loc[roads.fillna(False), key].dropna())


def _hbu_status(
    frame: pd.DataFrame,
    programs: pd.DataFrame,
    *,
    equipment_lots: frozenset = frozenset(),
    road_lots: frozenset = frozenset(),
) -> pd.Series:
    """Why each lot has the row it has - one of `HBU_STATUSES`.

    Written from the answer outwards, so what is reported is the *furthest* a
    lot got: a lot with a program is `solved` whatever else is true of it, and a
    lot without one is described by whether it had no candidates at all,
    candidates but none governing, a governing candidate the solver refused, or
    one it could not build a model from.

    The two exclusions read in opposite directions and that is deliberate.
    `equipment_zone` *refines* the answer, splitting `no_candidate_column` in
    two, because it explains an absence the envelopes had already produced.
    `road_parcel` *overrides* it, applied last and regardless of what the
    envelopes said, because it is a fact about the parcel rather than about the
    grid: the zone polygon over a roadway describes the block it serves.

    ``road_lots`` here is already `lot_uid`s and already the union of the two
    road predicates - `select_highest_best_use` does both translations, so this
    function never learns which publisher called a given parcel a street.
    """
    status = pd.Series("solved", index=frame.index, dtype="object")
    lots = frame.index.to_series()
    unsolved = frame["status"].isna()
    status[unsolved & (frame["num_candidates"] == 0)] = "no_candidate_column"
    status[unsolved & (frame["num_candidates"] > 0)] = "no_governing_column"
    status[
        unsolved & (frame["num_candidates"] == 0) & lots.isin(equipment_lots)
    ] = "equipment_zone"
    if not programs.empty:
        governing = programs[_governs_any(programs)]
        infeasible = set(
            governing.loc[
                ~governing["solved"].fillna(False).astype(bool)
                & (governing["status"] != "ERROR"),
                "lot_uid",
            ]
        )
        status[unsolved & lots.isin(infeasible)] = "infeasible"
        # Before `road_parcel` and after the rest, so a lot with one column that
        # raised and another that was merely infeasible is reported as the
        # harder failure - the one that has a message to read.
        errored = set(governing.loc[governing["status"] == "ERROR", "lot_uid"])
        status[unsolved & lots.isin(errored)] = "solver_error"
    status[lots.isin(road_lots)] = "road_parcel"
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
    # What that code says, in the manual's words. The one column here a person
    # reads rather than computes with: "4611" and "Garage de stationnement
    # pour automobiles" are the same fact, and only one of them tells a reader
    # scanning a redevelopment shortlist what is standing on the parcel.
    "dominant_use_description",
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
    investment: InvestmentAssumptions | None = None,
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

    # The verdict this module used to stop short of. The solve now carries a
    # discount rate, so both futures a lot's owner can choose between are
    # priced on it: `hbu_npv_cad` is redeveloping - the discounted value of
    # the new building less what it costs - and `existing_present_value_cad`
    # is keeping the standing one, its stabilised NOI through the same
    # `InvestmentAssumptions.annual_pv_factor`. The gain is the difference,
    # with a missing existing side read as nothing standing - the
    # `is_underbuilt` rule, because a vacant parcel is exactly the case the
    # column exists to rank. What neither side prices is the land, and that
    # is the point: the owner holds it in both futures, so it cancels.
    investment = investment or DEFAULT_INVESTMENT
    frame["hbu_npv_cad"] = _numeric(frame, "npv_cad")
    frame["hbu_present_value_cad"] = _numeric(frame, "present_value_cad")
    frame["existing_present_value_cad"] = (
        frame["existing_annual_stabilised_noi_cad"] * investment.annual_pv_factor
    )
    frame["redevelopment_npv_gain_cad"] = frame["hbu_npv_cad"] - frame[
        "existing_present_value_cad"
    ].fillna(0.0)
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
    frame["existing_dominant_use_description"] = _column_or_null(
        joined, "dominant_use_description"
    )
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


def investment_assumptions_of(hbu: pd.DataFrame) -> InvestmentAssumptions:
    """The `InvestmentAssumptions` the chosen programs were solved with.

    Read off the `program_assumptions` object `lot_development_programs`
    writes onto every row and `lot_highest_best_use` carries, for the same
    reason `operating_expense_ratio_of` reads its ratio off the comparables:
    the PV the gap puts on the *standing* building must be the one stance the
    solve took, not a second config that drifts apart. Falls back to the
    module defaults where the column is absent or predates the discounting -
    a partition that old carries no `npv_cad` either, so the columns built on
    this stay null rather than wrong.
    """
    default = InvestmentAssumptions()
    if hbu.empty or "program_assumptions" not in hbu.columns:
        return default
    values = hbu["program_assumptions"].dropna()
    if not len(values):
        return default
    try:
        payload = json.loads(values.iloc[0])
    except (TypeError, ValueError):
        return default
    if not isinstance(payload, dict) or "discount_rate_pct" not in payload:
        return default
    try:
        return InvestmentAssumptions(
            discount_rate_pct=float(payload["discount_rate_pct"]),
            hold_years=int(payload.get("hold_years", default.hold_years)),
            terminal_cap_rate_pct=(
                None
                if payload.get("terminal_cap_rate_pct") is None
                else float(payload["terminal_cap_rate_pct"])
            ),
            operating_expense_ratio=float(
                payload.get("operating_expense_ratio", default.operating_expense_ratio)
            ),
            new_build_rent_premium_pct=float(
                payload.get(
                    "new_build_rent_premium_pct", default.new_build_rent_premium_pct
                )
            ),
        )
    except (ProgramError, TypeError, ValueError):
        return default


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
