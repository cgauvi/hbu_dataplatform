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

`select_highest_best_use` collapses the candidates onto one row per **(lot,
zone)** - one row per *piece* of ground - and the shape of the choice matters:
what may be chosen is the *use*, never the rules, and never the site.

A lot can carry several envelope rows for three reasons, and only one of them
is anybody's choice. Within one zone and one usage family, a grid authorises
dwellings in more than one column and distinguishes them by *Largeur du
terrain min*: the column a parcel of this width is written for is
`select_governing_column`'s to pick, `envelope_assets` marks it
`governs_residential` - and `governs_commercial`, `governs_industrial` for
the families beside it - and taking a higher-earning column of the *same*
family instead would be reporting a program under rules the parcel may not
build to.

**Across zones there is no choice either, and it took a wrong answer to see
why.** A lot covered by two zones used to be treated as one site with two
competing readings, and `pct_of_lot` picked the winner: the best-covered zone
answered for the whole parcel and the others were dropped before anything was
solved. That is right for the case it was written for - a parcel on a boundary
picking up a sliver of its neighbour's zoning because two publishers drew two
lines - and wrong for the case it could not see. A zoning boundary does not
have to follow a lot line, and on a large parcel it usually does not: lot
1 740 794 is 27 044 m2 with 24 596 in H04-072 (H.7, eight storeys) and 2 440 in
C04-083 (C.4 and H, six), and the old rule priced eight storeys over all of it
including the ground H04-072 does not reach, while never pricing the C.4 at
all. Over Villeray-Saint-Michel-Parc-Extension that is 121 ha under the wrong
grid.

So the zones are not a menu and they are not a contest - they are **two
sites**, and both are solved. `lot_zone_pieces` is what makes that expressible:
each piece carries its own area, its own street and its own share of what
stands, so a piece is a `Lot` in `program`'s sense and the split parcel is two
calls to `solve_program` rather than one. The slivers do not come back with it;
they never reach this module, because that table applies the same two cutoffs
before writing a piece at all. `is_primary_zone` marks the largest piece, which
is the row a reader wanting one answer per parcel takes and the answer this
module used to give.

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
families. Nothing is maximised here at all any more: every piece keeps its own
answer, and the only ranking left is the one a reader does when they sort a
shortlist.

A piece with candidates but no governing column keeps its row and says so in
`hbu_status`: `no_governing_column` is almost always ground with no measured
frontage under a grid that states a width minimum, which reads as 0 m and
qualifies for nothing. On a split lot that is now a real answer rather than a
gap - an interior piece behind a street-facing one genuinely fronts nothing -
and it is worth being able to count either way.

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

**A piece whose zone authorises only Équipements collectifs.** A park, a
school, a hospital, a cemetery. `program` has never priced the ``E`` family -
a school is not something a proforma rents by the square foot - so those
columns were never candidates, and what made these parcels development sites
anyway was the *other* zone: a lot on a zone boundary picks up a sliver of its
neighbour's, and `_chosen` resolved between zones only among the rows that had
produced a program, so a zone that produced none dropped silently out of the
contest and a 1.7% sliver of the block next door answered for the whole parcel.

Two things close that, and it is worth keeping them apart because only one of
them survives the move to pieces. The sliver itself is gone upstream:
`lot_zone_pieces` writes no piece for a zone under a per cent and a square
metre of a lot, so the 1.7% corner of Parc Jarry never reaches a solver. What
is left is the gate here, and it is now applied **per piece** rather than per
lot - `equipment_zone_pieces` - which is the only reading that works once both
zones are answered: a parcel half park and half housing has one piece nobody
may build on and one piece somebody may, and calling the whole lot an
equipment parcel would lose the second. On the 2026
Villeray-Saint-Michel-Parc-Extension partition the two together remove 343
answers that were a neighbour's grid, 206 of them on Équipements parcels, Parc
Jarry among them.

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

import numpy as np
import pandas as pd

from urban_rag.comparables import (
    INCOME_CLASSES,
    IncomeAssumptions,
    is_road_use_code,
)
from dataclasses import replace as _replace

from urban_rag.program import (
    BASEMENT_LEVELS_ALLOWED,
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
    RetainedBuilding,
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
    # minimum the parcel cannot meet. Stalls it has nowhere to put no longer
    # land here on their own: a piece the parking alone stopped is solved
    # again without it, reported `solved`, and marked `parking_waived` on its
    # program row with the stalls it owes in `waived_stalls`.
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
    "annual_residential_gross_revenue_cad",
    "annual_commercial_gross_revenue_cad",
    "annual_industrial_gross_revenue_cad",
    "num_dwellings",
    "units",
    "floors",
    "height_m",
    "footprint_m2",
    "gross_floor_area_m2",
    "density_floor_area_m2",
    "residential_area_m2",
    "unit_area_m2",
    "commercial_area_m2",
    "industrial_area_m2",
    "basement_area_m2",
    "basement_residential_area_m2",
    "basement_commercial_area_m2",
    "basement_industrial_area_m2",
    "underground_area_m2",
    "underground_plate_m2",
    "garage_area_m2",
    "surface_area_m2",
    "residential_floors",
    "commercial_floors",
    "industrial_floors",
    "basement_levels",
    "basement_residential_levels",
    "basement_commercial_levels",
    "basement_industrial_levels",
    "basement_dwellings",
    "underground_levels",
    "underground_stalls",
    "surface_stalls",
    "garage_stalls",
    "total_stalls",
    # Whether the stalls above are zero because the program was solved with
    # its parking waived - the model was infeasible with the stalls and solved
    # without them - and how many it owes at the stated ratios if so. See
    # `program.solve_program` on `waive_parking_if_infeasible`.
    "parking_waived",
    "waived_stalls",
    # What the parking earns and what it buys: the stalls somebody rents and
    # their rent a year (inside `annual_gross_revenue_cad`), the stalls per
    # dwelling provided, the lease-up months that coverage saves and what the
    # saving is worth in present value (inside `present_value_cad`). See
    # `program.solve_program` on what a stall is worth.
    "rented_stalls",
    "annual_parking_gross_revenue_cad",
    "parking_coverage",
    "lease_up_months_saved",
    "absorption_value_cad",
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
    "annual_residential_gross_revenue_cad",
    "annual_commercial_gross_revenue_cad",
    "annual_industrial_gross_revenue_cad",
    "height_m",
    "footprint_m2",
    "gross_floor_area_m2",
    "density_floor_area_m2",
    "residential_area_m2",
    "unit_area_m2",
    "commercial_area_m2",
    "industrial_area_m2",
    "basement_area_m2",
    "basement_residential_area_m2",
    "basement_commercial_area_m2",
    "basement_industrial_area_m2",
    "underground_area_m2",
    "underground_plate_m2",
    "garage_area_m2",
    "surface_area_m2",
    "construction_cost_cad",
    "commercial_cost_cad",
    "industrial_cost_cad",
    "parking_cost_cad",
    "total_capital_cost_cad",
    "annual_parking_gross_revenue_cad",
    "parking_coverage",
    "lease_up_months_saved",
    "absorption_value_cad",
)

#: The counts of a program, same purpose as `_PROGRAM_FLOATS`.
_PROGRAM_COUNTS: tuple[str, ...] = (
    "num_dwellings",
    "floors",
    "residential_floors",
    "commercial_floors",
    "industrial_floors",
    "basement_levels",
    "basement_residential_levels",
    "basement_commercial_levels",
    "basement_industrial_levels",
    "basement_dwellings",
    "underground_levels",
    "underground_stalls",
    "surface_stalls",
    "garage_stalls",
    "total_stalls",
    "waived_stalls",
    "rented_stalls",
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
    # The parcel, and the ground this zone governs. Both, because a reader
    # holding one program row has to be able to tell "this is a 27 044 m2 lot"
    # from "this is the 2 440 m2 of it C04-083 covers" - and it is the second
    # that was solved.
    "lot_area_m2",
    "piece_area_m2",
    "num_lot_zones",
    "is_primary_zone",
    "primary_frontage_m",
    "primary_street_name",
    "buildable_area_m2",
    "parkable_area_m2",
    "placeable_area_m2",
)

#: The columns a chosen program brings across from its candidate row. The
#: piece's own facts are not among them: those come from `_piece_index`, which
#: has a row for every piece including the ones no candidate was chosen for -
#: and `feature_id` and `pct_of_lot` are the piece's, so they moved there when
#: the grain did.
_CHOSEN_COLUMNS: tuple[str, ...] = (
    "source_table",
    "column_index",
    "grid_zone",
    "usages",
    "permits_residential",
    "permits_commercial",
    "permits_industrial",
    "governs_residential",
    "governs_commercial",
    "governs_industrial",
    "buildable_area_m2",
    "placeable_area_m2",
    *PROGRAM_COLUMNS,
)

#: What a `lot_highest_best_use` row says about the ground before the envelope
#: chosen for it and the program that fills it.
#:
#: The key leads and it is a *pair*: one row per (lot, zone), because a zoning
#: boundary crossing a large parcel makes two sites of it. Everything after
#: `scrape_date` down to `footprint_share_basis` is the piece as
#: `lot_zone_pieces` measured it, carried onto the answer so a reader holding
#: one row knows what ground was solved, how much of its parcel that is, which
#: street it faces, and what share of the standing building sits on it - the
#: last being what `use_gap` divides the roll by.
_HBU_PIECE_COLUMNS: tuple[str, ...] = (
    "lot_uid",
    "feature_id",
    "lot_number",
    "neighborhood",
    "scrape_date",
    # The parcel and the piece of it, always both: a reader has to be able to
    # tell a whole lot from a tenth of one.
    "lot_area_m2",
    "piece_area_m2",
    "pct_of_lot",
    "num_lot_zones",
    "zone_rank",
    "is_primary_zone",
    "primary_frontage_m",
    "primary_street_name",
    "secondary_frontage_m",
    "secondary_street_name",
    "num_frontages",
    "existing_footprint_m2",
    "area_share",
    "footprint_share",
    "footprint_share_basis",
    "parkable_area_m2",
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
    *_HBU_PIECE_COLUMNS,
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
    #: Below-grade levels of *usage* a program may take where the grid's level
    #: rows authorise one. A modelling bound rather than a norm, like
    #: `ParkingRules.max_underground_levels` beside it, and `NO_BASEMENT` is
    #: how a run asks what the borough is worth built entirely above grade.
    basement_levels_allowed: int = BASEMENT_LEVELS_ALLOWED
    #: Seconds CP-SAT may spend on one envelope. A borough is tens of thousands
    #: of models of fifteen variables each and nearly all of them are solved in
    #: milliseconds; this bounds the handful that are not. A model that runs out
    #: comes back `FEASIBLE` or `UNKNOWN` rather than `OPTIMAL`, and the asset
    #: counts both, so a limit set too low is visible rather than silent.
    max_seconds: float = 10.0
    #: Whether a candidate that holds nothing with its stalls - infeasible,
    #: or solved and empty - is solved again without the obligation. On by
    #: default: for a borough, "no program at all" is a worse answer about a
    #: parcel than "this program, short this many stalls", and the row says
    #: which it is through `parking_waived` and `waived_stalls`.
    #: `program.solve_program` documents when the second solve happens and
    #: why it never masks a printed minimum or a rent that does not pay.
    waive_parking_if_empty: bool = True

    def as_metadata(self) -> dict[str, object]:
        """The object every row carries, so a program can be read back."""
        return {
            "stalls_per_dwelling": self.parking.stalls_per_dwelling,
            "stalls_per_1000_sqft": self.parking.stalls_per_1000_sqft,
            "underground_stall_area_sqft": self.parking.underground_area_sqft,
            "surface_stall_area_sqft": self.parking.surface_area_sqft,
            "garage_stall_area_sqft": self.parking.garage_area_sqft,
            "underground_stall_cost_cad": self.parking.underground_cost_cad,
            "surface_stall_cost_cad": self.parking.surface_cost_cad,
            "garage_stall_cost_cad": self.parking.garage_cost_cad,
            "max_underground_levels": self.parking.max_underground_levels,
            "underground_lot_share": self.parking.underground_lot_share,
            "max_surface_stalls": self.parking.max_surface_stalls,
            "max_garage_stalls": self.parking.max_garage_stalls,
            "parking_stall_rent_cad_month": self.parking.monthly_rent_cad,
            "parking_stall_occupancy_pct": self.parking.occupancy_pct,
            "market_stalls_per_dwelling": self.parking.market_stalls_per_dwelling,
            "market_stalls_per_1000_sqft": self.parking.market_stalls_per_1000_sqft,
            "parking_absorption_saving_months": self.parking.absorption_saving_months,
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
            "months_per_year": MONTHS_PER_YEAR,
            "discount_rate_pct": self.investment.discount_rate_pct,
            "hold_years": self.investment.hold_years,
            "terminal_cap_rate_pct": self.investment.terminal_cap_rate_pct,
            "operating_expense_ratio": self.investment.operating_expense_ratio,
            "new_build_rent_premium_pct": (
                self.investment.new_build_rent_premium_pct
            ),
            "below_grade_rent_discount_pct": (
                self.investment.below_grade_rent_discount_pct
            ),
            "construction_months": self.investment.construction_months,
            "lease_up_months": self.investment.lease_up_months,
            "below_grade_cost_premium": self.construction.below_grade_premium,
            "basement_levels_allowed": self.basement_levels_allowed,
            "max_seconds": self.max_seconds,
            "waive_parking_if_empty": self.waive_parking_if_empty,
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


#: The area `lot_of` sizes a building on, in the order it looks for it.
#:
#: `piece_area_m2` is the ground *this zone* governs and is the right answer
#: whenever it is there - see the module docstring on why a split lot is two
#: sites rather than one. `lot_area_m2` is the fallback and it is exactly the
#: old behaviour: a parquet written before `lot_zone_pieces` existed carries
#: only the parcel, and reading it back should give the answer it gave then
#: rather than fail. The two are equal on every unsplit lot, which is most of
#: a borough.
_PARCEL_AREA_COLUMNS: tuple[str, ...] = ("piece_area_m2", "lot_area_m2")


def lot_of(row: Mapping) -> Lot:
    """One row of `lot_zoning_envelopes`, back as the parcel the solver sizes.

    **The parcel is the piece, not the lot.** `piece_area_m2` is the ground the
    row's zone actually governs, and on a lot two zones cut in two it is not
    the lot's area - pricing eight storeys over a commercial strip the H grid
    does not reach is the error this column exists to stop. `lot_area_m2` is
    the documented fallback for a partition written before `lot_zone_pieces`,
    and the two agree on every lot one zone covers whole.

    A missing frontage is 0 m rather than an error, which is the reading
    `envelope_assets.meets_min_lot_width` gives it too: a piece nothing
    measured satisfies no width minimum, and excluding it is the conservative
    answer.
    `buildable_area_m2` is absent on a partition where `lot_buildable_setbacks`
    has not run, and `Lot` reads `None` as "no margin cap known" rather than as
    no buildable area at all.

    `parkable_area_m2` is absent on the same terms and reads the same way -
    "nobody measured the yard's shape", which leaves the surface stalls bounded
    by the area arithmetic alone. It is measured off the *piece*, opened inside
    the ground this zone governs: it used to be the whole parcel's yard, which
    was the same thing while one zone answered for a lot and is a double count
    now that both do - each piece would be handed the other's back garden.
    A measured **0.0** is not absence and must not be collapsed into it: it
    says this ground parks no car, and `_float_or_none` is careful to return it
    rather than None.

    `placeable_area_m2` is absent on the same terms again, and it belongs to a
    zoning column the way `buildable_area_m2` does rather than to the lot: it
    is the largest rectangle that fits inside *that column's* margins, so a
    parcel governed by two columns with different setbacks has two of them.
    Absent, the footprint stays capped on the two area norms alone and the
    yard on ``lot area - footprint``. A measured **0.0** again says something
    rather than nothing - these margins hold no building at all.
    """
    return Lot(
        area_m2=_parcel_area_of(row),
        frontage_m=_float_or_none(row.get("primary_frontage_m")) or 0.0,
        lot_number=_text_or_none(row.get("lot_number")),
        buildable_area_m2=_float_or_none(row.get("buildable_area_m2")),
        parkable_area_m2=_float_or_none(row.get("parkable_area_m2")),
        placeable_area_m2=_float_or_none(row.get("placeable_area_m2")),
    )


def _parcel_area_of(row: Mapping) -> float:
    """The ground one envelope row is solved over, in square metres.

    `_PARCEL_AREA_COLUMNS` in order, first present and non-null wins. Raises
    `KeyError` naming both when neither is there, rather than defaulting to
    zero: a parcel of no area is a `ProgramError` one function later and a
    confusing one, since nothing in the message would say the area column was
    simply missing.
    """
    for name in _PARCEL_AREA_COLUMNS:
        value = _float_or_none(row.get(name))
        if value is not None:
            return value
    raise KeyError(
        f"no parcel area on this row: expected one of "
        f"{', '.join(_PARCEL_AREA_COLUMNS)}"
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


def primary_zone(envelopes: pd.DataFrame) -> pd.Series:
    """The largest piece of each lot, as a zone number keyed by `lot_uid`.

    A *label*, not a filter, and that is the whole of what changed here. This
    function used to be `governing_zone`: it named the one zone that spoke for
    a parcel, and `candidate_envelopes` dropped every row belonging to any
    other zone before a solver saw it. Two zones on one lot were read as two
    publishers' lines disagreeing, and the best-covered one won.

    That reading is right about a sliver and wrong about a boundary that
    genuinely crosses a large parcel, and the two are not distinguishable by
    the rule it used. Both are now handled, and by different things.
    `lot_zone_pieces` drops the sliver - under a per cent *and* under a square
    metre of the lot, before a piece is written at all - so the few square
    centimetres of residential zone at the corner of Parc Jarry never arrive.
    What is left is real ground under a real grid, and it is solved rather than
    ranked: see the module docstring on lot 1 740 794 and the 121 ha of VSMPE
    that used to be priced under a grid that does not govern it.

    So what this returns is only the answer to "if I want one row for this
    parcel, which one" - the same question `is_primary_zone` answers on every
    piece row, computed the same way. Prefer that column where it is there;
    this is what a frame without it falls back on, and what keeps the ranking
    in one place.

    Ties are broken on `feature_id` so a lot split exactly evenly names the
    same zone on every run rather than whichever the join placed first. A frame
    carrying neither `is_primary_zone` nor `pct_of_lot` - a hand-built one in a
    test - gets an empty Series, since there is nothing to rank by.
    """
    if "lot_uid" not in envelopes.columns or "feature_id" not in envelopes.columns:
        return pd.Series(dtype="object")
    if "is_primary_zone" in envelopes.columns:
        # Written by `lot_zone_pieces`, which ranked the pieces against the
        # clip areas rather than against `pct_of_lot` rounded onto an envelope
        # row. Preferred for that reason, and because it is the column the map
        # and the gold tables carry.
        marked = envelopes[envelopes["is_primary_zone"].fillna(False).astype(bool)]
        if not marked.empty:
            return (
                marked.sort_values(["lot_uid", "feature_id"], kind="stable")
                .drop_duplicates("lot_uid", keep="first")
                .set_index("lot_uid")["feature_id"]
            )
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

    A candidate authorises at least one of the three priced families and
    parses into a solver input. The flags are upstream columns read rather than
    re-derived - see the module docstring - with `ensure_use_flags` as the
    documented fallback for a parquet written before the commercial and
    industrial ones existed. A frame missing every permits flag is treated as
    though each row passed, so a hand-built frame in a test need not carry
    columns the test is not about.

    **Every zone of a lot is a candidate now.** This used to narrow to the
    lot's governing zone and it no longer narrows on the zone at all: a piece
    of ground under a grid is a site, and which of a parcel's pieces is worth
    the most is a question for a reader sorting a shortlist rather than one
    this function should answer by deleting rows. What kept the slivers out was
    never really this filter - it is the two cutoffs, and they now live in
    `lot_zone_pieces` where they can be applied once and carried on the row.

    An Équipements piece still stays one, and by the gate it always had rather
    than by this: its columns are not candidates because `program` prices no
    ``E`` family, and `equipment_zone_pieces` is what says so in `hbu_status`.
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
        # The gross split by the family that earns it. Annual only, unlike the
        # total above it: every reader of the split is downstream of the gap,
        # which is annual on both sides, and three more monthly columns would
        # be six columns to say one thing. The three sum to
        # `annual_gross_revenue_cad` less `annual_parking_gross_revenue_cad`
        # beside them - each carries its own basement plate, so this is not
        # the area split and cannot be rebuilt from one.
        "annual_residential_gross_revenue_cad": (
            program.residential_gross_revenue_cad * MONTHS_PER_YEAR
        ),
        "annual_commercial_gross_revenue_cad": (
            program.commercial_gross_revenue_cad * MONTHS_PER_YEAR
        ),
        "annual_industrial_gross_revenue_cad": (
            program.industrial_gross_revenue_cad * MONTHS_PER_YEAR
        ),
        "num_dwellings": program.total_dwellings,
        "units": json.dumps(dict(program.units), ensure_ascii=False),
        "floors": program.floors,
        "height_m": program.height_m,
        "footprint_m2": program.footprint_m2,
        "gross_floor_area_m2": program.gross_floor_area_m2,
        "density_floor_area_m2": program.density_floor_area_m2,
        "residential_area_m2": program.footprint_m2 * program.residential_floors,
        "unit_area_m2": program.unit_area_m2,
        "commercial_area_m2": program.commercial_area_m2,
        "industrial_area_m2": program.industrial_area_m2,
        "basement_area_m2": program.basement_area_m2,
        "basement_residential_area_m2": program.basement_residential_area_m2,
        "basement_commercial_area_m2": program.basement_commercial_area_m2,
        "basement_industrial_area_m2": program.basement_industrial_area_m2,
        "underground_area_m2": program.underground_area_m2,
        "underground_plate_m2": program.underground_plate_m2,
        "garage_area_m2": program.garage_area_m2,
        "surface_area_m2": program.surface_area_m2,
        "residential_floors": program.residential_floors,
        "commercial_floors": program.commercial_floors,
        "industrial_floors": program.industrial_floors,
        "basement_levels": program.basement_levels,
        "basement_residential_levels": program.basement_residential_levels,
        "basement_commercial_levels": program.basement_commercial_levels,
        "basement_industrial_levels": program.basement_industrial_levels,
        "basement_dwellings": program.basement_dwellings,
        "underground_levels": program.underground_levels,
        "underground_stalls": program.underground_stalls,
        "surface_stalls": program.surface_stalls,
        "garage_stalls": program.garage_stalls,
        "total_stalls": program.total_stalls,
        "parking_waived": bool(program.parking_waived),
        "waived_stalls": int(program.waived_stalls),
        "rented_stalls": int(program.rented_stalls),
        "annual_parking_gross_revenue_cad": (
            program.parking_gross_revenue_cad * MONTHS_PER_YEAR
        ),
        "parking_coverage": program.parking_coverage,
        "lease_up_months_saved": program.lease_up_months_saved,
        "absorption_value_cad": program.absorption_value_cad,
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
            basement_levels_allowed=assumptions.basement_levels_allowed,
            max_seconds=assumptions.max_seconds,
            waive_parking_if_empty=assumptions.waive_parking_if_empty,
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
    "parking_waived": False,
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
    """One row per **(lot, zone)**: the program of the piece that zone governs.

    ``programs`` is `solve_envelopes`' output and ``envelopes`` is the frame it
    was built from - both, because a piece whose every envelope authorises
    commerce has no candidate row at all and would otherwise vanish from a table
    that is meant to be an inventory. Every piece the envelopes reach keeps a
    row, and `hbu_status` says which of `HBU_STATUSES` it is.

    **The grain is the piece, and this function no longer chooses between
    pieces.** It used to return one row per lot, picked among the zones by
    coverage; a lot cut in two by a zoning boundary is two sites, so both are
    reported and the ranking a reader wants is theirs to do. `is_primary_zone`
    marks the largest, `num_lot_zones` says how many there are, and
    `lot_number` is what groups them back into a parcel - on the map, in the
    dashboard and in a `GROUP BY`. See the module docstring.

    What is still chosen, and all that is, is *within* a piece: among the rows
    `governs_residential` and its two siblings mark, the highest-earning, ties
    broken by column index. `num_candidates` travels with it, so a piece where
    the choice was real is distinguishable from one where it was not.

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
    pieces = _piece_index(envelopes)
    if pieces.empty:
        return pd.DataFrame(columns=list(HBU_COLUMNS))

    # Both road predicates speak lot numbers and everything from here on speaks
    # (lot_uid, feature_id), so the translation happens once, here, off the
    # piece index that already holds the lot number. Unioned before the
    # translation: they are two ways of learning the same fact and they reach
    # different parcels - the roll knows a right of way it assessed, the
    # cadastre knows every street Montreal never put on the roll.
    #
    # A road parcel is a road parcel whole. Unlike the equipment gate beside
    # it, this one is a fact about the *ground* rather than about a grid, so
    # every piece of a street lot is street - splitting avenue Querbes between
    # two zones does not make half of it a development site.
    road_numbers = (
        road_parcel_lots(assessments) if assessments is not None else frozenset()
    ) | cadastral_road_lots(road_lots, assessments)
    road_keys = frozenset(
        pieces.index[pieces["lot_number"].isin(road_numbers)]
        if road_numbers and "lot_number" in pieces.columns
        else ()
    )
    chosen = _chosen(programs)
    # Before the join, so a road parcel's row is the same shape as any other
    # piece without a program: nulls across the envelope and the program, and
    # `hbu_status` carrying the reason.
    chosen = chosen[~chosen.index.isin(road_keys)]
    frame = pieces.join(chosen, how="left")
    frame["hbu_status"] = _hbu_status(
        frame,
        programs,
        equipment_pieces=equipment_zone_pieces(envelopes),
        road_pieces=road_keys,
    )
    frame["hbu_dominant_use"] = _dominant_use(frame)
    return frame.reset_index()[list(HBU_COLUMNS)]


def _governs_any(programs: pd.DataFrame) -> pd.Series:
    """Whether each candidate row governs its piece for *some* family.

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
    """The winning candidate of each piece, indexed by (`lot_uid`,
    `feature_id`).

    One row per (lot, zone) reaches this, each already the best building its
    zone permits across all three families - the mix is `solve_program`'s to
    decide, not this function's - so ordinarily there is nothing left to
    choose and this is a filter on "solved, and something governs it".

    **It no longer ranks a lot's zones against each other.** It used to sort
    on `pct_of_lot` and keep one row per lot, which is the rule the module
    docstring now argues against: two zones cutting a large parcel are two
    sites and both are reported. The `drop_duplicates` that remains is on the
    piece and is a guard rather than a choice - a duplicate (lot, zone) would
    mean one grid reached a piece twice, and keeping the higher-earning row of
    an identical pair is the same answer either way.
    """
    keys = ["lot_uid", "feature_id"]
    if programs.empty:
        empty = pd.DataFrame(columns=list(_CHOSEN_COLUMNS))
        empty.index = pd.MultiIndex.from_arrays([[], []], names=keys)
        return empty
    solved = programs[
        _governs_any(programs) & programs["solved"].fillna(False).astype(bool)
    ]
    value = "npv_cad" if "npv_cad" in solved.columns else "monthly_net_operating_income_cad"
    ranked = solved.sort_values(
        [*keys, value, "column_index"],
        ascending=[True, True, False, True],
        kind="stable",
    ).drop_duplicates(keys)
    wanted = [name for name in _CHOSEN_COLUMNS if name in ranked.columns]
    return ranked.set_index(keys)[wanted].reindex(columns=list(_CHOSEN_COLUMNS))


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


#: The piece columns `_piece_index` carries straight through from the
#: envelopes, as (name, aggregation). Every one is constant within a (lot,
#: zone) group by construction - `lot_zoning_envelopes` writes them onto every
#: column of a grid - so `first` is not a choice among differing values.
#:
#: `num_zones` is the exception and is computed rather than carried: it counts
#: the zones of the *lot*, which is a fact about the parcel and therefore the
#: same on each of its pieces, and it is what a reader checks before treating
#: one row as the answer for a lot. It duplicates `num_lot_zones` where that
#: column is present, and is what a frame without it falls back on.
_PIECE_PASSTHROUGH: tuple[str, ...] = (
    "lot_number",
    "neighborhood",
    "scrape_date",
    "lot_area_m2",
    "piece_area_m2",
    "pct_of_lot",
    "num_lot_zones",
    "zone_rank",
    "is_primary_zone",
    "primary_frontage_m",
    "primary_street_name",
    "secondary_frontage_m",
    "secondary_street_name",
    "num_frontages",
    "existing_footprint_m2",
    "area_share",
    "footprint_share",
    "footprint_share_basis",
    # Optional the way `buildable_area_m2` is optional upstream: a frame from a
    # partition written before the cadastre was measured for parking simply has
    # no such column, and an index that raised on it would cost the whole
    # borough its inventory over a number only the surface stalls read.
    "parkable_area_m2",
)


def _piece_index(envelopes: pd.DataFrame) -> pd.DataFrame:
    """One row per (lot, zone) the envelopes reach, with what it is and how many.

    The inventory this table is built on, and the grain change in one function:
    it used to group by `lot_uid` alone and return one row per parcel. A lot a
    zoning boundary crosses is two sites, so it is two rows, and everything
    downstream - the gap, the shortlist, the map - inherits the pair.

    `num_candidates` counts *candidates* rather than envelope rows: a piece
    under a grid with one Habitation column and three Commerce ones had one
    choice, not four, and reporting four would make the Habitation column look
    like three failed parses.

    A column `lot_zone_pieces` supplies but this frame lacks - an older parquet
    - comes back null rather than raising, for the reason the parking area
    always has: an inventory that refuses a borough over one missing measure is
    worse than one that reports it as unknown.
    """
    keys = ["lot_uid", "feature_id"]
    if envelopes.empty:
        empty = pd.DataFrame(
            columns=[name for name in _HBU_PIECE_COLUMNS if name not in keys]
        )
        empty.index = pd.MultiIndex.from_arrays([[], []], names=keys)
        return empty
    candidates = candidate_envelopes(envelopes)
    present = [name for name in _PIECE_PASSTHROUGH if name in envelopes.columns]
    per_piece = envelopes.groupby(keys, sort=False).agg(
        **{name: (name, "first") for name in present}
    )
    for name in _PIECE_PASSTHROUGH:
        if name not in per_piece.columns:
            per_piece[name] = pd.NA
    # A fact about the parcel, so it is counted over the lot and broadcast back
    # onto each of its pieces rather than aggregated within one.
    zones_per_lot = envelopes.groupby("lot_uid", sort=False)["feature_id"].nunique()
    per_piece["num_zones"] = (
        per_piece.index.get_level_values("lot_uid").map(zones_per_lot).astype("int64")
    )
    per_piece["num_candidates"] = _count_by_piece(candidates, per_piece.index)
    per_piece["num_governing_candidates"] = _count_by_piece(
        candidates[_governs_any(candidates)], per_piece.index
    )
    return per_piece


def _count_by_piece(frame: pd.DataFrame, index: pd.MultiIndex) -> pd.Series:
    """How many rows of ``frame`` each piece of ``index`` has - 0, not null."""
    if frame.empty:
        return pd.Series(0, index=index, dtype="int64")
    return (
        frame.groupby(["lot_uid", "feature_id"], sort=False)
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


def equipment_zone_pieces(envelopes: pd.DataFrame) -> frozenset:
    """The **(lot, zone) pieces** whose grid authorises *Équipements collectifs*.

    A park, a school, a hospital, a cemetery, a fire station.

    **Per piece, not per lot, and the difference is load-bearing.** This used
    to read the lot's governing zone alone, because a lot had exactly one zone
    that spoke for it and the others were slivers to be ignored. Now a lot has
    as many sites as it has pieces, and a parcel that is half park and half
    housing has one piece nobody may build on and one piece somebody may.
    Answering "is this lot an equipment parcel" would have to lose one of them;
    answering it per piece loses neither.

    Membership is not on its own a reason to have no program: a grid that
    prints an ``E`` column beside an ``H`` one authorises both, and that piece
    solves like any other. It is what tells `_hbu_status` *why* a piece with no
    candidate has none - a use this module deliberately does not price, rather
    than a grid whose usage row it failed to read.
    """
    if envelopes.empty or "lot_uid" not in envelopes.columns:
        return frozenset()
    if "feature_id" not in envelopes.columns:
        return frozenset()
    permits = _permits_equipment(envelopes)
    marked = envelopes.loc[permits, ["lot_uid", "feature_id"]]
    return frozenset(map(tuple, marked.to_numpy()))


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
    equipment_pieces: frozenset = frozenset(),
    road_pieces: frozenset = frozenset(),
) -> pd.Series:
    """Why each piece has the row it has - one of `HBU_STATUSES`.

    Written from the answer outwards, so what is reported is the *furthest* a
    piece got: a piece with a program is `solved` whatever else is true of it,
    and one without is described by whether it had no candidates at all,
    candidates but none governing, a governing candidate the solver refused, or
    one it could not build a model from.

    **Per piece rather than per lot**, which is what lets a split parcel say
    two different things about its two halves - `equipment_zone` on the park
    side and `solved` on the housing side is the ordinary case, and a per-lot
    answer would have to pick one of them.

    The two exclusions read in opposite directions and that is deliberate.
    `equipment_zone` *refines* the answer, splitting `no_candidate_column` in
    two, because it explains an absence the envelopes had already produced.
    `road_parcel` *overrides* it, applied last and regardless of what the
    envelopes said, because it is a fact about the ground rather than about the
    grid: the zone polygon over a roadway describes the block it serves.

    ``road_pieces`` here is already (lot_uid, feature_id) pairs and already the
    union of the two road predicates - `select_highest_best_use` does both
    translations, so this function never learns which publisher called a given
    parcel a street.
    """
    status = pd.Series("solved", index=frame.index, dtype="object")
    pieces = pd.Series(list(frame.index), index=frame.index, dtype="object")
    unsolved = frame["status"].isna()
    status[unsolved & (frame["num_candidates"] == 0)] = "no_candidate_column"
    status[unsolved & (frame["num_candidates"] > 0)] = "no_governing_column"
    status[
        unsolved & (frame["num_candidates"] == 0) & pieces.isin(equipment_pieces)
    ] = "equipment_zone"
    if not programs.empty:
        governing = programs[_governs_any(programs)]
        infeasible = set(
            map(
                tuple,
                governing.loc[
                    ~governing["solved"].fillna(False).astype(bool)
                    & (governing["status"] != "ERROR"),
                    ["lot_uid", "feature_id"],
                ].to_numpy(),
            )
        )
        status[unsolved & pieces.isin(infeasible)] = "infeasible"
        # Before `road_parcel` and after the rest, so a piece with one column
        # that raised and another that was merely infeasible is reported as the
        # harder failure - the one that has a message to read.
        errored = set(
            map(
                tuple,
                governing.loc[
                    governing["status"] == "ERROR", ["lot_uid", "feature_id"]
                ].to_numpy(),
            )
        )
        status[unsolved & pieces.isin(errored)] = "solver_error"
    status[pieces.isin(road_pieces)] = "road_parcel"
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
    # What the enhancement solve stands on: the roll's storey count is the
    # plate's divisor, and the year is what the shortlist's teardown screen
    # reads. Carried here so the gap row says what the building is.
    "num_storeys",
    "year_built",
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
    `lot_assessment_comparables`, joined on the lot number. One row per (lot,
    zone) of ``hbu`` and no more: this is a table about zoning envelopes, and a
    piece the grids do not reach has nothing to compare against however well
    the roll describes it.

    **The roll is per lot and the answer is per piece, so the roll is split.**
    A parcel a zoning boundary crosses is two sites with two programs, and the
    assessment behind it is one row describing one building. Subtracting the
    whole of that building from each half would report a teardown twice;
    ignoring the split would report the piece with nothing on it as though the
    neighbour's triplex stood there.

    `_allocate_existing` is the split, and what it divides by is measured
    rather than assumed: `footprint_share`, the share of the lot's building
    footprint that actually stands on this piece, computed by
    `postgis.compute_lot_zone_pieces` from `building_lot_intersections`. So a
    corner commercial strip carrying the whole of the retail block is charged
    the whole of it, and the yard behind is vacant land with its full envelope
    to build. Land value goes by `area_share` instead, because the ground
    divides by the ground. See that function for the argument, and for the
    documented fallback where a lot carries no measured footprint at all.

    A lot one zone covers whole has a share of 1 and every number below is
    exactly what it was before the pieces existed.

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
    joined = _allocate_existing(frame, _join_existing(frame, existing))

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

    # The commerce and the industry with the cellar under them, which the two
    # `hbu_*_floor_area_m2` columns above deliberately exclude: those are the
    # above-grade plates, because the roll's side of the gap is above-grade
    # floor and a gap between two different definitions is not a gap. This
    # pair is the whole of what the program would lease, and it is a *timing*
    # input rather than a comparison - `urban_rag.proforma` divides it by an
    # absorption rate in square feet a month to say how long the space takes
    # to fill. The solve digs for shops across this borough, so the cellar is
    # a third of the retail floor on a typical answer and leaving it out would
    # lease the building a quarter faster than it fills.
    for class_name in ("commercial", "industrial"):
        frame[f"hbu_{class_name}_floor_area_with_cellar_m2"] = _numeric(
            frame, f"{class_name}_area_m2"
        ) + _numeric(frame, f"basement_{class_name}_area_m2")

    frame["existing_num_dwellings"] = _numeric(joined, "num_dwellings")
    frame["existing_num_storeys"] = _numeric(joined, "num_storeys")
    frame["existing_year_built"] = _numeric(joined, "year_built")
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
    # The same NOI split by the family that earns it, netted with the one
    # ratio so the three sum to the total above rather than to something near
    # it. This is the *income* mix and not the floor mix: the three
    # `hbu_*_floor_area_m2` columns above are above-grade plates in square
    # metres, and at the solver's own rates a square foot of commerce earns
    # about four times what a square foot of housing does. A reader weighting
    # anything by use - a blended cap rate, a lease-up, a thesis - wants
    # these, and `urban_rag.proforma` is the reader this exists for.
    #
    # One ratio for all three is the same simplification the solve makes and
    # is stated here rather than hidden: a triple-net retail lease leaves the
    # landlord a far smaller expense load than an apartment does, so this
    # understates the commercial share of NOI. Splitting the ratio is the
    # obvious next input, and it belongs beside `operating_expense_ratio`
    # rather than here.
    # Each family's share of the *space* rent, applied to the whole NOI. The
    # parking's rent is inside `annual_gross_revenue_cad` and belongs to no
    # family, so netting each family's own rent would leave the three short
    # of the total by exactly that; spreading it by share keeps them summing
    # to the line above, with the parking income riding on the floor its
    # tenants live in. Identical to netting each line where nothing is rented.
    space_gross = sum(
        _numeric(frame, f"annual_{class_name}_gross_revenue_cad").fillna(0.0)
        for class_name in INCOME_CLASSES
    )
    for class_name in INCOME_CLASSES:
        share = (
            _numeric(frame, f"annual_{class_name}_gross_revenue_cad") / space_gross
        ).where(space_gross > 0.0, 0.0)
        frame[f"hbu_{class_name}_noi_cad"] = frame["hbu_annual_stabilised_noi_cad"] * share
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
    # The standing building earns from today, so its stream is not pushed out
    # by a build or a lease-up: `hold_pv_factor`, not the delayed
    # `annual_pv_factor` the proposal's `npv_cad` was solved on. That
    # difference is the whole of what a rebuild gives up while the site is a
    # hole in the ground, and it is in the gain below by construction.
    frame["existing_present_value_cad"] = (
        frame["existing_annual_stabilised_noi_cad"] * investment.hold_pv_factor
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
            below_grade_rent_discount_pct=float(
                payload.get(
                    "below_grade_rent_discount_pct",
                    default.below_grade_rent_discount_pct,
                )
            ),
            construction_months=int(
                payload.get("construction_months", default.construction_months)
            ),
            lease_up_months=int(
                payload.get("lease_up_months", default.lease_up_months)
            ),
        )
    except (ProgramError, TypeError, ValueError):
        return default


def program_assumptions_of(hbu: pd.DataFrame) -> ProgramAssumptions:
    """The whole `ProgramAssumptions` the chosen programs were solved with.

    `investment_assumptions_of` widened to every field `as_metadata` writes,
    for the one caller that has to solve again on the same terms: the
    enhancement in `solve_enhancements` prices its addition at the rates the
    rebuild was priced at, or the two futures are not comparable. Falls back
    to the module defaults field by field where a key is absent - an older
    partition - and wholesale where the payload cannot be read.
    """
    default = ProgramAssumptions()
    if hbu.empty or "program_assumptions" not in hbu.columns:
        return default
    values = hbu["program_assumptions"].dropna()
    if not len(values):
        return default
    try:
        payload = json.loads(values.iloc[0])
    except (TypeError, ValueError):
        return default
    if not isinstance(payload, dict):
        return default

    def number(name: str, fallback):
        value = payload.get(name)
        return fallback if value is None else value

    try:
        parking = default.parking
        parking = ParkingRules(
            stalls_per_dwelling=float(number("stalls_per_dwelling", parking.stalls_per_dwelling)),
            stalls_per_1000_sqft=float(number("stalls_per_1000_sqft", parking.stalls_per_1000_sqft)),
            underground_area_sqft=float(number("underground_stall_area_sqft", parking.underground_area_sqft)),
            surface_area_sqft=float(number("surface_stall_area_sqft", parking.surface_area_sqft)),
            garage_area_sqft=float(number("garage_stall_area_sqft", parking.garage_area_sqft)),
            underground_cost_cad=float(number("underground_stall_cost_cad", parking.underground_cost_cad)),
            surface_cost_cad=float(number("surface_stall_cost_cad", parking.surface_cost_cad)),
            garage_cost_cad=float(number("garage_stall_cost_cad", parking.garage_cost_cad)),
            amortization_months=int(number("amortization_months", parking.amortization_months)),
            max_underground_levels=int(number("max_underground_levels", parking.max_underground_levels)),
            underground_lot_share=float(number("underground_lot_share", parking.underground_lot_share)),
            max_surface_stalls=(
                None if payload.get("max_surface_stalls") is None
                else int(payload["max_surface_stalls"])
            ),
            max_garage_stalls=(
                None if payload.get("max_garage_stalls") is None
                else int(payload["max_garage_stalls"])
            ),
            monthly_rent_cad=float(
                number("parking_stall_rent_cad_month", parking.monthly_rent_cad)
            ),
            occupancy_pct=float(
                number("parking_stall_occupancy_pct", parking.occupancy_pct)
            ),
            market_stalls_per_dwelling=float(
                number("market_stalls_per_dwelling", parking.market_stalls_per_dwelling)
            ),
            market_stalls_per_1000_sqft=float(
                number("market_stalls_per_1000_sqft", parking.market_stalls_per_1000_sqft)
            ),
            absorption_saving_months=float(
                number(
                    "parking_absorption_saving_months",
                    parking.absorption_saving_months,
                )
            ),
        )
        costs = default.construction
        construction = ConstructionCosts(
            residential_cost_per_sqft=float(number("residential_cost_per_sqft_cad", costs.residential_cost_per_sqft)),
            commercial_cost_per_sqft=float(number("commercial_cost_per_sqft_cad", costs.commercial_cost_per_sqft)),
            industrial_cost_per_sqft=float(number("industrial_cost_per_sqft_cad", costs.industrial_cost_per_sqft)),
            below_grade_premium=float(number("below_grade_cost_premium", costs.below_grade_premium)),
            amortization_months=int(number("amortization_months", costs.amortization_months)),
        )
        rents = default.non_residential
        non_residential = NonResidentialEconomics(
            commercial_per_sqft_year=float(number("commercial_rent_per_sqft_year_cad", rents.commercial_per_sqft_year)),
            industrial_per_sqft_year=float(number("industrial_rent_per_sqft_year_cad", rents.industrial_per_sqft_year)),
            commercial_vacancy_pct=float(number("commercial_vacancy_pct", rents.commercial_vacancy_pct)),
            industrial_vacancy_pct=float(number("industrial_vacancy_pct", rents.industrial_vacancy_pct)),
        )
        storeys = default.heights
        heights = StoreyHeights(
            residential_m=float(number("residential_storey_height_m", storeys.residential_m)),
            commercial_m=float(number("commercial_storey_height_m", storeys.commercial_m)),
            industrial_m=float(number("industrial_storey_height_m", storeys.industrial_m)),
        )
        return ProgramAssumptions(
            parking=parking,
            construction=construction,
            non_residential=non_residential,
            heights=heights,
            investment=investment_assumptions_of(hbu),
            basement_levels_allowed=int(
                number("basement_levels_allowed", default.basement_levels_allowed)
            ),
            max_seconds=float(number("max_seconds", default.max_seconds)),
            waive_parking_if_empty=bool(
                number("waive_parking_if_empty", default.waive_parking_if_empty)
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


#: What the roll says about the *building*, and therefore what
#: `footprint_share` divides. Every one of these is a quantity of building or
#: an income it earns, so it belongs where the building stands: a corner strip
#: with the whole of the block on it takes the whole of the floor area, the
#: dwellings and the NOI, and the yard behind it takes none.
_FOOTPRINT_ALLOCATED: tuple[str, ...] = (
    *_EXISTING_AREA_COLUMNS.values(),
    "num_dwellings",
    "num_assessment_units",
    "gross_income_cad",
    "net_operating_income_cad",
)

#: What the roll says about the *ground*, and therefore what `area_share`
#: divides. `total_assessed_value` is land and building together and the roll
#: does not always separate them, so it goes by area: the alternative is to
#: split one number by two rules, and an assessed value that does not sum back
#: to the parcel's is worse than one allocated by the simpler measure.
_AREA_ALLOCATED: tuple[str, ...] = ("total_assessed_value",)

#: Which of the allocated columns are **counts** rather than quantities, and
#: therefore have to come back as whole numbers.
#:
#: A share of a dwelling is not a dwelling. Nine tenths of a triplex is two
#: dwellings or three, and `gold.lot_redevelopment_gap` types both of these
#: `integer`, so an unrounded share reaches Postgres as
#: `invalid input syntax for type integer: "0.984..."` - which is where this
#: rule was learned.
#:
#: Rounded rather than truncated, for the reason `retained_building_of` rounds:
#: a piece holding 60 per cent of a five-unit plex holds three dwellings, and
#: `int()` would say two. The consequence is that a split lot's pieces can sum
#: to one more or one fewer dwelling than the parcel has, and that is the right
#: trade - a count that is wrong by one on two rows beats a count that is a
#: fraction on both, and every *quantity* beside them (floor area, income,
#: value) still sums back exactly.
_ALLOCATED_COUNTS: frozenset[str] = frozenset(
    {"num_dwellings", "num_assessment_units"}
)

#: What the roll says about the building that is not a quantity of it - the
#: age, the storey count, the use code, the cap rate, the expense ratio. These
#: are *descriptions*, and a description does not divide: half a triplex is
#: still three storeys of 1910 construction. Carried onto every piece of the
#: lot unchanged, which is why they are named here rather than left to fall
#: through a default that would have scaled them.
#:
#: `num_storeys` is the one worth pausing on, because it is a count and looks
#: divisible. It is the plate's divisor in `solve_enhancements` - floor area
#: over storeys is the footprint the addition is built on - and the floor area
#: above it is already allocated, so scaling the storeys too would divide the
#: same share twice and put a half-height building on the piece.
_UNALLOCATED: tuple[str, ...] = tuple(
    name
    for name in _EXISTING_COLUMNS
    if name not in {*_FOOTPRINT_ALLOCATED, *_AREA_ALLOCATED}
)


def _allocate_existing(hbu: pd.DataFrame, joined: pd.DataFrame) -> pd.DataFrame:
    """The roll's side of a lot, divided between that lot's zone pieces.

    ``joined`` is `_join_existing`'s output - the whole parcel's assessment
    repeated on each of its pieces - and this scales the columns that are
    quantities of a building or of ground down to the piece's share of it.

    Three rules and every column of `_EXISTING_COLUMNS` falls under exactly
    one, named in `_FOOTPRINT_ALLOCATED`, `_AREA_ALLOCATED` and `_UNALLOCATED`
    above rather than decided by a default here. The shares themselves are
    `lot_zone_pieces`' - measured off the building footprints for the first and
    off the clip areas for the second - and both sum to 1 across a lot's
    pieces, so a borough's totals are unchanged by the split.

    **A frame with no shares is returned untouched**, which is the whole of the
    backward compatibility this needs: a partition written before
    `lot_zone_pieces` has one row per lot, a share of 1 is what that means, and
    multiplying by a column of nulls would silently empty the roll instead.
    """
    if joined.empty:
        return joined
    footprint = _share(hbu, "footprint_share")
    area = _share(hbu, "area_share")
    if footprint is None and area is None:
        return joined
    result = joined.copy()
    for name, share in (
        *((name, footprint) for name in _FOOTPRINT_ALLOCATED),
        *((name, area) for name in _AREA_ALLOCATED),
    ):
        if name not in result.columns or share is None:
            continue
        scaled = _numeric(result, name) * share
        # A count comes back whole; a quantity keeps its fraction. See
        # `_ALLOCATED_COUNTS`. `Int64` rather than `int` so a lot the roll
        # never reached keeps its null instead of becoming a zero, which is
        # the distinction every other column here is careful about.
        result[name] = (
            scaled.round().astype("Int64")
            if name in _ALLOCATED_COUNTS
            else scaled
        )
    return result


def _share(hbu: pd.DataFrame, column: str) -> pd.Series | None:
    """One of `lot_zone_pieces`' two allocators, or None if it is not there.

    A null share is read as 1.0 rather than as 0: it means "nothing said how
    to divide this", and the answer to that is the whole parcel - the number
    this table reported before there were pieces - not an empty building.
    """
    if column not in hbu.columns:
        return None
    values = pd.to_numeric(hbu[column], errors="coerce")
    if values.notna().sum() == 0:
        return None
    return values.fillna(1.0)


def _join_existing(hbu: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    """The roll's side of every lot, aligned to ``hbu``'s rows.

    One row of the roll can now align to several rows of ``hbu`` - a lot two
    zones cut in two has two - and that is what the `reindex` below does
    naturally. `_allocate_existing` is what then divides it between them; this
    function only repeats it.

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


# --------------------------------------------------------------------------
# the second future: the building stays and grows
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class EnhancementRules:
    """What an addition costs in time and disruption, and how far it may go.

    ``construction_months`` and ``lease_up_months`` are the addition's own -
    shorter than a rebuild's, because the shell is there - and push the added
    income out the way `InvestmentAssumptions` pushes a rebuild's.
    ``disruption_share`` is the share of the standing building's NOI lost
    while the works are on: tenants decanted from the top floor, the yard a
    site, a shop behind hoarding. ``addition_cost_premium`` and
    ``max_added_storeys`` are `RetainedBuilding`'s, stated once here for the
    borough. ``max_seconds`` bounds each CP-SAT run, as `ProgramAssumptions.
    max_seconds` bounds the rebuild's.
    """

    construction_months: int = 9
    lease_up_months: int = 3
    disruption_share: float = 0.25
    addition_cost_premium: float = 1.5
    max_added_storeys: int = 1
    max_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.construction_months < 0 or self.lease_up_months < 0:
            raise ValueError("the enhancement's months must not be negative")
        if not 0.0 <= self.disruption_share <= 1.0:
            raise ValueError("disruption_share is a share of NOI and must be in [0, 1]")
        if self.addition_cost_premium <= 0:
            raise ValueError("addition_cost_premium must be positive")
        if self.max_added_storeys < 0:
            raise ValueError("max_added_storeys must not be negative")
        if self.max_seconds <= 0:
            raise ValueError("max_seconds must be positive")

    def as_metadata(self) -> dict[str, object]:
        return {
            "enhance_construction_months": self.construction_months,
            "enhance_lease_up_months": self.lease_up_months,
            "enhance_disruption_share": self.disruption_share,
            "addition_cost_premium": self.addition_cost_premium,
            "max_added_storeys": self.max_added_storeys,
            "enhance_max_seconds": self.max_seconds,
        }


#: Why a lot has no enhancement program, beside CP-SAT's own statuses.
ENHANCEMENT_STATUSES: tuple[str, ...] = (
    "OPTIMAL",
    "FEASIBLE",
    "INFEASIBLE",
    "UNKNOWN",
    "ERROR",
    "no_building",  # nothing stands, or the roll states no storey count
    "not_underbuilt",  # the envelope holds no more than what stands
    "no_program",  # the rebuild was not solved, so there is nothing to grow toward
    "no_envelope",  # the governing zone's columns could not be rebuilt
)

#: What `solve_enhancements` writes, in reading order. Every money figure is
#: the addition's own; the whole building's floor is `enhance_gross_floor_
#: area_m2` and what it adds is `enhance_added_floor_area_m2`.
ENHANCEMENT_COLUMNS: tuple[str, ...] = (
    "enhance_status",
    "enhance_solved",
    "enhance_solve_error",
    "enhance_floors",
    "enhance_added_storeys",
    "enhance_footprint_m2",
    "enhance_gross_floor_area_m2",
    "enhance_added_floor_area_m2",
    "enhance_added_dwellings",
    "enhance_num_dwellings",
    "enhance_units",
    "enhance_added_commercial_area_m2",
    "enhance_added_industrial_area_m2",
    "enhance_surface_stalls",
    # The addition's counterpart of `parking_waived` / `waived_stalls`: the
    # standing shop's floor owes stalls too, and a building with no yard to
    # put them on had no enhancement at all until the parking was waived.
    "enhance_parking_waived",
    "enhance_waived_stalls",
    "enhance_capital_cost_cad",
    "enhance_added_annual_gross_income_cad",
    "enhance_added_annual_stabilised_noi_cad",
    # The addition's income by the family that earns it, summing to the line
    # above. The counterpart of the gap's `hbu_*_noi_cad` on the other future,
    # and read for the same reason: an addition of retail leases at a
    # different speed and sells at a different cap than one of flats.
    "enhance_added_residential_noi_cad",
    "enhance_added_commercial_noi_cad",
    "enhance_added_industrial_noi_cad",
    "enhance_present_value_cad",
    "enhance_npv_cad",
    "enhance_disruption_cad",
    "enhance_gain_cad",
    "enhance_binding",
)

#: The three things an owner can do with a lot, and what each is worth.
FUTURES: tuple[str, ...] = ("hold", "enhance", "rebuild")
FUTURE_COLUMNS: tuple[str, ...] = (
    "hold_value_cad",
    "enhance_value_cad",
    "rebuild_value_cad",
    "best_future",
)


def retained_building_of(
    existing: Mapping,
    rules: EnhancementRules | None = None,
    *,
    share: float = 1.0,
) -> RetainedBuilding | None:
    """The roll's building, as the block the enhancement builds on.

    ``None`` where there is nothing to retain: no floor, or no storey count
    to divide it by. The plate is floor over storeys - the roll states no
    footprint - which is exact for a plex and generous for a building with a
    setback storey; the BDOI footprint on `lot_profiles` is the better number
    where that asset has run, and is the obvious next input here.

    ``share`` is `lot_zone_pieces`' `footprint_share`: the roll describes a
    lot and an enhancement is solved on a *piece* of one, so the building has
    to be split the same way `use_gap` splits it. What is scaled is every
    quantity of building - the three floor areas, the dwellings, the gross -
    and what is not is the **storey count**, because a description does not
    divide: half a triplex is still three storeys, and scaling both would
    divide the plate twice and put a half-height building on the piece.

    A piece with no building on it comes back ``None`` and is correctly
    reported as having nothing to enhance. 1.0 is the whole parcel, which is
    what one zone covering a lot whole means and what every caller passed
    before the pieces existed.
    """
    rules = rules or EnhancementRules()
    share = 1.0 if share is None else max(float(share), 0.0)
    storeys = _int_or_none(existing.get("num_storeys"))
    residential = (
        _float_or_none(existing.get("residential_floor_area_m2")) or 0.0
    ) * share
    commercial = (
        _float_or_none(existing.get("commercial_floor_area_m2")) or 0.0
    ) * share
    industrial = (
        _float_or_none(existing.get("industrial_floor_area_m2")) or 0.0
    ) * share
    floor = residential + commercial + industrial
    if floor <= 0.0 or storeys is None or storeys < 1:
        return None
    gross = (_float_or_none(existing.get("gross_income_cad")) or 0.0) * share
    return RetainedBuilding(
        footprint_m2=floor / storeys,
        storeys=storeys,
        residential_floor_area_m2=residential,
        commercial_floor_area_m2=commercial,
        industrial_floor_area_m2=industrial,
        # Rounded rather than truncated: a piece holding 60 % of a five-unit
        # plex holds three dwellings, and int() would say two.
        dwellings=round((_int_or_none(existing.get("num_dwellings")) or 0) * share),
        monthly_gross_revenue_cad=gross / MONTHS_PER_YEAR,
        max_added_storeys=rules.max_added_storeys,
        addition_cost_premium=rules.addition_cost_premium,
    )


def envelope_of(group: pd.DataFrame) -> ZoneEnvelope | None:
    """One zone's envelope rows, as the `ZoneEnvelope` `_envelope_row` solves.

    The governing half of `_envelope_row`, factored out so the enhancement
    can be solved against exactly the rule-set the rebuild was: the asset's
    own `governs_*` flags pick each family's column, and a zone where nothing
    governs falls back the same way. ``None`` where no row parses.
    """
    parsed: list[tuple[dict, ZoneColumn]] = []
    for record in group.to_dict("records"):
        try:
            parsed.append((record, zone_column_of(record)))
        except (ProgramError, ValueError, KeyError):
            continue
    if not parsed:
        return None
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
    if envelope.is_empty:
        envelope = ZoneEnvelope.of([zone_column for _, zone_column in parsed], 0.0)
        if envelope.is_empty:
            envelope = ZoneEnvelope.single(parsed[0][1])
    return envelope


def _income_shares(program) -> dict[str, float]:
    """Each family's share of a program's gross, keyed by `INCOME_CLASSES`.

    Shares rather than dollars because the caller multiplies them back onto an
    NOI the solve already netted: one operating expense ratio covers the whole
    building, so a share of the gross is the same share of the income and the
    parts add back to the whole exactly. A program earning nothing is all
    zeroes rather than a division by it.
    """
    parts = {
        "residential": program.residential_gross_revenue_cad,
        "commercial": program.commercial_gross_revenue_cad,
        "industrial": program.industrial_gross_revenue_cad,
    }
    # Over the three family rents rather than over the gross: the parking's
    # rent is inside the gross and belongs to no family, and dividing by the
    # gross would leave the three shares summing to less than one. Spread this
    # way the parking's income rides with the floor its tenants live in.
    space_gross = sum(parts.values())
    if space_gross <= 0.0:
        return dict.fromkeys(parts, 0.0)
    return {name: value / space_gross for name, value in parts.items()}


def _enhancement_row(program, retained: RetainedBuilding, *, disruption: float) -> dict:
    """One enhancement's answer, flattened.

    ``disruption`` is what the works cost the standing building, and it is
    charged only where there are works: an enhancement that adds nothing
    (`nothing_pencils`, the solver's answer normalised to the standing
    building) has no site, loses no rent, and is worth exactly what holding
    is worth - its gain is 0, not minus a disruption on works nobody does.
    """
    solved = program.status in ("OPTIMAL", "FEASIBLE")
    # The addition's own income, split by what earns it. `program` here is the
    # *addition* - the retained building's rent is `retained_monthly_gross_cad`
    # and outside every figure below - so these are what the new floor adds,
    # which is exactly what a return on the addition is computed from.
    shares = _income_shares(program)
    nothing_added = solved and program.added_floor_area_m2 <= 0.0
    if nothing_added:
        disruption = 0.0
    added_commercial = max(
        program.commercial_area_m2 - retained.commercial_floor_area_m2, 0.0
    )
    added_industrial = max(
        program.industrial_area_m2 - retained.industrial_floor_area_m2, 0.0
    )
    npv = program.npv_cad if solved else 0.0
    return {
        "enhance_status": program.status,
        "enhance_solved": solved,
        "enhance_solve_error": None,
        "enhance_floors": program.floors if solved else 0,
        "enhance_added_storeys": max(program.floors - retained.storeys, 0) if solved else 0,
        "enhance_footprint_m2": program.footprint_m2 if solved else 0.0,
        "enhance_gross_floor_area_m2": program.gross_floor_area_m2 if solved else 0.0,
        "enhance_added_floor_area_m2": program.added_floor_area_m2 if solved else 0.0,
        "enhance_added_dwellings": program.total_dwellings if solved else 0,
        "enhance_num_dwellings": (
            program.total_dwellings + retained.dwellings if solved else retained.dwellings
        ),
        "enhance_units": json.dumps(dict(program.units), ensure_ascii=False),
        "enhance_added_commercial_area_m2": added_commercial if solved else 0.0,
        "enhance_added_industrial_area_m2": added_industrial if solved else 0.0,
        "enhance_surface_stalls": program.surface_stalls if solved else 0,
        "enhance_parking_waived": bool(program.parking_waived) if solved else False,
        "enhance_waived_stalls": int(program.waived_stalls) if solved else 0,
        "enhance_capital_cost_cad": program.total_capital_cost_cad if solved else 0.0,
        "enhance_added_annual_gross_income_cad": (
            program.gross_revenue_cad * MONTHS_PER_YEAR if solved else 0.0
        ),
        "enhance_added_annual_stabilised_noi_cad": (
            program.annual_stabilised_noi_cad if solved else 0.0
        ),
        **{
            f"enhance_added_{name}_noi_cad": (
                program.annual_stabilised_noi_cad * shares[name] if solved else 0.0
            )
            for name in INCOME_CLASSES
        },
        "enhance_present_value_cad": program.present_value_cad if solved else 0.0,
        "enhance_npv_cad": npv,
        "enhance_disruption_cad": disruption if solved else 0.0,
        "enhance_gain_cad": (npv - disruption) if solved else None,
        "enhance_binding": json.dumps(
            list(program.binding) + (["nothing_pencils"] if nothing_added else []),
            ensure_ascii=False,
        ),
    }


_NO_ENHANCEMENT: dict = {
    "enhance_status": None,
    "enhance_solved": False,
    "enhance_solve_error": None,
    "enhance_floors": 0,
    "enhance_added_storeys": 0,
    "enhance_footprint_m2": 0.0,
    "enhance_gross_floor_area_m2": 0.0,
    "enhance_added_floor_area_m2": 0.0,
    "enhance_added_dwellings": 0,
    "enhance_num_dwellings": 0,
    "enhance_units": "{}",
    "enhance_added_commercial_area_m2": 0.0,
    "enhance_added_industrial_area_m2": 0.0,
    "enhance_surface_stalls": 0,
    "enhance_parking_waived": False,
    "enhance_waived_stalls": 0,
    "enhance_capital_cost_cad": 0.0,
    "enhance_added_annual_gross_income_cad": 0.0,
    "enhance_added_annual_stabilised_noi_cad": 0.0,
    "enhance_added_residential_noi_cad": 0.0,
    "enhance_added_commercial_noi_cad": 0.0,
    "enhance_added_industrial_noi_cad": 0.0,
    "enhance_present_value_cad": 0.0,
    "enhance_npv_cad": 0.0,
    "enhance_disruption_cad": 0.0,
    "enhance_gain_cad": None,
    "enhance_binding": "[]",
}


def solve_enhancements(
    hbu: pd.DataFrame,
    existing: pd.DataFrame,
    envelopes: pd.DataFrame,
    economics: UnitEconomics,
    *,
    assumptions: ProgramAssumptions | None = None,
    rules: EnhancementRules | None = None,
) -> pd.DataFrame:
    """One `solve_program` per lot with a building and room above it, the
    building retained.

    ``hbu`` is `select_highest_best_use`'s output - the rebuild, and the zone
    it was solved in; ``existing`` is `lot_assessment_comparables`, the
    building that stands; ``envelopes`` is `lot_zoning_envelopes`, from which
    the zone's rule-set is rebuilt exactly as the rebuild saw it. The addition
    is priced on ``assumptions`` - the rebuild's own rates, read back off the
    hbu rows by `program_assumptions_of` - with the timing and the premium
    swapped for ``rules``'.

    Solved only where it can mean something: a solved rebuild, a building the
    roll gives a floor and a storey count for, and an envelope holding more
    than stands. Every other lot keeps a row with its `enhance_status` saying
    which of those it lacked. Returns `ENHANCEMENT_COLUMNS` indexed like
    ``hbu``.
    """
    assumptions = assumptions or ProgramAssumptions()
    rules = rules or EnhancementRules()
    investment = _replace(
        assumptions.investment,
        construction_months=rules.construction_months,
        lease_up_months=rules.lease_up_months,
    )
    key = "NO_LOT" if "NO_LOT" in existing.columns else "lot_number"
    by_number: dict = {}
    if not existing.empty and key in existing.columns:
        by_number = {
            str(number): record
            for number, record in zip(
                existing[key], existing.drop_duplicates(key).to_dict("records")
            )
        } if len(existing) == len(existing.drop_duplicates(key)) else {
            str(record[key]): record
            for record in existing.drop_duplicates(key).to_dict("records")
        }
    grouped = (
        {
            (uid, feature): group
            for (uid, feature), group in envelopes.groupby(
                ["lot_uid", "feature_id"], sort=False
            )
        }
        if not envelopes.empty and {"lot_uid", "feature_id"} <= set(envelopes.columns)
        else {}
    )

    rows: list[dict] = []
    for record in hbu.to_dict("records"):
        if record.get("hbu_status") != "solved" or not record.get("solved", True):
            rows.append({**_NO_ENHANCEMENT, "enhance_status": "no_program"})
            continue
        standing = by_number.get(str(record.get("lot_number")))
        # The piece's share of the parcel's building, the same allocator
        # `use_gap` divides the roll by. A piece with nothing standing on it
        # has nothing to enhance and reports `no_building`, which is the true
        # answer rather than a gap.
        share = _float_or_none(record.get("footprint_share"))
        share = 1.0 if share is None else share
        retained = (
            retained_building_of(standing, rules, share=share) if standing else None
        )
        if retained is None:
            rows.append({**_NO_ENHANCEMENT, "enhance_status": "no_building"})
            continue
        proposed = _float_or_none(record.get("gross_floor_area_m2")) or 0.0
        if proposed <= retained.floor_area_m2:
            rows.append({**_NO_ENHANCEMENT, "enhance_status": "not_underbuilt"})
            continue
        group = grouped.get((record.get("lot_uid"), record.get("feature_id")))
        envelope = envelope_of(group) if group is not None else None
        if envelope is None:
            rows.append({**_NO_ENHANCEMENT, "enhance_status": "no_envelope"})
            continue
        try:
            program = solve_program(
                envelope,
                lot_of(record),
                economics,
                parking=assumptions.parking,
                construction=assumptions.construction,
                non_residential=assumptions.non_residential,
                heights=assumptions.heights,
                investment=investment,
                basement_levels_allowed=0,
                max_seconds=rules.max_seconds,
                retained=retained,
                waive_parking_if_empty=assumptions.waive_parking_if_empty,
            )
        except (ProgramError, ValueError, KeyError) as exc:
            rows.append(
                {**_NO_ENHANCEMENT, "enhance_status": "ERROR", "enhance_solve_error": str(exc)}
            )
            continue
        # What the works cost the building that keeps earning: a share of its
        # NOI for the months the site is one. Undiscounted, because it is
        # spent in the first year, where a dollar is a dollar.
        # Scaled by the same share as the building it is earned by: the works
        # disturb the part of the parcel being built on, not the neighbour's
        # half of it. `_enhancement_row` drops it where the solve adds
        # nothing - no works, no site, nothing disturbed.
        standing_noi = (
            _float_or_none(standing.get("net_operating_income_cad")) or 0.0
        ) * share
        disruption = (
            standing_noi * rules.disruption_share * rules.construction_months
            / MONTHS_PER_YEAR
        )
        rows.append(_enhancement_row(program, retained, disruption=disruption))
    frame = pd.DataFrame(rows, columns=list(ENHANCEMENT_COLUMNS))
    frame.index = hbu.index
    return frame


def three_futures(frame: pd.DataFrame) -> pd.DataFrame:
    """What holding, enhancing and rebuilding are each worth to the owner.

    All three on one footing - the land excluded, since the owner holds it in
    every future - and before the site's own costs, which `urban_rag.
    opportunities` adds when it prices the buyer. ``hold`` is the standing
    building's discounted NOI; ``enhance`` is that plus the addition's own
    NPV less the disruption; ``rebuild`` is the proposal's NPV, whose income
    starts only after the build and the lease-up. ``best_future`` is the
    largest, ``hold`` on a tie and wherever nothing else was solved.
    """
    hold = _numeric(frame, "existing_present_value_cad").fillna(0.0)
    gain = _numeric(frame, "enhance_gain_cad")
    enhance = (hold + gain).where(gain.notna())
    rebuild = _numeric(frame, "hbu_npv_cad")
    values = pd.DataFrame(
        {"hold": hold, "enhance": enhance, "rebuild": rebuild}, index=frame.index
    )
    # `hold` is never null, so the row-wise max has a value everywhere and the
    # ties go to holding by column order.
    best = values.fillna(-np.inf).idxmax(axis=1)
    result = pd.DataFrame(index=frame.index)
    result["hold_value_cad"] = hold.round(2)
    result["enhance_value_cad"] = enhance.round(2)
    result["rebuild_value_cad"] = rebuild.round(2)
    result["best_future"] = best.astype("object")
    return result
