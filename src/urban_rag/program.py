"""The most rent a lot's zoning envelope can carry, solved with CP-SAT.

A highest-and-best-use question ends in a number: given what the *grille des
usages et des normes* lets you build on this parcel, and what CMHC says the
neighbourhood pays, what unit mix is worth the most? Everything upstream of
this module - the cadastre, the spatial joins, the frontage, the rent and
vacancy grids - exists to fill in the inputs of that one question.

The answer is an integer program, and deliberately not a formula. Three of the
four caps interact: the dwelling ceiling is a count, the density cap is a floor
*area*, and the site-coverage cap is a footprint that only becomes floor area
once multiplied by a number of storeys that is itself capped. `gross floor
area = footprint x floors` is a product of two decision variables, which is
what rules out a linear program and what `AddMultiplicationEquality` is for.
Add the requirement that dwellings come in whole numbers and CP-SAT is the tool
that fits the shape of the problem rather than the one that happens to be
installed.

**What this module is handed, and what it is not.** It takes a `ZoneColumn`
already parsed out of a zoning grid, not a PDF. `urban_rag.zoning_grid` is what
produces one, and it is a separate module for a reason: `linked_documents`
flattens a grid to text for the embedding corpus and stops there, and the
flattened text does not survive the trip - `pypdf`'s default extraction drops
the column alignment that says *which* column an `X` belongs to, so
`Tous sauf le RDC   X` cannot be attributed to the Habitation column without
the x-coordinates that `extraction_mode="layout"` keeps. Keeping the two apart
is also what makes this module testable: the arithmetic below is exercised
against hand-written envelopes rather than against heterogeneous municipal
PDFs. `lot_zoning_envelopes` is where a parsed column meets a real parcel - one
row of it is one call to `solve_program`.

**Units.** The zoning grid is metric and the unit schedule is imperial, so the
two are converted at exactly one place (`M2_PER_SQFT`) and every constraint is
built in square metres. Mixing them is the failure this module is most exposed
to: a density ratio computed with square feet over square metres is wrong by a
factor of ten and looks entirely plausible.

**Vacancy is a percentage.** `vacancy_rates` stores rates as published - 0.2%
is `0.2`, not `0.002` - so the occupancy factor divides by 100. Reading that
column as a fraction understates revenue by two orders of magnitude, which is
the kind of error that produces a confident answer rather than an exception.

**Parking is where the envelope stops being one number.** Every dwelling owes
stalls (`ParkingRules.stalls_per_dwelling`) and so does every thousand square
feet of commerce or industry (`ParkingRules.stalls_per_1000_sqft`), and the two
ways to provide them are not substitutes. Article 38 1° of by-law 01-283
excludes *une aire de stationnement des véhicules [...] située en sous-sol, de
même que leurs voies
d'accès* from the *superficie de plancher* the density index is computed on -
so an underground stall is invisible to the *Densité* cap and, being below
grade, to *En étage* as well. An above-grade stall is neither: it sits in a
storey that counts against both. The trade the solver is asked to make is
therefore a real one - underground buys FSR back at a higher price per stall,
above grade is cheaper per stall but is paid for in dwellings that no longer
fit. Neither dominates, which is why both are decision variables.

**One footprint, identical floors.** Every storey - residential, above-grade
parking, underground - is the same plate, so the footprint is whatever the
hungriest floor type needs and the model carries a single `footprint`
variable. That is an assumption about the building, not a norm: a real garage
would run wider than the tower above it (article 43 excludes *une partie du
bâtiment qui est entièrement sous terre* from the site-coverage calculation,
so it could), and modelling that would need a second footprint.

**The storey cap is printed twice.** A grid states *En étage* and *Hauteur en
mètre* side by side, and they are two ceilings on the same stack rather than
one stated in two units: a storey is only a fixed number of metres if you say
what is in it, and `StoreyHeights` is where that is said - three metres for a
dwelling storey, four for commerce or for industry, three for an above-grade
deck. Six storeys of housing clear an 18 m limit and four of commerce do not
clear a 15, so a column printing ``6`` and ``15`` allows five of one or three
of the other, and neither number is the storey row.

That is what a metric cap does to the mix: it prices the storey types against
each other. *En étage* charges a retail plate exactly what it charges a
residential one, so commerce wins any storey its rent can pay for; *Hauteur*
charges it a third more, on top of the parking a retail plate already owes,
which is what can hand the storey back to the housing. Both caps are linear in
the storey counts - no product of decision variables, so the metric one costs
the model nothing to carry - and `binding` reports `height_max` where it is
what stopped the building.

An underground level is charged nothing, which is not a simplification: height
is measured from grade up. It is the same exclusion article 38 1° gives the
parking against *Densité*, arriving from a different article and pointing the
same way, and it is why a tight envelope digs.

**The objective is discounted net profit.** The question a land developer
actually asks of a parcel is not "what earns the most a month" but "what is
worth the most to build": the present value of the income the building will
throw off, less what it costs to put up. Revenue is `rent x (1 - vacancy)` a
month; `InvestmentAssumptions.operating_expense_ratio` takes off the share of
it that never reaches the owner, and the stabilised NOI that remains is
discounted at `discount_rate_pct` over `hold_years`, with the building sold on
at `terminal_cap_rate_pct` at the end of the hold. Against that present value
stand the capital costs in full and undiscounted - the dwellings at
`ConstructionCosts.residential_cost_per_sqft` over the schedule in
`UNIT_AREAS_SQFT`, the non-residential floors per gross square foot, and the
stalls at `ParkingRules`' per-stall prices - because they are spent at the
start where a dollar is worth a dollar. Every term is linear in the same
decision variables the old monthly objective was, so the model is the same
shape; only the prices changed. `UNDISCOUNTED_INVESTMENT` is the old
objective's exact prices - zero discount, zero expenses, no terminal value -
under which the argmax is what it always was, and the tests that pin cap
arithmetic use it so they stay about the caps.

CMHC surveys the *standing stock's* average rent, and a proforma is priced at
what a new building leases for; `new_build_rent_premium_pct` is the stated
gap between the two, applied to the dwelling rents only - the non-residential
rates are already market quotes. Set it to zero to price the proposal at the
stock average, which is the conservative reading and the one the module gave
before the premium existed.

What the objective does **not** net out is everything nobody handed this
module: land, soft costs, demolition, financing during construction, and any
revenue the stalls themselves might earn. It is the value of the completed
building less the cost of building it, and no further. `AMORTIZATION_MONTHS`
survives for one purpose: `DevelopmentProgram.net_operating_income` still
reports the straight-line monthly figure the platform's other tables read,
computed from the chosen program rather than maximised.

**Why the revenue is not scaled by area.** An earlier objective maximised
`area_sqft x rent x (1 - vacancy)`, which is dollars-times-square-feet and
was only ever a proxy for "bigger is better". Netting a real cost against it
would not work: a three-bedroom would show $2 029 800 of monthly "revenue"
against $1 220 of monthly construction, and the cost side would be rounding
error on a number that was never dollars. CMHC publishes rent per dwelling
per month, so that is what the revenue is, and the unit schedule earns its
keep on the cost side instead - where a square foot genuinely is priced.

The mix this produces is different, and the difference is the point: rent per
square foot falls as dwellings get larger while construction cost per square
foot does not, so a binding envelope now fills with the class that earns most
per square metre rather than the class that is simply biggest.

**Dwellings are not the only thing a zone authorises.** A column headed
``H.2, C.2`` permits commerce beside the housing and one headed ``I.1``
permits industry instead of it, so the envelope those columns describe can be
filled with more than one kind of space. `commercial_floors` and
`industrial_floors` are storeys of that space - whole plates, like the parking
storeys and for the same "one footprint, identical floors" reason - and they
compete with the dwellings for the storeys *En etage* allows and for the floor
area *Densite* allows.

**And a zone states its usages across columns, not within one.** Most boroughs
print one usage family per column - all 1 463 parsed columns of
Villeray-Saint-Michel-Parc-Extension carry exactly one code - so a zone
permitting housing and commerce says so in two columns, and reading a column
at a time can only ever describe a pure building. `ZoneEnvelope` is the whole
zone as one rule-set, and `solve_program` takes one: the usage floor area is
split by the model rather than fixed by which column it was handed. Each
column's norms bind only while the family it heads is built, so the pure
programs remain feasible points of the same model and a mixed answer is
returned exactly when it is worth more. A single `ZoneColumn` is still
accepted, wrapped by `ZoneEnvelope.single`, and solves what it always did.

Non-residential space is priced and rented **per square foot**, which is the
one place its arithmetic differs from a dwelling's: CMHC surveys a rent per
dwelling because a two-bedroom is a two-bedroom, and nobody leases "a retail
unit". Costs are `COMMERCIAL_COST_PER_SQFT_CAD` and
`INDUSTRIAL_COST_PER_SQFT_CAD`, amortised over the same `AMORTIZATION_MONTHS`
as everything else. Revenues are `COMMERCIAL_REVENUE_PER_SQFT_CAD` and
`INDUSTRIAL_REVENUE_PER_SQFT_CAD` less `COMMERCIAL_VACANCY_PCT` and
`INDUSTRIAL_VACANCY_PCT` - the same `rent x (1 - vacancy)` a dwelling gets,
with the vacancy stated rather than surveyed. The rents are **annual**, the
unit commercial leasing is quoted in, divided by `MONTHS_PER_YEAR` to sit
beside a monthly dwelling rent. Reading $80/sf as a monthly figure would let a
retail floor collect $960 a square foot a year, which is the same
order-of-magnitude error the vacancy note above is about, pointed the other
way.

**And that space is what makes the parking bite.** A dwelling owes half a
stall; a thousand square feet of commerce owes three, and a stall is 300
square feet of the plate it sits on. A retail storey therefore very nearly
demands a parking storey of its own - floor area *Densite* counts and a storey
*En etage* counts - which is what stops commerce from simply taking every
storey a mixed-use column allows. Whether it still wins is now a question
about the parcel rather than a foregone conclusion, and that is the point of
putting the ratio in.

`NO_PARKING` and `NO_CONSTRUCTION_COST` strip either cost back out, which is
what the tests that pin the rent arithmetic use so a change to a published
rate cannot move a number that is about something else. `NO_NON_RESIDENTIAL`
does the same for the commerce and the industry, leaving the all-residential
question the module answered before they existed.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum

import numpy as np
from ortools.sat.python import cp_model

#: Square metres in a square foot, exactly. The grid's areas, widths and
#: ratios are metric; the unit schedule below is in square feet.
M2_PER_SQFT = 0.09290304

#: Rentable area per dwelling, in square feet, keyed by CMHC's bedroom class.
#:
#: Keyed that way rather than to "studio/1br/2br/3br" so a rent or vacancy
#: frame joins straight on - `cmhc.BEDROOM_TYPES` is where those spellings come
#: from, and CMHC's top class is `3_bedroom_plus` ("3 chambres +") rather than
#: exactly three. A three-bedroom is what it is sized as here.
UNIT_AREAS_SQFT: Mapping[str, float] = {
    "studio": 500.0,
    "1_bedroom": 600.0,
    "2_bedroom": 900.0,
    "3_bedroom_plus": 1200.0,
}

#: Areas are held as integers in hundredths of a square metre. CP-SAT is an
#: integer solver, and rounding a 46.45 m2 studio to a whole 46 would lose ~1%
#: of every unit's area - which compounds across a hundred of them into a
#: floor's worth of slack in the density constraint.
AREA_SCALE = 100

#: Heights are held as integers in hundredths of a metre - centimetres - for
#: the reason above. A grid prints *Hauteur en metre* to the decimetre where it
#: prints one at all (``0/12,5``), and a storey height is a stated assumption
#: rather than a measurement, so a centimetre is finer than either input and
#: the rounding never reaches the answer.
HEIGHT_SCALE = 100

#: The objective is money, and it is an integer for the same reason - held in
#: ten-thousandths of a dollar rather than in cents. Cents are ample for a
#: dwelling, whose coefficient is hundreds of dollars a month, but a square
#: metre of industrial floor nets about twenty cents a month and enters the
#: objective per *hundredth* of a square metre: a fifth of a cent, which
#: rounds to a whole cent with over a percent of error and can tip a choice
#: between two kinds of space that are genuinely close. Four decimal places
#: put that rounding back under a basis point for every term.
MONEY_SCALE = 10_000

#: Commercial and industrial rents are quoted per square foot per *year*; the
#: objective is a month. This is the whole of the conversion between them.
MONTHS_PER_YEAR = 12

#: Stalls owed per dwelling. Villeray's own by-law abolished residential
#: parking minima, so this is a *program* assumption - what the building is
#: being designed to offer - rather than a norm read off the grid.
STALLS_PER_DWELLING = 0.5

#: Stalls owed per thousand square feet of commercial or industrial floor. The
#: same kind of assumption and a much heavier one: at three per thousand a
#: 3 000 sq ft retail plate owes nine stalls, and nine above-grade stalls at
#: `ABOVE_GRADE_STALL_AREA_SQFT` fill 2 700 sq ft - very nearly a second plate
#: of the same size. Commerce that pays for its own parking storey is a
#: different proposition from commerce that does not, and this ratio is the
#: whole of the difference; it is one number precisely so it can be moved.
#:
#: One rate for both, because the demand a use generates is about trips rather
#: than about what the guide charges to build the floor - and because nothing
#: upstream distinguishes a `C.2` warehouse-club from a `C.1` corner store.
STALLS_PER_1000_SQFT = 3.0

#: Stall demand is accumulated at millionths so both ratios stay exact against
#: the things they are charged on. A dwelling ratio would be happy at
#: hundredths, but the non-residential ratio is charged against an *area*
#: held in hundredths of a square metre - about a three-thousandth of a stall
#: apiece - and at hundredths that coefficient would round to nothing at all.
#:
#: The scale is also what keeps a part stall rounding **up**, which is how a
#: ratio is read everywhere one is written into a by-law: the constraint is
#: `SCALE x stalls >= demand`, so 13 dwellings at 0,5 owe 7 stalls, not 6.
STALL_DEMAND_SCALE = 1_000_000

#: Area one stall occupies, in square feet, including its share of the aisles
#: and ramps reaching it. Underground is the larger of the two: the same car
#: needs more structure around it below grade - columns on a grid the tower
#: above dictates rather than the parking layout, plus the ramp that article
#: 38 1° counts as part of the excluded area.
UNDERGROUND_STALL_AREA_SQFT = 400.0
ABOVE_GRADE_STALL_AREA_SQFT = 300.0

#: Dollars per stall, Montreal, from the Altus Group Canadian Cost Guide as
#: `urban_rag.estimator` publishes it: the midpoint of `parkade_ug`'s mtl
#: `[51 925, 68 675]` and `parkade_ag`'s mtl `[38 500, 57 750]`, both flagged
#: `perStall`. Underground is the dearer per stall *and* the larger per stall.
#: Above grade would dominate it outright were the FSR it burns not the more
#: expensive half of the bargain, which is what makes the choice a choice.
UNDERGROUND_STALL_COST_CAD = 60_300.0
ABOVE_GRADE_STALL_COST_CAD = 48_125.0

#: Months a capital cost is spread over to be comparable with a monthly rent.
#: Straight line, undiscounted, 25 years, and shared by the stalls and the
#: dwellings so the two are charged on the same footing. No longer the
#: objective's horizon - `InvestmentAssumptions` is that - but still what
#: `DevelopmentProgram.net_operating_income` reports the legacy monthly figure
#: at, so a table written before the discounting existed reads unchanged.
AMORTIZATION_MONTHS = 300

# -- what a dollar of income is worth to a developer -------------------------
#
# The four numbers that turn a rent roll into a verdict, and none of them is a
# norm or a survey: they are the stance an investor takes towards time, risk
# and running costs, stated here and configurable at every call site. The
# defaults describe a patient Montreal multifamily developer of the mid-2020s;
# a lender's committee would argue with each of them, which is the point of
# them being fields.

#: Annual rate a future dollar of NOI is discounted at. The opportunity cost
#: of the capital, unlevered - no construction loan is modelled, so this is
#: the whole of the time value in the objective.
DISCOUNT_RATE_PCT = 5.0

#: Years the completed building is held and its NOI collected before the sale.
HOLD_YEARS = 25

#: Cap rate the stabilised NOI is sold at when the hold ends - the reversion,
#: discounted like everything else. Montreal multifamily has traded between 4
#: and 5.5 in recent memory; `None` drops the sale and values the income
#: stream alone, which reads as a hold to worthlessness and is deliberately
#: not the default.
TERMINAL_CAP_RATE_PCT = 4.5

#: Share of gross income that never reaches the owner - taxes, insurance,
#: management, maintenance - for a building that is NEW, which the proposal is
#: by construction. The same conventional 0.35 `comparables.IncomeAssumptions`
#: states for the same reason, restated rather than imported so this module
#: keeps owing that one nothing.
OPERATING_EXPENSE_RATIO = 0.35

#: What a new dwelling leases for over the stock average CMHC surveys, in
#: percent. CMHC's grid averages a borough's standing stock - most of it old,
#: much of it rent-controlled in practice - and nobody underwrites a proforma
#: at that number; new construction in Villeray asks 25 to 40 percent over it.
#: Applied to the dwelling rents only: the commercial and industrial rates are
#: already quotes for space a tenant would sign today.
NEW_BUILD_RENT_PREMIUM_PCT = 30.0

#: Dollars per square foot of dwelling, Montreal, from the Altus Group
#: Canadian Cost Guide as `urban_rag.estimator` publishes it: the midpoint of
#: `condo_wood`'s mtl `[225, 290]` - "Wood Frame Condo (Up to 6 Storeys)",
#: which is `estimator.LOW_RISE_CONDO_TYPE_ID` and for the same reason. A
#: borough zoned two-to-six storeys builds wood frame, and the band a lot ends
#: up in is the solver's *answer* rather than its input, so this cannot be
#: picked off the storey count without branching on a decision variable.
#:
#: The guide prices four bands above it - `condo_12` at mtl `[275, 335]` is
#: the next - and a lot whose grid allows a tower is being under-costed here.
#: Pass `ConstructionCosts(residential_cost_per_sqft=...)` where the structure
#: is known; `estimator.CONDO_TYPE_IDS` is the list of bands to read it from.
RESIDENTIAL_COST_PER_SQFT_CAD = 257.5

#: Dollars per square foot of commercial and of industrial floor, Montreal,
#: rounded from the `montreal_nonresidential_costs` snapshot: a round average
#: across the guide's `cat="commercial"` rows - offices by storey band and
#: class, fitouts, retail, hotels - and across its `cat="industrial"` rows -
#: warehouse, distribution, urban storage. Averaged rather than picked, and
#: rounded rather than carried to the cent, because neither the grid nor
#: anything else upstream says *which* commerce a `C.2` column will be filled
#: with, and a spuriously precise rate would suggest it did. The snapshot is
#: where the rows behind these live - `estimator.NON_RESIDENTIAL_CATEGORIES`
#: is the list of `cat` values it keeps - and
#: `ConstructionCosts(commercial_cost_per_sqft=...)` is how to pass a
#: particular one where the tenant is known.
#:
#: Unlike `RESIDENTIAL_COST_PER_SQFT_CAD` these are charged against *gross*
#: floor area rather than against a schedule of leasable units, so the
#: corridors and cores the residential figure quietly leaves unpriced are paid
#: for here. The two sides of the ledger are consistent about it: the revenue
#: below is charged on the same gross area.
COMMERCIAL_COST_PER_SQFT_CAD = 300.0
INDUSTRIAL_COST_PER_SQFT_CAD = 200.0

#: Dollars of rent per square foot per **year**, gross. A stated program
#: assumption, not a published survey - there is no CMHC for retail, and the
#: guide this module's costs come from prices building and not leasing. The
#: unit is the one commercial leasing is quoted in, and `MONTHS_PER_YEAR` is
#: where it meets a monthly dwelling rent.
#:
#: Asking rent rather than effective: `COMMERCIAL_VACANCY_PCT` below is what
#: turns one into the other, the same way CMHC's published vacancy does for a
#: dwelling.
COMMERCIAL_REVENUE_PER_SQFT_CAD = 80.0
INDUSTRIAL_REVENUE_PER_SQFT_CAD = 30.0

#: Share of the floor that earns nothing, in percent, as published vacancy is
#: written. Stated rather than surveyed - there is no CMHC for retail, and the
#: cost guide upstream prices building and not leasing - which is the one way
#: these differ from the rates in `UnitEconomics.vacancy_rate_pct` beside them.
#: Read as a percentage for exactly that reason: a reader who has both objects
#: in front of them should not have to remember that one is a fraction.
#:
#: The same figure for both because it is one assumption, not two measurements.
#: A market where retail is soft and industrial is not is modelled by moving
#: them apart, which is what the two fields on `NonResidentialEconomics` are.
COMMERCIAL_VACANCY_PCT = 7.0
INDUSTRIAL_VACANCY_PCT = 7.0

# -- what a building costs to keep standing ---------------------------------
#
# An operating expense ratio is one number covering taxes, insurance,
# management and *maintenance*, and only the last of those depends on how old
# the building is. A 1910 triplex and a building finished this year do not
# spend the same share of their rent on the roof, the pointing, the risers and
# the wiring, and charging them the same ratio is the single assumption that
# most flatters standing stock against redeveloping it - which is exactly the
# comparison `urban_rag.hbu` exists to make.
#
# So the ratio is split in two. The base is what a *new* building costs to run
# and is the caller's `operating_expense_ratio`; the premium below is what age
# adds to it, and it is the only part that varies per lot.

#: Additional share of gross income lost to maintenance per year of building
#: age. Stated, not surveyed: Statistics Canada publishes operating expenses
#: for lessors of residential buildings by geography and industry, and CMHC
#: publishes rents and vacancies, but neither breaks maintenance out *by age of
#: building* - so there is no series to read this off, and inventing a
#: measured-looking one would be worse than saying which it is.
#:
#: The figure is set from the span it has to cover rather than from a study of
#: one year: repair and maintenance conventionally runs about a tenth of
#: effective gross income for newly built multi-residential and about a fifth
#: for pre-war walk-ups, so roughly ten points of gross separates the two ends
#: of Montreal's stock. At 0.0012 a year that ten points is reached at
#: `MAX_MAINTENANCE_PREMIUM` below, which a building hits at 83 years old.
MAINTENANCE_PREMIUM_PER_YEAR = 0.0012

#: The most age may add, whatever the year built. Maintenance on an old
#: building is bounded by the fact that an owner who stops spending on it stops
#: collecting the rent as well - past a point a building is renewed or it
#: leaves the stock, and neither of those is "the same building costing ever
#: more". Without a cap a 1850 stone house would be charged 20 points of gross
#: on a curve fitted to nothing that old.
MAX_MAINTENANCE_PREMIUM = 0.10

#: Age charged to a building whose `year_built` the roll does not state. Not
#: zero: an unstated year is far likelier to be old stock than new - a building
#: finished recently has a permit, a file and a year - and reading it as new
#: would hand the least-documented buildings the cheapest maintenance in the
#: borough. Roughly the median age of Montreal's assessed stock, and the run
#: reports how many lots took it so the size of the assumption is visible
#: rather than buried in an average.
ASSUMED_BUILDING_AGE_YEARS = 50.0

#: Underground levels the model may stack. Not a norm - nothing in the grid
#: bounds excavation - but a domain has to end somewhere, and a solution
#: sitting on this one is reported as `max_underground_levels` in `binding`
#: rather than passed off as the envelope's own answer.
MAX_UNDERGROUND_LEVELS = 6

#: Floor to floor, in metres, by what the storey is filled with. Three metres
#: is the dwelling storey a Montreal walk-up is built at; four is what a retail
#: or a warehouse plate needs once the plenum over it - ducts, sprinklers, the
#: clear height a tenant signs a lease for - is counted, and that difference is
#: the whole of the reason a commercial storey is a dearer thing to spend a
#: metric cap on than a residential one.
#:
#: Stated assumptions, like `AMORTIZATION_MONTHS` and unlike the norms read off
#: a grid: no by-law says a dwelling storey is three metres. They are named
#: here and passed as `StoreyHeights` precisely so a building known to be built
#: otherwise can say so.
RESIDENTIAL_STOREY_HEIGHT_M = 3.0
COMMERCIAL_STOREY_HEIGHT_M = 4.0
INDUSTRIAL_STOREY_HEIGHT_M = 4.0

#: An above-grade parking storey, which is the one storey type the three-and-
#: four rule above says nothing about. It is above grade, so it is inside the
#: height the by-law measures and cannot be zero; three metres is the figure a
#: self-parking deck is laid out at, and it is the residential number rather
#: than the commercial one because a garage carries no plenum.
ABOVE_GRADE_PARKING_STOREY_HEIGHT_M = 3.0

#: What an underground level adds to the building's height: nothing. Not an
#: approximation but the definition - *hauteur en metre* is measured from grade
#: up, so a level dug below it is outside the measurement the same way article
#: 38 1° puts it outside the *superficie de plancher*. It is a named constant
#: rather than a silence in the arithmetic so that the one place the model
#: could have charged for it is visibly a zero.
UNDERGROUND_LEVEL_HEIGHT_M = 0.0

#: A usage code that authorises dwellings: bare ``H`` as printed in zone
#: C01-001, or one of the numbered classes ``H.1``..``H.7``, optionally
#: lettered (``H.7A``). Footnote markers - CMHC-style ``C.3(9)`` - are stripped
#: before matching, since a footnote qualifies a usage rather than renaming it.
_RESIDENTIAL_USAGE = re.compile(r"^H(?:\.\d+[A-Za-z]?)?$")

#: The same shape for the other two families of usage the model can now fill an
#: envelope with: ``C``/``C.1``..``C.7`` for *Commerce* and ``I``/``I.1``..
#: ``I.4`` for *Industrie*. *Equipements collectifs* (``E``) is authorised by
#: plenty of columns and is deliberately absent - a school or a clinic is not
#: something a proforma rents by the square foot, and pricing one as if it were
#: would put a number on a decision nobody in this pipeline is making.
_COMMERCIAL_USAGE = re.compile(r"^C(?:\.\d+[A-Za-z]?)?$")
_INDUSTRIAL_USAGE = re.compile(r"^I(?:\.\d+[A-Za-z]?)?$")

#: And the family none of the three above matches: ``E``/``E.1``..``E.7`` for
#: *Equipements collectifs et institutionnels*. Written out beside them because
#: "not priced" and "not read" are two different things to say about a column,
#: and `is_equipment_usage` is what lets `urban_rag.hbu` say the first.
_EQUIPMENT_USAGE = re.compile(r"^E(?:\.\d+[A-Za-z]?)?$")
_FOOTNOTE = re.compile(r"\(\s*\d+\s*\)")

#: The most dwellings each *Habitation* class may hold, from by-law 01-283's
#: own definition of the classes. ``None`` is "no ceiling in the code".
#:
#: **The class is a norm, not a label.** A grid states *Nombre de logements
#: maximal* only where the class leaves room to choose - H.4 spans four to
#: eight dwellings, so the grid must say which - and leaves the row blank
#: where the code has already fixed the number (H.1, H.2 and H.3 are the
#: single, the duplex and the triplex) or left it open above (H.7). Reading
#: only the printed row therefore loses the ceiling entirely on the classes
#: that are *most* tightly bound: in Villeray-Saint-Michel-Parc-Extension it
#: is blank on all 498 columns headed H.1, H.2 or H.3, and the solver was
#: filling a duplex envelope with whatever the storeys and the site coverage
#: allowed - about thirteen dwellings on a 500 m2 lot where the code permits
#: two.
#:
#: The published grids corroborate the ranges rather than merely agreeing
#: with them: every printed value falls inside its class - H.4 prints 4, 5,
#: 6 and 8; H.5 prints 12; H.6 prints 36 - and the row is blank in exactly
#: the two cases where printing it would add nothing.
#:
#: Bare ``H`` carries no class and so no ceiling: it is the whole category,
#: and `class_max_dwellings` returns ``None`` for it rather than guessing at
#: the most restrictive member.
RESIDENTIAL_CLASS_MAX_DWELLINGS: Mapping[str, int | None] = {
    "H.1": 1,
    "H.2": 2,
    "H.3": 3,
    "H.4": 8,
    "H.5": 12,
    "H.6": 36,
    "H.7": None,
}

#: The class part of a usage code, ignoring the optional trailing letter that
#: distinguishes two grids' readings of one class (``H.7A``). Matched after
#: `_normalise_usage` has taken the footnote marker off.
_RESIDENTIAL_CLASS = re.compile(r"^(H\.\d+)[A-Za-z]?$")


def class_max_dwellings(usages: Iterable[str]) -> int | None:
    """The dwelling ceiling ``usages`` imply, or ``None`` where they imply none.

    The **most permissive** of the classes present, because a column headed
    ``H.2, H.4`` authorises both and a building may be either: taking the
    minimum would forbid the four-plex the ``H.4`` at its head allows. One
    unclassed code - bare ``H``, or a class this mapping does not carry -
    lifts the ceiling for the same reason, since it authorises a category
    whose own bound is not stated here.

    Non-residential codes are ignored rather than rejected, so a mixed
    column's ``C.4`` neither contributes a ceiling nor removes one.
    """
    caps: list[int] = []
    for usage in usages:
        normalised = _normalise_usage(usage)
        if not is_residential_usage(normalised):
            continue
        match = _RESIDENTIAL_CLASS.match(normalised)
        if match is None or match.group(1) not in RESIDENTIAL_CLASS_MAX_DWELLINGS:
            # Bare `H`, or a class the by-law numbers and this table does not.
            return None
        cap = RESIDENTIAL_CLASS_MAX_DWELLINGS[match.group(1)]
        if cap is None:
            return None
        caps.append(cap)
    return max(caps) if caps else None


class ProgramError(ValueError):
    """An envelope or a price list that cannot be turned into a model."""


class BuildingLevel(str, Enum):
    """The rows of the grid's *Niveaux de bâtiment autorisés* block.

    A grid marks the levels a usage may occupy with an `X` in that usage's
    column, and more than one row can be marked - a column carrying both
    *Rez-de-chaussée* and *Tous sauf le RDC* is authorised everywhere, spelled
    as two rules instead of one. `permitted_floors` is where they add up.
    """

    GROUND = "rez_de_chaussee"
    BELOW_GROUND = "inferieurs_au_rdc"
    SECOND = "immediatement_superieur_au_rdc"
    ALL_EXCEPT_GROUND = "tous_sauf_le_rdc"
    ALL = "tous_les_niveaux"

    def __str__(self) -> str:
        return self.value


def building_age_years(
    year_built,
    *,
    reference_year: float,
    assumed_age_years: float = ASSUMED_BUILDING_AGE_YEARS,
):
    """How old a building is in ``reference_year``, in years.

    Scalar or array; the array form is what `urban_rag.comparables` charges a
    whole borough's lots with in one pass. A year the roll does not state - or
    states as something that cannot be a year - becomes ``assumed_age_years``
    rather than a null, so the lot still gets an income; `ASSUMED_BUILDING_AGE_YEARS`
    says why that default is not zero.

    A negative age is clipped to zero. The roll carries a handful of units
    whose stated year is in the future (a building permitted but not finished),
    and a building cannot be less than new.
    """
    year = np.asarray(year_built, dtype="float64")
    age = float(reference_year) - year
    age = np.where(np.isnan(age), float(assumed_age_years), age)
    return np.clip(age, 0.0, None)


def maintenance_premium(
    age_years,
    *,
    per_year: float = MAINTENANCE_PREMIUM_PER_YEAR,
    cap: float = MAX_MAINTENANCE_PREMIUM,
):
    """What age adds to a building's operating expense ratio.

    A share of gross income, on the same scale as the base ratio it is added
    to: 0.04 is four more points of gross going to maintenance than a new
    building spends. Linear in age to `cap` and flat after it - see the two
    constants for why each is the number it is.

    Scalar or array, and never negative: this is a *premium*, so a building
    newer than the reference year costs the base ratio and no less. A model in
    which a new building were cheaper than new would be one where the base
    ratio meant something other than what it says.
    """
    if per_year < 0:
        raise ProgramError(
            f"maintenance premium per year is a share of gross income added "
            f"with age and cannot be negative, got {per_year!r}"
        )
    if cap < 0:
        raise ProgramError(
            f"maximum maintenance premium is a share of gross income and "
            f"cannot be negative, got {cap!r}"
        )
    age = np.clip(np.asarray(age_years, dtype="float64"), 0.0, None)
    return np.minimum(age * float(per_year), float(cap))


def effective_operating_expense_ratio(
    age_years,
    *,
    base_ratio: float,
    per_year: float = MAINTENANCE_PREMIUM_PER_YEAR,
    cap: float = MAX_MAINTENANCE_PREMIUM,
):
    """The base ratio plus what age adds, bounded below 1.

    The one place the two halves are put together, so the solver and the
    comparables asset cannot end up adding them differently. Bounded strictly
    below 1.0 because a ratio of 1 is a building whose gross income all leaves
    again - an NOI of exactly zero - and anything past it is negative income
    from a positive rent, which is not what a high maintenance bill means.

    A base at or above 1 is the caller's error and is refused rather than
    clipped: `IncomeAssumptions` already rejects it, and silently repairing one
    here would let a typo produce a plausible-looking cap rate.
    """
    if not 0.0 <= base_ratio < 1.0:
        raise ProgramError(
            "base operating expense ratio is a share of gross income and must "
            f"be in [0, 1), got {base_ratio!r}"
        )
    premium = maintenance_premium(age_years, per_year=per_year, cap=cap)
    return np.minimum(float(base_ratio) + premium, _MAX_EXPENSE_RATIO)


#: The most any effective ratio may reach. Strictly below 1 so an NOI stays
#: positive where a gross income is - see `effective_operating_expense_ratio`.
_MAX_EXPENSE_RATIO = 0.99


def is_residential_usage(usage: str) -> bool:
    """Whether ``usage`` is one of the Habitation classes.

    Matches the whole code, so ``C.4`` and ``I.2`` are rejected and the ``H``
    inside a word is not mistaken for one.
    """
    return bool(_RESIDENTIAL_USAGE.match(_normalise_usage(usage)))


def is_commercial_usage(usage: str) -> bool:
    """Whether ``usage`` is one of the Commerce classes.

    Anchored the same way `is_residential_usage` is, which is what keeps
    ``CH.1`` and ``Commerce`` out: the code is the whole string or it is not
    the code.
    """
    return bool(_COMMERCIAL_USAGE.match(_normalise_usage(usage)))


def is_industrial_usage(usage: str) -> bool:
    """Whether ``usage`` is one of the Industrie classes."""
    return bool(_INDUSTRIAL_USAGE.match(_normalise_usage(usage)))


def is_equipment_usage(usage: str) -> bool:
    """Whether ``usage`` is one of the *Équipements collectifs* classes.

    The one family this module deliberately does not price, and the only one
    whose matcher exists to *recognise* rather than to authorise: nothing here
    reads it, and `urban_rag.hbu` calls it to tell a park from a grid whose
    usage row it simply could not read. Anchored like the three above, so the
    ``E`` of ``E.4(1)`` is a code and the ``E`` of a word is not.
    """
    return bool(_EQUIPMENT_USAGE.match(_normalise_usage(usage)))


def _normalise_usage(usage: str) -> str:
    """A usage code with its footnote marker and its whitespace taken off."""
    return _FOOTNOTE.sub("", usage).strip()


def permitted_floors(levels: Iterable[BuildingLevel], total_floors: int) -> int:
    """How many storeys a usage may occupy, given the rows marked for it.

    Each marked row contributes the storeys it names, and the contributions add
    up because the grid states them as separate authorisations:

    ===============================  ==========================
    Row                              Storeys
    ===============================  ==========================
    Tous les niveaux                 ``total_floors``
    Tous sauf le RDC                 ``total_floors - 1``
    Rez-de-chaussée (RDC)            1
    Inférieurs au RDC                1
    Immédiatement supérieur au RDC   1
    ===============================  ==========================

    The sum is capped at ``total_floors``: *Rez-de-chaussée* alongside *Tous
    sauf le RDC* covers the building exactly once, not the ground floor twice.
    A cellar counts as a storey here because the model's floors are identical
    by assumption - it is buildable area either way, and the grid's own
    *En étage* maximum is what bounds the total.
    """
    if total_floors < 0:
        raise ProgramError(f"total_floors must not be negative, got {total_floors}")
    contributions = {
        BuildingLevel.ALL: total_floors,
        BuildingLevel.ALL_EXCEPT_GROUND: max(total_floors - 1, 0),
        BuildingLevel.GROUND: 1,
        BuildingLevel.BELOW_GROUND: 1,
        BuildingLevel.SECOND: 1,
    }
    allowed = sum(contributions[level] for level in set(levels))
    return min(allowed, total_floors)


@dataclass(frozen=True)
class ZoneColumn:
    """One column of a *grille des usages et des normes*.

    A grid is read down its columns: each is a separate set of rules for the
    usages listed at its head, and a zone with three of them is three
    envelopes, not one. Every field here is optional except the usages and the
    storey maximum, because a grid prints ``-`` wherever a norm does not apply
    and an absent norm is not a norm of zero - ``Largeur du terrain min (m) -``
    means *any* width qualifies, and reading it as 0 happens to be equivalent
    while reading ``Densité min/max -`` as 0/0 forbids building at all.
    """

    #: The codes at the head of the column, e.g. ``("H.2",)`` or ``("H",)``.
    usages: tuple[str, ...]
    #: *En étage min/max* - the building's storey ceiling, before the level
    #: rows narrow it to what this usage may occupy.
    floors_max: int
    #: The rows of *Niveaux de bâtiment autorisés* marked for this column.
    levels: frozenset[BuildingLevel] = frozenset()
    #: *En étage min/max*, the minimum. A grid that prints ``2/6`` requires two.
    floors_min: int = 0
    #: *Hauteur en mètre min/max* - the same ceiling measured the other way.
    #: ``None`` where the grid prints ``-``, and independent of *En étage*
    #: rather than derived from it: a grid states both, they can disagree, and
    #: whichever is reached first is the one that stops the building. See
    #: `StoreyHeights` for what a storey costs against this one.
    height_min_m: float | None = None
    height_max_m: float | None = None
    #: *Largeur du terrain min (m)*. ``None`` where the grid prints ``-``.
    min_lot_width_m: float | None = None
    #: *Nombre de logements maximal*. ``None`` where the grid leaves it blank.
    max_dwellings: int | None = None
    #: *Densité min/max* - gross floor area over lot area.
    density_min: float | None = None
    density_max: float | None = None
    #: *Taux d'implantation au sol min/max (%)* - footprint over lot area.
    site_coverage_min_pct: float | None = None
    site_coverage_max_pct: float | None = None
    #: The zone this column belongs to, carried for reporting only.
    zone: str | None = None

    def __post_init__(self) -> None:
        if self.floors_max < 0:
            raise ProgramError(f"{self!r}: floors_max must not be negative")
        if self.floors_min < 0:
            raise ProgramError(f"{self!r}: floors_min must not be negative")
        for name in ("density_min", "density_max", "height_min_m", "height_max_m"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ProgramError(f"{self!r}: {name} must not be negative")
        for name in ("site_coverage_min_pct", "site_coverage_max_pct"):
            value = getattr(self, name)
            if value is not None and not 0 <= value <= 100:
                raise ProgramError(f"{self!r}: {name} must be a percentage")

    @property
    def permits_residential(self) -> bool:
        return any(is_residential_usage(usage) for usage in self.usages)

    @property
    def permits_commercial(self) -> bool:
        return any(is_commercial_usage(usage) for usage in self.usages)

    @property
    def permits_industrial(self) -> bool:
        return any(is_industrial_usage(usage) for usage in self.usages)

    @property
    def permitted_floors_count(self) -> int:
        """Storeys the level rows allow this column's usages to occupy.

        One number for the whole column, and that is the grid's own doing: the
        *Niveaux de batiment autorises* block is marked per column, not per
        usage code, so a column headed ``H.2, C.2`` says where "H.2 and C.2"
        may go and never which of the two belongs on the ground floor. The
        model therefore lets any of the column's usages occupy any of the
        storeys it allows. A grid that means "commerce at the RDC, housing
        above" prints that as two columns, and two columns are two envelopes.
        """
        return permitted_floors(self.levels, self.floors_max)

    @property
    def residential_floors(self) -> int:
        """Storeys this column's usage may occupy."""
        return self.permitted_floors_count

    @property
    def class_max_dwellings(self) -> int | None:
        """The dwelling ceiling this column's *usage codes* imply.

        `RESIDENTIAL_CLASS_MAX_DWELLINGS` is where the numbers are and why
        they are a norm rather than a naming convention.
        """
        return class_max_dwellings(self.usages)

    @property
    def effective_max_dwellings(self) -> int | None:
        """The dwelling ceiling that actually binds: printed **and** class.

        The grid's *Nombre de logements maximal* and the ceiling the class
        carries are two statements of one norm, and a building answers to
        both - so this is the smaller of the two, or whichever exists alone,
        or ``None`` where neither does. `max_dwellings` stays exactly what
        the grid printed, because a table that reports the norms should
        report the one on the page.
        """
        printed = self.max_dwellings
        implied = self.class_max_dwellings
        if printed is None:
            return implied
        if implied is None:
            return printed
        return min(printed, implied)


@dataclass(frozen=True)
class Lot:
    """The parcel, as the two upstream measurements leave it.

    ``frontage_m`` is what `lot_frontage` computes - the longest street edge,
    `frontage_rank = 1` - and is the width the grid's *Largeur du terrain min*
    is tested against. They are not the same measurement in principle (a lot's
    width and its street frontage differ on a wedge-shaped parcel) and this
    treats them as one, which is the approximation the frontage asset exists to
    support.

    ``buildable_area_m2`` is what `lot_buildable_setbacks` computes: the area
    left once the grid's four margins are taken off the parcel, for the column
    being solved. It is the *second* cap on a footprint and it is optional
    because it has to be - it is computed from a zoning column, so it belongs
    to one candidate rather than to the lot, and a caller solving a column
    whose grid states no margins has nothing to pass. ``None`` leaves the
    footprint capped on *Taux d'implantation* alone, which is what every
    caller did before that asset existed.

    Passing it is what makes a shallow lot solve differently from a deep one of
    the same area - which, on the coverage cap alone, it does not.
    """

    area_m2: float
    frontage_m: float
    lot_number: str | None = None
    buildable_area_m2: float | None = None

    def __post_init__(self) -> None:
        if self.area_m2 <= 0:
            raise ProgramError(f"lot area must be positive, got {self.area_m2}")
        if self.frontage_m < 0:
            raise ProgramError(f"frontage must not be negative, got {self.frontage_m}")
        if self.buildable_area_m2 is not None and self.buildable_area_m2 < 0:
            # 0 is allowed and is a real answer - a parcel narrower than twice
            # its side margin has nowhere to put a building, and the solver
            # should return an empty program for it rather than refuse the lot.
            raise ProgramError(
                f"buildable area must not be negative, got "
                f"{self.buildable_area_m2}"
            )


@dataclass(frozen=True)
class StoreyHeights:
    """Floor to floor, in metres, by what the storey is filled with.

    The metric half of the storey cap. *En étage* counts plates and *Hauteur
    en mètre* measures them, so a grid printing both states two ceilings on the
    same stack, and the shorter one is the answer: six residential storeys need
    18 m and four commercial ones need 16, so a column printing ``6`` storeys
    and ``15`` metres allows five of housing and three of commerce, whatever
    the storey row says on its own.

    Which is what makes this more than a unit conversion. The heights are not
    equal - four metres for commerce and industry against three for a dwelling
    - so under a metric cap the storey types stop being interchangeable in a
    way *En étage* alone never made them: a retail plate now costs a third more
    of the ceiling than the housing it displaces, on top of the parking it
    already owes. A tight *Hauteur* is where commerce stops outbidding housing
    for a storey it would otherwise win.

    An underground level is not in here, and its absence is the rule rather
    than an omission - `UNDERGROUND_LEVEL_HEIGHT_M` is the zero written down.
    Height is measured from grade up, so digging is free of this cap exactly as
    article 38 1° makes it free of *Densité*, and the two exclusions point the
    same way: underground is where the model puts what an envelope has no room
    for above.
    """

    #: Metres per residential storey.
    residential_m: float = RESIDENTIAL_STOREY_HEIGHT_M
    #: Metres per commercial and per industrial storey. Taller than a dwelling
    #: storey, and the reason a metric cap ranks the usages differently from a
    #: storey cap.
    commercial_m: float = COMMERCIAL_STOREY_HEIGHT_M
    industrial_m: float = INDUSTRIAL_STOREY_HEIGHT_M
    #: Metres per above-grade parking storey. Above grade, therefore measured;
    #: see `ABOVE_GRADE_PARKING_STOREY_HEIGHT_M` for why it is the residential
    #: figure and not the commercial one.
    above_grade_parking_m: float = ABOVE_GRADE_PARKING_STOREY_HEIGHT_M

    def __post_init__(self) -> None:
        for name in (
            "residential_m",
            "commercial_m",
            "industrial_m",
            "above_grade_parking_m",
        ):
            value = getattr(self, name)
            if value <= 0:
                # Not merely nonsensical: a storey of no height would be one
                # the metric cap never charges for, and the solver would stack
                # them without limit under any *Hauteur* at all. The way to
                # take the cap off is a column that prints no maximum.
                raise ProgramError(f"{name} must be positive, got {value}")

    @property
    def residential_cm(self) -> int:
        """The same, in the hundredths of a metre the model counts in."""
        return _scale_height(self.residential_m)

    @property
    def commercial_cm(self) -> int:
        return _scale_height(self.commercial_m)

    @property
    def industrial_cm(self) -> int:
        return _scale_height(self.industrial_m)

    @property
    def above_grade_parking_cm(self) -> int:
        return _scale_height(self.above_grade_parking_m)

    @property
    def shortest_cm(self) -> int:
        """The cheapest storey to spend a metric cap on.

        What bounds the *building's* storeys, as against any one usage's:
        below this many centimetres no storey of any kind can be added, so
        `height_max / shortest` is the most plates a height cap can hold.
        """
        return min(
            self.residential_cm,
            self.commercial_cm,
            self.industrial_cm,
            self.above_grade_parking_cm,
        )

    def height_m(
        self,
        *,
        residential: int = 0,
        above_grade_parking: int = 0,
        commercial: int = 0,
        industrial: int = 0,
    ) -> float:
        """What a storey split stands, in metres. The underground is not in it."""
        return _unscale_height(
            self.residential_cm * residential
            + self.above_grade_parking_cm * above_grade_parking
            + self.commercial_cm * commercial
            + self.industrial_cm * industrial
        )


#: Three metres a dwelling storey, four a commercial or industrial one, three
#: an above-grade deck, nothing below grade.
DEFAULT_STOREY_HEIGHTS = StoreyHeights()


@dataclass(frozen=True)
class ParkingRules:
    """What the dwellings owe in stalls, and what a stall costs to provide.

    Two rows of numbers that look alike and are not. The *areas* are what a
    stall takes out of a floor plate, and the underground one is larger; the
    *costs* are what a stall takes out of the objective, and the underground
    one is also larger. Underground is worse on both counts and is still
    routinely the right answer, because the third column - the one that has no
    field here because it is the by-law's rather than the builder's - is that
    an underground stall consumes no *superficie de plancher* and no storey.
    That exclusion is article 38 1° of 01-283 and it is applied in
    `solve_program`, not here: this dataclass is the price list, and the
    by-law is the reason the price list is worth consulting.

    Defaults are the module constants, so `ParkingRules()` is the program the
    constants describe. `NO_PARKING` is the same object with the ratio at
    zero, which drops every parking variable to a domain of ``{0}`` and leaves
    the pure-envelope question the module answered before parking existed.
    """

    #: Stalls per dwelling. Rounded **up** in aggregate, never per dwelling.
    stalls_per_dwelling: float = STALLS_PER_DWELLING
    #: Stalls per thousand square feet of commercial or industrial floor.
    #: Rounded up in the same aggregate, and *with* the dwellings rather than
    #: beside them: a building owes one number of stalls, so a half stall owed
    #: by the retail and a half owed by the housing are one stall between them
    #: and not two.
    stalls_per_1000_sqft: float = STALLS_PER_1000_SQFT
    #: Square feet a stall occupies, aisles and ramps included.
    underground_area_sqft: float = UNDERGROUND_STALL_AREA_SQFT
    above_grade_area_sqft: float = ABOVE_GRADE_STALL_AREA_SQFT
    #: Dollars of capital per stall.
    underground_cost_cad: float = UNDERGROUND_STALL_COST_CAD
    above_grade_cost_cad: float = ABOVE_GRADE_STALL_COST_CAD
    #: Months that capital is spread over to be subtractable from a rent.
    amortization_months: int = AMORTIZATION_MONTHS
    #: The deepest the model may dig. A modelling bound, not a norm.
    max_underground_levels: int = MAX_UNDERGROUND_LEVELS

    def __post_init__(self) -> None:
        if self.stalls_per_dwelling < 0:
            raise ProgramError(
                f"stalls_per_dwelling must not be negative, "
                f"got {self.stalls_per_dwelling}"
            )
        if self.stalls_per_1000_sqft < 0:
            raise ProgramError(
                f"stalls_per_1000_sqft must not be negative, "
                f"got {self.stalls_per_1000_sqft}"
            )
        for name in (
            "underground_area_sqft",
            "above_grade_area_sqft",
            "underground_cost_cad",
            "above_grade_cost_cad",
        ):
            value = getattr(self, name)
            if value < 0:
                raise ProgramError(f"{name} must not be negative, got {value}")
        if self.amortization_months <= 0:
            raise ProgramError(
                f"amortization_months must be positive, got {self.amortization_months}"
            )
        if self.max_underground_levels < 0:
            raise ProgramError(
                f"max_underground_levels must not be negative, "
                f"got {self.max_underground_levels}"
            )

    @property
    def required(self) -> bool:
        """Whether anything the building might hold owes a stall at all.

        Either ratio is enough. A column that authorises only commerce owes
        stalls without a dwelling anywhere in the program, and reading this off
        `stalls_per_dwelling` alone would let it park them nowhere.
        """
        return self.stalls_per_dwelling > 0 or self.stalls_per_1000_sqft > 0

    def monthly_cost(self, *, underground: int, above_grade: int) -> float:
        """What this many stalls costs a month, at the stated horizon."""
        capital = self.capital_cost(underground=underground, above_grade=above_grade)
        return capital / self.amortization_months

    def capital_cost(self, *, underground: int, above_grade: int) -> float:
        """What this many stalls costs to build, in dollars."""
        return (
            underground * self.underground_cost_cad
            + above_grade * self.above_grade_cost_cad
        )


#: The program the module constants describe: half a stall a dwelling, priced
#: and sized as the cost guide and the by-law leave them.
DEFAULT_PARKING = ParkingRules()

#: No stalls owed by anything, and none priced - the envelope question on its
#: own. Both ratios, so a column that authorises commerce is asked the same
#: question a bare Habitation column is.
NO_PARKING = ParkingRules(stalls_per_dwelling=0.0, stalls_per_1000_sqft=0.0)


@dataclass(frozen=True)
class ConstructionCosts:
    """What the dwellings cost to build, per square foot of dwelling.

    Charged against the *unit* schedule rather than against the gross floor
    area, which is what `UNIT_AREAS_SQFT` is doing on this side of the ledger:
    the corridors, lobbies, stairs and shafts that the difference between the
    two would pay for are not priced here at all. That understates the build
    on any real plan and is the assumption to revisit before this number is
    taken to a lender.

    The parking is **not** in here. A stall is priced per stall by
    `ParkingRules`, the way the cost guide prices it, and adding a per-square-
    foot charge on top would bill the same concrete twice.
    """

    #: Dollars per square foot of dwelling.
    residential_cost_per_sqft: float = RESIDENTIAL_COST_PER_SQFT_CAD
    #: Dollars per square foot of commercial and of industrial floor. Charged
    #: against the *gross* area of those storeys rather than against a unit
    #: schedule, which is the difference noted at the constants above: there is
    #: no `UNIT_AREAS_SQFT` for retail, so there is no leasable-versus-gross
    #: gap to fall through, and the revenue is charged on the same area.
    commercial_cost_per_sqft: float = COMMERCIAL_COST_PER_SQFT_CAD
    industrial_cost_per_sqft: float = INDUSTRIAL_COST_PER_SQFT_CAD
    #: Months that capital is spread over to be subtractable from a rent.
    amortization_months: int = AMORTIZATION_MONTHS

    def __post_init__(self) -> None:
        for name in (
            "residential_cost_per_sqft",
            "commercial_cost_per_sqft",
            "industrial_cost_per_sqft",
        ):
            value = getattr(self, name)
            if value < 0:
                raise ProgramError(f"{name} must not be negative, got {value}")
        if self.amortization_months <= 0:
            raise ProgramError(
                f"amortization_months must be positive, got {self.amortization_months}"
            )

    def capital_cost(self, area_sqft: float) -> float:
        """What this many square feet of dwelling costs to build, in dollars."""
        return area_sqft * self.residential_cost_per_sqft

    def monthly_cost(self, area_sqft: float) -> float:
        """The same, charged against a month of rent at the stated horizon."""
        return self.capital_cost(area_sqft) / self.amortization_months

    def commercial_capital_cost(self, area_sqft: float) -> float:
        """What this many square feet of commercial floor costs, in dollars."""
        return area_sqft * self.commercial_cost_per_sqft

    def commercial_monthly_cost(self, area_sqft: float) -> float:
        """The same, at the stated horizon."""
        return self.commercial_capital_cost(area_sqft) / self.amortization_months

    def industrial_capital_cost(self, area_sqft: float) -> float:
        """What this many square feet of industrial floor costs, in dollars."""
        return area_sqft * self.industrial_cost_per_sqft

    def industrial_monthly_cost(self, area_sqft: float) -> float:
        """The same, at the stated horizon."""
        return self.industrial_capital_cost(area_sqft) / self.amortization_months


#: The build the module constants describe.
DEFAULT_CONSTRUCTION = ConstructionCosts()

#: Space that costs nothing to put up - the revenue side on its own. All three
#: rates, so a test that pins a rent is not quietly still paying for a retail
#: floor.
NO_CONSTRUCTION_COST = ConstructionCosts(
    residential_cost_per_sqft=0.0,
    commercial_cost_per_sqft=0.0,
    industrial_cost_per_sqft=0.0,
)


@dataclass(frozen=True)
class UnitEconomics:
    """What a dwelling of each class earns, per CMHC's published grids.

    Both maps are keyed by CMHC bedroom class and both are read exactly as
    `average_rents` and `vacancy_rates` write them: rent in dollars a month,
    vacancy **in percent**.

    A class CMHC suppressed - `status` of `suppressed` or `no_units`, written
    as null - simply has no key here. That is not an error and is not a zero:
    it means the survey will not price that class in this neighbourhood, so the
    solver is not allowed to build it and says so in
    `DevelopmentProgram.unpriced_types`. Treating a suppressed cell as $0 rent
    would silently rule the class out; treating it as free would let the model
    build a tower of them.
    """

    average_rent_cad: Mapping[str, float]
    vacancy_rate_pct: Mapping[str, float] = field(default_factory=dict)

    def monthly_revenue(self, unit_type: str) -> float | None:
        """What one dwelling of ``unit_type`` collects in a month, or ``None``.

        ``rent x (1 - vacancy)``: effective gross income for a single
        dwelling, in the dollars CMHC published it in. Not scaled by area -
        the rent already knows how big the dwelling is, because CMHC surveys
        by bedroom class - and scaling it again is what made the old objective
        a proxy rather than a number. Size earns its keep on the cost side, in
        `ConstructionCosts`.

        This is revenue, not income: `solve_program` is where the build cost
        is netted off it, and the module docstring lists what is netted off
        neither.
        """
        rent = self.average_rent_cad.get(unit_type)
        if rent is None:
            return None
        vacancy_pct = self.vacancy_rate_pct.get(unit_type)
        occupancy = 1.0 - (vacancy_pct or 0.0) / 100.0
        return rent * occupancy


@dataclass(frozen=True)
class NonResidentialEconomics:
    """What a square foot of commerce or industry earns, per year.

    The counterpart of `UnitEconomics` for the space that is not dwellings,
    and deliberately a different shape rather than a fifth key in that map.
    Two things differ and either would be a silent error if the two were
    merged: the unit is a square foot and not a dwelling, and the period is a
    year and not a month. The third element is the same on both sides -
    `rent x (1 - vacancy)` - and it is spelled the same way here, in percent
    as published vacancy is written, so a reader holding both objects does not
    have to remember which one is a fraction.

    Where the vacancy comes from is what still differs: CMHC surveys the
    dwelling figure and this one is stated. `COMMERCIAL_VACANCY_PCT` says what
    the module assumes and why, and a caller with a broker's number for the
    submarket should pass it.

    Zero on either rate is how a caller says "not this one" without reaching
    for the zoning: `NO_NON_RESIDENTIAL` is that, for both.
    """

    #: Dollars per square foot per year, asking - the vacancy below is what
    #: makes it effective.
    commercial_per_sqft_year: float = COMMERCIAL_REVENUE_PER_SQFT_CAD
    industrial_per_sqft_year: float = INDUSTRIAL_REVENUE_PER_SQFT_CAD
    #: Share of the floor earning nothing, **in percent**.
    commercial_vacancy_pct: float = COMMERCIAL_VACANCY_PCT
    industrial_vacancy_pct: float = INDUSTRIAL_VACANCY_PCT
    #: Months in the year the rates above are quoted over. A field rather than
    #: a bare 12 so the conversion is visible at the call site of a proforma
    #: that quotes monthly rates instead.
    months_per_year: int = MONTHS_PER_YEAR

    def __post_init__(self) -> None:
        for name in ("commercial_per_sqft_year", "industrial_per_sqft_year"):
            value = getattr(self, name)
            if value < 0:
                raise ProgramError(f"{name} must not be negative, got {value}")
        for name in ("commercial_vacancy_pct", "industrial_vacancy_pct"):
            value = getattr(self, name)
            if not 0 <= value <= 100:
                raise ProgramError(f"{name} must be a percentage, got {value}")
        if self.months_per_year <= 0:
            raise ProgramError(
                f"months_per_year must be positive, got {self.months_per_year}"
            )

    @property
    def commercial_per_sqft_month(self) -> float:
        """Asking rent a month. Not what the floor earns - see below."""
        return self.commercial_per_sqft_year / self.months_per_year

    @property
    def industrial_per_sqft_month(self) -> float:
        """Asking rent a month. Not what the floor earns - see below."""
        return self.industrial_per_sqft_year / self.months_per_year

    @property
    def commercial_effective_per_sqft_month(self) -> float:
        """`rent x (1 - vacancy)` for a square foot of commerce, a month.

        The figure the objective is built on, and the one to compare against a
        dwelling's `UnitEconomics.monthly_revenue`.
        """
        return self.commercial_per_sqft_month * (
            1.0 - self.commercial_vacancy_pct / 100.0
        )

    @property
    def industrial_effective_per_sqft_month(self) -> float:
        """`rent x (1 - vacancy)` for a square foot of industry, a month."""
        return self.industrial_per_sqft_month * (
            1.0 - self.industrial_vacancy_pct / 100.0
        )

    def commercial_monthly_revenue(self, area_sqft: float) -> float:
        """What this much commercial floor collects in a month, vacancy off."""
        return area_sqft * self.commercial_effective_per_sqft_month

    def industrial_monthly_revenue(self, area_sqft: float) -> float:
        """What this much industrial floor collects in a month, vacancy off."""
        return area_sqft * self.industrial_effective_per_sqft_month


#: The rents the module constants describe.
DEFAULT_NON_RESIDENTIAL = NonResidentialEconomics()

#: Space that earns nothing, which is how the all-residential question is asked
#: of a column that happens to authorise commerce as well. Not the same as a
#: column that forbids it: the variables still exist and the solver still
#: declines to build, because at zero rent every square foot of it is a loss.
NO_NON_RESIDENTIAL = NonResidentialEconomics(
    commercial_per_sqft_year=0.0, industrial_per_sqft_year=0.0
)


@dataclass(frozen=True)
class InvestmentAssumptions:
    """What a stream of rent is worth to the developer being modelled.

    The objective's price list, the way `ParkingRules` is the parking's: four
    stated assumptions that turn a monthly effective gross rent into a present
    value, and one that says what rent a *new* building actually leases at.
    None is a norm and none is surveyed, so every one is a field.

    The arithmetic is a plain unlevered proforma. A dollar of monthly gross
    becomes ``12 x (1 - operating_expense_ratio)`` dollars of annual
    stabilised NOI; that NOI is collected for `hold_years` and discounted at
    `discount_rate_pct`, and the building is sold at the end at
    `terminal_cap_rate_pct` - the reversion, discounted like the rents before
    it. `pv_per_monthly_gross` is the whole pipeline as one multiplier, which
    is what lets the objective stay linear: value in, capital out, per
    dwelling, per square foot and per stall.
    """

    #: Annual discount rate, in percent.
    discount_rate_pct: float = DISCOUNT_RATE_PCT
    #: Years of NOI collected before the sale.
    hold_years: int = HOLD_YEARS
    #: Cap rate of the sale that ends the hold, in percent. ``None`` values
    #: the income stream alone - a hold to worthlessness, for a caller who
    #: wants the conservative floor rather than a market answer.
    terminal_cap_rate_pct: float | None = TERMINAL_CAP_RATE_PCT
    #: Share of gross income spent running the building, for a building that
    #: is new - which the proposal is by construction.
    operating_expense_ratio: float = OPERATING_EXPENSE_RATIO
    #: What a new dwelling leases for over CMHC's stock average, in percent.
    #: Dwellings only; the non-residential rates are already market quotes.
    new_build_rent_premium_pct: float = NEW_BUILD_RENT_PREMIUM_PCT

    def __post_init__(self) -> None:
        if self.discount_rate_pct < 0:
            raise ProgramError(
                f"discount_rate_pct must not be negative, "
                f"got {self.discount_rate_pct}"
            )
        if self.hold_years <= 0:
            raise ProgramError(f"hold_years must be positive, got {self.hold_years}")
        if self.terminal_cap_rate_pct is not None and self.terminal_cap_rate_pct <= 0:
            # A cap rate of zero prices the sale at infinity; the way to have
            # no sale is None, which says so rather than dividing by it.
            raise ProgramError(
                f"terminal_cap_rate_pct must be positive or None, "
                f"got {self.terminal_cap_rate_pct}"
            )
        if not 0.0 <= self.operating_expense_ratio < 1.0:
            raise ProgramError(
                "operating_expense_ratio is a share of gross income and must "
                f"be in [0, 1), got {self.operating_expense_ratio!r}"
            )
        if self.new_build_rent_premium_pct < 0:
            raise ProgramError(
                f"new_build_rent_premium_pct must not be negative, "
                f"got {self.new_build_rent_premium_pct}"
            )

    @property
    def rent_premium_factor(self) -> float:
        """What a surveyed dwelling rent is multiplied by on the proforma."""
        return 1.0 + self.new_build_rent_premium_pct / 100.0

    @property
    def annual_pv_factor(self) -> float:
        """Present value of one dollar a year of stabilised NOI.

        The annuity over the hold plus the discounted reversion: at the
        defaults - 5% over 25 years, sold at a 4.5 cap - a dollar a year is
        worth $20.66 today, against the $25.00 the old undiscounted
        amortisation implicitly paid for it.
        """
        rate = self.discount_rate_pct / 100.0
        years = self.hold_years
        annuity = (
            float(years) if rate == 0.0 else (1.0 - (1.0 + rate) ** -years) / rate
        )
        reversion = 0.0
        if self.terminal_cap_rate_pct is not None:
            reversion = (100.0 / self.terminal_cap_rate_pct) / (1.0 + rate) ** years
        return annuity + reversion

    @property
    def pv_per_monthly_gross(self) -> float:
        """Present value of one dollar a month of effective gross rent.

        The whole proforma as one multiplier: twelve months, the expense
        ratio off, then `annual_pv_factor`. The objective is this times the
        gross, less the capital - so a program's `present_value_cad` is
        exactly its monthly gross times this number.
        """
        return (
            MONTHS_PER_YEAR
            * (1.0 - self.operating_expense_ratio)
            * self.annual_pv_factor
        )


#: The proforma the module constants describe.
DEFAULT_INVESTMENT = InvestmentAssumptions()

#: The objective the module maximised before it discounted: zero discount,
#: zero expenses, no reversion, no rent premium, held for the amortisation's
#: own 25 years. Under these prices the objective is exactly
#: `AMORTIZATION_MONTHS` times the old monthly NOI, so the argmax - the mix,
#: the storeys, where the stalls go - is what it always was. The tests that
#: pin cap arithmetic pass this so they stay about the caps rather than about
#: the price of money.
UNDISCOUNTED_INVESTMENT = InvestmentAssumptions(
    discount_rate_pct=0.0,
    hold_years=AMORTIZATION_MONTHS // MONTHS_PER_YEAR,
    terminal_cap_rate_pct=None,
    operating_expense_ratio=0.0,
    new_build_rent_premium_pct=0.0,
)


@dataclass(frozen=True)
class DevelopmentProgram:
    """The mix the solver settled on, and the envelope it fills."""

    #: Dwellings by CMHC bedroom class. Classes the solver declined to build
    #: are absent rather than zero.
    units: Mapping[str, int]
    #: Storeys **above grade**, residential and parking together. This is the
    #: number *En étage* is tested against; the underground levels are not
    #: storeys and are counted separately.
    floors: int
    footprint_m2: float
    #: The *superficie de plancher* the density index is computed on:
    #: `footprint x floors`, above grade only. The underground levels are
    #: excluded by article 38 1° and are in `underground_area_m2` instead.
    gross_floor_area_m2: float
    #: Floor area the dwellings actually occupy; at most the gross above.
    unit_area_m2: float
    #: The legacy monthly figure: revenue over the mix less the capital
    #: amortised at `AMORTIZATION_MONTHS`. No longer the objective - that is
    #: `npv_cad` - but computed from the chosen program and reported so every
    #: table that reads a monthly NOI keeps meaning what it meant.
    net_operating_income: float
    status: str
    #: The building's height above grade, in metres: the storey split below
    #: priced at `StoreyHeights`. This is the number *Hauteur en mètre* is
    #: tested against, and the underground levels are not in it - they are not
    #: measured from grade up, the same way they are not floor area.
    height_m: float = 0.0
    #: The storey split behind `floors`. Residential floors carry the mix;
    #: parking floors carry stalls and nothing else; the other two carry the
    #: usages the column authorises beside the housing, and are zero on a
    #: column that authorises none.
    residential_floors: int = 0
    above_grade_parking_floors: int = 0
    commercial_floors: int = 0
    industrial_floors: int = 0
    #: Levels dug below grade. Not storeys, and not floor area.
    underground_levels: int = 0
    #: Stalls, by where they were put. Their sum meets the dwellings' demand.
    underground_stalls: int = 0
    above_grade_stalls: int = 0
    #: `footprint x underground_levels` - built, paid for, and invisible to
    #: both the density cap and the storey cap.
    underground_area_m2: float = 0.0
    #: `footprint x commercial_floors` and `footprint x industrial_floors`.
    #: Above grade, so both are inside `gross_floor_area_m2` and both counted
    #: against *Densite* - they are floor area the by-law sees, unlike the
    #: underground parking beside them.
    commercial_area_m2: float = 0.0
    industrial_area_m2: float = 0.0
    #: What the stalls cost to build, in dollars. A capital figure, unlike
    #: `net_operating_income`, which is monthly.
    parking_cost_cad: float = 0.0
    #: What the dwellings cost to build, in dollars. Capital, and priced off
    #: `UNIT_AREAS_SQFT` rather than off the gross floor area.
    construction_cost_cad: float = 0.0
    #: What the commercial and industrial storeys cost to build, in dollars.
    #: Capital, and priced off their gross area - the one above and not a unit
    #: schedule, because there is none for them.
    commercial_cost_cad: float = 0.0
    industrial_cost_cad: float = 0.0
    #: Effective gross income a month, before any cost is taken off it. The
    #: dwelling side is priced at the proforma rent - CMHC's average times
    #: `InvestmentAssumptions.rent_premium_factor` - because that is the rent
    #: every dollar figure on this object is built from.
    gross_revenue_cad: float = 0.0
    #: The objective: discounted net profit, in dollars. The present value of
    #: the stabilised income (`present_value_cad`) less the capital in
    #: `total_capital_cost_cad`. Negative means the best program the envelope
    #: allows still does not pay for itself at these prices - a real answer,
    #: and the mix reported alongside is the one that loses least... except
    #: that the solver never builds a loss, so a negative here only appears
    #: when a *minimum* forced floor to be built.
    npv_cad: float = 0.0
    #: What the completed building is worth: the stabilised NOI discounted
    #: over the hold plus the discounted sale. `npv_cad` plus the capital.
    present_value_cad: float = 0.0
    #: Gross revenue annualised with the operating expense ratio off - the
    #: figure the discounting was applied to, and the one to put beside the
    #: comparables' stabilised NOI for a standing building.
    annual_stabilised_noi_cad: float = 0.0
    #: Which cap the answer is sitting against, for whoever asks why it is not
    #: bigger. More than one can bind at once.
    binding: tuple[str, ...] = ()
    #: Classes left out because CMHC published no rent for them.
    unpriced_types: tuple[str, ...] = ()
    zone: str | None = None
    lot_number: str | None = None

    @property
    def total_dwellings(self) -> int:
        return sum(self.units.values())

    @property
    def total_stalls(self) -> int:
        return self.underground_stalls + self.above_grade_stalls

    @property
    def commercial_area_sqft(self) -> float:
        return self.commercial_area_m2 / M2_PER_SQFT

    @property
    def industrial_area_sqft(self) -> float:
        return self.industrial_area_m2 / M2_PER_SQFT

    @property
    def non_residential_area_m2(self) -> float:
        """The commerce and the industry together. Parking is not in here -
        it is reported by where it was put, and the underground half of it is
        not floor area at all."""
        return self.commercial_area_m2 + self.industrial_area_m2

    @property
    def total_capital_cost_cad(self) -> float:
        """Dwellings, non-residential floors and stalls together, in dollars."""
        return (
            self.construction_cost_cad
            + self.commercial_cost_cad
            + self.industrial_cost_cad
            + self.parking_cost_cad
        )

    @property
    def profit(self) -> float:
        """`net_operating_income`, under the name it used to carry."""
        return self.net_operating_income

    @property
    def solved(self) -> bool:
        return self.status in ("OPTIMAL", "FEASIBLE")


#: What a storey of the reported stack is filled with, ground upwards. Not an
#: order the solver chose - `solve_program` counts storeys by type and never
#: places one, because the grid it reads never says which usage belongs on
#: which level (see `ZoneColumn.permitted_floors_count`). This is the stacking
#: a reader assumes when nothing says otherwise: commerce at grade where there
#: is any, industry behind it, the parking deck as a podium over both, and the
#: housing on top of the lot. Changing it changes a drawing, never a number.
FLOOR_STACK_ORDER: tuple[str, ...] = (
    "commercial",
    "industrial",
    "parking",
    "residential",
)

#: How many decimals the metres and square metres of a stack are reported to.
#: The stack is a restatement of columns that are already on the row, so it is
#: rounded to something a person can read; the unrounded figures are the
#: columns themselves.
_STACK_PRECISION = 2


def floor_stack(
    program: DevelopmentProgram,
    *,
    heights: StoreyHeights = DEFAULT_STOREY_HEIGHTS,
) -> list[dict]:
    """What stands on each storey, as runs of identical ones, bottom upwards.

    `DevelopmentProgram` answers in totals - five residential storeys, one of
    commerce, two levels dug - and the question a reader actually has of a
    proposed building is what is on floor three. This is that, and it is a
    *view* of the program rather than anything new: every number here is a
    column beside it, re-cut by storey.

    **Runs, not storeys.** One entry per run of identical levels rather than
    one per level, which is the module docstring's "one footprint, identical
    floors" arriving at its conclusion: every plate of a given use is the same
    plate, so a use is a single run however tall it is, and a fifteen-storey
    tower over retail and a deck is four entries and not fifteen. An entry
    spans ``from_level`` to ``to_level`` inclusive and says how many
    ``floors`` that is.

    **Levels are numbered from grade.** Level 1 is the *rez-de-chaussée* and
    there is no level 0; the dug levels are -1 downwards, so a building with
    two of them starts at -2. That sign is also the one distinction the
    by-law makes: a below-grade level is not *superficie de plancher* (article
    38 1°) and is not measured from grade up, so ``counts_as_floor_area`` is
    false there and ``storey_height_m`` is `UNDERGROUND_LEVEL_HEIGHT_M` - the
    zero `StoreyHeights` documents rather than an omission.

    **The order is a reporting convention and nothing more.**
    `FLOOR_STACK_ORDER` is where it is written down and why it carries no
    weight: the *Niveaux de bâtiment autorisés* block is marked per column and
    not per usage, so the solver is free to put any of a column's usages on
    any storey it allows and never records which. Two programs with the same
    counts stack the same way here whatever an architect would do with them.

    **Every entry has every key**, so ``jsonb_array_elements`` over a column of
    these needs no branch: ``stalls`` is 0 on a floor that parks nothing,
    ``dwellings`` is 0 and ``units`` is empty on a floor that houses nobody.
    The dwelling mix sits on the residential run whole rather than divided by
    its storeys - the solver chose a mix for the building, not for a plate,
    and splitting it would be inventing the part it did not choose.

    Areas and heights are rounded to `_STACK_PRECISION`; the unrounded figures
    are `gross_floor_area_m2`, `underground_area_m2` and `height_m` on the
    program itself.
    """
    plate = program.footprint_m2
    stack: list[dict] = []

    def run(
        use: str,
        *,
        position: str,
        first: int,
        floors: int,
        storey_height_m: float,
        counts_as_floor_area: bool,
        stalls: int = 0,
        dwellings: int = 0,
        units: Mapping[str, int] | None = None,
    ) -> dict:
        return {
            "use": use,
            "position": position,
            "from_level": first,
            "to_level": first + floors - 1,
            "floors": floors,
            "floor_plate_m2": round(plate, _STACK_PRECISION),
            "floor_area_m2": round(plate * floors, _STACK_PRECISION),
            "counts_as_floor_area": counts_as_floor_area,
            "storey_height_m": round(storey_height_m, _STACK_PRECISION),
            "height_m": round(storey_height_m * floors, _STACK_PRECISION),
            "stalls": stalls,
            "dwellings": dwellings,
            "units": dict(units or {}),
        }

    if program.underground_levels > 0:
        stack.append(
            run(
                "parking",
                position="below_grade",
                first=-program.underground_levels,
                floors=program.underground_levels,
                storey_height_m=UNDERGROUND_LEVEL_HEIGHT_M,
                counts_as_floor_area=False,
                stalls=program.underground_stalls,
            )
        )

    above: dict[str, tuple[int, float, dict]] = {
        "commercial": (program.commercial_floors, heights.commercial_m, {}),
        "industrial": (program.industrial_floors, heights.industrial_m, {}),
        "parking": (
            program.above_grade_parking_floors,
            heights.above_grade_parking_m,
            {"stalls": program.above_grade_stalls},
        ),
        "residential": (
            program.residential_floors,
            heights.residential_m,
            {"dwellings": program.total_dwellings, "units": program.units},
        ),
    }
    level = 1
    for use in FLOOR_STACK_ORDER:
        floors, storey_height_m, extra = above[use]
        if floors <= 0:
            continue
        stack.append(
            run(
                use,
                position="above_grade",
                first=level,
                floors=floors,
                storey_height_m=storey_height_m,
                counts_as_floor_area=True,
                **extra,
            )
        )
        level += floors
    return stack


def select_governing_column(
    columns: Sequence[ZoneColumn],
    frontage_m: float,
    *,
    permits,
) -> ZoneColumn | None:
    """The column of one usage family that governs a lot of this width.

    A grid can authorise a family in more than one column, and the columns are
    then distinguished by the parcel they apply to: *Largeur du terrain min*
    rises across them, so the widest minimum the lot still satisfies is the
    column written for a lot of its size. Columns the lot is too narrow for are
    not applicable; among the rest, the most specific wins.

    A minimum is a floor, so a lot exactly as wide as one **meets** it - the
    test is ``min_lot_width_m <= frontage_m``. A column printing ``-`` has no
    minimum and is the fallback every lot qualifies for.

    ``permits`` is which family is being asked about - one of the three
    `ZoneColumn.permits_*` properties, passed as a callable so the rule is
    written once for all of them. Returns ``None`` when the grid authorises
    the family nowhere, or when every one of its columns demands more frontage
    than the lot has.

    One winner per family, and the winners can disagree: a lot can be governed
    by an ``H.2`` column for its housing and a ``C.4`` one for its commerce,
    and *which of those two to build* is the developer's choice - the one
    `urban_rag.hbu.select_highest_best_use` now makes on discounted net
    profit, across the governing column of each family.
    """
    eligible = [
        column
        for column in columns
        if permits(column)
        and (column.min_lot_width_m is None or column.min_lot_width_m <= frontage_m)
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda column: column.min_lot_width_m or 0.0)


def select_residential_column(
    columns: Sequence[ZoneColumn], frontage_m: float
) -> ZoneColumn | None:
    """The Habitation column that governs a lot of this width."""
    return select_governing_column(
        columns, frontage_m, permits=lambda column: column.permits_residential
    )


def select_commercial_column(
    columns: Sequence[ZoneColumn], frontage_m: float
) -> ZoneColumn | None:
    """The Commerce column that governs a lot of this width."""
    return select_governing_column(
        columns, frontage_m, permits=lambda column: column.permits_commercial
    )


def select_industrial_column(
    columns: Sequence[ZoneColumn], frontage_m: float
) -> ZoneColumn | None:
    """The Industrie column that governs a lot of this width."""
    return select_governing_column(
        columns, frontage_m, permits=lambda column: column.permits_industrial
    )


@dataclass(frozen=True)
class ZoneEnvelope:
    """Every column governing one lot, as the single rule-set a building answers to.

    **Why this exists.** A grid states one usage family per column in most
    boroughs - in Villeray-Saint-Michel-Parc-Extension, every one of the 1 463
    parsed columns carries exactly one code - so "this zone allows housing and
    commerce" is printed as two columns and never as one. Solving each column
    on its own and keeping the better answer therefore cannot propose the
    building the zone actually permits: retail at grade with dwellings above
    is a *mix*, and a mix is not the maximum of two pure programs. This is the
    object that lets one model hold all three families at once, with the floor
    area split between them a decision rather than a choice made in advance.

    **Which norms bind a mixed building.** Each family's governing column is
    the grid's own pick for that family (`select_governing_column`, on
    *Largeur du terrain min*), and its norms bind the whole building **when
    that family is actually built**. A building of housing and commerce
    answers to the intersection of the H column's caps and the C column's; one
    of commerce alone answers to the C column's only. That is what makes the
    single-family programs feasible points of this same model rather than
    separate solves - `solve_program` never has to be asked twice - and it is
    the conservative reading where two columns disagree: a mixed building is
    held to the stricter of the two, never the looser.

    The properties below are the **loosest** bound across the families present,
    and they exist to size CP-SAT's variable domains, which need only contain
    every feasible point. The binding caps are the per-family ones, applied
    inside `solve_program` conditionally on the family being used. Reading a
    property here as "the norm" would be a mistake: `floors_max` is the tallest
    any single family may stand, not the tallest the building may.
    """

    #: The governing column of each family, absent where the grid authorises
    #: that family nowhere on this lot - or where every column stating it
    #: demands more frontage than the lot has.
    residential: ZoneColumn | None = None
    commercial: ZoneColumn | None = None
    industrial: ZoneColumn | None = None

    @classmethod
    def of(
        cls, columns: Sequence[ZoneColumn], frontage_m: float
    ) -> ZoneEnvelope:
        """The envelope a lot of this width gets from a zone's columns.

        One `select_governing_column` pass per family, which is the same rule
        `envelope_assets` writes its `governs_*` flags with - so the envelope
        assembled here is the one those flags already mark.
        """
        return cls(
            residential=select_residential_column(columns, frontage_m),
            commercial=select_commercial_column(columns, frontage_m),
            industrial=select_industrial_column(columns, frontage_m),
        )

    @classmethod
    def single(cls, column: ZoneColumn) -> ZoneEnvelope:
        """One column as an envelope, governing whichever families it heads.

        The bridge for every caller that has a column rather than a zone -
        `solve_program` accepts either, and a pure ``H`` column wrapped here
        produces exactly the model it produced before this class existed.
        """
        return cls(
            residential=column if column.permits_residential else None,
            commercial=column if column.permits_commercial else None,
            industrial=column if column.permits_industrial else None,
        )

    @property
    def columns(self) -> tuple[ZoneColumn, ...]:
        """The distinct governing columns, in family order.

        Identity, not equality: two columns of one grid can state identical
        norms, and a zone that governs its housing and its commerce with two
        such columns still has two of them.
        """
        seen: list[ZoneColumn] = []
        for column in (self.residential, self.commercial, self.industrial):
            if column is not None and not any(column is other for other in seen):
                seen.append(column)
        return tuple(seen)

    @property
    def permits_residential(self) -> bool:
        return self.residential is not None

    @property
    def permits_commercial(self) -> bool:
        return self.commercial is not None

    @property
    def permits_industrial(self) -> bool:
        return self.industrial is not None

    @property
    def is_empty(self) -> bool:
        """Whether the grid authorises none of the three families here."""
        return not self.columns

    @property
    def zone(self) -> str | None:
        """The zone these columns belong to, carried for reporting."""
        for column in self.columns:
            if column.zone is not None:
                return column.zone
        return None

    @property
    def usages(self) -> tuple[str, ...]:
        """Every usage code the governing columns carry, deduplicated."""
        codes: list[str] = []
        for column in self.columns:
            for usage in column.usages:
                if usage not in codes:
                    codes.append(usage)
        return tuple(codes)

    # -- domain bounds -----------------------------------------------------
    # The loosest norm across the governing columns. Domains, not caps: see
    # the class docstring.

    @property
    def floors_max(self) -> int:
        return max((column.floors_max for column in self.columns), default=0)

    @property
    def floors_min(self) -> int:
        return min((column.floors_min for column in self.columns), default=0)

    @property
    def permitted_floors_count(self) -> int:
        return max(
            (column.permitted_floors_count for column in self.columns), default=0
        )

    @property
    def height_max_m(self) -> float | None:
        return _loosest_max(column.height_max_m for column in self.columns)

    @property
    def height_min_m(self) -> float | None:
        return _loosest_min(column.height_min_m for column in self.columns)

    @property
    def density_max(self) -> float | None:
        return _loosest_max(column.density_max for column in self.columns)

    @property
    def density_min(self) -> float | None:
        return _loosest_min(column.density_min for column in self.columns)

    @property
    def site_coverage_max_pct(self) -> float | None:
        return _loosest_max(column.site_coverage_max_pct for column in self.columns)

    @property
    def site_coverage_min_pct(self) -> float | None:
        return _loosest_min(column.site_coverage_min_pct for column in self.columns)

    @property
    def max_dwellings(self) -> int | None:
        """The dwelling ceiling, from the Habitation column alone.

        A ``C.4`` column states no *Nombre de logements maximal* and implies
        no class ceiling, and it must not be allowed to lift the H column's.
        """
        if self.residential is None:
            return None
        return self.residential.effective_max_dwellings


def _loosest_max(values: Iterable[float | None]) -> float | None:
    """The weakest of several maxima: the largest, or ``None`` if any is absent.

    An absent maximum is no maximum at all, so it dominates - a family whose
    column prints ``Densite -`` may build to whatever else allows, and the
    domain has to reach that far.
    """
    seen = list(values)
    if not seen or any(value is None for value in seen):
        return None
    return max(value for value in seen if value is not None)


def _loosest_min(values: Iterable[float | None]) -> float | None:
    """The weakest of several minima: the smallest of those that are stated.

    An absent minimum is a minimum of nothing rather than a missing bound, so
    unlike `_loosest_max` it does not dominate - it simply contributes no
    floor, and a family that states one still needs the domain to start below
    it.
    """
    stated = [value for value in values if value is not None]
    return min(stated) if stated else None


def solve_program(
    column: ZoneColumn | ZoneEnvelope,
    lot: Lot,
    economics: UnitEconomics,
    *,
    unit_areas_sqft: Mapping[str, float] = UNIT_AREAS_SQFT,
    parking: ParkingRules = DEFAULT_PARKING,
    construction: ConstructionCosts = DEFAULT_CONSTRUCTION,
    non_residential: NonResidentialEconomics = DEFAULT_NON_RESIDENTIAL,
    heights: StoreyHeights = DEFAULT_STOREY_HEIGHTS,
    investment: InvestmentAssumptions = DEFAULT_INVESTMENT,
    max_seconds: float = 10.0,
) -> DevelopmentProgram:
    """The program maximising discounted net profit on ``lot``.

    The model, in square metres:

    ====================================  ===================================
    ``footprint``                         within *Taux d'implantation* x lot
                                          area **and** `Lot.buildable_area_m2`
                                          where one is given, and shared by
                                          every floor
    ``residential_floors``                within *En étage min* .. storeys the
                                          level rows allow this usage
    ``parking_floors``                    above-grade parking storeys, 0 or
                                          more
    ``commercial_floors``                 0 unless the column's usages include
                                          a ``C`` class
    ``industrial_floors``                 0 unless they include an ``I`` class
    ``floors``                            the four added up, within
                                          *En étage max*
    ``underground_levels``                0 .. `max_underground_levels`; not
                                          storeys
    ``residential_area``                  ``footprint x residential_floors``
    ``parking_area``                      ``footprint x parking_floors``
    ``commercial_area``                   ``footprint x commercial_floors``
    ``industrial_area``                   ``footprint x industrial_floors``
    ``underground_area``                  ``footprint x underground_levels``
    ``height``                            the above-grade storeys at
                                          `StoreyHeights`, within *Hauteur en
                                          mètre*; the underground levels add
                                          nothing to it
    ``gross``                             the four above-grade areas, within
                                          *Densité* x lot area
    ``sum(area_t x n_t) <= residential``  the dwellings fit in their storeys
    ``sum(n_t) <= max_dwellings``         *Nombre de logements maximal*
    ``stalls >= n_t and area ratios``     everything's stalls are provided,
                                          in one inequality
    ``stall_area x stalls <= area``       and they fit where they were put
    ====================================  ===================================

    Five products of decision variables now rather than one, and they share
    the ``footprint`` factor, which is what makes ``gross`` a plain sum of four
    of them instead of a sixth multiplication.

    **Two ceilings on the same stack.** *En étage* counts storeys and
    *Hauteur en mètre* measures them, and a grid states both. They are not the
    same cap because the storeys are not the same height: a residential plate
    stands 3 m, a commercial or industrial one 4 m, an above-grade deck 3 m, so
    ``6`` storeys and ``15`` metres is five of housing or three of commerce and
    never six of anything. Both are linear in the storey counts, which is why
    the metric one adds a constraint and no new product - the expensive part of
    this model is the areas, and the height does not touch them.

    Where it bites is the mix. Under *En étage* alone a retail plate and a
    residential plate cost the same storey, so commerce takes every storey its
    rent can pay for; under *Hauteur* it costs a third more of the ceiling as
    well, on top of the parking it already owes, and a column with a tight
    metric cap is where the housing wins a storey back. `binding` reports
    ``height_max`` where the answer is sitting against it.

    **What the density cap does and does not see.** ``gross`` is above-grade
    area only. ``underground_area`` is built and paid for and appears in
    neither the *Densité* constraint nor the storey count, which is article
    38 1° of 01-283 - *une aire de stationnement des véhicules [...] située en
    sous-sol, de même que leurs voies d'accès* - expressed as the one place
    the two parking options stop being interchangeable.

    **Why the choice is not trivial.** An underground stall is bigger and
    dearer than an above-grade one, so with a slack envelope the solver puts
    the parking above grade and pockets the difference. Tighten *Densité*
    until a parking storey costs a dwelling worth more than the price gap and
    it digs instead. Both behaviours are the model working.

    Commerce and industry sharpen that choice rather than adding to it. At
    `stalls_per_1000_sqft` a non-residential plate owes roughly its own area
    back in stalls, so above grade it costs two storeys of envelope to get
    one storey of rent - and the density cap, which the retail floor is
    already inside, has to hold both. Underground, only the retail floor
    counts. A mixed-use column with a tight *Densité* is where the model digs
    first.

    **What is being maximised.** Discounted net profit: each dwelling
    contributes its effective proforma rent through
    `InvestmentAssumptions.pv_per_monthly_gross` - twelve months, the expense
    ratio off, the annuity over the hold, the discounted sale - less its build
    cost in full; each stall subtracts its capital outright, because it earns
    nothing to discount. Every cost and every value is per dwelling, per
    square foot or per stall, so they fold into the objective's coefficients
    and the program stays the same shape it was. Under
    `UNDISCOUNTED_INVESTMENT` the coefficients are exactly the old monthly
    ones times `AMORTIZATION_MONTHS`, so the argmax is what it always was.

    A square foot of commerce or of industry folds in the same way, one step
    further out: its rent and its build cost are both per square foot, so the
    pair collapses to a single coefficient on the *area* variable rather than
    on a count. Whether commerce outbids housing for a storey is now entirely
    a question of the rates: at the module's stated $80 a foot it does, at
    the ~$27 the borough's surveyed retail rent resolves to it rarely covers
    its own build, and both answers are the arithmetic rather than a
    preference - which is the reason to be able to change all four rates from
    the call site.

    **A column with no Habitation at its head now solves too.** A pure ``C``
    or ``I`` column is the same model with the dwelling counts pinned at
    zero: commerce and industry compete for the storeys, owe their stalls,
    and answer to every printed cap. *En étage min* is then owed by those
    storeys - the parking is not allowed to pay it there either. Only a
    column authorising none of the three priced families is refused.

    **Where the non-residential storeys go.** Nowhere in particular: they are
    plates in the same stack, and the model has no notion of *which* storey a
    plate is. A grid saying "commerce at the RDC, housing above" says it in
    two columns with different *Niveaux* rows, and two columns are two
    envelopes - see `ZoneColumn.permitted_floors_count`. Within one column the
    storeys are interchangeable, which overstates what a single mixed-use
    column can really be built as, and is the assumption to revisit before a
    mixed-use answer is taken seriously.

    A class whose rent does not cover its construction contributes a negative
    coefficient and the solver declines to build it, and the same is true of a
    commercial or industrial storey. A parcel where that is true of everything
    comes back with no dwellings and no floors at all, which is an answer about
    the parcel: at these rents and these rates, nothing pencils.

    Every cap is skipped where the grid does not state one. All storeys are
    identical, and a storey is dwellings, parking, commerce or industry and
    never two of them - both by assumption, and both the reason the envelope
    collapses to a single footprint and a handful of counts.

    A minimum - on density, coverage or storeys - can make the model
    infeasible, and so now can the parking: a lot with no room for the stalls
    its dwellings owe has no program at this ratio. That is a real answer
    about the parcel rather than a bug, and `NO_PARKING` is how to ask the
    question without it. The status says which.

    **A zone, not a column.** The first argument may be a `ZoneEnvelope` - the
    governing column of each family a zone authorises - and that is the form
    that answers the question a mixed-use zone actually poses. Where a grid
    prints its housing and its commerce in two columns, solving the two
    separately can only ever return the better *pure* building; handed both at
    once, the model splits the floor area between them and returns the mix
    that is worth the most, which may be neither. A bare `ZoneColumn` is
    wrapped with `ZoneEnvelope.single` and behaves exactly as it did.

    **What binds a mixed building.** Each family's norms are enforced only
    when that family is built, through the `use_*` literals below - so a
    building of housing and commerce answers to the intersection of the two
    columns' caps, and one of commerce alone answers to the C column's alone.
    The single-family programs therefore remain feasible points of this model:
    it never returns less than solving each column apart would have, and where
    two columns disagree it holds the mix to the stricter of them.
    """
    envelope = (
        column if isinstance(column, ZoneEnvelope) else ZoneEnvelope.single(column)
    )
    if envelope.is_empty:
        usages = column.usages if isinstance(column, ZoneColumn) else envelope.usages
        raise ProgramError(
            f"{usages} authorises none of the usages this module "
            "prices (Habitation, Commerce, Industrie); an Equipements "
            "collectifs column is deliberately not a proforma"
        )
    column = envelope

    # *Hauteur en mètre*, as centimetres. Rounded the way each bound wants to
    # be read: a maximum floors so a building may not exceed it by a rounding,
    # a minimum ceils so one may not fall short of it by the same.
    height_cap_cm = (
        _floor_scaled_height(column.height_max_m)
        if column.height_max_m is not None
        else None
    )
    height_floor_cm = (
        _ceil_scaled_height(column.height_min_m)
        if column.height_min_m is not None
        else 0
    )
    if height_cap_cm is not None and height_floor_cm > height_cap_cm:
        # Two rows of the same column contradicting each other, like
        # `site_coverage_range` below and named for the same reason.
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("height_range",),
        )
    # The cheapest storey the column's own usages can spend the metric cap
    # on: three metres where dwellings are among them, four where only
    # commerce or industry is. What *En étage min* is priced at in the check
    # below, because the minimum is met by usage storeys and not by parking.
    usage_heights_cm = [
        cm
        for cm, permitted in (
            (heights.residential_cm, column.permits_residential),
            (heights.commercial_cm, column.permits_commercial),
            (heights.industrial_cm, column.permits_industrial),
        )
        if permitted
    ]
    cheapest_usage_cm = min(usage_heights_cm)
    if (
        height_cap_cm is not None
        and cheapest_usage_cm * column.floors_min > height_cap_cm
    ):
        # *En étage min* demands storeys that *Hauteur max* has no room for.
        # Named apart from the level-row contradiction below because it is a
        # different pair of rows disagreeing, and because the fix is a
        # different one: a shorter `StoreyHeights` can resolve this and cannot
        # resolve that.
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("height_max_below_floors_min",),
        )

    # The storeys the level rows allow, at their loosest across the governing
    # columns. A bound on the variable domains only: the level rows are marked
    # per column, so the storeys a *family* may occupy come from that family's
    # own column and are imposed per column further down.
    permitted_count = column.permitted_floors_count
    if all(
        governing.permitted_floors_count < governing.floors_min
        for governing in envelope.columns
    ):
        # Not an empty solution but a contradiction between two rows of the
        # same column, so it is named rather than returned as a bare
        # INFEASIBLE the caller would have to diagnose. The height cannot be
        # the cause here - the check above already priced the minimum at the
        # cheapest storey the column can build - so this is the level rows'
        # own doing. *En étage min* is read as a minimum on the *usage*
        # storeys, not on the building's, so a parking floor is not allowed
        # to talk the grid out of it.
        #
        # Every governing column, because one column contradicting itself in a
        # zone whose other column does not is a building that can still be
        # built - out of the family the coherent column heads.
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("floors_min_exceeds_permitted_levels",),
        )

    # The metric cap, read as the storey count it allows *each* usage. Read
    # per usage rather than once for the building because the storey types are
    # different heights: the same 15 m is five residential storeys and three
    # commercial ones.
    height_floors_cap = (
        None if height_cap_cm is None else height_cap_cm // heights.residential_cm
    )
    res_floors_allowed = (
        envelope.residential.permitted_floors_count
        if envelope.residential is not None
        else 0
    )
    if height_floors_cap is not None:
        res_floors_allowed = min(res_floors_allowed, height_floors_cap)
    # *En étage min* is owed by the usage storeys between them and not by the
    # dwellings in particular, so it is a constraint on their sum rather than
    # a floor under this variable's domain - the commerce of a mixed zone may
    # supply it. Held at zero here and imposed per governing column below,
    # which on a single column is the same rule the module has always applied:
    # a pure Habitation column spares no other family to supply it, so the sum
    # constraint reduces to the dwellings owing the whole minimum.
    res_floors_min = 0

    unpriced = tuple(
        sorted(
            unit_type
            for unit_type in unit_areas_sqft
            if economics.monthly_revenue(unit_type) is None
        )
    )
    priced = {
        unit_type: area_sqft
        for unit_type, area_sqft in unit_areas_sqft.items()
        if unit_type not in unpriced
    }
    if not priced and not (column.permits_commercial or column.permits_industrial):
        # No dwelling can be priced and nothing else is authorised, so there
        # is no model to build. A column that also authorises commerce or
        # industry keeps solving: CMHC suppressing a borough's rents says
        # nothing about what a retail floor earns.
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("no_priced_unit_type",),
            unpriced=unpriced,
        )

    footprint_lo = _ceil_scaled(
        lot.area_m2 * (column.site_coverage_min_pct or 0.0) / 100.0
    )
    footprint_hi = _site_coverage_cap(column, lot)
    if footprint_lo > footprint_hi:
        return _empty_program(
            column,
            lot,
            status="INFEASIBLE",
            binding=("site_coverage_range",),
            unpriced=unpriced,
        )

    # The second cap, and an independent one: *Taux d'implantation* says what
    # share of the parcel may be covered, the margins say where on it, and a
    # building satisfies both. Applied after the range check above so a column
    # whose own two coverage bounds contradict each other still reports that
    # rather than being masked by a lot too narrow to build on.
    if lot.buildable_area_m2 is not None:
        footprint_hi = min(footprint_hi, _floor_scaled(lot.buildable_area_m2))
        if footprint_lo > footprint_hi:
            # Not a contradiction inside the grid, unlike `site_coverage_range`
            # above: the column is coherent and this parcel cannot satisfy it.
            # A named binding rather than a bare INFEASIBLE because the fix is
            # a different one - *Taux d'implantation min* demands a footprint
            # the margins leave no room for, which on a narrow lot is the whole
            # answer to what may be built here.
            return _empty_program(
                column,
                lot,
                status="INFEASIBLE",
                binding=("buildable_area_below_site_coverage_min",),
                unpriced=unpriced,
            )

    density_cap = (
        _floor_scaled(lot.area_m2 * column.density_max)
        if column.density_max is not None
        else None
    )

    # Two ceilings, and confusing them is the easy mistake. The *building* may
    # stack `floors_max` storeys of any kind; the *dwellings* may only occupy
    # the storeys the level rows allow. The first sizes the area variables,
    # the second sizes the unit counts.
    residential_hi = footprint_hi * res_floors_allowed
    if density_cap is not None:
        residential_hi = min(residential_hi, density_cap)

    # A storey that is not dwellings has to fit inside `floors_max` alongside
    # them, and the dwellings already claim what they owe *En étage min* -
    # nothing, on a column that authorises none. Commerce and industry are
    # bounded by the level rows on top of that, the same way the dwellings
    # are - they are usages of this column, and the rows are the column's.
    spare_floors = max(column.floors_max - res_floors_min, 0)
    parking_floors_hi = spare_floors
    commercial_floors_hi = (
        min(spare_floors, envelope.commercial.permitted_floors_count)
        if envelope.commercial is not None
        else 0
    )
    industrial_floors_hi = (
        min(spare_floors, envelope.industrial.permitted_floors_count)
        if envelope.industrial is not None
        else 0
    )
    if height_cap_cm is not None:
        # Same reading of the same cap, for the storey types that are not the
        # dwellings. A four-metre plate gets fewer of them than the housing
        # does out of the identical number of metres, which is the whole of
        # what a metric cap adds to a storey cap.
        parking_floors_hi = min(
            parking_floors_hi, height_cap_cm // heights.above_grade_parking_cm
        )
        commercial_floors_hi = min(
            commercial_floors_hi, height_cap_cm // heights.commercial_cm
        )
        industrial_floors_hi = min(
            industrial_floors_hi, height_cap_cm // heights.industrial_cm
        )
    # An underground level is not a storey and stands no metres, so nothing
    # here narrows it - `UNDERGROUND_LEVEL_HEIGHT_M` is that zero, and the
    # reason a lot whose *Hauteur* binds digs rather than stops.
    underground_levels_hi = parking.max_underground_levels if parking.required else 0

    # 0 where no dwelling can be priced at all - a pure Commerce or Industrie
    # column, or a borough CMHC suppressed entirely. The counts then have
    # empty domains rather than the arithmetic dividing by nothing.
    smallest_unit_area = (
        min(_scale_area(area_sqft * M2_PER_SQFT) for area_sqft in priced.values())
        if priced
        else 0
    )
    dwellings_hi = (
        int(residential_hi // smallest_unit_area) if smallest_unit_area else 0
    )
    if column.max_dwellings is not None:
        dwellings_hi = min(dwellings_hi, column.max_dwellings)

    # The two ratios, each expressed against the thing it is charged on: one
    # per dwelling, one per hundredth of a square metre of non-residential
    # floor. `STALL_DEMAND_SCALE` is what lets them be added together.
    stall_ratio = round(parking.stalls_per_dwelling * STALL_DEMAND_SCALE)
    area_stall_ratio = round(
        parking.stalls_per_1000_sqft
        / 1000.0
        / (AREA_SCALE * M2_PER_SQFT)
        * STALL_DEMAND_SCALE
    )

    # The most non-residential floor this envelope could hold, which is what
    # bounds the stalls it could owe. Both caps apply: the storeys the level
    # rows spare, and the density the grid prints.
    non_residential_area_hi = footprint_hi * (
        commercial_floors_hi + industrial_floors_hi
    )
    if density_cap is not None:
        non_residential_area_hi = min(non_residential_area_hi, density_cap)

    # Ceiling division: a part stall is a stall, which is how a ratio is read
    # everywhere one is written into a by-law.
    stalls_hi = -(
        -(stall_ratio * dwellings_hi + area_stall_ratio * non_residential_area_hi)
        // STALL_DEMAND_SCALE
    )
    underground_stall_area = _scale_area(parking.underground_area_sqft * M2_PER_SQFT)
    above_grade_stall_area = _scale_area(parking.above_grade_area_sqft * M2_PER_SQFT)

    model = cp_model.CpModel()

    footprint = model.NewIntVar(footprint_lo, footprint_hi, "footprint")
    residential_floors = model.NewIntVar(
        res_floors_min, res_floors_allowed, "residential_floors"
    )
    parking_floors = model.NewIntVar(0, parking_floors_hi, "parking_floors")
    commercial_floors = model.NewIntVar(0, commercial_floors_hi, "commercial_floors")
    industrial_floors = model.NewIntVar(0, industrial_floors_hi, "industrial_floors")
    underground_levels = model.NewIntVar(0, underground_levels_hi, "underground_levels")
    # The building's own ceiling, which is the storey row narrowed by the most
    # plates the metric cap could hold if every one of them were the shortest
    # kind available. Any real mix stands taller than that, and the height
    # constraint below is what holds it - this only keeps CP-SAT out of a
    # region it can prove nothing in. It cannot fall below *En étage min*: the
    # check above already established that `floors_min` residential storeys
    # fit, and no storey type is taller than the tallest of them.
    floors_hi = column.floors_max
    if height_cap_cm is not None:
        floors_hi = min(floors_hi, height_cap_cm // heights.shortest_cm)
    floors = model.NewIntVar(column.floors_min, floors_hi, "floors")
    model.Add(
        floors
        == residential_floors + parking_floors + commercial_floors + industrial_floors
    )
    # Whether each family is built at all. These are what make one model do
    # the work of three: a column's norms bind the building only when the
    # family it heads is present, so the pure-Habitation and pure-Commerce
    # programs stay feasible points here rather than separate solves, and the
    # mixed one is held to both columns at once.
    usage_floors = {
        "residential": residential_floors,
        "commercial": commercial_floors,
        "industrial": industrial_floors,
    }
    governing = {
        "residential": envelope.residential,
        "commercial": envelope.commercial,
        "industrial": envelope.industrial,
    }
    used: dict[str, cp_model.IntVar] = {}
    for family, floors_var in usage_floors.items():
        if governing[family] is None:
            continue
        literal = model.NewBoolVar(f"use_{family}")
        # Reified both ways: the solver may not claim a family is absent while
        # standing storeys of it, nor pay a column's caps for a family it did
        # not build.
        model.Add(floors_var >= 1).OnlyEnforceIf(literal)
        model.Add(floors_var == 0).OnlyEnforceIf(literal.Not())
        used[family] = literal

    # The level rows are the *column's*, not any one usage's, so the storeys
    # its usages occupy between them are bounded by what those rows allow -
    # not by that number three times over. A column marked "Tous sauf le RDC"
    # on a six-storey grid authorises five storeys of housing-or-commerce, and
    # without this the solver would stack two of housing on four of commerce
    # and hand back a building standing on a ground floor nothing may occupy.
    #
    # Per governing column, because that is the grain the rows are marked at:
    # the families one column heads share its allowance, and a second column's
    # families answer to its own. On a single column - the pure ``H`` grid, and
    # every zone in a borough that prints one usage per column - all three
    # families fall in one group and this is exactly the sum it always was.
    #
    # The parking is *not* in these sums: a stall is not one of the usages the
    # grid marks these rows for, and the storey it sits in is bounded by
    # *En etage max* alone - which is the constraint above.
    for group, group_column in _families_by_column(governing):
        model.Add(
            sum(usage_floors[family] for family in group)
            <= group_column.permitted_floors_count
        )

    # *En étage min* is not one of those allowances. The level rows say which
    # storeys a family may occupy and are read per column above; the minimum
    # says how tall the *building* must be, and every usage storey pays it
    # whichever column authorised it. Charging it to one family's storeys
    # instead would make a Commerce column printing "2/6" beside level rows
    # marking only the RDC impossible to use at all - it would demand two
    # storeys of commerce where the same column allows one - and a grid saying
    # "retail at grade, two storeys minimum" is describing a building with
    # something above the shop, not a contradiction.
    #
    # The parking is still not in the sum: a stall is not a usage, and a
    # building may not meet its storey minimum with a garage.
    usage_floor_total = residential_floors + commercial_floors + industrial_floors
    if column.floors_min:
        # The loosest minimum any governing column states, owed whatever the
        # building turns out to be made of. Unconditional, unlike the per-family
        # tightenings below, because without it a model that used no family at
        # all could meet *En étage min* with parking storeys and hand back a
        # garage - the storeys the dwellings' own domain used to guarantee
        # before the mix became a decision.
        model.Add(usage_floor_total >= column.floors_min)

    # The storey and footprint norms each column states, imposed the same way:
    # on the whole building, and only while the family whose column states it
    # is built. This is the "intersection of the uses built" reading - a mixed
    # building meets the strictest cap among the columns it draws its usages
    # from, and a single-family building meets only its own. *Hauteur* and
    # *Densité* get the same treatment below, once the variables they bound
    # exist.
    for family, literal in used.items():
        governing_column = governing[family]
        if governing_column.floors_max < floors_hi:
            model.Add(floors <= governing_column.floors_max).OnlyEnforceIf(literal)
        if governing_column.floors_min:
            model.Add(
                usage_floor_total >= governing_column.floors_min
            ).OnlyEnforceIf(literal)
        family_footprint_cap = _site_coverage_cap(governing_column, lot)
        if family_footprint_cap < footprint_hi:
            model.Add(footprint <= family_footprint_cap).OnlyEnforceIf(literal)
        family_footprint_floor = _ceil_scaled(
            lot.area_m2 * (governing_column.site_coverage_min_pct or 0.0) / 100.0
        )
        if family_footprint_floor > footprint_lo:
            model.Add(footprint >= family_footprint_floor).OnlyEnforceIf(literal)

    # The products that rule out an LP: every floor type is a footprint the
    # model chooses, stacked a number of times the model also chooses. All
    # three share `footprint`, which is the "every floor is the same plate"
    # assumption stated as algebra.
    residential_area = model.NewIntVar(
        0, max(footprint_hi * res_floors_allowed, 0), "residential_area"
    )
    model.AddMultiplicationEquality(residential_area, [footprint, residential_floors])

    parking_area = model.NewIntVar(
        0, max(footprint_hi * parking_floors_hi, 0), "parking_area"
    )
    model.AddMultiplicationEquality(parking_area, [footprint, parking_floors])

    commercial_area = model.NewIntVar(
        0, max(footprint_hi * commercial_floors_hi, 0), "commercial_area"
    )
    model.AddMultiplicationEquality(commercial_area, [footprint, commercial_floors])

    industrial_area = model.NewIntVar(
        0, max(footprint_hi * industrial_floors_hi, 0), "industrial_area"
    )
    model.AddMultiplicationEquality(industrial_area, [footprint, industrial_floors])

    underground_area = model.NewIntVar(
        0, max(footprint_hi * underground_levels_hi, 0), "underground_area"
    )
    model.AddMultiplicationEquality(underground_area, [footprint, underground_levels])

    # *Hauteur en mètre*, and the one constraint in this model that is linear
    # in the decision variables it reads. The four above-grade storey types
    # enter at their own heights; `underground_levels` does not enter at all,
    # which is the by-law's own arithmetic rather than a shortcut - height is
    # measured from grade up.
    height = model.NewIntVar(
        0,
        max(
            heights.residential_cm * res_floors_allowed
            + heights.above_grade_parking_cm * parking_floors_hi
            + heights.commercial_cm * commercial_floors_hi
            + heights.industrial_cm * industrial_floors_hi,
            0,
        ),
        "height",
    )
    model.Add(
        height
        == heights.residential_cm * residential_floors
        + heights.above_grade_parking_cm * parking_floors
        + heights.commercial_cm * commercial_floors
        + heights.industrial_cm * industrial_floors
    )
    if height_cap_cm is not None:
        model.Add(height <= height_cap_cm)
    if height_floor_cm:
        # A minimum, like *Densité min* and *En étage min*, and infeasible in
        # the same way: a metric minimum no permitted stack of storeys reaches
        # is an answer about the column rather than a bug.
        model.Add(height >= height_floor_cm)

    # `gross` is the *superficie de plancher*: above grade only, article 38 1°.
    # Commerce and industry are in it - they are storeys, and nothing in the
    # by-law excludes a storey for what is done inside it.
    gross = model.NewIntVar(0, max(footprint_hi * column.floors_max, 0), "gross")
    model.Add(
        gross == residential_area + parking_area + commercial_area + industrial_area
    )

    if density_cap is not None:
        model.Add(gross <= density_cap)
    if column.density_min is not None:
        model.Add(gross >= _ceil_scaled(lot.area_m2 * column.density_min))

    # The other half of the per-family norms begun above the areas: *Hauteur en
    # mètre* and *Densité*, each binding the whole building while the family
    # whose column prints it is built. An absent cap on a family that is built
    # lifts nothing - the loosest bound is already what sized the domain, and
    # the columns that do state one still bind.
    for family, literal in used.items():
        governing_column = governing[family]
        family_height_cap = (
            _floor_scaled_height(governing_column.height_max_m)
            if governing_column.height_max_m is not None
            else None
        )
        if family_height_cap is not None and (
            height_cap_cm is None or family_height_cap < height_cap_cm
        ):
            model.Add(height <= family_height_cap).OnlyEnforceIf(literal)
        family_height_floor = (
            _ceil_scaled_height(governing_column.height_min_m)
            if governing_column.height_min_m is not None
            else 0
        )
        if family_height_floor > height_floor_cm:
            model.Add(height >= family_height_floor).OnlyEnforceIf(literal)
        family_density_cap = (
            _floor_scaled(lot.area_m2 * governing_column.density_max)
            if governing_column.density_max is not None
            else None
        )
        if family_density_cap is not None and (
            density_cap is None or family_density_cap < density_cap
        ):
            model.Add(gross <= family_density_cap).OnlyEnforceIf(literal)
        if governing_column.density_min is not None and (
            column.density_min is None
            or governing_column.density_min > column.density_min
        ):
            model.Add(
                gross >= _ceil_scaled(lot.area_m2 * governing_column.density_min)
            ).OnlyEnforceIf(literal)

    counts: dict[str, cp_model.IntVar] = {}
    for unit_type, area_sqft in priced.items():
        area = _scale_area(area_sqft * M2_PER_SQFT)
        if area <= 0:
            raise ProgramError(f"{unit_type!r} has a non-positive area")
        # Bounded by the envelope, so the domain is finite even when the grid
        # states no dwelling ceiling - an unbounded count makes CP-SAT search a
        # space it never needs to visit.
        ceiling = residential_hi // area
        if column.max_dwellings is not None:
            ceiling = min(ceiling, column.max_dwellings)
        counts[unit_type] = model.NewIntVar(0, max(int(ceiling), 0), unit_type)

    dwellings = sum(counts.values())
    model.Add(
        sum(
            _scale_area(priced[unit_type] * M2_PER_SQFT) * count
            for unit_type, count in counts.items()
        )
        <= residential_area
    )
    if column.max_dwellings is not None:
        model.Add(dwellings <= column.max_dwellings)

    underground_stalls = model.NewIntVar(0, stalls_hi, "underground_stalls")
    above_grade_stalls = model.NewIntVar(0, stalls_hi, "above_grade_stalls")
    # Scaled rather than divided, so half a stall a dwelling stays exact and
    # the remainder rounds the way a by-law rounds it - up. One inequality for
    # both demands rather than one apiece, because the building owes a single
    # number of stalls: a half stall owed by the retail and a half owed by the
    # housing are one stall between them, and two constraints would buy two.
    #
    # This is where the commerce stops being free floor area. Three stalls per
    # thousand square feet against a stall's own 300 is very nearly a parking
    # plate for every retail plate, and a parking plate is a storey *En etage*
    # counts and *Densite* counts - so a retail floor that pays for itself
    # above grade has to earn its neighbour's keep too, and the solver may
    # prefer to dig, or to build less of it.
    model.Add(
        STALL_DEMAND_SCALE * (underground_stalls + above_grade_stalls)
        >= stall_ratio * dwellings
        + area_stall_ratio * (commercial_area + industrial_area)
    )
    model.Add(underground_stall_area * underground_stalls <= underground_area)
    model.Add(above_grade_stall_area * above_grade_stalls <= parking_area)
    # A parking level exists to hold stalls. Without this the solver would be
    # free to raise an empty one to satisfy a density *minimum* - floor area
    # the by-law would count and nobody would ever build.
    model.Add(parking_floors <= above_grade_stalls)
    model.Add(underground_levels <= underground_stalls)

    # Discounted net profit, folded to one coefficient per decision variable.
    # `pv_per_monthly_gross` is the whole proforma - twelve months, the
    # expense ratio off, the annuity over the hold, the discounted sale - so a
    # dwelling's value is its effective proforma rent through that multiplier
    # less its build cost in full, and a stall is simply its capital: it earns
    # nothing, so nothing of it survives to offset the price. Rounded once, at
    # the coefficient, rather than separately on each side - two roundings
    # apiece would show up as a tie broken the wrong way between classes this
    # close together.
    pv_per_monthly_gross = investment.pv_per_monthly_gross
    rent_premium = investment.rent_premium_factor
    underground_value = round(parking.underground_cost_cad * MONEY_SCALE)
    above_grade_value = round(parking.above_grade_cost_cad * MONEY_SCALE)
    net_value = {
        unit_type: round(
            (
                economics.monthly_revenue(unit_type)
                * rent_premium
                * pv_per_monthly_gross
                - construction.capital_cost(area_sqft)
            )
            * MONEY_SCALE
        )
        for unit_type, area_sqft in priced.items()
    }
    # The same fold, one level of aggregation up: rent and build cost are both
    # per square foot for these, so the pair is a single coefficient on the
    # area variable. `_area_coefficient` is where the per-square-foot figure
    # meets the hundredths of a square metre the area is held in. No rent
    # premium: these rates are already quotes for new space.
    commercial_value = _area_coefficient(
        non_residential.commercial_effective_per_sqft_month * pv_per_monthly_gross
        - construction.commercial_cost_per_sqft
    )
    industrial_value = _area_coefficient(
        non_residential.industrial_effective_per_sqft_month * pv_per_monthly_gross
        - construction.industrial_cost_per_sqft
    )
    model.Maximize(
        sum(net_value[unit_type] * count for unit_type, count in counts.items())
        + commercial_value * commercial_area
        + industrial_value * industrial_area
        - underground_value * underground_stalls
        - above_grade_value * above_grade_stalls
    )

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = max_seconds
    status = solver.Solve(model)
    status_name = solver.StatusName(status)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return _empty_program(
            column, lot, status=status_name, binding=(), unpriced=unpriced
        )

    units = {
        unit_type: solver.Value(count)
        for unit_type, count in counts.items()
        if solver.Value(count) > 0
    }
    chosen_parking_area = solver.Value(parking_area)
    chosen_commercial_area = solver.Value(commercial_area)
    chosen_industrial_area = solver.Value(industrial_area)
    commercial_sqft = _unscale(chosen_commercial_area) / M2_PER_SQFT
    industrial_sqft = _unscale(chosen_industrial_area) / M2_PER_SQFT
    chosen_height = solver.Value(height)
    chosen_underground_levels = solver.Value(underground_levels)
    chosen_underground_stalls = solver.Value(underground_stalls)
    chosen_above_grade_stalls = solver.Value(above_grade_stalls)
    unit_area = sum(
        _scale_area(priced[unit_type] * M2_PER_SQFT) * quantity
        for unit_type, quantity in units.items()
    )

    # The money, from the chosen program rather than from the objective, so
    # every figure is exact where the objective's coefficients were rounded.
    # The dwelling side of the gross is the proforma rent - the survey times
    # the premium - because that is the rent every dollar below is built from.
    parking_cost = parking.capital_cost(
        underground=chosen_underground_stalls,
        above_grade=chosen_above_grade_stalls,
    )
    construction_cost = sum(
        construction.capital_cost(priced[unit_type]) * quantity
        for unit_type, quantity in units.items()
    )
    commercial_cost = construction.commercial_capital_cost(commercial_sqft)
    industrial_cost = construction.industrial_capital_cost(industrial_sqft)
    total_capital = construction_cost + commercial_cost + industrial_cost + parking_cost
    gross_revenue = (
        sum(
            economics.monthly_revenue(unit_type) * rent_premium * quantity
            for unit_type, quantity in units.items()
        )
        + non_residential.commercial_monthly_revenue(commercial_sqft)
        + non_residential.industrial_monthly_revenue(industrial_sqft)
    )
    present_value = gross_revenue * pv_per_monthly_gross
    annual_stabilised_noi = (
        gross_revenue * MONTHS_PER_YEAR * (1.0 - investment.operating_expense_ratio)
    )

    return DevelopmentProgram(
        units=units,
        floors=solver.Value(floors),
        height_m=_unscale_height(chosen_height),
        footprint_m2=_unscale(solver.Value(footprint)),
        gross_floor_area_m2=_unscale(solver.Value(gross)),
        unit_area_m2=_unscale(unit_area),
        # The legacy monthly figure, restated from the chosen program - not
        # the objective, which is `npv_cad` below.
        net_operating_income=(
            gross_revenue - total_capital / construction.amortization_months
        ),
        npv_cad=present_value - total_capital,
        present_value_cad=present_value,
        annual_stabilised_noi_cad=annual_stabilised_noi,
        status=status_name,
        residential_floors=solver.Value(residential_floors),
        above_grade_parking_floors=solver.Value(parking_floors),
        commercial_floors=solver.Value(commercial_floors),
        industrial_floors=solver.Value(industrial_floors),
        commercial_area_m2=_unscale(chosen_commercial_area),
        industrial_area_m2=_unscale(chosen_industrial_area),
        underground_levels=chosen_underground_levels,
        underground_stalls=chosen_underground_stalls,
        above_grade_stalls=chosen_above_grade_stalls,
        underground_area_m2=_unscale(solver.Value(underground_area)),
        parking_cost_cad=parking_cost,
        construction_cost_cad=construction_cost,
        commercial_cost_cad=commercial_cost,
        industrial_cost_cad=industrial_cost,
        gross_revenue_cad=gross_revenue,
        binding=_binding_caps(
            column,
            lot,
            total_dwellings=sum(units.values()),
            unit_area=unit_area,
            parking_area=chosen_parking_area,
            commercial_area=chosen_commercial_area,
            industrial_area=chosen_industrial_area,
            residential_floors=solver.Value(residential_floors),
            commercial_floors=solver.Value(commercial_floors),
            industrial_floors=solver.Value(industrial_floors),
            density_cap=density_cap,
            footprint_hi=footprint_hi,
            permits_residential=column.permits_residential and bool(priced),
            res_floors_allowed=res_floors_allowed,
            usage_floors_cap=min(
                permitted_count,
                max(commercial_floors_hi, industrial_floors_hi, res_floors_allowed),
            ),
            smallest_unit_area=smallest_unit_area,
            underground_levels=chosen_underground_levels,
            underground_levels_hi=underground_levels_hi,
            height_floors_cap=height_floors_cap,
        ),
        unpriced_types=unpriced,
        zone=column.zone,
        lot_number=lot.lot_number,
    )


def _families_by_column(
    governing: Mapping[str, ZoneColumn | None],
) -> list[tuple[tuple[str, ...], ZoneColumn]]:
    """The families grouped by the column that governs them.

    Identity, not equality: two columns of one grid can state identical norms,
    and a zone governing its housing by one and its commerce by the other has
    two allowances rather than one shared between them. Grouping by value would
    silently merge them and charge a mixed building a single column's level
    rows twice over.

    A borough printing one usage per column yields one group per family; a
    single `ZoneColumn` handed to `solve_program` yields one group holding
    every family it heads, which is the shape the module had before envelopes
    existed.
    """
    groups: list[tuple[list[str], ZoneColumn]] = []
    for family, column in governing.items():
        if column is None:
            continue
        for names, existing in groups:
            if existing is column:
                names.append(family)
                break
        else:
            groups.append(([family], column))
    return [(tuple(names), column) for names, column in groups]


def _site_coverage_cap(column: ZoneColumn | ZoneEnvelope, lot: Lot) -> int:
    """*Taux d'implantation au sol max* x lot area, as a scaled footprint.

    A column stating no maximum may cover the whole parcel, which is what the
    100 stands in for - the grid's silence is not a coverage of zero.

    Its own function because two places need the same number and they must not
    disagree: `solve_program` takes it as one of the two footprint ceilings,
    and `_binding_caps` compares it against the other to say which of them
    produced the answer. Computed twice by hand, the second copy is the one
    that goes stale and starts reporting the wrong binding norm.
    """
    return _floor_scaled(
        lot.area_m2
        * (
            100.0
            if column.site_coverage_max_pct is None
            else column.site_coverage_max_pct
        )
        / 100.0
    )


def _footprint_cap_norm(column: ZoneColumn | ZoneEnvelope, lot: Lot) -> str:
    """Which of the two footprint ceilings is the binding one.

    `solve_program` caps a footprint at the lesser of *Taux d'implantation au
    sol* and the area the zone's margins leave, and "the envelope is full" is a
    different answer to the caller depending on which. A borough reporting
    `setbacks` is one where the margins, not the coverage, decide what gets
    built - and the fix for a site is a different one: a coverage cap is
    argued at the plan, a margin is argued at the lot line.

    Ties go to the coverage, which is the norm that has always been reported
    and the one a reader will recognise.
    """
    if lot.buildable_area_m2 is None:
        return "site_coverage_max"
    if _floor_scaled(lot.buildable_area_m2) < _site_coverage_cap(column, lot):
        return "setbacks"
    return "site_coverage_max"


def _binding_caps(
    column: ZoneColumn | ZoneEnvelope,
    lot: Lot,
    *,
    total_dwellings: int,
    unit_area: int,
    parking_area: int,
    commercial_area: int,
    industrial_area: int,
    residential_floors: int,
    commercial_floors: int,
    industrial_floors: int,
    density_cap: int | None,
    footprint_hi: int,
    permits_residential: bool,
    res_floors_allowed: int,
    usage_floors_cap: int,
    smallest_unit_area: int,
    underground_levels: int,
    underground_levels_hi: int,
    height_floors_cap: int | None,
) -> tuple[str, ...]:
    """Which caps the answer is pressed against.

    Reported rather than inferred by the reader, because "why is this only
    twelve units" is the first question asked of any number this module
    returns, and the candidate answers are not distinguishable from the program
    alone.

    Computed from the *caps* rather than from the solution, which matters
    because the solution is not unique in the dimensions nobody is paying for.
    Once the mix is fixed, any footprint x storeys product big enough to hold
    it is equally optimal - 13 dwellings needing 1 393.5 m2 fit just as well
    under a 280 m2 x 5 envelope as under 278.7 m2 x 5 - so reading "is the
    footprint at its maximum" off the chosen point reports whichever tied
    solution CP-SAT happened to return. What is actually true of the parcel is
    whether one more dwelling *could* have been placed, and which printed norm
    stopped it.

    The solution values that do belong here are the above-grade storeys that
    are not dwellings: the parking, the commerce and the industry. Each is
    floor area the by-law counts and the solver chose to spend on something
    else, so what is left of the density cap after them is a genuine ceiling on
    the dwellings rather than an artefact of a tie. `max_underground_levels` is
    reported on the same footing, with the caveat that it is this module's
    bound rather than anything the grid prints.

    `height_max` is reported beside `floors` and from the caps for the same
    reason the rest of them are: the height of a tied solution is as arbitrary
    as its storey count, since a storey nobody pays for can stand empty at no
    cost to the objective. What is true of the parcel is how many residential
    storeys *Hauteur en mètre* leaves room for - 15 m is five at three metres
    apiece - and whether that is fewer than the level rows would have allowed.
    Where it is, the metric cap is what built the envelope the dwellings just
    filled, and both norms are named because a reader who changes only the
    storey row will get the same answer back.

    `commercial_floor_area` and `industrial_floor_area` are reported on a
    condition of their own rather than inside the density branch, because they
    can be the answer while no printed norm binds at all: storeys the level
    rows would have allowed the dwellings, spent on space that outbid them.
    A caller reading one of them is being told that the housing figure is small
    for a reason that is in the rates rather than in the grid, which is a
    different answer from "the envelope is full" and the only one the grid
    cannot give.
    """
    binding: list[str] = []
    if column.max_dwellings is not None and total_dwellings >= column.max_dwellings:
        binding.append("max_dwellings")

    envelope_cap = footprint_hi * res_floors_allowed
    non_residential_area = parking_area + commercial_area + industrial_area
    remaining_density = (
        None if density_cap is None else density_cap - non_residential_area
    )
    residential_cap = (
        envelope_cap
        if remaining_density is None
        else min(envelope_cap, remaining_density)
    )
    if permits_residential and residential_cap - unit_area < smallest_unit_area:
        # No further dwelling of any priced class fits in the largest envelope
        # the grid allows, so the envelope is what stops the mix. Which norm
        # produced that envelope is the useful half of the answer.
        if remaining_density is not None and remaining_density <= envelope_cap:
            binding.append("density_max")
            if parking_area > 0:
                binding.append("above_grade_parking")
        if remaining_density is None or envelope_cap <= remaining_density:
            # Which of the two footprint ceilings produced this envelope - the
            # coverage or the margins. See `_footprint_cap_norm`; on a lot with
            # no buildable area passed it is always the coverage, which is what
            # this line said before that cap existed.
            binding.append(_footprint_cap_norm(column, lot))
            binding.append("floors")
            if (
                height_floors_cap is not None
                and height_floors_cap <= res_floors_allowed
            ):
                # `res_floors_allowed` is the storey row and the metric cap
                # whichever is smaller, so this is the metric one being at
                # least as tight - the storeys the envelope was built from are
                # the ones *Hauteur* left room for.
                binding.append("height_max")

    if permits_residential and residential_floors < res_floors_allowed:
        if commercial_area > 0:
            binding.append("commercial_floor_area")
        if industrial_area > 0:
            binding.append("industrial_floor_area")

    if not permits_residential and (commercial_area > 0 or industrial_area > 0):
        # A column with no dwelling in the question: what stopped the commerce
        # or the industry is the envelope's own arithmetic, read from the caps
        # the way the residential branch reads them. A column whose floors sit
        # at every storey the rows and the height leave is 'floors'; one whose
        # remaining *Densité* cannot hold another plate at any footprint the
        # coverage allows is 'density_max'; and the footprint norm names which
        # of the two ceilings built the plate.
        used_floors = commercial_floors + industrial_floors
        gross_above = non_residential_area  # residential is zero here
        if density_cap is not None and density_cap - gross_above < footprint_hi:
            binding.append("density_max")
        if used_floors >= usage_floors_cap:
            binding.append(_footprint_cap_norm(column, lot))
            binding.append("floors")

    if underground_levels_hi and underground_levels >= underground_levels_hi:
        binding.append("max_underground_levels")
    return tuple(binding)


def _empty_program(
    column: ZoneColumn | ZoneEnvelope,
    lot: Lot,
    *,
    status: str,
    binding: tuple[str, ...],
    unpriced: tuple[str, ...] = (),
) -> DevelopmentProgram:
    return DevelopmentProgram(
        units={},
        floors=0,
        footprint_m2=0.0,
        gross_floor_area_m2=0.0,
        unit_area_m2=0.0,
        net_operating_income=0.0,
        status=status,
        binding=binding,
        unpriced_types=unpriced,
        zone=column.zone,
        lot_number=lot.lot_number,
    )


def _scale_area(m2: float) -> int:
    return round(m2 * AREA_SCALE)


def _floor_scaled(m2: float) -> int:
    return math.floor(m2 * AREA_SCALE)


def _ceil_scaled(m2: float) -> int:
    return math.ceil(m2 * AREA_SCALE)


def _unscale(scaled: int) -> float:
    return scaled / AREA_SCALE


def _scale_height(m: float) -> int:
    return round(m * HEIGHT_SCALE)


def _floor_scaled_height(m: float) -> int:
    return math.floor(m * HEIGHT_SCALE)


def _ceil_scaled_height(m: float) -> int:
    return math.ceil(m * HEIGHT_SCALE)


def _unscale_height(scaled: int) -> float:
    return scaled / HEIGHT_SCALE


def _area_coefficient(per_sqft_month: float) -> int:
    """Dollars a square foot a month, as an objective coefficient.

    The area variables are hundredths of a square metre and the objective is
    ten-thousandths of a dollar, so this is the whole of the conversion between
    a rate a leasing agent would quote and a number CP-SAT can multiply. Signed
    on purpose: a rate whose build cost exceeds its rent comes through negative
    and the solver declines the storey, which is the same behaviour a dwelling
    class gets from `net_value`.
    """
    return round(per_sqft_month / M2_PER_SQFT / AREA_SCALE * MONEY_SCALE)
