# The development program — a lot's envelope, solved with CP-SAT

`urban_rag.program` answers one question: given what the *grille des usages et
des normes* lets you build on this parcel, and what CMHC says the
neighbourhood pays, **what is the most valuable thing to build?** Everything
upstream of it — the cadastre, the roll, the frontage, the setbacks, the rent
and vacancy grids — exists to fill in that one question's inputs.

The answer is an integer program, solved by Google OR-Tools' CP-SAT
(`ortools>=9.10`), and the module is
[`src/urban_rag/program.py`](../src/urban_rag/program.py). One call to
`solve_program` is one row of `silver.lot_development_programs` — hbu_infra's
[sql/017](../../hbu_infra/sql/017_silver_lot_development_programs.sql).

| | |
| --- | --- |
| module | [`urban_rag.program`](../src/urban_rag/program.py) — no Dagster imports and no I/O, the same posture `comparables` and `role_foncier` take |
| caller | [`urban_rag.hbu`](../src/urban_rag/hbu.py) `solve_envelopes`, one call per candidate envelope row |
| asset | `lot_development_programs` in [`urban_rag.hbu_assets`](../src/urban_rag/hbu_assets.py), `make programs` |
| entry point | `solve_program(column_or_envelope, lot, economics, **assumptions) -> DevelopmentProgram` |
| tests | [`tests/unit/test_program.py`](../tests/unit/test_program.py), against hand-written envelopes rather than municipal PDFs |

## Why an integer program and not a formula

Three of the printed caps interact, and they are stated in three different
units:

- the **dwelling ceiling** is a count — *Nombre de logements maximal*, plus
  the ceiling the *Habitation* class itself carries;
- the **density cap** is a floor *area* — *Densité* × lot area;
- the **site-coverage cap** is a *footprint* — *Taux d'implantation au sol* —
  which only becomes floor area once multiplied by a number of storeys that is
  itself capped, by *En étage* and by *Hauteur en mètre* beside it.

`gross floor area = footprint × floors` is a **product of two decision
variables**, which is what rules out a linear program and what
`AddMultiplicationEquality` is for. Add the requirement that dwellings come in
whole numbers and CP-SAT is the tool that fits the shape of the problem rather
than the one that happens to be installed.

**What it is handed, and what it is not.** A `ZoneColumn` already parsed out of
a grid, never a PDF. [`urban_rag.zoning_grid`](../src/urban_rag/zoning_grid.py)
produces one, and it is a separate module for a reason: `linked_documents`
flattens a grid to text for the embedding corpus and `pypdf`'s default
extraction drops the column alignment that says *which* column an `X` belongs
to, so `Tous sauf le RDC   X` cannot be attributed to the Habitation column
without the x-coordinates `extraction_mode="layout"` keeps. Keeping the two
apart is also what makes the arithmetic here testable.

**Units.** The grid is metric, the unit schedule is imperial, and the two meet
at exactly one constant — `M2_PER_SQFT = 0.09290304`. Every constraint is
built in square metres. A density ratio computed with square feet over square
metres is wrong by a factor of ten and looks entirely plausible.

**Vacancy is a percentage.** `vacancy_rates` stores rates as CMHC publishes
them — 0.2 % is `0.2`, not `0.002` — so the occupancy factor divides by 100.
Reading that column as a fraction understates revenue by two orders of
magnitude, which is the kind of error that produces a confident answer rather
than an exception.

## The decision variables

One CP-SAT model per (lot, zone). Areas are integers in hundredths of a square
metre, heights in centimetres.

| variable | domain | what it is |
| --- | --- | --- |
| `footprint` | *Taux d'implantation* × lot area, ∧ `buildable_area_m2`, ∧ `placeable_area_m2` | one plate, shared by **every** floor |
| `residential_floors` | 0 .. the storeys the level rows and *Hauteur* allow | dwelling storeys |
| `commercial_floors` | 0 unless the zone heads a `C` class | storeys of commerce |
| `industrial_floors` | 0 unless it heads an `I` class | storeys of industry |
| `floors` | *En étage min* .. *En étage max* | the three above, added up |
| `height` | 0 .. *Hauteur en mètre max* | those three storey types priced at `StoreyHeights` |
| `underground_levels` | 0 .. `max_underground_levels` (6) | dug **parking** levels — not storeys, not floor area, not footprint; dug at the **parcel's** plate (`underground_lot_share` × lot area), not the building's |
| `basement_{residential,commercial,industrial}_levels` | 0 .. `permitted_basement_levels` (1) | the sous-sol of *usage* — not storeys, **is** floor area |
| `<class>` and `basement_<class>` | 0 .. what the envelope holds | dwellings per CMHC bedroom class, above grade and below, counted apart because they are **priced** apart |
| `surface_stalls`, `garage_stalls`, `underground_stalls` | 0 .. the stalls the program could owe | the three provisions, below |
| `use_{residential,commercial,industrial}` | bool | whether that family is built at all — what makes one model do the work of three |
| `has_basement`, `digs` | bool | guards against an empty cellar and a hole deeper than its stalls |

Six of those are **products** — `residential_area`, `commercial_area`,
`industrial_area` and the three `basement_*_area` — each one `footprint ×` a
level count. They all share the `footprint` factor, which is the "one plate,
identical floors" assumption stated as algebra, and it is why `gross` is a
plain sum of three of them rather than a seventh multiplication. The dug
parking is deliberately **not** a product: its plate is the parcel's, a
constant, so "the stalls fit in the levels dug" is linear in the level count
(see [the three provisions](#parking-three-provisions-no-dominance)).

Three derived areas sit on top:

| | |
| --- | --- |
| `gross` | the three above-grade areas — *superficie de plancher* above grade |
| `basement_area` | the three cellar usage plates |
| `density_area` | `gross + basement_area`, and **what *Densité* is actually tested against** |

## The constraints

### The norms the grid prints

| norm | constraint |
| --- | --- |
| *En étage max* | `floors <= floors_max`, and per family while that family is built |
| *En étage min* | `residential + commercial + industrial >= floors_min` — owed by the **usage** storeys, so a garage may not satisfy it |
| *Niveaux de bâtiment autorisés* | **two** constraints, because the block says two things. *How many*: per governing column, `sum(that column's usage floors) <= permitted_floors_count`, and the same again for its one cellar. *And which*: the families whose column does not authorise the RDC (`GROUND_FLOOR_LEVELS` — anything but *Tous les niveaux* and *Rez-de-chaussée*) sum to at most `floors - 1`, so the ground floor goes to a usage allowed to stand on it. One inequality over all of them: a building has one ground floor, not one per column |
| *Hauteur en mètre* | `height == 3·res + 4·com + 4·ind` metres, within min and max; an underground level adds **nothing** |
| *Densité* | `density_area` within `density_min` and `density_max` × lot area |
| *Taux d'implantation au sol* | `footprint` within min and max × lot area; the dug plate is **not** in it — article 43 excludes *une partie du bâtiment qui est entièrement sous terre* |
| *Nombre de logements maximal* | `sum(dwellings) <= max_dwellings` — the printed row **∧** the ceiling the `H.n` class carries |
| *Largeur du terrain min* | not a constraint at all: it decides *which column governs*, in `select_governing_column`, before the model is built |

**The class is a norm, not a label.** `RESIDENTIAL_CLASS_MAX_DWELLINGS` reads
by-law 01-283's own definitions — H.1 is 1, H.2 is 2, H.3 is 3, H.4 is 8, H.5
is 12, H.6 is 36, H.7 unbounded — because a grid prints *Nombre de logements
maximal* only where the class leaves room to choose. In
Villeray-Saint-Michel-Parc-Extension that row is blank on all 498 columns
headed H.1, H.2 or H.3, and reading only the printed row filled a duplex
envelope with about thirteen dwellings on a 500 m² lot.

**Two ceilings on the same stack.** *En étage* counts storeys and *Hauteur*
measures them, and they are not the same cap because the storeys are not the
same height. A column printing `6` and `15` allows **five** storeys of housing
at 3 m or **three** of commerce at 4 m, and never six of anything. Both are
linear in the storey counts, so the metric cap costs the model nothing to
carry — and where it bites is the *mix*: *En étage* charges a retail plate
exactly what it charges a residential one, so commerce wins any storey its
rent can pay for; *Hauteur* charges it a third more, on top of the parking a
retail plate already owes, which is what can hand the storey back to the
housing.

**A minimum can make the model infeasible**, and that is a real answer about
the parcel rather than a bug — see [the vocabulary](#binding--why-the-answer-is-not-bigger)
below, which names which pair of rows disagreed.

**So can the parking, and so can its price, and both cases are answered
twice.** A parcel whose forced floor owes stalls it has no yard, bay or dig
for has no program at the stated ratios; a house whose one dwelling
cannot pay for the parkade it owes has an empty one. With
`waive_parking_if_empty` the solver asks the second question itself: where
CP-SAT — not one of the named contradictions — says `INFEASIBLE`, or where
the answer is `nothing_pencils`, the model is solved again under
`ParkingRules.waived` (nothing owed; the rent, the market ceilings and the
absorption kept, so whatever fits and pays is still built). A program that
builds something without the obligation was stopped by the obligation and by
nothing else, and it comes back as the building it is with `parking_waived`
set and `waived_stalls` counting the shortfall against the original ratios;
one that is still infeasible or still empty keeps its answer, so a printed
minimum and a rent that does not pay are never masked. Off by default on
`solve_program`; on by default in `hbu.ProgramAssumptions`, where the
borough's tables carry the flag on every program row, the enhancement carries
`enhance_parking_waived`, and the map's Deal pane says so before any figure.

### The three caps the grid does not print

The footprint is narrowed three times, and the order matters because whichever
is tightest is what `binding` reports:

| cap | source | what it is |
| --- | --- | --- |
| *Taux d'implantation* × lot area | the grid | an area; an absent norm means 100 % of the parcel, not 0 |
| `Lot.buildable_area_m2` | [`lot_buildable_setbacks`](assets.md) | the parcel less **this column's** four margins — also an area |
| `Lot.placeable_area_m2` | [`massing.placeable_area_m2`](massing.md) | the largest **rectangle** those margins actually hold |

The third is the one that makes the answer buildable. The first two are both
areas, and an 8 886 m² envelope shaped like a skewed parallelogram holds no
rectangle above about 5 519 m². Capping on the areas alone prices a plate that
fits nowhere on the parcel, and then reads the ground it did not take as yard
— which is how a 7 045 m² plate and 2 369 m² of asphalt came to be an
`OPTIMAL` answer on a 9 415 m² lot, satisfying every inequality in the model
with 0.89 m² to spare and describing no site that could be built.

`placeable_area_m2` is measured at the same settings
[`lot_building_massing`](massing.md) will later draw the building with, so the
program the solver prices and the program the placer draws are the same one by
construction. Before it, the massing shrank the plate afterwards while the
dwellings, the NOI and the NPV stayed on a footprint that was never available.
The trade is deliberate: a smaller program, and one that can be drawn.

**The yard is rationed off that same rectangle.** The ground a surface stall
may stand on is `lot area − placeable area` — the whole rectangle charged
whether or not the chosen footprint fills it. Deliberately conservative and
deliberately independent of `footprint`: a building smaller than its envelope
does not hand the difference back to the parking, because the rectangle is one
shape and its leftovers are not a parking lot. What that buys is a bound that
holds *before* the footprint is chosen.

## Parking: three provisions, no dominance

Every dwelling owes `stalls_per_dwelling` stalls and every thousand square
feet of commerce or industry owes `stalls_per_1000_sqft`, in **one**
inequality — a building owes a single number of stalls, so a half owed by the
retail and a half owed by the housing are one stall between them and not two,
and the remainder rounds the way a by-law rounds it, up. Both ratios are
*program* assumptions rather than norms: Villeray abolished residential
parking minima.

**A stall earns as well as costs.** `monthly_rent_cad` (120) at
`occupancy_pct` (85) a month, on the stalls the occupants would rent — at most
`market_stalls_per_dwelling` (1.0) per dwelling and
`market_stalls_per_1000_sqft` (3.0) per thousand square feet of shop — through
the same `pv_per_monthly_gross` a dwelling's rent goes through. So the model
provides at least what is owed and may build more where more pays: a surface
stall is worth several times its cost, a garage bay about breaks even, a dug
stall never pays from its rent alone. The market ceiling is the whole of what
lets the rent into the objective: without it the yard would be paved for
tenants who do not exist. `rented_stalls` and
`annual_parking_gross_revenue_cad` report it, and the parking rent is inside
the gross — the three family lines sum to the gross less it.

**And it leases the housing faster.** `absorption_saving_months` (2.0) of
lease-up at a full stall per dwelling, concave in the coverage below it
(`saving(c) = S × (1 − (1 − c)²)`: the first quarter stall saves 44 % of it,
the last 6 %), priced as the present value a shorter lease-up adds to the
dwellings' rent. Linear in the model by tranches — four slices of a quarter
stall per dwelling, each dwelling of each class assignable to each slice, the
slices bounded by the dwellings and the stalls they consume by the stalls
provided, each slice valued at the *marginal* gain — so concavity orders the
fill on its own. At 5 % two months of lease-up are about 0.4 % of a
dwelling's present value: a real term and a small one. It is exactly zero
wherever `lease_up_months` is, so the undiscounted tables in this page are
unchanged by it. `parking_coverage`, `lease_up_months_saved` and
`absorption_value_cad` report it; the value is inside `present_value_cad` and
`npv_cad` and in no NOI, being timing rather than income.

| provision | rationed by | *Densité* sees it | *Implantation* sees it | *En étage* sees it | area | $/stall |
| --- | --- | --- | --- | --- | --- | --- |
| **surface** — the yard | the land, and its **shape** | no | no | no | 300 sf | 6 105 |
| **integrated** — a ground-floor bay (`garage_stalls`) | one plate | **yes** | **yes** | no | 300 sf | 15 450 |
| **underground** — a dug level | the **parcel**: `underground_lot_share` × lot area, per level, up to `max_underground_levels` | no | no | no | 400 sf | 60 300 |

Read the last column and the ordering looks settled; read the middle ones and
it is not. Article 38 1° of by-law 01-283 excludes *une aire de
stationnement des véhicules […] située en sous-sol, de même que leurs voies
d'accès* from the *superficie de plancher* the density index is computed on,
and article 43 excludes *une partie du bâtiment qui est entièrement sous
terre* from the site coverage — so a dug stall is invisible to *Densité*, to
*Taux d'implantation* and, being below grade, to *Hauteur* and *En étage* as
well. A garage bay is floor area under the plate without being a storey, and
what it really costs the building is the floor the dwellings do not get —
`solve_program` says exactly that by putting the bays and the units in **one**
inequality against the residential plates, and bounds the bays at one plate:
more than a ground floor's worth of bays is a parking structure, which this
model does not build.

**The hole is the parcel's, not the plate's.** The dug levels used to be
`footprint × underground_levels` — the parkade confined to the building above
it — which on a narrow lot priced digging out of reach: a 100 m² plate owing
twelve stalls dug five levels deep. Article 43 is what frees it, and a real
parkade does run out under the yard, so the plate of a dug level is now
`underground_lot_share` (1.0) of `Lot.area_m2` and nothing about the building.
That also makes it cheap to model: with the plate a constant, *the stalls fit
in the levels dug* is `stall_area × stalls <= plate × levels`, linear, and the
model carries one product fewer. `underground_area_m2` is the area the stalls
actually took and `underground_plate_m2` is that over the levels — the plate
of one dug level, which may exceed `footprint_m2` and on a small house usually
does. The share is a modelling bound like `max_underground_levels`, recorded
in `program_assumptions`; 0 forbids digging.

**There is no above-grade parking deck.** A storey of stalls — the Altus
guide's `parkade_ag`, $48 125 a stall — is a storey *En étage* counts and
floor area *Densité* counts both, it was never what got built on a Villeray
lot, and every program that used to deck now bays or digs. Its two columns
(`above_grade_parking_floors`, `above_grade_stalls`) are dropped from the
tables rather than left at zero.

So the cheapest is spent first and runs out first, because what rations it is
the very thing a coverage cap takes away. A house on a big lot parks on the
yard; a house on a tight one puts a garage in its ground floor and gives up
part of a dwelling for it; only a building whose plate *and* yard are spoken
for pays to dig. Any of the three combines with any other.

**Lot 3 791 023 is the worked case.** A 233 m² parcel 7.6 m wide on Bordeaux,
zoned H.4 with a printed ceiling of four dwellings on two storeys at 60 %
coverage. No stall stands on a yard that narrow (`parkable_area_m2` is 0), so
the two stalls the four one-bedrooms owe go into the ground floor as bays:
the plate grows from the 111 m² the dwellings need to the 140 m² the coverage
cap allows, and 55.74 m² of the ground floor is garage. Nothing about them is
free — $15 450 apiece, 55.74 m² of the *superficie de plancher*, the plate at
*Taux d'implantation* — but they displace no dwelling, because the dwelling
row was already the cap and the envelope had the room. The alternative the
model priced and declined is two dug stalls on one 74 m² level under the
parcel, $89 700 dearer. What the row used to say was "two storeys of housing,
two stalls", with nothing to show where the stalls went; `floor_stack` now
carries `parking_area_m2` on the residential run, so the ground floor that is
two bays of garage reads as exactly that.

**Leaving the yard out is what made this module answer a detached house with
nothing.** A grid printing `H.1`, *En étage* 1/1 and *Taux d'implantation*
35 % permits one dwelling on one storey — no second dwelling to share a stall,
and the single permitted storey is the dwelling's own. That left an underground
level as the only provision the model knew, and one dwelling does not earn back
a $60 300 parkade stall: a lot whose zoning plainly allows a house came back as
0 m² and 0 dwellings.

**And a yard has a shape, not only an area.** `surface_stall_area × stalls +
footprint <= lot area` is an area against an area, and it is satisfied on a
parcel four metres wide where 27.87 m² of "yard" is a ribbon down one side and
no car can stand in it at any price. `Lot.parkable_area_m2` —
`massing.parking_capacity_m2`, the largest parking-shaped rectangle the parcel
holds, at least a stall's 5.5 m deep in every direction — is a second,
independent ceiling on `surface_stalls`, applied to the variable's *domain* so
a parcel measured to hold no parking prunes the option outright rather than
being told about it once per branch. `binding` reports
`surface_parking_shape` where the shape is what stopped it, because no printed
norm will say so.

Three further guards keep the parking honest:

- `underground_levels <= underground_stalls` — a level exists to hold stalls;
- while `digs`, `underground_stall_area × stalls >= plate × (levels − 1) + 1`
  — the hole is no deeper than the stalls need at the parcel's plate. Nothing
  in the objective charges for a dug *level*, only for the stalls in it, so
  without this every depth that holds the parking is equally optimal and the
  reported figure is whichever one the search happened to return; with it the
  level count is pinned and `underground_plate_m2` follows;
- `garage_stall_area × garage_stalls <= footprint` — more than a plate's worth
  of bays is not a garage any more, it is a parking structure, and the stalls
  beyond a ground floor's worth are dug.

## Below grade: two things under one building

The dug parking is parking. `basement_area` is usage. Both are dug and only
one is parked in, and that is the whole of the difference article 38 1° draws:
it excludes a below-grade **stall and its access ramp**, and it excludes
nothing else. They differ in plate as well: the cellar is flat under the
building, on `footprint`, and the parkade is on the parcel's plate.

| | *En étage* | *Hauteur* | *Densité* | *Implantation* | plate |
| --- | --- | --- | --- | --- | --- |
| underground parking level | no | no | **no** | no | the parcel's (`underground_plate_m2`) |
| sous-sol of dwellings or shops | no | no | **yes** | the ground floor's | `footprint` |

That makes the cellar the one plate in this model charged by a *single* cap,
and the cap most often not the binding one — which is exactly why it has to be
**priced** rather than merely permitted. A plate costing no storey and no
metres would otherwise be free floor area, and the solver would take one on
every parcel whose *En étage* ran out before its *Densité* did.
`BELOW_GRADE_COST_PREMIUM` (15 % dearer to build) and
`BELOW_GRADE_RENT_DISCOUNT_PCT` (15 % cheaper to lease) are what it costs
instead. `basement_residential_levels <= sum(basement dwellings)` and the
`has_basement` guard are what stop an *empty* one being dug for the density
minimum alone.

**At the module's own rates those two put a sous-sol *dwelling* just under
water**, by a margin narrow enough to be worth stating: the discount at which
a cellar unit stops paying for itself is 9.8 % for a studio, 13.9 % for a
one-bedroom, 8.2 % for a two and essentially zero for a three — every one of
them under 15. So the model digs for **shops** across the borough and for
apartments nowhere. That is an answer about Montreal construction costs rather
than about the by-law, and it is a close one: move
`below_grade_rent_discount_pct` a few points and the sous-sol apartment comes
back.

**Who may have one is two rules, not one.** A *dwelling* goes below grade only
where the grid names the level — *Inférieurs au RDC*, which is all
`BASEMENT_LEVELS` holds, and which 91 of Villeray's 1 555 columns mark.
Commerce and industry go there under that row *or* under *Tous les niveaux*
(`UNRESTRICTED_LEVELS`), on the reading that a column confining a shop to no
floor in particular has not excluded the floor beneath it: the stock room
under a store is the same usage as the store. Somebody's home is not, and a
grid that means to allow one says so.

`BASEMENT_LEVELS_ALLOWED = 1` is the module's count and not the by-law's — the
*Niveaux* block says which levels a usage may occupy and never how many.
`basement_levels_allowed=NO_BASEMENT` asks what a parcel is worth built
entirely above grade.

## The objective: discounted net profit

The question a developer actually asks of a parcel is not "what earns the most
a month" but "what is worth the most to build". So the objective is

```
maximise   Σ dwellings × (proforma rent × pv_per_monthly_gross − build cost)
         + Σ non-residential area × (effective rent × pv_per_monthly_gross − build cost)
         − Σ stalls × capital cost per stall
```

Every term is linear in the same decision variables, and the whole proforma
folds into **one multiplier**, `InvestmentAssumptions.pv_per_monthly_gross`:

| step | field |
| --- | --- |
| a month of effective gross rent → a year | `MONTHS_PER_YEAR` |
| gross → stabilised NOI | `operating_expense_ratio` (0.35) |
| NOI → present value over the hold | `discount_rate_pct` (5), `hold_years` (25) |
| plus the discounted sale that ends it | `terminal_cap_rate_pct` (4.5) |
| pushed out by the build and half the lease-up | `construction_months`, `lease_up_months` |

At the defaults with no delay a dollar a year of NOI is worth **$20.66**
today, against the $25.00 the old undiscounted straight-line amortisation
implicitly paid for it. The `lot_development_programs` asset runs 18
construction months and 6 of lease-up, which pushes the stream out 1.75 years
and takes about **9 %** off — and that is exactly what a rebuild gives up
against a building already earning, which is why
[the three futures](site-theses.md) discount with different factors.

**Costs are undiscounted and in full**, because they are spent at the start
where a dollar is a dollar: dwellings at `residential_cost_per_sqft` over the
`UNIT_AREAS_SQFT` schedule, non-residential floors per *gross* square foot,
stalls at `ParkingRules`' per-stall prices. A stall subtracts its capital
outright — it earns nothing, so nothing of it survives to offset the price.

**Rents.** CMHC surveys the *standing stock's* average rent and a proforma is
priced at what a new building leases for, so `new_build_rent_premium_pct`
(30 %) is the stated gap — applied to the dwelling rents **only**, since the
non-residential rates are already market quotes. Set it to zero to price the
proposal at the stock average, which is the conservative reading.

**Why the revenue is not scaled by area.** An earlier objective maximised
`area_sqft × rent × (1 − vacancy)`, which is dollars-times-square-feet and was
only ever a proxy for "bigger is better" — a three-bedroom showed $2 029 800
of monthly "revenue" against $1 220 of monthly construction, so a real cost
netted against it would have been rounding error. CMHC publishes rent per
dwelling per month, so that is what the revenue is, and the unit schedule
earns its keep on the **cost** side where a square foot genuinely is priced.
The mix that produces is different, and that is the point: rent per square
foot falls as dwellings get larger while construction cost per square foot
does not, so a binding envelope fills with the class that earns most per
square metre rather than the class that is simply biggest.

A class whose rent does not cover its construction gets a **negative
coefficient** and the solver declines to build it. The same is true of a
commercial or industrial storey. A parcel where that is true of everything
comes back with nothing at all and `binding == ("nothing_pencils",)`, which is
an answer about the parcel: at these rents and these rates, nothing pencils.

**What the objective does *not* net out** is everything nobody handed this
module: land, soft costs, demolition, financing during construction, and any
revenue the stalls themselves might earn. It is the value of the completed
building less the cost of building it, and no further. Land, deliberately, is
in neither side of [the gap](site-theses.md) either — the owner holds it in
both futures.

`AMORTIZATION_MONTHS` (300) survives for one purpose:
`DevelopmentProgram.net_operating_income` still reports the straight-line
monthly figure the platform's other tables read, computed from the chosen
program rather than maximised. `UNDISCOUNTED_INVESTMENT` is the old
objective's exact prices — zero discount, zero expenses, no terminal value, no
premium — under which the argmax is what it always was, and the tests that pin
cap arithmetic use it so they stay about the caps.

## A piece of ground, not a lot

Before any of what follows: **what the solver is handed is not always a whole
parcel.** A zoning boundary does not have to follow a lot line, and on a large
lot it usually does not. Lot 1 740 794 in Villeray-Saint-Michel-Parc-Extension
is 27 044 m² with 24 596 of them in H04-072, which permits H.7 to eight
storeys, and 2 440 in C04-083, which permits C.4 and H to six. Those are two
development sites: two envelopes, two street frontages — the commercial strip
has the 19.8 m of Jarry and the housing behind it 15.2 m of D'Hérelle — and two
answers.

The platform used to answer once. `hbu.governing_zone` kept the best-covered
zone, gave it the whole parcel, and dropped the rest before a solver saw them,
so the eight storeys were priced over 2 440 m² H04-072 does not reach and the
C.4 was never priced at all. Over that borough it is 1 862 split lots and 205
ha of ground on secondary pieces that the primary zone's grid answered for.

`silver.lot_zone_pieces` is the grain that fixes it: one row per (lot, zone),
carrying the clipped polygon, its own area, the street *that piece* faces, and
its share of what already stands on the parcel. `Lot.area_m2` is
`piece_area_m2`; `Lot.frontage_m` is the piece's own rank-1 edge. Everything in
this document is about one piece, and on the great majority of lots — one zone
covering a parcel whole — the piece is the lot and nothing reads differently.

## A zone, not a column

A grid states its usages **across** columns, not within one. All 1 463 parsed
columns of Villeray-Saint-Michel-Parc-Extension carry exactly one usage code,
so a zone permitting housing and commerce says so in two columns — and reading
a column at a time can only ever describe a *pure* building. Retail at grade
with dwellings above is the ordinary form of a Montreal commercial street, and
it is not the maximum of two pure programs.

`ZoneEnvelope` is the whole zone as one rule-set: the governing column of each
family, picked by `select_governing_column` on *Largeur du terrain min*, with
the most permissive column breaking a tie among those at the same rung. Ranking
by width alone left the winner to the order the grid happens to be printed in
— ascending class order — so the duplex column won on 14 937 of Villeray's
lot × zone pairs and the borough's stated capacity came out 23 045 dwellings
short of what the code permits.

`solve_program` takes one envelope and **splits the floor area between the
families itself**. Each column's norms bind the whole building only while the
family it heads is actually built, through the `use_*` literals — reified both
ways, so the solver may neither claim a family is absent while standing levels
of it nor pay a column's caps for a family it did not build. Three
consequences:

- a mixed building answers to the **intersection** of the columns it draws its
  usages from, never the looser of them;
- the pure programs stay **feasible points of the same model**, so the answer
  is never worse than solving each column apart would have been;
- `solve_program` is never asked twice.

Across the 425 solvable zones of Villeray-Saint-Michel-Parc-Extension the
mixed model is unchanged on 373 and higher on 48, by a median of $136 000.

A single `ZoneColumn` is still accepted and is wrapped by
`ZoneEnvelope.single`, solving exactly what it always did. A column heading
none of the three priced families raises `ProgramError`: *Équipements
collectifs* is deliberately unpriced, because a school or a clinic is not
something a proforma rents by the square foot.

**And the mix comes back as income, not only as floor.** The answer carries
`residential_gross_revenue_cad`, `commercial_gross_revenue_cad` and
`industrial_gross_revenue_cad` beside the total they sum to — annualised into
`annual_*_gross_revenue_cad` on the table, and netted into
`hbu_residential_noi_cad` and its neighbours one step downstream. They are
carried rather than derived because the two ways of asking "how much of this
building is retail" give very different answers: at the rates below a square
foot of commerce earns about four times what a square foot of housing does, so
a single retail storey under five of flats is a sixth of the floor and very
nearly half the rent. The cellar plates compound it — they are rented at
`BELOW_GRADE_RENT_DISCOUNT_PCT` under the storey above and stand outside
`commercial_area_m2` entirely — so no arithmetic on the area columns
reconstructs the split.

What reads it is [`urban_rag.proforma`](site-theses.md): a lease-up and an
exit cap rate are both weighted by which family earns the income, and a mixed
building priced on the dwelling side of either is being priced as a building
it is not.

**Where the non-residential storeys go: nowhere in particular, with one
exception.** They are plates in the same stack and the model has no notion of
*which* storey a plate is — the *Niveaux* block is marked per column, not per
usage, so nothing in the grid says the shop belongs at the RDC. That
overstates what a single mixed-use column can really be built as, and it is
the assumption to revisit before a mixed answer is taken seriously.

The exception is the ground floor, and it is the one level the block *does*
name. A column marked *Tous sauf le RDC* is five storeys **and** a level it
may not be on, and only the second of those keeps a lone `H` column off the
RDC — `permitted_floors_count` is satisfied by five storeys of housing
standing on levels 1 to 5. So the families their column keeps off the ground
floor sum to at most `floors - 1`, and `FLOOR_STACK_ORDER` — commerce, then
industry, then housing, bottom upwards — puts something authorised there in
the storey they leave. Every level above the first is still interchangeable.

**Rebuilds only.** An enhancement (`retained`) is a building that already
stands, on a ground floor already occupied and already lawful — as of right,
or as a *droit acquis* where the zoning changed under it — and the storeys
this solve chooses go on **top** of it, above the RDC by construction. Holding
the addition to the rule would ask the owner of a plex to put a shop under it
before adding a storey, and on a zone authorising nothing else it would refuse
the enhancement outright. So neither the constraint nor
`no_usage_permitted_on_ground_floor` applies where a building is retained.

## Integers, and the four scales

CP-SAT is an integer solver, so every quantity is scaled. Each scale is set
from the rounding error it has to keep out of the answer:

| scale | unit | why |
| --- | --- | --- |
| `AREA_SCALE = 100` | hundredths of a m² | rounding a 46.45 m² studio to 46 loses ~1 % of every unit, which compounds into a floor's worth of slack in the density constraint |
| `HEIGHT_SCALE = 100` | centimetres | a grid prints *Hauteur* to the decimetre at best, and a storey height is an assumption; a centimetre never reaches the answer |
| `MONEY_SCALE = 10 000` | ten-thousandths of a dollar | a square metre of industrial floor nets about twenty cents a month and enters per *hundredth* of a m² — a fifth of a cent, which rounds to a whole cent with over a percent of error and can tip a close choice |
| `STALL_DEMAND_SCALE = 1 000 000` | millionths of a stall | the non-residential ratio is charged against an area held in hundredths of a m² — about a three-thousandth of a stall apiece, which at hundredths would round to nothing |

Maxima **floor** and minima **ceil** when they are scaled, so a rounding can
never let a building exceed a cap or fall short of a minimum.

## What comes back

`DevelopmentProgram` — a frozen dataclass, one per solve. The asset flattens
it with `hbu.program_row`; the whole shape is in
[assets.md](assets.md) and in the SQL. The parts worth knowing:

| | |
| --- | --- |
| `units` / `basement_units` | dwellings by CMHC class, whole building / the part below grade. `basement_units` is a **subset**, not a second mapping to add |
| `floors`, `height_m` | above grade only; the numbers *En étage* and *Hauteur* are tested against |
| `footprint_m2` | one plate |
| `gross_floor_area_m2` | *superficie de plancher* above grade |
| `density_floor_area_m2` | that plus the cellar's usage plates — **the number *Densité* was tested against** |
| `underground_area_m2` | dug parking, the stalls' own area: built, paid for, in no cap |
| `underground_plate_m2` | that over `underground_levels` — the plate of one dug level, bounded by the parcel and not by `footprint_m2`, which it may exceed |
| `garage_area_m2` | inside `gross_floor_area_m2`; `unit_area_m2` falls short of the plate by exactly this — the integrated bays, in the ground floor |
| `surface_area_m2` | on the yard: not floor area, not footprint, not under the building — the one parking area that is not part of a building at all, which is why [the massing draws it as a second polygon](massing.md) |
| `npv_cad` | **the objective**: `present_value_cad` less `total_capital_cost_cad` |
| `annual_stabilised_noi_cad` | the figure the discounting was applied to, and the one to put beside a standing building's |
| `net_operating_income` | the legacy monthly figure, from the chosen program rather than maximised |
| `binding` | which caps the answer is pressed against — below |
| `unpriced_types` | bedroom classes CMHC suppressed, which the solver was not allowed to build |
| `retained_*` | what an [enhancement](#the-enhancement-solve) kept |

**Money figures are computed from the chosen program, not from the
objective**, so every one is exact where the objective's coefficients were
rounded.

### `floor_stack`

`floor_stack(program)` re-cuts the same numbers by storey: one entry per
**run** of identical levels, bottom upwards, as jsonb on the row. A
fifteen-storey tower over retail is two entries, not fifteen — that is "one
plate, identical floors" arriving at its conclusion. The one plate that is not
the building's is the dug one: the below-grade parking run's `floor_plate_m2`
is `underground_plate_m2`.

Level 1 is the *rez-de-chaussée* and there is no level 0; dug levels are −1
downwards. `counts_as_floor_area` is false on the below-grade **parking** run
and true on the below-grade **usage** runs, which is the one place two entries
at the same sign of level answer to different caps. Every entry carries every
key, so `jsonb_array_elements` over the column needs no branch. The surface
stalls are not in the stack — a stall on the yard is in no storey — so
totalling `stalls` over it counts what is parked *in the building*: the dug
stalls on the below-grade parking run and the garage bays on the residential
run, in its first level, which is the ground floor. `parking_area_m2` on
every entry says how much of that run's floor the stalls take — the whole of
a dug run, `garage_area_m2` on the housing, 0 elsewhere — so a two-storey
house with two bays reads as one residential run whose ground floor is two
bays of garage rather than as two storeys of housing with stalls attached.

`FLOOR_STACK_ORDER` (commerce, industry, housing) is a **reporting convention
and nothing more**: the solver counts storeys by type and never places one.
Changing it changes a drawing, never a number.

### `binding` — why the answer is not bigger

"Why is this only twelve units" is the first question asked of any number this
module returns, and the candidates are not distinguishable from the program
alone. So `binding` is computed **from the caps** rather than from the
solution — once the mix is fixed, any `footprint × storeys` product big enough
to hold it is equally optimal, and reading "is the footprint at its maximum"
off the chosen point reports whichever tied solution CP-SAT happened to
return. More than one name can appear at once.

| name | meaning |
| --- | --- |
| `nothing_pencils` | the solve was optimal and the optimum was to build nothing. **Not** the same as a zoning column that permits nothing — the envelope is whatever the grid prints, and what is zero is the best program inside it |
| `max_dwellings` | the printed row, or the class ceiling, is reached |
| `density_max` | no further dwelling fits in what *Densité* leaves — the garage bays counted beside the dwellings, being floor out of the same plates |
| `site_coverage_max` | the plate is at *Taux d'implantation* |
| `setbacks` | the plate is at what the zone's margins leave — a borough reporting this is one where the margins, not the coverage, decide what gets built |
| `placement` | the plate is at the largest rectangle those margins hold. Neither printed norm decided it; the *shape* of the ground did, and the fix is a different building rather than a variance |
| `floors` | every storey the level rows allow is used |
| `height_max` | *Hauteur en mètre* built the envelope the dwellings just filled — reported beside `floors`, because a reader who changes only the storey row gets the same answer back |
| `commercial_floor_area` / `industrial_floor_area` | storeys the level rows would have allowed the dwellings, spent on space that outbid them. The housing figure is small for a reason that is in the **rates**, not in the grid — the only answer the grid cannot give |
| `ground_floor_excluded` | a storey went to the ground floor because the families that wanted it may not stand there — on a rebuild; an enhancement keeps the ground floor it has. Neither a printed cap nor a rate: a *Tous sauf le RDC* column building five storeys under a six-storey grid is being told the sixth is at grade and is somebody else's, and that changing *En étage max* will not hand it back. The row to change is the *Niveaux* one |
| `basement_unbuilt` | the storeys are full and the cellar the level rows allow is standing empty: there *is* one more plate here, neither storey cap would charge for it, and it did not pay for itself |
| `basement_levels` | the cellar is full at `BASEMENT_LEVELS_ALLOWED` |
| `max_underground_levels` | the hole is at this module's own bound, not a printed one |
| `surface_parking_shape` | the yard has no room for one more stall **in the shape a stall needs**. The answer to "why is this house paying for a parkade" — the cheapest parking in the model ran out on the parcel's geometry rather than on its area |
| `yard_full` | the parcel has no ground left once the building's own rectangle is taken off it. Reported alongside `surface_parking_shape` where both hold: they are two different exhaustions |

Nine further names are returned with an `INFEASIBLE` status and an otherwise
empty program, each naming *which pair of rows* disagreed, because the fix
differs:

| name | what contradicts what |
| --- | --- |
| `height_range` | *Hauteur* min above *Hauteur* max |
| `height_max_below_floors_min` | *En étage min* demands storeys *Hauteur max* has no room for — a shorter `StoreyHeights` can resolve this and cannot resolve the next one |
| `floors_min_exceeds_permitted_levels` | *En étage min* above what the level rows allow, on **every** governing column |
| `no_usage_permitted_on_ground_floor` | no governing column authorises a usage on the RDC — every one is marked *Tous sauf le RDC*, *Immédiatement supérieur au RDC* or *Inférieurs au RDC* — and the grid either demands a storey (*En étage min*) or allows no cellar either. A building of storeys has a ground floor and this one has nothing that may occupy it. The case left out is deliberate: no storey demanded and a cellar allowed is not a contradiction, it is a sous-sol |
| `site_coverage_range` | *Taux d'implantation* min above max |
| `buildable_area_below_site_coverage_min` | the column is coherent; the margins leave less than its coverage minimum demands |
| `placeable_area_below_site_coverage_min` | the margins leave area enough, and no *rectangle* inside them does |
| `no_priced_unit_type` | CMHC priced no bedroom class and the zone authorises nothing else |
| `retained_footprint_exceeds_cap` | the standing building's plate is larger than the caps now allow — a non-conforming building the grid would not let stand again |

### Statuses

`OPTIMAL` and `FEASIBLE` are `solved`. `FEASIBLE` or `UNKNOWN` means
`max_seconds` (10 s by default) was reached — the asset counts both as
`num_not_optimal`, so a limit set too low is visible rather than silent.
`INFEASIBLE` is the solver's answer about a parcel. `ERROR` is **not** a
CP-SAT status and is deliberately unlike one: it is the model failing to be
*built* at all, with the exception text in `solve_error`, and a run reporting
any is a stale parquet rather than a fact about the parcels.

A row with `parking_waived` set is `solved` — its `status` is the second
solve's — and is the one kind of solved row whose stall count is *what fit
and paid* rather than *what is owed*. `waived_stalls` is the shortfall; the
assets count such rows as `num_parking_waived` (and the gap's enhancements as
`num_enhancements_parking_waived`), and before the waiver existed every one
of them was in `num_infeasible` or `num_empty_programs`.

## The knobs

Everything below is a frozen dataclass with module-constant defaults, so
`solve_program(column, lot, economics)` gets exactly the program this page
describes. Every one of them travels into the row as `program_assumptions`
jsonb — the platform's rule for stated assumptions is that the row records the
value that produced it.

| object | what it prices | notable defaults |
| --- | --- | --- |
| `UnitEconomics` | CMHC's two grids: rent a month, vacancy in percent | from `average_rents` and `vacancy_rates`; a suppressed class has **no key**, which is neither zero nor free |
| `NonResidentialEconomics` | commerce and industry, per square foot per **year** | $80 and $30 asking, 7 % vacancy each — overridden by the borough's [surveyed rents](commercial-rents.md) where that partition exists |
| `ConstructionCosts` | build rates per square foot | $257.50 residential (Altus `condo_wood` midpoint), $300 commercial, $200 industrial, +15 % below grade |
| `ParkingRules` | ratios, stall areas and per-stall prices; what a stall earns and the market ceiling on the stalls that earn; the lease-up saving; how deep and how wide the hole may go | 0.5 per dwelling, 3.0 per 1 000 sf; the three rates in the table above; `max_underground_levels=6`, `underground_lot_share=1.0`; $120 a month at 85 %, at most 1.0 per dwelling and 3.0 per 1 000 sf; 2.0 months saved at full coverage |
| `StoreyHeights` | floor to floor by what fills the storey | 3 m residential, 4 m commercial and industrial |
| `InvestmentAssumptions` | what a stream of rent is worth | 5 % discount, 25-year hold, 4.5 terminal cap, 0.35 OPEX, +30 % new-build premium, −15 % below grade |

None of those is a norm and none is surveyed except the CMHC rents, which is
why every one is a field. `ProgramConfig` on the asset exposes them all;
`hbu.ProgramAssumptions` is the object they are bundled into.

Four constants exist so a test can strip one term out and stay about
something else: `NO_PARKING`, `NO_CONSTRUCTION_COST`, `NO_NON_RESIDENTIAL` and
`NO_BASEMENT`, plus `UNDISCOUNTED_INVESTMENT` for the old objective's exact
prices.

### The enhancement solve

Pass a `RetainedBuilding` and the same model answers a different question: not
"what would you build here" but "what would you **add**". The standing
building is a fixed lower bound on the plate and the usage storeys, at most
`max_added_storeys` go on top, nothing is dug and no bay is carved out of the
ground floor — the addition parks on the yard or not at all — and
the new floor is costed at `addition_cost_premium` (1.5) times the rates.
Every money figure that comes back is the **addition's own**, since a constant
moves no argmax. That is the *enhance* branch of
[the three futures](site-theses.md), run by `hbu.solve_enhancements` inside
`make hbu`.

## Running it

```
make programs DATE=2026-09-01 NEIGHBORHOOD=villeray-saint-michel-parc-extension
```

It reads `lot_zoning_envelopes`, `lot_buildable_setbacks`, `average_rents`,
`vacancy_rates` and `commercial_rents`, joins on `placeable_area_m2` and
`parkable_area_m2`, then runs one model per candidate row. A borough is tens
of thousands of models and nearly all of them solve in milliseconds.

**A failed solve costs its row, not the borough.** A `ProgramError` — a lot of
zero area, a grid whose two coverage rows contradict each other — is recorded
in `solve_error` and the row comes back unsolved. The ones that fail are the
interesting ones; failing the partition would hide them all behind the first.

The run metadata is what to read afterwards. Beyond the totals:

| metric | what a bad number means |
| --- | --- |
| `num_not_optimal` | `max_seconds` was reached. A handful is fine; a third of the borough is a time limit set too low |
| `num_solver_errors` | above zero is a stale parquet — `solver_ready` promised this would not happen |
| `num_without_buildable_area` | those rows are capped on *Taux d'implantation* alone, which overstates a shallow parcel. A few percent is ordinary; the whole borough means [`lot_buildable_setbacks`](assets.md) has not run |
| `num_without_placeable_area` | the same for the shape cap: those plates may fit nowhere |
| `num_unparkable_lots` | parcels no car can stand on; their programs had to dig or bay the stalls |
| `num_empty_programs` | `nothing_pencils` — nothing the caps allow earns back what it costs |
| `num_parking_waived` | programs that only exist with their stalls waived — each stands on a parking variance of its `waived_stalls`. Before the waiver every one of these was in `num_infeasible` or `num_empty_programs`; a jump here is a borough of forced plates with no yard, or of houses that cannot pay for a parkade |
| `total_rented_stalls`, `annual_parking_revenue_millions` | what the parking earns over the borough, on the stalls the occupants would rent |
| `total_basement_dwellings` | expected to be **zero**: a cellar dwelling does not pay for itself at these rates. Above zero means a rent or a cost moved |
| `binding_caps` | the histogram of the vocabulary above, over the borough |

## What it does not model

Stated here rather than left to be discovered:

- **One footprint for every floor above grade, and for the cellar.** The dug
  parking is the one plate that is not the building's — it runs out under the
  yard to `underground_lot_share` of the parcel, which is what article 43
  permits — but a real sous-sol of usage runs out under the yard too, and this
  one does not. Modelling that needs a second `footprint` variable; the
  parkade got away without one because its plate is a constant.
- **The hole may reach the lot line.** `underground_lot_share` is 1.0, the
  by-law's reading, and a parkade wall does not stand on a lot line. A caller
  wanting a margin passes less; nothing here measures one.
- **A storey is one use.** Never dwellings *and* commerce, and never a plate
  half of one and half of the other.
- **Storeys are interchangeable within a column.** Nothing says the shop is at
  grade. A grid that means "commerce at the RDC, housing above" says it in two
  columns with different *Niveaux* rows, and two columns are two envelopes.
- **The residential build rate is charged against the unit schedule**, so the
  corridors, lobbies, stairs and shafts are not priced at all. The
  non-residential rates are charged against gross area and do pay for them.
  This understates the build on any real plan, and is the assumption to
  revisit before a number here goes to a lender.
- **`condo_wood` for every building.** A lot whose grid allows a tower is
  under-costed: the band is the solver's *answer* rather than its input, so it
  cannot be picked off the storey count without branching on a decision
  variable. Pass `ConstructionCosts(residential_cost_per_sqft=...)` where the
  structure is known.
- **No land, no soft costs, no demolition, no construction financing, and no
  parking revenue.**
- **`Largeur du terrain min` is tested against the frontage**, not against a
  width. They differ on a wedge-shaped parcel; see
  [street-frontage.md](street-frontage.md).

## Where it sits

| | |
| --- | --- |
| upstream | [`lot_zoning_envelopes`](assets.md) (the norms), [`lot_buildable_setbacks`](assets.md) and [`massing`](massing.md) (the two shape caps), [`lot_frontage`](street-frontage.md) (which column governs), [CMHC](cmhc-surveys.md) (dwelling rents and vacancy), [commercial rents](commercial-rents.md), [the Altus guide](construction-costs.md) (build rates) |
| downstream | `lot_highest_best_use` (one program per lot), `lot_redevelopment_gap` and [the three futures](site-theses.md), [`lot_investment_opportunities`](opportunities.md), [`lot_building_massing`](massing.md) |
