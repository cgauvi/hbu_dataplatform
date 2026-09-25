# Single-family lots

Every answer the HBU chain gives is a **rental building's**. `solve_program`
earns rent per dwelling by CMHC bedroom class, charges construction per square
foot, and maximises the discounted net profit of holding the result and selling
it at a cap rate. That is the right question for a plex, a walk-up or a
mixed-use block, and the wrong one for a house lot.

Under a grid that caps the dwellings at one, the objective can only choose
*which single rental unit* to build, and the answer is always the cheapest one
that still rents: a one-storey, 600 sq ft one-bedroom. A second storey adds
cost and no rent, so the storey and height limits never bind. Lot 2 076 513 in
Sainte-Foy (zone 31234Ha, two storeys, 10 m, H1 with *Nombre de logements
max.* 1) is the case that showed it: an 84 m², 3 m high program with
`binding = ['max_dwellings']`. Nobody values a house lot that way. A house is
bought and sold rather than leased by the unit, and its value grows with its
floor area, its lot and its location.

Until that thesis exists, these pieces are **left out of the rental solve** and
the map says so. This page describes the gate as it stands and what the thesis
that should replace it would take.

## The gate (since 2026-09-25)

`hbu.single_family_zone_pieces` names the pieces off the envelopes before
anything is solved. A **(lot, zone) piece** is single-family when the building
the solver would be asked about holds at most `SINGLE_FAMILY_MAX_DWELLINGS` (1)
dwelling and no commerce or industry. "The building the solver would be asked
about" is read exactly as `_envelope_row` reads it, so the gate and the solve
cannot disagree:

- **The governing columns**, where the piece has any: the rows marked
  `governs_<family>` for a family they also permit. If a grid prints `H.1` for
  narrow lots and `H.2` from 15 m of frontage, the narrow lot is single-family
  and the wide one is not, because that is what each may build.
- **Every candidate column** where nothing governs, which is the fallback the
  solve takes too. A house zone with no measured frontage is still a house
  zone, not `no_governing_column`.

Among those columns, at least one must permit housing, none may permit
commerce or industry, and every one that permits housing must cap it at one.
The cap is `ZoneColumn.effective_max_dwellings`: the smaller of the printed
*Nombre de logements maximal* and the ceiling the usage class implies. So each
of these counts:

- Montreal's `H.1` (the count row is left blank because the class carries it).
- Quebec City's and Saguenay's `H` with `1` printed.
- Both at once.

A bare `H` with nothing printed does not count. Neither does a duplex cap, a
house over a shop, nor any column that also permits `C` or `I`: the solver
has something real to build there. *Équipements* columns are ignored, because
`program` prices no `E`. Saguenay prints `H` and `E` together on some 20,000
rows and Quebec City a separate `E` column beside the house one (31234Ha does);
both are still house lots.

What follows from the gate:

| where | what it does to a single-family piece |
| --- | --- |
| `candidate_envelopes` | leaves every row of the piece out, so nothing is solved |
| `lot_development_programs` | filters to candidates *before* the setback merge, the rectangle fit and the yard measurement, which are its setup cost. Reports `num_single_family_pieces`. A tile of nothing but house lots writes an **empty** partition and publishes it, which prunes an earlier run's programs, instead of failing |
| `lot_highest_best_use` | keeps the row, with `hbu_status = 'single_family_zone'` and nulls across the program. `num_candidates` is 0. `road_parcel` still overrides it |
| `lot_building_massing` / `lot_surface_parking` | nothing drawn, as for every piece that is not `solved` |
| `lot_redevelopment_gap` | `enhance_status = 'single_family_zone'`: the addition would be priced as rental floor too |
| `lot_investment_opportunities` | `site_thesis = 'none'`, since every thesis needs a program |
| the map | the Lot, Programme and Deal panes, the Capacity and Land-use tooltips, and the chat agent all say *single-family zone, not priced as rental* rather than "no programme solved" |

`lot_zoning_envelopes`, `lot_buildable_setbacks` and `lot_profiles` are **not**
gated. The zoning read and the buildable envelope are facts about the ground,
and they are exactly the inputs the house thesis below needs.

### How big it is

Measured on the 2026-09-01 envelopes with the function itself:

| city | pieces | single-family | pieces the solve no longer sees | what they had been getting |
| --- | ---: | ---: | ---: | --- |
| SSC | 36,660 | 14,388 | 33,503 → 19,115 (−43%) | 9,166 one-dwelling programs, 7,617 of them one storey |
| SAG | 78,479 | 24,969 | 77,078 → 52,109 (−32%) | 21,399 `infeasible` (`no_priced_unit_type`: Saguenay's CMHC grid prices no dwelling) |
| VSMPE | 27,923 | 2,195 | 26,932 → 24,737 (−8%) | 1,670 one-dwelling programs |
| CIL | 25,569 | 151 | 24,548 → 24,397 (−1%) | 13 one-dwelling programs, 101 empty |

The counts include road parcels under a house zone (1,018 in SSC, 3,290 in
SAG), which keep `road_parcel`. The candidate *rows* fall further than the
pieces on SSC (37,095 → 22,707), because Quebec City prints the house columns
several to a zone.

### Re-running

Nothing in hbu_infra changes: `hbu_status` and `enhance_status` are free text,
and only their column comments were updated. To see the gate on a partition:

1. `lot_development_programs`, then `lot_highest_best_use`,
   `lot_redevelopment_gap`, `lot_building_massing`, `lot_investment_opportunities`
   for the tiles.
2. Re-render the map tiles so the Capacity and Land-use layers carry the new
   status.

The map code reads the new status whenever it appears. A partition that has not
been re-run shows its old one-unit programs until it is.

## The thesis to build

What a house lot is worth, and what could be done with it, is a question about
**sale prices**, not rents. The pieces are all known already: every
single-family piece has its grid norms, its buildable envelope, its placeable
rectangle and its roll entry. What is missing is a price.

### Data it needs

**Transactions.** Sale price, date, lot and building, per sale. Neither the
open-data roll extracts nor Montreal's assessment units carry sale prices, so
this has to come from outside the roll. The candidate sources below have not
been checked against licence, cost or coverage:

- the *Registre foncier du Québec* (deeds of sale, one document at a time,
  paid per consultation);
- commercial aggregators of the registry, such as JLR (Equifax), which sell
  transaction feeds with the lot number attached;
- Centris MLS listings and sales, through a broker or a data licence;
- QPAREB's quarterly market statistics: median prices by sector and property
  type, with no per-lot detail. Enough to calibrate, not enough to comp.

**A cheap proxy while that is sorted out.** MAMH publishes each municipality's
*proportion médiane* and *facteur comparatif* for its roll, the ratio the
assessor's own sales analysis found between assessed and market value. Assessed
value × facteur comparatif is a median-level market value for every lot already
on the roll. It is a proxy, not a comp: it inherits the roll's lag and misses
anything the assessor did not see. But it would put a dollar figure on every
house lot without a new source.

**Comparables.** `lot_assessment_comparables` already finds the lots the roll
says are alike: same use, similar floor and lot area, nearby. With transactions
joined on lot number, the same neighbourhood becomes a set of sale comps rather
than assessment comps.

### Models it should price

In roughly the order they would be worth building:

1. **Hold, at market.** What the house is worth today, from comps or the
   proxy: the baseline every other future is measured against, as
   `hold_value_cad` is for rentals.
2. **Rebuild for sale.** Knock down, build the largest house the envelope
   allows (the storeys, the height, the coverage and the placeable rectangle
   are all on the row already), and sell it at new-build comps. That gives a
   **residual land value**: sale price less construction (the cost guide),
   soft costs, carry and a builder's margin. The spread between that and the
   hold value is the teardown thesis for houses. On 31234Ha it is the
   two-storey, 10 m house the rental solve could never propose.
3. **Enlarge for sale.** Add a storey or an extension where the grid leaves
   room, priced as the uplift in sale value per added square foot against the
   addition's cost. This is the house version of the enhancement future.
4. **Split the lot.** A lot with at least twice the governing column's
   *Largeur du terrain min* and enough area for two could be subdivided into
   two house lots. The width test is already on `envelope_assets`.
5. **Add an accessory dwelling.** Where a city's by-law authorises one, a
   second unit is rent on top of a house. Which zones allow it, and on what
   conditions, has to be read city by city before it can be a norm.

Each would get its own status and its own columns rather than bending the
rental `DevelopmentProgram` into shape. A `lot_single_family_values` asset at
the piece grain, beside `lot_development_programs` and fed from the same
envelopes, is the natural home. The gate above is the seam: the pieces it names
are the ones that asset would answer for.

### What stays open

- **Where the line sits.** One dwelling is where the rental objective
  collapses entirely. A duplex cap still leaves it something to build, so
  duplexes stay in the rental solve, although a duplex is also often bought to
  live in. `SINGLE_FAMILY_MAX_DWELLINGS` is the one constant to move if the
  house thesis should cover them.
- **A tile of nothing but house lots** writes an empty programs file, so it has
  no row to stamp `program_assumptions` on. Its gap rows then price the
  standing buildings at `InvestmentAssumptions`' module defaults, which are the
  config's defaults unless a run overrides them.
- **The setbacks step is still paid** on every house lot (`lot_buildable_setbacks`
  took 4h28m on SAG). It is kept because the rebuild-for-sale model needs the
  envelope. If that model is far off, gating the setbacks the same way is the
  next speed-up.
