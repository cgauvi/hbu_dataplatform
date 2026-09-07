# Why a site is acquirable

[`gold.lot_investment_opportunities`](opportunities.md) files every lot under an
**investment thesis** — what you would build — and ranks the under-built ones
within it on yield on cost. That answers *what*. It does not answer *why this
parcel and not the one beside it*: why it might be on the market, what has to
be cleared off it first, and what that clearing costs.

The second axis of the same table answers that. `site_thesis` is one of four
values plus `none`, each a predicate over facts the platform already holds —
the roll's year built and storey count, the solver's storeys and footprint,
the CUBF use code, and the grid's own *Patrimoine* rows — and each carrying its
own cost into its own yield. Nothing is re-solved; this is a classification
and a few divisions over four parquet files, the same posture the first axis
takes.

```bash
make opportunities DATE=2026-09-01 NEIGHBORHOOD=VSMPE
make opportunities DATE=2026-09-01 NEIGHBORHOOD=VSMPE TEARDOWN_MAX_YEAR=1945 SITE_TOP_N=10
make opportunities DATE=2026-09-01 NEIGHBORHOOD=VSMPE REQUIRE_POSITIVE_NPV=false
```

The arithmetic is in [`urban_rag.opportunities`](../src/urban_rag/opportunities.py)
(`SiteRules`, `assign_site_thesis`, `rank_site_opportunities`); the joins are in
[`urban_rag.opportunity_assets`](../src/urban_rag/opportunity_assets.py); the
columns are hbu_infra's `sql/021`; the grid rows are read by
[`urban_rag.zoning_grid`](../src/urban_rag/zoning_grid.py) and carried by
`silver.zoning_grid_columns`.

## The fact that shapes every thesis

In Villeray the zoning envelope, not the building, is the binding constraint.
Storey headroom — the solver's storey count less the roll's — on VSMPE's
2026-09-01 partition:

| headroom (storeys) | assessed lots |
| --- | --- |
| −1 or less | 2,702 |
| 0 | 14,986 |
| 1 | 2,557 |
| 2 or more | 575 |

Over 85% of the stock cannot add a floor. A screen on "old building" alone
would surface like-for-like replacements, not development plays. So the
teardown thesis carries a headroom term, the improvement thesis *is* the
headroom, and a lot with none of it can only be a brownfield or an infill.

## The four theses

Resolved in this order when more than one holds; the four `is_*_site` booleans
keep the ones that also fired, so a lot filed under brownfield can still be
read as a teardown.

| `site_thesis` | when | what is cleared |
| --- | --- | --- |
| `brownfield` | the dominant use standing on the lot is a contamination-risk activity, and the governing zone is not a heritage sector | the building, then the ground: characterisation and remediation |
| `teardown` | a building old enough to be presumed obsolete, filling little of its envelope, under a grid allowing storeys above it, outside a heritage sector | the building |
| `infill` | nothing stands on the lot and the solver has a program | nothing |
| `improvement` | the building stays and gains a storey on its footprint or an annex on the ground the solver would cover | nothing — the addition is the whole cost |
| `none` | no condition held | — |

Brownfield leads because it changes the cost side the most and a gas station
is a gas station whatever else is true of it; teardown before infill because a
lot with a building on it is not empty; improvement last because keeping the
building is what remains once nothing argues for removing it.

Every threshold below is config on `OpportunityConfig`, recorded on every row
in `screen_assumptions`, and settable from `make opportunities`.

### Teardown: an obsolete improvement under an unused envelope

All of these at once, on a lot the gap table calls under-built:

- `existing_year_built` ≤ `teardown_max_year_built` (1960) — the inter-war and
  post-war plexes; the 1960s stock is mostly built to its envelope anyway.
- `built_share` ≤ `teardown_max_built_share` (0.40) — the standing floor is
  under two-fifths of what the solver would put there.
- `storey_headroom` ≥ `min_storey_headroom` (2) — below two a teardown is a
  like-for-like replacement.
- not `is_demolition_restricted` — see [Heritage](#heritage-and-the-piia).

Sized on VSMPE 2026-09-01, with the verdict positive (rebuilding beats holding,
discounted):

| screen | lots | median yield on cost | NPV gain | assessed value | mean lot |
| --- | --- | --- | --- | --- | --- |
| pre-1945, ≤50% built, headroom ≥2 | 46 | 4.6% | $13.5M | $26M | 290 m² |
| **pre-1960, ≤40% built, headroom ≥2 (the default)** | 106 | 4.4% | $37M | $74M | 418 m² |
| reference: every NPV-positive lot | 1,705 | 3.8% | $570M | $1.7B | 728 m² |

All 106 come out residential, and they cluster on the C04 artery grids where
a six-to-eight-storey cap sits over one- and two-storey stock: lot 2 214 147
(1955, two storeys under six, 10% of its envelope, +$3.6M), 2 214 747 (1936,
+$2.2M), 2 249 734 (a 1949 auto-services shed under an eight-storey grid).
These yields are *before* demolition; the table's `site_yield_on_cost_pct`
carries it.

A second, independent obsolescence signal is in the roll but not yet on the
lot: the building's share of assessed value (`rl0403a` over `rl0404a`). About
665 VSMPE lots are land-dominant (building under 30% of the total), 230 of
them under-built and 105 NPV-positive, and only 9 overlap the age screen —
a different population (parking, garages, old commercial). Carrying
`land_value` and `building_value` onto the lot is the same per-lot sum
[`lot_assessed_values`](assessment-roll.md) already makes for the total, and
is the obvious next term for this thesis.

### Brownfield: a contamination-risk use, and the zoning now allows housing

The use code alone, deliberately **not** screened on `is_underbuilt`: a gas
station built to its envelope is still a conversion play, because what changes
is the use rather than the floor. The lot is filed when
`existing_dominant_use_code` starts with one of `brownfield_use_prefixes`, the
solver has a program for it, and it is not demolition-restricted.

The default prefixes, and where the presumption comes from:

| prefix | MEFQ family | why |
| --- | --- | --- |
| `2`, `3` | industries manufacturières (2000–3999) | most of RPRT Annexe III is manufacturing sub-sectors — paints, metal fabrication, chemicals |
| `42` | transport par véhicule automobile (yards, bus and truck garages) | fleet garages are Annexe III (SCIAN 811199, limited to fleets and dealers) |
| `487` | récupération et triage | salvage yards |
| `55` | vente de véhicules, stations-service | *postes de distribution de carburant à équipements pétroliers à risque élevé* is an Annexe III category by name |
| `6231` | nettoyage à sec | not in Annexe III; perchloroethylene is a Phase I trigger by practice |
| `63` | entreposage | not in Annexe III; bulk and cold storage are Phase I triggers by practice |
| `64` | services de l'automobile (garages) | ordinary repair (SCIAN 81111) is *not* in Annexe III; every lender's Phase I flags it anyway |

The regime: LQE art. 31.51 requires a characterisation within six months of an
Annexe III activity ceasing, art. 31.53 before any change of use — including
to housing — and a rehabilitation plan where the criteria are exceeded. The
list is by SCIAN, and the codebook snapshot
[`cubf_use_codes`](assessment-roll.md#the-use-code-and-what-it-says) carries
the SCIAN correspondence per CUBF code, so a join can replace the prefix list.
Until it does, the list is stated so a screen can be read back against it.

Sized on VSMPE 2026-09-01:

| step | lots |
| --- | --- |
| dominant use in a risk family | 417 |
| of which the governing envelope permits housing | 111 |
| of which the solver's program is housing or mixed | 109 |
| of which rebuilding beats holding | 56 |

Those 56 total 9 ha, $187M of discounted gain and a median yield of 4.8%
(before remediation) against the borough's 3.8%, because the income they earn
today is near zero. Gas stations are 9 of 11 NPV-positive. One machine shop on
H04-072 (lot 1 740 794, 2.7 ha) carries $86M of the gain alone, and a transport
yard on C04-083 another $34M. Overlap with the teardown screen is 4 lots, so
the two are complementary rather than redundant.

Under [item 7 below](#not-only-residential) the remaining 300-odd — a
warehouse in a pure-I zone the solver rebuilds as a warehouse — are filed too;
they are ranked only where that pays, and the switch that ranks them anyway is
`REQUIRE_POSITIVE_NPV=false`.

### Infill: nothing stands on it

`existing_floor_area_m2` is nothing, `existing_num_dwellings` is nothing, the
lot is under-built and the solver has a program. Surface parking lots (CUBF
4621) fall here — 18 of them were NPV-positive at a 5% median yield in the
probe — as does vacant land (9xxx). A parking lot is not a risk activity under
the regime and is not in the brownfield list; the two theses are kept apart
so the cost side stays honest.

### Improvement: the building stays

The one thesis that needs no old building. Two moves, both inside the envelope
the solver already respected:

- **a storey** — the standing footprint (the roll's floor area over its
  storey count) repeated up to `improvement_max_added_storeys` times (1), and
  no higher than the solver's own storey count, which is the envelope's
  ceiling read through both *En étage* and the height in metres;
- **an annex** — the ground the solver's footprint covers that the standing
  one does not, raised to the standing building's height.

Both together are capped at the floor gap. The addition earns the solver's
NOI per square metre for this lot's own program, costs its capital cost per
square metre times `addition_cost_premium`, and is filed when it reaches
`improvement_min_floor_m2` (40 m²). A lot the roll states no storey count for
gets no program: guessing a footprint would put an invented building on the
shortlist, and `num_lots_storeys_unknown` in the run's metadata counts them.

A rooftop construction housing part of a dwelling is subject to the borough's
PIIA (RCA06-14001, art. 9) wherever the by-law applies, and an *agrandissement*
visible from the street is reviewed under it — so `has_piia_review` is the
flag to read beside an improvement, and it does not screen one.

## Heritage and the PIIA

The grid's *Patrimoine* block used to stop the parser, because everything
under it is stated once for the zone rather than per column. It now reads four
of those rows and repeats them on every column of the zone, into
`silver.zoning_grid_columns`:

| row | column | values on VSMPE's 632 grids |
| --- | --- | --- |
| *Secteur d'intérêt patrimonial* | `heritage_sector` | `Oui` on 148, `A` on 1, `-` on the rest |
| *PIIA (secteur)* | `piia_sector` | a sector number, `2` to `6`, on 279; `-` on the rest |
| *PAE* | `pae` | `Oui` on 5 |
| *Articles visés* | `specific_articles` | article numbers of 01-283, `665.62` on 182 |

The opportunities asset joins them through the HBU row's governing zone and
writes four flags:

| flag | reads | screens by default |
| --- | --- | --- |
| `is_heritage_sector` | `heritage_sector` states a sector | **yes** — out of `teardown` and `brownfield` |
| `has_piia_review` | `piia_sector` states a sector | no — flagged |
| `demolition_review_required` | the sector, or `existing_year_built` < `demolition_review_year` (1940) | no — flagged |
| `is_demolition_restricted` | whichever of the three the config screens on | — |

**Why the heritage sector screens and the PIIA does not.** A *secteur
d'intérêt patrimonial* is where the borough's demolition by-law
(RCA04-14007) sends a contributing building to committee with an *étude de la
valeur patrimoniale* and a reuse programme, and a refusal is the ordinary
outcome. A PIIA sector is an architectural review of what is *built* — new
construction, additions visible from the street, rooftop constructions,
conversions of a non-residential building to four or more dwellings — and it
does not bar removing what stands. It also covers close to half the borough's
zones; screening on it would gut the teardown thesis on a design-review rule.
`EXCLUDE_PIIA_SECTORS=true` turns it into a screen for a mandate that wants
one.

**The 1940 line.** Bill 69 (2021, c. 10, in force 1 April 2021) obliges every
municipality to keep a demolition by-law (LAU art. 148.0.2) that applies at
least to *immeubles patrimoniaux* — cited buildings, buildings in a heritage
site, and buildings in the inventory the MRC must keep of pre-1940 immovables
with heritage value (LPC art. 120; LAU art. 148.0.1). Until the by-law and the
inventory were in place, any demolition of a building *construit avant 1940*
needed 90 days' notice to the minister. Montréal's agglomeration adopted its
inventories through 2025; the VSMPE list holds **4,612 immovables, adopted
2025-09-25**, and is on the open-data portal. That list, not the year, is the
true screen — `demolition_review_required` is the year as a proxy for it, and
the inventory is the natural next bronze source. In VSMPE every teardown goes
to committee regardless: RCA04-14007 refers any work destroying the structure
of floors, exterior walls or roof, at $6,000 and three to four months
(against $385 for a certificate that is not referred).

## Each thesis costs its own denominator

For the three theses that clear the ground:

```
site_yield_on_cost_pct = 100 × hbu_annual_stabilised_noi_cad
    / (hbu_total_capital_cost_cad + assessed value × market_value_factor
       + demolition_cost_cad + site_assessment_cost_cad + remediation_cost_cad)
```

and for an improvement the building stays, so the yield is the addition's own:
`improvement_noi_cad` over `improvement_cost_cad`. `site_total_project_cost_cad`
is the denominator either way, kept so the yield is checkable from the row.

| rate | default | applies to | source |
| --- | --- | --- | --- |
| `demolition_cost_cad_per_m2` | $150 | standing gross floor of a residential building | Québec plex demolition $10–30/pi² ($108–323/m²), $10k–50k a building (Soumission Rénovation, 2026); $5–15/pi² at the low end (Soumissions Démolition) |
| `demolition_cost_cad_per_m2_nonresidential` | $250 | standing gross floor of anything else | low-rise commercial and light industrial $15–35/sf ($160–380/m²) (HKC, Ontario 2026); asbestos abatement adds $25–75/pi² where present |
| `site_assessment_cost_cad` | $12,000 | once, per brownfield-use lot | Phase I $1,000–2,000 and Phase II $4,000–10,000 (ImmoFacile); characterisation $1,000–5,000 (Soumission Rénovation, 2024) |
| `remediation_cost_cad_per_m2_residential` | $150 | lot area, where the program includes housing | hydrocarbon soils $35–55/t plus the 2026 landfill *redevance* of $12.00/t and traceability $2.34/t; a metre of soil is about 1.8 t/m², so haul and disposal alone run $90–125/m² per metre of depth, before excavation and backfill |
| `remediation_cost_cad_per_m2_nonresidential` | $75 | lot area, where the program is commerce or industry only | the same soils to the commercial criterion, which leaves more in place |
| `addition_cost_premium` | 1.5 | the solver's capital cost per m², on an improvement | storey additions $175–400/pi² and rear annexes $240–450/pi² in Montréal, against Altus 2025 new-build hard costs of $210–275/pi² for wood-frame apartments and $135–185 for row housing — an implied 1.3–2× |

None of these is a per-lot survey. Remediation in particular is priced per
square metre of lot because that is what the platform knows about the ground;
a real site costs what its Phase II finds. Altus's 2025 guide has no demolition
line and no Québec source publishes a per-square-metre plex or masonry rate, so
the two demolition defaults sit inside the ranges above rather than at a
published point.

## Not only residential

None of the four theses requires housing. The site thesis says why the parcel
is acquirable; `investment_thesis` beside it says what the solver would build,
and a warehouse in a pure-industrial zone whose best use is a bigger warehouse
is filed as a `brownfield` or a `teardown` like any other. The remediation rate
follows the program: the residential criterion where it includes housing, the
lower one otherwise.

What keeps those lots off the *ranked* list is the verdict. In this borough
the solver's commercial and industrial programs mostly lose money at the
surveyed rents (see [`hbu-solve-is-npv-multi-use`](../README.md)), so with
`require_positive_npv` on — the default, and the role `is_underbuilt` plays on
the first axis — an industrial teardown keeps its `site_thesis` and gets no
`site_thesis_rank`. `REQUIRE_POSITIVE_NPV=false` ranks every filed lot on its
site yield, which is what a mandate looking for the best of a losing zone
asks for.

## Three futures, not one

The site thesis says why a parcel is acquirable. It does not say what to do
with it, and until now the platform priced only two of the three things an
owner can do: keep the building, or clear the lot and build the solver's
programme. The third — keep the building and grow it — was the closed-form
estimate under the improvement thesis above. It is now a solve of its own,
and all three are priced on one footing, twice: for the owner and for a
buyer.

### The enhancement solve

`hbu.solve_enhancements` runs the same CP-SAT model as the rebuild with the
standing building handed in as a `RetainedBuilding`:

- the plate is a floor under the footprint — an annex may widen it, nothing
  narrows it — and the usage storeys are a floor under the storey count;
- at most `max_added_storeys` (1) go on top, the structure's limit rather
  than the grid's, whose own ceiling still applies;
- nothing is dug under it, no deck is stacked on it and no bay is carved out
  of its ground floor: the addition parks on the yard or not at all;
- the standing floor keeps earning what the roll says it earns; only the new
  floor is priced, at new-build rents and at `addition_cost_premium` (1.5)
  times the rates the rebuild was costed at;
- its income starts after the addition's own build and lease-up (9 and 3
  months), and `enhance_disruption_share` (0.25) of the standing NOI is lost
  for the months the site is one.

Every `enhance_*` money figure on the gap row is the addition's own, and
`enhance_gain_cad` — its NPV less the disruption — is what enhancing adds
over holding. A lot with nothing to grow says why in `enhance_status`:
`no_building` (nothing stands, or the roll states no storey count),
`not_underbuilt`, `no_program`, `no_envelope`, or `INFEASIBLE` where the
standing plate is more than today's grid would let stand. The solve runs
on the under-built lots with a building — some 5,000 in VSMPE — and its
rates are read back off the rebuild's rows by `program_assumptions_of`, so
the two futures cannot drift apart. The improvement thesis reads the solve
where the gap carries one (`improvement_source = 'solve'`) and falls back to
the estimate on a partition that does not.

The footprint is the roll's floor over its storeys, which is exact for a
plex and generous for a setback storey; the BDOI footprint on `lot_profiles`
is the better input and the next one.

**Where an addition breaks even.** At the borough's own CMHC rents — a
one-bedroom at $980, a two-bedroom at $1,344, plus the 30% new-build premium
— a storey and an annex on a two-storey plex pay at an addition premium up to
about 1.2 and not above it, with or without the half-stall a dwelling owes:

| addition premium | added floor | new dwellings | addition NPV, 9 + 3 months |
| --- | --- | --- | --- |
| 1.0 | 223 m² | 4 one-bedrooms | +$156k |
| 1.1 | 223 m² | 4 | +$94k |
| 1.2 | 223 m² | 4 | +$32k |
| 1.3 | nothing | 0 | 0 |
| 1.5 (the sourced default) | nothing | 0 | 0 |

So at the default rate no enhancement in VSMPE pays, and the enhance column
on the two panes reads "nothing pencils" borough-wide; that is the arithmetic
at rents a third over a stock average of $980, not a statement that plexes
cannot take a storey. The rent side is the conservative one — new
one-bedrooms in Villeray ask well over the $1,274 that premium produces —
and `ADDITION_PREMIUM=1.2` on `make hbu` is the one-line way to see the
borough where additions clear. The rebuild on the same lot, at 18 + 6
months, pays +$488k on $2.9M, so the futures rank rebuild over enhance over
hold wherever the grid has the storeys.

### Time in the proforma

A rebuild's income does not start on day one. `InvestmentAssumptions` now
carries `construction_months` and `lease_up_months`, and the whole stream —
the annuity over the hold and the sale that ends it — is pushed out by the
build plus half the lease-up, since a linear fill loses half of it on
average:

```
delay_years      = (construction_months + lease_up_months / 2) / 12
annual_pv_factor = hold_pv_factor × (1 + r) ^ −delay_years
```

The programs asset solves the rebuild at 18 and 6 months, which at 5% takes
about 9% off its present value. The standing building's income is valued
with `hold_pv_factor` and no delay, so `redevelopment_npv_gain_cad` now
carries the months the site earns nothing. Capital is still spent at day
one, undiscounted, which is the conservative side. Soft costs, financing and
contingency are still not modelled, and the page says so where it says what
the NPV is.

### For the owner

The land cancels: the owner holds it in every future. On the gap row,
before the site's own costs:

| future | `*_value_cad` | what it is |
| --- | --- | --- |
| hold | `hold_value_cad` | the standing NOI discounted over the hold and sold at the cap, starting today |
| enhance | `enhance_value_cad` | hold, plus the addition's NPV, less the disruption |
| rebuild | `rebuild_value_cad` | the proposal's present value with its income delayed, less its capital |

`best_future` is the largest, `hold` on a tie. The shortlist table repeats
the three with the demolition, the characterisation and the remediation
taken off the rebuild (`owner_*_value_cad`), states each against holding
(`owner_gain_enhance_cad`, `owner_gain_rebuild_cad`) and names the winner
(`owner_best_future`). **The teardown thesis now follows the money**: a lot
whose enhancement is worth more to its owner than its rebuild is filed under
`improvement` however old the building, and `site_verdict_cad` — the
owner's gain from the thesis's own future — is what the rank breaks ties on
and what `require_positive_npv` screens.

### For a buyer

A buyer pays for the land and the building first. The price the platform
puts on that is

```
acquisition_cost_cad = max(existing_total_assessed_value × market_value_factor,
                           hold_value_cad)
```

because a seller keeps the better of what the roll says the property is
worth and what its income is worth to them. `market_value_factor` — the
setting that used to be called `land_value_factor`, and always scaled the
roll's whole value — is where a reader who knows what plexes trade over
assessment puts it; the 2026 roll values everything as of July 2024, and
1.1 to 1.3 is the honest range.

Each future is then its owner's value less that price (`buyer_npv_*_cad`),
its stabilised NOI over everything paid to reach it (`buyer_yield_*_pct`),
and the most a buyer could pay for it and still clear the discount rate,
which is the value itself (`residual_price_*_cad`). `buyer_best_future` is
the largest NPV. Every buyer column is null where the roll never assessed the
lot: there is no price to pay. The map's **Buyer** and **Owner** panes show
the three futures for one lot from these columns and nothing else, and the
chat's `lot_futures` tool says the same in a sentence each.

## The columns

Added to `gold.lot_investment_opportunities` beside the first axis:

| column | what |
| --- | --- |
| `existing_year_built`, `existing_num_storeys`, `existing_dominant_use_code` | what the screen read off the roll |
| `hbu_floors`, `hbu_footprint_m2`, `grid_zone` | what it read off the solver |
| `heritage_sector`, `piia_sector` | what it read off the grid |
| `storey_headroom`, `built_share`, `existing_footprint_m2` | the derived inputs |
| `is_brownfield_use`, `is_heritage_sector`, `has_piia_review`, `demolition_review_required`, `is_demolition_restricted` | the facts before the theses |
| `is_brownfield_site`, `is_teardown_site`, `is_infill_site`, `is_improvement_site`, `site_thesis` | the theses |
| `improvement_added_storeys`, `improvement_floor_m2`, `improvement_cost_cad`, `improvement_noi_cad`, `improvement_yield_pct` | the addition |
| `demolition_cost_cad`, `site_assessment_cost_cad`, `remediation_cost_cad`, `site_total_project_cost_cad`, `site_yield_on_cost_pct` | the site's own cost and yield |
| `site_thesis_rank`, `is_top_site_opportunity`, `num_ranked_in_site_thesis` | the rank within the site thesis |
| `redevelopment_npv_gain_cad`, `site_verdict_cad` | the rebuild's gain over holding as the gap states it, and the thesis's own gain with the site costs in |
| `enhance_*`, `hold_value_cad`, `enhance_value_cad`, `rebuild_value_cad`, `best_future` | the enhancement solve and the three futures, carried from the gap |
| `owner_*_value_cad`, `owner_gain_*_cad`, `owner_best_future` | the three futures for the owner, site costs on the rebuild |
| `acquisition_cost_cad`, `buyer_npv_*_cad`, `buyer_yield_*_pct`, `residual_price_*_cad`, `buyer_best_future` | the three futures for a buyer |

Every lot keeps its row; `site_thesis = 'none'` with the booleans false is a
lot no condition held on, and a thesis with a null rank is one that held and
does not pay. `screen_assumptions` carries every threshold and rate, and
`heritage_source` says whether the zone columns carried the *Patrimoine* rows
at all — `absent` means the partition's grids were parsed before they were
read, and `num_lots_heritage_unknown` is then the whole borough.

## Running it

The asset reads four partitions and fails naming whichever is missing:
`lot_redevelopment_gap`, `lot_highest_best_use`, `lot_assessment_comparables`
and `zoning_grid_columns`. The last is the one to re-materialize for an
existing partition, so the heritage rows exist to read:

```bash
make envelopes DATE=2026-09-01 NEIGHBORHOOD=VSMPE   # re-parses the grids
make opportunities DATE=2026-09-01 NEIGHBORHOOD=VSMPE
```

`sql/012`, `sql/019` and `sql/021` widen the tables with `ADD COLUMN IF NOT
EXISTS`, so `db.py init` against the target database comes first. The
enhancement is solved by `make hbu`, whose gap half now reads the envelopes
and the rents beside the roll, at the defaults of `GapConfig` unless
`ENHANCE_MONTHS`, `ENHANCE_LEASE_UP`, `DISRUPTION_SHARE`, `ADDITION_PREMIUM`
or `MAX_ADDED_STOREYS` say otherwise; the rebuild's own timing is
`CONSTRUCTION_MONTHS` and `LEASE_UP_MONTHS` on `make programs`. A change to
either timing re-solves the borough; a change to a rate or a threshold here
is `make opportunities` alone. Every variable the
target reads — `TEARDOWN_MAX_YEAR`, `TEARDOWN_MAX_BUILT_SHARE`,
`MIN_STOREY_HEADROOM`, `EXCLUDE_HERITAGE_SECTORS`, `EXCLUDE_PIIA_SECTORS`,
`DEMOLITION_COST_M2`, `DEMOLITION_COST_NONRES_M2`, `SITE_ASSESSMENT_COST`,
`REMEDIATION_RES_M2`, `REMEDIATION_NONRES_M2`, `ADDITION_PREMIUM`,
`REQUIRE_POSITIVE_NPV`, `SITE_TOP_N` — is listed by `make help`.

## Where it is read

The map draws an **Opportunities** layer from this table — the lots with a
site thesis, coloured by it, with a filter per thesis and a *top of each*
switch — and the HBU pane explains the lot's site thesis under the verdict:
which conditions held, what the heritage rows say, and what the site's own
denominator adds. `hbu_rag_map`'s README covers both.

## Open items

- **The heritage inventory.** The borough's list of 4,612 pre-1940 immovables
  with heritage value is the screen the statute actually names; the year is a
  proxy. It is one open-data download and one point-in-lot join.
- **The SCIAN join.** Replace the CUBF prefix list with Annexe III's own
  SCIAN codes through the codebook's correspondence column.
- **The building-value share**, as a second obsolescence signal for the
  teardown thesis.
- **Soft costs, financing and contingency** on both the rebuild and the
  addition; the timing is in, these are not.
- **Remediation per lot.** A Phase I on record beats any rate; there is no
  public source for one.

## Sources

- [Loi 2021, c. 10 (Bill 69)](https://www.publicationsduquebec.gouv.qc.ca/fileadmin/Fichiers_client/lois_et_reglements/LoisAnnuelles/fr/2021/2021C10F.PDF) — LPC art. 120, LAU arts. 148.0.1 and 148.0.2, transitional ss. 136–140
- [LAU art. 148.0.2](https://www.legisquebec.gouv.qc.ca/fr/version/lc/A-19.1?code=se:148_0_2&historique=20250101) · [LQE art. 31.51](https://www.legisquebec.gouv.qc.ca/fr/version/lc/Q-2?code=se:31_51&historique=20250101) · [LQE art. 31.53](https://www.legisquebec.gouv.qc.ca/fr/version/lc/q-2?code=se:31_53&historique=20240321) · [RPRT, Q-2 r. 37](https://www.legisquebec.gouv.qc.ca/fr/document/rc/Q-2,%20r.%2037)
- [Québec: contrôle des démolitions](https://www.quebec.ca/culture/patrimoine-archeologie/soutien-municipalites-communautes-autochtones/controle-demolitions) · [guide de prise de décision](https://www.quebec.ca/habitation-territoire/amenagement-developpement-territoires/amenagement-territoire/guide-prise-decision-urbanisme/reglementation/demolition-immeubles)
- [Montréal: inventaires des immeubles patrimoniaux](https://montreal.ca/articles/inventaires-des-immeubles-patrimoniaux-32414) · [the dataset](https://donnees.montreal.ca/dataset/inventaires-des-immeubles-patrimoniaux-construits-avant-1940)
- [Montréal: permis de démolition](https://montreal.ca/demarches/obtenir-un-permis-pour-la-demolition-dun-immeuble) · [PIIA](https://montreal.ca/demarches/faire-approuver-un-projet-lie-un-plan-dimplantation-et-dintegration-architecturale) · [VSP modifications, PUM 2050](https://montreal.ca/articles/modifications-apportees-la-reglementation-de-vsp-en-lien-avec-le-pum-2050-106938)
- [01-283, 2013 codification](https://ocpm.qc.ca/sites/default/files/pdf/P73/6d.pdf) · [RCA06-14001, 2011 codification](https://ocpm.qc.ca/sites/default/files/pdf/P69/3k.pdf) · [a VSP grid](https://portail-m4s.s3.montreal.ca/pdf/vsp_h02-032_grille_de_zonage.pdf)
- [Altus 2025 Canadian Cost Guide](https://spearrealty.ca/wp-content/uploads/2025/09/2025-Canadian-Cost-Guide-Altus-Group-1.pdf)
- Demolition: [Soumission Rénovation](https://soumissionrenovation.ca/fr/blogue/couts-projets-demolition) · [Soumissions Démolition](https://www.soumissionsdemolition.ca/cout-demolition-maison-quebec/) · [DemoPrep](https://demoprep.ca/cost-of-demolition-in-montreal/) · [HKC, Ontario](https://www.hkcconstruction.com/blogs/commercial-demolition-cost-ontario-2026) · [asbestos](https://soumissionrenovation.ca/fr/blogue/quel-est-le-cout-du-desamiantage-au-pied-carre-au-quebec)
- Contaminated land: [ImmoFacile](https://immofacile.ca/les-etapes-pour-decontaminer-un-terrain/) · [Soumission Rénovation](https://soumissionrenovation.ca/fr/blogue/etapes-projet-decontamination) · [redevances](https://www.environnement.gouv.qc.ca/sol/terrains/redevances-sols-contamines.htm) · [traçabilité](https://www.environnement.gouv.qc.ca/sol/terrains/tracabilite/index.htm)
- Additions: [Soumission Rénovation](https://soumissionrenovation.ca/fr/blogue/savoir-ajout-etage-maison) · [Plan Maison Québec](https://www.planmaisonquebec.com/post/projet-dajout-d%C3%A9tage-sur-une-maison-fonctionnement-et-co%C3%BBts-au-qu%C3%A9bec) · [Item Construction](https://itemconstruction.com/prix-pour-ajouter-un-etage-maison/) · [Billdr](https://www.billdr.ai/fr/guides/renovation-maison/maison-agrandissement-montreal-cout)
