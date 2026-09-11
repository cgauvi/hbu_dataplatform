# Why a site is acquirable

[`gold.lot_investment_opportunities`](opportunities.md) files every lot under an
**investment thesis** — what you would build — and ranks the under-built ones
within it on yield on cost. That answers *what*. It does not answer *why this
parcel and not the one beside it*: why it might be on the market, what has to
be cleared off it first, and what that clearing costs.

The second axis of the same table answers that. `site_thesis` is one of four
values plus `none`, each a predicate over facts the platform already holds —
the roll's year built and storey count, the solver's storeys and footprint,
the CUBF use code, the measured footprint on the piece, the grid's own
*Patrimoine* rows and the owner's three futures — and each carrying its own
cost into its own yield. Nothing is re-solved; this is a classification and a
few divisions over four parquet files, the same posture the first axis takes.

The row is the **lot × zone piece** since 2026-09-08, keyed on `lot_uid` and
`feature_id`: a lot two zones cut in two has two rows, each filed on its own
piece's program and its own share of the roll, and the map regularly shows two
theses on one parcel. "Lot" below means a piece unless it says otherwise.
VSMPE 2026-09-01 is 27,923 pieces on 24,785 lots, 22,597 of them carrying an
assessed value. Every stated assumption on this page — and every other the
chain makes, from the stall ratio to the discount rate — is collected in
[assumptions.md](assumptions.md).

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
2026-09-01 partition as materialized on 2026-09-10, over the pieces carrying
an assessed value (2,266 of those state no storey count and have no headroom
to state):

| headroom (storeys) | assessed pieces |
| --- | --- |
| −1 or less | 1,436 |
| 0 | 13,564 |
| 1 | 4,541 |
| 2 or more | 790 |

Three-quarters of the stock cannot add a floor, and under 4% has the two
storeys of headroom the teardown thesis asks for. The distribution moved with
the piece grain (2026-09-08) and with the level rows being read as a
placement rather than a count (2026-09-10), so an older reading of this table
— over 85% at zero or less — is the previous solve, not a different borough.
A screen on "old building" alone would surface like-for-like replacements,
not development plays. So the
teardown thesis carries a headroom term, the improvement thesis *is* the
headroom, and a lot with none of it can only be a brownfield or an infill.

## The four theses

Resolved in this order when more than one holds; the four `is_*_site` booleans
keep the ones that also fired, so a lot filed under brownfield can still be
read as a teardown.

| `site_thesis` | when | what is cleared |
| --- | --- | --- |
| `brownfield` | the dominant use on the lot is a contamination-risk activity, and — where a building stands to be demolished — the governing zone is not a heritage or PIIA sector | the building, then the ground: characterisation and remediation |
| `teardown` | a building the roll states floor for, old enough to be presumed obsolete, filling little of its envelope, under a grid allowing storeys above it, outside a heritage sector | the building |
| `infill` | nothing stands on the lot, the solver has a program, and the lot is not a lane the roll never listed | nothing — a vacant lot whose *use* is a risk activity is a `brownfield` first |
| `improvement` | the building stays and gains a storey on its footprint or an annex on the ground the solver would cover | nothing — the addition is the whole cost |
| `none` | no condition held | — |

Brownfield leads because it changes the cost side the most and a gas station
is a gas station whatever else is true of it; teardown before infill because a
lot with a building on it is not empty; improvement last because keeping the
building is what remains once nothing argues for removing it.

Every threshold below is config on `OpportunityConfig`, recorded on every row
in `screen_assumptions`, and settable from `make opportunities` — all but
`improvement_min_floor_m2` and `exclude_demolition_review`, which have no make
variable and take the Dagster config directly.

### Teardown: an obsolete improvement under an unused envelope

All of these at once, on a lot the gap table calls under-built:

- `existing_year_built` ≤ `teardown_max_year_built` (1960) — the inter-war and
  post-war plexes; the 1960s stock is mostly built to its envelope anyway.
- `built_share` ≤ `teardown_max_built_share` (0.40) — the standing floor is
  under two-fifths of what the solver would put there.
- `storey_headroom` ≥ `min_storey_headroom` (2) — below two a teardown is a
  like-for-like replacement.
- not `is_demolition_restricted` — see [Heritage](#heritage-and-the-piia).
- `existing_floor_area_m2` > 0 — there has to be a building to demolish. The
  term reads the floor rather than `existing_year_built` because `built_share`
  fills a missing floor with zero, not null: a lot the roll dates but states no
  floor for scores 0.0 on the under-built test, passes it, and proposes a
  demolition whose `demolition_cost_cad` is nothing, there being no floor to
  charge the rate against. No VSMPE teardown was one of those; the term is what
  stops the [heritage clause](#heritage-and-the-piia) from creating seven.
- and, where the futures are priced, `owner_gain_enhance_cad` not above
  `owner_gain_rebuild_cad`: a lot whose owner does better adding a storey than
  clearing the site is filed under `improvement` however old the building —
  see [For the owner](#for-the-owner). On VSMPE 65 enhancements beat their
  rebuild that way.

Sized on VSMPE 2026-09-01, with the verdict positive (rebuilding beats holding,
discounted). **These three rows predate both the proforma screens and the PIIA
screen below**, and are kept because they are what moving the two thresholds
costs, which has not changed. As materialized on 2026-09-10 the partition
files 51 teardown pieces and ranks 36, at a 4.0% median site yield on cost,
$2.0M of owner's verdict between them, $23M of assessed value and a 303 m²
mean piece:

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

Sized on VSMPE 2026-09-01 when the thesis was written — per lot, before the
PIIA screen and before the futures were priced:

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

As materialized on 2026-09-10, per piece and with the PIIA screen on: 373
pieces carry a risk use, 298 of them have a program and 83 a program with
housing in it; 90 are filed `brownfield` — the screen takes the ones with a
standing building in a heritage or PIIA sector — and 20 pay their owner and
are ranked.

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

Which is why `infill` is not the fallback for a vacant lot whose use *is* a
risk activity. Both theses fire on a contaminated yard — nothing stands on it,
and the regime presumes against it — and precedence names it `brownfield`, so
the cost line a reader sees matches the $115k the row already carries. The one
route by which a risk use could land here was the demolition screen; see
[Heritage](#heritage-and-the-piia) for the clause that closed it.

**A ruelle reads exactly like a vacant lot to this thesis**, and the borough
has a great many of them: no floor, no dwellings, an envelope and a program.
It is the one thesis ground the roll never listed can reach — the other three
need a use code or a stated floor, which is the roll speaking — so on VSMPE
2026-09-01 it was filing 594 pieces on 482 lots with a median 3.7 m of
frontage, 289 of them with no frontage at all, as sites ready to build on.
Lot 2 249 035 is the case: 920 m² the roll never listed, 16.7 m² of a
neighbour's building clipped onto it, no street.

What tells a lane from a vacant lot is not the roll but the ground: the
*measured* footprint, `silver.lot_zone_pieces`' clip of the cadastre's
buildings onto the piece, which the gap carries as `existing_footprint_m2`
and this table restates as `existing_footprint_coverage`, over the piece's
area. `is_unassessed_vacant` is two conditions and both are required:

- **nothing on the roll at all** — no assessment unit, no assessed value, no
  stated floor, no dwelling, no use code. Not the unit count alone: it is
  footprint-allocated across a split parcel's pieces and rounded, so a bare
  yard behind an assessed building carries `0` units while its area-allocated
  value is still on the row — 97 such pieces, every one a real site. And not
  `has_assessment`, which is true on every row.
- **under `unassessed_vacant_max_coverage` (0.05) of the ground under a
  building.** Below the line the pieces are lanes; above it, the 178 off-roll
  pieces with a garage or a shed over a twentieth of them are something the
  roll missed, and stay.

A lot both hold on is not an infill and keeps its row at `none`. Either alone
is a site: an assessed parking lot with nothing on it is this thesis's own
case. Where the frame carries no measured footprint — a partition older than
the pieces — the screen does not fire, because absence of the measure is not
absence of a building; `0` turns it off. The 594 were unranked already (the
roll never priced them, so there is no acquisition cost and no IRR), so the
shortlist is unchanged; what changes is the inventory the map's Opportunities
layer draws and the counts a borough total sums. As materialized on
2026-09-10, 457 pieces are filed `infill` — 279 on the roll and rankable, 178
off it with something measurable standing on them — and 220 are ranked;
4,730 pieces carry `is_unassessed_vacant`, most of them with no program to be
filed under anything.

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
square metre times `addition_cost_premium`, and is filed, on a lot the gap
calls under-built, when it reaches `improvement_min_floor_m2` (40 m²). A lot
the roll states no storey count for
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
| `has_piia_review` | `piia_sector` states a sector | **yes** — out of `teardown` and `brownfield` |
| `demolition_review_required` | the sector, or `existing_year_built` < `demolition_review_year` (1940) | no — flagged |
| `is_demolition_restricted` | whichever of the three the config screens on, **and** `existing_floor_area_m2` > 0 | — |

**Why both screen, and what the screen leaves alone.** A *secteur d'intérêt
patrimonial* is where the borough's demolition by-law (RCA04-14007) sends a
contributing building to committee with an *étude de la valeur patrimoniale*
and a reuse programme, and a refusal is the ordinary outcome.

A PIIA sector is a discretionary architectural review of what is *built* — new
construction, additions visible from the street, rooftop constructions,
conversions of a non-residential building to four or more dwellings. Read
narrowly it does not bar removing what stands, and until 2026-09-08 it was
flagged rather than screened for that reason. It now screens, because the
thing a teardown asks for is exactly the thing the review is written to judge:
the replacement building, on a street the sector exists to hold together. A
lot where the rebuild has to survive a discretionary design review before it
can be priced is not a teardown a shortlist should be offering.

**The screen removes only the theses that demolish.** `teardown` and
`brownfield` drop; `improvement` does not read
`is_demolition_restricted` at all, so a PIIA lot that can carry a storey or
an annex keeps its `improvement` thesis and its rank within it. That is the
intended landing: the borough's PIIA sectors are full of under-built plexes
worth adding to, and the enhancement solve already prices exactly that. Lot
2 214 159 (grid C04-083, PIIA sector 6, 1945, two storeys under six, 13% of
its envelope) is the shape of it — a teardown at rank 13 before, an
enhancement candidate after.

**And only where there is something to demolish.** `is_demolition_restricted`
carries `existing_floor_area_m2` > 0, so a lot the roll states no floor for is
never screened: there is no demolition there to refer to committee, and what a
mandate proposes on it is remediation and new construction — which is what
`infill` proposes on the same sector, unscreened, today. Without that clause
the screen dropped `brownfield` off exactly those lots and `infill`, which
does not read the flag either, caught them. Lot **2 249 816** (grid C01-121,
CUBF 6419 *Autres services de l'automobile*, PIIA sector, no stated floor, no
stated year) was the case: `is_brownfield_use` true, $12,000 of
characterisation and $103,261 of remediation charged against it on the cost
side, and `site_thesis` reading `infill` — the one thesis that means nothing
stands on it and nothing has to be cleared. It is a `brownfield` from
2026-09-09, with `is_infill_site` still true beside it, because both are.

Sized on VSMPE 2026-09-01: of the 36 risk-use lots the screen pushed into
`infill`, all 36 come back to `brownfield`. The 81 it pushed into
`improvement` stay there — those have a standing building, so the screen still
bites, and `improvement` is the intended landing for them. `teardown` is
untouched either way: `built_share` already forces standing floor on it, so
the clause can never hand out a demolition play on a building the roll
describes.

The cost is real and worth stating: `piia_sector` is set on 279 of VSMPE's 632
grids, and 9,114 pieces carry `has_piia_review` (8,672 of them
`is_demolition_restricted`, having a building to keep), so this is the widest
screen on the page. `EXCLUDE_PIIA_SECTORS=false` restores the flag-only posture for a
mandate that wants to see the demolition plays anyway.

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
surveyed rents (see [the objective](development-program.md#the-objective-discounted-net-profit)), so with
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
- nothing is dug under it and no bay is carved out of its ground floor: the
  addition parks on the yard or not at all — and where the yard cannot hold
  the stalls the addition owes, the solve is run again with the obligation
  waived, as the rebuild's is, and `enhance_parking_waived` and
  `enhance_waived_stalls` say so (82 VSMPE enhancements, against 407
  rebuilds);
- the standing floor keeps earning what the roll says it earns; only the new
  floor is priced, at new-build rents and at `addition_cost_premium` (1.5)
  times the rates the rebuild was costed at;
- its income starts after the addition's own build and lease-up (9 and 3
  months), and `enhance_disruption_share` (0.25) of the standing NOI is lost
  for the months the site is one.

Every `enhance_*` money figure on the gap row is the addition's own, and
`enhance_gain_cad` — its NPV less the disruption — is what enhancing adds
over holding. Where the solve adds nothing (`nothing_pencils` in
`enhance_binding`: no storey and no annex pays at the premium, and the answer
is normalised to the standing building) there are no works, so no disruption
is charged either: the gain is 0, `enhance_value_cad` equals `hold_value_cad`,
and the shortlist states no enhancement return, budget or lease-up for it. A
lot with nothing to grow says why in `enhance_status`:
`no_building` (nothing stands, or the roll states no storey count),
`not_underbuilt`, `no_program`, `no_envelope`, or `INFEASIBLE` where the
standing plate is more than today's grid would let stand. The solve runs
on the under-built lots with a building — some 5,000 in VSMPE — and its
rates are read back off the rebuild's rows by `program_assumptions_of`, so
the two futures cannot drift apart. The improvement thesis reads the solve
where the gap carries one (`improvement_source = 'solve'`) and falls back to
the closed-form estimate wherever it does not — a partition from before the
solve, **or a row whose enhancement came back `INFEASIBLE` or `ERROR`**. That
second case is most of the thesis on VSMPE: of the 912 pieces filed
`improvement` on 2026-09-10, 837 are estimates on a standing plate today's
grid would not let stand again, and 75 are the solve's own — and since the
estimate is what the rank reads, 837 of the 839 ranked improvements are the
closed form. See the [open items](#open-items).

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

So at the default rate almost no enhancement in VSMPE pays — 10,189 of the
10,281 solved add nothing, 92 add a storey or an annex, 65 of those beat
their own rebuild for the owner and 5 beat holding too — and the enhance
column on the Deal pane reads "nothing pencils" nearly borough-wide; that is
the arithmetic
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
one, undiscounted, which is the conservative side. Soft costs and
contingency are not in this NPV — they enter the
[returns](#yield-on-cost-and-irr) below, on the same budget — and financing
is in neither; the page says so where it says what the NPV is.

### For the owner

The land cancels: the owner holds it in every future. On the gap row,
before the site's own costs:

| future | `*_value_cad` | what it is |
| --- | --- | --- |
| hold | `hold_value_cad` | the standing NOI discounted over the hold and sold at the cap, starting today |
| enhance | `enhance_value_cad` | hold, plus the addition's NPV, less the disruption; equal to hold where nothing pencils |
| rebuild | `rebuild_value_cad` | the proposal's present value with its income delayed, less its capital |

`best_future` is the largest, `hold` on a tie — on VSMPE as of 2026-09-10,
`hold` on 26,749 pieces, `rebuild` on 1,169 and `enhance` on 5. The shortlist
table repeats
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
the largest NPV, `hold` on a tie, and `none` where even the largest is below
zero: at that price the buyer walks, which is the fourth option a buyer has
and an owner does not. On VSMPE as of 2026-09-10 that is the answer on 20,382
of the 22,597 priced pieces; a buyer holds on 2,163, rebuilds on 49 and
enhances on 3. Every buyer column is null where the roll never assessed the
lot: there is no price to pay. The map's **Deal** pane shows the three
futures for one lot from these columns — there was an Owner pane beside it,
and it is gone, because the question it answered is not the one a
transaction turns on — and the chat's `lot_futures` tool says the same in a
sentence each.

## Yield on cost and IRR

A present value is one number and a decision needs two. Every future on the
shortlist now carries the proforma around it — `urban_rag.proforma` — and
the two returns a screen is read on:

- **yield on all-in cost**: the future's stabilised NOI over everything a
  buyer pays to reach it — the price, the site's costs, the hard cost with
  soft costs, contingency and builder's-risk insurance on it — held against
  the area's cap rate. A development has to earn a spread over buying the
  same income already built; `min_yoc_spread_bps` (100) is that spread.
- **IRR**, unlevered and annual: the price and the site's costs at day one,
  the budget spent evenly over the build, the income filling linearly over
  the lease-up, the hold, and the sale at the terminal cap less selling
  costs. Held against the lot's own blended cap plus `hurdle_irr_spread_bps`
  (100) — see below for why that is a spread and not a level. Stated twice — the **buyer's**
  on the whole building after the price, and the **owner's** on the
  increment over the building they have: no price, the standing income given
  up during and after the works, the difference in value at the sale.

### A building that is not all dwellings

The solver has filled envelopes with three families — housing, commerce,
industry — for as long as this pipeline has had them, and a good many of this
borough's grids ask for it. A column heading `H` marked *Tous sauf le RDC*
keeps housing off the ground floor; the `C.4` beside it in the same zone is
marked *Tous les niveaux* and may take that floor. The alternative to shops at
grade there is an empty storey, and the solve prices both and picks.

Both returns above were shaped for an apartment block, though, and until now
priced that answer as a building it is not:

- **the lease-up** was dwellings over `absorption_units_per_month`, so a
  program with *no* dwellings filled in the solve's stated months however much
  floor it held — fifteen thousand square feet of retail leased as fast as an
  empty six-plex. It is now the longest of the dwellings over their rate, the
  commercial floor over `commercial_absorption_sqft_per_month`, and the
  industrial floor over its own. The longest and not the sum: a rental office
  and a leasing agent are not waiting on each other. The floor measured is the
  plate *plus its cellar*, because the solve digs for shops across this borough
  and a sous-sol of retail is routinely a third of the space again.
- **the exit** capitalised every dollar at the multifamily terminal cap. Each
  family now carries its own — the residential cap plus
  `commercial_cap_rate_spread_bps` (175) or `industrial_cap_rate_spread_bps`
  (100) — and the cap a lot is valued and screened at is those **blended by
  income**, per lot. Never by floor: at the solve's own rates a square foot of
  commerce earns about four times a square foot of housing, so a ground floor
  of shops under five of flats is a sixth of the building's floor and very
  nearly half its rent, and a floor weighting would hand it a cap it has no
  business getting. `hbu_commercial_noi_cad` and its two neighbours on
  `gold.lot_redevelopment_gap` are that weight.

So `market_cap_rate_pct` is a per-lot column from here on rather than one
number per partition, `yoc_spread_rebuild_bps` is measured against the lot's
own blend, and a retail scheme is asked for the yield its own exit implies
instead of being let through on an apartment's. The standing building is
capped the same way, on `existing_dominant_income_class` — which is what makes
an owner's rebuild worth more when what they give up is a warehouse.

Both corrections push a commerce-heavy lot's stated return **down**, and that
is the point rather than a regression: it was previously leased at housing's
speed and sold at housing's cap, and neither was a number anyone had chosen.
The income never moved — the solve has always priced every square foot of
commerce it builds, and it has always been in the yield's numerator. What
changed is that "is the shop worth more than the empty floor" is now asked on
the shop's own terms. `hbu_non_residential_income_share` says at a glance
whether a lot is a mixed-use answer at all, and `exit_cap_rate_rebuild_pct`
says what the IRR actually sold the building at.

`is_good_candidate` is the screen: the thesis's own future (the enhancement
on an improvement, the rebuild on the rest) clears the cap rate by the
spread *or* clears the hurdle from the buyer's chair, and pays against
holding. Either bar, since 2026-09-10: the yield is a static ratio and the
IRR carries the lease-up, so a lot can clear one and miss the other — VSMPE's
2 784 705 clears its 5.5% hurdle on an 87 bps spread — and a deal that clears
is reported rather than lost to the screen it missed. `clears_cap_rate` and
`clears_hurdle` beside it say which.
`site_thesis_rank` is on the buyer's IRR from here on, the yield the
tiebreak; the map's **Opportunities** layer draws good candidates with a
green edge and can be narrowed to them, and every pane and tool says both
numbers.

### The assumptions, and where they come from

These are the proforma's own lines. Every other assumption in the chain — the
solve's, the roll's, the enhancement's and the theses' — is collected with
them in [assumptions.md](assumptions.md).

| line | default | basis |
| --- | --- | --- |
| soft costs | 18% of hard | architecture and engineering, permits and the borough's fees, legal, marketing, developer overhead; 15–25% on a Montréal wood-frame mid-rise |
| contingency | 7% of hard | what a lender asks on a costed, unbuilt budget; 5–10% |
| builder's-risk insurance | 1% of hard | the course-of-construction policy; operating insurance is inside the 35% expense ratio the NOI already carries |
| selling costs | 2.5% of the sale | brokerage and legal on the exit; 2–3% |
| absorption | 4 dwellings a month | the lease-up is the longer of the solve's months and the dwellings over this; a 20-unit building takes at least 5 months, a 100-unit one 25 |
| commercial absorption | 1 500 sq ft a month | a retail or small-office podium on a borough high street; a 15 000 sq ft podium takes 10 months against the 6 a residential solve states |
| industrial absorption | 5 000 sq ft a month | industrial leases in far larger blocks to far fewer tenants, so it fills faster per foot once it goes |
| market cap rate | the terminal cap, 4.5% | what stabilised multifamily income has sold at in Montréal, 4–5.5% in recent memory; `MARKET_CAP_RATE` overrides it. **Residential**: the two spreads below put the other families over it |
| commercial cap spread | 175 bps | commerce at about 6.25 against multifamily's 4.5, where Montréal retail and suburban-class office have sat |
| industrial cap spread | 100 bps | industrial at 5.5 — tighter than retail, because the sector has been bid tighter than retail for a decade |
| development spread | 100 bps | the floor a developer prices risk at over buying built income, over the lot's **blended** cap |
| IRR hurdle spread | 100 bps | over that same blended cap, so the two screens are two views of one bar. Per lot: 5.5% for an apartment block exiting at 4.5, 7.25% for a retail scheme exiting at 6.25 |

The roll's own cap rate for the lots around each one travels as
`comparable_cap_rate_pct` for the reader: it is NOI over assessed value,
runs around 3.5% in this borough, and is not what the screen holds the yield
against. Financing, taxes on the gain and rent growth are still not
modelled, on both sides, so the IRR and the NPV disagree only by what this
section adds — and one operating expense ratio still covers all three
families, which is the solve's own simplification and the one that most
flatters the dwellings: a triple-net retail lease leaves its landlord a far
lighter expense load than an apartment does. All twelve lines are
`make opportunities` variables (`SOFT_COST_PCT`, `CONTINGENCY_PCT`,
`BUILDERS_RISK_PCT`, `SELLING_COST_PCT`, `ABSORPTION_PER_MONTH`,
`COMMERCIAL_ABSORPTION_SQFT`, `INDUSTRIAL_ABSORPTION_SQFT`,
`MARKET_CAP_RATE`, `COMMERCIAL_CAP_SPREAD_BPS`, `INDUSTRIAL_CAP_SPREAD_BPS`,
`MIN_YOC_SPREAD_BPS`, `HURDLE_SPREAD_BPS`, `HURDLE_IRR`), recorded on every
row. Setting the two
spreads to 0 prices every family at the residential cap, which is the stance
this pipeline took before they existed.

### Why the hurdle is a spread and not a level

`HURDLE_IRR` was 12% flat until 2026-09-10, and it flagged nothing anywhere —
not because the borough is poor but because 12 is a number from a different
model. In a **flat-NOI** proforma the cap rate is an identity, not a
coincidence:

> buy at a 4.5% cap → collect 4.5% forever → sell at 4.5% → **IRR = 4.50%**
> (4.44% after selling costs)

So the cap is the *indifference* point — what doing nothing but buying the
finished building pays — and a hurdle set at it prices development risk at
zero. The bar has to be cap **plus** a premium, and the yield screen already
states that premium: 100 bps. Held against the same base, the two screens stop
disagreeing:

| yield on cost | spread vs cap | buyer IRR |
| --- | --- | --- |
| 4.5% | 0 bps | 4.23% |
| **5.5%** | **100 bps** ← the yield test | **5.55%** ← the IRR bar |
| 6.0% | 150 bps | 6.15% |
| 12.0% | 750 bps | 12.00% ← what a flat 12 demanded |

A flat 12% needed a **12% yield on cost** — 750 bps over the exit cap, a 2.7×
value on cost. It is a levered, growth-carrying convention: with rent growth
added, the same 6% YoC deal scores 7.97% at 2% growth and 8.92% at 3%, and
reaches 12% only at **6.3% annual NOI growth**. This module has no growth and
no financing by design, so importing the number was a category error.

Making it a spread also makes it **per lot**, which it has to be now that the
exit cap is: a pure-commercial scheme exits at 6.25% and must beat 7.25%; an
apartment block exits at 4.5% and must beat 5.5%. A single number set for the
apartment block would pass a retail deal returning less than buying the same
shops already built. `site_hurdle_irr_pct` reports the bar on every row.

And once the two screens share a base, the IRR test starts doing the job the
yield ratio structurally cannot — pricing the **timeline**. At a fixed 6% YoC:
12 months' build and a 6-month lease-up scores 6.21%, 24 and 24 scores 5.70%.
Fifty basis points from timing alone, decisive against a 5.5% bar and
invisible against a 12% one — and precisely the axis commercial absorption
moves.

`HURDLE_IRR=12` still restores the flat convention on every lot, and
`screen_assumptions` records which regime produced a flag.

Expect this to start flagging lots, and expect them to be **marginal by
construction**: they clear a 100 bps development spread, not a merchant
builder's return. A green flag here is not a 12% deal.

### The cap is blended to value the parts

The blend is the one cap that reproduces the building valued as the sum of its
separately-capitalised income streams:

    cap = sum(NOI_f) / sum(NOI_f / cap_f)

the harmonic mean, weighted by income. An NOI-weighted *arithmetic* mean — an
average of the two cap rates — is always the larger of the two and so always
values a mixed building under its parts: 14 bps high and 2.65% cheap on a
50/50 split, 9 to 13 bps on the real Villeray mixed lots. Only the harmonic
form makes `total NOI / cap` equal `sum(NOI_f / cap_f)`, which is the whole
property a blended cap is supposed to have.

Worked, on lot 3 456 608 — $37,989 of housing at 4.5% and $21,049 of commerce
at 6.25%:

    value = 37,989 / 0.045 + 21,049 / 0.0625 = $1,180,989
    cap   = 59,038 / 1,180,989              = 5.00%

against the 5.12% an average of the two caps would have said.

## The columns

Added to `gold.lot_investment_opportunities` beside the first axis:

| column | what |
| --- | --- |
| `existing_year_built`, `existing_num_storeys`, `existing_dominant_use_code` | what the screen read off the roll |
| `hbu_floors`, `hbu_footprint_m2`, `grid_zone` | what it read off the solver |
| `heritage_sector`, `piia_sector` | what it read off the grid |
| `storey_headroom`, `built_share`, `existing_footprint_m2`, `existing_footprint_coverage` | the derived inputs — the last is the *measured* footprint over the piece's ground, where `existing_footprint_m2` is the roll's floor over its storeys |
| `is_brownfield_use`, `is_unassessed_vacant`, `is_heritage_sector`, `has_piia_review`, `demolition_review_required`, `is_demolition_restricted` | the facts before the theses |
| `is_brownfield_site`, `is_teardown_site`, `is_infill_site`, `is_improvement_site`, `site_thesis` | the theses |
| `improvement_added_storeys`, `improvement_floor_m2`, `improvement_cost_cad`, `improvement_noi_cad`, `improvement_yield_pct` | the addition |
| `demolition_cost_cad`, `site_assessment_cost_cad`, `remediation_cost_cad`, `site_total_project_cost_cad`, `site_yield_on_cost_pct` | the site's own cost and yield |
| `site_thesis_rank`, `is_top_site_opportunity`, `num_ranked_in_site_thesis` | the rank within the site thesis |
| `redevelopment_npv_gain_cad`, `site_verdict_cad` | the rebuild's gain over holding as the gap states it, and the thesis's own gain with the site costs in |
| `enhance_*`, `hold_value_cad`, `enhance_value_cad`, `rebuild_value_cad`, `best_future` | the enhancement solve and the three futures, carried from the gap |
| `owner_*_value_cad`, `owner_gain_*_cad`, `owner_best_future` | the three futures for the owner, site costs on the rebuild |
| `acquisition_cost_cad`, `buyer_npv_*_cad`, `buyer_yield_*_pct`, `residual_price_*_cad`, `buyer_best_future` | the three futures for a buyer |
| `rebuild_budget_cad`, `rebuild_soft_cost_cad`, `rebuild_contingency_cad`, `rebuild_builders_risk_cad`, `rebuild_total_development_cost_cad`, `rebuild_lease_up_months`, and the `enhance_*` twins | the budget and the lease-up behind each future |
| `buyer_yoc_*_pct`, `buyer_irr_*_pct`, `buyer_multiple_*`, `owner_yoc_*_pct`, `owner_irr_*_pct`, `yoc_spread_*_bps`, `market_cap_rate_pct`, `comparable_cap_rate_pct` | the returns, both chairs |
| `site_irr_pct`, `owner_site_irr_pct`, `site_all_in_yield_on_cost_pct`, `site_yoc_spread_bps`, `site_hurdle_irr_pct`, `clears_cap_rate`, `clears_hurdle`, `is_good_candidate` | the thesis's own returns, the bar each was held against, and the screen |
| `hbu_residential_noi_cad`, `hbu_commercial_noi_cad`, `hbu_industrial_noi_cad`, and the `enhance_added_*_noi_cad` twins | the future's NOI by the family that earns it, summing to the whole; the weight behind every cap below |
| `hbu_commercial_floor_area_with_cellar_m2`, `hbu_industrial_floor_area_with_cellar_m2` | the non-residential floor the program would actually lease, cellar included, which the `hbu_*_floor_area_m2` columns exclude; the lease-up is measured on these |
| `market_cap_rate_enhance_pct`, `exit_cap_rate_rebuild_pct`, `exit_cap_rate_enhance_pct`, `hbu_non_residential_income_share`, `enhance_non_residential_income_share` | the cap each future is screened and sold at once blended to its own mix, and the share that did the blending |
| `hbu_parking_waived`, `hbu_waived_stalls`, `enhance_parking_waived`, `enhance_waived_stalls` | whether the rebuild and the addition were solved without the stalls they owe, and how many |
| `improvement_source`, `site_costs_cad`, `enhance_assumptions` | which of the solve and the estimate the addition is; the three site costs summed, which is what the rebuild carries and the other two futures do not; the enhancement's own settings |
| `feature_id`, `piece_area_m2`, `num_lot_zones`, `is_primary_zone` | the piece: which zone polygon, its ground, how many pieces the lot has, and whether this is the largest |

Every piece keeps its row; `site_thesis = 'none'` with the booleans false is a
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
`UNASSESSED_VACANT_MAX_COVERAGE`,
`DEMOLITION_COST_M2`, `DEMOLITION_COST_NONRES_M2`, `SITE_ASSESSMENT_COST`,
`REMEDIATION_RES_M2`, `REMEDIATION_NONRES_M2`, `ADDITION_PREMIUM`,
`REQUIRE_POSITIVE_NPV`, `SITE_TOP_N` — is listed by `make help`.

## Where it is read

The map draws an **Opportunities** layer from this table — the pieces with a
site thesis, coloured by it, with a filter per thesis, a *top of each* switch
and a green edge on the good candidates — and the **Deal** pane explains the
piece under *Why this site*: which conditions held, what the heritage rows
say, what each of the three futures builds, and what the site's own
denominator adds. The **Overview** pane totals the four theses for the
borough. The HBU and Buyer panes that used to split the programme from the
price were folded into Deal; `hbu_rag_map`'s README covers it.

## Open items

- **The heritage inventory.** The borough's list of 4,612 pre-1940 immovables
  with heritage value is the screen the statute actually names; the year is a
  proxy. It is one open-data download and one point-in-lot join.
- **The SCIAN join.** Replace the CUBF prefix list with Annexe III's own
  SCIAN codes through the codebook's correspondence column.
- **The building-value share**, as a second obsolescence signal for the
  teardown thesis.
- **Financing, taxes on the gain and rent growth**, on both futures. Soft
  costs, contingency and builder's risk are in the proforma returns since
  2026-09-10 but not in the solve's objective or in `site_verdict_cad`, so a
  rebuild can rank on an NPV its all-in yield does not clear.
- **The estimate under an infeasible enhancement.** Where the solve comes
  back `INFEASIBLE` — the standing plate is more than today's grid would let
  stand — the improvement thesis falls back to the closed-form storey-and-
  annex estimate, and on VSMPE that is 837 of the 912 improvements and 837 of
  the 839 ranked. A solve that says the building cannot grow under this grid
  and an estimate that adds a storey to it anyway are two answers on one
  row; the honest thesis for an infeasible enhancement is probably `none`, or
  the estimate should be reserved for the partitions the solve never ran on.
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
