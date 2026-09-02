# What a lot yields, and which lots are like it

[assessment-roll.md](assessment-roll.md) ends with `silver.lot_assessed_values`
— one row per lot, and what the property standing on it is assessed at. That
number answers half a question. The other half is whether it is *a lot of
money for this lot*, and there are exactly two ways to ask:

- **A cap rate.** Net operating income over value. The roll publishes the
  denominator and this asset builds the numerator.
- **A comparable.** What the most similar lots nearby are assessed at, per
  dwelling and per square metre, applied back to the subject.

`silver.lot_assessment_comparables` computes both, and feeds them to
`gold.lot_profiles`.

```bash
make comparables DATE=2026-08-26 NEIGHBORHOOD=VSMPE
make comparables DATE=2026-08-26 NEIGHBORHOOD=VSMPE OPEX=0.40 K_COMPARABLES=12
```

The arithmetic is in [`urban_rag.comparables`](../src/urban_rag/comparables.py),
which has no Dagster imports; the partition handling is in
[`urban_rag.comparables_assets`](../src/urban_rag/comparables_assets.py). The
table is hbu_infra's `sql/016_silver_lot_assessment_comparables.sql`.

## It re-derives the placement rather than copying the totals

`lot_assessed_values` sums one column — `rl0404a` — over the assessment units
standing on each lot, and keeps the totals but not the (unit, lot) pairs it
summed them over. A cap rate needs the *dwellings* that earn the income and the
*floor* that houses it, and a comparable needs to know what kind of thing the
lot is. None of that survives a sum over values, so the pairs are built again
here.

They are built by the same function, not a second copy of it:
`role_assets.place_units_on_lots` is what both assets call — the roll's own
cadastre crosswalk first, then the assessment point for the units it cannot
place. Two implementations of that join would be two answers to which lot a
property is on.

**`place_unmatched_by_point` must match the run that produced the partition.**
It decides which units reach a lot at all, so a mismatch would sum the
characteristics here over a different set of units than the totals there. The
asset checks: it compares its own unit count against the one it carried in and
reports `num_units_disagreeing`, which is 0 on a healthy partition.

The value columns are **carried, not recomputed**. `total_assessed_value` and
its apportioned twin travel through untouched, so the two silver tables cannot
end up disagreeing about what a lot is worth.

## The income side, and what is measured in it

The roll says what stands on the lot. CMHC says what a dwelling rents for here.
Cushman & Wakefield say what a square foot of office and warehouse rents for in
this borough's submarket. Every row carries the lot in `income_assumptions`.

| | Where it comes from | Measured? |
| --- | --- | --- |
| dwellings, floor area, use code | the roll (`rl0311a`, `rl0308a`, `rl0105a`) | ✅ |
| average rent, vacancy rate | CMHC, for this borough | ✅ |
| office, industrial $/sqft/yr | C&W MarketBeat, escalated by StatCan — [commercial-rents.md](commercial-rents.md) | ✅ |
| retail $/sqft/yr | `RETAIL_BASE`, escalated by StatCan's retail index | stated |
| their vacancies | `urban_rag.program`, 7% | stated |
| operating expense ratio (new build) | `OPEX`, default 0.35 | stated |
| maintenance premium for age | the roll's `year_built` on a stated curve — [maintenance.md](maintenance.md) | age measured, curve stated |
| market value factor | `MARKET_FACTOR`, default 1.0 | stated |

```
residential  = num_dwellings × rent × 12 × (1 − vacancy/100)
retail       = retail_floor_m2     / M2_PER_SQFT × retail_rate     × (1 − 0.07)
office       = office_floor_m2     / M2_PER_SQFT × office_rate     × (1 − 0.07)
industrial   = industrial_floor_m2 / M2_PER_SQFT × industrial_rate × (1 − 0.07)

commercial_income_cad    = retail + office
gross_income_cad         = the classes that could be priced, added
net_operating_income_cad = gross × (1 − effective_operating_expense_ratio)
                                          └─ OPEX + maintenance_premium(age)
cap_rate_pct             = 100 × NOI / (assessed_value × market_value_factor)
```

**Commerce is charged in two halves and reported as one.** The CUBF's 4000s are
retail and its 5000s and 6000s are offices and services, and the two rent
dollars a square foot apart — so `rent_class_of` splits them where
`income_class_of` does not. `commercial_floor_area_m2` and
`commercial_income_cad` keep exactly the meaning they had; `retail_*` and
`office_*` travel beside them.

`rent_provenance` in `income_assumptions` names, per class, which publisher,
which quarter, which submarket and whether the figure was measured, escalated or
stated. A rate with no provenance beside it cannot be read against next
quarter's.

**The floor is split by each unit's own use code, not by the lot's.** A unit is
one *unité d'évaluation* with one `rl0105a`, so the class its whole floor
belongs to is a property of the row rather than a judgement about the parcel — a
triplex over a dépanneur gets both a residential and a commercial income because
its two units say so. `dominant_use_code` exists for reading and filtering, not
for the arithmetic, and `dominant_use_description` beside it is the MEFQ's
own words for that code — *Garage de stationnement pour automobiles* rather
than `4611`. It is carried across from `silver.assessment_units`, which
merged the codebook onto every unit, so nothing here looks a code up twice;
it is for **reading only**, since two editions of the manual can word one
code differently and the code is what a filter should match. See
[the use code, and what it says](assessment-roll.md#the-use-code-and-what-it-says).

The CUBF's leading digit is the category, and that is all `use_class_of` reads
— with one wrinkle that is easy to get wrong. **Manufacturing is written `2-3`
in the manual**: industry is numbered 2000 through 3999, so the nine categories
do not sit on nine digits, and reading them as if they did mislabels every
class from 3 up by one.

| leading digit | CUBF class | priced as | charged as |
| --- | --- | --- | --- |
| 1 | Habitation | residential | residential |
| 2, 3 | Industries manufacturières | industrial | industrial |
| 4 | Transports, communications et services publics | industrial | industrial |
| 5 | Commerciale | commercial | retail |
| 6 | Services | commercial | office |
| 7 | Culturelle, récréative et de loisirs | commercial | office |
| 8 | Production et extraction de richesses naturelles | industrial | industrial |
| 9 | Immeubles non exploités et étendues d'eau | nothing | nothing |
| anything else | `unknown` | nothing | nothing |

The categories are Annexe 2C.1's own, and `bronze/cubf_use_codes` snapshots
that annexe — so this table can be read back against the publisher rather than
taken on trust.

The mapping from eight classes to three is this platform's judgement and lives
in one constant (`CUBF_CLASSES`). Floor the classifier cannot place earns
nothing rather than being priced at the average of a guess — the run reports
`unclassified_floor_area_ha` so the size of that is visible.

**Vacancy is netted per class and the expense ratio once at the end.** They are
different things — income never collected, against collected income that leaves
again — and applying either to the other's result would charge one of them
twice.

**Null, not zero, where a class was not priced.** A borough CMHC suppressed the
rent for has a null residential income however many dwellings stand in it, and
`gross_income_cad` is null only when *no* class could be priced. A lane with
neither dwellings nor floor is the second; a triplex in a suppressed borough is
the first, and a shop in that same borough still has a rate, because its income
never depended on CMHC.

> A cap rate is a **ratio**, so the whole-counting is safe. A unit spanning
> several lots contributes its dwellings and floor to each of them exactly as it
> contributes its whole value to each in `total_assessed_value`. That
> over-counts a borough on both sides of the fraction and cancels in the
> quotient: a shared triplex makes both lots report the yield of a triplex.
> `cap_rate_pct` is not a column to `SUM()` in any case.

**An assessed value is not a market value.** Quebec's roll is triennial and
every unit in it is valued as of one reference date; the province publishes a
*facteur comparatif* to carry a roll figure to a market one, and it is not in
this publication. `MARKET_FACTOR=1.0` therefore reports the yield **on the
roll** and says so on every row.

## The comparables: one distance, five features

For each lot, the `k` most similar lots in the borough — similar on what it is
used for, how big the parcel is, how much floor stands on it, how many
dwellings that floor holds, and how far away it is. Not a filter and a sort:
each feature becomes a dimensionless distance, and the composite is the
weighted Euclidean norm over them.

```
d(i,j) = sqrt( Σ  wᶠ · dᶠ(i,j)² )
```

`ComparableWeights` states both halves of every judgement in one place — the
*scale* that turns metres or a size ratio into a comparable number, and the
*weight* that says how much that number counts:

| feature | scale (one unit is…) | weight |
| --- | --- | --- |
| ground distance | 500 m (`distance_scale_m`) | 1.0 |
| lot area | a factor of 2 (`size_ratio_scale`) | 1.0 |
| floor area | a factor of 2 | 1.0 |
| dwellings | a factor of 2 | 1.0 |
| use code | same code 0 · same class 0.35 · neither 1.0 | **1.5** |

So a lot 500 m away, one twice the size, and one from a different CUBF class
are all roughly one unit apart. Use leads at 1.5 because a comparable of the
wrong kind is not a comparable at any distance — the test
`test_the_same_use_code_beats_a_nearer_lot_of_another_class` pins exactly that.

**Size is compared as a ratio and distance is not.** Twice the floor area is the
same difference in kind at 100 m² as at 2,000, while 300 m is 300 m anywhere in
the borough — a log on the second would make the far side of the parcel and the
far side of the borough look alike. Sizes go through `log1p`, so a lot with no
floor sits at the origin of the axis instead of at a negative infinity on it.

**The use code is never read as a magnitude.** 1000 and 4000 are residential and
commercial, not three thousand apart.

**A feature neither side can state costs `missing_penalty` (1.0).** A lot no
assessment unit stands on has no floor area, no dwelling count and no use code —
not a zero of each. Dropping those components would make a lane the nearest
neighbour of every lane; reading them as zero would make it the nearest
neighbour of every empty warehouse. It is a stated cost instead, and it is why
an unassessed lot's comparables come out chosen mostly on size and distance,
which is all that is actually known about it.

### Every lot is a subject; only a valued lot is a candidate

A lane, a park or a city parcel gets a neighbour list and can never be *in* one.
A comparable with no assessed value contributes no dollars per dwelling and no
dollars per square metre, which is the only thing a neighbour is consulted for.

That asymmetry is the point: it is what values a vacant parcel off the built
ones around it. `num_candidates` is reported per run, so a thin pool is visible
rather than inferred.

**The pool is this borough.** `lot_assessed_values` is partitioned by borough,
so the valued lots available as comparables are the ones in the same partition,
and a parcel on the boundary draws its neighbours from its own side of it. That
is a limitation of how the upstream is partitioned rather than a modelling
choice.

**The search is exact and quadratic, in chunks.** No tree and no spatial
pre-filter: the composite metric is not the one a KD-tree would index, and
pruning on ground distance alone would drop the identical triplex two streets
over in favour of the warehouse next door. 512 subjects are scored against every
candidate at a time, which holds the working set to ~90 MB against a borough's
22,000 valued lots. `max_distance_m` bounds the *answer*, not the work — a lot
with nothing inside it reports an empty list rather than a far one.

## From comparables to a value

Three median ratios, each taken over the neighbours that *have* it — so a
commercial comparable with no dwellings sits out the per-dwelling median and
counts in the other two:

- `comparable_value_per_dwelling_cad`
- `comparable_value_per_floor_m2_cad`
- `comparable_value_per_land_m2_cad`

Median rather than mean, for the reason every appraisal takes medians: one
condominium tower's common-parts lot carrying 402 units and $258M sits in the
same neighbour list as four triplexes, and a mean would let it decide the answer
on its own.

`estimated_value_cad` applies the first ratio the lot can actually use, and
`estimated_value_basis` names which:

| basis | when | applied to |
| --- | --- | --- |
| `per_dwelling` | the lot has dwellings | `num_dwellings` |
| `per_floor_area` | it has floor area | `floor_area_m2` |
| `per_land_area` | always available | `lot_area_m2` |
| `none` | no comparable had any ratio | — |

Ground area is last and is what makes the column complete — it is the only ratio
a parcel carrying nothing can be valued on. Three estimates arrived at three
ways are not interchangeable, so the basis travels beside the number rather than
being inferred from what else the row has.

> `lot_area_m2` is the **polygon's** area, projected to EPSG:32188 — not the
> roll's `rl0302a`, which is carried as `roll_land_area_m2` for reading only.
> The polygon has an area for every lot including the unassessed ones, and a
> divided co-ownership states the whole parcel's superficie on *every one* of
> its apartments, so summing the roll's column over a 402-unit tower measures
> the same ground four hundred times.

### The screen

`assessed_to_estimated_ratio` is the two put side by side, and is what this
asset exists for. Well under 1 is a parcel the roll values below what its own
neighbours imply — either a mispricing or a lot doing less with its ground than
the ones around it. Against `gold.lot_profiles`, that is one predicate:

```sql
SELECT lot_number, assessed_to_estimated_ratio, footprint_cap_m2, buildable_area_m2
  FROM gold.lot_profiles
 WHERE neighborhood = 'VSMPE' AND scrape_date = '2026-08-26'
   AND assessed_to_estimated_ratio < 0.7
   AND buildable_area_m2 > built_area_m2
 ORDER BY assessed_to_estimated_ratio;
```

A parcel the roll values below its neighbourhood, with room left under its
zoning to do something about it. `cap_rate_pct` sorts the same inventory by
what it currently earns instead; both columns are indexed on both tables.

`comparable_cap_rate_pct` is the same income over the *estimated* value, so it
differs from `cap_rate_pct` by exactly `assessed_to_estimated_ratio`. A lot
where the two are far apart is a lot whose assessment and whose neighbourhood
are telling different stories.

## What reaches gold

`gold.lot_profiles` joins this table on `lot_number` — a plain `LEFT JOIN`, like
`lot_assessed_values`, because it is already one row per lot. Seventeen columns
land: the roll's characteristics (`num_dwellings`, the four floor areas,
`dominant_use_code`, `dominant_use_description`, `year_built`), the income
pair, the two cap rates, the
estimate and its basis, the ratio, `num_comparables`, and the two jsonb objects
`comparables` and `income_assumptions`.

The counts `COALESCE` to 0 and the measures do not — the same split every other
join into that table makes. **`num_dwellings` is the one count that stays
NULL**, because there it is a measure: a lot no assessment unit stands on has no
dwelling count, while one carrying a warehouse has a count of 0, and coalescing
the first would make the two read alike.

## Where it sits in the day

Behind `lot-values` (06:30) and both CMHC silver assets (05:55, 05:58), and
ahead of `lot_profiles`:

```
roll            04:52   →  assessment_units
lots            05:40   →  neighborhood_lots
vacancy/rents   05:55/58
lot-values      06:30   →  silver.lot_assessed_values
comparables     06:40   →  silver.lot_assessment_comparables
lot-profiles     —         (unscheduled; see assets.md)
```

`sql/016` has no `-- requires:` header, so the table lands on the first
`db.py init` and the asset is scheduled normally — the same footing `sql/013`
and `sql/014` are on.
