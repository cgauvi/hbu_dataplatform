# What age costs a building

An operating expense ratio is one number covering four things — taxes,
insurance, management and **maintenance** — and only the last of them depends
on how old the building is. Until this curve existed, `lot_assessment_comparables`
charged every lot in the borough the same 0.35, which means a 1910 walk-up and
a building finished this year were assumed to spend the same share of their rent
on the roof, the pointing, the risers and the wiring.

That assumption is not neutral. It is the one that most **flatters standing
stock against redeveloping it**, because the whole case for redevelopment is
that the new building earns more *and* costs less to keep — and a flat ratio
throws away the second half. `gold.lot_redevelopment_gap` is the table that
comparison lands in, so the flat ratio was quietly narrowing the gap the table
exists to find.

## The ratio is split in two

```
effective_operating_expense_ratio = operating_expense_ratio      (the base, a NEW building)
                                  + maintenance_premium(age)     (what age adds)
```

The base keeps the name, the default and the meaning `OPEX` always had, minus
the age: it is what a building costs to run when it is new. The premium is the
only part that varies per lot, and it is bounded so the effective ratio stays
strictly below 1 — a ratio of 1 is a building whose whole rent leaves again, and
past it a positive rent would produce a negative income.

| | Where it comes from | Measured? |
| --- | --- | --- |
| `year_built` | the roll (`rl0307a`), per lot | ✅ |
| reference year | the partition's own scrape date | ✅ |
| `maintenance_premium_per_year` | `MAINTENANCE_PER_YEAR`, default 0.0012 | stated |
| `max_maintenance_premium` | `MAX_MAINTENANCE`, default 0.10 | stated |
| `assumed_building_age_years` | `ASSUMED_AGE`, default 50 | stated |

**The age is measured and the curve is stated**, and the split is deliberate.
Statistics Canada publishes operating expenses for lessors of residential
buildings by geography and industry, and CMHC publishes rents and vacancies,
but **neither breaks maintenance out by age of building** — so there is no
series to read the curve off, and giving it a measured-looking provenance would
be worse than saying which it is. The three parameters are therefore stated,
they live in one place (`urban_rag.program`), and every row carries them in
`income_assumptions` so a rate can be read back against the curve that produced
it. 0.43 is a 1955 building on the default curve and a 1990 one on a steeper
one; the ratio alone does not say which.

The default curve is set from the span it has to cover rather than from one
year's accounts: repair and maintenance conventionally runs about a tenth of
effective gross income for newly built multi-residential and about a fifth for
pre-war walk-ups, so roughly ten points of gross separates the two ends of
Montreal's stock. At 0.0012 a year, a building reaches the 0.10 cap at 83 years
old.

| built | age in 2026 | premium | effective ratio |
| --- | --- | --- | --- |
| 2026 | 0 | 0.000 | 0.350 |
| 1990 | 36 | 0.043 | 0.393 |
| 1976 | 50 | 0.060 | 0.410 |
| 1920 | 106 | 0.100 (capped) | 0.450 |

The cap is there because maintenance on an old building is bounded by the fact
that an owner who stops spending on it stops collecting the rent as well: past
a point a building is renewed or it leaves the stock, and neither of those is
"the same building costing ever more".

## An unstated year is charged an age, not zero

A lot the roll states no `year_built` for is charged `assumed_building_age_years`.
Reading it as new would hand the least-documented buildings in the borough the
cheapest maintenance in it, and a building finished recently is precisely the
one that *has* a permit, a file and a year. The run reports
**`num_lots_age_assumed`** — counted off `year_built` rather than off the age,
since the age is never null once a reference year is set — so a partition whose
cap rates are being set by the assumption rather than by the roll is visible
rather than buried in an average.

## Where it feeds in

Three places, and the third is the one the change is for:

**1. `silver.lot_assessment_comparables`** — `annual_income` nets the gross with
the per-lot effective ratio instead of the partition-wide base. The gross is
untouched: what a triplex earns is a function of its dwellings and the rent, and
only what the owner *keeps* depends on the roof. Three columns travel beside it —
`building_age_years`, `maintenance_premium`, `effective_operating_expense_ratio` —
and they reach Postgres through that table's `attributes` jsonb catch-all, so no
migration is needed for them.

Everything downstream of `net_operating_income_cad` moves with it:
`cap_rate_pct`, `comparable_cap_rate_pct`, and both of those in
`gold.lot_profiles`.

**2. The reference year is the partition's, not the wall clock's.** Ages are
taken against `int(scrape_date[:4])`. A partition re-materialized next January
must produce the cap rates its key says it does, and `datetime.now()` here would
make every one of them drift by a year of maintenance.

**3. `gold.lot_redevelopment_gap`** — the two sides are now charged *different*
ratios, and that asymmetry is the point. `hbu.use_gap` reads the **base** off
the upstream's `income_assumptions`, and the building it proposes is new by
construction, so the base is the whole of its ratio. The existing building's NOI
arrives already net of its own age premium. Both ratios travel on the row —
`hbu_operating_expense_ratio` beside `existing_effective_operating_expense_ratio`,
with `existing_building_age_years` behind them — so a reader comparing two NOIs
can see they were netted differently and by how much, rather than discovering it
from the gap.

`existing_maintenance_penalty_cad` prices the premium on its own: the standing
building's gross times its premium, which is what age alone costs it in dollars
a year, and what redevelopment would stop paying. It is 0 on a building the
curve found new, which is the honest answer for a parcel whose maintenance
redeveloping would not improve.

> This is a change to what the numbers *mean*, not only to their inputs: every
> cap rate on a building older than new is lower than it was before the curve,
> because the building is now charged for its age. `MAINTENANCE_PER_YEAR=0`
> reproduces the flat ratio exactly, and is one setting rather than a code path.

```bash
make comparables DATE=2026-08-26 NEIGHBORHOOD=VSMPE
make comparables DATE=2026-08-26 NEIGHBORHOOD=VSMPE MAINTENANCE_PER_YEAR=0     # flat, as before
make comparables DATE=2026-08-26 NEIGHBORHOOD=VSMPE MAINTENANCE_PER_YEAR=0.002 MAX_MAINTENANCE=0.15
```

## What is still not modelled

The premium is a function of **age alone**. A gut-renovated 1910 triplex and a
neglected one are charged the same, because the roll records the year the
building went up and not the year anything in it was last replaced. A
building's *effective* age is the number an appraiser would reach for, and
nothing in this platform's sources publishes one.

The roll's own two year columns are `rl0307a`, the year, and `rl0307b`, which
says whether that year is known or estimated — it is `'R'` on 403 973 of
Montreal's 437 192 units in the 2026 roll and null on the rest, and it is *not*
a renovation date. Neither is read here beyond the year itself, so a unit whose
year is an assessor's estimate is charged exactly as one whose year is on a
permit. **17 164 of Montreal's units (3.9%) state no year at all** and are the
ones `assumed_building_age_years` is for.
