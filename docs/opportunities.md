# Which lots to look at first

[`lot_redevelopment_gap`](assets.md) answers *how far is this lot from its
highest and best use* for every parcel in the borough. That is the right
question and the wrong shape to act on: twenty-odd thousand rows, most of them
uninteresting, sorted by nothing and faceted by nothing.

`gold.lot_investment_opportunities` turns it into a shortlist. It does exactly
two things the gap table does not — files each lot under an **investment
thesis**, and **ranks** the under-built ones within that thesis — and it
re-solves nothing.

```bash
make opportunities DATE=2026-08-26 NEIGHBORHOOD=VSMPE
make opportunities DATE=2026-08-26 NEIGHBORHOOD=VSMPE MIXED_MIN_SHARE=0.10 TOP_N=10
make opportunities DATE=2026-08-26 NEIGHBORHOOD=VSMPE LAND_FACTOR=1.3
```

The arithmetic is in [`urban_rag.opportunities`](../src/urban_rag/opportunities.py);
the asset is [`urban_rag.opportunity_assets`](../src/urban_rag/opportunity_assets.py);
the table is hbu_infra's `sql/021`.

## The thesis is what you would build

`investment_thesis` is read off the **proposed** program — the mix of
residential, commercial and industrial floor the solver would put on the lot —
not off the existing use.

A warehouse whose highest and best use is an apartment block is a
**residential** opportunity. Filing it under industrial because that is what
stands there today would put it in the one facet that will never look at it.

`existing_dominant_income_class` travels beside it, so a conversion play is one
predicate:

```sql
SELECT lot_number, existing_dominant_income_class, investment_thesis,
       yield_on_cost_pct, annual_stabilised_noi_gap_cad
  FROM gold.lot_investment_opportunities
 WHERE is_top_opportunity
   AND existing_dominant_income_class <> investment_thesis
 ORDER BY yield_on_cost_pct DESC;
```

Five values, four of them theses:

| thesis | when |
| --- | --- |
| `residential` | ≥ `dominant_share` of proposed floor is dwellings |
| `mixed_use` | the *smaller* of residential and commercial ≥ `mixed_min_share` |
| `commercial` | ≥ `dominant_share` commercial |
| `industrial` | ≥ `dominant_share` industrial |
| `none` | the solver produced no program |

A street and a park land in `none` and cannot be ranked, which is the whole of
what this asset has to do about them: `lot_highest_best_use` withholds the
program from a `road_parcel` and an `equipment_zone` lot, so no proposed floor
reaches the thesis rules above and no shortlist can contain one. Read
`hbu_status` on `gold.lot_highest_best_use` to tell those apart from a parcel
the solver merely found infeasible.

Where the lines fall is a mandate's judgement, so both thresholds are config and
both land on every row in `screen_assumptions`:

- **`dominant_share`** (0.85) — a building seven-eighths dwellings is
  residential even with a shop at the bottom.
- **`mixed_min_share`** (0.15) — roughly a ground floor under five or six
  residential storeys, which is where the commercial component stops being
  incidental and starts being something a lender asks about.

The two are deliberately **not complements**. Between them lies a band — one
class over 85%, the other under 15% — that resolves to the dominant class, and
that band is why a single threshold would not do. `ThesisRules` refuses settings
where they would contradict each other (a 0.7/0.4 pairing would make the
dominant rule unreachable).

A lot that is neither dominant nor mixed — a 60/40 residential/industrial split
— falls to whichever class is largest, because that is most of the building.

> Industrial does not mix here. The grids that authorise it in this borough
> authorise little else, and a warehouse with a sales counter is not a
> mixed-use investment.

## The rank is yield on cost

```
yield_on_cost_pct = 100 × hbu_annual_stabilised_noi_cad
                        / (hbu_total_capital_cost_cad + land × land_value_factor)
```

**Why not the raw NOI gap.** Ranking on `annual_stabilised_noi_gap_cad` sorts on
parcel size almost regardless of what a building costs, so every facet's top ten
becomes the ten biggest lots in the borough. Yield on cost is what a developer
actually compares two sites on, and it lets a small cheap parcel beat a large
dear one — which in the fixture borough is exactly what happens: a lot with a
$100k smaller gap wins its facet on a 15.4% yield against 6.3%.

The gap is the **tiebreak**, not the sort. Two sites at the same yield are
ordered by the dollars a year the redevelopment adds — return first, size
second, rather than a weighted score nobody can defend line by line.

**The land is in the denominator**, at its assessed value, and that is the one
judgement in the formula. A developer pays for the ground as well as the
building; leaving it out would rank a $4M teardown beside an empty lot as though
they cost the same to acquire. `land_value_factor` scales it — 1.0 costs the
land at the roll, honest for the same reason
[`market_value_factor`](comparables.md) defaults there.

`is_land_assessed` is false where the roll never reached the lot. Its land would
otherwise be counted at nothing and it would rank top of every facet, so the
yield and the rank are both NULL there instead.

**`thesis_rank` is within the thesis**, so rank 1 is the best residential play
*and* the best industrial one. A single borough-wide rank would bury every facet
under whichever happens to yield best — which is exactly what faceting is for.
`num_ranked_in_thesis` is the denominator that rank means nothing without: rank
12 of 14 and rank 12 of 900 are different answers.

## Every lot keeps its row

Three kinds of lot are unranked, and each says why:

| | `thesis_rank` | why |
| --- | --- | --- |
| already built to its envelope | NULL | `is_underbuilt = false` |
| the roll never assessed it | NULL | `is_land_assessed = false` |
| the solver found no program | NULL | `investment_thesis = 'none'` |

The table is an inventory with a shortlist marked in it, not the shortlist
alone — the same reason `lot_profiles` kept every lot rather than replacing
`vacant_lots` with a narrower selection. A screen is then a predicate rather
than a different table.

`is_top_opportunity` marks the first `top_n` of each thesis. It is a flag over
the rank, so changing the shortlist length moves that column and nothing else —
the cheapest of these settings to change your mind about.

## It is its own asset because it is cheap

Every input is a column `lot_redevelopment_gap` already wrote, so this is a
classification, a division and two sorts over one parquet file. Changing what
counts as mixed-use, or the land factor, or the shortlist length should cost
seconds — not a borough of CP-SAT models. That is the same split
`lot_redevelopment_gap` itself makes behind `lot_highest_best_use`, and
`lot_assessment_comparables` behind `lot_assessed_values`.

It also carries a **curated subset** of the gap row rather than all of it. The
per-class square-foot conversions, the binding caps, the parking and the storey
counts stay in `sql/019` and `sql/018`, one join away on `lot_uid`; what is here
is what a screening question needs to decide whether to open the parcel at all.
No geometry either — join `gold.lot_profiles` on `lot_number` for that.

## Reading a run

The facet summary is in the run's metadata rather than a second table, because
it is a `GROUP BY investment_thesis` over the rows the asset already writes:

```
VSMPE 2026-08-26: 22443 lot(s), 8104 ranked, 100 shortlisted -
  residential 6902 (best 12.4 pct), mixed_use 780 (best 9.1 pct),
  commercial 301 (best 7.8 pct), industrial 121 (best 6.9 pct)
```

`shortlist_noi_gap_millions` and `shortlist_project_cost_millions` are the
borough-scale pair: what the whole shortlist would add a year, and what it would
take to build.

## Where it sits

```
programs        →  silver/lot_development_programs   (the solve, expensive)
hbu             →  gold/lot_highest_best_use         (the choice)
                   gold/lot_redevelopment_gap        (the comparison)
opportunities   →  gold/lot_investment_opportunities (the shortlist)
```

No schedule yet, like the rest of the HBU chain — see
[assets.md](assets.md). `sql/021` has no `-- requires:` header, so the table
lands on the first `db.py init`.
