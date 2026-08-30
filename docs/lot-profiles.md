# Every lot, profiled

`lot_profiles` is the gold table of the lot lineage. Four upstreams each hold
one row per (lot × something), and each of them is the wrong shape for the
question a person actually asks:

| upstream | grain | what it contributes |
|---|---|---|
| `silver.building_lot_intersections` | (building, lot) | `num_buildings`, `built_area_m2`, `category` |
| `silver.lot_frontage` | (lot, street side) | `primary_*` and `secondary_*`, `num_frontages` |
| `rag.lot_documents` | (lot, feature, document) | `doc_*` and the `documents` array |
| `silver/lot_zoning_envelopes` | (lot, grid column) | `num_zoning_envelopes` and the `zoning_envelopes` array |
| `silver.lot_buildable_setbacks` | (lot, zone, grid column) | `buildable_area_m2`, `footprint_cap_m2` and the buildable figures merged into each `zoning_envelopes` entry |

Two more are already the right shape, and are the only upstreams here that are:

| upstream | grain | what it contributes |
|---|---|---|
| `silver.lot_assessed_values` | **lot** | `total_assessed_value`, `total_assessed_value_apportioned`, `num_assessment_units`, `num_shared_units`, `num_units_by_point`, `roll_year` |
| `silver.lot_assessment_comparables` | **lot** | `cap_rate_pct`, `comparable_cap_rate_pct`, `net_operating_income_cad`, `estimated_value_cad`, `assessed_to_estimated_ratio`, `num_dwellings`, the four floor areas, `dominant_use_code`, and the `comparables` / `income_assumptions` objects |

Both have `(scrape_date, neighborhood, lot_number)` as their primary key, so
neither needs a CTE or a pivot — a plain `LEFT JOIN` on `lot_number`, which cannot fan a lot
out into several rows. It is Quebec's *rôle d'évaluation foncière* carried onto
the cadastre, and it is what makes this table answer the whole of a
highest-and-best-use question rather than half of it: `overall_average_rent_cad`
is what a building earns, `construction_costs` is what it costs to put up, and
`total_assessed_value` is what the ground is already worth standing as it is.

**Two totals, and only one of them may be summed.** `total_assessed_value`
counts each assessment unit whole on every lot it covers — right for "what is
the property on this lot worth", wrong to `SUM()` across a borough, where it
over-counts by every unit that spans more than one lot.
`total_assessed_value_apportioned` divides each unit across its lots, so that
one adds up; `num_shared_units` is exactly where the two diverge. Both stay
**NULL rather than 0** on a lot no unit stands on, the same rule the frontage
measures follow — a lane carrying no assessed property is not a lane worth
nothing, and a reader averaging $0 across the borough's lanes would be
answering a question it did not ask.

**And what that ground *earns* on it comes from the second.** A total is half an
answer; whether $900,000 is a lot of money for a given lot is a cap rate on one
side and a comparable on the other, and `silver.lot_assessment_comparables` is
both. `cap_rate_pct` is the roll's dwellings and floor area priced at CMHC's
borough rent and at `urban_rag.program`'s stated non-residential rates, over
what the lot is assessed at. `estimated_value_cad` is what the k most similar
lots in the borough imply instead, and `assessed_to_estimated_ratio` is the two
side by side — the screen a highest-and-best-use question actually starts from:

```sql
SELECT lot_number, assessed_to_estimated_ratio, footprint_cap_m2
  FROM gold.lot_profiles
 WHERE assessed_to_estimated_ratio < 0.7
   AND buildable_area_m2 > built_area_m2
 ORDER BY assessed_to_estimated_ratio;
```

The counts `COALESCE` to 0 and the measures do not, the same split every other
join here makes — with one exception worth naming: **`num_dwellings` stays
NULL**, because there it is a measure. A lot no assessment unit stands on has no
dwelling count; one carrying a warehouse has a count of 0, and coalescing the
first would make the two read alike. The whole asset, and every stated
assumption behind those two rates, is [comparables.md](comparables.md).

Four more contribute nothing per-lot at all, and are written identically onto
every row of the partition:

| upstream | grain | what it contributes |
|---|---|---|
| `silver/vacancy_rates` | (dwelling type, bedroom class) | `vacancy_rates`, `overall_vacancy_rate_pct` |
| `silver/average_rents` | (bedroom class) | `average_rents`, `overall_average_rent_cad` |
| `bronze/montreal_nonresidential_costs` | (building type) | the parking half of `construction_costs`, `underground_stall_cost_low/high_cad`, `above_grade_stall_cost_low/high_cad` |
| `bronze/montreal_residential_costs` | (building type) | the condo half of `construction_costs`, `condo_cost_low/high_cad_sqft` |

Somebody asking "what can I do with lot 1 234 567" wants one row. This asset is
where they collapse onto it, and every one of the per-lot five arrives by
`LEFT JOIN`: a lot no building touches, a lot facing no street, a lot no
document covers, a lot no readable grid reaches and a lot no assessment unit
stands on are each a real answer, and an inner join would delete exactly those
rows.

The last five never reach Postgres as tables. `lot_zoning_envelopes` is
staged into a temp table keyed on `lot_number` — not `lot_uid`, which is a
bigserial `load_lots` mints again on every reload — and aggregated by the same
kind of `LEFT JOIN` as the rest. The two CMHC objects are passed as jsonb
parameters, because CMHC surveys neighborhoods and publishes no geometry: there
is nothing per-lot about them and nothing to join on. The two cost snapshots
are one jsonb parameter between them, for a stronger version of the same
reason — the Altus guide prices nine Canadian *markets* and no geometry at all,
so a Montreal rate is a Montreal rate on every lot of every borough. They are
also the only upstream here partitioned **by date alone**: every borough of a
day reads the same two files, and a partition missing one fails naming a date
rather than a borough.

## Why it is not `vacant_lots`

It used to be. That asset selected the parcels carrying nothing bigger than a
shed, which made it a table that could answer *where is the empty land* and
nothing else — every lot its `WHERE` clause dropped was a lot the reader could
no longer see, so "the widest built lots on this street" or "how much of the
borough is built out" needed a different table over the same join.

Keeping every lot and carrying `has_building` alongside costs one boolean
column and turns the old question into a filter over an inventory:

```sql
SELECT lot_number, primary_street_name, round(primary_frontage_m::numeric, 1)
FROM gold.lot_profiles
WHERE neighborhood = 'VSMPE' AND scrape_date = '2026-08-18'
  AND NOT has_building
ORDER BY primary_frontage_m DESC NULLS LAST
LIMIT 20;
```

`category` is carried forward unchanged, so the three-way distinction that
asset drew is not lost — `no_building`, `shed_only`, and `building_sliver` for
the corner of a neighbour's triplex crossing the cadastral line. It gains a
fourth value, `built`, for the case the old table expressed by having no row.

## `has_building` is not the negation of "vacant"

It is `num_buildings > 0` — "does any footprint intersect this parcel" — which
a 12 m² shed satisfies. Whether a lot is *usably* empty depends on a threshold,
a threshold is a judgement about the built form rather than a property of the
data, and a boolean cannot carry one. So that lives in `category`, computed
against `LotProfilesConfig.max_built_area_m2`, which every row records the way
`silver.lot_frontage` records its `buffer_m`:

```
make lot-profiles DATE=2026-08-18 NEIGHBORHOOD=VSMPE
```

The two columns are worth having side by side because they answer different
questions: one is a fact about the footprints, the other is this judgement
applied to them.

## The two frontages

`silver.lot_frontage` is one row per (lot, street side), ranked longest first;
these are its top two, pivoted. Ranks beyond the second are counted in
`num_frontages` and summed into `total_frontage_m` rather than given a third
pair of columns every other row would leave empty. Which edge is primary is
read off that table's own `frontage_rank` rather than re-decided here, so the
two cannot disagree.

A lot facing no street reports `num_frontages = 0` and **NULL** metres — not
`0`, which would claim it was measured. `num_without_frontage` in the run's
metadata is the number to watch, for the same reason it is in `lot_frontage`:
a few percent are true interior parcels, a third of the borough is a street
snapshot that stopped short.

## The linked PDF

The last hop of the chain `silver.lot_features` opens up. `rag.chunks.feature_ids`
records which map features cite each indexed PDF, and `rag.lot_documents` puts
that together with the features covering a lot. The highest-coverage document
**across every layer** is flattened into `doc_url`/`doc_title` so the common
read is a column rather than a jsonb path, and the whole set travels in
`documents` as JSON, most-of-the-lot first:

```json
[{"source_table": "vsp_reg_zone", "feature_id": "C01-001",
  "doc_id": "…", "url": "https://…/grille.pdf",
  "title": "Grille des usages et des normes C01-001", "pct_of_lot": 100.0}]
```

That column is written to the parquet as a JSON **string**, not as a nested
type. Letting pyarrow infer one types the column from whatever the partition
happens to hold — a borough where no lot has a document infers `list<null>` and
one where they do infers `list<struct<…>>`, which is two files with different
schemas for the same asset. Postgres keeps real `jsonb` to query; `json.loads`
on read is the cost. It is the same rule that makes `_fetch_partition`
re-select `scrape_date` as text.

`num_with_documents` at zero with a healthy `num_lots` means `rag.chunks` holds
no corpus for that partition, not that the borough is unzoned.

## The zoning envelopes

`silver/lot_zoning_envelopes` is one row per (lot, grid column) — the grain
[`urban_rag.program`](../src/urban_rag/program.py) solves at — and
`zoning_envelopes` is that lot's rows, the zone covering most of it first:

```json
[{"feature_id": "C01-001", "source_table": "Reglement_urbanisme__VSP_REG_ZONE",
  "pct_of_lot": 92.0, "column_index": 0, "usages": ["H.1", "H.2"],
  "levels": ["1", "2"], "permits_residential": true, "floors_max": 3,
  "min_lot_width_m": 7.6, "site_coverage_max_pct": 70.0,
  "governs_residential": true, "solver_ready": true}]
```

Every column of that table travels except the lot's own — its area, its two
frontages, its neighborhood and scrape date are already columns of the profile
row and would only be restated once per envelope. A norm restated per envelope
is the trade the denormalisation makes; the lot's area restated per envelope is
just waste. `usages`, `levels` and `parse_notes` are decoded back into real
JSON on the way in: they are strings in the parquet so the file's schema is the
same shape in every partition, and `jsonb` has no such problem.

`governs_residential` marks the column `select_residential_column` picks for
that lot's width. There is at most one per (lot, zone) and often none, so it is
not flattened into a column of its own the way `doc_url` is.

`num_without_zoning_envelopes` is the number to watch. A lot with no envelope
is one no readable grid reaches — either the cadastre stretches past the feature
scrape, or that zone's PDF failed to parse — and `lot_zoning_envelopes` reports
the same gap from its own side as `num_lots_unzoned`. If the *staged* count and
the landed count disagree, the envelope file names lots this partition's
cadastre does not have, which is what a stale
`silver/lot_zoning_envelopes` looks like from here; the run logs it as a
warning naming the asset to re-materialize.

## The borough's rental market

`vacancy_rates` and `average_rents` are CMHC's grids for the borough, as one
object each:

```json
{"survey_year": 2023, "survey_period": "octobre 2023",
 "num_quartiers_mapped": 3, "overall_vacancy_rate_pct": 0.5,
 "num_published_cells": 4,
 "cells": [{"dwelling_type": "all", "bedroom_type": "all",
            "vacancy_rate_pct": 0.5, "min_vacancy_rate_pct": 0.3,
            "max_vacancy_rate_pct": 0.7, "num_quartiers": 2,
            "averaged_quartiers": "Parc-Extension, Villeray"}]}
```

Identical on every lot of the partition, which is the point: the question this
table is read for — what is this parcel worth building — is asked one lot at a
time and answered against the market the lot sits in. The provenance sits above
the cells because a borough figure is the unweighted mean of its quartiers with
most cells suppressed, so `num_quartiers` is the denominator that mean was
actually taken over, and the survey year says which publication it came from.

Every cell travels, suppressed ones included — a suppressed rate is a fact
about the survey, and an absent cell would read as a gap in the pipeline.
`overall_vacancy_rate_pct` and `overall_average_rent_cad` are the `all`/`all`
cells flattened onto columns of their own, the same rule `doc_url` follows, and
they are read back **out of the jsonb in SQL** rather than passed beside it, so
the column and the object it was flattened from cannot disagree. An empty `{}`
is a partition whose CMHC silver asset has not run; an object whose cells all
read null is a borough CMHC suppresses entirely. Only the first is a pipeline
problem.

## What it costs to build there

The rents are half of *what is this parcel worth building*. `construction_costs`
is the other half — Montreal's column of the Altus Group Canadian Cost Guide,
as the two `bronze/montreal_*_costs` snapshots publish it:

```json
{"city": "mtl", "city_label": "Montreal",
 "source_last_modified": "Tue, 12 Aug 2026 09:31:00 GMT",
 "cost_scrape_date": "2026-08-20",
 "condo_band": "condo_wood",
 "underground_stall_cost_low_cad": 51925, "underground_stall_cost_high_cad": 68675,
 "above_grade_stall_cost_low_cad": 38500, "above_grade_stall_cost_high_cad": 57750,
 "condo_cost_low_cad_sqft": 225, "condo_cost_high_cad_sqft": 290,
 "parking": [{"id": "parkade_ug", "label": "Parking – Underground Garage",
              "cat": "parking", "unit_flag": "perStall",
              "rate_low": 51925, "rate_high": 68675},
             {"id": "parkade_ag", "...": "..."}],
 "residential": [{"id": "condo_wood", "label": "Wood Frame Condo (Up to 6 Storeys)",
                  "cat": "residential", "unit_flag": null,
                  "rate_low": 225, "rate_high": 290},
                 {"id": "condo_12", "...": "..."}]}
```

**The two families are not in the same unit, and mixing them is the mistake the
guide's own `perStall` flag exists to prevent.** `parking` is dollars per
*stall*; `residential` is dollars per square foot. That is why they travel
under separate keys and why `unit_flag` rides on every entry — a reader who
takes a list whole still has the publisher's own answer to what a figure buys.

Six rates are flattened onto columns, the same rule `doc_url` and
`overall_average_rent_cad` follow, and read back **out of the jsonb in SQL**
for the same reason: a column and the object it came from cannot be allowed to
disagree. The parking pair is the choice a building actually makes — stalls dug
out underneath (`parkade_ug`) or a garage integrated into it at grade
(`parkade_ag`); underground is dearer per stall *and* larger per stall, while
above grade burns floor area the envelope would rather spend on dwellings,
which is what makes it a choice at all. `urban_rag.program` hardcodes the
midpoints of exactly these two pairs today; these columns are where it can read
them from instead. The guide's third parking type, `surface_lot`, is
deliberately not carried: an asphalt lot is not a parking structure.

`condo_cost_low/high_cad_sqft` is **one** of the five condominium / apartment
bands the guide prices by storey count. Which one is
`LotProfilesConfig.condo_type_id`, and it is config rather than a constant for
the same reason `max_built_area_m2` is — wood frame up to six storeys is what a
borough of triplexes builds, and a downtown parcel under a 40-storey envelope
is not that building at any price. The band chosen is named on every row as
`condo_band`, so a table can always be read back against the assumption that
produced it, and the other four bands stay in `residential` whatever it is set
to.

A type the guide stops publishing is a warning and a NULL column, not a failed
partition: losing one of sixty rates should not cost a borough its cadastre. A
*configured* band that was never a band is different — that is a typo in the
run config, and it fails before Postgres is touched.

