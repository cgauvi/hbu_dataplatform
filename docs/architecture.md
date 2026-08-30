# Architecture

## Layers

The pipeline is a medallion: **bronze** holds what a publisher returned,
**silver** the same facts at this platform's grain, **gold** one question
answered. The layer is declared once, in
[layers.py](../src/urban_rag/layers.py), and both the Dagster asset key
(`silver/lot_zoning_envelopes`) and the prefix the asset writes under
(`<root>/silver/lot_zoning_envelopes/...`) are derived from it — so the two
cannot drift. `definitions.py` refuses to load if an asset has no declared
layer, or if a declared layer names an asset nothing registers.

What each layer promises a reader:

| Layer | Contract | Fails when |
| --- | --- | --- |
| **bronze** | What the publisher returned, plus `scrape_date` / `scraped_at` / `source_*`. Invalid rings survive, CMHC's respellings survive, a text column stays text. Scoping a query (one borough's outline handed to Infolot, the Montreal slice of a national survey) is a bound on the request, not an interpretation of the answer — so it stays bronze. | the fetch fails |
| **silver** | EPSG:4326, geometry valid, the crosswalks in `partitions.py` applied, one row per declared grain. | a crosswalk names something the snapshot does not publish; a declared grain is breached |
| **gold** | Named for the question, at the grain whoever asks it reads. | its upstream partition was never loaded |

Postgres is a **serving copy** of silver and gold, never the only copy, and
**the schema a table is in is the layer its asset is in**: `silver/vacancy_rates`
in the tree is `silver.vacancy_rates` in the database, and `gold/lot_profiles`
is `gold.lot_profiles`. Every one of those tables is partitioned by
`(neighborhood, scrape_date)` and written by one upsert — see [The silver and
gold tables](#the-silver-and-gold-tables).

Two things in Postgres sit outside that rule and are not exceptions to it.
`rag.lots`, `rag.buildings` and `rag.features` are *bronze* snapshots loaded
into PostGIS because the silver joins are computed over them there; `rag.chunks`
is the pgvector index `document_index` publishes. Neither is a silver or gold
dataset's own table.

The tree is the record — losing the database costs a reload rather than a
re-scrape, which for a live municipal source no later run can undo.

## The silver and gold tables

Every silver and gold dataset has one table, in the schema its layer is named
for, and one way of being written to it — `urban_rag.warehouse`, which is the
single writer. Before it, four assets reached Postgres through a loader each,
all of them writing into `rag`, all of them deleting a partition and
re-inserting it; the seven that did not reach Postgres at all had no table to
reach.

Three rules hold for every one of those tables.

**The schema is the layer.** Looked up from `urban_rag.layers` rather than
written down twice, so moving an asset between layers moves its table with it
instead of leaving the two disagreeing.

**The grain is `(neighborhood, scrape_date, natural key)`.** Each table is
partitioned `PARTITION BY LIST (neighborhood)` and then `PARTITION BY RANGE
(scrape_date)` by month, so a borough's month is a leaf a reader's `WHERE`
prunes to. Postgres requires a partitioned table's unique constraint to contain
its partition keys, which is not a tax here but the grain restated — and it is
exactly what a write conflicts on:

```sql
INSERT INTO silver.neighborhood_streets (...)
VALUES (...)
ON CONFLICT (scrape_date, neighborhood, cote_rue_id)
DO UPDATE SET ...
```

| Table | Conflicts on, beyond the partition |
| --- | --- |
| `silver.vacancy_rates` | `dwelling_type`, `bedroom_type` |
| `silver.quartier_vacancy_rates` | `quartier`, `dwelling_type`, `bedroom_type` |
| `silver.average_rents` | `bedroom_type` |
| `silver.quartier_average_rents` | `quartier`, `bedroom_type` |
| `silver.building_lot_intersections` | `building_uid`, `lot_uid` |
| `silver.lot_features` | `lot_uid`, `source_table`, `feature_id` |
| `silver.neighborhood_streets` | `cote_rue_id` |
| `silver.lot_frontage` | `lot_uid`, `cote_rue_id` |
| `silver.document_chunks` | `chunk_id` |
| `silver.zoning_grid_columns` | `source_table`, `feature_id`, `column_index` |
| `silver.lot_zoning_envelopes` | `lot_uid`, `feature_id`, `column_index` |
| `silver.lot_buildable_setbacks` | `lot_uid`, `feature_id`, `column_index` |
| `silver.lot_assessed_values` | `lot_number` |
| `silver.assessment_units` | `id_provinc` |
| `gold.lot_profiles` | `lot_number` |

**A write is an upsert, and a partition is still a snapshot.** The frame is
COPYed into a staging table shaped `LIKE` the target, upserted in one
statement, and the partition's rows the staging table does not have are then
deleted. The upsert is what lets a re-run land while readers are querying —
nothing is ever missing mid-load, the way a delete-then-insert leaves it — and
the prune is what keeps snapshot semantics, which the upsert alone cannot: a lot
that disappears from the cadastre has no row to conflict with and would
otherwise sit there forever.

That last half also settles what to do about a key that does not survive a
re-scrape. `silver.building_lot_intersections` conflicts on `building_uid`, a
bigserial `load_buildings` mints again on every load, so a re-run upserts
nothing and prunes everything — which is precisely the delete-then-insert that
table needs. The mechanism is one; what changes per table is only which columns
are the key.

Partitions are created on demand by hbu_infra's `warehouse.ensure_partition`,
called with the partition about to be written, so a borough enabled for the
first time and the first load of a new month both just work. It is deliberately
not a `DEFAULT` partition: rows that land in a default cannot be moved by
attaching the partition they belong in.

Every asset reports what it published in its run metadata: the parquet-first
ones as `<dataset>_rows_upserted` (and `<dataset>_rows_pruned` when the prune
found anything), the PostGIS ones alongside the counts they already report —
`num_building_lot_rows_pruned`, `num_profiles_pruned`. A prune of zero is the
steady state; a prune of four thousand on a re-run is the borough's cadastre
having moved under the partition.

Those PostGIS joins take the same road by a different door. Their rows are
computed in the database and never pass through Python, so they go through
`warehouse.upsert_select` instead: the statement lands in the same staging
table, and the same upsert and prune run over it.

One asset writes a partition it was not asked for, and it is the third door.
`assessment_units` merges a publication that has no borough axis at all — one
provincial roll, one date partition — so it hands `publish_by_neighborhood` a
frame *per borough*, cut by where each unit's point falls, and every one of
them is upserted in a single transaction. Each borough is still pruned against
its own frame alone, so a unit that moved across a borough line between scrapes
leaves the partition it left and not the one it joined. See
[`src/urban_rag/warehouse.py`](../src/urban_rag/warehouse.py).

## Output layout

Layer first, then one prefix per asset, keyed by scrape date and then by
borough:

```
<root>/<layer>/<asset>/<YYYY-MM-DD>[/<neighborhood>]/
```

```
data/
├── bronze/
│   ├── spectrum_table_catalog/2026-08-18/
│   │   └── tables.parquet
│   ├── neighborhood_features/2026-08-18/VSMPE/
│   │   ├── Apaisement__VSP_TRA_AFFICHEUR.parquet
│   │   ├── Reglement_urbanisme__VSP_REG_ZONE.parquet
│   │   └── ...
│   ├── reference_neighborhoods/2026-08-18/
│   │   ├── quartiers.parquet
│   │   └── nombre_logements.parquet
│   ├── neighborhood_lots/2026-08-18/VSMPE/
│   │   └── lots.parquet
│   ├── neighborhood_buildings/2026-08-18/VSMPE/
│   │   └── buildings.parquet
│   ├── cmhc_vacancy_survey/2026-08-18/
│   │   └── quartier_vacancy_rates.parquet
│   ├── cmhc_rent_survey/2026-08-18/
│   │   └── quartier_average_rents.parquet
│   ├── street_network/2026-08-18/
│   │   └── street_sides.parquet
│   ├── montreal_residential_costs/2026-08-18/
│   │   └── residential_costs.parquet
│   ├── montreal_nonresidential_costs/2026-08-18/
│   │   └── non_residential_costs.parquet
│   ├── property_assessment_roll/2026-08-18/
│   │   ├── rol_unite_p.parquet       # one point per assessment unit
│   │   ├── unite_evaln.parquet       # what the roll says about each
│   │   └── lot_cadst.parquet         # one row per (unit, lot it covers)
│   └── linked_documents/2026-08-18/VSMPE/
│       └── documents.parquet
├── silver/
│   ├── assessment_units/2026-08-18/
│   │   └── assessment_units.parquet
│   ├── lot_assessed_values/2026-08-18/VSMPE/
│   │   └── lot_assessed_values.parquet
│   ├── vacancy_rates/2026-08-18/VSMPE/
│   │   ├── vacancy_rates.parquet
│   │   └── quartier_vacancy_rates.parquet
│   ├── average_rents/2026-08-18/VSMPE/
│   │   ├── average_rents.parquet
│   │   └── quartier_average_rents.parquet
│   ├── building_lot_intersections/2026-08-18/VSMPE/
│   │   ├── building_lots.parquet
│   │   └── lot_features.parquet
│   ├── neighborhood_streets/2026-08-18/VSMPE/
│   │   └── neighborhood_streets.parquet
│   ├── lot_frontage/2026-08-18/VSMPE/
│   │   └── lot_frontage.parquet
│   ├── zoning_grid_columns/2026-08-18/VSMPE/
│   │   └── zone_columns.parquet
│   ├── lot_zoning_envelopes/2026-08-18/VSMPE/
│   │   └── lot_zoning_envelopes.parquet
│   ├── document_chunks/2026-08-18/VSMPE/
│   │   └── chunks.parquet
│   └── document_embeddings/2026-08-18/VSMPE/
│       └── embeddings.parquet
└── gold/
    └── lot_profiles/2026-08-18/VSMPE/
        └── lot_profiles.parquet
```

`<root>` is `data/` by default and `s3://$S3_BUCKET/` when that is set — see
[S3 output](setup.md#s3-output). One prefix per asset means a partition can be listed,
copied or dropped without touching what another asset wrote for the same day;
the layer above it means silver and gold can be dropped and rebuilt wholesale
(`make clean-silver`) while the bronze snapshots — the part no later run can
re-fetch — stay put.

Nothing reads a layer name off a hard-coded string. `ParquetStore.partition_dir`
takes an asset name and finds the layer itself, so an asset reading its
upstream's output does not have to know which layer that upstream is in.

The keys are bare values, not hive `key=value` pairs, so `neighborhood` and
`scrape_date` are written as **columns** instead of being recovered from the
path. A file that is copied out of the tree still knows which snapshot it
belongs to:

```python
import geopandas as gpd

zones = gpd.read_parquet("data/neighborhood_features/2026-08-18/VSMPE/Reglement_urbanisme__VSP_REG_ZONE.parquet")
zones.crs        # EPSG:4326
zones.columns    # source table attributes + source_table + neighborhood
                 #   + scrape_date + scraped_at
```

The whole history still reads back as one dataset — `read_parquet` over
`data/neighborhood_features/**/*.parquet`, then group by those two columns.

Geometry is reprojected to EPSG:4326 **server side** (`MI_Transform`) — the
service stores it in `epsg:42104`, an MTM-zone-8 variant, and exposes no
output-SRS parameter. Tables with no geometry column land as plain parquet;
the MapInfo `MI_Style` column is dropped everywhere.

Each partition is a full snapshot: re-materializing clears the directory first,
so a table that disappears upstream does not linger as a stale file.

Geometries are written exactly as the service returns them, including the
handful with self-intersecting rings that shapely rejects (5 of 3,135 rows in
the first VSMPE snapshot). They are counted in the `num_invalid_geometries`
materialization metadata and logged per table; repair with
`gdf.geometry.make_valid()` downstream if a consumer needs it.

