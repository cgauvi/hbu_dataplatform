# Architecture

## Layers

The pipeline is a medallion: **bronze** holds what a publisher returned,
**silver** the same facts at this platform's grain, **gold** one question
answered. The layer is declared once, in
[layers.py](../src/hbu_dataplatform/core/layers.py), and both the Dagster asset key
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
`scrape_date` and one of two spatial columns — see [Two spatial
axes](#two-spatial-axes) — and written by one upsert — see [The silver and
gold tables](#the-silver-and-gold-tables).

Two things in Postgres sit outside that rule and are not exceptions to it.
`rag.lots`, `rag.buildings`, `rag.features` and `rag.addresses` are *bronze*
snapshots loaded into PostGIS because the silver joins are computed over them
there; `rag.chunks` is the pgvector index `document_index` publishes. Neither
is a silver or gold dataset's own table.

The tree is the record — losing the database costs a reload rather than a
re-scrape, which for a live municipal source no later run can undo.

## Package layout

`src/hbu_dataplatform/` is grouped by what a module is about, not by which
medallion layer it writes. Each subpackage owns the pure logic (clients,
parsers, the solver) *and* the Dagster assets and resources that wrap it, so
`definitions.py` is the only module that has to know about every package.
Every `__init__.py` is empty on purpose: packages depend on each other's
modules, never on a package object, which is what keeps the graph free of
import cycles.

```
hbu_dataplatform/
  definitions.py        the code location: the ASSETS list, jobs, schedules, resources
  dagster_home.py       writes dagster.yaml from the environment, then execs the command
  core/                 generic plumbing, imports nothing outside itself
    storage  warehouse  layers  frames  postgis  digest  tile_grid  tile_cut
    open_data           a CKAN client with no portal baked in
    http                USER_AGENT and the CA bundle every client sends and trusts
    pg                  PgSettings: where Postgres is and how to authenticate
    resources           ParquetStore, PostgisResource, CkanResource
  partitions/
    axes                the date, neighborhood and tile axes and their helpers
    cities              City, and the switch from a key to the city it is in
    guards              the bronze scrape-month guard
    neighborhoods, tiles   the two CLIs
  cities/               one package per publisher city
    montreal/           registry (boroughs, Spectrum namespaces, CMHC quartiers,
                        MarketBeat submarkets), spectrum, costs/, rents/
    quebec_city/        registry (arrondissements, quartiers), zoning, council/
    saguenay/           registry (the one key, the limit layer), zoning
  sources/              province- or nation-wide publishers, one package each:
                        infolot, bdoi, addresses, roll, cubf, rfu, rqtt, cmhc
                        (client, assets, resources), plus donnees_quebec
  boundaries/           reference_neighborhoods and the borough/city outlines
  zoning/               the grid parser, the zoning features, envelopes, pieces, setbacks
  cadastre/             the lot chain's joins: cadastre, building intersections, frontage
  hbu/                  program (CP-SAT), hbu, massing, proforma, opportunities,
                        comparables, and their assets
  map/                  layer specs, the MVT render, PMTiles, the two map assets
  rag/                  documents, chunks, embeddings, the DuckDB and pgvector stores,
                        the corpus assets and resources, the `urban-rag` CLI
```

Three edges are worth knowing. `partitions.cities` reads the three city
registries, which import nothing, so adding a city is a registry module plus
a line in `cities.py`. The zoning parsers (`zoning.zoning_grid`,
`cities.*.zoning`) import `hbu.program` for the zone-column vocabulary while
`hbu`'s assets import `zoning`'s; that is a package-level loop but not a
module-level one, because `program.py` imports nothing from the package.
And `partitions.guards` lives beside the axes rather than in `core` because
it reads the scrape timezone the date axis is cut in.

## Two spatial axes

Every partitioned table has `scrape_date` as one key. Which spatial column is
the other is the table's *axis* (`hbu_dataplatform.core.warehouse.Axis`), and there are two.

**The borough axis** — `neighborhood`, a key from
[partitions.py](../src/hbu_dataplatform/partitions/axes.py): `VSMPE`, `CIL`, `SAG` — is the
**publisher's** unit. Bronze is fetched per borough because that is what a
publisher answers for: a Spectrum namespace, an arrondissement outline handed
to Infolot, a grid PDF per by-law. What stays borough-shaped downstream is what
a publisher bounds — the CMHC and C&W tables, `zoning_grid_columns`,
`assessment_units`, the corpus, and the map's aggregates and tiles.

**The tile axis** — `cell_partition`, a cell of the *tile cut* — is the
**computation's** unit, and the whole lot chain runs on it: the seventeen
tables from `silver.building_lot_intersections` to `gold.lot_building_massing`.
A cell is a Web Mercator tile named by its quadkey — `0302303330102` is
thirteen digits, so zoom 13 — and the cut
([tile_cut.py](../src/hbu_dataplatform/core/tile_cut.py)) is a checked-in set of cells at
whatever depth holds about 20,000 lots: z13 over Montreal, z10–z12 over Quebec
City, seeded from the cadastre by `scripts/seed_tile_cut.py` and never
re-derived, because a Dagster partition key that moved would orphan everything
under it. Every row of `rag.lots`, `rag.buildings`, `rag.features` and
`rag.addresses` carries `cell_key`, the zoom-19 quadkey of its interior point,
written on load; `cell_partition` is the cut cell that prefixes it. A row of
the lot chain inherits its lot's; a street side is placed by its midpoint and
an address by its own point.

Why a cell and not a borough: boroughs get redrawn (Montreal's in 2002 and
2006, Quebec City's in 2009) and range from VSMPE's 17 km² to Saguenay's
1,150; a cell is permanent, about one run's worth of lots wherever it is, and
the cells run in parallel. And a re-cut, when a cell outgrows its budget, is a
string operation — the new partition is a longer prefix of a `cell_key` the
row already has, and four children sum exactly to their parent — where a
borough redraw is a point-in-polygon over the province and no arithmetic
reconciles the before and after.

**Reads go global.** A tile run *writes* only rows whose cell is its own, but
*reads* the whole snapshot — `scrape_date` alone, no spatial predicate:
neighbouring lots for adjacency and slivers, every building for the clip,
every street side for frontage, every address for the snap. Nothing spatial
is cut by a cell edge; the cell is a write-ownership claim, not a scope. That
is what removed the artefacts the old `WHERE neighborhood = X` reads left along
every borough line — frontage under-measured against a clipped street side, a
comparables pool that stopped at the boundary, false slivers along it, an
address 2 m over the line that never snapped.

**The hop between the axes** is `neighborhood_cadastre` (silver, borough axis):
it lands a borough's three bronze snapshots in `rag.*` with their cell
addresses, fails on ground the cut does not cover (`num_rows_outside_cut` —
the cut only covers what has been loaded, so a fourth city is a re-seed before
a load), and reports `tiles_touched`: the tile runs that have to follow,
because a reload remints `lot_uid` and cascades into every one of them. A tile
asset that reads a borough-axis upstream — the CMHC rents, the C&W rents, the
grids — asks which boroughs its lots came from (`postgis.neighborhoods_of_tile`)
and joins each lot on its own `neighborhood`, which every moved row still
carries as an indexed attribute: the map, `map_cell_aggregates` and `map_tiles`
read by borough and are untouched. In Dagster that dependency is a
`MultiPartitionMapping` — identity on `date`, every partition on the spatial
dimension.

## The silver and gold tables

Every silver and gold dataset has one table, in the schema its layer is named
for, and one way of being written to it — `hbu_dataplatform.core.warehouse`, which is the
single writer. Before it, four assets reached Postgres through a loader each,
all of them writing into `rag`, all of them deleting a partition and
re-inserting it; the seven that did not reach Postgres at all had no table to
reach.

Three rules hold for every one of those tables.

**The schema is the layer.** Looked up from `hbu_dataplatform.core.layers` rather than
written down twice, so moving an asset between layers moves its table with it
instead of leaving the two disagreeing.

**The grain is `(partition, scrape_date, natural key)`.** Each table is
partitioned `PARTITION BY LIST (<its axis>)` — `cell_partition` for the lot
chain, `neighborhood` for what a publisher bounds — and then `PARTITION BY
RANGE (scrape_date)` by month, so a partition's month is a leaf a reader's
`WHERE` prunes to. Postgres requires a partitioned table's unique constraint
to contain its partition keys, which is not a tax here but the grain restated
— and it is exactly what a write conflicts on:

```sql
INSERT INTO silver.lot_frontage (...)
VALUES (...)
ON CONFLICT (scrape_date, cell_partition, lot_uid, cote_rue_id)
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
| `silver.lot_addresses` | `address_id` |
| `silver.document_chunks` | `chunk_id` |
| `silver.zoning_grid_columns` | `source_table`, `feature_id`, `column_index` |
| `silver.lot_zone_pieces` | `lot_uid`, `feature_id` |
| `silver.lot_zoning_envelopes` | `lot_uid`, `feature_id`, `column_index` |
| `silver.lot_buildable_setbacks` | `lot_uid`, `feature_id`, `column_index` |
| `silver.lot_development_programs` | `lot_uid`, `feature_id`, `column_index` |
| `silver.assessment_units` | `id_provinc` |
| `silver.lot_assessed_values` | `lot_number` |
| `silver.lot_assessment_comparables` | `lot_number` |
| `silver.commercial_rents` | `rent_class` |
| `gold.lot_profiles` | `lot_number` |
| `gold.lot_highest_best_use` | `lot_uid`, `feature_id` |
| `gold.lot_redevelopment_gap` | `lot_uid`, `feature_id` |
| `gold.lot_investment_opportunities` | `lot_uid`, `feature_id` |
| `gold.lot_building_massing` | `lot_uid`, `feature_id` |
| `gold.lot_surface_parking` | `lot_uid`, `feature_id` |
| `gold.map_cell_aggregates` | `layer`, `cell_z`, `cell_x`, `cell_y` |

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
called with the partition about to be written — a borough key or a cut cell,
the function does not care which — so a borough enabled for the first time, a
cell first written to and the first load of a new month all just work. It is
deliberately not a `DEFAULT` partition: rows that land in a default cannot be
moved by attaching the partition they belong in.

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
[`src/hbu_dataplatform/core/warehouse.py`](../src/hbu_dataplatform/core/warehouse.py).

## A load ends by refreshing the statistics it invalidated

Every write path above closes with an `ANALYZE`, and it is there for a reader
rather than for the writer.

A load rewrites most of a partition. `warehouse` upserts the whole thing and
prunes what the staging table did not hold; `postgis._replace_partition`, which
loads the three `rag` working-set tables, deletes the borough's rows and COPYs
them back. Either way the planner's statistics are left describing whatever was
there before the load. Autovacuum gets to them eventually, and *eventually* is
the problem: the window between a load finishing and the statistics catching up
is exactly when somebody opens the map to look at what was loaded.

What goes wrong in that window does not look like a statistics problem, which
is why it is worth writing down. `map_tiles` renders the cadastre, the
footprints, the zoning layer, the utilisation shading and the proposed massing
as **vector tiles** — one `ST_AsMVT` statement per band of 256-pixel tiles,
thousands of tiles per borough, each a GiST lookup narrowed by
`(neighborhood, scrape_date)`. Handed stale row counts the planner
mis-estimates that filter's selectivity, drops the index scan for a sequential
one, and every band becomes a scan of the borough. The run does not fail. It
takes hours instead of minutes, on precisely the partition that was most
recently loaded — and `hbu_rag_map`'s panes, which still read by viewport,
meet the same plan. (The map's tiles themselves no longer touch the database
at all: they are the PMTiles archives that asset writes, fetched off S3.)

Two details of how it is done:

**The leaf, not the parent.** `ANALYZE` on a partitioned table walks every
partition under it, which for a table holding a year of boroughs is most of the
load's runtime again for statistics no query needed refreshed. So
`warehouse.ensure_partition` now returns the leaf name hbu_infra's function has
always handed back, and `_merge` analyzes that. The three `rag` tables are not
partitioned, so `postgis.analyze` takes the table itself.

**Inside the load's transaction.** `ANALYZE` is permitted in a transaction
block — unlike `VACUUM` — and takes only a `ShareUpdateExclusiveLock`, so it
blocks other maintenance and no reader. A load that rolls back rolls its
statistics back with it, which is the right outcome: the rows it would have
described are not there either.

An emptied partition is analyzed too. A borough that loaded nothing this time
is as much a change of shape as one that loaded everything, and the planner is
as wrong about it.

## Output layout

Layer first, then one prefix per asset, keyed by scrape date and then by the
spatial partition — a borough key for what a publisher bounds, a cell of the
tile cut for the lot chain (see [Two spatial axes](#two-spatial-axes)):

```
<root>/<layer>/<asset>/<YYYY-MM-DD>[/<neighborhood> | /<tile>]/
```

```
data/
├── bronze/
│   ├── spectrum_table_catalog/2026-09-01/
│   │   └── tables.parquet
│   ├── neighborhood_features/2026-09-01/VSMPE/
│   │   ├── Apaisement__VSP_TRA_AFFICHEUR.parquet
│   │   ├── Reglement_urbanisme__VSP_REG_ZONE.parquet
│   │   └── ...
│   ├── reference_neighborhoods/2026-09-01/
│   │   ├── quartiers.parquet
│   │   └── nombre_logements.parquet
│   ├── neighborhood_lots/2026-09-01/VSMPE/
│   │   └── lots.parquet
│   ├── neighborhood_buildings/2026-09-01/VSMPE/
│   │   └── buildings.parquet
│   ├── cmhc_vacancy_survey/2026-09-01/
│   │   └── quartier_vacancy_rates.parquet
│   ├── cmhc_rent_survey/2026-09-01/
│   │   └── quartier_average_rents.parquet
│   ├── street_network/2026-09-01/
│   │   └── street_segments.parquet
│   ├── montreal_residential_costs/2026-09-01/
│   │   └── residential_costs.parquet
│   ├── montreal_nonresidential_costs/2026-09-01/
│   │   └── non_residential_costs.parquet
│   ├── property_assessment_roll/2026-09-01/
│   │   ├── rol_unite_p.parquet       # one point per assessment unit
│   │   ├── unite_evaln.parquet       # what the roll says about each
│   │   └── lot_cadst.parquet         # one row per (unit, lot it covers)
│   └── linked_documents/2026-09-01/VSMPE/
│       └── documents.parquet
├── silver/
│   ├── assessment_units/2026-09-01/
│   │   └── assessment_units.parquet
│   ├── lot_assessed_values/2026-09-01/0302303330102/
│   │   └── lot_assessed_values.parquet
│   ├── vacancy_rates/2026-09-01/VSMPE/
│   │   ├── vacancy_rates.parquet
│   │   └── quartier_vacancy_rates.parquet
│   ├── average_rents/2026-09-01/VSMPE/
│   │   ├── average_rents.parquet
│   │   └── quartier_average_rents.parquet
│   ├── neighborhood_cadastre/2026-09-01/VSMPE/
│   │   └── cadastre.json               # what landed in rag.*, and which cells
│   ├── building_lot_intersections/2026-09-01/0302303330102/
│   │   ├── building_lots.parquet
│   │   └── lot_features.parquet
│   ├── neighborhood_streets/2026-09-01/0302303330102/
│   │   └── neighborhood_streets.parquet
│   ├── lot_frontage/2026-09-01/0302303330102/
│   │   └── lot_frontage.parquet
│   ├── zoning_grid_columns/2026-09-01/VSMPE/
│   │   └── zone_columns.parquet
│   ├── lot_zoning_envelopes/2026-09-01/0302303330102/
│   │   └── lot_zoning_envelopes.parquet
│   ├── document_chunks/2026-09-01/VSMPE/
│   │   └── chunks.parquet
│   ├── document_embeddings/2026-09-01/VSMPE/
│   │   └── embeddings.parquet
│   ├── lot_zone_pieces/2026-09-01/0302303330102/
│   │   └── lot_zone_pieces.parquet
│   ├── lot_addresses/2026-09-01/0302303330102/
│   │   └── lot_addresses.parquet
│   ├── lot_buildable_setbacks/2026-09-01/0302303330102/
│   │   └── lot_buildable_setbacks.parquet
│   └── lot_development_programs/2026-09-01/0302303330102/
│       └── lot_development_programs.parquet
└── gold/
    ├── lot_profiles/2026-09-01/0302303330102/
    │   └── lot_profiles.parquet
    ├── lot_highest_best_use/2026-09-01/0302303330102/
    │   └── lot_highest_best_use.parquet
    ├── lot_redevelopment_gap/2026-09-01/0302303330102/
    │   └── lot_redevelopment_gap.parquet
    ├── lot_investment_opportunities/2026-09-01/0302303330102/
    │   └── lot_investment_opportunities.parquet
    ├── lot_building_massing/2026-09-01/0302303330102/
    │   └── lot_building_massing.parquet   # two geometry columns:
    │                                      #   the building and its asphalt
    ├── map_cell_aggregates/2026-09-01/VSMPE/
    │   └── map_cell_aggregates.parquet
    └── map_tiles/2026-09-01/VSMPE/      # not parquet: one PMTiles archive
        ├── zones.pmtiles                 #   per map layer, read by hbu_rag_map
        ├── lots.pmtiles                  #   straight off S3 with range requests
        ├── ...
        └── map_tiles.json                # which archives exist, and what each holds
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

The keys are bare values, not hive `key=value` pairs, so the partition —
`neighborhood` or `cell_partition` — and `scrape_date` are written as
**columns** instead of being recovered from the path, which is also why a
borough key and a cell can share the slot: the file says which it is. A file that is copied out of the tree still knows which snapshot it
belongs to:

```python
import geopandas as gpd

zones = gpd.read_parquet("data/neighborhood_features/2026-09-01/VSMPE/Reglement_urbanisme__VSP_REG_ZONE.parquet")
zones.crs        # EPSG:4326
zones.columns    # source table attributes + source_table + source_namespace
                 #   + neighborhood + scrape_date + scraped_at
```

`source_namespace` is the unit the *publisher* files the layer under — Montreal's
Spectrum namespace `19_VSMPE`, and the city for Quebec and Saguenay, which each
publish one zoning layer and no namespace at all. It is what makes `C01-001` in
one borough a different feature from `C01-001` in the next, now that
`source_table` is the slug and the slug drops it. See
`hbu_dataplatform.partitions.axes.cities.source_namespace_for` and
hbu_infra's `027_features_source_namespace.sql`.

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
