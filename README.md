# urban_rag

Dagster pipeline that snapshots Montreal's [Spectrum Spatial Feature
Service](https://spectrum.montreal.ca/connect/analyst/controller/connectProxy/rest/Spatial/FeatureService)
into local (geo)parquet, partitioned by neighborhood and scrape date.

Currently enabled: **VSMPE** (Villeray–Saint-Michel–Parc-Extension, 24 tables).

## Assets

| Asset | Partitions | Output |
| --- | --- | --- |
| `spectrum_table_catalog` | date | The service's full table list for that day (577 tables across 19 namespaces), as one `tables.parquet` |
| `neighborhood_features` | date × neighborhood | One parquet file per source table |
| `reference_neighborhoods` | date | Montreal's 91 housing reference neighborhoods from donnees.montreal.ca, with their dwelling counts |
| `neighborhood_lots` | date × neighborhood | Every cadastral lot inside that borough, from Quebec's Infolot service, as one `lots.parquet` |
| `vacancy_rates` | date × neighborhood | CMHC Rental Market Survey vacancy rates for that borough, averaged over the survey's own neighborhoods |
| `average_rents` | date × neighborhood | CMHC HMIP average rents for that borough, averaged by bedroom type over the survey's own neighborhoods |
| `lots_with_vacancy_rates` | date × neighborhood | Cadastral lots with the CMHC vacancy-rate grid joined by neighborhood name, before spatial joins |
| `linked_documents` | date × neighborhood | The PDFs those tables link to, fetched and flattened to text |
| `document_chunks` | date × neighborhood | Those documents cut into retrieval-sized chunks |
| `document_embeddings` | date × neighborhood | A bge-m3 vector per chunk |
| `document_index` | date × neighborhood | Those vectors upserted into the Postgres/pgvector store the query side reads |

The last four build the retrieval corpus and are described under [The document
corpus](#the-document-corpus) and [The shared vector
store](#the-shared-vector-store).

The catalog is a separate asset because the published table list drifts —
boroughs add and retire layers without notice, and a scrape is only
reproducible if you know what existed on that date. `neighborhood_features`
reads that day's `tables.parquet` through a
`MultiToSingleDimensionPartitionMapping`, so the `2026-08-18|VSMPE` partition
consumes exactly the `2026-08-18` catalog.

## Output layout

Every asset owns one prefix, keyed by scrape date and then by borough:

```
<root>/<asset>/<YYYY-MM-DD>[/<neighborhood>]/
```

```
data/
├── spectrum_table_catalog/2026-08-18/
│   └── tables.parquet
├── neighborhood_features/2026-08-18/VSMPE/
│   ├── Apaisement__VSP_TRA_AFFICHEUR.parquet
│   ├── Reglement_urbanisme__VSP_REG_ZONE.parquet
│   └── ...
├── reference_neighborhoods/2026-08-18/
│   └── ...
├── neighborhood_lots/2026-08-18/VSMPE/
│   └── lots.parquet
├── vacancy_rates/2026-08-18/VSMPE/
│   ├── vacancy_rates.parquet
│   └── quartier_vacancy_rates.parquet
├── average_rents/2026-08-18/VSMPE/
│   ├── average_rents.parquet
│   └── quartier_average_rents.parquet
└── lots_with_vacancy_rates/2026-08-18/VSMPE/
    └── lots_with_vacancy_rates.parquet
```

`<root>` is `data/` by default and `s3://$S3_BUCKET/` when that is set — see
[S3 output](#s3-output). One prefix per asset means a partition can be listed,
copied or dropped without touching what another asset wrote for the same day.

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

## The reference neighborhoods

The Spectrum layers are borough-scoped. The city's housing division also
publishes an island-wide division into 91 *quartiers de référence en
habitation* — historical, socio-economically homogeneous units used for
housing analysis — through the open-data portal rather than through Spectrum:

- [donnees.montreal.ca/dataset/quartiers](https://donnees.montreal.ca/dataset/quartiers),
  CC BY 4.0, updated irregularly.

`reference_neighborhoods` snapshots it per scrape date. There is no borough
axis to partition on, so this asset is partitioned by **date only**:

```
<root>/reference_neighborhoods/2026-08-20/
├── quartiers.parquet          # the layer, EPSG:4326, one row per quartier
└── nombre_logements.parquet   # dwellings per quartier, 2017 assessment roll
```

The two join on `no_qr`. Column names are lower-cased on the way in, because
the portal spells the same field `No_QR` in one file and `no_qr` in the other,
and identifiers stay text so the zero padding (`01`..`91`) survives; `nb_log`
is the one column cast to an integer. `source_file`, `scrape_date` and
`scraped_at` are added to both.

The dataset publishes the layer as SHP and CSV too, but those are the same rows
with the geometry zipped or dropped, so only the GeoJSON is read. Resources are
looked up by the filename their download URL ends in, not by resource id or
title: CKAN mints a new id whenever the city replaces a file, and both files
are published under the same French title.

A failure on the layer fails the partition; a failure on the dwelling counts
only skips that file and lands in the `dwellings_error` metadata, the same way
one unreadable Spectrum layer does not cost a whole borough.

## The cadastral lots

Boroughs publish zoning; the *lots* those rules apply to are provincial data,
from the Registre foncier's **Infolot** service. `neighborhood_lots` snapshots
every lot intersecting one borough per scrape date:

```
<root>/neighborhood_lots/2026-08-20/VSMPE/
└── lots.parquet     # 24 953 lots, EPSG:4326, 1 730 ha
```

Useful columns: `NO_LOT` (the lot number, `"2 170 935"`), `VA_SUPRF_LOT_CALCL`
(area in m², computed from the geometry) next to `VA_SUPRF_LOT` (as declared),
`CO_STATT_LOT` with `DA_STATT_LOT` (status and since when), and
`DH_DERNR_MODFC_GEOMT` (last edit to the geometry — the column to diff two
scrape dates on). The two epoch-millisecond columns are converted to UTC
timestamps on the way in; everything else is written as the service returned
it.

Lots have no borough of their own, so the boundary comes from Montreal's side:
the asset dissolves that borough's quartiers out of `reference_neighborhoods`
and hands the outline to Infolot as the query geometry. That makes it the one
asset that joins the two sources, and it is why it depends on the open-data
snapshot for the same date. A lot straddling a border is returned for **both**
boroughs — the query keeps whatever intersects, rather than cutting geometry.

### Why it is read the way it is

Infolot is an ArcGIS `MapServer` behind a GeoCortex security module, and three
things about it decide the shape of `urban_rag.infolot`:

- **Not every service answers.** `Infolot` is open; its sibling
  `Infolot_Anonyme` returns HTTP 500 from the security module to everything,
  `?f=json` included. There is no token to obtain — the open one is the one to
  use.
- **Paging is advertised but broken.** The layer reports
  `supportsPagination: true`, yet the same window asked for in pages of 200
  returns 124, 836 or 47 rows depending on the offset, with
  `exceededTransferLimit` unset. A paged read truncates silently.
- **So reads are two-phase.** `returnIdsOnly` ignores `maxRecordCount` and
  returns every matching id in one response; those ids are then fetched in
  batches of 250 by `objectIds`. A batch that comes back short is an error,
  not a warning — that is the whole point of fetching by id. Batches are
  POSTed, because a borough outline alone runs to ~100 KB of ring coordinates.

The layer's `minScale` only stops the lots from *drawing* on a zoomed-out map;
`/query` ignores it.

> The MERN WMS at `geoegl.msp.gouv.qc.ca/apis/mern/cadastre` renders the same
> lots, and is what a map viewer uses. It is not usable as a source here: it
> answers 403 to any request without a `Referer` from the one origin its
> gateway allows, its WFS `GetFeature` is blocked, and `GetFeatureInfo` — the
> only vector way out — is capped at 1:20 000, so it needs thousands of tiled
> requests and still returns a different set at each scale (a 500 m tile
> returned 80 lots where its four 250 m sub-tiles returned 122).

## The vacancy rates

`vacancy_rates` reads CMHC's [Rental Market
Survey](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market/urban-rental-market-survey-data-vacancy-rates)
— one workbook a year, every Canadian centre in it — and keeps the slice where
`Province == "Qc"` and `Centre == "Montréal"`:

```
<root>/vacancy_rates/2026-08-20/VSMPE/
├── vacancy_rates.parquet             # 15 rows: the borough average
└── quartier_vacancy_rates.parquet    # 45 rows: what it averaged
```

CMHC surveys the Montreal **census metropolitan area** and cuts it into its own
neighborhoods, which do not line up with the boroughs everything else here is
partitioned on. `VSMPE` is three of them, `Outremont` is one, and `PR` is the
borough *plus* Senneville, which CMHC will not split out. The crosswalk is
`CMHC_QUARTIERS` in [partitions.py](src/urban_rag/partitions.py), a third map
alongside the Spectrum namespaces and the borough codes.

It holds one canonical name per quartier, not one per publication: CMHC
*respells* names between survey years — 2022 prints `South West ~ Sud-Ouest`
where 2023 prints `Sud-Ouest`, and swaps a slash for a hyphen in Pierrefonds'
— so names are matched with case, accents and punctuation collapsed, and the
bilingual half dropped. That is deliberately not fuzzy: two names differing by
a letter still differ, a sheet where two quartiers collapse to the same key is
refused, and a quartier the map names but the workbook does not publish **fails
the partition** rather than quietly shortening the average.

Unlike the lots and the buildings, the cut is a **name lookup, not a spatial
join** — the survey publishes rates, not geometry — which makes this the one
borough-partitioned asset with no dependency on `reference_neighborhoods`.

The `Quartier` sheet is a cross-tab: five bedroom classes, each two columns
wide (the rate, then a letter grading its reliability). It is unpivoted to one
row per `dwelling_type` × `bedroom_type`, keyed to stable snake_case
(`apartment_other` × `2_bedroom`, `all` × `all`, …) rather than to the French
labels, because the same survey is published in an English workbook whose
headers read differently.

A rate is stored in **percent as published** — `0.2%` is `0.2`, not `0.002` —
and is null wherever there is none. The two reasons for that are kept apart in
`status`, because they mean different things and neither is an average-able
zero:

| `status` | Sheet | Meaning |
| --- | --- | --- |
| `published` | `1.6%` | A rate, with `reliability` in `a`..`d` |
| `suppressed` | `**` | Measured, withheld for confidentiality or reliability |
| `no_units` | `--` | No dwelling of that class exists in the quartier |

The borough figure is the **unweighted mean** of its quartiers' published
rates. Unweighted because this table publishes rates and nothing to weight them
by — the universe counts live in a different CMHC table — so `num_quartiers`
sits on every row next to `num_quartiers_mapped`, and the two rarely match:
suppression is heavy at this geography. For VSMPE in the 2023 survey, 31 of 45
cells are suppressed and the borough's overall rate is Parc-Extension's alone:

```python
import pandas as pd

rates = pd.read_parquet("data/vacancy_rates/2026-08-20/VSMPE/vacancy_rates.parquet")
rates.loc[
    (rates.dwelling_type == "all") & (rates.bedroom_type == "all"),
    ["vacancy_rate_pct", "num_quartiers", "num_quartiers_mapped", "averaged_quartiers"],
]
#    vacancy_rate_pct  num_quartiers  num_quartiers_mapped  averaged_quartiers
# 14              0.3              1                     3      Parc-Extension
```

Every `dwelling_type` × `bedroom_type` is written whether or not anything was
published for it, so the grid is the same 15 rows for every borough and a
suppressed cell is visible as a row rather than as an absence.

The survey year is **resource config, not a partition dimension** — the survey
is annual and this pipeline's date axis is the scrape date. `survey_year` and
`survey_period` (`octobre 2023`) are written as columns, and the field defaults
to `URBAN_RAG_CMHC_SURVEY_YEAR`, so pointing a run at another year takes:

```powershell
$env:URBAN_RAG_CMHC_SURVEY_YEAR = "2022"
uv run dagster asset materialize --select vacancy_rates --partition "2026-08-20|VSMPE" -m urban_rag.definitions
```

An env var rather than config alone because `--config-json` replaces a
resource's config *wholesale* — set `survey_year` that way and `cache_dir`,
which the code location is what knows, has to be restated with it.

2022 and 2023 are published under the French slug as of this writing; 2024 and
anything before 2022 answer 404. The workbook is cached under
`data/cache/cmhc/`, keyed by filename and shared across every scrape date —
the same posture as the PDF and BDOI caches, since a published survey year is
final.

## The average rents

`average_rents` reads CMHC's HMIP reading-mode page for
`Montreal - Average Rent by Bedroom Type by Neighbourhood`, then applies the
same `CMHC_QUARTIERS` crosswalk as `vacancy_rates`:

```powershell
uv run dagster asset materialize --select average_rents --partition "2026-08-20|VSMPE" -m urban_rag.definitions
```

It writes five borough rows, one per `bedroom_type`, plus the quartier cells
behind the mean:

```
<root>/average_rents/2026-08-20/VSMPE/
├── average_rents.parquet
└── quartier_average_rents.parquet
```

The rent is an unweighted mean of published quartier rents, in dollars as
published. Suppressed `**` cells stay null and drop out of the mean.

## Lots with CMHC vacancy rates

`lots_with_vacancy_rates` is the early, non-spatial join between
`neighborhood_lots` and `vacancy_rates` for the same date × neighborhood
partition:

```
<root>/lots_with_vacancy_rates/2026-08-20/VSMPE/
└── lots_with_vacancy_rates.parquet
```

The join key is the partition's `neighborhood` column. CMHC's 15-row
`dwelling_type` × `bedroom_type` grid is widened into `cmhc_*` columns before
the merge, so the output remains one row per lot. That keeps
`building_lot_intersections` working against true lot geometries while loading
the CMHC context into `rag.lots.attributes` ahead of the spatial join.

## The document corpus

The regulation tables carry no prose of their own. `LIEN_GRILLE` holds a
link, not a description:

```
Reglement_urbanisme__VSP_REG_ZONE.LIEN_GRILLE
  http://www1.ville.montreal.qc.ca/CartesInteractives/villeray/doc/zone/C01-001.pdf
```

So the retrievable text lives in the linked PDFs — one per zone, 633 rows
pointing at 632 distinct "grille des spécifications" documents for VSMPE —
and three assets turn them into an embedded corpus:

| Asset | Output |
| --- | --- |
| `linked_documents` | One row per distinct link: the PDF fetched and flattened to text |
| `document_chunks` | Paragraph-aligned, overlapping chunks, sized in the encoder's tokens |
| `document_embeddings` | One 1024-wide float32 [BAAI/bge-m3](https://huggingface.co/BAAI/bge-m3) vector per chunk |

```
data/
├── cache/pdf/<sha256(url)>.pdf          # outside the partitions on purpose
├── linked_documents/2026-08-18/VSMPE/documents.parquet
├── document_chunks/2026-08-18/VSMPE/chunks.parquet
└── document_embeddings/2026-08-18/VSMPE/embeddings.parquet
```

Each step owns its own prefix, so re-chunking replaces `document_chunks/` and
leaves the downloads and the vectors where they are.

A fourth asset, `document_index`, publishes the result into Postgres — it owns
no prefix, because it writes to a database rather than to the tree. See [The
shared vector store](#the-shared-vector-store).

A published zoning grid never changes once issued, so the download cache sits
outside the partition tree and every later scrape date reads it from disk
instead of from the city's web server.

There is no OCR step: a link that answers with a scan instead of a
born-digital PDF fails its own row with `no text layer`, leaving the rest of
the partition alone, exactly as a dead link does. Both are counted in
`num_failed` and listed by URL in the `failures` metadata, so adding an OCR
fallback later is a matter of reading that list.

Chunks are cut on paragraph boundaries and measured with bge-m3's own tokenizer
— 512 tokens with 64 of overlap by default — because a chunk budget is only
meaningful in the units the model truncates by. The overlap is carried *inside*
that budget, so no chunk is ever handed to the encoder over-length. Vectors are
L2-normalised, so cosine similarity is a plain dot product.

The vector column is written as an Arrow fixed-size list. Parquet has no
such type, so the width survives only in the `ARROW:schema` metadata:
`pq.read_table` hands back `fixed_size_list<float>[1024]`, while DuckDB
reads a plain `FLOAT[]` and needs `CAST(embedding AS FLOAT[1024])` before
vss will build an HNSW index over it.

bge-m3 is the default for the whole project, not only this corpus:
`rag.embeddings.DEFAULT_MODEL`, overridable per run with
`URBAN_RAG_EMBEDDING_MODEL`. Changing it changes the vector width, which means
anything already indexed has to be rebuilt.

Which column of which table is worth indexing is a registry, not a guess:
`DOCUMENT_SOURCES` in [rag/documents.py](src/urban_rag/rag/documents.py). Other
tables link to web pages, photos, or one shared modality page; add them there
when each earns an extractor.

## Retrieval

`embeddings.parquet` is the corpus; the query side is a DuckDB database with an
HNSW index over it, driven by a `urban-rag` CLI.

```powershell
uv run urban-rag index                      # load document_embeddings/**/embeddings.parquet
uv run urban-rag status                     # what is in the store
uv run urban-rag search "..." -k 5          # retrieval only, no generation
uv run urban-rag ask "..." -k 5             # retrieval, then a local LLM answers
```

Indexing is a **load, not a computation** — the assets already paid for the
fetching, chunking and embedding, so `index` reads the parquet straight into a
table, casts the vectors back to `FLOAT[1024]`, and builds the index over the
finished table. 518 chunks land in about four seconds. Re-running replaces the
contents rather than appending, mirroring the scrape's snapshot semantics; where
a resolution appears in several scrape dates, the newest copy wins.

| Command | Needs the 2 GB encoder? |
| --- | --- |
| `index`, `status` | No — pure DuckDB |
| `search`, `ask` | Yes, to embed the question |
| `ask` | …plus a local instruct model (default `Qwen/Qwen2.5-1.5B-Instruct`, `URBAN_RAG_LLM_MODEL`) |

`search` and `ask` default to the model the store was **built** with rather than
to `DEFAULT_MODEL`: only the vector width has to match for a query to succeed, so
querying a bge-m3 index with a different encoder returns confident nonsense
instead of an error. The width itself is checked, and a mismatch is reported as
`IndexMismatch` rather than a SQL error.

Generation is a local open-weights model through `transformers`, so answering
needs no API key and no egress once the weights are cached. Retrieved passages
are numbered and the model is asked to cite them; `ask` prints the source URLs
underneath regardless, so an answer can be checked against the resolutions it
came from rather than trusted.

`ask` is the expensive command, and on CPU it is expensive in **memory** before
it is expensive in time: it holds the encoder and the generator in one process,
so weights are loaded in bfloat16 rather than float32 — about 3 GB for the
1.5B default plus 2.2 GB for bge-m3. With less than that free, weight loading
does not fail, it *thrashes*, and a run that should take a minute stalls
indefinitely partway through a progress bar. Close what you can, drop
`--max-new-tokens` (it is the wall clock under greedy decoding, not a ceiling),
or point `URBAN_RAG_LLM_MODEL` at something smaller such as
`Qwen/Qwen2.5-0.5B-Instruct`. `search` needs the encoder only.

Two things that bite on this machine specifically:

- **`INSTALL vss` returns `HTTP 403`.** DuckDB's own extension downloader is
  refused by the inspecting proxy, though `curl` fetches the identical URL fine.
  [rag/vss.py](src/urban_rag/rag/vss.py) falls back to downloading the extension
  through `requests` (which honours the corporate bundle) and installing it from
  a local file, cached under `~/.cache/urban_rag/duckdb/`. Set
  `URBAN_RAG_VSS_EXTENSION` to a pre-downloaded `.duckdb_extension` on a machine
  with no egress at all.
- **`data/vect_db.duckdb` is registered in `.vscode/settings.json`**, so the
  editor's DuckDB panel holds the write lock and `index` fails with `is open in
  another process`. Disconnect it there, or reload the window. The lock is taken
  *before* any work rather than after, so this is reported in the first second.

## The shared vector store

The DuckDB file is *one process's* store: it takes an exclusive write lock, it
lives on whatever disk that process happens to have, and it is rebuilt in full
to change. That is the right trade on a laptop and the wrong one for a
deployment, where the pipeline runs in one place and a map, an API or a second
Dagster run reads the vectors from another — while a load is in flight.

The same corpus therefore also goes into **Postgres with the `pgvector`
extension**, on RDS. Same commands, one flag:

```bash
uv run urban-rag status --backend postgres
uv run urban-rag search "hauteur maximale en mètres" -k 5 --backend postgres
uv run urban-rag index  --backend postgres            # full reload from parquet
```

`URBAN_RAG_BACKEND=postgres` makes it the default; `make status BACKEND=postgres`
does the same through the Makefile. Both stores return the same `Hit`, so
[retriever.py](src/urban_rag/rag/retriever.py) and the chain neither know nor
care which one answered.

### Loading is an asset, not a command

`urban-rag index --backend postgres` is the *reload*: it drops the table and
replays every partition, which is what changing encoder needs and what leaves
the corpus unqueryable while it runs. The steady state is the `document_index`
asset, one partition at a time:

```bash
make publish DATE=2026-08-18 NEIGHBORHOOD=VSMPE
# or
uv run dagster asset materialize --select document_index --partition "2026-08-18|VSMPE" -m urban_rag.definitions
```

It is a load, not a computation — no encoder, no PDF, no model weights — so it
runs in the slim image and finishes in seconds. One partition is one
transaction: a reader sees it either as it was or as it now is.

It sits in its own job (`document_index_job`) rather than in `rag_corpus_job`,
because the corpus is built from the city's servers and this step publishes to a
database that has to be reachable — the second failing should not cost the
first.

### What the first load creates

```
rag.chunks                   one row per chunk, embedding vector(1024)
rag.chunks_meta              schema_version, embedding_model, dimension, source
rag.chunks_embedding_hnsw    HNSW, vector_cosine_ops, m=16, ef_construction=64
rag.chunks_partition         btree (neighborhood, scrape_date)
```

Two things it cannot create, both needing a role this pipeline should not have:
the **database**, and the **extension** — `CREATE EXTENSION vector` requires
`rds_superuser`. Those, the `urban_rag` role, and the `rag` and `dagster`
schemas it owns all come from **hbu_infra**, which is where the master
credentials live:

```bash
cd ../hbu_infra
make db-bootstrap ENV=dev    # urban_rag + grants, password → Secrets Manager
make db-init      ENV=dev    # extensions, rag/dagster schemas, and spatial tables
```

For any other Postgres — a local container, a scratch database — use the same
`hbu_infra` SQL rather than a dataplatform target. Set `DATABASE_URL` for the
superuser connection in that repo and run its `db-bootstrap`/`db-init` path.

`feature_ids` lands as `jsonb` rather than the JSON string the parquet carries:
in a shared database "which zones cite this document" is a query someone will
want. It reads back as text either way.

### Snapshot semantics, in a table people are reading

The DuckDB store gets its snapshot semantics for free — it is rebuilt from
scratch, so a document that stopped being cited stops being retrievable. A live
table cannot be emptied that way, so the same guarantee is two rules instead:

- **newest wins.** A chunk is keyed by `chunk_id`, which is derived from the
  document's URL, so the same resolution re-embedded on a later scrape date
  collides with itself. The upsert overwrites only when
  `EXCLUDED.scrape_date >= chunks.scrape_date`.
- **superseded rows are pruned.** After a partition lands, that borough's older
  scrape dates are deleted. What kept an old date is exactly what today's
  snapshot no longer links to. Set `prune_superseded=False` on the resource to
  keep the history instead.

`chunk_id` can also arrive twice inside *one* partition — two source tables
occasionally link the same PDF — and `ON CONFLICT` refuses to touch a row twice
in one statement, so the load de-duplicates before inserting.

### Connecting

Configuration is environment-first, so the code location, the CLI and anything
else that opens the store agree without an endpoint being committed:

| Variable | |
| --- | --- |
| `URBAN_RAG_PG_HOST` | RDS endpoint. Unset means libpq's own `PGHOST` |
| `URBAN_RAG_PG_PORT`, `_DATABASE`, `_USER` | default `5432`, `urban_rag`, `urban_rag` |
| `URBAN_RAG_PG_PASSWORD` | a literal password; prefer either of the next two |
| `URBAN_RAG_PG_SECRET_ID` | Secrets Manager secret holding `{"username", "password"}` |
| `URBAN_RAG_PG_IAM_AUTH` | `1` to sign each connection with an RDS IAM auth token |
| `URBAN_RAG_PG_SSLMODE` | default `verify-full` |
| `URBAN_RAG_PG_SSLROOTCERT` | the RDS CA bundle `verify-full` needs |
| `URBAN_RAG_PG_SCHEMA`, `_TABLE` | default `rag`, `chunks` |
| `URBAN_RAG_PG_DSN` | full libpq string, overriding all of the above |
| `URBAN_RAG_BACKEND` | `postgres` to make it the CLI default |

Credentials are resolved **per connection**, in that order, because an IAM auth
token is signed for fifteen minutes — a long-lived resource that cached one
would hand out an expired token on its second run. IAM authentication needs
`GRANT rds_iam` in the database *and* `rds-db:connect` in the task role's IAM
policy; it is the option with nothing long-lived to leak.

TLS defaults to `verify-full`, which is the only mode that authenticates the
server it is talking to, and it needs Amazon's CA on disk:

```bash
curl -o ~/.postgresql/root.crt --create-dirs \
     https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

That is checked *before* connecting, since libpq's own message for a missing
one names a file most people have never heard of. `URBAN_RAG_PG_SSLMODE=require`
encrypts without authenticating the server; `disable` is for a local
`pgvector/pgvector` container, which is also the easiest thing to point
`URBAN_RAG_PG_DSN` at.

An RDS instance is only reachable from inside its VPC, so a run from a laptop
needs the VPN or a tunnel; the connection failure says so rather than repeating
libpq's one-liner.

### Sizing

HNSW lives or dies by whether the index fits in memory: roughly
`rows × dimension × 4 bytes` plus the graph. bge-m3's 1024-wide vectors are ~4
KB apiece, so 100k chunks is ~400 MB before the graph — comfortable on a
`db.t4g.medium`, not on a `db.t4g.micro`. A rebuild raises
`maintenance_work_mem` for its own session; raise it on the parameter group if
the index build still spills to disk.

Search widens pgvector's candidate list (`hnsw.ef_search`, 40 by default) to
`max(100, 4k)`: the index returns its candidates and the `WHERE` clause is
applied to them *afterwards*, so filtering by neighborhood or scrape date is a
reason to ask for more of them, not fewer.

## Setup

```powershell
uv sync --python 3.12 --extra dev --extra rag
```

On Linux — WSL, the devcontainer, or CI — the same thing through the Makefile,
which clears `SSL_CERT_FILE` for the resolver on its own:

```bash
make sync
```

Two environment gotchas on a managed laptop:

- **`uv` fails with `invalid peer certificate: UnknownIssuer`.** The globally
  set `SSL_CERT_FILE` (Zscaler root) breaks uv's resolver. Clear it for the
  install and let uv use the OS store: `Remove-Item Env:SSL_CERT_FILE;
  $env:UV_SYSTEM_CERTS = "1"`.
- **The pipeline itself needs the opposite.** `requests` verifies against
  `certifi` and ignores `SSL_CERT_FILE`, so the client falls back to it (and to
  `REQUESTS_CA_BUNDLE` / `CURL_CA_BUNDLE`) explicitly. Keep `SSL_CERT_FILE`
  pointed at the corporate root at run time, or set `ca_bundle` on the
  `SpectrumResource`.
- **The model download needs the opposite again.** huggingface.co is *not*
  behind the inspecting proxy, so verifying it against the corporate root fails
  with `CERTIFICATE_VERIFY_FAILED`. `rag.embeddings.trusted_ca` swaps the three
  CA variables for `certifi` while the weights download and restores them after;
  override with `URBAN_RAG_HF_CA_BUNDLE` if your machine needs something else.

## Containers

The `Makefile` targets Linux — the devcontainer or WSL. It stops with a message
under Git Bash rather than silently handing Dagster an MSYS path, which is what
the `cygpath` translation it used to carry was for. The image itself carries no
`make`: inside it, `dagster` and `urban-rag` are already on `PATH`.

### Devcontainer

Open the folder in VS Code and *Reopen in Container*. It builds the `dev` stage
of the [Dockerfile](Dockerfile) and runs `uv sync --extra dev --extra rag`
against the bind-mounted workspace; port 2500 is forwarded, and `make dagster_run` binds
`0.0.0.0` so the UI is reachable from the host.

The venv lives at `/opt/venv`, not `.venv/`, so the workspace bind mount cannot
shadow it and the Windows virtualenv in the same folder is neither used nor
overwritten. uv's download cache and the Hugging Face cache are named volumes,
so a rebuild does not re-download bge-m3; the venv is not, so a rebuilt image
is never shadowed by a stale copy of itself.

The retrieval stack is in the devcontainer image, which is why it is several
gigabytes: the Dagster code location does not load without it — see [The rag
extra is not optional](#the-rag-extra-is-not-optional).

### WSL

`make sync && make dagster_run` is all it takes, with one caveat: keep the
checkout on the WSL filesystem (`~/urban_rag`), not under `/mnt/c`. Dagster's
default run and event storage is SQLite, and SQLite's locking over the 9p mount
that backs `/mnt/c` is where this goes wrong first — as does the DuckDB write
lock the vector store takes.

### Image

```bash
make docker-build      # the deployable image
make docker-run        # UI on http://localhost:2500
make docker-shell      # poke around; dagster and urban-rag are on PATH
```

Two paths are the contract with the host — both bind-mountable, both
overridable:

| Path | Holds | Env |
| --- | --- | --- |
| `/data` | PDF cache, `vect_db.duckdb`, and parquet snapshots when `S3_BUCKET` is unset | `URBAN_RAG_DATA_DIR` |
| `/dagster_home` | run and event storage, schedule state | `DAGSTER_HOME` |

### S3 output

Set `S3_BUCKET` (in `.env` or the environment) to write every (geo)parquet
output — the catalog, the Spectrum scrape, the open-data snapshot and the RAG
corpus — under `s3://$S3_BUCKET/` instead of `/data`. The prefixes are the same
[output layout](#output-layout) the local tree uses, one per asset:

```
s3://$S3_BUCKET/spectrum_table_catalog/2026-08-20/tables.parquet
s3://$S3_BUCKET/neighborhood_features/2026-08-20/VSMPE/*.parquet
s3://$S3_BUCKET/reference_neighborhoods/2026-08-20/*.parquet
s3://$S3_BUCKET/neighborhood_lots/2026-08-20/VSMPE/lots.parquet
s3://$S3_BUCKET/vacancy_rates/2026-08-20/VSMPE/vacancy_rates.parquet
s3://$S3_BUCKET/average_rents/2026-08-20/VSMPE/average_rents.parquet
s3://$S3_BUCKET/lots_with_vacancy_rates/2026-08-20/VSMPE/lots_with_vacancy_rates.parquet
s3://$S3_BUCKET/linked_documents/2026-08-20/VSMPE/documents.parquet
s3://$S3_BUCKET/document_chunks/2026-08-20/VSMPE/chunks.parquet
s3://$S3_BUCKET/document_embeddings/2026-08-20/VSMPE/embeddings.parquet
```

Credentials come from the named profile in `AWS_PROFILE` (default
`charles_gauvin_east_1`), resolved from `~/.aws/credentials`; in
`docker-compose.yml` that file is bind-mounted read-only into the container.
The PDF cache, the Dagster IO manager, and `vect_db.duckdb` always stay on
local disk - only asset output moves.

The image is several gigabytes, and on Linux most of that is the CUDA runtime
the locked torch pulls in — which the Windows resolution never did. If the
deployment target is CPU-only, re-lock torch against
`download.pytorch.org/whl/cpu` through a `[tool.uv.sources]` entry: the lock,
not the Dockerfile, is what decides this.

DuckDB's `vss` extension is baked in at build time through
[rag/vss.py](src/urban_rag/rag/vss.py)'s own downloader, so a running container
needs no egress to `extensions.duckdb.org`. The step is non-fatal — the build
says so and carries on, and `load_vss` retries on first use.

It does fail on the corporate network, and not the way the module's docstring
expects: `extensions.duckdb.org` answers `200` and then truncates the body at
~690 KB of ~11 MB, for `curl` and `requests` alike, so there is no client-side
route around it. Every test in `tests/unit/test_store.py` needs the extension,
so `make docker-test` reports 19 failures there and 39 passes elsewhere until
it is supplied from outside:

```bash
# on a machine with clean egress
curl -O http://extensions.duckdb.org/v<duckdb-version>/linux_amd64/vss.duckdb_extension.gz
gunzip vss.duckdb_extension.gz
```

then mount it and point `URBAN_RAG_VSS_EXTENSION` at it — the escape hatch
`vss.py` already provides for exactly this.

#### The rag extra costs several gigabytes for five assets that never load it

`pyproject.toml` says the retrieval stack "stays out of the base install that
the Dagster scrape only needs", and it does: `resources.py` imports
`rag.embeddings` at module scope, but that module only reaches for
sentence-transformers/torch lazily, inside methods, so the code location loads
fine with `uv sync --extra dev` alone.

```bash
make docker-build-slim
```

builds a ~510 MB image whose code location loads and lists all five assets -
`ask`/`search`/`index` just are not available without the `rag` extra to run
the encoder they need.

### Compose

`dagster dev` is a single process running both the webserver and the daemon,
which is fine on a laptop and wrong for a deployment: with no daemon, the two
daily schedules never fire. [docker-compose.yml](docker-compose.yml) splits
them, which is also the shape that maps onto two ECS services:

```bash
make up      # build and start webserver + daemon
make logs
make down
```

That stack uses the default SQLite and filesystem storage under `/dagster_home`
until Postgres connection settings are present. With `URBAN_RAG_PG_HOST` plus a
password or secret (or an explicit `DAGSTER_POSTGRES_URL`), the image
entrypoint writes a `dagster.yaml` that stores Dagster run/event/schedule
metadata in the same database under the `dagster` schema. Run
`hbu_infra`'s `make db-bootstrap`/`make db-init` first so that schema exists.

## Running

Every command below has a Makefile target on Linux (`make dagster_run`,
`make materialize`, `make corpus`, `make index`); `make help` lists them with
the variables they read. Raw:

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"

# UI
uv run dagster dev

# or headless, one partition at a time
uv run dagster asset materialize --select spectrum_table_catalog --partition 2026-08-18 -m urban_rag.definitions
uv run dagster asset materialize --select neighborhood_features --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the open-data neighborhoods, date-partitioned only
uv run dagster asset materialize --select reference_neighborhoods --partition 2026-08-18 -m urban_rag.definitions

# the cadastral lots for that borough, which read the snapshot above
uv run dagster asset materialize --select neighborhood_lots --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the CMHC vacancy rates for that borough, which depend on nothing upstream
uv run dagster asset materialize --select vacancy_rates --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the CMHC average rents for that borough, also independent
uv run dagster asset materialize --select average_rents --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# lots enriched with CMHC rates by neighborhood name, before spatial joins
uv run dagster asset materialize --select lots_with_vacancy_rates --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# then the corpus over that snapshot's linked PDFs
uv run dagster asset materialize --select "linked_documents,document_chunks,document_embeddings" --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# and publish those vectors to the Postgres/pgvector store
uv run dagster asset materialize --select document_index --partition "2026-08-18|VSMPE" -m urban_rag.definitions
```

Schedules run daily in `America/Toronto`: the catalog at 04:00, the features
at 04:20, the reference neighborhoods at 04:40, the vacancy rates at 04:45 and
the average rents at 04:50 (the CMHC assets are independent of the Spectrum
assets — the minutes only keep them from overlapping), then the lots at 05:40,
the buildings at 05:50, the lot × CMHC name join at 06:10 and the building ×
lot join at 07:00 behind them. All target *today's*
partition — `end_offset=1` on the daily partitions exists for that reason,
since "scrape date" means the day the fetch happened, not a closed event
window.

## Adding neighborhoods

Add keys to `ENABLED_NEIGHBORHOODS` in [partitions.py](src/urban_rag/partitions.py);
all 17 borough namespaces are already mapped there. Existing partitions are
untouched, and the new ones backfill from the UI.

## Talking to the service

The proxy in front of the Feature Service has two quirks that dictate how every
request is built — a mandatory `url` parameter, and the fact that it is the
*only* parameter forwarded. Both are documented at the top of
[spectrum.py](src/urban_rag/spectrum.py), with the behaviors verified against
the live service:

- `pageLength` is honoured only when `page` is also present.
- `count.json` returns HTTP 500; `select count(*)` works.
- `MI_Buffer` / `MI_Circle` / `MI_Distance` do not exist. Bounding-box filters
  do: `where Obj within MI_Box(minx,miny,maxx,maxy,'epsg:4326')`.

Default request delay is 0.25s. This is Montreal's live Analyst server, not an
open-data mirror — for bulk needs, check
[donnees.montreal.ca](https://donnees.montreal.ca) first.

## Tests

```powershell
uv run pytest
```

Offline only: the client is exercised with a stub session that asserts the URL
shapes the proxy demands.
