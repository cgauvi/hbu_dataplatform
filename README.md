# urban_rag

Dagster pipeline that snapshots Montreal's [Spectrum Spatial Feature
Service](https://spectrum.montreal.ca/connect/analyst/controller/connectProxy/rest/Spatial/FeatureService)
into local (geo)parquet, partitioned by neighborhood and scrape date, then joins
it to the provincial cadastre, the assessment roll, CMHC's rental surveys and the
zoning grids — so that what may be built on a lot, what that would cost, and what
the ground is already worth read off one row.

Currently enabled: **VSMPE** (Villeray–Saint-Michel–Parc-Extension, 24 tables).

Full documentation is in **[docs/](docs/README.md)**.

## Quick start

```powershell
uv sync --python 3.12 --extra dev --extra rag
$env:DAGSTER_HOME = "$PWD\.dagster_home"
uv run dagster dev
```

On Linux — WSL, the devcontainer, or CI — the Makefile does the same and carries
the flags each target needs:

```bash
make sync
make dagster_run                                     # UI on :2500
make materialize DATE=2026-08-18 NEIGHBORHOOD=VSMPE  # a full scrape of one partition
make help                                            # every target, with the variables it reads
```

A managed laptop needs two opposite certificate settings — `uv` wants the OS
store, the pipeline wants the corporate root, and the model download wants both
at once. That, the devcontainer, WSL, the images, S3 output and compose are all
in [docs/setup.md](docs/setup.md). Materializing single partitions by hand, the
schedules, and adding a borough are in [docs/running.md](docs/running.md).

## The shape of it

A medallion in three layers, declared once in
[layers.py](src/urban_rag/layers.py) so the asset key and the prefix it writes
under cannot drift apart:

| Layer | What it promises a reader |
| --- | --- |
| **bronze** | What the publisher returned, plus the scrape columns. Invalid rings and odd spellings survive |
| **silver** | EPSG:4326, geometry valid, crosswalks applied, one row per declared grain |
| **gold** | Named for the question, at the grain whoever asks it reads |

Output lands under `<root>/<layer>/<asset>/<YYYY-MM-DD>[/<neighborhood>]/`,
`<root>` being `data/` or `s3://$S3_BUCKET/`. Each partition is a full snapshot,
and the tree — not the database — is the record: losing Postgres costs a reload
rather than a re-scrape, which for a live municipal source no later run can undo.

Postgres is a **serving copy** of silver and gold, and the schema a table sits in
is the layer its asset is in — `silver/vacancy_rates` in the tree is
`silver.vacancy_rates` in the database. Every such table is partitioned by
`(neighborhood, scrape_date)` and written by one upsert-then-prune.

The layer contracts in full, the single writer behind every table, and the output
tree: [docs/architecture.md](docs/architecture.md).

## The assets

27 assets, listed with their partitions and outputs in
[docs/assets.md](docs/assets.md).

| Layer | |
| --- | --- |
| **bronze** | `spectrum_table_catalog` `neighborhood_features` `reference_neighborhoods` `neighborhood_lots` `neighborhood_buildings` `cmhc_vacancy_survey` `cmhc_rent_survey` `street_network` `montreal_residential_costs` `montreal_nonresidential_costs` `property_assessment_roll` `linked_documents` |
| **silver** | `vacancy_rates` `average_rents` `building_lot_intersections` `assessment_units` `lot_assessed_values` `lot_assessment_comparables` `neighborhood_streets` `lot_frontage` `zoning_grid_columns` `lot_zoning_envelopes` `lot_buildable_setbacks` `document_chunks` `document_embeddings` |
| **gold** | `lot_profiles` `document_index` |

They read seven publishers: Spectrum and the open-data portal (Ville de
Montréal), Infolot and the assessment roll (Québec), BDOI, CMHC, and the Altus
cost guide. Why each is read the way it is — and what each one gets wrong — is a
page per source under [docs/](docs/README.md#the-data).

### Three assets are blocked

`lot_frontage`, `lot_buildable_setbacks` and `lot_profiles` are registered and
have jobs, but **no schedule**: each reads a relation hbu_infra creates. The SQL
files all exist; what is outstanding is `db.py init` against the target database
— twice, since `sql/006_lot_documents.sql` carries a `-- requires: rag.chunks`
header and only lands after `document_index` has run.

Each fails up front naming the file to apply, rather than letting psycopg raise.
Run them by hand with `make frontage`, `make setbacks` and `make lot-profiles`;
the envelope pair they sit behind has no schedule either (`make envelopes`).
Details, and the order the three want scheduling in, are in
[docs/assets.md](docs/assets.md).

## Retrieval

The zoning PDFs are fetched, chunked and embedded by three assets, then queried
through a `urban-rag` CLI over either DuckDB or Postgres/pgvector:

```powershell
uv run urban-rag index                      # load document_embeddings/**/embeddings.parquet
uv run urban-rag status                     # what is in the store
uv run urban-rag search "..." -k 5          # retrieval only, no generation
uv run urban-rag ask "..." -k 5             # retrieval, then a local LLM answers
```

`--backend postgres` (or `URBAN_RAG_BACKEND=postgres`) points the same commands
at the shared store. How the corpus is built, what the two backends each
guarantee, and how to connect to RDS: [docs/corpus.md](docs/corpus.md).

## Tests

```powershell
uv run pytest
```

Offline by default — the Feature Service is stubbed and every PostGIS statement
is stubbed at the function that issues it. The spatial assets also have
integration tests that run the real SQL against a real PostGIS on a committed
slice of VSMPE; they skip unless `URBAN_RAG_TEST_PG_URL` is set. See
[docs/running.md](docs/running.md#tests).
