# urban_rag

Dagster pipeline that snapshots Montreal's [Spectrum Spatial Feature
Service](https://spectrum.montreal.ca/connect/analyst/controller/connectProxy/rest/Spatial/FeatureService)
into local (geo)parquet, partitioned by neighborhood and scrape date, then joins
it to the provincial cadastre, the assessment roll, CMHC's rental surveys and the
zoning grids — so that what may be built on a lot, what that would cost, and what
the ground is already worth read off one row.

Registered by default: **VSMPE** (Villeray–Saint-Michel–Parc-Extension, 24 Spectrum tables) and **CIL** (La Cité-Limoilou, Quebec City - its zoning layer and specification grid; see [docs/quebec-city.md](docs/quebec-city.md)). **SAG** (Ville de Saguenay, one key for the whole municipality - its zoning layer and a grid PDF per zone; see [docs/saguenay.md](docs/saguenay.md)) is known and is registered on request. The list lives in the Dagster instance: `make neighborhoods` shows it, `make neighborhood-add NEIGHBORHOOD=<key>` widens it.

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
make materialize DATE=2026-09-01 NEIGHBORHOOD=VSMPE  # a full scrape of one partition
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

41 assets, listed with their partitions and outputs in
[docs/assets.md](docs/assets.md).

| Layer | |
| --- | --- |
| **bronze** | `spectrum_table_catalog` `neighborhood_features` `reference_neighborhoods` `neighborhood_lots` `neighborhood_buildings` `cmhc_vacancy_survey` `cmhc_rent_survey` `street_network` `neighborhood_addresses` `linked_documents` `montreal_residential_costs` `montreal_nonresidential_costs` `property_assessment_roll` `cubf_use_codes` `uniformized_property_wealth` `montreal_commercial_rents` `commercial_rent_index` |
| **silver** | `assessment_units` `lot_assessed_values` `lot_assessment_comparables` `commercial_rents` `vacancy_rates` `average_rents` `building_lot_intersections` `neighborhood_streets` `lot_addresses` `lot_frontage` `document_chunks` `document_embeddings` `zoning_grid_columns` `lot_zone_pieces` `lot_zoning_envelopes` `lot_buildable_setbacks` `lot_development_programs` |
| **gold** | `lot_profiles` `lot_highest_best_use` `lot_redevelopment_gap` `lot_building_massing` `lot_investment_opportunities` `map_cell_aggregates` `document_index` |

They read eight publishers: Spectrum and the open-data portal (Ville de
Montréal), Infolot, the assessment roll and Adresses Québec (Québec), BDOI,
CMHC, and the Altus cost guide. Why each is read the way it is — and what each
one gets wrong — is a page per source under [docs/](docs/README.md#the-data).

`lot_development_programs` is where the highest-and-best-use question
actually gets solved — one `urban_rag.program.solve_program` CP-SAT run per
candidate envelope, maximising discounted net profit (`npv_cad`) over the mix
of dwellings, commerce, industry and parking. The model itself — its caps, the
three places a stall can go, and the `binding` vocabulary that says why an
answer is not bigger — is
[docs/development-program.md](docs/development-program.md).
`lot_highest_best_use` picks the governing envelope's program for each lot,
and `lot_redevelopment_gap` puts it
beside what `lot_assessment_comparables` says already stands there — the floor
area gap by residential/commercial/industrial class, in m² and sqft, and the
two incomes reconciled onto one stated definition of NOI. `make programs` and
`make hbu` run the three by hand.

`lot_redevelopment_gap` now prices **three futures** per lot on one footing:
keep the building, keep it and grow it (a second CP-SAT solve with the
standing building retained), or clear the lot and build the programme, whose
income starts only after the build and the lease-up. `docs/site-theses.md`.

`lot_investment_opportunities` turns the gap into two shortlists: an
**investment thesis** — what you would build — ranked on yield on cost, and a
**site thesis** — why the parcel is acquirable: `brownfield`, `teardown`,
`infill` or `improvement`, each costing its own demolition, remediation or
addition, with a heritage sector read off the grid's own *Patrimoine* row
keeping a lot out of the two that demolish. `make opportunities`, and
[docs/opportunities.md](docs/opportunities.md) and
[docs/site-theses.md](docs/site-theses.md).

`lot_building_massing` then draws the answer: one rectangle per lot, fitted
inside that lot's setback envelope so the margins are respected by
construction, in EPSG:4326 and ready to put on a map beside the cadastre.
Its `footprint_fit_pct` is a check on the solver rather than a decoration —
`solve_program` caps a footprint on the lesser of two *areas* and never asks
whether a building of that area has a shape the parcel can take.
It draws the parking as a *second* polygon, into
`gold.lot_surface_parking`: a surface stall is not a building, so it is
fitted into the *piece* that zone governs rather than into the setback
envelope, with 5.5 m clear in every direction. That shape check also runs
upstream, where it can act — a parcel too narrow to stand a car on cannot
park on the ground at all, and its programme has to dig or bay the stalls
instead.
`make massing`, and [docs/massing.md](docs/massing.md).

`lot_addresses` gives every one of those answers a name a person recognises.
Adresses Québec publishes the province's official civic addresses as points and
records **no lot number on any of them**, so putting an address on a parcel is a
spatial join or it does not happen; `silver.lot_addresses` does it at the
`(lot_uid, feature_id)` grain the gold tables are keyed on, which is what lets
the map label a site *7430 Rue Lajeunesse* rather than *2 784 705* and lets the
corpus answer a question asked as a street and a number. A row is an addressable
*unit* rather than a front door — roughly half of them carry an apartment prefix
— so the units on a site and the doors on it are counted separately, and
`is_primary_address` marks the one a label takes. The product is advertised
under a WMS endpoint, which renders pictures and cannot answer this; the REST
face of the same server can. `make addresses`, and
[docs/addresses.md](docs/addresses.md).

### Eleven assets are blocked

`lot_frontage`, `lot_zone_pieces`, `lot_addresses`, `lot_buildable_setbacks`,
`lot_profiles`, `lot_development_programs`, `lot_highest_best_use`,
`lot_redevelopment_gap`, `lot_building_massing`,
`lot_investment_opportunities` and `map_cell_aggregates` are registered and
have jobs,
but **no schedule**: each reads a relation hbu_infra creates. The SQL files all exist; what is
outstanding is `db.py init` against the target database — twice for
`lot_profiles`, since `sql/006_lot_documents.sql` carries a
`-- requires: rag.chunks` header and only lands after `document_index`
has run.

Each fails up front naming the file to apply, rather than letting psycopg raise.
Run them by hand with `make frontage`, `make zone-pieces`, `make addresses`,
`make setbacks`, `make lot-profiles`, `make programs`, `make hbu`, `make
massing`, `make opportunities` and `make map_cells`; the envelope pair they all
sit behind has no schedule either (`make envelopes`), and neither does
`neighborhood_addresses`, the bronze half `make addresses` materializes with
its silver join.
`zone-pieces` is the first of the zoning chain rather than an addition to it —
the envelopes join the ground each zone governs rather than the raw overlaps,
so nothing below it is correct until it has run for the partition. Details, and
the order they want scheduling in, are in [docs/assets.md](docs/assets.md).

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
