# urban_rag

Dagster pipeline that snapshots Montreal's [Spectrum Spatial Feature
Service](https://spectrum.montreal.ca/connect/analyst/controller/connectProxy/rest/Spatial/FeatureService)
into local (geo)parquet, partitioned by neighborhood and scrape date.

Currently enabled: **VSMPE** (Villeray–Saint-Michel–Parc-Extension, 24 tables).

## Assets

| Asset | Partitions | Output |
| --- | --- | --- |
| `spectrum_table_catalog` | date | The service's full table list for that day (577 tables across 19 namespaces), held by the IO manager |
| `neighborhood_features` | date × neighborhood | One parquet file per source table, written directly to `data/spectrum/` |
| `linked_documents` | date × neighborhood | The PDFs those tables link to, fetched and flattened to text |
| `document_chunks` | date × neighborhood | Those documents cut into retrieval-sized chunks |
| `document_embeddings` | date × neighborhood | A bge-m3 vector per chunk |

The last three build the retrieval corpus and are described under [The document
corpus](#the-document-corpus).

The catalog is a separate asset because the published table list drifts —
boroughs add and retire layers without notice, and a scrape is only
reproducible if you know what existed on that date. `neighborhood_features`
reads it through a `MultiToSingleDimensionPartitionMapping`, so the
`2026-08-18|VSMPE` partition consumes exactly the `2026-08-18` catalog.

## Output layout

```
data/spectrum/
└── neighborhood=VSMPE/
    └── scrape_date=2026-08-18/
        ├── Apaisement__VSP_TRA_AFFICHEUR.parquet
        ├── Reglement_urbanisme__VSP_REG_ZONE.parquet
        └── ...
```

Hive-style directories, so the whole history reads back as one dataset:

```python
import geopandas as gpd

zones = gpd.read_parquet("data/spectrum/neighborhood=VSMPE/scrape_date=2026-08-18/Reglement_urbanisme__VSP_REG_ZONE.parquet")
zones.crs        # EPSG:4326
zones.columns    # source table attributes + source_table + scraped_at
```

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

## The document corpus

The regulation tables carry no prose of their own. `EN_SAVOIR_PLUS` holds a
link, not a description:

```
Reglement_urbanisme__VSP_REG_PPCMOI.EN_SAVOIR_PLUS
  http://www1.ville.montreal.qc.ca/CartesInteractives/villeray/doc/pp_pv/PP_CA11140080.pdf
```

So the retrievable text lives in the linked PDFs — 229 rows pointing at 227
distinct resolutions — and three assets turn them into an embedded corpus:

| Asset | Output |
| --- | --- |
| `linked_documents` | One row per distinct link: the PDF fetched and flattened to text (200 of 227 for VSMPE) |
| `document_chunks` | Paragraph-aligned, overlapping chunks, sized in the encoder's tokens (518 for VSMPE) |
| `document_embeddings` | One 1024-wide float32 [BAAI/bge-m3](https://huggingface.co/BAAI/bge-m3) vector per chunk |

```
data/
├── cache/pdf/<sha256(url)>.pdf          # outside the partitions on purpose
└── rag/neighborhood=VSMPE/scrape_date=2026-08-18/
    ├── documents.parquet
    ├── chunks.parquet
    └── embeddings.parquet
```

A published resolution never changes, so the download cache sits outside the
partition tree and every later scrape date reads it from disk instead of from
the city's web server.

There is no OCR step. 200 of the 227 links are born-digital and carry a text
layer; the other 27 are scans — all of them older `doc/pe/` exemption files —
and each fails its own row with `no text layer`, leaving the rest of the
partition alone, exactly as a dead link does. They are counted in
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
uv run urban-rag index                      # load data/rag/**/embeddings.parquet
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
| `/data` | parquet snapshots, PDF cache, `vect_db.duckdb` | `URBAN_RAG_DATA_DIR` |
| `/dagster_home` | run and event storage, schedule state | `DAGSTER_HOME` |

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

#### The rag extra is not optional

It should be. `pyproject.toml` says the retrieval stack "stays out of the base
install that the Dagster scrape only needs", and everything expensive in it is
already imported lazily — but `resources.py` imports `rag.embeddings`, which
subclasses `langchain_core.embeddings.Embeddings` at module scope. So
`uv sync --extra dev` alone produces a code location that fails to import, and
the image has to carry torch to load five assets that never call it.

One pure-Python package is the whole difference. Move `langchain-core` from the
`rag` extra into the base dependencies, re-lock, and

```bash
make docker-build-slim
```

builds a ~510 MB image whose code location loads. Verified by installing
`langchain-core` into that image by hand: `dagster asset list` goes from
`ModuleNotFoundError` to all five assets.

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

That stack still uses the default SQLite and filesystem storage under
`/dagster_home`. Before AWS, point `DAGSTER_HOME` at a `dagster.yaml` with
Postgres run and event storage: two containers sharing one SQLite file is the
first thing to break once the schedules are actually running.

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

# then the corpus over that snapshot's linked PDFs
uv run dagster asset materialize --select "linked_documents,document_chunks,document_embeddings" --partition "2026-08-18|VSMPE" -m urban_rag.definitions
```

Two schedules run daily in `America/Toronto`: the catalog at 04:00, the
features at 04:20. Both target *today's* partition — `end_offset=1` on the
daily partitions exists for that reason, since "scrape date" means the day the
fetch happened, not a closed event window.

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
