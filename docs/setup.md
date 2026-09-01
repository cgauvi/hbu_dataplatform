# Setup and containers

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
- **The model download needs both halves at once.** huggingface.co itself is
  *not* behind the inspecting proxy, so verifying it against the corporate root
  fails with `CERTIFICATE_VERIFY_FAILED` — but the file CDN it redirects the
  weights to, `us.aws.cdn.hf.co`, *is* intercepted, and verifying that against
  `certifi` fails the same way. `rag.embeddings.trusted_ca` swaps the three CA
  variables for `certifi` while the weights download and restores them after,
  which resolves the metadata and then dies at the transfer's TLS handshake.
  Point `URBAN_RAG_HF_CA_BUNDLE` at the concatenated bundle — a superset of
  `certifi`, so it satisfies both hosts.

  The failure is worth recognising because the message names the wrong problem:
  `huggingface_hub` 1.x is `httpx`-based, `httpx` errors are not `OSError`
  subclasses, and `transformers` wraps anything that is not an `OSError` in
  `Can't load the model for 'BAAI/bge-m3' … a file named pytorch_model.bin`.
  The real `httpx.ConnectError` is the `__cause__`, further down the chain.

## Containers

The `Makefile` targets Linux — the devcontainer or WSL. It stops with a message
under Git Bash rather than silently handing Dagster an MSYS path, which is what
the `cygpath` translation it used to carry was for. The image itself carries no
`make`: inside it, `dagster` and `urban-rag` are already on `PATH`.

### Devcontainer

Open the folder in VS Code and *Reopen in Container*. It builds the `dev` stage
of the [Dockerfile](../Dockerfile) and runs `uv sync --extra dev --extra rag`
against the bind-mounted workspace; port 2500 is forwarded, and `make dagster_run` binds
`0.0.0.0` so the UI is reachable from the host.

The venv lives at `/opt/venv`, not `.venv/`, so the workspace bind mount cannot
shadow it and the Windows virtualenv in the same folder is neither used nor
overwritten. uv's download cache and the Hugging Face cache are named volumes,
so a rebuild does not re-download bge-m3; the venv is not, so a rebuilt image
is never shadowed by a stale copy of itself.

The retrieval stack is in the devcontainer image, which is why it is several
gigabytes: the Dagster code location does not load without it — see [The rag
extra is not optional](#the-rag-extra-costs-several-gigabytes-for-five-assets-that-never-load-it).

### WSL

`make sync && make dagster_run` is all it takes, with one caveat: keep the
checkout on the WSL filesystem (`~/urban_rag`), not under `/mnt/c`. Dagster's
default run and event storage is SQLite, and SQLite's locking over the 9p mount
that backs `/mnt/c` is where this goes wrong first — as does the DuckDB write
lock the vector store takes. Pointing Dagster at Postgres
([Dagster's own storage](#dagsters-own-storage)) removes the first of those two
but not the second.

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
| `/dagster_home` | `dagster.yaml`, plus run and event storage when it is not on Postgres | `DAGSTER_HOME` |

### S3 output

Set `S3_BUCKET` (in `.env` or the environment) to write every (geo)parquet
output — the catalog, the Spectrum scrape, the open-data snapshot and the RAG
corpus — under `s3://$S3_BUCKET/` instead of `/data`. The prefixes are the same
[output layout](architecture.md#output-layout) the local tree uses, one per asset:

```
s3://$S3_BUCKET/bronze/spectrum_table_catalog/2026-08-20/tables.parquet
s3://$S3_BUCKET/bronze/neighborhood_features/2026-08-20/VSMPE/*.parquet
s3://$S3_BUCKET/bronze/reference_neighborhoods/2026-08-20/*.parquet
s3://$S3_BUCKET/bronze/neighborhood_lots/2026-08-20/VSMPE/lots.parquet
s3://$S3_BUCKET/bronze/neighborhood_buildings/2026-08-20/VSMPE/buildings.parquet
s3://$S3_BUCKET/bronze/cmhc_vacancy_survey/2026-08-20/quartier_vacancy_rates.parquet
s3://$S3_BUCKET/bronze/montreal_residential_costs/2026-08-20/residential_costs.parquet
s3://$S3_BUCKET/bronze/linked_documents/2026-08-20/VSMPE/documents.parquet
s3://$S3_BUCKET/silver/vacancy_rates/2026-08-20/VSMPE/vacancy_rates.parquet
s3://$S3_BUCKET/silver/average_rents/2026-08-20/VSMPE/average_rents.parquet
s3://$S3_BUCKET/silver/building_lot_intersections/2026-08-20/VSMPE/building_lots.parquet
s3://$S3_BUCKET/silver/building_lot_intersections/2026-08-20/VSMPE/lot_features.parquet
s3://$S3_BUCKET/silver/lot_zoning_envelopes/2026-08-20/VSMPE/lot_zoning_envelopes.parquet
s3://$S3_BUCKET/silver/document_chunks/2026-08-20/VSMPE/chunks.parquet
s3://$S3_BUCKET/silver/document_embeddings/2026-08-20/VSMPE/embeddings.parquet
s3://$S3_BUCKET/gold/lot_profiles/2026-08-20/VSMPE/lot_profiles.parquet
```

A bucket policy or lifecycle rule can now be written per layer: bronze is the
part that cannot be re-fetched and wants versioning, silver and gold are
derived and can be rebuilt from it.

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
[rag/vss.py](../src/urban_rag/rag/vss.py)'s own downloader, so a running container
needs no egress to `extensions.duckdb.org`. The step is non-fatal — the build
says so and carries on, and `load_vss` retries on first use.

That download does fail on the corporate network, and not the way the module's
first docstring expected: `extensions.duckdb.org` answers `200` and then
truncates the body at a few hundred KB of ~11 MB — for DuckDB's own downloader,
`curl` and `requests` alike — while answering `403` to the Range request that
would let a client resume. There is no client-side route around that.

**Resolved by not downloading it.** `duckdb-extension-vss` ships the same
binary DuckDB publishes, as a wheel, and PyPI is not inspected the way the
extension repository is. It is in the `dev` extra with its `duckdb-extensions`
helper, so `uv sync --extra dev` is all `tests/unit/test_store.py` needs — those
19 tests failed on any machine that could not reach the extension repository
and now pass without egress.

`load_vss` tries the wheel first and still falls back to `INSTALL vss` and to
its own download, so a base install without the dev extra is unchanged. Note
that both packages come from
[santosh-d3vpl3x/duckdb_extensions](https://github.com/santosh-d3vpl3x/duckdb_extensions),
a third party repackaging DuckDB's published binaries — not DuckDB Labs — and
that `duckdb-extension-vss` pins `duckdb==` its own version, so the two move in
lockstep.

On a machine with no egress at all and no wheel, the older escape hatch is
still there:

```bash
# on a machine with clean egress
curl -O http://extensions.duckdb.org/v<duckdb-version>/linux_amd64/vss.duckdb_extension.gz
gunzip vss.duckdb_extension.gz
```

then mount it and point `URBAN_RAG_VSS_EXTENSION` at it.

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
daily schedules never fire. [docker-compose.yml](../docker-compose.yml) splits
them, which is also the shape that maps onto two ECS services:

```bash
make up      # build and start webserver + daemon
make logs
make down
```

That stack uses the default SQLite and filesystem storage under `/dagster_home`
until Postgres connection settings are present — see
[Dagster's own storage](#dagsters-own-storage) below, which is the same
mechanism.

### Dagster's own storage

Two different databases are in play and they are easy to confuse. The
Postgres/pgvector one holds the *corpus* — `rag.chunks` and the spatial tables,
which `make publish` and `make index BACKEND=postgres` write. This section is
about the other one: Dagster's own bookkeeping — run history, the event log,
schedule and sensor state, the tick records the daemon reads.

By default that bookkeeping is SQLite plus loose files under `DAGSTER_HOME`,
which is fine for one laptop and wrong for anything with two processes: the
webserver and the daemon in [docker-compose.yml](../docker-compose.yml) are peers
contending for the same SQLite writer lock, and a Fargate task's local disk
does not survive a redeploy at all.

Set `URBAN_RAG_PG_HOST` plus a password or secret — or an explicit
`DAGSTER_POSTGRES_URL` — and that bookkeeping moves to Postgres instead, into a
`dagster` schema kept separate from `rag`:

```bash
cd ../hbu_infra
make db-bootstrap ENV=dev     # creates the urban_rag role and both schemas
make db-init      ENV=dev
eval "$(./scripts/db.py env --app)"   # or plain `env` for the master user

cd ../hbu_dataplatform
make dagster_run
```

[src/urban_rag/dagster_home.py](../src/urban_rag/dagster_home.py) is what reads
those variables: it writes `$DAGSTER_HOME/dagster.yaml` and then execs the
dagster command it was handed. Both entry points go through it — the image's
`ENTRYPOINT`, and every dagster target in the Makefile — so a laptop run and a
container run configure the instance the same way. With no Postgres settings in
the environment it leaves the local SQLite default alone, and will not clobber a
`dagster.yaml` you wrote by hand.

`DAGSTER_POSTGRES_SCHEMA` overrides the schema name; it reaches libpq as a
`search_path` in the connection URL rather than as anything Dagster is told
about, which is why the schema has to exist first. `db.py check` lists what
lands there.

One constraint worth knowing: `URBAN_RAG_PG_IAM_AUTH=1` works for the corpus
but not for this. RDS IAM tokens expire after fifteen minutes and Dagster holds
its instance-storage connections open indefinitely, so a durable password or a
Secrets Manager id is required here — `dagster_home.py` says so rather than
failing later.

When a laptop reaches RDS through `make db-tunnel`, keep TLS and the tunnel's
address separate. With `sslmode=verify-full`, `URBAN_RAG_PG_HOST` must stay as
the RDS endpoint, because that is the name in the server certificate. Point the
socket at the local tunnel with `URBAN_RAG_PG_HOSTADDR`:

```bash
URBAN_RAG_PG_HOST=hbu-dev.cedzstv1bm7z.us-east-1.rds.amazonaws.com
URBAN_RAG_PG_HOSTADDR=127.0.0.1
URBAN_RAG_PG_PORT=5433
URBAN_RAG_PG_SSLMODE=verify-full
```

For Docker Compose, leave the bastion tunnel open on the host and let Compose
map the RDS hostname to Docker's host gateway instead:

```bash
cd ../hbu_infra
make db-tunnel ENV=dev LOCAL_PORT=5433

cd ../hbu_dataplatform
make up-tunnel TUNNEL_DB_HOST=hbu-dev.cedzstv1bm7z.us-east-1.rds.amazonaws.com TUNNEL_PORT=5433
```
