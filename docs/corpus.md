# Corpus and retrieval

## The document corpus

The regulation tables carry no prose of their own. `LIEN_GRILLE` holds a
link, not a description:

```
Reglement_urbanisme__VSP_REG_ZONE.LIEN_GRILLE
  http://www1.ville.montreal.qc.ca/CartesInteractives/villeray/doc/zone/C01-001.pdf
```

So the retrievable text lives in the linked PDFs — one per zone, 633 rows
pointing at 632 distinct "grille des usages et des normes" documents for
VSMPE (two zones share a grid) — and three assets turn them into an embedded
corpus:

| Asset | Output |
| --- | --- |
| `linked_documents` | One row per distinct link: the PDF fetched and flattened to text |
| `document_chunks` | Paragraph-aligned, overlapping chunks, sized in the encoder's tokens — and the one of the three with a table of its own, `silver.document_chunks`, which keeps every scrape date rather than only the current one |
| `document_embeddings` | One 1024-wide float32 [BAAI/bge-m3](https://huggingface.co/BAAI/bge-m3) vector per chunk. No table: its vectors' home is `rag.chunks`, the pgvector index `document_index` writes, and a `silver.document_embeddings` would be a second copy of the only thing here measured in gigabytes |

```
data/
├── cache/pdf/<sha256(url)>.pdf          # outside the partitions on purpose
├── linked_documents/2026-09-01/VSMPE/documents.parquet
├── document_chunks/2026-09-01/VSMPE/chunks.parquet
└── document_embeddings/2026-09-01/VSMPE/embeddings.parquet
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
`DOCUMENT_SOURCES` in [rag/documents.py](../src/urban_rag/rag/documents.py) — today
`Reglement_urbanisme__VSP_REG_ZONE` → `LIEN_GRILLE`, and nothing else. Most
other tables link to web pages, photos, or one shared modality page.
`VSP_REG_PPCMOI` is the exception worth knowing about: its `EN_SAVOIR_PLUS`
holds 227 per-resolution PDFs and was the corpus before the zoning grids
replaced it, because a project-specific resolution answers a different question
than a zone's standing rules. Add it back to the registry when that question is
worth indexing.

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
  [rag/vss.py](../src/urban_rag/rag/vss.py) falls back to downloading the extension
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
[retriever.py](../src/urban_rag/rag/retriever.py) and the chain neither know nor
care which one answered.

### Loading is an asset, not a command

`urban-rag index --backend postgres` is the *reload*: it drops the table and
replays every partition, which is what changing encoder needs and what leaves
the corpus unqueryable while it runs. The steady state is the `document_index`
asset, one partition at a time:

```bash
make publish DATE=2026-09-01 NEIGHBORHOOD=VSMPE
# or
uv run dagster asset materialize --select gold/document_index --partition "2026-09-01|VSMPE" -m urban_rag.definitions
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
| `URBAN_RAG_PG_HOSTADDR` | optional tunnel address; `HOST` remains the name verified by TLS |
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

Through an SSM tunnel, do not set `URBAN_RAG_PG_HOST=localhost` with
`verify-full`: the RDS certificate is issued to the RDS endpoint, not to
`localhost`. Use the RDS endpoint as `HOST`, the tunnel port as `PORT`, and
`URBAN_RAG_PG_HOSTADDR=127.0.0.1` for native runs.

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
