# Operating the pipeline

## Running

Every command below has a Makefile target on Linux (`make dagster_run`,
`make materialize`, `make corpus`, `make index`); `make help` lists them with
the variables they read. Raw:

`--select` takes an asset's full `<layer>/<asset>` key — the prefix
`urban_rag.layers` gives it. A bare name selects nothing and dagster answers
`DagsterInvalidSubsetError`, naming the prefixed key it meant.

Raw invocations skip the `urban_rag.dagster_home` entrypoint the Makefile and
the image both go through, so they read whatever `dagster.yaml` is already in
`DAGSTER_HOME` — prefix them with `python -m urban_rag.dagster_home` to get the
Postgres config generated from the environment instead.

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"

# UI
uv run dagster dev

# or headless, one partition at a time
uv run dagster asset materialize --select bronze/spectrum_table_catalog --partition 2026-09-01 -m urban_rag.definitions
uv run dagster asset materialize --select bronze/neighborhood_features --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# the open-data neighborhoods, date-partitioned only
uv run dagster asset materialize --select bronze/reference_neighborhoods --partition 2026-09-01 -m urban_rag.definitions

# the cadastral lots for that borough, which read the snapshot above
uv run dagster asset materialize --select bronze/neighborhood_lots --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# the CMHC vacancy rates for that borough, which depend on nothing upstream
uv run dagster asset materialize --select silver/vacancy_rates --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# the CMHC average rents for that borough, also independent
uv run dagster asset materialize --select silver/average_rents --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# the island-wide street network, date-partitioned only
uv run dagster asset materialize --select bronze/street_network --partition 2026-09-01 -m urban_rag.definitions

# the Montreal construction cost rates, date-partitioned only
uv run dagster asset materialize --select bronze/montreal_residential_costs,bronze/montreal_nonresidential_costs --partition 2026-09-01 -m urban_rag.definitions

# that borough's sides of street, cut out of it
uv run dagster asset materialize --select silver/neighborhood_streets --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# the province-wide assessment roll and the merge that makes it readable, both
# date-partitioned only. The first run of a roll year downloads 572 MB and
# unpacks a 2.8 GB GeoPackage into data/cache/role/; later dates reuse both.
# Needs reference_neighborhoods for the same date: the merge is also cut into
# one silver.assessment_units partition per borough against those outlines
uv run dagster asset materialize --select bronze/property_assessment_roll,silver/assessment_units --partition 2026-09-01 -m urban_rag.definitions

# what every lot in that borough is assessed at — needs the two above for the
# same date and neighborhood_lots for the same partition
uv run dagster asset materialize --select silver/lot_assessed_values --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# what each lot yields on that, and which lots are like it — needs the line
# above plus vacancy_rates and average_rents for the same partition. Run it
# with the same BY_POINT as lot_assessed_values above: that flag decides which
# units reach a lot at all
uv run dagster asset materialize --select silver/lot_assessment_comparables --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# how much street each lot faces — needs building_lot_intersections first, for
# the rag.lots this reads, and hbu_infra's sql/007 + sql/008 applied
uv run dagster asset materialize --select silver/lot_frontage --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# then the corpus over that snapshot's linked PDFs
uv run dagster asset materialize --select "bronze/linked_documents,silver/document_chunks,silver/document_embeddings" --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# and publish those vectors to the Postgres/pgvector store
uv run dagster asset materialize --select gold/document_index --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# the zoning grids read as tables, and the envelope per (lot, grid column)
uv run dagster asset materialize --select "silver/zoning_grid_columns,silver/lot_zoning_envelopes" --partition "2026-09-01|VSMPE" -m urban_rag.definitions

# finally the gold row per lot, which reads the three silver parquet
# partitions above as well as rag.lots — needs hbu_infra's sql/009 + sql/006
uv run dagster asset materialize --select gold/lot_profiles --partition "2026-09-01|VSMPE" -m urban_rag.definitions
```

Schedules run monthly in `America/Toronto`, all on the 1st: the catalog at 04:00, the features
at 04:20, the reference neighborhoods at 04:40, the vacancy rates at 04:45 and
the average rents at 04:50 (the CMHC assets are independent of the Spectrum
assets — the minutes only keep them from overlapping), then the lots at 05:40,
the buildings at 05:50, the borough's street sides at 06:20 and the
building × lot join at 07:00 behind them. The
island-wide street network is snapshot at 04:50, alongside the other sources
that have no upstream here. The assessment lineage runs on its own chain: the
province-wide roll and its merge at 04:52, the per-lot totals at 06:30 behind
the cadastre, and the cap rates and comparables at 06:40 behind those and the
CMHC pair. `lot_frontage`, the envelope pair and `lot_profiles` have no schedule yet — see
[Assets](assets.md). All target *this month's*
partition — `end_offset=1` on the monthly partitions exists for that reason,
since "scrape date" means the month the fetch happened in, not a closed event
window. A partition key is always the first of its month (`2026-09-01`), and a
run started on any other day of the month lands on that same key.

## Adding neighborhoods

Add keys to `ENABLED_NEIGHBORHOODS` in [partitions.py](../src/urban_rag/partitions.py);
all 17 borough namespaces are already mapped there. Existing partitions are
untouched, and the new borough starts at the **current** month.

Adding a key crosses it with every month since `SCRAPE_START_DATE`, so the UI
will show the borough's earlier partitions as missing and offer to backfill
them. Do not: bronze records what a publisher returned *now*, and the sources
behind it have no time travel, so filling `2026-08-01` in September writes
September's data under an August key. The bronze assets refuse it — see
[the scrape-month guard](#the-scrape-month-guard) — and those earlier
partitions stay empty because the borough genuinely was not scraped then.

Materialize the current month for the new borough instead, and let the
schedules carry it from there. Its silver and gold partitions follow from that
bronze, and *those* are backfillable in the ordinary way.

## The scrape-month guard

Every bronze asset refuses a partition whose month is not the one being lived
in, whichever way the run was launched — the UI's backfill dialog, a schedule,
or `dagster asset materialize` from a `make` target. The refusal happens before
the fetch and before the partition directory is cleared, so a mistaken backfill
costs nothing:

```
bronze/street_network was asked for 2026-08-01, but bronze records what a
publisher returned *now* ... Materialize 2026-09-01 instead.
```

Silver and gold are deliberately not guarded: they recompute from bronze
parquet already on disk, so re-deriving a past month after a fixed crosswalk is
exactly what backfilling is for.

The one legitimate reason to write a past month is recovery — a scrape that ran
on the 1st, failed on the write, and was noticed in the following month. Launch
that run with the tag `urban_rag/allow_stale_scrape=true`, set in the Launchpad
or with `--tag`. It is logged as a warning and stays visible in the run's tags.
See [guards.py](../src/urban_rag/guards.py).

## Talking to the service

The proxy in front of the Feature Service has two quirks that dictate how every
request is built — a mandatory `url` parameter, and the fact that it is the
*only* parameter forwarded. Both are documented at the top of
[spectrum.py](../src/urban_rag/spectrum.py), with the behaviors verified against
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
shapes the proxy demands, and every PostGIS statement is stubbed at the
function that issues it.

### The spatial ones need a database

Stubbing PostGIS tests the asset's plumbing and not the measure, which is how a
`buffer_m` too small to reach 90 % of a borough's lots survived in
`compute_lot_frontage`: every unit test passed, because none of them ever
intersected a lot with a street. That measure is gone — frontage is now the
boundary a lot shares with the street's own cadastral parcel — but the lesson
stands, and the tests that would have caught it are these. `tests/integration` runs the real SQL against
a real PostGIS on a committed slice of VSMPE — 164 lots around lot 3 790 556,
see [tests/fixtures/frontage](../tests/fixtures/frontage/README.md). It is opt-in
and skips when `URBAN_RAG_TEST_PG_URL` is unset, so `make test` stays offline.

```bash
docker run -d --name urban_postgis \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=urban \
    -p 55432:5432 postgis/postgis:16-3.4-alpine

URBAN_RAG_TEST_PG_URL=postgresql://postgres:postgres@localhost:55432/urban \
    uv run pytest tests/integration
```

Point it at a **throwaway** database: the fixture applies hbu_infra's schema
(from `../hbu_infra/sql`, or `URBAN_RAG_INFRA_SQL`) and truncates the partition
it loads into.
