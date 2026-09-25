# Operating the pipeline

## Running

Every command below has a Makefile target on Linux (`make dagster_run`,
`make materialize`, `make corpus`, `make index`); `make help` lists them with
the variables they read. Raw:

`--select` takes an asset's full `<layer>/<asset>` key — the prefix
`hbu_dataplatform.layers` gives it. A bare name selects nothing and dagster answers
`DagsterInvalidSubsetError`, naming the prefixed key it meant.

Raw invocations skip the `hbu_dataplatform.dagster_home` entrypoint the Makefile and
the image both go through, so they read whatever `dagster.yaml` is already in
`DAGSTER_HOME` — prefix them with `python -m hbu_dataplatform.dagster_home` to get the
Postgres config generated from the environment instead.

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"

# UI
uv run dagster dev

# or headless, one partition at a time
uv run dagster asset materialize --select bronze/spectrum_table_catalog --partition 2026-09-01 -m hbu_dataplatform.definitions
uv run dagster asset materialize --select bronze/neighborhood_features --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions

# the open-data neighborhoods, date-partitioned only
uv run dagster asset materialize --select bronze/reference_neighborhoods --partition 2026-09-01 -m hbu_dataplatform.definitions

# the cadastral lots for that borough, which read the snapshot above
uv run dagster asset materialize --select bronze/neighborhood_lots --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions

# the CMHC vacancy rates for that borough, which depend on nothing upstream
uv run dagster asset materialize --select silver/vacancy_rates --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions

# the CMHC average rents for that borough, also independent
uv run dagster asset materialize --select silver/average_rents --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions

# the province-wide RQTT road network, date-partitioned only
uv run dagster asset materialize --select bronze/street_network --partition 2026-09-01 -m hbu_dataplatform.definitions

# the Montreal construction cost rates, date-partitioned only
uv run dagster asset materialize --select bronze/montreal_residential_costs,bronze/montreal_nonresidential_costs --partition 2026-09-01 -m hbu_dataplatform.definitions

# the cell's road centre lines - the sides whose midpoint falls in it, whole.
# Everything from here on that reads the cadastre is DATE x TILE: one cell of
# the quadkey cut (`make tiles` lists them; see architecture.md, "Two spatial
# axes"), not a borough
uv run dagster asset materialize --select silver/neighborhood_streets --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions

# the hop from the borough to the cells: land VSMPE's lots, buildings and
# features in rag.* with each row's cell address. Its `tiles_touched` metadata
# is the list of cells the tile runs below have to be run for
uv run dagster asset materialize --select silver/neighborhood_cadastre --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions

# the two spatial joins, for the lots this cell owns against every building
# and feature in the snapshot
uv run dagster asset materialize --select silver/building_lot_intersections --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions

# the province-wide assessment roll and the merge that makes it readable, both
# date-partitioned only. The first run of a roll year downloads 572 MB and
# unpacks a 2.8 GB GeoPackage into data/cache/role/; later dates reuse both.
# Needs reference_neighborhoods for the same date: the merge is also cut into
# one silver.assessment_units partition per borough against those outlines.
# cubf_use_codes rides along because the merge looks every unit's use code up
# in it, and fails naming it when that date has none
uv run dagster asset materialize --select bronze/property_assessment_roll,bronze/cubf_use_codes,silver/assessment_units --partition 2026-09-01 -m hbu_dataplatform.definitions

# what every lot in that cell is assessed at — needs the roll above for the
# same date and neighborhood_cadastre for every borough the cell holds
uv run dagster asset materialize --select silver/lot_assessed_values --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions

# what each lot yields on that, and which lots are like it — needs the line
# above for every cell in reach (the pool is the snapshot, not the cell) plus
# vacancy_rates and average_rents for the boroughs the cell holds. Run it
# with the same BY_POINT as lot_assessed_values above: that flag decides which
# units reach a lot at all
uv run dagster asset materialize --select silver/lot_assessment_comparables --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions

# how much street each lot faces — needs neighborhood_cadastre first, for the
# rag.lots this reads, and hbu_infra's sql/007 + sql/008 applied
uv run dagster asset materialize --select silver/lot_frontage --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions

# then the corpus over that snapshot's linked PDFs
uv run dagster asset materialize --select "bronze/linked_documents,silver/document_chunks,silver/document_embeddings" --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions

# and publish those vectors to the Postgres/pgvector store
uv run dagster asset materialize --select gold/document_index --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions

# the ground each zone governs, cut out of each lot: one row per (lot, zone),
# with that piece's own area and the street it faces. Everything below reads
# it, because a zoning boundary crossing a large parcel makes two sites of it
uv run dagster asset materialize --select silver/lot_zone_pieces --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions

# the zoning grids read as tables - per borough, a grid is a by-law's - and
# the envelope per (lot, zone, grid column), per cell
uv run dagster asset materialize --select silver/zoning_grid_columns --partition "2026-09-01|VSMPE" -m hbu_dataplatform.definitions
uv run dagster asset materialize --select silver/lot_zoning_envelopes --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions

# finally the gold row per lot, which reads the silver parquet partitions
# above as well as rag.lots — needs hbu_infra's sql/009 + sql/006
uv run dagster asset materialize --select gold/lot_profiles --partition "2026-09-01|0302303330102" -m hbu_dataplatform.definitions
```

Schedules run monthly in `America/Toronto`, all on the 1st. Nineteen of them,
in three bands — the sources that have no upstream here first, then the
borough cuts, then the cadastre landing and the tile runs over it. A tile
schedule fires once per cell of the cut rather than once per registered
borough:

| | |
| --- | --- |
| 04:00 | `spectrum_table_catalog` |
| 04:20 | `neighborhood_features` |
| 04:40 | `reference_neighborhoods` |
| 04:45 | the two CMHC surveys · `uniformized_property_wealth` |
| 04:47 | the MarketBeats and the rent index · the two cost snapshots |
| 04:50 | `street_network` — the province-wide RQTT |
| 04:52 | `property_assessment_roll`, `cubf_use_codes` and `assessment_units` |
| 05:40 | `neighborhood_lots` |
| 05:50 | `neighborhood_buildings` |
| 05:55 | `vacancy_rates` |
| 05:58 | `average_rents` |
| 06:10 | `commercial_rents` |
| 06:20 | `neighborhood_streets` — the cell's sides of the RQTT, whole |
| 07:00 | `neighborhood_cadastre` — every registered borough landed in `rag.*` |
| 07:20 | `building_lot_intersections` — per cell |
| 07:40 | `lot_assessed_values` — per cell |
| 08:20 | `lot_assessment_comparables` — per cell, and forty minutes back because its pool is every cell's values |

The minutes inside a band only keep independent fetches from overlapping; the
gaps between bands are real dependencies. Twelve assets have no schedule at
all, and neither do the envelope pair, the corpus chain or
`neighborhood_addresses` — see [Assets](assets.md). All scheduled runs target
*this month's* partition — `end_offset=1` on the monthly partitions exists for that reason,
since "scrape date" means the month the fetch happened in, not a closed event
window. A partition key is always the first of its month (`2026-09-01`), and a
run started on any other day of the month lands on that same key.

## Adding neighborhoods

The neighborhood axis is a `DynamicPartitionsDefinition`: which boroughs the
pipeline scrapes is recorded in the Dagster instance - the `dagster` schema on
Postgres, or the local home - rather than in code. Register a key with

```bash
make neighborhood-add NEIGHBORHOOD=CIL      # `make neighborhoods` lists them
```

which refuses anything `known_neighborhoods()` in
[partitions.py](../src/hbu_dataplatform/partitions.py) cannot resolve into its
sources. All 17 Montreal borough namespaces, Quebec City's six
arrondissements and Saguenay's single `SAG` key are mapped there; a fresh
instance is seeded with `DEFAULT_NEIGHBORHOODS` (`VSMPE` and `CIL`) the first
time anything reads the axis. The schedules, the roll's borough cut and the UI's partition dialog all
read the same registered list. Existing partitions are untouched, and the new
borough starts at the **current** month. A Quebec City key reads other
publishers on the way in - see [quebec-city.md](quebec-city.md); so does
`SAG` - see [saguenay.md](saguenay.md).

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

## The tile axis

The lot chain — everything from `building_lot_intersections` to
`lot_building_massing` — is not partitioned by borough but by **cell of the
tile cut**: a Web Mercator tile named by its quadkey, `0302303330102` for the
Villeray cell, at whatever depth holds about 20,000 lots. The axis is static,
every cell of `hbu_dataplatform.tile_cut.CUT`, so there is nothing to register;
`make tiles` lists them with their city, and a partition key reads
`2026-09-01|0302303330102`. Why the chain runs this way is in
[architecture.md](architecture.md#two-spatial-axes).

What that changes for an operator:

- **Create the month's partitions before running cells side by side.**
  `make tiles-ensure DATE=...` creates every tile table's leaf for every
  cell, one short transaction each. A cell that creates its own mid-run
  takes an exclusive lock on the parent table: two doing it at once
  deadlock, and one alone blocks every other cell until it commits.
  Sequential runs do not need it; parallel ones do.
- **A borough is loaded, then its cells are run.** `make cadastre
  NEIGHBORHOOD=VSMPE` lands the borough in `rag.*` and reports
  `tiles_touched`; `make tiles-of NEIGHBORHOOD=VSMPE` asks Postgres for the
  same list afterwards. Every tile step then runs once per cell —
  `make frontage TILE=...` — and `scripts/materialize_borough.sh` does that
  loop for you: its tile steps resolve the borough's cells and run each.
- **A reload touches every cell the borough is in.** Reloading a borough
  remints `lot_uid` and the cascade empties its lots' rows in every cell
  they fall in, so the whole tile chain re-runs for those cells, not just
  one partition. A cell straddling two boroughs needs both landed before
  its runs mean anything.
- **A new city is a re-seed, not a registration.** The cut only covers
  ground that has been loaded, so the first `neighborhood_cadastre` run of
  a fourth city fails with `num_rows_outside_cut`. Apply hbu_infra's
  `028_cell_key.sql` so its lots have a `cell_key`, run
  `scripts/seed_tile_cut.py` behind the tunnel, paste the `TILE_CITIES` it
  prints into `tile_cut.py` (it refuses to print a cut that moves an
  existing cell — adding a city is additive), and deploy. The new cells
  appear on the axis; the old ones and everything under them are untouched.
- **Borough-axis inputs are read per lot.** The CMHC and C&W tables and the
  zoning grids stay per borough; a tile run resolves which boroughs its lots
  came from and joins each lot on its own `neighborhood`, which every row of
  the lot chain still carries.

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
See [guards.py](../src/hbu_dataplatform/guards.py).

## Talking to the service

The proxy in front of the Feature Service has two quirks that dictate how every
request is built — a mandatory `url` parameter, and the fact that it is the
*only* parameter forwarded. Both are documented at the top of
[spectrum.py](../src/hbu_dataplatform/spectrum.py), with the behaviors verified against
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
