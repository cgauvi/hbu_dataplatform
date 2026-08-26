# urban_rag

Dagster pipeline that snapshots Montreal's [Spectrum Spatial Feature
Service](https://spectrum.montreal.ca/connect/analyst/controller/connectProxy/rest/Spatial/FeatureService)
into local (geo)parquet, partitioned by neighborhood and scrape date.

Currently enabled: **VSMPE** (Villeray–Saint-Michel–Parc-Extension, 24 tables).

## Layers

The pipeline is a medallion: **bronze** holds what a publisher returned,
**silver** the same facts at this platform's grain, **gold** one question
answered. The layer is declared once, in
[layers.py](src/urban_rag/layers.py), and both the Dagster asset key
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

Postgres is a **serving copy** of silver and gold, never the only copy.
`rag.lots`, `rag.buildings` and `rag.features` are silver tables that happen to
live in the serving database; `rag.lot_profiles` and `rag.chunks` are gold. The
tree is the record — losing the database costs a reload rather than a
re-scrape, which for a live municipal source no later run can undo.

## Assets

| Layer | Asset | Partitions | Output |
| --- | --- | --- | --- |
| bronze | `spectrum_table_catalog` | date | The service's full table list for that day (577 tables across 19 namespaces), as one `tables.parquet` |
| bronze | `neighborhood_features` | date × neighborhood | One parquet file per source table |
| bronze | `reference_neighborhoods` | date | Montreal's 91 housing reference neighborhoods from donnees.montreal.ca, with their dwelling counts |
| bronze | `neighborhood_lots` | date × neighborhood | Every cadastral lot inside that borough, from Quebec's Infolot service, as one `lots.parquet` |
| bronze | `neighborhood_buildings` | date × neighborhood | BDOI building footprints inside that borough, as one `buildings.parquet` |
| bronze | `cmhc_vacancy_survey` | date | The Montreal-CMA slice of the CMHC Rental Market Survey, as published |
| bronze | `cmhc_rent_survey` | date | The Montreal-CMA slice of the CMHC HMIP average-rent page, as published |
| bronze | `street_network` | date | Montreal's *géobase double* — one line per side of street — as published, island-wide |
| bronze | `montreal_residential_costs` | date | The Montreal column of the Altus construction cost guide's residential types — condo/apartment by storey band, townhouses, single family, seniors, student residences — in $/sf |
| bronze | `montreal_nonresidential_costs` | date | The same column's commercial and industrial types in $/sf, plus the three parking types in **$/stall** |
| bronze | `linked_documents` | date × neighborhood | The PDFs those tables link to, fetched and flattened to text |
| silver | `vacancy_rates` | date × neighborhood | That borough's quartiers taken out of the snapshot and averaged into one rate per dwelling type × bedroom class |
| silver | `average_rents` | date × neighborhood | The same, per bedroom class, for rents |
| silver | `building_lot_intersections` | date × neighborhood | Both spatial joins, computed against one load of the cadastre — repaired on the way in, which is where `make_valid` runs: building footprints clipped to the lots they intersect (`rag.building_lots`) and map features clipped to the lots they cover (`rag.lot_features`, the hop from a lot to its documents), as two geoparquet files |
| silver | `neighborhood_streets` | date × neighborhood | That day's street sides clipped to one borough, with the published length, the length inside it, and the share that survived the cut |
| silver | `lot_frontage` | date × neighborhood | How much of each lot's boundary faces each street side, in metres, longest first — as geoparquet and as `rag.lot_frontage`. **Blocked**, see below |
| silver | `zoning_grid_columns` | date × neighborhood | Those PDFs read as the tables they are — one row per column of each *grille des usages et des normes*, with its usages, authorised levels and every norm of its CADRE BÂTI block as columns |
| silver | `lot_zoning_envelopes` | date × neighborhood | Every lot's zoning envelope, denormalised to the grain `urban_rag.program` reads — one row per (lot, grid column), with the lot's area, its primary and secondary frontage, and the norms that bound what may be built on it |
| silver | `document_chunks` | date × neighborhood | Those documents cut into retrieval-sized chunks |
| silver | `document_embeddings` | date × neighborhood | A bge-m3 vector per chunk |
| gold | `lot_profiles` | date × neighborhood | Every lot in the borough, one row each — whether a building stands on it and how many, its primary and secondary street frontage in metres, the zoning PDF that covers most of it, the zoning envelopes that govern it, and the borough's CMHC vacancy and rent grids — as geoparquet and as `rag.lot_profiles`. **Blocked**, see below |
| gold | `document_index` | date × neighborhood | Those vectors upserted into the Postgres/pgvector store the query side reads |

The corpus assets are described under [The document
corpus](#the-document-corpus) and [The shared vector
store](#the-shared-vector-store).

The catalog is a separate asset because the published table list drifts —
boroughs add and retire layers without notice, and a scrape is only
reproducible if you know what existed on that date. `neighborhood_features`
reads that day's `tables.parquet` through a
`MultiToSingleDimensionPartitionMapping`, so the `2026-08-18|VSMPE` partition
consumes exactly the `2026-08-18` catalog.

`document_index` is the one asset that writes no parquet of its own, and
deliberately: it is a *load* of `document_embeddings`, which is already in the
tree. Its record is that file.

`lot_profiles` is registered and has a job, but **no schedule**, because it
reads two relations hbu_infra has to create first. `sql/009_lot_profiles.sql`
creates the table it writes into, and `sql/006_lot_documents.sql` creates the
`rag.lot_documents` view it takes the document columns from — and that second
file carries a `-- requires: rag.chunks` header, so `db.py init` skips it on a
database that has never held a corpus and it only lands on the *next* init,
after `document_index` has run. Both files exist; what is outstanding is
`db.py init` against the target database, twice. `compute_lot_profiles` checks
for both up front and fails naming the file to apply rather than letting
psycopg raise `relation "rag.lot_profiles" does not exist`. Add the schedule
then. Run it by hand with `make lot-profiles`.

Five of its inputs come from the tree rather than from Postgres —
`lot_zoning_envelopes`, `vacancy_rates`, `average_rents` and the two
`montreal_*_costs` snapshots — and the envelope pair has no schedule of its own
either, so a scheduled `lot_profiles` would need `zoning_envelopes_job` ahead
of it. A partition missing any of the five fails naming the asset to
materialize, before the rows it was going to replace are deleted. Run the pair
by hand with `make envelopes`, and the cost snapshots with `make costs` — those
are partitioned by date alone, so one run of them serves every borough of that
day.

`lot_frontage` is blocked the same way, one step further along: hbu_infra
*has* `sql/007_streets.sql` and `sql/008_lot_frontage.sql`, but a database
they have not been applied to yet answers `relation "rag.streets" does not
exist`. So it too is registered, given a job, and left off the schedules
until `db.py init` has run against the target database. Run it by hand with
`make frontage`. Its upstream `neighborhood_streets` writes only parquet and
is scheduled normally.

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
│   └── linked_documents/2026-08-18/VSMPE/
│       └── documents.parquet
├── silver/
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
[S3 output](#s3-output). One prefix per asset means a partition can be listed,
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
<root>/bronze/reference_neighborhoods/2026-08-20/
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
<root>/bronze/neighborhood_lots/2026-08-20/VSMPE/
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

Two assets, and the seam between them is where this pipeline's bronze/silver
line is easiest to see.

`cmhc_vacancy_survey` (bronze) reads CMHC's [Rental Market
Survey](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market/urban-rental-market-survey-data-vacancy-rates)
— one workbook a year, every Canadian centre in it — and keeps the slice where
`Province == "Qc"` and `Centre == "Montréal"`, exactly as published: every
quartier the survey prints for that centre, under the survey's own spellings,
subtotal rows included. It is partitioned by **date alone**, because there is
nothing borough-shaped about a workbook — one read per day rather than the same
file re-downloaded and re-parsed once per enabled borough.

`vacancy_rates` (silver) applies the crosswalk to that snapshot:

```
<root>/bronze/cmhc_vacancy_survey/2026-08-20/
└── quartier_vacancy_rates.parquet    # every Montreal-CMA quartier, as published

<root>/silver/vacancy_rates/2026-08-20/VSMPE/
├── vacancy_rates.parquet             # 15 rows: the borough average
└── quartier_vacancy_rates.parquet    # 45 rows: this borough's, relabelled
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
refused, and a quartier the map names but the snapshot does not publish **fails
the partition** rather than quietly shortening the average.

That refusal is the reason for the split. It is a *silver* failure: the bronze
snapshot lands regardless, so a respelling costs a one-line crosswalk fix and a
re-run of one silver asset against parquet already on disk — where before it
cost the day's scrape and left nothing behind to diagnose it with. The same
applies to `cmhc_rent_survey` and `average_rents`.

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
uv run dagster asset materialize --select silver/vacancy_rates --partition "2026-08-20|VSMPE" -m urban_rag.definitions
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
uv run dagster asset materialize --select silver/average_rents --partition "2026-08-20|VSMPE" -m urban_rag.definitions
```

It writes five borough rows, one per `bedroom_type`, plus the quartier cells
behind the mean:

```
<root>/silver/average_rents/2026-08-20/VSMPE/
├── average_rents.parquet
└── quartier_average_rents.parquet
```

The rent is an unweighted mean of published quartier rents, in dollars as
published. Suppressed `**` cells stay null and drop out of the mean.

### Where the CMHC grid meets the cadastre

Not here, and no longer in a silver asset of its own. There used to be a
`lots_with_vacancy_rates` between `neighborhood_lots` and
`building_lot_intersections`, whose job was to widen CMHC's `dwelling_type` ×
`bedroom_type` grid into `cmhc_*` columns and merge it onto every lot by the
partition's `neighborhood` name. The merge was correct and the placement was
not: the grid rode from there into `rag.lots.attributes` and through both
PostGIS joins without anything reading it, and it made every lot file in the
tree carry sixty survey columns that said nothing about the parcel.

Both surveys are borough figures, so the denormalisation has to happen
somewhere — CMHC publishes no geometry and there is nothing to join on but the
name. It now happens at [the grain that asks the
question](#every-lot-profiled), as `vacancy_rates` and `average_rents` jsonb on
`rag.lot_profiles`. What was left of that asset was the geometry repair, and it
moved into `building_lot_intersections`, next to the `ST_Intersection` calls it
exists for.

## What it costs to build

A zoning envelope says what *may* be built on a lot. It says nothing about
what that would cost, and a highest-and-best-use answer needs both. The cost
side comes from the Altus Group Canadian Cost Guide, as published by the ZEF
construction estimator:

- [zef-builds.github.io/construction-estimator](https://zef-builds.github.io/construction-estimator/),
  a browser tool over the 2026 guide, nine Canadian markets, ~64 building types.

Unlike every other source here it publishes nothing to fetch as data. The cost
table ships as one of the JavaScript files the page loads —
`data/building-types.js`, 16 kB, declaring `CITIES` and a `TYPES` array whose
entries carry a `rates` map of `city -> [low, high]`. That file is the source,
so `urban_rag.estimator` reads it directly rather than driving the page, and
parses the array literals rather than evaluating them: bare keys are quoted and
the result goes through `json.loads`, so a file that grows a function call
fails with its own text in the message instead of running.

Two assets, split the way a proforma asks the question rather than the way the
guide is laid out:

```
bronze/montreal_residential_costs      date   condo/apartment by storey band, and the rest of `residential`
bronze/montreal_nonresidential_costs   date   commercial, industrial, parking
```

Both are **date-partitioned only**. The guide prices nine markets and knows
nothing about boroughs, so there is no borough axis to partition on — the same
posture as `street_network` and the two CMHC surveys. Taking the Montreal
column out of a nine-city table is a bound on what was asked for rather than an
interpretation of what came back, which is what keeps these in bronze.

The storey band is the axis the residential rates actually vary along, and it
is published as part of the label rather than as a number:

| `label` | `rate_low` | `rate_high` |
| --- | ---: | ---: |
| Condominium / Apartment (Up to 12 Storeys) | 275 | 335 |
| Condominium / Apartment (13–39 Storeys) | 320 | 330 |
| Condominium / Apartment (40–60 Storeys) | 330 | 375 |
| Condominium / Apartment (60+ Storeys) | 330 | 425 |
| Wood Frame Condo (Up to 6 Storeys) | 225 | 290 |

Those are dollars per square foot, and the rows arrive in the guide's own
order, which for this family is ascending storeys. **The band stays inside the
label.** Turning `(13–39 Storeys)` into a `storeys_min`/`storeys_max` pair is
reading the label, which is silver's job — see the layer contract in
`urban_rag.layers`. There is no silver asset over these yet; when there is, it
is the place that parse belongs.

### Parking is priced per stall, not per square foot

The one thing to read before reading a rate. `unit_flag` carries whichever
optional key the publisher set on the entry, and is null for everything priced
per square foot:

| `label` | `cat` | `unit_flag` | `rate_low` | `rate_high` |
| --- | --- | --- | ---: | ---: |
| Warehouse | industrial | *(null)* | 120 | 185 |
| Parking – Underground Garage | parking | `perStall` | 51,925 | 68,675 |
| Parking – Above Grade Garage | parking | `perStall` | 38,500 | 57,750 |
| Parking – Surface Lot | parking | `perStall` | 3,960 | 8,250 |

The figures are four orders of magnitude apart, so a reader that ignores the
column will not get a slightly wrong answer — it will get a stall priced as a
square foot. The guide's `institutional` and `infrastructure` categories are
published too and are read by neither asset.

Column names are the publisher's own keys wherever the publisher has one
(`id`, `label`, `sector`, `cat`, `sourceNote`), so a partition lines up against
`data/building-types.js` without a crosswalk. Beside them are the usual
snapshot columns, plus one this source needs more than most:
`source_last_modified` is the `Last-Modified` header the script was served
with — the publisher's own answer to *when did this last change*, for a file
that carries no version stamp of its own.

Run both by hand with `make costs`; they are scheduled at 4:47 alongside the
CMHC surveys, which have no upstream in this pipeline either.

## From a lot to its documents

A highest-and-best-use question is asked about a lot, and answered out of a
PDF. `rag.chunks.feature_ids` already covers the second half of that trip: it
records which map features cite each indexed document, because the corpus is
built from `VSP_REG_ZONE.LIEN_GRILLE`, one *grille des usages et des normes*
per zone. What was missing is the first half — from a lot to the features
covering it.

**There is no id for that hop, and there cannot be one.** The two sides come
from different governments:

| | Source | Key |
| --- | --- | --- |
| `neighborhood_lots` | Infolot, Registre foncier du Québec | `NO_LOT` |
| `neighborhood_features` | Spectrum Spatial, Ville de Montréal | `NUMERO_COMPLET` |

Neither publisher carries the other's key, and the provincial cadastre has no
municipal zoning column at all — a lot record knows its number, its area and
its survey history, and nothing about what may be built on it. Geometry is the
only thing the two layers share, so the join is spatial by necessity rather
than by preference.

`building_lot_intersections` is that join, alongside the building one. Per
date × neighborhood partition it loads the borough's map features into
`rag.features` — which nothing filled before, so `rag.chunk_features`,
`rag.search_near` and `rag.search_at_lot` had all been reading an empty table —
loads its lots into `rag.lots`, and intersects the two into `rag.lot_features`,
one row per (lot, feature) pair holding the clipped slice and `pct_of_lot`, the
share of the lot that feature covers.

### One asset, three upstreams, two joins

The lot × feature and building × lot joins live in one asset because they are
one load. Both are computed against `rag.lots`, and both fill it from the same
`neighborhood_lots` file — so as two assets they replaced that table twice per
partition, in transactions that raced each other for it: whichever committed
second overwrote the rows the first had just computed against.

So `building_lot_intersections` takes three upstreams — `neighborhood_lots`,
`neighborhood_buildings` and `neighborhood_features` — loads all three inside
one transaction, computes both joins against that single load, and writes both
to one partition directory:

```
<root>/silver/building_lot_intersections/2026-08-20/VSMPE/
├── building_lots.parquet
└── lot_features.parquet
```

The cost is that the two now fail together: a borough whose feature scrape
landed empty no longer gets its building join either. That is the honest
reading anyway — a partition holding one join and not the other was never a
state the downstream `lot_profiles`/corpus pair could use.

### Where the cadastre is repaired

Here, on the way into `rag.lots`. `neighborhood_lots` is bronze: it *counts*
self-intersecting rings and writes them through, because a snapshot is a
faithful copy of what Infolot returned. What reads them is `ST_Intersection`,
twice, and on an invalid ring that either raises or returns a shape nobody
asked for — so `make_valid` runs once, before the load, and
`num_geometries_repaired` reports what it touched rather than fixing it
silently.

One row per `NO_LOT` is checked at the same time. Infolot answers a boundary
query by object id, so the same lot comes back twice when a borough outline is
a multipolygon and the lot straddles two of its rings; `load_lots` would
swallow that with `ON CONFLICT … DO NOTHING`, and a duplicate that got past it
would multiply every pair both joins produce — a plausible-looking number
rather than a crash.

### Computed in PostGIS, kept as geoparquet

Both spatial joins — and the gold `lot_profiles` on top of them — run their work
in Postgres, because by the time they run PostGIS already holds every layer
loaded and GiST-indexed, and `ST_Intersection` over those indexes is the tool
for the job. Where the work happens and where the record lives are separate
questions, though: each join is read back out of the same transaction that
computed it and written to the tree as geoparquet.

That matters more here than it usually would. Every input is a dated snapshot
of a live municipal or provincial service, and **no later run can re-scrape an
earlier day** — so a table that existed only in Postgres could not be rebuilt
after the database was lost, and "just recompute it from bronze" holds only for
as long as bronze is still there and the code still agrees with it. One extra
query per partition turns each join into a file.

The parquet also carries a key the surrogate `*_uid` columns do not: `lot_number`
is joined into `building_lots.parquet`, and `lot_features.parquet` already holds
the `(source_table, feature_id)` pair `rag.chunks` cites. Those survive a reload
minting fresh bigserials; the `*_uid` values do not.

A side benefit: the joins are now readable — and testable — without a live
database.

The chain then runs end to end in SQL:

```
rag.lots  ──ST_Intersection──▶  rag.lot_features  ──feature_id──▶  rag.chunks
 NO_LOT                          pct_of_lot                        url, text
```

and `rag.lot_documents` (hbu_infra, `sql/006_lot_documents.sql`) is that chain
written once:

```sql
SELECT lot_number, feature_id, pct_of_lot, url
  FROM rag.lot_documents
 WHERE lot_number = '1425926'
   AND coverage_rank = 1;
```

`rag.search_at_lot_number(embedding, '1425926')` is the retrieval version:
`rag.search_at_lot` entered from a lot number rather than a point, reading the
join back out of `rag.lot_features` instead of recomputing it per query.

### Two things the geometry makes true

**A lot can have more than one zone, and usually one of them is noise.** A
cadastral boundary and a zoning boundary are drawn by different offices from
different surveys, so they miss each other by centimetres all along a street
and every lot picks up a sliver of its neighbour's zone. Nothing is thresholded
away at load time — the same posture `rag.building_lots` takes toward the
corner of a triplex crossing a lot line — because the cutoff belongs to the
question, not to the geometry. `pct_of_lot` is the column to filter on, and
`coverage_rank = 1` takes the zone that actually governs the lot. A lot
genuinely split between two zones has two real rows, and no ranking will tell
you which of those is the artifact.

**Layers are loaded under the file slug, not the Spectrum path.** A feature
parquet carries `source_table = /19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE`,
but `linked_documents` writes the slug `Reglement_urbanisme__VSP_REG_ZONE` into
`rag.chunks.source_table`, and every join from geometry to corpus matches those
two columns to each other. `load_features` takes the slug for that reason;
loading a layer under the path would fail nothing and match nothing.

The slug also drops the borough namespace, and zone numbers restart at
`C01-001` in every borough — so the borough is part of the match too, not just
of the partition filter, both in `rag.lot_features` and in
`rag.chunk_features`.

## The street network, and what a lot fronts on

Frontage is the measurement a highest-and-best-use question turns on after
area. Two 400 m² lots side by side are not the same development site if one has
30 m on a boulevard and the other 6 m on a lane: the width of the street edge
decides what can be built, how it is entered, and what it is worth. Neither
publisher records it. Infolot draws the parcel; Montreal draws the roadway; the
relation between them is geometry — the same situation `rag.lot_features` is
in, and it is answered the same way.

The street layer is the *géobase double*, not the plain géobase:

- [donnees.montreal.ca/dataset/geobase-double](https://donnees.montreal.ca/dataset/geobase-double),
  CC BY 4.0, ~91 MB of GeoJSON, 91,546 features.

The plain géobase draws one centre line per segment. The double projects that
onto the curb and sidewalk limits and draws **one line per side of street**,
which is what a frontage question needs: a lot faces one side of a street, not
the axis of the roadway. `COTE_RUE_ID` is the publisher's key for a side and is
unique island-wide, which is what lets `rag.streets` upsert against a real
natural key rather than replace a partition wholesale.

Three assets, and the borough axis appears in the middle one:

```
bronze/street_network        date            the island, as published
silver/neighborhood_streets  date × borough  clipped to one borough
silver/lot_frontage          date × borough  the join, in metres
```

`street_network` is partitioned by date alone because one download serves every
borough — the same posture as `reference_neighborhoods` and the two CMHC
surveys. `neighborhood_buildings` and `neighborhood_lots` cut their sources to
a borough in *bronze*, at the query, because those sources are fetched per
borough; here the cut happens in silver, from a file already on disk, since
re-downloading 91 MB per partition would be work done for nothing.

The cut is a real clip, not a selection. A side crossing the borough line is
`ST_Intersection`-ed against the boundary, so the geometry in a `VSMPE`
partition is inside VSMPE. What was published survives beside it:

| Column | |
| --- | --- |
| `segment_length_m` | the side as the city published it |
| `length_in_borough_m` | what is left after the clip |
| `pct_in_borough` | the share that survived it |

VSMPE's first snapshot: 4,262 sides of the island's 91,546, 445.8 km, 170 of
them cut at the boundary. Lengths are computed in **EPSG:32188** (NAD83 / MTM
zone 8), not in the 4326 the geometry is stored in — a degree is not a metre,
and `GeoSeries.length` on lon/lat returns a number in degrees that reads like
one in metres. PostGIS gets the same answer downstream through `geography`;
GeoPandas has no such type, so the projection is explicit.

### The measure is taken on the lot's boundary

The obvious statement of the question does not work:

```sql
-- Reports 0 for every row.
SELECT ST_Length(ST_Intersection(lot.geom, street_buffer.geom))
```

That intersects two polygons, gets a polygon, and **`ST_Length` of an areal
geometry is 0 in PostGIS** — `ST_Perimeter` is the function for polygons.
Frontage is a length along the parcel's *edge*, so the left-hand side has to be
the boundary:

```sql
SELECT ST_Length(geography(
    ST_CollectionExtract(
        ST_Intersection(ST_Boundary(l.geom), ST_Buffer(geography(s.geom), 3.0)::geometry),
        2  -- linework only: a boundary grazing the buffer clips to a point
    )
))
```

Two details in there earn their keep. The buffer goes through `geography` so
its distance is in **metres**; `ST_Buffer` on a 4326 geometry takes degrees,
where `3` would be some 300 km. And `ST_CollectionExtract(..., 2)` drops the
points where a boundary only touches the buffer's edge — an intersection, but
not a frontage.

### Why there is a buffer at all

A lot line does not sit on the curb line. There is a sidewalk between them, and
the city publishes the géobase *à titre indicatif* rather than to survey
accuracy, so the two layers disagree by a metre or two even where they agree in
substance. `buffer_m` is how much of that to forgive.

It is `FrontageConfig` rather than a constant, the same way `lot_profiles` makes
its shed cutoff config: it is a judgement about the street section, not a
property of the data. The default is **3 m** — wide enough to cross a sidewalk
and the publisher's own error, narrow enough to limit the one bias the measure
has: the first `buffer_m` of each *side* boundary falls inside the buffer too
and is counted as frontage, so a widened buffer inflates every corner. Every
row carries the `buffer_m` it was computed with, so a table can always be read
back against its own cutoff.

```
make frontage DATE=2026-08-18 NEIGHBORHOOD=VSMPE BUFFER_M=5
```

### Reading the result

One row per (lot, street side) that face each other, ordered longest frontage
first — in SQL, so the parquet answers "which lots have the most street" by
being read from the top.

`frontage_rank` is 1 for a lot's longest frontage and is the column to filter
on when a question wants *the* street a lot fronts on:

```sql
SELECT lot_number, street_name, round(frontage_m::numeric, 1) AS frontage_m
FROM rag.lot_frontage
WHERE neighborhood = 'VSMPE' AND scrape_date = '2026-08-18'
  AND frontage_rank = 1
ORDER BY frontage_m DESC
LIMIT 20;
```

A corner lot legitimately has two rows; a lot clipping a side street by 40 cm
has a second one that is a survey artifact. Nothing is thresholded away at load
time, for the same reason `rag.lot_features` keeps its slivers — the cutoff
belongs to the question. `num_lots_without_frontage` in the run's metadata is
the number to watch: a few percent are true interior parcels, a third of the
borough is a street snapshot that stopped short.

### What this asset does *not* load

`rag.lots`. It is already holding this partition when `lot_frontage` runs,
because `building_lot_intersections` put it there, and that asset's docstring
explains why loading the cadastre from two places is a race rather than a
redundancy: whoever commits second replaces the rows the first just computed
against. So `lot_frontage` depends on it, loads only `rag.streets`, and fails
with a message naming it if `rag.lots` comes back empty — the same guard
`lot_profiles` carries for the same reason.

## Every lot, profiled

`lot_profiles` is the gold table of the lot lineage. Four upstreams each hold
one row per (lot × something), and each of them is the wrong shape for the
question a person actually asks:

| upstream | grain | what it contributes |
|---|---|---|
| `rag.building_lots` | (building, lot) | `num_buildings`, `built_area_m2`, `category` |
| `rag.lot_frontage` | (lot, street side) | `primary_*` and `secondary_*`, `num_frontages` |
| `rag.lot_documents` | (lot, feature, document) | `doc_*` and the `documents` array |
| `silver/lot_zoning_envelopes` | (lot, grid column) | `num_zoning_envelopes` and the `zoning_envelopes` array |

Four more contribute nothing per-lot at all, and are written identically onto
every row of the partition:

| upstream | grain | what it contributes |
|---|---|---|
| `silver/vacancy_rates` | (dwelling type, bedroom class) | `vacancy_rates`, `overall_vacancy_rate_pct` |
| `silver/average_rents` | (bedroom class) | `average_rents`, `overall_average_rent_cad` |
| `bronze/montreal_nonresidential_costs` | (building type) | the parking half of `construction_costs`, `underground_stall_cost_low/high_cad`, `above_grade_stall_cost_low/high_cad` |
| `bronze/montreal_residential_costs` | (building type) | the condo half of `construction_costs`, `condo_cost_low/high_cad_sqft` |

Somebody asking "what can I do with lot 1 234 567" wants one row. This asset is
where they collapse onto it, and every one of the per-lot four arrives by
`LEFT JOIN`: a lot no building touches, a lot facing no street, a lot no
document covers and a lot no readable grid reaches are each a real answer, and
an inner join would delete exactly those rows.

The last five never reach Postgres as tables. `lot_zoning_envelopes` is
staged into a temp table keyed on `lot_number` — not `lot_uid`, which is a
bigserial `load_lots` mints again on every reload — and aggregated by the same
kind of `LEFT JOIN` as the rest. The two CMHC objects are passed as jsonb
parameters, because CMHC surveys neighborhoods and publishes no geometry: there
is nothing per-lot about them and nothing to join on. The two cost snapshots
are one jsonb parameter between them, for a stronger version of the same
reason — the Altus guide prices nine Canadian *markets* and no geometry at all,
so a Montreal rate is a Montreal rate on every lot of every borough. They are
also the only upstream here partitioned **by date alone**: every borough of a
day reads the same two files, and a partition missing one fails naming a date
rather than a borough.

### Why it is not `vacant_lots`

It used to be. That asset selected the parcels carrying nothing bigger than a
shed, which made it a table that could answer *where is the empty land* and
nothing else — every lot its `WHERE` clause dropped was a lot the reader could
no longer see, so "the widest built lots on this street" or "how much of the
borough is built out" needed a different table over the same join.

Keeping every lot and carrying `has_building` alongside costs one boolean
column and turns the old question into a filter over an inventory:

```sql
SELECT lot_number, primary_street_name, round(primary_frontage_m::numeric, 1)
FROM rag.lot_profiles
WHERE neighborhood = 'VSMPE' AND scrape_date = '2026-08-18'
  AND NOT has_building
ORDER BY primary_frontage_m DESC NULLS LAST
LIMIT 20;
```

`category` is carried forward unchanged, so the three-way distinction that
asset drew is not lost — `no_building`, `shed_only`, and `building_sliver` for
the corner of a neighbour's triplex crossing the cadastral line. It gains a
fourth value, `built`, for the case the old table expressed by having no row.

### `has_building` is not the negation of "vacant"

It is `num_buildings > 0` — "does any footprint intersect this parcel" — which
a 12 m² shed satisfies. Whether a lot is *usably* empty depends on a threshold,
a threshold is a judgement about the built form rather than a property of the
data, and a boolean cannot carry one. So that lives in `category`, computed
against `LotProfilesConfig.max_built_area_m2`, which every row records the way
`rag.lot_frontage` records its `buffer_m`:

```
make lot-profiles DATE=2026-08-18 NEIGHBORHOOD=VSMPE
```

The two columns are worth having side by side because they answer different
questions: one is a fact about the footprints, the other is this judgement
applied to them.

### The two frontages

`rag.lot_frontage` is one row per (lot, street side), ranked longest first;
these are its top two, pivoted. Ranks beyond the second are counted in
`num_frontages` and summed into `total_frontage_m` rather than given a third
pair of columns every other row would leave empty. Which edge is primary is
read off that table's own `frontage_rank` rather than re-decided here, so the
two cannot disagree.

A lot facing no street reports `num_frontages = 0` and **NULL** metres — not
`0`, which would claim it was measured. `num_without_frontage` in the run's
metadata is the number to watch, for the same reason it is in `lot_frontage`:
a few percent are true interior parcels, a third of the borough is a street
snapshot that stopped short.

### The linked PDF

The last hop of the chain `rag.lot_features` opens up. `rag.chunks.feature_ids`
records which map features cite each indexed PDF, and `rag.lot_documents` puts
that together with the features covering a lot. The highest-coverage document
**across every layer** is flattened into `doc_url`/`doc_title` so the common
read is a column rather than a jsonb path, and the whole set travels in
`documents` as JSON, most-of-the-lot first:

```json
[{"source_table": "vsp_reg_zone", "feature_id": "C01-001",
  "doc_id": "…", "url": "https://…/grille.pdf",
  "title": "Grille des usages et des normes C01-001", "pct_of_lot": 100.0}]
```

That column is written to the parquet as a JSON **string**, not as a nested
type. Letting pyarrow infer one types the column from whatever the partition
happens to hold — a borough where no lot has a document infers `list<null>` and
one where they do infers `list<struct<…>>`, which is two files with different
schemas for the same asset. Postgres keeps real `jsonb` to query; `json.loads`
on read is the cost. It is the same rule that makes `_fetch_partition`
re-select `scrape_date` as text.

`num_with_documents` at zero with a healthy `num_lots` means `rag.chunks` holds
no corpus for that partition, not that the borough is unzoned.

### The zoning envelopes

`silver/lot_zoning_envelopes` is one row per (lot, grid column) — the grain
[`urban_rag.program`](src/urban_rag/program.py) solves at — and
`zoning_envelopes` is that lot's rows, the zone covering most of it first:

```json
[{"feature_id": "C01-001", "source_table": "Reglement_urbanisme__VSP_REG_ZONE",
  "pct_of_lot": 92.0, "column_index": 0, "usages": ["H.1", "H.2"],
  "levels": ["1", "2"], "permits_residential": true, "floors_max": 3,
  "min_lot_width_m": 7.6, "site_coverage_max_pct": 70.0,
  "governs_residential": true, "solver_ready": true}]
```

Every column of that table travels except the lot's own — its area, its two
frontages, its neighborhood and scrape date are already columns of the profile
row and would only be restated once per envelope. A norm restated per envelope
is the trade the denormalisation makes; the lot's area restated per envelope is
just waste. `usages`, `levels` and `parse_notes` are decoded back into real
JSON on the way in: they are strings in the parquet so the file's schema is the
same shape in every partition, and `jsonb` has no such problem.

`governs_residential` marks the column `select_residential_column` picks for
that lot's width. There is at most one per (lot, zone) and often none, so it is
not flattened into a column of its own the way `doc_url` is.

`num_without_zoning_envelopes` is the number to watch. A lot with no envelope
is one no readable grid reaches — either the cadastre stretches past the feature
scrape, or that zone's PDF failed to parse — and `lot_zoning_envelopes` reports
the same gap from its own side as `num_lots_unzoned`. If the *staged* count and
the landed count disagree, the envelope file names lots this partition's
cadastre does not have, which is what a stale
`silver/lot_zoning_envelopes` looks like from here; the run logs it as a
warning naming the asset to re-materialize.

### The borough's rental market

`vacancy_rates` and `average_rents` are CMHC's grids for the borough, as one
object each:

```json
{"survey_year": 2023, "survey_period": "octobre 2023",
 "num_quartiers_mapped": 3, "overall_vacancy_rate_pct": 0.5,
 "num_published_cells": 4,
 "cells": [{"dwelling_type": "all", "bedroom_type": "all",
            "vacancy_rate_pct": 0.5, "min_vacancy_rate_pct": 0.3,
            "max_vacancy_rate_pct": 0.7, "num_quartiers": 2,
            "averaged_quartiers": "Parc-Extension, Villeray"}]}
```

Identical on every lot of the partition, which is the point: the question this
table is read for — what is this parcel worth building — is asked one lot at a
time and answered against the market the lot sits in. The provenance sits above
the cells because a borough figure is the unweighted mean of its quartiers with
most cells suppressed, so `num_quartiers` is the denominator that mean was
actually taken over, and the survey year says which publication it came from.

Every cell travels, suppressed ones included — a suppressed rate is a fact
about the survey, and an absent cell would read as a gap in the pipeline.
`overall_vacancy_rate_pct` and `overall_average_rent_cad` are the `all`/`all`
cells flattened onto columns of their own, the same rule `doc_url` follows, and
they are read back **out of the jsonb in SQL** rather than passed beside it, so
the column and the object it was flattened from cannot disagree. An empty `{}`
is a partition whose CMHC silver asset has not run; an object whose cells all
read null is a borough CMHC suppresses entirely. Only the first is a pipeline
problem.

### What it costs to build there

The rents are half of *what is this parcel worth building*. `construction_costs`
is the other half — Montreal's column of the Altus Group Canadian Cost Guide,
as the two `bronze/montreal_*_costs` snapshots publish it:

```json
{"city": "mtl", "city_label": "Montreal",
 "source_last_modified": "Tue, 12 Aug 2026 09:31:00 GMT",
 "cost_scrape_date": "2026-08-20",
 "condo_band": "condo_wood",
 "underground_stall_cost_low_cad": 51925, "underground_stall_cost_high_cad": 68675,
 "above_grade_stall_cost_low_cad": 38500, "above_grade_stall_cost_high_cad": 57750,
 "condo_cost_low_cad_sqft": 225, "condo_cost_high_cad_sqft": 290,
 "parking": [{"id": "parkade_ug", "label": "Parking – Underground Garage",
              "cat": "parking", "unit_flag": "perStall",
              "rate_low": 51925, "rate_high": 68675},
             {"id": "parkade_ag", "...": "..."}],
 "residential": [{"id": "condo_wood", "label": "Wood Frame Condo (Up to 6 Storeys)",
                  "cat": "residential", "unit_flag": null,
                  "rate_low": 225, "rate_high": 290},
                 {"id": "condo_12", "...": "..."}]}
```

**The two families are not in the same unit, and mixing them is the mistake the
guide's own `perStall` flag exists to prevent.** `parking` is dollars per
*stall*; `residential` is dollars per square foot. That is why they travel
under separate keys and why `unit_flag` rides on every entry — a reader who
takes a list whole still has the publisher's own answer to what a figure buys.

Six rates are flattened onto columns, the same rule `doc_url` and
`overall_average_rent_cad` follow, and read back **out of the jsonb in SQL**
for the same reason: a column and the object it came from cannot be allowed to
disagree. The parking pair is the choice a building actually makes — stalls dug
out underneath (`parkade_ug`) or a garage integrated into it at grade
(`parkade_ag`); underground is dearer per stall *and* larger per stall, while
above grade burns floor area the envelope would rather spend on dwellings,
which is what makes it a choice at all. `urban_rag.program` hardcodes the
midpoints of exactly these two pairs today; these columns are where it can read
them from instead. The guide's third parking type, `surface_lot`, is
deliberately not carried: an asphalt lot is not a parking structure.

`condo_cost_low/high_cad_sqft` is **one** of the five condominium / apartment
bands the guide prices by storey count. Which one is
`LotProfilesConfig.condo_type_id`, and it is config rather than a constant for
the same reason `max_built_area_m2` is — wood frame up to six storeys is what a
borough of triplexes builds, and a downtown parcel under a 40-storey envelope
is not that building at any price. The band chosen is named on every row as
`condo_band`, so a table can always be read back against the assumption that
produced it, and the other four bands stay in `residential` whatever it is set
to.

A type the guide stops publishing is a warning and a NULL column, not a failed
partition: losing one of sixty rates should not cost a borough its cadastre. A
*configured* band that was never a band is different — that is a typo in the
run config, and it fails before Postgres is touched.

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
`DOCUMENT_SOURCES` in [rag/documents.py](src/urban_rag/rag/documents.py) — today
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
uv run dagster asset materialize --select gold/document_index --partition "2026-08-18|VSMPE" -m urban_rag.definitions
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
[rag/vss.py](src/urban_rag/rag/vss.py)'s own downloader, so a running container
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

`--select` takes an asset's full `<layer>/<asset>` key — the prefix
`urban_rag.layers` gives it. A bare name selects nothing and dagster answers
`DagsterInvalidSubsetError`, naming the prefixed key it meant.

```powershell
$env:DAGSTER_HOME = "$PWD\.dagster_home"

# UI
uv run dagster dev

# or headless, one partition at a time
uv run dagster asset materialize --select bronze/spectrum_table_catalog --partition 2026-08-18 -m urban_rag.definitions
uv run dagster asset materialize --select bronze/neighborhood_features --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the open-data neighborhoods, date-partitioned only
uv run dagster asset materialize --select bronze/reference_neighborhoods --partition 2026-08-18 -m urban_rag.definitions

# the cadastral lots for that borough, which read the snapshot above
uv run dagster asset materialize --select bronze/neighborhood_lots --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the CMHC vacancy rates for that borough, which depend on nothing upstream
uv run dagster asset materialize --select silver/vacancy_rates --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the CMHC average rents for that borough, also independent
uv run dagster asset materialize --select silver/average_rents --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the island-wide street network, date-partitioned only
uv run dagster asset materialize --select bronze/street_network --partition 2026-08-18 -m urban_rag.definitions

# the Montreal construction cost rates, date-partitioned only
uv run dagster asset materialize --select bronze/montreal_residential_costs,bronze/montreal_nonresidential_costs --partition 2026-08-18 -m urban_rag.definitions

# that borough's sides of street, cut out of it
uv run dagster asset materialize --select silver/neighborhood_streets --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# how much street each lot faces — needs building_lot_intersections first, for
# the rag.lots this reads, and hbu_infra's sql/007 + sql/008 applied
uv run dagster asset materialize --select silver/lot_frontage --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# then the corpus over that snapshot's linked PDFs
uv run dagster asset materialize --select "bronze/linked_documents,silver/document_chunks,silver/document_embeddings" --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# and publish those vectors to the Postgres/pgvector store
uv run dagster asset materialize --select gold/document_index --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# the zoning grids read as tables, and the envelope per (lot, grid column)
uv run dagster asset materialize --select "silver/zoning_grid_columns,silver/lot_zoning_envelopes" --partition "2026-08-18|VSMPE" -m urban_rag.definitions

# finally the gold row per lot, which reads the three silver parquet
# partitions above as well as rag.lots — needs hbu_infra's sql/009 + sql/006
uv run dagster asset materialize --select gold/lot_profiles --partition "2026-08-18|VSMPE" -m urban_rag.definitions
```

Schedules run daily in `America/Toronto`: the catalog at 04:00, the features
at 04:20, the reference neighborhoods at 04:40, the vacancy rates at 04:45 and
the average rents at 04:50 (the CMHC assets are independent of the Spectrum
assets — the minutes only keep them from overlapping), then the lots at 05:40,
the buildings at 05:50, the borough's street sides at 06:20 and the
building × lot join at 07:00 behind them. The
island-wide street network is snapshot at 04:50, alongside the other sources
that have no upstream here. `lot_frontage`, the envelope pair and `lot_profiles` have no schedule yet — see
[Assets](#assets). All target *today's*
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
