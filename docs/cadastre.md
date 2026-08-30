# The cadastre

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

