# Civic addresses

Adresses Québec, the MRNF's official address points for the whole province,
and the spatial join that puts each of them on a parcel — `bronze/
neighborhood_addresses` and `silver/lot_addresses`.

The point of the pair is a join key. Everything this platform answers is keyed
on a lot, or since the grain changed on a *piece* of one, and a lot is a
nine-digit number nobody recognises. An address is what a person asks in and
what a map labels with, so `silver.lot_addresses` is what lets
`gold.lot_investment_opportunities` say **7430 Rue Lajeunesse** instead of
**2 784 705**, and what lets the corpus answer a question phrased as a street
and a number.

## The publisher, and the endpoint that cannot answer

The product is `AQ Adresses` — *"les données officielles sur les adresses et
leur localisation sous forme de points pour tout le Québec"* — published by the
Ministère des Ressources naturelles et des Forêts, restamped on its own
cadence. The stamp is on every row as `Version` (`AQ20260901` at the time of
writing) and is the only thing the service says about its own freshness, so it
is carried through to every table and into the asset's metadata.

It is advertised under a **WMS** endpoint:

```
https://servicescarto.mrnf.gouv.qc.ca/pes/services/Territoire/AQ_ADRESSES_WMS/MapServer/WMSServer
```

WMS cannot be read for this. It is a picture protocol: `GetMap` returns a
rendered PNG, and `GetFeatureInfo` answers "what is under this pixel" one pixel
at a time — against a layer the same service refuses to draw above 1:10,000.
Neither hands back a borough's addresses as data, and no WMS request can be
filtered by a parcel.

The same ArcGIS MapServer publishes a **REST** endpoint beside its WMS
connector, and that one can:

```
https://servicescarto.mrnf.gouv.qc.ca/pes/rest/services/Territoire/AQ_ADRESSES_WMS/MapServer/0
```

Its own metadata declares `capabilities: Map,Query,Data`,
`supportsSpatialFilter: true` and `supportedQueryFormats: JSON, geoJSON, PBF`.
So the WMS URL is recorded in `hbu_dataplatform.adresses_quebec.DEFAULT_WMS_URL` and in
the bronze asset's metadata as the product's front door, and the REST sibling is
what is actually read.

## There is no lot number on an address

This is the fact the whole design turns on. The layer publishes ten fields:

| Field | What it is |
| --- | --- |
| `IdAdr` | A 32-character UUID, stable across editions and unique across Québec. The natural key |
| `AdresseFormatee` | The whole address as one string — the only place the street name, the municipality and the unit exist |
| `NoCivq` | The civic number, as an integer |
| `NoCivqSuf` | Its suffix. Usually a letter; in Old Québec a fraction |
| `NbUnite` | The publisher's count of units at the address |
| `CaractAdr` | A characteristic. Empty on every row of both registered boroughs |
| `Position` | The coordinates again, as text |
| `Version` | The edition stamp |
| `OBJECTID` | The service's own row id, which paging orders on |
| `SHAPE` | The point |

**Nothing cadastral.** No `NoLot`, no matricule, nothing naming a parcel. So
"the address of lot 2 784 705" is not a question this publisher can be asked,
and "query by lot" is only available as a *spatial* query — which is exactly
what the silver asset is.

## The fetch

`bronze/neighborhood_addresses` queries the REST layer with the borough's
outline from `reference_neighborhoods` as a spatial filter, POSTed because a few
hundred vertices of a borough do not survive a query string.

Two details cost a rewrite each:

**The rings are wound the way Esri reads them** — clockwise for an outer ring,
counter-clockwise for a hole, the opposite of GeoJSON's rule. Handing ArcGIS a
GeoJSON-wound polygon does not fail; it is read as a *hole*, and the answer is
every address outside the borough.

**The pages are walked under an explicit `orderByFields`.** The service caps a
response at 1,000 rows and sets `exceededTransferLimit` when it truncates, so
the walk is `resultOffset`-based. ArcGIS promises no order between two unordered
queries, so paging without one returns a sample of the answer with duplicates
in it rather than the answer. The row count is asked for up front with
`returnCountOnly`, so a short walk is a fact the run can report rather than
something to infer from the last page.

An outline past `MAX_FILTER_VERTICES` (1,000) is sent as its bounding box
instead, because past that the request body costs more than the rows it saves.
That only ever *widens* the query: the asset clips to the true outline itself,
so the polygon and the envelope differ in transfer and not in result.

## The parse, and why it is silver's

`AdresseFormatee` is the only place the street lives, and taking a string apart
is interpretation rather than transport — so bronze keeps it whole and
`lot_addresses` splits it, in Python, before the points are loaded. The shape:

```
[<unit>-]<civic>[ <suffix>] <street>[ (<disambiguation>)], <municipality> <postal>
```

```
204-7430 Rue Lajeunesse, Montréal H2R2H8
7390 A Rue De Lanaudière, Montréal H2E1Y4
PH4-5785 Rue Boyer, Montréal H2S2H7
8635 12e Avenue (Montréal), Montréal H1Z3J1
1-27 1/2 Rue Sainte-Angèle, Québec G1R4G5
```

Three things about it are not guessable:

* **The suffix may be spaced or not.** `7390 A Rue De Lanaudière` and
  `336B-8755 Rue Saint-Hubert` are the same field. A parser that requires the
  suffix to be attached reads `A` as the first word of the street.
* **The suffix is not always a letter.** Old Québec numbers houses `27 1/2` and
  `8 1/4`, and the publisher puts the fraction in `NoCivqSuf`. A `[A-Za-z]`
  suffix class parses the whole of Montreal and fails 18 addresses in La
  Cité-Limoilou, every one of them in the old city.
* **The parenthesis is a disambiguator, not part of the street.**
  `12e Avenue (Montréal)` is printed where a street name repeats elsewhere in
  the province; the street is `12e Avenue`.

Over 4,000 records from both registered boroughs the parser reads every one, and
its suffix never disagrees with the `NoCivqSuf` column. Where it does fail it
returns nothing rather than a half-filled row — a partial parse would report a
street name that is really a street and a number, with nothing saying so — and
the asset logs the count.

`civic_number` and `civic_suffix` come from the publisher's own columns rather
than from the parse, because it states those two outright.

## The join

`silver.lot_addresses` places each point twice: on its parcel, and within it on
the zone piece it stands in. The second is what makes it joinable —
`gold.lot_highest_best_use`, `gold.lot_redevelopment_gap`,
`gold.lot_investment_opportunities`, `gold.lot_building_massing` and
`gold.lot_surface_parking` are all keyed on `(lot_uid, feature_id)` since the
piece grain landed. `lot_number` travels beside `lot_uid` for the usual reason:
`lot_uid` is a bigserial `load_lots` mints again on every reload.

**Three bases for a match, and the row says which it used.**

| `match_basis` | |
| --- | --- |
| `within` | The point is inside the parcel. The ordinary case |
| `snapped` | The point is outside every parcel but within `max_snap_m` (2 m) of one, and was given the nearest. `snap_distance_m` records how far it reached |
| *(not written)* | The point is on nobody's parcel. Counted as `num_unmatched` and left out |

The snap exists because the address points and the cadastre are two publishers'
surveys of the same ground: where a point was digitised against a building face
rather than a lot line, it lands just outside the parcel it belongs to. Zero is
defensible and `ADDRESS_SNAP_M=0` is how to ask for it; the rows record their
basis either way, so a borough leaning on the snap is visible rather than
assumed.

A point on nobody's parcel is **not** written, because the grain of the table is
an address *on a lot* and a row with no lot is a null in the one column every
reader joins on. It sits in the right of way, or on ground the cadastre did not
draw. `num_unmatched` is the number to watch: a borough where it is large is one
whose cadastre did not land, not one without addresses.

`piece_basis` is the same idea one level down — `piece` for a point inside one
of its lot's zone pieces, `primary` for one inside the lot but in none of them
(the piece cutoffs drop slivers, so a parcel's pieces need not cover it
entirely), and `none` for a lot no zoning layer governs at all, which takes `-`
for `feature_id` — the same dash `sql/025` writes for a migrated row. A `none`
row joins to no gold row, and that is visible rather than mysterious.

## A row is an addressable unit, not a front door

Roughly half the points in a dense borough carry a unit prefix: `204-7430 Rue
Lajeunesse` and `7430 Rue Lajeunesse` are two rows of this layer. Counting rows
on a parcel counts **units**, and reporting that as buildings would make one
walk-up look like eight.

So both are stated. `num_piece_addresses` counts the rows on a site;
`num_piece_civic_addresses` counts the distinct civic addresses among them
(`7430 Rue Lajeunesse`, suffix included, unit dropped). `num_lot_addresses` is
the parcel's own total, carried so a reader holding one piece does not have to
join back.

`address_rank` orders a site's rows by civic number and then by unit, so
`is_primary_address` marks the lowest-numbered door — the one address a map
labels the parcel with, and the one a RAG answer quotes when it cannot list
forty.

## Running it

```bash
make addresses DATE=2026-09-01 NEIGHBORHOOD=VSMPE
make addresses DATE=2026-09-01 NEIGHBORHOOD=VSMPE ADDRESS_SNAP_M=0
```

Needs `hbu_infra` `sql/026_silver_lot_addresses.sql` applied — it creates both
`rag.addresses` and `silver.lot_addresses` — and `make zone-pieces` run first
for the same partition, since an address is placed on the piece grain and
without the pieces there is nothing to key it to. Until 026 is applied the run
fails naming the file, which is why the silver asset is registered and given a
job but left off the daily schedules, the posture `lot_frontage` and
`lot_zone_pieces` take.

The two halves are separate jobs. The fetch is a few minutes of paging against a
provincial server; re-running only the join after a change to the snap is
`--select silver/lot_addresses`.

## Reading it back

One address per site, which is what a label wants:

```sql
SELECT o.lot_number, o.feature_id, a.formatted_address, o.site_thesis
  FROM gold.lot_investment_opportunities o
  JOIN silver.lot_addresses a
    ON a.lot_uid = o.lot_uid
   AND a.feature_id = o.feature_id
   AND a.neighborhood = o.neighborhood
   AND a.scrape_date = o.scrape_date
   AND a.is_primary_address
 WHERE o.neighborhood = 'VSMPE' AND o.scrape_date = '2026-09-01'
 ORDER BY o.thesis_rank;
```

And the direction a person actually asks in — from an address to what may be
built on it (`lot_addresses_street_idx` is over `lower(street_name)`, so the
lookup does not depend on how the asker capitalised it):

```sql
SELECT a.formatted_address, h.lot_number, h.feature_id, h.npv_cad
  FROM silver.lot_addresses a
  JOIN gold.lot_highest_best_use h
    ON h.lot_uid = a.lot_uid
   AND h.feature_id = a.feature_id
   AND h.neighborhood = a.neighborhood
   AND h.scrape_date = a.scrape_date
 WHERE lower(a.street_name) = lower('Rue Lajeunesse')
   AND a.civic_number = 7430
   AND a.neighborhood = 'VSMPE' AND a.scrape_date = '2026-09-01';
```
