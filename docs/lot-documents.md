# From a lot to its documents

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
loads its lots into `rag.lots`, and intersects the two into `silver.lot_features`,
one row per (lot, feature) pair holding the clipped slice and `pct_of_lot`, the
share of the lot that feature covers.

## One asset, three upstreams, two joins

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

## Where the cadastre is repaired

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

## Computed in PostGIS, kept as geoparquet

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
rag.lots  ──ST_Intersection──▶  silver.lot_features  ──feature_id──▶  rag.chunks
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
join back out of `silver.lot_features` instead of recomputing it per query.

## Two things the geometry makes true

**A lot can have more than one zone, and usually one of them is noise.** A
cadastral boundary and a zoning boundary are drawn by different offices from
different surveys, so they miss each other by centimetres all along a street
and every lot picks up a sliver of its neighbour's zone. Nothing is thresholded
away at load time — the same posture `silver.building_lot_intersections` takes toward the
corner of a triplex crossing a lot line — because the cutoff belongs to the
question, not to the geometry. `pct_of_lot` is the column to filter on, and
`coverage_rank = 1` takes the zone that actually governs the lot. A lot
genuinely split between two zones has two real rows, and no ranking will tell
you which of those is the artifact.

The questions this platform asks answer at one square metre, and
`overlap_area_m2` on `rag.lot_documents` is what lets them. It is deliberately
a different shape of cutoff from `pct_of_lot`: how much of a lot a zone should
govern before it counts is a judgement about the borough, and a percentage
states it, but whether a zone reaches the lot at all is not — below a square
metre the two surveys have simply missed each other, on a 200 m² duplex parcel
and on Parc Jarry alike. `postgis.MIN_ZONE_OVERLAP_M2` is that value here,
`EnvelopeConfig.min_overlap_m2` and `compute_lot_profiles` read it, and
`rag.search_at_lot_number(..., min_overlap_m2 => 1)` is its default in the
corpus search. Pass 0 to any of them to get every overlap back.

**Layers are loaded under the file slug, not the Spectrum path.** A feature
parquet carries `source_table = /19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE`,
but `linked_documents` writes the slug `Reglement_urbanisme__VSP_REG_ZONE` into
`rag.chunks.source_table`, and every join from geometry to corpus matches those
two columns to each other. `load_features` takes the slug for that reason;
loading a layer under the path would fail nothing and match nothing.

The slug also drops the borough namespace, and zone numbers restart at
`C01-001` in every borough — so the borough is part of the match too, not just
of the partition filter, both in `silver.lot_features` and in
`rag.chunk_features`.

