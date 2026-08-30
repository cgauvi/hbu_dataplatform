# The street network, and what a lot fronts on

Frontage is the measurement a highest-and-best-use question turns on after
area. Two 400 m² lots side by side are not the same development site if one has
30 m on a boulevard and the other 6 m on a lane: the width of the street edge
decides what can be built, how it is entered, and what it is worth. Neither
publisher records it. Infolot draws the parcel; Montreal draws the roadway; the
relation between them is geometry — the same situation `silver.lot_features` is
in, and it is answered the same way.

The street layer is the *géobase double*, not the plain géobase:

- [donnees.montreal.ca/dataset/geobase-double](https://donnees.montreal.ca/dataset/geobase-double),
  CC BY 4.0, ~91 MB of GeoJSON, 91,546 features.

The plain géobase draws one centre line per segment. The double projects that
onto the curb and sidewalk limits and draws **one line per side of street**,
which is what a frontage question needs: a lot faces one side of a street, not
the axis of the roadway. `COTE_RUE_ID` is the publisher's key for a side and is
unique island-wide, which is what lets `silver.neighborhood_streets` upsert against a real
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

## The measure is taken on the lot's boundary

The obvious statement of the question does not work:

```sql
-- Reports 0 for every row.
SELECT ST_Length(ST_Intersection(lot.geom, street_buffer.geom))
```

That intersects two polygons, gets a polygon, and **`ST_Length` of an areal
geometry is 0 in PostGIS** — `ST_Perimeter` is the function for polygons.
Frontage is a length along the parcel's *edge*, so the left-hand side has to be
the boundary:

## It is not a clip against a buffered street

It was, and that measure could not be made to work at any setting. The street
side was buffered by `buffer_m` and the lot boundary clipped to the result,
which fails from both ends at once:

- **Too narrow and it reaches nothing.** A lot line does not sit on the curb
  line — the géobase double is drawn along the roadway, published *à titre
  indicatif*, and the lot is behind a sidewalk and a service strip. The median
  lot in VSMPE sits **4.85 m** from its nearest street side. At the 3 m this
  defaulted to, **22,545 of the borough's 24,952 lots (90 %) got no row at
  all.**
- **Too wide and it inflates what it does reach.** A lot's two *side*
  boundaries run at the street, so their first `buffer_m` falls inside the
  buffer too and is counted. Every lot gains 2 m of frontage it does not have
  per metre of buffer, and past about 12 m the buffer reaches the far kerb and
  a mid-block lot acquires a second street.

Lot 3 790 556 on avenue Chabot — a rectangle with 15.24 m of street edge —
measured like this under the clip:

| `buffer_m` | rows | frontage |
| --- | --- | --- |
| 3 m | **0** | no street at all |
| 4 m | 1 | 16.27 m |
| 8 m | 1 | 24.27 m |
| 12 m | **2** — reaches across Chabot | 32.27 m |

What is measured instead is the lot boundary that runs *along* a street side.
The boundary is chopped into 1 m pieces; each piece is matched to the single
nearest side within `buffer_m`; and a piece counts only if it runs within 45°
of parallel to that side. The parallel test needs no trigonometry — for a piece
of length `L` whose ends sit `d1` and `d2` from the side, `|d1 - d2| / L` *is*
the sine of the angle between them, 0 along the street and 1 straight at it:

```sql
-- per 1 m piece of ST_Segmentize(ST_Boundary(lot), 1.0), in EPSG:32188
CROSS JOIN LATERAL (                    -- nearest side wins, and only it:
    SELECT cote_rue_id, geom            -- the two sides of one roadway are
    FROM sides                          -- 5-8 m apart, well inside the reach
    WHERE ST_DWithin(sides.geom, piece.geom, buffer_m)
    ORDER BY sides.geom <-> piece.geom
    LIMIT 1
) AS near
WHERE abs(ST_Distance(ST_StartPoint(piece.geom), near.geom)
        - ST_Distance(ST_EndPoint(piece.geom), near.geom))
      / ST_Length(piece.geom) <= 0.7071
```

Measured in **EPSG:32188** rather than through `geography`, for the same reason
street lengths are: metres in MTM zone 8 are metres on the ground. It also
gives the assignment an index to walk — `<->` against a GiST index turns each
of the borough's 2.6 million pieces into a probe instead of a scan, which is
the difference between a minute and not finishing.

## What `buffer_m` now means

It is a **reach**, not a buffer: how far a lot boundary may be from a street
side and still be matched to it. It decides *which lots get measured*, and no
longer what they measure — lot 3 790 556 comes out at 15.24 m at a reach of 6,
8, 10 and 12 m alike.

That is what lets it be wide enough to be useful. The default is **10 m**,
where coverage plateaus:

| reach | lots with no frontage | of 24,952 |
| --- | --- | --- |
| 3 m | 22,545 | 90.4 % |
| 6 m | 5,121 | 20.5 % |
| 8 m | 1,316 | 5.3 % |
| **10 m** | **698** | **2.8 %** |
| 12 m | 673 | 2.7 % |

Measured by running the asset over VSMPE 2026-08-26. What is left at 10 m is
the right residual rather than a shortfall: those 698 lots have a **median
area of 56 m²** and sit a **median 26.5 m** from the nearest street side —
interior remnants deep inside blocks, which is what a lot with no frontage is
supposed to be.

It is `FrontageConfig` rather than a constant, the same way `lot_profiles` makes
its shed cutoff config: a judgement about the street section, not a property of
the data. Every row carries the `buffer_m` it was computed with, so a table can
always be read back against its own cutoff — and a table whose rows say `3.0`
was measured the old way and is missing most of its borough.

```
make frontage DATE=2026-08-18 NEIGHBORHOOD=VSMPE BUFFER_M=12
```

## Lots that face no street

Every lot in a Montreal borough is expected to have street on at least one
side. The ones that do not are a finding, not a shrug: a handful are genuine
interior parcels, but a share that jumps is a street snapshot that stopped
short or a reach that is too tight — which is exactly what the 3 m default
looked like. So the run logs the count, the share **and a sample of the lot
numbers**, and publishes all three as asset metadata:

```
num_lots_without_frontage     687
pct_lots_without_frontage     2.75
lots_without_frontage         1 000 140, 1 000 141, 1 000 215, ...
```

It is a warning rather than a failure — a borough that measures badly is a
number to read, not a partition to refuse — and `tests/integration` asserts the
stronger form on a slice where every lot does face a street.

## Reading the result

One row per (lot, street side) that face each other, ordered longest frontage
first — in SQL, so the parquet answers "which lots have the most street" by
being read from the top.

`frontage_rank` is 1 for a lot's longest frontage and is the column to filter
on when a question wants *the* street a lot fronts on:

```sql
SELECT lot_number, street_name, round(frontage_m::numeric, 1) AS frontage_m
FROM silver.lot_frontage
WHERE neighborhood = 'VSMPE' AND scrape_date = '2026-08-18'
  AND frontage_rank = 1
ORDER BY frontage_m DESC
LIMIT 20;
```

A corner lot legitimately has two rows; a lot clipping a side street by 40 cm
has a second one that is a survey artifact. Nothing is thresholded away at load
time, for the same reason `silver.lot_features` keeps its slivers — the cutoff
belongs to the question. `num_lots_without_frontage` in the run's metadata is
the number to watch: a few percent are true interior parcels, a third of the
borough is a street snapshot that stopped short.

## What this asset does *not* load

Anything. Both sides of the join are already in Postgres when `lot_frontage`
runs: `rag.lots` because `building_lot_intersections` put it there, and
`silver.neighborhood_streets` because that asset owns its own table. Each of
those is loaded in exactly one place, which is not tidiness but the fix for a
real race — `building_lot_intersections`'s docstring has the long version:
whoever commits second replaces the rows the first just computed against.

So `lot_frontage` depends on both assets, guards on both partitions being
populated, and fails with a message naming the one that came back empty — the
same guard `lot_profiles` carries for the same reason. It used to load the
streets itself, which left `silver.neighborhood_streets` with a writer that was
not the asset it is named for.

