# The street network, and what a lot fronts on

Frontage is the measurement a highest-and-best-use question turns on after
area. Two 400 m² lots side by side are not the same development site if one has
30 m on a boulevard and the other 6 m on a lane: the width of the street edge
decides what can be built, how it is entered, and what it is worth. Neither
publisher records it — but one of them draws it, and the drawing is simpler
than it looks.

**In Quebec's renewed cadastre, the street is a lot.** Infolot publishes avenue
Chabot as parcels 3 946 199, 3 946 200 and their neighbours, roughly 13.5 m
wide, exactly as it publishes the houses along it. So a lot's frontage is the
length of the boundary it *shares* with one of those:

```sql
ST_Length(ST_Intersection(ST_Boundary(lot), ST_Boundary(road_lot)))
```

That is the whole measure. Adjacent parcels in this cadastre are topologically
clean, so the intersection is the shared edge exactly — no buffer, no
tolerance, no angle test, and nothing to tune.

The street layer is still needed, and the *géobase double*, not the plain
géobase:

- [donnees.montreal.ca/dataset/geobase-double](https://donnees.montreal.ca/dataset/geobase-double),
  CC BY 4.0, ~91 MB of GeoJSON, 91,546 features.

The plain géobase draws one centre line per segment. The double projects that
onto the curb and sidewalk limits and draws **one line per side of street**.
`COTE_RUE_ID` is the publisher's key for a side and is unique island-wide,
which is what lets `silver.neighborhood_streets` upsert against a real natural
key rather than replace a partition wholesale.

Three assets, and the borough axis appears in the middle one:

```
bronze/street_network        date            the island, as published
silver/neighborhood_streets  date × borough  clipped to one borough
silver/lot_frontage          date × borough  the shared edges, in metres
```

`street_network` is partitioned by date alone because one download serves every
borough. The cut to a borough happens in silver, from a file already on disk,
since re-downloading 91 MB per partition would be work done for nothing. It is
a real clip: a side crossing the borough line is `ST_Intersection`-ed against
the boundary, and what was published survives beside it in
`segment_length_m`, `length_in_borough_m` and `pct_in_borough`.

VSMPE's first snapshot: 4,262 sides of the island's 91,546, 445.8 km, 170 of
them cut at the boundary. Lengths are computed in **EPSG:32188** (NAD83 / MTM
zone 8), not in the 4326 the geometry is stored in — a degree is not a metre.

## What the street layer is for now

Two things, and measuring is not one of them.

**It says which parcels are the roadway.** A géobase double side is drawn along
the roadway, so it runs *inside* the parcel that is the roadway and enters no
other. A lot is a road lot when at least `min_street_m` of street line runs
within it. Over the fixture the separation is total rather than marginal:

| | street line inside the parcel |
| --- | --- |
| the 14 road lots | **105 m – 325 m** each |
| all 150 other parcels | **none** |

So the rule picks out all fourteen with no false positive and no false
negative, and the default of **1 m** is a guard against a side clipping the
corner of an ordinary parcel where the two publishers disagree — not a
threshold anything real sits near. `test_the_measure_does_not_move_with_the_cutoff`
sweeps it from 0.5 m to 100 m and no frontage moves.

**It names the street.** A cadastral parcel has no street name, so each shared
edge is labelled with the nearest géobase side *that runs inside the road lot
the edge came from*. The restriction matters. A corner lot's two edges belong
to two road lots, and the nearest side overall labels both with whichever
street happens to be closer — lot 3 790 549 comes back as Chabot twice. With
the restriction it reads 31.2 m on Jarry and 13.1 m on Chabot, which is what it
is. Naming is all the géobase does to the numbers: a label landing on the wrong
side of a corner costs a name, never a metre.

## Why the assessment roll cannot do the identifying

It is the obvious place to look, and it does not work here. The roll files the
public way under CUBF 45xx — 451 *Autoroute*, 452 *Boulevard*, 453 *Artère
principale*, 454 *Artère secondaire*, 455 *Rue et avenue pour l'accès local*,
456 *Ruelle et passage*, 459 *Autres*.

**Montreal does not enter its roadways on the roll.** Of the fourteen road lots
in the Villeray fixture, *none* appears in `b05v_lot_cadst` at all — not
miscoded, absent. City-wide the roll states 859 CUBF-45 units among 437,192:

| group | | units |
| --- | --- | --- |
| 451 | Autoroute | 55 |
| 452 | Boulevard | 2 |
| 453 | Artère principale | 2 |
| 454 | Artère secondaire | 0 |
| 455 | Rue et avenue pour l'accès local | 152 |
| 456 | Ruelle, passage, piste cyclable, sentiers | 557 |
| 459 | Autres routes et voies publiques | 91 |

The 95,415 CUBF-45 units island-wide are mostly other municipalities, which do
assess their roads. A roll reaching a parcel is a fact about tenure; it is not
a map of the street network. `urban_rag.hbu.road_parcel_lots` says the same
thing from the other side, and still reads the roll — for what the roll is
good for, which is knowing that a parcel it *did* reach is not a development
site.

## Lanes settle themselves

The géobase double draws the public roadway and no *ruelle*, so the borough's
lane parcels — three of them in the fixture, 3.6 to 4.5 m wide — are not road
lots, and a lot backing onto one gets no frontage from it. That is the intended
reading: a lane is access, not street edge. It needs no second rule and no
CUBF 456 exclusion, because the street layer never claimed them in the first
place.

## What this replaced, and why

The measure was a clip against a buffered street *line*, and every part of it
was compensation for a line standing in for a polygon.

- **Too narrow and it reached nothing.** A lot line does not sit on the curb
  line. The median lot in VSMPE sits **4.85 m** from its nearest street side,
  and at the 3 m this defaulted to, **22,545 of the borough's 24,952 lots
  (90 %) got no row at all.**
- **Too wide and it inflated what it did reach.** A lot's two *side* boundaries
  run at the street, so their first `buffer_m` fell inside the buffer and was
  counted — two metres of phantom frontage per metre of buffer, per lot.

Lot 3 790 556 on avenue Chabot — a rectangle with 15.24 m of street edge —
measured like this under the clip:

| `buffer_m` | rows | frontage |
| --- | --- | --- |
| 3 m | **0** | no street at all |
| 4 m | 1 | 16.27 m |
| 8 m | 1 | 24.27 m |
| 12 m | **2** — reaches across Chabot | 32.27 m |

A later version kept the buffer only as a *reach* and measured the boundary
running parallel to the side: the lot boundary chopped into 1 m pieces, each
matched to the nearest side within reach, each kept only if it ran within 45°
of parallel. That was flat in the reach and got this lot right, but it was an
elaborate reconstruction of a shared edge the cadastre already holds — and it
still had a reach deciding *which* lots got measured, which is what the
complaint was about. It also credited lot 3 790 483, an interior parcel served
off a ruelle that touches the street at a single point, with frontage it does
not have.

**A tolerance would make the new measure worse, not safer.** Snapping the two
boundaries together by 5 cm adds a uniform 0.10 m to every lot in the fixture
and invents 0.1 m for that corner-touch parcel. Exact is both simpler and
right.

## `buffer_m` is now written as 0

The column stays, and 0 is the truthful reading of it: "how far from the street
a lot boundary counted as facing it" is nothing at all, because the boundary
has to *be* the road lot's edge. It also dates a partition — rows saying 3.0 or
10.0 were measured the old way and are reporting a different quantity from rows
saying 0.

`min_street_m` replaces it as the one setting, and it decides only which
parcels count as roadway:

```
make frontage DATE=2026-08-18 NEIGHBORHOOD=VSMPE MIN_STREET_M=1.0
```

## Lots that face no street

Every lot in a Montreal borough that is not itself a road is expected to have
street on at least one side. The ones that do not are a finding, not a shrug: a
handful are genuine interior parcels or parcels served off a lane, but a share
that jumps is a street snapshot that stopped short. So the run logs the count,
the share **and a sample of the lot numbers**, and publishes them as asset
metadata:

```
num_road_lots                  14
num_lots_without_frontage       1
pct_lots_without_frontage    0.67
lots_without_frontage     3 790 483
```

**Road lots are out of the denominator**, not counted among the failures — a
street facing no street is the definition, not a finding. It is a warning
rather than a failure: a borough that measures badly is a number to read, not a
partition to refuse.

## Reading the result

One row per (lot, street side) that face each other, ordered longest frontage
first — in SQL, so the parquet answers "which lots have the most street" by
being read from the top.

The cadastre cuts a roadway at every intersection, so a lot running the length
of a block can meet one street through two road-lot parcels. That is one
frontage, and the rows are grouped back to (lot, `cote_rue_id`) — which is also
this table's primary key, so `envelopes` and `lot_profiles`, which pivot on
`cote_rue_id` and `street_name`, read exactly what they read before.

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

A corner lot legitimately has two rows. Nothing is thresholded away at load
time, for the same reason `silver.lot_features` keeps its slivers — the cutoff
belongs to the question.

## What this asset does *not* load

Anything. Both sides are already in Postgres when `lot_frontage` runs:
`rag.lots` because `building_lot_intersections` put it there, and
`silver.neighborhood_streets` because that asset owns its own table. Each of
those is loaded in exactly one place, which is not tidiness but the fix for a
real race — `building_lot_intersections`'s docstring has the long version:
whoever commits second replaces the rows the first just computed against.

So `lot_frontage` depends on both assets, guards on both partitions being
populated, and fails with a message naming the one that came back empty — the
same guard `lot_profiles` carries for the same reason. It used to load the
streets itself, which left `silver.neighborhood_streets` with a writer that was
not the asset it is named for.
