# Massing — the proposed building, drawn on the ground

`gold.lot_building_massing` is one rectangle per lot, fitted inside that lot's
setback envelope, in EPSG:4326 and ready to put on a map. It exists because
`urban_rag.program` answers in numbers and numbers are what nobody can
sanity-check: *a 287 m² footprint under five storeys* is not something you can
look at and call wrong.

Written by the `lot_building_massing` asset over `urban_rag.massing`; the table
is hbu_infra's [sql/022](../../hbu_infra/sql/022_gold_lot_building_massing.sql).

That asset draws a **second** polygon beside it — the surface parking, on the
yard the building leaves, in `gold.lot_surface_parking`. It is a separate shape
and a separate table because a surface stall is not a building; see [The parking
is a second polygon](#the-parking-is-a-second-polygon-and-never-part-of-the-first)
below.

## The check it performs

`solve_program` caps a footprint at the **lesser of two areas** — *Taux
d'implantation au sol* × lot area, and the buildable area
[`lot_buildable_setbacks`](assets.md) leaves after the zone's four margins —
and then stops. An area is not a shape.

A buildable envelope of 200 m² that runs 40 m deep and 5 m wide holds no
rectangle of 200 m² at all. A solver working in areas will spend all 200 of
them anyway, report a footprint, cost a building on it, and be wrong in a way
nothing downstream would catch. So:

| column | what it says |
| --- | --- |
| `footprint_m2` | what the solver costed |
| `placed_footprint_m2` | the largest rectangle that actually fits the margins |
| `footprint_fit_pct` | `100 × placed / solved` — **the column to read** |
| `footprint_shortfall_m2` | the difference, in square metres |

A fit below 100 is this table reporting a shape the answer upstream cannot
take. Those lots are *shrunk* rather than dropped, because dropping them would
hide exactly the parcels worth opening a map on.

One caveat the percentage cannot carry alone: a low fit can also mean the
**ratio list was too short** rather than the parcel too thin. `aspect_ratio` is
what distinguishes them — a lot shrunk at the *last* ratio tried was hitting
the list; one shrunk at an earlier ratio was hitting the parcel.

## How a rectangle is chosen

**The margins are respected by construction.** The rectangle is fitted inside
the buildable polygon `lot_buildable_setbacks` computes — the parcel with the
setbacks already subtracted — so a rectangle contained in it honours them.
Nothing here re-implements a margin rule, and a partition with no setbacks
draws nothing at all rather than a rectangle with the margins ignored, which
would look entirely plausible on a map.

**The parcel's own grain sets the angle.** Candidate angles come from the
buildable polygon's `minimum_rotated_rectangle` — its long edge is the
parcel's axis — and both that and its perpendicular are tried.

**A few aspect ratios, squarest first.** `1.0, 1.5, 2.0, 3.0` by default
(`make massing RATIOS=...`), each at both angles, and the first that fits at
the full footprint wins. Squarest first is the useful order: a square is the
most compact rectangle of a given area, so a lot with room to spare gets one,
and a lot that needs 3:1 is telling you its envelope is long and thin. Past
3:1 a "building" is a wall, which is why the list stops there.

**Placement is searched, then pushed flush.** The centroid is tried first, so a
building with room to spare sits centred in its envelope. A building that needs
the extra room is pushed hard against the envelope's edges and corners — which
is both where a Montreal walk-up actually sits (on its front setback line) and
the only way to find the one position a tight envelope allows. A fixed grid of
centres never lands on it: a 10 m wide arm takes a 10 m building only when
centred at exactly 5 m.

**A split parcel is fitted in its larger half.** Margins that meet in the
middle cut a parcel in two, and a building goes in one of the pieces.

## The parking is a second polygon, and never part of the first

A program can park on the ground: `surface_stalls` stand on the yard the
footprint leaves, costing no storey and no *superficie de plancher* because a
car outdoors is not in a building. That last clause is also why the asphalt is
not in the massing. **A surface stall is not a building** — no floor area, no
storey, no height — so folding it into the rectangle would inflate the very
footprint `footprint_fit_pct` is checking, and a map extruding that rectangle
to `height_m` would raise a solid where there is a parking lot.

So the same asset draws a second shape and publishes it to
`gold.lot_surface_parking`, hbu_infra's
[sql/024](../../hbu_infra/sql/024_gold_lot_surface_parking.sql). One asset, one
parquet with two geometry columns, two tables in one transaction.

**The yard is the parcel, not the envelope.** A setback is a margin a
*building* keeps. A car in a side or rear yard stands exactly where the margin
said no building may go, so the container is the lot boundary less the drawn
building — and `lot_building_massing` reads `rag.lots` for it, because no
downstream table carries a parcel outline.

**Nothing is reserved for reaching it.** A surface stall need not front the
street and usually does not — it is reached from the back lane, or across the
parcel's own front yard — so requiring the asphalt to touch the frontage would
refuse the ordinary case. Nothing here proves a car can *get to* the stall it
can stand on. That is a stated assumption, not a forgotten check.

**A stall is 5.5 m long, and that is the whole point.** The stalls are already
bounded upstream by `surface_stall_area × stalls + footprint <= lot area`,
which is an area against an area and is satisfied on a parcel two metres wide.
So a bay here is fitted **depth first** — at least `MIN_PARKING_DEPTH_M`, and
at least `MIN_PARKING_WIDTH_M` across — and the width follows from the area.
Note which dimension goes with which: a four-metre strip is a *driveway*, and
it parks cars in single file parallel to its own length. Only a parcel that
takes a car in neither orientation is unparkable.

| column | what it says |
| --- | --- |
| `surface_parking_area_m2` | the yard the solver reserved |
| `placed_surface_parking_m2` | the asphalt that actually fits |
| `surface_parking_fit_pct` | `100 × placed / reserved` — **the column to read** |
| `placed_surface_stalls` | how many cars that ground really holds |
| `num_parking_bays` | 1, or a front yard and a rear one |

`parking_status` is `fitted`, `shrunk`, `no_fit`, `no_yard`, `no_lot_geometry`,
`no_parking` or `no_program`. Only the first two reach Postgres — a lot that
parks underground has no polygon, and the warehouse skips a shapeless row.

**Up to three patches, unlike the building.** A building is one massing or it
is nothing; parking honestly comes in pieces, and a building across the middle
of its parcel leaves a front yard and a rear yard with stalls in both. The
search places the biggest bay, cuts it out, and looks again. Greedy, so it can
under-state a yard and can never claim ground that is not there.

## And the solver is stopped before it gets here

`Lot.parkable_area_m2` is a **real constraint on the solve**, not a report
about it: the largest parking-shaped rectangle a parcel holds, measured off
`rag.lots` by `massing.parking_capacity_m2` and handed to `solve_program`
beside the area bound. A parcel measuring 0 parks nothing on the ground and its
program must dig, deck, bay it into the ground floor, or be smaller —
`binding` says `surface_parking_shape` on exactly those rows, because no
printed norm will.

That cap errs both ways and both are stated in `parking_capacity_m2`: it
ignores the building, which makes it generous, and it measures one rectangle
where `fit_parking` allows three, which makes it strict on a two-lobed parcel.
`surface_parking_fit_pct` is the exact question, asked once a real building is
standing.

## What it is not

A schematic, not a design. Real buildings are L-shaped, step back above a
podium, and put parking under a footprint wider than the tower above it. A
rectangle of the right area in the right place answers *does it fit, does it
look like the block around it, is this absurd* — and stopping there is what
keeps it honest.

`floors`, `height_m` and the storey split ride along so a map can extrude the
rectangle without a join back to `gold.lot_highest_best_use`.

## Every lot keeps a row in the tree; only the drawn ones reach Postgres

`massing_status` is one of:

| status | meaning |
| --- | --- |
| `fitted` | a rectangle of the full solved footprint fits |
| `shrunk` | none does; the largest that fits is drawn — see above |
| `no_fit` | the envelope holds nothing of `min_footprint_m2` |
| `no_buildable_geometry` | no setback envelope for the governing (lot, zone, column) |
| `no_program` | `lot_highest_best_use` has no program for this lot |

`parking_status` is the same idea for the asphalt and a separate column,
because a lot can perfectly well have a building that fits and parking that
does not — one status cannot say both.

The last three have no polygon, and `urban_rag.warehouse` skips a row with no
geometry on the way into a spatial table — so they are in the tree and not in
`gold.lot_building_massing`. Nothing is lost by it: a reader who wants them in
SQL anti-joins `gold.lot_highest_best_use`, which has every lot and an
`hbu_status` saying why it has no program. The run reports `num_lots` and
`num_drawn` side by side so the gap is never a surprise.

Nothing is drawn on a street or on a park, and this asset does not decide that:
`lot_highest_best_use` gives a `road_parcel` and an `equipment_zone` lot no
program, so both arrive here as `no_program` and never reach a rectangle. The
gate is one asset upstream on purpose — a parcel nobody may build on should be
excluded once, where the reason can be stated, rather than in each of the four
tables that read the answer.

## Looking at it

```bash
make massing DATE=2026-08-24 NEIGHBORHOOD=VSMPE
```

needs `hbu` and `setbacks` for the same partition, and `rag.lots` loaded for
the borough — the parcel outlines the parking is fitted onto are read from
Postgres, and without them every row is `no_lot_geometry` and the buildings are
drawn regardless. Then open

```
data/gold/lot_building_massing/2026-08-24/VSMPE/lot_building_massing.parquet
```

in QGIS beside `silver/lot_buildable_setbacks` (the envelope each rectangle was
fitted into) and `bronze/neighborhood_lots` (the parcel). Three layers, and the
sanity check is visual: the rectangle inside the envelope inside the lot.

One file, **two** geometry columns: `geometry` is the building and
`parking_geometry` is the asphalt. QGIS asks which to use when it opens the
file, so open it twice to see both — and the second sanity check is that they
never overlap, and that the parking is inside the lot without being inside the
envelope.

Sort on `footprint_fit_pct` ascending to get the lots where the solver's
footprint does not fit the ground it was costed on — the run's metadata reports
`num_fit_below_90_pct` and `num_fit_below_50_pct` for the same reason. Sort on
`surface_parking_fit_pct` for the same question about the yard, with
`num_parking_fit_below_90_pct` and `num_parking_fit_below_50_pct` beside it.
