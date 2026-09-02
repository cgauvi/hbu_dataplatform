# Massing — the proposed building, drawn on the ground

`gold.lot_building_massing` is one rectangle per lot, fitted inside that lot's
setback envelope, in EPSG:4326 and ready to put on a map. It exists because
`urban_rag.program` answers in numbers and numbers are what nobody can
sanity-check: *a 287 m² footprint under five storeys* is not something you can
look at and call wrong.

Written by the `lot_building_massing` asset over `urban_rag.massing`; the table
is hbu_infra's [sql/022](../../hbu_infra/sql/022_gold_lot_building_massing.sql).

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

needs `hbu` and `setbacks` for the same partition. Then open

```
data/gold/lot_building_massing/2026-08-24/VSMPE/lot_building_massing.parquet
```

in QGIS beside `silver/lot_buildable_setbacks` (the envelope each rectangle was
fitted into) and `bronze/neighborhood_lots` (the parcel). Three layers, and the
sanity check is visual: the rectangle inside the envelope inside the lot.

Sort on `footprint_fit_pct` ascending to get the lots where the solver's
footprint does not fit the ground it was costed on — the run's metadata reports
`num_fit_below_90_pct` and `num_fit_below_50_pct` for the same reason.
