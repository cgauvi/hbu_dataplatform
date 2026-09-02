# The frontage fixture

One block of Villeray, carved out of the real scrape so `tests/integration`
can measure against the publisher's own geometry rather than a rectangle
someone drew.

| file | rows | from |
| --- | --- | --- |
| `lots.parquet` | 164 | `data/bronze/neighborhood_lots/<date>/VSMPE/lots.parquet` |
| `street_sides.parquet` | 102 | `data/silver/neighborhood_streets/<date>/VSMPE/neighborhood_streets.parquet` |

The window is every lot within **120 m of lot 3 790 556** — a 476.1 m² parcel
on avenue Chabot whose front boundary sits 3.48 m behind cote_rue_id
11000531 — plus every street side those lots could reach, with 60 m to spare so
no lot's nearest side is one the window cut off.

It is chosen to be the regression case, not a happy path. The 164 lots split
**14 road lots and 150 ordinary parcels**: Infolot draws avenue Chabot, rue
Bordeaux and rue Jarry as parcels of their own — the `3 946 xxx` family, some
13.5 m wide — and lot 3 790 556's frontage is the 15.24 m of boundary it shares
with 3 946 200.

- The road lots carry **105 m to 325 m** of geobase street line each and every
  other parcel carries **none**, so identifying them has two orders of
  magnitude of headroom. `test_the_measure_does_not_move_with_the_cutoff`
  sweeps `min_street_m` from 0.5 to 100 and the frontage does not move.
- **149 of the 150** non-road parcels share boundary with a road lot. The one
  that does not — 3 790 483 — is served off a ruelle and touches the street at
  a single point, and is asserted by name so a second one appearing is a
  failure rather than a rounding of the count.
- Under the buffer measure this replaced, the same slice matched 14 lots at
  `buffer_m = 3.0` and all 164 at 10.0 — including the road lots and the
  lane-served parcel, which is what a buffer wide enough to reach the lots also
  buys.

## Regenerating

Only when the columns change — the geometry deliberately does not track the
latest scrape, because the expected values in `test_lot_frontage.py` are
pinned to this one. Columns are renamed to the shape the two tables use
(`NO_LOT` → `lot_number`, `COTE_RUE_ID` → `cote_rue_id`,
`length_in_borough_m` → `length_m`) so the loader in `conftest.py` is a plain
insert; geometry stays in EPSG:4326.
