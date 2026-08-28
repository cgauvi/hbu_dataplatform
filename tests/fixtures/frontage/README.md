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

It is chosen to be the regression case, not a happy path:

- at `buffer_m = 3.0`, the old default, **14 of the 164 lots** match a street
  and the subject lot matches nothing;
- at `buffer_m = 10.0`, the current default, **all 164** do.

Both are asserted, so lowering the default back under the borough's setback
fails a test that says why rather than quietly emptying the table.

## Regenerating

Only when the columns change — the geometry deliberately does not track the
latest scrape, because the expected values in `test_lot_frontage.py` are
pinned to this one. Columns are renamed to the shape the two tables use
(`NO_LOT` → `lot_number`, `COTE_RUE_ID` → `cote_rue_id`,
`length_in_borough_m` → `length_m`) so the loader in `conftest.py` is a plain
insert; geometry stays in EPSG:4326.
