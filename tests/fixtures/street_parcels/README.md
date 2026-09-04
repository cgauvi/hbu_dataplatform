# The street-parcel fixture

Avenue Querbes between Ball and Saint-Roch, in Parc-Extension, carved out of
the real scrape. The sibling of [`../frontage`](../frontage/README.md) and a
different question: that window is about *measuring* a frontage, this one is
about *identifying* the parcels that are the street.

| file | rows | from |
| --- | --- | --- |
| `lots.parquet` | 114 | `data/bronze/neighborhood_lots/<date>/VSMPE/lots.parquet` |
| `street_sides.parquet` | 39 | `data/silver/neighborhood_streets/<date>/VSMPE/neighborhood_streets.parquet` |

The window is every lot within **30 m** of lot 2 249 179 or lot 2 249 339 —
the two parcels that *are* avenue Querbes over that stretch, 3 319 m² and
3 298 m², each about 9 m wide and a block long — plus every street side within
90 m of them, taken whole so no side is clipped.

## Why these two lots

They are the failure the road gate was widened for. Neither appears on the
assessment roll — Montreal does not enter its own roadways on it — so
`hbu.road_parcel_lots`, which reads the roll's CUBF 45xx codes, cannot see
them. Under the zoning of the blocks either side, `lot_development_programs`
solved a building on each, `lot_highest_best_use` chose it,
`lot_redevelopment_gap` called both under-built and
`lot_investment_opportunities` ranked them. `hbu.cadastral_road_lots` is what
now stops that, off `lot_frontage`'s road lots.

## The separation

The same two orders of magnitude the Chabot fixture has, on a busier slice:

- **14 of the 114 lots** are the roadway. They carry **126 m to 606 m** of
  geobase double street line each — Querbes, Ball, Saint-Roch, Durocher,
  De L'Épée and Bloomfield.
- The other **100** carry at most **0.32 m**, all of it at corners where the
  two publishers disagree by a few centimetres. Lots 2 249 342 and 2 249 343
  are the extreme case and are worth knowing about: 2.9 m² slivers at an
  intersection, clipped by a side and correctly *not* road lots.
- So `DEFAULT_ROAD_LOT_MIN_STREET_M` at 1.0 sits in an empty band.
  `test_the_identification_does_not_turn_on_the_cutoff` sweeps it from 0.5 to
  50 and the same 14 parcels come back.

The band ends at 50 rather than at 606 because **the cutoff is compared per
(parcel, side), not per parcel**: a strip holding 606 m across six sides
survives only while the cutoff is under its *longest single* side. The
shortest of those here is 57.6 m, on lot 2 590 348, against 0.28 m for the
longest side any ordinary parcel carries — still a factor of two hundred, and
`test_a_cutoff_above_a_strips_longest_side_loses_it` pins the far edge so the
shape of the rule is not something a reader has to rediscover.

## Regenerating

Only when the columns change — the expected values in
`tests/integration/test_street_parcels.py` are pinned to this geometry, not to
the latest scrape. Columns are renamed to the shape the two tables use
(`NO_LOT` → `lot_number`, `VA_SUPRF_LOT_CALCL` → `area_m2`, `COTE_RUE_ID` →
`cote_rue_id`, `NOM_VOIE` → `street_name`, `length_in_borough_m` → `length_m`)
so the loader in `conftest.py` is a plain insert; geometry stays in EPSG:4326.
