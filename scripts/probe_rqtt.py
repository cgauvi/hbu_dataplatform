"""Step-zero probe of the RQTT road layer, over the Montreal island bbox.

Answers the three questions the plan is sized off, and nothing else:

  1. What `ClsRte` values are there, and do any of them mean a back lane?
     `postgis.DEFAULT_ROAD_LOT_MIN_STREET_M` excludes Montreal's ruelles today
     only because the geobase double does not draw them. If RQTT does, every
     lot backing onto a lane starts fronting one.
  2. Is `IdRte` unique? It is what silver declares its grain on.
  3. How many segments is an island's worth, against the geobase double's
     91,546 street *sides* (so ~45,800 centre lines if the coverage matches)?

Run with the dagster-check venv from hbu_dataplatform/.
"""

from __future__ import annotations

import pandas as pd

from hbu_dataplatform.rqtt import (
    ROAD_CLASS_FIELD,
    ROAD_LAYER,
    STREET_ID_FIELD,
    STREET_NAME_FIELD,
    RqttFetcher,
    bbox_in_source_crs,
    layer_names,
    read_layer,
)

pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 200)

# The island of Montreal, plus a little water. Hard-coded here only because
# this probe runs before `reference_neighborhoods` is materialized; the asset
# itself takes the box from the published outline (`city_bounds`).
MONTREAL_BBOX = (-73.99, 45.39, -73.46, 45.72)

fetcher = RqttFetcher(cache_dir="data/cache/rqtt", request_delay_seconds=0)
gpkg, version = fetcher.geopackage("20260703")
print(f"geopackage: {gpkg} ({gpkg.stat().st_size / 1e9:.2f} GB), vintage {version}")
print(f"layers: {', '.join(layer_names(gpkg))}\n")

roads = read_layer(gpkg, ROAD_LAYER, bbox=bbox_in_source_crs(MONTREAL_BBOX))
print(f"=== {len(roads):,} segments in the Montreal bbox ===")
print(f"(geobase double publishes 91,546 street SIDES island-wide "
      f"= ~45,773 centre lines)\n")

print(f"=== {ROAD_CLASS_FIELD} values ===")
print(roads[ROAD_CLASS_FIELD].value_counts(dropna=False).to_string())

print(f"\n=== CaractRte values ===")
print(roads["CaractRte"].value_counts(dropna=False).head(30).to_string())

print(f"\n=== {STREET_ID_FIELD} uniqueness ===")
print(f"rows {len(roads):,}, distinct {roads[STREET_ID_FIELD].nunique():,}, "
      f"nulls {int(roads[STREET_ID_FIELD].isna().sum())}")
print(f"AQRP_UUID distinct: {roads['AQRP_UUID'].nunique():,}")

print(f"\n=== {STREET_NAME_FIELD} ===")
print(f"named {int(roads[STREET_NAME_FIELD].notna().sum()):,}, "
      f"unnamed {int(roads[STREET_NAME_FIELD].isna().sum()):,}, "
      f"distinct names {roads[STREET_NAME_FIELD].nunique():,}")

lane_like = roads[
    roads[STREET_NAME_FIELD].fillna("").str.contains("ruelle", case=False)
]
print(f"\n=== names containing 'ruelle': {len(lane_like):,} ===")
if len(lane_like):
    print(lane_like[[STREET_NAME_FIELD, ROAD_CLASS_FIELD]].head(15).to_string())
    print("\nclass breakdown of those:")
    print(lane_like[ROAD_CLASS_FIELD].value_counts().to_string())

print(f"\n=== geometry types ===")
print(roads.geom_type.value_counts().to_string())

print(f"\n=== Gestion (top 15) ===")
print(roads["Gestion"].value_counts(dropna=False).head(15).to_string())
