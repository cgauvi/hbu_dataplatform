"""Read-only: what the RQTT snapshot would cut into VSMPE.

Does exactly what `neighborhood_streets` does - reads bronze, takes the borough
outline, selects then clips - but writes nothing and touches no database, so
the borough-level numbers can be compared against the geobase double's before
any partition is replaced.

The geobase double's VSMPE first snapshot, for reference: 4,262 sides of the
island's 91,546, 445.8 km, 170 cut at the boundary.
"""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import geopandas as gpd  # noqa: E402
import shapely  # noqa: E402

from hbu_dataplatform.open_data_assets import (  # noqa: E402
    STREET_SEGMENTS_FILE,
    borough_boundary,
    street_network,
)
from hbu_dataplatform.partitions import metric_crs_for  # noqa: E402
from hbu_dataplatform.resources import ParquetStore  # noqa: E402
from hbu_dataplatform.rqtt import ROAD_CLASS_FIELD, STREET_ID_FIELD, STREET_NAME_FIELD  # noqa: E402
from hbu_dataplatform.storage import join, output_root, storage_options  # noqa: E402

DATE = "2026-09-01"
NEIGHBORHOOD = "VSMPE"

store = ParquetStore(root_dir=output_root())
path = join(store.partition_dir(street_network.key.path[-1], DATE), STREET_SEGMENTS_FILE)
print(f"reading {path}")
streets = gpd.read_parquet(path, storage_options=storage_options(path))
print(f"snapshot: {len(streets):,} segments, crs {streets.crs}")

boundary = borough_boundary(store, DATE, NEIGHBORHOOD)
metric_crs = metric_crs_for(NEIGHBORHOOD)

touching = streets[streets.intersects(boundary)].copy()
published_m = touching.geometry.to_crs(metric_crs).length
clipped_geom = touching.geometry.intersection(boundary)
keep = clipped_geom.geom_type.isin(["LineString", "MultiLineString", "LinearRing"])
touching = touching[keep]
clipped = touching.set_geometry(
    gpd.GeoSeries(clipped_geom[keep], index=touching.index, crs=touching.crs)
)
inside_m = clipped.geometry.to_crs(metric_crs).length
published_m = published_m.loc[clipped.index]
pct = (100.0 * inside_m / published_m).where(published_m > 0, 0.0)

print()
print(f"=== {NEIGHBORHOOD} {DATE} ===")
print(f"segments inside the borough : {len(clipped):,}   (geobase double: 4,262 sides)")
print(f"total length                : {inside_m.sum() / 1000:.1f} km  (geobase double: 445.8 km)")
print(f"cut at the boundary         : {int((pct < 99.9).sum()):,}   (geobase double: 170)")
print(f"distinct street names       : {clipped[STREET_NAME_FIELD].nunique():,}")
print(f"unnamed segments            : {int(clipped[STREET_NAME_FIELD].isna().sum()):,}")
print(f"duplicate ids               : {int(clipped[STREET_ID_FIELD].duplicated().sum())}")
print(f"invalid geometries          : {int((~clipped.geometry.is_valid).sum())}")
print()
print("road classes in the borough:")
print(clipped[ROAD_CLASS_FIELD].value_counts(dropna=False).to_string())
print()
lanes = clipped[clipped[STREET_NAME_FIELD].fillna("").str.contains("ruelle", case=False)]
print(f"segments named 'Ruelle *'   : {len(lanes)}")
