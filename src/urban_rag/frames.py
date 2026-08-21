"""Turn Spectrum GeoJSON responses into (geo)parquet on the local disk."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import shape

from urban_rag.spectrum import STYLE_COLUMN

Frame = pd.DataFrame | gpd.GeoDataFrame

_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def table_slug(table: str) -> str:
    """File-safe name for a table path, minus its namespace.

    ``/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE`` becomes
    ``Reglement_urbanisme__VSP_REG_ZONE``.
    """
    parts = [p for p in table.split("/") if p]
    without_namespace = parts[1:] if len(parts) > 1 else parts
    return _UNSAFE.sub("_", "__".join(without_namespace))


def features_to_frame(
    features: list[dict],
    *,
    extra_columns: dict[str, Any] | None = None,
) -> Frame:
    """GeoJSON features -> GeoDataFrame (EPSG:4326) or plain DataFrame.

    Geometry has already been reprojected server side by ``MI_Transform``, so
    the CRS is asserted rather than converted here.
    """
    records: list[dict] = []
    geometries: list[Any] = []
    for feature in features:
        properties = dict(feature.get("properties") or {})
        properties.pop(STYLE_COLUMN, None)
        records.append(properties)
        raw_geometry = feature.get("geometry")
        geometries.append(shape(raw_geometry) if raw_geometry else None)

    frame = pd.DataFrame.from_records(records)
    for name, value in (extra_columns or {}).items():
        if name not in frame.columns:
            frame[name] = value

    frame = _flatten_nested(frame)

    if any(geometry is not None for geometry in geometries):
        return gpd.GeoDataFrame(
            frame,
            geometry=gpd.GeoSeries(geometries, crs="EPSG:4326"),
            crs="EPSG:4326",
        )
    return frame


def write_frame(frame: Frame, path: Path) -> Path:
    """Write GeoParquet when there is geometry, plain parquet otherwise."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception:
        # A column with mixed python types (Spectrum is loosely typed) will
        # stop Arrow from inferring a schema. Stringify the offenders rather
        # than losing the whole table.
        frame = _stringify_object_columns(frame)
        frame.to_parquet(path, index=False)
    return path


def write_vectors(
    frame: pd.DataFrame,
    vectors: Any,
    path: Path,
    *,
    column: str = "embedding",
) -> Path:
    """Write ``frame`` plus one fixed-size list column of float32 vectors.

    Parquet itself has no fixed-size list type, so the width survives only in
    the ``ARROW:schema`` metadata pyarrow writes alongside: ``pq.read_table``
    gives back ``fixed_size_list<float>[n]``, while DuckDB sees a plain
    ``FLOAT[]`` and needs ``CAST(embedding AS FLOAT[n])`` before vss will index
    it. Written this way regardless, because the alternative loses the width
    everywhere rather than in one reader.
    """
    vectors = np.ascontiguousarray(vectors, dtype=np.float32)
    if vectors.ndim != 2 or len(vectors) != len(frame):
        raise ValueError(
            f"Expected one vector per row, got {vectors.shape} for {len(frame)} rows"
        )

    table = pa.Table.from_pandas(frame, preserve_index=False)
    values = pa.array(vectors.reshape(-1), type=pa.float32())
    table = table.append_column(
        column, pa.FixedSizeListArray.from_arrays(values, vectors.shape[1])
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def count_invalid_geometries(frame: Frame) -> int:
    """Rows whose geometry shapely rejects, e.g. self-intersecting rings."""
    if not isinstance(frame, gpd.GeoDataFrame):
        return 0
    geometry = frame.geometry
    return int((geometry.notna() & ~geometry.is_valid).sum())


def _flatten_nested(frame: pd.DataFrame) -> pd.DataFrame:
    """JSON-encode any dict/list cells so Arrow can type the column."""
    for column in frame.columns:
        if frame[column].dtype != object:
            continue
        if frame[column].map(lambda v: isinstance(v, (dict, list))).any():
            frame[column] = frame[column].map(
                lambda v: json.dumps(v, ensure_ascii=False)
                if isinstance(v, (dict, list))
                else v
            )
    return frame


def _stringify_object_columns(frame: Frame) -> Frame:
    geometry_column = getattr(frame, "geometry", None)
    geometry_name = frame.geometry.name if geometry_column is not None else None
    for column in frame.columns:
        if column == geometry_name or frame[column].dtype != object:
            continue
        frame[column] = frame[column].map(lambda v: None if v is None else str(v))
    return frame
