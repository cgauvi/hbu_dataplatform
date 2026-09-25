"""Rendering a layer's tiles in PostGIS, one band of tiles per statement.

The SQL here is hbu_rag_map's ``mvt_tile`` and ``mvt_aggregate_tile`` written
for a *range* of tiles rather than for one: a ``generate_series`` over the
tile columns and rows of a band, and the same ``ST_AsMVTGeom`` clip inside a
``LATERAL`` per tile. Three details carry over unchanged and are worth
knowing:

* the envelope is built twice and the two are not interchangeable - the 3857
  one is what ``ST_AsMVTGeom`` measures against, and a 4326 copy is what the
  ``&&`` tests, because every GiST index here is on the 4326 column;
* there is no ``ST_Intersects``: the ``&&`` is the index's prefilter and the
  exact test is the clip itself, which comes back NULL for a shape whose
  envelope overlaps the tile while its geometry does not;
* there is no simplification: quantising onto the extent grid *is* the
  simplification, and unlike a tolerance in degrees it cannot produce an
  invalid ring. The one exception is an outline tile below
  `map_tiles.AGGREGATE_OUTLINE_ZOOM`, thinned *before* projection so a
  borough-wide dissolved union is not carried through the transform vertex by
  vertex to draw a shape a dozen pixels wide.

Nothing here opens a connection: every function takes a cursor, so the asset
decides the transaction and the tests hand in a fake.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import Any

from hbu_dataplatform.map import map_tiles
from hbu_dataplatform.core import tile_grid

logger = logging.getLogger(__name__)

#: A rendered band: the tile column, the tile row, and the MVT bytes.
Tile = tuple[int, int, bytes]

#: Postgres type names that become a TileJSON ``Number`` field. Everything
#: else but ``bool`` is a ``String`` - text, uuid, jsonb-as-text, dates.
_NUMBER_TYPES = frozenset(
    {"int2", "int4", "int8", "float4", "float8", "numeric", "oid"}
)

#: The aggregate source, screened to one layer at one cell level within the
#: partition - the read `map_cell_aggregates_layer_level_idx` exists for.
_AGGREGATE_SOURCE = "gold.map_cell_aggregates a"
_AGGREGATE_WHERE = """a.layer = %(layer)s
               AND a.cell_z = %(cell_z)s
               AND a.neighborhood = %(neighborhood)s
               AND a.scrape_date = %(scrape_date)s::date"""


def band_statement(spec: map_tiles.TileLayerSpec, zoom: int) -> str:
    """The statement that renders every tile of ``spec`` at ``zoom`` in a band.

    Which of the layer's two shapes answers is decided by the zoom alone -
    `map_tiles.serves_aggregate` and `serves_outline` - and the three shapes
    differ in exactly four things: what is selected, from where, which column
    the index test runs on, and what is projected. Everything downstream of
    those - the envelope pair, the clip, the quantisation, the NULL screen -
    is the same tile.
    """
    if map_tiles.serves_aggregate(spec.name, zoom):
        outline = map_tiles.serves_outline(zoom)
        columns = map_tiles.OUTLINE_COLUMNS if outline else map_tiles.AGGREGATE_COLUMNS
        source = _AGGREGATE_SOURCE
        where = _AGGREGATE_WHERE
        index_geom = "a.geom"
        geom = (
            "ST_SimplifyPreserveTopology(a.geom, %(tolerance)s)" if outline else "a.geom"
        )
    else:
        columns = spec.columns
        source = spec.source
        where = spec.where
        index_geom = spec.bbox_geom
        geom = spec.geom

    # The `::integer` casts are load-bearing. psycopg 3 binds a small Python
    # int as `smallint`, and `generate_series(smallint, smallint)` is
    # ambiguous - Postgres refuses to choose between the integer and bigint
    # forms - so the range parameters have to say what they are. The other
    # integer parameters are cast for the same reason before it bites there.
    return f"""
        WITH tiles AS (
            SELECT x, y,
                   ST_TileEnvelope(%(z)s::integer, x, y) AS mercator,
                   ST_Transform(
                       ST_TileEnvelope(%(z)s::integer, x, y, margin => %(margin)s::float8),
                       4326
                   ) AS lonlat
              FROM generate_series(%(x0)s::integer, %(x1)s::integer) AS x,
                   generate_series(%(y0)s::integer, %(y1)s::integer) AS y
        )
        SELECT t.x, t.y, tile.body
          FROM tiles t
          CROSS JOIN LATERAL (
              SELECT ST_AsMVT(f, %(layer)s, %(extent)s::integer, 'geom') AS body
                FROM (
                    SELECT {columns},
                           ST_AsMVTGeom(
                               ST_Transform({geom}, 3857),
                               t.mercator,
                               %(extent)s::integer,
                               %(buffer)s::integer,
                               true
                           ) AS geom
                      FROM {source}
                     WHERE {index_geom} && t.lonlat
                       AND {where}
                ) f
               WHERE f.geom IS NOT NULL
          ) tile
         WHERE tile.body IS NOT NULL
        """


def zoom_params(
    spec: map_tiles.TileLayerSpec, zoom: int, params: dict[str, Any]
) -> dict[str, Any]:
    """``params`` plus what one zoom of ``spec`` binds beyond them."""
    return {
        **params,
        "z": zoom,
        "layer": spec.name,
        "cell_z": map_tiles.aggregate_cell_zoom(zoom),
        "tolerance": map_tiles.outline_tolerance_deg(zoom),
    }


def _extent(
    cursor: Any, sql: str, params: dict[str, Any]
) -> tuple[float, float, float, float] | None:
    cursor.execute(
        f"""
        SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e)
          FROM (SELECT ({sql}) AS e) box
        """,
        params,
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    return tuple(float(value) for value in row)  # type: ignore[return-value]


def layer_extent(
    cursor: Any, spec: map_tiles.TileLayerSpec, params: dict[str, Any]
) -> tuple[float, float, float, float] | None:
    """``(west, south, east, north)`` of everything ``spec``'s archive draws, or None.

    The union of the layer's own extent and, where it has one, its cells' at
    the base level - the two cover the same ground by construction, and the
    union is cheaper to take than to prove. None means the layer holds
    nothing for this partition and no archive should be written.
    """
    boxes = []
    detail = _extent(
        cursor,
        f"SELECT ST_Extent({spec.bbox_geom}) FROM {spec.source} WHERE {spec.where}",
        params,
    )
    if detail:
        boxes.append(detail)
    if spec.has_aggregate:
        cells = _extent(
            cursor,
            f"SELECT ST_Extent(a.geom) FROM {_AGGREGATE_SOURCE} WHERE {_AGGREGATE_WHERE}",
            {**params, "layer": spec.name, "cell_z": tile_grid.BASE_CELL_ZOOM},
        )
        if cells:
            boxes.append(cells)
    if not boxes:
        return None
    return (
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    )


def _field_type(type_code: Any) -> str:
    """A TileJSON field type for a Postgres type OID."""
    try:
        from psycopg.postgres import types  # noqa: PLC0415

        info = types.get(type_code)
        name = info.name if info is not None else None
    except Exception:  # noqa: BLE001 - a registry lookup is not worth a failed run
        name = None
    if name == "bool":
        return "Boolean"
    if name in _NUMBER_TYPES:
        return "Number"
    return "String"


def _described_fields(
    cursor: Any, columns: str, source: str, where: str, params: dict[str, Any]
) -> dict[str, str]:
    cursor.execute(
        f"SELECT {columns} FROM {source} WHERE {where} LIMIT 0",
        params,
    )
    return {
        column.name: _field_type(column.type_code)
        for column in (cursor.description or [])
    }


def tile_fields(
    cursor: Any, spec: map_tiles.TileLayerSpec, params: dict[str, Any]
) -> dict[str, str]:
    """The properties ``spec``'s tiles carry, as TileJSON ``fields``.

    Read off the statement's own description with ``LIMIT 0`` rather than
    written down twice, so a column added to a spec reaches the archive's
    metadata without a second list to keep in step. A layer with cells behind
    it reports the cells' columns as well: one archive, one field list.
    """
    fields = _described_fields(cursor, spec.columns, spec.source, spec.where, params)
    if spec.has_aggregate:
        fields.update(
            _described_fields(
                cursor,
                map_tiles.AGGREGATE_COLUMNS,
                _AGGREGATE_SOURCE,
                _AGGREGATE_WHERE,
                {**params, "layer": spec.name, "cell_z": tile_grid.BASE_CELL_ZOOM},
            )
        )
    return fields


def render_band(
    cursor: Any,
    spec: map_tiles.TileLayerSpec,
    *,
    zoom: int,
    x0: int,
    x1: int,
    y0: int,
    y1: int,
    params: dict[str, Any],
) -> list[Tile]:
    """Every non-empty tile of ``spec`` in the band, as ``(x, y, bytes)``."""
    cursor.execute(
        band_statement(spec, zoom),
        {**zoom_params(spec, zoom, params), "x0": x0, "x1": x1, "y0": y0, "y1": y1},
    )
    # psycopg hands bytea back as a memoryview; the archive wants bytes.
    return [(int(x), int(y), bytes(body)) for x, y, body in cursor.fetchall() if body]


def render_layer(
    cursor: Any,
    spec: map_tiles.TileLayerSpec,
    *,
    bounds: tuple[float, float, float, float],
    params: dict[str, Any],
    progress: Callable[[int, int, int], None] | None = None,
) -> Iterator[tuple[int, list[Tile]]]:
    """Every zoom of ``spec``'s archive over ``bounds``, coarsest first.

    Yields ``(zoom, tiles)`` with the tiles in Hilbert order - the order the
    PMTiles directory wants, so the archive comes out clustered. ``progress``
    is told ``(zoom, tiles_rendered, tiles_addressed)`` after each zoom.
    """
    for zoom in map_tiles.zoom_range(spec.name):
        x0, x1, y0, y1 = map_tiles.tile_range(bounds, zoom)
        tiles: list[Tile] = []
        for y_start, y_end in map_tiles.bands(x0, x1, y0, y1):
            tiles.extend(
                render_band(
                    cursor, spec, zoom=zoom, x0=x0, x1=x1, y0=y_start, y1=y_end, params=params
                )
            )
        if progress is not None:
            progress(zoom, len(tiles), (x1 - x0 + 1) * (y1 - y0 + 1))
        yield zoom, map_tiles.hilbert_sorted(tiles, zoom)
