"""Packing one layer's tiles into a PMTiles archive.

A PMTiles file is a header, a directory of ``(tile id, offset, length)``
entries, a JSON metadata blob and then the tile data - one file a browser can
read any single tile out of with two or three HTTP range requests, the first
of which fetches the header and the root directory together. That is what
lets hbu_rag_map draw straight off S3: no tile server, no database, and a
CDN or the browser's own cache in front of a static object.

The `pmtiles` package's writer does the format; this module does the two
things it leaves to the caller. Tiles are **gzipped one by one** - the
``tile_compression`` the header declares, which the browser undoes with its
own ``DecompressionStream`` - and identical tiles are stored once, which the
writer handles by hashing but only detects for tiles written in order. The
header's bounds and zooms come from what was actually written rather than
from what was asked for, so an archive says truthfully how far it reaches.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, BinaryIO

from pmtiles.reader import MemorySource, Reader
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import Writer

from urban_rag import map_tiles

#: Version 3 of the format; the only one the reader in the browser accepts
#: and the only one the package writes.
SPEC_VERSION = 3


@dataclass
class ArchiveSummary:
    """What an archive holds, for the manifest and the run's metadata."""

    layer: str
    tile_count: int = 0
    #: Compressed bytes handed to the writer. The file is smaller where tiles
    #: repeat - an empty-but-present tile, a cell that fills four children -
    #: so this is what was *rendered*, not the archive's size.
    tile_bytes: int = 0
    by_zoom: dict[int, int] = field(default_factory=dict)
    min_zoom: int | None = None
    max_zoom: int | None = None

    def as_manifest(self) -> dict[str, Any]:
        return {
            "tiles": self.tile_count,
            "bytes": self.tile_bytes,
            "min_zoom": self.min_zoom,
            "max_zoom": self.max_zoom,
            "tiles_by_zoom": {str(zoom): count for zoom, count in sorted(self.by_zoom.items())},
        }


def write_archive(
    target: BinaryIO,
    tiles_by_zoom: Iterable[tuple[int, Iterable[tuple[int, int, bytes]]]],
    *,
    layer: str,
    bounds: tuple[float, float, float, float],
    fields: dict[str, str],
    metadata: dict[str, Any] | None = None,
) -> ArchiveSummary:
    """Write ``tiles_by_zoom`` to ``target`` as one archive of MVT layer ``layer``.

    ``tiles_by_zoom`` is ``(zoom, [(x, y, mvt bytes), ...])`` coarsest first,
    each zoom's tiles in Hilbert order - what `tile_render.render_layer`
    yields. An empty tile is skipped: absent from the directory is what the
    format means by "nothing here", and the browser draws nothing for it
    without a request for its bytes.

    ``bounds`` is ``(west, south, east, north)`` in EPSG:4326; ``fields`` the
    TileJSON field types the layer's properties have; ``metadata`` anything
    else worth recording in the archive's JSON, under its own keys.

    Returns the summary. **With no tile written, nothing is written to
    ``target`` at all** - an archive with an empty directory is invalid, and
    the caller should discard the file rather than upload it.
    """
    writer = Writer(target)
    summary = ArchiveSummary(layer=layer)

    for zoom, tiles in tiles_by_zoom:
        written = 0
        for x, y, body in tiles:
            if not body:
                continue
            data = gzip.compress(bytes(body), mtime=0)
            writer.write_tile(zxy_to_tileid(zoom, x, y), data)
            summary.tile_bytes += len(data)
            written += 1
        if written:
            summary.by_zoom[zoom] = written
            summary.tile_count += written
            summary.min_zoom = zoom if summary.min_zoom is None else min(summary.min_zoom, zoom)
            summary.max_zoom = zoom if summary.max_zoom is None else max(summary.max_zoom, zoom)

    if summary.tile_count == 0:
        writer.tile_f.close()
        return summary

    west, south, east, north = bounds
    header = {
        "tile_type": TileType.MVT,
        "tile_compression": Compression.GZIP,
        "min_zoom": summary.min_zoom,
        "max_zoom": summary.max_zoom,
        "min_lon_e7": map_tiles.e7(west),
        "min_lat_e7": map_tiles.e7(south),
        "max_lon_e7": map_tiles.e7(east),
        "max_lat_e7": map_tiles.e7(north),
        "center_zoom": summary.min_zoom,
        "center_lon_e7": map_tiles.e7((west + east) / 2),
        "center_lat_e7": map_tiles.e7((south + north) / 2),
    }
    archive_metadata = {
        "name": layer,
        "format": "pbf",
        "type": "overlay",
        "version": "1",
        # The one MVT layer every tile carries, named for the map layer. The
        # browser's style table is keyed on it, so it has to be the map's name
        # rather than the table's.
        "vector_layers": [
            {
                "id": layer,
                "minzoom": summary.min_zoom,
                "maxzoom": summary.max_zoom,
                "fields": dict(fields),
            }
        ],
        **(metadata or {}),
    }
    writer.finalize(header, archive_metadata)
    return summary


def open_archive(data: bytes) -> Reader:
    """A reader over an archive held in memory - what the tests round-trip with."""
    return Reader(MemorySource(data))


def archive_tile(reader: Reader, z: int, x: int, y: int) -> bytes | None:
    """One tile's MVT bytes, gunzipped, or None where the archive holds none."""
    stored = reader.get(z, x, y)
    if stored is None:
        return None
    return gzip.decompress(stored)


def archive_metadata(reader: Reader) -> dict[str, Any]:
    return json.loads(json.dumps(reader.metadata()))
