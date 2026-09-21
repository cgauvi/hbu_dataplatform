"""The map's tiles: the specs, the statements, the archive, and the asset.

Nothing here opens a database or touches S3. The statements are checked for
their shape - which column the index test runs on, which table answers below
the detail zoom, which screens have become properties - because those are the
things that were quietly wrong in the on-demand renderer this replaces and
would be quietly wrong again here. The archive is round-tripped through the
`pmtiles` reader, and the asset is materialised against a stubbed renderer to
check what it writes where.
"""

from __future__ import annotations

import io
import json
from contextlib import contextmanager
from pathlib import Path

import pytest
from dagster import MultiPartitionKey, materialize

from urban_rag import map_tiles, pmtiles_archive, tile_assets, tile_grid, tile_render
from urban_rag.rag.documents import DOCUMENT_SOURCES
from urban_rag.resources import ParquetStore, PostgisResource

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"

#: Villeray, roughly.
BOUNDS = (-73.64, 45.53, -73.60, 45.56)


# ---------------------------------------------------------------------------
# the specs
# ---------------------------------------------------------------------------


def test_every_layer_has_a_spec_a_detail_zoom_and_relations():
    for layer in map_tiles.LAYERS:
        spec = map_tiles.layer_spec(layer)
        assert spec.name == layer
        assert layer in map_tiles.DETAIL_ZOOM
        assert map_tiles.LAYER_RELATIONS[layer]
        assert spec.feature_id


def test_an_unknown_layer_is_refused_by_name():
    with pytest.raises(KeyError, match="no tile spec"):
        map_tiles.layer_spec("parks")


def test_the_aggregate_layers_are_the_five_the_pyramid_builds():
    """One archive holds both kinds of tile, so the set that has cells to fall
    back to has to be exactly the set `map_cell_aggregates` dissolves."""
    assert set(map_tiles.AGGREGATE_LAYERS) == set(tile_grid.LAYERS)
    for layer in map_tiles.AGGREGATE_LAYERS:
        assert "gold.map_cell_aggregates" in map_tiles.LAYER_RELATIONS[layer]


def test_layers_with_cells_start_at_the_map_floor_and_the_rest_at_their_detail_zoom():
    for layer in map_tiles.LAYERS:
        zooms = map_tiles.zoom_range(layer)
        assert zooms.stop == map_tiles.MAX_TILE_ZOOM + 1
        if layer in map_tiles.AGGREGATE_LAYERS:
            assert zooms.start == map_tiles.MIN_DISPLAY_ZOOM
        else:
            assert zooms.start == max(map_tiles.MIN_DISPLAY_ZOOM, map_tiles.DETAIL_ZOOM[layer])
    # Zoning has no gate and draws as itself from the map's floor.
    assert map_tiles.zoom_range("zones").start == map_tiles.MIN_DISPLAY_ZOOM
    assert not map_tiles.serves_aggregate("zones", map_tiles.MIN_DISPLAY_ZOOM)


def test_the_detail_zoom_decides_which_table_answers():
    for layer, detail in map_tiles.DETAIL_ZOOM.items():
        if layer not in map_tiles.AGGREGATE_LAYERS:
            assert not map_tiles.serves_aggregate(layer, 0)
            continue
        assert map_tiles.serves_aggregate(layer, detail - 1)
        assert not map_tiles.serves_aggregate(layer, detail)


def test_an_aggregate_tile_is_filled_from_cells_four_zooms_finer():
    for zoom in range(map_tiles.MIN_DISPLAY_ZOOM, max(map_tiles.DETAIL_ZOOM.values())):
        cell = map_tiles.aggregate_cell_zoom(zoom)
        assert cell in tile_grid.CELL_ZOOMS
        assert cell == zoom + tile_grid.ZOOM_OFFSET


def test_the_link_attribute_agrees_with_the_document_registry():
    """The zone tile reads one attribute for the grid link; the corpus records
    one per source. They agree today, and this is what says so."""
    assert map_tiles.link_attribute_agrees()
    assert set(DOCUMENT_SOURCES.values()) == {map_tiles.ZONING_URL_ATTRIBUTE}


# ---------------------------------------------------------------------------
# the grid arithmetic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("zoom", [6, 12, 15, 19])
def test_a_tile_range_covers_its_bounds(zoom):
    x0, x1, y0, y1 = map_tiles.tile_range(BOUNDS, zoom)
    assert x0 <= x1 and y0 <= y1
    west, _south, _east, north = tile_grid.tile_bounds(zoom, x0, y0)
    _west, south, east, _north = tile_grid.tile_bounds(zoom, x1, y1)
    assert west <= BOUNDS[0] and east >= BOUNDS[2]
    assert south <= BOUNDS[1] and north >= BOUNDS[3]


def test_the_first_row_is_the_northern_one():
    """Rows grow southward on the grid, so the range has to start at the
    northern edge - the bug this guards against is a range that is empty
    because y0 > y1."""
    x0, x1, y0, y1 = map_tiles.tile_range(BOUNDS, 15)
    assert y0 < y1


def test_bands_cover_every_row_once_and_stay_under_the_cap():
    x0, x1, y0, y1 = 100, 131, 40, 99  # 32 columns, 60 rows
    rows = []
    for start, end in map_tiles.bands(x0, x1, y0, y1, per_statement=64):
        assert (end - start + 1) * (x1 - x0 + 1) <= 64
        rows.extend(range(start, end + 1))
    assert rows == list(range(y0, y1 + 1))


def test_a_band_is_never_empty_even_for_a_wide_range():
    """A partition wider than the cap still gets one row per statement."""
    assert list(map_tiles.bands(0, 1000, 5, 6, per_statement=64)) == [(5, 5), (6, 6)]


def test_hilbert_order_is_the_directory_order():
    from pmtiles.tile import zxy_to_tileid

    tiles = [(3, 3, b"a"), (0, 0, b"b"), (1, 2, b"c")]
    ordered = map_tiles.hilbert_sorted(tiles, 4)
    ids = [zxy_to_tileid(4, x, y) for x, y, _ in ordered]
    assert ids == sorted(ids)


def test_e7_rounds_outward():
    """The header's bounds have to *contain* the data, so a west edge rounds
    down and an east edge rounds up."""
    assert map_tiles.e7(-73.64) <= -736400000
    assert map_tiles.e7(45.56) >= 455600000


# ---------------------------------------------------------------------------
# the statements
# ---------------------------------------------------------------------------


def test_the_band_statement_asks_the_4326_index_and_clips_in_3857():
    sql = tile_render.band_statement(map_tiles.layer_spec("lots"), 16)
    assert "l.geom && t.lonlat" in sql
    assert "ST_Transform(l.geom, 3857)" in sql
    assert "ST_AsMVTGeom(" in sql
    assert "ST_AsMVT(f, %(layer)s, %(extent)s::integer, 'geom')" in sql
    # Cast, and not decoratively: psycopg 3 binds a small int as smallint and
    # `generate_series(smallint, smallint)` is ambiguous on Postgres.
    assert "generate_series(%(x0)s::integer, %(x1)s::integer)" in sql
    assert "generate_series(%(y0)s::integer, %(y1)s::integer)" in sql
    assert "ST_TileEnvelope(%(z)s::integer, x, y)" in sql
    # No fuse: a tile is bounded by its own area and the cells by construction.
    assert "LIMIT" not in sql


def test_below_the_detail_zoom_the_statement_reads_the_cells():
    sql = tile_render.band_statement(map_tiles.layer_spec("lots"), 13)
    assert "gold.map_cell_aggregates a" in sql
    assert "a.cell_z = %(cell_z)s" in sql
    assert "a.cell_z AS agg_level" in sql
    assert "a.attributes::text AS attributes" in sql
    assert "rag.lots" not in sql


def test_below_the_outline_zoom_the_cells_are_thinned_before_projection():
    sql = tile_render.band_statement(map_tiles.layer_spec("lots"), 9)
    assert "ST_SimplifyPreserveTopology(a.geom, %(tolerance)s)" in sql
    # Outline columns: the flag and the shading, nothing for a tooltip.
    assert "agg_level" in sql
    assert "attributes" not in sql
    assert "feature_count" not in sql


def test_zoom_params_carry_the_cell_level_and_the_tolerance():
    params = tile_render.zoom_params(
        map_tiles.layer_spec("lots"), 9, map_tiles.layer_params(NEIGHBORHOOD, DATE)
    )
    assert params["cell_z"] == 13
    assert params["layer"] == "lots"
    assert params["neighborhood"] == NEIGHBORHOOD
    assert params["scrape_date"] == DATE
    assert params["tolerance"] == pytest.approx(map_tiles.outline_tolerance_deg(9))


def test_a_layer_without_cells_never_reads_the_aggregate_table():
    for layer in ("zones", "land_use", "opportunities", "surface_parking"):
        for zoom in map_tiles.zoom_range(layer):
            assert "map_cell_aggregates" not in tile_render.band_statement(
                map_tiles.layer_spec(layer), zoom
            )


def test_the_partition_screen_is_fixed_on_every_layer():
    """An archive is one partition; no spec may leave either half optional."""
    for layer in map_tiles.LAYERS:
        spec = map_tiles.layer_spec(layer)
        assert "neighborhood = %(neighborhood)s" in spec.where
        assert "scrape_date = %(scrape_date)s::date" in spec.where
        assert "IS NULL OR" not in spec.where


def test_the_sidebar_screens_travel_as_properties_not_predicates():
    """What the on-demand renderer applied in SQL, the browser now applies to
    what it holds - so the inputs have to be on the tile."""
    lots = map_tiles.layer_spec("lots")
    assert "AS area_m2" in lots.columns
    assert "min_area" not in lots.where

    capacity = map_tiles.layer_spec("capacity")
    assert "g.is_underbuilt" in capacity.columns
    assert "only_underbuilt" not in capacity.where

    for layer in ("massing", "surface_parking"):
        spec = map_tiles.layer_spec(layer)
        assert "AS is_underbuilt" in spec.columns
        assert "only_underbuilt" not in spec.where

    opportunities = map_tiles.layer_spec("opportunities")
    assert "o.site_thesis <> 'none'" in opportunities.where
    assert "%(site_thesis)s" not in opportunities.where
    assert "o.is_top_site_opportunity" in opportunities.columns
    assert "o.is_good_candidate" in opportunities.columns

    land_use = map_tiles.layer_spec("land_use")
    assert "AS existing_use" in land_use.columns
    assert "AS hbu_use" in land_use.columns
    assert "use_side" not in land_use.columns


def test_the_three_answer_layers_draw_the_piece_and_join_on_the_cadastral_number():
    for layer in ("land_use", "capacity", "opportunities"):
        spec = map_tiles.layer_spec(layer)
        assert "silver.lot_zone_pieces l" in spec.source
        assert "lot_number   = l.lot_number" in spec.source
        assert "feature_id   = l.feature_id" in spec.source
        assert spec.geom == "l.geom"


def test_the_buildings_tile_reads_the_precomputed_clip_only():
    spec = map_tiles.layer_spec("buildings")
    assert spec.source.startswith("silver.building_lot_intersections")
    assert "building_lot_key" in spec.columns
    assert "ST_Intersection" not in spec.source


def test_the_zone_tile_matches_every_zoning_source():
    spec = map_tiles.layer_spec("zones")
    assert "f.source_table = ANY(%(source_tables)s)" in spec.where
    params = map_tiles.layer_params(NEIGHBORHOOD, DATE)
    assert set(params["source_tables"]) == set(DOCUMENT_SOURCES)
    assert f"'{map_tiles.ZONING_URL_ATTRIBUTE}' AS zoning_pdf_url" in spec.columns


class _Cursor:
    """Records statements; answers an extent, a description, and a band."""

    def __init__(self, extent=BOUNDS, band=()):
        self.statements: list[tuple[str, object]] = []
        self._extent = extent
        self._band = list(band)
        self.description = None

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if "LIMIT 0" in sql:
            self.description = [
                _Column("lot_uid", 20), _Column("area_m2", 701), _Column("flag", 16)
            ]
        else:
            self.description = None

    def fetchone(self):
        sql, _ = self.statements[-1]
        if "ST_Extent" in sql:
            return self._extent
        return ("present",)

    def fetchall(self):
        return list(self._band)


class _Column:
    def __init__(self, name, type_code):
        self.name = name
        self.type_code = type_code


def test_tile_fields_are_read_off_the_statement_and_typed():
    cursor = _Cursor()
    fields = tile_render.tile_fields(
        cursor, map_tiles.layer_spec("lots"), map_tiles.layer_params(NEIGHBORHOOD, DATE)
    )
    assert fields == {"lot_uid": "Number", "area_m2": "Number", "flag": "Boolean"}
    # A layer with cells describes the cells' columns too, in one list.
    assert sum("LIMIT 0" in sql for sql, _ in cursor.statements) == 2


def test_the_layer_extent_is_the_union_of_the_features_and_their_cells():
    cursor = _Cursor(extent=BOUNDS)
    assert tile_render.layer_extent(
        cursor, map_tiles.layer_spec("lots"), map_tiles.layer_params(NEIGHBORHOOD, DATE)
    ) == BOUNDS
    assert sum("ST_Extent" in sql for sql, _ in cursor.statements) == 2


def test_an_empty_layer_has_no_extent():
    cursor = _Cursor(extent=(None, None, None, None))
    assert (
        tile_render.layer_extent(
            cursor, map_tiles.layer_spec("zones"), map_tiles.layer_params(NEIGHBORHOOD, DATE)
        )
        is None
    )


def test_render_band_drops_empty_bodies_and_hands_back_bytes():
    cursor = _Cursor(band=[(1, 2, memoryview(b"\x1a\x02")), (1, 3, None), (2, 2, b"")])
    tiles = tile_render.render_band(
        cursor,
        map_tiles.layer_spec("lots"),
        zoom=16,
        x0=1,
        x1=2,
        y0=2,
        y1=3,
        params=map_tiles.layer_params(NEIGHBORHOOD, DATE),
    )
    assert tiles == [(1, 2, b"\x1a\x02")]
    _sql, params = cursor.statements[-1]
    assert (params["x0"], params["x1"], params["y0"], params["y1"]) == (1, 2, 2, 3)
    assert params["z"] == 16


# ---------------------------------------------------------------------------
# the archive
# ---------------------------------------------------------------------------


def _tiles():
    return [
        (6, [(19, 22, b"coarse")]),
        (15, [(9646, 11732, b"tile-a"), (9647, 11732, b"tile-a"), (9646, 11733, b"")]),
    ]


def test_an_archive_round_trips_through_the_reader():
    buffer = io.BytesIO()
    summary = pmtiles_archive.write_archive(
        buffer,
        _tiles(),
        layer="lots",
        bounds=BOUNDS,
        fields={"lot_uid": "Number"},
        metadata={"hbu": {"layer": "lots"}},
    )
    assert summary.tile_count == 3
    assert summary.by_zoom == {6: 1, 15: 2}
    assert (summary.min_zoom, summary.max_zoom) == (6, 15)

    reader = pmtiles_archive.open_archive(buffer.getvalue())
    header = reader.header()
    assert (header["min_zoom"], header["max_zoom"]) == (6, 15)
    assert header["tile_type"].name == "MVT"
    assert header["tile_compression"].name == "GZIP"
    assert header["min_lon_e7"] <= -736400000 and header["max_lat_e7"] >= 455600000

    assert pmtiles_archive.archive_tile(reader, 15, 9646, 11732) == b"tile-a"
    assert pmtiles_archive.archive_tile(reader, 6, 19, 22) == b"coarse"
    # The empty tile was never stored, and a tile nobody wrote is absent.
    assert pmtiles_archive.archive_tile(reader, 15, 9646, 11733) is None
    assert pmtiles_archive.archive_tile(reader, 15, 1, 1) is None

    metadata = pmtiles_archive.archive_metadata(reader)
    assert metadata["vector_layers"][0]["id"] == "lots"
    assert metadata["vector_layers"][0]["fields"] == {"lot_uid": "Number"}
    assert metadata["vector_layers"][0]["minzoom"] == 6
    assert metadata["hbu"] == {"layer": "lots"}


def test_identical_tiles_are_stored_once():
    buffer = io.BytesIO()
    pmtiles_archive.write_archive(
        buffer, _tiles(), layer="lots", bounds=BOUNDS, fields={}
    )
    header = pmtiles_archive.open_archive(buffer.getvalue()).header()
    assert header["addressed_tiles_count"] == 3
    assert header["tile_contents_count"] == 2


def test_an_archive_with_no_tiles_writes_nothing():
    buffer = io.BytesIO()
    summary = pmtiles_archive.write_archive(
        buffer, [(15, [(1, 1, b"")])], layer="lots", bounds=BOUNDS, fields={}
    )
    assert summary.tile_count == 0
    assert buffer.getvalue() == b""


# ---------------------------------------------------------------------------
# the asset
# ---------------------------------------------------------------------------


class _Connection:
    def cursor(self):
        return _Cursor()


@pytest.fixture
def stubbed_renderer(monkeypatch):
    """A database that holds every layer but two, rendered from canned tiles."""
    empty = {"massing", "surface_parking"}

    @contextmanager
    def connect(self):
        yield _Connection()

    def extent(cursor, spec, params):
        return None if spec.name in empty else BOUNDS

    def fields(cursor, spec, params):
        return {"lot_uid": "Number"}

    def render(cursor, spec, *, bounds, params, progress=None):
        for zoom in (spec.min_zoom, spec.max_zoom):
            yield zoom, [(0, 0, f"{spec.name}@{zoom}".encode())]

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(tile_assets, "layer_extent", extent)
    monkeypatch.setattr(tile_assets, "tile_fields", fields)
    monkeypatch.setattr(tile_assets, "render_layer", render)
    return empty


def _materialize(tmp_path: Path, layers: list[str] | None = None):
    run_config = None
    if layers is not None:
        run_config = {"ops": {"gold__map_tiles": {"config": {"layers": layers}}}}
    return materialize(
        [tile_assets.map_tiles_asset],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": ParquetStore(root_dir=str(tmp_path)), "postgis": PostgisResource()},
        run_config=run_config,
        raise_on_error=False,
    )


def _partition_dir(tmp_path: Path) -> Path:
    return tmp_path / "gold" / "map_tiles" / DATE / NEIGHBORHOOD


def test_the_asset_writes_an_archive_per_layer_and_a_manifest(tmp_path, stubbed_renderer):
    result = _materialize(tmp_path)
    assert result.success

    out = _partition_dir(tmp_path)
    written = sorted(path.name for path in out.glob("*.pmtiles"))
    assert written == sorted(
        map_tiles.archive_file(layer) for layer in map_tiles.LAYERS if layer not in stubbed_renderer
    )
    manifest = json.loads((out / map_tiles.MANIFEST_FILE).read_text())
    assert manifest["scrape_date"] == DATE
    assert manifest["neighborhood"] == NEIGHBORHOOD
    assert manifest["zoom_offset"] == tile_grid.ZOOM_OFFSET
    assert manifest["outline_zoom"] == map_tiles.AGGREGATE_OUTLINE_ZOOM
    assert manifest["empty_layers"] == sorted(stubbed_renderer)
    assert manifest["skipped_layers"] == {}
    assert manifest["bounds"] == list(BOUNDS)
    lots = manifest["layers"]["lots"]
    assert lots["file"] == "lots.pmtiles"
    assert lots["detail_zoom"] == map_tiles.DETAIL_ZOOM["lots"]
    assert lots["has_aggregate"] is True
    assert lots["feature_id"] == "lot_uid"
    assert lots["tiles"] == 2
    assert lots["min_zoom"] == map_tiles.MIN_DISPLAY_ZOOM
    assert lots["max_zoom"] == map_tiles.MAX_TILE_ZOOM
    # In draw order, so a reader of the file sees the map's order.
    assert list(manifest["layers"]) == [
        layer for layer in map_tiles.LAYERS if layer not in stubbed_renderer
    ]

    reader = pmtiles_archive.open_archive((out / "lots.pmtiles").read_bytes())
    assert pmtiles_archive.archive_tile(reader, map_tiles.MAX_TILE_ZOOM, 0, 0) == b"lots@19"

    (materialization,) = result.asset_materializations_for_node("gold__map_tiles")
    metadata = {key: value.value for key, value in materialization.metadata.items()}
    assert metadata["num_layers"] == len(map_tiles.LAYERS) - len(stubbed_renderer)
    assert metadata["num_tiles"] == 2 * metadata["num_layers"]
    assert set(metadata["layers_empty"]) == stubbed_renderer


def test_a_narrowed_run_keeps_the_other_archives_and_their_manifest_entries(
    tmp_path, stubbed_renderer
):
    assert _materialize(tmp_path).success
    out = _partition_dir(tmp_path)
    before = (out / "zones.pmtiles").read_bytes()
    (out / "lots.pmtiles").write_bytes(b"stale")

    assert _materialize(tmp_path, layers=["lots"]).success

    assert (out / "lots.pmtiles").read_bytes() != b"stale"
    assert (out / "zones.pmtiles").read_bytes() == before
    manifest = json.loads((out / map_tiles.MANIFEST_FILE).read_text())
    assert "zones" in manifest["layers"] and "lots" in manifest["layers"]


def test_a_layer_whose_relations_are_missing_is_skipped_not_fatal(
    tmp_path, stubbed_renderer, monkeypatch
):
    def missing(cursor, relations):
        return [name for name in relations if name == "gold.lot_investment_opportunities"]

    monkeypatch.setattr(tile_assets, "_missing_relations", missing)
    result = _materialize(tmp_path)
    assert result.success
    manifest = json.loads((_partition_dir(tmp_path) / map_tiles.MANIFEST_FILE).read_text())
    assert "opportunities" in manifest["skipped_layers"]
    assert "opportunities" not in manifest["layers"]
    assert not (_partition_dir(tmp_path) / "opportunities.pmtiles").exists()


def test_an_unknown_layer_fails_the_run_before_anything_is_written(tmp_path, stubbed_renderer):
    result = _materialize(tmp_path, layers=["lots", "parks"])
    assert not result.success
    assert not _partition_dir(tmp_path).exists()
