"""Offline tests for the street network: the RQTT snapshot and the tile's
sides selected out of it.

Nothing here touches the network. The province-wide archive is stubbed the way
`test_rqtt` stubs it - a tiny real GeoPackage, written in the MTQ Lambert the
MRNF publishes in and zipped under `OGC(GPKG)/` - so what runs is GDAL's own
reading and the real bbox push-down, and both assets run against a temp
directory through `dagster.materialize`.

The geometry is deliberately small and rectilinear, and sits where the three
cities do, because `neighborhood_streets` measures in each city's MTM zone and
a shape somewhere else would project to numbers that mean nothing. The Montreal
segments sit under one cell of the cut - `TILE`, which holds Villeray - and
`test_the_fixture_sits_where_the_tests_say_it_does` checks that against
`tile_grid.quadkey_bounds` rather than assuming it. The fixture's `AQRP_UUID`s
are numeric strings rather than real uuids so the assertions below can read
as `[1, 2]`.
"""

from __future__ import annotations

import zipfile
from contextlib import contextmanager
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import LineString, MultiLineString, box

from asset_helpers import materialization_metadata

from hbu_dataplatform.core import tile_grid
from hbu_dataplatform.core.frames import write_frame
from hbu_dataplatform.boundaries.assets import (
    QUARTIERS_FILE,
    QUEBEC_BOROUGHS_FILE,
    SAGUENAY_LIMITS_FILE,
    STREET_ID_COLUMN,
    STREET_SEGMENTS_FILE,
    reference_neighborhoods,
    street_network,
)
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.sources.rqtt.resources import RqttResource
from hbu_dataplatform.sources.rqtt.client import (
    PUBLISHED_CRS,
    ROAD_LAYER,
    STREET_ID_FIELD,
    STREET_NAME_FIELD,
    WGS84,
    RqttFetcher,
)
from hbu_dataplatform.core.storage import join
from hbu_dataplatform.sources.rqtt import assets as street_assets
from hbu_dataplatform.sources.rqtt.assets import (
    STREETS_FILE_OUT,
    _length_m,
    neighborhood_streets,
)

DATE = "2026-08-01"
#: The cut cell the Montreal fixture sits under: a z14 cell holding Villeray,
#: -73.630 to -73.608 by 45.537 to 45.553.
TILE = "0302303330102"
#: The borough whose outline the fixture draws inside that cell - what a
#: side's `neighborhood` resolves to, as an attribute rather than a bound.
NEIGHBORHOOD = "VSMPE"
#: `partitions.NEIGHBORHOOD_BOROUGH_CODES["VSMPE"]` - what `borough_boundary`
#: cuts the reference layer on.
BOROUGH_CODE = "25"

#: A square kilometre of "borough", give or take, in the middle of the island,
#: and inside `TILE` with room to spare on the east.
BOROUGH = box(-73.630, 45.540, -73.620, 45.550)
#: One arrondissement each for the other two cities, drawn where they are.
QUEBEC_BOROUGH = box(-71.25, 46.79, -71.20, 46.84)
SAGUENAY_CITY = box(-71.17, 48.38, -71.12, 48.42)

#: The vintage the stubbed server publishes.
VERSION = "20260703"
LAST_MODIFIED = "Fri, 03 Jul 2026 17:15:26 GMT"


def segment(
    uid: str,
    name: str,
    coordinates: list[list[float]],
    *,
    road_class: str = "Locale",
    characteristic=None,
) -> dict:
    """One road segment, spelled the way the MRNF spells it."""
    return {
        "properties": {
            STREET_ID_FIELD: uid,
            STREET_NAME_FIELD: name,
            "ClsRte": road_class,
            "CaractRte": characteristic,
            "IdRte": f"rte-{uid}",
            "Gestion": "Municipal",
            "NoRte": None,
            "Version": "AQ20260501",
        },
        "coordinates": coordinates,
    }


#: Three Montreal segments: one wholly inside the borough, one running out
#: through its eastern edge (midpoint on the line, still under `TILE`), and
#: one a kilometre beyond it whose midpoint is in the cell to the east even
#: though its western end reaches back into `TILE`.
INSIDE = segment("1", "Jarry", [[-73.628, 45.545], [-73.624, 45.545]])
STRADDLING = segment("2", "Papineau", [[-73.624, 45.547], [-73.616, 45.547]])
OUTSIDE = segment("3", "Saint-Denis", [[-73.610, 45.547], [-73.605, 45.547]])
#: Under `TILE`, and in no registered borough's outline: the island past the
#: line, which is still the tile's ground.
NO_BOROUGH = segment("5", "Hors", [[-73.615, 45.547], [-73.611, 45.547]])

#: One segment in each of the other two cities, so the bbox-per-city read has
#: something to find and `num_segments_by_city` has something to report.
QUEBEC_SEGMENT = segment("10", "Rue Saint-Joseph", [[-71.23, 46.81], [-71.22, 46.81]])
SAGUENAY_SEGMENT = segment("20", "Rue Louis-Hudon", [[-71.15, 48.40], [-71.14, 48.40]])

#: A ferry link across the river: `Liaison maritime` is one of the classes
#: `rqtt.roadway_only` drops, because a line through open water would otherwise
#: make whatever parcel it crosses a road lot.
FERRY = segment(
    "4", "Traverse", [[-73.629, 45.541], [-73.621, 45.541]], road_class="Liaison maritime"
)


def road_frame(segments: list[dict]) -> gpd.GeoDataFrame:
    """The segments as the archive publishes them: MTQ Lambert, its columns."""
    frame = gpd.GeoDataFrame(
        [item["properties"] for item in segments],
        geometry=[LineString(item["coordinates"]) for item in segments],
        crs=WGS84,
    )
    return frame.to_crs(PUBLISHED_CRS)


def zipped_geopackage(directory: Path, segments: list[dict]) -> bytes:
    """The road layer as a real GeoPackage, zipped the way the RQTT ships."""
    directory.mkdir(parents=True, exist_ok=True)
    gpkg = directory / "RQTT.gpkg"
    road_frame(segments).to_file(gpkg, layer=ROAD_LAYER, driver="GPKG")
    archive = directory / "RQTT_GPKG.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(gpkg, f"OGC(GPKG)/{gpkg.name}")
    payload = archive.read_bytes()
    gpkg.unlink()
    archive.unlink()
    return payload


class FakeResponse:
    def __init__(self, content: bytes, *, headers=None):
        self.content = content
        self.headers = {"Content-Type": "application/zip", **(headers or {})}
        self.status_code = 200

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start : start + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    """Answers HEAD with a vintage and GET with the canned archive."""

    def __init__(self, content: bytes):
        self.content = content
        self.calls: list[str] = []

    def head(self, url, timeout=None, allow_redirects=True):
        self.calls.append(f"HEAD {url}")
        return FakeResponse(b"", headers={"Last-Modified": LAST_MODIFIED})

    def get(self, url, timeout=None, stream=False):
        self.calls.append(f"GET {url}")
        return FakeResponse(self.content)


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_quartiers(store, *, geometry=None, code=BOROUGH_CODE):
    """The reference layer, as `reference_neighborhoods` writes it."""
    frame = gpd.GeoDataFrame(
        {"no_qr": ["01"], "no_arr": [code], "nom_qr": ["Villeray"]},
        geometry=[geometry if geometry is not None else BOROUGH],
        crs=WGS84,
    )
    write_frame(
        frame,
        join(
            store.partition_dir(reference_neighborhoods.key.path[-1], DATE),
            QUARTIERS_FILE,
        ),
    )


def write_outlines(store):
    """Every city's outline, which is what `city_bounds` reads to draw a box.

    `street_network` is bounded per city, so unlike the silver tests below -
    which only ever cut VSMPE - bronze needs all three files present.
    """
    write_quartiers(store)
    outline_dir = store.partition_dir(reference_neighborhoods.key.path[-1], DATE)
    write_frame(
        gpd.GeoDataFrame(
            {"abreviation": ["CIL"], "nom": ["La Cité-Limoilou"]},
            geometry=[QUEBEC_BOROUGH],
            crs=WGS84,
        ),
        join(outline_dir, QUEBEC_BOROUGHS_FILE),
    )
    write_frame(
        gpd.GeoDataFrame(
            {"nom": ["Saguenay"], "type": ["ville"]},
            geometry=[SAGUENAY_CITY],
            crs=WGS84,
        ),
        join(outline_dir, SAGUENAY_LIMITS_FILE),
    )


# -- bronze: street_network ------------------------------------------------


def materialize_bronze(store, monkeypatch, tmp_path, *, segments=None, scrape_date=DATE):
    """Run `street_network` against a stubbed archive.

    Patched on the class rather than on an instance: Dagster rebuilds the
    resource from its config before the run, so an instance attribute would not
    survive into the asset.
    """
    if segments is None:
        segments = [INSIDE, STRADDLING, OUTSIDE, QUEBEC_SEGMENT, SAGUENAY_SEGMENT]
    session = FakeSession(zipped_geopackage(tmp_path / "src", segments))
    fetcher = RqttFetcher(
        cache_dir=tmp_path / "cache",
        base_url="https://example/rqtt",
        request_delay_seconds=0,
        session=session,
    )
    monkeypatch.setattr(RqttResource, "fetcher", lambda self: fetcher)
    return materialize(
        [street_network],
        partition_key=scrape_date,
        resources={"rqtt": RqttResource(cache_dir=str(tmp_path / "cache")), "store": store},
    )


def bronze_frame(tmp_path) -> gpd.GeoDataFrame:
    return gpd.read_parquet(
        tmp_path / "store" / "bronze" / "street_network" / DATE / STREET_SEGMENTS_FILE
    )


def test_the_snapshot_lands_under_the_date_partition(store, monkeypatch, tmp_path):
    write_outlines(store)

    result = materialize_bronze(store, monkeypatch, tmp_path)

    assert result.success
    frame = bronze_frame(tmp_path)
    # `Saint-Denis` is outside every city's box and never reaches the file;
    # the other four are one per city plus the straddler.
    assert len(frame) == 4
    assert frame.crs.to_string() == WGS84
    # The prefix carries bare values, so the date has to travel as a column.
    assert set(frame["scrape_date"]) == {DATE}
    assert {"source_file", "scraped_at", "rqtt_version"} <= set(frame.columns)


def test_the_publisher_column_names_survive_bronze(store, monkeypatch, tmp_path):
    """Silver is where `AQRP_UUID` becomes `COTE_RUE_ID`, not bronze.

    The rest of the lot lineage carries its publishers' names through bronze
    untouched, and renaming here would put the translation in two places.
    """
    write_outlines(store)

    materialize_bronze(store, monkeypatch, tmp_path)

    frame = bronze_frame(tmp_path)
    assert STREET_ID_FIELD in frame.columns
    assert STREET_ID_COLUMN not in frame.columns
    assert {"ClsRte", "IdRte", "Gestion"} <= set(frame.columns)


def test_the_box_is_drawn_per_city(store, monkeypatch, tmp_path):
    """Three boxes, hundreds of kilometres apart, out of one province-wide read."""
    write_outlines(store)

    result = materialize_bronze(store, monkeypatch, tmp_path)

    metadata = materialization_metadata(result, street_network)
    assert metadata["num_segments_by_city"].data == {
        "montreal": 2,
        "quebec": 1,
        "saguenay": 1,
    }
    assert metadata["num_street_segments"].value == 4
    assert metadata["rqtt_version"].value == VERSION
    assert "CC-BY" in metadata["license"].value


def test_what_is_not_a_roadway_is_dropped(store, monkeypatch, tmp_path):
    """A ferry link crosses water, and a line through water would make
    whatever parcel it crosses a road lot."""
    write_outlines(store)

    result = materialize_bronze(
        store,
        monkeypatch,
        tmp_path,
        segments=[INSIDE, FERRY, QUEBEC_SEGMENT, SAGUENAY_SEGMENT],
    )

    frame = bronze_frame(tmp_path)
    assert sorted(frame[STREET_ID_FIELD].astype(int)) == [1, 10, 20]
    assert materialization_metadata(result, street_network)["num_not_roadway"].value == 1


def test_a_city_the_snapshot_cannot_reach_is_a_failure(store, monkeypatch, tmp_path):
    """Not an empty box: a city with no road in it is a broken outline or a
    broken archive, never a city with no roads."""
    write_outlines(store)

    with pytest.raises(Failure, match="no road segment inside"):
        materialize_bronze(
            store, monkeypatch, tmp_path, segments=[INSIDE, QUEBEC_SEGMENT]
        )


def test_a_missing_outline_names_the_asset_to_run(store, monkeypatch, tmp_path):
    with pytest.raises(Failure, match="materialize reference_neighborhoods"):
        materialize_bronze(store, monkeypatch, tmp_path)


def test_a_bronze_rerun_replaces_the_previous_snapshot(store, monkeypatch, tmp_path):
    write_outlines(store)
    partition = tmp_path / "store" / "bronze" / "street_network" / DATE
    partition.mkdir(parents=True, exist_ok=True)
    stale = partition / "street_sides_retired.parquet"
    gpd.GeoDataFrame(
        {"a": [1]}, geometry=[LineString([(0, 0), (1, 1)])], crs=WGS84
    ).to_parquet(stale)

    materialize_bronze(store, monkeypatch, tmp_path)

    assert not stale.exists()
    assert (partition / STREET_SEGMENTS_FILE).exists()


# -- silver: neighborhood_streets ------------------------------------------


def write_bronze(store, *segments, scrape_date=DATE):
    """The snapshot, as `street_network` writes it: WGS84, publisher columns."""
    if not segments:
        segments = (INSIDE, STRADDLING, OUTSIDE)
    frame = gpd.GeoDataFrame(
        [item["properties"] for item in segments],
        geometry=[LineString(item["coordinates"]) for item in segments],
        crs=WGS84,
    )
    frame["scrape_date"] = scrape_date
    write_frame(
        frame,
        join(
            store.partition_dir(street_network.key.path[-1], scrape_date),
            STREET_SEGMENTS_FILE,
        ),
    )


def midpoint_key(item: dict) -> str:
    """The zoom-19 quadkey of a fixture segment's midpoint - what the asset
    selects on."""
    point = LineString(item["coordinates"]).interpolate(0.5, normalized=True)
    return tile_grid.quadkey_of(point.x, point.y)


def test_the_fixture_sits_where_the_tests_say_it_does():
    """Checked rather than assumed: `INSIDE` and `STRADDLING` have their
    midpoints under `TILE`, `OUTSIDE` has its midpoint in the cell to the
    east while its western end still reaches into `TILE` - which is what
    makes it the case that separates selection by midpoint from selection
    by intersection."""
    lo, hi = tile_grid.quadkey_bounds(TILE)
    assert lo <= tile_grid.quadkey_of(-73.6210, 45.5395) < hi
    assert lo <= midpoint_key(INSIDE) < hi
    assert lo <= midpoint_key(STRADDLING) < hi
    assert not lo <= midpoint_key(OUTSIDE) < hi
    z, x, y = tile_grid.cell_of_quadkey(TILE)
    west, south, east, north = tile_grid.tile_bounds(z, x, y)
    assert west < OUTSIDE["coordinates"][0][0] < east
    assert south < OUTSIDE["coordinates"][0][1] < north


@pytest.fixture(autouse=True)
def stub_postgis(monkeypatch):
    """The upsert into `silver.neighborhood_streets`, recorded rather than run.

    The asset publishes the same frame it writes to the tree, which needs a
    database; every test here is about the selection, so the load is stubbed
    and the frame it was handed is kept for the tests that do care.
    """
    seen: dict[str, object] = {}

    @contextmanager
    def connect(self):
        yield object()

    def load_streets(connection, frame, *, tile, scrape_date):
        seen["frame"] = frame
        seen["partition"] = (tile, scrape_date)
        return {
            "copied": len(frame),
            "duplicates": 0,
            "upserted": len(frame),
            "pruned": 0,
        }

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(street_assets, "load_streets", load_streets)
    return seen


def materialize_silver(store):
    return materialize(
        [neighborhood_streets],
        partition_key=MultiPartitionKey({"date": DATE, "tile": TILE}),
        resources={"store": store, "postgis": PostgisResource()},
    )


def read_silver(tmp_path) -> gpd.GeoDataFrame:
    return gpd.read_parquet(
        tmp_path
        / "store"
        / "silver"
        / "neighborhood_streets"
        / DATE
        / TILE
        / STREETS_FILE_OUT
    )


def test_only_the_sides_whose_midpoint_is_in_the_tile_are_kept(store, tmp_path):
    write_quartiers(store)
    write_bronze(store)

    assert materialize_silver(store).success

    frame = read_silver(tmp_path)
    # `Saint-Denis` reaches into the tile and has its midpoint in the cell
    # to the east; it is that cell's side, and it is whole there.
    assert sorted(frame[STREET_ID_COLUMN].astype(int)) == [1, 2]


def test_the_publishers_key_becomes_this_platforms(store, tmp_path):
    """`_as_street_sides` is the one place a publisher's vocabulary is
    translated, and it now runs for every city rather than for two of three."""
    write_quartiers(store)
    write_bronze(store)

    materialize_silver(store)

    frame = read_silver(tmp_path)
    assert STREET_ID_COLUMN in frame.columns
    # The publisher's own columns travel alongside untouched.
    assert {"ClsRte", "IdRte"} <= set(frame.columns)


def test_a_side_is_stored_whole_wherever_its_ends_reach(store, tmp_path):
    """Selected, not clipped. `Papineau` runs out through the borough's
    eastern edge, and nothing is taken off it: the cell is a write-ownership
    claim, and a lot on the line measures its frontage on the whole side."""
    write_quartiers(store)
    write_bronze(store)

    materialize_silver(store)

    frame = read_silver(tmp_path).set_index(STREET_ID_COLUMN)
    straddling = frame.loc["2"]
    assert straddling.geometry.equals(LineString(STRADDLING["coordinates"]))
    assert not straddling.geometry.within(BOROUGH.buffer(1e-9))
    # The published length is the side's length; there is no surviving piece
    # to report beside it any more.
    assert {"length_in_borough_m", "pct_in_borough"}.isdisjoint(frame.columns)
    published = _length_m(
        gpd.GeoSeries([LineString(STRADDLING["coordinates"])], crs=WGS84),
        "EPSG:32188",
    ).iloc[0]
    assert straddling["segment_length_m"] == pytest.approx(published, rel=1e-9)


def test_lengths_are_metres_and_not_degrees(store, tmp_path):
    """Measured in EPSG:32188. In 4326 the same line is 0.004 "long"."""
    write_quartiers(store)
    write_bronze(store, INSIDE)

    materialize_silver(store)

    frame = read_silver(tmp_path)
    # ~0.004 degrees of longitude at 45.5 N, which is a little over 300 m.
    assert 290.0 < frame["segment_length_m"].iloc[0] < 330.0


def test_the_partition_travels_as_columns(store, tmp_path):
    write_quartiers(store)
    write_bronze(store)

    materialize_silver(store)

    frame = read_silver(tmp_path)
    assert set(frame["cell_partition"]) == {TILE}
    assert set(frame["scrape_date"]) == {DATE}
    assert frame.crs.to_string() == WGS84
    # The row's own address at full depth: every key is under the tile.
    assert (frame["cell_key"].str.len() == tile_grid.BASE_CELL_ZOOM).all()
    assert frame["cell_key"].str.startswith(TILE).all()
    keyed = frame.set_index(STREET_ID_COLUMN)
    assert keyed.loc["1", "cell_key"] == midpoint_key(INSIDE)
    assert keyed.loc["2", "cell_key"] == midpoint_key(STRADDLING)


def test_the_borough_is_the_outline_holding_the_midpoint(store, tmp_path):
    """An attribute, not a bound: `Jarry` sits in VSMPE's outline, `Hors`
    sits in the tile but in no registered borough, and both are the tile's."""
    write_quartiers(store)
    write_bronze(store, INSIDE, NO_BOROUGH)

    result = materialize_silver(store)

    frame = read_silver(tmp_path).set_index(STREET_ID_COLUMN)
    assert frame.loc["1", "neighborhood"] == NEIGHBORHOOD
    # NULL in the file, whichever spelling of null the reader hands back.
    assert pd.isna(frame.loc["5", "neighborhood"])
    metadata = materialization_metadata(result, neighborhood_streets)
    assert metadata["num_without_borough"].value == 1
    assert metadata["neighborhoods"].value == NEIGHBORHOOD


def test_a_borough_whose_outline_cannot_be_read_is_skipped(store, tmp_path):
    """`CIL` is registered by default and the fixture writes no Quebec City
    arrondissements: the sides still land, without a borough from it."""
    write_quartiers(store)
    write_bronze(store)

    result = materialize_silver(store)

    assert result.success
    frame = read_silver(tmp_path)
    assert set(frame["neighborhood"]) == {NEIGHBORHOOD}


def test_silver_metadata_reports_the_selection(store):
    write_quartiers(store)
    write_bronze(store)

    metadata = materialization_metadata(materialize_silver(store), neighborhood_streets)

    assert metadata["tile"].value == TILE
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_sides"].value == 2
    assert metadata["num_segments_in_snapshot"].value == 3
    assert metadata["num_streets_named"].value == 2
    assert metadata["num_without_borough"].value == 0
    assert metadata["num_invalid_geometries"].value == 0
    assert metadata["total_length_km"].value > 0
    assert "num_boundary_clipped" not in metadata


def test_the_load_is_handed_the_tile(store, stub_postgis):
    write_quartiers(store)
    write_bronze(store)

    materialize_silver(store)

    assert stub_postgis["partition"] == (TILE, DATE)
    loaded = stub_postgis["frame"]
    assert {"cell_key", "cell_partition", "neighborhood"} <= set(loaded.columns)
    assert set(loaded["cell_partition"]) == {TILE}


def test_the_same_segment_arriving_twice_is_refused(store):
    """One row per COTE_RUE_ID is the grain; a duplicate would multiply every
    frontage pair the join downstream produces."""
    write_quartiers(store)
    write_bronze(
        store, INSIDE, segment("1", "Jarry", [[-73.627, 45.546], [-73.625, 45.546]])
    )

    with pytest.raises(Failure, match="appear more than once"):
        materialize_silver(store)


def test_a_tile_no_midpoint_falls_in_is_a_failure(store):
    """Not an empty partition: a cut cell holds lots, so a snapshot with no
    side under it is missing the ground, never a cell with no streets."""
    write_quartiers(store)
    write_bronze(store, OUTSIDE)

    with pytest.raises(Failure, match="No street side has its midpoint in tile"):
        materialize_silver(store)


def test_a_missing_snapshot_names_the_asset_to_run(store):
    write_quartiers(store)

    with pytest.raises(Failure, match="materialize street_network"):
        materialize_silver(store)


def test_no_outline_at_all_names_the_asset_to_run(store):
    """One borough's outline missing is skipped; every outline missing is
    the reference layer missing, and that is fatal."""
    write_bronze(store)

    with pytest.raises(Failure, match="materialize reference_neighborhoods"):
        materialize_silver(store)


def test_a_silver_rerun_replaces_the_previous_partition(store, tmp_path):
    partition = tmp_path / "store" / "silver" / "neighborhood_streets" / DATE / TILE
    partition.mkdir(parents=True)
    stale = partition / "neighborhood_streets_retired.parquet"
    gpd.GeoDataFrame(
        {"a": [1]}, geometry=[LineString([(0, 0), (1, 1)])], crs=WGS84
    ).to_parquet(stale)
    write_quartiers(store)
    write_bronze(store)

    materialize_silver(store)

    assert not stale.exists()
    assert (partition / STREETS_FILE_OUT).exists()


def test_a_failure_message_carries_the_partition(store):
    """Every guard in this asset names the tile and the date, because a
    backfill fails one partition at a time and the message is what says which."""
    write_quartiers(store)
    write_bronze(
        store, INSIDE, segment("1", "Jarry", [[-73.627, 45.546], [-73.625, 45.546]])
    )

    with pytest.raises(Failure, match=f"{TILE} {DATE}"):
        materialize_silver(store)


def test_load_streets_promotes_every_segment_to_multi(monkeypatch):
    """The column is `geometry(MultiLineString, 4326)`, and a typmod rejects a
    bare `LineString` rather than promoting it - so `load_streets` promotes.

    Direct, because the asset's own tests stub the load away: this helper only
    ever runs against a real database, which is where the promotion failing
    would first be seen.
    """
    from hbu_dataplatform.core import postgis, warehouse

    seen: dict[str, object] = {}

    def upsert_frame(connection, dataset, frame, **kwargs):
        seen["frame"] = frame
        return {"copied": len(frame), "duplicates": 0, "upserted": len(frame), "pruned": 0}

    monkeypatch.setattr(warehouse, "upsert_frame", upsert_frame)

    frame = gpd.GeoDataFrame(
        {STREET_ID_COLUMN: [1, 2]},
        geometry=[
            LineString([(-73.627, 45.546), (-73.625, 45.546)]),
            MultiLineString(
                [
                    [(-73.627, 45.547), (-73.626, 45.547)],
                    [(-73.624, 45.547), (-73.623, 45.547)],
                ]
            ),
        ],
        crs=WGS84,
    )

    postgis.load_streets(object(), frame, tile=TILE, scrape_date=DATE)

    loaded = seen["frame"]
    assert set(loaded.geometry.geom_type) == {"MultiLineString"}
    # Measured geodesically here rather than in a projected CRS, so it agrees
    # with the `ST_Length(geography(...))` every other measure is stated in.
    assert (loaded["length_m"] > 0).all()
