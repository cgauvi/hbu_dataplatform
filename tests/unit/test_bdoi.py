"""Offline tests for the BDOI client and the building asset it feeds.

The province-wide zipped shapefiles are stubbed by writing tiny real
shapefiles to a temp dir and zipping them, so what is under test is GDAL's
own zip/shapefile reading, the on-disk cache, and the spatial cut of the
combined layer down to one borough - not the network.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import geopandas as gpd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import box

from urban_rag.bdoi import BdoiError, BdoiFetcher, QUEBEC_FILES, read_shapefile_zip
from urban_rag.bdoi_assets import BUILDINGS_FILE, neighborhood_buildings
from urban_rag.frames import write_frame
from urban_rag.open_data_assets import QUARTIERS_FILE, reference_neighborhoods
from urban_rag.resources import BdoiResource, ParquetStore
from urban_rag.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
#: The borough code `VSMPE` maps to in the reference layer.
BOROUGH_CODE = "25"


class FakeResponse:
    def __init__(self, content: bytes, *, content_type: str = "application/zip"):
        self.content = content
        self.headers = {"Content-Type": content_type}
        self.status_code = 200

    def raise_for_status(self):
        return None


class FakeSession:
    """Replays canned zip bytes, keyed by the tail of the URL."""

    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.calls: list[str] = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        filename = url.rsplit("/", 1)[-1]
        if filename not in self.files:
            raise AssertionError(f"unexpected download: {url}")
        return FakeResponse(self.files[filename])


def zipped_shapefile(directory: Path, name: str, frame: gpd.GeoDataFrame) -> Path:
    """Write ``frame`` as a real shapefile, zipped the way BDOI is published."""
    shp_dir = directory / f"{name}_src"
    shp_dir.mkdir(parents=True)
    frame.to_file(shp_dir / f"{name}.shp")

    zip_path = directory / f"{name}.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for component in shp_dir.iterdir():
            archive.write(component, component.name)
    return zip_path


# -- client -----------------------------------------------------------------


def test_fetch_downloads_and_caches_by_filename(tmp_path):
    zip_path = zipped_shapefile(
        tmp_path / "src",
        "a",
        gpd.GeoDataFrame({"id": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"),
    )
    session = FakeSession({"BDOI_v3_QC_1.zip": zip_path.read_bytes()})
    fetcher = BdoiFetcher(
        cache_dir=tmp_path / "cache",
        base_url="https://example/zip",
        request_delay_seconds=0,
        session=session,
    )

    path = fetcher.fetch("BDOI_v3_QC_1.zip")

    assert path == tmp_path / "cache" / "BDOI_v3_QC_1.zip"
    assert session.calls == ["https://example/zip/BDOI_v3_QC_1.zip"]

    # A second fetch reuses the cache instead of downloading again.
    fetcher.fetch("BDOI_v3_QC_1.zip")
    assert session.calls == ["https://example/zip/BDOI_v3_QC_1.zip"]


def test_a_non_zip_response_is_rejected(tmp_path):
    session = FakeSession({"BDOI_v3_QC_1.zip": b"<html>404 not found</html>"})
    fetcher = BdoiFetcher(
        cache_dir=tmp_path / "cache",
        base_url="https://example/zip",
        request_delay_seconds=0,
        session=session,
    )

    with pytest.raises(BdoiError, match="not a zip"):
        fetcher.fetch("BDOI_v3_QC_1.zip")


def test_read_shapefile_zip_reprojects_to_4326(tmp_path):
    frame = gpd.GeoDataFrame({"id": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:3857")
    zip_path = zipped_shapefile(tmp_path, "a", frame)

    read = read_shapefile_zip(zip_path)

    assert read.crs.to_string() == "EPSG:4326"
    assert len(read) == 1


def test_an_unreadable_zip_is_reported_with_the_path(tmp_path):
    bogus = tmp_path / "bogus.zip"
    bogus.write_bytes(b"not actually a zip")

    with pytest.raises(BdoiError, match="bogus.zip"):
        read_shapefile_zip(bogus)


# -- asset --------------------------------------------------------------


class FakeBdoiFetcher:
    """Returns pre-written local paths instead of downloading anything."""

    def __init__(self, paths: dict[str, Path]):
        self.paths = paths

    def fetch(self, filename: str) -> Path:
        return self.paths[filename]


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_quartiers(store, *, code=BOROUGH_CODE):
    """The upstream boundary the asset clips the province-wide layer to."""
    path = join(
        store.partition_dir(reference_neighborhoods.key.path[-1], DATE), QUARTIERS_FILE
    )
    frame = gpd.GeoDataFrame(
        {"no_qr": ["01"], "no_arr": [code]},
        geometry=[box(-73.7, 45.4, -73.4, 45.7)],
        crs="EPSG:4326",
    )
    write_frame(frame, path)


def stub_bdoi(tmp_path, monkeypatch, *, inside, outside):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    zip_dir = tmp_path / "zips"
    zip_dir.mkdir(exist_ok=True)
    file_1, file_2 = QUEBEC_FILES
    path_1 = zipped_shapefile(zip_dir, file_1.removesuffix(".zip"), inside)
    path_2 = zipped_shapefile(zip_dir, file_2.removesuffix(".zip"), outside)
    fetcher = FakeBdoiFetcher({file_1: path_1, file_2: path_2})
    monkeypatch.setattr(BdoiResource, "fetcher", lambda self: fetcher)
    return fetcher


@pytest.fixture
def bdoi(tmp_path, monkeypatch):
    """One building inside VSMPE's borough, one far outside it."""
    return stub_bdoi(
        tmp_path,
        monkeypatch,
        inside=gpd.GeoDataFrame(
            {"link_id": [1]},
            geometry=[box(-73.6, 45.5, -73.55, 45.55)],
            crs="EPSG:4326",
        ),
        outside=gpd.GeoDataFrame(
            {"link_id": [2]}, geometry=[box(10, 10, 11, 11)], crs="EPSG:4326"
        ),
    )


def run(store):
    return materialize(
        [neighborhood_buildings],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={
            "bdoi": BdoiResource(cache_dir=str(store.root_dir)),
            "store": store,
        },
        selection=[neighborhood_buildings],
    )


def test_buildings_land_under_date_then_neighborhood(store, bdoi):
    write_quartiers(store)

    assert run(store).success

    path = join(
        store.partition_dir("neighborhood_buildings", DATE, NEIGHBORHOOD),
        BUILDINGS_FILE,
    )
    frame = gpd.read_parquet(path)
    assert len(frame) == 1
    assert frame["link_id"].tolist() == [1]
    # The partition keys travel as columns, since the path holds bare values.
    assert set(frame["neighborhood"]) == {NEIGHBORHOOD}
    assert set(frame["scrape_date"]) == {DATE}
    assert frame.crs == "EPSG:4326"


def test_the_two_extracts_are_concatenated_before_the_clip(store, tmp_path, monkeypatch):
    """Both files intersect the borough, so both must contribute a row."""
    stub_bdoi(
        tmp_path,
        monkeypatch,
        inside=gpd.GeoDataFrame(
            {"link_id": [1]},
            geometry=[box(-73.6, 45.5, -73.55, 45.55)],
            crs="EPSG:4326",
        ),
        outside=gpd.GeoDataFrame(
            {"link_id": [2]},
            geometry=[box(-73.55, 45.52, -73.5, 45.57)],
            crs="EPSG:4326",
        ),
    )
    write_quartiers(store)

    assert run(store).success

    path = join(
        store.partition_dir("neighborhood_buildings", DATE, NEIGHBORHOOD),
        BUILDINGS_FILE,
    )
    frame = gpd.read_parquet(path)
    assert sorted(frame["link_id"].tolist()) == [1, 2]


def test_no_intersecting_building_fails_with_what_it_means(store, tmp_path, monkeypatch):
    write_quartiers(store)
    stub_bdoi(
        tmp_path,
        monkeypatch,
        inside=gpd.GeoDataFrame(
            {"link_id": [1]}, geometry=[box(10, 10, 11, 11)], crs="EPSG:4326"
        ),
        outside=gpd.GeoDataFrame(
            {"link_id": [2]}, geometry=[box(20, 20, 21, 21)], crs="EPSG:4326"
        ),
    )

    with pytest.raises(Failure, match="No BDOI building intersects"):
        run(store)


def test_a_missing_upstream_boundary_fails_with_what_to_run(store, bdoi):
    with pytest.raises(Failure, match="materialize reference_neighborhoods"):
        run(store)


def test_a_rerun_replaces_the_previous_snapshot(store, bdoi):
    write_quartiers(store)
    partition_dir = store.partition_dir("neighborhood_buildings", DATE, NEIGHBORHOOD)
    stale = Path(join(partition_dir, "buildings_retired.parquet"))
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"")

    run(store)

    assert not stale.exists()
