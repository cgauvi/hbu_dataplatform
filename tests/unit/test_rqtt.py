"""Offline tests for the RQTT client.

Nothing here touches the network. The province-wide archive is stubbed by
writing a tiny real GeoPackage - the road layer named the way the MRNF names
it, in the MTQ Lambert the MRNF publishes it in - and zipping it under the
`OGC(GPKG)/` folder the real archive nests it in. What is under test is GDAL's
own GeoPackage reading, the bbox push-down, the reprojection to WGS84, and the
one thing this source needs that the assessment roll does not: a cache keyed on
a vintage the server reports rather than on a filename the URL states.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import geopandas as gpd
import pytest
import requests
from shapely.geometry import LineString

from hbu_dataplatform.sources.rqtt.client import (
    CACHE_NAME_TEMPLATE,
    PUBLISHED_CRS,
    ROAD_LAYER,
    STREET_ID_FIELD,
    STREET_NAME_FIELD,
    WGS84,
    RqttError,
    RqttFetcher,
    bbox_in_source_crs,
    layer_names,
    read_layer,
    version_from_last_modified,
)

#: The vintage the fake server publishes, and the header it says it in.
VERSION = "20260703"
LAST_MODIFIED = "Fri, 03 Jul 2026 17:15:26 GMT"
ARCHIVE = CACHE_NAME_TEMPLATE.format(version=VERSION)

#: Two segments in Montreal and one in Quebec City, in WGS84 - written to the
#: fixture in `PUBLISHED_CRS`, because that is what the archive is in and the
#: bbox push-down is only meaningful against the published projection.
MONTREAL_SEGMENTS = [
    ("rue-jarry", "Rue Jarry", "Locale", [(-73.62, 45.54), (-73.61, 45.54)]),
    ("rue-chabot", "Rue Chabot", "Locale", [(-73.615, 45.535), (-73.615, 45.545)]),
]
QUEBEC_SEGMENT = (
    "boul-charest",
    "Boulevard Charest Est",
    "Collectrice",
    [(-71.23, 46.81), (-71.22, 46.81)],
)


def road_layer(rows) -> gpd.GeoDataFrame:
    """The road network as the archive publishes it: MTQ Lambert, its columns."""
    frame = gpd.GeoDataFrame(
        {
            STREET_ID_FIELD: [row[0] for row in rows],
            STREET_NAME_FIELD: [row[1] for row in rows],
            "ClsRte": [row[2] for row in rows],
            "Gestion": ["Municipalité" for _ in rows],
            "geometry": [LineString(row[3]) for row in rows],
        },
        crs=WGS84,
    )
    return frame.to_crs(PUBLISHED_CRS)


def zipped_geopackage(directory: Path, roads: gpd.GeoDataFrame) -> Path:
    """Write the road layer as a real GeoPackage, zipped the way the RQTT is.

    The archive nests the GeoPackage under `OGC(GPKG)/`, which is what
    `_geopackage_member` has to find its way through, and ships the other
    transport networks beside it - `Route_Verte` here stands in for the eight
    layers this platform never reads.
    """
    directory.mkdir(parents=True, exist_ok=True)
    gpkg = directory / "RQTT.gpkg"
    roads.to_file(gpkg, layer=ROAD_LAYER, driver="GPKG")
    roads.to_file(gpkg, layer="Route_Verte", driver="GPKG", mode="a")

    zip_path = directory / ARCHIVE
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(gpkg, f"OGC(GPKG)/{gpkg.name}")
    gpkg.unlink()
    return zip_path


class FakeResponse:
    def __init__(self, content: bytes, *, content_type="application/zip", headers=None):
        self.content = content
        self.headers = {"Content-Type": content_type, **(headers or {})}
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
    """Replays canned archive bytes, and answers HEAD with a vintage.

    ``last_modified=None`` is the server that does not say, and ``head_error``
    the one that cannot be reached at all - the two cases `fetch` has to fall
    back from rather than guess through.
    """

    def __init__(self, content: bytes, *, last_modified=LAST_MODIFIED, head_error=False):
        self.content = content
        self.last_modified = last_modified
        self.head_error = head_error
        self.calls: list[str] = []

    def head(self, url, timeout=None, allow_redirects=True):
        self.calls.append(f"HEAD {url}")
        if self.head_error:
            raise requests.ConnectionError("no route to host")
        headers = {}
        if self.last_modified:
            headers["Last-Modified"] = self.last_modified
        return FakeResponse(b"", headers=headers)

    def get(self, url, timeout=None, stream=False):
        self.calls.append(f"GET {url}")
        return FakeResponse(self.content)


def fetcher_for(tmp_path: Path, archive: Path, **kwargs) -> tuple[RqttFetcher, FakeSession]:
    session = FakeSession(archive.read_bytes(), **kwargs)
    return (
        RqttFetcher(
            cache_dir=tmp_path / "cache",
            base_url="https://example/rqtt",
            request_delay_seconds=0,
            session=session,
        ),
        session,
    )


@pytest.fixture
def archive(tmp_path) -> Path:
    return zipped_geopackage(
        tmp_path / "src", road_layer(MONTREAL_SEGMENTS + [QUEBEC_SEGMENT])
    )


# -- the vintage ------------------------------------------------------------


def test_last_modified_becomes_the_cache_stamp():
    assert version_from_last_modified(LAST_MODIFIED) == "20260703"


def test_an_unparseable_last_modified_is_an_error():
    with pytest.raises(RqttError, match="not a date"):
        version_from_last_modified("whenever")


def test_fetch_asks_the_server_which_vintage_is_current(tmp_path, archive):
    fetcher, session = fetcher_for(tmp_path, archive)

    path, version = fetcher.fetch()

    assert version == VERSION
    assert path == tmp_path / "cache" / ARCHIVE
    # The HEAD comes first: it is what decides whether 390 MB has to move.
    assert session.calls[0].startswith("HEAD ")


def test_fetch_downloads_once_and_reuses_the_vintage(tmp_path, archive):
    fetcher, session = fetcher_for(tmp_path, archive)

    fetcher.fetch()
    downloads = [call for call in session.calls if call.startswith("GET ")]
    assert len(downloads) == 1

    # A second fetch still asks which vintage is current - that is the check
    # that a reissue is noticed - but moves no bytes when it is the cached one.
    fetcher.fetch()
    assert [call for call in session.calls if call.startswith("GET ")] == downloads


def test_a_new_vintage_lands_beside_the_old_one(tmp_path, archive):
    fetcher, session = fetcher_for(tmp_path, archive)
    fetcher.fetch()

    session.last_modified = "Tue, 01 Dec 2026 09:00:00 GMT"
    path, version = fetcher.fetch()

    assert version == "20261201"
    assert path.name == CACHE_NAME_TEMPLATE.format(version="20261201")
    # The July copy is still there: a vintage is not overwritten, so a
    # partition built from it can still say which bytes it read.
    assert (tmp_path / "cache" / ARCHIVE).exists()
    assert fetcher.cached_versions() == ("20260703", "20261201")


def test_a_server_that_states_no_vintage_is_an_error(tmp_path, archive):
    fetcher, _ = fetcher_for(tmp_path, archive, last_modified=None)

    with pytest.raises(RqttError, match="no Last-Modified"):
        fetcher.fetch()


def test_an_unreachable_head_falls_back_to_the_newest_cached_vintage(
    tmp_path, archive
):
    fetcher, session = fetcher_for(tmp_path, archive)
    fetcher.fetch()

    session.head_error = True
    path, version = fetcher.fetch()

    assert version == VERSION
    assert path == tmp_path / "cache" / ARCHIVE


def test_an_unreachable_head_with_nothing_cached_raises(tmp_path, archive):
    fetcher, _ = fetcher_for(tmp_path, archive, head_error=True)

    with pytest.raises(RqttError):
        fetcher.fetch()


def test_a_non_zip_response_leaves_no_cache_entry(tmp_path, archive):
    fetcher, session = fetcher_for(tmp_path, archive)
    session.content = b"<html>404 not found</html>"

    with pytest.raises(RqttError, match="not a zip"):
        fetcher.fetch()
    # The half-written file must not be left where the next run would trust it.
    assert not (tmp_path / "cache" / ARCHIVE).exists()
    assert not list((tmp_path / "cache").glob("*.part"))


# -- unpacking and reading --------------------------------------------------


def test_geopackage_unpacks_once_and_carries_the_vintage(tmp_path, archive):
    fetcher, _ = fetcher_for(tmp_path, archive)

    path, version = fetcher.geopackage()

    assert version == VERSION
    # Named for the vintage, so two vintages cannot serve each other's bytes.
    assert path.name == f"RQTT_{VERSION}.gpkg"
    assert path.exists() and path.stat().st_size
    assert not list((tmp_path / "cache").glob("*.part"))

    stamp = path.stat().st_mtime_ns
    fetcher.geopackage()
    assert path.stat().st_mtime_ns == stamp


def test_the_road_layer_is_one_of_the_layers_published(tmp_path, archive):
    fetcher, _ = fetcher_for(tmp_path, archive)
    path, _ = fetcher.geopackage()

    assert ROAD_LAYER in layer_names(path)


def test_read_layer_comes_back_in_wgs84(tmp_path, archive):
    fetcher, _ = fetcher_for(tmp_path, archive)
    path, _ = fetcher.geopackage()

    frame = read_layer(path)

    assert frame.crs.to_epsg() == 4326
    assert len(frame) == 3
    assert set(frame[STREET_NAME_FIELD]) == {
        "Rue Jarry",
        "Rue Chabot",
        "Boulevard Charest Est",
    }


def test_a_bbox_keeps_the_segments_inside_it(tmp_path, archive):
    fetcher, _ = fetcher_for(tmp_path, archive)
    path, _ = fetcher.geopackage()

    montreal = bbox_in_source_crs((-73.7, 45.4, -73.5, 45.6))
    frame = read_layer(path, bbox=montreal)

    # Quebec City is 250 km away and is never built, which is the whole point
    # of pushing the box into the R-tree rather than filtering afterwards.
    assert set(frame[STREET_NAME_FIELD]) == {"Rue Jarry", "Rue Chabot"}


def test_a_bbox_in_degrees_would_select_nothing(tmp_path, archive):
    """The mistake `bbox_in_source_crs` exists to make impossible.

    OGR reads the filter in the dataset's own CRS, so handing it lon/lat does
    not raise - it quietly matches nothing, which downstream reads like an
    empty borough rather than a wrong projection.
    """
    fetcher, _ = fetcher_for(tmp_path, archive)
    path, _ = fetcher.geopackage()

    assert read_layer(path, bbox=(-73.7, 45.4, -73.5, 45.6)).empty


def test_bbox_is_re_enveloped_after_projection():
    """A lon/lat rectangle bends in Lambert, so the corners are re-bounded."""
    bounds = (-73.7, 45.4, -73.4, 45.7)
    minx, miny, maxx, maxy = bbox_in_source_crs(bounds)

    assert minx < maxx and miny < maxy
    # Projected metres, not degrees - the failure this guards is handing the
    # input straight back.
    assert abs(minx) > 1000 and abs(miny) > 1000


def test_an_archive_without_a_geopackage_is_an_error(tmp_path):
    empty = tmp_path / "src" / ARCHIVE
    empty.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(empty, "w") as handle:
        handle.writestr("OGC(GPKG)/readme.txt", b"no geopackage here")
    fetcher, _ = fetcher_for(tmp_path, empty)

    with pytest.raises(RqttError, match="no .gpkg member"):
        fetcher.geopackage()
