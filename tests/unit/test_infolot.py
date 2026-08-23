"""Offline tests for the Infolot client and the lot asset it feeds.

What is under test is the two-phase read the service forces - ask for ids,
then fetch them by id - and the guard that makes a short batch an error rather
than a silently truncated partition.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
import shapely
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import Polygon, box

from urban_rag.infolot import (
    OBJECT_ID_FIELD,
    InfolotClient,
    InfolotError,
    esri_polygon,
    normalize_dates,
)
from urban_rag.frames import write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.open_data_assets import QUARTIERS_FILE, reference_neighborhoods
from urban_rag.resources import InfolotResource, ParquetStore
from urban_rag.storage import join

DATE = "2026-08-20"
NEIGHBORHOOD = "VSMPE"
#: The borough code `VSMPE` maps to in the reference layer.
BOROUGH_CODE = "25"


class FakeResponse:
    def __init__(self, payload, *, content_type="application/json", text=""):
        self._payload = payload
        self.headers = {"Content-Type": content_type}
        self.text = text
        self.status_code = 200

    def json(self):
        return self._payload


class FakeSession:
    """Replays one canned payload per call and records what was posted."""

    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls: list[tuple[str, dict]] = []

    def post(self, url, data=None, timeout=None):
        self.calls.append((url, data or {}))
        return FakeResponse(self.payloads.pop(0))


def make_client(payloads, **kwargs):
    session = FakeSession(payloads)
    client = InfolotClient(
        "https://example/MapServer",
        request_delay_seconds=0,
        session=session,
        **kwargs,
    )
    return client, session


def lot_feature(object_id, no_lot="1 000 000"):
    return {
        "type": "Feature",
        "properties": {
            OBJECT_ID_FIELD: object_id,
            "NO_LOT": no_lot,
            "VA_SUPRF_LOT_CALCL": 100.0,
            "DA_STATT_LOT": 1046754000000,
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-73.6, 45.5], [-73.6, 45.6], [-73.5, 45.6], [-73.6, 45.5]]],
        },
    }


# -- client ---------------------------------------------------------------


def test_lot_ids_posts_the_polygon_and_asks_only_for_ids():
    client, session = make_client([{"objectIds": [7, 8, 9]}])

    assert client.lot_ids(esri_polygon(box(0, 0, 1, 1))) == [7, 8, 9]

    url, data = session.calls[0]
    assert url == "https://example/MapServer/3/query"
    assert data["returnIdsOnly"] == "true"
    assert data["geometryType"] == "esriGeometryPolygon"
    assert data["spatialRel"] == "esriSpatialRelIntersects"
    # `maxRecordCount` does not apply to this call, so no paging is attempted.
    assert "resultOffset" not in data


def test_fetch_lots_batches_by_object_id():
    payloads = [
        {"features": [lot_feature(1), lot_feature(2)]},
        {"features": [lot_feature(3)]},
    ]
    client, session = make_client(payloads, batch_size=2)

    features = list(client.fetch_lots([1, 2, 3]))

    assert len(features) == 3
    assert [data["objectIds"] for _, data in session.calls] == ["1,2", "3"]
    # GeoJSON, so the geometry needs no ring conversion on this side.
    assert all(data["f"] == "geojson" for _, data in session.calls)
    assert all(data["outSR"] == "4326" for _, data in session.calls)


def test_fetch_lots_rejects_a_short_batch():
    """The whole point of fetching by id: a short answer is data loss."""
    client, _ = make_client([{"features": [lot_feature(1)]}], batch_size=2)

    with pytest.raises(InfolotError, match="Asked for 2 lots by id, got 1"):
        list(client.fetch_lots([1, 2]))


def test_service_errors_arrive_with_http_200():
    client, _ = make_client([{"error": {"message": "Invalid or missing input"}}])

    with pytest.raises(InfolotError, match="Invalid or missing input"):
        client.lot_ids(esri_polygon(box(0, 0, 1, 1)))


def test_esri_polygon_carries_holes_and_multipolygon_parts():
    ring = box(0, 0, 10, 10)
    hole = box(2, 2, 4, 4)
    with_hole = Polygon(ring.exterior.coords, [hole.exterior.coords])
    multi = shapely.union_all([with_hole, box(20, 20, 21, 21)])

    payload = esri_polygon(multi, srs=4326)

    # Two exteriors plus the one interior.
    assert len(payload["rings"]) == 3
    assert payload["spatialReference"] == {"wkid": 4326}


def test_esri_polygon_rejects_an_empty_geometry():
    with pytest.raises(ValueError, match="empty polygon"):
        esri_polygon(Polygon())


def test_normalize_dates_turns_epoch_milliseconds_into_timestamps():
    frame = normalize_dates(pd.DataFrame({"DA_STATT_LOT": [1046754000000, None]}))

    assert str(frame["DA_STATT_LOT"].dt.date[0]) == "2003-03-04"
    assert pd.isna(frame["DA_STATT_LOT"][1])


# -- asset ----------------------------------------------------------------


class FakeInfolotClient:
    """Returns every lot it is given, ignoring the query geometry."""

    def __init__(self, features):
        self.features = features
        self.queried_with = None

    def lot_ids(self, geometry, *, in_srs=4326):
        self.queried_with = geometry
        return [f["properties"][OBJECT_ID_FIELD] for f in self.features]

    def fetch_lots(self, object_ids, *, target_srs=4326):
        wanted = set(object_ids)
        return (
            f for f in self.features if f["properties"][OBJECT_ID_FIELD] in wanted
        )


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path))


@pytest.fixture
def infolot(monkeypatch):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    client = FakeInfolotClient([lot_feature(1, "1 000 001"), lot_feature(2, "1 000 002")])
    monkeypatch.setattr(InfolotResource, "client", lambda self: client)
    return client


def write_quartiers(store, *, code=BOROUGH_CODE):
    """The upstream boundary the asset cuts its query geometry from."""
    path = join(
        store.partition_dir(reference_neighborhoods.key.path[-1], DATE), QUARTIERS_FILE
    )
    frame = gpd.GeoDataFrame(
        {"no_qr": ["01"], "no_arr": [code]},
        geometry=[box(-73.7, 45.4, -73.4, 45.7)],
        crs="EPSG:4326",
    )
    write_frame(frame, path)


def run(store):
    return materialize(
        [neighborhood_lots],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"infolot": InfolotResource(), "store": store},
        selection=[neighborhood_lots],
    )


def test_lots_land_under_date_then_neighborhood(store, infolot):
    write_quartiers(store)

    assert run(store).success

    path = join(
        store.partition_dir("neighborhood_lots", DATE, NEIGHBORHOOD), LOTS_FILE
    )
    frame = gpd.read_parquet(path)
    assert len(frame) == 2
    # The partition keys travel as columns, since the path holds bare values.
    assert set(frame["neighborhood"]) == {NEIGHBORHOOD}
    assert set(frame["scrape_date"]) == {DATE}
    assert frame.crs == "EPSG:4326"


def test_the_query_geometry_is_the_borough_boundary(store, infolot):
    write_quartiers(store)

    run(store)

    rings = infolot.queried_with["rings"]
    assert shapely.Polygon(rings[0]).bounds == pytest.approx(
        (-73.7, 45.4, -73.4, 45.7)
    )


def test_a_missing_upstream_boundary_fails_with_what_to_run(store, infolot):
    with pytest.raises(Failure, match="materialize reference_neighborhoods"):
        run(store)


def test_a_borough_absent_from_the_reference_layer_fails(store, infolot):
    write_quartiers(store, code="99")

    with pytest.raises(Failure, match="No reference neighborhood carries"):
        run(store)
