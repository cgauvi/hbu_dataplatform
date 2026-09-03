"""Offline tests for the street network: the island-wide snapshot and the
borough slice cut out of it.

Nothing here touches the network - the CKAN payload and the GeoJSON are
canned, and both assets run against a temp directory through
`dagster.materialize`.

The geometry is deliberately small and rectilinear, and sits where Montreal
does, because `neighborhood_streets` measures in EPSG:32188 and a shape
somewhere else would project to numbers that mean nothing.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import geopandas as gpd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import LineString, MultiLineString, Polygon, box

from asset_helpers import materialization_metadata

from urban_rag.frames import write_frame
from urban_rag.open_data import CkanClient
from urban_rag.open_data_assets import (
    QUARTIERS_FILE,
    STREETS_DATASET,
    STREETS_FILE,
    STREET_ID_COLUMN,
    reference_neighborhoods,
    street_network,
)
from urban_rag.resources import OpenDataResource, ParquetStore, PostgisResource
from urban_rag.storage import join
from urban_rag import street_assets
from urban_rag.street_assets import STREETS_FILE_OUT, neighborhood_streets

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
#: `partitions.NEIGHBORHOOD_BOROUGH_CODES["VSMPE"]` - what `borough_boundary`
#: cuts the reference layer on.
BOROUGH_CODE = "25"
DOWNLOAD_BASE = "https://donnees.montreal.ca/dataset/abc/resource/def/download"

#: A square kilometre of "borough", give or take, in the middle of the island.
BOROUGH = box(-73.630, 45.540, -73.620, 45.550)

PACKAGE_PAYLOAD = {
    "success": True,
    "result": {
        "name": STREETS_DATASET,
        "title": "Géobase double - côtés de rue du réseau routier",
        "license_title": "Creative Commons Attribution 4.0 International",
        "resources": [
            {
                "id": "16f7fa0a",
                "name": "Géobase double",
                "format": "JSON",
                "url": f"{DOWNLOAD_BASE}/gbdouble.json",
                "last_modified": "2026-08-20T18:51:00",
            },
            {
                "id": "9acb6c57",
                "name": "Géobase double",
                "format": "ZIP",
                "url": f"{DOWNLOAD_BASE}/gbdouble.zip",
                "last_modified": "2026-08-20T18:51:00",
            },
        ],
    },
}


def side(cote_rue_id: int, name: str, coordinates: list[list[float]]) -> dict:
    """One street side, spelled the way the city spells it."""
    return {
        "type": "Feature",
        "properties": {
            "COTE_RUE_ID": cote_rue_id,
            "ID_TRC": cote_rue_id // 10,
            "NOM_VOIE": name,
            "NOM_VILLE": "MTL",
            "COTE": "Gauche",
            "TYPE_F": "rue",
        },
        "geometry": {"type": "MultiLineString", "coordinates": [coordinates]},
    }


#: Three sides: one wholly inside the borough, one running out through its
#: eastern edge, and one entirely outside it.
INSIDE = side(1, "Jarry", [[-73.628, 45.545], [-73.624, 45.545]])
STRADDLING = side(2, "Papineau", [[-73.624, 45.547], [-73.616, 45.547]])
OUTSIDE = side(3, "Saint-Denis", [[-73.610, 45.547], [-73.605, 45.547]])


def geojson(*features: dict) -> bytes:
    return json.dumps({"type": "FeatureCollection", "features": list(features)}).encode(
        "utf-8"
    )


class FakeResponse:
    def __init__(self, content: bytes, *, content_type="application/json"):
        self.content = content
        self.headers = {"Content-Type": content_type}
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return json.loads(self.content)


class FakeSession:
    """Replays the canned package and one canned download."""

    def __init__(self, content: bytes | None = None):
        self.content = content if content is not None else geojson(INSIDE, STRADDLING)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params or {}))
        if url.endswith("package_show"):
            return FakeResponse(json.dumps(PACKAGE_PAYLOAD).encode("utf-8"))
        return FakeResponse(self.content, content_type="application/json")


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_quartiers(store, *, geometry=None, code=BOROUGH_CODE):
    """The reference layer, as `reference_neighborhoods` writes it."""
    frame = gpd.GeoDataFrame(
        {"no_qr": ["01"], "no_arr": [code], "nom_qr": ["Villeray"]},
        geometry=[geometry if geometry is not None else BOROUGH],
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(reference_neighborhoods.key.path[-1], DATE),
            QUARTIERS_FILE,
        ),
    )


# -- bronze: street_network ------------------------------------------------


def materialize_bronze(store, monkeypatch, *, session=None, scrape_date=DATE):
    """Run `street_network` with the portal stubbed out.

    Patched on the class rather than on an instance: Dagster rebuilds the
    resource from its config before the run, so an instance attribute would not
    survive into the asset.
    """
    session = session or FakeSession()
    monkeypatch.setattr(
        OpenDataResource,
        "client",
        lambda self: CkanClient(
            "https://portal", request_delay_seconds=0, session=session
        ),
    )
    return materialize(
        [street_network],
        partition_key=scrape_date,
        resources={"open_data": OpenDataResource(), "store": store},
    )


def test_the_snapshot_lands_under_the_date_partition(store, monkeypatch, tmp_path):
    result = materialize_bronze(store, monkeypatch)

    assert result.success
    path = tmp_path / "store" / "bronze" / "street_network" / DATE / STREETS_FILE
    frame = gpd.read_parquet(path)
    assert len(frame) == 2
    assert frame.crs.to_string() == "EPSG:4326"
    # The prefix carries bare values, so the date has to travel as a column.
    assert frame["scrape_date"].tolist() == [DATE, DATE]
    assert {"source_file", "scraped_at"} <= set(frame.columns)


def test_the_publishers_column_names_survive_bronze(store, monkeypatch, tmp_path):
    """Unlike `reference_neighborhoods`, which lower-cases to join its two files.

    `COTE_RUE_ID` is the key silver declares its grain on and the one loaded
    into `rag.streets.cote_rue_id`; renaming it here would break both.
    """
    materialize_bronze(store, monkeypatch)

    frame = gpd.read_parquet(
        tmp_path / "store" / "bronze" / "street_network" / DATE / STREETS_FILE
    )
    assert STREET_ID_COLUMN in frame.columns
    assert "cote_rue_id" not in frame.columns
    assert {"NOM_VOIE", "COTE", "TYPE_F"} <= set(frame.columns)


def test_bronze_metadata_reports_the_counts_and_the_licence(store, monkeypatch):
    result = materialize_bronze(store, monkeypatch)

    metadata = materialization_metadata(result, street_network)
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_street_sides"].value == 2
    assert metadata["num_street_ids"].value == 2
    assert metadata["num_street_names"].value == 2
    assert metadata["num_invalid_geometries"].value == 0
    assert "Creative Commons" in metadata["license"].value


def test_a_layer_without_the_street_id_is_refused(store, monkeypatch):
    """The key everything downstream is grained on; a snapshot without it is
    not a snapshot of this layer."""
    renamed = json.loads(geojson(INSIDE))
    renamed["features"][0]["properties"].pop("COTE_RUE_ID")

    with pytest.raises(Failure, match=STREET_ID_COLUMN):
        materialize_bronze(
            store, monkeypatch, session=FakeSession(json.dumps(renamed).encode("utf-8"))
        )


def test_a_bronze_rerun_replaces_the_previous_snapshot(store, monkeypatch, tmp_path):
    partition = tmp_path / "store" / "bronze" / "street_network" / DATE
    partition.mkdir(parents=True)
    stale = partition / "gbdouble_retired.parquet"
    gpd.GeoDataFrame(
        {"a": [1]}, geometry=[LineString([(0, 0), (1, 1)])], crs="EPSG:4326"
    ).to_parquet(stale)

    materialize_bronze(store, monkeypatch)

    assert not stale.exists()
    assert (partition / STREETS_FILE).exists()


# -- silver: neighborhood_streets ------------------------------------------


def write_bronze(store, *features, scrape_date=DATE):
    """The island-wide snapshot, as `street_network` writes it."""
    if not features:
        features = (INSIDE, STRADDLING, OUTSIDE)
    payload = json.loads(geojson(*features))
    frame = gpd.GeoDataFrame(
        [feature["properties"] for feature in payload["features"]],
        geometry=[
            LineString(feature["geometry"]["coordinates"][0])
            for feature in payload["features"]
        ],
        crs="EPSG:4326",
    )
    frame["scrape_date"] = scrape_date
    write_frame(
        frame,
        join(
            store.partition_dir(street_network.key.path[-1], scrape_date), STREETS_FILE
        ),
    )


@pytest.fixture(autouse=True)
def stub_postgis(monkeypatch):
    """The upsert into `silver.neighborhood_streets`, recorded rather than run.

    The asset publishes the same frame it writes to the tree, which needs a
    database; every test here is about the cut, so the load is stubbed and the
    frame it was handed is kept for the two tests that do care.
    """
    seen: dict[str, object] = {}

    @contextmanager
    def connect(self):
        yield object()

    def load_streets(connection, frame, *, neighborhood, scrape_date):
        seen["frame"] = frame
        seen["partition"] = (neighborhood, scrape_date)
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
        partition_key=MultiPartitionKey(
            {"date": DATE, "neighborhood": NEIGHBORHOOD}
        ),
        resources={"store": store, "postgis": PostgisResource()},
    )


def read_silver(tmp_path) -> gpd.GeoDataFrame:
    return gpd.read_parquet(
        tmp_path
        / "store"
        / "silver"
        / "neighborhood_streets"
        / DATE
        / NEIGHBORHOOD
        / STREETS_FILE_OUT
    )


def test_only_the_sides_reaching_the_borough_are_kept(store, tmp_path):
    write_quartiers(store)
    write_bronze(store)

    assert materialize_silver(store).success

    frame = read_silver(tmp_path)
    # `Saint-Denis` is a kilometre east of the boundary.
    assert sorted(frame[STREET_ID_COLUMN].astype(int)) == [1, 2]


def test_a_side_crossing_the_boundary_is_cut_at_it(store, tmp_path):
    write_quartiers(store)
    write_bronze(store)

    materialize_silver(store)

    frame = read_silver(tmp_path).set_index(STREET_ID_COLUMN)
    inside = frame.loc[1]
    straddling = frame.loc[2]
    # Wholly inside: nothing was taken off it.
    assert inside["length_in_borough_m"] == pytest.approx(
        inside["segment_length_m"], rel=1e-9
    )
    assert inside["pct_in_borough"] == pytest.approx(100.0, abs=0.01)
    # Half in, half out: the published length survives as its own column so the
    # cut is visible rather than silently making the street shorter.
    assert straddling["length_in_borough_m"] < straddling["segment_length_m"]
    assert 45.0 < straddling["pct_in_borough"] < 55.0
    # Clipped, not selected: the geometry stops at the boundary.
    assert straddling.geometry.within(BOROUGH.buffer(1e-9))


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
    assert set(frame["neighborhood"]) == {NEIGHBORHOOD}
    assert set(frame["scrape_date"]) == {DATE}
    assert frame.crs.to_string() == "EPSG:4326"


def test_silver_metadata_reports_the_cut(store):
    write_quartiers(store)
    write_bronze(store)

    metadata = materialization_metadata(materialize_silver(store), neighborhood_streets)

    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_street_sides"].value == 2
    assert metadata["num_street_sides_island_wide"].value == 3
    assert metadata["num_streets_named"].value == 2
    # One of the two straddles the boundary; the other is wholly inside.
    assert metadata["num_boundary_clipped"].value == 1
    assert metadata["num_invalid_geometries"].value == 0
    assert metadata["total_length_km"].value > 0


def test_a_side_that_only_grazes_the_boundary_is_dropped(store, tmp_path):
    """It intersects, and clips to a point. A point is not a street inside the
    borough, and a zero-length row would be one."""
    grazing = side(4, "Grazing", [[-73.620, 45.545], [-73.615, 45.545]])
    write_quartiers(store)
    write_bronze(store, INSIDE, grazing)

    assert materialize_silver(store).success

    frame = read_silver(tmp_path)
    assert sorted(frame[STREET_ID_COLUMN].astype(int)) == [1]


def test_the_same_side_arriving_twice_is_refused(store):
    """One row per COTE_RUE_ID is the grain; a duplicate would multiply every
    frontage pair the join downstream produces."""
    write_quartiers(store)
    write_bronze(store, INSIDE, side(1, "Jarry", [[-73.627, 45.546], [-73.625, 45.546]]))

    with pytest.raises(Failure, match="appear more than once"):
        materialize_silver(store)


def test_a_borough_no_street_reaches_is_a_failure(store):
    """Not an empty partition: an empty street layer for a borough is a broken
    boundary or a broken snapshot, never a borough with no streets."""
    write_quartiers(store, geometry=Polygon.from_bounds(-73.50, 45.60, -73.49, 45.61))
    write_bronze(store)

    with pytest.raises(Failure, match="No street side intersects"):
        materialize_silver(store)


def test_a_missing_snapshot_names_the_asset_to_run(store):
    write_quartiers(store)

    with pytest.raises(Failure, match="materialize street_network"):
        materialize_silver(store)


def test_a_missing_boundary_names_the_asset_to_run(store):
    write_bronze(store)

    with pytest.raises(Failure, match="materialize reference_neighborhoods"):
        materialize_silver(store)


def test_a_silver_rerun_replaces_the_previous_partition(store, tmp_path):
    partition = (
        tmp_path / "store" / "silver" / "neighborhood_streets" / DATE / NEIGHBORHOOD
    )
    partition.mkdir(parents=True)
    stale = partition / "neighborhood_streets_retired.parquet"
    gpd.GeoDataFrame(
        {"a": [1]}, geometry=[LineString([(0, 0), (1, 1)])], crs="EPSG:4326"
    ).to_parquet(stale)
    write_quartiers(store)
    write_bronze(store)

    materialize_silver(store)

    assert not stale.exists()
    assert (partition / STREETS_FILE_OUT).exists()


def test_the_helper_that_keeps_only_linework(store):
    """`_lines_only` is what stops a graze from becoming a zero-length street."""
    from shapely.geometry import GeometryCollection, Point

    from urban_rag.street_assets import _lines_only

    line = LineString([(0, 0), (1, 0)])
    assert _lines_only(line) is line
    assert _lines_only(Point(0, 0)) is None
    assert _lines_only(GeometryCollection([Point(0, 0)])) is None
    mixed = GeometryCollection([Point(2, 2), line])
    assert _lines_only(mixed).geom_type in ("LineString", "MultiLineString")


def test_a_failure_message_carries_the_partition(store):
    """Every guard in this asset names the borough and the date, because a
    backfill fails one partition at a time and the message is what says which."""
    write_quartiers(store)
    write_bronze(store, INSIDE, side(1, "Jarry", [[-73.627, 45.546], [-73.625, 45.546]]))

    with pytest.raises(Failure, match=f"{NEIGHBORHOOD} {DATE}"):
        materialize_silver(store)


def test_load_streets_promotes_every_side_to_multi(monkeypatch):
    """The column is `geometry(MultiLineString, 4326)`, and a typmod rejects a
    bare `LineString` rather than promoting it - so `load_streets` promotes.

    Direct, because the asset's own tests stub the load away: this helper only
    ever runs against a real database, which is where the promotion failing
    would first be seen.
    """
    from urban_rag import postgis, warehouse

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
        crs="EPSG:4326",
    )

    postgis.load_streets(
        object(), frame, neighborhood=NEIGHBORHOOD, scrape_date=DATE
    )

    loaded = seen["frame"]
    assert set(loaded.geometry.geom_type) == {"MultiLineString"}
    # Measured geodesically here rather than in a projected CRS, so it agrees
    # with the `ST_Length(geography(...))` every other measure is stated in.
    assert (loaded["length_m"] > 0).all()
