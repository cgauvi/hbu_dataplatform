"""Offline tests for Adresses Québec: the parser, the client's paging, the
spatial filter it builds, and the bronze asset that cuts a borough out of it.

The network is stubbed at the session, so what is under test is the paging
contract, the Esri ring winding, the parse of `AdresseFormatee`, and the clip
down to one borough - not the provincial server.

The parser cases are real rows off the service, kept verbatim because every one
of them cost a rewrite to learn: the suffix that is spaced, the suffix that is
a fraction, and the parenthesised municipality that is not part of the street.
"""

from __future__ import annotations

import json

import geopandas as gpd
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import Point, box

from urban_rag.address_assets import (
    ADDRESSES_FILE,
    neighborhood_addresses,
    parse_address_frame,
)
from urban_rag.adresses_quebec import (
    ADDRESS_FIELDS,
    MAX_FILTER_VERTICES,
    AdressesQuebecClient,
    AdressesQuebecError,
    civic_address,
    esri_query_geometry,
    features_to_frame,
    parse_addresses,
    parse_formatted_address,
)
from urban_rag.frames import write_frame
from urban_rag.open_data_assets import QUARTIERS_FILE, reference_neighborhoods
from urban_rag.resources import AdressesQuebecResource, ParquetStore
from urban_rag.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
BOROUGH_CODE = "25"


# -- the parser ---------------------------------------------------------------


@pytest.mark.parametrize(
    "formatted,expected",
    [
        (
            "7460 Rue Cartier, Montréal H2E2J5",
            {
                "unit": None,
                "civic": "7460",
                "suffix": None,
                "street": "Rue Cartier",
                "municipality": "Montréal",
                "postal_code": "H2E2J5",
            },
        ),
        # The unit prefix - roughly half a dense borough carries one.
        (
            "204-7430 Rue Lajeunesse, Montréal H2R2H8",
            {"unit": "204", "civic": "7430", "street": "Rue Lajeunesse"},
        ),
        # A unit that is not a number at all.
        ("PH4-5785 Rue Boyer, Montréal H2S2H7", {"unit": "PH4", "civic": "5785"}),
        ("A-6700 Rue Boyer, Montréal H2S2J7", {"unit": "A", "civic": "6700"}),
        # The suffix, spaced. Parsed as a suffix and *not* as the first word of
        # the street, which is the bug this case exists to catch.
        (
            "7390 A Rue De Lanaudière, Montréal H2E1Y4",
            {"civic": "7390", "suffix": "A", "street": "Rue De Lanaudière"},
        ),
        # The same suffix, unspaced, with a unit in front of it.
        (
            "336B-8755 Rue Saint-Hubert, Montréal H2M0A2",
            {"unit": "336B", "civic": "8755", "street": "Rue Saint-Hubert"},
        ),
        # Old Québec numbers houses in halves and quarters, and the publisher
        # puts the fraction in the suffix column.
        (
            "11 1/2 Rue Hébert, Québec G1R3T5",
            {"civic": "11", "suffix": "1/2", "street": "Rue Hébert"},
        ),
        (
            "4-26 1/2 Rue des Jardins, Québec G1R4L5",
            {"unit": "4", "civic": "26", "suffix": "1/2", "street": "Rue des Jardins"},
        ),
        # The parenthesis disambiguates a street name that repeats elsewhere in
        # the province; it is not part of the street.
        (
            "8635 12e Avenue (Montréal), Montréal H1Z3J1",
            {
                "civic": "8635",
                "street": "12e Avenue",
                "disambiguation": "Montréal",
                "municipality": "Montréal",
            },
        ),
        # A street whose name begins with a number, after a civic number.
        ("8386 14e Avenue, Montréal H1Z3M3", {"civic": "8386", "street": "14e Avenue"}),
        ("46 1/2 Côte de la Fabrique, Québec G1R3V7", {"street": "Côte de la Fabrique"}),
    ],
)
def test_the_formatted_address_comes_apart(formatted, expected):
    parsed = parse_formatted_address(formatted)
    assert parsed is not None, formatted
    for field, value in expected.items():
        assert parsed[field] == value, f"{field} of {formatted!r}"


def test_a_string_of_another_shape_is_refused_rather_than_half_read():
    """Half a parse would report a street name that is really a street and a
    number, with nothing on the row saying so."""
    assert parse_formatted_address("not an address at all") is None
    assert parse_formatted_address("") is None
    assert parse_formatted_address(None) is None


def test_the_vectorized_parse_matches_the_scalar_one_and_nulls_the_rest():
    series = pd.Series(
        [
            "7390 A Rue De Lanaudière, Montréal H2E1Y4",
            "204-7430 Rue Lajeunesse, Montréal H2R2H8",
            "nonsense",
        ],
        dtype="string",
    )

    frame = parse_addresses(series)

    assert frame["street"].tolist()[:2] == ["Rue De Lanaudière", "Rue Lajeunesse"]
    assert frame["suffix"][0] == "A"
    assert frame["unit"][1] == "204"
    # The row that did not match comes back all-null rather than partly filled.
    assert frame.iloc[2].isna().all()


def test_the_civic_address_drops_the_unit_and_keeps_the_suffix():
    """Counting rows counts units; counting these counts doors."""
    assert civic_address(7430, None, "Rue Lajeunesse") == "7430 Rue Lajeunesse"
    assert civic_address(7390, "A", "Rue De Lanaudière") == "7390 A Rue De Lanaudière"
    assert civic_address(None, None, "Rue Boyer") is None


@pytest.mark.parametrize("null", [None, float("nan"), pd.NA, ""])
def test_a_null_suffix_is_left_out_rather_than_printed(null):
    """`str(pandas.NA)` is '<NA>', so the obvious guard puts the null *into*
    the address instead of leaving it out."""
    assert civic_address(7430, null, "Rue Lajeunesse") == "7430 Rue Lajeunesse"


@pytest.mark.parametrize("null", [None, float("nan"), pd.NA])
def test_an_address_with_no_street_is_still_the_number(null):
    """A row the parser could not read keeps its civic number rather than
    losing the only part of it the publisher stated outright."""
    assert civic_address(7430, None, null) == "7430"


# -- the spatial filter -------------------------------------------------------


def test_a_polygon_filter_is_wound_the_way_esri_reads_it():
    """Counter-clockwise is GeoJSON's rule and means *hole* to ArcGIS - which
    does not fail, it answers with everything outside the borough."""
    esri, kind, vertices = esri_query_geometry(box(-73.7, 45.4, -73.4, 45.7))

    assert kind == "esriGeometryPolygon"
    assert esri["spatialReference"] == {"wkid": 4326}
    ring = esri["rings"][0]
    assert vertices == len(ring)
    # The shoelace sum is positive for a clockwise ring in screen/Esri terms.
    area = sum(
        (ring[i][0] * ring[i + 1][1]) - (ring[i + 1][0] * ring[i][1])
        for i in range(len(ring) - 1)
    )
    assert area < 0, "exterior ring must be clockwise for ArcGIS"


def test_too_detailed_an_outline_falls_back_to_its_envelope():
    """Which only ever widens the query - the asset clips locally either way."""
    circle = Point(-73.6, 45.5).buffer(0.05, quad_segs=MAX_FILTER_VERTICES)

    esri, kind, vertices = esri_query_geometry(circle)

    assert kind == "esriGeometryEnvelope"
    assert vertices == 4
    assert set(esri) == {"xmin", "ymin", "xmax", "ymax", "spatialReference"}
    assert esri["xmin"] == pytest.approx(circle.bounds[0])


def test_a_non_polygon_is_sent_as_its_envelope():
    _, kind, _ = esri_query_geometry(Point(-73.6, 45.5))
    assert kind == "esriGeometryEnvelope"


# -- the client ---------------------------------------------------------------


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.headers = {"Content-Type": "application/json;charset=utf-8"}
        self.status_code = 200
        self.text = json.dumps(payload)

    def json(self):
        return self.payload


class FakeSession:
    """Answers a query out of a canned list of addresses, honouring paging."""

    def __init__(self, features, *, page_size=2):
        self.features = features
        self.page_size = page_size
        self.posts: list[dict] = []
        self.headers: dict[str, str] = {}
        self.verify = None

    def mount(self, *args, **kwargs):
        return None

    def post(self, url, data=None, timeout=None):
        self.posts.append(data)
        if data.get("returnCountOnly") == "true":
            return FakeResponse({"count": len(self.features)})
        offset = int(data.get("resultOffset", 0))
        count = int(data.get("resultRecordCount", self.page_size))
        page = self.features[offset : offset + count]
        return FakeResponse({"type": "FeatureCollection", "features": page})


def address_feature(object_id: int, formatted: str, lon=-73.6, lat=45.5) -> dict:
    """One feature as the service returns it.

    `NoCivq`/`NoCivqSuf` are taken from the parser rather than written out,
    because on the real layer they are the publisher's own columns and always
    agree with the string - so a fixture where they disagree would be testing
    something that cannot happen.
    """
    parsed = parse_formatted_address(formatted)
    assert parsed is not None, formatted
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "OBJECTID": object_id,
            "IdAdr": f"id-{object_id}",
            "AdresseFormatee": formatted,
            "NoCivq": int(parsed["civic"]),
            "NoCivqSuf": parsed["suffix"],
            "NbUnite": 1,
            "CaractAdr": "",
            "Position": f"{lon}, {lat}",
            "Version": "AQ20260901",
        },
    }


def make_client(features, *, page_size=2, **kwargs):
    session = FakeSession(features, page_size=page_size)
    client = AdressesQuebecClient(
        "https://example/MapServer/0",
        request_delay_seconds=0,
        page_size=page_size,
        session=session,
        **kwargs,
    )
    return client, session


def test_the_pages_are_walked_to_the_end_and_ordered():
    features = [address_feature(i, f"{7400 + i} Rue Boyer, Montréal H2S2J7") for i in range(5)]
    client, session = make_client(features, page_size=2)

    fetched = list(client.fetch_addresses(box(-73.7, 45.4, -73.4, 45.7)))

    assert [f["properties"]["OBJECTID"] for f in fetched] == [0, 1, 2, 3, 4]
    offsets = [int(p["resultOffset"]) for p in session.posts]
    assert offsets == [0, 2, 4]
    # Without a total order the pages are not a partition of the answer.
    assert all(p["orderByFields"] == "OBJECTID ASC" for p in session.posts)
    assert all(p["outFields"] == ",".join(ADDRESS_FIELDS) for p in session.posts)


def test_a_last_short_page_ends_the_walk_without_an_extra_request():
    features = [address_feature(i, f"{7400 + i} Rue Boyer, Montréal H2S2J7") for i in range(4)]
    client, session = make_client(features, page_size=2)

    list(client.fetch_addresses(box(-73.7, 45.4, -73.4, 45.7)))

    # 4 rows at 2 per page: the third request returns nothing and stops it.
    assert [int(p["resultOffset"]) for p in session.posts] == [0, 2, 4]


def test_a_service_ignoring_the_offset_is_stopped_rather_than_paged_for_ever():
    class StuckSession(FakeSession):
        def post(self, url, data=None, timeout=None):
            self.posts.append(data)
            return FakeResponse({"features": self.features[: self.page_size]})

    session = StuckSession([address_feature(1, "7400 Rue Boyer, Montréal H2S2J7")] * 2)
    client = AdressesQuebecClient(
        "https://example/MapServer/0",
        request_delay_seconds=0,
        page_size=2,
        max_pages=3,
        session=session,
    )

    with pytest.raises(AdressesQuebecError, match="resultOffset"):
        list(client.fetch_addresses(box(-73.7, 45.4, -73.4, 45.7)))
    assert len(session.posts) == 3


def test_an_arcgis_error_object_is_raised_rather_than_read_as_an_answer():
    """ArcGIS reports an error with HTTP 200 and an `error` key, so the status
    code is not what says whether this worked."""

    class ErrorSession(FakeSession):
        def post(self, url, data=None, timeout=None):
            return FakeResponse(
                {"error": {"message": "Invalid geometry", "details": ["ring"]}}
            )

    client, _ = make_client([])
    client._session = ErrorSession([])

    with pytest.raises(AdressesQuebecError, match="Invalid geometry"):
        client.count(box(-73.7, 45.4, -73.4, 45.7))


def test_features_become_a_flat_frame_with_lon_lat():
    frame = features_to_frame([address_feature(1, "7400 Rue Boyer, Montréal H2S2J7")])

    assert list(frame.columns) == [*ADDRESS_FIELDS, "longitude", "latitude"]
    assert frame["IdAdr"].tolist() == ["id-1"]
    assert frame["longitude"].tolist() == [-73.6]


def test_an_empty_answer_still_has_the_right_columns():
    frame = features_to_frame([])
    assert list(frame.columns) == [*ADDRESS_FIELDS, "longitude", "latitude"]
    assert frame.empty


# -- the bronze asset ---------------------------------------------------------


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


class FakeClient:
    def __init__(self, features, *, claims=None):
        self.features = features
        # What the service says is there, when that differs from what it then
        # hands over - which is the whole of the paging check.
        self.claims = len(features) if claims is None else claims

    def count(self, geometry):
        return self.claims

    def fetch_addresses(self, geometry, target_srs=4326):
        return iter(self.features)


def stub_client(monkeypatch, features, *, claims=None):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    client = FakeClient(features, claims=claims)
    monkeypatch.setattr(AdressesQuebecResource, "client", lambda self: client)
    return client


def run(store):
    return materialize(
        [neighborhood_addresses],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"addresses": AdressesQuebecResource(), "store": store},
        selection=[neighborhood_addresses],
    )


def test_addresses_land_under_date_then_neighborhood(store, monkeypatch):
    stub_client(
        monkeypatch,
        [address_feature(1, "7430 Rue Lajeunesse, Montréal H2R2H8", -73.6, 45.5)],
    )
    write_quartiers(store)

    assert run(store).success

    path = join(
        store.partition_dir("neighborhood_addresses", DATE, NEIGHBORHOOD),
        ADDRESSES_FILE,
    )
    frame = gpd.read_parquet(path)
    assert len(frame) == 1
    assert frame["IdAdr"].tolist() == ["id-1"]
    # Bronze keeps the publisher's string unparsed - see urban_rag.layers.
    assert frame["AdresseFormatee"].tolist() == ["7430 Rue Lajeunesse, Montréal H2R2H8"]
    assert frame["Version"].tolist() == ["AQ20260901"]
    # The partition keys travel as columns, since the path holds bare values.
    assert set(frame["neighborhood"]) == {NEIGHBORHOOD}
    assert set(frame["scrape_date"]) == {DATE}
    assert frame.crs == "EPSG:4326"


def test_a_point_outside_the_borough_is_cut(store, monkeypatch):
    """The service's spatial filter is a bound on the request; this is the cut."""
    stub_client(
        monkeypatch,
        [
            address_feature(1, "7430 Rue Lajeunesse, Montréal H2R2H8", -73.6, 45.5),
            address_feature(2, "1 Rue Ailleurs, Laval H7N1A1", -73.0, 45.9),
        ],
    )
    write_quartiers(store)

    result = run(store)

    frame = gpd.read_parquet(
        join(
            store.partition_dir("neighborhood_addresses", DATE, NEIGHBORHOOD),
            ADDRESSES_FILE,
        )
    )
    assert frame["IdAdr"].tolist() == ["id-1"]
    materialization = result.asset_materializations_for_node("bronze__neighborhood_addresses")
    metadata = materialization[0].metadata
    assert metadata["num_addresses"].value == 1
    assert metadata["num_outside_boundary"].value == 1


def test_a_walk_that_loses_rows_to_the_paging_is_refused(store, monkeypatch):
    """`resultOffset` without a stable order returns a sample of the answer
    rather than the answer, and it does so with no error at all - so the only
    thing that catches it is the count the service gave beforehand."""
    features = [
        address_feature(i, f"{7400 + i} Rue Boyer, Montréal H2S2J7", -73.6, 45.5)
        for i in range(10)
    ]
    stub_client(monkeypatch, features, claims=1000)
    write_quartiers(store)

    with pytest.raises(Failure, match="not partitioning"):
        run(store)


def test_a_borough_that_gained_an_address_mid_walk_is_only_logged(
    store, monkeypatch
):
    """Two requests against a live service are not one instant. A handful
    either way is the layer moving, not the paging failing."""
    features = [
        address_feature(i, f"{7400 + i} Rue Boyer, Montréal H2S2J7", -73.6, 45.5)
        for i in range(10)
    ]
    stub_client(monkeypatch, features, claims=12)
    write_quartiers(store)

    result = run(store)

    assert result.success
    metadata = result.asset_materializations_for_node(
        "bronze__neighborhood_addresses"
    )[0].metadata
    assert metadata["num_addresses"].value == 10
    assert metadata["num_service_count"].value == 12


def test_a_borough_the_service_answers_empty_fails_rather_than_writing_nothing(
    store, monkeypatch
):
    stub_client(monkeypatch, [])
    write_quartiers(store)

    with pytest.raises(Failure, match="no address"):
        run(store)


def test_every_point_outside_the_outline_fails_rather_than_writing_nothing(
    store, monkeypatch
):
    """The shape a wrongly-wound spatial filter takes: rows, none of them here."""
    stub_client(monkeypatch, [address_feature(2, "1 Rue Ailleurs, Laval H7N1A1", -73.0, 45.9)])
    write_quartiers(store)

    with pytest.raises(Failure, match="falls inside"):
        run(store)


# -- what the join reads back -------------------------------------------------

#: The names `postgis.compute_lot_addresses` lifts out of `rag.addresses`'
#: jsonb. A rename on either side is a column of nulls in silver.lot_addresses
#: and nothing that raises, which is why they are pinned here.
ATTRIBUTES_THE_SQL_READS = {
    "formatted_address",
    "unit",
    "civic_number",
    "civic_suffix",
    "civic_address",
    "street_name",
    "municipality",
    "postal_code",
    "num_units",
    "characteristic",
    "source_version",
    "object_id",
}


def test_the_parsed_frame_carries_every_name_the_join_reads():
    points = gpd.GeoDataFrame(
        features_to_frame(
            [
                address_feature(1, "7390 A Rue De Lanaudière, Montréal H2E1Y4"),
                address_feature(2, "204-7430 Rue Lajeunesse, Montréal H2R2H8"),
            ]
        ).drop(columns=["longitude", "latitude"]),
        geometry=[Point(-73.6, 45.5), Point(-73.61, 45.51)],
        crs="EPSG:4326",
    )

    parsed = parse_address_frame(points)

    assert ATTRIBUTES_THE_SQL_READS <= set(parsed.columns)
    assert "address_id" in parsed.columns
    assert parsed["street_name"].tolist() == ["Rue De Lanaudière", "Rue Lajeunesse"]
    assert parsed["unit"].tolist()[1] == "204"
    assert parsed["civic_address"].tolist() == [
        "7390 A Rue De Lanaudière",
        "7430 Rue Lajeunesse",
    ]
    # The layer states the number in a column of its own, so the parse never
    # gets to disagree with it.
    assert parsed["civic_number"].tolist() == [7390, 7430]
    assert parsed.crs == "EPSG:4326"


# -- the silver join, on the tile axis ------------------------------------------

TILE = "0302303330102"


def write_bronze(store, neighborhood, features):
    """One borough's bronze snapshot, as `neighborhood_addresses` writes it."""
    frame = features_to_frame(features)
    points = gpd.GeoDataFrame(
        frame.drop(columns=["longitude", "latitude"]),
        geometry=gpd.points_from_xy(frame["longitude"], frame["latitude"]),
        crs="EPSG:4326",
    ).assign(neighborhood=neighborhood, scrape_date=DATE)
    write_frame(
        points,
        join(store.partition_dir("neighborhood_addresses", DATE, neighborhood), ADDRESSES_FILE),
    )


def stub_join(monkeypatch, *, boroughs=(NEIGHBORHOOD,), num_addresses=2):
    """Patch the four Postgres calls `lot_addresses` makes, recording each."""
    from contextlib import contextmanager

    from urban_rag import address_assets
    from urban_rag.resources import PostgisResource

    calls: dict[str, list] = {"loaded": [], "compute": [], "fetch": [], "boroughs": []}

    @contextmanager
    def connect(self):
        yield object()

    def neighborhoods_of_tile(connection, *, tile, scrape_date):
        calls["boroughs"].append((tile, scrape_date))
        return tuple(boroughs)

    def load_addresses(connection, frame, *, neighborhood, scrape_date):
        calls["loaded"].append((neighborhood, len(frame), "street_name" in frame))
        return len(frame)

    def compute_lot_addresses(connection, *, tile, scrape_date, max_snap_m):
        calls["compute"].append((tile, scrape_date, max_snap_m))
        return {
            "num_addresses": num_addresses, "num_points": 3, "num_unmatched": 1,
            "num_lots": 2, "num_pieces": 2, "num_no_piece": 0, "num_snapped": 0,
            "num_unit_addresses": 0, "num_primary_piece": 2, "num_unparsed": 0,
        }

    def fetch_lot_addresses(connection, *, tile, scrape_date):
        calls["fetch"].append((tile, scrape_date))
        return gpd.GeoDataFrame(
            {"address_id": ["id-1"], "cell_partition": [tile]},
            geometry=[Point(-73.6, 45.5)],
            crs="EPSG:4326",
        )

    monkeypatch.setattr(PostgisResource, "connect", connect)
    for name, value in {
        "neighborhoods_of_tile": neighborhoods_of_tile,
        "load_addresses": load_addresses,
        "compute_lot_addresses": compute_lot_addresses,
        "fetch_lot_addresses": fetch_lot_addresses,
    }.items():
        monkeypatch.setattr(address_assets, name, value)
    return calls


def run_join(store):
    from urban_rag.address_assets import lot_addresses
    from urban_rag.resources import PostgisResource

    return materialize(
        [lot_addresses],
        partition_key=MultiPartitionKey({"date": DATE, "tile": TILE}),
        resources={"store": store, "postgis": PostgisResource()},
    )


def test_the_join_loads_every_borough_of_the_tile_then_joins_the_tile(
    store, monkeypatch
):
    """The fetch is a borough's and the join a tile's: a tile spanning two
    boroughs loads both snapshots, parsed, then joins its own points."""
    calls = stub_join(monkeypatch, boroughs=(NEIGHBORHOOD, "RPP"))
    write_bronze(store, NEIGHBORHOOD, [address_feature(1, "7430 Rue Lajeunesse, Montréal H2R2H8")])
    write_bronze(
        store,
        "RPP",
        [
            address_feature(2, "6700 Rue Boyer, Montréal H2S2J7"),
            address_feature(3, "6702 Rue Boyer, Montréal H2S2J7"),
        ],
    )

    result = run_join(store)

    assert result.success
    assert calls["boroughs"] == [(TILE, DATE)]
    assert calls["loaded"] == [(NEIGHBORHOOD, 1, True), ("RPP", 2, True)]
    assert calls["compute"][0][:2] == (TILE, DATE)
    assert calls["fetch"] == [(TILE, DATE)]
    written = gpd.read_parquet(
        join(store.partition_dir("lot_addresses", DATE, TILE), "lot_addresses.parquet")
    )
    assert written["address_id"].tolist() == ["id-1"]
    metadata = result.asset_materializations_for_node("silver__lot_addresses")[0].metadata
    assert metadata["tile"].value == TILE
    assert metadata["neighborhoods"].value == f"{NEIGHBORHOOD}, RPP"
    assert metadata["num_points_loaded_for_boroughs"].value == 3


def test_a_tile_with_no_lots_names_the_cadastre(store, monkeypatch):
    stub_join(monkeypatch, boroughs=())

    with pytest.raises(Failure, match="neighborhood_cadastre"):
        run_join(store)


def test_a_join_that_places_nothing_fails_inside_the_transaction(store, monkeypatch):
    stub_join(monkeypatch, num_addresses=0)
    write_bronze(store, NEIGHBORHOOD, [address_feature(1, "7430 Rue Lajeunesse, Montréal H2R2H8")])

    with pytest.raises(Failure, match="falls on a parcel"):
        run_join(store)
