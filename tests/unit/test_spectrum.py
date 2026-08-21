"""Offline tests for the pieces that encode the proxy's quirks."""

from __future__ import annotations

import pytest

from urban_rag.frames import features_to_frame, table_slug
from urban_rag.spectrum import (
    Column,
    SpectrumAccessDenied,
    SpectrumClient,
    TableMetadata,
)


class FakeResponse:
    def __init__(self, payload, *, content_type="application/json", text=""):
        self._payload = payload
        self.headers = {"Content-Type": content_type}
        self.text = text
        self.status_code = 200

    def json(self):
        return self._payload


class FakeSession:
    """Records calls and replays canned payloads."""

    def __init__(self, payload):
        self.payload = payload
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params or {}))
        return FakeResponse(self.payload)


def make_client(payload):
    session = FakeSession(payload)
    client = SpectrumClient(
        "https://example/FeatureService",
        request_delay_seconds=0,
        session=session,
    )
    return client, session


def test_list_tables_sends_empty_url_parameter_and_filters_namespace():
    client, session = make_client({"Tables": ["/19_VSMPE/A/B", "/18_VM/C/D"]})

    assert client.list_tables("19_VSMPE") == ["/19_VSMPE/A/B"]

    url, params = session.calls[0]
    assert url == "https://example/FeatureService/tables.json"
    # The proxy 500s without this parameter, even empty.
    assert params == {"url": ""}


def test_search_embeds_the_query_string_inside_the_url_parameter():
    client, session = make_client({"features": []})
    metadata = TableMetadata(
        table="/19_VSMPE/A/B",
        columns=(Column("Obj", "Geometry"),),
        geometry_column="Obj",
        native_crs="epsg:42104",
    )

    list(client.fetch_features(metadata, page_length=100))

    url, params = session.calls[0]
    assert url == "https://example/FeatureService/tables"
    assert params["url"] == (
        "features.json?q=select MI_Transform(Obj,'epsg:4326'), * "
        'from "/19_VSMPE/A/B"&page=1&pageLength=100'
    )


def test_metadata_encodes_slashes_in_the_path_not_in_the_url_parameter():
    client, session = make_client(
        {"Metadata": [{"name": "Obj", "type": "Geometry"}, {"name": "X", "type": "String"}]}
    )

    metadata = client.table_metadata("/19_VSMPE/A/B")

    url, params = session.calls[0]
    assert url.endswith("/tables/%2F19_VSMPE%2FA%2FB/metadata.json")
    assert params == {"url": ""}
    assert metadata.geometry_column == "Obj"
    assert metadata.attribute_columns == ("X",)


def test_tables_without_geometry_skip_the_transform():
    client, _ = make_client({})
    metadata = TableMetadata(
        table="/19_VSMPE/A/B", columns=(), geometry_column=None, native_crs=None
    )

    assert client.build_select(metadata) == 'select * from "/19_VSMPE/A/B"'


def test_access_denied_is_raised_as_its_own_error():
    client, _ = make_client(
        {
            "type": "com.mapinfo.midev.service.feature.ws.v1.ServiceException",
            "message": "Access denied - /VdeM [EXECUTE].",
        }
    )

    with pytest.raises(SpectrumAccessDenied):
        client.list_tables()


def test_features_become_a_geodataframe_without_the_style_column():
    features = [
        {
            "properties": {"MI_Style": {"type": "MapBasicAreaStyle"}, "NUMERO": "01-001"},
            "geometry": {"type": "Point", "coordinates": [-73.6, 45.5]},
        }
    ]

    frame = features_to_frame(features, extra_columns={"source_table": "/a/b/c"})

    assert "MI_Style" not in frame.columns
    assert frame.loc[0, "NUMERO"] == "01-001"
    assert frame.loc[0, "source_table"] == "/a/b/c"
    assert frame.crs.to_string() == "EPSG:4326"


def test_features_without_geometry_stay_a_plain_dataframe():
    frame = features_to_frame([{"properties": {"X": 1}, "geometry": None}])

    assert not hasattr(frame, "crs")


def test_table_slug_drops_the_namespace():
    assert (
        table_slug("/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE")
        == "Reglement_urbanisme__VSP_REG_ZONE"
    )
