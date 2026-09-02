"""Offline tests for the open-data portal client and the quartiers asset.

Nothing here touches the network: the CKAN payload and the two files are
canned, and the asset runs against a temp directory through `dagster.materialize`.
"""

from __future__ import annotations

import json

import geopandas as gpd
import pandas as pd
import pytest
from dagster import materialize

from asset_helpers import materialization_metadata

from urban_rag.open_data import CkanClient, OpenDataError, Resource, decode_csv
from urban_rag.open_data_assets import (
    DWELLINGS_CSV,
    QUARTIERS_GEOJSON,
    reference_neighborhoods,
)
from urban_rag.resources import OpenDataResource, ParquetStore

DOWNLOAD_BASE = "https://donnees.montreal.ca/dataset/abc/resource/def/download"

PACKAGE_PAYLOAD = {
    "success": True,
    "result": {
        "name": "quartiers",
        "title": "Quartiers de référence en habitation",
        "license_title": "Creative Commons Attribution 4.0 International",
        "resources": [
            {
                "id": "a80e611f",
                "name": "Quartiers de référence en habitation",
                "format": "GeoJSON",
                "url": f"{DOWNLOAD_BASE}/{QUARTIERS_GEOJSON}",
                "last_modified": "2017-01-13T15:58:14.678619",
            },
            {
                "id": "58ca829c",
                "name": "Nombre de logements dans les quartiers de référence",
                "format": "CSV",
                "url": f"{DOWNLOAD_BASE}/{DWELLINGS_CSV}",
                "last_modified": "2026-08-19T06:38:50.983080",
            },
        ],
    },
}

GEOJSON = json.dumps(
    {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "no_qr": "01",
                    "nom_qr": "Cartierville",
                    "no_arr": "23",
                    "Nom_arr": "Ahuntsic-Cartierville",
                    "nom_mun": "Montréal",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-73.7, 45.5], [-73.6, 45.5], [-73.6, 45.6], [-73.7, 45.5]]
                    ],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "no_qr": "02",
                    "nom_qr": "Nouveau-Bordeaux",
                    "no_arr": "23",
                    "Nom_arr": "Ahuntsic-Cartierville",
                    "nom_mun": "Montréal",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-73.5, 45.5], [-73.4, 45.5], [-73.4, 45.6], [-73.5, 45.5]]
                    ],
                },
            },
        ],
    }
).encode("utf-8")

DWELLINGS = (
    "No_QR,Nom_QR,No_arr,Nom_arr_Montreal,Nom_mun,Nb_log\n"
    "01,Cartierville,23,Ahuntsic-Cartierville,Montréal,10070\n"
    "02,Nouveau-Bordeaux,23,Ahuntsic-Cartierville,Montréal,13476\n"
).encode("utf-8")


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
    """Replays the canned payloads, keyed by the tail of the URL."""

    def __init__(self, files: dict[str, bytes] | None = None):
        self.files = {
            QUARTIERS_GEOJSON: GEOJSON,
            DWELLINGS_CSV: DWELLINGS,
            **(files or {}),
        }
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params or {}))
        if url.endswith("package_show"):
            return FakeResponse(json.dumps(PACKAGE_PAYLOAD).encode("utf-8"))
        filename = url.rsplit("/", 1)[-1]
        if filename not in self.files:
            raise AssertionError(f"unexpected download: {url}")
        return FakeResponse(
            self.files[filename], content_type="application/octet-stream"
        )


def make_client(session: FakeSession | None = None) -> tuple[CkanClient, FakeSession]:
    session = session or FakeSession()
    return (
        CkanClient("https://portal", request_delay_seconds=0, session=session),
        session,
    )


# -- client ----------------------------------------------------------------


def test_package_show_is_called_with_the_dataset_slug():
    client, session = make_client()

    package = client.package("quartiers")

    url, params = session.calls[0]
    assert url == "https://portal/api/3/action/package_show"
    assert params == {"id": "quartiers"}
    assert package.license_title.startswith("Creative Commons")
    assert len(package.resources) == 2


def test_package_show_failure_in_the_body_is_raised():
    session = FakeSession()
    session.get = lambda url, params=None, timeout=None: FakeResponse(
        json.dumps(
            {"success": False, "error": {"message": 'Dataset "nope" was not found.'}}
        ).encode("utf-8")
    )
    client, _ = make_client(session)

    with pytest.raises(OpenDataError, match="was not found"):
        client.package("nope")


def test_resources_are_looked_up_by_download_filename():
    client, _ = make_client()
    package = client.package("quartiers")

    assert package.resource(DWELLINGS_CSV).resource_id == "58ca829c"
    # Both files are published under the same French title, so the title
    # cannot be the key - the filename is.
    assert package.resource(QUARTIERS_GEOJSON).format == "GeoJSON"


def test_unknown_filename_lists_what_the_dataset_does_publish():
    client, _ = make_client()
    package = client.package("quartiers")

    with pytest.raises(OpenDataError, match=QUARTIERS_GEOJSON):
        package.resource("quartiers.parquet")


def test_percent_encoded_filenames_are_decoded():
    resource = Resource("id", "n", "CSV", f"{DOWNLOAD_BASE}/nombre%20de%20logements.csv")

    assert resource.filename == "nombre de logements.csv"


def test_an_html_error_page_is_not_mistaken_for_the_file():
    session = FakeSession()
    # The portal serves its error pages with HTTP 200, so status is no guide.
    session.get = lambda url, params=None, timeout=None: FakeResponse(
        b"<html>404</html>", content_type="text/html; charset=utf-8"
    )
    client, _ = make_client(session)

    with pytest.raises(OpenDataError, match="HTML page"):
        client.download(Resource("id", "n", "CSV", f"{DOWNLOAD_BASE}/x.csv"))


# -- csv decoding ----------------------------------------------------------


def test_decode_csv_keeps_identifiers_as_text():
    frame = decode_csv(DWELLINGS)

    # "01" must not become 1: it is the join key against the layer.
    assert frame["No_QR"].tolist() == ["01", "02"]


def test_decode_csv_reads_cp1252_and_semicolons():
    frame = decode_csv("no_qr;nom_qr\n01;Côte-des-Neiges\n".encode("cp1252"))

    assert frame["nom_qr"].tolist() == ["Côte-des-Neiges"]


def test_decode_csv_strips_the_excel_byte_order_mark():
    frame = decode_csv("no_qr,nb_log\n01,10070\n".encode("utf-8-sig"))

    assert list(frame.columns) == ["no_qr", "nb_log"]


def test_undecodable_bytes_are_reported_with_the_filename():
    with pytest.raises(OpenDataError, match="broken.csv"):
        decode_csv(b"\xff\x81\xfe", filename="broken.csv")


# -- asset -----------------------------------------------------------------


def materialize_partition(
    tmp_path, monkeypatch, *, scrape_date="2026-08-01", session=None
):
    """Run the asset against a temp directory, with the portal stubbed out.

    Patched on the class rather than on an instance: Dagster rebuilds the
    resource from its config before the run, so an instance attribute would
    not survive into the asset.
    """
    session = session or FakeSession()
    monkeypatch.setattr(
        OpenDataResource,
        "client",
        lambda self: CkanClient(
            "https://portal", request_delay_seconds=0, session=session
        ),
    )
    result = materialize(
        [reference_neighborhoods],
        partition_key=scrape_date,
        resources={
            "open_data": OpenDataResource(),
            "store": ParquetStore(root_dir=str(tmp_path)),
        },
    )
    assert result.success
    return result


def test_asset_writes_both_files_under_the_date_partition(tmp_path, monkeypatch):
    materialize_partition(tmp_path, monkeypatch, scrape_date="2026-08-01")

    partition = tmp_path / "bronze" / "reference_neighborhoods" / "2026-08-01"
    assert sorted(p.name for p in partition.glob("*.parquet")) == [
        "nombre_logements.parquet",
        "quartiers.parquet",
    ]

    layer = gpd.read_parquet(partition / "quartiers.parquet")
    assert len(layer) == 2
    assert layer.crs.to_string() == "EPSG:4326"
    # Column names are lower-cased so the two files join on `no_qr`.
    assert "nom_arr" in layer.columns and "Nom_arr" not in layer.columns
    assert layer["no_qr"].tolist() == ["01", "02"]
    # The prefix carries bare values, so the date has to travel as a column.
    assert layer["scrape_date"].tolist() == ["2026-08-01", "2026-08-01"]
    assert {"source_file", "scraped_at"} <= set(layer.columns)

    counts = pd.read_parquet(partition / "nombre_logements.parquet")
    assert counts["nb_log"].sum() == 23546
    assert counts["no_qr"].tolist() == ["01", "02"]


def test_metadata_reports_the_counts_and_the_licence(tmp_path, monkeypatch):
    result = materialize_partition(tmp_path, monkeypatch)

    metadata = materialization_metadata(result, reference_neighborhoods)
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_neighborhoods"].value == 2
    assert metadata["total_dwellings"].value == 23546
    assert metadata["num_invalid_geometries"].value == 0
    assert "Creative Commons" in metadata["license"].value


def test_a_rerun_replaces_the_previous_snapshot(tmp_path, monkeypatch):
    partition = tmp_path / "bronze" / "reference_neighborhoods" / "2026-08-01"
    partition.mkdir(parents=True)
    stale = partition / "quartiers_retired.parquet"
    pd.DataFrame({"a": [1]}).to_parquet(stale)

    materialize_partition(tmp_path, monkeypatch, scrape_date="2026-08-01")

    assert not stale.exists()


def test_a_broken_dwellings_file_does_not_cost_the_layer(tmp_path, monkeypatch):
    session = FakeSession(files={DWELLINGS_CSV: b"\xff\x81\xfe"})

    result = materialize_partition(tmp_path, monkeypatch, session=session)

    partition = tmp_path / "bronze" / "reference_neighborhoods" / "2026-08-01"
    assert (partition / "quartiers.parquet").exists()
    assert not (partition / "nombre_logements.parquet").exists()
    metadata = materialization_metadata(result, reference_neighborhoods)
    assert "dwellings_error" in metadata
