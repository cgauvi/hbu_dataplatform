"""Quebec City's zoning: the grid workbook read, the row-to-column translation,
the layer client, and the city registry behind the partition key."""

from __future__ import annotations

import io
import json

import openpyxl
import pytest
from dagster import DagsterInstance

from urban_rag import partitions
from urban_rag.partitions import (
    DEFAULT_NEIGHBORHOODS,
    NEIGHBORHOOD_PARTITIONS_NAME,
    City,
    city_of,
    cmhc_centre_for,
    enabled_neighborhoods,
    known_neighborhoods,
    metric_crs_for,
    metric_srid_for,
    register_neighborhoods,
    unregister_neighborhoods,
)
from urban_rag.program import BuildingLevel
from urban_rag.quebec import (
    DEFAULT_SHEET_URL_TEMPLATE,
    GRID_URL_COLUMN,
    GRID_ZONE_COLUMN,
    STOREY_HEIGHT_M,
    QuebecZoningClient,
    QuebecZoningError,
    borough_prefix,
    grid_columns,
    normalize_label,
    read_zoning_grid,
)

# -- the registry -------------------------------------------------------------


def test_every_key_belongs_to_one_city():
    assert city_of("VSMPE") is City.MONTREAL
    assert city_of("CIL") is City.QUEBEC
    assert set(known_neighborhoods()) >= {"VSMPE", "CIL", "RIV", "SSC", "CHA", "BEA", "HSC"}
    with pytest.raises(KeyError, match="Unknown neighborhood"):
        city_of("Nowhere")


def test_the_metric_projection_follows_the_city():
    assert metric_crs_for("VSMPE") == "EPSG:32188"
    assert metric_crs_for("CIL") == "EPSG:32187"
    assert metric_srid_for("CIL") == 32187
    assert cmhc_centre_for("CIL") == "Québec"


def test_every_default_key_is_known_and_has_a_cmhc_crosswalk():
    for key in DEFAULT_NEIGHBORHOODS:
        assert key in known_neighborhoods()
        assert partitions.quartiers_for(key)


def test_a_fresh_instance_is_seeded_with_the_defaults():
    instance = DagsterInstance.ephemeral()
    # The suite's conftest seeds ephemeral instances; take the keys off first
    # so this exercises the seeding itself.
    for key in instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME):
        instance.delete_dynamic_partition(NEIGHBORHOOD_PARTITIONS_NAME, key)
    assert instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME) == []

    assert enabled_neighborhoods(instance) == DEFAULT_NEIGHBORHOODS
    assert set(instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME)) == set(
        DEFAULT_NEIGHBORHOODS
    )


def test_registering_adds_only_what_is_missing_and_refuses_the_unknown():
    instance = DagsterInstance.ephemeral()
    enabled_neighborhoods(instance)

    assert register_neighborhoods(instance, ["CIL", "RIV", "RIV"]) == ("RIV",)
    assert "RIV" in enabled_neighborhoods(instance)
    with pytest.raises(KeyError, match="Unknown neighborhood key"):
        register_neighborhoods(instance, ["RIV", "Nowhere"])

    assert unregister_neighborhoods(instance, ["RIV", "Never"]) == ("RIV",)
    assert "RIV" not in enabled_neighborhoods(instance)


def test_without_an_instance_the_defaults_are_the_answer():
    assert enabled_neighborhoods(None) == DEFAULT_NEIGHBORHOODS


# -- the grid workbook ---------------------------------------------------------


def workbook(rows: list[dict]) -> bytes:
    """A workbook shaped like the city's: three header rows, then the names.

    The columns are the handful the translation reads; the real sheet has 300.
    """
    columns = [
        ("", "", "Zone à modifier"),
        ("", "", "Dominante"),
        ("Habitation", "H1", "H1 autorisé"),
        ("", "", "H1 isolé nb max. logement par bâtiment"),
        ("", "", "H1 jumelé nb max. logement par bâtiment"),
        ("", "", "H1 rangée nb max. logement par bâtiment"),
        ("", "", "H1 Localisation"),
        ("Commerce de consommation et de services", "C1", "C1 autorisé"),
        ("", "", "C1 Localisation"),
        ("", "C2", "C2 autorisé"),
        ("", "", "C2 Localisation"),
        ("Industrie", "I2", "I2 autorisé"),
        ("Publique", "P1", "P1 autorisé"),
        ("Récréation extérieure", "R1", "R1 autorisé"),
        ("Usages particuliers", "", "Usage spécifiquement exclu"),
        ("Dimensions générales", "", "Sup. min. (m2)"),
        ("", "", "Largeur min.(m)"),
        ("Dimensions particulières", "", "Sup. min. (m2)"),
        ("", "", "Largeur min.(m)"),
        ("Dimension du bâtiment principal – Dimensions générales", "", "Hauteur min. (m)"),
        ("", "", "Hauteur max. (m)"),
        ("", "", "Nombre d'étages\nmin."),
        ("", "", "Nombre d'étages\nmax."),
        ("Dimension du bâtiment principal – Dimensions particulières", "", "Hauteur max. (m)"),
        ("Normes d'implantation générales", "", "Marge avant (m)"),
        ("", "", "Marge latérale (m)"),
        ("", "", "Marge arrière (m)"),
        ("", "", "POS min. (%)"),
        ("", "", "Aire verte min. (%)"),
        ("Normes de densité", "", "Nb de log. à l'hectare max. (log/ha)"),
        ("Dispositions particulières", "", "PIIA"),
        ("", "", "Arrondissement historique"),
    ]
    names = [name for _, _, name in columns]
    book = openpyxl.Workbook()
    sheet = book.active
    sheet.title = "Modifications"
    sheet.append(["EN DATE DU 1er SEPTEMBRE 2026"])
    sheet.append([group or None for group, _, _ in columns])
    sheet.append([sub or None for _, sub, _ in columns])
    sheet.append(names)
    for row in rows:
        sheet.append([row.get(name) for name in names])
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


MIXED = {
    "Zone à modifier": "11017Md",
    "Dominante": "Md",
    "H1 autorisé": "H1",
    "H1 isolé nb max. logement par bâtiment": "0",
    "H1 jumelé nb max. logement par bâtiment": "6",
    "H1 rangée nb max. logement par bâtiment": "12",
    "H1 Localisation": "2,2+",
    "C1 autorisé": "C1",
    "C2 autorisé": "C2",
    "C2 Localisation": "R,R+,1",
    "I2 autorisé": "I2",
    "P1 autorisé": "P1",
    "Usage spécifiquement exclu": "La location d'une chambre est prohibée - article 177",
    "Largeur min.(m)": "9",
    "Hauteur min. (m)": "7",
    "Hauteur max. (m)": "20",
    "Marge avant (m)": "6",
    "Marge latérale (m)": "1.5",
    "Marge arrière (m)": "7.5",
    "POS min. (%)": "35",
    "Aire verte min. (%)": "10",
    "Nb de log. à l'hectare max. (log/ha)": "100",
    "PIIA": "PIIA",
}

PARK = {"Zone à modifier": "11001Ra", "Dominante": "Ra", "R1 autorisé": "R1"}

STATED_STOREYS = {
    "Zone à modifier": "11016Hb",
    "Dominante": "Hb",
    "H1 autorisé": "H1",
    "H1 isolé nb max. logement par bâtiment": "3",
    "Hauteur max. (m)": "14",
    "Nombre d'étages\nmax.": "3",
    "Arrondissement historique": "Arrondissement historique",
}


def test_the_workbook_reads_one_row_per_zone_with_group_prefixed_doubles():
    grid = read_zoning_grid(workbook([MIXED, PARK, STATED_STOREYS]))

    assert grid[GRID_ZONE_COLUMN].tolist() == ["11017Md", "11001Ra", "11016Hb"]
    assert grid.attrs["title"] == "EN DATE DU 1er SEPTEMBRE 2026"
    # A name printed once keeps its bare text; one printed twice is told
    # apart by the group above it.
    assert "H1 autorisé" in grid.columns
    assert "Dimensions générales: Largeur min.(m)" in grid.columns
    assert "Dimensions particulières: Largeur min.(m)" in grid.columns
    assert "Dimension du bâtiment principal – Dimensions générales: Hauteur max. (m)" in grid.columns


def test_a_zone_stated_twice_is_refused():
    with pytest.raises(QuebecZoningError, match="more than once"):
        read_zoning_grid(workbook([PARK, PARK]))


def test_a_workbook_with_no_zone_rows_is_refused():
    with pytest.raises(QuebecZoningError, match="no zone rows"):
        read_zoning_grid(workbook([]))


def rows_of(*zones: dict) -> dict[str, dict]:
    grid = read_zoning_grid(workbook(list(zones)))
    return {record[GRID_ZONE_COLUMN]: record for record in grid.to_dict("records")}


def test_a_mixed_zone_becomes_one_column_per_family_with_its_own_floors():
    columns = grid_columns(rows_of(MIXED)["11017Md"])
    by_family = {column.usages[0]: column for column in columns}

    assert list(by_family) == ["H", "C", "I", "E"]
    assert by_family["H"].usages_by_category == {"habitation": "H1"}
    assert by_family["C"].usages_by_category == {"commerce": "C1, C2"}
    assert by_family["E"].usages_by_category == {"equipements": "P1"}
    # Housing from the second floor up is read as everything but the RDC,
    # and says so; commerce at R, R+ and 1 is the ground floor and everything.
    assert by_family["H"].levels == frozenset({BuildingLevel.ALL_EXCEPT_GROUND})
    assert any(note.startswith("levels: H1") for note in by_family["H"].notes)
    assert by_family["C"].levels == frozenset(
        {BuildingLevel.GROUND, BuildingLevel.ALL, BuildingLevel.SECOND}
    )
    # A group with no Localisation may go anywhere.
    assert by_family["I"].levels == frozenset({BuildingLevel.ALL})
    assert all(column.permits_residential is (family == "H") for family, column in by_family.items())


def test_the_norms_are_the_zones_and_shared_by_every_column():
    columns = grid_columns(rows_of(MIXED)["11017Md"])
    for column in columns:
        assert column.zone == "11017Md"
        assert column.height_min_m == 7.0
        assert column.height_max_m == 20.0
        # No storey count stated: derived from the height, and noted.
        assert column.floors_max == int(20 // STOREY_HEIGHT_M) == 5
        assert any(note.startswith("floors_max: derived") for note in column.notes)
        assert column.min_lot_width_m == 9.0
        assert column.front_margin_min_m == 6.0
        assert column.side_margin_min_m == 1.5
        assert column.rear_margin_min_m == 7.5
        assert column.site_coverage_min_pct == 35.0
        # No maximum stated: the green area is the bound, and noted.
        assert column.site_coverage_max_pct == 90.0
        assert any(note.startswith("site_coverage_max_pct") for note in column.notes)
        # The largest of the three type ceilings; isolé's 0 is a refusal.
        assert column.max_dwellings == 12
        assert column.implantation_mode == "J-C"
        assert column.density_max is None
        assert any("log/ha" in note for note in column.notes)
        assert column.excluded_usages.startswith("La location")
        assert column.piia_sector == "PIIA"
        assert column.heritage_sector is None
        assert column.to_zone_column().floors_max == 5


def test_a_stated_storey_count_wins_over_the_height():
    (column,) = [c for c in grid_columns(rows_of(STATED_STOREYS)["11016Hb"]) if c.usages == ("H",)]
    assert column.floors_max == 3
    assert not any(note.startswith("floors_max") for note in column.notes)
    assert column.implantation_mode == "I"
    assert column.max_dwellings == 3
    assert column.heritage_sector == "Arrondissement historique"


def test_a_park_is_equipment_only_and_not_solver_ready():
    columns = grid_columns(rows_of(PARK)["11001Ra"])
    assert [column.usages for column in columns] == [("E",)]
    assert columns[0].floors_max is None
    with pytest.raises(Exception, match="no storey maximum"):
        columns[0].to_zone_column()


def test_an_empty_cell_is_empty_however_pandas_spells_it():
    # `to_dict("records")` hands back NaN for a blank cell in a numeric
    # column, and NaN must not read as "authorised".
    record = rows_of(PARK)["11001Ra"]
    assert all(
        isinstance(record.get(name), float) or record.get(name) is None
        for name in ("H1 autorisé", "C1 autorisé")
    )
    assert [c.usages for c in grid_columns(record)] == [("E",)]


def test_labels_normalise_accents_case_and_breaks():
    assert normalize_label("Nombre d'étages\nmax.") == "nombre d'etages max."
    assert normalize_label("  Marge   Latérale (m) ") == "marge laterale (m)"
    assert normalize_label(None) == ""


def test_the_arrondissement_is_the_leading_digit():
    assert borough_prefix("11017Md") == "1"
    assert borough_prefix("53091Hb") == "5"
    assert borough_prefix("") is None


# -- the layer client ----------------------------------------------------------


class FakeResponse:
    def __init__(self, payload, *, content_type="application/json"):
        self._payload = payload
        self.headers = {"Content-Type": content_type}
        self.status_code = 200
        self.text = json.dumps(payload) if not isinstance(payload, bytes) else ""
        self.content = payload if isinstance(payload, bytes) else self.text.encode()

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self):
        self.posts: list[dict] = []
        self.headers = {}

    def post(self, url, data=None, timeout=None):
        self.posts.append(data)
        if data.get("returnIdsOnly") == "true":
            return FakeResponse({"objectIds": [7, 8, 9]})
        ids = [int(i) for i in data["objectIds"].split(",")]
        return FakeResponse(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"OBJECTID": i, "IGDS_TEXT_STRING": f"1100{i}Hb"},
                        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
                    }
                    for i in ids
                ],
            }
        )

    def get(self, url, params=None, timeout=None):
        return FakeResponse(b"PK\x03\x04grid", content_type="application/octet-stream")


def test_the_layer_is_read_in_two_phases_and_batches():
    session = FakeSession()
    client = QuebecZoningClient(
        "https://example/FeatureServer/2",
        request_delay_seconds=0,
        batch_size=2,
        session=session,
    )
    ids = client.zone_ids({"rings": [[[0, 0], [1, 0], [1, 1], [0, 0]]], "spatialReference": {"wkid": 4326}})
    assert ids == [7, 8, 9]
    features = list(client.fetch_zones(ids))
    assert [f["properties"]["IGDS_TEXT_STRING"] for f in features] == ["11007Hb", "11008Hb", "11009Hb"]
    # One id query, then two batches of at most two.
    assert [p.get("returnIdsOnly") for p in session.posts] == ["true", None, None]
    assert session.posts[1]["outSR"] == "4326" and session.posts[1]["f"] == "geojson"
    assert client.fetch_grid().startswith(b"PK")


def test_a_short_batch_is_an_error_not_a_shorter_borough():
    class ShortSession(FakeSession):
        def post(self, url, data=None, timeout=None):
            response = super().post(url, data, timeout)
            if data.get("objectIds"):
                payload = response.json()
                payload["features"] = payload["features"][:1]
                return FakeResponse(payload)
            return response

    client = QuebecZoningClient(
        "https://example/FeatureServer/2", request_delay_seconds=0, session=ShortSession()
    )
    with pytest.raises(QuebecZoningError, match="dropped rows"):
        list(client.fetch_zones([7, 8]))


def test_a_zone_sheet_url_is_built_from_the_zone_code():
    """No request, unlike Saguenay's: the handler is keyed on the code itself."""
    client = QuebecZoningClient(
        "https://example/FeatureServer/2", request_delay_seconds=0, session=FakeSession()
    )

    assert client.sheet_url_for("13001Hb") == (
        "https://carte.ville.quebec.qc.ca/GrillesZonage/HandlerZonage.ashx?13001Hb"
    )
    # Whitespace off a parquet cell must not reach the query string.
    assert client.sheet_url_for("  14040Hb ").endswith("?14040Hb")


def test_the_sheet_url_template_is_overridable():
    client = QuebecZoningClient(
        "https://example/FeatureServer/2",
        sheet_url_template="https://mirror.test/grids/{zone}.pdf",
        request_delay_seconds=0,
        session=FakeSession(),
    )

    assert client.sheet_url_for("13001Hb") == "https://mirror.test/grids/13001Hb.pdf"


def test_the_sheet_link_column_is_montreals():
    """The whole point of the name: `DOCUMENT_SOURCES` keys on it for all three
    cities, so a Quebec zone row has to spell it the way Montreal's does."""
    from urban_rag.rag.documents import DOCUMENT_SOURCES
    from urban_rag.quebec import ZONING_SLUG

    assert GRID_URL_COLUMN == "LIEN_GRILLE"
    assert DOCUMENT_SOURCES[ZONING_SLUG] == GRID_URL_COLUMN
    assert "{zone}" in DEFAULT_SHEET_URL_TEMPLATE
