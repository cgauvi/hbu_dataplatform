"""Quebec City's zoning: the grid workbook read, the row-to-column translation,
the layer client, and the city registry behind the partition key."""

from __future__ import annotations

import io
import json

import openpyxl
import pytest
from dagster import DagsterInstance

from hbu_dataplatform.partitions.cities import quartiers_for
from hbu_dataplatform.partitions.axes import (
    DEFAULT_NEIGHBORHOODS,
    NEIGHBORHOOD_PARTITIONS_NAME,
    enabled_neighborhoods,
    known_neighborhoods,
    register_neighborhoods,
    unregister_neighborhoods,
)
from hbu_dataplatform.partitions.cities import (
    City,
    city_of,
    cmhc_centre_for,
    metric_crs_for,
    metric_srid_for,
    source_namespace_for,
)
from hbu_dataplatform.hbu.program import BuildingLevel
from hbu_dataplatform.cities.quebec_city.zoning import (
    DEFAULT_SHEET_URL_TEMPLATE,
    GRID_URL_COLUMN,
    GRID_ZONE_COLUMN,
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


def test_the_collision_source_namespace_exists_to_resolve_is_real():
    """Two Montreal boroughs publish the same zone number under the same slug.

    This is what 005_silver_lot_features.sql widened the uniqueness of
    `rag.features` for, and it is the whole reason a fourth column is in that
    key at all. `frames.table_slug` drops the namespace deliberately - the slug
    has to match `rag.chunks.source_table` - so the slug alone cannot tell the
    two rows apart, and neither can the zone number.
    """
    from hbu_dataplatform.core.frames import table_slug

    vsmpe = "/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE"
    rpp = "/16_RPP/Reglement_urbanisme/VSP_REG_ZONE"

    # Same slug, and a zone number that restarts in every borough.
    assert table_slug(vsmpe) == table_slug(rpp) == "Reglement_urbanisme__VSP_REG_ZONE"

    # So (source_table, feature_id) is ambiguous across boroughs...
    assert (table_slug(vsmpe), "C01-001") == (table_slug(rpp), "C01-001")

    # ...and source_namespace is what resolves it.
    assert source_namespace_for("VSMPE") != source_namespace_for("RPP")


def test_source_namespace_is_the_publishers_unit_not_the_borough():
    """Montreal's is the Spectrum namespace; the other two cities have none.

    Quebec City and Saguenay each publish one zoning layer for the whole
    municipality, so the city is the honest answer. The empty string would not
    be: a qualifier that is the same for every row qualifies nothing, and the
    day this column joins the uniqueness key that would be the difference
    between a constraint and a decoration.
    """
    assert source_namespace_for("VSMPE") == "19_VSMPE"
    assert source_namespace_for("AC") == "01_AC"

    assert source_namespace_for("CIL") == "quebec"
    assert source_namespace_for("RIV") == "quebec"
    assert source_namespace_for("SAG") == "saguenay"

    # Never empty, for any key the axis will accept.
    for key in known_neighborhoods():
        assert source_namespace_for(key)

    with pytest.raises(KeyError, match="Unknown neighborhood"):
        source_namespace_for("Nowhere")


def test_quebec_zone_codes_are_unique_city_wide_so_one_namespace_is_enough():
    """Why collapsing six arrondissements onto `quebec` is safe.

    The leading digit of a Quebec City zone code *is* the arrondissement, so
    two arrondissements cannot publish the same code. That is the property that
    lets `source_namespace` be the city here and still be a real qualifier -
    and if it ever stopped holding, this test is what would say so.
    """
    assert borough_prefix("11005Mb") == "1"
    assert borough_prefix("21005Mb") == "2"
    assert borough_prefix("11005Mb") != borough_prefix("21005Mb")


def test_the_metric_projection_follows_the_city():
    assert metric_crs_for("VSMPE") == "EPSG:32188"
    assert metric_crs_for("CIL") == "EPSG:32187"
    assert metric_srid_for("CIL") == 32187
    assert cmhc_centre_for("CIL") == "Québec"


def test_every_default_key_is_known_and_has_a_cmhc_crosswalk():
    for key in DEFAULT_NEIGHBORHOODS:
        assert key in known_neighborhoods()
        assert quartiers_for(key)


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
        ("Normes de densité", "", "Nb de log. à l'hectare min. (log/ha)"),
        ("", "", "Nb de log. à l'hectare max. (log/ha)"),
        ("", "", "Sup. max. de plancher Vente au détail par bâtiment (m²)"),
        ("", "", "Sup. max. de plancher Adminstration par bâtiment (m²)"),
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
    "Nb de log. à l'hectare min. (log/ha)": "40",
    "Nb de log. à l'hectare max. (log/ha)": "100",
    # Retail and administration, differing, the way 316 of the borough's zones
    # print them. The tighter of the two is the commerce family's ceiling.
    "Sup. max. de plancher Vente au détail par bâtiment (m²)": "2200",
    "Sup. max. de plancher Adminstration par bâtiment (m²)": "1100",
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
        # No storey count stated, so none is reported: the sheet prints the
        # *Nombre d'étages* row blank and so does this. The height is what
        # bounds the envelope, and the note says so.
        assert column.floors_max is None
        assert any(note.startswith("floors_max: not stated") for note in column.notes)
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
        # *Densité* is a floor-area ratio and the grid prints none; the
        # dwellings-per-hectare pair is its own norm and is carried as one.
        assert column.density_max is None
        assert column.dwelling_density_min_per_ha == 40.0
        assert column.dwelling_density_max_per_ha == 100.0
        # The tighter of the retail and administration ceilings.
        assert column.commercial_floor_max_m2 == 1100.0
        assert column.excluded_usages.startswith("La location")
        assert column.piia_sector == "PIIA"
        assert column.heritage_sector is None
        # The solver still gets a ceiling: 20 m at the shortest storey this
        # platform builds. Six, not the five a 3.5 m storey used to invent -
        # and `solve_program` enforces the 20 m itself, so a commercial stack
        # is still stopped at five by the metric cap rather than by this.
        assert column.to_zone_column().floors_max == 6
        assert column.to_zone_column().height_max_m == 20.0


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
    from hbu_dataplatform.rag.documents import DOCUMENT_SOURCES
    from hbu_dataplatform.cities.quebec_city.zoning import ZONING_SLUG

    assert GRID_URL_COLUMN == "LIEN_GRILLE"
    assert DOCUMENT_SOURCES[ZONING_SLUG] == GRID_URL_COLUMN
    assert "{zone}" in DEFAULT_SHEET_URL_TEMPLATE


# -- heritage ------------------------------------------------------------------

GRADE_DOMAIN = {
    "fields": [
        {"name": "NO_SEQ", "type": "esriFieldTypeInteger", "domain": None},
        {
            "name": "EVALUATION_VALEUR_PATRIMO_NO",
            "type": "esriFieldTypeSmallInteger",
            "domain": {
                "type": "codedValue",
                "codedValues": [
                    {"name": "exceptionnel", "code": 1},
                    {"name": "supérieur", "code": 2},
                    {"name": "présumé", "code": 5},
                ],
            },
        },
    ]
}


def test_a_coded_field_gets_its_label_beside_the_code():
    import pandas as pd

    from hbu_dataplatform.cities.quebec_city.zoning import (
        coded_values,
        label_coded_values,
    )

    labels = coded_values(GRADE_DOMAIN)
    assert list(labels) == ["EVALUATION_VALEUR_PATRIMO_NO"]

    frame = pd.DataFrame({"EVALUATION_VALEUR_PATRIMO_NO": [2, 5, 9]})
    out = label_coded_values(frame, labels)
    # The code stays as published; an unlisted one reads as missing.
    assert out["EVALUATION_VALEUR_PATRIMO_NO"].tolist() == [2, 5, 9]
    assert out["EVALUATION_VALEUR_PATRIMO_NO_LIBELLE"].tolist()[:2] == ["supérieur", "présumé"]
    assert pd.isna(out["EVALUATION_VALEUR_PATRIMO_NO_LIBELLE"].iloc[2])


def test_presume_and_confirme_are_not_grades():
    from hbu_dataplatform.cities.quebec_city.zoning import UNGRADED_CODES

    assert UNGRADED_CODES == {5, 6}


def test_a_fiche_url_is_built_from_the_fiche_number():
    from hbu_dataplatform.cities.quebec_city.zoning import fiche_url_for

    assert fiche_url_for(353) == (
        "https://www.ville.quebec.qc.ca/citoyens/patrimoine/bati/fiche.aspx?fiche=353"
    )
    assert fiche_url_for(353.0).endswith("=353")
    assert fiche_url_for(None) is None
    assert fiche_url_for(float("nan")) is None


def test_every_heritage_layer_reaches_lot_features():
    """A layer without one of `FEATURE_ID_COLUMNS` is skipped by the join -
    which is how Saguenay's zoning once went missing."""
    from hbu_dataplatform.cadastre.cadastre_assets import FEATURE_ID_COLUMNS
    from hbu_dataplatform.cities.quebec_city.zoning import (
        FICHE_URL_COLUMN,
        HERITAGE_ID_COLUMN,
        HERITAGE_LAYERS,
    )

    assert HERITAGE_ID_COLUMN in FEATURE_ID_COLUMNS
    # The fiche is not a by-law: the corpus must not download it.
    assert FICHE_URL_COLUMN != GRID_URL_COLUMN
    slugs = [layer.slug for layer in HERITAGE_LAYERS]
    assert len(set(slugs)) == len(slugs) == 7
    assert {layer.layer_id for layer in HERITAGE_LAYERS} == set(range(5, 12))


class HeritageSession(FakeSession):
    """Layer 11 holds two studied buildings, layer 6 fails, the rest are empty."""

    def post(self, url, data=None, timeout=None):
        self.posts.append({"url": url, **data})
        layer = url.rsplit("/", 2)[-2]
        if layer == "6":
            return FakeResponse({"error": {"message": "boom"}})
        if data.get("returnIdsOnly") == "true":
            return FakeResponse({"objectIds": [1, 2] if layer == "11" else []})
        ids = [int(i) for i in data["objectIds"].split(",")]
        return FakeResponse(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "OBJECTID": i,
                            "NO_SEQ": 350 + i,
                            "EVALUATION_VALEUR_PATRIMO_NO": {1: 2, 2: 5}[i],
                        },
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                        },
                    }
                    for i in ids
                ],
            }
        )

    def get(self, url, params=None, timeout=None):
        return FakeResponse(GRADE_DOMAIN)


def test_the_heritage_layers_land_beside_the_zoning(tmp_path):
    import logging
    from types import SimpleNamespace

    import pandas as pd

    from hbu_dataplatform.zoning.features_assets import _quebec_heritage

    session = HeritageSession()
    service = QuebecZoningClient(
        "https://example/FeatureServer/2", request_delay_seconds=0, session=session
    ).on_layer("https://example/CI_COMMUNAUTE_CULTURE_PATRIMOINE/FeatureServer")
    context = SimpleNamespace(log=logging.getLogger("test"))

    result = _quebec_heritage(
        context,
        service,
        {"rings": [[[0, 0], [1, 0], [1, 1], [0, 0]]], "spatialReference": {"wkid": 4326}},
        str(tmp_path),
        {"neighborhood": "CIL", "scrape_date": "2026-09-01"},
    )

    assert result["written"] == {"Patrimoine__BATIMENT_ETUDIE": 2}
    assert list(result["failed"]) == ["Patrimoine__IMMEUBLE_CLASSE"]
    # The one session carries every layer's requests.
    assert all(p["url"].endswith("/query") for p in session.posts)

    frame = pd.read_parquet(tmp_path / "Patrimoine__BATIMENT_ETUDIE.parquet")
    assert frame["ID"].tolist() == ["351", "352"]
    assert frame["EVALUATION_VALEUR_PATRIMO_NO_LIBELLE"].tolist() == ["supérieur", "présumé"]
    assert frame["LIEN_FICHE"].iloc[0].endswith("fiche=351")
    assert (frame["source_table"] == "Patrimoine__BATIMENT_ETUDIE").all()
    # Only the layer that had rows was written.
    assert [p.name for p in tmp_path.iterdir()] == ["Patrimoine__BATIMENT_ETUDIE.parquet"]
