"""The three council assets, offline: a fake host serving one council's
listing, two minutes, a fiche, a sommaire and a resolution."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from dagster import MultiPartitionKey, materialize

from asset_helpers import stub_publish
from test_quebec_council import FakeResponse, make_pdf
from hbu_dataplatform.cities.quebec_city.council import assets as council_assets
from hbu_dataplatform.cities.quebec_city.council.assets import (
    council_minutes,
    council_minutes_documents,
    council_planning_items,
)
from hbu_dataplatform.rag.documents import PdfFetcher
from hbu_dataplatform.cities.quebec_city.council.councils import CouncilFetcher
from hbu_dataplatform.cities.quebec_city.resources import CouncilMinutesResource
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.rag.resources import PdfCache

DATE = "2026-08-01"

LISTING = "https://affichagesite.villequebec.quebec/conseil-quartier/proces-verbaux/12"
MINUTE_JUNE = "https://affichagesite.villequebec.quebec/fichiers/5af2da14-d90e-4b3f-95b7-0c2104939eb1"
MINUTE_MAY = "https://affichagesite.villequebec.quebec/fichiers/a578eb37-8551-4f83-8c94-50288302abdc"
FICHE = "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917"
SOMMAIRE = "https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf"
EXTRACT = "https://gpddocs.ville.quebec.qc.ca/gpdblob/CA1-2025-0215.pdf"
YOUTUBE = "https://www.youtube.com/watch?v=Enbfsee-cvY"

LISTING_HTML = """<html><body><div class="page"><div id="texte">
<p>Cette page presente les proces-verbaux du Conseil de quartier de Montcalm</p>
<h2>2025</h2>
<ul class="colonnes-2 liste-proces-verbaux">
<li><a href="/fichiers/5af2da14-d90e-4b3f-95b7-0c2104939eb1" target="_blank" title="Proc&#xE8;s verbal du 16 juin 2025">16 juin <span class="note">(PDF : 340 Ko)</span></a></li>
<li><a href="/fichiers/a578eb37-8551-4f83-8c94-50288302abdc" target="_blank" title="Proc&#xE8;s verbal du 27 mai 2025">27 mai <span class="note">(PDF : 317 Ko)</span></a></li>
</ul></div></div></body></html>"""

FICHE_HTML = f"""<html><head><script>var x = 1;</script></head><body>
<h1>Fiche</h1><h1>Augmentation du nombre de logements au 355, boulevard Rene-Levesque Ouest</h1>
<h3>Renseignements sur le projet</h3>
<p>La reglementation en vigueur dans la zone 14040Hb limite le nombre maximal de logements a 8 alors que le projet en propose 10.</p>
<h3>Documentation</h3>
<ul><li><a href="{SOMMAIRE}">Sommaire decisionnel</a></li>
<li><a href="/citoyens/participation-citoyenne/activites/CPFichierAzure.ashx?Fichier=0d7f8e36.jpg">Plan</a></li></ul>
<p>Adoption du reglement : 7 juillet 2025</p>
</body></html>"""

MINUTE_JUNE_LINES = [
    "Sixieme assemblee ordinaire du Conseil de quartier Montcalm",
    "Le lundi 16 juin 2025, 19h",
    "Ordre du jour",
    "1. Ouverture de l'assemblee.",
    "2. Lecture et adoption de l'ordre du jour.",
    "3. Consultation publique et demande d'opinion",
    "4. Tresorerie",
    "5. Levee de l'assemblee.",
    "Proces-verbal",
    "1. Ouverture de l'assemblee a 19h.",
    "2. Lecture et adoption de l'ordre du jour.",
    "3. Consultation publique et demande d'opinion",
    "Augmentation du nombre de logements au 355, boulevard Rene-Levesque Ouest",
    "La reglementation en vigueur dans la zone 14040Hb limite le nombre maximal",
    "de logements a 8 alors que le projet en propose 10.",
    "Le Conseil de quartier Montcalm est d'accord avec l'augmentation du nombre",
    "de logements a 10 dans la zone 14040Hb.",
    "4. Tresorerie",
    "Le solde au compte est de 3 228 $.",
    "5. Levee de l'assemblee a 21h.",
]
MINUTE_MAY_LINES = [
    "Cinquieme assemblee ordinaire du Conseil de quartier Montcalm",
    "1. Ouverture de l'assemblee.",
    "2. Tresorerie",
    "3. Levee de l'assemblee.",
    "Proces-verbal",
    "1. Ouverture de l'assemblee a 19h.",
    "2. Tresorerie",
    "Le solde au compte est de 4 010 $.",
    "3. Levee de l'assemblee a 20h.",
]
SOMMAIRE_LINES = [
    "sommaire decisionnel",
    "IDENTIFICATION GT2025-233 Numero :",
    "Gestion du territoire Unite administrative responsable",
    "Adoption du Reglement modifiant le Reglement de l'Arrondissement de la Cite-Limoilou sur l'urbanisme",
    "relativement a la zone 14040Hb, R.C.A.1V.Q. 549 (355, boulevard Rene-Levesque Ouest)",
    "Objet",
    "30 Avril 2025 Date :",
    "La presente modification au reglement vise a augmenter le nombre maximal de logements autorise par",
    "batiment de 8 logements a 10 logements.",
    "No de dossier 5809",
    "2 De donner un avis de motion relativement au Reglement.",
]
EXTRACT_LINES = [
    "SEANCE DU CONSEIL D'ARRONDISSEMENT",
    "Extrait du proces-verbal de la seance ordinaire, tenue le lundi 7 juillet 2025 a 17 h 30",
    "CA1-2025-0215 Adoption du Reglement modifiant le Reglement de l'Arrondissement de la",
    "Cite-Limoilou sur l'urbanisme relativement a la zone 14040Hb, R.C.A.1V.Q. 549 - GT2025-233",
    "il est resolu d'adopter le Reglement modifiant le Reglement de l'Arrondissement.",
    "Adoptee a l'unanimite",
]


class FakeHost:
    """Every URL the three assets may ask for, and a log of what was asked."""

    def __init__(self):
        self.calls: list[str] = []
        self.pages = {
            LISTING: FakeResponse(LISTING_HTML, "text/html; charset=utf-8"),
            FICHE: FakeResponse(FICHE_HTML, "text/html; charset=utf-8"),
            MINUTE_JUNE: FakeResponse(make_pdf(MINUTE_JUNE_LINES, links=[FICHE + "#texte", YOUTUBE]), "application/pdf"),
            MINUTE_MAY: FakeResponse(make_pdf(MINUTE_MAY_LINES), "application/pdf"),
            SOMMAIRE: FakeResponse(make_pdf(SOMMAIRE_LINES, links=[EXTRACT]), "application/pdf"),
            EXTRACT: FakeResponse(make_pdf(EXTRACT_LINES, links=[SOMMAIRE]), "application/pdf"),
        }

    def get(self, url, timeout=None):
        self.calls.append(url)
        try:
            return self.pages[url]
        except KeyError:
            raise AssertionError(f"unexpected fetch of {url}") from None


@pytest.fixture
def host(monkeypatch, tmp_path):
    fake = FakeHost()

    def pdf_fetcher(self):
        return PdfFetcher(cache_dir=tmp_path / "cache", request_delay_seconds=0, session=fake)

    def council_fetcher(self, pdf_fetcher):
        return CouncilFetcher(pdf_fetcher, request_delay_seconds=0, session=fake)

    # Patched on the classes: Dagster rebuilds the resources before the run.
    monkeypatch.setattr(PdfCache, "fetcher", pdf_fetcher)
    monkeypatch.setattr(CouncilMinutesResource, "fetcher", council_fetcher)
    return fake


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "data"))


def run(assets, store, tmp_path, *, neighborhood="CIL", council_ids=(12,)):
    config = {"config": {"council_ids": list(council_ids)}}
    configured = {"bronze__council_minutes", "bronze__council_minutes_documents"}
    ops = {a.node_def.name: config for a in assets if a.node_def.name in configured}
    return materialize(
        assets,
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": neighborhood}),
        resources={
            "store": store,
            "pdf_cache": PdfCache(cache_dir=str(tmp_path / "cache")),
            "council_minutes_source": CouncilMinutesResource(),
            "postgis": PostgisResource(),
        },
        run_config={"ops": ops},
        raise_on_error=False,
    )


def test_the_minutes_land_as_one_row_per_pdf_with_their_links(host, store, tmp_path):
    result = run([council_minutes], store, tmp_path)
    assert result.success
    frame = pd.read_parquet(store.partition_dir("council_minutes", DATE, "CIL") + "/minutes.parquet")
    assert list(frame["url"]) == [MINUTE_JUNE, MINUTE_MAY]
    assert list(frame["meeting_date"]) == ["2025-06-16", "2025-05-27"]
    assert list(frame["council_id"]) == [12, 12]
    assert frame["council_name"].iloc[0] == "Montcalm"
    assert "zone 14040Hb" in frame["text"].iloc[0]
    # Every link the PDF carries, as published - the policy of what to follow
    # is the next asset's.
    assert json.loads(frame["links"].iloc[0]) == [FICHE + "#texte", YOUTUBE]
    assert json.loads(frame["links"].iloc[1]) == []
    metadata = result.asset_materializations_for_node("bronze__council_minutes")[0].metadata
    assert metadata["num_minutes"].value == 2
    assert metadata["councils"].value == {"Montcalm": 2}
    assert metadata["num_failed"].value == 0


def test_a_borough_without_councils_writes_an_empty_file(host, store, tmp_path):
    result = run([council_minutes], store, tmp_path, neighborhood="VSMPE", council_ids=())
    assert result.success
    frame = pd.read_parquet(store.partition_dir("council_minutes", DATE, "VSMPE") + "/minutes.parquet")
    assert frame.empty
    assert host.calls == []


def test_a_council_outside_the_borough_is_refused(host, store, tmp_path):
    result = run([council_minutes], store, tmp_path, neighborhood="CIL", council_ids=(2,), )
    assert not result.success


def test_the_trail_follows_the_fiche_to_the_sommaire_to_the_extract(host, store, tmp_path):
    result = run([council_minutes, council_minutes_documents], store, tmp_path)
    assert result.success
    frame = pd.read_parquet(store.partition_dir("council_minutes_documents", DATE, "CIL") + "/documents.parquet")
    by_url = frame.set_index("url")
    assert list(by_url.index) == [FICHE, SOMMAIRE, EXTRACT]
    assert list(by_url["kind"]) == ["fiche", "gpd", "gpd"]
    assert list(by_url["depth"]) == [1, 2, 3]
    assert by_url.loc[SOMMAIRE, "document_number"] == "GT2025-233"
    assert by_url.loc[SOMMAIRE, "parent_url"] == FICHE
    assert by_url.loc[EXTRACT, "parent_doc_id"] == by_url.loc[SOMMAIRE, "doc_id"]
    # The fiche's anchor is canonicalised away, its text kept, its photo not
    # followed; YouTube is counted as ignored; the extract's link back to the
    # sommaire is a URL already seen.
    assert by_url.loc[FICHE, "title"].startswith("Augmentation du nombre de logements")
    assert "propose 10" in by_url.loc[FICHE, "text"]
    minutes = pd.read_parquet(store.partition_dir("council_minutes", DATE, "CIL") + "/minutes.parquet")
    june_id = minutes["doc_id"].iloc[0]
    assert all(json.loads(ids) == [june_id] for ids in by_url["minutes_doc_ids"])
    metadata = result.asset_materializations_for_node("bronze__council_minutes_documents")[0].metadata
    assert metadata["num_documents"].value == 3
    assert metadata["num_links_ignored"].value == 1
    assert metadata["kinds"].value == {"gpd": 2, "fiche": 1}
    # The PDFs came off the shared cache the second time they were asked for.
    assert host.calls.count(SOMMAIRE) == 1


def test_the_planning_items_are_one_per_agenda_item_and_one_per_document(host, store, tmp_path, monkeypatch):
    seen = stub_publish(monkeypatch, council_assets)
    result = run([council_minutes, council_minutes_documents, council_planning_items], store, tmp_path)
    assert result.success
    frame = pd.read_parquet(store.partition_dir("council_planning_items", DATE, "CIL") + "/items.parquet")

    assert seen["partition"] == ("CIL", DATE)
    assert list(seen["datasets"]) == ["council_planning_items"]

    minute_items = frame[frame["source_kind"] == "minutes"]
    # The June minute's item 3 is the only agenda item about planning; the
    # treasury and the May minute yield nothing.
    assert list(minute_items["item_index"]) == [3]
    consultation = minute_items.iloc[0]
    assert consultation["item_kind"] == "zoning_amendment"
    assert consultation["council_name"] == "Montcalm" and consultation["meeting_date"] == "2025-06-16"
    assert json.loads(consultation["subject_zone_codes"]) == ["14040Hb"]
    assert consultation["max_dwellings_before"] == 8 and consultation["max_dwellings_after"] == 10
    assert consultation["council_opinion"] == "favorable"
    assert consultation["decision"] == "consultation"

    documents = frame[frame["source_kind"] != "minutes"]
    assert sorted(documents["source_kind"]) == ["fiche", "gpd", "gpd"]
    assert set(documents["document_number"].dropna()) == {"GT2025-233", "CA1-2025-0215"}
    sommaire = frame[frame["document_number"] == "GT2025-233"].iloc[0]
    assert sommaire["decision"] == "notice_of_motion"
    assert sommaire["file_number"] == "5809"
    assert json.loads(sommaire["bylaw_numbers"]) == ["R.C.A.1V.Q. 549"]
    assert sommaire["max_dwellings_before"] == 8 and sommaire["max_dwellings_after"] == 10
    extract = frame[frame["document_number"] == "CA1-2025-0215"].iloc[0]
    assert extract["decision"] == "adopted" and extract["decision_date"] == "2025-07-07"
    # Every trail row remembers the minute and the council it was reached from.
    assert (frame["council_id"] == 12).all()
    fiche = frame[frame["source_kind"] == "fiche"].iloc[0]
    assert fiche["title"].startswith("Augmentation du nombre de logements")
    assert fiche["max_dwellings_after"] == 10

    metadata = result.asset_materializations_for_node("silver__council_planning_items")[0].metadata
    assert metadata["num_from_minutes"].value == 1
    assert metadata["num_from_documents"].value == 3
    assert metadata["num_agenda_items_dropped"].value == 4 + 3
    assert metadata["num_minutes_without_items"].value == 1
    assert metadata["num_with_dwelling_cap"].value == 3


def test_the_items_survive_without_a_trail(host, store, tmp_path, monkeypatch):
    """A partition whose trail asset never ran still reads its minutes."""
    stub_publish(monkeypatch, council_assets)
    assert run([council_minutes], store, tmp_path).success
    result = materialize(
        [council_planning_items],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": "CIL"}),
        resources={"store": store, "postgis": PostgisResource()},
    )
    assert result.success
    frame = pd.read_parquet(store.partition_dir("council_planning_items", DATE, "CIL") + "/items.parquet")
    assert list(frame["source_kind"]) == ["minutes"]
