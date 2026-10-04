"""The three CUCQ assets, offline: a fake portal index listing two sittings,
one minute with a list and one scan, the placement and the database
recorded rather than run."""

from __future__ import annotations

import json
from contextlib import contextmanager

import pandas as pd
import pytest
from asset_helpers import stub_publish_by_neighborhood
from dagster import materialize
from test_documents import WordRuler
from test_quebec_council import make_pdf

from hbu_dataplatform.cities.quebec_city.cucq import assets as cucq_assets
from hbu_dataplatform.cities.quebec_city.cucq.assets import (
    cucq_decisions,
    cucq_minutes,
    cucq_minutes_chunks,
)
from hbu_dataplatform.cities.quebec_city.cucq.placement import Placement
from hbu_dataplatform.cities.quebec_city.cucq.portal import (
    DEFAULT_SEARCH_ENDPOINT,
    CucqPortalClient,
)
from hbu_dataplatform.cities.quebec_city.resources import CucqMinutesResource
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.rag.documents import PdfFetcher
from hbu_dataplatform.rag.resources import EmbeddingModel, PdfCache

DATE = "2026-08-01"
BLOB = "https://gpddocs.ville.quebec.qc.ca/gpdblob/"
MINUTE = BLOB + "PV_CUCQ_OR_2026-06-23.pdf"
SCAN = BLOB + "PV_CUCQ_OR_2006-05-02.pdf"
ARAGO = "467, Rue Arago Ouest"

MINUTE_LINES = [
    "proces-verbal",
    "Commission d'urbanisme et de conservation de Quebec",
    "2026-25",
    "Proces-verbal d'une seance de la Commission d'urbanisme et de conservation de Quebec, tenue le",
    "mardi 23 juin 2026 a 13 h 00 via Microsoft Teams.",
    "1. Ouverture de la seance",
    "6. Demandes de permis approuvees",
    "Resolution C.U. 2026-098",
    "Sur motion dument proposee et appuyee, il est unanimement resolu d'approuver les demandes.",
    "8. Demandes de permis refusees",
    "Resolution C.U. 2026-100",
    "Sur motion dument proposee et appuyee, il est unanimement resolu de refuser les demandes.",
    "9. Auditions",
    "Les requerants de la demande 20250428-007 sont entendus en audition.",
    "10. Cloture de la seance",
    "Liste des demandes ayant ete presentees a la Commission d'urbanisme et de conservation de Quebec",
    "Seance du 23 juin 2026",
    "Demande approuvee",
    "Martel, Steeve20250206-036",
    "1408, Avenue Harriet",
    "Construction d'un nouveau batiment d'habitation de 1 a 3 logements",
    "Demande refusee",
    "Devmico Construction Inc.20250428-007",
    "467, 469, 471, Rue Arago Ouest",
    "Demolition d'un batiment principal",
]

INDEX_PAYLOAD = {
    "@odata.count": 2,
    "value": [
        {
            "metadata_storage_name": "PV_CUCQ_OR_2026-06-23.pdf",
            "Numero": "PV_CUCQ_OR_2026-06-23.pdf",
            "Date": "2026-06-23",
            "Annee": "2026",
            "Objet": "Proc%C3%A8s-verbal%20de%20la%20s%C3%A9ance%20de%20la%20CUCQ%20tenue%20le%2023%20juin%202026",
            "Instance": "Commission%20d%27urbanisme%20et%20de%20conservation%20de%20Qu%C3%A9bec",
            "Type": "Proc%C3%A8s-verbaux",
            "Uniteadministrative": "Planification%20de%20l%27am%C3%A9nagement%20et%20de%20l%27environnement",
            "content": "",
        },
        {
            "metadata_storage_name": "PV_CUCQ_OR_2006-05-02.pdf",
            "Numero": "PV_CUCQ_OR_2006-05-02.pdf",
            "Date": "2006-05-02",
            "Annee": "2006",
            "Objet": "Proc%C3%A8s-verbal%20de%20la%20s%C3%A9ance%20de%20la%20CUCQ%20tenue%20le%202%20mai%202006",
            "Instance": "Commission%20d%27urbanisme%20et%20de%20conservation%20de%20Qu%C3%A9bec",
            "Type": "Proc%C3%A8s-verbaux",
            "Uniteadministrative": None,
            "content": "",
        },
    ],
}


class FakeResponse:
    def __init__(self, body, content_type, payload=None):
        self.content = body if isinstance(body, bytes) else body.encode("utf-8")
        self.headers = {"Content-Type": content_type}
        self.encoding = "utf-8"
        self.payload = payload

    @property
    def text(self):
        return self.content.decode("utf-8")

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeHost:
    """The index and the blob host, and a log of what was asked."""

    def __init__(self):
        self.calls: list[str] = []
        self.pdfs = {
            MINUTE: make_pdf(MINUTE_LINES),
            # A scan: a page with nothing on it.
            SCAN: make_pdf([]),
        }

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(url)
        if url.startswith(DEFAULT_SEARCH_ENDPOINT):
            assert headers["api-key"]
            return FakeResponse("{}", "application/json", payload=INDEX_PAYLOAD if params["$skip"] == "0" else {"value": []})
        try:
            return FakeResponse(self.pdfs[url], "application/pdf")
        except KeyError:
            raise AssertionError(f"unexpected fetch of {url}") from None


@pytest.fixture
def host(monkeypatch, tmp_path):
    fake = FakeHost()

    def pdf_fetcher(self):
        return PdfFetcher(cache_dir=tmp_path / "cache", request_delay_seconds=0, session=fake)

    def client(self):
        return CucqPortalClient(session=fake, request_delay_seconds=0, query_key="test-key")

    monkeypatch.setattr(PdfCache, "fetcher", pdf_fetcher)
    monkeypatch.setattr(CucqMinutesResource, "client", client)
    return fake


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "data"))


@pytest.fixture
def placed(monkeypatch):
    """The address lookup in PostGIS, answered from a dict: Arago is in CIL,
    Harriet is in a borough whose addresses are not loaded."""
    asked: list[tuple[str, int, str]] = []

    def place_addresses(connection, addresses):
        asked.extend(addresses)
        return {ARAGO: Placement(neighborhood="CIL", lot_number="1 303 691", match_basis="address")}

    monkeypatch.setattr(cucq_assets, "place_addresses", place_addresses)
    return asked


@pytest.fixture
def published(monkeypatch):
    return stub_publish_by_neighborhood(monkeypatch, cucq_assets)


@pytest.fixture
def word_ruler(monkeypatch):
    monkeypatch.setattr(EmbeddingModel, "ruler", lambda self: WordRuler())


@pytest.fixture
def stub_chunks_db(monkeypatch):
    """The chunks' upsert into silver.document_chunks, recorded per borough."""
    seen: dict[str, object] = {"upserts": [], "deletes": []}

    def upsert_frame(connection, dataset, frame, *, partition, scrape_date, prune=True):
        seen["upserts"].append((dataset, frame, partition, scrape_date, prune))
        return {"copied": len(frame), "duplicates": 0, "upserted": len(frame), "pruned": 0}

    class _Cursor:
        rowcount = 0

        def execute(self, sql, params=None):
            seen["deletes"].append((sql, params))

    class _Connection:
        def cursor(self):
            return _Cursor()

    @contextmanager
    def connect(self):
        yield _Connection()

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(cucq_assets, "upsert_frame", upsert_frame)
    return seen


def run(assets, store, tmp_path, *, config=None):
    ops = {"bronze__cucq_minutes": {"config": config}} if config else {}
    return materialize(
        assets,
        partition_key=DATE,
        resources={
            "store": store,
            "pdf_cache": PdfCache(cache_dir=str(tmp_path / "cache")),
            "cucq_source": CucqMinutesResource(),
            "postgis": PostgisResource(),
            "embedding_model": EmbeddingModel(max_tokens=40, overlap_tokens=0),
        },
        run_config={"ops": ops},
        raise_on_error=False,
    )


def read(store, asset_name, file):
    return pd.read_parquet(store.partition_dir(asset_name, DATE) + "/" + file)


# -- bronze -------------------------------------------------------------------


def test_the_minutes_land_one_row_per_sitting_scans_included(host, store, tmp_path):
    result = run([cucq_minutes], store, tmp_path)
    assert result.success
    frame = read(store, "cucq_minutes", "minutes.parquet")
    assert list(frame["url"]) == [MINUTE, SCAN]
    assert list(frame["series"]) == ["OR", "OR"]
    assert list(frame["meeting_date"]) == ["2026-06-23", "2006-05-02"]
    assert list(frame["has_text"]) == [True, False]
    assert list(frame["text_source"]) == ["pdf", "none"]
    minute, scan = frame.iloc[0], frame.iloc[1]
    assert "Rue Arago Ouest" in minute["text"]
    assert minute["objet"] == "Procès-verbal de la séance de la CUCQ tenue le 23 juin 2026"
    assert minute["administrative_unit"] == "Planification de l'aménagement et de l'environnement"
    assert minute["num_pages"] == 1 and json.loads(minute["links"]) == []
    # The scan is a row, with the file's size and hash and no text.
    assert scan["text"] == "" and scan["num_chars"] == 0
    assert scan["num_bytes"] > 0 and scan["content_sha256"]
    assert "no text" in scan["read_error"]
    metadata = result.asset_materializations_for_node("bronze__cucq_minutes")[0].metadata
    assert metadata["num_minutes"].value == 2
    assert metadata["num_textless"].value == 1
    assert metadata["num_failed"].value == 0
    assert metadata["by_series"].value == {"OR": 2}
    assert metadata["earliest_sitting"].value == "2006-05-02"
    assert host.calls.count(MINUTE) == 1


def test_a_series_filter_that_leaves_nothing_is_a_failure(host, store, tmp_path):
    result = run([cucq_minutes], store, tmp_path, config={"series": ["DEM"]})
    assert not result.success


def test_a_rerun_takes_the_pdfs_off_the_cache(host, store, tmp_path):
    assert run([cucq_minutes], store, tmp_path).success
    assert run([cucq_minutes], store, tmp_path).success
    assert host.calls.count(MINUTE) == 1
    metadata = run([cucq_minutes], store, tmp_path).asset_materializations_for_node("bronze__cucq_minutes")[0].metadata
    assert metadata["num_from_cache"].value == 2


# -- silver: the decisions ----------------------------------------------------


def test_the_decisions_are_read_placed_and_published_per_borough(host, store, tmp_path, placed, published):
    result = run([cucq_minutes, cucq_decisions], store, tmp_path)
    assert result.success
    frame = read(store, "cucq_decisions", "decisions.parquet")
    assert len(frame) == 2
    by_number = frame.set_index("request_number")

    arago = by_number.loc["20250428-007"]
    assert arago["applicant"] == "Devmico Construction Inc."
    assert arago["address_line"] == "467, 469, 471, Rue Arago Ouest"
    assert json.loads(arago["addresses"]) == ["467, Rue Arago Ouest", "469, Rue Arago Ouest", "471, Rue Arago Ouest"]
    assert arago["address_key"] == ARAGO
    assert arago["works_kind"] == "demolition_main" and bool(arago["is_demolition"])
    assert arago["decision"] == "refused" and arago["outcome"] == "refused"
    assert arago["resolution_number"] == "C.U. 2026-100"
    assert bool(arago["was_heard"])
    assert arago["sitting_number"] == "2026-25" and arago["meeting_date"] == "2026-06-23"
    assert arago["sitting_kind"] == "regular" and arago["series"] == "OR"
    # Placed: the borough and the lot of the door it was matched to.
    assert arago["neighborhood"] == "CIL" and arago["lot_number"] == "1 303 691" and arago["match_basis"] == "address"
    assert arago["title"].startswith("CUCQ, séance du 23 juin 2026 - 467, 469, 471, Rue Arago Ouest")
    assert "Décision : demande refusée (résolution C.U. 2026-100)" in arago["text"]

    harriet = by_number.loc["20250206-036"]
    assert harriet["decision"] == "approved" and harriet["resolution_number"] == "C.U. 2026-098"
    assert harriet["dwellings_min"] == 1 and harriet["dwellings_max"] == 3
    assert pd.isna(harriet["neighborhood"]) and pd.isna(harriet["lot_number"])

    # Every row has a corpus document of its own, and the minute's id too.
    assert frame["chunk_doc_id"].nunique() == 2
    assert frame["doc_id"].nunique() == 1
    # The lookup asked for both doors; only the placed borough was published.
    assert {key for key, _, _ in placed} == {ARAGO, "1408, Avenue Harriet"}
    assert published["dataset"] == "cucq_decisions"
    assert published["scrape_date"] == DATE
    assert list(published["frames"]) == ["CIL"]
    assert list(published["frames"]["CIL"]["request_number"]) == ["20250428-007"]

    metadata = result.asset_materializations_for_node("silver__cucq_decisions")[0].metadata
    assert metadata["num_decisions"].value == 2
    assert metadata["num_minutes_textless"].value == 1
    assert metadata["num_placed"].value == 1 and metadata["num_unplaced"].value == 1
    assert metadata["placed_by_neighborhood"].value == {"CIL": 1}
    assert metadata["unplaced_sample"].value == ["1408, Avenue Harriet"]
    assert metadata["by_decision"].value == {"approved": 1, "refused": 1}
    assert metadata["num_demolitions"].value == 1
    assert metadata["CIL_rows_upserted"].value == 1


def test_nothing_placed_publishes_nothing_and_keeps_the_parquet(host, store, tmp_path, monkeypatch, published):
    monkeypatch.setattr(cucq_assets, "place_addresses", lambda connection, addresses: {})
    result = run([cucq_minutes, cucq_decisions], store, tmp_path)
    assert result.success
    frame = read(store, "cucq_decisions", "decisions.parquet")
    assert len(frame) == 2 and frame["neighborhood"].isna().all()
    assert published["calls"] == 0


# -- silver: the corpus -------------------------------------------------------


def test_the_corpus_is_one_document_per_placed_decision(host, store, tmp_path, placed, published, word_ruler, stub_chunks_db):
    result = run([cucq_minutes, cucq_decisions, cucq_minutes_chunks], store, tmp_path)
    assert result.success
    chunks = read(store, "cucq_minutes_chunks", "chunks.parquet")
    decisions = read(store, "cucq_decisions", "decisions.parquet")
    arago = decisions[decisions["request_number"] == "20250428-007"].iloc[0]

    # Only the placed decision is in the corpus, under its own doc id.
    assert set(chunks["doc_id"]) == {arago["chunk_doc_id"]}
    assert set(chunks["neighborhood"]) == {"CIL"}
    assert set(chunks["source_table"]) == {"cucq_minutes"}
    assert set(chunks["title"]) == {arago["title"]}
    assert set(chunks["url"]) == {MINUTE}
    assert list(chunks["chunk_index"]) == list(range(len(chunks)))
    assert chunks["chunk_id"].iloc[0] == f"{arago['chunk_doc_id']}:0000"
    assert set(chunks["feature_ids"]) == {"[]"}
    assert set(chunks["minute_doc_id"]) == {arago["doc_id"]} and set(chunks["request_number"]) == {"20250428-007"}
    assert "Rue Arago Ouest" in " ".join(chunks["text"])

    # Upserted into the borough's document_chunks without pruning the other
    # corpora; the CUCQ rows alone are pruned by hand.
    ((dataset, frame, partition, scrape_date, prune),) = stub_chunks_db["upserts"]
    assert (dataset, partition, scrape_date, prune) == ("document_chunks", "CIL", DATE, False)
    assert len(frame) == len(chunks)
    ((sql, params),) = stub_chunks_db["deletes"]
    assert "source_table LIKE 'cucq\\_%%'" in sql
    assert params[:2] == ["CIL", DATE]

    metadata = result.asset_materializations_for_node("silver__cucq_minutes_chunks")[0].metadata
    assert metadata["num_documents"].value == 1
    assert metadata["num_decisions_unplaced"].value == 1
    assert metadata["chunks_by_neighborhood"].value == {"CIL": len(chunks)}


def test_an_unplaced_corpus_is_empty(host, store, tmp_path, monkeypatch, published, word_ruler, stub_chunks_db):
    monkeypatch.setattr(cucq_assets, "place_addresses", lambda connection, addresses: {})
    result = run([cucq_minutes, cucq_decisions, cucq_minutes_chunks], store, tmp_path)
    assert result.success
    assert read(store, "cucq_minutes_chunks", "chunks.parquet").empty
    assert stub_chunks_db["upserts"] == []
