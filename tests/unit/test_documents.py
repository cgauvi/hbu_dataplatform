"""Offline tests for PDF fetching and chunking.

The chunker is measured with a whitespace ruler rather than bge-m3's
tokenizer: the packing rules are what is under test, and a 2 GB download is
not something a unit test should need.
"""

from __future__ import annotations

import pandas as pd
import pytest

from hbu_dataplatform.rag import documents
from hbu_dataplatform.rag.documents import (
    DOCUMENT_SOURCES,
    MIN_PDF_BYTES,
    ZONING_SOURCES,
    DocumentError,
    PdfFetcher,
    chunk_text,
    document_id,
    document_urls,
    normalize_text,
    read_pdf,
)


class WordRuler:
    """A ``TokenRuler`` where one whitespace-delimited word is one token."""

    def count(self, text: str) -> int:
        return len(text.split())

    def split(self, text: str, max_tokens: int) -> list[str]:
        words = text.split()
        return [
            " ".join(words[start : start + max_tokens])
            for start in range(0, len(words), max_tokens)
        ]


class FakeResponse:
    def __init__(self, content, *, content_type="application/pdf"):
        self.content = content
        self.headers = {"Content-Type": content_type}

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls: list[str] = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        return self.response


def make_fetcher(tmp_path, response):
    session = FakeSession(response)
    fetcher = PdfFetcher(
        cache_dir=tmp_path, request_delay_seconds=0, session=session
    )
    return fetcher, session


def test_the_corpus_is_built_from_the_zoning_grids_and_the_resolutions():
    """The registry is the whole definition of what gets indexed.

    One grid table per city, and only Montreal's publishes the link itself.
    `_saguenay_features` resolves one grid id per zone and `_quebec_features`
    formats a handler URL from the zone code; both write it under Montreal's
    column name, which is what lets one corpus pipeline serve three cities.

    Plus Montreal's projets particuliers, which are the other half of "what
    may be built here": a PPCMOI resolution overrides the grid for one site,
    so a grid-only answer about such a site is confidently wrong.
    """
    assert DOCUMENT_SOURCES == {
        "Reglement_urbanisme__VSP_REG_ZONE": "LIEN_GRILLE",
        "Zonage__ZONAGE_SAGUENAY": "LIEN_GRILLE",
        "Zonage__ZONAGE_EN_VIGUEUR": "LIEN_GRILLE",
        "Reglement_urbanisme__VSP_REG_PPCMOI": "EN_SAVOIR_PLUS",
    }


def test_every_zoning_layer_is_also_a_document_source():
    """A subset, not an equality - and the difference is load-bearing.

    A table is a ZONE source if lots are cut against it, and a DOCUMENT source
    if it links prose worth retrieving. They were the same list until PPCMOI
    joined the corpus: a projet particulier is a document about one site and
    emphatically not a zone, so cutting lots against it would invent a zone
    piece per resolution - and the whole HBU chain is keyed on
    (lot_uid, feature_id), so that is a second, bogus development site for
    every lot a PPCMOI touches, not a cosmetic error.
    """
    assert set(ZONING_SOURCES) <= set(DOCUMENT_SOURCES)


def test_a_projet_particulier_is_not_a_zone_source():
    """Named explicitly, because deriving one list from the other is exactly
    the mistake this guards."""
    assert "Reglement_urbanisme__VSP_REG_PPCMOI" in DOCUMENT_SOURCES
    assert "Reglement_urbanisme__VSP_REG_PPCMOI" not in ZONING_SOURCES


def test_document_urls_are_distinct_and_keep_their_first_seen_order():
    frame = pd.DataFrame(
        {"LIEN_GRILLE": ["http://x/b.pdf", "http://x/a.pdf", "http://x/b.pdf", None]}
    )

    links = document_urls(frame, "LIEN_GRILLE")

    assert links == ["http://x/b.pdf", "http://x/a.pdf"]


def test_document_urls_rejects_a_missing_column():
    with pytest.raises(DocumentError):
        document_urls(pd.DataFrame({"OTHER": ["http://x"]}), "LIEN_GRILLE")


def test_document_id_is_stable_for_a_url():
    assert document_id("http://x/a.pdf") == document_id("http://x/a.pdf")
    assert document_id("http://x/a.pdf") != document_id("http://x/b.pdf")


def test_fetch_caches_by_url_and_does_not_hit_the_server_twice(tmp_path):
    body = b"%PDF-1.4 body"
    fetcher, session = make_fetcher(tmp_path, FakeResponse(body))

    first, cached = fetcher.fetch("http://x/a.pdf")
    second, cached_again = fetcher.fetch("http://x/a.pdf")

    assert first == second == body
    assert (cached, cached_again) == (False, True)
    assert session.calls == ["http://x/a.pdf"]


def test_fetch_rejects_an_html_body_served_as_a_dead_link(tmp_path):
    fetcher, _ = make_fetcher(
        tmp_path, FakeResponse(b"<html>404</html>", content_type="text/html")
    )

    with pytest.raises(DocumentError, match="not a PDF"):
        fetcher.fetch("http://x/gone.pdf")
    # Nothing poisons the cache for the next run.
    assert list(tmp_path.iterdir()) == []


def test_read_pdf_rejects_bytes_that_are_not_a_pdf():
    with pytest.raises(DocumentError, match="unreadable PDF"):
        read_pdf("http://x/a.pdf", b"not a pdf at all")


def test_normalize_text_rejoins_words_split_across_a_line_break():
    assert normalize_text("recomman-\ndation") == "recommandation"


def test_normalize_text_keeps_paragraph_breaks_but_collapses_runs():
    text = normalize_text("Article  1\n\n\n\nArticle   2")

    assert text == "Article 1\n\nArticle 2"


def test_chunk_text_keeps_whole_paragraphs_within_the_budget():
    text = "\n\n".join(["a b c", "d e f", "g h i"])

    chunks = chunk_text(text, WordRuler(), max_tokens=6, overlap_tokens=0)

    assert chunks == ["a b c\n\nd e f", "g h i"]


def test_chunk_text_repeats_a_trailing_paragraph_as_overlap():
    text = "\n\n".join(["a b c", "d e f", "g h i"])

    chunks = chunk_text(text, WordRuler(), max_tokens=6, overlap_tokens=3)

    assert chunks == ["a b c\n\nd e f", "d e f\n\ng h i"]


def test_chunk_text_splits_a_paragraph_that_overruns_the_budget_alone():
    chunks = chunk_text("a b c d e", WordRuler(), max_tokens=2, overlap_tokens=0)

    assert chunks == ["a b", "c d", "e"]


def test_chunk_text_counts_the_overlap_inside_the_budget():
    # The carried paragraph plus the incoming one would come to 9 tokens, so
    # the overlap is dropped rather than allowed to overrun `max_tokens`.
    text = "\n\n".join(["a b c", "d e f", "g h i j k l"])

    chunks = chunk_text(text, WordRuler(), max_tokens=6, overlap_tokens=3)

    assert chunks == ["a b c\n\nd e f", "g h i j k l"]


@pytest.mark.parametrize("max_tokens", [4, 6, 11])
def test_no_chunk_ever_exceeds_the_budget(max_tokens):
    text = "\n\n".join(f"{'w' * n} " * n for n in range(1, 12))

    chunks = chunk_text(text, WordRuler(), max_tokens=max_tokens, overlap_tokens=3)

    ruler = WordRuler()
    assert chunks and all(ruler.count(chunk) <= max_tokens for chunk in chunks)


def test_chunk_text_terminates_when_every_unit_fills_a_chunk():
    text = "\n\n".join(["a b", "c d", "e f"])

    chunks = chunk_text(text, WordRuler(), max_tokens=2, overlap_tokens=1)

    assert chunks == ["a b", "c d", "e f"]


def test_chunk_text_of_an_empty_document_is_no_chunks():
    assert chunk_text("   \n\n  ", WordRuler(), max_tokens=8, overlap_tokens=0) == []


@pytest.mark.parametrize(
    ("max_tokens", "overlap_tokens"),
    [(0, 0), (8, 8), (8, 9), (8, -1)],
)
def test_chunk_text_rejects_an_impossible_geometry(max_tokens, overlap_tokens):
    with pytest.raises(ValueError):
        chunk_text(
            "a b c",
            WordRuler(),
            max_tokens=max_tokens,
            overlap_tokens=overlap_tokens,
        )


def test_a_quebec_zone_row_is_cited_by_its_zone_code():
    """The id column each city's documents are cited by.

    Quebec City's layer carries `IGDS_TEXT_STRING` and neither of Montreal's
    two names, so this is what decides that a retrieved CIL passage says
    "11004Mc" rather than nothing. It publishes no usage description either,
    so the title is null - the one field a Quebec document is missing.
    """
    from hbu_dataplatform.rag.assets import _features_by_url

    frame = pd.DataFrame(
        {
            "LIEN_GRILLE": [
                "https://carte.ville.quebec.qc.ca/GrillesZonage/HandlerZonage.ashx?11004Mc",
                "https://carte.ville.quebec.qc.ca/GrillesZonage/HandlerZonage.ashx?11007Hb",
            ],
            "IGDS_TEXT_STRING": ["11004Mc", "11007Hb"],
            "NATURE": ["Zone", "Zone"],
            "STATUT": ["En vigueur", "En vigueur"],
        }
    )

    index = _features_by_url(frame, "LIEN_GRILLE")

    entry = index[
        "https://carte.ville.quebec.qc.ca/GrillesZonage/HandlerZonage.ashx?11004Mc"
    ]
    assert entry["feature_ids"] == '["11004Mc"]'
    assert entry["title"] is None


def test_montreals_zone_number_still_wins_where_both_columns_exist():
    """`_ID_COLUMNS` is first-match-wins, so the order is load-bearing."""
    from hbu_dataplatform.rag.assets import _features_by_url

    frame = pd.DataFrame(
        {
            "LIEN_GRILLE": ["http://x/C01-001.pdf"],
            "NUMERO_COMPLET": ["C01-001"],
            "IGDS_TEXT_STRING": ["11004Mc"],
            "USAGE": ["Commerce"],
        }
    )

    entry = _features_by_url(frame, "LIEN_GRILLE")["http://x/C01-001.pdf"]

    assert entry["feature_ids"] == '["C01-001"]'
    assert entry["title"] == "Commerce"


def test_a_publishers_empty_sheet_is_told_apart_from_a_scan():
    """Both are "a PDF with no text", and only one is an OCR candidate.

    Quebec City answers an unknown zone code with a valid, empty PDF of about
    850 bytes where a real sheet is 110-125 kB. Counted as a scan it inflates
    the OCR backlog with files that have nothing to read.

    The size test belongs here and not in `fetch`, because size alone says
    nothing: the empty sheet below is 600-odd bytes and so is a perfectly
    valid two-line council minute.
    """
    from test_quebec_council import make_pdf

    empty = make_pdf([])
    assert len(empty) < MIN_PDF_BYTES

    with pytest.raises(DocumentError, match="empty sheet"):
        read_pdf("http://x/unknown-zone.pdf", empty)


def test_a_big_pdf_with_no_text_is_still_an_ocr_candidate(monkeypatch):
    """The other half: past the threshold, no text means a scan.

    The threshold is lowered rather than the file padded - trailing bytes
    after a PDF's trailer make it unreadable, which is a third failure and not
    the one under test.
    """
    from test_quebec_council import make_pdf

    monkeypatch.setattr(documents, "MIN_PDF_BYTES", 1)

    with pytest.raises(DocumentError, match="would need OCR"):
        read_pdf("http://x/scanned.pdf", make_pdf([]))
