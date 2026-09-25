"""Offline tests for PDF fetching and chunking.

The chunker is measured with a whitespace ruler rather than bge-m3's
tokenizer: the packing rules are what is under test, and a 2 GB download is
not something a unit test should need.
"""

from __future__ import annotations

import pandas as pd
import pytest

from hbu_dataplatform.rag.documents import (
    DOCUMENT_SOURCES,
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


def test_the_corpus_is_built_from_the_zoning_grids():
    """The registry is the whole definition of what gets indexed.

    One table per city, and only Montreal's publishes the link itself.
    `_saguenay_features` resolves one grid id per zone and
    `_quebec_features` formats a handler URL from the zone code; both write it
    under Montreal's column name, which is what lets one corpus pipeline serve
    three cities.
    """
    assert DOCUMENT_SOURCES == {
        "Reglement_urbanisme__VSP_REG_ZONE": "LIEN_GRILLE",
        "Zonage__ZONAGE_SAGUENAY": "LIEN_GRILLE",
        "Zonage__ZONAGE_EN_VIGUEUR": "LIEN_GRILLE",
    }


def test_every_zoning_layer_is_also_a_document_source():
    """True today, and the names stay apart because it need not be.

    A table is a zone source if lots are cut against it and a document source
    if it links prose worth retrieving. Quebec City was a zone source and not
    a document source until its per-zone sheet was found.
    """
    assert set(ZONING_SOURCES) == set(DOCUMENT_SOURCES)


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
    fetcher, session = make_fetcher(tmp_path, FakeResponse(b"%PDF-1.4 body"))

    first, cached = fetcher.fetch("http://x/a.pdf")
    second, cached_again = fetcher.fetch("http://x/a.pdf")

    assert first == second == b"%PDF-1.4 body"
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
    from hbu_dataplatform.rag_assets import _features_by_url

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
    from hbu_dataplatform.rag_assets import _features_by_url

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
