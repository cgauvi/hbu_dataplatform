"""The council corpus: how a minute is chunked, named, and tied back to the
agenda items read out of it - with a whitespace ruler, as test_documents
measures the chunker."""

from __future__ import annotations

import pytest

from hbu_dataplatform.cities.quebec_city.council.corpus import (
    chunk_council_document,
    chunk_ids_covering,
    chunk_spans,
    council_title,
    french_date,
    is_council_source_table,
    source_table_for,
)
from hbu_dataplatform.cities.quebec_city.council.items import item_spans, repair_text
from hbu_dataplatform.rag.documents import Document
from test_documents import WordRuler

MINUTE = "\n".join(
    [
        "Sixième assemblée ordinaire du Conseil de quartier Montcalm",
        "Le lundi 16 juin 2025, 19h",
        "",
        "Ordre du jour",
        "1. Ouverture de l'assemblée.",
        "2. Lecture et adoption de l'ordre du jour.",
        "3. Consultation publique et demande d'opinion",
        "4. Trésorerie",
        "5. Levée de l'assemblée.",
        "",
        "Procès-verbal",
        "",
        "1. Ouverture de l'assemblée à 19h.",
        "",
        "2. Lecture et adoption de l'ordre du jour.",
        "",
        "3. Consultation publique et demande d'opinion",
        "Augmentation du nombre de logements au 355, boulevard René-Lévesque Ouest",
        "La réglementation en vigueur dans la zone 14040Hb limite le nombre maximal",
        "de logements à 8 alors que le projet en propose 10.",
        "",
        "Le Conseil de quartier Montcalm est d'accord avec l'augmentation du nombre",
        "de logements à 10 dans la zone 14040Hb.",
        "",
        "4. Trésorerie",
        "Le solde au compte est de 3 228 $.",
        "",
        "5. Levée de l'assemblée à 21h.",
    ]
)


def _document(text: str) -> Document:
    return Document(doc_id="abc123", url="https://x/y.pdf", text=text, num_pages=1, content_sha256="", num_bytes=0)


def test_source_tables_share_one_prefix():
    assert source_table_for("minutes") == "council_minutes"
    assert source_table_for("gpd") == "council_gpd"
    assert is_council_source_table("council_consultation_file")
    assert not is_council_source_table("Zonage__ZONAGE_EN_VIGUEUR")
    with pytest.raises(ValueError):
        source_table_for("newsletter")


def test_titles_name_the_document_by_what_it_is():
    assert council_title("minutes", council_name="Montcalm", meeting_date="2025-06-16") == (
        "Procès-verbal du conseil de quartier de Montcalm, 16 juin 2025"
    )
    assert council_title("gpd", document_number="GT2025-233", text="Sommaire décisionnel\nAdoption du Règlement modifiant\nObjet") == (
        "Sommaire décisionnel GT2025-233 - Sommaire décisionnel"
    )
    assert council_title("gpd", document_number="CA1-2025-0215") == "Résolution CA1-2025-0215"
    assert council_title("fiche", title="Augmentation du nombre de logements") == "Augmentation du nombre de logements"
    assert french_date(None) is None


def test_chunks_are_cut_from_the_repaired_text_and_found_in_it():
    text, chunks = chunk_council_document(_document(MINUTE), WordRuler(), max_tokens=30, overlap_tokens=0)
    assert text == repair_text(MINUTE)
    assert len(chunks) > 2
    spans = chunk_spans(text, [chunk.text for chunk in chunks])
    assert all(exact for _, _, exact in spans)
    # Each chunk's span reads back as its own text, modulo the paragraph join.
    for chunk, (start, end, _) in zip(chunks, spans):
        assert text[start:end].split() == chunk.text.split()
    # And they are in order.
    assert [s for s, _, _ in spans] == sorted(s for s, _, _ in spans)


def test_an_item_is_covered_by_the_chunks_overlapping_its_span():
    text, chunks = chunk_council_document(_document(MINUTE), WordRuler(), max_tokens=30, overlap_tokens=0)
    spans = chunk_spans(text, [chunk.text for chunk in chunks])
    ids = [chunk.chunk_id for chunk in chunks]
    by_item = {index: (start, end) for index, start, end in item_spans(MINUTE)}
    consultation = chunk_ids_covering(by_item[3], spans, ids)
    assert consultation, "the consultation item has to be covered by at least one chunk"
    covered = " ".join(chunks[ids.index(c)].text for c in consultation)
    # `repair_text` writes the typographic apostrophe.
    assert "14040Hb" in covered and "est d’accord" in covered
    # The treasury's chunks are not the consultation's, unless one chunk
    # straddles the heading - in which case it is cited for both, which is
    # the right answer for a passage that holds both.
    treasury = chunk_ids_covering(by_item[4], spans, ids)
    assert treasury
    assert set(treasury) - set(consultation) or set(consultation) - set(treasury)


def test_a_chunk_that_cannot_be_located_takes_the_previous_end_and_says_so():
    text = "Un paragraphe.\n\nUn deuxième paragraphe assez long pour compter.\n\nUn troisième."
    spans = chunk_spans(text, ["Un paragraphe.", "texte décodé autrement par le tokenizer", "Un troisième."])
    assert spans[0] == (0, len("Un paragraphe."), True)
    assert spans[1][2] is False and spans[1][0] == spans[0][1]
    assert spans[2][2] is True and text[spans[2][0]:spans[2][1]] == "Un troisième."


def test_item_spans_cover_the_body_in_order():
    spans = item_spans(MINUTE)
    assert [index for index, _, _ in spans] == [1, 2, 3, 4, 5]
    text = repair_text(MINUTE)
    assert text[spans[2][1]:spans[2][2]].startswith("3. Consultation publique")
    # An item ends where its text does, before the blank line the next one
    # starts after.
    assert spans[2][2] <= spans[3][1] and text[spans[2][2]:spans[3][1]].strip() == ""
    # A minute with no agenda is one item over the whole text.
    assert item_spans("Rien à signaler.") == [(0, 0, len("Rien à signaler."))]
