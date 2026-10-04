"""The minutes and their trail as a corpus: how a council document is cut
into chunks, named, and tied back to the planning items read out of it.

The zoning corpus (`hbu_dataplatform.rag`) chunks a grid and cites it by the
zones that link to it. A procès-verbal is cited by nothing - no map feature
links a minute - and it is *about* several things at once: five agenda
items, each its own decision. So the corpus has to be joinable the other
way round, from a planning item to the passages that state it, and that is
what this module adds on top of the chunker:

* **the same text on both sides.** `items.read_minutes` reads a span of
  `items.repair_text(text)`, so the chunks are cut from that repaired text
  too, and an item and a chunk overlap, or do not, by character offset.
  Chunking the bronze text instead would put every offset a few characters
  out by the end of a long minute.
* **`chunk_spans`** finds each chunk in the text it was cut from. A chunk is
  its paragraphs joined by a blank line, and a paragraph is a verbatim
  slice of the text - except when the tokenizer had to cut one that
  overran the budget on its own, whose pieces are decoded tokens. So the
  first paragraph is searched for whole, then by its opening characters,
  and a chunk found neither way takes the previous one's end, which is
  close and is said to be a guess.
* **`chunk_ids_covering`** is the join: the chunks whose span overlaps an
  item's span, in document order. That list is what
  `council_planning_items` writes into `citations.chunk_ids`.

`source_table_for` names the corpus the chunks file under. `rag.chunks`
distinguishes publishers by `source_table` - the zoning grids are
`Zonage__ZONAGE_EN_VIGUEUR` - and a reader of the corpus should be able to
ask for the minutes alone, or for the sommaires alone, so each document
kind is its own table name under one prefix.
"""

from __future__ import annotations

import re
from datetime import date

from hbu_dataplatform.cities.quebec_city.council.items import first_title, repair_text
from hbu_dataplatform.rag.documents import Chunk, Document, TokenRuler, chunk_document

#: Every council source table starts with this, which is how a reader asks
#: for "the council corpus" without listing the kinds.
SOURCE_TABLE_PREFIX = "council_"

#: The document kinds bronze/council_minutes_documents files, plus the
#: minutes themselves - `council_minutes_documents.kind`.
DOCUMENT_KINDS: tuple[str, ...] = ("minutes", "fiche", "gpd", "consultation_file", "council_file")

#: How many words of a paragraph's head and tail identify it in the text it
#: was cut from. Matched on words rather than characters because a paragraph
#: the tokenizer had to split comes back with its line breaks as spaces, and
#: a chunk's own paragraphs are joined by a blank line the text may not have
#: exactly so - whitespace is the one thing the two sides do not agree on.
_LOCATE_WORDS = 12

_MONTHS_FR = (
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
)

_GPD_SOMMAIRE = re.compile(r"^GT\d{4}-\d{3,4}$")
_GPD_RESOLUTION = re.compile(r"^CA\d-\d{4}-\d{4}$")
_GPD_MOTION = re.compile(r"^AM\d-\d{4}-\d{4}$")


def source_table_for(kind: str) -> str:
    """``council_minutes``, ``council_gpd``, ``council_fiche``, ...: the
    `rag.chunks.source_table` a document of ``kind`` files under."""
    kind = (kind or "").strip().lower()
    if kind not in DOCUMENT_KINDS:
        raise ValueError(f"{kind!r} is not a council document kind {DOCUMENT_KINDS}")
    return f"{SOURCE_TABLE_PREFIX}{kind}"


def is_council_source_table(source_table: str | None) -> bool:
    return bool(source_table) and str(source_table).startswith(SOURCE_TABLE_PREFIX)


def french_date(value: date | str | None) -> str | None:
    """``16 juin 2025`` for a date, None for none."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        value = date.fromisoformat(value[:10])
    return f"{value.day} {_MONTHS_FR[value.month - 1]} {value.year}"


def council_title(
    kind: str,
    *,
    council_name: str | None = None,
    meeting_date: date | str | None = None,
    document_number: str | None = None,
    title: str | None = None,
    text: str | None = None,
) -> str | None:
    """What a chunk of this document is titled in the corpus.

    A minute is named by its council and its assembly; a GPD document by
    what its number says it is - a sommaire, a resolution, a notice of
    motion - and that number; a fiche by its own heading; anything else by
    the first line of its text that reads like a title. The title goes
    into `rag.chunks.title`, which the lexical arm of the search weights
    highest, so "sommaire GT2025-233" finds the document by name.
    """
    if kind == "minutes":
        when = french_date(meeting_date)
        name = f"Procès-verbal du conseil de quartier de {council_name}" if council_name else "Procès-verbal du conseil de quartier"
        return f"{name}, {when}" if when else name
    heading = (title or "").strip() or (first_title(repair_text(text)) if text else None)
    if document_number:
        if _GPD_SOMMAIRE.match(document_number):
            label = f"Sommaire décisionnel {document_number}"
        elif _GPD_RESOLUTION.match(document_number):
            label = f"Résolution {document_number}"
        elif _GPD_MOTION.match(document_number):
            label = f"Avis de motion {document_number}"
        else:
            label = document_number
        return f"{label} - {heading}" if heading else label
    if kind == "consultation_file" and heading:
        return f"Consultation - {heading}"
    return heading or None


def chunk_council_document(
    document: Document,
    ruler: TokenRuler,
    *,
    max_tokens: int,
    overlap_tokens: int,
) -> tuple[str, list[Chunk]]:
    """``(repaired text, chunks)``: the document chunked from the text the
    planning items are read from, so `chunk_spans` lands on it."""
    text = repair_text(document.text)
    repaired = Document(
        doc_id=document.doc_id,
        url=document.url,
        text=text,
        num_pages=document.num_pages,
        content_sha256=document.content_sha256,
        num_bytes=document.num_bytes,
    )
    return text, chunk_document(repaired, ruler, max_tokens=max_tokens, overlap_tokens=overlap_tokens)


def chunk_spans(text: str, chunks: list[str]) -> list[tuple[int, int, bool]]:
    """``[(start, end, exact)]`` for each chunk in ``text``, in order.

    ``exact`` is False for a chunk placed by the previous one's end because
    neither its first paragraph nor that paragraph's head could be found -
    which happens when the tokenizer cut an oversized paragraph and decoded
    its pieces a little differently from the source.
    """
    spans: list[tuple[int, int, bool]] = []
    cursor = 0
    previous_end = 0
    for chunk in chunks:
        words = chunk.split()
        found = _locate(text, words[:_LOCATE_WORDS], cursor)
        exact = found is not None
        start = found[0] if exact else previous_end
        tail = _locate(text, words[-_LOCATE_WORDS:], start)
        end = tail[1] if tail is not None else min(len(text), start + len(chunk))
        if end < start:
            end = min(len(text), start + len(chunk))
        spans.append((start, end, exact))
        # Chunks overlap by their trailing paragraphs, so the next one may
        # start inside this one - but never at or before where this began.
        cursor = start + 1
        previous_end = end
    return spans


def _locate(text: str, words: list[str], cursor: int) -> tuple[int, int] | None:
    """``(start, end)`` of ``words`` in ``text`` from ``cursor``, any
    whitespace between them, or None."""
    if not words:
        return None
    pattern = re.compile(r"\s+".join(re.escape(word) for word in words))
    match = pattern.search(text, cursor)
    return (match.start(), match.end()) if match else None


def chunk_ids_covering(
    span: tuple[int, int],
    spans: list[tuple[int, int, bool]],
    chunk_ids: list[str],
) -> list[str]:
    """The chunks whose span overlaps ``span``, in document order.

    An item that ends exactly where a chunk begins does not overlap it: the
    test is on an open interval, so a chunk carrying only the next
    heading is not cited for the item before it.
    """
    start, end = span
    return [
        chunk_id
        for (chunk_start, chunk_end, _), chunk_id in zip(spans, chunk_ids)
        if chunk_start < end and chunk_end > start
    ]


__all__ = [
    "DOCUMENT_KINDS",
    "SOURCE_TABLE_PREFIX",
    "chunk_council_document",
    "chunk_ids_covering",
    "chunk_spans",
    "council_title",
    "french_date",
    "is_council_source_table",
    "source_table_for",
]
