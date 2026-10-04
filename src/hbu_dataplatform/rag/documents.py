"""Fetch the PDFs a scraped table links to, and cut them into chunks.

The regulation tables carry no prose of their own: ``VSP_REG_ZONE`` holds one
``LIEN_GRILLE`` per zone, an ``http://www1.ville.montreal.qc.ca/.../zone/
C01-001.pdf`` link to that zone's *grille des usages et des normes* - the PDF
stating its authorised usages, heights, densities, implantation and margins.
The RAG corpus is therefore built from those linked grids, not from the
parquet cells.

Kept free of Dagster and of the embedding stack: chunking takes a
:class:`TokenRuler`, so tests exercise it with a trivial counter instead of
loading a 2 GB model.
"""

from __future__ import annotations

import bisect
import hashlib
import io
import re
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hbu_dataplatform.core.http import default_ca_bundle

#: Tables whose URL column points at a document worth indexing, keyed by the
#: file slug written by ``neighborhood_features``. The zoning grids are the
#: standing rules: one PDF per zone, so a retrieved passage is already scoped
#: to the parcel the map is asking about. ``VSP_REG_PPCMOI`` is the other half
#: of the answer - a *projet particulier* is a resolution that overrides the
#: grid for one site, so a question about what may be built there is wrong
#: without it.
#:
#: Other tables carry links too, but to web pages (``Education__*``,
#: ``VSP_REG_BATIMENT_*``), photos (``Ruelle_verte__*``) or a single shared
#: modality page (``Stationnement__*``, ``VSP_REG_PIIA``) - none of which is a
#: document about one place.
#:
#: All three cities are here on the same terms, and none of the other two
#: publishes the link as an attribute. Saguenay's polygons carry no link, so
#: `assets._saguenay_features` resolves one grid id per zone and writes the
#: URL under Montreal's column name. Quebec City's carry none either, but its
#: sheets are served from a handler keyed on the zone code the polygons do
#: carry, so `assets._quebec_features` formats the URL rather than resolving
#: it. Either way the column exists by the time this runs, which is what lets
#: one corpus pipeline serve three cities.
DOCUMENT_SOURCES: dict[str, str] = {
    "Reglement_urbanisme__VSP_REG_ZONE": "LIEN_GRILLE",
    "Zonage__ZONAGE_SAGUENAY": "LIEN_GRILLE",
    "Zonage__ZONAGE_EN_VIGUEUR": "LIEN_GRILLE",
    # 227 per-resolution PDFs behind 229 polygons. Its parquet carries `ID`,
    # which is already in `assets._ID_COLUMNS`, so the resolutions are filed
    # against the same feature ids `rag.features` holds and the spatial
    # searches reach them without further work.
    "Reglement_urbanisme__VSP_REG_PPCMOI": "EN_SAVOIR_PLUS",
}

#: Every scraped table that *is* a zoning layer - the ones `lot_zone_pieces`
#: cuts lots against and `lot_zoning_envelopes` joins grid columns to.
#:
#: This USED to be `tuple(DOCUMENT_SOURCES)`, and stopped being so the moment
#: PPCMOI joined the corpus. A projet particulier is a document about a site;
#: it is not a zone. Cutting lots against it would invent a zone piece per
#: resolution, and since the whole HBU chain is keyed on (lot_uid, feature_id)
#: that is not a cosmetic error - it is a second, bogus development site for
#: every lot a PPCMOI touches.
#:
#: So the two lists are now genuinely different, which is what the names
#: always said they were for: a table is a zone source if lots are cut against
#: it, and a document source if it links prose worth retrieving.
ZONING_SOURCES: tuple[str, ...] = (
    "Reglement_urbanisme__VSP_REG_ZONE",
    "Zonage__ZONAGE_SAGUENAY",
    "Zonage__ZONAGE_EN_VIGUEUR",
)

#: Below this, a PDF with NO TEXT is a publisher's placeholder rather than a
#: scan. Quebec City serves an unknown zone code as a valid, empty about 850-byte
#: PDF; a real grid sheet is 110-125 kB. Only ever applied together with "has
#: no text layer" - on its own it would reject valid small documents, and a
#: two-line council minute is a perfectly good 622-byte PDF.
MIN_PDF_BYTES = 2048

DEFAULT_MAX_TOKENS = 512
DEFAULT_OVERLAP_TOKENS = 64

_PARAGRAPH_BREAK = re.compile(r"\n\s*\n+")
_LINE_BREAK_HYPHEN = re.compile(r"(\w)-\n(\w)")
_HORIZONTAL_SPACE = re.compile("[ \t\u00a0]+")


class DocumentError(RuntimeError):
    """A linked document could not be fetched or read."""


class TokenRuler(Protocol):
    """Whatever measures a string the way the embedding model will."""

    def count(self, text: str) -> int:
        """Tokens ``text`` costs, special tokens excluded."""

    def split(self, text: str, max_tokens: int) -> list[str]:
        """Cut ``text`` into pieces of at most ``max_tokens`` tokens."""


@dataclass(frozen=True)
class Document:
    """One fetched PDF, flattened to text."""

    doc_id: str
    url: str
    text: str
    num_pages: int
    content_sha256: str
    num_bytes: int
    #: Character offset into ``text`` where each kept page starts, in order.
    #: Empty for a document read before this existed, which is why every
    #: reader of it treats "no offsets" as "page unknown" rather than page 1.
    page_offsets: tuple[int, ...] = ()

    @property
    def num_chars(self) -> int:
        return len(self.text)

    def page_of(self, offset: int) -> int | None:
        """The 1-based page a character offset falls on, if it is known.

        Pages with no text are dropped before the offsets are built, so this
        numbers the pages that *have* text. For a grid sheet - one page, or two
        - that is the same thing; for a long minute it is not, and the number
        is the nth page with text rather than the nth sheet of paper.
        """
        if not self.page_offsets:
            return None
        return bisect.bisect_right(self.page_offsets, offset)


@dataclass(frozen=True)
class Chunk:
    doc_id: str
    chunk_index: int
    text: str
    num_tokens: int
    #: The pages this chunk's text came from, 1-based and inclusive. None when
    #: the document carried no page map, or when the chunk could not be placed
    #: in it - a citation then names the document and says nothing about where,
    #: which is the honest answer rather than a guess at page 1.
    page_from: int | None = None
    page_to: int | None = None

    @property
    def chunk_id(self) -> str:
        return f"{self.doc_id}:{self.chunk_index:04d}"


def document_id(url: str) -> str:
    """Stable id for a link, so chunk ids survive a re-scrape unchanged."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def document_urls(frame, url_column: str) -> list[str]:
    """Distinct, non-empty links in ``url_column``, in first-seen order."""
    if url_column not in frame.columns:
        raise DocumentError(f"Column {url_column!r} is not in the table")
    seen: dict[str, None] = {}
    for value in frame[url_column].dropna():
        url = str(value).strip()
        if url.startswith(("http://", "https://")):
            seen.setdefault(url, None)
    return list(seen)


class PdfFetcher:
    """Downloads linked PDFs, with an on-disk cache keyed by the URL.

    A published zoning grid is reissued under a new file when the zone is
    amended rather than edited in place, so a cached copy is reused across
    scrape dates rather than pulled from the city's web server again on every
    run.
    """

    def __init__(
        self,
        *,
        cache_dir: Path | str,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self._session = session or self._build_session(max_retries, ca_bundle)

    @staticmethod
    def _build_session(max_retries: int, ca_bundle: str | None) -> requests.Session:
        session = requests.Session()
        bundle = ca_bundle or default_ca_bundle()
        if bundle:
            session.verify = bundle
        retry = Retry(
            total=max_retries,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def cache_path(self, url: str) -> Path:
        return self.cache_dir / f"{document_id(url)}.pdf"

    def fetch(self, url: str) -> tuple[bytes, bool]:
        """Return ``(pdf_bytes, came_from_cache)``."""
        cached = self.cache_path(url)
        if cached.exists() and cached.stat().st_size:
            return cached.read_bytes(), True

        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise DocumentError(f"{url}: {exc}") from exc

        content = response.content
        content_type = response.headers.get("Content-Type", "")
        if not content.startswith(b"%PDF") and "pdf" not in content_type:
            # Dead links answer 200 with an HTML "page not found" body.
            raise DocumentError(
                f"{url}: not a PDF (Content-Type {content_type!r}, "
                f"{len(content)} bytes)"
            )


        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(content)
        return content, False


def read_pdf(url: str, content: bytes, *, keep_hyphens: bool = False) -> Document:
    """Extract a PDF's text layer. No OCR: these are born-digital files.

    ``keep_hyphens`` is for prose rather than grids: a zoning grid's
    line-end hyphen is a syllable break to undo, a minute's is as likely the
    hyphen of *Cité-Limoilou* or *GT2025-233* to keep. See `normalize_text`.
    """
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    try:
        reader = PdfReader(io.BytesIO(content))
        pages = [page.extract_text() or "" for page in reader.pages]
    except (PdfReadError, ValueError, OSError) as exc:
        raise DocumentError(f"{url}: unreadable PDF ({exc})") from exc

    # Normalised per page and then joined, rather than joined and then
    # normalised, so that each page's start offset is known in the final
    # string. The two are byte-identical - verified over 120 of the cached
    # sheets - because no rule in `normalize_text` reaches across the "\n\n"
    # the pages are joined with: the hyphen rule needs a single newline, and
    # the whitespace rule does not match newlines at all. Were that to change,
    # every chunk boundary would move and the whole corpus would need
    # rebuilding rather than upserting, so it is worth the test that pins it.
    kept = [
        normalize_text(page, keep_hyphens=keep_hyphens)
        for page in pages
        if page.strip()
    ]
    offsets: list[int] = []
    cursor = 0
    for page in kept:
        offsets.append(cursor)
        cursor += len(page) + 2  # the "\n\n" each join adds
    text = "\n\n".join(kept)
    if not text:
        # Two different failures wearing the same error, and the OCR backlog
        # is the thing that cares. A scan is a big file with no text layer; a
        # publisher's "no such zone" is a tiny file with no text layer -
        # Quebec City answers an unknown code with a valid, empty, about 850-byte
        # PDF where a real sheet is 110-125 kB.
        #
        # The size test lives here rather than in `fetch` because size ALONE
        # says nothing: a two-line council minute is a valid 622-byte PDF.
        # Textless *and* tiny is the pair that means "placeholder".
        if len(content) < MIN_PDF_BYTES:
            raise DocumentError(
                f"{url}: {len(content)}-byte PDF with no text - the publisher "
                f"served an empty sheet, which usually means it does not know "
                f"this code. Not a scan, and not one for OCR."
            )
        raise DocumentError(
            f"{url}: no text layer over {len(pages)} page(s); "
            "a scanned document would need OCR"
        )
    return Document(
        doc_id=document_id(url),
        url=url,
        text=text,
        num_pages=len(pages),
        content_sha256=hashlib.sha256(content).hexdigest(),
        num_bytes=len(content),
        page_offsets=tuple(offsets),
    )


def normalize_text(text: str, *, keep_hyphens: bool = False) -> str:
    """Undo the artefacts of PDF extraction while keeping paragraph breaks.

    A word split across two lines is joined. By default the hyphen goes with
    the break, which is right for a grid's running text; ``keep_hyphens``
    joins the two halves *with* it, for prose where the hyphen is the word's
    own - proper nouns and file numbers - and a syllable break is the rarer
    case.
    """
    # The soft hyphen and the NUL are both extraction artefacts, and a NUL
    # is one Postgres refuses in a text column.
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\u00ad", "").replace("\x00", "")
    joined = r"\1-\2" if keep_hyphens else r"\1\2"
    text = _LINE_BREAK_HYPHEN.sub(joined, text)  # word split across two lines
    text = _HORIZONTAL_SPACE.sub(" ", text)
    lines = [line.strip() for line in text.split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def chunk_text(
    text: str,
    ruler: TokenRuler,
    *,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    overlap_tokens: int = DEFAULT_OVERLAP_TOKENS,
) -> list[str]:
    """Pack paragraphs into chunks of at most ``max_tokens`` tokens.

    Paragraph boundaries are respected wherever they fit, so a chunk rarely
    starts mid-sentence; only a paragraph that overruns the budget on its own
    is cut by the tokenizer. Consecutive chunks overlap by at most
    ``overlap_tokens``, repeating whole trailing paragraphs.
    """
    if max_tokens <= 0:
        raise ValueError("max_tokens must be positive")
    if not 0 <= overlap_tokens < max_tokens:
        raise ValueError("overlap_tokens must be in [0, max_tokens)")

    units = [
        (piece, ruler.count(piece))
        for piece in _paragraph_units(text, ruler, max_tokens)
    ]
    if not units:
        return []

    chunks: list[str] = []
    current: list[tuple[str, int]] = []
    size = 0
    for unit, cost in units:
        if current and size + cost > max_tokens:
            chunks.append(_join(current))
            current = _overlap_tail(current, overlap_tokens)
            # The overlap is carried *inside* the budget, not on top of it: drop
            # its oldest paragraphs until the incoming one still fits.
            while current and sum(c for _, c in current) + cost > max_tokens:
                current.pop(0)
            size = sum(c for _, c in current)
        current.append((unit, cost))
        size += cost
    if current:
        chunks.append(_join(current))
    return chunks


def chunk_document(
    document: Document,
    ruler: TokenRuler,
    *,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    overlap_tokens: int = DEFAULT_OVERLAP_TOKENS,
) -> list[Chunk]:
    pieces = chunk_text(
        document.text, ruler, max_tokens=max_tokens, overlap_tokens=overlap_tokens
    )
    chunks: list[Chunk] = []
    cursor = 0
    for index, piece in enumerate(pieces):
        start, end = _locate(document.text, piece, cursor)
        if start is not None:
            # Chunks overlap, so the next one may begin before this one ended.
            # Advancing to this chunk's start - not its end - keeps the search
            # moving forward without stepping over the overlap.
            cursor = start
        chunks.append(
            Chunk(
                doc_id=document.doc_id,
                chunk_index=index,
                text=piece,
                num_tokens=ruler.count(piece),
                page_from=None if start is None else document.page_of(start),
                page_to=None if end is None else document.page_of(end - 1),
            )
        )
    return chunks


def _locate(text: str, piece: str, cursor: int) -> tuple[int | None, int | None]:
    """Where a chunk sits in the document it came from.

    Found by its first and last paragraph rather than by the chunk string: the
    chunker rejoins paragraphs with a single blank line, so a chunk whose
    source had three newlines between two paragraphs is not a literal
    substring of the document. The paragraphs themselves are.

    Returns ``(None, None)`` when the text cannot be placed, which a caller
    reads as "page unknown". Better a citation with no page than a wrong one.
    """
    paragraphs = [p for p in _PARAGRAPH_BREAK.split(piece) if p.strip()]
    if not paragraphs:
        return None, None
    start = text.find(paragraphs[0].strip(), cursor)
    if start < 0:
        return None, None
    last = paragraphs[-1].strip()
    end = text.find(last, start)
    return (start, end + len(last)) if end >= 0 else (start, start + len(piece))


def _paragraph_units(text: str, ruler: TokenRuler, max_tokens: int) -> Iterator[str]:
    for paragraph in _PARAGRAPH_BREAK.split(text):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        if ruler.count(paragraph) <= max_tokens:
            yield paragraph
        else:
            yield from ruler.split(paragraph, max_tokens)


def _overlap_tail(
    units: list[tuple[str, int]], overlap_tokens: int
) -> list[tuple[str, int]]:
    """Trailing whole paragraphs worth at most ``overlap_tokens``.

    The last unit is never carried alone as the entire tail of a chunk that
    holds only it, which would loop forever on an oversized paragraph.
    """
    if overlap_tokens <= 0 or len(units) < 2:
        return []
    tail: list[tuple[str, int]] = []
    budget = overlap_tokens
    for unit, cost in reversed(units[1:]):
        if cost > budget:
            break
        tail.insert(0, (unit, cost))
        budget -= cost
    return tail


def _join(units: Iterable[tuple[str, int]]) -> str:
    return "\n\n".join(piece for piece, _ in units)
