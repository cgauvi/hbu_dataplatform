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

from urban_rag.spectrum import default_ca_bundle

#: Tables whose URL column points at a document worth indexing, keyed by the
#: file slug written by ``neighborhood_features``. The zoning grids are the
#: corpus: one PDF per zone, so a retrieved passage is already scoped to the
#: parcel the map is asking about.
#:
#: Other tables carry links too, but to web pages (``Education__*``,
#: ``VSP_REG_BATIMENT_*``), photos (``Ruelle_verte__*``) or a single shared
#: modality page (``Stationnement__*``, ``VSP_REG_PIIA``). ``VSP_REG_PPCMOI``
#: is the one genuine second corpus - 227 per-resolution PDFs behind
#: ``EN_SAVOIR_PLUS`` - and is left out only because a project-specific
#: resolution answers a different question than a zone's standing rules; add
#: it here when that question is worth indexing.
DOCUMENT_SOURCES: dict[str, str] = {
    "Reglement_urbanisme__VSP_REG_ZONE": "LIEN_GRILLE",
}

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

    @property
    def num_chars(self) -> int:
        return len(self.text)


@dataclass(frozen=True)
class Chunk:
    doc_id: str
    chunk_index: int
    text: str
    num_tokens: int

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


def read_pdf(url: str, content: bytes) -> Document:
    """Extract a PDF's text layer. No OCR: these are born-digital files."""
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    try:
        reader = PdfReader(io.BytesIO(content))
        pages = [page.extract_text() or "" for page in reader.pages]
    except (PdfReadError, ValueError, OSError) as exc:
        raise DocumentError(f"{url}: unreadable PDF ({exc})") from exc

    text = normalize_text("\n\n".join(page for page in pages if page.strip()))
    if not text:
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
    )


def normalize_text(text: str) -> str:
    """Undo the artefacts of PDF extraction while keeping paragraph breaks."""
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\u00ad", "")
    text = _LINE_BREAK_HYPHEN.sub(r"\1\2", text)  # word split across two lines
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
    return [
        Chunk(
            doc_id=document.doc_id,
            chunk_index=index,
            text=piece,
            num_tokens=ruler.count(piece),
        )
        for index, piece in enumerate(pieces)
    ]


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
