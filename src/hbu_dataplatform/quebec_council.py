"""Quebec City's conseils de quartier: where their minutes are, and the trail
from a minute to the decision it reports on.

Quebec City has a *conseil de quartier* per neighbourhood - thirty of them,
three to nine per arrondissement - and each one files the minutes of its
monthly assembly as a PDF. When the city amends a zoning by-law it asks the
council for an opinion and holds the public consultation at the council's
assembly, so the minutes are where an amendment first surfaces in prose: the
zone, the address, what is asked (eight dwellings to ten) and what the
neighbourhood answered.

The city's page for a council's minutes
(``.../conseils_quartier/montcalm/proces-verbaux.aspx``) is a shell. Its list
is an iframe served by a second host::

    https://affichagesite.villequebec.quebec/conseil-quartier/proces-verbaux/12

one ``<h2>`` per year, one ``<a href="/fichiers/<guid>">`` per assembly, the
date in the link's ``title`` ("Procès verbal du 16 juin 2025"). ``12`` is the
council's id on that host, which is published nowhere the page shows; the
registry below was built by probing the ids and reading the one line each
answer prints ("... du Conseil de quartier de Montcalm").

The trail from a minute to a decision is three hops, each on a different host
of the same city:

1. the minute links the consultation's *fiche*
   (``www.ville.quebec.qc.ca/.../activites/fiche.aspx?IdProjet=917``), an HTML
   page carrying the project's description, its timeline and its documents;
2. the fiche links the *sommaire décisionnel* on the city's decision system
   (``gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf``), which bundles the
   fiche de modification, the by-law and the amended grid, and the consultation
   report and presentation (``CPFichierAzure.ashx?Fichier=<guid>.pdf``);
3. the sommaire links the resolutions taken on it (``CA1-2025-0215.pdf``, the
   arrondissement council's extract adopting the by-law; ``AM1-2025-0146``,
   the notice of motion).

`classify_link` is the whole policy of what the trail follows: those hosts
and paths, and nothing else - a minute also links YouTube, the snow-removal
page and the borough's newsletter.

Deliberately free of Dagster imports, like the other publisher clients. The
PDFs go through `rag.documents.PdfFetcher`, whose cache is keyed by URL and
shared across scrape dates, because a filed minute never changes; the fiche
pages are fetched every time, because a fiche gains its report and its
adoption date after the assembly.
"""

from __future__ import annotations

import html as html_module
import io
import re
import time
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hbu_dataplatform.rag.documents import DocumentError, PdfFetcher
from hbu_dataplatform.spectrum import USER_AGENT, default_ca_bundle

#: The iframe the city's minutes page embeds, per council id.
DEFAULT_LISTING_URL_TEMPLATE = (
    "https://affichagesite.villequebec.quebec/conseil-quartier/proces-verbaux/{council_id}"
)

#: Where a PDF the listing links to is served from - the hrefs are relative.
LISTING_BASE_URL = "https://affichagesite.villequebec.quebec/"


class CouncilError(RuntimeError):
    """A council's listing, or a page on the trail, could not be read."""


@dataclass(frozen=True)
class NeighborhoodCouncil:
    """One conseil de quartier: its id on the listing host and its borough."""

    council_id: int
    name: str
    neighborhood: str

    @property
    def listing_url(self) -> str:
        return DEFAULT_LISTING_URL_TEMPLATE.format(council_id=self.council_id)


#: Every council the listing host answers for, keyed by its id there, with the
#: arrondissement key (`partitions.known_neighborhoods`) it sits in. Ids 16 and
#: 32+ answer an empty page.
#:
#: La Cité-Limoilou's nine are the ones checked against the city's quartier
#: map and run; the other twenty-one are placed from the same map and have not
#: been fetched yet. A council in the wrong borough would fetch cleanly and
#: file its minutes under a partition they do not belong to, so a new borough
#: is worth a look at its first run's `councils` metadata.
NEIGHBORHOOD_COUNCILS: dict[int, NeighborhoodCouncil] = {
    council.council_id: council
    for council in (
        # -- CIL, La Cité-Limoilou -------------------------------------------
        NeighborhoodCouncil(9, "Lairet", "CIL"),
        NeighborhoodCouncil(11, "Maizerets", "CIL"),
        NeighborhoodCouncil(12, "Montcalm", "CIL"),
        NeighborhoodCouncil(17, "Saint-Jean-Baptiste", "CIL"),
        NeighborhoodCouncil(18, "Saint-Roch", "CIL"),
        NeighborhoodCouncil(19, "Saint-Sacrement", "CIL"),
        NeighborhoodCouncil(20, "Saint-Sauveur", "CIL"),
        NeighborhoodCouncil(25, "Vieux-Limoilou", "CIL"),
        NeighborhoodCouncil(
            21, "Vieux-Québec–Cap-Blanc–Colline parlementaire", "CIL"
        ),
        # -- SSC, Sainte-Foy–Sillery–Cap-Rouge --------------------------------
        NeighborhoodCouncil(2, "Cap-Rouge", "SSC"),
        NeighborhoodCouncil(4, "Cité-Universitaire", "SSC"),
        NeighborhoodCouncil(15, "Plateau", "SSC"),
        NeighborhoodCouncil(26, "Pointe-de-Sainte-Foy", "SSC"),
        NeighborhoodCouncil(27, "Saint-Louis", "SSC"),
        NeighborhoodCouncil(28, "Sillery", "SSC"),
        # -- RIV, Les Rivières -------------------------------------------------
        NeighborhoodCouncil(7, "Duberger–Les Saules", "RIV"),
        NeighborhoodCouncil(13, "Neufchâtel-Est–Lebourgneuf", "RIV"),
        NeighborhoodCouncil(24, "Vanier", "RIV"),
        # -- CHA, Charlesbourg -------------------------------------------------
        NeighborhoodCouncil(6, "Jésuites", "CHA"),
        NeighborhoodCouncil(14, "Notre-Dame-des-Laurentides", "CHA"),
        NeighborhoodCouncil(30, "Orsainville", "CHA"),
        # -- BEA, Beauport -----------------------------------------------------
        NeighborhoodCouncil(3, "Chutes-Montmorency", "BEA"),
        NeighborhoodCouncil(29, "Vieux-Bourg", "BEA"),
        NeighborhoodCouncil(31, "Sainte-Thérèse-de-Lisieux", "BEA"),
        # -- HSC, La Haute-Saint-Charles ---------------------------------------
        NeighborhoodCouncil(1, "Aéroport", "HSC"),
        NeighborhoodCouncil(5, "Châtels", "HSC"),
        NeighborhoodCouncil(8, "Lac-Saint-Charles", "HSC"),
        NeighborhoodCouncil(10, "Loretteville", "HSC"),
        NeighborhoodCouncil(22, "Saint-Émile", "HSC"),
        NeighborhoodCouncil(23, "Val-Bélair", "HSC"),
    )
}


def councils_for(neighborhood: str) -> tuple[NeighborhoodCouncil, ...]:
    """The councils of one arrondissement, in id order. Empty for a Montreal
    or Saguenay key: neither city has this institution."""
    return tuple(
        council
        for council in sorted(NEIGHBORHOOD_COUNCILS.values(), key=lambda c: c.council_id)
        if council.neighborhood == neighborhood
    )


# ---------------------------------------------------------------------------
# the listing
# ---------------------------------------------------------------------------

FRENCH_MONTHS = {
    "janvier": 1,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
}

_FRENCH_DATE = re.compile(
    r"(\d{1,2})(?:er)?\s+([a-zéû]+)\s+(\d{4})", re.IGNORECASE
)


def parse_french_date(text: str) -> date | None:
    """``16 juin 2025`` (also ``1er juin 2025``, ``lundi 7 juillet 2025``) as
    a date, or None when ``text`` states none."""
    match = _FRENCH_DATE.search(_ascii_fold(text))
    if not match:
        return None
    day, month_name, year = match.groups()
    month = FRENCH_MONTHS.get(month_name.lower())
    if month is None:
        return None
    try:
        return date(int(year), month, int(day))
    except ValueError:
        return None


def _ascii_fold(text: str) -> str:
    return (
        text.replace("é", "e")
        .replace("É", "E")
        .replace("û", "u")
        .replace("è", "e")
        .replace("ê", "e")
    )


@dataclass(frozen=True)
class MinutesLink:
    """One assembly's minutes, as the listing presents it."""

    url: str
    title: str
    year: int | None
    meeting_date: date | None
    size_label: str | None


class _ListingParser(HTMLParser):
    """Walk the listing: ``<h2>`` sets the year, ``<a href="/fichiers/...">``
    is a minute, its ``<span class="note">`` the size."""

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[MinutesLink] = []
        self._year: int | None = None
        self._in_h2 = False
        self._current: dict | None = None
        self._in_note = False

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "h2":
            self._in_h2 = True
        elif tag == "a" and (attributes.get("href") or "").startswith("/fichiers/"):
            self._current = {
                "url": urljoin(self.base_url, attributes["href"]),
                "title": html_module.unescape(attributes.get("title") or ""),
                "text": "",
                "note": "",
            }
        elif tag == "span" and self._current is not None and "note" in (
            attributes.get("class") or ""
        ):
            self._in_note = True

    def handle_endtag(self, tag):
        if tag == "h2":
            self._in_h2 = False
        elif tag == "span" and self._in_note:
            self._in_note = False
        elif tag == "a" and self._current is not None:
            current = self._current
            self._current = None
            title = current["title"] or current["text"].strip()
            self.links.append(
                MinutesLink(
                    url=current["url"],
                    title=title,
                    year=self._year,
                    meeting_date=parse_french_date(title),
                    size_label=current["note"].strip().strip("()").strip() or None,
                )
            )

    def handle_data(self, data):
        if self._in_h2:
            digits = re.search(r"\d{4}", data)
            if digits:
                self._year = int(digits.group())
        elif self._current is not None:
            if self._in_note:
                self._current["note"] += data
            else:
                self._current["text"] += data


def parse_minutes_listing(page: str, *, base_url: str = LISTING_BASE_URL) -> list[MinutesLink]:
    """Every minute the listing links, in page order (newest first)."""
    parser = _ListingParser(base_url)
    parser.feed(page)
    return parser.links


# ---------------------------------------------------------------------------
# the trail
# ---------------------------------------------------------------------------

#: What a link on the trail is, by where it points. The values are the `kind`
#: column of `council_minutes_documents`.
KIND_FICHE = "fiche"
KIND_GPD = "gpd"
KIND_CONSULTATION_FILE = "consultation_file"
KIND_COUNCIL_FILE = "council_file"

_FICHE = re.compile(r"/participation-citoyenne/activites/fiche\.aspx\?IdProjet=\d+", re.I)
_GPD = re.compile(r"^/gpdblob/([A-Za-z0-9\-]+)\.pdf$", re.I)
_CONSULTATION_FILE = re.compile(r"CPFichierAzure\.ashx\?Fichier=([0-9a-f\-]+)\.pdf(?:&|$)", re.I)
_COUNCIL_FILE = re.compile(r"^/fichiers/([0-9a-f\-]+)$", re.I)


def classify_link(url: str) -> str | None:
    """Which kind of trail document ``url`` is, or None when the trail does
    not follow it. The whole policy of what gets fetched."""
    parts = urlparse(url)
    host = parts.netloc.lower()
    if host == "www.ville.quebec.qc.ca":
        if _FICHE.search(parts.path + "?" + parts.query):
            return KIND_FICHE
        if _CONSULTATION_FILE.search(parts.path + "?" + parts.query):
            return KIND_CONSULTATION_FILE
        return None
    if host == "gpddocs.ville.quebec.qc.ca" and _GPD.match(parts.path):
        return KIND_GPD
    if host == "affichagesite.villequebec.quebec" and _COUNCIL_FILE.match(parts.path):
        return KIND_COUNCIL_FILE
    return None


def canonical_url(url: str) -> str:
    """One spelling per document, so the same fiche reached with and without
    ``www`` or a trailing anchor is one row."""
    parts = urlparse(url.strip())
    host = parts.netloc.lower()
    if host == "ville.quebec.qc.ca":
        host = "www.ville.quebec.qc.ca"
    path = parts.path
    query = parts.query
    kind = classify_link(f"{parts.scheme or 'https'}://{host}{path}?{query}")
    if kind == KIND_FICHE:
        match = re.search(r"IdProjet=(\d+)", query, re.I)
        query = f"IdProjet={match.group(1)}" if match else query
    elif kind == KIND_CONSULTATION_FILE:
        match = re.search(r"Fichier=([^&]+)", query, re.I)
        query = f"Fichier={match.group(1)}" if match else query
    elif kind in (KIND_GPD, KIND_COUNCIL_FILE):
        # A minute links the sommaire with Google's `_gl` campaign parameter
        # appended, and the fiche links it bare: one document, one row.
        query = ""
    return f"https://{host}{path}" + (f"?{query}" if query else "")


def document_number_of(url: str) -> str | None:
    """The decision system's own number for a GPD document - ``GT2025-233``,
    ``CA1-2025-0215`` - read off the file name; None for any other kind."""
    match = _GPD.match(urlparse(url).path)
    return match.group(1) if match else None


#: A URL written out in a PDF's text rather than linked - the consultation
#: report prints the fiche's address that way, without a scheme.
_TEXT_URL = re.compile(
    r"(?:https?://)?(?:www\.)?(?:ville\.quebec\.qc\.ca|gpddocs\.ville\.quebec\.qc\.ca|"
    r"affichagesite\.villequebec\.quebec)/[^\s<>\"')]+"
)


def urls_in_text(text: str) -> list[str]:
    """City URLs spelled out in running text, with a scheme put back on."""
    found: dict[str, None] = {}
    for match in _TEXT_URL.finditer(text):
        url = match.group().rstrip(".,;:")
        if not url.startswith("http"):
            url = "https://" + url
        if url.startswith("https://ville.quebec.qc.ca"):
            url = url.replace("https://ville.", "https://www.ville.", 1)
        found.setdefault(url, None)
    return list(found)


def pdf_links(content: bytes) -> list[str]:
    """The URI link annotations of a PDF, in page order, deduplicated.

    Where a minute's outgoing links live: the PDF is exported from a word
    processor and its hyperlinks survive as ``/Link`` annotations carrying a
    ``/URI`` action, while the visible text says only "voir la fiche".
    """
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    found: dict[str, None] = {}
    try:
        reader = PdfReader(io.BytesIO(content))
        for page in reader.pages:
            for annotation in page.get("/Annots") or []:
                try:
                    annotation = annotation.get_object()
                    action = annotation.get("/A")
                    uri = action.get("/URI") if action else None
                except Exception:  # noqa: BLE001 - a malformed annotation is skipped
                    continue
                if uri:
                    found.setdefault(str(uri).strip(), None)
    except (PdfReadError, ValueError, OSError):
        return []
    return list(found)


# ---------------------------------------------------------------------------
# the fiche page
# ---------------------------------------------------------------------------


class _FicheParser(HTMLParser):
    """The fiche's visible text and its hrefs, from ``<h1>`` to the footer."""

    _SKIP = {"script", "style", "noscript", "head"}
    _BREAKS = {"p", "div", "li", "h1", "h2", "h3", "h4", "br", "tr", "ul"}

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.hrefs: list[str] = []
        self._pieces: list[str] = []
        self._skip_depth = 0
        self._started = False
        self._in_footer = False

    def handle_starttag(self, tag, attrs):
        if tag in self._SKIP:
            self._skip_depth += 1
            return
        attributes = dict(attrs)
        if tag == "h1":
            self._started = True
        if tag == "footer" or (attributes.get("id") or "").lower() == "footer":
            self._in_footer = True
        if tag == "a" and attributes.get("href"):
            href = attributes["href"].strip()
            if href.startswith("www."):
                # A fiche writes one of its links scheme-less; joined as a
                # relative path it would point under the fiche's own folder.
                href = "https://" + href
            self.hrefs.append(urljoin(self.base_url, href))
        if tag in self._BREAKS:
            self._pieces.append("\n")

    def handle_endtag(self, tag):
        if tag in self._SKIP and self._skip_depth:
            self._skip_depth -= 1
        elif tag in self._BREAKS:
            self._pieces.append("\n")

    def handle_data(self, data):
        if self._skip_depth or not self._started or self._in_footer:
            return
        self._pieces.append(data)

    @property
    def text(self) -> str:
        text = html_module.unescape("".join(self._pieces))
        text = re.sub(r"[ \t ]+", " ", text)
        lines = [line.strip() for line in text.split("\n")]
        return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


@dataclass(frozen=True)
class FichePage:
    url: str
    title: str | None
    text: str
    links: tuple[str, ...]


def parse_fiche(page: str, url: str) -> FichePage:
    """A consultation fiche as text plus the links it carries.

    The title is the ``<h1>`` after "Fiche"; the text keeps the project's
    description, its documents' captions and the *Démarche de participation
    publique* timeline, which is where the adoption date is stated before any
    resolution is filed.
    """
    parser = _FicheParser(url)
    parser.feed(page)
    text = parser.text
    title = None
    match = re.search(r"^Fiche\s*\n\s*(.+)$", text, re.M)
    if match:
        title = match.group(1).strip()
    return FichePage(url=url, title=title, text=text, links=tuple(dict.fromkeys(parser.hrefs)))


# ---------------------------------------------------------------------------
# fetching
# ---------------------------------------------------------------------------


class CouncilFetcher:
    """Reads the listing and the fiche pages live, and the PDFs through the
    shared cache.

    ``session`` is injectable for the tests; the default is paced and
    patient the way every publisher client here is, and verifies TLS against
    the same bundle `PdfFetcher` uses.
    """

    def __init__(
        self,
        pdf_fetcher: PdfFetcher,
        *,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
        listing_url_template: str = DEFAULT_LISTING_URL_TEMPLATE,
    ) -> None:
        self.pdf_fetcher = pdf_fetcher
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self.listing_url_template = listing_url_template
        self._session = session or self._build_session(max_retries, ca_bundle)

    @staticmethod
    def _build_session(max_retries: int, ca_bundle: str | None) -> requests.Session:
        session = requests.Session()
        session.headers["User-Agent"] = USER_AGENT
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

    def listing_url(self, council: NeighborhoodCouncil) -> str:
        return self.listing_url_template.format(council_id=council.council_id)

    def fetch_html(self, url: str) -> str:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise CouncilError(f"{url}: {exc}") from exc
        content_type = response.headers.get("Content-Type", "")
        if "html" not in content_type and response.content[:5] == b"%PDF-":
            raise CouncilError(f"{url}: expected a page, got a PDF")
        response.encoding = response.encoding or "utf-8"
        return response.text

    def list_minutes(self, council: NeighborhoodCouncil) -> list[MinutesLink]:
        url = self.listing_url(council)
        page = self.fetch_html(url)
        links = parse_minutes_listing(page, base_url=LISTING_BASE_URL)
        if not links:
            raise CouncilError(
                f"{url}: the listing names no minutes; the host may have moved "
                f"council {council.council_id} ({council.name})"
            )
        return links

    def fetch_pdf(self, url: str) -> tuple[bytes, bool]:
        """``(pdf_bytes, came_from_cache)``, raising `DocumentError` like
        the fetcher it wraps."""
        return self.pdf_fetcher.fetch(url)

    def fetch_fiche(self, url: str) -> FichePage:
        return parse_fiche(self.fetch_html(url), url)


__all__ = [
    "CouncilError",
    "CouncilFetcher",
    "DocumentError",
    "FichePage",
    "KIND_CONSULTATION_FILE",
    "KIND_COUNCIL_FILE",
    "KIND_FICHE",
    "KIND_GPD",
    "MinutesLink",
    "NEIGHBORHOOD_COUNCILS",
    "NeighborhoodCouncil",
    "canonical_url",
    "classify_link",
    "councils_for",
    "document_number_of",
    "parse_fiche",
    "parse_french_date",
    "parse_minutes_listing",
    "pdf_links",
    "urls_in_text",
]
