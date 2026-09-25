"""Saguenay's zoning: the polygons on Données Québec and the grid behind the counter.

A third city, and a third arrangement of the same two things. Montreal links a
*grille des usages et des normes* PDF from each row of its zone table and
`hbu_dataplatform.zoning.zoning_grid` reads it. Quebec City publishes its zones as an ArcGIS
layer and its norms as one workbook for the city, which is what
`hbu_dataplatform.cities.quebec_city.zoning` exists for. Saguenay splits the difference:

* **The zones**, as ``sag_zonage`` on Données Québec
  (https://www.donneesquebec.ca/recherche/dataset/sag_zonage): 2,839 polygons
  in EPSG:4326 carrying three fields and no norms - ``id``, ``municipalite``
  (94068 on every row) and ``no_zone``, the zone number the by-law and the
  grid are both keyed on. Fetched through the same Données Québec resource
  Quebec City's outlines come from, because it is the same portal.
* **The grid**, as one PDF per zone, generated on demand by the city's
  reporting service at `GRID_PDF_URL`. It is a real *grille des usages et des
  normes* under by-law VS-R-2012-3, laid out as a table of columns exactly the
  way Montreal's is, so it is read the same way - see `parse_grid_pdf`.

**The PDF is keyed on an id the polygons do not carry**, which is the one
awkward step. `GRID_PDF_URL` takes the zoning service's own primary key, not
``no_zone``: zone 70520 is id 2700. The city's planning page resolves one from
the other by POSTing ``no_zone`` to `ZONE_LOOKUP_URL`, a thin proxy in front of
``api.saguenay.ca`` (which answers 403 to anything else), and that is what
`SaguenayZoningClient.zone_ids` does - once per zone, because the proxy
forwards *only* that one parameter. Page size and page number are ignored, so
the list cannot be pulled down in bulk and 2,837 lookups is the honest cost of
the index. `zone_ids` is therefore written to bronze beside the polygons and
read back rather than rebuilt per asset.

What that buys is worth the walk: because every zone has its own document with
its own URL, Saguenay goes through `hbu_dataplatform.rag.assets.linked_documents` and
the embedding corpus unchanged - the path Montreal takes and the one Quebec
City cannot, having no per-zone document to index at all.

Deliberately free of Dagster imports, like the other publisher clients.
"""

from __future__ import annotations

import math
import re
import time
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hbu_dataplatform.hbu.program import BuildingLevel
from hbu_dataplatform.core.http import USER_AGENT, default_ca_bundle
from hbu_dataplatform.zoning.zoning_grid import (
    MIN_COLUMN_SUPPORT,
    GridColumn,
    GridParseError,
    _Cell,
    _page_rows,
)

#: The zoning polygons, as a Données Québec resource id. Resolved through the
#: portal's API the way every other dataset here is, rather than hard-coding
#: the download URL, so a re-publication under the same dataset is followed.
ZONING_DATASET = "sag_zonage"
ZONING_GEOJSON = "sag_zonage.geojson"

#: The administrative limit - the city, its three arrondissements and the
#: former municipalities amalgamated into it - read for Saguenay the way
#: `vque_2` is read for Quebec City.
#:
#: The road network that used to sit beside it here (`sag-reseau-routier`) is
#: gone: every city's streets now come from the province-wide RQTT, so there is
#: no Saguenay-specific street feed to name. See `hbu_dataplatform.sources.rqtt.client`.
LIMITS_DATASET = "sag_limite_administrative"
LIMITS_GEOJSON = "sag_limiteadministrative.geojson"

#: The zone number, on the zoning layer and in the by-law. A string in the
#: GeoJSON ("1000", "70520"), and the join key between the polygon, the grid
#: and `rag.features.feature_id`.
ZONE_CODE_FIELD = "no_zone"

#: The layer's own row id, and the *code géographique* it stamps every feature
#: with. The latter is checked rather than assumed: a Données Québec dataset
#: re-published with a neighbour's rows in it would otherwise pass silently.
OBJECT_ID_FIELD = "id"
MUNICIPALITY_FIELD = "municipalite"

#: Columns the limit layer names its polygons by - see
#: `partitions.SAGUENAY_OUTLINE_TYPE` for why both are needed.
LIMIT_NAME_FIELD = "nom"
LIMIT_TYPE_FIELD = "type"

#: Where the grid index and the zoning snapshot are written, in the shape
#: `frames.table_slug` gives a Spectrum table - `<folder>__<TABLE>` - so they
#: sit beside the other cities' layers as peers rather than as a special case.
ZONING_SLUG = "Zonage__ZONAGE_SAGUENAY"

#: The column the per-zone grid URL is written to. Named as Montreal's zone
#: table names its own link column, because that is what
#: `rag.documents.DOCUMENT_SOURCES` reads and there is no reason for the two
#: to differ.
GRID_URL_COLUMN = "LIEN_GRILLE"

#: The city's planning page resolves a zone number to the reporting service's
#: id by POSTing ``no_zone`` here. A GET, or any other parameter, returns the
#: unfiltered first page of 500 - which is why this is a POST with exactly one
#: field and why the index is built one zone at a time.
ZONE_LOOKUP_URL = (
    "https://ville.saguenay.ca/ajax/urbanisme/grilleusagesnormes_parnumerozone"
)
ZONE_LOOKUP_FIELD = "no_zone"

#: The grid itself, keyed on the id `zone_ids` resolves. ``diffusion=true`` is
#: what the city's own download link carries.
GRID_PDF_URL = "https://zonage.saguenay.ca/rapports/v1/zonages/grille/pdf/{zone_id}?diffusion=true"


class SaguenayZoningError(RuntimeError):
    """The zone index or a grid could not be read."""


# ---------------------------------------------------------------------------
# the zones
# ---------------------------------------------------------------------------


class SaguenayZoningClient:
    """Zone-number to grid-PDF, over the city's two public endpoints.

    The polygons are not fetched here - they come off Données Québec through
    the same resource the other cities' layers do. What this owns is the part
    only Saguenay has: the id lookup and the generated document.
    """

    def __init__(
        self,
        *,
        lookup_url: str = ZONE_LOOKUP_URL,
        grid_url: str = GRID_PDF_URL,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.1,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.lookup_url = lookup_url
        self.grid_url = grid_url
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
            allowed_methods=("GET", "POST"),
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["User-Agent"] = USER_AGENT
        return session

    def _wait(self) -> None:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)

    def zone_id(self, no_zone: str) -> int | None:
        """The reporting service's id for one zone number, or None if unknown.

        ``None`` rather than a raise: the zoning layer and the zoning service
        are two publications of the same by-law and they do not agree exactly
        - the layer draws 2,837 distinct numbers and the service lists 2,919 -
        so a polygon the service has never heard of is a fact to record, not a
        partition to fail.
        """
        self._wait()
        try:
            response = self._session.post(
                self.lookup_url,
                data={ZONE_LOOKUP_FIELD: str(no_zone).strip()},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise SaguenayZoningError(
                f"{self.lookup_url}: looking up zone {no_zone!r} failed ({exc})"
            ) from exc

        data = payload.get("data") or []
        if not isinstance(data, list) or not data:
            return None
        # A lookup that came back unfiltered is the proxy ignoring the body -
        # every zone would then resolve to the same first row, which is a
        # silently wrong index rather than a missing one.
        meta = payload.get("meta") or {}
        if int(meta.get("totalCount") or 0) > len(data):
            raise SaguenayZoningError(
                f"{self.lookup_url}: zone {no_zone!r} returned "
                f"{meta.get('totalCount')} rows; the lookup was not filtered."
            )
        found = data[0]
        if str(found.get("numero_zone")) != str(no_zone).strip():
            return None
        return int(found["id"])

    def zone_ids(
        self,
        zones: Iterable[str],
        *,
        progress: "Any | None" = None,
    ) -> dict[str, int]:
        """`zone_id` for each of ``zones``, skipping the ones with no row."""
        index: dict[str, int] = {}
        for position, no_zone in enumerate(dict.fromkeys(zones), start=1):
            found = self.zone_id(no_zone)
            if found is not None:
                index[str(no_zone)] = found
            if progress and position % 100 == 0:
                progress(f"resolved {position} zone number(s), {len(index)} found")
        return index

    def grid_url_for(self, zone_id: int) -> str:
        """The published URL of one zone's grid."""
        return self.grid_url.format(zone_id=int(zone_id))

    def fetch_grid(self, zone_id: int) -> bytes:
        """One zone's grid, as the service generates it."""
        url = self.grid_url_for(zone_id)
        self._wait()
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise SaguenayZoningError(f"{url}: {exc}") from exc
        content = response.content
        if not content.startswith(b"%PDF"):
            raise SaguenayZoningError(
                f"{url}: not a PDF (Content-Type "
                f"{response.headers.get('Content-Type')!r})"
            )
        return content


# ---------------------------------------------------------------------------
# reading one grid
# ---------------------------------------------------------------------------

#: The title every grid page carries, with its spaces taken out - see
#: `_despace` for why nothing here is matched on the text as printed.
GRID_TITLE = "grilledesusagesetdesnormes"

#: How far, in points, two cells' midpoints may sit apart and still be read as
#: the same column. Much tighter than Montreal's twelve, and it has to be: a
#: Saguenay grid fits between one and seventeen columns into the same band, so
#: at the wide end the pitch is twenty-four points and at the narrow end it is
#: eleven. One column's cells agree to within two points across a whole page -
#: the anchor rows below place their values at 316, 316, 317, 316, 315 - so
#: clustering at four separates seventeen columns without splitting one.
COLUMN_CLUSTER_TOLERANCE = 4.0

#: The share of the pitch between two columns within which a cell that is not
#: an anchor - a usage code, a structure star - is attributed to one. Half the
#: pitch would reach exactly to the midpoint between two columns; this stops
#: short of it, so a cell that belongs to neither is dropped rather than
#: assigned to whichever is a hair nearer.
COLUMN_REACH = 0.4

#: What a grid prints where a norm does not apply, despaced.
_ABSENT = {"-", "--", "s.o.", "so", "n/a", "na", ""}

_NUMBER = re.compile(r"^-?\d+(?:[.,]\d+)?$")

#: The sections the grid prints, by the prefix each header row despaces to.
#: Matched longest-first, so ``5-1-terrain`` is the lot block and ``5-normes``
#: the heading above it, and ``1-classes`` is not confused with ``10-``,
#: ``11-``, ``12-`` or ``13-``.
_SECTIONS: Mapping[str, str] = {
    "1-classesd'usages": "usages",
    "2-usagespecifiquementautorise": "only_permitted",
    "3-usagespecifiquementexclu": "excluded",
    "4-structuredubatimentprincipal": "structure",
    "5-normesdelotissement": "lotting",
    "5-1-terrain": "lot",
    "6-normesdezonage": "zoning",
    "6-1-marges": "margins",
    "6-2-dimensions": "dimensions",
    "6-3-rapports": "ratios",
    "7-autresreglements": "other_bylaws",
    "8-articlesapplicables": "articles",
    "9-normesspecifiques": "specific_norms",
    "10-dispositionsparticulieres": "particular",
    "11-notes": "notes",
    "12-avisdemotion": "motion",
    "13-amendements": "amendments",
}
_SECTION_PREFIXES: tuple[str, ...] = tuple(
    sorted(_SECTIONS, key=len, reverse=True)
)

#: The rows whose cells define the columns: the six margins and the three
#: building dimensions, each of which a grid fills in for every column it has.
#: The lot block's three rows are deliberately *not* anchors - a grid may state
#: lotting norms for fewer columns than it has - but they are read.
_ANCHOR_LABELS = frozenset(
    {
        "avant(metre)",
        "laterale1(metre)",
        "laterale2(metre)",
        "lateralesurrue(metre)",
        "arriere(metre)",
        "arrieresurrue(metre)",
        "hauteur(etage)",
        "largeur(metre)",
        "superficied'implantationausol(metrecarre)",
    }
)

#: The *6-1 - MARGES DU BÂTIMENT PRINCIPAL* rows, and the `GridColumn` field
#: each fills. This is the whole of Saguenay's setback vocabulary and the one
#: place it differs in shape from the other two cities: the by-law states a
#: *separate* margin for a side and a rear line that faces a street, where
#: Montreal states one *Avant secondaire* and Quebec City states none at all.
#:
#: Which of the two on-street figures governs is a fact about the *lot* and not
#: about the zone - a corner lot's second street edge is a side line, a through
#: lot's is its rear - so both are carried here and
#: `postgis.compute_lot_buildable_setbacks` chooses per lot, by the same test
#: it already uses to tell a rear line from a side one. See
#: `GridColumn.rear_on_street_margin_min_m`.
_MARGIN_LABELS: Mapping[str, str] = {
    "avant(metre)": "front_margin_min_m",
    "lateralesurrue(metre)": "secondary_front_margin_min_m",
    "arrieresurrue(metre)": "rear_on_street_margin_min_m",
    "arriere(metre)": "rear_margin_min_m",
}

#: The two side margins, which have one field between them - see `_side_margin`.
_SIDE_LABELS: tuple[str, ...] = ("laterale1(metre)", "laterale2(metre)")

#: *4 - STRUCTURE DU BÂTIMENT PRINCIPAL*, as the ``I-J-C`` letters
#: `postgis.SIDE_SETBACK_FACTORS` already reads. A column starred on a row may
#: be built in that form; the most permissive starred form is what decides
#: whether the side margin applies at all.
_STRUCTURE_LABELS: Mapping[str, str] = {
    "detachee(isolee)": "I",
    "jumelee": "J",
    "enrangee": "C",
    "contigue": "C",
}
_STRUCTURE_ORDER = ("I", "J", "C")

#: The mark a grid puts in a column to say the row applies to it. The service
#: prints a black star; `_is_mark` is what actually decides, because the rows
#: that carry one carry nothing else, and a reader keyed to one code point
#: would go silently blank if the service changed its dingbat.
_MARK = "★"

#: A usage class code as section 1 prints it: a letter or two, a number, and
#: sometimes a trailing letter - ``H01``, ``c4a``, ``I2``, ``p1a``, ``S1``.
#: ``ID`` (*industrie différée*) carries no digit and is listed separately.
_USAGE_CODE = re.compile(r"^([A-Za-z]{1,2})(\d{1,2})([a-z])?$")
_EXTRA_USAGE_CODES: Mapping[str, str] = {"id": "I"}

#: Which family each code's leading letter belongs to, and the
#: `envelope_assets.USAGE_CATEGORIES` name it is reported under.
#:
#: ``S`` is *Services* - administrative, financial, real-estate, personal,
#: professional and research - and is read as commerce rather than as a family
#: of its own: those are the uses Montreal's ``C`` classes carry and the ones
#: this platform prices at an office or retail rate. ``P`` (parks, cultural,
#: sporting and public-affairs establishments) and ``R`` (recreation) are
#: equipment, which is recognised and deliberately not priced. ``A``
#: (agriculture) and ``F`` (forestry) are priced by nothing here and are noted
#: on the column rather than emitted as a usage.
_FAMILY_BY_LETTER: Mapping[str, str] = {
    "H": "H",
    "C": "C",
    "S": "C",
    "I": "I",
    "P": "E",
    "R": "E",
}
_FAMILY_CATEGORY: Mapping[str, str] = {
    "H": "habitation",
    "C": "commerce",
    "I": "industrie",
    "E": "equipements",
}
_UNPRICED_LETTERS = frozenset({"A", "F"})

#: The most dwellings each *Habitation* class may hold, where by-law
#: VS-R-2012-3's own name for the class fixes it. Read exactly as far as the
#: name goes and no further: *unifamiliale*, *bifamiliale* and *trifamiliale*
#: are the single, the duplex and the triplex, and a *maison mobile* is one
#: dwelling by definition.
#:
#: ``None`` is "this class states no ceiling here", and it is the honest answer
#: for the rest. *Multifamiliale catégorie A/B/C* are ranges the by-law defines
#: in its definitions chapter and the grid never prints - no sampled grid
#: states a dwelling count in any row or note - so guessing at them would put a
#: cap on the solver that nothing published supports. `class_max_dwellings`
#: below lifts the ceiling entirely when one of these is authorised, which is
#: the same rule `program.class_max_dwellings` applies to Montreal's bare ``H``.
CLASS_MAX_DWELLINGS: Mapping[str, int | None] = {
    "H01": 1,  # Unifamiliale
    "H02": 2,  # Bifamiliale
    "H03": 3,  # Trifamiliale
    "H04": None,  # Multifamiliale, categorie A
    "H05": None,  # Multifamiliale, categorie B
    "H06": None,  # Multifamiliale, categorie C
    "H07": 1,  # Maison mobile
    "H08": None,  # Habitation collective
    "H09": 1,  # Habitation rurale
    "H10": 1,  # Habitation de villegiature
}

#: *La hauteur totale maximale à respecter pour le bâtiment principal est de
#: 12,5 mètres* - the one norm Saguenay states in prose rather than in a cell,
#: printed under *9 - NORMES SPÉCIFIQUES* or *10 - DISPOSITIONS
#: PARTICULIÈRES*. It is a real ceiling on the envelope and the only one in
#: metres the grid gives, so it is read; being stated once for the zone, it
#: goes on every column of the page, with the sentence it came from in
#: `GridColumn.notes`.
_HEIGHT_SENTENCE = re.compile(
    r"hauteur\s*totale\s*maximale.*?est\s*de\s*(\d+(?:[.,]\d+)?)\s*m", re.IGNORECASE
)

#: How a zone number is printed in the page header: ``Zone 70520``.
_ZONE_HEADER = re.compile(r"^zone(\d+)$")


def _is_mark(cell: _Cell) -> bool:
    """Whether a cell marks its column, on a row whose values are marks.

    True for anything printed that is not a quantity and not a unit caption.
    The rows this is asked about - the structure forms, and the specifically
    authorised and excluded uses - state no quantity at all: a column either
    carries the service's star or carries nothing. Testing for *printed rather
    than numeric* is therefore the same answer as testing for the star, and it
    survives the service changing which glyph it draws.

    A ``min./max.`` pair is rejected along with a bare number, though no row
    this is called on prints one. The rule is meant to read "a mark is not a
    norm", and a reading that let ``1/3`` through would only be correct by
    accident of which rows happen to ask.
    """
    if _is_caption(cell.text):
        return False
    text = _despace(cell.text)
    if not text or text in _ABSENT:
        return False
    parts = text.split("/")
    return not all(_number(part) is not None for part in parts if part)


def _despace(text: object) -> str:
    """A cell as it can be matched: unaccented, lower-cased, and unspaced.

    Every other reader here normalises whitespace; this one removes it. These
    PDFs are typeset with the letters of a word kerned apart one pair at a
    time, so a row label arrives from the content stream as ``Latérale 1 (m
    ètre)`` and the same label on the next grid as ``Latérale 1(m ètre)``. The
    spaces are an artefact of the typesetting and carry no information at all
    in a label or a number, so taking them all out is what makes one spelling
    of the match work on every grid - and it is why ``7 0520`` and ``70520``
    are the same zone.
    """
    if text is None:
        return ""
    flat = unicodedata.normalize("NFKD", str(text))
    flat = "".join(ch for ch in flat if not unicodedata.combining(ch))
    flat = flat.replace("’", "'").replace("ʼ", "'")
    flat = flat.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", "", flat).strip().lower()


def parse_grid_pdf(content: bytes, *, url: str | None = None) -> list[GridColumn]:
    """Every column of the grid in ``content``.

    Returns an empty list for a grid that states no norms at all, which is a
    real and common answer here and not a failure: Saguenay draws a zone over
    every river, park and rail yard in the municipality, and those grids print
    their usage classes and then leave the whole of sections 5 and 6 blank.
    There is no envelope to solve on such a zone, and raising would cost the
    partition the eight hundred zones that do state norms.

    **A wide grid runs over several pages, and they are one grid.** A zone
    authorising more classes than fit across a page is printed as a sheet per
    slice of columns, each repeating the row labels and the zone-level
    sections: zone 64500 takes three such sheets for its forty-one columns,
    interleaved with three pages of its amendment table, which carry no grid
    and are skipped. The columns are therefore numbered across the document
    rather than within a page - `column_index` is part of the key
    `silver.zoning_grid_columns` is written on, and restarting it per page
    would collapse the second sheet onto the first.
    """
    from dataclasses import replace
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError
    import io

    where = url or "<bytes>"
    try:
        reader = PdfReader(io.BytesIO(content))
        pages = [_page_rows(page) for page in reader.pages]
    except (PdfReadError, ValueError, OSError) as exc:
        raise GridParseError(f"{where}: unreadable PDF ({exc})") from exc

    if not any(is_grid_page(rows) for rows in pages):
        raise GridParseError(
            f"{where}: no page carries a {GRID_TITLE!r} heading"
        )

    columns: list[GridColumn] = []
    for rows in pages:
        if not is_grid_page(rows):
            continue
        base = len(columns)
        columns.extend(
            replace(column, column_index=base + offset)
            for offset, column in enumerate(parse_grid_page(rows))
        )
    return columns


def is_grid_page(rows: Sequence[Sequence[_Cell]]) -> bool:
    """Whether a page is one of these grids.

    The title sits in the page header, which is at the *end* of the extracted
    rows rather than the start - see `_reading_order` - so unlike Montreal's
    reader this looks at the whole page rather than the first few rows.
    """
    return any(GRID_TITLE in _despace(_line(row)) for row in rows)


def _line(row: Iterable[_Cell]) -> str:
    return " ".join(cell.text for cell in row)


def _reading_order(
    rows: Sequence[Sequence[_Cell]],
) -> list[Sequence[_Cell]]:
    """The page's rows top to bottom, whichever way they arrived.

    `zoning_grid._page_rows` sorts on descending ``ty``, which is the top of a
    page in the ordinary case. These documents are generated by a reporting
    service that lays its content out downwards in an inverted text space, so
    the same sort hands back the amendment table first and the letterhead
    last. Rather than assume either convention, the section headers decide:
    they are numbered 1 to 13 down the page, so the order that reads them
    ascending is the reading order. A page carrying fewer than two headers is
    left as it came, having nothing to say either way.
    """
    numbered = [
        (index, _section_number(row))
        for index, row in enumerate(rows)
        if _section_number(row) is not None
    ]
    if len(numbered) < 2:
        return list(rows)
    descending = sum(
        1
        for (_, first), (_, second) in zip(numbered, numbered[1:])
        if second < first
    )
    ascending = len(numbered) - 1 - descending
    return list(reversed(rows)) if descending > ascending else list(rows)


def _section_number(row: Sequence[_Cell]) -> float | None:
    """The section a header row opens, as a sortable number, or None."""
    name = _section_of(row)
    if name is None:
        return None
    label = _despace(row[0].text)
    for prefix in _SECTION_PREFIXES:
        if label.startswith(prefix):
            head = prefix.split("-")[:2]
            major = head[0]
            minor = head[1] if len(head) > 1 and head[1].isdigit() else "0"
            return float(f"{int(major)}.{int(minor)}")
    return None


def _section_of(row: Sequence[_Cell]) -> str | None:
    """Which section a row opens, or None where it opens none."""
    label = _despace(row[0].text)
    for prefix in _SECTION_PREFIXES:
        if label.startswith(prefix):
            return _SECTIONS[prefix]
    return None


def parse_grid_page(rows: Sequence[Sequence[_Cell]]) -> list[GridColumn]:
    """The columns of one grid page, from its rows of positioned cells."""
    rows = _reading_order(rows)
    zone = _zone(rows)
    centers = _column_centers(rows)
    if not centers:
        return []
    reach = _column_reach(centers)

    fields: list[dict[str, Any]] = [
        {
            "zone": zone,
            "column_index": index,
            "usages_by_category": {},
            # Saguenay's grid has no *Niveaux de bâtiment autorisés* block at
            # all: the by-law regulates which storeys a usage may occupy in its
            # own chapters, not in the grid. Every column therefore authorises
            # its usages on every level it has, which is what an unstated
            # restriction means and what `program.permitted_floors` reads.
            "levels": {BuildingLevel.ALL},
            "codes": [],
        }
        for index in range(len(centers))
    ]
    notes: list[list[str]] = [[] for _ in centers]
    sides: list[dict[str, float]] = [{} for _ in centers]
    unpriced: list[set[str]] = [set() for _ in centers]
    descriptions: list[str] = []
    section: str | None = None

    for row in rows:
        opened = _section_of(row)
        if opened is not None:
            section = opened
            descriptions = []
            continue
        if section is None:
            continue

        label = _despace(row[0].text)
        values = _by_column(row[1:], centers, reach)

        if section == "usages":
            _read_usage_row(row, centers, reach, fields, unpriced)
        elif section in ("only_permitted", "excluded"):
            _read_marked_text_row(
                row, centers, reach, fields, descriptions, section
            )
        elif section == "structure" and label in _STRUCTURE_LABELS:
            letter = _STRUCTURE_LABELS[label]
            for index, cell in values.items():
                if _is_mark(cell):
                    fields[index].setdefault("modes", set()).add(letter)
        elif section == "lot" and label == "largeur(metre)":
            for index, cell in values.items():
                number = _number(cell.text)
                if number is not None:
                    fields[index]["min_lot_width_m"] = number
        elif section == "margins":
            if label in _MARGIN_LABELS:
                for index, cell in values.items():
                    number = _number(cell.text)
                    if number is not None:
                        fields[index][_MARGIN_LABELS[label]] = number
            elif label in _SIDE_LABELS:
                for index, cell in values.items():
                    number = _number(cell.text)
                    if number is not None:
                        sides[index][label] = number
        elif section == "dimensions" and label == "hauteur(etage)":
            for index, cell in values.items():
                low, high, note = _storeys(cell.text)
                if low is not None:
                    fields[index]["floors_min"] = low
                if high is not None:
                    fields[index]["floors_max"] = high
                if note:
                    notes[index].append(f"Hauteur (etage): {note}")

    height_max_m, height_note = _stated_height(rows)
    articles = _articles(rows)

    columns: list[GridColumn] = []
    for index, values in enumerate(fields):
        codes = values.pop("codes")
        modes = values.pop("modes", set())
        if not codes and not unpriced[index]:
            # A ruled band with no usage code at its head is a column of the
            # drawing rather than of the by-law - the same reading
            # `GridColumn.is_empty` gives Montreal's blank rightmost band.
            #
            # A band headed *only* by usages nothing here prices - agriculture,
            # forestry - is a different thing and is kept: it is a real column
            # of the by-law, its margins and storeys are read like any other's,
            # and its note says what it authorises. It comes back with no
            # `usages`, so `GridColumn.is_empty` is true of it and
            # `zoning_grid_columns` drops it exactly where it drops Montreal's
            # blank band. Reading and judging stay in the places they are.
            continue
        side, side_note = _side_margin(sides[index])
        column_notes = list(notes[index])
        if side_note:
            column_notes.append(side_note)
        if height_note:
            column_notes.append(height_note)
        if unpriced[index]:
            column_notes.append(
                "usages not carried (priced by nothing here): "
                + ", ".join(sorted(unpriced[index]))
            )
        usages, by_category = _families(codes)
        dwellings, dwelling_note = class_max_dwellings(codes)
        if dwelling_note:
            column_notes.append(dwelling_note)
        columns.append(
            GridColumn(
                zone=values.pop("zone"),
                column_index=len(columns),
                usages=usages,
                usages_by_category=by_category,
                levels=frozenset(values.pop("levels")),
                implantation_mode=_implantation(modes),
                side_margin_min_m=side,
                max_dwellings=dwellings,
                height_max_m=height_max_m,
                specific_articles=articles,
                notes=tuple(column_notes),
                **{
                    key: value
                    for key, value in values.items()
                    if key not in ("column_index", "usages_by_category")
                },
            )
        )
    return columns


# ---------------------------------------------------------------------------
# rows
# ---------------------------------------------------------------------------


def _read_usage_row(
    row: Sequence[_Cell],
    centers: Sequence[float],
    reach: float,
    fields: list[dict[str, Any]],
    unpriced: list[set[str]],
) -> None:
    """One row of *1 - CLASSES D'USAGES PERMISES*.

    The class code is printed *in* the column it belongs to, which is what
    makes a mixed zone readable: a grid authorising H01 in its first column and
    c1a in its second prints them at the two offsets its norms are printed at.
    The description runs down the left and is not read here - it is the
    by-law's prose for a code this platform knows by its code.
    """
    for index, cell in _by_column(row, centers, reach).items():
        code = _despace(cell.text)
        letter, normalised = _usage_family(code)
        if letter is None:
            continue
        if letter in _UNPRICED_LETTERS:
            unpriced[index].add(normalised)
            continue
        fields[index]["codes"].append(normalised)


def _read_marked_text_row(
    row: Sequence[_Cell],
    centers: Sequence[float],
    reach: float,
    fields: list[dict[str, Any]],
    descriptions: list[str],
    section: str,
) -> None:
    """A row of *2 - USAGE SPÉCIFIQUEMENT AUTORISÉ* or *3 - ... EXCLU*.

    These name one use in prose and star the columns it applies to, so the
    text is the row's label and the marks say whose it is. The label travels
    across rows because a long description wraps, and the star lands on the
    last of them.
    """
    label = row[0].text.strip()
    marked = {
        index
        for index, cell in _by_column(row[1:], centers, reach).items()
        if _is_mark(cell)
    }
    if not marked:
        if label and not _despace(label).startswith("#") and len(label) > 2:
            descriptions.append(label)
        return
    text = " ".join([*descriptions, label]).strip()
    descriptions.clear()
    text = re.sub(r"\s+", " ", text)
    # The code the city files the use under is printed beside the description
    # and is not the description; drop a leading bare number.
    text = re.sub(r"^\d{3,}\s*", "", text).strip()
    if not text:
        return
    key = "only_permitted_usages" if section == "only_permitted" else "excluded_usages"
    for index in marked:
        existing = fields[index].get(key)
        fields[index][key] = f"{existing}; {text}" if existing else text


def _zone(rows: Sequence[Sequence[_Cell]]) -> str | None:
    """The zone number printed in the page header, as ``Zone 70520``.

    It is the id that joins this grid to its polygon - ``no_zone`` on the
    layer, `feature_id` in `silver.lot_features` - so a grid that does not
    print one can be parsed but cannot be attached to a parcel.
    """
    for row in rows:
        for cell in row:
            found = _ZONE_HEADER.match(_despace(cell.text))
            if found:
                return found.group(1)
    return None


def _articles(rows: Sequence[Sequence[_Cell]]) -> str | None:
    """*8 - ARTICLES APPLICABLES*, as the one string the zone states it in.

    Stated once for the zone rather than per column, like Montreal's *Articles
    visés*, so every column of the page carries it.
    """
    collected: list[str] = []
    section: str | None = None
    for row in _reading_order(rows):
        opened = _section_of(row)
        if opened is not None:
            section = opened
            continue
        if section != "articles":
            continue
        text = re.sub(r"\s+", " ", _line(row)).strip()
        if text and _despace(text) not in _ABSENT:
            collected.append(text)
    return "; ".join(collected) if collected else None


def _stated_height(
    rows: Sequence[Sequence[_Cell]],
) -> tuple[float | None, str | None]:
    """The height ceiling the zone states in prose, and the sentence it is in.

    The *lowest* stated, where a grid states more than one: zone 1000 prints
    the same 9,5 m sentence under both *NORMES SPÉCIFIQUES* and *DISPOSITIONS
    PARTICULIÈRES*, and a grid printing two different ceilings is stating that
    both bind.
    """
    found: list[tuple[float, str]] = []
    for row in rows:
        text = re.sub(r"\s+", " ", _line(row)).strip()
        match = _HEIGHT_SENTENCE.search(text.replace(" ", ""))
        if not match:
            continue
        value = _number(match.group(1))
        if value is not None and value > 0:
            found.append((value, text))
    if not found:
        return None, None
    height, sentence = min(found, key=lambda pair: pair[0])
    return height, (
        f"height_max_m: {height:g} m read from the zone's stated norm "
        f"({sentence[:120]}), which the grid prints in prose rather than in a "
        "column - it is applied to every column of the zone"
    )


# ---------------------------------------------------------------------------
# values
# ---------------------------------------------------------------------------


def _usage_family(code: str) -> tuple[str | None, str]:
    """A section-1 cell as ``(family letter, code)``, or ``(None, "")``.

    The letter decides the family and the code is kept as the by-law spells
    it, upper-cased on the letter so ``c4a`` and ``C4a`` are one code.
    """
    if code in _EXTRA_USAGE_CODES:
        return _EXTRA_USAGE_CODES[code], code.upper()
    found = _USAGE_CODE.match(code)
    if not found:
        return None, ""
    letter = found.group(1).upper()
    if letter not in _FAMILY_BY_LETTER and letter not in _UNPRICED_LETTERS:
        return None, ""
    normalised = f"{letter}{found.group(2)}{found.group(3) or ''}"
    if letter in _UNPRICED_LETTERS:
        return letter, normalised
    return _FAMILY_BY_LETTER[letter], normalised


def _families(codes: Sequence[str]) -> tuple[tuple[str, ...], dict[str, str]]:
    """The codes of one column as the families the solver's matchers read.

    Emitted as the bare family letters - ``H``, ``C``, ``I``, ``E`` - for the
    reason `hbu_dataplatform.cities.quebec_city.zoning.grid_columns` emits them: Saguenay's classes do
    not map onto Montreal's numbered ones, and `program.is_residential_usage`
    matches a whole code. The classes themselves travel in
    ``usages_by_category``, where a reader and the map both find them.
    """
    by_family: dict[str, list[str]] = {}
    for code in codes:
        family, _ = _usage_family(_despace(code))
        if family is None or family in _UNPRICED_LETTERS:
            continue
        by_family.setdefault(family, []).append(code)
    ordered = [family for family in ("H", "C", "I", "E") if family in by_family]
    return (
        tuple(ordered),
        {
            _FAMILY_CATEGORY[family]: ", ".join(dict.fromkeys(by_family[family]))
            for family in ordered
        },
    )


def class_max_dwellings(codes: Iterable[str]) -> tuple[int | None, str | None]:
    """The dwelling ceiling a column's *Habitation* classes imply, and a note.

    The **most permissive** of the classes present, and ``None`` as soon as one
    of them states no ceiling - the same rule `program.class_max_dwellings`
    applies, and for the same reason: a column headed ``H01, H04`` authorises
    both, so capping it at the single-family one would forbid the building its
    own grid allows.
    """
    residential = [code for code in codes if code.upper().startswith("H")]
    if not residential:
        return None, None
    ceilings = [CLASS_MAX_DWELLINGS.get(code.upper(), None) for code in residential]
    if any(ceiling is None for ceiling in ceilings):
        open_ended = sorted(
            code
            for code, ceiling in zip(residential, ceilings)
            if ceiling is None
        )
        return None, (
            "max_dwellings: none carried - "
            + ", ".join(open_ended)
            + " state no dwelling ceiling the grid or the class name fixes"
        )
    return max(ceilings), None


def _side_margin(stated: Mapping[str, float]) -> tuple[float | None, str | None]:
    """The two *Latérale* margins as the one the setback carve applies.

    Saguenay states a margin for each side line and this platform subtracts one
    distance from every side-class edge, so the two have to become one number.
    The **mean** is that number, and it is exact rather than a compromise: for
    a parcel whose side lines are parallel - which is what a side line is -
    leaving 4 m on one side and 8 m on the other removes precisely as much
    ground as leaving 6 m on both. Taking the smaller would hand the lot four
    metres of width the by-law does not allow, and taking the larger would take
    four away from it.

    The two printed figures are not lost: the note carries them, and a row of
    `silver.lot_buildable_setbacks` can be read back against them.
    """
    first = stated.get(_SIDE_LABELS[0])
    second = stated.get(_SIDE_LABELS[1])
    present = [value for value in (first, second) if value is not None]
    if not present:
        return None, None
    if len(present) == 1:
        return present[0], None
    if first == second:
        return first, None
    mean = (first + second) / 2
    return mean, (
        f"side_margin_min_m: {mean:g} m is the mean of Laterale 1 "
        f"{first:g} m and Laterale 2 {second:g} m, which removes the same "
        "ground from a parcel with parallel side lines as the two do"
    )


def _implantation(modes: Iterable[str]) -> str | None:
    """The starred structures, in the ``I-J-C`` order `postgis` parses."""
    present = {mode for mode in modes}
    ordered = [mode for mode in _STRUCTURE_ORDER if mode in present]
    return "-".join(ordered) if ordered else None


def _storeys(text: str) -> tuple[int | None, int | None, str | None]:
    """A *Hauteur (étage)* cell, printed ``min./max.`` as ``1/3``."""
    raw = _despace(text)
    if raw in _ABSENT:
        return None, None, None
    parts = raw.split("/")
    if len(parts) == 2:
        low, high = (_number(part) for part in parts)
        if low is None and high is None:
            return None, None, text.strip()
        return (
            int(low) if low is not None else None,
            int(high) if high is not None else None,
            None,
        )
    value = _number(raw)
    if value is None:
        return None, None, text.strip()
    # A single figure under a min./max. caption is the maximum it is named
    # for, and says nothing about the other bound.
    return None, int(value), f"{text.strip()} read as a maximum"


def _number(text: object) -> float | None:
    """A cell as the number it prints, or None where it prints none."""
    raw = _despace(text).replace(",", ".")
    if raw in _ABSENT:
        return None
    return float(raw) if _NUMBER.match(raw) else None


# ---------------------------------------------------------------------------
# geometry
# ---------------------------------------------------------------------------


def _column_centers(rows: Sequence[Sequence[_Cell]]) -> list[float]:
    """The midpoint of each column, left to right.

    Built from `_ANCHOR_LABELS` only, and clustered tightly - see
    `COLUMN_CLUSTER_TOLERANCE`. The ``min.`` and ``min./max.`` captions each
    row prints between its label and its values sit to the left of the band
    and would otherwise found a column of their own, so they are dropped by
    name rather than by position: unlike Montreal's, they are close enough to
    the first column on a seventeen-column grid to land inside it.
    """
    midpoints = sorted(
        cell.center
        for row in rows
        if _despace(row[0].text) in _ANCHOR_LABELS
        for cell in row[1:]
        if not _is_caption(cell.text)
    )
    if not midpoints:
        return []

    clusters: list[list[float]] = [[midpoints[0]]]
    for midpoint in midpoints[1:]:
        if midpoint - clusters[-1][-1] <= COLUMN_CLUSTER_TOLERANCE:
            clusters[-1].append(midpoint)
        else:
            clusters.append([midpoint])
    return [
        sum(cluster) / len(cluster)
        for cluster in clusters
        if len(cluster) >= MIN_COLUMN_SUPPORT
    ]


def _is_caption(text: str) -> bool:
    """Whether a cell is a row's unit caption rather than one of its values."""
    raw = _despace(text)
    return raw.startswith("min") or raw.startswith("max")


def _column_reach(centers: Sequence[float]) -> float:
    """How far from a column's midpoint a cell may sit and still be in it.

    A share of the *pitch* rather than a constant, because these grids fit
    between one and seventeen columns into the same band and a tolerance that
    is generous at nine columns reaches into the neighbour at seventeen. With
    one column there is no pitch to measure and nothing to confuse it with, so
    the whole page is in reach.
    """
    if len(centers) < 2:
        return math.inf
    gaps = sorted(
        second - first for first, second in zip(centers, centers[1:])
    )
    pitch = gaps[len(gaps) // 2]
    return max(pitch * COLUMN_REACH, COLUMN_CLUSTER_TOLERANCE)


def _by_column(
    cells: Iterable[_Cell], centers: Sequence[float], reach: float
) -> dict[int, _Cell]:
    """Attribute cells to columns by midpoint; drop what lands in none."""
    placed: dict[int, _Cell] = {}
    for cell in cells:
        if _is_caption(cell.text):
            continue
        distances = [abs(cell.center - center) for center in centers]
        nearest = min(range(len(centers)), key=distances.__getitem__)
        if distances[nearest] <= reach:
            placed[nearest] = cell
    return placed


__all__ = [
    "CLASS_MAX_DWELLINGS",
    "GRID_PDF_URL",
    "GRID_URL_COLUMN",
    "LIMITS_DATASET",
    "LIMITS_GEOJSON",
    "LIMIT_NAME_FIELD",
    "LIMIT_TYPE_FIELD",
    "MUNICIPALITY_FIELD",
    "OBJECT_ID_FIELD",
    "SaguenayZoningClient",
    "SaguenayZoningError",
    "ZONE_CODE_FIELD",
    "ZONE_LOOKUP_URL",
    "ZONING_DATASET",
    "ZONING_GEOJSON",
    "ZONING_SLUG",
    "class_max_dwellings",
    "is_grid_page",
    "parse_grid_page",
    "parse_grid_pdf",
]
