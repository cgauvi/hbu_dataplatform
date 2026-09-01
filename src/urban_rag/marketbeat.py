"""Client for Cushman & Wakefield's MarketBeat, the Montreal office and
industrial rent surveys.

CMHC prices a dwelling and the Altus guide prices a building to put up; neither
says what a square foot of retail, office or warehouse *earns*. That number was
a stated constant in `urban_rag.program` until this module, and this is the
only free, quarterly, machine-reachable publication of it for Montreal.

Three things about the publication shape every call made here.

**The URLs cannot be constructed, so they are discovered.** The filename drifts
between sectors and between quarters - ``montreal-americas-office-marketbeat-
q22026.pdf`` one quarter, ``montreal_americas_office_marketbeat-q12026-.pdf``
the next, ``q2-2025-montreal-industrial-marketbeat.pdf`` the one before - and a
name that worked last quarter answers 403 this one. The *path* is the stable
part: every report sits under ``/marketbeat-pdfs/<year>/q<n>/canada/``. So
`discover_reports` reads the landing page and takes the year and quarter off
the path and the sector off the filename, rather than guessing at a name. That
is the same posture `urban_rag.rag.documents` takes toward the zoning PDFs -
follow the links the publisher gives rather than inventing them.

**The rents are on the submarket table, not only in the headline.** Page one
carries one figure for the whole market; page two carries one row per
submarket, and Montreal's *Midtown North* is the submarket Villeray-Saint-
Michel-Parc-Extension sits in. A borough-level rent is a much better answer for
this platform than an island-level one, and it is the same improvement
`vacancy_rates` makes over a CMA-wide CMHC figure. `MONTREAL_TOTALS` is the
fallback for a borough no submarket is mapped to.

**The two sectors quote rent differently, and the difference is 30%.** The
office table is *full service* - the footnote says so - which is a gross rent
with operating costs in it. The industrial table is *weighted direct net
asking*, with the operating costs in a column of their own, ``OVERALL WEIGHTED
AVG ADDITIONAL RENT``. So an industrial gross rent is the sum of two columns
and an office gross rent is one, and `parse_submarkets` returns both under one
schema so nothing downstream has to remember which is which. Reading the
industrial net as though it were gross understates a warehouse by a quarter,
which is the kind of error that produces a plausible number rather than a
crash.

**The rows have a variable number of columns.** A submarket with no
construction under way simply omits those cells, so `Montreal Midtown North`
has five numbers before its rents and `Montreal East` has seven. The parser
therefore reads *from the right*: the trailing money tokens are the rents
whatever precedes them, and the leading non-numeric tokens are the name. A
column-index parser would silently take a square footage as a rent on exactly
the rows that omit a column.

Deliberately free of Dagster imports, mirroring `urban_rag.estimator`,
`urban_rag.role_foncier` and `urban_rag.cmhc`.
"""

from __future__ import annotations

import io
import re
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

#: The page that lists every Montreal MarketBeat. Read for its links rather
#: than for its prose - see the module docstring on why the PDF names cannot be
#: constructed.
DEFAULT_LANDING_URL = (
    "https://www.cushmanwakefield.com/en/canada/insights/canada-marketbeats/"
    "montreal-marketbeats"
)

#: The two sectors this reads. C&W publishes no Montreal *retail* MarketBeat,
#: which is why `urban_rag.crspi` exists: the retail rent this platform needs
#: is a stated base carried forward by Statistics Canada's index, and there is
#: no free survey of the level to replace it with.
OFFICE = "office"
INDUSTRIAL = "industrial"
SECTORS: tuple[str, ...] = (OFFICE, INDUSTRIAL)

#: What each sector's rent column actually is, as its own footnote states it.
#: Office is "full service asking" - gross, operating costs included. Industrial
#: is "weighted direct net asking $psf/year", with the operating costs beside it
#: in `ADDITIONAL RENT`. Named here because the difference is about a quarter of
#: an industrial rent and nothing downstream should have to rediscover it.
RENT_BASIS: dict[str, str] = {OFFICE: "gross", INDUSTRIAL: "net"}

#: The row of the submarket table covering the whole market. Spelled with the
#: accent the PDF uses on the industrial report and without it on the office
#: one, so it is matched case- and accent-insensitively - see `_is_totals`.
MONTREAL_TOTALS = "MONTREAL TOTALS"

#: A `$1,234.56` cell. The thousands separator is optional and never appears in
#: a rent, but the same pattern is used to reject the square footages that
#: precede one.
_MONEY = re.compile(r"^\$[\d,]+(?:\.\d{1,2})?$")

#: Anything that opens with a digit, a sign or a bracket: the numeric cells a
#: submarket name is not. `-207,581` and `(1,234)` are both absorption figures
#: and `4.7%` is a vacancy rate.
_NUMERIC = re.compile(r"^[(\-+]?[\d.,]")

#: `/marketbeat-pdfs/<year>/q<n>/` - the one part of a report's URL that has
#: not moved. Everything after it has.
_PERIOD_IN_PATH = re.compile(r"/marketbeat-pdfs/(\d{4})/q([1-4])/", re.I)

#: How many trailing money cells a submarket row carries. Both sectors publish
#: two; a row with one (an office submarket whose Class A cell is `N/A`) still
#: parses, and `None` lands in the second.
_MAX_RENT_CELLS = 2

#: Where the submarket table starts and stops. Both are needed, and not just
#: the second: the office report prints its transaction blocks *above* the
#: table and the industrial one below it, so a window anchored only at the end
#: empties one of the two. See `_table_lines`.
_TABLE_START = re.compile(r"^SUBMARKET\b", re.I)
_TABLE_END = re.compile(r"^KEY (LEASE|SALES) TRANSACTIONS", re.I)

#: The fewest numeric cells a real submarket row carries between its name and
#: its rents. Every one states at least an inventory, a vacant area and a
#: vacancy rate; a stray line that happens to end in money does not.
_MIN_DATA_CELLS = 3


class MarketBeatError(RuntimeError):
    """A report could not be discovered, fetched, or read as a MarketBeat."""


@dataclass(frozen=True)
class Report:
    """One published MarketBeat: which sector, which quarter, and where.

    ``year`` and ``quarter`` come off the URL *path* rather than the filename,
    for the reason the module docstring gives, and they are what
    `latest_by_sector` orders on - a landing page lists five quarters and the
    newest is the one this platform wants.
    """

    sector: str
    year: int
    quarter: int
    url: str

    @property
    def period(self) -> str:
        """``2026-Q2`` - how a quarter is written on every row this produces."""
        return f"{self.year}-Q{self.quarter}"

    @property
    def period_start(self) -> str:
        """First day of the quarter, for lining a report up with an index.

        `urban_rag.crspi` publishes a quarter as the month it starts in, so
        this is what makes the two joinable without either of them having to
        know the other's spelling.
        """
        return f"{self.year}-{3 * (self.quarter - 1) + 1:02d}-01"


def discover_reports(html: str) -> tuple[Report, ...]:
    """Every Montreal MarketBeat the landing page links to.

    The sector is read off the filename and the period off the path. A link
    that carries neither is not a MarketBeat - the page also links a vendor
    code of conduct - and is dropped rather than guessed at.

    Deduplicated on (sector, period) keeping the first seen, because the page
    occasionally lists a revised report (`...-q3-2025-v2.pdf`) beside the one
    it replaced, and the revision is listed first.
    """
    reports: dict[tuple[str, int, int], Report] = {}
    for href in re.findall(r'href="([^"]+\.pdf[^"]*)"', html, re.I):
        match = _PERIOD_IN_PATH.search(href)
        if not match:
            continue
        name = href.rsplit("/", 1)[-1].lower()
        sector = next((s for s in SECTORS if s in name), None)
        if sector is None:
            continue
        year, quarter = int(match.group(1)), int(match.group(2))
        reports.setdefault((sector, year, quarter), Report(sector, year, quarter, href))
    return tuple(
        sorted(reports.values(), key=lambda r: (r.sector, -r.year, -r.quarter))
    )


def latest_by_sector(reports: "tuple[Report, ...]") -> dict[str, Report]:
    """The newest report for each sector, keyed by sector.

    Raises when a sector is missing entirely rather than returning a short
    mapping: a partition with no industrial rent would price every warehouse in
    the borough at nothing, and that is a thing to fail on rather than to
    discover in a cap rate three assets later.
    """
    latest: dict[str, Report] = {}
    for report in sorted(reports, key=lambda r: (r.year, r.quarter)):
        latest[report.sector] = report
    missing = [sector for sector in SECTORS if sector not in latest]
    if missing:
        raise MarketBeatError(
            f"The landing page lists no MarketBeat for: {', '.join(missing)}. "
            f"It offered {len(reports)} report(s) for "
            f"{sorted({r.sector for r in reports})}."
        )
    return latest


class MarketBeatFetcher:
    """Fetches the landing page and the report PDFs, with an on-disk cache.

    Cached by report *filename*, the same posture `RoleFetcher` and
    `CmhcFetcher` take: a published quarter is final, so only the first scrape
    date of a quarter pays for the download. The landing page itself is never
    cached - it is what says which quarter is current.
    """

    def __init__(
        self,
        *,
        cache_dir: Path | str,
        landing_url: str = DEFAULT_LANDING_URL,
        timeout_seconds: float = 120.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.landing_url = landing_url
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self._session = session or self._build_session(
            max_retries, ca_bundle, referer=landing_url
        )

    @staticmethod
    def _build_session(
        max_retries: int, ca_bundle: str | None, *, referer: str | None = None
    ) -> requests.Session:
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
        session.mount("https://", HTTPAdapter(max_retries=retry))
        # A browser string rather than this platform's own: the asset host
        # answers 403 to an unrecognised agent, which is a refusal to serve a
        # public PDF rather than a rate limit, and no retry gets past it.
        session.headers["User-Agent"] = BROWSER_USER_AGENT
        # And a Referer, because the agent string alone stopped being enough:
        # the asset CDN now answers 403 to a request that arrives without one,
        # while serving the same URL with it. The landing page is the honest
        # value - it is the page the PDF is linked from, and the page this
        # fetcher genuinely read to find the link.
        if referer:
            session.headers["Referer"] = referer
        return session

    def landing_html(self) -> str:
        """The page that lists the reports. Never cached - see the class docs."""
        return self._get(self.landing_url).decode("utf-8", errors="replace")

    def report_pdf(self, report: Report) -> bytes:
        """One report's bytes, from the cache when it is already there."""
        name = f"{report.sector}-{report.period}.pdf"
        cached = self.cache_dir / name
        if cached.exists() and cached.stat().st_size:
            return cached.read_bytes()
        body = self._get(report.url)
        if not body.startswith(b"%PDF"):
            # A 200 that is not a PDF is this host's other failure mode: an
            # asset that has moved answers with an HTML shell rather than a
            # 404, and pypdf's error for that names neither the sector nor the
            # quarter.
            raise MarketBeatError(
                f"{report.url}: answered {len(body)} bytes that are not a PDF"
            )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(body)
        return body

    def _get(self, url: str) -> bytes:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise MarketBeatError(f"{url}: {exc}") from exc
        return response.content


#: The agent string the asset host will serve. Named rather than inlined
#: because it is a fact about that host's behaviour - see `_build_session`.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

#: This platform's own agent, kept for the landing page where it is accepted
#: and for anyone reading this module to see which call is which.
PLATFORM_USER_AGENT = USER_AGENT


def parse_submarkets(pdf_bytes: bytes, *, sector: str) -> pd.DataFrame:
    """One row per submarket, with the rent columns put on one footing.

    Returns ``submarket``, ``net_rent_psf_cad``, ``additional_rent_psf_cad``,
    ``gross_rent_psf_cad`` and ``premium_rent_psf_cad`` - the same five columns
    for both sectors, filled in from whichever two the sector publishes:

    * **office** states one full-service (gross) rent for all classes and a
      second for Class AAA/A. The net and the additional rent are unknowable
      from it and stay null; ``premium_rent_psf_cad`` is the Class A figure.
    * **industrial** states a direct net rent and the additional rent beside
      it, so the gross is their sum and the premium is null.

    A gross rent is therefore always populated and always means the same thing,
    which is the point: it is what a proforma charges a tenant, and the two
    publishers' conventions should not reach any reader of this frame.
    """
    if sector not in SECTORS:
        raise MarketBeatError(
            f"{sector!r} is not a MarketBeat sector; known: {', '.join(SECTORS)}"
        )
    rows = [
        row
        for line in _table_lines(pdf_bytes)
        if (row := _parse_row(line)) is not None
    ]
    if not rows:
        raise MarketBeatError(
            f"No submarket row was found in the {sector} report. The table on "
            "page two did not parse, which usually means the layout changed."
        )

    frame = pd.DataFrame(rows, columns=["submarket", "first", "second"])
    frame = frame.drop_duplicates(subset="submarket", keep="first")
    if sector == INDUSTRIAL:
        frame["net_rent_psf_cad"] = frame["first"]
        frame["additional_rent_psf_cad"] = frame["second"]
        frame["gross_rent_psf_cad"] = frame["first"] + frame["second"].fillna(0.0)
        frame["premium_rent_psf_cad"] = None
    else:
        frame["net_rent_psf_cad"] = None
        frame["additional_rent_psf_cad"] = None
        frame["gross_rent_psf_cad"] = frame["first"]
        frame["premium_rent_psf_cad"] = frame["second"]
    frame["sector"] = sector
    frame["is_market_total"] = frame["submarket"].map(_is_totals)
    return frame.drop(columns=["first", "second"]).reset_index(drop=True)


def market_total(frame: pd.DataFrame) -> pd.Series:
    """The whole-market row of a parsed report.

    Raises rather than falling back to a mean over the submarkets: those are
    unweighted rows of very different inventories, and averaging them would
    answer a question the publisher already answers properly one line below.
    """
    totals = frame[frame["is_market_total"]]
    if totals.empty:
        raise MarketBeatError(
            "The report has no market-total row; the submarket table parsed "
            f"{len(frame)} row(s) but none of them is {MONTREAL_TOTALS!r}."
        )
    return totals.iloc[0]


def _table_lines(pdf_bytes: bytes) -> list[str]:
    """The report's lines, cut off where the submarket table ends.

    All pages rather than page two: the table has moved between page two and
    page three across quarters, and a page index is one more thing that breaks
    silently when the publisher adds a chart.

    Bounded by the table's own header and footer, and that is not tidiness. The
    KEY SALES TRANSACTIONS block also ends its rows in money - ``Emballages
    Carrousel 239,825 $34,900,000 / $146`` - so a parser reading the whole page
    picks up a $146-per-square-foot *sale price* as a submarket rent, with a
    plausible-looking name attached.

    **The window has to start at the header, not merely end at the footer**,
    because the two sectors order their page differently: the industrial report
    puts the transactions under the table and the office one puts them above
    it. Cutting at the first `_TABLE_END` in the document would therefore throw
    away the whole office table - which is exactly the shape of bug that leaves
    one sector working and the other silently empty.

    Falling back to every line when no header is found is deliberate: a
    publisher that renames the column should cost a parse that the row guards
    then reject loudly, rather than an empty frame that reads as a quiet
    market with no submarkets in it.
    """
    return _window(_extract_lines(pdf_bytes))


def _extract_lines(pdf_bytes: bytes) -> list[str]:
    """Every non-empty line of the PDF, whitespace collapsed.

    The one place pypdf is called, and separate from `_window` below so the
    windowing can be tested against lines that look like a real report without
    a real report having to be built - the seam `tests/unit/test_rents.py`
    patches.
    """
    from pypdf import PdfReader

    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [page.extract_text() or "" for page in reader.pages]
    except Exception as exc:  # pypdf raises its own error types
        raise MarketBeatError(f"not readable as a PDF ({exc})") from exc
    return [
        " ".join(line.split())
        for page in pages
        for line in page.splitlines()
        if line.strip()
    ]


def _window(lines: list[str]) -> list[str]:
    """The slice of ``lines`` between the table's header and its footer."""
    start = next(
        (index for index, line in enumerate(lines) if _TABLE_START.match(line)), None
    )
    if start is None:
        return lines
    end = next(
        (
            index
            for index in range(start + 1, len(lines))
            if _TABLE_END.match(lines[index])
        ),
        len(lines),
    )
    return lines[start:end]


def _parse_row(line: str) -> tuple[str, float, float | None] | None:
    """One submarket row as (name, first rent, second rent), or None.

    Read from the right, for the reason the module docstring gives: the number
    of columns between the name and the rents varies by row, so the rents are
    identified by being the trailing money cells and the name by being the
    leading non-numeric ones.
    """
    tokens = line.split()
    rents: list[float | None] = []
    while tokens and len(rents) < _MAX_RENT_CELLS:
        token = tokens[-1]
        if _MONEY.match(token):
            rents.append(float(token.lstrip("$").replace(",", "")))
        elif token.upper() in {"N/A", "NA", "-"}:
            rents.append(None)
        else:
            break
        tokens.pop()
    rents.reverse()

    # A row states its rents or it is not a row of this table. One is enough -
    # an office submarket whose Class A cell is `N/A` still has an all-classes
    # rent - but the first of them has to be a number.
    if not rents or rents[0] is None:
        return None
    name = []
    for token in tokens:
        if _NUMERIC.match(token):
            break
        name.append(token)
    if not name or len(name) > 6:
        # No name, or a sentence: the footnotes also end in money, and a
        # paragraph is not a submarket.
        return None
    # And the cells between the two: a real row states an inventory, a vacant
    # area and a rate before it states a rent. This is the second guard on the
    # transaction blocks, after `_TABLE_END` - belt and braces, because the one
    # that fails silently costs a wrong rent rather than a missing one.
    if len(tokens) - len(name) < _MIN_DATA_CELLS:
        return None
    second = rents[1] if len(rents) > 1 else None
    return " ".join(name), rents[0], second


def _is_totals(name: str) -> bool:
    """Whether a submarket label is the whole-market row.

    Accent- and case-insensitive: the industrial report writes ``Montréal
    TOTALS`` and the office one ``MONTREAL TOTALS``, and a comparison that
    cared would silently find neither. The office report also carries
    ``CENTRAL TOTAL`` and ``MIDTOWN TOTAL`` sub-totals, which are *not* the
    market and are excluded by requiring the city in the label.
    """
    folded = (
        str(name)
        .upper()
        .replace("É", "E")
        .replace("È", "E")
        .replace("é".upper(), "E")
    )
    return folded.startswith("MONTREAL") and "TOTAL" in folded
