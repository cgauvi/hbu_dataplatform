"""Client for the MEFQ's *codes d'utilisation des biens-fonds* - the roll's codebook.

`urban_rag.role_foncier` reads `rl0105a` off every assessment unit and this
reads what `rl0105a` *says*. They are two publications by one ministry: the
MAMH files the roll as a GeoPackage and numbers its use codes in Annexe 2C.1 of
the *Manuel d'évaluation foncière du Québec*, published on its own as a single
spreadsheet. Nothing in the roll carries the text - a unit states ``4611`` and
the manual is the only thing that says that is a parking garage.

**The sheet is a hierarchy flattened into one column.** `CUBF` holds one, two,
three and four characters on different rows, and the width is the level: ``1``
is the category (*RÉSIDENTIELLE*), ``10`` the rubric, ``100`` the subgroup and
``1000`` the code an assessor actually writes on a unit. Only the four-character
rows are use codes, which is what `use_code_descriptions` selects; the rest are
headings, and merging one onto a unit would put a category name where a use
belongs.

**The category is not always one digit.** ``2-3`` is a single row spanning both
leading digits - manufacturing is numbered 2000 through 3999 - which is why
`CUBF` is read as text rather than as a number, and why this module offers no
"category of a code" at all. `urban_rag.comparables.CUBF_CLASSES` is what maps a
leading digit to an income class, and it is that module's judgement rather than
the manual's.

**No cache, unlike the roll beside it.** A roll year is final - each
municipality files once and the archive is never revised - so
`urban_rag.role_foncier` caches 572 MB by filename forever. This is 185 kB at a
fixed URL with no year in it, reissued whenever the manual is amended (the
workbook carries a `MAJ<year>` change-log sheet per amendment, back to 2010), so
every scrape date fetches it again. The same posture `urban_rag.crspi` and
`urban_rag.estimator` take, and for the same reason.

**The roll archive ships a copy of this file, and it is the same bytes.**
`ROLE2026_GEOPACKAGE.zip` carries `CUBF_MEFQ.xlsx` beside the GeoPackage, and
on the 2026 roll it is byte-identical to what this module downloads - 189,379
bytes, the same SHA-256. So the two possible sources agree today, and reading
the standalone one is chosen for what it costs rather than for what it says:
the codebook is then a 185 kB fetch that can be materialized, re-read and
re-run on its own, instead of something recoverable only by unpacking 2.8 GB.
If they ever diverge, the archive's copy is the one that roll's assessors coded
against, and the difference is worth reading before switching -
`assessment_units` reports `mefq_edition` and names every
`num_use_codes_not_in_the_manual` per run, which is where a divergence would
first show.

Deliberately free of Dagster imports, mirroring `urban_rag.open_data`,
`urban_rag.cmhc` and `urban_rag.crspi`.
"""

from __future__ import annotations

import io
import math
import re
import time
from typing import Mapping

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

#: Where the workbook is served from. The landing page is `SOURCE_URL` below;
#: this is the CDN path it links to, which carries no year and no revision in
#: it - the file at this address is whatever edition is current.
DEFAULT_URL = (
    "https://cdn-contenu.quebec.ca/cdn-contenu/adm/min/affaires-municipales/"
    "publications/evaluation_fonciere/manuel_evaluation_fonciere/CUBF_MEFQ.xlsx"
)

#: The page a reader should be sent to, rather than the CDN path above.
SOURCE_URL = (
    "https://www.quebec.ca/habitation-territoire/information-fonciere/"
    "evaluation-fonciere/manuel/codes-utilisation-biens-fonds"
)

#: The sheet holding the list itself.
LISTE_SHEET = "LISTE NUMÉRIQUE"

#: What the sheet's four columns are called once they leave the workbook's
#: shouting. `cubf` and not `use_code` because this frame is bronze and holds
#: the headings too: only the four-character rows are use codes, and calling
#: the column that on a row reading ``RÉSIDENTIELLE`` would be a lie.
COLUMNS: tuple[str, ...] = ("cubf", "scian", "description", "remarque")

#: Where the header row is found. Located by value rather than by index because
#: the sheet carries a free-text notice above it - "En cas de divergence avec le
#: MEFQ (édition 2025), ce dernier a préséance" - and an edition that adds a
#: second line of it would otherwise shift every column silently. The same
#: posture `urban_rag.cmhc._header_row` takes on the survey workbook.
HEADER_FIRST_CELL = "CUBF"

#: How many characters a use code has. The rows narrower than this are the
#: hierarchy above it - see the module docstring.
USE_CODE_LENGTH = 4

#: What the manual's text is called once it is on a row rather than in the
#: codebook. Named here, in the module that reads the sheet, because it is the
#: one column that travels the whole chain - `silver.assessment_units` looks it
#: up, `lot_assessment_comparables` carries the dominant unit's, and
#: `gold.lot_redevelopment_gap` reports it as the existing use - and a name
#: spelled out at four sites is a name that ends up spelled two ways.
#:
#: Only the description. The codebook also publishes a SCIAN correspondence and
#: the manual's remarks, several of which run to a paragraph of assessment
#: instruction, and those belong beside the codebook rather than repeated onto
#: 437 thousand units. A reader who wants them joins the bronze file on the code.
USE_DESCRIPTION_COLUMN = "use_description"

#: Matches the notice above the header, for the edition it names. Reported as
#: metadata rather than relied on: this file has no version in its URL or its
#: name, so the edition is the only thing that says which manual a description
#: came from, and a re-worded notice should cost that one field rather than the
#: snapshot.
_EDITION = re.compile(r"édition\s+(\d{4})", re.IGNORECASE)

#: Trailing ``.0`` a parquet round trip leaves on a code that shared its column
#: with a null - see `use_code_key`.
_FLOAT_TAIL = re.compile(r"\.0$")


class CubfError(RuntimeError):
    """The workbook could not be fetched, or is not the sheet it should be."""


class CubfFetcher:
    """Downloads the workbook. No cache - see the module docstring."""

    def __init__(
        self,
        *,
        url: str = DEFAULT_URL,
        timeout_seconds: float = 120.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.url = url
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
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["User-Agent"] = USER_AGENT
        return session

    def fetch(self) -> bytes:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(self.url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise CubfError(f"{self.url}: {exc}") from exc
        content = response.content
        if not content.startswith(b"PK"):
            # xlsx is a zip; a moved file or a redirected error page answers
            # 200 with a body that is not one. The same check
            # `urban_rag.cmhc.CmhcFetcher.fetch` makes.
            content_type = response.headers.get("Content-Type", "")
            raise CubfError(
                f"{self.url}: not an xlsx (Content-Type {content_type!r}, "
                f"{len(content)} bytes)"
            )
        return content


def read_liste(workbook: bytes) -> pd.DataFrame:
    """The `LISTE NUMÉRIQUE` sheet, as published.

    Columns: `COLUMNS`, all four as text, plus nothing. The hierarchy rows are
    kept alongside the use codes and the sheet's order is preserved, because
    this is what the asset writes to bronze - `use_code_descriptions` is what
    selects the leaves out of it.

    The footer the ministry signs the sheet with - three lines of directorate
    and a publication date, each a single cell in column A - is dropped, along
    with the notice above the header. Both are recognised by having a `CUBF`
    that is not a code and no description beside it, rather than by their
    wording, which is re-set each edition.
    """
    try:
        raw = pd.read_excel(
            io.BytesIO(workbook),
            sheet_name=LISTE_SHEET,
            header=None,
            dtype=str,
            engine="openpyxl",
        )
    except ValueError as exc:  # pandas names a missing sheet this way
        raise CubfError(
            f"the workbook has no {LISTE_SHEET!r} sheet ({exc})"
        ) from exc
    except Exception as exc:  # openpyxl raises its own error types
        raise CubfError(f"not readable as an xlsx ({exc})") from exc

    header_row = _header_row(raw)
    body = raw.iloc[header_row + 1 :, : len(COLUMNS)]
    body.columns = list(COLUMNS)
    frame = body.map(_clean).reset_index(drop=True)
    # A row with neither a code nor a description says nothing: the sheet's
    # blank separators and the signature block at the bottom are all of that
    # shape, and the notice above the header has been cut already.
    kept = frame[frame["description"].notna() | _is_numbered(frame["cubf"])]
    return kept.reset_index(drop=True)


def sheet_names(workbook: bytes) -> tuple[str, ...]:
    """Every sheet in the workbook, in order.

    `read_liste` reads exactly one of them. The rest are the `MAJ<year>` change
    logs - one per amendment since 2010, each listing the codes that edition
    added, retired or re-worded - and the asset names them in its metadata
    rather than leaving a reader to assume the file held only the list.
    """
    try:
        book = pd.ExcelFile(io.BytesIO(workbook), engine="openpyxl")
    except Exception as exc:  # openpyxl raises its own error types
        raise CubfError(f"not readable as an xlsx ({exc})") from exc
    with book:
        return tuple(book.sheet_names)


def edition_of(workbook: bytes) -> str | None:
    """The manual edition the sheet's own notice names, or None.

    The workbook has no version in its URL, its filename or its cells beyond
    this one line, so it is the only thing that distinguishes two downloads of
    a file that is revised in place. Returns None rather than raising when the
    notice is re-worded: an edition nobody could read is worth a null column,
    not a failed snapshot.
    """
    try:
        head = pd.read_excel(
            io.BytesIO(workbook),
            sheet_name=LISTE_SHEET,
            header=None,
            dtype=str,
            nrows=8,
            engine="openpyxl",
        )
    except Exception:
        return None
    for value in head.to_numpy().ravel():
        if isinstance(value, str) and (found := _EDITION.search(value)):
            return found.group(1)
    return None


def use_code_descriptions(liste: pd.DataFrame) -> dict[str, str]:
    """The four-character codes of ``liste``, mapped to their description.

    This is the whole of what silver merges onto an assessment unit. The
    hierarchy rows are dropped, because a unit states a use and not a heading;
    a code the sheet numbers but leaves undescribed is dropped too, since the
    point of the column is the text - the 2025 edition has exactly one, ``9800``
    (*rubrique temporaire pour nouveaux usages*), which is a slot held open for
    a use not yet named.

    Keyed by `use_code_key`, so the same normalisation stands between this and
    `rl0105a` on both sides of the merge.
    """
    if liste.empty:
        return {}
    codes = liste["cubf"].map(use_code_key)
    keep = codes.notna() & liste["description"].notna()
    return {
        code: description
        for code, description in zip(codes[keep], liste["description"][keep])
    }


def describe(codes: pd.Series, descriptions: Mapping[str, str]) -> pd.Series:
    """``codes`` mapped through ``descriptions``, null where the manual has none.

    A `Series` of the same index and dtype ``object``, so it can be assigned
    straight onto the frame the codes came from. Every code is normalised by
    `use_code_key` first: the roll's own column and the manual's are the same
    four characters, but only after a parquet round trip has been undone on
    each - see that function.
    """
    return codes.map(use_code_key).map(descriptions).astype("object")


def use_code_key(value) -> str | None:
    """``value`` as the four-character string `rl0105a` is printed as, or None.

    The roll's GeoPackage types the column as text and the manual's spreadsheet
    numbers it, so the two arrive as ``'4611'`` and ``4611``. Worse, a frame
    that has been through a parquet round trip with a null in the column can
    hand either side back as a float, and ``str(4611.0)`` is ``'4611.0'`` -
    which matches no code, would find no description, and would look exactly
    like a use the manual has never numbered.

    One normalisation used on both sides of every merge, so the two cannot
    disagree about what a code is. Exactly four digits or None - and **not**
    left-padded, which is the one temptation here worth naming. No CUBF begins
    with a zero: the manual's categories run 1 through 9, and the same column
    holds its hierarchy at one, two and three characters wide. Padding ``100``
    to ``0100`` would turn the *Logement* heading into a use code that reads
    like a real one, and would hand its text to every unit the roll left blank.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        text = _FLOAT_TAIL.sub("", text)
    if len(text) != USE_CODE_LENGTH or not text.isdigit():
        return None
    return text


def _header_row(raw: pd.DataFrame) -> int:
    """Index of the row whose first cell is `HEADER_FIRST_CELL`."""
    first = raw.iloc[:, 0].astype("string").str.strip()
    matches = first[first.str.casefold() == HEADER_FIRST_CELL.casefold()]
    if matches.empty:
        raise CubfError(
            f"the {LISTE_SHEET!r} sheet has no {HEADER_FIRST_CELL!r} header "
            "row; the workbook's layout has changed"
        )
    return int(matches.index[0])


def _is_numbered(cubf: pd.Series) -> pd.Series:
    """Which rows carry a number in `CUBF` rather than prose.

    What separates a described-less code from the block the ministry signs the
    sheet with: ``9800`` is numbered and has no description, and *Direction
    générale de la fiscalité et de l'évaluation foncière* is neither. Read as
    "is it digits" rather than "is it four digits", so the hierarchy rows above
    a code survive into bronze with it.
    """
    text = cubf.astype("string").str.strip()
    return text.str.fullmatch(r"\d+").fillna(False)


def _clean(value) -> str | None:
    """A cell as trimmed text, or None where it holds nothing.

    Whitespace-trimmed on both ends because the sheet's descriptions and
    remarks are hand-entered and several carry trailing spaces or a trailing
    newline; the inner wrapping is left alone, since a remark really is several
    sentences.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None
