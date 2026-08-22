"""Client for CMHC's Rental Market Survey vacancy-rate tables.

CMHC publishes one workbook per survey year, covering every Canadian centre
at once, with no per-borough download to ask for - the same posture as the
BDOI extracts in `urban_rag.bdoi`. It is fetched once and cached on disk keyed
by filename: a published survey year is final, so every scrape date reuses the
copy already there instead of pulling the workbook again.

The `Quartier` sheet is a cross-tab, not a table: five bedroom classes each
occupy a *pair* of columns - the rate, then the letter grading its
reliability - under a single header row. `read_quartier_sheet` unpivots that
into one row per (quartier, dwelling type, bedroom class), which is the shape
the asset averages over.

Deliberately free of Dagster imports, mirroring `urban_rag.open_data`,
`urban_rag.infolot` and `urban_rag.bdoi`.
"""

from __future__ import annotations

import os
import re
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

#: Where the workbooks the landing page links to actually live. The page
#: itself is `cmhc_assets.SOURCE_URL`; these are on the asset CDN, which
#: serves them under a flat slug with no directory listing.
DEFAULT_BASE_URL = (
    "https://assets.cmhc-schl.gc.ca/sites/cmhc/professional/"
    "housing-markets-data-and-research/housing-data-tables/rental-market/"
    "urban-rental-market-survey-data-vacancy-rates"
)

#: Latest survey year published under the French slug. The workbook is
#: reissued once a year; bump this (and check the sheet still parses) rather
#: than guessing forward - an unpublished year answers 404, not an empty file.
DEFAULT_SURVEY_YEAR = 2023

#: Overrides `DEFAULT_SURVEY_YEAR` when set. An environment variable rather
#: than config-only because `--config-json` replaces a resource's config
#: wholesale: setting `survey_year` that way means restating `cache_dir`,
#: which the code location is what knows.
SURVEY_YEAR_VAR = "URBAN_RAG_CMHC_SURVEY_YEAR"

#: The sheet holding the neighborhood breakdown. The workbook also carries
#: `SDR` (census subdivisions) and `SR` (survey zones), which are the same
#: survey aggregated coarser and are not read here.
QUARTIER_SHEET = "Quartier"

#: Where the header row is found, and what the leading columns hold. Located
#: by value rather than by index so an added title line above it does not
#: silently shift every column.
HEADER_FIRST_CELL = "Province"
LABEL_COLUMNS = ("province", "centre", "zone", "quartier", "dwelling_label")

#: First column of the (rate, reliability) pairs, and how wide a pair is.
FIRST_VALUE_COLUMN = len(LABEL_COLUMNS)
PAIR_WIDTH = 2

#: Bedroom class -> the key written to parquet, by the sheet's own header
#: label (whitespace-collapsed: the workbook wraps two of them mid-cell).
#: Keyed to stable snake_case rather than carried through in French because
#: the same survey is published in an English workbook whose headers read
#: "Bachelor"/"1 Bedroom", and a downstream filter should not have to care
#: which of the two was scraped.
BEDROOM_TYPES: dict[str, str] = {
    "Studios": "studio",
    "1 chambre": "1_bedroom",
    "2 chambres": "2_bedroom",
    "3 chambres +": "3_bedroom_plus",
    "Tous les log.": "all",
}

#: Structure type -> parquet key, from the `Type de logement` column. Same
#: reasoning as `BEDROOM_TYPES`.
DWELLING_TYPES: dict[str, str] = {
    "En bande": "row",
    "App. & autres": "apartment_other",
    "Total": "all",
}

#: The two reasons a cell holds no rate. They mean different things and are
#: kept apart in the `status` column: `--` is a structural zero (the class
#: does not exist in that quartier), `**` is a rate CMHC measured but will not
#: publish. Neither is an average-able zero.
SUPPRESSED = "**"
NO_UNITS = "--"

STATUS_PUBLISHED = "published"
STATUS_SUPPRESSED = "suppressed"
STATUS_NO_UNITS = "no_units"

#: Value of the `Quartier` column on the rows that total a zone rather than
#: describe one neighborhood. Dropped: they would double-count an average.
TOTAL_LABEL = "Total"

#: Some publications print both languages in one cell, English first:
#: ``South West ~ Sud-Ouest``. Only the French workbook is read here, so the
#: half after the separator is the name it means.
BILINGUAL_SEPARATOR = " ~ "


class CmhcError(RuntimeError):
    """The workbook could not be fetched, or is not shaped as expected."""


def normalize_quartier(name: str) -> str:
    """Match key for a quartier name, insensitive to how CMHC spells it.

    The survey renames nothing between 2022 and 2023, but it does *respell*:
    ``Sud-Ouest`` is published as ``South West ~ Sud-Ouest`` in one year and
    bare in the next, and Pierrefonds' quartier swaps a slash for a hyphen.
    Punctuation, case and accents are therefore collapsed before matching
    against `urban_rag.partitions.CMHC_QUARTIERS`, so the crosswalk holds one
    canonical name per quartier instead of one per publication.

    Deliberately *not* fuzzy: two names that differ by a letter still differ,
    and `read_quartier_sheet` refuses a sheet where two quartiers collapse to
    the same key.
    """
    stripped = unicodedata.normalize("NFKD", strip_bilingual(name))
    without_accents = "".join(c for c in stripped if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", without_accents.casefold()).strip()


def strip_bilingual(name: str) -> str:
    """The French half of a ``English ~ French`` label, or the name as given."""
    return name.rsplit(BILINGUAL_SEPARATOR, 1)[-1].strip()


def default_survey_year() -> int:
    """`SURVEY_YEAR_VAR` if it holds a year, `DEFAULT_SURVEY_YEAR` otherwise.

    Read per instantiation rather than at import, so a `.env` loaded later - or
    a variable set for one run - still reaches the resource.
    """
    raw = os.environ.get(SURVEY_YEAR_VAR)
    if raw is None or not raw.strip():
        return DEFAULT_SURVEY_YEAR
    try:
        return int(raw)
    except ValueError:
        raise CmhcError(f"{SURVEY_YEAR_VAR}={raw!r} is not a year") from None


def filename_for(survey_year: int) -> str:
    """Name of the French workbook for a survey year."""
    return f"urban-rental-market-survey-data-vacancy-rates-{survey_year}-fr.xlsx"


class CmhcFetcher:
    """Downloads the survey workbook, with an on-disk cache keyed by name."""

    def __init__(
        self,
        *,
        cache_dir: Path | str,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 120.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.base_url = base_url.rstrip("/")
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

    def cache_path(self, survey_year: int) -> Path:
        return self.cache_dir / filename_for(survey_year)

    def url_for(self, survey_year: int) -> str:
        return f"{self.base_url}/{filename_for(survey_year)}"

    def fetch(self, survey_year: int = DEFAULT_SURVEY_YEAR) -> Path:
        """Download the workbook (or reuse the cache), returning its local path.

        The download URL the site hands out carries a `rev=` cache-buster and
        a Google Analytics `_gl` blob; neither is required to reach the file,
        so the plain slug is requested instead of pinning one publication's
        query string.
        """
        cached = self.cache_path(survey_year)
        if cached.exists() and cached.stat().st_size:
            return cached

        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        url = self.url_for(survey_year)
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise CmhcError(f"{url}: {exc}") from exc

        content = response.content
        if not content.startswith(b"PK"):
            # xlsx is a zip; a dead year or a redirected error page answers
            # 200 with a body that is not one.
            content_type = response.headers.get("Content-Type", "")
            raise CmhcError(
                f"{url}: not an xlsx (Content-Type {content_type!r}, "
                f"{len(content)} bytes)"
            )

        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(content)
        return cached


def read_quartier_sheet(path: Path | str) -> pd.DataFrame:
    """The `Quartier` sheet, unpivoted to one row per bedroom class.

    Columns: the four geography labels the sheet carries verbatim
    (`province`, `centre`, `zone`, `quartier`), plus `dwelling_type`,
    `bedroom_type`, `vacancy_rate_pct`, `reliability` and `status`.

    ``vacancy_rate_pct`` is in *percent*, as published (`0.2%` -> ``0.2``),
    and is null wherever `status` is not ``published``.
    """
    try:
        raw = pd.read_excel(
            path, sheet_name=QUARTIER_SHEET, header=None, dtype=str, engine="openpyxl"
        )
    except Exception as exc:  # openpyxl raises its own error types
        raise CmhcError(f"{path}: not readable as an xlsx ({exc})") from exc

    header_row = _header_row(raw, path)
    bedrooms = _bedroom_columns(raw.iloc[header_row], path)

    body = raw.iloc[header_row + 1 :]
    # Everything below the table - the copyright line, the source, the legend
    # for `a`..`d`/`**`/`--` - carries a single cell in column A and nothing in
    # the geography columns, so requiring all four drops the footer without
    # having to recognise its wording.
    labelled = body[body.iloc[:, : len(LABEL_COLUMNS)].notna().all(axis=1)]

    records: list[dict] = []
    for row in labelled.itertuples(index=False, name=None):
        labels = dict(zip(LABEL_COLUMNS, (_clean(value) for value in row)))
        labels["quartier"] = strip_bilingual(labels["quartier"])
        dwelling_label = labels.pop("dwelling_label")
        dwelling_type = DWELLING_TYPES.get(dwelling_label)
        if dwelling_type is None:
            raise CmhcError(
                f"{path}: unknown 'Type de logement' {dwelling_label!r}; "
                f"known: {', '.join(sorted(DWELLING_TYPES))}"
            )
        for bedroom_type, column in bedrooms.items():
            rate, status = _parse_rate(row[column])
            records.append(
                labels
                | {
                    "dwelling_type": dwelling_type,
                    "bedroom_type": bedroom_type,
                    "vacancy_rate_pct": rate,
                    "reliability": _parse_reliability(row[column + 1]),
                    "status": status,
                }
            )

    if not records:
        raise CmhcError(f"{path}: the {QUARTIER_SHEET!r} sheet has no data rows")
    frame = pd.DataFrame.from_records(records)
    frame["vacancy_rate_pct"] = frame["vacancy_rate_pct"].astype("Float64")
    _reject_colliding_quartiers(frame, path)
    return frame


def _reject_colliding_quartiers(frame: pd.DataFrame, path: Path | str) -> None:
    """Refuse a sheet where `normalize_quartier` merges two distinct names.

    The normalization is what lets one crosswalk serve several publications;
    a collision would silently average two neighborhoods together, so it is an
    error rather than something to resolve by guessing.
    """
    names = frame[["centre", "quartier"]].drop_duplicates()
    names = names.assign(key=names["quartier"].map(normalize_quartier))
    collisions = names[names.duplicated(["centre", "key"], keep=False)]
    if not collisions.empty:
        merged = sorted(set(collisions["quartier"]))
        raise CmhcError(
            f"{path}: quartier names that differ only in punctuation or "
            f"accents: {', '.join(merged)}"
        )


def survey_period(path: Path | str) -> str | None:
    """The reference month printed above the table, e.g. ``octobre 2023``.

    Provenance only: it says which month the rates describe, which the file
    name (a *publication* year) does not. ``None`` if the workbook stops
    printing it, since nothing downstream is keyed on it.
    """
    try:
        head = pd.read_excel(
            path,
            sheet_name=QUARTIER_SHEET,
            header=None,
            dtype=str,
            engine="openpyxl",
            nrows=6,
            usecols=[0],
        )
    except Exception as exc:
        raise CmhcError(f"{path}: not readable as an xlsx ({exc})") from exc

    for value in head.iloc[:, 0]:
        text = _clean(value)
        # The title line above it is a full sentence; the period is a bare
        # "<month> <year>", which is the only line matching this.
        if re.fullmatch(r"[^\W\d_]+\s+\d{4}", text, flags=re.UNICODE):
            return text
    return None


# -- parsing helpers --------------------------------------------------------


def _clean(value) -> str:
    """Collapse a cell's whitespace; the workbook wraps labels mid-cell."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _header_row(raw: pd.DataFrame, path: Path | str) -> int:
    for index in range(len(raw)):
        if _clean(raw.iat[index, 0]) == HEADER_FIRST_CELL:
            return index
    raise CmhcError(
        f"{path}: no header row starting with {HEADER_FIRST_CELL!r} in "
        f"the {QUARTIER_SHEET!r} sheet"
    )


def _bedroom_columns(header: pd.Series, path: Path | str) -> dict[str, int]:
    """Bedroom key -> index of its *rate* column; the grade sits one right.

    Read off the header rather than assumed, so a workbook that adds or drops
    a bedroom class fails here instead of silently shifting every rate one
    column over.
    """
    found: dict[str, int] = {}
    for column in range(FIRST_VALUE_COLUMN, len(header), PAIR_WIDTH):
        label = _clean(header.iat[column])
        if not label:
            continue
        bedroom_type = BEDROOM_TYPES.get(label)
        if bedroom_type is None:
            raise CmhcError(
                f"{path}: unknown bedroom column {label!r}; "
                f"known: {', '.join(BEDROOM_TYPES)}"
            )
        found[bedroom_type] = column

    missing = [key for key in BEDROOM_TYPES.values() if key not in found]
    if missing:
        raise CmhcError(f"{path}: bedroom column(s) missing: {', '.join(missing)}")
    return found


def _parse_rate(value) -> tuple[float | None, str]:
    """A rate cell -> (percent, status). Published rates read like ``1.6%``."""
    text = _clean(value)
    if text == NO_UNITS:
        return None, STATUS_NO_UNITS
    if text == SUPPRESSED or not text:
        return None, STATUS_SUPPRESSED
    try:
        return float(text.rstrip("%").replace(",", ".")), STATUS_PUBLISHED
    except ValueError:
        raise CmhcError(f"Unparseable vacancy rate {text!r}") from None


def _parse_reliability(value) -> str | None:
    """The letter grading a rate: ``a``..``d``, or None where there is none.

    Cells with no rate carry a placeholder - an apostrophe on most rows, an
    empty string on some - rather than being left blank.
    """
    text = _clean(value)
    return text if text in ("a", "b", "c", "d") else None
