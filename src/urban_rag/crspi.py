"""Client for Statistics Canada's Commercial Rents Services Price Index.

Table `18-10-0260-01`, quarterly, published as one zipped CSV at a stable URL
with no key and no auth. It is the only free, structured, Montreal-specific
commercial rent series there is, and it does two things nothing else here can:

**It covers retail, which no free survey of the *level* does.** Cushman &
Wakefield publish a Montreal office MarketBeat and a Montreal industrial one
and no retail one at all - see `urban_rag.marketbeat` - so the retail rent this
platform needs has to come from a stated base carried forward. This is what
carries it.

**It is quarterly and never behind.** A MarketBeat lands weeks after the
quarter it describes, and a scrape date in between has to price something. The
index is what ages the last published level to the quarter being scraped
rather than pretending the two are the same date.

**It is an index, not a level, and the difference is the whole design.** Every
series is `2019=100` *for that series*, so `Retail / Office` at a given quarter
is how retail has moved **relative to** office since 2019 - not the ratio of
their rents. Multiplying an office rent by that ratio to get a retail rent
would silently assume the two were equal in 2019, which they were not, and
would produce a confident number that is wrong by whatever the 2019 gap was.
So the index is only ever used to move *one* series through time
(`escalate`), and never to convert one series into another. `escalate` refuses
to be handed two different building types for exactly that reason.

The table is small - 14 kB zipped, about 1,850 rows - and is revised, so it is
fetched per scrape date rather than cached by name. That is the posture
`urban_rag.estimator` takes toward the cost guide and the opposite of the one
`urban_rag.role_foncier` takes toward a published roll year, and the reason is
the same in both: what can be revised is re-read.

Deliberately free of Dagster imports, mirroring `urban_rag.marketbeat` and
`urban_rag.estimator`.
"""

from __future__ import annotations

import csv
import io
import time
import zipfile

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

#: Commercial rents services price index, quarterly.
DEFAULT_TABLE_ID = "18100260"
DEFAULT_BASE_URL = "https://www150.statcan.gc.ca/n1/tbl/csv"

#: The `GEO` value for the Montreal CMA, matched on a prefix because the
#: published label carries an accent (`Montréal, Quebec`) and a comparison that
#: spelled it out would be one mojibake away from finding nothing. The table
#: also carries `Quebec` (the province) and `Québec, Quebec` (the city), and
#: neither is this.
MONTREAL_GEO_PREFIX = "Montréal,"

#: How the publisher's `Building Type` maps onto the income classes
#: `urban_rag.comparables` prices floor area under. `total` is carried but
#: never priced against: it is the blend of the other three and exists as a
#: cross-check on a partition where one series is suppressed.
BUILDING_TYPES: dict[str, str] = {
    "Office buildings": "office",
    "Retail buildings": "retail",
    "Industrial buildings and warehouses": "industrial",
    "Total, building type": "total",
}

#: The three that are priced. `total` is deliberately absent - see above.
PRICED_TYPES: tuple[str, ...] = ("office", "retail", "industrial")

#: The index's own base period, carried onto every row so a reader knows what
#: 100 means without going back to the publisher.
BASE_PERIOD = "2019=100"


class CrspiError(RuntimeError):
    """The table could not be fetched, unzipped, or read as the CRSPI."""


class CrspiFetcher:
    """Downloads the zipped table. No cache - see the module docstring."""

    def __init__(
        self,
        *,
        table_id: str = DEFAULT_TABLE_ID,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 120.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.table_id = table_id
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

    @property
    def url(self) -> str:
        return f"{self.base_url}/{self.table_id}-eng.zip"

    def fetch(self) -> bytes:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(self.url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise CrspiError(f"{self.url}: {exc}") from exc
        if not response.content.startswith(b"PK"):
            raise CrspiError(
                f"{self.url}: answered {len(response.content)} bytes that are "
                "not a zip"
            )
        return response.content


def read_montreal(archive: bytes) -> pd.DataFrame:
    """The Montreal CMA rows, one per (quarter, building type).

    Returns ``period`` (``2026-04``, the month the quarter starts in, as the
    publisher writes it), ``building_type`` (this platform's name for it) and
    ``index_value``. Sorted by period so `latest_period` and `escalate` can
    both assume it.

    Rows the publisher suppressed come back with an empty `VALUE` and are
    dropped: an index with no value is not an index of zero, and one reaching
    `escalate` would zero out a rent.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
            names = [
                name
                for name in bundle.namelist()
                if name.lower().endswith(".csv")
                and "metadata" not in name.lower()
            ]
            if not names:
                raise CrspiError(
                    f"the archive holds no data CSV (members: {bundle.namelist()})"
                )
            with bundle.open(names[0]) as handle:
                rows = list(csv.DictReader(io.TextIOWrapper(handle, "utf-8-sig")))
    except zipfile.BadZipFile as exc:
        raise CrspiError(f"not a readable zip ({exc})") from exc

    if not rows:
        raise CrspiError("the table is empty")
    for column in ("REF_DATE", "GEO", "Building Type", "VALUE"):
        if column not in rows[0]:
            raise CrspiError(
                f"the table has no {column!r} column; it published "
                f"{', '.join(rows[0])}"
            )

    records = []
    for row in rows:
        if not _is_montreal(row["GEO"]):
            continue
        building_type = BUILDING_TYPES.get(row["Building Type"].strip())
        if building_type is None:
            continue
        value = (row.get("VALUE") or "").strip()
        if not value:
            continue
        try:
            records.append(
                {
                    "period": row["REF_DATE"].strip(),
                    "building_type": building_type,
                    "index_value": float(value),
                }
            )
        except ValueError:
            continue

    if not records:
        published = sorted({row["GEO"] for row in rows})
        raise CrspiError(
            "the table carries no Montreal CMA row. It published these "
            f"geographies: {', '.join(published)}"
        )
    frame = pd.DataFrame.from_records(records)
    frame["base_period"] = BASE_PERIOD
    return frame.sort_values(["building_type", "period"]).reset_index(drop=True)


def latest_period(frame: pd.DataFrame, building_type: str) -> str:
    """The most recent quarter one series was published for."""
    series = frame[frame["building_type"] == building_type]
    if series.empty:
        raise CrspiError(
            f"no {building_type!r} series for Montreal; the table carried "
            f"{sorted(frame['building_type'].unique())}"
        )
    return str(series["period"].iloc[-1])


def index_at(frame: pd.DataFrame, building_type: str, period: str) -> float | None:
    """One series' index value at a quarter, or None if it is not published.

    ``period`` is matched on the publisher's own ``YYYY-MM`` spelling, so a
    caller holding a MarketBeat quarter passes ``report.period_start[:7]``.
    """
    match = frame[
        (frame["building_type"] == building_type) & (frame["period"] == str(period))
    ]
    return None if match.empty else float(match["index_value"].iloc[0])


def escalate(
    level: float,
    frame: pd.DataFrame,
    *,
    building_type: str,
    from_period: str,
    to_period: str | None = None,
) -> tuple[float, str, str]:
    """Carry one measured rent from the quarter it was measured to another.

    Returns ``(escalated_level, to_period, basis)``, where ``basis`` is
    ``"measured"`` when the two quarters are the same and ``"escalated"``
    otherwise - so a row can always say whether its rent is a survey figure or
    a survey figure moved.

    **One building type, both ends.** The ratio taken here is
    ``I(to) / I(from)`` *within a single series*, which is exactly what a price
    index means. Taking it across two series would be the mistake the module
    docstring describes, and the signature is what makes that mistake
    unavailable: there is one ``building_type``, not two.

    A quarter the index does not cover - one newer than the last publication,
    or a `from_period` the series does not reach back to - returns the level
    unmoved and a basis of ``"unescalated"`` rather than raising. A rent that
    is a quarter stale is a far better answer than no rent at all, and the
    basis column is what keeps that visible instead of silent.
    """
    target = to_period or latest_period(frame, building_type)
    start = index_at(frame, building_type, from_period)
    end = index_at(frame, building_type, target)
    if start is None or end is None or start <= 0:
        return float(level), str(from_period), "unescalated"
    if str(from_period) == str(target):
        return float(level), str(target), "measured"
    return float(level) * end / start, str(target), "escalated"


def _is_montreal(geo: str) -> bool:
    """Whether a `GEO` label is the Montreal CMA.

    Prefix-matched on the accented spelling the table publishes, with an
    unaccented fallback: the CSV is UTF-8 and pandas reads it correctly, but a
    re-encoded copy is one of the likelier things to go wrong between the
    publisher and here, and the province (`Quebec`) and the city of Québec are
    both close enough to be caught by a looser test.
    """
    label = str(geo).strip()
    return label.startswith(MONTREAL_GEO_PREFIX) or label.startswith("Montreal,")
