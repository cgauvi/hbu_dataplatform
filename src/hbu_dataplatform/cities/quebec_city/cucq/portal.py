"""Where the minutes of the Commission d'urbanisme et de conservation de
Québec are, and how they are listed.

The CUCQ is the body that decides a permit on the city's heritage sectors -
the *secteurs assujettis* of Vieux-Québec, Saint-Roch, Montcalm, Sillery,
the Trait-Carré, the Vieux-Bourg and the rest - and, since the 2023
demolition by-law (R.V.Q. 3117), every demolition of a building in those
sectors. A conseil de quartier is *consulted* about a zoning amendment; the
CUCQ *decides* a request, one address at a time, and files the minutes of
each sitting with the list of what it approved, approved conditionally and
refused. Those minutes are the per-building record the council corpus does
not have.

They are not on the city's own pages. The CUCQ page links "Procès-verbaux"
to the city's decisions portal::

    https://decisions.ville.quebec.qc.ca/index.html?inst=cucq&type=proces-verbaux

which is a single-page front end (AzSearch.js) over an **Azure Cognitive
Search** index. The page ships the index's name, its service and a
*query-only* key to every browser in ``js/scripts.js``, and every search
the page runs is a GET against::

    https://srch-gpd-p.search.windows.net/indexes/stgpdprod01-index/docs

with ``api-key: <that key>``. This module makes the same requests. The key
is the portal's public read credential and the index is read-only to it,
but it is a credential, so it is not written here: `CucqMinutesResource`
takes it, or it is read from ``CUCQ_SEARCH_QUERY_KEY`` - copied off the
portal's ``js/scripts.js`` - and a rotation is a new value there.

What the index holds for a document: ``Numero`` (the file name), ``Date``
(the sitting, ISO), ``Annee``, ``Objet`` ("Procès-verbal de la séance de la
CUCQ tenue le 23 juin 2026"), ``Instance``, ``Type``,
``Uniteadministrative``, ``metadata_storage_name`` and ``content``, the
publisher's own text extraction. **The string facets are stored
percent-encoded** - ``Proc%C3%A8s-verbaux``, ``Commission%20d%27urbanisme
...`` - because the portal decodes them with ``decodeURIComponent`` on
display, so a filter has to spell them that way too (`odata_literal`) and a
listing has to decode them back (`decode_field`).

The PDFs themselves are blobs behind ``gpddocs.ville.quebec.qc.ca/gpdblob/``,
the same host the conseils de quartier trail fetches its sommaires from, and
go through the same URL-keyed cache: a filed minute never changes. Two
series: ``PV_CUCQ_OR_<date>.pdf`` is a regular sitting (weekly, 1,291 of
them from 2000 to 2026 as of 2026-10) and ``PV_CUCQ_DEM_<date>.pdf`` is a
sitting of the *comité de démolition* the Commission has held since
2024-01 for the heritage demolitions the by-law sends to a public hearing.
The minutes before 2013 are scans with no text layer, in the PDF and in
the index alike.

Deliberately free of Dagster imports, like the other publisher clients.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import date
from urllib.parse import quote, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hbu_dataplatform.core.http import USER_AGENT, default_ca_bundle

#: The index the portal searches, and the API version its bundle speaks.
DEFAULT_SEARCH_ENDPOINT = (
    "https://srch-gpd-p.search.windows.net/indexes/stgpdprod01-index/docs"
)
DEFAULT_API_VERSION = "2020-06-30"

#: Where the query key is read from when the resource is given none. The
#: key is the one ``js/scripts.js`` on decisions.ville.quebec.qc.ca hands
#: every browser (``new AzSearch.Automagic({index, queryKey, service})``) -
#: public by construction, see the module docstring - but a credential all
#: the same, so it lives in the environment and not in this file.
QUERY_KEY_ENV = "CUCQ_SEARCH_QUERY_KEY"

#: Where a listed document is served from: ``blobURL`` in the same script.
DEFAULT_BLOB_URL = "https://gpddocs.ville.quebec.qc.ca/gpdblob/"

#: The facet values, as the portal displays them. `odata_literal` spells
#: them the way the index stores them.
INSTANCE_CUCQ = "Commission d'urbanisme et de conservation de Québec"
TYPE_MINUTES = "Procès-verbaux"

#: The index caps ``$top`` at 1000; a longer listing pages with ``$skip``.
PAGE_SIZE = 1000
#: And ``$skip`` at 100,000, which is a hundred pages - far more than the
#: 1,309 minutes there are, so a listing that reaches it is a loop.
MAX_PAGES = 100

#: The two series the file names carry. Anything else is kept as its code.
SERIES_REGULAR = "OR"
SERIES_DEMOLITION_COMMITTEE = "DEM"

_STORAGE_NAME = re.compile(r"^PV_CUCQ_([A-Z]+)_(\d{4}-\d{2}-\d{2})\.pdf$", re.I)

#: What the listing asks the index for. ``content`` is the publisher's own
#: text extraction and is asked for so a PDF that cannot be fetched still
#: has a text; it is the same extraction as the PDF's layer - empty for the
#: scans - so it is a fallback and not a second source.
SELECT_FIELDS: tuple[str, ...] = (
    "metadata_storage_name",
    "Numero",
    "Date",
    "Annee",
    "Objet",
    "Instance",
    "Type",
    "Uniteadministrative",
    "content",
)


class CucqError(RuntimeError):
    """The portal's index could not be read."""


@dataclass(frozen=True)
class MinuteRecord:
    """One sitting's minutes, as the index lists it."""

    storage_name: str
    url: str
    series: str | None
    meeting_date: date | None
    year: int | None
    number: str | None
    objet: str | None
    instance: str | None
    administrative_unit: str | None
    #: The index's own text of the document; "" when it has none.
    index_text: str


def odata_literal(value: str) -> str:
    """``value`` as the index stores a string facet, quoted for ``$filter``.

    The portal filters with ``encodeURIComponent(value).replace(/'/g,
    "%27")`` - so the index holds the facet percent-encoded and the apostrophe
    as ``%27``. `quote` with JavaScript's unreserved set is the same
    encoding, and the ``%27`` means no quote is left to double.
    """
    encoded = quote(value, safe="-_.!~*'()").replace("'", "%27")
    return f"'{encoded}'"


def decode_field(value: str | None) -> str | None:
    """A stored facet decoded back to what the portal displays."""
    if value is None or value == "null":
        return None
    return unquote(value)


def series_of(storage_name: str) -> str | None:
    """``OR`` or ``DEM`` from ``PV_CUCQ_OR_2026-06-23.pdf``; None when the
    name is not of that shape."""
    match = _STORAGE_NAME.match(storage_name.strip())
    return match.group(1).upper() if match else None


def date_of(storage_name: str) -> date | None:
    """The sitting date the file name carries, when it does."""
    match = _STORAGE_NAME.match(storage_name.strip())
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(2))
    except ValueError:
        return None


def minutes_filter(*, since_year: int | None = None) -> str:
    """The ``$filter`` that is the CUCQ's minutes, and nothing else."""
    clauses = [
        f"Instance eq {odata_literal(INSTANCE_CUCQ)}",
        f"Type eq {odata_literal(TYPE_MINUTES)}",
    ]
    if since_year is not None:
        # ``Annee`` is a string facet; four-digit years compare the same way
        # as strings as they do as numbers.
        clauses.append(f"Annee ge '{int(since_year)}'")
    return " and ".join(clauses)


def parse_records(payload: dict, *, blob_url: str = DEFAULT_BLOB_URL) -> list[MinuteRecord]:
    """The documents of one search response, decoded."""
    records: list[MinuteRecord] = []
    for item in payload.get("value", []):
        name = (item.get("metadata_storage_name") or "").strip()
        if not name:
            continue
        when = item.get("Date")
        meeting_date = None
        if when:
            try:
                meeting_date = date.fromisoformat(str(when)[:10])
            except ValueError:
                meeting_date = None
        if meeting_date is None:
            meeting_date = date_of(name)
        year = item.get("Annee")
        records.append(
            MinuteRecord(
                storage_name=name,
                url=blob_url + quote(name, safe=""),
                series=series_of(name),
                meeting_date=meeting_date,
                year=int(year) if year not in (None, "", "null") else None,
                number=decode_field(item.get("Numero")),
                objet=decode_field(item.get("Objet")),
                instance=decode_field(item.get("Instance")),
                administrative_unit=decode_field(item.get("Uniteadministrative")),
                index_text=(item.get("content") or "").strip(),
            )
        )
    return records


class CucqPortalClient:
    """Reads the decisions portal's index the way the portal page does.

    ``session`` is injectable for the tests; the default is paced and
    patient the way every publisher client here is, and verifies TLS
    against the same bundle `PdfFetcher` uses.
    """

    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_SEARCH_ENDPOINT,
        api_version: str = DEFAULT_API_VERSION,
        query_key: str | None = None,
        blob_url: str = DEFAULT_BLOB_URL,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        page_size: int = PAGE_SIZE,
        session: requests.Session | None = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_version = api_version
        self.query_key = query_key if query_key is not None else os.environ.get(QUERY_KEY_ENV)
        self.blob_url = blob_url if blob_url.endswith("/") else blob_url + "/"
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self.page_size = max(1, min(int(page_size), PAGE_SIZE))
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

    def search(self, params: dict) -> dict:
        """One GET against the index; the parsed JSON."""
        if not self.query_key:
            raise CucqError(
                f"no query key for {self.endpoint}: set {QUERY_KEY_ENV} to the "
                "queryKey decisions.ville.quebec.qc.ca/js/scripts.js hands the "
                "browser, or pass query_key to CucqMinutesResource"
            )
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        query = {"api-version": self.api_version, **params}
        try:
            response = self._session.get(
                self.endpoint,
                params=query,
                headers={"api-key": self.query_key, "Accept": "application/json"},
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise CucqError(f"{self.endpoint}: {exc}") from exc
        except ValueError as exc:
            raise CucqError(f"{self.endpoint}: the index answered something other than JSON") from exc
        if "error" in payload:
            raise CucqError(f"{self.endpoint}: {payload['error']}")
        return payload

    def list_minutes(self, *, since_year: int | None = None) -> list[MinuteRecord]:
        """Every CUCQ minute the index lists, newest sitting first."""
        records: list[MinuteRecord] = []
        filter_clause = minutes_filter(since_year=since_year)
        for page in range(MAX_PAGES):
            payload = self.search(
                {
                    "search": "*",
                    "$filter": filter_clause,
                    "$orderby": "Date desc",
                    "$select": ",".join(SELECT_FIELDS),
                    "$top": str(self.page_size),
                    "$skip": str(page * self.page_size),
                    "$count": "true" if page == 0 else "false",
                }
            )
            batch = parse_records(payload, blob_url=self.blob_url)
            records.extend(batch)
            if len(payload.get("value", [])) < self.page_size:
                break
        else:
            raise CucqError(
                f"{self.endpoint}: more than {MAX_PAGES * self.page_size} CUCQ "
                "minutes listed; the filter has stopped meaning what it did"
            )
        if not records:
            raise CucqError(
                f"{self.endpoint}: the index lists no CUCQ minutes for "
                f"{filter_clause!r}; the instance or the type may have been respelled"
            )
        return records

    def minute_url(self, storage_name: str) -> str:
        return self.blob_url + quote(storage_name, safe="")


__all__ = [
    "DEFAULT_API_VERSION",
    "DEFAULT_BLOB_URL",
    "DEFAULT_SEARCH_ENDPOINT",
    "INSTANCE_CUCQ",
    "QUERY_KEY_ENV",
    "SELECT_FIELDS",
    "SERIES_DEMOLITION_COMMITTEE",
    "SERIES_REGULAR",
    "TYPE_MINUTES",
    "CucqError",
    "CucqPortalClient",
    "MinuteRecord",
    "date_of",
    "decode_field",
    "minutes_filter",
    "odata_literal",
    "parse_records",
    "series_of",
]
