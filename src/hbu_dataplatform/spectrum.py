"""Client for Montreal's Spectrum Spatial Feature Service.

The service is a Precisely Spectrum Spatial ``FeatureService`` reached through
Spectrum Analyst's ``connectProxy``. Two quirks of that proxy shape every call
made here:

1. A ``url`` query parameter must be present, even when empty, or the proxy
   answers ``HTTP 500 - Missing url parameter in the clientRequest``.
2. The proxy forwards *only* that parameter: it appends the value to the
   upstream path and drops every other query parameter. Any real query string
   (``q``, ``page``, ``pageLength``) therefore has to be embedded inside the
   ``url`` value itself.

So there are two request shapes:

* parameterless endpoints keep their normal path and pass ``url=``
  (``/tables.json?url=``, ``/tables/<%2F-encoded name>/metadata.json?url=``);
* anything needing a query string stops the path at ``/tables`` and carries the
  rest inside ``url`` (``/tables?url=features.json?q=...&page=1``).

A table name percent-encoded *inside* ``url`` gets double-encoded and rejected,
which is why metadata uses the path shape while searches keep raw slashes
inside the SQL string.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_BASE_URL = (
    "https://spectrum.montreal.ca/connect/analyst/controller/connectProxy"
    "/rest/Spatial/FeatureService"
)

#: Sent by every client in the project. Not decoration: donnees.montreal.ca
#: answers 403 to the `python-requests/x.y` default that `requests` would
#: otherwise send, and naming the caller is the polite thing to do on three
#: public servers that owe this pipeline nothing.
USER_AGENT = "urban-rag/0.1.0 (Dagster pipeline)"

#: Geometry is stored in each table's native CRS (usually epsg:42104, an
#: MTM-zone-8 variant). MI_Transform is the only way to get lon/lat out, as the
#: service exposes no output-SRS parameter.
WGS84 = "epsg:4326"

#: Spectrum returns MapInfo rendering instructions in this column. It is a
#: nested object carrying no analytical value, so it is dropped before writing.
STYLE_COLUMN = "MI_Style"


class SpectrumError(RuntimeError):
    """The service answered, but with an error rather than features."""


class SpectrumAccessDenied(SpectrumError):
    """The named table exists but is not readable by anonymous callers."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class TableMetadata:
    """Column layout of one named table."""

    table: str
    columns: tuple[Column, ...]
    geometry_column: str | None
    native_crs: str | None

    @property
    def attribute_columns(self) -> tuple[str, ...]:
        """Columns that survive into parquet (no geometry, no style)."""
        return tuple(c.name for c in self.columns if c.type not in ("Geometry", "Style"))


class SpectrumClient:
    """Thin, dependency-light wrapper over the Feature Service REST API.

    Deliberately free of Dagster imports so it can be exercised from a plain
    script or a unit test with a stubbed session.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
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
            status_forcelist=(429, 502, 503, 504),
            allowed_methods=("GET",),
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["Accept"] = "application/json"
        session.headers["User-Agent"] = USER_AGENT
        return session

    # -- transport ---------------------------------------------------------

    def _get(self, path: str, url_param: str) -> dict:
        # Be a polite guest: this is Montreal's live Analyst server, not an
        # open-data mirror.
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)

        response = self._session.get(
            f"{self.base_url}/{path}" if path else self.base_url,
            params={"url": url_param},
            timeout=self.timeout_seconds,
        )
        if "json" not in response.headers.get("Content-Type", ""):
            # The proxy renders Tomcat error pages as HTML.
            raise SpectrumError(
                f"Non-JSON response ({response.status_code}) for {path!r} "
                f"url={url_param!r}: {_excerpt(response.text)}"
            )

        payload = response.json()
        _raise_for_service_exception(payload, path, url_param)
        return payload

    def _search(self, sql: str, *, page: int | None, page_length: int) -> dict:
        # `pageLength` is only honoured when `page` is also supplied; without
        # both, the service returns the entire result set in one response.
        inner = f"features.json?q={sql}"
        if page is not None:
            inner += f"&page={page}&pageLength={page_length}"
        return self._get("tables", inner)

    # -- api ---------------------------------------------------------------

    def list_tables(self, namespace: str | None = None) -> list[str]:
        """All named tables, optionally limited to one top-level namespace.

        Namespaces are the first path segment (``19_VSMPE``, ``18_VM``, ...).
        """
        tables: list[str] = self._get("tables.json", "")["Tables"]
        if namespace is None:
            return tables
        prefix = f"/{namespace.strip('/')}/"
        return [t for t in tables if t.startswith(prefix)]

    def table_metadata(self, table: str) -> TableMetadata:
        encoded = table.replace("/", "%2F")
        entries = self._get(f"tables/{encoded}/metadata.json", "")["Metadata"]
        columns = tuple(
            Column(name=entry["name"], type=entry["type"]) for entry in entries
        )
        geometry = next((c for c in columns if c.type == "Geometry"), None)
        native_crs = None
        if geometry is not None:
            for entry in entries:
                if entry["name"] == geometry.name:
                    crs = entry.get("crs") or {}
                    native_crs = crs.get("properties", {}).get("name")
        return TableMetadata(
            table=table,
            columns=columns,
            geometry_column=geometry.name if geometry else None,
            native_crs=native_crs,
        )

    def count(self, table: str) -> int:
        """Row count. The dedicated ``count.json`` endpoint 500s, so use SQL."""
        payload = self._search(
            f'select count(*) from "{table}"', page=None, page_length=1
        )
        features = payload.get("features") or []
        if not features:
            return 0
        return int(features[0]["properties"]["Count__"])

    def build_select(self, metadata: TableMetadata, target_srs: str = WGS84) -> str:
        """SQL for one table, reprojecting geometry when the table has any.

        ``select MI_Transform(<geom>,...), *`` keeps the attribute list open
        ended - safer than naming every column, since the service rejects the
        whole query if one identifier is unknown - and the reprojected geometry
        is the one that lands in the GeoJSON ``geometry`` member.
        """
        projection = "*"
        if metadata.geometry_column:
            projection = f"MI_Transform({metadata.geometry_column},'{target_srs}'), *"
        return f'select {projection} from "{metadata.table}"'

    def fetch_features(
        self,
        metadata: TableMetadata,
        *,
        page_length: int = 500,
        target_srs: str = WGS84,
        max_pages: int = 1000,
    ) -> Iterator[dict]:
        """Yield GeoJSON features for a table, one page at a time."""
        sql = self.build_select(metadata, target_srs)
        for page in range(1, max_pages + 1):
            payload = self._search(sql, page=page, page_length=page_length)
            features = payload.get("features") or []
            yield from features
            if len(features) < page_length:
                return
        raise SpectrumError(
            f"{metadata.table}: stopped after {max_pages} pages of {page_length}; "
            "raise max_pages if the table is really that large"
        )


def default_ca_bundle() -> str | None:
    """CA bundle to trust, for laptops behind a TLS-inspecting proxy.

    ``requests`` verifies against ``certifi`` and reads only
    ``REQUESTS_CA_BUNDLE``/``CURL_CA_BUNDLE``, while managed machines usually
    advertise their corporate root through ``SSL_CERT_FILE`` instead. Without
    this, every call fails with a certificate-verify error.
    """
    for variable in ("REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"):
        value = os.environ.get(variable)
        if value and Path(value).exists():
            return value
    return None


def _raise_for_service_exception(payload: object, path: str, url_param: str) -> None:
    if not isinstance(payload, dict):
        return
    message = payload.get("message")
    is_exception = "ServiceException" in str(payload.get("type", ""))
    # Some failures come back as a bare {"value": "..."} envelope instead.
    is_bare_error = "features" not in payload and "Tables" not in payload
    if is_exception or (message and is_bare_error):
        text = f"{path} url={url_param!r}: {message or payload}"
        if "Access denied" in str(message):
            raise SpectrumAccessDenied(text)
        raise SpectrumError(text)


def _excerpt(text: str, limit: int = 300) -> str:
    return " ".join(text.split())[:limit]
