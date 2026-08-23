"""Client for donnees.montreal.ca, the city's CKAN open-data portal.

Two calls cover everything the pipeline needs: ``package_show`` for a
dataset's resource list, and a plain GET on a resource's ``url`` for the file
itself.

Resources are looked up by the *filename* their download URL ends in, not by
resource id or title. CKAN mints a new resource id whenever the city replaces
a file, and the French titles get re-worded between publications, but the
filename has been stable on these datasets since 2016.

Deliberately free of Dagster imports so it can be exercised from a plain
script or a unit test with a stubbed session.
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

DEFAULT_BASE_URL = "https://donnees.montreal.ca"

#: Encodings the portal's CSVs are published in, in the order tried. The
#: BOM-aware variant comes first: Excel-exported files carry one, and reading
#: those as plain UTF-8 leaves ``﻿`` glued to the first column name.
CSV_ENCODINGS = ("utf-8-sig", "cp1252")


class OpenDataError(RuntimeError):
    """The portal answered, but with an error rather than the file asked for."""


@dataclass(frozen=True)
class Resource:
    """One downloadable file attached to a dataset."""

    resource_id: str
    name: str
    format: str
    url: str
    last_modified: str | None = None

    @property
    def filename(self) -> str:
        """Last path segment of the download URL, percent-decoded."""
        return unquote(urlparse(self.url).path.rsplit("/", 1)[-1])


@dataclass(frozen=True)
class Package:
    """A dataset's metadata: what it is licensed as, and what it publishes."""

    dataset: str
    title: str
    license_title: str | None
    resources: tuple[Resource, ...]

    def resource(self, filename: str) -> Resource:
        """The resource whose download URL ends in ``filename``."""
        for resource in self.resources:
            if resource.filename == filename:
                return resource
        published = ", ".join(sorted(r.filename for r in self.resources))
        raise OpenDataError(
            f"Dataset {self.dataset!r} publishes no {filename!r}; it has: {published}"
        )


class CkanClient:
    """Thin wrapper over the portal's CKAN API and its download URLs."""

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
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["User-Agent"] = USER_AGENT
        return session

    def package(self, dataset: str) -> Package:
        """Resource list for a dataset, by its slug (``quartiers``) or id."""
        payload = self._get_json(
            f"{self.base_url}/api/3/action/package_show", params={"id": dataset}
        )
        # CKAN reports failure in the body with HTTP 200 as often as not.
        if not payload.get("success"):
            error = payload.get("error", {})
            raise OpenDataError(
                f"package_show({dataset!r}) failed: "
                f"{error.get('message') or error or 'no reason given'}"
            )

        result = payload.get("result") or {}
        return Package(
            dataset=result.get("name") or dataset,
            title=result.get("title") or "",
            license_title=result.get("license_title"),
            resources=tuple(
                Resource(
                    resource_id=item.get("id", ""),
                    name=item.get("name", ""),
                    format=item.get("format", ""),
                    url=item.get("url", ""),
                    last_modified=item.get("last_modified") or item.get("created"),
                )
                for item in result.get("resources", [])
            ),
        )

    def download(self, resource: Resource) -> bytes:
        """The resource's bytes, straight from its download URL."""
        response = self._get(resource.url)
        content_type = response.headers.get("Content-Type", "")
        if content_type.startswith("text/html"):
            # The portal serves its error pages with HTTP 200; none of the
            # formats fetched here are HTML, so this can only be one of those.
            raise OpenDataError(
                f"{resource.filename}: got an HTML page, not the file "
                f"({resource.url})"
            )
        return response.content

    # -- transport ---------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> requests.Response:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(
                url, params=params, timeout=self.timeout_seconds
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise OpenDataError(f"GET {url} failed: {exc}") from exc
        return response

    def _get_json(self, url: str, params: dict | None = None) -> dict:
        response = self._get(url, params)
        try:
            return response.json()
        except ValueError as exc:
            raise OpenDataError(f"GET {url} returned no JSON: {exc}") from exc


def decode_csv(content: bytes, *, filename: str = "csv") -> pd.DataFrame:
    """Parse a portal CSV without assuming its encoding or its separator.

    Every column comes back as text on purpose: the identifiers are
    zero-padded (``no_qr`` runs ``01``..``91``), and letting pandas infer would
    turn them into integers that no longer join against the geographic layer.
    Callers cast the columns they know to be numeric.
    """
    for encoding in CSV_ENCODINGS:
        try:
            text = content.decode(encoding)
        except UnicodeDecodeError:
            continue
        # sep=None sniffs `,` vs `;`; the portal publishes both, and has
        # switched between them across re-uploads of the same file.
        return pd.read_csv(io.StringIO(text), sep=None, engine="python", dtype=str)
    raise OpenDataError(
        f"{filename}: not decodable as any of {', '.join(CSV_ENCODINGS)}"
    )
