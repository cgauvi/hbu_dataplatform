"""Client for StatCan's Open Database of Buildings (table 34-26-0001).

Quebec's building footprints are published as two province-wide zipped
shapefiles rather than one file per borough, unlike every other source this
pipeline reads. Both are downloaded once - not per neighborhood partition -
and cached on disk keyed by filename, the same posture as `PdfFetcher`: a
published extract does not change, so a scrape date reuses the copy already
on disk instead of pulling gigabytes again on every run.

Deliberately free of Dagster imports, mirroring `urban_rag.open_data` and
`urban_rag.infolot`.
"""

from __future__ import annotations

import time
from pathlib import Path

import geopandas as gpd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

#: https://www150.statcan.gc.ca/n1/en/catalogue/34260001
DEFAULT_BASE_URL = "https://www150.statcan.gc.ca/pub/34-26-0001/2018001/zip"

#: Quebec's two extracts. Both are needed for full coverage of the province;
#: there is no single "QC" file.
QUEBEC_FILES: tuple[str, ...] = ("BDOI_v3_QC_1.zip", "BDOI_v3_QC_2.zip")


class BdoiError(RuntimeError):
    """A BDOI file could not be fetched or read as a shapefile."""


class BdoiFetcher:
    """Downloads the zipped shapefiles, with an on-disk cache keyed by name."""

    def __init__(
        self,
        *,
        cache_dir: Path | str,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 300.0,
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

    def cache_path(self, filename: str) -> Path:
        return self.cache_dir / filename

    def fetch(self, filename: str) -> Path:
        """Download ``filename`` (or reuse the cache), returning its local path."""
        cached = self.cache_path(filename)
        if cached.exists() and cached.stat().st_size:
            return cached

        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        url = f"{self.base_url}/{filename}"
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise BdoiError(f"{url}: {exc}") from exc

        content = response.content
        if not content.startswith(b"PK"):
            # A dead link or a redirected error page answers 200 with a body
            # that is not a zip at all.
            content_type = response.headers.get("Content-Type", "")
            raise BdoiError(
                f"{url}: not a zip (Content-Type {content_type!r}, "
                f"{len(content)} bytes)"
            )

        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(content)
        return cached


def read_shapefile_zip(path: Path | str) -> gpd.GeoDataFrame:
    """Read the shapefile inside a zip, reprojected to EPSG:4326.

    Handed to GDAL's own zip handling via the ``zip://`` prefix, so the
    archive is never unpacked to disk on top of the cached copy.
    """
    try:
        frame = gpd.read_file(f"zip://{path}")
    except Exception as exc:  # pyogrio/fiona raise their own error types
        raise BdoiError(f"{path}: not readable as a shapefile ({exc})") from exc
    if frame.crs is None:
        raise BdoiError(f"{path}: shapefile carries no CRS")
    return frame.to_crs("EPSG:4326")
