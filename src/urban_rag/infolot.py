"""Client for Infolot, Quebec's cadastral lot service.

Infolot is an ArcGIS ``MapServer`` published by the Registre foncier and
fronted by a GeoCortex security module. Three of its quirks shape every call
made here:

1. Only some services behind that module answer at all. ``Infolot`` is open;
   its sibling ``Infolot_Anonyme`` answers ``HTTP 500`` (a GeoCortex
   ``SecuriteModule`` exception rendered as an ASP.NET error page) to every
   request, including ``?f=json``. There is no token to obtain - the open
   service is simply the one to use.
2. ``resultOffset``/``resultRecordCount`` paging is advertised
   (``supportsPagination: true``) but does not work: the same window asked for
   in pages of 200 returns 124, 836 or 47 rows depending on the offset, with
   ``exceededTransferLimit`` unset, so a paged read silently truncates. What
   *is* reliable is ``returnIdsOnly``, which ignores ``maxRecordCount`` and
   returns every matching id in one response.
3. So reads are two-phase: ask for the ids inside a geometry, then fetch them
   in batches by ``objectIds``. Batches are POSTed because an id list is far
   too long for a URL, and because the query geometry - a whole borough
   boundary - runs to ~100 KB of ring coordinates on its own.

The layer's ``minScale`` is a *drawing* constraint: it stops the lots from
rendering on a zoomed-out map, and has no bearing on ``/query``.

Deliberately free of Dagster imports so it can be exercised from a plain
script or a unit test with a stubbed session.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Iterator
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

DEFAULT_BASE_URL = (
    "https://appli.foncier.gouv.qc.ca/arcgiswa/rest/services"
    "/PRODC-E/Infolot/MapServer"
)

#: Layer id of ``Lot``, the cadastral polygons. Its siblings under the same
#: service are annotation layers (lot numbers, bearings, radii) and coarser
#: representations of the same lots for small-scale drawing.
LOT_LAYER = 3

#: The layer's OID field, which is what ``returnIdsOnly`` hands back. Named
#: after the geometry element rather than the lot: one lot can own several
#: polygons, so this is not `NO_LOT`.
OBJECT_ID_FIELD = "NO_INTER_ELEMN_GEOMT"

WGS84 = 4326

#: Rows per ``objectIds`` batch. The service caps a single response at
#: ``maxRecordCount`` (1000); this stays well under it so that one slow batch
#: is cheap to retry.
DEFAULT_BATCH_SIZE = 250

#: Columns the service returns as epoch milliseconds - the lot's status date
#: and the last time its geometry was edited. See `normalize_dates`.
EPOCH_MS_COLUMNS = ("DA_STATT_LOT", "DH_DERNR_MODFC_GEOMT")


class InfolotError(RuntimeError):
    """The service answered, but with an error rather than features."""


class InfolotClient:
    """Two-phase reader over the Infolot ``Lot`` layer."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        layer: int = LOT_LAYER,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        batch_size: int = DEFAULT_BATCH_SIZE,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.layer = layer
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self.batch_size = batch_size
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
        session.headers["Accept"] = "application/json"
        session.headers["User-Agent"] = USER_AGENT
        return session

    @property
    def query_url(self) -> str:
        return f"{self.base_url}/{self.layer}/query"

    # -- transport ---------------------------------------------------------

    def _post(self, data: dict[str, Any]) -> dict:
        # Be a polite guest: this is the Registre foncier's live server.
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)

        response = self._session.post(
            self.query_url, data=data, timeout=self.timeout_seconds
        )
        if "json" not in response.headers.get("Content-Type", ""):
            # The GeoCortex module renders its refusals as HTML error pages.
            raise InfolotError(
                f"Non-JSON response ({response.status_code}) from "
                f"{self.query_url}: {_excerpt(response.text)}"
            )

        payload = response.json()
        # ArcGIS reports failure in the body, with HTTP 200.
        if isinstance(payload, dict) and "error" in payload:
            error = payload["error"]
            raise InfolotError(
                f"{self.query_url}: {error.get('message') or error} "
                f"{'; '.join(error.get('details') or [])}".strip()
            )
        return payload

    # -- api ---------------------------------------------------------------

    def lot_ids(self, geometry: dict, *, in_srs: int = WGS84) -> list[int]:
        """Object ids of every lot intersecting ``geometry``.

        ``geometry`` is an Esri geometry object - see `esri_polygon`. This is
        the one call that is not subject to ``maxRecordCount``, which is why
        the whole read is built around it.
        """
        payload = self._post(
            {
                "geometry": json.dumps(geometry),
                "geometryType": "esriGeometryPolygon",
                "inSR": str(in_srs),
                "spatialRel": "esriSpatialRelIntersects",
                "where": "1=1",
                "returnIdsOnly": "true",
                "f": "json",
            }
        )
        return list(payload.get("objectIds") or [])

    def fetch_lots(
        self, object_ids: Iterable[int], *, target_srs: int = WGS84
    ) -> Iterator[dict]:
        """Yield GeoJSON features for ``object_ids``, one batch at a time.

        ``f=geojson`` is asked for so the geometry arrives as GeoJSON rings
        rather than as Esri's own ``rings`` encoding, which would otherwise
        have to be converted - orientation, holes and all - on this side.
        """
        ids = list(object_ids)
        for start in range(0, len(ids), self.batch_size):
            batch = ids[start : start + self.batch_size]
            payload = self._post(
                {
                    "objectIds": ",".join(str(i) for i in batch),
                    # Ignored next to `objectIds`, but the service rejects the
                    # request outright when neither is present.
                    "where": "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "outSR": str(target_srs),
                    "f": "geojson",
                }
            )
            features = payload.get("features") or []
            if len(features) != len(batch):
                # Silent short reads are the failure mode this client exists
                # to avoid, so they are an error rather than a warning.
                raise InfolotError(
                    f"Asked for {len(batch)} lots by id, got {len(features)}; "
                    "the service dropped rows from the batch."
                )
            yield from features


def esri_polygon(geometry, *, srs: int = WGS84) -> dict:
    """Shapely (Multi)Polygon -> the Esri geometry object ``/query`` expects.

    Esri carries exterior and interior rings in one flat ``rings`` list and
    infers which is which from winding order, which is exactly how shapely
    already stores them, so no re-orientation is needed.
    """
    polygons = getattr(geometry, "geoms", None) or [geometry]
    rings: list[list[list[float]]] = []
    for polygon in polygons:
        if polygon.is_empty:
            continue
        rings.append([list(point) for point in polygon.exterior.coords])
        rings.extend(
            [list(point) for point in interior.coords] for interior in polygon.interiors
        )
    if not rings:
        raise ValueError("Cannot build a query geometry from an empty polygon")
    return {"rings": rings, "spatialReference": {"wkid": srs}}


def normalize_dates(frame: pd.DataFrame) -> pd.DataFrame:
    """Turn the epoch-millisecond columns into UTC timestamps, in place.

    Parquet would otherwise carry them as bare int64, which no reader can tell
    apart from the several other numeric columns on the layer.
    """
    for column in EPOCH_MS_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], unit="ms", utc=True)
    return frame


def _excerpt(text: str, limit: int = 300) -> str:
    return " ".join(text.split())[:limit]
