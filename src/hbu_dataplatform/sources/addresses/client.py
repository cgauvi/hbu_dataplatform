"""Adresses Québec: the province's official civic addresses, as points.

The publisher is the *Ministère des Ressources naturelles et des Forêts*, and
the product is ``AQ Adresses`` — "les données officielles sur les adresses et
leur localisation sous forme de points pour tout le Québec". One point per
address, province-wide, restamped on its own cadence; the stamp travels on
every row as ``Version`` (``AQ20260901`` at the time of writing), which is the
only thing the service says about its own freshness.

**Why this module does not speak WMS.** The endpoint the product is advertised
under is `DEFAULT_WMS_URL` below, and WMS is a *picture* protocol: `GetMap`
returns a rendered PNG and `GetFeatureInfo` answers "what is under this pixel",
one pixel at a time, against a layer the same service refuses to draw above
1:10,000. Neither can hand back a borough's addresses as data, and no WMS
request can be filtered by a parcel. The same ArcGIS MapServer publishes a REST
endpoint beside its WMS connector — `DEFAULT_SERVICE_URL`, the identical layer
declaring ``capabilities: Map,Query,Data`` and ``supportsSpatialFilter`` — and
that one answers a spatial query in GeoJSON. So the WMS URL is recorded here as
the product's front door and the REST sibling is what gets read.

**There is no lot number on an address.** The layer publishes ten fields and
`ADDRESS_FIELDS` is all of them: a formatted address, a civic number and its
suffix, a unit count, a stable UUID, a positional note, the version, and the
point. Nothing cadastral — no ``NoLot``, no matricule. So "the address of lot
2 784 705" is not a question this publisher can be asked directly; it is
answered by putting the points on the cadastre, which is what
`hbu_dataplatform.core.postgis.compute_lot_addresses` does and why the join is silver's
work rather than a query parameter here.

**A point is a unit, not a building.** Roughly half of the addresses in a dense
borough carry a unit prefix — ``204-7430 Rue Lajeunesse`` and ``7430 Rue
Lajeunesse`` are two rows — so a count of rows on a parcel is a count of
addressable units and not of front doors. `parse_formatted_address` splits the
unit off precisely so both questions stay answerable; see `civic_address`.

The one paging rule worth knowing: the service caps a response at
`MAX_RECORD_COUNT` rows and sets ``exceededTransferLimit`` when it truncates,
so `AdressesQuebecClient.fetch_addresses` walks ``resultOffset`` under an
explicit ``orderByFields``. Without the ordering the pages are not a partition
of the answer and rows are silently both duplicated and dropped.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator, Sequence
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hbu_dataplatform.cities.montreal.spectrum import USER_AGENT, default_ca_bundle

#: The endpoint the product is published under, kept for the record. It is a
#: WMS connector on the MapServer below and cannot answer a feature query —
#: see the module docstring.
DEFAULT_WMS_URL = (
    "https://servicescarto.mrnf.gouv.qc.ca/pes/services/Territoire/"
    "AQ_ADRESSES_WMS/MapServer/WMSServer"
)

#: The REST face of that same MapServer, layer 0 (``Adresses``). This is what
#: is actually read.
DEFAULT_SERVICE_URL = (
    "https://servicescarto.mrnf.gouv.qc.ca/pes/rest/services/Territoire/"
    "AQ_ADRESSES_WMS/MapServer/0"
)

WGS84 = 4326

#: What the service will return in one response, from its own layer metadata.
#: Asking for more is not an error and does not get more - it silently
#: truncates and says so in ``exceededTransferLimit``.
MAX_RECORD_COUNT = 1000

#: Every field the layer publishes, in the order its metadata lists them.
#: Requested by name rather than as ``*`` so a field added upstream shows up as
#: a changed schema in review rather than as a wider parquet nobody noticed.
ADDRESS_FIELDS: tuple[str, ...] = (
    "OBJECTID",
    "IdAdr",
    "AdresseFormatee",
    "NoCivq",
    "NoCivqSuf",
    "NbUnite",
    "CaractAdr",
    "Position",
    "Version",
)

#: The layer's OID, which is what the paging below orders on. Any stable total
#: order would do; this is the one field guaranteed unique and indexed.
OBJECT_ID_FIELD = "OBJECTID"

#: The publisher's stable identifier for an address - a 32-character hex UUID,
#: unique across the province and carried through to `silver.lot_addresses` as
#: `address_id`. The one field here that survives a re-publication, which is
#: what makes it the natural key rather than `OBJECTID`.
ADDRESS_ID_FIELD = "IdAdr"

#: How many vertices of a borough outline are worth sending as a spatial
#: filter. Past this the request body gets large and the service spends longer
#: on the filter than on the answer, so `esri_query_geometry` falls back to the
#: outline's bounding box. That only ever *widens* the query - every caller
#: clips the result to the true boundary itself - so the fallback costs
#: transfer and never correctness.
MAX_FILTER_VERTICES = 1000


class AdressesQuebecError(RuntimeError):
    """The address service could not be read, or answered with something else."""


# ---------------------------------------------------------------------------
# the formatted address
# ---------------------------------------------------------------------------

#: `AdresseFormatee`, taken apart. The layer states the civic number and its
#: suffix in columns of their own but says nothing about the street, the
#: municipality or the unit, and those are three of the four things a reader
#: wants; the formatted string is the only place they exist.
#:
#: The shape, from the two boroughs this platform reads (4,000 records, all of
#: which parse, and whose `suffix` never once disagrees with the `NoCivqSuf`
#: column):
#:
#:     [<unit>-]<civic>[ <suffix>] <street>[ (<disambiguation>)], <muni> <postal>
#:
#:     204-7430 Rue Lajeunesse, Montréal H2R2H8
#:     7390 A Rue De Lanaudière, Montréal H2E1Y4
#:     PH4-5785 Rue Boyer, Montréal H2S2H7
#:     8635 12e Avenue (Montréal), Montréal H1Z3J1
#:     1-27 1/2 Rue Sainte-Angèle, Québec G1R4G5
#:
#: Three details that each cost a rewrite to learn:
#:
#: * **The suffix may be spaced or not.** ``7390 A Rue De Lanaudière`` and
#:   ``336B-8755 Rue Saint-Hubert`` are the same field. Matched with an
#:   optional space, and cross-checked against the `NoCivqSuf` column.
#: * **The suffix is not always a letter.** Old Québec numbers houses ``27
#:   1/2`` and ``8 1/4``, and the publisher puts the fraction in `NoCivqSuf`.
#:   A ``[A-Za-z]`` suffix class parses the whole of Montreal and fails 18
#:   addresses in La Cité-Limoilou, every one of them in the old city.
#: * **The parenthesis is a disambiguator, not part of the street.** ``12e
#:   Avenue (Montréal)`` is printed where the street name repeats elsewhere in
#:   the province; the street is ``12e Avenue``.
ADDRESS_RE = re.compile(
    r"""^\s*
    (?:(?P<unit>[^,]*?)\s*-\s*)?                        # 204- , PH4- , A-
    (?P<civic>\d+)
    (?:\s*(?P<suffix>[A-Za-z]|\d+/\d+))?                # A , B , 1/2 , 1/4
    \s+
    (?P<street>.+?)
    (?:\s*\((?P<disambiguation>[^)]*)\))?               # (Montréal)
    ,\s*
    (?P<municipality>.+?)
    (?:\s+(?P<postal_code>[A-Z]\d[A-Z]\s?\d[A-Z]\d))?
    \s*$""",
    re.VERBOSE,
)

#: The columns `parse_formatted_address` produces, in the order it produces
#: them.
PARSED_COLUMNS: tuple[str, ...] = (
    "unit",
    "civic",
    "suffix",
    "street",
    "disambiguation",
    "municipality",
    "postal_code",
)


def parse_formatted_address(formatted: str) -> dict[str, str | None] | None:
    """`AdresseFormatee` taken apart, or `None` if it does not have the shape.

    Returns `None` rather than a half-filled dict: a string that does not match
    is one this parser has not seen, and a caller that quietly kept the two
    fields it could find would report a street name that is really a street
    name and a number, with nothing saying so.
    """
    match = ADDRESS_RE.match(formatted or "")
    if match is None:
        return None
    return {name: match.group(name) for name in PARSED_COLUMNS}


def parse_addresses(formatted: "pd.Series") -> "pd.DataFrame":
    """`parse_formatted_address` over a column, as a frame of `PARSED_COLUMNS`.

    The vectorized path, for the silver asset. A row that does not match comes
    back all-null, which is what `unparsed` counts.
    """
    parsed = formatted.str.extract(ADDRESS_RE)
    # `str.extract` names its columns after the regex's groups, which is the
    # order they are declared in rather than the order stated here.
    return parsed.reindex(columns=list(PARSED_COLUMNS))


def _missing(value: object) -> bool:
    """Whether ``value`` is any of the several nulls that reach this module.

    There are four, and they do not answer to one test: `None` from the
    publisher's JSON, `float('nan')` from a numeric column, `pandas.NA` from a
    nullable dtype, and the empty string the layer uses for an absent
    characteristic. `pandas.isna` covers the first three - and `str(pandas.NA)`
    is ``'<NA>'`` rather than ``'nan'``, which is how a null gets *into* a
    formatted string instead of being left out of it.
    """
    if value is None:
        return True
    try:
        if bool(pd.isna(value)):
            return True
    except (TypeError, ValueError):  # an array-like; not a scalar null
        return False
    return isinstance(value, str) and not value.strip()


def civic_address(
    civic: object, suffix: object, street: object
) -> str | None:
    """``7430 Rue Lajeunesse`` - the address without its unit.

    The grain a *door* is counted at, as opposed to the addressable unit a row
    of this layer is. On a walk-up with eight apartments the layer publishes
    nine rows - the eight units and the building - carrying one civic address
    between them, so counting rows and counting civic addresses answer two
    different questions and `silver.lot_addresses` reports both.
    """
    if _missing(civic):
        return None
    number = (
        str(int(civic)) if isinstance(civic, (int, float)) else str(civic).strip()
    )
    parts = [number]
    if not _missing(suffix):
        parts.append(str(suffix).strip())
    if not _missing(street):
        parts.append(str(street).strip())
    return " ".join(parts)


# ---------------------------------------------------------------------------
# the spatial filter
# ---------------------------------------------------------------------------


def esri_query_geometry(geometry: Any) -> tuple[dict, str, int]:
    """A shapely polygon as an Esri geometry, with the type and vertex count.

    Returns the ``(geometry, geometryType, vertices)`` triple the query below
    posts. A polygon of at most `MAX_FILTER_VERTICES` vertices is sent as
    rings; anything larger - and anything that is not a polygon - is sent as
    its envelope, because past that size the request body costs more than the
    rows it saves.

    **Rings are wound the way Esri reads them**: clockwise for an outer ring,
    counter-clockwise for a hole, which is the opposite of GeoJSON's rule.
    Handing ArcGIS a GeoJSON-wound polygon is not an error and does not fail -
    it is read as a hole, and the answer is every address *outside* the
    borough. `shapely.geometry.polygon.orient(..., sign=-1.0)` is what flips
    it; the sign is negative because shapely's positive is counter-clockwise.
    """
    from shapely.geometry import MultiPolygon, Polygon
    from shapely.geometry.polygon import orient

    minx, miny, maxx, maxy = geometry.bounds
    envelope = (
        {
            "xmin": minx,
            "ymin": miny,
            "xmax": maxx,
            "ymax": maxy,
            "spatialReference": {"wkid": WGS84},
        },
        "esriGeometryEnvelope",
        4,
    )

    if isinstance(geometry, Polygon):
        polygons = [geometry]
    elif isinstance(geometry, MultiPolygon):
        polygons = list(geometry.geoms)
    else:
        return envelope

    rings: list[list[list[float]]] = []
    vertices = 0
    for polygon in polygons:
        # sign=-1.0 orients the exterior clockwise and the interiors
        # counter-clockwise, which is Esri's convention - see the docstring.
        oriented = orient(polygon, sign=-1.0)
        for ring in (oriented.exterior, *oriented.interiors):
            coordinates = [[float(x), float(y)] for x, y in ring.coords]
            vertices += len(coordinates)
            rings.append(coordinates)

    if not rings or vertices > MAX_FILTER_VERTICES:
        return envelope
    return (
        {"rings": rings, "spatialReference": {"wkid": WGS84}},
        "esriGeometryPolygon",
        vertices,
    )


# ---------------------------------------------------------------------------
# the client
# ---------------------------------------------------------------------------


class AdressesQuebecClient:
    """A paging reader over the ``Adresses`` layer.

    Same posture as `hbu_dataplatform.cities.quebec_city.zoning.QuebecZoningClient`, which reads the same
    kind of service: POSTed, paced, and retried on the status codes a live
    municipal server uses to mean "later".

    POST rather than GET throughout, because the spatial filter is a borough
    outline and a few hundred vertices of it do not fit in a query string that
    every proxy between here and Quebec City will forward intact.
    """

    def __init__(
        self,
        service_url: str = DEFAULT_SERVICE_URL,
        *,
        timeout_seconds: float = 120.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        page_size: int = MAX_RECORD_COUNT,
        max_pages: int = 5000,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.service_url = service_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self.page_size = min(int(page_size), MAX_RECORD_COUNT)
        self.max_pages = int(max_pages)
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

    @property
    def query_url(self) -> str:
        return f"{self.service_url}/query"

    def _post(self, data: dict[str, Any]) -> dict:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.post(
                self.query_url, data=data, timeout=self.timeout_seconds
            )
        except requests.RequestException as exc:
            raise AdressesQuebecError(f"{self.query_url}: {exc}") from exc
        if "json" not in response.headers.get("Content-Type", ""):
            raise AdressesQuebecError(
                f"Non-JSON response ({response.status_code}) from "
                f"{self.query_url}: {response.text[:300]!r}"
            )
        payload = response.json()
        # ArcGIS answers an error with HTTP 200 and an `error` object, so the
        # status code is not what says whether this worked.
        if isinstance(payload, dict) and "error" in payload:
            error = payload["error"]
            raise AdressesQuebecError(
                f"{self.query_url}: {error.get('message') or error} "
                f"{'; '.join(error.get('details') or [])}".strip()
            )
        return payload

    def layer_metadata(self) -> dict:
        """The layer's own description: its fields, its extent, its limits."""
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(
                self.service_url, params={"f": "json"}, timeout=self.timeout_seconds
            )
        except requests.RequestException as exc:
            raise AdressesQuebecError(f"{self.service_url}: {exc}") from exc
        payload = response.json()
        if "error" in payload:
            raise AdressesQuebecError(f"{self.service_url}: {payload['error']}")
        return payload

    def count(self, geometry: Any) -> int:
        """How many addresses the filter matches, before any of them are read.

        Asked ahead of the paging rather than inferred from it: it is what says
        whether a walk that stopped at `max_pages` stopped because it was done
        or because it ran out of pages, and the two are not distinguishable
        from the last page alone.
        """
        esri, geometry_type, _ = esri_query_geometry(geometry)
        payload = self._post(
            {
                **self._spatial_filter(esri, geometry_type),
                "returnCountOnly": "true",
                "f": "json",
            }
        )
        return int(payload.get("count") or 0)

    def fetch_addresses(
        self, geometry: Any, *, target_srs: int = WGS84
    ) -> Iterator[dict]:
        """Yield every address GeoJSON feature intersecting ``geometry``.

        Paged on ``resultOffset`` under an explicit ``orderByFields``. The
        ordering is not a nicety: ArcGIS does not promise a stable order
        between two unordered queries, so paging without it returns a sample
        of the answer with duplicates in it rather than the answer.
        """
        esri, geometry_type, _ = esri_query_geometry(geometry)
        spatial = self._spatial_filter(esri, geometry_type)
        offset = 0
        for page in range(self.max_pages):
            payload = self._post(
                {
                    **spatial,
                    "outFields": ",".join(ADDRESS_FIELDS),
                    "returnGeometry": "true",
                    "outSR": str(target_srs),
                    "orderByFields": f"{OBJECT_ID_FIELD} ASC",
                    "resultOffset": str(offset),
                    "resultRecordCount": str(self.page_size),
                    "f": "geojson",
                }
            )
            features = payload.get("features") or []
            yield from features
            if len(features) < self.page_size:
                return
            offset += len(features)
        raise AdressesQuebecError(
            f"{self.query_url}: still returning full pages after "
            f"{self.max_pages} of them ({offset} addresses). Either the filter "
            "is wider than a borough or the service is ignoring resultOffset; "
            "raise max_pages only once you know which."
        )

    @staticmethod
    def _spatial_filter(esri: dict, geometry_type: str) -> dict[str, str]:
        import json as _json

        return {
            "geometry": _json.dumps(esri),
            "geometryType": geometry_type,
            "inSR": str(WGS84),
            "spatialRel": "esriSpatialRelIntersects",
            "where": "1=1",
        }


def features_to_frame(features: Sequence[dict]) -> "pd.DataFrame":
    """GeoJSON features -> a flat frame of `ADDRESS_FIELDS` plus lon/lat.

    Kept out of the asset so the shape of a fetch can be tested without one.
    Returns an empty frame with the right columns when handed nothing, so a
    caller never has to special-case the borough that answered with zero.
    """
    columns = [*ADDRESS_FIELDS, "longitude", "latitude"]
    rows: list[dict[str, Any]] = []
    for feature in features:
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") or [None, None]
        row = {name: properties.get(name) for name in ADDRESS_FIELDS}
        row["longitude"] = coordinates[0]
        row["latitude"] = coordinates[1]
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)
