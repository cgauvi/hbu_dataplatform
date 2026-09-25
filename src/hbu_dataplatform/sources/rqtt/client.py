"""Client for the RQTT, Quebec's province-wide land transport network.

The MRNF publishes the *Référentiel québécois du transport terrestre* - the
renamed AQréseau+ - as one province-wide GeoPackage, zipped, at a URL with no
version in it at all:

    https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Vectoriel/Theme/Local/RQTT/OGC(GPKG)/RQTT_GPKG.zip

Four things about that archive shape every call made here.

**It is 390 MB compressed and 1.27 GB unpacked**, so the download is streamed
to a cache on disk rather than held in memory, and the GeoPackage is unpacked
beside it rather than read through `zip://` - a GeoPackage is SQLite, SQLite
reads by seeking, and a seek inside a deflate stream decompresses from the
start of the member every time. `hbu_dataplatform.sources.roll.client` makes the same two
choices for the same reasons, and this module is deliberately its twin.

**The URL carries no version, unlike the roll's `ROLE2026_GEOPACKAGE.zip`.**
It always serves whatever is current, and the file is reissued three times a
year (April, July, December). So the cache cannot be keyed by the published
filename the way `RoleFetcher`'s is - every vintage would collide on one name,
and a stale copy would be reused forever. It is keyed on the `Last-Modified`
the server reports instead, resolved by a `HEAD` before any body is pulled:
`RQTT_GPKG_20260703.zip`. A new vintage lands beside the old one under its own
name, and `fetch` says which one it used so a partition can record the vintage
it was built from. That is the most this source allows - an old vintage cannot
be re-fetched once the MRNF rotates it, because there is no URL for it.

**There is no municipality column.** The roll can push `code_mun IN (...)` into
OGR as an attribute filter; the road network has nothing equivalent - `Gestion`
names the managing authority in prose, not a code. The cut is therefore
spatial, and the GeoPackage ships an R-tree on the geometry (`rtree_
Reseau_routier_geom`), so a bounding box in the *published* CRS is pushed down
to the index and the province's other segments are never built. `bbox_in_
source_crs` is what turns a borough's WGS84 bounds into that box.

**The published CRS is EPSG:3798**, NAD83 / MTQ Lambert - not the MTM zones
this platform measures in, and not the 32198 Quebec Lambert that is easy to
reach for by mistake. Geometry comes back reprojected to WGS84 like every other
layer here, and the metres are measured downstream in the borough's own MTM
zone (`partitions.metric_crs_for`).

Of the nine layers in the archive only `Reseau_routier` is read; the other
eight are trails, railways, bridges and airstrips. `UNREAD_LAYERS` names them
so a run can report what it left rather than leaving them unaccounted for.

Deliberately free of Dagster imports, mirroring `hbu_dataplatform.sources.roll.client`,
`hbu_dataplatform.sources.bdoi.client` and `hbu_dataplatform.core.open_data`.
"""

from __future__ import annotations

import re
import shutil
import time
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from hbu_dataplatform.cities.montreal.spectrum import USER_AGENT, default_ca_bundle

#: https://diffusion.mern.gouv.qc.ca - the MRNF's geospatial diffusion host.
#: The path is the RQTT's own corner of the *Réseau géodésique québécois* tree;
#: the same theme is published there as SHP and FGDB, which are not read.
DEFAULT_BASE_URL = (
    "https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Vectoriel/Theme/Local/RQTT"
)

#: The archive, under `OGC(GPKG)/`. The parentheses are the publisher's and are
#: part of the path; they are safe unencoded and `requests` leaves them alone.
ARCHIVE_PATH = "OGC(GPKG)/RQTT_GPKG.zip"

#: How a cached archive is named, stamped with the vintage `Last-Modified`
#: reports. See the module docstring for why the published name cannot be used.
CACHE_NAME_TEMPLATE = "RQTT_GPKG_{version}.zip"

#: Matches a cached archive, so `fetch` can fall back to the newest vintage
#: already on disk when the `HEAD` cannot be made.
_CACHE_NAME = re.compile(r"^RQTT_GPKG_(\d{8})\.zip$")

#: The one layer this platform reads: the road network, one MULTILINESTRING per
#: segment. Not a layer name that carries a year, unlike the roll's, so it is
#: written out rather than resolved by prefix.
ROAD_LAYER = "Reseau_routier"

#: The eight layers that are not read. Trails, the cycling network, railways
#: and their level crossings, bridges and airstrips - none of them is a street
#: a lot can front on, and `Route_Verte` in particular would put a bike path
#: through a borough as if it were a roadway.
UNREAD_LAYERS = (
    "Route_Verte",
    "Sentiers_motoneige_FCMQ",
    "Sentiers_quad_FQCQ",
    "Route_Blanche",
    "Reseau_ferroviaire",
    "Reseau_ferroviaire_PN",
    "Ponts",
    "Transport_aerien",
)

#: EPSG:3798, NAD83 / MTQ Lambert - what the archive stores geometry in.
PUBLISHED_CRS = "EPSG:3798"

#: The CRS every layer in this platform is stored and joined in.
WGS84 = "EPSG:4326"

#: The segment's own key, and the street's name - the two columns
#: `street_assets._as_street_sides` renames to this platform's `COTE_RUE_ID` /
#: `NOM_VOIE`.
#:
#: **`AQRP_UUID` and not `IdRte`**, which is the obvious-looking choice and is
#: wrong. Measured over the 93,521 segments inside the Montreal bounding box of
#: the 2026-07-03 vintage: `IdRte` has 47 nulls and 93,474 distinct values, so
#: it is neither complete nor unique and would fail `street_assets.
#: _require_unique_streets` outright; `AQRP_UUID` is 93,521 distinct, exactly
#: one per row. `IdRte` travels in `ROAD_DETAIL_FIELDS` so the publisher's own
#: road identifier is still readable.
STREET_ID_FIELD = "AQRP_UUID"
STREET_NAME_FIELD = "NomRte"

#: The road's functional class. This platform has never carried a road
#: hierarchy - the geobase double published none - and an arterial is a
#: different development frontage from a local street. Carried into
#: `silver.neighborhood_streets.attributes` for a later change to use.
#:
#: The values, over the same Montreal box: `Locale` 61,602, `Collectrice
#: municipale` 13,260, `Artère` 11,369, `Autoroute` 3,722, `Nationale` 2,045,
#: `Régionale` 856, `Sans classe` 205, `Collectrice` 203, `Rue piétonne` 19,
#: `Liaison maritime` 4, and 236 with none. Note what is *not* there: no
#: `Ruelle`. Only 50 segments in the box carry "Ruelle" in their name at all,
#: all of them classed `Locale`, which is the named laneways rather than
#: Montreal's thousands of back lanes - so the lane exclusion that
#: `postgis.DEFAULT_ROAD_LOT_MIN_STREET_M` gets for free from the geobase
#: double's coverage largely survives this source.
ROAD_CLASS_FIELD = "ClsRte"

#: The road's physical characteristic, and where the things that are not a
#: roadway hide: `Bretelle` 2,685, `Pont à étagement` 1,172, `Voie de desserte`
#: 1,102, `Traverse` 815, `Chemin privé` 677, `Pont` 390, `Pont ferroviaire`
#: 159, `Tunnel` 71, the two `Passerelle piétonnière` kinds, `Passage pour
#: pipeline` 2. 85,373 of the 93,521 carry none at all.
ROAD_CHARACTERISTIC_FIELD = "CaractRte"

#: What is dropped before a segment reaches silver, and it is deliberately a
#: short list.
#:
#: **Highways and ramps stay.** They are the tempting things to drop - nobody
#: fronts on an autoroute - and dropping them would be a bug. The street line's
#: first job is to identify which parcels are *roadway*, and
#: `hbu.cadastral_road_lots` is what stops the solver being handed one to
#: develop. A highway with no line inside it is not a road lot, and its parcel
#: becomes a development site. Whether a lot abutting it earns frontage is a
#: question the exact shared-edge measure answers on its own.
#:
#: So what goes is only what is not a road at all: a ferry link across water, a
#: footbridge, a railway bridge, a pipeline crossing. Each of these would
#: otherwise run a line through a parcel that carries no roadway and make it
#: one.
EXCLUDED_ROAD_CLASSES = ("Liaison maritime",)
EXCLUDED_ROAD_CHARACTERISTICS = (
    "Traverse",
    "Passerelle piétonnière",
    "Passerelle piétonnière et cycliste",
    "Pont ferroviaire",
    "Passage pour pipeline",
)

#: Carried into `silver.neighborhood_streets.attributes` rather than dropped:
#: the publisher's own road id, the authority that manages it (`Municipal` for
#: 86,671 of the Montreal box, `Transports Québec` for 5,280, `Privé` for 917),
#: the route number, and the vintage stamp the segment itself carries.
ROAD_DETAIL_FIELDS = ("IdRte", "Gestion", "NoRte", "Version")


class RqttError(RuntimeError):
    """Raised when the archive cannot be fetched, unpacked or read."""


def version_from_last_modified(header: str) -> str:
    """`Last-Modified` as the `YYYYMMDD` stamp a cached archive is named with.

    The header is RFC 7231 (`Fri, 03 Jul 2026 17:15:26 GMT`), which is what
    `parsedate_to_datetime` reads. Only the date survives: the MRNF reissues
    this file three times a year, so the time of day distinguishes nothing and
    a cache name is easier to recognise without it.
    """
    try:
        stamp = parsedate_to_datetime(header)
    except (TypeError, ValueError) as exc:
        raise RqttError(f"Last-Modified {header!r} is not a date") from exc
    if stamp is None:
        raise RqttError(f"Last-Modified {header!r} is not a date")
    return stamp.strftime("%Y%m%d")


def bbox_in_source_crs(
    bounds: tuple[float, float, float, float], *, crs: str = WGS84
) -> tuple[float, float, float, float]:
    """``bounds`` transformed into `PUBLISHED_CRS`, for `read_layer`'s ``bbox``.

    OGR reads a spatial filter in the *dataset's* CRS, and this dataset is in
    MTQ Lambert while everything upstream of it here is in WGS84. Handing
    degrees to a projected R-tree does not fail - it selects nothing, or very
    nearly nothing, which is the kind of empty partition that reads like a bad
    boundary rather than a bad filter. So the transform is explicit and this
    helper is the only way the box is built.

    The box is transformed by its corners, which is correct for a bounding box
    only because it is then re-bounded: a Lambert projection bends the edges of
    a lon/lat rectangle, so the four projected corners are taken as a point set
    and their own envelope is returned, which contains the curved edges rather
    than cutting them.
    """
    minx, miny, maxx, maxy = bounds
    corners = gpd.GeoSeries(
        gpd.points_from_xy(
            [minx, maxx, minx, maxx],
            [miny, miny, maxy, maxy],
        ),
        crs=crs,
    ).to_crs(PUBLISHED_CRS)
    projected = corners.total_bounds
    return (
        float(projected[0]),
        float(projected[1]),
        float(projected[2]),
        float(projected[3]),
    )


class RqttFetcher:
    """Downloads and unpacks the archive, with an on-disk cache keyed by vintage."""

    def __init__(
        self,
        *,
        cache_dir: Path | str,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = 1800.0,
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
            allowed_methods=("GET", "HEAD"),
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["User-Agent"] = USER_AGENT
        return session

    @property
    def url(self) -> str:
        return f"{self.base_url}/{ARCHIVE_PATH}"

    def cache_path(self, version: str) -> Path:
        return self.cache_dir / CACHE_NAME_TEMPLATE.format(version=version)

    def remote_version(self) -> str:
        """The vintage the server is currently publishing, from a `HEAD`.

        A `HEAD` rather than a ranged `GET`: it costs one round trip and no
        body, and the answer decides whether 390 MB needs to move at all.
        """
        try:
            response = self._session.head(
                self.url, timeout=self.timeout_seconds, allow_redirects=True
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RqttError(f"{self.url}: {exc}") from exc
        header = response.headers.get("Last-Modified")
        if not header:
            raise RqttError(
                f"{self.url}: no Last-Modified header, so the vintage this "
                "cache is keyed on cannot be resolved"
            )
        return version_from_last_modified(header)

    def cached_versions(self) -> tuple[str, ...]:
        """Every vintage already on disk, oldest first."""
        if not self.cache_dir.is_dir():
            return ()
        found = []
        for path in self.cache_dir.iterdir():
            match = _CACHE_NAME.match(path.name)
            if match and path.stat().st_size:
                found.append(match.group(1))
        return tuple(sorted(found))

    def fetch(self, version: str | None = None) -> tuple[Path, str]:
        """Download the archive (or reuse the cache), as (path, vintage).

        ``version`` pins a vintage already on disk; left None, the server is
        asked which one is current and that one is fetched if it is not cached
        already.

        The vintage travels back with the path because it is the only record of
        *which* RQTT a partition was built from - the URL does not say, and the
        MRNF overwrites it three times a year.

        Streamed to a `.part` file and renamed on success, like
        `RoleFetcher.fetch`: this is 390 MB, and a download killed halfway must
        not leave a truncated archive the next run treats as cached.
        """
        if version is None:
            try:
                version = self.remote_version()
            except RqttError:
                # Offline, or the host is refusing HEAD. A vintage on disk is a
                # better answer than no answer: this source is reissued three
                # times a year, so the newest cached copy is almost certainly
                # the current one, and the caller is told which it got.
                cached = self.cached_versions()
                if not cached:
                    raise
                return self.cache_path(cached[-1]), cached[-1]

        cached = self.cache_path(version)
        if cached.exists() and cached.stat().st_size:
            return cached, version

        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        partial = cached.with_suffix(cached.suffix + ".part")
        cached.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._session.get(
                self.url, timeout=self.timeout_seconds, stream=True
            ) as response:
                response.raise_for_status()
                content_type = response.headers.get("Content-Type", "")
                with open(partial, "wb") as handle:
                    first = True
                    for chunk in response.iter_content(1 << 22):
                        if not chunk:
                            continue
                        if first and not chunk.startswith(b"PK"):
                            # A dead link or a redirected error page answers 200
                            # with a body that is not a zip at all. Checked on
                            # the first bytes rather than after the download, so
                            # 390 MB of HTML is not written first.
                            raise RqttError(
                                f"{self.url}: not a zip (Content-Type "
                                f"{content_type!r})"
                            )
                        first = False
                        handle.write(chunk)
        except requests.RequestException as exc:
            partial.unlink(missing_ok=True)
            raise RqttError(f"{self.url}: {exc}") from exc
        except BaseException:
            partial.unlink(missing_ok=True)
            raise

        if not partial.stat().st_size:
            partial.unlink(missing_ok=True)
            raise RqttError(f"{self.url}: the server returned an empty body")
        partial.replace(cached)
        return cached, version

    def geopackage(self, version: str | None = None) -> tuple[Path, str]:
        """The archive's GeoPackage, unpacked beside it, as (path, vintage).

        Unpacked rather than read through `zip://`, for the reason the module
        docstring gives: a GeoPackage is SQLite, and SQLite seeks. The unpacked
        copy carries the vintage in its name too, so two vintages can sit in
        the cache without the second one silently serving the first's bytes.
        """
        archive, version = self.fetch(version)
        member = _geopackage_member(archive)
        unpacked = self.cache_dir / f"RQTT_{version}.gpkg"
        if unpacked.exists() and unpacked.stat().st_size:
            return unpacked, version

        partial = unpacked.with_suffix(unpacked.suffix + ".part")
        try:
            with zipfile.ZipFile(archive) as zf, zf.open(member) as src:
                with open(partial, "wb") as handle:
                    shutil.copyfileobj(src, handle, 1 << 24)
        except (zipfile.BadZipFile, OSError) as exc:
            partial.unlink(missing_ok=True)
            raise RqttError(
                f"{archive}: {member} could not be unpacked ({exc})"
            ) from exc
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        partial.replace(unpacked)
        return unpacked, version


def _geopackage_member(path: Path | str) -> str:
    """Name of the single `.gpkg` inside the archive.

    Named rather than guessed at a fixed path: the member sits under an
    `OGC(GPKG)/` directory inside the zip, and a publisher that moves it should
    cost a re-read rather than a `KeyError` on a hard-coded string.
    """
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
    except zipfile.BadZipFile as exc:
        raise RqttError(f"{path}: not a readable zip ({exc})") from exc
    members = [name for name in names if name.lower().endswith(".gpkg")]
    if not members:
        raise RqttError(f"{path}: no .gpkg member (contains {names})")
    if len(members) > 1:
        raise RqttError(f"{path}: expected one .gpkg, got {members}")
    return members[0]


def layer_names(path: Path | str) -> tuple[str, ...]:
    """Every layer in the GeoPackage, in the order it lists them."""
    try:
        return tuple(str(row[0]) for row in pyogrio.list_layers(str(path)))
    except Exception as exc:  # pyogrio raises its own error types
        raise RqttError(f"{path}: not readable as a GeoPackage ({exc})") from exc


def read_layer(
    path: Path | str,
    layer: str = ROAD_LAYER,
    *,
    bbox: tuple[float, float, float, float] | None = None,
    where: str | None = None,
    columns: list[str] | None = None,
) -> gpd.GeoDataFrame:
    """One layer of the GeoPackage, cut down to ``bbox`` before it is built.

    ``bbox`` is in `PUBLISHED_CRS` and must come from `bbox_in_source_crs` -
    OGR reads a spatial filter in the dataset's own CRS, and this one is MTQ
    Lambert. It is pushed into the GeoPackage's R-tree, so the province's other
    segments are never read: the whole road network is some millions of rows,
    and an island's worth is a few tens of thousands.

    Geometry comes back reprojected to WGS84, the CRS every other layer in this
    platform is stored and joined in, for the reason
    `hbu_dataplatform.sources.roll.client.read_layer` gives.
    """
    try:
        frame = pyogrio.read_dataframe(
            str(path), layer=layer, bbox=bbox, where=where, columns=columns
        )
    except Exception as exc:  # pyogrio raises its own error types
        raise RqttError(f"{path}: layer {layer!r} not readable ({exc})") from exc

    if frame.crs is None:
        raise RqttError(f"{path}: layer {layer!r} carries no CRS")
    return frame.to_crs(WGS84)


def roadway_only(
    segments: gpd.GeoDataFrame,
    *,
    classes: tuple[str, ...] = EXCLUDED_ROAD_CLASSES,
    characteristics: tuple[str, ...] = EXCLUDED_ROAD_CHARACTERISTICS,
) -> gpd.GeoDataFrame:
    """``segments`` minus the lines that are not a roadway.

    See `EXCLUDED_ROAD_CLASSES` for why this list is as short as it is: what is
    removed here is a ferry link, a footbridge, a railway bridge or a pipeline
    crossing - geometry that would otherwise run a line through a parcel
    carrying no roadway and make it a road lot. Every actual road stays,
    autoroutes and ramps included.

    A missing class or characteristic keeps the row. `Sans classe` and the 236
    segments with no class at all are still streets somebody drives on, and
    this filter's job is to remove what is positively known not to be a road,
    not to demand a label before believing one.
    """
    keep = pd.Series(True, index=segments.index)
    if classes and ROAD_CLASS_FIELD in segments.columns:
        keep &= ~segments[ROAD_CLASS_FIELD].isin(classes)
    if characteristics and ROAD_CHARACTERISTIC_FIELD in segments.columns:
        keep &= ~segments[ROAD_CHARACTERISTIC_FIELD].isin(characteristics)
    return segments[keep]
