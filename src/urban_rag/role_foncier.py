"""Client for the *rôle d'évaluation foncière*, Quebec's property assessment roll.

The MAMH publishes every municipality's assessment roll as one province-wide
GeoPackage, zipped, at a URL stamped with the fiscal year the roll takes effect
in:

    https://donneesouvertes.affmunqc.net/role/ROLE2026_GEOPACKAGE.zip

Three things about that archive shape every call made here.

**It is 572 MB compressed and 2.8 GB unpacked**, so the download is streamed to
a cache on disk rather than held in memory, and it is keyed by filename and
shared across every scrape date - the same posture as `BdoiFetcher` and
`CmhcFetcher`. A published roll year is final; re-fetching it on every scrape
date would pull half a gigabyte to get the same bytes back.

**The GeoPackage is unpacked before it is read**, unlike the BDOI shapefiles,
which `urban_rag.bdoi` hands to GDAL through `zip://` without ever touching
disk. A GeoPackage is a SQLite database, and SQLite reads it by seeking; a seek
inside a deflate stream means decompressing from the start of the member every
time, so `/vsizip` turns a table scan into hours. The unpacked copy sits beside
the archive in the same cache.

**The layer names carry the roll year** - `rol_unite_p_2026`,
`b05v_unite_evaln_2026` - so they are resolved by prefix (`layer_named`) rather
than written out. Next year's archive is the same five layers with a different
suffix, and a hard-coded name would fail on the one line that is guaranteed to
change.

The five layers, and the one relation between them: `rol_unite_p` is the only
one with geometry - one point per *unité d'évaluation*, placed at the unit's
visual centre - and the other four hang off it by `id_provinc`, the municipality
code and the 18-character matricule concatenated. Three are read:
`b05v_unite_evaln`, the characteristics table, one row per unit, holding the
assessed values; and `b05v_lot_cadst`, the crosswalk naming every cadastre lot
a unit covers, which is what lets a lot be valued by lot number rather than by
where a point falls. `b05v_adr_unite_evaln` (addresses) and `b05v_repar_fisc`
(fiscal breakdowns) are not read.

Deliberately free of Dagster imports, mirroring `urban_rag.bdoi` and
`urban_rag.open_data`.
"""

from __future__ import annotations

import os
import re
import shutil
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.spectrum import USER_AGENT, default_ca_bundle

#: https://donneesouvertes.affmunqc.net - the MAMH's open-data host.
DEFAULT_BASE_URL = "https://donneesouvertes.affmunqc.net/role"

#: Fiscal year of the roll to read. A roll is triennial and each municipality
#: files its own between 15 August and 15 September of the preceding year, so
#: the archive for a year is published as one file, once, and never revised.
DEFAULT_ROLL_YEAR = 2026

#: Overrides `DEFAULT_ROLL_YEAR` without restating the rest of the resource -
#: see `RoleResource.roll_year`.
ROLL_YEAR_VAR = "URBAN_RAG_ROLL_YEAR"

#: Layer name prefixes, minus the `_<year>` suffix `layer_named` resolves.
#:
#: The point layer: one *unité d'évaluation* per row, in EPSG:4269 (NAD83).
POINT_LAYER = "rol_unite_p"
#: The characteristics table: one row per unit, and where the values are.
UNITS_LAYER = "b05v_unite_evaln"
#: The cadastre crosswalk: one row per (unit, lot the unit covers). The roll's
#: own answer to "which lots is this property on", and what
#: `urban_rag.role_assets` joins to Infolot's polygons by lot number rather
#: than by geometry. One-to-many - 43% of Montreal's units name two lots or
#: more - which is why it is its own file and its own grain.
CADASTRE_LAYER = "b05v_lot_cadst"

#: The two tables this pipeline does not read. Listed so a run can report what
#: it dropped rather than leaving the archive's other 5 million rows
#: unaccounted for.
UNREAD_LAYERS = ("b05v_adr_unite_evaln", "b05v_repar_fisc")

#: What every table in the archive is keyed on: the municipality's five-digit
#: geographic code followed by the 18-character matricule.
JOIN_KEY = "id_provinc"

#: `rl0404a`, VALEUR IMMEUBLE - the value of the whole property (land plus
#: buildings) entered on the roll in force. `rl0402a` and `rl0403a` are its
#: land and building halves.
VALUE_COLUMN = "rl0404a"

#: The characteristics `UNITS_LAYER` describes a unit by, under the MAMH codes
#: it publishes them as. Named here rather than written out at each use for the
#: reason `VALUE_COLUMN` is: `rl0308a` says nothing at a call site, and a roll
#: that renames a field should be one edit rather than a search.
#:
#: `USE_CODE_COLUMN` is the CUBF - four digits whose *leading* one is the
#: category, which is why `urban_rag.comparables` reads a character of it and
#: never compares two codes as numbers. `LAND_AREA_COLUMN` is the unit's own
#: superficie and is not the cadastre's: a divided co-ownership states the
#: whole parcel's area on every one of its apartments, so the lot polygon is
#: the ground measurement to trust and this one is carried for reading.
USE_CODE_COLUMN = "rl0105a"
FRONTAGE_COLUMN = "rl0301a"
LAND_AREA_COLUMN = "rl0302a"
STOREYS_COLUMN = "rl0306a"
YEAR_BUILT_COLUMN = "rl0307a"
FLOOR_AREA_COLUMN = "rl0308a"
DWELLINGS_COLUMN = "rl0311a"
NONRESIDENTIAL_UNITS_COLUMN = "rl0312a"
RENTAL_ROOMS_COLUMN = "rl0313a"
LAND_VALUE_COLUMN = "rl0402a"
BUILDING_VALUE_COLUMN = "rl0403a"

#: `rl0103a`, NUMÉRO LOT - the cadastre lot number a unit covers, in
#: `CADASTRE_LAYER`. Seven digits, unpadded and unspaced (`"1243415"`), where
#: Infolot spells the same lot `"1 243 415"` - see
#: `urban_rag.role_assets.lot_key`, which is what makes the two comparable.
ROLL_LOT_COLUMN = "rl0103a"

#: `rl0103b`, SUFFIXE NUMÉRO LOT. Not part of the key: a renewed Quebec lot is
#: the number alone, and the suffix only distinguishes rows of the *non-renewed*
#: cadastre that name the same one. Ignoring it makes 1,758 of Montreal's
#: crosswalk rows duplicate (unit, lot) pairs, which `lot_assessed_values`
#: drops rather than counts twice.
ROLL_LOT_SUFFIX_COLUMN = "rl0103b"

#: Geographic code of Ville de Montréal. The other on-island municipalities
#: (Westmount, Mont-Royal, Côte-Saint-Luc, ...) file their own rolls under
#: their own codes and are not boroughs, so they are outside every partition
#: this pipeline has - see `urban_rag.partitions`.
MONTREAL_CODE_MUN = "66023"

#: The CRS the point layer is published in. NAD83, not WGS 84: about a metre
#: and a half away from EPSG:4326 in Montreal, which is enough to move a point
#: across a lot line. `read_layer` reprojects.
PUBLISHED_CRS = "EPSG:4269"

#: What the rest of this platform stores and joins in.
WGS84 = "EPSG:4326"

_VALID_CODE_MUN = re.compile(r"^\d{5}$")


class RoleError(RuntimeError):
    """The archive could not be fetched, unpacked, or read as a GeoPackage."""


def default_roll_year() -> int:
    """`ROLL_YEAR_VAR` if it holds a year, `DEFAULT_ROLL_YEAR` otherwise.

    Read per instantiation rather than at import, so a `.env` loaded later - or
    a variable set for one run - still reaches the resource. Mirrors
    `urban_rag.cmhc.default_survey_year`.
    """
    raw = os.environ.get(ROLL_YEAR_VAR)
    if raw is None or not raw.strip():
        return DEFAULT_ROLL_YEAR
    try:
        return int(raw)
    except ValueError:
        raise RoleError(f"{ROLL_YEAR_VAR}={raw!r} is not a year") from None


def filename_for(roll_year: int) -> str:
    """Name of the zipped GeoPackage for a fiscal year."""
    return f"ROLE{roll_year}_GEOPACKAGE.zip"


def municipality_filter(codes: list[str] | tuple[str, ...]) -> str | None:
    """An OGR attribute filter selecting ``codes``, or None for all of them.

    The codes reach OGR as SQL, so they are checked against the five-digit
    shape the column actually holds rather than quoted and hoped for.
    """
    cleaned = [str(code).strip() for code in codes if str(code).strip()]
    if not cleaned:
        return None
    for code in cleaned:
        if not _VALID_CODE_MUN.match(code):
            raise RoleError(
                f"{code!r} is not a five-digit municipality code; `code_mun` "
                "holds values like '66023' (Ville de Montréal)."
            )
    quoted = ", ".join(f"'{code}'" for code in cleaned)
    return f"code_mun IN ({quoted})"


class RoleFetcher:
    """Downloads and unpacks the archive, with an on-disk cache keyed by name."""

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
            allowed_methods=("GET",),
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers["User-Agent"] = USER_AGENT
        return session

    def cache_path(self, filename: str) -> Path:
        return self.cache_dir / filename

    def fetch(self, filename: str) -> Path:
        """Download ``filename`` (or reuse the cache), returning its local path.

        Streamed to a `.part` file and renamed on success, unlike
        `BdoiFetcher.fetch` which reads the whole body into memory: this is
        half a gigabyte, and a download killed halfway must not leave a
        truncated archive that the next run treats as cached.
        """
        cached = self.cache_path(filename)
        if cached.exists() and cached.stat().st_size:
            return cached

        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        url = f"{self.base_url}/{filename}"
        partial = cached.with_suffix(cached.suffix + ".part")
        cached.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._session.get(
                url, timeout=self.timeout_seconds, stream=True
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
                            # the first bytes rather than after the download,
                            # so half a gigabyte of HTML is not written first.
                            raise RoleError(
                                f"{url}: not a zip (Content-Type "
                                f"{content_type!r})"
                            )
                        first = False
                        handle.write(chunk)
        except requests.RequestException as exc:
            partial.unlink(missing_ok=True)
            raise RoleError(f"{url}: {exc}") from exc
        except BaseException:
            partial.unlink(missing_ok=True)
            raise

        if not partial.stat().st_size:
            partial.unlink(missing_ok=True)
            raise RoleError(f"{url}: the server returned an empty body")
        partial.replace(cached)
        return cached

    def geopackage(self, filename: str) -> Path:
        """The archive's GeoPackage, unpacked beside it and cached.

        Unpacked rather than read through `zip://`, for the reason the module
        docstring gives: a GeoPackage is SQLite, and SQLite seeks.
        """
        archive = self.fetch(filename)
        member = _geopackage_member(archive)
        unpacked = self.cache_dir / Path(member).name
        if unpacked.exists() and unpacked.stat().st_size:
            return unpacked

        partial = unpacked.with_suffix(unpacked.suffix + ".part")
        try:
            with zipfile.ZipFile(archive) as zf, zf.open(member) as src:
                with open(partial, "wb") as handle:
                    shutil.copyfileobj(src, handle, 1 << 24)
        except (zipfile.BadZipFile, OSError) as exc:
            partial.unlink(missing_ok=True)
            raise RoleError(
                f"{archive}: {member} could not be unpacked ({exc})"
            ) from exc
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        partial.replace(unpacked)
        return unpacked


def _geopackage_member(path: Path | str) -> str:
    """Name of the single `.gpkg` inside the archive.

    Named rather than guessed at a fixed path: the archive also ships an Excel
    codebook, a PDF of the attribute structure and a directory of dBASE value
    domains, and the folder they all sit in carries the roll year.
    """
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
    except zipfile.BadZipFile as exc:
        raise RoleError(f"{path}: not a readable zip ({exc})") from exc
    members = [name for name in names if name.lower().endswith(".gpkg")]
    if not members:
        raise RoleError(f"{path}: no .gpkg member (contains {names})")
    if len(members) > 1:
        raise RoleError(f"{path}: expected one .gpkg, got {members}")
    return members[0]


def layer_names(path: Path | str) -> tuple[str, ...]:
    """Every layer in the GeoPackage, in the order it lists them."""
    try:
        return tuple(str(row[0]) for row in pyogrio.list_layers(str(path)))
    except Exception as exc:  # pyogrio raises its own error types
        raise RoleError(f"{path}: not readable as a GeoPackage ({exc})") from exc


def layer_named(path: Path | str, prefix: str) -> str:
    """The one layer whose name is ``prefix`` or ``prefix``+``_<year>``.

    Resolved rather than hard-coded because the roll year is part of every
    layer name, and matched on the whole prefix so `b05v_unite_evaln` does not
    also claim `b05v_adr_unite_evaln`.
    """
    published = layer_names(path)
    matches = [
        name
        for name in published
        if name == prefix or name.startswith(f"{prefix}_")
    ]
    if not matches:
        raise RoleError(
            f"{path} publishes no layer named {prefix!r}; it has: "
            f"{', '.join(published)}"
        )
    if len(matches) > 1:
        raise RoleError(
            f"{path} publishes {len(matches)} layers matching {prefix!r}: "
            f"{', '.join(matches)}"
        )
    return matches[0]


def read_layer(
    path: Path | str,
    layer: str,
    *,
    where: str | None = None,
    geometry: bool = True,
) -> gpd.GeoDataFrame | pd.DataFrame:
    """One layer of the GeoPackage, filtered server side by ``where``.

    ``where`` is handed to OGR as an attribute filter rather than applied to a
    frame afterwards, so the rows outside it are never built: the province's
    3.7 million assessment units are ten times Montreal's, and reading them to
    throw them away is the difference between a few hundred megabytes of memory
    and a few gigabytes.

    Geometry comes back reprojected to `WGS84`, the CRS every other layer in
    this platform is stored and joined in - the same thing
    `urban_rag.bdoi.read_shapefile_zip` does for BDOI's province-wide extracts,
    and for the same reason. `PUBLISHED_CRS` is NAD83, which is close enough to
    WGS 84 to look right on a map and far enough to put a point on the wrong
    side of a lot line.
    """
    try:
        frame = pyogrio.read_dataframe(
            str(path), layer=layer, where=where, read_geometry=geometry
        )
    except Exception as exc:  # pyogrio raises its own error types
        raise RoleError(f"{path}: layer {layer!r} not readable ({exc})") from exc

    if not geometry:
        return frame
    if frame.crs is None:
        raise RoleError(f"{path}: layer {layer!r} carries no CRS")
    return frame.to_crs(WGS84)
