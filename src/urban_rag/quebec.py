"""Quebec City's zoning: the ArcGIS layer that draws it and the grid that states it.

Montreal publishes a zone as a Spectrum table row carrying a link to the PDF
grid that states its norms, and `urban_rag.zoning_grid` reads that PDF. Quebec
City publishes the same two things apart and in the open:

* **The zones**, as ``Zonage en vigueur`` - layer 2 of the
  ``CI_AMENAGEMENT_ENVIRONNEMENT`` feature service on the city's ArcGIS Online
  organisation, which is what the city's own interactive map draws
  (https://experience.arcgis.com/experience/aadd65187ef64037bd13170afb450e15).
  One polygon per zone, 4,789 city-wide, in NAD83 / MTM zone 7, carrying the
  zone code (``IGDS_TEXT_STRING``, e.g. ``11005Mb``), its status and its
  nature and nothing else.
* **The grid**, as one workbook for the whole city
  (https://www.donneesquebec.ca/recherche/dataset/grille-de-specifications-du-zonage):
  one row per zone, 300-odd columns, stating which usage groups are
  authorised and where in the building, the building's heights and storeys,
  the margins, the minimum site coverage and green area, and the dwelling
  density. The join key is the zone code, and the 4,789 rows match the 4,789
  polygons one for one.

The zone code's leading digit is the arrondissement (``1`` is La
Cité-Limoilou), which is why a borough's zones can be recognised without a
spatial test; the fetch is still bounded by the borough outline, the way every
other borough-scoped read here is, so a zone straddling the line is in both
partitions.

`QuebecZoningClient` reads the layer the way `urban_rag.infolot.InfolotClient`
reads the cadastre - ids inside a geometry first, then features by id in
batches - because the two are the same kind of service and the same failure
mode (a paged read that silently truncates) is the one to avoid.
`read_zoning_grid` turns the workbook into a frame with one row per zone and
one column per header cell, named by the header's own text. `grid_columns`
then reads one zone's row into the `GridColumn` objects the envelope assets
already consume, which is where Quebec City's vocabulary is translated into
this platform's - see there for what is translated faithfully and what is
approximated.

Deliberately free of Dagster imports, like the other publisher clients.
"""

from __future__ import annotations

import io
import json
import math
import re
import time
import unicodedata
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from urban_rag.program import BuildingLevel
from urban_rag.spectrum import USER_AGENT, default_ca_bundle
from urban_rag.zoning_grid import GridColumn

#: The zoning polygons, as the city's interactive map draws them.
DEFAULT_ZONING_LAYER_URL = (
    "https://services1.arcgis.com/4GCvRJNX6LNyFVQ0/arcgis/rest/services/"
    "CI_AMENAGEMENT_ENVIRONNEMENT/FeatureServer/2"
)

#: The specification grid, one workbook for the whole city, refreshed by the
#: city on its own cadence (the first sheet's title carries the date).
DEFAULT_GRID_URL = "https://carte.ville.quebec.qc.ca/DonneesOuvertes/vdq-zonage-grille.xlsx"

#: And the same norms a second time, one *grille de spécifications* PDF per
#: zone, generated on demand by the city's map server from the zone code.
#:
#: The workbook above and this sheet are not alternatives. The workbook is the
#: machine-readable one - `grid_columns` turns a row into the norms the solver
#: needs - and it is silent about everything a by-law states in prose: the
#: notes at the foot of a grid, the conditional usages, the PIIA and heritage
#: mentions. That prose is what the corpus is for, and it exists only here.
#:
#: **A zone code the handler does not know answers 200 with an 847-byte blank
#: PDF rather than a 404** (a real sheet is 110-125 KB). Nothing in the
#: response says so, which is why this template is only ever applied to a code
#: that came off the zoning layer itself. A blank one still costs nothing
#: downstream: it carries no text layer, so `rag.documents.read_pdf` refuses it
#: and `linked_documents` files it under `failures` like any dead link.
DEFAULT_SHEET_URL_TEMPLATE = (
    "https://carte.ville.quebec.qc.ca/GrillesZonage/HandlerZonage.ashx?{zone}"
)

#: Where the sheet's URL is written on a zone row. Montreal's zone table names
#: the column and Saguenay already borrowed the name; a third city spelling it
#: the same way is what lets one corpus pipeline serve all three - see
#: `rag.documents.DOCUMENT_SOURCES`.
GRID_URL_COLUMN = "LIEN_GRILLE"

#: The layer's zone code, which is also the grid's first column. Spelled the
#: way the service spells it; the Données Québec GeoJSON of the same layer
#: calls it ``ID``.
ZONE_CODE_FIELD = "IGDS_TEXT_STRING"

#: The layer's OID, which is what ``returnIdsOnly`` hands back.
OBJECT_ID_FIELD = "OBJECTID"

#: File slugs the two snapshots are written under, in the shape
#: `frames.table_slug` gives a Spectrum table - `<folder>__<TABLE>` - so they
#: sit beside the Montreal layers as peers rather than as a special case.
ZONING_SLUG = "Zonage__ZONAGE_EN_VIGUEUR"
GRID_SLUG = "Zonage__GRILLE_SPECIFICATIONS"

#: The grid's own column for the zone code, and the names this module gives
#: the columns it reads. Everything else keeps the header cell's text.
GRID_ZONE_COLUMN = "zone"

#: Rows per ``objectIds`` batch. The service caps a response at 2,000; a whole
#: borough is under a thousand zones.
DEFAULT_BATCH_SIZE = 200

WGS84 = 4326

#: Metres of building per storey where the grid states a height in metres and
#: no storey count. Read off the grid itself: of the 1,640 zones stating both,
#: the city pairs 9 m with 2 storeys, 12-13 m with 3, 15-16 m with 4, 21-22 m
#: with 6 and 26 m with 8, and ``floor(height / 3.5)`` reproduces every one
#: of those but the last (7 for 8). A third of the residential zones in La
#: Cité-Limoilou state no storey count, so without this they would have no
#: ceiling and no envelope.
STOREY_HEIGHT_M = 3.5

#: Where a usage may go in the building, as the grid's *Localisation* codes
#: spell it: ``S`` the basement, ``R`` the ground floor, ``R+`` the ground
#: floor and everything above, ``1`` the floor above the ground floor, ``1+``
#: that floor and everything above, ``2``/``2+``/``3`` and so on the floors
#: further up. A blank is no restriction.
#:
#: This platform's `BuildingLevel` has five rows, Montreal's, and two of these
#: codes have no exact row: ``2+`` means "the second floor above ground and
#: up", one floor narrower than *Tous sauf le RDC*. It is read as that row
#: and the column carries a note, which overstates the housing floors of a
#: mixed zone by one at most - the ground floor is still kept for the
#: commerce the same grid puts there.
_LEVEL_CODES: dict[str, frozenset[BuildingLevel]] = {
    "S": frozenset({BuildingLevel.BELOW_GROUND}),
    "R": frozenset({BuildingLevel.GROUND}),
    "R+": frozenset({BuildingLevel.ALL}),
    "1": frozenset({BuildingLevel.SECOND}),
    "1+": frozenset({BuildingLevel.ALL_EXCEPT_GROUND}),
}

#: The grid's usage groups, by the family this platform prices them under.
#: ``H`` is *Habitation* (H1 dwellings, H2 with community services, H3
#: rooming houses, H4 collective housing), ``C`` the commerce groups, ``I``
#: industry, and ``E`` the public (``P``) and outdoor recreation (``R``)
#: groups - equipment in Montreal's alphabet, and not priced by this
#: platform either. Agriculture and forest are read and reported, and priced
#: by nothing.
_USAGE_FAMILIES: dict[str, tuple[str, ...]] = {
    "H": ("H1", "H2", "H3", "H4"),
    "C": (
        "C1", "C2", "C3", "C4", "C5", "C10", "C11", "C12", "C13", "C14",
        "C20", "C21", "C30", "C31", "C32", "C33", "C34", "C35", "C36", "C37",
        "C38", "C40", "C41",
    ),
    "I": ("I1", "I2", "I3", "I4", "I5"),
    "E": ("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "R1", "R2", "R3", "R4"),
    "A": ("A1", "A2", "A3", "F1", "F2"),
}

#: The `envelope_assets.USAGE_CATEGORIES` name each family's groups are
#: reported under.
_FAMILY_CATEGORY: dict[str, str] = {
    "H": "habitation",
    "C": "commerce",
    "I": "industrie",
    "E": "equipements",
}

#: Groups whose *Localisation* row is read: the family's floors are the floors
#: its first authorised group may occupy. H1 is the dwelling group and the
#: one the solver prices; the others are read in order when H1 is absent.
_NUMBER = re.compile(r"^-?\d+(?:[.,]\d+)?$")

#: Header rows above the data in the workbook's first sheet: a title row, the
#: usage-class row, the usage-group row, then the column names.
_HEADER_ROWS = 4


class QuebecZoningError(RuntimeError):
    """The zoning layer or the grid could not be read."""


# ---------------------------------------------------------------------------
# the zones
# ---------------------------------------------------------------------------


class QuebecZoningClient:
    """Two-phase reader over the ``Zonage en vigueur`` layer."""

    def __init__(
        self,
        layer_url: str = DEFAULT_ZONING_LAYER_URL,
        *,
        grid_url: str = DEFAULT_GRID_URL,
        sheet_url_template: str = DEFAULT_SHEET_URL_TEMPLATE,
        timeout_seconds: float = 60.0,
        request_delay_seconds: float = 0.25,
        max_retries: int = 3,
        batch_size: int = DEFAULT_BATCH_SIZE,
        ca_bundle: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.layer_url = layer_url.rstrip("/")
        self.grid_url = grid_url
        self.sheet_url_template = sheet_url_template
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
        session.headers["User-Agent"] = USER_AGENT
        return session

    @property
    def query_url(self) -> str:
        return f"{self.layer_url}/query"

    def _post(self, data: dict[str, Any]) -> dict:
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        response = self._session.post(
            self.query_url, data=data, timeout=self.timeout_seconds
        )
        if "json" not in response.headers.get("Content-Type", ""):
            raise QuebecZoningError(
                f"Non-JSON response ({response.status_code}) from "
                f"{self.query_url}: {response.text[:300]!r}"
            )
        payload = response.json()
        if isinstance(payload, dict) and "error" in payload:
            error = payload["error"]
            raise QuebecZoningError(
                f"{self.query_url}: {error.get('message') or error} "
                f"{'; '.join(error.get('details') or [])}".strip()
            )
        return payload

    def layer_metadata(self) -> dict:
        """The layer's own description: fields, extent, edit dates."""
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        response = self._session.get(
            self.layer_url, params={"f": "json"}, timeout=self.timeout_seconds
        )
        payload = response.json()
        if "error" in payload:
            raise QuebecZoningError(f"{self.layer_url}: {payload['error']}")
        return payload

    def zone_ids(self, geometry: dict, *, in_srs: int = WGS84) -> list[int]:
        """Object ids of every zone intersecting ``geometry`` (Esri JSON)."""
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

    def fetch_zones(
        self, object_ids: Iterable[int], *, target_srs: int = WGS84
    ) -> Iterator[dict]:
        """Yield GeoJSON features for ``object_ids``, reprojected by the service."""
        ids = list(object_ids)
        for start in range(0, len(ids), self.batch_size):
            batch = ids[start : start + self.batch_size]
            payload = self._post(
                {
                    "objectIds": ",".join(str(i) for i in batch),
                    "where": "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "outSR": str(target_srs),
                    "f": "geojson",
                }
            )
            features = payload.get("features") or []
            if len(features) != len(batch):
                raise QuebecZoningError(
                    f"Asked for {len(batch)} zones by id, got {len(features)}; "
                    "the service dropped rows from the batch."
                )
            yield from features

    def sheet_url_for(self, zone_code: str) -> str:
        """The published URL of one zone's grid sheet.

        Built, not looked up: unlike Saguenay's, this handler is keyed on the
        zone code the polygons already carry, so there is no id to resolve and
        no request to spend. Nothing here validates the code - see
        `DEFAULT_SHEET_URL_TEMPLATE` on what an unknown one answers.
        """
        return self.sheet_url_template.format(zone=str(zone_code).strip())

    def fetch_grid(self) -> bytes:
        """The specification workbook, as published."""
        if self.request_delay_seconds:
            time.sleep(self.request_delay_seconds)
        try:
            response = self._session.get(self.grid_url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise QuebecZoningError(f"{self.grid_url}: {exc}") from exc
        content = response.content
        if not content.startswith(b"PK"):
            raise QuebecZoningError(
                f"{self.grid_url}: not an xlsx (Content-Type "
                f"{response.headers.get('Content-Type')!r})"
            )
        return content


# ---------------------------------------------------------------------------
# the grid
# ---------------------------------------------------------------------------


def normalize_label(text: object) -> str:
    """A header cell or a value, comparable: unaccented, lower, one-spaced.

    The workbook's headers carry line breaks, doubled spaces and the accents
    the terminal this was written on could not always show, so nothing here
    is matched on the raw text.
    """
    if text is None:
        return ""
    flat = unicodedata.normalize("NFKD", str(text))
    flat = "".join(ch for ch in flat if not unicodedata.combining(ch))
    flat = flat.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", flat).strip().lower()


def read_zoning_grid(content: bytes) -> pd.DataFrame:
    """The workbook -> one row per zone, one column per header cell.

    Columns are named by the header cell's own text, with the *group* the
    workbook prints above it prefixed (``Normes d'implantation générales:
    Marge avant (m)``) wherever the bare name is printed more than once - the
    building's ``Hauteur max. (m)`` is stated twice, once generally and once
    for particular types, and the two are two columns. The usage columns
    (``H1 autorisé``, ``C2 Localisation``) are unique already and keep their
    bare names. `GRID_ZONE_COLUMN` is the zone code.

    Values are kept as the cells hold them - strings mostly, a few numbers
    and dates - so the bronze snapshot is the workbook; `grid_columns` is
    where they are read.
    """
    try:
        import openpyxl
    except ImportError as exc:  # pragma: no cover - dependency of the package
        raise QuebecZoningError("openpyxl is required to read the grid") from exc

    workbook = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    rows = sheet.iter_rows(values_only=True)
    try:
        title, groups, _subgroups, header = (next(rows) for _ in range(_HEADER_ROWS))
    except StopIteration as exc:
        raise QuebecZoningError("The grid workbook has no header rows") from exc

    names = _column_names(groups, header)
    if normalize_label(header[0]) != "zone a modifier":
        raise QuebecZoningError(
            f"The grid's first column is {header[0]!r}, not the zone code"
        )

    records: list[dict] = []
    for row in rows:
        code = row[0] if row else None
        if code is None or str(code).strip() == "":
            break
        record = {GRID_ZONE_COLUMN: str(code).strip()}
        for name, value in zip(names[1:], row[1:]):
            if name is None:
                continue
            record[name] = _cell(value)
        records.append(record)
    if not records:
        raise QuebecZoningError("The grid workbook holds no zone rows")

    frame = pd.DataFrame.from_records(records)
    frame.attrs["title"] = str(title[0]) if title and title[0] else ""
    duplicates = frame[GRID_ZONE_COLUMN][frame[GRID_ZONE_COLUMN].duplicated()]
    if not duplicates.empty:
        raise QuebecZoningError(
            f"The grid states {duplicates.nunique()} zone(s) more than once, "
            f"e.g. {', '.join(duplicates.astype(str).head(5))}"
        )
    return frame


def _column_names(groups: tuple, header: tuple) -> list[str | None]:
    """Header text per column, group-prefixed where the bare text repeats."""
    bare = [_flat(cell) for cell in header]
    counts: dict[str, int] = {}
    for name in bare:
        if name:
            counts[name] = counts.get(name, 0) + 1
    names: list[str | None] = []
    group = ""
    for index, name in enumerate(bare):
        if index < len(groups) and groups[index]:
            group = _flat(groups[index])
        if not name:
            names.append(None)
        elif counts[name] > 1 and group:
            names.append(f"{group}: {name}")
        else:
            names.append(name)
    return names


def _flat(cell: object) -> str:
    return re.sub(r"\s+", " ", str(cell)).strip() if cell is not None else ""


def _empty(value: object) -> bool:
    """Whether a cell holds nothing: None, blank, or the NaN pandas makes of None."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return isinstance(value, str) and not value.strip()


def _cell(value: object):
    if _empty(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return value


# ---------------------------------------------------------------------------
# a zone's row -> grid columns
# ---------------------------------------------------------------------------


def grid_columns(row: Mapping[str, Any]) -> list[GridColumn]:
    """One zone's grid row, as the columns the envelope assets read.

    One column per usage *family* the zone authorises - dwellings, commerce,
    industry, equipment - each carrying the zone's one set of building norms
    and the floors the grid lets that family occupy. That is the shape a
    Montreal grid prints a mixed zone in (commerce at the RDC in one column,
    housing above in the next) and the shape `select_governing_column`
    expects, so a Quebec zone permitting C2 on the ground floor and H1 from
    the second up becomes two envelopes rather than one column whose levels
    would have to mean two things.

    What is translated, and how faithfully:

    * **Usages.** The families are emitted as the bare codes the solver's
      matchers read - ``H``, ``C``, ``I``, ``E`` - and the groups behind them
      (``H1, H2``) are carried in ``usages_by_category``. Quebec's groups do
      not map onto Montreal's numbered classes, so no class ceiling applies
      and the dwelling cap comes from the grid's own *nb max. logement par
      bâtiment*, the largest of the isolé / jumelé / rangée figures.
    * **Storeys.** *Nombre d'étages max.* where stated; otherwise
      ``floor(height_max_m / STOREY_HEIGHT_M)``, noted on the column. The
      height itself is carried as printed.
    * **Levels.** From the family's *Localisation* codes - see `_LEVEL_CODES`
      for the one approximation.
    * **Site coverage.** *POS min.* is the minimum. The grid states no
      maximum; ``100 - aire verte min.`` is carried as one where a green area
      is required, noted, since a building cannot stand on ground the by-law
      keeps green.
    * **Margins, lot width, implantation.** The three margins as printed; the
      lot's *Largeur min.* under *Dimensions générales*; the implantation
      mode read off which H1 types (isolé / jumelé / rangée) are given a
      dwelling count, in the ``I-J-C`` letters `postgis` already parses.
    * **Not translated.** The dwelling density (*log/ha*) - a per-hectare
      figure, not a floor-area ratio, so it does not go in ``density_max``;
      the particular dimensions and norms stated per building type; the
      PDAD code. Each is noted where present.
    """
    lookup = _Lookup(row)
    zone = lookup.text("zone a modifier") or lookup.text(GRID_ZONE_COLUMN)
    notes: list[str] = []
    norms = _norms(lookup, notes)
    zone_fields = _zone_fields(lookup)

    columns: list[GridColumn] = []
    for family, groups in _USAGE_FAMILIES.items():
        authorised = [group for group in groups if lookup.text(f"{group} autorise")]
        if not authorised or family == "A":
            continue
        levels, level_note = _levels(lookup, authorised)
        family_notes = list(notes)
        if level_note:
            family_notes.append(level_note)
        columns.append(
            GridColumn(
                zone=zone,
                column_index=len(columns),
                usages=(family,),
                usages_by_category={_FAMILY_CATEGORY[family]: ", ".join(authorised)},
                levels=levels,
                **norms,
                **zone_fields,
                notes=tuple(family_notes),
            )
        )
    return columns


class _Lookup:
    """A grid row, read by normalised header text."""

    def __init__(self, row: Mapping[str, Any]) -> None:
        self._by_label = {normalize_label(key): value for key, value in row.items()}

    def raw(self, *labels: str):
        """The first non-empty value under any of ``labels``.

        A label may name the column bare (``hauteur max. (m)``) or with its
        group (``dimension du batiment principal - dimensions generales:
        hauteur max. (m)``); the bare form matches the first column whose
        normalised name ends with it, which for the doubled norms is the
        *general* one - the particular block is printed after it.
        """
        for label in labels:
            wanted = normalize_label(label)
            if wanted in self._by_label:
                value = self._by_label[wanted]
                if not _empty(value):
                    return value
            for key, value in self._by_label.items():
                if key.endswith(": " + wanted) and not _empty(value):
                    return value
        return None

    def text(self, *labels: str) -> str | None:
        value = self.raw(*labels)
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def number(self, *labels: str) -> float | None:
        value = self.raw(*labels)
        if value is None:
            return None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return None if isinstance(value, float) and math.isnan(value) else float(value)
        text = str(value).strip().replace(",", ".")
        return float(text) if _NUMBER.match(text) else None


def _norms(lookup: _Lookup, notes: list[str]) -> dict:
    height_min = lookup.number("dimension du batiment principal - dimensions generales: hauteur min. (m)", "hauteur min. (m)")
    height_max = lookup.number("dimension du batiment principal - dimensions generales: hauteur max. (m)", "hauteur max. (m)")
    floors_min = lookup.number("dimension du batiment principal - dimensions generales: nombre d'etages min.", "nombre d'etages min.")
    floors_max = lookup.number("dimension du batiment principal - dimensions generales: nombre d'etages max.", "nombre d'etages max.")
    if floors_max is None and height_max:
        floors_max = float(max(1, math.floor(height_max / STOREY_HEIGHT_M)))
        notes.append(
            f"floors_max: derived from Hauteur max. {height_max:g} m at "
            f"{STOREY_HEIGHT_M} m per storey"
        )

    coverage_min = lookup.number("normes d'implantation generales: pos min. (%)", "pos min. (%)")
    green_min = lookup.number("normes d'implantation generales: aire verte min. (%)", "aire verte min. (%)")
    coverage_max = None
    if green_min is not None and green_min > 0:
        coverage_max = 100.0 - green_min
        notes.append(f"site_coverage_max_pct: 100 - aire verte min. {green_min:g}%")

    density_max = lookup.number("nb de log. a l'hectare max. (log/ha)")
    if density_max:
        notes.append(f"dwelling density: {density_max:g} log/ha max, not carried (per hectare, not a floor-area ratio)")
    if lookup.text("dimension du batiment principal - dimensions particulieres: groupe + type + log. ou ch. min. + log. ou ch. max."):
        notes.append("particular building dimensions stated per type: not carried")
    if lookup.text("normes d'implantation particulieres: groupe + type + log. ou ch. min. + log. ou ch. max."):
        notes.append("particular implantation norms stated per type: not carried")

    dwellings = [
        lookup.number(f"h1 {kind} nb max. logement par batiment")
        for kind in ("isole", "jumele", "rangee")
    ]
    stated = [value for value in dwellings if value]
    max_dwellings = int(max(stated)) if stated else None

    modes = [
        letter
        for letter, kind in (("I", "isole"), ("J", "jumele"), ("C", "rangee"))
        if _type_permitted(lookup, kind)
    ]
    implantation = "-".join(modes) if modes else None

    return {
        "floors_min": int(floors_min) if floors_min is not None else None,
        "floors_max": int(floors_max) if floors_max is not None else None,
        "height_min_m": height_min,
        "height_max_m": height_max,
        "min_lot_width_m": lookup.number("dimensions generales: largeur min.(m)", "largeur min.(m)"),
        "implantation_mode": implantation,
        "site_coverage_min_pct": coverage_min,
        "site_coverage_max_pct": coverage_max,
        "density_min": None,
        "density_max": None,
        "max_dwellings": max_dwellings,
        "specific_use_area_max_m2": lookup.number(
            "sup. max. de plancher vente au detail par batiment (m²)",
            "sup. max. de plancher vente au detail par batiment (m2)",
        ),
        "front_margin_min_m": lookup.number("normes d'implantation generales: marge avant (m)", "marge avant (m)"),
        "front_margin_max_m": None,
        "secondary_front_margin_min_m": None,
        "secondary_front_margin_max_m": None,
        "side_margin_min_m": lookup.number("normes d'implantation generales: marge laterale (m)", "marge laterale (m)"),
        "rear_margin_min_m": lookup.number("normes d'implantation generales: marge arriere (m)", "marge arriere (m)"),
        "only_permitted_usages": lookup.text("usage specifiquement autorise"),
        "excluded_usages": lookup.text("usage specifiquement exclu"),
    }


def _type_permitted(lookup: _Lookup, kind: str) -> bool:
    """Whether H1 buildings of ``kind`` are given a dwelling count.

    A type the grid prints no count for is not authorised in that form; a
    stated ``0`` maximum is an explicit refusal (``11016Hb`` prints isolé
    0-0 and jumelé 1-12).
    """
    low = lookup.number(f"h1 {kind} nb min. logement par batiment")
    high = lookup.number(f"h1 {kind} nb max. logement par batiment")
    if high is not None:
        return high > 0
    return low is not None and low > 0


def _levels(
    lookup: _Lookup, groups: Iterable[str]
) -> tuple[frozenset[BuildingLevel], str | None]:
    """The floors a family may occupy, from the first group stating any."""
    for group in groups:
        code = lookup.text(f"{group} localisation")
        if code:
            return _parse_levels(code, group)
    return frozenset({BuildingLevel.ALL}), None


def _parse_levels(code: str, group: str) -> tuple[frozenset[BuildingLevel], str | None]:
    levels: set[BuildingLevel] = set()
    approximated: list[str] = []
    for token in (part.strip().upper() for part in code.split(",")):
        if not token:
            continue
        if token in _LEVEL_CODES:
            levels |= _LEVEL_CODES[token]
        elif re.match(r"^\d+(\+| A \d+)?$", token):
            # Any floor above the first above ground: narrower than "all but
            # the RDC" by the floors it skips, which is the note.
            levels.add(BuildingLevel.ALL_EXCEPT_GROUND)
            approximated.append(token)
        else:
            approximated.append(token)
    if not levels:
        levels = {BuildingLevel.ALL}
    note = (
        f"levels: {group} Localisation {code!r} read as "
        f"{', '.join(sorted(str(level) for level in levels))}"
        if approximated
        else None
    )
    return frozenset(levels), note


def _zone_fields(lookup: _Lookup) -> dict:
    articles = [
        text
        for text in (
            lookup.text("autres dispositions particulieres"),
            lookup.text("zonage a competence ville"),
        )
        if text
    ]
    return {
        "heritage_sector": lookup.text("arrondissement historique"),
        "piia_sector": lookup.text("piia"),
        "pae": lookup.text("pae"),
        "specific_articles": "; ".join(articles) if articles else None,
    }


def borough_prefix(zone_code: str) -> str | None:
    """The arrondissement digit a zone code starts with, or None."""
    return zone_code[0] if zone_code and zone_code[0].isdigit() else None
