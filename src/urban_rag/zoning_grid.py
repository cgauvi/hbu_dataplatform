"""Read a *grille des usages et des normes* back out of its published PDF.

`urban_rag.program` solves an envelope and says plainly what it is missing: it
takes a `ZoneColumn` "already parsed out of a zoning grid, not a PDF", and
nothing produced one. This module is that parser. `linked_documents` flattens
the same PDFs to text for the embedding corpus and stops there, and the
flattened text is not recoverable into a grid - `pypdf`'s default extraction
concatenates a row's cells in drawing order, so ``Tous sauf le RDC   X`` says
that *some* column is authorised above the ground floor and not which one.

**The x-coordinate is the entire content of a grid.** A grille is a table with
no ruling lines in its text layer: the row labels run down the left, and each
column of norms is a band of right-aligned cells at a fixed horizontal offset.
Which band a cell falls in is the only thing that attaches ``H`` to a storey
maximum of 6 rather than to the ``C.4`` beside it. `extraction_mode="layout"`
is what keeps that: it pads each line with spaces in proportion to the gaps on
the page, so a character offset in the extracted line is a usable proxy for a
point on it, and the parsing problem becomes one of clustering offsets.

**Columns are found, not assumed.** A grid carries one to four of them and
prints no count anywhere. The offsets are recovered from the rows of the
*CADRE BATI* block - height, storeys, lot width, implantation mode, coverage,
density, the four margins - because those are the rows a grid fills in for
every column it has, including with ``-``. Their cells' *right* edges cluster
into as many groups as the grid has columns, and every other cell on the page
is then attributed to whichever cluster its own right edge lands in. Cells
that land in none - the ``min/max (m)`` unit captions to the left of the band,
the full-width footnotes below it - are dropped, which is what makes those
captions harmless rather than something to strip by name.

**What is not a norm.** A grid prints ``-`` wherever a norm does not apply,
and `ZoneColumn` is built to hold that distinction: an absent minimum is
``None`` and not ``0``, because reading ``Densite min/max -`` as ``0/0``
forbids building at all. Every field here is therefore optional, and
`GridColumn.to_zone_column` refuses on the one the solver cannot do without -
the storey maximum, which is what bounds the envelope.

**This module reads; it does not judge.** A cell it cannot make a number of is
recorded in `GridColumn.notes` with the text that produced it, and the column
is returned with that field unset rather than dropped. Municipal PDFs are
heterogeneous enough that a parser which raises on the first surprise reports
nothing about the hundreds it could read.
"""

from __future__ import annotations

import io
import re
import unicodedata
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field

from urban_rag.program import BuildingLevel, ZoneColumn, is_residential_usage

#: The title every grid page carries, normalized. A PDF linked from
#: ``LIEN_GRILLE`` is not always a grid - the same column has carried
#: P.P.C.M.O.I. resolutions - so the title is what decides whether a page is
#: one.
GRID_TITLE = "grille des usages et des normes"

#: How far apart, in characters of layout-extracted text, two cells' right
#: edges may sit and still be the same column. Cells within one column vary by
#: a character or two (a ``-`` is drawn one place right of a ``50/70``); the
#: gap between adjacent columns is over twenty.
COLUMN_TOLERANCE = 4

#: How many of the *CADRE BATI* rows have to place a cell at an offset before
#: it counts as a column. Below this a stray full-width value - the lone ``-``
#: under *Articles vises* - would be read as a column of its own.
MIN_COLUMN_SUPPORT = 3

#: Cells split on runs of two or more spaces. One space is what separates the
#: words *inside* a cell (``min/max (m)``, ``I-J``), so splitting on any
#: whitespace would cut every caption into pieces.
_CELLS = re.compile(r"\S+(?: \S+)*")

#: The unit caption a row prints between its label and its values. It is kept
#: out of the offsets the column bands are computed from; on every other row it
#: is dropped by landing outside those bands rather than by this pattern.
_UNIT = re.compile(r"^(?:min|max|min/max)\b|^\(.+\)$")

#: A footnote marker, as printed after a value or a usage code. It qualifies
#: the norm rather than changing it, so it is stripped before a number is read.
_FOOTNOTE = re.compile(r"\(\s*\d+\s*\)")

_NUMBER = re.compile(r"^-?\d+(?:[.,]\d+)?$")

#: What a grid prints where a norm does not apply. Not zero - see the module
#: docstring, and `ZoneColumn`.
_ABSENT = {"-", "--", "s.o.", "so", "n/a", "na", ""}

#: The rows whose cells define the columns: every one of them is filled in for
#: every column a grid has, with ``-`` where the norm does not apply, so their
#: right edges are the grid's own column geometry.
_ANCHOR_LABELS = frozenset(
    {
        "en metre",
        "en etage",
        "largeur du terrain",
        "mode d'implantation",
        "taux d'implantation au sol",
        "densite",
        "avant principale",
        "avant secondaire",
        "laterale",
        "arriere",
        "pourcentage d'ouvertures",
        "pourcentage de maconnerie",
    }
)

#: The *Categories d'usages autorisees* rows, and the field each fills. The
#: code in a column under one of them is one of that column's usages;
#: `program.is_residential_usage` is what decides which of them authorise a
#: dwelling, so every category is carried rather than filtered here.
_USAGE_LABELS: Mapping[str, str] = {
    "habitation": "habitation",
    "commerce": "commerce",
    "industrie": "industrie",
    "equipements collectifs et institutionnels": "equipements",
}

#: The *Niveaux de batiment autorises* rows. A column marked ``X`` on one of
#: them may occupy the storeys it names; `program.permitted_floors` adds them
#: up.
_LEVEL_LABELS: Mapping[str, BuildingLevel] = {
    "rez-de-chaussee (rdc)": BuildingLevel.GROUND,
    "inferieurs au rdc": BuildingLevel.BELOW_GROUND,
    "immediatement superieur au rdc": BuildingLevel.SECOND,
    "tous sauf le rdc": BuildingLevel.ALL_EXCEPT_GROUND,
    "tous les niveaux": BuildingLevel.ALL,
}

#: The numeric rows, and what each cell in them means. ``pair`` is a printed
#: ``min/max``; ``min`` and ``max`` are the rows whose caption names one bound
#: only, so a lone ``3`` under *Laterale min (m)* is a minimum and a lone ``4``
#: under *Nombre de logements maximal* is a maximum.
_NUMERIC_LABELS: Mapping[str, tuple[str, tuple[str, ...]]] = {
    "en metre": ("pair", ("height_min_m", "height_max_m")),
    "en etage": ("pair", ("floors_min", "floors_max")),
    "largeur du terrain": ("min", ("min_lot_width_m",)),
    "taux d'implantation au sol": (
        "pair",
        ("site_coverage_min_pct", "site_coverage_max_pct"),
    ),
    "densite": ("pair", ("density_min", "density_max")),
    "nombre de logements maximal": ("max", ("max_dwellings",)),
    "superficie des usages specifiques": ("max", ("specific_use_area_max_m2",)),
    "avant principale": ("pair", ("front_margin_min_m", "front_margin_max_m")),
    "avant secondaire": (
        "pair",
        ("secondary_front_margin_min_m", "secondary_front_margin_max_m"),
    ),
    "laterale": ("min", ("side_margin_min_m",)),
    "arriere": ("min", ("rear_margin_min_m",)),
}

#: Rows kept as printed. Whether ``I-J`` is buildable on this parcel is a
#: question about the neighbours rather than about the lot, so the mode travels
#: as text for whoever asks instead of becoming a constraint here.
_TEXT_LABELS: Mapping[str, str] = {
    "mode d'implantation": "implantation_mode",
    "usages uniquement autorises": "only_permitted_usages",
    "usages exclus": "excluded_usages",
}

#: Fields the grid states as whole things. Kept apart from the floats so a
#: storey maximum reads as ``6`` and not ``6.0`` in the table this feeds.
_INTEGER_FIELDS = frozenset({"floors_min", "floors_max", "max_dwellings"})

#: Where the grid stops being a table of columns. Everything below *Patrimoine*
#: - the patrimonial sector, the discretionary by-laws, the amendment list - is
#: stated once for the zone rather than per column, and its full-width values
#: would be read as columns if the scan ran on.
_END_LABEL = "patrimoine"


class GridParseError(ValueError):
    """A page that does not yield a grid, or a column the solver cannot use."""


@dataclass(frozen=True)
class GridColumn:
    """One column of a grid, as printed.

    A superset of `ZoneColumn`: the solver uses the four caps that bound an
    envelope, and the rest - heights in metres, the four margins, the
    implantation mode - is carried because it is what someone reading a row of
    the table this feeds wants beside them, and because re-reading every PDF in
    the borough to add a column later is the expensive way to get it.
    """

    zone: str | None
    column_index: int
    #: Every usage code at the head of the column, in category order.
    usages: tuple[str, ...] = ()
    #: The same codes, keyed by the row each was printed on.
    usages_by_category: Mapping[str, str] = field(default_factory=dict)
    levels: frozenset[BuildingLevel] = frozenset()

    floors_min: int | None = None
    floors_max: int | None = None
    height_min_m: float | None = None
    height_max_m: float | None = None
    min_lot_width_m: float | None = None
    implantation_mode: str | None = None
    site_coverage_min_pct: float | None = None
    site_coverage_max_pct: float | None = None
    density_min: float | None = None
    density_max: float | None = None
    max_dwellings: int | None = None
    specific_use_area_max_m2: float | None = None
    front_margin_min_m: float | None = None
    front_margin_max_m: float | None = None
    secondary_front_margin_min_m: float | None = None
    secondary_front_margin_max_m: float | None = None
    side_margin_min_m: float | None = None
    rear_margin_min_m: float | None = None
    only_permitted_usages: str | None = None
    excluded_usages: str | None = None

    #: Cells that were printed but could not be read, as ``label: text``. A
    #: column with notes is still returned - see the module docstring.
    notes: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        """Whether the column heads no usage at all.

        A grid's rightmost band is sometimes ruled and left blank. It carries
        norms in no row and no usage code, and is a column of the drawing
        rather than of the by-law.
        """
        return not self.usages

    @property
    def permits_residential(self) -> bool:
        return any(is_residential_usage(usage) for usage in self.usages)

    def to_zone_column(self) -> ZoneColumn:
        """This column as the solver's input.

        Raises on the one field `solve_program` cannot be run without. A grid
        printing ``-`` for *En etage* states no storey ceiling, and an envelope
        with no ceiling on the one dimension the density and coverage caps are
        multiplied by is unbounded rather than generous.
        """
        if self.floors_max is None:
            raise GridParseError(
                f"zone {self.zone} column {self.column_index}: no storey "
                "maximum (En etage), so the envelope has no ceiling"
            )
        return ZoneColumn(
            usages=self.usages,
            floors_max=self.floors_max,
            levels=self.levels,
            floors_min=self.floors_min or 0,
            min_lot_width_m=self.min_lot_width_m,
            max_dwellings=self.max_dwellings,
            density_min=self.density_min,
            density_max=self.density_max,
            site_coverage_min_pct=self.site_coverage_min_pct,
            site_coverage_max_pct=self.site_coverage_max_pct,
            zone=self.zone,
        )


@dataclass(frozen=True)
class _Cell:
    """One cell of a layout-extracted line, with the offsets it spans.

    ``end`` is what identifies the column: the grid right-aligns its values, so
    a ``-`` and a ``50/70`` in the same column share a right edge and share
    nothing else.
    """

    start: int
    end: int
    text: str


def parse_grid_pdf(content: bytes, *, url: str | None = None) -> list[GridColumn]:
    """Every column of every grid page in ``content``.

    A linked PDF is not always a single grid: the zone table's ``LIEN_GRILLE``
    has pointed at multi-page extracts whose grid is not the first page, and an
    annex can hold several. Each page carrying the title is parsed on its own,
    so a page that is prose costs nothing and a page that is a grid is found
    wherever it sits.
    """
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    where = url or "<bytes>"
    try:
        reader = PdfReader(io.BytesIO(content))
        pages = [
            page.extract_text(extraction_mode="layout") or "" for page in reader.pages
        ]
    except (PdfReadError, ValueError, OSError) as exc:
        raise GridParseError(f"{where}: unreadable PDF ({exc})") from exc

    columns: list[GridColumn] = []
    for text in pages:
        if is_grid_page(text):
            columns.extend(parse_grid_page(text))
    if not columns:
        raise GridParseError(
            f"{where}: no page carries a {GRID_TITLE!r} with readable columns"
        )
    return columns


def is_grid_page(text: str) -> bool:
    """Whether ``text`` is a grid page rather than prose that mentions one.

    The title is looked for in the first few lines only. A P.P.C.M.O.I.
    resolution derogating "a la grille des usages et des normes" says the words
    in its body, and reading that as a grid would make columns out of its
    paragraph indents.
    """
    head = [line for line in text.split("\n") if line.strip()][:4]
    return any(GRID_TITLE in _normalize(line) for line in head)


def parse_grid_page(text: str) -> list[GridColumn]:
    """The columns of one grid page.

    Returns an empty list where the page has a title but no column band: a
    grid whose *CADRE BATI* block failed to extract is a page this cannot read,
    not a zone with no norms.
    """
    rows = [cells for cells in map(_cells, text.split("\n")) if cells]
    zone = _zone(rows)
    rows = list(_grid_block(rows))

    edges = _column_edges(rows)
    if not edges:
        return []

    fields: list[dict] = [
        {
            "zone": zone,
            "column_index": index,
            "usages_by_category": {},
            "levels": set(),
        }
        for index in range(len(edges))
    ]
    notes: list[list[str]] = [[] for _ in edges]

    for row in rows:
        label = _normalize(row[0].text)
        values = _by_column(row[1:], edges)
        if label in _USAGE_LABELS:
            for index, cell in values.items():
                if _normalize(cell.text) not in _ABSENT:
                    fields[index]["usages_by_category"][_USAGE_LABELS[label]] = (
                        cell.text.strip()
                    )
        elif label in _LEVEL_LABELS:
            for index, cell in values.items():
                if cell.text.strip().upper().startswith("X"):
                    fields[index]["levels"].add(_LEVEL_LABELS[label])
        elif label in _TEXT_LABELS:
            for index, cell in values.items():
                if _normalize(cell.text) not in _ABSENT:
                    fields[index][_TEXT_LABELS[label]] = cell.text.strip()
        elif label in _NUMERIC_LABELS:
            kind, names = _NUMERIC_LABELS[label]
            for index, cell in values.items():
                parsed, note = _numeric(cell.text, kind, names)
                fields[index].update(parsed)
                if note:
                    notes[index].append(f"{row[0].text.strip()}: {note}")

    return [
        GridColumn(
            **{
                **values,
                "usages": tuple(
                    values["usages_by_category"][category]
                    for category in _USAGE_LABELS.values()
                    if category in values["usages_by_category"]
                ),
                "usages_by_category": dict(values["usages_by_category"]),
                "levels": frozenset(values["levels"]),
                "notes": tuple(column_notes),
            }
        )
        for values, column_notes in zip(fields, notes)
    ]


def _cells(line: str) -> list[_Cell]:
    return [
        _Cell(match.start(), match.end(), match.group())
        for match in _CELLS.finditer(line)
    ]


def _zone(rows: Sequence[Sequence[_Cell]]) -> str | None:
    """The zone number printed beside ``ZONE :``.

    It is the id the map joins on - ``NUMERO_COMPLET`` in the feature table,
    ``feature_id`` in `rag.lot_features` - so a grid that does not print one
    can be parsed but cannot be attached to a parcel.
    """
    for row in rows:
        for index, cell in enumerate(row):
            if _normalize(cell.text).startswith("zone :") and index + 1 < len(row):
                return row[index + 1].text.strip()
    return None


def _grid_block(rows: Iterable[Sequence[_Cell]]) -> Iterator[Sequence[_Cell]]:
    """The rows above *Patrimoine* - see `_END_LABEL`."""
    for row in rows:
        if _normalize(row[0].text) == _END_LABEL:
            return
        yield row


def _column_edges(rows: Sequence[Sequence[_Cell]]) -> list[int]:
    """The right edge of each column, left to right.

    Built from `_ANCHOR_LABELS` only. Every other row on the page is attributed
    to these, which is what keeps a full-width value below the band - or a unit
    caption to the left of it - from inventing a column.
    """
    offsets = sorted(
        cell.end
        for row in rows
        if _normalize(row[0].text) in _ANCHOR_LABELS
        for cell in row[1:]
        if not _UNIT.match(cell.text)
    )
    if not offsets:
        return []

    clusters: list[list[int]] = [[offsets[0]]]
    for offset in offsets[1:]:
        if offset - clusters[-1][-1] <= COLUMN_TOLERANCE:
            clusters[-1].append(offset)
        else:
            clusters.append([offset])
    return [
        cluster[len(cluster) // 2]
        for cluster in clusters
        if len(cluster) >= MIN_COLUMN_SUPPORT
    ]


def _by_column(cells: Iterable[_Cell], edges: Sequence[int]) -> dict[int, _Cell]:
    """Attribute cells to columns by right edge; drop what lands in none.

    The last cell wins a collision, which happens only on a row whose unit
    caption itself right-aligns into the first column's band. A caption cannot
    be a value and a value can, so preferring the later of the two is
    preferring the one printed further right - the value.
    """
    placed: dict[int, _Cell] = {}
    for cell in cells:
        distances = [abs(cell.end - edge) for edge in edges]
        nearest = min(range(len(edges)), key=distances.__getitem__)
        if distances[nearest] <= COLUMN_TOLERANCE:
            placed[nearest] = cell
    return placed


def _numeric(
    text: str, kind: str, names: Sequence[str]
) -> tuple[dict[str, float | int], str | None]:
    """One cell of a numeric row, as the fields it fills and a note if not.

    ``-`` fills nothing, deliberately: an absent norm leaves the field at
    ``None``, which is what `ZoneColumn` reads as "no such norm" rather than as
    a norm of zero.
    """
    raw = _FOOTNOTE.sub("", text).strip()
    if _normalize(raw) in _ABSENT:
        return {}, None

    parts = [part.strip() for part in raw.split("/")]
    if kind == "pair" and len(parts) == 2:
        # Half a pair is a real thing to print: *Avant principale min/max (m)
        # 6/* in zone E01-003 is a three-metre setback with no outer limit, and
        # is one bound stated and one absent rather than a malformed cell.
        bounds: dict[str, float | int] = {}
        unread = False
        for name, part in zip(names, parts):
            if _normalize(part) in _ABSENT:
                continue
            value = _number(part)
            if value is None:
                unread = True
                continue
            bounds[name] = _cast(name, value)
        return bounds, text.strip() if unread else None
    if kind == "pair" and len(parts) == 1:
        # A min/max row printing one number states the bound it is named for -
        # the maximum - and says nothing about the other. Noted, because a grid
        # doing this is worth seeing rather than silently halved.
        value = _number(parts[0])
        if value is None:
            return {}, text.strip()
        return {names[1]: _cast(names[1], value)}, f"{text.strip()} read as a maximum"
    if kind in ("min", "max") and len(parts) == 1:
        value = _number(parts[0])
        if value is None:
            return {}, text.strip()
        return {names[0]: _cast(names[0], value)}, None
    return {}, text.strip()


def _number(text: str) -> float | None:
    """A grid's number. Decimals are printed the French way, ``0,5``."""
    candidate = re.sub(r"[\s  ]", "", text).replace(",", ".")
    return float(candidate) if _NUMBER.match(candidate) else None


def _cast(name: str, value: float) -> float | int:
    return int(round(value)) if name in _INTEGER_FIELDS else value


def _normalize(text: str) -> str:
    """Lowercase, unaccented, straight-quoted, single-spaced.

    The labels are matched against this rather than against what is printed:
    the same row is ``Mode d'implantation`` with a typographic apostrophe in
    one borough's template and a straight one in another's, and the accents
    survive extraction only as reliably as the font's encoding does.
    """
    text = text.replace("’", "'").replace("ʼ", "'")
    decomposed = unicodedata.normalize("NFKD", text)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", stripped).strip().lower()
