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
column of norms is a band of cells centred on a fixed horizontal offset. Which
band a cell falls in is the only thing that attaches ``H`` to a storey maximum
of 6 rather than to the ``C.4`` beside it. So the offsets are taken off the
page itself: every string a content stream shows carries a text matrix, and
`pypdf` resolves that into the user-space points the string spans. Cells are
what those strings form once the ones less than a word apart are joined, and
the parsing problem becomes one of clustering their midpoints.

**Why not `extraction_mode="layout"`.** It renders a page into a fixed-width
grid of characters, which is a usable proxy for x only while one character
width describes the whole page. These grilles are typeset at ``Tf 1`` with the
scale held in the text matrix, and they place their columns by kerning the gaps
inside a single ``TJ``; `pypdf` measures a space against the nominal font size,
so the padding it emits runs to twice the width of the page, by a factor that
differs on every row. Zone C04-049 prints its two columns on twelve rows of
*CADRE BATI* and no two of those rows agree to within four characters, which is
a grid reported as carrying no readable columns at all. Widening the tolerance
does not recover it - the same offsets are what says how many columns a grid
has - so it is the geometry that had to be replaced, not the tolerance that
reads it.

**Columns are found, not assumed.** A grid carries one to seven of them and
prints no count anywhere. The offsets are recovered from the rows of the
*CADRE BATI* block - height, storeys, lot width, implantation mode, coverage,
density, the four margins - because those are the rows a grid fills in for
every column it has, including with ``-``. Their cells' midpoints cluster into
as many groups as the grid has columns, and every other cell on the page is
then attributed to whichever cluster its own midpoint lands in. Cells that land
in none - the ``min/max (m)`` unit captions to the left of the band, the
full-width footnotes below it - are dropped, which is what makes those captions
harmless rather than something to strip by name.

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
from typing import TYPE_CHECKING

from urban_rag.program import BuildingLevel, ZoneColumn, is_residential_usage

if TYPE_CHECKING:
    from pypdf._page import PageObject
    from pypdf._text_extraction._layout_mode._text_state_params import TextStateParams

#: The title every grid page carries, normalized. A PDF linked from
#: ``LIEN_GRILLE`` is not always a grid - the same column has carried
#: P.P.C.M.O.I. resolutions - so the title is what decides whether a page is
#: one.
GRID_TITLE = "grille des usages et des normes"

#: How far, in points, a cell's midpoint may sit from a column's and still be
#: in it. A grid centres its values in the band, so the midpoints of one
#: column's cells sit within about three points of each other across a page and
#: within nine at the worst; the pitch between adjacent columns is around
#: forty. This sits between the two.
COLUMN_TOLERANCE = 12.0

#: How many of the *CADRE BATI* rows have to place a cell at an offset before
#: it counts as a column. Below this a stray full-width value - the lone ``-``
#: under *Articles vises* - would be read as a column of its own.
MIN_COLUMN_SUPPORT = 3

#: How wide a gap ends a cell, as a fraction of the height of the font either
#: side of it. About two and a half spaces: one space is what separates the
#: words *inside* a cell (``min/max (m)``, ``I-J``) and a superscript rides a
#: little further out than that, while a cell boundary is most of a column
#: wide - sixteen points against six, at the nine-point body these are set in.
CELL_GAP_EM = 0.7

#: ...and how wide a gap is a space rather than the kerning between two halves
#: of a word, in the same units. These grids break a string wherever they kern
#: it - ``Habitation`` arrives as ``Habit`` and ``ation``, a tenth of a point
#: apart - and a label rejoined as ``Habit ation`` matches no row, just as one
#: rejoined as ``Categoriesdusages`` would not.
WORD_GAP_EM = 0.12

#: How far apart, in points, two baselines may sit and still be one row, where
#: that is more than half the font height. Superscripts - the ``e`` of ``(2e
#: etage)`` - ride about three points above the line they belong to.
ROW_TOLERANCE = 1.0

#: The unit caption a row prints between its label and its values. It is kept
#: out of the offsets the column bands are computed from; on every other row it
#: is dropped by landing outside those bands rather than by this pattern.
_UNIT = re.compile(r"^(?:min|max|min/max)\b|^\(.+\)$")

#: A footnote marker, as printed after a value or a usage code. It qualifies
#: the norm rather than changing it, so it is stripped before a number is read.
#: Numbered in most boroughs and lettered in roman where the notes are set
#: under the grid rather than beside it - zone C01-122 prints ``3/8 (i)``, and
#: read as a number that is no storey ceiling at all. The alternation is what
#: keeps ``(I-J-C)`` a caption: a marker is digits or numerals and nothing else.
_FOOTNOTE = re.compile(r"\(\s*(?:\d+|[ivx]+)\s*\)", re.IGNORECASE)

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
    """One cell of a row, and the span of the page it covers, in points.

    ``center`` is what identifies the column: the grid centres its values in
    the band, so a ``-`` and a ``50/70`` in the same column share a midpoint
    and share nothing else - not a right edge, which the wider of the two
    overhangs by six points, nor a left one.
    """

    start: float
    end: float
    text: str

    @property
    def center(self) -> float:
        return (self.start + self.end) / 2


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
        pages = [_page_rows(page) for page in reader.pages]
    except (PdfReadError, ValueError, OSError) as exc:
        raise GridParseError(f"{where}: unreadable PDF ({exc})") from exc

    columns: list[GridColumn] = []
    for rows in pages:
        if is_grid_page(rows):
            columns.extend(parse_grid_page(rows))
    if not columns:
        raise GridParseError(
            f"{where}: no page carries a {GRID_TITLE!r} with readable columns"
        )
    return columns


def is_grid_page(rows: Sequence[Sequence[_Cell]]) -> bool:
    """Whether a page is a grid rather than prose that mentions one.

    The title is looked for in the first few rows only. A P.P.C.M.O.I.
    resolution derogating "a la grille des usages et des normes" says the words
    in its body, and reading that as a grid would make columns out of the
    indents of its paragraphs.
    """
    return any(GRID_TITLE in _normalize(_line(row)) for row in rows[:4])


def _line(row: Iterable[_Cell]) -> str:
    """A row as the one string it would read as, cells and all."""
    return " ".join(cell.text for cell in row)


def parse_grid_page(rows: Sequence[Sequence[_Cell]]) -> list[GridColumn]:
    """The columns of one grid page, from its rows of positioned cells.

    Returns an empty list where the page has a title but no column band: a
    grid whose *CADRE BATI* block failed to extract is a page this cannot read,
    not a zone with no norms.
    """
    zone = _zone(rows)
    rows = list(_grid_block(rows))

    centers = _column_centers(rows)
    if not centers:
        return []

    fields: list[dict] = [
        {
            "zone": zone,
            "column_index": index,
            "usages_by_category": {},
            "levels": set(),
        }
        for index in range(len(centers))
    ]
    notes: list[list[str]] = [[] for _ in centers]

    for row in rows:
        label = _normalize(row[0].text)
        values = _by_column(row[1:], centers)
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


def _page_rows(page: PageObject) -> list[list[_Cell]]:
    """A page as rows of cells, top to bottom and each row left to right."""
    shown = [fragment for fragment in _fragments(page) if fragment.text.strip()]
    shown.sort(key=lambda fragment: (-fragment.ty, fragment.tx))

    lines: list[list[TextStateParams]] = []
    baseline = 0.0
    for fragment in shown:
        # Half the font height keeps the rows of a grid - eleven points apart -
        # apart, while gathering the superscript of a ``(2e etage)`` caption
        # into the row it qualifies rather than leaving it one of its own.
        if not lines or abs(baseline - fragment.ty) >= max(
            ROW_TOLERANCE, fragment.font_height / 2
        ):
            lines.append([])
            baseline = fragment.ty
        lines[-1].append(fragment)

    return [cells for line in lines if (cells := _cells(line))]


def _cells(line: Iterable[TextStateParams]) -> list[_Cell]:
    """One row's strings joined back into the cells they were printed as.

    A content stream shows as much or as little of a line at a time as its
    typesetter chose, and these grids break a string wherever they kern it -
    ``Densite`` arrives as ``Densit`` and ``e``. So a cell is not a string but
    a run of them, and where one ends is a question about the gap to the next.
    """
    cells: list[_Cell] = []
    for fragment in sorted(line, key=lambda fragment: fragment.tx):
        start, end, text = _span(fragment)
        if not text:
            continue
        # Both gaps are measured against the height of the font, which is the
        # one size on a `TextStateParams` that has been through the text
        # matrix. `space_tx` has not, and on a grid set at ``Tf 1`` it reports
        # the space of a one-point font - the same arithmetic that costs
        # `extraction_mode="layout"` its geometry, and for the same reason.
        em = fragment.font_height or fragment.font_size
        gap = start - cells[-1].end if cells else 0.0
        if not cells or gap > em * CELL_GAP_EM:
            cells.append(_Cell(start, end, text))
            continue
        cells[-1] = _Cell(
            cells[-1].start,
            max(cells[-1].end, end),
            cells[-1].text + (" " if gap > em * WORD_GAP_EM else "") + text,
        )
    return cells


def _span(fragment: TextStateParams) -> tuple[float, float, str]:
    """Where a fragment's ink starts and ends, and what it says.

    Added up from the font's own glyph widths rather than read off
    `displaced_tx`, which carries the character and word spacing with it.
    ``Tw`` is a per-space advance in text space, so a grid set at ``Tf 1``
    scales it by the whole text matrix: zone E02-139 sets ``4.2293 Tw`` and
    takes it back with a positive kern after each space, which renders
    correctly and reports every trailing space as thirty-eight points wide.
    Believing that, ``Laterale min (m) 3`` is one cell forty points wide whose
    text is not a number, instead of a caption and a margin of three metres.

    Character spacing is dropped with it, at a cost of a hundredth of an em
    per glyph - which is the width of the ink, not of a column.
    """
    widths = fragment.font.character_widths
    default = widths.get("default", 500)
    em = (fragment.font_height or fragment.font_size) / 1000

    def advance(text: str) -> float:
        return sum(widths.get(char, default) for char in text) * em

    text = fragment.text
    indent = len(text) - len(text.lstrip())
    start = fragment.tx + advance(text[:indent])
    stripped = text.strip()
    return start, start + advance(stripped), stripped


def _fragments(page: PageObject) -> list[TextStateParams]:
    """Every string the page shows, with the user-space x it spans.

    `pypdf` works these out to lay a page out as fixed-width text and then
    spends them on that: `extract_text` returns the character grid and not the
    coordinates it was built from. These are the same `TextStateParams` its
    layout mode assembles, taken before that projection is imposed on them -
    which is a private name because the public one answers a question about
    characters and this module's is about points. See the module docstring.
    """
    from pypdf._text_extraction._layout_mode._fixed_width_page import (
        recurse_to_target_op,
        resolve_font,
    )
    from pypdf._text_extraction._layout_mode._text_state_manager import (
        TextStateManager,
    )
    from pypdf.generic import ContentStream

    contents = page.get_contents()
    if contents is None:
        return []

    fonts = page._layout_mode_fonts()
    operations = iter(ContentStream(contents, page.pdf, "bytes").operations)
    state = TextStateManager()
    shown: list[TextStateParams] = []
    for operands, operator in operations:
        if operator in (b"BT", b"q"):
            # Text is shown inside these; the call consumes the operations up
            # to the matching end and hands back what they placed.
            _, placed = recurse_to_target_op(
                operations,
                state,
                b"ET" if operator == b"BT" else b"Q",
                fonts,
                True,
            )
            shown.extend(placed)
        elif operator == b"Tf":
            state.set_font(resolve_font(fonts, operands[0]), operands[1])
        else:
            state.set_state_param(operator, operands)
    return shown


def _zone(rows: Sequence[Sequence[_Cell]]) -> str | None:
    """The zone number printed beside ``ZONE :``.

    It is the id the map joins on - ``NUMERO_COMPLET`` in the feature table,
    ``feature_id`` in `silver.lot_features` - so a grid that does not print one
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


def _column_centers(rows: Sequence[Sequence[_Cell]]) -> list[float]:
    """The midpoint of each column, left to right.

    Built from `_ANCHOR_LABELS` only. Every other row on the page is attributed
    to these, which is what keeps a full-width value below the band - or a unit
    caption to the left of it - from inventing a column.
    """
    midpoints = sorted(
        cell.center
        for row in rows
        if _normalize(row[0].text) in _ANCHOR_LABELS
        for cell in row[1:]
        if not _UNIT.match(cell.text.strip())
    )
    if not midpoints:
        return []

    clusters: list[list[float]] = [[midpoints[0]]]
    for midpoint in midpoints[1:]:
        if midpoint - clusters[-1][-1] <= COLUMN_TOLERANCE:
            clusters[-1].append(midpoint)
        else:
            clusters.append([midpoint])
    return [
        cluster[len(cluster) // 2]
        for cluster in clusters
        if len(cluster) >= MIN_COLUMN_SUPPORT
    ]


def _by_column(cells: Iterable[_Cell], centers: Sequence[float]) -> dict[int, _Cell]:
    """Attribute cells to columns by midpoint; drop what lands in none.

    The last cell wins a collision, which happens only on a row whose unit
    caption itself centres into the first column's band. A caption cannot be a
    value and a value can, so preferring the later of the two is preferring the
    one printed further right - the value.
    """
    placed: dict[int, _Cell] = {}
    for cell in cells:
        distances = [abs(cell.center - center) for center in centers]
        nearest = min(range(len(centers)), key=distances.__getitem__)
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
