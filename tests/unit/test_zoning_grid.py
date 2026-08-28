"""What `urban_rag.zoning_grid` reads off a grid, and what it refuses to guess.

The parser's whole subject is horizontal position, so the fixtures here are
real PDFs rather than text: `grid_pdf` types a page the way a grille is
typeset - a label at the left margin, a unit caption in the middle, and each
column's value *centred* on its own band - and the assertions run through
`parse_grid_pdf`, the same entry `zoning_grid_columns` calls. Nothing is
stubbed, so what these cover is the parse a published grid actually gets.

The page is set in Courier, whose every glyph is six tenths of an em wide, so
`_at` can centre a value on a column exactly rather than approximately; the
published grids are set in Arial and land within about three points of their
own centres, which `COLUMN_TOLERANCE` is sized for.

The offsets and the wording are copied from the published grids for zones
C01-001 and C01-002 (Villeray-Saint-Michel-Parc-Extension, by-law 01-283), so
the two-column case is a real one: a *Commerce* column authorised on every
level and a bare *Habitation* column authorised on every level but the ground
floor, which is the example `urban_rag.program` names in its own docstring.
"""

from __future__ import annotations

import pytest

from urban_rag.program import BuildingLevel
from urban_rag.zoning_grid import (
    GridParseError,
    parse_grid_pdf,
)

#: Point size every fixture page is set at, and the width of one Courier glyph
#: at it. Courier is metrically fixed - every glyph is six tenths of an em -
#: which is what makes `_at` exact. Seven and a half points rather than the
#: nine the published grids are set at because Courier is the wider face: at
#: this size its ``min/max (m)`` spans 230 to 280 points, which is where Arial
#: puts the same caption at nine. The point is that the fixture crowds its
#: columns exactly as hard as a real page does.
FONT_SIZE = 7.5
GLYPH = FONT_SIZE * 0.6

#: Where each column is centred, in points. Two columns forty apart, as the
#: published grids place them.
COLUMNS = (303.5, 344.0)

#: Where a row's unit caption is centred - left of the first column, which is
#: what keeps `min/max (m)` from being read as a value.
UNIT_CENTER = 255.0

#: The left margin every row label starts at, and the leading between rows.
MARGIN = 49.6
LEADING = 10.9


def _at(center: float, text: str) -> float:
    """The left edge that centres ``text`` on ``center``."""
    return center - len(text) * GLYPH / 2


def grid_pdf(
    *,
    zone: str = "C01-001",
    habitation: tuple[str, ...] = ("", "H"),
    commerce: tuple[str, ...] = ("C.4", ""),
    all_levels: tuple[str, ...] = ("X", ""),
    except_ground: tuple[str, ...] = ("", "X"),
    floors: tuple[str, ...] = ("2/6", "2/6"),
    lot_width: tuple[str, ...] = ("-", "-"),
    coverage: tuple[str, ...] = ("50/70", "50/70"),
    density: tuple[str, ...] = ("0/4,5", "0/4,5"),
    dwellings: tuple[str, ...] = ("", ""),
    front_margin: tuple[str, ...] = ("2,5/3,5", "2,5/3,5"),
    columns: tuple[float, ...] = COLUMNS,
) -> bytes:
    """A one-page PDF of a grid carrying the rows this parser reads."""
    items: list[tuple[float, float, str]] = []
    top = 747.0

    def line(label: str, unit: str = "", *values: str) -> None:
        nonlocal top
        items.append((MARGIN, top, label))
        if unit:
            items.append((_at(UNIT_CENTER, unit), top, unit))
        for value, center in zip(values, columns):
            if value:
                items.append((_at(center, value), top, value))
        top -= LEADING

    line("Grille des usages et des normes")
    line("")
    items.append((460.0, top + LEADING, "ZONE :"))
    items.append((521.0, top + LEADING, zone))
    line("USAGES AUTORISÉS")
    line("Catégories d’usages autorisées")
    line("Habitation", "", *habitation)
    line("Commerce", "", *commerce)
    line("Industrie")
    line("Équipements collectifs et institutionnels")
    line("Niveaux de bâtiment autorisés")
    line("Rez-de-chaussée (RDC)")
    line("Inférieurs au RDC")
    line("Immédiatement supérieur au RDC", "(2e étage)")
    line("Tous sauf le RDC", "", *except_ground)
    line("Tous les niveaux", "", *all_levels)
    line("Nombre de logements maximal", "", *dwellings)
    line("CADRE BÂTI")
    line("En mètre", "min/max (m)", "0/23", "0/23")
    line("En étage", "min/max", *floors)
    line("Largeur du terrain", "min (m)", *lot_width)
    line("Mode d’implantation", "(I-J-C)", "I-J", "I-J")
    line("Taux d’implantation au sol", "min/max (%)", *coverage)
    line("Densité", "min/max", *density)
    line("Avant principale", "min/max (m)", *front_margin)
    line("Avant secondaire", "min/max (m)", "1/2,5", "1/2,5")
    line("Latérale", "min (m)", "3", "3")
    line("Arrière", "min (m)", "4", "4")
    line("Pourcentage d’ouvertures", "min/max (%)", "10/100", "10/40")
    line("Pourcentage de maçonnerie", "min (%)", "-", "80")
    line("Patrimoine")
    # Below Patrimoine every value is stated once for the zone, centred
    # somewhere of its own. None of it may reach a column.
    line("Secteur d’intérêt patrimonial")
    items.append((_at(427.0, "-"), top + LEADING, "-"))
    line("Articles visés")
    items.append((_at(316.0, "-"), top + LEADING, "-"))
    line("PIIA (secteur)")
    items.append((_at(327.0, "3"), top + LEADING, "3"))
    line("PAE")
    items.append((_at(328.0, "-"), top + LEADING, "-"))

    return _pdf([item for item in items if item[2]])


def _pdf(items, *, width: int = 612, height: int = 792) -> bytes:
    """``(x, y, text)`` in points, as the bytes of a one-page PDF.

    Written out by hand rather than with a writer: what these tests are about
    is where a string sits on a page, and a page whose every string is placed
    by an explicit text matrix is the shortest way to say so.
    """

    def escape(text: str) -> str:
        return text.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")

    # cp1252 is what /WinAnsiEncoding declares below, and is what carries the
    # typographic apostrophe of ``Mode d’implantation`` - the character the
    # published grids print and `_normalize` straightens.
    stream = "\n".join(
        f"BT /F1 {FONT_SIZE} Tf 1 0 0 1 {x:.3f} {y:.3f} Tm ({escape(text)}) Tj ET"
        for x, y, text in items
    ).encode("cp1252")

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {width} {height}] "
            f"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>"
        ).encode(),
        b"<< /Length "
        + str(len(stream)).encode()
        + b" >>\nstream\n"
        + stream
        + b"\nendstream",
        (
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier "
            b"/Encoding /WinAnsiEncoding >>"
        ),
    ]

    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    start_xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode() + b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{start_xref}\n%%EOF\n"
    ).encode()
    return bytes(out)


def column_of(page: bytes, index: int):
    columns = parse_grid_pdf(page)
    assert len(columns) > index, f"only {len(columns)} column(s) parsed"
    return columns[index]


def test_finds_one_column_per_band_and_reads_the_zone():
    columns = parse_grid_pdf(grid_pdf())
    assert [c.column_index for c in columns] == [0, 1]
    assert {c.zone for c in columns} == {"C01-001"}


def test_attaches_each_usage_code_to_the_column_it_is_printed_over():
    commerce, habitation = parse_grid_pdf(grid_pdf())
    assert commerce.usages == ("C.4",)
    assert habitation.usages == ("H",)
    assert habitation.permits_residential
    assert not commerce.permits_residential


def test_levels_follow_the_column_and_not_the_row():
    """The failure the layout mode exists to prevent.

    Both level rows carry a single ``X``, and flattened text says only that
    *some* column is authorised on all levels and *some* column above the
    ground floor. Which is which is the x-coordinate.
    """
    commerce, habitation = parse_grid_pdf(grid_pdf())
    assert commerce.levels == frozenset({BuildingLevel.ALL})
    assert habitation.levels == frozenset({BuildingLevel.ALL_EXCEPT_GROUND})


def test_reads_the_norms_as_the_pairs_they_are_printed_as():
    habitation = column_of(grid_pdf(), 1)
    assert (habitation.floors_min, habitation.floors_max) == (2, 6)
    assert (habitation.site_coverage_min_pct, habitation.site_coverage_max_pct) == (
        50.0,
        70.0,
    )
    # Printed the French way, "0/4,5".
    assert (habitation.density_min, habitation.density_max) == (0.0, 4.5)
    assert (habitation.front_margin_min_m, habitation.front_margin_max_m) == (2.5, 3.5)
    assert habitation.side_margin_min_m == 3.0
    assert habitation.rear_margin_min_m == 4.0
    assert habitation.implantation_mode == "I-J"


def test_storeys_and_dwellings_stay_whole_numbers():
    habitation = column_of(grid_pdf(dwellings=("", "12")), 1)
    assert habitation.floors_max == 6 and isinstance(habitation.floors_max, int)
    assert habitation.max_dwellings == 12 and isinstance(habitation.max_dwellings, int)


def test_a_dash_is_an_absent_norm_and_not_a_norm_of_zero():
    """The distinction `ZoneColumn` is built around.

    ``Largeur du terrain -`` means any width qualifies. Read as 0 it happens to
    be equivalent; the same reading of ``Densité -`` forbids building at all,
    which is why the absence is carried rather than defaulted.
    """
    habitation = column_of(grid_pdf(lot_width=("-", "-"), density=("-", "-")), 1)
    assert habitation.min_lot_width_m is None
    assert habitation.density_min is None
    assert habitation.density_max is None


def test_half_a_pair_is_the_bound_it_states():
    """As zone E01-003 prints it: ``Avant principale min/max (m)  6/``."""
    habitation = column_of(grid_pdf(), 1)
    assert habitation.front_margin_min_m == 2.5

    both = parse_grid_pdf(grid_pdf(front_margin=("2,5/3,5", "6/")))
    assert both[1].front_margin_min_m == 6.0
    assert both[1].front_margin_max_m is None
    assert both[1].notes == ()


def test_a_footnote_qualifies_a_norm_rather_than_breaking_it():
    page = grid_pdf(density=("0/4,5", "0/4,5(9)"))
    assert column_of(page, 1).density_max == 4.5


def test_a_footnote_lettered_in_roman_is_still_a_footnote():
    """As zone C01-122 prints it: ``En étage min/max  3/8 (i)``.

    A marker the parser does not know is a marker costs the storey ceiling,
    which is the one norm `to_zone_column` cannot do without.
    """
    habitation = column_of(grid_pdf(floors=("2/6", "3/8 (i)")), 1)
    assert (habitation.floors_min, habitation.floors_max) == (3, 8)
    assert habitation.notes == ()


def test_a_caption_in_brackets_is_not_a_footnote():
    """``(I-J-C)`` names the modes a grid may print, and is not one of them."""
    assert column_of(grid_pdf(), 1).implantation_mode == "I-J"


def test_values_below_patrimoine_are_not_columns():
    """The lone ``-`` under *Articles visés* sits at its own offset.

    Three of them do, in fact, which is enough to look like a column band if
    the scan runs past the end of the table.
    """
    assert len(parse_grid_pdf(grid_pdf())) == 2


def test_unit_captions_are_not_values():
    habitation = column_of(grid_pdf(), 1)
    assert habitation.implantation_mode == "I-J"  # not "(I-J-C)"
    assert habitation.notes == ()


def test_three_column_grid():
    page = grid_pdf(
        zone="C01-002",
        habitation=("", "", ""),
        commerce=("C.3(9)", "", "C.7A"),
        all_levels=("X", "X", "X"),
        except_ground=("", "", ""),
        floors=("2/6", "2/6", "2/6"),
        lot_width=("-", "-", "-"),
        coverage=("50/85", "50/85", "50/85"),
        density=("0/4,5", "0/4,5", "0/4,5"),
        columns=(303.5, 344.0, 384.5),
    )
    columns = parse_grid_pdf(page)
    assert [c.usages for c in columns] == [("C.3(9)",), (), ("C.7A",)]
    assert [c.is_empty for c in columns] == [False, True, False]


def test_to_zone_column_hands_the_solver_its_input():
    zone_column = column_of(grid_pdf(), 1).to_zone_column()
    assert zone_column.usages == ("H",)
    assert zone_column.permits_residential
    assert zone_column.floors_max == 6
    assert zone_column.floors_min == 2
    # Six storeys authorised, the ground floor not among them.
    assert zone_column.residential_floors == 5
    assert zone_column.density_max == 4.5
    assert zone_column.zone == "C01-001"


def test_to_zone_column_refuses_a_column_with_no_storey_ceiling():
    """An envelope with no cap on storeys is unbounded, not generous."""
    habitation = column_of(grid_pdf(floors=("2/6", "-")), 1)
    assert habitation.floors_max is None
    with pytest.raises(GridParseError, match="storey maximum"):
        habitation.to_zone_column()


def test_a_page_with_no_column_band_is_a_page_that_cannot_be_read():
    """A title with no CADRE BÂTI block is a page this cannot read.

    Not a zone with no norms - so the page contributes no columns, and a PDF
    of nothing but such pages is a parse failure rather than a silent zero.
    """
    page = _pdf(
        [
            (MARGIN, 747.0, "Grille des usages et des normes"),
            (460.0, 720.0, "ZONE :"),
            (521.0, 720.0, "C01-001"),
        ]
    )
    with pytest.raises(GridParseError, match="readable columns"):
        parse_grid_pdf(page)


def test_prose_that_mentions_a_grid_is_not_a_grid():
    """A P.P.C.M.O.I. resolution says the words in its body.

    Reading it as a grid would make columns out of its paragraph indents, so
    it has to fail as a non-grid rather than parse as an empty one.
    """
    resolution = _pdf(
        [
            (MARGIN, 747.0, "Extrait authentique du procès-verbal d’une séance"),
            (MARGIN, 720.0, "Adopter la résolution PP21-14005 à l'effet de permettre"),
            (MARGIN, 709.0, "la production de bières artisanales, en dérogation aux"),
            (MARGIN, 698.0, "usages autorisés à la grille des usages et des normes."),
        ]
    )
    with pytest.raises(GridParseError, match="readable columns"):
        parse_grid_pdf(resolution)

    assert parse_grid_pdf(grid_pdf())
