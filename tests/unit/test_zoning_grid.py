"""What `urban_rag.zoning_grid` reads off a grid, and what it refuses to guess.

The parser's whole subject is horizontal position, so the fixtures here are
built the way `extraction_mode="layout"` hands a page over: a label at the left
margin, a unit caption in the middle, and each column's value *right-aligned*
at its own offset. `grid_page` below does that alignment, which keeps the tests
about the grid rather than about counting spaces.

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
    is_grid_page,
    parse_grid_page,
)

#: Where each column's values end, in characters. Two columns about twenty
#: apart, as the published grids place them.
EDGES = (153, 177)

#: Where a row's unit caption ends - left of the first column, which is what
#: keeps `min/max (m)` from being read as a value.
UNIT_END = 122


def row(label: str, unit: str = "", *values: str, edges=EDGES) -> str:
    """One line of a grid, laid out the way layout extraction hands it over.

    ``values`` are right-aligned on ``edges``; an empty one leaves that column
    blank, which is how a grid prints a norm that column does not carry.
    """
    line = f"     {label}"
    if unit:
        line = line.ljust(UNIT_END - len(unit)) + unit
    for value, edge in zip(values, edges):
        if value:
            line = line.ljust(edge - len(value)) + value
    return line


def grid_page(
    *,
    zone: str = "C01-001",
    habitation: tuple[str, str] = ("", "H"),
    commerce: tuple[str, str] = ("C.4", ""),
    all_levels: tuple[str, str] = ("X", ""),
    except_ground: tuple[str, str] = ("", "X"),
    floors: tuple[str, str] = ("2/6", "2/6"),
    lot_width: tuple[str, str] = ("-", "-"),
    coverage: tuple[str, str] = ("50/70", "50/70"),
    density: tuple[str, str] = ("0/4,5", "0/4,5"),
    dwellings: tuple[str, str] = ("", ""),
    front_margin: tuple[str, str] = ("2,5/3,5", "2,5/3,5"),
    edges=EDGES,
) -> str:
    """A grid page carrying the rows this parser reads."""
    lines = [
        "  Grille des usages et des normes",
        "",
        row("USAGES AUTORISÉS", "", *["" for _ in edges]).ljust(246)
        + "ZONE :".ljust(28)
        + zone,
        row("Catégories d’usages autorisées"),
        row("Habitation", "", *habitation, edges=edges),
        row("Commerce", "", *commerce, edges=edges),
        row("Industrie"),
        row("Équipements collectifs et institutionnels"),
        row("Niveaux de bâtiment autorisés"),
        row("Rez-de-chaussée (RDC)"),
        row("Inférieurs au RDC"),
        row("Immédiatement supérieur au RDC", "(2e étage)"),
        row("Tous sauf le RDC", "", *except_ground, edges=edges),
        row("Tous les niveaux", "", *all_levels, edges=edges),
        row("Nombre de logements maximal", "", *dwellings, edges=edges),
        "     CADRE BÂTI",
        row("En mètre", "min/max (m)", "0/23", "0/23", edges=edges),
        row("En étage", "min/max", *floors, edges=edges),
        row("Largeur du terrain", "min (m)", *lot_width, edges=edges),
        row("Mode d’implantation", "(I-J-C)", "I-J", "I-J", edges=edges),
        row("Taux d’implantation au sol", "min/max (%)", *coverage, edges=edges),
        row("Densité", "min/max", *density, edges=edges),
        row("Avant principale", "min/max (m)", *front_margin, edges=edges),
        row("Avant secondaire", "min/max (m)", "1/2,5", "1/2,5", edges=edges),
        row("Latérale", "min (m)", "3", "3", edges=edges),
        row("Arrière", "min (m)", "4", "4", edges=edges),
        row("Pourcentage d’ouvertures", "min/max (%)", "10/100", "10/40", edges=edges),
        row("Pourcentage de maçonnerie", "min (%)", "-", "80", edges=edges),
        "     Patrimoine",
        # Below Patrimoine every value is stated once for the zone, at an
        # offset of its own. None of it may reach a column.
        "     Secteur d’intérêt patrimonial".ljust(224) + "-",
        "     Articles visés".ljust(168) + "-",
        "     PIIA (secteur)".ljust(168) + "3",
        "     PAE".ljust(168) + "-",
    ]
    return "\n".join(lines)


def column_of(page: str, index: int):
    columns = parse_grid_page(page)
    assert len(columns) > index, f"only {len(columns)} column(s) parsed"
    return columns[index]


def test_finds_one_column_per_band_and_reads_the_zone():
    columns = parse_grid_page(grid_page())
    assert [c.column_index for c in columns] == [0, 1]
    assert {c.zone for c in columns} == {"C01-001"}


def test_attaches_each_usage_code_to_the_column_it_is_printed_over():
    commerce, habitation = parse_grid_page(grid_page())
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
    commerce, habitation = parse_grid_page(grid_page())
    assert commerce.levels == frozenset({BuildingLevel.ALL})
    assert habitation.levels == frozenset({BuildingLevel.ALL_EXCEPT_GROUND})


def test_reads_the_norms_as_the_pairs_they_are_printed_as():
    habitation = column_of(grid_page(), 1)
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
    habitation = column_of(grid_page(dwellings=("", "12")), 1)
    assert habitation.floors_max == 6 and isinstance(habitation.floors_max, int)
    assert habitation.max_dwellings == 12 and isinstance(habitation.max_dwellings, int)


def test_a_dash_is_an_absent_norm_and_not_a_norm_of_zero():
    """The distinction `ZoneColumn` is built around.

    ``Largeur du terrain -`` means any width qualifies. Read as 0 it happens to
    be equivalent; the same reading of ``Densité -`` forbids building at all,
    which is why the absence is carried rather than defaulted.
    """
    habitation = column_of(grid_page(lot_width=("-", "-"), density=("-", "-")), 1)
    assert habitation.min_lot_width_m is None
    assert habitation.density_min is None
    assert habitation.density_max is None


def test_half_a_pair_is_the_bound_it_states():
    """As zone E01-003 prints it: ``Avant principale min/max (m)  6/``."""
    habitation = column_of(grid_page(), 1)
    assert habitation.front_margin_min_m == 2.5

    both = parse_grid_page(grid_page(front_margin=("2,5/3,5", "6/")))
    assert both[1].front_margin_min_m == 6.0
    assert both[1].front_margin_max_m is None
    assert both[1].notes == ()


def test_a_footnote_qualifies_a_norm_rather_than_breaking_it():
    page = grid_page(density=("0/4,5", "0/4,5(9)"))
    assert column_of(page, 1).density_max == 4.5


def test_values_below_patrimoine_are_not_columns():
    """The lone ``-`` under *Articles visés* sits at its own offset.

    Three of them do, in fact, which is enough to look like a column band if
    the scan runs past the end of the table.
    """
    assert len(parse_grid_page(grid_page())) == 2


def test_unit_captions_are_not_values():
    habitation = column_of(grid_page(), 1)
    assert habitation.implantation_mode == "I-J"  # not "(I-J-C)"
    assert habitation.notes == ()


def test_three_column_grid():
    edges = (144, 167, 189)
    page = grid_page(
        zone="C01-002",
        habitation=("", "", ""),
        commerce=("C.3(9)", "", "C.7A"),
        all_levels=("X", "X", "X"),
        except_ground=("", "", ""),
        floors=("2/6", "2/6", "2/6"),
        lot_width=("-", "-", "-"),
        coverage=("50/85", "50/85", "50/85"),
        density=("0/4,5", "0/4,5", "0/4,5"),
        edges=edges,
    )
    columns = parse_grid_page(page)
    assert [c.usages for c in columns] == [("C.3(9)",), (), ("C.7A",)]
    assert [c.is_empty for c in columns] == [False, True, False]


def test_to_zone_column_hands_the_solver_its_input():
    zone_column = column_of(grid_page(), 1).to_zone_column()
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
    habitation = column_of(grid_page(floors=("2/6", "-")), 1)
    assert habitation.floors_max is None
    with pytest.raises(GridParseError, match="storey maximum"):
        habitation.to_zone_column()


def test_a_page_with_no_column_band_yields_nothing():
    """A title with no CADRE BÂTI block is a page this cannot read.

    Not a zone with no norms - which is why it is an empty list to be counted
    rather than a column of nulls to be solved.
    """
    page = "  Grille des usages et des normes\n\n     ZONE :   C01-001\n"
    assert parse_grid_page(page) == []


def test_prose_that_mentions_a_grid_is_not_a_grid():
    resolution = (
        "  Extrait authentique du procès-verbal d’une séance du conseil\n\n"
        "Adopter la résolution PP21-14005 à l'effet de permettre la production\n"
        "de bières artisanales, en dérogation aux usages autorisés à la grille\n"
        "des usages et des normes de l'annexe C.\n"
    )
    assert not is_grid_page(resolution)
    assert is_grid_page(grid_page())
