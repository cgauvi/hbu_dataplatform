"""Saguenay's zoning: the grid PDF read, the margin translation, and the registry.

Like `test_zoning_grid`, the fixtures here are real PDFs rather than text,
because the parser's whole subject is horizontal position - which column a
value is centred on is the only thing that attaches a fifteen-metre front
margin to the ``c4a`` beside it rather than to the ``I2`` further along. The
page builder is shared with that module: a grid is a grid, and only the rows
and their wording differ.

Two things here are Saguenay's own and are what most of these tests are about.

**The rows run up the page.** The city's reporting service lays its content
out in an inverted text space, so a published grid hands `page_rows` its
amendment table first and its letterhead last. `_reading_order` is what sorts
that out, and `test_a_grid_reads_the_same_either_way_up` builds the same grid
both ways and asserts the parse does not change.

**The by-law prices four non-front margins where the others price two.**
*Latérale 1*, *Latérale 2*, *Latérale sur rue* and *Arrière sur rue* have to
land in the fields `silver.lot_buildable_setbacks` already has, and the
fifth - the rear-on-street figure - is carried for
`postgis.compute_lot_buildable_setbacks` to choose per lot.
"""

from __future__ import annotations

import pytest

from test_zoning_grid import _pdf

from hbu_dataplatform.partitions import axes as partitions
from hbu_dataplatform.cities.saguenay.registry import (
    SAGUENAY_OUTLINE_TYPE,
    saguenay_outline_name_for,
)
from hbu_dataplatform.partitions.cities import (
    City,
    city_of,
    cmhc_centre_for,
    metric_crs_for,
    metric_srid_for,
    municipality_code_for,
    quartiers_for,
)
from hbu_dataplatform.partitions.axes import known_neighborhoods
from hbu_dataplatform.cities.montreal.registry import submarket_for
from hbu_dataplatform.hbu.program import BuildingLevel
from hbu_dataplatform.cities.saguenay.zoning import (
    CLASS_MAX_DWELLINGS,
    SaguenayZoningClient,
    SaguenayZoningError,
    class_max_dwellings,
    parse_grid_pdf,
)
from hbu_dataplatform.zoning.zoning_grid import GridParseError

FONT_SIZE = 7.5
GLYPH = FONT_SIZE * 0.6
MARGIN = 30.0
LEADING = 11.0

#: What the fixtures put in a column to mark it. The published grids print a
#: black star (U+2605); the shared page builder writes its content stream in
#: cp1252, which has no such character, so the fixtures mark with a letter
#: instead. That is not a gap in the coverage: `_is_mark` decides by "printed
#: and not a number" precisely so the parser does not depend on which dingbat
#: the service draws, and `test_the_published_star_is_a_mark` pins the real
#: glyph against it directly.
MARK = "X"

#: Where each column is centred, in points, and where the ``min.`` caption
#: sits. Copied from the published grid for zone 70520, whose nine columns run
#: from 316 to 506 at a pitch of just under twenty-four points - tighter than
#: Montreal's forty, which is why `COLUMN_CLUSTER_TOLERANCE` is four and not
#: twelve.
COLUMNS = (316.0, 340.0, 363.0, 387.0, 410.0, 434.0, 458.0, 482.0, 506.0)
UNIT_CENTER = 228.0


def _at(center: float, text: str) -> float:
    return center - len(text) * GLYPH / 2


def grid_pdf(
    *,
    zone: str = "70520",
    codes: tuple[str, ...] = ("c4a", "c4b"),
    structures: dict[str, tuple[str, ...]] | None = None,
    storeys: tuple[str, ...] = ("1/3", "1/3"),
    lot_width: tuple[str, ...] = ("35", "35"),
    front: tuple[str, ...] = ("15", "13"),
    side_one: tuple[str, ...] = ("6", "4"),
    side_two: tuple[str, ...] = ("6", "6"),
    side_on_street: tuple[str, ...] = ("15", "13"),
    rear: tuple[str, ...] = ("15", "8"),
    rear_on_street: tuple[str, ...] = ("15", "8"),
    height_sentence: str | None = (
        "La hauteur totale maximale à respecter pour le bâtiment principal est "
        "de 12,5 mètres."
    ),
    only_permitted: str | None = None,
    excluded: str | None = None,
    articles: str | None = "Article 1083 à 1085 du chapitre 9",
    columns: tuple[float, ...] = COLUMNS,
    upside_down: bool = True,
) -> bytes:
    """A one-page PDF of a Saguenay grid, in the published layout.

    ``upside_down`` reproduces what the city's reporting service actually
    emits - section 13 nearest the top of the text space and section 1 nearest
    the bottom - which is the default because it is what every published grid
    does. Pass ``False`` for the ordinary convention; the parse must agree.
    """
    structures = structures or {"Détachée (isolée)": (MARK,) * len(codes)}
    rows: list[tuple[str, str, tuple[str, ...]]] = []

    def line(label: str, unit: str = "", *values: str) -> None:
        rows.append((label, unit, values))

    line("Règlement de zonage VS-R-2012-3")
    line("Grille des usages et des normes")
    line("1- CLASSES D'USAGES PERMISES")
    # The class code is printed *in* the column it heads, with the by-law's
    # prose for it running down the left.
    for index, code in enumerate(codes):
        padded = tuple("" if other != index else code for other in range(len(codes)))
        line(f"Usage {code}.", "", *padded)
    line("2- USAGE SPÉCIFIQUEMENT AUTORISÉ")
    if only_permitted:
        line(only_permitted, "", *((MARK,) * len(codes)))
    line("3- USAGE SPÉCIFIQUEMENT EXCLU")
    if excluded:
        line(excluded, "", *((MARK,) * len(codes)))
    line("4- STRUCTURE DU BÂTIMENT PRINCIPAL")
    for label, marks in structures.items():
        line(label, "", *marks)
    line("5- NORMES DE LOTISSEMENT")
    line("5-1- TERRAIN")
    line("Largeur (mètre)", "min.", *lot_width)
    line("6- NORMES DE ZONAGE")
    line("6-1 - MARGES DU BÂTIMENT PRINCIPAL")
    line("Avant (mètre)", "min.", *front)
    line("Latérale 1 (mètre)", "min.", *side_one)
    line("Latérale 2 (mètre)", "min.", *side_two)
    line("Latérale sur rue (mètre)", "min.", *side_on_street)
    line("Arrière (mètre)", "min.", *rear)
    line("Arrière sur rue (mètre)", "min.", *rear_on_street)
    line("6-2 - DIMENSIONS DU BÂTIMENT PRINCIPAL")
    line("Hauteur (étage)", "min./max.", *storeys)
    # The building's own width, which is *not* the lot's - the same label in
    # two sections, which is the one thing the section tracking has to get
    # right. A grid stating 8 m here and 35 m above states a 35 m lot.
    line("Largeur (mètre)", "min.", "8", "8")
    line("6-3 - RAPPORTS DU BÂTIMENT PRINCIPAL")
    line("7- AUTRES RÈGLEMENTS APPLICABLES")
    line("8- ARTICLES APPLICABLES")
    if articles:
        line(articles)
    line("9- NORMES SPÉCIFIQUES")
    if height_sentence:
        line(height_sentence)
    line("10- DISPOSITIONS PARTICULIÈRES")
    line("11- NOTES (ARTICLES)")
    line("12- AVIS DE MOTION")
    line("13- AMENDEMENTS")

    items: list[tuple[float, float, str]] = []
    top = 747.0
    # The zone number rides in the page header, beside the by-law's name.
    header_row = 1
    for index, (label, unit, values) in enumerate(rows):
        items.append((MARGIN, top, label))
        if index == header_row:
            items.append((520.0, top, f"Zone {zone}"))
        if unit:
            items.append((_at(UNIT_CENTER, unit), top, unit))
        for value, center in zip(values, columns):
            if value:
                items.append((_at(center, value), top, value))
        top -= LEADING

    if upside_down:
        # Mirror every baseline about the middle of the used band, which turns
        # the page over without moving anything sideways: the columns are the
        # point, and they must not shift.
        highest = max(y for _, y, _ in items)
        lowest = min(y for _, y, _ in items)
        items = [(x, highest + lowest - y, text) for x, y, text in items]

    return _pdf([item for item in items if item[2]])


# -- the registry ------------------------------------------------------------


def test_saguenay_is_a_city_of_its_own():
    assert city_of("SAG") is City.SAGUENAY
    assert "SAG" in known_neighborhoods()
    assert municipality_code_for(City.SAGUENAY) == "94068"


def test_the_metric_projection_is_mtm_zone_seven():
    """The city sits inside MTM 7, nearer its meridian than Quebec City is."""
    assert metric_crs_for("SAG") == "EPSG:32187"
    assert metric_srid_for("SAG") == 32187


def test_the_cmhc_crosswalk_covers_the_whole_city_and_no_subtotal():
    assert cmhc_centre_for("SAG") == "Saguenay"
    quartiers = quartiers_for("SAG")
    assert "Chicoutimi-Nord" in quartiers and "La Baie" in quartiers
    # A `Total` row beside the quartiers it sums would count the stock twice.
    assert "Total" not in quartiers


def test_there_is_no_marketbeat_submarket():
    """C&W publishes none for Saguenay; `rent_assets` flags the proxy."""
    assert submarket_for("SAG") is None


def test_the_outline_is_selected_by_type_as_well_as_name():
    """*Chicoutimi* is an arrondissement and a secteur; the city is neither."""
    assert saguenay_outline_name_for("SAG") == "Saguenay"
    assert SAGUENAY_OUTLINE_TYPE == "ville"
    with pytest.raises(KeyError, match="Unknown neighborhood"):
        saguenay_outline_name_for("Nowhere")


# -- the grid ----------------------------------------------------------------


def test_the_zone_number_comes_off_the_page_header():
    (first, _) = parse_grid_pdf(grid_pdf(zone="70520"))
    assert first.zone == "70520"


def test_each_column_takes_its_own_row_of_margins():
    """The whole point of reading position rather than text."""
    first, second = parse_grid_pdf(grid_pdf())

    assert first.front_margin_min_m == 15.0
    assert second.front_margin_min_m == 13.0
    assert first.rear_margin_min_m == 15.0
    assert second.rear_margin_min_m == 8.0


def test_the_side_margin_is_the_mean_of_the_two_the_grid_prints():
    """One field, two printed figures, and the area has to come out right.

    The carve subtracts one distance from every side-class edge, so 4 m on one
    side and 6 m on the other becomes 5 m on both - which removes exactly the
    same ground from a parcel whose side lines are parallel, and a side line is
    parallel by definition. Both printed figures survive in the note.
    """
    first, second = parse_grid_pdf(grid_pdf())

    assert first.side_margin_min_m == 6.0  # 6 and 6, so nothing to average
    assert second.side_margin_min_m == 5.0  # 4 and 6
    assert not any("mean of" in note for note in first.notes)
    assert any(
        "mean of Laterale 1 4 m and Laterale 2 6 m" in note for note in second.notes
    )


def test_a_single_side_margin_is_taken_as_printed():
    (only,) = parse_grid_pdf(
        grid_pdf(codes=("c4a",), side_one=("3",), side_two=("",))
    )
    assert only.side_margin_min_m == 3.0


def test_both_on_street_margins_are_carried_for_the_lot_to_choose():
    """The zone states both; which applies is a fact about the parcel.

    A corner lot's second street edge is a side line and a through lot's is its
    rear line, so collapsing the two here would decide per zone what has to be
    decided per lot. `compute_lot_buildable_setbacks` makes the choice.
    """
    first, second = parse_grid_pdf(grid_pdf())

    assert first.secondary_front_margin_min_m == 15.0
    assert first.rear_on_street_margin_min_m == 15.0
    # The column where the by-law prices them apart: 13 m against a street on
    # the side, 8 m against one at the rear.
    assert second.secondary_front_margin_min_m == 13.0
    assert second.rear_on_street_margin_min_m == 8.0


def test_the_lot_width_comes_from_the_lotting_section_not_the_building():
    """*Largeur (mètre)* is printed twice and means two different things."""
    first, _ = parse_grid_pdf(grid_pdf(lot_width=("35", "35")))
    # 35 is the terrain's minimum width; 8 is the building's, and is not a
    # constraint on the parcel.
    assert first.min_lot_width_m == 35.0


def test_storeys_come_off_the_min_max_cell():
    first, _ = parse_grid_pdf(grid_pdf(storeys=("1/3", "2/8")))
    second = parse_grid_pdf(grid_pdf(storeys=("1/3", "2/8")))[1]

    assert (first.floors_min, first.floors_max) == (1, 3)
    assert (second.floors_min, second.floors_max) == (2, 8)


def test_a_grid_with_no_storey_ceiling_falls_back_to_its_height_sentence():
    """Saguenay states the height in prose, and that is a ceiling too.

    The storey cell is blank and the column reports it blank; the sentence
    under the grid says 12,5 metres, and four storeys at the shortest storey
    this platform builds is what the solver gets. Nothing here is invented -
    `floors_max` on the parsed column stays `None`.
    """
    (only,) = parse_grid_pdf(grid_pdf(codes=("c4a",), storeys=("",)))

    assert only.floors_max is None
    assert only.height_max_m == 12.5
    assert only.to_zone_column().floors_max == 4


def test_a_grid_with_neither_a_storey_ceiling_nor_a_height_is_not_solver_ready():
    """The one field the solver cannot do without, in either of its two forms."""
    (only,) = parse_grid_pdf(
        grid_pdf(codes=("c4a",), storeys=("",), height_sentence=None)
    )

    assert only.floors_max is None
    assert only.height_max_m is None
    with pytest.raises(GridParseError, match="no storey"):
        only.to_zone_column()


def test_the_height_ceiling_is_read_out_of_the_prose_it_is_stated_in():
    """Saguenay states it in a sentence, not a cell, and it still binds."""
    first, second = parse_grid_pdf(grid_pdf())

    assert first.height_max_m == 12.5
    assert second.height_max_m == 12.5
    assert any("read from the zone's stated norm" in n for n in first.notes)


def test_a_grid_stating_no_height_sentence_states_no_height():
    (only,) = parse_grid_pdf(grid_pdf(codes=("c4a",), height_sentence=None))
    assert only.height_max_m is None


def test_the_published_star_is_a_mark_and_a_norm_is_not():
    """What `_is_mark` actually decides, against the glyph the service draws.

    The fixtures above mark with a letter because the page builder's content
    stream is cp1252 and U+2605 is not in it. This is where the real character
    is pinned, alongside the cells that must *not* read as marks: a number is a
    norm, a unit caption is furniture, and a dash is an absent norm.
    """
    from hbu_dataplatform.cities.saguenay.zoning import Cell, _is_mark

    def cell(text: str) -> Cell:
        return Cell(start=310.0, end=322.0, text=text)

    assert _is_mark(cell("★"))
    assert _is_mark(cell(MARK))
    assert not _is_mark(cell("15"))
    assert not _is_mark(cell("1/3"))  # a storey pair is not a mark either
    assert not _is_mark(cell("min."))
    assert not _is_mark(cell("-"))
    assert not _is_mark(cell(""))


def test_the_structure_rows_become_the_implantation_letters():
    """The mode decides whether the side margin applies at all, in `postgis`."""
    (only,) = parse_grid_pdf(
        grid_pdf(
            codes=("H01",),
            structures={
                "Détachée (isolée)": (MARK,),
                "Jumelée": (MARK,),
                "En rangée": (MARK,),
            },
        )
    )
    assert only.implantation_mode == "I-J-C"


def test_an_unstarred_structure_is_not_permitted():
    (only,) = parse_grid_pdf(
        grid_pdf(
            codes=("H01",),
            structures={"Détachée (isolée)": (MARK,), "En rangée": ("",)},
        )
    )
    assert only.implantation_mode == "I"


# -- usages ------------------------------------------------------------------


def test_usage_codes_become_the_families_the_solver_matches():
    """Saguenay's classes are not Montreal's, so the family letter travels."""
    housing, commerce = parse_grid_pdf(grid_pdf(codes=("H01", "c4a")))

    assert housing.usages == ("H",)
    assert housing.usages_by_category == {"habitation": "H01"}
    assert housing.permits_residential
    assert commerce.usages == ("C",)
    assert commerce.usages_by_category == {"commerce": "C4a"}


def test_services_are_priced_as_commerce():
    """``S`` is offices and personal services - Montreal's ``C`` classes."""
    (only,) = parse_grid_pdf(grid_pdf(codes=("S1",)))
    assert only.usages == ("C",)


def test_parks_and_recreation_are_equipment_and_are_not_priced():
    park, recreation = parse_grid_pdf(grid_pdf(codes=("p1a", "r2a")))
    assert park.usages == ("E",)
    assert recreation.usages == ("E",)


def test_agriculture_is_reported_on_the_column_and_not_priced():
    """Nothing here prices a field, so ``A`` is a note rather than a usage.

    The column is still a column - it states margins and storeys like any
    other, and a parcel in it still has an envelope - so what the family
    changes is only what the solver may put in it.
    """
    (only,) = parse_grid_pdf(grid_pdf(codes=("A1",), storeys=("1/2",)))

    assert only.usages == ()
    assert only.usages_by_category == {}
    assert any("A1" in note and "priced by nothing" in note for note in only.notes)
    # ...and the norms beside it are read as usual.
    assert only.front_margin_min_m == 15.0


def test_the_dwelling_ceiling_comes_from_the_class_the_by_law_names():
    """The grid prints no dwelling count anywhere, so the class is all there is."""
    assert class_max_dwellings(["H01"])[0] == 1
    assert class_max_dwellings(["H02"])[0] == 2
    assert class_max_dwellings(["H03"])[0] == 3
    assert class_max_dwellings(["C4a"])[0] is None


def test_an_open_ended_class_lifts_the_ceiling_rather_than_capping_it():
    """A column headed H01 *and* H04 authorises both, so the looser governs."""
    ceiling, note = class_max_dwellings(["H01", "H04"])

    assert ceiling is None
    assert "H04" in note and "no dwelling ceiling" in note
    # ...and the multifamily classes are honestly unknown rather than guessed.
    assert CLASS_MAX_DWELLINGS["H04"] is None


def test_a_specifically_authorised_use_lands_on_the_columns_it_is_starred_for():
    (only,) = parse_grid_pdf(
        grid_pdf(codes=("H09",), only_permitted="Centre équestre")
    )
    assert only.only_permitted_usages == "Centre équestre"


def test_a_specifically_excluded_use_lands_the_same_way():
    (only,) = parse_grid_pdf(grid_pdf(codes=("c4a",), excluded="Débit de boisson"))
    assert only.excluded_usages == "Débit de boisson"


def test_the_applicable_articles_are_stated_once_for_the_zone():
    first, second = parse_grid_pdf(grid_pdf())
    assert first.specific_articles == second.specific_articles
    assert "1083" in first.specific_articles


# -- the page itself ---------------------------------------------------------


def test_a_grid_reads_the_same_either_way_up():
    """The published documents run up the page; nothing else may depend on it.

    `_reading_order` decides from the section numbers rather than from the
    coordinate convention, so the same grid typeset both ways must parse to
    the same columns.
    """
    up = parse_grid_pdf(grid_pdf(upside_down=True))
    down = parse_grid_pdf(grid_pdf(upside_down=False))

    assert len(up) == len(down) == 2
    for a, b in zip(up, down):
        assert a == b


def test_a_grid_that_states_no_norms_yields_no_column():
    """A zone drawn over a river prints its classes and nothing else.

    Common enough that raising would cost the partition the zones that do
    state norms - Saguenay draws one over every park and rail yard it has.
    """
    columns = parse_grid_pdf(
        grid_pdf(
            codes=("p1a",),
            lot_width=("",),
            front=("",),
            side_one=("",),
            side_two=("",),
            side_on_street=("",),
            rear=("",),
            rear_on_street=("",),
            storeys=("",),
        )
    )
    assert columns == []


def test_a_document_that_is_not_a_grid_is_refused():
    not_a_grid = _pdf([(72.0, 700.0, "Avis public"), (72.0, 680.0, "Zone 70520")])
    with pytest.raises(GridParseError, match="no page carries"):
        parse_grid_pdf(not_a_grid, url="http://x/notice.pdf")


def test_columns_are_numbered_across_a_multi_page_grid():
    """A wide zone is printed as a sheet per slice, and they are one grid.

    `column_index` is part of the key `silver.zoning_grid_columns` is written
    on, so restarting it per sheet would collapse the second onto the first.
    """
    page = grid_pdf(codes=("c4a", "c4b"))
    both = _two_pages(page, grid_pdf(codes=("I1", "I2")))

    columns = parse_grid_pdf(both)

    assert [c.column_index for c in columns] == [0, 1, 2, 3]
    assert [c.usages for c in columns] == [("C",), ("C",), ("I",), ("I",)]


def _two_pages(first: bytes, second: bytes) -> bytes:
    """The two fixtures as one document, via pypdf's writer."""
    import io

    from pypdf import PdfReader, PdfWriter

    writer = PdfWriter()
    for content in (first, second):
        for page in PdfReader(io.BytesIO(content)).pages:
            writer.add_page(page)
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


# -- the client --------------------------------------------------------------


class _FakeResponse:
    def __init__(self, payload=None, content=b"", status=200):
        self._payload = payload
        self.content = content
        self.status_code = status
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class _FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.posts: list[dict] = []
        self.headers: dict[str, str] = {}

    def post(self, url, data=None, timeout=None):
        self.posts.append(data or {})
        return _FakeResponse(self.payload)

    def get(self, url, timeout=None):
        return _FakeResponse(content=b"%PDF-1.4 ...")


def _client(payload):
    session = _FakeSession(payload)
    client = SaguenayZoningClient(session=session, request_delay_seconds=0)
    return client, session


def test_the_lookup_posts_the_zone_number_and_returns_its_id():
    """A GET, or any other field, comes back unfiltered - hence the POST."""
    client, session = _client(
        {
            "meta": {"totalCount": 1},
            "data": [{"id": 2700, "numero_zone": 70520, "code_activite": 0}],
        }
    )

    assert client.zone_id("70520") == 2700
    assert session.posts == [{"no_zone": "70520"}]


def test_a_zone_the_service_does_not_list_is_not_an_error():
    """The layer draws 2,837 numbers and the service lists 2,919 of its own."""
    client, _ = _client({"meta": {"totalCount": 0}, "data": []})
    assert client.zone_id("99999") is None


def test_a_lookup_that_came_back_unfiltered_is_refused():
    """Every zone resolving to the same first row is worse than none resolving."""
    client, _ = _client(
        {
            "meta": {"totalCount": 2919},
            "data": [{"id": 2701, "numero_zone": 27990, "code_activite": 0}],
        }
    )
    with pytest.raises(SaguenayZoningError, match="not filtered"):
        client.zone_id("70520")


def test_a_row_for_another_zone_is_not_taken_as_this_one():
    client, _ = _client(
        {
            "meta": {"totalCount": 1},
            "data": [{"id": 2701, "numero_zone": 27990, "code_activite": 0}],
        }
    )
    assert client.zone_id("70520") is None


def test_the_grid_url_is_keyed_on_the_service_id_not_the_zone_number():
    client, _ = _client({"meta": {"totalCount": 0}, "data": []})
    assert client.grid_url_for(2700).startswith(
        "https://zonage.saguenay.ca/rapports/v1/zonages/grille/pdf/2700"
    )
