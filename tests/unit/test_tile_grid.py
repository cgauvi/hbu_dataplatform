"""The tile grid the low-zoom map aggregates are built on, and the SQL of it.

Two properties carry the whole design and both are tested here directly:

**A parent is the four cells that reduce to it.** Everything in
`compute_map_cell_aggregates` above the base level is a rollup that halves the
two cell indices, and the counts are only exact if the cell a feature lands in
at zoom 19 reduces to the cell it would have landed in at zoom 15. That is
`test_the_pyramid_agrees_with_direct_assignment`, and it is the test to keep if
all the others go.

**The SQL and the Python compute the same cell.** `tile_grid.cell_of` is the
readable spelling and `postgis._cell_x_sql`/`_cell_y_sql` are the one that
actually runs, and a grid that is written twice is a grid that will disagree
with itself. Nothing here reaches a database - the generated expression is
arithmetic, so it is translated into Python and evaluated, which checks the
formula and not Postgres. What that catches is the whole class of bug worth
catching offline: a wrong span, a flipped axis, a missing clamp. What it does
not catch is a function Postgres spells differently, which is why
`tests/integration` materializes the asset against a real database.
"""

from __future__ import annotations

import math

import pytest

from hbu_dataplatform.core import postgis, tile_grid

#: Real coordinates, because the grid is only interesting where the data is.
#: Villeray, the southern tip of the island, and a point out in the river that
#: no layer covers - the last one for the clamps rather than for the cadastre.
POINTS = [
    (-73.6200, 45.5535),
    (-73.6395, 45.5410),
    (-73.5680, 45.5900),
    (-73.5545, 45.4700),
    (-73.5000, 45.6200),
]


# ---------------------------------------------------------------------------
# the grid itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lon,lat", POINTS)
@pytest.mark.parametrize("zoom", tile_grid.CELL_ZOOMS)
def test_a_point_is_inside_the_cell_it_is_assigned_to(lon, lat, zoom):
    """The round trip: the cell holding a point contains that point.

    `cell_of` and `tile_bounds` are inverses of each other, and if they are not
    then every clip in the rollup is against a box that does not belong to the
    cell it is filed under - which would be invisible in the output, since the
    map would still draw *something* everywhere.
    """
    x, y = tile_grid.cell_of(lon, lat, zoom)
    west, south, east, north = tile_grid.tile_bounds(zoom, x, y)
    assert west <= lon <= east
    assert south <= lat <= north


@pytest.mark.parametrize("lon,lat", POINTS)
def test_the_pyramid_agrees_with_direct_assignment(lon, lat):
    """Rolling a cell up N levels lands where assigning it directly would.

    The property every summed measure in `gold.map_cell_aggregates` depends on.
    The rollup never re-reads a feature - it halves the indices of the level
    below - so if this failed, a borough's counts would be right at zoom 19 and
    quietly wrong at every level above it.
    """
    base = tile_grid.cell_of(lon, lat, tile_grid.BASE_CELL_ZOOM)
    for zoom in tile_grid.CELL_ZOOMS:
        levels = tile_grid.BASE_CELL_ZOOM - zoom
        assert tile_grid.parent_of(*base, levels) == tile_grid.cell_of(lon, lat, zoom)


def test_a_served_tile_holds_exactly_the_cells_it_claims_to():
    """The bound the design rests on, as arithmetic rather than as a comment.

    A tile at display zoom Z is subdivided into `cells_per_tile()` cells at
    Z + ZOOM_OFFSET, which is what caps a served tile's feature count no matter
    how dense the borough is.
    """
    assert tile_grid.cells_per_tile() == 4**tile_grid.ZOOM_OFFSET
    zoom = 12
    cell_zoom = tile_grid.cell_zoom_for(zoom)
    assert cell_zoom == zoom + tile_grid.ZOOM_OFFSET

    west, south, east, north = tile_grid.tile_bounds(zoom, 1234, 1478)
    # Every corner of the tile, nudged inwards so a boundary does not land in
    # the neighbour, has to fall in the same block of child cells.
    nudge = (east - west) * 1e-6
    corners = [
        (west + nudge, south + nudge),
        (east - nudge, south + nudge),
        (west + nudge, north - nudge),
        (east - nudge, north - nudge),
    ]
    children = {tile_grid.cell_of(lon, lat, cell_zoom) for lon, lat in corners}
    parents = {
        tile_grid.parent_of(x, y, tile_grid.ZOOM_OFFSET) for x, y in children
    }
    assert parents == {(1234, 1478)}


def test_latitudes_beyond_mercator_are_clamped_rather_than_infinite():
    """A pole has no row in the grid, so it gets the last one instead.

    Nothing in Montreal is near this. It is here because the alternative to a
    clamp is `tan(pi/2)`, and a NaN cell index would take down a partition over
    one stray geometry rather than misplacing it.
    """
    for lat in (90.0, -90.0, 89.9, -89.9):
        x, y = tile_grid.cell_of(-73.62, lat, 19)
        assert 0 <= y < (1 << 19)
        assert 0 <= x < (1 << 19)


# ---------------------------------------------------------------------------
# the SQL spelling of the same grid
# ---------------------------------------------------------------------------


def _as_python(expression: str) -> str:
    """The SQL cell expression as something Python can evaluate.

    Only the spellings differ - `greatest`/`least` for `max`/`min`, the cast to
    `integer` that Python does not need, `pi()` as a call. The arithmetic is untouched, which
    is the point: this evaluates the expression that will be sent to Postgres
    rather than a second copy of the formula written for the test.
    """
    return expression.replace("::integer", "")


_SQL_NAMESPACE = {
    "greatest": max,
    "least": min,
    "floor": math.floor,
    "asinh": math.asinh,
    "tan": math.tan,
    "radians": math.radians,
    "pi": lambda: math.pi,
}


@pytest.mark.parametrize("lon,lat", POINTS)
@pytest.mark.parametrize("zoom", tile_grid.CELL_ZOOMS)
def test_the_sql_and_the_python_pick_the_same_cell(lon, lat, zoom):
    """The two spellings of the grid agree, which is why there may be two."""
    x_sql = _as_python(postgis._cell_x_sql("LON", zoom))
    y_sql = _as_python(postgis._cell_y_sql("LAT", zoom))
    namespace = {**_SQL_NAMESPACE, "LON": lon, "LAT": lat}

    assert (eval(x_sql, namespace), eval(y_sql, namespace)) == tile_grid.cell_of(
        lon, lat, zoom
    )


def test_the_sql_clamps_a_latitude_off_the_grid():
    """The clamp survives translation into SQL, not just into Python."""
    y_sql = _as_python(postgis._cell_y_sql("LAT", 19))
    for lat in (90.0, -90.0):
        row = eval(y_sql, {**_SQL_NAMESPACE, "LAT": lat})
        assert 0 <= row < (1 << 19)


# ---------------------------------------------------------------------------
# the layer specs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layer", tile_grid.LAYERS)
def test_every_layer_shades_on_a_measure_it_actually_computes(layer):
    """A `value` naming a measure nothing produces is NULL on every cell.

    The failure this prevents is silent and total: the column resolves to a
    jsonb key that is never written, every cell comes back NULL, and the map
    draws the whole borough in the "not answered" colour - which is a legible
    thing for it to draw, so nobody would read it as a bug.
    """
    spec = tile_grid.layer_spec(layer)
    known = set(tile_grid.UNIVERSAL_MEASURES) | set(spec.point_measures)
    assert spec.numerator in known, f"{layer}: {spec.numerator} is not computed"
    assert spec.denominator in known, f"{layer}: {spec.denominator} is not computed"


@pytest.mark.parametrize("layer", tile_grid.LAYERS)
def test_every_layer_screens_on_the_partition(layer):
    """No layer may summarise one borough's ground against another's cadastre."""
    spec = tile_grid.layer_spec(layer)
    assert "%(neighborhood)s" in spec.where
    assert "%(scrape_date)s" in spec.where


@pytest.mark.parametrize("layer", tile_grid.LAYERS)
def test_the_dimension_matches_what_the_layer_is(layer):
    """`streets` is linework and the other four are areal.

    Extracting the wrong dimension does not error - it returns an empty
    geometry of the type asked for - so a layer mis-declared here would simply
    produce no cells at all.
    """
    spec = tile_grid.layer_spec(layer)
    assert spec.dimension == (2 if layer == "streets" else 3)


def test_an_unknown_layer_is_named_in_the_error():
    with pytest.raises(KeyError, match="parks"):
        tile_grid.layer_spec("parks")


# ---------------------------------------------------------------------------
# the generated statements
# ---------------------------------------------------------------------------


def _normalise(statement: str) -> str:
    return " ".join(statement.split())


def _top_level_expressions(select_list: str) -> list[str]:
    """Split a SQL select list on its top-level commas.

    Depth-aware, because half these expressions are `COALESCE(a, b)` and a
    naive `split(",")` would count each of those as two columns - which would
    make the test below pass for the wrong reason and keep passing when the
    alignment actually broke.
    """
    expressions: list[str] = []
    depth = 0
    current = ""
    for char in select_list:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            expressions.append(current.strip())
            current = ""
            continue
        current += char
    if current.strip():
        expressions.append(current.strip())
    return expressions


def test_the_final_select_produces_every_column_the_upsert_writes():
    """The SELECT list and the column list are one thing written twice.

    `warehouse.upsert_select` pairs them **positionally**, so a column added to
    one and not the other is an insert whose values all land one place to the
    left of where they belong - and Postgres only raises where the types
    happen to disagree, which for a run of five `double precision` columns is
    nowhere at all.
    """
    statement = _normalise(postgis._aggregate_select(tile_grid.LAYERS))
    body = statement.split("SELECT %(scrape_date)s::date,")[1].split(" FROM shape ")[0]
    expressions = _top_level_expressions("%(scrape_date)s::date," + body)

    assert len(expressions) == len(postgis.MAP_CELL_COLUMNS)
    # And they are in the order the column list names, spot-checked where the
    # name appears in the expression that produces it.
    for column, expression in zip(postgis.MAP_CELL_COLUMNS, expressions):
        if column in ("layer", "cell_z", "cell_x", "cell_y", "geom"):
            assert expression == f"shape.{column}"
        if column in ("dissolved_area_m2", "dissolved_length_m", "cell_area_m2"):
            assert expression == f"shape.{column}"
        if column == "attributes":
            assert expression.startswith("COALESCE(measures.attributes")
        if column == "feature_count":
            assert expression.startswith("COALESCE(measures.feature_count")


def test_the_select_has_a_case_arm_for_every_layer_it_is_built_for():
    """A layer with no arm gets a NULL `value_kind`, which the table rejects.

    That is the good failure. The bad one is a layer whose arm is missing from
    the *value* CASE only - it would insert cleanly with a NULL value and draw
    as an entirely un-answered borough.
    """
    statement = _normalise(postgis._aggregate_select(tile_grid.LAYERS))
    for layer in tile_grid.LAYERS:
        spec = tile_grid.layer_spec(layer)
        assert f"WHEN '{layer}' THEN" in statement
        assert f"'{spec.value_kind}'" in statement
    # Twice per layer: once for the value, once for the kind that names it.
    assert statement.count("WHEN '") == 2 * len(tile_grid.LAYERS)


def test_a_narrowed_run_still_produces_one_arm_per_layer_it_writes():
    """`MapAggregateConfig.layers` may narrow the run, and the CASE follows."""
    statement = _normalise(postgis._aggregate_select(("lots",)))
    assert statement.count("WHEN '") == 2
    assert "WHEN 'lots' THEN" in statement
    assert "WHEN 'massing' THEN" not in statement


def test_a_ratio_divides_by_nullif_so_an_empty_cell_is_not_a_zero():
    """The difference between "no programme here" and "0% of it used".

    Both would be drawn, and one of them is the opposite of the truth.
    """
    spec = tile_grid.layer_spec("capacity")
    expression = postgis._value_expression(spec)
    assert "NULLIF(" in expression
    assert "hbu_floor_area_m2" in expression
    assert "existing_floor_area_m2" in expression


def test_capacity_shades_on_summed_floor_area_not_on_averaged_percentages():
    """The one number on this table that is easy to get plausibly wrong.

    A cell's `used_pct` has to be `sum(existing) / sum(hbu)`. The mean of the
    lots' own percentages is a different number, it is between 0 and 100 like
    the right one, and the band palette would colour it without complaint.
    """
    spec = tile_grid.layer_spec("capacity")
    assert spec.numerator == "existing_floor_area_m2"
    assert spec.denominator == "hbu_floor_area_m2"
    assert spec.scale == 100.0
    # Both sides have to be summed per feature, which is what puts them in
    # `point_measures` rather than leaving them to be averaged later.
    assert set(spec.point_measures) >= {"existing_floor_area_m2", "hbu_floor_area_m2"}


def test_the_measure_seed_counts_each_feature_once():
    """`feature_count` rides as a measure of 1, summed - not as a count(*).

    That is what lets it roll up through the same generic statement as every
    other measure, and it is why a feature straddling four cells still counts
    once: it is assigned by a single representative point.

    `ST_PointOnSurface` and not `ST_Centroid`, which is the choice worth
    pinning: the centroid of an L-shaped parcel can fall outside it, and on a
    grid this fine that is a lot filed under ground it does not touch.
    """
    recorded = _FakeCursor()
    postgis._seed_cell_measures(
        recorded,
        tile_grid.layer_spec("lots"),
        {"layer": "lots", "neighborhood": "VSMPE", "scrape_date": "2026-08-27"},
    )
    statement = _normalise(recorded.statements[0][0])
    assert "ST_PointOnSurface" in statement
    assert "ST_Centroid" not in statement
    assert "('feature_count', (1.0)::double precision)" in statement
    assert "sum(measured.value)" in statement
    assert "count(*)" not in statement


def test_the_rollup_halves_indices_rather_than_re_reading_geometry():
    """The pyramid is a shift over the level below, not a second pass at the
    source.

    Both statements a level is built from have to read `_agg_*` and group on
    the halved indices. A rollup that reached back to `rag.lots` would be
    correct and four times slower at every level; one that grouped on the
    unshifted indices would silently copy the child level upwards.
    """
    recorded = _FakeCursor()
    postgis._roll_up_level(recorded, 17)
    issued = [_normalise(statement) for statement, _ in recorded.statements]
    assert len(issued) == 2

    cells, measures = issued
    assert "INSERT INTO _agg_cells" in cells
    assert "FROM _agg_cells" in cells
    assert "ST_Union(geom)" in cells

    assert "INSERT INTO _agg_measures" in measures
    assert "FROM _agg_measures" in measures
    assert "sum(value)" in measures

    for statement, params in recorded.statements:
        assert "cell_x >> 1, cell_y >> 1" in _normalise(statement)
        # Read the level one finer, write the level asked for. Reversing these
        # builds every level out of the coarsest one.
        assert params == {"zoom": 17, "child": 18}


@pytest.mark.parametrize("layer", tile_grid.LAYERS)
def test_the_geometry_seed_extracts_the_layer_dimension(layer):
    """Every clip goes through `ST_CollectionExtract` at the layer's dimension.

    Without it a polygon clipped along an edge it shares with the cell boundary
    contributes that edge to the union, and the map draws a hairline grid over
    the borough.
    """
    spec = tile_grid.layer_spec(layer)
    recorded = _FakeCursor()
    postgis._seed_cell_geometry(
        recorded, spec, {"layer": layer, "neighborhood": "VSMPE", "scrape_date": "2026-08-27"}
    )
    statement = _normalise(recorded.statements[0][0])
    assert "ST_CollectionExtract( ST_Intersection" in statement
    assert f", {spec.dimension} )" in statement
    assert "ST_Union(clip.geom)" in statement
    # The base level, and only the base level, touches raw geometry.
    assert f"{tile_grid.BASE_CELL_ZOOM}," in statement


# -- the quadkey: a cell's address as a string --------------------------------


@pytest.mark.parametrize("seed", [1, 7, 13])
def test_a_prefix_is_exactly_the_ancestor_parent_of_computes(seed):
    """The property the whole addressing scheme rests on.

    `parent_of` halves the two indices; chopping a character off a quadkey is
    supposed to be the same operation. If these two ever disagree, a row's
    stored address stops naming the cell it is grouped under, and every rollup
    keyed on a prefix quietly reads the wrong ground. This is the test to keep
    if all the others in this section go.
    """
    random = __import__("random").Random(seed)
    for _ in range(2_000):
        zoom = random.randint(1, tile_grid.BASE_CELL_ZOOM)
        lon = random.uniform(-179.9, 179.9)
        lat = random.uniform(-84.0, 84.0)
        x, y = tile_grid.cell_of(lon, lat, zoom)
        key = tile_grid.quadkey(zoom, x, y)

        assert tile_grid.cell_of_quadkey(key) == (zoom, x, y)
        assert tile_grid.quadkey_of(lon, lat, zoom) == key

        for levels in range(1, zoom):
            assert tile_grid.cell_of_quadkey(key[:-levels]) == (
                zoom - levels,
                *tile_grid.parent_of(x, y, levels),
            )


def test_quadkey_bounds_is_the_half_open_range_of_what_it_covers():
    """`cell_key >= lo AND cell_key < hi` has to select a cell's rows exactly.

    Half-open rather than `LIKE 'prefix%'` because a range over a plain text
    column is what an index answers; the equivalence is what this checks.
    """
    random = __import__("random").Random(11)
    for _ in range(2_000):
        depth = random.randint(1, tile_grid.BASE_CELL_ZOOM - 1)
        full = tile_grid.quadkey_of(
            random.uniform(-179.9, 179.9), random.uniform(-84.0, 84.0)
        )
        prefix = full[:depth]
        low, high = tile_grid.quadkey_bounds(prefix)

        assert low <= full < high
        assert full.startswith(prefix) == (low <= full < high)

        # A sibling's ground is outside the range, which is the half that
        # `LIKE` gets right by luck and a hand-rolled range gets wrong.
        sibling = prefix[:-1] + str((int(prefix[-1]) + 1) % 4)
        if sibling != prefix:
            assert not low <= sibling + full[depth:] < high


def test_the_last_cell_of_the_grid_has_an_upper_bound_at_all():
    """`"333..."` has no successor to be the exclusive end, so one is invented.

    `"4"` is not a quadkey and sorts above every string of `0-3`, which is the
    only thing the bound has to do. Getting this wrong loses the south-east
    corner of the world rather than erroring.
    """
    low, high = tile_grid.quadkey_bounds("333")
    assert high == tile_grid.ABOVE_ALL_QUADKEYS
    assert low <= "3" * tile_grid.BASE_CELL_ZOOM < high


def test_a_prefix_cannot_be_deeper_than_the_keys_it_bounds():
    with pytest.raises(ValueError, match="deeper than depth"):
        tile_grid.quadkey_bounds("0" * (tile_grid.BASE_CELL_ZOOM + 1))


# -- the third copy of the grid: hbu_infra's SQL function ----------------------

#: Where hbu_infra's `sql/` tree is, when it is not beside this checkout. The
#: same switch `tests/integration/conftest.py` uses.
INFRA_ENV = "URBAN_RAG_INFRA_SQL"


def _infra_sql_dir():
    import os
    import pathlib

    configured = os.environ.get(INFRA_ENV)
    if configured:
        return pathlib.Path(configured)
    return pathlib.Path(__file__).resolve().parents[2].parent / "hbu_infra" / "sql"


def _quadkey_sql_expressions():
    """The `x` and `y` expressions out of `warehouse.quadkey`, as written.

    Read from the file that will actually be applied rather than copied into
    this test, for the reason the `_cell_x_sql` tests above give: a grid
    written twice is a grid that will disagree with itself, and this is now the
    *third* spelling of it - `tile_grid.cell_of`, `postgis._cell_x_sql`, and
    hbu_infra's function.
    """
    import re

    path = _infra_sql_dir() / "028_cell_key.sql"
    if not path.is_file():
        pytest.skip(f"hbu_infra sql/ not found at {path.parent} - set {INFRA_ENV}")
    body = path.read_text(encoding="utf-8")

    match = re.search(
        r"greatest\(0, least\(\(1 << z\) - 1,\s*(.+?)\s*\)\) AS x.*?"
        r"greatest\(0, least\(\(1 << z\) - 1,\s*(.+?)\s*\)\) AS y",
        body,
        re.DOTALL,
    )
    assert match, "028_cell_key.sql no longer has the two clamped expressions"
    return match.group(1), match.group(2)


def test_the_sql_function_addresses_the_same_cell_as_the_python(monkeypatch):
    """hbu_infra's `warehouse.quadkey` and `tile_grid.quadkey_of`, side by side.

    The arithmetic is evaluated rather than re-implemented - `greatest`/`least`
    for `max`/`min`, `pi()` as a call, the casts dropped - so this checks the
    expression that will be sent to Postgres. What it cannot check is a
    function Postgres spells differently; `tests/integration` is where that is
    caught, against a real database.
    """
    x_sql, y_sql = _quadkey_sql_expressions()

    def sql_cell(lon, lat, zoom):
        namespace = dict(_SQL_NAMESPACE, lon=lon, lat=lat, z=zoom)
        x = eval(_as_python(x_sql).replace("::bigint", ""), {}, namespace)  # noqa: S307
        y = eval(_as_python(y_sql).replace("::bigint", ""), {}, namespace)  # noqa: S307
        # The clamp the regex left outside the captured expressions.
        span = 1 << zoom
        return max(0, min(span - 1, int(x))), max(0, min(span - 1, int(y)))

    for lon, lat in POINTS:
        for zoom in (6, 11, 15, tile_grid.BASE_CELL_ZOOM):
            assert sql_cell(lon, lat, zoom) == tile_grid.cell_of(lon, lat, zoom)

    # And the string the function builds from those two, digit for digit.
    for lon, lat in POINTS:
        zoom = tile_grid.BASE_CELL_ZOOM
        x, y = sql_cell(lon, lat, zoom)
        digits = "".join(
            str(
                (1 if (x >> (level - 1)) & 1 else 0)
                + (2 if (y >> (level - 1)) & 1 else 0)
            )
            for level in range(zoom, 0, -1)  # the SQL's ORDER BY lvl DESC
        )
        assert digits == tile_grid.quadkey_of(lon, lat, zoom)


def test_the_sql_stores_the_address_at_the_zoom_the_pyramid_is_seeded_at():
    """19 in the file has to be `BASE_CELL_ZOOM`, or every coarser address a
    prefix of it would name the wrong cell."""
    path = _infra_sql_dir() / "028_cell_key.sql"
    if not path.is_file():
        pytest.skip(f"hbu_infra sql/ not found at {path.parent} - set {INFRA_ENV}")
    body = path.read_text(encoding="utf-8")

    assert body.count(f"ST_Y(ST_PointOnSurface(geom)), {tile_grid.BASE_CELL_ZOOM}") == 3

    # Centroid is the bug this column exists to avoid, so it must not be
    # *called* - though the file is expected to name it, since explaining why
    # it is not used is half of what that comment block is for.
    statements = "\n".join(
        line for line in body.splitlines() if not line.lstrip().startswith("--")
    )
    assert "ST_Centroid" not in statements


# -- the cut: variable depth over the same grid --------------------------------


def test_cut_cell_of_resolves_a_ragged_cut_without_geometry():
    """A cut is deliberately not uniform: dense ground is cut finer.

    So the lookup cannot assume a depth, and walks prefixes shortest-first.
    """
    cut = {"0", "10", "11", "120", "121", "122", "123", "13", "2", "3"}
    tile_grid.validate_cut(cut)

    assert tile_grid.cut_cell_of("0333", cut) == "0"
    assert tile_grid.cut_cell_of("1012", cut) == "10"
    assert tile_grid.cut_cell_of("1213", cut) == "121"
    assert tile_grid.cut_cell_of("31", cut) == "3"

    with pytest.raises(KeyError, match="no cell of the cut"):
        tile_grid.cut_cell_of("9", cut)


def test_a_cut_that_contains_a_cell_and_its_ancestor_is_refused():
    """Which of the two a lot belongs to would be a coin toss.

    `cut_cell_of` returns the shortest match, so a nested cut would not error -
    it would silently assign every lot under `"12"` to `"1"` instead. The check
    belongs where the cut is built, not where it is read.
    """
    with pytest.raises(ValueError, match="contains both"):
        tile_grid.validate_cut({"0", "1", "12", "2", "3"})

    with pytest.raises(ValueError, match="empty"):
        tile_grid.validate_cut(set())
    with pytest.raises(ValueError, match="root"):
        tile_grid.validate_cut({""})
    with pytest.raises(ValueError, match="not 0-3"):
        tile_grid.validate_cut({"1", "5"})


class _FakeCursor:
    """Records statements instead of running them - the shape `test_postgis_loads`
    uses, kept local because this file needs nothing else from a cursor."""

    def __init__(self):
        self.statements: list[tuple[str, object]] = []
        self.rowcount = 0

    def execute(self, statement: str, params=None):
        self.statements.append((statement, params))
        return self
