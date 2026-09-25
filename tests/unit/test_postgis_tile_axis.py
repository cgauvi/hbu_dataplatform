"""The tile axis, held across every `hbu_dataplatform.core.postgis` function that moved.

The per-function files - `test_postgis_intersections`, `_lot_profiles`,
`_setbacks` - drive one computation each against a fake cursor and read what
it asked for. What this file holds is the rule they all follow, stated once:
a converted function *writes* one cell and *reads* the snapshot. Its SQL
names `cell_partition = tile` on the owned set and `scrape_date` alone on
every pool, and nowhere - not in a predicate, not in a join - does it bind a
borough. `neighborhood` survives as a column the row carries, stamped from
the lot, and that is the only way the word may appear.

Two of the converted functions cannot be driven here at all without a fake
that knows the arity of every `fetchone` they make - `compute_lot_frontage`
alone unpacks five different tuples - so the rule is checked against the
*source* of each function and each SQL constant rather than against captured
statements. `inspect.getsource` sees the same string literal the cursor would.

Nothing here touches a database.
"""

from __future__ import annotations

import inspect
import re

import pytest
import shapely.wkb
from shapely.geometry import box

from hbu_dataplatform.core import postgis, warehouse

TILE = "0302303330102"
DATE = "2026-09-01"


# -- no converted function binds a borough ----------------------------------

#: Every function converted to `tile=`, the readers included. `load_streets`
#: is here because its write is the cell's; the loaders that stay on the
#: borough axis (`load_lots`, `load_features`, `load_addresses`) and
#: `compute_map_cell_aggregates` are deliberately absent.
CONVERTED = (
    postgis.load_streets,
    postgis.compute_intersections,
    postgis.compute_lot_features,
    postgis._measure_fallback_frontage,
    postgis.compute_lot_frontage,
    postgis._setback_lot_uids,
    postgis._setback_lots_already_published,
    postgis.compute_lot_buildable_setbacks,
    postgis.compute_lot_zone_pieces,
    postgis.compute_lot_profiles,
    postgis.compute_lot_addresses,
    postgis.fetch_lots,
    postgis.fetch_building_lots,
    postgis.fetch_lot_features,
    postgis.fetch_lot_frontage,
    postgis.fetch_lot_zone_pieces,
    postgis.fetch_lot_addresses,
    postgis.fetch_lot_buildable_setbacks,
    postgis.fetch_lot_polygons,
    postgis.fetch_zone_piece_polygons,
    postgis.fetch_lot_profiles,
)

#: The SQL those functions keep as module constants, checked the same way.
CONSTANTS = {
    "_FALLBACK_FRONTAGE_SELECT": postgis._FALLBACK_FRONTAGE_SELECT,
    "_SETBACK_EDGES_SQL": postgis._SETBACK_EDGES_SQL,
    "_ZONE_PIECE_SELECT": postgis._ZONE_PIECE_SELECT,
    "_LOT_ADDRESS_SELECT": postgis._LOT_ADDRESS_SELECT,
}

#: A join of one table's borough to another's - the five the borough axis
#: had, and the shape none may take now.
BOROUGH_JOIN = re.compile(r"\w+\.neighborhood\s*=\s*\w+\.neighborhood")


def _assert_no_borough_binding(name: str, text: str) -> None:
    assert "neighborhood = %" not in text, f"{name} binds a borough predicate"
    assert "%(neighborhood)s" not in text, f"{name} binds a borough parameter"
    assert "neighborhood=neighborhood" not in text, f"{name} publishes by borough"
    joined = BOROUGH_JOIN.search(text)
    assert joined is None, f"{name} joins on borough: {joined.group(0)}"


@pytest.mark.parametrize("function", CONVERTED, ids=lambda f: f.__name__)
def test_no_converted_function_binds_a_borough(function):
    _assert_no_borough_binding(function.__name__, inspect.getsource(function))


@pytest.mark.parametrize("name", sorted(CONSTANTS), ids=str)
def test_no_converted_statement_binds_a_borough(name):
    _assert_no_borough_binding(name, CONSTANTS[name])


@pytest.mark.parametrize("function", CONVERTED, ids=lambda f: f.__name__)
def test_every_converted_function_takes_the_tile_by_name(function):
    """`tile=` and not `neighborhood=`, keyword-only, so a caller that still
    passes the borough fails at the call rather than writing the wrong cell."""
    parameters = inspect.signature(function).parameters
    assert "tile" in parameters, function.__name__
    if not function.__name__.startswith("_"):
        # The private helpers are called positionally from one place each.
        assert parameters["tile"].kind is inspect.Parameter.KEYWORD_ONLY
    assert "neighborhood" not in parameters, function.__name__


# -- the row carries the lot's own three -----------------------------------

#: Every column tuple a tile-axis table is written from or read into. The
#: three travel on all of them: `neighborhood` because the map still reads
#: by it, `cell_partition` because the table is partitioned on it, and
#: `cell_key` because it is the row's permanent address.
COLUMN_TUPLES = {
    "_BUILDING_LOT_COLUMNS": postgis._BUILDING_LOT_COLUMNS,
    "_LOT_FEATURE_COLUMNS": postgis._LOT_FEATURE_COLUMNS,
    "_LOT_FRONTAGE_COLUMNS": postgis._LOT_FRONTAGE_COLUMNS,
    "_LOT_FRONTAGE_WRITE_COLUMNS": postgis._LOT_FRONTAGE_WRITE_COLUMNS,
    "_LOT_BUILDABLE_COLUMNS": postgis._LOT_BUILDABLE_COLUMNS,
    "_LOT_PROFILE_COLUMNS": postgis._LOT_PROFILE_COLUMNS,
    "_LOT_ZONE_PIECE_COLUMNS": postgis._LOT_ZONE_PIECE_COLUMNS,
    "_ZONE_PIECE_COLUMNS": postgis._ZONE_PIECE_COLUMNS,
    "_ZONE_PIECE_GEOMETRY_COLUMNS": postgis._ZONE_PIECE_GEOMETRY_COLUMNS,
    "_LOT_ADDRESS_COLUMNS": postgis._LOT_ADDRESS_COLUMNS,
    "_LOT_ADDRESS_FETCH_COLUMNS": postgis._LOT_ADDRESS_FETCH_COLUMNS,
    "_LOT_GEOMETRY_COLUMNS": postgis._LOT_GEOMETRY_COLUMNS,
    "_LOT_COLUMNS": postgis._LOT_COLUMNS,
}


@pytest.mark.parametrize("name", sorted(COLUMN_TUPLES), ids=str)
def test_every_tile_column_tuple_carries_the_lots_own_three(name):
    labels = {postgis._selected_as(column) for column in COLUMN_TUPLES[name]}
    for column in ("neighborhood", "cell_key", "cell_partition"):
        assert column in labels, f"{name} lacks {column}"


@pytest.mark.parametrize(
    ("dataset", "columns"),
    [
        ("lot_frontage", postgis._LOT_FRONTAGE_WRITE_COLUMNS),
        ("lot_zone_pieces", postgis._ZONE_PIECE_COLUMNS),
        ("lot_addresses", postgis._LOT_ADDRESS_COLUMNS),
    ],
    ids=str,
)
def test_a_write_list_supplies_what_its_table_conflicts_on(dataset, columns):
    """The same check `warehouse._require_key_columns` makes at write time,
    made here so the constants cannot drift from the tables' keys - which
    now lead with `cell_partition`."""
    table = warehouse.table_for(dataset)
    assert table.axis is warehouse.Axis.TILE
    missing = [key for key in table.conflict_columns if key not in columns]
    assert not missing, f"{dataset} write list lacks {missing}"


# -- the readers -------------------------------------------------------------


class RecordingCursor:
    """Records statements and answers `fetchall` with whatever it was given."""

    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements: list[tuple[str, object]] = []

    def execute(self, statement: str, params=None):
        self.statements.append((statement, params))
        return self

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


READERS = (
    postgis.fetch_lots,
    postgis.fetch_building_lots,
    postgis.fetch_lot_features,
    postgis.fetch_lot_frontage,
    postgis.fetch_lot_zone_pieces,
    postgis.fetch_lot_addresses,
    postgis.fetch_lot_buildable_setbacks,
    postgis.fetch_lot_polygons,
    postgis.fetch_zone_piece_polygons,
    postgis.fetch_lot_profiles,
)


@pytest.mark.parametrize("reader", READERS, ids=lambda f: f.__name__)
def test_a_reader_reads_one_cell_of_one_date(reader):
    cursor = RecordingCursor()

    frame = reader(FakeConnection(cursor), tile=TILE, scrape_date=DATE)

    assert len(cursor.statements) == 1
    statement, params = cursor.statements[0]
    text = " ".join(statement.split())
    assert "WHERE cell_partition = %s AND scrape_date = %s::date" in text
    assert "neighborhood = %s" not in text
    assert params == [TILE, DATE]
    assert len(frame) == 0


def test_fetch_lots_is_the_working_set_a_tile_asset_starts_from():
    """`rag.lots` by cell, with the jsonb as the dict psycopg hands back.

    The bronze cadastre is one borough's file and a cell is not a borough, so
    this read is where a tile-axis asset gets its lots - and it has to carry
    the three columns every table downstream is stamped with.
    """
    parcel = box(-73.62, 45.54, -73.61, 45.55)
    cursor = RecordingCursor(
        rows=[
            (
                7,
                "2 170 935",
                "VSMPE",
                "0302303330102133210",
                TILE,
                123.4,
                {"CO_STATT_LOT": "AC"},
                DATE,
                shapely.wkb.dumps(parcel),
            )
        ]
    )

    frame = postgis.fetch_lots(FakeConnection(cursor), tile=TILE, scrape_date=DATE)

    statement, params = cursor.statements[0]
    text = " ".join(statement.split())
    assert text.startswith(
        "SELECT lot_uid, lot_number, neighborhood, cell_key, cell_partition, "
        "area_m2, attributes, scrape_date::text, ST_AsBinary(geom) FROM rag.lots"
    )
    assert "ORDER BY lot_number" in text
    assert params == [TILE, DATE]

    assert list(frame.columns) == [
        "lot_uid",
        "lot_number",
        "neighborhood",
        "cell_key",
        "cell_partition",
        "area_m2",
        "attributes",
        "scrape_date",
        "geometry",
    ]
    row = frame.iloc[0]
    assert row["lot_uid"] == 7
    assert row["lot_number"] == "2 170 935"
    assert row["neighborhood"] == "VSMPE"
    assert row["cell_partition"] == TILE
    assert row["cell_key"].startswith(TILE)
    assert row["attributes"] == {"CO_STATT_LOT": "AC"}
    # Re-selected as text, the way every file in the tree spells it.
    assert row["scrape_date"] == DATE
    assert frame.geometry.iloc[0].equals(parcel)
    assert frame.crs is not None and frame.crs.to_epsg() == 4326
