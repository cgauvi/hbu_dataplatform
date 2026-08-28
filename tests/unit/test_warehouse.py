"""Offline tests for `urban_rag.warehouse`, the one write path into
`silver.*` and `gold.*`.

Everything this module does is Postgres in substance - COPY, `INSERT ... ON
CONFLICT`, a DELETE against a staging table - so nothing here touches a
database. What can be checked without one, and is worth checking, is the SQL
it builds and the values it hands to the driver: which columns a frame is
matched to, what the geometry and the jsonb catch-all become on the wire, that
the partition key is written from the partition and not from the frame, and
that the prune is scoped to the partition rather than to the table.

That last one is the reason for `test_the_prune_is_scoped_to_the_partition`.
A `DELETE ... WHERE NOT EXISTS` with the partition predicate dropped would
still pass every other test here and would empty the table on every load.

`test_every_statement_is_one_psycopg_will_accept` is the same guard
`test_postgis_lot_profiles` carries and for the same reason: psycopg reads a
literal `%` - in the SQL or in a comment - as the start of a placeholder, and
the failure comes at execution time with a message that names nothing.
"""

from __future__ import annotations

import json

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from psycopg._queries import _query2pg_nocache
from shapely.geometry import box

from urban_rag import warehouse
from urban_rag.layers import Layer, assets_in
from urban_rag.warehouse import (
    MissingRelation,
    conflict_clause,
    table_for,
    upsert_frame,
    upsert_select,
)

NEIGHBORHOOD = "VSMPE"
DATE = "2026-08-26"

#: `silver.neighborhood_streets` as `_target_columns` reads it back, in catalog
#: order - see hbu_infra's sql/007_silver_streets.sql. The one table with both
#: a column map and a jsonb catch-all, so it exercises every branch of the
#: matcher; and `loaded_at` is the column no frame ever fills, which is the
#: other branch.
STREET_COLUMNS = [
    "scrape_date",
    "neighborhood",
    "cote_rue_id",
    "street_name",
    "length_m",
    "attributes",
    "geom",
    "loaded_at",
]


class FakeCopy:
    """psycopg's COPY context, reduced to the rows written through it."""

    def __init__(self, rows: list[list[object]]):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def write_row(self, values):
        self.rows.append(list(values))


class FakeCursor:
    """Answers the statements `warehouse` issues, dispatching on their text."""

    def __init__(
        self,
        *,
        columns: list[str] | None = None,
        missing: tuple[str, ...] = (),
        upserted: int = 0,
        pruned: int = 0,
    ):
        self.columns = columns if columns is not None else STREET_COLUMNS
        self.missing = missing
        self._upserted = upserted
        self._pruned = pruned
        self.statements: list[tuple[str, object]] = []
        self.copied: list[list[object]] = []
        self.copy_statements: list[str] = []
        self.rowcount = 0
        self._result: object = None

    def execute(self, statement: str, params=None):
        self.statements.append((statement, params))
        text = " ".join(statement.split())

        if "to_regclass" in text:
            (name,) = params
            self._result = (None if name in self.missing else name,)
        elif "warehouse.ensure_partition" in text:
            self._result = ("silver.neighborhood_streets__vsmpe__202608",)
        elif "FROM pg_attribute" in text:
            self._result = [(name,) for name in self.columns]
        elif text.startswith("DROP TABLE") or text.startswith("CREATE TEMP TABLE"):
            pass
        elif text.startswith("INSERT INTO") and "_load (" in text.split(" SELECT")[0]:
            self.rowcount = self._upserted  # upsert_select's staging INSERT
        elif text.startswith("INSERT INTO"):
            self.rowcount = self._upserted
        elif text.startswith("DELETE FROM"):
            self.rowcount = self._pruned
        else:  # pragma: no cover - a statement this stub does not know about
            raise AssertionError(f"unexpected statement: {text[:80]}")
        return self

    def copy(self, statement: str):
        self.copy_statements.append(statement)
        return FakeCopy(self.copied)

    def fetchone(self):
        return self._result

    def fetchall(self):
        return self._result

    # -- reading the statements back -------------------------------------

    def issued(self) -> list[str]:
        return [" ".join(statement.split()) for statement, _ in self.statements]

    def one(self, fragment: str) -> str:
        matches = [text for text in self.issued() if fragment in text]
        assert matches, f"no statement contains {fragment!r}"
        assert len(matches) == 1, f"{len(matches)} statements contain {fragment!r}"
        return matches[0]


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def streets(**overrides) -> gpd.GeoDataFrame:
    """One borough's street sides, as `neighborhood_streets` writes them.

    Shouting column names, a couple of columns no target column matches, and
    the partition columns already filled in - all three of which the write path
    has to handle without being told.
    """
    frame = {
        "COTE_RUE_ID": ["11", "22"],
        "NOM_VOIE": ["Jarry", None],
        "TYPE_F": ["rue", "ruelle"],
        "pct_in_borough": [100.0, 62.5],
        "length_m": [120.0, 80.0],
        "neighborhood": [NEIGHBORHOOD, NEIGHBORHOOD],
        "scrape_date": [DATE, DATE],
    }
    frame.update(overrides)
    return gpd.GeoDataFrame(
        frame,
        geometry=[box(0, 0, 1, 1).boundary, box(1, 0, 2, 1).boundary],
        crs="EPSG:4326",
    )


def upsert(cursor, frame=None, dataset="neighborhood_streets", **kwargs):
    return upsert_frame(
        FakeConnection(cursor),
        dataset,
        streets() if frame is None else frame,
        neighborhood=NEIGHBORHOOD,
        scrape_date=DATE,
        **kwargs,
    )


# -- the registry ----------------------------------------------------------


def test_every_table_is_in_the_schema_its_asset_layer_names():
    """The schema is derived from `urban_rag.layers`, never written twice.

    An asset moved between layers moves its table with it; the alternative is
    a table in `silver` for an asset the tree files under `gold/`.
    """
    for table in warehouse.TABLES.values():
        assert table.schema in {"silver", "gold"}
        assert table.qualified == f"{table.schema}.{table.name}"


def test_the_registry_covers_every_silver_and_gold_asset_but_the_three_named():
    """The three documented absences, and why each one is not a gap.

    `document_embeddings` and `document_index` publish to rag.chunks;
    `assessment_units` has no borough axis to partition a table by. All three
    are argued for in the comment above `TABLES`, so this test is what keeps a
    *fourth* absence from arriving quietly when an asset is added.
    """
    published = {table.asset for table in warehouse.TABLES.values()}
    declared = set(assets_in(Layer.SILVER)) | set(assets_in(Layer.GOLD))
    assert declared - published == {
        "assessment_units",
        "document_embeddings",
        "document_index",
    }


def test_the_conflict_target_is_the_partition_then_the_natural_key():
    table = table_for("neighborhood_streets")
    assert table.conflict_columns == ("scrape_date", "neighborhood", "cote_rue_id")


def test_a_dataset_with_no_table_names_the_registry_to_add_it_to():
    with pytest.raises(KeyError) as caught:
        table_for("nothing_publishes_this")
    assert "urban_rag.warehouse.TABLES" in str(caught.value)


# -- the SQL ---------------------------------------------------------------


def test_the_conflict_clause_updates_everything_but_the_key():
    table = table_for("neighborhood_streets")

    clause = conflict_clause(table, ["scrape_date", "neighborhood", "cote_rue_id", "geom"])

    assert clause.startswith(
        "ON CONFLICT (scrape_date, neighborhood, cote_rue_id) DO UPDATE SET "
    )
    assert "geom = EXCLUDED.geom" in clause
    # The key is what was matched on; overwriting it with itself is noise.
    assert "cote_rue_id = EXCLUDED" not in clause
    # Set to now() rather than to the staging row's default, which on a re-run
    # would still read as the first insert's timestamp.
    assert clause.endswith("loaded_at = now()")


def test_a_missing_table_names_the_file_to_apply():
    """hbu_infra owns every table here, so the message that helps is the file."""
    cursor = FakeCursor(missing=("silver.neighborhood_streets",))

    with pytest.raises(MissingRelation) as caught:
        upsert(cursor)

    message = str(caught.value)
    assert "silver.neighborhood_streets" in message
    assert "sql/007_silver_streets.sql" in message
    assert "db.py init" in message


def test_a_key_column_the_frame_cannot_fill_is_named_up_front():
    """Otherwise the failure is a not-null violation three statements later.

    The usual cause is banal — a source that renamed a column, or an entry
    missing from the table's own column map — and neither is visible in
    `null value in column "cote_rue_id" violates not-null constraint`.
    """
    cursor = FakeCursor()

    with pytest.raises(ValueError) as caught:
        upsert(cursor, streets().drop(columns=["COTE_RUE_ID"]))

    message = str(caught.value)
    assert "silver.neighborhood_streets conflicts on" in message
    assert "nothing supplies cote_rue_id" in message
    # What the frame *did* carry, so the rename is visible rather than guessed.
    assert "NOM_VOIE" in message


def test_the_partition_is_created_before_anything_is_written():
    """A borough enabled today and a new month both just work.

    `ensure_partition` runs ahead of the COPY rather than being declared
    ahead of the run, so nothing has to be told about either.
    """
    cursor = FakeCursor()

    upsert(cursor)

    issued = cursor.issued()
    ensured = next(i for i, t in enumerate(issued) if "ensure_partition" in t)
    written = next(i for i, t in enumerate(issued) if t.startswith("CREATE TEMP TABLE"))
    assert ensured < written
    _, params = cursor.statements[
        next(i for i, (s, _) in enumerate(cursor.statements) if "ensure_partition" in s)
    ]
    assert params == ["silver.neighborhood_streets", NEIGHBORHOOD, DATE]


def test_the_staging_table_is_shaped_like_the_target():
    """`LIKE` rather than a column list of our own.

    It is what lets the COPY be text-format: the staging column types are the
    target's, so Postgres parses each value with the column's own input
    function and this module owns no cast at all.
    """
    cursor = FakeCursor()

    upsert(cursor)

    assert (
        "CREATE TEMP TABLE silver_neighborhood_streets_load "
        "(LIKE silver.neighborhood_streets INCLUDING DEFAULTS) ON COMMIT DROP"
        in cursor.issued()
    )


def test_the_upsert_deduplicates_on_the_conflict_target():
    """A key repeated inside one load would fail `ON CONFLICT DO UPDATE`.

    Infolot really does answer a boundary query with the same lot twice, and
    the loaders this replaced resolved that with `DO NOTHING`. `DISTINCT ON`
    keeps the tolerance; the `ORDER BY` is what makes which copy wins the same
    on every run.
    """
    cursor = FakeCursor()

    upsert(cursor)

    insert = cursor.one("INSERT INTO silver.neighborhood_streets")
    assert "SELECT DISTINCT ON (scrape_date, neighborhood, cote_rue_id)" in insert
    assert "ORDER BY scrape_date, neighborhood, cote_rue_id" in insert
    assert "ON CONFLICT (scrape_date, neighborhood, cote_rue_id) DO UPDATE SET" in insert


def test_the_prune_is_scoped_to_the_partition():
    """The half of the write that keeps snapshot semantics.

    An upsert alone cannot: a street side that disappears upstream has no row
    to conflict with and would sit in the table forever. A prune that lost its
    partition predicate would empty the table on every load, and would pass
    every other test here.
    """
    cursor = FakeCursor(pruned=7)

    result = upsert(cursor)

    delete = cursor.one("DELETE FROM silver.neighborhood_streets")
    assert "t.neighborhood = %s AND t.scrape_date = %s::date" in delete
    assert "NOT EXISTS (SELECT 1 FROM silver_neighborhood_streets_load s" in delete
    assert "s.cote_rue_id IS NOT DISTINCT FROM t.cote_rue_id" in delete
    assert result["pruned"] == 7


def test_the_prune_can_be_turned_off():
    cursor = FakeCursor()

    result = upsert(cursor, prune=False)

    assert not [text for text in cursor.issued() if text.startswith("DELETE FROM")]
    assert result["pruned"] == 0


def test_dropped_duplicates_are_reported_rather_than_swallowed():
    """`copied` minus `upserted`, which costs no extra query."""
    cursor = FakeCursor(upserted=1)

    result = upsert(cursor)

    assert result["copied"] == 2
    assert result["upserted"] == 1
    assert result["duplicates"] == 1


def test_every_statement_is_one_psycopg_will_accept():
    """A stray `%` - in the SQL or in a comment - fails the whole query."""
    cursor = FakeCursor()

    upsert(cursor)

    assert cursor.statements, "nothing was executed"
    for statement, params in cursor.statements:
        if params is None:
            continue
        _query2pg_nocache(statement.encode("utf-8"), "utf-8")


# -- what reaches the wire -------------------------------------------------


def copied(cursor) -> list[dict[str, object]]:
    """The COPYed rows, keyed by the column each value was written under."""
    header = cursor.copy_statements[0]
    names = header[header.index("(") + 1 : header.index(")")].split(", ")
    return [dict(zip(names, row)) for row in cursor.copied]


def test_the_columns_are_matched_by_name_and_by_the_tables_own_map():
    cursor = FakeCursor()

    upsert(cursor)

    row = copied(cursor)[0]
    # Mapped: the geobase shouts, this platform does not.
    assert row["cote_rue_id"] == "11"
    assert row["street_name"] == "Jarry"
    # Matched by name, no entry needed. Written as "120" rather than "120.0":
    # an integral float narrows so that an integer column can take it, and
    # `double precision` parses either spelling to the same value.
    assert row["length_m"] == "120"
    # A target column the frame has nothing for is left out of the write
    # entirely, so the table's own default still applies - and `loaded_at` in
    # particular is set by the upsert, not carried in from a staging row.
    assert "loaded_at" not in row


def test_the_partition_is_written_from_the_key_not_from_the_frame():
    """A stale file would otherwise write into a partition nothing prunes."""
    cursor = FakeCursor()

    upsert(
        cursor,
        streets(neighborhood=["MHM", "MHM"], scrape_date=["2020-01-01"] * 2),
    )

    for row in copied(cursor):
        assert row["neighborhood"] == NEIGHBORHOOD
        assert row["scrape_date"] == DATE


def test_geometry_travels_as_hex_ewkb():
    """The `geometry` input function parses it, so nothing calls ST_GeomFromWKB.

    The SRID has to be in the value: a bare WKB hex string loses it, and a
    geometry column typed 4326 would then reject the row.
    """
    cursor = FakeCursor()

    upsert(cursor)

    value = copied(cursor)[0]["geom"]
    assert isinstance(value, str)
    # EWKB sets the high bit of the type word and appends the SRID; 4326 is
    # 0x10E6, little-endian, right after the geometry type.
    assert value.upper().startswith("0102000020E6100000")


def test_a_row_with_no_geometry_is_not_written_to_a_spatial_table():
    """It cannot be joined, clipped or drawn, and it would hole every measure."""
    frame = streets()
    frame = frame.set_geometry(gpd.GeoSeries([None, frame.geometry.iloc[1]], crs=4326))
    cursor = FakeCursor()

    result = upsert(cursor, frame)

    assert result["copied"] == 1
    assert copied(cursor)[0]["cote_rue_id"] == "22"


def test_what_no_column_claimed_lands_in_the_jsonb_catch_all():
    """A layer that gains a column upstream lands in the same table.

    The posture rag.features.attributes already takes: the source adds and
    retires columns between releases, and a schema that needs a migration each
    time the city edits a layer will not survive the pipeline.
    """
    cursor = FakeCursor()

    upsert(cursor)

    attributes = json.loads(copied(cursor)[0]["attributes"])
    assert attributes == {"TYPE_F": "rue", "pct_in_borough": 100.0}
    # Not the geometry, and not a column that found a target of its own.
    assert "geometry" not in attributes
    assert "NOM_VOIE" not in attributes


def test_a_json_string_column_is_re_parsed_rather_than_quoted():
    """`urban_rag.frames` JSON-encodes a nested cell on the way to parquet.

    Storing that string as a jsonb *string* would make a list upstream a
    quoted blob here, which nothing could query.
    """
    cursor = FakeCursor()

    upsert(cursor, streets(TYPE_F=['["a", "b"]', "{}"]))

    assert json.loads(copied(cursor)[0]["attributes"])["TYPE_F"] == ["a", "b"]


def test_every_flavour_of_missing_becomes_null():
    """`str(nan)` is "nan" - a street called "nan" rather than an absent name."""
    cursor = FakeCursor()

    upsert(cursor, streets(NOM_VOIE=[float("nan"), None]))

    rows = copied(cursor)
    assert rows[0]["street_name"] is None
    assert rows[1]["street_name"] is None
    assert json.loads(rows[0]["attributes"])["pct_in_borough"] == 100.0


def test_numpy_and_pandas_scalars_survive_the_trip():
    """Frames carry numpy types, and `repr()` of one is not always its value.

    `numpy.float64` is a Python `float` by isinstance, so it reaches the float
    branch - and under numpy 2 its repr is `np.float64(120.5)`, which a
    `double precision` column rejects outright.
    """
    cursor = FakeCursor()

    upsert(
        cursor,
        streets(
            length_m=np.array([120.5, 80.25]),
            pct_in_borough=pd.array([100, 62], dtype="Int64"),
        ),
    )

    rows = copied(cursor)
    assert rows[0]["length_m"] == "120.5"
    assert json.loads(rows[0]["attributes"])["pct_in_borough"] == 100


def test_an_integral_float_is_written_without_its_point_zero():
    """A count pandas widened to float64 still has to reach an integer column.

    `num_frontages` is `integer` in sql/012_silver_zoning.sql and comes from a
    `groupby().agg(size)`, but it is left-joined onto every lot - and a lot
    facing no street gets NaN, which promotes the whole column to float64. The
    count 1 then reaches the COPY as "1.0" and Postgres refuses it with
    `invalid input syntax for type integer`. A non-integral float is a real
    disagreement with the column and is still written as itself.
    """
    cursor = FakeCursor()

    upsert(cursor, streets(length_m=[120.0, 80.5]))

    rows = copied(cursor)
    assert rows[0]["length_m"] == "120"
    assert rows[1]["length_m"] == "80.5"


def test_a_naT_is_null_rather_than_the_word():
    """`pd.NaT` is a `datetime`, and its isoformat is the string "NaT".

    Written through, a `timestamptz` column rejects it naming neither the row
    nor the column - which is why the missing-value test runs before the date
    branch rather than after it.
    """
    cursor = FakeCursor()

    upsert(cursor, streets(NOM_VOIE=[pd.NaT, "Jarry"]))

    assert copied(cursor)[0]["street_name"] is None


def test_an_empty_frame_still_prunes_the_partition():
    """A borough whose layer went empty upstream should end up empty here.

    Nothing is copied, so nothing conflicts, and it is the prune that has to
    notice - which is the case a delete-then-insert got right for free and an
    upsert on its own gets wrong.
    """
    cursor = FakeCursor(pruned=4)

    result = upsert(cursor, streets().iloc[:0])

    assert result["copied"] == 0
    assert result["pruned"] == 4
    assert cursor.one("DELETE FROM silver.neighborhood_streets")


# -- the in-database path --------------------------------------------------


def test_a_computed_partition_is_staged_before_it_is_merged():
    """The three PostGIS joins produce rows that never pass through Python.

    They still land in a staging table first, because the prune has to know
    which keys this run produced and a bare `INSERT ... SELECT` leaves nothing
    behind to ask.
    """
    cursor = FakeCursor(columns=["scrape_date", "neighborhood", "lot_number"])

    result = upsert_select(
        cursor,
        "lot_profiles",
        ("scrape_date", "neighborhood", "lot_number"),
        "SELECT l.scrape_date, l.neighborhood, l.lot_number FROM rag.lots l",
        {"neighborhood": NEIGHBORHOOD},
        neighborhood=NEIGHBORHOOD,
        scrape_date=DATE,
    )

    issued = cursor.issued()
    staged = next(
        i for i, t in enumerate(issued) if t.startswith("INSERT INTO gold_lot_profiles_load")
    )
    merged = next(
        i for i, t in enumerate(issued) if t.startswith("INSERT INTO gold.lot_profiles")
    )
    assert staged < merged
    assert "ON CONFLICT (scrape_date, neighborhood, lot_number) DO UPDATE SET" in (
        issued[merged]
    )
    assert result["copied"] == 0
