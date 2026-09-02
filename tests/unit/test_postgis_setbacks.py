"""Offline test for `postgis.compute_lot_buildable_setbacks`, on a fake cursor.

The measure itself is PostGIS in substance - a boundary sorted by the angle it
runs at, four buffers, one difference - and there is no PostGIS here. What can
be checked without one, and is worth checking, is everything around it: the
guard on hbu_infra's relations, the tolerance validation, the shape of the dict
the asset reads back, and that every statement is one psycopg will accept.

That last one is not hypothetical, and this module is more exposed to it than
most: the statement below talks about *Taux d'implantation au sol* in per cent,
and psycopg reads a literal `%` - **including one inside a SQL comment** - as
the start of a placeholder and fails the whole query with `incomplete
placeholder: '%'`. `test_every_statement_is_one_psycopg_will_accept` runs each
statement through psycopg's own parser, which short of a database is the only
thing that reliably catches it. It is also why the mode test in that SQL is
written with `strpos` rather than `LIKE 'per-cent-contigu-per-cent'`.

The geometry is covered by `tests/integration/test_lot_setbacks.py`, which
needs a real PostGIS and a real Montreal parcel - the same split
`test_frontage.py` and `tests/integration/test_lot_frontage.py` make, and for
the reason that module's docstring gives: a measure that matched nothing would
pass every test here.
"""

from __future__ import annotations

import pytest
from psycopg._queries import _query2pg_nocache

from urban_rag.postgis import (
    DEFAULT_SETBACK_EDGE_TOLERANCE_M,
    SETBACK_MAX_SIN,
    SETBACK_SEGMENT_M,
    SIDE_SETBACK_FACTORS,
    MissingRelation,
    compute_lot_buildable_setbacks,
)

NEIGHBORHOOD = "VSMPE"
DATE = "2026-08-20"


class FakeCursor:
    """Answers the statements `compute_lot_buildable_setbacks` issues.

    Dispatches on the text rather than on a call counter, so reordering the
    statements does not silently hand one of them another's result - the same
    posture `test_postgis_lot_profiles.FakeCursor` takes.
    """

    def __init__(
        self,
        *,
        missing: tuple[str, ...] = (),
        num_lots: int = 10,
        lots_sorted: int = 8,
        num_envelopes: int = 14,
        lots_with_envelopes: int = 9,
        measured: tuple[object, ...] = (8, 11, 3, 1, 4_800.0, 41.5),
        by_rule: tuple[tuple[str, int], ...] = (("contigu", 9), ("isole", 5)),
        lot_uids: tuple[str, ...] | None = None,
        published: tuple[str, ...] = (),
    ):
        self.missing = missing
        self.num_lots = num_lots
        self.lots_sorted = lots_sorted
        self.num_envelopes = num_envelopes
        self.lots_with_envelopes = lots_with_envelopes
        #: lots measured, rows bound by margins, rows bound by coverage, rows
        #: with nowhere to build, total buildable area, mean share of a lot.
        self.measured = measured
        self.by_rule = by_rule
        #: The partition's lots, which the compute slices into batches, and
        #: the ones a previous run already committed - what `resume` skips.
        self.lot_uids = (
            list(lot_uids)
            if lot_uids is not None
            else [f"L{n:04d}" for n in range(num_lots)]
        )
        self.published = list(published)
        self.statements: list[tuple[str, object]] = []
        self.rowcount = 0
        self._result: object = None

    def execute(self, statement: str, params=None):
        self.statements.append((statement, params))
        text = " ".join(statement.split())

        if "to_regclass" in text:
            (name,) = params
            self._result = (None if name in self.missing else name,)
        elif "warehouse.ensure_partition" in text:
            self._result = ("silver.lot_buildable_setbacks_vsmpe_202608",)
        elif text.startswith("DROP TABLE") or text.startswith("CREATE INDEX"):
            pass
        elif text.startswith("ANALYZE"):
            pass
        elif text.startswith("CREATE TEMP TABLE _setback_edges"):
            pass
        elif text.startswith("CREATE TEMP TABLE"):
            pass
        elif text == "SELECT count(*) FROM _setback_edges":
            self._result = (self.lots_sorted,)
        elif "INSERT INTO silver_lot_buildable_setbacks_load" in text:
            self.rowcount = 14
        elif "INSERT INTO silver.lot_buildable_setbacks" in text:
            self.rowcount = 14
        elif text.startswith("DELETE FROM silver.lot_buildable_setbacks"):
            self.rowcount = 2
        elif text.startswith("SELECT lot_uid FROM rag.lots"):
            self._result = [(uid,) for uid in self.lot_uids]
        elif "SELECT DISTINCT lot_uid FROM silver.lot_buildable_setbacks" in text:
            self._result = [(uid,) for uid in self.published]
        elif "GROUP BY side_setback_rule" in text:
            self._result = list(self.by_rule)
        elif "FROM silver.lot_buildable_setbacks" in text and "FILTER" in text:
            self._result = tuple(self.measured)
        elif "FROM silver.lot_zoning_envelopes" in text:
            self._result = (self.num_envelopes, self.lots_with_envelopes)
        elif "FROM rag.lots" in text:
            self._result = (self.num_lots,)
        elif "FROM pg_attribute" in text:
            self._result = []
        else:  # pragma: no cover - a statement this stub does not know about
            raise AssertionError(f"unexpected statement: {text[:90]}")
        return self

    def fetchone(self):
        return self._result

    def fetchall(self):
        return self._result


class FakeConnection:
    """Counts commits: batching is only worth anything if they happen."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


def compute(cursor, **kwargs):
    return run(cursor, **kwargs)[0]


def run(cursor, **kwargs):
    """The result and the connection it was computed on, for the commit count."""
    connection = FakeConnection(cursor)
    result = compute_lot_buildable_setbacks(
        connection,
        neighborhood=NEIGHBORHOOD,
        scrape_date=DATE,
        **kwargs,
    )
    return result, connection


# -- what the driver will accept -------------------------------------------


def test_every_statement_is_one_psycopg_will_accept():
    """A stray `%` - in the SQL or in a comment - fails the whole query.

    This statement is about *Taux d'implantation au sol*, which is a
    percentage, so the temptation to write the sign is real and the cost of
    writing it is the whole partition. Run through psycopg's own parser rather
    than a regex of our own, so what the test accepts is what the driver does.
    """
    cursor = FakeCursor()

    compute(cursor)

    assert cursor.statements, "nothing was executed"
    for statement, params in cursor.statements:
        if params is None:
            continue
        _query2pg_nocache(statement.encode("utf-8"), "utf-8")


def test_the_mode_test_avoids_like_so_it_needs_no_per_cent_sign():
    """The reason the mode is matched with `strpos` and not with `LIKE`.

    `LIKE '%contigu%'` is the obvious way to write it and is unusable here:
    each wildcard is a per-cent sign, and psycopg reads the first as the start
    of a placeholder. Asserted rather than left as a comment because the next
    person to touch that CASE will reach for LIKE.
    """
    cursor = FakeCursor()

    compute(cursor)

    insert = next(
        statement
        for statement, _ in cursor.statements
        if "side_setback_rule" in statement and "strpos" in statement
    )
    assert "%" not in insert.replace("%(", "\x00").replace("%s", "\x00"), (
        "a literal per-cent sign reached the statement"
    )
    assert "contigu" in insert and "jumel" in insert and "isol" in insert


# -- the guard on hbu_infra ------------------------------------------------


def test_a_missing_relation_names_the_file_to_apply():
    cursor = FakeCursor(missing=("silver.lot_zoning_envelopes",))

    with pytest.raises(MissingRelation) as caught:
        compute(cursor)

    message = str(caught.value)
    assert "silver.lot_zoning_envelopes" in message
    assert "sql/012_silver_zoning.sql" in message
    assert not any(
        "INSERT" in statement or "DELETE" in statement
        for statement, _ in cursor.statements
    ), "the partition was rewritten against relations that are not there"


def test_every_missing_relation_is_reported_at_once():
    cursor = FakeCursor(missing=("silver.lot_frontage", "rag.lots"))

    with pytest.raises(MissingRelation) as caught:
        compute(cursor)

    message = str(caught.value)
    assert "silver.lot_frontage" in message
    assert "rag.lots" in message


def test_the_target_table_is_left_for_the_warehouse_to_check():
    """It is checked once, by `warehouse.upsert_select`, and not here.

    Listing it in `_BUILDABLE_RELATIONS` too would report the same absence from
    two places with two different messages, and the warehouse's is the one that
    knows about partitions. So a database missing only the target still fails
    naming sql/015 - just not from the pre-flight guard, which by then has
    already passed.
    """
    cursor = FakeCursor(missing=("silver.lot_buildable_setbacks",))

    with pytest.raises(MissingRelation) as caught:
        compute(cursor)

    assert "sql/015_silver_lot_buildable_setbacks.sql" in str(caught.value)
    # The pre-flight guard ran and did not ask about the target: every
    # to_regclass before the warehouse's own is one of the three inputs.
    checked = [
        params[0]
        for statement, params in cursor.statements
        if "to_regclass" in statement
    ]
    assert checked[:3] == [
        "rag.lots",
        "silver.lot_frontage",
        "silver.lot_zoning_envelopes",
    ]
    assert checked.count("silver.lot_buildable_setbacks") == 1


# -- the tolerance ---------------------------------------------------------


def test_a_zero_tolerance_is_refused_rather_than_silently_matching_nothing():
    """At 0 the street edge subtracts nothing and every lot's front boundary
    comes back as a side, which is a wrong answer rather than a failed one."""
    cursor = FakeCursor()

    with pytest.raises(ValueError) as caught:
        compute(cursor, edge_tolerance_m=0.0)

    assert "edge_tolerance_m" in str(caught.value)
    assert not cursor.statements, "the database was touched before validating"


def test_a_negative_tolerance_is_refused():
    with pytest.raises(ValueError):
        compute(FakeCursor(), edge_tolerance_m=-1.0)


def test_the_tolerance_reaches_the_statement_and_the_row():
    """It decides which boundary counted as street, so it travels on the row -
    the same rule `lot_frontage.buffer_m` follows."""
    cursor = FakeCursor()

    result = compute(cursor, edge_tolerance_m=0.25)

    assert result["edge_tolerance_m"] == 0.25
    sort = next(
        params
        for statement, params in cursor.statements
        if statement.strip().startswith("CREATE TEMP TABLE _setback_edges")
    )
    assert sort["tolerance_m"] == 0.25
    # And the sort runs at the shared frontage constants rather than at ones of
    # its own, which is what keeps the rear/side test the same test.
    assert sort["max_sin"] == SETBACK_MAX_SIN
    assert sort["step_m"] == SETBACK_SEGMENT_M


def test_the_default_is_the_shipped_constant():
    cursor = FakeCursor()

    result = compute(cursor)

    assert result["edge_tolerance_m"] == DEFAULT_SETBACK_EDGE_TOLERANCE_M


# -- the boundary sort -----------------------------------------------------


def test_the_sort_is_a_temp_table_and_is_indexed_and_analyzed_before_the_join():
    """It is joined once per (lot, zone, column), so it is per-lot work that
    has to sit on the lot side of the fan-out - the same reason
    `compute_lot_frontage` stages `_frontage_sides`."""
    cursor = FakeCursor()

    compute(cursor)

    order = [
        " ".join(statement.split())[:60] for statement, _ in cursor.statements
    ]
    created = next(
        index for index, text in enumerate(order)
        if text.startswith("CREATE TEMP TABLE _setback_edges")
    )
    indexed = next(
        index for index, text in enumerate(order)
        if text.startswith("CREATE INDEX ON _setback_edges")
    )
    analyzed = next(
        index for index, text in enumerate(order)
        if text.startswith("ANALYZE _setback_edges")
    )
    joined = next(
        index for index, text in enumerate(order) if "INSERT INTO" in text
    )
    assert created < indexed < analyzed < joined


def test_the_sort_is_dropped_first_so_a_second_call_on_one_connection_works():
    cursor = FakeCursor()

    compute(cursor)

    statements = [" ".join(s.split()) for s, _ in cursor.statements]
    dropped = statements.index("DROP TABLE IF EXISTS _setback_edges")
    created = next(
        index
        for index, text in enumerate(statements)
        if text.startswith("CREATE TEMP TABLE _setback_edges")
    )
    assert dropped < created


# -- what the asset reads back ---------------------------------------------


def test_the_result_carries_every_key_the_asset_reads():
    cursor = FakeCursor()

    result = compute(cursor)

    assert result["rows"] == 14
    assert result["pruned"] == 2
    assert result["num_lots"] == 10
    assert result["num_lots_sorted"] == 8
    assert result["num_lots_measured"] == 8
    assert result["num_envelopes"] == 14
    assert result["num_lots_with_envelopes"] == 9
    assert result["num_bound_by_setbacks"] == 11
    assert result["num_bound_by_site_coverage"] == 3
    assert result["num_unbuildable"] == 1
    assert result["total_buildable_area_m2"] == 4_800.0
    assert result["mean_buildable_pct_of_lot"] == 41.5
    assert result["by_side_setback_rule"] == {"contigu": 9, "isole": 5}
    assert result["max_sin"] == SETBACK_MAX_SIN
    assert result["segment_m"] == SETBACK_SEGMENT_M


def test_the_lots_that_could_not_be_sorted_are_counted_not_hidden():
    """A lot with no frontage row has no front edge to measure angles against,
    so it gets no row - and the asset warns on the share."""
    cursor = FakeCursor(num_lots=100, lots_sorted=61)

    result = compute(cursor)

    assert result["lots_without_frontage"] == 39


def test_a_partition_whose_envelopes_never_landed_reports_zero_rather_than_raising():
    """The distinction the asset needs to tell two gaps apart: no envelopes is
    a different failure from no lots, and both are the caller's to name."""
    cursor = FakeCursor(num_envelopes=0, lots_with_envelopes=0)

    result = compute(cursor)

    assert result["num_envelopes"] == 0
    assert result["num_lots"] == 10


# -- the side rule ---------------------------------------------------------


def test_the_three_readings_of_the_mode_are_the_ones_the_sql_produces():
    """`SIDE_SETBACK_FACTORS` is documentation of the CASE in the statement,
    so the keys have to be the strings that CASE emits."""
    cursor = FakeCursor()

    compute(cursor)

    insert = next(
        statement
        for statement, _ in cursor.statements
        if "side_setback_rule" in statement and "strpos" in statement
    )
    for rule in SIDE_SETBACK_FACTORS:
        assert f"'{rule}'" in insert


def test_contiguous_building_takes_no_side_setback():
    """The whole reason the mode is read at all: subtracting the printed
    Laterale from both sides of every plex lot understates most of VSMPE."""
    assert SIDE_SETBACK_FACTORS["contigu"] == 0.0


def test_semi_detached_takes_half_from_each_side_which_is_one_whole_margin():
    """Half off both sides removes exactly what a whole margin off one side
    does, for any parcel whose side lines are parallel - and which side carries
    the party wall is a fact about the neighbour nothing here publishes."""
    assert SIDE_SETBACK_FACTORS["jumele"] == 0.5


def test_an_unstated_mode_takes_the_full_margin_on_both_sides():
    """The conservative reading. A column stating no mode must not quietly
    hand a lot more buildable area than its grid allows."""
    assert SIDE_SETBACK_FACTORS["unknown"] == SIDE_SETBACK_FACTORS["isole"] == 1.0


# -- the batching ----------------------------------------------------------


def test_the_borough_is_cut_into_batches_that_each_commit():
    """The whole point: a dropped connection costs a batch, not the run.

    Ten lots at four a batch is three slices, and each has to reach the
    database on its own - plus one commit for the prune that follows them.
    A batch that only commits at the end is the behaviour this replaced.
    """
    cursor = FakeCursor(num_lots=10)

    result, connection = run(cursor, batch_lots=4)

    assert result["num_batches"] == 3
    assert result["batch_lots"] == 4
    assert connection.commits == 4


def test_each_batch_carves_only_its_own_lots():
    """The slice reaches the SQL, or every batch would redo the borough."""
    cursor = FakeCursor(num_lots=10)

    compute(cursor, batch_lots=4)

    # The edges build alone: each batch also carries the slice into its
    # upsert, and counting both would double every batch.
    slices = [
        params["lot_uids"]
        for text, params in cursor.statements
        if " ".join(text.split()).startswith("CREATE TEMP TABLE _setback_edges")
    ]
    assert [len(s) for s in slices] == [4, 4, 2]
    # Every lot exactly once, and in the order `_setback_lot_uids` fixed.
    assert [uid for s in slices for uid in s] == cursor.lot_uids


def test_a_resumed_run_skips_what_a_previous_one_committed():
    """What makes the retry cheap rather than merely possible."""
    cursor = FakeCursor(num_lots=10, published=[f"L{n:04d}" for n in range(6)])

    result = compute(cursor, batch_lots=4)

    assert result["num_lots_resumed"] == 6
    # Four left, so one batch rather than three.
    assert result["num_batches"] == 1


def test_resume_is_keyed_on_the_tolerance_so_a_new_one_redoes_the_borough():
    """Two settings mixed in one table is the failure resume must not cause.

    The published rows are read back at the tolerance being run, so a run at a
    different one finds none of them and recomputes - which is why
    `edge_tolerance_m` travels on every row.
    """
    cursor = FakeCursor(num_lots=10, published=[f"L{n:04d}" for n in range(10)])

    compute(cursor, batch_lots=4, edge_tolerance_m=0.2)

    lookup = [
        params
        for text, params in cursor.statements
        if "SELECT DISTINCT lot_uid FROM silver.lot_buildable_setbacks"
        in " ".join(text.split())
    ]
    assert lookup and lookup[0][-1] == 0.2


def test_resume_off_recomputes_everything_already_published():
    cursor = FakeCursor(num_lots=10, published=[f"L{n:04d}" for n in range(10)])

    result = compute(cursor, batch_lots=4, resume=False)

    assert result["num_lots_resumed"] == 0
    assert result["num_batches"] == 3


def test_a_batch_clears_its_own_lots_before_rewriting_them():
    """The upsert conflicts on (lot, feature, column) and cannot drop a column
    the grid stopped stating; the single statement pruned those at the end and
    a per-slice prune cannot see them. So each slice replaces its own lots."""
    cursor = FakeCursor(num_lots=4)

    compute(cursor, batch_lots=4)

    deletes = [
        params
        for text, params in cursor.statements
        if " ".join(text.split()).startswith("DELETE FROM silver.lot_buildable_setbacks")
    ]
    # One per batch, scoped to that batch's lots, plus the final prune.
    assert deletes[0][-1] == cursor.lot_uids


def test_the_partition_is_still_pruned_against_the_whole_expected_set():
    """A lot that has left the cadastre loses its rows, and every committed
    batch keeps them - which a staging-table prune could not do here."""
    cursor = FakeCursor(num_lots=4)

    result = compute(cursor, batch_lots=2)

    prunes = [
        (text, params)
        for text, params in cursor.statements
        if "NOT (lot_uid = ANY" in " ".join(text.split())
    ]
    assert len(prunes) == 1
    assert prunes[0][1][-1] == cursor.lot_uids
    assert result["pruned"] == 2


def test_a_partition_with_no_envelopes_writes_nothing_at_all():
    """The guard has to run *before* the first batch commits.

    A batch deletes its own lots and then rewrites them; on a partition whose
    envelopes never landed there is nothing to rewrite, so that sequence would
    delete a good answer and commit nothing in its place.
    """
    cursor = FakeCursor(num_lots=10, num_envelopes=0)

    result, connection = run(cursor, batch_lots=4)

    assert result["num_envelopes"] == 0
    assert result["num_batches"] == 0
    assert connection.commits == 0
    assert not [
        text
        for text, _ in cursor.statements
        if " ".join(text.split()).startswith("DELETE FROM silver.lot_buildable_setbacks")
    ]


def test_batch_lots_zero_does_the_whole_partition_in_one_transaction():
    """The old behaviour, kept reachable for a connection that cannot drop."""
    cursor = FakeCursor(num_lots=10)

    result = compute(cursor, batch_lots=0)

    assert result["num_batches"] == 1
