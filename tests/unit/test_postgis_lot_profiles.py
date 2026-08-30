"""Offline test for `postgis.compute_lot_profiles`, against a fake cursor.

The statement itself is Postgres-only in substance - four grouped CTEs left
joined onto `rag.lots` - and there is no PostGIS to run it against here. What
*can* be checked without one, and is worth checking, is everything around it:
the guard on hbu_infra's relations, the threshold validation, what the three
caller-supplied inputs turn into on the way to the query, the shape of the dict
the asset reads, and that every statement is one psycopg will accept.

That last one is not hypothetical. psycopg reads `%` as the start of a
placeholder, so a single per-cent sign anywhere in the query - **including in a
SQL comment** - fails the whole thing at execution time with `incomplete
placeholder: '%'`. A comment reading "covering 100% of the lot" did exactly
that once. `test_every_statement_is_one_psycopg_will_accept` runs each
statement through psycopg's own parser, which is the only thing that reliably
catches it short of a database.
"""

from __future__ import annotations

import pytest
from psycopg._queries import _query2pg_nocache

from urban_rag.postgis import (
    DEFAULT_MAX_BUILT_AREA_M2,
    LOT_CATEGORIES,
    MissingRelation,
    compute_lot_profiles,
)

NEIGHBORHOOD = "VSMPE"
DATE = "2026-08-20"


class FakeCopy:
    """psycopg's COPY context, reduced to the rows written through it."""

    def __init__(self, rows: list[list[object]]):
        self.rows = rows
        self.types: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def set_types(self, types):
        self.types = list(types)

    def write_row(self, values):
        self.rows.append(list(values))


class FakeCursor:
    """Answers the statements `compute_lot_profiles` issues, in any order.

    Dispatches on the text rather than on a call counter, so reordering the
    statements does not silently hand one of them another's result.
    """

    def __init__(
        self,
        *,
        missing: tuple[str, ...] = (),
        num_envelopes: int = 4,
        assessed: tuple[object, ...] = (7, 19, 4_200_000.0, 2026),
        buildable: tuple[object, ...] = (9, 5, 41.5),
        comparables: tuple[object, ...] = (8, 6, 4.25, 0.82, 1_450_000.0),
    ):
        self.missing = missing
        self.num_envelopes = num_envelopes
        #: What the assessment join reports back: lots carrying a total, the
        #: units standing on them, the apportioned total and the roll year.
        self.assessed = assessed
        #: What the comparables join reports back: lots with a neighbour set,
        #: lots with a cap rate, the median rate, the median assessed-to-
        #: estimated ratio, and the borough's net operating income.
        self.comparables = comparables
        #: What the setback join reports back: lots carrying a buildable area,
        #: how many of them are stopped by their margins rather than by *Taux
        #: d'implantation*, and the mean share of a lot left buildable.
        self.buildable = buildable
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
            # The partition is created on demand before every write - see
            # hbu_infra's sql/003_warehouse.sql. Nothing to answer here.
            self._result = ("gold.lot_profiles_vsmpe_202608",)
        elif text.startswith("CREATE TEMP TABLE") or text.startswith("DROP TABLE"):
            pass
        elif "INSERT INTO gold_lot_profiles_load" in text:
            # The staging table `urban_rag.warehouse.upsert_select` lands the
            # computed rows in, before the upsert and the prune below.
            self.rowcount = 10
        elif "INSERT INTO gold.lot_profiles" in text:
            self.rowcount = 10
        elif text.startswith("DELETE FROM gold.lot_profiles"):
            self.rowcount = 3
        elif "GROUP BY category" in text:
            self._result = [
                ("built", 6, 2_400.0),
                ("no_building", 3, 15_000.0),
                ("shed_only", 1, 5_000.0),
            ]
        elif "FROM gold.lot_profiles" in text and "FILTER" in text:
            #  profiles, built, fronted, corner, documented, buildings,
            #  area, max primary, mean primary, enveloped, envelopes, then the
            #  setback join (lots with a buildable area, how many are bound by
            #  their margins, the mean share left buildable), then overall
            #  vacancy, overall rent, then the six cost rates: underground
            #  low/high and above-grade low/high per stall, condo low/high per
            #  square foot - then the assessment join: lots with a total,
            #  units on them, the apportioned total, and the roll year those
            #  values came from - and last the comparables join: lots with a
            #  neighbour set, lots with a cap rate, the median rate, the median
            #  assessed-to-estimated ratio, and the borough's net operating
            #  income.
            self._result = (
                10, 6, 8, 2, 7, 12, 22_400.0, 31.25, 12.34,
                3, self.num_envelopes, *self.buildable, 0.5, 1_275.0,
                51_925.0, 68_675.0, 38_500.0, 57_750.0, 225.0, 290.0,
                *self.assessed, *self.comparables,
            )
        elif "FROM rag.lots" in text:
            self._result = (10,)
        elif "FROM pg_attribute" in text:
            self._result = []
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


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def compute(cursor, **kwargs):
    return compute_lot_profiles(
        FakeConnection(cursor),
        neighborhood=NEIGHBORHOOD,
        scrape_date=DATE,
        **kwargs,
    )


def _statement_containing(cursor, needle: str) -> str:
    """The one executed statement mentioning ``needle``, whitespace-collapsed.

    Dispatching on the text rather than on an index for the reason `FakeCursor`
    does: reordering the statements should not silently point a test at
    another one.
    """
    found = [
        " ".join(statement.split())
        for statement, _ in cursor.statements
        if needle in statement
    ]
    assert found, f"no statement mentions {needle!r}"
    return found[0]


def test_every_statement_is_one_psycopg_will_accept():
    """A stray `%` - in the SQL or in a comment - fails the whole query.

    Run through psycopg's own parser rather than a regex of our own, so what
    the test accepts is exactly what the driver accepts.
    """
    cursor = FakeCursor()

    compute(cursor)

    assert cursor.statements, "nothing was executed"
    for statement, params in cursor.statements:
        if params is None:
            continue
        # Raises ProgrammingError on an incomplete placeholder, which is what
        # a literal per-cent sign in a comment looks like to the driver.
        _query2pg_nocache(statement.encode("utf-8"), "utf-8")


def test_the_result_carries_every_key_the_asset_reads():
    cursor = FakeCursor()

    result = compute(cursor)

    assert result["profiles"] == 10
    # These two agreeing is what says every lot got a profile.
    assert result["num_lots"] == 10
    assert result["num_profiles"] == 10
    assert result["num_with_building"] == 6
    assert result["num_without_building"] == 4
    assert result["num_with_frontage"] == 8
    assert result["num_with_secondary_frontage"] == 2
    assert result["num_with_documents"] == 7
    assert result["num_buildings"] == 12
    assert result["total_lot_area_m2"] == pytest.approx(22_400.0)
    assert result["max_primary_frontage_m"] == pytest.approx(31.25)
    assert result["mean_primary_frontage_m"] == pytest.approx(12.34)


def test_the_assessment_join_is_reported_back_to_the_asset():
    cursor = FakeCursor()

    result = compute(cursor)

    assert result["num_with_assessed_value"] == 7
    assert result["num_assessment_units"] == 19
    assert result["total_assessed_value_apportioned"] == pytest.approx(4_200_000.0)
    assert result["roll_year"] == 2026


def test_a_partition_the_roll_never_reached_reports_none_not_zero():
    """`silver.lot_assessed_values` has not run for this partition.

    Zero would read as a borough whose every lot is worth nothing, which is a
    different claim from one whose roll has not been joined yet - the same
    distinction the nullable column itself draws.
    """
    cursor = FakeCursor(assessed=(0, 0, None, None))

    result = compute(cursor)

    assert result["num_with_assessed_value"] == 0
    assert result["total_assessed_value_apportioned"] is None
    assert result["roll_year"] is None


def test_the_assessment_table_is_joined_on_the_lot_number_not_the_uid():
    """`lot_uid` is a bigserial `load_lots` mints again on every reload, and
    `silver.lot_assessed_values` does not carry one at all."""
    cursor = FakeCursor()

    compute(cursor)

    insert = _statement_containing(cursor, "silver.lot_assessed_values")
    assert "assessed.lot_number = l.lot_number" in insert
    # Scoped to the partition as well - the table holds every borough-day -
    # and by the parameters rather than by equality with `l`, so the partition
    # prunes at plan time.
    assert "assessed.neighborhood = %(neighborhood)s" in insert
    assert "assessed.scrape_date = %(scrape_date)s::date" in insert
    assert "assessed.lot_uid" not in insert


def test_a_lot_with_no_assessment_unit_keeps_a_null_total_and_a_zero_count():
    """The same split the frontage columns make, and for the same reason.

    A lane carrying no assessed property was not valued at zero - it was not
    valued. The counts are a measurement and do COALESCE.
    """
    cursor = FakeCursor()

    compute(cursor)

    insert = _statement_containing(cursor, "silver.lot_assessed_values")
    assert "COALESCE(assessed.num_assessment_units, 0)" in insert
    assert "COALESCE(assessed.num_shared_units, 0)" in insert
    assert "COALESCE(assessed.num_units_by_point, 0)" in insert
    # Left alone, so a lot the roll never reached lands NULL.
    assert "COALESCE(assessed.total_assessed_value" not in insert
    assert "assessed.total_assessed_value," in insert
    assert "assessed.total_assessed_value_apportioned," in insert


def test_the_borough_total_is_summed_from_the_apportioned_column():
    """The other total counts a multi-lot unit whole on each of its lots, so
    summing it across a borough over-reports - by $5.1B of $29.4B on the first
    VSMPE snapshot. Only one of the two adds up this way."""
    cursor = FakeCursor()

    compute(cursor)

    metrics = _statement_containing(cursor, "FILTER (WHERE has_building)")
    assert "sum(total_assessed_value_apportioned)" in metrics
    assert "sum(total_assessed_value)" not in metrics


def test_a_category_no_lot_fell_into_reads_as_zero_rather_than_missing():
    """The reason `LOT_CATEGORIES` is a constant and not just SQL.

    The asset reports one metadata entry per category; a key the GROUP BY
    never produced would be a `KeyError` at the end of a successful run.
    """
    cursor = FakeCursor()

    result = compute(cursor)

    assert set(result["by_category"]) == set(LOT_CATEGORIES)
    assert result["by_category"]["built"] == 6
    # No row came back for it, and it still reads as a count.
    assert result["by_category"]["building_sliver"] == 0
    assert result["area_by_category"]["building_sliver"] == 0.0


def test_the_threshold_reaches_the_statement():
    cursor = FakeCursor()

    compute(cursor, max_built_area_m2=60.0)

    insert = next(
        params
        for statement, params in cursor.statements
        if "INSERT INTO gold_lot_profiles_load" in statement
    )
    assert insert["threshold"] == 60.0
    assert insert["neighborhood"] == NEIGHBORHOOD
    assert insert["scrape_date"] == DATE


def test_a_negative_threshold_is_refused_before_anything_is_deleted():
    cursor = FakeCursor()

    with pytest.raises(ValueError, match="must not be negative"):
        compute(cursor, max_built_area_m2=-1.0)

    assert cursor.statements == [], "the partition was touched anyway"


def test_zero_selects_only_the_lots_no_building_touches():
    """0 is a legitimate threshold, not a missing one - `ge=0`, not `gt=0`."""
    cursor = FakeCursor()

    compute(cursor, max_built_area_m2=0.0)

    insert = next(
        params
        for statement, params in cursor.statements
        if "INSERT INTO gold_lot_profiles_load" in statement
    )
    assert insert["threshold"] == 0.0


def test_a_missing_relation_names_the_hbu_infra_file_to_apply():
    """`rag.lot_documents` is the one most likely to be absent.

    sql/006 carries a `-- requires: rag.chunks` header, so `db.py init` skips
    it until a corpus has been indexed.
    """
    cursor = FakeCursor(missing=("rag.lot_documents",))

    with pytest.raises(MissingRelation) as caught:
        compute(cursor)

    message = str(caught.value)
    assert "rag.lot_documents" in message
    assert "sql/006_lot_documents.sql" in message
    assert not any(
        "INSERT" in statement or "DELETE" in statement
        for statement, _ in cursor.statements
    ), "the partition was rewritten against relations that are not there"


def test_every_missing_relation_is_reported_at_once():
    """On a fresh database several are absent, and finding that out one failed
    run per file is three runs too many."""
    cursor = FakeCursor(missing=("silver.lot_frontage", "gold.lot_profiles"))

    with pytest.raises(MissingRelation) as caught:
        compute(cursor)

    message = str(caught.value)
    assert "sql/008_silver_lot_frontage.sql" in message
    assert "sql/009_gold_lot_profiles.sql" in message


def test_the_default_threshold_keeps_a_shed_and_drops_a_garage():
    """A garden shed is 10-30 m2 and a detached garage 30-60."""
    assert DEFAULT_MAX_BUILT_AREA_M2 == 30.0


def test_the_envelopes_are_staged_before_the_partition_is_touched():
    """A malformed envelope should cost nothing.

    The COPY runs ahead of every statement that writes gold.lot_profiles, so a
    partition is only rebuilt once there is something to rebuild it from.
    """
    cursor = FakeCursor()

    compute(
        cursor,
        zoning_envelopes=[("1 234 567", {"feature_id": "C01-001"})],
    )

    issued = [" ".join(statement.split()) for statement, _ in cursor.statements]
    staged = next(
        i for i, text in enumerate(issued)
        if "CREATE TEMP TABLE lot_profiles_envelopes_load" in text
    )
    written = next(
        i for i, text in enumerate(issued)
        if "INSERT INTO gold.lot_profiles" in text
        or text.startswith("DELETE FROM gold.lot_profiles")
    )
    assert staged < written


def test_an_envelope_is_staged_with_its_lot_number_and_its_place_in_the_array():
    """Keyed on lot_number, not lot_uid - a bigserial a reload mints again.

    The order the asset handed them in travels as `ordinal`, so the array is
    ordered by the producer's own decision rather than by anything re-derived
    from inside the jsonb.
    """
    cursor = FakeCursor()

    result = compute(
        cursor,
        zoning_envelopes=[
            ("1 234 567", {"feature_id": "C01-001", "pct_of_lot": 92.0}),
            ("1 234 567", {"feature_id": "C01-002", "pct_of_lot": 8.0}),
            ("7 654 321", {"feature_id": "C01-001", "pct_of_lot": 100.0}),
        ],
    )

    assert result["num_envelopes_staged"] == 3
    assert [row[0] for row in cursor.copied] == [
        "1 234 567",
        "1 234 567",
        "7 654 321",
    ]
    assert [row[1] for row in cursor.copied] == [0, 1, 2]
    assert "lot_number, ordinal, envelope" in cursor.copy_statements[0]


def test_a_partition_with_no_envelopes_still_creates_the_table_it_joins_to():
    """The INSERT names the staging table unconditionally.

    A borough whose grids all failed to parse should land zero envelopes, not
    fail to plan.
    """
    cursor = FakeCursor(num_envelopes=0)

    result = compute(cursor)

    assert result["num_envelopes_staged"] == 0
    assert cursor.copied == []
    assert any(
        "CREATE TEMP TABLE" in statement for statement, _ in cursor.statements
    )


def test_staged_and_landed_are_reported_separately():
    """They differ when the envelope file names lots this cadastre does not
    have, which is what a stale silver/lot_zoning_envelopes looks like."""
    cursor = FakeCursor(num_envelopes=1)

    result = compute(
        cursor,
        zoning_envelopes=[
            ("1 234 567", {"feature_id": "C01-001"}),
            ("no such lot", {"feature_id": "C01-002"}),
        ],
    )

    assert result["num_envelopes_staged"] == 2
    assert result["num_zoning_envelopes"] == 1


def test_the_cmhc_objects_reach_the_statement_as_jsonb_parameters():
    """One object each for the whole borough - CMHC publishes no geometry, so
    there is nothing per-lot about them and nothing to join on."""
    cursor = FakeCursor()

    result = compute(
        cursor,
        vacancy_rates={"survey_year": 2023, "overall_vacancy_rate_pct": 0.5},
        average_rents={"survey_year": 2023, "overall_average_rent_cad": 1_275.0},
    )

    insert = next(
        params
        for statement, params in cursor.statements
        if "INSERT INTO gold_lot_profiles_load" in statement
    )
    assert insert["vacancy_rates"].obj["survey_year"] == 2023
    assert insert["average_rents"].obj["overall_average_rent_cad"] == 1_275.0
    assert result["has_vacancy_rates"] is True
    assert result["has_average_rents"] is True
    # Read back out of the table rather than echoed from what went in.
    assert result["overall_vacancy_rate_pct"] == pytest.approx(0.5)
    assert result["overall_average_rent_cad"] == pytest.approx(1_275.0)


def test_the_cost_guide_reaches_the_statement_as_one_jsonb_parameter():
    """One Montreal object on every row of every borough.

    A stronger version of the CMHC case: CMHC at least surveys neighborhoods,
    while the Altus guide prices nine Canadian markets and publishes no
    geometry at all, so there is nothing whatever to join a rate on.
    """
    cursor = FakeCursor()

    result = compute(
        cursor,
        construction_costs={
            "city": "mtl",
            "condo_band": "condo_wood",
            "underground_stall_cost_low_cad": 51_925.0,
            "condo_cost_low_cad_sqft": 225.0,
            "parking": [{"id": "parkade_ug", "unit_flag": "perStall"}],
        },
    )

    insert = next(
        params
        for statement, params in cursor.statements
        if "INSERT INTO gold_lot_profiles_load" in statement
    )
    assert insert["construction_costs"].obj["condo_band"] == "condo_wood"
    assert insert["construction_costs"].obj["city"] == "mtl"
    assert result["has_construction_costs"] is True


def test_every_flattened_rate_is_read_back_out_of_the_table():
    """Not echoed from what went in: a column and the jsonb it was flattened
    from cannot be allowed to disagree, so the six are re-selected the same way
    the two CMHC figures are."""
    cursor = FakeCursor()

    result = compute(cursor, construction_costs={"city": "mtl"})

    # Dollars per stall - the guide flags these rows `perStall`.
    assert result["underground_stall_cost_low_cad"] == pytest.approx(51_925.0)
    assert result["underground_stall_cost_high_cad"] == pytest.approx(68_675.0)
    assert result["above_grade_stall_cost_low_cad"] == pytest.approx(38_500.0)
    assert result["above_grade_stall_cost_high_cad"] == pytest.approx(57_750.0)
    # Dollars per square foot.
    assert result["condo_cost_low_cad_sqft"] == pytest.approx(225.0)
    assert result["condo_cost_high_cad_sqft"] == pytest.approx(290.0)


def test_the_flattened_rates_are_cast_out_of_the_object_not_passed_beside_it():
    """The INSERT reads each rate column back out of the same jsonb parameter.

    That is what makes them one value rather than two, so the statement carries
    exactly one construction-cost parameter and six casts over it - never a
    seventh parameter a caller could set independently.
    """
    cursor = FakeCursor()

    compute(cursor, construction_costs={"city": "mtl"})

    statement, params = next(
        (statement, params)
        for statement, params in cursor.statements
        if "INSERT INTO gold_lot_profiles_load" in statement
    )
    assert [key for key in params if "construction" in key] == ["construction_costs"]
    for column in (
        "underground_stall_cost_low_cad",
        "underground_stall_cost_high_cad",
        "above_grade_stall_cost_low_cad",
        "above_grade_stall_cost_high_cad",
        "condo_cost_low_cad_sqft",
        "condo_cost_high_cad_sqft",
    ):
        assert f"'{column}'" in statement, f"{column} is not read out of the object"


def test_a_partition_with_no_cost_guide_writes_an_empty_object_not_null():
    """`construction_costs` is NOT NULL in 009_lot_profiles.sql, and '{}' is a
    partition whose bronze snapshot was never read."""
    cursor = FakeCursor()

    result = compute(cursor)

    insert = next(
        params
        for statement, params in cursor.statements
        if "INSERT INTO gold_lot_profiles_load" in statement
    )
    assert insert["construction_costs"].obj == {}
    assert result["has_construction_costs"] is False


def test_a_partition_with_no_cmhc_figures_writes_an_empty_object_not_null():
    """`vacancy_rates` is NOT NULL in 009_gold_lot_profiles.sql, and '{}' is a
    different answer from a suppressed grid."""
    cursor = FakeCursor()

    result = compute(cursor)

    insert = next(
        params
        for statement, params in cursor.statements
        if "INSERT INTO gold_lot_profiles_load" in statement
    )
    assert insert["vacancy_rates"].obj == {}
    assert insert["average_rents"].obj == {}
    assert result["has_vacancy_rates"] is False
