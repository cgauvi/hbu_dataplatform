"""Offline test for `compute_intersections`'s sliver screen, against a stub.

`ST_Intersection` needs PostGIS and there is none here, so what this pins is
everything around the clip: that the screen reaches the statement, that its two
cutoffs arrive as bound parameters rather than as interpolated text, and that
psycopg will accept the statement at all.

The last one is not hypothetical - see `test_postgis_lot_profiles` for the
per-cent sign that broke a query from inside a comment. The screen this file
covers computes a *percentage*, so it is exactly the shape that goes wrong.
"""

from __future__ import annotations

import pytest
from psycopg._queries import _query2pg_nocache

from urban_rag import postgis
from urban_rag.postgis import (
    MIN_BUILDING_OVERLAP_M2,
    MIN_BUILDING_PCT_OF_BUILDING,
    compute_intersections,
)

NEIGHBORHOOD = "VSMPE"
DATE = "2026-09-01"


class FakeCursor:
    """Answers the one statement `compute_intersections` runs for itself."""

    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(sql)

    def fetchone(self):
        return (2, 1234.5)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


@pytest.fixture
def captured(monkeypatch):
    """The arguments `compute_intersections` hands `warehouse.upsert_select`."""
    seen: dict[str, object] = {}

    def upsert_select(cursor, dataset, columns, select, params=None, **kwargs):
        seen["dataset"] = dataset
        seen["columns"] = tuple(columns)
        seen["select"] = select
        seen["params"] = params
        seen["kwargs"] = kwargs
        return {"upserted": 7, "pruned": 0}

    monkeypatch.setattr(postgis.warehouse, "upsert_select", upsert_select)
    return seen


def run(**kwargs):
    cursor = FakeCursor()
    result = compute_intersections(
        FakeConnection(cursor), neighborhood=NEIGHBORHOOD, scrape_date=DATE, **kwargs
    )
    return result, cursor


def test_the_screen_is_in_the_statement(captured):
    """Both halves of it, and as an `or` rather than an `and`.

    A footprint is kept when it is large enough to be a building *or* when it
    is enough of its own footprint to be one. Written as an `and` this would
    delete the terraces the table exists to report - a townhouse standing
    wholly on its parcel is a few per cent of the block-long outline BDOI drew
    it inside - so the operator is the assertion.
    """
    run()
    select = captured["select"]

    assert "ST_Dimension(clipped.geom) = 2" in select
    assert "%(min_overlap_m2)s" in select
    assert "%(min_pct_of_building)s" in select

    screen = select[select.index("ST_Dimension(clipped.geom) = 2"):]
    assert " OR " in screen


def test_the_cutoffs_are_bound_parameters_at_their_documented_values(captured):
    run()

    assert captured["params"]["min_overlap_m2"] == MIN_BUILDING_OVERLAP_M2
    assert captured["params"]["min_pct_of_building"] == MIN_BUILDING_PCT_OF_BUILDING
    # The partition still keys the write, and the cutoffs are not part of it.
    assert captured["kwargs"] == {
        "neighborhood": NEIGHBORHOOD,
        "scrape_date": DATE,
    }


def test_a_caller_can_ask_at_another_value(captured):
    """The constants are the default, not the only answer.

    `ZonePieceConfig` does the same for the zone cutoffs, and for the same
    reason: the value is a judgement about the borough, and a caller
    re-deriving the table at another one should not have to edit the module.
    """
    run(min_overlap_m2=2.5, min_pct_of_building=1.0)

    assert captured["params"]["min_overlap_m2"] == 2.5
    assert captured["params"]["min_pct_of_building"] == 1.0


def test_the_statement_is_one_psycopg_will_accept(captured):
    """A bare `%` anywhere - a comment included - breaks it at execution time.

    The screen divides by an area and multiplies by 100, so the temptation to
    write "per cent" as a sign is right there in the code it guards.
    """
    run()

    _query2pg_nocache(captured["select"].encode(), "utf-8")


def test_the_summary_still_counts_what_landed(captured):
    """The screen changes the rows, not the shape of the dict above them."""
    result, cursor = run()

    assert result == {
        "intersections": 7,
        "pruned": 0,
        "buildings_matched": 2,
        "total_area_m2": 1234.5,
    }
    assert len(cursor.statements) == 1
