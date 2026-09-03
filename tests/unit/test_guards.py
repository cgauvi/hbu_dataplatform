"""The bronze scrape-month guard: what it refuses, what it lets through.

These are the tests the suite-wide opt-out in `conftest.py` does not apply to -
each one carries `@pytest.mark.scrape_month_guard`, so the real predicate runs
and the clock is moved by patching `current_scrape_month` rather than by
waiting.

What is under test is the refusal itself and the two things that make it
usable: that it names the partition to materialize instead, and that the
recovery tag gets through. `test_definitions.py` covers the other half - that
no bronze asset can be registered without it.
"""

# No `from __future__ import annotations` here, unlike most of this suite, and
# not an oversight: Dagster resolves the `context` parameter's annotation off
# the function object, and PEP 563 turns it into a string it will not accept -
# with or without the guard decorator. It is why no asset module in `src/` uses
# that import either.

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MultiPartitionKey,
    asset,
    materialize,
)

from urban_rag import guards
from urban_rag.guards import (
    ALLOW_STALE_SCRAPE_TAG,
    current_scrape_month,
    guard_current_scrape_month,
    guards_scrape_month,
    scrape_date_of,
)
from urban_rag.partitions import SCRAPE_TIMEZONE, date_partitions, scrape_partitions

pytestmark = pytest.mark.scrape_month_guard

#: The month these tests pretend to be living in.
NOW = "2026-09-01"
PAST = "2026-08-01"
NEIGHBORHOOD = "VSMPE"


@pytest.fixture
def clock(monkeypatch):
    """Pin the guard's idea of the current month to `NOW`."""
    monkeypatch.setattr(guards, "current_scrape_month", lambda now=None: NOW)


# A stand-in for a bronze asset rather than a real one: what is under test is
# the decorator, and a real asset would drag a publisher's stub in with it.
# `fetched` records whether the body ran, which is the thing that must not
# happen on a refusal - the guard has to fire before the fetch and before
# `clear_parquet` empties the directory.
@asset(key_prefix=["bronze"], partitions_def=date_partitions)
@guard_current_scrape_month
def dated_source(context: AssetExecutionContext) -> MaterializeResult:
    fetched.append(context.partition_key)
    return MaterializeResult(metadata={"rows": 1})


@asset(key_prefix=["bronze"], partitions_def=scrape_partitions)
@guard_current_scrape_month
def borough_source(context: AssetExecutionContext) -> MaterializeResult:
    fetched.append(context.partition_key.keys_by_dimension["date"])
    return MaterializeResult(metadata={"rows": 1})


fetched: list[str] = []


@pytest.fixture(autouse=True)
def _clear_record():
    fetched.clear()
    yield
    fetched.clear()


def test_the_current_month_is_allowed_through(clock):
    result = materialize([dated_source], partition_key=NOW)
    assert result.success
    assert fetched == [NOW]


def test_a_past_month_is_refused(clock):
    with pytest.raises(Failure) as raised:
        materialize([dated_source], partition_key=PAST)
    assert "was asked for 2026-08-01" in str(raised.value)


def test_the_refusal_happens_before_the_fetch(clock):
    """The whole point: nothing is downloaded and no directory is cleared."""
    with pytest.raises(Failure):
        materialize([dated_source], partition_key=PAST)
    assert fetched == []


def test_the_refusal_names_the_partition_to_materialize_instead(clock):
    with pytest.raises(Failure) as raised:
        materialize([dated_source], partition_key=PAST)
    assert f"Materialize {NOW} instead" in str(raised.value)


def test_the_refusal_names_the_recovery_tag(clock):
    with pytest.raises(Failure) as raised:
        materialize([dated_source], partition_key=PAST)
    assert f"{ALLOW_STALE_SCRAPE_TAG}=true" in str(raised.value)


def test_the_recovery_tag_lets_a_past_month_through(clock):
    result = materialize(
        [dated_source],
        partition_key=PAST,
        tags={ALLOW_STALE_SCRAPE_TAG: "true"},
    )
    assert result.success
    assert fetched == [PAST]


def test_any_other_tag_value_does_not_waive_the_guard(clock):
    """Only the exact string waives it - a stray `1` or `yes` must not."""
    for value in ("1", "yes", "True", ""):
        with pytest.raises(Failure):
            materialize(
                [dated_source],
                partition_key=PAST,
                tags={ALLOW_STALE_SCRAPE_TAG: value},
            )
    assert fetched == []


def test_a_multipartition_key_is_read_on_its_date_axis(clock):
    """The borough-shaped half of bronze carries a `MultiPartitionKey`."""
    key = MultiPartitionKey({"date": PAST, "neighborhood": NEIGHBORHOOD})
    with pytest.raises(Failure) as raised:
        materialize([borough_source], partition_key=key)
    assert "was asked for 2026-08-01" in str(raised.value)


def test_a_multipartition_current_month_is_allowed_through(clock):
    key = MultiPartitionKey({"date": NOW, "neighborhood": NEIGHBORHOOD})
    result = materialize([borough_source], partition_key=key)
    assert result.success
    assert fetched == [NOW]


def test_the_decorator_leaves_the_asset_usable():
    """`functools.wraps` is what keeps Dagster reading the real signature."""
    assert dated_source.op.compute_fn.decorated_fn.__name__ == "dated_source"
    assert guards_scrape_month(dated_source)


class _Key:
    def __init__(self, key):
        self.partition_key = key


def test_scrape_date_of_reads_a_plain_key():
    assert scrape_date_of(_Key("2026-09-01")) == "2026-09-01"


def test_scrape_date_of_reads_a_multipartition_key():
    key = MultiPartitionKey({"date": "2026-09-01", "neighborhood": NEIGHBORHOOD})
    assert scrape_date_of(_Key(key)) == "2026-09-01"


def test_scrape_date_of_trims_a_timestamped_key():
    """`MultiPartitionsDefinition` hands the date axis back as `YYYY-MM-DD`."""
    assert scrape_date_of(_Key("2026-09-01-00:00")) == "2026-09-01"


@pytest.mark.parametrize(
    ("moment", "expected"),
    [
        ("2026-09-01T00:00:00", "2026-09-01"),
        ("2026-09-03T16:29:00", "2026-09-01"),
        ("2026-09-30T23:59:59", "2026-09-01"),
        ("2026-12-31T23:59:59", "2026-12-01"),
    ],
)
def test_current_scrape_month_is_always_the_first_of_the_month(moment, expected):
    now = datetime.fromisoformat(moment).replace(tzinfo=ZoneInfo(SCRAPE_TIMEZONE))
    assert current_scrape_month(now) == expected


def test_current_scrape_month_uses_the_partitions_timezone():
    """The hours where UTC has rolled over and Montreal has not.

    01:30 UTC on the 1st is 21:30 on the last day of the previous month in
    Toronto, and `date_partitions` cuts its windows in Toronto - so a guard
    reading UTC would demand the *next* month's key for half a day.
    """
    utc_says_september = datetime(2026, 9, 1, 1, 30, tzinfo=ZoneInfo("UTC"))
    assert current_scrape_month(utc_says_september) == "2026-08-01"


def test_the_conftest_opt_out_is_not_in_force_here():
    """A guard against the guard's own tests being silently neutered.

    Everything above depends on `is_stale_scrape` being the real one. If the
    marker ever stops holding conftest off, these tests would keep passing
    while testing nothing, so the seam itself is asserted.
    """
    assert guards.is_stale_scrape("1999-01-01") is True
