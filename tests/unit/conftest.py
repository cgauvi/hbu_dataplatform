"""Fixtures every unit test gets, and the one cross-cutting guard they opt out of.

The bronze assets refuse to fetch into a month other than the one being lived
in - see `urban_rag.guards`. That is a guard about the wall clock, and the unit
tests are pinned to fixed August-2026 fixtures: left live, it would fail all of
them from September 2026 onward, and every one of those failures would be about
the calendar rather than about the asset under test.

So it is held off here for the suite at large, and the tests that are actually
about it mark themselves `@pytest.mark.scrape_month_guard` to keep it running.
That way the opt-out is visible in one place instead of being spelled as a run
tag at each of the ninety-odd `materialize` calls that would otherwise need it.
"""

from __future__ import annotations

import pytest
from dagster import DagsterInstance
from dagster._core.definitions.partitions.context import partition_loading_context

from urban_rag import guards
from urban_rag.partitions import DEFAULT_NEIGHBORHOODS, NEIGHBORHOOD_PARTITIONS_NAME


@pytest.fixture(autouse=True)
def scrape_month_guard(request, monkeypatch):
    """Freeze the bronze month guard unless the test asks for it."""
    if request.node.get_closest_marker("scrape_month_guard"):
        return
    monkeypatch.setattr(guards, "is_stale_scrape", lambda scrape_date: False)


@pytest.fixture(autouse=True)
def seeded_neighborhood_axis(monkeypatch):
    """Every ephemeral instance a test materializes against knows the defaults.

    The neighborhood axis is dynamic - its keys live in the Dagster instance,
    not in code - and `materialize()` validates a partition key against the
    instance it runs on, which for the suite is the ephemeral one it creates
    itself. So that instance is seeded with `DEFAULT_NEIGHBORHOODS` on
    creation, the way `enabled_neighborhoods` seeds a fresh home, and a test
    can ask for ``2026-08-01|VSMPE`` the way it always has.
    """
    original = DagsterInstance.ephemeral

    def seeded(*args, **kwargs):
        instance = original(*args, **kwargs)
        instance.add_dynamic_partitions(
            NEIGHBORHOOD_PARTITIONS_NAME, list(DEFAULT_NEIGHBORHOODS)
        )
        return instance

    monkeypatch.setattr(DagsterInstance, "ephemeral", staticmethod(seeded))
    # `execute_in_process` validates the key *before* it creates its instance,
    # through whatever partition-loading context is current; give it one that
    # knows the same keys.
    with partition_loading_context(dynamic_partitions_store=seeded()):
        yield
