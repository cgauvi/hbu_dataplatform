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

from urban_rag import guards


@pytest.fixture(autouse=True)
def scrape_month_guard(request, monkeypatch):
    """Freeze the bronze month guard unless the test asks for it."""
    if request.node.get_closest_marker("scrape_month_guard"):
        return
    monkeypatch.setattr(guards, "is_stale_scrape", lambda scrape_date: False)
