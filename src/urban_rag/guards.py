"""Refuse to fetch a live source into a month other than the one we are in.

Bronze records *what a publisher returned, and when* - see `urban_rag.layers`.
None of the publishers behind it have a time-travel API: Spectrum's Feature
Service, Infolot, the CKAN portal and CMHC all answer for *now*. So a bronze
partition key is an observation time, and the only observation time a fetch
can honestly claim is the month the fetch happens in.

That makes a bronze backfill a fabrication rather than a gap being filled.
Materializing `bronze/neighborhood_lots/2026-06-01/VSMPE` in September writes
September's cadastre into a directory whose name - and whose `scrape_date`
column - both say June, and nothing downstream can tell afterward: `scraped_at`
is stamped at write time and would say September too.

Re-running an *already materialized* past partition is worse than a lie, it is
a loss. `storage.clear_parquet` empties the directory first, by design, so the
re-run replaces a real observation with a later one. For a live municipal
source that is not something a later date can undo.

Silver and gold are deliberately not guarded. They are deterministic
recomputations over bronze parquet that is already on disk, so re-deriving
history after a fix - a corrected crosswalk, a repaired use code - is exactly
what backfilling is for.

The guard sits inside the asset rather than in front of the runs that reach it
because every entry point converges here and nothing else does: the UI's
backfill dialog, `dagster asset materialize` from the Makefile (which runs
in-process and never sees a run coordinator), a sensor, and the schedules.
`definitions._assert_bronze_assets_guarded` then checks at import time that no
bronze asset was registered without it.
"""

from __future__ import annotations

import functools
from datetime import datetime
from zoneinfo import ZoneInfo

from dagster import Failure

from urban_rag.partitions import SCRAPE_TIMEZONE

#: Run tag that waives the guard for one run, set deliberately in the
#: Launchpad or with `dagster job launch --tag`. It exists for recovery rather
#: than for convenience: a scrape that ran on the 1st, failed on the write, and
#: was noticed on the 2nd of the following month has a real reason to land in
#: the month it was asked for, and so does a run that crosses midnight on the
#: 31st. A door with a name gets used correctly and is visible in the run tags
#: afterward; a wall with no door gets worked around by commenting out the guard.
ALLOW_STALE_SCRAPE_TAG = "urban_rag/allow_stale_scrape"


def current_scrape_month(now: datetime | None = None) -> str:
    """The only partition key a live fetch may honestly write.

    Always the first of the month, in the same timezone `date_partitions` cuts
    its windows in - the two have to agree about where a month ends or the
    guard is wrong for the hours between the zone's midnight and UTC's.
    """
    moment = (now or datetime.now(ZoneInfo(SCRAPE_TIMEZONE))).astimezone(
        ZoneInfo(SCRAPE_TIMEZONE)
    )
    return moment.replace(day=1).strftime("%Y-%m-%d")


def scrape_date_of(context) -> str:
    """The date half of a partition key, whichever shape the key is.

    Bronze is split between the two: island- and province-wide sources are
    keyed by `date_partitions` alone and get a plain `2026-09-01`, while the
    borough-shaped ones are keyed by `scrape_partitions` and get a
    `MultiPartitionKey`. Both carry the same date, in the same format.
    """
    key = context.partition_key
    dimensions = getattr(key, "keys_by_dimension", None)
    return (dimensions["date"] if dimensions else key)[:10]


def is_stale_scrape(scrape_date: str) -> bool:
    """Whether ``scrape_date`` names a month other than the one being lived in.

    The one predicate the guard consults, so there is a single place to reason
    about - and, in the unit tests, a single seam to hold still. Those tests
    are pinned to fixed August-2026 fixtures and are not about the calendar, so
    `tests/unit/conftest.py` turns this off for them and the tests that *are*
    about the guard mark themselves to keep it live.
    """
    return scrape_date != current_scrape_month()


def guard_current_scrape_month(fn):
    """Fail the run unless its partition is the month being lived in.

    Applied *under* `@asset`, so Dagster still builds the op from the real
    signature - `functools.wraps` sets `__wrapped__` and `inspect.signature`
    follows it, which is what keeps the resource parameters visible.
    """

    @functools.wraps(fn)
    def wrapper(context, *args, **kwargs):
        scrape_date = scrape_date_of(context)
        if is_stale_scrape(scrape_date):
            current = current_scrape_month()
            asset_name = context.asset_key.path[-1]
            if context.run.tags.get(ALLOW_STALE_SCRAPE_TAG) != "true":
                raise Failure(
                    description=(
                        f"{'/'.join(context.asset_key.path)} was asked for "
                        f"{scrape_date}, but bronze records what a publisher "
                        f"returned *now* - the sources behind it have no time "
                        f"travel, so this run would write {current} data under "
                        f"a {scrape_date} key. Materialize {current} instead. "
                        f"If you are recovering an interrupted run, re-launch "
                        f"with the tag {ALLOW_STALE_SCRAPE_TAG}=true."
                    )
                )
            context.log.warning(
                "%s is fetching into %s while the current scrape month is %s; "
                "allowed by %s. What lands will be %s data under a %s key.",
                asset_name,
                scrape_date,
                current,
                ALLOW_STALE_SCRAPE_TAG,
                current,
                scrape_date,
            )
        return fn(context, *args, **kwargs)

    wrapper.__guards_scrape_month__ = True
    return wrapper


def guards_scrape_month(definition) -> bool:
    """Whether a registered asset's compute function carries the guard."""
    compute_fn = definition.op.compute_fn
    inner = getattr(compute_fn, "decorated_fn", compute_fn)
    return bool(getattr(inner, "__guards_scrape_month__", False))
