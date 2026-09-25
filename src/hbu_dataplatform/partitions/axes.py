"""Partition definitions: one axis per neighborhood, one per scrape month, one
per tile of the cut.

The neighborhood axis is a `DynamicPartitionsDefinition`: which boroughs the
pipeline scrapes is a fact recorded in Dagster's own instance storage - the
`dagster` schema on Postgres, or the local SQLite instance - rather than a
tuple frozen into this module. Adding a borough is therefore a registration
(`make neighborhood-add NEIGHBORHOOD=CIL`, or `register_neighborhoods`) and
not a deploy, and the UI, the schedules and every `materialize` call read the
same list. What *may* be registered is declared beside it: a key has to be
one `hbu_dataplatform.partitions.cities` can resolve into its city, and each
city's own registry (`hbu_dataplatform.cities.<city>.registry`) says what its
publishers file that key under.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from dagster import (
    DynamicPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

from hbu_dataplatform.core import tile_cut
from hbu_dataplatform.partitions.cities import NEIGHBORHOOD_CITIES

#: The name the neighborhood axis is registered under in the Dagster instance.
#: It is what `instance.get_dynamic_partitions` and the UI's partition dialog
#: know the axis by, so it is spelled once.
NEIGHBORHOOD_PARTITIONS_NAME = "neighborhood"

#: The keys registered when the instance has none yet - a fresh SQLite home,
#: or a Postgres schema Dagster has just created. Not "the enabled
#: neighborhoods": that list lives in the instance and is read back with
#: `enabled_neighborhoods`. Widen the running set with
#: `make neighborhood-add`, not by editing this.
DEFAULT_NEIGHBORHOODS: tuple[str, ...] = ("VSMPE", "CIL")

#: First month the pipeline may scrape. Must be the first of a month: a
#: `MonthlyPartitionsDefinition` cuts its windows on month boundaries and
#: rejects a start date that does not sit on one.
#:
#: `end_offset=1` makes the *current* month a valid partition, which is what
#: "month of scrape" means here - unlike the usual event-time reading, where
#: the latest complete partition would be last month. Partition keys are still
#: `YYYY-MM-DD`, always the first of the month, so nothing downstream that
#: reads a scrape date as a plain ISO date has to change.
SCRAPE_START_DATE = "2026-08-01"

#: Where a scrape month begins and ends. Declared once because two things have
#: to agree about it: the windows `date_partitions` cuts, and the month
#: `hbu_dataplatform.partitions.guards` compares a partition key against. If they disagreed, the
#: guard would be wrong for the hours between this zone's midnight and UTC's.
SCRAPE_TIMEZONE = "America/Toronto"

date_partitions = MonthlyPartitionsDefinition(
    start_date=SCRAPE_START_DATE,
    timezone=SCRAPE_TIMEZONE,
    end_offset=1,
)

#: The borough axis. Its keys are whatever the instance holds under
#: `NEIGHBORHOOD_PARTITIONS_NAME`; `register_neighborhoods` is the one way in
#: and it refuses a key `known_neighborhoods` cannot resolve.
neighborhood_partitions = DynamicPartitionsDefinition(
    name=NEIGHBORHOOD_PARTITIONS_NAME
)

scrape_partitions = MultiPartitionsDefinition(
    {"date": date_partitions, "neighborhood": neighborhood_partitions}
)

#: The name of the second spatial axis: a cell of the tile cut. `tile` rather
#: than `cell` because Dagster prints a multi-partition key with its
#: dimensions in name order, and `date` < `tile` keeps the key reading
#: ``2026-09-01|0302303330102`` - date first, the way the borough keys do.
#: `cell_key`/`cell_partition` stay the *column* vocabulary; this is the
#: partition's.
TILE_DIMENSION = "tile"

#: The tile axis. Static, from the cut, because the cut *is* the checked-in
#: constant (`hbu_dataplatform.core.tile_cut` says why it is not derived): a registry
#: beside it could only drift from it, and a city is added by seeding the
#: cut, which is a deploy. There is no `make tile-add`.
#:
#: What lives on this axis is the lot chain - every silver and gold table
#: whose rows have a lot, a street side or an address point to be placed by.
#: What stays on the borough axis is what a *publisher* bounds: the bronze
#: fetches, the CMHC and C&W tables, the zoning grid, the corpus.
tile_partitions = StaticPartitionsDefinition(sorted(tile_cut.CUT))

tile_scrape_partitions = MultiPartitionsDefinition(
    {"date": date_partitions, TILE_DIMENSION: tile_partitions}
)


def borough_partition_of(context) -> tuple[str, str]:
    """``(neighborhood, scrape_date)`` of a run on the borough axis.

    ``context`` is the `AssetExecutionContext` in hand. The date is cut to
    its ``YYYY-MM-DD`` because a `MultiPartitionKey` carries the date
    dimension as Dagster's own string, which is the same ten characters
    today and need not stay so.
    """
    dimensions = context.partition_key.keys_by_dimension
    return dimensions[NEIGHBORHOOD_PARTITIONS_NAME], dimensions["date"][:10]


def tile_partition_of(context) -> tuple[str, str]:
    """``(tile, scrape_date)`` of a run on the tile axis."""
    dimensions = context.partition_key.keys_by_dimension
    return dimensions[TILE_DIMENSION], dimensions["date"][:10]

def known_neighborhoods() -> tuple[str, ...]:
    """Every key this module can resolve into its sources, both cities."""
    return tuple(NEIGHBORHOOD_CITIES)

def enabled_neighborhoods(instance=None) -> tuple[str, ...]:
    """The keys registered in the Dagster instance, seeding it when empty.

    Reads `instance.get_dynamic_partitions`, which is the same store the UI,
    the schedules and `dagster asset materialize` validate a partition key
    against. An instance that holds no key at all - a fresh home - is seeded
    with `DEFAULT_NEIGHBORHOODS` first, so the very first run has a borough to
    run for and the seeding is visible in the instance afterwards rather than
    being a default nothing recorded.

    ``instance`` is the `DagsterInstance` in hand: `context.instance` inside
    an asset or a schedule, or `DagsterInstance.get()` from a CLI. With none,
    the defaults are returned and nothing is written, which is what a caller
    with no instance - a docstring, a test of the crosswalks - can honestly
    be told.
    """
    if instance is None:
        return DEFAULT_NEIGHBORHOODS
    keys = tuple(instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME))
    if keys:
        return keys
    instance.add_dynamic_partitions(
        NEIGHBORHOOD_PARTITIONS_NAME, list(DEFAULT_NEIGHBORHOODS)
    )
    return DEFAULT_NEIGHBORHOODS


def register_neighborhoods(instance, keys: Iterable[str]) -> tuple[str, ...]:
    """Add ``keys`` to the neighborhood axis; returns the ones newly added.

    Refuses a key `known_neighborhoods` does not list, before touching the
    instance: a registered key with no crosswalk would be offered by the UI
    and fail at `namespace_for` or `borough_boundary` on its first run, which
    is the wrong place to learn that a borough was misspelled.
    """
    wanted = tuple(
        dict.fromkeys(str(key).strip() for key in keys if str(key).strip())
    )
    unknown = [key for key in wanted if key not in NEIGHBORHOOD_CITIES]
    if unknown:
        raise KeyError(
            f"Unknown neighborhood key(s) {unknown}; known keys: "
            f"{', '.join(known_neighborhoods())}"
        )
    existing = set(instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME))
    added = tuple(key for key in wanted if key not in existing)
    if added:
        instance.add_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME, list(added))
    return added


def unregister_neighborhoods(instance, keys: Iterable[str]) -> tuple[str, ...]:
    """Take ``keys`` off the axis; returns the ones that were registered.

    The parquet and the Postgres rows a borough has already produced are left
    exactly where they are - this only stops Dagster offering the key. Put it
    back with `register_neighborhoods` and every existing partition is visible
    again.
    """
    existing = set(instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME))
    removed = tuple(key for key in dict.fromkeys(keys) if key in existing)
    for key in removed:
        instance.delete_dynamic_partition(NEIGHBORHOOD_PARTITIONS_NAME, key)
    return removed

def partition_keys_for(
    scrape_date: str, neighborhoods: Sequence[str]
) -> list[str]:
    """``date|neighborhood`` keys, in the order `MultiPartitionKey` prints them."""
    return [f"{scrape_date}|{neighborhood}" for neighborhood in neighborhoods]
