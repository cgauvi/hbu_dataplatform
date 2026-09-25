"""The cut: which cells of the tile grid are partitions, and why it is frozen.

`tile_grid` defines the grid and how to address a cell on it. This decides
**which** cells the pipeline actually partitions on - a variable-depth set,
cut finer where there are more lots, so a run is roughly the same amount of
work wherever it is.

Free of Dagster and of psycopg, like `tile_grid` and `map_tiles`: `build_cut`
is a pure function of a list of addresses, and everything else here is a
lookup.

---------------------------------------------------------------------------
Why it is frozen rather than derived
---------------------------------------------------------------------------

The obvious thing is an asset that recounts lots every month and re-cuts. That
would destroy the one property the whole scheme is for. A Dagster partition key
is not just a label - materialization records, backfills and every downstream
partition are keyed on it - so a cut that moves when a subdivision lands in
Villeray re-keys the axis and orphans all of it, every month, forever.

So the cut is a checked-in constant. `CUT` below is the live one and
`CUT_VERSION` names it. An asset may *propose* changes -
`num_tiles_over_budget`, `max_lots_in_a_tile`, `proposed_splits` - and a human
promotes them in a deliberate migration. Drift is a report, never a write.

---------------------------------------------------------------------------
What changes, and what a change costs
---------------------------------------------------------------------------

**Adding a city is additive.** Its ground is under cells nothing else uses, so
new members appear and existing ones are untouched.

**Re-cutting a branch** is the one disruptive change, and it is mechanical
because a row's `cell_key` is at full depth and never moves:

    cell_key        0302303330102123123      unchanged, always
    cell_partition  0302303330102       ->   03023033301021

Register the four children, move the rows by prefix, unregister the parent,
drop its leaves. Nothing is recomputed from geometry, and the history stays
exactly comparable - the four children sum to what the parent held, because
tiles nest perfectly (`tile_grid.parent_of`). That is the thing a borough
redraw could never offer.

One trap, and it is not obvious: `warehouse.dataset_versions` is keyed on
`cell_partition`, so the four children look like datasets nobody has ever
loaded. Seed their digests from the parent's in the same transaction, or the
next tick re-fetches ground that did not change.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable

from urban_rag import tile_grid

#: Lots per cell the cut aims at. Roughly what VSMPE holds (24,953 on
#: 2026-08-20), which the chain already materializes end to end in 2h23m - so
#: a cell is "about one run's worth" by construction rather than by a number
#: somebody liked. Raising it makes runs longer and partitions fewer; lowering
#: it makes change detection finer, since a digest is per cell and a
#: subdivision only invalidates the cell it lands in.
DEFAULT_BUDGET = 20_000

#: Bumped whenever `CUT` changes. Stored beside the data so a partition can say
#: which cut minted it, and so a reader can tell a re-cut from a reload.
#: ``0`` means the cut has not been seeded yet.
CUT_VERSION = 2

#: The live cut: every cell the pipeline partitions on, as quadkeys.
#:
#: Seeded 2026-09-25 by `scripts/seed_tile_cut.py` over hbu-dev's 147,147 lots
#: of 2026-09-01 - VSMPE, CIL, SSC and SAG - at `DEFAULT_BUDGET`. 23 cells,
#: zooms 9 to 13, largest holding 18,239 lots and none over budget.
#:
#: The spread is the design working: Montreal's five cells are z13 but for the
#: z9 that is the rest of the island with few lots on it, Quebec City's six are
#: z10 and z12 where SSC's density pushed a branch down, and Saguenay's twelve
#: run z9 to z12 over a 1,150 km² city. A fixed zoom that suited one city would
#: have been wrong for the others.
#:
#: Version 1 (2026-09-22, ten cells) was replaced before anything was
#: partitioned on it: it counted VSMPE's two scrape dates as two sets of lots,
#: which split its cell into four z14s it did not need, and SSC and SAG were
#: not in it. The script now cuts over one date.
#:
#: **This grows when a city is loaded, and that is additive** - new ground is
#: under cells nothing else uses. Re-cutting an existing branch is the
#: disruptive case; see the module docstring for what it costs and the trap in
#: `warehouse.dataset_versions` that comes with it.
#:
#: Each cell names the city whose publishers its ground belongs to, as the
#: plain value of `urban_rag.partitions.City` (a string here rather than the
#: enum, because `partitions` imports this module and not the other way
#: round). A cell has exactly one city by construction: the cut is built per
#: city's lots and two cities' ground diverges high in the tree - Montreal
#: and Quebec City part at the seventh digit. A run on the tile axis has no
#: borough to ask the CRS or the CMHC centre of, so it asks the cell.
TILE_CITIES: dict[str, str] = {
    # Montreal - VSMPE and the island around it
    "030230331": "montreal",
    "0302303330100": "montreal",
    "0302303330101": "montreal",
    "0302303330102": "montreal",
    "0302303330103": "montreal",
    # Quebec City - CIL and SSC
    "0302312100": "quebec",
    "0302312101": "quebec",
    "0302312102": "quebec",
    "030231210300": "quebec",
    "030231210301": "quebec",
    "030231210302": "quebec",
    # Saguenay - SAG
    "030231010": "saguenay",
    "030231011": "saguenay",
    "0302310120": "saguenay",
    "03023101210": "saguenay",
    "030231012110": "saguenay",
    "030231012111": "saguenay",
    "030231012112": "saguenay",
    "030231012113": "saguenay",
    "03023101212": "saguenay",
    "03023101213": "saguenay",
    "0302310123": "saguenay",
    "030231013": "saguenay",
}

CUT: frozenset[str] = frozenset(TILE_CITIES)


def city_value_of(tile: str) -> str:
    """The city a cut cell's ground belongs to, as a `City` value string.

    ``tile`` is a member of `CUT`, not a full-depth key: resolve a lot's
    `cell_key` with `cell_partition_of` first. Raises `KeyError` for a cell
    that is not in the cut, which is the same answer `cell_partition_of` gives
    for ground the cut does not cover.
    """
    try:
        return TILE_CITIES[tile]
    except KeyError:
        raise KeyError(
            f"{tile!r} is not a cell of the cut (version {CUT_VERSION}); "
            f"the cut has {len(CUT)} cell(s)"
        ) from None


def build_cut(
    keys: Iterable[str],
    *,
    budget: int = DEFAULT_BUDGET,
    min_zoom: int = 1,
    max_zoom: int | None = None,
) -> frozenset[str]:
    """The coarsest cut in which no cell holds more than ``budget`` of ``keys``.

    ``keys`` are full-depth quadkeys - one per lot, from
    `tile_grid.quadkey_of(lon, lat)` over a representative point. Subdivides
    from the root and stops a branch as soon as it is small enough, so dense
    ground ends up deep and empty ground stays shallow.

    Cells with no keys under them are **not** emitted. A cut is built over
    ground that has lots on it; the rest of the world is not a partition
    nobody writes to, it is simply absent. `tile_grid.cut_cell_of` raises for a
    key outside the cut, which is the honest answer to "which partition owns a
    lot in a city we have never loaded".

    ``max_zoom`` bounds the recursion for the pathological case - more than
    ``budget`` lots at a single point, which a bad geocode really can produce -
    and defaults to the grid's own floor. A cell at that depth may exceed the
    budget; that is reported rather than looped on forever.
    """
    if budget < 1:
        raise ValueError(f"budget must be at least 1, got {budget}")
    depth_limit = tile_grid.BASE_CELL_ZOOM if max_zoom is None else max_zoom
    if not 1 <= min_zoom <= depth_limit:
        raise ValueError(
            f"min_zoom must be between 1 and {depth_limit}, got {min_zoom}"
        )

    ordered = sorted(keys)
    if not ordered:
        return frozenset()

    cut: set[str] = set()

    def walk(prefix: str, low: int, high: int) -> None:
        """``ordered[low:high]`` are exactly the keys under ``prefix``."""
        if low >= high:
            return
        depth = len(prefix)
        if depth >= min_zoom and (high - low <= budget or depth >= depth_limit):
            cut.add(prefix)
            return
        for digit in "0123":
            child = prefix + digit
            # The keys under a child are a contiguous run, because the list is
            # sorted and a prefix orders its descendants together - the same
            # Z-order property the partition key is chosen for.
            start = bisect_left(ordered, child, low, high)
            end = bisect_right(ordered, child + tile_grid.ABOVE_ALL_QUADKEYS, start, high)
            walk(child, start, end)

    for digit in "0123":
        start = bisect_left(ordered, digit)
        end = bisect_right(ordered, digit + tile_grid.ABOVE_ALL_QUADKEYS)
        walk(digit, start, end)

    tile_grid.validate_cut(cut)
    return frozenset(cut)


def cell_partition_of(cell_key: str, cut: frozenset[str] | None = None) -> str:
    """The partition that owns ``cell_key``, from the live cut.

    A prefix walk, no database and no geometry - see `tile_grid.cut_cell_of`.
    """
    live = CUT if cut is None else cut
    if not live:
        raise RuntimeError(
            "the tile cut has not been seeded: urban_rag.tile_cut.CUT is "
            "empty, so no cell_key resolves to a partition. Build one with "
            "`build_cut` over the cadastre's quadkeys and check the result in "
            "as CUT, bumping CUT_VERSION - see the module docstring on why it "
            "is a constant rather than an asset."
        )
    return tile_grid.cut_cell_of(cell_key, live)


def cut_report(
    keys: Iterable[str],
    *,
    cut: frozenset[str] | None = None,
    budget: int = DEFAULT_BUDGET,
) -> dict[str, object]:
    """How the live cut is holding up against the keys actually present.

    The drift an asset reports and never acts on: which cells have outgrown the
    budget, the worst one, and what splitting them would produce. Everything
    here is a number for a human to read before deciding to re-cut.

    ``num_keys_outside_cut`` is the one to watch after loading a new city - it
    counts ground the cut does not cover at all, which is not drift but a
    missing branch.
    """
    live = CUT if cut is None else cut
    counts: dict[str, int] = {}
    outside = 0
    for key in keys:
        try:
            cell = tile_grid.cut_cell_of(key, live)
        except KeyError:
            outside += 1
            continue
        counts[cell] = counts.get(cell, 0) + 1

    over = {cell: n for cell, n in counts.items() if n > budget}
    return {
        "cut_version": CUT_VERSION,
        "num_cells": len(live),
        "num_cells_holding_keys": len(counts),
        "num_keys_outside_cut": outside,
        "num_tiles_over_budget": len(over),
        "max_lots_in_a_tile": max(counts.values(), default=0),
        "budget": budget,
        # Sorted so a report is stable between runs and a diff of two reports
        # is readable.
        "proposed_splits": sorted(over),
    }
