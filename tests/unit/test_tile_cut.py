"""The cut: variable-depth partitions over the tile grid.

Three properties carry it, and each has a test here:

**Every lot lands in exactly one cell.** Not "at most one" - a lot with no
partition is a lot nothing writes, and a lot with two is a lot two runs both
claim. `tile_grid.validate_cut` is what rules out the second and
`test_every_key_lands_in_exactly_one_cell` is what checks the first.

**A cell is about one run's worth of work.** That is the whole reason the cut
is adaptive rather than a fixed zoom: lot density varies by three orders of
magnitude between downtown Montreal and the Saguenay forest, so one zoom is
either 25,000 lots or 40. `test_the_cut_is_deeper_where_the_lots_are` is the
one that would fail if the recursion stopped adapting.

**It never writes itself.** The cut is a checked-in constant; an asset reports
drift and a human promotes it. `cut_report` is that report, and it returns
numbers rather than a new cut on purpose.

Nothing here touches a database: `build_cut` is a pure function of a list of
quadkeys.
"""

from __future__ import annotations

import random

import pytest

from urban_rag import tile_cut, tile_grid


def cluster(count, lon, lat, spread, *, seed):
    """``count`` lots scattered around a point, as full-depth quadkeys."""
    rng = random.Random(seed)
    return [
        tile_grid.quadkey_of(lon + rng.gauss(0, spread), lat + rng.gauss(0, spread))
        for _ in range(count)
    ]


@pytest.fixture
def three_cities():
    """Dense Montreal, a smaller Quebec City, a sparse Saguenay.

    The real spread this design exists for - roughly the densities of the three
    cities the pipeline loads, at a scale a unit test can hold.
    """
    return (
        cluster(40_000, -73.60, 45.53, 0.04, seed=1)
        + cluster(12_000, -71.22, 46.81, 0.05, seed=2)
        + cluster(1_200, -71.07, 48.42, 0.20, seed=3)
    )


def test_every_key_lands_in_exactly_one_cell(three_cities):
    cut = tile_cut.build_cut(three_cities, budget=5_000)
    tile_grid.validate_cut(cut)

    for key in random.Random(9).sample(three_cities, 400):
        holders = [cell for cell in cut if key.startswith(cell)]
        assert holders == [tile_grid.cut_cell_of(key, cut)]


def test_no_cell_holds_more_than_the_budget(three_cities):
    for budget in (20_000, 5_000, 1_000):
        cut = tile_cut.build_cut(three_cities, budget=budget)
        counts: dict[str, int] = {}
        for key in three_cities:
            cell = tile_grid.cut_cell_of(key, cut)
            counts[cell] = counts.get(cell, 0) + 1

        assert sum(counts.values()) == len(three_cities)
        assert max(counts.values()) <= budget


def test_the_cut_is_deeper_where_the_lots_are(three_cities):
    """The point of an adaptive cut, stated as an assertion.

    A fixed zoom that suits Montreal buries the Saguenay in empty partitions,
    and one that suits the Saguenay gives Montreal a partition it cannot
    materialize in an evening.
    """
    cut = tile_cut.build_cut(three_cities, budget=5_000)
    depths = {len(cell) for cell in cut}
    assert len(depths) > 1, "a cut with one depth is a fixed zoom, not a cut"

    def depth_at(lon, lat):
        key = tile_grid.quadkey_of(lon, lat)
        return len(tile_grid.cut_cell_of(key, cut))

    assert depth_at(-73.60, 45.53) > depth_at(-71.07, 48.42)


def test_a_cell_with_no_lots_is_not_a_partition(three_cities):
    """The cut covers ground that has lots, and says so when ground does not.

    A key outside it is not assigned to the nearest cell or to a catch-all -
    both would put rows somewhere nobody chose. `num_keys_outside_cut` is how a
    newly loaded city announces itself.
    """
    cut = tile_cut.build_cut(three_cities, budget=5_000)
    counts = {tile_grid.cut_cell_of(k, cut) for k in three_cities}
    assert counts == set(cut), "cells holding nothing should not be emitted"

    paris = cluster(50, 2.35, 48.85, 0.01, seed=4)
    with pytest.raises(KeyError, match="no cell of the cut"):
        tile_grid.cut_cell_of(paris[0], cut)

    report = tile_cut.cut_report(three_cities + paris, cut=cut, budget=5_000)
    assert report["num_keys_outside_cut"] == 50
    assert report["num_tiles_over_budget"] == 0
    assert report["num_cells_holding_keys"] == len(cut)


def test_the_report_proposes_and_does_not_decide(three_cities):
    """Read against a budget tighter than the cut was built for.

    The cells that have outgrown it are named, in sorted order so two reports
    diff cleanly - and the cut itself is untouched.
    """
    cut = tile_cut.build_cut(three_cities, budget=20_000)
    report = tile_cut.cut_report(three_cities, cut=cut, budget=2_000)

    assert report["num_tiles_over_budget"] > 0
    assert report["proposed_splits"] == sorted(report["proposed_splits"])
    assert all(cell in cut for cell in report["proposed_splits"])
    assert report["max_lots_in_a_tile"] > 2_000

    # Reading the report changed nothing.
    assert tile_cut.build_cut(three_cities, budget=20_000) == cut


def test_an_empty_cut_is_an_error_rather_than_a_default():
    """A cut that quietly fell back to "one cell for the world" would put every
    row in a partition nobody chose, and it would look like it worked."""
    with pytest.raises(RuntimeError, match="has not been seeded"):
        tile_cut.cell_partition_of("0302303330102123123", cut=frozenset())


def test_the_live_cut_is_seeded_and_well_formed():
    """The checked-in cut, against the properties every cut has to have.

    Seeded from hbu-dev on 2026-09-22. This does not assert the exact members -
    that would make adding a city a test failure - but it does assert that what
    is checked in could not misassign a lot.
    """
    assert tile_cut.CUT_VERSION >= 1
    assert tile_cut.CUT, "the live cut is empty; see scripts/seed_tile_cut.py"

    # No member nests inside another, so `cut_cell_of` names exactly one cell.
    tile_grid.validate_cut(tile_cut.CUT)

    # Every member is a real quadkey at a depth the grid has.
    for cell in tile_cut.CUT:
        assert 1 <= len(cell) <= tile_grid.BASE_CELL_ZOOM
        tile_grid.cell_of_quadkey(cell)


def test_every_cell_of_the_live_cut_names_its_city():
    """A run on the tile axis has no borough to ask the CRS of; it asks the
    cell. So the cut and its cities are one mapping, and every value is a
    city the crosswalks know."""
    from urban_rag.partitions import City, city_of_tile

    assert set(tile_cut.TILE_CITIES) == tile_cut.CUT
    for cell in tile_cut.CUT:
        assert isinstance(city_of_tile(cell), City)
        assert tile_cut.city_value_of(cell) == city_of_tile(cell).value


def test_a_cell_outside_the_cut_has_no_city():
    with pytest.raises(KeyError, match="not a cell of the cut"):
        tile_cut.city_value_of("0302310121")


def test_the_live_cut_still_covers_the_cities_it_was_built_from():
    """A lot in Villeray and one in La Cité-Limoilou each resolve to one cell.

    The regression this guards is a re-cut that drops a branch: `cut_cell_of`
    raises for uncovered ground rather than guessing, so the symptom would be a
    partition that cannot be written rather than one written wrongly - but it
    should be caught here, not there.
    """
    for lon, lat in [(-73.6210, 45.5395), (-71.2200, 46.8100)]:
        key = tile_grid.quadkey_of(lon, lat)
        cell = tile_cut.cell_partition_of(key)
        assert key.startswith(cell)


def test_building_a_cut_is_deterministic(three_cities):
    """Same lots, same cut - whatever order they arrive in.

    The cut is a partition key; one that depended on row order would re-key the
    axis on a reload that changed nothing.
    """
    shuffled = list(three_cities)
    random.Random(5).shuffle(shuffled)
    assert tile_cut.build_cut(shuffled, budget=5_000) == tile_cut.build_cut(
        three_cities, budget=5_000
    )


def test_an_empty_cadastre_is_an_empty_cut_not_a_crash():
    assert tile_cut.build_cut([], budget=10) == frozenset()


def test_a_budget_that_cannot_be_met_stops_at_the_grid_floor():
    """More than `budget` lots at one point is a bad geocode, not a reason to
    recurse forever. The cell is emitted over budget and the report says so."""
    same_spot = [tile_grid.quadkey_of(-73.60, 45.53)] * 50
    cut = tile_cut.build_cut(same_spot, budget=10, max_zoom=6)

    assert cut == {tile_grid.quadkey_of(-73.60, 45.53)[:6]}
    assert tile_cut.cut_report(same_spot, cut=cut, budget=10)[
        "num_tiles_over_budget"
    ] == 1
