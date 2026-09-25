"""The content digest, and mostly the ways it could be quietly wrong.

A digest decides whether a month's data gets written at all, so it has two
failure directions and they are not symmetric.

**Too sensitive** is loud and cheap: the digest changes every tick, nothing is
ever skipped, and the only cost is the work the pipeline was doing anyway.
Three tests here cover it - row order, column order, and a re-serialised
geometry - because those are the ways a publisher legitimately returns the same
data twice.

**Not sensitive enough is silent.** The pipeline serves stale data, reports
success, and there is nothing to notice. Everything from
`test_a_changed_value_changes_the_digest` down exists for that direction, and
it is why they enumerate types rather than spot-checking one: a column whose
change is invisible is invisible for as long as nobody looks.

Nothing here touches a database or Dagster - `frame_digest` is a pure function
of a frame.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point, Polygon

from hbu_dataplatform.digest import PROVENANCE_COLUMNS, frame_digest

SQUARE = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

#: The same square, re-serialised the way a publisher's own export would: a
#: different start vertex, and the ring wound the other way.
SQUARE_AGAIN = Polygon([(1, 1), (0, 1), (0, 0), (1, 0)][::-1])


def frame(ids=("a", "b"), values=(1.0, 2.0), geometries=None):
    return gpd.GeoDataFrame(
        {"id": list(ids), "v": list(values)},
        geometry=list(geometries) if geometries else [SQUARE] * len(ids),
        crs="EPSG:4326",
    )


# -- the same data, returned differently ---------------------------------------


def test_row_order_is_not_content():
    """ArcGIS does not promise an order, and a digest of the order is useless."""
    assert frame_digest(frame(("a", "b"), (1.0, 2.0))) == frame_digest(
        frame(("b", "a"), (2.0, 1.0))
    )


def test_column_order_is_not_content():
    one = frame()
    assert frame_digest(one) == frame_digest(one[["v", "id", "geometry"]])


def test_a_reserialised_geometry_is_not_a_change():
    """Same shape, different start vertex, opposite winding.

    Without `shapely.normalize` this is the rule that would fail, and it would
    fail in the safe direction - every re-export would look like a change and
    the feature would just never skip anything.
    """
    assert frame_digest(frame(geometries=[SQUARE, SQUARE])) == frame_digest(
        frame(geometries=[SQUARE_AGAIN, SQUARE_AGAIN])
    )


def test_provenance_is_not_content():
    """`scraped_at` and `loaded_at` change on every run by construction."""
    with_stamps = frame()
    for column in PROVENANCE_COLUMNS:
        with_stamps[column] = "2026-09-21T12:00:00"
    assert frame_digest(with_stamps) == frame_digest(frame())


# -- the silent direction ------------------------------------------------------


def test_a_duplicated_row_is_not_the_same_as_one_row():
    """The XOR trap, made a test.

    XOR-aggregating row digests is the tempting incremental-friendly choice and
    it cancels identical pairs - so a lot returned twice and a lot returned not
    at all would hash alike. Infolot really does return a duplicate when a
    borough outline is a multipolygon, which is why `warehouse._merge` carries
    a `DISTINCT ON`, so this is a real input rather than a hypothetical one.
    """
    assert frame_digest(frame(("a", "a"), (1.0, 1.0))) != frame_digest(
        frame(("a",), (1.0,))
    )


@pytest.mark.parametrize(
    "column, before, after",
    [
        ("text", "C01-001", "C01-002"),
        ("int", 3, 4),
        ("float", 1.0, 1.000_000_1),
        ("bool", True, False),
        ("null", None, 0),
    ],
)
def test_a_changed_value_changes_the_digest(column, before, after):
    """Per type, because "nothing reads that column" is how a blind spot starts."""
    one = pd.DataFrame({column: [before]})
    other = pd.DataFrame({column: [after]})
    assert frame_digest(one) != frame_digest(other)


def test_a_moved_vertex_changes_the_digest():
    """Coordinates are snapped, not ignored. A metre is a change."""
    moved = Polygon([(0, 0), (1, 0), (1, 1.000_01), (0, 1)])
    assert frame_digest(frame(geometries=[SQUARE, SQUARE])) != frame_digest(
        frame(geometries=[moved, SQUARE])
    )


def test_a_dropped_or_added_column_changes_the_digest():
    """The column names are hashed, so a layer that gains a field says so even
    before any row carries a value in it."""
    one = frame()
    widened = one.copy()
    widened["new"] = None
    assert frame_digest(one) != frame_digest(widened)
    assert frame_digest(one) != frame_digest(one.drop(columns=["v"]))


def test_a_row_that_disappeared_changes_the_digest():
    assert frame_digest(frame(("a", "b"), (1.0, 2.0))) != frame_digest(
        frame(("a",), (1.0,))
    )


def test_an_empty_frame_is_not_the_same_as_a_row():
    """A publisher that went to zero rows has changed, and this has to say so -
    the case where "no data" and "no change" would otherwise look alike."""
    empty = gpd.GeoDataFrame({"id": [], "v": []}, geometry=[], crs="EPSG:4326")
    assert frame_digest(empty) != frame_digest(frame(("a",), (1.0,)))
    assert frame_digest(empty) == frame_digest(empty.copy())


def test_null_does_not_collide_with_the_text_that_spells_it():
    """A source is entitled to publish the literal string "None"."""
    assert frame_digest(pd.DataFrame({"x": [None]})) != frame_digest(
        pd.DataFrame({"x": ["None"]})
    )
    assert frame_digest(pd.DataFrame({"x": [float("nan")]})) == frame_digest(
        pd.DataFrame({"x": [None]})
    )


def test_a_geometry_that_went_missing_changes_the_digest():
    assert frame_digest(frame(geometries=[SQUARE, SQUARE])) != frame_digest(
        frame(geometries=[SQUARE, None])
    )


# -- stability -----------------------------------------------------------------


def test_the_digest_is_stable_across_calls_and_copies():
    """It is compared against a value stored last month, so it has to depend on
    the content and on nothing else - not on identity, not on an index."""
    one = frame()
    reindexed = one.sample(frac=1.0, random_state=3).reset_index(drop=True)
    assert frame_digest(one) == frame_digest(one)
    assert frame_digest(one) == frame_digest(one.copy())
    assert frame_digest(one) == frame_digest(reindexed)


def test_minus_zero_is_zero():
    assert frame_digest(pd.DataFrame({"x": [0.0]})) == frame_digest(
        pd.DataFrame({"x": [-0.0]})
    )


def test_a_point_layer_digests_too():
    """`rag.features` holds points and lines as well as polygons."""
    points = gpd.GeoDataFrame(
        {"id": ["a"]}, geometry=[Point(-73.6, 45.5)], crs="EPSG:4326"
    )
    assert frame_digest(points) != frame_digest(
        gpd.GeoDataFrame({"id": ["a"]}, geometry=[Point(-73.7, 45.5)], crs="EPSG:4326")
    )
