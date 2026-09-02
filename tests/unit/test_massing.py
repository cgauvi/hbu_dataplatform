"""Offline tests for `urban_rag.massing` and the asset over it.

The fit is geometry and nothing is stubbed: every rectangle these draw is
tested for actually being inside the envelope it was fitted into, which is the
one property the whole module rests on. `test_every_fitted_rectangle_is_inside`
is that check applied across a spread of envelope shapes at once, and it is
the test to keep if all the others go.

The envelopes are hand-built rather than taken from a partition, because the
cases worth covering are shapes rather than boroughs: a square with room to
spare, a narrow Villeray lot, a sliver that holds no rectangle of its own
area, an L whose centroid is outside it, and a parcel the margins cut in two.
"""

from __future__ import annotations

import math

import geopandas as gpd
import pandas as pd
import pytest
from asset_helpers import materialization_metadata, stub_publish as stub_publish_into
from dagster import Failure, MultiPartitionKey, materialize
from shapely.affinity import rotate, translate
from shapely.geometry import MultiPolygon, Polygon, box

from urban_rag import massing, massing_assets
from urban_rag.frames import write_frame
from urban_rag.hbu_assets import LOT_HBU_FILE, lot_highest_best_use
from urban_rag.massing import MASSING_STATUSES, fit_rectangle
from urban_rag.massing_assets import LOT_MASSING_FILE, lot_building_massing
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.setback_assets import LOT_SETBACKS_FILE, lot_buildable_setbacks
from urban_rag.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"

#: A spread of buildable envelopes, each a shape worth covering rather than a
#: borough worth sampling. The areas are in square metres and the geometry is
#: in a projected CRS - `fit_rectangle` measures in metres and would fit
#: nothing at all in degrees.
ENVELOPES = {
    # Room to spare: a square is what should be drawn.
    "roomy square": box(0, 0, 30, 30),
    # The Villeray shape: narrow at the street, deep back from it.
    "narrow deep": box(0, 0, 9, 28),
    # Same area as `narrow deep` and no rectangle of it: 5 m will not take one.
    "sliver": box(0, 0, 5, 40),
    # Not axis-aligned, so the angle has to be read off the parcel.
    "rotated": rotate(box(0, 0, 11, 26), 37.0, origin="centroid"),
    # Concave, and its centroid falls outside it.
    "L shape": Polygon([(0, 0), (30, 0), (30, 10), (10, 10), (10, 30), (0, 30)]),
    # Margins that met in the middle and cut the parcel in two.
    "split": MultiPolygon([box(0, 0, 12, 12), box(0, 20, 16, 40)]),
}


# --------------------------------------------------------------------------
# the fit itself
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(ENVELOPES))
@pytest.mark.parametrize("share", [0.25, 0.5, 0.9, 1.0, 1.4])
def test_every_fitted_rectangle_is_inside(name, share):
    """The one property the module rests on, over shapes x sizes.

    A rectangle this module draws is always inside the envelope it was fitted
    into - which is what makes "respects the setbacks" true by construction
    rather than by a rule reimplemented here. Tested at sizes from a quarter of
    the envelope to well over it, so the shrink path is covered as well as the
    full-size one.
    """
    envelope = ENVELOPES[name]
    target = envelope.area * share
    result = fit_rectangle(envelope, target)
    if result.geometry is None:
        assert result.status in ("no_fit", "no_buildable_geometry")
        return
    # A hair of slack: the rectangle is built from floats and the envelope's
    # own edges are floats, so exact containment is not the right predicate.
    assert envelope.buffer(1e-9).contains(result.geometry)
    assert result.placed_footprint_m2 <= target + 1e-6
    assert result.geometry.area == pytest.approx(result.placed_footprint_m2, rel=1e-6)


def test_a_roomy_envelope_takes_the_squarest_ratio():
    """Ratios are tried squarest first, so a lot with room gets the square."""
    result = fit_rectangle(box(0, 0, 30, 30), 200.0)
    assert result.status == "fitted"
    assert result.aspect_ratio == 1.0
    assert result.width_m == pytest.approx(result.depth_m)


def test_a_narrow_envelope_takes_a_longer_one():
    """A 9 m frontage will not take a 14 m square, so the ratio goes up."""
    result = fit_rectangle(ENVELOPES["narrow deep"], 190.0)
    assert result.status == "fitted"
    assert result.aspect_ratio > 1.0
    assert min(result.width_m, result.depth_m) <= 9.0 + 1e-9


def test_a_sliver_reports_the_shortfall_rather_than_the_footprint():
    """The check this module exists for - see its docstring.

    5 m by 40 m is 200 m2 of buildable area and holds no rectangle of 200 m2.
    A solver capping on area alone spends all 200; this says how much of that
    the parcel can actually take.
    """
    result = fit_rectangle(ENVELOPES["sliver"], 200.0)
    assert result.status == "shrunk"
    assert result.placed_footprint_m2 < 200.0
    assert ENVELOPES["sliver"].buffer(1e-9).contains(result.geometry)


def test_a_concave_envelope_is_fitted_off_its_edges_not_its_centroid():
    """An L's centroid is outside it, and a rectangle centred there fits nothing."""
    envelope = ENVELOPES["L shape"]
    assert not envelope.contains(envelope.centroid)
    result = fit_rectangle(envelope, 200.0)
    assert result.status == "fitted"
    assert envelope.buffer(1e-9).contains(result.geometry)


def test_the_angle_comes_from_the_parcel():
    """A rotated parcel gets a rotated building, not an axis-aligned one."""
    result = fit_rectangle(ENVELOPES["rotated"], 220.0)
    assert result.geometry is not None
    # 37 degrees, or the perpendicular to it, modulo the 180 the angle is
    # reduced to. Either is the parcel's own grain.
    assert min(
        abs(result.rotation_deg - 37.0), abs(result.rotation_deg - 127.0)
    ) < 1.0


def test_a_split_parcel_is_fitted_in_its_larger_half():
    """Margins can cut a parcel in two; a building goes in one of the pieces."""
    result = fit_rectangle(ENVELOPES["split"], 250.0)
    assert result.geometry is not None
    bigger = max(ENVELOPES["split"].geoms, key=lambda part: part.area)
    assert bigger.buffer(1e-9).contains(result.geometry)


def test_a_building_with_room_to_spare_sits_centred():
    """Centred is both the stable answer and the one that reads on a map."""
    envelope = box(0, 0, 40, 40)
    result = fit_rectangle(envelope, 100.0)
    assert result.status == "fitted"
    assert result.geometry.centroid.x == pytest.approx(envelope.centroid.x, abs=0.5)
    assert result.geometry.centroid.y == pytest.approx(envelope.centroid.y, abs=0.5)


def test_a_tight_building_is_pushed_against_the_envelope():
    """The flush placements - what a walk-up on its front setback line does.

    A 10 m x 30 m envelope filled edge to edge: the only rectangle of 300 m2
    that fits is exactly 30 x 10, and the only position it fits in is centred
    at (5, 15). No grid of candidate centres lands on that by luck, so this
    passes only because `_Frame.flush_centres` computes it.
    """
    envelope = box(0, 0, 10, 30)
    result = fit_rectangle(envelope, 300.0)
    assert result.status == "fitted"
    assert envelope.buffer(1e-9).contains(result.geometry)
    assert min(result.width_m, result.depth_m) == pytest.approx(10.0, rel=1e-9)
    assert max(result.width_m, result.depth_m) == pytest.approx(30.0, rel=1e-9)


@pytest.mark.parametrize(
    "geometry,target,expected",
    [
        (None, 200.0, "no_buildable_geometry"),
        (Polygon(), 200.0, "no_buildable_geometry"),
        (box(0, 0, 2, 2), 200.0, "no_fit"),
        (box(0, 0, 30, 30), 0.0, "no_program"),
        (box(0, 0, 30, 30), float("nan"), "no_program"),
    ],
)
def test_nothing_drawn_says_why(geometry, target, expected):
    """Four ways to have no rectangle, and they are not the same answer."""
    result = fit_rectangle(geometry, target)
    assert result.status == expected
    assert result.status in MASSING_STATUSES
    assert result.geometry is None


def test_a_smaller_target_never_fits_worse():
    """Monotone in the target, which a bisection on the scale has to be."""
    envelope = ENVELOPES["narrow deep"]
    previous = 0.0
    for target in (50.0, 100.0, 150.0, 200.0, 300.0):
        result = fit_rectangle(envelope, target)
        assert result.placed_footprint_m2 >= previous - 1e-6
        previous = result.placed_footprint_m2


# --------------------------------------------------------------------------
# over a partition
# --------------------------------------------------------------------------


def hbu_row(lot_uid=1, **overrides):
    """One row of `lot_highest_best_use`, at the columns the massing reads."""
    return {
        "lot_uid": lot_uid,
        "lot_number": f"2 216 {lot_uid:03d}",
        "neighborhood": NEIGHBORHOOD,
        "scrape_date": DATE,
        "feature_id": "C01-001",
        "column_index": 0,
        "hbu_status": "solved",
        "lot_area_m2": 500.0,
        "primary_frontage_m": 20.0,
        "buildable_area_m2": 240.0,
        "footprint_m2": 200.0,
        "gross_floor_area_m2": 1000.0,
        "floors": 5,
        "height_m": 15.0,
        "residential_floors": 4,
        "commercial_floors": 1,
        "industrial_floors": 0,
        "above_grade_parking_floors": 0,
        "underground_levels": 1,
        "num_dwellings": 11,
        **overrides,
    }


def setback_row(geometry, lot_uid=1, **overrides):
    """One row of `lot_buildable_setbacks`, with its buildable polygon."""
    return {
        "lot_uid": lot_uid,
        "feature_id": "C01-001",
        "column_index": 0,
        "geometry": geometry,
        **overrides,
    }


#: A metric envelope placed near Villeray, so `to_crs` has something real to do
#: and the fit happens in the metres it has to happen in.
def near_villeray(shape):
    """``shape``, moved to where VSMPE actually is in EPSG:32188."""
    return translate(shape, xoff=299_000.0, yoff=5_046_000.0)


def envelopes_gdf(rows):
    return gpd.GeoDataFrame(
        [{k: v for k, v in row.items() if k != "geometry"} for row in rows],
        geometry=[row["geometry"] for row in rows],
        crs=massing.METRIC_CRS,
    ).to_crs("EPSG:4326")


def test_massing_frame_keeps_every_lot_and_says_why():
    """A lot with no program and one with no envelope are different answers."""
    hbu = pd.DataFrame(
        [
            hbu_row(1),
            hbu_row(2, hbu_status="no_governing_column", footprint_m2=None),
            hbu_row(3),
        ]
    )
    setbacks = envelopes_gdf(
        [
            setback_row(near_villeray(box(0, 0, 20, 20)), lot_uid=1),
            # lot 3 has a program and no envelope to fit it into.
        ]
    )
    frame = massing.massing_frame(hbu, setbacks)
    assert len(frame) == 3
    assert dict(zip(frame["lot_uid"], frame["massing_status"])) == {
        1: "fitted",
        2: "no_program",
        3: "no_buildable_geometry",
    }
    assert frame.crs == "EPSG:4326"
    assert frame.loc[frame["lot_uid"] == 1, "geometry"].notna().all()
    assert frame.loc[frame["lot_uid"] != 1, "geometry"].isna().all()


def test_massing_frame_reports_the_fit():
    hbu = pd.DataFrame([hbu_row(1, footprint_m2=200.0, floors=5)])
    setbacks = envelopes_gdf([setback_row(near_villeray(box(0, 0, 20, 20)))])
    row = massing.massing_frame(hbu, setbacks).iloc[0]
    assert row["massing_status"] == "fitted"
    assert row["placed_footprint_m2"] == pytest.approx(200.0)
    assert row["footprint_shortfall_m2"] == pytest.approx(0.0, abs=1e-6)
    assert row["footprint_fit_pct"] == pytest.approx(100.0)
    assert row["placed_gross_floor_area_m2"] == pytest.approx(1000.0)


def test_massing_frame_shrinks_a_footprint_the_shape_cannot_take():
    """The sliver, through the frame rather than the fitter."""
    hbu = pd.DataFrame([hbu_row(1, footprint_m2=200.0)])
    setbacks = envelopes_gdf([setback_row(near_villeray(box(0, 0, 5, 40)))])
    row = massing.massing_frame(hbu, setbacks).iloc[0]
    assert row["massing_status"] == "shrunk"
    assert row["footprint_fit_pct"] < 100.0
    assert row["footprint_shortfall_m2"] > 0.0


def test_a_lot_with_no_rectangle_has_no_shortfall():
    """NaN, not zero: an undrawn lot has no gap, it has no drawing."""
    hbu = pd.DataFrame([hbu_row(1)])
    row = massing.massing_frame(hbu, envelopes_gdf([])).iloc[0]
    assert row["massing_status"] == "no_buildable_geometry"
    assert pd.isna(row["footprint_shortfall_m2"])
    assert pd.isna(row["footprint_fit_pct"])


def test_the_rectangle_lands_where_the_lot_is():
    """A round trip through EPSG:4326 has to come back to the same ground."""
    envelope = near_villeray(box(0, 0, 20, 20))
    hbu = pd.DataFrame([hbu_row(1, footprint_m2=200.0)])
    frame = massing.massing_frame(hbu, envelopes_gdf([setback_row(envelope)]))
    drawn = frame.iloc[0]["geometry"]
    back = gpd.GeoSeries([drawn], crs="EPSG:4326").to_crs(massing.METRIC_CRS).iloc[0]
    assert envelope.buffer(1e-6).contains(back)
    assert back.area == pytest.approx(200.0, rel=1e-4)


# --------------------------------------------------------------------------
# the asset
# --------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture(autouse=True)
def stub_publish(monkeypatch):
    return stub_publish_into(monkeypatch, massing_assets)


def write_upstreams(store, *, hbu_rows=None, setback_rows=None, setbacks=True):
    write_frame(
        pd.DataFrame(hbu_rows or [hbu_row(1)]),
        join(
            store.partition_dir(lot_highest_best_use.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_HBU_FILE,
        ),
    )
    if setbacks:
        rows = setback_rows or [setback_row(near_villeray(box(0, 0, 20, 20)))]
        write_frame(
            envelopes_gdf(rows),
            join(
                store.partition_dir(
                    lot_buildable_setbacks.key.path[-1], DATE, NEIGHBORHOOD
                ),
                LOT_SETBACKS_FILE,
            ),
        )


def run(store, run_config=None):
    return materialize(
        [lot_building_massing],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[lot_building_massing],
        run_config=run_config,
    )


def read_output(store):
    return gpd.read_parquet(
        join(
            store.partition_dir(lot_building_massing.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_MASSING_FILE,
        )
    )


def test_asset_writes_a_map_ready_polygon(store, stub_publish):
    write_upstreams(store)
    result = run(store)
    assert result.success

    frame = read_output(store)
    assert len(frame) == 1
    assert frame.crs == "EPSG:4326"
    row = frame.iloc[0]
    assert row["massing_status"] == "fitted"
    assert row["geometry"] is not None
    assert row["footprint_fit_pct"] == pytest.approx(100.0)
    # Villeray, roughly: the polygon has to land on the island rather than at
    # the origin, which is what a CRS mistake looks like from here.
    assert -74.5 < row["geometry"].centroid.x < -73.0
    assert 45.0 < row["geometry"].centroid.y < 46.0

    assert stub_publish["datasets"].keys() == {"lot_building_massing"}
    metadata = materialization_metadata(result, lot_building_massing)
    assert metadata["num_lots"].value == 1
    assert metadata["num_drawn"].value == 1
    assert metadata["num_fitted"].value == 1


def test_asset_keeps_undrawn_lots_in_the_tree(store):
    """The tree keeps every lot; the count of what reaches Postgres says so."""
    write_upstreams(
        store,
        hbu_rows=[hbu_row(1), hbu_row(2)],
        setback_rows=[setback_row(near_villeray(box(0, 0, 20, 20)), lot_uid=1)],
    )
    result = run(store)
    assert result.success
    frame = read_output(store)
    assert len(frame) == 2
    assert int(frame["geometry"].notna().sum()) == 1

    metadata = materialization_metadata(result, lot_building_massing)
    assert metadata["num_lots"].value == 2
    assert metadata["num_drawn"].value == 1
    assert metadata["num_not_drawn"].value == 1
    assert metadata["num_no_buildable_geometry"].value == 1


def test_asset_without_setbacks_draws_nothing_rather_than_ignoring_margins(store):
    """A rectangle on a parcel with the margins ignored would look plausible."""
    write_upstreams(store, setbacks=False)
    result = run(store)
    assert result.success
    frame = read_output(store)
    assert list(frame["massing_status"]) == ["no_buildable_geometry"]
    assert frame["geometry"].isna().all()


def test_asset_config_changes_the_rectangle(store):
    """The aspect ratios are config, and a different list draws differently.

    The square fits the 20 m x 20 m envelope at full footprint; 3:1 does not -
    180 m2 at 3:1 is 23.2 m long and the envelope is 20 - so forcing that ratio
    shrinks the building. Both halves of that are the point: the config reaches
    the fit, and a ratio the parcel cannot take is reported rather than
    quietly replaced with one it can.
    """
    write_upstreams(
        store,
        hbu_rows=[hbu_row(1, footprint_m2=180.0)],
        setback_rows=[setback_row(near_villeray(box(0, 0, 20, 20)))],
    )
    run(store)
    square = read_output(store).iloc[0]
    assert square["aspect_ratio"] == 1.0
    assert square["massing_status"] == "fitted"
    assert square["width_m"] == pytest.approx(math.sqrt(180.0), rel=1e-6)

    run(
        store,
        run_config={
            "ops": {
                "gold__lot_building_massing": {"config": {"aspect_ratios": [3.0]}}
            }
        },
    )
    long = read_output(store).iloc[0]
    assert long["aspect_ratio"] == 3.0
    # Shrunk, so the sides are not sqrt(180 x 3) - but they still stand in the
    # ratio that was asked for, which is what the config controls.
    assert long["massing_status"] == "shrunk"
    assert long["width_m"] / long["depth_m"] == pytest.approx(3.0, rel=1e-6)
    assert long["placed_footprint_m2"] < 180.0


def test_asset_fails_naming_a_missing_upstream(store):
    with pytest.raises(Failure, match="materialize lot_highest_best_use"):
        run(store)
