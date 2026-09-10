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
from urban_rag import program as program_module
from urban_rag import postgis
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


def test_a_road_parcel_is_never_drawn():
    """The parcel that *is* avenue Querbes, with everything a build needs.

    A street lot is a clean rectangle of a few thousand square metres with a
    buildable envelope inside it, so nothing about its geometry stops a
    footprint being placed on it - the only thing that does is `hbu_status`.
    This pins that the massing reads the status and not the numbers beside it,
    because a road parcel drawn on a map is the one failure that looks entirely
    plausible: a mid-rise standing in the middle of the roadway.
    """
    hbu = pd.DataFrame(
        [
            hbu_row(1),
            hbu_row(
                2,
                lot_number="2 249 179",
                hbu_status="road_parcel",
                # Not nulled out, deliberately: the assertion is about the
                # status winning over a full set of numbers.
                footprint_m2=200.0,
            ),
        ]
    )
    setbacks = envelopes_gdf(
        [
            setback_row(near_villeray(box(0, 0, 20, 20)), lot_uid=1),
            setback_row(near_villeray(box(40, 0, 60, 20)), lot_uid=2),
        ]
    )
    frame = massing.massing_frame(hbu, setbacks).set_index("lot_uid")
    assert frame.loc[1, "massing_status"] == "fitted"
    assert frame.loc[1, "geometry"] is not None
    # It keeps its row and says why, the way every unbuilt lot here does.
    assert frame.loc[2, "massing_status"] == "no_program"
    assert frame.loc[2, "geometry"] is None


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

    assert stub_publish["datasets"].keys() == {
        "lot_building_massing",
        "lot_surface_parking",
    }
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


# --------------------------------------------------------------------------
# surface parking
# --------------------------------------------------------------------------
#
# The building's fit is checked against the *envelope*; the parking's is
# checked against the *parcel*, because a setback is a margin a building keeps
# and a car in a side yard stands exactly where the margin said no building
# may go. These are hand-built parcels for the same reason the envelopes above
# are: what matters is the shape, not the borough.

#: One surface stall's ground allowance, the figure the solver charges the
#: yard. Read off `program` rather than restated, the way `massing` reads it.
STALL_M2 = program_module.SURFACE_STALL_AREA_SQFT * program_module.M2_PER_SQFT

PARCELS = {
    # An ordinary Villeray lot. Parks comfortably.
    "villeray": box(0, 0, 10, 30),
    # The whole point: the same 300 m2, two metres wide. The area constraint
    # upstream is as satisfied here as it is above, and no car can stand on it
    # in any orientation - a stall is 2.6 m across even parked parallel.
    "ribbon": box(0, 0, 2, 150),
    # And its counterpart, which is why the reject is on *both* dimensions: a
    # four-metre strip is a driveway. It takes no car nose-in and it takes a
    # row of them parallel to its own length, so it is not unparkable and
    # rejecting it would sell a borough of them parkades they do not need.
    "driveway": box(0, 0, 4, 75),
    # Exactly one stall deep, which is the boundary the by-law dimension sets.
    "one stall deep": box(0, 0, massing.MIN_PARKING_DEPTH_M, 60),
    # A hair under it, which is the other side of the same boundary.
    "just too shallow": box(0, 0, massing.MIN_PARKING_DEPTH_M - 0.2, 60),
    # Not axis-aligned: the depth has to be measured on the parcel's own grain.
    "rotated": rotate(box(0, 0, 12, 34), 37.0, origin="centroid"),
    # Two lobes joined by a neck too thin to park in.
    "dumbbell": MultiPolygon([box(0, 0, 14, 14), box(0, 24, 14, 40)]),
}


@pytest.mark.parametrize("name", sorted(PARCELS))
def test_parking_capacity_never_exceeds_the_parcel(name):
    """The bound is a bound: it can never claim ground the parcel has not got."""
    parcel = PARCELS[name]
    assert 0.0 <= massing.parking_capacity_m2(parcel) <= parcel.area + 1e-6


def test_a_ribbon_holds_no_parking_whatever_its_area():
    """The failure the whole thing exists for, in one assertion.

    300 m2 either way. The area constraint in `solve_program` cannot tell these
    two parcels apart and would let both park ten cars.
    """
    assert massing.parking_capacity_m2(PARCELS["villeray"]) > 250.0
    assert massing.parking_capacity_m2(PARCELS["ribbon"]) == 0.0
    assert PARCELS["villeray"].area == pytest.approx(PARCELS["ribbon"].area)


def test_the_depth_boundary_is_one_stall():
    """5.5 m clear is the dimension, and the boundary case is inclusive.

    A yard exactly one stall across erodes to a zero-width line at the bare
    radius, so the opening is inset by `_FIT_EPSILON_M` - the by-law asks for
    5.5 m of ground, not for more than 5.5 m of it.
    """
    assert massing.parking_capacity_m2(PARCELS["one stall deep"]) > 0.0
    assert massing.parking_capacity_m2(PARCELS["just too shallow"]) == 0.0
    assert massing.parking_capacity_m2(box(0, 0, 5.2, 5.2)) == 0.0
    assert massing.parking_capacity_m2(box(0, 0, 2.4, 90.0)) == 0.0


def test_a_narrow_strip_is_not_parkable():
    """The strict reading, and the expensive half of it.

    This test used to assert the opposite: that a 4 m strip is a driveway, that
    a car parks down it in single file, and that rejecting it would sell a
    borough of side yards parkades they do not need. That was the anisotropic
    rule - a stall's length one way and its width the other - and it went with
    the rectangle search, which could orient a bay along the strip.

    What replaced it is a morphological opening, which has no orientation to
    give: ground is parkable where 5.5 m is clear *in every direction*, so a
    strip narrower than that parks nothing at any length. That is a deliberate
    choice of the strict reading over the loose one and it is not free - it is
    the narrow-lot tail of the borough, and those programs now dig, deck or bay
    their stalls. `parkable_ground` states the trade and `MIN_PARKING_DEPTH_M`
    is the one place to reverse it: set it to 2.6 and the driveway comes back.
    """
    assert massing.parking_capacity_m2(PARCELS["driveway"]) == 0.0
    assert massing.fit_parking(PARCELS["driveway"], 3 * STALL_M2).status == "no_fit"

    loose = massing.parkable_ground(PARCELS["driveway"], min_depth_m=2.6)
    assert loose.area == pytest.approx(PARCELS["driveway"].area, rel=1e-6)


def test_a_rectangular_parcel_is_its_own_capacity():
    """A rectangle's largest inscribed rectangle is the rectangle.

    Exact rather than searched - see `_RECTANGULAR_TOLERANCE`. The rotated
    parcel is here because the shortcut has to read the parcel's own grain
    rather than the axes it happens to be stored on.
    """
    for name in ("villeray", "rotated"):
        parcel = PARCELS[name]
        assert massing.parking_capacity_m2(parcel) == pytest.approx(
            parcel.area, rel=1e-9
        )


def test_capacity_counts_every_lobe_now():
    """The approximation the rectangle search made, and no longer makes.

    A dumbbell holds a 196 m2 lobe and a 224 m2 one. The old bound measured a
    single rectangle and reported the parcel at its better lobe - documented,
    but wrong by 47 pct on a shape a builder would pave both halves of. The
    opening keeps both, because it is not looking for a rectangle at all.
    """
    parcel = PARCELS["dumbbell"]
    capacity = massing.parking_capacity_m2(parcel)
    assert parcel.area == pytest.approx(196.0 + 224.0)
    assert capacity == pytest.approx(parcel.area, rel=1e-6)


# ---- the yard is a band, not a rectangle ---------------------------------
#
# The shape a real Montreal yard has is a band wrapping the building, and the
# three-rectangle search read one at a little over half its area. These pin
# down what replaced it: an opening for what may be paved, and a distance band
# for which part of it is.


def test_a_ring_yard_is_parkable_all_the_way_round():
    """The failure the rewrite exists for, in one assertion.

    A plate in the middle of a parcel leaves a ring. No rectangle covers a
    ring, so the old search read this yard at a fraction of itself; the opening
    keeps the whole of it, because it is not looking for a rectangle.
    """
    lot, plate = box(0, 0, 60, 60), box(20, 20, 40, 40)
    yard = massing.yard_of(lot, plate)
    assert yard.area == pytest.approx(3200.0)
    assert massing.parking_capacity_m2(yard) == pytest.approx(3200.0, rel=1e-6)


def test_the_paving_is_one_piece_and_hugs_the_building():
    """Contiguous and near the plate - the two properties that make it a plan."""
    lot, plate = box(0, 0, 60, 60), box(20, 20, 40, 40)
    yard = massing.yard_of(lot, plate)
    parked = massing.fit_parking(yard, 1000.0, building=plate)

    assert parked.status == "fitted"
    assert parked.area_m2 == pytest.approx(1000.0, abs=1.0)
    assert parked.num_bays == 1
    # Every bit of it is within the band's own depth of the building, which is
    # what "hugs" means and what a Hilbert prefix would not give.
    assert plate.buffer(parked.depth_m + 1e-6).contains(parked.geometry)
    # And it is inside the yard, so nothing was paved under the building or
    # over the lot line.
    assert yard.buffer(1e-6).contains(parked.geometry)


def test_the_band_grows_with_the_program():
    """More stalls reach further out, and never further than they need to."""
    lot, plate = box(0, 0, 60, 60), box(20, 20, 40, 40)
    yard = massing.yard_of(lot, plate)
    depths = [
        massing.fit_parking(yard, area, building=plate).depth_m
        for area in (400.0, 1200.0, 2400.0)
    ]
    assert depths == sorted(depths)
    assert depths[0] < depths[-1]


def test_the_area_is_met_exactly_when_the_ground_is_there():
    """The 1-1 property: the polygon drawn is the area the solver charged for.

    A rectangle search could only land on the areas its ratio ladder allowed;
    a bisected band lands on the number itself, which is what makes
    `surface_parking_fit_pct` a check rather than a running discount.
    """
    lot, plate = box(0, 0, 60, 60), box(20, 20, 40, 40)
    yard = massing.yard_of(lot, plate)
    for area in (27.87, 100.0, 733.0, 1500.0, 2999.0):
        parked = massing.fit_parking(yard, area, building=plate)
        assert parked.status == "fitted"
        assert parked.area_m2 == pytest.approx(area, abs=1.0)


def test_a_yard_too_small_is_paved_whole_and_reported_short():
    """`shrunk` is the honest answer, and it is what the fit pct reports."""
    # An 8 m ring: wide enough to park in, nowhere near 5 000 m2 of it.
    lot, plate = box(0, 0, 40, 40), box(8, 8, 32, 32)
    yard = massing.yard_of(lot, plate)
    parked = massing.fit_parking(yard, 5_000.0, building=plate)
    assert parked.status == "shrunk"
    assert parked.area_m2 < 5_000.0
    assert parked.area_m2 <= yard.area + 1e-6


def test_the_second_lobe_is_reached_only_after_the_first_is_full():
    """Order matters, and the near ground goes first.

    A plate against one end of a dumbbell parcel: a small program parks beside
    it in one piece, and only a program too big for that lobe crosses to the
    other.
    """
    parcel = MultiPolygon([box(0, 0, 20, 20), box(0, 40, 20, 60)])
    plate = box(2, 2, 12, 12)
    yard = massing.yard_of(parcel, plate)

    near = massing.fit_parking(yard, 150.0, building=plate)
    assert near.num_bays == 1
    assert near.geometry.bounds[3] <= 25.0  # never left the first lobe

    far = massing.fit_parking(yard, 500.0, building=plate)
    assert far.num_bays == 2
    assert far.geometry.bounds[3] > 40.0


def test_more_pieces_than_max_bays_are_dropped_not_counted():
    """A program parked across five scraps is not the one the solver priced."""
    parcel = MultiPolygon(
        [box(0, 0, 20, 20)] + [box(30 * i, 40, 30 * i + 12, 52) for i in range(4)]
    )
    plate = box(2, 2, 12, 12)
    yard = massing.yard_of(parcel, plate)
    parked = massing.fit_parking(yard, yard.area, building=plate, max_bays=2)
    assert parked.num_bays <= 2
    assert parked.status == "shrunk"


def test_without_a_building_the_band_grows_from_the_middle():
    """The documented fallback - a blob, not a plan, and still one piece."""
    parked = massing.fit_parking(box(0, 0, 60, 60), 900.0)
    assert parked.status == "fitted"
    assert parked.area_m2 == pytest.approx(900.0, abs=1.0)
    assert parked.num_bays == 1


def test_a_band_has_no_width_or_angle():
    """Null rather than 0.0, because a band has no such dimension - see `Parking`."""
    lot, plate = box(0, 0, 60, 60), box(20, 20, 40, 40)
    parked = massing.fit_parking(massing.yard_of(lot, plate), 900.0, building=plate)
    assert parked.width_m is None
    assert parked.rotation_deg is None
    assert parked.depth_m > 0.0


def test_no_parcel_no_capacity():
    assert massing.parking_capacity_m2(None) == 0.0
    assert massing.parking_capacity_m2(Polygon()) == 0.0


# ---- and the same question asked of the building --------------------------
#
# `placeable_area_m2` is `parking_capacity_m2`'s counterpart for the plate, and
# `solve_program` takes it as the cap that makes the footprint it prices one
# this module can actually draw.


@pytest.mark.parametrize("name", list(ENVELOPES))
def test_placeable_area_never_exceeds_the_envelope(name):
    """A bound has to be a bound before it is anything else."""
    envelope = ENVELOPES[name]
    assert massing.placeable_area_m2(envelope) <= envelope.area + 1e-9


def test_a_rectangular_envelope_on_the_ratio_ladder_places_whole():
    """A square holds itself, so the cap costs a regular parcel nothing."""
    envelope = ENVELOPES["roomy square"]
    assert massing.placeable_area_m2(envelope) == pytest.approx(
        envelope.area, rel=1e-6
    )


@pytest.mark.parametrize("name", ["narrow deep", "rotated"])
def test_a_rectangle_off_the_ratio_ladder_places_a_little_under(name):
    """And the shortfall is `DEFAULT_ASPECT_RATIOS`, not the search.

    9 x 28 is a ratio of 3.11 and 11 x 26 of 2.36; neither is a rung, so
    neither envelope places the whole of itself even though it is a rectangle.
    Unlike `parking_capacity_m2`, which has `_RECTANGULAR_TOLERANCE` to settle
    exactly this case, this number has no shortcut - and must not grow one.
    It is the area `fit_rectangle` will place, and a cap the placer cannot draw
    to would hand the massing a plate to shrink, which is the whole thing it
    exists to stop.
    """
    envelope = ENVELOPES[name]
    placed = massing.placeable_area_m2(envelope)
    assert 0.8 * envelope.area < placed < envelope.area


def test_an_awkward_envelope_places_far_less_than_its_area():
    """The whole reason the column exists.

    An L of 500 m2 holds no rectangle anywhere near 500, and it is exactly this
    gap that `solve_program` was pricing before it had this number: the area
    caps allow the 500, no building of it fits, and the massing shrank the
    plate afterwards with the economics already computed on the full one.
    """
    envelope = ENVELOPES["L shape"]
    placed = massing.placeable_area_m2(envelope)
    assert envelope.area == pytest.approx(500.0)
    assert placed < 0.75 * envelope.area


def test_placeable_area_agrees_with_what_the_placer_draws():
    """The point of measuring it with the same search at the same settings.

    A footprint capped here has to be one `fit_rectangle` accepts whole later,
    or the cap has bought nothing: the solve would still be handing the massing
    a plate to shrink.
    """
    for name in ENVELOPES:
        envelope = ENVELOPES[name]
        placed = massing.placeable_area_m2(envelope)
        if placed < massing.MIN_FOOTPRINT_M2:
            continue
        assert fit_rectangle(envelope, placed).status == "fitted"


def test_placeable_area_measures_one_building():
    """A split envelope is reported at its better half, not at both.

    The same approximation `parking_capacity_m2` makes, and a firmer one: a
    building is one massing or it is nothing, while parking honestly comes in
    patches and gets `PARKING_MAX_BAYS` of them.
    """
    envelope = ENVELOPES["split"]
    placed = massing.placeable_area_m2(envelope)
    assert envelope.area == pytest.approx(144.0 + 320.0)
    # Inside the better lobe, not across both - and short of even that one,
    # because 16 x 20 is a ratio of 1.25 and `DEFAULT_ASPECT_RATIOS` does not
    # carry it. That is the placer's own ladder rather than a second
    # approximation: a cap this module could not draw to would buy nothing.
    assert 144.0 < placed <= 320.0 + 1e-9


def test_no_envelope_no_placeable_area():
    assert massing.placeable_area_m2(None) == 0.0
    assert massing.placeable_area_m2(Polygon()) == 0.0


# ---- drawing it -----------------------------------------------------------


def building_on(parcel, footprint_m2):
    """A plausible building on ``parcel``, for a yard to be measured against."""
    return massing.fit_rectangle(parcel.buffer(-1.5), footprint_m2)


@pytest.mark.parametrize("name", ["villeray", "rotated", "dumbbell"])
def test_every_drawn_bay_is_on_the_lot_and_clear_of_the_building(name):
    """The one property the parking drawing rests on, across parcel shapes.

    Two halves and both matter: the asphalt is inside the parcel boundary, and
    it does not run under the building. The second is what makes it a *second*
    polygon rather than a bigger first one.
    """
    parcel = PARCELS[name]
    massed = building_on(parcel, parcel.area * 0.35)
    yard = massing.yard_of(parcel, massed.geometry)
    parked = massing.fit_parking(yard, 4 * STALL_M2)
    assert parked.geometry is not None, parked.status
    # Buffered in by a micron, the way the fit itself is inset: a rectangle
    # sitting exactly on the boundary is inside it, and floating point is not
    # asked to agree to the last bit.
    assert parcel.contains(parked.geometry.buffer(-1e-6))
    assert not parked.geometry.intersects(massed.geometry.buffer(-1e-6))


def test_the_parking_is_never_folded_into_the_massing():
    """A surface stall is not a building, and the two shapes never merge."""
    parcel = PARCELS["villeray"]
    massed = building_on(parcel, 120.0)
    parked = massing.fit_parking(massing.yard_of(parcel, massed.geometry), 4 * STALL_M2)
    assert massed.geometry.intersection(parked.geometry).area == pytest.approx(0.0)
    # The massing is exactly the footprint it was costed at, with no yard in it.
    assert massed.geometry.area == pytest.approx(massed.placed_footprint_m2, rel=1e-9)


def test_parking_takes_more_than_one_bay_when_the_yard_is_split():
    """A building across the middle leaves a front yard and a rear yard.

    Both get stalls, because both would. A single rectangle would report this
    parcel at half its real capacity, which is the difference between a sanity
    check and a false alarm.
    """
    parcel = box(0, 0, 10, 30)
    building = box(0, 10, 10, 20)  # a band straight across, 100 m2
    parked = massing.fit_parking(massing.yard_of(parcel, building), 6 * STALL_M2)
    assert parked.num_bays >= 2
    assert parked.area_m2 > 100.0
    assert not parked.geometry.intersects(building.buffer(-1e-6))


def test_one_bay_is_a_polygon_and_several_are_a_multipolygon():
    parcel = box(0, 0, 20, 40)
    single = massing.fit_parking(parcel, 2 * STALL_M2)
    assert single.num_bays == 1
    assert isinstance(single.geometry, Polygon)

    split = massing.fit_parking(
        massing.yard_of(parcel, box(0, 15, 20, 25)), 20 * STALL_M2
    )
    assert split.num_bays >= 2
    assert isinstance(split.geometry, MultiPolygon)


def test_asking_for_more_parking_never_places_less():
    """The placed area must depend on the ground, not on the size of the ask.

    It did not: the width bisection ran up to `target / depth`, so eight
    halvings of a large ceiling resolved coarsely and a lot asked for forty
    stalls was told it held less ground than the same lot asked for four. The
    ceiling is bounded by the parcel now, and this is what says so.
    """
    yard = massing.yard_of(box(0, 0, 10, 30), box(1, 6, 9, 24))
    placed = [
        massing.fit_parking(yard, stalls * STALL_M2).area_m2
        for stalls in (1, 2, 4, 10, 40)
    ]
    assert placed == sorted(placed), placed


def test_a_yard_that_cannot_take_it_all_is_shrunk_rather_than_dropped():
    """The same posture the footprint takes, for the same reason."""
    yard = massing.yard_of(PARCELS["villeray"], box(1, 6, 9, 24))
    parked = massing.fit_parking(yard, 40 * STALL_M2)
    assert parked.status == "shrunk"
    assert parked.geometry is not None
    assert 0.0 < parked.area_m2 < 40 * STALL_M2


def test_a_yard_that_can_take_it_all_is_fitted():
    parked = massing.fit_parking(box(0, 0, 30, 30), 4 * STALL_M2)
    assert parked.status == "fitted"
    assert parked.area_m2 == pytest.approx(4 * STALL_M2)
    assert parked.depth_m >= massing.MIN_PARKING_DEPTH_M


def test_every_drawn_piece_holds_a_stall_at_any_angle():
    """The by-law dimension, checked on the shape rather than trusted.

    Checked as the opening itself defines it - a disc of half a stall's depth
    fits somewhere inside every piece drawn - rather than against the sides of
    a bounding rectangle, which is what the measurement used to be and is not
    a statement about a band.
    """
    radius = massing.MIN_PARKING_DEPTH_M / 2.0 - 1e-6
    for stalls in (1, 3, 8, 25):
        parked = massing.fit_parking(box(0, 0, 18, 40), stalls * STALL_M2)
        assert parked.geometry is not None
        for piece in getattr(parked.geometry, "geoms", [parked.geometry]):
            assert piece.buffer(-radius).area > 0.0


@pytest.mark.parametrize(
    "yard, target, expected",
    [
        (box(0, 0, 30, 30), 0.0, "no_parking"),
        (box(0, 0, 30, 30), float("nan"), "no_parking"),
        (None, 4 * STALL_M2, "no_yard"),
        (Polygon(), 4 * STALL_M2, "no_yard"),
        # A yard with real area and no room for a car anywhere in it.
        (box(0, 0, 1.0, 200.0), 4 * STALL_M2, "no_fit"),
    ],
)
def test_nothing_parked_says_why(yard, target, expected):
    parked = massing.fit_parking(yard, target)
    assert parked.status == expected
    assert parked.geometry is None
    assert parked.num_bays == 0


# ---- the two polygons, through the asset ----------------------------------


def parcel_row(geometry, lot_uid=1, feature_id="C01-001"):
    """One piece, as `fetch_zone_piece_polygons` hands it back.

    The ground a zone governs rather than the whole parcel - which is what the
    asset now measures a yard inside, since a lot two zones cut in two draws
    two buildings and each may only pave its own half. `feature_id` defaults to
    the zone `hbu_row` uses, so a test that says nothing about zoning gets one
    piece covering its lot, which is the unsplit case and what these tests were
    written against.
    """
    return {
        "lot_uid": lot_uid,
        "feature_id": feature_id,
        "lot_number": f"2 216 {lot_uid:03d}",
        "neighborhood": NEIGHBORHOOD,
        "scrape_date": DATE,
        "piece_area_m2": geometry.area,
        "num_lot_zones": 1,
        "geometry": geometry,
    }


def parcels_gdf(rows):
    return gpd.GeoDataFrame(
        [{k: v for k, v in row.items() if k != "geometry"} for row in rows],
        geometry=[row["geometry"] for row in rows],
        crs=massing.METRIC_CRS,
    ).to_crs("EPSG:4326")


def set_parcels(rows):
    """What the stubbed `rag.lots` read returns for the rest of this test."""
    _PARCELS_FOR_RUN[:] = list(rows)


_PARCELS_FOR_RUN: list = []


@pytest.fixture(autouse=True)
def stub_lots(monkeypatch):
    """Patch out the geometry read this asset makes for the yard.

    `stub_publish` already replaces `PostgisResource.connect` with something
    that yields a bare object, so the read has to be stubbed too rather than
    left to fail on it. Empty by default, which is the honest default: a test
    that says nothing about the ground gets `no_lot_geometry` and no parking,
    and the building half of the asset is unaffected - which is exactly the
    behaviour a partition with no cadastre loaded should have.

    **Both reads are stubbed.** The asset asks `silver.lot_zone_pieces` first
    - the ground each zone governs, which is what a surface stall may stand on
    - and falls back to `rag.lots` only where that table is not there. Leaving
    the second stubbed alone would make every test here exercise the fallback.
    """
    _PARCELS_FOR_RUN.clear()

    empty = gpd.GeoDataFrame(
        {"lot_uid": [], "feature_id": [], "lot_number": [], "piece_area_m2": []},
        geometry=[],
        crs="EPSG:4326",
    )

    def fetch_zone_piece_polygons(connection, *, neighborhood, scrape_date):
        if not _PARCELS_FOR_RUN:
            return empty
        return parcels_gdf(_PARCELS_FOR_RUN)

    def fetch_lot_polygons(connection, *, neighborhood, scrape_date):
        return empty

    monkeypatch.setattr(postgis, "fetch_lot_polygons", fetch_lot_polygons)
    monkeypatch.setattr(
        postgis, "fetch_zone_piece_polygons", fetch_zone_piece_polygons
    )
    return _PARCELS_FOR_RUN


def test_asset_draws_the_parking_as_a_second_polygon(store):
    """Two shapes, two columns, and the parking never inside the massing."""
    set_parcels([parcel_row(near_villeray(box(0, 0, 24, 40)))])
    write_upstreams(
        store,
        hbu_rows=[
            hbu_row(
                1,
                footprint_m2=200.0,
                lot_area_m2=960.0,
                surface_stalls=4,
                surface_area_m2=4 * STALL_M2,
            )
        ],
        setback_rows=[setback_row(near_villeray(box(2, 3, 22, 37)))],
    )
    run(store)
    row = read_output(store).iloc[0]

    assert row["massing_status"] == "fitted"
    assert row["parking_status"] == "fitted"
    assert row["geometry"] is not None
    assert row["parking_geometry"] is not None
    # The building is the footprint it was costed at and nothing else, and the
    # asphalt is somewhere else entirely.
    assert not row["geometry"].intersects(row["parking_geometry"].buffer(-1e-9))
    assert row["surface_parking_fit_pct"] == pytest.approx(100.0, abs=1.0)
    assert row["placed_surface_stalls"] == 4


def test_asset_publishes_the_massing_and_the_parking_apart(store, stub_publish):
    """One transaction, two tables, one geometry each."""
    set_parcels([parcel_row(near_villeray(box(0, 0, 24, 40)))])
    write_upstreams(
        store,
        hbu_rows=[
            hbu_row(1, surface_stalls=4, surface_area_m2=4 * STALL_M2, lot_area_m2=960.0)
        ],
        setback_rows=[setback_row(near_villeray(box(2, 3, 22, 37)))],
    )
    run(store)

    published = stub_publish["datasets"]
    assert set(published) == {"lot_building_massing", "lot_surface_parking"}
    massing_frame_out = published["lot_building_massing"]
    parking_frame_out = published["lot_surface_parking"]
    # Each frame carries exactly the shape its table is for, and not the other.
    assert "parking_geometry" not in massing_frame_out.columns
    assert massing_frame_out.geometry.name == "geometry"
    assert parking_frame_out.geometry.name == "geometry"
    assert parking_frame_out.geometry.iloc[0] is not None
    # The building polygon is gone from the parking frame rather than riding
    # along into the jsonb catch-all.
    assert list(parking_frame_out.columns).count("geometry") == 1
    assert not parking_frame_out.geometry.iloc[0].intersects(
        massing_frame_out.geometry.iloc[0].buffer(-1e-9)
    )


def test_asset_without_the_cadastre_still_draws_the_buildings(store):
    """No parcels loaded is `no_lot_geometry`, not a failed partition.

    And emphatically not a fallback to the setback envelope: parking drawn
    inside the margins is parking in the one place it is least likely to be,
    and it would look entirely plausible on a map.
    """
    write_upstreams(
        store,
        hbu_rows=[hbu_row(1, surface_stalls=4, surface_area_m2=4 * STALL_M2)],
    )
    run(store)
    row = read_output(store).iloc[0]
    assert row["massing_status"] == "fitted"
    assert row["parking_status"] == "no_lot_geometry"
    assert row["parking_geometry"] is None
    assert row["geometry"] is not None


def test_asset_reports_no_parking_where_the_program_parks_elsewhere(store):
    """A program that dug or decked has nothing to draw and has not failed."""
    set_parcels([parcel_row(near_villeray(box(0, 0, 24, 40)))])
    write_upstreams(
        store,
        hbu_rows=[hbu_row(1, surface_stalls=0, surface_area_m2=0.0)],
        setback_rows=[setback_row(near_villeray(box(2, 3, 22, 37)))],
    )
    run(store)
    row = read_output(store).iloc[0]
    assert row["parking_status"] == "no_parking"
    assert row["parking_geometry"] is None
    # No shortfall either: a program that parks underground is not a lot whose
    # yard came up short, and putting the two in one bucket would make the fit
    # column unreadable.
    assert pd.isna(row["surface_parking_fit_pct"])


def test_asset_reports_a_yard_that_cannot_take_the_stalls(store):
    """The whole point, end to end: same area, wrong shape.

    A parcel of 300 m2 that is 2.4 m across - under the 2.6 m a stall needs
    even parked parallel, so no car stands on it in any orientation.
    `solve_program` would have been stopped by `parkable_area_m2` upstream, but
    a partition solved before that existed still lands here, and this is what
    the massing says about it.
    """
    set_parcels([parcel_row(near_villeray(box(0, 0, 2.4, 125)))])
    write_upstreams(
        store,
        hbu_rows=[
            hbu_row(
                1,
                lot_area_m2=300.0,
                footprint_m2=60.0,
                surface_stalls=4,
                surface_area_m2=4 * STALL_M2,
            )
        ],
        setback_rows=[setback_row(near_villeray(box(0.2, 2, 2.2, 123)))],
    )
    run(store)
    row = read_output(store).iloc[0]
    assert row["parking_status"] == "no_fit"
    assert row["parking_geometry"] is None
    assert row["surface_parking_fit_pct"] == pytest.approx(0.0)
    assert row["surface_parking_shortfall_m2"] == pytest.approx(4 * STALL_M2)


def test_asset_metadata_counts_the_parking(store):
    set_parcels([parcel_row(near_villeray(box(0, 0, 24, 40)))])
    write_upstreams(
        store,
        hbu_rows=[
            hbu_row(1, surface_stalls=4, surface_area_m2=4 * STALL_M2, lot_area_m2=960.0)
        ],
        setback_rows=[setback_row(near_villeray(box(2, 3, 22, 37)))],
    )
    metadata = materialization_metadata(run(store), lot_building_massing)
    assert metadata["num_parked"].value == 1
    assert metadata["num_parking_fitted"].value == 1
    assert metadata["solved_surface_stalls"].value == 4
    assert metadata["placed_surface_stalls"].value == 4
    assert metadata["min_parking_depth_m"].value == massing.MIN_PARKING_DEPTH_M


def test_asset_config_reaches_the_parking_search(store):
    """One bay reports a split yard at its better half; three use both.

    The knob is config for the same reason the aspect ratios are - nothing in
    a by-law says how many patches of asphalt a lot has - and this is what
    says it reaches the fit.
    """
    parcel = near_villeray(box(0, 0, 20, 44))
    set_parcels([parcel_row(parcel)])
    write_upstreams(
        store,
        hbu_rows=[
            hbu_row(
                1,
                lot_area_m2=880.0,
                footprint_m2=280.0,
                surface_stalls=16,
                surface_area_m2=16 * STALL_M2,
            )
        ],
        # A band across the middle, so the yard is a front piece and a back one.
        setback_rows=[setback_row(near_villeray(box(0, 15, 20, 29)))],
    )

    def placed(max_bays):
        run(
            store,
            run_config={
                "ops": {
                    "gold__lot_building_massing": {
                        "config": {"parking_max_bays": max_bays}
                    }
                }
            },
        )
        return read_output(store).iloc[0]

    one = placed(1)
    three = placed(3)
    assert one["num_parking_bays"] == 1
    assert three["num_parking_bays"] > 1
    assert three["placed_surface_parking_m2"] > one["placed_surface_parking_m2"]
