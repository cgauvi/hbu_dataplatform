"""`load_addresses` and `compute_lot_addresses` against a real PostGIS.

The unit tests stub the database out, which is right for the paging and the
parse and no help at all for the join itself: a point-in-polygon that matched
everything to the first lot would pass every one of them. This module joins.

The subject is the parcel the other integration modules use - lot **3 790 556**
on avenue Chabot in Villeray, a clean rectangle of 476.1 m2 - cut into two zone
pieces by a boundary 9 m back from the street, the same fixture
`test_lot_zone_pieces` builds and for the same reason: the platform's real
problem is a zoning boundary that does not follow a lot line, and an address
has to land on the right side of one.

Five points are placed against that parcel, one per branch of the join:

    front piece   one address, inside the 9 m strip
    rear piece    two addresses at one civic number - a door and a unit above
                  it - which is what makes `num_piece_addresses` and
                  `num_piece_civic_addresses` different numbers
    just outside  1 m off the lot line, which the 2 m snap must reach
    far outside   200 m away, which it must not

Run them with a throwaway PostGIS; see conftest.py for the one-liner.
"""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Point, box
from shapely.ops import transform

from conftest import (
    NEIGHBORHOOD,
    ZONE_SOURCE_TABLE,
    load_zone_clips,
)

from hbu_dataplatform.core.postgis import (
    DEFAULT_ADDRESS_SNAP_M,
    compute_lot_addresses,
    compute_lot_zone_pieces,
    fetch_lot_addresses,
    load_addresses,
)

#: A scrape date of this module's own, so it cannot collide with the partitions
#: the other integration modules load into the same database.
SCRAPE_DATE = "2000-01-03"

LOT_NUMBER = "3 790 556"
FRONT_ZONE = "C01-777"
REAR_ZONE = "H01-999"
BOUNDARY_DEPTH_M = 9.0

#: Metres between the near point and the parcel. Inside `DEFAULT_ADDRESS_SNAP_M`
#: with room to spare, so a test that fails on it has failed on the snap and not
#: on a rounding.
NEAR_M = 1.0

#: Metres between the far point and the parcel. Well past the snap, for the same
#: reason in the other direction.
FAR_M = 200.0

#: A second near point, offset due **east** rather than wherever the buffer's
#: midpoint happens to fall. This one is the regression guard for a real bug.
#:
#: The snap's candidate filter has to work in degrees to use the GiST index,
#: and a degree of longitude is shorter than a degree of latitude - 78 km
#: against 111 km here. A radius converted at the *latitude* scale is therefore
#: only about 1.4 m wide in the east-west direction, so a point well inside the
#: 2 m snap was found or missed depending only on which way it lay from the
#: parcel. Measured against this fixture, the old radius stopped reaching at a
#: true distance of about 1.52 m:
#:
#:     true distance   1.52 m   found by the latitude radius
#:     true distance   1.69 m   missed by it, inside the snap
#:     true distance   2.03 m   outside the snap, correctly refused either way
#:
#: 1.8 m sits in the middle of that window - comfortably inside the 2 m snap,
#: comfortably outside the radius the bug computed. See
#: `postgis._M_PER_DEGREE_LAT`.
EAST_M = 1.8


def metric_transformers():
    """(to metres, back to degrees) for the borough this fixture is in."""
    from pyproj import Transformer

    to_metric = Transformer.from_crs("EPSG:4326", "EPSG:32188", always_xy=True)
    to_wgs = Transformer.from_crs("EPSG:32188", "EPSG:4326", always_xy=True)
    return to_metric.transform, to_wgs.transform


@pytest.fixture(scope="module")
def split_lot(connection):
    """Lot 3 790 556 in `rag.lots`, cut into a front and a rear zone piece.

    Loaded here rather than reusing the session `loaded` fixture because this
    module writes its own scrape date - see `SCRAPE_DATE`.
    """
    import pathlib

    fixtures = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "frontage"
    lots = gpd.read_parquet(fixtures / "lots.parquet")
    lot = lots[lots["lot_number"] == LOT_NUMBER].iloc[0]

    cursor = connection.cursor()
    with connection.transaction():
        cursor.execute(
            "DELETE FROM rag.lots WHERE neighborhood = %s AND scrape_date = %s::date",
            [NEIGHBORHOOD, SCRAPE_DATE],
        )
        cursor.execute(
            "INSERT INTO rag.lots (lot_number, neighborhood, scrape_date, area_m2, geom)"
            " VALUES (%s, %s, %s::date, %s,"
            " ST_Multi(ST_GeomFromWKB(decode(%s, 'hex'), 4326)))",
            [
                LOT_NUMBER,
                NEIGHBORHOOD,
                SCRAPE_DATE,
                float(lot["area_m2"]),
                lot.geometry.wkb_hex,
            ],
        )

    # The zoning boundary, drawn 9 m back from the parcel's northern edge in
    # metres and carried back to 4326 - the same cut test_lot_zone_pieces makes.
    to_metric, to_wgs = metric_transformers()
    metric_lot = transform(to_metric, lot.geometry)
    minx, miny, maxx, maxy = metric_lot.bounds
    front_box = box(minx - 1, maxy - BOUNDARY_DEPTH_M, maxx + 1, maxy + 1)
    rear_box = box(minx - 1, miny - 1, maxx + 1, maxy - BOUNDARY_DEPTH_M)

    front = transform(to_wgs, metric_lot.intersection(front_box))
    rear = transform(to_wgs, metric_lot.intersection(rear_box))

    load_zone_clips(
        connection,
        SCRAPE_DATE,
        [(LOT_NUMBER, FRONT_ZONE, front), (LOT_NUMBER, REAR_ZONE, rear)],
    )
    # No `ensure_partition` for either of the two tables below: both are
    # written through `hbu_dataplatform.core.warehouse`, which calls hbu_infra's own
    # `warehouse.ensure_partition` and builds *two* levels - a borough leaf
    # that is itself partitioned by month. Creating a flat leaf for the same
    # borough here would claim the LIST value and make the real one fail, and
    # the error surfaces on the month partition rather than on the collision.
    # The conftest helper is for tables these tests INSERT into directly.
    # Wrapped, because the conftest connection is autocommit and every
    # `warehouse` write stages into a temp table declared ON COMMIT DROP - so
    # without a transaction the staging table is gone before the INSERT that
    # fills it. The same reason test_lot_zone_pieces wraps its own call.
    with connection.transaction():
        compute_lot_zone_pieces(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            zone_sources=(ZONE_SOURCE_TABLE,),
            metric_srid=32188,
        )
    return {"lot": lot.geometry, "front": front, "rear": rear}


def address_row(address_id, civic, unit, street, point, *, suffix=None):
    """One row shaped the way `address_assets.parse_address_frame` shapes it."""
    from hbu_dataplatform.sources.addresses.client import civic_address

    return {
        "address_id": address_id,
        "object_id": int(address_id.rsplit("-", 1)[-1]),
        "formatted_address": (
            f"{unit + '-' if unit else ''}{civic} {street}, Montréal H2R2H8"
        ),
        "civic_number": civic,
        "civic_suffix": suffix,
        "unit": unit,
        "street_name": street,
        "municipality": "Montréal",
        "postal_code": "H2R2H8",
        "num_units": 1,
        "characteristic": "",
        "source_version": "AQ20260901",
        "civic_address": civic_address(civic, suffix, street),
        "geometry": point,
    }


@pytest.fixture(scope="module")
def joined(connection, split_lot):
    """The five points loaded and joined, with the run's own counters."""
    to_metric, to_wgs = metric_transformers()
    front_point = split_lot["front"].representative_point()
    rear_point = split_lot["rear"].representative_point()

    # A point inside the snap, and one far past it. Taken off the exterior of a
    # buffer rather than off the bounding box: lot 3 790 556 is a rectangle but
    # it is *not* axis-aligned in MTM zone 8, so a point 1 m beyond its bbox is
    # several metres from the parcel itself and the snap would rightly refuse
    # it. Every point on `buffer(d).exterior` of a convex parcel is exactly d
    # from it, which is the distance these tests mean to be testing.
    metric_lot = transform(to_metric, split_lot["lot"])
    near_metric = metric_lot.buffer(NEAR_M).exterior.interpolate(0.5, normalized=True)
    far_metric = metric_lot.buffer(FAR_M).exterior.interpolate(0.5, normalized=True)
    assert metric_lot.distance(near_metric) == pytest.approx(NEAR_M, abs=0.01)
    assert metric_lot.distance(far_metric) == pytest.approx(FAR_M, abs=0.5)
    near = transform(to_wgs, near_metric)
    far = transform(to_wgs, far_metric)

    # Due east of the parcel, and exactly EAST_M from it. The parcel is not
    # axis-aligned, so its bounding box corner is not its edge: the edge at
    # this latitude is where a horizontal line through the parcel leaves it,
    # and the offset is taken from there. Purely east-west, which is the whole
    # point of this fixture.
    minx, miny, maxx, maxy = metric_lot.bounds
    mid_y = (miny + maxy) / 2
    across = LineString([(minx - 10, mid_y), (maxx + 10, mid_y)])
    east_edge_x = across.intersection(metric_lot).bounds[2]
    # Stepping east by one metre does not move the point one metre *away* from
    # a parcel whose edge is slanted, so the easting is scaled by how much of
    # it turns into distance. Measured with a probe rather than assumed, since
    # it is a property of this parcel's rotation.
    per_metre_east = metric_lot.distance(Point(east_edge_x + 1.0, mid_y))
    east_metric = Point(east_edge_x + EAST_M / per_metre_east, mid_y)
    assert metric_lot.distance(east_metric) == pytest.approx(EAST_M, abs=0.01)
    east = transform(to_wgs, east_metric)

    frame = gpd.GeoDataFrame(
        [
            address_row("addr-1", 7401, None, "Avenue Chabot", front_point),
            # Two rows at one civic address: the door, and a unit above it.
            address_row("addr-2", 7405, None, "Avenue Chabot", rear_point),
            address_row("addr-3", 7405, "2", "Avenue Chabot", rear_point),
            address_row("addr-4", 7409, None, "Avenue Chabot", near),
            address_row("addr-5", 9999, None, "Rue Ailleurs", far),
            address_row("addr-6", 7411, None, "Avenue Chabot", east),
        ],
        geometry="geometry",
        crs="EPSG:4326",
    )

    # One transaction for the load and the join, which is what the asset does:
    # an empty answer has to roll the loaded points back with it rather than
    # leaving a borough half landed.
    with connection.transaction():
        loaded = load_addresses(
            connection, frame, neighborhood=NEIGHBORHOOD, scrape_date=SCRAPE_DATE
        )
        result = compute_lot_addresses(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            max_snap_m=DEFAULT_ADDRESS_SNAP_M,
        )
    rows = fetch_lot_addresses(
        connection, neighborhood=NEIGHBORHOOD, scrape_date=SCRAPE_DATE
    )
    return {"loaded": loaded, "result": result, "rows": rows}


def test_every_point_is_loaded_into_the_working_set(joined):
    assert joined["loaded"] == 6
    assert joined["result"]["num_points"] == 6


def test_the_point_on_nobody_s_parcel_is_counted_and_not_written(joined):
    """The grain is an address *on a lot*, so a null lot_uid is not an option."""
    result = joined["result"]
    assert result["num_unmatched"] == 1
    assert result["num_addresses"] == 5
    assert "addr-5" not in set(joined["rows"]["address_id"])


def test_a_point_just_off_the_lot_line_is_snapped_to_it(joined):
    """Two publishers' surveys of the same ground disagree at the edges."""
    rows = joined["rows"].set_index("address_id")

    assert joined["result"]["num_snapped"] == 2
    assert rows.loc["addr-4", "match_basis"] == "snapped"
    assert rows.loc["addr-4", "lot_number"] == LOT_NUMBER
    assert 0 < rows.loc["addr-4", "snap_distance_m"] <= DEFAULT_ADDRESS_SNAP_M
    # Everything else landed inside the parcel and reached for nothing.
    assert set(rows.loc[["addr-1", "addr-2", "addr-3"], "match_basis"]) == {"within"}
    assert set(rows.loc[["addr-1", "addr-2", "addr-3"], "snap_distance_m"]) == {0.0}


def test_a_point_due_east_is_snapped_as_readily_as_one_due_north(joined):
    """The snap's candidate filter works in degrees, and a degree of longitude
    is shorter than a degree of latitude. A radius converted at the latitude
    scale is ~1.4 m wide east-west, so this point - 1.7 m away, inside the 2 m
    snap - was found or missed depending only on which way it lay."""
    rows = joined["rows"].set_index("address_id")

    assert "addr-6" in rows.index
    assert rows.loc["addr-6", "match_basis"] == "snapped"
    assert rows.loc["addr-6", "snap_distance_m"] == pytest.approx(EAST_M, abs=0.1)


def test_an_address_lands_on_the_zone_piece_it_stands_in(joined):
    """The whole point of the grain: the front strip and the housing behind it
    are two sites, and an address belongs to one of them."""
    rows = joined["rows"].set_index("address_id")

    assert rows.loc["addr-1", "feature_id"] == FRONT_ZONE
    assert rows.loc["addr-2", "feature_id"] == REAR_ZONE
    assert rows.loc["addr-3", "feature_id"] == REAR_ZONE
    assert set(rows["piece_basis"]) <= {"piece", "primary"}
    assert rows.loc["addr-1", "piece_basis"] == "piece"
    assert rows.loc["addr-1", "source_table"] == ZONE_SOURCE_TABLE


def test_units_and_doors_are_counted_separately(joined):
    """Counting rows would make one duplex look like two buildings.

    Asserted against the rows themselves rather than against numbers written
    out here: which piece the snapped point lands beside is a property of the
    fixture's geometry, and pinning it would make this test fail for a reason
    that has nothing to do with the two counts.
    """
    rows = joined["rows"]
    rear = rows[rows["feature_id"] == REAR_ZONE]

    # The two counts agree with what is actually on the piece...
    assert (rear["num_piece_addresses"] == len(rear)).all()
    assert (
        rear["num_piece_civic_addresses"] == rear["civic_address"].nunique()
    ).all()
    # ...and they differ, because addr-2 and addr-3 share one door.
    assert rear["num_piece_civic_addresses"].iloc[0] < rear["num_piece_addresses"].iloc[0]
    assert {"addr-2", "addr-3"} <= set(rear["address_id"])

    # The front piece holds one address, so for it the two counts coincide.
    front = rows[rows["feature_id"] == FRONT_ZONE]
    assert front["num_piece_addresses"].tolist() == [1]
    assert front["num_piece_civic_addresses"].tolist() == [1]


def test_one_address_per_site_is_marked_for_a_map_to_label_it_with(joined):
    rows = joined["rows"].set_index("address_id")

    # Rank 1 within each piece, ordered by civic number and then by unit - so
    # the door comes before the apartment above it.
    assert rows.loc["addr-1", "address_rank"] == 1
    assert bool(rows.loc["addr-1", "is_primary_address"])
    assert rows.loc["addr-2", "address_rank"] == 1
    assert rows.loc["addr-3", "address_rank"] == 2
    assert not bool(rows.loc["addr-3", "is_primary_address"])

    primary = joined["rows"][joined["rows"]["is_primary_address"]]
    # One per (lot, piece), which is what makes it a label rather than a list.
    assert len(primary) == len(
        joined["rows"].groupby(["lot_uid", "feature_id"]).size()
    )


def test_the_parcel_s_own_total_travels_on_every_row(joined):
    """A reader holding one piece should not have to join back for the parcel."""
    rows = joined["rows"]
    assert set(rows["num_lot_addresses"]) == {5}


def test_the_join_is_keyed_for_the_gold_tables(joined):
    """(lot_uid, feature_id) is what gold is keyed on since the grain changed."""
    rows = joined["rows"]
    assert rows["lot_uid"].notna().all()
    assert rows["feature_id"].notna().all()
    assert rows["lot_number"].eq(LOT_NUMBER).all()


def test_the_parsed_address_survives_the_round_trip(joined):
    """The join reads these back out of jsonb, so a rename upstream is a column
    of nulls here and nothing that raises."""
    rows = joined["rows"].set_index("address_id")

    assert rows.loc["addr-3", "unit"] == "2"
    assert rows.loc["addr-3", "street_name"] == "Avenue Chabot"
    assert rows.loc["addr-3", "civic_number"] == 7405
    assert rows.loc["addr-3", "civic_address"] == "7405 Avenue Chabot"
    assert rows.loc["addr-3", "municipality"] == "Montréal"
    assert rows.loc["addr-3", "postal_code"] == "H2R2H8"
    assert rows.loc["addr-3", "source_version"] == "AQ20260901"
    assert joined["result"]["num_unparsed"] == 0


def test_a_re_run_replaces_the_partition_rather_than_doubling_it(
    connection, split_lot, joined
):
    """The upsert-then-prune every table here is written by."""
    with connection.transaction():
        again = compute_lot_addresses(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            max_snap_m=DEFAULT_ADDRESS_SNAP_M,
        )
    assert again["num_addresses"] == joined["result"]["num_addresses"]

    rows = fetch_lot_addresses(
        connection, neighborhood=NEIGHBORHOOD, scrape_date=SCRAPE_DATE
    )
    assert len(rows) == len(joined["rows"])
    assert rows["address_id"].is_unique


def test_turning_the_snap_off_drops_the_point_it_had_reached_for(
    connection, split_lot, joined
):
    """0 is the setting that says 'inside the parcel or nowhere', and the count
    it changes is the one the asset reports."""
    with connection.transaction():
        strict = compute_lot_addresses(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            max_snap_m=0.0,
        )
    assert strict["num_snapped"] == 0
    assert strict["num_addresses"] == joined["result"]["num_addresses"] - 2
    assert strict["num_unmatched"] == joined["result"]["num_unmatched"] + 2

    # Put the partition back for whatever runs after this module.
    with connection.transaction():
        compute_lot_addresses(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            max_snap_m=DEFAULT_ADDRESS_SNAP_M,
        )


def test_a_negative_snap_is_refused(connection):
    with pytest.raises(ValueError, match="max_snap_m"):
        compute_lot_addresses(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            max_snap_m=-1.0,
        )
