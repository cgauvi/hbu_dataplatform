"""Identifying the parcels that *are* the street, on real Montreal geometry.

`tests/unit/test_hbu.py` covers what happens once a parcel is known to be the
roadway: it keeps its row, loses its program, reports `road_parcel`, and drops
out of the redevelopment gap and the investment shortlist behind it. What no
offline test can cover is the step before that - *deciding* which parcels those
are - because the decision is a PostGIS length over two publishers' geometry,
and a fixture built out of rectangles would agree with any rule at all.

This module measures. The subject is **avenue Querbes between Ball and
Saint-Roch**, which Quebec's renewed cadastre draws as lots **2 249 179** and
**2 249 339** - two strips of 3 319 m2 and 3 298 m2, each about 9 m wide and a
block long, numbered exactly like the houses along them.

They are the case the gate was widened for. Neither is on the assessment roll,
so `hbu.road_parcel_lots` - which reads the roll's CUBF 45xx codes - cannot see
either one, and the highest-and-best-use chain answered for them out of the
zoning of the blocks either side: a mid-rise on the roadway, in the
redevelopment gap, on the investment shortlist. Nothing in the cadastre's own
attributes says otherwise; both carry the same `CO_TYPE_POLGN`, `CO_STATT_LOT`
and `CO_TYPE_MORCL` as every house in Parc-Extension. The geobase double is
what tells them apart, and this is the test that it does.

Run them with a throwaway PostGIS; see conftest.py for the one-liner.
"""

from __future__ import annotations

import pytest

from conftest import NEIGHBORHOOD, STREET_PARCEL_SCRAPE_DATE

from urban_rag.hbu import cadastral_road_lots
from urban_rag.postgis import (
    DEFAULT_ROAD_LOT_MIN_STREET_M,
    ROAD_LOT_COLUMNS,
    compute_lot_frontage,
)

#: The two parcels this module is about, and what they are.
QUERBES_LOTS = ("2 249 179", "2 249 339")

#: How much geobase double street line runs inside each of them, in metres,
#: measured in EPSG:32188. Not a value read off a previous run: 363 m by 9 m is
#: 3 270 m2, which is the area Infolot publishes for lot 2 249 179 to within a
#: percent, because the strip *is* the block of street.
QUERBES_STREET_M = {"2 249 179": 363.4, "2 249 339": 369.5}

#: An ordinary Parc-Extension parcel in the same window - a 222 m2 house lot
#: sharing its front boundary with lot 2 249 179, so it *faces* Querbes rather
#: than being a piece of it. The control: whatever identifies the two strips
#: above must leave the parcels abutting them alone.
HOUSE_LOT = "2 249 175"

#: How the slice splits. Six streets cross this window, cut at each
#: intersection into 14 road lots; the other 100 parcels are buildings and
#: yards. See the fixture's README for the two orders of magnitude between them.
NUM_ROAD_LOTS = 14
NUM_LOTS = 114

#: The most street line any *non*-road parcel in the slice carries, in metres.
#: A corner clipped where the cadastre and the geobase disagree by a few
#: centimetres - lots 2 249 342 and 2 249 343, two 2.9 m2 slivers at an
#: intersection. This is the whole of the false-positive pressure on the
#: cutoff, and it is three hundred times below it.
MAX_NON_ROAD_STREET_M = 0.32

#: The band `min_street_m` may be set anywhere inside without changing which
#: parcels are the roadway, in metres.
#:
#: **The cutoff is per (parcel, side), not per parcel** - see the
#: `_frontage_road_sides` query - so what decides whether a strip survives is
#: its *longest single* side, not the 126 m to 606 m it carries in total. The
#: shortest of those in this slice is 57.6 m, on lot 2 590 348; the longest any
#: ordinary parcel carries from one side is 0.28 m. That is the empty band, and
#: it is a factor of two hundred wide.
STABLE_CUTOFF_M = (0.5, 1.0, 5.0, 25.0, 50.0)


@pytest.fixture
def measured(connection, loaded_street_parcels):
    """One run of the real thing over the slice, at the shipped default.

    Per test rather than per module, the same as `test_lot_frontage.py`'s: the
    tests below re-run at other cutoffs and `silver.lot_frontage` holds one row
    set per partition, so a shared run would leave whichever test went last
    deciding what the others read.
    """
    with connection.transaction():
        result = compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=STREET_PARCEL_SCRAPE_DATE,
            min_street_m=DEFAULT_ROAD_LOT_MIN_STREET_M,
        )
    return result


def road_lots_at(connection, min_street_m):
    """The road lot frame one run at ``min_street_m`` produces."""
    with connection.transaction():
        result = compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=STREET_PARCEL_SCRAPE_DATE,
            min_street_m=min_street_m,
        )
    return result["road_lots"]


# -- the two lots the failure was reported for -----------------------------


def test_the_querbes_lots_are_identified_as_the_roadway(measured):
    """The regression. Lot 2 249 179 and lot 2 249 339 are avenue Querbes."""
    numbers = cadastral_road_lots(measured["road_lots"])

    for lot in QUERBES_LOTS:
        assert lot in numbers, f"{lot} is a street parcel and was not found"


def test_the_querbes_lots_carry_a_block_of_street_line_each(measured):
    """*Why* they are, per parcel, so the identification is readable.

    A block of street, not a side clipping a corner: the strip contains the
    whole run of the geobase double between two intersections, on both sides
    of the roadway and across every cross street it meets.
    """
    frame = measured["road_lots"].set_index("lot_number")

    for lot, expected in QUERBES_STREET_M.items():
        assert frame.loc[lot, "street_m_inside"] == pytest.approx(expected, abs=0.5)
        # Six cross streets and two sides of Querbes: the cadastre cuts a
        # roadway at each intersection and the geobase cuts a side at each one
        # too, so one strip meets many sides.
        assert frame.loc[lot, "num_street_sides"] > 1


def test_a_house_lot_on_the_same_street_is_not_one(measured):
    """The control. Facing Querbes is not being Querbes, and the difference is
    that the street line runs *inside* the roadway and enters no other parcel.
    """
    assert HOUSE_LOT not in cadastral_road_lots(measured["road_lots"])


def test_the_querbes_lots_get_no_frontage_row(connection, measured):
    """A street does not front on itself, which is why the road lots cannot
    travel as a column of `silver.lot_frontage` and need a file of their own.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT count(*) FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date
          AND lot_number = ANY(%s)
        """,
        [NEIGHBORHOOD, STREET_PARCEL_SCRAPE_DATE, list(QUERBES_LOTS)],
    )
    (rows,) = cursor.fetchone()
    assert rows == 0


# -- the slice as a whole --------------------------------------------------


def test_the_road_lots_are_the_ones_the_street_runs_through(measured):
    frame = measured["road_lots"]

    assert measured["num_lots"] == NUM_LOTS
    assert measured["num_road_lots"] == NUM_ROAD_LOTS
    assert len(frame) == NUM_ROAD_LOTS
    assert list(frame.columns) == list(ROAD_LOT_COLUMNS)
    # Ordered so the file is read from the top and so a re-run names the same
    # parcels in the same order.
    assert frame["street_m_inside"].is_monotonic_decreasing


def test_every_road_lot_carries_far_more_street_than_the_cutoff(measured):
    """The cutoff is the run's only judgement, and this is the headroom on it.

    The least any road lot carries is 126 m against a 1 m cutoff, so the
    identification is not a question of where the threshold was set.
    """
    least = measured["road_lots"]["street_m_inside"].min()

    assert least > 100.0 * DEFAULT_ROAD_LOT_MIN_STREET_M
    assert least > MAX_NON_ROAD_STREET_M * 300


def test_a_cutoff_above_a_strips_longest_side_loses_it(
    connection, loaded_street_parcels
):
    """The far edge of the band, asserted rather than left to be discovered.

    `min_street_m` is compared against one (parcel, side) pair at a time, so a
    strip holding 606 m of street across six sides of 100 m drops out at a
    cutoff of 150. Nothing sets it there - the shipped default is 1.0 and the
    band above is two hundred times wide - but the shape of the rule is worth
    pinning, because "how much street is inside this parcel" and "how much of
    one side is inside it" are different questions and only the second is asked.
    """
    numbers = cadastral_road_lots(road_lots_at(connection, 100.0))

    assert len(numbers) < NUM_ROAD_LOTS
    # And it is the short strips that go first, not an arbitrary subset: both
    # blocks of Querbes are cut into 64 m and 66 m sides at the cross streets.
    for lot in QUERBES_LOTS:
        assert lot not in numbers


@pytest.mark.parametrize("min_street_m", STABLE_CUTOFF_M)
def test_the_identification_does_not_turn_on_the_cutoff(
    connection, loaded_street_parcels, min_street_m
):
    """The band between the two populations is empty across a factor of two
    hundred, so the same 14 parcels come back over all of it. A rule whose
    answer moved with its setting would be a tuning knob on which parcels are
    development sites, which is not something anyone should be able to tune.

    See `STABLE_CUTOFF_M` for where the band ends and why it ends there: the
    test is per (parcel, side), so a cutoff above a strip's longest single side
    drops it however much street it holds in total.
    """
    numbers = cadastral_road_lots(road_lots_at(connection, min_street_m))

    assert len(numbers) == NUM_ROAD_LOTS
    for lot in QUERBES_LOTS:
        assert lot in numbers, f"{lot} was lost at min_street_m={min_street_m}"
    assert HOUSE_LOT not in numbers


def test_a_cutoff_above_every_parcel_finds_no_road_at_all(
    connection, loaded_street_parcels
):
    """The other end of the sweep, so the parametrisation above is known to be
    testing something: at 10 km nothing in the slice qualifies, and the frame
    comes back empty with its columns rather than absent."""
    frame = road_lots_at(connection, 10_000.0)

    assert frame.empty
    assert list(frame.columns) == list(ROAD_LOT_COLUMNS)
    assert cadastral_road_lots(frame) == frozenset()
