"""`compute_lot_frontage` against a real PostGIS and real Montreal geometry.

The unit tests stub the database out, which is the right call for the asset's
plumbing and no help at all for the measure: a frontage query that matched
nothing would pass every one of them. This module measures.

The subject is lot **3 790 556**, a 476.1 m2 parcel on avenue Chabot in
Villeray. It is a clean rectangle, 15.24 m of street edge by 31.24 m deep, and
it shares that street edge with lot **3 946 200** - the strip of Chabot itself,
which Infolot draws as a parcel like any other. Two independent publishers
agree on its size: Infolot draws the polygon and carries 476.1269 m2 as an
attribute, and the 2026 assessment roll records 476.1 m2 of land against the
same lot number.

That shared edge is the whole measure now, and this module's job is to pin the
two things the old measure got wrong. It reported 20.3 m for this lot at a 6 m
buffer and 32.3 m at 12 m, because the buffer swallowed the first `buffer_m` of
both side boundaries; and at the 3 m it defaulted to it reported nothing at
all, because the lot line sits 3.48 m behind the geobase side. The shared edge
has no such setting - see `test_the_measure_does_not_move_with_the_cutoff`.

Run them with a throwaway PostGIS; see conftest.py for the one-liner.
"""

from __future__ import annotations

import pytest

from conftest import NEIGHBORHOOD, SCRAPE_DATE

from urban_rag.postgis import DEFAULT_ROAD_LOT_MIN_STREET_M, compute_lot_frontage

#: The lot every assertion here is about, and the street side it fronts on.
LOT_NUMBER = "3 790 556"
COTE_RUE_ID = "11000531"
STREET_NAME = "Chabot"

#: The parcel that *is* avenue Chabot where the subject lot meets it. Infolot
#: draws the right-of-way as a lot, which is what makes the measure below an
#: ordinary shared boundary between two parcels.
ROAD_LOT_NUMBER = "3 946 200"

#: Its front boundary, in metres. The lot is a rectangle whose street edge
#: measures 15.238 m in EPSG:32188 and whose two side edges measure 31.237 m.
#: Not a value read off a previous run: the same 15.24 m falls out of the
#: geometry with shapely, it is twice the 7.619 m that the single-width lots
#: either side of it measure, and it is now also - exactly - the length of
#: boundary this lot shares with lot 3 946 200.
FRONTAGE_M = 15.24

#: How far the front boundary sits behind the geobase side. The number that
#: made a 3 m buffer miss this lot, and that the shared edge does not care
#: about at all: the side is used to identify and name Chabot, never to measure
#: the distance to it.
SETBACK_M = 3.48

#: How many parcels in the slice are the roadway, and how many are not. The
#: 164 lots of the fixture split 14 / 150, and the split is total rather than
#: marginal - see `test_the_road_lots_are_the_ones_the_street_runs_through`.
NUM_ROAD_LOTS = 14
NUM_LOTS = 164


@pytest.fixture
def measured(connection, loaded):
    """One run of the real thing over the fixture, at the shipped default.

    Per test, not per module: several tests below re-run the measure at other
    cutoffs, and `silver.lot_frontage` holds one partition, so a shared run
    would leave whichever test happened to go last deciding what the others
    read. The slice is 164 lots, so re-running it is cheap.
    """
    with connection.transaction():
        result = compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            min_street_m=DEFAULT_ROAD_LOT_MIN_STREET_M,
        )
    return result


def frontages_for(connection, lot_number):
    """Every `silver.lot_frontage` row for one lot, longest first."""
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT cote_rue_id, street_name, frontage_m, lot_perimeter_m,
               pct_of_perimeter, frontage_rank, buffer_m,
               ST_GeometryType(geom)
        FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date AND lot_number = %s
        ORDER BY frontage_rank
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, lot_number],
    )
    return cursor.fetchall()


def road_lot_numbers(connection):
    """The parcels a geobase side runs at least a metre inside."""
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT l.lot_number
        FROM rag.lots l
        JOIN silver.neighborhood_streets s
          ON s.neighborhood = l.neighborhood AND s.scrape_date = l.scrape_date
         AND ST_Intersects(l.geom, s.geom)
        WHERE l.neighborhood = %s AND l.scrape_date = %s::date
          AND ST_Length(ST_Intersection(
                  ST_Transform(s.geom, 32188), ST_Transform(l.geom, 32188)
              )) >= %s
        ORDER BY 1
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, DEFAULT_ROAD_LOT_MIN_STREET_M],
    )
    return [row[0] for row in cursor.fetchall()]


def lots_without_frontage(connection, *, exclude_roads=True):
    """Lots that got no row.

    Road lots are left out by default: one is not a parcel that failed to find
    a street, it *is* the street. `exclude_roads=False` is for the tests that
    run at a cutoff where nothing is a road lot, since the exclusion below is
    computed at the shipped default and would not match.
    """
    roads = set(road_lot_numbers(connection)) if exclude_roads else set()
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT lot_number
        FROM rag.lots l
        WHERE l.neighborhood = %s AND l.scrape_date = %s::date
          AND NOT EXISTS (
              SELECT 1 FROM silver.lot_frontage f
              WHERE f.lot_uid = l.lot_uid
                AND f.neighborhood = l.neighborhood
                AND f.scrape_date = l.scrape_date
          )
        ORDER BY lot_number
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )
    return [row[0] for row in cursor.fetchall() if row[0] not in roads]


# -- the road lots ---------------------------------------------------------


def test_the_road_lots_are_the_ones_the_street_runs_through(connection, measured):
    """The identification the whole measure rests on, and it is not marginal.

    A geobase double side is drawn along the roadway, so it runs *inside* the
    parcel that is the roadway and enters no other. In this slice that picks
    out fourteen parcels and no others - and the subject lot's own street,
    3 946 200, is one of them.
    """
    roads = road_lot_numbers(connection)

    assert len(roads) == NUM_ROAD_LOTS
    assert ROAD_LOT_NUMBER in roads
    # They are a contiguous family in the cadastre, which is what a borough's
    # right-of-way parcels look like. The subject lot is not among them.
    assert all(number.startswith("3 946") for number in roads), roads
    assert LOT_NUMBER not in roads


def test_no_parcel_is_a_sliver_away_from_the_street(connection, measured):
    """The assumption the zero tolerance rests on, asserted rather than hoped.

    Measuring the shared edge exactly is right only because this cadastre is a
    topological survey: abutting parcels reference the same points rather than
    coming close, so two neighbours sit at distance exactly 0 and everything
    else sits metres away. Nothing is supposed to land in between.

    A parcel a few millimetres off a road lot would be the failure that hurts,
    and it would be quiet: `ST_Intersects` drops it from the join and the lot
    comes out looking like an interior parcel rather than like a bug. This is
    the band that would catch it, and it is empty.
    """
    assert measured["num_lots_near_road_without_frontage"] == 0


def test_neighbouring_parcels_share_their_vertices_exactly(connection, measured):
    """Why no tolerance is needed: the shared edge is shared, not adjacent.

    The subject lot and the strip of Chabot it fronts on carry the two
    endpoints of their common boundary as the *same* coordinates, so the
    intersection of the two boundaries is exact rather than approximate - and
    stays exact through `ST_Transform`, which maps one input coordinate to one
    output coordinate whichever parcel it is reached from.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT ST_Distance(
                   ST_Transform(lot.geom, 32188), ST_Transform(road.geom, 32188)
               ),
               ST_NPoints(ST_Intersection(
                   ST_Boundary(ST_Transform(lot.geom, 32188)),
                   ST_Boundary(ST_Transform(road.geom, 32188))
               ))
        FROM rag.lots lot, rag.lots road
        WHERE lot.neighborhood = %s AND lot.scrape_date = %s::date
          AND road.neighborhood = lot.neighborhood
          AND road.scrape_date = lot.scrape_date
          AND lot.lot_number = %s AND road.lot_number = %s
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, LOT_NUMBER, ROAD_LOT_NUMBER],
    )
    distance_m, num_points = cursor.fetchone()

    # Exactly zero, not merely small - that is the difference between a shared
    # vertex and a near miss, and it is what a tolerance would be papering over.
    assert distance_m == 0.0
    # And the intersection is a real line, not a degenerate point.
    assert num_points >= 2


def test_a_road_lot_does_not_front_on_itself(connection, measured):
    """A street is not a development site with 300 m of frontage.

    It is also not a lot that "faces no street", which is why the run reports
    the road lots separately rather than counting them among the failures.
    """
    assert frontages_for(connection, ROAD_LOT_NUMBER) == []
    assert measured["num_road_lots"] == NUM_ROAD_LOTS
    assert measured["num_lots"] == NUM_LOTS


# -- the lot ---------------------------------------------------------------


def test_the_lot_fronts_on_exactly_one_street(connection, measured):
    """It is a mid-block parcel, so it has one street and not two.

    A second row here would mean the measure had reached across Chabot to its
    other side (cote_rue_id 11000532, 11.5 m away) or picked up the lane
    behind - both of which a buffer wide enough to reach the lot at all can do,
    and neither of which is a frontage. A shared boundary cannot: the lot does
    not touch either.
    """
    rows = frontages_for(connection, LOT_NUMBER)

    assert [row[0] for row in rows] == [COTE_RUE_ID]
    assert rows[0][1] == STREET_NAME
    assert rows[0][5] == 1


def test_the_frontage_is_the_lots_street_edge(connection, measured):
    """15.24 m: the boundary it shares with lot 3 946 200, and nothing else."""
    (row,) = frontages_for(connection, LOT_NUMBER)
    _, _, frontage_m, perimeter_m, pct, _, buffer_m, _ = row

    assert frontage_m == pytest.approx(FRONTAGE_M, abs=0.05)
    # 2 * (15.238 + 31.237)
    assert perimeter_m == pytest.approx(92.95, abs=0.1)
    assert pct == pytest.approx(100.0 * frontage_m / perimeter_m, abs=0.01)
    # No buffer was used, and the column says so rather than carrying a cutoff
    # that no longer exists. A partition whose rows say 3.0 or 10.0 was
    # measured the old way and is reporting a different quantity.
    assert buffer_m == 0.0


def test_the_frontage_geometry_is_the_shared_boundary(connection, measured):
    """Linework, and only as much of the lot's edge as the street lot touches."""
    (row,) = frontages_for(connection, LOT_NUMBER)
    assert row[7] in ("ST_MultiLineString", "ST_LineString", "ST_GeometryCollection")

    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT ST_Length(ST_Transform(geom, 32188)),
               ST_Length(ST_Transform(geom, 32188)) <= lot_perimeter_m + 0.01
        FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date AND lot_number = %s
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, LOT_NUMBER],
    )
    length_m, within_perimeter = cursor.fetchone()

    # The geometry is the thing that was measured, so it is the same length.
    assert length_m == pytest.approx(FRONTAGE_M, abs=0.05)
    assert within_perimeter


def test_the_frontage_is_exactly_the_edge_shared_with_the_road_lot(
    connection, measured
):
    """Stated against the cadastre itself rather than against a pinned number.

    This is the definition the module is testing, computed independently of
    `compute_lot_frontage`: the intersection of the two parcel boundaries. If
    the two ever disagree, the measure has stopped being the shared edge.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT ST_Length(ST_Intersection(
                   ST_Boundary(ST_Transform(lot.geom, 32188)),
                   ST_Boundary(ST_Transform(road.geom, 32188))
               ))
        FROM rag.lots lot, rag.lots road
        WHERE lot.neighborhood = %s AND lot.scrape_date = %s::date
          AND road.neighborhood = lot.neighborhood
          AND road.scrape_date = lot.scrape_date
          AND lot.lot_number = %s AND road.lot_number = %s
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, LOT_NUMBER, ROAD_LOT_NUMBER],
    )
    (shared_m,) = cursor.fetchone()
    (row,) = frontages_for(connection, LOT_NUMBER)

    assert shared_m == pytest.approx(FRONTAGE_M, abs=0.05)
    assert row[2] == pytest.approx(shared_m, abs=0.001)


@pytest.mark.parametrize("min_street_m", [0.5, 1.0, 5.0, 25.0, 100.0])
def test_the_measure_does_not_move_with_the_cutoff(connection, loaded, min_street_m):
    """The property the buffer clip never had, and the reason for the fix.

    `min_street_m` decides which parcels are read as roadway. It must not decide
    what any lot then measures, and it cannot: the frontage is a shared
    boundary, computed once the road lots are known. The old `buffer_m` was
    load-bearing in both roles at once, so widening it to reach the borough's
    lots inflated every frontage in it by twice the widening - 20.3 m at 6 m,
    32.3 m at 12 m, for this same lot.

    The range here spans two orders of magnitude because the identification has
    that much headroom: the road lots carry 105 m to 325 m of street line and
    every other parcel carries none.
    """
    with connection.transaction():
        compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            min_street_m=min_street_m,
        )

    rows = frontages_for(connection, LOT_NUMBER)

    assert [row[0] for row in rows] == [COTE_RUE_ID]
    assert rows[0][2] == pytest.approx(FRONTAGE_M, abs=0.05)


def test_the_setback_behind_the_geobase_no_longer_costs_the_lot(connection, measured):
    """The bug, pinned so it cannot come back.

    The lot's front boundary sits 3.48 m behind cote_rue_id 11000531, and under
    the buffer clip that was fatal: at the 3 m default this lot - and 90 % of
    the borough - matched no street at all. The distance is now used for
    nothing. It is not what finds the street, and it is not what measures it;
    the geobase side only names Chabot, from inside the road lot.
    """
    (row,) = frontages_for(connection, LOT_NUMBER)

    assert row[1] == STREET_NAME
    assert row[2] == pytest.approx(FRONTAGE_M, abs=0.05)
    # The gap that used to lose it is larger than any tolerance in the measure,
    # because there is no tolerance in the measure.
    assert SETBACK_M > 0.0


# -- every lot -------------------------------------------------------------


def test_all_but_one_lot_faces_a_street(connection, measured):
    """A lot in a Montreal borough that faces no street is a finding.

    Some are real - a landlocked remnant, a parcel served off a lane - but a
    lot with no frontage is far more often the measure failing to reach it,
    which is what a too-narrow buffer looked like here. In this slice exactly
    one parcel is the real thing, and it is asserted by name so that a second
    one appearing is a test failure rather than a rounding of the count.

    Lot 3 790 483 is a 320 m2 interior parcel: 22.36 m against a neighbour on
    each side, 14.33 m against lot 3 790 509 - one of the borough's ruelles -
    at the rear, and a single *point* of contact with the corner of road lot
    3 946 205. A point is not a frontage, and the run is right to give it none.
    The old buffer measure credited it with street it does not have.
    """
    flagged = lots_without_frontage(connection)

    assert flagged == ["3 790 483"], (
        f"{len(flagged)} of {NUM_LOTS - NUM_ROAD_LOTS} non-road lot(s) share "
        f"no boundary with a road lot: {', '.join(flagged)}"
    )


def test_the_run_reports_the_lots_it_could_not_place(connection, loaded):
    """Whatever the count, the caller is handed the lot numbers behind it.

    Run with a cutoff no parcel can meet, so that nothing is a road lot and
    every parcel in the slice goes unplaced. This is what `frontage_assets`
    turns into a warning and into the `lots_without_frontage` metadata: a share
    on its own says a partition is bad, and the numbers say which rows to look
    at.
    """
    with connection.transaction():
        result = compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            min_street_m=10_000.0,
        )

    sample = result["lots_without_frontage"]
    every = lots_without_frontage(connection, exclude_roads=False)

    assert result["num_road_lots"] == 0, "no parcel holds 10 km of street line"
    assert result["lots_matched"] == 0
    assert sample, "with no road lots, nothing in the slice can be placed"
    # `sample` is capped and ordered - see _LOTS_WITHOUT_FRONTAGE_SAMPLE - so it
    # is the *head* of the real set rather than all of it, and that is the
    # assertion worth making: it is a place to start looking, and the count is
    # what says how much there is to look at.
    assert len(sample) < len(every), "the sample is capped, so it is not the set"
    assert sample == every[: len(sample)]
    assert len(every) == result["num_lots"] - result["lots_matched"]


def test_no_lot_is_given_more_frontage_than_it_has_boundary(connection, measured):
    """The invariant the buffer clip broke.

    Frontage is a length along the parcel's edge, so a lot's frontages cannot
    add up to more edge than the parcel has. Under the old measure they could
    and did: each street side contributed the first `buffer_m` of the lot's
    side boundaries on top of the real edge, and a corner lot counted the same
    metres twice. A shared boundary is a subset of the perimeter by
    construction, so this now holds for a reason rather than by luck.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT lot_number, sum(frontage_m), max(lot_perimeter_m)
        FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date
        GROUP BY lot_number
        HAVING sum(frontage_m) > max(lot_perimeter_m) + 0.01
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )
    over = cursor.fetchall()

    assert not over, f"frontage exceeds the lot's own perimeter for {over}"


def test_the_primary_frontage_is_the_longest_one(connection, measured):
    """`frontage_rank` 1 is what a caller filters on for *the* street."""
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT lot_number
        FROM silver.lot_frontage a
        WHERE neighborhood = %s AND scrape_date = %s::date AND frontage_rank = 1
          AND EXISTS (
              SELECT 1 FROM silver.lot_frontage b
              WHERE b.lot_uid = a.lot_uid
                AND b.neighborhood = a.neighborhood
                AND b.scrape_date = a.scrape_date
                AND b.frontage_m > a.frontage_m
          )
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )

    assert cursor.fetchall() == []


def test_a_corner_lot_gets_both_of_its_streets_named_correctly(connection, measured):
    """The naming rule, and why it is restricted to the road lot's own sides.

    Lot 3 790 549 is a corner parcel: 31.2 m on Jarry through one road lot and
    13.1 m on Chabot through another. Labelling each shared edge with the
    nearest geobase side *anywhere* puts Chabot on both of them, because the
    Chabot side is closer to the Jarry edge than the Jarry side is. Restricting
    the candidates to the sides running inside the road lot the edge came from
    is what gets it right, and it costs nothing: a road lot always has at least
    one side inside it, because that is what made it a road lot.
    """
    rows = frontages_for(connection, "3 790 549")

    assert [(row[1], round(row[2], 1)) for row in rows] == [
        ("Jarry", 31.2),
        ("Chabot", 13.1),
    ]


def test_a_lot_meeting_one_street_through_two_road_lots_gets_one_row(
    connection, measured
):
    """The cadastre cuts a roadway at every intersection; a frontage is not cut.

    A lot running along a block can share boundary with two parcels of the same
    street. That is one frontage on one street side, and the rows are grouped
    back to (lot, cote_rue_id) - which is also this table's primary key, so
    anything else would be a constraint violation rather than a wrong number.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT lot_uid, cote_rue_id, count(*)
        FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date
        GROUP BY lot_uid, cote_rue_id
        HAVING count(*) > 1
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )

    assert cursor.fetchall() == []


def test_a_lot_is_never_measured_against_both_sides_of_one_street(
    connection, measured
):
    """Chabot's two sides are 11.5 m apart, well inside the old cutoff.

    Under the buffer clip nothing but "nearest side wins" kept a lot off the
    far kerb, so it was worth an assertion of its own: the sides of one roadway
    share an ID_TRC, and a cote_rue_id is that ID with the side appended, so
    two rows differing only in their last digit are a lot measured against both
    kerbs of one street.

    A shared boundary cannot reach across a roadway at all, and the road lots
    that used to be the awkward case here - a parcel that *is* the street does
    touch both of its own sides - no longer produce rows. So this should now be
    empty without an exclusion, which is a stronger statement than it was.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT lot_number, array_agg(cote_rue_id ORDER BY cote_rue_id)
        FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date
        GROUP BY lot_number, left(cote_rue_id, length(cote_rue_id) - 1)
        HAVING count(*) > 1
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )
    both_sides = cursor.fetchall()

    assert not both_sides, f"measured against both sides of one street: {both_sides}"
