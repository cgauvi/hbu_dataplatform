"""`compute_lot_frontage` against a real PostGIS and real Montreal geometry.

The unit tests stub the database out, which is the right call for the asset's
plumbing and no help at all for the measure: a frontage query that matched
nothing would pass every one of them. This module measures.

The subject is lot **3 790 556**, a 476.1 m2 parcel on avenue Chabot in
Villeray. It is a clean rectangle, 15.24 m of street edge by 31.24 m deep, and
its front boundary sits 3.48 m behind cote_rue_id 11000531 - the "Droite" side
of Chabot between civic 7905 and 8097. Two independent publishers agree on its
size: Infolot draws the polygon and carries 476.1269 m2 as an attribute, and
the 2026 assessment roll records 476.1 m2 of land against the same lot number.

That 3.48 m gap is the bug this module was written for. `buffer_m` defaulted to
3.0, the lot line sits further back than that, and so this lot - along with
90 % of the borough - matched no street at all. See
`test_the_old_three_metre_cutoff_is_what_lost_the_lot`.

Run them with a throwaway PostGIS; see conftest.py for the one-liner.
"""

from __future__ import annotations

import pytest

from conftest import NEIGHBORHOOD, SCRAPE_DATE

from urban_rag.postgis import DEFAULT_FRONTAGE_BUFFER_M, compute_lot_frontage

#: The lot every assertion here is about, and the street side it fronts on.
LOT_NUMBER = "3 790 556"
COTE_RUE_ID = "11000531"
STREET_NAME = "Chabot"

#: Its front boundary, in metres. The lot is a rectangle whose street edge
#: measures 15.238 m in EPSG:32188 and whose two side edges measure 31.237 m;
#: the measure recovers the first and, being an assignment over 1 m pieces,
#: is exact to well under that. Not a value read off a previous run: the same
#: 15.24 m falls out of the geometry with shapely, and 15.238 m is twice the
#: 7.619 m that the single-width lots either side of it measure.
FRONTAGE_M = 15.24

#: How far the front boundary sits behind the street side. The number that
#: makes a 3 m cutoff miss this lot and a 10 m one find it.
SETBACK_M = 3.48


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
            buffer_m=DEFAULT_FRONTAGE_BUFFER_M,
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


def lots_without_frontage(connection):
    """The lot numbers that got no row at all - the ones to be suspicious of."""
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT l.lot_number
        FROM rag.lots l
        WHERE l.neighborhood = %s AND l.scrape_date = %s::date
          AND NOT EXISTS (
              SELECT 1 FROM silver.lot_frontage f
              WHERE f.lot_uid = l.lot_uid
                AND f.neighborhood = l.neighborhood
                AND f.scrape_date = l.scrape_date
          )
        ORDER BY l.lot_number
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )
    return [row[0] for row in cursor.fetchall()]


# -- the lot ---------------------------------------------------------------


def test_the_lot_fronts_on_exactly_one_street(connection, measured):
    """It is a mid-block parcel, so it has one street and not two.

    A second row here would mean the measure had reached across Chabot to its
    other side (cote_rue_id 11000532, 11.5 m away) or picked up the lane
    behind - both of which a cutoff wide enough to reach the lot at all can do,
    and neither of which is a frontage.
    """
    rows = frontages_for(connection, LOT_NUMBER)

    assert [row[0] for row in rows] == [COTE_RUE_ID]
    assert rows[0][1] == STREET_NAME
    assert rows[0][5] == 1


def test_the_frontage_is_the_lots_street_edge(connection, measured):
    """15.24 m: the front boundary, and nothing else.

    The number to watch is not just its size but its stability - see
    `test_the_measure_does_not_grow_with_the_cutoff`. Under the buffer clip
    this replaced, the same lot measured 20.3 m at a 6 m buffer and 32.3 m at
    12 m, because the buffer swallowed the first `buffer_m` of both side
    boundaries.
    """
    (row,) = frontages_for(connection, LOT_NUMBER)
    _, _, frontage_m, perimeter_m, pct, _, buffer_m, _ = row

    assert frontage_m == pytest.approx(FRONTAGE_M, abs=0.05)
    # 2 * (15.238 + 31.237)
    assert perimeter_m == pytest.approx(92.95, abs=0.1)
    assert pct == pytest.approx(100.0 * frontage_m / perimeter_m, abs=0.01)
    # The cutoff a row was measured under travels on the row.
    assert buffer_m == DEFAULT_FRONTAGE_BUFFER_M


def test_the_frontage_geometry_is_the_facing_boundary(connection, measured):
    """Linework, and only as much of the lot's edge as faces the street."""
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


@pytest.mark.parametrize("buffer_m", [6.0, 8.0, 10.0, 12.0])
def test_the_measure_does_not_grow_with_the_cutoff(connection, loaded, buffer_m):
    """The property the buffer clip did not have, and the reason for the fix.

    `buffer_m` decides which lots are near enough to a street to be measured.
    It must not decide what they then measure - once it is wide enough to reach
    this lot, the answer is the lot's front edge whatever it is set to. Without
    this, widening the cutoff to reach the borough's lots would have inflated
    every frontage in it by twice the widening.
    """
    with connection.transaction():
        compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            buffer_m=buffer_m,
        )

    rows = frontages_for(connection, LOT_NUMBER)

    assert [row[0] for row in rows] == [COTE_RUE_ID]
    assert rows[0][2] == pytest.approx(FRONTAGE_M, abs=0.05)


def test_the_old_three_metre_cutoff_is_what_lost_the_lot(connection, loaded):
    """The bug, pinned so it cannot come back as a default.

    The lot's front boundary sits 3.48 m behind cote_rue_id 11000531, so a 3 m
    cutoff cannot see it - and could not see 90 % of the borough either. This
    asserts the symptom rather than the fix: if someone lowers the default back
    under the setback, the test above stops finding the lot and this one
    explains why.
    """
    with connection.transaction():
        result = compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            buffer_m=3.0,
        )

    assert frontages_for(connection, LOT_NUMBER) == []
    # And it is not this one lot: most of the slice goes missing with it.
    assert result["lots_matched"] < 0.2 * result["num_lots"]
    assert DEFAULT_FRONTAGE_BUFFER_M > SETBACK_M


# -- every lot -------------------------------------------------------------


def test_every_lot_faces_at_least_one_street(connection, measured):
    """A lot in a Montreal borough that faces no street is a finding.

    Some are real - a landlocked remnant, the inside of a block - but a lot
    with no frontage is far more often the measure failing to reach it, which
    is exactly what a too-narrow cutoff looks like and exactly what it looked
    like here. Every lot in this slice has street on at least one side, so the
    assertion is flat rather than a tolerance; the lot numbers are in the
    message because "12 lots failed" is not something anyone can act on.
    """
    flagged = lots_without_frontage(connection)

    assert not flagged, (
        f"{len(flagged)} of {measured['num_lots']} lot(s) face no street side "
        f"within {DEFAULT_FRONTAGE_BUFFER_M} m and are potentially "
        f"problematic: {', '.join(flagged)}"
    )


def test_the_run_reports_the_lots_it_could_not_place(connection, loaded):
    """Whatever the count, the caller is handed the lot numbers behind it.

    Run deliberately too tight, so there is something to report. This is what
    `frontage_assets` turns into a warning and into the
    `lots_without_frontage` metadata: a share on its own says a partition is
    bad, and the numbers say which rows to go and look at.
    """
    with connection.transaction():
        result = compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            buffer_m=3.0,
        )

    sample = result["lots_without_frontage"]
    every = lots_without_frontage(connection)

    assert sample, "a 3 m cutoff leaves most of the slice unmatched"
    # The subject of this module is one of them, at that cutoff.
    assert LOT_NUMBER in every
    # `sample` is capped and ordered - see _LOTS_WITHOUT_FRONTAGE_SAMPLE - so
    # it is the head of the real set rather than all of it. It is a place to
    # start looking; the count is what says how much there is to look at.
    assert sample == every[: len(sample)]
    assert len(sample) <= result["num_lots"] - result["lots_matched"]
    assert len(every) == result["num_lots"] - result["lots_matched"]


def test_no_lot_is_given_more_frontage_than_it_has_boundary(connection, measured):
    """The invariant the buffer clip broke.

    Frontage is a length along the parcel's edge, so a lot's frontages cannot
    add up to more edge than the parcel has. Under the old measure they could
    and did: each street side contributed the first `buffer_m` of the lot's
    side boundaries on top of the real edge, and a corner lot counted the same
    metres twice.
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


def test_a_lot_is_never_measured_against_both_sides_of_one_street(
    connection, measured
):
    """Chabot's two sides are 11.5 m apart, well inside the cutoff.

    Nothing but "nearest side wins" keeps a lot off the far one, so it is worth
    an assertion of its own: the sides of one roadway share an ID_TRC, and a
    cote_rue_id is that ID with the side appended, so two rows that differ only
    in their last digit are a lot measured against both kerbs of one street.

    Excluding the parcels the street runs *through*. Infolot draws the
    right-of-way itself as a lot - 3 946 200 is the strip of Chabot the subject
    of this module fronts on - and a road parcel touching both of its own sides
    is the correct answer, not a leak. Every lot caught here before this
    exclusion was one of those, and no ordinary parcel was.
    """
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT lot_number, array_agg(cote_rue_id ORDER BY cote_rue_id)
        FROM silver.lot_frontage f
        WHERE f.neighborhood = %s AND f.scrape_date = %s::date
          AND NOT EXISTS (
              SELECT 1
              FROM rag.lots l
              JOIN silver.neighborhood_streets s
                ON s.neighborhood = l.neighborhood
               AND s.scrape_date = l.scrape_date
               AND ST_Intersects(l.geom, s.geom)
              WHERE l.lot_uid = f.lot_uid
          )
        GROUP BY lot_number, left(cote_rue_id, length(cote_rue_id) - 1)
        HAVING count(*) > 1
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )
    both_sides = cursor.fetchall()

    assert not both_sides, f"measured against both sides of one street: {both_sides}"
