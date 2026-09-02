"""`compute_lot_buildable_setbacks` against a real PostGIS and real geometry.

The unit tests stub the database out, which is right for the asset's plumbing
and no help at all for the measure: a boundary sort that put every edge in the
same class would pass every one of them. This module measures.

The subject is the same parcel `test_lot_frontage.py` uses - lot **3 790 556**
on avenue Chabot in Villeray - and it is here for the same reason it is there:
it is a clean rectangle, 15.238 m of street edge by 31.237 m deep, 476.1 m2,
and two independent publishers agree on its size. A rectangle is what makes
every number below arrivable at by hand, which is the point. It is *not* what
the code assumes: nothing here estimates a depth or multiplies a width by one,
and the same statement run on the wedge next door would sort its boundary the
same way and difference the same four buffers out of it. The rectangle is the
fixture, not the model.

What it lets us state exactly, at *Avant* 3 m, *Arrière* 3 m, *Latérale* 1.5 m:

    front strip     y <= 3           over the full 15.238 m width
    rear strip      y >= 28.237      likewise
    side strips     x <= 1.5 and x >= 13.738, over the full 31.237 m depth
    what is left    12.238 x 25.237 = 308.85 m2

and, because *Mode d'implantation* decides whether the side strips are taken
at all, three answers off one grid:

    ============  =================  ==========  ===========
    mode          side setback       width left  buildable
    ============  =================  ==========  ===========
    isolé (I)     1.5 m both sides   12.238 m    308.85 m2
    jumelé (J)    0.75 m both sides  13.738 m    346.71 m2
    contigu (C)   none               15.238 m    384.56 m2
    ============  =================  ==========  ===========

The gap between the first and the last is 75.7 m2 on one 476 m2 parcel - a
quarter of the buildable area - which is why the mode is read rather than the
*Latérale* row taken at face value, and why the two tests at the foot of this
module are the ones to look at first when a borough's numbers seem low.

Run them with a throwaway PostGIS; see conftest.py for the one-liner.
"""

from __future__ import annotations

import pytest

from conftest import NEIGHBORHOOD, SCRAPE_DATE

from urban_rag.postgis import (
    DEFAULT_ROAD_LOT_MIN_STREET_M,
    compute_lot_buildable_setbacks,
    compute_lot_frontage,
)

#: The lot every assertion here is about. A rectangle, so the areas below are
#: arithmetic rather than numbers read off a previous run.
LOT_NUMBER = "3 790 556"
WIDTH_M = 15.238
DEPTH_M = 31.237

#: The zone the fixture puts it in, and the one grid column that governs it.
FEATURE_ID = "H01-999"
COLUMN_INDEX = 0

#: The margins the fixture grid states, in metres.
FRONT_M = 3.0
REAR_M = 3.0
SIDE_M = 1.5

#: What is left under each reading of *Mode d'implantation*. Computed here the
#: way a person would, so a failure says which of the three rules moved rather
#: than only that a number changed.
DEPTH_LEFT_M = DEPTH_M - FRONT_M - REAR_M
BUILDABLE_ISOLE_M2 = (WIDTH_M - 2 * SIDE_M) * DEPTH_LEFT_M
BUILDABLE_JUMELE_M2 = (WIDTH_M - SIDE_M) * DEPTH_LEFT_M
BUILDABLE_CONTIGU_M2 = WIDTH_M * DEPTH_LEFT_M

#: How far off the hand figure a measured area may sit. The boundary is sorted
#: in 1 m pieces and the front edge is the one `lot_frontage` measured, so the
#: two ends of each strip are right to well under a metre; 3 m2 is about a
#: tenth of a metre of width on this parcel.
TOLERANCE_M2 = 3.0


@pytest.fixture
def zoned(connection, loaded):
    """One grid column over every lot in the slice, with a mode to override.

    Written straight into `silver.lot_zoning_envelopes` rather than through
    `lot_zoning_envelopes`: that asset parses PDFs, and what is being tested
    here is the subtraction, not the parse. The columns filled in are the ones
    `compute_lot_buildable_setbacks` reads and no others.
    """

    def load(implantation_mode, *, site_coverage_max_pct=None):
        cursor = connection.cursor()
        with connection.transaction():
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS "
                f"silver.lot_zoning_envelopes_{NEIGHBORHOOD.lower()} "
                f"PARTITION OF silver.lot_zoning_envelopes "
                f"FOR VALUES IN ('{NEIGHBORHOOD}')"
            )
            cursor.execute(
                "DELETE FROM silver.lot_zoning_envelopes "
                "WHERE neighborhood = %s AND scrape_date = %s::date",
                [NEIGHBORHOOD, SCRAPE_DATE],
            )
            cursor.execute(
                """
                INSERT INTO silver.lot_zoning_envelopes
                    (scrape_date, neighborhood, lot_uid, feature_id,
                     column_index, lot_number, source_table, lot_area_m2,
                     pct_of_lot, implantation_mode, site_coverage_max_pct,
                     front_margin_min_m, rear_margin_min_m, side_margin_min_m,
                     permits_residential, governs_residential, solver_ready)
                SELECT %s::date, %s, l.lot_uid, %s, %s, l.lot_number,
                       'zonage', l.area_m2, 100.0, %s, %s, %s, %s, %s,
                       true, true, true
                  FROM rag.lots l
                 WHERE l.neighborhood = %s AND l.scrape_date = %s::date
                """,
                [
                    SCRAPE_DATE, NEIGHBORHOOD, FEATURE_ID, COLUMN_INDEX,
                    implantation_mode, site_coverage_max_pct,
                    FRONT_M, REAR_M, SIDE_M,
                    NEIGHBORHOOD, SCRAPE_DATE,
                ],
            )

    return load


@pytest.fixture
def measured(connection, loaded, zoned):
    """Frontage, then setbacks, over the fixture slice at one mode.

    Per test rather than per module: several tests below re-run at other modes,
    and `silver.lot_buildable_setbacks` holds one partition, so a shared run
    would leave whichever test went last deciding what the others read. The
    slice is 164 lots, so re-running it is cheap.
    """

    def run(implantation_mode, **kwargs):
        zoned(implantation_mode, **kwargs)
        with connection.transaction():
            compute_lot_frontage(
                connection,
                neighborhood=NEIGHBORHOOD,
                scrape_date=SCRAPE_DATE,
                min_street_m=DEFAULT_ROAD_LOT_MIN_STREET_M,
            )
        with connection.transaction():
            return compute_lot_buildable_setbacks(
                connection, neighborhood=NEIGHBORHOOD, scrape_date=SCRAPE_DATE
            )

    return run


def row_for(connection, lot_number):
    """The one `silver.lot_buildable_setbacks` row for a lot."""
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT buildable_area_m2, buildable_pct_of_lot, lot_area_m2,
               side_setback_rule, side_setback_m, front_setback_m,
               rear_setback_m, front_edge_m, side_edge_m, rear_edge_m,
               footprint_cap_m2, footprint_cap_binding, coverage_cap_m2,
               ST_GeometryType(geom)
        FROM silver.lot_buildable_setbacks
        WHERE neighborhood = %s AND scrape_date = %s::date AND lot_number = %s
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, lot_number],
    )
    rows = cursor.fetchall()
    assert len(rows) == 1, f"expected one row for {lot_number}, got {len(rows)}"
    keys = (
        "buildable_area_m2", "buildable_pct_of_lot", "lot_area_m2",
        "side_setback_rule", "side_setback_m", "front_setback_m",
        "rear_setback_m", "front_edge_m", "side_edge_m", "rear_edge_m",
        "footprint_cap_m2", "footprint_cap_binding", "coverage_cap_m2",
        "geometry_type",
    )
    return dict(zip(keys, rows[0]))


# -- the boundary sort ------------------------------------------------------


def test_the_boundary_sorts_into_a_front_two_sides_and_a_rear(
    connection, measured
):
    """The sort is what everything else rests on, so it is asserted directly
    rather than only through the area it produces.

    On this rectangle the four classes have known lengths: one 15.238 m street
    edge, one 15.238 m rear edge parallel to it, and two 31.237 m sides. The
    front is what `lot_frontage` measured; the other two classes are this
    function's own work.
    """
    measured("I")

    row = row_for(connection, LOT_NUMBER)

    assert row["front_edge_m"] == pytest.approx(WIDTH_M, abs=0.2)
    assert row["rear_edge_m"] == pytest.approx(WIDTH_M, abs=0.5)
    # Both sides, and the 5 cm of each that the frontage tolerance takes off
    # at the front corner.
    assert row["side_edge_m"] == pytest.approx(2 * DEPTH_M, abs=1.0)


def test_the_sides_are_not_counted_as_rear(connection, measured):
    """The whole point of the angle test. A side line runs at the street and a
    rear line runs along it, and if the two were not separated the *Arrière*
    margin would be taken off all four edges."""
    measured("I")

    row = row_for(connection, LOT_NUMBER)

    assert row["side_edge_m"] > row["rear_edge_m"] * 3, (
        "the two 31 m sides did not sort apart from the 15 m rear"
    )


# -- the subtraction --------------------------------------------------------


def test_the_margins_come_off_the_edges_they_govern(connection, measured):
    """308.85 m2 of a 476.1 m2 parcel, which is the rectangle arithmetic in
    this module's docstring and is arrived at here by sorting a real boundary
    and differencing four buffers out of a real polygon."""
    measured("I")

    row = row_for(connection, LOT_NUMBER)

    assert row["buildable_area_m2"] == pytest.approx(
        BUILDABLE_ISOLE_M2, abs=TOLERANCE_M2
    )
    assert row["buildable_pct_of_lot"] == pytest.approx(
        100.0 * BUILDABLE_ISOLE_M2 / row["lot_area_m2"], abs=1.0
    )
    assert row["geometry_type"] == "ST_MultiPolygon"


def test_the_setbacks_actually_applied_travel_on_the_row(connection, measured):
    """A number that depends on four distances is unreadable without them, so
    each is a column - the rule `lot_frontage.buffer_m` follows."""
    measured("I")

    row = row_for(connection, LOT_NUMBER)

    assert row["front_setback_m"] == FRONT_M
    assert row["rear_setback_m"] == REAR_M
    assert row["side_setback_m"] == SIDE_M
    assert row["side_setback_rule"] == "isole"


# -- mode d'implantation ----------------------------------------------------


def test_a_contiguous_column_takes_no_side_setback(connection, measured):
    """The one that matters most in a borough of plexes: 384.56 m2 against
    308.85, a quarter more buildable area off the same grid."""
    measured("I-J-C")

    row = row_for(connection, LOT_NUMBER)

    assert row["side_setback_rule"] == "contigu"
    assert row["side_setback_m"] == 0.0
    assert row["buildable_area_m2"] == pytest.approx(
        BUILDABLE_CONTIGU_M2, abs=TOLERANCE_M2
    )


def test_a_semi_detached_column_takes_one_margin_across_two_sides(
    connection, measured
):
    """Half off each side rather than the whole off one, because which line
    carries the party wall is a fact about the neighbour nothing here
    publishes - and for a parcel with parallel sides the two remove exactly the
    same area. 346.71 m2, halfway between the other two."""
    measured("I-J")

    row = row_for(connection, LOT_NUMBER)

    assert row["side_setback_rule"] == "jumele"
    assert row["side_setback_m"] == SIDE_M / 2
    assert row["buildable_area_m2"] == pytest.approx(
        BUILDABLE_JUMELE_M2, abs=TOLERANCE_M2
    )


def test_the_most_permissive_mode_the_column_allows_is_the_one_applied(
    connection, measured
):
    """`I-J-C` permits the contiguous form, and this table answers what *may*
    be built. Reading it as isolated because that letter comes first would
    understate every lot in the zone."""
    measured("I-J-C")
    contiguous = row_for(connection, LOT_NUMBER)["buildable_area_m2"]

    measured("I")
    isolated = row_for(connection, LOT_NUMBER)["buildable_area_m2"]

    # The difference is exactly the two side strips: 1.5 m x 2 x 25.237 m.
    assert contiguous - isolated == pytest.approx(
        2 * SIDE_M * DEPTH_LEFT_M, abs=TOLERANCE_M2
    )


def test_a_column_stating_no_mode_takes_the_full_margin_on_both_sides(
    connection, measured
):
    """The conservative reading: a grid that says nothing must not hand a lot
    more room than one that says `isolé`."""
    measured(None)

    row = row_for(connection, LOT_NUMBER)

    assert row["side_setback_rule"] == "unknown"
    assert row["side_setback_m"] == SIDE_M
    assert row["buildable_area_m2"] == pytest.approx(
        BUILDABLE_ISOLE_M2, abs=TOLERANCE_M2
    )


def test_the_mode_is_read_from_words_as_well_as_from_letter_codes(
    connection, measured
):
    """VSMPE prints `I-J-C`; another borough's template spells the modes out.
    Both have to reach the same rule, which is why the SQL tests the stems as
    well as the codes."""
    measured("Isolé, jumelé, contigu")

    assert row_for(connection, LOT_NUMBER)["side_setback_rule"] == "contigu"


# -- the second cap ---------------------------------------------------------


def test_the_coverage_cap_binds_when_it_is_tighter_than_the_margins(
    connection, measured
):
    """*Taux d'implantation* at 50 per cent of 476.1 m2 is 238 m2, under the
    308.85 the margins leave - so the footprint a building may take is the
    coverage, and the row says so."""
    measured("I", site_coverage_max_pct=50.0)

    row = row_for(connection, LOT_NUMBER)

    assert row["coverage_cap_m2"] == pytest.approx(
        0.5 * row["lot_area_m2"], abs=0.1
    )
    assert row["footprint_cap_m2"] == pytest.approx(row["coverage_cap_m2"])
    assert row["footprint_cap_binding"] == "site_coverage"


def test_the_margins_bind_when_the_coverage_is_the_looser_of_the_two(
    connection, measured
):
    """At 90 per cent - 428 m2 - the coverage allows more than the margins
    leave, so the buildable envelope is the cap and the shape is what limits
    the building."""
    measured("I", site_coverage_max_pct=90.0)

    row = row_for(connection, LOT_NUMBER)

    assert row["footprint_cap_m2"] == pytest.approx(
        row["buildable_area_m2"], abs=0.1
    )
    assert row["footprint_cap_binding"] == "setbacks"


def test_a_column_with_no_coverage_maximum_leaves_the_margins_standing(
    connection, measured
):
    """LEAST() ignores a NULL argument, which would silently drop the
    comparison a reader thinks happened. The CASE in the statement is there to
    stop that, and this is what it is for."""
    measured("I", site_coverage_max_pct=None)

    row = row_for(connection, LOT_NUMBER)

    assert row["coverage_cap_m2"] is None
    assert row["footprint_cap_m2"] == pytest.approx(row["buildable_area_m2"])
    assert row["footprint_cap_binding"] == "setbacks"


# -- the borough, not the parcel -------------------------------------------


def test_every_lot_with_a_frontage_and_an_envelope_gets_a_row(
    connection, measured
):
    """A lot with no frontage row has no front edge to sort against and is
    absent by design, so the two counts the caller gets back are what say
    whether that is a handful of interior parcels or a broken partition."""
    result = measured("I")

    assert result["num_lots_measured"] == result["num_lots_sorted"]
    assert result["num_lots_sorted"] > 0
    assert result["lots_without_frontage"] == (
        result["num_lots"] - result["num_lots_sorted"]
    )


def test_no_lot_is_handed_more_buildable_area_than_it_has(connection, measured):
    """The invariant that catches a buffer subtracted in the wrong direction:
    whatever the margins are, what is left is part of the parcel."""
    measured("I-J-C")

    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT count(*)
        FROM silver.lot_buildable_setbacks
        WHERE neighborhood = %s AND scrape_date = %s::date
          AND buildable_area_m2 > lot_area_m2 + 0.01
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )
    (over,) = cursor.fetchone()
    assert over == 0
