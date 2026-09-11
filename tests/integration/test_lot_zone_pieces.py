"""`compute_lot_zone_pieces` against a real PostGIS and real geometry.

The unit tests hand `lot_zoning_envelopes` a piece table already built, which
is right for the join it makes and no help at all for the cut itself: a clip
that returned the whole parcel every time would pass every one of them. This
module cuts.

The subject is the parcel the other two integration modules use - lot
**3 790 556** on avenue Chabot in Villeray - and it is here for the reason it
is there: a clean rectangle, 15.238 m of street edge by 31.237 m deep, 476.1 m2,
on which every number below is arrivable at by hand. A zoning boundary is drawn
across it at a stated depth, which is the whole fixture: the platform's real
problem is a boundary that does not follow a lot line, and the cheapest honest
way to test what happens then is to draw one.

What that lets us state exactly, at a boundary 9 m back from the street:

    front piece   15.238 x 9      = 137.1 m2, and it holds the street edge
    rear piece    15.238 x 22.237 = 338.8 m2, and it holds none

which is the arrangement the whole grain exists for - a commercial strip on a
boulevard with housing behind it - in miniature and to the centimetre. The
front piece is 28.8 % of the parcel and gets 100 % of its frontage; the rear
piece is 71.2 % of the parcel and fronts nothing. Under the old grain both
would have carried 476.1 m2 and 15.238 m of Chabot.

Run them with a throwaway PostGIS; see conftest.py for the one-liner.
"""

from __future__ import annotations

import pytest
from shapely.geometry import box
from shapely.ops import transform

from conftest import (
    NEIGHBORHOOD,
    SCRAPE_DATE,
    ZONE_SOURCE_TABLE,
    load_zone_clips,
    whole_lot_clips,
)

from urban_rag.postgis import (
    DEFAULT_ROAD_LOT_MIN_STREET_M,
    MIN_ZONE_PIECE_AREA_M2,
    compute_lot_frontage,
    compute_lot_zone_pieces,
)

#: The lot every assertion here is about, and its measurements.
LOT_NUMBER = "3 790 556"
LOT_AREA_M2 = 476.1
WIDTH_M = 15.238
DEPTH_M = 31.237

#: How far back from the street the fixture draws the zoning boundary. 9 m is
#: chosen so neither piece is near a cutoff - the front comes out at about a
#: fifth of the parcel - so a test that fails here has failed on the *cut* and
#: not on a threshold.
#:
#: The pieces are **not** ``width x depth``, and it is worth saying why rather
#: than quietly using a tolerance wide enough to hide it: lot 3 790 556 is a
#: rectangle but it is not axis-aligned in MTM zone 8, so a horizontal cut
#: through its bounding box takes a slanted corner off it. That is the right
#: fixture for this module - a zoning boundary has no reason to run parallel to
#: a lot line, which is the entire premise of the piece grain - and it is why
#: every area assertion below is taken against the shape the test itself cut
#: rather than against a number multiplied out here.
BOUNDARY_DEPTH_M = 9.0

FRONT_ZONE = "C01-777"
REAR_ZONE = "H01-999"

#: How far a measured area may sit from the shapely area of the same clip. The
#: only slack is the round trip through EPSG:4326 that `silver.lot_features`
#: stores the clip in.
TOLERANCE_M2 = 0.5

#: Where the clips are cut. The same MTM zone 8 every metric operation in
#: `urban_rag.postgis` uses, so a metre here is the metre the function measures.
METRIC_SRID = 32188


@pytest.fixture(scope="module")
def frontage(connection, loaded):
    """`silver.lot_frontage` over the slice, which the pieces re-rank inside.

    Module-scoped: the frontage does not depend on how the zones are drawn, and
    it is the slow half of the setup.
    """
    with connection.transaction():
        return compute_lot_frontage(
            connection,
            neighborhood=NEIGHBORHOOD,
            scrape_date=SCRAPE_DATE,
            min_street_m=DEFAULT_ROAD_LOT_MIN_STREET_M,
        )


def lot_geometry(connection, lot_number: str):
    """One parcel, in metres, as a shapely polygon.

    Read back out of `rag.lots` rather than out of the fixture parquet so the
    geometry a test cuts is the geometry the function will cut.
    """
    import shapely.wkb

    cursor = connection.cursor()
    cursor.execute(
        f"""
        SELECT ST_AsBinary(ST_Transform(geom, {METRIC_SRID}))
          FROM rag.lots
         WHERE neighborhood = %s AND scrape_date = %s::date AND lot_number = %s
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, lot_number],
    )
    (wkb,) = cursor.fetchone()
    return shapely.wkb.loads(bytes(wkb))


def to_wgs84(connection, geometry):
    """A metric shape back in EPSG:4326, which is what `lot_features` stores."""
    import shapely.wkb

    cursor = connection.cursor()
    cursor.execute(
        f"SELECT ST_AsBinary(ST_Transform("
        f"ST_GeomFromWKB(decode(%s, 'hex'), {METRIC_SRID}), 4326))",
        [geometry.wkb_hex],
    )
    (wkb,) = cursor.fetchone()
    return shapely.wkb.loads(bytes(wkb))


def split_at(connection, lot_number: str, depth_m: float):
    """The parcel cut by a line ``depth_m`` back from its street edge.

    Returns ``(front, rear)`` as metric shapes, front being the piece holding
    the lower-y edge - which on this parcel is the side avenue Chabot runs
    along, so "front" and "rear" mean what they say rather than merely "one
    part and the other". The cut is a straight horizontal line through the
    bounding box and the parcel is not aligned to it, so the front piece is a
    trapezoid; see `BOUNDARY_DEPTH_M` on why that is the fixture rather than an
    accident of it.
    """
    parcel = lot_geometry(connection, lot_number)
    minx, miny, maxx, maxy = parcel.bounds
    cut = miny + depth_m
    front = parcel.intersection(box(minx - 1, miny - 1, maxx + 1, cut))
    rear = parcel.intersection(box(minx - 1, cut, maxx + 1, maxy + 1))
    return front, rear


@pytest.fixture
def split(connection, loaded, frontage):
    """The subject parcel cut in two, and the pieces computed from it.

    Returns ``(result, expected)`` - the run's own metadata, and the two areas
    the test just cut, in square metres and keyed by zone. The expected areas
    come from shapely rather than from a constant so an assertion is about what
    `compute_lot_zone_pieces` did with a stated clip, not about whether the
    fixture author multiplied the right two numbers.

    Every other lot in the slice is given one whole-lot clip first, so the
    borough around the subject is the unsplit case and the counts a test reads
    are about the one parcel it drew a boundary across.
    """

    def run(depth_m=BOUNDARY_DEPTH_M, **kwargs):
        whole_lot_clips(connection, SCRAPE_DATE, REAR_ZONE)
        front, rear = split_at(connection, LOT_NUMBER, depth_m)
        # `load_zone_clips` replaces the partition, so the whole-lot clips for
        # the *other* lots have to be restated alongside the two being drawn.
        # Read back rather than recomputed: what the other lots are zoned as
        # is not what any test here is about.
        others = other_lot_clips(connection)
        load_zone_clips(
            connection,
            SCRAPE_DATE,
            [
                *others,
                (LOT_NUMBER, FRONT_ZONE, to_wgs84(connection, front)),
                (LOT_NUMBER, REAR_ZONE, to_wgs84(connection, rear)),
            ],
        )
        with connection.transaction():
            result = compute_lot_zone_pieces(
                connection,
                neighborhood=NEIGHBORHOOD,
                scrape_date=SCRAPE_DATE,
                zone_sources=(ZONE_SOURCE_TABLE,),
                **kwargs,
            )
        return result, {FRONT_ZONE: front.area, REAR_ZONE: rear.area}

    return run


def other_lot_clips(connection):
    """Whole-lot clips for every parcel in the slice but the subject."""
    import shapely.wkb

    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT lot_number, ST_AsBinary(geom)
          FROM rag.lots
         WHERE neighborhood = %s AND scrape_date = %s::date
           AND lot_number <> %s
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, LOT_NUMBER],
    )
    return [
        (lot_number, REAR_ZONE, shapely.wkb.loads(bytes(wkb)))
        for lot_number, wkb in cursor.fetchall()
    ]


def pieces_for(connection, lot_number: str) -> dict:
    """The `silver.lot_zone_pieces` rows for a lot, keyed by zone."""
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT feature_id, piece_area_m2, lot_area_m2, pct_of_lot,
               num_lot_zones, zone_rank, is_primary_zone,
               primary_frontage_m, primary_street_name, num_frontages,
               existing_footprint_m2, area_share, footprint_share,
               footprint_share_basis, ST_GeometryType(geom)
          FROM silver.lot_zone_pieces
         WHERE neighborhood = %s AND scrape_date = %s::date AND lot_number = %s
        """,
        [NEIGHBORHOOD, SCRAPE_DATE, lot_number],
    )
    names = (
        "piece_area_m2", "lot_area_m2", "pct_of_lot", "num_lot_zones",
        "zone_rank", "is_primary_zone", "primary_frontage_m",
        "primary_street_name", "num_frontages", "existing_footprint_m2",
        "area_share", "footprint_share", "footprint_share_basis", "geom_type",
    )
    return {
        row[0]: dict(zip(names, row[1:]))
        for row in cursor.fetchall()
    }


# -- the cut ----------------------------------------------------------------


def test_a_boundary_across_a_lot_makes_two_pieces(connection, split):
    """The whole point: one parcel, two rows, two areas that sum to it."""
    _result, expected = split()
    pieces = pieces_for(connection, LOT_NUMBER)

    assert set(pieces) == {FRONT_ZONE, REAR_ZONE}
    assert pieces[FRONT_ZONE]["piece_area_m2"] == pytest.approx(
        expected[FRONT_ZONE], abs=TOLERANCE_M2
    )
    assert pieces[REAR_ZONE]["piece_area_m2"] == pytest.approx(
        expected[REAR_ZONE], abs=TOLERANCE_M2
    )
    total = sum(piece["piece_area_m2"] for piece in pieces.values())
    assert total == pytest.approx(LOT_AREA_M2, abs=TOLERANCE_M2)
    # And the parcel is on both rows, unchanged, so a reader can tell a piece
    # from the lot it is a piece of without a join.
    for piece in pieces.values():
        assert piece["lot_area_m2"] == pytest.approx(LOT_AREA_M2, abs=TOLERANCE_M2)
        assert piece["num_lot_zones"] == 2


def test_the_larger_piece_is_the_primary_one(connection, split):
    """`is_primary_zone` is what a reader wanting one row per lot takes."""
    split()
    pieces = pieces_for(connection, LOT_NUMBER)

    assert pieces[REAR_ZONE]["is_primary_zone"]
    assert pieces[REAR_ZONE]["zone_rank"] == 1
    assert not pieces[FRONT_ZONE]["is_primary_zone"]
    assert pieces[FRONT_ZONE]["zone_rank"] == 2


def test_an_unsplit_lot_is_one_piece_that_is_its_own_parcel(connection, split):
    """The case almost every lot in a borough is, and the one to not break."""
    split()
    neighbour = next(
        lot_number
        for lot_number, _zone, _geom in other_lot_clips(connection)
    )
    pieces = pieces_for(connection, neighbour)

    assert len(pieces) == 1
    piece = pieces[REAR_ZONE]
    assert piece["num_lot_zones"] == 1
    assert piece["is_primary_zone"]
    assert piece["pct_of_lot"] == pytest.approx(100.0, abs=0.5)
    assert piece["piece_area_m2"] == pytest.approx(
        piece["lot_area_m2"], rel=0.005
    )
    # A share of 1 means "the whole of the roll", which is what one zone
    # covering a parcel whole has to divide out to.
    assert piece["area_share"] == pytest.approx(1.0)
    assert piece["footprint_share"] == pytest.approx(1.0)


def test_a_piece_is_typed_multipolygon_even_when_it_is_one(connection, split):
    """The column is MultiPolygon because a zone can cut a lot in two parts."""
    split()
    for piece in pieces_for(connection, LOT_NUMBER).values():
        assert piece["geom_type"] == "ST_MultiPolygon"


# -- the frontage, which is most of the point --------------------------------


def test_the_street_belongs_to_the_piece_that_touches_it(connection, split):
    """The half of the split that is not about area.

    The parcel fronts 15.238 m of avenue Chabot and the boundary is drawn
    behind that edge, so the front piece has all of it and the rear piece has
    none. Under the old grain both carried the full 15.238 m, and the rear
    piece - which fronts nothing - was tested for *Largeur du terrain min*
    against a street it does not touch.
    """
    split()
    pieces = pieces_for(connection, LOT_NUMBER)

    assert pieces[FRONT_ZONE]["primary_frontage_m"] == pytest.approx(
        WIDTH_M, abs=0.3
    )
    assert pieces[FRONT_ZONE]["primary_street_name"] == "Chabot"
    assert pieces[FRONT_ZONE]["num_frontages"] == 1

    assert pieces[REAR_ZONE]["primary_frontage_m"] is None
    assert pieces[REAR_ZONE]["num_frontages"] == 0


def test_the_pieces_do_not_between_them_invent_frontage(connection, split):
    """A metre of street counted twice would be a metre the parcel does not have.

    The tolerance the clip is taken at lets two pieces meeting at a corner both
    claim the few centimetres between them, which is the right direction to be
    wrong in - see `DEFAULT_ZONE_PIECE_EDGE_TOLERANCE_M`. This is the guard on
    how far that goes: the pieces' frontages must not exceed the lot's by more
    than the tolerance itself.
    """
    split()
    pieces = pieces_for(connection, LOT_NUMBER)
    claimed = sum(
        piece["primary_frontage_m"] or 0.0 for piece in pieces.values()
    )
    assert claimed == pytest.approx(WIDTH_M, abs=0.5)


# -- what stands on each piece ----------------------------------------------


def test_a_piece_with_no_building_on_it_is_vacant_land(connection, split):
    """`footprint_share` is measured, and 0 is an answer rather than a gap.

    No buildings are loaded in this slice, so every lot carries no measured
    footprint at all and the allocator falls back to area - which is the
    documented fallback and what `footprint_share_basis` exists to declare.
    A share that silently read 0 here would report every parcel in the borough
    as having no assessment to compare against.
    """
    split()
    pieces = pieces_for(connection, LOT_NUMBER)

    for piece in pieces.values():
        assert piece["existing_footprint_m2"] == 0.0
        assert piece["footprint_share_basis"] == "area"
        assert piece["footprint_share"] == pytest.approx(piece["area_share"])

    # And the two shares still sum to exactly one, which is what keeps a
    # borough's totals unchanged by the split.
    assert sum(
        piece["footprint_share"] for piece in pieces.values()
    ) == pytest.approx(1.0)


# -- the cutoffs -------------------------------------------------------------


def test_a_sliver_of_a_neighbouring_zone_is_not_a_piece(connection, split):
    """Under a per cent *and* under a square metre: two publishers, one line.

    The cutoff that used to live in `lot_zoning_envelopes`, applied here now
    because this is where a site comes into existence. A boundary drawn 5 cm
    off the street edge cuts a strip of 0.76 m2 - 0.16 % of the parcel - which
    is the cadastre and the zoning layer missing each other along a lot line
    rather than a zone anybody could build under.
    """
    result, _expected = split(depth_m=0.05)
    pieces = pieces_for(connection, LOT_NUMBER)

    assert set(pieces) == {REAR_ZONE}
    assert pieces[REAR_ZONE]["num_lot_zones"] == 1
    # And with the sliver gone the survivor is the whole parcel again, which
    # is what makes dropping it safe: nothing of the lot is left unallocated.
    assert pieces[REAR_ZONE]["footprint_share"] == pytest.approx(1.0)
    assert int(result["num_split_lots"]) == 0


def test_the_cutoffs_can_be_relaxed_to_keep_the_sliver(connection, split):
    """Both of them, and both are needed: 0.76 m2 is also 0.16 % of the lot."""
    split(depth_m=0.05, min_pct_of_lot=0.0, min_overlap_m2=0.0)
    pieces = pieces_for(connection, LOT_NUMBER)

    assert set(pieces) == {FRONT_ZONE, REAR_ZONE}
    assert pieces[FRONT_ZONE]["piece_area_m2"] < 1.0


def test_a_large_piece_survives_a_percentage_that_would_drop_it(connection, split):
    """The one clause that is an `or`, and the failure it is insurance against.

    A percentage is the right measure on an ordinary parcel and turns around on
    a very large one - one per cent of Parc Jarry is a city block. Raising
    `min_pct_of_lot` past the front piece's 28.8 % is this fixture's way of
    putting a real site under a percentage that rejects it; the absolute floor
    is what keeps it.
    """
    _result, expected = split(min_pct_of_lot=50.0, min_piece_area_m2=50.0)
    pieces = pieces_for(connection, LOT_NUMBER)

    # 137 m2 at 28.8 % of the parcel: rejected by the percentage, kept by the
    # absolute floor beneath it.
    assert set(pieces) == {FRONT_ZONE, REAR_ZONE}
    assert pieces[FRONT_ZONE]["piece_area_m2"] == pytest.approx(
        expected[FRONT_ZONE], abs=TOLERANCE_M2
    )


def test_without_the_absolute_floor_the_percentage_drops_it(connection, split):
    """The other half of the pair above, so the clause is shown to be load-bearing."""
    split(min_pct_of_lot=50.0, min_piece_area_m2=1e9)
    pieces = pieces_for(connection, LOT_NUMBER)

    assert set(pieces) == {REAR_ZONE}


def test_the_default_floor_is_the_documented_one(connection, split):
    """A threshold that decides which sites exist travels on every row."""
    split()
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT min_pct_of_lot, min_overlap_m2, min_piece_area_m2
          FROM silver.lot_zone_pieces
         WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [NEIGHBORHOOD, SCRAPE_DATE],
    )
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1.0, 1.0, MIN_ZONE_PIECE_AREA_M2)


# -- what the run reports ----------------------------------------------------


def test_the_run_counts_the_split_lots_and_the_ground_at_stake(connection, split):
    """The two numbers that say whether this grain is doing anything here."""
    result, expected = split()

    assert int(result["num_split_lots"]) == 1
    assert int(result["num_pieces_on_split_lots"]) == 2
    assert int(result["num_pieces"]) == int(result["num_lots"]) + 1
    # The front piece is the ground the rear zone's grid used to answer for.
    # Reported in hectares to one decimal, which is the borough-scale unit the
    # number exists in - one 89 m2 piece rounds to 0.0 there, and asserting the
    # rounding rather than working around it is what keeps this test honest
    # about what the metadata actually says.
    assert float(result["secondary_piece_area_ha"]) == pytest.approx(
        round(expected[FRONT_ZONE] / 10_000.0, 1), abs=0.001
    )
    # And it is a *secondary* piece, which is the fact the hectares round away.
    assert int(result["num_large_secondary_pieces"]) == 0


def test_a_piece_with_no_street_is_counted_rather_than_hidden(connection, split):
    """`no_governing_column` downstream is nearly always one of these."""
    result, _expected = split()
    assert int(result["num_pieces_without_frontage"]) >= 1
