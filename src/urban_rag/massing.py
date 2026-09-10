"""A rectangle you can put on a map, for the building the solver costed.

`urban_rag.program` answers in numbers - a footprint of 287 m2, five storeys,
eleven dwellings - and numbers are exactly what nobody can sanity-check. A
footprint is an *area*, and the whole of what the solver knows about the shape
of the parcel it has to sit on is that area and one other area: *Taux
d'implantation au sol* x lot area, and what `lot_buildable_setbacks` leaves
after the zone's four margins. Whether a building of that footprint can
actually be drawn inside those margins is a question about geometry, and the
CP-SAT model never asks it.

This module asks it, by trying to draw one. For each lot it takes the
buildable polygon the setbacks asset computed - the parcel with the margins
already subtracted, so a rectangle inside it respects them by construction -
and looks for the largest axis-aligned-to-the-parcel rectangle of the target
area that fits. What comes out is one polygon per lot, in EPSG:4326, that can
be dropped straight onto a map beside the cadastre.

**The rectangle is a schematic, not a design.** Nothing here is an architect:
real buildings are L-shaped, they step back above a podium, they put the
parking under a footprint wider than the tower. A rectangle of the right area
in the right place is enough to answer the questions this exists for - does the
building fit, does it look like the block around it, is the solver's answer
absurd - and stopping there is what keeps it honest. `massing_status` says how
the drawing went and never pretends more than it did.

----------------------------------------------------------------------------
What the fit actually reveals
----------------------------------------------------------------------------

`solve_program` caps the footprint at the *lesser of* two areas and stops. That
is right as far as it goes and it is not the same as fitting: a buildable
envelope of 200 m2 that is 40 m deep and 5 m wide has room for no rectangle of
200 m2 at all, and a solver working in areas will happily spend all 200 of
them. So `footprint_fit_pct` below 100 is not a defect in this module - it is
this module reporting a defect in the *answer*, and it is the single most
useful column here.

That is why a rectangle that does not fit at full size is shrunk rather than
dropped. `placed_footprint_m2` is the largest rectangle of the chosen aspect
ratio that does fit, and the gap between it and `footprint_m2` is how much of
the solver's envelope is unbuildable in the shape it was costed at. Dropping
those rows would hide exactly the lots worth looking at.

----------------------------------------------------------------------------
How a rectangle is chosen
----------------------------------------------------------------------------

**The parcel's own grain sets the angle.** A Montreal lot is a long thin
rectangle perpendicular to the street, and a building on it runs the same way -
so the candidate angles come from the buildable polygon's own
`minimum_rotated_rectangle`, whose long edge is the parcel's axis. Both that
angle and its perpendicular are tried, because which of the two a building
takes is a question about the aspect ratio rather than about the lot.

**A few aspect ratios, in order, and the first that fits wins.**
`DEFAULT_ASPECT_RATIOS` is square, then three progressively longer rectangles.
They are tried in order and the search stops at the first that fits at full
area, which is both the cheap thing to do and the right one: a square is the
most compact use of a given area and the least likely to be an artefact of a
long thin envelope, so a lot where the square fits should get the square. A lot
where it does not is a lot whose envelope is genuinely long and thin, and the
ratio that fits is a fact about the parcel worth carrying - `aspect_ratio` is
on every row.

**Centres are searched, not assumed.** The centroid of a concave polygon can be
outside it, and a rectangle centred on the middle of an L-shaped envelope fits
nothing. So the candidates are the centroid, the polygon's own
`representative_point`, and a grid over its bounds filtered to the points
actually inside it, all tested at once through shapely's vectorised
`contains`. This is a search rather than a construction and it is approximate
in one direction only: it can fail to find a placement that exists, never
report one that does not. `GRID_STEPS` is what that costs and what it buys.

**A MultiPolygon is fitted in its largest part.** Margins can cut a parcel in
two - a corner lot deep enough that the two front setbacks meet - and a
building goes in one of the pieces, not across both.

----------------------------------------------------------------------------
The parking is a second polygon, and never part of the first
----------------------------------------------------------------------------

`solve_program` has three places to put a stall and one of them is the ground:
`surface_stalls` stand on the yard the footprint leaves, costing no storey and
no *superficie de plancher* because a car outdoors is not in a building. That
last clause is also why the parking is not in the massing. A surface stall is
not a building - it has no floor area, no storey and no height - so folding it
into the massing rectangle would inflate the footprint a reader is checking
`footprint_fit_pct` against, and a map extruding that rectangle to `height_m`
would raise a solid where there is asphalt. `fit_parking` draws it as its own
shape, `massing_frame` returns it in its own column, and `lot_building_massing`
publishes it to its own table.

**And the yard is the parcel, not the envelope.** A setback is a margin a
*building* keeps. A car standing in a side or rear yard is standing exactly
where the margin said no building may go, so the container for the parking is
the lot boundary less the drawn building - `yard_of` - and not the buildable
polygon the building was fitted into.

**Nothing is reserved for reaching it.** A surface stall does not have to front
the street, and on a Montreal block it usually does not: it is reached from the
back lane, or across the front yard of the same parcel. Requiring the parking
to touch the frontage would refuse the ordinary case, so no access route is
modelled at all - which is an assumption worth knowing rather than a check that
was forgotten.

**What is checked is the shape, which is the point.** The stalls are already
bounded upstream by `surface_stall_area x stalls + footprint <= lot area`, and
that is an area against an area: it is satisfied on a parcel four metres wide,
where no car can stand at all. So the ground is tested for depth -
`parkable_ground` opens the yard by half of `MIN_PARKING_DEPTH_M` and keeps
whatever survives, at whatever shape that leaves. `parking_capacity_m2` is that
region measured and handed to `solve_program` as `Lot.parkable_area_m2`, and
`fit_parking` is that region *drawn*, as a band grown out from the building
until it holds the program's area.

**One rule, two uses, and that is deliberate.** The bound and the drawing were
a rectangle search and a three-rectangle search, and being two searches they
disagreed: the solve priced parking the placer could not lay out, and
`surface_parking_fit_pct` came back under 100 on about half the borough. They
are now the same function, so a program is bounded by exactly the ground it
will be drawn on and the column is a check that can pass.

**A rectangle was the wrong primitive.** The ordinary Montreal answer is a band
wrapping the building - a ring, an L, a wedge behind a corner plate - and three
rectangles read a band at a little over half its area. Across 600 real VSMPE
yards the bays kept a median 34 pct of the ground and the opening keeps 79, at
0.12 ms against 4.4. What survives of the old strictness is the by-law
dimension itself, and it is stricter than it was: 5.5 m clear in *every*
direction, so a 3 m side-yard ribbon parks nothing at any length. See
`parkable_ground` for the trade and where to reverse it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd
import shapely
from shapely.geometry import Point, Polygon
from shapely.geometry.base import BaseGeometry

from urban_rag.comparables import METRIC_CRS

#: Width-to-depth ratios tried, in order, and the reason the order is that one:
#: a square is the most compact rectangle of a given area and therefore the one
#: most likely to fit, and the least likely to be an artefact of the envelope
#: it was fitted into. Each is tried at the parcel's own axis and at the
#: perpendicular, so 1:2 is covered by trying 2:1 the other way round and does
#: not need its own entry.
#:
#: 3:1 is the last because past it a "building" is a wall: a 300 m2 footprint
#: at 4:1 is 35 m by 8.7 m, which is a row of townhouses rather than the single
#: massing this module draws. A lot that fits nothing squarer than that is
#: reported `shrunk` instead, which is the more useful answer.
DEFAULT_ASPECT_RATIOS: tuple[float, ...] = (1.0, 1.5, 2.0, 3.0)

#: Candidate centres per axis, so the grid is this squared before the points
#: outside the polygon are dropped. 9 is a little over a metre apart on a
#: typical 10 m x 30 m Villeray envelope, which is finer than the decimetre the
#: margins themselves are stated to and far finer than the question being
#: asked. The cost is linear in a vectorised `contains` call that runs in C.
GRID_STEPS = 9

#: Halvings of the shrink search, when no rectangle of the target area fits.
#: Each one bisects the scale factor, so 8 puts `placed_footprint_m2` within
#: about half a percent of the largest rectangle of that ratio that does fit -
#: well inside the accuracy of everything upstream of it, and cheap because
#: each step is one vectorised call.
SHRINK_STEPS = 8

#: Below this, a footprint is not a building. A rectangle of a quarter of a
#: square metre would technically "fit" almost anywhere and would put a dot on
#: a map that reads as a successful massing; reporting `no_fit` is the honest
#: answer for an envelope that holds nothing.
MIN_FOOTPRINT_M2 = 10.0

#: How deep a strip of yard has to be before a car can stand on it, and the
#: whole reason surface parking is measured as a shape here rather than as an
#: area upstream. 5.5 m is the length of a stall in article 566 of by-law
#: 01-283. The 300 sq ft `program.SURFACE_STALL_AREA_SQFT` allows a stall is
#: 27.87 m2, and 27.87 m2 of yard laid out as a one-metre ribbon around a
#: building holds no car at all.
#:
#: The drive aisle is *not* added on top of this. It is already inside the
#: 300 sq ft - a stall itself is 2.6 x 5.5 = 14.3 m2 and the rest of the
#: allowance is the aisle and manoeuvring space the cost guide prices per
#: stall - so asking for 5.5 m of depth *and* 27.87 m2 of area per stall asks
#: for the stall and its share of the aisle without buying either twice.
MIN_PARKING_DEPTH_M = 5.5

#: Halvings of the bisection that grows the paving out from the building until
#: it holds the program's area. Each step is one buffer and one intersection,
#: so forty is about a millisecond and a half on a Villeray yard and lands the
#: area within a few square centimetres of the target - far inside the square
#: metre `_PARKING_AREA_TOLERANCE_M2` already calls `fitted`.
PARKING_BISECT_STEPS = 40

#: Separate patches of asphalt one program may be drawn as. Unlike the
#: building, which is one massing or it is nothing, parking genuinely comes in
#: pieces: a building sitting across the middle of its parcel leaves a front
#: yard and a rear yard, and stalls in both is the ordinary Montreal answer,
#: not a compromise. Insisting on a single rectangle would report that lot at
#: half its real capacity, and a sanity-check column that cries wolf on the
#: common case is worse than no column.
#:
#: Three rather than more because the pieces get small fast - the fourth patch
#: of a Villeray yard is a corner nobody paves - and each one costs another
#: pass of the search.
PARKING_MAX_BAYS = 3

#: How far short of the reserved area the bays may fall and still be `fitted`.
#: The total is reassembled from patches each bisected into place, so it lands
#: a little under the target even where the ground is plainly there. One square
#: metre is under four percent of a stall and an order of magnitude below the
#: precision of anything upstream.
_PARKING_AREA_TOLERANCE_M2 = 1.0

#: A nanometre off each half-dimension of every rectangle before it is tested
#: and reported, and the reason is arithmetic rather than planning: a rectangle
#: that exactly fills its envelope has corners computed through a rotation, and
#: `cos(pi/2)` is 6.1e-17 rather than 0. A 30 m side then lands about 1e-15 m
#: outside the boundary it should sit on, `contains` says no, and a parcel that
#: is exactly buildable to its margins comes back `shrunk` at 99.6%.
#:
#: Insetting rather than growing the envelope keeps the answer conservative -
#: a rectangle this module reports is inside the margins, never a nanometre
#: over - and it costs 1e-7 m2 of a footprint, which is eleven orders of
#: magnitude below the decimetre the margins themselves are stated to.
_FIT_EPSILON_M = 1e-9

#: Why a lot has the massing it has. One value per row, so "the borough has 300
#: lots whose footprint does not fit its own setbacks" is a `GROUP BY`.
MASSING_STATUSES: tuple[str, ...] = (
    # A rectangle of the full solved footprint fits inside the margins.
    "fitted",
    # None of the ratios fits at full area; the rectangle drawn is the largest
    # of the best ratio that does. `footprint_fit_pct` is how much of the
    # solver's footprint the parcel can actually carry in that shape, and this
    # is the status worth reading - see the module docstring.
    "shrunk",
    # The parcel has an envelope but nothing of `MIN_FOOTPRINT_M2` fits in it.
    # A slver of a lot, or a parcel narrower than twice its own side margin -
    # `lot_buildable_setbacks` reports a buildable area of 0 for those.
    "no_fit",
    # No buildable polygon for the governing (lot, zone, column). Either
    # `lot_buildable_setbacks` has not run for this partition, or it had no
    # front edge to sort this lot's boundary against - so there is nothing to
    # respect the margins *of*, and this module declines to draw a rectangle
    # rather than drawing one that ignores them.
    "no_buildable_geometry",
    # `lot_highest_best_use` has no program for this lot, or one with no
    # footprint at all. Nothing to draw, and not a failure to draw it.
    "no_program",
)

#: Why a lot's surface parking is drawn the way it is - the same idea as
#: `MASSING_STATUSES` and a separate column, because a lot can perfectly well
#: have a building that fits and parking that does not, and one status cannot
#: say both.
PARKING_STATUSES: tuple[str, ...] = (
    # The whole of the yard area the program reserved fits, at some depth of
    # at least one stall.
    "fitted",
    # Part of it does. The rectangle drawn is the largest that fits at a legal
    # depth, and `surface_parking_fit_pct` is how much of the program's own
    # surface parking the parcel can actually carry once the building is on
    # it. This is the status worth reading - see the module docstring.
    "shrunk",
    # There is a yard and no parking of one stall's dimensions fits in it.
    "no_fit",
    # The building covers the parcel, or there is no lot polygon to park on.
    "no_yard",
    # The lot polygon for this lot is missing, so nothing was checked. Not the
    # same as `no_yard`: that one is an answer, this one is an absence.
    "no_lot_geometry",
    # The program parks nothing on the yard - it dug, bayed, or owes no stall
    # at all. Nothing to draw, and not a failure to draw it.
    "no_parking",
    # `lot_highest_best_use` has no program for this lot.
    "no_program",
)

#: What a massing row carries besides its geometry, in reading order.
MASSING_COLUMNS: tuple[str, ...] = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "feature_id",
    "column_index",
    "hbu_status",
    "massing_status",
    # The solver's answer, and what could actually be drawn of it.
    "footprint_m2",
    "placed_footprint_m2",
    "footprint_shortfall_m2",
    "footprint_fit_pct",
    # The rectangle itself, for a reader who wants the numbers rather than the
    # polygon: the ratio chosen, its two sides in metres, and the bearing of
    # its long axis measured the way a compass is.
    "aspect_ratio",
    "width_m",
    "depth_m",
    "rotation_deg",
    # What sits on that footprint. Carried so a map can extrude the rectangle
    # to the height the solver costed without joining back.
    "floors",
    "height_m",
    "residential_floors",
    "commercial_floors",
    "industrial_floors",
    "underground_levels",
    "num_dwellings",
    # `placed_footprint_m2 * floors` - the gross floor area actually drawable,
    # against `gross_floor_area_m2` which is what was costed. The pair is the
    # same sanity check as the footprint one, carried up to the number the
    # density cap was tested against.
    "gross_floor_area_m2",
    "placed_gross_floor_area_m2",
    # What stands on the yard rather than on the footprint, summarised. The
    # polygon itself is not here - it is a second geometry, published to
    # `gold.lot_surface_parking` - but the numbers ride along so the massing
    # table alone answers "does this building's parking fit on this lot".
    "parking_status",
    "surface_stalls",
    "placed_surface_stalls",
    "surface_parking_area_m2",
    "placed_surface_parking_m2",
    "surface_parking_fit_pct",
    # The envelope it was fitted into, for scale.
    "buildable_area_m2",
    "lot_area_m2",
)

#: What a surface parking row carries, in reading order. A superset of the
#: identity `MASSING_COLUMNS` opens with, plus the rectangle's own dimensions
#: and the yard it was found in - and it is a separate list because it is a
#: separate table: `gold.lot_surface_parking` is keyed and drawn on its own
#: polygon, and a lot with no surface parking has no row in it.
PARKING_COLUMNS: tuple[str, ...] = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "feature_id",
    "column_index",
    "hbu_status",
    "massing_status",
    "parking_status",
    # What the solver put on the yard, and what could actually be drawn of it.
    "surface_stalls",
    "placed_surface_stalls",
    "surface_parking_area_m2",
    "placed_surface_parking_m2",
    "surface_parking_shortfall_m2",
    "surface_parking_fit_pct",
    # The band itself. `parking_depth_m` is how far the paving reaches out
    # from the building; `parking_width_m` and `parking_rotation_deg` are null
    # since the rectangle search was replaced - see `Parking`.
    "parking_width_m",
    "parking_depth_m",
    "parking_rotation_deg",
    "num_parking_bays",
    # The ground it was looked for in: the parcel less the drawn building,
    # and the shape-blind cap the solver was given for the same parcel.
    "yard_area_m2",
    "parkable_area_m2",
    "footprint_m2",
    "placed_footprint_m2",
    "lot_area_m2",
)


@dataclass(frozen=True)
class Massing:
    """One fitted rectangle, or the reason there is none."""

    geometry: Polygon | None
    status: str
    aspect_ratio: float | None = None
    width_m: float = 0.0
    depth_m: float = 0.0
    rotation_deg: float = 0.0
    placed_footprint_m2: float = 0.0


#: Nothing drawn, under a status that says why.
def _nothing(status: str) -> Massing:
    return Massing(geometry=None, status=status)


def fit_rectangle(
    buildable: BaseGeometry | None,
    target_area_m2: float,
    *,
    aspect_ratios: Sequence[float] = DEFAULT_ASPECT_RATIOS,
    grid_steps: int = GRID_STEPS,
    shrink_steps: int = SHRINK_STEPS,
    min_footprint_m2: float = MIN_FOOTPRINT_M2,
) -> Massing:
    """The largest rectangle of ``target_area_m2`` that fits inside ``buildable``.

    ``buildable`` is the parcel with its zone's four margins already
    subtracted, in a **projected** CRS whose unit is the metre - `METRIC_CRS`,
    which is what `to_metric` puts a frame into. Passing degrees would compute
    a footprint in square degrees and fit nothing.

    Ratios are tried in order and each at two angles, the parcel's own axis and
    its perpendicular; the first that fits at full area wins and the search
    stops there. If none does, the ratio that fits *largest* is shrunk to fit
    and the result is `shrunk` - see the module docstring for why that is the
    interesting answer rather than a failure.
    """
    if target_area_m2 is None or not math.isfinite(target_area_m2):
        return _nothing("no_program")
    if target_area_m2 < min_footprint_m2:
        return _nothing("no_program")
    polygon = _largest_part(buildable)
    if polygon is None or polygon.is_empty or polygon.area <= 0:
        return _nothing("no_buildable_geometry")

    angles = _candidate_angles(polygon)
    centres = _candidate_centres(polygon, grid_steps)
    if not len(centres):
        return _nothing("no_fit")

    # Prepared once and reused by every `contains` below - a hundred-odd calls
    # per lot in the shrink search, each against the same envelope. Without it
    # shapely rebuilds the polygon's index on every one of them.
    shapely.prepare(polygon)

    # A rectangle of more area than the envelope has cannot fit in it, whatever
    # its shape, so the full-size pass is skipped rather than run to fail eight
    # times. This is the common case on a tight parcel and is most of what
    # makes a borough tractable.
    frames = _frames(polygon, angles)
    if target_area_m2 <= polygon.area:
        for ratio in aspect_ratios:
            for angle, frame in zip(angles, frames):
                placed = _place(
                    polygon, centres, target_area_m2, ratio, angle, frame
                )
                if placed is not None:
                    # Full area, first ratio that takes it: nothing later in
                    # the list can be better, so stop rather than score
                    # the rest.
                    return Massing(
                        geometry=placed,
                        status="fitted",
                        aspect_ratio=ratio,
                        width_m=_side(target_area_m2, ratio),
                        depth_m=_side(target_area_m2, 1.0 / ratio),
                        rotation_deg=angle,
                        placed_footprint_m2=target_area_m2,
                    )

    # Nothing fits whole. Find the ratio and angle that carry the most area,
    # bisecting the scale factor rather than stepping it: the largest rectangle
    # that fits is monotone in the scale, so eight halvings settle it. The
    # search starts from the envelope's own area rather than from the target,
    # since no scale above that can fit and bisecting into it would waste the
    # first halvings on scales already known to fail.
    ceiling = min(1.0, polygon.area / target_area_m2)
    best: Massing | None = None
    for ratio in aspect_ratios:
        for angle, frame in zip(angles, frames):
            shrunk = _shrink_to_fit(
                polygon,
                centres,
                target_area_m2,
                ratio,
                angle,
                steps=shrink_steps,
                min_area=min_footprint_m2,
                ceiling=ceiling,
                frame=frame,
            )
            if shrunk is not None and (
                best is None or shrunk.placed_footprint_m2 > best.placed_footprint_m2
            ):
                best = shrunk
    return best if best is not None else _nothing("no_fit")


def _place(
    polygon: Polygon,
    centres: np.ndarray,
    area_m2: float,
    ratio: float,
    angle_deg: float,
    frame: "_Frame | None" = None,
) -> Polygon | None:
    """A rectangle of ``area_m2`` at ``ratio`` and ``angle_deg`` inside ``polygon``.

    Every candidate centre is tested in one vectorised `contains` call, and the
    rectangles themselves are built in one vectorised `shapely.polygons` over a
    numpy array of corners - which is the whole reason the centres are an array
    rather than a loop. Constructing them one at a time through `box` and
    `rotate` costs about thirty times as much, and this runs a hundred times
    per lot in the shrink search below.

    ``frame`` is the parcel's extent measured along this angle, and is what
    supplies the *flush* placements - a rectangle pushed hard against each edge
    and corner of that extent. They matter twice over. A fixed grid of centres
    is too coarse to find the one position a tight envelope allows: a 10 m wide
    left arm takes a 10 m wide building only when it is centred at exactly 5 m,
    and a grid stepping 3 m at a time never lands there. And a building pressed
    to the front of its envelope is what actually gets built - a Montreal
    walk-up sits on its front setback line, not floating in the middle of the
    lot - so these are the realistic positions as well as the findable ones.

    Returns the first that fits, or None. First rather than best on purpose:
    the centroid leads `_candidate_centres`, so a building with room to spare
    sits centred in its own envelope, and only a building that needs the extra
    room is pushed against an edge to find it.
    """
    # Inset by `_FIT_EPSILON_M` so a rectangle that exactly fills its envelope
    # is not rejected by the rounding in its own rotation - see that constant.
    half_w = max(_side(area_m2, ratio) / 2.0 - _FIT_EPSILON_M, 0.0)
    half_d = max(_side(area_m2, 1.0 / ratio) / 2.0 - _FIT_EPSILON_M, 0.0)
    if half_w <= 0.0 or half_d <= 0.0:
        return None
    radians = math.radians(angle_deg)
    cos, sin = math.cos(radians), math.sin(radians)
    rotation = np.array([[cos, sin], [-sin, cos]])

    candidates = centres
    if frame is not None:
        flush = frame.flush_centres(half_w, half_d)
        if len(flush):
            candidates = np.vstack([centres, flush]) if len(centres) else flush
    if not len(candidates):
        return None

    # The closed ring of one rectangle centred on the origin, rotated once and
    # then translated to every candidate centre at once.
    local = np.array(
        [
            [-half_w, -half_d],
            [half_w, -half_d],
            [half_w, half_d],
            [-half_w, half_d],
            [-half_w, -half_d],
        ]
    )
    corners = candidates[:, None, :] + (local @ rotation)[None, :, :]
    rectangles = shapely.polygons(corners)
    fits = shapely.contains(polygon, rectangles)
    matches = np.flatnonzero(fits)
    return rectangles[int(matches[0])] if matches.size else None


@dataclass(frozen=True)
class _Frame:
    """The parcel's extent measured along one candidate angle.

    A rectangle at that angle is axis-aligned in this frame, so the positions
    where it sits flush against the envelope are arithmetic here rather than a
    search: the three interesting offsets on each axis are hard against the
    low edge, centred, and hard against the high edge. `flush_centres` is those
    nine combinations, rotated back into the world.
    """

    angle_deg: float
    min_x: float
    min_y: float
    max_x: float
    max_y: float

    def flush_centres(self, half_w: float, half_d: float) -> np.ndarray:
        """The nine flush positions for a rectangle of these half-dimensions.

        Empty where the rectangle is wider or deeper than the parcel's own
        extent along this angle - there is then no position at all, flush or
        otherwise, and returning the impossible ones would only cost a
        `contains` call to reject them.
        """
        if self.max_x - self.min_x < 2 * half_w or self.max_y - self.min_y < 2 * half_d:
            return np.empty((0, 2), dtype="float64")
        xs = (
            self.min_x + half_w,
            (self.min_x + self.max_x) / 2.0,
            self.max_x - half_w,
        )
        ys = (
            self.min_y + half_d,
            (self.min_y + self.max_y) / 2.0,
            self.max_y - half_d,
        )
        local = np.array([(x, y) for x in xs for y in ys], dtype="float64")
        radians = math.radians(self.angle_deg)
        cos, sin = math.cos(radians), math.sin(radians)
        # Back out of the frame: the inverse of the `-angle` rotation the
        # bounds were measured in.
        return local @ np.array([[cos, sin], [-sin, cos]])


def _frames(polygon: Polygon, angles: Sequence[float]) -> list[_Frame]:
    """``polygon``'s extent along each candidate angle, measured once.

    The coordinates are rotated by *minus* the angle so a rectangle at that
    angle is axis-aligned in the result, which is what makes `flush_centres`
    arithmetic. Computed here rather than inside `_place` because the shrink
    search calls that a hundred times against the same two angles.
    """
    coords = shapely.get_coordinates(polygon)
    frames: list[_Frame] = []
    for angle in angles:
        radians = math.radians(-angle)
        cos, sin = math.cos(radians), math.sin(radians)
        rotated = coords @ np.array([[cos, sin], [-sin, cos]])
        frames.append(
            _Frame(
                angle_deg=angle,
                min_x=float(rotated[:, 0].min()),
                min_y=float(rotated[:, 1].min()),
                max_x=float(rotated[:, 0].max()),
                max_y=float(rotated[:, 1].max()),
            )
        )
    return frames


def _shrink_to_fit(
    polygon: Polygon,
    centres: np.ndarray,
    target_area_m2: float,
    ratio: float,
    angle_deg: float,
    *,
    steps: int,
    min_area: float,
    ceiling: float = 1.0,
    frame: "_Frame | None" = None,
) -> Massing | None:
    """The largest rectangle of this ratio and angle that fits, by bisection.

    The scale factor is on the *area*, so a `footprint_fit_pct` of 70 means
    seventy percent of the footprint the solver costed - which is the number a
    reader wants - rather than seventy percent of each side.
    """
    low, high = 0.0, ceiling
    found: Polygon | None = None
    found_area = 0.0
    for _ in range(steps):
        middle = (low + high) / 2.0
        area = target_area_m2 * middle
        if area < min_area:
            # Below the floor there is nothing worth drawing, so this half of
            # the interval is abandoned rather than searched: raising `low`
            # walks the search up towards the sizes that might still qualify.
            low = middle
            continue
        placed = _place(polygon, centres, area, ratio, angle_deg, frame)
        if placed is None:
            high = middle
        else:
            low = middle
            found, found_area = placed, area
    if found is None:
        return None
    return Massing(
        geometry=found,
        status="shrunk",
        aspect_ratio=ratio,
        width_m=_side(found_area, ratio),
        depth_m=_side(found_area, 1.0 / ratio),
        rotation_deg=angle_deg,
        placed_footprint_m2=found_area,
    )


def _side(area_m2: float, ratio: float) -> float:
    """One side of a rectangle of ``area_m2`` whose width/depth is ``ratio``."""
    return math.sqrt(area_m2 * ratio)


def _largest_part(geometry: BaseGeometry | None) -> Polygon | None:
    """The biggest polygon of ``geometry``, repaired if it needs it.

    Margins can cut a parcel in two and a building goes in one of the pieces.
    `make_valid` first, because the buildable geometry is a difference of
    buffered boundaries and those can come back with a zero-width sliver
    joining two lobes - which `contains` would then answer about a shape no
    building could occupy.
    """
    if geometry is None or geometry.is_empty:
        return None
    if not geometry.is_valid:
        geometry = shapely.make_valid(geometry)
    parts = [
        part
        for part in shapely.get_parts(shapely.normalize(geometry))
        if isinstance(part, Polygon) and not part.is_empty
    ]
    if not parts:
        return None
    return max(parts, key=lambda part: part.area)


def _candidate_angles(polygon: Polygon) -> tuple[float, float]:
    """The parcel's own axis, and the perpendicular to it.

    Read off the minimum rotated rectangle rather than off the frontage: the
    buildable polygon is what the building has to fit inside, and its own long
    edge is the direction that leaves the most room. On a rectangular Montreal
    lot the two are the same line anyway, since the margins are parallel to the
    boundary they are measured from.
    """
    try:
        corners = list(polygon.minimum_rotated_rectangle.exterior.coords)[:4]
    except (AttributeError, IndexError, ValueError):
        return (0.0, 90.0)
    if len(corners) < 3:
        return (0.0, 90.0)
    (x0, y0), (x1, y1), (x2, y2) = corners[0], corners[1], corners[2]
    first = math.hypot(x1 - x0, y1 - y0)
    second = math.hypot(x2 - x1, y2 - y1)
    if first >= second:
        angle = math.degrees(math.atan2(y1 - y0, x1 - x0))
    else:
        angle = math.degrees(math.atan2(y2 - y1, x2 - x1))
    angle %= 180.0
    return (angle, (angle + 90.0) % 180.0)


def _candidate_centres(polygon: Polygon, grid_steps: int) -> np.ndarray:
    """Points inside ``polygon`` a rectangle might be centred on.

    The centroid and `representative_point` first, because on a convex parcel
    one of them is the answer and the grid is then never consulted; the grid
    after, because a centroid can fall outside an L-shaped envelope and a
    building centred on a point that is not in the polygon fits nothing.
    """
    points: list[tuple[float, float]] = []
    centroid = polygon.centroid
    if not centroid.is_empty:
        points.append((centroid.x, centroid.y))
    interior = polygon.representative_point()
    if not interior.is_empty:
        points.append((interior.x, interior.y))

    min_x, min_y, max_x, max_y = polygon.bounds
    # Inset by half a step so the grid samples the inside rather than the
    # boundary, where no rectangle of any size is contained.
    step_x = (max_x - min_x) / (grid_steps + 1)
    step_y = (max_y - min_y) / (grid_steps + 1)
    xs = np.linspace(min_x + step_x, max_x - step_x, grid_steps)
    ys = np.linspace(min_y + step_y, max_y - step_y, grid_steps)
    grid_x, grid_y = np.meshgrid(xs, ys)
    grid_x, grid_y = grid_x.ravel(), grid_y.ravel()
    inside = shapely.contains_xy(polygon, grid_x, grid_y)
    points.extend(zip(grid_x[inside], grid_y[inside]))
    return np.asarray(points, dtype="float64").reshape(-1, 2)


# --------------------------------------------------------------------------
# surface parking
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Parking:
    """The surface parking drawn for one lot, or the reason there is none.

    `geometry` is a Polygon where one patch of asphalt held the whole program,
    and a MultiPolygon where it took more than one - see `PARKING_MAX_BAYS`.
    It is a band wrapping the building rather than a rectangle, so it has the
    shape of the yard it was cut from: a ring, an L, a wedge behind a corner
    plate.

    `depth_m` is the one dimension that band has - how far out from the
    building the paving reaches. `num_bays` is how many separate pieces it
    came out in, and 1 is the common answer.

    **`width_m` and `rotation_deg` are null, and are kept for the shape of the
    table rather than for what they say.** They described the largest
    *rectangle* when the parking was pieced together out of up to three of
    them; a band has no single width and no angle, and reporting 0.0 for them
    would read as a measurement rather than as their absence.
    """

    geometry: BaseGeometry | None
    status: str
    area_m2: float = 0.0
    width_m: float | None = None
    depth_m: float = 0.0
    rotation_deg: float | None = None
    num_bays: int = 0


#: Nothing parked, under a status that says why.
def _no_parking(status: str) -> Parking:
    return Parking(geometry=None, status=status)


def yard_of(lot: BaseGeometry | None, building: BaseGeometry | None):
    """The parcel less the building standing on it - where the cars go.

    The *lot*, not the buildable envelope, and that is the whole difference
    between this and `fit_rectangle`. A setback is a margin a **building**
    keeps; a car standing in a side or rear yard is standing exactly where the
    margin said no building may go. So the container here is the parcel itself,
    and the only thing taken out of it is the rectangle the building took.

    Nothing is reserved for reaching the parking from the street. That is a
    stated assumption rather than an oversight - a surface stall need not front
    the road, and on a Montreal block it usually does not: it is reached from
    the back lane, or across the front yard of the same parcel - and modelling
    an access route would mean drawing a driveway this module has no basis for
    drawing.

    Returns ``None`` where there is no parcel to park on. A building covering
    the parcel outright leaves an empty geometry, which every caller below
    reads as `no_yard`.
    """
    if lot is None or lot.is_empty:
        return None
    if building is None or building.is_empty:
        return lot
    return lot.difference(building)


def parkable_ground(
    yard: BaseGeometry | None,
    *,
    min_depth_m: float = MIN_PARKING_DEPTH_M,
    min_area_m2: float | None = None,
) -> BaseGeometry | None:
    """The part of ``yard`` a car can actually stand on, at whatever shape.

    A **morphological opening**: erode the yard by half a stall's depth and
    dilate it back. Ground that cannot hold a disc of that radius vanishes and
    everything else survives *as its own shape* - a ring around a building, an
    L, both side yards at once, the wedge behind a corner plate. What is left
    is the yard less its unparkable fringe, which is the honest answer to "how
    much of this can be paved" and the one a rectangle search cannot give.

    **Why this replaced the rectangles.** `fit_parking` used to piece the
    paving together out of up to `PARKING_MAX_BAYS` rectangles, and a rectangle
    is the wrong primitive for the shape a yard actually is: the ordinary
    Montreal answer is a band wrapping the building, and three rectangles read
    a band at a little over half its area. Measured across 600 real VSMPE
    yards, the bays kept a median 34 pct of the ground and this keeps 79 - and
    it does it in 0.12 ms against the search's 4.4.

    **The radius is half `min_depth_m`, and that is a strict reading.** A disc
    of 2.75 m fits only where 5.5 m is clear *in every direction* - a stall's
    full length whichever way the car is turned. A 3 m side-yard ribbon is
    therefore not parkable at any length, which is the intended answer and the
    expensive one: it is the narrow-lot tail, and those programs must dig or
    bay their stalls instead of pretending the margin will hold them. The
    looser reading - a disc of half a stall's *width*, so 2.6 m clear - is what
    the old ``min_width_m`` bay implied, and it keeps a median 95 pct. This
    module takes the strict one; `MIN_PARKING_DEPTH_M` is where to change it.

    Parts smaller than ``min_area_m2`` are dropped, defaulting to one stall's
    allowance: a 12 m2 corner that survives the opening still parks no car, and
    leaving it in would put a scrap of asphalt on the map and a fraction of a
    stall in the arithmetic.

    Returns ``None`` for no yard and an empty geometry for a yard that parks
    nothing - which every caller reads as `no_fit`, distinct from `no_yard`.
    """
    if yard is None or yard.is_empty or yard.area <= 0:
        return None
    # Inset by `_FIT_EPSILON_M`, so the boundary case is inclusive: a yard
    # exactly `min_depth_m` across erodes to a zero-width line at the bare
    # radius and would come back parking nothing, when 5.5 m clear is the
    # dimension the by-law asks for rather than the one it excludes.
    radius = min_depth_m / 2.0 - _FIT_EPSILON_M
    if radius <= 0:
        return yard
    # `join_style=2` (mitre) rather than the rounded default, so a square
    # corner of yard comes back square. Rounded joins would shave a disc's
    # worth off every corner on the dilation and report a rectangular yard at
    # less than its own area, which is a measurable bias on the common case.
    opened = yard.buffer(-radius, join_style=2).buffer(radius, join_style=2)
    # Clipped back to the yard because the dilation is not the exact inverse of
    # the erosion at a mitred corner: it can push a whisker past the lot line,
    # and parking outside the parcel is the one answer this must never give.
    opened = opened.intersection(yard)
    if opened.is_empty:
        return opened
    floor = _surface_stall_area_m2() if min_area_m2 is None else min_area_m2
    parts = [
        part for part in getattr(opened, "geoms", [opened]) if part.area >= floor
    ]
    if not parts:
        return opened.intersection(opened.buffer(-1e9))  # a typed empty
    if len(parts) == len(getattr(opened, "geoms", [opened])):
        return opened
    return shapely.union_all(parts)


def fit_parking(
    yard: BaseGeometry | None,
    target_area_m2: float,
    *,
    building: BaseGeometry | None = None,
    min_depth_m: float = MIN_PARKING_DEPTH_M,
    max_bays: int = PARKING_MAX_BAYS,
    bisect_steps: int = PARKING_BISECT_STEPS,
    min_area_m2: float | None = None,
) -> Parking:
    """The surface parking of ``target_area_m2``, drawn on ``yard``.

    Two steps, and they answer two different questions. `parkable_ground`
    settles **what may be paved** - the yard less the fringe too narrow to
    stand a car in, at whatever shape that leaves. This settles **which part of
    it is**, by growing a band out from the building until the band holds the
    program's area: ``region & building.buffer(d)``, bisecting ``d``.

    **Contiguous, and hugging the plate.** Those are the two properties that
    make the answer a site plan rather than a shading. A band is connected
    wherever the region is, so the result is one piece of asphalt on the great
    majority of parcels; and taking the *nearest* ground first is what a real
    lot does - the parking wraps the building, and what gets left over is the
    far corner of a deep parcel rather than the strip by the door. Where the
    region is genuinely in two lobes the band reaches the second only after the
    first is full, which is the same order and the honest picture.

    **Why a distance band rather than a space-filling curve.** A Hilbert or
    Morton prefix is the other way to take "a compact piece of a mask", and it
    is a worse fit here in three ways: the prefix is anchored to the curve's
    own quadrant recursion rather than to the building, so it has no notion of
    near; it is not connected inside an arbitrary mask, because the curve
    leaves the yard and comes back; and it needs the yard rasterised, where
    this stays in vector space and hits the target area to float precision -
    which is the whole point, since that area is the number the solver already
    charged the parcel for. A curve would earn its place if something needed a
    canonical pixel ordering reusable across tiles. Nothing here does.

    ``building`` is what to grow from, and it is optional: without it the band
    grows from the region's own centroid, which is a compact blob in the middle
    of the yard rather than a plan. Every caller inside this module passes the
    placed massing.

    **The area is met exactly, or the shortfall is reported.** A region smaller
    than the target is taken whole and returned `shrunk` - that is
    `surface_parking_fit_pct` below 100, and it says the program bought stalls
    the ground cannot hold. Where the region is larger, the bisection lands the
    band on the target and the status is `fitted`.

    ``max_bays`` survives from the rectangle search with its meaning intact:
    the most separate patches of asphalt one program may be drawn as. A band
    landing in more pieces than that keeps the largest and reports the rest as
    shortfall, because a program parked across five scattered scraps is not the
    program the solver priced.
    """
    if target_area_m2 is None or not math.isfinite(target_area_m2):
        return _no_parking("no_parking")
    if target_area_m2 <= 0.0:
        return _no_parking("no_parking")
    if yard is None or yard.is_empty or yard.area <= 0:
        return _no_parking("no_yard")

    region = parkable_ground(yard, min_depth_m=min_depth_m, min_area_m2=min_area_m2)
    if region is None or region.is_empty or region.area <= 0:
        # There is a yard, and no part of it a car can stand in.
        return _no_parking("no_fit")

    anchor = building if building is not None and not building.is_empty else None
    if anchor is None:
        anchor = region.centroid

    if region.area <= target_area_m2 + _PARKING_AREA_TOLERANCE_M2:
        # The whole parkable yard is not enough, so there is nothing to choose
        # between: pave it, and say how far short it fell.
        paved = region
    else:
        # The band, bisected. `hi` is what gets returned rather than `lo`,
        # because the invariant worth keeping is "this much area is definitely
        # covered": `lo` is the last distance known to fall *short*, and
        # returning it would draw a program a few square metres under the one
        # the solver priced.
        lo, hi = 0.0, _band_ceiling(anchor, region)
        for _ in range(max(1, bisect_steps)):
            mid = (lo + hi) / 2.0
            if _band(anchor, region, mid).area < target_area_m2:
                lo = mid
            else:
                hi = mid
        paved = _band(anchor, region, hi)

    paved = _largest_bays(paved, max_bays)
    if paved is None or paved.is_empty or paved.area <= 0:
        return _no_parking("no_fit")

    placed = paved.area
    complete = placed >= target_area_m2 - _PARKING_AREA_TOLERANCE_M2
    return Parking(
        geometry=paved,
        status="fitted" if complete else "shrunk",
        area_m2=min(placed, target_area_m2),
        # The band's own dimension, and the only one it has: how far out from
        # the building the paving reaches. A rectangle's width and angle went
        # with the rectangle search - see `Parking` on why they are now null.
        depth_m=_band_depth(anchor, paved),
        num_bays=len(getattr(paved, "geoms", [paved])),
    )


def _band(anchor: BaseGeometry, region: BaseGeometry, distance: float):
    """``region`` within ``distance`` of ``anchor`` - one step of the bisection.

    Mitred like the opening and for the same reason: a rounded buffer of a
    rectangular plate bulges at the corners and would pave a quarter-disc of
    ground the band has not reached yet.
    """
    return region.intersection(anchor.buffer(distance, join_style=2))


def _band_ceiling(anchor: BaseGeometry, region: BaseGeometry) -> float:
    """A distance that certainly covers ``region``, to bisect down from.

    The furthest any point of the region can lie from the anchor is bounded by
    the two bounding boxes together, so this is that diagonal - computed rather
    than fixed at a constant, because a constant too small silently caps the
    band on a large parcel and one too large spends the first halvings on
    distances already known to cover everything.
    """
    minx, miny, maxx, maxy = region.bounds
    anchor_minx, anchor_miny, anchor_maxx, anchor_maxy = anchor.bounds
    span_x = max(maxx, anchor_maxx) - min(minx, anchor_minx)
    span_y = max(maxy, anchor_maxy) - min(miny, anchor_miny)
    return math.hypot(span_x, span_y) + 1.0


def _band_depth(anchor: BaseGeometry, paved: BaseGeometry) -> float:
    """How far the paving reaches from the building, in metres.

    Measured off the drawn polygon rather than carried out of the bisection, so
    it still describes the shape after `_largest_bays` has dropped a piece.
    """
    if paved is None or paved.is_empty:
        return 0.0
    return float(max(anchor.distance(Point(vertex)) for vertex in _vertices(paved)))


def _vertices(geometry: BaseGeometry):
    """Every exterior vertex of ``geometry``, polygon or multipolygon."""
    for part in getattr(geometry, "geoms", [geometry]):
        yield from part.exterior.coords


def _largest_bays(paved: BaseGeometry, max_bays: int):
    """``paved`` cut down to its ``max_bays`` largest pieces.

    A program parked across five scattered scraps is not the program the solver
    priced, so the surplus is dropped rather than counted - and dropping it
    lowers `area_m2`, which is what turns the answer `shrunk`. The largest are
    kept because they are the ones a builder would pave.
    """
    parts = list(getattr(paved, "geoms", [paved]))
    if len(parts) <= max(1, max_bays):
        return paved
    parts.sort(key=lambda part: part.area, reverse=True)
    return shapely.union_all(parts[: max(1, max_bays)])


def placeable_area_m2(
    buildable: BaseGeometry | None,
    *,
    aspect_ratios: Sequence[float] = DEFAULT_ASPECT_RATIOS,
    grid_steps: int = GRID_STEPS,
    shrink_steps: int = SHRINK_STEPS,
    min_footprint_m2: float = MIN_FOOTPRINT_M2,
) -> float:
    """The largest single building ``buildable`` actually holds, in square metres.

    The counterpart of `parking_capacity_m2` for the building, and the answer
    to a question `buildable_area_m2` cannot be asked: an envelope's *area* is
    not a footprint it can take. A skewed parallelogram, an L, a wedge and a
    rectangle of one area hold very different buildings, and `solve_program`
    capping a footprint on the area alone will price a plate that fits nowhere
    on the parcel - which `lot_building_massing` then reports as `shrunk`,
    after the economics have already been computed on it.

    So this is the same rectangle `fit_rectangle` would draw, measured before
    the solve rather than after it, and handed to `solve_program` as
    `Lot.placeable_area_m2`. Deliberately the **same search** at the **same
    settings** the massing asset runs: the whole value of the number is that
    the program the solver prices is the program the placer can draw, and two
    different searches would not agree.

    Asked as ``fit_rectangle(buildable, buildable.area)``: no rectangle can
    hold more area than the envelope it sits in, so the full-size pass either
    settles it outright - a square envelope takes a square of its own area - or
    fails on every ratio and the shrink search returns the best of them. That
    is the maximum over the ratios and both angles.

    It lands **under** the true largest rectangle, which is the direction a
    feasibility bound has to err in, and by more than the bisection alone would
    cost: `DEFAULT_ASPECT_RATIOS` is a ladder of four rungs, so an envelope
    proportioned between two of them - a 9 x 28 m Villeray envelope is 3.11:1
    and there is no such rung - is reported at the best rung that fits inside
    it, a few percent short of its own area. That is deliberate rather than
    tolerated: this is the area `fit_rectangle` will place, and a cap the
    placer cannot draw to would hand the massing a plate to shrink, which is
    the whole thing the cap exists to stop.

    **It is a bound on one building, and that is a real restriction.** A parcel
    that would take two 400 m2 blocks and no single 800 m2 one is reported at
    400. That is the same choice `fit_rectangle` makes and for the same reason
    - this module draws one massing per lot - and unlike the parking, which
    honestly comes in patches and gets `PARKING_MAX_BAYS` of them, a building
    split in two is a different program rather than the same one rearranged.

    Returns 0.0 for an envelope with no geometry and for one that holds nothing
    worth calling a building, which is the reading `Lot.placeable_area_m2`
    gives a missing envelope and the conservative one.
    """
    polygon = _largest_part(buildable)
    if polygon is None or polygon.is_empty or polygon.area <= 0:
        return 0.0
    fitted = fit_rectangle(
        polygon,
        polygon.area,
        aspect_ratios=aspect_ratios,
        grid_steps=grid_steps,
        shrink_steps=shrink_steps,
        min_footprint_m2=min_footprint_m2,
    )
    return float(fitted.placed_footprint_m2)


def parking_capacity_m2(
    ground: BaseGeometry | None,
    *,
    min_depth_m: float = MIN_PARKING_DEPTH_M,
    min_area_m2: float | None = None,
) -> float:
    """How much of ``ground`` a car can stand on, in square metres.

    `parkable_ground` measured rather than described - this is that region's
    area, and it is the number `solve_program` is handed as
    `Lot.parkable_area_m2`. The same function the drawing uses, deliberately:
    a bound computed one way and a polygon drawn another is how the solve and
    `lot_building_massing` came to answer two different questions, and this
    module now has exactly one rule for what ground is parkable.

    **What to pass.** The *yard* - the parcel less the building - wherever the
    building is known, which since `placeable_area_m2` fixed the plate before
    the solve is everywhere the setbacks have geometry. That makes the bound
    exact rather than generous: it is the ground that answer will actually
    have. Passing the bare parcel is the fallback for a partition whose
    envelopes have no polygon, and it overstates the yard by whatever the
    building will stand on.

    **What it used to be.** The largest single parking-shaped *rectangle* the
    parcel held, which was strict in a way that mattered: a yard wrapping a
    building is a band, an L or a ring, and no rectangle reads more than about
    half of it. Across 600 real VSMPE yards the rectangle search kept a median
    34 pct of the ground and this keeps 79, and it does it in 0.12 ms against
    the search's 4.4. The strictness that remains is honest and is
    `parkable_ground`'s to explain: 5.5 m clear in every direction, so a 3 m
    side-yard ribbon parks nothing at any length.

    Returns 0.0 for ground with no geometry and for ground that parks nothing -
    which is the reading `Lot.parkable_area_m2` gives a missing parcel, and the
    conservative one.
    """
    region = parkable_ground(ground, min_depth_m=min_depth_m, min_area_m2=min_area_m2)
    if region is None or region.is_empty:
        return 0.0
    return float(region.area)


def massing_frame(
    hbu: pd.DataFrame,
    setbacks,
    lots=None,
    *,
    aspect_ratios: Sequence[float] = DEFAULT_ASPECT_RATIOS,
    grid_steps: int = GRID_STEPS,
    shrink_steps: int = SHRINK_STEPS,
    min_footprint_m2: float = MIN_FOOTPRINT_M2,
    min_parking_depth_m: float = MIN_PARKING_DEPTH_M,
    parking_max_bays: int = PARKING_MAX_BAYS,
):
    """One building rectangle per lot of ``hbu``, and its surface parking beside it.

    ``hbu`` is `lot_highest_best_use`, ``setbacks`` is `lot_buildable_setbacks`
    as a GeoDataFrame, and ``lots`` is the cadastre - the parcel polygons keyed
    on `lot_uid`. The setbacks join is on the *governing* (lot_uid, feature_id,
    column_index) that `lot_highest_best_use` already chose rather than on the
    lot, because two columns of one grid state different margins and the
    building being drawn is the one the chosen column allows; the cadastre join
    is on the lot alone, because a parcel has one boundary whatever column
    governs it.

    Returns a GeoDataFrame in EPSG:4326 with **two** geometry columns, one row
    per row of ``hbu``:

    * `geometry` - the building, fitted inside the buildable envelope so the
      zone's four margins are respected by construction. None where
      `massing_status` says nothing was drawn.
    * `parking_geometry` - the surface parking, fitted into the parcel less
      that building. None where `parking_status` says nothing was drawn, which
      includes every program that parks underground, in a ground floor bay,
      or not at all. The dug parking is not drawn either: its plate is the
      parcel's rather than the building's (`underground_plate_m2`), and a
      polygon of it would be the lot.

    **The parking is not part of the massing and never joins it.** A surface
    stall is not a building: it is not floor area, it is not a storey, and a
    map that extrudes it to `height_m` would draw a solid where there is
    asphalt. So the two shapes stay in two columns and are published to two
    tables, and the massing polygon is exactly what it was before this existed
    - `footprint_m2` of building, and nothing else.

    ``lots`` may be omitted, in which case every row is `no_lot_geometry`: the
    parking is not checked and, being unchecked, is not drawn either. That is
    the same posture `_read_setbacks` takes about a missing envelope, and for
    the same reason - a rectangle drawn on a parcel whose boundary was not
    consulted would look entirely plausible on a map.

    Every lot keeps its row. A lot with no program, a lot whose footprint would
    not fit and a lot whose parking would not fit are three different answers
    and all three are worth counting.
    """
    import geopandas as gpd

    frame = hbu.copy().reset_index(drop=True)
    envelopes = _buildable_by_key(setbacks)
    parcels = _lots_by_uid(lots)
    metric_crs = METRIC_CRS

    results: list[Massing] = []
    parked: list[Parking] = []
    yards: list[float] = []
    reserved: list[float] = []
    for row in frame.to_dict("records"):
        key = (row.get("lot_uid"), row.get("feature_id"), row.get("column_index"))
        buildable = envelopes.get(key)
        # The piece first, the parcel second: `_lots_by_uid` keys on whichever
        # the frame it was handed carries, and a partition without the pieces
        # falls back to the boundary exactly as it did before them.
        parcel = parcels.get((row.get("lot_uid"), row.get("feature_id")))
        if parcel is None:
            parcel = parcels.get(row.get("lot_uid"))
        if row.get("hbu_status") != "solved":
            results.append(_nothing("no_program"))
            parked.append(_no_parking("no_program"))
            yards.append(float("nan"))
            reserved.append(0.0)
            continue
        massing = fit_rectangle(
            buildable,
            _float(row.get("footprint_m2")),
            aspect_ratios=aspect_ratios,
            grid_steps=grid_steps,
            shrink_steps=shrink_steps,
            min_footprint_m2=min_footprint_m2,
        )
        results.append(massing)

        required = _surface_parking_area(row)
        reserved.append(required)
        if parcel is None:
            parked.append(_no_parking("no_lot_geometry"))
            yards.append(float("nan"))
            continue
        if not required:
            # Asked before the yard is computed: a program that parks nothing
            # on the ground is not a parking failure, and cutting the building
            # out of the parcel to prove it would be work for a row that has
            # nothing to say.
            parked.append(_no_parking("no_parking"))
            yards.append(float("nan"))
            continue
        yard = yard_of(parcel, massing.geometry)
        yards.append(float(yard.area) if yard is not None else float("nan"))
        parked.append(
            fit_parking(
                yard,
                required,
                # What the paving grows out from, so it wraps this building
                # rather than settling somewhere in the parcel: the *drawn*
                # massing, not the solved footprint, since a plate that had to
                # shrink leaves a different yard than the one that did not.
                building=massing.geometry,
                min_depth_m=min_parking_depth_m,
                max_bays=parking_max_bays,
            )
        )

    frame["massing_status"] = [result.status for result in results]
    frame["aspect_ratio"] = [result.aspect_ratio for result in results]
    frame["width_m"] = [result.width_m for result in results]
    frame["depth_m"] = [result.depth_m for result in results]
    frame["rotation_deg"] = [result.rotation_deg for result in results]
    frame["placed_footprint_m2"] = [result.placed_footprint_m2 for result in results]

    footprint = pd.to_numeric(frame.get("footprint_m2"), errors="coerce")
    placed = frame["placed_footprint_m2"]
    # NaN rather than 0 where nothing was drawn: a lot with no program has no
    # shortfall, and reporting one would put it in the same bucket as a lot
    # whose envelope genuinely cannot hold what was costed.
    drawn = frame["massing_status"].isin(("fitted", "shrunk"))
    frame["footprint_shortfall_m2"] = (footprint - placed).where(drawn)
    frame["footprint_fit_pct"] = (
        100.0 * placed / footprint.where(footprint > 0)
    ).where(drawn)
    frame["placed_gross_floor_area_m2"] = (
        placed * pd.to_numeric(frame.get("floors"), errors="coerce")
    ).where(drawn)

    frame["parking_status"] = [result.status for result in parked]
    frame["parking_width_m"] = [result.width_m for result in parked]
    frame["parking_depth_m"] = [result.depth_m for result in parked]
    frame["parking_rotation_deg"] = [result.rotation_deg for result in parked]
    frame["num_parking_bays"] = [result.num_bays for result in parked]
    frame["yard_area_m2"] = yards
    # Collected in the loop rather than recomputed here: this is the number the
    # fit above was measured against, and reading it a second time out of a
    # second `to_dict` pass over the borough would be both slower and one more
    # place for the two to drift apart.
    frame["surface_parking_area_m2"] = reserved
    frame["placed_surface_parking_m2"] = [result.area_m2 for result in parked]

    stall_area = _surface_stall_area_m2()
    required_area = pd.to_numeric(frame["surface_parking_area_m2"], errors="coerce")
    placed_area = frame["placed_surface_parking_m2"]
    # The same NaN-rather-than-zero rule as the footprint above: only a row
    # where parking was actually looked for has a shortfall to report, so a
    # program that dug or bayed is not counted as a parking failure.
    checked = frame["parking_status"].isin(("fitted", "shrunk", "no_fit", "no_yard"))
    frame["surface_parking_shortfall_m2"] = (required_area - placed_area).where(checked)
    frame["surface_parking_fit_pct"] = (
        100.0 * placed_area / required_area.where(required_area > 0)
    ).where(checked)
    # Floor, not round: half a stall of asphalt parks no car, and the whole
    # point of the column is to be the number of stalls that can stand there.
    frame["placed_surface_stalls"] = (
        (placed_area // stall_area).where(checked).astype("Float64")
    )

    # Both shapes are projected on their own. `GeoDataFrame.to_crs` transforms
    # the *active* geometry and leaves any other geometry column where it was,
    # so converting the frame once would silently hand back a parking polygon
    # still in metres - which reads as a valid EPSG:4326 shape somewhere off
    # the coast of Africa.
    building = gpd.GeoSeries(
        [result.geometry for result in results], crs=metric_crs, index=frame.index
    ).to_crs("EPSG:4326")
    parking = gpd.GeoSeries(
        [result.geometry for result in parked], crs=metric_crs, index=frame.index
    ).to_crs("EPSG:4326")

    carried = [
        name
        for name in (*MASSING_COLUMNS, *PARKING_COLUMNS)
        if name in frame.columns
    ]
    output = gpd.GeoDataFrame(
        frame[list(dict.fromkeys(carried))],
        geometry=building,
        crs="EPSG:4326",
    )
    output["parking_geometry"] = parking
    return output


def _surface_stall_area_m2() -> float:
    """One surface stall's ground allowance, in square metres.

    Read off `urban_rag.program` rather than restated, so the area this module
    tries to *draw* is the area the solver charged the yard for. Restating it
    would be a second number that agrees until somebody moves one of them.
    """
    from urban_rag.program import M2_PER_SQFT, SURFACE_STALL_AREA_SQFT

    return SURFACE_STALL_AREA_SQFT * M2_PER_SQFT


def _surface_parking_area(row) -> float:
    """The yard area one program reserved for stalls, in square metres.

    `surface_area_m2` is what `solve_program` set aside and is the number to
    use: it is the model's own reservation, at the hundredth of a square metre
    the model holds areas to, so drawing it is drawing what was solved.

    A partition written before that column existed falls back to the stall
    count times the standard allowance, which is the same figure to within the
    scaling. Returning 0.0 for a program with no surface stalls is what makes
    `no_parking` the status rather than a zero-area rectangle.
    """
    reserved = _float(row.get("surface_area_m2"))
    if math.isfinite(reserved) and reserved > 0:
        return reserved
    stalls = _float(row.get("surface_stalls"))
    if not math.isfinite(stalls) or stalls <= 0:
        return 0.0
    return float(stalls) * _surface_stall_area_m2()


def to_metric(frame):
    """``frame`` in `METRIC_CRS`, which is the CRS a fit has to happen in.

    NAD83 / MTM zone 8, the same projection `comparables` measures ground
    distance in and `postgis` computes frontage and setbacks in - named there
    rather than repeated as a number, so the rectangle drawn here is in the
    same metres the margins were subtracted in.
    """
    if frame is None or len(frame) == 0:
        return frame
    if frame.crs is None:
        frame = frame.set_crs("EPSG:4326")
    return frame.to_crs(METRIC_CRS)


def _buildable_by_key(setbacks) -> dict[tuple, BaseGeometry]:
    """The buildable polygon of each (lot, zone, column), in metres.

    A dict rather than a merge because the fit is a Python loop over rows
    anyway, and a left join of a GeoDataFrame onto a plain one loses the
    geometry column's dtype in a way that is easy to miss and annoying to find.
    """
    if setbacks is None or len(setbacks) == 0:
        return {}
    required = ("lot_uid", "feature_id", "column_index")
    if any(name not in setbacks.columns for name in required):
        return {}
    projected = to_metric(setbacks)
    return {
        (row.lot_uid, row.feature_id, row.column_index): row.geometry
        for row in projected.itertuples(index=False)
        if row.geometry is not None and not row.geometry.is_empty
    }


def _lots_by_uid(lots) -> dict:
    """The ground a surface stall may stand on, in metres, keyed for lookup.

    Keyed on **(lot_uid, feature_id)** where the frame carries a zone - it is
    `silver.lot_zone_pieces`, the ground each zone governs - and on `lot_uid`
    alone where it does not, which is `rag.lots` and the fallback. `massing_frame`
    tries the pair and then the lot, so both shapes work through one dict.

    The pair rather than the lot, and it took the piece grain to see why. A
    parcel boundary does belong to the parcel, so keying on the lot was right
    while one zone answered for it. With both zones answered, each is drawing a
    building and paving a yard on the same 27 044 m2 - so the parking would be
    checked against ground the other program has already built on, and the two
    rectangles could overlap on the map. The piece is the ground each row may
    actually use.

    A buildable envelope is still keyed on the (lot, zone, column) triple
    `_buildable_by_key` uses, because the margins that carved it are printed in
    a *column* and two columns of one grid state different ones.

    An absent frame is an empty dict, which every row then reads as
    `no_lot_geometry` - see `massing_frame` on why that is a status rather
    than a fallback to the buildable envelope.
    """
    if lots is None or len(lots) == 0:
        return {}
    if "lot_uid" not in lots.columns:
        return {}
    by_piece = "feature_id" in lots.columns
    projected = to_metric(lots)
    return {
        ((row.lot_uid, row.feature_id) if by_piece else row.lot_uid): row.geometry
        for row in projected.itertuples(index=False)
        if row.geometry is not None and not row.geometry.is_empty
    }


def _float(value) -> float:
    if value is None or pd.isna(value):
        return float("nan")
    return float(value)
