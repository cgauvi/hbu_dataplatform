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

`solve_program` has four places to put a stall and one of them is the ground:
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
where no car can stand at all. So a parking rectangle here is fitted **depth
first** - at least `MIN_PARKING_DEPTH_M`, the length of a stall - and the width
follows from the area. `parking_capacity_m2` is the same question asked of the
bare parcel and handed to `solve_program` as `Lot.parkable_area_m2`, which is
what keeps the answer from being drawn on ground it never fitted;
`surface_parking_fit_pct` is what is left over once a real building has taken
its place on the lot.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd
import shapely
from shapely.geometry import Polygon
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

#: The narrow side of one stall, and the floor on the other dimension: a
#: rectangle 5.5 m deep and 30 cm wide is not a parking space either.
MIN_PARKING_WIDTH_M = 2.6

#: Depths tried between `MIN_PARKING_DEPTH_M` and the square, shallowest
#: first. Four is enough because the ends of that range are the two layouts
#: that actually get built - one row of stalls down a side yard, and a square
#: court on a lot with room - and the middle of it is interpolation between
#: them rather than a third kind of parking lot.
PARKING_DEPTH_STEPS = 4

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

#: How much of its own minimum rotated rectangle a parcel may fail to fill and
#: still be treated as a rectangle by `parking_capacity_m2`. A thousandth of the
#: area is a couple of square decimetres on a Villeray lot - the survey's own
#: rounding rather than a shape - and the shortcut it buys is exact for a true
#: rectangle and the common case for a cadastre.
_RECTANGULAR_TOLERANCE = 0.001

#: How far short of the reserved area the bays may fall and still be `fitted`.
#: The total is reassembled from patches each bisected into place, so it lands
#: a little under the target even where the ground is plainly there. One square
#: metre is under four percent of a stall and an order of magnitude below the
#: precision of anything upstream.
_PARKING_AREA_TOLERANCE_M2 = 1.0

#: The same two searches, coarser, for `parking_capacity_m2`. That one runs
#: over every lot of a borough inside `lot_development_programs` rather than
#: once per drawn massing, and it is answering "does this parcel hold parking
#: at all" rather than "where exactly does it go" - a question a coarser grid
#: settles just as well, and the cheap rejection above settles outright for
#: most of the parcels that fail it.
CAPACITY_GRID_STEPS = 5
CAPACITY_SHRINK_STEPS = 6

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
    # The program parks nothing on the yard - it dug, decked, bayed, or owes
    # no stall at all. Nothing to draw, and not a failure to draw it.
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
    "above_grade_parking_floors",
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
    # The rectangle itself. `parking_depth_m` is the dimension the by-law
    # states and the search holds; `parking_width_m` is what the area then
    # asks for.
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
    `width_m`, `depth_m` and `rotation_deg` describe the **largest** bay, since
    a single set of dimensions cannot describe several; `num_bays` is what says
    whether they describe all of it.
    """

    geometry: BaseGeometry | None
    status: str
    area_m2: float = 0.0
    width_m: float = 0.0
    depth_m: float = 0.0
    rotation_deg: float = 0.0
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


def fit_parking(
    yard: BaseGeometry | None,
    target_area_m2: float,
    *,
    min_depth_m: float = MIN_PARKING_DEPTH_M,
    min_width_m: float = MIN_PARKING_WIDTH_M,
    depth_steps: int = PARKING_DEPTH_STEPS,
    grid_steps: int = GRID_STEPS,
    shrink_steps: int = SHRINK_STEPS,
    max_bays: int = PARKING_MAX_BAYS,
) -> Parking:
    """The surface parking of ``target_area_m2``, drawn on ``yard``.

    **Up to `max_bays` patches, largest first.** This is the one place the
    parking search is more generous than the building's, and the difference is
    real rather than a convenience: a building is one massing or it is nothing,
    while parking honestly comes in pieces. A building across the middle of its
    parcel leaves a front yard and a rear yard, and stalls in both is the
    ordinary answer rather than a compromise - so the largest rectangle is
    placed, cut out of the yard, and the search runs again on what is left
    until the program's area is met or nothing more will fit. Insisting on a
    single rectangle would report that lot at half its real capacity, which
    would make `surface_parking_fit_pct` noise on the common case.

    Greedy, and therefore not optimal: taking the biggest bay first can leave
    two pieces where a smaller first cut would have left one good one. It errs
    towards *less* parking placed, never more, which is the direction a sanity
    check should err in - it can under-state a yard and can never claim ground
    that is not there.

    ``yard`` is what `yard_of` leaves - the parcel less the building - in a
    **projected** CRS whose unit is the metre, the same requirement
    `fit_rectangle` makes and for the same reason.

    The rectangle is fitted **depth first**, which is the one way this search
    differs from the building's. A building is looked for at a few aspect
    *ratios* because nothing in a grid says how long a building is. A parking
    area has a dimension that is stated, and 5.5 m of it is not negotiable: a
    strip of yard shallower than a stall is long holds no car whatever its
    area. So the depths are tried from `min_depth_m` upwards and the width
    follows from the area, rather than the ratio being chosen and the two
    dimensions falling out of it.

    Shallowest first is the realistic order as well as the cheap one. A single
    row of stalls down a side or rear yard is what a Montreal parcel actually
    parks on, and it is the shape most likely to fit what a building has left;
    a square court is what a lot with room to spare gets, and it is the far end
    of the same ladder.

    Depths stop at ``sqrt(target_area_m2)``. Past that the "depth" is the
    longer side, which is the same rectangle turned ninety degrees - and both
    angles are tried - so carrying on would only re-test shapes already tested.

    Returns `Parking`, whose `status` is one of `PARKING_STATUSES`. A yard that
    cannot take the whole reserved area is `shrunk` rather than dropped,
    exactly as a footprint is: the gap is how much of the program's parking is
    standing on ground that cannot hold it, and hiding it would hide the lots
    worth looking at.
    """
    if target_area_m2 is None or not math.isfinite(target_area_m2):
        return _no_parking("no_parking")
    if target_area_m2 <= 0.0:
        return _no_parking("no_parking")
    if yard is None or yard.is_empty or yard.area <= 0:
        return _no_parking("no_yard")

    smallest = min_depth_m * min_width_m
    remaining = yard
    outstanding = target_area_m2
    bays: list[Parking] = []
    for _ in range(max(1, max_bays)):
        if outstanding < smallest:
            break
        bay = _fit_one_bay(
            remaining,
            outstanding,
            min_depth_m=min_depth_m,
            min_width_m=min_width_m,
            depth_steps=depth_steps,
            grid_steps=grid_steps,
            shrink_steps=shrink_steps,
        )
        if bay is None:
            break
        bays.append(bay)
        outstanding -= bay.area_m2
        if bay.status == "fitted":
            break
        # Cut the bay out and look again in what is left. Buffered by the fit
        # epsilon so the difference does not leave a zero-width sliver along
        # the seam - `_largest_part` would then hand the next pass a shape no
        # rectangle can sit in, and the search would spend a full ladder on it.
        remaining = remaining.difference(bay.geometry.buffer(_FIT_EPSILON_M))
        if remaining.is_empty or remaining.area <= 0:
            break

    if not bays:
        # There is a yard; nothing of one stall's dimensions stands in it.
        return _no_parking("no_fit")

    placed_area = sum(bay.area_m2 for bay in bays)
    largest = max(bays, key=lambda bay: bay.area_m2)
    geometry = (
        bays[0].geometry
        if len(bays) == 1
        else shapely.union_all([bay.geometry for bay in bays])
    )
    # `fitted` on the whole program's area rather than on the last bay's, and
    # compared with a tolerance because the area is reassembled from bays that
    # were each bisected into place. A square metre either way is four
    # centimetres of a stall.
    complete = placed_area >= target_area_m2 - _PARKING_AREA_TOLERANCE_M2
    return Parking(
        geometry=geometry,
        status="fitted" if complete else "shrunk",
        area_m2=min(placed_area, target_area_m2),
        width_m=largest.width_m,
        depth_m=largest.depth_m,
        rotation_deg=largest.rotation_deg,
        num_bays=len(bays),
    )


def _fit_one_bay(
    yard: BaseGeometry | None,
    target_area_m2: float,
    *,
    min_depth_m: float,
    min_width_m: float,
    depth_steps: int,
    grid_steps: int,
    shrink_steps: int,
) -> Parking | None:
    """The largest single patch of asphalt ``yard`` holds, up to the target.

    One pass of the ladder `fit_parking` runs up to `PARKING_MAX_BAYS` times.
    Returns ``None`` where nothing of one stall's dimensions fits, which is
    what ends that loop.
    """
    polygon = _largest_part(yard)
    if polygon is None or polygon.is_empty or polygon.area <= 0:
        return None
    if polygon.area < min_depth_m * min_width_m:
        # Not one stall's worth of ground, whatever shape it is in.
        return None

    angles = _candidate_angles(polygon)
    centres = _candidate_centres(polygon, grid_steps)
    if not len(centres):
        return None
    shapely.prepare(polygon)
    frames = _frames(polygon, angles)
    depths = _parking_depths(target_area_m2, min_depth_m, depth_steps)

    # Full size, shallowest depth first. A hit is the whole of what is still
    # outstanding and nothing later in the ladder can improve on it.
    if target_area_m2 <= polygon.area:
        for depth in depths:
            width = target_area_m2 / depth
            if width < min_width_m:
                continue
            for angle, frame in zip(angles, frames):
                placed = _place(
                    polygon, centres, target_area_m2, width / depth, angle, frame
                )
                if placed is not None:
                    return Parking(
                        geometry=placed,
                        status="fitted",
                        area_m2=target_area_m2,
                        width_m=width,
                        depth_m=depth,
                        rotation_deg=angle,
                        num_bays=1,
                    )

    # Nothing fits whole: at each depth, the widest rectangle that does. The
    # *width* is bisected rather than the area, unlike `_shrink_to_fit`. The
    # depth is a stated dimension being held rather than a proportion being
    # scaled, and shrinking the area at a fixed ratio would hand back a
    # shallower rectangle - a shape the stalls do not fit in, reported as
    # though they did.
    best: Parking | None = None
    for angle, frame in zip(angles, frames):
        # The depths are re-laddered against the *yard* here rather than
        # against the target. Above, `sqrt(target)` is the right ceiling
        # because a deeper rectangle of that exact area is a wider one turned
        # ninety degrees. Down here the area is no longer fixed - what is being
        # looked for is the biggest rectangle this ground holds - so a ladder
        # pinned to the target makes the answer depend on how much parking was
        # asked for, and a yard reported 109.96 m2 against ten stalls came back
        # 109.57 against forty. The ground has not moved; only the sampling
        # had.
        for depth in _parking_depths_for(frame, min_depth_m, depth_steps):
            if frame.max_x - frame.min_x < min_width_m:
                continue
            # Measured against the *yard's* full extent, never against the
            # target, and this is the whole of what keeps the answer stable.
            # Bisecting up to `target / depth` makes the resolution depend on
            # how much parking was asked for - eight halvings of a 200 m
            # ceiling settle to 80 cm and eight of a 20 m ceiling to 8 cm - so
            # the same yard came back 119.7 m2 against ten stalls and 118.9
            # against forty. How much ground there is at a given depth is a
            # fact about the ground.
            width, placed = _widest_at_depth(
                polygon,
                centres,
                depth,
                angle,
                frame,
                min_width_m=min_width_m,
                ceiling_m=frame.max_x - frame.min_x,
                steps=shrink_steps,
            )
            if placed is None:
                continue
            # The target is applied afterwards, as a clamp. Nobody is being
            # sold more asphalt than the program reserved, and clamping a
            # stable measurement is monotone in the target where bisecting to
            # it was not.
            wanted = target_area_m2 / depth
            if wanted < width:
                clamped = _place(
                    polygon, centres, wanted * depth, wanted / depth, angle, frame
                )
                if clamped is not None:
                    width, placed = wanted, clamped
            area = width * depth
            if best is None or area > best.area_m2:
                best = Parking(
                    geometry=placed,
                    # `fitted` where the clamp is what stopped it: this depth
                    # was not on the target's own ladder, and the ground turned
                    # out to hold the whole reservation at it.
                    status=(
                        "fitted"
                        if area >= target_area_m2 - _PARKING_AREA_TOLERANCE_M2
                        else "shrunk"
                    ),
                    area_m2=area,
                    width_m=width,
                    depth_m=depth,
                    rotation_deg=angle,
                    num_bays=1,
                )
    return best


def parking_capacity_m2(
    lot: BaseGeometry | None,
    *,
    min_depth_m: float = MIN_PARKING_DEPTH_M,
    min_width_m: float = MIN_PARKING_WIDTH_M,
    depth_steps: int = PARKING_DEPTH_STEPS,
    grid_steps: int = CAPACITY_GRID_STEPS,
    shrink_steps: int = CAPACITY_SHRINK_STEPS,
) -> float:
    """The most parking-shaped ground ``lot`` holds, in square metres.

    This is the number `solve_program` is handed as `Lot.parkable_area_m2`, and
    it is a **bound on the parcel rather than a layout on it**: the largest
    single rectangle at least `min_depth_m` deep that fits anywhere inside the
    lot boundary, with no building subtracted.

    The building is left out on purpose. At solve time there is no building to
    subtract - its footprint is the decision this cap is an input to - so a cap
    that assumed one would forbid programs the parcel can perfectly well take.

    **It errs in both directions, and knowing which is which matters.**
    Ignoring the building makes it generous: no particular answer will have
    this much yard once its plate is down. Insisting on a *single* rectangle
    makes it strict, and this is the half worth watching: unlike `fit_parking`,
    which pieces a program's parking together out of up to `PARKING_MAX_BAYS`
    patches, this measures one. An L-shaped or two-lobed parcel is therefore
    reported at the size of its better lobe, and a program wanting more than
    that is refused parking the land could arguably have given it. That is
    tolerable because the strict half only bites on parcels that are already
    odd, because the generous half is much the larger effect on ordinary ones,
    and because the alternative - running the full multi-bay search over every
    lot of a borough before a single program is solved - is not what the bound
    is worth.

    What it decisively rules out is the class of answer that made it worth
    computing at all: stalls standing on a four-metre ribbon, on a triangular
    remnant, on the tail of an L. `fit_parking` asks the exact question once a
    building has been placed, and `surface_parking_fit_pct` is what it answers
    with.

    Returns 0.0 for a parcel that holds no parking at any size, and for one
    with no geometry at all - which is the reading `Lot.parkable_area_m2`
    gives a missing lot, and the conservative one.
    """
    polygon = _largest_part(lot)
    if polygon is None or polygon.is_empty or polygon.area <= 0:
        return 0.0

    # Two answers off the minimum rotated rectangle, before any search. This
    # function runs once per parcel of a borough ahead of every solve, so the
    # cases it can settle in a few multiplications are worth settling there.
    bounding = polygon.minimum_rotated_rectangle
    sides = _rectangle_sides(bounding)

    # A parcel that cannot hold one car in either orientation holds no parking
    # at any angle. The bound is exact in this direction: a rectangle inside
    # the polygon is inside the polygon's minimum rotated rectangle too.
    #
    # Note which dimension goes with which. A car needs a stall's *length* one
    # way and a stall's *width* the other, so the test is on the long side
    # against `min_depth_m` and the short side against `min_width_m` - not on
    # the short side against the depth. Written the stricter way this rejected
    # every parcel under 5.5 m wide, which is a driveway: a four-metre strip
    # parks cars in single file, parallel to its own length, and telling a
    # borough of them to build parkades instead would be a large and confident
    # error.
    if sides is not None and (
        min(sides) < min_width_m or max(sides) < min_depth_m
    ):
        return 0.0

    # And a parcel that *is* a rectangle is its own answer - the largest
    # rectangle inside a rectangle is the rectangle - which the search would
    # otherwise spend forty-eight `contains` calls rediscovering to within the
    # precision of its own bisection. This is the common case rather than a
    # special one: a Montreal cadastral lot is a rectangle perpendicular to the
    # street, and the exact answer here is better than the searched one as well
    # as cheaper.
    if (
        sides is not None
        and not bounding.is_empty
        and bounding.area > 0
        and bounding.difference(polygon).area <= _RECTANGULAR_TOLERANCE * bounding.area
    ):
        return float(polygon.area)

    angles = _candidate_angles(polygon)
    centres = _candidate_centres(polygon, grid_steps)
    if not len(centres):
        return 0.0
    shapely.prepare(polygon)
    frames = _frames(polygon, angles)

    best = 0.0
    for angle, frame in zip(angles, frames):
        span_depth = frame.max_y - frame.min_y
        span_width = frame.max_x - frame.min_x
        if span_depth < min_depth_m or span_width < min_width_m:
            continue
        for depth in np.linspace(min_depth_m, span_depth, depth_steps):
            width, _ = _widest_at_depth(
                polygon,
                centres,
                float(depth),
                angle,
                frame,
                min_width_m=min_width_m,
                ceiling_m=span_width,
                steps=shrink_steps,
            )
            best = max(best, width * float(depth))
    return best


def _parking_depths(
    target_area_m2: float, min_depth_m: float, steps: int
) -> tuple[float, ...]:
    """The depths to try for a parking area of ``target_area_m2``.

    One stall's length up to the square, which is where the two axes meet: a
    rectangle deeper than that is a wider one turned ninety degrees, and both
    angles are tried anyway. A target too small to make a square that deep gets
    the single depth the floor allows.
    """
    square = math.sqrt(target_area_m2)
    if square <= min_depth_m or steps <= 1:
        return (min_depth_m,)
    return tuple(float(depth) for depth in np.linspace(min_depth_m, square, steps))


def _parking_depths_for(
    frame: "_Frame", min_depth_m: float, steps: int
) -> tuple[float, ...]:
    """The depths to try against a *yard* rather than against a target area.

    Used once the full area is known not to fit, where what is being looked for
    is the largest rectangle the ground holds at any legal depth. That is a
    property of the ground, so the ladder is measured off the ground: one
    stall's length up to the yard's own extent along this angle.
    """
    span = frame.max_y - frame.min_y
    if span <= min_depth_m or steps <= 1:
        return (min_depth_m,)
    return tuple(float(depth) for depth in np.linspace(min_depth_m, span, steps))


def _widest_at_depth(
    polygon: Polygon,
    centres: np.ndarray,
    depth_m: float,
    angle_deg: float,
    frame: "_Frame",
    *,
    min_width_m: float,
    ceiling_m: float,
    steps: int,
) -> tuple[float, Polygon | None]:
    """The widest rectangle ``depth_m`` deep that fits, bisecting the width.

    Expressed to `_place` as an area and a ratio, which is what that function
    takes: a rectangle of area ``w x d`` at ratio ``w / d`` has half-sides
    ``w / 2`` and ``d / 2``, so holding the depth is a matter of moving both
    arguments together rather than of a second placement routine.
    """
    low, high = 0.0, ceiling_m
    found: Polygon | None = None
    found_width = 0.0
    for _ in range(steps):
        middle = (low + high) / 2.0
        if middle < min_width_m:
            # Below one stall's width there is nothing worth drawing, so this
            # half of the interval is walked up rather than searched - the same
            # move `_shrink_to_fit` makes at its own floor.
            low = middle
            continue
        placed = _place(
            polygon, centres, middle * depth_m, middle / depth_m, angle_deg, frame
        )
        if placed is None:
            high = middle
        else:
            low = middle
            found, found_width = placed, middle
    return found_width, found


def _rectangle_sides(rectangle: BaseGeometry | None) -> tuple[float, float] | None:
    """The two side lengths of a `minimum_rotated_rectangle`, or None.

    Degenerate parcels are what the guard is for: a sliver whose minimum
    rotated rectangle collapses to a line has no exterior ring to read four
    corners off, and the caller wants "unknown" rather than an exception.
    """
    if rectangle is None or rectangle.is_empty:
        return None
    try:
        corners = list(rectangle.exterior.coords)[:4]
    except (AttributeError, IndexError, ValueError):
        return None
    if len(corners) < 3:
        return None
    (x0, y0), (x1, y1), (x2, y2) = corners[0], corners[1], corners[2]
    return (math.hypot(x1 - x0, y1 - y0), math.hypot(x2 - x1, y2 - y1))


# --------------------------------------------------------------------------
# over a partition
# --------------------------------------------------------------------------


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
    min_parking_width_m: float = MIN_PARKING_WIDTH_M,
    parking_depth_steps: int = PARKING_DEPTH_STEPS,
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
      includes every program that parks underground, on a deck, in a ground
      floor bay, or not at all.

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
                min_depth_m=min_parking_depth_m,
                min_width_m=min_parking_width_m,
                depth_steps=parking_depth_steps,
                grid_steps=grid_steps,
                shrink_steps=shrink_steps,
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
    # program that dug or decked is not counted as a parking failure.
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
    """Each parcel's boundary, in metres, keyed on `lot_uid`.

    Keyed on the lot alone rather than on the (lot, zone, column) triple
    `_buildable_by_key` uses, and the difference is not an oversight: a
    buildable envelope belongs to a zoning column because the margins that
    carved it are printed in one, while a parcel boundary belongs to the
    parcel. A lot straddling two zones has two envelopes and one shape.

    An absent frame is an empty dict, which every row then reads as
    `no_lot_geometry` - see `massing_frame` on why that is a status rather
    than a fallback to the buildable envelope.
    """
    if lots is None or len(lots) == 0:
        return {}
    if "lot_uid" not in lots.columns:
        return {}
    projected = to_metric(lots)
    return {
        row.lot_uid: row.geometry
        for row in projected.itertuples(index=False)
        if row.geometry is not None and not row.geometry.is_empty
    }


def _float(value) -> float:
    if value is None or pd.isna(value):
        return float("nan")
    return float(value)
