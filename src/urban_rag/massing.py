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
    # The envelope it was fitted into, for scale.
    "buildable_area_m2",
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
# over a partition
# --------------------------------------------------------------------------


def massing_frame(
    hbu: pd.DataFrame,
    setbacks,
    *,
    aspect_ratios: Sequence[float] = DEFAULT_ASPECT_RATIOS,
    grid_steps: int = GRID_STEPS,
    shrink_steps: int = SHRINK_STEPS,
    min_footprint_m2: float = MIN_FOOTPRINT_M2,
):
    """One rectangle per lot of ``hbu``, fitted inside its own buildable envelope.

    ``hbu`` is `lot_highest_best_use` and ``setbacks`` is
    `lot_buildable_setbacks` as a GeoDataFrame. The join is on the *governing*
    (lot_uid, feature_id, column_index) that `lot_highest_best_use` already
    chose - not on the lot - because two columns of one grid state different
    margins and the building being drawn is the one the chosen column allows.

    Returns a GeoDataFrame in EPSG:4326, one row per row of ``hbu``, with
    `MASSING_COLUMNS` and a `geometry` that is None where `massing_status` says
    nothing was drawn. Every lot keeps its row: a lot with no program and a lot
    whose footprint would not fit are different answers and both are worth
    counting.
    """
    import geopandas as gpd

    frame = hbu.copy().reset_index(drop=True)
    envelopes = _buildable_by_key(setbacks)
    metric_crs = METRIC_CRS

    results: list[Massing] = []
    for row in frame.to_dict("records"):
        key = (row.get("lot_uid"), row.get("feature_id"), row.get("column_index"))
        buildable = envelopes.get(key)
        if row.get("hbu_status") != "solved":
            results.append(_nothing("no_program"))
            continue
        results.append(
            fit_rectangle(
                buildable,
                _float(row.get("footprint_m2")),
                aspect_ratios=aspect_ratios,
                grid_steps=grid_steps,
                shrink_steps=shrink_steps,
                min_footprint_m2=min_footprint_m2,
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

    geometry = gpd.GeoSeries(
        [result.geometry for result in results], crs=metric_crs, index=frame.index
    )
    output = gpd.GeoDataFrame(
        frame[[name for name in MASSING_COLUMNS if name in frame.columns]],
        geometry=geometry,
        crs=metric_crs,
    )
    # Back to the CRS the rest of the tree is written in. The fit had to happen
    # in metres - a footprint in square degrees is not a footprint - and
    # EPSG:4326 is what every other geoparquet here carries.
    return output.to_crs("EPSG:4326")


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


def _float(value) -> float:
    if value is None or pd.isna(value):
        return float("nan")
    return float(value)
