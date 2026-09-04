"""The grid the map's low-zoom layers are dissolved onto, and what each one
measures once it is.

Free of Dagster and of psycopg the way `program`, `massing` and `hbu` are:
everything here is arithmetic on the Web Mercator tile grid plus a declaration
of what a cell of each layer carries. `urban_rag.postgis` turns these into the
rollup SQL, `urban_rag.aggregate_assets` runs it per partition, and
hbu_rag_map serves the result.

---------------------------------------------------------------------------
Why the cell is a tile
---------------------------------------------------------------------------

A cell is exactly one Web Mercator tile, at a zoom finer than the one being
looked at. The map serves display zoom Z from cells at ``Z + ZOOM_OFFSET``,
and that offset is the whole design:

* a tile at zoom Z contains exactly ``4 ** ZOOM_OFFSET`` tiles at zoom
  ``Z + ZOOM_OFFSET`` — 256 of them at the offset used here — so **a served
  tile carries at most 256 features at every zoom, by construction**. Not by a
  LIMIT that truncates silently, and not by a simplification whose cost still
  scales with the borough;
* cell edges are tile edges, so nothing straddles a seam, is cut by one, or is
  counted twice either side of it;
* at that offset a cell is ``256 / 2 ** ZOOM_OFFSET`` = 16 screen pixels:
  coarse enough to read as a texture, fine enough that the borough's shape
  survives in it.

The alternative — a metric grid somebody chose, 100 m or 250 m squares — has
none of those properties. Its cells straddle tiles at every zoom, the count
per tile varies with latitude and with which zoom you are at, and the whole
thing has to be re-cut when the display zoom changes. The tile grid is the one
grid the renderer already agrees with.

---------------------------------------------------------------------------
Two assignments, both additive
---------------------------------------------------------------------------

A lot straddles cells, and there is no single assignment that is correct for
every measure taken over it.

* Count it in every cell its geometry touches and the counts double-count —
  and the pyramid that rolls four children into one parent compounds that at
  every level.
* Assign it to one cell and take its *area* there and the area is wrong: the
  cell has been credited with land outside it.

So each measure uses the assignment that is exact for it, and both happen to
be additive up the pyramid, which is what makes the pyramid legitimate:

* **Clipped** — area, length, coverage. From ``ST_Intersection`` with the
  cell. Cells at one level are disjoint and tile their parent, so a parent's
  value is the sum of its four children's. These are the names in
  `UNIVERSAL_MEASURES`.
* **By representative point** — counts, floor areas, dwellings, money. From
  ``ST_PointOnSurface``: one point per feature, landing in exactly one cell.
  The level-Z cell holding that point is the parent of the level-(Z+1) cell
  holding it, so these roll up exactly too. These are a layer's
  `point_measures`.

A **ratio** is neither, and is the one to be careful with. `LayerSpec.value`
declares one as ``(numerator, denominator, scale)`` over names from either
group, and it is recomputed from those two at every level rather than averaged
from the children. The mean of per-lot percentages is not the percentage of
the sums, and on the capacity layer the second is what a reader of the map
believes they are being shown.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

#: How many zoom levels finer than the display zoom a cell is. Four, so a
#: served tile holds 16x16 = 256 cells and a cell is 16 screen pixels. Raising
#: it makes the map finer and every tile four times heavier; lowering it makes
#: the cells coarse enough to read as a chessboard laid over the borough.
ZOOM_OFFSET = 4

#: Cell zooms built for every layer, coarsest first. The pyramid is built from
#: the finest downwards, so `max(CELL_ZOOMS)` is the only level computed from
#: raw geometry and the rest are rollups of it.
#:
#: 1..19 — every level of the Web Mercator grid a tile request can name, so
#: `cell_zoom_for` never has to be clamped to a level built for a different
#: zoom. 12..19 is the range the map's own gates use today (display zooms
#: 8..15, from `MAP_MIN_ZOOM` in hbu_rag_map up to the highest gate any layer
#: has); everything below it is built so that lowering that floor further, or
#: pointing anything else at this table at country or continent zoom, is a
#: change in the other repository rather than a re-materialisation of every
#: borough. Lowering it from 11 to 8 was exactly that change, and it cost
#: nothing here.
#:
#: Uniform across layers for the same reason it always was: a layer's gate
#: moves whenever somebody tunes what is legible, and a table that depended on
#: those numbers would follow a one-line change over there with a full rebuild.
#:
#: Reaching down to 1 is close to free in rows and is **not** free in bytes.
#: Each level holds a quarter of the cells of the one below, so 1..14 together
#: add well under 1% of the row count of 15..19 — but a borough is a few
#: kilometres across, so from around zoom 11 down it fits inside a single cell
#: and every level below that stores another copy of its whole dissolved
#: union. See `urban_rag.postgis` on why those copies are stored intact rather
#: than simplified - the measures on the row are taken over the stored shape,
#: so thinning it here would break the rollup. hbu_rag_map thins it on the way
#: out instead: below its `AGGREGATE_OUTLINE_ZOOM` a tile is simplified at
#: request time, which is the one place it can be done without touching a
#: number anything reads.
CELL_ZOOMS: tuple[int, ...] = tuple(range(1, 20))

#: The zoom the pyramid is seeded at, from real geometry.
BASE_CELL_ZOOM = max(CELL_ZOOMS)

#: The map layers this module aggregates, in hbu_rag_map's own draw order.
#: `zones` is absent and is not an oversight: a zoning polygon is already
#: block-sized, it draws from zoom 0 and it has no gate to fill in.
LAYERS: tuple[str, ...] = ("capacity", "streets", "lots", "buildings", "massing")


@dataclass(frozen=True)
class LayerSpec:
    """One map layer's aggregate: where its rows come from and what a cell says.

    ``source`` is the relation the base level is computed from and ``geometry``
    the column on it that is dissolved. ``where`` is the partition screen, in
    the ``%(neighborhood)s`` / ``%(scrape_date)s`` parameters every query in
    `urban_rag.postgis` already uses.

    ``point_measures`` are the per-feature sums, as ``{output name: SQL over
    the source row}``. They are summed over the features whose representative
    point lands in the cell, and they are what `attributes` on the table
    carries.

    ``value`` names the shaded number, as ``(numerator, denominator, scale)``
    over names from ``point_measures`` or from `UNIVERSAL_MEASURES`. It is
    computed as ``scale * numerator / denominator`` and recomputed at every
    level of the pyramid rather than averaged - see the module docstring on
    why that distinction is load-bearing. ``value_kind`` says what the result
    means and travels on every row.
    """

    name: str
    source: str
    geometry: str
    where: str
    value_kind: str
    value: tuple[str, str, float]
    point_measures: dict[str, str]
    #: What survives the clip: 3 for the polygon layers, 2 for `streets`.
    #: Clipping a shape against a box gives back its own dimension plus the
    #: lower-dimensional pieces where it grazes an edge - a polygon clip can
    #: come back carrying stray edges and points - and unioning those into the
    #: cell would draw hairlines across the map. `ST_CollectionExtract` at this
    #: dimension is what drops them, and it is declared per layer rather than
    #: inferred so a line layer can never quietly be read as an areal one.
    dimension: int = 3

    @property
    def numerator(self) -> str:
        return self.value[0]

    @property
    def denominator(self) -> str:
        return self.value[1]

    @property
    def scale(self) -> float:
        return self.value[2]


#: Every cell carries these four regardless of layer, computed the same way
#: for all five: the area and the length of the dissolved clip, the cell's own
#: area, and the count of features whose point landed in it. A layer's `value`
#: may name any of them as a numerator or a denominator.
#:
#: Both the area and the length are taken on every layer rather than branching
#: on what the geometry is, because PostGIS already answers the question the
#: branch would ask: `ST_Area` of linework is 0 and `ST_Length` of an areal
#: geometry is 0 (its perimeter is `ST_Perimeter`, which is not wanted here).
#: So `streets` reports a length and no area, the four polygon layers report an
#: area and no length, and neither needs a special case.
UNIVERSAL_MEASURES: tuple[str, ...] = (
    "dissolved_area_m2",
    "dissolved_length_m",
    "cell_area_m2",
    "feature_count",
)


#: The partition screen every layer takes, in the parameter names every query
#: in `urban_rag.postgis` already binds.
_PARTITION = (
    "{alias}.neighborhood = %(neighborhood)s "
    "AND {alias}.scrape_date = %(scrape_date)s::date"
)


def _layers() -> dict[str, LayerSpec]:
    """The five specs, keyed by layer name.

    Schemas are written out the way the rest of `urban_rag.postgis` writes
    them - `rag` for the working set the spatial joins are computed over,
    `silver` and `gold` for this platform's own tables.
    """
    partition = _PARTITION

    specs = (
        # The one the whole exercise is for. A cell shaded by the share of its
        # permitted floor that is standing is the borough-wide read of
        # gold.lot_redevelopment_gap — "where is the headroom" — which at
        # present is a question the map refuses to answer at all below zoom 15.
        LayerSpec(
            name="capacity",
            source=(
                "rag.lots l "
                "JOIN gold.lot_redevelopment_gap g "
                "  ON g.lot_uid = l.lot_uid "
                " AND g.neighborhood = l.neighborhood "
                " AND g.scrape_date = l.scrape_date"
            ),
            geometry="l.geom",
            where=partition.format(alias="l"),
            value_kind="used_pct",
            # Recomputed from the two sums at every level — never the mean of
            # the per-lot percentages. See the module docstring.
            value=("existing_floor_area_m2", "hbu_floor_area_m2", 100.0),
            point_measures={
                "existing_floor_area_m2": "g.existing_floor_area_m2",
                "hbu_floor_area_m2": "g.hbu_floor_area_m2",
                "existing_num_dwellings": "g.existing_num_dwellings",
                "hbu_num_dwellings": "g.hbu_num_dwellings",
                "dwelling_gap": "g.dwelling_gap",
                # The screen the map's own filter uses, carried as a count so
                # a cell can say "31 of 44 lots here are under-built" without
                # a second layer.
                "num_underbuilt": "CASE WHEN g.is_underbuilt THEN 1 ELSE 0 END",
            },
        ),
        # Length rather than count: a cell holding one long side and one
        # holding six stubs are not the same street grid, and the count cannot
        # tell them apart. Clipped, so a side crossing four cells contributes
        # its own piece to each.
        LayerSpec(
            name="streets",
            source="silver.neighborhood_streets s",
            geometry="s.geom",
            where=partition.format(alias="s"),
            value_kind="street_km_per_km2",
            value=("dissolved_length_m", "cell_area_m2", 1000.0),
            point_measures={},
            dimension=2,
        ),
        LayerSpec(
            name="lots",
            source="rag.lots l",
            geometry="l.geom",
            where=partition.format(alias="l"),
            value_kind="lots_per_km2",
            value=("feature_count", "cell_area_m2", 1_000_000.0),
            point_measures={
                "lot_area_m2": "COALESCE(l.area_m2, ST_Area(geography(l.geom)))",
            },
        ),
        # The built fabric, as the share of the ground standing on it. The one
        # layer whose aggregate is a straight coverage number, because that is
        # what a footprint layer *is* once you stop being able to see the
        # individual footprints.
        LayerSpec(
            name="buildings",
            source="rag.buildings b",
            geometry="b.geom",
            where=partition.format(alias="b"),
            value_kind="built_coverage_pct",
            value=("dissolved_area_m2", "cell_area_m2", 100.0),
            point_measures={
                "footprint_area_m2": "COALESCE(b.area_m2, ST_Area(geography(b.geom)))",
            },
        ),
        # The proposal, per hectare of cell rather than per hectare of lot:
        # the question a zoomed-out massing layer answers is "where would the
        # housing go", and that is a density over ground, not over parcel.
        LayerSpec(
            name="massing",
            source="gold.lot_building_massing m",
            geometry="m.geom",
            where=partition.format(alias="m"),
            value_kind="proposed_dwellings_per_ha",
            value=("num_dwellings", "cell_area_m2", 10_000.0),
            point_measures={
                "num_dwellings": "m.num_dwellings",
                "placed_footprint_m2": "m.placed_footprint_m2",
                "placed_gross_floor_area_m2": "m.placed_gross_floor_area_m2",
                "floors": "m.floors",
                # Counted, not averaged: "9 of these 40 had to be shrunk" is
                # the finding, and a mean fit percentage over a cell hides
                # which lots it is about.
                "num_shrunk": "CASE WHEN m.massing_status = 'shrunk' THEN 1 ELSE 0 END",
            },
        ),
    )
    return {spec.name: spec for spec in specs}


def layer_spec(layer: str) -> LayerSpec:
    """The spec for ``layer``.

    Raises rather than returning None: every caller here is building SQL, and
    a missing spec would otherwise become a syntax error several frames away
    from the name that was wrong.
    """
    specs = _layers()
    try:
        return specs[layer]
    except KeyError:
        raise KeyError(
            f"{layer!r} has no aggregate spec in urban_rag.tile_grid; "
            f"known: {', '.join(sorted(specs))}"
        ) from None


def cell_zoom_for(display_zoom: int) -> int:
    """The cell level a display zoom is served from."""
    return display_zoom + ZOOM_OFFSET


def cells_per_tile() -> int:
    """How many cells a served tile holds. The bound this design exists for."""
    return 4**ZOOM_OFFSET


def tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """``(west, south, east, north)`` of tile ``z/x/y`` in EPSG:4326.

    The inverse of `cell_of`, and used by the tests to check the two agree.
    Kept here rather than left to PostGIS because a grid whose arithmetic only
    exists inside a SQL string is a grid nothing can assert against.
    """
    span = 1 << z
    west = x / span * 360.0 - 180.0
    east = (x + 1) / span * 360.0 - 180.0
    north = _lat_of(y, span)
    south = _lat_of(y + 1, span)
    return west, south, east, north


def cell_of(lon: float, lat: float, z: int) -> tuple[int, int]:
    """The ``(x, y)`` of the zoom-``z`` tile containing ``lon``/``lat``.

    The Python spelling of what the rollup does in SQL. Clamped to the grid at
    both ends: a longitude of exactly 180 would otherwise index one column
    past the last, and Mercator has no latitude beyond about 85.05 to index at
    all.
    """
    span = 1 << z
    x = int(math.floor((lon + 180.0) / 360.0 * span))
    lat = max(min(lat, 85.0511287798), -85.0511287798)
    radians = math.radians(lat)
    y = int(
        math.floor(
            (1.0 - math.asinh(math.tan(radians)) / math.pi) / 2.0 * span
        )
    )
    return max(0, min(span - 1, x)), max(0, min(span - 1, y))


def parent_of(x: int, y: int, levels: int = 1) -> tuple[int, int]:
    """The cell ``levels`` zooms coarser that contains ``(x, y)``.

    A halving per level, which is why the pyramid rolls up exactly: the four
    children of a cell are precisely the four that reduce to it.
    """
    return x >> levels, y >> levels


def _lat_of(y: int, span: int) -> float:
    """The northern latitude of tile row ``y`` on a grid ``span`` rows tall."""
    return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / span))))
