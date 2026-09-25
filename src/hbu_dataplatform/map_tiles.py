"""The map's vector tiles, declared: what each layer's tile carries, at which
zooms, and the grid arithmetic that enumerates them.

Free of Dagster and of psycopg the way `tile_grid` is. `hbu_dataplatform.tile_render`
turns these specs into the ``ST_AsMVT`` statements that produce the tiles,
`hbu_dataplatform.pmtiles_archive` packs a layer's tiles into one PMTiles file, and
`hbu_dataplatform.tile_assets` runs both per partition and writes the result into the
tree - S3 when `S3_BUCKET` is set - where hbu_rag_map reads it with HTTP range
requests and no database in the request path.

---------------------------------------------------------------------------
Why the tiles are built here and not served live
---------------------------------------------------------------------------

Until now hbu_rag_map rendered every tile on demand: a small HTTP server in the
Streamlit process ran one ``ST_AsMVT`` query per 256-pixel square, several
dozen per pan, against the same Postgres the panes read. That worked, and it
put the database in the path of every drag of the map, on a connection pool
sized for a few panes - and it meant a stale-statistics window after every
load turned into a map that stopped answering (see the note on ``ANALYZE`` in
docs/architecture.md).

A tile is a pure function of its ``(layer, z, x, y)`` and the partition behind
it, and the partition changes at most once a month. So the tiles are a gold
asset like everything else derived from the partition: rendered once, by the
same SQL, into a **PMTiles** archive per layer - a single file whose directory
lets a browser fetch any one tile with a byte-range request. Off S3 that is a
static file read; the app's task and the database see none of it.

**The SQL is the map's own, moved.** Every spec below is the tile query
hbu_rag_map used to run per request, with two changes and no others:

* the partition screen is fixed rather than optional - an archive *is* one
  partition, so ``(neighborhood, scrape_date)`` is always bound;
* the screens the sidebar toggles - the lot area range, the under-built
  filter, one site thesis, the shortlist, the use side - are gone from the
  ``WHERE`` and their inputs travel as **properties** instead. A static tile
  cannot be re-queried when a box is ticked, so the browser filters what it
  holds: ``area_m2`` is on every lot, ``is_underbuilt`` on every massing and
  every bay, ``site_thesis`` / ``is_top_site_opportunity`` /
  ``is_good_candidate`` on every opportunity, and both use classes on every
  land-use piece.

---------------------------------------------------------------------------
Two kinds of tile in one archive
---------------------------------------------------------------------------

Below a layer's `DETAIL_ZOOM` the archive holds the dissolved cells of
`gold.map_cell_aggregates` rather than the features - the same MVT layer
name, so the browser keeps one layer across the threshold - and below
`AGGREGATE_OUTLINE_ZOOM` those cells are outlines: shape and shading only,
simplified before projection. `serves_aggregate` and `serves_outline` are the
two decisions, and they mirror the names hbu_rag_map keeps for its legend and
its notes. The cell level a display zoom is filled from is
``zoom + tile_grid.ZOOM_OFFSET``, and that offset is what bounds an aggregate
tile at 256 features by construction.

Every zoom is stored explicitly, because the aggregate tiles have to be: a
tile at zoom 12 holds level-16 cells and a tile at zoom 13 holds level-17
ones, which is not a picture the browser could make by scaling the other.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass

from hbu_dataplatform import tile_grid
from hbu_dataplatform.rag.documents import DOCUMENT_SOURCES, ZONING_SOURCES

#: The floor of hbu_rag_map's map - `basemap.MAP_MIN_ZOOM` over there - and so
#: the coarsest zoom an archive holds. Mirrored rather than imported because
#: the two repositories share no code; a map that lowers its floor below this
#: sees no tiles there until this is lowered too, which is visible rather
#: than wrong.
MIN_DISPLAY_ZOOM = 6

#: The finest zoom stored. Leaflet's own ``maxZoom`` in the map is 19, and a
#: tile at 19 is 76 m across at Montreal's latitude - one or two parcels, a
#: few kilobytes. Storing it costs a few thousand tiles per borough and spares
#: the browser scaling a coarser one, where every stroke would double.
MAX_TILE_ZOOM = 19

#: The map layers, in hbu_rag_map's draw order. One archive per name.
LAYERS: tuple[str, ...] = (
    "zones",
    "land_use",
    "capacity",
    "opportunities",
    "streets",
    "lots",
    "buildings",
    "surface_parking",
    "massing",
)

#: The zoom each layer starts holding its own features at. Below it a layer
#: with an aggregate holds dissolved cells; a layer without one holds nothing,
#: and the archive simply starts here.
#:
#: These are facts about the data's density - "a lot is sub-pixel at 14" is a
#: statement about the cadastre - which is why they live beside the specs
#: rather than in the map, which mirrors them for its legend.
DETAIL_ZOOM: dict[str, int] = {
    # Zoning is block-sized and draws as itself at every zoom the map reaches.
    "zones": 0,
    "capacity": 15,
    # A class per lot over the whole cadastre, so it takes the lots' gate.
    "land_use": 15,
    "streets": 14,
    "lots": 15,
    "buildings": 16,
    "massing": 16,
    # A bay is smaller than the building beside it.
    "surface_parking": 16,
    # A few hundred lots in a borough rather than twenty-five thousand, so it
    # can draw itself from further out - and has to, because "where are the
    # opportunities" is a question asked of a borough.
    "opportunities": 12,
}

#: The layers that have dissolved cells to fall back to below their detail
#: zoom - exactly the five `map_cell_aggregates` builds.
AGGREGATE_LAYERS: tuple[str, ...] = tile_grid.LAYERS

#: The display zoom below which an aggregate tile is an **outline** rather than
#: a summary: the same dissolved cells, shaded by the same `value`, thinned,
#: and carrying nothing a tooltip could read. From 12 up a cell is a finding
#: worth hovering; from 11 down it is a dozen pixels of a borough that is
#: itself a few dozen across, and its `attributes` blob is weight carried to no
#: end. Mirrored by hbu_rag_map's ``AGGREGATE_OUTLINE_ZOOM`` for its notes.
AGGREGATE_OUTLINE_ZOOM = 12

#: Coordinate steps across a tile. The Mapbox default, and what every renderer
#: assumes when a tile does not say otherwise.
EXTENT = 4096

#: How far past the tile edge geometry is kept, in extent units, so a lot
#: straddling two tiles is not cut exactly at the seam and drawn along it in
#: both. A quarter of a 256-pixel tile's worth of slack.
BUFFER = 64

#: The same slack as the fraction of the envelope `ST_TileEnvelope` wants, so
#: the rows *selected* cover the same ground as the rows kept.
MARGIN = BUFFER / EXTENT

#: How much shape an outline cell may lose, in extent steps: half a screen
#: pixel, under what `ST_AsMVTGeom` is about to quantise away anyway.
OUTLINE_SIMPLIFY_STEPS = 8

#: How many tiles one statement renders. A band of rows across the partition's
#: width, cut so no statement asks for more than this - at zoom 19 a borough
#: is a few thousand tiles and one statement per band keeps the result set,
#: and the statement time, bounded.
TILES_PER_STATEMENT = 512

#: The file a layer's archive is written to, beside the manifest.
ARCHIVE_SUFFIX = ".pmtiles"

#: One JSON file per partition saying which archives were written and what
#: each holds. hbu_rag_map reads it before it draws, so a layer whose asset
#: has not run is a note rather than a silent blank.
MANIFEST_FILE = "map_tiles.json"

#: The attribute the zoning layer's own number lives in, and the one carrying
#: the link to its grid. Mirrors `ZONING_FIELDS[0]` and
#: `ZONING_URL_ATTRIBUTE` in hbu_rag_map; the link attribute is checked
#: against `rag.documents.DOCUMENT_SOURCES` by the unit tests so the two
#: registries cannot drift apart unnoticed.
ZONE_LABEL_ATTRIBUTE = "NUMERO_COMPLET"
ZONING_URL_ATTRIBUTE = "LIEN_GRILLE"


def archive_file(layer: str) -> str:
    """``lots`` -> ``lots.pmtiles``."""
    return f"{layer}{ARCHIVE_SUFFIX}"


@dataclass(frozen=True)
class TileLayerSpec:
    """One layer's tile query: what is selected, from where, and what is drawn.

    ``columns`` is the body of the tile CTE and every column becomes a tile
    property the browser reads, which is why the lists are short: a tile
    carries what the style function, the tooltip and the client-side filters
    need, once per feature per tile. ``geom`` is what gets projected, clipped
    and quantised; ``index_geom`` is what the bounding-box test picks
    candidates with, and is the same stored column on every spec here.

    ``feature_id`` names the property the map identifies a feature by - it is
    recorded in the archive's metadata rather than used here.
    """

    name: str
    source: str
    columns: str
    where: str
    geom: str
    feature_id: str
    index_geom: str | None = None

    @property
    def detail_zoom(self) -> int:
        return DETAIL_ZOOM[self.name]

    @property
    def has_aggregate(self) -> bool:
        return self.name in AGGREGATE_LAYERS

    @property
    def min_zoom(self) -> int:
        """The coarsest zoom the archive holds.

        The map's floor for a layer with cells behind it; the detail zoom for
        one without, because below that there is nothing true to draw.
        """
        if self.has_aggregate:
            return MIN_DISPLAY_ZOOM
        return max(MIN_DISPLAY_ZOOM, self.detail_zoom)

    @property
    def max_zoom(self) -> int:
        return MAX_TILE_ZOOM

    @property
    def bbox_geom(self) -> str:
        return self.index_geom or self.geom


def serves_aggregate(layer: str, zoom: int) -> bool:
    """Whether a tile of ``layer`` at ``zoom`` holds dissolved cells."""
    return layer in AGGREGATE_LAYERS and zoom < DETAIL_ZOOM[layer]


def serves_outline(zoom: int) -> bool:
    """Whether an aggregate tile at ``zoom`` is an outline rather than a summary."""
    return zoom < AGGREGATE_OUTLINE_ZOOM


def aggregate_cell_zoom(zoom: int) -> int:
    """The cell level an aggregate tile at display ``zoom`` is filled from.

    ``zoom + tile_grid.ZOOM_OFFSET``, clamped to the levels the pyramid holds.
    Below the range the coarsest level is the honest answer - still a true
    summary, of more ground than a cell should cover.
    """
    return max(
        tile_grid.CELL_ZOOMS[0],
        min(tile_grid.CELL_ZOOMS[-1], tile_grid.cell_zoom_for(zoom)),
    )


#: The relations each layer's tile query reads, so a layer whose source has
#: not been created on this database is skipped with a note rather than
#: aborting the transaction the other eight are rendered in. The aggregate
#: table is listed on the five layers that fall back to it.
LAYER_RELATIONS: dict[str, tuple[str, ...]] = {
    "zones": ("rag.features",),
    "land_use": (
        "silver.lot_zone_pieces",
        "gold.lot_redevelopment_gap",
        "gold.lot_highest_best_use",
    ),
    "capacity": (
        "silver.lot_zone_pieces",
        "gold.lot_redevelopment_gap",
        "gold.map_cell_aggregates",
    ),
    "opportunities": ("silver.lot_zone_pieces", "gold.lot_investment_opportunities"),
    "streets": ("silver.neighborhood_streets", "gold.map_cell_aggregates"),
    "lots": ("rag.lots", "gold.map_cell_aggregates"),
    "buildings": ("silver.building_lot_intersections", "gold.map_cell_aggregates"),
    "surface_parking": ("gold.lot_surface_parking", "gold.lot_redevelopment_gap"),
    "massing": (
        "gold.lot_building_massing",
        "gold.lot_redevelopment_gap",
        "gold.map_cell_aggregates",
    ),
}


def zoom_range(layer: str) -> range:
    """Every zoom ``layer``'s archive holds, coarsest first."""
    spec = layer_spec(layer)
    return range(spec.min_zoom, spec.max_zoom + 1)


def outline_tolerance_deg(zoom: int) -> float:
    """The Douglas-Peucker tolerance for an outline tile at ``zoom``, in degrees.

    Applied *before* the transform to 3857, which is the whole saving: a
    borough-wide dissolved union is thinned while its vertices are still
    cheap. A tile at ``zoom`` spans ``360 / 2**zoom`` degrees of longitude, so
    the tolerance is `OUTLINE_SIMPLIFY_STEPS` of that tile's own extent grid.
    """
    return 360.0 / (1 << zoom) / EXTENT * OUTLINE_SIMPLIFY_STEPS


def tile_range(
    bounds: tuple[float, float, float, float], zoom: int
) -> tuple[int, int, int, int]:
    """``(x0, x1, y0, y1)``, inclusive, of the tiles at ``zoom`` covering ``bounds``.

    ``bounds`` is ``(west, south, east, north)`` in EPSG:4326 - the order
    ``ST_Extent`` reports. Rows grow southward on the Web Mercator grid, so
    the first row is the one holding the *northern* edge.
    """
    west, south, east, north = bounds
    x0, y0 = tile_grid.cell_of(west, north, zoom)
    x1, y1 = tile_grid.cell_of(east, south, zoom)
    return x0, x1, y0, y1


def bands(
    x0: int, x1: int, y0: int, y1: int, *, per_statement: int = TILES_PER_STATEMENT
) -> Iterator[tuple[int, int]]:
    """Cut a tile range into row bands of at most ``per_statement`` tiles.

    Yields ``(y_start, y_end)`` inclusive. A band always spans the full width,
    so a statement's result is a rectangle of tiles rather than a ragged one.
    """
    width = x1 - x0 + 1
    rows = max(1, per_statement // max(width, 1))
    y = y0
    while y <= y1:
        yield y, min(y + rows - 1, y1)
        y += rows


def tile_count(bounds: tuple[float, float, float, float], layer: str) -> int:
    """How many tiles ``layer``'s archive would address over ``bounds``.

    An upper bound - the tiles over water and outside the borough come back
    empty and are not stored - reported by the asset beside the count that
    actually was.
    """
    total = 0
    for zoom in zoom_range(layer):
        x0, x1, y0, y1 = tile_range(bounds, zoom)
        total += (x1 - x0 + 1) * (y1 - y0 + 1)
    return total


# ---------------------------------------------------------------------------
# The specs
#
# Ported from hbu_rag_map's `queries._register_mvt_layers`, which they replace.
# Schemas are written out the way `hbu_dataplatform.postgis` writes them: `rag` for
# the working set, `silver` and `gold` for this platform's own tables.
# ---------------------------------------------------------------------------

#: The partition screen every spec takes, in the parameter names every query
#: in `hbu_dataplatform.postgis` already binds.
_PARTITION = (
    "{alias}.neighborhood = %(neighborhood)s "
    "AND {alias}.scrape_date = %(scrape_date)s::date"
)

#: How much of what the zoning would permit is standing today, as a
#: percentage. NULL on a lot with no solved programme, and NULL again where
#: the roll has a unit on the lot but states no floor area for it - the
#: numerator is unknown, not zero, and 0% would put a standing office at the
#: top of every under-built list. `existing_num_assessment_units` is what
#: separates the two, and it is the count rather than `has_assessment`
#: because that flag is true on every row.
_USED_PCT = (
    "CASE WHEN g.existing_num_assessment_units > 0"
    "      AND g.existing_floor_area_m2 IS NULL THEN NULL"
    " ELSE 100.0 * COALESCE(g.existing_floor_area_m2, 0)"
    " / NULLIF(g.hbu_floor_area_m2, 0) END"
)


def _headroom_m2(cls: str) -> str:
    return (
        f"GREATEST(g.hbu_{cls}_floor_area_m2"
        f" - COALESCE(g.existing_{cls}_floor_area_m2, 0), 0)"
    )


#: The three answer layers are drawn on the ground each answer is *about*:
#: one feature per (lot, zone) piece of `silver.lot_zone_pieces`, which
#: carries both the surrogate and its own polygon, so the answer is joined on
#: the cadastral number and the zone - the two that survive a reload of
#: `rag.lots`, where `lot_uid` does not.
_PIECE_SOURCE = "silver.lot_zone_pieces l"

#: What every piece-drawn feature says about the ground it covers and the
#: parcel it belongs to. `num_lot_zones` is the one a renderer must read: on
#: the few features where it is not 1 the feature is a *part* of the parcel
#: its `lot_number` names.
_PIECE_COLUMNS = """
               l.lot_uid,
               l.lot_number,
               l.feature_id,
               l.piece_area_m2,
               l.lot_area_m2,
               l.num_lot_zones,
               l.is_primary_zone,
               l.primary_street_name"""

#: The gap row joined to the piece it is about.
_GAP_JOIN = """
          JOIN gold.lot_redevelopment_gap g
            ON g.lot_number   = l.lot_number
           AND g.feature_id   = l.feature_id
           AND g.neighborhood = l.neighborhood
           AND g.scrape_date  = l.scrape_date"""

#: Both sides of the use question travel; the browser picks which one to
#: colour by. The map used to choose server-side from the URL's `use_side`
#: and emit one `use_class`; a static tile carries both and the style
#: function derives the class from the side it is told at render time.
_LAND_USE_COLUMNS = """
               g.hbu_status,
               g.existing_num_assessment_units,
               g.existing_dominant_income_class AS existing_use,
               g.existing_dominant_use_description,
               h.hbu_dominant_use               AS hbu_use,
               g.existing_floor_area_m2,
               g.hbu_floor_area_m2,
               l.existing_footprint_m2,
               h.footprint_m2                   AS hbu_footprint_m2,
               g.existing_num_dwellings,
               g.hbu_num_dwellings"""

#: Enough for a tooltip, a colour and the three client-side screens; the pane
#: reads the whole row again by lot.
_OPPORTUNITY_COLUMNS = """
               o.site_thesis,
               o.investment_thesis,
               o.site_thesis_rank,
               o.is_top_site_opportunity,
               o.site_yield_on_cost_pct,
               o.redevelopment_npv_gain_cad,
               o.existing_year_built,
               o.existing_num_storeys,
               o.hbu_floors,
               o.storey_headroom,
               o.existing_dominant_use_description,
               o.is_heritage_sector,
               o.has_piia_review,
               o.demolition_review_required,
               o.improvement_added_storeys,
               o.improvement_floor_m2,
               o.owner_best_future,
               o.buyer_best_future,
               o.site_irr_pct,
               o.site_all_in_yield_on_cost_pct,
               o.site_yoc_spread_bps,
               o.market_cap_rate_pct,
               o.is_good_candidate,
               o.clears_cap_rate,
               o.clears_hurdle"""


def _underbuilt_exists(alias: str) -> str:
    """Whether the lot a massing or a bay stands on is under-built.

    The map's under-built screen applied to these two layers through an
    ``EXISTS`` over the gap table; the same ``EXISTS`` now travels as a
    property so the browser can apply it. Joined on `lot_uid`, because the two
    gold tables are materialised together and share a generation.
    """
    return f"""EXISTS (
                   SELECT 1
                     FROM gold.lot_redevelopment_gap g
                    WHERE g.scrape_date  = {alias}.scrape_date
                      AND g.neighborhood = {alias}.neighborhood
                      AND g.lot_uid      = {alias}.lot_uid
                      AND g.is_underbuilt
               ) AS is_underbuilt"""


def _specs() -> dict[str, TileLayerSpec]:
    partition = _PARTITION
    specs = (
        # Every scraped layer that *is* zoning, one per city - the same
        # registry the corpus is built from, read from the other end.
        TileLayerSpec(
            name="zones",
            source="rag.features f",
            columns=f"""
               f.feature_id,
               COALESCE(NULLIF(f.attributes ->> '{ZONE_LABEL_ATTRIBUTE}', ''),
                        f.feature_id) AS zone_label,
               f.attributes ->> '{ZONING_URL_ATTRIBUTE}' AS zoning_pdf_url""",
            where=f"""
           f.source_table = ANY(%(source_tables)s)
           AND {partition.format(alias="f")}""",
            geom="f.geom",
            feature_id="feature_id",
        ),
        # What each lot is used for, on both sides of the proposal. The gap
        # and HBU tables are joined to each other on `lot_uid` on purpose -
        # they are materialised together - where the join to the piece is on
        # the cadastral number and the zone.
        TileLayerSpec(
            name="land_use",
            source=f"""{_PIECE_SOURCE}{_GAP_JOIN}
          LEFT JOIN gold.lot_highest_best_use h
            ON h.lot_uid      = g.lot_uid
           AND h.feature_id   = g.feature_id
           AND h.neighborhood = g.neighborhood
           AND h.scrape_date  = g.scrape_date""",
            columns=f"""{_PIECE_COLUMNS},{_LAND_USE_COLUMNS}""",
            where=partition.format(alias="l"),
            geom="l.geom",
            feature_id="lot_uid",
        ),
        # The piece's shape carrying the gap table's finding. `is_underbuilt`
        # is what the browser's under-built screen reads.
        TileLayerSpec(
            name="capacity",
            source=f"{_PIECE_SOURCE}{_GAP_JOIN}",
            columns=f"""{_PIECE_COLUMNS},
               g.hbu_status,
               g.existing_num_assessment_units,
               g.is_underbuilt,
               g.existing_floor_area_m2,
               g.hbu_floor_area_m2,
               g.existing_num_dwellings,
               g.hbu_num_dwellings,
               g.dwelling_gap,
               {_USED_PCT} AS used_pct,
               {_headroom_m2("residential")} AS residential_headroom_m2,
               {_headroom_m2("commercial")}  AS commercial_headroom_m2,
               {_headroom_m2("industrial")}  AS industrial_headroom_m2""",
            where=partition.format(alias="l"),
            geom="l.geom",
            feature_id="lot_uid",
        ),
        # The pieces that carry a site thesis. The only screen kept in the
        # SQL is the one that defines the layer - a thesis at all; narrowing
        # to one thesis or to the shortlist is the browser's.
        TileLayerSpec(
            name="opportunities",
            source=f"""{_PIECE_SOURCE}
          JOIN gold.lot_investment_opportunities o
            ON o.lot_number   = l.lot_number
           AND o.feature_id   = l.feature_id
           AND o.neighborhood = l.neighborhood
           AND o.scrape_date  = l.scrape_date""",
            columns=f"""{_PIECE_COLUMNS},{_OPPORTUNITY_COLUMNS}""",
            where=f"""
           {partition.format(alias="l")}
           AND o.site_thesis IS NOT NULL AND o.site_thesis <> 'none'""",
            geom="l.geom",
            feature_id="lot_uid",
        ),
        # The one line layer. `length_m` is its only measure and what the
        # tooltip says.
        TileLayerSpec(
            name="streets",
            source="silver.neighborhood_streets s",
            columns="""
               s.cote_rue_id,
               s.street_name,
               s.length_m""",
            where=partition.format(alias="s"),
            geom="s.geom",
            feature_id="cote_rue_id",
        ),
        # `area_m2` on every lot, so the browser can apply the area range.
        TileLayerSpec(
            name="lots",
            source="rag.lots l",
            columns="""
               l.lot_uid,
               l.lot_number,
               COALESCE(l.area_m2, ST_Area(l.geom::geography)) AS area_m2""",
            where=partition.format(alias="l"),
            geom="l.geom",
            feature_id="lot_uid",
        ),
        # The footprints clipped to the cadastre, not the footprints: one
        # feature per (building, lot), keyed on the pair, so a terrace BDOI
        # drew as one outline is one feature per parcel it stands on. This
        # platform is what builds the clip, so unlike the map there is no
        # fallback that computes it per tile.
        TileLayerSpec(
            name="buildings",
            source="silver.building_lot_intersections bl",
            columns="""
               bl.building_uid::text || ':' || bl.lot_uid::text
                   AS building_lot_key,
               bl.intersection_area_m2 AS area_m2""",
            where=partition.format(alias="bl"),
            geom="bl.geom",
            feature_id="building_lot_key",
        ),
        TileLayerSpec(
            name="surface_parking",
            source="gold.lot_surface_parking p",
            columns=f"""
               p.lot_uid,
               p.lot_number,
               p.parking_status,
               p.surface_stalls,
               p.placed_surface_stalls,
               p.placed_surface_parking_m2,
               p.num_parking_bays,
               p.surface_parking_fit_pct,
               {_underbuilt_exists("p")}""",
            where=partition.format(alias="p"),
            geom="p.geom",
            feature_id="lot_uid",
        ),
        TileLayerSpec(
            name="massing",
            source="gold.lot_building_massing m",
            columns=f"""
               m.lot_uid,
               m.lot_number,
               m.massing_status,
               m.floors,
               m.num_dwellings,
               m.commercial_floors,
               m.placed_footprint_m2,
               m.footprint_fit_pct,
               {_underbuilt_exists("m")}""",
            where=partition.format(alias="m"),
            geom="m.geom",
            feature_id="lot_uid",
        ),
    )
    return {spec.name: spec for spec in specs}


def layer_spec(layer: str) -> TileLayerSpec:
    """The spec for ``layer``; raises rather than returning None."""
    specs = _specs()
    try:
        return specs[layer]
    except KeyError:
        raise KeyError(
            f"{layer!r} has no tile spec in hbu_dataplatform.map_tiles; "
            f"known: {', '.join(sorted(specs))}"
        ) from None


def layer_params(neighborhood: str, scrape_date: str) -> dict[str, object]:
    """The parameters every spec's SQL binds, for one partition."""
    return {
        "neighborhood": neighborhood,
        "scrape_date": scrape_date,
        "source_tables": list(ZONING_SOURCES),
        "extent": EXTENT,
        "buffer": BUFFER,
        "margin": MARGIN,
    }


#: What an aggregate tile carries, as the columns of the tile CTE.
#: `agg_level` is the flag every style and tooltip branches on: the cell zoom,
#: present on a cell and on nothing else. `attributes` travels as text because
#: an MVT property is a scalar; the browser parses it per hovered cell.
AGGREGATE_COLUMNS = """
               a.layer,
               a.cell_z AS agg_level,
               a.feature_count,
               a.value,
               a.value_kind,
               a.coverage_pct,
               a.attributes::text AS attributes"""

#: What an *outline* tile carries instead: the flag, and the shading and its
#: vocabulary. Everything only the tooltip read is gone, and the browser reads
#: the missing count as "nothing to say" and opens no tooltip.
OUTLINE_COLUMNS = """
               a.layer,
               a.cell_z AS agg_level,
               a.value,
               a.value_kind"""


def link_attribute_agrees() -> bool:
    """Whether every zoning source's link attribute is the one the tile reads.

    `ZONING_URL_ATTRIBUTE` is one name for what `DOCUMENT_SOURCES` records
    per source. The two agree today; this is what a test asserts so a city
    whose polygons link their grid under another attribute is caught here
    rather than as a zone with no PDF on the map.
    """
    return all(attribute == ZONING_URL_ATTRIBUTE for attribute in DOCUMENT_SOURCES.values())


def hilbert_sorted(tiles: list[tuple[int, int, bytes]], zoom: int) -> list[tuple[int, int, bytes]]:
    """``tiles`` in the order a PMTiles directory wants them.

    Not needed for correctness - the writer sorts its directory - but a
    clustered archive is one whose tile data is laid out in directory order,
    which is what lets a browser's range requests for neighbouring tiles land
    on neighbouring bytes.
    """
    from pmtiles.tile import zxy_to_tileid  # noqa: PLC0415

    return sorted(tiles, key=lambda tile: zxy_to_tileid(zoom, tile[0], tile[1]))


def e7(value: float) -> int:
    """A coordinate as the ``int32`` ten-millionths the PMTiles header stores."""
    return int(math.floor(value * 10_000_000)) if value < 0 else int(math.ceil(value * 10_000_000))
