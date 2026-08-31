"""Loading the latest lots, buildings and map features into Postgres/PostGIS,
the spatial joins between them, and the per-lot profile they collapse into.

Every table named here is owned by hbu_infra (see its README and sql/) - this
module only ever writes into tables it assumes already exist, the same
ownership split `rag.chunks`/`rag_assets.py` draws the other way: hbu_infra
creates the tables, this repo fills them.

**Two schemas, and the line between them is the medallion layer.** `rag` holds
the working set the joins below are computed *over*: `rag.lots`,
`rag.buildings` and `rag.features`, loaded from the bronze snapshots by
`load_lots`/`load_buildings`/`load_features`. `silver` and `gold` hold what the
joins *produce*, one table per asset, and every write to them goes through
`urban_rag.warehouse` - see that module for the partitioning and the upsert.

`silver.building_lot_intersections` is the first derived join: for each
(building, lot) pair whose footprints intersect, one row holding the *clipped*
intersection geometry and its share of the building's area - so a warehouse
straddling three lots is assigned to each of them in proportion to the
footprint actually inside it, rather than to whichever lot its centroid happens
to land on. Computed with `ST_Intersection` in Postgres rather than in
GeoPandas: by the time this runs, PostGIS already holds both layers loaded and
GiST-indexed, and the join is exactly the kind of thing that index is for.

`silver.lot_features` is the other derived join, and the one the corpus hangs
off. `rag.chunks.feature_ids` records which map features cite each indexed
PDF, so a lot's documents are whatever the features covering it cite - but
there is no id that gets from a lot to a feature. The lots come from Infolot,
Quebec's cadastre, keyed by `NO_LOT`; the features come from Montreal's
Spectrum service, keyed by `NUMERO_COMPLET`, and neither publisher carries the
other's key. Geometry is the only thing the two share, so the join is spatial
by necessity rather than by choice - see `compute_lot_features`, and
hbu_infra's sql/005_silver_lot_features.sql for the table.

`silver.lot_frontage` is the third derived join, and the one that answers "how
much of this lot faces a street". Its right-hand side is
`silver.neighborhood_streets`, the city's *geobase double* - one line per side
of street, drawn along the curb - and the measure is taken on the lot's
`ST_Boundary`, not on the lot itself: a lot is a polygon, and `ST_Length` of a
polygon is zero, so intersecting the two solids and measuring the result would
report nothing at all. See `compute_lot_frontage` for how a boundary is matched
to a street side - and for why that is no longer a clip against a buffered one,
which could not be both wide enough to reach a borough's lot lines and narrow
enough to leave what it reached undistorted. hbu_infra's
sql/007_silver_streets.sql and sql/008_silver_lot_frontage.sql have the two
tables.

`gold.lot_profiles` is where the three joins above come back together, at the
grain they are all about: one row per lot, carrying how many buildings stand on
it, the two street edges it fronts on, and the document that governs it -
alongside four jsonb columns its caller hands in from the geoparquet tree at a
grain no table here holds. It replaces an earlier `rag.vacant_lots`, which read
the building join the other way round and kept only the parcels it found
nothing on - a table that could answer "where is the empty land" and nothing
else, because the lots its WHERE clause dropped were the ones a reader could no
longer see. Keeping every lot and carrying `has_building` costs one boolean and
makes that question a filter; see `compute_lot_profiles`.

None of this is a live view, and a partition is still a snapshot - but it is
now refreshed by an upsert followed by a prune rather than by a delete followed
by an insert. The difference is what a reader querying mid-load sees: with the
old order, a borough's rows were simply gone for the length of the recompute.
`urban_rag.warehouse` documents the trade in full; the part that matters here
is that a key which does not survive a re-scrape - BDOI publishes no building
id, unlike Infolot's lot number - degrades to exactly the delete-and-insert it
always was, because every old row is pruned and every new one inserted.
"Latest" still falls out for free: whatever is in the table for a neighborhood
*is* its latest scrape, and the Dagster asset that calls this only ever writes
the partition it was just handed.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date
from typing import TYPE_CHECKING, Any, Iterator, Sequence

import geopandas as gpd
import pandas as pd

from urban_rag import warehouse
from urban_rag.rag.pgvector import PgSettings, PostgresUnavailable
from urban_rag.warehouse import MissingRelation  # noqa: F401  (re-exported)

if TYPE_CHECKING:  # pragma: no cover - typing only, psycopg is imported lazily
    from psycopg import Connection

#: Columns never written to `attributes`: they get their own column, or (for
#: `scraped_at`) mean nothing once the row has its own Postgres `loaded_at`.
_ALWAYS_EXCLUDED = {"neighborhood", "scrape_date", "scraped_at"}


def _psycopg():
    try:
        import psycopg

        return psycopg
    except ImportError as exc:  # pragma: no cover - environment problem
        raise PostgresUnavailable(
            "psycopg is not installed - `pip install psycopg[binary]`"
        ) from exc


@contextmanager
def connect(settings: PgSettings) -> Iterator["Connection"]:
    """One connection, committed on a clean exit and rolled back otherwise.

    Same posture as `PgVectorStore.connect`: a partition's lots, buildings and
    the intersections between them are one transaction, so a reader never
    sees the join computed against only half of a reload.
    """
    psycopg = _psycopg()
    try:
        connection = psycopg.connect(**settings.connection_kwargs())
    except psycopg.OperationalError as exc:
        raise PostgresUnavailable(
            f"Could not connect to {settings.safe_target}: {exc}"
        ) from exc
    with connection:
        yield connection


#: The `rag` working set, and the hbu_infra file that creates all three.
#:
#: The only relations this module touches that nothing else already checks:
#: every silver and gold write goes through `urban_rag.warehouse`, which calls
#: `require_table` on its own target before it writes, while these three are
#: loaded by the raw DELETE/COPY/INSERT in `_replace_partition` and
#: `load_features` below. One source file for all three, because they are one
#: file's worth of DDL - a database missing `rag.lots` is missing the other two,
#: and naming them one failed run at a time is two runs too many.
WORKING_SET_RELATIONS: tuple[tuple[str, str], ...] = (
    ("rag.lots", "sql/002_spatial.sql"),
    ("rag.buildings", "sql/002_spatial.sql"),
    ("rag.features", "sql/002_spatial.sql"),
)


def require_working_set(connection: "Connection") -> None:
    """Raise `MissingRelation` unless the `rag` working set is there.

    Called once, before the first load, rather than from inside each loader:
    the three tables come from one file, so reporting them together is
    reporting the one thing that is actually wrong. Without it the first
    `DELETE FROM rag.lots` fails as a bare `psycopg.errors.UndefinedTable` -
    the identifier that failed to resolve, and nothing about which repo owns it
    or what to apply - and it fails only after the partition's parquet has been
    read, repaired and checked, so the run pays for all of that first.

    The same check `compute_lot_profiles` makes with `_LOT_PROFILE_RELATIONS`,
    moved ahead of the loads that have to land before any of those relations
    can be read.
    """
    _require_relations(connection.cursor(), WORKING_SET_RELATIONS)


def load_lots(
    connection: "Connection",
    frame: gpd.GeoDataFrame,
    *,
    neighborhood: str,
    scrape_date: str,
    lot_number_column: str = "NO_LOT",
) -> int:
    """Replace this borough's rows in `rag.lots` with ``frame``.

    Infolot's lot number is a real natural key, so a duplicate within one load
    (two boundary queries returning the same lot) is resolved with
    `ON CONFLICT ... DO NOTHING` rather than left to crash the whole load.
    """
    return _replace_partition(
        connection,
        "rag.lots",
        frame,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
        natural_key_column=lot_number_column,
        natural_key_target="lot_number",
    )


def load_buildings(
    connection: "Connection",
    frame: gpd.GeoDataFrame,
    *,
    neighborhood: str,
    scrape_date: str,
) -> int:
    """Replace this borough's rows in `rag.buildings` with ``frame``.

    Unlike `load_lots`, there is no natural key to upsert on - see the module
    docstring - so this is a plain delete-then-insert.
    """
    return _replace_partition(
        connection,
        "rag.buildings",
        frame,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
        natural_key_column=None,
        natural_key_target=None,
    )


def load_streets(
    connection: "Connection",
    frame: gpd.GeoDataFrame,
    *,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, int]:
    """Publish this borough's street sides to `silver.neighborhood_streets`.

    ``frame`` is one borough's slice of the geobase double, so the rows are
    street *sides* rather than centre lines: `COTE_RUE_ID` is unique across the
    island (91,546 of 91,546 in the first snapshot), which makes it as real a
    natural key as Infolot's lot number - and it is what the upsert conflicts
    on, inside the (scrape_date, neighborhood) partition.

    The one loader here that is a plain `urban_rag.warehouse` call and nothing
    else, because it is the one whose rows arrive as a frame rather than out of
    a join computed in the database. `NOM_VOIE` becomes `street_name` and
    `COTE_RUE_ID` becomes `cote_rue_id` through the table's own column map;
    `length_m` is measured in SQL below rather than in the frame, since the
    frame's `length_in_borough_m` is computed in a projected CRS by the asset
    and this column is the geography measure every other table here uses.
    Everything else the layer publishes lands in `attributes`.

    The geometry is forced to `MultiLineString` on the way, because the column
    is typed one and a typmod rejects a bare `LineString`. `_replace_partition`
    used to do this with `ST_Multi` in its INSERT; `urban_rag.warehouse` has no
    such step, on purpose - it writes what it is given and a cast per table
    would be a special case in the one place that has none. The knowledge that
    this column is Multi lives here, where the table is known.
    """
    frame = frame.assign(**{"length_m": _length_m(frame)}).set_geometry(
        frame.geometry.map(_as_multi_line)
    )
    return warehouse.upsert_frame(
        connection,
        "neighborhood_streets",
        frame,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )


def _as_multi_line(geometry: Any):
    """One street side as the `MultiLineString` its column is typed as.

    A side clipped at a borough line comes back as either shape - one piece or
    two - and `geometry(MultiLineString, 4326)` rejects the single one by
    typmod rather than promoting it.
    """
    from shapely.geometry import MultiLineString

    if geometry is None or geometry.is_empty:
        return geometry
    return (
        geometry
        if geometry.geom_type == "MultiLineString"
        else MultiLineString([geometry])
    )


def _length_m(frame: gpd.GeoDataFrame) -> pd.Series:
    """Each row's length in metres, measured the way PostGIS would.

    `GeoSeries.length` on a 4326 frame is in *degrees*, which is not a length.
    The geodesic measure is what `ST_Length(geography(...))` returns and what
    every measure in this module is stated in, so the two agree whichever side
    computed them - and pyproj's `Geod` is what geopandas itself reprojects
    with, so it is already installed wherever this runs.
    """
    from pyproj import Geod

    geod = Geod(ellps="WGS84")
    return frame.geometry.map(
        lambda geometry: None
        if geometry is None or geometry.is_empty
        else abs(geod.geometry_length(geometry))
    )


def load_features(
    connection: "Connection",
    frame: gpd.GeoDataFrame,
    *,
    neighborhood: str,
    scrape_date: str,
    source_table: str,
    feature_id_column: str,
) -> int:
    """Replace one source table's rows in `rag.features` for this partition.

    The partition deleted here is (neighborhood, scrape_date, `source_table`),
    not (neighborhood, scrape_date) as in `load_lots`/`load_buildings`: a
    borough's snapshot is two dozen separate layers written as two dozen
    parquet files and loaded one at a time, so deleting the whole borough would
    drop every layer already landed by the same run.

    ``source_table`` must be the file *slug* - `Reglement_urbanisme__VSP_REG_ZONE`,
    what `frames.table_slug` makes of the Spectrum path - and not the path
    `/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE` the parquet carries in a
    column of the same name. The slug is what `rag_assets.linked_documents`
    writes into `rag.chunks.source_table`, and every join from geometry to the
    corpus matches the two columns to each other; a row loaded under the path
    would be invisible to all of them. The frame's own `source_table` column is
    dropped rather than kept in `attributes` for the same reason - one row
    carrying two different meanings of the name is the confusion this docstring
    exists to prevent.

    ``feature_id_column`` is the column holding the id the corpus cites -
    `NUMERO_COMPLET` for the zoning layers. A layer with no such column has no
    way to be cited by a document and is not loaded; that choice is made by the
    caller, which knows the registry.
    """
    cursor = connection.cursor()
    cursor.execute(
        "DELETE FROM rag.features "
        "WHERE neighborhood = %s AND scrape_date = %s::date AND source_table = %s",
        [neighborhood, scrape_date, source_table],
    )
    if frame.empty:
        return 0
    if feature_id_column not in frame.columns:
        raise ValueError(
            f"{source_table}: no {feature_id_column!r} column to use as feature_id"
        )

    _psycopg()  # raises PostgresUnavailable with a clear message if missing
    from psycopg.types.json import Jsonb
    import shapely.wkb

    staging = "rag_features_load"
    # Dropped rather than left to ON COMMIT: the layers of one borough are
    # loaded in a single transaction, so the second call would otherwise meet
    # the first one's table still holding the first one's rows.
    cursor.execute(f"DROP TABLE IF EXISTS {staging}")
    cursor.execute(
        f"CREATE TEMP TABLE {staging} "
        "(feature_id text, source_table text, neighborhood text, scrape_date date, "
        "attributes jsonb, geom bytea) ON COMMIT DROP"
    )

    geometry_name = frame.geometry.name
    exclude = set(_ALWAYS_EXCLUDED) | {geometry_name, feature_id_column, "source_table"}
    attribute_columns = [c for c in frame.columns if c not in exclude]
    attrs = (
        json.loads(frame[attribute_columns].to_json(orient="records", date_format="iso"))
        if attribute_columns
        else [{}] * len(frame)
    )

    inserted = 0
    partition_date = _as_date(scrape_date)
    statement = (
        f"COPY {staging} (feature_id, source_table, neighborhood, scrape_date, "
        "attributes, geom) FROM STDIN (FORMAT BINARY)"
    )
    with cursor.copy(statement) as copy:
        copy.set_types(["text", "text", "text", "date", "jsonb", "bytea"])
        for index, row in enumerate(frame.itertuples(index=False)):
            geometry = getattr(row, geometry_name)
            if geometry is None or geometry.is_empty:
                continue
            feature_id = getattr(row, feature_id_column)
            if feature_id is None or str(feature_id).strip() == "":
                continue
            copy.write_row(
                [
                    str(feature_id),
                    source_table,
                    neighborhood,
                    partition_date,
                    Jsonb(attrs[index]),
                    shapely.wkb.dumps(geometry),
                ]
            )
            inserted += 1

    # No ST_Multi here, unlike `_replace_partition`: rag.features.geom is typed
    # `geometry(Geometry, 4326)` precisely because one layer's rows are
    # polygons and another's are points, so nothing needs normalising.
    cursor.execute(
        f"""
        INSERT INTO rag.features (
            feature_id, source_table, neighborhood, scrape_date, attributes, geom
        )
        SELECT feature_id, source_table, neighborhood, scrape_date, attributes,
               ST_SetSRID(ST_GeomFromWKB(geom), 4326)
        FROM {staging}
        ON CONFLICT (source_table, feature_id, neighborhood, scrape_date) DO NOTHING
        """
    )
    return inserted


def compute_intersections(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, object]:
    """(Re)compute `silver.building_lot_intersections` for one partition.

    Assumes `load_lots`/`load_buildings` already landed this partition's rows
    in `rag.lots`/`rag.buildings` - the join is `ON l.neighborhood =
    b.neighborhood AND l.scrape_date = b.scrape_date`, so a stale lot from a
    different date simply cannot match.

    Published through `urban_rag.warehouse.upsert_select` rather than inserted
    directly: the rows are produced in the database and never pass through
    Python, but the write still has to be the upsert-then-prune every other
    table here gets. `building_uid` is a bigserial `load_buildings` mints
    again on each load, so in practice a re-run of a partition prunes all of
    it and inserts all of it - which is what this table has always done, now
    said once in one place instead of here.
    """
    cursor = connection.cursor()
    result = warehouse.upsert_select(
        cursor,
        "building_lot_intersections",
        (
            "scrape_date",
            "neighborhood",
            "building_uid",
            "lot_uid",
            "lot_number",
            "building_area_m2",
            "intersection_area_m2",
            "pct_of_building",
            "geom",
        ),
        """
        SELECT
            b.scrape_date,
            b.neighborhood,
            b.building_uid,
            l.lot_uid,
            l.lot_number,
            ST_Area(geography(b.geom)),
            ST_Area(geography(clipped.geom)),
            CASE WHEN ST_Area(geography(b.geom)) > 0
                 THEN 100.0 * ST_Area(geography(clipped.geom)) / ST_Area(geography(b.geom))
                 ELSE 0.0
            END,
            clipped.geom
        FROM rag.buildings b
        JOIN rag.lots l
          ON l.neighborhood = b.neighborhood
         AND l.scrape_date = b.scrape_date
         AND ST_Intersects(b.geom, l.geom)
        -- Computed once via LATERAL rather than three times inline: a
        -- polygon clip is the expensive part of this query, ST_Area is not.
        CROSS JOIN LATERAL (SELECT ST_Intersection(b.geom, l.geom) AS geom) AS clipped
        WHERE b.neighborhood = %(neighborhood)s
          AND b.scrape_date = %(scrape_date)s::date
          -- A shared edge or corner intersects but clips to a line or point,
          -- which is not a "building on this lot" - only a 2D clip is.
          AND NOT ST_IsEmpty(clipped.geom)
          AND ST_Dimension(clipped.geom) = 2
        """,
        {"neighborhood": neighborhood, "scrape_date": scrape_date},
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    cursor.execute(
        """
        SELECT count(DISTINCT building_uid), COALESCE(sum(intersection_area_m2), 0)
        FROM silver.building_lot_intersections
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    buildings_matched, total_area_m2 = cursor.fetchone()
    return {
        "intersections": result["upserted"],
        "pruned": result["pruned"],
        "buildings_matched": int(buildings_matched),
        "total_area_m2": float(total_area_m2),
    }


def compute_lot_features(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, object]:
    """(Re)compute `silver.lot_features` for one (neighborhood, scrape_date).

    The join that gives a lot its documents, and the reason it has to be a
    spatial one: the lots come from Infolot, Quebec's cadastre, keyed by
    `NO_LOT`; the features come from Montreal's Spectrum service, keyed by
    `NUMERO_COMPLET`. Neither publisher carries the other's identifier, and the
    cadastre has no zoning column at all, so there is no id to link and the
    geometry is the only thing the two share.

    Assumes `load_lots` and `load_features` have already landed this
    partition's rows - the join is `ON l.neighborhood = f.neighborhood AND
    l.scrape_date = f.scrape_date`, so a lot from a different date cannot match
    a feature from this one.

    What counts as a match depends on what the feature is. An areal feature -
    a zone, a heritage sector - has to overlap the lot in *area*: a zone
    boundary running along a lot line intersects it, but clips to a line, and a
    line is not "this zone covers this lot". A point or line feature - a
    school, a street segment - only has to intersect, since it can never clip
    to an area and excluding it would mean excluding those layers entirely.
    """
    cursor = connection.cursor()
    result = warehouse.upsert_select(
        cursor,
        "lot_features",
        (
            "scrape_date",
            "neighborhood",
            "lot_uid",
            "source_table",
            "feature_id",
            "lot_number",
            "feature_uid",
            "lot_area_m2",
            "overlap_area_m2",
            "pct_of_lot",
            "geom",
        ),
        """
        SELECT
            l.scrape_date,
            l.neighborhood,
            l.lot_uid,
            f.source_table,
            f.feature_id,
            l.lot_number,
            f.feature_uid,
            ST_Area(geography(l.geom)),
            ST_Area(geography(clipped.geom)),
            CASE WHEN ST_Area(geography(l.geom)) > 0
                 THEN 100.0 * ST_Area(geography(clipped.geom)) / ST_Area(geography(l.geom))
                 ELSE 0.0
            END,
            clipped.geom
        FROM rag.lots l
        JOIN rag.features f
          ON f.neighborhood = l.neighborhood
         AND f.scrape_date = l.scrape_date
         AND ST_Intersects(l.geom, f.geom)
        -- Computed once via LATERAL rather than four times inline: the clip is
        -- the expensive part of this query, ST_Area is not.
        CROSS JOIN LATERAL (SELECT ST_Intersection(l.geom, f.geom) AS geom) AS clipped
        WHERE l.neighborhood = %(neighborhood)s
          AND l.scrape_date = %(scrape_date)s::date
          AND NOT ST_IsEmpty(clipped.geom)
          -- See the docstring: areal features must overlap in area, features
          -- that have no area only have to intersect.
          AND (ST_Dimension(clipped.geom) = 2 OR ST_Dimension(f.geom) < 2)
        """,
        {"neighborhood": neighborhood, "scrape_date": scrape_date},
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    cursor.execute(
        """
        SELECT count(DISTINCT lot_uid), count(DISTINCT feature_uid),
               count(DISTINCT source_table)
        FROM silver.lot_features
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    lots_matched, features_matched, layers = cursor.fetchone()

    # The denominator: "18 402 pairs" says nothing about coverage without the
    # count of lots that got none, which is the symptom of a partition loaded
    # only half way.
    cursor.execute(
        "SELECT count(*) FROM rag.lots WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    (num_lots,) = cursor.fetchone()

    return {
        "lot_features": result["upserted"],
        "pruned": result["pruned"],
        "lots_matched": int(lots_matched),
        "features_matched": int(features_matched),
        "layers": int(layers),
        "num_lots": int(num_lots),
    }


#: How far from a street side a lot boundary may be and still count as facing
#: it, in metres. The geobase double is drawn along the roadway, a lot line
#: sits behind the sidewalk and the service strip, and the city publishes the
#: layer "à titre indicatif" rather than to survey accuracy, so the gap is
#: several metres before any survey disagreement is added to it.
#:
#: Ten, not the three this used to be. Three was chosen as "wide enough to
#: cross a sidewalk", but measured against VSMPE it is not: the median lot in
#: that borough sits 4.85 m from its nearest street side, and at 3 m **90 % of
#: the borough's 24 952 lots got no row at all**. Running the measure over that
#: borough at each cutoff is what this value now follows -
#:
#:     cutoff   lots with no frontage
#:        3 m   22 545 of 24 952   (90.4 %)
#:        6 m    5 121             (20.5 %)
#:        8 m    1 316             ( 5.3 %)
#:       10 m      698             ( 2.8 %)
#:       12 m      673             ( 2.7 %)
#:
#: - and coverage plateaus at ten. What is left there is the right residual
#: rather than a shortfall: those 698 lots have a median area of 56 m2 and sit
#: a median 26.5 m from the nearest street side, which is an interior remnant
#: and not a lot the cutoff is hiding.
#:
#: Widening it used to cost accuracy, which is why it was kept small. It no
#: longer does: `compute_lot_frontage` measures only the boundary that runs
#: *along* a street side, so the corner over-count that grew with this number
#: is gone and the measure is flat in it. See that function.
DEFAULT_FRONTAGE_BUFFER_M = 10.0

#: How finely a lot boundary is chopped before each piece is matched to a
#: street, in metres. It sets the resolution of the measure - a corner lot's
#: frontage is right to within about this much - and the cost of it: halving it
#: doubles the rows the assignment runs over. One metre against parcels whose
#: median frontage is 8.6 m is roughly a tenth of the thing being measured.
FRONTAGE_SEGMENT_M = 1.0

#: How far from parallel a piece of lot boundary may run and still be counted
#: as facing the street it was matched to, as the sine of the angle between
#: them. 0.7071 is 45°.
#:
#: This is what makes the measure independent of `buffer_m`. A boundary piece
#: of length L whose ends sit d1 and d2 from the street side has
#: ``|d1 - d2| / L`` equal to the sine of the angle between the two: 0 for a
#: piece running along the street, 1 for one running straight at it. A lot's
#: *side* boundaries run at the street, and it is their first `buffer_m` that
#: the old buffer-clip counted as frontage - which is why widening the buffer
#: used to add two metres of phantom frontage per metre of buffer to every lot
#: in the borough. Dropping them costs nothing real: a lot line perpendicular
#: to the street is the neighbour's party line, not street edge.
FRONTAGE_MAX_SIN = 0.7071

#: Where the frontage geometry is measured. NAD83 / MTM zone 8 is the projected
#: system the island is surveyed in, the same one `street_assets.METRIC_CRS`
#: measures street length in. The measure needs a planar CRS rather than
#: `geography`: the assignment compares point-to-line distances across
#: thousands of candidate sides per lot, and `geography` has no index-backed
#: nearest-neighbour operator to do it with.
FRONTAGE_METRIC_SRID = 32188

#: How many of the lots that faced no street a run names. The count is the
#: measure of how bad a partition is; this sample is only where to start
#: looking, so it is small enough to read in a log line and in asset metadata.
_LOTS_WITHOUT_FRONTAGE_SAMPLE = 20


#: The relations `compute_lot_frontage` *reads*, and the hbu_infra file that
#: creates each. `silver.lot_frontage` is deliberately not among them: it is
#: the target, and `warehouse.upsert_select` checks that one for itself. These
#: two are the ones it cannot see, and the SELECT would otherwise fail on
#: whichever the planner resolved first.
_FRONTAGE_RELATIONS: tuple[tuple[str, str], ...] = (
    ("rag.lots", "sql/002_spatial.sql"),
    ("silver.neighborhood_streets", "sql/007_silver_streets.sql"),
)


def compute_lot_frontage(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
    buffer_m: float = DEFAULT_FRONTAGE_BUFFER_M,
) -> dict[str, object]:
    """(Re)compute `silver.lot_frontage` for one (neighborhood, scrape_date).

    How much of a lot faces a street, and which street. A lot with 30 m on a
    boulevard is a different development site from the one behind it with 6 m
    on a lane, and neither the cadastre nor the street network says so on its
    own: Infolot publishes the parcel, the geobase double publishes the sides
    of the roadway, and the relationship between them is geometric.

    **The measure is taken on the lot's boundary, not on the lot.** The
    obvious statement of the question -

        ST_Length(ST_Intersection(lot.geom, street_buffer.geom))

    - intersects two polygons, gets a polygon back, and `ST_Length` of a
    polygon is 0 in PostGIS: every row would report no frontage at all.
    Frontage is a length along the parcel's edge, so the left-hand side is the
    lot's boundary.

    **It is not a buffer clip.** It was, and the two things that were wrong
    with clipping the boundary to a buffered street side are the same thing
    seen from both ends. Making the buffer small enough not to distort the
    measure made it too small to reach the lots: at the 3 m this defaulted to,
    90 % of VSMPE's lots matched no street at all, because the geobase double
    is drawn along the roadway and the median lot line in that borough sits
    4.85 m behind it. Making it wide enough to reach them distorted the
    measure: a lot's two *side* boundaries run at the street, their first
    `buffer_m` falls inside the buffer too, and every lot in the borough gains
    two metres of frontage it does not have per metre of buffer. There is no
    value that is both, which is why this now measures something else.

    What it measures is the boundary that runs *along* a street side. Each lot
    boundary is chopped into `FRONTAGE_SEGMENT_M` pieces; each piece is matched
    to the single nearest street side within `buffer_m`; and a piece counts as
    frontage only if it runs within `FRONTAGE_MAX_SIN` of parallel to that
    side. The parallel test needs no trigonometry - for a piece of length L
    whose ends sit d1 and d2 from the side, ``|d1 - d2| / L`` *is* the sine of
    the angle between them - and it is what drops the side boundaries the
    buffer clip counted. The result is flat in `buffer_m`: lot 3 790 556
    measures 15.24 m on cote_rue_id 11000531 at a cutoff of 6, 8, 10 or 12 m,
    where the buffer clip reported 20.3, 24.3, 28.3 and 32.3 m for the same
    lot. So `buffer_m` is free to be wide enough to actually reach the lots.

    Matching each piece to the *nearest* side, rather than to every side within
    reach, is also what keeps a lot off the far side of its own street. The two
    sides of one roadway are only 5 to 8 m apart in this geobase, well inside a
    useful cutoff, but a lot's front boundary is always nearer its own side.

    Done in `FRONTAGE_METRIC_SRID` rather than through `geography`: the
    assignment is a nearest-neighbour search, and `<->` on the GiST index is
    what makes it one search per piece instead of a scan of every side in the
    borough. `buffer_m` is still metres - metres in MTM zone 8 are metres on
    the ground - and it is written onto every row so a table can always be read
    back against the cutoff that produced it.

    ``frontage_rank`` is 1 for the longest frontage a lot has, which is the
    column to filter on when a question wants *the* street a lot fronts on
    rather than every street it touches. A corner lot legitimately has two.

    A lot with no row here faced no street side within `buffer_m` - a genuine
    interior parcel, or a street snapshot that stops short of it. The caller
    gets the count and a sample of them back; see the `lots_without_frontage`
    key in the returned dict, which `frontage_assets` surfaces as metadata.

    Assumes `load_lots` and `load_streets` have already landed this partition's
    rows - both sides are filtered to (`neighborhood`, `scrape_date`) before
    anything is measured, so a street side from another date cannot match a lot
    from this one. `rag.lots` is loaded by `building_lot_intersections`, not here:
    two assets loading the same table from the same file in two transactions is
    the race that asset's docstring exists to describe.
    """
    cursor = connection.cursor()
    _require_relations(cursor, _FRONTAGE_RELATIONS)

    # The street sides, projected once and indexed, in a temp table rather than
    # a CTE. This is not tidiness: the assignment below is a nearest-neighbour
    # search per boundary piece, and there are some 2.6 million of them in a
    # borough. Against a CTE, `<->` has no index to walk and every piece scans
    # every side in the borough; against this, each piece is an index probe.
    # Measured on VSMPE, that is the difference between a query that does not
    # finish and one that takes about a minute.
    #
    # Dropped first rather than declared ON COMMIT DROP. This runs inside the
    # caller's transaction - `connect` above commits on a clean exit - so
    # ON COMMIT DROP would be the tidier declaration, and would also silently
    # destroy the table between the CREATE and the SELECT if a caller ever
    # handed this an autocommit connection. Session scope survives that, a
    # rollback still takes the table with it because DDL is transactional here,
    # and this makes a second call on one connection work either way.
    cursor.execute("DROP TABLE IF EXISTS _frontage_sides")
    cursor.execute(
        f"""
        CREATE TEMP TABLE _frontage_sides AS
        SELECT cote_rue_id, street_name,
               ST_Transform(geom, {int(FRONTAGE_METRIC_SRID)}) AS geom
        FROM silver.neighborhood_streets
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    cursor.execute("CREATE INDEX ON _frontage_sides USING gist (geom)")
    # Without stats the planner costs the LATERAL against a default row
    # estimate and can still choose a scan over the index it was just handed.
    cursor.execute("ANALYZE _frontage_sides")

    result = warehouse.upsert_select(
        cursor,
        "lot_frontage",
        (
            "scrape_date",
            "neighborhood",
            "lot_uid",
            "cote_rue_id",
            "lot_number",
            "street_name",
            "buffer_m",
            "frontage_m",
            "lot_perimeter_m",
            "pct_of_perimeter",
            "frontage_rank",
            "geom",
        ),
        """
        WITH parcels AS (
            SELECT lot_uid, lot_number, neighborhood, scrape_date,
                   ST_Transform(geom, %(srid)s) AS geom,
                   -- Carried from here rather than recomputed per street side:
                   -- the perimeter is a property of the lot, and the GROUP BY
                   -- below would otherwise have to group by a geometry.
                   ST_Perimeter(ST_Transform(geom, %(srid)s)) AS lot_perimeter_m
            FROM rag.lots
            WHERE neighborhood = %(neighborhood)s
              AND scrape_date = %(scrape_date)s::date
        ),
        -- Every lot boundary chopped into pieces of at most `step_m`, one row
        -- each. ST_Segmentize only ever adds vertices, so a boundary shorter
        -- than the step survives whole rather than being rounded away.
        pieces AS (
            SELECT p.lot_uid, (seg).geom AS geom
            FROM parcels p
            CROSS JOIN LATERAL ST_DumpSegments(
                ST_Segmentize(
                    ST_Boundary(p.geom), %(step_m)s::double precision
                )
            ) AS seg
        ),
        -- Winner takes all: the single nearest side within `buffer_m`, found
        -- through the GiST index rather than by scanning the borough's sides.
        -- Matching every side within reach instead would put a lot on both
        -- sides of its own street, which are 5 to 8 m apart in this geobase.
        assigned AS (
            SELECT c.lot_uid, c.geom, near.cote_rue_id, near.street_name,
                   ST_Distance(ST_StartPoint(c.geom), near.geom) AS d_start,
                   ST_Distance(ST_EndPoint(c.geom), near.geom) AS d_end,
                   ST_Length(c.geom) AS piece_m
            FROM pieces c
            CROSS JOIN LATERAL (
                SELECT s.cote_rue_id, s.street_name, s.geom
                FROM _frontage_sides s
                WHERE ST_DWithin(s.geom, c.geom, %(buffer_m)s::double precision)
                ORDER BY s.geom <-> c.geom
                LIMIT 1
            ) AS near
        ),
        -- The parallel test. |d_start - d_end| / length is the sine of the
        -- angle between the piece and the side it was matched to: 0 along the
        -- street, 1 straight at it. This is what the old buffer clip had no
        -- way to express, and why its answer grew with the buffer.
        facing AS (
            SELECT lot_uid, cote_rue_id, street_name, geom, piece_m
            FROM assigned
            WHERE piece_m > 0
              AND abs(d_start - d_end) / piece_m <= %(max_sin)s::double precision
        ),
        measured AS (
            SELECT
                p.scrape_date,
                p.neighborhood,
                p.lot_uid,
                p.lot_number,
                f.cote_rue_id,
                max(f.street_name) AS street_name,
                sum(f.piece_m) AS frontage_m,
                p.lot_perimeter_m,
                row_number() OVER (
                    PARTITION BY p.lot_uid
                    -- cote_rue_id only to make the order total, so a re-run
                    -- ranks two exactly equal frontages the same way twice.
                    ORDER BY sum(f.piece_m) DESC, f.cote_rue_id
                ) AS frontage_rank,
                -- Merged back into as few linestrings as the pieces allow -
                -- one per run of contiguous edge - and returned to 4326, the
                -- CRS every geometry in this database is stored in. A corner
                -- lot's frontage on one street is contiguous; a lot that meets
                -- the same side twice is legitimately two strands.
                ST_Transform(ST_LineMerge(ST_Collect(f.geom)), 4326) AS geom
            FROM facing f
            JOIN parcels p ON p.lot_uid = f.lot_uid
            GROUP BY p.scrape_date, p.neighborhood, p.lot_uid, p.lot_number,
                     p.lot_perimeter_m, f.cote_rue_id
        )
        SELECT
            scrape_date, neighborhood, lot_uid, cote_rue_id, lot_number,
            street_name,
            %(buffer_m)s::double precision, frontage_m, lot_perimeter_m,
            CASE WHEN lot_perimeter_m > 0
                 THEN 100.0 * frontage_m / lot_perimeter_m
                 ELSE 0.0
            END,
            frontage_rank,
            geom
        FROM measured
        WHERE frontage_m > 0
        """,
        {
            "neighborhood": neighborhood,
            "scrape_date": scrape_date,
            "buffer_m": float(buffer_m),
            "step_m": float(FRONTAGE_SEGMENT_M),
            "max_sin": float(FRONTAGE_MAX_SIN),
            "srid": int(FRONTAGE_METRIC_SRID),
        },
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    cursor.execute(
        """
        SELECT count(DISTINCT lot_uid), count(DISTINCT cote_rue_id),
               COALESCE(sum(frontage_m), 0), COALESCE(max(frontage_m), 0)
        FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    lots_matched, streets_matched, total_frontage_m, max_frontage_m = cursor.fetchone()

    # The denominators. "4 812 pairs" says nothing about coverage without the
    # count of lots that got none - a lot facing no street at all is either an
    # interior parcel or the symptom of a street snapshot that never landed.
    cursor.execute(
        "SELECT count(*) FROM rag.lots WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    (num_lots,) = cursor.fetchone()
    cursor.execute(
        "SELECT count(*) FROM silver.neighborhood_streets "
        "WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    (num_streets,) = cursor.fetchone()

    # Which lots those are, not only how many. Every lot in a Montreal borough
    # is expected to face at least one street side; the ones that do not are
    # either genuine interior parcels or the symptom of a street snapshot that
    # stops short of them, and either way they are the rows to go and look at.
    # Capped because a partition that has gone wrong produces thousands of
    # them, and the count above is what says how wrong - the sample only says
    # where to start. Ordered so a re-run names the same lots.
    cursor.execute(
        f"""
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
        LIMIT {_LOTS_WITHOUT_FRONTAGE_SAMPLE}
        """,
        [neighborhood, scrape_date],
    )
    lots_without_frontage = [row[0] for row in cursor.fetchall()]

    return {
        "frontages": result["upserted"],
        "pruned": result["pruned"],
        "lots_matched": int(lots_matched),
        "streets_matched": int(streets_matched),
        "total_frontage_m": float(total_frontage_m),
        "max_frontage_m": float(max_frontage_m),
        "num_lots": int(num_lots),
        "num_streets": int(num_streets),
        # A sample, not the whole set - see the query. `num_lots` minus
        # `lots_matched` is the true count.
        "lots_without_frontage": lots_without_frontage,
        "buffer_m": float(buffer_m),
    }


# --------------------------------------------------------------------------
# what is left of a lot once its margins are taken off it
# --------------------------------------------------------------------------

#: Where the buildable envelope is carved, how finely the boundary is chopped
#: to sort it, and how near parallel a piece has to run to count as the rear
#: rather than a side. All three are `compute_lot_frontage`'s values, aliased
#: rather than restated: the rear/side test *is* that function's parallel test
#: pointed at the lot's front edge instead of at a street side, so the two
#: moving apart should be a decision someone makes here rather than a drift
#: nobody notices. See `FRONTAGE_MAX_SIN` for the derivation.
SETBACK_METRIC_SRID = FRONTAGE_METRIC_SRID
SETBACK_SEGMENT_M = FRONTAGE_SEGMENT_M
SETBACK_MAX_SIN = FRONTAGE_MAX_SIN

#: How far off the lot's boundary a `silver.lot_frontage` linestring may sit
#: and still be subtracted from it as street edge, in metres.
#:
#: Not a tolerance on the data: that geometry was cut *from* this boundary one
#: asset earlier, so the two are the same line and this only has to absorb the
#: round trip through EPSG:4326 the table stores it in. Small enough that the
#: 5 cm it also nibbles off either end of a side edge is well under
#: `SETBACK_SEGMENT_M`, and therefore under the resolution of the sort it
#: feeds.
DEFAULT_SETBACK_EDGE_TOLERANCE_M = 0.05

#: The side setback each reading of *Mode d'implantation* produces, as a
#: multiple of the *Latérale min* the grid printed.
#:
#: Documented in sql/015 and restated here only as the numbers: contiguous
#: building is built to both party lines, semi-detached to one. A half margin
#: off both sides removes exactly what a whole margin off one side does for any
#: parcel whose side lines are parallel, and which side carries the party wall
#: is a fact about the neighbour that no layer this platform reads publishes.
SIDE_SETBACK_FACTORS: dict[str, float] = {
    "contigu": 0.0,
    "jumele": 0.5,
    "isole": 1.0,
    # A column stating no mode at all takes the full margin on both sides: the
    # conservative reading, and the one that cannot quietly hand a lot more
    # buildable area than its grid allows.
    "unknown": 1.0,
}

#: The relations `compute_lot_buildable_setbacks` reads, and the hbu_infra file
#: that creates each. `silver.lot_buildable_setbacks` is deliberately not among
#: them - it is the target, and `warehouse.upsert_select` checks that one.
_BUILDABLE_RELATIONS: tuple[tuple[str, str], ...] = (
    ("rag.lots", "sql/002_spatial.sql"),
    ("silver.lot_frontage", "sql/008_silver_lot_frontage.sql"),
    ("silver.lot_zoning_envelopes", "sql/012_silver_zoning.sql"),
)

#: The per-lot temp table the boundary sort lands in, named to the same
#: convention `_frontage_sides` follows.
_SETBACK_EDGES = "_setback_edges"

#: The boundary sort itself: one row per lot, carrying the parcel, the two
#: street edges `lot_frontage` measured, and the rear and side edges this
#: derives - every geometry in `SETBACK_METRIC_SRID`.
#:
#: A temp table and not a CTE, for the reason `_frontage_sides` is one: the
#: statement below joins it once per *(lot, zone, column)*, and a lot in a
#: borough of overlapping zones has several. Sorting a boundary is per-lot work
#: and belongs on the lot side of that fan-out - inlined as a CTE it would be
#: re-derived for every candidate envelope of every parcel.
_SETBACK_EDGES_SQL = """
WITH parcels AS (
    SELECT lot_uid, lot_number,
           ST_Transform(geom, %(srid)s) AS geom
      FROM rag.lots
     WHERE neighborhood = %(neighborhood)s
       AND scrape_date = %(scrape_date)s::date
),
-- The street edges, as `lot_frontage` measured them rather than as anything
-- guessed at from the parcel's shape. Rank 1 is the street the lot mostly
-- fronts on and takes *Avant principale*; rank 2 exists only on a corner lot
-- and takes *Avant secondaire*. `all_geom` is every rank, and is what gets
-- subtracted from the boundary below - a lot facing three streets must not
-- have its third edge come back as a side.
street AS (
    SELECT lot_uid,
           ST_Union(ST_Transform(geom, %(srid)s))
               FILTER (WHERE frontage_rank = 1) AS front_geom,
           ST_Union(ST_Transform(geom, %(srid)s))
               FILTER (WHERE frontage_rank = 2) AS secondary_geom,
           ST_Union(ST_Transform(geom, %(srid)s)) AS all_geom
      FROM silver.lot_frontage
     WHERE neighborhood = %(neighborhood)s
       AND scrape_date = %(scrape_date)s::date
     GROUP BY lot_uid
),
-- What is left of the boundary once the street edges are taken out of it,
-- chopped into pieces of at most `step_m`. ST_Segmentize only ever adds
-- vertices, so an edge shorter than the step survives whole.
pieces AS (
    SELECT p.lot_uid, (seg).geom AS geom
      FROM parcels p
      JOIN street s ON s.lot_uid = p.lot_uid
      CROSS JOIN LATERAL ST_DumpSegments(
          ST_Segmentize(
              ST_Difference(
                  ST_Boundary(p.geom),
                  ST_Buffer(s.all_geom, %(tolerance_m)s::double precision)
              ),
              %(step_m)s::double precision
          )
      ) AS seg
     WHERE s.front_geom IS NOT NULL
),
-- The sort. |d_start - d_end| / length is the sine of the angle between the
-- piece and the front edge: 0 for one running parallel to the street, 1 for
-- one running straight at it. A lot's rear line is parallel to its front and
-- its side lines are perpendicular, so the one threshold separates them.
classified AS (
    SELECT c.lot_uid, c.geom,
           ST_Length(c.geom) AS piece_m,
           CASE WHEN abs(
                        ST_Distance(ST_StartPoint(c.geom), s.front_geom)
                      - ST_Distance(ST_EndPoint(c.geom), s.front_geom)
                    ) / ST_Length(c.geom) <= %(max_sin)s::double precision
                THEN 'rear'
                ELSE 'side'
           END AS edge_class
      FROM pieces c
      JOIN street s ON s.lot_uid = c.lot_uid
     WHERE ST_Length(c.geom) > 0
),
edges AS (
    SELECT lot_uid,
           ST_Union(geom) FILTER (WHERE edge_class = 'rear') AS rear_geom,
           ST_Union(geom) FILTER (WHERE edge_class = 'side') AS side_geom,
           COALESCE(sum(piece_m) FILTER (WHERE edge_class = 'rear'), 0.0)
               AS rear_edge_m,
           COALESCE(sum(piece_m) FILTER (WHERE edge_class = 'side'), 0.0)
               AS side_edge_m
      FROM classified
     GROUP BY lot_uid
)
SELECT p.lot_uid,
       p.lot_number,
       p.geom AS lot_geom,
       ST_Area(p.geom) AS lot_area_m2,
       s.front_geom,
       s.secondary_geom,
       e.rear_geom,
       e.side_geom,
       COALESCE(ST_Length(s.front_geom), 0.0) AS front_edge_m,
       COALESCE(ST_Length(s.secondary_geom), 0.0) AS secondary_front_edge_m,
       COALESCE(e.rear_edge_m, 0.0) AS rear_edge_m,
       COALESCE(e.side_edge_m, 0.0) AS side_edge_m
  FROM parcels p
  JOIN street s ON s.lot_uid = p.lot_uid
  -- LEFT, unlike the join above: a lot whose entire boundary was measured as
  -- street edge - a through lot, an island parcel - has a front and no other
  -- class, and is a row with two zero lengths rather than a row that is gone.
  LEFT JOIN edges e ON e.lot_uid = p.lot_uid
 WHERE s.front_geom IS NOT NULL
"""


def compute_lot_buildable_setbacks(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
    edge_tolerance_m: float = DEFAULT_SETBACK_EDGE_TOLERANCE_M,
) -> dict[str, object]:
    """(Re)compute `silver.lot_buildable_setbacks` for one partition.

    What is left of each lot once the four margins its zoning grid states are
    taken off it, at the grain the grid states them: one row per (lot, zone,
    grid column). `silver.lot_zoning_envelopes` has carried *Avant principale*,
    *Avant secondaire*, *Latérale* and *Arrière* since sql/012 with nothing
    subtracting them, and `urban_rag.program` caps a footprint on *Taux
    d'implantation* alone - so a shallow lot and a deep one of the same area
    have been solving identically. This is the other cap.

    **The subtraction is directional, which is why it is not a negative
    buffer.** `ST_Buffer(lot, -d)` takes the same d off every edge; the margins
    are four different distances at four different edges, and no single d
    expresses them. So the boundary is sorted first:

    * **front** - `silver.lot_frontage.geom` at `frontage_rank = 1`, which is
      the boundary that asset *measured* as running along a street rather than
      an edge guessed at from the parcel's shape.
    * **secondary** - the same at rank 2. Only a corner lot has one, and it
      takes *Avant secondaire*, falling back to *Avant principale* where the
      grid states only the one - a corner lot's second street edge is still a
      street edge, and leaving it unregulated would hand a corner parcel more
      room than the mid-block lot beside it.
    * **rear** - of what is left, the pieces running within `SETBACK_MAX_SIN`
      of parallel to the front.
    * **side** - of what is left, everything else.

    That last test is `compute_lot_frontage`'s own, pointed at the lot's front
    edge instead of at a street side: for a piece of length L whose ends sit d1
    and d2 from the front, ``|d1 - d2| / L`` is the sine of the angle between
    them. It needs no trigonometry and it is why the two functions share one
    constant rather than each carrying a threshold.

    **It is also not a width x depth rectangle.** Estimating depth as area over
    frontage and multiplying ``(width - 2*side) x (depth - front - rear)`` is
    exact for a rectangle and wrong for the wedges, dog-legs and skewed rear
    lines a real cadastre is full of. Both inputs to the honest version are
    already here - the polygon in `rag.lots`, the street edge in
    `silver.lot_frontage` - so the proxy would buy nothing.

    **`side_setback_m` is not `side_margin_min_m`,** and this is the difference
    that moves the answer most. *Mode d'implantation* decides whether the side
    margin applies at all: a contiguous building is built to the party line and
    has no side setback, a semi-detached one has a single side. The grid prints
    the permitted modes together - VSMPE prints `I-J` and `I-J-C` - and the
    most permissive is applied, because this table answers what *may* be built.
    `SIDE_SETBACK_FACTORS` holds the three multiples; `side_setback_rule`
    records which was read and `side_margin_min_m` what the grid printed, so a
    row can be read back against the rule that produced it. Subtracting the
    printed margin from both sides of every lot in a borough of plexes would
    understate most of the stock.

    A margin the grid prints as ``-`` is NULL upstream and 0 here. The two
    coincide for this purpose - an unstated margin takes nothing off the lot -
    and the distinction stays visible in `silver.lot_zoning_envelopes` for a
    reader who needs "no rear margin" apart from "a rear margin of zero".

    **A lot with no frontage row gets no row here.** There is no edge to call
    the front, so the angle test has no reference and the four classes are
    undefined; inventing a front from the parcel's longest edge would put
    *Avant principale* on a party line. The count comes back in
    `lots_without_frontage` for the asset to report, the same posture
    `compute_lot_frontage` takes towards the same lots.

    `footprint_cap_m2` is the lesser of the buildable envelope and *Taux
    d'implantation au sol max* x lot area, and `footprint_cap_binding` says
    which of the two produced it. They are independent caps - one says where on
    the lot, the other how much of it - and a building satisfies both.

    Assumes `compute_lot_frontage` and `lot_zoning_envelopes` have both landed
    this partition. Both are dependencies on the assets rather than something
    the SQL can check: a borough whose envelopes were never computed yields no
    rows and looks exactly like a borough whose grids all failed to parse,
    which is why the caller gets `num_envelopes` back to tell them apart.
    """
    tolerance = float(edge_tolerance_m)
    if tolerance <= 0:
        raise ValueError(
            f"edge_tolerance_m must be positive, got {edge_tolerance_m!r}"
        )

    cursor = connection.cursor()
    _require_relations(cursor, _BUILDABLE_RELATIONS)

    parameters: dict[str, Any] = {
        "neighborhood": neighborhood,
        "scrape_date": scrape_date,
        "srid": int(SETBACK_METRIC_SRID),
        "step_m": float(SETBACK_SEGMENT_M),
        "max_sin": float(SETBACK_MAX_SIN),
        "tolerance_m": tolerance,
    }

    # Dropped rather than declared ON COMMIT DROP, for the reason
    # `_frontage_sides` is: this runs inside the caller's transaction, and
    # session scope is what makes a second call on one connection work whether
    # or not that connection is in autocommit.
    cursor.execute(f"DROP TABLE IF EXISTS {_SETBACK_EDGES}")
    cursor.execute(
        f"CREATE TEMP TABLE {_SETBACK_EDGES} AS {_SETBACK_EDGES_SQL}", parameters
    )
    cursor.execute(f"CREATE INDEX ON {_SETBACK_EDGES} (lot_uid)")
    # Without stats the planner costs the join below against a default row
    # estimate and can pick a scan over the index it was just handed - the
    # same reason `compute_lot_frontage` analyzes `_frontage_sides`.
    cursor.execute(f"ANALYZE {_SETBACK_EDGES}")
    cursor.execute(f"SELECT count(*) FROM {_SETBACK_EDGES}")
    (lots_sorted,) = cursor.fetchone()

    result = warehouse.upsert_select(
        cursor,
        "lot_buildable_setbacks",
        (
            "scrape_date", "neighborhood", "lot_uid", "feature_id",
            "column_index", "lot_number", "source_table", "lot_area_m2",
            "front_edge_m", "secondary_front_edge_m", "side_edge_m",
            "rear_edge_m",
            "implantation_mode", "side_setback_rule", "side_margin_min_m",
            "front_setback_m", "secondary_front_setback_m", "side_setback_m",
            "rear_setback_m",
            "buildable_area_m2", "buildable_pct_of_lot",
            "coverage_cap_m2", "footprint_cap_m2", "footprint_cap_binding",
            "pct_of_lot", "governs_residential", "solver_ready",
            "max_sin", "segment_m", "edge_tolerance_m",
            "geom",
        ),
        """
        WITH norms AS (
            -- The margins as the grid states them, and the mode read off the
            -- text it prints. `strpos` rather than LIKE throughout: a literal
            -- per-cent sign anywhere in this statement is read by psycopg as
            -- the start of a placeholder, and the accents are avoided by
            -- matching the stems 'isol', 'jumel' and 'contigu' - none of which
            -- carries one - so no unaccent extension is needed either.
            --
            -- Two spellings are handled because two exist: a borough printing
            -- the modes as words, and VSMPE's own template printing them as
            -- hyphenated letter codes ('I-J', 'I-J-C'). Most permissive wins,
            -- which is why contiguous is tested first.
            SELECT
                e.lot_uid, e.feature_id, e.column_index, e.lot_number,
                e.source_table, e.implantation_mode, e.side_margin_min_m,
                e.site_coverage_max_pct, e.pct_of_lot, e.governs_residential,
                e.solver_ready,
                CASE
                    WHEN e.implantation_mode IS NULL THEN 'unknown'
                    WHEN strpos(lower(e.implantation_mode), 'contigu') > 0
                      OR 'C' = ANY(string_to_array(
                             upper(translate(e.implantation_mode, ' .', '')), '-'))
                        THEN 'contigu'
                    WHEN strpos(lower(e.implantation_mode), 'jumel') > 0
                      OR 'J' = ANY(string_to_array(
                             upper(translate(e.implantation_mode, ' .', '')), '-'))
                        THEN 'jumele'
                    WHEN strpos(lower(e.implantation_mode), 'isol') > 0
                      OR 'I' = ANY(string_to_array(
                             upper(translate(e.implantation_mode, ' .', '')), '-'))
                        THEN 'isole'
                    ELSE 'unknown'
                END AS side_setback_rule,
                COALESCE(e.front_margin_min_m, 0.0) AS front_setback_m,
                -- *Avant secondaire* where the grid states one, *Avant
                -- principale* where it states only that, 0 where it states
                -- neither. See the docstring.
                COALESCE(
                    e.secondary_front_margin_min_m, e.front_margin_min_m, 0.0
                ) AS secondary_front_setback_m,
                COALESCE(e.rear_margin_min_m, 0.0) AS rear_setback_m
              FROM silver.lot_zoning_envelopes e
             WHERE e.neighborhood = %(neighborhood)s
               AND e.scrape_date = %(scrape_date)s::date
        ),
        applied AS (
            SELECT n.*,
                   CASE n.side_setback_rule
                       WHEN 'contigu' THEN 0.0
                       WHEN 'jumele' THEN COALESCE(n.side_margin_min_m, 0.0) / 2.0
                       ELSE COALESCE(n.side_margin_min_m, 0.0)
                   END AS side_setback_m
              FROM norms n
        ),
        carved AS (
            -- One ST_Difference against the union of the four buffers, rather
            -- than four nested differences: the cuts overlap at every corner
            -- of the parcel, and unioning them first is what keeps that from
            -- being counted twice.
            --
            -- ST_Buffer of an empty geometry, and of any geometry at distance
            -- 0, is an empty polygon - so a lot with no second street edge and
            -- a column stating no rear margin both cut nothing, without either
            -- needing a branch of its own. The COALESCEs are only to keep a
            -- NULL out of the array.
            SELECT a.*,
                   b.lot_number AS cadastre_lot_number,
                   b.lot_area_m2,
                   b.front_edge_m, b.secondary_front_edge_m,
                   b.side_edge_m, b.rear_edge_m,
                   ST_CollectionExtract(
                       ST_Difference(
                           b.lot_geom,
                           ST_UnaryUnion(ST_Collect(ARRAY[
                               ST_Buffer(b.front_geom, a.front_setback_m),
                               ST_Buffer(
                                   COALESCE(b.secondary_geom, blank.geom),
                                   a.secondary_front_setback_m
                               ),
                               ST_Buffer(
                                   COALESCE(b.rear_geom, blank.geom),
                                   a.rear_setback_m
                               ),
                               ST_Buffer(
                                   COALESCE(b.side_geom, blank.geom),
                                   a.side_setback_m
                               )
                           ]))
                       ),
                       3
                   ) AS buildable_geom
              FROM applied a
              JOIN {setback_edges} b ON b.lot_uid = a.lot_uid
              CROSS JOIN (
                  SELECT ST_SetSRID('LINESTRING EMPTY'::geometry, %(srid)s) AS geom
              ) blank
        ),
        measured AS (
            SELECT c.*,
                   ST_Area(c.buildable_geom) AS buildable_area_m2,
                   CASE WHEN c.site_coverage_max_pct IS NOT NULL
                             AND c.lot_area_m2 IS NOT NULL
                        THEN c.lot_area_m2 * c.site_coverage_max_pct / 100.0
                   END AS coverage_cap_m2
              FROM carved c
        )
        SELECT
            %(scrape_date)s::date,
            %(neighborhood)s,
            m.lot_uid,
            m.feature_id,
            m.column_index,
            -- The envelope's own lot number where it has one; older envelope
            -- files predate `lot_features` carrying it, and the cadastre side
            -- of the join always does.
            COALESCE(m.lot_number, m.cadastre_lot_number),
            m.source_table,
            m.lot_area_m2,
            m.front_edge_m,
            m.secondary_front_edge_m,
            m.side_edge_m,
            m.rear_edge_m,
            m.implantation_mode,
            m.side_setback_rule,
            m.side_margin_min_m,
            m.front_setback_m,
            m.secondary_front_setback_m,
            m.side_setback_m,
            m.rear_setback_m,
            m.buildable_area_m2,
            CASE WHEN m.lot_area_m2 > 0
                 THEN 100.0 * m.buildable_area_m2 / m.lot_area_m2
            END,
            m.coverage_cap_m2,
            -- LEAST ignores a NULL argument, which is exactly wrong here: a
            -- column stating no coverage maximum should leave the setback
            -- answer standing, not be silently dropped from a comparison a
            -- reader thinks happened. Spelled out instead.
            CASE WHEN m.coverage_cap_m2 IS NULL
                 THEN m.buildable_area_m2
                 ELSE LEAST(m.buildable_area_m2, m.coverage_cap_m2)
            END,
            -- Ties go to 'setbacks': it is the cap that also constrains the
            -- shape, so when the two agree on the area it is still the one
            -- doing the work.
            CASE WHEN m.coverage_cap_m2 IS NOT NULL
                      AND m.coverage_cap_m2 < m.buildable_area_m2
                 THEN 'site_coverage'
                 ELSE 'setbacks'
            END,
            m.pct_of_lot,
            m.governs_residential,
            m.solver_ready,
            %(max_sin)s::double precision,
            %(step_m)s::double precision,
            %(tolerance_m)s::double precision,
            ST_Multi(ST_Transform(m.buildable_geom, 4326))
        FROM measured m
        """.replace("{setback_edges}", _SETBACK_EDGES),
        parameters,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    # The denominators, and the two gaps worth telling apart: a lot with no
    # frontage row could not be sorted, and a lot with no envelope row has no
    # margins to subtract. Neither is a failure and they have different fixes.
    cursor.execute(
        """
        SELECT count(*) FROM rag.lots
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    (num_lots,) = cursor.fetchone()
    cursor.execute(
        """
        SELECT count(*), count(DISTINCT lot_uid)
        FROM silver.lot_zoning_envelopes
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    num_envelopes, lots_with_envelopes = cursor.fetchone()

    cursor.execute(
        """
        SELECT count(DISTINCT lot_uid),
               count(*) FILTER (WHERE footprint_cap_binding = 'setbacks'),
               count(*) FILTER (WHERE footprint_cap_binding = 'site_coverage'),
               count(*) FILTER (WHERE buildable_area_m2 <= 0),
               COALESCE(sum(buildable_area_m2), 0),
               COALESCE(avg(buildable_pct_of_lot), 0)
        FROM silver.lot_buildable_setbacks
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    (
        lots_measured,
        bound_by_setbacks,
        bound_by_coverage,
        num_unbuildable,
        total_buildable_area_m2,
        mean_buildable_pct,
    ) = cursor.fetchone()

    # How the borough's stock reads under each mode, which is the one number
    # that says whether the side rule is doing what it should: a VSMPE where
    # nothing came back 'contigu' is a mode column that failed to parse, not a
    # borough of detached houses.
    cursor.execute(
        """
        SELECT side_setback_rule, count(*)
        FROM silver.lot_buildable_setbacks
        WHERE neighborhood = %s AND scrape_date = %s::date
        GROUP BY side_setback_rule
        """,
        [neighborhood, scrape_date],
    )
    by_side_rule = {str(rule): int(count) for rule, count in cursor.fetchall()}

    return {
        "rows": result["upserted"],
        "pruned": result["pruned"],
        "num_lots": int(num_lots),
        # Sorted but not necessarily measured: a lot whose boundary was sorted
        # and whose zone published no readable grid has no envelope to
        # subtract, so it is in the first count and not the second.
        "num_lots_sorted": int(lots_sorted),
        "num_lots_measured": int(lots_measured),
        "lots_without_frontage": int(num_lots) - int(lots_sorted),
        "num_envelopes": int(num_envelopes),
        "num_lots_with_envelopes": int(lots_with_envelopes),
        "num_bound_by_setbacks": int(bound_by_setbacks),
        "num_bound_by_site_coverage": int(bound_by_coverage),
        # A real answer rather than a gap: a parcel narrower than twice its
        # side margin has nowhere to put a building.
        "num_unbuildable": int(num_unbuildable),
        "total_buildable_area_m2": float(total_buildable_area_m2),
        "mean_buildable_pct_of_lot": float(mean_buildable_pct),
        "by_side_setback_rule": by_side_rule,
        "edge_tolerance_m": tolerance,
        "max_sin": float(SETBACK_MAX_SIN),
        "segment_m": float(SETBACK_SEGMENT_M),
    }


#: The categories `compute_lot_profiles` sorts a lot into. Kept here rather
#: than only in SQL because the asset reports one count per category and a
#: missing key would silently read as zero.
#:
#: The first three are what the old `vacant_lots` table held; `built` is the
#: fourth case that table expressed by not having a row. Every lot lands in
#: exactly one of them.
LOT_CATEGORIES: tuple[str, ...] = (
    "built",
    "no_building",
    "shed_only",
    "building_sliver",
)

#: Default cutoff for "there is effectively nothing built here", in square
#: metres of footprint standing on the lot. A garden shed is 10-30 m2 and a
#: detached garage 30-60, so 30 keeps the shed and drops the garage.
DEFAULT_MAX_BUILT_AREA_M2 = 30.0

#: The relations `compute_lot_profiles` reads, and the hbu_infra file that
#: creates each. Checked up front so a partition fails naming what to apply
#: rather than on whichever identifier the planner happened to resolve first.
_LOT_PROFILE_RELATIONS: tuple[tuple[str, str], ...] = (
    ("rag.lots", "sql/002_spatial.sql"),
    ("silver.building_lot_intersections", "sql/004_silver_building_lots.sql"),
    ("silver.lot_frontage", "sql/008_silver_lot_frontage.sql"),
    # A view, and the one most likely to be missing: 006 carries a
    # `-- requires: rag.chunks` header, so `db.py init` skips it on a database
    # that has never held a corpus and it only lands on the *next* init after
    # document_index has run.
    ("rag.lot_documents", "sql/006_lot_documents.sql"),
    (
        "silver.lot_assessed_values",
        "sql/013_silver_lot_assessed_values.sql",
    ),
    (
        "silver.lot_assessment_comparables",
        "sql/016_silver_lot_assessment_comparables.sql",
    ),
    # Read twice below - once narrowed to the row governing each lot, once at
    # its own grain to merge into the envelope entries - so a database without
    # it fails here naming the file rather than on whichever of the two the
    # planner resolved first.
    (
        "silver.lot_buildable_setbacks",
        "sql/015_silver_lot_buildable_setbacks.sql",
    ),
    ("gold.lot_profiles", "sql/009_gold_lot_profiles.sql"),
)


#: The staging table `_stage_lot_envelopes` lands the envelope rows in, named
#: to the same `{table}_load` convention `_replace_partition` uses.
_ENVELOPE_STAGING = "lot_profiles_envelopes_load"


def _stage_lot_envelopes(
    cursor: Any, envelopes: "Sequence[tuple[str, dict]]"
) -> int:
    """COPY ``envelopes`` into a temp table `compute_lot_profiles` joins to.

    ``envelopes`` is `(lot_number, entry)` pairs in the order the asset wants
    them to appear in the array - most-of-the-lot first - and that order is
    staged alongside as `ordinal` rather than re-derived from inside the jsonb,
    so the ordering the producer decided is the ordering that lands.

    The join key is `lot_number` and not `lot_uid`, for the reason
    009_lot_profiles.sql gives for denormalising it in the first place:
    `lot_uid` is a bigserial that `load_lots` mints again on every reload, so
    an envelope file written against yesterday's load would join to the wrong
    parcels - or, worse, to plausible ones.

    The table is created whether or not there is anything to put in it: the
    INSERT below names it unconditionally, and a partition whose envelopes have
    not been computed should land zero rows rather than fail to plan.
    """
    cursor.execute(
        f"CREATE TEMP TABLE {_ENVELOPE_STAGING} "
        "(lot_number text, ordinal integer, envelope jsonb) ON COMMIT DROP"
    )
    if not envelopes:
        return 0

    _psycopg()  # raises PostgresUnavailable with a clear message if missing
    from psycopg.types.json import Jsonb

    staged = 0
    statement = (
        f"COPY {_ENVELOPE_STAGING} (lot_number, ordinal, envelope) "
        "FROM STDIN (FORMAT BINARY)"
    )
    with cursor.copy(statement) as copy:
        copy.set_types(["text", "integer", "jsonb"])
        for ordinal, (lot_number, entry) in enumerate(envelopes):
            if lot_number is None:
                continue
            copy.write_row([str(lot_number), ordinal, Jsonb(entry)])
            staged += 1
    return staged


def compute_lot_profiles(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
    max_built_area_m2: float = DEFAULT_MAX_BUILT_AREA_M2,
    vacancy_rates: dict | None = None,
    average_rents: dict | None = None,
    construction_costs: dict | None = None,
    zoning_envelopes: "Sequence[tuple[str, dict]]" = (),
) -> dict[str, object]:
    """(Re)compute `gold.lot_profiles` for one (neighborhood, scrape_date).

    One row per lot in the borough - every lot, not a selection of them. Three
    joins that each hold one row per (lot x something) are collapsed onto that
    grain and land side by side:

    * `silver.building_lot_intersections` -> `num_buildings`, `built_area_m2`, `category`
    * `silver.lot_frontage`  -> `primary_*` and `secondary_*`, `num_frontages`
    * `rag.lot_documents` -> `doc_*` and the `documents` array

    A fourth table joins without a CTE, because it is the one input that
    already arrives at this grain:

    * `silver.lot_assessed_values` -> `total_assessed_value` and its counts

    Its primary key is (scrape_date, neighborhood, lot_number), so there is
    nothing to group and the join cannot fan a lot out into several rows. It is
    Quebec's *rôle d'évaluation foncière* carried onto the cadastre - what the
    ground is worth standing as it is, which is the half of a
    highest-and-best-use question the cost columns do not answer.

    A fifth is read twice, because the two reads want different grains:

    * `silver.lot_buildable_setbacks` -> `buildable_area_m2`,
      `footprint_cap_m2` and the two columns that qualify them

    That table is one row per (lot, zone, column) like the envelopes. The
    flattened columns take the row *governing* the lot - the grid column
    `select_residential_column` chose, and failing that the zone covering most
    of the parcel - while `zoning_envelopes` merges each entry with its own
    row, so a reader solving one candidate gets the area left under that
    column's margins rather than under the lot's governing ones. Two columns of
    one grid legitimately state different margins, which is why the per-lot
    number cannot stand in for both.

    All four arrive by LEFT JOIN, which is the whole design: a lot no building
    touches, a lot facing no street, a lot no document covers and a lot no
    assessment unit stands on are each a real answer to the question this table
    is read for, and an inner join would delete exactly those rows. The counts
    are `COALESCE`d to 0 rather than left NULL - "no building stands here" is a
    measurement, not a gap - while the frontage and assessment *measures* stay
    NULL, because a lot with no frontage row was not measured at 0 m, it was
    not measured at all, and a lane carrying no assessed property is not a lane
    worth nothing.

    **Four more inputs arrive from the caller rather than from a rag table**,
    because none of them is loaded into Postgres at all. ``zoning_envelopes``
    is `silver/lot_zoning_envelopes` - `(lot_number, entry)` pairs, several per
    lot - staged into a temp table and aggregated into `zoning_envelopes` by
    the same kind of LEFT JOIN as the three above, so a lot no readable grid
    reaches keeps its row and reports zero. ``vacancy_rates`` and
    ``average_rents`` are `silver/vacancy_rates` and `silver/average_rents`,
    one object each for the whole borough: CMHC surveys neighborhoods and
    publishes no geometry, so there is nothing per-lot about them and they are
    written identically onto every row of the partition. That denormalisation
    is what replaced `lots_with_vacancy_rates`, which used to pivot the same
    grid onto the cadastre one layer earlier - where it rode through
    `rag.lots.attributes` and both spatial joins without anything reading it.

    ``construction_costs`` is the fourth and takes the same shape for a
    stronger version of the same reason: the Altus cost guide prices nine
    Canadian markets and knows nothing about boroughs, let alone parcels, so
    one Montreal object is written onto every row of every partition. It is
    what makes a profile row answer "what is this parcel worth building"
    without a second read - a rent on one side, a dollar per square foot and a
    dollar per stall on the other.

    `overall_vacancy_rate_pct`, `overall_average_rent_cad` and the six rate
    columns are read back out of those objects in SQL rather than passed
    beside them, so a flattened column and the jsonb it was flattened from
    cannot disagree.

    **`has_building` is not the negation of "vacant".** It is `num_buildings >
    0`, the plain reading of "is there a building on this parcel", and a lot
    carrying one 12 m2 shed satisfies it. Whether that lot is *usably* empty
    depends on ``max_built_area_m2``, which is a judgement rather than a
    property of the data, so it lives in `category` instead:

    * `no_building`     - nothing intersects the lot at all.
    * `shed_only`       - something does, the built area here is under the
                          threshold, and every building overlapping it is
                          itself small enough to be a shed.
    * `building_sliver` - something does, the built area here is still under
                          the threshold, but the building it belongs to is
                          large. A corner of the neighbour's triplex crossing
                          the cadastral line: empty in substance, but calling
                          it a shed would be wrong, and it is as often a
                          footprint/cadastre alignment artifact as a real
                          encroachment.
    * `built`           - more than the threshold stands on it.

    The area compared is the *clipped* `intersection_area_m2` summed over the
    lot, not the whole area of the buildings that overlap it: a warehouse
    straddling the boundary contributes only the slice actually inside, which
    is what "how much of this lot is built on" means.

    Assumes `compute_intersections`, `compute_lot_features` and
    `compute_lot_frontage` have already run for this partition. A lot looks
    unbuilt and unfronted either way, so those are dependencies on the assets
    and not something the SQL can check for itself - which is why the guard
    below only checks that the relations *exist*.
    """
    threshold = float(max_built_area_m2)
    if threshold < 0:
        raise ValueError(
            f"max_built_area_m2 must not be negative, got {max_built_area_m2!r}"
        )

    _psycopg()  # raises PostgresUnavailable with a clear message if missing
    from psycopg.types.json import Jsonb

    cursor = connection.cursor()
    _require_relations(cursor, _LOT_PROFILE_RELATIONS)

    # Ahead of the write so a malformed envelope costs nothing: the partition
    # is only rebuilt once there is something to rebuild it from.
    num_envelopes_staged = _stage_lot_envelopes(cursor, zoning_envelopes)

    result = warehouse.upsert_select(
        cursor,
        "lot_profiles",
        (
            "scrape_date", "neighborhood", "lot_number", "lot_uid", "lot_area_m2",
            "has_building", "num_buildings", "built_area_m2", "built_pct_of_lot",
            "largest_building_area_m2", "category", "max_built_area_m2",
            "num_frontages", "total_frontage_m",
            "primary_frontage_m", "primary_street_name", "primary_cote_rue_id",
            "secondary_frontage_m", "secondary_street_name", "secondary_cote_rue_id",
            "frontage_buffer_m",
            "num_documents", "doc_id", "doc_url", "doc_title", "doc_source_table",
            "doc_pct_of_lot", "documents",
            "num_zoning_envelopes", "zoning_envelopes",
            "buildable_area_m2", "buildable_pct_of_lot", "footprint_cap_m2",
            "footprint_cap_binding", "side_setback_rule",
            "num_assessment_units", "num_shared_units", "num_units_by_point",
            "total_assessed_value", "total_assessed_value_apportioned",
            "roll_year",
            "num_dwellings", "floor_area_m2", "residential_floor_area_m2",
            "commercial_floor_area_m2", "industrial_floor_area_m2",
            "retail_floor_area_m2", "office_floor_area_m2",
            "retail_income_cad", "office_income_cad",
            "dominant_use_code", "year_built",
            "gross_income_cad", "net_operating_income_cad",
            "cap_rate_pct", "comparable_cap_rate_pct", "income_assumptions",
            "estimated_value_cad", "estimated_value_basis",
            "assessed_to_estimated_ratio", "num_comparables", "comparables",
            "vacancy_rates", "overall_vacancy_rate_pct",
            "average_rents", "overall_average_rent_cad",
            "construction_costs",
            "underground_stall_cost_low_cad", "underground_stall_cost_high_cad",
            "above_grade_stall_cost_low_cad", "above_grade_stall_cost_high_cad",
            "condo_cost_low_cad_sqft", "condo_cost_high_cad_sqft",
            "geom",
        ),
        """
        -- Each CTE scans its join once for the whole partition and groups it
        -- to one row per lot. Written this way rather than as three LATERALs
        -- because `rag.lot_documents` is a view over a DISTINCT across every
        -- chunk in the borough, and a lateral would risk re-running that once
        -- per lot.
        WITH built AS (
            SELECT bl.lot_uid,
                   count(*) AS num_buildings,
                   sum(bl.intersection_area_m2) AS built_area_m2,
                   max(bl.building_area_m2) AS largest_building_area_m2
              FROM silver.building_lot_intersections bl
             WHERE bl.neighborhood = %(neighborhood)s
               AND bl.scrape_date = %(scrape_date)s::date
             GROUP BY bl.lot_uid
        ),
        frontage AS (
            -- `frontage_rank` is the upstream's own answer to "which street
            -- does this lot mostly face", computed with a row_number over
            -- frontage_m DESC - so the pivot reads it rather than re-deciding
            -- it, and the two tables cannot disagree about which edge is
            -- primary. cote_rue_id only makes the array order total.
            SELECT f.lot_uid,
                   count(*) AS num_frontages,
                   sum(f.frontage_m) AS total_frontage_m,
                   max(f.buffer_m) AS frontage_buffer_m,
                   (array_agg(f.frontage_m
                        ORDER BY f.frontage_rank, f.cote_rue_id))[1] AS primary_frontage_m,
                   (array_agg(f.street_name
                        ORDER BY f.frontage_rank, f.cote_rue_id))[1] AS primary_street_name,
                   (array_agg(f.cote_rue_id
                        ORDER BY f.frontage_rank, f.cote_rue_id))[1] AS primary_cote_rue_id,
                   -- Subscript 2 of a one-element array is NULL, which is
                   -- exactly what a mid-block lot should report for its second
                   -- street edge.
                   (array_agg(f.frontage_m
                        ORDER BY f.frontage_rank, f.cote_rue_id))[2] AS secondary_frontage_m,
                   (array_agg(f.street_name
                        ORDER BY f.frontage_rank, f.cote_rue_id))[2] AS secondary_street_name,
                   (array_agg(f.cote_rue_id
                        ORDER BY f.frontage_rank, f.cote_rue_id))[2] AS secondary_cote_rue_id
              FROM silver.lot_frontage f
             WHERE f.neighborhood = %(neighborhood)s
               AND f.scrape_date = %(scrape_date)s::date
             GROUP BY f.lot_uid
        ),
        applies AS (
            -- One row per (lot, document). `rag.lot_documents` is one row per
            -- (lot, feature, document), so a lot split across two zones of the
            -- same layer that both cite the same grid arrives twice; keeping
            -- the feature that covers most of the lot is what makes
            -- `num_documents` a count of documents rather than of overlaps.
            SELECT DISTINCT ON (ld.lot_uid, ld.source_table, ld.doc_id)
                   ld.lot_uid, ld.source_table, ld.doc_id, ld.url, ld.title,
                   ld.feature_id, ld.pct_of_lot
              FROM rag.lot_documents ld
             WHERE ld.neighborhood = %(neighborhood)s
               AND ld.scrape_date = %(scrape_date)s::date
             ORDER BY ld.lot_uid, ld.source_table, ld.doc_id, ld.pct_of_lot DESC
        ),
        docs AS (
            -- Ranked across every layer, not within one: the flattened doc_*
            -- columns answer "the PDF for this parcel", and the zoning grid
            -- covering the whole lot is that answer whichever layer it came
            -- from. `rag.lot_documents.coverage_rank` stays per source_table
            -- and is the column to use when the question names a layer.
            --
            -- No literal per-cent sign anywhere in this statement, here or in
            -- a comment: psycopg reads one as the start of a placeholder, so
            -- writing "100 per cent of" in words is the difference between
            -- this query running and it failing outright at execution time.
            -- test_postgis_lot_profiles.py guards it.
            SELECT a.lot_uid,
                   count(*) AS num_documents,
                   (array_agg(a.doc_id
                        ORDER BY a.pct_of_lot DESC, a.source_table, a.doc_id))[1] AS doc_id,
                   (array_agg(a.url
                        ORDER BY a.pct_of_lot DESC, a.source_table, a.doc_id))[1] AS doc_url,
                   (array_agg(a.title
                        ORDER BY a.pct_of_lot DESC, a.source_table, a.doc_id))[1] AS doc_title,
                   (array_agg(a.source_table
                        ORDER BY a.pct_of_lot DESC, a.source_table, a.doc_id))[1] AS doc_source_table,
                   (array_agg(a.pct_of_lot
                        ORDER BY a.pct_of_lot DESC, a.source_table, a.doc_id))[1] AS doc_pct_of_lot,
                   jsonb_agg(jsonb_build_object(
                       'source_table', a.source_table,
                       'feature_id',   a.feature_id,
                       'doc_id',       a.doc_id,
                       'url',          a.url,
                       'title',        a.title,
                       'pct_of_lot',   a.pct_of_lot
                   ) ORDER BY a.pct_of_lot DESC, a.source_table, a.doc_id) AS documents
              FROM applies a
             GROUP BY a.lot_uid
        ),
        buildable_by_column AS (
            -- `silver.lot_buildable_setbacks` at its own grain, as the object
            -- each envelope entry is merged with below. One row per (lot,
            -- zone, column), which is exactly the envelopes' grain, so this
            -- cannot fan an entry out.
            SELECT b.lot_number, b.feature_id, b.column_index,
                   jsonb_build_object(
                       'buildable_area_m2', b.buildable_area_m2,
                       'buildable_pct_of_lot', b.buildable_pct_of_lot,
                       'coverage_cap_m2', b.coverage_cap_m2,
                       'footprint_cap_m2', b.footprint_cap_m2,
                       'footprint_cap_binding', b.footprint_cap_binding,
                       'side_setback_rule', b.side_setback_rule,
                       'front_setback_m', b.front_setback_m,
                       'secondary_front_setback_m', b.secondary_front_setback_m,
                       'side_setback_m', b.side_setback_m,
                       'rear_setback_m', b.rear_setback_m
                   ) AS buildable
              FROM silver.lot_buildable_setbacks b
             WHERE b.neighborhood = %(neighborhood)s
               AND b.scrape_date = %(scrape_date)s::date
        ),
        buildable_lot AS (
            -- The same table narrowed to one row per lot, for the flattened
            -- columns. The row taken is the column that governs the parcel -
            -- the one `select_residential_column` chose - and failing that the
            -- zone covering most of it, which is the same precedence
            -- `zoning_envelopes` is ordered by. feature_id and column_index
            -- only make the order total, so a re-run picks the same row twice.
            SELECT DISTINCT ON (b.lot_number)
                   b.lot_number,
                   b.buildable_area_m2,
                   b.buildable_pct_of_lot,
                   b.footprint_cap_m2,
                   b.footprint_cap_binding,
                   b.side_setback_rule
              FROM silver.lot_buildable_setbacks b
             WHERE b.neighborhood = %(neighborhood)s
               AND b.scrape_date = %(scrape_date)s::date
               AND b.lot_number IS NOT NULL
             ORDER BY b.lot_number,
                      b.governs_residential DESC NULLS LAST,
                      b.pct_of_lot DESC NULLS LAST,
                      b.feature_id, b.column_index
        ),
        envelopes AS (
            -- Keyed on lot_number, not lot_uid: see `_stage_lot_envelopes`.
            -- Ordered by the ordinal the caller staged, which is the order
            -- silver/lot_zoning_envelopes decided - the zone covering most of
            -- the lot first - rather than one re-derived from inside the jsonb.
            --
            -- Each entry is merged with its own buildable figures rather than
            -- the lot's governing ones: a reader solving one candidate needs
            -- the area left under *that* column's margins, and two columns of
            -- one grid legitimately state different ones. The join reads
            -- feature_id and column_index back out of the staged jsonb because
            -- that is where the caller put them - the staging table carries
            -- only the lot number and the order.
            --
            -- `||` on the right-hand side, so a key the buildable table
            -- supplies wins over one of the same name already in the entry.
            -- There are none today; if a margin column is ever flattened onto
            -- both, this is the copy computed from the geometry.
            SELECT e.lot_number,
                   count(*) AS num_zoning_envelopes,
                   jsonb_agg(
                       e.envelope || COALESCE(b.buildable, '{}'::jsonb)
                       ORDER BY e.ordinal
                   ) AS zoning_envelopes
              FROM {envelope_staging} e
              LEFT JOIN buildable_by_column b
                     ON b.lot_number = e.lot_number
                    AND b.feature_id = e.envelope ->> 'feature_id'
                    AND b.column_index = (e.envelope ->> 'column_index')::integer
             GROUP BY e.lot_number
        )
        SELECT
            l.scrape_date,
            l.neighborhood,
            l.lot_number,
            l.lot_uid,
            l.area_m2,
            COALESCE(built.num_buildings, 0) > 0,
            COALESCE(built.num_buildings, 0),
            COALESCE(built.built_area_m2, 0.0),
            CASE WHEN l.area_m2 > 0
                 THEN 100.0 * COALESCE(built.built_area_m2, 0.0) / l.area_m2
                 ELSE NULL
            END,
            COALESCE(built.largest_building_area_m2, 0.0),
            CASE
                WHEN built.lot_uid IS NULL THEN 'no_building'
                WHEN built.built_area_m2 > %(threshold)s THEN 'built'
                WHEN built.largest_building_area_m2 > %(threshold)s THEN 'building_sliver'
                ELSE 'shed_only'
            END,
            %(threshold)s,
            COALESCE(frontage.num_frontages, 0),
            COALESCE(frontage.total_frontage_m, 0.0),
            -- Not COALESCEd, unlike the counts above: an unmeasured edge is
            -- not a 0 m edge. See the column comments in 009_lot_profiles.sql.
            frontage.primary_frontage_m,
            frontage.primary_street_name,
            frontage.primary_cote_rue_id,
            frontage.secondary_frontage_m,
            frontage.secondary_street_name,
            frontage.secondary_cote_rue_id,
            frontage.frontage_buffer_m,
            COALESCE(docs.num_documents, 0),
            docs.doc_id,
            docs.doc_url,
            docs.doc_title,
            docs.doc_source_table,
            docs.doc_pct_of_lot,
            COALESCE(docs.documents, '[]'::jsonb),
            COALESCE(envelopes.num_zoning_envelopes, 0),
            COALESCE(envelopes.zoning_envelopes, '[]'::jsonb),
            -- Not COALESCEd, unlike the envelope count above, and the same
            -- split the frontage columns make: a lot whose zone published no
            -- readable grid - or one with no frontage row to call a front edge
            -- - had no buildable area computed, which is not a buildable area
            -- of zero. A genuine 0 does appear on parcels narrower than twice
            -- their side margin, and the two must not read alike.
            buildable.buildable_area_m2,
            buildable.buildable_pct_of_lot,
            buildable.footprint_cap_m2,
            buildable.footprint_cap_binding,
            buildable.side_setback_rule,
            -- The counts COALESCE to 0 and the two totals do not, which is
            -- the same split the frontage columns above make: a lot no
            -- assessment unit stands on carries none, and that is a
            -- measurement - but its value was not measured at zero, it was
            -- not measured at all. A lane summed to $0 would quietly drag
            -- down every average taken over the borough.
            COALESCE(assessed.num_assessment_units, 0),
            COALESCE(assessed.num_shared_units, 0),
            COALESCE(assessed.num_units_by_point, 0),
            assessed.total_assessed_value,
            assessed.total_assessed_value_apportioned,
            assessed.roll_year,
            -- silver.lot_assessment_comparables, and the same split again: the
            -- two things that are counts COALESCE, and every measure does not.
            -- A lot with no cap rate is a lane that earns nothing the roll
            -- knows about *or* a borough whose rent CMHC suppressed, and
            -- neither is a yield of zero - which would drag down every average
            -- taken over the borough exactly as a $0 lane would.
            --
            -- num_dwellings is the one count that stays NULL, because there it
            -- is a measure: a lot no assessment unit stands on has no dwelling
            -- count, while one carrying a warehouse has a count of 0, and
            -- COALESCEing the first to 0 would make the two read alike.
            comparables.num_dwellings,
            comparables.floor_area_m2,
            comparables.residential_floor_area_m2,
            comparables.commercial_floor_area_m2,
            comparables.industrial_floor_area_m2,
            comparables.retail_floor_area_m2,
            comparables.office_floor_area_m2,
            comparables.retail_income_cad,
            comparables.office_income_cad,
            comparables.dominant_use_code,
            comparables.year_built,
            comparables.gross_income_cad,
            comparables.net_operating_income_cad,
            comparables.cap_rate_pct,
            comparables.comparable_cap_rate_pct,
            -- '{}' rather than NULL on both objects, so "this partition's
            -- silver asset has not run" reads as an empty object and not as a
            -- null column - the same distinction vacancy_rates and
            -- construction_costs draw below.
            COALESCE(comparables.income_assumptions, '{}'::jsonb),
            comparables.estimated_value_cad,
            comparables.estimated_value_basis,
            comparables.assessed_to_estimated_ratio,
            COALESCE(comparables.num_comparables, 0),
            COALESCE(comparables.comparables, '{}'::jsonb),
            -- The borough's own figures, written identically onto every lot:
            -- CMHC publishes no geometry, so there is nothing to join on and
            -- nothing per-lot to say.
            %(vacancy_rates)s::jsonb,
            (%(vacancy_rates)s::jsonb ->> 'overall_vacancy_rate_pct')::double precision,
            %(average_rents)s::jsonb,
            (%(average_rents)s::jsonb ->> 'overall_average_rent_cad')::double precision,
            -- The city's figures, on every row of every borough. The guide
            -- prices nine Canadian markets and no geometry at all, so this is
            -- the same denormalisation as the two above with even less to join
            -- on. Each rate column is read back out of the object rather than
            -- passed beside it, so the column and the jsonb cannot disagree -
            -- and a key the payload never set lands NULL, which is what "the
            -- guide was not read for this partition" should look like.
            %(construction_costs)s::jsonb,
            (%(construction_costs)s::jsonb
                ->> 'underground_stall_cost_low_cad')::double precision,
            (%(construction_costs)s::jsonb
                ->> 'underground_stall_cost_high_cad')::double precision,
            (%(construction_costs)s::jsonb
                ->> 'above_grade_stall_cost_low_cad')::double precision,
            (%(construction_costs)s::jsonb
                ->> 'above_grade_stall_cost_high_cad')::double precision,
            (%(construction_costs)s::jsonb
                ->> 'condo_cost_low_cad_sqft')::double precision,
            (%(construction_costs)s::jsonb
                ->> 'condo_cost_high_cad_sqft')::double precision,
            l.geom
        FROM rag.lots l
        LEFT JOIN built ON built.lot_uid = l.lot_uid
        LEFT JOIN frontage ON frontage.lot_uid = l.lot_uid
        LEFT JOIN docs ON docs.lot_uid = l.lot_uid
        LEFT JOIN envelopes ON envelopes.lot_number = l.lot_number
        -- One row per lot by construction (DISTINCT ON), so this cannot fan
        -- the profile out - the same guarantee the assessment join below
        -- relies on, arrived at by narrowing rather than by the source's own
        -- primary key. Keyed on lot_number for the reason the envelopes are.
        LEFT JOIN buildable_lot buildable
               ON buildable.lot_number = l.lot_number
        -- No CTE, unlike the four above: silver.lot_assessed_values is
        -- already one row per lot - its primary key is (scrape_date,
        -- neighborhood, lot_number) - so there is nothing to group and this
        -- join cannot fan the row out. Keyed on lot_number for the reason the
        -- envelopes are, and one that is stronger here: that table carries no
        -- lot_uid at all, because it is written from the geoparquet tree
        -- rather than from a rag.lots load.
        LEFT JOIN silver.lot_assessed_values assessed
               ON assessed.lot_number = l.lot_number
              -- Scoped by the parameters rather than by equality with `l`,
              -- which the WHERE below already pins to the same pair. Both
              -- forms are correct, but this one prunes the partition at plan
              -- time instead of leaving it to be derived through an
              -- equivalence class - and it is how all four CTEs above scope
              -- themselves, so the whole statement reads one way.
              AND assessed.neighborhood = %(neighborhood)s
              AND assessed.scrape_date = %(scrape_date)s::date
        -- The same shape as the join above it, and for all the same reasons:
        -- silver.lot_assessment_comparables is one row per lot by its own
        -- primary key, carries no lot_uid, and is scoped by the parameters
        -- rather than by equality with `l` so the partition prunes at plan
        -- time. It reads the table above rather than recomputing it, so the
        -- two sets of columns cannot disagree about the value a rate divides.
        LEFT JOIN silver.lot_assessment_comparables comparables
               ON comparables.lot_number = l.lot_number
              AND comparables.neighborhood = %(neighborhood)s
              AND comparables.scrape_date = %(scrape_date)s::date
        WHERE l.neighborhood = %(neighborhood)s
          AND l.scrape_date = %(scrape_date)s::date
        """.replace("{envelope_staging}", _ENVELOPE_STAGING),
        {
            "neighborhood": neighborhood,
            "scrape_date": scrape_date,
            "threshold": threshold,
            "vacancy_rates": Jsonb(vacancy_rates or {}),
            "average_rents": Jsonb(average_rents or {}),
            "construction_costs": Jsonb(construction_costs or {}),
        },
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    cursor.execute(
        """
        SELECT category, count(*), COALESCE(sum(lot_area_m2), 0)
        FROM gold.lot_profiles
        WHERE neighborhood = %s AND scrape_date = %s::date
        GROUP BY category
        """,
        [neighborhood, scrape_date],
    )
    counted: dict[str, int] = {}
    area_by_category: dict[str, float] = {}
    for category, count, area in cursor.fetchall():
        counted[category] = int(count)
        area_by_category[category] = float(area)

    # One pass for every headline number the asset reports. Counting these in
    # SQL rather than over the fetched frame keeps them true even if the read
    # back below is ever narrowed.
    cursor.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE has_building),
               count(*) FILTER (WHERE num_frontages > 0),
               count(*) FILTER (WHERE secondary_frontage_m IS NOT NULL),
               count(*) FILTER (WHERE num_documents > 0),
               COALESCE(sum(num_buildings), 0),
               COALESCE(sum(lot_area_m2), 0),
               COALESCE(max(primary_frontage_m), 0),
               COALESCE(avg(primary_frontage_m) FILTER (WHERE num_frontages > 0), 0),
               count(*) FILTER (WHERE num_zoning_envelopes > 0),
               COALESCE(sum(num_zoning_envelopes), 0),
               -- The setback join, from this side. A lot with envelopes but no
               -- buildable area is one whose boundary could not be sorted, so
               -- the gap between this and the count above is the frontage gap
               -- carried forward - which is worth seeing on the gold asset and
               -- not only on the silver one.
               count(*) FILTER (WHERE buildable_area_m2 IS NOT NULL),
               count(*) FILTER (WHERE footprint_cap_binding = 'setbacks'),
               COALESCE(avg(buildable_pct_of_lot), 0),
               -- Read back rather than reported from what was handed in: what
               -- the asset says landed should be what actually did.
               max(overall_vacancy_rate_pct),
               max(overall_average_rent_cad),
               -- Identical on every row, so max() is just "the value" - the
               -- same trick the two above use, and for the same reason there
               -- is no per-lot aggregate to take.
               max(underground_stall_cost_low_cad),
               max(underground_stall_cost_high_cad),
               max(above_grade_stall_cost_low_cad),
               max(above_grade_stall_cost_high_cad),
               max(condo_cost_low_cad_sqft),
               max(condo_cost_high_cad_sqft),
               -- The assessment join, from this side. A lot with units on it
               -- but no total is impossible by construction, so counting the
               -- non-null totals counts the lots the roll actually reached.
               count(*) FILTER (WHERE total_assessed_value IS NOT NULL),
               COALESCE(sum(num_assessment_units), 0),
               -- The apportioned column and not the other one: this is a sum
               -- across lots, and the whole reason there are two totals is
               -- that only this one adds up that way. Summing
               -- total_assessed_value here would over-report the borough by
               -- every unit that spans more than one lot.
               sum(total_assessed_value_apportioned),
               -- Identical on every row that has one, so max() is just "the
               -- roll these values came from" - the same trick the borough
               -- figures above use.
               max(roll_year),
               -- The comparables join, from this side. The gap between this
               -- and num_with_assessed_value is lots the roll reached but the
               -- income could not be priced for - a borough whose rent CMHC
               -- suppressed, carrying nothing but dwellings.
               count(*) FILTER (WHERE num_comparables > 0),
               count(*) FILTER (WHERE cap_rate_pct IS NOT NULL),
               -- Median rather than avg, and for the reason the silver asset
               -- takes medians: one condominium's common-parts lot carrying
               -- 402 units would otherwise decide the borough's figure.
               percentile_cont(0.5) WITHIN GROUP (ORDER BY cap_rate_pct),
               percentile_cont(0.5) WITHIN GROUP (
                   ORDER BY assessed_to_estimated_ratio
               ),
               COALESCE(sum(net_operating_income_cad), 0)
        FROM gold.lot_profiles
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    (
        num_profiles,
        with_building,
        with_frontage,
        with_secondary,
        with_documents,
        num_buildings,
        total_lot_area_m2,
        max_primary_frontage_m,
        mean_primary_frontage_m,
        with_envelopes,
        num_envelopes,
        with_buildable,
        bound_by_setbacks,
        mean_buildable_pct,
        overall_vacancy_rate_pct,
        overall_average_rent_cad,
        underground_stall_cost_low_cad,
        underground_stall_cost_high_cad,
        above_grade_stall_cost_low_cad,
        above_grade_stall_cost_high_cad,
        condo_cost_low_cad_sqft,
        condo_cost_high_cad_sqft,
        with_assessed_value,
        num_assessment_units,
        total_assessed_value_apportioned,
        roll_year,
        with_comparables,
        with_cap_rate,
        median_cap_rate_pct,
        median_assessed_to_estimated_ratio,
        net_operating_income_cad,
    ) = cursor.fetchone()

    # The denominator, read from rag.lots rather than from what was just
    # inserted: the two agreeing is what says every lot got a profile, and a
    # partition that was never loaded reports 0 here instead of looking empty.
    cursor.execute(
        "SELECT count(*) FROM rag.lots WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    (num_lots,) = cursor.fetchone()

    return {
        "profiles": result["upserted"],
        "pruned": result["pruned"],
        "num_lots": int(num_lots),
        "num_profiles": int(num_profiles),
        "by_category": {name: counted.get(name, 0) for name in LOT_CATEGORIES},
        "area_by_category": {
            name: area_by_category.get(name, 0.0) for name in LOT_CATEGORIES
        },
        "num_with_building": int(with_building),
        "num_without_building": int(num_profiles) - int(with_building),
        "num_with_frontage": int(with_frontage),
        "num_with_secondary_frontage": int(with_secondary),
        "num_with_documents": int(with_documents),
        # Staged versus landed: they differ when the envelope file names a lot
        # this partition's cadastre does not have, which is what a stale
        # silver/lot_zoning_envelopes looks like from here.
        "num_envelopes_staged": int(num_envelopes_staged),
        "num_zoning_envelopes": int(num_envelopes),
        "num_with_zoning_envelopes": int(with_envelopes),
        "num_with_buildable_area": int(with_buildable),
        # Of the lots that got one, how many are stopped by their margins
        # rather than by *Taux d'implantation*. The number that says which norm
        # actually shapes this borough.
        "num_bound_by_setbacks": int(bound_by_setbacks),
        "mean_buildable_pct_of_lot": float(mean_buildable_pct),
        "has_vacancy_rates": bool(vacancy_rates),
        "has_average_rents": bool(average_rents),
        "overall_vacancy_rate_pct": (
            None if overall_vacancy_rate_pct is None
            else float(overall_vacancy_rate_pct)
        ),
        "overall_average_rent_cad": (
            None if overall_average_rent_cad is None
            else float(overall_average_rent_cad)
        ),
        "has_construction_costs": bool(construction_costs),
        # Read back the same way, so what the asset reports is what the column
        # holds rather than what the payload claimed. A rate the guide does not
        # publish - or a partition whose bronze snapshot was never read - comes
        # back None rather than 0.
        **{
            name: (None if value is None else float(value))
            for name, value in (
                ("underground_stall_cost_low_cad", underground_stall_cost_low_cad),
                ("underground_stall_cost_high_cad", underground_stall_cost_high_cad),
                ("above_grade_stall_cost_low_cad", above_grade_stall_cost_low_cad),
                ("above_grade_stall_cost_high_cad", above_grade_stall_cost_high_cad),
                ("condo_cost_low_cad_sqft", condo_cost_low_cad_sqft),
                ("condo_cost_high_cad_sqft", condo_cost_high_cad_sqft),
            )
        },
        "num_with_assessed_value": int(with_assessed_value),
        "num_assessment_units": int(num_assessment_units),
        # None rather than 0.0 when the roll reached no lot in the partition:
        # the same distinction the column itself draws, kept all the way up to
        # the asset's metadata so "not joined yet" cannot read as "worth
        # nothing".
        "total_assessed_value_apportioned": (
            None if total_assessed_value_apportioned is None
            else float(total_assessed_value_apportioned)
        ),
        "roll_year": None if roll_year is None else int(roll_year),
        # The comparables join. `num_with_cap_rate` under `num_with_comparables`
        # is the ordinary case and not a fault: a lot gets neighbours from its
        # size and its ground alone, and needs a priced income to get a rate.
        "num_with_comparables": int(with_comparables),
        "num_with_cap_rate": int(with_cap_rate),
        "median_cap_rate_pct": (
            None if median_cap_rate_pct is None else float(median_cap_rate_pct)
        ),
        # Under 1 means the borough's roll sits below what its own comparables
        # imply, which for a triennial roll in a rising market is what it
        # should look like.
        "median_assessed_to_estimated_ratio": (
            None
            if median_assessed_to_estimated_ratio is None
            else float(median_assessed_to_estimated_ratio)
        ),
        "net_operating_income_cad": float(net_operating_income_cad),
        "num_buildings": int(num_buildings),
        "total_lot_area_m2": float(total_lot_area_m2),
        "max_primary_frontage_m": float(max_primary_frontage_m),
        "mean_primary_frontage_m": float(mean_primary_frontage_m),
    }


def _require_relations(
    cursor: Any, relations: tuple[tuple[str, str], ...]
) -> None:
    """Raise `MissingRelation` naming every relation not yet created.

    Every one of them belongs to hbu_infra, so the message that helps names the
    file to apply. Reported together rather than one at a time: on a fresh
    database several are missing at once, and finding that out one failed run
    per file is three runs too many.
    """
    missing: list[str] = []
    for name, source in relations:
        # to_regclass rather than a catalog lookup: it resolves a view and a
        # table alike, and returns NULL instead of raising for a name that is
        # not there, which is the whole point of asking.
        cursor.execute("SELECT to_regclass(%s)", [name])
        (resolved,) = cursor.fetchone()
        if resolved is None:
            missing.append(f"{name} ({source})")
    if missing:
        raise MissingRelation(
            "hbu_infra has not created: "
            + ", ".join(missing)
            + " - apply the file(s) above with `./scripts/db.py init` in that "
            "repo, then re-run this partition."
        )


# --------------------------------------------------------------------------
# reading a computed partition back out, so it can be written to the tree
# --------------------------------------------------------------------------
#
# The joins above are *computed* in PostGIS, because by the time they run it
# already holds both layers loaded and GiST-indexed and `ST_Intersection` over
# that index is the tool for the job. That is a statement about where the work
# happens, not about where the result lives: the tree is the record of every
# layer, and a join that existed only in Postgres would be the one part of the
# platform a lost database could not be rebuilt from.
#
# It could not be *re*computed either. Every input is a dated snapshot of a
# live municipal or provincial service, and no later run can re-scrape
# 2026-08-18 - so "recompute it from bronze" only works for as long as bronze
# is still there and the code still agrees with it. Reading the partition back
# out and writing it as geoparquet costs one query and makes the join a file.
#
# Called inside the same transaction that computed the join, so what is written
# is what that transaction produced rather than whatever a concurrent run may
# have left behind afterwards.


#: `silver.building_lot_intersections`, minus its geometry.
#:
#: No `building_lot_uid`: the surrogate went with the move to a partitioned
#: table, where a primary key has to contain the partition keys and a serial
#: nothing cites was the wrong half to keep. `lot_number` is a column of the
#: table now rather than something joined in from `rag.lots` - the `*_uid`
#: columns are bigserials a reload mints again, so on its own a row would carry
#: no key that survives one, and the parquet has carried the number for exactly
#: that reason since before the table did.
_BUILDING_LOT_COLUMNS = (
    "building_uid",
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "building_area_m2",
    "intersection_area_m2",
    "pct_of_building",
)

#: `silver.lot_features`, minus its geometry. `source_table` and `feature_id`
#: are the pair `rag.chunks` cites, so the *feature* side carries a durable key
#: of its own; `lot_number` gives the lot side one, for the reason
#: `_BUILDING_LOT_COLUMNS` gives - and `lot_zoning_envelopes` joins on exactly
#: that.
_LOT_FEATURE_COLUMNS = (
    "lot_uid",
    "lot_number",
    "feature_uid",
    "source_table",
    "feature_id",
    "neighborhood",
    "scrape_date",
    "lot_area_m2",
    "overlap_area_m2",
    "pct_of_lot",
)

#: `silver.lot_frontage`, minus its geometry. Same reading as the two above:
#: `cote_rue_id` is the street side's own durable key and `lot_number` the
#: lot's, while `lot_uid` is a bigserial worth keeping only for a join back
#: inside the same partition. There is no `street_uid`: the street side's
#: surrogate went with the move to a partitioned
#: `silver.neighborhood_streets`, where a primary key has to contain the
#: partition keys - and `cote_rue_id` was always the key that meant anything.
_LOT_FRONTAGE_COLUMNS = (
    "lot_uid",
    "lot_number",
    "cote_rue_id",
    "street_name",
    "neighborhood",
    "scrape_date",
    "buffer_m",
    "frontage_m",
    "lot_perimeter_m",
    "pct_of_perimeter",
    "frontage_rank",
)

#: `silver.lot_buildable_setbacks`, minus its geometry, in the order a reader
#: scans them: which lot and which column, how the boundary sorted, what was
#: taken off it, and what was left.
#:
#: `lot_number`, `feature_id` and `column_index` are the trio that survives a
#: reload - the same reasoning `_LOT_FRONTAGE_COLUMNS` gives - while `lot_uid`
#: is a bigserial worth keeping only for a join back inside one partition.
_LOT_BUILDABLE_COLUMNS = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "feature_id",
    "column_index",
    "source_table",
    "lot_area_m2",
    "front_edge_m",
    "secondary_front_edge_m",
    "side_edge_m",
    "rear_edge_m",
    "implantation_mode",
    "side_setback_rule",
    "side_margin_min_m",
    "front_setback_m",
    "secondary_front_setback_m",
    "side_setback_m",
    "rear_setback_m",
    "buildable_area_m2",
    "buildable_pct_of_lot",
    "coverage_cap_m2",
    "footprint_cap_m2",
    "footprint_cap_binding",
    "pct_of_lot",
    "governs_residential",
    "solver_ready",
    "max_sin",
    "segment_m",
    "edge_tolerance_m",
)

#: `gold.lot_profiles`, minus its geometry, in the order a reader scans them:
#: which lot, what stands on it, what it faces, what governs it.
#:
#: The four jsonb columns are selected as ``jsonb::text`` rather than as jsonb,
#: so the parquet carries a JSON *string* while Postgres keeps real jsonb to
#: query. The alternative - letting pyarrow infer a nested type from the
#: decoded lists - types the column from whatever that partition happens to
#: hold: a borough where no lot has a document infers `list<null>` and one
#: where they do infers `list<struct<...>>`, which is two files with different
#: schemas for the same asset. `zoning_envelopes` would be worse still, since
#: its entries carry every norm a grid states and a borough whose grids all
#: failed to parse would infer a different struct from one whose grids read
#: cleanly. The tree's rule against that is why `scrape_date` is re-selected as
#: text in `_fetch_partition`, and it applies here for the same reason.
#: `json.loads` on read is the cost.
_LOT_PROFILE_COLUMNS = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "lot_area_m2",
    "has_building",
    "num_buildings",
    "built_area_m2",
    "built_pct_of_lot",
    "largest_building_area_m2",
    "category",
    "max_built_area_m2",
    "num_frontages",
    "total_frontage_m",
    "primary_frontage_m",
    "primary_street_name",
    "primary_cote_rue_id",
    "secondary_frontage_m",
    "secondary_street_name",
    "secondary_cote_rue_id",
    "frontage_buffer_m",
    "num_documents",
    "doc_id",
    "doc_url",
    "doc_title",
    "doc_source_table",
    "doc_pct_of_lot",
    "documents::text AS documents",
    "num_zoning_envelopes",
    "zoning_envelopes::text AS zoning_envelopes",
    "buildable_area_m2",
    "buildable_pct_of_lot",
    "footprint_cap_m2",
    "footprint_cap_binding",
    "side_setback_rule",
    "num_assessment_units",
    "num_shared_units",
    "num_units_by_point",
    # Cast on the way out, unlike every other column here. The table stores
    # these as `numeric` - dollars summed over hundreds of rows, and a
    # borough's roll runs to $27 billion - and psycopg hands a numeric back as
    # a `Decimal`, which lands in the parquet as an arrow decimal128 where the
    # rest of the tree has floats. silver/lot_assessed_values writes the same
    # two columns as Float64 for the same reason, so the cast is what keeps the
    # gold file readable against the silver one it came from.
    "total_assessed_value::double precision AS total_assessed_value",
    (
        "total_assessed_value_apportioned::double precision"
        " AS total_assessed_value_apportioned"
    ),
    "roll_year",
    "num_dwellings",
    "floor_area_m2",
    "residential_floor_area_m2",
    "commercial_floor_area_m2",
    "industrial_floor_area_m2",
    "retail_floor_area_m2",
    "office_floor_area_m2",
    "retail_income_cad",
    "office_income_cad",
    "dominant_use_code",
    "year_built",
    "gross_income_cad",
    "net_operating_income_cad",
    "cap_rate_pct",
    "comparable_cap_rate_pct",
    "income_assumptions::text AS income_assumptions",
    # Cast for the reason the two totals above are: the column is `numeric`,
    # because it is dollars, and psycopg hands one back as a `Decimal` that
    # lands in the parquet as an arrow decimal128 where the rest of the tree
    # has floats. silver/lot_assessment_comparables writes it as a float.
    "estimated_value_cad::double precision AS estimated_value_cad",
    "estimated_value_basis",
    "assessed_to_estimated_ratio",
    "num_comparables",
    "comparables::text AS comparables",
    "vacancy_rates::text AS vacancy_rates",
    "overall_vacancy_rate_pct",
    "average_rents::text AS average_rents",
    "overall_average_rent_cad",
    "construction_costs::text AS construction_costs",
    "underground_stall_cost_low_cad",
    "underground_stall_cost_high_cad",
    "above_grade_stall_cost_low_cad",
    "above_grade_stall_cost_high_cad",
    "condo_cost_low_cad_sqft",
    "condo_cost_high_cad_sqft",
)


def fetch_building_lots(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `silver.building_lot_intersections` rows, as a frame.

    No join to `rag.lots` any more: the lot number is the table's own column
    since the move to `silver`, which is one less relation this read depends
    on still holding the partition it is about.
    """
    return _fetch_partition(
        connection,
        _BUILDING_LOT_COLUMNS,
        """
        FROM silver.building_lot_intersections
        WHERE neighborhood = %s AND scrape_date = %s::date
        ORDER BY building_uid, lot_uid
        """,
        [neighborhood, scrape_date],
    )


def fetch_lot_features(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `silver.lot_features` rows, as a GeoDataFrame."""
    return _fetch_partition(
        connection,
        _LOT_FEATURE_COLUMNS,
        """
        FROM silver.lot_features
        WHERE neighborhood = %s AND scrape_date = %s::date
        ORDER BY lot_uid, source_table, feature_id
        """,
        [neighborhood, scrape_date],
    )


def fetch_lot_frontage(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `silver.lot_frontage` rows, longest frontage first.

    Ordered in SQL rather than left to the reader, so the parquet itself
    answers "which lots have the most street" by being read from the top.
    """
    return _fetch_partition(
        connection,
        _LOT_FRONTAGE_COLUMNS,
        """
        FROM silver.lot_frontage
        WHERE neighborhood = %s AND scrape_date = %s::date
        ORDER BY frontage_m DESC, lot_number, cote_rue_id
        """,
        [neighborhood, scrape_date],
    )


def fetch_lot_buildable_setbacks(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `silver.lot_buildable_setbacks` rows, roomiest first.

    Ordered by what is left of the lot rather than by the cadastre's own
    numbering, the same choice `fetch_lot_frontage` makes and for the same
    reason: this table is read for the top of that list - the parcels with the
    most room to build on - and `gold.lot_profiles` is what a named lot is
    looked up in.
    """
    return _fetch_partition(
        connection,
        _LOT_BUILDABLE_COLUMNS,
        """
        FROM silver.lot_buildable_setbacks
        WHERE neighborhood = %s AND scrape_date = %s::date
        ORDER BY footprint_cap_m2 DESC, lot_number, feature_id, column_index
        """,
        [neighborhood, scrape_date],
    )


def fetch_lot_profiles(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `gold.lot_profiles` rows, as a GeoDataFrame.

    Ordered by lot number rather than by any of the measures: this one is the
    borough's whole inventory, so the order a reader wants is the cadastre's
    own. `silver.lot_frontage` sorts by frontage because it is read for the top of
    that list; this is read for a named parcel.
    """
    return _fetch_partition(
        connection,
        _LOT_PROFILE_COLUMNS,
        """
        FROM gold.lot_profiles
        WHERE neighborhood = %s AND scrape_date = %s::date
        ORDER BY lot_number
        """,
        [neighborhood, scrape_date],
    )


def _fetch_partition(
    connection: "Connection",
    columns: tuple[str, ...],
    from_clause: str,
    params: list[Any],
    *,
    date_column: str = "scrape_date",
    geometry_column: str = "geom",
) -> gpd.GeoDataFrame:
    """SELECT ``columns`` over ``from_clause`` and build a GeoDataFrame.

    ``columns`` may be table-qualified (``bl.lot_uid``) or an expression with
    an alias (``documents::text AS documents``); the frame is labelled with the
    bare names, so what lands in the parquet carries neither the SQL alias it
    happened to be selected through nor the cast it needed.

    `scrape_date` is re-selected as text and overwrites the `date` column,
    because the rest of the tree writes it as a ``YYYY-MM-DD`` string - a
    partition that types it differently from every other file is a join that
    silently matches nothing.
    """
    import shapely.wkb

    labels = [_selected_as(column) for column in columns]
    statement = (
        f"SELECT {', '.join(columns)}, "
        f"{date_column}::text, ST_AsBinary({geometry_column}) "
        f"{from_clause}"
    )
    cursor = connection.cursor()
    cursor.execute(statement, params)
    rows = cursor.fetchall()

    frame = pd.DataFrame(rows, columns=[*labels, "scrape_date_text", "geom_wkb"])
    geometries = [
        shapely.wkb.loads(bytes(value)) if value is not None else None
        for value in frame.pop("geom_wkb")
    ]
    frame["scrape_date"] = frame.pop("scrape_date_text")
    return gpd.GeoDataFrame(
        frame,
        geometry=gpd.GeoSeries(geometries, index=frame.index, crs="EPSG:4326"),
        crs="EPSG:4326",
    )


def _selected_as(column: str) -> str:
    """The name a selected expression should land in the frame under.

    Two forms reach this. A table-qualified column (``bl.lot_uid``) keeps its
    last segment; an expression that had to be aliased
    (``documents::text AS documents``) keeps the alias. Either way the parquet
    gets the bare column name rather than the SQL it was selected through.
    """
    expression, _, alias = column.rpartition(" AS ")
    return (alias if expression else column).rpartition(".")[2]


def _as_text(value: Any) -> str | None:
    """A staging cell as text, with a missing value left missing.

    `str(nan)` is `"nan"`, which would land in `rag.lots.lot_number` as a lot
    numbered "nan" rather than as the absent number it is - and in a NOT NULL
    key column, as a row that looks loaded and joins to nothing.
    `urban_rag.warehouse._as_text` is the same rule for the silver and gold
    write path, which does not share this one's binary COPY.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)


def _as_date(value: str | date) -> date:
    """A partition's scrape date as a `datetime.date`.

    Every caller in this module types `scrape_date` as `str`, because that is
    what a partition key is. The staging tables type the column as `date`, and
    the COPYs that fill them run `FORMAT BINARY` - where psycopg dumps each
    value by its Python type rather than letting Postgres parse a literal. A
    `str` there reaches `DateBinaryDumper` and fails with `descriptor
    'toordinal' ... doesn't apply to a 'str' object`, naming neither the column
    nor the partition. Text COPY would have parsed it; binary will not, so the
    conversion has to happen here.
    """
    return date.fromisoformat(value) if isinstance(value, str) else value


def _replace_partition(
    connection: "Connection",
    table: str,
    frame: gpd.GeoDataFrame,
    *,
    neighborhood: str,
    scrape_date: str,
    natural_key_column: str | None,
    natural_key_target: str | None,
) -> int:
    """Delete ``table``'s rows for this partition, then COPY ``frame`` in.

    The loader for the `rag` working set - `rag.lots` and `rag.buildings` -
    and nothing else. The silver and gold tables are written by
    `urban_rag.warehouse` instead, which upserts rather than replaces; these
    two keep the older shape on purpose, because they are not published
    datasets but the inputs the joins below are computed over, rebuilt from
    bronze on every run of the partition.

    Geometry travels as plain WKB bytes rather than through a registered
    PostGIS type - psycopg has no built-in adapter for `geometry`, and
    `ST_GeomFromWKB`/`ST_Multi` in the final INSERT is simpler than teaching it
    one. A staging temp table is the landing spot because a column typed
    `geometry(MultiPolygon, 4326)` rejects a bare Polygon by typmod; `ST_Multi`
    in the INSERT is what normalizes that, not the COPY itself.

    ``natural_key_column``/``natural_key_target`` name the frame column that
    is a real key and the column it lands in - `NO_LOT` into `lot_number` for
    the cadastre. Given, a repeated key inside one load is resolved with `ON
    CONFLICT ... DO NOTHING`; omitted, as for BDOI's buildings, there is no key
    to conflict on and every row is simply inserted.
    """
    if (natural_key_column is None) != (natural_key_target is None):
        raise ValueError(
            "natural_key_column and natural_key_target must be given together"
        )

    cursor = connection.cursor()
    cursor.execute(
        f"DELETE FROM {table} WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    if frame.empty:
        return 0

    _psycopg()  # raises PostgresUnavailable with a clear message if missing
    from psycopg.types.json import Jsonb
    import shapely.wkb

    staging = f"{table.replace('.', '_')}_load"
    key_ddl = f"{natural_key_target} text, " if natural_key_target else ""
    cursor.execute(
        f"CREATE TEMP TABLE {staging} "
        f"({key_ddl}neighborhood text, scrape_date date, "
        "attributes jsonb, geom bytea) ON COMMIT DROP"
    )

    geometry_name = frame.geometry.name
    exclude = set(_ALWAYS_EXCLUDED) | {geometry_name}
    if natural_key_column:
        exclude.add(natural_key_column)
    attribute_columns = [c for c in frame.columns if c not in exclude]
    attrs = (
        json.loads(frame[attribute_columns].to_json(orient="records", date_format="iso"))
        if attribute_columns
        else [{}] * len(frame)
    )

    key_targets = [natural_key_target] if natural_key_target else []
    columns = key_targets + ["neighborhood", "scrape_date", "attributes", "geom"]
    types = ["text"] * len(key_targets) + ["text", "date", "jsonb", "bytea"]

    inserted = 0
    partition_date = _as_date(scrape_date)
    statement = f"COPY {staging} ({', '.join(columns)}) FROM STDIN (FORMAT BINARY)"
    with cursor.copy(statement) as copy:
        copy.set_types(types)
        for index, row in enumerate(frame.itertuples(index=False)):
            geometry = getattr(row, geometry_name)
            if geometry is None or geometry.is_empty:
                continue
            values: list[Any] = (
                [_as_text(getattr(row, natural_key_column))]
                if natural_key_column
                else []
            )
            values += [
                neighborhood,
                partition_date,
                Jsonb(attrs[index]),
                shapely.wkb.dumps(geometry),
            ]
            copy.write_row(values)
            inserted += 1

    key_list = f"{natural_key_target}, " if natural_key_target else ""
    conflict = (
        f"ON CONFLICT ({natural_key_target}, scrape_date) DO NOTHING"
        if natural_key_target
        else ""
    )
    cursor.execute(
        f"""
        INSERT INTO {table} (
            {key_list}neighborhood, scrape_date, area_m2, attributes, geom
        )
        SELECT
            {key_list}neighborhood,
            scrape_date,
            ST_Area(geography(ST_Multi(ST_SetSRID(ST_GeomFromWKB(geom), 4326)))),
            attributes,
            ST_Multi(ST_SetSRID(ST_GeomFromWKB(geom), 4326))
        FROM {staging}
        {conflict}
        """
    )
    return inserted
