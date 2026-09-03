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
much of this lot faces a street". Its right-hand side is the cadastre itself:
in the renewed cadastre a roadway is a lot with a lot number, so a parcel's
frontage is the boundary it *shares* with one of those - an exact shared edge,
with no buffer, no tolerance and nothing to tune. `silver.neighborhood_streets`,
the city's *geobase double*, is what says which parcels are the roadway, since
the assessment roll does not reach Montreal's street lots at all; it then names
the street, which a cadastral parcel cannot do for itself. The measure is still
taken on `ST_Boundary` rather than on the lot - a lot is a polygon, and
`ST_Length` of a polygon is zero, so intersecting the two solids would report
nothing. See `compute_lot_frontage`, and `DEFAULT_ROAD_LOT_MIN_STREET_M` for
why the roll cannot identify a road lot here. hbu_infra's
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
from typing import TYPE_CHECKING, Any, Callable, Iterator, Sequence

import geopandas as gpd
import pandas as pd

from urban_rag import tile_grid, warehouse
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


def analyze(connection: "Connection", table: str) -> None:
    """Refresh the planner's statistics for a `rag` working-set table.

    Called at the end of each of the three loads below, and the reason is a
    reader rather than a writer. `_replace_partition` deletes a borough's rows
    and COPYs them back, which leaves the statistics describing the partition
    as it was before the load. Autovacuum notices in its own time, and the
    window between a load finishing and the stats catching up is exactly when
    somebody opens the map to look at what was loaded.

    What goes wrong in that window does not look like a statistics problem.
    hbu_rag_map draws the cadastre and the footprints as vector tiles - one
    query per tile, several dozen per pan, each a GiST lookup narrowed by
    `(neighborhood, scrape_date)`. Handed stale counts the planner
    mis-estimates that filter and drops the index scan for a sequential one,
    so every tile becomes a scan of the borough. The map does not fail; it
    stops answering, on the partition that was most recently loaded.

    ANALYZE is permitted inside a transaction - unlike VACUUM - and takes a
    ShareUpdateExclusiveLock, which blocks other maintenance and no reader. A
    load that rolls back rolls its statistics back with it.

    `urban_rag.warehouse._analyze` is the same step for the silver and gold
    tables, where it runs against the leaf rather than the whole table because
    those are partitioned and these three are not.
    """
    connection.cursor().execute(f"ANALYZE {table}")


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
        if shape.geom_type == "MultiLineString"
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
        analyze(connection, "rag.features")
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
    analyze(connection, "rag.features")
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


#: How much geobase double street line has to run *inside* a parcel before
#: that parcel is taken to be the roadway itself, in metres.
#:
#: This is what identifies a road lot, and identifying one is the whole of the
#: measure below: in Quebec's renewed cadastre a street is a lot like any other
#: - Infolot draws avenue Chabot as parcels 3 946 199, 3 946 200 and their
#: neighbours, some 13.5 m wide - so a lot's frontage is the length of boundary
#: it shares with one of them, and nothing has to be buffered, chopped or
#: matched by angle to find it.
#:
#: **The assessment roll cannot do this identifying, though it is the obvious
#: place to look.** The roll files the public way under CUBF 45xx, and on the
#: 2026 roll Montreal states 859 such units among 437,192 - 55 autoroutes, two
#: boulevards, two arterials, 152 local streets, 557 lanes and paths, 91
#: others. The city's own street parcels are not among them: of the fourteen
#: road lots in the Villeray fixture, **none appears in `b05v_lot_cadst` at
#: all**, because Montreal does not enter its roadways on the roll. The roll
#: reaching a parcel is a fact about tenure; it is not a map of the street
#: network, and `hbu.road_parcel_lots` says the same thing from the other side.
#:
#: So the street network identifies the street, which is what it is for. A
#: geobase double side is drawn along the roadway, so it runs *within* the
#: parcel that is the roadway and enters no other. Measured over the fixture,
#: the separation is total rather than marginal: the fourteen road lots carry
#: between 105 m and 325 m of street line each, every other parcel carries
#: none, and the rule picks out all fourteen with no false positive and no
#: false negative. One metre is therefore a guard against a side clipping the
#: corner of an ordinary parcel where the two publishers disagree, not a
#: threshold anything real sits near.
#:
#: It also settles the lanes without a second rule. The geobase double draws
#: the public roadway and no ruelle, so the borough's lane parcels - three of
#: them in the fixture, 3.6 to 4.5 m wide - are not road lots, and a lot
#: backing onto one gets no frontage from it. That is the intended reading: a
#: lane is access, not street edge.
DEFAULT_ROAD_LOT_MIN_STREET_M = 1.0

#: Where the frontage geometry is measured. NAD83 / MTM zone 8 is the projected
#: system the island is surveyed in, the same one `street_assets.METRIC_CRS`
#: measures street length in. A planar CRS rather than `geography`, which has
#: no index-backed nearest-neighbour operator for the naming step below.
FRONTAGE_METRIC_SRID = 32188

#: What is written to `silver.lot_frontage.buffer_m` now that there is no
#: buffer. Zero is the truthful reading of that column - "how far from the
#: street a lot boundary counted as facing it" is nothing at all, because the
#: boundary has to *be* the road lot's edge - and it is also the marker saying
#: which method produced a row: a partition whose rows say 3.0 or 10.0 was
#: measured by the old buffer-and-angle assignment against a linestring, and is
#: reporting a different quantity from one whose rows say 0.
#:
#: **Zero is safe here because the cadastre is a topological survey, not
#: because the tolerance was never needed.** Two abutting parcels do not merely
#: come close: they reference the same points. Lot 3 790 556 and lot 3 946 200,
#: the strip of Chabot it fronts on, share the two endpoints of their common
#: edge as *bitwise-identical* coordinates in the stored EPSG:4326, and
#: `ST_Transform` carries that through to EPSG:32188 unchanged, since the same
#: input coordinate maps to the same output coordinate for both parcels.
#: Measured across the fixture, all 150 ordinary parcels sit at a distance of
#: exactly 0.0 from their nearest road lot - not a small distance, zero - with
#: nothing in the millimetre, centimetre or decimetre bands where a sliver
#: would show.
#:
#: **A positive tolerance would be a tax on every lot to insure against that.**
#: It recovers no real frontage and adds twice itself to every parcel in the
#: borough, because the shared edge grows by the tolerance at each end:
#:
#:     tolerance   subject lot   added to every lot   corner-touch parcel
#:          0 m       15.2404 m                    -                   0 m
#:          1 mm      15.2424 m               +0.002 m             0.002 m
#:          1 cm      15.2604 m               +0.020 m             0.020 m
#:          5 cm      15.3404 m               +0.100 m             0.100 m
#:
#: The last column is the other cost: lot 3 790 483 touches a road lot at a
#: single *point* and has no street edge at all, and every tolerance invents
#: exactly twice itself of frontage for it. So the only parcel a tolerance
#: "recovers" is one that should not have a row.
#:
#: If a future partition ever does arrive with slivers, the fix is `ST_Snap`
#: rather than a buffer - it pulls near-coincident vertices onto the reference
#: and leaves already-coincident ones alone, so it costs a clean partition
#: nothing - and `_SLIVER_GAP_M` below is what would say so first.
FRONTAGE_NO_BUFFER = 0.0

#: How far apart two parcels have to be to be a survey gap rather than a
#: neighbour, in metres. Nothing is measured with this: it is the detector for
#: the one way the exact intersection above could fail quietly.
#:
#: A parcel that abuts a road lot sits at distance exactly 0. A parcel that is
#: genuinely elsewhere sits metres away. A parcel a few millimetres off a road
#: lot is neither - it is a cadastre that has stopped being topologically
#: clean, and `ST_Intersects` would drop it from the join and leave the lot
#: with no row, indistinguishable from an interior parcel. So the run counts
#: them and hands the count back as `num_lots_near_road_without_frontage`.
#:
#: Half a metre, because the failure being watched for is survey noise and the
#: nearest genuine non-neighbour in the fixture is orders of magnitude beyond
#: it. On VSMPE's committed slice this count is **0**, which is the assertion
#: `test_no_parcel_is_a_sliver_away_from_the_street` pins.
_SLIVER_GAP_M = 0.5

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
    min_street_m: float = DEFAULT_ROAD_LOT_MIN_STREET_M,
) -> dict[str, object]:
    """(Re)compute `silver.lot_frontage` for one (neighborhood, scrape_date).

    How much street each lot faces, and which street. A lot with 30 m on a
    boulevard is a different development site from the one behind it with 6 m
    on a lane, and no publisher records it: Infolot draws the parcel, the
    geobase double draws the sides of the roadway, and the relation between the
    two is geometric.

    **The street is a lot, and that is the whole measure.** In the renewed
    cadastre a roadway is a parcel with a lot number like any other - avenue
    Chabot is 3 946 199, 3 946 200 and their neighbours - so a lot's frontage
    is the length of the boundary it *shares* with one of them:

        ST_Length(ST_Intersection(ST_Boundary(lot), ST_Boundary(road_lot)))

    taken in `FRONTAGE_METRIC_SRID`. That intersection is the shared edge
    *exactly*, because the cadastre is a topological survey: two abutting
    parcels reference the same points rather than merely coming close, and the
    two endpoints of this lot's street edge are bitwise-identical coordinates
    in both polygons. Lot 3 790 556 on avenue Chabot comes out at 15.24 m,
    which is what its front boundary measures in the polygon and twice the
    7.62 m of the single-width lots either side of it.

    So there is no tolerance, and `_SLIVER_GAP_M` is the guard on the one
    assumption that buys - see it, and `FRONTAGE_NO_BUFFER`, for the
    measurements behind both.

    **It is no longer a buffered street line.** The old measure clipped the lot
    boundary against a buffered geobase side, then chopped the boundary into
    1 m pieces, matched each to the nearest side within `buffer_m` and kept the
    ones running within 45 degrees of parallel. Every part of that was
    compensation for the street being a line rather than a polygon: a lot line
    does not sit on the roadway, so the reach had to be widened until it found
    the lots (at 3 m, 90 % of Villeray matched nothing; the default ended at
    10 m), and once widened it caught the lot's *side* boundaries too, which
    the angle test then had to throw back out. The road lot removes the whole
    apparatus - there is no reach to tune, so there is nothing for a partition
    to be sensitive to.

    A **tolerance would make it worse, not safer.** A shared edge grows by the
    tolerance at each end, so buffering the road lot by `t` adds `2t` to every
    parcel in the borough - 0.10 m each at 5 cm - while recovering no frontage
    that is really there. The only parcel it gains is lot 3 790 483, which
    touches a road lot at a single *point*, has no street edge, and is credited
    with exactly `2t` of one. Exact is both simpler and right; see
    `FRONTAGE_NO_BUFFER` for the table.

    **The geobase still names the street, and only names it.** A cadastral
    parcel carries no street name, so each shared edge is labelled with the
    nearest geobase side *that runs inside the road lot the edge came from* -
    the restriction matters, because a corner lot's two edges belong to two
    road lots, and the nearest side overall labels both with whichever street
    happens to be closer. With it, lot 3 790 549 reads 31.2 m on Jarry and
    13.1 m on Chabot; without it, both edges come back Chabot. The side sits a
    median 3.7 m from the edge it names, and naming is all it does: a label
    that lands on the wrong side of a corner costs a name, never a metre.

    **A street is several road lots, so the rows are summed per side.** The
    cadastre cuts a roadway at each intersection, so a lot running the length
    of a block can meet one street through two parcels. Those are one frontage
    and are grouped back to (lot, `cote_rue_id`), which is also this table's
    key - so `envelopes` and `lot_profiles`, which pivot on `cote_rue_id` and
    `street_name`, read exactly what they read before.

    ``frontage_rank`` is 1 for the longest frontage a lot has, which is the
    column to filter on when a question wants *the* street a lot fronts on
    rather than every street it touches. A corner lot legitimately has two.

    A road lot gets no row of its own and is not counted among the lots facing
    nothing - a street does not front on itself. A lot with no row shares no
    boundary with any road lot: a genuine interior parcel, one reached only by
    a lane, or a street snapshot that stops short of it. The caller gets the
    count and a sample; see the `lots_without_frontage` key in the returned
    dict, which `frontage_assets` surfaces as metadata.

    Assumes `load_lots` and `load_streets` have already landed this partition's
    rows - both sides are filtered to (`neighborhood`, `scrape_date`) before
    anything is measured, so a street side from another date cannot identify a
    road lot from this one. `rag.lots` is loaded by `building_lot_intersections`
    and not here: two assets loading the same table from the same file in two
    transactions is the race that asset's docstring exists to describe.
    """
    cursor = connection.cursor()
    _require_relations(cursor, _FRONTAGE_RELATIONS)
    srid = int(FRONTAGE_METRIC_SRID)

    # Both sides of the join, projected once and indexed, in temp tables rather
    # than CTEs. The projection is what every predicate below runs against, and
    # doing it inline would reproject each parcel once per candidate pair; the
    # indexes are what turn the road-lot test and the adjacency join into index
    # probes instead of scans of the borough.
    #
    # Dropped first rather than declared ON COMMIT DROP. This runs inside the
    # caller's transaction - `connect` above commits on a clean exit - so
    # ON COMMIT DROP would be the tidier declaration, and would also silently
    # destroy the tables between the CREATE and the SELECT if a caller ever
    # handed this an autocommit connection. Session scope survives that, a
    # rollback still takes them with it because DDL is transactional here, and
    # this makes a second call on one connection work either way.
    for table in ("_frontage_sides", "_frontage_lots", "_frontage_road_sides"):
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

    cursor.execute(
        f"""
        CREATE TEMP TABLE _frontage_sides AS
        SELECT cote_rue_id, street_name, ST_Transform(geom, {srid}) AS geom
        FROM silver.neighborhood_streets
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    cursor.execute("CREATE INDEX ON _frontage_sides USING gist (geom)")

    cursor.execute(
        f"""
        CREATE TEMP TABLE _frontage_lots AS
        SELECT lot_uid, lot_number, neighborhood, scrape_date,
               ST_Transform(geom, {srid}) AS geom,
               -- A property of the lot, so it is carried from here rather than
               -- recomputed per road lot the parcel happens to touch.
               ST_Perimeter(ST_Transform(geom, {srid})) AS lot_perimeter_m
        FROM rag.lots
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    cursor.execute("CREATE INDEX ON _frontage_lots USING gist (geom)")
    cursor.execute("CREATE INDEX ON _frontage_lots (lot_uid)")

    # Which parcels are the roadway, and which sides run down each of them.
    # One table for both questions because they are one question: a road lot is
    # a parcel some side runs inside, and that same side is what names the
    # edges the parcel produces. See `DEFAULT_ROAD_LOT_MIN_STREET_M` for why
    # the length test separates roads from everything else so cleanly.
    cursor.execute(
        """
        CREATE TEMP TABLE _frontage_road_sides AS
        SELECT l.lot_uid AS road_lot_uid, s.cote_rue_id, s.street_name, s.geom
        FROM _frontage_lots l
        JOIN _frontage_sides s ON s.geom && l.geom
        WHERE ST_Length(ST_Intersection(s.geom, l.geom))
              >= %(min_street_m)s::double precision
        """,
        {"min_street_m": float(min_street_m)},
    )
    cursor.execute("CREATE INDEX ON _frontage_road_sides (road_lot_uid)")
    cursor.execute("CREATE INDEX ON _frontage_road_sides USING gist (geom)")
    # Without stats the planner costs the LATERAL below against a default row
    # estimate and can still choose a scan over the index it was just handed.
    cursor.execute("ANALYZE _frontage_sides")
    cursor.execute("ANALYZE _frontage_lots")
    cursor.execute("ANALYZE _frontage_road_sides")

    cursor.execute("SELECT count(DISTINCT road_lot_uid) FROM _frontage_road_sides")
    (num_road_lots,) = cursor.fetchone()

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
        WITH roads AS (
            SELECT DISTINCT r.road_lot_uid AS lot_uid, l.geom
            FROM _frontage_road_sides r
            JOIN _frontage_lots l ON l.lot_uid = r.road_lot_uid
        ),
        -- The shared boundary itself. Two parcels that abut in this cadastre
        -- share their edge exactly, so this is the frontage with nothing
        -- approximated - and `ST_CollectionExtract(..., 2)` keeps only the
        -- linear part, which is what drops a parcel meeting a road lot at a
        -- single corner rather than crediting it with a point of street.
        edges AS (
            SELECT p.lot_uid, r.lot_uid AS road_lot_uid,
                   ST_CollectionExtract(
                       ST_Intersection(ST_Boundary(p.geom), ST_Boundary(r.geom)), 2
                   ) AS geom
            FROM _frontage_lots p
            JOIN roads r
              ON r.geom && p.geom AND ST_Intersects(p.geom, r.geom)
            -- A road lot fronts on nothing: it is the street.
            WHERE NOT EXISTS (
                SELECT 1 FROM roads self WHERE self.lot_uid = p.lot_uid
            )
        ),
        -- Named by the nearest side running inside *that* road lot, not the
        -- nearest side anywhere - see the docstring on lot 3 790 549. The
        -- restriction is also what makes this an index probe per edge.
        named AS (
            SELECT e.lot_uid, e.geom, ST_Length(e.geom) AS edge_m,
                   near.cote_rue_id, near.street_name
            FROM edges e
            CROSS JOIN LATERAL (
                SELECT s.cote_rue_id, s.street_name
                FROM _frontage_road_sides s
                WHERE s.road_lot_uid = e.road_lot_uid
                ORDER BY s.geom <-> e.geom
                LIMIT 1
            ) AS near
            WHERE ST_Length(e.geom) > 0
        ),
        -- One row per (lot, street side). The cadastre cuts a roadway at every
        -- intersection, so a lot can meet one street through two road lots;
        -- that is one frontage, and this is where the two become it.
        measured AS (
            SELECT
                p.scrape_date,
                p.neighborhood,
                p.lot_uid,
                p.lot_number,
                n.cote_rue_id,
                max(n.street_name) AS street_name,
                sum(n.edge_m) AS frontage_m,
                p.lot_perimeter_m,
                row_number() OVER (
                    PARTITION BY p.lot_uid
                    -- cote_rue_id only to make the order total, so a re-run
                    -- ranks two exactly equal frontages the same way twice.
                    ORDER BY sum(n.edge_m) DESC, n.cote_rue_id
                ) AS frontage_rank,
                -- Merged back into as few linestrings as the edges allow, and
                -- returned to 4326, the CRS every geometry in this database is
                -- stored in. A lot meeting one street through two road lots
                -- merges into one run; a lot meeting the same side at two
                -- separate places is legitimately two strands.
                ST_Transform(ST_LineMerge(ST_Collect(n.geom)), 4326) AS geom
            FROM named n
            JOIN _frontage_lots p ON p.lot_uid = n.lot_uid
            GROUP BY p.scrape_date, p.neighborhood, p.lot_uid, p.lot_number,
                     p.lot_perimeter_m, n.cote_rue_id
        )
        SELECT
            scrape_date, neighborhood, lot_uid, cote_rue_id, lot_number,
            street_name,
            %(no_buffer)s::double precision, frontage_m, lot_perimeter_m,
            CASE WHEN lot_perimeter_m > 0
                 THEN 100.0 * frontage_m / lot_perimeter_m
                 ELSE 0.0
            END,
            frontage_rank,
            geom
        FROM measured
        WHERE frontage_m > 0
        """,
        {"no_buffer": float(FRONTAGE_NO_BUFFER)},
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
    # that is not itself a road is expected to face at least one street side;
    # the ones that do not are either genuine interior parcels or the symptom
    # of a street snapshot that stops short of them, and either way they are
    # the rows to go and look at. Road lots are excluded rather than reported
    # as landlocked - a street facing no street is the definition, not a
    # finding. Capped because a partition that has gone wrong produces
    # thousands of them, and the count is what says how wrong; the sample only
    # says where to start. Ordered so a re-run names the same lots.
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
          AND NOT EXISTS (
              SELECT 1 FROM _frontage_road_sides r
              WHERE r.road_lot_uid = l.lot_uid
          )
        ORDER BY lot_number
        LIMIT {_LOTS_WITHOUT_FRONTAGE_SAMPLE}
        """,
        [neighborhood, scrape_date],
    )
    lots_without_frontage = [row[0] for row in cursor.fetchall()]

    # The one way the exact intersection above can fail quietly, counted so it
    # cannot. A parcel that abuts a road lot sits at distance 0 and gets its
    # edge; a parcel that is genuinely elsewhere sits metres away and correctly
    # gets nothing. A parcel a few millimetres off a road lot is neither - it
    # is a survey gap, `ST_Intersects` drops it from the join, and the lot ends
    # up looking exactly like an interior parcel. This is that band, and on a
    # topologically clean cadastre it is empty. See `_SLIVER_GAP_M`.
    cursor.execute(
        """
        WITH roads AS (
            SELECT DISTINCT l.lot_uid, l.geom
            FROM _frontage_road_sides r
            JOIN _frontage_lots l ON l.lot_uid = r.road_lot_uid
        )
        SELECT count(*)
        FROM _frontage_lots l
        WHERE NOT EXISTS (SELECT 1 FROM roads s WHERE s.lot_uid = l.lot_uid)
          -- Near one ...
          AND EXISTS (
              SELECT 1 FROM roads road
              WHERE ST_DWithin(road.geom, l.geom, %(gap_m)s::double precision)
          )
          -- ... but abutting none. A corner parcel touching one road lot and
          -- lying a little off a second is not a sliver, which is why this is
          -- "intersects nothing" rather than "some road lot it misses".
          AND NOT EXISTS (
              SELECT 1 FROM roads road
              WHERE ST_Intersects(road.geom, l.geom)
          )
        """,
        {"gap_m": float(_SLIVER_GAP_M)},
    )
    (num_lots_near_road_without_frontage,) = cursor.fetchone()

    return {
        "frontages": result["upserted"],
        "pruned": result["pruned"],
        "lots_matched": int(lots_matched),
        "streets_matched": int(streets_matched),
        "total_frontage_m": float(total_frontage_m),
        "max_frontage_m": float(max_frontage_m),
        "num_lots": int(num_lots),
        "num_streets": int(num_streets),
        # The parcels that *are* the street. Not candidates for frontage, so
        # `num_lots` minus this is the denominator coverage is read against.
        "num_road_lots": int(num_road_lots),
        # Parcels that lie within `_SLIVER_GAP_M` of a road lot and abut none.
        # 0 on a topologically clean cadastre, which is what makes measuring
        # the shared edge exactly the right thing to do; anything else is a
        # survey gap swallowing frontage, and the number to act on before
        # reading the coverage below.
        "num_lots_near_road_without_frontage": int(
            num_lots_near_road_without_frontage
        ),
        # A sample, not the whole set - see the query. `num_lots` minus
        # `num_road_lots` minus `lots_matched` is the true count.
        "lots_without_frontage": lots_without_frontage,
        "min_street_m": float(min_street_m),
    }


# --------------------------------------------------------------------------
# what is left of a lot once its margins are taken off it
# --------------------------------------------------------------------------

#: Where the buildable envelope is carved, how finely the boundary is chopped
#: to sort it, and how near parallel a piece has to run to count as the rear
#: rather than a side.
#:
#: The first is `FRONTAGE_METRIC_SRID`, and for the same reason: metres in MTM
#: zone 8 are metres on the ground. The other two used to be aliases of
#: `compute_lot_frontage`'s values, because that function classified boundary
#: pieces by angle exactly as this one does. It no longer classifies anything -
#: a road lot's shared edge *is* the frontage, with nothing to chop or sort -
#: so the two constants live here now, where the only reader of them is.
#:
#: `SETBACK_SEGMENT_M` sets the resolution of the sort: a corner is placed to
#: within about a metre, and halving it doubles the rows the classification
#: runs over. `SETBACK_MAX_SIN` is how far from parallel a piece may run and
#: still count as the rear rather than a side, as the sine of the angle
#: between them - 0.7071 is 45 degrees. The test needs no trigonometry: for a
#: piece of length L whose ends sit d1 and d2 from the front edge,
#: ``|d1 - d2| / L`` *is* that sine, 0 for a piece parallel to the front and 1
#: for one running straight at it.
SETBACK_METRIC_SRID = FRONTAGE_METRIC_SRID
SETBACK_SEGMENT_M = 1.0
SETBACK_MAX_SIN = 0.7071

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

#: How many lots `compute_lot_buildable_setbacks` sorts, carves and publishes
#: per committed transaction.
#:
#: This is a *durability* setting, not a performance one. The work is the same
#: either way; what the batch size decides is how much of it a dropped
#: connection costs. Over an SSM port-forward that is the whole question: a
#: borough is roughly an hour of ST_Difference in one statement, the tunnel's
#: session is rebuilt every so often, and a transaction that spans a rebuild
#: rolls back entirely - so the unbatched version could not finish at all on
#: some days, whatever it did on others.
#:
#: 2,000 lots is about two minutes a slice on VSMPE's 24,952. Small enough to
#: land between two reconnects, large enough that the per-batch overhead - a
#: staging table, an ANALYZE of the leaf, one scan of the partition's envelopes
#: - stays a few per cent of the batch rather than most of it. Lower it on a
#: link that drops more often; a batch that never commits is worse than one
#: that is inefficient.
DEFAULT_SETBACK_BATCH_LOTS = 2000

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
       -- One batch's lots. `compute_lot_buildable_setbacks` drives this in
       -- slices rather than over the borough at once - see its docstring on
       -- why the whole borough in one statement is not survivable over a
       -- tunnel. Always present, so the batched and unbatched paths are one
       -- statement rather than two that can drift.
       AND lot_uid = ANY(%(lot_uids)s)
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


def _empty_setback_result(
    tolerance: float,
    *,
    num_lots: int,
    num_envelopes: int,
    num_lots_with_envelopes: int,
) -> dict[str, object]:
    """The result shape for a partition there was nothing to compute from.

    Every key the full return carries, so a caller reads one contract rather
    than two - `setback_assets` reaches straight into `num_lots` and
    `num_envelopes` to decide which gap to name, and a missing key would be a
    KeyError in the middle of reporting a failure.
    """
    return {
        "rows": 0,
        "pruned": 0,
        "num_batches": 0,
        "batch_lots": 0,
        "num_lots_resumed": 0,
        "num_lots": num_lots,
        "num_lots_sorted": 0,
        "num_lots_measured": 0,
        "lots_without_frontage": num_lots,
        "num_envelopes": num_envelopes,
        "num_lots_with_envelopes": num_lots_with_envelopes,
        "num_bound_by_setbacks": 0,
        "num_bound_by_site_coverage": 0,
        "num_unbuildable": 0,
        "total_buildable_area_m2": 0.0,
        "mean_buildable_pct_of_lot": 0.0,
        "by_side_setback_rule": {},
        "edge_tolerance_m": tolerance,
        "max_sin": float(SETBACK_MAX_SIN),
        "segment_m": float(SETBACK_SEGMENT_M),
    }


def _setback_lot_uids(
    cursor: "Cursor", neighborhood: str, scrape_date: str
) -> list[str]:
    """Every lot of the partition, in a stable order.

    The set `compute_lot_buildable_setbacks` slices into batches and, at the
    end, prunes against. Ordered so two runs cut the same borough at the same
    places: a resumed run then continues where the last one stopped instead of
    re-carving lots it has already published under a different batching.
    """
    cursor.execute(
        """
        SELECT lot_uid FROM rag.lots
        WHERE neighborhood = %s AND scrape_date = %s::date
        ORDER BY lot_uid
        """,
        [neighborhood, scrape_date],
    )
    return [row[0] for row in cursor.fetchall()]


def _setback_lots_already_published(
    cursor: "Cursor", neighborhood: str, scrape_date: str, tolerance: float
) -> set[str]:
    """Lots this partition already holds rows for, at ``tolerance``.

    Keyed on `edge_tolerance_m` and not on the partition alone, which is the
    difference between a resume and a silent half-answer: the tolerance
    travels on every row precisely so a run at a new one can tell its own work
    from the previous setting's, and redo the borough rather than leave two
    settings mixed in one table.
    """
    cursor.execute(
        """
        SELECT DISTINCT lot_uid FROM silver.lot_buildable_setbacks
        WHERE neighborhood = %s AND scrape_date = %s::date
          AND edge_tolerance_m = %s
        """,
        [neighborhood, scrape_date, tolerance],
    )
    return {row[0] for row in cursor.fetchall()}


def compute_lot_buildable_setbacks(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
    edge_tolerance_m: float = DEFAULT_SETBACK_EDGE_TOLERANCE_M,
    batch_lots: int = DEFAULT_SETBACK_BATCH_LOTS,
    resume: bool = True,
    progress: "Callable[[str], None] | None" = None,
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

    **The borough is done in committed slices, and that is a durability
    decision rather than a performance one.** ``batch_lots`` lots are sorted,
    carved and published per transaction, and each one commits before the next
    begins. The work is identical either way; what the slice size decides is
    how much of it a dropped connection costs.

    That matters because of where this runs from. The whole borough in one
    statement is the better part of an hour of `ST_Difference`, and a laptop
    reaches the database through an SSM port-forward whose session is torn
    down and rebuilt periodically - `hbu_infra/scripts/tunnel.sh` reconnects in
    about half a minute, but a transaction open across that gap does not
    survive it, and rolls back *everything*. Unbatched, this asset was not slow
    on a bad day, it was unable to finish at all: two consecutive VSMPE runs
    died at 11 and 47 minutes having committed nothing. Sliced, a drop costs
    the batch in flight.

    ``resume`` is what makes the retry cheap rather than merely possible. A
    re-run skips the lots already published for this partition **at this
    ``edge_tolerance_m``** - the tolerance is on every row, so a run at a
    different one correctly redoes the borough instead of silently mixing two
    settings in one table. Pass ``resume=False`` to force the whole partition.

    The prune that makes a partition a snapshot still happens, once, after the
    last batch: `warehouse.upsert_select` is called with ``prune=False`` per
    slice, and the rows for lots that no longer exist are deleted at the end
    against the full set this run expected. So the table is only ever a whole
    answer or the previous one, the same guarantee the single statement gave.
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

    expected = _setback_lot_uids(cursor, neighborhood, scrape_date)

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

    # Both counts are taken *before* anything is written, which the single
    # statement did not have to care about and this does. A batch deletes its
    # own lots before rewriting them and commits; on a partition whose
    # envelopes never landed, `carved` is empty, so that sequence would delete
    # a good answer and commit nothing in its place. Returning early instead
    # leaves the table untouched and lets the caller raise naming the asset to
    # run - which is the same failure it reported before, minus the damage.
    if int(num_lots) == 0 or int(num_envelopes) == 0:
        return _empty_setback_result(
            tolerance,
            num_lots=int(num_lots),
            num_envelopes=int(num_envelopes),
            num_lots_with_envelopes=int(lots_with_envelopes),
        )

    done = (
        _setback_lots_already_published(cursor, neighborhood, scrape_date, tolerance)
        if resume
        else set()
    )
    pending = [uid for uid in expected if uid not in done]
    size = max(int(batch_lots), 1) if batch_lots else max(len(pending), 1)
    batches = [pending[i : i + size] for i in range(0, len(pending), size)]

    if done and progress:
        progress(
            f"resuming: {len(done)} of {len(expected)} lot(s) already published "
            f"at edge_tolerance_m={tolerance}, {len(pending)} to go"
        )

    lots_sorted = len(done)
    upserted = 0
    for number, batch in enumerate(batches, start=1):
        # A fresh mapping per slice rather than one rebound in place: psycopg
        # reads it at execute time either way, but a shared dict makes every
        # statement of the run appear to have carried the *last* batch, which
        # is a trap for anything that inspects them afterwards.
        batch_parameters = {**parameters, "lot_uids": batch}

        # Dropped rather than declared ON COMMIT DROP, for the reason
        # `_frontage_sides` is: this runs inside the caller's transaction, and
        # session scope is what makes a second call on one connection work
        # whether or not that connection is in autocommit.
        cursor.execute(f"DROP TABLE IF EXISTS {_SETBACK_EDGES}")
        cursor.execute(
            f"CREATE TEMP TABLE {_SETBACK_EDGES} AS {_SETBACK_EDGES_SQL}",
            batch_parameters,
        )
        cursor.execute(f"CREATE INDEX ON {_SETBACK_EDGES} (lot_uid)")
        # Without stats the planner costs the join below against a default row
        # estimate and can pick a scan over the index it was just handed - the
        # same reason `compute_lot_frontage` analyzes `_frontage_sides`.
        cursor.execute(f"ANALYZE {_SETBACK_EDGES}")
        cursor.execute(f"SELECT count(*) FROM {_SETBACK_EDGES}")
        (sorted_here,) = cursor.fetchone()
        lots_sorted += int(sorted_here)

        # The batch's own rows, cleared before they are rewritten. The upsert
        # below conflicts on (lot_uid, feature_id, column_index) and so would
        # leave behind a row for a column the grid no longer states - which the
        # single-statement version pruned at the end and this one cannot, since
        # its prune only ever sees one slice. Deleting the slice first makes a
        # batch a replace of exactly its own lots, inside its own transaction.
        cursor.execute(
            "DELETE FROM silver.lot_buildable_setbacks "
            "WHERE neighborhood = %s AND scrape_date = %s::date "
            "AND lot_uid = ANY(%s)",
            [neighborhood, scrape_date, batch],
        )

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
            batch_parameters,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
            # The snapshot prune happens once, after the loop: this slice's
            # staging table knows only its own lots, and pruning against it
            # would delete every batch before it.
            prune=False,
        )
        upserted += int(result["upserted"])

        # What the whole change is for. Until this returns, the batch can still
        # be lost; after it, no later failure can take it back and a re-run
        # skips it. Committing the caller's transaction from inside is
        # deliberate and is why the docstring says so - `connect()` commits
        # again on a clean exit, which is a no-op, and rolls back only whatever
        # slice was open when something broke.
        connection.commit()
        if progress:
            progress(
                f"batch {number}/{len(batches)}: {len(batch)} lot(s), "
                f"{result['upserted']} row(s) committed"
            )

    # The prune the single statement used to do as part of its merge. Against
    # the full set this run expected rather than against a staging table, so a
    # lot that has left the cadastre loses its rows while every committed batch
    # keeps them.
    cursor.execute(
        "DELETE FROM silver.lot_buildable_setbacks "
        "WHERE neighborhood = %s AND scrape_date = %s::date "
        "AND NOT (lot_uid = ANY(%s))",
        [neighborhood, scrape_date, expected],
    )
    pruned = max(cursor.rowcount, 0)
    connection.commit()

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
        "rows": upserted,
        "pruned": pruned,
        # What the run actually did, against what it found already done. A
        # `num_batches` of 0 with a full `num_lots_measured` is a re-run that
        # had nothing left to do, which is what a resumed retry looks like once
        # it has caught up.
        "num_batches": len(batches),
        "batch_lots": int(size),
        "num_lots_resumed": len(done),
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

#: How much of a lot a zone has to cover, in square metres, before it counts
#: as covering it at all.
#:
#: The artefact cutoff, and the one number the whole platform reads it at. A
#: cadastral boundary and a zoning boundary are drawn by two offices from two
#: surveys, so they miss each other by centimetres along every lot line and
#: each parcel clips a corner of its neighbour's zone. `silver.lot_features`
#: keeps those rows deliberately - 005_silver_lot_features.sql argues that the
#: cutoff belongs to the question being asked - and this is the value each
#: question that asks "which zone governs this lot" answers at.
#:
#: Absolute rather than proportional because the artefact has an absolute size,
#: while a percentage means one thing on a 200 m2 duplex parcel and quite
#: another on Parc Jarry. That is what separates it from
#: `EnvelopeConfig.min_pct_of_lot`, which is a judgement about the borough and
#: is why that one defaults to keeping everything and this one does not.
#:
#: Read by `EnvelopeConfig.min_overlap_m2` and by `compute_lot_profiles`.
#: hbu_infra's `rag.search_at_lot_number` and hbu_rag_map's
#: `queries.MIN_ZONE_OVERLAP_M2` are the same square metre in the two repos
#: that cannot import this one.
MIN_ZONE_OVERLAP_M2 = 1.0

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

#: Columns `compute_lot_profiles` reads that arrived after the relation holding
#: them did, with the hbu_infra file that adds each.
#:
#: A relation being present is not the same as it being current, and a view is
#: where the difference bites: `rag.lot_documents` has existed since 006 was
#: first applied, and the revision that carries `overlap_area_m2` may not have
#: been. Without this the partition fails on `column ld.overlap_area_m2 does
#: not exist`, which names neither the view nor the file to re-apply.
_LOT_PROFILE_REQUIRED_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("rag.lot_documents", "overlap_area_m2", "sql/006_lot_documents.sql"),
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
    min_overlap_m2: float = MIN_ZONE_OVERLAP_M2,
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

    That third one is read at `min_overlap_m2`: a zone clipping under a square
    metre of a parcel contributes no document to it, because it is not a zone
    the parcel is in. See `MIN_ZONE_OVERLAP_M2`.

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
    _require_columns(cursor, _LOT_PROFILE_REQUIRED_COLUMNS)

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
            "dominant_use_code", "dominant_use_description", "year_built",
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
                   -- 0 on anything measured against the road lot, which is
                   -- every partition computed since - there is no buffer, so
                   -- the column that carried one now dates the row instead:
                   -- 3.0 or 10.0 here is a profile built on the old
                   -- buffer-and-angle frontage. See `compute_lot_frontage`.
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
            --
            -- The area cutoff is what keeps it a count of documents that
            -- *apply*. A parcel clipping a square metre of the block next door
            -- picks up that block's grid, and it lands in `documents` and in
            -- `num_documents` looking exactly like the grid of the zone the
            -- parcel is actually in. `doc_id` and the other flattened columns
            -- were already safe - they take the highest `pct_of_lot` - so this
            -- is the array and the count, which nothing else was guarding.
            SELECT DISTINCT ON (ld.lot_uid, ld.source_table, ld.doc_id)
                   ld.lot_uid, ld.source_table, ld.doc_id, ld.url, ld.title,
                   ld.feature_id, ld.pct_of_lot
              FROM rag.lot_documents ld
             WHERE ld.neighborhood = %(neighborhood)s
               AND ld.scrape_date = %(scrape_date)s::date
               AND ld.overlap_area_m2 >= %(min_overlap_m2)s
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
            -- What that code says, in the MEFQ's words. Carried up from
            -- sql/016 rather than looked up here: the description a lot
            -- is profiled with should be the one its silver partition was
            -- described with, not whatever edition of the manual a later
            -- reader holds. NULL is normal - a lane with no unit on it, a
            -- blank rl0105a, or a code the manual does not number.
            comparables.dominant_use_description,
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
            "min_overlap_m2": min_overlap_m2,
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


# --------------------------------------------------------------------------
# the low-zoom aggregates
#
# `gold.map_cell_aggregates` is the one table here whose rows are not a fact
# about a lot but a fact about a *pixel*: every gated map layer dissolved onto
# the Web Mercator tile grid, so a borough-wide view has something true to draw
# where at present it draws nothing at all. `urban_rag.tile_grid` holds the
# grid arithmetic and declares what each layer's cell carries; everything below
# is the SQL that fills it, and hbu_infra's sql/023 is the table.
#
# **The pyramid.** Only the finest level - `tile_grid.BASE_CELL_ZOOM` - is
# computed from real geometry. Every coarser level is four children rolled into
# their parent, which is a halving of the two cell indices and a `sum`. The
# alternative, clipping twenty-five thousand lots against the grid once per
# level, does the expensive part five times over to arrive at the same answer.
#
# **Two staging tables, and they are why this reads the way it does.** The
# geometry and the measures roll up on different keys - a union grouped by
# parent cell, and a sum grouped by parent cell *and measure name* - so they
# are accumulated apart and joined once at the end. Keeping the measures in
# long form (one row per cell per measure) is what makes that rollup generic:
# giving a layer another number in `tile_grid` needs no change here, because
# `sum(value) GROUP BY measure` does not care how many measures there are. They
# are pivoted into the `attributes` jsonb in the final select, which is the one
# place those names become keys.
#
# **Nothing crosses into Python until the upsert.** Five layers by five levels
# is twenty-five statements for a borough, each an aggregate over rows already
# loaded and GiST-indexed - the same posture `compute_lot_features` takes, for
# the same reason.
# --------------------------------------------------------------------------

#: The Mercator limit, in degrees. A latitude past this has no row in the tile
#: grid to land in - the projection sends it to infinity - so it is clamped
#: rather than left to produce a NaN cell index. Nothing in Montreal is near
#: it; the clamp is here so one stray geometry cannot take a partition down.
_MERCATOR_MAX_LAT = 85.0511287798

#: The measures read off the dissolved geometry rather than off the feature
#: rows: everything in `tile_grid.UNIVERSAL_MEASURES` except the count, which
#: comes from the representative points and has a column of its own.
#:
#: Derived rather than restated, because the two lists disagreeing is not a
#: crash: `_measure_reference` would resolve the stray name to a jsonb key that
#: is never written, and the layer shading on it would come back NULL on every
#: cell - a borough drawn entirely in the "not answered" grey, which is a
#: legible thing for a map to draw.
_GEOMETRY_MEASURES = tuple(
    name for name in tile_grid.UNIVERSAL_MEASURES if name != "feature_count"
)


def _sql_literal(value: str) -> str:
    """A Python string as a SQL string literal.

    These are layer and measure names out of `tile_grid`, never user input, and
    they are interpolated rather than bound because they appear inside `VALUES`
    lists and `CASE` arms built per layer. Quoting them properly anyway costs
    nothing and closes the one way a name could break the statement.
    """
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _cell_x_sql(lon: str, zoom: int) -> str:
    """The tile column containing longitude ``lon`` at ``zoom``, as SQL.

    `tile_grid.cell_of` is the same arithmetic in Python, and the unit tests
    check the two agree on real coordinates: a grid computed one way in SQL and
    asserted another way in Python is a grid with no test at all.
    """
    span = 1 << zoom
    return (
        f"greatest(0, least({span - 1}, "
        f"floor(((({lon}) + 180.0) / 360.0) * {span}.0)::integer))"
    )


def _cell_y_sql(lat: str, zoom: int) -> str:
    """The tile row containing latitude ``lat`` at ``zoom``, as SQL.

    Rows run north to south, so this is the one place where the smaller index
    comes from the larger coordinate. `asinh(tan(radians(lat)))` is the
    Mercator northing; Postgres has had `asinh` since 12.
    """
    span = 1 << zoom
    clamped = f"greatest({-_MERCATOR_MAX_LAT}, least({_MERCATOR_MAX_LAT}, {lat}))"
    return (
        f"greatest(0, least({span - 1}, "
        f"floor(((1.0 - asinh(tan(radians({clamped}))) / pi()) / 2.0) "
        f"* {span}.0)::integer))"
    )


def _cell_envelope_sql(zoom: str, x: str, y: str) -> str:
    """The cell ``zoom/x/y`` as an EPSG:4326 box.

    `ST_TileEnvelope` answers in 3857 and every layer here is stored in 4326,
    so one of the two has to move. Transforming the *envelope* rather than the
    geometry is what keeps the clip cheap and leaves the GiST indexes usable,
    and it is exact rather than approximate: the projection is separable and
    monotone on each axis, so the transform of an axis-parallel box is the box
    it should be.
    """
    return f"ST_Transform(ST_TileEnvelope({zoom}, {x}, {y}), 4326)"


def _create_aggregate_staging(cursor: Any) -> None:
    """The two temp tables the pyramid is accumulated in.

    ``ON COMMIT DROP`` rather than an explicit clean-up: the whole computation
    runs inside the caller's transaction, so these live exactly as long as it
    does and a failed partition leaves nothing behind for the retry to collide
    with.
    """
    cursor.execute(
        """
        CREATE TEMP TABLE _agg_cells (
            layer   text     NOT NULL,
            cell_z  smallint NOT NULL,
            cell_x  integer  NOT NULL,
            cell_y  integer  NOT NULL,
            geom    geometry NOT NULL
        ) ON COMMIT DROP
        """
    )
    cursor.execute(
        """
        CREATE TEMP TABLE _agg_measures (
            layer   text     NOT NULL,
            cell_z  smallint NOT NULL,
            cell_x  integer  NOT NULL,
            cell_y  integer  NOT NULL,
            measure text     NOT NULL,
            value   double precision
        ) ON COMMIT DROP
        """
    )
    # Every rollup and the final join look rows up by level. Two indexes on
    # temp tables of a few tens of thousands of rows, which is cheaper than the
    # five sequential scans they replace.
    cursor.execute("CREATE INDEX ON _agg_cells (cell_z, layer)")
    cursor.execute("CREATE INDEX ON _agg_measures (cell_z, layer)")


def _seed_cell_geometry(
    cursor: Any, spec: "tile_grid.LayerSpec", params: dict
) -> int:
    """Dissolve one layer's geometry into the finest cells.

    The clip, and the only statement in this module that touches raw geometry.
    A feature is expanded into every cell its *envelope* spans and then
    intersected with each, so a diagonal street side generates some cells its
    linework never enters; those clip to empty and are dropped by the `WHERE`.
    That waste is bounded - an envelope spans a few hundred cells at worst -
    and it is what lets the expansion be plain arithmetic on a bounding box
    instead of a spatial join.

    `ST_CollectionExtract` at the layer's own dimension is not optional. A
    polygon clipped against a box its edge runs along comes back as a
    collection carrying that edge, and unioning those into the cell would draw
    a hairline along every cell boundary the cadastre happens to touch.
    """
    zoom = tile_grid.BASE_CELL_ZOOM
    geom = spec.geometry
    envelope = _cell_envelope_sql(str(zoom), "cx.cell_x", "cy.cell_y")
    statement = f"""
        INSERT INTO _agg_cells (layer, cell_z, cell_x, cell_y, geom)
        SELECT %(layer)s, {zoom}, cx.cell_x, cy.cell_y, ST_Union(clip.geom)
          FROM {spec.source}
          CROSS JOIN LATERAL (
              SELECT {_cell_x_sql(f"ST_XMin({geom})", zoom)} AS x0,
                     {_cell_x_sql(f"ST_XMax({geom})", zoom)} AS x1,
                     {_cell_y_sql(f"ST_YMax({geom})", zoom)} AS y0,
                     {_cell_y_sql(f"ST_YMin({geom})", zoom)} AS y1
          ) AS box
          CROSS JOIN LATERAL generate_series(box.x0, box.x1) AS cx(cell_x)
          CROSS JOIN LATERAL generate_series(box.y0, box.y1) AS cy(cell_y)
          CROSS JOIN LATERAL (
              SELECT ST_CollectionExtract(
                         ST_Intersection({geom}, {envelope}),
                         {spec.dimension}
                     ) AS geom
          ) AS clip
         WHERE {spec.where}
           AND {geom} IS NOT NULL
           AND NOT ST_IsEmpty({geom})
           AND NOT ST_IsEmpty(clip.geom)
         GROUP BY cx.cell_x, cy.cell_y
    """
    cursor.execute(statement, params)
    return max(cursor.rowcount, 0)


def _seed_cell_measures(
    cursor: Any, spec: "tile_grid.LayerSpec", params: dict
) -> int:
    """Sum one layer's per-feature numbers into the finest cells.

    The other assignment, and the one that makes the counts exact: a feature
    contributes to precisely one cell, the one holding its `ST_PointOnSurface`.
    `ST_Centroid` is the obvious choice and the wrong one - the centroid of an
    L-shaped parcel can fall outside it, and on a grid this fine that is a lot
    credited to ground it does not touch.

    The measures land in long form, one row per (cell, measure), which is what
    lets the rollup above them be a single generic `sum`. `feature_count` rides
    along as a measure whose value is 1 rather than as a `count(*)`, for the
    same reason: it then rolls up through exactly the same statement as
    everything else.
    """
    zoom = tile_grid.BASE_CELL_ZOOM
    measures = {"feature_count": "1.0", **spec.point_measures}
    values = ", ".join(
        f"({_sql_literal(name)}, ({expression})::double precision)"
        for name, expression in measures.items()
    )
    statement = f"""
        INSERT INTO _agg_measures (layer, cell_z, cell_x, cell_y, measure, value)
        SELECT %(layer)s,
               {zoom},
               {_cell_x_sql("ST_X(point.geom)", zoom)},
               {_cell_y_sql("ST_Y(point.geom)", zoom)},
               measured.measure,
               sum(measured.value)
          FROM {spec.source}
          CROSS JOIN LATERAL (
              SELECT ST_PointOnSurface({spec.geometry}) AS geom
          ) AS point
          CROSS JOIN LATERAL (VALUES {values}) AS measured(measure, value)
         WHERE {spec.where}
           AND {spec.geometry} IS NOT NULL
           AND NOT ST_IsEmpty({spec.geometry})
         GROUP BY 3, 4, measured.measure
    """
    cursor.execute(statement, params)
    return max(cursor.rowcount, 0)


def _roll_up_level(cursor: Any, zoom: int) -> tuple[int, int]:
    """Build level ``zoom`` from the level one finer.

    Both halves are the same idea spelled for their own key: four children
    reduce to one parent by halving each index, which is what makes this a
    pyramid rather than five independent grids. `>>` rather than `/ 2` because
    these are grid indices and the shift is the definition, not an
    optimisation.

    Geometry is unioned and measures are summed, and both are exact for the
    reason `urban_rag.tile_grid` gives: the four children of a cell are
    disjoint and tile it exactly.
    """
    child = zoom + 1
    cursor.execute(
        """
        INSERT INTO _agg_cells (layer, cell_z, cell_x, cell_y, geom)
        SELECT layer, %(zoom)s, cell_x >> 1, cell_y >> 1, ST_Union(geom)
          FROM _agg_cells
         WHERE cell_z = %(child)s
         GROUP BY layer, cell_x >> 1, cell_y >> 1
        """,
        {"zoom": zoom, "child": child},
    )
    cells = max(cursor.rowcount, 0)
    cursor.execute(
        """
        INSERT INTO _agg_measures
               (layer, cell_z, cell_x, cell_y, measure, value)
        SELECT layer, %(zoom)s, cell_x >> 1, cell_y >> 1, measure, sum(value)
          FROM _agg_measures
         WHERE cell_z = %(child)s
         GROUP BY layer, cell_x >> 1, cell_y >> 1, measure
        """,
        {"zoom": zoom, "child": child},
    )
    return cells, max(cursor.rowcount, 0)


#: The columns of `gold.map_cell_aggregates` a run writes, in the order
#: `_aggregate_select` produces them. `warehouse.upsert_select` pairs the two
#: **positionally**, so this list and that SELECT are one thing written twice -
#: which is a thing worth naming rather than leaving inline, because a column
#: added to one and not the other shifts every value after it one place to the
#: left and Postgres only notices where the types happen to disagree.
MAP_CELL_COLUMNS: tuple[str, ...] = (
    "scrape_date",
    "neighborhood",
    "layer",
    "cell_z",
    "cell_x",
    "cell_y",
    "feature_count",
    "value",
    "value_kind",
    "dissolved_area_m2",
    "dissolved_length_m",
    "cell_area_m2",
    "coverage_pct",
    "attributes",
    "geom",
)


def _measure_reference(name: str) -> str:
    """Where a measure named in a `LayerSpec.value` is read from."""
    if name in _GEOMETRY_MEASURES:
        return f"shape.{name}"
    if name == "feature_count":
        return "measures.feature_count"
    return f"(measures.attributes ->> {_sql_literal(name)})::double precision"


def _value_expression(spec: "tile_grid.LayerSpec") -> str:
    """``spec``'s shaded number, as SQL over the joined staging rows.

    Every layer's value is ``scale * numerator / denominator``, and the two
    names are resolved to wherever that measure actually lives - a
    geometry-derived column, the count, or a key of the measures pivot. That
    indirection is what lets `tile_grid` declare a layer's shading as three
    values rather than as a fragment of SQL.

    `NULLIF` on the denominator is what produces the NULL the map shades as
    "not answered". On the capacity layer that is the whole difference between
    a cell nothing was solved for and a cell at 0% of its permitted floor, and
    those are opposite findings.
    """
    return (
        f"{spec.scale} * {_measure_reference(spec.numerator)} "
        f"/ NULLIF({_measure_reference(spec.denominator)}, 0)"
    )


def _aggregate_select(layers: Sequence[str]) -> str:
    """The statement that turns the two staging tables into table rows.

    Three things happen here and nowhere else, which is why it is assembled
    rather than written out:

    * the **geometry measures** are taken, in the `shape` CTE - the dissolved
      area and length of the clip, and the cell's own area, which is what every
      density on this table is per. Named `shape` rather than `geometry`
      because the latter is a PostGIS type name, and an alias that shadows one
      is a thing to trip over rather than a thing to read;
    * the **measures are pivoted** out of long form into `attributes`, with
      `feature_count` lifted into a column of its own because it is the one
      number every layer has and the one the map wants without parsing json;
    * each layer's **value** is computed by its own rule, in a CASE built from
      `tile_grid`'s specs - so the shading rule lives beside the layer
      declaration instead of inside this string.
    """
    specs = [tile_grid.layer_spec(name) for name in layers]
    value_cases = "\n               ".join(
        f"WHEN {_sql_literal(spec.name)} THEN {_value_expression(spec)}"
        for spec in specs
    )
    kind_cases = "\n               ".join(
        f"WHEN {_sql_literal(spec.name)} THEN {_sql_literal(spec.value_kind)}"
        for spec in specs
    )
    envelope = _cell_envelope_sql(
        "cells.cell_z::integer", "cells.cell_x", "cells.cell_y"
    )
    return f"""
        WITH shape AS (
            SELECT cells.layer,
                   cells.cell_z,
                   cells.cell_x,
                   cells.cell_y,
                   cells.geom,
                   -- Both taken on every layer: ST_Area of linework is 0 and
                   -- ST_Length of an areal geometry is 0, so the layers sort
                   -- themselves out without a branch here.
                   ST_Area(geography(cells.geom))   AS dissolved_area_m2,
                   ST_Length(geography(cells.geom)) AS dissolved_length_m,
                   ST_Area(geography({envelope}))   AS cell_area_m2
              FROM _agg_cells AS cells
        ),
        measures AS (
            SELECT layer,
                   cell_z,
                   cell_x,
                   cell_y,
                   max(value) FILTER (WHERE measure = 'feature_count')
                       AS feature_count,
                   COALESCE(
                       jsonb_object_agg(measure, value)
                           FILTER (WHERE measure <> 'feature_count'),
                       '{{}}'::jsonb
                   ) AS attributes
              FROM _agg_measures
             GROUP BY layer, cell_z, cell_x, cell_y
        )
        SELECT %(scrape_date)s::date,
               %(neighborhood)s,
               shape.layer,
               shape.cell_z,
               shape.cell_x,
               shape.cell_y,
               COALESCE(measures.feature_count, 0)::integer,
               CASE shape.layer
               {value_cases}
               END,
               CASE shape.layer
               {kind_cases}
               END,
               shape.dissolved_area_m2,
               shape.dissolved_length_m,
               shape.cell_area_m2,
               100.0 * shape.dissolved_area_m2
                   / NULLIF(shape.cell_area_m2, 0),
               COALESCE(measures.attributes, '{{}}'::jsonb),
               shape.geom
          FROM shape
          -- LEFT, and from the shape side: a cell a neighbouring lot's edge
          -- reaches into has a shape and no feature of its own. Dropping it
          -- would put a hole in the dissolved surface exactly where a large
          -- parcel meets a cell boundary.
          LEFT JOIN measures
                 ON measures.layer  = shape.layer
                AND measures.cell_z = shape.cell_z
                AND measures.cell_x = shape.cell_x
                AND measures.cell_y = shape.cell_y
    """


def compute_map_cell_aggregates(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
    layers: Sequence[str] | None = None,
) -> dict[str, object]:
    """(Re)compute `gold.map_cell_aggregates` for one (neighborhood, scrape_date).

    The map's five gated layers, dissolved onto the tile grid at every level in
    `tile_grid.CELL_ZOOMS`, so a view below a layer's gate has something true
    to draw instead of nothing. See `urban_rag.tile_grid` for why a cell is a
    tile and why two different assignments are used for the two kinds of
    measure.

    Assumes the partition's rows are already in Postgres - `rag.lots` and
    `rag.buildings` from `load_lots`/`load_buildings`, and the three published
    tables from their own assets. Every layer is screened on
    ``(neighborhood, scrape_date)``, so a borough can never be summarised
    against another date's cadastre.

    A layer whose source holds nothing for this partition contributes no rows
    rather than failing the run, which is both the honest outcome and the
    useful one: `lot_building_massing` may not have been materialized for a
    borough yet, and refusing to build the other four because of it would make
    this asset as fragile as its least-run input. The per-layer counts come
    back in the result, so an empty layer is visible rather than silent.
    """
    wanted = tuple(layers) if layers is not None else tile_grid.LAYERS
    cursor = connection.cursor()
    _require_relations(
        cursor,
        (("gold.map_cell_aggregates", "sql/023_gold_map_cell_aggregates.sql"),),
    )
    _create_aggregate_staging(cursor)

    params = {"neighborhood": neighborhood, "scrape_date": scrape_date}
    base_cells: dict[str, int] = {}
    for name in wanted:
        spec = tile_grid.layer_spec(name)
        layer_params = {**params, "layer": name}
        base_cells[name] = _seed_cell_geometry(cursor, spec, layer_params)
        _seed_cell_measures(cursor, spec, layer_params)

    # Coarsest last, each level reading the one below it. `CELL_ZOOMS` is
    # ascending and the base is its maximum, so this walks back down from
    # `BASE_CELL_ZOOM - 1`.
    for zoom in sorted(tile_grid.CELL_ZOOMS, reverse=True)[1:]:
        _roll_up_level(cursor, zoom)

    published = warehouse.upsert_select(
        cursor,
        "map_cell_aggregates",
        MAP_CELL_COLUMNS,
        _aggregate_select(wanted),
        params,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    cursor.execute(
        """
        SELECT layer, cell_z, count(*)
          FROM gold.map_cell_aggregates
         WHERE neighborhood = %s AND scrape_date = %s::date
         GROUP BY layer, cell_z
        """,
        [neighborhood, scrape_date],
    )
    by_level = {
        f"{layer}@z{zoom}": int(count) for layer, zoom, count in cursor.fetchall()
    }

    # The bound this whole design exists for, measured rather than asserted in
    # a comment: how many cells the busiest served tile would carry. It should
    # be `tile_grid.cells_per_tile()` and cannot exceed it, because a tile at
    # zoom Z holds exactly that many cells at Z + ZOOM_OFFSET - so what this
    # actually catches is a level built from the wrong child level.
    cursor.execute(
        f"""
        SELECT COALESCE(max(per_tile), 0)
          FROM (
              SELECT count(*) AS per_tile
                FROM gold.map_cell_aggregates
               WHERE neighborhood = %s AND scrape_date = %s::date
               GROUP BY layer,
                        cell_z,
                        cell_x >> {tile_grid.ZOOM_OFFSET},
                        cell_y >> {tile_grid.ZOOM_OFFSET}
          ) AS served
        """,
        [neighborhood, scrape_date],
    )
    (max_per_tile,) = cursor.fetchone()

    return {
        "published": published,
        "num_cells_by_layer_level": by_level,
        "num_base_cells_by_layer": base_cells,
        "max_cells_per_served_tile": int(max_per_tile or 0),
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


def _require_columns(
    cursor: Any, columns: tuple[tuple[str, str, str], ...]
) -> None:
    """Raise `MissingRelation` naming every column an older revision lacks.

    The companion to `_require_relations`, for the case that check cannot see:
    the relation is there and out of date. Same exception, because the fix is
    the same one - re-apply the file - and a caller that already handles a
    missing view should not need a second branch for a missing column of it.

    Assumes the relation itself exists; run it after `_require_relations`, so
    a database missing the view entirely is told that rather than this.
    """
    missing: list[str] = []
    for name, column, source in columns:
        cursor.execute(
            """
            SELECT 1 FROM pg_attribute
             WHERE attrelid = to_regclass(%s)
               AND attname = %s
               AND attnum > 0
               AND NOT attisdropped
            """,
            [name, column],
        )
        if cursor.fetchone() is None:
            missing.append(f"{name}.{column} ({source})")
    if missing:
        raise MissingRelation(
            "hbu_infra has an out-of-date definition, missing: "
            + ", ".join(missing)
            + " - re-apply the file(s) above with `./scripts/db.py init` in "
            "that repo, then re-run this partition."
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
    "dominant_use_description",
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


#: `gold.map_cell_aggregates`, for the copy written to the tree. `attributes`
#: is cast to text on the way out for the reason the other jsonb columns here
#: are: psycopg hands a jsonb back as a parsed dict, and a parquet column of
#: dicts is a struct whose fields are whatever the first row happened to have.
#: Text round-trips, and every reader of the tree already parses it.
_MAP_CELL_COLUMNS = (
    "layer",
    "cell_z",
    "cell_x",
    "cell_y",
    "neighborhood",
    "feature_count",
    "value",
    "value_kind",
    "dissolved_area_m2",
    "dissolved_length_m",
    "cell_area_m2",
    "coverage_pct",
    "attributes::text AS attributes",
)


def fetch_map_cell_aggregates(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `gold.map_cell_aggregates` rows, as a GeoDataFrame.

    Ordered by (layer, level, x, y) - the address, not any of the measures.
    This table is read a tile at a time rather than as a ranked list, so the
    order that helps is the one that puts a level's cells next to each other
    in the file.
    """
    return _fetch_partition(
        connection,
        _MAP_CELL_COLUMNS,
        """
        FROM gold.map_cell_aggregates
        WHERE neighborhood = %s AND scrape_date = %s::date
        ORDER BY layer, cell_z, cell_x, cell_y
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
        # A borough emptied is as much a change of shape as a borough loaded,
        # and the planner is as wrong about it either way.
        analyze(connection, table)
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
    analyze(connection, table)
    return inserted
