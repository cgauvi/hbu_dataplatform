"""Loading the latest lots, buildings and map features into Postgres/PostGIS,
the spatial joins between them, and the per-lot profile they collapse into.

`rag.lots` and `rag.buildings` are owned by hbu_infra (see its README and
sql/002_spatial.sql) - this module only ever DELETEs/INSERTs into tables it
assumes already exist, the same ownership split `rag.chunks`/`rag_assets.py`
draws the other way: hbu_infra creates the tables, this repo fills them.

`rag.building_lots` is the derived join: for each (building, lot) pair whose
footprints intersect, one row holding the *clipped* intersection geometry and
its share of the building's area - so a warehouse straddling three lots is
assigned to each of them in proportion to the footprint actually inside it,
rather than to whichever lot its centroid happens to land on. Computed with
`ST_Intersection` in Postgres rather than in GeoPandas: by the time this runs,
PostGIS already holds both layers loaded and GiST-indexed, and the join is
exactly the kind of thing that index is for.

`rag.lot_features` is the other derived join, and the one the corpus hangs
off. `rag.chunks.feature_ids` records which map features cite each indexed
PDF, so a lot's documents are whatever the features covering it cite - but
there is no id that gets from a lot to a feature. The lots come from Infolot,
Quebec's cadastre, keyed by `NO_LOT`; the features come from Montreal's
Spectrum service, keyed by `NUMERO_COMPLET`, and neither publisher carries the
other's key. Geometry is the only thing the two share, so the join is spatial
by necessity rather than by choice - see `compute_lot_features`, and
hbu_infra's sql/005_lot_features.sql for the table.

`rag.lot_frontage` is the third derived join, and the one that answers "how
much of this lot faces a street". Its right-hand side is `rag.streets`, the
city's *geobase double* - one line per side of street, drawn along the curb -
and the measure is taken on the lot's `ST_Boundary`, not on the lot itself: a
lot is a polygon, and `ST_Length` of a polygon is zero, so intersecting the
two solids and measuring the result would report nothing at all. See
`compute_lot_frontage` for the buffer this hangs on, and hbu_infra's
sql/007_streets.sql and sql/008_lot_frontage.sql for the two tables.

`rag.lot_profiles` is where the three joins above come back together, at the
grain they are all about: one row per lot, carrying how many buildings stand on
it, the two street edges it fronts on, and the document that governs it -
alongside three jsonb columns its caller hands in from the geoparquet tree,
since `silver/lot_zoning_envelopes`, `silver/vacancy_rates` and
`silver/average_rents` are never loaded into Postgres at all. It
replaces an earlier `rag.vacant_lots`, which read the building join the other
way round and kept only the parcels it found nothing on - a table that could
answer "where is the empty land" and nothing else, because the lots its WHERE
clause dropped were the ones a reader could no longer see. Keeping every lot
and carrying `has_building` costs one boolean and makes that question a filter;
see `compute_lot_profiles`.

None of this is a live view. A partition is refreshed by deleting and
reinserting its (neighborhood, scrape_date) rows, the same snapshot semantics
the geoparquet tree already has - not by upserting row by row, because BDOI's
buildings carry no key that survives a re-scrape the way Infolot's lot number
does. "Latest" therefore falls out for free: whatever is in Postgres for a
neighborhood *is* its latest scrape, because a new load always deletes the old
one for that same neighborhood and date first, and the Dagster asset that
calls this only ever loads the partition it was just handed.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Iterator, Sequence

import geopandas as gpd
import pandas as pd

from urban_rag.rag.pgvector import PgSettings, PostgresUnavailable

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
    street_id_column: str = "COTE_RUE_ID",
    street_name_column: str = "NOM_VOIE",
) -> int:
    """Replace this borough's rows in `rag.streets` with ``frame``.

    ``frame`` is one borough's slice of the geobase double, so the rows are
    street *sides* rather than centre lines: `COTE_RUE_ID` is unique across the
    island (91,546 of 91,546 in the first snapshot), which makes it as real a
    natural key as Infolot's lot number, and the same `ON CONFLICT ... DO
    NOTHING` applies for the same reason.

    ``street_name_column`` gets a column of its own rather than a slot in
    `attributes` because it is what a frontage row is read *for* - "22 m on
    Rue Jarry" is the answer, and digging it back out of jsonb at every read
    would be work done in the wrong place. The measure is `ST_Length` rather
    than `ST_Area`: these are lines, and a line has no area to record.
    """
    return _replace_partition(
        connection,
        "rag.streets",
        frame,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
        natural_key_column=street_id_column,
        natural_key_target="cote_rue_id",
        extra_text_columns=(("street_name", street_name_column),),
        measure_column="length_m",
        measure_function="ST_Length",
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
                    scrape_date,
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
    """(Re)compute `rag.building_lots` for one (neighborhood, scrape_date).

    Assumes `load_lots`/`load_buildings` already landed this partition's rows
    in `rag.lots`/`rag.buildings` - the join is `ON l.neighborhood =
    b.neighborhood AND l.scrape_date = b.scrape_date`, so a stale lot from a
    different date simply cannot match.
    """
    cursor = connection.cursor()
    cursor.execute(
        "DELETE FROM rag.building_lots WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    cursor.execute(
        """
        INSERT INTO rag.building_lots (
            building_uid, lot_uid, neighborhood, scrape_date,
            building_area_m2, intersection_area_m2, pct_of_building, geom
        )
        SELECT
            b.building_uid,
            l.lot_uid,
            b.neighborhood,
            b.scrape_date,
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
    )
    inserted = max(cursor.rowcount, 0)

    cursor.execute(
        """
        SELECT count(DISTINCT building_uid), COALESCE(sum(intersection_area_m2), 0)
        FROM rag.building_lots
        WHERE neighborhood = %s AND scrape_date = %s::date
        """,
        [neighborhood, scrape_date],
    )
    buildings_matched, total_area_m2 = cursor.fetchone()
    return {
        "intersections": inserted,
        "buildings_matched": int(buildings_matched),
        "total_area_m2": float(total_area_m2),
    }


def compute_lot_features(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, object]:
    """(Re)compute `rag.lot_features` for one (neighborhood, scrape_date).

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
    cursor.execute(
        "DELETE FROM rag.lot_features WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    cursor.execute(
        """
        INSERT INTO rag.lot_features (
            lot_uid, feature_uid, source_table, feature_id, neighborhood,
            scrape_date, lot_area_m2, overlap_area_m2, pct_of_lot, geom
        )
        SELECT
            l.lot_uid,
            f.feature_uid,
            f.source_table,
            f.feature_id,
            l.neighborhood,
            l.scrape_date,
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
    )
    inserted = max(cursor.rowcount, 0)

    cursor.execute(
        """
        SELECT count(DISTINCT lot_uid), count(DISTINCT feature_uid),
               count(DISTINCT source_table)
        FROM rag.lot_features
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
        "lot_features": inserted,
        "lots_matched": int(lots_matched),
        "features_matched": int(features_matched),
        "layers": int(layers),
        "num_lots": int(num_lots),
    }


#: How far from a street side a lot boundary may be and still count as facing
#: it, in metres. The geobase double is drawn along the curb and sidewalk
#: limits, and a lot line sits behind those - close, but not on them, and the
#: city publishes the layer "à titre indicatif" rather than to survey accuracy.
#: Three metres is wide enough to cross a sidewalk and the publisher's own
#: error, and narrow enough that the first three metres of each *side* boundary
#: are the only corner the measure over-counts.
DEFAULT_FRONTAGE_BUFFER_M = 3.0


def compute_lot_frontage(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
    buffer_m: float = DEFAULT_FRONTAGE_BUFFER_M,
) -> dict[str, object]:
    """(Re)compute `rag.lot_frontage` for one (neighborhood, scrape_date).

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
    Frontage is a length along the parcel's edge, so the left-hand side is
    `ST_Boundary(l.geom)` and the intersection is a linework clip.
    `ST_CollectionExtract(..., 2)` then drops the points where a boundary only
    grazes the buffer's edge, which are intersections but not frontage.

    The buffer is in *metres* via `geography`, not in degrees: `ST_Buffer` on a
    4326 geometry takes its distance in degrees, where 3 would be some 300 km.
    It is a parameter rather than a constant because it is a judgement about
    how far a lot line sits behind a curb line, and `buffer_m` is written onto
    every row so a table can always be read back against the cutoff that
    produced it.

    ``frontage_rank`` is 1 for the longest frontage a lot has, which is the
    column to filter on when a question wants *the* street a lot fronts on
    rather than every street it touches. A corner lot legitimately has two.

    Assumes `load_lots` and `load_streets` have already landed this partition's
    rows - the join is `ON s.neighborhood = l.neighborhood AND s.scrape_date =
    l.scrape_date`, so a street side from another date cannot match a lot from
    this one. `rag.lots` is loaded by `building_lot_intersections`, not here:
    two assets loading the same table from the same file in two transactions is
    the race that asset's docstring exists to describe.
    """
    cursor = connection.cursor()
    cursor.execute(
        "DELETE FROM rag.lot_frontage WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    cursor.execute(
        """
        WITH buffered AS (
            -- Buffered once per street side rather than once per candidate
            -- pair, and in metres through `geography` - see the docstring.
            SELECT street_uid, cote_rue_id, street_name,
                   ST_Buffer(geography(geom), %(buffer_m)s::double precision)::geometry AS geom
            FROM rag.streets
            WHERE neighborhood = %(neighborhood)s
              AND scrape_date = %(scrape_date)s::date
        )
        INSERT INTO rag.lot_frontage (
            lot_uid, street_uid, cote_rue_id, street_name, neighborhood,
            scrape_date, buffer_m, frontage_m, lot_perimeter_m,
            pct_of_perimeter, frontage_rank, geom
        )
        SELECT
            lot_uid, street_uid, cote_rue_id, street_name, neighborhood,
            scrape_date, %(buffer_m)s::double precision, frontage_m, lot_perimeter_m,
            CASE WHEN lot_perimeter_m > 0
                 THEN 100.0 * frontage_m / lot_perimeter_m
                 ELSE 0.0
            END,
            frontage_rank,
            geom
        FROM (
            SELECT
                l.lot_uid,
                s.street_uid,
                s.cote_rue_id,
                s.street_name,
                l.neighborhood,
                l.scrape_date,
                ST_Length(geography(faces.geom)) AS frontage_m,
                ST_Length(geography(ST_Boundary(l.geom))) AS lot_perimeter_m,
                row_number() OVER (
                    PARTITION BY l.lot_uid
                    -- street_uid only to make the order total, so a re-run
                    -- ranks two exactly equal frontages the same way twice.
                    ORDER BY ST_Length(geography(faces.geom)) DESC, s.street_uid
                ) AS frontage_rank,
                faces.geom
            FROM rag.lots l
            -- The buffered side, not the side itself: the GiST index on
            -- rag.lots.geom is what this predicate rides, and the buffer is
            -- already materialized by the CTE above.
            JOIN buffered s ON ST_Intersects(l.geom, s.geom)
            -- Computed once via LATERAL rather than three times inline: the
            -- clip is the expensive part of this query, ST_Length is not.
            CROSS JOIN LATERAL (
                SELECT ST_CollectionExtract(
                    ST_Intersection(ST_Boundary(l.geom), s.geom), 2
                ) AS geom
            ) AS faces
            WHERE l.neighborhood = %(neighborhood)s
              AND l.scrape_date = %(scrape_date)s::date
              AND NOT ST_IsEmpty(faces.geom)
              -- A boundary that only grazes the buffer clips to zero-length
              -- linework, which is a touch rather than a frontage.
              AND ST_Length(geography(faces.geom)) > 0
        ) AS measured
        """,
        {
            "neighborhood": neighborhood,
            "scrape_date": scrape_date,
            "buffer_m": float(buffer_m),
        },
    )
    inserted = max(cursor.rowcount, 0)

    cursor.execute(
        """
        SELECT count(DISTINCT lot_uid), count(DISTINCT street_uid),
               COALESCE(sum(frontage_m), 0), COALESCE(max(frontage_m), 0)
        FROM rag.lot_frontage
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
        "SELECT count(*) FROM rag.streets WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    (num_streets,) = cursor.fetchone()

    return {
        "frontages": inserted,
        "lots_matched": int(lots_matched),
        "streets_matched": int(streets_matched),
        "total_frontage_m": float(total_frontage_m),
        "max_frontage_m": float(max_frontage_m),
        "num_lots": int(num_lots),
        "num_streets": int(num_streets),
        "buffer_m": float(buffer_m),
    }


class MissingRelation(RuntimeError):
    """A table or view a computation here reads has not been created yet.

    Distinct from letting psycopg raise `relation "x" does not exist`: every
    table this module touches belongs to hbu_infra, so the useful message names
    the .sql file to apply rather than the identifier that failed to resolve.
    """


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
    ("rag.building_lots", "sql/004_building_lots.sql"),
    ("rag.lot_frontage", "sql/008_lot_frontage.sql"),
    # A view, and the one most likely to be missing: 006 carries a
    # `-- requires: rag.chunks` header, so `db.py init` skips it on a database
    # that has never held a corpus and it only lands on the *next* init after
    # document_index has run.
    ("rag.lot_documents", "sql/006_lot_documents.sql"),
    ("rag.lot_profiles", "sql/009_lot_profiles.sql"),
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
    """(Re)compute `rag.lot_profiles` for one (neighborhood, scrape_date).

    One row per lot in the borough - every lot, not a selection of them. Three
    joins that each hold one row per (lot x something) are collapsed onto that
    grain and land side by side:

    * `rag.building_lots` -> `num_buildings`, `built_area_m2`, `category`
    * `rag.lot_frontage`  -> `primary_*` and `secondary_*`, `num_frontages`
    * `rag.lot_documents` -> `doc_*` and the `documents` array

    All three arrive by LEFT JOIN, which is the whole design: a lot no building
    touches, a lot facing no street and a lot no document covers are each a
    real answer to the question this table is read for, and an inner join would
    delete exactly those rows. The counts are `COALESCE`d to 0 rather than left
    NULL - "no building stands here" is a measurement, not a gap - while the
    frontage *measures* stay NULL, because a lot with no frontage row was not
    measured at 0 m, it was not measured at all.

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

    # Ahead of the DELETE so a malformed envelope costs nothing: the partition
    # is only torn down once there is something to rebuild it from.
    num_envelopes_staged = _stage_lot_envelopes(cursor, zoning_envelopes)

    cursor.execute(
        "DELETE FROM rag.lot_profiles WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    cursor.execute(
        """
        INSERT INTO rag.lot_profiles (
            lot_uid, lot_number, neighborhood, scrape_date, lot_area_m2,
            has_building, num_buildings, built_area_m2, built_pct_of_lot,
            largest_building_area_m2, category, max_built_area_m2,
            num_frontages, total_frontage_m,
            primary_frontage_m, primary_street_name, primary_cote_rue_id,
            secondary_frontage_m, secondary_street_name, secondary_cote_rue_id,
            frontage_buffer_m,
            num_documents, doc_id, doc_url, doc_title, doc_source_table,
            doc_pct_of_lot, documents,
            num_zoning_envelopes, zoning_envelopes,
            vacancy_rates, overall_vacancy_rate_pct,
            average_rents, overall_average_rent_cad,
            construction_costs,
            underground_stall_cost_low_cad, underground_stall_cost_high_cad,
            above_grade_stall_cost_low_cad, above_grade_stall_cost_high_cad,
            condo_cost_low_cad_sqft, condo_cost_high_cad_sqft,
            geom
        )
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
              FROM rag.building_lots bl
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
              FROM rag.lot_frontage f
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
        envelopes AS (
            -- Keyed on lot_number, not lot_uid: see `_stage_lot_envelopes`.
            -- Ordered by the ordinal the caller staged, which is the order
            -- silver/lot_zoning_envelopes decided - the zone covering most of
            -- the lot first - rather than one re-derived from inside the jsonb.
            SELECT e.lot_number,
                   count(*) AS num_zoning_envelopes,
                   jsonb_agg(e.envelope ORDER BY e.ordinal) AS zoning_envelopes
              FROM {envelope_staging} e
             GROUP BY e.lot_number
        )
        SELECT
            l.lot_uid,
            l.lot_number,
            l.neighborhood,
            l.scrape_date,
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
    )
    inserted = max(cursor.rowcount, 0)

    cursor.execute(
        """
        SELECT category, count(*), COALESCE(sum(lot_area_m2), 0)
        FROM rag.lot_profiles
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
               max(condo_cost_high_cad_sqft)
        FROM rag.lot_profiles
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
        overall_vacancy_rate_pct,
        overall_average_rent_cad,
        underground_stall_cost_low_cad,
        underground_stall_cost_high_cad,
        above_grade_stall_cost_low_cad,
        above_grade_stall_cost_high_cad,
        condo_cost_low_cad_sqft,
        condo_cost_high_cad_sqft,
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
        "profiles": inserted,
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


#: `rag.building_lots`, minus its geometry, qualified by the alias each column
#: comes from. `lot_number` is joined in from `rag.lots` because the `*_uid`
#: columns are bigserials a reload mints again - on its own the parquet would
#: carry no key that survives one.
_BUILDING_LOT_COLUMNS = (
    "bl.building_lot_uid",
    "bl.building_uid",
    "bl.lot_uid",
    "l.lot_number",
    "bl.neighborhood",
    "bl.scrape_date",
    "bl.building_area_m2",
    "bl.intersection_area_m2",
    "bl.pct_of_building",
)

#: `rag.lot_features`, minus its geometry. `source_table` and `feature_id` are
#: the pair `rag.chunks` cites, so the *feature* side already carries a durable
#: key; `lot_number` is joined in from `rag.lots` to give the lot side one too,
#: for the same reason it is in `_BUILDING_LOT_COLUMNS` - `lot_uid` is a
#: bigserial a reload mints again, so on its own the parquet would carry no lot
#: key that survives one, and `lot_zoning_envelopes` joins on exactly that.
_LOT_FEATURE_COLUMNS = (
    "lf.lot_feature_uid",
    "lf.lot_uid",
    "l.lot_number",
    "lf.feature_uid",
    "lf.source_table",
    "lf.feature_id",
    "lf.neighborhood",
    "lf.scrape_date",
    "lf.lot_area_m2",
    "lf.overlap_area_m2",
    "lf.pct_of_lot",
)

#: `rag.lot_frontage`, minus its geometry. `lot_number` is joined in from
#: `rag.lots` for the same reason it is in `_BUILDING_LOT_COLUMNS` - the
#: `*_uid` columns are bigserials a reload mints again; `cote_rue_id` is the
#: street side's own durable key and needs no such join.
_LOT_FRONTAGE_COLUMNS = (
    "f.lot_frontage_uid",
    "f.lot_uid",
    "l.lot_number",
    "f.street_uid",
    "f.cote_rue_id",
    "f.street_name",
    "f.neighborhood",
    "f.scrape_date",
    "f.buffer_m",
    "f.frontage_m",
    "f.lot_perimeter_m",
    "f.pct_of_perimeter",
    "f.frontage_rank",
)

#: `rag.lot_profiles`, minus its geometry, in the order a reader scans them:
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
    """This partition's `rag.building_lots` rows, as a GeoDataFrame."""
    return _fetch_partition(
        connection,
        _BUILDING_LOT_COLUMNS,
        """
        FROM rag.building_lots bl
        JOIN rag.lots l ON l.lot_uid = bl.lot_uid
        WHERE bl.neighborhood = %s AND bl.scrape_date = %s::date
        ORDER BY bl.building_uid, bl.lot_uid
        """,
        [neighborhood, scrape_date],
        date_column="bl.scrape_date",
        geometry_column="bl.geom",
    )


def fetch_lot_features(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `rag.lot_features` rows, as a GeoDataFrame."""
    return _fetch_partition(
        connection,
        _LOT_FEATURE_COLUMNS,
        """
        FROM rag.lot_features lf
        JOIN rag.lots l ON l.lot_uid = lf.lot_uid
        WHERE lf.neighborhood = %s AND lf.scrape_date = %s::date
        ORDER BY lf.lot_uid, lf.feature_uid
        """,
        [neighborhood, scrape_date],
        date_column="lf.scrape_date",
        geometry_column="lf.geom",
    )


def fetch_lot_frontage(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `rag.lot_frontage` rows, longest frontage first.

    Ordered in SQL rather than left to the reader, so the parquet itself
    answers "which lots have the most street" by being read from the top.
    """
    return _fetch_partition(
        connection,
        _LOT_FRONTAGE_COLUMNS,
        """
        FROM rag.lot_frontage f
        JOIN rag.lots l ON l.lot_uid = f.lot_uid
        WHERE f.neighborhood = %s AND f.scrape_date = %s::date
        ORDER BY f.frontage_m DESC, l.lot_number, f.street_uid
        """,
        [neighborhood, scrape_date],
        date_column="f.scrape_date",
        geometry_column="f.geom",
    )


def fetch_lot_profiles(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    """This partition's `rag.lot_profiles` rows, as a GeoDataFrame.

    Ordered by lot number rather than by any of the measures: this one is the
    borough's whole inventory, so the order a reader wants is the cadastre's
    own. `rag.lot_frontage` sorts by frontage because it is read for the top of
    that list; this is read for a named parcel.
    """
    return _fetch_partition(
        connection,
        _LOT_PROFILE_COLUMNS,
        """
        FROM rag.lot_profiles
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

    `str(nan)` is `"nan"`, which would land in `rag.streets.street_name` as a
    street called "nan" rather than as the absent name it is - and in a NOT
    NULL key column, as a row that looks loaded and joins to nothing.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)


def _replace_partition(
    connection: "Connection",
    table: str,
    frame: gpd.GeoDataFrame,
    *,
    neighborhood: str,
    scrape_date: str,
    natural_key_column: str | None,
    natural_key_target: str | None,
    extra_text_columns: tuple[tuple[str, str], ...] = (),
    measure_column: str = "area_m2",
    measure_function: str = "ST_Area",
) -> int:
    """Delete ``table``'s rows for this partition, then COPY ``frame`` in.

    Geometry travels as plain WKB bytes rather than through a registered
    PostGIS type - psycopg has no built-in adapter for `geometry`, and
    `ST_GeomFromWKB`/`ST_Multi` in the final INSERT is simpler than teaching it
    one. A staging temp table is the landing spot because a column typed
    `geometry(MultiPolygon, 4326)` rejects a bare Polygon by typmod; `ST_Multi`
    in the INSERT is what normalizes that, not the COPY itself.

    ``extra_text_columns`` are ``(target column, frame column)`` pairs lifted
    out of `attributes` into columns of their own - `rag.streets.street_name`
    is the one caller that needs it. ``measure_function``/``measure_column``
    are what a row's one derived number is: `ST_Area` into `area_m2` for the
    polygon tables, `ST_Length` into `length_m` for the line one, since a
    street side has no area and a lot no length worth recording.
    """
    if (natural_key_column is None) != (natural_key_target is None):
        raise ValueError(
            "natural_key_column and natural_key_target must be given together"
        )
    missing = [source for _, source in extra_text_columns if source not in frame.columns]
    if missing:
        raise ValueError(f"{table}: no {', '.join(missing)} column(s) in the frame")

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
    # The natural key and the lifted columns are all text, and all travel
    # ahead of the partition keys, so one list covers the DDL, the COPY header
    # and the INSERT's select list below.
    text_targets = ([natural_key_target] if natural_key_target else []) + [
        target for target, _ in extra_text_columns
    ]
    text_sources = ([natural_key_column] if natural_key_column else []) + [
        source for _, source in extra_text_columns
    ]
    text_ddl = "".join(f"{target} text, " for target in text_targets)
    cursor.execute(
        f"CREATE TEMP TABLE {staging} "
        f"({text_ddl}neighborhood text, scrape_date date, "
        "attributes jsonb, geom bytea) ON COMMIT DROP"
    )

    geometry_name = frame.geometry.name
    exclude = set(_ALWAYS_EXCLUDED) | {geometry_name} | set(text_sources)
    attribute_columns = [c for c in frame.columns if c not in exclude]
    attrs = (
        json.loads(frame[attribute_columns].to_json(orient="records", date_format="iso"))
        if attribute_columns
        else [{}] * len(frame)
    )

    columns = text_targets + ["neighborhood", "scrape_date", "attributes", "geom"]
    types = ["text"] * len(text_targets) + ["text", "date", "jsonb", "bytea"]

    inserted = 0
    statement = f"COPY {staging} ({', '.join(columns)}) FROM STDIN (FORMAT BINARY)"
    with cursor.copy(statement) as copy:
        copy.set_types(types)
        for index, row in enumerate(frame.itertuples(index=False)):
            geometry = getattr(row, geometry_name)
            if geometry is None or geometry.is_empty:
                continue
            values: list[Any] = [
                _as_text(getattr(row, source)) for source in text_sources
            ]
            values += [
                neighborhood,
                scrape_date,
                Jsonb(attrs[index]),
                shapely.wkb.dumps(geometry),
            ]
            copy.write_row(values)
            inserted += 1

    key_list = "".join(f"{target}, " for target in text_targets)
    conflict = (
        f"ON CONFLICT ({natural_key_target}, scrape_date) DO NOTHING"
        if natural_key_target
        else ""
    )
    cursor.execute(
        f"""
        INSERT INTO {table} (
            {key_list}neighborhood, scrape_date, {measure_column}, attributes, geom
        )
        SELECT
            {key_list}neighborhood,
            scrape_date,
            {measure_function}(geography(ST_Multi(ST_SetSRID(ST_GeomFromWKB(geom), 4326)))),
            attributes,
            ST_Multi(ST_SetSRID(ST_GeomFromWKB(geom), 4326))
        FROM {staging}
        {conflict}
        """
    )
    return inserted
