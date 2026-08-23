"""Loading the latest lots and buildings into Postgres/PostGIS, the spatial
join between them, and the lots that join finds nothing on.

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

`rag.vacant_lots` reads that join back the other way round. `building_lots`
answers "what is on this lot"; the rows it does *not* have are the interesting
ones for a highest-and-best-use question, and a lot carrying nothing but a
shed is the same answer as a lot carrying nothing at all. Both are selected
here by one predicate over the clipped areas - see `compute_vacant_lots`.

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
from typing import TYPE_CHECKING, Any, Iterator

import geopandas as gpd

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


#: The categories `compute_vacant_lots` sorts a candidate lot into. Kept here
#: rather than only in SQL because the asset reports one count per category and
#: a missing key would silently read as zero.
VACANT_LOT_CATEGORIES: tuple[str, ...] = (
    "no_building",
    "shed_only",
    "building_sliver",
)

#: Default cutoff for "there is effectively nothing built here", in square
#: metres of footprint standing on the lot. A garden shed is 10-30 m2 and a
#: detached garage 30-60, so 30 keeps the shed and drops the garage.
DEFAULT_MAX_BUILT_AREA_M2 = 30.0


def compute_vacant_lots(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
    max_built_area_m2: float = DEFAULT_MAX_BUILT_AREA_M2,
) -> dict[str, object]:
    """(Re)compute `rag.vacant_lots` for one (neighborhood, scrape_date).

    A lot is a candidate when the footprint area standing on it is at most
    ``max_built_area_m2`` - which is one predicate covering both halves of the
    question, since a lot no building intersects has 0 m2 built on it and 0 is
    under any threshold.

    The area compared is the *clipped* `intersection_area_m2` summed over the
    lot, not the whole area of the buildings that overlap it: a warehouse
    straddling the boundary contributes only the slice actually inside, which
    is what "how much of this lot is built on" means. That is also why the
    category is not simply vacant-or-shed. Three cases fall out, and they are
    different things to a reader:

    * `no_building` - nothing intersects the lot at all.
    * `shed_only`   - something does, and every building overlapping it is
                      itself small enough to be a shed.
    * `building_sliver` - something does, the built area here is still under
                      the threshold, but the building it belongs to is large.
                      A corner of the neighbour's triplex crossing the
                      cadastral line, in other words: the lot is empty in
                      substance, but calling it a shed would be wrong, and
                      it is as often a footprint/cadastre alignment artifact
                      as a real encroachment.

    Assumes `compute_intersections` has already run for this partition, which
    is what puts the rows in `rag.building_lots` that the lateral counts - a
    lot looks empty either way, so this is a dependency on the asset, not
    something the SQL can check for itself.
    """
    threshold = float(max_built_area_m2)
    if threshold < 0:
        raise ValueError(
            f"max_built_area_m2 must not be negative, got {max_built_area_m2!r}"
        )

    cursor = connection.cursor()
    cursor.execute(
        "DELETE FROM rag.vacant_lots WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    cursor.execute(
        """
        INSERT INTO rag.vacant_lots (
            lot_uid, lot_number, neighborhood, scrape_date, category,
            lot_area_m2, built_area_m2, built_pct_of_lot, num_buildings,
            largest_building_area_m2, max_built_area_m2, geom
        )
        SELECT
            l.lot_uid,
            l.lot_number,
            l.neighborhood,
            l.scrape_date,
            CASE
                WHEN built.num_buildings = 0 THEN 'no_building'
                WHEN built.largest_building_area_m2 > %(threshold)s THEN 'building_sliver'
                ELSE 'shed_only'
            END,
            l.area_m2,
            built.built_area_m2,
            CASE WHEN l.area_m2 > 0
                 THEN 100.0 * built.built_area_m2 / l.area_m2
                 ELSE NULL
            END,
            built.num_buildings,
            built.largest_building_area_m2,
            %(threshold)s,
            l.geom
        FROM rag.lots l
        -- An aggregate over zero rows rather than a LEFT JOIN plus GROUP BY:
        -- the lot with no building at all is the case this asset exists for,
        -- and it has to survive the join to be seen. count(*) is 0 and the
        -- COALESCEd sums are 0.0 for exactly that lot.
        CROSS JOIN LATERAL (
            SELECT
                count(*) AS num_buildings,
                COALESCE(sum(bl.intersection_area_m2), 0.0) AS built_area_m2,
                COALESCE(max(bl.building_area_m2), 0.0) AS largest_building_area_m2
            FROM rag.building_lots bl
            WHERE bl.lot_uid = l.lot_uid
              AND bl.neighborhood = l.neighborhood
              AND bl.scrape_date = l.scrape_date
        ) AS built
        WHERE l.neighborhood = %(neighborhood)s
          AND l.scrape_date = %(scrape_date)s::date
          AND built.built_area_m2 <= %(threshold)s
        """,
        {
            "neighborhood": neighborhood,
            "scrape_date": scrape_date,
            "threshold": threshold,
        },
    )
    inserted = max(cursor.rowcount, 0)

    cursor.execute(
        """
        SELECT category, count(*), COALESCE(sum(lot_area_m2), 0)
        FROM rag.vacant_lots
        WHERE neighborhood = %s AND scrape_date = %s::date
        GROUP BY category
        """,
        [neighborhood, scrape_date],
    )
    counted: dict[str, int] = {}
    total_lot_area_m2 = 0.0
    for category, count, area in cursor.fetchall():
        counted[category] = int(count)
        total_lot_area_m2 += float(area)

    # The denominator: without it "412 candidates" says nothing about whether
    # the borough is half empty or the threshold is wrong.
    cursor.execute(
        "SELECT count(*) FROM rag.lots WHERE neighborhood = %s AND scrape_date = %s::date",
        [neighborhood, scrape_date],
    )
    (num_lots,) = cursor.fetchone()

    return {
        "candidates": inserted,
        "num_lots": int(num_lots),
        "by_category": {name: counted.get(name, 0) for name in VACANT_LOT_CATEGORIES},
        "total_lot_area_m2": total_lot_area_m2,
    }


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

    Geometry travels as plain WKB bytes rather than through a registered
    PostGIS type - psycopg has no built-in adapter for `geometry`, and
    `ST_GeomFromWKB`/`ST_Multi` in the final INSERT is simpler than teaching it
    one. A staging temp table is the landing spot because a column typed
    `geometry(MultiPolygon, 4326)` rejects a bare Polygon by typmod; `ST_Multi`
    in the INSERT is what normalizes that, not the COPY itself.
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
    key_column_ddl = f"{natural_key_target} text, " if natural_key_target else ""
    cursor.execute(
        f"CREATE TEMP TABLE {staging} "
        f"({key_column_ddl}neighborhood text, scrape_date date, "
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

    columns = ([natural_key_target] if natural_key_target else []) + [
        "neighborhood",
        "scrape_date",
        "attributes",
        "geom",
    ]
    types = (["text"] if natural_key_target else []) + ["text", "date", "jsonb", "bytea"]

    inserted = 0
    statement = f"COPY {staging} ({', '.join(columns)}) FROM STDIN (FORMAT BINARY)"
    with cursor.copy(statement) as copy:
        copy.set_types(types)
        for index, row in enumerate(frame.itertuples(index=False)):
            geometry = getattr(row, geometry_name)
            if geometry is None or geometry.is_empty:
                continue
            values: list[Any] = []
            if natural_key_column:
                values.append(str(getattr(row, natural_key_column)))
            values += [
                neighborhood,
                scrape_date,
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
        INSERT INTO {table} ({key_list}neighborhood, scrape_date, area_m2, attributes, geom)
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
