"""Loading the latest lots and buildings into Postgres/PostGIS, and the
spatial join between them.

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
