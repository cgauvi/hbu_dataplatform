"""A real PostGIS for the tests that need one.

Everything in `tests/unit` stubs the database out, and says why: the frontage
and building-lot joins are PostGIS statements, so a mock of them tests the
asset's plumbing and not the measure. That leaves the measure itself untested,
which is how a `buffer_m` too small to reach 90 % of a borough's lots survived
in `postgis.compute_lot_frontage` - every unit test passed, because none of
them ever intersected a lot with a street.

These tests close that gap by running the real SQL against a real PostGIS on a
small, committed slice of Villeray-Saint-Michel-Parc-Extension. They are opt-in
and skip when no database is configured, so `make test` stays offline.

Point them at a **throwaway** database - the schema is applied and the
partitions are truncated on the way in::

    docker run -d --name urban_postgis \\
        -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=urban \\
        -p 55432:5432 postgis/postgis:16-3.4-alpine

    URBAN_RAG_TEST_PG_URL=postgresql://postgres:postgres@localhost:55432/urban \\
        uv run pytest tests/integration

The tables are hbu_infra's, so the schema comes from its `sql/` tree - beside
this checkout by default, or wherever `URBAN_RAG_INFRA_SQL` points.
"""

from __future__ import annotations

import os
import pathlib

import geopandas as gpd
import pytest

psycopg = pytest.importorskip("psycopg")

#: The connection string. Absent, every test here skips.
DSN_ENV = "URBAN_RAG_TEST_PG_URL"

#: Where hbu_infra's `sql/` tree is, when it is not beside this checkout.
INFRA_ENV = "URBAN_RAG_INFRA_SQL"

#: The schemas the files below put their tables in. `000_roles.sql` is what
#: creates these in a real deployment, and it is deliberately not applied here
#: - it declares them `AUTHORIZATION urban_rag`, and that role does not exist
#: on a throwaway database. Creating them unowned is the one thing that file
#: does which these tests still need; its grants are not, because each table's
#: grant block already degrades to a notice when the role is absent.
SCHEMAS = ("rag", "silver", "gold", "warehouse")

#: The files that create what `compute_lot_frontage` reads and writes. Applied
#: in order and idempotent - every one is `CREATE ... IF NOT EXISTS`. The roles
#: file is not among them: each table's grant block already degrades to a
#: notice when `urban_rag` does not exist, which on a throwaway database it
#: does not. `SCHEMAS` above is what stands in for the rest of it.
SCHEMA_FILES = (
    "002_spatial.sql",
    "003_warehouse.sql",
    # The lot x feature clips `compute_lot_zone_pieces` cuts its pieces out of,
    # and the building x lot clips it measures each piece's footprint against.
    "004_silver_building_lots.sql",
    "005_silver_lot_features.sql",
    "007_silver_streets.sql",
    "008_silver_lot_frontage.sql",
    # What `compute_lot_buildable_setbacks` reads the margins from, and the
    # table it writes. Neither declares a foreign key on the frontage above, so
    # the order here is only the order the files are numbered in.
    "012_silver_zoning.sql",
    "015_silver_lot_buildable_setbacks.sql",
    # The ground each zone governs. Last because its own migration block
    # re-keys the gold tables, and those are not applied here at all - the
    # block is written to skip a table that is not there, which this ordering
    # is what exercises.
    "025_silver_lot_zone_pieces.sql",
)

#: The feature layer a zoning clip is filed under, and the one
#: `rag.documents.DOCUMENT_SOURCES` names. `silver.lot_features` holds every
#: layer in one table, so this is what separates a zone from a heritage sector
#: or a parking district.
ZONE_SOURCE_TABLE = "Reglement_urbanisme__VSP_REG_ZONE"

#: The slice of VSMPE these tests measure: every lot within 120 m of lot
#: 3 790 556, and every street side those lots could reach. Carved from the
#: real `bronze/neighborhood_lots` and `silver/neighborhood_streets` parquet so
#: the geometry is the publisher's, not a fixture author's idea of it.
FIXTURE_DIR = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "frontage"

#: The second slice: avenue Querbes between Ball and Saint-Roch, in
#: Parc-Extension. Carved the same way and for a different question - the
#: Chabot window above is about *measuring* a frontage, this one is about
#: *identifying* the parcels that are the street, which is what keeps a
#: roadway out of the highest-and-best-use inventory. See its README.
STREET_PARCEL_FIXTURE_DIR = (
    pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "street_parcels"
)

#: The partition each fixture is loaded into. A date of its own so a run
#: against a database that already holds real rows cannot collide with them,
#: and one per slice so the two never overwrite each other's partition - both
#: are the same borough, and `silver.lot_frontage` holds one row set per
#: (neighborhood, scrape_date).
NEIGHBORHOOD = "VSMPE"
SCRAPE_DATE = "2000-01-01"
STREET_PARCEL_SCRAPE_DATE = "2000-01-02"


def _infra_sql_dir() -> pathlib.Path:
    configured = os.environ.get(INFRA_ENV)
    if configured:
        return pathlib.Path(configured)
    return pathlib.Path(__file__).resolve().parents[2].parent / "hbu_infra" / "sql"


@pytest.fixture(scope="session")
def connection():
    """A connection to the configured database, with the schema applied."""
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        pytest.skip(f"{DSN_ENV} is not set - see tests/integration/conftest.py")

    sql_dir = _infra_sql_dir()
    if not sql_dir.is_dir():
        pytest.skip(f"hbu_infra sql/ not found at {sql_dir} - set {INFRA_ENV}")

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        for schema in SCHEMAS:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for name in SCHEMA_FILES:
            conn.execute((sql_dir / name).read_text(encoding="utf-8"))
        yield conn


@pytest.fixture(scope="session")
def loaded(connection):
    """The Chabot slice, in `rag.lots` and `silver.neighborhood_streets`."""
    return load_slice(connection, FIXTURE_DIR, SCRAPE_DATE)


@pytest.fixture(scope="session")
def loaded_street_parcels(connection):
    """The Querbes slice, in the same two tables under its own scrape date."""
    return load_slice(connection, STREET_PARCEL_FIXTURE_DIR, STREET_PARCEL_SCRAPE_DATE)


def load_slice(connection, fixture_dir: pathlib.Path, scrape_date: str):
    """One fixture slice, in `rag.lots` and `silver.neighborhood_streets`.

    Loaded the way the two owning assets load it - `building_lot_intersections`
    for the cadastre, `neighborhood_streets` for the sides - because
    `compute_lot_frontage` joins those two tables and nothing else.
    """
    lots = gpd.read_parquet(fixture_dir / "lots.parquet")
    sides = gpd.read_parquet(fixture_dir / "street_sides.parquet")

    with connection.transaction():
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM rag.lots WHERE neighborhood = %s AND scrape_date = %s::date",
            [NEIGHBORHOOD, scrape_date],
        )
        cursor.executemany(
            "INSERT INTO rag.lots "
            "(lot_number, neighborhood, scrape_date, area_m2, geom) VALUES "
            "(%s, %s, %s::date, %s, "
            "ST_Multi(ST_GeomFromWKB(decode(%s, 'hex'), 4326)))",
            [
                (
                    row["lot_number"],
                    NEIGHBORHOOD,
                    scrape_date,
                    float(row["area_m2"]),
                    row.geometry.wkb_hex,
                )
                for _, row in lots.iterrows()
            ],
        )

        # silver.neighborhood_streets is partitioned by neighborhood, and a
        # partitioned table rejects an insert with no partition to take it.
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS "
            f"silver.neighborhood_streets_{NEIGHBORHOOD.lower()} "
            f"PARTITION OF silver.neighborhood_streets "
            f"FOR VALUES IN ('{NEIGHBORHOOD}')"
        )
        cursor.execute(
            "DELETE FROM silver.neighborhood_streets "
            "WHERE neighborhood = %s AND scrape_date = %s::date",
            [NEIGHBORHOOD, scrape_date],
        )
        cursor.executemany(
            "INSERT INTO silver.neighborhood_streets "
            "(scrape_date, neighborhood, cote_rue_id, street_name, length_m, geom) "
            "VALUES (%s::date, %s, %s, %s, %s, "
            "ST_Multi(ST_GeomFromWKB(decode(%s, 'hex'), 4326)))",
            [
                (
                    scrape_date,
                    NEIGHBORHOOD,
                    str(row["cote_rue_id"]),
                    row["street_name"],
                    float(row["length_m"]),
                    row.geometry.wkb_hex,
                )
                for _, row in sides.iterrows()
            ],
        )

    return {"num_lots": len(lots), "num_streets": len(sides)}


def ensure_partition(connection, table: str, neighborhood: str = NEIGHBORHOOD) -> None:
    """A LIST partition for one borough, on a table that has none.

    Every table here is `PARTITION BY LIST (neighborhood)` and a partitioned
    table rejects an insert with no partition to take it. The real pipeline
    creates these through `urban_rag.warehouse.ensure_partition`; these tests
    write straight into the tables, so they have to make their own.
    """
    connection.cursor().execute(
        f"CREATE TABLE IF NOT EXISTS "
        f"{table.replace('.', '_')}_{neighborhood.lower()} "
        f"PARTITION OF {table} FOR VALUES IN ('{neighborhood}')"
    )


def load_zone_clips(connection, scrape_date: str, clips) -> None:
    """Zone clips into `silver.lot_features`, as the spatial join writes them.

    ``clips`` is an iterable of ``(lot_number, feature_id, geometry)`` in
    EPSG:4326, where the geometry is the piece of that lot the zone covers -
    which is exactly what `postgis.compute_lot_features` computes with
    ``ST_Intersection(lot, feature)``. Written directly rather than through
    that function because these tests want to *state* the split: a fixture that
    computed it from a synthetic zone polygon would be testing PostGIS's
    intersection rather than what `compute_lot_zone_pieces` does with one.

    The areas are measured here rather than passed in, so a clip and its
    `overlap_area_m2` cannot disagree - and `pct_of_lot` is taken against the
    parcel in `rag.lots`, which is what the cutoffs are applied to.
    """
    ensure_partition(connection, "silver.lot_features")
    cursor = connection.cursor()
    with connection.transaction():
        cursor.execute(
            "DELETE FROM silver.lot_features "
            "WHERE neighborhood = %s AND scrape_date = %s::date",
            [NEIGHBORHOOD, scrape_date],
        )
        cursor.executemany(
            """
            INSERT INTO silver.lot_features
                (scrape_date, neighborhood, lot_uid, source_table, feature_id,
                 lot_number, feature_uid, lot_area_m2, overlap_area_m2,
                 pct_of_lot, geom)
            SELECT %(scrape_date)s::date, %(neighborhood)s, l.lot_uid,
                   %(source_table)s, %(feature_id)s, l.lot_number,
                   NULL,
                   ST_Area(geography(l.geom)),
                   ST_Area(geography(clip.geom)),
                   CASE WHEN ST_Area(geography(l.geom)) > 0
                        THEN 100.0 * ST_Area(geography(clip.geom))
                                   / ST_Area(geography(l.geom))
                        ELSE 0.0
                   END,
                   clip.geom
              FROM rag.lots l
              CROSS JOIN LATERAL (
                  SELECT ST_GeomFromWKB(decode(%(wkb)s, 'hex'), 4326) AS geom
              ) clip
             WHERE l.neighborhood = %(neighborhood)s
               AND l.scrape_date = %(scrape_date)s::date
               AND l.lot_number = %(lot_number)s
            """,
            [
                {
                    "scrape_date": scrape_date,
                    "neighborhood": NEIGHBORHOOD,
                    "source_table": ZONE_SOURCE_TABLE,
                    "feature_id": feature_id,
                    "lot_number": lot_number,
                    "wkb": geometry.wkb_hex,
                }
                for lot_number, feature_id, geometry in clips
            ],
        )


def whole_lot_clips(connection, scrape_date: str, feature_id: str) -> None:
    """One clip per lot in the slice, covering the whole parcel.

    The unsplit case, and the one every test written before the piece grain
    assumes: a piece that *is* its lot, so `piece_area_m2` equals `lot_area_m2`
    and every measure downstream reads exactly as it did when the lot was the
    unit of work.
    """
    ensure_partition(connection, "silver.lot_features")
    cursor = connection.cursor()
    with connection.transaction():
        cursor.execute(
            "DELETE FROM silver.lot_features "
            "WHERE neighborhood = %s AND scrape_date = %s::date",
            [NEIGHBORHOOD, scrape_date],
        )
        cursor.execute(
            """
            INSERT INTO silver.lot_features
                (scrape_date, neighborhood, lot_uid, source_table, feature_id,
                 lot_number, lot_area_m2, overlap_area_m2, pct_of_lot, geom)
            SELECT %s::date, %s, l.lot_uid, %s, %s, l.lot_number,
                   ST_Area(geography(l.geom)), ST_Area(geography(l.geom)),
                   100.0, l.geom
              FROM rag.lots l
             WHERE l.neighborhood = %s AND l.scrape_date = %s::date
            """,
            [
                scrape_date, NEIGHBORHOOD, ZONE_SOURCE_TABLE, feature_id,
                NEIGHBORHOOD, scrape_date,
            ],
        )
