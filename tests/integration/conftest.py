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

#: The files that create what `compute_lot_frontage` reads and writes. Applied
#: in order and idempotent - every one is `CREATE ... IF NOT EXISTS`. The roles
#: file is not among them: each table's grant block already degrades to a
#: notice when `urban_rag` does not exist, which on a throwaway database it
#: does not.
SCHEMA_FILES = (
    "002_spatial.sql",
    "003_warehouse.sql",
    "007_silver_streets.sql",
    "008_silver_lot_frontage.sql",
)

#: The slice of VSMPE these tests measure: every lot within 120 m of lot
#: 3 790 556, and every street side those lots could reach. Carved from the
#: real `bronze/neighborhood_lots` and `silver/neighborhood_streets` parquet so
#: the geometry is the publisher's, not a fixture author's idea of it.
FIXTURE_DIR = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "frontage"

#: The partition the fixture is loaded into. A date of its own so a run against
#: a database that already holds real rows cannot collide with them.
NEIGHBORHOOD = "VSMPE"
SCRAPE_DATE = "2000-01-01"


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
        for name in SCHEMA_FILES:
            conn.execute((sql_dir / name).read_text(encoding="utf-8"))
        yield conn


@pytest.fixture(scope="session")
def loaded(connection):
    """The fixture slice, in `rag.lots` and `silver.neighborhood_streets`.

    Loaded the way the two owning assets load it - `building_lot_intersections`
    for the cadastre, `neighborhood_streets` for the sides - because
    `compute_lot_frontage` joins those two tables and nothing else.
    """
    lots = gpd.read_parquet(FIXTURE_DIR / "lots.parquet")
    sides = gpd.read_parquet(FIXTURE_DIR / "street_sides.parquet")

    with connection.transaction():
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM rag.lots WHERE neighborhood = %s AND scrape_date = %s::date",
            [NEIGHBORHOOD, SCRAPE_DATE],
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
                    SCRAPE_DATE,
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
            [NEIGHBORHOOD, SCRAPE_DATE],
        )
        cursor.executemany(
            "INSERT INTO silver.neighborhood_streets "
            "(scrape_date, neighborhood, cote_rue_id, street_name, length_m, geom) "
            "VALUES (%s::date, %s, %s, %s, %s, "
            "ST_Multi(ST_GeomFromWKB(decode(%s, 'hex'), 4326)))",
            [
                (
                    SCRAPE_DATE,
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
