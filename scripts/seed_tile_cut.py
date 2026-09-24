"""Check the grid agrees with itself, then build the cut to check in.

Two jobs, in this order, because the second is worthless if the first fails.

**Does the SQL address the same cell as the Python?** `warehouse.quadkey`
(hbu_infra/sql/028_cell_key.sql), `urban_rag.tile_grid.quadkey_of` and
`postgis._cell_x_sql` are three spellings of one grid. The unit tests compare
the first two as arithmetic; this compares them on the rows actually in the
database, which is the only place a difference between Postgres's `asinh` and
Python's would show up.

**What cut does this cadastre want?** Reads every `cell_key`, subdivides to
`tile_cut.DEFAULT_BUDGET`, and prints the literal to paste into
`urban_rag.tile_cut`. It does **not** write anything - the cut is a checked-in
constant on purpose, because a cut that re-derived itself would re-key the
Dagster axis every time a lot was subdivided. See that module's docstring.

Read-only. Run it behind the tunnel:

    eval "$(make -s -C ../hbu_infra db-env ENV=dev)"
    export URBAN_RAG_PG_HOST=127.0.0.1 URBAN_RAG_PG_PORT=5433
    uv run python scripts/seed_tile_cut.py
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

load_dotenv()

from urban_rag import tile_cut, tile_grid  # noqa: E402
from urban_rag.postgis import connect  # noqa: E402
from urban_rag.rag.pgvector import PgSettings  # noqa: E402

#: How many rows the agreement check reads. A few thousand is plenty - a
#: disagreement is systematic, not occasional.
SAMPLE = 5_000


def check_agreement(cursor, sample: int) -> int:
    """Compare the stored `cell_key` against the Python for the same point."""
    cursor.execute(
        """
        SELECT cell_key,
               ST_X(ST_PointOnSurface(geom)),
               ST_Y(ST_PointOnSurface(geom))
          FROM rag.lots
         WHERE cell_key IS NOT NULL
         ORDER BY lot_uid
         LIMIT %s
        """,
        [sample],
    )
    rows = cursor.fetchall()
    mismatched = 0
    for stored, lon, lat in rows:
        expected = tile_grid.quadkey_of(float(lon), float(lat))
        if stored != expected:
            mismatched += 1
            if mismatched <= 5:
                print(f"    {lon:.7f},{lat:.7f}  sql={stored}  python={expected}")
    print(f"  agreement: {len(rows) - mismatched}/{len(rows)} rows agree")
    return mismatched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--budget", type=int, default=tile_cut.DEFAULT_BUDGET)
    parser.add_argument("--sample", type=int, default=SAMPLE)
    args = parser.parse_args()

    with connect(PgSettings.from_env()) as connection:
        cursor = connection.cursor()

        cursor.execute(
            "SELECT count(*), count(cell_key), "
            "       min(length(cell_key)), max(length(cell_key)) FROM rag.lots"
        )
        total, addressed, shortest, longest = cursor.fetchone()
        print(f"rag.lots: {total} row(s), {addressed} addressed")
        if addressed == 0:
            print("  no cell_key at all - apply hbu_infra/sql/028_cell_key.sql first")
            return 2
        if (shortest, longest) != (tile_grid.BASE_CELL_ZOOM,) * 2:
            print(
                f"  !! cell_key lengths run {shortest}..{longest}, expected "
                f"{tile_grid.BASE_CELL_ZOOM} exactly - the backfill used a "
                "different zoom and every prefix would name the wrong cell"
            )
            return 2

        print(f"\nchecking {args.sample} row(s) against urban_rag.tile_grid")
        if check_agreement(cursor, args.sample):
            print(
                "  !! the SQL and the Python disagree. Do NOT seed a cut from "
                "this - fix 028_cell_key.sql first."
            )
            return 2

        print("\nreading every cell_key")
        cursor.execute(
            "SELECT cell_key FROM rag.lots WHERE cell_key IS NOT NULL"
        )
        keys = [row[0] for row in cursor.fetchall()]

    print(f"  {len(keys)} lot(s)")
    cut = tile_cut.build_cut(keys, budget=args.budget)
    report = tile_cut.cut_report(keys, cut=cut, budget=args.budget)

    depths = sorted({len(cell) for cell in cut})
    print(f"\ncut at budget={args.budget}: {len(cut)} cell(s), zooms {depths}")
    print(f"  largest holds {report['max_lots_in_a_tile']} lot(s)")
    print(f"  over budget:   {report['num_tiles_over_budget']}")

    print("\n--- paste into src/urban_rag/tile_cut.py ---")
    print(f"CUT_VERSION = 1")
    print("CUT: frozenset[str] = frozenset(")
    print("    (")
    for cell in sorted(cut):
        print(f'        "{cell}",')
    print("    )")
    print(")")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
