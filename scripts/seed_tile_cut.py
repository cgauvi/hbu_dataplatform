"""Check the grid agrees with itself, then build the cut to check in.

Two jobs, in this order, because the second is worthless if the first fails.

**Does the SQL address the same cell as the Python?** `warehouse.quadkey`
(hbu_infra/sql/028_cell_key.sql), `hbu_dataplatform.core.tile_grid.quadkey_of` and
`postgis._cell_x_sql` are three spellings of one grid. The unit tests compare
the first two as arithmetic; this compares them on the rows actually in the
database, which is the only place a difference between Postgres's `asinh` and
Python's would show up.

**What cut does this cadastre want?** Reads every `cell_key` with the borough
it was loaded for, subdivides to `tile_cut.DEFAULT_BUDGET`, names each cell's
city, and prints the literal to paste into `hbu_dataplatform.core.tile_cut`. It does
**not** write anything - the cut is a checked-in constant on purpose, because
a cut that re-derived itself would re-key the Dagster axis every time a lot
was subdivided. See that module's docstring.

**And it refuses to re-cut.** Adding a city is additive: its ground is under
cells nothing else uses, so every cell of the live cut comes back unchanged
and new ones appear beside them. A result that drops or splits a live cell is
the disruptive case that module describes, and this script stops rather than
printing it - `--allow-recut` is the deliberate override, for a migration that
has read the runbook.

Read-only. Run it behind the tunnel:

    eval "$(make -s -C ../hbu_infra db-env ENV=dev)"
    export URBAN_RAG_PG_HOST=127.0.0.1 URBAN_RAG_PG_PORT=5433
    uv run python scripts/seed_tile_cut.py
"""

from __future__ import annotations

import argparse
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv()

from hbu_dataplatform.core import tile_cut, tile_grid  # noqa: E402
from hbu_dataplatform.partitions.axes import city_of  # noqa: E402
from hbu_dataplatform.core.postgis import connect  # noqa: E402
from hbu_dataplatform.core.pg import PgSettings  # noqa: E402

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
    parser.add_argument(
        "--scrape-date",
        help="the snapshot to cut over (default: the latest in rag.lots) - one "
        "date only, or a borough loaded twice counts its lots twice",
    )
    parser.add_argument(
        "--allow-recut",
        action="store_true",
        help="print a cut that drops or splits a cell of the live one",
    )
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

        print(f"\nchecking {args.sample} row(s) against hbu_dataplatform.core.tile_grid")
        if check_agreement(cursor, args.sample):
            print(
                "  !! the SQL and the Python disagree. Do NOT seed a cut from "
                "this - fix 028_cell_key.sql first."
            )
            return 2

        if args.scrape_date is None:
            cursor.execute("SELECT max(scrape_date)::text FROM rag.lots")
            (args.scrape_date,) = cursor.fetchone()
        print(
            f"\nreading every cell_key of {args.scrape_date}, with the borough it "
            "was loaded for"
        )
        cursor.execute(
            "SELECT cell_key, neighborhood FROM rag.lots "
            "WHERE cell_key IS NOT NULL AND scrape_date = %s::date",
            [args.scrape_date],
        )
        rows = cursor.fetchall()

    keys = [row[0] for row in rows]
    print(f"  {len(keys)} lot(s)")
    cut = tile_cut.build_cut(keys, budget=args.budget)
    report = tile_cut.cut_report(keys, cut=cut, budget=args.budget)

    depths = sorted({len(cell) for cell in cut})
    print(f"\ncut at budget={args.budget}: {len(cut)} cell(s), zooms {depths}")
    print(f"  largest holds {report['max_lots_in_a_tile']} lot(s)")
    print(f"  over budget:   {report['num_tiles_over_budget']}")

    # A cell's city is the city of the boroughs its lots were loaded for.
    # More than one would mean two cities' ground under one cell, which the
    # grid makes impossible for the three loaded - they part high in the tree
    # - but a fourth city could sit next to one of them, and a cell that
    # straddled two would have no CRS to measure in.
    cities: dict[str, set[str]] = defaultdict(set)
    for key, neighborhood in rows:
        cities[tile_grid.cut_cell_of(key, cut)].add(str(city_of(neighborhood)))
    straddling = {cell: sorted(found) for cell, found in cities.items() if len(found) > 1}
    if straddling:
        print("  !! cell(s) holding lots of two cities - lower --budget until they split:")
        for cell, found in sorted(straddling.items()):
            print(f"     {cell}  {', '.join(found)}")
        return 2

    dropped = sorted(tile_cut.CUT - cut)
    added = sorted(cut - tile_cut.CUT)
    print(f"\nagainst the live cut (version {tile_cut.CUT_VERSION}):")
    print(f"  unchanged: {len(tile_cut.CUT) - len(dropped)}")
    print(f"  added:     {len(added)}")
    print(f"  dropped:   {len(dropped)}")
    if dropped and not args.allow_recut:
        print(
            "  !! this cut drops or splits live cell(s) - a re-cut, not an "
            "addition. Every partition under them would be re-keyed; see "
            "hbu_dataplatform.core.tile_cut on what that costs. Re-run with --allow-recut "
            "if that is the intent:"
        )
        for cell in dropped:
            print(f"     {cell}")
        return 2

    print("\n--- paste into src/hbu_dataplatform/core/tile_cut.py ---")
    print(f"CUT_VERSION = {tile_cut.CUT_VERSION + 1 if added or dropped else tile_cut.CUT_VERSION}")
    print("TILE_CITIES: dict[str, str] = {")
    by_city: dict[str, list[str]] = defaultdict(list)
    for cell in sorted(cut):
        by_city[next(iter(cities[cell]))].append(cell)
    for city in sorted(by_city):
        print(f"    # {city}")
        for cell in by_city[city]:
            marker = "  # new" if cell in added else ""
            print(f'    "{cell}": "{city}",{marker}')
    print("}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
