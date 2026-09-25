"""List the cells of the tile cut, and the ones a borough's lots fall in.

The tile axis is static - every cell of `urban_rag.tile_cut.CUT`, checked in
with the city it belongs to - so unlike `urban_rag.neighborhoods` there is
nothing to register. What an operator needs from the axis is two lists::

    python -m urban_rag.tiles list                    # every cell, by city
    python -m urban_rag.tiles of VSMPE 2026-09-01     # the cells VSMPE landed in

The second reads `rag.lots` and needs the database - it is the answer to
"which tile runs follow this borough's reload", and `scripts/materialize_borough.sh`
calls it for exactly that. It prints one cell per line so a shell can loop
over it.

The third creates every tile-axis table's leaf for every cell of a month,
ahead of the runs that write them::

    python -m urban_rag.tiles ensure 2026-09-01

It exists because of what creating a partition costs once cells run side by
side. `CREATE TABLE ... PARTITION OF` wants an exclusive lock on the parent,
and a run that creates its own leaf mid-transaction has usually already read
the parent - so two cells doing it at once each hold the lock the other needs
(it deadlocked on `lot_buildable_setbacks` the first morning), and one that
wins keeps the parent locked against every other cell until it commits, which
for setbacks is the length of the run. Created here, one short transaction per
leaf, the runs only ever find their leaf already there - `ensure_partition`'s
cheap path, which takes no lock at all.
"""

from __future__ import annotations

import argparse
import sys

from urban_rag import tile_cut
from urban_rag.partitions import city_of_tile


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="urban_rag.tiles", description="The cells of the tile cut."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="every cell of the cut, with its city")
    of = commands.add_parser("of", help="the cells a borough's loaded lots fall in")
    of.add_argument("neighborhood", metavar="NEIGHBORHOOD")
    of.add_argument("scrape_date", metavar="DATE")
    ensure = commands.add_parser(
        "ensure", help="create every tile table's leaf for every cell of a month"
    )
    ensure.add_argument("scrape_date", metavar="DATE")
    args = parser.parse_args(argv)

    if args.command == "ensure":
        return _ensure_leaves(args.scrape_date)

    if args.command == "list":
        print(f"cut version {tile_cut.CUT_VERSION}: {len(tile_cut.CUT)} cell(s)")
        for tile in sorted(tile_cut.CUT):
            print(f"  {tile:<19}  z{len(tile):<2}  {city_of_tile(tile)}")
        return 0

    from urban_rag.postgis import connect, tiles_of_neighborhood
    from urban_rag.rag.pgvector import PgSettings

    with connect(PgSettings.from_env()) as connection:
        tiles = tiles_of_neighborhood(
            connection, neighborhood=args.neighborhood, scrape_date=args.scrape_date
        )
    if not tiles:
        print(
            f"{args.neighborhood} {args.scrape_date}: no lot in rag.lots carries a "
            "cell_partition - run neighborhood_cadastre for it first",
            file=sys.stderr,
        )
        return 2
    for tile in tiles:
        print(tile)
    return 0


def _ensure_leaves(scrape_date: str) -> int:
    """Every (tile table, cell) leaf for ``scrape_date``, each committed alone."""
    from urban_rag import warehouse
    from urban_rag.postgis import connect
    from urban_rag.rag.pgvector import PgSettings

    tables = [t for t in warehouse.TABLES.values() if t.axis is warehouse.Axis.TILE]
    month = scrape_date[:7].replace("-", "")
    wanted = {
        (table.qualified, tile): f"{table.qualified}__{tile}__{month}"
        for table in tables
        for tile in sorted(tile_cut.CUT)
    }
    settings = PgSettings.from_env()
    # One read for what is already there, so a month that is complete costs
    # one connection rather than one per leaf.
    with connect(settings) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT name FROM unnest(%s::text[]) AS name "
            "WHERE to_regclass(name) IS NOT NULL",
            [list(wanted.values())],
        )
        existing = {row[0] for row in cursor.fetchall()}
    missing = [key for key, leaf in wanted.items() if leaf not in existing]
    by_name = {table.qualified: table for table in tables}
    for qualified, tile in missing:
        # One transaction per leaf, so no lock outlives the statement that
        # needed it.
        with connect(settings) as connection:
            warehouse.ensure_partition(
                connection.cursor(),
                by_name[qualified],
                partition=tile,
                scrape_date=scrape_date,
            )
    print(
        f"{len(tables)} table(s) x {len(tile_cut.CUT)} cell(s) for {scrape_date}: "
        f"{len(missing)} leaf/leaves created, {len(existing)} already there"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
