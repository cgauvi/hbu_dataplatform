"""How many bytes a lot actually costs, across every table that holds one.

The migration plan sizes a province-wide snapshot at roughly 150 GB, and that
number is an extrapolation from three tables out of twenty-six - the only
measured figures in either repo are the three leaves in hbu_infra's README.
An instance class gets chosen from it, so it is worth replacing with a
measurement before anything is bought.

Read-only: `pg_total_relation_size` over the catalog, divided by the lots in
the partitions those bytes belong to. Run it behind the tunnel:

    eval "$(make -s -C ../hbu_infra db-env ENV=dev)"
    export URBAN_RAG_PG_HOST=127.0.0.1 URBAN_RAG_PG_PORT=5433
    uv run python scripts/measure_capacity.py

`--lots` is how many lots the province has, for the extrapolation. Quebec's
cadastre is a little under four million.
"""

from __future__ import annotations

import argparse

from dotenv import load_dotenv

load_dotenv()

from hbu_dataplatform.core.postgis import connect  # noqa: E402
from hbu_dataplatform.rag.pgvector import PgSettings  # noqa: E402

PROVINCE_LOTS = 3_900_000


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lots", type=int, default=PROVINCE_LOTS)
    args = parser.parse_args()

    with connect(PgSettings.from_env()) as connection:
        cursor = connection.cursor()

        cursor.execute("SELECT count(*) FROM rag.lots")
        (loaded_lots,) = cursor.fetchone()
        if not loaded_lots:
            print("rag.lots is empty - nothing to measure")
            return 2

        # `relkind = 'r'` only: ordinary tables *and* partition leaves, which
        # are where every byte actually lives. A partitioned parent is 'p' and
        # holds no storage of its own - `pg_total_relation_size` on one returns
        # 0, not the sum of its children, so summing parents and skipping
        # leaves measures nothing at all. On hbu-dev that read 360 MB against a
        # real 5 GB, because all nineteen silver tables are partitioned.
        #
        # `pg_partition_root` folds a leaf back onto the table it belongs to,
        # so the output is one line per dataset rather than 713 per borough
        # and month.
        cursor.execute(
            """
            SELECT COALESCE(
                       pg_partition_root(c.oid)::regclass::text,
                       n.nspname || '.' || c.relname
                   )                             AS name,
                   -- ::bigint because `sum()` over bigint returns numeric,
                   -- which arrives as a Decimal and will not divide by a float.
                   sum(pg_total_relation_size(c.oid))::bigint AS bytes
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN ('rag', 'silver', 'gold', 'warehouse')
               AND c.relkind = 'r'
             GROUP BY 1
             ORDER BY 2 DESC
            """
        )
        rows = cursor.fetchall()

    total = sum(bytes_ for _, bytes_ in rows)
    print(f"lots loaded: {loaded_lots:,}\n")
    print(f"{'table':<44}{'size':>12}{'bytes/lot':>12}")
    print("-" * 68)
    for name, bytes_ in rows:
        if not bytes_:
            continue
        print(f"{name:<44}{bytes_ / 1e6:>10.1f} MB{bytes_ / loaded_lots:>11.0f}")
    print("-" * 68)
    print(f"{'total':<44}{total / 1e6:>10.1f} MB{total / loaded_lots:>11.0f}")

    per_lot = total / loaded_lots
    province = per_lot * args.lots
    print(
        f"\nextrapolated to {args.lots:,} lots: {province / 1e9:.0f} GB per snapshot"
    )
    print(
        "  (one snapshot. Multiply by however many versions are retained, not "
        "by twelve months - that is what write-on-change is for.)"
    )
    print(
        "\nCompare against the ceilings: dev is 100 GiB autoscale, prod 200 GiB."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
