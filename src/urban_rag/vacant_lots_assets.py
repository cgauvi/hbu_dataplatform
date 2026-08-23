"""Lots with effectively nothing built on them, read back out of the join
`building_lot_intersections` already computed.

`rag.building_lots` holds one row per (building, lot) pair that overlaps, so
it answers "what stands on this lot". The rows it does not hold are what a
highest-and-best-use question is actually looking for: the parcels where
something could stand and nothing does. This asset selects those, and it
treats "nothing at all" and "one shed" as the same answer - a 25 m2 garden
structure is not a use of a 400 m2 lot, and a rule that only caught the
strictly empty ones would miss most of the borough's real opportunities.

Downstream of `building_lot_intersections` for the *same* partition and
nothing else: that asset is what loads `rag.lots`/`rag.buildings` and computes
the join, so by the time this runs the three tables all hold this
(date x neighborhood) partition and the work here is one SQL statement. It
writes no parquet for the same reason its upstream does not - the output is a
Postgres table the query side reads, not another snapshot of a source.
"""

from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    asset,
)
from pydantic import Field

from urban_rag.building_lots_assets import GROUP, building_lot_intersections
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    DEFAULT_MAX_BUILT_AREA_M2,
    VACANT_LOT_CATEGORIES,
    compute_vacant_lots,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import PostgisResource


class VacantLotsConfig(Config):
    """Where the line between "built on" and "effectively empty" is drawn.

    Config rather than a constant because the threshold is a judgement about
    the built form, not a property of the data: 30 m2 is a shed in a borough
    of triplexes, and somewhere with detached garages on every lot may want
    60. Every run records the value it used in its metadata, and every row
    records it too, so a table can always be read back against the cutoff that
    produced it.
    """

    max_built_area_m2: float = Field(
        default=DEFAULT_MAX_BUILT_AREA_M2,
        ge=0,
        description=(
            "A lot is a candidate when at most this many square metres of "
            "building footprint stand on it. 0 selects only the lots no "
            "building touches at all."
        ),
    )


@asset(
    partitions_def=scrape_partitions,
    deps=[building_lot_intersections],
    group_name=GROUP,
    description=(
        "Lots carrying no building, or nothing bigger than a shed, as rows in "
        "Postgres rag.vacant_lots: one per candidate lot, with the footprint "
        "area standing on it, that area as a share of the lot, and which of "
        "the three cases it is - no_building, shed_only, or building_sliver "
        "(a corner of a large neighbouring building crossing the cadastral "
        "line, which is empty in substance but is not a shed). The area "
        "compared is the footprint clipped to this lot, so a warehouse next "
        "door counts only for the part actually inside. Replaces this "
        "partition's prior rows."
    ),
)
def vacant_lots(
    context: AssetExecutionContext,
    config: VacantLotsConfig,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    try:
        with postgis.connect() as connection:
            result = compute_vacant_lots(
                connection,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                max_built_area_m2=config.max_built_area_m2,
            )
    except PostgresUnavailable as exc:
        raise Failure(f"Postgres unreachable for {neighborhood} {scrape_date}: {exc}")

    num_lots = int(result["num_lots"])
    if num_lots == 0:
        # Not "no vacant lots": no lots at all, which means the partition was
        # never loaded rather than that the borough is full. Distinguishing
        # the two is the whole point of failing here instead of writing a
        # perfectly well-formed zero.
        raise Failure(
            f"rag.lots holds no lot for {neighborhood} {scrape_date} - "
            "materialize building_lot_intersections for this partition first."
        )

    candidates = int(result["candidates"])
    by_category = result["by_category"]
    context.log.info(
        "%s %s: %d of %d lot(s) with <= %.0f m2 built on them (%s)",
        neighborhood,
        scrape_date,
        candidates,
        num_lots,
        config.max_built_area_m2,
        ", ".join(f"{name}={by_category[name]}" for name in VACANT_LOT_CATEGORIES),
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": candidates,
            "num_lots": num_lots,
            "num_vacant_lots": candidates,
            "pct_of_lots": round(100.0 * candidates / num_lots, 2),
            **{f"num_{name}": by_category[name] for name in VACANT_LOT_CATEGORIES},
            "total_vacant_area_ha": round(result["total_lot_area_m2"] / 10_000, 2),
            # What the numbers above mean depends entirely on this, so it
            # travels with them rather than only in the run's config.
            "max_built_area_m2": config.max_built_area_m2,
        }
    )
