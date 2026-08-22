"""Which buildings sit on which lots, computed in Postgres from the latest
snapshot of each.

`neighborhood_lots` and `neighborhood_buildings` each write one borough's
snapshot to S3 as geoparquet; this asset is the join between them, and it
answers something neither carries on its own. A building's footprint does not
respect a cadastral boundary - a school, a warehouse, an apartment tower can
each span two or three lots - so "which lot is this building on" is not a
column either layer has. It is computed here, once both snapshots land, and
it lands in Postgres rather than another geoparquet: PostGIS already holds
both layers loaded and GiST-indexed by the time this asset runs, and
`ST_Intersection` over an index is the tool for exactly this join.

Depends on both source assets for the *same* partition (date x neighborhood):
neither one is missing a dimension the other has, so the partition mapping is
the identity, unlike `reference_neighborhoods`'s dependents which need
`MultiToSingleDimensionPartitionMapping`.
"""

import geopandas as gpd
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    asset,
)

from urban_rag.bdoi_assets import BUILDINGS_FILE, neighborhood_buildings
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import compute_intersections, load_buildings, load_lots
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import join, storage_options

GROUP = "building_lots"


@asset(
    partitions_def=scrape_partitions,
    deps=[neighborhood_lots, neighborhood_buildings],
    group_name=GROUP,
    description=(
        "Building footprints clipped to the lot(s) they intersect, as rows in "
        "Postgres rag.building_lots: one per (building, lot) pair, holding the "
        "clipped intersection geometry and its share of the building's area. "
        "A building spanning several lots gets one row per lot, in proportion "
        "to the footprint actually inside it - not assigned wholesale to "
        "whichever lot its centroid falls in. Loads this borough's latest "
        "neighborhood_lots/neighborhood_buildings snapshot into rag.lots/"
        "rag.buildings first, replacing that partition's prior rows."
    ),
)
def building_lot_intersections(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    lots_path = join(
        store.partition_dir(
            neighborhood_lots.key.path[-1], scrape_date, neighborhood
        ),
        LOTS_FILE,
    )
    buildings_path = join(
        store.partition_dir(
            neighborhood_buildings.key.path[-1], scrape_date, neighborhood
        ),
        BUILDINGS_FILE,
    )

    lots = _read_geoparquet(lots_path)
    buildings = _read_geoparquet(buildings_path)
    if lots.empty:
        raise Failure(f"{lots_path} holds no lot to intersect against.")
    if buildings.empty:
        raise Failure(f"{buildings_path} holds no building to intersect.")

    try:
        with postgis.connect() as connection:
            num_lots = load_lots(
                connection, lots, neighborhood=neighborhood, scrape_date=scrape_date
            )
            num_buildings = load_buildings(
                connection,
                buildings,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
            )
            result = compute_intersections(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
    except PostgresUnavailable as exc:
        raise Failure(
            f"Postgres unreachable for {neighborhood} {scrape_date}: {exc}"
        )

    context.log.info(
        "%s %s: %d lot(s), %d building(s) -> %d intersection(s) across %d building(s)",
        neighborhood,
        scrape_date,
        num_lots,
        num_buildings,
        result["intersections"],
        result["buildings_matched"],
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": result["intersections"],
            "num_lots": num_lots,
            "num_buildings": num_buildings,
            "num_intersections": result["intersections"],
            "num_buildings_matched": result["buildings_matched"],
            "num_buildings_unmatched": num_buildings - result["buildings_matched"],
            "total_intersection_area_ha": round(result["total_area_m2"] / 10_000, 2),
        }
    )


def _read_geoparquet(path: str) -> gpd.GeoDataFrame:
    return gpd.read_parquet(path, storage_options=storage_options(path))
