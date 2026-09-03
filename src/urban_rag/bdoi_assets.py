"""Asset sourced from StatCan's Open Database of Buildings (BDOI), rather than
from Spectrum or donnees.montreal.ca.

Quebec's building footprints are a pair of province-wide zipped shapefiles
with no borough axis of their own, the same posture as the cadastral lots in
`urban_rag.infolot_assets`: the borough comes from Montreal's side, via
`reference_neighborhoods`, and the province-wide layer is cut down to it with
a spatial join. That is why this asset is partitioned by date *and*
neighborhood even though BDOI itself is not.
"""

from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    asset,
)

from urban_rag.guards import guard_current_scrape_month
from urban_rag.bdoi import BdoiError, QUEBEC_FILES, read_shapefile_zip
from urban_rag.frames import count_invalid_geometries, write_frame
from urban_rag.layers import key_prefix
from urban_rag.open_data_assets import borough_boundary, reference_neighborhoods
from urban_rag.partitions import scrape_partitions
from urban_rag.resources import BdoiResource, ParquetStore
from urban_rag.storage import clear_parquet, join

GROUP = "bronze_bdoi"

#: The one file a partition is written to, under
#: `bronze/neighborhood_buildings/<YYYY-MM-DD>/<neighborhood>/`.
BUILDINGS_FILE = "buildings.parquet"

#: https://www150.statcan.gc.ca/n1/en/catalogue/34260001
SOURCE_URL = "https://www150.statcan.gc.ca/n1/en/catalogue/34260001"


@asset(
    key_prefix=key_prefix("neighborhood_buildings"),
    partitions_def=scrape_partitions,
    deps=[
        AssetDep(
            reference_neighborhoods,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        )
    ],
    group_name=GROUP,
    kinds={"geoparquet"},
    description=(
        "Building footprints intersecting one borough, as bronze/"
        "neighborhood_buildings/<YYYY-MM-DD>/<neighborhood>/buildings.parquet. "
        "Both of Quebec's BDOI extracts are downloaded once (cached on disk, "
        "keyed by filename), concatenated into a single layer, then cut down "
        "to that borough's boundary from reference_neighborhoods with a "
        f"spatial join. Source: {SOURCE_URL}"
    ),
)
@guard_current_scrape_month
def neighborhood_buildings(
    context: AssetExecutionContext,
    bdoi: BdoiResource,
    store: ParquetStore,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    boundary = borough_boundary(store, scrape_date, neighborhood)
    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    buildings = _quebec_buildings(bdoi)

    # The borough boundary is already in EPSG:4326, matching what
    # `read_shapefile_zip` reprojects the source layer to.
    clipped = buildings[buildings.intersects(boundary)].copy()
    if clipped.empty:
        raise Failure(
            f"No BDOI building intersects {neighborhood}; its boundary in "
            f"reference_neighborhoods for {scrape_date} may be empty."
        )

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # Written as columns because the output path holds bare keys rather than
    # hive `key=value` pairs, so a reader that opens one file still knows
    # which snapshot it belongs to.
    clipped["neighborhood"] = neighborhood
    clipped["scrape_date"] = scrape_date
    clipped["scraped_at"] = scraped_at

    path = write_frame(clipped, join(output_dir, BUILDINGS_FILE))

    # Reported, not repaired, so the snapshot stays a faithful copy.
    invalid = count_invalid_geometries(clipped)
    if invalid:
        context.log.warning("%s: %d invalid geometr(ies)", BUILDINGS_FILE, invalid)
    context.log.info("%d building(s) -> %s", len(clipped), path)

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(clipped),
            "num_buildings": len(clipped),
            "num_invalid_geometries": invalid,
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(SOURCE_URL),
        }
    )


def _quebec_buildings(bdoi: BdoiResource) -> gpd.GeoDataFrame:
    """Both BDOI extracts, fetched (or cached) and concatenated into one layer."""
    fetcher = bdoi.fetcher()
    parts: list[gpd.GeoDataFrame] = []
    for filename in QUEBEC_FILES:
        try:
            path = fetcher.fetch(filename)
            parts.append(read_shapefile_zip(path))
        except BdoiError as exc:
            raise Failure(f"BDOI read of {filename} failed: {exc}")
    return gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=parts[0].crs)
