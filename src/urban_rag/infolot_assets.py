"""Assets sourced from Infolot, Quebec's cadastral lot service.

Lots are provincial data with no borough axis of their own, so the borough
comes from Montreal's side: `reference_neighborhoods` supplies the boundary,
and the cadastre query is bounded by it. That makes this asset a join of the
two sources rather than a plain download, and it is why it is partitioned by
date *and* neighborhood while the service it reads is island-agnostic.
"""

from datetime import datetime, timezone

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
from urban_rag.frames import count_invalid_geometries, features_to_frame, write_frame
from urban_rag.infolot import (
    InfolotError,
    esri_polygon,
    normalize_dates,
)
from urban_rag.layers import key_prefix
from urban_rag.open_data_assets import borough_boundary, reference_neighborhoods
from urban_rag.partitions import scrape_partitions
from urban_rag.resources import InfolotResource, ParquetStore
from urban_rag.storage import clear_parquet, join

GROUP = "bronze_infolot"

#: The one file a partition is written to, under
#: `bronze/neighborhood_lots/<YYYY-MM-DD>/<neighborhood>/`.
LOTS_FILE = "lots.parquet"

#: Lot area in square metres, as computed by the service from the geometry.
#: Summed into the partition's metadata as a cheap sanity check on coverage.
AREA_COLUMN = "VA_SUPRF_LOT_CALCL"


@asset(
    key_prefix=key_prefix("neighborhood_lots"),
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
        "Every cadastral lot intersecting one borough, as "
        "bronze/neighborhood_lots/<YYYY-MM-DD>/<neighborhood>/lots.parquet. "
        "Bounded by that borough's boundary from reference_neighborhoods, so a "
        "lot straddling a border belongs to both - a bound on what is asked "
        "for, not an interpretation of what comes back, which is why this "
        "stays bronze. Geometry is reprojected to EPSG:4326 by the service. "
        "Source: Infolot, Registre foncier du Quebec."
    ),
)
@guard_current_scrape_month
def neighborhood_lots(
    context: AssetExecutionContext,
    infolot: InfolotResource,
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

    client = infolot.client()
    try:
        object_ids = client.lot_ids(esri_polygon(boundary))
        context.log.info(
            "%s: %d lot(s) intersect the borough boundary", neighborhood, len(object_ids)
        )
        features = list(client.fetch_lots(object_ids))
    except InfolotError as exc:
        # Unlike the Spectrum scrape there is no per-table salvage to do here:
        # the borough is one query, so a failure costs the whole partition.
        raise Failure(f"Infolot read for {neighborhood} {scrape_date} failed: {exc}")

    if not features:
        raise Failure(
            f"Infolot returned no lot inside {neighborhood}; its boundary in "
            f"reference_neighborhoods for {scrape_date} may be empty."
        )

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    frame = normalize_dates(
        features_to_frame(
            features,
            # Written as columns because the output path holds bare keys
            # rather than hive `key=value` pairs, so a reader that opens one
            # file still knows which snapshot it belongs to.
            extra_columns={
                "neighborhood": neighborhood,
                "scrape_date": scrape_date,
                "scraped_at": scraped_at,
            },
        )
    )
    path = write_frame(frame, join(output_dir, LOTS_FILE))

    # Reported, not repaired, so the snapshot stays a faithful copy.
    invalid = count_invalid_geometries(frame)
    if invalid:
        context.log.warning("%s: %d invalid geometr(ies)", LOTS_FILE, invalid)
    context.log.info("%d lot(s) -> %s", len(frame), path)

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_lots": len(frame),
            "num_distinct_lot_numbers": int(frame["NO_LOT"].nunique())
            if "NO_LOT" in frame.columns
            else len(frame),
            "num_invalid_geometries": invalid,
            "lot_area_ha": round(float(frame[AREA_COLUMN].sum()) / 10_000, 1)
            if AREA_COLUMN in frame.columns
            else 0.0,
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(infolot.base_url),
        }
    )

