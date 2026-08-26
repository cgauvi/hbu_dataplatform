"""Assets sourced from the ZEF construction cost estimator's publication of the
Altus Group Canadian Cost Guide.

Two assets, split the way a proforma asks the question rather than the way the
guide is laid out: what it costs to build the residences on a lot, and what it
costs to build everything else that might go on one - offices, retail, hotels,
warehouses, and the stalls under them.

Partitioned by date alone. The guide prices nine Canadian markets and knows
nothing about boroughs, so there is no borough axis to partition on: a
Montreal rate is a Montreal rate in Villeray as much as in Verdun. That is the
same posture `street_network` and the two CMHC surveys take, and for the same
reason - one read serves every borough, and the borough axis (if it ever
appears) belongs downstream.

Taking the Montreal column out of a nine-city table is a bound on what was
asked for rather than an interpretation of what came back, which is what keeps
these in bronze; see the layer contract in `urban_rag.layers`. What the guide
publishes is published here unchanged, down to the storey band living inside
the label, as `urban_rag.estimator.rates_frame` explains.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.estimator import (
    MONTREAL_CITY_ID,
    NON_RESIDENTIAL_CATEGORIES,
    RESIDENTIAL_CATEGORIES,
    EstimatorError,
    rates_frame,
)
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.partitions import date_partitions
from urban_rag.resources import EstimatorResource, ParquetStore
from urban_rag.storage import clear_parquet, join

GROUP = "bronze_construction_costs"

#: The one file each partition is written to, under
#: `bronze/<asset>/<YYYY-MM-DD>/`.
RESIDENTIAL_FILE = "residential_costs.parquet"
NON_RESIDENTIAL_FILE = "non_residential_costs.parquet"

#: https://zef-builds.github.io/construction-estimator/
SOURCE_URL = "https://zef-builds.github.io/construction-estimator/"


@asset(
    key_prefix=key_prefix("montreal_residential_costs"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"parquet"},
    description=(
        "Montreal construction cost rates for every residential building type "
        "the guide prices, as bronze/montreal_residential_costs/"
        "<YYYY-MM-DD>/residential_costs.parquet. One row per published type, "
        "in the guide's own order - which for condominium / apartment is "
        "ascending storeys: up to 12, 13-39, 40-60, 60+, plus the up-to-6 "
        "wood frame band. The storey band stays inside `label` where the "
        "publisher put it; `rate_low` and `rate_high` are dollars per square "
        "foot. Townhouses, single family, seniors housing and student "
        "residences are published under the same category and come with it. "
        f"Source: {SOURCE_URL}"
    ),
)
def montreal_residential_costs(
    context: AssetExecutionContext,
    estimator: EstimatorResource,
    store: ParquetStore,
) -> MaterializeResult:
    return _snapshot(
        context,
        estimator,
        store,
        categories=RESIDENTIAL_CATEGORIES,
        filename=RESIDENTIAL_FILE,
    )


@asset(
    key_prefix=key_prefix("montreal_nonresidential_costs"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"parquet"},
    description=(
        "Montreal construction cost rates for every commercial, industrial "
        "and parking type the guide prices, as bronze/"
        "montreal_nonresidential_costs/<YYYY-MM-DD>/"
        "non_residential_costs.parquet. Commercial covers offices by storey "
        "band and class, interior fitouts, retail and hotels; industrial "
        "covers warehouse, distribution and urban storage. Those are dollars "
        "per square foot. Parking is not: its three rows carry "
        "`unit_flag='perStall'` and are dollars per stall, which is why the "
        "column is worth reading before the rate is. The guide's "
        "institutional and infrastructure categories are published too and "
        f"are deliberately not read here. Source: {SOURCE_URL}"
    ),
)
def montreal_nonresidential_costs(
    context: AssetExecutionContext,
    estimator: EstimatorResource,
    store: ParquetStore,
) -> MaterializeResult:
    return _snapshot(
        context,
        estimator,
        store,
        categories=NON_RESIDENTIAL_CATEGORIES,
        filename=NON_RESIDENTIAL_FILE,
    )


def _snapshot(
    context: AssetExecutionContext,
    estimator: EstimatorResource,
    store: ParquetStore,
    *,
    categories: tuple[str, ...],
    filename: str,
) -> MaterializeResult:
    """Fetch the guide, keep ``categories`` at the resource's city, write it.

    Shared by both assets because the only thing that differs between them is
    which `cat` values they keep: one script is fetched, one city column is
    taken, one file is written. Each asset fetches it for itself rather than
    depending on the other - the script is 16 kB of static hosting, and making
    one asset's snapshot wait on the other's would buy nothing and cost a
    dependency edge that says something untrue about the source.
    """
    scrape_date = context.partition_key
    asset_name = context.asset_key.path[-1]

    output_dir = store.partition_dir(asset_name, scrape_date)
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    client = estimator.client()
    try:
        catalog, last_modified = client.catalog()
        frame = rates_frame(
            catalog,
            estimator.city,
            categories,
            # Written as columns because the output path holds bare keys
            # rather than hive `key=value` pairs, so a reader that opens one
            # file still knows which snapshot it belongs to. `Last-Modified`
            # is the publisher's own answer to "when did this last change",
            # which a dated snapshot of a file with no version stamp of its
            # own has nothing else to go on.
            extra_columns={
                "scrape_date": scrape_date,
                "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source_url": client.catalog_url,
                "source_last_modified": last_modified,
            },
        )
    except EstimatorError as exc:
        # The whole asset is one file and one city column, so there is no
        # per-category salvage to do the way `neighborhood_features` salvages
        # a table: a failure costs the partition.
        raise Failure(f"Cost guide read for {scrape_date} failed: {exc}")

    path = write_frame(frame, join(output_dir, filename))
    context.log.info("%d cost rate(s) -> %s", len(frame), path)

    by_category = frame["cat"].value_counts().sort_index()
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_types": len(frame),
            "num_categories": int(by_category.size),
            "city": f"{frame['city_label'].iloc[0]} ({estimator.city})",
            "source_last_modified": last_modified or "not published",
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(SOURCE_URL),
            "rates": MetadataValue.md(_rates_table(frame)),
        }
    )


def _rates_table(frame: pd.DataFrame) -> str:
    """The partition itself, as the markdown Dagster shows beside the run."""
    lines = [
        "| type | category | low | high | per |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for row in frame.itertuples(index=False):
        # `unit_flag` is null for everything priced per square foot, which the
        # source script's own header is what says so - see `UNIT_FLAGS`.
        unit = row.unit_flag if isinstance(row.unit_flag, str) else "sq ft"
        lines.append(
            f"| {row.label} | {row.cat} | ${row.rate_low:,.0f} | "
            f"${row.rate_high:,.0f} | {unit} |"
        )
    return "\n".join(lines)
