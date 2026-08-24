"""Code location for the urban_rag Spectrum scraper."""

from __future__ import annotations

from dagster import (
    AssetSelection,
    Definitions,
    FilesystemIOManager,
    MultiPartitionKey,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from urban_rag.assets import neighborhood_features, spectrum_table_catalog
from urban_rag.bdoi_assets import neighborhood_buildings
from urban_rag.building_lots_assets import building_lot_intersections
from urban_rag.cmhc_assets import average_rents, vacancy_rates
from urban_rag.infolot_assets import neighborhood_lots
from urban_rag.lot_vacancy_assets import lots_with_vacancy_rates
from urban_rag.open_data_assets import reference_neighborhoods
from urban_rag.partitions import (
    ENABLED_NEIGHBORHOODS,
    date_partitions,
    scrape_partitions,
)
from urban_rag.rag_assets import (
    document_chunks,
    document_embeddings,
    document_index,
    linked_documents,
)
from urban_rag.resources import (
    BdoiResource,
    CmhcResource,
    EmbeddingModel,
    InfolotResource,
    OpenDataResource,
    ParquetStore,
    PdfCache,
    PgVectorResource,
    PostgisResource,
    SpectrumResource,
)
from urban_rag.storage import DATA_ROOT, output_root

TIMEZONE = "America/Toronto"

catalog_job = define_asset_job(
    "spectrum_catalog_job",
    selection=AssetSelection.assets(spectrum_table_catalog),
    partitions_def=date_partitions,
)

features_job = define_asset_job(
    "neighborhood_features_job",
    selection=AssetSelection.assets(neighborhood_features),
    partitions_def=scrape_partitions,
)


reference_neighborhoods_job = define_asset_job(
    "reference_neighborhoods_job",
    selection=AssetSelection.assets(reference_neighborhoods),
    partitions_def=date_partitions,
)

lots_job = define_asset_job(
    "neighborhood_lots_job",
    selection=AssetSelection.assets(neighborhood_lots),
    partitions_def=scrape_partitions,
)

buildings_job = define_asset_job(
    "neighborhood_buildings_job",
    selection=AssetSelection.assets(neighborhood_buildings),
    partitions_def=scrape_partitions,
)

building_lots_job = define_asset_job(
    "building_lot_intersections_job",
    selection=AssetSelection.assets(building_lot_intersections),
    partitions_def=scrape_partitions,
)

vacancy_rates_job = define_asset_job(
    "vacancy_rates_job",
    selection=AssetSelection.assets(vacancy_rates),
    partitions_def=scrape_partitions,
)

average_rents_job = define_asset_job(
    "average_rents_job",
    selection=AssetSelection.assets(average_rents),
    partitions_def=scrape_partitions,
)

lots_with_vacancy_rates_job = define_asset_job(
    "lots_with_vacancy_rates_job",
    selection=AssetSelection.assets(lots_with_vacancy_rates),
    partitions_def=scrape_partitions,
)


rag_corpus_job = define_asset_job(
    "rag_corpus_job",
    selection=AssetSelection.assets(
        linked_documents, document_chunks, document_embeddings
    ),
    partitions_def=scrape_partitions,
)

# Separate from rag_corpus_job on purpose: the corpus is built from the city's
# servers and this one publishes it to a database that has to be reachable, and
# the second failing should not cost the first. Run it after a corpus run, or on
# its own to backfill a partition that was embedded while the store was down.
document_index_job = define_asset_job(
    "document_index_job",
    selection=AssetSelection.assets(document_index),
    partitions_def=scrape_partitions,
)


@schedule(
    job=catalog_job,
    cron_schedule="0 4 * * *",
    execution_timezone=TIMEZONE,
    description="Refresh the table catalog for today's scrape date.",
)
def daily_catalog_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(run_key=f"catalog-{scrape_date}", partition_key=scrape_date)


@schedule(
    job=features_job,
    # Twenty minutes behind the catalog, which is its upstream input.
    cron_schedule="20 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot every enabled neighborhood for today's scrape date.",
)
def daily_features_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"features-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=reference_neighborhoods_job,
    # Independent of the Spectrum assets, so it only avoids running at the
    # same minute as they do.
    cron_schedule="40 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the open-data reference neighborhoods for today.",
)
def daily_reference_neighborhoods_schedule(
    context: ScheduleEvaluationContext,
) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=f"reference-neighborhoods-{scrape_date}", partition_key=scrape_date
    )

@schedule(
    job=lots_job,
    # An hour behind reference_neighborhoods, which supplies the borough
    # boundary each partition is cut with.
    cron_schedule="40 5 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the cadastral lots of every enabled neighborhood.",
)
def daily_lots_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"lots-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=buildings_job,
    # An hour behind reference_neighborhoods too, alongside neighborhood_lots
    # which shares the same borough-boundary dependency.
    cron_schedule="50 5 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the BDOI building footprints of every enabled neighborhood.",
)
def daily_buildings_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"buildings-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=building_lots_job,
    # An hour behind lots/buildings, which it depends on for the same
    # partition - long enough for both to clear a borough's worth of rows.
    cron_schedule="0 7 * * *",
    execution_timezone=TIMEZONE,
    description="Recompute the building x lot join for every enabled neighborhood.",
)
def daily_building_lots_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"building-lots-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=vacancy_rates_job,
    # Alongside reference_neighborhoods rather than behind it: the survey has
    # no upstream in this pipeline, since the borough is picked by name rather
    # than cut out of a boundary.
    cron_schedule="45 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the CMHC vacancy rates of every enabled neighborhood.",
)
def daily_vacancy_rates_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"vacancy-rates-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=average_rents_job,
    # Same source and no pipeline upstream, but kept at a different minute so
    # a live CMHC hiccup affects one small run at a time.
    cron_schedule="50 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the CMHC average rents of every enabled neighborhood.",
)
def daily_average_rents_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"average-rents-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=lots_with_vacancy_rates_job,
    # After lots and vacancy_rates, before the building x lot spatial join.
    cron_schedule="10 6 * * *",
    execution_timezone=TIMEZONE,
    description="Join cadastral lots to CMHC vacancy rates by neighborhood.",
)
def daily_lots_with_vacancy_rates_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"lots-with-vacancy-rates-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


defs = Definitions(
    assets=[
        spectrum_table_catalog,
        neighborhood_features,
        reference_neighborhoods,
        neighborhood_lots,
        neighborhood_buildings,
        vacancy_rates,
        average_rents,
        lots_with_vacancy_rates,
        building_lot_intersections,
        linked_documents,
        document_chunks,
        document_embeddings,
        document_index,
    ],
    jobs=[
        catalog_job,
        features_job,
        reference_neighborhoods_job,
        lots_job,
        buildings_job,
        building_lots_job,
        vacancy_rates_job,
        average_rents_job,
        lots_with_vacancy_rates_job,
        rag_corpus_job,
        document_index_job,
    ],
    schedules=[
        daily_catalog_schedule,
        daily_features_schedule,
        daily_reference_neighborhoods_schedule,
        daily_lots_schedule,
        daily_buildings_schedule,
        daily_building_lots_schedule,
        daily_vacancy_rates_schedule,
        daily_average_rents_schedule,
        daily_lots_with_vacancy_rates_schedule,
    ],
    resources={
        "spectrum": SpectrumResource(),
        "open_data": OpenDataResource(),
        "infolot": InfolotResource(),
        # One tree for every asset: `<root>/<asset>/<date>[/<neighborhood>]`,
        # where the root is `s3://<S3_BUCKET>` when that is set and `data/`
        # otherwise.
        "store": ParquetStore(root_dir=output_root()),
        "pdf_cache": PdfCache(
            # Outside the partition tree on purpose: a published
            # resolution never changes, so every scrape date reuses it.
            # Always local: it is a cache keyed by URL, not pipeline output.
            cache_dir=str(DATA_ROOT / "cache" / "pdf"),
        ),
        "bdoi": BdoiResource(
            # Same posture as pdf_cache: a published BDOI extract never
            # changes, so it is cached once, outside the partition tree, and
            # always local.
            cache_dir=str(DATA_ROOT / "cache" / "bdoi"),
        ),
        "cmhc": CmhcResource(
            # Same posture as bdoi/pdf_cache: a published survey year is
            # final, so the workbook is cached once, outside the partition
            # tree, and always local.
            cache_dir=str(DATA_ROOT / "cache" / "cmhc"),
        ),
        "embedding_model": EmbeddingModel(),
        # The query side's store. Every field defaults to its URBAN_RAG_PG_*
        # variable, so the endpoint and the credentials live in the environment
        # (or in .env) rather than here - and `urban-rag --backend postgres`
        # reads the same ones.
        "pgvector": PgVectorResource(),
        # The plain PostGIS tables (rag.lots/rag.buildings/rag.building_lots),
        # same database as "pgvector" and configured the same way - every
        # field defaults to its URBAN_RAG_PG_* variable.
        "postgis": PostgisResource(),
        # Every asset writes its own parquet into `store` and returns a
        # MaterializeResult, so nothing of consequence passes through here;
        # it is pinned rather than left to DAGSTER_HOME only so a run from the
        # CLI and a run from the UI agree on where Dagster keeps its own
        # bookkeeping. Always local: there is no S3 backend configured for it.
        "io_manager": FilesystemIOManager(base_dir=str(DATA_ROOT / "dagster_io")),
    },
)
