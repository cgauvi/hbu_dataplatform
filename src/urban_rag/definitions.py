"""Code location for the urban_rag Spectrum scraper."""

from __future__ import annotations

import os
from pathlib import Path

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
from urban_rag.partitions import (
    ENABLED_NEIGHBORHOODS,
    date_partitions,
    scrape_partitions,
)
from urban_rag.rag_assets import (
    document_chunks,
    document_embeddings,
    linked_documents,
)
from urban_rag.resources import (
    DocumentStore,
    EmbeddingModel,
    GeoParquetStore,
    SpectrumResource,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get("URBAN_RAG_DATA_DIR", PROJECT_ROOT / "data"))

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


rag_corpus_job = define_asset_job(
    "rag_corpus_job",
    selection=AssetSelection.assets(
        linked_documents, document_chunks, document_embeddings
    ),
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


defs = Definitions(
    assets=[
        spectrum_table_catalog,
        neighborhood_features,
        linked_documents,
        document_chunks,
        document_embeddings,
    ],
    jobs=[catalog_job, features_job, rag_corpus_job],
    schedules=[daily_catalog_schedule, daily_features_schedule],
    resources={
        "spectrum": SpectrumResource(),
        "geoparquet": GeoParquetStore(root_dir=str(DATA_ROOT / "spectrum")),
        "documents": DocumentStore(
            root_dir=str(DATA_ROOT / "rag"),
            # Outside the partition tree on purpose: a published
            # resolution never changes, so every scrape date reuses it.
            cache_dir=str(DATA_ROOT / "cache" / "pdf"),
        ),
        "embedding_model": EmbeddingModel(),
        # Pinned rather than left to DAGSTER_HOME so that a catalog
        # materialized from the CLI is still loadable by the next run.
        "io_manager": FilesystemIOManager(base_dir=str(DATA_ROOT / "dagster_io")),
    },
)
