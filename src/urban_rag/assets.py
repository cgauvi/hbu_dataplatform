"""Assets: discover the named tables, then snapshot one borough per day."""

from collections import Counter
from datetime import datetime, timezone

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

from urban_rag.frames import (
    count_invalid_geometries,
    features_to_frame,
    table_slug,
    write_frame,
)
from urban_rag.partitions import date_partitions, namespace_for, scrape_partitions
from urban_rag.resources import ParquetStore, SpectrumResource
from urban_rag.spectrum import SpectrumError
from urban_rag.storage import (
    basename,
    clear_parquet,
    filesystem,
    join,
    storage_options,
)

GROUP = "spectrum"

#: The catalog's one file, under `<root>/spectrum_table_catalog/<date>/`.
CATALOG_FILE = "tables.parquet"


@asset(
    partitions_def=date_partitions,
    group_name=GROUP,
    description=(
        "Every named table published by the Feature Service on a given day, "
        "as `spectrum_table_catalog/<YYYY-MM-DD>/tables.parquet`. Kept as its "
        "own asset because the catalog drifts: boroughs add and retire layers "
        "without notice."
    ),
)
def spectrum_table_catalog(
    context: AssetExecutionContext, spectrum: SpectrumResource, store: ParquetStore
) -> MaterializeResult:
    scrape_date = context.partition_key
    tables = spectrum.client().list_tables()
    namespaces = [_namespace_of(table) for table in tables]
    by_namespace = Counter(namespace for namespace in namespaces if namespace)

    # Written to the store rather than handed to the IO manager, so the day's
    # catalog is a queryable file next to the snapshot it explains rather than
    # a pickle only Dagster can open.
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)
    frame = pd.DataFrame(
        {"table": tables, "namespace": namespaces, "scrape_date": scrape_date}
    )
    path = write_frame(frame, join(output_dir, CATALOG_FILE))
    context.log.info("%d table(s) -> %s", len(tables), path)

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(tables),
            "num_tables": len(tables),
            "num_namespaces": len(by_namespace),
            "output_path": MetadataValue.path(str(path)),
            "tables_per_namespace": MetadataValue.md(
                _markdown_table(
                    "Tables per namespace",
                    ("namespace", "tables"),
                    [(ns, str(n)) for ns, n in sorted(by_namespace.items())],
                )
            ),
        }
    )


@asset(
    partitions_def=scrape_partitions,
    deps=[
        AssetDep(
            spectrum_table_catalog,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        )
    ],
    group_name=GROUP,
    description=(
        "One (geo)parquet file per source table, under "
        "neighborhood_features/<YYYY-MM-DD>/<neighborhood>/. Geometry is "
        "reprojected to EPSG:4326 by the service; tables without geometry land "
        "as plain parquet."
    ),
)
def neighborhood_features(
    context: AssetExecutionContext,
    spectrum: SpectrumResource,
    store: ParquetStore,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    prefix = f"/{namespace_for(neighborhood)}/"
    catalog = _read_catalog(store, scrape_date)
    tables = sorted(t for t in catalog if t.startswith(prefix))
    if not tables:
        raise Failure(
            f"Catalog for {scrape_date} lists no tables under {prefix!r}; "
            "the borough may have been renamed upstream."
        )

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    _clear_partition(context, output_dir)
    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    client = spectrum.client()
    written: dict[str, int] = {}
    empty: list[str] = []
    failed: dict[str, str] = {}
    invalid_geometries = 0

    for table in tables:
        try:
            metadata = client.table_metadata(table)
            features = list(
                client.fetch_features(metadata, page_length=spectrum.page_length)
            )
            if not features:
                empty.append(table)
                context.log.info("%s: no rows, nothing written", table)
                continue

            frame = features_to_frame(
                features,
                # Written as columns because the output path holds bare keys
                # rather than hive `key=value` pairs, so a reader that opens
                # one file still knows which snapshot it belongs to.
                extra_columns={
                    "source_table": table,
                    "neighborhood": neighborhood,
                    "scrape_date": scrape_date,
                    "scraped_at": scraped_at,
                },
            )
            path = write_frame(frame, join(output_dir, f"{table_slug(table)}.parquet"))
            written[table] = len(frame)

            # Self-intersecting rings survive the MapInfo export; report them
            # rather than repairing, so the file stays a faithful copy.
            invalid = count_invalid_geometries(frame)
            if invalid:
                invalid_geometries += invalid
                context.log.warning("%s: %d invalid geometr(ies)", table, invalid)

            context.log.info(
                "%s: %d rows -> %s (crs %s)",
                table,
                len(frame),
                basename(path),
                metadata.native_crs or "none",
            )
        except SpectrumError as exc:
            # One unreadable layer should not cost the whole borough.
            failed[table] = str(exc)
            context.log.warning("%s: skipped (%s)", table, exc)

    if not written:
        raise Failure(
            f"No table under {prefix!r} produced rows "
            f"({len(failed)} failed, {len(empty)} empty)."
        )

    return MaterializeResult(
        metadata={
            "dagster/row_count": sum(written.values()),
            "num_tables_written": len(written),
            "num_tables_empty": len(empty),
            "num_tables_failed": len(failed),
            "num_invalid_geometries": invalid_geometries,
            "output_dir": MetadataValue.path(str(output_dir)),
            "rows_per_table": MetadataValue.md(
                _markdown_table(
                    f"Rows per table — {neighborhood} {scrape_date}",
                    ("table", "rows"),
                    [(t, str(n)) for t, n in sorted(written.items())],
                )
            ),
            **(
                {"failures": MetadataValue.json(failed)}
                if failed
                else {}
            ),
        }
    )


def _namespace_of(table: str) -> str:
    """``/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE`` -> ``19_VSMPE``."""
    parts = table.split("/")
    return parts[1] if len(parts) > 1 else ""


def _read_catalog(store: ParquetStore, scrape_date: str) -> list[str]:
    """The table names `spectrum_table_catalog` wrote for ``scrape_date``."""
    path = join(
        store.partition_dir(spectrum_table_catalog.key.path[-1], scrape_date),
        CATALOG_FILE,
    )
    if not filesystem(path).exists(path):
        raise Failure(
            f"{path} is missing; materialize spectrum_table_catalog for "
            f"{scrape_date} first."
        )
    frame = pd.read_parquet(
        path, columns=["table"], storage_options=storage_options(path)
    )
    return frame["table"].tolist()


def _clear_partition(context: AssetExecutionContext, output_dir: str) -> None:
    """A partition is a full snapshot, so drop files from a previous run."""
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))


def _markdown_table(
    title: str, headers: tuple[str, ...], rows: list[tuple[str, ...]]
) -> str:
    lines = [
        f"### {title}",
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)
