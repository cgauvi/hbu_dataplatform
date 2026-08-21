"""Assets: discover the named tables, then snapshot one borough per day."""

from collections import Counter
from datetime import datetime, timezone

from dagster import (
    AssetExecutionContext,
    AssetIn,
    Failure,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    Output,
    asset,
)

from urban_rag.frames import (
    count_invalid_geometries,
    features_to_frame,
    table_slug,
    write_frame,
)
from urban_rag.partitions import date_partitions, namespace_for, scrape_partitions
from urban_rag.resources import GeoParquetStore, SpectrumResource
from urban_rag.spectrum import SpectrumError

GROUP = "spectrum"


@asset(
    partitions_def=date_partitions,
    group_name=GROUP,
    description=(
        "Every named table published by the Feature Service on a given day. "
        "Kept as its own asset because the catalog drifts: boroughs add and "
        "retire layers without notice."
    ),
)
def spectrum_table_catalog(
    context: AssetExecutionContext, spectrum: SpectrumResource
) -> Output[list[str]]:
    tables = spectrum.client().list_tables()
    by_namespace = Counter(table.split("/")[1] for table in tables if "/" in table)

    return Output(
        tables,
        metadata={
            "num_tables": len(tables),
            "num_namespaces": len(by_namespace),
            "tables_per_namespace": MetadataValue.md(
                _markdown_table(
                    "Tables per namespace",
                    ("namespace", "tables"),
                    [(ns, str(n)) for ns, n in sorted(by_namespace.items())],
                )
            ),
        },
    )


@asset(
    partitions_def=scrape_partitions,
    ins={
        "spectrum_table_catalog": AssetIn(
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            )
        )
    },
    group_name=GROUP,
    description=(
        "One (geo)parquet file per source table, under "
        "neighborhood=<key>/scrape_date=<YYYY-MM-DD>/. Geometry is reprojected "
        "to EPSG:4326 by the service; tables without geometry land as plain "
        "parquet."
    ),
)
def neighborhood_features(
    context: AssetExecutionContext,
    spectrum_table_catalog: list[str],
    spectrum: SpectrumResource,
    geoparquet: GeoParquetStore,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    prefix = f"/{namespace_for(neighborhood)}/"
    tables = sorted(t for t in spectrum_table_catalog if t.startswith(prefix))
    if not tables:
        raise Failure(
            f"Catalog for {scrape_date} lists no tables under {prefix!r}; "
            "the borough may have been renamed upstream."
        )

    output_dir = geoparquet.partition_dir(neighborhood, scrape_date)
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
                extra_columns={"source_table": table, "scraped_at": scraped_at},
            )
            path = write_frame(frame, output_dir / f"{table_slug(table)}.parquet")
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
                path.name,
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


def _clear_partition(context: AssetExecutionContext, output_dir) -> None:
    """A partition is a full snapshot, so drop files from a previous run."""
    if not output_dir.exists():
        return
    stale = list(output_dir.glob("*.parquet"))
    for path in stale:
        path.unlink()
    if stale:
        context.log.info("Removed %d file(s) from a previous run", len(stale))


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
