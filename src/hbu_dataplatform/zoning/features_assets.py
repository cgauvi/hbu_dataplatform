"""Assets: discover the named tables, then snapshot one borough per day.

Three cities, one asset. A Montreal key snapshots every table its Spectrum
namespace publishes; a Quebec City key snapshots the city's zoning layer
bounded by the borough outline, and the specification grid rows for the
zones it holds - see `hbu_dataplatform.cities.quebec_city.zoning`; a Saguenay key snapshots the
municipal zoning layer with a grid link resolved per zone - see
`hbu_dataplatform.cities.saguenay.zoning`. All three land under
`bronze/neighborhood_features/<date>/<neighborhood>/` as one parquet per
table, which is the only shape anything downstream reads.
"""

import json
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

from hbu_dataplatform.partitions.guards import guard_current_scrape_month
from hbu_dataplatform.core.frames import (
    count_invalid_geometries,
    features_to_frame,
    table_slug,
    write_frame,
)
from hbu_dataplatform.sources.infolot.client import esri_polygon
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.boundaries.assets import borough_boundary, reference_neighborhoods
from hbu_dataplatform.partitions.axes import (
    City,
    city_of,
    date_partitions,
    municipality_code_for,
    namespace_for,
    scrape_partitions,
    source_namespace_for,
)
from hbu_dataplatform.cities.saguenay import zoning as saguenay
from hbu_dataplatform.cities.saguenay.zoning import SaguenayZoningError
from hbu_dataplatform.cities.quebec_city.zoning import (
    FICHE_URL_COLUMN,
    GRID_SLUG,
    GRID_URL_COLUMN,
    GRID_ZONE_COLUMN,
    HERITAGE_ID_COLUMN,
    HERITAGE_LAYERS,
    ZONE_CODE_FIELD,
    ZONING_SLUG,
    QuebecZoningClient,
    QuebecZoningError,
    borough_prefix,
    coded_values,
    fiche_url_for,
    label_coded_values,
    read_zoning_grid,
)
from hbu_dataplatform.core.resources import (
    ParquetStore,
    QuebecOpenDataResource,
    QuebecZoningResource,
    SaguenayZoningResource,
    SpectrumResource,
)
from hbu_dataplatform.cities.montreal.spectrum import SpectrumError
from hbu_dataplatform.core.storage import (
    basename,
    clear_parquet,
    filesystem,
    join,
    storage_options,
)

GROUP = "bronze_spectrum"

#: The catalog's one file, under `<root>/bronze/spectrum_table_catalog/<date>/`.
CATALOG_FILE = "tables.parquet"


@asset(
    key_prefix=key_prefix("spectrum_table_catalog"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"parquet"},
    description=(
        "Every named table published by the Feature Service on a given day, "
        "as `bronze/spectrum_table_catalog/<YYYY-MM-DD>/tables.parquet`. Kept as its "
        "own asset because the catalog drifts: boroughs add and retire layers "
        "without notice."
    ),
)
@guard_current_scrape_month
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
    key_prefix=key_prefix("neighborhood_features"),
    partitions_def=scrape_partitions,
    deps=[
        AssetDep(
            spectrum_table_catalog,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        ),
        # Only a Quebec City key reads this - its zoning fetch is bounded by
        # the borough outline - but the lineage is declared for both, since
        # the dependency is on the asset and not on which city ran.
        AssetDep(
            reference_neighborhoods,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        ),
    ],
    group_name=GROUP,
    kinds={"geoparquet", "parquet"},
    description=(
        "One (geo)parquet file per source table, under "
        "bronze/neighborhood_features/<YYYY-MM-DD>/<neighborhood>/. Geometry is "
        "reprojected to EPSG:4326 by the service; tables without geometry land "
        "as plain parquet. A Montreal borough is every table under its "
        "Spectrum namespace; a Quebec City borough is the city's 'Zonage en "
        f"vigueur' layer clipped to its outline ({ZONING_SLUG}), the "
        f"specification-grid rows for those zones ({GRID_SLUG}) and the city's "
        "heritage layers (Patrimoine__*): studied buildings with their grade, "
        "the cited, classified and designated immovables, the heritage sites "
        "and their protection areas."
    ),
)
@guard_current_scrape_month
def neighborhood_features(
    context: AssetExecutionContext,
    spectrum: SpectrumResource,
    quebec_zoning: QuebecZoningResource,
    saguenay_zoning: SaguenayZoningResource,
    quebec_open_data: QuebecOpenDataResource,
    store: ParquetStore,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    city = city_of(neighborhood)
    if city is City.QUEBEC:
        return _quebec_features(
            context,
            quebec_zoning,
            store,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    if city is City.SAGUENAY:
        return _saguenay_features(
            context,
            saguenay_zoning,
            quebec_open_data,
            store,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )

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
                    # The borough namespace the path carries and `table_slug`
                    # drops - what makes C01-001 here a different zone from
                    # C01-001 in the next borough. See
                    # `partitions.source_namespace_for`.
                    "source_namespace": source_namespace_for(neighborhood),
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


def _quebec_features(
    context: AssetExecutionContext,
    quebec_zoning: QuebecZoningResource,
    store: ParquetStore,
    *,
    neighborhood: str,
    scrape_date: str,
) -> MaterializeResult:
    """One Quebec City borough's zoning, as two tables.

    The zone polygons come from the city's ArcGIS layer, bounded by the
    borough outline the way `neighborhood_lots` bounds the cadastre - a bound
    on what is asked for, not an interpretation of what comes back, so a zone
    straddling the arrondissement line is in both partitions. The grid rows
    are the workbook's, filtered to the zone codes just fetched, with the
    workbook's own column names; reading them is `zoning_grid_columns`' job.
    """
    boundary = borough_boundary(store, scrape_date, neighborhood)
    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )

    client = quebec_zoning.client()
    try:
        object_ids = client.zone_ids(esri_polygon(boundary))
        context.log.info(
            "%s: %d zone(s) intersect the borough boundary",
            neighborhood,
            len(object_ids),
        )
        features = list(client.fetch_zones(object_ids))
        grid = read_zoning_grid(client.fetch_grid())
    except QuebecZoningError as exc:
        raise Failure(
            f"Quebec City zoning read for {neighborhood} {scrape_date} failed: {exc}"
        )
    if not features:
        raise Failure(
            f"The zoning layer returned no zone inside {neighborhood}; its "
            f"outline in reference_neighborhoods for {scrape_date} may be empty."
        )

    _clear_partition(context, output_dir)
    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    provenance = {
        "source_namespace": source_namespace_for(neighborhood),
        "neighborhood": neighborhood,
        "scrape_date": scrape_date,
        "scraped_at": scraped_at,
    }
    zones = features_to_frame(
        features, extra_columns={"source_table": ZONING_SLUG, **provenance}
    )
    if ZONE_CODE_FIELD not in zones.columns:
        raise Failure(
            f"The zoning layer publishes no {ZONE_CODE_FIELD} field; it has "
            f"{', '.join(sorted(zones.columns))}."
        )
    # The link is built here, which is what puts Quebec City on the path the
    # other two cities already take. The city serves one grid sheet per zone
    # from a handler keyed on the zone code, so unlike Saguenay's there is no
    # id to resolve and nothing to request - a formatted string per row, under
    # the column name Montreal's zone table uses. From here the corpus assets
    # are ordinary: `linked_documents` downloads one document per distinct
    # link, chunks it, embeds it, and `document_index` publishes it.
    #
    # The workbook this asset also writes is not made redundant by it. That is
    # where the solver's norms come from; the sheet is where the by-law's prose
    # is, and retrieval has had nothing to answer a Quebec City question with.
    zones[GRID_URL_COLUMN] = [
        client.sheet_url_for(code) for code in zones[ZONE_CODE_FIELD].astype(str)
    ]

    zones_path = write_frame(zones, join(output_dir, f"{ZONING_SLUG}.parquet"))
    invalid = count_invalid_geometries(zones)
    if invalid:
        context.log.warning("%s: %d invalid geometr(ies)", ZONING_SLUG, invalid)

    codes = set(zones[ZONE_CODE_FIELD].astype(str))
    borough_grid = grid[grid[GRID_ZONE_COLUMN].astype(str).isin(codes)].copy()
    for column, value in {"source_table": GRID_SLUG, **provenance}.items():
        borough_grid[column] = value
    grid_path = write_frame(borough_grid, join(output_dir, f"{GRID_SLUG}.parquet"))
    missing = sorted(codes - set(borough_grid[GRID_ZONE_COLUMN].astype(str)))
    if missing:
        context.log.warning(
            "%s: %d zone(s) drawn on the map have no grid row, e.g. %s",
            neighborhood,
            len(missing),
            ", ".join(missing[:5]),
        )
    by_borough = Counter(borough_prefix(code) or "?" for code in codes)
    context.log.info(
        "%s %s: %d zone(s) -> %s; %d grid row(s) -> %s",
        neighborhood,
        scrape_date,
        len(zones),
        basename(zones_path),
        len(borough_grid),
        basename(grid_path),
    )

    heritage = _quebec_heritage(
        context,
        client.on_layer(quebec_zoning.heritage_service_url),
        esri_polygon(boundary),
        output_dir,
        provenance,
    )
    invalid += heritage["invalid"]

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(zones),
            "num_tables_written": 2 + len(heritage["written"]),
            "num_heritage_tables_written": len(heritage["written"]),
            "num_heritage_tables_failed": len(heritage["failed"]),
            "heritage_rows_per_table": MetadataValue.md(
                _markdown_table(
                    f"Heritage rows per table — {neighborhood} {scrape_date}",
                    ("table", "rows"),
                    [(t, str(n)) for t, n in heritage["written"].items()],
                )
            ),
            **(
                {"heritage_failures": MetadataValue.json(heritage["failed"])}
                if heritage["failed"]
                else {}
            ),
            "heritage_url": MetadataValue.url(quebec_zoning.heritage_service_url),
            "num_zones": len(zones),
            "num_zone_codes": len(codes),
            "num_grid_rows": len(borough_grid),
            "num_zones_without_grid_row": len(missing),
            "num_invalid_geometries": invalid,
            "grid_title": grid.attrs.get("title", ""),
            "output_dir": MetadataValue.path(str(output_dir)),
            # The zone code's leading digit is the arrondissement, so this is
            # how many of the zones fetched belong to the borough itself and
            # how many are neighbours straddling its line.
            "zones_by_arrondissement_digit": MetadataValue.md(
                _markdown_table(
                    f"Zones by arrondissement digit — {neighborhood} {scrape_date}",
                    ("digit", "zones"),
                    [(digit, str(n)) for digit, n in sorted(by_borough.items())],
                )
            ),
            "num_grid_sheets_linked": int(zones[GRID_URL_COLUMN].notna().sum()),
            "source_url": MetadataValue.url(quebec_zoning.layer_url),
            "grid_url": MetadataValue.url(quebec_zoning.grid_url),
            "sheet_url_template": quebec_zoning.sheet_url_template,
        }
    )


def _quebec_heritage(
    context: AssetExecutionContext,
    service: QuebecZoningClient,
    boundary: dict,
    output_dir: str,
    provenance: dict,
) -> dict:
    """Quebec City's heritage layers inside one borough, one parquet each.

    Bounded by the outline the way the zones are, so a building on the line
    is in both partitions. Each row keeps the layer's fields as published and
    gains three things: the coded fields' labels beside their codes (the grade
    is a code, and two of its six codes are not grades - see
    `quebec.UNGRADED_CODES`), the row's id under `quebec.HERITAGE_ID_COLUMN`
    so the layer reaches `silver.lot_features`, and for a studied building the
    fiche it is described on.

    One unreadable layer is skipped and reported rather than costing the
    borough its zoning, as a Montreal borough skips an unreadable Spectrum
    table.
    """
    written: dict[str, int] = {}
    failed: dict[str, str] = {}
    invalid_total = 0
    for layer in HERITAGE_LAYERS:
        reader = service.on_layer(f"{service.layer_url}/{layer.layer_id}")
        try:
            labels = coded_values(reader.layer_metadata())
            features = list(reader.fetch_zones(reader.zone_ids(boundary)))
        except QuebecZoningError as exc:
            failed[layer.slug] = str(exc)
            context.log.warning("%s: skipped (%s)", layer.slug, exc)
            continue
        if not features:
            context.log.info("%s: no rows inside the borough", layer.slug)
            continue

        frame = features_to_frame(
            features, extra_columns={"source_table": layer.slug, **provenance}
        )
        if layer.id_field not in frame.columns:
            failed[layer.slug] = f"no {layer.id_field} field"
            context.log.warning("%s: skipped (no %s)", layer.slug, layer.id_field)
            continue
        frame = label_coded_values(frame, labels)
        frame[HERITAGE_ID_COLUMN] = frame[layer.id_field].astype("Int64").astype(str)
        if layer.id_field == "NO_SEQ":
            frame[FICHE_URL_COLUMN] = [fiche_url_for(v) for v in frame["NO_SEQ"]]

        path = write_frame(frame, join(output_dir, f"{layer.slug}.parquet"))
        written[layer.slug] = len(frame)
        invalid = count_invalid_geometries(frame)
        if invalid:
            invalid_total += invalid
            context.log.warning("%s: %d invalid geometr(ies)", layer.slug, invalid)
        context.log.info("%s: %d rows -> %s", layer.slug, len(frame), basename(path))
    return {"written": written, "failed": failed, "invalid": invalid_total}


def _saguenay_features(
    context: AssetExecutionContext,
    saguenay_zoning: SaguenayZoningResource,
    quebec_open_data: QuebecOpenDataResource,
    store: ParquetStore,
    *,
    neighborhood: str,
    scrape_date: str,
) -> MaterializeResult:
    """Saguenay's zoning, as one table carrying a link per zone.

    The polygons come off Données Québec whole - the city publishes one
    municipal file and this partition is the whole municipality, so unlike the
    other two cities there is nothing to bound the fetch by. The outline is
    still read and still used: a polygon that falls outside it is a
    neighbouring municipality's row in a file that claims to be Saguenay's,
    which is worth knowing about rather than loading.

    **The link is built here, and that is what makes the rest ordinary.** Each
    zone's grid is its own PDF behind an id the polygons do not carry, so this
    resolves the id per zone and writes the resulting URL as `LIEN_GRILLE` -
    the column name Montreal's zone table uses. From that point Saguenay is on
    Montreal's path: `linked_documents` downloads one document per distinct
    link, `zoning_grid_columns` parses it, and the corpus assets index it.
    `_quebec_features` writes the same column for the same reason, from a URL
    template rather than a lookup - all three cities meet here.

    The id lookup is the expensive step - one request per zone, because the
    city's proxy forwards no other parameter - so it is done once here and
    read back from bronze by everything downstream.
    """
    boundary = borough_boundary(store, scrape_date, neighborhood)
    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )

    client = quebec_open_data.client()
    package = client.package(saguenay.ZONING_DATASET)
    resource = package.resource(saguenay.ZONING_GEOJSON)
    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    provenance = {
        "source_table": saguenay.ZONING_SLUG,
        "source_namespace": source_namespace_for(neighborhood),
        "neighborhood": neighborhood,
        "scrape_date": scrape_date,
        "scraped_at": scraped_at,
    }
    try:
        payload = json.loads(client.download(resource))
    except ValueError as exc:
        raise Failure(f"{resource.filename}: not valid JSON ({exc})") from exc
    features = payload.get("features") or []
    if not features:
        raise Failure(f"{resource.filename}: the portal returned no features")
    # The same normalization the Spectrum scrape and the other two cities'
    # layers get - nested values JSON-encoded, style dropped, EPSG:4326
    # asserted. The file names no `crs` member, which in GeoJSON *is* WGS 84.
    zones = features_to_frame(features, extra_columns=provenance)
    if saguenay.ZONE_CODE_FIELD not in zones.columns:
        raise Failure(
            f"{resource.filename} has no {saguenay.ZONE_CODE_FIELD} column; it "
            f"publishes {', '.join(sorted(zones.columns))}."
        )

    # The layer stamps every row with the city's *code géographique*. A row
    # carrying another is a neighbour's, and dropping it silently would put
    # someone else's by-law on a Saguenay parcel.
    expected = municipality_code_for(City.SAGUENAY)
    stated = zones[saguenay.MUNICIPALITY_FIELD].astype(str)
    foreign = zones[stated != expected]
    if not foreign.empty:
        context.log.warning(
            "%s: %d row(s) carry %s != %s and are dropped",
            resource.filename,
            len(foreign),
            saguenay.MUNICIPALITY_FIELD,
            expected,
        )
        zones = zones[stated == expected].copy()

    outside = int((~zones.intersects(boundary)).sum())
    if outside:
        context.log.warning(
            "%s: %d zone(s) fall outside the %s outline and are dropped",
            resource.filename,
            outside,
            neighborhood,
        )
        zones = zones[zones.intersects(boundary)].copy()
    if zones.empty:
        raise Failure(
            f"No zoning polygon survives the {neighborhood} outline; its "
            f"limits in reference_neighborhoods for {scrape_date} may be empty."
        )

    codes = [str(code) for code in zones[saguenay.ZONE_CODE_FIELD]]
    grid_client = saguenay_zoning.client()
    try:
        index = grid_client.zone_ids(codes, progress=context.log.info)
    except SaguenayZoningError as exc:
        raise Failure(
            f"Saguenay grid index for {neighborhood} {scrape_date} failed: {exc}"
        )
    missing = sorted({code for code in codes if code not in index})
    if missing:
        context.log.warning(
            "%s: %d zone(s) drawn on the map have no grid in the zoning "
            "service, e.g. %s",
            neighborhood,
            len(missing),
            ", ".join(missing[:5]),
        )

    _clear_partition(context, output_dir)
    zones[saguenay.GRID_URL_COLUMN] = [
        grid_client.grid_url_for(index[code]) if code in index else None
        for code in codes
    ]
    zones["zone_id"] = [index.get(code) for code in codes]

    path = write_frame(zones, join(output_dir, f"{saguenay.ZONING_SLUG}.parquet"))
    invalid = count_invalid_geometries(zones)
    if invalid:
        context.log.warning(
            "%s: %d invalid geometr(ies)", saguenay.ZONING_SLUG, invalid
        )
    context.log.info(
        "%s %s: %d zone(s), %d with a grid -> %s",
        neighborhood,
        scrape_date,
        len(zones),
        len(index),
        basename(path),
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(zones),
            "num_tables_written": 1,
            "num_zones": len(zones),
            "num_zone_codes": int(zones[saguenay.ZONE_CODE_FIELD].nunique()),
            # The count that decides how much of the borough gets an envelope:
            # a zone with no grid has no norms to solve and no document to
            # index, and it is the one gap this asset can see on its own.
            "num_zones_with_grid": len(index),
            "num_zones_without_grid": len(missing),
            "num_zones_outside_outline": outside,
            "num_invalid_geometries": invalid,
            "output_dir": MetadataValue.path(str(output_dir)),
            "source_url": MetadataValue.url(
                f"{quebec_open_data.base_url}/dataset/{saguenay.ZONING_DATASET}"
            ),
            "grid_url": MetadataValue.url(saguenay.GRID_PDF_URL),
            "zoning_last_modified": resource.last_modified or "unknown",
            "license": package.license_title or "unknown",
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
