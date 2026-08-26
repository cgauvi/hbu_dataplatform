"""Assets sourced from donnees.montreal.ca rather than from Spectrum.

The portal publishes island-wide files, so these are partitioned by scrape
date alone: there is no borough axis to slice them on.
"""

import json
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import shapely
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.frames import count_invalid_geometries, features_to_frame, write_frame
from urban_rag.layers import key_prefix
from urban_rag.open_data import OpenDataError, decode_csv
from urban_rag.partitions import borough_code_for, date_partitions
from urban_rag.resources import OpenDataResource, ParquetStore
from urban_rag.storage import clear_parquet, filesystem, join

GROUP = "bronze_open_data"

#: Portal slug of https://donnees.montreal.ca/dataset/quartiers
QUARTIERS_DATASET = "quartiers"

#: The geographic layer: 91 reference neighborhoods, already in EPSG:4326.
QUARTIERS_GEOJSON = "quartierreferencehabitation.geojson"

#: The one file the geographic layer is written to, under
#: `bronze/reference_neighborhoods/<YYYY-MM-DD>/`. Read back by
#: `urban_rag.infolot_assets` to bound each borough's cadastre query.
QUARTIERS_FILE = "quartiers.parquet"

#: Dwelling counts per neighborhood, from the 2017 property-assessment roll.
#: The dataset also publishes the layer as CSV and SHP; the CSV is the GeoJSON
#: minus its geometry, and the SHP the same data in a zip, so neither is read.
DWELLINGS_CSV = "nombrelogementsquartiersreference.csv"

#: Written as an integer even though the portal types it as text, since it is
#: the one column in that file meant to be summed.
DWELLINGS_COUNT_COLUMN = "nb_log"

#: Column in the reference layer holding the borough code a boundary is cut
#: on - see `urban_rag.partitions.NEIGHBORHOOD_BOROUGH_CODES`.
BOROUGH_CODE_COLUMN = "no_arr"

#: Portal slug of https://donnees.montreal.ca/dataset/geobase-double
STREETS_DATASET = "geobase-double"

#: The *géobase double*: one line per side of street, drawn along the curb and
#: sidewalk limits, rather than the single centre line the plain géobase
#: publishes. Sides are what a frontage question needs - a lot faces one side
#: of a street, not the axis of the roadway - which is why this dataset and not
#: `geobase`. 91,546 features island-wide, ~91 MB of GeoJSON.
STREETS_GEOJSON = "gbdouble.json"

#: The one file the street network is written to, under
#: `bronze/street_network/<YYYY-MM-DD>/`. Read back by
#: `urban_rag.street_assets` to cut each borough's slice out of it.
STREETS_FILE = "street_sides.parquet"

#: The geobase double's own key for a street side, unique island-wide (91,546
#: of 91,546 in the first snapshot). Carried through silver and into
#: `rag.streets.cote_rue_id` as the id that survives a reload.
STREET_ID_COLUMN = "COTE_RUE_ID"

#: The street's name, kept as its own column all the way to `rag.streets`
#: because it is what a frontage row is read for.
STREET_NAME_COLUMN = "NOM_VOIE"


@asset(
    key_prefix=key_prefix("reference_neighborhoods"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"geoparquet", "parquet"},
    description=(
        "Montreal's 91 reference neighborhoods for housing analysis, snapshot "
        "per scrape date under bronze/reference_neighborhoods/<YYYY-MM-DD>/: the "
        "geographic layer as geoparquet, plus the dwelling counts published "
        "alongside it. Source: https://donnees.montreal.ca/dataset/quartiers"
    ),
)
def reference_neighborhoods(
    context: AssetExecutionContext,
    open_data: OpenDataResource,
    store: ParquetStore,
) -> MaterializeResult:
    scrape_date = context.partition_key
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)

    client = open_data.client()
    package = client.package(QUARTIERS_DATASET)
    geojson = package.resource(QUARTIERS_GEOJSON)
    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    # The layer is the asset; a failure here is worth the whole partition.
    frame = _geojson_to_frame(
        client.download(geojson),
        source_file=geojson.filename,
        scrape_date=scrape_date,
        scraped_at=scraped_at,
    )
    path = write_frame(frame, join(output_dir, QUARTIERS_FILE))
    invalid = count_invalid_geometries(frame)
    if invalid:
        # Reported, not repaired, so the snapshot stays a faithful copy.
        context.log.warning("%s: %d invalid geometr(ies)", QUARTIERS_FILE, invalid)
    context.log.info("%s: %d rows -> %s", geojson.filename, len(frame), path)

    metadata = {
        "dagster/row_count": len(frame),
        "num_neighborhoods": int(frame["no_qr"].nunique())
        if "no_qr" in frame.columns
        else len(frame),
        "num_invalid_geometries": invalid,
        "output_dir": MetadataValue.path(str(output_dir)),
        "source_url": MetadataValue.url(
            f"https://donnees.montreal.ca/dataset/{QUARTIERS_DATASET}"
        ),
        "license": package.license_title or "unknown",
        "quartiers_last_modified": geojson.last_modified or "unknown",
    }

    # The counts are a companion table, so a bad one costs its own file only.
    try:
        dwellings = package.resource(DWELLINGS_CSV)
        counts = _dwellings_to_frame(
            client.download(dwellings),
            source_file=dwellings.filename,
            scrape_date=scrape_date,
            scraped_at=scraped_at,
        )
        write_frame(counts, join(output_dir, "nombre_logements.parquet"))
        context.log.info("%s: %d rows", dwellings.filename, len(counts))
        metadata |= {
            "num_dwelling_rows": len(counts),
            "total_dwellings": int(counts[DWELLINGS_COUNT_COLUMN].sum())
            if DWELLINGS_COUNT_COLUMN in counts.columns
            else 0,
            "dwellings_last_modified": dwellings.last_modified or "unknown",
        }
    except (OpenDataError, ValueError) as exc:
        context.log.warning("%s: skipped (%s)", DWELLINGS_CSV, exc)
        metadata["dwellings_error"] = str(exc)

    return MaterializeResult(metadata=metadata)


@asset(
    key_prefix=key_prefix("street_network"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"geoparquet"},
    description=(
        "Montreal's geobase double - one line per side of street, drawn along "
        "the curb and sidewalk limits - snapshot per scrape date under "
        f"bronze/street_network/<YYYY-MM-DD>/{STREETS_FILE}. Island-wide and "
        "as published: the borough slice is cut in silver. Source: "
        "https://donnees.montreal.ca/dataset/geobase-double"
    ),
)
def street_network(
    context: AssetExecutionContext,
    open_data: OpenDataResource,
    store: ParquetStore,
) -> MaterializeResult:
    scrape_date = context.partition_key
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)

    client = open_data.client()
    package = client.package(STREETS_DATASET)
    geojson = package.resource(STREETS_GEOJSON)
    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    # Column names are left exactly as the city spells them, unlike
    # `reference_neighborhoods` above: that asset lower-cases because its two
    # files disagree with each other (`No_QR` against `no_qr`) about a column
    # they have to join on. This is one file with one spelling, and the rest of
    # the lot lineage carries its publishers' names through bronze untouched -
    # `NO_LOT` from Infolot, `NUMERO_COMPLET` from Spectrum, `COTE_RUE_ID` here.
    frame = _geojson_to_frame(
        client.download(geojson),
        source_file=geojson.filename,
        scrape_date=scrape_date,
        scraped_at=scraped_at,
        normalize_columns=False,
    )
    if STREET_ID_COLUMN not in frame.columns:
        raise Failure(
            f"{geojson.filename} has no {STREET_ID_COLUMN} column; it publishes "
            f"{', '.join(sorted(frame.columns))}."
        )

    path = write_frame(frame, join(output_dir, STREETS_FILE))
    invalid = count_invalid_geometries(frame)
    if invalid:
        # Reported, not repaired, so the snapshot stays a faithful copy.
        context.log.warning("%s: %d invalid geometr(ies)", STREETS_FILE, invalid)
    context.log.info("%s: %d street side(s) -> %s", geojson.filename, len(frame), path)

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_street_sides": len(frame),
            # The key silver declares its grain on. Reported rather than
            # enforced: bronze keeps whatever the publisher sent, and a
            # duplicate here is the publisher's fact, not this asset's failure.
            "num_street_ids": int(frame[STREET_ID_COLUMN].nunique()),
            "num_street_names": int(frame[STREET_NAME_COLUMN].nunique())
            if STREET_NAME_COLUMN in frame.columns
            else 0,
            "num_invalid_geometries": invalid,
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(
                f"https://donnees.montreal.ca/dataset/{STREETS_DATASET}"
            ),
            "license": package.license_title or "unknown",
            "streets_last_modified": geojson.last_modified or "unknown",
        }
    )


def _geojson_to_frame(
    content: bytes,
    *,
    source_file: str,
    scrape_date: str,
    scraped_at: str,
    normalize_columns: bool = True,
):
    """GeoJSON bytes -> GeoDataFrame, with the provenance columns attached.

    Parsed rather than handed to ``gpd.read_file`` so the same normalization
    the Spectrum scrape applies (nested values JSON-encoded, style dropped)
    applies here too. Both layers this reads are published in EPSG:4326 - the
    quartiers layer says so, and the geobase double names no `crs` member at
    all, which in GeoJSON *means* WGS 84 - and that is the CRS
    `features_to_frame` asserts.

    ``normalize_columns`` lower-cases the column names. On by default for the
    quartiers pair, which needs it to join; off for the street network, which
    keeps its publisher's spelling - see `street_network`.
    """
    try:
        payload = json.loads(content)
    except ValueError as exc:
        raise Failure(f"{source_file}: not valid JSON ({exc})") from exc

    features = payload.get("features") or []
    if not features:
        raise Failure(f"{source_file}: the portal returned no features")

    frame = features_to_frame(
        features,
        # `scrape_date` is a column because the output path holds a bare date
        # rather than a hive `scrape_date=` key.
        extra_columns={
            "source_file": source_file,
            "scrape_date": scrape_date,
            "scraped_at": scraped_at,
        },
    )
    if normalize_columns:
        frame.columns = [_normalize(name, frame) for name in frame.columns]
    return frame


def _dwellings_to_frame(
    content: bytes, *, source_file: str, scrape_date: str, scraped_at: str
):
    """Dwelling-count CSV -> DataFrame keyed the same way as the layer.

    Column names are lower-cased because the two files disagree on their
    spelling (``No_QR`` against ``no_qr``) while naming the same field; the
    portal's own data dictionary uses the lower-case form for both.
    """
    frame = decode_csv(content, filename=source_file)
    frame.columns = [_normalize(name, frame) for name in frame.columns]
    if DWELLINGS_COUNT_COLUMN in frame.columns:
        frame[DWELLINGS_COUNT_COLUMN] = pd.to_numeric(
            frame[DWELLINGS_COUNT_COLUMN], errors="coerce"
        ).astype("Int64")
    frame["source_file"] = source_file
    frame["scrape_date"] = scrape_date
    frame["scraped_at"] = scraped_at
    return frame


def _normalize(name: str, frame) -> str:
    """Lower-case a column name, unless the geometry column is called that."""
    geometry = getattr(frame, "geometry", None)
    if geometry is not None and name == frame.geometry.name:
        return name
    return str(name).strip().lower()


def borough_boundary(store: ParquetStore, scrape_date: str, neighborhood: str):
    """The borough's outline, dissolved from its reference neighborhoods.

    Repaired with ``make_valid`` before the union: the published layer has
    self-intersecting rings that make ``union_all`` raise. The repair is
    confined to this query geometry and never reaches what gets written, which
    is the service's own output.

    Shared by every asset that bounds a province- or city-wide source against
    one borough's shape - see `urban_rag.infolot_assets` and
    `urban_rag.bdoi_assets`.
    """
    code = borough_code_for(neighborhood)
    path = join(
        store.partition_dir(reference_neighborhoods.key.path[-1], scrape_date),
        QUARTIERS_FILE,
    )
    fs = filesystem(path)
    if not fs.exists(path):
        raise Failure(
            f"{path} is missing; materialize reference_neighborhoods for "
            f"{scrape_date} first."
        )
    with fs.open(path, "rb") as handle:
        quartiers = gpd.read_parquet(handle)

    borough = quartiers[quartiers[BOROUGH_CODE_COLUMN] == code]
    if borough.empty:
        published = sorted(quartiers[BOROUGH_CODE_COLUMN].dropna().unique())
        raise Failure(
            f"No reference neighborhood carries {BOROUGH_CODE_COLUMN}={code!r} "
            f"for {neighborhood}; the layer has: {', '.join(published)}"
        )
    return shapely.union_all(shapely.make_valid(borough.geometry.values._data))
