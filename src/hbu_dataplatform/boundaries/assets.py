"""Assets sourced from the cities' open-data portals rather than from Spectrum.

Both portals publish city-wide files, so these are partitioned by scrape date
alone: there is no borough axis to slice them on. Each asset writes all three
cities' layers side by side under one date - the reference neighborhoods, the
arrondissements and Saguenay's administrative limits; the geobase double and
the two centre-line networks - and `borough_boundary` picks the file a
neighborhood key's city needs.

Quebec City and Saguenay share a portal (Données Québec) and therefore share a
client; Montreal has its own. That is why `quebec_open_data` fetches Saguenay's
datasets too, and why the resource is named for the portal's first user rather
than for the city.
"""

import json
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import shapely
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pydantic import Field

from hbu_dataplatform.partitions.guards import guard_current_scrape_month
from hbu_dataplatform.core.frames import (
    count_invalid_geometries,
    features_to_frame,
    write_frame,
)
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.core.open_data import OpenDataError, decode_csv
from hbu_dataplatform.cities.saguenay import zoning as saguenay
from hbu_dataplatform.partitions.axes import (
    SAGUENAY_OUTLINE_TYPE,
    City,
    borough_code_for,
    city_of,
    date_partitions,
    quebec_abbreviation_for,
    saguenay_outline_name_for,
)
from hbu_dataplatform.core.resources import (
    OpenDataResource,
    ParquetStore,
    QuebecOpenDataResource,
    RqttResource,
)
from hbu_dataplatform.sources.rqtt.client import (
    EXCLUDED_ROAD_CHARACTERISTICS,
    EXCLUDED_ROAD_CLASSES,
    ROAD_CLASS_FIELD,
    ROAD_LAYER,
    STREET_ID_FIELD,
    STREET_NAME_FIELD,
    UNREAD_LAYERS,
    RqttError,
    bbox_in_source_crs,
    read_layer,
    roadway_only,
)
from hbu_dataplatform.core.storage import clear_parquet, filesystem, join

GROUP = "bronze_open_data"

#: Portal slug of https://donnees.montreal.ca/dataset/quartiers
QUARTIERS_DATASET = "quartiers"

#: The geographic layer: 91 reference neighborhoods, already in EPSG:4326.
QUARTIERS_GEOJSON = "quartierreferencehabitation.geojson"

#: The one file the geographic layer is written to, under
#: `bronze/reference_neighborhoods/<YYYY-MM-DD>/`. Read back by
#: `hbu_dataplatform.sources.infolot.assets` to bound each borough's cadastre query.
QUARTIERS_FILE = "quartiers.parquet"

#: Dwelling counts per neighborhood, from the 2017 property-assessment roll.
#: The dataset also publishes the layer as CSV and SHP; the CSV is the GeoJSON
#: minus its geometry, and the SHP the same data in a zip, so neither is read.
DWELLINGS_CSV = "nombrelogementsquartiersreference.csv"

#: Written as an integer even though the portal types it as text, since it is
#: the one column in that file meant to be summed.
DWELLINGS_COUNT_COLUMN = "nb_log"

#: Column in the reference layer holding the borough code a boundary is cut
#: on - see `hbu_dataplatform.partitions.axes.NEIGHBORHOOD_BOROUGH_CODES`.
BOROUGH_CODE_COLUMN = "no_arr"

#: The one file the street network is written to, under
#: `bronze/street_network/<YYYY-MM-DD>/`. Read back by
#: `hbu_dataplatform.sources.rqtt.assets` to cut each borough's slice out of it.
#:
#: One file for every city, where there used to be three. The three municipal
#: layers this replaced - Montreal's `geobase-double`, Quebec City's `vque_18`,
#: Saguenay's `sag-reseau-routier` - agreed on nothing but the fact of being
#: roads, so each needed its own slug, its own id and name columns, and its own
#: branch here. The RQTT is one province-wide publication with one schema, so a
#: fourth city needs a bounding box and no code at all. See `hbu_dataplatform.sources.rqtt.client`.
STREET_SEGMENTS_FILE = "street_segments.parquet"

#: This platform's own names for the segment's key and the street's name, which
#: `street_assets._as_street_sides` renames the publisher's pair to.
#:
#: `COTE_RUE_ID` is a *côté de rue*, a side of street, and is now a historical
#: name rather than a description: the geobase double drew two lines per
#: street, one along each curb, and the RQTT draws one down the axis. The name
#: is kept because it is in the primary key of `silver.neighborhood_streets`
#: and `silver.lot_frontage`, denormalised into `gold.lot_profiles`, and read
#: by the map's tile queries - re-keying all of that would buy a better word
#: and risk the lineage. What the change of geometry costs is in
#: `street_assets._as_street_sides` and
#: `postgis.DEFAULT_FRONTAGE_FALLBACK_BUFFERS_M`.
STREET_ID_COLUMN = "COTE_RUE_ID"

#: The street's name, kept as its own column all the way to `silver.neighborhood_streets`
#: because it is what a frontage row is read for.
STREET_NAME_COLUMN = "NOM_VOIE"

#: Quebec City's six arrondissements, from Données Québec
#: (https://www.donneesquebec.ca/recherche/dataset/vque_2): one polygon each,
#: EPSG:4326, carrying the three-letter ``ABREVIATION`` the partition keys in
#: `hbu_dataplatform.partitions.axes.QUEBEC_BOROUGH_ABBREVIATIONS` are. Written beside
#: the Montreal quartiers under the same date, and cut on by
#: `borough_boundary` for a Quebec City key.
QUEBEC_BOROUGHS_DATASET = "vque_2"
QUEBEC_BOROUGHS_GEOJSON = "vdq-arrondissement.geojson"
QUEBEC_BOROUGHS_FILE = "arrondissements_quebec.parquet"

#: The abbreviation column, lower-cased like the quartiers' columns are.
QUEBEC_ABBREVIATION_COLUMN = "abreviation"

#: Saguenay's administrative limit, from the same Données
#: Québec portal Quebec City's layers come from - see `hbu_dataplatform.cities.saguenay.zoning` for
#: the dataset slugs and the field names. The limit layer is the outline a
#: Saguenay partition is cut against, and unlike either of the other two
#: cities' it holds three *kinds* of polygon in one file: the city, its three
#: arrondissements, and the former municipalities amalgamated in 2002. Taking
#: it whole would union the territory three times over, so the row is selected
#: on `partitions.SAGUENAY_OUTLINE_TYPE` as well as on its name.
SAGUENAY_LIMITS_FILE = "limites_saguenay.parquet"


@asset(
    key_prefix=key_prefix("reference_neighborhoods"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"geoparquet", "parquet"},
    description=(
        "Montreal's 91 reference neighborhoods for housing analysis, snapshot "
        "per scrape date under bronze/reference_neighborhoods/<YYYY-MM-DD>/: the "
        "geographic layer as geoparquet, plus the dwelling counts published "
        "alongside it. Source: https://donnees.montreal.ca/dataset/quartiers. "
        "Quebec City's six arrondissements are written beside them as "
        f"{QUEBEC_BOROUGHS_FILE}, from Données Québec, so a borough of either "
        "city has an outline under the same date."
    ),
)
@guard_current_scrape_month
def reference_neighborhoods(
    context: AssetExecutionContext,
    open_data: OpenDataResource,
    quebec_open_data: QuebecOpenDataResource,
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

    # The second city's outlines are as load-bearing as the first's - every
    # Quebec City partition is cut against them - so a failure here is worth
    # the whole partition too.
    quebec = quebec_open_data.client()
    quebec_package = quebec.package(QUEBEC_BOROUGHS_DATASET)
    arrondissements = quebec_package.resource(QUEBEC_BOROUGHS_GEOJSON)
    quebec_frame = _geojson_to_frame(
        quebec.download(arrondissements),
        source_file=arrondissements.filename,
        scrape_date=scrape_date,
        scraped_at=scraped_at,
    )
    if QUEBEC_ABBREVIATION_COLUMN not in quebec_frame.columns:
        raise Failure(
            f"{arrondissements.filename} has no {QUEBEC_ABBREVIATION_COLUMN} "
            f"column; it publishes {', '.join(sorted(quebec_frame.columns))}."
        )
    quebec_path = write_frame(quebec_frame, join(output_dir, QUEBEC_BOROUGHS_FILE))
    quebec_invalid = count_invalid_geometries(quebec_frame)
    if quebec_invalid:
        context.log.warning(
            "%s: %d invalid geometr(ies)", QUEBEC_BOROUGHS_FILE, quebec_invalid
        )
    context.log.info(
        "%s: %d arrondissement(s) -> %s",
        arrondissements.filename,
        len(quebec_frame),
        quebec_path,
    )
    metadata |= {
        "num_quebec_boroughs": len(quebec_frame),
        "quebec_boroughs": ", ".join(
            sorted(quebec_frame[QUEBEC_ABBREVIATION_COLUMN].astype(str))
        ),
        "num_quebec_invalid_geometries": quebec_invalid,
        "quebec_source_url": MetadataValue.url(
            f"{quebec_open_data.base_url}/dataset/{QUEBEC_BOROUGHS_DATASET}"
        ),
        "quebec_license": quebec_package.license_title or "unknown",
        "quebec_boroughs_last_modified": arrondissements.last_modified or "unknown",
    }

    # And the third city's, on the same footing and for the same reason. The
    # layer is small - twelve polygons - and every Saguenay partition is cut
    # against one of them.
    saguenay_package = quebec.package(saguenay.LIMITS_DATASET)
    limits = saguenay_package.resource(saguenay.LIMITS_GEOJSON)
    saguenay_frame = _geojson_to_frame(
        quebec.download(limits),
        source_file=limits.filename,
        scrape_date=scrape_date,
        scraped_at=scraped_at,
    )
    for column in (saguenay.LIMIT_NAME_FIELD, saguenay.LIMIT_TYPE_FIELD):
        if column not in saguenay_frame.columns:
            raise Failure(
                f"{limits.filename} has no {column} column; it publishes "
                f"{', '.join(sorted(saguenay_frame.columns))}."
            )
    saguenay_path = write_frame(
        saguenay_frame, join(output_dir, SAGUENAY_LIMITS_FILE)
    )
    saguenay_invalid = count_invalid_geometries(saguenay_frame)
    if saguenay_invalid:
        context.log.warning(
            "%s: %d invalid geometr(ies)", SAGUENAY_LIMITS_FILE, saguenay_invalid
        )
    context.log.info(
        "%s: %d limit(s) -> %s", limits.filename, len(saguenay_frame), saguenay_path
    )
    metadata |= {
        "num_saguenay_limits": len(saguenay_frame),
        "saguenay_limit_types": ", ".join(
            sorted(saguenay_frame[saguenay.LIMIT_TYPE_FIELD].dropna().astype(str).unique())
        ),
        "num_saguenay_invalid_geometries": saguenay_invalid,
        "saguenay_source_url": MetadataValue.url(
            f"{quebec_open_data.base_url}/dataset/{saguenay.LIMITS_DATASET}"
        ),
        "saguenay_license": saguenay_package.license_title or "unknown",
        "saguenay_limits_last_modified": limits.last_modified or "unknown",
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


class StreetNetworkConfig(Config):
    """Which cities' road networks to cut out of the province-wide RQTT.

    The archive covers Quebec, and this pipeline has boroughs in three cities,
    so the snapshot is bounded before it is read: the whole `Reseau_routier` is
    some millions of segments and an island's worth is ninety-odd thousand.
    The bound is a bounding box per city rather than an attribute filter,
    because the RQTT publishes no municipality code - see `hbu_dataplatform.sources.rqtt.client`.

    It defaults to every city `partitions.City` knows. Naming fewer is how a
    run is made cheap while working on one city; naming none is not allowed,
    because an empty street network is not a smaller snapshot, it is a broken
    one.

    `road_classes` and `road_characteristics` are the values *dropped*, and
    they default to `rqtt.EXCLUDED_ROAD_CLASSES` and
    `rqtt.EXCLUDED_ROAD_CHARACTERISTICS` - a ferry link, a footbridge, a
    railway bridge, a pipeline crossing. Read `rqtt.EXCLUDED_ROAD_CLASSES` for
    why highways and ramps are deliberately *not* on that list.
    """

    cities: list[str] = Field(
        default=[city.value for city in City],
        description="City names to bound the read by. Empty is refused.",
    )
    road_classes: list[str] = Field(
        default=list(EXCLUDED_ROAD_CLASSES),
        description="ClsRte values to drop. Empty keeps every class.",
    )
    road_characteristics: list[str] = Field(
        default=list(EXCLUDED_ROAD_CHARACTERISTICS),
        description="CaractRte values to drop. Empty keeps every one.",
    )


@asset(
    key_prefix=key_prefix("street_network"),
    partitions_def=date_partitions,
    deps=[reference_neighborhoods],
    group_name=GROUP,
    kinds={"geopackage", "geoparquet"},
    description=(
        "Quebec's road network from the RQTT, bounded to the cities this "
        "pipeline has boroughs in and snapshot per scrape date under "
        f"bronze/street_network/<YYYY-MM-DD>/{STREET_SEGMENTS_FILE}. One "
        "centre line per segment, as published: the borough slice is cut in "
        "silver. One file for every city, where Montreal's geobase double, "
        "Quebec City's vque_18 and Saguenay's sag-reseau-routier used to be "
        "three. Source: https://diffusion.mern.gouv.qc.ca - RQTT, CC-BY 4.0."
    ),
)
@guard_current_scrape_month
def street_network(
    context: AssetExecutionContext,
    config: StreetNetworkConfig,
    rqtt: RqttResource,
    store: ParquetStore,
) -> MaterializeResult:
    scrape_date = context.partition_key
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)

    if not config.cities:
        raise Failure(
            "street_network was asked for no cities; an empty street network "
            "is a broken snapshot rather than a cheap one."
        )
    try:
        cities = [City(name) for name in config.cities]
    except ValueError as exc:
        raise Failure(
            f"{exc}; the cities this platform knows are "
            f"{', '.join(city.value for city in City)}."
        ) from None

    fetcher = rqtt.fetcher()
    try:
        # The vintage is discovered, not asked for: the URL has no version in
        # it. It travels onto every row and into the metadata, because it is
        # the only record of which RQTT this partition was built from.
        geopackage, version = fetcher.geopackage()
    except RqttError as exc:
        raise Failure(f"RQTT read for {scrape_date} failed: {exc}") from exc

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    per_city: dict[str, int] = {}
    frames = []
    for city in cities:
        bounds = city_bounds(store, scrape_date, city)
        try:
            segments = read_layer(
                geopackage, ROAD_LAYER, bbox=bbox_in_source_crs(bounds)
            )
        except RqttError as exc:
            raise Failure(f"{city.value}: {exc}") from exc
        if segments.empty:
            raise Failure(
                f"The RQTT has no road segment inside {city.value}'s bounding "
                f"box {bounds}; check that reference_neighborhoods for "
                f"{scrape_date} holds that city's outline."
            )
        per_city[city.value] = len(segments)
        frames.append(segments)

    # Concatenated rather than kept apart: the three boxes are hundreds of
    # kilometres from each other, so a segment cannot be in two of them, and
    # silver clips each borough out of whatever is on disk. The de-duplication
    # is a guard against that assumption rather than a step that does work.
    network = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True), crs=frames[0].crs
    )
    boxed = len(network)
    network = network.drop_duplicates(subset=[STREET_ID_FIELD])

    dropped = boxed - len(network)
    network = roadway_only(
        network,
        classes=tuple(config.road_classes),
        characteristics=tuple(config.road_characteristics),
    ).reset_index(drop=True)
    not_roadway = boxed - dropped - len(network)

    if STREET_ID_FIELD not in network.columns:
        raise Failure(
            f"{ROAD_LAYER} has no {STREET_ID_FIELD} column; it publishes "
            f"{', '.join(sorted(network.columns))}."
        )

    # Column names are left exactly as the MRNF spells them - silver is where
    # `AQRP_UUID` becomes `COTE_RUE_ID` - the way the rest of the lot lineage
    # carries its publishers' names through bronze untouched.
    network["rqtt_version"] = version
    network["source_file"] = geopackage.name
    network["source_layer"] = ROAD_LAYER
    network["scrape_date"] = scrape_date
    network["scraped_at"] = scraped_at

    path = write_frame(network, join(output_dir, STREET_SEGMENTS_FILE))
    invalid = count_invalid_geometries(network)
    if invalid:
        # Reported, not repaired, so the snapshot stays a faithful copy.
        context.log.warning(
            "%s: %d invalid geometr(ies)", STREET_SEGMENTS_FILE, invalid
        )
    context.log.info(
        "RQTT %s: %d segment(s) across %s (%d not roadway, %d duplicate) -> %s",
        version,
        len(network),
        ", ".join(f"{name} {count}" for name, count in per_city.items()),
        not_roadway,
        dropped,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(network),
            "num_street_segments": len(network),
            "num_segments_by_city": MetadataValue.json(per_city),
            "num_not_roadway": not_roadway,
            "num_duplicate_ids": dropped,
            # The key silver declares its grain on. Reported rather than
            # enforced: bronze keeps whatever the publisher sent, and a
            # duplicate here is the publisher's fact, not this asset's failure.
            "num_street_ids": int(network[STREET_ID_FIELD].nunique()),
            "num_street_names": int(network[STREET_NAME_FIELD].nunique()),
            "num_unnamed_segments": int(network[STREET_NAME_FIELD].isna().sum()),
            "num_invalid_geometries": invalid,
            "road_classes": MetadataValue.json(
                network[ROAD_CLASS_FIELD].value_counts(dropna=False).to_dict()
                if ROAD_CLASS_FIELD in network.columns
                else {}
            ),
            # The one record of which RQTT this is: the URL states no version,
            # and the MRNF overwrites it three times a year.
            "rqtt_version": version,
            "geopackage_path": MetadataValue.path(str(geopackage)),
            "layers_not_read": MetadataValue.json(list(UNREAD_LAYERS)),
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(fetcher.url),
            "license": "CC-BY 4.0",
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
    one borough's shape - see `hbu_dataplatform.sources.infolot.assets` and
    `hbu_dataplatform.sources.bdoi.assets`. A Quebec City key is cut out of the
    arrondissement layer written beside the quartiers, on its abbreviation;
    a Saguenay key out of the administrative-limit layer written beside both.
    """
    city = city_of(neighborhood)
    if city is City.QUEBEC:
        return _quebec_boundary(store, scrape_date, neighborhood)
    if city is City.SAGUENAY:
        return _saguenay_boundary(store, scrape_date, neighborhood)

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


def _quebec_boundary(store: ParquetStore, scrape_date: str, neighborhood: str):
    """A Quebec City arrondissement's outline, off the layer as published.

    One polygon per arrondissement rather than a dissolve of smaller units,
    so `make_valid` is a guard here rather than a repair the layer has needed.
    """
    code = quebec_abbreviation_for(neighborhood)
    path = join(
        store.partition_dir(reference_neighborhoods.key.path[-1], scrape_date),
        QUEBEC_BOROUGHS_FILE,
    )
    fs = filesystem(path)
    if not fs.exists(path):
        raise Failure(
            f"{path} is missing; materialize reference_neighborhoods for "
            f"{scrape_date} first - it writes Quebec City's arrondissements "
            "beside the Montreal quartiers, and a snapshot taken before it did "
            "has no outline for a Quebec City borough."
        )
    with fs.open(path, "rb") as handle:
        boroughs = gpd.read_parquet(handle)

    borough = boroughs[boroughs[QUEBEC_ABBREVIATION_COLUMN].astype(str) == code]
    if borough.empty:
        published = sorted(
            boroughs[QUEBEC_ABBREVIATION_COLUMN].dropna().astype(str).unique()
        )
        raise Failure(
            f"No arrondissement carries {QUEBEC_ABBREVIATION_COLUMN}={code!r} "
            f"for {neighborhood}; the layer has: {', '.join(published)}"
        )
    return shapely.union_all(shapely.make_valid(borough.geometry.values._data))


def _saguenay_boundary(store: ParquetStore, scrape_date: str, neighborhood: str):
    """Saguenay's outline, off the administrative-limit layer.

    Selected on the *type* as well as the name, which neither other city needs.
    The layer publishes twelve polygons of three kinds under one schema - the
    city (``ville``), its three arrondissements, and the seven former
    municipalities amalgamated into it in 2002 (``secteur``) - so a lookup by
    name alone would find *Chicoutimi* twice over, once as an arrondissement
    and once as a secteur, and a union of the file would be the territory
    three times.
    """
    name = saguenay_outline_name_for(neighborhood)
    path = join(
        store.partition_dir(reference_neighborhoods.key.path[-1], scrape_date),
        SAGUENAY_LIMITS_FILE,
    )
    fs = filesystem(path)
    if not fs.exists(path):
        raise Failure(
            f"{path} is missing; materialize reference_neighborhoods for "
            f"{scrape_date} first - it writes Saguenay's administrative limits "
            "beside the other two cities' outlines, and a snapshot taken "
            "before it did has no outline for a Saguenay key."
        )
    with fs.open(path, "rb") as handle:
        limits = gpd.read_parquet(handle)

    wanted = limits[
        (limits[saguenay.LIMIT_TYPE_FIELD].astype(str) == SAGUENAY_OUTLINE_TYPE)
        & (limits[saguenay.LIMIT_NAME_FIELD].astype(str) == name)
    ]
    if wanted.empty:
        published = sorted(
            f"{row[saguenay.LIMIT_TYPE_FIELD]}/{row[saguenay.LIMIT_NAME_FIELD]}"
            for _, row in limits.iterrows()
        )
        raise Failure(
            f"No limit carries {saguenay.LIMIT_TYPE_FIELD}="
            f"{SAGUENAY_OUTLINE_TYPE!r} and {saguenay.LIMIT_NAME_FIELD}="
            f"{name!r} for {neighborhood}; the layer has: {', '.join(published)}"
        )
    return shapely.union_all(shapely.make_valid(wanted.geometry.values._data))


#: Which outline file each city's territory is bounded from, and how to select
#: the rows that are the territory itself. Montreal's quartiers and Quebec
#: City's arrondissements tile their city exactly, so every row counts;
#: Saguenay's layer holds three *kinds* of polygon under one schema, so only
#: the `ville` row does - the same trap `_saguenay_boundary` documents.
_CITY_OUTLINES: dict[City, str] = {
    City.MONTREAL: QUARTIERS_FILE,
    City.QUEBEC: QUEBEC_BOROUGHS_FILE,
    City.SAGUENAY: SAGUENAY_LIMITS_FILE,
}


def city_bounds(
    store: ParquetStore, scrape_date: str, city: City
) -> tuple[float, float, float, float]:
    """The city's whole territory as a WGS84 bounding box.

    What `street_network` cuts the province-wide RQTT down to before reading
    it. Deliberately the *city's* box and not a borough's: bronze holds one
    snapshot for every borough of a city, so a box drawn around the enabled
    boroughs would make the file depend on which keys happened to be
    registered, and adding a borough would silently invalidate every partition
    taken before it. The territory does not move; the partition list does.

    A box rather than the outline itself, because it is a filter and not a
    measurement. It is handed to OGR's R-tree, which indexes envelopes, and the
    segments it lets through that are outside the city proper are cut away in
    silver against the real boundary - see `hbu_dataplatform.sources.rqtt.assets`.
    """
    filename = _CITY_OUTLINES[city]
    path = join(
        store.partition_dir(reference_neighborhoods.key.path[-1], scrape_date),
        filename,
    )
    fs = filesystem(path)
    if not fs.exists(path):
        raise Failure(
            f"{path} is missing; materialize reference_neighborhoods for "
            f"{scrape_date} first - it writes every city's outline, and the "
            f"{city.value} road network is cut against this one."
        )
    with fs.open(path, "rb") as handle:
        outline = gpd.read_parquet(handle)

    if city is City.SAGUENAY:
        outline = outline[
            outline[saguenay.LIMIT_TYPE_FIELD].astype(str) == SAGUENAY_OUTLINE_TYPE
        ]
    if outline.empty:
        raise Failure(f"{path} holds no polygon to bound {city.value} with.")
    minx, miny, maxx, maxy = outline.total_bounds
    return (float(minx), float(miny), float(maxx), float(maxy))
