"""Civic addresses: the province's address points, and the parcels they stand on.

Two assets over one publisher — Adresses Québec, the MRNF's official address
points for the whole province (`hbu_dataplatform.adresses_quebec`, which also says why
the REST layer is read rather than the WMS endpoint the product is advertised
under, and why a "query by lot" is not something that service can be asked).

`neighborhood_addresses` (bronze) is the borough's points as published.
`lot_addresses` (silver) is the spatial join that puts each of them on a
parcel and, within it, on the zone piece this platform answers questions at —
so an address is joinable to every gold table on `(lot_uid, feature_id)`.

**The two run on two axes.** The fetch is a borough's, because the publisher
is asked with a borough's outline. The join is a tile's - a cut cell's worth of
points, `rag.addresses.cell_partition` - against every parcel in the snapshot,
so a point two metres over a borough line still lands on the parcel it stands
in front of. The tile's boroughs are read off its lots
(`postgis.neighborhoods_of_tile`), and each one's bronze snapshot is parsed and
loaded before the join runs; a tile spanning two boroughs loads both. A row
belongs to the tile its *point* is in, not the tile of the lot it is placed on
- see `postgis.load_addresses`.

**Why the join has to exist.** The address layer publishes ten fields and not
one of them is cadastral. There is no lot number, no matricule, nothing that
points at a parcel. So the only way to say "2 784 705 is 7430 Rue Lajeunesse"
is to put the point on the polygon, which is `postgis.compute_lot_addresses`.

**What this buys the two readers downstream.** The map can label a parcel with
something a person recognises instead of a nine-digit lot number, and the
corpus can answer "what is at 7430 Rue Lajeunesse" by matching a street and a
number rather than by hoping the phrase appears in a zoning PDF. Both need the
same thing from this asset — an address on the *piece* grain, because that is
what `gold.lot_highest_best_use`, `gold.lot_redevelopment_gap` and
`gold.lot_investment_opportunities` are keyed on since the grain changed.

**The parse is silver's work and it happens before the load.**
`AdresseFormatee` is the only place the street name, the municipality and the
unit exist, and taking a string apart is interpretation rather than transport —
so bronze keeps the string and `lot_addresses` splits it, in Python, before the
points go into `rag.addresses`. The join then reads the parts back out of the
jsonb rather than re-implementing a regex in SQL.

**`silver.lot_addresses` and `rag.addresses` are owned by hbu_infra** —
sql/026_silver_lot_addresses.sql. Until it is applied a run fails naming the
file, which is why the silver asset is registered and given a job but left off
the daily schedules, the posture `lot_frontage` and `lot_zone_pieces` take.
"""

from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Config,
    DimensionPartitionMapping,
    Failure,
    IdentityPartitionMapping,
    MaterializeResult,
    MetadataValue,
    MultiPartitionMapping,
    MultiToSingleDimensionPartitionMapping,
    asset,
)
from pydantic import Field

from hbu_dataplatform.adresses_quebec import (
    ADDRESS_ID_FIELD,
    DEFAULT_WMS_URL,
    AdressesQuebecError,
    civic_address,
    esri_query_geometry,
    features_to_frame,
    parse_addresses,
)
from hbu_dataplatform.building_lots_assets import building_lot_intersections
from hbu_dataplatform.frames import count_invalid_geometries, write_frame
from hbu_dataplatform.guards import guard_current_scrape_month
from hbu_dataplatform.layers import key_prefix
from hbu_dataplatform.open_data_assets import borough_boundary, reference_neighborhoods
from hbu_dataplatform.partitions import (
    scrape_partitions,
    tile_partition_of,
    tile_scrape_partitions,
)
from hbu_dataplatform.postgis import (
    DEFAULT_ADDRESS_SNAP_M,
    MissingRelation,
    compute_lot_addresses,
    fetch_lot_addresses,
    load_addresses,
    neighborhoods_of_tile,
)
from hbu_dataplatform.rag.pgvector import PostgresUnavailable
from hbu_dataplatform.resources import AdressesQuebecResource, ParquetStore, PostgisResource
from hbu_dataplatform.storage import clear_parquet, filesystem, join, storage_options
from hbu_dataplatform.zone_piece_assets import lot_zone_pieces

BRONZE_GROUP = "bronze_addresses"
SILVER_GROUP = "silver_addresses"

#: The one file a bronze partition is written to, under
#: `bronze/neighborhood_addresses/<YYYY-MM-DD>/<neighborhood>/`.
ADDRESSES_FILE = "addresses.parquet"

#: The one file a silver partition is written to, under
#: `silver/lot_addresses/<YYYY-MM-DD>/<tile>/`.
LOT_ADDRESSES_FILE = "lot_addresses.parquet"

#: How the tile-axis join depends on the borough-axis fetch: the date maps to
#: itself and the borough dimension is left unlisted, which Dagster reads as
#: "all of them". A tile does not know its boroughs until it reads its lots,
#: so the dependency cannot name them. The same bridge `lot_zoning_envelopes`
#: crosses to `zoning_grid_columns`.
_BOROUGH_BRIDGE = MultiPartitionMapping(
    {"date": DimensionPartitionMapping("date", IdentityPartitionMapping())}
)

#: https://www.donneesquebec.ca/recherche/dataset/adresses-quebec
SOURCE_URL = "https://www.donneesquebec.ca/recherche/dataset/adresses-quebec"

#: How far short of its own stated count the paged walk may come before the
#: partition is refused, as a percentage and as an absolute floor - whichever
#: is larger, so a small borough is not failed by a rounding and a large one is
#: not passed by a thousand missing rows.
#:
#: The count and the walk are two requests against a live service, so they are
#: not taken at the same instant: a borough that gains or loses an address in
#: between is an ordinary few rows and is logged rather than raised. What this
#: catches is the other cause - pages that are not a partition of the answer,
#: which is what `resultOffset` without a stable `orderByField` produces, and
#: which loses rows without any error at all.
MAX_PAGING_SHORTFALL_PCT = 1.0
MIN_PAGING_SHORTFALL = 25


# ---------------------------------------------------------------------------
# bronze
# ---------------------------------------------------------------------------


@asset(
    key_prefix=key_prefix("neighborhood_addresses"),
    partitions_def=scrape_partitions,
    deps=[
        AssetDep(
            reference_neighborhoods,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        )
    ],
    group_name=BRONZE_GROUP,
    kinds={"geoparquet"},
    description=(
        "Adresses Quebec's official civic address points for one borough, as "
        "bronze/neighborhood_addresses/<YYYY-MM-DD>/<neighborhood>/"
        f"{ADDRESSES_FILE}. The MRNF's province-wide layer is queried with "
        "that borough's outline from reference_neighborhoods as a spatial "
        "filter, paged 1000 rows at a time, then cut to the outline itself. "
        "Ten published fields, unrepaired and unparsed - including "
        "AdresseFormatee, the only place the street name exists, and Version, "
        "the publisher's own freshness stamp. A row is an addressable unit "
        f"rather than a front door. Source: {SOURCE_URL}"
    ),
)
@guard_current_scrape_month
def neighborhood_addresses(
    context: AssetExecutionContext,
    addresses: AdressesQuebecResource,
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

    client = addresses.client()
    _, filter_type, filter_vertices = esri_query_geometry(boundary)
    context.log.info(
        "Querying %s with a %s of %d vertices",
        addresses.service_url,
        filter_type,
        filter_vertices,
    )

    try:
        expected = client.count(boundary)
        features = list(client.fetch_addresses(boundary))
    except AdressesQuebecError as exc:
        raise Failure(f"Adresses Quebec read for {neighborhood} failed: {exc}")

    # The service said how many there were before the paging started, so a
    # short walk is a fact rather than something to infer from the last page.
    # A handful either way is the borough gaining or losing an address between
    # the two calls, which is ordinary; a material shortfall means the paging
    # is not partitioning the answer, and that is silent data loss.
    shortfall = expected - len(features)
    duplicates = len(features) - len({_address_id(f) for f in features})
    if shortfall > 0 and shortfall > max(
        MIN_PAGING_SHORTFALL, expected * MAX_PAGING_SHORTFALL_PCT / 100.0
    ):
        raise Failure(
            f"Adresses Quebec said {expected} address(es) intersect "
            f"{neighborhood} and the paged walk returned {len(features)}, "
            f"{shortfall} short ({100.0 * shortfall / expected:.1f}%). That is "
            "past what the layer changing under the query explains, so the "
            "pages are not partitioning the answer - check that the service "
            "still honours resultOffset under orderByFields before trusting "
            "this partition."
        )
    if shortfall or duplicates:
        context.log.warning(
            "Asked for %d address(es), paged %d (%d duplicate id(s)) - the "
            "layer changed under the query between the two calls",
            expected,
            len(features),
            duplicates,
        )

    frame = features_to_frame(features)
    if frame.empty:
        raise Failure(
            f"Adresses Quebec returned no address for {neighborhood} "
            f"{scrape_date}; its boundary in reference_neighborhoods may be "
            "empty, or the spatial filter may be wound the wrong way."
        )

    points = gpd.GeoDataFrame(
        frame.drop(columns=["longitude", "latitude"]),
        geometry=gpd.points_from_xy(frame["longitude"], frame["latitude"]),
        crs="EPSG:4326",
    )
    # The spatial filter is the service's; this is the cut that is ours. A
    # polygon filter and a bounding-box fallback then differ only in how much
    # was transferred - see `adresses_quebec.esri_query_geometry`.
    inside = points[points.within(boundary)].copy()
    if inside.empty:
        raise Failure(
            f"None of the {len(points)} address(es) the service returned falls "
            f"inside {neighborhood}'s outline for {scrape_date}."
        )

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    inside["neighborhood"] = neighborhood
    inside["scrape_date"] = scrape_date
    inside["scraped_at"] = scraped_at

    path = write_frame(inside, join(output_dir, ADDRESSES_FILE))

    versions = sorted({str(v) for v in inside["Version"].dropna().unique()})
    num_units = int(pd.to_numeric(inside["NbUnite"], errors="coerce").fillna(0).sum())
    context.log.info(
        "%s %s: %d address point(s) of %d returned, %s -> %s",
        neighborhood,
        scrape_date,
        len(inside),
        len(points),
        f"version {', '.join(versions)}" if versions else "no version stated",
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(inside),
            "num_addresses": len(inside),
            "num_returned": len(points),
            "num_outside_boundary": len(points) - len(inside),
            # The two numbers that say whether the paged walk was a partition
            # of the answer: what the service said was there before it started,
            # and how many ids came back twice.
            "num_service_count": expected,
            "num_duplicate_ids": duplicates,
            "num_stated_units": num_units,
            "num_invalid_geometries": count_invalid_geometries(inside),
            # The publisher's own freshness stamp, and the only thing it says
            # about how old these addresses are. More than one means the
            # borough was answered out of two editions of the layer.
            "source_version": ", ".join(versions) or "unstated",
            "query_geometry_type": filter_type,
            "query_geometry_vertices": filter_vertices,
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(SOURCE_URL),
            "source_service_url": MetadataValue.url(addresses.service_url),
            # Recorded, not read - see `AdressesQuebecResource.wms_url`.
            "source_wms_url": MetadataValue.url(DEFAULT_WMS_URL),
        }
    )


# ---------------------------------------------------------------------------
# silver
# ---------------------------------------------------------------------------


class AddressJoinConfig(Config):
    """How far off a parcel an address may sit and still be that parcel's."""

    max_snap_m: float = Field(
        default=DEFAULT_ADDRESS_SNAP_M,
        ge=0.0,
        description=(
            "An address point outside every parcel is given the nearest one "
            "within this many metres, and records match_basis='snapped'. The "
            "address points and the cadastre are two publishers' surveys of "
            "the same ground and disagree at the edges; 0 disables the "
            "fallback and drops those addresses instead."
        ),
    )


@asset(
    key_prefix=key_prefix("lot_addresses"),
    partitions_def=tile_scrape_partitions,
    deps=[
        # The fetch is a borough's; the join is a tile's. See `_BOROUGH_BRIDGE`.
        AssetDep(neighborhood_addresses, partition_mapping=_BOROUGH_BRIDGE),
        building_lot_intersections,
        lot_zone_pieces,
    ],
    group_name=SILVER_GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "Every civic address standing on a parcel, at the lot x zone piece "
        "grain the gold tables are keyed on: one row per address point, "
        "stating its lot_uid, lot_number and feature_id, so an address joins "
        "straight onto lot_highest_best_use, lot_redevelopment_gap and "
        "lot_investment_opportunities. AdresseFormatee is split into unit, "
        "civic number, suffix, street, municipality and postal code before "
        "the load. A point inside no parcel but within max_snap_m of one is "
        "given it and says so in match_basis; a point on nobody's parcel is "
        "counted and not written. address_rank orders a site's addresses by "
        "civic number, so is_primary_address is the one a map labels it with, "
        "and num_piece_addresses (units) is reported beside "
        "num_piece_civic_addresses (doors). Loads the bronze points of every "
        "borough the tile's lots belong to, then joins the tile's own points "
        "to every parcel in the snapshot. Upserted into silver.lot_addresses "
        "on (scrape_date, cell_partition, address_id) and written to "
        f"silver/lot_addresses/<YYYY-MM-DD>/<tile>/{LOT_ADDRESSES_FILE}."
    ),
)
def lot_addresses(
    context: AssetExecutionContext,
    config: AddressJoinConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    tile, scrape_date = tile_partition_of(context)

    try:
        with postgis.connect() as connection:
            # The boroughs this tile's lots were fetched under, which is where
            # the address points were fetched too. Asked of the database
            # rather than of the tree because nothing in the tree says which
            # boroughs a cell holds - the lots do.
            neighborhoods = neighborhoods_of_tile(
                connection, tile=tile, scrape_date=scrape_date
            )
            if not neighborhoods:
                raise Failure(
                    f"rag.lots holds no lot for tile {tile} {scrape_date}, so "
                    "there is no borough to read address points for - "
                    "materialize neighborhood_cadastre for the boroughs this "
                    "tile covers first."
                )
            loaded = 0
            for neighborhood in neighborhoods:
                parsed = _parsed_points(context, store, neighborhood, scrape_date)
                # Per borough, because the bronze snapshot and the table's
                # replace are both per borough; a second tile of the same
                # borough loads the same rows again, which the load resolves
                # on the publisher's id. The rows are addressed by their own
                # point on the way in - see `postgis.load_addresses`.
                loaded += load_addresses(
                    connection,
                    parsed,
                    neighborhood=neighborhood,
                    scrape_date=scrape_date,
                )
            result = compute_lot_addresses(
                connection,
                tile=tile,
                scrape_date=scrape_date,
                max_snap_m=config.max_snap_m,
            )
            if int(result["num_addresses"]) == 0:
                # Inside the transaction, so an empty answer rolls back rather
                # than replacing a good partition with nothing - the posture
                # `lot_zone_pieces` takes.
                raise Failure(
                    f"tile {tile} {scrape_date}: none of the {loaded} address "
                    f"point(s) loaded for {', '.join(neighborhoods)} falls on "
                    "a parcel of this tile. Check that neighborhood_cadastre "
                    "landed those boroughs' lots, and that the two layers are "
                    "in the same CRS."
                )
            frame = fetch_lot_addresses(connection, tile=tile, scrape_date=scrape_date)
    except PostgresUnavailable as exc:
        raise Failure(f"Postgres unreachable for tile {tile} {scrape_date}: {exc}")
    except MissingRelation as exc:
        raise Failure(str(exc))

    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, tile)
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_ADDRESSES_FILE))

    num_addresses = int(result["num_addresses"])
    num_points = int(result["num_points"])
    num_unmatched = int(result["num_unmatched"])
    context.log.info(
        "%s %s: %d address(es) on %d lot(s) over %d zone piece(s); %d of %d "
        "point(s) reached no parcel -> %s",
        tile,
        scrape_date,
        num_addresses,
        int(result["num_lots"]),
        int(result["num_pieces"]),
        num_unmatched,
        num_points,
        path,
    )
    if num_unmatched:
        context.log.warning(
            "%s %s: %d of %d address point(s) fall on no parcel within %g m "
            "and are not written - they sit in the right of way, or on ground "
            "the cadastre did not draw",
            tile,
            scrape_date,
            num_unmatched,
            num_points,
            config.max_snap_m,
        )
    num_no_piece = int(result["num_no_piece"])
    if num_no_piece:
        context.log.warning(
            "%s %s: %d address(es) stand on a lot no zoning layer governs and "
            "carry '-' for feature_id, so they join to no gold row",
            tile,
            scrape_date,
            num_no_piece,
        )

    return MaterializeResult(
        metadata={
            "dagster/row_count": num_addresses,
            "tile": tile,
            # Whose points were loaded on the way to the join.
            "neighborhoods": ", ".join(neighborhoods),
            "num_addresses": num_addresses,
            "num_lots": int(result["num_lots"]),
            "num_pieces": int(result["num_pieces"]),
            # The tile's own points, which is what the join ran over. The
            # boroughs' whole snapshots were loaded and are a larger number.
            "num_points_loaded": num_points,
            "num_points_loaded_for_boroughs": loaded,
            # The number to watch, and the pair to read together: how many
            # points the cadastre could not place, and how many it placed only
            # by reaching for them.
            "num_unmatched": num_unmatched,
            "num_snapped": int(result["num_snapped"]),
            "pct_unmatched": round(
                100.0 * num_unmatched / num_points if num_points else 0.0, 2
            ),
            # A unit is not a door - see the module docstring.
            "num_unit_addresses": int(result["num_unit_addresses"]),
            "num_addresses_on_primary_piece": int(result["num_primary_piece"]),
            "num_addresses_without_piece": num_no_piece,
            "num_unparsed": int(result["num_unparsed"]),
            "max_snap_m": config.max_snap_m,
            "output_path": MetadataValue.path(str(path)),
            "rows_upserted": int(result.get("upserted", 0)),
            "rows_deleted": int(result.get("deleted", 0)),
        }
    )


def parse_address_frame(points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """The bronze snapshot with `AdresseFormatee` taken apart.

    Renames the publisher's columns to this platform's vocabulary and adds the
    six parsed ones plus `civic_address`. Kept a module function rather than
    an inner one so the shape of what reaches `rag.addresses` can be tested
    without a database - `postgis.compute_lot_addresses` reads every one of
    these names back out of the jsonb, so a rename here is a silent null
    there.
    """
    frame = points.copy()
    parsed = parse_addresses(frame["AdresseFormatee"].astype("string"))

    out = gpd.GeoDataFrame(
        {
            "address_id": frame[ADDRESS_ID_FIELD].astype("string"),
            "object_id": pd.to_numeric(frame["OBJECTID"], errors="coerce").astype(
                "Int64"
            ),
            "formatted_address": frame["AdresseFormatee"].astype("string"),
            # The publisher's own columns win over the parsed ones for the two
            # facts it states outright: the string is where the street lives,
            # not where the number does.
            "civic_number": pd.to_numeric(frame["NoCivq"], errors="coerce").astype(
                "Int64"
            ),
            "civic_suffix": frame["NoCivqSuf"].astype("string"),
            "unit": parsed["unit"].astype("string"),
            "street_name": parsed["street"].astype("string"),
            "municipality": parsed["municipality"].astype("string"),
            "postal_code": parsed["postal_code"].astype("string"),
            "num_units": pd.to_numeric(frame["NbUnite"], errors="coerce").astype(
                "Int64"
            ),
            "characteristic": frame["CaractAdr"].astype("string"),
            "source_version": frame["Version"].astype("string"),
        },
        geometry=frame.geometry,
        crs=frame.crs,
    )
    out["civic_address"] = [
        civic_address(number, suffix, street)
        for number, suffix, street in zip(
            out["civic_number"], out["civic_suffix"], out["street_name"]
        )
    ]
    return out


def _parsed_points(
    context: AssetExecutionContext,
    store: ParquetStore,
    neighborhood: str,
    scrape_date: str,
) -> gpd.GeoDataFrame:
    """One borough's bronze snapshot, parsed and ready for `load_addresses`.

    The parse is silver's work - see the module docstring - and it happens
    here, per borough, because the bronze file is a borough's. A snapshot that
    did not parse a street is warned about and loaded anyway: the row's point
    is still on the right lot, and how many there are is what says whether
    the parser needs widening.
    """
    bronze_path = join(
        store.partition_dir(
            neighborhood_addresses.key.path[-1], scrape_date, neighborhood
        ),
        ADDRESSES_FILE,
    )
    points = _read_addresses(bronze_path)
    if points.empty:
        raise Failure(f"{bronze_path} holds no address to join.")

    parsed = parse_address_frame(points)
    unparsed = int(parsed["street_name"].isna().sum())
    if unparsed:
        context.log.warning(
            "%s %s: %d of %d formatted address(es) did not parse; their "
            "street, unit and municipality are null",
            neighborhood,
            scrape_date,
            unparsed,
            len(parsed),
        )
    return parsed


def _address_id(feature: dict) -> str | None:
    """One GeoJSON feature's publisher id, for the duplicate count above."""
    return (feature.get("properties") or {}).get(ADDRESS_ID_FIELD)


def _read_addresses(path: str) -> gpd.GeoDataFrame:
    """The bronze geoparquet, or a `Failure` naming the asset that writes it."""
    fs = filesystem(path)
    if not fs.exists(path):
        raise Failure(
            f"{path} is missing; materialize bronze/neighborhood_addresses for "
            "this partition first."
        )
    return gpd.read_parquet(path, storage_options=storage_options(path))
