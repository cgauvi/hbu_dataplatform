"""One borough's cadastre, landed in PostGIS: the hop from the borough axis
to the tile axis.

A borough is what a *publisher* answers for. Infolot answers a boundary query
drawn from the borough's outline, BDOI is fetched the same way, and Spectrum
files its layers under a namespace that is the borough - so the three bronze
snapshots this asset reads are per borough by construction, and so is this
asset. A tile is what a *computation* is run over: a cell of the cut, about
one run's worth of lots wherever it is, with no borough line inside it to
bind a join. `building_lot_intersections` and everything after it run per
tile, and they read the lots not from the borough's parquet but from
`rag.lots`, where every row carries the cell that owns it. This asset is the
one place the two axes meet: it takes the borough's files off the tree and
puts them in the tables the tile runs compute over.

**This is also where the cadastre's geometry is repaired.**
`neighborhood_lots` reports self-intersecting rings and writes them through,
because a bronze snapshot is a faithful copy of what Infolot returned. Silver
owes its readers geometry they can compute with, and the readers here are
`ST_Intersection` in two separate PostGIS joins - which on an invalid ring
either raises or returns a shape nobody asked for. `make_valid` runs once, on
the way into `rag.lots`, and the count of rows it touched is reported so the
repair is visible rather than silent. One row per `NO_LOT` is checked at the
same time: Infolot answers a boundary query by object id, so the same lot can
come back twice when a borough outline is a multipolygon, and a duplicate here
would multiply every pair both joins produce - the kind of error that shows up
as a plausible-looking number rather than as a crash.

**A reload is a cascade, and `tiles_touched` names its reach.** `load_lots`
replaces the borough's rows and mints a new `lot_uid` for every one of them,
and every silver and gold table of the lot chain is keyed on that uid. So the
tiles this borough's lots fall in - read back from `rag.lots` after the load
and written into `cadastre.json` beside the counts - are exactly the tile
runs that have to follow, and `scripts/materialize_tiles.sh` reads the list
for that purpose. A borough that straddles two cells cascades into both.

The three loads are one transaction on purpose. Split across assets they
raced each other for `rag.lots`, and whoever committed second replaced the
rows the first had just computed against; here a reader never sees a borough
half landed, and a failure - a layer with nothing loadable, ground the cut
does not cover - rolls the lots and buildings back with it.

Depends on all three source assets for the *same* partition (date x
neighborhood): none of them is missing a dimension the others have, so the
partition mapping is the identity, unlike `reference_neighborhoods`'s
dependents which need `MultiToSingleDimensionPartitionMapping`.
"""

import io
import json

import geopandas as gpd
import shapely
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from hbu_dataplatform.core import tile_cut
from hbu_dataplatform.zoning.features_assets import neighborhood_features
from hbu_dataplatform.sources.bdoi.assets import BUILDINGS_FILE, neighborhood_buildings
from hbu_dataplatform.core.frames import count_invalid_geometries
from hbu_dataplatform.sources.infolot.assets import LOTS_FILE, neighborhood_lots
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.partitions.axes import (
    borough_partition_of,
    scrape_partitions,
    source_namespace_for,
)
from hbu_dataplatform.core.postgis import (
    GroundOutsideCut,
    MissingRelation,
    load_buildings,
    load_features,
    load_lots,
    require_working_set,
    tiles_of_neighborhood,
)
from hbu_dataplatform.core.pg import PostgresUnavailable
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.core.storage import (
    basename,
    clear_files,
    filesystem,
    join,
    storage_options,
    write_bytes,
)

GROUP = "silver_cadastre"

#: What landed, under `silver/neighborhood_cadastre/<YYYY-MM-DD>/<neighborhood>/`.
#: Not a dataset: the rows went to `rag.*`, and the tree's record of the load
#: is the counts and the tiles they fell in.
CADASTRE_FILE = "cadastre.json"

#: Infolot's lot number, and the grain this asset repairs the cadastre at
#: before it is loaded.
LOT_NUMBER_COLUMN = "NO_LOT"

#: Columns that hold the id a document cites, in precedence order. The same
#: list `rag_assets._ID_COLUMNS` reads when it writes `feature_ids` onto a
#: chunk - the two have to agree or the join they exist for matches nothing.
#: Named here rather than imported from there because the dependency runs the
#: wrong way: `rag_assets` builds the corpus, this builds the geometry it is
#: about, and neither is downstream of the other. ``IGDS_TEXT_STRING`` is the
#: zone code on Quebec City's zoning layer - see `hbu_dataplatform.cities.quebec_city.zoning`;
#: ``no_zone`` is Saguenay's - see `hbu_dataplatform.cities.saguenay.zoning`. It precedes ``ID``
#: because Saguenay's layer carries both, and the zone code is the one a grid
#: is keyed on; its ``id`` is the reporting service's own primary key, which
#: no document cites. `test_cadastre` pins this tuple to the other.
FEATURE_ID_COLUMNS = ("NUMERO_COMPLET", "no_zone", "ID", "IGDS_TEXT_STRING")


@asset(
    key_prefix=key_prefix("neighborhood_cadastre"),
    partitions_def=scrape_partitions,
    deps=[neighborhood_lots, neighborhood_buildings, neighborhood_features],
    group_name=GROUP,
    kinds={"postgres"},
    description=(
        "One borough's cadastre landed in PostGIS: the hop from the borough "
        "axis, which is what a publisher answers for, to the tile axis, which "
        "is what a computation is run over. Loads this partition's "
        "neighborhood_lots (with its self-intersecting rings repaired, which "
        "is what makes ST_Intersection over them mean anything), "
        "neighborhood_buildings and neighborhood_features snapshots into "
        "rag.lots/rag.buildings/rag.features in one transaction, each row "
        "stamped with the cut cell that owns it, and fails if any ground "
        "falls outside the cut. Reloading a borough remints every lot_uid "
        "and cascades into every tile its lots fall in; tiles_touched names "
        "them, and writes them with the counts to silver/"
        f"neighborhood_cadastre/<YYYY-MM-DD>/<neighborhood>/{CADASTRE_FILE}."
    ),
)
def neighborhood_cadastre(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)

    lots_path = join(
        store.partition_dir(neighborhood_lots.key.path[-1], scrape_date, neighborhood),
        LOTS_FILE,
    )
    buildings_path = join(
        store.partition_dir(
            neighborhood_buildings.key.path[-1], scrape_date, neighborhood
        ),
        BUILDINGS_FILE,
    )

    lots = _read_geoparquet(lots_path)
    buildings = _read_geoparquet(buildings_path)
    if lots.empty:
        raise Failure(f"{lots_path} holds no lot to load.")
    if buildings.empty:
        raise Failure(f"{buildings_path} holds no building to load.")

    # Bronze reports invalid rings and keeps them; what reads this frame is
    # `ST_Intersection` twice over, which on a self-intersecting ring either
    # raises or answers a question nobody asked. Repaired before the load, so
    # what both joins see is what lands in rag.lots.
    repaired = count_invalid_geometries(lots)
    if repaired:
        lots = _make_valid(lots)
        context.log.info("Repaired %d invalid lot geometr(ies)", repaired)
    still_invalid = count_invalid_geometries(lots)
    if still_invalid:
        raise Failure(
            f"{still_invalid} lot geometr(ies) are still invalid after "
            "make_valid; the partition cannot be joined against."
        )
    _require_unique_lots(lots, neighborhood=neighborhood, scrape_date=scrape_date)

    features_dir = store.partition_dir(
        neighborhood_features.key.path[-1], scrape_date, neighborhood
    )
    feature_paths = _partition_parquet(features_dir)
    if not feature_paths:
        raise Failure(f"{features_dir} holds no feature parquet.")

    loaded: dict[str, int] = {}
    skipped: dict[str, str] = {}
    try:
        with postgis.connect() as connection:
            # Before the first load, not on the way into it. All three tables
            # come from one hbu_infra file, so a database that has never had
            # it applied should say so once and name the file - rather than
            # failing on whichever `DELETE FROM rag.<x>` ran first, with an
            # identifier and no owner.
            require_working_set(connection)
            # Once, for every tile run that follows - the reason the load is
            # an asset of its own.
            num_lots = load_lots(
                connection, lots, neighborhood=neighborhood, scrape_date=scrape_date
            )
            num_buildings = load_buildings(
                connection,
                buildings,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
            )
            for path in feature_paths:
                # The slug the file is named for, which is also what
                # `rag_assets.linked_documents` writes into
                # `rag.chunks.source_table` - not the Spectrum path the
                # parquet carries in a column of the same name. See
                # `postgis.load_features`.
                slug = basename(path)[: -len(".parquet")]
                frame = _read_parquet(path)
                reason = _unloadable(frame)
                if reason:
                    skipped[slug] = reason
                    context.log.info("%s: skipped - %s", slug, reason)
                    continue
                loaded[slug] = load_features(
                    connection,
                    frame,
                    neighborhood=neighborhood,
                    scrape_date=scrape_date,
                    source_table=slug,
                    # From the registry rather than from the frame's own
                    # column: parquet written before this change carries no
                    # such column, and the registry is the definition either
                    # way.
                    source_namespace=source_namespace_for(neighborhood),
                    feature_id_column=_id_column(frame),
                )
            # Inside the transaction on purpose: raising here rolls the loaded
            # lots and buildings back rather than leaving a borough half
            # landed with no features to join them to.
            if not loaded:
                raise Failure(
                    f"{features_dir}: none of its {len(feature_paths)} table(s) "
                    f"carries geometry and one of {FEATURE_ID_COLUMNS}, so no "
                    "feature could be loaded."
                )
            # After the load and inside its transaction, so the list is the
            # cells these rows were just stamped with and not what an earlier
            # snapshot of the borough fell in.
            tiles = tiles_of_neighborhood(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
    except PostgresUnavailable as exc:
        raise Failure(f"Postgres unreachable for {neighborhood} {scrape_date}: {exc}")
    except MissingRelation as exc:
        raise Failure(str(exc))
    except GroundOutsideCut as exc:
        # The loaders stamp every row with its cut cell and refuse a row that
        # gets none: a lot no tile owns is a lot no run will ever compute
        # over. The transaction has rolled back; the message names the ground
        # and the fix.
        raise Failure(str(exc)) from exc

    num_features = sum(loaded.values())
    record = {
        "neighborhood": neighborhood,
        "scrape_date": scrape_date,
        "num_lots": num_lots,
        "num_geometries_repaired": repaired,
        "num_buildings": num_buildings,
        "num_features": num_features,
        "features_per_layer": loaded,
        "skipped_layers": skipped,
        "cut_version": tile_cut.CUT_VERSION,
        "tiles_touched": list(tiles),
    }
    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_files(output_dir, "*.json")
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = join(output_dir, CADASTRE_FILE)
    write_bytes(path, io.BytesIO(json.dumps(record, indent=2).encode("utf-8")))

    context.log.info(
        "%s %s: %d lot(s), %d building(s), %d feature(s) across %d layer(s) "
        "landed in rag.*, falling in %d tile(s): %s -> %s",
        neighborhood,
        scrape_date,
        num_lots,
        num_buildings,
        num_features,
        len(loaded),
        len(tiles),
        ", ".join(tiles),
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": num_lots,
            "num_lots": num_lots,
            # What bronze handed over and this asset had to fix, rather than
            # what is left: after the guard above, what is left is always zero.
            "num_geometries_repaired": repaired,
            "num_buildings": num_buildings,
            "num_features": num_features,
            "num_layers": len(loaded),
            "num_layers_skipped": len(skipped),
            "skipped": MetadataValue.json(skipped),
            "features_per_layer": MetadataValue.json(loaded),
            # Always zero on a run that got this far - the loaders raise
            # `GroundOutsideCut` on the first row that has no cell - and
            # reported anyway, so the run says it checked.
            "num_rows_outside_cut": 0,
            # The cascade: every tile run that has to follow this load.
            "tiles_touched": ", ".join(tiles),
            "num_tiles": len(tiles),
            "output_path": MetadataValue.path(str(path)),
        }
    )


def _partition_parquet(directory: str) -> list[str]:
    """Every parquet in ``directory``, as paths in the store's own scheme.

    `fsspec.glob` drops the `s3://` prefix from what it returns, so the names
    are rejoined onto the directory rather than used as they come back.
    """
    fs = filesystem(directory)
    if not fs.exists(directory):
        return []
    found = fs.glob(join(directory, "*.parquet"))
    return sorted(join(directory, basename(path)) for path in found)


def _read_geoparquet(path: str) -> gpd.GeoDataFrame:
    return gpd.read_parquet(path, storage_options=storage_options(path))


def _make_valid(lots: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """``lots`` with every self-intersecting ring repaired.

    Applied to the whole column rather than to the invalid rows only: shapely's
    `make_valid` is a no-op on geometry that is already valid, and selecting
    first would cost a second validity pass to save nothing.
    """
    repaired = lots.copy()
    return repaired.set_geometry(
        gpd.GeoSeries(
            shapely.make_valid(repaired.geometry.values._data),
            index=repaired.index,
            crs=repaired.crs,
        )
    )


def _require_unique_lots(
    lots: gpd.GeoDataFrame, *, neighborhood: str, scrape_date: str
) -> None:
    """One row per lot number - the grain the joins downstream are computed at.

    Infolot answers a boundary query by object id, so the same lot can come
    back twice when a borough outline is a multipolygon and the lot straddles
    two of its rings. Bronze keeps both rows; a duplicate here would multiply
    every pair the two spatial joins produce, which is the kind of error that
    shows up as a plausible-looking number rather than as a crash.

    `load_lots` would swallow it - it resolves a repeated lot number with
    `ON CONFLICT ... DO NOTHING` - so the check has to be here, where the
    duplicate can still be reported rather than silently dropped.
    """
    if LOT_NUMBER_COLUMN not in lots.columns:
        return
    numbers = lots[LOT_NUMBER_COLUMN]
    duplicated = numbers[numbers.duplicated(keep=False)]
    if not duplicated.empty:
        repeated = sorted(set(duplicated.astype(str)))
        raise Failure(
            f"{neighborhood} {scrape_date}: {len(repeated)} lot number(s) appear "
            f"more than once, e.g. {', '.join(repeated[:5])}. "
            f"One row per {LOT_NUMBER_COLUMN} is the grain the joins downstream "
            "are computed at."
        )


def _read_parquet(path: str):
    """A layer's parquet, as a GeoDataFrame when it has geometry.

    `neighborhood_features` writes tables without geometry as plain parquet,
    which `gpd.read_parquet` refuses rather than degrades, so the fallback is
    what tells the two apart.
    """
    try:
        return gpd.read_parquet(path, storage_options=storage_options(path))
    except (ValueError, AttributeError):
        import pandas as pd

        return pd.read_parquet(path, storage_options=storage_options(path))


def _id_column(frame) -> str | None:
    return next((c for c in FEATURE_ID_COLUMNS if c in frame.columns), None)


def _unloadable(frame) -> str | None:
    """Why this layer cannot be loaded into `rag.features`, or None."""
    if not isinstance(frame, gpd.GeoDataFrame):
        return "no geometry"
    if frame.empty:
        return "no rows"
    if _id_column(frame) is None:
        return f"no id column (looked for {', '.join(FEATURE_ID_COLUMNS)})"
    return None
