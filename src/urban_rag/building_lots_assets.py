"""What sits on a lot and what covers it: two PostGIS joins, one asset.

Both joins hang off the same left-hand side. `neighborhood_lots` writes one
borough's cadastre to the tree, and two different questions are asked of it
that no layer answers on its own:

* **which buildings stand on this lot.** A footprint does not respect a
  cadastral boundary - a school, a warehouse, an apartment tower can each span
  two or three lots - so "which lot is this building on" is not a column
  `neighborhood_buildings` has.
* **which map features cover this lot.** `neighborhood_lots` comes from
  Infolot, Quebec's cadastre, keyed by `NO_LOT`; `neighborhood_features` comes
  from Montreal's Spectrum service, keyed by `NUMERO_COMPLET`. The two
  publishers share nothing but the ground, so the hop from a lot to the zone
  covering it - and from there, through `rag.chunks.feature_ids`, to the PDFs
  the corpus holds about it - is spatial and has to be computed.

They are one asset because they are one load. Both need this borough's lots in
`rag.lots`, and splitting them meant loading that table twice per partition,
from the same file, in two transactions that raced each other for it: whoever
committed second replaced the rows the first had just computed against. Here
the lots land once, both joins are computed against them, and both are read
back - all inside a single transaction, so a reader never sees one join built
against a half-replaced cadastre.

The joins are *computed* in Postgres: by the time this runs PostGIS holds
every layer loaded and GiST-indexed, and `ST_Intersection` over those indexes
is the tool for exactly this. The results are then read back out and written
to `silver/building_lot_intersections/<date>/<neighborhood>/`, because where
the work happens and where the record lives are two different questions.
Postgres is the serving copy the query side reads; the geoparquet is what that
copy can be rebuilt from, and it is the only one of the two that survives
losing the database - which matters here more than it usually would, since
every input is a dated snapshot of a live service and no later run can
re-scrape an earlier day.

One partition, two files: `building_lots.parquet` and `lot_features.parquet`.
The cost of the merge is that they now fail together - a borough whose feature
scrape landed empty no longer gets its building join either - and that is the
honest reading anyway, since a partition holding one join and not the other
was never a state the downstream `lot_profiles`/corpus pair could use.

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

Both used to live in `lots_with_vacancy_rates`, which sat between the bronze
cadastre and this asset for one other reason: to pivot CMHC's vacancy grid onto
every lot. That grid rode through `rag.lots.attributes` and both joins below
without anything reading it, and it now reaches the question that wants it -
`lot_profiles` - as jsonb at the grain that asks it. What was left of that
asset was the repair, and it belongs next to the `ST_Intersection` calls it
exists for.

Depends on all three source assets for the *same* partition (date x
neighborhood): none of them is missing a dimension the others have, so the
partition mapping is the identity, unlike `reference_neighborhoods`'s
dependents which need `MultiToSingleDimensionPartitionMapping`.
"""

import geopandas as gpd
import shapely
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.assets import neighborhood_features
from urban_rag.bdoi_assets import BUILDINGS_FILE, neighborhood_buildings
from urban_rag.frames import count_invalid_geometries, write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    compute_intersections,
    compute_lot_features,
    fetch_building_lots,
    fetch_lot_features,
    load_buildings,
    load_features,
    load_lots,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import (
    basename,
    clear_parquet,
    filesystem,
    join,
    storage_options,
)

GROUP = "silver_joins"

#: The building x lot join, under
#: `silver/building_lot_intersections/<YYYY-MM-DD>/<neighborhood>/`.
BUILDING_LOTS_FILE = "building_lots.parquet"

#: The lot x feature join, written to the same partition directory.
LOT_FEATURES_FILE = "lot_features.parquet"

#: Infolot's lot number, and the grain this asset repairs the cadastre at
#: before either join is computed.
LOT_NUMBER_COLUMN = "NO_LOT"

#: Columns that hold the id a document cites, in precedence order. The same
#: list `rag_assets._ID_COLUMNS` reads when it writes `feature_ids` onto a
#: chunk - the two have to agree or the join they exist for matches nothing.
#: Named here rather than imported from there because the dependency runs the
#: wrong way: `rag_assets` builds the corpus, this builds the geometry it is
#: about, and neither is downstream of the other.
FEATURE_ID_COLUMNS = ("NUMERO_COMPLET", "ID")


@asset(
    key_prefix=key_prefix("building_lot_intersections"),
    partitions_def=scrape_partitions,
    deps=[neighborhood_lots, neighborhood_buildings, neighborhood_features],
    group_name=GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "The two spatial joins a lot needs, computed against one load of this "
        "borough's cadastre. Building footprints clipped to the lot(s) they "
        "intersect - one row per (building, lot) pair, in proportion to the "
        "footprint actually inside each lot rather than assigned wholesale to "
        "whichever lot the centroid falls in - and map features clipped to the "
        "lot(s) they cover, which is the hop from a lot to the PDFs the corpus "
        "holds about it. Loads this partition's neighborhood_lots (with its "
        "self-intersecting rings repaired, which is what makes ST_Intersection "
        "over them mean anything), neighborhood_buildings and "
        "neighborhood_features snapshots into "
        "rag.lots/rag.buildings/rag.features, computes rag.building_lots and "
        "rag.lot_features, then writes both to silver/"
        "building_lot_intersections/<YYYY-MM-DD>/<neighborhood>/ as "
        f"{BUILDING_LOTS_FILE} and {LOT_FEATURES_FILE}. Replaces that "
        "partition's prior rows in both places."
    ),
)
def building_lot_intersections(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

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
        raise Failure(f"{lots_path} holds no lot to intersect against.")
    if buildings.empty:
        raise Failure(f"{buildings_path} holds no building to intersect.")

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
            # Once, for both joins below - the reason they share an asset.
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
            buildings_result = compute_intersections(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
            features_result = compute_lot_features(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
            # Inside the transaction that computed them, so the files are
            # those joins and not whatever a concurrent run leaves behind
            # after the commit. Written outside it, below, so an S3 upload
            # does not hold a write transaction open for its duration.
            building_lots = fetch_building_lots(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
            lot_features = fetch_lot_features(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
    except PostgresUnavailable as exc:
        raise Failure(f"Postgres unreachable for {neighborhood} {scrape_date}: {exc}")

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    building_lots_path = write_frame(
        building_lots, join(output_dir, BUILDING_LOTS_FILE)
    )
    lot_features_path = write_frame(lot_features, join(output_dir, LOT_FEATURES_FILE))

    num_features = sum(loaded.values())
    context.log.info(
        "%s %s: %d lot(s), %d building(s), %d feature(s) across %d layer(s) -> "
        "%d building intersection(s) across %d building(s) -> %s; "
        "%d lot x feature pair(s) covering %d lot(s) -> %s",
        neighborhood,
        scrape_date,
        num_lots,
        num_buildings,
        num_features,
        len(loaded),
        buildings_result["intersections"],
        buildings_result["buildings_matched"],
        building_lots_path,
        features_result["lot_features"],
        features_result["lots_matched"],
        lot_features_path,
    )

    return MaterializeResult(
        metadata={
            # The building join is the one `lot_profiles` reads, so it stays
            # the headline row count; the lot x feature side reports its own
            # `num_lot_features` beside it.
            "dagster/row_count": buildings_result["intersections"],
            "num_lots": num_lots,
            # What bronze handed over and this asset had to fix, rather than
            # what is left: after the guard above, what is left is always zero.
            "num_geometries_repaired": repaired,
            "num_buildings": num_buildings,
            "num_intersections": buildings_result["intersections"],
            "num_buildings_matched": buildings_result["buildings_matched"],
            "num_buildings_unmatched": (
                num_buildings - buildings_result["buildings_matched"]
            ),
            "total_intersection_area_ha": round(
                buildings_result["total_area_m2"] / 10_000, 2
            ),
            "num_building_lot_rows_written": len(building_lots),
            "building_lots_path": MetadataValue.path(str(building_lots_path)),
            "num_features": num_features,
            "num_layers_loaded": len(loaded),
            "num_layers_skipped": len(skipped),
            "num_lot_features": features_result["lot_features"],
            "num_lots_matched": features_result["lots_matched"],
            # A lot covered by nothing is the symptom worth seeing: either the
            # borough's zoning layer failed to load, or the cadastre reaches
            # past the boundary the features were scraped inside.
            "num_lots_uncovered": num_lots - features_result["lots_matched"],
            "num_features_matched": features_result["features_matched"],
            "num_lot_feature_rows_written": len(lot_features),
            "lot_features_path": MetadataValue.path(str(lot_features_path)),
            "features_per_layer": MetadataValue.json(loaded),
            **({"skipped_layers": MetadataValue.json(skipped)} if skipped else {}),
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
    """One row per lot number - the grain both joins below are computed at.

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
            f"One row per {LOT_NUMBER_COLUMN} is the grain both joins here are "
            "computed at."
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
