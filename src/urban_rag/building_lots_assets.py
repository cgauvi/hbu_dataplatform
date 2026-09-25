"""What sits on a lot and what covers it: two PostGIS joins, one tile, one
asset.

Both joins hang off the same left-hand side - the lots this cell of the cut
owns, as `neighborhood_cadastre` landed them in `rag.lots` - and two different
questions are asked of it that no layer answers on its own:

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

**The owned set is the tile's; the pool is the snapshot's.** Each join takes
the lots whose `cell_partition` is this tile and runs them against every
building and every feature loaded for the date, with no cell and no borough
in the predicate. A building straddling a cell edge is clipped to the lots on
each side by each side's run - the pair it makes with a lot is that lot's row,
whichever tile computed it - and the "10% of the whole building" screen in
`postgis.compute_intersections` is measured against the whole building, not
against the part of it under this cell. Nothing spatial is cut by the cell;
the cell is a write-ownership claim, and the artefacts the borough line used
to leave in these joins - a footprint clipped where the borough ended, a zone
that stopped at the namespace - are gone with the line.

The load is not here any more. `neighborhood_cadastre` lands a borough's
three snapshots in `rag.*` on the borough axis, because a borough is what a
publisher answers for; this asset computes on the tile axis, because a tile
is what a computation is run over, and the bridge between the two is the
partition mapping below. What stays together is the pair of joins: they are
computed and read back inside one transaction, so a reader never sees one
join built against a cadastre a concurrent reload was half way through
replacing.

The joins are *computed* in Postgres: by the time this runs PostGIS holds
every layer loaded and GiST-indexed, and `ST_Intersection` over those indexes
is the tool for exactly this. The results are then read back out and written
to `silver/building_lot_intersections/<date>/<tile>/`, because where the
work happens and where the record lives are two different questions.
Postgres is the serving copy the query side reads; the geoparquet is what that
copy can be rebuilt from, and it is the only one of the two that survives
losing the database - which matters here more than it usually would, since
every input is a dated snapshot of a live service and no later run can
re-scrape an earlier day.

One partition, two files: `building_lots.parquet` and `lot_features.parquet`.
They fail together - a tile whose feature join comes up empty gets no building
join either - and that is the honest reading anyway, since a partition holding
one join and not the other was never a state the downstream `lot_profiles`/
corpus pair could use.

Depends on `neighborhood_cadastre` through the bridge: the date is the
identity, and the borough dimension is left unmapped, which Dagster reads as
*every* borough of that date. That is the true dependency - a cell can hold
lots from two boroughs, and which ones is a fact in `rag.lots`, not in the
partition key.
"""

from dagster import (
    AssetDep,
    AssetExecutionContext,
    DimensionPartitionMapping,
    Failure,
    IdentityPartitionMapping,
    MaterializeResult,
    MetadataValue,
    MultiPartitionMapping,
    asset,
)

from urban_rag.cadastre_assets import neighborhood_cadastre
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.partitions import tile_partition_of, tile_scrape_partitions
from urban_rag.postgis import (
    MissingRelation,
    compute_intersections,
    compute_lot_features,
    fetch_building_lots,
    fetch_lot_features,
    require_working_set,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, join

GROUP = "silver_joins"

#: The building x lot join, under
#: `silver/building_lot_intersections/<YYYY-MM-DD>/<tile>/`.
BUILDING_LOTS_FILE = "building_lots.parquet"

#: The lot x feature join, written to the same partition directory.
LOT_FEATURES_FILE = "lot_features.parquet"


@asset(
    key_prefix=key_prefix("building_lot_intersections"),
    partitions_def=tile_scrape_partitions,
    deps=[
        AssetDep(
            neighborhood_cadastre,
            partition_mapping=MultiPartitionMapping(
                {"date": DimensionPartitionMapping("date", IdentityPartitionMapping())}
            ),
        )
    ],
    group_name=GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "The two spatial joins a lot needs, computed for the lots this cell "
        "of the cut owns against every building and every feature in the "
        "day's snapshot. Building footprints clipped to the lot(s) they "
        "intersect - one row per (building, lot) pair, in proportion to the "
        "footprint actually inside each lot rather than assigned wholesale to "
        "whichever lot the centroid falls in; a building straddling a cell "
        "edge is clipped to the lots on each side by each side's run, and the "
        "'10% of the whole building' screen is measured against the whole "
        "building - and map features clipped to the lot(s) they cover, which "
        "is the hop from a lot to the PDFs the corpus holds about it. Reads "
        "rag.lots/rag.buildings/rag.features as neighborhood_cadastre landed "
        "them, upserts silver.building_lot_intersections and "
        "silver.lot_features for this tile, then writes both to silver/"
        "building_lot_intersections/<YYYY-MM-DD>/<tile>/ as "
        f"{BUILDING_LOTS_FILE} and {LOT_FEATURES_FILE}."
    ),
)
def building_lot_intersections(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    tile, scrape_date = tile_partition_of(context)

    try:
        with postgis.connect() as connection:
            # Before the first join, not on the way into it. The three tables
            # come from one hbu_infra file, so a database that has never had
            # it applied should say so once and name the file - rather than
            # failing inside a join with an identifier and no owner.
            require_working_set(connection)
            buildings_result = compute_intersections(
                connection, tile=tile, scrape_date=scrape_date
            )
            features_result = compute_lot_features(
                connection, tile=tile, scrape_date=scrape_date
            )
            # Inside the transaction that computed them, so the files are
            # those joins and not whatever a concurrent run leaves behind
            # after the commit. Written outside it, below, so an S3 upload
            # does not hold a write transaction open for its duration.
            building_lots = fetch_building_lots(
                connection, tile=tile, scrape_date=scrape_date
            )
            lot_features = fetch_lot_features(
                connection, tile=tile, scrape_date=scrape_date
            )
    except PostgresUnavailable as exc:
        raise Failure(f"Postgres unreachable for {tile} {scrape_date}: {exc}")
    except MissingRelation as exc:
        raise Failure(str(exc))

    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, tile)
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    building_lots_path = write_frame(
        building_lots, join(output_dir, BUILDING_LOTS_FILE)
    )
    lot_features_path = write_frame(lot_features, join(output_dir, LOT_FEATURES_FILE))

    num_lots = int(features_result["num_lots"])
    context.log.info(
        "%s %s: %d lot(s) -> %d building intersection(s) across %d building(s) "
        "-> %s; %d lot x feature pair(s) covering %d lot(s) across %d layer(s) "
        "-> %s",
        tile,
        scrape_date,
        num_lots,
        buildings_result["intersections"],
        buildings_result["buildings_matched"],
        building_lots_path,
        features_result["lot_features"],
        features_result["lots_matched"],
        features_result["layers"],
        lot_features_path,
    )

    return MaterializeResult(
        metadata={
            # The building join is the one `lot_profiles` reads, so it stays
            # the headline row count; the lot x feature side reports its own
            # `num_lot_features` beside it.
            "dagster/row_count": buildings_result["intersections"],
            "tile": tile,
            # The tile's own lots, counted in `rag.lots` by the feature join:
            # the denominator every coverage figure below is read against.
            "num_lots": num_lots,
            "num_intersections": buildings_result["intersections"],
            "num_buildings_matched": buildings_result["buildings_matched"],
            "total_intersection_area_ha": round(
                buildings_result["total_area_m2"] / 10_000, 2
            ),
            "num_building_lot_rows_written": len(building_lots),
            "building_lots_path": MetadataValue.path(str(building_lots_path)),
            "num_layers": features_result["layers"],
            "num_lot_features": features_result["lot_features"],
            "num_lots_matched": features_result["lots_matched"],
            # A lot covered by nothing is the symptom worth seeing: either its
            # borough's zoning layer failed to load, or the cadastre reaches
            # past the boundary the features were scraped inside.
            "num_lots_uncovered": num_lots - features_result["lots_matched"],
            "num_features_matched": features_result["features_matched"],
            "num_lot_feature_rows_written": len(lot_features),
            "lot_features_path": MetadataValue.path(str(lot_features_path)),
            # What the upsert dropped as superseded. Normally the whole prior
            # partition, since `building_uid` is a bigserial `load_buildings`
            # mints again - see `urban_rag.warehouse`. A partition being
            # computed for the first time prunes nothing.
            "num_building_lot_rows_pruned": buildings_result["pruned"],
            "num_lot_feature_rows_pruned": features_result["pruned"],
        }
    )
