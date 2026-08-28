"""How much street each lot faces, longest first.

Frontage is the measurement a highest-and-best-use question turns on after
area. Two lots of 400 m2 side by side are not the same site if one has 30 m on
a boulevard and the other 6 m on a lane: the width of the street edge decides
what can be built, how it is entered, and what it is worth. Neither publisher
records it. Infolot draws the parcel and Montreal's geobase double draws the
sides of the roadway, and the relation between the two is geometry - the same
reason `building_lot_intersections` computes its joins rather than reading
them off a column.

A lot's boundary does not sit *on* the street line. The geobase double is
drawn along the roadway, published "à titre indicatif", and the lot line is
behind it by a sidewalk, a service strip and whatever the survey disagrees by -
4.85 m for the median lot in VSMPE. So the lot's boundary is chopped up, each
piece is matched to the nearest street side within `FrontageConfig.buffer_m`,
and the pieces that run *along* that side rather than at it are its frontage.
See `postgis.compute_lot_frontage` for why the measure is taken on the
boundary and not on the lot - `ST_Length` of a polygon is zero, so the direct
reading of the question would report no frontage anywhere - and for why it is
no longer a clip against a buffered street, which could not be both wide
enough to reach the lots and narrow enough not to distort what it measured.

The cutoff is `FrontageConfig.buffer_m` rather than a constant, for the same
reason `lot_profiles` makes its shed cutoff config: a judgement about the
built form rather than a property of the data, and every row records the value
it was computed with so a table can be read back against its own cutoff.

A lot that faces no street side within it gets no row. That is expected for a
true interior parcel and is a symptom otherwise, so the count, the share and a
sample of the lot numbers are logged as a warning and published as metadata
rather than left to be noticed - see `num_lots_without_frontage` below.

**This asset loads nothing.** Both sides of the join are already in Postgres
when it runs: `rag.lots` because `building_lot_intersections` put it there, and
`silver.neighborhood_streets` because that asset now owns its own table. Each
of those is loaded in exactly one place, which is not tidiness but the fix for
a real race - see `building_lots_assets` on why loading the cadastre from two
assets means whoever commits second replaces the rows the first just computed
against. So the dependencies here are on those two assets, the guards below are
on their partitions being populated, and the only table this one writes is its
own.

**`silver.lot_frontage` is owned by hbu_infra**, like every other table this
repo writes into - sql/008_silver_lot_frontage.sql. Until it is applied to the
database, a run fails naming the file to apply, which is why this asset is
registered and given a job but left off the daily schedules. See
`urban_rag.definitions`, and `lot_profiles` for the same posture.
"""

import geopandas as gpd
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pydantic import Field

from urban_rag.building_lots_assets import building_lot_intersections
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    DEFAULT_FRONTAGE_BUFFER_M,
    MissingRelation,
    compute_lot_frontage,
    fetch_lot_frontage,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, join, storage_options
from urban_rag.street_assets import STREETS_FILE_OUT, neighborhood_streets

GROUP = "silver_streets"

#: The one file a partition is written to, under
#: `silver/lot_frontage/<YYYY-MM-DD>/<neighborhood>/`.
LOT_FRONTAGE_FILE = "lot_frontage.parquet"


class FrontageConfig(Config):
    """How far behind the curb line a lot boundary may sit and still face it.

    Config rather than a constant because it is a judgement about the street
    section, not a property of the data: a borough of deep service strips and
    grass verges holds its lot lines further back than one of them.

    Widening it used to cost accuracy at the corners, which is why it defaulted
    to a value too small to reach most lots at all. It no longer does -
    `postgis.compute_lot_frontage` measures only the boundary running *along* a
    street side, so the measure is flat in this - but the value that produced a
    table still travels on every row of it, because which lots got a row at all
    depends on it.
    """

    buffer_m: float = Field(
        default=DEFAULT_FRONTAGE_BUFFER_M,
        gt=0,
        description=(
            "Distance from a street side within which a lot boundary counts as "
            "facing it, in metres."
        ),
    )


@asset(
    key_prefix=key_prefix("lot_frontage"),
    partitions_def=scrape_partitions,
    deps=[building_lot_intersections, neighborhood_streets],
    group_name=GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "How much of each lot's boundary faces each street side, in metres, "
        "longest frontage first. Buffers each side of "
        "silver.neighborhood_streets, clips every lot boundary within reach of "
        "it, and measures what is left; frontage_rank is 1 for the street a "
        "lot mostly fronts on, so a corner lot has two rows and an interior "
        "lot none. Computed against the rag.lots that "
        "building_lot_intersections landed for the same partition, upserted "
        "into silver.lot_frontage on (scrape_date, neighborhood, lot_uid, "
        f"cote_rue_id) and written to silver/lot_frontage/<YYYY-MM-DD>/"
        f"<neighborhood>/{LOT_FRONTAGE_FILE}."
    ),
)
def lot_frontage(
    context: AssetExecutionContext,
    config: FrontageConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    streets_path = join(
        store.partition_dir(
            neighborhood_streets.key.path[-1], scrape_date, neighborhood
        ),
        STREETS_FILE_OUT,
    )
    # Read only to check the partition is there and to report the denominator:
    # the street sides this measures against are the ones `neighborhood_streets`
    # already upserted into `silver.neighborhood_streets`. That asset used to
    # write only parquet and this one loaded the table on its way past, which
    # left a table whose writer was not the asset it is named for.
    streets = _read_streets(
        streets_path, neighborhood=neighborhood, scrape_date=scrape_date
    )
    if streets.empty:
        raise Failure(f"{streets_path} holds no street side to measure against.")

    try:
        with postgis.connect() as connection:
            result = compute_lot_frontage(
                connection,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                buffer_m=config.buffer_m,
            )
            num_streets = int(result["num_streets"])
            if num_streets == 0:
                raise Failure(
                    f"silver.neighborhood_streets holds no street side for "
                    f"{neighborhood} {scrape_date}, though {streets_path} has "
                    f"{len(streets)} - re-materialize neighborhood_streets for "
                    "this partition."
                )
            num_lots = int(result["num_lots"])
            if num_lots == 0:
                # Not "no lot faces a street": no lots at all, which means the
                # cadastre was never loaded rather than that the borough is
                # landlocked. Raised inside the transaction so the frontage it
                # just computed rolls back with it rather than sitting in
                # silver.lot_frontage against a cadastre that is not there.
                raise Failure(
                    f"rag.lots holds no lot for {neighborhood} {scrape_date} - "
                    "materialize building_lot_intersections for this partition "
                    "first."
                )
            # Inside the transaction that computed it, so the file is that
            # answer rather than whatever a concurrent run leaves behind after
            # the commit. Written outside it, below, so an S3 upload does not
            # hold a write transaction open for its duration.
            frame = fetch_lot_frontage(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
    except PostgresUnavailable as exc:
        raise Failure(f"Postgres unreachable for {neighborhood} {scrape_date}: {exc}")
    except MissingRelation as exc:
        raise Failure(str(exc))

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_FRONTAGE_FILE))

    lots_matched = int(result["lots_matched"])
    context.log.info(
        "%s %s: %d street side(s) within %.1f m -> %d frontage(s) across "
        "%d of %d lot(s), %.1f km in total, longest %.1f m -> %s",
        neighborhood,
        scrape_date,
        num_streets,
        config.buffer_m,
        int(result["frontages"]),
        lots_matched,
        num_lots,
        result["total_frontage_m"] / 1000.0,
        result["max_frontage_m"],
        path,
    )

    # Every lot in a Montreal borough is expected to face a street. The ones
    # that do not are named rather than only counted: a handful are genuine
    # interior parcels, but a run where the share jumps is a street snapshot
    # that stopped short or a cutoff that is too tight, and the lot numbers
    # are what turns that from a percentage into something to go and look at.
    # A warning rather than a Failure - see `test_no_lot_matching_is_not_a
    # _failure`: a borough measuring badly is a number to read, not a
    # partition to refuse.
    unmatched = num_lots - lots_matched
    sample = [str(number) for number in result.get("lots_without_frontage", [])]
    if unmatched:
        context.log.warning(
            "%s %s: %d of %d lot(s) (%.1f %%) face no street side within "
            "%.1f m and are flagged as potentially problematic%s",
            neighborhood,
            scrape_date,
            unmatched,
            num_lots,
            100.0 * unmatched / num_lots,
            config.buffer_m,
            f" - e.g. {', '.join(sample)}" if sample else "",
        )

    return MaterializeResult(
        metadata={
            "dagster/row_count": int(result["frontages"]),
            "num_frontages": int(result["frontages"]),
            "num_streets": num_streets,
            "num_lots": num_lots,
            "num_lots_with_frontage": lots_matched,
            # The symptom worth seeing: a lot facing nothing is either a true
            # interior parcel or a partition whose street snapshot stops short
            # of it. Under a few percent is the first; a third of the borough
            # is the second.
            "num_lots_without_frontage": unmatched,
            # Which ones, up to a sample's worth - the count says how bad, this
            # says where to start. Empty when every lot faced a street.
            "lots_without_frontage": MetadataValue.text(", ".join(sample)),
            "pct_lots_without_frontage": round(
                100.0 * unmatched / num_lots, 2
            )
            if num_lots
            else 0.0,
            "num_streets_matched": int(result["streets_matched"]),
            "num_rows_pruned": int(result["pruned"]),
            "total_frontage_km": round(result["total_frontage_m"] / 1000.0, 2),
            "max_frontage_m": round(result["max_frontage_m"], 1),
            "mean_frontage_m": round(result["total_frontage_m"] / lots_matched, 1)
            if lots_matched
            else 0.0,
            # What the numbers above mean depends entirely on this, so it
            # travels with them rather than only in the run's config.
            "buffer_m": config.buffer_m,
            "output_path": MetadataValue.path(str(path)),
        }
    )


def _read_streets(
    path: str, *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    try:
        return gpd.read_parquet(path, storage_options=storage_options(path))
    except FileNotFoundError as exc:
        raise Failure(
            f"{path} does not exist - materialize neighborhood_streets for "
            f"{neighborhood} {scrape_date} first."
        ) from exc
