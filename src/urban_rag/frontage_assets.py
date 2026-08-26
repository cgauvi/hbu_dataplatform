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
drawn along the curb and sidewalk limits, published "à titre indicatif", and
the lot line is behind those by a sidewalk's width and whatever the survey
disagrees by. So the street side is buffered by a few metres and the lot's
boundary is clipped to that buffer; what is left is the part of the parcel's
edge facing that street, and its length is the frontage. See
`postgis.compute_lot_frontage` for why the measure is taken on `ST_Boundary`
and not on the lot - `ST_Length` of a polygon is zero, so the direct reading of
the question would report no frontage anywhere.

The buffer is `FrontageConfig.buffer_m` rather than a constant, for the same
reason `lot_profiles` makes its shed cutoff config: a judgement about the
built form rather than a property of the data, and every row records the value
it was computed with so a table can be read back against its own cutoff.

**This asset loads `rag.streets` and nothing else.** `rag.lots` is already
holding this partition when it runs, because `building_lot_intersections` put
it there - and that asset's docstring explains why loading the cadastre from
two places is a race rather than a redundancy: whoever commits second replaces
the rows the first just computed against. So the dependency here is on that
asset, the guard below is on `rag.lots` being populated, and the only table
this one replaces is its own.

**`rag.streets` and `rag.lot_frontage` are owned by hbu_infra**, like every
other table this repo writes into - sql/007_streets.sql and
sql/008_lot_frontage.sql. Until those are applied to the database, a run fails
on `relation "rag.streets" does not exist`, which is why this asset is
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
from urban_rag.open_data_assets import STREET_ID_COLUMN, STREET_NAME_COLUMN
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    DEFAULT_FRONTAGE_BUFFER_M,
    compute_lot_frontage,
    fetch_lot_frontage,
    load_streets,
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
    section, not a property of the data: three metres crosses a sidewalk, and
    a borough with grass verges and deep service strips may want five. Widening
    it costs accuracy at the corners - the first `buffer_m` of each *side*
    boundary falls inside the buffer too and is counted as frontage - so the
    value that produced a table travels on every row of it.
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
        "longest frontage first. Loads this partition's neighborhood_streets "
        "into rag.streets, buffers each side, clips every lot boundary within "
        "reach of it, and measures what is left; frontage_rank is 1 for the "
        "street a lot mostly fronts on, so a corner lot has two rows and an "
        "interior lot none. Computed against the rag.lots that "
        "building_lot_intersections landed for the same partition, written to "
        f"rag.lot_frontage and to silver/lot_frontage/<YYYY-MM-DD>/"
        f"<neighborhood>/{LOT_FRONTAGE_FILE}. Replaces that partition's prior "
        "rows in both places."
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
    streets = _read_streets(
        streets_path, neighborhood=neighborhood, scrape_date=scrape_date
    )
    if streets.empty:
        raise Failure(f"{streets_path} holds no street side to measure against.")

    try:
        with postgis.connect() as connection:
            num_streets = load_streets(
                connection,
                streets,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                street_id_column=STREET_ID_COLUMN,
                street_name_column=STREET_NAME_COLUMN,
            )
            result = compute_lot_frontage(
                connection,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                buffer_m=config.buffer_m,
            )
            num_lots = int(result["num_lots"])
            if num_lots == 0:
                # Not "no lot faces a street": no lots at all, which means the
                # cadastre was never loaded rather than that the borough is
                # landlocked. Raised inside the transaction so the streets it
                # just landed roll back with it rather than sitting in
                # rag.streets with nothing to join against.
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

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_FRONTAGE_FILE))

    lots_matched = int(result["lots_matched"])
    context.log.info(
        "%s %s: %d street side(s) buffered by %.1f m -> %d frontage(s) across "
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
            "num_lots_without_frontage": num_lots - lots_matched,
            "num_streets_matched": int(result["streets_matched"]),
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
