"""How much street each lot faces, longest first.

Frontage is the measurement a highest-and-best-use question turns on after
area. Two lots of 400 m2 side by side are not the same site if one has 30 m on
a boulevard and the other 6 m on a lane: the width of the street edge decides
what can be built, how it is entered, and what it is worth. Neither publisher
records it, but between them they draw it - and the drawing is simpler than it
looks, because **in Quebec's renewed cadastre the street is a lot**. Infolot
publishes avenue Chabot as parcels 3 946 199, 3 946 200 and their neighbours,
some 13.5 m wide, exactly as it publishes the houses along it. So a lot's
frontage is the length of the boundary it shares with one of those, and the
whole measure is one intersection of two parcel boundaries - no buffer, no
tolerance, nothing to tune. See `postgis.compute_lot_frontage`.

That replaces a measure that could not be made to hold still. The street was
taken as the geobase double's *line*, the lot boundary was matched to it within
`buffer_m`, and the pieces running within 45 degrees of parallel were kept.
Every part of that was compensation for a line standing in for a polygon: a lot
line does not sit on the roadway, so the reach had to widen until it reached
the lots - at 3 m it missed 90 % of Villeray, and the default ended at 10 m -
and a reach that wide then caught the lot's own side boundaries, which the
angle test had to throw back out. A road lot has none of those problems, and
lot 3 790 556 measures 15.24 m against it, which is what the polygon says.

**The assessment roll is the obvious way to find a road lot and it does not
work here.** The roll files the public way under CUBF 45xx, but Montreal does
not enter its roadways on the roll at all: of the fourteen road lots in the
Villeray fixture, none appears in the roll's cadastre crosswalk, and the city
states 859 CUBF-45 units in 437,192. So the street network identifies the
street, which is what it is for - a geobase double side is drawn along the
roadway and so runs *inside* the parcel that is the roadway. Over the fixture
that picks out all fourteen with no false positive and no false negative. See
`postgis.DEFAULT_ROAD_LOT_MIN_STREET_M`, which is the guard on that test rather
than a threshold anything sits near.

`silver.neighborhood_streets` earns its dependency twice over, then: it says
which parcels are the roadway, and it *names* them, since a cadastral parcel
carries no street name. Naming is all it does to the numbers - a label landing
on the wrong side of a corner costs a name, never a metre.

`FrontageConfig.min_street_m` is what is left to configure, and it decides
which parcels count as road rather than what any lot then measures. The measure
itself has no setting at all, which is the point of the change.

A lot that shares no boundary with any road lot gets no row: a true interior
parcel, one reached only by a lane, or a street snapshot that stopped short. A
road lot gets no row either - a street does not front on itself - and is left
out of the coverage denominator rather than reported as landlocked. The count,
the share and a sample of the lot numbers are logged as a warning and published
as metadata rather than left to be noticed; see `num_lots_without_frontage`.

**This asset loads nothing.** Both sides of the join are already in Postgres
when it runs: `rag.lots` because `building_lot_intersections` put it there, and
`silver.neighborhood_streets` because that asset owns its own table. Each of
those is loaded in exactly one place, which is not tidiness but the fix for a
real race - see `building_lots_assets` on why loading the cadastre from two
assets means whoever commits second replaces the rows the first just computed
against. So the dependencies here are on those two assets, the guards below are
on their partitions being populated, and the only table this one writes is its
own.

**`silver.lot_frontage` is owned by hbu_infra**, like every other table this
repo writes into - sql/008_silver_lot_frontage.sql. Until it is applied to the
database, a run fails naming the file to apply, which is why this asset is
registered and given a job but left off the daily schedules. See
`urban_rag.definitions`, and `lot_profiles` for the same posture.

The `buffer_m` column that table carries is written as **0** now, and that is
both true and useful: nothing is allowed between the lot line and the street,
because the boundary has to *be* the road lot's edge. It also dates a
partition - rows saying 3.0 or 10.0 were measured the old way and are reporting
a different quantity from rows saying 0.
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
    DEFAULT_ROAD_LOT_MIN_STREET_M,
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
    """How much street line has to run inside a parcel for it to be a road.

    Config rather than a constant for the reason `lot_profiles` makes its shed
    cutoff config: it is a judgement about how far the two publishers may
    disagree before a geobase side clipping the corner of an ordinary parcel
    would be read as that parcel being a street.

    It is not the old `buffer_m` under a new name, and the difference is the
    whole point of the change. `buffer_m` decided what every lot in the borough
    *measured*, so the table moved when it moved. This decides only which
    parcels are the roadway, and the fixture separates those from everything
    else by two orders of magnitude - road lots carry 105 to 325 m of street
    line, every other parcel carries none - so no real parcel sits near it and
    the frontages do not move with it.
    """

    min_street_m: float = Field(
        default=DEFAULT_ROAD_LOT_MIN_STREET_M,
        gt=0,
        description=(
            "How much geobase double street line must run inside a parcel for "
            "that parcel to count as the roadway, in metres."
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
        "longest frontage first. The street is a cadastral lot in its own "
        "right, so this is the boundary a parcel shares with one - an exact "
        "intersection, with no buffer and no tolerance. Road lots are the "
        "parcels a silver.neighborhood_streets side runs inside, and that side "
        "also names the street; frontage_rank is 1 for the street a lot mostly "
        "fronts on, so a corner lot has two rows and an interior lot none. "
        "Computed against the rag.lots that "
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
    # the sides that identify and name the road lots are the ones
    # `neighborhood_streets` already upserted into `silver.neighborhood_streets`.
    # That asset used to write only parquet and this one loaded the table on its
    # way past, which left a table whose writer was not the asset it is named
    # for.
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
                min_street_m=config.min_street_m,
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
    num_road_lots = int(result["num_road_lots"])
    # The lots that could have had frontage. A road lot is not one of them - it
    # is the street - so it is out of the denominator rather than counted as a
    # parcel that failed to find one.
    num_candidates = num_lots - num_road_lots
    context.log.info(
        "%s %s: %d street side(s) identifying %d road lot(s) -> %d frontage(s) "
        "across %d of %d non-road lot(s), %.1f km in total, longest %.1f m "
        "-> %s",
        neighborhood,
        scrape_date,
        num_streets,
        num_road_lots,
        int(result["frontages"]),
        lots_matched,
        num_candidates,
        result["total_frontage_m"] / 1000.0,
        result["max_frontage_m"],
        path,
    )

    # Every lot in a Montreal borough that is not itself a road is expected to
    # face a street. The ones that do not are named rather than only counted: a
    # handful are genuine interior parcels or parcels reached only by a lane,
    # but a run where the share jumps is a street snapshot that stopped short -
    # and the lot numbers are what turns that from a percentage into something
    # to go and look at. A warning rather than a Failure - see
    # `test_no_lot_matching_is_not_a_failure`: a borough measuring badly is a
    # number to read, not a partition to refuse.
    unmatched = num_candidates - lots_matched
    sample = [str(number) for number in result.get("lots_without_frontage", [])]
    if unmatched > 0:
        context.log.warning(
            "%s %s: %d of %d non-road lot(s) (%.1f %%) share no boundary with "
            "any road lot and are flagged as potentially problematic%s",
            neighborhood,
            scrape_date,
            unmatched,
            num_candidates,
            100.0 * unmatched / num_candidates if num_candidates else 0.0,
            f" - e.g. {', '.join(sample)}" if sample else "",
        )

    return MaterializeResult(
        metadata={
            "dagster/row_count": int(result["frontages"]),
            "num_frontages": int(result["frontages"]),
            "num_streets": num_streets,
            "num_lots": num_lots,
            # The parcels that *are* the street, and so the gap between
            # `num_lots` and the denominator the two counts below are read
            # against. A borough where this collapses to near zero is a street
            # snapshot that did not land, not a borough without roads.
            "num_road_lots": num_road_lots,
            "num_lots_with_frontage": lots_matched,
            # The symptom worth seeing: a lot facing nothing is a true interior
            # parcel, one reached only by a lane, or a partition whose street
            # snapshot stops short of it. Under a few percent is the first two;
            # a third of the borough is the last.
            "num_lots_without_frontage": max(unmatched, 0),
            # Which ones, up to a sample's worth - the count says how bad, this
            # says where to start. Empty when every lot faced a street.
            "lots_without_frontage": MetadataValue.text(", ".join(sample)),
            "pct_lots_without_frontage": round(
                100.0 * unmatched / num_candidates, 2
            )
            if num_candidates > 0
            else 0.0,
            "num_streets_matched": int(result["streets_matched"]),
            "num_rows_pruned": int(result["pruned"]),
            "total_frontage_km": round(result["total_frontage_m"] / 1000.0, 2),
            "max_frontage_m": round(result["max_frontage_m"], 1),
            "mean_frontage_m": round(result["total_frontage_m"] / lots_matched, 1)
            if lots_matched
            else 0.0,
            # Which parcels counted as road, which is the only judgement in the
            # run - the frontages themselves are exact and have no setting. It
            # travels with the numbers rather than living only in the run's
            # config, the way `buffer_m` used to.
            "min_street_m": config.min_street_m,
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
