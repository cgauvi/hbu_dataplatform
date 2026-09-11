"""The piece of a lot that one zone governs, as a site in its own right.

A zoning boundary does not have to follow a lot line, and on a large parcel it
usually does not. This platform used to answer as though it did.

`lot_zoning_envelopes` has always written a row per (lot, zone, column) - the
zones were never *lost* - but every row carried the whole lot's area and the
whole lot's frontage, and `hbu.governing_zone` - now `hbu.primary_zone`, and a
label rather than a filter - then kept the best-covered zone and discarded the
others before anything was solved. So a split parcel was answered once, under
one grid, over ground that grid does not govern.

Lot 1 740 794 in Villeray-Saint-Michel-Parc-Extension is the case to picture.
27 044 m2: 24 596 of them in H04-072, which permits H.7 to eight storeys, and
2 440 in C04-083, which permits C.4 and H to six. Those are two sites, and the
old answer was wrong about both of them at once - the eight storeys were priced
over the whole parcel including the 2 440 m2 H04-072 does not reach, and the
C.4 rights were not priced at all. Over the borough that is 1 861 lots carrying
more than one zone, 628 of them with two or more pieces above 500 m2, and 121
ha of land priced under a grid that does not govern it.

`lot_zone_pieces` is the grain that fixes it: one row per (lot, zone), carrying
the piece's own area, its own street, and its own share of what already stands
on the parcel. Everything downstream of the zoning reads it - the envelope, the
setbacks, the CP-SAT solve, the redevelopment gap, the IRR, the map - and
`lot_number` is what groups the pieces back into a parcel for a reader.

**The frontage is the piece's, and that is most of what changes.**
`lot_frontage` measures the street a *lot* faces; a piece faces only the part
of that edge lying on its own boundary. On 1 740 794 the lot's rank-1 frontage
is 19.8 m of Jarry and it belongs entirely to the commercial piece - the
residential remainder behind it has 15.2 m of D'Hérelle and no Jarry at all. A
commercial strip on a boulevard with housing behind it is the ordinary form of
a Montreal arterial, and until now both pieces read the boulevard.

**Two allocators, because the roll is per lot and the answer is per piece.**
`footprint_share` divides everything the standing *building* is or earns, and
it is measured rather than assumed - `building_lot_intersections`' lot-clipped
footprints, clipped again to the piece. `area_share` divides what belongs to
the ground. Pro-rating a building by area would be simpler and wrong in the
ordinary case: a corner commercial strip is a tenth of the parcel and carries
the whole of the retail block standing on it. See
`postgis.compute_lot_zone_pieces`, where both are argued, and `hbu.use_gap`,
which is what spends them.

**This asset loads nothing.** All three inputs are already in Postgres when it
runs: `silver.lot_features` and `silver.building_lot_intersections` because
`building_lot_intersections` put them there, `silver.lot_frontage` because
`lot_frontage` owns its own table. The same posture `lot_frontage` and
`lot_buildable_setbacks` take, and for the same reason - two assets loading one
table from one file in two transactions is the race `building_lots_assets`
describes.

**`silver.lot_zone_pieces` is owned by hbu_infra**, like every other table this
repo writes into - sql/025_silver_lot_zone_pieces.sql. Until it is applied a
run fails naming the file, which is why this asset is registered and given a
job but left off the daily schedules. See `urban_rag.definitions`, and
`lot_frontage` and `lot_buildable_setbacks` for the same posture.
"""

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
from urban_rag.frontage_assets import lot_frontage
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    DEFAULT_ZONE_PIECE_EDGE_TOLERANCE_M,
    MIN_ZONE_OVERLAP_M2,
    MIN_ZONE_PCT_OF_LOT,
    MIN_ZONE_PIECE_AREA_M2,
    MissingRelation,
    compute_lot_zone_pieces,
    fetch_lot_zone_pieces,
)
from urban_rag.rag.documents import DOCUMENT_SOURCES
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, join

GROUP = "silver_zoning"

#: One file per partition, under
#: `silver/lot_zone_pieces/<YYYY-MM-DD>/<neighborhood>/`.
LOT_ZONE_PIECES_FILE = "lot_zone_pieces.parquet"


class ZonePieceConfig(Config):
    """How much of a lot a zone has to cover to be one of its pieces.

    The first two used to be `envelope_assets.EnvelopeConfig`'s and are argued
    in full at `postgis.MIN_ZONE_PCT_OF_LOT` and `postgis.MIN_ZONE_OVERLAP_M2`:
    a parcel on a zone boundary picks up a sliver of the neighbouring zone
    because two publishers drew two lines, and the sliver is small absolutely
    *and* small proportionally, so it takes two measures to catch. They moved
    here with the join, because this asset is now the first place they are
    applied - the envelopes read these pieces rather than the raw overlaps -
    and a threshold that decides which sites exist belongs on the asset that
    decides it.

    **The third is new, and it is an `or` where those two are an `and`.** That
    asymmetry is the whole of it. A percentage is the right measure for the
    ordinary parcel and turns around on a very large one: one per cent of Parc
    Jarry is 15 900 m2, an entire city block, and a zone governing it would be
    dropped as a survey artefact. So a clip that is large in its own right is
    kept whatever share of its parcel it is.

    Over Villeray-Saint-Michel-Parc-Extension that clause is nearly inert - it
    recovers 3 pieces totalling 0.2 ha, none above 1 000 m2 - and that is the
    point rather than a disappointment: it is insurance against a borough of
    bigger parcels, priced at almost nothing in one without them. See
    `postgis.MIN_ZONE_PIECE_AREA_M2`.
    """

    min_pct_of_lot: float = Field(
        default=MIN_ZONE_PCT_OF_LOT,
        ge=0.0,
        le=100.0,
        description=(
            "A zone gives a lot a piece when it covers at least this "
            "percentage of it. Guards against the sliver a cadastral boundary "
            "and a zoning boundary produce where they disagree, in the "
            "measure min_overlap_m2 cannot see."
        ),
    )
    min_overlap_m2: float = Field(
        default=MIN_ZONE_OVERLAP_M2,
        ge=0.0,
        description=(
            "A zone gives a lot a piece when it covers at least this many "
            "square metres of it. The absolute half of the same cutoff."
        ),
    )
    min_piece_area_m2: float = Field(
        default=MIN_ZONE_PIECE_AREA_M2,
        ge=0.0,
        description=(
            "A clip at least this large is a piece whatever share of its "
            "parcel it is - the one clause that is an or rather than an and, "
            "so a percentage cannot discard a city block off a very large lot."
        ),
    )
    edge_tolerance_m: float = Field(
        default=DEFAULT_ZONE_PIECE_EDGE_TOLERANCE_M,
        ge=0.0,
        description=(
            "How far off a piece's boundary a lot_frontage linestring may sit "
            "and still count as that piece's street edge, in metres. Larger "
            "than the setbacks' tolerance because the boundary here is a "
            "computed clip rather than the line the frontage was cut from."
        ),
    )


@asset(
    key_prefix=key_prefix("lot_zone_pieces"),
    partitions_def=scrape_partitions,
    deps=[building_lot_intersections, lot_frontage],
    group_name=GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "The piece of each lot that one zone governs, as a site in its own "
        "right: one row per (lot, zone), carrying the clipped polygon, its "
        "area, the street *that piece* faces - lot_frontage's edges cut to it "
        "and re-ranked within it, which is how a commercial strip on a "
        "boulevard stops sharing the boulevard with the housing behind it - "
        "and its share of what already stands on the parcel. Two allocators: "
        "footprint_share divides the roll's building (floor area, dwellings, "
        "income, NOI) by the footprint measured on this piece, area_share "
        "divides the ground. A zone clipping under min_pct_of_lot (1%) and "
        "min_overlap_m2 (1 m2) of a lot gets no piece unless it clears "
        "min_piece_area_m2 (500 m2) outright. is_primary_zone marks the "
        "largest piece, which is the row a reader wanting one answer per lot "
        "takes. Computed against the silver.lot_features, silver.lot_frontage "
        "and silver.building_lot_intersections rows already in Postgres for "
        "this partition, upserted into silver.lot_zone_pieces on "
        "(scrape_date, neighborhood, lot_uid, feature_id) and written to "
        f"silver/lot_zone_pieces/<YYYY-MM-DD>/<neighborhood>/"
        f"{LOT_ZONE_PIECES_FILE}."
    ),
)
def lot_zone_pieces(
    context: AssetExecutionContext,
    config: ZonePieceConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)

    try:
        with postgis.connect() as connection:
            result = compute_lot_zone_pieces(
                connection,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                zone_sources=tuple(DOCUMENT_SOURCES),
                min_pct_of_lot=config.min_pct_of_lot,
                min_overlap_m2=config.min_overlap_m2,
                min_piece_area_m2=config.min_piece_area_m2,
                edge_tolerance_m=config.edge_tolerance_m,
            )
            if int(result["num_pieces"]) == 0:
                # Raised inside the transaction so the (empty) answer rolls
                # back with it rather than replacing a good partition with
                # nothing. Two causes and they have different fixes, so both
                # are named: no lot x zone rows at all, or every one of them
                # under the cutoffs.
                raise Failure(
                    f"{neighborhood} {scrape_date}: no lot is covered by a "
                    f"{'/'.join(DOCUMENT_SOURCES)} feature at or above "
                    f"{config.min_pct_of_lot}% and {config.min_overlap_m2} m2, "
                    f"or {config.min_piece_area_m2} m2 outright, so there is "
                    "no piece to state. Check that "
                    "building_lot_intersections loaded the zoning layer for "
                    "this partition."
                )
            # Inside the transaction that computed it, so the file is that
            # answer rather than whatever a concurrent run leaves after the
            # commit - the posture `lot_frontage` takes.
            frame = fetch_lot_zone_pieces(
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
    path = write_frame(frame, join(output_dir, LOT_ZONE_PIECES_FILE))

    num_pieces = int(result["num_pieces"])
    num_lots = int(result["num_lots"])
    num_split = int(result["num_split_lots"])
    context.log.info(
        "%s %s: %d piece(s) over %d lot(s), %d of them split between two or "
        "more zones; %d secondary piece(s) of %g m2 or more holding %.1f ha "
        "that the primary zone's grid used to answer for -> %s",
        neighborhood,
        scrape_date,
        num_pieces,
        num_lots,
        num_split,
        int(result["num_large_secondary_pieces"]),
        config.min_piece_area_m2,
        float(result["secondary_piece_area_ha"]),
        path,
    )
    without_frontage = int(result["num_pieces_without_frontage"])
    if without_frontage:
        # Said out loud rather than left in metadata: a piece with no street of
        # its own qualifies for no column its grid prints a *Largeur du terrain
        # min* for, so this is the count that explains a borough of
        # `no_governing_column` rows downstream. It is not a new gap - see the
        # module docstring - but it is the one worth being able to size.
        context.log.info(
            "%s %s: %d of %d piece(s) share no street edge and read 0 m of "
            "frontage - interior remnants, and pieces of lots lot_frontage "
            "could not measure at all",
            neighborhood,
            scrape_date,
            without_frontage,
            num_pieces,
        )
    by_area = int(result["num_pieces_by_area_share"])
    if by_area:
        context.log.warning(
            "%s %s: %d of %d piece(s) carry no measured footprint, so their "
            "share of the roll falls back to area rather than to where the "
            "building stands - check that building_lot_intersections landed "
            "this partition's buildings",
            neighborhood,
            scrape_date,
            by_area,
            num_pieces,
        )

    return MaterializeResult(
        metadata={
            "dagster/row_count": num_pieces,
            "num_pieces": num_pieces,
            "num_lots": num_lots,
            # The two numbers that say whether this grain is doing anything on
            # this borough, and the pair to read together: how many parcels are
            # really split, and how much land the pieces that used to be
            # discarded actually hold.
            "num_split_lots": num_split,
            "num_pieces_on_split_lots": int(result["num_pieces_on_split_lots"]),
            "num_large_secondary_pieces": int(
                result["num_large_secondary_pieces"]
            ),
            "secondary_piece_area_ha": float(result["secondary_piece_area_ha"]),
            "pct_lots_split": round(
                100.0 * num_split / num_lots if num_lots else 0.0, 2
            ),
            "num_pieces_without_frontage": without_frontage,
            "num_vacant_pieces": int(result["num_vacant_pieces"]),
            "num_pieces_by_area_share": by_area,
            # What the row counts mean depends entirely on these, so they
            # travel with them rather than only in the run's config.
            "min_pct_of_lot": config.min_pct_of_lot,
            "min_overlap_m2": config.min_overlap_m2,
            "min_piece_area_m2": config.min_piece_area_m2,
            "edge_tolerance_m": config.edge_tolerance_m,
            "output_path": MetadataValue.path(str(path)),
            "rows_upserted": int(result.get("upserted", 0)),
            "rows_deleted": int(result.get("deleted", 0)),
        }
    )


def _partition(context: AssetExecutionContext) -> tuple[str, str]:
    dimensions = context.partition_key.keys_by_dimension
    return dimensions["neighborhood"], dimensions["date"][:10]
