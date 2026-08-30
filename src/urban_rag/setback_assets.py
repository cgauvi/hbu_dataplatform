"""What is left of a lot once its zone's margins are taken off it.

The zoning grid states four margins - *Avant principale*, *Avant secondaire*,
*Latérale*, *Arrière* - and `lot_zoning_envelopes` has carried all four since
they were first parsed, with nothing subtracting them. `urban_rag.program` caps
a footprint on *Taux d'implantation au sol* alone, so a deep mid-block lot and
a shallow one of the same area have been solving identically. They are not the
same site, and this asset is the difference.

**Two caps, not one refinement of a cap.** *Taux d'implantation* says what
share of a parcel may be covered; the margins say *where* on it. Neither
implies the other, and a building satisfies both - so `footprint_cap_m2` is the
lesser of the two, and `footprint_cap_binding` names which produced it. A
borough whose rows mostly read `setbacks` is one shaped by its margins rather
than by its coverage, which is a fact about the by-law nobody can read off the
grid.

**The subtraction is directional.** `ST_Buffer(lot, -d)` takes the same d off
every edge and the margins are four distances at four edges, so the boundary is
sorted first: the street edges are what `lot_frontage` already measured, and
what is left is split into rear and side by the angle it runs at relative to
the front. That test is `compute_lot_frontage`'s own parallel test pointed at a
different reference line, which is why the two share a constant rather than
each carrying a threshold. See `postgis.compute_lot_buildable_setbacks`.

**The mode is what moves the answer, not the margin.** *Mode d'implantation*
decides whether the side margin applies at all - a contiguous building is built
to the party line and has none - and VSMPE's grids print `I-J` and `I-J-C`,
where the `C` is exactly that permission. Subtracting the printed *Latérale*
from both sides of every lot in a borough of plexes would understate most of
the stock. `side_setback_rule` records which reading each row was computed
under, and the asset reports the borough's split across the three so a mode
column that failed to parse shows up as a number rather than as a quietly
smaller envelope.

**This asset loads nothing.** All three inputs are already in Postgres when it
runs: `rag.lots` because `building_lot_intersections` put it there,
`silver.lot_frontage` and `silver.lot_zoning_envelopes` because those assets own
their own tables. So the dependencies here are on those assets, and the only
table this one writes is its own - the same posture `lot_frontage` takes, and
for the same reason: two assets loading one table from one file in two
transactions is the race `building_lots_assets` describes.

**`silver.lot_buildable_setbacks` is owned by hbu_infra**, like every other
table this repo writes into - sql/015_silver_lot_buildable_setbacks.sql. Until
it is applied a run fails naming the file, which is why this asset is
registered and given a job but left off the daily schedules. See
`urban_rag.definitions`, and `lot_frontage` and `lot_profiles` for the same
posture.
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

from urban_rag.envelope_assets import lot_zoning_envelopes
from urban_rag.frames import write_frame
from urban_rag.frontage_assets import lot_frontage
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    DEFAULT_SETBACK_EDGE_TOLERANCE_M,
    MissingRelation,
    compute_lot_buildable_setbacks,
    fetch_lot_buildable_setbacks,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, join

GROUP = "silver_zoning"

#: The one file a partition is written to, under
#: `silver/lot_buildable_setbacks/<YYYY-MM-DD>/<neighborhood>/`.
LOT_SETBACKS_FILE = "lot_buildable_setbacks.parquet"


class SetbackConfig(Config):
    """How far off the boundary a measured street edge still counts as on it.

    Config rather than a constant for consistency with `FrontageConfig`, but it
    is a much smaller judgement than that one: the frontage geometry was cut
    from this very boundary one asset earlier, so the two are the same line and
    this only absorbs the round trip through the EPSG:4326 the table stores it
    in. Widening it costs a little of each side edge at the corners; the
    default is a twentieth of the metre the boundary is chopped at, so what it
    costs is well under the resolution of the sort it feeds.

    The value that produced a table travels on every row of it, the same rule
    `lot_frontage.buffer_m` follows.
    """

    edge_tolerance_m: float = Field(
        default=DEFAULT_SETBACK_EDGE_TOLERANCE_M,
        gt=0,
        description=(
            "Distance within which a measured frontage linestring counts as "
            "lying on the lot boundary, in metres."
        ),
    )


@asset(
    key_prefix=key_prefix("lot_buildable_setbacks"),
    partitions_def=scrape_partitions,
    deps=[lot_frontage, lot_zoning_envelopes],
    group_name=GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "What is left of each lot once its zone's margins are subtracted, one "
        "row per (lot, zone, grid column). Sorts the lot boundary into front, "
        "secondary front, side and rear - the street edges from "
        "lot_frontage, the rest split by the angle it runs at relative to the "
        "front - buffers each by the margin that governs it, and differences "
        "the union out of the parcel. side_setback_m applies Mode "
        "d'implantation rather than the printed Laterale: contiguous building "
        "takes none, semi-detached half, and side_setback_rule says which was "
        "read. footprint_cap_m2 is the lesser of the buildable envelope and "
        "Taux d'implantation au sol max x lot area, with "
        "footprint_cap_binding naming which one bound. A lot with no frontage "
        "row has no front edge to sort against and gets no row. Upserted into "
        "silver.lot_buildable_setbacks on (scrape_date, neighborhood, "
        "lot_uid, feature_id, column_index) and written to "
        "silver/lot_buildable_setbacks/<YYYY-MM-DD>/<neighborhood>/"
        f"{LOT_SETBACKS_FILE}."
    ),
)
def lot_buildable_setbacks(
    context: AssetExecutionContext,
    config: SetbackConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    try:
        with postgis.connect() as connection:
            result = compute_lot_buildable_setbacks(
                connection,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                edge_tolerance_m=config.edge_tolerance_m,
            )
            num_lots = int(result["num_lots"])
            if num_lots == 0:
                # Not "the borough has no lots": the partition was never
                # loaded. Raised inside the transaction so the upsert rolls
                # back with it rather than leaving a partition half replaced.
                raise Failure(
                    f"rag.lots holds no lot for {neighborhood} {scrape_date} - "
                    "materialize building_lot_intersections for this partition "
                    "first."
                )
            if int(result["num_envelopes"]) == 0:
                # A different gap from the one above and with a different fix,
                # so it is named separately rather than left to surface as a
                # table of zero rows. Nothing here can be computed without the
                # margins, and the margins are the envelope asset's.
                raise Failure(
                    f"silver.lot_zoning_envelopes holds no row for "
                    f"{neighborhood} {scrape_date} - materialize "
                    "lot_zoning_envelopes for this partition first."
                )
            # Inside the transaction that computed it, so the file is that
            # answer rather than whatever a concurrent run leaves behind.
            frame = fetch_lot_buildable_setbacks(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(str(exc)) from exc

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_SETBACKS_FILE))

    lots_measured = int(result["num_lots_measured"])
    bound_by_setbacks = int(result["num_bound_by_setbacks"])
    bound_by_coverage = int(result["num_bound_by_site_coverage"])
    rows = int(result["rows"])
    context.log.info(
        "%s %s: %d envelope(s) across %d lot(s) -> %d row(s), %.1f ha "
        "buildable, mean %.1f pct of lot; %d bound by margins, %d by "
        "coverage -> %s",
        neighborhood,
        scrape_date,
        int(result["num_envelopes"]),
        lots_measured,
        rows,
        result["total_buildable_area_m2"] / 10_000.0,
        result["mean_buildable_pct_of_lot"],
        bound_by_setbacks,
        bound_by_coverage,
        path,
    )

    # A lot whose boundary could not be sorted has no front edge, which for a
    # Montreal parcel means its frontage row is missing rather than that it is
    # interior. A warning and not a Failure, the posture `lot_frontage` takes
    # towards the same lots: a borough measuring badly is a number to read.
    unsorted = num_lots - int(result["num_lots_sorted"])
    if unsorted:
        context.log.warning(
            "%s %s: %d of %d lot(s) (%.1f pct) have no measured frontage, so "
            "no front edge to sort a boundary against, and are absent from "
            "this table - re-materialize lot_frontage if the share is large",
            neighborhood,
            scrape_date,
            unsorted,
            num_lots,
            100.0 * unsorted / num_lots,
        )

    by_rule = result["by_side_setback_rule"]
    if not by_rule.get("contigu") and not by_rule.get("jumele"):
        # Every column read as isolated or unknown. Possible in principle and
        # wrong in VSMPE, whose grids print I-J and I-J-C throughout, so it is
        # surfaced rather than left to look like a borough of detached houses.
        context.log.warning(
            "%s %s: no column read as contiguous or semi-detached - every "
            "side margin was subtracted from both sides. Check "
            "implantation_mode on silver.zoning_grid_columns: %s",
            neighborhood,
            scrape_date,
            by_rule or "no rows",
        )

    return MaterializeResult(
        metadata={
            "dagster/row_count": rows,
            "num_rows": rows,
            "num_lots": num_lots,
            "num_lots_measured": lots_measured,
            # The two gaps, told apart. A lot missing from the first was never
            # sorted (no frontage row); one missing from the second was sorted
            # and had no grid to subtract.
            "num_lots_sorted": int(result["num_lots_sorted"]),
            "num_lots_without_frontage": int(result["lots_without_frontage"]),
            "num_lots_with_envelopes": int(result["num_lots_with_envelopes"]),
            "num_envelopes": int(result["num_envelopes"]),
            # The headline: which norm actually shapes this borough.
            "num_bound_by_setbacks": bound_by_setbacks,
            "num_bound_by_site_coverage": bound_by_coverage,
            "pct_bound_by_setbacks": round(100.0 * bound_by_setbacks / rows, 1)
            if rows
            else 0.0,
            # A real answer, not a gap: a parcel narrower than twice its side
            # margin has nowhere to put a building.
            "num_unbuildable": int(result["num_unbuildable"]),
            "total_buildable_area_ha": round(
                result["total_buildable_area_m2"] / 10_000.0, 2
            ),
            "mean_buildable_pct_of_lot": round(
                result["mean_buildable_pct_of_lot"], 1
            ),
            # How the borough read under each *Mode d'implantation*. The number
            # that says whether the side rule did what it should - see the
            # warning above.
            "by_side_setback_rule": MetadataValue.json(by_rule),
            "num_rows_pruned": int(result["pruned"]),
            # What the numbers above mean depends on these, so they travel with
            # them rather than only in the run's config.
            "edge_tolerance_m": config.edge_tolerance_m,
            "max_sin": result["max_sin"],
            "segment_m": result["segment_m"],
            "output_path": MetadataValue.path(str(path)),
        }
    )
