"""The building the solver costed, drawn on the ground it would stand on.

One gold asset over two upstreams, and the only one in this platform whose
output is meant to be *looked at* rather than queried. `lot_highest_best_use`
says a lot's highest and best use is a 287 m2 footprint under five storeys;
`lot_buildable_setbacks` says what is left of the parcel once the zone's four
margins come off. Neither says whether a building of that footprint can be
drawn inside those margins, because `solve_program` works in areas and an area
is not a shape.

`lot_building_massing` draws it. One rectangle per lot, fitted inside the
buildable polygon so the margins are respected by construction, in EPSG:4326
and ready to put on a map beside the cadastre. The geometry is in
`urban_rag.massing`, free of Dagster the way `program`, `comparables` and
`hbu` are; this module is the partition handling.

**The column worth reading is `footprint_fit_pct`.** A buildable envelope of
200 m2 that runs 40 m deep and 5 m wide holds no rectangle of 200 m2 at all,
and the solver - capping on the *lesser of two areas* and stopping - will
spend all 200 of them regardless. So a fit below 100% is this asset reporting
a shape the answer upstream cannot actually take, which is exactly what a
sanity check is for. Those rows are shrunk to the largest rectangle that does
fit rather than dropped, because dropping them would hide the lots most worth
looking at.

**Every lot keeps a row in the tree; only the drawn ones reach Postgres.**
That split is `urban_rag.warehouse`'s rule rather than this asset's choice - a
row with no shape is not a row of a spatial table, so it is skipped on the way
into `gold.lot_building_massing` - and it is why the parquet is the record
here more than usually. `massing_status` distinguishes the four ways a lot ends
up without a rectangle, and a reader wanting them in SQL gets them by
anti-joining `gold.lot_highest_best_use`, which has every lot and its
`hbu_status`. The run's metadata reports both counts so the gap is never a
surprise.

**Why an asset and not a notebook.** The fit is deterministic, it is partitioned
like everything else, and its answer changes whenever the program does - a
different `stalls_per_dwelling` is a different footprint is a different
rectangle. Keeping it in the lineage is what makes "re-materialize the borough
and look at the map" one command rather than a script somebody has to remember
to re-run.
"""

from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pydantic import Field

from urban_rag.frames import write_frame
from urban_rag.hbu_assets import LOT_HBU_FILE, lot_highest_best_use
from urban_rag.layers import key_prefix
from urban_rag.massing import (
    DEFAULT_ASPECT_RATIOS,
    GRID_STEPS,
    MASSING_STATUSES,
    MIN_FOOTPRINT_M2,
    SHRINK_STEPS,
    massing_frame,
)
from urban_rag.partitions import scrape_partitions
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.setback_assets import LOT_SETBACKS_FILE, lot_buildable_setbacks
from urban_rag.storage import clear_parquet, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

GROUP = "gold_hbu"

#: One file per partition, under
#: `gold/lot_building_massing/<YYYY-MM-DD>/<neighborhood>/`.
LOT_MASSING_FILE = "lot_building_massing.parquet"


class MassingConfig(Config):
    """How hard to look for a rectangle, and what shapes to look for.

    Every field is a property of the *search* rather than of the parcel or the
    by-law, which is what makes them config: nothing in a zoning grid says a
    building is a rectangle, let alone which rectangle. The defaults are
    `urban_rag.massing`'s own constants, and the value that produced a table
    travels in the run's metadata the way every stated assumption in this
    platform does.
    """

    aspect_ratios: list[float] = Field(
        default=list(DEFAULT_ASPECT_RATIOS),
        description=(
            "Width-to-depth ratios to try, in order; the first that fits at "
            "full footprint wins. Each is tried at the parcel's own axis and "
            "at the perpendicular, so 2.0 covers 1:2 as well. Squarest first "
            "is the useful order - a square is the most compact rectangle of "
            "a given area and the least likely to be an artefact of a long "
            "thin envelope."
        ),
    )
    grid_steps: int = Field(
        default=GRID_STEPS,
        ge=1,
        le=64,
        description=(
            "Candidate centres per axis, squared before the ones outside the "
            "envelope are dropped. Raising it finds placements a coarser grid "
            "misses, at a linear cost in a vectorised call."
        ),
    )
    shrink_steps: int = Field(
        default=SHRINK_STEPS,
        ge=1,
        le=24,
        description=(
            "Bisections of the scale factor when no rectangle of the full "
            "footprint fits. 8 puts placed_footprint_m2 within about half a "
            "percent of the largest that does."
        ),
    )
    min_footprint_m2: float = Field(
        default=MIN_FOOTPRINT_M2,
        gt=0.0,
        description=(
            "Below this a footprint is not a building, and the lot is "
            "reported no_fit rather than given a rectangle that reads as a "
            "dot on a map."
        ),
    )


@asset(
    key_prefix=key_prefix("lot_building_massing"),
    partitions_def=scrape_partitions,
    deps=[lot_highest_best_use, lot_buildable_setbacks],
    group_name=GROUP,
    kinds={"shapely", "postgres", "geoparquet"},
    description=(
        "The highest-and-best-use building of every lot, as a rectangle on the "
        "ground: one row per lot, with a polygon in EPSG:4326 fitted inside "
        "that lot's buildable envelope so the zone's four margins are "
        "respected by construction. A few aspect ratios are tried at the "
        "parcel's own axis and its perpendicular, squarest first, and the "
        "first that fits at the solved footprint wins; aspect_ratio, width_m, "
        "depth_m and rotation_deg say which rectangle was drawn. Where none "
        "fits - a long thin envelope holds no rectangle of its own area - the "
        "best is shrunk rather than dropped, and footprint_fit_pct is how "
        "much of the solver's footprint the parcel can actually carry in that "
        "shape. That column is the sanity check this asset exists for: "
        "solve_program caps a footprint on the lesser of two areas and never "
        "asks whether the shape fits. massing_status is one of "
        f"{', '.join(MASSING_STATUSES)}. floors and height_m ride along so a "
        "map can extrude the rectangle without a join. Every lot keeps a row "
        "in the tree; only the lots with a polygon reach "
        "gold.lot_building_massing, since a row with no shape is not a row of "
        "a spatial table. Written to gold/lot_building_massing/<YYYY-MM-DD>/"
        f"<neighborhood>/{LOT_MASSING_FILE} and upserted on (scrape_date, "
        "neighborhood, lot_uid)."
    ),
)
def lot_building_massing(
    context: AssetExecutionContext,
    config: MassingConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    hbu = _read(
        store,
        lot_highest_best_use,
        LOT_HBU_FILE,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )
    if hbu.empty:
        raise Failure(
            f"{lot_highest_best_use.key.path[-1]} holds no lot for "
            f"{neighborhood} {scrape_date}; there is nothing to draw."
        )
    setbacks = _read_setbacks(context, store, neighborhood, scrape_date)

    frame = massing_frame(
        hbu,
        setbacks,
        aspect_ratios=tuple(config.aspect_ratios),
        grid_steps=config.grid_steps,
        shrink_steps=config.shrink_steps,
        min_footprint_m2=config.min_footprint_m2,
    )
    frame["neighborhood"] = neighborhood
    frame["scrape_date"] = scrape_date
    frame["computed_at"] = datetime.now(timezone.utc).isoformat()

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_MASSING_FILE))

    drawn = frame[frame["geometry"].notna()]
    try:
        loaded = publish(
            postgis.connect,
            {"lot_building_massing": frame},
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but gold.lot_building_massing could not be "
            f"updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    by_status = {
        status: int((frame["massing_status"] == status).sum())
        for status in MASSING_STATUSES
    }
    fit = pd.to_numeric(frame["footprint_fit_pct"], errors="coerce")
    context.log.info(
        "%s %s: %d lot(s) -> %s; %.1f ha of footprint drawn of %.1f ha "
        "solved, median fit %.1f%% -> %s",
        neighborhood,
        scrape_date,
        len(frame),
        ", ".join(f"{name}={count}" for name, count in by_status.items()),
        _sum(frame, "placed_footprint_m2") / 10_000.0,
        _sum(frame, "footprint_m2") / 10_000.0,
        float(fit.median()) if fit.notna().any() else 0.0,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_lots": len(frame),
            # What reaches the database, and what does not. The gap is every
            # lot with no polygon - see the module docstring on why the tree
            # keeps them and the spatial table does not.
            "num_drawn": len(drawn),
            "num_not_drawn": len(frame) - len(drawn),
            **{f"num_{name}": count for name, count in by_status.items()},
            # The sanity check, as one number. A borough where this is well
            # under 100 is a borough whose footprints are being capped on an
            # area that its parcels cannot take in any rectangle - which is a
            # finding about `solve_program`, not about this asset.
            "median_footprint_fit_pct": round(float(fit.median()), 1)
            if fit.notna().any()
            else 0.0,
            "mean_footprint_fit_pct": round(float(fit.mean()), 1)
            if fit.notna().any()
            else 0.0,
            # Lots where the shape costs more than a tenth of the footprint.
            # The list to open a map on.
            "num_fit_below_90_pct": int((fit < 90).sum()),
            "num_fit_below_50_pct": int((fit < 50).sum()),
            "solved_footprint_ha": round(_sum(frame, "footprint_m2") / 10_000.0, 2),
            "placed_footprint_ha": round(
                _sum(frame, "placed_footprint_m2") / 10_000.0, 2
            ),
            "solved_gross_floor_area_ha": round(
                _sum(frame, "gross_floor_area_m2") / 10_000.0, 2
            ),
            "placed_gross_floor_area_ha": round(
                _sum(frame, "placed_gross_floor_area_m2") / 10_000.0, 2
            ),
            # Which rectangle the borough actually takes. A borough that is all
            # 3.0 is one whose parcels are long and thin, which is Villeray;
            # one that is all 1.0 has room to spare everywhere.
            "aspect_ratios_used": MetadataValue.json(
                {
                    str(ratio): int(count)
                    for ratio, count in frame["aspect_ratio"]
                    .value_counts()
                    .sort_index()
                    .items()
                }
            ),
            "aspect_ratios_tried": MetadataValue.json(list(config.aspect_ratios)),
            "grid_steps": config.grid_steps,
            "shrink_steps": config.shrink_steps,
            "min_footprint_m2": config.min_footprint_m2,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


def _read_setbacks(
    context: AssetExecutionContext,
    store: ParquetStore,
    neighborhood: str,
    scrape_date: str,
) -> gpd.GeoDataFrame:
    """The buildable envelopes, or an empty frame with a warning.

    Missing rather than fatal, and that is a judgement about what this asset is
    for: without the setbacks there is no margin to respect, so every lot comes
    back `no_buildable_geometry` and the partition is a table of statuses rather
    than of polygons. That is a useless table but an honest one, and it is
    better than a rectangle drawn on a parcel with the margins ignored - which
    is what falling back to the lot boundary would produce, and which would
    look entirely plausible on a map.
    """
    partition_dir = store.partition_dir(
        lot_buildable_setbacks.key.path[-1], scrape_date, neighborhood
    )
    path = join(partition_dir, LOT_SETBACKS_FILE)
    if not filesystem(path).exists(path):
        context.log.warning(
            "%s is missing, so no lot has a buildable envelope to fit a "
            "building into and every row will be no_buildable_geometry - "
            "materialize %s for this partition first",
            path,
            lot_buildable_setbacks.key.path[-1],
        )
        return gpd.GeoDataFrame(
            {"lot_uid": [], "feature_id": [], "column_index": []},
            geometry=[],
            crs="EPSG:4326",
        )
    return gpd.read_parquet(path, storage_options=storage_options(path))


def _sum(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return 0.0
    total = pd.to_numeric(frame[column], errors="coerce").sum(min_count=1)
    return float(total) if pd.notna(total) else 0.0


def _partition(context: AssetExecutionContext) -> tuple[str, str]:
    dimensions = context.partition_key.keys_by_dimension
    return dimensions["neighborhood"], dimensions["date"][:10]


def _read(
    store: ParquetStore,
    asset_def,
    name: str,
    *,
    neighborhood: str,
    scrape_date: str,
) -> pd.DataFrame:
    """One upstream partition, named by its asset rather than by its path."""
    asset_name = asset_def.key.path[-1]
    path = join(store.partition_dir(asset_name, scrape_date, neighborhood), name)
    if not filesystem(path).exists(path):
        raise Failure(
            f"{path} is missing; materialize {asset_name} for "
            f"{neighborhood} {scrape_date} first."
        )
    return pd.read_parquet(path, storage_options=storage_options(path))
