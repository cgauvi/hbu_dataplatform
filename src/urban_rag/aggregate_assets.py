"""The map's gated layers, dissolved onto the tile grid so a zoomed-out view
has something true to draw.

Every spatial layer hbu_rag_map serves is gated on zoom, and for a good
reason: a lot is sub-pixel below zoom 15, and twenty-five thousand of them is a
solid grey rectangle that costs a second of browser time to produce. So below
its gate a layer is simply not drawn — and "not drawn" is the wrong answer to
*where in this borough is the headroom*, which is the one question a
borough-wide view exists to answer.

`map_cell_aggregates` is that question's answer, precomputed. One row per
(layer, cell), where a cell is a Web Mercator tile four zooms finer than the
one being looked at, carrying what the layer dissolves to over that ground.
The grid arithmetic and the per-layer measures are in `urban_rag.tile_grid`;
the rollup SQL is `urban_rag.postgis.compute_map_cell_aggregates`; the table is
hbu_infra's sql/023.

**Why it is an asset and not a materialized view.** The obvious alternative is
to let the map aggregate on the fly — `GROUP BY` the lots into cells at query
time. That would be a borough-wide aggregate on every tile request, at exactly
the zoom where a borough-wide aggregate is most expensive, repeated for every
tile on screen and every user looking. Precomputing turns the whole low-zoom
band into an index scan over a few hundred rows.

**Why it is gold and not a cache.** Every number here is derived, and derived
in a way that is easy to get subtly wrong — see `tile_grid` on the two
assignments and on why a cell's percentage is recomputed from sums rather than
averaged. Those decisions belong in the lineage with the rest of them, where
they are versioned, partitioned and testable, and not in a serving layer where
the only record of them would be a query string.

**It is downstream of everything.** Its five sources are the working set
(`rag.lots`, `rag.buildings`), one silver table and two gold ones, so it runs
last and re-runs whenever any of them does. A source that has not been
materialized for a partition contributes no cells rather than failing the run:
a borough without `lot_building_massing` should still get a utilisation
surface, and the metadata reports the per-layer counts so an empty layer is
visible rather than silent.
"""

from datetime import datetime, timezone

from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pydantic import Field

from urban_rag import tile_grid
from urban_rag.building_lots_assets import building_lot_intersections
from urban_rag.frames import write_frame
from urban_rag.hbu_assets import lot_redevelopment_gap
from urban_rag.layers import key_prefix
from urban_rag.massing_assets import lot_building_massing
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    compute_map_cell_aggregates,
    fetch_map_cell_aggregates,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, join
from urban_rag.street_assets import neighborhood_streets
from urban_rag.warehouse import MissingRelation, published_metadata

GROUP = "gold_map"

#: One file per partition, under
#: `gold/map_cell_aggregates/<YYYY-MM-DD>/<neighborhood>/`.
MAP_CELLS_FILE = "map_cell_aggregates.parquet"


class MapAggregateConfig(Config):
    """Which layers to build. Everything else about the grid is not config.

    `tile_grid.ZOOM_OFFSET` and `CELL_ZOOMS` are deliberately *not* here. They
    are not preferences: the offset is what bounds a served tile at 256
    features, and the levels are what the map's zoom range needs. Exposing
    them as run config would let a single run write a partition whose tiles no
    longer have that bound, and nothing downstream would notice until a
    borough-wide view fell over.
    """

    layers: list[str] = Field(
        default=list(tile_grid.LAYERS),
        description=(
            "The map layers to dissolve, from urban_rag.tile_grid.LAYERS. "
            "Narrow it to rebuild one layer's cells after its source asset "
            "re-ran; the default is all five. Note that a narrowed run still "
            "prunes the partition, so the layers left out are removed rather "
            "than kept - this table is a snapshot of the partition, like "
            "every other one here."
        ),
    )


@asset(
    key_prefix=key_prefix("map_cell_aggregates"),
    partitions_def=scrape_partitions,
    # The working set behind `rag.lots` and `rag.buildings`, plus the three
    # published tables. Declared as deps rather than as inputs because every
    # one of them is read *in Postgres* - the rollup never sees a frame.
    deps=[
        building_lot_intersections,
        neighborhood_streets,
        lot_redevelopment_gap,
        lot_building_massing,
    ],
    group_name=GROUP,
    kinds={"postgis", "postgres", "geoparquet"},
    description=(
        "Every zoom-gated map layer dissolved onto the Web Mercator tile grid, "
        "so a borough-wide view has something true to draw where it currently "
        "draws nothing. One row per (layer, cell) for lots, buildings, "
        "utilisation, proposed massing and street sides, at cell zooms "
        f"{tile_grid.CELL_ZOOMS[0]}-{tile_grid.CELL_ZOOMS[-1]}; the map serves "
        f"display zoom Z from cells at Z + {tile_grid.ZOOM_OFFSET}, so a "
        f"served tile carries at most {tile_grid.cells_per_tile()} features at "
        "any zoom by construction. Each cell carries the dissolved union of "
        "its features clipped to it, the count of features whose "
        "representative point falls in it, and one `value` named by "
        "`value_kind` - lots per km2, built coverage, used floor as a share of "
        "permitted, proposed dwellings per hectare, street km per km2. "
        "Percentages are recomputed from summed numerators and denominators at "
        "every level, never averaged from the level below. Written to "
        f"gold/map_cell_aggregates/<YYYY-MM-DD>/<neighborhood>/{MAP_CELLS_FILE} "
        "and upserted on (scrape_date, neighborhood, layer, cell_z, cell_x, "
        "cell_y)."
    ),
)
def map_cell_aggregates(
    context: AssetExecutionContext,
    config: MapAggregateConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)

    unknown = sorted(set(config.layers) - set(tile_grid.LAYERS))
    if unknown:
        raise Failure(
            f"{', '.join(unknown)} is not a layer urban_rag.tile_grid knows "
            f"how to dissolve; it has {', '.join(tile_grid.LAYERS)}."
        )
    if not config.layers:
        raise Failure(
            "No layers selected, which would prune this partition to nothing. "
            "Leave `layers` at its default to build all five."
        )

    # One transaction for the whole pyramid, and it has to be: the two staging
    # tables are `ON COMMIT DROP`, so the seed, the four rollups and the upsert
    # are one unit or they are nothing.
    try:
        with postgis.connect() as connection:
            result = compute_map_cell_aggregates(
                connection,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                layers=config.layers,
            )
            cells = fetch_map_cell_aggregates(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"gold.map_cell_aggregates could not be computed for "
            f"{neighborhood} {scrape_date}: {exc}"
        ) from exc

    cells["computed_at"] = datetime.now(timezone.utc).isoformat()

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    # The tree keeps a copy for the reason `urban_rag.layers` gives for every
    # other asset: losing the database should cost a reload rather than a
    # recompute. It is the weakest case in this platform for that rule - these
    # rows can be rebuilt from three gold tables in a minute - and it is still
    # kept, because "which assets are exempt" is a worse thing to have to
    # remember than one extra file per partition.
    path = write_frame(cells, join(output_dir, MAP_CELLS_FILE))

    by_layer = {
        layer: int(count)
        for layer, count in cells["layer"].value_counts().sort_index().items()
    }
    empty = sorted(set(config.layers) - set(by_layer))
    for layer in empty:
        # Not a failure: a borough whose massing has not been solved should
        # still get the other four surfaces. But an empty layer that said
        # nothing would read on the map as "nothing can be built here", which
        # is the opposite of what a missing source means.
        context.log.warning(
            "%s %s: no cells for the %s layer - its source table holds nothing "
            "for this partition, so the map will draw nothing there below the "
            "gate. Materialize that layer's asset and re-run.",
            neighborhood,
            scrape_date,
            layer,
        )

    max_per_tile = int(result["max_cells_per_served_tile"])
    context.log.info(
        "%s %s: %d cell(s) over %d layer(s) at zooms %s; busiest served tile "
        "holds %d of a possible %d -> %s",
        neighborhood,
        scrape_date,
        len(cells),
        len(by_layer),
        "-".join(str(zoom) for zoom in (tile_grid.CELL_ZOOMS[0], tile_grid.CELL_ZOOMS[-1])),
        max_per_tile,
        tile_grid.cells_per_tile(),
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(cells),
            "num_cells": len(cells),
            **{f"num_cells_{layer}": count for layer, count in by_layer.items()},
            "layers_empty": MetadataValue.json(empty),
            "num_cells_by_layer_level": MetadataValue.json(
                result["num_cells_by_layer_level"]
            ),
            # The bound the whole design rests on, reported per run rather than
            # asserted once: a served tile can hold at most this many cells,
            # and seeing the number stay at or under `cells_per_tile` is what
            # says the pyramid is still built the way it is documented.
            "max_cells_per_served_tile": max_per_tile,
            "cells_per_served_tile_limit": tile_grid.cells_per_tile(),
            "cell_zooms": MetadataValue.json(list(tile_grid.CELL_ZOOMS)),
            "display_zooms_served": MetadataValue.json(
                [zoom - tile_grid.ZOOM_OFFSET for zoom in tile_grid.CELL_ZOOMS]
            ),
            "zoom_offset": tile_grid.ZOOM_OFFSET,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata({"map_cell_aggregates": result["published"]}),
        }
    )


def _partition(context: AssetExecutionContext) -> tuple[str, str]:
    dimensions = context.partition_key.keys_by_dimension
    return dimensions["neighborhood"], dimensions["date"][:10]
