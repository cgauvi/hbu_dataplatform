"""The map's tiles, rendered once per partition into PMTiles archives.

hbu_rag_map used to render every vector tile on demand, in the Streamlit
process, with one ``ST_AsMVT`` query per 256-pixel square against the same
Postgres the panes read. `map_tiles` is that rendering moved here, where the
rest of the partition's derived tables are made: the same SQL, run once over
every tile of every zoom the map can be at, and packed into one PMTiles file
per layer that the map reads straight off S3 with HTTP range requests.

**Why it is an asset and not a cache.** A tile is a pure function of the
partition behind it, and the partition changes at most once a month; the
map's cache rendered the same tile for every task and every session and
threw it away on restart. Materialised here it is versioned, partitioned and
kept in the tree beside the tables it was drawn from - and it can be rebuilt
from them in minutes, which is the same argument `map_cell_aggregates` makes
for itself.

**It is downstream of everything the map draws.** Nine layers over the
working set, three silver tables and five gold ones, so it runs last and
re-runs whenever any of them does. A source that has not been materialised
for a partition contributes no archive rather than failing the run: a
borough without `lot_building_massing` should still get its cadastre, and the
manifest names the layers that came back empty so the map can say so rather
than draw a silent blank.

**One file per layer, not one for all nine.** The map holds one Leaflet
layer per map layer, ticked independently; an archive per layer means a
layer that is off costs no bytes, and a layer whose asset re-ran can be
rebuilt alone with ``layers=[...]``.
"""

import json
import tempfile
from datetime import datetime, timezone
from typing import Any

from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pydantic import Field

from hbu_dataplatform.map import map_tiles
from hbu_dataplatform.core import tile_grid
from hbu_dataplatform.map.aggregate_assets import map_cell_aggregates
from hbu_dataplatform.cadastre.building_lots_assets import building_lot_intersections
from hbu_dataplatform.hbu.hbu_assets import lot_highest_best_use, lot_redevelopment_gap
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.hbu.massing_assets import lot_building_massing
from hbu_dataplatform.hbu.opportunity_assets import lot_investment_opportunities
from hbu_dataplatform.partitions.axes import borough_partition_of, scrape_partitions
from hbu_dataplatform.map.pmtiles_archive import ArchiveSummary, write_archive
from hbu_dataplatform.core.pg import PostgresUnavailable
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.core.storage import clear_files, join, write_bytes
from hbu_dataplatform.sources.rqtt.assets import neighborhood_streets
from hbu_dataplatform.map.tile_render import layer_extent, render_layer, tile_fields
from hbu_dataplatform.zoning.zone_piece_assets import lot_zone_pieces

GROUP = "gold_map"


class MapTilesConfig(Config):
    """Which layers to render. Everything else about a tile is not config.

    The extent, the buffer, the zoom each layer draws itself from and the
    offset an aggregate tile is filled at are all facts the map's renderer
    was written against - a tile built with a different extent draws at the
    wrong scale, and one built at a different detail zoom draws cells where
    the legend says lots. They live in `hbu_dataplatform.map.map_tiles` as constants.
    """

    layers: list[str] = Field(
        default=list(map_tiles.LAYERS),
        description=(
            "The map layers to render, from hbu_dataplatform.map.map_tiles.LAYERS. Narrow "
            "it to rebuild one layer's archive after its source asset re-ran; "
            "the default is all nine. A narrowed run replaces only the "
            "archives it names and rewrites the manifest for the whole "
            "partition, so the layers left out keep their files."
        ),
    )


@asset(
    # Named explicitly: the function cannot be called `map_tiles` without
    # shadowing the module of that name it reads its specs from.
    name="map_tiles",
    key_prefix=key_prefix("map_tiles"),
    partitions_def=scrape_partitions,
    # Every table a tile reads, as deps rather than inputs: the rendering
    # happens in Postgres and never sees a frame. `building_lot_intersections`
    # stands for the working set as well - it is the asset that loads
    # `rag.lots`, `rag.buildings` and `rag.features`.
    deps=[
        building_lot_intersections,
        neighborhood_streets,
        lot_zone_pieces,
        lot_redevelopment_gap,
        lot_highest_best_use,
        lot_investment_opportunities,
        lot_building_massing,
        map_cell_aggregates,
    ],
    group_name=GROUP,
    kinds={"postgis", "pmtiles", "s3"},
    description=(
        "Every map layer rendered as Mapbox Vector Tiles at zooms "
        f"{map_tiles.MIN_DISPLAY_ZOOM}-{map_tiles.MAX_TILE_ZOOM} and packed into "
        "one PMTiles archive per layer, so hbu_rag_map draws a borough straight "
        "off S3 with HTTP range requests and no database in the request path. "
        "Below a layer's detail zoom the tiles hold the dissolved cells of "
        "map_cell_aggregates at zoom + "
        f"{tile_grid.ZOOM_OFFSET}, and below zoom {map_tiles.AGGREGATE_OUTLINE_ZOOM} "
        "their outlines. The screens the map's sidebar toggles travel as tile "
        "properties and are applied in the browser. Written to "
        f"gold/map_tiles/<YYYY-MM-DD>/<neighborhood>/<layer>{map_tiles.ARCHIVE_SUFFIX} "
        f"beside a {map_tiles.MANIFEST_FILE} naming what was built."
    ),
)
def map_tiles_asset(
    context: AssetExecutionContext,
    config: MapTilesConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)

    unknown = sorted(set(config.layers) - set(map_tiles.LAYERS))
    if unknown:
        raise Failure(
            f"{', '.join(unknown)} is not a layer hbu_dataplatform.map.map_tiles knows how "
            f"to render; it has {', '.join(map_tiles.LAYERS)}."
        )
    if not config.layers:
        raise Failure("No layers selected. Leave `layers` at its default to render all nine.")

    output_dir = store.partition_dir("map_tiles", scrape_date, neighborhood)
    previous = _read_manifest(output_dir)
    removed = clear_files(
        output_dir,
        *[map_tiles.archive_file(layer) for layer in config.layers],
    )
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    # The manifest is what the map reads, so it is rewritten rather than
    # removed: minus the layers this run is replacing, whose archives are gone,
    # and still naming the ones it is not touching. A municipality is rendered
    # in batches that take hours each, and a partition with no manifest is a
    # partition the map cannot draw at all - so between batches, and if a
    # batch is killed, what was built stays visible. Only when nothing is
    # left to name (a full run, or a first one) does the file go.
    interim = _interim_manifest(previous, config.layers)
    if interim is None:
        clear_files(output_dir, map_tiles.MANIFEST_FILE)
    else:
        _write_json(join(output_dir, map_tiles.MANIFEST_FILE), interim)
        context.log.info(
            "Manifest keeps %s while %s render(s)",
            ", ".join(interim["layers"]),
            ", ".join(config.layers),
        )

    params = map_tiles.layer_params(neighborhood, scrape_date)
    built: dict[str, dict[str, Any]] = {}
    empty: list[str] = []
    skipped: dict[str, str] = {}
    bounds_union: list[float] | None = None

    try:
        with postgis.connect() as connection:
            cursor = connection.cursor()
            for layer in config.layers:
                spec = map_tiles.layer_spec(layer)
                missing = _missing_relations(cursor, map_tiles.LAYER_RELATIONS[layer])
                if missing:
                    skipped[layer] = f"not created on this database: {', '.join(missing)}"
                    context.log.warning(
                        "%s %s: skipping the %s layer - %s. Apply hbu_infra's sql/ "
                        "and materialize the source asset.",
                        neighborhood,
                        scrape_date,
                        layer,
                        skipped[layer],
                    )
                    continue

                bounds = layer_extent(cursor, spec, params)
                if bounds is None:
                    empty.append(layer)
                    context.log.warning(
                        "%s %s: no rows for the %s layer - its source holds nothing "
                        "for this partition, so no archive is written and the map "
                        "will say so. Materialize that layer's asset and re-run.",
                        neighborhood,
                        scrape_date,
                        layer,
                    )
                    continue

                fields = tile_fields(cursor, spec, params)
                summary = _render_archive(
                    context, cursor, spec, output_dir, bounds, fields, params,
                    neighborhood=neighborhood, scrape_date=scrape_date,
                )
                if summary.tile_count == 0:
                    empty.append(layer)
                    continue

                built[layer] = {
                    "file": map_tiles.archive_file(layer),
                    "detail_zoom": spec.detail_zoom,
                    "has_aggregate": spec.has_aggregate,
                    "feature_id": spec.feature_id,
                    "bounds": list(bounds),
                    "fields": fields,
                    **summary.as_manifest(),
                }
                bounds_union = _union(bounds_union, bounds)
    except (PostgresUnavailable, MissingRelationInTransaction) as exc:
        raise Failure(
            f"map tiles could not be rendered for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    # A narrowed run keeps the archives it did not name, and the manifest has
    # to go on describing them - so their entries are carried over from the
    # previous manifest rather than dropped.
    if previous:
        for layer, entry in previous.get("layers", {}).items():
            if layer not in config.layers and layer not in built:
                built[layer] = entry
                bounds_union = _union(bounds_union, tuple(entry.get("bounds") or ()) or None)

    manifest = {
        "scrape_date": scrape_date,
        "neighborhood": neighborhood,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "min_display_zoom": map_tiles.MIN_DISPLAY_ZOOM,
        "max_tile_zoom": map_tiles.MAX_TILE_ZOOM,
        "zoom_offset": tile_grid.ZOOM_OFFSET,
        "outline_zoom": map_tiles.AGGREGATE_OUTLINE_ZOOM,
        "extent": map_tiles.EXTENT,
        "bounds": bounds_union,
        "layers": {layer: built[layer] for layer in map_tiles.LAYERS if layer in built},
        "empty_layers": sorted(empty),
        "skipped_layers": skipped,
    }
    manifest_path = join(output_dir, map_tiles.MANIFEST_FILE)
    _write_json(manifest_path, manifest)

    total_tiles = sum(int(entry["tiles"]) for entry in built.values())
    total_bytes = sum(int(entry["bytes"]) for entry in built.values())
    context.log.info(
        "%s %s: %d tile(s), %.1f MB over %d layer(s) -> %s",
        neighborhood,
        scrape_date,
        total_tiles,
        total_bytes / 1e6,
        len(built),
        output_dir,
    )

    return MaterializeResult(
        metadata={
            "num_tiles": total_tiles,
            "tile_bytes": total_bytes,
            "num_layers": len(built),
            **{f"num_tiles_{layer}": int(entry["tiles"]) for layer, entry in built.items()},
            **{f"bytes_{layer}": int(entry["bytes"]) for layer, entry in built.items()},
            "layers_empty": MetadataValue.json(sorted(empty)),
            "layers_skipped": MetadataValue.json(skipped),
            "bounds": MetadataValue.json(bounds_union),
            "zooms": MetadataValue.json(
                [map_tiles.MIN_DISPLAY_ZOOM, map_tiles.MAX_TILE_ZOOM]
            ),
            "detail_zooms": MetadataValue.json(map_tiles.DETAIL_ZOOM),
            "manifest_path": MetadataValue.path(str(manifest_path)),
            "output_path": MetadataValue.path(str(output_dir)),
        }
    )


class MissingRelationInTransaction(RuntimeError):
    """Raised when a relation vanished between the check and the read."""


def _render_archive(
    context: AssetExecutionContext,
    cursor: Any,
    spec: map_tiles.TileLayerSpec,
    output_dir: str,
    bounds: tuple[float, float, float, float],
    fields: dict[str, str],
    params: dict[str, Any],
    *,
    neighborhood: str,
    scrape_date: str,
) -> ArchiveSummary:
    """Render ``spec`` into a temporary file and, if it holds anything, publish it.

    Spooled locally first because the writer seeks - it lays the directory
    out ahead of the tile data it has already buffered - and an S3 object
    cannot be seeked into. The upload is one streamed copy.
    """

    def progress(zoom: int, rendered: int, addressed: int) -> None:
        context.log.debug(
            "%s %s: %s z%d - %d of %d tile(s) hold something",
            neighborhood, scrape_date, spec.name, zoom, rendered, addressed,
        )

    with tempfile.TemporaryFile() as spool:
        summary = write_archive(
            spool,
            render_layer(cursor, spec, bounds=bounds, params=params, progress=progress),
            layer=spec.name,
            bounds=bounds,
            fields=fields,
            metadata={
                "hbu": {
                    "scrape_date": scrape_date,
                    "neighborhood": neighborhood,
                    "layer": spec.name,
                    "detail_zoom": spec.detail_zoom,
                    "zoom_offset": tile_grid.ZOOM_OFFSET,
                    "outline_zoom": map_tiles.AGGREGATE_OUTLINE_ZOOM,
                    "feature_id": spec.feature_id,
                }
            },
        )
        if summary.tile_count == 0:
            context.log.warning(
                "%s %s: the %s layer rendered no tile over its own extent; "
                "no archive written.",
                neighborhood,
                scrape_date,
                spec.name,
            )
            return summary
        spool.seek(0)
        path = join(output_dir, map_tiles.archive_file(spec.name))
        size = write_bytes(path, spool)
    context.log.info(
        "%s %s: %s - %d tile(s) at z%d-%d, %.1f MB -> %s",
        neighborhood,
        scrape_date,
        spec.name,
        summary.tile_count,
        summary.min_zoom,
        summary.max_zoom,
        size / 1e6,
        path,
    )
    return summary


def _missing_relations(cursor: Any, relations: tuple[str, ...]) -> list[str]:
    """The relations of ``relations`` not on this database.

    `to_regclass` rather than a catalog lookup: it resolves a view and a
    table alike, and returns NULL instead of raising for a name that is not
    there - which matters here, because an error would abort the transaction
    the other layers are being rendered in.
    """
    missing: list[str] = []
    for name in relations:
        cursor.execute("SELECT to_regclass(%s)", [name])
        (resolved,) = cursor.fetchone()
        if resolved is None:
            missing.append(name)
    return missing


def _read_manifest(output_dir: str) -> dict[str, Any] | None:
    from hbu_dataplatform.core.storage import read_text  # noqa: PLC0415

    text = read_text(join(output_dir, map_tiles.MANIFEST_FILE))
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def _interim_manifest(
    previous: dict[str, Any] | None, rendering: list[str]
) -> dict[str, Any] | None:
    """The previous manifest with the layers now being re-rendered taken out.

    None when there is nothing left to describe - no previous manifest, or
    one whose every layer is in ``rendering`` - which the caller turns into
    no manifest at all, since a file naming archives that were just deleted
    is worse than none. The bounds are re-united over what remains, and the
    layers under way are named under ``rendering`` so a reader of the file
    can tell a batch in progress from a partition that was never built.
    """
    if not previous:
        return None
    kept = {
        layer: entry
        for layer, entry in (previous.get("layers") or {}).items()
        if layer not in rendering
    }
    if not kept:
        return None
    bounds: list[float] | None = None
    for entry in kept.values():
        bounds = _union(bounds, tuple(entry.get("bounds") or ()) or None)
    return {
        **previous,
        "bounds": bounds,
        "layers": kept,
        "empty_layers": [
            layer for layer in previous.get("empty_layers") or [] if layer not in rendering
        ],
        "skipped_layers": {
            layer: why
            for layer, why in (previous.get("skipped_layers") or {}).items()
            if layer not in rendering
        },
        "rendering": sorted(rendering),
    }


def _write_json(path: str, payload: dict[str, Any]) -> None:
    import io  # noqa: PLC0415

    write_bytes(path, io.BytesIO(json.dumps(payload, indent=2).encode("utf-8")))


def _union(
    current: list[float] | None, bounds: tuple[float, ...] | None
) -> list[float] | None:
    if not bounds or len(bounds) != 4:
        return current
    if current is None:
        return list(bounds)
    return [
        min(current[0], bounds[0]),
        min(current[1], bounds[1]),
        max(current[2], bounds[2]),
        max(current[3], bounds[3]),
    ]
