"""The zoning envelope of every lot, at the grain the solver reads.

`hbu_dataplatform.hbu.program` answers one question - what unit mix is the envelope of
this parcel worth - and needs three things to ask it: a `ZoneColumn` off the
zone's *grille des usages et des normes*, the lot's area, and its frontage.
Each of the three already exists somewhere in this platform, and none of them
next to the other two. These two assets close that gap.

**`zoning_grid_columns`** parses the PDFs `linked_documents` already fetched.
The corpus reads those files as prose; this reads them as the tables they are,
one row per column of each grid, with the norms as columns of the row - see
`hbu_dataplatform.zoning.zoning_grid` for why that is a question about x-coordinates. A grid
is published per zone but linked per feature, and two zones can share one
file, so the rows are emitted per ``(document, feature_id, column)``: the join
key downstream is the zone number the map carries, not the one printed on the
page, and joining on the printed one would drop the second zone of a shared
grid.

**`lot_zoning_envelopes`** is the denormalised table itself: one row per
``(lot, zone, grid column)``, carrying the *piece* of the lot that zone
governs, its own primary and secondary frontage, and every norm that column
states. It is a join of two things this platform already computes and nothing
more - `lot_zone_pieces` says what ground each zone governs and what faces the
street, and the grid columns above say what may be built on it. A row of it is
one call to `solve_program`, with `governs_residential` marking the row
`select_residential_column` would pick.

**The parcel a row describes is the piece, not the lot.** This is the change
`lot_zone_pieces` exists for and it reaches every column here.
`piece_area_m2` is the ground *this zone* covers and it is what the solver
sizes a building on; `lot_area_m2` is the whole parcel, carried so a reader
knows what the piece is a piece of. `primary_frontage_m` is the street the
piece faces rather than the street its lot faces - on lot 1 740 794 those are
two different streets, and the commercial strip on Jarry used to be solved as
though the housing behind it shared its frontage. See
`hbu_dataplatform.zoning.zone_piece_assets`.

**Two axes meet here.** `lot_zone_pieces` runs per tile - a cut cell's lots -
and `zoning_grid_columns` runs per borough, because a grid is something a
borough publishes. A tile can span two boroughs, and Montreal restarts its zone
numbers at C01-001 in every one of them, so the join is not on the zone number
alone: each piece is joined to *its own lot's* borough's grid, read for every
borough the tile's lots belong to. That is the bridge every tile asset with a
borough-axis upstream crosses, and it is spelled out in the deps as a
`MultiPartitionMapping` on the date alone - the borough dimension is left to
"all", which is what a tile that does not know its boroughs until it reads its
lots needs.

**Why denormalised.** The alternative is three tables and a join at read time,
and the reader is a solver that runs per lot: every column it needs on one row
is what makes "solve this borough" a scan rather than a query plan. The cost
is the usual one - a norm restated on every lot in its zone - and it is worth
paying here because the grain is the question's own grain.

**A row is not an answer.** Nothing here decides anything: `permits_residential`
and `governs_residential` are the grid's own reading of itself, `solver_ready`
says only that the four caps a CP-SAT model needs are present, and every value
is the one printed on the page with ``-`` carried through as null rather than
as zero. What the envelope is worth is `solve_program`'s to say.

**Nor does it decide which zones exist.** It used to: this asset carried the
two sliver cutoffs and applied them to `building_lot_intersections`' lot x
feature side on its way past. They moved to `zone_piece_assets.ZonePieceConfig`
along with the join itself, because they decide which *sites* there are, and
that is now a table rather than a filter inside a join - one every downstream
asset reads, so the zone the map shows, the zone the corpus answers from and
the zone the solver priced still cannot disagree. The values are on every row
here (`min_pct_of_lot`, `min_overlap_m2`, `min_piece_area_m2`), carried through
from the piece, so nothing about reading this table back has changed.
"""

import json
from collections.abc import Sequence

import pandas as pd
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

from hbu_dataplatform.zoning.features_assets import neighborhood_features
from hbu_dataplatform.core.frames import write_frame
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.partitions.axes import (
    City,
    borough_partition_of,
    city_of,
    scrape_partitions,
    tile_partition_of,
    tile_scrape_partitions,
)
from hbu_dataplatform.hbu.program import (
    ProgramError,
    ZoneColumn,
    is_commercial_usage,
    is_industrial_usage,
    permitted_floors,
    select_governing_column,
    storey_ceiling_from_height,
)
from hbu_dataplatform.cities.quebec_city.zoning import (
    GRID_SLUG,
    GRID_ZONE_COLUMN,
    ZONING_SLUG,
    grid_columns,
)
from hbu_dataplatform.rag.documents import DOCUMENT_SOURCES
from hbu_dataplatform.rag.pgvector import PostgresUnavailable
from hbu_dataplatform.rag.assets import DOCUMENTS_FILE, linked_documents
from hbu_dataplatform.core.resources import ParquetStore, PdfCache, PostgisResource
from hbu_dataplatform.core.storage import (
    clear_parquet,
    filesystem,
    join,
    storage_options,
)
from hbu_dataplatform.core.warehouse import MissingRelation, publish, published_metadata
from hbu_dataplatform.zoning.zone_piece_assets import (
    LOT_ZONE_PIECES_FILE,
    lot_zone_pieces,
)
from hbu_dataplatform.cities.saguenay.zoning import (
    parse_grid_pdf as parse_saguenay_grid_pdf,
)
from hbu_dataplatform.zoning.zoning_grid import (
    ZONE_FIELDS,
    GridColumn,
    GridParseError,
    parse_grid_pdf,
)

GROUP = "silver_zoning"

#: One file per partition, under
#: `silver/zoning_grid_columns/<YYYY-MM-DD>/<neighborhood>/`.
ZONE_COLUMNS_FILE = "zone_columns.parquet"

#: One file per partition, under
#: `silver/lot_zoning_envelopes/<YYYY-MM-DD>/<tile>/`.
LOT_ENVELOPES_FILE = "lot_zoning_envelopes.parquet"

#: How a tile asset depends on a borough one. The date maps to itself and the
#: borough dimension is left unlisted, which Dagster reads as "all of them":
#: a tile does not know which boroughs it holds until it reads its lots, so
#: the dependency cannot name them. See the module docstring.
_BOROUGH_BRIDGE = MultiPartitionMapping(
    {"date": DimensionPartitionMapping("date", IdentityPartitionMapping())}
)

#: What every row of a tile-axis table carries about the lot it belongs to,
#: taken off `lot_zone_pieces` and never stamped here: a tile can span two
#: boroughs, so the borough is the lot's own, and the two cell columns are the
#: address the warehouse partitions on.
_OWNERSHIP_COLUMNS = ("neighborhood", "cell_key", "cell_partition")

#: What each row carries about the ground it describes, taken straight from
#: `lot_zone_pieces` and not recomputed here.
#:
#: The frontage pair is the piece's own, which is the part worth naming twice:
#: `lot_frontage` ranks a *lot's* street edges longest first, and
#: `lot_zone_pieces` cuts those edges to each piece and ranks them again inside
#: it. So `primary_frontage_m` is the street this zone's ground fronts on, and
#: on a split parcel that is not always the street the lot fronts on -
#: `program.Lot.frontage_m` is the first of these, and the second is carried
#: because a corner piece is a different site from an interior one at the same
#: area.
#:
#: `piece_area_m2` is the area the solver sizes a building on and `lot_area_m2`
#: is the parcel it belongs to; the two are equal on the great majority of rows
#: and are emphatically not the same column. The shares are what `use_gap`
#: divides the roll by, carried here so the gap needs no second join.
PIECE_FIELDS: tuple[str, ...] = (
    "lot_area_m2",
    "piece_area_m2",
    "pct_of_lot",
    "num_lot_zones",
    "zone_rank",
    "is_primary_zone",
    "primary_frontage_m",
    "primary_street_name",
    "primary_cote_rue_id",
    "secondary_frontage_m",
    "secondary_street_name",
    "secondary_cote_rue_id",
    "num_frontages",
    "lot_frontage_m",
    "frontage_buffer_m",
    "existing_footprint_m2",
    "area_share",
    "footprint_share",
    "footprint_share_basis",
    "min_pct_of_lot",
    "min_overlap_m2",
    "min_piece_area_m2",
)

#: The norms carried straight through from a parsed grid column, in the order
#: they are printed. Named once so the two assets cannot disagree about the
#: schema, and so a field added to `zoning_grid.GridColumn` reaches the table
#: by being added here rather than in four places.
NORM_FIELDS = (
    "floors_min",
    "floors_max",
    "height_min_m",
    "height_max_m",
    "min_lot_width_m",
    "implantation_mode",
    "site_coverage_min_pct",
    "site_coverage_max_pct",
    "density_min",
    "density_max",
    # Quebec City's only, and NULL on the other two cities' columns: a
    # dwelling count per hectare of lot, which is not the floor-area ratio
    # above. See `program.ZoneColumn.dwelling_density_min_per_ha`.
    "dwelling_density_min_per_ha",
    "dwelling_density_max_per_ha",
    "max_dwellings",
    "specific_use_area_max_m2",
    # Also Quebec City's: the tighter of the grid's retail and administration
    # floor ceilings, per building. See `quebec._commercial_floor_cap`.
    "commercial_floor_max_m2",
    "front_margin_min_m",
    "front_margin_max_m",
    "secondary_front_margin_min_m",
    "secondary_front_margin_max_m",
    "side_margin_min_m",
    "rear_margin_min_m",
    # Saguenay's only, and NULL on the other two cities' columns - see
    # `GridColumn.rear_on_street_margin_min_m`. It reaches
    # `silver.lot_buildable_setbacks` through here and is chosen between, per
    # lot, in `postgis.compute_lot_buildable_setbacks`.
    "rear_on_street_margin_min_m",
    "only_permitted_usages",
    "excluded_usages",
)

#: The *Categories d'usages* rows, as their own columns. A reader asking "which
#: lots allow housing" reads `permits_residential`; one asking "what else is
#: allowed beside it" reads these.
USAGE_CATEGORIES = ("habitation", "commerce", "industrie", "equipements")


@asset(
    key_prefix=key_prefix("zoning_grid_columns"),
    partitions_def=scrape_partitions,
    deps=[linked_documents, neighborhood_features],
    group_name=GROUP,
    kinds={"pypdf", "postgres", "parquet"},
    description=(
        "The zoning grids this partition fetched, read as tables rather than "
        "as prose: one row per column of each 'grille des usages et des "
        "normes', with the usages at its head, the building levels it is "
        "authorised on, and every norm of its CADRE BATI block - storeys, "
        "height, minimum lot width, implantation mode, site coverage, "
        "density, dwelling ceiling and the four margins - as columns. Emitted "
        "once per (document, feature_id, column), so a grid two zones share "
        "reaches both. A norm the grid prints as '-' is null and not zero. "
        "A Quebec City borough reads the city's specification workbook "
        f"({GRID_SLUG}) instead of PDFs, one row per zone, into the same "
        "columns - see hbu_dataplatform.cities.quebec_city.zoning.grid_columns for what is translated. "
        f"Writes silver/zoning_grid_columns/<YYYY-MM-DD>/<neighborhood>/"
        f"{ZONE_COLUMNS_FILE} and upserts silver.zoning_grid_columns on "
        "(scrape_date, neighborhood, source_table, feature_id, column_index)."
    ),
)
def zoning_grid_columns(
    context: AssetExecutionContext,
    store: ParquetStore,
    pdf_cache: PdfCache,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    if city_of(neighborhood) is City.QUEBEC:
        # The city publishes its norms as a workbook, snapshotted beside the
        # zoning layer by `neighborhood_features`; there is no PDF to read.
        grids = _read(
            store.partition_dir(
                neighborhood_features.key.path[-1], scrape_date, neighborhood
            ),
            f"{GRID_SLUG}.parquet",
        )
        if grids.empty:
            raise Failure(
                f"{neighborhood} {scrape_date}: {GRID_SLUG} holds no zone row, "
                "so there is no grid to read."
            )
        rows, failures = _quebec_grid_rows(
            grids, neighborhood=neighborhood, scrape_date=scrape_date
        )
        num_documents = int(grids[GRID_ZONE_COLUMN].nunique()) - len(failures)
    else:
        documents = _read(
            store.partition_dir(linked_documents.key.path[-1], scrape_date, neighborhood),
            DOCUMENTS_FILE,
        )
        # The corpus indexes whatever `DOCUMENT_SOURCES` links to; only the zone
        # table's links are grids, and the others would each cost a parse failure.
        grids = documents[documents["source_table"].isin(DOCUMENT_SOURCES)]
        if grids.empty:
            raise Failure(
                f"{neighborhood} {scrape_date}: linked_documents holds no document "
                f"from {', '.join(DOCUMENT_SOURCES)}, so there is no grid to read."
            )

        # The same on-disk cache `linked_documents` filled, keyed by URL: a grid is
        # reissued under a new file when its zone is amended rather than edited in
        # place, so this re-reads bytes rather than re-downloading them.
        fetcher = pdf_cache.fetcher()

        # Both cities publish a grid per zone and neither publishes the same
        # document. Montreal's is a borough template typeset by the borough;
        # Saguenay's is generated by its zoning service from the by-law, with
        # its own sections, its own vocabulary and its rows laid out up the
        # page rather than down it - so the reader is chosen by city and the
        # rest of this branch is shared. See `hbu_dataplatform.cities.saguenay.zoning`.
        parse = (
            parse_saguenay_grid_pdf
            if city_of(neighborhood) is City.SAGUENAY
            else parse_grid_pdf
        )

        rows: list[dict] = []
        failures: dict[str, str] = {}
        num_documents = 0
        for document in grids.itertuples(index=False):
            try:
                content, _ = fetcher.fetch(document.url)
                columns = parse(content, url=document.url)
            except (GridParseError, OSError) as exc:
                # One unreadable grid costs its zone, not the borough - the same
                # posture `linked_documents` takes towards a dead link.
                failures[document.url] = str(exc)
                context.log.warning("%s", exc)
                continue

            num_documents += 1
            feature_ids = _feature_ids(document) or [None]
            rows.extend(
                {
                    "doc_id": document.doc_id,
                    "source_table": document.source_table,
                    "neighborhood": neighborhood,
                    "scrape_date": scrape_date,
                    "url": document.url,
                    # What the map calls this zone, and what the join downstream is
                    # keyed on. `grid_zone` is what the page prints; the two agree
                    # except where a grid is shared, which is the case this grain
                    # exists for.
                    "feature_id": feature_id,
                    "grid_zone": column.zone,
                    **_column_row(column),
                }
                for feature_id in feature_ids
                for column in columns
                if not column.is_empty
            )

    if not rows:
        raise Failure(
            f"{neighborhood} {scrape_date}: none of the {len(grids)} linked "
            f"grid(s) could be read ({len(failures)} failed)."
        )

    frame = pd.DataFrame(rows)
    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, ZONE_COLUMNS_FILE))
    keyed = frame[frame["feature_id"].notna()]
    loaded = _publish(
        postgis,
        {"zoning_grid_columns": keyed},
        partition=neighborhood,
        scrape_date=scrape_date,
    )

    residential = int(frame["permits_residential"].sum())
    solvable = int(frame["solver_ready"].sum())
    context.log.info(
        "%s %s: %d of %d grid(s) read -> %d column(s) over %d zone(s), "
        "%d residential, %d solver-ready -> %s",
        neighborhood,
        scrape_date,
        num_documents,
        len(grids),
        len(frame),
        frame["feature_id"].nunique(),
        residential,
        solvable,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_documents": len(grids),
            "num_documents_parsed": num_documents,
            "num_documents_failed": len(failures),
            "num_columns": len(frame),
            "num_zones": int(frame["feature_id"].nunique()),
            "num_residential_columns": residential,
            "num_solver_ready_columns": solvable,
            # A residential column the solver cannot take is the symptom worth
            # seeing: it is a grid whose storey maximum did not survive the
            # parse, and every lot in that zone is unanswerable until it does.
            "num_residential_not_solver_ready": residential
            - int((frame["permits_residential"] & frame["solver_ready"]).sum()),
            "num_columns_with_notes": int((frame["parse_notes"] != "[]").sum()),
            "output_path": MetadataValue.path(str(path)),
            # A grid nothing on the map cites has no zone number to be keyed
            # on, so it reaches the tree and not the table. Rare, and worth
            # seeing when it is not.
            "num_columns_unkeyed": len(frame) - len(keyed),
            **published_metadata(loaded),
            **({"failures": MetadataValue.json(failures)} if failures else {}),
        }
    )


@asset(
    key_prefix=key_prefix("lot_zoning_envelopes"),
    partitions_def=tile_scrape_partitions,
    deps=[
        lot_zone_pieces,
        # A borough-axis upstream of a tile asset: the date maps across and
        # the borough is resolved at run time from the lots - see
        # `_BOROUGH_BRIDGE`.
        AssetDep(zoning_grid_columns, partition_mapping=_BOROUGH_BRIDGE),
    ],
    group_name=GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "Every zoning envelope in one tile, denormalised to the grain "
        "hbu_dataplatform.hbu.program reads: one row per (lot, zone, grid column), "
        "carrying the piece of the lot *that zone governs* - its own area and "
        "its own primary and secondary street frontage, from lot_zone_pieces "
        "- and every norm the column states: storeys and the levels the usage "
        "may occupy, minimum lot width, site coverage, density, dwelling "
        "ceiling, heights and margins. Joins lot_zone_pieces to "
        "zoning_grid_columns on the lot's own borough and the zone number, "
        "and nothing else - a tile spanning two boroughs reads both grids. "
        "piece_area_m2 is what the solver sizes a building on and lot_area_m2 "
        "is the parcel it belongs to; on a split lot they differ and the "
        "frontage can be a different street on each piece. Which pieces exist "
        "is lot_zone_pieces' decision and its three cutoffs travel on every "
        "row here. governs_residential marks the column "
        "select_residential_column picks for that piece's width. Writes "
        f"silver/lot_zoning_envelopes/<YYYY-MM-DD>/<tile>/"
        f"{LOT_ENVELOPES_FILE} and upserts silver.lot_zoning_envelopes on "
        "(scrape_date, cell_partition, lot_uid, feature_id, column_index)."
    ),
)
def lot_zoning_envelopes(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    tile, scrape_date = tile_partition_of(context)
    pieces = _read(
        store.partition_dir(lot_zone_pieces.key.path[-1], scrape_date, tile),
        LOT_ZONE_PIECES_FILE,
    )

    if pieces.empty:
        raise Failure(
            f"tile {tile} {scrape_date}: lot_zone_pieces holds no piece, so "
            "no envelope can be built. Materialize it for this partition "
            "first."
        )
    missing = [name for name in _OWNERSHIP_COLUMNS if name not in pieces.columns]
    if missing:
        raise Failure(
            f"tile {tile} {scrape_date}: lot_zone_pieces carries no "
            f"{', '.join(missing)} column - the file predates the tile axis. "
            "Re-materialize lot_zone_pieces for this partition."
        )
    # The geometry stays in `silver.lot_zone_pieces` and on the map. This table
    # is read by a solver scanning a tile, and a polygon on every row of it
    # is weight nothing here reads - the same reason `lot_buildable_setbacks`
    # publishes its envelope and the programs do not carry it.
    pieces = pieces.drop(columns=["geom"], errors="ignore")
    num_lots = int(pieces["lot_uid"].nunique())

    # The boroughs the tile's lots belong to, read off the lots themselves:
    # the grid is a borough's, and a piece is joined to its own lot's borough's
    # grid, so this is exactly the set of grid partitions to read. Off the
    # rows rather than out of Postgres so a database that is down still costs
    # the load and not the join - the posture `_publish` keeps.
    neighborhoods = tuple(sorted(pieces["neighborhood"].dropna().unique()))
    columns = _read_grid_columns(store, scrape_date, neighborhoods)

    # An inner join on purpose: a zone that published no readable grid has no
    # envelope to state, and a row of nulls would be one to solve. On the
    # borough as well as the zone number, because Montreal restarts its zone
    # numbers in every borough and a tile can hold two of them.
    envelopes = pieces.merge(
        columns,
        on=["neighborhood", "source_table", "feature_id"],
        how="inner",
        suffixes=("", "_grid"),
    )
    if envelopes.empty:
        # Two different things look alike here, and only one is a fault. A
        # join that is broken - a renamed column, a slug that no longer
        # matches - breaks for every zone, so the pieces and the grid do not
        # even share a zoning *layer*. A cell that simply holds a handful of
        # zones with no readable grid - Saguenay has dozens city-wide, and a
        # cell on the edge of the cadastre can hold one piece - shares the
        # layer and misses the zones. On a borough the two could not be told
        # apart and both failed; on a cell the second is an empty partition.
        shared_layers = set(pieces["source_table"]) & set(columns["source_table"])
        if not shared_layers:
            raise Failure(
                f"tile {tile} {scrape_date}: the {len(pieces)} lot x zone "
                f"piece(s) and the {len(columns)} grid column(s) of "
                f"{', '.join(neighborhoods)} share no zoning layer "
                "(source_table), so the join itself is broken rather than a "
                "zone missing its grid. The map's feature id and the grid's "
                "are the same column (NUMERO_COMPLET) and should match."
            )
        context.log.warning(
            "tile %s %s: none of its %d lot x zone piece(s) is in a zone "
            "with a readable grid; publishing an empty partition",
            tile,
            scrape_date,
            len(pieces),
        )

    # The piece's frontage, not its lot's - which is the whole reason
    # `lot_zone_pieces` re-ranks the street edges inside each piece. A
    # commercial strip and the housing behind it are held to two different
    # *Largeur du terrain min* tests because they face two different streets.
    envelopes["meets_min_lot_width"] = envelopes["min_lot_width_m"].isna() | (
        envelopes["min_lot_width_m"] <= envelopes["primary_frontage_m"].fillna(0.0)
    )
    for family, flags in _governing(envelopes).items():
        envelopes[f"governs_{family}"] = flags

    frame = envelopes[list(_OUTPUT_COLUMNS)].sort_values(
        ["lot_uid", "feature_id", "column_index"], kind="stable"
    )
    # One row per (lot, zone, column) - the grain this asset upserts at, and
    # the grain the solver reads. Postgres enforces it and would have hidden
    # the problem: `ON CONFLICT` collapses a repeated key, so a duplicate would
    # be invisible in the table and still be in the parquet, which is what
    # `hbu_candidates` actually reads. The way one arrives is a zone number
    # cited twice by one grid's `feature_ids`, and the cost is a second CP-SAT
    # model on the same envelope and a lot counted twice in whatever sums it.
    keyed = ["lot_uid", "feature_id", "column_index"]
    num_duplicates = int(frame.duplicated(subset=keyed).sum())
    if num_duplicates:
        context.log.warning(
            "%s %s: %d duplicate (lot, zone, column) row(s) - the same "
            "envelope reached by more than one grid citing the zone; keeping "
            "the first of each",
            tile,
            scrape_date,
            num_duplicates,
        )
        frame = frame.drop_duplicates(subset=keyed, keep="first")

    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, tile)
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_ENVELOPES_FILE))
    loaded = _publish(
        postgis,
        {"lot_zoning_envelopes": frame},
        partition=tile,
        scrape_date=scrape_date,
    )

    solvable = frame[frame["governs_residential"] & frame["solver_ready"]]
    with_frontage = int(frame["primary_frontage_m"].notna().sum())
    split = frame[frame["num_lot_zones"] > 1]
    context.log.info(
        "%s %s: %d of %d lot(s) covered by a zone -> %d envelope row(s) over "
        "%d piece(s), %d of them on lots split between two or more zones; "
        "%d solvable on %d lot(s), %d row(s) with a measured frontage -> %s",
        tile,
        scrape_date,
        int(frame["lot_uid"].nunique()),
        num_lots,
        len(frame),
        int(frame.groupby(["lot_uid", "feature_id"], sort=False).ngroups),
        len(split),
        len(solvable),
        int(solvable["lot_uid"].nunique()),
        with_frontage,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "tile": tile,
            # Whose grids were read - one for most tiles, two where a cut cell
            # straddles a borough line.
            "neighborhoods": ", ".join(neighborhoods),
            "num_lots": num_lots,
            "num_lots_zoned": int(frame["lot_uid"].nunique()),
            # A lot no grid reaches cannot be solved at all, and the two
            # reasons - the cadastre stretching past the feature scrape, or a
            # zone whose PDF failed to parse - both show up here first.
            "num_lots_unzoned": num_lots - int(frame["lot_uid"].nunique()),
            "num_envelopes": len(frame),
            # The piece count, beside the row count that has always been here.
            # A row is a *column* of a grid and several describe one piece of
            # ground; the piece is what gets solved, so it is the number to
            # read against `lot_development_programs`.
            "num_pieces": int(
                frame.groupby(["lot_uid", "feature_id"], sort=False).ngroups
            ),
            "num_envelopes_on_split_lots": len(split),
            "num_split_lots": int(split["lot_uid"].nunique()),
            "num_residential_envelopes": int(frame["permits_residential"].sum()),
            "num_commercial_envelopes": int(frame["permits_commercial"].sum()),
            "num_industrial_envelopes": int(frame["permits_industrial"].sum()),
            "num_governing_envelopes": int(frame["governs_residential"].sum()),
            "num_governing_commercial": int(frame["governs_commercial"].sum()),
            "num_governing_industrial": int(frame["governs_industrial"].sum()),
            "num_solvable_envelopes": len(solvable),
            "num_lots_solvable": int(solvable["lot_uid"].nunique()),
            "num_rows_with_frontage": with_frontage,
            # An envelope with no frontage cannot be tested against *Largeur du
            # terrain*; `meets_min_lot_width` reads a missing frontage as 0, so
            # a column with a width minimum is excluded rather than assumed.
            "num_rows_without_frontage": len(frame) - with_frontage,
            "num_corner_pieces": int((frame["num_frontages"] > 1).sum()),
            # The piece, then the parcel. The two medians differ by exactly the
            # ground the split lots hand to their secondary zones, which is the
            # quantity this whole grain exists to stop mispricing.
            "median_piece_area_m2": round(
                float(frame["piece_area_m2"].median()), 1
            ),
            "median_lot_area_m2": round(float(frame["lot_area_m2"].median()), 1),
            # What the row count means depends entirely on these. Read off the
            # rows rather than off this asset's config, because the cutoffs are
            # `lot_zone_pieces`' now - see the module docstring.
            "min_pct_of_lot": _threshold(frame, "min_pct_of_lot"),
            "min_overlap_m2": _threshold(frame, "min_overlap_m2"),
            "min_piece_area_m2": _threshold(frame, "min_piece_area_m2"),
            "num_duplicate_rows_dropped": num_duplicates,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


def _quebec_grid_rows(
    grids: pd.DataFrame, *, neighborhood: str, scrape_date: str
) -> tuple[list[dict], dict[str, str]]:
    """The workbook rows of one borough, as the rows of this table.

    One zone is one grid, keyed by its own code - the same code the zoning
    layer carries in `IGDS_TEXT_STRING`, which is what `rag.features` has as
    `feature_id` and what `lot_zoning_envelopes` joins on. `doc_id` and
    `url` are null: the workbook is one document for the city, not one per
    zone, and nothing in the corpus cites it.
    """
    rows: list[dict] = []
    failures: dict[str, str] = {}
    for record in grids.to_dict("records"):
        zone = str(record.get(GRID_ZONE_COLUMN) or "").strip()
        if not zone:
            continue
        try:
            columns = grid_columns(record)
        except (GridParseError, ProgramError, ValueError, TypeError) as exc:
            failures[zone] = str(exc)
            continue
        rows.extend(
            {
                "doc_id": None,
                "source_table": ZONING_SLUG,
                "neighborhood": neighborhood,
                "scrape_date": scrape_date,
                "url": None,
                "feature_id": zone,
                "grid_zone": column.zone,
                **_column_row(column),
            }
            for column in columns
            if not column.is_empty
        )
    return rows, failures


def _threshold(frame: pd.DataFrame, column: str) -> float:
    """One of `lot_zone_pieces`' cutoffs, read back off the rows it wrote.

    The same value on every row by construction, so the first is the answer -
    but taken as a max rather than as `iloc[0]`, so a partition that somehow
    mixed two settings reports the looser one rather than whichever row the
    join happened to place first. Absent on a parquet written before the
    pieces existed, which is a `nan` rather than a failed materialization.
    """
    if column not in frame.columns:
        return float("nan")
    return float(pd.to_numeric(frame[column], errors="coerce").max())


#: The table, in reading order: the piece, then what it faces, then the zone,
#: then what the zone allows. Declared rather than inherited from the merge so
#: the column order is a decision and the surrogate keys of the join do not
#: leak into it.
_OUTPUT_COLUMNS = (
    "lot_uid",
    "lot_number",
    # The lot's own borough and its cell address, off the piece - see
    # `_OWNERSHIP_COLUMNS`.
    *_OWNERSHIP_COLUMNS,
    "scrape_date",
    # The parcel, then the piece of it this row is about. Both, always: a
    # reader holding one row has to be able to tell a whole lot from a tenth
    # of one, and `piece_area_m2` alone cannot say which it is.
    "lot_area_m2",
    "piece_area_m2",
    "num_lot_zones",
    "zone_rank",
    "is_primary_zone",
    # The piece's own street, re-ranked inside it - not the lot's.
    "primary_frontage_m",
    "primary_street_name",
    "primary_cote_rue_id",
    "secondary_frontage_m",
    "secondary_street_name",
    "secondary_cote_rue_id",
    "num_frontages",
    "lot_frontage_m",
    "frontage_buffer_m",
    # What already stands on this piece, and the two shares that divide the
    # roll between a lot's pieces. `use_gap` is the reader; see
    # `postgis.compute_lot_zone_pieces` for why there are two.
    "existing_footprint_m2",
    "area_share",
    "footprint_share",
    "footprint_share_basis",
    "feature_id",
    "source_table",
    "pct_of_lot",
    "min_pct_of_lot",
    "min_overlap_m2",
    "min_piece_area_m2",
    "doc_id",
    "url",
    "grid_zone",
    "column_index",
    "usages",
    *(f"usage_{category}" for category in USAGE_CATEGORIES),
    "permits_residential",
    "permits_commercial",
    "permits_industrial",
    "levels",
    "residential_floors",
    *NORM_FIELDS,
    "meets_min_lot_width",
    "governs_residential",
    "governs_commercial",
    "governs_industrial",
    "solver_ready",
    "solver_error",
    "parse_notes",
)


def _column_row(column: GridColumn) -> dict:
    """One parsed grid column, flattened to the columns of the table.

    `solver_ready` is decided by building the `ZoneColumn` and seeing whether
    it holds, rather than by re-checking the fields here: the solver's own
    constructor is what defines the answer, and a second copy of that rule
    would be the copy that goes stale.
    """
    try:
        zone_column = column.to_zone_column()
        solver_error = None
    except (GridParseError, ProgramError) as exc:
        zone_column = None
        solver_error = str(exc)

    return {
        "column_index": column.column_index,
        "usages": json.dumps(list(column.usages), ensure_ascii=False),
        **{
            f"usage_{category}": column.usages_by_category.get(category)
            for category in USAGE_CATEGORIES
        },
        "permits_residential": column.permits_residential,
        # The two families beside Habitation, read off the same usage codes
        # with the same anchored matchers the solver itself uses - so a
        # column's flags here and its `ZoneColumn.permits_*` downstream
        # cannot disagree.
        "permits_commercial": any(
            is_commercial_usage(str(usage)) for usage in column.usages
        ),
        "permits_industrial": any(
            is_industrial_usage(str(usage)) for usage in column.usages
        ),
        "levels": json.dumps(
            sorted(str(level) for level in column.levels), ensure_ascii=False
        ),
        # How many storeys this column's usage may actually occupy, which is
        # the storey maximum narrowed by the level rows - the number the
        # envelope is built from, and not one the grid prints anywhere.
        "residential_floors": (
            permitted_floors(column.levels, column.floors_max)
            if column.floors_max is not None
            else None
        ),
        **{name: getattr(column, name) for name in NORM_FIELDS},
        # Stated once for the zone and repeated on every column of its grid -
        # the heritage sector, the PIIA sector, the PAE flag and the cited
        # articles. Carried here so a screen downstream can read "is this
        # zone a secteur d'interet patrimonial" off the zone it already joins
        # to, rather than re-parsing the PDF to find out.
        **{name: getattr(column, name) for name in ZONE_FIELDS},
        "solver_ready": zone_column is not None,
        "solver_error": solver_error,
        "parse_notes": json.dumps(list(column.notes), ensure_ascii=False),
    }


def _governing(envelopes: pd.DataFrame) -> dict[str, pd.Series]:
    """Which row of each (lot, zone) governs that piece, per usage family.

    `select_governing_column` is the rule, and it is called rather than
    reimplemented: a grid authorises a family in more than one column and
    distinguishes them by *Largeur du terrain min*, so the column that governs
    a parcel is the widest minimum it still satisfies - and, among the columns
    a zone prints at that same width, the one permitting the most dwellings.
    Decided once per family, because a zone's Habitation rule and its Commerce
    rule are two rules and a piece of ground is governed by each. The choice is
    made within one (lot, zone) at a time, and that grouping now means
    something stronger than it used to: two zones covering one lot are two
    *sites*, each with its own area and its own street, so each picks its own
    governing column against its own frontage. It used to mean the opposite -
    the two were competing readings of one site, and `pct_of_lot` decided which
    to keep - and the piece with the narrower street was answered under the
    other piece's width. See `hbu_dataplatform.zoning.zone_piece_assets`.

    False on every row of a piece with no measured frontage and a width
    minimum - the missing frontage reads as 0, which excludes the column rather
    than assuming it qualifies.
    """
    families: dict[str, object] = {
        "residential": lambda column: column.permits_residential,
        "commercial": lambda column: column.permits_commercial,
        "industrial": lambda column: column.permits_industrial,
    }
    governs = {
        family: pd.Series(False, index=envelopes.index) for family in families
    }
    ready = envelopes[envelopes["solver_ready"]]
    for _, group in ready.groupby(["lot_uid", "feature_id"], sort=False):
        frontage_m = float(group["primary_frontage_m"].fillna(0.0).iloc[0])
        by_index = {
            index: _as_zone_column(row)
            for index, row in group.iterrows()
        }
        for family, permits in families.items():
            chosen = select_governing_column(
                list(by_index.values()), frontage_m, permits=permits
            )
            if chosen is None:
                continue
            # Identity, not equality: two columns of one grid can state
            # identical norms (a zone printing the same envelope for H and
            # for C), and matching on value would mark both.
            for index, candidate in by_index.items():
                if candidate is chosen:
                    governs[family][index] = True
                    break
    return governs


def _as_zone_column(row: pd.Series) -> ZoneColumn:
    """Rebuild the solver's input from a row of the table.

    Only the fields `select_governing_column` reads are filled in - the
    usages, which its `permits` predicate tests, the width minimum, and the
    printed *Nombre de logements maximal*, which is half of the ceiling that
    separates two columns written for one width. That is why this is private
    to `_governing` and not the table's public inverse. A row is turned back
    into a full `ZoneColumn` by whoever solves it, from the columns this asset
    wrote.

    `floors_max` is the one field that has to be *derived* rather than read:
    it is required on a `ZoneColumn` and a Quebec City grid leaves it blank on
    nine tenths of La Cite-Limoilou, stating a height instead. Through
    `storey_ceiling_from_height` so this agrees with the two other places that
    rebuild a column - a disagreement here would pick a different governing
    column than the one that gets solved.
    """
    floors_max = (
        None if pd.isna(row["floors_max"]) else int(row["floors_max"])
    )
    if floors_max is None:
        height_max_m = (
            None if pd.isna(row["height_max_m"]) else float(row["height_max_m"])
        )
        floors_max = storey_ceiling_from_height(height_max_m)
    return ZoneColumn(
        usages=tuple(json.loads(row["usages"])),
        floors_max=floors_max,
        min_lot_width_m=(
            None if pd.isna(row["min_lot_width_m"]) else float(row["min_lot_width_m"])
        ),
        max_dwellings=(
            None if pd.isna(row["max_dwellings"]) else int(row["max_dwellings"])
        ),
        zone=row["feature_id"],
    )


def _feature_ids(document) -> list[str]:
    """The map features whose link is this document, as `rag_assets` wrote them.

    Distinct, in the order the document lists them. A zone number repeated in
    the array would emit that zone's columns twice, and the duplicate survives
    all the way to the parquet the solver reads - `silver.zoning_grid_columns`
    is keyed on (source_table, feature_id, column_index) and would collapse it
    in the table while leaving it in the file.
    """
    try:
        ids = json.loads(document.feature_ids or "[]")
    except (TypeError, ValueError):
        return []
    return list(
        dict.fromkeys(str(value) for value in ids if value is not None)
    )


def _publish(
    postgis: PostgisResource,
    datasets: dict[str, pd.DataFrame],
    *,
    partition: str,
    scrape_date: str,
) -> dict[str, dict[str, int]]:
    """Upsert what was just written, naming the file already on disk if not.

    After the parquet, deliberately: parsing a borough's grids is minutes of
    pypdf over documents a later run may no longer be able to fetch, and a
    database that is down should cost the load rather than the parse.

    ``partition`` is the borough for `zoning_grid_columns` and the tile for
    `lot_zoning_envelopes` - whichever axis the table is on.
    """
    try:
        return publish(
            postgis.connect,
            datasets,
            partition=partition,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{', '.join(datasets)} for {partition} {scrape_date} were "
            f"written to the tree but could not be published: {exc}"
        ) from exc


def _read_grid_columns(
    store: ParquetStore, scrape_date: str, neighborhoods: Sequence[str]
) -> pd.DataFrame:
    """Every borough's `zoning_grid_columns` partition, as one frame.

    One parquet per borough, because that asset is on the borough axis, and
    the `neighborhood` column is set from the partition read rather than
    trusted off the file: it is what the join back to the pieces is keyed on,
    and a stale file stamped with another borough would join its grids to the
    wrong lots. A tile whose lots belong to no borough at all has nothing to
    read, which is a fact about `neighborhood_cadastre` and is raised as one.
    """
    if not neighborhoods:
        raise Failure(
            f"{scrape_date}: the pieces name no borough, so there is no grid "
            "to read them against - rag.lots carries no neighborhood for "
            "these lots."
        )
    frames = []
    for neighborhood in neighborhoods:
        frame = _read(
            store.partition_dir(
                zoning_grid_columns.key.path[-1], scrape_date, neighborhood
            ),
            ZONE_COLUMNS_FILE,
        )
        frames.append(frame.assign(neighborhood=neighborhood))
    return pd.concat(frames, ignore_index=True)


def _read(partition_dir: str, name: str) -> pd.DataFrame:
    path = join(partition_dir, name)
    if not filesystem(path).exists(path):
        raise Failure(f"{path} is missing; materialize its upstream asset first.")
    return pd.read_parquet(path, storage_options=storage_options(path))
