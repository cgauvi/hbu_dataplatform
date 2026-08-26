"""The zoning envelope of every lot, at the grain the solver reads.

`urban_rag.program` answers one question - what unit mix is the envelope of
this parcel worth - and needs three things to ask it: a `ZoneColumn` off the
zone's *grille des usages et des normes*, the lot's area, and its frontage.
Each of the three already exists somewhere in this platform, and none of them
next to the other two. These two assets close that gap.

**`zoning_grid_columns`** parses the PDFs `linked_documents` already fetched.
The corpus reads those files as prose; this reads them as the tables they are,
one row per column of each grid, with the norms as columns of the row - see
`urban_rag.zoning_grid` for why that is a question about x-coordinates. A grid
is published per zone but linked per feature, and two zones can share one
file, so the rows are emitted per ``(document, feature_id, column)``: the join
key downstream is the zone number the map carries, not the one printed on the
page, and joining on the printed one would drop the second zone of a shared
grid.

**`lot_zoning_envelopes`** is the denormalised table itself: one row per
``(lot, grid column)``, carrying the lot's area, its primary and secondary
frontage, and every norm that column states. It is a join of three things this
platform already computes and nothing more - `building_lot_intersections`'s
lot x feature side says which zone covers which parcel, `lot_frontage` says
how much street each parcel faces, and the grid columns above say what may be
built. A row of it is one call to `solve_program`, with `governs_residential`
marking the row `select_residential_column` would pick.

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
"""

import json

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

from urban_rag.building_lots_assets import (
    LOT_FEATURES_FILE,
    building_lot_intersections,
)
from urban_rag.frames import write_frame
from urban_rag.frontage_assets import LOT_FRONTAGE_FILE, lot_frontage
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.program import (
    ProgramError,
    ZoneColumn,
    permitted_floors,
    select_residential_column,
)
from urban_rag.rag.documents import DOCUMENT_SOURCES
from urban_rag.rag_assets import DOCUMENTS_FILE, linked_documents
from urban_rag.resources import ParquetStore, PdfCache
from urban_rag.storage import clear_parquet, filesystem, join, storage_options
from urban_rag.zoning_grid import GridColumn, GridParseError, parse_grid_pdf

GROUP = "silver_zoning"

#: One file per partition, under
#: `silver/zoning_grid_columns/<YYYY-MM-DD>/<neighborhood>/`.
ZONE_COLUMNS_FILE = "zone_columns.parquet"

#: One file per partition, under
#: `silver/lot_zoning_envelopes/<YYYY-MM-DD>/<neighborhood>/`.
LOT_ENVELOPES_FILE = "lot_zoning_envelopes.parquet"

#: The frontage a lot "has": `lot_frontage` ranks a lot's street edges longest
#: first, so rank 1 is the street it fronts on and rank 2 is the other side of
#: a corner lot. `program.Lot.frontage_m` is the first of these, and the second
#: is carried because a corner parcel is a different site from an interior one
#: at the same area - which is the judgement the row exists to support.
FRONTAGE_RANKS = {1: "primary", 2: "secondary"}

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
    "max_dwellings",
    "specific_use_area_max_m2",
    "front_margin_min_m",
    "front_margin_max_m",
    "secondary_front_margin_min_m",
    "secondary_front_margin_max_m",
    "side_margin_min_m",
    "rear_margin_min_m",
    "only_permitted_usages",
    "excluded_usages",
)

#: The *Categories d'usages* rows, as their own columns. A reader asking "which
#: lots allow housing" reads `permits_residential`; one asking "what else is
#: allowed beside it" reads these.
USAGE_CATEGORIES = ("habitation", "commerce", "industrie", "equipements")


class EnvelopeConfig(Config):
    """How much of a lot a zone has to cover to be one of its envelopes.

    Zone polygons and cadastral polygons are drawn by two publishers and agree
    only approximately, so a parcel on a zone boundary picks up a sliver of the
    neighbouring zone - a fraction of a percent of its area, and not a set of
    rules anybody would build under. Config rather than a constant because it
    is a judgement about how far apart the two publishers' lines are, not a
    property of the data.

    The default keeps every overlap: `pct_of_lot` is on every row, so a table
    written at 0 can be read back at any threshold, and one written at 5 cannot
    be read back at 0.
    """

    min_pct_of_lot: float = Field(
        default=0.0,
        ge=0.0,
        le=100.0,
        description=(
            "A zone gives a lot an envelope row when it covers at least this "
            "percentage of it. 0 keeps every overlap the join found."
        ),
    )


@asset(
    key_prefix=key_prefix("zoning_grid_columns"),
    partitions_def=scrape_partitions,
    deps=[linked_documents],
    group_name=GROUP,
    kinds={"pypdf", "parquet"},
    description=(
        "The zoning grids this partition fetched, read as tables rather than "
        "as prose: one row per column of each 'grille des usages et des "
        "normes', with the usages at its head, the building levels it is "
        "authorised on, and every norm of its CADRE BATI block - storeys, "
        "height, minimum lot width, implantation mode, site coverage, "
        "density, dwelling ceiling and the four margins - as columns. Emitted "
        "once per (document, feature_id, column), so a grid two zones share "
        "reaches both. A norm the grid prints as '-' is null and not zero. "
        f"Writes silver/zoning_grid_columns/<YYYY-MM-DD>/<neighborhood>/"
        f"{ZONE_COLUMNS_FILE}."
    ),
)
def zoning_grid_columns(
    context: AssetExecutionContext,
    store: ParquetStore,
    pdf_cache: PdfCache,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
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

    rows: list[dict] = []
    failures: dict[str, str] = {}
    num_documents = 0
    for document in grids.itertuples(index=False):
        try:
            content, _ = fetcher.fetch(document.url)
            columns = parse_grid_pdf(content, url=document.url)
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
            **({"failures": MetadataValue.json(failures)} if failures else {}),
        }
    )


@asset(
    key_prefix=key_prefix("lot_zoning_envelopes"),
    partitions_def=scrape_partitions,
    deps=[building_lot_intersections, lot_frontage, zoning_grid_columns],
    group_name=GROUP,
    kinds={"parquet"},
    description=(
        "Every lot's zoning envelope, denormalised to the grain "
        "urban_rag.program reads: one row per (lot, grid column), carrying the "
        "lot's area, its primary and secondary street frontage, and every norm "
        "the column states - storeys and the levels the usage may occupy, "
        "minimum lot width, site coverage, density, dwelling ceiling, heights "
        "and margins. Joins building_lot_intersections' lot x feature side to "
        "zoning_grid_columns on the zone number, and lot_frontage on the lot. "
        "governs_residential marks the column select_residential_column picks "
        "for that lot's width. Writes silver/lot_zoning_envelopes/"
        f"<YYYY-MM-DD>/<neighborhood>/{LOT_ENVELOPES_FILE}."
    ),
)
def lot_zoning_envelopes(
    context: AssetExecutionContext,
    config: EnvelopeConfig,
    store: ParquetStore,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    lot_features = _read(
        store.partition_dir(
            building_lot_intersections.key.path[-1], scrape_date, neighborhood
        ),
        LOT_FEATURES_FILE,
    )
    frontage = _read(
        store.partition_dir(lot_frontage.key.path[-1], scrape_date, neighborhood),
        LOT_FRONTAGE_FILE,
    )
    columns = _read(
        store.partition_dir(
            zoning_grid_columns.key.path[-1], scrape_date, neighborhood
        ),
        ZONE_COLUMNS_FILE,
    )

    num_lots = int(lot_features["lot_uid"].nunique())
    covered = lot_features[
        lot_features["source_table"].isin(DOCUMENT_SOURCES)
        & (lot_features["pct_of_lot"] >= config.min_pct_of_lot)
    ]
    if covered.empty:
        raise Failure(
            f"{neighborhood} {scrape_date}: no lot is covered by a "
            f"{'/'.join(DOCUMENT_SOURCES)} feature at or above "
            f"{config.min_pct_of_lot}% of its area, so no envelope can be "
            "built. Check that building_lot_intersections loaded the zoning "
            "layer for this partition."
        )
    if "lot_number" not in covered.columns:
        # Added to `postgis._LOT_FEATURE_COLUMNS` after some partitions were
        # already written. The lot number is a label, not a key this asset
        # joins on, so an older file costs the label rather than the run.
        context.log.warning(
            "%s: carries no lot_number - re-materialize "
            "building_lot_intersections for this partition to get it.",
            LOT_FEATURES_FILE,
        )
        covered = covered.assign(lot_number=None)

    # An inner join on purpose: a lot whose zone published no readable grid
    # has no envelope to state, and a row of nulls would be one to solve.
    envelopes = covered.merge(
        columns,
        on=["source_table", "feature_id"],
        how="inner",
        suffixes=("", "_grid"),
    )
    if envelopes.empty:
        raise Failure(
            f"{neighborhood} {scrape_date}: the {len(covered)} lot x zone "
            f"pair(s) and the {len(columns)} grid column(s) share no zone "
            "number. The map's feature id and the grid's are the same column "
            "(NUMERO_COMPLET) and should match."
        )

    envelopes = envelopes.merge(_frontage_by_lot(frontage), on="lot_uid", how="left")
    envelopes["meets_min_lot_width"] = envelopes["min_lot_width_m"].isna() | (
        envelopes["min_lot_width_m"] <= envelopes["primary_frontage_m"].fillna(0.0)
    )
    envelopes["governs_residential"] = _governing(envelopes)

    frame = envelopes[list(_OUTPUT_COLUMNS)].sort_values(
        ["lot_uid", "feature_id", "column_index"], kind="stable"
    )

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_ENVELOPES_FILE))

    solvable = frame[frame["governs_residential"] & frame["solver_ready"]]
    with_frontage = int(frame["primary_frontage_m"].notna().sum())
    context.log.info(
        "%s %s: %d of %d lot(s) covered by a zone -> %d envelope row(s), "
        "%d solvable on %d lot(s), %d row(s) with a measured frontage -> %s",
        neighborhood,
        scrape_date,
        int(frame["lot_uid"].nunique()),
        num_lots,
        len(frame),
        len(solvable),
        int(solvable["lot_uid"].nunique()),
        with_frontage,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_lots": num_lots,
            "num_lots_zoned": int(frame["lot_uid"].nunique()),
            # A lot no grid reaches cannot be solved at all, and the two
            # reasons - the cadastre stretching past the feature scrape, or a
            # zone whose PDF failed to parse - both show up here first.
            "num_lots_unzoned": num_lots - int(frame["lot_uid"].nunique()),
            "num_envelopes": len(frame),
            "num_residential_envelopes": int(frame["permits_residential"].sum()),
            "num_governing_envelopes": int(frame["governs_residential"].sum()),
            "num_solvable_envelopes": len(solvable),
            "num_lots_solvable": int(solvable["lot_uid"].nunique()),
            "num_rows_with_frontage": with_frontage,
            # An envelope with no frontage cannot be tested against *Largeur du
            # terrain*; `meets_min_lot_width` reads a missing frontage as 0, so
            # a column with a width minimum is excluded rather than assumed.
            "num_rows_without_frontage": len(frame) - with_frontage,
            "num_corner_lots": int((frame["num_frontages"] > 1).sum()),
            "median_lot_area_m2": round(float(frame["lot_area_m2"].median()), 1),
            # What the row count means depends entirely on this, so it travels
            # with it rather than only in the run's config.
            "min_pct_of_lot": config.min_pct_of_lot,
            "output_path": MetadataValue.path(str(path)),
        }
    )


#: The table, in reading order: the lot, then what it faces, then the zone,
#: then what the zone allows. Declared rather than inherited from the merge so
#: the column order is a decision and the surrogate keys of the join do not
#: leak into it.
_OUTPUT_COLUMNS = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "lot_area_m2",
    "primary_frontage_m",
    "primary_street_name",
    "primary_cote_rue_id",
    "secondary_frontage_m",
    "secondary_street_name",
    "secondary_cote_rue_id",
    "num_frontages",
    "frontage_buffer_m",
    "feature_id",
    "source_table",
    "pct_of_lot",
    "overlap_area_m2",
    "doc_id",
    "url",
    "grid_zone",
    "column_index",
    "usages",
    *(f"usage_{category}" for category in USAGE_CATEGORIES),
    "permits_residential",
    "levels",
    "residential_floors",
    *NORM_FIELDS,
    "meets_min_lot_width",
    "governs_residential",
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
        "solver_ready": zone_column is not None,
        "solver_error": solver_error,
        "parse_notes": json.dumps(list(column.notes), ensure_ascii=False),
    }


def _frontage_by_lot(frontage: pd.DataFrame) -> pd.DataFrame:
    """One row per lot: its primary and secondary street edge.

    `lot_frontage` holds one row per (lot, street side) and ranks them longest
    first, which is the shape a corner lot needs and the wrong shape for a
    table whose grain is the lot. Ranks beyond the second are dropped from the
    columns but still counted in `num_frontages`, so a lot facing three streets
    is visible as one without carrying a third pair of columns every other row
    would leave empty.
    """
    per_lot = frontage.groupby("lot_uid", sort=False).agg(
        num_frontages=("frontage_m", "size"),
        frontage_buffer_m=("buffer_m", "max"),
    )
    wide = per_lot
    for rank, prefix in FRONTAGE_RANKS.items():
        ranked = (
            frontage[frontage["frontage_rank"] == rank]
            .drop_duplicates("lot_uid")
            .set_index("lot_uid")[["frontage_m", "street_name", "cote_rue_id"]]
            .rename(
                columns={
                    "frontage_m": f"{prefix}_frontage_m",
                    "street_name": f"{prefix}_street_name",
                    "cote_rue_id": f"{prefix}_cote_rue_id",
                }
            )
        )
        wide = wide.join(ranked, how="left")
    return wide.reset_index()


def _governing(envelopes: pd.DataFrame) -> pd.Series:
    """Which row of each (lot, zone) is the column that governs the lot.

    `select_residential_column` is the rule, and it is called rather than
    reimplemented: a grid authorises dwellings in more than one column and
    distinguishes them by *Largeur du terrain min*, so the column that governs
    a parcel is the widest minimum it still satisfies. The choice is made
    within one zone at a time, because two zones covering the same lot are two
    separate readings of it and `pct_of_lot` is what says which is the real
    one.

    False on every row of a lot with no measured frontage and a width minimum -
    the missing frontage reads as 0, which excludes the column rather than
    assuming it qualifies.
    """
    governs = pd.Series(False, index=envelopes.index)
    residential = envelopes[envelopes["permits_residential"] & envelopes["solver_ready"]]
    for _, group in residential.groupby(["lot_uid", "feature_id"], sort=False):
        frontage_m = float(group["primary_frontage_m"].fillna(0.0).iloc[0])
        by_index = {
            index: _as_zone_column(row)
            for index, row in group.iterrows()
        }
        chosen = select_residential_column(list(by_index.values()), frontage_m)
        if chosen is None:
            continue
        # Identity, not equality: two columns of one grid can state identical
        # norms (a zone printing the same envelope for H and for C), and
        # matching on value would mark both.
        for index, candidate in by_index.items():
            if candidate is chosen:
                governs[index] = True
                break
    return governs


def _as_zone_column(row: pd.Series) -> ZoneColumn:
    """Rebuild the solver's input from a row of the table.

    Only the two fields `select_residential_column` reads are filled in, which
    is why this is private to `_governing` and not the table's public inverse.
    A row is turned back into a full `ZoneColumn` by whoever solves it, from
    the columns this asset wrote.
    """
    return ZoneColumn(
        usages=tuple(json.loads(row["usages"])),
        floors_max=int(row["floors_max"]),
        min_lot_width_m=(
            None if pd.isna(row["min_lot_width_m"]) else float(row["min_lot_width_m"])
        ),
        zone=row["feature_id"],
    )


def _feature_ids(document) -> list[str]:
    """The map features whose link is this document, as `rag_assets` wrote them."""
    try:
        ids = json.loads(document.feature_ids or "[]")
    except (TypeError, ValueError):
        return []
    return [str(value) for value in ids if value is not None]


def _partition(context: AssetExecutionContext) -> tuple[str, str]:
    dimensions = context.partition_key.keys_by_dimension
    return dimensions["neighborhood"], dimensions["date"][:10]


def _read(partition_dir: str, name: str) -> pd.DataFrame:
    path = join(partition_dir, name)
    if not filesystem(path).exists(path):
        raise Failure(f"{path} is missing; materialize its upstream asset first.")
    return pd.read_parquet(path, storage_options=storage_options(path))
