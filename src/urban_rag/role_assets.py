"""What every property in the borough is assessed at, and what that makes a lot worth.

Three assets over one publication - Quebec's *rôle d'évaluation foncière*, the
province-wide property assessment roll the MAMH republishes as open data.

`property_assessment_roll` snapshots it. The archive holds five layers and this
keeps two: `rol_unite_p`, one point per *unité d'évaluation*, and
`b05v_unite_evaln`, the characteristics table those points are described by.
The other three - addresses, cadastral lot numbers, fiscal breakdowns - are
one-to-many against the unit and are not read; `UNREAD_LAYERS` names them so a
run reports what it left rather than leaving eleven million rows unaccounted
for.

`assessment_units` puts the two back together on `id_provinc`, which is what
the publisher splits them by: the geometry is in one file and everything true
of the property is in the other, and neither is usable alone.

`lot_assessed_values` carries that onto the cadastre. It is the join this whole
chain exists for - Infolot draws the lot, the roll values the property, and
nothing published connects them - and it is spatial, for a reason worth being
explicit about.

**Why the lot join is spatial.** The roll *does* publish the lot numbers a unit
covers, in `b05v_lot_cadst`. That table would give the exact many-to-many
mapping. It is not read here, so the units are placed on lots by where their
point falls, and that trades a known error for a different one:

* An assessment point sits at the **visual centre of the unit**, so a unit
  spanning three lots lands entirely on whichever of the three holds its
  centre. Its full value is attributed there and the other two get none of it.
* A point that falls in a lane, a park, or a lot the cadastre draws slightly
  differently matches nothing at all, and its value is attributed nowhere.

Both are visible rather than assumed: `num_units_unmatched_in_snapshot` counts
the second, and the first is why `num_assessment_units` travels beside every
total. On the first VSMPE snapshot 26,484 units landed on 21,676 of the
borough's 24,952 lots; the other 3,276 are mostly lanes, parks and city parcels
that carry no assessed property, which is what `num_lots_unvalued` reports.
Reading `b05v_lot_cadst` and joining on the lot number is the refinement this
asset is shaped to accept, and the reason the grain is declared here rather
than assumed downstream.

**The sum is over units, not over buildings.** A divided-co-ownership building
is one unit per apartment, all of them on the one `PC-*` common-parts lot: the
largest lot in the first VSMPE snapshot carries 402 units and $258M. That is
the number a highest-and-best-use question wants - what the ground is
currently worth in aggregate - and it is only readable as such because the unit
count sits next to it.

**`rl0404a` is VALEUR IMMEUBLE**, the whole property on the roll in force -
land plus buildings, which the roll also splits as `rl0402a` and `rl0403a`. It
is an assessed value for taxation, not a market appraisal, and Montreal's roll
is triennial: every unit in a 2026 roll is valued as of the same reference
date, so the totals compare across lots and do not track a market between
rolls.

**Only the last of the three has a Postgres table.** `lot_assessed_values`
owns `silver.lot_assessed_values` (hbu_infra's
sql/013_silver_lot_assessed_values.sql), like every other borough-scoped silver
asset. `assessment_units` is a documented absence in
`urban_rag.warehouse.TABLES`: every warehouse table is `PARTITION BY LIST
(neighborhood)` and this is the one silver asset with no borough axis to
supply one. Its record is the tree, which is where the record lives anyway.
"""

from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import shapely
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    asset,
)
from pydantic import Field

from urban_rag.frames import count_invalid_geometries, write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.layers import key_prefix
from urban_rag.open_data_assets import GROUP as OPEN_DATA_GROUP
from urban_rag.partitions import date_partitions, scrape_partitions
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource, RoleResource
from urban_rag.role_foncier import (
    JOIN_KEY,
    MONTREAL_CODE_MUN,
    POINT_LAYER,
    UNITS_LAYER,
    UNREAD_LAYERS,
    VALUE_COLUMN,
    RoleError,
    filename_for,
    layer_named,
    municipality_filter,
    read_layer,
)
from urban_rag.storage import clear_parquet, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

#: Shared with `urban_rag.open_data_assets` rather than restated: both are
#: municipal/provincial open-data portals read straight off a published URL,
#: and the group is what the Dagster UI sorts them into. Imported so the two
#: cannot drift apart into `bronze_open_data` and `bronze_opendata`.
GROUP = OPEN_DATA_GROUP

#: The silver pair below. Its own group rather than `silver_joins`, which is
#: the PostGIS cadastre joins, or `silver_streets`: this is the assessment
#: lineage, and both of its assets are pandas merges over the tree.
SILVER_GROUP = "silver_assessment"

#: The two files a bronze partition is written to, under
#: `bronze/property_assessment_roll/<YYYY-MM-DD>/`. Named for the layers they
#: are, minus the roll-year suffix the publisher stamps on: bronze carries its
#: publishers' vocabulary, the same way `NO_LOT` and `COTE_RUE_ID` survive
#: into it untouched.
POINTS_FILE = "rol_unite_p.parquet"
UNITS_FILE = "unite_evaln.parquet"

#: The one file `assessment_units` writes, under
#: `silver/assessment_units/<YYYY-MM-DD>/`.
ASSESSMENT_UNITS_FILE = "assessment_units.parquet"

#: The one file `lot_assessed_values` writes, under
#: `silver/lot_assessed_values/<YYYY-MM-DD>/<neighborhood>/`.
LOT_VALUES_FILE = "lot_assessed_values.parquet"

#: Infolot's lot number, and the grain `lot_assessed_values` groups at.
LOT_NUMBER_COLUMN = "NO_LOT"

#: Columns the characteristics table repeats from the point layer verbatim -
#: the same fact keyed by the same `id_provinc`, published twice. Dropped from
#: the right-hand side of the merge rather than suffixed, but only after the
#: two sides are checked against each other: silver is the layer that is
#: allowed to refuse, and two spellings of one municipality code is a thing to
#: refuse rather than to pick a winner for.
#:
#: `identifiant` is deliberately absent. It also appears in both, and the two
#: do *not* agree - the point layer leaves it null across Montreal while the
#: characteristics table fills it in - so it is suffixed and kept.
REPEATED_COLUMNS = ("code_mun", "mat18")

#: Suffix the merge gives the characteristics table's side of a name collision
#: that `REPEATED_COLUMNS` does not cover.
UNITS_SUFFIX = "_unite"

#: Provenance columns this pipeline adds in bronze. Identical on both files by
#: construction, so they are dropped from the right-hand side unchecked.
PROVENANCE_COLUMNS = (
    "roll_year",
    "source_file",
    "source_layer",
    "scrape_date",
    "scraped_at",
)

SOURCE_URL = "https://donneesouvertes.affmunqc.net/role/"


class AssessmentRollConfig(Config):
    """Which municipalities' rolls to keep out of the province-wide archive.

    Config rather than a constant, and a filter rather than the whole province,
    for two reasons that point the same way. Scoping a source to the territory
    being modelled is a bound on what was asked for rather than an
    interpretation of what came back, which is what keeps this in bronze - the
    same move `cmhc_vacancy_survey` makes when it keeps the Montreal CMA out of
    a national survey. And the province is 3.7 million assessment units against
    Montreal's 437 thousand, so the bound is also the difference between a few
    hundred megabytes of memory and a few gigabytes.

    It defaults to Ville de Montréal, which is every borough this pipeline has.
    Set it to `[]` for the whole province, or add codes for the other on-island
    municipalities - Westmount, Mont-Royal, Côte-Saint-Luc and the rest file
    their own rolls and are not boroughs.
    """

    municipality_codes: list[str] = Field(
        default=[MONTREAL_CODE_MUN],
        description=(
            "Five-digit `code_mun` values to keep. Empty keeps the province."
        ),
    )


@asset(
    key_prefix=key_prefix("property_assessment_roll"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"geopackage", "geoparquet", "parquet"},
    description=(
        "Quebec's property assessment roll, snapshot per scrape date under "
        "bronze/property_assessment_roll/<YYYY-MM-DD>/: one point per unité "
        f"d'évaluation as {POINTS_FILE}, and the characteristics table those "
        f"points are described by as {UNITS_FILE}. Two of the archive's five "
        "layers; the addresses, cadastral lot numbers and fiscal breakdowns "
        "are one-to-many against the unit and are not read. Scoped to Ville de "
        "Montréal by default, out of a province-wide publication. Source: "
        f"{SOURCE_URL}"
    ),
)
def property_assessment_roll(
    context: AssetExecutionContext,
    config: AssessmentRollConfig,
    role: RoleResource,
    store: ParquetStore,
) -> MaterializeResult:
    scrape_date = context.partition_key
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)

    filename = filename_for(role.roll_year)
    try:
        # Downloaded once and unpacked once, then shared by every scrape date:
        # a published roll year never changes. The first run of a year pays
        # 572 MB and 2.8 GB of disk; every later one pays neither.
        fetcher = role.fetcher()
        geopackage = fetcher.geopackage(filename)
        where = municipality_filter(config.municipality_codes)
        points_layer = layer_named(geopackage, POINT_LAYER)
        units_layer = layer_named(geopackage, UNITS_LAYER)
        context.log.info(
            "%s: reading %s and %s%s",
            filename,
            points_layer,
            units_layer,
            f" where {where}" if where else " (whole province)",
        )
        points = read_layer(geopackage, points_layer, where=where)
        units = read_layer(geopackage, units_layer, where=where, geometry=False)
    except RoleError as exc:
        # One archive, one query each: a failure here costs the whole
        # partition, the way it does for Infolot rather than for the Spectrum
        # scrape's per-table salvage.
        raise Failure(f"Assessment roll read for {scrape_date} failed: {exc}")

    if points.empty or units.empty:
        raise Failure(
            f"{filename} returned {len(points)} point(s) and {len(units)} "
            f"characteristics row(s) for {where or 'the whole province'}; "
            "check the municipality codes against the archive's `code_mun`."
        )

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    paths = {}
    for frame, layer, filename_out in (
        (points, points_layer, POINTS_FILE),
        (units, units_layer, UNITS_FILE),
    ):
        # Written as columns because the output path holds bare keys rather
        # than hive `key=value` pairs, so a reader that opens one file still
        # knows which snapshot - and which roll - it belongs to.
        frame["roll_year"] = role.roll_year
        frame["source_file"] = filename
        frame["source_layer"] = layer
        frame["scrape_date"] = scrape_date
        frame["scraped_at"] = scraped_at
        paths[filename_out] = write_frame(frame, join(output_dir, filename_out))

    # Reported, not repaired, so the snapshot stays a faithful copy.
    invalid = count_invalid_geometries(points)
    if invalid:
        context.log.warning("%s: %d invalid geometr(ies)", POINTS_FILE, invalid)
    context.log.info(
        "%s %s: %d point(s), %d characteristics row(s) -> %s",
        filename,
        scrape_date,
        len(points),
        len(units),
        output_dir,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(points),
            "num_assessment_points": len(points),
            "num_characteristics_rows": len(units),
            # The key silver declares its grain on. Reported rather than
            # enforced: bronze keeps whatever the publisher sent, and a
            # duplicate here is the publisher's fact, not this asset's failure.
            "num_assessment_units": int(points[JOIN_KEY].nunique()),
            "num_municipalities": int(points["code_mun"].nunique()),
            "num_invalid_geometries": invalid,
            "roll_year": role.roll_year,
            "municipality_filter": where or "none (whole province)",
            "layers_not_read": MetadataValue.json(list(UNREAD_LAYERS)),
            "points_path": MetadataValue.path(str(paths[POINTS_FILE])),
            "units_path": MetadataValue.path(str(paths[UNITS_FILE])),
            "geopackage_path": MetadataValue.path(str(geopackage)),
            "source_url": MetadataValue.url(f"{SOURCE_URL}{filename}"),
        }
    )


@asset(
    key_prefix=key_prefix("assessment_units"),
    partitions_def=date_partitions,
    deps=[property_assessment_roll],
    group_name=SILVER_GROUP,
    kinds={"geoparquet"},
    description=(
        "Every assessment unit as one row: its point, and the characteristics "
        "the roll describes it by - assessed values, floor area, storeys, year "
        "built, dwellings, use code. The two bronze files merged on "
        f"{JOIN_KEY}, which is what the publisher splits them by. Written to "
        f"silver/assessment_units/<YYYY-MM-DD>/{ASSESSMENT_UNITS_FILE}. As "
        "wide as the bronze snapshot - the roll has no borough axis, and the "
        "borough cut happens against the cadastre one asset later."
    ),
)
def assessment_units(
    context: AssetExecutionContext, store: ParquetStore
) -> MaterializeResult:
    scrape_date = context.partition_key
    bronze_dir = store.partition_dir(
        property_assessment_roll.key.path[-1], scrape_date
    )

    written_by = "/".join(property_assessment_roll.key.path)
    points = _read_geoparquet(
        join(bronze_dir, POINTS_FILE), written_by=written_by, partition=scrape_date
    )
    units = _read_parquet(
        join(bronze_dir, UNITS_FILE), written_by=written_by, partition=scrape_date
    )
    if points.empty:
        raise Failure(f"{join(bronze_dir, POINTS_FILE)} holds no assessment point.")
    if units.empty:
        raise Failure(f"{join(bronze_dir, UNITS_FILE)} holds no characteristics row.")

    # One row per unit on both sides - the grain this asset declares, and what
    # makes the merge below a merge rather than a multiplication. Bronze keeps
    # whatever the publisher sent; a duplicate reaching the lot totals
    # downstream would double a property's value and look like a plausible
    # number rather than an error.
    _require_unique_units(points, POINTS_FILE, scrape_date=scrape_date)
    _require_unique_units(units, UNITS_FILE, scrape_date=scrape_date)

    _require_agreement(points, units, scrape_date=scrape_date)
    right = units.drop(
        columns=[
            column
            for column in (*REPEATED_COLUMNS, *PROVENANCE_COLUMNS)
            if column in units.columns
        ]
    )
    merged = points.merge(
        right, on=JOIN_KEY, how="inner", suffixes=("", UNITS_SUFFIX)
    )
    if merged.empty:
        raise Failure(
            f"No {JOIN_KEY} is in both bronze files for {scrape_date}; the two "
            "layers were read from different archives."
        )

    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(merged, join(output_dir, ASSESSMENT_UNITS_FILE))

    # A unit with a point and no characteristics is a property with no value on
    # it; one with characteristics and no point is a property that cannot be put
    # on a lot. Both are dropped by the inner merge, so both are counted here.
    points_unmatched = len(points) - len(merged)
    units_unmatched = len(units) - len(merged)
    total_value = float(merged[VALUE_COLUMN].sum())
    context.log.info(
        "%s: %d unit(s) merged from %d point(s) and %d characteristics row(s), "
        "$%.1fB assessed -> %s",
        scrape_date,
        len(merged),
        len(points),
        len(units),
        total_value / 1e9,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(merged),
            "num_assessment_units": len(merged),
            "num_points": len(points),
            "num_characteristics_rows": len(units),
            "num_points_unmatched": points_unmatched,
            "num_characteristics_unmatched": units_unmatched,
            "total_assessed_value": round(total_value, 2),
            "total_assessed_value_billions": round(total_value / 1e9, 2),
            "num_invalid_geometries": count_invalid_geometries(merged),
            "roll_year": int(merged["roll_year"].iloc[0]),
            "output_path": MetadataValue.path(str(path)),
        }
    )


@asset(
    key_prefix=key_prefix("lot_assessed_values"),
    partitions_def=scrape_partitions,
    deps=[
        neighborhood_lots,
        AssetDep(
            assessment_units,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        ),
    ],
    group_name=SILVER_GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "What every lot in one borough is assessed at: the assessment units "
        "whose point falls inside it, summed. One row per NO_LOT with the "
        f"lot's geometry, the number of units on it and the total {VALUE_COLUMN} "
        "(VALEUR IMMEUBLE) they carry - a lot with none keeps its row with a "
        "null total, since no assessed property is not a property worth zero. "
        f"Written to silver/lot_assessed_values/<YYYY-MM-DD>/<neighborhood>/"
        f"{LOT_VALUES_FILE} and upserted into silver.lot_assessed_values on "
        "(scrape_date, neighborhood, lot_number)."
    ),
)
def lot_assessed_values(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    lots_path = join(
        store.partition_dir(neighborhood_lots.key.path[-1], scrape_date, neighborhood),
        LOTS_FILE,
    )
    units_path = join(
        store.partition_dir(assessment_units.key.path[-1], scrape_date),
        ASSESSMENT_UNITS_FILE,
    )
    lots = _read_geoparquet(
        lots_path,
        written_by="/".join(neighborhood_lots.key.path),
        partition=f"{scrape_date} {neighborhood}",
    )
    units = _read_geoparquet(
        units_path,
        written_by="/".join(assessment_units.key.path),
        partition=scrape_date,
    )
    if lots.empty:
        raise Failure(f"{lots_path} holds no lot to value.")
    if units.empty:
        raise Failure(f"{units_path} holds no assessment unit.")
    if LOT_NUMBER_COLUMN not in lots.columns:
        raise Failure(
            f"{lots_path} has no {LOT_NUMBER_COLUMN} column - it was not "
            "written by neighborhood_lots."
        )

    # Bronze reports invalid rings and keeps them; what reads this frame is a
    # point-in-polygon join, which on a self-intersecting ring either raises or
    # answers a question nobody asked. The same repair
    # `building_lot_intersections` makes on the way into PostGIS, made here for
    # the same reason and counted so it stays visible.
    repaired = count_invalid_geometries(lots)
    if repaired:
        lots = _make_valid(lots)
        context.log.info("Repaired %d invalid lot geometr(ies)", repaired)
    still_invalid = count_invalid_geometries(lots)
    if still_invalid:
        raise Failure(
            f"{still_invalid} lot geometr(ies) are still invalid after "
            "make_valid; the partition cannot be joined against."
        )
    _require_unique_lots(lots, neighborhood=neighborhood, scrape_date=scrape_date)

    lots = lots.to_crs(units.crs)
    paired = gpd.sjoin(
        units[[JOIN_KEY, VALUE_COLUMN, "geometry"]],
        lots[[LOT_NUMBER_COLUMN, "geometry"]],
        predicate="within",
        how="inner",
    )
    if paired.empty:
        # Not "this borough has no assessed property": no unit anywhere in the
        # snapshot falls in any of its lots, which means the two sides do not
        # overlap at all - a municipality filter that excluded this borough, or
        # a cadastre and a roll from territories that do not meet.
        raise Failure(
            f"No assessment unit falls inside any {neighborhood} lot for "
            f"{scrape_date}. {units_path} holds {len(units)} unit(s) and "
            f"{lots_path} holds {len(lots)} lot(s); check that "
            "property_assessment_roll kept this borough's municipality."
        )
    totals = (
        paired.groupby(LOT_NUMBER_COLUMN)
        .agg(
            num_assessment_units=(JOIN_KEY, "nunique"),
            total_assessed_value=(VALUE_COLUMN, "sum"),
        )
        .reset_index()
    )

    # Left, not inner: a lot nothing is assessed on is a lane, a park or a city
    # parcel, and its absence from the answer would read as a lot that has no
    # row rather than as one that carries no assessed property. The total stays
    # null there - a sum over nothing is not a value of zero.
    valued = lots.merge(totals, on=LOT_NUMBER_COLUMN, how="left")
    valued["num_assessment_units"] = (
        valued["num_assessment_units"].fillna(0).astype("int64")
    )
    valued["total_assessed_value"] = valued["total_assessed_value"].astype("Float64")
    valued["roll_year"] = int(units["roll_year"].iloc[0])
    # `scrape_date` is already a column, from bronze, and is overwritten rather
    # than trusted: this partition's date is the one that was asked for.
    valued["neighborhood"] = neighborhood
    valued["scrape_date"] = scrape_date

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(valued, join(output_dir, LOT_VALUES_FILE))

    # After the parquet, deliberately: the file is the record and a database
    # that is down should cost a re-run of the load rather than of the join.
    try:
        loaded = publish(
            postgis.connect,
            {"lot_assessed_values": valued},
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.lot_assessed_values could not be "
            f"updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    lots_valued = len(totals)
    units_matched = int(paired[JOIN_KEY].nunique())
    total_value = float(totals["total_assessed_value"].sum())
    context.log.info(
        "%s %s: %d of %d unit(s) fell inside %d of %d lot(s), $%.1fB assessed "
        "-> %s",
        neighborhood,
        scrape_date,
        units_matched,
        len(units),
        lots_valued,
        len(lots),
        total_value / 1e9,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(valued),
            "num_lots": len(lots),
            "num_lots_valued": lots_valued,
            # The symptom worth seeing. A few lanes and parks is the honest
            # reading; a third of the borough is the cadastre and the roll
            # disagreeing about where the ground is.
            "num_lots_unvalued": len(lots) - lots_valued,
            "num_units_in_snapshot": len(units),
            "num_units_matched": units_matched,
            # Units whose point fell in no lot of this borough. Almost all of
            # them are in another borough; what is left are the ones this
            # partition attributes to nobody.
            "num_units_unmatched_in_snapshot": len(units) - units_matched,
            "num_geometries_repaired": repaired,
            "total_assessed_value": round(total_value, 2),
            "total_assessed_value_billions": round(total_value / 1e9, 2),
            "max_lot_value": round(float(totals["total_assessed_value"].max()), 2),
            "max_units_on_a_lot": int(totals["num_assessment_units"].max()),
            "mean_units_per_valued_lot": round(
                float(totals["num_assessment_units"].mean()), 2
            ),
            "roll_year": int(valued["roll_year"].iloc[0]),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


def _read_geoparquet(path: str, *, written_by: str, partition: str) -> gpd.GeoDataFrame:
    try:
        return gpd.read_parquet(path, storage_options=storage_options(path))
    except FileNotFoundError as exc:
        raise Failure(_missing(path, written_by, partition)) from exc


def _read_parquet(path: str, *, written_by: str, partition: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, storage_options=storage_options(path))
    except FileNotFoundError as exc:
        raise Failure(_missing(path, written_by, partition)) from exc


def _missing(path: str, written_by: str, partition: str) -> str:
    """Name the asset to materialize, rather than only the file that is absent."""
    return f"{path} does not exist - materialize {written_by} for {partition} first."


def _make_valid(lots: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """``lots`` with every self-intersecting ring repaired.

    Applied to the whole column rather than to the invalid rows only: shapely's
    `make_valid` is a no-op on geometry that is already valid, and selecting
    first would cost a second validity pass to save nothing. The same function
    `building_lot_intersections` runs before its two PostGIS joins.
    """
    repaired = lots.copy()
    return repaired.set_geometry(
        gpd.GeoSeries(
            shapely.make_valid(repaired.geometry.values._data),
            index=repaired.index,
            crs=repaired.crs,
        )
    )


def _require_unique_units(frame, filename: str, *, scrape_date: str) -> None:
    """One row per `JOIN_KEY` - the grain the merge in `assessment_units` needs.

    The roll publishes one row per unit in each of these two layers, so a
    duplicate means the archive carried the same unit twice rather than that
    the province reuses the key. Left unchecked it would multiply the merge and
    then double a lot's total downstream, which shows up as a plausible-looking
    number rather than as a crash.
    """
    keys = frame[JOIN_KEY]
    duplicated = keys[keys.duplicated(keep=False)]
    if not duplicated.empty:
        repeated = sorted(set(duplicated.astype(str)))
        raise Failure(
            f"{filename} for {scrape_date}: {len(repeated)} {JOIN_KEY} value(s) "
            f"appear more than once, e.g. {', '.join(repeated[:5])}. "
            f"One row per {JOIN_KEY} is the grain this asset declares."
        )


def _require_agreement(points, units, *, scrape_date: str) -> None:
    """The columns both layers publish say the same thing about the same unit.

    `code_mun` and `mat18` are `id_provinc` taken apart - the archive spells the
    key out three ways - so they are dropped from one side of the merge rather
    than suffixed onto it. Checked first: two spellings of one municipality code
    is a thing for silver to refuse rather than to pick a winner for, and if it
    ever happens it means the two layers were read from different archives.
    """
    shared = [
        column
        for column in REPEATED_COLUMNS
        if column in points.columns and column in units.columns
    ]
    if not shared:
        return
    left = points.set_index(JOIN_KEY)
    right = units.set_index(JOIN_KEY)
    common = left.index.intersection(right.index)
    for column in shared:
        # Compared as text: these are identifier columns, and a dtype that
        # arrived as a number on one side and a string on the other would
        # otherwise read as a disagreement about the value.
        mine = left.loc[common, column].astype("string").fillna("")
        theirs = right.loc[common, column].astype("string").fillna("")
        disagree = mine != theirs
        if disagree.any():
            example = str(common[disagree.values][0])
            raise Failure(
                f"{scrape_date}: {int(disagree.sum())} unit(s) carry a "
                f"different {column} in {POINTS_FILE} than in {UNITS_FILE}, "
                f"e.g. {JOIN_KEY}={example}. The two layers describe the same "
                "roll and cannot disagree about it."
            )


def _require_unique_lots(
    lots: gpd.GeoDataFrame, *, neighborhood: str, scrape_date: str
) -> None:
    """One row per lot number - the grain the totals are grouped at.

    Infolot answers a boundary query by object id, so the same lot can come
    back twice when a borough outline is a multipolygon and the lot straddles
    two of its rings. Bronze keeps both rows; a duplicate here would count
    every unit on that lot twice and inflate its total by exactly as much.
    """
    numbers = lots[LOT_NUMBER_COLUMN]
    duplicated = numbers[numbers.duplicated(keep=False)]
    if not duplicated.empty:
        repeated = sorted(set(duplicated.astype(str)))
        raise Failure(
            f"{neighborhood} {scrape_date}: {len(repeated)} lot number(s) appear "
            f"more than once, e.g. {', '.join(repeated[:5])}. "
            f"One row per {LOT_NUMBER_COLUMN} is the grain this asset groups at."
        )
