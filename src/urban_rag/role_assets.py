"""What every property in the borough is assessed at, and what that makes a lot worth.

Three assets over one publication - Quebec's *rôle d'évaluation foncière*, the
province-wide property assessment roll the MAMH republishes as open data.

`property_assessment_roll` snapshots it. The archive holds five layers and this
keeps three: `rol_unite_p`, one point per *unité d'évaluation*;
`b05v_unite_evaln`, the characteristics table those points are described by;
and `b05v_lot_cadst`, the crosswalk naming every cadastre lot a unit covers.
The other two - addresses and fiscal breakdowns - are not read; `UNREAD_LAYERS`
names them so a run reports what it left rather than leaving five million rows
unaccounted for.

`assessment_units` puts the first two back together on `id_provinc`, which is
what the publisher splits them by: the geometry is in one file and everything
true of the property is in the other, and neither is usable alone.

`lot_assessed_values` carries that onto the cadastre. It is the join this whole
chain exists for - Infolot draws the lot, the roll values the property, and
nothing published connects them.

**The lot join is by lot number, with the point as a fallback.** The roll
states which lots a property covers, so that statement is used rather than an
inference from geometry: `b05v_lot_cadst` gives (unit, lot number) and
`lot_key` is all that stands between the roll's ``"1243415"`` and Infolot's
``"1 243 415"``. On the first VSMPE snapshot that places 21,204 units on 21,862
of the borough's 24,952 lots, and 96.7% of those units sit on exactly one lot.

It cannot place everything, and what it misses is not random. **A condominium
unit names its *private* lot numbers, which Infolot does not draw** - the
polygon there is the `PC-*` common parts - so the crosswalk alone loses every
divided co-ownership in the borough: 5,211 units and $2.9B, none of which name
a lot that exists in the cadastre. Their point falls squarely on the `PC-*` lot
the tower stands on, so the point places them, and `LotValuesConfig` decides
whether it may. `num_units_by_point` says how many rows came that way, so a
table can be read back against the choice that produced it.

Together the two routes beat either alone: 26,489 units on 22,443 of VSMPE's
24,952 lots, against 21,862 for the crosswalk by itself and 21,676 for the
point by itself.

**Two totals, because one number cannot be both.** A unit spanning several lots
has its whole value counted on each of them in `total_assessed_value` - which
is the right answer to "what is the assessed value of the property standing on
this lot" and the wrong one to sum across a borough, where it over-counts:
$32.37B against $27.24B on VSMPE. `total_assessed_value_apportioned` splits
each unit's value across the lots it covers, so that one adds up - one unit's
$150,476,700 on four lots is $37,619,175 apportioned to each.
`num_shared_units` says how many of a lot's units are counted whole somewhere
else too, which is exactly where the two differ; 691 of the borough's units are
shared that way.

**The sum is over units, not over buildings.** A divided-co-ownership building
is one unit per apartment, all of them on the one `PC-*` common-parts lot: the
largest lot in the first VSMPE snapshot carries 402 units and $258M. That is
the number a highest-and-best-use question wants - what the ground is currently
worth in aggregate - and it is only readable as such because the unit count
sits next to it.

**`rl0404a` is VALEUR IMMEUBLE**, the whole property on the roll in force -
land plus buildings, which the roll also splits as `rl0402a` and `rl0403a`. It
is an assessed value for taxation, not a market appraisal, and Montreal's roll
is triennial: every unit in a 2026 roll is valued as of the same reference
date, so the totals compare across lots and do not track a market between
rolls.

**Both silver assets have a Postgres table, and one of them fills several
partitions at once.** `lot_assessed_values` owns `silver.lot_assessed_values`
(hbu_infra's sql/013), and is borough-partitioned the ordinary way: it is
materialized per borough and publishes the borough it was asked for.
`assessment_units` owns `silver.assessment_units` (sql/014) and is not. The
roll has no borough axis to be partitioned on - it is one publication for the
province, merged once - so the asset stays date-partitioned and its parquet
stays province-wide, and the borough each unit belongs to is read off the map:
`assign_boroughs` puts every unit in the borough whose `reference_neighborhoods`
boundary its point falls inside, and `warehouse.publish_by_neighborhood` upserts
all of them in one transaction.

That is the same cut `neighborhood_streets` makes on the island-wide geobase,
made against points rather than lines, and it is why the tree and the table do
not hold the same rows: the parquet carries every municipality
`AssessmentRollConfig` kept, and the table carries the boroughs.
`num_units_outside_every_borough` is the difference.

The roll states a borough of its own - `arrond`, which is ``REM`` plus the
`no_arr` the reference layer carries - and it is *not* what the table is
partitioned on. Geometry decides, because that is what every other borough cut
in this platform decides on; `num_units_arrond_disagrees` counts where the two
publishers part company, so the choice is visible rather than assumed.
"""

import re
from dataclasses import dataclass
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
from urban_rag.open_data_assets import borough_boundary, reference_neighborhoods
from urban_rag.partitions import (
    ENABLED_NEIGHBORHOODS,
    borough_code_for,
    date_partitions,
    scrape_partitions,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource, RoleResource
from urban_rag.role_foncier import (
    CADASTRE_LAYER,
    JOIN_KEY,
    MONTREAL_CODE_MUN,
    POINT_LAYER,
    ROLL_LOT_COLUMN,
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
from urban_rag.warehouse import (
    MissingRelation,
    publish,
    publish_by_neighborhood,
    published_metadata,
)

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
CADASTRE_FILE = "lot_cadst.parquet"

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

#: The borough the point layer says a unit is in: ``REM`` followed by the
#: two-digit `no_arr` the reference-neighborhood layer carries, so ``REM25`` is
#: VSMPE. Not what `silver.assessment_units` is partitioned on - that borough
#: is the one whose boundary the point falls inside, the same cut every other
#: borough-scoped asset here makes - but it is the publisher's own answer to
#: the same question, and `assign_boroughs` counts where the two disagree
#: rather than assuming they never do.
ARROND_COLUMN = "arrond"
ARROND_PREFIX = "REM"

SOURCE_URL = "https://donneesouvertes.affmunqc.net/role/"

#: Whitespace of every kind, for `lot_key`. `\s` in a `str` pattern is already
#: Unicode-aware, so this covers the no-break and narrow no-break spaces French
#: thousands separators are published with as well as the plain one.
_SPACES = re.compile(r"\s+")


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


class LotValuesConfig(Config):
    """Whether to fall back to the assessment point for units the roll's own
    cadastre crosswalk cannot place.

    Config rather than a constant because it decides which of two known errors
    a partition carries, and that is a judgement about what the totals are for
    rather than a property of the data.

    **On** (the default) is the fuller answer. `b05v_lot_cadst` names a
    condominium unit's *private* lot numbers, which Infolot does not draw - the
    polygon there is the `PC-*` common parts - so the crosswalk alone loses
    every divided co-ownership in the borough: 5,211 units and $2.9B of the
    first VSMPE snapshot. Their point falls on the `PC-*` lot, which is the lot
    a reader asking what that ground is worth means.

    **Off** is the stricter one: every row then comes from the roll's own
    statement of which lots a property covers, and nothing is inferred from
    where a dot was placed. `num_units_by_point` is 0, and the condominium
    towers are absent rather than approximated.

    Either way `num_units_by_point` says how many rows came from which, so a
    table can be read back against the choice that produced it.
    """

    place_unmatched_by_point: bool = Field(
        default=True,
        description=(
            "Place units the lot-number crosswalk cannot resolve by where "
            "their point falls. Off leaves them unplaced."
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
        f"d'évaluation as {POINTS_FILE}, the characteristics table those "
        f"points are described by as {UNITS_FILE}, and the crosswalk naming "
        f"every cadastre lot a unit covers as {CADASTRE_FILE}. Three of the "
        "archive's five layers; the addresses and fiscal breakdowns are not "
        "read. Scoped to Ville de Montréal by default, out of a province-wide "
        f"publication. Source: {SOURCE_URL}"
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
        cadastre_layer = layer_named(geopackage, CADASTRE_LAYER)
        context.log.info(
            "%s: reading %s, %s and %s%s",
            filename,
            points_layer,
            units_layer,
            cadastre_layer,
            f" where {where}" if where else " (whole province)",
        )
        points = read_layer(geopackage, points_layer, where=where)
        units = read_layer(geopackage, units_layer, where=where, geometry=False)
        cadastre = read_layer(
            geopackage, cadastre_layer, where=where, geometry=False
        )
    except RoleError as exc:
        # One archive, one query each: a failure here costs the whole
        # partition, the way it does for Infolot rather than for the Spectrum
        # scrape's per-table salvage.
        raise Failure(f"Assessment roll read for {scrape_date} failed: {exc}")

    if points.empty or units.empty or cadastre.empty:
        raise Failure(
            f"{filename} returned {len(points)} point(s), {len(units)} "
            f"characteristics row(s) and {len(cadastre)} lot crosswalk row(s) "
            f"for {where or 'the whole province'}; check the municipality "
            "codes against the archive's `code_mun`."
        )

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    paths = {}
    for frame, layer, filename_out in (
        (points, points_layer, POINTS_FILE),
        (units, units_layer, UNITS_FILE),
        (cadastre, cadastre_layer, CADASTRE_FILE),
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
        "%s %s: %d point(s), %d characteristics row(s), %d lot crosswalk "
        "row(s) -> %s",
        filename,
        scrape_date,
        len(points),
        len(units),
        len(cadastre),
        output_dir,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(points),
            "num_assessment_points": len(points),
            "num_characteristics_rows": len(units),
            "num_lot_crosswalk_rows": len(cadastre),
            # One row per (unit, lot), so this is the lot side of the grain
            # `lot_assessed_values` joins Infolot's polygons on.
            "num_cadastre_lots": int(cadastre[ROLL_LOT_COLUMN].nunique()),
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
            "cadastre_path": MetadataValue.path(str(paths[CADASTRE_FILE])),
            "geopackage_path": MetadataValue.path(str(geopackage)),
            "source_url": MetadataValue.url(f"{SOURCE_URL}{filename}"),
        }
    )


@asset(
    key_prefix=key_prefix("assessment_units"),
    partitions_def=date_partitions,
    deps=[property_assessment_roll, reference_neighborhoods],
    group_name=SILVER_GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "Every assessment unit as one row: its point, and the characteristics "
        "the roll describes it by - assessed values, floor area, storeys, year "
        "built, dwellings, use code. The two bronze files merged on "
        f"{JOIN_KEY}, which is what the publisher splits them by. Written to "
        f"silver/assessment_units/<YYYY-MM-DD>/{ASSESSMENT_UNITS_FILE} as wide "
        "as the bronze snapshot, and upserted into silver.assessment_units on "
        "(scrape_date, neighborhood, id_provinc) - one borough partition per "
        "enabled borough, holding the units whose point falls inside it."
    ),
)
def assessment_units(
    context: AssetExecutionContext, store: ParquetStore, postgis: PostgisResource
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

    # The parquet stays province-wide and the table does not: the borough is
    # assigned from where each point falls, and every enabled borough is a
    # partition published out of this one date partition. Published after the
    # file is written, the posture every silver asset here takes - the roll is
    # a 572 MB download and a merge over 437 thousand rows, so a database that
    # is down should cost the load rather than the merge.
    cut = assign_boroughs(store, merged, scrape_date=scrape_date)
    if not cut.frames:
        # The same refusal `neighborhood_streets` makes when nothing intersects
        # the borough, and for the same reason: silver is the layer that is
        # allowed to refuse, and a run that published no borough at all is a
        # boundary that did not load rather than a province with no properties
        # in it. The parquet is already written, so the re-run costs the merge
        # and not the download.
        raise Failure(
            f"{path} was written, but no assessment unit fell inside any of "
            f"{', '.join(ENABLED_NEIGHBORHOODS)} for {scrape_date}. The "
            "outlines in reference_neighborhoods may be empty for that date."
        )

    try:
        loaded = publish_by_neighborhood(
            postgis.connect,
            "assessment_units",
            cut.frames,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.assessment_units could not be "
            f"updated for {scrape_date}: {exc}"
        ) from exc

    # A unit with a point and no characteristics is a property with no value on
    # it; one with characteristics and no point is a property that cannot be put
    # on a lot. Both are dropped by the inner merge, so both are counted here.
    points_unmatched = len(points) - len(merged)
    units_unmatched = len(units) - len(merged)
    total_value = float(merged[VALUE_COLUMN].sum())
    context.log.info(
        "%s: %d unit(s) merged from %d point(s) and %d characteristics row(s), "
        "$%.1fB assessed -> %s; %d in %d borough(s), %d outside every one",
        scrape_date,
        len(merged),
        len(points),
        len(units),
        total_value / 1e9,
        path,
        cut.placed,
        len(cut.frames),
        cut.outside,
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
            "num_units_in_a_borough": cut.placed,
            # Every unit the province publishes for a municipality this
            # pipeline does not partition, plus the handful whose point falls
            # outside every enabled boundary. Not an error - the table is the
            # boroughs' - but it is the number that says how much of the
            # parquet the table does not carry.
            "num_units_outside_every_borough": cut.outside,
            "num_units_arrond_disagrees": cut.arrond_mismatch,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
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
        AssetDep(
            property_assessment_roll,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        ),
    ],
    group_name=SILVER_GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "What every lot in one borough is assessed at. Units are placed on "
        "lots by the roll's own cadastre crosswalk - b05v_lot_cadst, joined to "
        "Infolot on the lot number - and, for the units it cannot place, by "
        "where their point falls. One row per NO_LOT with the lot's geometry, "
        f"the units on it, the total {VALUE_COLUMN} (VALEUR IMMEUBLE) they "
        "carry and that total apportioned across the lots each unit spans. A "
        "lot with no unit keeps its row with a null total, since no assessed "
        "property is not a property worth zero. Written to silver/"
        f"lot_assessed_values/<YYYY-MM-DD>/<neighborhood>/{LOT_VALUES_FILE} "
        "and upserted into silver.lot_assessed_values on (scrape_date, "
        "neighborhood, lot_number)."
    ),
)
def lot_assessed_values(
    context: AssetExecutionContext,
    config: LotValuesConfig,
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
    cadastre_path = join(
        store.partition_dir(property_assessment_roll.key.path[-1], scrape_date),
        CADASTRE_FILE,
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
    crosswalk = _read_parquet(
        cadastre_path,
        written_by="/".join(property_assessment_roll.key.path),
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

    # Bronze reports invalid rings and keeps them. Only the point fallback
    # below reads the geometry, but it reads it with a point-in-polygon test,
    # which on a self-intersecting ring either raises or answers a question
    # nobody asked. The same repair `building_lot_intersections` makes on the
    # way into PostGIS, made here for the same reason and counted so it stays
    # visible.
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
    lots = lots.assign(lot_key=lots[LOT_NUMBER_COLUMN].map(lot_key))
    by_number = _pairs_by_lot_number(crosswalk, units, lots)
    placed = set(by_number[JOIN_KEY])

    # The fallback, and why it is on by default: the roll names a condominium
    # unit's *private* lots, and Infolot draws the common parts. So a tower's
    # units name lot numbers that have no polygon in the cadastre, match
    # nothing above, and their value - $2.9B across 5,211 units in the first
    # VSMPE snapshot - would simply be missing. Their point still falls
    # squarely on the `PC-*` lot the building stands on, which is the lot a
    # reader asking what that ground is worth means.
    by_point = (
        _pairs_by_point(units[~units[JOIN_KEY].isin(placed)], lots)
        if config.place_unmatched_by_point
        else _empty_pairs()
    )
    pairs = pd.concat([by_number, by_point], ignore_index=True)
    if pairs.empty:
        # Not "this borough has no assessed property": no unit in the snapshot
        # reaches any of its lots by either route, which means the two sides do
        # not overlap at all - a municipality filter that excluded this
        # borough, or a cadastre and a roll from territories that do not meet.
        raise Failure(
            f"No assessment unit could be placed on any {neighborhood} lot for "
            f"{scrape_date}. {units_path} holds {len(units)} unit(s), "
            f"{cadastre_path} {len(crosswalk)} crosswalk row(s), and "
            f"{lots_path} {len(lots)} lot(s); check that "
            "property_assessment_roll kept this borough's municipality."
        )

    # Apportioned over the lots the unit covers *in the snapshot*, not over the
    # ones in this borough: a unit straddling a borough line should contribute
    # its share here and the rest there, and dividing by the local lots alone
    # would hand this partition the whole of it.
    pairs["value_share"] = pairs[VALUE_COLUMN] / pairs["num_lots"]
    totals = (
        pairs.groupby(LOT_NUMBER_COLUMN)
        .agg(
            num_assessment_units=(JOIN_KEY, "nunique"),
            # Units this lot shares with another - the ones whose whole value
            # is counted here *and* elsewhere. Exactly what makes the two
            # totals below differ, reported so the difference is attributable.
            num_shared_units=("shared", "sum"),
            num_units_by_point=("by_point", "sum"),
            total_assessed_value=(VALUE_COLUMN, "sum"),
            total_assessed_value_apportioned=("value_share", "sum"),
        )
        .reset_index()
    )

    # Left, not inner: a lot nothing is assessed on is a lane, a park or a city
    # parcel, and its absence from the answer would read as a lot that has no
    # row rather than as one that carries no assessed property. The total stays
    # null there - a sum over nothing is not a value of zero.
    valued = lots.drop(columns="lot_key").merge(
        totals, on=LOT_NUMBER_COLUMN, how="left"
    )
    for column in ("num_assessment_units", "num_shared_units", "num_units_by_point"):
        valued[column] = valued[column].fillna(0).astype("int64")
    for column in ("total_assessed_value", "total_assessed_value_apportioned"):
        valued[column] = valued[column].astype("Float64")
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
    units_matched = int(pairs[JOIN_KEY].nunique())
    by_number_units = int(by_number[JOIN_KEY].nunique())
    by_point_units = int(by_point[JOIN_KEY].nunique())
    full_total = float(totals["total_assessed_value"].sum())
    apportioned_total = float(totals["total_assessed_value_apportioned"].sum())
    context.log.info(
        "%s %s: %d unit(s) on %d of %d lot(s) - %d by lot number, %d by point "
        "- $%.2fB assessed, $%.2fB apportioned -> %s",
        neighborhood,
        scrape_date,
        units_matched,
        lots_valued,
        len(lots),
        by_number_units,
        by_point_units,
        full_total / 1e9,
        apportioned_total / 1e9,
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
            # How each unit got here. The first is the roll's own answer; the
            # second is the fallback, and a large one means a borough thick
            # with divided co-ownership rather than a broken join.
            "num_units_by_lot_number": by_number_units,
            "num_units_by_point": by_point_units,
            # Units the crosswalk placed on more than one lot. Each is counted
            # whole on every one of them, which is exactly the gap between the
            # two totals below.
            "num_units_on_several_lots": int(
                by_number.loc[by_number["num_lots"] > 1, JOIN_KEY].nunique()
            ),
            # Units that name no lot of this borough and whose point falls in
            # none either. Almost all are in another borough; what is left this
            # partition attributes to nobody.
            "num_units_unmatched_in_snapshot": len(units) - units_matched,
            "num_geometries_repaired": repaired,
            # Sum over lots of each unit's whole value. Over-counts the borough
            # by exactly the multi-lot units' repeated value - read one lot's
            # row with this, and the borough's total with the one below.
            "total_assessed_value": round(full_total, 2),
            "total_assessed_value_billions": round(full_total / 1e9, 2),
            # Each unit's value split across the lots it covers, so this one
            # sums across lots without counting anything twice.
            "total_assessed_value_apportioned": round(apportioned_total, 2),
            "total_assessed_value_apportioned_billions": round(
                apportioned_total / 1e9, 2
            ),
            "placed_unmatched_by_point": config.place_unmatched_by_point,
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


#: The columns a pair frame carries, whichever route produced it: the unit, the
#: lot it was placed on, the value it brings, how many lots that value is spread
#: over, and which route placed it.
_PAIR_COLUMNS = (
    JOIN_KEY,
    LOT_NUMBER_COLUMN,
    VALUE_COLUMN,
    "num_lots",
    "shared",
    "by_point",
)


@dataclass(frozen=True)
class BoroughCut:
    """One province-wide merge, cut into the borough partitions it publishes.

    ``frames`` is keyed by neighborhood partition key, and a borough that no
    unit fell in is simply absent rather than an empty frame - an empty frame
    would upsert nothing and prune the whole partition, which is the right
    answer for a borough whose units really have gone and the wrong one for a
    boundary that failed to load.

    The three counts are what makes the cut readable back. ``placed`` and
    ``outside`` add up to the merge, and ``arrond_mismatch`` is the cross-check
    against the roll's own `arrond`.
    """

    frames: dict[str, gpd.GeoDataFrame]
    placed: int
    outside: int
    arrond_mismatch: int


def assign_boroughs(
    store: ParquetStore, units: gpd.GeoDataFrame, *, scrape_date: str
) -> BoroughCut:
    """Put every unit in the borough its point falls inside.

    The roll is one publication for the province and `assessment_units` merges
    it once, so this is where the island is cut into partitions - the same
    hinge `neighborhood_streets` turns on for the island-wide geobase, and for
    the same reason: one download serves every borough, and re-reading it per
    borough would be work done for nothing.

    A spatial join rather than 17 `within` passes, so the units' own index does
    the work; the boundaries come from `reference_neighborhoods` for the same
    scrape date, which is what every other borough cut in this platform is
    taken against.

    **`intersects`, not `within`.** A unit whose point lands exactly on the
    borough line belongs to that borough rather than to none. Two boroughs
    cannot both claim it in practice - the published outlines do not overlap -
    and if they ever did, the unit would be a row in each partition, which is
    what the partition key already means.

    A unit that falls in no enabled borough is dropped, and counted. Most of
    them are the point of the count: with `CODE_MUN='[]'` the merge is the
    whole province, and this table is the boroughs'.
    """
    boroughs = gpd.GeoDataFrame(
        {"neighborhood": list(ENABLED_NEIGHBORHOODS)},
        geometry=[
            borough_boundary(store, scrape_date, name)
            for name in ENABLED_NEIGHBORHOODS
        ],
        crs=units.crs,
    )
    joined = gpd.sjoin(units, boroughs, how="inner", predicate="intersects")
    # `neighborhood` is the only thing wanted from the right-hand side. The
    # bookkeeping column sjoin carries the right index across in is dropped by
    # what it is *not* rather than by its name - the name is a geopandas
    # detail, and anything left over would land in the jsonb catch-all as a
    # column no reader of the table could account for.
    placed = joined.drop(
        columns=[
            name
            for name in joined.columns
            if name not in set(units.columns) and name != "neighborhood"
        ]
    )
    frames = {
        str(name): frame.copy()
        for name, frame in placed.groupby("neighborhood", sort=False)
    }
    # Counted distinctly rather than as `len(placed)`: a unit on a borough line
    # that two outlines both claimed is two rows in two partitions and one
    # property, and `placed + outside` has to be the merge.
    inside = int(placed[JOIN_KEY].nunique())
    return BoroughCut(
        frames=frames,
        placed=inside,
        outside=len(units) - inside,
        arrond_mismatch=_arrond_mismatches(frames),
    )


def _arrond_mismatches(frames: dict[str, gpd.GeoDataFrame]) -> int:
    """Units the roll files under a borough other than the one they fell in.

    Zero is the expected answer and is worth having said: the roll's `arrond`
    and the city's published outlines are two agencies' answers to one
    question, and a partition where they part company is a boundary that moved
    or a point that is in the wrong place - either way something to look at
    rather than something to average over. Counted and never acted on; the
    geometry decides, because that is what the rest of the platform cuts on.
    """
    total = 0
    for neighborhood, frame in frames.items():
        if ARROND_COLUMN not in frame.columns:
            continue
        expected = f"{ARROND_PREFIX}{borough_code_for(neighborhood)}"
        stated = frame[ARROND_COLUMN]
        total += int((stated.notna() & (stated != expected)).sum())
    return total


def lot_key(number) -> str | None:
    """A lot number as the two publishers can be compared on.

    Infolot writes a lot as ``"1 243 415"`` and the roll writes the same lot as
    ``"1243415"``, so the spaces come out and nothing else does. In particular
    the `PC-` prefix on a divided co-ownership's common parts is *kept*: the
    roll has no such lot, so those keys are meant to miss rather than to be
    coerced into matching something.
    """
    if number is None:
        return None
    text = str(number).strip()
    if not text or text.lower() == "nan":
        return None
    # Every kind of space, not just U+0020: French thousands separators are
    # published as no-break (U+00A0) and narrow no-break (U+202F) spaces as
    # often as plain ones, and one of those left in makes the key miss
    # silently. Written as a pattern rather than as literals, which would be
    # invisible characters in the source.
    return _SPACES.sub("", text)


def _empty_pairs() -> pd.DataFrame:
    """A pair frame with no rows, for when the fallback is switched off."""
    return pd.DataFrame({column: [] for column in _PAIR_COLUMNS})


def _pairs_by_lot_number(
    crosswalk: pd.DataFrame, units: gpd.GeoDataFrame, lots: gpd.GeoDataFrame
) -> pd.DataFrame:
    """(unit, lot) pairs from the roll's own cadastre crosswalk.

    The roll states which lots a property covers, so this is its answer rather
    than an inference from geometry - the reason `b05v_lot_cadst` is snapshot
    at all. Three things happen on the way:

    * **The suffix is dropped.** `ROLL_LOT_SUFFIX_COLUMN` distinguishes rows of
      the non-renewed cadastre naming one renewed lot, so ignoring it leaves
      duplicate (unit, lot) rows - 1,758 of Montreal's - which would count a
      unit twice on the same lot.
    * **`num_lots` is counted before the borough is cut**, over every lot the
      snapshot says the unit covers. That is what makes the apportioned total
      add up across boroughs instead of giving each one the whole unit.
    * **The lot numbers are keyed with `lot_key`**, which is the only thing
      standing between the roll's ``"1243415"`` and Infolot's ``"1 243 415"``.
    """
    if crosswalk.empty or ROLL_LOT_COLUMN not in crosswalk.columns:
        return _empty_pairs()

    edges = crosswalk[[JOIN_KEY, ROLL_LOT_COLUMN]].copy()
    edges["lot_key"] = edges[ROLL_LOT_COLUMN].map(lot_key)
    edges = edges[edges["lot_key"].notna()].drop_duplicates(
        subset=[JOIN_KEY, "lot_key"]
    )
    if edges.empty:
        return _empty_pairs()

    spread = edges.groupby(JOIN_KEY)["lot_key"].transform("size")
    edges = edges.assign(num_lots=spread)

    paired = edges.merge(
        units[[JOIN_KEY, VALUE_COLUMN]], on=JOIN_KEY, how="inner"
    ).merge(lots[[LOT_NUMBER_COLUMN, "lot_key"]], on="lot_key", how="inner")
    if paired.empty:
        return _empty_pairs()
    paired["shared"] = paired["num_lots"] > 1
    paired["by_point"] = False
    return paired[list(_PAIR_COLUMNS)]


def _pairs_by_point(units: gpd.GeoDataFrame, lots: gpd.GeoDataFrame) -> pd.DataFrame:
    """(unit, lot) pairs from where each unit's point falls.

    The fallback for units the crosswalk could not place. A point sits at the
    unit's visual centre and falls in exactly one lot, so `num_lots` is 1 and
    the two totals agree on every row this produces.
    """
    if units.empty:
        return _empty_pairs()
    paired = gpd.sjoin(
        units[[JOIN_KEY, VALUE_COLUMN, "geometry"]],
        lots[[LOT_NUMBER_COLUMN, "geometry"]],
        predicate="within",
        how="inner",
    )
    if paired.empty:
        return _empty_pairs()
    # A point on a shared boundary can land in two lots; the value is not
    # duplicated over them, so the first is taken and `num_lots` stays 1.
    paired = paired.drop_duplicates(subset=[JOIN_KEY])
    return pd.DataFrame(
        {
            JOIN_KEY: paired[JOIN_KEY].to_numpy(),
            LOT_NUMBER_COLUMN: paired[LOT_NUMBER_COLUMN].to_numpy(),
            VALUE_COLUMN: paired[VALUE_COLUMN].to_numpy(),
            "num_lots": 1,
            "shared": False,
            "by_point": True,
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
