"""Every lot in the borough, with what stands on it, what it faces, and what
governs it - one row per cadastral parcel.

This is the platform's gold layer for the lot lineage. Four silver joins each
hold one row per (lot x something), and each of them is the wrong shape for the
question a person actually asks. `building_lot_intersections` is one row per
(building, lot); `lot_frontage` is one row per (lot, street side);
`rag.lot_documents` is one row per (lot, feature, document);
`lot_zoning_envelopes` is one row per (lot, grid column). Somebody asking "what
can I do with lot 1 234 567" wants one row, and this asset is where the four
collapse onto it.

**It replaces `vacant_lots`, and the replacement is the point.** That asset
selected the parcels carrying nothing bigger than a shed, which made it a table
that could answer "where is the empty land" and nothing else: every lot its
WHERE clause dropped was a lot the reader could no longer see, so "the widest
built lots on this street" or "how much of the borough is built out" needed a
different table over the same join. Keeping every lot and carrying
`has_building` alongside costs one boolean column, and turns the vacant-land
question into a filter over an inventory -

    frame[~frame["has_building"]]

- while leaving every other question answerable from the same rows. `category`
is carried forward unchanged from that asset, so the three-way distinction it
drew (nothing at all / a shed / the neighbour's wall crossing the line) is not
lost either; it gains a fourth value, `built`, for the case the old table
expressed by having no row.

**`has_building` is not the negation of "vacant".** It is "does any footprint
intersect this parcel", which a 12 m2 shed satisfies. Usable emptiness depends
on a threshold, thresholds are judgements rather than properties of the data,
and a boolean cannot carry one - so that lives in `category` and in
`max_built_area_m2`, which every row records.

The frontage columns are the top two of `silver.lot_frontage`, pivoted: a lot
mostly fronts on one street, a corner lot on two, and ranks beyond the second
are counted in `num_frontages` and summed into `total_frontage_m` rather than
given a third pair of columns every other row would leave empty. Which edge is
primary is read off that table's own `frontage_rank` rather than re-decided
here, so the two cannot disagree. A lot facing no street reports
`num_frontages = 0` and NULL metres - not 0 m, which would claim it was
measured.

The document columns are the last hop of the chain `silver.lot_features` opens up:
`rag.chunks.feature_ids` records which map features cite each indexed PDF, and
`rag.lot_documents` puts that together with the features covering a lot. The
highest-coverage one across every layer is flattened into `doc_url`/`doc_title`
so the common read is a column, and the whole set travels in `documents` as
JSON, most-of-the-lot first.

**Four of the inputs never reach Postgres at all**, and are handed to the
INSERT by this asset instead of read out of a `rag` table.
`lot_zoning_envelopes` is staged into a temp table and aggregated into
`zoning_envelopes` - every grid column that governs the lot, with the norms it
states, so a reader holding one profile row has what `program.solve_program`
needs. `vacancy_rates` and `average_rents` become one object each. Those two
are the *borough's* figures and are identical on every row of the partition,
which is the point: CMHC surveys neighborhoods and publishes no geometry, so
there is nothing per-lot about them and nothing to join on.

**The cost columns are the fourth, and the same trade at a coarser grain.**
`montreal_residential_costs` and `montreal_nonresidential_costs` are the
Montreal column of the Altus Group Canadian Cost Guide, and they become one
`construction_costs` object. The guide prices nine Canadian markets and knows
nothing about boroughs, so this is denormalisation with even less to join on
than CMHC has - a Montreal rate is a Montreal rate on every lot of every
borough. It is here because it is the other half of the question this table
exists to answer: `overall_average_rent_cad` is what a building earns, and
these are what it costs to put up.

Six of them are flattened onto columns of their own, the way `doc_url` and
`overall_average_rent_cad` are. `underground_stall_cost_low/high_cad` and
`above_grade_stall_cost_low/high_cad` are the two structures a building
actually chooses between - stalls dug out underneath, or a garage integrated
into it at grade - and they are **dollars per stall**, not per square foot,
which is why `unit_flag` travels beside every rate in the object.
`condo_cost_low/high_cad_sqft` is one condominium / apartment band out of the
five the guide prices; which band is `LotProfilesConfig.condo_type_id`'s to
say, because it is a claim about what would be built here rather than
something the data settles, and the band chosen is named on every row as
`condo_band`. The other four stay in the object. `urban_rag.program` hardcodes
the midpoints of the two parking pairs today; these columns are where it can
read them from instead.

**That is what replaced `lots_with_vacancy_rates`.** That asset pivoted the
same CMHC grid onto the cadastre one layer earlier, before the spatial joins -
where it rode through `rag.lots.attributes` and both PostGIS joins without
anything ever reading it, and where it made every lot file in the tree carry
sixty survey columns that had nothing to do with the parcel. The join was
always by borough name; doing it here costs the same denormalisation at the
grain that actually asks the question, and lets `building_lot_intersections`
read the bronze cadastre directly. The geometry repair that asset also did did
not go away - it moved to `building_lot_intersections`, next to the
`ST_Intersection` calls it exists for.

Downstream of `building_lot_intersections`, `lot_frontage`, `document_index`,
`lot_zoning_envelopes`, `vacancy_rates` and `average_rents` for the *same*
partition, and of the two cost snapshots for the same *date* - those are
partitioned by date alone, so they map onto the `date` dimension the way
`vacancy_rates` maps its own bronze survey. The first three are what land
`rag.lots`, `silver.building_lot_intersections`, `silver.lot_features`, `silver.lot_frontage` and
`rag.chunks`, so by the time this runs the work here is five parquet reads and
one SQL statement. Like those,
the answer is computed in Postgres and then written to the tree as well -
`gold.lot_profiles` is what the query side reads, and
`gold/lot_profiles/<date>/<neighborhood>/` is the record it can be rebuilt
from.

**Two of the relations this reads are hbu_infra's to create.**
sql/009_gold_lot_profiles.sql creates the table itself, and sql/006_lot_documents.sql
creates the `rag.lot_documents` view - the second carries a
`-- requires: rag.chunks` header, so `db.py init` skips it on a database that
has never held a corpus and it only lands on the next init after
`document_index` has run. `compute_lot_profiles` checks for both up front and
fails naming the file to apply, rather than letting psycopg raise on whichever
identifier the planner resolved first. Until they are applied this asset is
registered and given a job but deliberately left off the daily schedules; see
`urban_rag.definitions`.
"""

import json

import pandas as pd
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

from urban_rag.building_lots_assets import building_lot_intersections
from urban_rag.cmhc_assets import (
    AVERAGE_RENTS_FILE,
    VACANCY_FILE,
    average_rents,
    vacancy_rates,
)
from urban_rag.envelope_assets import LOT_ENVELOPES_FILE, lot_zoning_envelopes
from urban_rag.estimator import (
    CONDO_TYPE_IDS,
    INTEGRATED_PARKING_TYPE_ID,
    LOW_RISE_CONDO_TYPE_ID,
    PARKING_TYPE_IDS,
    UNDERGROUND_PARKING_TYPE_ID,
)
from urban_rag.estimator_assets import (
    NON_RESIDENTIAL_FILE,
    RESIDENTIAL_FILE,
    montreal_nonresidential_costs,
    montreal_residential_costs,
)
from urban_rag.frames import write_frame
from urban_rag.frontage_assets import lot_frontage
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import (
    DEFAULT_MAX_BUILT_AREA_M2,
    LOT_CATEGORIES,
    MissingRelation,
    compute_lot_profiles,
    fetch_lot_profiles,
)
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.rag_assets import document_index
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, filesystem, join, storage_options

GROUP = "gold_lots"

#: The one file a partition is written to, under
#: `gold/lot_profiles/<YYYY-MM-DD>/<neighborhood>/`.
LOT_PROFILES_FILE = "lot_profiles.parquet"

#: Columns of `lot_zoning_envelopes` that do *not* travel into the
#: `zoning_envelopes` jsonb: the lot's own facts, which the profile row already
#: carries as columns and would only repeat here once per envelope. Named as
#: what to leave out rather than as what to take, so a norm added to
#: `envelope_assets.NORM_FIELDS` reaches this column by being added there and
#: nowhere else.
_ENVELOPE_LOT_COLUMNS = frozenset(
    {
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
    }
)

#: Envelope columns `zoning_grid_columns` wrote as JSON *strings*, for the same
#: schema-stability reason `documents` is a string in the parquet. Decoded on
#: the way into jsonb, where that reason does not apply and a string that looks
#: like a list is just a list nobody can query.
_ENVELOPE_JSON_COLUMNS = ("usages", "levels", "parse_notes")

#: The CMHC cell every reader means by "the vacancy rate here" / "the rent
#: here". Flattened out of the object onto its own column by the INSERT, and
#: named here because this is where the object is built.
_OVERALL_VACANCY = ("all", "all")
_OVERALL_BEDROOM = "all"

#: Columns of a bronze cost snapshot that travel into a `construction_costs`
#: entry: what the guide priced, and what one dollar figure buys. The city and
#: the provenance are *not* here - they are identical across every entry, so
#: they sit once at the top of the object rather than once per rate.
_COST_ENTRY_COLUMNS: tuple[str, ...] = (
    "id",
    "label",
    "cat",
    "unit_flag",
    "rate_low",
    "rate_high",
)

#: Where each flattened rate column reads from: the column name, the published
#: `id` it comes out of, and which end of that type's published range it takes.
#:
#: A table rather than six lookups because the same three facts have to line up
#: in four places - this payload, the INSERT's six casts, 009_lot_profiles.sql
#: and the run's metadata - and a column named after a type the guide no longer
#: publishes should be one edit to notice, not four.
#:
#: The condo pair is absent: which band it reads is `LotProfilesConfig`'s to
#: decide, so it is built alongside rather than listed here.
_FLATTENED_PARKING_RATES: tuple[tuple[str, str, str], ...] = (
    ("underground_stall_cost_low_cad", UNDERGROUND_PARKING_TYPE_ID, "rate_low"),
    ("underground_stall_cost_high_cad", UNDERGROUND_PARKING_TYPE_ID, "rate_high"),
    ("above_grade_stall_cost_low_cad", INTEGRATED_PARKING_TYPE_ID, "rate_low"),
    ("above_grade_stall_cost_high_cad", INTEGRATED_PARKING_TYPE_ID, "rate_high"),
)

#: The two flattened condo columns, and the end of the configured band's range
#: each takes.
_FLATTENED_CONDO_RATES: tuple[tuple[str, str], ...] = (
    ("condo_cost_low_cad_sqft", "rate_low"),
    ("condo_cost_high_cad_sqft", "rate_high"),
)

#: Provenance columns both bronze snapshots write onto every row, carried to
#: the top of `construction_costs` for the reason the CMHC objects carry
#: `survey_year`: a rate with no publication behind it cannot be read against
#: next quarter's. `scrape_date` is renamed on the way in - the profile row
#: already has one, and it is the *cadastre's*, not the guide's.
_COST_PROVENANCE_COLUMNS: tuple[str, ...] = (
    "city",
    "city_label",
    "source_url",
    "source_last_modified",
)


class LotProfilesConfig(Config):
    """Where the line between "built on" and "effectively empty" is drawn.

    Config rather than a constant because the threshold is a judgement about
    the built form, not a property of the data: 30 m2 is a shed in a borough
    of triplexes, and somewhere with detached garages on every lot may want
    60. Every run records the value it used in its metadata, and every row
    records it too, so a table can always be read back against the cutoff that
    produced it.

    It moves `category` only. `has_building` is `num_buildings > 0` whatever
    this is set to, which is what makes the two columns worth having side by
    side: one is a fact about the footprints, the other is this judgement
    applied to them.
    """

    max_built_area_m2: float = Field(
        default=DEFAULT_MAX_BUILT_AREA_M2,
        ge=0,
        description=(
            "A lot counts as effectively empty when at most this many square "
            "metres of building footprint stand on it. 0 makes only the lots "
            "no building touches at all anything other than 'built'."
        ),
    )

    #: The second judgement this asset makes, and Config for the same reason
    #: as the first. The cost guide prices five condominium / apartment bands
    #: by storey count, and which one belongs in `condo_cost_low_cad_sqft` is
    #: a claim about what would actually get built here rather than something
    #: the data settles: wood frame up to six storeys is what a borough of
    #: triplexes puts up, and a downtown parcel under a 40-storey envelope is
    #: not the same building at any price.
    #:
    #: The other four bands are in `construction_costs` whatever this is set
    #: to, and the band chosen is named in that object as `condo_band` - so a
    #: table can always be read back against the assumption that produced it,
    #: the same rule `max_built_area_m2` follows.
    condo_type_id: str = Field(
        default=LOW_RISE_CONDO_TYPE_ID,
        description=(
            "Which published condominium / apartment band the per-square-foot "
            "condo_cost_* columns are taken from. One of: "
            f"{', '.join(CONDO_TYPE_IDS)}."
        ),
    )


@asset(
    key_prefix=key_prefix("lot_profiles"),
    partitions_def=scrape_partitions,
    deps=[
        building_lot_intersections,
        lot_frontage,
        document_index,
        lot_zoning_envelopes,
        vacancy_rates,
        average_rents,
        # Partitioned by date alone: the guide prices nine Canadian markets and
        # knows nothing about boroughs, so one snapshot serves every one of
        # them. Mapped onto this asset's `date` dimension the same way
        # `vacancy_rates` maps its own bronze survey.
        *(
            AssetDep(
                costs,
                partition_mapping=MultiToSingleDimensionPartitionMapping(
                    partition_dimension_name="date"
                ),
            )
            for costs in (montreal_residential_costs, montreal_nonresidential_costs)
        ),
    ],
    group_name=GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "Every lot in the borough, one row each: the neighborhood it is in, "
        "whether a building stands on it (has_building) and how many "
        "(num_buildings), the footprint area on it and that area as a share "
        "of the lot, its primary and secondary street frontage in metres with "
        "the street each faces, and the zoning PDF that covers most of it - "
        "doc_url/doc_title, plus every applicable document in the documents "
        "column as JSON. category sorts the lot into built, no_building, "
        "shed_only or building_sliver against max_built_area_m2, which every "
        "row carries. Three more JSON columns collapse the rest of the "
        "lineage onto the same row: zoning_envelopes is this lot's "
        "lot_zoning_envelopes rows - every grid column that governs it, with "
        "the norms it states - and vacancy_rates/average_rents are the "
        "borough's CMHC survey, with the all/all cell flattened into "
        "overall_vacancy_rate_pct and overall_average_rent_cad. A fourth, "
        "construction_costs, is Montreal's column of the Altus cost guide: "
        "the underground and integrated ground-level parking rates flattened "
        "into underground_stall_cost_low/high_cad and "
        "above_grade_stall_cost_low/high_cad - dollars per stall, not per "
        "square foot - and the configured condominium / apartment band into "
        "condo_cost_low/high_cad_sqft, with the other four bands kept in the "
        "object. Replaces the "
        "old vacant_lots asset: that selection is now `WHERE NOT "
        "has_building`. Upserted into gold.lot_profiles on (scrape_date, "
        "neighborhood, lot_number) for the query side, and written to "
        "gold/lot_profiles/<YYYY-MM-DD>/<neighborhood>/"
        f"{LOT_PROFILES_FILE} as the record."
    ),
)
def lot_profiles(
    context: AssetExecutionContext,
    config: LotProfilesConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    # Read before the connection is opened: five parquet reads and a JSON
    # build have no business happening inside a write transaction, and a
    # partition missing one of them should fail naming what to materialize
    # rather than after deleting the rows it was going to replace.
    envelopes = _lot_envelopes(
        _read(
            store.partition_dir(
                lot_zoning_envelopes.key.path[-1], scrape_date, neighborhood
            ),
            LOT_ENVELOPES_FILE,
            asset_name=lot_zoning_envelopes.key.path[-1],
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    )
    vacancy = _vacancy_payload(
        _read(
            store.partition_dir(
                vacancy_rates.key.path[-1], scrape_date, neighborhood
            ),
            VACANCY_FILE,
            asset_name=vacancy_rates.key.path[-1],
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    )
    rents = _rents_payload(
        _read(
            store.partition_dir(
                average_rents.key.path[-1], scrape_date, neighborhood
            ),
            AVERAGE_RENTS_FILE,
            asset_name=average_rents.key.path[-1],
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    )
    # No neighborhood on either of these: the cost guide is partitioned by date
    # alone, so `<date>/` is the whole path and every borough of that day reads
    # the same two files.
    costs = _construction_costs_payload(
        context,
        residential=_read(
            store.partition_dir(montreal_residential_costs.key.path[-1], scrape_date),
            RESIDENTIAL_FILE,
            asset_name=montreal_residential_costs.key.path[-1],
            scrape_date=scrape_date,
        ),
        non_residential=_read(
            store.partition_dir(
                montreal_nonresidential_costs.key.path[-1], scrape_date
            ),
            NON_RESIDENTIAL_FILE,
            asset_name=montreal_nonresidential_costs.key.path[-1],
            scrape_date=scrape_date,
        ),
        condo_type_id=config.condo_type_id,
    )

    try:
        with postgis.connect() as connection:
            result = compute_lot_profiles(
                connection,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
                max_built_area_m2=config.max_built_area_m2,
                vacancy_rates=vacancy,
                average_rents=rents,
                construction_costs=costs,
                zoning_envelopes=envelopes,
            )
            num_lots = int(result["num_lots"])
            if num_lots == 0:
                # Not "the borough has no lots": the partition was never
                # loaded. Distinguishing the two is the whole point of failing
                # here instead of writing a perfectly well-formed zero. Raised
                # inside the transaction so the DELETE above rolls back with
                # it rather than leaving the previous run's rows removed.
                raise Failure(
                    f"rag.lots holds no lot for {neighborhood} {scrape_date} - "
                    "materialize building_lot_intersections for this partition "
                    "first."
                )
            # Inside the transaction that computed it, so the file is that
            # answer rather than whatever a concurrent run leaves behind after
            # the commit.
            frame = fetch_lot_profiles(
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
    path = write_frame(frame, join(output_dir, LOT_PROFILES_FILE))

    profiles = int(result["profiles"])
    by_category = result["by_category"]
    context.log.info(
        "%s %s: %d lot(s) profiled - %d built on, %d with frontage "
        "(%d on two streets), %d with a document, %d with a zoning envelope "
        "(%d in all); %s at <= %.0f m2 -> %s",
        neighborhood,
        scrape_date,
        profiles,
        int(result["num_with_building"]),
        int(result["num_with_frontage"]),
        int(result["num_with_secondary_frontage"]),
        int(result["num_with_documents"]),
        int(result["num_with_zoning_envelopes"]),
        int(result["num_zoning_envelopes"]),
        ", ".join(f"{name}={by_category[name]}" for name in LOT_CATEGORIES),
        config.max_built_area_m2,
        path,
    )

    staged = int(result["num_envelopes_staged"])
    landed = int(result["num_zoning_envelopes"])
    if staged != landed:
        # The envelope file names lots this partition's cadastre does not
        # have, which is what a stale silver/lot_zoning_envelopes looks like
        # from here. Not fatal - the lots that did match are still right.
        context.log.warning(
            "%s %s: %d of %d envelope row(s) matched no lot in rag.lots - "
            "re-materialize lot_zoning_envelopes for this partition",
            neighborhood,
            scrape_date,
            staged - landed,
            staged,
        )

    num_without_building = int(result["num_without_building"])
    num_with_frontage = int(result["num_with_frontage"])
    return MaterializeResult(
        metadata={
            "dagster/row_count": profiles,
            # These two agreeing is what says every lot got a profile. They
            # can only differ if the INSERT dropped rows, which nothing in it
            # should do - so a gap here is a bug rather than a data quality
            # observation, and it is worth being able to see at a glance.
            "num_lots": num_lots,
            "num_profiles": profiles,
            # What the upsert superseded: on a first run of a partition, zero;
            # on a re-run after the cadastre moved, the lots that are no longer
            # in it. See `urban_rag.warehouse`.
            "num_profiles_pruned": int(result["pruned"]),
            "num_with_building": int(result["num_with_building"]),
            "num_without_building": num_without_building,
            "pct_without_building": round(100.0 * num_without_building / profiles, 2)
            if profiles
            else 0.0,
            **{f"num_{name}": by_category[name] for name in LOT_CATEGORIES},
            "num_buildings": int(result["num_buildings"]),
            "total_lot_area_ha": round(result["total_lot_area_m2"] / 10_000, 2),
            "vacant_area_ha": round(
                sum(
                    area
                    for name, area in result["area_by_category"].items()
                    if name != "built"
                )
                / 10_000,
                2,
            ),
            "num_with_frontage": num_with_frontage,
            # The symptom worth seeing, carried up from lot_frontage: a lot
            # facing nothing is either a true interior parcel or a partition
            # whose street snapshot stops short of it. Under a few percent is
            # the first; a third of the borough is the second.
            "num_without_frontage": profiles - num_with_frontage,
            "num_with_secondary_frontage": int(result["num_with_secondary_frontage"]),
            "max_primary_frontage_m": round(result["max_primary_frontage_m"], 1),
            "mean_primary_frontage_m": round(result["mean_primary_frontage_m"], 1),
            # Zero here with a healthy num_lots means rag.chunks holds no
            # corpus for this partition, not that the borough is unzoned.
            "num_with_documents": int(result["num_with_documents"]),
            "num_without_documents": profiles - int(result["num_with_documents"]),
            "num_with_zoning_envelopes": int(result["num_with_zoning_envelopes"]),
            # A lot with no envelope is one no readable grid reaches - either
            # the cadastre stretches past the feature scrape, or that zone's
            # PDF failed to parse. lot_zoning_envelopes reports the same gap
            # from its own side as num_lots_unzoned.
            "num_without_zoning_envelopes": profiles
            - int(result["num_with_zoning_envelopes"]),
            "num_zoning_envelopes": int(result["num_zoning_envelopes"]),
            # Borough figures, identical on every row - reported once here
            # rather than left to be read out of one lot's jsonb. "suppressed"
            # is CMHC publishing nothing for the borough, which is a fact
            # about the survey and not a gap in this partition.
            "cmhc_survey_year": vacancy.get("survey_year") or "unknown",
            "cmhc_survey_period": vacancy.get("survey_period") or "unknown",
            "overall_vacancy_rate_pct": _rate_metadata(
                result["overall_vacancy_rate_pct"]
            ),
            "overall_average_rent_cad": _rate_metadata(
                result["overall_average_rent_cad"]
            ),
            "num_cmhc_vacancy_cells": len(vacancy.get("cells", [])),
            "num_cmhc_rent_cells": len(rents.get("cells", [])),
            # The cost guide's figures, reported once for the same reason the
            # CMHC ones are: they are identical on every row, and reading them
            # out of one lot's jsonb to check a run would be absurd. Which
            # publication they came from matters as much as the numbers - a
            # rate has no meaning against next quarter's without it.
            "cost_guide_last_modified": (
                costs.get("source_last_modified") or "not published"
            ),
            "condo_band": costs.get("condo_band") or "none",
            "num_cost_rates": (
                len(costs.get("parking", [])) + len(costs.get("residential", []))
            ),
            **{
                name: _rate_metadata(result[name], missing="not published")
                for name, *_ in _FLATTENED_PARKING_RATES
            },
            **{
                name: _rate_metadata(result[name], missing="not published")
                for name, _ in _FLATTENED_CONDO_RATES
            },
            # What `category` means depends entirely on this, so it travels
            # with the counts rather than only in the run's config.
            "max_built_area_m2": config.max_built_area_m2,
            "output_path": MetadataValue.path(str(path)),
        }
    )


def _read(
    partition_dir: str,
    name: str,
    *,
    asset_name: str,
    scrape_date: str,
    neighborhood: str | None = None,
) -> pd.DataFrame:
    """One upstream partition's parquet, or a `Failure` naming what to run.

    The five inputs read this way are declared deps, so a missing file means
    the partition was never materialized rather than that this asset is
    reaching for something optional - and the message that helps says which
    asset to run, the same posture `building_lot_intersections` takes.

    ``neighborhood`` is optional because the two cost snapshots are partitioned
    by date alone; naming a borough in *their* failure would send the reader
    looking for a partition key that does not exist.
    """
    path = join(partition_dir, name)
    if not filesystem(path).exists(path):
        partition = scrape_date if neighborhood is None else f"{neighborhood} {scrape_date}"
        raise Failure(
            f"{path} is missing - materialize {asset_name} for {partition} first."
        )
    return pd.read_parquet(path, storage_options=storage_options(path))


def _records(frame: pd.DataFrame) -> list[dict]:
    """``frame`` as plain JSON-able dicts, with missing values as null.

    Round-tripped through pandas' own JSON writer rather than `to_dict`: that
    one hands back `nan`, `NaT` and numpy scalars, none of which psycopg will
    adapt into jsonb, and each of which would land as the string "NaN" if it
    got as far as `json.dumps`.
    """
    if frame.empty:
        return []
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _lot_envelopes(frame: pd.DataFrame) -> list[tuple[str, dict]]:
    """`lot_zoning_envelopes` as `(lot_number, entry)` pairs for the staging.

    One pair per (lot, grid column), the zone covering most of the lot first -
    the same order `documents` is built in, and for the same reason: the first
    entry is the one a reader who only wants one will take.

    Keyed on `lot_number` rather than `lot_uid` because `lot_uid` is a
    bigserial `load_lots` mints again on every reload; see
    `postgis._stage_lot_envelopes`.
    """
    if frame.empty or "lot_number" not in frame.columns:
        return []

    order = [
        column
        for column in ("pct_of_lot", "feature_id", "column_index")
        if column in frame.columns
    ]
    ordered = (
        frame.sort_values(
            order,
            ascending=[column != "pct_of_lot" for column in order],
            kind="stable",
        )
        if order
        else frame
    )
    entry_columns = [
        column for column in ordered.columns if column not in _ENVELOPE_LOT_COLUMNS
    ]

    pairs: list[tuple[str, dict]] = []
    lot_numbers = ordered["lot_number"].tolist()
    for lot_number, entry in zip(lot_numbers, _records(ordered[entry_columns])):
        if lot_number is None or pd.isna(lot_number):
            # `building_lot_intersections` only started writing lot_number
            # into lot_features partway through; an older envelope file has
            # nothing to join on, and a row that cannot be placed is dropped
            # rather than attached to an arbitrary lot.
            continue
        pairs.append((str(lot_number), _decode_json_columns(entry)))
    return pairs


def _decode_json_columns(entry: dict) -> dict:
    """One envelope entry with its JSON-string columns turned back into JSON."""
    for column in _ENVELOPE_JSON_COLUMNS:
        value = entry.get(column)
        if isinstance(value, str):
            try:
                entry[column] = json.loads(value)
            except ValueError:
                # Leave it as the string it is: an unparseable note is still
                # worth carrying, and guessing at it would be worse.
                pass
    return entry


def _vacancy_payload(frame: pd.DataFrame) -> dict:
    """`vacancy_rates` as the object every lot of the partition carries.

    The survey provenance sits at the top and the grid travels underneath as
    `cells`, because a borough figure is the unweighted mean of its quartiers
    with most cells suppressed - the number means nothing without the year it
    was published for and the `num_quartiers` it was actually taken over.
    """
    cells = _records(
        frame[
            [
                column
                for column in (
                    "dwelling_type",
                    "bedroom_type",
                    "vacancy_rate_pct",
                    "min_vacancy_rate_pct",
                    "max_vacancy_rate_pct",
                    "num_quartiers",
                    "averaged_quartiers",
                )
                if column in frame.columns
            ]
        ]
    )
    overall = next(
        (
            cell["vacancy_rate_pct"]
            for cell in cells
            if (cell.get("dwelling_type"), cell.get("bedroom_type"))
            == _OVERALL_VACANCY
        ),
        None,
    )
    return {
        **_survey_provenance(frame),
        "overall_vacancy_rate_pct": overall,
        "num_published_cells": sum(
            1 for cell in cells if cell.get("vacancy_rate_pct") is not None
        ),
        "cells": cells,
    }


def _rents_payload(frame: pd.DataFrame) -> dict:
    """`average_rents` as the object every lot of the partition carries."""
    cells = _records(
        frame[
            [
                column
                for column in (
                    "bedroom_type",
                    "average_rent_cad",
                    "min_average_rent_cad",
                    "max_average_rent_cad",
                    "num_quartiers",
                    "averaged_quartiers",
                )
                if column in frame.columns
            ]
        ]
    )
    overall = next(
        (
            cell["average_rent_cad"]
            for cell in cells
            if cell.get("bedroom_type") == _OVERALL_BEDROOM
        ),
        None,
    )
    return {
        **_survey_provenance(frame),
        "overall_average_rent_cad": overall,
        "num_published_cells": sum(
            1 for cell in cells if cell.get("average_rent_cad") is not None
        ),
        "cells": cells,
    }


def _construction_costs_payload(
    context: AssetExecutionContext,
    *,
    residential: pd.DataFrame,
    non_residential: pd.DataFrame,
    condo_type_id: str,
) -> dict:
    """The cost guide as the object every lot of the partition carries.

    Built the same way as the two CMHC objects, and denormalised for a stronger
    version of the same reason. CMHC at least surveys neighborhoods; the Altus
    guide prices nine Canadian *markets* and publishes no geometry at all, so a
    Montreal rate is the same rate in Villeray as in Verdun and there is
    nothing whatever to join it on. One object, on every row.

    Two families travel under their own keys rather than in one flat `cells`
    list, because they are not in the same unit and mixing them is exactly the
    mistake the guide's own `perStall` flag exists to prevent: `parking` is
    dollars per stall, `residential` is dollars per square foot. `unit_flag`
    rides on every entry regardless, so a reader who takes the list whole still
    has the guide's own answer to what a figure buys.

    The six flattened keys sit at the top beside the provenance, where the
    INSERT's `->>` casts read them - the same place `overall_vacancy_rate_pct`
    sits in the vacancy object, and for the same reason: the column and the
    jsonb are then one value rather than two that can drift apart.

    A type the guide has stopped publishing is a warning and a NULL column, not
    a failure. The profile is a table about parcels; losing one of sixty rates
    should not cost a borough its cadastre, and a NULL that is logged is easier
    to act on than a partition that will not build. A *configured* band that
    was never a band is a different thing - that is a typo in the run config,
    and it fails here, before Postgres is touched.
    """
    if condo_type_id not in CONDO_TYPE_IDS:
        raise Failure(
            f"condo_type_id={condo_type_id!r} is not a published condominium / "
            f"apartment band; the guide publishes: {', '.join(CONDO_TYPE_IDS)}."
        )

    parking = _cost_entries(non_residential, PARKING_TYPE_IDS)
    condos = _cost_entries(residential, CONDO_TYPE_IDS)
    by_id = {entry["id"]: entry for entry in (*parking, *condos)}

    flattened: dict[str, object] = {
        column: (by_id.get(type_id) or {}).get(end)
        for column, type_id, end in _FLATTENED_PARKING_RATES
    }
    flattened.update(
        {
            column: (by_id.get(condo_type_id) or {}).get(end)
            for column, end in _FLATTENED_CONDO_RATES
        }
    )

    unpublished = sorted(
        {
            type_id
            for _, type_id, _ in _FLATTENED_PARKING_RATES
            if type_id not in by_id
        }
        | ({condo_type_id} if condo_type_id not in by_id else set())
    )
    if unpublished:
        # The columns named after these land NULL. Worth saying loudly: a rate
        # that quietly stopped arriving looks identical to one the proforma
        # simply never asked for.
        context.log.warning(
            "The cost guide published no rate for %s - the columns flattened "
            "from them are NULL for this partition; check "
            "estimator.PARKING_TYPE_IDS and CONDO_TYPE_IDS against the "
            "guide's own ids.",
            ", ".join(unpublished),
        )

    return {
        **_cost_provenance(residential),
        # The guide's own snapshot date, not the cadastre's. They are the same
        # partition key today; naming it separately is what keeps that a fact
        # rather than an assumption the next reader has to make.
        "cost_scrape_date": _first_value(residential, "scrape_date"),
        # Which band `condo_cost_*` was taken from. The two columns mean
        # nothing without it, so it travels on every row - the same rule
        # `max_built_area_m2` follows for `category`.
        "condo_band": condo_type_id,
        **flattened,
        "parking": parking,
        "residential": condos,
    }


def _cost_entries(frame: pd.DataFrame, type_ids: tuple[str, ...]) -> list[dict]:
    """The rows of a bronze cost snapshot for ``type_ids``, in that order.

    Ordered by the tuple rather than by the frame, so the array a reader takes
    whole is in the order this pipeline declares - ascending storeys for the
    condo bands - rather than in whichever order the publisher's file happened
    to list them in this quarter.
    """
    if frame.empty or "id" not in frame.columns:
        return []

    columns = [column for column in _COST_ENTRY_COLUMNS if column in frame.columns]
    wanted = frame[frame["id"].isin(type_ids)]
    by_id = {entry["id"]: entry for entry in _records(wanted[columns])}
    return [by_id[type_id] for type_id in type_ids if type_id in by_id]


def _cost_provenance(frame: pd.DataFrame) -> dict:
    """Which publication of the guide the rates below it came out of.

    Read off the residential snapshot alone. Both bronze assets fetch the same
    16 kB file, and on the same partition date, so their `source_last_modified`
    agrees unless the publisher happened to replace it between the two runs -
    a window measured in seconds, and one where either answer is true.
    """
    return {column: _first_value(frame, column) for column in _COST_PROVENANCE_COLUMNS}


def _first_value(frame: pd.DataFrame, column: str):
    """One column's value off the first row, as plain JSON-able data.

    Both cost snapshots write these onto every row, so the first row is the
    partition's. Routed through `_records` for the reason it exists: a
    `pd.NA` out of a nullable string column is not something psycopg will
    adapt into jsonb.
    """
    if frame.empty or column not in frame.columns:
        return None
    return _records(frame.head(1)[[column]])[0].get(column)


def _survey_provenance(frame: pd.DataFrame) -> dict:
    """Which CMHC publication the cells below it came out of.

    Both silver assets write these onto every row, so the first one is the
    partition's - and an empty frame is a partition with no survey to describe
    rather than a reason to fail: the cells will be empty too, and that reads
    as "CMHC published nothing here".
    """
    columns = ("survey_year", "survey_period", "num_quartiers_mapped")
    if frame.empty:
        return dict.fromkeys(columns)
    first = _records(frame.head(1)[[c for c in columns if c in frame.columns]])[0]
    return {column: first.get(column) for column in columns}


def _rate_metadata(
    value: float | None, *, missing: str = "suppressed"
) -> MetadataValue:
    """A published figure, or the fact that there was none to report.

    `MetadataValue.float(None)` would render as a blank cell, which reads as
    "the pipeline lost it" rather than as "the publisher does not publish it" -
    a distinction the two CMHC silver assets already make in their own
    metadata. ``missing`` is which of those two publishers is being spoken for:
    CMHC *suppresses* a cell it surveyed and will not print, while the cost
    guide simply has no row for a type, so "not published" is what the cost
    columns say.
    """
    if value is None:
        return MetadataValue.text(missing)
    return MetadataValue.float(float(value))
