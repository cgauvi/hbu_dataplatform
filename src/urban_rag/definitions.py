"""Code location for the urban_rag pipeline.

Assets are grouped and keyed by medallion layer - `bronze/`, `silver/`,
`gold/` - declared once in `urban_rag.layers` and used for both the Dagster
asset key and the prefix each asset writes under. `_assert_layers_declared`
below checks the two sets against each other at import time, so an asset
registered without a layer is a code-location load error rather than a
`KeyError` on its first materialization.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    Definitions,
    FilesystemIOManager,
    MultiPartitionKey,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from urban_rag.assets import neighborhood_features, spectrum_table_catalog
from urban_rag.bdoi_assets import neighborhood_buildings
from urban_rag.building_lots_assets import building_lot_intersections
from urban_rag.cmhc_assets import (
    average_rents,
    cmhc_rent_survey,
    cmhc_vacancy_survey,
    vacancy_rates,
)
from urban_rag.envelope_assets import lot_zoning_envelopes, zoning_grid_columns
from urban_rag.estimator_assets import (
    montreal_nonresidential_costs,
    montreal_residential_costs,
)
from urban_rag.frontage_assets import lot_frontage
from urban_rag.infolot_assets import neighborhood_lots
from urban_rag.layers import ASSET_LAYERS
from urban_rag.lot_profiles_assets import lot_profiles
from urban_rag.open_data_assets import reference_neighborhoods, street_network
from urban_rag.partitions import (
    ENABLED_NEIGHBORHOODS,
    date_partitions,
    scrape_partitions,
)
from urban_rag.rag_assets import (
    document_chunks,
    document_embeddings,
    document_index,
    linked_documents,
)
from urban_rag.resources import (
    BdoiResource,
    CmhcResource,
    EmbeddingModel,
    EstimatorResource,
    InfolotResource,
    OpenDataResource,
    ParquetStore,
    PdfCache,
    PgVectorResource,
    PostgisResource,
    RoleResource,
    SpectrumResource,
)
from urban_rag.role_assets import (
    assessment_units,
    lot_assessed_values,
    property_assessment_roll,
)
from urban_rag.setback_assets import lot_buildable_setbacks
from urban_rag.storage import DATA_ROOT, output_root
from urban_rag.street_assets import neighborhood_streets

TIMEZONE = "America/Toronto"

#: Every asset this code location registers. Named once so `Definitions` and
#: the layer check below cannot disagree about what is in it.
ASSETS = [
    # bronze
    spectrum_table_catalog,
    neighborhood_features,
    reference_neighborhoods,
    neighborhood_lots,
    neighborhood_buildings,
    cmhc_vacancy_survey,
    cmhc_rent_survey,
    street_network,
    linked_documents,
    montreal_residential_costs,
    montreal_nonresidential_costs,
    property_assessment_roll,
    # silver
    assessment_units,
    lot_assessed_values,
    vacancy_rates,
    average_rents,
    building_lot_intersections,
    neighborhood_streets,
    lot_frontage,
    document_chunks,
    document_embeddings,
    zoning_grid_columns,
    lot_zoning_envelopes,
    lot_buildable_setbacks,
    # gold
    lot_profiles,
    document_index,
]


def _assert_layers_declared() -> None:
    """Every registered asset has a layer, and every declared layer an asset.

    Both halves matter. An asset missing from `ASSET_LAYERS` has no prefix to
    write under and would fail on its first materialization rather than here;
    a name left in `ASSET_LAYERS` after its asset was renamed is a row nothing
    reads, which is how the table starts lying about the tree.
    """
    registered = {definition.key.path[-1] for definition in ASSETS}
    declared = set(ASSET_LAYERS)
    if undeclared := sorted(registered - declared):
        raise ValueError(
            f"Registered asset(s) with no layer in urban_rag.layers: "
            f"{', '.join(undeclared)}"
        )
    if unregistered := sorted(declared - registered):
        raise ValueError(
            f"urban_rag.layers declares layer(s) for asset(s) this code "
            f"location does not register: {', '.join(unregistered)}"
        )


_assert_layers_declared()

catalog_job = define_asset_job(
    "spectrum_catalog_job",
    selection=AssetSelection.assets(spectrum_table_catalog),
    partitions_def=date_partitions,
)

features_job = define_asset_job(
    "neighborhood_features_job",
    selection=AssetSelection.assets(neighborhood_features),
    partitions_def=scrape_partitions,
)


reference_neighborhoods_job = define_asset_job(
    "reference_neighborhoods_job",
    selection=AssetSelection.assets(reference_neighborhoods),
    partitions_def=date_partitions,
)

lots_job = define_asset_job(
    "neighborhood_lots_job",
    selection=AssetSelection.assets(neighborhood_lots),
    partitions_def=scrape_partitions,
)

buildings_job = define_asset_job(
    "neighborhood_buildings_job",
    selection=AssetSelection.assets(neighborhood_buildings),
    partitions_def=scrape_partitions,
)

building_lots_job = define_asset_job(
    "building_lot_intersections_job",
    selection=AssetSelection.assets(building_lot_intersections),
    partitions_def=scrape_partitions,
)

lot_profiles_job = define_asset_job(
    "lot_profiles_job",
    selection=AssetSelection.assets(lot_profiles),
    partitions_def=scrape_partitions,
)

# The geobase double is one 91 MB download for the whole island, so DATE only -
# same posture as reference_neighborhoods and the two CMHC surveys. The borough
# axis appears one asset later, in neighborhood_streets.
street_network_job = define_asset_job(
    "street_network_job",
    selection=AssetSelection.assets(street_network),
    partitions_def=date_partitions,
)

neighborhood_streets_job = define_asset_job(
    "neighborhood_streets_job",
    selection=AssetSelection.assets(neighborhood_streets),
    partitions_def=scrape_partitions,
)

# The assessment roll is one 572 MB download for the whole province, so DATE
# only - same posture as street_network and the two CMHC surveys. The bronze
# snapshot and the merge that makes it usable share a run: neither is
# borough-shaped, the merge is a few seconds over a file the snapshot has just
# written, and a day whose points landed without their characteristics is a day
# with a table nothing can read. The borough axis appears one asset later, in
# lot_assessed_values.
assessment_roll_job = define_asset_job(
    "assessment_roll_job",
    selection=AssetSelection.assets(property_assessment_roll, assessment_units),
    partitions_def=date_partitions,
)

lot_assessed_values_job = define_asset_job(
    "lot_assessed_values_job",
    selection=AssetSelection.assets(lot_assessed_values),
    partitions_def=scrape_partitions,
)

lot_frontage_job = define_asset_job(
    "lot_frontage_job",
    selection=AssetSelection.assets(lot_frontage),
    partitions_def=scrape_partitions,
)

# Its own job rather than a place in `zoning_envelopes_job`, though it reads
# that job's output: parsing a borough's grids is minutes of pypdf over
# documents a later run may no longer be able to fetch, and this is one PostGIS
# statement over what they already left in the database. Re-running the
# subtraction after a change to the margin rules should not re-parse a
# borough's PDFs to do it.
lot_buildable_setbacks_job = define_asset_job(
    "lot_buildable_setbacks_job",
    selection=AssetSelection.assets(lot_buildable_setbacks),
    partitions_def=scrape_partitions,
)

# The two CMHC surveys are read once per scrape date, not once per borough:
# there is nothing borough-shaped about either publication, and the crosswalk
# that cuts them into boroughs runs in the silver assets below.
cmhc_survey_job = define_asset_job(
    "cmhc_survey_job",
    selection=AssetSelection.assets(cmhc_vacancy_survey, cmhc_rent_survey),
    partitions_def=date_partitions,
)

# Both cost assets in one run, for the same reason the two CMHC surveys share
# one: they read the same publication, neither has a borough axis, and a day
# where one snapshot lands and the other does not is a day whose residential
# and non-residential rates came from different revisions of the guide.
construction_costs_job = define_asset_job(
    "construction_costs_job",
    selection=AssetSelection.assets(
        montreal_residential_costs, montreal_nonresidential_costs
    ),
    partitions_def=date_partitions,
)

vacancy_rates_job = define_asset_job(
    "vacancy_rates_job",
    selection=AssetSelection.assets(vacancy_rates),
    partitions_def=scrape_partitions,
)

average_rents_job = define_asset_job(
    "average_rents_job",
    selection=AssetSelection.assets(average_rents),
    partitions_def=scrape_partitions,
)

# The envelope pair, kept off the corpus job: the grids are parsed from the
# PDFs that job already downloaded, and re-reading them as tables is cheap
# enough to re-run on its own whenever the parser changes - which it will, for
# as long as the boroughs keep publishing their own templates.
zoning_envelopes_job = define_asset_job(
    "zoning_envelopes_job",
    selection=AssetSelection.assets(zoning_grid_columns, lot_zoning_envelopes),
    partitions_def=scrape_partitions,
)


rag_corpus_job = define_asset_job(
    "rag_corpus_job",
    selection=AssetSelection.assets(
        linked_documents, document_chunks, document_embeddings
    ),
    partitions_def=scrape_partitions,
)

# Separate from rag_corpus_job on purpose: the corpus is built from the city's
# servers and this one publishes it to a database that has to be reachable, and
# the second failing should not cost the first. Run it after a corpus run, or on
# its own to backfill a partition that was embedded while the store was down.
document_index_job = define_asset_job(
    "document_index_job",
    selection=AssetSelection.assets(document_index),
    partitions_def=scrape_partitions,
)


@schedule(
    job=catalog_job,
    cron_schedule="0 4 * * *",
    execution_timezone=TIMEZONE,
    description="Refresh the table catalog for today's scrape date.",
)
def daily_catalog_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(run_key=f"catalog-{scrape_date}", partition_key=scrape_date)


@schedule(
    job=features_job,
    # Twenty minutes behind the catalog, which is its upstream input.
    cron_schedule="20 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot every enabled neighborhood for today's scrape date.",
)
def daily_features_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"features-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=reference_neighborhoods_job,
    # Independent of the Spectrum assets, so it only avoids running at the
    # same minute as they do.
    cron_schedule="40 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the open-data reference neighborhoods for today.",
)
def daily_reference_neighborhoods_schedule(
    context: ScheduleEvaluationContext,
) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=f"reference-neighborhoods-{scrape_date}", partition_key=scrape_date
    )

@schedule(
    job=street_network_job,
    # Alongside reference_neighborhoods and the CMHC surveys rather than behind
    # them: the geobase double has no upstream in this pipeline. One run for the
    # whole island; the boroughs are cut out of it an hour and a half later.
    cron_schedule="50 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the island-wide geobase double for today.",
)
def daily_street_network_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(run_key=f"street-network-{scrape_date}", partition_key=scrape_date)


@schedule(
    job=assessment_roll_job,
    # Behind reference_neighborhoods (40 4), and that ordering is load-bearing
    # rather than tidy: the roll itself has no upstream here, but the
    # `assessment_units` half of this job cuts the province into borough
    # partitions against those boundaries, and a run that finds no quartiers
    # file for the date fails naming it. Otherwise alongside street_network and
    # the CMHC surveys. Kept at its own minute because the first run of a roll
    # year pulls 572 MB and unpacks 2.8 GB, and a run that long should not be
    # sharing a slot with the city's servers.
    cron_schedule="52 4 * * *",
    execution_timezone=TIMEZONE,
    description=(
        "Snapshot the property assessment roll for today, merge it, and "
        "publish each borough's units."
    ),
)
def daily_assessment_roll_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=f"assessment-roll-{scrape_date}", partition_key=scrape_date
    )


@schedule(
    job=lot_assessed_values_job,
    # After assessment_units (52 4) and neighborhood_lots (40 5), which supply
    # the two sides of the join. Behind the cadastre rather than beside it: the
    # lots are what the units are placed on, and a borough whose cadastre has
    # not landed would value nothing.
    #
    # Scheduled, unlike `lot_frontage` and `lot_profiles`: this asset also
    # upserts into silver.lot_assessed_values, but hbu_infra's
    # sql/013_silver_lot_assessed_values.sql carries no `-- requires:` header,
    # so it lands on the *first* `db.py init` - the same footing
    # `neighborhood_streets` and the CMHC pair are on.
    cron_schedule="30 6 * * *",
    execution_timezone=TIMEZONE,
    description="Total today's assessment roll onto every enabled borough's lots.",
)
def daily_lot_assessed_values_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"lot-assessed-values-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=lots_job,
    # An hour behind reference_neighborhoods, which supplies the borough
    # boundary each partition is cut with.
    cron_schedule="40 5 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the cadastral lots of every enabled neighborhood.",
)
def daily_lots_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"lots-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=buildings_job,
    # An hour behind reference_neighborhoods too, alongside neighborhood_lots
    # which shares the same borough-boundary dependency.
    cron_schedule="50 5 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the BDOI building footprints of every enabled neighborhood.",
)
def daily_buildings_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"buildings-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=building_lots_job,
    # An hour behind lots/buildings/features, which it depends on for the same
    # partition - long enough for all three to clear a borough's worth of rows.
    cron_schedule="0 7 * * *",
    execution_timezone=TIMEZONE,
    description=(
        "Recompute the building x lot and lot x feature joins for every "
        "enabled neighborhood."
    ),
)
def daily_building_lots_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"building-lots-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=cmhc_survey_job,
    # Alongside reference_neighborhoods rather than behind it: the surveys have
    # no upstream in this pipeline. One run for both, and one for the whole
    # island - the boroughs are cut out of the result an hour later.
    cron_schedule="45 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot both CMHC surveys for today's scrape date.",
)
def daily_cmhc_survey_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(run_key=f"cmhc-survey-{scrape_date}", partition_key=scrape_date)


@schedule(
    job=construction_costs_job,
    # Alongside the CMHC surveys and reference_neighborhoods rather than behind
    # anything: the cost guide has no upstream in this pipeline, and no borough
    # axis to wait for one. Kept at its own minute so a publisher that has
    # moved the file fails one small run rather than sharing a run with the
    # city's servers.
    cron_schedule="47 4 * * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the Montreal construction cost rates for today.",
)
def daily_construction_costs_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        run_key=f"construction-costs-{scrape_date}", partition_key=scrape_date
    )


@schedule(
    job=vacancy_rates_job,
    # Behind cmhc_survey_job, which is now its upstream: the crosswalk is
    # applied to that day's snapshot rather than to a fresh download.
    cron_schedule="55 5 * * *",
    execution_timezone=TIMEZONE,
    description="Cut today's CMHC vacancy survey into every enabled borough.",
)
def daily_vacancy_rates_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"vacancy-rates-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=average_rents_job,
    # Same upstream, kept at a different minute so one borough's crosswalk
    # failure is one small run at a time.
    cron_schedule="58 5 * * *",
    execution_timezone=TIMEZONE,
    description="Cut today's CMHC rent survey into every enabled borough.",
)
def daily_average_rents_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"average-rents-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=neighborhood_streets_job,
    # After street_network (50 4) and reference_neighborhoods (40 4), which
    # supply the island-wide layer and the boundary it is cut with. Ahead of
    # daily_building_lots_schedule rather than behind the cadastre: the two
    # share no input.
    cron_schedule="20 6 * * *",
    execution_timezone=TIMEZONE,
    description="Cut today's geobase double into every enabled borough.",
)
def daily_neighborhood_streets_schedule(context: ScheduleEvaluationContext):
    scrape_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    for neighborhood in ENABLED_NEIGHBORHOODS:
        yield RunRequest(
            run_key=f"neighborhood-streets-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


# Every scheduled silver asset above now publishes to Postgres as well as to
# the tree - `neighborhood_streets` to `silver.neighborhood_streets`, the two
# CMHC assets to `silver.vacancy_rates`/`silver.average_rents` and the quartier
# tables beside them. All of those are created by hbu_infra files with no
# `-- requires:` header, so they land on the *first* `db.py init` and a
# database that has had one is enough. Until it has, those schedules fail every
# morning naming the file to apply - which is the same failure `lot_frontage`
# and `lot_profiles` are kept off the schedules for, at a much lower cost: the
# parquet is written before the publish, so a re-run after `db.py init` is a
# load rather than a scrape.

# No schedule for `lot_frontage`, and for the same reason as `lot_profiles`
# below: `silver.lot_frontage` is hbu_infra's to create, and until
# sql/008_silver_lot_frontage.sql has been applied to the database a nightly
# run fails naming it every morning. The file exists in that repo; what is
# outstanding is `db.py init` against the target database. It is registered and
# has a job, so it appears in the lineage and can be run by hand the moment the
# table lands - see `lot_frontage_job` and `make frontage`. Add the schedule
# then, at 30 7, behind daily_building_lots_schedule which loads the cadastre
# it reads.

# No schedule for `lot_buildable_setbacks` either, and for the same reason as
# `lot_frontage` above: `silver.lot_buildable_setbacks` is hbu_infra's to
# create, and until sql/015_silver_lot_buildable_setbacks.sql has been applied
# a nightly run fails naming it every morning. It is registered and has a job -
# see `lot_buildable_setbacks_job` and `make setbacks`. Add the schedule when
# the table lands, at 35 7: behind `lot_frontage`, whose street edges it sorts
# a boundary against, and behind `zoning_envelopes_job`, which supplies the
# margins it subtracts. Both of those have to run first and neither is
# scheduled yet, so the ordering is a matter for whoever adds all three.

# No schedule for `lot_profiles`, unlike every other asset here, and not an
# oversight: it reads two relations hbu_infra has to create first.
# sql/009_gold_lot_profiles.sql creates the table it writes into, and
# sql/006_lot_documents.sql creates the `rag.lot_documents` view it takes the
# document columns from - and that second file carries a `-- requires:
# rag.chunks` header, so `db.py init` skips it on a database that has never
# held a corpus and it only lands on the *next* init after `document_index` has
# run. Both files exist in that repo; what is outstanding is `db.py init`
# against the target database, twice. `compute_lot_profiles` checks for both up
# front and names the file to apply, so the failure says what to do rather than
# `relation "gold.lot_profiles" does not exist`. It is registered and has a job,
# so it appears in the lineage and can be run by hand the moment the relations
# land - see `lot_profiles_job` and `make lot-profiles`. Add the schedule then,
# at 40 7, behind lot_frontage which supplies the frontage it pivots - and
# behind `zoning_envelopes_job`, which has no schedule of its own either. Three
# of this asset's inputs come from the tree rather than from Postgres
# (lot_zoning_envelopes, vacancy_rates, average_rents), and it fails naming the
# one that is missing rather than writing a partition without it.


defs = Definitions(
    assets=ASSETS,
    jobs=[
        catalog_job,
        features_job,
        reference_neighborhoods_job,
        lots_job,
        buildings_job,
        building_lots_job,
        lot_profiles_job,
        street_network_job,
        neighborhood_streets_job,
        assessment_roll_job,
        lot_assessed_values_job,
        lot_frontage_job,
        zoning_envelopes_job,
        lot_buildable_setbacks_job,
        cmhc_survey_job,
        construction_costs_job,
        vacancy_rates_job,
        average_rents_job,
        rag_corpus_job,
        document_index_job,
    ],
    schedules=[
        daily_catalog_schedule,
        daily_features_schedule,
        daily_reference_neighborhoods_schedule,
        daily_street_network_schedule,
        daily_assessment_roll_schedule,
        daily_lot_assessed_values_schedule,
        daily_lots_schedule,
        daily_buildings_schedule,
        daily_building_lots_schedule,
        daily_cmhc_survey_schedule,
        daily_construction_costs_schedule,
        daily_vacancy_rates_schedule,
        daily_average_rents_schedule,
        daily_neighborhood_streets_schedule,
    ],
    resources={
        "spectrum": SpectrumResource(),
        "open_data": OpenDataResource(),
        "infolot": InfolotResource(),
        # One tree for every asset: `<root>/<asset>/<date>[/<neighborhood>]`,
        # where the root is `s3://<S3_BUCKET>` when that is set and `data/`
        # otherwise.
        "store": ParquetStore(root_dir=output_root()),
        "pdf_cache": PdfCache(
            # Outside the partition tree on purpose: a published
            # resolution never changes, so every scrape date reuses it.
            # Always local: it is a cache keyed by URL, not pipeline output.
            cache_dir=str(DATA_ROOT / "cache" / "pdf"),
        ),
        "bdoi": BdoiResource(
            # Same posture as pdf_cache: a published BDOI extract never
            # changes, so it is cached once, outside the partition tree, and
            # always local.
            cache_dir=str(DATA_ROOT / "cache" / "bdoi"),
        ),
        # No cache_dir, unlike bdoi/cmhc/pdf_cache below: the cost guide is one
        # 16 kB script that its publisher can revise on any day, so each scrape
        # date fetches it again rather than reusing a copy.
        "estimator": EstimatorResource(),
        "cmhc": CmhcResource(
            # Same posture as bdoi/pdf_cache: a published survey year is
            # final, so the workbook is cached once, outside the partition
            # tree, and always local.
            cache_dir=str(DATA_ROOT / "cache" / "cmhc"),
        ),
        "role": RoleResource(
            # Same posture again, and by far the largest of these caches: a
            # published roll year is final, so the 572 MB archive and the
            # 2.8 GB GeoPackage unpacked beside it are fetched once and shared
            # by every scrape date. Always local - the GeoPackage has to be on
            # a filesystem to be read at all, since SQLite reads it by seeking.
            cache_dir=str(DATA_ROOT / "cache" / "role"),
        ),
        "embedding_model": EmbeddingModel(),
        # The query side's store. Every field defaults to its URBAN_RAG_PG_*
        # variable, so the endpoint and the credentials live in the environment
        # (or in .env) rather than here - and `urban-rag --backend postgres`
        # reads the same ones.
        "pgvector": PgVectorResource(),
        # The plain PostGIS tables (rag.lots/rag.buildings/rag.features and
        # the joins between them),
        # same database as "pgvector" and configured the same way - every
        # field defaults to its URBAN_RAG_PG_* variable.
        "postgis": PostgisResource(),
        # Every asset writes its own parquet into `store` and returns a
        # MaterializeResult, so nothing of consequence passes through here;
        # it is pinned rather than left to DAGSTER_HOME only so a run from the
        # CLI and a run from the UI agree on where Dagster keeps its own
        # bookkeeping. Always local: there is no S3 backend configured for it.
        "io_manager": FilesystemIOManager(base_dir=str(DATA_ROOT / "dagster_io")),
    },
)
