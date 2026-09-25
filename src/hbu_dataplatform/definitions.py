"""Code location for the hbu_dataplatform pipeline.

Assets are grouped and keyed by medallion layer - `bronze/`, `silver/`,
`gold/` - declared once in `hbu_dataplatform.core.layers` and used for both the Dagster
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

from hbu_dataplatform.sources.addresses.assets import (
    lot_addresses,
    neighborhood_addresses,
)
from hbu_dataplatform.map.aggregate_assets import map_cell_aggregates
from hbu_dataplatform.zoning.features_assets import (
    neighborhood_features,
    spectrum_table_catalog,
)
from hbu_dataplatform.sources.bdoi.assets import neighborhood_buildings
from hbu_dataplatform.cadastre.building_lots_assets import building_lot_intersections
from hbu_dataplatform.cadastre.cadastre_assets import neighborhood_cadastre
from hbu_dataplatform.sources.cmhc.assets import (
    average_rents,
    cmhc_rent_survey,
    cmhc_vacancy_survey,
    vacancy_rates,
)
from hbu_dataplatform.hbu.comparables_assets import lot_assessment_comparables
from hbu_dataplatform.sources.cubf.assets import cubf_use_codes
from hbu_dataplatform.cities.montreal.rents.assets import (
    commercial_rent_index,
    commercial_rents,
    montreal_commercial_rents,
)
from hbu_dataplatform.zoning.envelope_assets import (
    lot_zoning_envelopes,
    zoning_grid_columns,
)
from hbu_dataplatform.zoning.zone_piece_assets import lot_zone_pieces
from hbu_dataplatform.cities.montreal.costs.assets import (
    montreal_nonresidential_costs,
    montreal_residential_costs,
)
from hbu_dataplatform.cadastre.frontage_assets import lot_frontage
from hbu_dataplatform.hbu.opportunity_assets import lot_investment_opportunities
from hbu_dataplatform.hbu.hbu_assets import (
    lot_development_programs,
    lot_highest_best_use,
    lot_redevelopment_gap,
)
from hbu_dataplatform.sources.infolot.assets import neighborhood_lots
from hbu_dataplatform.partitions.guards import guards_scrape_month
from hbu_dataplatform.core.layers import ASSET_LAYERS, Layer, layer_of
from hbu_dataplatform.hbu.lot_profiles_assets import lot_profiles
from hbu_dataplatform.hbu.massing_assets import lot_building_massing
from hbu_dataplatform.boundaries.assets import reference_neighborhoods
from hbu_dataplatform.sources.rqtt.assets import street_network
from hbu_dataplatform.partitions.axes import (
    TILE_DIMENSION,
    date_partitions,
    enabled_neighborhoods,
    scrape_partitions,
    tile_partitions,
    tile_scrape_partitions,
)
from hbu_dataplatform.cities.quebec_city.council.assets import (
    council_minutes,
    council_minutes_documents,
    council_planning_items,
)
from hbu_dataplatform.rag.assets import (
    document_chunks,
    document_embeddings,
    document_index,
    linked_documents,
)
from hbu_dataplatform.sources.addresses.resources import AdressesQuebecResource
from hbu_dataplatform.sources.bdoi.resources import BdoiResource
from hbu_dataplatform.sources.cmhc.resources import CmhcResource
from hbu_dataplatform.cities.quebec_city.resources import (
    CouncilMinutesResource,
    QuebecZoningResource,
)
from hbu_dataplatform.cities.montreal.resources import (
    CrspiResource,
    EstimatorResource,
    MarketBeatResource,
    OpenDataResource,
    SpectrumResource,
)
from hbu_dataplatform.sources.cubf.resources import CubfResource
from hbu_dataplatform.rag.resources import EmbeddingModel, PdfCache, PgVectorResource
from hbu_dataplatform.sources.infolot.resources import InfolotResource
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.sources.donnees_quebec import QuebecOpenDataResource
from hbu_dataplatform.sources.rfu.resources import RfuResource
from hbu_dataplatform.sources.roll.resources import RoleResource
from hbu_dataplatform.sources.rqtt.resources import RqttResource
from hbu_dataplatform.cities.saguenay.resources import SaguenayZoningResource
from hbu_dataplatform.sources.rfu.assets import uniformized_property_wealth
from hbu_dataplatform.sources.roll.assets import (
    assessment_units,
    lot_assessed_values,
    property_assessment_roll,
)
from hbu_dataplatform.zoning.setback_assets import lot_buildable_setbacks
from hbu_dataplatform.core.storage import DATA_ROOT, output_root
from hbu_dataplatform.sources.rqtt.assets import neighborhood_streets
from hbu_dataplatform.map.tile_assets import map_tiles_asset

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
    neighborhood_addresses,
    linked_documents,
    council_minutes,
    council_minutes_documents,
    montreal_residential_costs,
    montreal_nonresidential_costs,
    property_assessment_roll,
    cubf_use_codes,
    uniformized_property_wealth,
    montreal_commercial_rents,
    commercial_rent_index,
    # silver
    assessment_units,
    lot_assessed_values,
    lot_assessment_comparables,
    commercial_rents,
    vacancy_rates,
    average_rents,
    neighborhood_cadastre,
    building_lot_intersections,
    neighborhood_streets,
    lot_addresses,
    lot_frontage,
    document_chunks,
    document_embeddings,
    council_planning_items,
    zoning_grid_columns,
    lot_zone_pieces,
    lot_zoning_envelopes,
    lot_buildable_setbacks,
    lot_development_programs,
    # gold
    lot_profiles,
    lot_highest_best_use,
    lot_redevelopment_gap,
    lot_investment_opportunities,
    lot_building_massing,
    map_cell_aggregates,
    map_tiles_asset,
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
            f"Registered asset(s) with no layer in hbu_dataplatform.core.layers: "
            f"{', '.join(undeclared)}"
        )
    if unregistered := sorted(declared - registered):
        raise ValueError(
            f"hbu_dataplatform.core.layers declares layer(s) for asset(s) this code "
            f"location does not register: {', '.join(unregistered)}"
        )


_assert_layers_declared()


def _assert_bronze_assets_guarded() -> None:
    """Every bronze asset refuses to fetch into a month it is not in.

    The guard has to be spelled at each asset - it is the only place every way
    of launching a run converges - so this is what keeps "spell it at each
    asset" from meaning "remember to". A bronze asset registered without
    `guard_current_scrape_month` is a code-location load error here, rather
    than a fabricated snapshot noticed a quarter later.

    Silver and gold are skipped on purpose: they recompute from bronze parquet
    already on disk, so backfilling them is a feature. See `hbu_dataplatform.partitions.guards`.
    """
    unguarded = sorted(
        definition.key.path[-1]
        for definition in ASSETS
        if layer_of(definition.key.path[-1]) is Layer.BRONZE
        and not guards_scrape_month(definition)
    )
    if unguarded:
        raise ValueError(
            f"Bronze asset(s) registered without @guard_current_scrape_month: "
            f"{', '.join(unguarded)}. Bronze records what a publisher returned "
            f"now, so it must refuse a past partition - see hbu_dataplatform.partitions.guards."
        )


_assert_bronze_assets_guarded()

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

# The hop from the borough axis to the tile axis. A borough is what a
# publisher answers for, so the three bronze snapshots above are per borough;
# a tile is what a computation is run over, so everything from
# `building_lot_intersections` down is per cell of the cut. This is the one
# job on the borough axis that writes to Postgres: it lands the borough's
# cadastre in `rag.*` with each row's `cell_key` and `cell_partition`, and
# reports which tiles it touched - which is the list of tile runs that have
# to follow it, because a reload remints `lot_uid` and cascades into every
# one of them.
cadastre_job = define_asset_job(
    "neighborhood_cadastre_job",
    selection=AssetSelection.assets(neighborhood_cadastre),
    partitions_def=scrape_partitions,
)

building_lots_job = define_asset_job(
    "building_lot_intersections_job",
    selection=AssetSelection.assets(building_lot_intersections),
    partitions_def=tile_scrape_partitions,
)

lot_profiles_job = define_asset_job(
    "lot_profiles_job",
    selection=AssetSelection.assets(lot_profiles),
    partitions_def=tile_scrape_partitions,
)

# The RQTT is one 390 MB download for the whole province, so DATE only -
# same posture as reference_neighborhoods and the two CMHC surveys. The tile
# axis appears one asset later, in neighborhood_streets, which keeps the
# sides whose midpoint falls in the cell - whole, not clipped.
street_network_job = define_asset_job(
    "street_network_job",
    selection=AssetSelection.assets(street_network),
    partitions_def=date_partitions,
)

neighborhood_streets_job = define_asset_job(
    "neighborhood_streets_job",
    selection=AssetSelection.assets(neighborhood_streets),
    partitions_def=tile_scrape_partitions,
)

# The assessment roll is one 572 MB download for the whole province, so DATE
# only - same posture as street_network and the two CMHC surveys. The bronze
# snapshot and the merge that makes it usable share a run: neither is
# borough-shaped, the merge is a few seconds over a file the snapshot has just
# written, and a day whose points landed without their characteristics is a day
# with a table nothing can read. The borough axis appears in the same run, in
# `assessment_units`' per-borough partitions; the tile axis one asset later, in
# lot_assessed_values.
#
# `cubf_use_codes` rides along for the same reason rather than getting a
# schedule of its own. It is a 185 kB spreadsheet against the roll's 572 MB and
# nothing about it is borough-shaped either, but it is a genuine input here and
# not a companion: `assessment_units` looks the MEFQ's text onto every unit
# from it, so a day whose codebook did not land is a day whose units carry a
# use code and no words. Ordered ahead of the merge by the dependency, not by
# this list.
assessment_roll_job = define_asset_job(
    "assessment_roll_job",
    selection=AssetSelection.assets(
        property_assessment_roll, cubf_use_codes, assessment_units
    ),
    partitions_def=date_partitions,
)

lot_assessed_values_job = define_asset_job(
    "lot_assessed_values_job",
    selection=AssetSelection.assets(lot_assessed_values),
    partitions_def=tile_scrape_partitions,
)

# Both commercial-rent snapshots in one run, for the reason the two CMHC
# surveys share one and the two cost snapshots do: neither has a borough axis,
# and the silver asset behind them needs both. A day where the MarketBeats
# landed and the index did not is a day whose rents cannot be carried to the
# quarter being scraped.
commercial_rent_sources_job = define_asset_job(
    "commercial_rent_sources_job",
    selection=AssetSelection.assets(montreal_commercial_rents, commercial_rent_index),
    partitions_def=date_partitions,
)

commercial_rents_job = define_asset_job(
    "commercial_rents_job",
    selection=AssetSelection.assets(commercial_rents),
    partitions_def=scrape_partitions,
)

# Its own job rather than a place in `lot_assessed_values_job`, though it reads
# that job's output and re-derives the placement behind it: the neighbour
# search is a pass over the cell whose whole shape is set by config nothing
# upstream shares - k, the radius, the weights - and re-scoring the cell
# after a change to those should not re-total the roll to do it. The same split
# `lot_buildable_setbacks_job` makes behind `zoning_envelopes_job`.
lot_assessment_comparables_job = define_asset_job(
    "lot_assessment_comparables_job",
    selection=AssetSelection.assets(lot_assessment_comparables),
    partitions_def=tile_scrape_partitions,
)

lot_frontage_job = define_asset_job(
    "lot_frontage_job",
    selection=AssetSelection.assets(lot_frontage),
    partitions_def=tile_scrape_partitions,
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
    partitions_def=tile_scrape_partitions,
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

# One row per Quebec organisme municipal, so DATE only - the same posture as
# the roll and the two CMHC surveys. It is the roll's companion rather than its
# dependant: the roll says what a lot is worth on paper and this says what to
# multiply that by, but the two are separate publications on separate cadences
# and neither run needs the other's output.
uniformized_property_wealth_job = define_asset_job(
    "uniformized_property_wealth_job",
    selection=AssetSelection.assets(uniformized_property_wealth),
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

# The pieces, ahead of the envelopes that join them and on their own. It is one
# PostGIS pass over tables already loaded and GiST-indexed - about five seconds
# on a borough - and its config is about *where a site is*, not about what may
# be built on one: how much of a parcel a zone has to cover, and how far off a
# clipped boundary a street edge may sit. Re-cutting a borough after a change
# to those should not re-parse its grids to do it, which is the same split
# `lot_buildable_setbacks_job` makes behind `zoning_envelopes_job`.
lot_zone_pieces_job = define_asset_job(
    "lot_zone_pieces_job",
    selection=AssetSelection.assets(lot_zone_pieces),
    partitions_def=tile_scrape_partitions,
)

# The addresses, in two jobs rather than one. The fetch is a few minutes of
# paging against a provincial server and the join is a point-in-polygon over
# rows already in Postgres, so re-running the join after a change to the snap
# tolerance should not re-scrape a hundred thousand points to do it - the same
# split `lot_zone_pieces_job` makes behind `zoning_envelopes_job`.
neighborhood_addresses_job = define_asset_job(
    "neighborhood_addresses_job",
    selection=AssetSelection.assets(neighborhood_addresses),
    partitions_def=scrape_partitions,
)

lot_addresses_job = define_asset_job(
    "lot_addresses_job",
    selection=AssetSelection.assets(lot_addresses),
    partitions_def=tile_scrape_partitions,
)

# The grids, kept off the corpus job: they are parsed from the PDFs that job
# already downloaded, and re-reading them as tables is cheap enough to re-run
# on its own whenever the parser changes - which it will, for as long as the
# boroughs keep publishing their own templates. On the borough axis, because a
# grid is a property of a by-law and a by-law is a borough's.
zoning_grid_columns_job = define_asset_job(
    "zoning_grid_columns_job",
    selection=AssetSelection.assets(zoning_grid_columns),
    partitions_def=scrape_partitions,
)

# The envelopes, on the tile axis behind them: a join of the cell's zone pieces
# to whichever boroughs' grids those pieces fall under. Two jobs where there
# was one because the two assets no longer share a partition key.
zoning_envelopes_job = define_asset_job(
    "zoning_envelopes_job",
    selection=AssetSelection.assets(lot_zoning_envelopes),
    partitions_def=tile_scrape_partitions,
)


# The solve, on its own. It is the expensive step of the three - a borough is
# tens of thousands of CP-SAT models - and it is the one whose config is about
# the *building* rather than about the data: stalls per dwelling, the cost per
# square foot, what a storey stands. Re-designing that building should re-solve
# without re-reading the assessment lineage, and re-reading the assessment
# lineage should not re-solve. The same split `lot_assessment_comparables_job`
# makes behind `lot_assessed_values_job`.
lot_development_programs_job = define_asset_job(
    "lot_development_programs_job",
    selection=AssetSelection.assets(lot_development_programs),
    partitions_def=tile_scrape_partitions,
)

# The two gold assets in one run, and not with the solve above: choosing among
# the programs is a sort and comparing them against the roll is a join, so the
# pair is seconds over what the solve already wrote. Splitting them further
# would be two jobs to run in sequence for no saving; keeping them with the
# solve would mean re-solving a borough to re-read it at a different expense
# ratio, which is `lot_redevelopment_gap`'s single largest lever and is not
# even this pipeline's config - it is whatever `comparables` was run at.
lot_hbu_job = define_asset_job(
    "lot_hbu_job",
    selection=AssetSelection.assets(lot_highest_best_use, lot_redevelopment_gap),
    partitions_def=tile_scrape_partitions,
)

# Its own job rather than a place in `lot_hbu_job`, though it reads that job's
# output: this one is geometry rather than arithmetic - a rectangle fitted per
# lot against the setback envelope - and its config is about the *search* (the
# aspect ratios, how finely to look for a placement) rather than about the
# building or the money. Re-drawing a borough at a different set of ratios
# should not re-choose every lot's envelope to do it, and it costs about a
# minute where the two it sits behind cost seconds.
lot_massing_job = define_asset_job(
    "lot_massing_job",
    selection=AssetSelection.assets(lot_building_massing),
    partitions_def=tile_scrape_partitions,
)

# Its own job rather than a place in `lot_hbu_job`, though it reads that
# job's output: this is a classification and two sorts over one parquet
# file, and re-screening a borough at a different mixed-use threshold or a
# different land factor should not re-solve it. The same split
# `lot_redevelopment_gap` makes behind `lot_highest_best_use`.
# Its own job, and the last one in the chain: this reads the two gold tables
# above plus the streets and the working set, so it can only be right once they
# are. Kept apart from all of them for the reason the split between them
# already draws - re-dissolving a borough onto the tile grid should not re-solve
# it - and because this is the one job whose output is a *rendering* rather than
# an answer. A different `ZOOM_OFFSET` would be a different map at the same
# findings, and that is not a reason to re-run the solver.
map_aggregates_job = define_asset_job(
    "map_aggregates_job",
    selection=AssetSelection.assets(map_cell_aggregates),
    partitions_def=scrape_partitions,
)

# The very last job in the chain, and the one whose output is not a table:
# every map layer rendered as vector tiles and packed into a PMTiles archive
# per layer, which hbu_rag_map reads straight off S3. Kept apart from the
# aggregates job it sits behind for the reason that job gives - re-rendering a
# borough's tiles after a style-relevant column changed should not re-dissolve
# it - and because this one is minutes of PostGIS per borough where the other
# is one.
map_tiles_job = define_asset_job(
    "map_tiles_job",
    selection=AssetSelection.assets(map_tiles_asset),
    partitions_def=scrape_partitions,
)

lot_opportunities_job = define_asset_job(
    "lot_opportunities_job",
    selection=AssetSelection.assets(lot_investment_opportunities),
    partitions_def=tile_scrape_partitions,
)

rag_corpus_job = define_asset_job(
    "rag_corpus_job",
    selection=AssetSelection.assets(
        linked_documents, document_chunks, document_embeddings
    ),
    partitions_def=scrape_partitions,
)

# The conseils de quartier minutes, the documents they trail to, and the
# planning items read out of both. Its own job rather than part of the corpus:
# it reads a Quebec City institution the other two cities do not have, and
# nothing downstream of the corpus depends on it yet.
council_minutes_job = define_asset_job(
    "council_minutes_job",
    selection=AssetSelection.assets(
        council_minutes, council_minutes_documents, council_planning_items
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


def _scrape_month(context: ScheduleEvaluationContext) -> str:
    """The partition key a schedule tick belongs to.

    `date_partitions` is monthly, so a key is always the first of its month.
    The crons below happen to fire on the 1st, but that is a choice about when
    to scrape rather than what makes the key valid - a tick on any other day
    has to land on the same partition, and `.replace(day=1)` is what says so.
    """
    return context.scheduled_execution_time.replace(day=1).strftime("%Y-%m-%d")


def _tile_requests(prefix: str, scrape_date: str):
    """One `RunRequest` per cell of the cut, for a job on the tile axis.

    The tile axis is static - every cell of `hbu_dataplatform.core.tile_cut.CUT` - so
    there is no instance to ask, unlike `enabled_neighborhoods`. A cell no
    borough has been loaded into yet runs and computes over nothing, which is
    a cheap run rather than a wrong one; the cut only ever holds cells that
    had lots when it was seeded.
    """
    for tile in tile_partitions.get_partition_keys():
        yield RunRequest(
            run_key=f"{prefix}-{tile}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, TILE_DIMENSION: tile}
            ),
        )


@schedule(
    job=catalog_job,
    cron_schedule="0 4 1 * *",
    execution_timezone=TIMEZONE,
    description="Refresh the table catalog for this month's scrape date.",
)
def monthly_catalog_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = _scrape_month(context)
    return RunRequest(run_key=f"catalog-{scrape_date}", partition_key=scrape_date)


@schedule(
    job=features_job,
    # Twenty minutes behind the catalog, which is its upstream input.
    cron_schedule="20 4 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot every enabled neighborhood for this month's scrape date.",
)
def monthly_features_schedule(context: ScheduleEvaluationContext):
    scrape_date = _scrape_month(context)
    for neighborhood in enabled_neighborhoods(context.instance):
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
    cron_schedule="40 4 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the open-data reference neighborhoods for this month.",
)
def monthly_reference_neighborhoods_schedule(
    context: ScheduleEvaluationContext,
) -> RunRequest:
    scrape_date = _scrape_month(context)
    return RunRequest(
        run_key=f"reference-neighborhoods-{scrape_date}", partition_key=scrape_date
    )

@schedule(
    job=street_network_job,
    # Alongside reference_neighborhoods and the CMHC surveys rather than behind
    # them: street_network's only upstream is reference_neighborhoods, which is
    # on the same date axis and runs before it. One run for the
    # whole island; the boroughs are cut out of it an hour and a half later.
    cron_schedule="50 4 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the RQTT road network for this month.",
)
def monthly_street_network_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = _scrape_month(context)
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
    cron_schedule="52 4 1 * *",
    execution_timezone=TIMEZONE,
    description=(
        "Snapshot the property assessment roll for this month, merge it, and "
        "publish each borough's units."
    ),
)
def monthly_assessment_roll_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = _scrape_month(context)
    return RunRequest(
        run_key=f"assessment-roll-{scrape_date}", partition_key=scrape_date
    )


@schedule(
    job=lot_assessed_values_job,
    # After assessment_units (52 4) and neighborhood_cadastre (0 7), which
    # supply the two sides of the join. Behind the cadastre rather than beside
    # it: the lots are read out of rag.lots now, not out of a borough's
    # parquet, and a cell whose boroughs have not landed would value nothing.
    #
    # Scheduled, unlike `lot_frontage` and `lot_profiles`: this asset also
    # upserts into silver.lot_assessed_values, but hbu_infra's
    # sql/013_silver_lot_assessed_values.sql carries no `-- requires:` header,
    # so it lands on the *first* `db.py init` - the same footing
    # `neighborhood_streets` and the CMHC pair are on.
    cron_schedule="40 7 1 * *",
    execution_timezone=TIMEZONE,
    description="Total this month's assessment roll onto the lots of every cell of the cut.",
)
def monthly_lot_assessed_values_schedule(context: ScheduleEvaluationContext):
    yield from _tile_requests("lot-assessed-values", _scrape_month(context))


@schedule(
    job=commercial_rent_sources_job,
    # Alongside the other sources with no upstream here. Two PDFs and a 14 kB
    # table, so it costs nothing and can sit early.
    cron_schedule="47 4 1 * *",
    execution_timezone=TIMEZONE,
    description=(
        "Snapshot the MarketBeats and the commercial rent index for this month."
    ),
)
def monthly_commercial_rent_sources_schedule(
    context: ScheduleEvaluationContext,
) -> RunRequest:
    scrape_date = _scrape_month(context)
    return RunRequest(
        run_key=f"commercial-rent-sources-{scrape_date}", partition_key=scrape_date
    )


@schedule(
    job=commercial_rents_job,
    # Behind the two snapshots above, and ahead of lot_assessment_comparables
    # at 40 6 which prices every square foot of commercial floor against what
    # this resolves.
    cron_schedule="10 6 1 * *",
    execution_timezone=TIMEZONE,
    description="Resolve each borough's retail, office and industrial rent.",
)
def monthly_commercial_rents_schedule(context: ScheduleEvaluationContext):
    scrape_date = _scrape_month(context)
    for neighborhood in enabled_neighborhoods(context.instance):
        yield RunRequest(
            run_key=f"commercial-rents-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=lot_assessment_comparables_job,
    # Forty minutes behind lot_assessed_values (40 7), which supplies the lot
    # geometry and the two totals - and not ten, as it used to be: the pool a
    # cell's comparables are drawn from is every valued lot of the snapshot in
    # reach, which is other cells' runs of that job, so all of them have to
    # have landed rather than just this cell's. Well behind the CMHC pair
    # (55 5, 58 5), which supplies the rent and the vacancy the income is
    # priced at. Last of the assessment lineage, and the one gold reads after
    # it.
    #
    # Scheduled for the reason lot_assessed_values is: hbu_infra's
    # sql/016_silver_lot_assessment_comparables.sql carries no `-- requires:`
    # header, so the table lands on the first `db.py init` rather than waiting
    # on a corpus the way sql/006 does.
    cron_schedule="20 8 1 * *",
    execution_timezone=TIMEZONE,
    description=(
        "Price this month's roll onto the lots of every cell of the cut and find "
        "each lot's comparables."
    ),
)
def monthly_lot_assessment_comparables_schedule(context: ScheduleEvaluationContext):
    yield from _tile_requests("lot-comparables", _scrape_month(context))


@schedule(
    job=lots_job,
    # An hour behind reference_neighborhoods, which supplies the borough
    # boundary each partition is cut with.
    cron_schedule="40 5 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the cadastral lots of every enabled neighborhood.",
)
def monthly_lots_schedule(context: ScheduleEvaluationContext):
    scrape_date = _scrape_month(context)
    for neighborhood in enabled_neighborhoods(context.instance):
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
    cron_schedule="50 5 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the BDOI building footprints of every enabled neighborhood.",
)
def monthly_buildings_schedule(context: ScheduleEvaluationContext):
    scrape_date = _scrape_month(context)
    for neighborhood in enabled_neighborhoods(context.instance):
        yield RunRequest(
            run_key=f"buildings-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=cadastre_job,
    # An hour behind lots/buildings/features, which it depends on for the same
    # partition - long enough for all three to clear a borough's worth of rows.
    cron_schedule="0 7 1 * *",
    execution_timezone=TIMEZONE,
    description=(
        "Land every enabled borough's cadastre in rag.lots/buildings/features, "
        "addressed to the cells of the cut."
    ),
)
def monthly_cadastre_schedule(context: ScheduleEvaluationContext):
    scrape_date = _scrape_month(context)
    for neighborhood in enabled_neighborhoods(context.instance):
        yield RunRequest(
            run_key=f"cadastre-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=building_lots_job,
    # Twenty minutes behind the cadastre (0 7): the joins read rag.lots, and a
    # cell straddling two boroughs needs both of them landed. The first job on
    # the tile axis, so the first that runs once per cell of the cut rather
    # than once per borough.
    cron_schedule="20 7 1 * *",
    execution_timezone=TIMEZONE,
    description=(
        "Recompute the building x lot and lot x feature joins for every cell "
        "of the cut."
    ),
)
def monthly_building_lots_schedule(context: ScheduleEvaluationContext):
    yield from _tile_requests("building-lots", _scrape_month(context))


@schedule(
    job=cmhc_survey_job,
    # Alongside reference_neighborhoods rather than behind it: the surveys have
    # no upstream in this pipeline. One run for both, and one for the whole
    # island - the boroughs are cut out of the result an hour later.
    cron_schedule="45 4 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot both CMHC surveys for this month's scrape date.",
)
def monthly_cmhc_survey_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    scrape_date = _scrape_month(context)
    return RunRequest(run_key=f"cmhc-survey-{scrape_date}", partition_key=scrape_date)


@schedule(
    job=construction_costs_job,
    # Alongside the CMHC surveys and reference_neighborhoods rather than behind
    # anything: the cost guide has no upstream in this pipeline, and no borough
    # axis to wait for one. Kept at its own minute so a publisher that has
    # moved the file fails one small run rather than sharing a run with the
    # city's servers.
    cron_schedule="47 4 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the Montreal construction cost rates for this month.",
)
def monthly_construction_costs_schedule(
    context: ScheduleEvaluationContext,
) -> RunRequest:
    scrape_date = _scrape_month(context)
    return RunRequest(
        run_key=f"construction-costs-{scrape_date}", partition_key=scrape_date
    )


@schedule(
    job=uniformized_property_wealth_job,
    # Ahead of the roll (52 4) rather than behind it, though nothing forces the
    # order: this is a 275 kB CSV against the roll's 572 MB, so a day on which
    # both are due gets the cheap one out of the way first, and a morning where
    # the big download stalls still has the factor for that date.
    cron_schedule="45 4 1 * *",
    execution_timezone=TIMEZONE,
    description="Snapshot the RFU, and with it the year's facteur comparatif.",
)
def monthly_uniformized_property_wealth_schedule(
    context: ScheduleEvaluationContext,
) -> RunRequest:
    scrape_date = _scrape_month(context)
    return RunRequest(
        run_key=f"uniformized-property-wealth-{scrape_date}",
        partition_key=scrape_date,
    )


@schedule(
    job=vacancy_rates_job,
    # Behind cmhc_survey_job, which is now its upstream: the crosswalk is
    # applied to that day's snapshot rather than to a fresh download.
    cron_schedule="55 5 1 * *",
    execution_timezone=TIMEZONE,
    description="Cut this month's CMHC vacancy survey into every enabled borough.",
)
def monthly_vacancy_rates_schedule(context: ScheduleEvaluationContext):
    scrape_date = _scrape_month(context)
    for neighborhood in enabled_neighborhoods(context.instance):
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
    cron_schedule="58 5 1 * *",
    execution_timezone=TIMEZONE,
    description="Cut this month's CMHC rent survey into every enabled borough.",
)
def monthly_average_rents_schedule(context: ScheduleEvaluationContext):
    scrape_date = _scrape_month(context)
    for neighborhood in enabled_neighborhoods(context.instance):
        yield RunRequest(
            run_key=f"average-rents-{neighborhood}-{scrape_date}",
            partition_key=MultiPartitionKey(
                {"date": scrape_date, "neighborhood": neighborhood}
            ),
        )


@schedule(
    job=neighborhood_streets_job,
    # After street_network (50 4) and reference_neighborhoods (40 4), which
    # supply the province-wide layer and the outlines a side's borough is read
    # off. Ahead of monthly_building_lots_schedule rather than behind the
    # cadastre: the two share no input.
    cron_schedule="20 6 1 * *",
    execution_timezone=TIMEZONE,
    description="Keep this month's road network, whole, per cell of the cut.",
)
def monthly_neighborhood_streets_schedule(context: ScheduleEvaluationContext):
    yield from _tile_requests("neighborhood-streets", _scrape_month(context))


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
# then, at 30 7, behind monthly_building_lots_schedule which loads the cadastre
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

# No schedule for the three highest-and-best-use assets either, and for the
# same two reasons stacked. `silver.lot_development_programs`,
# `gold.lot_highest_best_use` and `gold.lot_redevelopment_gap` are hbu_infra's
# to create (sql/017, sql/018, sql/019) and none has been applied yet; and every
# one of their inputs comes from an asset that has no schedule of its own -
# `zoning_envelopes_job` supplies the envelopes, `lot_buildable_setbacks_job`
# the margins, and a nightly run of these would fail behind the ones already
# failing. All three are registered and have jobs, so they appear in the lineage
# and can be run by hand - see `lot_development_programs_job`, `lot_hbu_job`,
# `make programs` and `make hbu`. Add the schedules when the relations and the
# upstream schedules both land, at 45 7 and 50 7: behind `lot_profiles` at 40 7,
# because they read the same envelopes it does and there is no reason for two
# borough-wide passes over them to overlap.
#
# `lot_redevelopment_gap` also reads `lot_assessment_comparables`, which *is*
# scheduled (40 6) - so of its four upstreams that one needs no waiting, and the
# gap it computes is only as fresh as the envelopes behind the other three.
#
# `lot_highest_best_use` also reads `lot_frontage`, and one file of it:
# `road_lots.parquet`, the parcels that are the street. That was already
# upstream through the envelopes, which take their frontage from the same
# asset, so the ordering does not change - but what a *missing* frontage
# partition costs here is different in kind. Without it the road gate falls
# back to the assessment roll, and the roll does not record Montreal's
# roadways at all, so a borough's streets come back through the solver as
# development sites. The asset warns and carries on rather than failing; when
# these get schedules, `lot_frontage` belongs ahead of them for that reason and
# not only for the frontages.

# No schedule for `lot_building_massing` either, behind all of them:
# `gold.lot_building_massing` is hbu_infra's sql/022 and has not been applied,
# and it reads `lot_highest_best_use` for the footprint and
# `lot_buildable_setbacks` for the envelope to fit it into - neither scheduled.
# Registered with `lot_massing_job`; run by hand with `make massing`. It is the
# one asset here whose output is meant to be looked at rather than queried, so
# it is also the one most often run on its own after a config change: a
# different set of aspect ratios redraws a borough without re-solving it. Add
# the schedule at 55 7, last of the lineage, when the relation and the upstream
# schedules land.

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
        cadastre_job,
        building_lots_job,
        lot_profiles_job,
        street_network_job,
        neighborhood_streets_job,
        assessment_roll_job,
        lot_assessed_values_job,
        lot_assessment_comparables_job,
        commercial_rent_sources_job,
        commercial_rents_job,
        lot_frontage_job,
        neighborhood_addresses_job,
        lot_addresses_job,
        lot_zone_pieces_job,
        zoning_grid_columns_job,
        zoning_envelopes_job,
        lot_buildable_setbacks_job,
        lot_development_programs_job,
        lot_hbu_job,
        lot_massing_job,
        lot_opportunities_job,
        map_aggregates_job,
        map_tiles_job,
        cmhc_survey_job,
        construction_costs_job,
        uniformized_property_wealth_job,
        vacancy_rates_job,
        average_rents_job,
        rag_corpus_job,
        document_index_job,
    ],
    schedules=[
        monthly_catalog_schedule,
        monthly_features_schedule,
        monthly_reference_neighborhoods_schedule,
        monthly_street_network_schedule,
        monthly_assessment_roll_schedule,
        monthly_lot_assessed_values_schedule,
        monthly_lot_assessment_comparables_schedule,
        monthly_commercial_rent_sources_schedule,
        monthly_commercial_rents_schedule,
        monthly_lots_schedule,
        monthly_buildings_schedule,
        monthly_cadastre_schedule,
        monthly_building_lots_schedule,
        monthly_cmhc_survey_schedule,
        monthly_construction_costs_schedule,
        monthly_uniformized_property_wealth_schedule,
        monthly_vacancy_rates_schedule,
        monthly_average_rents_schedule,
        monthly_neighborhood_streets_schedule,
    ],
    resources={
        "spectrum": SpectrumResource(),
        "open_data": OpenDataResource(),
        # Quebec City's and Saguenay's publishers - Données Québec for the
        # outlines and the public ways of both, the ArcGIS zoning layer and
        # grid workbook of the first, the grid documents of the second.
        "quebec_open_data": QuebecOpenDataResource(),
        "quebec_zoning": QuebecZoningResource(),
        # Saguenay's zone lookup and its per-zone grid documents. Its polygons
        # come through `quebec_open_data` above - one portal, two cities.
        "saguenay_zoning": SaguenayZoningResource(),
        # Adresses Quebec, the MRNF's province-wide address points. No
        # cache_dir: it is queried per borough with that borough's outline, so
        # there is nothing shared between partitions to keep.
        "addresses": AdressesQuebecResource(),
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
        # The conseils de quartier listing host and the fiche pages; its PDFs
        # share `pdf_cache` above, since a filed minute never changes either.
        "council_minutes_source": CouncilMinutesResource(),
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
        # Donnees Quebec rather than the city's portal - see `RfuResource`.
        "rfu": RfuResource(),
        "marketbeat": MarketBeatResource(
            # Same posture as bdoi/cmhc/role: a published quarter is final, so
            # the two PDFs are fetched once and shared by every scrape date
            # until the next quarter lands. The landing page they are
            # discovered from is never cached - it is what says which quarter
            # is current.
            cache_dir=str(DATA_ROOT / "cache" / "marketbeat"),
        ),
        # No cache_dir, like `estimator`: the CRSPI table is 14 kB and is
        # revised, so every scrape date reads it again.
        "crspi": CrspiResource(),
        "cmhc": CmhcResource(
            # Same posture as bdoi/pdf_cache: a published survey year is
            # final, so the workbook is cached once, outside the partition
            # tree, and always local.
            cache_dir=str(DATA_ROOT / "cache" / "cmhc"),
        ),
        # No cache_dir, unlike "role" below it, and the contrast is the
        # point: a roll year is final and this is not. The codebook is
        # 185 kB at a URL with no year in it, reissued whenever the
        # manual is amended, so every scrape date reads it again - the
        # rule "estimator" and "crspi" already follow.
        "cubf": CubfResource(),
        "role": RoleResource(
            # Same posture again, and by far the largest of these caches: a
            # published roll year is final, so the 572 MB archive and the
            # 2.8 GB GeoPackage unpacked beside it are fetched once and shared
            # by every scrape date. Always local - the GeoPackage has to be on
            # a filesystem to be read at all, since SQLite reads it by seeking.
            cache_dir=str(DATA_ROOT / "cache" / "role"),
        ),
        "rqtt": RqttResource(
            # The second of the two large caches, and the one whose vintage is
            # discovered rather than named: the 390 MB archive and the 1.27 GB
            # GeoPackage beside it are fetched once per *published* vintage -
            # three a year - and shared by every scrape date in between. Always
            # local, for the reason "role" is.
            cache_dir=str(DATA_ROOT / "cache" / "rqtt"),
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
