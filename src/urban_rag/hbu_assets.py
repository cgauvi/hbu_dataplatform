"""The highest and best use of every lot, and how far the ground is from it.

Three assets over one question. `urban_rag.program` has been able to answer it
for one parcel since it was written; what was missing was anything that asked
it for a borough, and anything that put the answer beside what is standing on
the ground today. `urban_rag.hbu` is the arithmetic, free of Dagster the way
`comparables` and `role_foncier` are; this module is the partition handling.

**`lot_development_programs`** solves. One row per (lot, grid column) that
authorises dwellings and that `zoning_grid_columns` could turn into a
`ZoneColumn` - the same grain `lot_zoning_envelopes` writes, minus the rows
there is no program to state for. Every candidate keeps its row whether it won
or lost, because "why not the other column" is a question with an answer and
this is where the answer is.

**`lot_highest_best_use`** chooses. One row per lot, carrying the program of
the envelope that governs it - the grid's own pick within a zone, and the zone
covering most of the lot across zones. See `urban_rag.hbu` for why that is not
a maximisation over the candidates: the maximisation is inside `solve_program`,
over the mix, and picking the highest-earning *column* would be reporting a
building under rules the parcel may not be built to. Every lot the envelopes
reach keeps a row, and `hbu_status` says why the ones without a program have
none - including the two kinds of parcel that are not development sites at
all: `road_parcel`, which the roll files under a CUBF road code, and
`equipment_zone`, whose governing zone authorises only *Équipements
collectifs*.

It is the road gate that gives this asset its one non-zoning input,
`lot_assessment_comparables`, and reading the roll here rather than one asset
earlier is the whole of the reason the gate is a *choice* and not a filter on
the solve. `dominant_use_code` is a column of the comparables lineage, and
putting it upstream would make `ComparablesConfig.operating_expense_ratio`
re-solve a borough - exactly what the split below exists to prevent. A road
parcel is therefore solved and then not chosen, which costs one CP-SAT run per
street and keeps the two lineages apart.

**`lot_redevelopment_gap`** compares. One row per lot: the floor area standing
on it by class against the floor area its envelope could hold, in square metres
and in square feet, and the two incomes on one stated definition of NOI. The
existing side is `lot_assessment_comparables`' - the roll's dwellings and floor
area, split by each unit's own CUBF code and priced at CMHC's borough rent -
and the reconciliation between a monthly development objective and an annual
stabilised income is the whole substance of `hbu.use_gap`.

**Why three assets and not one.** They cost different things and change for
different reasons. Solving a borough is tens of thousands of CP-SAT models and
is the expensive step; choosing among the answers is a sort; comparing them
against the roll is a join. A change to `ComparablesConfig.operating_expense_ratio`
should not re-solve a borough to see its effect, and a change to
`stalls_per_dwelling` should re-solve without touching the assessment lineage.
It is the same split `lot_assessment_comparables` makes behind
`lot_assessed_values`, and `lot_buildable_setbacks` behind `zoning_envelopes_job`.

**What none of them read is Postgres.** Every input is a parquet partition this
platform already writes - the envelopes, the setbacks, the two CMHC grids, the
comparables - so the work here is five reads, a solve and two joins. The three
tables they *write* are hbu_infra's (sql/017, sql/018, sql/019), and until
`db.py init` has applied them the assets fail naming the file, the same way
`lot_frontage` and `lot_profiles` do. The parquet is written before the publish,
so a database that is down costs a re-run of the load rather than of the solve.
"""

import json
from datetime import datetime, timezone

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

from urban_rag.cmhc_assets import (
    AVERAGE_RENTS_FILE,
    VACANCY_FILE,
    average_rents,
    vacancy_rates,
)
from urban_rag.comparables_assets import (
    LOT_COMPARABLES_FILE,
    lot_assessment_comparables,
)
from urban_rag.envelope_assets import LOT_ENVELOPES_FILE, lot_zoning_envelopes
from urban_rag.frames import write_frame
from urban_rag.frontage_assets import ROAD_LOTS_FILE, lot_frontage
from urban_rag.hbu import (
    HBU_STATUSES,
    ProgramAssumptions,
    cadastral_road_lots,
    investment_assumptions_of,
    operating_expense_ratio_of,
    road_parcel_lots,
    select_highest_best_use,
    solve_envelopes,
    unit_economics,
    use_gap,
)
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.program import (
    ConstructionCosts,
    InvestmentAssumptions,
    NonResidentialEconomics,
    ParkingRules,
    StoreyHeights,
)
from urban_rag.rent_assets import COMMERCIAL_RENTS_FILE, commercial_rents
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.setback_assets import LOT_SETBACKS_FILE, lot_buildable_setbacks
from urban_rag.storage import clear_parquet, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

SILVER_GROUP = "silver_hbu"
GOLD_GROUP = "gold_hbu"

#: One file per partition, under
#: `silver/lot_development_programs/<YYYY-MM-DD>/<neighborhood>/`.
LOT_PROGRAMS_FILE = "lot_development_programs.parquet"

#: One file per partition, under
#: `gold/lot_highest_best_use/<YYYY-MM-DD>/<neighborhood>/`.
LOT_HBU_FILE = "lot_highest_best_use.parquet"

#: One file per partition, under
#: `gold/lot_redevelopment_gap/<YYYY-MM-DD>/<neighborhood>/`.
LOT_GAP_FILE = "lot_redevelopment_gap.parquet"

#: The columns of `lot_buildable_setbacks` this reads: the grain, and the one
#: measurement. `footprint_cap_m2` is deliberately *not* among them -
#: `solve_program` applies *Taux d'implantation* itself, and handing it a cap
#: that already has would not be wrong so much as unreadable, since `binding`
#: could then no longer say which of the two stopped the footprint.
_SETBACK_COLUMNS = ("lot_uid", "feature_id", "column_index", "buildable_area_m2")

#: The grain the envelopes, the setbacks and the programs all share.
_ENVELOPE_KEYS = ("lot_uid", "feature_id", "column_index")

#: What `lot_redevelopment_gap` actually writes, out of everything `use_gap`
#: computes. `use_gap` returns its input `hbu` frame *and* every gap column
#: appended to it, because a caller holding the return value in memory should
#: not have to re-join `lot_highest_best_use` to read `annual_gross_revenue_cad`
#: beside the gap it produced. A reader of the *table*, though, already has
#: `gold.lot_highest_best_use` to join on `lot_uid` for the envelope and the
#: program in full - carrying all fifty-odd of those columns a second time here
#: would be the "one table, more columns" sql/016's header argues against, for
#: a table whose whole job is the comparison rather than the envelope. So this
#: is identity, `hbu_status`, and every column `use_gap` *adds* - nothing it
#: only carries through.
_GAP_OUTPUT_COLUMNS = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "lot_area_m2",
    "primary_frontage_m",
    "hbu_status",
    "has_assessment",
    "is_underbuilt",
    "existing_residential_floor_area_m2",
    "hbu_residential_floor_area_m2",
    "residential_floor_area_gap_m2",
    "residential_floor_area_gap_sqft",
    "existing_commercial_floor_area_m2",
    "hbu_commercial_floor_area_m2",
    "commercial_floor_area_gap_m2",
    "commercial_floor_area_gap_sqft",
    "existing_industrial_floor_area_m2",
    "hbu_industrial_floor_area_m2",
    "industrial_floor_area_gap_m2",
    "industrial_floor_area_gap_sqft",
    "existing_floor_area_m2",
    "hbu_floor_area_m2",
    "floor_area_gap_m2",
    "floor_area_gap_sqft",
    "hbu_unit_area_m2",
    "existing_num_dwellings",
    "hbu_num_dwellings",
    "dwelling_gap",
    "existing_annual_gross_income_cad",
    "hbu_annual_gross_income_cad",
    "annual_gross_income_gap_cad",
    "operating_expense_ratio",
    # The two sides are netted with different ratios - the proposal is new and
    # pays the base, the standing building pays its age too - so both travel
    # and the age behind the difference travels with them.
    "hbu_operating_expense_ratio",
    "existing_effective_operating_expense_ratio",
    "existing_building_age_years",
    "existing_maintenance_premium",
    "existing_maintenance_penalty_cad",
    "existing_annual_stabilised_noi_cad",
    "hbu_annual_stabilised_noi_cad",
    "annual_stabilised_noi_gap_cad",
    "hbu_annual_noi_after_construction_cad",
    "hbu_total_capital_cost_cad",
    # The discounted pair, and the verdict between them - see `hbu.use_gap`.
    "hbu_npv_cad",
    "hbu_present_value_cad",
    "existing_present_value_cad",
    "redevelopment_npv_gain_cad",
    "existing_num_assessment_units",
    "existing_total_assessed_value",
    "existing_cap_rate_pct",
    "existing_dominant_use_code",
    "existing_dominant_use_description",
    "existing_dominant_income_class",
)


class ProgramConfig(Config):
    """The building the solver is asked to design, where it is not the grid's.

    Every field here is a *stated assumption* rather than a norm read off a
    zoning grid, which is what makes it config: no by-law says a dwelling storey
    is three metres or that a building offers half a stall per dwelling, and
    `urban_rag.program`'s module docstring is careful to say so at each of them.
    The defaults are that module's own constants, so a run that configures
    nothing gets exactly the program it documents.

    The value that produced a table travels on every row of it as
    `program_assumptions`, the rule `max_built_area_m2` and `frontage_buffer_m`
    follow, so an answer can always be read back against the building it assumed.

    `amortization_months` is the one to move first when a number looks wrong -
    it is what lets a one-time capital cost be subtracted from a recurring rent
    at all, and it is straight line and undiscounted.
    """

    stalls_per_dwelling: float = Field(
        default=ParkingRules().stalls_per_dwelling,
        ge=0.0,
        description=(
            "Stalls each dwelling owes. Villeray abolished residential parking "
            "minima, so this is what the building offers rather than what the "
            "by-law demands; 0 removes the residential half of the demand."
        ),
    )
    stalls_per_1000_sqft: float = Field(
        default=ParkingRules().stalls_per_1000_sqft,
        ge=0.0,
        description=(
            "Stalls each thousand square feet of commercial or industrial "
            "floor owes. The single heaviest lever on whether a mixed-use "
            "column fills with commerce or with housing."
        ),
    )
    residential_cost_per_sqft_cad: float = Field(
        default=ConstructionCosts().residential_cost_per_sqft,
        ge=0.0,
        description=(
            "Dollars per square foot of dwelling. The default is the Altus "
            "wood-frame condo midpoint, which under-costs a lot zoned for a "
            "tower - see estimator.CONDO_TYPE_IDS for the bands above it."
        ),
    )
    commercial_cost_per_sqft_cad: float = Field(
        default=ConstructionCosts().commercial_cost_per_sqft,
        ge=0.0,
        description="Dollars per square foot of commercial floor, gross.",
    )
    industrial_cost_per_sqft_cad: float = Field(
        default=ConstructionCosts().industrial_cost_per_sqft,
        ge=0.0,
        description="Dollars per square foot of industrial floor, gross.",
    )
    amortization_months: int = Field(
        default=ConstructionCosts().amortization_months,
        gt=0,
        description=(
            "Months a capital cost is spread over to be comparable with a "
            "monthly rent. Straight line, undiscounted, no financing."
        ),
    )
    commercial_rent_per_sqft_year_cad: float | None = Field(
        default=None,
        ge=0.0,
        description=(
            "Asking rent per square foot per YEAR, gross. Annual because that "
            "is how commercial leasing is quoted; the dwelling rent beside it "
            "is monthly because that is how CMHC surveys one. None - the "
            "default - reads the borough's own surveyed retail rate off "
            "silver/commercial_rents for the partition, falling back to "
            "urban_rag.program's stated constant where that asset has not "
            "run; a number here overrides both."
        ),
    )
    industrial_rent_per_sqft_year_cad: float | None = Field(
        default=None,
        ge=0.0,
        description=(
            "Asking rent per square foot per YEAR of industrial floor. None "
            "reads the surveyed industrial rate the way the commercial field "
            "does; a number overrides it."
        ),
    )
    commercial_vacancy_pct: float = Field(
        default=NonResidentialEconomics().commercial_vacancy_pct,
        ge=0.0,
        le=100.0,
        description=(
            "Share of commercial floor earning nothing, IN PERCENT as "
            "published vacancy is written. Stated, not surveyed - there is no "
            "CMHC for retail."
        ),
    )
    industrial_vacancy_pct: float = Field(
        default=NonResidentialEconomics().industrial_vacancy_pct,
        ge=0.0,
        le=100.0,
        description="Share of industrial floor earning nothing, in percent.",
    )
    residential_storey_height_m: float = Field(
        default=StoreyHeights().residential_m,
        gt=0.0,
        description=(
            "Floor to floor of a dwelling storey, in metres. What "
            "Hauteur en metre charges for a residential plate."
        ),
    )
    commercial_storey_height_m: float = Field(
        default=StoreyHeights().commercial_m,
        gt=0.0,
        description=(
            "Floor to floor of a commercial or industrial storey. Taller than "
            "a dwelling storey once the plenum is counted, which is why a "
            "metric height cap ranks the two differently from a storey cap."
        ),
    )
    discount_rate_pct: float = Field(
        default=InvestmentAssumptions().discount_rate_pct,
        ge=0.0,
        description=(
            "Annual rate a future dollar of NOI is discounted at, unlevered. "
            "The price of time in the objective, and with the terminal cap "
            "the largest lever on whether anything pencils at all."
        ),
    )
    hold_years: int = Field(
        default=InvestmentAssumptions().hold_years,
        gt=0,
        description="Years of NOI collected before the sale ends the hold.",
    )
    terminal_cap_rate_pct: float | None = Field(
        default=InvestmentAssumptions().terminal_cap_rate_pct,
        gt=0.0,
        description=(
            "Cap rate the stabilised NOI is sold at when the hold ends. None "
            "drops the sale and values the income stream alone."
        ),
    )
    operating_expense_ratio: float = Field(
        default=InvestmentAssumptions().operating_expense_ratio,
        ge=0.0,
        lt=1.0,
        description=(
            "Share of gross income spent running the NEW building the solver "
            "proposes - taxes, insurance, management, maintenance. The same "
            "convention (and default) as the comparables asset's base ratio; "
            "run both at one OPEX so the two sides of the gap stay netted "
            "alike."
        ),
    )
    new_build_rent_premium_pct: float = Field(
        default=InvestmentAssumptions().new_build_rent_premium_pct,
        ge=0.0,
        description=(
            "What a new dwelling leases for over the stock average CMHC "
            "surveys, in percent. 0 prices the proposal at the stock average, "
            "which is the conservative reading and understates every new "
            "building's proforma."
        ),
    )
    max_seconds: float = Field(
        default=ProgramAssumptions().max_seconds,
        gt=0.0,
        description=(
            "Seconds CP-SAT may spend on one envelope. Nearly every model here "
            "solves in milliseconds; this bounds the handful that do not, and "
            "num_not_optimal is what says whether it was reached."
        ),
    )

    def assumptions(
        self, *, surveyed_rents: dict[str, float] | None = None
    ) -> ProgramAssumptions:
        """This run's config as the object `urban_rag.hbu` is handed.

        The storey heights collapse four fields to three on purpose: commerce
        and industry are given one height because the difference between a
        retail plenum and a warehouse clear height is not something anything
        upstream distinguishes, and an above-grade parking deck takes the
        residential figure because a garage carries no plenum at all. Both are
        `urban_rag.program`'s own defaults, stated here rather than silently
        inherited.

        ``surveyed_rents`` is the borough's `commercial_rents` partition as a
        ``{rent_class: rent_psf_cad}`` map. Retail stands in for the solver's
        one commercial rate - the ground-floor space a mixed-use column in
        this borough actually means, not an office tower - and industrial for
        industrial. A rate stated in the config wins over the survey; the
        module constant is the floor under both.
        """
        commercial_rent = self.commercial_rent_per_sqft_year_cad
        if commercial_rent is None:
            commercial_rent = (surveyed_rents or {}).get(
                "retail", NonResidentialEconomics().commercial_per_sqft_year
            )
        industrial_rent = self.industrial_rent_per_sqft_year_cad
        if industrial_rent is None:
            industrial_rent = (surveyed_rents or {}).get(
                "industrial", NonResidentialEconomics().industrial_per_sqft_year
            )
        return ProgramAssumptions(
            parking=ParkingRules(
                stalls_per_dwelling=self.stalls_per_dwelling,
                stalls_per_1000_sqft=self.stalls_per_1000_sqft,
                amortization_months=self.amortization_months,
            ),
            construction=ConstructionCosts(
                residential_cost_per_sqft=self.residential_cost_per_sqft_cad,
                commercial_cost_per_sqft=self.commercial_cost_per_sqft_cad,
                industrial_cost_per_sqft=self.industrial_cost_per_sqft_cad,
                amortization_months=self.amortization_months,
            ),
            non_residential=NonResidentialEconomics(
                commercial_per_sqft_year=commercial_rent,
                industrial_per_sqft_year=industrial_rent,
                commercial_vacancy_pct=self.commercial_vacancy_pct,
                industrial_vacancy_pct=self.industrial_vacancy_pct,
            ),
            heights=StoreyHeights(
                residential_m=self.residential_storey_height_m,
                commercial_m=self.commercial_storey_height_m,
                industrial_m=self.commercial_storey_height_m,
                above_grade_parking_m=self.residential_storey_height_m,
            ),
            investment=InvestmentAssumptions(
                discount_rate_pct=self.discount_rate_pct,
                hold_years=self.hold_years,
                terminal_cap_rate_pct=self.terminal_cap_rate_pct,
                operating_expense_ratio=self.operating_expense_ratio,
                new_build_rent_premium_pct=self.new_build_rent_premium_pct,
            ),
            max_seconds=self.max_seconds,
        )


@asset(
    key_prefix=key_prefix("lot_development_programs"),
    partitions_def=scrape_partitions,
    deps=[
        lot_zoning_envelopes,
        lot_buildable_setbacks,
        vacancy_rates,
        average_rents,
        commercial_rents,
    ],
    group_name=SILVER_GROUP,
    kinds={"ortools", "postgres", "parquet"},
    description=(
        "What may profitably be built under every zoning envelope of one "
        "borough: one row per (lot, grid column) that authorises dwellings, "
        "commerce or industry and that the grid parser could turn into a "
        "solver input, each the answer to one urban_rag.program CP-SAT run. "
        "Carries the mix of dwellings by CMHC bedroom class, the storeys "
        "split into residential, commercial, industrial and above-grade "
        "parking, the underground levels, the footprint and gross floor "
        "area, the sous-sol of usage beside it - basement_area_m2, which "
        "neither storey cap sees and which the density index counts, so "
        "density_floor_area_m2 rather than gross_floor_area_m2 is what "
        "Densite was tested against - the stalls by where they were put - "
        "dug, decked, bayed into the "
        "ground floor, or standing on the yard the footprint leaves - "
        "surface_area_m2 for the ground those last ones take (floor area of "
        "no kind, and not part of the footprint either), parkable_area_m2 for "
        "the largest parking-shaped rectangle the parcel actually holds, "
        "which is what stops a four-metre lot from parking on its own width, "
        "floor_stack saying what "
        "stands on each storey as runs of identical levels, what each part "
        "costs to "
        "build, and the discounted net profit (npv_cad) that is the "
        "objective - the stabilised NOI discounted over the hold plus the "
        "discounted sale, less the capital - with the legacy monthly NOI "
        "restated beside it. Commerce and industry are priced at the "
        "borough's surveyed rents (silver/commercial_rents) where that "
        "partition exists. binding names the caps the answer is pressed "
        "against and unpriced_types the bedroom classes CMHC suppressed. A "
        "candidate the solver refuses keeps its row with its status; one "
        "whose model could not be built keeps its row with solve_error. "
        "Written to silver/lot_development_programs/"
        f"<YYYY-MM-DD>/<neighborhood>/{LOT_PROGRAMS_FILE} and upserted into "
        "silver.lot_development_programs on (scrape_date, neighborhood, "
        "lot_uid, feature_id, column_index)."
    ),
)
def lot_development_programs(
    context: AssetExecutionContext,
    config: ProgramConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    envelopes = _read(
        store,
        lot_zoning_envelopes,
        LOT_ENVELOPES_FILE,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )
    if envelopes.empty:
        raise Failure(
            f"{lot_zoning_envelopes.key.path[-1]} holds no envelope for "
            f"{neighborhood} {scrape_date}; there is nothing to solve."
        )
    envelopes = _with_buildable_area(context, store, envelopes, neighborhood, scrape_date)
    envelopes = _with_parkable_area(
        context, postgis, envelopes, neighborhood, scrape_date
    )

    economics, suppressed = unit_economics(
        _read(
            store,
            average_rents,
            AVERAGE_RENTS_FILE,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        ),
        _read(
            store,
            vacancy_rates,
            VACANCY_FILE,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        ),
    )
    if len(suppressed) == len(("studio", "1_bedroom", "2_bedroom", "3_bedroom_plus")):
        # Not raised: a borough CMHC published nothing for is a fact about the
        # survey, and the envelopes are still worth solving - every program
        # comes back empty with all four classes in `unpriced_types`, which is
        # an answer a reader can act on. Warned once, here, rather than left to
        # be inferred from a borough of zeros.
        context.log.warning(
            "%s %s: CMHC published no rent for any bedroom class, so every "
            "program will be empty - see unpriced_types on each row",
            neighborhood,
            scrape_date,
        )
    elif suppressed:
        context.log.info(
            "%s %s: CMHC suppressed %s; the solver will not build %s",
            neighborhood,
            scrape_date,
            ", ".join(suppressed),
            "them" if len(suppressed) > 1 else "it",
        )

    surveyed = _surveyed_commercial_rents(context, store, neighborhood, scrape_date)
    assumptions = config.assumptions(surveyed_rents=surveyed)
    frame = solve_envelopes(envelopes, economics, assumptions=assumptions)
    if frame.empty:
        raise Failure(
            f"{neighborhood} {scrape_date}: none of the {len(envelopes)} "
            "envelope row(s) authorises a dwelling and parses into a solver "
            "input, so no program can be stated. Check "
            "num_residential_not_solver_ready on zoning_grid_columns."
        )
    # The assumptions travel with the answer, the rule every stated assumption
    # in this platform follows: a program means nothing without the building it
    # was designed as, and a table written at one set of rates cannot be read
    # back against another.
    frame["program_assumptions"] = json.dumps(
        assumptions.as_metadata(), ensure_ascii=False
    )
    frame["neighborhood"] = neighborhood
    frame["scrape_date"] = scrape_date
    frame["computed_at"] = datetime.now(timezone.utc).isoformat()

    path = _write(context, store, frame, LOT_PROGRAMS_FILE, neighborhood, scrape_date)
    loaded = _publish(
        postgis,
        {"lot_development_programs": frame},
        neighborhood=neighborhood,
        scrape_date=scrape_date,
        path=path,
    )

    solved = frame[frame["solved"]]
    optimal = int((frame["status"] == "OPTIMAL").sum())
    context.log.info(
        "%s %s: %d of %d envelope row(s) solvable -> %d program(s) on %d "
        "lot(s), %d solved (%d optimal), %d dwelling(s), %.1f ha of floor -> %s",
        neighborhood,
        scrape_date,
        len(frame),
        len(envelopes),
        len(frame),
        int(frame["lot_uid"].nunique()),
        len(solved),
        optimal,
        int(solved["num_dwellings"].sum()),
        float(solved["gross_floor_area_m2"].sum()) / 10_000.0,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_envelopes": len(envelopes),
            "num_candidates": len(frame),
            # An envelope row that is not a candidate authorises no dwelling or
            # did not parse. The first is ordinary - a Commerce column - and the
            # second is the symptom worth seeing, which zoning_grid_columns
            # reports from its own side.
            "num_envelopes_not_candidates": len(envelopes) - len(frame),
            "num_lots": int(frame["lot_uid"].nunique()),
            "num_solved": len(solved),
            "num_optimal": optimal,
            # Feasible-but-not-optimal, or unknown: both mean max_seconds was
            # reached. A handful is fine, a third of the borough is a time
            # limit set too low.
            "num_not_optimal": len(solved) - optimal,
            "num_infeasible": int(
                (~frame["solved"] & (frame["status"] != "ERROR")).sum()
            ),
            # A model that could not be built at all, which solver_ready
            # promised would not happen - so anything above zero is a stale
            # parquet rather than a fact about the parcels.
            "num_solver_errors": int((frame["status"] == "ERROR").sum()),
            "num_empty_programs": int((solved["num_dwellings"] == 0).sum()),
            "num_with_commercial": int((solved["commercial_floors"] > 0).sum()),
            "num_with_industrial": int((solved["industrial_floors"] > 0).sum()),
            "num_digging": int((solved["underground_levels"] > 0).sum()),
            # The other reason to dig, and the one the density index counts: a
            # sous-sol of usage. At the module's own rates a cellar dwelling
            # does not pay for itself and a cellar shop does, so a borough
            # reporting these on its housing is one where a rent or a cost
            # moved - see `program.BELOW_GRADE_RENT_DISCOUNT_PCT` for how close
            # that call is.
            "num_with_basement": int((solved["basement_levels"] > 0).sum()),
            "total_basement_dwellings": int(solved["basement_dwellings"].sum()),
            "total_basement_floor_area_ha": round(
                float(solved["basement_area_m2"].sum()) / 10_000.0, 2
            ),
            "num_with_buildable_area": int(frame["buildable_area_m2"].notna().sum()),
            # Rows the setbacks asset has not measured are capped on Taux
            # d'implantation alone, which overstates a shallow parcel. Under a
            # few percent is ordinary; the whole borough means that asset has
            # not run for this partition.
            "num_without_buildable_area": int(frame["buildable_area_m2"].isna().sum()),
            # The yard's *shape*, measured off the cadastre. A row without it
            # has its surface stalls bounded on area alone, which is what every
            # run did before this existed; a row measuring zero sits on a
            # parcel no car can stand on, and its program had to dig, deck or
            # bay the stalls instead - `binding` says `surface_parking_shape`
            # on exactly those.
            "num_with_parkable_area": _notna_count(frame, "parkable_area_m2"),
            "num_without_parkable_area": len(frame)
            - _notna_count(frame, "parkable_area_m2"),
            "num_unparkable_lots": _zero_count(frame, "parkable_area_m2"),
            "num_surface_parking": int((solved["surface_stalls"] > 0).sum()),
            "total_surface_parking_ha": round(
                float(solved["surface_area_m2"].sum()) / 10_000.0, 2
            ),
            "total_dwellings": int(solved["num_dwellings"].sum()),
            "total_gross_floor_area_ha": round(
                float(solved["gross_floor_area_m2"].sum()) / 10_000.0, 2
            ),
            "total_npv_millions": round(float(solved["npv_cad"].sum()) / 1e6, 2),
            "total_monthly_noi_millions": round(
                float(solved["monthly_net_operating_income_cad"].sum()) / 1e6, 2
            ),
            "unpriced_bedroom_types": MetadataValue.json(list(suppressed)),
            "binding_caps": MetadataValue.json(_binding_counts(solved)),
            "program_assumptions": MetadataValue.json(assumptions.as_metadata()),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


@asset(
    key_prefix=key_prefix("lot_highest_best_use"),
    partitions_def=scrape_partitions,
    deps=[
        lot_development_programs,
        lot_zoning_envelopes,
        # The roll, and one column of it: `dominant_use_code`, which is what
        # says a parcel is a street rather than a site. It is a dependency of
        # the *choice* and not of the solve on purpose - see the module
        # docstring - so a change to ComparablesConfig re-runs a join and a
        # sort here rather than a borough of CP-SAT models upstream.
        lot_assessment_comparables,
        # The other half of that same question, and the half the roll cannot
        # answer: the parcels a geobase double side runs inside, which is what
        # Montreal's own street lots are. Already upstream through the
        # envelopes, which take their frontage from it; named here because
        # this asset now reads a file of its own from that partition.
        lot_frontage,
    ],
    group_name=GOLD_GROUP,
    kinds={"parquet", "postgres"},
    description=(
        "The highest and best use of every lot in one borough, one row each: "
        "the most profitable of the zoning envelopes that govern the parcel, "
        "with the envelope named beside it. Within a zone each usage family's "
        "governing column is the grid's own pick on Largeur du terrain min - "
        "select_governing_column's, carried through lot_zoning_envelopes as "
        "governs_residential / governs_commercial / governs_industrial - "
        "across zones the one covering most of the lot wins, and among the "
        "governing columns the developer's choice is made on discounted net "
        "profit (npv_cad): which use to build is the one real choice, and it "
        "is priced the way a land developer prices it. hbu_dominant_use says "
        "in one word what kind of building won, and floor_stack what stands "
        "on each storey - one entry per run of identical levels, bottom "
        "upwards, with the indoor stalls and the dwelling mix on the runs "
        "holding them, the surface ones being on no storey at all. Carries "
        "the dwellings by "
        "bedroom class, the storey split, the footprint and floor area, the "
        "stalls, what it costs to build, the npv and present value, and the "
        "monthly and annual net operating income, plus num_candidates and "
        "num_zones so a real choice is distinguishable from none. Two kinds "
        "of parcel keep their row and get no program: one that is the road - "
        "either the roll files it under a CUBF road code (4510-4599) or a "
        "geobase double street side runs down the inside of it, which is how "
        "Montreal's own street lots are found, since the roll never records "
        "them - and one whose governing zone authorises only Equipements "
        "collectifs (a park, a school, a cemetery). Every lot "
        "the envelopes reach keeps a row: hbu_status is one of "
        f"{', '.join(HBU_STATUSES)}. Written to gold/lot_highest_best_use/"
        f"<YYYY-MM-DD>/<neighborhood>/{LOT_HBU_FILE} and upserted into "
        "gold.lot_highest_best_use on (scrape_date, neighborhood, lot_uid)."
    ),
)
def lot_highest_best_use(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    envelopes = _read(
        store,
        lot_zoning_envelopes,
        LOT_ENVELOPES_FILE,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )
    programs = _read(
        store,
        lot_development_programs,
        LOT_PROGRAMS_FILE,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )
    if envelopes.empty:
        raise Failure(
            f"{lot_zoning_envelopes.key.path[-1]} holds no envelope for "
            f"{neighborhood} {scrape_date}; there is no lot to answer for."
        )
    assessments = _read(
        store,
        lot_assessment_comparables,
        LOT_COMPARABLES_FILE,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )
    road_lots = _road_lots(context, store, neighborhood, scrape_date)

    frame = select_highest_best_use(
        programs, envelopes, assessments=assessments, road_lots=road_lots
    )
    # Carried from the programs rather than recomputed: the assumptions that
    # produced a chosen program are the ones that produced the candidate it was
    # chosen from, and a second copy would be the one that goes stale.
    frame["program_assumptions"] = _first(programs, "program_assumptions")
    frame["neighborhood"] = neighborhood
    frame["scrape_date"] = scrape_date
    frame["computed_at"] = datetime.now(timezone.utc).isoformat()

    path = _write(context, store, frame, LOT_HBU_FILE, neighborhood, scrape_date)
    loaded = _publish(
        postgis,
        {"lot_highest_best_use": frame},
        neighborhood=neighborhood,
        scrape_date=scrape_date,
        path=path,
    )

    by_status = {
        status: int((frame["hbu_status"] == status).sum()) for status in HBU_STATUSES
    }
    answered = frame[frame["hbu_status"] == "solved"]
    context.log.info(
        "%s %s: %d lot(s) with an envelope -> %s; %d dwelling(s), %.1f ha of "
        "floor, %d lot(s) with a real choice of zone -> %s",
        neighborhood,
        scrape_date,
        len(frame),
        ", ".join(f"{name}={count}" for name, count in by_status.items()),
        int(answered["num_dwellings"].sum()),
        float(answered["gross_floor_area_m2"].sum()) / 10_000.0,
        int((frame["num_zones"] > 1).sum()),
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_lots": len(frame),
            "num_answered": by_status["solved"],
            "num_unanswered": len(frame) - by_status["solved"],
            **{f"num_{name}": count for name, count in by_status.items()},
            "num_candidates": int(frame["num_candidates"].sum()),
            # What the road gate actually took away rather than what it merely
            # labelled: parcels the roll calls a street that the solver had
            # produced a building for. Zero means the roll reached no roadway
            # in this borough, which is a fact about the roll worth seeing.
            "num_road_programs_withheld": _road_programs_withheld(
                programs, assessments, road_lots
            ),
            # The two road predicates, side by side. They are not alternatives
            # and neither contains the other: the roll knows a right of way it
            # assessed, the cadastre knows every street Montreal never put on
            # the roll. A borough where the second collapses to near zero is a
            # lot_frontage partition that did not land, not a borough of
            # roadless blocks - which is exactly the failure this gate had
            # before, silently.
            "num_road_parcels_on_the_roll": len(road_parcel_lots(assessments)),
            "num_road_parcels_in_the_cadastre": len(
                cadastral_road_lots(road_lots, assessments)
            ),
            # How many marginal calls the roll overturned - parcels the
            # geometry caught by barely more street line than the cutoff asks
            # for, which the roll files as primarily something else. The roll's
            # whole vote, and worth seeing because it is the one place a use
            # code is allowed to put a parcel back into the inventory.
            "num_road_parcels_rescued_by_the_roll": (
                len(cadastral_road_lots(road_lots))
                - len(cadastral_road_lots(road_lots, assessments))
            ),
            # A lot two zones reach is a lot on a zoning boundary, where the
            # answer depends on which line is believed. pct_of_lot decides it
            # and travels on every row, so a pick made off a 2 percent sliver
            # is visible rather than inferred.
            "num_lots_on_a_zone_boundary": int((frame["num_zones"] > 1).sum()),
            "num_lots_with_two_columns": int(
                (frame["num_governing_candidates"] > 1).sum()
            ),
            "total_dwellings": int(answered["num_dwellings"].sum()),
            "total_gross_floor_area_ha": round(
                float(answered["gross_floor_area_m2"].sum()) / 10_000.0, 2
            ),
            "total_commercial_area_ha": round(
                float(answered["commercial_area_m2"].sum()) / 10_000.0, 2
            ),
            "total_industrial_area_ha": round(
                float(answered["industrial_area_m2"].sum()) / 10_000.0, 2
            ),
            "total_npv_millions": round(float(answered["npv_cad"].sum()) / 1e6, 2),
            # What kind of building the borough's answers are, at a glance: a
            # borough of `residential` rows and one of `mixed` rows are two
            # different findings about the by-law and the rents together.
            "dominant_use_counts": MetadataValue.json(
                {
                    str(name): int(count)
                    for name, count in frame["hbu_dominant_use"]
                    .value_counts(dropna=True)
                    .items()
                }
            ),
            "total_annual_noi_millions": round(
                float(answered["annual_net_operating_income_cad"].sum()) / 1e6, 2
            ),
            "total_capital_cost_billions": round(
                float(answered["total_capital_cost_cad"].sum()) / 1e9, 2
            ),
            "median_dwellings_per_lot": _median(answered, "num_dwellings"),
            "binding_caps": MetadataValue.json(_binding_counts(answered)),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


@asset(
    key_prefix=key_prefix("lot_redevelopment_gap"),
    partitions_def=scrape_partitions,
    deps=[lot_highest_best_use, lot_assessment_comparables],
    group_name=GOLD_GROUP,
    kinds={"parquet", "postgres"},
    description=(
        "How far each lot is from its highest and best use, one row per lot. "
        "The floor area standing on it - the assessment roll's, split into "
        "residential, commercial and industrial by each unit's own CUBF use "
        "code by lot_assessment_comparables - against the floor area its "
        "governing zoning envelope could hold, per class, in square metres and "
        "square feet; the dwellings likewise. And the two incomes on one "
        "definition: gross rent less vacancy less the same "
        "operating_expense_ratio the comparables asset netted its own NOI "
        "with, annual on both sides, so annual_stabilised_noi_gap_cad is a "
        "subtraction of like from like. The solver's own objective - income "
        "after the amortised cost of building, before operating expenses - is "
        "kept separately as hbu_annual_noi_after_construction_cad with "
        "hbu_total_capital_cost_cad beside it. The discounted verdict is "
        "stated too, at the same InvestmentAssumptions the solve ran with: "
        "hbu_npv_cad is redeveloping, existing_present_value_cad is keeping "
        "the standing building, and redevelopment_npv_gain_cad is the "
        "difference - the land cancels, since the owner holds it either way. "
        "is_underbuilt is the screen: "
        "an envelope that holds more floor than the roll says stands on it. "
        "This table is the comparison and not a second copy of the envelope: "
        "the floors, stalls, binding caps and dollar figures of the program "
        "itself are gold.lot_highest_best_use's, one join away on lot_uid. "
        f"Written to gold/lot_redevelopment_gap/<YYYY-MM-DD>/<neighborhood>/"
        f"{LOT_GAP_FILE} and upserted into gold.lot_redevelopment_gap on "
        "(scrape_date, neighborhood, lot_uid)."
    ),
)
def lot_redevelopment_gap(
    context: AssetExecutionContext,
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
            f"{neighborhood} {scrape_date}; there is nothing to compare."
        )
    existing = _read(
        store,
        lot_assessment_comparables,
        LOT_COMPARABLES_FILE,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    # Read off the comparables rather than configured here, so both sides of
    # every NOI subtraction are netted with one number - see `hbu.use_gap`.
    opex = operating_expense_ratio_of(existing)
    # And the investment stance off the programs, for the same reason: the PV
    # put on the standing building must be the one the solve priced the
    # proposal at, or the npv gain compares two different ideas of money.
    investment = investment_assumptions_of(hbu)
    computed = use_gap(
        hbu, existing, operating_expense_ratio=opex, investment=investment
    )
    # Narrowed to the comparison itself - see `_GAP_OUTPUT_COLUMNS`. `use_gap`
    # carries the whole hbu frame along for a caller holding it in memory; a
    # reader of the table wants gold.lot_highest_best_use for the envelope and
    # this table for the gap, not both in one place twice.
    frame = computed[list(_GAP_OUTPUT_COLUMNS)].copy()
    frame["computed_at"] = datetime.now(timezone.utc).isoformat()

    path = _write(context, store, frame, LOT_GAP_FILE, neighborhood, scrape_date)
    loaded = _publish(
        postgis,
        {"lot_redevelopment_gap": frame},
        neighborhood=neighborhood,
        scrape_date=scrape_date,
        path=path,
    )

    matched = int(frame["has_assessment"].sum())
    underbuilt = int(frame["is_underbuilt"].sum())
    context.log.info(
        "%s %s: %d lot(s), %d matched to the roll, %d under-built; "
        "%.1f ha of floor gap, $%.1fM of annual stabilised NOI gap "
        "at an expense ratio of %.2f -> %s",
        neighborhood,
        scrape_date,
        len(frame),
        matched,
        underbuilt,
        float(frame["floor_area_gap_m2"].sum(min_count=1) or 0.0) / 10_000.0,
        float(frame["annual_stabilised_noi_gap_cad"].sum(min_count=1) or 0.0) / 1e6,
        opex,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_lots": len(frame),
            # The join, from this side. A lane, a park or a city parcel has no
            # assessment unit on it and a few percent is the honest reading; a
            # third of the borough means the roll and the cadastre disagree
            # about where the ground is, which lot_assessed_values reports as
            # num_lots_unvalued from its own side.
            "num_with_assessment": matched,
            "num_without_assessment": len(frame) - matched,
            "num_underbuilt": underbuilt,
            "pct_underbuilt": round(100.0 * underbuilt / len(frame), 1)
            if len(frame)
            else 0.0,
            **{
                f"{class_name}_floor_area_gap_ha": _sum_ha(
                    frame, f"{class_name}_floor_area_gap_m2"
                )
                for class_name in ("residential", "commercial", "industrial")
            },
            "floor_area_gap_ha": _sum_ha(frame, "floor_area_gap_m2"),
            "existing_floor_area_ha": _sum_ha(frame, "existing_floor_area_m2"),
            "hbu_floor_area_ha": _sum_ha(frame, "hbu_floor_area_m2"),
            "existing_dwellings": _sum_int(frame, "existing_num_dwellings"),
            "hbu_dwellings": _sum_int(frame, "hbu_num_dwellings"),
            "dwelling_gap": _sum_int(frame, "dwelling_gap"),
            "existing_annual_stabilised_noi_millions": _sum_millions(
                frame, "existing_annual_stabilised_noi_cad"
            ),
            "hbu_annual_stabilised_noi_millions": _sum_millions(
                frame, "hbu_annual_stabilised_noi_cad"
            ),
            "annual_stabilised_noi_gap_millions": _sum_millions(
                frame, "annual_stabilised_noi_gap_cad"
            ),
            # The solver's own objective, which nets the build rather than the
            # operating expenses. Reported beside the stabilised pair rather
            # than instead of it, because a borough where this is negative and
            # the gap above is positive is a borough where redevelopment earns
            # more and does not pay for itself.
            "hbu_annual_noi_after_construction_millions": _sum_millions(
                frame, "hbu_annual_noi_after_construction_cad"
            ),
            # The discounted verdict, summed where it is positive: what
            # redeveloping every lot it pays to redevelop would be worth, over
            # keeping what stands. The signed total is not reported because it
            # answers "what if every lot were redeveloped including the ones
            # that should not be", which nobody is asking.
            "redevelopment_npv_gain_positive_millions": round(
                float(
                    frame["redevelopment_npv_gain_cad"].clip(lower=0).sum(min_count=1)
                    or 0.0
                )
                / 1e6,
                2,
            ),
            "num_npv_gain_positive": int(
                (frame["redevelopment_npv_gain_cad"] > 0).sum()
            ),
            "hbu_total_capital_cost_billions": round(
                float(frame["hbu_total_capital_cost_cad"].sum(min_count=1) or 0.0)
                / 1e9,
                2,
            ),
            # What both NOIs were netted with, carried up because it is the
            # single largest lever on the gap and is not this asset's own
            # config - it is whatever lot_assessment_comparables was run at.
            "operating_expense_ratio": opex,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


# --------------------------------------------------------------------------
# the partition handling every one of the three shares
# --------------------------------------------------------------------------


def _surveyed_commercial_rents(
    context: AssetExecutionContext,
    store: ParquetStore,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, float]:
    """The borough's resolved commercial rents, as `{rent_class: rent_psf_cad}`.

    Optional the way the setbacks are: `commercial_rents` has its own chain
    (the MarketBeats and the rent index) and a partition without it is
    ordinary rather than broken. What it costs is the rates - the solver then
    prices commerce and industry at `urban_rag.program`'s stated constants,
    which flatter retail by a factor of three - so the fallback is warned
    about rather than silent, and the resolved rates travel on every row in
    `program_assumptions` either way.
    """
    partition_dir = store.partition_dir(
        commercial_rents.key.path[-1], scrape_date, neighborhood
    )
    path = join(partition_dir, COMMERCIAL_RENTS_FILE)
    if not filesystem(path).exists(path):
        context.log.warning(
            "%s is missing, so commerce and industry are priced at the stated "
            "module constants - materialize %s for this partition to price "
            "them at the borough's surveyed rents",
            path,
            commercial_rents.key.path[-1],
        )
        return {}
    frame = pd.read_parquet(path, storage_options=storage_options(path))
    if "rent_class" not in frame.columns or "rent_psf_cad" not in frame.columns:
        return {}
    rates = {
        str(row["rent_class"]): float(row["rent_psf_cad"])
        for _, row in frame.iterrows()
        if pd.notna(row["rent_psf_cad"])
    }
    if rates:
        context.log.info(
            "%s %s: pricing non-residential floor at the surveyed rents (%s)",
            neighborhood,
            scrape_date,
            ", ".join(f"{name} ${rate:.2f}/sqft/yr" for name, rate in rates.items()),
        )
    return rates


def _with_buildable_area(
    context: AssetExecutionContext,
    store: ParquetStore,
    envelopes: pd.DataFrame,
    neighborhood: str,
    scrape_date: str,
) -> pd.DataFrame:
    """The envelopes, plus what each column's own margins leave buildable.

    A left join at the (lot, zone, column) grain the two tables share, and
    optional on purpose: `lot_buildable_setbacks` has no schedule and reads a
    relation hbu_infra has to create, so a partition without it is ordinary
    rather than broken. What it costs is the footprint cap - `solve_program`
    falls back to *Taux d'implantation* alone, which overstates a shallow
    parcel - and the count of rows either way is in the run's metadata.

    A lot with no frontage gets no setback row at all (there is no front edge to
    sort a boundary against), so this is a left join rather than an inner one:
    the frontage gap must not also cost the program.
    """
    partition_dir = store.partition_dir(
        lot_buildable_setbacks.key.path[-1], scrape_date, neighborhood
    )
    path = join(partition_dir, LOT_SETBACKS_FILE)
    if not filesystem(path).exists(path):
        context.log.warning(
            "%s is missing, so every footprint is capped on Taux "
            "d'implantation alone - materialize %s for this partition to cap "
            "it on the margins as well",
            path,
            lot_buildable_setbacks.key.path[-1],
        )
        return envelopes.assign(buildable_area_m2=None)

    setbacks = pd.read_parquet(path, storage_options=storage_options(path))
    missing = [name for name in _SETBACK_COLUMNS if name not in setbacks.columns]
    if missing:
        raise Failure(
            f"{path} has no {', '.join(missing)} column - it was not written "
            f"by {lot_buildable_setbacks.key.path[-1]}."
        )
    return envelopes.merge(
        setbacks[list(_SETBACK_COLUMNS)].drop_duplicates(list(_ENVELOPE_KEYS)),
        on=list(_ENVELOPE_KEYS),
        how="left",
    )


def _notna_count(frame: pd.DataFrame, column: str) -> int:
    """How many rows carry ``column``, on a frame that may not have it at all.

    An absent column is zero rows rather than a KeyError: `parkable_area_m2` is
    optional by design, and the metadata that reports how optional it turned
    out to be must not be the thing that fails the run.
    """
    if column not in frame.columns:
        return 0
    return int(frame[column].notna().sum())


def _zero_count(frame: pd.DataFrame, column: str) -> int:
    """How many rows measured ``column`` at exactly zero - see `_notna_count`."""
    if column not in frame.columns:
        return 0
    return int((pd.to_numeric(frame[column], errors="coerce") == 0).sum())


def _with_parkable_area(
    context: AssetExecutionContext,
    postgis: PostgisResource,
    envelopes: pd.DataFrame,
    neighborhood: str,
    scrape_date: str,
) -> pd.DataFrame:
    """The envelopes, plus the shape of the yard each parcel could park on.

    `solve_program` bounds surface stalls at ``stall area x stalls + footprint
    <= lot area``, which is an area against an area and is satisfied on a
    parcel four metres wide - where no car can stand at any price.
    `massing.parking_capacity_m2` is the same question asked of the parcel's
    *shape*: the largest rectangle at least one stall deep that fits inside the
    boundary. Merged on here so `Lot.parkable_area_m2` reaches the solver, and
    at the **lot** grain rather than the (lot, zone, column) one the setbacks
    use - a parcel has one boundary however many columns govern it, so it is
    measured once per lot and broadcast.

    Read from `rag.lots` rather than from a parquet because that is where a
    parcel keyed on `lot_uid` lives: `bronze/neighborhood_lots` is the Infolot
    scrape, which predates the uid and cannot be joined to an envelope.

    Optional the way `_with_buildable_area` is optional, and for a stronger
    reason. Failing the partition when the cadastre is unreachable would cost
    a borough its programs over one bound; leaving the column absent restores
    exactly the behaviour every run had before this existed - surface stalls
    bounded on area alone - and says so in the log. What it costs is named
    there too, because "the yard was never measured" and "the yard measured
    zero" are different answers and only one of them is in the data.
    """
    from urban_rag.massing import parking_capacity_m2, to_metric
    from urban_rag.postgis import fetch_lot_polygons

    try:
        with postgis.connect() as connection:
            lots = fetch_lot_polygons(
                connection, neighborhood=neighborhood, scrape_date=scrape_date
            )
    except (PostgresUnavailable, MissingRelation) as exc:
        context.log.warning(
            "rag.lots could not be read for %s %s (%s), so every surface "
            "stall is bounded on the yard's *area* alone - a four-metre "
            "parcel will still be allowed to park on it. Load the cadastre "
            "for this partition to bound it on the yard's shape as well.",
            neighborhood,
            scrape_date,
            exc,
        )
        return envelopes

    if lots.empty:
        context.log.warning(
            "rag.lots holds no parcel for %s %s, so no yard shape was "
            "measured and every surface stall is bounded on area alone",
            neighborhood,
            scrape_date,
        )
        return envelopes

    # In metres once, for the whole borough: the fit is a rectangle against a
    # boundary and a rectangle in square degrees is not a rectangle.
    projected = to_metric(lots)
    capacity = {
        row.lot_uid: parking_capacity_m2(row.geometry)
        for row in projected.itertuples(index=False)
        if row.geometry is not None and not row.geometry.is_empty
    }
    measured = envelopes["lot_uid"].map(capacity)
    unparkable = int((measured == 0).sum())
    context.log.info(
        "%s %s: yard shape measured on %d of %d envelope row(s); %d row(s) "
        "sit on a parcel that holds no surface stall at all",
        neighborhood,
        scrape_date,
        int(measured.notna().sum()),
        len(envelopes),
        unparkable,
    )
    return envelopes.assign(parkable_area_m2=measured)


def _binding_counts(programs: pd.DataFrame) -> dict[str, int]:
    """How many solved programs each cap stopped, most common first.

    The borough-level answer to "why is nothing bigger than this", and worth a
    line of metadata rather than a query: a borough bound by `density_max` and
    one bound by `site_coverage_max` are two different planning arguments.
    """
    if programs.empty or "binding" not in programs.columns:
        return {}
    counts: dict[str, int] = {}
    for value in programs["binding"]:
        for cap in _json_list(value):
            counts[cap] = counts.get(cap, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: -item[1]))


def _json_list(value) -> list:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return []
    return decoded if isinstance(decoded, list) else []


def _first(frame: pd.DataFrame, column: str):
    """The partition-wide value of a column identical on every row."""
    if frame.empty or column not in frame.columns:
        return None
    values = frame[column].dropna()
    return values.iloc[0] if len(values) else None


def _median(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    value = pd.to_numeric(frame[column], errors="coerce").median()
    return round(float(value), 1) if pd.notna(value) else 0.0


def _sum_ha(frame: pd.DataFrame, column: str) -> float:
    """A column of square metres, summed and reported in hectares."""
    total = pd.to_numeric(frame[column], errors="coerce").sum(min_count=1)
    return round(float(total) / 10_000.0, 2) if pd.notna(total) else 0.0


def _road_programs_withheld(
    programs: pd.DataFrame, assessments: pd.DataFrame, road_lots: pd.DataFrame | None
) -> int:
    """How many road parcels the solver had already built a program for.

    The count the gate is worth judging on. `num_road_parcel` says how many
    parcels are the street; this says how many of them the zoning would
    otherwise have put a building on, which is the number that was wrong before
    and is the number to watch if either predicate's coverage changes. Counted
    over lot numbers, which is the key both of them answer in.
    """
    roads = road_parcel_lots(assessments) | cadastral_road_lots(road_lots, assessments)
    if not roads or programs.empty or "lot_number" not in programs.columns:
        return 0
    solved = programs["solved"].fillna(False).astype(bool)
    numbers = programs.loc[solved, "lot_number"]
    return int(numbers[numbers.isin(roads)].nunique())


def _road_lots(
    context: AssetExecutionContext,
    store: ParquetStore,
    neighborhood: str,
    scrape_date: str,
) -> pd.DataFrame | None:
    """The parcels `lot_frontage` identified as the roadway, if it has run.

    Optional the way `_with_buildable_area`'s setbacks are, and for the same
    reason: `lot_frontage` has no schedule and reads a relation hbu_infra has
    to create, so a partition without it must answer as it did before rather
    than fail. What it costs is much larger here, though, and the warning says
    so - without this file the road gate is the assessment roll alone, and the
    roll does not reach Montreal's street lots at all, so a borough's roadways
    come back through the solver as development sites.
    """
    path = join(
        store.partition_dir(lot_frontage.key.path[-1], scrape_date, neighborhood),
        ROAD_LOTS_FILE,
    )
    if not filesystem(path).exists(path):
        context.log.warning(
            "%s is missing, so a parcel is only a road if the assessment roll "
            "says so - and the roll does not record Montreal's roadways. "
            "Materialize %s for this partition to keep the solver off the "
            "street lots",
            path,
            lot_frontage.key.path[-1],
        )
        return None
    return pd.read_parquet(path, storage_options=storage_options(path))


def _sum_millions(frame: pd.DataFrame, column: str) -> float:
    total = pd.to_numeric(frame[column], errors="coerce").sum(min_count=1)
    return round(float(total) / 1e6, 2) if pd.notna(total) else 0.0


def _sum_int(frame: pd.DataFrame, column: str) -> int:
    total = pd.to_numeric(frame[column], errors="coerce").sum(min_count=1)
    return int(total) if pd.notna(total) else 0


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
    """One upstream partition, named by its asset rather than by its path.

    Fails naming what to materialize rather than letting pandas raise on a path
    a reader would have to decode - the same posture `envelope_assets._read` and
    `lot_profiles_assets._read` take.
    """
    asset_name = asset_def.key.path[-1]
    path = join(store.partition_dir(asset_name, scrape_date, neighborhood), name)
    if not filesystem(path).exists(path):
        raise Failure(
            f"{path} is missing; materialize {asset_name} for "
            f"{neighborhood} {scrape_date} first."
        )
    return pd.read_parquet(path, storage_options=storage_options(path))


def _write(
    context: AssetExecutionContext,
    store: ParquetStore,
    frame: pd.DataFrame,
    name: str,
    neighborhood: str,
    scrape_date: str,
) -> str:
    """This partition's one file, replacing whatever a previous run left."""
    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    return write_frame(frame, join(output_dir, name))


def _publish(
    postgis: PostgisResource,
    datasets: dict[str, pd.DataFrame],
    *,
    neighborhood: str,
    scrape_date: str,
    path: str,
) -> dict[str, dict[str, int]]:
    """Upsert what was just written, naming the file already on disk if not.

    After the parquet, deliberately: solving a borough is tens of thousands of
    CP-SAT models, and a database that is down should cost the load rather than
    the solve.
    """
    try:
        return publish(
            postgis.connect,
            datasets,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but {', '.join(datasets)} could not be "
            f"published for {neighborhood} {scrape_date}: {exc}"
        ) from exc
