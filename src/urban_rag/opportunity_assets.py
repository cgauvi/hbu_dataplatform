"""The under-built lots worth looking at first, ranked within an investment
thesis - and filed under a site thesis that says why the parcel is acquirable.

One gold asset over the HBU chain. `lot_redevelopment_gap` already answers
*how far is this lot from its highest and best use* for every parcel in the
borough, which is the right question and the wrong shape to act on: twenty-odd
thousand rows, most of them uninteresting, sorted by nothing and faceted by
nothing.

`lot_investment_opportunities` turns that into a shortlist. It files each lot
under an investment thesis and ranks the under-built ones within that thesis
on yield on cost, and the arithmetic for both is in `urban_rag.opportunities`,
free of Dagster the way `hbu` and `comparables` are.

**The thesis is the proposed program, not the existing use.** A warehouse whose
highest and best use is an apartment block is a *residential* opportunity;
filing it under industrial because that is what stands there today would put it
in the one facet that will never look at it. `existing_dominant_income_class`
travels beside `investment_thesis` so a conversion play is still visible as one
- and the two differing is often exactly where the biggest gaps are.

**The rank is yield on cost, and the NOI gap is only the tiebreak.** Ranking on
the raw gap sorts on parcel size almost regardless of what a building costs, so
every facet's top ten becomes the ten biggest lots in the borough. Yield on
cost is what a developer compares two sites on, and it lets a small cheap parcel
beat a large dear one. The land is in the denominator at its assessed value,
which is the one judgement in the formula - see `urban_rag.opportunities`.

**A second axis says why the site is on the market.** `site_thesis` is one of
`brownfield`, `teardown`, `infill` or `improvement`, resolved in that order,
with a boolean per thesis so the ones that also held are not lost. It reads
three things the gap table does not carry, and this asset joins them in:

* the roll's ``year_built`` and ``num_storeys`` for the lot, from
  `lot_assessment_comparables`, on the cadastral number;
* the solver's ``floors`` and ``footprint_m2`` and the zone it chose, from
  `lot_highest_best_use`, on ``lot_uid`` - a gold-to-gold join, safe because
  the two are materialized together and share a generation by construction;
* the governing zone's *Secteur d'interet patrimonial* and *PIIA (secteur)*
  rows, from `zoning_grid_columns`, on the zone - so a building in a heritage
  sector is kept out of the two theses that demolish, and a PIIA review is
  marked on the rest.

Each site thesis carries its own costs into its own yield - demolition,
characterisation and remediation, or the premium an addition pays over new
build - and is ranked within itself. The rates are config, stated on every
row in `screen_assumptions`, and their provenance is docs/site-theses.md.

**It re-solves nothing.** Every input is a column an upstream asset already
wrote, so this is a classification, a few divisions and four sorts over four
parquet files. That is the whole reason it is its own asset rather than more
columns on the gap table: changing what counts as "mixed-use", or the
demolition rate, or how many lots make the shortlist, should cost seconds
rather than a borough of CP-SAT models.

**Every lot keeps its row.** A lot that is not under-built, one the solver found
no program for, and one the roll never assessed all keep a row with a null rank
and a reason - `investment_thesis`, `is_land_assessed` and the gap table's own
`hbu_status` between them say which; `site_thesis = 'none'` says no site
condition held. The table is an inventory with two shortlists marked in it,
not the shortlists alone.

The facet summaries a reader usually wants first - how many lots per thesis,
what the shortlist yields, what it would cost - are in the run's metadata
rather than in a second table, because they are a ``GROUP BY`` over the rows
this asset already writes.
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

from urban_rag.comparables_assets import (
    LOT_COMPARABLES_FILE,
    lot_assessment_comparables,
)
from urban_rag.envelope_assets import ZONE_COLUMNS_FILE, zoning_grid_columns
from urban_rag.frames import write_frame
from urban_rag.hbu import ENHANCEMENT_COLUMNS, FUTURE_COLUMNS
from urban_rag.hbu_assets import (
    GOLD_GROUP,
    LOT_GAP_FILE,
    LOT_HBU_FILE,
    lot_highest_best_use,
    lot_redevelopment_gap,
)
from urban_rag.layers import key_prefix
from urban_rag.opportunities import (
    DEFAULT_BROWNFIELD_USE_PREFIXES,
    INVESTMENT_THESES,
    SITE_THESES,
    SiteRules,
    ThesisRules,
    futures_summary,
    rank_opportunities,
    rank_site_opportunities,
    site_thesis_summary,
    thesis_summary,
)
from urban_rag.partitions import scrape_partitions
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

#: The one file a partition writes, under
#: `gold/lot_investment_opportunities/<YYYY-MM-DD>/<neighborhood>/`.
LOT_OPPORTUNITIES_FILE = "lot_investment_opportunities.parquet"

#: What this asset carries forward from `lot_redevelopment_gap`, in the order
#: the table lists it. A curated subset rather than the whole gap row: this is a
#: shortlist a person reads, and the forty-odd columns of per-class floor areas
#: and square-foot conversions belong in the table it came from, one join away
#: on `lot_uid`. What is here is what a screening question needs to decide
#: whether to open the parcel.
_CARRIED: tuple[str, ...] = (
    "lot_uid",
    "lot_number",
    "neighborhood",
    "scrape_date",
    "lot_area_m2",
    "primary_frontage_m",
    "hbu_status",
    "is_underbuilt",
    # What stands there now, and what the roll thinks it is worth.
    "existing_dominant_income_class",
    # The MEFQ's words for the dominant unit's use code, and the one column
    # here that exists purely to be read. This is a shortlist someone opens
    # parcels from, and "Garage de stationnement pour automobiles" decides that
    # faster than any ratio beside it.
    "existing_dominant_use_description",
    # The code itself, since the second axis screens on it: a reader checking
    # why a lot is a brownfield wants the four characters the prefix matched.
    "existing_dominant_use_code",
    "existing_num_dwellings",
    "existing_floor_area_m2",
    "existing_total_assessed_value",
    "existing_cap_rate_pct",
    "existing_annual_stabilised_noi_cad",
    # What the solver would put there.
    "hbu_num_dwellings",
    "hbu_floor_area_m2",
    "hbu_residential_floor_area_m2",
    "hbu_commercial_floor_area_m2",
    "hbu_industrial_floor_area_m2",
    "hbu_annual_stabilised_noi_cad",
    "hbu_total_capital_cost_cad",
    # The gap between the two, which is what makes it an opportunity.
    "dwelling_gap",
    "floor_area_gap_m2",
    "annual_stabilised_noi_gap_cad",
    "operating_expense_ratio",
    # The verdict the second axis ranks behind: rebuilding against holding,
    # discounted. Carried so the shortlist says whether the play pays without
    # a join back to the gap.
    "redevelopment_npv_gain_cad",
    "existing_present_value_cad",
    "hbu_npv_cad",
    # The second future - the building retained and grown - and the three
    # side by side, as the gap solved and valued them. Carried whole: the
    # buyer's and the owner's panes read every one of these off this row.
    *ENHANCEMENT_COLUMNS,
    *FUTURE_COLUMNS,
    "enhance_assumptions",
)

#: What the roll's side adds, from `lot_assessment_comparables`, and the names
#: it takes here - prefixed like every other existing-side column of the gap.
_ROLL_COLUMNS: dict[str, str] = {
    "year_built": "existing_year_built",
    "num_storeys": "existing_num_storeys",
}

#: What the solver's side adds, from `lot_highest_best_use`, and the names it
#: takes here. `feature_id` and `source_table` are the join to the zone and
#: are not carried; `grid_zone` is, because it is what a person reads.
_PROGRAM_COLUMNS: dict[str, str] = {
    "floors": "hbu_floors",
    "footprint_m2": "hbu_footprint_m2",
    "grid_zone": "grid_zone",
}

#: The zone-level rows read off the grid, from `zoning_grid_columns`.
_ZONE_COLUMNS: tuple[str, ...] = ("heritage_sector", "piia_sector")


class OpportunityConfig(Config):
    """Where the facet lines fall, what the land costs, and how long the list is.

    Every one of these is a judgement about a mandate rather than a property of
    the data, and every row records the lot - see `screen_assumptions` - so a
    shortlist can be read back against the rules that produced it. The same rule
    `max_built_area_m2` and `frontage_buffer_m` follow.

    ``dominant_share`` is the share of proposed floor one class needs to own the
    lot outright; ``mixed_min_share`` is what the smaller of residential and
    commercial needs for the lot to be mixed-use instead. 0.85 and 0.15 mean a
    ground-floor shop under five storeys of flats is a mixed-use play and a
    single unit at the base of a tower is not. `ThesisRules` refuses settings
    where the two rules would contradict each other.

    ``market_value_factor`` scales the roll's assessed value - land and
    building together, since that is what `rl0404a` is - on its way into the
    yield's denominator and into the price a buyer pays. 1.0 costs the
    property at the roll, which is the honest default for the same reason
    `ComparablesConfig.market_value_factor` defaults there: Quebec's *facteur
    comparatif* is not in the published roll, and the 2026 roll values
    everything as of July 2024. A reader who knows what plexes trade over
    assessment sets 1.1 to 1.3 here and gets a yield, and a buyer's NPV, on
    something nearer what the property would actually cost.

    ``top_n`` is how many lots per thesis `is_top_opportunity` marks. It moves
    a flag and nothing else - every lot keeps its row and its rank whatever it
    is set to - so it is the cheapest of these to change your mind about.

    The rest are the second axis - see `urban_rag.opportunities.SiteRules` for
    what each one means and docs/site-theses.md for where the defaults come
    from. ``site_top_n`` is `top_n`'s twin for `is_top_site_opportunity`.
    """

    dominant_share: float = Field(
        default=0.85,
        gt=0.0,
        le=1.0,
        description=(
            "Share of proposed floor one class needs to own the lot outright."
        ),
    )
    mixed_min_share: float = Field(
        default=0.15,
        gt=0.0,
        lt=0.5,
        description=(
            "What the smaller of residential and commercial needs for the lot "
            "to be mixed-use."
        ),
    )
    market_value_factor: float = Field(
        default=1.0,
        gt=0,
        description=(
            "Scales the roll's assessed value (land and building) into the "
            "yield's denominator and the buyer's price. 1.0 costs the "
            "property at the roll; 1.1 to 1.3 is what plexes trade over it."
        ),
    )
    top_n: int = Field(
        default=25,
        ge=1,
        description="Lots per thesis that is_top_opportunity marks.",
    )

    # -- the second axis ----------------------------------------------------
    teardown_max_year_built: int = Field(
        default=1960,
        description=(
            "Youngest year built a building may have and still be presumed "
            "obsolete for the teardown thesis."
        ),
    )
    teardown_max_built_share: float = Field(
        default=0.40,
        gt=0.0,
        le=1.0,
        description=(
            "Largest share of the proposed floor the standing floor may fill "
            "for the teardown thesis."
        ),
    )
    min_storey_headroom: int = Field(
        default=2,
        ge=0,
        description=(
            "Storeys the governing envelope must allow above what stands for "
            "the teardown thesis."
        ),
    )
    brownfield_use_prefixes: list[str] = Field(
        default_factory=lambda: list(DEFAULT_BROWNFIELD_USE_PREFIXES),
        description=(
            "CUBF prefixes read as a contamination-risk use: manufacturing, "
            "transport yards, salvage, vehicle sales and service stations, "
            "dry cleaning, warehousing, automotive repair."
        ),
    )
    exclude_heritage_sectors: bool = Field(
        default=True,
        description=(
            "Keep a lot whose governing zone is a secteur d'interet "
            "patrimonial out of the teardown and brownfield theses."
        ),
    )
    exclude_piia_sectors: bool = Field(
        default=False,
        description=(
            "Do the same for a zone under the PIIA by-law. Off by default: a "
            "PIIA reviews the architecture, it does not bar demolition, and it "
            "covers close to half the borough."
        ),
    )
    demolition_review_year: int = Field(
        default=1940,
        description=(
            "Buildings older than this are flagged demolition_review_required "
            "- the Loi sur le patrimoine culturel's floor for a municipal "
            "demolition by-law."
        ),
    )
    exclude_demolition_review: bool = Field(
        default=False,
        description="Turn demolition_review_required into a screen as well.",
    )
    demolition_cost_cad_per_m2: float = Field(
        default=150.0,
        ge=0.0,
        description=(
            "Demolition, per m2 of the standing gross floor area of a "
            "residential building."
        ),
    )
    demolition_cost_cad_per_m2_nonresidential: float = Field(
        default=250.0,
        ge=0.0,
        description=(
            "Demolition, per m2 of the standing gross floor area of a "
            "commercial or industrial building."
        ),
    )
    site_assessment_cost_cad: float = Field(
        default=12_000.0,
        ge=0.0,
        description=(
            "Phase I and II environmental site assessment, once per lot with "
            "a contamination-risk use."
        ),
    )
    remediation_cost_cad_per_m2_residential: float = Field(
        default=150.0,
        ge=0.0,
        description=(
            "Remediation per m2 of lot where the program includes housing - "
            "the stricter criterion."
        ),
    )
    remediation_cost_cad_per_m2_nonresidential: float = Field(
        default=75.0,
        ge=0.0,
        description=(
            "Remediation per m2 of lot where the program is commerce or "
            "industry only."
        ),
    )
    addition_cost_premium: float = Field(
        default=1.5,
        gt=0.0,
        description=(
            "What a storey or an annex costs per m2 as a multiple of the "
            "solver's new-build cost per m2 for the same lot."
        ),
    )
    improvement_max_added_storeys: int = Field(
        default=1,
        ge=0,
        description="Storeys the improvement thesis may add on the standing footprint.",
    )
    improvement_min_floor_m2: float = Field(
        default=40.0,
        ge=0.0,
        description="Smallest addition worth filing under the improvement thesis.",
    )
    require_positive_npv: bool = Field(
        default=True,
        description=(
            "Rank a site thesis only where rebuilding beats holding "
            "(or the addition earns). Off, every filed lot is ranked."
        ),
    )
    site_top_n: int = Field(
        default=25,
        ge=1,
        description="Lots per site thesis that is_top_site_opportunity marks.",
    )

    def rules(self) -> ThesisRules:
        """The thesis thresholds this run classifies with."""
        return ThesisRules(
            dominant_share=self.dominant_share,
            mixed_min_share=self.mixed_min_share,
        )

    def site_rules(self) -> SiteRules:
        """The site-thesis thresholds, switches and rates this run applies."""
        return SiteRules(
            teardown_max_year_built=self.teardown_max_year_built,
            teardown_max_built_share=self.teardown_max_built_share,
            min_storey_headroom=self.min_storey_headroom,
            brownfield_use_prefixes=tuple(self.brownfield_use_prefixes),
            exclude_heritage_sectors=self.exclude_heritage_sectors,
            exclude_piia_sectors=self.exclude_piia_sectors,
            demolition_review_year=self.demolition_review_year,
            exclude_demolition_review=self.exclude_demolition_review,
            demolition_cost_cad_per_m2=self.demolition_cost_cad_per_m2,
            demolition_cost_cad_per_m2_nonresidential=(
                self.demolition_cost_cad_per_m2_nonresidential
            ),
            site_assessment_cost_cad=self.site_assessment_cost_cad,
            remediation_cost_cad_per_m2_residential=(
                self.remediation_cost_cad_per_m2_residential
            ),
            remediation_cost_cad_per_m2_nonresidential=(
                self.remediation_cost_cad_per_m2_nonresidential
            ),
            addition_cost_premium=self.addition_cost_premium,
            improvement_max_added_storeys=self.improvement_max_added_storeys,
            improvement_min_floor_m2=self.improvement_min_floor_m2,
            require_positive_npv=self.require_positive_npv,
        )


@asset(
    key_prefix=key_prefix("lot_investment_opportunities"),
    partitions_def=scrape_partitions,
    deps=[
        lot_redevelopment_gap,
        lot_highest_best_use,
        lot_assessment_comparables,
        zoning_grid_columns,
    ],
    group_name=GOLD_GROUP,
    kinds={"parquet", "postgres"},
    description=(
        "The under-built lots worth looking at first, one row per lot, faceted "
        "by investment thesis and ranked within it. investment_thesis is read "
        "off the *proposed* program - the mix of residential, commercial and "
        "industrial floor the highest-and-best-use solver would build - so a "
        "warehouse whose best use is flats is a residential opportunity; the "
        "existing use travels beside it as existing_dominant_income_class, and "
        "the two differing is a conversion play. yield_on_cost_pct is the "
        "stabilised NOI of that proposed building over what it costs to get "
        "there: construction plus the property at its assessed value, scaled "
        "by market_value_factor. thesis_rank orders the under-built lots within "
        "each thesis on that yield, breaking ties on the annual NOI gap, and "
        "is_top_opportunity marks the first top_n of each. site_thesis is the "
        "second axis - why the parcel is acquirable: brownfield (a "
        "contamination-risk use standing on it), teardown (an obsolete "
        "building under an unused envelope), infill (nothing standing) or "
        "improvement (a storey or an annex on the building that stays), "
        "resolved in that order and each carrying its own demolition, "
        "remediation or addition cost into site_yield_on_cost_pct; a lot in a "
        "heritage sector is kept out of the two that demolish, and a PIIA "
        "review is flagged. site_thesis_rank orders each site thesis on that "
        "yield and is_top_site_opportunity marks the first site_top_n. Each "
        "lot's three futures - keep, enhance, tear down and rebuild - are "
        "priced twice beside that: for the owner, land cancelling, as "
        "owner_gain_* and owner_best_future; and for a buyer paying the "
        "larger of the roll's value times market_value_factor and the standing "
        "income's worth, as buyer_npv_*, buyer_yield_*, residual_price_* and "
        "buyer_best_future. Every lot keeps its row. Written to "
        "gold/lot_investment_opportunities/"
        f"<YYYY-MM-DD>/<neighborhood>/{LOT_OPPORTUNITIES_FILE} and upserted "
        "into gold.lot_investment_opportunities on (scrape_date, neighborhood, "
        "lot_uid)."
    ),
)
def lot_investment_opportunities(
    context: AssetExecutionContext,
    config: OpportunityConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    gap = _read(
        store, lot_redevelopment_gap, LOT_GAP_FILE,
        neighborhood=neighborhood, scrape_date=scrape_date,
    )
    if gap.empty:
        # Not "the borough has no opportunities": the partition upstream was
        # never computed. Distinguishing the two is why this fails rather than
        # writing a well-formed empty shortlist.
        raise Failure(
            f"{lot_redevelopment_gap.key.path[-1]} holds no lot for "
            f"{neighborhood} {scrape_date}; there is nothing to rank."
        )
    hbu = _read(
        store, lot_highest_best_use, LOT_HBU_FILE,
        neighborhood=neighborhood, scrape_date=scrape_date,
    )
    existing = _read(
        store, lot_assessment_comparables, LOT_COMPARABLES_FILE,
        neighborhood=neighborhood, scrape_date=scrape_date,
    )
    # The one optional input: a partition whose grids were parsed before the
    # zone-level rows were read has a zone columns file with no heritage in
    # it, and one materialized by hand may have none at all. Either way the
    # honest answer is "unknown", which screens nothing and is counted.
    zones = _read(
        store, zoning_grid_columns, ZONE_COLUMNS_FILE,
        neighborhood=neighborhood, scrape_date=scrape_date, optional=True,
    )

    inputs = gap.copy()
    _join_program(inputs, hbu)
    _join_roll(inputs, existing)
    heritage_known, num_zone_unmatched = _join_zones(inputs, hbu, zones)

    rules = config.rules()
    site_rules = config.site_rules()
    ranked = rank_opportunities(
        inputs,
        rules=rules,
        market_value_factor=config.market_value_factor,
        top_n=config.top_n,
    )
    sited = rank_site_opportunities(
        inputs,
        rules=site_rules,
        market_value_factor=config.market_value_factor,
        top_n=config.site_top_n,
    )
    joined = [
        *(name for name in _ROLL_COLUMNS.values()),
        *(name for name in _PROGRAM_COLUMNS.values()),
        *_ZONE_COLUMNS,
    ]
    frame = pd.concat(
        [
            inputs[[c for c in _CARRIED if c in inputs.columns]],
            inputs[joined],
            ranked,
            sited,
        ],
        axis=1,
    )
    # Recorded on every row, not only in the run config: `is_top_opportunity`
    # and `investment_thesis` mean nothing without the thresholds behind them,
    # and a table read a month later has only the row.
    frame["screen_assumptions"] = json.dumps(
        {
            **rules.as_metadata(),
            "market_value_factor": config.market_value_factor,
            "top_n": config.top_n,
            **site_rules.as_metadata(),
            "site_top_n": config.site_top_n,
            "heritage_source": "zoning_grid_columns" if heritage_known else "absent",
        },
        ensure_ascii=False,
    )
    frame["neighborhood"] = neighborhood
    frame["scrape_date"] = scrape_date
    frame["computed_at"] = datetime.now(timezone.utc).isoformat()

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_OPPORTUNITIES_FILE))

    # After the parquet, the posture every asset here takes: the file is the
    # record, and a database that is down should cost a re-run of the load.
    try:
        loaded = publish(
            postgis.connect,
            {"lot_investment_opportunities": frame},
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but gold.lot_investment_opportunities could "
            f"not be updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    summary = thesis_summary(frame)
    site_summary = site_thesis_summary(frame)
    futures = futures_summary(frame)
    ranked_rows = int(frame["thesis_rank"].notna().sum())
    site_ranked_rows = int(frame["site_thesis_rank"].notna().sum())
    context.log.info(
        "%s %s: %d lot(s), %d ranked, %d shortlisted - %s; site theses %s -> %s",
        neighborhood,
        scrape_date,
        len(frame),
        ranked_rows,
        int(frame["is_top_opportunity"].sum()),
        ", ".join(
            f"{row.investment_thesis} {row.num_ranked}"
            f" (best {row.best_yield_on_cost_pct or float('nan'):.1f} pct)"
            for row in summary.itertuples()
        ),
        ", ".join(
            f"{row.site_thesis} {row.num_lots} filed / {row.num_ranked} ranked"
            for row in site_summary.itertuples()
        ),
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_lots": len(frame),
            "num_ranked": ranked_rows,
            # A lot with a thesis but no rank is one that is not under-built,
            # or one the roll never assessed so the yield has no denominator.
            # Both are ordinary; a partition where they are most of the rows is
            # an upstream that did not land.
            "num_unranked": len(frame) - ranked_rows,
            "num_top_opportunities": int(frame["is_top_opportunity"].sum()),
            "num_without_assessed_land": int((~frame["is_land_assessed"]).sum()),
            **{
                f"num_{row.investment_thesis}": row.num_lots
                for row in summary.itertuples()
            },
            **{
                f"num_{row.investment_thesis}_ranked": row.num_ranked
                for row in summary.itertuples()
            },
            **{
                f"best_{row.investment_thesis}_yield_on_cost_pct": (
                    row.best_yield_on_cost_pct
                    if row.best_yield_on_cost_pct is not None
                    else "none ranked"
                )
                for row in summary.itertuples()
            },
            **{
                f"median_{row.investment_thesis}_yield_on_cost_pct": (
                    row.median_yield_on_cost_pct
                    if row.median_yield_on_cost_pct is not None
                    else "none ranked"
                )
                for row in summary.itertuples()
            },
            # What the whole shortlist would add and what it would take, as one
            # pair of numbers per run. The borough-scale read this asset is for.
            "shortlist_noi_gap_millions": round(
                sum(row.top_noi_gap_cad or 0.0 for row in summary.itertuples())
                / 1e6,
                2,
            ),
            "shortlist_project_cost_millions": round(
                sum(
                    row.top_project_cost_cad or 0.0 for row in summary.itertuples()
                )
                / 1e6,
                2,
            ),
            "thesis_summary": MetadataValue.md(
                summary.to_markdown(index=False)
            ),
            # -- the second axis ------------------------------------------
            "num_site_ranked": site_ranked_rows,
            "num_top_site_opportunities": int(
                frame["is_top_site_opportunity"].sum()
            ),
            **{
                f"num_{row.site_thesis}_sites": row.num_lots
                for row in site_summary.itertuples()
            },
            **{
                f"num_{row.site_thesis}_sites_ranked": row.num_ranked
                for row in site_summary.itertuples()
            },
            **{
                f"median_{row.site_thesis}_site_yield_on_cost_pct": (
                    row.median_site_yield_on_cost_pct
                    if row.median_site_yield_on_cost_pct is not None
                    else "none ranked"
                )
                for row in site_summary.itertuples()
            },
            # The heritage screen, by the numbers: how many lots it kept out,
            # and how many it could not judge. A borough where the last is most
            # of the rows has a zone columns partition parsed before the
            # *Patrimoine* rows were read - re-run `zoning_grid_columns`.
            "num_heritage_sector_lots": int(frame["is_heritage_sector"].sum()),
            "num_piia_review_lots": int(frame["has_piia_review"].sum()),
            "num_demolition_review_lots": int(
                frame["demolition_review_required"].sum()
            ),
            "num_demolition_restricted_lots": int(
                frame["is_demolition_restricted"].sum()
            ),
            # A grid printing ``-`` against the heritage row is a known
            # answer - not a sector - and is null here like everywhere else,
            # so "unknown" is the lots whose governing zone found no grid row
            # at all, or every lot where the zone file carried no such rows.
            "num_lots_heritage_unknown": (
                num_zone_unmatched if heritage_known else len(frame)
            ),
            "num_lots_storeys_unknown": int(
                frame["existing_num_storeys"].isna().sum()
            ),
            "num_brownfield_use_lots": int(frame["is_brownfield_use"].sum()),
            "site_thesis_summary": MetadataValue.md(
                site_summary.to_markdown(index=False)
            ),
            # The three futures, twice: which wins for the owner and which for
            # a buyer at the run's market factor. A borough where the owner's
            # answer is `hold` almost everywhere and the buyer's `rebuild`
            # nowhere is a borough priced above what its envelopes earn.
            **{
                f"num_owner_best_{row.future}": row.owner_wins
                for row in futures.itertuples()
            },
            **{
                f"num_buyer_best_{row.future}": row.buyer_wins
                for row in futures.itertuples()
            },
            "num_buyer_priced": int(frame["acquisition_cost_cad"].notna().sum()),
            "num_enhancements_solved": int(frame["enhance_solved"].fillna(False).sum())
            if "enhance_solved" in frame.columns else 0,
            "futures_summary": MetadataValue.md(futures.to_markdown(index=False)),
            # The judgements, so a run reads back against the one before it.
            "dominant_share": config.dominant_share,
            "mixed_min_share": config.mixed_min_share,
            "market_value_factor": config.market_value_factor,
            "top_n": config.top_n,
            "site_top_n": config.site_top_n,
            "site_rules": MetadataValue.json(site_rules.as_metadata()),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


def _join_program(inputs: pd.DataFrame, hbu: pd.DataFrame) -> None:
    """The solver's storeys, footprint and zone, aligned on ``lot_uid``.

    Gold to gold on the surrogate, which is safe here for the reason the map
    is careful about elsewhere: the gap and the HBU parquet are written by
    one `make hbu` and share a generation of `rag.lots` by construction. The
    join keys to the zone (`feature_id`, `source_table`) ride along for
    `_join_zones` and are dropped before the frame is written.
    """
    wanted = [*_PROGRAM_COLUMNS, "feature_id", "source_table"]
    if hbu.empty or "lot_uid" not in hbu.columns or "lot_uid" not in inputs.columns:
        for name in [*_PROGRAM_COLUMNS.values(), "feature_id", "source_table"]:
            inputs[name] = None
        return
    right = (
        hbu.drop_duplicates("lot_uid")
        .set_index("lot_uid")
        .reindex(columns=wanted)
        .rename(columns=_PROGRAM_COLUMNS)
    )
    aligned = right.reindex(inputs["lot_uid"].to_numpy())
    aligned.index = inputs.index
    for name in aligned.columns:
        inputs[name] = aligned[name]


def _join_roll(inputs: pd.DataFrame, existing: pd.DataFrame) -> None:
    """The roll's year built and storey count, aligned on the lot number.

    The same left join `hbu._join_existing` makes, on the same key: both
    sides carry Infolot's own spelling of the number. Duplicates on the right
    are dropped rather than allowed to multiply the left.
    """
    key = "NO_LOT" if "NO_LOT" in existing.columns else "lot_number"
    if existing.empty or key not in existing.columns or "lot_number" not in inputs:
        for name in _ROLL_COLUMNS.values():
            inputs[name] = None
        return
    right = (
        existing.drop_duplicates(key)
        .set_index(key)
        .reindex(columns=list(_ROLL_COLUMNS))
        .rename(columns=_ROLL_COLUMNS)
    )
    aligned = right.reindex(inputs["lot_number"].to_numpy())
    aligned.index = inputs.index
    for name in aligned.columns:
        # The gap carries these itself since the enhancement solve; a
        # partition from before it does not. Fill what is missing rather than
        # overwrite what the gap already joined on the same key.
        if name in inputs.columns:
            inputs[name] = inputs[name].where(inputs[name].notna(), aligned[name])
        else:
            inputs[name] = aligned[name]


def _join_zones(
    inputs: pd.DataFrame, hbu: pd.DataFrame, zones: pd.DataFrame
) -> tuple[bool, int]:
    """The governing zone's heritage rows, aligned through the HBU row.

    Zone-level rather than column-level, so the zone columns are collapsed to
    one row per ``(source_table, feature_id)`` before the join - every column
    of one grid carries the same four values. Returns whether the zone file
    carried the heritage columns at all, and how many lots named a zone the
    file had no row for - a lot with no solved program names none, and is
    counted - so the run can say how much of the borough the screen could
    judge.
    """
    known = not zones.empty and all(name in zones.columns for name in _ZONE_COLUMNS)
    keys = ["source_table", "feature_id"]
    if not known or any(name not in inputs.columns for name in keys):
        for name in _ZONE_COLUMNS:
            inputs[name] = None
        inputs.drop(columns=[k for k in keys if k in inputs.columns], inplace=True)
        return known, len(inputs)
    right = (
        zones.dropna(subset=["feature_id"])
        .drop_duplicates(keys)
        .set_index(keys)[list(_ZONE_COLUMNS)]
    )
    index = pd.MultiIndex.from_arrays(
        [inputs["source_table"].to_numpy(), inputs["feature_id"].to_numpy()],
        names=keys,
    )
    matched = int(index.isin(right.index).sum())
    aligned = right.reindex(index)
    aligned.index = inputs.index
    for name in _ZONE_COLUMNS:
        inputs[name] = aligned[name]
    inputs.drop(columns=keys, inplace=True)
    return True, len(inputs) - matched


def _read(
    store: ParquetStore,
    asset_def,
    name: str,
    *,
    neighborhood: str,
    scrape_date: str,
    optional: bool = False,
) -> pd.DataFrame:
    """The upstream partition, or a `Failure` naming what to materialize.

    The same shape `hbu_assets._read` takes, and for the same reason: the
    message that helps names the asset to run rather than the path that was
    absent. ``optional`` returns an empty frame instead, for the one input a
    partition can honestly lack.
    """
    asset_name = asset_def.key.path[-1]
    path = join(store.partition_dir(asset_name, scrape_date, neighborhood), name)
    if not filesystem(path).exists(path):
        if optional:
            return pd.DataFrame()
        raise Failure(
            f"{path} is missing; materialize {asset_name} for "
            f"{neighborhood} {scrape_date} first."
        )
    return pd.read_parquet(path, storage_options=storage_options(path))
