"""The under-built lots worth looking at first, ranked within an investment
thesis.

One gold asset over one upstream. `lot_redevelopment_gap` already answers *how
far is this lot from its highest and best use* for every parcel in the borough,
which is the right question and the wrong shape to act on: twenty-odd thousand
rows, most of them uninteresting, sorted by nothing and faceted by nothing.

`lot_investment_opportunities` turns that into a shortlist. It does exactly two
things the gap table does not - it files each lot under an investment thesis,
and it ranks the under-built ones within that thesis on yield on cost - and the
arithmetic for both is in `urban_rag.opportunities`, free of Dagster the way
`hbu` and `comparables` are.

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

**It re-solves nothing.** Every input is a column `lot_redevelopment_gap`
already wrote, so this is a classification, a division and two sorts over one
parquet file. That is the whole reason it is its own asset rather than four
more columns on the gap table: changing what counts as "mixed-use", or the land
factor, or how many lots make the shortlist, should cost seconds rather than a
borough of CP-SAT models. It is the same split `lot_redevelopment_gap` itself
makes behind `lot_highest_best_use`.

**Every lot keeps its row.** A lot that is not under-built, one the solver found
no program for, and one the roll never assessed all keep a row with a null rank
and a reason - `investment_thesis`, `is_land_assessed` and the gap table's own
`hbu_status` between them say which. The table is an inventory with a shortlist
marked in it, not the shortlist alone: the same reason `lot_profiles` kept every
lot instead of replacing `vacant_lots` with a narrower selection.

The facet summary a reader usually wants first - how many lots per thesis, what
the shortlist yields, what it would cost - is in the run's metadata rather than
in a second table, because it is a `GROUP BY investment_thesis` over the rows
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

from urban_rag.frames import write_frame
from urban_rag.hbu_assets import GOLD_GROUP, LOT_GAP_FILE, lot_redevelopment_gap
from urban_rag.layers import key_prefix
from urban_rag.opportunities import (
    INVESTMENT_THESES,
    ThesisRules,
    rank_opportunities,
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
)


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

    ``land_value_factor`` scales the assessed value on its way into the yield's
    denominator. 1.0 costs the land at the roll, which is the honest default for
    the same reason `ComparablesConfig.market_value_factor` defaults there:
    Quebec's *facteur comparatif* is not in the published roll. A reader who
    knows the year's factor sets it here and gets a yield on something nearer
    what the ground would actually cost to buy.

    ``top_n`` is how many lots per thesis `is_top_opportunity` marks. It moves
    a flag and nothing else - every lot keeps its row and its rank whatever it
    is set to - so it is the cheapest of these to change your mind about.
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
    land_value_factor: float = Field(
        default=1.0,
        gt=0,
        description=(
            "Scales the assessed value into the yield's denominator. 1.0 "
            "costs the land at the roll."
        ),
    )
    top_n: int = Field(
        default=25,
        ge=1,
        description="Lots per thesis that is_top_opportunity marks.",
    )

    def rules(self) -> ThesisRules:
        """The thesis thresholds this run classifies with."""
        return ThesisRules(
            dominant_share=self.dominant_share,
            mixed_min_share=self.mixed_min_share,
        )


@asset(
    key_prefix=key_prefix("lot_investment_opportunities"),
    partitions_def=scrape_partitions,
    deps=[lot_redevelopment_gap],
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
        "there: construction plus the land at its assessed value, scaled by "
        "land_value_factor. thesis_rank orders the under-built lots within "
        "each thesis on that yield, breaking ties on the annual NOI gap, and "
        "is_top_opportunity marks the first top_n of each. A lot that is not "
        "under-built, one the solver found no program for, and one the roll "
        "never assessed each keep their row with a null rank - the table is an "
        "inventory with a shortlist marked in it, not the shortlist alone. "
        "Written to gold/lot_investment_opportunities/<YYYY-MM-DD>/"
        f"<neighborhood>/{LOT_OPPORTUNITIES_FILE} and upserted into "
        "gold.lot_investment_opportunities on (scrape_date, neighborhood, "
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

    rules = config.rules()
    ranked = rank_opportunities(
        gap,
        rules=rules,
        land_value_factor=config.land_value_factor,
        top_n=config.top_n,
    )
    frame = pd.concat(
        [gap[[c for c in _CARRIED if c in gap.columns]], ranked], axis=1
    )
    # Recorded on every row, not only in the run config: `is_top_opportunity`
    # and `investment_thesis` mean nothing without the thresholds behind them,
    # and a table read a month later has only the row.
    frame["screen_assumptions"] = json.dumps(
        {
            **rules.as_metadata(),
            "land_value_factor": config.land_value_factor,
            "top_n": config.top_n,
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
    ranked_rows = int(frame["thesis_rank"].notna().sum())
    context.log.info(
        "%s %s: %d lot(s), %d ranked, %d shortlisted - %s -> %s",
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
            # The judgements, so a run reads back against the one before it.
            "dominant_share": config.dominant_share,
            "mixed_min_share": config.mixed_min_share,
            "land_value_factor": config.land_value_factor,
            "top_n": config.top_n,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


def _read(
    store: ParquetStore,
    asset_def,
    name: str,
    *,
    neighborhood: str,
    scrape_date: str,
) -> pd.DataFrame:
    """The upstream partition, or a `Failure` naming what to materialize.

    The same shape `hbu_assets._read` takes, and for the same reason: the
    message that helps names the asset to run rather than the path that was
    absent.
    """
    asset_name = asset_def.key.path[-1]
    path = join(store.partition_dir(asset_name, scrape_date, neighborhood), name)
    if not filesystem(path).exists(path):
        raise Failure(
            f"{path} is missing; materialize {asset_name} for "
            f"{neighborhood} {scrape_date} first."
        )
    return pd.read_parquet(path, storage_options=storage_options(path))
