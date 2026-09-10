"""Which under-built lots to look at first, and under which investment thesis.

`lot_redevelopment_gap` answers *how far is this lot from its highest and best
use* for every parcel in a borough, which is the right question and the wrong
shape to act on: twenty-two thousand rows, most of them uninteresting, sorted by
nothing. This module turns that into a ranked, faceted shortlist.

Two things happen here and nothing else does.

**A thesis is assigned**, from the *proposed* program rather than the existing
use. The facet describes the opportunity - "the lots where the play is a
mixed-use build" - which is what an investment mandate screens on. A warehouse
whose best use is an apartment block is a *residential* opportunity, and
classifying it as industrial because that is what stands there today would file
it under the one thesis that will never look at it. `existing_income_class` is
carried beside it so a conversion is still visible as one.

**An opportunity is ranked**, on yield on cost, within its thesis. The
alternative - ranking on the raw NOI gap - sorts on parcel size almost
regardless of what a building would cost, so every facet's top ten becomes the
ten biggest lots in the borough. Yield on cost is what a developer actually
compares two sites on, and it lets a small cheap parcel beat a large dear one:

    yield_on_cost_pct = 100 x hbu_annual_stabilised_noi_cad
                            / (hbu_total_capital_cost_cad + land)

**Land is in the denominator at its assessed value**, and that is the one
judgement in the formula. A developer pays for the ground as well as the
building, and leaving it out would rank a $4M teardown beside an empty lot as
though they cost the same to acquire. The roll's assessed value is what this
platform has for that - not a market price, and `market_value_factor` is where a
reader who knows the year's *facteur comparatif* puts it. `is_land_assessed`
says whether a row had one at all, because a lot the roll never reached has its
land counted at nothing and would otherwise rank absurdly well.

**The NOI gap is the tiebreak, not the sort.** Two sites at the same yield are
ordered by how many dollars a year the redevelopment actually adds, so the
ranking prefers return first and size second rather than trading one off
against the other in a weighted score nobody can defend line by line.

**Nothing here re-solves anything.** Every input is a column
`lot_redevelopment_gap` already wrote; this is a classification, a division and
two sorts. That is why it is its own asset: a change to what counts as
"mixed-use", or to the land factor, should cost a sort over a parquet file and
not a borough of CP-SAT models.

**A second axis says why the site is acquirable.** `investment_thesis` names
what you would build; `site_thesis` names why the parcel is on the market at
all - the improvement is obsolete (`teardown`), the use standing on it is one
the contaminated-land regime presumes against (`brownfield`), nothing stands
on it (`infill`), or the building stays and gains a storey or a rear annex
(`improvement`). Those are predicates over the roll's year and storey count,
the solver's storeys and footprint, the use code, and the grid's own
*Patrimoine* rows - so a lot in a *secteur d'interet patrimonial* is kept out
of the two theses that demolish. Each thesis costs its own denominator -
demolition, characterisation and remediation, or the premium an addition pays
over new build - and is ranked within itself on that yield, the way the first
axis is. See `SiteRules` and `rank_site_opportunities`.

Deliberately free of Dagster imports, mirroring `urban_rag.hbu`,
`urban_rag.comparables` and `urban_rag.program`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import pandas as pd

from urban_rag.proforma import ProformaAssumptions, Timing
from urban_rag.proforma import returns as proforma_returns
from urban_rag.proforma import screens as proforma_screens

#: The theses a lot can be filed under, in the order every payload and every
#: count lists them. `none` is not a thesis and is not rankable: it is a lot the
#: solver produced no program for, kept so the facet counts add up to the
#: partition.
RESIDENTIAL = "residential"
MIXED_USE = "mixed_use"
COMMERCIAL = "commercial"
INDUSTRIAL = "industrial"
NO_THESIS = "none"

INVESTMENT_THESES: tuple[str, ...] = (
    RESIDENTIAL,
    MIXED_USE,
    COMMERCIAL,
    INDUSTRIAL,
)

#: Every value `assign_thesis` can answer, including the one that is not a
#: thesis. Named so a reader of the table knows the domain is closed.
THESIS_VALUES: tuple[str, ...] = (*INVESTMENT_THESES, NO_THESIS)

#: The three proposed-floor columns a thesis is read off, and the thesis each
#: one carries when it dominates. Residential and commercial together are what
#: `MIXED_USE` is; industrial does not mix here, because the zoning grids that
#: authorise it in this borough authorise little else and a warehouse with a
#: sales counter is not a mixed-use investment.
_THESIS_COLUMNS: dict[str, str] = {
    "hbu_residential_floor_area_m2": RESIDENTIAL,
    "hbu_commercial_floor_area_m2": COMMERCIAL,
    "hbu_industrial_floor_area_m2": INDUSTRIAL,
}

#: Below this many square metres of proposed floor, a program is not a program.
#: A solver answer of four square metres is a degenerate model rather than a
#: building, and filing it under a thesis would put it in a shortlist.
_MIN_PROGRAM_FLOOR_M2 = 1.0

#: Below this, a denominator is not a denominator - a division by a capital cost
#: of nothing is not an infinite yield.
_MIN_DENOMINATOR = 1e-9

#: Slack on the one comparison between two shares. `1.0 - 0.9` is
#: 0.09999999999999998 in binary, so `ThesisRules(0.9, 0.1)` - a pairing anyone
#: might reasonably choose - fails a strict test of a rule it actually
#: satisfies. Far below any threshold set deliberately.
_SHARE_TOLERANCE = 1e-9


@dataclass(frozen=True)
class ThesisRules:
    """Where the lines between the four theses fall.

    Config rather than constants because "what counts as mixed-use" is a
    mandate's judgement and not a property of the data, and because the two
    thresholds move the facet counts more than anything else here. Every row
    records them - see `as_metadata` - so a shortlist can be read back against
    the rules that produced it, the rule `max_built_area_m2` follows.

    ``dominant_share`` is the share of proposed floor one class needs to own the
    lot outright. 0.85 means a building that is seven-eighths dwellings is a
    residential play even with a shop at the bottom - which is what a residential
    mandate would say about it.

    ``mixed_min_share`` is what the *smaller* of residential and commercial needs
    for the lot to be mixed-use instead. 0.15 is roughly a ground floor under
    five or six residential storeys, which is the point at which the commercial
    component stops being incidental and starts being something a lender asks
    about.

    The two are deliberately not complements. Between them lies a band - one
    class over 85%, the other under 15% - that resolves to the dominant class,
    and that band is the whole reason a single threshold would not do.
    """

    dominant_share: float = 0.85
    mixed_min_share: float = 0.15

    def __post_init__(self) -> None:
        if not 0.0 < self.dominant_share <= 1.0:
            raise ValueError(
                f"dominant_share is a share and must be in (0, 1], got "
                f"{self.dominant_share!r}"
            )
        if not 0.0 < self.mixed_min_share < 0.5:
            # At or above 0.5 the smaller of two shares can never reach it and
            # nothing would ever be mixed-use, which is a setting that silently
            # empties a facet rather than one that narrows it.
            raise ValueError(
                f"mixed_min_share is the smaller share of a two-class mix and "
                f"must be in (0, 0.5), got {self.mixed_min_share!r}"
            )
        if self.mixed_min_share > 1.0 - self.dominant_share + _SHARE_TOLERANCE:
            # Otherwise the two rules disagree about the same lot: a 0.80/0.20
            # split would be both "commercial dominant" and "mixed". Mixed wins
            # in `assign_thesis`, so this would make `dominant_share`
            # unreachable rather than wrong - still worth refusing.
            #
            # Compared with a tolerance because the complement of a share is
            # not exact in binary: `1.0 - 0.9` is 0.09999999999999998, so the
            # perfectly sensible pairing (0.9, 0.1) would be refused by a
            # strict comparison. The tolerance is far below any threshold
            # anyone would set deliberately.
            raise ValueError(
                f"mixed_min_share ({self.mixed_min_share}) must not exceed "
                f"1 - dominant_share ({1.0 - self.dominant_share:.4f}), or the "
                "dominant rule can never fire"
            )

    def as_metadata(self) -> dict[str, float]:
        return {
            "dominant_share": self.dominant_share,
            "mixed_min_share": self.mixed_min_share,
        }


def assign_thesis(frame: pd.DataFrame, rules: ThesisRules | None = None) -> pd.Series:
    """The investment thesis each lot's *proposed* program falls under.

    Read off the three `hbu_*_floor_area_m2` columns as shares of their own
    total, so a lot is classified by what the solver would build on it and not
    by how big that is. A lot with no program - the solver found none, or the
    envelope authorises no dwellings - comes back `none`.

    Mixed-use is tested before dominance, because a lot can satisfy both when
    the thresholds are set close together and "mixed" is the more specific
    claim. `ThesisRules.__post_init__` refuses the settings where that would
    make dominance unreachable.
    """
    rules = rules or ThesisRules()
    areas = pd.DataFrame(
        {
            thesis: _numeric(frame, column)
            for column, thesis in _THESIS_COLUMNS.items()
        }
    )
    total = areas.sum(axis=1, min_count=1)
    has_program = total.notna() & (total > _MIN_PROGRAM_FLOOR_M2)
    shares = areas.div(total.where(has_program), axis=0)

    residential = shares[RESIDENTIAL].fillna(0.0)
    commercial = shares[COMMERCIAL].fillna(0.0)
    industrial = shares[INDUSTRIAL].fillna(0.0)

    mixed = (
        residential.clip(upper=commercial) >= rules.mixed_min_share
    ) & has_program
    thesis = pd.Series(NO_THESIS, index=frame.index, dtype="object")
    # Dominance first, then mixed over the top of it: a lot that is both is
    # mixed, and one that is neither falls through to whichever class is
    # largest - a 60/40 residential/industrial split is a residential play,
    # because that is what most of the building is.
    # `fillna(0.0)` before `idxmax`, and not for tidiness: a lot with no
    # program at all is an all-null row, and pandas raises `Encountered all NA
    # values` on one rather than answering. The zeros never reach the result -
    # `has_program` masks the whole row back to `none` a line later.
    largest = shares.fillna(0.0).idxmax(axis=1)
    thesis = thesis.mask(has_program, largest)
    for column, name in _THESIS_COLUMNS.items():
        dominant = has_program & (shares[name] >= rules.dominant_share)
        thesis = thesis.mask(dominant, name)
    thesis = thesis.mask(mixed, MIXED_USE)
    return thesis.astype("object")


def yield_on_cost_pct(
    frame: pd.DataFrame, *, market_value_factor: float = 1.0
) -> pd.Series:
    """Stabilised NOI over what it costs to get there, in percent.

    The denominator is the construction cost plus the land at its assessed
    value - see the module docstring on why the land is in it. A row missing
    either side comes back null rather than being scored on half a denominator:
    a yield computed without the land would be systematically flattering, and
    silently so.

    Percent, matching `cap_rate_pct` beside it, so the two can be read against
    each other without either being rescaled first.
    """
    noi = _numeric(frame, "hbu_annual_stabilised_noi_cad")
    cost = _numeric(frame, "hbu_total_capital_cost_cad")
    land = _numeric(frame, "existing_total_assessed_value") * float(market_value_factor)
    # A lot the roll never reached has no assessed value. Treating that as land
    # costing nothing would rank it top of every facet, so the whole row is
    # null instead and `is_land_assessed` says why.
    basis = cost + land
    return 100.0 * noi / basis.where(basis.abs() > _MIN_DENOMINATOR)


def rank_opportunities(
    frame: pd.DataFrame,
    *,
    rules: ThesisRules | None = None,
    market_value_factor: float = 1.0,
    top_n: int = 25,
) -> pd.DataFrame:
    """``frame`` with its thesis, its yield, and its rank within that thesis.

    Returns the columns this asset adds, indexed like ``frame``:
    ``investment_thesis``, ``is_land_assessed``, ``yield_on_cost_pct``,
    ``total_project_cost_cad``, ``thesis_rank``, ``is_top_opportunity`` and
    ``num_ranked_in_thesis``.

    **Only under-built lots are ranked.** `is_underbuilt` is
    `lot_redevelopment_gap`'s own screen - an envelope that holds more floor
    than the roll says stands on the parcel - and a lot already built to its
    envelope is not an opportunity however well it would yield if it were
    empty. Everything else keeps its row with a null rank, so the table stays
    an inventory rather than becoming a selection.

    **Rank is dense and within the thesis**, so `thesis_rank = 1` is the best
    residential play *and* the best industrial one. A single borough-wide rank
    would bury every facet under whichever one happens to yield best, which is
    exactly what faceting is for.
    """
    rules = rules or ThesisRules()
    thesis = assign_thesis(frame, rules)
    land = _numeric(frame, "existing_total_assessed_value")
    cost = _numeric(frame, "hbu_total_capital_cost_cad")
    yields = yield_on_cost_pct(frame, market_value_factor=market_value_factor)

    result = pd.DataFrame(index=frame.index)
    result["investment_thesis"] = thesis
    result["is_land_assessed"] = land.notna()
    result["yield_on_cost_pct"] = yields.round(4)
    result["total_project_cost_cad"] = (
        cost + land * float(market_value_factor)
    ).round(2)

    rankable = (
        _boolean(frame, "is_underbuilt")
        & thesis.isin(INVESTMENT_THESES)
        & yields.notna()
    )
    gap = _numeric(frame, "annual_stabilised_noi_gap_cad")
    # Sorted rather than `groupby.rank`, because the tiebreak is a second
    # column: rank on yield, break ties on the dollars a year the
    # redevelopment adds. The **pair** makes the order total, so a re-run of an
    # unchanged partition produces the same shortlist rather than reshuffling
    # two sites that scored identically - and the pair rather than `lot_uid`
    # alone because a row is a piece of a lot now, and a parcel a zoning
    # boundary crosses contributes two of them to the same thesis.
    order = pd.DataFrame(
        {
            "thesis": thesis,
            "yield": yields,
            "gap": gap,
            "tie": _numeric(frame, "lot_uid"),
            "tie_zone": _zone_key(frame),
        }
    )[rankable].sort_values(
        ["thesis", "yield", "gap", "tie", "tie_zone"],
        ascending=[True, False, False, True, True],
        kind="stable",
    )
    ranks = order.groupby("thesis", sort=False).cumcount() + 1
    result["thesis_rank"] = ranks.reindex(frame.index).astype("Int64")
    result["is_top_opportunity"] = (
        result["thesis_rank"].notna() & (result["thesis_rank"] <= int(top_n))
    )
    counts = order.groupby("thesis", sort=False).size()
    result["num_ranked_in_thesis"] = (
        thesis.map(counts).where(rankable).astype("Int64")
    )
    return result


def thesis_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per thesis: how many lots, and what the shortlist looks like.

    The borough-level read this asset exists to support - "is there anything
    industrial here at all, and does it yield" - answered without a reader
    having to aggregate the parcel table themselves. Every thesis in
    `INVESTMENT_THESES` gets a row whether or not any lot fell in it, because a
    facet that is empty is an answer and a missing row is not.
    """
    ranked = frame[frame["thesis_rank"].notna()] if "thesis_rank" in frame else frame
    rows = []
    for thesis in INVESTMENT_THESES:
        in_thesis = frame[frame["investment_thesis"] == thesis]
        scored = ranked[ranked["investment_thesis"] == thesis]
        top = (
            scored[scored["is_top_opportunity"]]
            if "is_top_opportunity" in scored
            else scored
        )
        rows.append(
            {
                "investment_thesis": thesis,
                "num_lots": int(len(in_thesis)),
                "num_ranked": int(len(scored)),
                "num_top": int(len(top)),
                "median_yield_on_cost_pct": _median(scored, "yield_on_cost_pct"),
                "best_yield_on_cost_pct": _max(scored, "yield_on_cost_pct"),
                "total_noi_gap_cad": _total(scored, "annual_stabilised_noi_gap_cad"),
                "top_noi_gap_cad": _total(top, "annual_stabilised_noi_gap_cad"),
                "top_project_cost_cad": _total(top, "total_project_cost_cad"),
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# The second axis: why the site is acquirable
# --------------------------------------------------------------------------

#: The site theses, in **precedence order**: the first that fires names the
#: lot in `site_thesis`, and the others survive as booleans. Brownfield leads
#: because it changes the cost side the most and a gas station is a gas station
#: whatever else is true of it; teardown before infill because a lot with a
#: building on it is not empty; improvement last because keeping the building
#: is what is left once nothing argues for removing it.
BROWNFIELD = "brownfield"
TEARDOWN = "teardown"
INFILL = "infill"
IMPROVEMENT = "improvement"
NO_SITE_THESIS = "none"

SITE_THESES: tuple[str, ...] = (BROWNFIELD, TEARDOWN, INFILL, IMPROVEMENT)
SITE_THESIS_VALUES: tuple[str, ...] = (*SITE_THESES, NO_SITE_THESIS)

#: The boolean each site thesis writes, so a reader can ask "is this also a
#: teardown" of a lot filed under brownfield.
SITE_FLAG_COLUMNS: Mapping[str, str] = {
    BROWNFIELD: "is_brownfield_site",
    TEARDOWN: "is_teardown_site",
    INFILL: "is_infill_site",
    IMPROVEMENT: "is_improvement_site",
}

#: CUBF prefixes whose activities carry a contamination presumption under
#: Quebec's *Reglement sur la protection et la rehabilitation des terrains*:
#: every manufacturing category (2000-3999), motor-vehicle transport yards and
#: garages (42xx), salvage and recycling (487x), vehicle sales and service
#: stations (55xx), dry cleaning (6231), warehousing (63xx) and automotive
#: repair (64xx). A prefix rather than a code list because the roll's leaves
#: are numerous and the presumption attaches to the family. The regulation
#: lists activities by SCIAN, and the codebook `cubf_use_codes` snapshots
#: carries the SCIAN correspondence, so a join can replace this list; a stated
#: list is what a screen can be read back against today.
DEFAULT_BROWNFIELD_USE_PREFIXES: tuple[str, ...] = (
    "2", "3", "42", "487", "55", "6231", "63", "64",
)

#: What the grid prints where a zone-level row does not apply, lower-cased.
#: Anything else against *Secteur d'interet patrimonial* is read as a sector,
#: so ``A`` - one grid in the borough - counts as well as ``Oui``.
_ABSENT_TEXT = {"", "-", "--", "non", "no", "s.o.", "n/a", "na", "none", "nan"}


@dataclass(frozen=True)
class SiteRules:
    """Where the lines fall for the four site theses, and what each costs.

    Every field is a judgement about a mandate rather than a property of the
    data, and every row records them in `screen_assumptions`, the rule
    `ThesisRules` follows.

    **The teardown screen.** ``teardown_max_year_built`` is the youngest a
    building may be and still be presumed obsolete - 1960 takes in the
    borough's inter-war and post-war plexes and leaves the 1960s stock, which
    in this borough is largely built to its envelope anyway.
    ``teardown_max_built_share`` is how little of the envelope the standing
    floor may fill: 0.4 means the building is under two-fifths of what the
    lot could carry. ``min_storey_headroom`` is the storeys the governing
    envelope allows above what stands; below two, a teardown is a like-for-
    like replacement rather than a development, and over 85% of this
    borough's lots have none at all. All three have to hold at once.

    **The brownfield screen** is the use code alone - see
    `DEFAULT_BROWNFIELD_USE_PREFIXES` - and deliberately not the under-built
    test: a gas station built to its envelope is still a conversion play,
    because what changes is the use rather than the floor.

    **Heritage.** ``exclude_heritage_sectors`` keeps every lot whose governing
    zone prints *Secteur d'interet patrimonial: Oui* out of the two theses
    that demolish - and only where something stands to be demolished, see
    `heritage_flags`; the borough's demolition by-law refers those to
    committee, and a contributing building is generally refused.
    ``exclude_piia_sectors``
    does the same for a zone under the discretionary PIIA by-law, and defaults
    **on**: the review's whole subject is how a replacement building meets the
    street, so a demolition-and-rebuild is the case it is written to refuse,
    and a mandate that buys to knock down should not be shown those lots. It
    screens only the theses that demolish - the building may still gain a
    storey or an annex, so the lot keeps its `improvement` thesis and stays
    ranked there, and a lot with no building at all keeps its `brownfield`
    one. ``exclude_piia_sectors=False`` restores the older posture,
    where the PIIA is flagged (`has_piia_review`) and not screened.
    ``demolition_review_year`` is the year below which the *Loi sur le
    patrimoine culturel* obliges a municipality's demolition by-law to apply -
    1940 - and a building older than that is flagged
    `demolition_review_required`; ``exclude_demolition_review`` turns that
    flag into a screen too.

    **The costs** are per square metre and stated, not surveyed per lot:
    ``demolition_cost_cad_per_m2`` over the standing gross floor of a
    residential building and ``demolition_cost_cad_per_m2_nonresidential``
    over anything else - masonry, slab, and the abatement a commercial or
    industrial shell usually carries; ``site_assessment_cost_cad`` once per
    brownfield site, for the Phase I and II characterisation the regime
    requires on a change of use;
    ``remediation_cost_cad_per_m2_residential`` and ``_nonresidential`` over
    the lot, because the residential criterion is the stricter one and a
    rebuild to commerce or industry cleans to a lower bar;
    ``addition_cost_premium`` is what a storey or an annex costs per square
    metre over new build, for the shoring, the tie-ins and the occupied site.
    See docs/site-theses.md for where each default comes from.

    **The improvement screen.** ``improvement_max_added_storeys`` caps the
    storeys added on top of the standing building (one is what a plex owner
    does; two is steel); ``improvement_min_floor_m2`` is the smallest addition
    worth filing - below it a permit and a crane swamp the arithmetic.

    **The lane screen.** ``unassessed_vacant_max_coverage`` is the share of a
    piece's ground a measured building footprint may cover and the piece
    still be read as bare, *on ground the roll never listed*. A lot with no
    assessment unit, no value, no floor, no dwelling and no use code, with
    under that share of it under a building, is a *ruelle*, a park remnant
    or a street sliver, and is kept out of `infill` - the one thesis an
    off-roll lot can reach, and the one whose definition is that the ground
    is ready to build on. 0.05 is the line: on VSMPE 2026-09-01 the pieces
    below it have a median 3.7 m of frontage, and the ones above it carry a
    garage or a shed the roll missed. Either condition alone is a site; see
    `unassessed_vacant`. 0 turns the screen off.

    ``require_positive_npv`` is the rankability screen, the role
    `is_underbuilt` plays on the first axis: a lot keeps its site thesis
    whatever the verdict, and is *ranked* only where redeveloping beats
    holding (or, for an improvement, where the addition earns anything).
    Turned off, a borough whose industrial programs all lose money still gets
    its industrial teardowns ordered.
    """

    teardown_max_year_built: int = 1960
    teardown_max_built_share: float = 0.40
    min_storey_headroom: int = 2
    brownfield_use_prefixes: tuple[str, ...] = DEFAULT_BROWNFIELD_USE_PREFIXES
    exclude_heritage_sectors: bool = True
    exclude_piia_sectors: bool = True
    demolition_review_year: int = 1940
    exclude_demolition_review: bool = False
    demolition_cost_cad_per_m2: float = 150.0
    demolition_cost_cad_per_m2_nonresidential: float = 250.0
    site_assessment_cost_cad: float = 12_000.0
    remediation_cost_cad_per_m2_residential: float = 150.0
    remediation_cost_cad_per_m2_nonresidential: float = 75.0
    addition_cost_premium: float = 1.5
    improvement_max_added_storeys: int = 1
    improvement_min_floor_m2: float = 40.0
    unassessed_vacant_max_coverage: float = 0.05
    require_positive_npv: bool = True

    def __post_init__(self) -> None:
        if not 0.0 < self.teardown_max_built_share <= 1.0:
            raise ValueError(
                "teardown_max_built_share is a share of the envelope and must "
                f"be in (0, 1], got {self.teardown_max_built_share!r}"
            )
        if not 0.0 <= self.unassessed_vacant_max_coverage <= 1.0:
            raise ValueError(
                "unassessed_vacant_max_coverage is a share of the ground and "
                f"must be in [0, 1], got {self.unassessed_vacant_max_coverage!r}"
            )
        if self.min_storey_headroom < 0:
            raise ValueError("min_storey_headroom must not be negative")
        if self.improvement_max_added_storeys < 0:
            raise ValueError("improvement_max_added_storeys must not be negative")
        for name in (
            "demolition_cost_cad_per_m2",
            "demolition_cost_cad_per_m2_nonresidential",
            "site_assessment_cost_cad",
            "remediation_cost_cad_per_m2_residential",
            "remediation_cost_cad_per_m2_nonresidential",
            "improvement_min_floor_m2",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")
        if self.addition_cost_premium <= 0:
            raise ValueError("addition_cost_premium must be positive")
        prefixes = tuple(
            str(prefix).strip() for prefix in self.brownfield_use_prefixes
        )
        if not prefixes or any(not prefix for prefix in prefixes):
            raise ValueError("brownfield_use_prefixes must be non-empty codes")
        object.__setattr__(self, "brownfield_use_prefixes", prefixes)

    def as_metadata(self) -> dict[str, object]:
        return {
            "teardown_max_year_built": self.teardown_max_year_built,
            "teardown_max_built_share": self.teardown_max_built_share,
            "min_storey_headroom": self.min_storey_headroom,
            "brownfield_use_prefixes": list(self.brownfield_use_prefixes),
            "exclude_heritage_sectors": self.exclude_heritage_sectors,
            "exclude_piia_sectors": self.exclude_piia_sectors,
            "demolition_review_year": self.demolition_review_year,
            "exclude_demolition_review": self.exclude_demolition_review,
            "demolition_cost_cad_per_m2": self.demolition_cost_cad_per_m2,
            "demolition_cost_cad_per_m2_nonresidential": (
                self.demolition_cost_cad_per_m2_nonresidential
            ),
            "site_assessment_cost_cad": self.site_assessment_cost_cad,
            "remediation_cost_cad_per_m2_residential": (
                self.remediation_cost_cad_per_m2_residential
            ),
            "remediation_cost_cad_per_m2_nonresidential": (
                self.remediation_cost_cad_per_m2_nonresidential
            ),
            "addition_cost_premium": self.addition_cost_premium,
            "improvement_max_added_storeys": self.improvement_max_added_storeys,
            "improvement_min_floor_m2": self.improvement_min_floor_m2,
            "unassessed_vacant_max_coverage": self.unassessed_vacant_max_coverage,
            "require_positive_npv": self.require_positive_npv,
        }


def storey_headroom(frame: pd.DataFrame) -> pd.Series:
    """The storeys the governing envelope allows above what stands.

    The solver's own storey count less the roll's, so it respects both the
    grid's *En etage* and its height in metres - whichever stopped the
    building first. Null where either side is missing: a lot with no building
    has no headroom to state, because there is nothing to add to.
    """
    return _numeric(frame, "hbu_floors") - _numeric(frame, "existing_num_storeys")


def built_share(frame: pd.DataFrame) -> pd.Series:
    """How much of the proposed floor already stands, as a share.

    Null where the solver proposed no floor: a share of nothing is not a
    share, and a lot with no program is not under-built either.
    """
    proposed = _numeric(frame, "hbu_floor_area_m2")
    standing = _numeric(frame, "existing_floor_area_m2").fillna(0.0)
    return standing / proposed.where(proposed > _MIN_PROGRAM_FLOOR_M2)


def footprint_coverage(frame: pd.DataFrame) -> pd.Series:
    """How much of the piece's ground a *measured* building covers, as a share.

    ``existing_footprint_m2`` on the gap is the cadastre's building polygons
    clipped to the piece by `lot_zone_pieces` - what stands on the ground,
    whatever the roll says - over ``piece_area_m2``, or the parcel where a
    frame from before the pieces has none. Zero where the clip found nothing;
    null where the frame carries no measured footprint at all, which is a
    partition older than the pieces, and null is not zero here: absence of
    the measure is not absence of a building.

    The same column name means something else on
    `lot_investment_opportunities`: there it is `improvement_program`'s
    inferred plate, the roll's floor over its storey count, and is null on
    exactly the lots this share exists for. This reads the gap.
    """
    if "existing_footprint_m2" not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    footprint = _numeric(frame, "existing_footprint_m2").fillna(0.0)
    ground = _numeric(frame, "piece_area_m2")
    ground = ground.where(ground.notna(), _numeric(frame, "lot_area_m2"))
    return footprint / ground.where(ground > 0.0)


def roll_reached(frame: pd.DataFrame) -> pd.Series:
    """Whether the assessment roll describes this ground at all.

    Any sign of it counts - an assessment unit, an assessed value, a stated
    floor, a dwelling or a use code - and not the unit count alone. The count
    is footprint-allocated across a split parcel's pieces and rounded to a
    whole number (`hbu._allocate_existing`), so a bare yard behind an
    assessed building carries ``0`` units while its area-allocated value is
    still on the row: 97 such pieces on VSMPE 2026-09-01, every one of them
    a real site. Nor `has_assessment`, which `use_gap` writes as "the join
    returned something" and is true on every row. A lane, a park remnant or
    a street sliver the roll never lists fails all five.
    """
    units = _numeric(frame, "existing_num_assessment_units")
    value = _numeric(frame, "existing_total_assessed_value")
    floor = _numeric(frame, "existing_floor_area_m2")
    dwellings = _numeric(frame, "existing_num_dwellings")
    use = _text(frame, "existing_dominant_use_code") != ""
    return (
        (units > 0.0).fillna(False)
        | (value > 0.0).fillna(False)
        | (floor > 0.0).fillna(False)
        | (dwellings > 0.0).fillna(False)
        | use
    ).astype("bool")


def unassessed_vacant(
    frame: pd.DataFrame, rules: SiteRules | None = None
) -> pd.Series:
    """Ground the roll never reached, with nothing measurable standing on it.

    Two conditions and both are required: `roll_reached` is false, and
    `footprint_coverage` is under ``SiteRules.unassessed_vacant_max_coverage``.
    Either alone is a site. An assessed parking lot with nothing on it is the
    infill thesis's own case, and an off-roll parcel with a garage over a
    tenth of it is something the roll missed rather than a lane. Both
    together is what a *ruelle* looks like in this data: no unit, no value,
    no use code, a few square metres of a neighbour's shed clipped onto it,
    and no street frontage to speak of. Lot 2 249 035 was the case - 920 m2
    with 16.7 m2 of building on it, nothing on the roll, no frontage - and
    `assign_site_thesis` filed it as an infill, because `empty` is the
    roll's silence and that is what a lane and a vacant lot have in common.
    The measured footprint is what tells them apart.

    Where the coverage is null - a frame with no measured footprint - the
    screen does not fire. A threshold of 0 turns it off.
    """
    rules = rules or SiteRules()
    coverage = footprint_coverage(frame)
    bare = (coverage < float(rules.unassessed_vacant_max_coverage)).fillna(False)
    return (~roll_reached(frame) & bare).astype("bool")


def heritage_flags(
    frame: pd.DataFrame, rules: SiteRules | None = None
) -> pd.DataFrame:
    """What the zone and the roll say about removing the standing building.

    Four booleans. `is_heritage_sector` reads the grid's *Secteur d'interet
    patrimonial*; `has_piia_review` its *PIIA (secteur)*;
    `demolition_review_required` is the sector or a building older than
    `SiteRules.demolition_review_year`; `is_demolition_restricted` is which of
    those the rules turn into a screen, **on a lot with a building to
    demolish**.

    That last clause is the whole difference between a sector flag and a
    screen. A *secteur d'interet patrimonial* and a PIIA both restrict
    removing something, and on a lot where the roll states no floor there is
    nothing to remove: what a mandate proposes there is remediation and new
    construction, which is what `infill` proposes on the same sector
    unscreened. Without the clause the screen dropped `brownfield` off
    exactly those lots and `infill` - which does not read this flag - caught
    them, so a contaminated garage yard was filed under the one thesis whose
    definition is that nothing stands on it. Lot 2 249 816 (grid C01-121,
    CUBF 6419 *Autres services de l'automobile*, PIIA sector, no stated
    floor) was the case: `is_brownfield_use` true and $115k of characterisation
    and remediation charged against it, filed as an infill.

    ``existing_floor_area_m2`` is the test because it is the module's
    definition of a standing building everywhere else - `built_share`, the
    `empty` that gates `infill`, and the demolition cost `site_costs` charges
    all key on it. So the screen now bites exactly where the arithmetic pays
    to demolish something.

    Lifting it made `teardown` reachable on a lot with no floor, which is why
    `assign_site_thesis` now requires standing floor there rather than a
    stated year: `built_share` fills a missing floor with zero, so such a lot
    passed the under-built test at 0.0 and proposed a demolition that cost
    nothing to carry out. Seven VSMPE lots did exactly that before that term
    was tightened.
    """
    rules = rules or SiteRules()
    heritage = _present(frame, "heritage_sector")
    piia = _present(frame, "piia_sector")
    year = _numeric(frame, "existing_year_built")
    old = (year < float(rules.demolition_review_year)).fillna(False).astype("bool")
    review = heritage | old
    demolishable = (_numeric(frame, "existing_floor_area_m2") > 0.0).fillna(False)
    restricted = demolishable & (
        (heritage & rules.exclude_heritage_sectors)
        | (piia & rules.exclude_piia_sectors)
        | (review & rules.exclude_demolition_review)
    )
    return pd.DataFrame(
        {
            "is_heritage_sector": heritage,
            "has_piia_review": piia,
            "demolition_review_required": review.astype("bool"),
            "is_demolition_restricted": restricted.astype("bool"),
        },
        index=frame.index,
    )


def brownfield_use(
    frame: pd.DataFrame, rules: SiteRules | None = None
) -> pd.Series:
    """Whether the dominant use code is one the contaminated-land regime
    presumes against - see `DEFAULT_BROWNFIELD_USE_PREFIXES`."""
    rules = rules or SiteRules()
    prefixes = tuple(rules.brownfield_use_prefixes)
    return _use_code_text(frame).map(
        lambda code: bool(code) and code.startswith(prefixes)
    ).astype("bool")


def improvement_program(
    frame: pd.DataFrame, rules: SiteRules | None = None
) -> pd.DataFrame:
    """The storey or annex the standing building could gain, and what it earns.

    Two sources, and the row says which. Where the gap table carries an
    **enhancement solve** - `hbu.solve_enhancements`, the same CP-SAT model
    with the standing building retained - its answer is the program: the
    storeys and floor it added, what that cost at the addition premium, and
    the stabilised NOI the new floor earns. Where it does not (a partition
    from before the solve, or a lot it could not be run on) the program is
    the **estimate**: the standing footprint - the roll's floor area over
    its storey count - repeated up to ``improvement_max_added_storeys``
    times and no higher than the solver's own storey count, plus an annex on
    the ground the solver's footprint covers and the standing one does not,
    the two capped at the floor gap and priced at the solver's NOI and
    capital cost per square metre times ``addition_cost_premium``.

    A lot the roll states no storey count for gets no estimate: guessing a
    footprint would put an invented building on the shortlist.
    """
    rules = rules or SiteRules()
    floor = _numeric(frame, "existing_floor_area_m2")
    storeys = _numeric(frame, "existing_num_storeys")
    hbu_floors = _numeric(frame, "hbu_floors")
    hbu_floor = _numeric(frame, "hbu_floor_area_m2")
    hbu_footprint = _numeric(frame, "hbu_footprint_m2")
    gap = _numeric(frame, "floor_area_gap_m2")
    noi = _numeric(frame, "hbu_annual_stabilised_noi_cad")
    capex = _numeric(frame, "hbu_total_capital_cost_cad")

    footprint = floor / storeys.where(storeys >= 1.0)
    added_storeys = (hbu_floors - storeys).clip(
        lower=0.0, upper=float(rules.improvement_max_added_storeys)
    )
    storey_floor = added_storeys * footprint
    annex_floor = (hbu_footprint - footprint).clip(lower=0.0) * storeys.clip(
        upper=hbu_floors
    )
    total = (storey_floor.fillna(0.0) + annex_floor.fillna(0.0)).where(
        footprint.notna()
    )
    estimate = pd.concat([total, gap], axis=1).min(axis=1, skipna=False)
    estimate = estimate.where(floor > 0.0).clip(lower=0.0)
    denominator = hbu_floor.where(hbu_floor > _MIN_PROGRAM_FLOOR_M2)
    estimate_noi = estimate * (noi / denominator)
    estimate_cost = (
        estimate * (capex / denominator) * float(rules.addition_cost_premium)
    )

    solved = _boolean(frame, "enhance_solved")
    solve_floor = _numeric(frame, "enhance_added_floor_area_m2")
    solve_storeys = _numeric(frame, "enhance_added_storeys")
    solve_cost = _numeric(frame, "enhance_capital_cost_cad")
    solve_noi = _numeric(frame, "enhance_added_annual_stabilised_noi_cad")

    improvement = estimate.mask(solved, solve_floor)
    added = added_storeys.mask(solved, solve_storeys)
    added_cost = estimate_cost.mask(solved, solve_cost)
    added_noi = estimate_noi.mask(solved, solve_noi)
    yield_pct = (
        100.0 * added_noi / added_cost.where(added_cost.abs() > _MIN_DENOMINATOR)
    )
    source = pd.Series(
        np.where(solved, "solve", np.where(improvement.notna(), "estimate", "none")),
        index=frame.index,
        dtype="object",
    )
    return pd.DataFrame(
        {
            "existing_footprint_m2": footprint.round(2),
            "improvement_source": source,
            "improvement_added_storeys": added.round(0).astype("Int64"),
            "improvement_floor_m2": improvement.round(2),
            "improvement_cost_cad": added_cost.round(2),
            "improvement_noi_cad": added_noi.round(2),
            "improvement_yield_pct": yield_pct.round(4),
        },
        index=frame.index,
    )


def site_costs(
    frame: pd.DataFrame, use: pd.Series, rules: SiteRules | None = None
) -> pd.DataFrame:
    """What clearing the site costs, before anything is built on it.

    Demolition of the standing floor on every lot that has some, at the rate
    for the building's class; and on a contamination-risk use, the Phase I
    and II characterisation once and the remediation over the lot, to the
    residential criterion where the proposal includes housing and the lower
    one otherwise. ``site_costs_cad`` is the three added up, and is what the
    rebuild carries that the hold and the enhancement do not.
    """
    rules = rules or SiteRules()
    floor = _numeric(frame, "existing_floor_area_m2").fillna(0.0)
    lot_area = _numeric(frame, "lot_area_m2").fillna(0.0)
    brownfield = use.astype("bool").to_numpy()

    is_residential = (
        _text(frame, "existing_dominant_income_class").str.lower() == RESIDENTIAL
    ).to_numpy()
    demolition_rate = np.where(
        is_residential,
        float(rules.demolition_cost_cad_per_m2),
        float(rules.demolition_cost_cad_per_m2_nonresidential),
    )
    demolition = pd.Series(
        np.where(floor.to_numpy() > 0.0, floor.to_numpy() * demolition_rate, 0.0),
        index=frame.index,
    )
    assessment = pd.Series(
        np.where(brownfield, float(rules.site_assessment_cost_cad), 0.0),
        index=frame.index,
    )
    proposed = _numeric(frame, "hbu_floor_area_m2")
    residential = _numeric(frame, "hbu_residential_floor_area_m2").fillna(0.0)
    to_housing = (
        (residential / proposed.where(proposed > _MIN_PROGRAM_FLOOR_M2)) > 0.0
    ).fillna(False).to_numpy()
    rate = np.where(
        to_housing,
        float(rules.remediation_cost_cad_per_m2_residential),
        float(rules.remediation_cost_cad_per_m2_nonresidential),
    )
    remediation = pd.Series(
        np.where(brownfield, lot_area.to_numpy() * rate, 0.0), index=frame.index
    )
    return pd.DataFrame(
        {
            "demolition_cost_cad": demolition.round(2),
            "site_assessment_cost_cad": assessment.round(2),
            "remediation_cost_cad": remediation.round(2),
            "site_costs_cad": (demolition + assessment + remediation).round(2),
        },
        index=frame.index,
    )


#: The three things that can be done with a lot, in the order a tie resolves.
FUTURES: tuple[str, ...] = ("hold", "enhance", "rebuild")
#: The buyer's fourth: walk away, where no future clears the discount rate at
#: the price. Not a future the owner has - holding costs an owner nothing.
NO_FUTURE = "none"


def futures_economics(
    frame: pd.DataFrame,
    costs: pd.DataFrame,
    rules: SiteRules | None = None,
    *,
    market_value_factor: float = 1.0,
) -> pd.DataFrame:
    """Hold, enhance and rebuild, priced for the owner and for a buyer.

    **The owner** holds the land in every future, so it cancels: the three
    values are the gap table's - the standing building's discounted NOI, that
    plus the addition's gain, the proposal's NPV with its income pushed out
    by the build - with the site's own costs taken off the rebuild here.
    ``owner_gain_*`` is each against holding, and ``owner_best_future`` the
    largest, ``hold`` on a tie.

    **The buyer** pays for the land and the building first. The price is the
    larger of what the roll says the property is worth, scaled by
    ``market_value_factor`` for what the roll misses, and what the standing
    income is worth to whoever holds it: a seller keeps the better of the
    two. Each future's ``buyer_npv_*`` is its value less that price;
    ``residual_price_*`` is the most a buyer could pay and still clear the
    discount rate, which is the value itself; ``buyer_yield_*`` is the
    stabilised NOI of the future over everything paid to reach it.
    ``buyer_best_future`` is the largest NPV, ``hold`` on a tie, and ``none``
    where even the largest is below zero: at that price the buyer walks.

    Every buyer column is null where the roll never assessed the lot - there
    is no price to pay - and every enhance column where no enhancement was
    solved.
    """
    rules = rules or SiteRules()
    existing_pv = _numeric(frame, "existing_present_value_cad")
    hold = _numeric(frame, "hold_value_cad")
    hold = hold.where(hold.notna(), existing_pv).fillna(0.0)

    enhance_gain = _numeric(frame, "enhance_gain_cad")
    enhance_value = (hold + enhance_gain).where(enhance_gain.notna())

    rebuild_raw = _numeric(frame, "rebuild_value_cad")
    rebuild_raw = rebuild_raw.where(rebuild_raw.notna(), _numeric(frame, "hbu_npv_cad"))
    rebuild_raw = rebuild_raw.where(
        rebuild_raw.notna(), hold + _numeric(frame, "redevelopment_npv_gain_cad")
    )
    site = _numeric(costs, "site_costs_cad").fillna(0.0)
    rebuild_value = rebuild_raw - site

    owner_gain_rebuild = rebuild_value - hold
    owner = pd.DataFrame(
        {
            "hold": pd.Series(0.0, index=frame.index),
            "enhance": enhance_gain,
            "rebuild": owner_gain_rebuild,
        }
    )
    owner_best = owner.fillna(-np.inf).idxmax(axis=1)

    assessed = _numeric(frame, "existing_total_assessed_value") * float(market_value_factor)
    acquisition = pd.concat([assessed, hold], axis=1).max(axis=1).where(assessed.notna())

    buyer_hold = hold - acquisition
    buyer_enhance = enhance_value - acquisition
    buyer_rebuild = rebuild_value - acquisition
    buyer = pd.DataFrame(
        {"hold": buyer_hold, "enhance": buyer_enhance, "rebuild": buyer_rebuild}
    )
    # The largest NPV, and only where it clears zero: a buyer whose every
    # future loses money at the price has a fourth option the owner does not,
    # which is not to buy. `none` says so; the least-bad loss is not a best.
    filled = buyer.fillna(-np.inf)
    buyer_best = filled.idxmax(axis=1).where(filled.max(axis=1) >= 0.0, NO_FUTURE)
    buyer_best = buyer_best.where(acquisition.notna())

    existing_noi = _numeric(frame, "existing_annual_stabilised_noi_cad").fillna(0.0)
    added_noi = _numeric(frame, "enhance_added_annual_stabilised_noi_cad")
    enhance_capex = _numeric(frame, "enhance_capital_cost_cad")
    hbu_noi = _numeric(frame, "hbu_annual_stabilised_noi_cad")
    hbu_capex = _numeric(frame, "hbu_total_capital_cost_cad")

    def yield_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
        return 100.0 * numerator / denominator.where(denominator.abs() > _MIN_DENOMINATOR)

    return pd.DataFrame(
        {
            "owner_hold_value_cad": hold.round(2),
            "owner_enhance_value_cad": enhance_value.round(2),
            "owner_rebuild_value_cad": rebuild_value.round(2),
            "owner_gain_enhance_cad": enhance_gain.round(2),
            "owner_gain_rebuild_cad": owner_gain_rebuild.round(2),
            "owner_best_future": owner_best.astype("object"),
            "acquisition_cost_cad": acquisition.round(2),
            "buyer_npv_hold_cad": buyer_hold.round(2),
            "buyer_npv_enhance_cad": buyer_enhance.round(2),
            "buyer_npv_rebuild_cad": buyer_rebuild.round(2),
            "buyer_yield_hold_pct": yield_pct(existing_noi, acquisition).round(4),
            "buyer_yield_enhance_pct": yield_pct(
                existing_noi + added_noi, acquisition + enhance_capex
            ).round(4),
            "buyer_yield_rebuild_pct": yield_pct(
                hbu_noi, acquisition + hbu_capex + site
            ).round(4),
            "residual_price_enhance_cad": enhance_value.round(2),
            "residual_price_rebuild_cad": rebuild_value.round(2),
            "buyer_best_future": buyer_best.astype("object"),
        },
        index=frame.index,
    )


def assign_site_thesis(
    frame: pd.DataFrame,
    rules: SiteRules | None = None,
    futures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Which site theses fire on each lot, and the one that names it.

    Returns the four `SITE_FLAG_COLUMNS`, the heritage flags,
    `is_brownfield_use`, `storey_headroom`, `built_share`,
    `existing_footprint_coverage` and `is_unassessed_vacant` (the lane
    screen, see `unassessed_vacant`), the improvement program and
    `site_thesis` - the first of `SITE_THESES` whose flag is set, or
    ``none``.

    ``futures`` is `futures_economics`' frame, and with it the teardown
    follows the money: a lot whose enhancement is worth more to its owner
    than its rebuild is not a teardown however old the building, and is
    filed under `improvement` instead. Without it - a caller holding only
    the gap - the thresholds decide alone.
    """
    rules = rules or SiteRules()
    heritage = heritage_flags(frame, rules)
    restricted = heritage["is_demolition_restricted"]
    has_program = (
        _numeric(frame, "hbu_floor_area_m2") > _MIN_PROGRAM_FLOOR_M2
    ).fillna(False)
    underbuilt = _boolean(frame, "is_underbuilt")
    floor = _numeric(frame, "existing_floor_area_m2")
    dwellings = _numeric(frame, "existing_num_dwellings").fillna(0.0)
    year = _numeric(frame, "existing_year_built")
    headroom = storey_headroom(frame)
    share = built_share(frame)
    coverage = footprint_coverage(frame)
    off_roll = unassessed_vacant(frame, rules)
    use = brownfield_use(frame, rules)
    program = improvement_program(frame, rules)

    # One definition of a standing building, and every thesis reads it: the
    # roll's floor area. `built_share` divides by it, `site_costs` charges
    # demolition on it, `empty` is its negation, and `heritage_flags` screens
    # on it. A stated year of construction is not a second definition - a lot
    # the roll dates but states no floor for was `standing` here and `empty`
    # two lines down, which let a teardown fire on it: `built_share` fills a
    # missing floor with zero rather than null, so 0.0 <= 0.40 passes the
    # under-built test and the demolition it proposed cost nothing, because
    # there was no floor to charge for.
    built = (floor > 0.0).fillna(False)
    empty = ~built & (dwellings <= 0.0)

    enhance_beats_rebuild = pd.Series(False, index=frame.index)
    if futures is not None:
        gain_enhance = _numeric(futures, "owner_gain_enhance_cad")
        gain_rebuild = _numeric(futures, "owner_gain_rebuild_cad")
        enhance_beats_rebuild = (
            gain_enhance.notna() & gain_rebuild.notna() & (gain_enhance > gain_rebuild)
        )

    is_brownfield = use & has_program & ~restricted
    is_teardown = (
        built
        & has_program
        & underbuilt
        & (year <= float(rules.teardown_max_year_built)).fillna(False)
        & (share <= rules.teardown_max_built_share).fillna(False)
        & (headroom >= float(rules.min_storey_headroom)).fillna(False)
        & ~restricted
        & ~enhance_beats_rebuild
    )
    # The one thesis ground the roll never listed can reach: the other three
    # need a use code or a stated floor, which is the roll speaking. So a
    # lane, a park remnant or a street sliver could only ever land here, and
    # did - `empty` is the roll's silence, which a *ruelle* and a vacant lot
    # have in common. `unassessed_vacant` reads the measured footprint to
    # tell them apart; 610 VSMPE pieces with a median 3.7 m of frontage were
    # filed as infills before it did.
    is_infill = empty & has_program & underbuilt & ~off_roll
    is_improvement = (
        built
        & has_program
        & underbuilt
        & (
            program["improvement_floor_m2"] >= float(rules.improvement_min_floor_m2)
        ).fillna(False)
    )

    flags = {
        SITE_FLAG_COLUMNS[BROWNFIELD]: is_brownfield.astype("bool"),
        SITE_FLAG_COLUMNS[TEARDOWN]: is_teardown.astype("bool"),
        SITE_FLAG_COLUMNS[INFILL]: is_infill.astype("bool"),
        SITE_FLAG_COLUMNS[IMPROVEMENT]: is_improvement.astype("bool"),
    }
    thesis = pd.Series(NO_SITE_THESIS, index=frame.index, dtype="object")
    # Reverse precedence, so the earlier thesis overwrites the later one.
    for name in reversed(SITE_THESES):
        thesis = thesis.mask(flags[SITE_FLAG_COLUMNS[name]], name)

    result = pd.DataFrame(index=frame.index)
    result["storey_headroom"] = headroom.round(0).astype("Int64")
    result["built_share"] = share.round(4)
    result["existing_footprint_coverage"] = coverage.round(4)
    result["is_brownfield_use"] = use
    result["is_unassessed_vacant"] = off_roll
    for column in heritage.columns:
        result[column] = heritage[column]
    for column, flag in flags.items():
        result[column] = flag
    result["site_thesis"] = thesis.astype("object")
    for column in program.columns:
        result[column] = program[column]
    return result


def site_yield_on_cost_pct(
    frame: pd.DataFrame,
    assigned: pd.DataFrame,
    rules: SiteRules | None = None,
    *,
    market_value_factor: float = 1.0,
    costs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Each site thesis's own denominator, and the yield on it.

    For the three theses that clear the ground the numerator is the proposed
    building's stabilised NOI, as on the first axis, and the denominator is
    construction plus the property at its assessed value plus what the site
    itself costs - `site_costs`. For an improvement the building stays, so
    the yield is the addition's own: what the added floor earns over what it
    costs, from `improvement_program`.

    ``site_total_project_cost_cad`` is the denominator, kept so the yield is
    checkable from the row; on an improvement it is the addition's cost.
    """
    rules = rules or SiteRules()
    if costs is None:
        costs = site_costs(frame, assigned["is_brownfield_use"], rules)
    noi = _numeric(frame, "hbu_annual_stabilised_noi_cad")
    capex = _numeric(frame, "hbu_total_capital_cost_cad")
    land = _numeric(frame, "existing_total_assessed_value") * float(market_value_factor)
    basis = capex + land + _numeric(costs, "site_costs_cad").fillna(0.0)
    cleared = 100.0 * noi / basis.where(basis.abs() > _MIN_DENOMINATOR)

    improving = assigned["site_thesis"] == IMPROVEMENT
    site_yield = cleared.mask(improving, _numeric(assigned, "improvement_yield_pct"))
    total = basis.mask(improving, _numeric(assigned, "improvement_cost_cad"))
    return pd.DataFrame(
        {
            "demolition_cost_cad": costs["demolition_cost_cad"],
            "site_assessment_cost_cad": costs["site_assessment_cost_cad"],
            "remediation_cost_cad": costs["remediation_cost_cad"],
            "site_total_project_cost_cad": total.round(2),
            "site_yield_on_cost_pct": site_yield.round(4),
        },
        index=frame.index,
    )


#: The timing a partition from before the delay existed is read as: the
#: building standing on day one, sold at the module's cap. Only reached when
#: the caller hands over nothing, which the asset never does.
_DEFAULT_TIMING = Timing(
    construction_months=0, lease_up_months=0, hold_years=25,
    terminal_cap_rate_pct=4.5, discount_rate_pct=5.0,
)


def rank_site_opportunities(
    frame: pd.DataFrame,
    *,
    rules: SiteRules | None = None,
    market_value_factor: float = 1.0,
    top_n: int = 25,
    proforma: ProformaAssumptions | None = None,
    rebuild_timing: Timing | None = None,
    enhance_timing: Timing | None = None,
) -> pd.DataFrame:
    """``frame`` filed under its site thesis, costed, priced for the owner and
    the buyer, its returns stated, and ranked within the thesis.

    Returns everything `assign_site_thesis`, `site_yield_on_cost_pct`,
    `futures_economics`, `proforma.returns` and `proforma.screens` add, plus
    ``site_thesis_rank``, ``is_top_site_opportunity`` and
    ``num_ranked_in_site_thesis``, indexed like ``frame``.

    **Rank is within the site thesis**, on the buyer's IRR of that thesis's
    own future - the addition on an improvement, the rebuild on the rest -
    with the all-in yield on cost, then the owner's verdict, then `lot_uid`
    as tiebreaks, so a re-run of an unchanged partition produces the same
    list. A lot the roll never priced has no IRR and is unranked; its
    thesis and the owner's numbers stand.

    **Only lots where the play pays are ranked** when
    `SiteRules.require_positive_npv` holds: the owner's verdict above zero.
    `is_good_candidate` is the stricter test on top of the rank - the IRR
    over the hurdle or the yield over the area's cap rate plus the spread,
    either bar, which is what a screen should surface first.

    ``rebuild_timing`` and ``enhance_timing`` are the months and the hold the
    two futures were solved with, read off the rows by the asset; the module
    default is a building standing on day one, for a caller that has none.
    """
    rules = rules or SiteRules()
    proforma = proforma or ProformaAssumptions()
    rebuild_timing = rebuild_timing or _DEFAULT_TIMING
    enhance_timing = enhance_timing or rebuild_timing
    use = brownfield_use(frame, rules)
    costs = site_costs(frame, use, rules)
    futures = futures_economics(
        frame, costs, rules, market_value_factor=market_value_factor
    )
    assigned = assign_site_thesis(frame, rules, futures)
    economics = site_yield_on_cost_pct(
        frame, assigned, rules, market_value_factor=market_value_factor, costs=costs
    )
    result = pd.concat([assigned, economics, futures], axis=1)

    thesis = result["site_thesis"]
    improving = thesis == IMPROVEMENT
    rebuild_gain = _numeric(futures, "owner_gain_rebuild_cad")
    enhance_gain = _numeric(futures, "owner_gain_enhance_cad")
    improvement_verdict = enhance_gain.where(
        enhance_gain.notna(), _numeric(result, "improvement_noi_cad")
    )
    verdict = rebuild_gain.mask(improving, improvement_verdict)
    result["site_verdict_cad"] = verdict.round(2)

    # The returns: the all-in budget, the IRRs and the screens, on the rows
    # as priced so far. `pd.concat` twice rather than once so the screens can
    # read `site_verdict_cad` and `site_thesis` off the same frame.
    priced = pd.concat([frame, result], axis=1)
    priced = priced.loc[:, ~priced.columns.duplicated()]
    stated = proforma_returns(priced, proforma, rebuild_timing, enhance_timing)
    verdicts = proforma_screens(
        pd.concat([priced, stated], axis=1), stated, proforma
    )
    result = pd.concat([result, stated, verdicts], axis=1)

    irr = _numeric(verdicts, "site_irr_pct")
    yields = _numeric(verdicts, "site_all_in_yield_on_cost_pct")
    rankable = thesis.isin(SITE_THESES) & irr.notna()
    if rules.require_positive_npv:
        rankable &= (verdict > 0.0).fillna(False)

    order = pd.DataFrame(
        {
            "thesis": thesis,
            "irr": irr,
            "yield": yields,
            "verdict": verdict,
            "tie": _numeric(frame, "lot_uid"),
            "tie_zone": _zone_key(frame),
        }
    )[rankable].sort_values(
        ["thesis", "irr", "yield", "verdict", "tie", "tie_zone"],
        ascending=[True, False, False, False, True, True],
        kind="stable",
    )
    ranks = order.groupby("thesis", sort=False).cumcount() + 1
    result["site_thesis_rank"] = ranks.reindex(frame.index).astype("Int64")
    result["is_top_site_opportunity"] = (
        result["site_thesis_rank"].notna()
        & (result["site_thesis_rank"] <= int(top_n))
    )
    counts = order.groupby("thesis", sort=False).size()
    result["num_ranked_in_site_thesis"] = (
        thesis.map(counts).where(rankable).astype("Int64")
    )
    return result


def site_thesis_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per site thesis: how many lots, how many pay, what they yield.

    Every thesis in `SITE_THESES` gets a row whether or not any lot fell in
    it, for the reason `thesis_summary` does: an empty facet is an answer.
    ``total_verdict_cad`` is `site_verdict_cad` summed over the ranked lots -
    the owner's gain over holding, in dollars, on every thesis.
    """
    ranked = (
        frame[frame["site_thesis_rank"].notna()]
        if "site_thesis_rank" in frame
        else frame
    )
    rows = []
    for thesis in SITE_THESES:
        in_thesis = frame[frame["site_thesis"] == thesis]
        scored = ranked[ranked["site_thesis"] == thesis]
        top = (
            scored[scored["is_top_site_opportunity"]]
            if "is_top_site_opportunity" in scored
            else scored
        )
        rows.append(
            {
                "site_thesis": thesis,
                "num_lots": int(len(in_thesis)),
                "num_ranked": int(len(scored)),
                "num_top": int(len(top)),
                "num_good_candidates": int(
                    scored["is_good_candidate"].fillna(False).sum()
                ) if "is_good_candidate" in scored else 0,
                "median_site_yield_on_cost_pct": _median(
                    scored, "site_yield_on_cost_pct"
                ),
                "best_site_yield_on_cost_pct": _max(scored, "site_yield_on_cost_pct"),
                "median_site_irr_pct": _median(scored, "site_irr_pct"),
                "best_site_irr_pct": _max(scored, "site_irr_pct"),
                "median_all_in_yield_on_cost_pct": _median(
                    scored, "site_all_in_yield_on_cost_pct"
                ),
                "total_verdict_cad": _total(scored, "site_verdict_cad"),
                "top_project_cost_cad": _total(top, "site_total_project_cost_cad"),
                "ranked_lot_area_ha": round(
                    float(_numeric(scored, "lot_area_m2").sum()) / 10_000.0, 2
                ),
            }
        )
    return pd.DataFrame(rows)


def futures_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per future: how many lots it wins for the owner and for a
    buyer, and what those wins are worth. The borough-level read of the
    three futures, for the run's metadata."""
    rows = []
    for future in FUTURES:
        owner_wins = frame[frame.get("owner_best_future", pd.Series(dtype=object)) == future]
        buyer_wins = frame[frame.get("buyer_best_future", pd.Series(dtype=object)) == future]
        rows.append(
            {
                "future": future,
                "owner_wins": int(len(owner_wins)),
                "buyer_wins": int(len(buyer_wins)),
                "owner_gain_cad": (
                    _total(owner_wins, f"owner_gain_{future}_cad")
                    if future != "hold" else 0.0
                ),
                "buyer_npv_cad": _total(buyer_wins, f"buyer_npv_{future}_cad"),
            }
        )
    return pd.DataFrame(rows)


def _zone_key(frame: pd.DataFrame) -> pd.Series:
    """The zone of each row, as the second half of a total sort order.

    A shortlist row is a piece of a lot since `lot_zone_pieces`, so `lot_uid`
    alone no longer distinguishes two rows: a parcel a zoning boundary crosses
    contributes one per zone, and two of them can land in the same thesis at
    the same yield. Without this the order between them is whatever the sort
    was handed, and a re-run of an unchanged partition can renumber a
    shortlist - which is the one thing the tiebreak exists to prevent.

    Empty string where the column is absent, which is a hand-built frame in a
    test and orders the same way on every row.
    """
    if "feature_id" not in frame.columns:
        return pd.Series("", index=frame.index, dtype="object")
    return frame["feature_id"].astype("string").fillna("")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    """One column as float64, or all-NaN when the frame does not carry it."""
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").astype("float64")


def _boolean(frame: pd.DataFrame, column: str) -> pd.Series:
    """One column as a plain bool, with missing read as False.

    `is_underbuilt` arrives from a parquet round trip where an all-null column
    is object dtype, and `~` on one of those is arithmetic negation rather than
    logical - the same trap `role_assets._empty_pairs` documents.
    """
    if column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype="bool")
    return frame[column].fillna(False).astype("bool")


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    """One column as stripped text, with every kind of missing as ``""``."""
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype="object")
    values = frame[column].astype("object").where(frame[column].notna(), "")
    return values.map(lambda value: str(value).strip()).astype("object")


def _present(frame: pd.DataFrame, column: str) -> pd.Series:
    """Whether a zone-level text row states anything - see `_ABSENT_TEXT`."""
    return _text(frame, column).map(
        lambda value: value.lower() not in _ABSENT_TEXT
    ).astype("bool")


def _use_code_text(frame: pd.DataFrame) -> pd.Series:
    """`existing_dominant_use_code` as the four-character string it is.

    A parquet round trip can hand a code back as ``1000.0``; the comparables
    module makes the same repair on the way in, and this is that repair again
    for a frame that skipped it.
    """
    codes = _text(frame, "existing_dominant_use_code")
    return codes.map(
        lambda code: code[:-2] if code.endswith(".0") and code[:-2].isdigit() else code
    )


def _median(frame: pd.DataFrame, column: str) -> float | None:
    values = _numeric(frame, column).dropna()
    return round(float(values.median()), 4) if len(values) else None


def _max(frame: pd.DataFrame, column: str) -> float | None:
    values = _numeric(frame, column).dropna()
    return round(float(values.max()), 4) if len(values) else None


def _total(frame: pd.DataFrame, column: str) -> float | None:
    values = _numeric(frame, column).dropna()
    return round(float(values.sum()), 2) if len(values) else None
