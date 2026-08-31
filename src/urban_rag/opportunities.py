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
platform has for that - not a market price, and `land_value_factor` is where a
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

Deliberately free of Dagster imports, mirroring `urban_rag.hbu`,
`urban_rag.comparables` and `urban_rag.program`.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

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
    frame: pd.DataFrame, *, land_value_factor: float = 1.0
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
    land = _numeric(frame, "existing_total_assessed_value") * float(land_value_factor)
    # A lot the roll never reached has no assessed value. Treating that as land
    # costing nothing would rank it top of every facet, so the whole row is
    # null instead and `is_land_assessed` says why.
    basis = cost + land
    return 100.0 * noi / basis.where(basis.abs() > _MIN_DENOMINATOR)


def rank_opportunities(
    frame: pd.DataFrame,
    *,
    rules: ThesisRules | None = None,
    land_value_factor: float = 1.0,
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
    yields = yield_on_cost_pct(frame, land_value_factor=land_value_factor)

    result = pd.DataFrame(index=frame.index)
    result["investment_thesis"] = thesis
    result["is_land_assessed"] = land.notna()
    result["yield_on_cost_pct"] = yields.round(4)
    result["total_project_cost_cad"] = (
        cost + land * float(land_value_factor)
    ).round(2)

    rankable = (
        _boolean(frame, "is_underbuilt")
        & thesis.isin(INVESTMENT_THESES)
        & yields.notna()
    )
    gap = _numeric(frame, "annual_stabilised_noi_gap_cad")
    # Sorted rather than `groupby.rank`, because the tiebreak is a second
    # column: rank on yield, break ties on the dollars a year the
    # redevelopment adds. `lot_uid` makes the order total, so a re-run of an
    # unchanged partition produces the same shortlist rather than reshuffling
    # two lots that scored identically.
    order = pd.DataFrame(
        {
            "thesis": thesis,
            "yield": yields,
            "gap": gap,
            "tie": _numeric(frame, "lot_uid"),
        }
    )[rankable].sort_values(
        ["thesis", "yield", "gap", "tie"],
        ascending=[True, False, False, True],
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


def _median(frame: pd.DataFrame, column: str) -> float | None:
    values = _numeric(frame, column).dropna()
    return round(float(values.median()), 4) if len(values) else None


def _max(frame: pd.DataFrame, column: str) -> float | None:
    values = _numeric(frame, column).dropna()
    return round(float(values.max()), 4) if len(values) else None


def _total(frame: pd.DataFrame, column: str) -> float | None:
    values = _numeric(frame, column).dropna()
    return round(float(values.sum()), 2) if len(values) else None
