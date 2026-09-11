"""The returns behind a future: an unlevered proforma, its IRR, and the screens.

`urban_rag.program` prices a building the way a solver has to - one present
value, hard cost at day one, no soft cost, no sale cost - because every term
has to fold into a linear coefficient. That is the right shape for choosing
an envelope and the wrong shape for deciding whether to write a cheque. This
module takes the futures the gap and the shortlist already priced and puts
the rest of a development budget and a timeline around them:

* **the budget**: hard cost, plus soft costs (design, permits, fees, legal,
  marketing), a contingency, and builder's-risk insurance for the months of
  construction, each a stated share of hard cost; the site's own costs; and,
  for a buyer, the acquisition;
* **the timeline**: the acquisition and the site costs at day one, the
  budget spent evenly over the construction months, the income filling
  linearly over the lease-up - which is the longer of the solve's own
  lease-up and what an absorption rate in dwellings a month says this many
  units take - then stabilised for the hold, and the sale at the terminal
  cap less selling costs;
* **the returns**: the unlevered IRR of that stream, the equity multiple,
  and the stabilised yield on the all-in cost;
* **the screens**: yield on cost against the area's cap rate plus a
  development spread, and the IRR against a hurdle. A good candidate clears
  both and pays against holding.

Two perspectives, because the same lot has two IRRs. **A buyer** pays the
acquisition at day one and gets the whole building. **An owner** already has
the building, so the outlay is the budget alone and the return is the
*increment*: the new income less the standing income they give up during
the works and after, and at the sale the difference in value. The owner's
IRR is what "should I do this" is asked in; the buyer's is what "should I
buy this to do it" is asked in.

**And a building is not all dwellings.** `urban_rag.program` fills an envelope
with three families - housing, commerce, industry - and returns them priced,
so a mixed answer arrives here with a ground floor of shops and five storeys
of flats over it, or with no dwelling in it at all. Two of the terms above are
about dwellings specifically, and pricing a retail podium through either of
them prices a building that does not exist:

* **the lease-up.** ``absorption_units_per_month`` is dwellings a month, so a
  program with no dwellings used to fill in the solve's stated months however
  much floor it held - fifteen thousand square feet of retail leased as fast
  as an empty six-plex. Commerce and industry lease by the square foot and
  much more slowly than that; `commercial_absorption_sqft_per_month` and its
  industrial twin are what they lease at, and the lease-up is the longest of
  the three rather than the residential one alone. The families fill in
  parallel, not in sequence: a leasing agent and a rental office are not
  waiting on each other.
* **the exit.** A stabilised income sells at the cap its *own* market pays,
  and a shop is not an apartment: `commercial_cap_rate_spread_bps` and
  `industrial_cap_rate_spread_bps` are what each trades over the multifamily
  cap the solve sold everything at. The cap a mixed building is valued and
  screened at is those blended by **income** - `blended_cap_rate` - which is
  the whole reason `urban_rag.hbu` carries `hbu_commercial_noi_cad` beside
  the floor areas. Floor is the wrong weight and wrong by a lot: at the
  solver's own rates a square foot of commerce earns about four times a square
  foot of housing, so a single retail storey under five of flats is a sixth of
  the floor and very nearly half the rent, and blending on area would hand it
  a cap it has no business getting.

Both corrections push a commerce-heavy program's stated return **down**, and
that is the point rather than a regrettable side effect: it was previously
being leased at housing's speed and sold at housing's cap, and neither was a
number anyone had chosen. What the corrections do not touch is the income
itself - the solve already prices every square foot of commerce it builds, and
the yield on cost has always had it in the numerator. Whether a ground floor
of shops beats the empty storey or the parking deck that a *Tous sauf le RDC*
column otherwise leaves at grade is now a question this module answers on the
commerce's own terms instead of on the dwellings'.

Everything is annual, year-end, unlevered, with the operating expense ratio
the NOI already carries - insurance in operation is inside it, and the
builder's-risk policy during construction is the one insurance stated apart.
No financing, no taxes on the gain, no rent growth: the same flat-NOI stance
the solve took, so the IRR and the NPV disagree only by what this module
adds and never by a different idea of the rent. One expense ratio covers all
three families, which is the solve's simplification carried forward and the
one that most flatters the dwellings: a triple-net retail lease leaves its
landlord a far lighter expense load than an apartment does. Deliberately free
of Dagster imports, like every arithmetic module here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import pandas as pd

#: Months in a year, restated so this module owes `program` nothing.
MONTHS_PER_YEAR = 12

#: Square metres in a square foot, exactly, restated for the same reason. The
#: floor areas arriving here are metric and the absorption rates commerce is
#: leased at are imperial; this is the one place the two meet.
M2_PER_SQFT = 0.09290304

#: The families a building's income is split into, in reporting order. The
#: same three `urban_rag.comparables.INCOME_CLASSES` names, restated so this
#: module owes that one nothing either.
INCOME_FAMILIES: tuple[str, ...] = ("residential", "commercial", "industrial")

#: Below this, a denominator is not a denominator.
_MIN_DENOMINATOR = 1e-9

#: The rate interval the IRR is searched in, as fractions: a project that
#: loses nine-tenths of its money or triples it a year is outside anything
#: this platform would act on, and a wider bracket only slows the bisection.
_IRR_LOW = -0.9
_IRR_HIGH = 2.0
_IRR_ITERATIONS = 80


@dataclass(frozen=True)
class ProformaAssumptions:
    """The budget lines the solve leaves out, the absorption, and the screens.

    All shares are in percent of the hard cost (or of the sale price, for the
    selling cost), stated rather than surveyed, and every row records them.

    ``soft_cost_pct`` - architecture and engineering, permits and the borough's
    fees, legal, marketing, developer overhead. Montreal wood-frame mid-rise
    runs 15 to 25 percent of hard; 18 is the middle. ``contingency_pct`` is
    the allowance a lender asks for on a costed but unbuilt budget, 5 to 10;
    ``builders_risk_pct`` is the course-of-construction policy, under a
    percent of hard on a wood-frame build. ``selling_cost_pct`` is brokerage
    and legal on the exit, 2 to 3 percent of the price.

    ``absorption_units_per_month`` is how fast a new building leases in this
    market; four a month is a small mid-rise in a borough at near-zero
    vacancy. The lease-up used here is the longer of the solve's own months
    and the dwellings over this rate, so a twenty-unit building takes at
    least five months to fill whatever the solve assumed.

    ``commercial_absorption_sqft_per_month`` and
    ``industrial_absorption_sqft_per_month`` are the same idea for the space a
    mixed or non-residential program holds, in the unit that space is leased
    in. 1 500 sq ft a month is a retail or small-office podium in a borough
    high street - a fifteen-thousand-foot podium takes ten months, against the
    six a residential solve states - and 5 000 is industrial, which leases in
    far larger blocks to far fewer tenants and so fills faster per foot once
    it goes. Both are per *building*, not per tenant, and both are stated
    assumptions: there is no CMHC for retail absorption any more than there is
    for retail rent.

    ``market_cap_rate_pct`` is what stabilised **residential** income sells at
    in the area - ``None`` reads the terminal cap the solve sold at, which is
    the same number by construction. ``commercial_cap_rate_spread_bps`` and
    ``industrial_cap_rate_spread_bps`` are what the other two families trade
    over it, and `blended_cap_rate` is where a mixed building's own cap comes
    from. 175 basis points puts commerce at about 6.25 against multifamily's
    4.5, which is where Montreal retail and suburban-class office have sat;
    100 puts industrial at 5.5, tighter than retail because the sector has
    been bid tighter than retail for a decade. Set either to zero to value
    every family at the residential cap, which is what this module did before
    the spreads existed.

    ``min_yoc_spread_bps`` is the development spread a yield on cost has to
    clear over that blended cap: a hundred basis points is the floor a
    developer prices risk at over buying the same income already built. Held
    against the *blend* rather than the residential cap, so a retail scheme is
    asked for the higher yield its own exit implies rather than let through on
    an apartment's.

    ``hurdle_irr_spread_bps`` is the same idea for the IRR, and it is a spread
    rather than a level for a reason this module's own arithmetic forces.
    **In a flat-NOI model the cap rate is the unlevered return**: buy at a
    4.5 cap, collect a flat 4.5 forever, sell at 4.5, and the IRR is 4.5 (4.44
    after selling costs). So the cap is the *indifference* point - what doing
    nothing but buying the finished building pays - and a hurdle set at it
    prices development risk at zero. The hurdle is therefore the lot's own
    blended cap plus this spread, and the same hundred basis points the yield
    test uses, so the two screens are two views of one bar rather than two
    different bars.

    That makes the hurdle **per lot**, which it has to be now that the exit is:
    a pure-commercial scheme exits at 6.25 and must beat 7.25, an apartment
    block exits at 4.5 and must beat 5.5. A single number cannot say that, and
    a single number set for the apartment block would pass a retail deal
    returning less than buying the same shops already built.

    ``hurdle_irr_pct`` overrides all of that with a flat level on every lot.
    ``None`` - the default - derives it as above. Twelve is the conservative
    institutional hurdle for ground-up multifamily and fifteen what a merchant
    builder asks, and **both are levered, growth-carrying numbers that this
    module cannot reach**: with no rent growth and no financing, a 12 percent
    unlevered IRR here needs a 12 percent yield on cost - 750 bps over the cap,
    a 2.7x value on cost - or 6.3 percent annual NOI growth the module does not
    model. Set it only to reproduce an outside convention, knowing that.
    """

    soft_cost_pct: float = 18.0
    contingency_pct: float = 7.0
    builders_risk_pct: float = 1.0
    selling_cost_pct: float = 2.5
    absorption_units_per_month: float = 4.0
    commercial_absorption_sqft_per_month: float = 1_500.0
    industrial_absorption_sqft_per_month: float = 5_000.0
    market_cap_rate_pct: float | None = None
    commercial_cap_rate_spread_bps: float = 175.0
    industrial_cap_rate_spread_bps: float = 100.0
    min_yoc_spread_bps: float = 100.0
    hurdle_irr_spread_bps: float = 100.0
    hurdle_irr_pct: float | None = None

    def __post_init__(self) -> None:
        for name in (
            "soft_cost_pct",
            "contingency_pct",
            "builders_risk_pct",
            "selling_cost_pct",
            "min_yoc_spread_bps",
            "hurdle_irr_spread_bps",
            # Zero is meaningful on both - it values that family at the
            # residential cap - but a negative one says commerce sells dearer
            # than housing, which is not what a spread over it means.
            "commercial_cap_rate_spread_bps",
            "industrial_cap_rate_spread_bps",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")
        if self.selling_cost_pct >= 100.0:
            raise ValueError("selling_cost_pct is a share of the price")
        for name in (
            "absorption_units_per_month",
            "commercial_absorption_sqft_per_month",
            "industrial_absorption_sqft_per_month",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.market_cap_rate_pct is not None and self.market_cap_rate_pct <= 0:
            raise ValueError("market_cap_rate_pct must be positive or None")

    def hurdle_irr(self, market_cap_pct: pd.Series) -> pd.Series:
        """The IRR each lot has to clear, given the cap its own mix exits at.

        ``hurdle_irr_pct`` where the caller stated a flat one, else that lot's
        blended cap plus `hurdle_irr_spread_bps`. A lot with no cap - nothing
        to sell, so nothing to be indifferent to - gets no hurdle rather than
        a hurdle of the spread alone, and `screens` reads that as not cleared.
        """
        if self.hurdle_irr_pct is not None:
            return pd.Series(float(self.hurdle_irr_pct), index=market_cap_pct.index)
        return market_cap_pct + self.hurdle_irr_spread_bps / 100.0

    def cap_rate_spread_bps(self, family: str) -> float:
        """What ``family`` trades over the residential cap, in basis points.

        Zero for the residential family itself, which is the cap the spreads
        are quoted against rather than a family without one.
        """
        return {
            "residential": 0.0,
            "commercial": self.commercial_cap_rate_spread_bps,
            "industrial": self.industrial_cap_rate_spread_bps,
        }[family]

    @property
    def budget_factor(self) -> float:
        """What a dollar of hard cost becomes once the rest of the budget is on it."""
        return 1.0 + (
            self.soft_cost_pct + self.contingency_pct + self.builders_risk_pct
        ) / 100.0

    def as_metadata(self) -> dict[str, object]:
        return {
            "soft_cost_pct": self.soft_cost_pct,
            "contingency_pct": self.contingency_pct,
            "builders_risk_pct": self.builders_risk_pct,
            "selling_cost_pct": self.selling_cost_pct,
            "absorption_units_per_month": self.absorption_units_per_month,
            "commercial_absorption_sqft_per_month": (
                self.commercial_absorption_sqft_per_month
            ),
            "industrial_absorption_sqft_per_month": (
                self.industrial_absorption_sqft_per_month
            ),
            "market_cap_rate_pct": self.market_cap_rate_pct,
            "commercial_cap_rate_spread_bps": self.commercial_cap_rate_spread_bps,
            "industrial_cap_rate_spread_bps": self.industrial_cap_rate_spread_bps,
            "min_yoc_spread_bps": self.min_yoc_spread_bps,
            "hurdle_irr_spread_bps": self.hurdle_irr_spread_bps,
            "hurdle_irr_pct": self.hurdle_irr_pct,
        }


@dataclass(frozen=True)
class Timing:
    """When the money moves: the solve's own stance, read off its rows."""

    construction_months: int
    lease_up_months: int
    hold_years: int
    terminal_cap_rate_pct: float | None
    discount_rate_pct: float


def lease_up_months(
    dwellings: pd.Series,
    floor_months: int,
    assumptions: ProformaAssumptions,
    *,
    commercial_m2: pd.Series | None = None,
    industrial_m2: pd.Series | None = None,
) -> pd.Series:
    """The longest of the stated lease-up and what absorption says each family
    of space in this building takes to fill.

    The dwellings over `ProformaAssumptions.absorption_units_per_month`, the
    commerce and the industry over their own rates in square feet a month, and
    the solve's own stated floor - the largest of whichever are given.

    **The longest and not the sum**, because the three fill in parallel: the
    rental office and the leasing agent are not waiting on each other, and a
    building whose flats take ten months and whose podium takes twelve is
    stabilised in twelve. Sequencing them would model a developer who leases
    one family at a time, which is not how a mixed building comes online.

    Passing neither area is what every caller did before commerce was priced
    here, and it answers exactly what it answered then - which is why a
    building with no dwellings *and* no areas given still gets the stated
    months rather than nothing.
    """
    index = dwellings.index
    longest = np.full(len(index), float(floor_months))
    longest = np.maximum(
        longest,
        np.ceil(
            dwellings.fillna(0.0).to_numpy(dtype="float64")
            / float(assumptions.absorption_units_per_month)
        ),
    )
    for area, rate in (
        (commercial_m2, assumptions.commercial_absorption_sqft_per_month),
        (industrial_m2, assumptions.industrial_absorption_sqft_per_month),
    ):
        if area is None:
            continue
        sqft = area.fillna(0.0).to_numpy(dtype="float64") / M2_PER_SQFT
        longest = np.maximum(longest, np.ceil(sqft / float(rate)))
    return pd.Series(longest, index=index)


def blended_cap_rate(
    residential_cap_pct: float | None,
    assumptions: ProformaAssumptions,
    *,
    income: Mapping[str, pd.Series],
) -> pd.Series | None:
    """The cap a building of this income mix is valued at, per lot.

    Each family's cap is ``residential_cap_pct`` plus its own spread from
    `ProformaAssumptions.cap_rate_spread_bps`, and the answer is the one cap
    that values the whole building at what its parts are separately worth.
    ``income`` maps family name to that family's share of the NOI, in dollars.
    ``None`` in, ``None`` out: a caller with no residential cap is a caller not
    selling the building, and there is nothing to blend onto.

    **Weighted by income and never by floor.** The whole point of the blend is
    that the families earn at different rates per square foot - four to one
    between commerce and housing at the solver's own rents - so a cap weighted
    by area would put a mixed building's exit much nearer the apartment cap
    than the money it actually collects justifies. `urban_rag.hbu` writes
    `hbu_commercial_noi_cad` and its two neighbours precisely so this weight
    is available; the floor-area columns beside them are for the gap, which is
    a different question.

    **And it is the harmonic mean, not the arithmetic one**, which is the
    difference between a cap that values the building and a cap that merely
    averages two numbers. A building of two income streams is worth
    ``sum(NOI_f / cap_f)`` - each stream sold to the market that buys it - so
    the single cap reproducing that price is::

        cap = sum(NOI_f) / sum(NOI_f / cap_f)

    and ``total NOI / cap`` is then the parts-sum by construction, which is
    the whole property a blended cap is supposed to have. An NOI-weighted
    *arithmetic* mean is always the larger of the two and so always values a
    mixed building under its parts: at the module's own spreads a 50/50
    residential/commercial split comes out 14 bps high and 2.65 % cheap, and
    real Villeray mixed lots run 9 to 13 bps and about 2 %. Small, one-signed,
    and avoidable for one division.

    Negative income is clipped to zero rather than carried. The solve cannot
    produce it - a gross revenue is a sum of non-negative terms and the
    expense ratio is below one - but a negative term would subtract from the
    denominator above and hand back a cap that is not between the family caps
    it was built from, which is worse than a rounding error.

    A row whose families sum to nothing - a lot the solve declined, or one
    from a partition written before the split existed - gets the residential
    cap unblended, which is what this module valued everything at before.
    """
    if residential_cap_pct is None:
        return None
    base = float(residential_cap_pct)
    index = next(iter(income.values())).index
    if base <= _MIN_DENOMINATOR:
        # A cap of nothing values an income at infinity, so there is no blend
        # to take and dividing by it would say so with an `inf` rather than
        # with a number. Passed straight through, `_capitalise` reads it the
        # way it reads a null cap: this row is not sold. `ProformaAssumptions`
        # and `InvestmentAssumptions` both refuse a non-positive cap, so this
        # is reachable only from a hand-built `Timing`.
        return pd.Series(base, index=index)
    # Per-lot value, in units of a dollar of NOI over a cap in percent: the
    # 100 that would turn each cap into a fraction cancels against the 100
    # that turns the answer back into one, so it is never written.
    value = pd.Series(0.0, index=index)
    total = pd.Series(0.0, index=index)
    for family, noi in income.items():
        share = noi.fillna(0.0).astype("float64").clip(lower=0.0)
        value = value + share / (base + assumptions.cap_rate_spread_bps(family) / 100.0)
        total = total + share
    blended = total / value.where(value.abs() > _MIN_DENOMINATOR)
    return blended.fillna(base)


def cash_flows(
    *,
    outlay_at_start: pd.Series,
    budget: pd.Series,
    construction_months: int,
    lease_up: pd.Series,
    income_after: pd.Series,
    income_before: pd.Series | None,
    hold_years: int,
    terminal_cap_rate_pct: float | pd.Series | None,
    selling_cost_pct: float,
    terminal_cap_rate_before_pct: float | pd.Series | None = None,
) -> np.ndarray:
    """One row of annual, year-end cash flows per lot, year 0 first.

    ``outlay_at_start`` goes out at year 0 (acquisition, site costs).
    ``budget`` is spent evenly over ``construction_months``. ``income_after``
    is the stabilised annual NOI once the building is done; it fills linearly
    over ``lease_up`` months after construction and then runs for
    ``hold_years``, at the end of which the building is sold at
    ``terminal_cap_rate_pct`` less ``selling_cost_pct`` - or not sold at all
    where the cap is ``None``. ``income_before`` is what the lot earned
    without the project, for an *incremental* stream: it is taken off every
    period, so the owner's flow is the difference and the sale is the
    difference in value.

    **The cap may be one number or one per lot.** A pandas Series is a cap
    blended to each building's own income mix (`blended_cap_rate`), which is
    what a mixed-use answer needs and what a scalar cannot say. A row whose
    cap is null or zero is not sold, the same as passing ``None`` for every
    row: a cap of nothing values an income at infinity.

    ``terminal_cap_rate_before_pct`` is the cap the *given-up* income is
    valued at when there is one, defaulting to the cap above. The two differ
    exactly when the two buildings are differently made - an owner replacing a
    warehouse with flats gives up industrial income and gains residential, and
    valuing both halves of that difference at one cap prices neither.
    """
    n = len(outlay_at_start)
    lease = lease_up.to_numpy(dtype="float64")
    max_lease = int(np.nanmax(lease)) if n else 0
    total_months = construction_months + max_lease + hold_years * MONTHS_PER_YEAR
    years = int(np.ceil(total_months / MONTHS_PER_YEAR)) + 1
    flows = np.zeros((n, years), dtype="float64")

    flows[:, 0] -= outlay_at_start.fillna(0.0).to_numpy(dtype="float64")

    # The budget, month by month, then rolled into the year it lands in.
    hard = budget.fillna(0.0).to_numpy(dtype="float64")
    if construction_months > 0:
        per_month = hard / construction_months
        for month in range(1, construction_months + 1):
            flows[:, (month - 1) // MONTHS_PER_YEAR + 1] -= per_month
    else:
        flows[:, 0] -= hard

    # The income: nothing during the build, a linear fill over the lease-up,
    # then stabilised until the hold ends - per row, since the fill differs.
    noi = income_after.fillna(0.0).to_numpy(dtype="float64") / MONTHS_PER_YEAR
    before = (
        income_before.fillna(0.0).to_numpy(dtype="float64") / MONTHS_PER_YEAR
        if income_before is not None
        else np.zeros(n)
    )
    end_month = construction_months + lease + hold_years * MONTHS_PER_YEAR
    months = np.arange(1, total_months + 1, dtype="float64")
    # Shares of the stabilised NOI, month by month, one row per lot.
    started = months[None, :] - construction_months
    share = np.clip(started / np.where(lease[:, None] > 0, lease[:, None], 1.0), 0.0, 1.0)
    share = np.where(started <= 0, 0.0, share)
    share = np.where(months[None, :] > end_month[:, None], 0.0, share)
    monthly = share * noi[:, None]
    # What the lot earned anyway is given up every month the project runs.
    monthly -= np.where(months[None, :] <= end_month[:, None], before[:, None], 0.0)
    year_of = ((months - 1) // MONTHS_PER_YEAR + 1).astype(int)
    for year in range(1, years):
        flows[:, year] += monthly[:, year_of == year].sum(axis=1)

    # The sale, in the year the hold ends. Each half of the difference is
    # capitalised at its own cap, which is the same cap unless the caller said
    # otherwise - see the docstring on why an owner's two buildings differ.
    if terminal_cap_rate_pct is not None:
        cap_after = _cap_rates(terminal_cap_rate_pct, n)
        cap_before = _cap_rates(
            terminal_cap_rate_pct
            if terminal_cap_rate_before_pct is None
            else terminal_cap_rate_before_pct,
            n,
        )
        value_after = _capitalise(noi * MONTHS_PER_YEAR, cap_after)
        value_before = _capitalise(before * MONTHS_PER_YEAR, cap_before)
        sale = (value_after - value_before) * (1.0 - selling_cost_pct / 100.0)
        sale_year = np.ceil(end_month / MONTHS_PER_YEAR).astype(int)
        flows[np.arange(n), np.minimum(sale_year, years - 1)] += sale
    return flows


def _cap_rates(cap: float | pd.Series, n: int) -> np.ndarray:
    """A cap per row, in percent, from either a scalar or a per-lot Series."""
    if isinstance(cap, pd.Series):
        return cap.to_numpy(dtype="float64")
    return np.full(n, float(cap))


def _capitalise(annual_income: np.ndarray, cap_pct: np.ndarray) -> np.ndarray:
    """Income over its cap, and zero where the cap is not a cap.

    A null or zero cap is a row the caller has no exit for rather than a row
    worth infinitely much, so it is sold for nothing - the same answer a
    ``None`` cap gives every row.
    """
    live = np.isfinite(cap_pct) & (cap_pct > _MIN_DENOMINATOR)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(live, annual_income / np.where(live, cap_pct / 100.0, 1.0), 0.0)


def irr_pct(flows: np.ndarray) -> np.ndarray:
    """The unlevered IRR of each row, in percent, or NaN where none exists.

    Bisection on the annual NPV between `_IRR_LOW` and `_IRR_HIGH`, one
    vectorised pass for every row at once. A row with no sign change - all
    outflow, or nothing at all - has no rate that zeroes it and comes back
    NaN rather than an endpoint, and so does a row the bracket does not hold.
    """
    n, years = flows.shape
    result = np.full(n, np.nan)
    if n == 0:
        return result
    has_out = (flows < 0).any(axis=1)
    has_in = (flows > 0).any(axis=1)
    live = has_out & has_in
    if not live.any():
        return result
    t = np.arange(years, dtype="float64")
    rows = flows[live]

    def npv(rate: np.ndarray) -> np.ndarray:
        factors = (1.0 + rate)[:, None] ** -t[None, :]
        return (rows * factors).sum(axis=1)

    low = np.full(rows.shape[0], _IRR_LOW)
    high = np.full(rows.shape[0], _IRR_HIGH)
    npv_low = npv(low)
    npv_high = npv(high)
    bracketed = np.sign(npv_low) != np.sign(npv_high)
    for _ in range(_IRR_ITERATIONS):
        mid = (low + high) / 2.0
        npv_mid = npv(mid)
        go_up = np.sign(npv_mid) == np.sign(npv_low)
        low = np.where(go_up, mid, low)
        npv_low = np.where(go_up, npv_mid, npv_low)
        high = np.where(go_up, high, mid)
    found = np.where(bracketed, (low + high) / 2.0 * 100.0, np.nan)
    result[np.flatnonzero(live)] = found
    return result


def equity_multiple(flows: np.ndarray) -> np.ndarray:
    """Every dollar back over every dollar in, or NaN with nothing in."""
    outflow = np.where(flows < 0, -flows, 0.0).sum(axis=1)
    inflow = np.where(flows > 0, flows, 0.0).sum(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(outflow > _MIN_DENOMINATOR, inflow / outflow, np.nan)


def returns(
    frame: pd.DataFrame,
    assumptions: ProformaAssumptions,
    rebuild: Timing,
    enhance: Timing,
) -> pd.DataFrame:
    """Every return this module states, for both perspectives, indexed like
    ``frame``.

    Reads the futures the shortlist already carries - the acquisition, the
    hard costs, the site costs, the incomes, the dwellings, and the income
    split by use family - and writes: the all-in budgets, the
    absorption-driven lease-ups, the buyer's IRR and equity multiple on each
    future, the owner's incremental IRR on the two that build, the yield on
    all-in cost for each, the blended cap rate each future's yield is held
    against, and the screens.

    **A frame without the use split is priced as it always was.** The three
    `hbu_*_noi_cad` columns and the two non-residential areas are read where
    they are there and read as nothing where they are not, and a building of
    nothing but dwellings blends to the residential cap and leases at the
    dwelling rate - so a partition written before the split existed comes back
    with the numbers it came back with then, rather than with nulls.
    """
    index = frame.index
    acquisition = _numeric(frame, "acquisition_cost_cad")
    site = _numeric(frame, "site_costs_cad").fillna(0.0)
    hard_rebuild = _numeric(frame, "hbu_total_capital_cost_cad")
    solved = (
        frame["enhance_solved"].fillna(False).astype("bool")
        if "enhance_solved" in frame.columns
        else pd.Series(False, index=index)
    )
    # A solve that added nothing (`nothing_pencils`, normalised to the
    # standing building) is not an addition to return on: no budget, no
    # build, no lease-up, and a stream identical to holding. It states no
    # enhancement return at all rather than a hold's dressed as one. A frame
    # without the column - a partition before the solve - is left as it was.
    nothing_added = (_numeric(frame, "enhance_added_floor_area_m2") <= 0.0).fillna(False)
    solved = solved & ~nothing_added
    # The addition: the enhancement solve where the gap carries one, else the
    # shortlist's own closed-form estimate (`improvement_program`), so an
    # improvement on a partition without the solve still has a return. The
    # estimate states no dwellings, so its lease-up is the stated floor.
    estimate_cost = _numeric(frame, "improvement_cost_cad")
    estimated = ~solved & (estimate_cost > 0.0).fillna(False)
    enhance_solved = solved | estimated
    hard_enhance = _numeric(frame, "enhance_capital_cost_cad").where(solved).where(
        ~estimated, estimate_cost
    )
    noi_after = _numeric(frame, "hbu_annual_stabilised_noi_cad")
    noi_added = _numeric(frame, "enhance_added_annual_stabilised_noi_cad").where(
        solved
    ).where(~estimated, _numeric(frame, "improvement_noi_cad"))
    noi_before = _numeric(frame, "existing_annual_stabilised_noi_cad").fillna(0.0)
    disruption = _numeric(frame, "enhance_disruption_cad").fillna(0.0)
    dwellings_after = _numeric(frame, "hbu_num_dwellings")
    dwellings_added = _numeric(frame, "enhance_added_dwellings").where(solved)

    # The mix, on each future. The rebuild's is the solve's own NOI split; the
    # addition's is the enhancement solve's, which is the *added* income and
    # not the whole building's - the standing half is `noi_before` and is
    # capped on what already stands. Absent columns read as nothing, which
    # makes an all-residential building of a frame that never carried a split.
    income_rebuild = {
        family: _numeric(frame, f"hbu_{family}_noi_cad") for family in INCOME_FAMILIES
    }
    income_enhance = {
        family: _numeric(frame, f"enhance_added_{family}_noi_cad").where(solved)
        for family in INCOME_FAMILIES
    }
    # An improvement priced off the closed-form estimate has no split of its
    # own; the estimate is an addition of dwellings, so it counts as one.
    income_enhance["residential"] = income_enhance["residential"].where(
        ~estimated, _numeric(frame, "improvement_noi_cad")
    )

    factor = assumptions.budget_factor
    budget_rebuild = hard_rebuild * factor
    budget_enhance = hard_enhance.where(enhance_solved) * factor
    # The lease-up now hears from the commerce as well as from the dwellings.
    # The rebuild's areas carry their cellars (`use_gap` puts them there,
    # because the solve digs for shops); the enhancement solves with no
    # basement at all, so its two are the whole of its non-residential floor.
    lease_rebuild = lease_up_months(
        dwellings_after,
        rebuild.lease_up_months,
        assumptions,
        commercial_m2=_numeric(frame, "hbu_commercial_floor_area_with_cellar_m2"),
        industrial_m2=_numeric(frame, "hbu_industrial_floor_area_with_cellar_m2"),
    )
    lease_enhance = lease_up_months(
        dwellings_added,
        enhance.lease_up_months,
        assumptions,
        commercial_m2=_numeric(frame, "enhance_added_commercial_area_m2").where(solved),
        industrial_m2=_numeric(frame, "enhance_added_industrial_area_m2").where(solved),
    )

    # The caps. Two bases and one blend each, because the two do different
    # work: `exit_*` is what the building is sold at inside the IRR - the
    # solve's own terminal cap, so the IRR and the NPV keep selling the same
    # building - and `market_*` is what the yield on cost is *screened*
    # against, which a caller may override with an area cap of their own.
    # Each is blended to the income mix of the thing it values.
    standing_income = _standing_income(frame, noi_before)
    exit_rebuild = blended_cap_rate(
        rebuild.terminal_cap_rate_pct, assumptions, income=income_rebuild
    )
    exit_enhance = blended_cap_rate(
        enhance.terminal_cap_rate_pct, assumptions, income=income_enhance
    )
    exit_standing = blended_cap_rate(
        rebuild.terminal_cap_rate_pct, assumptions, income=standing_income
    )
    exit_standing_enhance = blended_cap_rate(
        enhance.terminal_cap_rate_pct, assumptions, income=standing_income
    )
    base_cap = assumptions.market_cap_rate_pct or rebuild.terminal_cap_rate_pct
    market_rebuild = blended_cap_rate(base_cap, assumptions, income=income_rebuild)
    # The enhanced building is the standing income plus the addition's, so its
    # market cap is blended over both rather than over the addition alone.
    market_enhance = blended_cap_rate(
        base_cap,
        assumptions,
        income={
            family: standing_income[family].fillna(0.0)
            + income_enhance[family].fillna(0.0)
            for family in INCOME_FAMILIES
        },
    )

    # -- the buyer: the whole building, after paying for the lot -----------
    buyer_rebuild = cash_flows(
        outlay_at_start=acquisition + site,
        budget=budget_rebuild,
        construction_months=rebuild.construction_months,
        lease_up=lease_rebuild,
        income_after=noi_after,
        income_before=None,
        hold_years=rebuild.hold_years,
        terminal_cap_rate_pct=exit_rebuild,
        selling_cost_pct=assumptions.selling_cost_pct,
    )
    # The enhancement keeps the standing income through the works less the
    # disruption, then adds the new floor's. Modelled as the standing NOI
    # from day one (a hold) plus the addition's incremental stream, so the
    # disruption is the one place the two differ.
    buyer_enhance = cash_flows(
        outlay_at_start=acquisition,
        budget=budget_enhance,
        construction_months=enhance.construction_months,
        lease_up=lease_enhance,
        income_after=noi_added,
        income_before=None,
        hold_years=enhance.hold_years,
        terminal_cap_rate_pct=exit_enhance,
        selling_cost_pct=assumptions.selling_cost_pct,
    )
    buyer_enhance = _add_standing(
        buyer_enhance,
        noi_before,
        disruption,
        enhance,
        assumptions,
        cap_rate_pct=exit_standing_enhance,
    )
    buyer_hold = cash_flows(
        outlay_at_start=acquisition,
        budget=pd.Series(0.0, index=index),
        construction_months=0,
        lease_up=pd.Series(0.0, index=index),
        income_after=noi_before,
        income_before=None,
        hold_years=rebuild.hold_years,
        terminal_cap_rate_pct=exit_standing,
        selling_cost_pct=assumptions.selling_cost_pct,
    )

    # -- the owner: the increment over the building they have --------------
    owner_rebuild = cash_flows(
        outlay_at_start=site,
        budget=budget_rebuild,
        construction_months=rebuild.construction_months,
        lease_up=lease_rebuild,
        income_after=noi_after,
        income_before=noi_before,
        hold_years=rebuild.hold_years,
        # The two halves of the difference are two different buildings - a
        # warehouse given up for flats is the case this exists for - so each
        # is capped on its own mix rather than both on the proposal's.
        terminal_cap_rate_pct=exit_rebuild,
        terminal_cap_rate_before_pct=exit_standing,
        selling_cost_pct=assumptions.selling_cost_pct,
    )
    owner_enhance = cash_flows(
        outlay_at_start=pd.Series(0.0, index=index),
        budget=budget_enhance,
        construction_months=enhance.construction_months,
        lease_up=lease_enhance,
        income_after=noi_added,
        income_before=None,
        hold_years=enhance.hold_years,
        terminal_cap_rate_pct=exit_enhance,
        selling_cost_pct=assumptions.selling_cost_pct,
    )
    owner_enhance = _subtract_disruption(owner_enhance, disruption, enhance)

    priced = acquisition.notna().to_numpy()
    solved = enhance_solved.to_numpy()

    def only(values: np.ndarray, mask: np.ndarray) -> pd.Series:
        return pd.Series(np.where(mask, values, np.nan), index=index)

    tdc_rebuild = acquisition + site + budget_rebuild
    tdc_enhance = acquisition + budget_enhance
    yoc_rebuild = _yield(noi_after, tdc_rebuild)
    yoc_enhance = _yield(noi_before + noi_added, tdc_enhance)
    yoc_hold = _yield(noi_before, acquisition)
    owner_yoc_rebuild = _yield(noi_after - noi_before, site + budget_rebuild)
    owner_yoc_enhance = _yield(noi_added, budget_enhance)

    # Each yield against the cap of the building that earns it. A retail
    # scheme is now asked to clear its own exit rather than an apartment's,
    # which is the whole difference the blend makes to the screen.
    nan = pd.Series(np.nan, index=index)
    market_rebuild = nan if market_rebuild is None else market_rebuild
    market_enhance = nan if market_enhance is None else market_enhance
    spread_rebuild = (yoc_rebuild - market_rebuild) * 100.0
    spread_enhance = (yoc_enhance - market_enhance) * 100.0

    return pd.DataFrame(
        {
            "rebuild_budget_cad": budget_rebuild.round(2),
            "rebuild_soft_cost_cad": (hard_rebuild * assumptions.soft_cost_pct / 100.0).round(2),
            "rebuild_contingency_cad": (hard_rebuild * assumptions.contingency_pct / 100.0).round(2),
            "rebuild_builders_risk_cad": (hard_rebuild * assumptions.builders_risk_pct / 100.0).round(2),
            "rebuild_lease_up_months": lease_rebuild.astype("Int64"),
            "rebuild_total_development_cost_cad": tdc_rebuild.round(2),
            "enhance_budget_cad": budget_enhance.round(2),
            "enhance_lease_up_months": lease_enhance.where(enhance_solved).astype("Int64"),
            "enhance_total_development_cost_cad": tdc_enhance.round(2),
            # The cap each yield is screened against, per lot, because it is
            # blended to that lot's own mix. On an all-residential building it
            # is the one number it always was.
            "market_cap_rate_pct": market_rebuild.round(4),
            "market_cap_rate_enhance_pct": market_enhance.where(
                enhance_solved
            ).round(4),
            # The cap each future is *sold* at inside the IRR, which is the
            # solve's terminal cap blended the same way. Reported so a reader
            # can see why two lots with the same yield have different IRRs.
            "exit_cap_rate_rebuild_pct": (
                nan if exit_rebuild is None else exit_rebuild.round(4)
            ),
            "exit_cap_rate_enhance_pct": (
                nan
                if exit_enhance is None
                else exit_enhance.where(enhance_solved).round(4)
            ),
            # What share of each future's stabilised income is not housing -
            # the weight behind both caps above, and the column that says at a
            # glance whether a lot is a mixed-use answer at all.
            "hbu_non_residential_income_share": _non_residential_share(
                income_rebuild
            ).round(4),
            "enhance_non_residential_income_share": _non_residential_share(
                income_enhance
            ).where(enhance_solved).round(4),
            "buyer_yoc_hold_pct": yoc_hold.round(4),
            "buyer_yoc_enhance_pct": yoc_enhance.where(enhance_solved).round(4),
            "buyer_yoc_rebuild_pct": yoc_rebuild.round(4),
            "buyer_irr_hold_pct": only(irr_pct(buyer_hold), priced).round(4),
            "buyer_irr_enhance_pct": only(irr_pct(buyer_enhance), priced & solved).round(4),
            "buyer_irr_rebuild_pct": only(irr_pct(buyer_rebuild), priced).round(4),
            "buyer_multiple_rebuild": only(equity_multiple(buyer_rebuild), priced).round(4),
            "buyer_multiple_enhance": only(equity_multiple(buyer_enhance), priced & solved).round(4),
            "owner_yoc_rebuild_pct": owner_yoc_rebuild.round(4),
            "owner_yoc_enhance_pct": owner_yoc_enhance.where(enhance_solved).round(4),
            "owner_irr_rebuild_pct": pd.Series(irr_pct(owner_rebuild), index=index).round(4),
            "owner_irr_enhance_pct": only(irr_pct(owner_enhance), solved).round(4),
            "yoc_spread_rebuild_bps": spread_rebuild.round(1),
            "yoc_spread_enhance_bps": spread_enhance.where(enhance_solved).round(1),
        },
        index=index,
    )


def screens(
    frame: pd.DataFrame, futures_returns: pd.DataFrame, assumptions: ProformaAssumptions
) -> pd.DataFrame:
    """The good-candidate test, per lot, on the thesis's own future.

    An improvement is judged on the enhancement's numbers and every other
    thesis on the rebuild's: the buyer's IRR against the hurdle, the yield on
    all-in cost against the area's cap rate plus the spread, and the owner's
    verdict above zero. A deal that clears *either* bar and pays is a good
    candidate; `clears_cap_rate` and `clears_hurdle` beside it say which.
    The two are not one test twice over: the yield is a static ratio and the
    IRR carries the timeline, so a lot with a short lease-up can clear the
    hurdle on a spread the yield test refuses, and that is a deal a reader
    should see rather than lose to the screen it missed. The verdict is not
    optional either way - a return that clears the hurdle on a price nobody
    would sell at is not one.

    **Both thresholds are the lot's own**, built off the cap its income mix
    exits at (`blended_cap_rate`): the yield has to clear that cap by
    `min_yoc_spread_bps` and the IRR has to clear it by
    `hurdle_irr_spread_bps`. So a pure-commercial scheme exiting at 6.25 is
    asked for a 7.25 IRR and an apartment block exiting at 4.5 for 5.5, which
    is the same question asked of two different buildings rather than one
    number that suits neither. `ProformaAssumptions.hurdle_irr` is where the
    level comes from, and `site_hurdle_irr_pct` reports it per row - a screen
    read a month later has only the row, and a pass/fail whose bar is not on
    it cannot be checked.

    The yield's spread column arrives already computed against the same cap,
    which is why only the IRR side is worked out here.
    """
    improving = frame["site_thesis"] == "improvement" if "site_thesis" in frame else pd.Series(False, index=frame.index)
    yoc = _numeric(futures_returns, "buyer_yoc_rebuild_pct").mask(
        improving, _numeric(futures_returns, "buyer_yoc_enhance_pct")
    )
    irr = _numeric(futures_returns, "buyer_irr_rebuild_pct").mask(
        improving, _numeric(futures_returns, "buyer_irr_enhance_pct")
    )
    owner_irr = _numeric(futures_returns, "owner_irr_rebuild_pct").mask(
        improving, _numeric(futures_returns, "owner_irr_enhance_pct")
    )
    spread = _numeric(futures_returns, "yoc_spread_rebuild_bps").mask(
        improving, _numeric(futures_returns, "yoc_spread_enhance_bps")
    )
    # The cap this lot's own future exits at - the rebuild's on every thesis
    # but an improvement, matching the yield and the IRR above it.
    market_cap = _numeric(futures_returns, "market_cap_rate_pct").mask(
        improving, _numeric(futures_returns, "market_cap_rate_enhance_pct")
    )
    hurdle = assumptions.hurdle_irr(market_cap)
    verdict = _numeric(frame, "site_verdict_cad")
    clears_cap = (spread >= float(assumptions.min_yoc_spread_bps)).fillna(False)
    # A lot with no hurdle has no cap, so there is nothing it could be worth
    # more than: not cleared, the same answer a missing IRR gets.
    clears_hurdle = (irr >= hurdle).fillna(False)
    pays = (verdict > 0.0).fillna(False)
    return pd.DataFrame(
        {
            "site_irr_pct": irr.round(4),
            "owner_site_irr_pct": owner_irr.round(4),
            "site_all_in_yield_on_cost_pct": yoc.round(4),
            "site_yoc_spread_bps": spread.round(1),
            # The bar the IRR beside it was held against, per lot.
            "site_hurdle_irr_pct": hurdle.round(4),
            "clears_cap_rate": clears_cap.astype("bool"),
            "clears_hurdle": clears_hurdle.astype("bool"),
            "is_good_candidate": ((clears_cap | clears_hurdle) & pays).astype("bool"),
        },
        index=frame.index,
    )


def _add_standing(
    flows: np.ndarray,
    noi_before: pd.Series,
    disruption: pd.Series,
    timing: Timing,
    assumptions: ProformaAssumptions,
    *,
    cap_rate_pct: float | pd.Series | None = None,
) -> np.ndarray:
    """A buyer who enhances also collects the standing income from day one,
    less the disruption during the works, and sells the whole building.

    ``cap_rate_pct`` is what the *standing* half sells at - the building
    already there, capped on the use already in it - and defaults to the
    timing's own. The addition beside it is capped on its own mix in
    `cash_flows`, so the whole building's exit is the two halves priced
    separately, which is what a blended cap is when you can name the halves.
    """
    n, years = flows.shape
    before = noi_before.fillna(0.0).to_numpy(dtype="float64")
    out = flows.copy()
    end_year = min(
        int(np.ceil((timing.construction_months + timing.hold_years * MONTHS_PER_YEAR) / MONTHS_PER_YEAR)),
        years - 1,
    )
    for year in range(1, years):
        out[:, year] += np.where(year <= end_year, before, 0.0)
    out = _subtract_disruption(out, disruption, timing)
    cap = timing.terminal_cap_rate_pct if cap_rate_pct is None else cap_rate_pct
    if cap is not None:
        sale = _capitalise(before, _cap_rates(cap, n)) * (
            1.0 - assumptions.selling_cost_pct / 100.0
        )
        out[:, end_year] += sale
    return out


def _subtract_disruption(
    flows: np.ndarray, disruption: pd.Series, timing: Timing
) -> np.ndarray:
    """The standing income lost during the addition's works, taken off the
    years the construction spans."""
    out = flows.copy()
    total = disruption.fillna(0.0).to_numpy(dtype="float64")
    months = max(timing.construction_months, 1)
    per_month = total / months
    for month in range(1, months + 1):
        year = min((month - 1) // MONTHS_PER_YEAR + 1, out.shape[1] - 1)
        out[:, year] -= per_month
    return out


def _non_residential_share(income: Mapping[str, pd.Series]) -> pd.Series:
    """The share of an income mix that is commerce and industry, 0 to 1.

    NaN where the mix earns nothing, which is a lot with no program rather
    than a lot with an all-residential one - the two would otherwise both
    read zero and only one of them means "no commerce here".
    """
    total = sum(noi.fillna(0.0) for noi in income.values())
    non_residential = sum(
        income[family].fillna(0.0) for family in INCOME_FAMILIES if family != "residential"
    )
    return non_residential / total.where(total.abs() > _MIN_DENOMINATOR)


def _standing_income(frame: pd.DataFrame, noi_before: pd.Series) -> dict[str, pd.Series]:
    """The building already on the lot, as an income mix to blend a cap over.

    The roll does not split a property's income the way the solver splits a
    program's, but it does say which class the property's *dominant* unit is
    in - `existing_dominant_income_class` - and that is what a standing
    building is capped on in practice: nobody prices a triplex with a
    dépanneur under it at anything but the residential cap. So the whole of
    `noi_before` is attributed to that class, and to residential wherever the
    roll named none, which is the class the module valued everything at
    before.
    """
    dominant = (
        frame["existing_dominant_income_class"]
        if "existing_dominant_income_class" in frame.columns
        else pd.Series(None, index=frame.index, dtype="object")
    )
    dominant = dominant.where(dominant.isin(INCOME_FAMILIES), "residential")
    total = noi_before.fillna(0.0)
    return {
        family: total.where(dominant == family, 0.0) for family in INCOME_FAMILIES
    }


def _yield(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return 100.0 * numerator / denominator.where(denominator.abs() > _MIN_DENOMINATOR)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce").astype("float64")
