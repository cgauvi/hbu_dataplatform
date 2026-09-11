"""The returns behind a future: the budget, the timeline, the IRR, the screens."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from urban_rag.proforma import (
    M2_PER_SQFT,
    ProformaAssumptions,
    Timing,
    blended_cap_rate,
    cash_flows,
    equity_multiple,
    irr_pct,
    lease_up_months,
    returns,
    screens,
)

REBUILD = Timing(
    construction_months=18, lease_up_months=6, hold_years=25,
    terminal_cap_rate_pct=4.5, discount_rate_pct=5.0,
)
ENHANCE = Timing(
    construction_months=9, lease_up_months=3, hold_years=25,
    terminal_cap_rate_pct=4.5, discount_rate_pct=5.0,
)


def _npv(flows: np.ndarray, rate_pct: float) -> np.ndarray:
    t = np.arange(flows.shape[1])
    return (flows / (1 + rate_pct / 100.0) ** t).sum(axis=1)


# -- the irr ----------------------------------------------------------------


def test_the_irr_zeroes_the_npv():
    flows = np.array([[-1000.0, 100.0, 100.0, 100.0, 1100.0]])
    rate = irr_pct(flows)[0]
    assert rate == pytest.approx(10.0, abs=1e-6)
    assert _npv(flows, rate)[0] == pytest.approx(0.0, abs=1e-6)


def test_rows_with_no_sign_change_have_no_irr():
    flows = np.array([[-1000.0, -10.0, 0.0], [0.0, 0.0, 0.0], [-100.0, 50.0, 80.0]])
    rates = irr_pct(flows)
    assert np.isnan(rates[0]) and np.isnan(rates[1])
    assert rates[2] > 0


def test_a_losing_project_has_a_negative_irr():
    flows = np.array([[-1000.0, 200.0, 200.0, 200.0]])
    assert irr_pct(flows)[0] < 0


def test_the_multiple_is_every_dollar_back_over_every_dollar_in():
    flows = np.array([[-1000.0, -500.0, 600.0, 2400.0]])
    assert equity_multiple(flows)[0] == pytest.approx(2.0)


# -- the timeline -------------------------------------------------------------


def test_absorption_lengthens_the_lease_up_and_never_shortens_it():
    assumptions = ProformaAssumptions(absorption_units_per_month=4.0)
    months = lease_up_months(pd.Series([2.0, 20.0, 60.0, np.nan]), 6, assumptions)
    assert months.tolist() == [6.0, 6.0, 15.0, 6.0]


def test_commerce_leases_by_the_square_foot_and_lengthens_the_same_lease_up():
    """A retail podium used to fill in the stated months however big it was."""
    assumptions = ProformaAssumptions(
        absorption_units_per_month=4.0, commercial_absorption_sqft_per_month=1_000.0
    )
    # 1 000 m2 is 10 763 sq ft, which is eleven months at a thousand a month.
    months = lease_up_months(
        pd.Series([0.0, 0.0]),
        6,
        assumptions,
        commercial_m2=pd.Series([0.0, 1_000.0]),
    )
    assert months.tolist() == [6.0, 11.0]


def test_the_families_lease_in_parallel_so_the_longest_one_wins():
    assumptions = ProformaAssumptions(
        absorption_units_per_month=4.0,
        commercial_absorption_sqft_per_month=1_000.0,
        industrial_absorption_sqft_per_month=1_000.0,
    )
    sqft = pd.Series([5_000.0 * M2_PER_SQFT])
    months = lease_up_months(
        pd.Series([40.0]),  # ten months of dwellings
        6,
        assumptions,
        commercial_m2=sqft,  # five
        industrial_m2=sqft,  # five
    )
    # The longest, not the sum: a rental office and a leasing agent are not
    # waiting on each other.
    assert months.tolist() == [10.0]


# -- the cap rate a mixed building sells at ---------------------------------


def test_a_pure_building_gets_its_own_familys_cap():
    assumptions = ProformaAssumptions(
        commercial_cap_rate_spread_bps=200.0, industrial_cap_rate_spread_bps=100.0
    )
    blended = blended_cap_rate(
        4.5,
        assumptions,
        income={
            "residential": pd.Series([100.0, 0.0, 0.0]),
            "commercial": pd.Series([0.0, 100.0, 0.0]),
            "industrial": pd.Series([0.0, 0.0, 100.0]),
        },
    )
    assert blended.tolist() == pytest.approx([4.5, 6.5, 5.5])


def test_the_blend_values_the_building_at_what_its_parts_are_worth():
    """The property that picks the harmonic mean out of every other mean:
    `total NOI / cap` is the two streams capitalised separately and added."""
    assumptions = ProformaAssumptions(
        commercial_cap_rate_spread_bps=200.0, industrial_cap_rate_spread_bps=100.0
    )
    res, com, ind = 50.0, 50.0, 25.0
    blended = blended_cap_rate(
        4.5,
        assumptions,
        income={
            "residential": pd.Series([res]),
            "commercial": pd.Series([com]),
            "industrial": pd.Series([ind]),
        },
    )
    parts = res / 0.045 + com / 0.065 + ind / 0.055
    assert (res + com + ind) / (blended.iloc[0] / 100.0) == pytest.approx(parts)


def test_the_blend_is_income_weighted_and_never_floor_weighted():
    """Half the income from commerce moves the cap half the spread's worth -
    which floor area, at four to one on rent per square foot, would not."""
    assumptions = ProformaAssumptions(commercial_cap_rate_spread_bps=200.0)
    income = {
        "residential": pd.Series([90.0, 50.0, 10.0]),
        "commercial": pd.Series([10.0, 50.0, 90.0]),
        "industrial": pd.Series([0.0, 0.0, 0.0]),
    }
    blended = blended_cap_rate(4.5, assumptions, income=income)
    # Monotone in the commercial share, and strictly inside the two caps.
    assert blended.is_monotonic_increasing
    assert (blended > 4.5).all() and (blended < 6.5).all()


def test_the_blend_is_below_the_arithmetic_mean_on_any_real_mix():
    """An NOI-weighted arithmetic mean is the larger of the two and so values
    a mixed building under its parts - 14 bps and 2.65 pct on a 50/50 split."""
    assumptions = ProformaAssumptions(commercial_cap_rate_spread_bps=175.0)
    income = {
        "residential": pd.Series([50_000.0]),
        "commercial": pd.Series([50_000.0]),
        "industrial": pd.Series([0.0]),
    }
    blended = blended_cap_rate(4.5, assumptions, income=income).iloc[0]
    arithmetic = 0.5 * 4.5 + 0.5 * 6.25
    assert blended < arithmetic
    assert (arithmetic - blended) * 100 == pytest.approx(14.2, abs=0.1)


def test_income_that_cannot_be_negative_is_not_carried_as_negative():
    """A negative term would subtract from the value the blend divides by and
    hand back a cap outside the family caps it was built from."""
    blended = blended_cap_rate(
        4.5,
        ProformaAssumptions(),
        income={
            "residential": pd.Series([100.0]),
            "commercial": pd.Series([-100.0]),
            "industrial": pd.Series([0.0]),
        },
    )
    assert blended.iloc[0] == pytest.approx(4.5)


def test_a_building_that_earns_nothing_keeps_the_residential_cap():
    """A lot the solve declined, and a partition written before the split."""
    blended = blended_cap_rate(
        4.5,
        ProformaAssumptions(),
        income={name: pd.Series([0.0, np.nan]) for name in
                ("residential", "commercial", "industrial")},
    )
    assert blended.tolist() == [4.5, 4.5]


def test_no_residential_cap_is_no_blend_at_all():
    assert blended_cap_rate(None, ProformaAssumptions(), income={
        "residential": pd.Series([1.0])
    }) is None


def test_a_row_with_no_cap_is_not_sold():
    """A per-lot cap of nothing sells for nothing rather than for infinity."""
    flows = cash_flows(
        outlay_at_start=pd.Series([0.0, 0.0]),
        budget=pd.Series([0.0, 0.0]),
        construction_months=0,
        lease_up=pd.Series([0.0, 0.0]),
        income_after=pd.Series([100_000.0, 100_000.0]),
        income_before=None,
        hold_years=2,
        terminal_cap_rate_pct=pd.Series([5.0, np.nan]),
        selling_cost_pct=0.0,
    )
    assert flows[0].sum() == pytest.approx(100_000.0 * 2 + 2_000_000.0)
    assert flows[1].sum() == pytest.approx(100_000.0 * 2)


def test_the_budget_is_spent_over_the_build_and_the_income_starts_after():
    flows = cash_flows(
        outlay_at_start=pd.Series([500_000.0]),
        budget=pd.Series([1_200_000.0]),
        construction_months=18,
        lease_up=pd.Series([6.0]),
        income_after=pd.Series([120_000.0]),
        income_before=None,
        hold_years=25,
        terminal_cap_rate_pct=4.5,
        selling_cost_pct=2.5,
    )
    row = flows[0]
    assert row[0] == -500_000.0
    # Two-thirds of the budget in year one, a third in year two.
    assert row[1] == pytest.approx(-800_000.0)
    # Year two: the last six months of the build, then the six-month fill:
    # 1/6 .. 6/6 of a monthly 10k, less the last third of the budget.
    assert row[2] == pytest.approx(-400_000.0 + 10_000.0 * (1 + 2 + 3 + 4 + 5 + 6) / 6)
    assert row[3] == pytest.approx(120_000.0)
    # The hold ends 24 + 300 months in, year 27, with the sale on top.
    sale = 120_000.0 / 0.045 * 0.975
    assert row[27] == pytest.approx(120_000.0 + sale)
    assert row[28] == 0.0 if len(row) > 28 else True


def test_an_incremental_stream_gives_up_the_standing_income():
    flows = cash_flows(
        outlay_at_start=pd.Series([0.0]),
        budget=pd.Series([600_000.0]),
        construction_months=12,
        lease_up=pd.Series([0.0]),
        income_after=pd.Series([100_000.0]),
        income_before=pd.Series([40_000.0]),
        hold_years=2,
        terminal_cap_rate_pct=5.0,
        selling_cost_pct=0.0,
    )
    row = flows[0]
    assert row[1] == pytest.approx(-600_000.0 - 40_000.0)
    assert row[2] == pytest.approx(60_000.0)
    # Year three: the increment and the difference in value at the sale.
    assert row[3] == pytest.approx(60_000.0 + (100_000.0 - 40_000.0) / 0.05)


def test_no_sale_where_there_is_no_cap():
    flows = cash_flows(
        outlay_at_start=pd.Series([0.0]),
        budget=pd.Series([100.0]),
        construction_months=0,
        lease_up=pd.Series([0.0]),
        income_after=pd.Series([10.0]),
        income_before=None,
        hold_years=2,
        terminal_cap_rate_pct=None,
        selling_cost_pct=2.5,
    )
    assert flows[0][:3] == pytest.approx([-100.0, 10.0, 10.0])
    assert flows[0][3:].sum() == 0.0


# -- the returns on a shortlist row -----------------------------------------------


def _row(**overrides) -> pd.DataFrame:
    row = {
        "site_thesis": "teardown",
        "acquisition_cost_cad": 500_000.0,
        "site_costs_cad": 36_000.0,
        "hbu_total_capital_cost_cad": 1_500_000.0,
        "hbu_annual_stabilised_noi_cad": 150_000.0,
        "hbu_num_dwellings": 10,
        "existing_annual_stabilised_noi_cad": 20_000.0,
        "enhance_solved": True,
        "enhance_capital_cost_cad": 450_000.0,
        "enhance_added_annual_stabilised_noi_cad": 36_000.0,
        "enhance_added_dwellings": 3,
        "enhance_disruption_cad": 3_750.0,
        "site_verdict_cad": 400_000.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_the_budget_carries_the_soft_lines():
    assumptions = ProformaAssumptions(soft_cost_pct=18.0, contingency_pct=7.0, builders_risk_pct=1.0)
    out = returns(_row(), assumptions, REBUILD, ENHANCE)
    assert assumptions.budget_factor == pytest.approx(1.26)
    assert out.loc[0, "rebuild_budget_cad"] == pytest.approx(1_500_000.0 * 1.26)
    assert out.loc[0, "rebuild_soft_cost_cad"] == pytest.approx(270_000.0)
    assert out.loc[0, "rebuild_total_development_cost_cad"] == pytest.approx(
        500_000.0 + 36_000.0 + 1_890_000.0
    )
    assert out.loc[0, "buyer_yoc_rebuild_pct"] == pytest.approx(
        100 * 150_000.0 / 2_426_000.0, abs=1e-3
    )
    assert out.loc[0, "market_cap_rate_pct"] == 4.5
    assert out.loc[0, "yoc_spread_rebuild_bps"] == pytest.approx(
        (100 * 150_000.0 / 2_426_000.0 - 4.5) * 100, abs=0.2
    )


def test_the_buyers_irr_is_the_rate_that_zeroes_the_whole_stream():
    out = returns(_row(), ProformaAssumptions(), REBUILD, ENHANCE)
    rate = out.loc[0, "buyer_irr_rebuild_pct"]
    assert 0 < rate < 20
    # Rebuilt by hand: the same stream, the same rate, zero.
    lease = lease_up_months(pd.Series([10.0]), 6, ProformaAssumptions())
    flows = cash_flows(
        outlay_at_start=pd.Series([536_000.0]),
        budget=pd.Series([1_500_000.0 * ProformaAssumptions().budget_factor]),
        construction_months=18,
        lease_up=lease,
        income_after=pd.Series([150_000.0]),
        income_before=None,
        hold_years=25,
        terminal_cap_rate_pct=4.5,
        selling_cost_pct=2.5,
    )
    # The rate is stored to four decimals of a percent, which on a $2.4M
    # stream is tens of dollars of NPV.
    assert abs(_npv(flows, rate)[0]) < 100.0


def test_the_owners_irr_is_on_the_increment():
    out = returns(_row(), ProformaAssumptions(), REBUILD, ENHANCE)
    owner = out.loc[0, "owner_irr_rebuild_pct"]
    buyer = out.loc[0, "buyer_irr_rebuild_pct"]
    # No land to buy but a building's income to give up: a different number.
    assert not np.isnan(owner) and owner != buyer
    assert out.loc[0, "owner_yoc_rebuild_pct"] == pytest.approx(
        100 * 130_000.0 / (36_000.0 + 1_890_000.0), abs=1e-3
    )


def test_the_enhancement_returns_need_a_solve_and_the_buyers_a_price():
    out = returns(
        _row(enhance_solved=False, acquisition_cost_cad=None),
        ProformaAssumptions(), REBUILD, ENHANCE,
    )
    for column in ("buyer_irr_rebuild_pct", "buyer_irr_enhance_pct", "buyer_irr_hold_pct",
                   "owner_irr_enhance_pct", "enhance_budget_cad", "buyer_yoc_enhance_pct"):
        assert pd.isna(out.loc[0, column]), column
    # The owner's rebuild needs neither.
    assert not pd.isna(out.loc[0, "owner_irr_rebuild_pct"])


def test_an_enhancement_that_adds_nothing_has_no_returns_of_its_own():
    """A solve normalised to the standing building is a hold, not an
    addition: no budget, no lease-up, no IRR of its own - and none of the
    hold's dressed up as one."""
    nothing = returns(
        _row(
            enhance_added_floor_area_m2=0.0, enhance_added_dwellings=0,
            enhance_capital_cost_cad=0.0, enhance_added_annual_stabilised_noi_cad=0.0,
            enhance_disruption_cad=0.0,
        ),
        ProformaAssumptions(), REBUILD, ENHANCE,
    )
    for column in ("buyer_irr_enhance_pct", "owner_irr_enhance_pct", "enhance_budget_cad",
                   "enhance_lease_up_months", "buyer_yoc_enhance_pct",
                   "buyer_multiple_enhance", "yoc_spread_enhance_bps"):
        assert pd.isna(nothing.loc[0, column]), column
    # The same row with floor on it is an addition and is priced.
    something = returns(_row(enhance_added_floor_area_m2=200.0), ProformaAssumptions(), REBUILD, ENHANCE)
    assert not pd.isna(something.loc[0, "buyer_irr_enhance_pct"])
    assert something.loc[0, "enhance_lease_up_months"] >= ENHANCE.lease_up_months


def test_buying_and_keeping_returns_about_the_cap_rate():
    """Flat NOI bought at its income value and sold at the same cap: the IRR
    is the cap rate less the selling cost's drag, near enough."""
    out = returns(
        _row(acquisition_cost_cad=20_000.0 / 0.045),
        ProformaAssumptions(selling_cost_pct=0.0), REBUILD, ENHANCE,
    )
    assert out.loc[0, "buyer_irr_hold_pct"] == pytest.approx(4.5, abs=0.05)
    assert out.loc[0, "buyer_yoc_hold_pct"] == pytest.approx(4.5, abs=1e-3)


def test_a_market_cap_rate_overrides_the_terminal_one():
    out = returns(_row(), ProformaAssumptions(market_cap_rate_pct=5.5), REBUILD, ENHANCE)
    assert out.loc[0, "market_cap_rate_pct"] == 5.5


# -- the screens ------------------------------------------------------------------


def test_a_good_candidate_clears_the_cap_the_hurdle_and_pays():
    frame = _row()
    out = returns(frame, ProformaAssumptions(hurdle_irr_pct=1.0, min_yoc_spread_bps=50.0), REBUILD, ENHANCE)
    verdict = screens(frame, out, ProformaAssumptions(hurdle_irr_pct=1.0, min_yoc_spread_bps=50.0))
    assert verdict.loc[0, "site_irr_pct"] == out.loc[0, "buyer_irr_rebuild_pct"]
    assert verdict.loc[0, "clears_cap_rate"]
    assert verdict.loc[0, "clears_hurdle"]
    assert verdict.loc[0, "is_good_candidate"]

    strict = screens(frame, out, ProformaAssumptions(hurdle_irr_pct=40.0, min_yoc_spread_bps=400.0))
    assert not strict.loc[0, "clears_hurdle"]
    assert not strict.loc[0, "clears_cap_rate"]
    assert not strict.loc[0, "is_good_candidate"]


def test_a_deal_that_clears_either_bar_is_a_good_candidate():
    """The yield is a static ratio and the IRR carries the lease-up, so a lot
    can clear one bar and miss the other - VSMPE's 2 784 705 clears its
    hurdle on an 87 bps spread. Either way it is a deal that clears, and the
    screen reports it; the two flags beside it say which bar it was."""
    frame = _row()
    out = returns(frame, ProformaAssumptions(), REBUILD, ENHANCE)
    hurdle_only = screens(frame, out, ProformaAssumptions(hurdle_irr_pct=1.0, min_yoc_spread_bps=400.0))
    assert hurdle_only.loc[0, "clears_hurdle"] and not hurdle_only.loc[0, "clears_cap_rate"]
    assert hurdle_only.loc[0, "is_good_candidate"]
    cap_only = screens(frame, out, ProformaAssumptions(hurdle_irr_pct=40.0, min_yoc_spread_bps=50.0))
    assert cap_only.loc[0, "clears_cap_rate"] and not cap_only.loc[0, "clears_hurdle"]
    assert cap_only.loc[0, "is_good_candidate"]


def test_an_improvement_is_judged_on_the_enhancement():
    frame = _row(site_thesis="improvement")
    out = returns(frame, ProformaAssumptions(), REBUILD, ENHANCE)
    verdict = screens(frame, out, ProformaAssumptions())
    assert verdict.loc[0, "site_irr_pct"] == out.loc[0, "buyer_irr_enhance_pct"]
    assert verdict.loc[0, "site_all_in_yield_on_cost_pct"] == out.loc[0, "buyer_yoc_enhance_pct"]


def test_the_hurdle_is_the_lots_own_cap_plus_a_spread():
    """A pure-commercial scheme exits wider and is asked for more, which one
    flat number cannot say."""
    a = ProformaAssumptions(
        commercial_cap_rate_spread_bps=175.0, hurdle_irr_spread_bps=100.0
    )
    housing = _mixed(hbu_residential_noi_cad=150_000.0, hbu_commercial_noi_cad=0.0,
                     hbu_commercial_floor_area_with_cellar_m2=0.0)
    shops = _mixed(hbu_residential_noi_cad=0.0, hbu_commercial_noi_cad=150_000.0)
    for frame, cap in ((housing, 4.5), (shops, 6.25)):
        out = returns(frame, a, REBUILD, ENHANCE)
        verdict = screens(pd.concat([frame, out], axis=1), out, a)
        assert out.loc[0, "market_cap_rate_pct"] == pytest.approx(cap)
        assert verdict.loc[0, "site_hurdle_irr_pct"] == pytest.approx(cap + 1.0)


def test_the_hurdle_is_what_the_irr_is_actually_tested_against():
    a = ProformaAssumptions(hurdle_irr_spread_bps=100.0)
    frame = _mixed()
    out = returns(frame, a, REBUILD, ENHANCE)
    verdict = screens(pd.concat([frame, out], axis=1), out, a)
    irr = verdict.loc[0, "site_irr_pct"]
    hurdle = verdict.loc[0, "site_hurdle_irr_pct"]
    assert verdict.loc[0, "clears_hurdle"] == (irr >= hurdle)


def test_a_flat_hurdle_overrides_the_derived_one_on_every_lot():
    """The levered convention, for a caller who wants to reproduce it."""
    a = ProformaAssumptions(hurdle_irr_pct=12.0)
    frame = _mixed()
    out = returns(frame, a, REBUILD, ENHANCE)
    verdict = screens(pd.concat([frame, out], axis=1), out, a)
    assert verdict.loc[0, "site_hurdle_irr_pct"] == pytest.approx(12.0)
    assert not verdict.loc[0, "clears_hurdle"]


def test_a_deal_that_clears_the_yield_test_now_nearly_clears_the_irr_one():
    """The two screens are two views of one bar at the same spread, so a deal
    sitting exactly on the yield test lands within a point of the hurdle -
    against the seven points a flat 12 put between them."""
    a = ProformaAssumptions(min_yoc_spread_bps=100.0, hurdle_irr_spread_bps=100.0)
    acq, hard = 500_000.0, 1_000_000.0
    tdc = acq + hard * a.budget_factor
    frame = pd.DataFrame([{
        "site_thesis": "teardown", "acquisition_cost_cad": acq,
        "site_costs_cad": 0.0, "hbu_total_capital_cost_cad": hard,
        "hbu_annual_stabilised_noi_cad": tdc * 5.5 / 100.0,  # exactly cap + 100 bps
        "hbu_num_dwellings": 20, "existing_annual_stabilised_noi_cad": 0.0,
        "enhance_solved": False, "site_verdict_cad": 1.0,
    }])
    out = returns(frame, a, REBUILD, ENHANCE)
    verdict = screens(pd.concat([frame, out], axis=1), out, a)
    assert verdict.loc[0, "clears_cap_rate"]
    assert verdict.loc[0, "site_hurdle_irr_pct"] == pytest.approx(5.5)
    assert abs(verdict.loc[0, "site_irr_pct"] - 5.5) < 1.0


def test_a_thesis_that_does_not_pay_is_never_a_good_candidate():
    frame = _row(site_verdict_cad=-1.0)
    out = returns(frame, ProformaAssumptions(hurdle_irr_pct=0.0, min_yoc_spread_bps=0.0), REBUILD, ENHANCE)
    verdict = screens(frame, out, ProformaAssumptions(hurdle_irr_pct=0.0, min_yoc_spread_bps=0.0))
    assert verdict.loc[0, "clears_cap_rate"] and verdict.loc[0, "clears_hurdle"]
    assert not verdict.loc[0, "is_good_candidate"]


def test_assumptions_that_make_no_sense_are_refused():
    with pytest.raises(ValueError):
        ProformaAssumptions(soft_cost_pct=-1.0)
    with pytest.raises(ValueError):
        ProformaAssumptions(absorption_units_per_month=0.0)
    with pytest.raises(ValueError):
        ProformaAssumptions(selling_cost_pct=100.0)
    with pytest.raises(ValueError):
        ProformaAssumptions(commercial_absorption_sqft_per_month=0.0)
    with pytest.raises(ValueError):
        ProformaAssumptions(industrial_absorption_sqft_per_month=-1.0)
    # A negative spread would say commerce sells dearer than housing, which is
    # not what a spread over the residential cap means.
    with pytest.raises(ValueError):
        ProformaAssumptions(commercial_cap_rate_spread_bps=-1.0)


# -- a building that is not all dwellings -----------------------------------


def _mixed(**overrides) -> pd.DataFrame:
    """A shortlist row with a ground floor of shops under the flats: a third
    of the income is commerce, on 600 m2 of it."""
    mix = {
        "hbu_annual_stabilised_noi_cad": 150_000.0,
        "hbu_residential_noi_cad": 100_000.0,
        "hbu_commercial_noi_cad": 50_000.0,
        "hbu_industrial_noi_cad": 0.0,
        "hbu_commercial_floor_area_with_cellar_m2": 600.0,
        "hbu_industrial_floor_area_with_cellar_m2": 0.0,
        "existing_dominant_income_class": "residential",
    }
    mix.update(overrides)
    return _row(**mix)


def test_a_mixed_program_sells_at_a_cap_blended_to_its_own_income():
    assumptions = ProformaAssumptions(commercial_cap_rate_spread_bps=180.0)
    out = returns(_mixed(), assumptions, REBUILD, ENHANCE)
    # A third of the income is commerce, so a third of the 180 bp spread.
    assert out.loc[0, "hbu_non_residential_income_share"] == pytest.approx(
        1 / 3, abs=1e-4  # the column is reported to four places
    )
    # 100k of housing at 4.5 and 50k of commerce at 6.3, valued apart and
    # added, is one building at 4.9737 - not the 5.1 an average of the two
    # caps would say, which would price it 2 pct under its own parts.
    parts = 100_000.0 / 0.045 + 50_000.0 / 0.063
    blended = 100.0 * 150_000.0 / parts
    assert blended == pytest.approx(4.9737, abs=1e-4)
    assert out.loc[0, "market_cap_rate_pct"] == pytest.approx(blended, abs=1e-4)
    assert out.loc[0, "exit_cap_rate_rebuild_pct"] == pytest.approx(blended, abs=1e-4)
    # And the screen holds the yield against that rather than against 4.5.
    yoc = out.loc[0, "buyer_yoc_rebuild_pct"]
    assert out.loc[0, "yoc_spread_rebuild_bps"] == pytest.approx(
        (yoc - blended) * 100, abs=0.2
    )


def test_the_commerce_costs_the_mixed_program_lease_up_and_exit_value():
    """The same income, once as dwellings and once as a third of it shops."""
    assumptions = ProformaAssumptions(commercial_absorption_sqft_per_month=1_000.0)
    housing = returns(
        _mixed(hbu_residential_noi_cad=150_000.0, hbu_commercial_noi_cad=0.0,
               hbu_commercial_floor_area_with_cellar_m2=0.0),
        assumptions, REBUILD, ENHANCE,
    )
    mixed = returns(_mixed(), assumptions, REBUILD, ENHANCE)
    # 600 m2 is 6 458 sq ft: seven months, against the six the dwellings take.
    assert housing.loc[0, "rebuild_lease_up_months"] == 6
    assert mixed.loc[0, "rebuild_lease_up_months"] == 7
    # Same NOI, same cost, so the same yield on cost - and a lower IRR, because
    # it fills more slowly and sells at a wider cap.
    assert mixed.loc[0, "buyer_yoc_rebuild_pct"] == pytest.approx(
        housing.loc[0, "buyer_yoc_rebuild_pct"]
    )
    assert mixed.loc[0, "buyer_irr_rebuild_pct"] < housing.loc[0, "buyer_irr_rebuild_pct"]


def test_the_standing_building_is_capped_on_what_already_stands():
    """An owner giving up a warehouse for flats gives up industrial income,
    and the two halves of that difference are not one cap."""
    assumptions = ProformaAssumptions(industrial_cap_rate_spread_bps=200.0)
    on_industry = returns(
        _mixed(existing_dominant_income_class="industrial"), assumptions, REBUILD, ENHANCE
    )
    on_housing = returns(
        _mixed(existing_dominant_income_class="residential"), assumptions, REBUILD, ENHANCE
    )
    # The standing income is worth less at the wider cap, so it is cheaper to
    # give up and the owner's rebuild is worth more.
    assert on_industry.loc[0, "owner_irr_rebuild_pct"] > on_housing.loc[0, "owner_irr_rebuild_pct"]
    # And buying that same income to hold it yields the same but exits lower.
    assert on_industry.loc[0, "buyer_yoc_hold_pct"] == pytest.approx(
        on_housing.loc[0, "buyer_yoc_hold_pct"]
    )
    assert on_industry.loc[0, "buyer_irr_hold_pct"] < on_housing.loc[0, "buyer_irr_hold_pct"]


def test_zero_spreads_price_every_family_at_the_residential_cap():
    """The stance this module took before the spreads existed, on one switch."""
    flat = ProformaAssumptions(
        commercial_cap_rate_spread_bps=0.0, industrial_cap_rate_spread_bps=0.0
    )
    out = returns(_mixed(), flat, REBUILD, ENHANCE)
    assert out.loc[0, "market_cap_rate_pct"] == pytest.approx(4.5)
    assert out.loc[0, "exit_cap_rate_rebuild_pct"] == pytest.approx(4.5)


def test_a_frame_with_no_use_split_is_priced_as_it_always_was():
    """A partition written before the split reads as all-residential rather
    than as a lot with no cap."""
    out = returns(_row(), ProformaAssumptions(), REBUILD, ENHANCE)
    assert out.loc[0, "market_cap_rate_pct"] == pytest.approx(4.5)
    assert out.loc[0, "exit_cap_rate_rebuild_pct"] == pytest.approx(4.5)
    assert pd.isna(out.loc[0, "hbu_non_residential_income_share"])
    assert out.loc[0, "rebuild_lease_up_months"] == 6


def test_an_improvement_estimate_stands_in_for_a_missing_solve():
    """A partition without the enhancement solve still states a return on the
    closed-form addition, and one with neither states none."""
    out = returns(
        _row(enhance_solved=False, improvement_cost_cad=300_000.0, improvement_noi_cad=24_000.0),
        ProformaAssumptions(), REBUILD, ENHANCE,
    )
    assert out.loc[0, "enhance_budget_cad"] == pytest.approx(300_000.0 * ProformaAssumptions().budget_factor)
    assert not pd.isna(out.loc[0, "buyer_irr_enhance_pct"])
    assert not pd.isna(out.loc[0, "owner_irr_enhance_pct"])
    assert out.loc[0, "enhance_lease_up_months"] == ENHANCE.lease_up_months
    none = returns(_row(enhance_solved=False), ProformaAssumptions(), REBUILD, ENHANCE)
    assert pd.isna(none.loc[0, "buyer_irr_enhance_pct"])
