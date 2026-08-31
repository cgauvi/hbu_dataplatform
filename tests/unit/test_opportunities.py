"""Which under-built lots to look at first, and under which thesis.

Two halves. `urban_rag.opportunities` is arithmetic over a frame and is tested
directly - where the thesis lines fall, what the yield divides by, and what the
ranking prefers. `lot_investment_opportunities` is tested by materializing it
over a hand-written `lot_redevelopment_gap` partition, the same way
`test_comparables.py` exercises the asset it sits behind.

The fixture borough is nine lots, each the only one exercising its case: the
four theses, the mixed-use boundary on both sides, a lot that is not
under-built, one the roll never assessed, and one the solver found no program
for.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize

from asset_helpers import materialization_metadata, stub_publish
from urban_rag import opportunity_assets
from urban_rag.hbu_assets import LOT_GAP_FILE, lot_redevelopment_gap
from urban_rag.frames import write_frame
from urban_rag.opportunities import (
    COMMERCIAL,
    INDUSTRIAL,
    INVESTMENT_THESES,
    MIXED_USE,
    NO_THESIS,
    RESIDENTIAL,
    ThesisRules,
    assign_thesis,
    rank_opportunities,
    thesis_summary,
    yield_on_cost_pct,
)
from urban_rag.opportunity_assets import (
    LOT_OPPORTUNITIES_FILE,
    lot_investment_opportunities,
)
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import join

DATE = "2026-08-26"
NEIGHBORHOOD = "VSMPE"


def gap_frame(rows: list[dict]) -> pd.DataFrame:
    """A `lot_redevelopment_gap` partition, defaults filled in."""
    defaults = {
        "lot_number": "1 000 000",
        "lot_area_m2": 300.0,
        "primary_frontage_m": 10.0,
        "hbu_status": "solved",
        "is_underbuilt": True,
        "existing_dominant_income_class": "residential",
        "existing_num_dwellings": 2,
        "existing_floor_area_m2": 200.0,
        "existing_total_assessed_value": 400_000.0,
        "existing_cap_rate_pct": 3.5,
        "existing_annual_stabilised_noi_cad": 20_000.0,
        "hbu_num_dwellings": 10,
        "hbu_floor_area_m2": 1_000.0,
        "hbu_residential_floor_area_m2": 1_000.0,
        "hbu_commercial_floor_area_m2": 0.0,
        "hbu_industrial_floor_area_m2": 0.0,
        "hbu_annual_stabilised_noi_cad": 120_000.0,
        "hbu_total_capital_cost_cad": 1_500_000.0,
        "dwelling_gap": 8,
        "floor_area_gap_m2": 800.0,
        "annual_stabilised_noi_gap_cad": 100_000.0,
        "operating_expense_ratio": 0.35,
    }
    frame = pd.DataFrame([{**defaults, **row} for row in rows])
    frame.insert(0, "lot_uid", range(1, len(frame) + 1))
    frame["neighborhood"] = NEIGHBORHOOD
    frame["scrape_date"] = DATE
    return frame


# -- the thesis -------------------------------------------------------------


@pytest.mark.parametrize(
    "resi, comm, indus, expected",
    [
        (1000.0, 0.0, 0.0, RESIDENTIAL),
        (0.0, 1000.0, 0.0, COMMERCIAL),
        (0.0, 0.0, 1000.0, INDUSTRIAL),
        # 95/5 - the commercial slice is incidental, so the dominant rule wins.
        (950.0, 50.0, 0.0, RESIDENTIAL),
        # 80/20 - a ground floor under five storeys of flats.
        (800.0, 200.0, 0.0, MIXED_USE),
        # Exactly on the mixed threshold, which is inclusive.
        (850.0, 150.0, 0.0, MIXED_USE),
        # Neither dominant nor mixed by the commercial share: falls to the
        # largest class, because that is most of the building.
        (600.0, 0.0, 400.0, RESIDENTIAL),
        (0.0, 0.0, 0.0, NO_THESIS),
    ],
)
def test_the_thesis_is_read_off_the_proposed_mix(resi, comm, indus, expected):
    frame = pd.DataFrame(
        {
            "hbu_residential_floor_area_m2": [resi],
            "hbu_commercial_floor_area_m2": [comm],
            "hbu_industrial_floor_area_m2": [indus],
        }
    )

    assert assign_thesis(frame).iloc[0] == expected


def test_a_lot_with_no_program_is_not_filed_under_a_thesis():
    """An all-null row, which is what a lot the solver never reached looks
    like. `idxmax` raises on one, so this is also a regression guard."""
    frame = pd.DataFrame(
        {
            "hbu_residential_floor_area_m2": [np.nan],
            "hbu_commercial_floor_area_m2": [np.nan],
            "hbu_industrial_floor_area_m2": [np.nan],
        }
    )

    assert assign_thesis(frame).iloc[0] == NO_THESIS


def test_the_thesis_describes_the_opportunity_not_the_existing_use():
    """A warehouse whose best use is flats is a residential play. Filing it
    under industrial would put it in the one facet that never looks at it."""
    frame = pd.DataFrame(
        {
            "existing_dominant_income_class": ["industrial"],
            "hbu_residential_floor_area_m2": [2000.0],
            "hbu_commercial_floor_area_m2": [0.0],
            "hbu_industrial_floor_area_m2": [0.0],
        }
    )

    assert assign_thesis(frame).iloc[0] == RESIDENTIAL


def test_moving_the_mixed_threshold_moves_the_facet():
    frame = pd.DataFrame(
        {
            "hbu_residential_floor_area_m2": [900.0],
            "hbu_commercial_floor_area_m2": [100.0],
            "hbu_industrial_floor_area_m2": [0.0],
        }
    )

    assert assign_thesis(frame, ThesisRules()).iloc[0] == RESIDENTIAL
    assert (
        assign_thesis(frame, ThesisRules(mixed_min_share=0.10)).iloc[0] == MIXED_USE
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dominant_share": 0.0},
        {"dominant_share": 1.5},
        {"mixed_min_share": 0.5},
        {"mixed_min_share": 0.0},
        # 0.7 dominant with 0.4 mixed: an 0.65/0.35 split would satisfy both,
        # so the dominant rule could never fire on its own.
        {"dominant_share": 0.7, "mixed_min_share": 0.4},
    ],
)
def test_rules_that_contradict_themselves_are_refused(kwargs):
    with pytest.raises(ValueError):
        ThesisRules(**kwargs)


# -- the yield --------------------------------------------------------------


def test_yield_on_cost_puts_the_land_in_the_denominator():
    """Leaving it out would rank a teardown beside an empty lot as though they
    cost the same to acquire."""
    frame = pd.DataFrame(
        {
            "hbu_annual_stabilised_noi_cad": [120_000.0],
            "hbu_total_capital_cost_cad": [1_500_000.0],
            "existing_total_assessed_value": [400_000.0],
        }
    )

    assert yield_on_cost_pct(frame).iloc[0] == pytest.approx(
        100.0 * 120_000.0 / 1_900_000.0
    )


def test_the_land_factor_scales_only_the_land():
    frame = pd.DataFrame(
        {
            "hbu_annual_stabilised_noi_cad": [120_000.0],
            "hbu_total_capital_cost_cad": [1_500_000.0],
            "existing_total_assessed_value": [400_000.0],
        }
    )

    assert yield_on_cost_pct(frame, land_value_factor=1.5).iloc[0] == pytest.approx(
        100.0 * 120_000.0 / (1_500_000.0 + 600_000.0)
    )


def test_a_lot_the_roll_never_assessed_has_no_yield_rather_than_a_flattering_one():
    """Counting its land at nothing would rank it top of every facet."""
    frame = pd.DataFrame(
        {
            "hbu_annual_stabilised_noi_cad": [120_000.0],
            "hbu_total_capital_cost_cad": [1_500_000.0],
            "existing_total_assessed_value": [np.nan],
        }
    )

    assert pd.isna(yield_on_cost_pct(frame).iloc[0])


# -- the ranking ------------------------------------------------------------


def test_yield_beats_size_which_is_the_whole_point():
    """A small cheap parcel outranks a large dear one. Ranking on the raw gap
    would put every facet's biggest lot on top regardless of cost."""
    frame = gap_frame(
        [
            {
                "hbu_annual_stabilised_noi_cad": 400_000.0,
                "hbu_total_capital_cost_cad": 9_000_000.0,
                "existing_total_assessed_value": 1_000_000.0,
                "annual_stabilised_noi_gap_cad": 380_000.0,
            },
            {
                "hbu_annual_stabilised_noi_cad": 120_000.0,
                "hbu_total_capital_cost_cad": 900_000.0,
                "existing_total_assessed_value": 200_000.0,
                "annual_stabilised_noi_gap_cad": 100_000.0,
            },
        ]
    )

    ranked = rank_opportunities(frame)

    # The second lot has the smaller gap and the better yield, and wins.
    assert ranked["thesis_rank"].tolist() == [2, 1]


def test_the_noi_gap_breaks_a_tie_on_yield():
    frame = gap_frame(
        [
            {
                "hbu_annual_stabilised_noi_cad": 100_000.0,
                "hbu_total_capital_cost_cad": 1_000_000.0,
                "existing_total_assessed_value": 0.0,
                "annual_stabilised_noi_gap_cad": 10_000.0,
            },
            {
                "hbu_annual_stabilised_noi_cad": 100_000.0,
                "hbu_total_capital_cost_cad": 1_000_000.0,
                "existing_total_assessed_value": 0.0,
                "annual_stabilised_noi_gap_cad": 90_000.0,
            },
        ]
    )

    ranked = rank_opportunities(frame)

    assert ranked["thesis_rank"].tolist() == [2, 1]


def test_rank_is_within_the_thesis_not_across_the_borough():
    """Rank 1 is the best residential play *and* the best industrial one."""
    frame = gap_frame(
        [
            {"hbu_annual_stabilised_noi_cad": 200_000.0},
            {
                "hbu_residential_floor_area_m2": 0.0,
                "hbu_industrial_floor_area_m2": 1_000.0,
                "hbu_annual_stabilised_noi_cad": 60_000.0,
            },
        ]
    )

    ranked = pd.concat([frame, rank_opportunities(frame)], axis=1)

    assert ranked.loc[0, "investment_thesis"] == RESIDENTIAL
    assert ranked.loc[1, "investment_thesis"] == INDUSTRIAL
    assert ranked["thesis_rank"].tolist() == [1, 1]


def test_a_lot_already_built_to_its_envelope_is_not_an_opportunity():
    frame = gap_frame([{"is_underbuilt": False}, {"is_underbuilt": True}])

    ranked = rank_opportunities(frame)

    assert pd.isna(ranked.loc[0, "thesis_rank"])
    assert ranked.loc[1, "thesis_rank"] == 1
    # It keeps its row and its thesis - the table is an inventory.
    assert ranked.loc[0, "investment_thesis"] == RESIDENTIAL


def test_an_all_null_underbuilt_column_does_not_select_everything():
    """`~` on an object column of Python bools is arithmetic negation, which is
    the trap `role_assets._empty_pairs` documents. `_boolean` is the guard."""
    frame = gap_frame([{"is_underbuilt": None}, {"is_underbuilt": None}])
    frame["is_underbuilt"] = frame["is_underbuilt"].astype("object")

    ranked = rank_opportunities(frame)

    assert ranked["thesis_rank"].isna().all()


def test_top_n_marks_a_flag_and_does_not_change_the_rank():
    frame = gap_frame(
        [
            {"hbu_annual_stabilised_noi_cad": noi}
            for noi in (300_000.0, 200_000.0, 100_000.0)
        ]
    )

    two = rank_opportunities(frame, top_n=2)
    three = rank_opportunities(frame, top_n=3)

    assert two["thesis_rank"].tolist() == three["thesis_rank"].tolist()
    assert two["is_top_opportunity"].tolist() == [True, True, False]
    assert three["is_top_opportunity"].tolist() == [True, True, True]


def test_the_ranking_is_stable_across_identical_runs():
    """Two lots that score identically must not reshuffle between runs, or a
    shortlist changes for a partition nothing about changed."""
    frame = gap_frame([{}, {}, {}])

    first = rank_opportunities(frame)["thesis_rank"].tolist()
    second = rank_opportunities(frame)["thesis_rank"].tolist()

    assert first == second


def test_every_thesis_gets_a_summary_row_even_when_empty():
    """An empty facet is an answer; a missing row is not."""
    frame = gap_frame([{}])
    summary = thesis_summary(pd.concat([frame, rank_opportunities(frame)], axis=1))

    assert summary["investment_thesis"].tolist() == list(INVESTMENT_THESES)
    industrial = summary.set_index("investment_thesis").loc[INDUSTRIAL]
    assert industrial["num_lots"] == 0
    assert pd.isna(industrial["best_yield_on_cost_pct"])


# -- the asset --------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture(autouse=True)
def published(monkeypatch):
    return stub_publish(monkeypatch, opportunity_assets)


#: Nine lots, each the only one exercising its case.
BOROUGH = [
    # 0 residential, best yield of its thesis
    {
        "hbu_annual_stabilised_noi_cad": 200_000.0,
        "hbu_total_capital_cost_cad": 900_000.0,
    },
    # 1 residential, worse yield
    {"hbu_annual_stabilised_noi_cad": 100_000.0},
    # 2 mixed-use
    {"hbu_residential_floor_area_m2": 800.0, "hbu_commercial_floor_area_m2": 200.0},
    # 3 commercial
    {"hbu_residential_floor_area_m2": 0.0, "hbu_commercial_floor_area_m2": 1000.0},
    # 4 industrial, converting from a warehouse
    {
        "hbu_residential_floor_area_m2": 0.0,
        "hbu_industrial_floor_area_m2": 1500.0,
        "existing_dominant_income_class": "industrial",
    },
    # 5 a warehouse whose best use is flats - the conversion play
    {"existing_dominant_income_class": "industrial"},
    # 6 already built to its envelope
    {"is_underbuilt": False},
    # 7 the roll never assessed it
    {"existing_total_assessed_value": None},
    # 8 no program at all
    {
        "hbu_residential_floor_area_m2": 0.0,
        "hbu_commercial_floor_area_m2": 0.0,
        "hbu_industrial_floor_area_m2": 0.0,
        "hbu_status": "no_program",
        "hbu_annual_stabilised_noi_cad": None,
        "hbu_total_capital_cost_cad": None,
    },
]


@pytest.fixture
def borough(store):
    write_frame(
        gap_frame(BOROUGH),
        join(
            store.partition_dir(
                lot_redevelopment_gap.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_GAP_FILE,
        ),
    )
    return store


def run(store, **config):
    return materialize(
        [lot_investment_opportunities],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        run_config=(
            {"ops": {"gold__lot_investment_opportunities": {"config": config}}}
            if config
            else None
        ),
    )


def written(store) -> pd.DataFrame:
    return pd.read_parquet(
        Path(
            store.partition_dir(
                lot_investment_opportunities.key.path[-1], DATE, NEIGHBORHOOD
            )
        )
        / LOT_OPPORTUNITIES_FILE
    ).set_index("lot_uid")


def test_every_lot_keeps_its_row(borough):
    result = run(borough)

    assert result.success
    assert len(written(borough)) == len(BOROUGH)


def test_the_four_theses_are_all_represented(borough):
    run(borough)

    theses = set(written(borough)["investment_thesis"])
    assert {RESIDENTIAL, MIXED_USE, COMMERCIAL, INDUSTRIAL, NO_THESIS} == theses


def test_a_conversion_shows_up_as_the_two_columns_differing(borough):
    """The screen a mandate actually runs: what it is now against what it
    should be."""
    run(borough)

    frame = written(borough)
    # Lot 6 (uid 6) is a warehouse whose best use is flats.
    row = frame.loc[6]
    assert row["existing_dominant_income_class"] == "industrial"
    assert row["investment_thesis"] == RESIDENTIAL


def test_the_unranked_rows_each_say_why(borough):
    run(borough)

    frame = written(borough)
    assert not frame.loc[7, "is_underbuilt"]          # built to its envelope
    assert not frame.loc[8, "is_land_assessed"]       # roll never reached it
    assert frame.loc[9, "investment_thesis"] == NO_THESIS
    assert frame.loc[[7, 8, 9], "thesis_rank"].isna().all()


def test_the_best_residential_yield_ranks_first(borough):
    run(borough)

    frame = written(borough)
    residential = frame[frame["investment_thesis"] == RESIDENTIAL]
    best = residential.sort_values("thesis_rank").index[0]
    assert best == 1  # the lot with the cheapest build and the highest NOI


def test_every_row_records_the_thresholds_behind_its_facet(borough):
    """A shortlist read a month later has only the row."""
    run(
        borough,
        dominant_share=0.9,
        mixed_min_share=0.10,
        land_value_factor=1.3,
        top_n=2,
    )

    payload = json.loads(written(borough)["screen_assumptions"].iloc[0])
    assert payload["dominant_share"] == 0.9
    assert payload["mixed_min_share"] == 0.10
    assert payload["land_value_factor"] == 1.3
    assert payload["top_n"] == 2


def test_the_land_factor_moves_every_yield(borough):
    run(borough)
    at_roll = written(borough)["yield_on_cost_pct"].dropna()
    run(borough, land_value_factor=2.0)
    dearer = written(borough)["yield_on_cost_pct"].dropna()

    assert (dearer < at_roll).all()


def test_the_run_reports_a_facet_summary(borough):
    result = run(borough, top_n=1)

    metadata = materialization_metadata(result, lot_investment_opportunities)
    assert metadata["num_lots"].value == len(BOROUGH)
    assert metadata["num_residential"].value == 5
    assert metadata["num_top_opportunities"].value == len(
        {RESIDENTIAL, MIXED_USE, COMMERCIAL, INDUSTRIAL}
    )
    assert "thesis_summary" in metadata


def test_a_missing_upstream_names_the_asset_to_materialize(store):
    with pytest.raises(Failure, match="materialize lot_redevelopment_gap"):
        run(store)


def test_the_frame_that_was_written_is_the_frame_that_is_published(borough, published):
    run(borough)

    assert published["calls"] == 1
    assert set(published["datasets"]) == {"lot_investment_opportunities"}
    assert len(published["datasets"]["lot_investment_opportunities"]) == len(BOROUGH)
