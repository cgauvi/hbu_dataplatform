"""Offline tests for the lot x CMHC name join.

The asset intentionally joins by the borough partition's ``neighborhood``
column, not by geometry, and widens CMHC's dwelling x bedroom grid before the
join so the lot layer remains one row per cadastral lot.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import box

from urban_rag.cmhc_assets import VACANCY_FILE, vacancy_rates
from urban_rag.frames import write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.lot_vacancy_assets import (
    LOTS_WITH_VACANCY_FILE,
    lots_with_vacancy_rates,
)
from urban_rag.resources import ParquetStore
from urban_rag.storage import join

DATE = "2026-08-20"
NEIGHBORHOOD = "VSMPE"


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_lots(store, *, neighborhood=NEIGHBORHOOD, scrape_date=DATE):
    path = join(
        store.partition_dir(neighborhood_lots.key.path[-1], DATE, NEIGHBORHOOD),
        LOTS_FILE,
    )
    frame = gpd.GeoDataFrame(
        {
            "NO_LOT": ["1", "2"],
            "neighborhood": [neighborhood, neighborhood],
            "scrape_date": [scrape_date, scrape_date],
        },
        geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1)],
        crs="EPSG:4326",
    )
    write_frame(frame, path)


def write_rates(store):
    path = join(
        store.partition_dir(vacancy_rates.key.path[-1], DATE, NEIGHBORHOOD),
        VACANCY_FILE,
    )
    frame = pd.DataFrame(
        {
            "neighborhood": [NEIGHBORHOOD, NEIGHBORHOOD],
            "scrape_date": [DATE, DATE],
            "dwelling_type": ["all", "apartment_other"],
            "bedroom_type": ["all", "2_bedroom"],
            "vacancy_rate_pct": [0.5, 0.4],
            "min_vacancy_rate_pct": [0.3, 0.2],
            "max_vacancy_rate_pct": [0.7, 0.6],
            "num_quartiers": [2, 2],
            "num_quartiers_mapped": [3, 3],
            "averaged_quartiers": [
                "Parc-Extension, Villeray",
                "Parc-Extension, Villeray",
            ],
            "survey_year": [2023, 2023],
            "survey_period": ["octobre 2023", "octobre 2023"],
        }
    )
    write_frame(frame, path)


def run(store):
    return materialize(
        [lots_with_vacancy_rates],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store},
        selection=[lots_with_vacancy_rates],
    )


def read_output(store):
    return gpd.read_parquet(
        join(
            store.partition_dir(
                lots_with_vacancy_rates.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOTS_WITH_VACANCY_FILE,
        )
    )


def test_lots_are_enriched_by_neighborhood_without_multiplying_rows(store):
    write_lots(store)
    write_rates(store)

    result = run(store)

    assert result.success
    frame = read_output(store)
    assert len(frame) == 2
    assert frame.crs.to_string() == "EPSG:4326"
    assert frame["NO_LOT"].tolist() == ["1", "2"]
    assert frame["cmhc_all_all_vacancy_rate_pct"].tolist() == [0.5, 0.5]
    assert frame["cmhc_all_all_num_quartiers"].tolist() == [2, 2]
    assert frame["cmhc_apartment_other_2_bedroom_vacancy_rate_pct"].tolist() == [
        0.4,
        0.4,
    ]
    assert frame["cmhc_survey_year"].tolist() == [2023, 2023]

    metadata = result.asset_materializations_for_node("lots_with_vacancy_rates")[
        0
    ].metadata
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_cmhc_rate_cells"].value == 2
    assert metadata["overall_vacancy_rate_pct"].value == pytest.approx(0.5)


def test_a_lot_partition_mismatch_fails_before_the_merge(store):
    write_lots(store, neighborhood="PMR")
    write_rates(store)

    with pytest.raises(Failure, match="carry neighborhood values"):
        run(store)
