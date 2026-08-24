"""Offline test for `building_lot_intersections`.

`urban_rag.postgis`'s load/compute functions are Postgres-only in substance -
they issue COPY and INSERT ... ST_Intersection statements - so nothing here
touches a real database. What is worth testing without one is the asset's own
logic: which partition's enriched lot parquet it reads, what it hands to
`postgis.load_lots`/`load_buildings`/`compute_intersections`, and how their
return values turn into `MaterializeResult` metadata. `postgis.py` itself has
no unit test for the same reason `rag/pgvector.py`'s `load_partition` does
not - both need Postgres/PostGIS to mean anything.
"""

from __future__ import annotations

from contextlib import contextmanager

import geopandas as gpd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import box

from urban_rag import building_lots_assets
from urban_rag.bdoi_assets import BUILDINGS_FILE, neighborhood_buildings
from urban_rag.building_lots_assets import building_lot_intersections
from urban_rag.frames import write_frame
from urban_rag.lot_vacancy_assets import (
    LOTS_WITH_VACANCY_FILE,
    lots_with_vacancy_rates,
)
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import join

DATE = "2026-08-20"
NEIGHBORHOOD = "VSMPE"


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_lots(store, *, lot_numbers=("1", "2"), geometries=None):
    path = join(
        store.partition_dir(
            lots_with_vacancy_rates.key.path[-1], DATE, NEIGHBORHOOD
        ),
        LOTS_WITH_VACANCY_FILE,
    )
    frame = gpd.GeoDataFrame(
        {"NO_LOT": list(lot_numbers)},
        geometry=geometries if geometries is not None else [box(0, 0, 1, 1), box(1, 0, 2, 1)],
        crs="EPSG:4326",
    )
    write_frame(frame, path)


def write_buildings(store, *, link_ids=(1,), geometries=None):
    path = join(
        store.partition_dir(neighborhood_buildings.key.path[-1], DATE, NEIGHBORHOOD),
        BUILDINGS_FILE,
    )
    frame = gpd.GeoDataFrame(
        {"link_id": list(link_ids)},
        # Straddles both lots `write_lots` writes by default.
        geometry=geometries if geometries is not None else [box(0.5, 0.25, 1.5, 0.75)],
        crs="EPSG:4326",
    )
    write_frame(frame, path)


def stub_postgis(monkeypatch, *, intersections=1, buildings_matched=1, total_area_m2=50_000.0):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    calls: dict[str, tuple] = {}

    @contextmanager
    def connect(self):
        yield object()

    def load_lots(connection, frame, *, neighborhood, scrape_date):
        calls["lots"] = (neighborhood, scrape_date, len(frame))
        return len(frame)

    def load_buildings(connection, frame, *, neighborhood, scrape_date):
        calls["buildings"] = (neighborhood, scrape_date, len(frame))
        return len(frame)

    def compute_intersections(connection, *, neighborhood, scrape_date):
        calls["intersections"] = (neighborhood, scrape_date)
        return {
            "intersections": intersections,
            "buildings_matched": buildings_matched,
            "total_area_m2": total_area_m2,
        }

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(building_lots_assets, "load_lots", load_lots)
    monkeypatch.setattr(building_lots_assets, "load_buildings", load_buildings)
    monkeypatch.setattr(
        building_lots_assets, "compute_intersections", compute_intersections
    )
    return calls


def run(store):
    return materialize(
        [building_lot_intersections],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[building_lot_intersections],
    )


def test_loads_both_partitions_then_computes_the_join(store, monkeypatch):
    write_lots(store)
    write_buildings(store)
    calls = stub_postgis(
        monkeypatch, intersections=2, buildings_matched=1, total_area_m2=50_000.0
    )

    result = run(store)

    assert result.success
    # Loaded before the join was computed, and with this partition's own key.
    assert calls["lots"] == (NEIGHBORHOOD, DATE, 2)
    assert calls["buildings"] == (NEIGHBORHOOD, DATE, 1)
    assert calls["intersections"] == (NEIGHBORHOOD, DATE)


def test_metadata_reports_what_was_loaded_and_matched(store, monkeypatch):
    write_lots(store)
    write_buildings(store)
    stub_postgis(monkeypatch, intersections=2, buildings_matched=1, total_area_m2=50_000.0)

    result = run(store)

    metadata = result.asset_materializations_for_node(
        "building_lot_intersections"
    )[0].metadata
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_lots"].value == 2
    assert metadata["num_buildings"].value == 1
    assert metadata["num_intersections"].value == 2
    assert metadata["num_buildings_matched"].value == 1
    assert metadata["num_buildings_unmatched"].value == 0
    assert metadata["total_intersection_area_ha"].value == pytest.approx(5.0)


def test_no_lot_in_the_partition_fails_with_what_it_means(store, monkeypatch):
    write_lots(store, lot_numbers=(), geometries=[])
    write_buildings(store)
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="holds no lot"):
        run(store)


def test_no_building_in_the_partition_fails_with_what_it_means(store, monkeypatch):
    write_lots(store)
    write_buildings(store, link_ids=(), geometries=[])
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="holds no building"):
        run(store)
