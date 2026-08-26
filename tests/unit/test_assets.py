"""Offline tests for the Spectrum assets and the tree they write into.

The Feature Service is stubbed out, so what is actually under test is the
output layout - `<root>/<asset>/<date>[/<neighborhood>]` - and the handoff
between the two assets, which now goes through a parquet file in that tree
rather than through Dagster's IO manager.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize

from urban_rag.assets import (
    CATALOG_FILE,
    neighborhood_features,
    spectrum_table_catalog,
)
from urban_rag.resources import ParquetStore, SpectrumResource
from urban_rag.spectrum import Column, TableMetadata

DATE = "2026-08-20"
NEIGHBORHOOD = "VSMPE"

ZONE_TABLE = "/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE"
ZONE_SLUG = "Reglement_urbanisme__VSP_REG_ZONE"

#: One VSMPE table, one from another borough, so the namespace filter has
#: something to exclude.
TABLES = [ZONE_TABLE, "/18_VM/Reglement_urbanisme/VM_REG_ZONE"]

FEATURES = [
    {
        "type": "Feature",
        "properties": {"ID": 1, "DESCRIPTION": "zone A"},
        "geometry": {"type": "Point", "coordinates": [-73.6, 45.5]},
    },
    {
        "type": "Feature",
        "properties": {"ID": 2, "DESCRIPTION": "zone B"},
        "geometry": {"type": "Point", "coordinates": [-73.7, 45.6]},
    },
]


class FakeClient:
    """Just the three methods the assets call."""

    def __init__(self, tables=None, features=None):
        self.tables = TABLES if tables is None else tables
        self.features = FEATURES if features is None else features

    def list_tables(self, namespace=None):
        return list(self.tables)

    def table_metadata(self, table):
        return TableMetadata(
            table=table,
            columns=(Column("ID", "Integer"), Column("DESCRIPTION", "String")),
            geometry_column="SP_GEOMETRY",
            native_crs="epsg:42104",
        )

    def fetch_features(self, metadata, *, page_length=500):
        return iter(self.features)


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path))


@pytest.fixture
def spectrum(monkeypatch):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    client = FakeClient()
    monkeypatch.setattr(SpectrumResource, "client", lambda self: client)
    return client


def materialize_catalog(store, *, scrape_date=DATE):
    result = materialize(
        [spectrum_table_catalog],
        partition_key=scrape_date,
        resources={"spectrum": SpectrumResource(), "store": store},
    )
    assert result.success
    return result


def materialize_features(store, *, scrape_date=DATE, neighborhood=NEIGHBORHOOD):
    return materialize(
        [neighborhood_features],
        partition_key=MultiPartitionKey(
            {"date": scrape_date, "neighborhood": neighborhood}
        ),
        resources={"spectrum": SpectrumResource(), "store": store},
    )


def test_catalog_lands_under_its_own_asset_prefix(tmp_path, store, spectrum):
    materialize_catalog(store)

    frame = pd.read_parquet(
        tmp_path / "bronze" / "spectrum_table_catalog" / DATE / CATALOG_FILE
    )
    assert frame["table"].tolist() == TABLES
    assert frame["namespace"].tolist() == ["19_VSMPE", "18_VM"]
    assert frame["scrape_date"].unique().tolist() == [DATE]


def test_features_land_under_asset_date_and_neighborhood(tmp_path, store, spectrum):
    materialize_catalog(store)

    assert materialize_features(store).success

    partition = tmp_path / "bronze" / "neighborhood_features" / DATE / NEIGHBORHOOD
    assert [p.name for p in partition.glob("*.parquet")] == [f"{ZONE_SLUG}.parquet"]

    frame = gpd.read_parquet(partition / f"{ZONE_SLUG}.parquet")
    assert len(frame) == 2
    assert frame.crs.to_string() == "EPSG:4326"
    # The prefix carries bare values, so both keys travel as columns.
    assert frame["neighborhood"].unique().tolist() == [NEIGHBORHOOD]
    assert frame["scrape_date"].unique().tolist() == [DATE]
    assert frame["source_table"].unique().tolist() == [ZONE_TABLE]


def test_features_read_the_catalog_written_for_their_own_date(
    tmp_path, store, spectrum
):
    materialize_catalog(store, scrape_date="2026-08-19")

    with pytest.raises(Failure, match="spectrum_table_catalog"):
        materialize_features(store, scrape_date=DATE)


def test_a_rerun_replaces_the_previous_snapshot(tmp_path, store, spectrum):
    materialize_catalog(store)
    partition = tmp_path / "bronze" / "neighborhood_features" / DATE / NEIGHBORHOOD
    partition.mkdir(parents=True)
    stale = partition / "Reglement_urbanisme__VSP_REG_RETIRED.parquet"
    pd.DataFrame({"a": [1]}).to_parquet(stale)

    assert materialize_features(store).success

    assert not stale.exists()
