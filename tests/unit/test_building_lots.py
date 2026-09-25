"""Offline test for `building_lot_intersections`, both joins it computes.

`hbu_dataplatform.postgis`'s compute functions are Postgres-only in substance - they
issue INSERT ... ST_Intersection statements - so nothing here touches a real
database. What is worth testing without one is the asset's own logic: which
tile it computes for, what it hands to the two compute functions and the two
readers, where the frames land, and how the return values turn into
`MaterializeResult` metadata.

The load - the three bronze parquets, the ring repair, the slug each layer is
loaded under - is `neighborhood_cadastre`'s now and is tested in
`test_cadastre`. What this asset depends on it through is the bridge from the
borough axis to the tile axis, and that is pinned here.

`postgis.py` itself has no unit test for the same reason `rag/pgvector.py`'s
`load_partition` does not - both need Postgres/PostGIS to mean anything.
"""

from __future__ import annotations

from contextlib import contextmanager

import geopandas as gpd
import pytest
from dagster import (
    DimensionPartitionMapping,
    Failure,
    IdentityPartitionMapping,
    MultiPartitionKey,
    MultiPartitionMapping,
    materialize,
)
from shapely.geometry import box

from asset_helpers import materialization_metadata

from hbu_dataplatform import building_lots_assets
from hbu_dataplatform.building_lots_assets import building_lot_intersections
from hbu_dataplatform.cadastre_assets import neighborhood_cadastre
from hbu_dataplatform.partitions import tile_scrape_partitions
from hbu_dataplatform.resources import ParquetStore, PostgisResource
from hbu_dataplatform.storage import join

DATE = "2026-08-01"
TILE = "0302303330102"
NEIGHBORHOOD = "VSMPE"
ZONE_SLUG = "Reglement_urbanisme__VSP_REG_ZONE"

#: A zoom-19 key under `TILE`, the shape every row of a tile table carries.
CELL_KEY = TILE + "23123"


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def stub_postgis(
    monkeypatch,
    *,
    intersections=1,
    buildings_matched=1,
    total_area_m2=50_000.0,
    lot_features=3,
    lots_matched=2,
    features_matched=2,
    num_lots=2,
    layers=1,
    require_raises=None,
):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    calls: dict[str, object] = {}

    @contextmanager
    def connect(self):
        yield object()

    def require_working_set(connection):
        """The real one runs `to_regclass` against a connection the stubs above
        do not have; what a test needs of it is whether it ran and what it
        raised."""
        calls["require_working_set"] = True
        if require_raises is not None:
            raise require_raises

    def compute_intersections(connection, *, tile, scrape_date):
        calls["intersections"] = (tile, scrape_date)
        return {
            "intersections": intersections,
            "buildings_matched": buildings_matched,
            "pruned": 0,
            "total_area_m2": total_area_m2,
        }

    def compute_lot_features(connection, *, tile, scrape_date):
        calls["join"] = (tile, scrape_date)
        return {
            "lot_features": lot_features,
            "pruned": 0,
            "lots_matched": lots_matched,
            "features_matched": features_matched,
            "layers": layers,
            "num_lots": num_lots,
        }

    def fetch_building_lots(connection, *, tile, scrape_date):
        """What the real one reads back out: `intersections` clipped pairs.

        Shaped like `silver.building_lot_intersections` rather than faithful
        to it - the columns the asset itself touches are the row count and
        the geometry, and the SQL behind the real function needs PostGIS to
        mean anything. The three address columns are what every tile table
        carries: the lot's cell, the tile that owns it, the borough it was
        fetched under.
        """
        calls["fetch_building_lots"] = (tile, scrape_date)
        return gpd.GeoDataFrame(
            {
                "building_uid": list(range(1, intersections + 1)),
                "lot_uid": list(range(1, intersections + 1)),
                "lot_number": [str(i) for i in range(1, intersections + 1)],
                "neighborhood": [NEIGHBORHOOD] * intersections,
                "cell_key": [CELL_KEY] * intersections,
                "cell_partition": [tile] * intersections,
                "scrape_date": [scrape_date] * intersections,
                "intersection_area_m2": [1.0] * intersections,
            },
            geometry=[box(0, 0, 1, 1)] * intersections,
            crs="EPSG:4326",
        )

    def fetch_lot_features(connection, *, tile, scrape_date):
        """What the real one reads back out: `lot_features` clipped pairs.

        Shaped like `silver.lot_features` rather than faithful to it - the SQL
        behind the real function needs PostGIS to mean anything.
        """
        calls["fetch_lot_features"] = (tile, scrape_date)
        return gpd.GeoDataFrame(
            {
                "lot_uid": list(range(1, lot_features + 1)),
                "feature_uid": list(range(1, lot_features + 1)),
                "source_table": [ZONE_SLUG] * lot_features,
                "feature_id": [f"C01-{i:03d}" for i in range(1, lot_features + 1)],
                "neighborhood": [NEIGHBORHOOD] * lot_features,
                "cell_key": [CELL_KEY] * lot_features,
                "cell_partition": [tile] * lot_features,
                "scrape_date": [scrape_date] * lot_features,
                "pct_of_lot": [50.0] * lot_features,
            },
            geometry=[box(0, 0, 1, 1)] * lot_features,
            crs="EPSG:4326",
        )

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(
        building_lots_assets, "require_working_set", require_working_set
    )
    monkeypatch.setattr(
        building_lots_assets, "compute_intersections", compute_intersections
    )
    monkeypatch.setattr(
        building_lots_assets, "compute_lot_features", compute_lot_features
    )
    monkeypatch.setattr(
        building_lots_assets, "fetch_building_lots", fetch_building_lots
    )
    monkeypatch.setattr(
        building_lots_assets, "fetch_lot_features", fetch_lot_features
    )
    return calls


def run(store):
    return materialize(
        [building_lot_intersections],
        partition_key=MultiPartitionKey({"date": DATE, "tile": TILE}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[building_lot_intersections],
    )


def test_the_joins_are_on_the_tile_axis():
    """A tile is what a computation is run over."""
    assert building_lot_intersections.partitions_def is tile_scrape_partitions


def test_the_dependency_on_the_cadastre_is_the_bridge():
    """Date to date, and every borough of it.

    A cell can hold lots from two boroughs, and which ones is a fact in
    `rag.lots` rather than in the partition key - so the borough dimension is
    left unmapped, which Dagster reads as all of them.
    """
    mapping = building_lot_intersections.get_partition_mapping(neighborhood_cadastre.key)
    assert isinstance(mapping, MultiPartitionMapping)
    assert set(mapping.downstream_mappings_by_upstream_dimension) == {"date"}
    date = mapping.downstream_mappings_by_upstream_dimension["date"]
    assert isinstance(date, DimensionPartitionMapping)
    assert date.dimension_name == "date"
    assert isinstance(date.partition_mapping, IdentityPartitionMapping)


def test_computes_both_joins_for_the_tile_and_reads_them_back(store, monkeypatch):
    calls = stub_postgis(
        monkeypatch, intersections=2, buildings_matched=1, total_area_m2=50_000.0
    )

    result = run(store)

    assert result.success
    # Computed with this partition's own key - the cell, not a borough.
    assert calls["intersections"] == (TILE, DATE)
    assert calls["join"] == (TILE, DATE)
    # Read back out of the same transaction that computed them.
    assert calls["fetch_building_lots"] == (TILE, DATE)
    assert calls["fetch_lot_features"] == (TILE, DATE)
    # Checked before any of it, so the run does not reach a join to find out.
    assert calls["require_working_set"] is True


def test_a_missing_rag_working_set_names_the_file_to_apply(store, monkeypatch):
    """The three `rag` tables are hbu_infra's, and nothing else checks them.

    `neighborhood_cadastre` checks them before it loads; this checks them
    again before it computes, because a tile run on a database that has never
    had sql/002 applied would otherwise fail inside a join as `relation
    "rag.lots" does not exist` - an identifier, with nothing about which repo
    owns it.
    """
    calls = stub_postgis(
        monkeypatch,
        require_raises=building_lots_assets.MissingRelation(
            "hbu_infra has not created: rag.lots (sql/002_spatial.sql), "
            "rag.buildings (sql/002_spatial.sql), "
            "rag.features (sql/002_spatial.sql)"
        ),
    )

    with pytest.raises(Failure, match="sql/002_spatial.sql"):
        run(store)

    # Nothing was computed: the check runs before the first join.
    assert "intersections" not in calls
    assert "join" not in calls


def test_both_joins_land_as_geoparquet_under_one_silver_partition(store, monkeypatch):
    stub_postgis(monkeypatch, intersections=2, lot_features=3)

    assert run(store).success

    output_dir = store.partition_dir(
        building_lot_intersections.key.path[-1], DATE, TILE
    )
    assert "/silver/building_lot_intersections/" in output_dir
    assert output_dir.endswith(f"/{DATE}/{TILE}")

    building_lots = gpd.read_parquet(
        join(output_dir, building_lots_assets.BUILDING_LOTS_FILE)
    )
    assert len(building_lots) == 2
    assert building_lots.crs.to_string() == "EPSG:4326"
    # The keys travel as columns, the same as everywhere else in the tree:
    # the tile that owns the row, and the borough it was fetched under.
    assert building_lots["cell_partition"].unique().tolist() == [TILE]
    assert building_lots["neighborhood"].unique().tolist() == [NEIGHBORHOOD]
    assert building_lots["scrape_date"].unique().tolist() == [DATE]

    lot_features = gpd.read_parquet(
        join(output_dir, building_lots_assets.LOT_FEATURES_FILE)
    )
    assert len(lot_features) == 3
    assert lot_features.crs.to_string() == "EPSG:4326"
    assert lot_features["cell_partition"].unique().tolist() == [TILE]
    assert lot_features["scrape_date"].unique().tolist() == [DATE]
    # The pair `rag.chunks` cites, so the file carries a key that survives a
    # reload minting new *_uid values.
    assert lot_features["source_table"].unique().tolist() == [ZONE_SLUG]


def test_a_rerun_replaces_the_previous_partition(store, monkeypatch, tmp_path):
    partition = (
        tmp_path / "store" / "silver" / "building_lot_intersections" / DATE / TILE
    )
    partition.mkdir(parents=True)
    stale = partition / "building_lots_retired.parquet"
    gpd.GeoDataFrame({"a": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_parquet(
        stale
    )
    stub_postgis(monkeypatch)

    run(store)

    assert not stale.exists()
    assert (partition / building_lots_assets.BUILDING_LOTS_FILE).exists()


def test_metadata_reports_what_was_matched(store, monkeypatch):
    stub_postgis(
        monkeypatch,
        intersections=2,
        buildings_matched=1,
        total_area_m2=50_000.0,
        lot_features=3,
        lots_matched=1,
        features_matched=2,
        num_lots=2,
        layers=1,
    )

    result = run(store)

    metadata = materialization_metadata(result, building_lot_intersections)
    assert metadata["tile"].value == TILE
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_lots"].value == 2
    assert metadata["num_intersections"].value == 2
    assert metadata["num_buildings_matched"].value == 1
    assert metadata["total_intersection_area_ha"].value == pytest.approx(5.0)
    assert metadata["num_layers"].value == 1
    assert metadata["num_lot_features"].value == 3
    assert metadata["num_lots_matched"].value == 1
    # The symptom worth seeing: a lot no feature covers at all.
    assert metadata["num_lots_uncovered"].value == 1
    assert metadata["num_building_lot_rows_written"].value == 2
    assert metadata["num_lot_feature_rows_written"].value == 3
