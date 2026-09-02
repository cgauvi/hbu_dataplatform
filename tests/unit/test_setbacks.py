"""Offline test for the `lot_buildable_setbacks` asset.

`postgis.compute_lot_buildable_setbacks` is Postgres-only in substance - it
sorts a boundary by the angle it runs at and differences four buffers out of a
polygon - so nothing here touches a real database, the same posture
`test_frontage.py` takes towards the join it covers.

What is worth testing without one is the asset's own logic: that it loads
nothing (all three inputs are already in Postgres, put there by the assets it
depends on), that the configured tolerance reaches the SQL and the metadata,
how the compute function's return value becomes `MaterializeResult` metadata,
and the two gaps it has to tell apart and warn about rather than swallow - a
partition whose cadastre never landed, and one whose envelopes never did.

The statement itself is covered by `test_postgis_setbacks.py` for the things a
fake cursor can see, and by `tests/integration/test_lot_setbacks.py` for the
geometry, which needs a real PostGIS.
"""

from __future__ import annotations

from contextlib import contextmanager

import geopandas as gpd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import Polygon

from asset_helpers import materialization_metadata

from urban_rag.postgis import DEFAULT_SETBACK_EDGE_TOLERANCE_M
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.setback_assets import LOT_SETBACKS_FILE, lot_buildable_setbacks

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def stub_postgis(
    monkeypatch,
    *,
    rows=6,
    num_lots=10,
    lots_sorted=8,
    lots_measured=8,
    num_envelopes=14,
    lots_with_envelopes=9,
    bound_by_setbacks=4,
    bound_by_coverage=2,
    num_unbuildable=1,
    total_buildable_area_m2=4_800.0,
    mean_buildable_pct=41.5,
    by_side_rule=None,
):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    calls: dict[str, object] = {}

    @contextmanager
    def connect(self):
        yield object()

    def compute_lot_buildable_setbacks(
        connection, *, neighborhood, scrape_date, edge_tolerance_m
    ):
        calls["compute"] = (neighborhood, scrape_date, edge_tolerance_m)
        return {
            "rows": rows,
            "pruned": 0,
            "num_lots": num_lots,
            "num_lots_sorted": lots_sorted,
            "num_lots_measured": lots_measured,
            "lots_without_frontage": num_lots - lots_sorted,
            "num_envelopes": num_envelopes,
            "num_lots_with_envelopes": lots_with_envelopes,
            "num_bound_by_setbacks": bound_by_setbacks,
            "num_bound_by_site_coverage": bound_by_coverage,
            "num_unbuildable": num_unbuildable,
            "total_buildable_area_m2": total_buildable_area_m2,
            "mean_buildable_pct_of_lot": mean_buildable_pct,
            "by_side_setback_rule": (
                {"contigu": 9, "isole": 5} if by_side_rule is None else by_side_rule
            ),
            "edge_tolerance_m": edge_tolerance_m,
            "max_sin": 0.7071,
            "segment_m": 1.0,
        }

    def fetch_lot_buildable_setbacks(connection, *, neighborhood, scrape_date):
        """What the real one reads back: `rows` buildable envelopes.

        Shaped like `silver.lot_buildable_setbacks` rather than faithful to it -
        what the asset itself touches is the row count and the geometry.
        """
        calls["fetch"] = (neighborhood, scrape_date)
        return gpd.GeoDataFrame(
            {
                "lot_uid": list(range(1, rows + 1)),
                "lot_number": [str(index) for index in range(1, rows + 1)],
                "neighborhood": [neighborhood] * rows,
                "scrape_date": [scrape_date] * rows,
                "feature_id": ["H01-001"] * rows,
                "column_index": [0] * rows,
                "buildable_area_m2": [200.0 - index for index in range(rows)],
                "footprint_cap_m2": [180.0 - index for index in range(rows)],
                "footprint_cap_binding": ["setbacks"] * rows,
                "side_setback_rule": ["contigu"] * rows,
            },
            geometry=[
                Polygon([(0, 0), (0.0001, 0), (0.0001, 0.0001), (0, 0.0001)])
            ]
            * rows,
            crs="EPSG:4326",
        )

    monkeypatch.setattr(PostgisResource, "connect", connect)
    for name, value in {
        "compute_lot_buildable_setbacks": compute_lot_buildable_setbacks,
        "fetch_lot_buildable_setbacks": fetch_lot_buildable_setbacks,
    }.items():
        monkeypatch.setattr(f"urban_rag.setback_assets.{name}", value)
    return calls


def materialize_partition(store, *, run_config=None):
    return materialize(
        [lot_buildable_setbacks],
        partition_key=MultiPartitionKey(
            {"date": DATE, "neighborhood": NEIGHBORHOOD}
        ),
        resources={"store": store, "postgis": PostgisResource()},
        run_config=run_config,
    )


def config_for(edge_tolerance_m: float) -> dict:
    return {
        "ops": {
            "silver__lot_buildable_setbacks": {
                "config": {"edge_tolerance_m": edge_tolerance_m}
            }
        }
    }


def test_the_partition_is_measured_and_written(store, monkeypatch, tmp_path):
    calls = stub_postgis(monkeypatch)

    assert materialize_partition(store).success

    assert calls["compute"][:2] == (NEIGHBORHOOD, DATE)
    assert calls["fetch"] == (NEIGHBORHOOD, DATE)

    path = (
        tmp_path / "store" / "silver" / "lot_buildable_setbacks" / DATE
        / NEIGHBORHOOD / LOT_SETBACKS_FILE
    )
    frame = gpd.read_parquet(path)
    assert len(frame) == 6
    assert frame.crs.to_string() == "EPSG:4326"


def test_the_asset_loads_nothing(store, monkeypatch):
    """All three inputs are already in Postgres when this runs, put there by
    the assets it depends on. Loading any of them here would be the race
    `building_lots_assets` describes, from a third direction."""
    calls = stub_postgis(monkeypatch)

    assert materialize_partition(store).success

    assert set(calls) == {"compute", "fetch"}


def test_the_configured_tolerance_reaches_the_query_and_the_metadata(
    store, monkeypatch
):
    stub_postgis(monkeypatch)

    result = materialize_partition(store, run_config=config_for(0.5))

    metadata = materialization_metadata(result, lot_buildable_setbacks)
    assert metadata["edge_tolerance_m"].value == 0.5


def test_the_default_tolerance_is_the_shipped_constant(store, monkeypatch):
    calls = stub_postgis(monkeypatch)

    assert materialize_partition(store).success

    assert calls["compute"][2] == DEFAULT_SETBACK_EDGE_TOLERANCE_M


def test_the_metadata_carries_which_norm_shapes_the_borough(store, monkeypatch):
    """The headline this asset exists to produce: whether the margins or the
    coverage decide what gets built here."""
    stub_postgis(monkeypatch, rows=6, bound_by_setbacks=4, bound_by_coverage=2)

    result = materialize_partition(store)

    metadata = materialization_metadata(result, lot_buildable_setbacks)
    assert metadata["num_bound_by_setbacks"].value == 4
    assert metadata["num_bound_by_site_coverage"].value == 2
    assert metadata["pct_bound_by_setbacks"].value == pytest.approx(66.7)


def test_the_two_gaps_are_reported_separately(store, monkeypatch):
    """A lot missing from the sort had no frontage row; one missing from the
    measure was sorted and had no grid to subtract. Different fixes."""
    stub_postgis(
        monkeypatch,
        num_lots=100,
        lots_sorted=80,
        lots_measured=70,
        lots_with_envelopes=90,
    )

    result = materialize_partition(store)

    metadata = materialization_metadata(result, lot_buildable_setbacks)
    assert metadata["num_lots"].value == 100
    assert metadata["num_lots_sorted"].value == 80
    assert metadata["num_lots_without_frontage"].value == 20
    assert metadata["num_lots_measured"].value == 70
    assert metadata["num_lots_with_envelopes"].value == 90


def test_a_lot_with_nowhere_to_build_is_a_number_not_a_gap(store, monkeypatch):
    """A parcel narrower than twice its side margin has a buildable area of
    zero, which is an answer. It must not read as "not measured"."""
    stub_postgis(monkeypatch, num_unbuildable=3)

    result = materialize_partition(store)

    metadata = materialization_metadata(result, lot_buildable_setbacks)
    assert metadata["num_unbuildable"].value == 3


def test_a_partition_whose_cadastre_never_landed_fails_naming_what_to_run(
    store, monkeypatch
):
    stub_postgis(monkeypatch, num_lots=0)

    with pytest.raises(Failure, match="rag.lots holds no lot"):
        materialize_partition(store)


def test_a_partition_whose_envelopes_never_landed_says_so_separately(
    store, monkeypatch
):
    """A different gap from the one above, with a different fix, so it is named
    rather than left to surface as a table of zero rows."""
    stub_postgis(monkeypatch, num_envelopes=0, lots_with_envelopes=0)

    with pytest.raises(Failure, match="materialize lot_zoning_envelopes"):
        materialize_partition(store)


def test_a_borough_where_no_column_read_as_contiguous_is_warned_about(
    store, monkeypatch
):
    """VSMPE's grids print I-J and I-J-C throughout, so every column reading as
    isolated is a mode that failed to parse - and would quietly shrink every
    envelope in the borough rather than fail."""
    stub_postgis(monkeypatch, by_side_rule={"isole": 9, "unknown": 5})

    result = materialize_partition(store)

    assert result.success
    metadata = materialization_metadata(result, lot_buildable_setbacks)
    assert metadata["by_side_setback_rule"].data == {"isole": 9, "unknown": 5}


def test_a_borough_reading_mostly_contiguous_is_not_warned_about(
    store, monkeypatch
):
    stub_postgis(monkeypatch, by_side_rule={"contigu": 12, "isole": 2})

    result = materialize_partition(store)

    assert result.success
    metadata = materialization_metadata(result, lot_buildable_setbacks)
    assert metadata["by_side_setback_rule"].data == {"contigu": 12, "isole": 2}


def test_a_previous_run_is_cleared_before_the_new_file_lands(
    store, monkeypatch, tmp_path
):
    stub_postgis(monkeypatch, rows=6)
    assert materialize_partition(store).success

    stub_postgis(monkeypatch, rows=2)
    assert materialize_partition(store).success

    output_dir = (
        tmp_path / "store" / "silver" / "lot_buildable_setbacks" / DATE
        / NEIGHBORHOOD
    )
    files = sorted(path.name for path in output_dir.glob("*.parquet"))
    assert files == [LOT_SETBACKS_FILE]
    assert len(gpd.read_parquet(output_dir / LOT_SETBACKS_FILE)) == 2
