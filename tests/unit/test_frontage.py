"""Offline test for `lot_frontage`.

`urban_rag.postgis.load_streets`/`compute_lot_frontage` are Postgres-only in
substance - they issue COPY and INSERT ... ST_Intersection statements against
a buffered `geography` - so nothing here touches a real database, the same
posture `test_building_lots.py` takes for the two joins it covers.

What is worth testing without one is the asset's own logic: which partition it
reads, that it loads `rag.streets` and *only* `rag.streets` (the cadastre is
`building_lot_intersections`'s to load, and loading it twice is the race that
asset's docstring describes), that the configured buffer reaches the SQL and
the row it produced, and how the compute function's return value turns into
`MaterializeResult` metadata.

The SQL itself is covered by neither this nor `postgis.py`'s own tests, for the
same reason `rag/pgvector.py`'s `load_partition` has none: PostGIS is what
gives it meaning.
"""

from __future__ import annotations

from contextlib import contextmanager

import geopandas as gpd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import LineString

from asset_helpers import materialization_metadata

from urban_rag.frames import write_frame
from urban_rag.frontage_assets import LOT_FRONTAGE_FILE, lot_frontage
from urban_rag.postgis import DEFAULT_FRONTAGE_BUFFER_M
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import join
from urban_rag.street_assets import STREETS_FILE_OUT, neighborhood_streets

DATE = "2026-08-24"
NEIGHBORHOOD = "VSMPE"


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_streets(store, *, street_ids=(1, 2), names=("Jarry", "Papineau")):
    """One borough's street sides, as `neighborhood_streets` writes them."""
    frame = gpd.GeoDataFrame(
        {
            "COTE_RUE_ID": list(street_ids),
            "NOM_VOIE": list(names),
            "neighborhood": [NEIGHBORHOOD] * len(street_ids),
            "scrape_date": [DATE] * len(street_ids),
            "length_in_borough_m": [100.0] * len(street_ids),
        },
        geometry=[
            LineString([(-73.628, 45.545 + index / 1000), (-73.624, 45.545 + index / 1000)])
            for index in range(len(street_ids))
        ],
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(
                neighborhood_streets.key.path[-1], DATE, NEIGHBORHOOD
            ),
            STREETS_FILE_OUT,
        ),
    )


def stub_postgis(
    monkeypatch,
    *,
    frontages=3,
    lots_matched=2,
    streets_matched=2,
    num_lots=4,
    num_streets=2,
    total_frontage_m=60.0,
    max_frontage_m=30.0,
    lots_without_frontage=("3 790 556", "3 790 557"),
):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    calls: dict[str, object] = {}

    @contextmanager
    def connect(self):
        yield object()

    def compute_lot_frontage(connection, *, neighborhood, scrape_date, buffer_m):
        calls["compute"] = (neighborhood, scrape_date, buffer_m)
        return {
            "frontages": frontages,
            "lots_matched": lots_matched,
            "streets_matched": streets_matched,
            "total_frontage_m": total_frontage_m,
            "max_frontage_m": max_frontage_m,
            "num_lots": num_lots,
            "num_streets": num_streets,
            # A sample of the lots that faced nothing, as the real one returns
            # it - the count is num_lots - lots_matched, this is which ones.
            "lots_without_frontage": list(lots_without_frontage),
            "buffer_m": buffer_m,
            "pruned": 0,
        }

    def fetch_lot_frontage(connection, *, neighborhood, scrape_date):
        """What the real one reads back out: `frontages` measured pairs.

        Shaped like `rag.lot_frontage` rather than faithful to it - the columns
        the asset itself touches are the row count and the geometry, and the
        SQL behind the real function needs PostGIS to mean anything.
        """
        calls["fetch"] = (neighborhood, scrape_date)
        return gpd.GeoDataFrame(
            {
                "lot_uid": list(range(1, frontages + 1)),
                "lot_number": [str(i) for i in range(1, frontages + 1)],
                "cote_rue_id": ["1"] * frontages,
                "street_name": ["Jarry"] * frontages,
                "neighborhood": [neighborhood] * frontages,
                "scrape_date": [scrape_date] * frontages,
                "buffer_m": [calls["compute"][2]] * frontages,
                # Descending, the way the real ORDER BY returns them.
                "frontage_m": [30.0 - i for i in range(frontages)],
                "frontage_rank": [1] * frontages,
            },
            geometry=[LineString([(0, 0), (0.0001, 0)])] * frontages,
            crs="EPSG:4326",
        )

    monkeypatch.setattr(PostgisResource, "connect", connect)
    for name, value in {
        "compute_lot_frontage": compute_lot_frontage,
        "fetch_lot_frontage": fetch_lot_frontage,
    }.items():
        monkeypatch.setattr(f"urban_rag.frontage_assets.{name}", value)
    return calls


def materialize_partition(store, *, run_config=None):
    return materialize(
        [lot_frontage],
        partition_key=MultiPartitionKey(
            {"date": DATE, "neighborhood": NEIGHBORHOOD}
        ),
        resources={"store": store, "postgis": PostgisResource()},
        run_config=run_config,
    )


def config_for(buffer_m: float) -> dict:
    return {"ops": {"silver__lot_frontage": {"config": {"buffer_m": buffer_m}}}}


def test_the_partition_is_loaded_measured_and_written(store, monkeypatch, tmp_path):
    calls = stub_postgis(monkeypatch)
    write_streets(store)

    assert materialize_partition(store).success

    assert calls["compute"][:2] == (NEIGHBORHOOD, DATE)
    assert calls["fetch"] == (NEIGHBORHOOD, DATE)

    path = (
        tmp_path / "store" / "silver" / "lot_frontage" / DATE / NEIGHBORHOOD
        / LOT_FRONTAGE_FILE
    )
    frame = gpd.read_parquet(path)
    assert len(frame) == 3
    assert frame.crs.to_string() == "EPSG:4326"


def test_nothing_is_loaded_here(store, monkeypatch):
    """Both sides of the join are already in Postgres when this runs.

    `rag.lots` belongs to `building_lot_intersections` and
    `silver.neighborhood_streets` to `neighborhood_streets`. Two assets loading
    the same table from the same file in two transactions is a race, not a
    redundancy - whoever commits second replaces the rows the first computed
    against."""
    calls = stub_postgis(monkeypatch)
    write_streets(store)

    materialize_partition(store)

    assert set(calls) == {"compute", "fetch"}


def test_the_written_rows_are_ordered_longest_frontage_first(
    store, monkeypatch, tmp_path
):
    """Ordered in SQL rather than left to the reader, so the parquet answers
    "which lots have the most street" by being read from the top."""
    stub_postgis(monkeypatch, frontages=4)
    write_streets(store)

    materialize_partition(store)

    frame = gpd.read_parquet(
        tmp_path / "store" / "silver" / "lot_frontage" / DATE / NEIGHBORHOOD
        / LOT_FRONTAGE_FILE
    )
    assert frame["frontage_m"].is_monotonic_decreasing


def test_the_default_buffer_is_the_one_postgis_declares(store, monkeypatch):
    calls = stub_postgis(monkeypatch)
    write_streets(store)

    materialize_partition(store)

    assert calls["compute"][2] == DEFAULT_FRONTAGE_BUFFER_M


def test_a_configured_buffer_reaches_the_query_and_the_rows(
    store, monkeypatch, tmp_path
):
    """It is a judgement about the street section, not a property of the data,
    so it is config - and it travels on every row, so a table can be read back
    against the cutoff that produced it."""
    calls = stub_postgis(monkeypatch)
    write_streets(store)

    materialize_partition(store, run_config=config_for(5.0))

    assert calls["compute"][2] == 5.0
    frame = gpd.read_parquet(
        tmp_path / "store" / "silver" / "lot_frontage" / DATE / NEIGHBORHOOD
        / LOT_FRONTAGE_FILE
    )
    assert set(frame["buffer_m"]) == {5.0}


def test_a_buffer_of_zero_is_refused(store, monkeypatch):
    """Every lot boundary would have to fall exactly on the curb line, which
    would report no frontage anywhere rather than an unbuffered measurement."""
    stub_postgis(monkeypatch)
    write_streets(store)

    with pytest.raises(Exception):
        materialize_partition(store, run_config=config_for(0.0))


def test_metadata_reports_the_coverage_and_the_cutoff(store, monkeypatch):
    stub_postgis(
        monkeypatch, frontages=3, lots_matched=2, num_lots=4, total_frontage_m=60.0
    )
    write_streets(store)

    metadata = materialization_metadata(materialize_partition(store), lot_frontage)

    assert metadata["dagster/row_count"].value == 3
    assert metadata["num_frontages"].value == 3
    assert metadata["num_streets"].value == 2
    assert metadata["num_lots"].value == 4
    assert metadata["num_lots_with_frontage"].value == 2
    # The symptom worth seeing: a lot facing nothing is either a true interior
    # parcel or a street snapshot that stops short of it.
    assert metadata["num_lots_without_frontage"].value == 2
    assert metadata["mean_frontage_m"].value == 30.0
    assert metadata["buffer_m"].value == DEFAULT_FRONTAGE_BUFFER_M


def test_a_borough_with_no_lots_loaded_is_a_failure(store, monkeypatch):
    """Not "no lot faces a street": no lots at all means the cadastre was never
    loaded, and a perfectly well-formed zero would hide that."""
    stub_postgis(monkeypatch, num_lots=0)
    write_streets(store)

    with pytest.raises(Failure, match="materialize building_lot_intersections"):
        materialize_partition(store)


def test_no_lot_matching_is_not_a_failure(store, monkeypatch):
    """Unlike the above. A borough whose lots all sit back from every curb is a
    number to look at, not a partition to refuse."""
    stub_postgis(monkeypatch, frontages=0, lots_matched=0, num_lots=4)
    write_streets(store)

    result = materialize_partition(store)

    assert result.success
    metadata = materialization_metadata(result, lot_frontage)
    assert metadata["num_lots_without_frontage"].value == 4
    assert metadata["mean_frontage_m"].value == 0.0


def test_the_lots_that_face_no_street_are_named_not_only_counted(store, monkeypatch):
    """"Two lots failed" is not something anyone can act on.

    A lot with no frontage is either a real interior parcel or the measure not
    reaching it - the second is what a cutoff below the borough's setback looks
    like, and it is diagnosed by pulling up the lots, so the run publishes
    which ones alongside how many."""
    stub_postgis(
        monkeypatch,
        lots_matched=2,
        num_lots=4,
        lots_without_frontage=("3 790 556", "3 790 557"),
    )
    write_streets(store)

    metadata = materialization_metadata(materialize_partition(store), lot_frontage)

    assert metadata["num_lots_without_frontage"].value == 2
    assert metadata["pct_lots_without_frontage"].value == 50.0
    assert metadata["lots_without_frontage"].value == "3 790 556, 3 790 557"


def test_a_borough_where_every_lot_faces_a_street_flags_nothing(store, monkeypatch):
    """The expected shape of a healthy partition, and the one the fixture in
    tests/integration measures: every lot in it has street on a side."""
    stub_postgis(
        monkeypatch, lots_matched=4, num_lots=4, lots_without_frontage=()
    )
    write_streets(store)

    metadata = materialization_metadata(materialize_partition(store), lot_frontage)

    assert metadata["num_lots_without_frontage"].value == 0
    assert metadata["pct_lots_without_frontage"].value == 0.0
    assert metadata["lots_without_frontage"].value == ""


def test_a_missing_street_partition_names_the_asset_to_run(store, monkeypatch):
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="materialize neighborhood_streets"):
        materialize_partition(store)


def test_a_rerun_replaces_the_previous_partition(store, monkeypatch, tmp_path):
    stub_postgis(monkeypatch)
    write_streets(store)
    partition = tmp_path / "store" / "silver" / "lot_frontage" / DATE / NEIGHBORHOOD
    partition.mkdir(parents=True)
    stale = partition / "lot_frontage_retired.parquet"
    gpd.GeoDataFrame(
        {"a": [1]}, geometry=[LineString([(0, 0), (1, 1)])], crs="EPSG:4326"
    ).to_parquet(stale)

    materialize_partition(store)

    assert not stale.exists()
    assert (partition / LOT_FRONTAGE_FILE).exists()
