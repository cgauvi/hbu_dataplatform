"""Offline test for `building_lot_intersections`, both joins it computes.

`urban_rag.postgis`'s load/compute functions are Postgres-only in substance -
they issue COPY and INSERT ... ST_Intersection statements - so nothing here
touches a real database. What is worth testing without one is the asset's own
logic: which partitions it reads, what it hands to
`postgis.load_lots`/`load_buildings`/`load_features` and the two compute
functions, and how their return values turn into `MaterializeResult` metadata.

On the feature half that is more than plumbing: which of a borough's two dozen
parquet files it decides to load, under which `source_table` value, and which
it skips. Loading a layer under the wrong name is the failure this half exists
to avoid, and it is silent - the join simply matches nothing.

The silver contract over the cadastre is tested here too, since this is the
asset that now keeps it: `neighborhood_lots` is bronze and writes
self-intersecting rings through, and what reads them is `ST_Intersection`
twice over. An invalid ring gives a wrong answer rather than an error, and a
duplicated lot number multiplies every pair both joins produce - so both are
caught before the load rather than after it.

`postgis.py` itself has no unit test for the same reason `rag/pgvector.py`'s
`load_partition` does not - both need Postgres/PostGIS to mean anything.
"""

from __future__ import annotations

from contextlib import contextmanager

import geopandas as gpd
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import Point, Polygon, box

from asset_helpers import materialization_metadata

from urban_rag import building_lots_assets
from urban_rag.assets import neighborhood_features
from urban_rag.bdoi_assets import BUILDINGS_FILE, neighborhood_buildings
from urban_rag.building_lots_assets import building_lot_intersections
from urban_rag.frames import write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
ZONE_SLUG = "Reglement_urbanisme__VSP_REG_ZONE"

#: A bowtie: the self-intersecting ring shapely rejects and Infolot publishes a
#: handful of per borough. `make_valid` turns it into a valid MultiPolygon.
BOWTIE = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_lots(store, *, lot_numbers=("1", "2"), geometries=None):
    """The cadastre as `neighborhood_lots` writes it - bronze, unrepaired."""
    path = join(
        store.partition_dir(neighborhood_lots.key.path[-1], DATE, NEIGHBORHOOD),
        LOTS_FILE,
    )
    frame = gpd.GeoDataFrame(
        {
            "NO_LOT": list(lot_numbers),
            "neighborhood": [NEIGHBORHOOD] * len(lot_numbers),
            "scrape_date": [DATE] * len(lot_numbers),
        },
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


def features_dir(store):
    return store.partition_dir(neighborhood_features.key.path[-1], DATE, NEIGHBORHOOD)


def write_zones(store, *, slug=ZONE_SLUG):
    """A zoning layer as `neighborhood_features` writes it.

    `source_table` holds the Spectrum path, not the slug - the discrepancy the
    asset has to get right.
    """
    frame = gpd.GeoDataFrame(
        {
            "NUMERO_COMPLET": ["C01-001", "C01-002"],
            "LIEN_GRILLE": ["http://example/C01-001.pdf", "http://example/C01-002.pdf"],
            "source_table": [f"/19_{NEIGHBORHOOD}/Reglement_urbanisme/VSP_REG_ZONE"] * 2,
        },
        geometry=[box(0, 0, 1.5, 1), box(1.5, 0, 2, 1)],
        crs="EPSG:4326",
    )
    write_frame(frame, join(features_dir(store), f"{slug}.parquet"))


def write_layer_without_id(store, *, slug="Ruelle_verte__VSP_TP_RUELLE_VERTE"):
    frame = gpd.GeoDataFrame(
        {"NOM": ["a"]}, geometry=[Point(0.5, 0.5)], crs="EPSG:4326"
    )
    write_frame(frame, join(features_dir(store), f"{slug}.parquet"))


def write_layer_without_geometry(store, *, slug="Mairie__VSMPE_Mairie_1"):
    frame = pd.DataFrame({"NUMERO_COMPLET": ["x"], "NOM": ["a"]})
    write_frame(frame, join(features_dir(store), f"{slug}.parquet"))


def write_partition(store):
    """Every input of one partition, in its default shape."""
    write_lots(store)
    write_buildings(store)
    write_zones(store)


def stub_postgis(
    monkeypatch,
    *,
    intersections=1,
    buildings_matched=1,
    total_area_m2=50_000.0,
    lot_features=3,
    lots_matched=2,
    features_matched=2,
    require_raises=None,
):
    """Patched on the class: Dagster rebuilds the resource before the run."""
    calls: dict[str, object] = {"features": [], "lot_loads": []}

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

    def load_lots(connection, frame, *, neighborhood, scrape_date):
        calls["lots"] = (neighborhood, scrape_date, len(frame))
        # Kept whole, not just counted: what the asset repaired on the way in
        # is only visible in the frame it handed over.
        calls["loaded_lots"] = frame
        # Once per run is the point of the merge, so the count is kept too.
        calls["lot_loads"].append((neighborhood, scrape_date))
        return len(frame)

    def load_buildings(connection, frame, *, neighborhood, scrape_date):
        calls["buildings"] = (neighborhood, scrape_date, len(frame))
        return len(frame)

    def load_features(
        connection, frame, *, neighborhood, scrape_date, source_table, feature_id_column
    ):
        calls["features"].append((source_table, feature_id_column, len(frame)))
        return len(frame)

    def compute_intersections(connection, *, neighborhood, scrape_date):
        calls["intersections"] = (neighborhood, scrape_date)
        return {
            "intersections": intersections,
            "buildings_matched": buildings_matched,
            "pruned": 0,
            "total_area_m2": total_area_m2,
        }

    def compute_lot_features(connection, *, neighborhood, scrape_date):
        calls["join"] = (neighborhood, scrape_date)
        return {
            "lot_features": lot_features,
            "pruned": 0,
            "lots_matched": lots_matched,
            "features_matched": features_matched,
            "layers": 1,
            "num_lots": 2,
        }

    def fetch_building_lots(connection, *, neighborhood, scrape_date):
        """What the real one reads back out: `intersections` clipped pairs.

        Shaped like `rag.building_lots` rather than faithful to it - the
        columns the asset itself touches are the row count and the geometry,
        and the SQL behind the real function needs PostGIS to mean anything.
        """
        calls["fetch_building_lots"] = (neighborhood, scrape_date)
        return gpd.GeoDataFrame(
            {
                "building_uid": list(range(1, intersections + 1)),
                "lot_uid": list(range(1, intersections + 1)),
                "lot_number": [str(i) for i in range(1, intersections + 1)],
                "neighborhood": [neighborhood] * intersections,
                "scrape_date": [scrape_date] * intersections,
                "intersection_area_m2": [1.0] * intersections,
            },
            geometry=[box(0, 0, 1, 1)] * intersections,
            crs="EPSG:4326",
        )

    def fetch_lot_features(connection, *, neighborhood, scrape_date):
        """What the real one reads back out: `lot_features` clipped pairs.

        Shaped like `rag.lot_features` rather than faithful to it - the SQL
        behind the real function needs PostGIS to mean anything.
        """
        calls["fetch_lot_features"] = (neighborhood, scrape_date)
        return gpd.GeoDataFrame(
            {
                "lot_uid": list(range(1, lot_features + 1)),
                "feature_uid": list(range(1, lot_features + 1)),
                "source_table": [ZONE_SLUG] * lot_features,
                "feature_id": [f"C01-{i:03d}" for i in range(1, lot_features + 1)],
                "neighborhood": [neighborhood] * lot_features,
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
    monkeypatch.setattr(building_lots_assets, "load_lots", load_lots)
    monkeypatch.setattr(building_lots_assets, "load_buildings", load_buildings)
    monkeypatch.setattr(building_lots_assets, "load_features", load_features)
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
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[building_lot_intersections],
    )


def test_loads_all_three_partitions_then_computes_both_joins(store, monkeypatch):
    write_partition(store)
    calls = stub_postgis(
        monkeypatch, intersections=2, buildings_matched=1, total_area_m2=50_000.0
    )

    result = run(store)

    assert result.success
    # Loaded before the joins were computed, and with this partition's own key.
    assert calls["lots"] == (NEIGHBORHOOD, DATE, 2)
    assert calls["buildings"] == (NEIGHBORHOOD, DATE, 1)
    assert calls["features"] == [(ZONE_SLUG, "NUMERO_COMPLET", 2)]
    assert calls["intersections"] == (NEIGHBORHOOD, DATE)
    assert calls["join"] == (NEIGHBORHOOD, DATE)
    # Read back out of the same transaction that computed them.
    assert calls["fetch_building_lots"] == (NEIGHBORHOOD, DATE)
    assert calls["fetch_lot_features"] == (NEIGHBORHOOD, DATE)
    # Checked before any of it, so the run does not reach a COPY to find out.
    assert calls["require_working_set"] is True


def test_a_missing_rag_working_set_names_the_file_to_apply(store, monkeypatch):
    """The three `rag` tables are hbu_infra's, and nothing else checks them.

    Every silver and gold write goes through `urban_rag.warehouse`, which
    checks its own target; `rag.lots`/`rag.buildings`/`rag.features` are loaded
    by raw DELETE/COPY/INSERT, so without the preflight a database that has
    never had sql/002 applied fails as `relation "rag.lots" does not exist` -
    an identifier, with nothing about which repo owns it.
    """
    write_partition(store)
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

    # Nothing was loaded: the check runs before the first DELETE, so a
    # partition on a database with no working set costs no write at all.
    assert "lots" not in calls
    assert "buildings" not in calls
    assert calls["features"] == []


def test_the_lots_are_loaded_once_for_both_joins(store, monkeypatch):
    """The reason the two joins share an asset.

    Split across two assets they loaded `rag.lots` twice per partition, from
    the same file, in transactions that raced each other for the table.
    """
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    assert calls["lot_loads"] == [(NEIGHBORHOOD, DATE)]
    # And both joins were computed against that one load.
    assert calls["intersections"] == (NEIGHBORHOOD, DATE)
    assert calls["join"] == (NEIGHBORHOOD, DATE)


def test_both_joins_land_as_geoparquet_under_one_silver_partition(store, monkeypatch):
    write_partition(store)
    stub_postgis(monkeypatch, intersections=2, lot_features=3)

    assert run(store).success

    output_dir = store.partition_dir(
        building_lot_intersections.key.path[-1], DATE, NEIGHBORHOOD
    )
    assert "/silver/building_lot_intersections/" in output_dir

    building_lots = gpd.read_parquet(
        join(output_dir, building_lots_assets.BUILDING_LOTS_FILE)
    )
    assert len(building_lots) == 2
    assert building_lots.crs.to_string() == "EPSG:4326"
    # The keys travel as columns, the same as everywhere else in the tree.
    assert building_lots["neighborhood"].unique().tolist() == [NEIGHBORHOOD]
    assert building_lots["scrape_date"].unique().tolist() == [DATE]

    lot_features = gpd.read_parquet(
        join(output_dir, building_lots_assets.LOT_FEATURES_FILE)
    )
    assert len(lot_features) == 3
    assert lot_features.crs.to_string() == "EPSG:4326"
    assert lot_features["neighborhood"].unique().tolist() == [NEIGHBORHOOD]
    assert lot_features["scrape_date"].unique().tolist() == [DATE]
    # The pair `rag.chunks` cites, so the file carries a key that survives a
    # reload minting new *_uid values.
    assert lot_features["source_table"].unique().tolist() == [ZONE_SLUG]


def test_features_are_loaded_under_the_slug_not_the_spectrum_path(store, monkeypatch):
    """The one that silently breaks everything downstream if it regresses.

    `rag.chunks.source_table` holds the slug, and every join from a feature to
    the corpus matches the two columns to each other - so a layer loaded under
    `/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE` would join to nothing at all.
    """
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    run(store)

    assert calls["features"] == [(ZONE_SLUG, "NUMERO_COMPLET", 2)]


def test_layers_without_an_id_or_geometry_are_skipped_not_loaded(store, monkeypatch):
    write_partition(store)
    write_layer_without_id(store)
    write_layer_without_geometry(store)
    calls = stub_postgis(monkeypatch)

    result = run(store)

    assert [slug for slug, _, _ in calls["features"]] == [ZONE_SLUG]
    metadata = materialization_metadata(result, building_lot_intersections)
    assert metadata["num_layers_loaded"].value == 1
    assert metadata["num_layers_skipped"].value == 2
    assert set(metadata["skipped_layers"].data) == {
        "Ruelle_verte__VSP_TP_RUELLE_VERTE",
        "Mairie__VSMPE_Mairie_1",
    }


def test_metadata_reports_what_was_loaded_and_matched(store, monkeypatch):
    write_partition(store)
    stub_postgis(
        monkeypatch,
        intersections=2,
        buildings_matched=1,
        total_area_m2=50_000.0,
        lot_features=3,
        lots_matched=1,
        features_matched=2,
    )

    result = run(store)

    metadata = materialization_metadata(result, building_lot_intersections)
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_lots"].value == 2
    assert metadata["num_buildings"].value == 1
    assert metadata["num_intersections"].value == 2
    assert metadata["num_buildings_matched"].value == 1
    assert metadata["num_buildings_unmatched"].value == 0
    assert metadata["total_intersection_area_ha"].value == pytest.approx(5.0)
    assert metadata["num_features"].value == 2
    assert metadata["num_lot_features"].value == 3
    assert metadata["num_lots_matched"].value == 1
    # The symptom worth seeing: a lot no feature covers at all.
    assert metadata["num_lots_uncovered"].value == 1
    assert metadata["features_per_layer"].data == {ZONE_SLUG: 2}


def test_no_lot_in_the_partition_fails_with_what_it_means(store, monkeypatch):
    write_partition(store)
    write_lots(store, lot_numbers=(), geometries=[])
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="holds no lot"):
        run(store)


def test_no_building_in_the_partition_fails_with_what_it_means(store, monkeypatch):
    write_partition(store)
    write_buildings(store, link_ids=(), geometries=[])
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="holds no building"):
        run(store)


def test_no_feature_parquet_at_all_fails_with_what_it_means(store, monkeypatch):
    write_lots(store)
    write_buildings(store)
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="holds no feature parquet"):
        run(store)


def test_only_unloadable_layers_fails_rather_than_computing_an_empty_join(
    store, monkeypatch
):
    write_lots(store)
    write_buildings(store)
    write_layer_without_id(store)
    calls = stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="no feature could be loaded"):
        run(store)

    # It fails before either join rather than computing an empty one, and
    # inside the transaction, so the lots and buildings it did load are rolled
    # back with it.
    assert "intersections" not in calls
    assert "join" not in calls


def test_an_invalid_ring_is_repaired_before_the_lots_are_loaded(store, monkeypatch):
    """The bronze/silver line for geometry.

    `neighborhood_lots` counts these and writes them as they came; this asset
    is where they get fixed, because what reads them is `ST_Intersection`.
    """
    write_partition(store)
    write_lots(store, geometries=[BOWTIE, box(1, 0, 2, 1)])
    calls = stub_postgis(monkeypatch)

    result = run(store)

    assert result.success
    loaded = calls["loaded_lots"]
    assert loaded.geometry.is_valid.all()
    # Still one row per lot: make_valid turned the bowtie into a MultiPolygon,
    # it did not explode it into its two triangles.
    assert len(loaded) == 2
    assert loaded["NO_LOT"].tolist() == ["1", "2"]

    metadata = materialization_metadata(result, building_lot_intersections)
    # Reported, so the repair is visible rather than silent.
    assert metadata["num_geometries_repaired"].value == 1


def test_valid_geometry_is_left_alone_and_reported_as_such(store, monkeypatch):
    write_partition(store)
    stub_postgis(monkeypatch)

    result = run(store)

    metadata = materialization_metadata(result, building_lot_intersections)
    assert metadata["num_geometries_repaired"].value == 0


def test_a_duplicated_lot_number_fails_rather_than_multiplying_the_joins(
    store, monkeypatch
):
    """Infolot can return the same lot twice for one boundary query.

    `load_lots` resolves a repeat with `ON CONFLICT ... DO NOTHING`, so nothing
    downstream would ever say it happened - and a duplicate that got past it
    would show up as a plausible-looking pair count rather than as an error.
    """
    write_partition(store)
    write_lots(store, lot_numbers=("1", "1"))
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="appear more than once"):
        run(store)
