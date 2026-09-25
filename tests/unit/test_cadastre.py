"""Offline tests for `neighborhood_cadastre`, the load half of what used to
be `building_lot_intersections`.

`hbu_dataplatform.core.postgis`'s loaders are Postgres-only in substance - they issue
DELETE and COPY - so nothing here touches a real database. What is worth
testing without one is the asset's own logic: which partitions it reads,
what it hands to `postgis.load_lots`/`load_buildings`/`load_features`, what
it refuses, and how the tiles the load fell in are reported back.

On the feature half that is more than plumbing: which of a borough's two dozen
parquet files it decides to load, under which `source_table` value, and which
it skips. Loading a layer under the wrong name is the failure this half exists
to avoid, and it is silent - the join simply matches nothing.

The silver contract over the cadastre is tested here too, since this is the
asset that keeps it: `neighborhood_lots` is bronze and writes
self-intersecting rings through, and what reads them is `ST_Intersection`
twice over. An invalid ring gives a wrong answer rather than an error, and a
duplicated lot number multiplies every pair both joins produce - so both are
caught before the load rather than after it.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import geopandas as gpd
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import Point, Polygon, box

from asset_helpers import materialization_metadata

from hbu_dataplatform.cadastre import cadastre_assets
from hbu_dataplatform.zoning.features_assets import neighborhood_features
from hbu_dataplatform.sources.bdoi.assets import BUILDINGS_FILE, neighborhood_buildings
from hbu_dataplatform.cadastre.cadastre_assets import (
    CADASTRE_FILE,
    neighborhood_cadastre,
)
from hbu_dataplatform.core.frames import write_frame
from hbu_dataplatform.sources.infolot.assets import LOTS_FILE, neighborhood_lots
from hbu_dataplatform.partitions.axes import scrape_partitions
from hbu_dataplatform.core.postgis import GroundOutsideCut
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.core.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
ZONE_SLUG = "Reglement_urbanisme__VSP_REG_ZONE"

#: The two cut cells a borough of the fixture's size would fall in.
TILES = ("0302303330102", "03023033301023")

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


def stub_postgis(monkeypatch, *, require_raises=None, lots_raise=None, tiles=TILES):
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
        # Once per run is the point, so the count is kept too.
        calls["lot_loads"].append((neighborhood, scrape_date))
        if lots_raise is not None:
            raise lots_raise
        return len(frame)

    def load_buildings(connection, frame, *, neighborhood, scrape_date):
        calls["buildings"] = (neighborhood, scrape_date, len(frame))
        return len(frame)

    def load_features(
        connection,
        frame,
        *,
        neighborhood,
        scrape_date,
        source_table,
        source_namespace,
        feature_id_column,
    ):
        calls["features"].append(
            (source_table, source_namespace, feature_id_column, len(frame))
        )
        return len(frame)

    def tiles_of_neighborhood(connection, *, neighborhood, scrape_date):
        calls["tiles_of_neighborhood"] = (neighborhood, scrape_date)
        return tuple(tiles)

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(cadastre_assets, "require_working_set", require_working_set)
    monkeypatch.setattr(cadastre_assets, "load_lots", load_lots)
    monkeypatch.setattr(cadastre_assets, "load_buildings", load_buildings)
    monkeypatch.setattr(cadastre_assets, "load_features", load_features)
    monkeypatch.setattr(cadastre_assets, "tiles_of_neighborhood", tiles_of_neighborhood)
    return calls


def run(store):
    return materialize(
        [neighborhood_cadastre],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[neighborhood_cadastre],
    )


def read_record(store) -> dict:
    path = join(
        store.partition_dir(neighborhood_cadastre.key.path[-1], DATE, NEIGHBORHOOD),
        CADASTRE_FILE,
    )
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def test_the_load_is_on_the_borough_axis():
    """A borough is what a publisher answers for; the three snapshots this
    reads are per borough by construction, and so is the load."""
    assert neighborhood_cadastre.partitions_def is scrape_partitions


def test_loads_all_three_partitions_then_asks_which_tiles_they_fell_in(
    store, monkeypatch
):
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    result = run(store)

    assert result.success
    # Loaded with this partition's own key.
    assert calls["lots"] == (NEIGHBORHOOD, DATE, 2)
    assert calls["buildings"] == (NEIGHBORHOOD, DATE, 1)
    assert calls["features"] == [(ZONE_SLUG, "19_VSMPE", "NUMERO_COMPLET", 2)]
    # And the cascade read back inside the same transaction.
    assert calls["tiles_of_neighborhood"] == (NEIGHBORHOOD, DATE)
    # Checked before any of it, so the run does not reach a COPY to find out.
    assert calls["require_working_set"] is True


def test_a_missing_rag_working_set_names_the_file_to_apply(store, monkeypatch):
    """The three `rag` tables are hbu_infra's, and nothing else checks them.

    Every silver and gold write goes through `hbu_dataplatform.core.warehouse`, which
    checks its own target; `rag.lots`/`rag.buildings`/`rag.features` are loaded
    by raw DELETE/COPY/INSERT, so without the preflight a database that has
    never had sql/002 applied fails as `relation "rag.lots" does not exist` -
    an identifier, with nothing about which repo owns it.
    """
    write_partition(store)
    calls = stub_postgis(
        monkeypatch,
        require_raises=cadastre_assets.MissingRelation(
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


def test_the_lots_are_loaded_once(store, monkeypatch):
    """One load per borough per run, for every tile run that follows.

    Loading `rag.lots` from two assets meant two transactions racing each
    other for the table; the load being an asset of its own is what keeps it
    to one.
    """
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    assert calls["lot_loads"] == [(NEIGHBORHOOD, DATE)]


def test_ground_outside_the_cut_is_a_failure_naming_the_ground(store, monkeypatch):
    """A lot no cell of the cut owns is a lot no tile run will ever compute
    over; the loader refuses it and the asset says so rather than landing a
    borough the chain cannot see."""
    write_partition(store)
    stub_postgis(
        monkeypatch,
        lots_raise=GroundOutsideCut(
            "rag.lots", NEIGHBORHOOD, 3, ("0302310012301230123", "0302310012301230130")
        ),
    )

    with pytest.raises(Failure) as failure:
        run(store)

    message = str(failure.value)
    assert "rag.lots" in message
    assert "3 row(s) of VSMPE fall outside the tile cut" in message
    assert "0302310012301230123" in message
    # And the fix, which is a re-seed rather than a retry.
    assert "seed_tile_cut" in message


def test_the_tiles_touched_are_reported_and_written(store, monkeypatch):
    """The cascade a reload sets off: every lot_uid is reminted, and these are
    the tile runs that have to follow."""
    write_partition(store)
    stub_postgis(monkeypatch, tiles=TILES)

    result = run(store)

    metadata = materialization_metadata(result, neighborhood_cadastre)
    assert metadata["tiles_touched"].value == ", ".join(TILES)
    assert metadata["num_tiles"].value == 2
    # The tree records what landed, so a reader with no database can still
    # tell which tiles a borough's load reached.
    record = read_record(store)
    assert record["tiles_touched"] == list(TILES)
    assert record["neighborhood"] == NEIGHBORHOOD
    assert record["scrape_date"] == DATE
    assert record["num_lots"] == 2
    assert record["num_buildings"] == 1
    assert record["features_per_layer"] == {ZONE_SLUG: 2}


def test_a_rerun_replaces_the_previous_record(store, monkeypatch, tmp_path):
    partition = (
        tmp_path / "store" / "silver" / "neighborhood_cadastre" / DATE / NEIGHBORHOOD
    )
    partition.mkdir(parents=True)
    stale = partition / "cadastre_retired.json"
    stale.write_text("{}", encoding="utf-8")
    write_partition(store)
    stub_postgis(monkeypatch)

    run(store)

    assert not stale.exists()
    assert (partition / CADASTRE_FILE).exists()


def test_features_are_loaded_under_the_slug_not_the_spectrum_path(store, monkeypatch):
    """The one that silently breaks everything downstream if it regresses.

    `rag.chunks.source_table` holds the slug, and every join from a feature to
    the corpus matches the two columns to each other - so a layer loaded under
    `/19_VSMPE/Reglement_urbanisme/VSP_REG_ZONE` would join to nothing at all.
    """
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    run(store)

    assert calls["features"] == [(ZONE_SLUG, "19_VSMPE", "NUMERO_COMPLET", 2)]


def test_layers_without_an_id_or_geometry_are_skipped_not_loaded(store, monkeypatch):
    write_partition(store)
    write_layer_without_id(store)
    write_layer_without_geometry(store)
    calls = stub_postgis(monkeypatch)

    result = run(store)

    assert [slug for slug, _, _, _ in calls["features"]] == [ZONE_SLUG]
    metadata = materialization_metadata(result, neighborhood_cadastre)
    assert metadata["num_layers"].value == 1
    assert metadata["num_layers_skipped"].value == 2
    assert set(metadata["skipped"].data) == {
        "Ruelle_verte__VSP_TP_RUELLE_VERTE",
        "Mairie__VSMPE_Mairie_1",
    }


def test_metadata_reports_what_was_loaded(store, monkeypatch):
    write_partition(store)
    stub_postgis(monkeypatch)

    result = run(store)

    metadata = materialization_metadata(result, neighborhood_cadastre)
    assert metadata["dagster/row_count"].value == 2
    assert metadata["num_lots"].value == 2
    assert metadata["num_buildings"].value == 1
    assert metadata["num_features"].value == 2
    assert metadata["num_layers"].value == 1
    assert metadata["features_per_layer"].data == {ZONE_SLUG: 2}
    # The loader raises on the first row with no cell, so a run that got this
    # far checked and found none.
    assert metadata["num_rows_outside_cut"].value == 0


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


def test_only_unloadable_layers_fails_rather_than_landing_half_a_borough(
    store, monkeypatch
):
    write_lots(store)
    write_buildings(store)
    write_layer_without_id(store)
    calls = stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="no feature could be loaded"):
        run(store)

    # It fails inside the transaction, so the lots and buildings it did load
    # are rolled back with it, and before the cascade is asked for.
    assert "tiles_of_neighborhood" not in calls


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

    metadata = materialization_metadata(result, neighborhood_cadastre)
    # Reported, so the repair is visible rather than silent.
    assert metadata["num_geometries_repaired"].value == 1


def test_valid_geometry_is_left_alone_and_reported_as_such(store, monkeypatch):
    write_partition(store)
    stub_postgis(monkeypatch)

    result = run(store)

    metadata = materialization_metadata(result, neighborhood_cadastre)
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


def write_saguenay_zones(store, *, slug="Zonage__ZONAGE_SAGUENAY"):
    """Saguenay's zoning layer, in the shape `neighborhood_features` writes it.

    The columns are the published ones: a lowercase ``id`` that is the
    reporting service's own primary key, and ``no_zone``, the number printed
    on the grid. Neither is ``ID``, so a tuple that lists only Montreal's and
    Quebec City's columns matches nothing here - which is how the whole
    partition came to be unloadable.
    """
    frame = gpd.GeoDataFrame(
        {
            "id": [2468, 2069],
            "municipalite": ["Saguenay"] * 2,
            "no_zone": ["1000", "1002"],
            "LIEN_GRILLE": [
                "https://zonage.saguenay.ca/rapports/v1/zonages/grille/pdf/2468",
                "https://zonage.saguenay.ca/rapports/v1/zonages/grille/pdf/2069",
            ],
            "source_table": ["Zonage/ZONAGE_SAGUENAY"] * 2,
        },
        geometry=[box(0, 0, 1.5, 1), box(1.5, 0, 2, 1)],
        crs="EPSG:4326",
    )
    write_frame(frame, join(features_dir(store), f"{slug}.parquet"))


def test_saguenays_zoning_layer_is_keyed_on_no_zone_not_skipped(store, monkeypatch):
    """The regression that blocked SAG's whole chain at its first silver step.

    `FEATURE_ID_COLUMNS` knew Montreal's `NUMERO_COMPLET` and Quebec City's
    `IGDS_TEXT_STRING` but not Saguenay's `no_zone`, so its one zoning layer
    was skipped as having no id column - and with nothing else to load, the
    asset failed the partition outright rather than landing a borough with no
    features. Matching is exact, so the layer's lowercase `id` is not `ID`.
    """
    write_lots(store)
    write_buildings(store)
    write_saguenay_zones(store)
    calls = stub_postgis(monkeypatch)

    result = run(store)

    assert [(slug, column) for slug, _, column, _ in calls["features"]] == [
        ("Zonage__ZONAGE_SAGUENAY", "no_zone")
    ]
    metadata = materialization_metadata(result, neighborhood_cadastre)
    assert metadata["num_layers"].value == 1
    assert metadata["num_layers_skipped"].value == 0


def test_the_zone_code_wins_over_the_services_own_primary_key(store, monkeypatch):
    """`no_zone` precedes `ID`, because a document cites the zone number.

    Saguenay's layer carries both an internal key and the printed zone number;
    only the latter is what a grid is served under and what a chunk cites, so
    a layer carrying an upper-case `ID` as well must still key on `no_zone`.
    """
    frame = gpd.GeoDataFrame(
        {
            "ID": [2468],
            "no_zone": ["1000"],
            "source_table": ["Zonage/ZONAGE_SAGUENAY"],
        },
        geometry=[box(0, 0, 1, 1)],
        crs="EPSG:4326",
    )
    write_lots(store)
    write_buildings(store)
    write_frame(frame, join(features_dir(store), "Zonage__ZONAGE_SAGUENAY.parquet"))
    calls = stub_postgis(monkeypatch)

    run(store)

    assert [column for _, _, column, _ in calls["features"]] == ["no_zone"]


def test_the_two_id_column_tuples_agree():
    """The invariant `FEATURE_ID_COLUMNS`' own docstring states.

    `rag_assets._ID_COLUMNS` writes `feature_ids` onto a chunk and this one
    keys the geometry those chunks are about; they are deliberately not
    imported from each other, so nothing but this test keeps them equal. They
    drifted once - `no_zone` was added to one and not the other - and the
    symptom was a whole city that could not be loaded.
    """
    from hbu_dataplatform.rag.assets import _ID_COLUMNS

    assert cadastre_assets.FEATURE_ID_COLUMNS == _ID_COLUMNS
