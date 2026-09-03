"""Offline tests for `zoning_grid_columns` and `lot_zoning_envelopes`.

Neither asset touches a database - the joins they read were computed in
PostGIS by `building_lot_intersections` and `lot_frontage`, and what is left
here is a parse and two merges, which is exactly what these cover.

The one seam stubbed out is the download. `PdfCache` hands back a fetcher that
reads bytes off disk or off the city's web server; the fixture below
substitutes one that serves bytes from a dict, and nothing else is replaced -
`zoning_grid_columns` runs the real `parse_grid_pdf` over a real PDF, so what
these cover includes the parse.

The grid itself comes from `test_zoning_grid.grid_pdf` - the published
C01-001, one Commerce column and one bare Habitation column - so what these
assert about `usages`, `levels` and `governs_residential` is a real grid's own
reading of itself.
"""

from __future__ import annotations

import json

import geopandas as gpd
import pandas as pd
import pytest
from asset_helpers import materialization_metadata, stub_publish as stub_publish_into
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import LineString, Polygon
from test_zoning_grid import grid_pdf

from urban_rag.building_lots_assets import (
    LOT_FEATURES_FILE,
    building_lot_intersections,
)
from urban_rag.envelope_assets import (
    LOT_ENVELOPES_FILE,
    ZONE_COLUMNS_FILE,
    lot_zoning_envelopes,
    zoning_grid_columns,
)
from urban_rag import envelope_assets
from urban_rag.frames import write_frame
from urban_rag.frontage_assets import LOT_FRONTAGE_FILE, lot_frontage
from urban_rag.rag_assets import DOCUMENTS_FILE, linked_documents
from urban_rag.resources import ParquetStore, PdfCache, PostgisResource
from urban_rag.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
ZONE_TABLE = "Reglement_urbanisme__VSP_REG_ZONE"
GRID_URL = "http://example.invalid/zone/C01-001.pdf"


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture
def cache(tmp_path):
    return PdfCache(cache_dir=str(tmp_path / "pdf"), request_delay_seconds=0.0)


@pytest.fixture
def stub_pdfs(monkeypatch):
    """Serve PDFs out of a dict instead of off the network, keyed by URL.

    Returns the dict the test fills in; a URL left out of it raises on fetch,
    which is how the dead-link case below is set up.
    """
    pages: dict[str, bytes] = {}

    class _Fetcher:
        def fetch(self, url):
            if url not in pages:
                raise OSError(f"{url}: no such document")
            return pages[url], True

    monkeypatch.setattr(PdfCache, "fetcher", lambda self: _Fetcher())
    return pages


def write_documents(store, *, urls=(GRID_URL,), feature_ids=(["C01-001"],)):
    """One partition of `linked_documents`, at the columns this asset reads."""
    frame = pd.DataFrame(
        {
            "doc_id": [f"doc{index}" for index in range(len(urls))],
            "source_table": [ZONE_TABLE] * len(urls),
            "neighborhood": [NEIGHBORHOOD] * len(urls),
            "scrape_date": [DATE] * len(urls),
            "url": list(urls),
            "feature_ids": [json.dumps(ids) for ids in feature_ids],
            "title": [None] * len(urls),
        }
    )
    write_frame(
        frame,
        join(
            store.partition_dir(
                linked_documents.key.path[-1], DATE, NEIGHBORHOOD
            ),
            DOCUMENTS_FILE,
        ),
    )


def write_lot_features(store, *, lot_uids=(1,), zones=("C01-001",), pct=(100.0,)):
    """The lot x feature side of `building_lot_intersections`."""
    frame = gpd.GeoDataFrame(
        {
            "lot_feature_uid": list(range(1, len(lot_uids) + 1)),
            "lot_uid": list(lot_uids),
            "lot_number": [f"2 216 {uid:03d}" for uid in lot_uids],
            "feature_uid": list(range(1, len(lot_uids) + 1)),
            "source_table": [ZONE_TABLE] * len(lot_uids),
            "feature_id": list(zones),
            "neighborhood": [NEIGHBORHOOD] * len(lot_uids),
            "scrape_date": [DATE] * len(lot_uids),
            "lot_area_m2": [400.0] * len(lot_uids),
            "overlap_area_m2": [400.0 * p / 100.0 for p in pct],
            "pct_of_lot": list(pct),
        },
        geometry=[Polygon([(0, 0), (0, 0.001), (0.001, 0.001), (0.001, 0)])]
        * len(lot_uids),
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(
                building_lot_intersections.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_FEATURES_FILE,
        ),
    )


def write_frontage(store, *, lot_uids=(1,), lengths=(30.0,), ranks=(1,)):
    """`lot_frontage`, one row per (lot, street side), longest first."""
    frame = gpd.GeoDataFrame(
        {
            "lot_frontage_uid": list(range(1, len(lot_uids) + 1)),
            "lot_uid": list(lot_uids),
            "lot_number": [f"2 216 {uid:03d}" for uid in lot_uids],
            "street_uid": list(range(1, len(lot_uids) + 1)),
            "cote_rue_id": [f"c{index}" for index in range(len(lot_uids))],
            "street_name": ["Jarry", "Papineau", "De Castelnau"][: len(lot_uids)]
            if len(lot_uids) <= 3
            else ["Jarry"] * len(lot_uids),
            "neighborhood": [NEIGHBORHOOD] * len(lot_uids),
            "scrape_date": [DATE] * len(lot_uids),
            "buffer_m": [3.0] * len(lot_uids),
            "frontage_m": list(lengths),
            "lot_perimeter_m": [80.0] * len(lot_uids),
            "pct_of_perimeter": [length / 80.0 * 100 for length in lengths],
            "frontage_rank": list(ranks),
        },
        geometry=[LineString([(0, 0), (0.0001, 0)])] * len(lot_uids),
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(lot_frontage.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_FRONTAGE_FILE,
        ),
    )


@pytest.fixture(autouse=True)
def stub_publish(monkeypatch):
    """The upsert into silver.zoning_grid_columns / silver.lot_zoning_envelopes.

    Recorded rather than run: both assets publish the same frame they write to
    the tree, and every test here is about the parse and the join. The frames
    handed over are kept for the tests that check what reaches the database.
    """
    return stub_publish_into(monkeypatch, envelope_assets)


def run_columns(store, cache):
    return materialize(
        [zoning_grid_columns],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={
            "store": store,
            "pdf_cache": cache,
            "postgis": PostgisResource(),
        },
        selection=[zoning_grid_columns],
    )


def run_envelopes(store, run_config=None):
    return materialize(
        [lot_zoning_envelopes],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[lot_zoning_envelopes],
        run_config=run_config,
    )


def read_columns(store):
    return pd.read_parquet(
        join(
            store.partition_dir(
                zoning_grid_columns.key.path[-1], DATE, NEIGHBORHOOD
            ),
            ZONE_COLUMNS_FILE,
        )
    )


def read_envelopes(store):
    return pd.read_parquet(
        join(
            store.partition_dir(
                lot_zoning_envelopes.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_ENVELOPES_FILE,
        )
    )


# -- zoning_grid_columns ----------------------------------------------------


def test_writes_one_row_per_grid_column(store, cache, stub_pdfs):
    stub_pdfs[GRID_URL] = grid_pdf()
    write_documents(store)

    result = run_columns(store, cache)
    assert result.success

    frame = read_columns(store)
    assert len(frame) == 2
    assert sorted(json.loads(u)[0] for u in frame["usages"]) == ["C.4", "H"]
    assert frame["feature_id"].tolist() == ["C01-001", "C01-001"]
    assert frame["grid_zone"].tolist() == ["C01-001", "C01-001"]


def test_carries_the_norms_as_columns_of_the_row(store, cache, stub_pdfs):
    stub_pdfs[GRID_URL] = grid_pdf()
    write_documents(store)
    run_columns(store, cache)

    habitation = read_envelope_column(read_columns(store))
    assert habitation["floors_min"] == 2
    assert habitation["floors_max"] == 6
    # Authorised on every level but the ground floor, so five of the six.
    assert habitation["residential_floors"] == 5
    assert habitation["density_max"] == 4.5
    assert habitation["site_coverage_max_pct"] == 70.0
    assert habitation["usage_habitation"] == "H"
    assert pd.isna(habitation["usage_commerce"])
    # "Largeur du terrain -": no minimum, which is not a minimum of zero.
    assert pd.isna(habitation["min_lot_width_m"])
    assert habitation["solver_ready"]


def read_envelope_column(frame):
    residential = frame[frame["permits_residential"]]
    assert len(residential) == 1
    return residential.iloc[0]


def test_a_grid_two_zones_share_reaches_both(store, cache, stub_pdfs):
    """`linked_documents` dedupes by URL, so one document can be two zones."""
    stub_pdfs[GRID_URL] = grid_pdf()
    write_documents(store, feature_ids=(["C01-001", "C01-009"],))

    run_columns(store, cache)
    frame = read_columns(store)
    assert sorted(frame["feature_id"].unique()) == ["C01-001", "C01-009"]
    assert len(frame) == 4
    # The page prints one zone whatever the map links it from.
    assert set(frame["grid_zone"]) == {"C01-001"}


def test_one_unreadable_grid_costs_its_zone_and_not_the_borough(
    store, cache, stub_pdfs
):
    stub_pdfs[GRID_URL] = grid_pdf()
    write_documents(
        store,
        urls=(GRID_URL, "http://example.invalid/zone/dead.pdf"),
        feature_ids=(["C01-001"], ["C01-002"]),
    )

    result = run_columns(store, cache)
    assert result.success

    metadata = materialization_metadata(result, zoning_grid_columns)
    assert metadata["num_documents"].value == 2
    assert metadata["num_documents_parsed"].value == 1
    assert metadata["num_documents_failed"].value == 1
    assert read_columns(store)["feature_id"].unique().tolist() == ["C01-001"]


def test_every_grid_failing_fails_the_partition(store, cache, stub_pdfs):
    write_documents(store)  # nothing registered in stub_pdfs
    with pytest.raises(Failure, match="could be read"):
        run_columns(store, cache)


def test_a_column_with_no_storey_ceiling_lands_but_is_not_solver_ready(
    store, cache, stub_pdfs
):
    stub_pdfs[GRID_URL] = grid_pdf(floors=("2/6", "-"))
    write_documents(store)
    run_columns(store, cache)

    habitation = read_envelope_column(read_columns(store))
    assert not habitation["solver_ready"]
    assert "storey maximum" in habitation["solver_error"]
    assert pd.isna(habitation["floors_max"])


# -- lot_zoning_envelopes ---------------------------------------------------


def materialize_both(store, cache, stub_pdfs, *, feature_ids=None, **envelope_kwargs):
    stub_pdfs.setdefault(GRID_URL, grid_pdf())
    if feature_ids is None:
        write_documents(store)
    else:
        write_documents(store, feature_ids=(feature_ids,))
    run_columns(store, cache)
    return run_envelopes(store, **envelope_kwargs)


def test_one_row_per_lot_and_grid_column(store, cache, stub_pdfs):
    write_lot_features(store)
    write_frontage(store)
    result = materialize_both(store, cache, stub_pdfs)
    assert result.success

    frame = read_envelopes(store)
    assert len(frame) == 2
    assert set(frame["lot_uid"]) == {1}
    assert frame["lot_area_m2"].unique().tolist() == [400.0]
    assert frame["lot_number"].unique().tolist() == ["2 216 001"]


def test_carries_the_primary_and_secondary_frontage(store, cache, stub_pdfs):
    write_lot_features(store)
    write_frontage(store, lot_uids=(1, 1), lengths=(30.0, 12.0), ranks=(1, 2))

    materialize_both(store, cache, stub_pdfs)
    frame = read_envelopes(store)

    assert frame["primary_frontage_m"].unique().tolist() == [30.0]
    assert frame["primary_street_name"].unique().tolist() == ["Jarry"]
    assert frame["secondary_frontage_m"].unique().tolist() == [12.0]
    assert frame["secondary_street_name"].unique().tolist() == ["Papineau"]
    assert frame["num_frontages"].unique().tolist() == [2]


def test_an_interior_lot_has_no_frontage_and_says_so(store, cache, stub_pdfs):
    write_lot_features(store)
    write_frontage(store, lot_uids=(2,), lengths=(30.0,))  # a different lot

    materialize_both(store, cache, stub_pdfs)
    frame = read_envelopes(store)

    assert frame["primary_frontage_m"].isna().all()
    assert frame["secondary_frontage_m"].isna().all()
    # No minimum width to fail, so the column still applies.
    assert frame["meets_min_lot_width"].all()


def test_governs_residential_marks_the_column_the_solver_would_pick(
    store, cache, stub_pdfs
):
    write_lot_features(store)
    write_frontage(store)
    materialize_both(store, cache, stub_pdfs)

    frame = read_envelopes(store)
    governing = frame[frame["governs_residential"]]
    assert len(governing) == 1
    assert json.loads(governing.iloc[0]["usages"]) == ["H"]
    # The Commerce column authorises no dwelling, so it governs nothing here.
    assert not frame[~frame["permits_residential"]]["governs_residential"].any()


def test_a_lot_too_narrow_for_every_residential_column_governs_none(
    store, cache, stub_pdfs
):
    """*Largeur du terrain min* is a real answer about the parcel."""
    stub_pdfs[GRID_URL] = grid_pdf(lot_width=("-", "18"))
    write_lot_features(store)
    write_frontage(store, lengths=(9.0,))

    materialize_both(store, cache, stub_pdfs)
    frame = read_envelopes(store)

    assert frame["min_lot_width_m"].max() == 18.0
    assert not frame["meets_min_lot_width"].all()
    assert not frame["governs_residential"].any()


def test_a_sliver_of_a_neighbouring_zone_can_be_configured_away(
    store, cache, stub_pdfs
):
    # Two *distinct* zones, which is what a lot on a boundary actually meets:
    # the same number twice is one zone, and the assets now say so.
    write_lot_features(
        store, lot_uids=(1, 1), zones=("C01-001", "C01-009"), pct=(97.0, 3.0)
    )
    write_frontage(store)

    materialize_both(store, cache, stub_pdfs, feature_ids=["C01-001", "C01-009"])
    assert len(read_envelopes(store)) == 4

    run_envelopes(
        store,
        run_config={
            "ops": {
                "silver__lot_zoning_envelopes": {"config": {"min_pct_of_lot": 5.0}}
            }
        },
    )
    frame = read_envelopes(store)
    assert len(frame) == 2
    assert frame["pct_of_lot"].unique().tolist() == [97.0]


def test_a_square_metre_of_a_neighbouring_zone_is_not_an_envelope(
    store, cache, stub_pdfs
):
    """The artefact cutoff, which unlike `min_pct_of_lot` is on by default.

    0.2% of a 400 m2 lot is 0.8 m2 - the cadastre and the zoning layer missing
    each other along a lot line, not a second set of rules. It goes out
    without being configured away, and the percentage cutoff would not have
    caught it: at its default of 0 every overlap is kept.
    """
    write_lot_features(
        store, lot_uids=(1, 1), zones=("C01-001", "C01-009"), pct=(99.8, 0.2)
    )
    write_frontage(store)

    result = materialize_both(
        store, cache, stub_pdfs, feature_ids=["C01-001", "C01-009"]
    )
    frame = read_envelopes(store)
    assert frame["feature_id"].unique().tolist() == ["C01-001"]
    assert len(frame) == 2

    metadata = materialization_metadata(result, lot_zoning_envelopes)
    # One lot x zone pair, which the grid then turns into the two columns the
    # kept zone contributes.
    assert metadata["min_overlap_m2"].value == 1.0
    assert metadata["num_sliver_pairs_dropped"].value == 1


def test_the_artefact_cutoff_can_be_turned_off(store, cache, stub_pdfs):
    write_lot_features(
        store, lot_uids=(1, 1), zones=("C01-001", "C01-009"), pct=(99.8, 0.2)
    )
    write_frontage(store)

    materialize_both(
        store,
        cache,
        stub_pdfs,
        feature_ids=["C01-001", "C01-009"],
        run_config={
            "ops": {
                "silver__lot_zoning_envelopes": {"config": {"min_overlap_m2": 0.0}}
            }
        },
    )
    assert len(read_envelopes(store)) == 4


def test_a_zone_a_grid_cites_twice_is_one_zone(store, cache, stub_pdfs):
    """A repeated id in `feature_ids` is not two zones sharing a grid.

    `silver.zoning_grid_columns` is keyed on (source_table, feature_id,
    column_index) and `silver.lot_zoning_envelopes` on (lot_uid, feature_id,
    column_index), so Postgres would collapse the repeat and leave it in the
    parquet - which is the file `hbu_candidates` reads, and a second CP-SAT
    model on the same envelope.
    """
    write_lot_features(store)
    write_frontage(store)

    materialize_both(store, cache, stub_pdfs, feature_ids=["C01-001", "C01-001"])

    columns = read_columns(store)
    assert len(columns) == 2
    assert columns["feature_id"].unique().tolist() == ["C01-001"]

    frame = read_envelopes(store)
    assert len(frame) == 2
    assert not frame.duplicated(
        subset=["lot_uid", "feature_id", "column_index"]
    ).any()


def test_a_zone_no_grid_was_parsed_for_fails_the_partition(store, cache, stub_pdfs):
    write_lot_features(store, zones=("C01-999",))
    write_frontage(store)
    with pytest.raises(Failure, match="share no zone number"):
        materialize_both(store, cache, stub_pdfs)


def test_metadata_counts_what_is_solvable(store, cache, stub_pdfs):
    write_lot_features(store)
    write_frontage(store)
    result = materialize_both(store, cache, stub_pdfs)

    metadata = materialization_metadata(result, lot_zoning_envelopes)
    assert metadata["num_envelopes"].value == 2
    assert metadata["num_lots_zoned"].value == 1
    assert metadata["num_residential_envelopes"].value == 1
    assert metadata["num_governing_envelopes"].value == 1
    assert metadata["num_solvable_envelopes"].value == 1
    assert metadata["num_lots_solvable"].value == 1
    assert metadata["min_pct_of_lot"].value == 0.0
    assert metadata["min_overlap_m2"].value == 1.0
    assert metadata["num_sliver_pairs_dropped"].value == 0
    assert metadata["num_duplicate_rows_dropped"].value == 0


def test_a_row_is_one_call_to_solve_program(store, cache, stub_pdfs):
    """The point of the table: a governing row is the solver's whole input."""
    from urban_rag.program import (
        BuildingLevel,
        Lot,
        UnitEconomics,
        ZoneColumn,
        solve_program,
    )

    write_lot_features(store)
    write_frontage(store)
    materialize_both(store, cache, stub_pdfs)

    row = read_envelopes(store).query("governs_residential").iloc[0]
    program = solve_program(
        ZoneColumn(
            usages=tuple(json.loads(row["usages"])),
            floors_max=int(row["floors_max"]),
            levels=frozenset(
                BuildingLevel(level) for level in json.loads(row["levels"])
            ),
            floors_min=int(row["floors_min"]),
            density_max=float(row["density_max"]),
            site_coverage_max_pct=float(row["site_coverage_max_pct"]),
            zone=row["feature_id"],
        ),
        Lot(
            area_m2=float(row["lot_area_m2"]),
            frontage_m=float(row["primary_frontage_m"]),
            lot_number=row["lot_number"],
        ),
        UnitEconomics(average_rent_cad={"2_bedroom": 1_500.0}),
    )
    assert program.solved
    assert program.total_dwellings > 0
    assert program.zone == "C01-001"
    assert program.lot_number == "2 216 001"
    # Six storeys authorised, the ground floor not among them - the number the
    # grid never prints, carried on the row as `residential_floors`. It bounds
    # the *dwellings*: `program.floors` is the whole building, and the sixth
    # storey the level rows deny them is where the stalls end up.
    assert program.residential_floors <= int(row["residential_floors"]) == 5
    assert program.floors <= int(row["floors_max"]) == 6
