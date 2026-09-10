"""Offline tests for `zoning_grid_columns` and `lot_zoning_envelopes`.

Neither asset touches a database - the clip and the frontage they read were
computed in PostGIS by `lot_zone_pieces`, and what is left here is a parse and
one merge, which is exactly what these cover. The piece grain itself - the
clip, the re-ranked frontage and the two cutoffs - is covered against real
geometry in `tests/integration/test_lot_zone_pieces.py`.

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
from shapely.geometry import Polygon
from test_zoning_grid import grid_pdf

from urban_rag.envelope_assets import (
    LOT_ENVELOPES_FILE,
    ZONE_COLUMNS_FILE,
    lot_zoning_envelopes,
    zoning_grid_columns,
)
from urban_rag import envelope_assets
from urban_rag.frames import write_frame
from urban_rag.rag_assets import DOCUMENTS_FILE, linked_documents
from urban_rag.resources import ParquetStore, PdfCache, PostgisResource
from urban_rag.storage import join
from urban_rag.zone_piece_assets import LOT_ZONE_PIECES_FILE, lot_zone_pieces

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


def write_pieces(
    store,
    *,
    lot_uids=(1,),
    zones=("C01-001",),
    pct=(100.0,),
    lot_area_m2=400.0,
    frontages=(30.0,),
    streets=("Jarry",),
    secondary_frontages=(None,),
    secondary_streets=(None,),
    footprint_shares=None,
):
    """One partition of `lot_zone_pieces`, at the columns this asset reads.

    The single input `lot_zoning_envelopes` has since the pieces became a table
    of their own: the ground each zone governs, the street *that ground* faces,
    and the shares that divide the lot's assessment between its pieces. What
    used to be assembled here from `building_lot_intersections` and
    `lot_frontage` is assembled in PostGIS now - see
    `tests/integration/test_lot_zone_pieces.py`, which is where the clip, the
    re-ranked frontage and the two cutoffs are covered against real geometry.

    ``pct`` is what turns `lot_area_m2` into each piece's own area, so a split
    lot is written by passing one entry per zone.
    """
    count = len(lot_uids)
    areas = [lot_area_m2 * value / 100.0 for value in pct]
    # Rank within the lot, largest piece first - `lot_zone_pieces` writes this
    # and `_piece_index` reads it back rather than re-deriving it.
    order: dict[int, list[int]] = {}
    for index, uid in enumerate(lot_uids):
        order.setdefault(uid, []).append(index)
    ranks = [0] * count
    for indexes in order.values():
        for rank, index in enumerate(
            sorted(indexes, key=lambda i: (-areas[i], zones[i])), start=1
        ):
            ranks[index] = rank
    shares = (
        list(footprint_shares)
        if footprint_shares is not None
        else [value / sum(pct[i] for i in order[uid]) for value, uid in zip(pct, lot_uids)]
    )
    frame = gpd.GeoDataFrame(
        {
            "lot_uid": list(lot_uids),
            "feature_id": list(zones),
            "lot_number": [f"2 216 {uid:03d}" for uid in lot_uids],
            "source_table": [ZONE_TABLE] * count,
            "neighborhood": [NEIGHBORHOOD] * count,
            "scrape_date": [DATE] * count,
            "lot_area_m2": [lot_area_m2] * count,
            "piece_area_m2": areas,
            "pct_of_lot": list(pct),
            "num_lot_zones": [len(order[uid]) for uid in lot_uids],
            "zone_rank": ranks,
            "is_primary_zone": [rank == 1 for rank in ranks],
            "primary_frontage_m": list(frontages),
            "primary_street_name": list(streets),
            "primary_cote_rue_id": [f"c{index}" for index in range(count)],
            "secondary_frontage_m": list(secondary_frontages),
            "secondary_street_name": list(secondary_streets),
            "secondary_cote_rue_id": [None] * count,
            "num_frontages": [
                1 + (1 if value is not None else 0) for value in secondary_frontages
            ],
            "lot_frontage_m": [
                sum(frontages[i] or 0.0 for i in order[uid]) for uid in lot_uids
            ],
            "frontage_buffer_m": [0.0] * count,
            "existing_footprint_m2": [0.0] * count,
            "lot_footprint_m2": [0.0] * count,
            "num_buildings": [0] * count,
            "area_share": [
                value / sum(pct[i] for i in order[uid])
                for value, uid in zip(pct, lot_uids)
            ],
            "footprint_share": shares,
            "footprint_share_basis": ["area"] * count,
            "min_pct_of_lot": [1.0] * count,
            "min_overlap_m2": [1.0] * count,
            "min_piece_area_m2": [500.0] * count,
            "edge_tolerance_m": [0.25] * count,
        },
        geometry=[Polygon([(0, 0), (0, 0.001), (0.001, 0.001), (0.001, 0)])] * count,
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(lot_zone_pieces.key.path[-1], DATE, NEIGHBORHOOD),
            LOT_ZONE_PIECES_FILE,
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




def test_one_row_per_piece_and_grid_column(store, cache, stub_pdfs):
    write_pieces(store)
    result = materialize_both(store, cache, stub_pdfs)
    assert result.success

    frame = read_envelopes(store)
    assert len(frame) == 2
    assert set(frame["lot_uid"]) == {1}
    assert frame["lot_area_m2"].unique().tolist() == [400.0]
    # One zone covering the parcel whole: the piece *is* the lot, which is the
    # case on the great majority of a borough's rows.
    assert frame["piece_area_m2"].unique().tolist() == [400.0]
    assert frame["num_lot_zones"].unique().tolist() == [1]
    assert frame["is_primary_zone"].all()
    assert frame["lot_number"].unique().tolist() == ["2 216 001"]


def write_split_lot(store):
    """Lot 1 740 794's shape, at the columns this asset joins.

    27 044 m2 split 90.94 / 9.02 between two zones, the smaller one holding
    the street the parcel mostly fronts on. Named once because four tests
    below are about four different consequences of the same parcel.
    """
    write_pieces(
        store,
        lot_uids=(1, 1),
        zones=("C01-001", "C01-009"),
        pct=(90.94, 9.02),
        lot_area_m2=27_044.0,
        frontages=(15.2, 19.8),
        streets=("Herelle", "Jarry"),
        secondary_frontages=(None, None),
        secondary_streets=(None, None),
    )


def test_a_split_lot_keeps_both_zones_with_their_own_ground(
    store, cache, stub_pdfs
):
    """Both zones are envelopes, each sized to the ground its own grid governs.

    The piece areas sum to the parcel and neither of them *is* the parcel.
    Before `lot_zone_pieces` the best-covered zone was kept and handed all
    27 044 m2 of it, and the other was dropped before anything was solved.
    """
    write_split_lot(store)

    materialize_both(store, cache, stub_pdfs, feature_ids=["C01-001", "C01-009"])
    frame = read_envelopes(store)

    assert sorted(frame["feature_id"].unique()) == ["C01-001", "C01-009"]
    by_zone = frame.drop_duplicates("feature_id").set_index("feature_id")
    assert by_zone.loc["C01-001", "piece_area_m2"] == pytest.approx(24_593.8, abs=1.0)
    assert by_zone.loc["C01-009", "piece_area_m2"] == pytest.approx(2_439.4, abs=1.0)
    # The parcel is on every row and is not the ground any of them governs.
    assert frame["lot_area_m2"].unique().tolist() == [27_044.0]
    assert frame["num_lot_zones"].unique().tolist() == [2]
    # The larger piece is the one a reader wanting a single row would take.
    assert by_zone.loc["C01-001", "is_primary_zone"]
    assert not by_zone.loc["C01-009", "is_primary_zone"]


def test_each_piece_faces_its_own_street(store, cache, stub_pdfs):
    """The half of the split that is not about area.

    A commercial strip on a boulevard with housing behind it is the ordinary
    form of a Montreal arterial, and the two pieces face different streets.
    Every envelope row used to carry the *lot's* rank-1 frontage, so the
    housing behind was tested for width against a boulevard it does not touch.
    """
    write_split_lot(store)

    materialize_both(store, cache, stub_pdfs, feature_ids=["C01-001", "C01-009"])
    frame = read_envelopes(store).drop_duplicates("feature_id").set_index("feature_id")

    assert frame.loc["C01-001", "primary_street_name"] == "Herelle"
    assert frame.loc["C01-001", "primary_frontage_m"] == 15.2
    assert frame.loc["C01-009", "primary_street_name"] == "Jarry"
    assert frame.loc["C01-009", "primary_frontage_m"] == 19.8


def test_carries_the_primary_and_secondary_frontage(store, cache, stub_pdfs):
    write_pieces(
        store,
        secondary_frontages=(12.0,),
        secondary_streets=("Papineau",),
    )

    materialize_both(store, cache, stub_pdfs)
    frame = read_envelopes(store)

    assert frame["primary_frontage_m"].unique().tolist() == [30.0]
    assert frame["primary_street_name"].unique().tolist() == ["Jarry"]
    assert frame["secondary_frontage_m"].unique().tolist() == [12.0]
    assert frame["secondary_street_name"].unique().tolist() == ["Papineau"]
    assert frame["num_frontages"].unique().tolist() == [2]


def test_an_interior_piece_has_no_frontage_and_says_so(store, cache, stub_pdfs):
    write_pieces(store, frontages=(None,), streets=(None,))

    materialize_both(store, cache, stub_pdfs)
    frame = read_envelopes(store)

    assert frame["primary_frontage_m"].isna().all()
    assert frame["secondary_frontage_m"].isna().all()
    # No minimum width to fail, so the column still applies.
    assert frame["meets_min_lot_width"].all()


def test_governs_residential_marks_the_column_the_solver_would_pick(
    store, cache, stub_pdfs
):
    write_pieces(store)
    materialize_both(store, cache, stub_pdfs)

    frame = read_envelopes(store)
    governing = frame[frame["governs_residential"]]
    assert len(governing) == 1
    assert json.loads(governing.iloc[0]["usages"]) == ["H"]
    # The Commerce column authorises no dwelling, so it governs nothing here.
    assert not frame[~frame["permits_residential"]]["governs_residential"].any()


def test_a_piece_too_narrow_for_every_residential_column_governs_none(
    store, cache, stub_pdfs
):
    """*Largeur du terrain min* is a real answer about the ground."""
    stub_pdfs[GRID_URL] = grid_pdf(lot_width=("-", "18"))
    write_pieces(store, frontages=(9.0,))

    materialize_both(store, cache, stub_pdfs)
    frame = read_envelopes(store)

    assert frame["min_lot_width_m"].max() == 18.0
    assert not frame["meets_min_lot_width"].all()
    assert not frame["governs_residential"].any()


def test_the_width_test_is_taken_against_the_piece_not_the_lot(
    store, cache, stub_pdfs
):
    """Two pieces of one parcel, one wide enough for the grid and one not.

    The whole reason the frontage is re-ranked inside each piece: under the
    lot's own rank-1 edge both rows would have read 19.8 m and both would have
    qualified for an 18 m minimum, including the piece that faces 15.2 m.
    """
    stub_pdfs[GRID_URL] = grid_pdf(lot_width=("-", "18"))
    write_split_lot(store)

    materialize_both(store, cache, stub_pdfs, feature_ids=["C01-001", "C01-009"])
    frame = read_envelopes(store)
    residential = frame[frame["permits_residential"]].set_index("feature_id")

    assert not residential.loc["C01-001", "meets_min_lot_width"]
    assert residential.loc["C01-009", "meets_min_lot_width"]
    assert set(frame.loc[frame["governs_residential"], "feature_id"]) == {"C01-009"}


def test_a_zone_a_grid_cites_twice_is_one_zone(store, cache, stub_pdfs):
    """A repeated id in `feature_ids` is not two zones sharing a grid.

    `silver.zoning_grid_columns` is keyed on (source_table, feature_id,
    column_index) and `silver.lot_zoning_envelopes` on (lot_uid, feature_id,
    column_index), so Postgres would collapse the repeat and leave it in the
    parquet - which is the file `hbu_candidates` reads, and a second CP-SAT
    model on the same envelope.
    """
    write_pieces(store)

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
    write_pieces(store, zones=("C01-999",))
    with pytest.raises(Failure, match="share no zone number"):
        materialize_both(store, cache, stub_pdfs)


def test_metadata_counts_what_is_solvable(store, cache, stub_pdfs):
    write_pieces(store)
    result = materialize_both(store, cache, stub_pdfs)

    metadata = materialization_metadata(result, lot_zoning_envelopes)
    assert metadata["num_envelopes"].value == 2
    # Two columns of one grid describe one piece of ground.
    assert metadata["num_pieces"].value == 1
    assert metadata["num_lots_zoned"].value == 1
    assert metadata["num_split_lots"].value == 0
    assert metadata["num_residential_envelopes"].value == 1
    assert metadata["num_governing_envelopes"].value == 1
    assert metadata["num_solvable_envelopes"].value == 1
    assert metadata["num_lots_solvable"].value == 1
    # Read off the rows the pieces wrote rather than off this asset's config,
    # which no longer carries them.
    assert metadata["min_pct_of_lot"].value == 1.0
    assert metadata["min_overlap_m2"].value == 1.0
    assert metadata["min_piece_area_m2"].value == 500.0
    assert metadata["num_duplicate_rows_dropped"].value == 0


def test_metadata_counts_the_split_lots(store, cache, stub_pdfs):
    write_split_lot(store)
    result = materialize_both(
        store, cache, stub_pdfs, feature_ids=["C01-001", "C01-009"]
    )

    metadata = materialization_metadata(result, lot_zoning_envelopes)
    assert metadata["num_pieces"].value == 2
    assert metadata["num_split_lots"].value == 1
    assert metadata["num_envelopes_on_split_lots"].value == 4
    # The piece median is under the parcel median, which is the arithmetic of
    # the whole change in one pair of numbers.
    assert (
        metadata["median_piece_area_m2"].value < metadata["median_lot_area_m2"].value
    )


def test_a_row_is_one_call_to_solve_program(store, cache, stub_pdfs):
    """The point of the table: a zone's governing rows are the solver's whole
    input.

    The zone's rows, and not one of them. C01-001 prints its ``C.4`` on *Tous
    les niveaux* and its ``H`` on *Tous sauf le RDC* - shops at grade, flats
    over them - so the Habitation row alone is a building with nothing on its
    ground floor, which `solve_program` now refuses by name. This rebuilds the
    `ZoneEnvelope` the way `hbu._program_row` does, which is the call the
    asset actually feeds.
    """
    from urban_rag.program import (
        BuildingLevel,
        Lot,
        UnitEconomics,
        NonResidentialEconomics,
        ZoneColumn,
        ZoneEnvelope,
        solve_program,
    )

    write_pieces(store)
    materialize_both(store, cache, stub_pdfs)

    rows = read_envelopes(store)
    row = rows.query("governs_residential").iloc[0]

    def column_of(source):
        return ZoneColumn(
            usages=tuple(json.loads(source["usages"])),
            floors_max=int(source["floors_max"]),
            levels=frozenset(
                BuildingLevel(level) for level in json.loads(source["levels"])
            ),
            floors_min=int(source["floors_min"]),
            density_max=float(source["density_max"]),
            site_coverage_max_pct=float(source["site_coverage_max_pct"]),
            zone=source["feature_id"],
        )

    program = solve_program(
        ZoneEnvelope.of(
            [column_of(other) for _, other in rows.iterrows()],
            frontage_m=float(row["primary_frontage_m"]),
        ),
        # The piece, which is the ground the zone governs - `hbu.lot_of` reads
        # exactly this column.
        Lot(
            area_m2=float(row["piece_area_m2"]),
            frontage_m=float(row["primary_frontage_m"]),
            lot_number=row["lot_number"],
        ),
        UnitEconomics(average_rent_cad={"2_bedroom": 1_500.0}),
        # The borough's own surveyed retail rather than the module's $80
        # default, which outbids the housing for all six storeys and would
        # leave this asserting on a building with no dwellings in it.
        non_residential=NonResidentialEconomics(commercial_per_sqft_year=26.6007),
    )
    assert program.solved
    assert program.total_dwellings > 0
    assert program.zone == "C01-001"
    assert program.lot_number == "2 216 001"
