"""Offline tests for the assessment-roll client and the three assets over it.

Nothing here touches the network. The province-wide archive is stubbed by
writing a tiny real GeoPackage - two layers, named the way the publisher names
them, with the roll year stamped on - and zipping it the way the MAMH ships it,
so what is under test is GDAL's own GeoPackage reading, the download and unpack
cache, the merge on `id_provinc`, and the point-in-lot totals - not the network.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import Point, box

from asset_helpers import materialization_metadata, stub_publish

from urban_rag import role_assets
from urban_rag.frames import write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.resources import ParquetStore, PostgisResource, RoleResource
from urban_rag.role_assets import (
    ASSESSMENT_UNITS_FILE,
    LOT_VALUES_FILE,
    POINTS_FILE,
    UNITS_FILE,
    assessment_units,
    lot_assessed_values,
    property_assessment_roll,
)
from urban_rag.role_foncier import (
    JOIN_KEY,
    MONTREAL_CODE_MUN,
    POINT_LAYER,
    UNITS_LAYER,
    VALUE_COLUMN,
    RoleError,
    RoleFetcher,
    filename_for,
    layer_named,
    municipality_filter,
    read_layer,
)
from urban_rag.storage import join

DATE = "2026-08-26"
NEIGHBORHOOD = "VSMPE"
ROLL_YEAR = 2026
ARCHIVE = filename_for(ROLL_YEAR)

#: What the publisher calls the two layers this reads, roll year and all.
POINT_LAYER_NAME = f"{POINT_LAYER}_{ROLL_YEAR}"
UNITS_LAYER_NAME = f"{UNITS_LAYER}_{ROLL_YEAR}"

#: Another municipality's code, so the `code_mun` filter has something to drop.
LAVAL_CODE_MUN = "65005"


def unit_id(code_mun: str, matricule: str) -> str:
    """`id_provinc` as the roll builds it: municipality code, then matricule."""
    return f"{code_mun}{matricule}"


def points_layer(rows: list[tuple[str, str, float, float]]) -> gpd.GeoDataFrame:
    """`rol_unite_p`: one point per unit, published in NAD83 like the real one."""
    return gpd.GeoDataFrame(
        {
            JOIN_KEY: [unit_id(code, mat) for code, mat, _, _ in rows],
            "code_mun": [code for code, _, _, _ in rows],
            "mat18": [mat for _, mat, _, _ in rows],
            "arrond": ["REM25" for _ in rows],
        },
        geometry=[Point(x, y) for _, _, x, y in rows],
        crs="EPSG:4269",
    )


def units_layer(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    """`b05v_unite_evaln`: the characteristics, keyed the same way."""
    return pd.DataFrame(
        {
            JOIN_KEY: [unit_id(code, mat) for code, mat, _ in rows],
            "code_mun": [code for code, _, _ in rows],
            "mat18": [mat for _, mat, _ in rows],
            "rl0102a": ["REM25" for _ in rows],
            VALUE_COLUMN: [value for _, _, value in rows],
        }
    )


def zipped_geopackage(
    directory: Path,
    points: gpd.GeoDataFrame,
    units: pd.DataFrame,
    *,
    name: str = ARCHIVE,
) -> Path:
    """Write both layers as a real GeoPackage, zipped the way the roll is.

    The archive nests the GeoPackage under a year-stamped folder and ships a
    codebook and a PDF beside it, so the fixture does too - that is what
    `_geopackage_member` has to find its way through.
    """
    directory.mkdir(parents=True, exist_ok=True)
    gpkg = directory / f"Role_{ROLL_YEAR}_2.gpkg"
    points.to_file(gpkg, layer=POINT_LAYER_NAME, driver="GPKG")
    # A non-spatial layer: GeoPandas needs a GeoDataFrame to write one, so the
    # table goes in through pyogrio, which is what reads it back.
    pyogrio.write_dataframe(units, gpkg, layer=UNITS_LAYER_NAME, append=True)

    zip_path = directory / name
    folder = f"Role{ROLL_YEAR}_geopackage"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(gpkg, f"{folder}/{gpkg.name}")
        archive.writestr(f"{folder}/CUBF_MEFQ.xlsx", b"not read")
        archive.writestr(f"{folder}/Structure d'attributs.pdf", b"not read either")
    gpkg.unlink()
    return zip_path


class FakeResponse:
    def __init__(self, content: bytes, *, content_type: str = "application/zip"):
        self.content = content
        self.headers = {"Content-Type": content_type}
        self.status_code = 200

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start : start + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    """Replays canned archive bytes, keyed by the tail of the URL."""

    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.calls: list[str] = []

    def get(self, url, timeout=None, stream=False):
        self.calls.append(url)
        filename = url.rsplit("/", 1)[-1]
        if filename not in self.files:
            raise AssertionError(f"unexpected download: {url}")
        return FakeResponse(self.files[filename])


def fetcher_for(tmp_path: Path, archive: Path) -> tuple[RoleFetcher, FakeSession]:
    session = FakeSession({archive.name: archive.read_bytes()})
    return (
        RoleFetcher(
            cache_dir=tmp_path / "cache",
            base_url="https://example/role",
            request_delay_seconds=0,
            session=session,
        ),
        session,
    )


# -- client -----------------------------------------------------------------


def test_filename_carries_the_roll_year():
    assert filename_for(2026) == "ROLE2026_GEOPACKAGE.zip"
    assert filename_for(2027) == "ROLE2027_GEOPACKAGE.zip"


def test_fetch_downloads_once_and_caches_by_filename(tmp_path):
    archive = zipped_geopackage(
        tmp_path / "src",
        points_layer([(MONTREAL_CODE_MUN, "1" * 18, -73.6, 45.5)]),
        units_layer([(MONTREAL_CODE_MUN, "1" * 18, 100.0)]),
    )
    fetcher, session = fetcher_for(tmp_path, archive)

    path = fetcher.fetch(ARCHIVE)

    assert path == tmp_path / "cache" / ARCHIVE
    assert session.calls == [f"https://example/role/{ARCHIVE}"]

    # A second fetch reuses the cache instead of downloading half a gigabyte.
    fetcher.fetch(ARCHIVE)
    assert session.calls == [f"https://example/role/{ARCHIVE}"]


def test_a_non_zip_response_is_rejected_and_leaves_no_cache_entry(tmp_path):
    session = FakeSession({ARCHIVE: b"<html>404 not found</html>"})
    fetcher = RoleFetcher(
        cache_dir=tmp_path / "cache",
        base_url="https://example/role",
        request_delay_seconds=0,
        session=session,
    )

    with pytest.raises(RoleError, match="not a zip"):
        fetcher.fetch(ARCHIVE)
    # The half-written file must not be left where the next run would trust it.
    assert not (tmp_path / "cache" / ARCHIVE).exists()
    assert not list((tmp_path / "cache").glob("*.part"))


def test_geopackage_is_unpacked_beside_the_archive_and_cached(tmp_path):
    archive = zipped_geopackage(
        tmp_path / "src",
        points_layer([(MONTREAL_CODE_MUN, "1" * 18, -73.6, 45.5)]),
        units_layer([(MONTREAL_CODE_MUN, "1" * 18, 100.0)]),
    )
    fetcher, _ = fetcher_for(tmp_path, archive)

    gpkg = fetcher.geopackage(ARCHIVE)

    assert gpkg.parent == tmp_path / "cache"
    assert gpkg.suffix == ".gpkg"
    stamp = gpkg.stat().st_mtime_ns
    # Unpacked once: 2.8 GB is not a thing to write per scrape date.
    assert fetcher.geopackage(ARCHIVE) == gpkg
    assert gpkg.stat().st_mtime_ns == stamp


def test_layer_named_resolves_the_roll_year_suffix(tmp_path):
    archive = zipped_geopackage(
        tmp_path / "src",
        points_layer([(MONTREAL_CODE_MUN, "1" * 18, -73.6, 45.5)]),
        units_layer([(MONTREAL_CODE_MUN, "1" * 18, 100.0)]),
    )
    fetcher, _ = fetcher_for(tmp_path, archive)
    gpkg = fetcher.geopackage(ARCHIVE)

    assert layer_named(gpkg, POINT_LAYER) == POINT_LAYER_NAME
    assert layer_named(gpkg, UNITS_LAYER) == UNITS_LAYER_NAME
    with pytest.raises(RoleError, match="no layer named"):
        layer_named(gpkg, "b05v_lot_cadst")


def test_read_layer_filters_in_ogr_and_reprojects_to_4326(tmp_path):
    archive = zipped_geopackage(
        tmp_path / "src",
        points_layer(
            [
                (MONTREAL_CODE_MUN, "1" * 18, -73.6, 45.5),
                (LAVAL_CODE_MUN, "2" * 18, -73.7, 45.6),
            ]
        ),
        units_layer(
            [(MONTREAL_CODE_MUN, "1" * 18, 100.0), (LAVAL_CODE_MUN, "2" * 18, 200.0)]
        ),
    )
    fetcher, _ = fetcher_for(tmp_path, archive)
    gpkg = fetcher.geopackage(ARCHIVE)

    points = read_layer(
        gpkg, POINT_LAYER_NAME, where=municipality_filter([MONTREAL_CODE_MUN])
    )

    assert list(points["code_mun"]) == [MONTREAL_CODE_MUN]
    # Published in NAD83; the rest of the platform joins in WGS 84.
    assert points.crs.to_string() == "EPSG:4326"

    units = read_layer(gpkg, UNITS_LAYER_NAME, geometry=False)
    assert not isinstance(units, gpd.GeoDataFrame)
    assert len(units) == 2


def test_municipality_filter_rejects_anything_that_is_not_a_code():
    assert municipality_filter([]) is None
    assert municipality_filter(["66023"]) == "code_mun IN ('66023')"
    assert (
        municipality_filter(["66023", "65005"]) == "code_mun IN ('66023', '65005')"
    )
    with pytest.raises(RoleError, match="five-digit"):
        municipality_filter(["66023' OR '1'='1"])


# -- assets -----------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture
def role(tmp_path, monkeypatch):
    """Two Montreal units on the same lot, and one in Laval to be filtered out.

    Patched on the class rather than on an instance: Dagster rebuilds the
    resource before the run.
    """
    archive = zipped_geopackage(
        tmp_path / "src",
        points_layer(
            [
                (MONTREAL_CODE_MUN, "1" * 18, -73.60, 45.50),
                (MONTREAL_CODE_MUN, "2" * 18, -73.60, 45.50),
                (MONTREAL_CODE_MUN, "3" * 18, -73.40, 45.50),
                (LAVAL_CODE_MUN, "4" * 18, -73.70, 45.60),
            ]
        ),
        units_layer(
            [
                (MONTREAL_CODE_MUN, "1" * 18, 300_000.0),
                (MONTREAL_CODE_MUN, "2" * 18, 200_000.0),
                (MONTREAL_CODE_MUN, "3" * 18, 500_000.0),
                (LAVAL_CODE_MUN, "4" * 18, 900_000.0),
            ]
        ),
    )
    fetcher, session = fetcher_for(tmp_path, archive)
    monkeypatch.setattr(RoleResource, "fetcher", lambda self: fetcher)
    return session


def run_roll(store, *, codes=(MONTREAL_CODE_MUN,)):
    return materialize(
        [property_assessment_roll],
        partition_key=DATE,
        resources={"role": RoleResource(cache_dir=str(store.root_dir)), "store": store},
        run_config={
            "ops": {
                "bronze__property_assessment_roll": {
                    "config": {"municipality_codes": list(codes)}
                }
            }
        },
    )


def run_units(store):
    return materialize(
        [assessment_units],
        partition_key=DATE,
        resources={"store": store},
    )


def write_lots(store, lots: gpd.GeoDataFrame) -> None:
    """The upstream cadastre `lot_assessed_values` totals the roll onto."""
    write_frame(
        lots,
        join(
            store.partition_dir(
                neighborhood_lots.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOTS_FILE,
        ),
    )


@pytest.fixture
def published(monkeypatch):
    """`silver.lot_assessed_values` needs a database; the upsert is recorded."""
    return stub_publish(monkeypatch, role_assets)


def run_lot_values(store):
    return materialize(
        [lot_assessed_values],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
    )


def test_bronze_writes_both_layers_under_the_scrape_date(store, role):
    result = run_roll(store)

    assert result.success
    output_dir = Path(
        store.partition_dir(property_assessment_roll.key.path[-1], DATE)
    )
    assert (output_dir / POINTS_FILE).exists()
    assert (output_dir / UNITS_FILE).exists()

    points = gpd.read_parquet(output_dir / POINTS_FILE)
    # Filtered in OGR, so Laval never reaches the frame.
    assert len(points) == 3
    assert set(points["code_mun"]) == {MONTREAL_CODE_MUN}
    assert points.crs.to_string() == "EPSG:4326"
    # The path holds bare keys, so the snapshot travels as columns.
    assert set(points["scrape_date"]) == {DATE}
    assert set(points["roll_year"]) == {ROLL_YEAR}
    assert set(points["source_layer"]) == {POINT_LAYER_NAME}

    metadata = materialization_metadata(result, property_assessment_roll)
    assert metadata["num_assessment_points"].value == 3
    assert metadata["num_characteristics_rows"].value == 3
    assert metadata["municipality_filter"].value == "code_mun IN ('66023')"


def test_bronze_keeps_the_province_when_no_code_is_given(store, role):
    result = run_roll(store, codes=())

    assert result.success
    metadata = materialization_metadata(result, property_assessment_roll)
    assert metadata["num_assessment_points"].value == 4
    assert metadata["num_municipalities"].value == 2


def test_bronze_fails_when_no_municipality_matches(store, role):
    with pytest.raises(Failure, match="check the municipality codes"):
        run_roll(store, codes=("12345",))


def test_silver_merges_the_two_layers_on_id_provinc(store, role):
    run_roll(store)

    result = run_units(store)

    assert result.success
    merged = gpd.read_parquet(
        Path(store.partition_dir(assessment_units.key.path[-1], DATE))
        / ASSESSMENT_UNITS_FILE
    )
    assert len(merged) == 3
    # The value column is what the merge exists to bring across.
    assert merged[VALUE_COLUMN].sum() == 1_000_000.0
    # `code_mun` and `mat18` are published in both and are dropped from the
    # right-hand side rather than suffixed, since they say the same thing.
    assert "code_mun_unite" not in merged.columns
    assert "mat18_unite" not in merged.columns
    # `rl0102a` is the characteristics table's own name for `arrond`, so both
    # survive: bronze vocabulary is not reconciled away.
    assert {"arrond", "rl0102a"} <= set(merged.columns)

    metadata = materialization_metadata(result, assessment_units)
    assert metadata["num_assessment_units"].value == 3
    assert metadata["num_points_unmatched"].value == 0
    assert metadata["total_assessed_value"].value == 1_000_000.0


def test_silver_refuses_a_duplicated_unit(store, role):
    run_roll(store)
    bronze = Path(store.partition_dir(property_assessment_roll.key.path[-1], DATE))
    points = gpd.read_parquet(bronze / POINTS_FILE)
    write_frame(pd.concat([points, points.iloc[:1]]), str(bronze / POINTS_FILE))

    with pytest.raises(Failure, match="appear more than once"):
        run_units(store)


def test_silver_names_the_asset_to_materialize_when_bronze_is_missing(store):
    with pytest.raises(Failure, match="does not exist"):
        run_units(store)


def test_lot_values_sum_every_unit_inside_a_lot(store, role, published):
    run_roll(store)
    run_units(store)
    write_lots(
        store,
        gpd.GeoDataFrame(
            {"NO_LOT": ["1 000 001", "1 000 002", "1 000 003"]},
            geometry=[
                # Two units fall in the first lot, one in the second, none in
                # the third.
                box(-73.61, 45.49, -73.59, 45.51),
                box(-73.41, 45.49, -73.39, 45.51),
                box(-73.31, 45.49, -73.29, 45.51),
            ],
            crs="EPSG:4326",
        ),
    )

    result = run_lot_values(store)

    assert result.success
    values = gpd.read_parquet(
        Path(
            store.partition_dir(
                lot_assessed_values.key.path[-1], DATE, NEIGHBORHOOD
            )
        )
        / LOT_VALUES_FILE
    ).set_index("NO_LOT")

    assert values.loc["1 000 001", "num_assessment_units"] == 2
    assert values.loc["1 000 001", "total_assessed_value"] == 500_000.0
    assert values.loc["1 000 002", "num_assessment_units"] == 1
    assert values.loc["1 000 002", "total_assessed_value"] == 500_000.0
    # A lot nothing is assessed on keeps its row, and its total stays null: a
    # sum over nothing is not a value of zero.
    assert values.loc["1 000 003", "num_assessment_units"] == 0
    assert pd.isna(values.loc["1 000 003", "total_assessed_value"])
    assert set(values["neighborhood"]) == {NEIGHBORHOOD}
    assert set(values["scrape_date"]) == {DATE}

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_lots"].value == 3
    assert metadata["num_lots_valued"].value == 2
    assert metadata["num_lots_unvalued"].value == 1
    assert metadata["num_units_matched"].value == 3
    assert metadata["max_units_on_a_lot"].value == 2
    assert metadata["total_assessed_value"].value == 1_000_000.0


def test_lot_values_publish_the_frame_they_wrote(store, role, published):
    """`silver.lot_assessed_values` is this asset's own table.

    Published after the parquet and from the same frame, so the file and the
    serving copy cannot say different things about a partition.
    """
    run_roll(store)
    run_units(store)
    write_lots(
        store,
        gpd.GeoDataFrame(
            {"NO_LOT": ["1 000 001"]},
            geometry=[box(-73.61, 45.49, -73.59, 45.51)],
            crs="EPSG:4326",
        ),
    )

    result = run_lot_values(store)

    assert published["calls"] == 1
    assert published["partition"] == (NEIGHBORHOOD, DATE)
    assert set(published["datasets"]) == {"lot_assessed_values"}
    assert len(published["datasets"]["lot_assessed_values"]) == 1
    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["lot_assessed_values_rows_upserted"].value == 1


def test_lot_values_refuse_a_duplicated_lot(store, role, published):
    run_roll(store)
    run_units(store)
    lots = gpd.GeoDataFrame(
        {"NO_LOT": ["1 000 001", "1 000 001"]},
        geometry=[box(-73.61, 45.49, -73.59, 45.51)] * 2,
        crs="EPSG:4326",
    )
    write_lots(store, lots)

    with pytest.raises(Failure, match="appear more than once"):
        run_lot_values(store)


def test_lot_values_refuse_a_partition_with_no_overlap_at_all(store, role, published):
    run_roll(store)
    run_units(store)
    # Lots nowhere near any assessment point: the two sides do not meet, which
    # is a filter or a partition mistake rather than a borough of empty lanes.
    write_lots(
        store,
        gpd.GeoDataFrame(
            {"NO_LOT": ["1 000 009"]},
            geometry=[box(10, 10, 11, 11)],
            crs="EPSG:4326",
        ),
    )

    with pytest.raises(Failure, match="No assessment unit falls inside"):
        run_lot_values(store)


def test_lot_values_report_units_that_fell_in_no_lot(store, role, published):
    run_roll(store)
    run_units(store)
    write_lots(
        store,
        gpd.GeoDataFrame(
            {"NO_LOT": ["1 000 002"]},
            geometry=[box(-73.41, 45.49, -73.39, 45.51)],
            crs="EPSG:4326",
        ),
    )

    result = run_lot_values(store)

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_units_matched"].value == 1
    # The two units on the other lot are somewhere else in the snapshot, and
    # this partition attributes them to nobody.
    assert metadata["num_units_unmatched_in_snapshot"].value == 2
