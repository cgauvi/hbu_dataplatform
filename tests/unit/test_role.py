"""Offline tests for the assessment-roll client and the three assets over it.

Nothing here touches the network. The province-wide archive is stubbed by
writing a tiny real GeoPackage - three layers, named the way the publisher
names them, with the roll year stamped on - and zipping it the way the MAMH
ships it, so what is under test is GDAL's own GeoPackage reading, the download
and unpack cache, the merge on `id_provinc`, and the lot totals - not the
network.

The fixture borough is built to carry one of each shape the real one does: two
units on an ordinary lot, one unit spanning two lots (which is what makes the
full and apportioned totals differ), a divided co-ownership whose private lot
the cadastre does not draw (which only its point can place), and a lane nothing
is assessed on. See `borough_lots` and the `role` fixture.
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

from asset_helpers import (
    materialization_metadata,
    stub_publish,
    stub_publish_by_neighborhood,
)

from urban_rag import role_assets
from urban_rag.cubf import USE_DESCRIPTION_COLUMN
from urban_rag.cubf_assets import CUBF_FILE, cubf_use_codes
from urban_rag.frames import write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.open_data_assets import QUARTIERS_FILE, reference_neighborhoods
from urban_rag.partitions import borough_code_for
from urban_rag.resources import ParquetStore, PostgisResource, RoleResource
from urban_rag.role_assets import (
    ARROND_PREFIX,
    ASSESSMENT_UNITS_FILE,
    CADASTRE_FILE,
    LOT_VALUES_FILE,
    POINTS_FILE,
    UNITS_FILE,
    assessment_units,
    lot_assessed_values,
    lot_key,
    property_assessment_roll,
)
from urban_rag.role_foncier import (
    CADASTRE_LAYER,
    JOIN_KEY,
    MONTREAL_CODE_MUN,
    POINT_LAYER,
    ROLL_LOT_COLUMN,
    ROLL_LOT_SUFFIX_COLUMN,
    UNITS_LAYER,
    USE_CODE_COLUMN,
    VALUE_COLUMN,
    RoleError,
    RoleFetcher,
    filename_for,
    layer_named,
    municipality_filter,
    read_layer,
)
from urban_rag.storage import join

DATE = "2026-08-01"
NEIGHBORHOOD = "VSMPE"
ROLL_YEAR = 2026
ARCHIVE = filename_for(ROLL_YEAR)

#: What the publisher calls the three layers this reads, roll year and all.
POINT_LAYER_NAME = f"{POINT_LAYER}_{ROLL_YEAR}"
UNITS_LAYER_NAME = f"{UNITS_LAYER}_{ROLL_YEAR}"
CADASTRE_LAYER_NAME = f"{CADASTRE_LAYER}_{ROLL_YEAR}"

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


def units_layer(
    rows: list[tuple[str, str, float]], *, use_codes: list[str | None] | None = None
) -> pd.DataFrame:
    """`b05v_unite_evaln`: the characteristics, keyed the same way.

    ``use_codes`` is parallel to ``rows`` and adds `USE_CODE_COLUMN`. Left out
    entirely by default rather than filled with a placeholder, because a roll
    without that column is a real shape this pipeline has to survive - it is
    what `assessment_units` warns about instead of failing on - and most of the
    fixtures here are about the merge rather than about the use code.
    """
    frame = pd.DataFrame(
        {
            JOIN_KEY: [unit_id(code, mat) for code, mat, _ in rows],
            "code_mun": [code for code, _, _ in rows],
            "mat18": [mat for _, mat, _ in rows],
            "rl0102a": ["REM25" for _ in rows],
            VALUE_COLUMN: [value for _, _, value in rows],
        }
    )
    if use_codes is not None:
        frame[USE_CODE_COLUMN] = use_codes
    return frame


def cadastre_layer(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    """`b05v_lot_cadst`: one row per (unit, lot the unit covers).

    Lot numbers are written the way the roll writes them - seven digits, no
    spaces - against Infolot's ``"1 000 001"``, which is the gap `lot_key`
    closes.
    """
    return pd.DataFrame(
        {
            JOIN_KEY: [unit_id(code, mat) for code, mat, _ in rows],
            "code_mun": [code for code, _, _ in rows],
            "mat18": [mat for _, mat, _ in rows],
            ROLL_LOT_COLUMN: [lot for _, _, lot in rows],
            ROLL_LOT_SUFFIX_COLUMN: [None for _ in rows],
        }
    )


def zipped_geopackage(
    directory: Path,
    points: gpd.GeoDataFrame,
    units: pd.DataFrame,
    cadastre: pd.DataFrame,
    *,
    name: str = ARCHIVE,
) -> Path:
    """Write the three layers as a real GeoPackage, zipped the way the roll is.

    The archive nests the GeoPackage under a year-stamped folder and ships a
    codebook and a PDF beside it, so the fixture does too - that is what
    `_geopackage_member` has to find its way through.
    """
    directory.mkdir(parents=True, exist_ok=True)
    gpkg = directory / f"Role_{ROLL_YEAR}_2.gpkg"
    points.to_file(gpkg, layer=POINT_LAYER_NAME, driver="GPKG")
    # Non-spatial layers: GeoPandas needs a GeoDataFrame to write one, so the
    # tables go in through pyogrio, which is what reads them back.
    pyogrio.write_dataframe(units, gpkg, layer=UNITS_LAYER_NAME, append=True)
    pyogrio.write_dataframe(cadastre, gpkg, layer=CADASTRE_LAYER_NAME, append=True)

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
        cadastre_layer([(MONTREAL_CODE_MUN, "1" * 18, "1000001")]),
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
        cadastre_layer([(MONTREAL_CODE_MUN, "1" * 18, "1000001")]),
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
        cadastre_layer([(MONTREAL_CODE_MUN, "1" * 18, "1000001")]),
    )
    fetcher, _ = fetcher_for(tmp_path, archive)
    gpkg = fetcher.geopackage(ARCHIVE)

    assert layer_named(gpkg, POINT_LAYER) == POINT_LAYER_NAME
    assert layer_named(gpkg, UNITS_LAYER) == UNITS_LAYER_NAME
    assert layer_named(gpkg, CADASTRE_LAYER) == CADASTRE_LAYER_NAME
    # And the prefix match is whole-name: `b05v_unite_evaln` must not also
    # claim `b05v_adr_unite_evaln`, which the real archive ships beside it.
    with pytest.raises(RoleError, match="no layer named"):
        layer_named(gpkg, "b05v_repar_fisc")


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
        cadastre_layer(
            [
                (MONTREAL_CODE_MUN, "1" * 18, "1000001"),
                (LAVAL_CODE_MUN, "2" * 18, "2000002"),
            ]
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


#: The four Montreal units the asset fixtures below are built from, and the
#: shape of the borough each one is there to exercise:
#:
#: * `U_PAIR_A` / `U_PAIR_B` - two units on one ordinary lot, both placed by
#:   lot number. The plain case.
#: * `U_SPLIT` - one unit covering two lots, which is what makes the full and
#:   apportioned totals differ.
#: * `U_CONDO` - a divided co-ownership: it names a private lot the cadastre
#:   does not draw, so only its point can place it, on the `PC-*` lot.
U_PAIR_A, U_PAIR_B, U_SPLIT, U_CONDO = ("1" * 18, "2" * 18, "3" * 18, "5" * 18)
U_LAVAL = "4" * 18

#: The lots those units land on. `LOT_EMPTY` is the lane nothing is assessed
#: on, and `LOT_UNDRAWN` is the private lot the condo names and Infolot has no
#: polygon for.
LOT_A, LOT_SPLIT_1, LOT_SPLIT_2 = ("1 000 001", "1 000 002", "1 000 003")
LOT_CONDO, LOT_EMPTY = ("PC-9001", "1 000 004")
LOT_UNDRAWN = "9999999"

#: The use codes the fixture's units are assessed under.
#:
#: * `CODE_DWELLING` and `CODE_GARAGE` are real MEFQ codes the fixture sheet
#:   numbers. 4611 is the one the description column exists for: "4611" and
#:   "Garage de stationnement pour automobiles" are the same fact about a
#:   parcel, and only one of them can be read.
#: * `CODE_UNKNOWN` is four digits the manual does not number - what the roll
#:   and the manual being amended on their own cadences looks like from here.
#:   It must give a null description and keep its unit, not drop it.
CODE_DWELLING, CODE_GARAGE, CODE_UNKNOWN = ("1000", "4611", "1234")

#: What the fixture codebook publishes, as `cubf_use_codes` writes it: the
#: sheet's own hierarchy rows alongside the four-character leaves. The headings
#: are there on purpose - bronze keeps them, and the merge in silver has to
#: select the leaves out. One that let "100" through would describe a unit with
#: the name of a heading, which is the failure `use_code_key` refuses to pad
#: its way into.
CUBF_ROWS: tuple[tuple[str, str | None], ...] = (
    ("1", "RÉSIDENTIELLE"),
    ("10", "LOGEMENT"),
    ("100", "Logement"),
    (CODE_DWELLING, "Logement"),
    ("46", "TERRAIN ET GARAGE DE STATIONNEMENT POUR VÉHICULES"),
    (CODE_GARAGE, "Garage de stationnement pour automobiles (infrastructure)"),
    # Numbered by the manual and left undescribed, the way 9800 really is.
    ("9800", None),
)


@pytest.fixture
def role(tmp_path, monkeypatch):
    """A borough with one of each shape, plus a Laval unit to be filtered out.

    Patched on the class rather than on an instance: Dagster rebuilds the
    resource before the run.
    """
    archive = zipped_geopackage(
        tmp_path / "src",
        points_layer(
            [
                (MONTREAL_CODE_MUN, U_PAIR_A, -73.60, 45.50),
                (MONTREAL_CODE_MUN, U_PAIR_B, -73.60, 45.50),
                (MONTREAL_CODE_MUN, U_SPLIT, -73.40, 45.50),
                (MONTREAL_CODE_MUN, U_CONDO, -73.30, 45.50),
                (LAVAL_CODE_MUN, U_LAVAL, -73.70, 45.60),
            ]
        ),
        units_layer(
            [
                (MONTREAL_CODE_MUN, U_PAIR_A, 300_000.0),
                (MONTREAL_CODE_MUN, U_PAIR_B, 200_000.0),
                (MONTREAL_CODE_MUN, U_SPLIT, 500_000.0),
                (MONTREAL_CODE_MUN, U_CONDO, 800_000.0),
                (LAVAL_CODE_MUN, U_LAVAL, 900_000.0),
            ],
            # One of each shape the codebook merge has to handle: a code the
            # manual numbers, the garage code this whole column exists to make
            # readable, a code in force that the manual has never numbered, and
            # an assessor who left the field blank.
            use_codes=[CODE_DWELLING, CODE_GARAGE, CODE_UNKNOWN, None, CODE_DWELLING],
        ),
        cadastre_layer(
            [
                (MONTREAL_CODE_MUN, U_PAIR_A, "1000001"),
                (MONTREAL_CODE_MUN, U_PAIR_B, "1000001"),
                # One unit, two lots - the value is counted whole on each in
                # `total_assessed_value` and halved in the apportioned one.
                (MONTREAL_CODE_MUN, U_SPLIT, "1000002"),
                (MONTREAL_CODE_MUN, U_SPLIT, "1000003"),
                # The condo names its private lot, which has no polygon.
                (MONTREAL_CODE_MUN, U_CONDO, LOT_UNDRAWN),
                (LAVAL_CODE_MUN, U_LAVAL, "2000002"),
            ]
        ),
    )
    fetcher, session = fetcher_for(tmp_path, archive)
    monkeypatch.setattr(RoleResource, "fetcher", lambda self: fetcher)
    return session


def borough_lots() -> gpd.GeoDataFrame:
    """The cadastre the fixture's units are placed on.

    Infolot spells a lot number with spaces and the roll does not, which is the
    gap `lot_key` closes - so these are written the way Infolot writes them.
    """
    return gpd.GeoDataFrame(
        {"NO_LOT": [LOT_A, LOT_SPLIT_1, LOT_SPLIT_2, LOT_CONDO, LOT_EMPTY]},
        geometry=[
            box(-73.61, 45.49, -73.59, 45.51),  # both U_PAIR_* points
            box(-73.41, 45.49, -73.39, 45.51),  # U_SPLIT's point
            box(-73.51, 45.49, -73.49, 45.51),  # U_SPLIT by lot number only
            box(-73.31, 45.49, -73.29, 45.51),  # U_CONDO's point
            box(-73.21, 45.49, -73.19, 45.51),  # nothing at all
        ],
        crs="EPSG:4326",
    )


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


#: The borough outline the fixture's Montreal points fall inside, and the
#: Laval one does not. Wide enough to hold all three Montreal units - which sit
#: at -73.60, -73.40 and -73.30 - and nowhere near U_LAVAL at (-73.70, 45.60).
BOROUGH = box(-73.65, 45.45, -73.25, 45.55)


def write_quartiers(store, *, geometry=None, code=None):
    """The reference layer `assign_boroughs` cuts the province against.

    As `reference_neighborhoods` writes it: one row per quartier, carrying the
    borough code `borough_code_for` resolves the partition key to.
    """
    frame = gpd.GeoDataFrame(
        {
            "no_qr": ["01"],
            "no_arr": [code or borough_code_for(NEIGHBORHOOD)],
            "nom_qr": ["Villeray"],
        },
        geometry=[geometry if geometry is not None else BOROUGH],
        crs="EPSG:4326",
    )
    write_frame(
        frame,
        join(
            store.partition_dir(reference_neighborhoods.key.path[-1], DATE),
            QUARTIERS_FILE,
        ),
    )


def write_codebook(store, rows=CUBF_ROWS, *, edition: str = "2025"):
    """The MEFQ list `assessment_units` looks its descriptions up in.

    As `cubf_use_codes` writes it - the sheet's four columns plus the
    provenance bronze stamps - so what is under test here is the same lookup
    the real partition feeds, over the same column names.
    """
    write_frame(
        pd.DataFrame(
            {
                "cubf": [code for code, _ in rows],
                "scian": [None for _ in rows],
                "description": [text for _, text in rows],
                "remarque": [None for _ in rows],
                "source_file": ["CUBF_MEFQ.xlsx" for _ in rows],
                "source_sheet": ["LISTE NUMÉRIQUE" for _ in rows],
                "mefq_edition": [edition for _ in rows],
                "scrape_date": [DATE for _ in rows],
                "scraped_at": ["2026-08-01T00:00:00+00:00" for _ in rows],
            }
        ),
        join(store.partition_dir(cubf_use_codes.key.path[-1], DATE), CUBF_FILE),
    )


@pytest.fixture(autouse=True)
def units_published(monkeypatch):
    """`silver.assessment_units` needs a database; the borough cut is recorded.

    Autouse, the same posture `test_streets` takes for its own load: every
    test here that materializes the silver merge would otherwise open a real
    connection, and none of them is about the upsert. A test that wants to read
    the cut back asks for the fixture by name.
    """
    return stub_publish_by_neighborhood(monkeypatch, role_assets)


def run_units(store, *, quartiers: bool = True, codebook: bool = True):
    """Materialize `assessment_units`, over the boundary it cuts the roll on.

    The quartiers are written here rather than in every test because most of
    these are about the merge and not about the cut, and an asset that cannot
    find a boundary fails before it merges anything. Only when there is none
    already, so a test that wants a different outline writes it first;
    ``quartiers=False`` is for the one that wants none at all.

    The codebook is written on the same terms and for the same reason: the
    asset now looks every unit's use code up in it, so a partition without one
    fails before it merges anything. ``codebook=False`` is for the test that
    wants that failure.
    """
    written = join(
        store.partition_dir(reference_neighborhoods.key.path[-1], DATE),
        QUARTIERS_FILE,
    )
    if quartiers and not Path(written).exists():
        write_quartiers(store)
    listed = join(
        store.partition_dir(cubf_use_codes.key.path[-1], DATE), CUBF_FILE
    )
    if codebook and not Path(listed).exists():
        write_codebook(store)
    return materialize(
        [assessment_units],
        partition_key=DATE,
        resources={"store": store, "postgis": PostgisResource()},
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


def run_lot_values(store, *, by_point: bool = True):
    return materialize(
        [lot_assessed_values],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        run_config={
            "ops": {
                "silver__lot_assessed_values": {
                    "config": {"place_unmatched_by_point": by_point}
                }
            }
        },
    )


def lot_values(store) -> gpd.GeoDataFrame:
    """The partition `lot_assessed_values` just wrote, indexed by lot number."""
    return gpd.read_parquet(
        Path(
            store.partition_dir(lot_assessed_values.key.path[-1], DATE, NEIGHBORHOOD)
        )
        / LOT_VALUES_FILE
    ).set_index("NO_LOT")


def test_bronze_writes_all_three_layers_under_the_scrape_date(store, role):
    result = run_roll(store)

    assert result.success
    output_dir = Path(
        store.partition_dir(property_assessment_roll.key.path[-1], DATE)
    )
    assert (output_dir / POINTS_FILE).exists()
    assert (output_dir / UNITS_FILE).exists()
    assert (output_dir / CADASTRE_FILE).exists()

    points = gpd.read_parquet(output_dir / POINTS_FILE)
    # Filtered in OGR, so Laval never reaches the frame.
    assert len(points) == 4
    assert set(points["code_mun"]) == {MONTREAL_CODE_MUN}
    assert points.crs.to_string() == "EPSG:4326"
    # The path holds bare keys, so the snapshot travels as columns.
    assert set(points["scrape_date"]) == {DATE}
    assert set(points["roll_year"]) == {ROLL_YEAR}
    assert set(points["source_layer"]) == {POINT_LAYER_NAME}

    # One row per (unit, lot), so the crosswalk is longer than the unit list.
    crosswalk = pd.read_parquet(output_dir / CADASTRE_FILE)
    assert len(crosswalk) == 5
    assert set(crosswalk["source_layer"]) == {CADASTRE_LAYER_NAME}

    metadata = materialization_metadata(result, property_assessment_roll)
    assert metadata["num_assessment_points"].value == 4
    assert metadata["num_characteristics_rows"].value == 4
    assert metadata["num_lot_crosswalk_rows"].value == 5
    assert metadata["num_cadastre_lots"].value == 4
    assert metadata["municipality_filter"].value == "code_mun IN ('66023')"


def test_bronze_keeps_the_province_when_no_code_is_given(store, role):
    result = run_roll(store, codes=())

    assert result.success
    metadata = materialization_metadata(result, property_assessment_roll)
    assert metadata["num_assessment_points"].value == 5
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
    assert len(merged) == 4
    # The value column is what the merge exists to bring across.
    assert merged[VALUE_COLUMN].sum() == 1_800_000.0
    # `code_mun` and `mat18` are published in both and are dropped from the
    # right-hand side rather than suffixed, since they say the same thing.
    assert "code_mun_unite" not in merged.columns
    assert "mat18_unite" not in merged.columns
    # `rl0102a` is the characteristics table's own name for `arrond`, so both
    # survive: bronze vocabulary is not reconciled away.
    assert {"arrond", "rl0102a"} <= set(merged.columns)

    metadata = materialization_metadata(result, assessment_units)
    assert metadata["num_assessment_units"].value == 4
    assert metadata["num_points_unmatched"].value == 0
    assert metadata["total_assessed_value"].value == 1_800_000.0


def described(store) -> pd.Series:
    """The merged partition's use description, indexed by matricule."""
    merged = gpd.read_parquet(
        Path(store.partition_dir(assessment_units.key.path[-1], DATE))
        / ASSESSMENT_UNITS_FILE
    )
    return merged.set_index("mat18")[USE_DESCRIPTION_COLUMN]


def test_silver_gives_the_use_code_the_manuals_words(store, role):
    """The whole point of the codebook: 4611 becomes a parking garage."""
    run_roll(store)

    result = run_units(store)

    assert result.success
    text = described(store)
    assert text[U_PAIR_A] == "Logement"
    assert (
        text[U_PAIR_B] == "Garage de stationnement pour automobiles (infrastructure)"
    )


def test_silver_keeps_a_unit_whose_code_the_manual_does_not_number(store, role):
    """A code in force before its edition lands is a null, not a lost property.

    The roll and the MEFQ are amended on their own cadences, so this is an
    ordinary state rather than an error - and the run has to *name* the code,
    because "which one" is the only useful thing about it.
    """
    run_roll(store)

    result = run_units(store)

    text = described(store)
    assert pd.isna(text[U_SPLIT])
    # Still four units: the lookup is a left join and drops nothing.
    assert len(text) == 4

    metadata = materialization_metadata(result, assessment_units)
    assert metadata["num_use_codes_not_in_the_manual"].value == 1
    assert CODE_UNKNOWN in metadata["use_codes_not_in_the_manual"].value
    # Three of the four units state a code at all; the condo's is blank.
    assert metadata["num_units_with_a_use_code"].value == 3
    assert metadata["num_units_described"].value == 2
    assert metadata["mefq_edition"].value == "2025"


def test_silver_describes_nothing_for_a_unit_the_assessor_left_blank(store, role):
    """A null `rl0105a` is a property nobody classified, and stays one."""
    run_roll(store)

    run_units(store)

    assert pd.isna(described(store)[U_CONDO])


def test_silver_never_describes_a_unit_with_a_hierarchy_heading(store, role):
    """The sheet's headings are not use codes, however code-shaped they look.

    `100` is the *Logement* subgroup and `10` the rubric above it. A lookup
    that left-padded either to four characters would hand a unit the name of a
    heading, which reads exactly like a real answer.
    """
    run_roll(store)
    write_codebook(store, rows=(("100", "Logement"), ("10", "LOGEMENT")))

    run_units(store)

    assert described(store).isna().all()


def test_silver_names_the_codebook_to_materialize_when_it_is_missing(store, role):
    run_roll(store)

    with pytest.raises(Failure, match="cubf_use_codes"):
        run_units(store, codebook=False)


def test_silver_survives_a_roll_that_states_no_use_code_at_all(
    store, tmp_path, monkeypatch
):
    """No `rl0105a` column is a warning and a null column, not a failure.

    The consequence of an unclassifiable roll lands in
    `lot_assessment_comparables`, which already counts the floor it could not
    price; refusing the merge here would cost the partition instead.
    """
    archive = zipped_geopackage(
        tmp_path / "src",
        points_layer([(MONTREAL_CODE_MUN, U_PAIR_A, -73.60, 45.50)]),
        units_layer([(MONTREAL_CODE_MUN, U_PAIR_A, 300_000.0)]),
        cadastre_layer([(MONTREAL_CODE_MUN, U_PAIR_A, "1000001")]),
    )
    fetcher, _ = fetcher_for(tmp_path, archive)
    monkeypatch.setattr(RoleResource, "fetcher", lambda self: fetcher)
    run_roll(store)

    result = run_units(store)

    assert result.success
    assert described(store).isna().all()
    metadata = materialization_metadata(result, assessment_units)
    assert metadata["num_units_with_a_use_code"].value == 0
    assert metadata["num_use_codes_not_in_the_manual"].value == 0


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


def test_silver_publishes_a_borough_partition_for_each_boundary_hit(
    store, role, units_published
):
    """The roll has no borough axis, so this is where the province is cut.

    One date partition in the tree, one Postgres partition per borough, and the
    borough is the one whose outline the unit's point falls inside - not the
    `arrond` the roll states, which travels alongside as a cross-check.
    """
    run_roll(store)

    result = run_units(store)

    assert result.success
    assert units_published["dataset"] == "assessment_units"
    assert units_published["scrape_date"] == DATE
    # All three Montreal points are inside `BOROUGH`; the Laval one was
    # filtered out in bronze.
    assert set(units_published["frames"]) == {NEIGHBORHOOD}
    published = units_published["frames"][NEIGHBORHOOD]
    assert len(published) == 4
    assert set(published["neighborhood"]) == {NEIGHBORHOOD}
    # The spatial join's bookkeeping column is not a column of the table, and
    # would otherwise land in `attributes` as noise.
    assert "index_right" not in published.columns

    metadata = materialization_metadata(result, assessment_units)
    assert metadata["num_units_in_a_borough"].value == 4
    assert metadata["num_units_outside_every_borough"].value == 0
    assert metadata[f"{NEIGHBORHOOD}_rows_upserted"].value == 4


def test_silver_keeps_the_province_in_the_tree_and_the_boroughs_in_the_table(
    store, role, units_published
):
    """The parquet and the table do not hold the same rows, on purpose.

    Westmount and Laval file rolls too and are not boroughs, so a run that kept
    the province writes every unit to the tree and publishes only the ones that
    fell in a borough. The difference is a count rather than an error.
    """
    run_roll(store, codes=())

    result = run_units(store)

    merged = gpd.read_parquet(
        Path(store.partition_dir(assessment_units.key.path[-1], DATE))
        / ASSESSMENT_UNITS_FILE
    )
    assert len(merged) == 5  # the Laval unit is in the tree
    assert len(units_published["frames"][NEIGHBORHOOD]) == 4  # and not in the table

    metadata = materialization_metadata(result, assessment_units)
    assert metadata["num_assessment_units"].value == 5
    assert metadata["num_units_in_a_borough"].value == 4
    assert metadata["num_units_outside_every_borough"].value == 1


def test_silver_counts_where_the_roll_and_the_map_disagree_on_the_borough(
    store, role, units_published
):
    """Two agencies, one question. The geometry decides and the count says so.

    The fixture's points carry no `arrond` of their own, so this drives the
    disagreement from the other side: the outline is filed under a borough code
    the units were never going to match.
    """
    run_roll(store)
    bronze = Path(store.partition_dir(property_assessment_roll.key.path[-1], DATE))
    points = gpd.read_parquet(bronze / POINTS_FILE)
    # Three units the roll files in this borough, one it files next door.
    points["arrond"] = [
        f"{ARROND_PREFIX}{borough_code_for(NEIGHBORHOOD)}"
    ] * 3 + [f"{ARROND_PREFIX}99"]
    write_frame(points, str(bronze / POINTS_FILE))

    result = run_units(store)

    assert result.success
    # Placed by geometry regardless - all four points are inside the outline.
    assert len(units_published["frames"][NEIGHBORHOOD]) == 4
    metadata = materialization_metadata(result, assessment_units)
    assert metadata["num_units_arrond_disagrees"].value == 1


def test_silver_names_reference_neighborhoods_when_the_boundary_is_missing(
    store, role
):
    """The cut has no boundary to make, and the message says which asset owns it."""
    run_roll(store)

    with pytest.raises(Failure, match="materialize reference_neighborhoods"):
        run_units(store, quartiers=False)


def test_silver_refuses_a_cut_that_placed_no_unit_in_any_borough(store, role):
    """A boundary that holds none of them is a boundary, not a fact about Quebec.

    The same refusal `neighborhood_streets` makes when nothing intersects the
    borough. The parquet is written first either way, so the re-run costs the
    merge rather than the 572 MB download.
    """
    run_roll(store)
    write_quartiers(store, geometry=box(-70.0, 40.0, -69.0, 41.0))

    with pytest.raises(Failure, match="no assessment unit fell inside"):
        run_units(store)

    assert (
        Path(store.partition_dir(assessment_units.key.path[-1], DATE))
        / ASSESSMENT_UNITS_FILE
    ).exists()

def test_lot_values_place_units_by_the_rolls_own_lot_numbers(store, role, published):
    """The roll says which lots a property covers; that is what is used.

    `LOT_SPLIT_2` is the case worth watching: no unit's point falls inside it,
    and it is valued anyway, because `U_SPLIT` names it in the crosswalk.
    """
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots())

    result = run_lot_values(store)

    assert result.success
    values = lot_values(store)

    # Two units on one lot, both by lot number, neither shared.
    assert values.loc[LOT_A, "num_assessment_units"] == 2
    assert values.loc[LOT_A, "total_assessed_value"] == 500_000.0
    assert values.loc[LOT_A, "total_assessed_value_apportioned"] == 500_000.0
    assert values.loc[LOT_A, "num_shared_units"] == 0
    assert values.loc[LOT_A, "num_units_by_point"] == 0

    # Valued off the crosswalk alone - no point falls in it. A spatial join
    # would have left this lot empty.
    assert values.loc[LOT_SPLIT_2, "num_assessment_units"] == 1
    assert values.loc[LOT_SPLIT_2, "num_units_by_point"] == 0

    assert set(values["neighborhood"]) == {NEIGHBORHOOD}
    assert set(values["scrape_date"]) == {DATE}

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_lots"].value == 5
    assert metadata["num_lots_valued"].value == 4
    assert metadata["num_units_by_lot_number"].value == 3
    assert metadata["num_units_matched"].value == 4


def test_a_unit_on_several_lots_is_whole_on_each_and_split_across_them(
    store, role, published
):
    """The two totals, and the one number that says where they differ.

    `U_SPLIT` is worth $500,000 and covers two lots. Its whole value is on each
    of them, because that is what "the property on this lot is assessed at"
    means; the apportioned column halves it, because that is what adds up.
    """
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots())

    result = run_lot_values(store)
    values = lot_values(store)

    for lot in (LOT_SPLIT_1, LOT_SPLIT_2):
        assert values.loc[lot, "num_assessment_units"] == 1
        assert values.loc[lot, "num_shared_units"] == 1
        assert values.loc[lot, "total_assessed_value"] == 500_000.0
        assert values.loc[lot, "total_assessed_value_apportioned"] == 250_000.0

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_units_on_several_lots"].value == 1
    # 300k + 200k on LOT_A, 500k twice over the split pair, 800k on the condo.
    assert metadata["total_assessed_value"].value == 2_300_000.0
    # The same, counting the split unit once: this is the one that adds up.
    assert metadata["total_assessed_value_apportioned"].value == 1_800_000.0


def test_a_condominium_is_placed_by_its_point_because_its_lots_are_undrawn(
    store, role, published
):
    """The fallback, and the reason it exists.

    `U_CONDO` names a private lot the cadastre has no polygon for - which is
    every divided co-ownership in a real borough - so the crosswalk cannot
    place it and only its point can, on the `PC-*` common-parts lot.
    """
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots())

    result = run_lot_values(store)
    values = lot_values(store)

    assert values.loc[LOT_CONDO, "num_assessment_units"] == 1
    assert values.loc[LOT_CONDO, "num_units_by_point"] == 1
    assert values.loc[LOT_CONDO, "total_assessed_value"] == 800_000.0
    # Placed by a point, so it sits on one lot and the two totals agree.
    assert values.loc[LOT_CONDO, "total_assessed_value_apportioned"] == 800_000.0

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_units_by_point"].value == 1
    assert metadata["placed_unmatched_by_point"].value is True


def test_the_point_fallback_can_be_switched_off(store, role, published):
    """Off, every row comes from the roll's own statement of which lots it
    covers - and the condominium is absent rather than approximated."""
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots())

    result = run_lot_values(store, by_point=False)
    values = lot_values(store)

    assert values.loc[LOT_CONDO, "num_assessment_units"] == 0
    assert pd.isna(values.loc[LOT_CONDO, "total_assessed_value"])
    assert int(values["num_units_by_point"].sum()) == 0

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_units_by_point"].value == 0
    assert metadata["num_lots_valued"].value == 3
    assert metadata["placed_unmatched_by_point"].value is False
    assert metadata["total_assessed_value"].value == 1_500_000.0


def test_a_lot_nothing_is_assessed_on_keeps_its_row_with_a_null_total(
    store, role, published
):
    """A lane is not worth nothing; it is unassessed, and the two read
    differently to anyone averaging over the borough."""
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots())

    result = run_lot_values(store)
    values = lot_values(store)

    assert values.loc[LOT_EMPTY, "num_assessment_units"] == 0
    assert pd.isna(values.loc[LOT_EMPTY, "total_assessed_value"])
    assert pd.isna(values.loc[LOT_EMPTY, "total_assessed_value_apportioned"])

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_lots_unvalued"].value == 1


def test_the_lot_key_closes_the_gap_between_the_two_spellings():
    """Infolot writes "1 000 001", the roll writes "1000001"."""
    assert lot_key("1 000 001") == "1000001"
    assert lot_key("1000001") == "1000001"
    # A no-break space is what French thousands separators are often published
    # with, and one left in would make the key miss silently.
    assert lot_key("1 000 001") == "1000001"
    # The PC- prefix is kept: the roll has no such lot, so the key is meant to
    # miss rather than be coerced into matching a numbered one.
    assert lot_key("PC-9001") == "PC-9001"
    assert lot_key(None) is None
    assert lot_key("  ") is None


def test_the_suffix_does_not_split_one_lot_into_two(store, role, published, tmp_path):
    """`rl0103b` distinguishes non-renewed rows naming one renewed lot.

    Left in the key it would count a unit twice on the same lot - 1,758 of
    Montreal's crosswalk rows are exactly that shape.
    """
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots())
    # Give U_PAIR_A a second crosswalk row for the same lot, under a suffix.
    cadastre_path = join(
        store.partition_dir(property_assessment_roll.key.path[-1], DATE),
        CADASTRE_FILE,
    )
    crosswalk = pd.read_parquet(cadastre_path)
    repeated = crosswalk[crosswalk[JOIN_KEY] == unit_id(MONTREAL_CODE_MUN, U_PAIR_A)]
    repeated = repeated.assign(**{ROLL_LOT_SUFFIX_COLUMN: "1"})
    write_frame(pd.concat([crosswalk, repeated], ignore_index=True), cadastre_path)

    run_lot_values(store)
    values = lot_values(store)

    # Still two units and still $500,000: the duplicate row was dropped, not
    # counted.
    assert values.loc[LOT_A, "num_assessment_units"] == 2
    assert values.loc[LOT_A, "total_assessed_value"] == 500_000.0


def test_lot_values_publish_the_frame_they_wrote(store, role, published):
    """`silver.lot_assessed_values` is this asset's own table.

    Published after the parquet and from the same frame, so the file and the
    serving copy cannot say different things about a partition.
    """
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots())

    result = run_lot_values(store)

    assert published["calls"] == 1
    assert published["partition"] == (NEIGHBORHOOD, DATE)
    assert set(published["datasets"]) == {"lot_assessed_values"}
    assert len(published["datasets"]["lot_assessed_values"]) == 5
    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["lot_assessed_values_rows_upserted"].value == 5


def test_lot_values_refuse_a_duplicated_lot(store, role, published):
    run_roll(store)
    run_units(store)
    lots = borough_lots()
    write_lots(store, pd.concat([lots, lots.iloc[:1]], ignore_index=True))

    with pytest.raises(Failure, match="appear more than once"):
        run_lot_values(store)


def test_lot_values_refuse_a_partition_with_no_overlap_at_all(store, role, published):
    run_roll(store)
    run_units(store)
    # Lots that no unit names and no point falls in: the two sides do not meet,
    # which is a filter or a partition mistake rather than a borough of lanes.
    write_lots(
        store,
        gpd.GeoDataFrame(
            {"NO_LOT": ["8 000 009"]},
            geometry=[box(10, 10, 11, 11)],
            crs="EPSG:4326",
        ),
    )

    with pytest.raises(Failure, match="No assessment unit could be placed"):
        run_lot_values(store)


def test_lot_values_report_units_the_borough_holds_none_of(store, role, published):
    run_roll(store)
    run_units(store)
    write_lots(store, borough_lots().iloc[:1])  # LOT_A only

    result = run_lot_values(store)

    metadata = materialization_metadata(result, lot_assessed_values)
    assert metadata["num_units_matched"].value == 2
    # U_SPLIT and U_CONDO are elsewhere in the snapshot, and this partition
    # attributes them to nobody.
    assert metadata["num_units_unmatched_in_snapshot"].value == 2
