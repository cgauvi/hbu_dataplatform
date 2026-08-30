"""Offline test for `lot_profiles`, the gold asset over the lot lineage.

`postgis.compute_lot_profiles` is Postgres-only in substance - one INSERT over
three grouped joins - so nothing here touches a database. What is worth testing
without one is the asset's own logic: that the threshold reaches the query, how
the result turns into metadata, that the answer lands as geoparquet under
`gold/`, that an unloaded partition fails with what it means rather than with a
well-formed zero, and that a missing hbu_infra relation fails naming the file
to apply.

The one behaviour that is the whole reason this asset replaced `vacant_lots` is
covered too: a lot with a building on it is kept rather than filtered out, so
the row count is the borough's whole cadastre and the vacant question is a
column.

Four of its inputs *are* testable without a database, because they never reach
one as tables: `lot_zoning_envelopes`, `vacancy_rates`, `average_rents` and the
two bronze cost snapshots are read out of the tree and turned into jsonb by
this module, so what the query is handed is the asset's own work. That is what
replaced `lots_with_vacancy_rates`, and it is covered here.

The cost snapshots differ from the other three in one way worth testing: they
are partitioned by date alone, because the guide prices nine Canadian markets
and knows nothing about boroughs. Every borough of a day reads the same two
files, and a missing one names a date rather than a borough.

The asset cannot run for real yet: `rag.lot_profiles` and the
`rag.lot_documents` view are hbu_infra's, and sql/006 only lands on a `db.py
init` run after a corpus has been indexed. See the module docstring of
`urban_rag.lot_profiles_assets`.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import geopandas as gpd
import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from shapely.geometry import box

from asset_helpers import materialization_metadata

from urban_rag import lot_profiles_assets
from urban_rag.cmhc_assets import (
    AVERAGE_RENTS_FILE,
    VACANCY_FILE,
    average_rents,
    vacancy_rates,
)
from urban_rag.envelope_assets import LOT_ENVELOPES_FILE, lot_zoning_envelopes
from urban_rag.estimator_assets import (
    NON_RESIDENTIAL_FILE,
    RESIDENTIAL_FILE,
    montreal_nonresidential_costs,
    montreal_residential_costs,
)
from urban_rag.frames import write_frame
from urban_rag.lot_profiles_assets import LOT_PROFILES_FILE, lot_profiles
from urban_rag.postgis import DEFAULT_MAX_BUILT_AREA_M2, MissingRelation
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import join

DATE = "2026-08-20"
NEIGHBORHOOD = "VSMPE"


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


def write_envelopes(store, *, lot_numbers=("1", "2"), pct_of_lot=(92.0, 100.0)):
    """`lot_zoning_envelopes` as its asset writes it: one row per (lot, column).

    Shaped like the real file rather than faithful to all fifty of its columns -
    what this module does with it is decide which columns travel into the jsonb
    and in what order, and those are the ones here.
    """
    frame = pd.DataFrame(
        {
            "lot_uid": list(range(1, len(lot_numbers) + 1)),
            "lot_number": list(lot_numbers),
            "neighborhood": [NEIGHBORHOOD] * len(lot_numbers),
            "scrape_date": [DATE] * len(lot_numbers),
            "lot_area_m2": [400.0] * len(lot_numbers),
            "primary_frontage_m": [20.0] * len(lot_numbers),
            "feature_id": [f"C01-{i:03d}" for i in range(1, len(lot_numbers) + 1)],
            "pct_of_lot": list(pct_of_lot),
            "column_index": [0] * len(lot_numbers),
            "usages": ['["H.1"]'] * len(lot_numbers),
            "levels": ['["1", "2"]'] * len(lot_numbers),
            "parse_notes": ["[]"] * len(lot_numbers),
            "floors_max": [3] * len(lot_numbers),
            "min_lot_width_m": [None] * len(lot_numbers),
            "permits_residential": [True] * len(lot_numbers),
            "governs_residential": [True] * len(lot_numbers),
            "solver_ready": [True] * len(lot_numbers),
        }
    )
    write_frame(
        frame,
        join(
            store.partition_dir(
                lot_zoning_envelopes.key.path[-1], DATE, NEIGHBORHOOD
            ),
            LOT_ENVELOPES_FILE,
        ),
    )


def write_vacancy(store, *, overall=0.5):
    frame = pd.DataFrame(
        {
            "neighborhood": [NEIGHBORHOOD] * 2,
            "scrape_date": [DATE] * 2,
            "dwelling_type": ["all", "apartment_other"],
            "bedroom_type": ["all", "2_bedroom"],
            "vacancy_rate_pct": [overall, None],
            "min_vacancy_rate_pct": [0.3, None],
            "max_vacancy_rate_pct": [0.7, None],
            "num_quartiers": [2, 0],
            "num_quartiers_mapped": [3, 3],
            "averaged_quartiers": ["Parc-Extension, Villeray", ""],
            "survey_year": [2023, 2023],
            "survey_period": ["octobre 2023", "octobre 2023"],
        }
    )
    write_frame(
        frame,
        join(
            store.partition_dir(vacancy_rates.key.path[-1], DATE, NEIGHBORHOOD),
            VACANCY_FILE,
        ),
    )


def write_rents(store, *, overall=1_275.0):
    frame = pd.DataFrame(
        {
            "neighborhood": [NEIGHBORHOOD] * 2,
            "scrape_date": [DATE] * 2,
            "bedroom_type": ["all", "2_bedroom"],
            "average_rent_cad": [overall, None],
            "min_average_rent_cad": [1_100.0, None],
            "max_average_rent_cad": [1_400.0, None],
            "num_quartiers": [3, 0],
            "num_quartiers_mapped": [3, 3],
            "averaged_quartiers": ["Parc-Extension, Petite-Patrie, Villeray", ""],
            "survey_year": [2023, 2023],
            "survey_period": ["octobre 2023", "octobre 2023"],
        }
    )
    write_frame(
        frame,
        join(
            store.partition_dir(average_rents.key.path[-1], DATE, NEIGHBORHOOD),
            AVERAGE_RENTS_FILE,
        ),
    )


#: The Montreal column of the Altus guide, as `urban_rag.estimator` publishes
#: it, for the types a lot profile carries a rate for. Real figures rather than
#: round ones, so a test that asserts a number is asserting the guide's number:
#: `parkade_ug` and `parkade_ag` are the two the parking columns come from, and
#: their midpoints are what `program.py` hardcodes today.
PARKING_RATES = {
    "parkade_ug": ("Parking - Underground Garage", 51_925.0, 68_675.0),
    "parkade_ag": ("Parking - Above Grade Garage", 38_500.0, 57_750.0),
    # Priced per stall like the two above and deliberately never read: an
    # asphalt lot is not a parking structure.
    "surface_lot": ("Parking - Surface Lot", 3_960.0, 8_250.0),
}
CONDO_RATES = {
    "condo_wood": ("Wood Frame Condo (Up to 6 Storeys)", 225.0, 290.0),
    "condo_12": ("Condominium / Apartment (Up to 12 Storeys)", 275.0, 335.0),
    "condo_13_39": ("Condominium / Apartment (13-39 Storeys)", 320.0, 330.0),
    "condo_40_60": ("Condominium / Apartment (40-60 Storeys)", 330.0, 375.0),
    "condo_60plus": ("Condominium / Apartment (60+ Storeys)", 330.0, 425.0),
}

LAST_MODIFIED = "Tue, 12 Aug 2026 09:31:00 GMT"


def _costs_frame(rates, *, cat, unit_flag):
    """One bronze cost snapshot, shaped the way `estimator.rates_frame` writes
    it - the publisher's own column names, plus the provenance columns the
    asset adds through `extra_columns`."""
    ids = list(rates)
    return pd.DataFrame(
        {
            "id": ids,
            "label": [rates[i][0] for i in ids],
            "sector": ["private"] * len(ids),
            "cat": [cat] * len(ids),
            "unit_flag": [unit_flag] * len(ids),
            "sourceNote": [None] * len(ids),
            "city": ["mtl"] * len(ids),
            "city_label": ["Montreal"] * len(ids),
            "prov": ["QC"] * len(ids),
            "rate_low": [rates[i][1] for i in ids],
            "rate_high": [rates[i][2] for i in ids],
            "scrape_date": [DATE] * len(ids),
            "scraped_at": ["2026-08-20T12:00:00+00:00"] * len(ids),
            "source_url": [
                "https://zef-builds.github.io/construction-estimator/"
                "data/building-types.js"
            ]
            * len(ids),
            "source_last_modified": [LAST_MODIFIED] * len(ids),
        }
    )


def write_costs(store, *, parking=None, condos=None):
    """The two bronze cost snapshots, under `<date>/` with no borough.

    The guide prices nine Canadian markets and knows nothing about boroughs, so
    these are partitioned by date alone and every borough of that day reads the
    same two files - which is exactly what the paths here assert.
    """
    parking = PARKING_RATES if parking is None else parking
    condos = CONDO_RATES if condos is None else condos

    # The non-residential snapshot holds commercial and industrial types too;
    # only the parking ones are ever read, and one commercial row is here to
    # make sure the read is a selection rather than "whatever the file holds".
    non_residential = pd.concat(
        [
            _costs_frame(parking, cat="parking", unit_flag="perStall"),
            _costs_frame(
                {"office_a": ("Office - Class A", 340.0, 425.0)},
                cat="commercial",
                unit_flag=None,
            ),
        ],
        ignore_index=True,
    )
    write_frame(
        non_residential,
        join(
            store.partition_dir(montreal_nonresidential_costs.key.path[-1], DATE),
            NON_RESIDENTIAL_FILE,
        ),
    )

    # Same on the residential side: townhouses share the category and are not
    # what "an apartment building on this lot" means.
    residential = pd.concat(
        [
            _costs_frame(condos, cat="residential", unit_flag=None),
            _costs_frame(
                {"townhouse_row": ("Row Townhouse", 145.0, 195.0)},
                cat="residential",
                unit_flag=None,
            ),
        ],
        ignore_index=True,
    )
    write_frame(
        residential,
        join(
            store.partition_dir(montreal_residential_costs.key.path[-1], DATE),
            RESIDENTIAL_FILE,
        ),
    )


def write_partition(store):
    """The five upstream files this asset reads out of the tree."""
    write_envelopes(store)
    write_vacancy(store)
    write_rents(store)
    write_costs(store)


def stub_postgis(
    monkeypatch,
    *,
    num_lots=10,
    with_building=6,
    with_frontage=8,
    with_secondary=2,
    with_documents=7,
    with_assessed_value=5,
    roll_year=2026,
    compute_raises=None,
):
    """Patched on the class: Dagster rebuilds the resource before the run.

    ``num_lots`` is both the cadastre's size and the number of profiles, which
    is the invariant this asset is built on - every lot gets a row, so the two
    only differ if the INSERT dropped something.
    """
    calls: dict[str, object] = {}

    @contextmanager
    def connect(self):
        yield object()

    def compute_lot_profiles(
        connection,
        *,
        neighborhood,
        scrape_date,
        max_built_area_m2,
        vacancy_rates=None,
        average_rents=None,
        construction_costs=None,
        zoning_envelopes=(),
    ):
        calls["compute"] = (neighborhood, scrape_date, max_built_area_m2)
        calls["vacancy_rates"] = vacancy_rates
        calls["average_rents"] = average_rents
        calls["construction_costs"] = construction_costs
        calls["zoning_envelopes"] = list(zoning_envelopes)
        if compute_raises is not None:
            raise compute_raises
        without_building = num_lots - with_building
        staged = len(calls["zoning_envelopes"])
        return {
            "profiles": num_lots,
            "pruned": 0,
            "num_lots": num_lots,
            "num_profiles": num_lots,
            "by_category": {
                "built": with_building,
                "no_building": without_building,
                "shed_only": 0,
                "building_sliver": 0,
            },
            "area_by_category": {
                "built": 2_400.0,
                "no_building": 20_000.0,
                "shed_only": 0.0,
                "building_sliver": 0.0,
            },
            "num_with_building": with_building,
            "num_without_building": without_building,
            "num_with_frontage": with_frontage,
            "num_with_secondary_frontage": with_secondary,
            "num_with_documents": with_documents,
            "num_envelopes_staged": staged,
            "num_zoning_envelopes": staged,
            "num_with_zoning_envelopes": min(staged, num_lots),
            # The setback join, read back out of gold.lot_profiles by the real
            # function. A lot only gets a buildable area where it had both an
            # envelope to take margins from and a frontage row to sort its
            # boundary against, so the stub bounds it by the envelope count the
            # way the table does.
            "num_with_buildable_area": min(staged, with_frontage),
            "num_bound_by_setbacks": min(staged, with_frontage),
            "mean_buildable_pct_of_lot": 41.5,
            "has_vacancy_rates": bool(vacancy_rates),
            "has_average_rents": bool(average_rents),
            "overall_vacancy_rate_pct": (vacancy_rates or {}).get(
                "overall_vacancy_rate_pct"
            ),
            "overall_average_rent_cad": (average_rents or {}).get(
                "overall_average_rent_cad"
            ),
            "has_construction_costs": bool(construction_costs),
            # Read back out of the table by the real function, so the stub
            # answers from the payload the way the table would: a key the
            # payload never set is a NULL column, not a zero.
            **{
                column: (construction_costs or {}).get(column)
                for column in (
                    "underground_stall_cost_low_cad",
                    "underground_stall_cost_high_cad",
                    "above_grade_stall_cost_low_cad",
                    "above_grade_stall_cost_high_cad",
                    "condo_cost_low_cad_sqft",
                    "condo_cost_high_cad_sqft",
                )
            },
            # The assessment join, read back out of gold.lot_profiles by the
            # real function. `None` for the total rather than 0.0 when no lot
            # carries one - a borough whose roll has not landed is not a
            # borough whose ground is worth nothing.
            "num_with_assessed_value": with_assessed_value,
            "num_assessment_units": with_assessed_value * 3,
            # A borough-scale figure rather than one scaled off the lot
            # count, because the asset reports it in billions rounded to two
            # places and a stub total of a few million would round to 0.0 and
            # test nothing. $29.4B is the first VSMPE snapshot's apportioned
            # total.
            "total_assessed_value_apportioned": (
                29_400_000_000.0 if with_assessed_value else None
            ),
            "roll_year": roll_year,
            # The comparables join, read back the same way. A lot gets
            # neighbours from its size and its ground alone and needs a priced
            # income to get a rate, so the stub keeps the second under the
            # first the way the table does - and both under the lots the roll
            # actually reached, since only a valued lot can be a comparable.
            "num_with_comparables": num_lots,
            "num_with_cap_rate": with_assessed_value,
            "median_cap_rate_pct": 4.25 if with_assessed_value else None,
            # Under 1: a triennial roll sitting below what its own comparables
            # imply is what a rising market looks like from here.
            "median_assessed_to_estimated_ratio": (
                0.82 if with_assessed_value else None
            ),
            "net_operating_income_cad": 1_450_000.0 if with_assessed_value else 0.0,
            "num_buildings": with_building * 2,
            "total_lot_area_m2": 22_400.0,
            "max_primary_frontage_m": 31.25,
            "mean_primary_frontage_m": 12.34,
        }

    def fetch_lot_profiles(connection, *, neighborhood, scrape_date):
        calls["fetch"] = (neighborhood, scrape_date)
        built = [index < with_building for index in range(num_lots)]
        return gpd.GeoDataFrame(
            {
                "lot_uid": list(range(1, num_lots + 1)),
                "lot_number": [str(index) for index in range(1, num_lots + 1)],
                "neighborhood": [neighborhood] * num_lots,
                "scrape_date": [scrape_date] * num_lots,
                "lot_area_m2": [400.0] * num_lots,
                "has_building": built,
                "num_buildings": [2 if flag else 0 for flag in built],
                "category": [
                    "built" if flag else "no_building" for flag in built
                ],
                "primary_frontage_m": [
                    20.0 if index < with_frontage else None
                    for index in range(num_lots)
                ],
                "secondary_frontage_m": [
                    8.0 if index < with_secondary else None
                    for index in range(num_lots)
                ],
                "num_documents": [
                    1 if index < with_documents else 0 for index in range(num_lots)
                ],
                "doc_url": [
                    "https://example.test/grille.pdf" if index < with_documents
                    else None
                    for index in range(num_lots)
                ],
                # Written as a JSON string rather than a nested type, so a
                # partition where no lot has a document types the column the
                # same way as one where they all do.
                "documents": [
                    json.dumps(
                        [{"doc_id": "C01-001", "url": "https://example.test/g.pdf"}]
                    )
                    if index < with_documents
                    else "[]"
                    for index in range(num_lots)
                ],
            },
            geometry=[box(0, 0, 1, 1)] * num_lots,
            crs="EPSG:4326",
        )

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(
        lot_profiles_assets, "compute_lot_profiles", compute_lot_profiles
    )
    monkeypatch.setattr(
        lot_profiles_assets, "fetch_lot_profiles", fetch_lot_profiles
    )
    return calls


def run(store, **config):
    """Materialize the asset over whatever the test wrote.

    The three silver inputs are read before Postgres is touched, so a test that
    is about the query still needs them on disk; `write_partition` fills them
    in unless the test wrote its own first.
    """
    return materialize(
        [lot_profiles],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        selection=[lot_profiles],
        run_config=(
            {"ops": {"gold__lot_profiles": {"config": config}}} if config else None
        ),
    )


def test_the_answer_lands_as_geoparquet_under_gold(store, monkeypatch):
    write_partition(store)
    calls = stub_postgis(monkeypatch, num_lots=10)

    assert run(store).success

    assert calls["compute"] == (NEIGHBORHOOD, DATE, DEFAULT_MAX_BUILT_AREA_M2)
    # Read back out of the same transaction that computed it.
    assert calls["fetch"] == (NEIGHBORHOOD, DATE)

    path = join(
        store.partition_dir(lot_profiles.key.path[-1], DATE, NEIGHBORHOOD),
        LOT_PROFILES_FILE,
    )
    assert "/gold/lot_profiles/" in path
    frame = gpd.read_parquet(path)
    assert len(frame) == 10
    assert frame.crs.to_string() == "EPSG:4326"
    assert frame["neighborhood"].unique().tolist() == [NEIGHBORHOOD]
    assert frame["scrape_date"].unique().tolist() == [DATE]


def test_every_lot_is_kept_and_the_vacant_ones_are_a_filter(store, monkeypatch):
    """The whole reason this replaced `vacant_lots`.

    That asset wrote only the parcels it found nothing on, so the lots it
    dropped were the ones a reader could no longer see. Here the file is the
    borough's whole cadastre and "where is the empty land" is a column.
    """
    write_partition(store)
    stub_postgis(monkeypatch, num_lots=10, with_building=6)

    assert run(store).success

    path = join(
        store.partition_dir(lot_profiles.key.path[-1], DATE, NEIGHBORHOOD),
        LOT_PROFILES_FILE,
    )
    frame = gpd.read_parquet(path)

    assert len(frame) == 10, "every lot gets a row, built or not"
    assert frame["has_building"].sum() == 6
    assert len(frame[~frame["has_building"]]) == 4
    # The built lots are still there with their building count, which is what
    # the old table could not say.
    assert frame.loc[frame["has_building"], "num_buildings"].tolist() == [2] * 6
    assert frame.loc[~frame["has_building"], "num_buildings"].tolist() == [0] * 4


def test_the_lot_carries_its_two_frontages_its_neighborhood_and_its_pdf(
    store, monkeypatch
):
    write_partition(store)
    stub_postgis(
        monkeypatch, num_lots=10, with_frontage=8, with_secondary=2, with_documents=7
    )

    assert run(store).success

    path = join(
        store.partition_dir(lot_profiles.key.path[-1], DATE, NEIGHBORHOOD),
        LOT_PROFILES_FILE,
    )
    frame = gpd.read_parquet(path)

    # Primary on the lots that face a street, and NULL - not 0 - on the two
    # that face none: an unmeasured edge is not a zero-metre edge.
    assert frame["primary_frontage_m"].notna().sum() == 8
    assert frame["primary_frontage_m"].isna().sum() == 2
    # Secondary only where there is a second street edge, so a corner lot.
    assert frame["secondary_frontage_m"].notna().sum() == 2

    assert frame["neighborhood"].unique().tolist() == [NEIGHBORHOOD]

    assert frame["doc_url"].notna().sum() == 7
    # JSON text rather than a nested type, so the column is typed the same way
    # in a partition where nothing is covered.
    covered = json.loads(frame["documents"].iloc[0])
    assert covered[0]["url"] == "https://example.test/g.pdf"
    assert json.loads(frame["documents"].iloc[-1]) == []


def test_metadata_carries_the_threshold_the_counts_mean_nothing_without(
    store, monkeypatch
):
    write_partition(store)
    stub_postgis(
        monkeypatch,
        num_lots=10,
        with_building=6,
        with_frontage=8,
        with_secondary=2,
        with_documents=7,
    )

    result = run(store)

    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["dagster/row_count"].value == 10
    # The invariant: a profile per lot.
    assert metadata["num_lots"].value == 10
    assert metadata["num_profiles"].value == 10

    assert metadata["num_with_building"].value == 6
    assert metadata["num_without_building"].value == 4
    assert metadata["pct_without_building"].value == pytest.approx(40.0)
    assert metadata["num_built"].value == 6
    assert metadata["num_no_building"].value == 4
    assert metadata["num_buildings"].value == 12

    assert metadata["num_with_frontage"].value == 8
    assert metadata["num_without_frontage"].value == 2
    assert metadata["num_with_secondary_frontage"].value == 2
    assert metadata["max_primary_frontage_m"].value == pytest.approx(31.2)

    assert metadata["num_with_documents"].value == 7
    assert metadata["num_without_documents"].value == 3

    assert metadata["total_lot_area_ha"].value == pytest.approx(2.24)
    # Every category but `built`, which is the old vacant_lots total.
    assert metadata["vacant_area_ha"].value == pytest.approx(2.0)
    # What `category` means depends entirely on this.
    assert metadata["max_built_area_m2"].value == DEFAULT_MAX_BUILT_AREA_M2


def test_the_configured_threshold_reaches_the_query_and_the_metadata(
    store, monkeypatch
):
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    result = run(store, max_built_area_m2=60.0)

    assert calls["compute"] == (NEIGHBORHOOD, DATE, 60.0)
    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["max_built_area_m2"].value == 60.0


def test_a_partition_with_no_lots_at_all_says_what_it_means(store, monkeypatch):
    """Zero lots is an unloaded partition, not a borough with no cadastre.

    Telling the two apart is the whole reason this fails rather than writing a
    perfectly well-formed zero.
    """
    write_partition(store)
    stub_postgis(monkeypatch, num_lots=0, with_building=0)

    with pytest.raises(Failure, match="rag.lots holds no lot"):
        run(store)


def test_a_missing_hbu_infra_relation_names_the_file_to_apply(store, monkeypatch):
    """`rag.lot_documents` is the one most likely to be absent.

    sql/006 carries a `-- requires: rag.chunks` header, so `db.py init` skips
    it until a corpus has been indexed - a failure that says so beats
    `relation "rag.lot_documents" does not exist`.
    """
    write_partition(store)
    stub_postgis(
        monkeypatch,
        compute_raises=MissingRelation(
            "hbu_infra has not created: rag.lot_documents "
            "(sql/006_lot_documents.sql)"
        ),
    )

    with pytest.raises(Failure, match="sql/006_lot_documents.sql"):
        run(store)


def test_the_envelopes_reach_the_query_keyed_on_lot_number_most_of_the_lot_first(
    store, monkeypatch
):
    """`lot_uid` is a bigserial `load_lots` mints again on every reload, so the
    key that survives one is the lot number - the same reason 009 denormalises
    it in the first place."""
    write_partition(store)
    write_envelopes(store, lot_numbers=("1", "2", "1"), pct_of_lot=(8.0, 100.0, 92.0))
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    staged = calls["zoning_envelopes"]
    assert [lot_number for lot_number, _ in staged] == ["2", "1", "1"]
    # Within the whole partition and across lots, most-of-the-lot first: the
    # first entry of a lot's array is the zone a reader who only wants one
    # should get.
    assert [entry["pct_of_lot"] for _, entry in staged] == [100.0, 92.0, 8.0]


def test_an_envelope_entry_drops_the_lot_columns_the_profile_row_already_has(
    store, monkeypatch
):
    """A norm restated once per envelope is the trade; the lot's own area and
    frontage restated once per envelope is just waste."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    _, entry = calls["zoning_envelopes"][0]
    for column in ("lot_uid", "lot_number", "lot_area_m2", "primary_frontage_m",
                   "neighborhood", "scrape_date"):
        assert column not in entry, f"{column} is already a column of the profile"
    # What the envelope is actually for.
    assert entry["feature_id"] == "C01-002"
    assert entry["floors_max"] == 3
    assert entry["governs_residential"] is True


def test_the_json_string_columns_are_decoded_into_real_json(store, monkeypatch):
    """`zoning_grid_columns` writes these as strings so the parquet schema is
    stable across partitions. jsonb has no such problem, and a string that
    looks like a list is a list nobody can query."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    _, entry = calls["zoning_envelopes"][0]
    assert entry["usages"] == ["H.1"]
    assert entry["levels"] == ["1", "2"]
    assert entry["parse_notes"] == []


def test_the_cmhc_grid_becomes_one_object_per_borough_not_one_per_lot(
    store, monkeypatch
):
    """CMHC surveys neighborhoods and publishes no geometry, so there is
    nothing per-lot about these and nothing to join them on."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    vacancy = calls["vacancy_rates"]
    assert vacancy["survey_year"] == 2023
    assert vacancy["survey_period"] == "octobre 2023"
    assert vacancy["num_quartiers_mapped"] == 3
    assert vacancy["overall_vacancy_rate_pct"] == pytest.approx(0.5)
    # Every cell travels, suppressed ones included: a suppressed rate is a
    # fact about the survey, and an absent cell would read as a gap.
    assert len(vacancy["cells"]) == 2
    assert vacancy["num_published_cells"] == 1
    suppressed = next(c for c in vacancy["cells"] if c["bedroom_type"] == "2_bedroom")
    assert suppressed["vacancy_rate_pct"] is None

    rents = calls["average_rents"]
    assert rents["overall_average_rent_cad"] == pytest.approx(1_275.0)
    assert rents["num_published_cells"] == 1
    assert len(rents["cells"]) == 2


def test_the_assessment_counts_reach_the_metadata(store, monkeypatch):
    write_partition(store)
    stub_postgis(monkeypatch, num_lots=10, with_assessed_value=7)

    result = run(store)

    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["num_with_assessed_value"].value == 7
    # The symptom worth seeing, the same way num_without_frontage is: a lane or
    # a park is the honest reading, a third of the borough is the roll and the
    # cadastre disagreeing about where the ground is.
    assert metadata["num_without_assessed_value"].value == 3
    assert metadata["num_assessment_units"].value == 21
    assert metadata["total_assessed_value_apportioned_billions"].value == pytest.approx(
        29.4
    )
    # The roll is triennial and this partition's axis is the cadastre's scrape
    # date, so the row cannot be read back without it.
    assert metadata["roll_year"].value == 2026


def test_a_partition_whose_roll_has_not_landed_says_so_rather_than_zero(
    store, monkeypatch
):
    """`lot_assessed_values` has not run for this partition.

    A $0.0B borough and a borough nobody has valued are different answers, and
    the second one is the one worth acting on.
    """
    write_partition(store)
    stub_postgis(monkeypatch, num_lots=10, with_assessed_value=0, roll_year=None)

    result = run(store)

    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["num_with_assessed_value"].value == 0
    assert metadata["num_without_assessed_value"].value == 10
    assert metadata["total_assessed_value_apportioned_billions"].value == "not assessed"
    assert metadata["roll_year"].value == "unknown"


def test_the_borough_figures_and_the_envelope_counts_reach_the_metadata(
    store, monkeypatch
):
    write_partition(store)
    stub_postgis(monkeypatch, num_lots=10)

    result = run(store)

    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["num_zoning_envelopes"].value == 2
    assert metadata["num_with_zoning_envelopes"].value == 2
    # The symptom worth seeing: a lot no readable grid reaches.
    assert metadata["num_without_zoning_envelopes"].value == 8
    assert metadata["cmhc_survey_year"].value == 2023
    assert metadata["overall_vacancy_rate_pct"].value == pytest.approx(0.5)
    assert metadata["overall_average_rent_cad"].value == pytest.approx(1_275.0)
    assert metadata["num_cmhc_vacancy_cells"].value == 2
    assert metadata["num_cmhc_rent_cells"].value == 2


def test_a_suppressed_cmhc_figure_is_said_rather_than_left_blank(store, monkeypatch):
    """`MetadataValue.float(None)` renders as an empty cell, which reads as
    "the pipeline lost it" rather than as "CMHC does not publish it"."""
    write_partition(store)
    write_vacancy(store, overall=None)
    stub_postgis(monkeypatch)

    result = run(store)

    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["overall_vacancy_rate_pct"].value == "suppressed"


def test_the_cost_guide_becomes_one_object_for_the_whole_city(store, monkeypatch):
    """A stronger version of the CMHC case. CMHC at least surveys
    neighborhoods; the guide prices nine Canadian markets and publishes no
    geometry at all, so a Montreal rate is a Montreal rate on every lot of
    every borough and there is nothing whatever to join it on."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    costs = calls["construction_costs"]
    assert costs["city"] == "mtl"
    assert costs["city_label"] == "Montreal"
    # Which publication the rates came out of. A rate has no meaning against
    # next quarter's without it - the same reason the CMHC objects carry
    # survey_year.
    assert costs["source_last_modified"] == LAST_MODIFIED
    # The guide's own snapshot date, named apart from the cadastre's.
    assert costs["cost_scrape_date"] == DATE


def test_the_two_parking_structures_are_flattened_onto_their_own_columns(
    store, monkeypatch
):
    """Underground and the integrated ground-level garage: the pair a building
    actually chooses between. Dollars per *stall*, which is why unit_flag rides
    along with every rate."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    costs = calls["construction_costs"]
    assert costs["underground_stall_cost_low_cad"] == pytest.approx(51_925.0)
    assert costs["underground_stall_cost_high_cad"] == pytest.approx(68_675.0)
    assert costs["above_grade_stall_cost_low_cad"] == pytest.approx(38_500.0)
    assert costs["above_grade_stall_cost_high_cad"] == pytest.approx(57_750.0)

    # The whole set travels too, in the order this pipeline declares rather
    # than the publisher's - and per stall, said so on every entry.
    assert [entry["id"] for entry in costs["parking"]] == ["parkade_ug", "parkade_ag"]
    assert {entry["unit_flag"] for entry in costs["parking"]} == {"perStall"}
    # A surface lot is priced per stall like the other two and is deliberately
    # not carried: it is not a parking structure, and neither is the office
    # row sharing the same bronze file.
    assert "surface_lot" not in {entry["id"] for entry in costs["parking"]}
    assert "office_a" not in {entry["id"] for entry in costs["parking"]}


def test_every_condo_band_travels_and_the_configured_one_becomes_a_column(
    store, monkeypatch
):
    """Which band belongs in the flattened column is a judgement about what
    would actually get built, not something the data settles - so it is Config,
    and the band chosen is named on every row."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    assert run(store).success

    costs = calls["construction_costs"]
    # Wood frame up to six storeys, the default: what a borough of triplexes
    # builds, and dollars per square foot rather than per stall.
    assert costs["condo_band"] == "condo_wood"
    assert costs["condo_cost_low_cad_sqft"] == pytest.approx(225.0)
    assert costs["condo_cost_high_cad_sqft"] == pytest.approx(290.0)

    # All five bands are carried, ascending by storey, whichever one was
    # flattened. Townhouses share the category upstream and are not what "an
    # apartment building on this lot" means.
    assert [entry["id"] for entry in costs["residential"]] == [
        "condo_wood",
        "condo_12",
        "condo_13_39",
        "condo_40_60",
        "condo_60plus",
    ]
    assert "townhouse_row" not in {entry["id"] for entry in costs["residential"]}
    assert {entry["unit_flag"] for entry in costs["residential"]} == {None}


def test_the_configured_condo_band_moves_the_flattened_column(store, monkeypatch):
    """A downtown parcel under a 40-storey envelope is not a wood frame walkup
    at any price, so the band is a knob rather than a constant."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    result = run(store, condo_type_id="condo_40_60")

    costs = calls["construction_costs"]
    assert costs["condo_band"] == "condo_40_60"
    assert costs["condo_cost_low_cad_sqft"] == pytest.approx(330.0)
    assert costs["condo_cost_high_cad_sqft"] == pytest.approx(375.0)
    # The other four are still there, so nothing is lost by choosing.
    assert len(costs["residential"]) == 5

    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["condo_band"].value == "condo_40_60"


def test_a_condo_band_the_guide_never_published_fails_before_postgres(
    store, monkeypatch
):
    """A typo in the run config, caught where it is cheap - not as a column
    that silently reads NULL on every lot in the borough."""
    write_partition(store)
    calls = stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="condo_type_id='condo_7'"):
        run(store, condo_type_id="condo_7")

    assert "compute" not in calls, "Postgres was touched before the config was read"


def test_a_type_the_guide_stopped_publishing_is_a_null_column_not_a_failure(
    store, monkeypatch
):
    """Losing one of sixty rates should not cost a borough its cadastre. The
    column lands NULL and the run says so."""
    write_partition(store)
    write_costs(store, parking={"parkade_ug": PARKING_RATES["parkade_ug"]})
    calls = stub_postgis(monkeypatch)

    result = run(store)

    costs = calls["construction_costs"]
    assert costs["underground_stall_cost_low_cad"] == pytest.approx(51_925.0)
    # Never published this quarter, so there is nothing to flatten.
    assert costs["above_grade_stall_cost_low_cad"] is None
    assert costs["above_grade_stall_cost_high_cad"] is None

    metadata = materialization_metadata(result, lot_profiles)
    # Said rather than left blank: an empty cell reads as "the pipeline lost
    # it" rather than as "the guide does not price it".
    assert metadata["above_grade_stall_cost_low_cad"].value == "not published"
    assert metadata["underground_stall_cost_low_cad"].value == pytest.approx(51_925.0)


def test_the_cost_figures_reach_the_metadata(store, monkeypatch):
    """Identical on every row, so they are reported once beside the run rather
    than left to be read out of one lot's jsonb."""
    write_partition(store)
    stub_postgis(monkeypatch)

    result = run(store)

    metadata = materialization_metadata(result, lot_profiles)
    assert metadata["cost_guide_last_modified"].value == LAST_MODIFIED
    assert metadata["condo_band"].value == "condo_wood"
    # Two parking structures and five condo bands.
    assert metadata["num_cost_rates"].value == 7
    assert metadata["underground_stall_cost_high_cad"].value == pytest.approx(68_675.0)
    assert metadata["condo_cost_low_cad_sqft"].value == pytest.approx(225.0)


def test_a_missing_silver_partition_names_the_asset_to_materialize(
    store, monkeypatch
):
    """These are declared deps read out of the tree, so a missing file means
    the partition was never materialized - and the message that helps says
    which asset to run."""
    write_vacancy(store)
    write_rents(store)
    write_costs(store)
    stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="materialize lot_zoning_envelopes"):
        run(store)


def test_a_missing_cost_snapshot_names_a_date_and_no_borough(store, monkeypatch):
    """The guide is partitioned by date alone, so naming a borough in its
    failure would send the reader looking for a partition key that does not
    exist."""
    write_envelopes(store)
    write_vacancy(store)
    write_rents(store)
    stub_postgis(monkeypatch)

    with pytest.raises(Failure) as excinfo:
        run(store)

    message = str(excinfo.value)
    assert f"materialize montreal_residential_costs for {DATE} first" in message
    assert NEIGHBORHOOD not in message


def test_the_silver_files_are_read_before_the_partition_is_deleted(
    store, monkeypatch
):
    """A partition missing an input should fail naming it, not after tearing
    down the rows it was going to replace."""
    write_envelopes(store)
    write_rents(store)
    calls = stub_postgis(monkeypatch)

    with pytest.raises(Failure, match="materialize vacancy_rates"):
        run(store)

    assert "compute" not in calls, "Postgres was touched before the inputs were read"
