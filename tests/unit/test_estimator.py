"""Offline tests for the cost-guide client and the two bronze assets over it.

Nothing here touches the network: `CATALOG_JS` below is a trimmed copy of the
real `data/building-types.js` - same shape, same quirks (en dashes in the
labels, `perStall` on the parking entries, `sourceNote` on nothing the assets
read), fed to the client through a stubbed session.

The parser half is what most of these are about. The source is JavaScript
rather than JSON and it belongs to someone else, so the tests that matter most
are the ones that say what happens when it changes underneath: a renamed
array, a city that stops being priced, a type that grows a second unit flag.
"""

from __future__ import annotations

import pandas as pd
import pytest
from dagster import Failure, materialize

from urban_rag.estimator import (
    MONTREAL_CITY_ID,
    NON_RESIDENTIAL_CATEGORIES,
    RESIDENTIAL_CATEGORIES,
    EstimatorClient,
    EstimatorError,
    parse_catalog,
    rates_frame,
)
from urban_rag.estimator_assets import (
    NON_RESIDENTIAL_FILE,
    RESIDENTIAL_FILE,
    montreal_nonresidential_costs,
    montreal_residential_costs,
)
from urban_rag.resources import EstimatorResource, ParquetStore
from urban_rag.storage import join

from asset_helpers import materialization_metadata

DATE = "2026-08-01"
LAST_MODIFIED = "Fri, 08 May 2026 02:14:08 GMT"

#: A trimmed `data/building-types.js`: three cities, and one or two types per
#: category the assets read, plus an institutional one neither of them keeps.
#: The comment block, the en dashes and the unquoted keys are all as published.
CATALOG_JS = """/**
 * data/building-types.js
 * Master catalog. CITIES and TYPES. Rates are $/sf low-high unless a unit
 * flag (perStall, perLM, perSM, perUnit, perAcre) says otherwise.
 */
const CITIES = [
  {id:"tor",label:"Toronto",prov:"ON"},
  {id:"mtl",label:"Montreal",prov:"QC"},
  {id:"van",label:"Vancouver",prov:"BC"}
];

const TYPES = [
  {id:"condo_12",label:"Condominium / Apartment (Up to 12 Storeys)",\
sector:"private",cat:"residential",rates:{tor:[245,390],mtl:[275,335],van:[330,400]}},
  {id:"condo_13_39",label:"Condominium / Apartment (13–39 Storeys)",\
sector:"private",cat:"residential",rates:{tor:[280,350],mtl:[320,330],van:[340,435]}},
  {id:"condo_60plus",label:"Condominium / Apartment (60+ Storeys)",\
sector:"private",cat:"residential",rates:{tor:[350,480],mtl:[330,425],van:[370,480]}},
  {id:"uni_residence",label:"University / College – Student Residence",\
sector:"public",cat:"residential",rates:{tor:[380,500],mtl:[375,470],van:[395,580]}},
  {id:"office_a",label:"Office 5–30 Storeys (Class A)",\
sector:"private",cat:"commercial",rates:{tor:[305,450],mtl:[280,375],van:[345,425]}},
  {id:"warehouse",label:"Warehouse",\
sector:"private",cat:"industrial",rates:{tor:[75,180],mtl:[120,185],van:[120,200]}},
  {id:"school_elem",label:"Elementary School",\
sector:"public",cat:"institutional",rates:{tor:[400,520],mtl:[350,470],van:[420,540]}},
  {id:"parkade_ug",label:"Parking – Underground Garage",sector:"private",\
cat:"parking",rates:{tor:[55275,95475],mtl:[51925,68675],van:[65325,100500]},perStall:true},
  {id:"surface_lot",label:"Parking – Surface Lot",sector:"private",\
cat:"parking",rates:{tor:[4620,9900],mtl:[3960,8250],van:[4290,9900]},perStall:true}
];
"""


class FakeResponse:
    def __init__(self, body: str, *, last_modified: str | None = LAST_MODIFIED):
        self.content = body.encode("utf-8")
        self.headers = {"Content-Type": "application/javascript; charset=utf-8"}
        if last_modified is not None:
            self.headers["Last-Modified"] = last_modified

    def raise_for_status(self):
        return None


class FakeSession:
    """Replays one canned script, and records what was asked for."""

    def __init__(self, body: str = CATALOG_JS, **response_kwargs):
        self.body = body
        self.response_kwargs = response_kwargs
        self.headers: dict[str, str] = {}
        self.calls: list[str] = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        return FakeResponse(self.body, **self.response_kwargs)


def client(body: str = CATALOG_JS, **response_kwargs) -> EstimatorClient:
    return EstimatorClient(
        session=FakeSession(body, **response_kwargs),
        # Nothing to be polite to: the session never opens a socket.
        request_delay_seconds=0.0,
    )


class StubEstimator(EstimatorResource):
    """`EstimatorResource` with the socket replaced, and nothing else.

    A subclass rather than a monkeypatched instance because Dagster rebuilds a
    `ConfigurableResource` from its config when it initializes it, so an
    attribute set on the instance the test holds is not the one the asset
    gets. `body` and `last_modified` are ordinary pydantic fields, which
    survive that round trip the way `base_url` does.
    """

    body: str = CATALOG_JS
    last_modified: str | None = LAST_MODIFIED

    def client(self) -> EstimatorClient:
        return EstimatorClient(
            self.base_url,
            request_delay_seconds=0.0,
            session=FakeSession(self.body, last_modified=self.last_modified),
        )


@pytest.fixture
def store(tmp_path) -> ParquetStore:
    return ParquetStore(root_dir=str(tmp_path))


@pytest.fixture
def estimator() -> StubEstimator:
    return StubEstimator()


# -- the parser -------------------------------------------------------------


def test_parses_the_cities_and_types_the_script_declares():
    catalog = parse_catalog(CATALOG_JS)

    assert [c.id for c in catalog.cities] == ["tor", "mtl", "van"]
    assert catalog.city("mtl").label == "Montreal"
    assert catalog.city("mtl").prov == "QC"
    assert len(catalog.types) == 9


def test_keeps_the_publishers_own_text_including_en_dashes():
    catalog = parse_catalog(CATALOG_JS)
    labels = {t.id: t.label for t in catalog.types}

    # The band is inside the label, and the dash in it is an en dash. A test
    # that accepted a hyphen here would pass against a mojibaked snapshot.
    assert labels["condo_13_39"] == "Condominium / Apartment (13–39 Storeys)"
    assert labels["parkade_ug"] == "Parking – Underground Garage"


def test_a_type_with_no_unit_flag_is_priced_per_square_foot():
    catalog = parse_catalog(CATALOG_JS)
    by_id = {t.id: t for t in catalog.types}

    assert by_id["condo_12"].unit_flag is None
    assert by_id["parkade_ug"].unit_flag == "perStall"


def test_single_quotes_and_trailing_commas_parse():
    """Neither appears in today's file; both are legal JavaScript."""
    script = (
        "const CITIES = [{id:'mtl',label:'Montreal',prov:'QC'},];\n"
        "const TYPES = [{id:'x',label:'X',sector:'private',cat:'residential',"
        "rates:{mtl:[1,2]},},];\n"
    )
    catalog = parse_catalog(script)

    assert catalog.city("mtl").label == "Montreal"
    assert catalog.types[0].rates["mtl"] == (1.0, 2.0)


def test_a_renamed_array_says_what_it_was_looking_for():
    script = CATALOG_JS.replace("const TYPES", "const BUILDING_TYPES")

    with pytest.raises(EstimatorError, match=r"const TYPES"):
        parse_catalog(script)


def test_a_type_priced_two_ways_at_once_is_refused():
    script = CATALOG_JS.replace(
        'rates:{tor:[55275,95475],mtl:[51925,68675],van:[65325,100500]},perStall:true',
        'rates:{tor:[55275,95475],mtl:[51925,68675],van:[65325,100500]},'
        "perStall:true,perSM:true",
    )

    with pytest.raises(EstimatorError, match=r"more than one unit flag"):
        parse_catalog(script)


def test_a_rate_that_is_not_a_pair_is_refused():
    script = CATALOG_JS.replace("mtl:[275,335]", "mtl:[275]")

    with pytest.raises(EstimatorError, match=r"not a \[low, high\] pair"):
        parse_catalog(script)


def test_a_script_that_calls_something_fails_rather_than_running_it():
    script = CATALOG_JS.replace("mtl:[275,335]", "mtl:inflate([275,335])")

    with pytest.raises(EstimatorError, match=r"not readable as a JSON array"):
        parse_catalog(script)


# -- the city slice ---------------------------------------------------------


def test_the_frame_keeps_only_the_requested_city_and_categories():
    catalog = parse_catalog(CATALOG_JS)
    frame = rates_frame(catalog, MONTREAL_CITY_ID, RESIDENTIAL_CATEGORIES)

    assert set(frame["city"]) == {"mtl"}
    assert set(frame["cat"]) == {"residential"}
    # The institutional and non-residential types are not in it, and the
    # Toronto and Vancouver columns are gone.
    assert "school_elem" not in set(frame["id"])
    assert list(frame["rate_low"]) == [275.0, 320.0, 330.0, 375.0]


def test_residential_rows_keep_the_guides_order_which_is_by_storey_band():
    catalog = parse_catalog(CATALOG_JS)
    frame = rates_frame(catalog, MONTREAL_CITY_ID, RESIDENTIAL_CATEGORIES)

    assert list(frame["id"]) == [
        "condo_12",
        "condo_13_39",
        "condo_60plus",
        "uni_residence",
    ]


def test_the_non_residential_frame_carries_all_three_categories():
    catalog = parse_catalog(CATALOG_JS)
    frame = rates_frame(catalog, MONTREAL_CITY_ID, NON_RESIDENTIAL_CATEGORIES)

    assert list(frame["cat"]) == [
        "commercial",
        "industrial",
        "parking",
        "parking",
    ]
    # Parking is dollars per stall, not per square foot, and the frame has to
    # say so - the figures are four orders of magnitude apart.
    parking = frame[frame["cat"] == "parking"]
    assert set(parking["unit_flag"]) == {"perStall"}
    assert list(parking["rate_low"]) == [51925.0, 3960.0]
    assert frame[frame["cat"] != "parking"]["unit_flag"].isna().all()


def test_a_city_the_guide_stops_pricing_says_which_ones_are_left():
    catalog = parse_catalog(CATALOG_JS)

    with pytest.raises(EstimatorError, match=r"it prices: mtl, tor, van"):
        rates_frame(catalog, "qbc", RESIDENTIAL_CATEGORIES)


def test_text_columns_are_typed_even_when_every_value_is_missing():
    """So a partition where nothing carries a `sourceNote` writes the same
    parquet schema as one where something does."""
    catalog = parse_catalog(CATALOG_JS)
    frame = rates_frame(catalog, MONTREAL_CITY_ID, RESIDENTIAL_CATEGORIES)

    assert frame["sourceNote"].isna().all()
    assert pd.api.types.is_string_dtype(frame["sourceNote"])
    assert pd.api.types.is_string_dtype(frame["unit_flag"])


# -- the client -------------------------------------------------------------


def test_the_client_reads_the_script_and_the_header_beside_it():
    session = FakeSession()
    catalog, last_modified = EstimatorClient(
        session=session, request_delay_seconds=0.0
    ).catalog()

    assert session.calls == [
        "https://zef-builds.github.io/construction-estimator/data/building-types.js"
    ]
    assert last_modified == LAST_MODIFIED
    assert catalog.city("mtl").label == "Montreal"


def test_a_publisher_that_sends_no_last_modified_is_not_an_error():
    catalog, last_modified = client(last_modified=None).catalog()

    assert last_modified is None
    assert catalog.types


# -- the assets -------------------------------------------------------------


def run(asset_def, store, estimator):
    return materialize(
        [asset_def],
        partition_key=DATE,
        resources={"estimator": estimator, "store": store},
    )


def read(store, asset_def, filename) -> pd.DataFrame:
    return pd.read_parquet(
        join(store.partition_dir(asset_def.key.path[-1], DATE), filename)
    )


def test_the_residential_asset_writes_one_row_per_published_type(store, estimator):
    result = run(montreal_residential_costs, store, estimator)
    assert result.success

    frame = read(store, montreal_residential_costs, RESIDENTIAL_FILE)
    assert list(frame["id"]) == [
        "condo_12",
        "condo_13_39",
        "condo_60plus",
        "uni_residence",
    ]
    assert set(frame["cat"]) == {"residential"}


def test_the_non_residential_asset_writes_commercial_industrial_and_parking(
    store, estimator
):
    result = run(montreal_nonresidential_costs, store, estimator)
    assert result.success

    frame = read(store, montreal_nonresidential_costs, NON_RESIDENTIAL_FILE)
    assert list(frame["cat"]) == ["commercial", "industrial", "parking", "parking"]


def test_a_partition_says_which_snapshot_it_is(store, estimator):
    assert run(montreal_residential_costs, store, estimator).success

    frame = read(store, montreal_residential_costs, RESIDENTIAL_FILE)
    assert set(frame["scrape_date"]) == {DATE}
    assert set(frame["source_last_modified"]) == {LAST_MODIFIED}
    assert frame["scraped_at"].str.endswith("+00:00").all()
    assert set(frame["source_url"]) == {
        "https://zef-builds.github.io/construction-estimator/data/building-types.js"
    }


def test_both_assets_land_under_bronze(store, estimator):
    for asset_def in (montreal_residential_costs, montreal_nonresidential_costs):
        assert asset_def.key.path[0] == "bronze"

    assert run(montreal_residential_costs, store, estimator).success
    assert "/bronze/montreal_residential_costs/" in store.partition_dir(
        "montreal_residential_costs", DATE
    ).replace("\\", "/")


def test_the_run_reports_what_it_wrote(store, estimator):
    result = run(montreal_nonresidential_costs, store, estimator)

    metadata = materialization_metadata(result, montreal_nonresidential_costs)
    assert metadata["dagster/row_count"].value == 4
    assert metadata["num_categories"].value == 3
    assert metadata["city"].value == "Montreal (mtl)"
    assert "perStall" in metadata["rates"].value


def test_a_rerun_replaces_the_partition_rather_than_adding_to_it(store, estimator):
    assert run(montreal_residential_costs, store, estimator).success
    first = read(store, montreal_residential_costs, RESIDENTIAL_FILE)

    assert run(montreal_residential_costs, store, estimator).success
    second = read(store, montreal_residential_costs, RESIDENTIAL_FILE)

    assert len(second) == len(first)


def test_a_restructured_publication_fails_the_partition(store):
    broken = StubEstimator(body="const NOTHING = [];")

    with pytest.raises(Failure, match=r"Cost guide read for 2026-08-01 failed"):
        run(montreal_residential_costs, store, broken)
