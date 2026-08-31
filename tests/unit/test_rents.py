"""What a square foot of commercial floor earns, and where the number is from.

Three layers, tested the way the rest of this suite tests a source: the two
clients against bytes that look like what the publishers send, and the assets
by materializing them over a `ParquetStore` with those bytes stubbed in.

The MarketBeat fixtures are the shape `pypdf` actually extracts from a real
report - a headline block, then a submarket table whose rows carry a *variable*
number of columns, then the transaction blocks that also end in money. Written
as text rather than as a real PDF because what is under test is the parser, and
`parse_submarkets` takes bytes only to hand them to pypdf: `_table_lines` is
monkeypatched so the tests exercise the row logic rather than pypdf's.
"""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize

from asset_helpers import materialization_metadata, stub_publish
from urban_rag import crspi, marketbeat, rent_assets
from urban_rag.crspi import (
    BUILDING_TYPES,
    CrspiError,
    escalate,
    index_at,
    latest_period,
    read_montreal,
)
from urban_rag.marketbeat import (
    INDUSTRIAL,
    OFFICE,
    MarketBeatError,
    Report,
    discover_reports,
    latest_by_sector,
    market_total,
    parse_submarkets,
)
from urban_rag.partitions import submarket_for
from urban_rag.rent_assets import (
    COMMERCIAL_RENTS_FILE,
    MARKETBEAT_FILE,
    RENT_INDEX_FILE,
    commercial_rent_index,
    commercial_rents,
    montreal_commercial_rents,
)
from urban_rag.resources import (
    CrspiResource,
    MarketBeatResource,
    ParquetStore,
    PostgisResource,
)
from urban_rag.storage import join

DATE = "2026-08-26"
NEIGHBORHOOD = "VSMPE"

ASSETS = "https://assets.cushmanwakefield.com/-/media/cw/marketbeat-pdfs"


# -- discovering the reports ------------------------------------------------


LANDING_HTML = f"""
<html><body>
  <a href="{ASSETS}/2026/q2/canada/montreal-americas-office-marketbeat-q22026.pdf?rev=aa">Office</a>
  <a href="{ASSETS}/2026/q2/canada/montreal_americas_industrial_marketbeat-q22026.pdf?rev=bb">Industrial</a>
  <a href="{ASSETS}/2026/q1/canada/montreal_americas_office_marketbeat-q12026-.pdf?rev=cc">Office Q1</a>
  <a href="{ASSETS}/2025/q4/canada/montreal-industrial-marketbeat-q4-2025.pdf?rev=dd">Ind Q4</a>
  <a href="https://www.cushmanwakefield.com/-/media/cw/global/vendor-code.pdf?rev=ee">Not a report</a>
</body></html>
"""


def test_the_period_is_read_off_the_path_not_the_filename():
    """The filename changes shape every quarter; `/<year>/q<n>/` does not."""
    reports = {(r.sector, r.period) for r in discover_reports(LANDING_HTML)}

    assert (OFFICE, "2026-Q2") in reports
    assert (INDUSTRIAL, "2026-Q2") in reports
    # Underscores one quarter, hyphens the next, a trailing dash the one after.
    assert (OFFICE, "2026-Q1") in reports
    assert (INDUSTRIAL, "2025-Q4") in reports


def test_a_pdf_that_is_not_a_marketbeat_is_dropped():
    reports = discover_reports(LANDING_HTML)

    assert all("vendor-code" not in report.url for report in reports)
    assert len(reports) == 4


def test_the_latest_quarter_wins_per_sector():
    latest = latest_by_sector(discover_reports(LANDING_HTML))

    assert latest[OFFICE].period == "2026-Q2"
    assert latest[INDUSTRIAL].period == "2026-Q2"


def test_a_sector_the_page_does_not_list_is_refused():
    """A partition with no industrial rent would price every warehouse at
    nothing - a thing to fail on, not to discover in a cap rate later."""
    office_only = f'<a href="{ASSETS}/2026/q2/canada/montreal-office-marketbeat.pdf">o</a>'

    with pytest.raises(MarketBeatError, match="industrial"):
        latest_by_sector(discover_reports(office_only))


def test_a_report_lines_up_with_the_index_on_its_quarter():
    assert Report(OFFICE, 2026, 2, "u").period_start == "2026-04-01"
    assert Report(OFFICE, 2026, 1, "u").period_start == "2026-01-01"
    assert Report(OFFICE, 2025, 4, "u").period_start == "2025-10-01"


# -- parsing the submarket table --------------------------------------------


#: An industrial report as pypdf extracts it. Note the *variable* column count:
#: Midtown North states no construction figures and Montreal East states three,
#: so the rents can only be found from the right. The two transaction blocks at
#: the bottom also end in money and must not be read as submarkets.
INDUSTRIAL_LINES = [
    "*Rental rates reflect weighted direct net asking $psf/year",
    "SUBMARKET INVENTORY",
    "OVERALL WEIGHTED AVG NET RENT*",
    "Montréal Midtown North 43,524,675 2,057,207 4.7% -207,581 -117,743 $12.98 $4.09",
    "Montréal East 72,425,356 5,748,614 7.9% -492,153 -636,840 195,680 195,680 $14.05 $4.85",
    "Lachine 19,605,933 1,585,933 8.1% -61,603 79,712 $12.29 $4.39",
    "Montréal TOTALS 356,929,878 27,342,657 7.7% -1,752,251 -1,839,430 625,279 749,083 798,580 $14.06 $4.68",
    "KEY LEASE TRANSACTIONS Q2 2026",
    "1600 50th Avenue Lachine Bulletproof Logistics Dream REIT 242,615 Direct",
    "KEY SALES TRANSACTIONS Q2 2026",
    "1485 de Coulomb Street South Shore Transcontinental Inc. /",
    "Emballages Carrousel 239,825 $34,900,000 / $146",
]

#: An office report, and the transaction block comes *above* the table - which
#: is the order the real ones use and the reason the parser cannot simply cut
#: at the first transactions heading.
OFFICE_LINES = [
    "*Rental rates reflect full service asking",
    "KEY LEASE TRANSACTIONS Q2 2026",
    "1000 De La Gauchetiere Financial Core Confidential 55,000 Direct",
    "SUBMARKET INVENTORY",
    "OVERALL AVG ASKING RENT (ALL CLASSES)*",
    "Financial Core 22,265,229 293,377 4,301,529 20.6% -204,640 539,216 0 $41.97 $46.05",
    "Midtown North 9,914,338 108,533 1,616,057 17.4% -393,410 99,681 0 $22.39 $32.80",
    "Midtown Central 6,852,987 191,501 1,040,071 18.0% -44,091 104,637 0 $33.76 N/A",
    "CENTRAL TOTAL 55,245,946 1,185,982 9,494,021 19.3% -471,070 1,205,800 193,750 $41.62 $47.40",
    "MONTREAL TOTALS 108,223,263 2,308,864 17,059,694 17.9% -1,079,033 1,948,460 193,750 $36.59 $43.00",
]


@pytest.fixture
def lines(monkeypatch):
    """Hand `parse_submarkets` extracted lines instead of a real PDF."""

    def use(sector_lines):
        monkeypatch.setattr(
            marketbeat, "_extract_lines", lambda _bytes: list(sector_lines)
        )

    return use


def test_industrial_gross_is_the_net_and_the_additional_rent_added(lines):
    """The two publishers quote rent differently and the gap is a quarter of
    an industrial rent - so the frame puts both on one footing."""
    lines(INDUSTRIAL_LINES)

    frame = parse_submarkets(b"", sector=INDUSTRIAL)
    total = market_total(frame)

    assert total["net_rent_psf_cad"] == 14.06
    assert total["additional_rent_psf_cad"] == 4.68
    assert total["gross_rent_psf_cad"] == pytest.approx(18.74)


def test_office_gross_is_the_published_column_and_has_no_net(lines):
    lines(OFFICE_LINES)

    total = market_total(parse_submarkets(b"", sector=OFFICE))

    assert total["gross_rent_psf_cad"] == 36.59
    assert total["premium_rent_psf_cad"] == 43.00
    assert pd.isna(total["net_rent_psf_cad"])


def test_rows_with_different_column_counts_all_parse(lines):
    """Midtown North omits the construction columns and Montreal East does
    not; reading from the right is what makes both work."""
    lines(INDUSTRIAL_LINES)

    frame = parse_submarkets(b"", sector=INDUSTRIAL).set_index("submarket")

    assert frame.loc["Montréal Midtown North", "net_rent_psf_cad"] == 12.98
    assert frame.loc["Montréal East", "net_rent_psf_cad"] == 14.05
    assert frame.loc["Lachine", "net_rent_psf_cad"] == 12.29


def test_a_sale_price_is_never_read_as_a_rent(lines):
    """`$34,900,000 / $146` ends in money and is not a submarket. It is the
    likeliest wrong number this parser could produce."""
    lines(INDUSTRIAL_LINES)

    names = set(parse_submarkets(b"", sector=INDUSTRIAL)["submarket"])

    assert not any("Carrousel" in name for name in names)
    assert len(names) == 4


def test_the_office_table_survives_its_transactions_block_being_above_it(lines):
    """The two sectors order the page differently. Cutting at the first
    transactions heading would empty this one."""
    lines(OFFICE_LINES)

    frame = parse_submarkets(b"", sector=OFFICE)

    assert len(frame) == 5
    assert "Midtown North" in set(frame["submarket"])


def test_an_n_a_class_a_cell_still_yields_a_rent(lines):
    lines(OFFICE_LINES)

    row = parse_submarkets(b"", sector=OFFICE).set_index("submarket").loc[
        "Midtown Central"
    ]

    assert row["gross_rent_psf_cad"] == 33.76
    assert pd.isna(row["premium_rent_psf_cad"])


def test_a_regional_subtotal_is_not_the_market_total(lines):
    """`CENTRAL TOTAL` is a real aggregate and is not the island."""
    lines(OFFICE_LINES)

    assert market_total(parse_submarkets(b"", sector=OFFICE))["submarket"] == (
        "MONTREAL TOTALS"
    )


def test_a_table_that_did_not_parse_is_refused(lines):
    lines(["SUBMARKET INVENTORY", "nothing here looks like a row"])

    with pytest.raises(MarketBeatError, match="No submarket row"):
        parse_submarkets(b"", sector=INDUSTRIAL)


def test_an_unknown_sector_is_refused():
    with pytest.raises(MarketBeatError, match="not a MarketBeat sector"):
        parse_submarkets(b"", sector="retail")


# -- the index --------------------------------------------------------------


def crspi_zip(rows: list[tuple[str, str, str, str]]) -> bytes:
    """A CRSPI table as Statistics Canada ships it: a zipped CSV."""
    # `GEO` and `Building Type` are quoted because both contain a comma -
    # `Montréal, Quebec` - exactly as the published table quotes them. An
    # unquoted fixture splits the city off from the province and the Montreal
    # filter then matches nothing, which is a bug in the fixture that reads
    # like a bug in the client.
    header = "REF_DATE,GEO,DGUID,Building Type,UOM,VALUE,STATUS\n"
    body = "".join(
        f'{period},"{geo}",D,"{building_type}",2019=100,{value},\n'
        for period, geo, building_type, value in rows
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("18100260.csv", header + body)
        archive.writestr("18100260_MetaData.csv", "not read")
    return buffer.getvalue()


INDEX_ROWS = [
    ("2024-01", "Montréal, Quebec", "Office buildings", "105.0"),
    ("2026-04", "Montréal, Quebec", "Office buildings", "114.2"),
    ("2024-01", "Montréal, Quebec", "Retail buildings", "100.0"),
    ("2026-04", "Montréal, Quebec", "Retail buildings", "110.0"),
    ("2024-01", "Montréal, Quebec", "Industrial buildings and warehouses", "120.0"),
    ("2026-04", "Montréal, Quebec", "Industrial buildings and warehouses", "130.9"),
    ("2026-04", "Toronto, Ontario", "Office buildings", "999.0"),
    ("2026-04", "Quebec", "Office buildings", "888.0"),
    ("2026-04", "Montréal, Quebec", "Retail buildings", ""),
]


def test_only_the_montreal_cma_rows_are_kept():
    """The table also carries the province and the city of Quebec, and neither
    is this."""
    frame = read_montreal(crspi_zip(INDEX_ROWS))

    assert index_at(frame, "office", "2026-04") == 114.2
    assert 999.0 not in set(frame["index_value"])
    assert 888.0 not in set(frame["index_value"])


def test_a_suppressed_cell_is_dropped_rather_than_read_as_zero():
    frame = read_montreal(crspi_zip(INDEX_ROWS))

    assert not (frame["index_value"] == 0).any()


def test_a_table_with_no_montreal_row_is_refused():
    with pytest.raises(CrspiError, match="no Montreal CMA row"):
        read_montreal(crspi_zip([("2026-04", "Toronto, Ontario", "Office buildings", "1")]))


def test_escalating_moves_one_series_through_time():
    frame = read_montreal(crspi_zip(INDEX_ROWS))

    level, period, basis = escalate(
        36.59, frame, building_type="office", from_period="2024-01"
    )

    assert period == "2026-04"
    assert basis == "escalated"
    assert level == pytest.approx(36.59 * 114.2 / 105.0)


def test_a_level_already_at_the_latest_quarter_is_measured_not_moved():
    frame = read_montreal(crspi_zip(INDEX_ROWS))

    level, _, basis = escalate(
        18.74, frame, building_type="industrial", from_period="2026-04"
    )

    assert basis == "measured"
    assert level == 18.74


def test_a_quarter_the_index_does_not_reach_leaves_the_level_alone():
    """A rent one quarter stale beats no rent, and the basis says which."""
    frame = read_montreal(crspi_zip(INDEX_ROWS))

    level, _, basis = escalate(
        26.0, frame, building_type="retail", from_period="1999-01"
    )

    assert basis == "unescalated"
    assert level == 26.0


def test_escalate_takes_one_building_type_and_cannot_cross_two():
    """The index is 2019=100 *per series*, so Retail/Office is relative
    movement since 2019 and not a level ratio. The signature is the guard."""
    import inspect

    parameters = inspect.signature(escalate).parameters

    assert "building_type" in parameters
    assert not any("from_building" in name for name in parameters)


def test_every_published_building_type_maps_to_a_class():
    assert set(BUILDING_TYPES.values()) == {"office", "retail", "industrial", "total"}


# -- the assets -------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


class FakeFetcher:
    """Serves the canned landing page and one PDF per sector."""

    def __init__(self, html: str):
        self.html = html
        self.calls: list[str] = []

    def landing_html(self) -> str:
        return self.html

    def report_pdf(self, report) -> bytes:
        self.calls.append(report.url)
        return b"%PDF-fake"


@pytest.fixture
def marketbeat_source(monkeypatch, lines):
    """Both MarketBeats, without a network or a real PDF.

    `_table_lines` is patched per sector by re-reading the sector off the call,
    which is what lets one fixture serve the office and industrial parsers with
    different tables.
    """
    fetcher = FakeFetcher(LANDING_HTML)
    monkeypatch.setattr(MarketBeatResource, "fetcher", lambda self: fetcher)

    original = marketbeat.parse_submarkets

    def parse(pdf_bytes, *, sector):
        monkeypatch.setattr(
            marketbeat,
            "_extract_lines",
            lambda _b: list(OFFICE_LINES if sector == OFFICE else INDUSTRIAL_LINES),
        )
        return original(pdf_bytes, sector=sector)

    monkeypatch.setattr(rent_assets, "parse_submarkets", parse)
    return fetcher


@pytest.fixture
def crspi_source(monkeypatch):
    class Fetcher:
        url = "https://example/18100260-eng.zip"

        def fetch(self) -> bytes:
            return crspi_zip(INDEX_ROWS)

    monkeypatch.setattr(CrspiResource, "fetcher", lambda self: Fetcher())


def run_sources(store) -> None:
    for asset_def in (montreal_commercial_rents, commercial_rent_index):
        result = materialize(
            [asset_def],
            partition_key=DATE,
            resources={
                "store": store,
                "marketbeat": MarketBeatResource(cache_dir=str(store.root_dir)),
                "crspi": CrspiResource(),
            },
        )
        assert result.success


def test_the_bronze_snapshot_carries_both_sectors(
    store, marketbeat_source, crspi_source
):
    run_sources(store)

    frame = pd.read_parquet(
        Path(store.partition_dir(montreal_commercial_rents.key.path[-1], DATE))
        / MARKETBEAT_FILE
    )

    assert set(frame["sector"]) == {OFFICE, INDUSTRIAL}
    assert set(frame["published_rent_basis"]) == {"gross", "net"}
    assert set(frame["report_period"]) == {"2026-Q2"}


def test_the_index_snapshot_keeps_montreal_only(store, marketbeat_source, crspi_source):
    run_sources(store)

    frame = pd.read_parquet(
        Path(store.partition_dir(commercial_rent_index.key.path[-1], DATE))
        / RENT_INDEX_FILE
    )

    assert set(frame["building_type"]) >= {"office", "retail", "industrial"}
    assert frame["period"].max() == "2026-04"


@pytest.fixture(autouse=True)
def published(monkeypatch):
    return stub_publish(monkeypatch, rent_assets)


def run_rents(store, **config):
    return materialize(
        [commercial_rents],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": NEIGHBORHOOD}),
        resources={"store": store, "postgis": PostgisResource()},
        run_config=(
            {"ops": {"silver__commercial_rents": {"config": config}}}
            if config
            else None
        ),
    )


def resolved(store) -> pd.DataFrame:
    return pd.read_parquet(
        Path(
            store.partition_dir(
                commercial_rents.key.path[-1], DATE, NEIGHBORHOOD
            )
        )
        / COMMERCIAL_RENTS_FILE
    ).set_index("rent_class")


def test_the_borough_is_priced_at_its_own_submarket(
    store, marketbeat_source, crspi_source
):
    """VSMPE is Midtown North, where office asks $22.39 against $36.59
    island-wide. Pricing a Villeray shop at the island average would overstate
    it by 60 per cent."""
    run_sources(store)
    assert run_rents(store).success

    frame = resolved(store)
    assert frame.loc["office", "submarket"] == "Midtown North"
    assert frame.loc["office", "is_submarket_rate"]
    assert frame.loc["office", "published_rent_psf_cad"] == 22.39


def test_the_industrial_rate_is_the_submarket_gross_not_its_net(
    store, marketbeat_source, crspi_source
):
    run_sources(store)
    run_rents(store)

    row = resolved(store).loc["industrial"]
    # Midtown North: $12.98 net + $4.09 additional.
    assert row["published_net_rent_psf_cad"] == 12.98
    assert row["published_additional_rent_psf_cad"] == 4.09
    assert row["published_rent_psf_cad"] == pytest.approx(17.07)


def test_the_measured_rates_are_escalated_to_the_index(
    store, marketbeat_source, crspi_source
):
    """The report is 2026-Q2 and so is the index's latest quarter, so these
    are measured rather than moved - and the column says so."""
    run_sources(store)
    run_rents(store)

    frame = resolved(store)
    assert frame.loc["office", "rent_basis"] == "measured"
    assert frame.loc["office", "index_period"] == "2026-04"


def test_retail_is_stated_and_carried_forward_by_the_index(
    store, marketbeat_source, crspi_source
):
    """Nobody publishes a free Montreal retail level, so this one is stated -
    and the basis never claims otherwise."""
    run_sources(store)
    run_rents(store, retail_base_gross_rent_psf_cad=26.0, retail_base_period="2024-01")

    row = resolved(store).loc["retail"]
    assert row["source"] == "stated_base"
    assert row["rent_basis"] == "escalated"
    # 100.0 -> 110.0 on the retail series.
    assert row["rent_psf_cad"] == pytest.approx(26.0 * 110.0 / 100.0)


def test_a_borough_with_no_submarket_falls_back_and_says_so(
    store, marketbeat_source, crspi_source, monkeypatch
):
    monkeypatch.setattr(rent_assets, "submarket_for", lambda _n: None)
    run_sources(store)
    run_rents(store)

    frame = resolved(store)
    assert not frame.loc["office", "is_submarket_rate"]
    assert frame.loc["office", "published_rent_psf_cad"] == 36.59
    assert "no MarketBeat submarket" in frame.loc["office", "note"]


def test_every_class_is_resolved_or_the_partition_fails(
    store, marketbeat_source, crspi_source
):
    """A null rate would reach a cap rate as a silently missing income term."""
    run_sources(store)
    run_rents(store)

    frame = resolved(store)
    assert set(frame.index) == {"retail", "office", "industrial"}
    assert frame["rent_psf_cad"].notna().all()


def test_a_missing_upstream_names_the_asset_to_materialize(store):
    with pytest.raises(Failure, match="materialize montreal_commercial_rents"):
        run_rents(store)


def test_the_run_reports_the_submarket_and_the_three_rates(
    store, marketbeat_source, crspi_source
):
    run_sources(store)
    result = run_rents(store)

    metadata = materialization_metadata(result, commercial_rents)
    assert metadata["submarket"].value == "Midtown North"
    assert metadata["office_rent_psf_cad"].value == pytest.approx(22.39)
    assert metadata["num_submarket_rates"].value == 2


def test_vsmpe_is_mapped_to_a_submarket_at_all():
    """The crosswalk is what makes any of the above a borough rent."""
    assert submarket_for(NEIGHBORHOOD) == "Midtown North"
    assert submarket_for("not-a-borough") is None
