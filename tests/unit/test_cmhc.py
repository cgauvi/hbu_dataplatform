"""Offline tests for the CMHC survey client and the vacancy-rate asset.

Nothing here touches the network: the workbook is built cell by cell with
openpyxl in the shape the real `Quartier` sheet has - a title, a reference
month, then a header row over five (rate, reliability) pairs - and the asset
runs against a temp directory through `dagster.materialize`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from dagster import Failure, MultiPartitionKey, materialize
from openpyxl import Workbook

from urban_rag.cmhc import (
    AVERAGE_RENTS_READING_MODE_URL,
    QUARTIER_SHEET,
    CmhcError,
    CmhcFetcher,
    CmhcReadingModeFetcher,
    filename_for,
    normalize_quartier,
    read_average_rents_reading_mode,
    read_quartier_sheet,
    strip_bilingual,
    survey_period,
)
from urban_rag.cmhc_assets import (
    AVERAGE_RENTS_FILE,
    QUARTIER_AVERAGE_RENTS_FILE,
    QUARTIERS_FILE,
    VACANCY_FILE,
    average_rents,
    vacancy_rates,
)
from urban_rag.partitions import CMHC_QUARTIERS, quartiers_for
from urban_rag.resources import CmhcResource, ParquetStore
from urban_rag.storage import join

DATE = "2026-08-20"
NEIGHBORHOOD = "VSMPE"
SURVEY_YEAR = 2023

#: The header labels the real workbook carries, newlines included - two of
#: them wrap mid-cell, which is what `read_quartier_sheet` has to collapse.
HEADER = (
    "Province",
    "Centre",
    "Zone",
    "Quartier",
    "Type de\nlogement",
    "Studios",
    None,
    "1 chambre",
    None,
    "2 chambres",
    None,
    "3 chambres\n+",
    None,
    "Tous les\nlog.",
    None,
)

ZONE = "Villeray/St-Michel/Pc-Extension"

#: One row per (quartier, type de logement), five (rate, grade) pairs wide.
#: Parc-Extension and Villeray both publish a "tous les log." rate so the
#: average has two values to take; Saint-Michel suppresses everything, and
#: the row-house line is `--` throughout, as it is in the real sheet.
ROWS = [
    (ZONE, "Parc-Extension", "En bande", "--", "--", "--", "--", "--"),
    (ZONE, "Parc-Extension", "App. & autres", "**", "**", "0.2%", "**", "0.3%"),
    (ZONE, "Parc-Extension", "Total", "**", "**", "0.2%", "**", "0.3%"),
    (ZONE, "Villeray", "En bande", "--", "--", "--", "--", "--"),
    (ZONE, "Villeray", "App. & autres", "**", "**", "0.6%", "**", "0.7%"),
    (ZONE, "Villeray", "Total", "**", "**", "0.6%", "**", "0.7%"),
    (ZONE, "Saint-Michel", "En bande", "--", "--", "--", "--", "--"),
    (ZONE, "Saint-Michel", "App. & autres", "**", "0.1%", "**", "**", "**"),
    (ZONE, "Saint-Michel", "Total", "**", "0.1%", "**", "**", "**"),
    # The zone subtotal, which must not be averaged in alongside its parts.
    (ZONE, "Total", "Total", "**", "0.1%", "0.4%", "**", "0.5%"),
]

AVERAGE_RENTS_HTML = """
<html>
  <body>
    <h1>Montr\u00e9al \u2014 Average Rent by Bedroom Type by Neighbourhood</h1>
    <section>
      <h2>Reference Period</h2>
      <button>previous</button><button>next</button>
      <span>2023</span><span>2024</span><span>2025</span>
      <button>Apply</button>
    </section>
    <table>
      <tr>
        <th></th>
        <th>Studio</th><th>1 Bedroom</th><th>2 Bedroom</th>
        <th>3 Bedroom +</th><th>Total</th>
      </tr>
      <tr>
        <td>Montr\u00e9al</td>
        <td>1,005</td><td>a</td><td>1,131</td><td>a</td>
        <td>1,346</td><td>a</td><td>1,625</td><td>a</td>
        <td>1,291</td><td>a</td>
      </tr>
      <tr>
        <td>Parc-Extension</td>
        <td>737</td><td>c</td><td>941</td><td>c</td>
        <td>**</td><td></td><td>**</td><td></td>
        <td>1,028</td><td>d</td>
      </tr>
      <tr>
        <td>Saint-Michel</td>
        <td>**</td><td></td><td>**</td><td></td>
        <td>1,184</td><td>d</td><td>**</td><td></td>
        <td>1,121</td><td>d</td>
      </tr>
      <tr>
        <td>Villeray</td>
        <td>906</td><td>c</td><td>1,019</td><td>b</td>
        <td>1,504</td><td>d</td><td>**</td><td></td>
        <td>1,439</td><td>c</td>
      </tr>
    </table>
  </body>
</html>
"""


def write_workbook(
    path: Path, *, rows=ROWS, header=HEADER, period="octobre 2023"
) -> Path:
    """A workbook shaped like the published one, holding ``rows``."""
    book = Workbook()
    sheet = book.active
    sheet.title = QUARTIER_SHEET
    sheet.append(["Taux d'inoccupation ... selon le quartier"])
    sheet.append([period])
    sheet.append([])
    sheet.append(list(header))
    for zone, quartier, dwelling, *rates in rows:
        line: list[str] = ["Qc", "Montréal", zone, quartier, dwelling]
        for rate in rates:
            # Cells with no rate carry an apostrophe where the grade goes,
            # exactly as the real sheet does.
            line += [rate, "b" if rate.endswith("%") else "'"]
        sheet.append(line)
    sheet.append([])
    sheet.append(["© 2024 Société canadienne d'hypothèques et de logement"])
    sheet.append(["** Donnée non fournie pour des raisons de confidentialité"])
    book.save(path)
    return path


@pytest.fixture
def workbook(tmp_path) -> Path:
    return write_workbook(tmp_path / filename_for(SURVEY_YEAR))


class FakeResponse:
    def __init__(self, content: bytes, *, content_type: str = "application/xlsx"):
        self.content = content
        self.text = content.decode("utf-8", errors="replace")
        self.headers = {"Content-Type": content_type}
        self.status_code = 200

    def raise_for_status(self):
        return None


class FakeSession:
    """Replays canned bytes, keyed by the tail of the URL."""

    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.calls: list[str] = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        filename = url.rsplit("/", 1)[-1]
        if filename not in self.files:
            raise AssertionError(f"unexpected download: {url}")
        return FakeResponse(self.files[filename])


class FakeReadingModeFetcher:
    def __init__(self, html: str = AVERAGE_RENTS_HTML):
        self.html = html

    def fetch_average_rents(self) -> str:
        return self.html


# -- client -----------------------------------------------------------------


def test_fetch_downloads_and_caches_by_survey_year(tmp_path, workbook):
    session = FakeSession({filename_for(SURVEY_YEAR): workbook.read_bytes()})
    fetcher = CmhcFetcher(
        cache_dir=tmp_path / "cache",
        base_url="https://example/vacancy",
        request_delay_seconds=0,
        session=session,
    )

    path = fetcher.fetch(SURVEY_YEAR)

    assert path == tmp_path / "cache" / filename_for(SURVEY_YEAR)
    assert session.calls == [f"https://example/vacancy/{filename_for(SURVEY_YEAR)}"]

    # A second fetch reuses the cache instead of downloading again.
    fetcher.fetch(SURVEY_YEAR)
    assert len(session.calls) == 1


def test_reading_mode_fetches_the_average_rent_page():
    key = AVERAGE_RENTS_READING_MODE_URL.rsplit("/", 1)[-1]
    session = FakeSession({key: AVERAGE_RENTS_HTML.encode("utf-8")})
    fetcher = CmhcReadingModeFetcher(
        average_rents_url=AVERAGE_RENTS_READING_MODE_URL,
        request_delay_seconds=0,
        session=session,
    )

    assert "Average Rent by Bedroom Type" in fetcher.fetch_average_rents()
    assert session.calls == [AVERAGE_RENTS_READING_MODE_URL]


def test_a_non_xlsx_response_is_rejected(tmp_path):
    session = FakeSession({filename_for(2099): b"<html>404 not found</html>"})
    fetcher = CmhcFetcher(
        cache_dir=tmp_path / "cache",
        base_url="https://example/vacancy",
        request_delay_seconds=0,
        session=session,
    )

    with pytest.raises(CmhcError, match="not an xlsx"):
        fetcher.fetch(2099)


def test_the_sheet_is_unpivoted_to_one_row_per_bedroom_class(workbook):
    survey = read_quartier_sheet(workbook)

    # 10 source rows x 5 bedroom classes; the footer contributes nothing.
    assert len(survey) == len(ROWS) * 5
    assert set(survey["quartier"]) == {
        "Parc-Extension",
        "Villeray",
        "Saint-Michel",
        "Total",
    }
    assert set(survey["dwelling_type"]) == {"row", "apartment_other", "all"}

    published = survey[survey["status"] == "published"]
    rate = published[
        (published["quartier"] == "Parc-Extension")
        & (published["dwelling_type"] == "all")
        & (published["bedroom_type"] == "all")
    ]
    # Percent as published, not a fraction.
    assert rate["vacancy_rate_pct"].iloc[0] == pytest.approx(0.3)
    assert rate["reliability"].iloc[0] == "b"


def test_reading_mode_average_rents_are_unpivoted_by_bedroom_type():
    table = read_average_rents_reading_mode(AVERAGE_RENTS_HTML)
    rents = table.frame

    assert table.survey_year == 2025
    assert table.survey_period == "October 2025"
    assert len(rents) == 20
    assert set(rents["quartier"]) == {
        "Montr\u00e9al",
        "Parc-Extension",
        "Saint-Michel",
        "Villeray",
    }

    parc = rents[
        (rents["quartier"] == "Parc-Extension")
        & (rents["bedroom_type"] == "studio")
    ].iloc[0]
    assert parc["average_rent_cad"] == pytest.approx(737)
    assert parc["reliability"] == "c"
    assert parc["status"] == "published"

    suppressed = rents[
        (rents["quartier"] == "Parc-Extension")
        & (rents["bedroom_type"] == "2_bedroom")
    ].iloc[0]
    assert pd.isna(suppressed["average_rent_cad"])
    assert suppressed["status"] == "suppressed"
    assert pd.isna(suppressed["reliability"])


def test_the_two_kinds_of_missing_rate_stay_distinct(workbook):
    survey = read_quartier_sheet(workbook)
    parc = survey[survey["quartier"] == "Parc-Extension"]

    # `--` is a structural zero, `**` a rate withheld - neither is a 0.0.
    assert set(parc[parc["dwelling_type"] == "row"]["status"]) == {"no_units"}
    withheld = parc[
        (parc["dwelling_type"] == "apartment_other")
        & (parc["bedroom_type"] == "studio")
    ]
    assert withheld["status"].iloc[0] == "suppressed"
    assert survey["vacancy_rate_pct"][survey["status"] != "published"].isna().all()
    # The apostrophe standing in for an absent grade is not read as one.
    assert survey["reliability"][survey["status"] != "published"].isna().all()


def test_an_unknown_bedroom_column_is_rejected(tmp_path):
    header = list(HEADER)
    header[7] = "1.5 chambre"
    path = write_workbook(tmp_path / "odd.xlsx", header=header)

    with pytest.raises(CmhcError, match="unknown bedroom column"):
        read_quartier_sheet(path)


def test_a_missing_bedroom_column_is_rejected(tmp_path):
    header = list(HEADER)[:-2]  # drop "Tous les log." and its grade column
    rows = [row[:-1] for row in ROWS]
    path = write_workbook(tmp_path / "short.xlsx", header=header, rows=rows)

    with pytest.raises(CmhcError, match="missing: all"):
        read_quartier_sheet(path)


def test_a_bilingual_label_keeps_its_french_half(tmp_path):
    """The 2022 workbook prints some names as `English ~ French`."""
    bilingual = [
        (zone, "South West ~ Sud-Ouest" if q == "Villeray" else q, *rest)
        for zone, q, *rest in ROWS
    ]
    survey = read_quartier_sheet(
        write_workbook(tmp_path / "bilingual.xlsx", rows=bilingual)
    )

    assert "Sud-Ouest" in set(survey["quartier"])
    assert not any(" ~ " in name for name in survey["quartier"])


@pytest.mark.parametrize(
    "published, mapped",
    [
        # Both respellings the 2022 workbook uses against the 2023 names.
        ("South West ~ Sud-Ouest", "Sud-Ouest"),
        ("South West", "Sud-Ouest"),
        ("Senneville/Roxboro-Pierrefonds", "Senneville-Roxboro-Pierrefonds"),
        ("East Ville-Marie ~ Ville-Marie Est", "Ville-Marie Est"),
        ("East Ville-Marie", "Ville-Marie Est"),
        ("Cote-des-Neiges", "Côte-des-Neiges"),
    ],
)
def test_respellings_match_the_crosswalk(published, mapped):
    assert normalize_quartier(published) == normalize_quartier(mapped)


def test_normalization_still_separates_distinct_quartiers():
    assert normalize_quartier("Ville-Marie") != normalize_quartier("Ville-Marie Est")
    assert normalize_quartier("Mercier") != normalize_quartier("Mercier Est")
    # Every name the crosswalk claims stays distinct under it.
    claimed = [q for quartiers in CMHC_QUARTIERS.values() for q in quartiers]
    assert len({normalize_quartier(q) for q in claimed}) == len(claimed)


def test_a_name_with_no_bilingual_half_is_left_alone():
    assert strip_bilingual("Parc-Extension") == "Parc-Extension"


def test_two_names_collapsing_to_one_key_are_rejected(tmp_path):
    colliding = [
        (zone, "Parc Extension" if q == "Villeray" else q, *rest)
        for zone, q, *rest in ROWS
    ]
    path = write_workbook(tmp_path / "collide.xlsx", rows=colliding)

    with pytest.raises(CmhcError, match="differ only in punctuation"):
        read_quartier_sheet(path)


def test_the_reference_month_is_read_off_the_sheet(workbook, tmp_path):
    assert survey_period(workbook) == "octobre 2023"
    # Absent is not an error: nothing downstream is keyed on it.
    assert survey_period(write_workbook(tmp_path / "np.xlsx", period="")) is None


# -- the borough crosswalk --------------------------------------------------


def test_every_mapped_borough_is_a_known_partition_key():
    from urban_rag.partitions import NEIGHBORHOOD_NAMESPACES

    assert set(CMHC_QUARTIERS) == set(NEIGHBORHOOD_NAMESPACES)


def test_no_quartier_is_claimed_by_two_boroughs():
    claimed = [q for quartiers in CMHC_QUARTIERS.values() for q in quartiers]
    assert len(claimed) == len(set(claimed))


def test_an_unmapped_borough_names_the_keys_it_knows():
    with pytest.raises(KeyError, match="No CMHC quartier mapping"):
        quartiers_for("Nowhere")


# -- asset ------------------------------------------------------------------


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture
def cache(tmp_path, workbook) -> str:
    """A cache dir already holding the workbook, so no fetch is attempted."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / workbook.name).write_bytes(workbook.read_bytes())
    return str(cache_dir)


def run(store, cache, *, neighborhood=NEIGHBORHOOD):
    return materialize(
        [vacancy_rates],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": neighborhood}),
        resources={
            "cmhc": CmhcResource(cache_dir=cache, survey_year=SURVEY_YEAR),
            "store": store,
        },
    )


def read_output(store, filename, *, neighborhood=NEIGHBORHOOD):
    return pd.read_parquet(
        join(
            store.partition_dir(vacancy_rates.key.path[-1], DATE, neighborhood),
            filename,
        )
    )


def run_rents(store, cache, monkeypatch, *, neighborhood=NEIGHBORHOOD):
    monkeypatch.setattr(
        CmhcResource,
        "reading_mode_fetcher",
        lambda self: FakeReadingModeFetcher(),
    )
    return materialize(
        [average_rents],
        partition_key=MultiPartitionKey({"date": DATE, "neighborhood": neighborhood}),
        resources={
            "cmhc": CmhcResource(cache_dir=cache),
            "store": store,
        },
    )


def read_rent_output(store, filename, *, neighborhood=NEIGHBORHOOD):
    return pd.read_parquet(
        join(
            store.partition_dir(average_rents.key.path[-1], DATE, neighborhood),
            filename,
        )
    )


def test_rates_land_under_date_then_neighborhood(store, cache):
    assert run(store, cache).success

    averages = read_output(store, VACANCY_FILE)
    # The full grid, published or not: 3 dwelling types x 5 bedroom classes.
    assert len(averages) == 15
    assert set(averages["neighborhood"]) == {NEIGHBORHOOD}
    assert set(averages["scrape_date"]) == {DATE}
    assert set(averages["survey_year"]) == {SURVEY_YEAR}
    assert set(averages["survey_period"]) == {"octobre 2023"}


def test_the_borough_rate_is_the_mean_of_its_quartiers(store, cache):
    run(store, cache)
    averages = read_output(store, VACANCY_FILE)

    overall = averages[
        (averages["dwelling_type"] == "all") & (averages["bedroom_type"] == "all")
    ].iloc[0]

    # (0.3 + 0.7) / 2; Saint-Michel suppresses its own and drops out.
    assert overall["vacancy_rate_pct"] == pytest.approx(0.5)
    assert overall["min_vacancy_rate_pct"] == pytest.approx(0.3)
    assert overall["max_vacancy_rate_pct"] == pytest.approx(0.7)
    assert overall["num_quartiers"] == 2
    assert overall["averaged_quartiers"] == "Parc-Extension, Villeray"
    # How many the map claims, against how many had a rate to average.
    assert overall["num_quartiers_mapped"] == 3


def test_the_zone_subtotal_is_not_averaged_in(store, cache):
    run(store, cache)

    quartiers = read_output(store, QUARTIERS_FILE)
    assert "Total" not in set(quartiers["quartier"])
    assert set(quartiers["quartier"]) == set(quartiers_for(NEIGHBORHOOD))


def test_a_fully_suppressed_cell_is_a_row_rather_than_an_absence(store, cache):
    run(store, cache)
    averages = read_output(store, VACANCY_FILE)

    row_houses = averages[averages["dwelling_type"] == "row"]
    assert len(row_houses) == 5
    assert row_houses["vacancy_rate_pct"].isna().all()
    assert (row_houses["num_quartiers"] == 0).all()
    assert (row_houses["averaged_quartiers"] == "").all()


def test_the_quartier_rows_behind_the_average_are_kept(store, cache):
    run(store, cache)
    quartiers = read_output(store, QUARTIERS_FILE)

    # 3 quartiers x 3 dwelling types x 5 bedroom classes.
    assert len(quartiers) == 45
    assert set(quartiers["centre"]) == {"Montréal"}
    assert set(quartiers["province"]) == {"Qc"}
    assert set(quartiers["neighborhood"]) == {NEIGHBORHOOD}


def test_a_respelled_quartier_is_averaged_and_relabelled(store, cache, tmp_path):
    respelled = [
        (zone, "South West ~ Villeray" if q == "Villeray" else q, *rest)
        for zone, q, *rest in ROWS
    ]
    path = Path(cache) / filename_for(SURVEY_YEAR)
    path.unlink()
    write_workbook(path, rows=respelled)

    run(store, cache)
    averages = read_output(store, VACANCY_FILE)
    overall = averages[
        (averages["dwelling_type"] == "all") & (averages["bedroom_type"] == "all")
    ].iloc[0]

    # Still both quartiers, under the crosswalk's own spelling.
    assert overall["num_quartiers"] == 2
    assert overall["averaged_quartiers"] == "Parc-Extension, Villeray"


def test_a_renamed_quartier_fails_the_partition(store, cache, tmp_path, monkeypatch):
    renamed = [
        (zone, "Parc-Ext." if q == "Parc-Extension" else q, *rest)
        for zone, q, *rest in ROWS
    ]
    path = Path(cache) / filename_for(SURVEY_YEAR)
    path.unlink()
    write_workbook(path, rows=renamed)

    with pytest.raises(Failure, match="publishes no quartier named"):
        run(store, cache)


def test_a_rerun_replaces_the_partition(store, cache):
    run(store, cache)
    stale = join(
        store.partition_dir(vacancy_rates.key.path[-1], DATE, NEIGHBORHOOD),
        "leftover.parquet",
    )
    pd.DataFrame({"a": [1]}).to_parquet(stale)

    run(store, cache)

    assert not Path(stale).exists()


def test_average_rents_land_under_date_then_neighborhood(store, cache, monkeypatch):
    assert run_rents(store, cache, monkeypatch).success

    averages = read_rent_output(store, AVERAGE_RENTS_FILE)
    assert len(averages) == 5
    assert set(averages["neighborhood"]) == {NEIGHBORHOOD}
    assert set(averages["scrape_date"]) == {DATE}
    assert set(averages["survey_year"]) == {2025}
    assert set(averages["survey_period"]) == {"October 2025"}


def test_average_rent_is_the_mean_of_its_quartiers(store, cache, monkeypatch):
    run_rents(store, cache, monkeypatch)
    averages = read_rent_output(store, AVERAGE_RENTS_FILE)

    overall = averages[averages["bedroom_type"] == "all"].iloc[0]

    # (1028 + 1121 + 1439) / 3; all three quartiers publish a total rent.
    assert overall["average_rent_cad"] == pytest.approx(1196)
    assert overall["min_average_rent_cad"] == pytest.approx(1028)
    assert overall["max_average_rent_cad"] == pytest.approx(1439)
    assert overall["num_quartiers"] == 3
    assert overall["averaged_quartiers"] == "Parc-Extension, Saint-Michel, Villeray"
    assert overall["num_quartiers_mapped"] == 3

    two_bedroom = averages[averages["bedroom_type"] == "2_bedroom"].iloc[0]
    assert two_bedroom["average_rent_cad"] == pytest.approx(1344)
    assert two_bedroom["averaged_quartiers"] == "Saint-Michel, Villeray"


def test_average_rent_quartier_rows_are_kept(store, cache, monkeypatch):
    run_rents(store, cache, monkeypatch)
    quartiers = read_rent_output(store, QUARTIER_AVERAGE_RENTS_FILE)

    # 3 quartiers x 5 bedroom classes.
    assert len(quartiers) == 15
    assert set(quartiers["centre"]) == {"Montr\u00e9al"}
    assert set(quartiers["neighborhood"]) == {NEIGHBORHOOD}
    assert set(quartiers["quartier"]) == set(quartiers_for(NEIGHBORHOOD))


def test_a_fully_suppressed_rent_cell_is_a_row(store, cache, monkeypatch):
    run_rents(store, cache, monkeypatch)
    averages = read_rent_output(store, AVERAGE_RENTS_FILE)

    three_bedroom = averages[averages["bedroom_type"] == "3_bedroom_plus"].iloc[0]
    assert pd.isna(three_bedroom["average_rent_cad"])
    assert three_bedroom["num_quartiers"] == 0
    assert three_bedroom["averaged_quartiers"] == ""


def test_an_average_rent_rerun_replaces_the_partition(store, cache, monkeypatch):
    run_rents(store, cache, monkeypatch)
    stale = join(
        store.partition_dir(average_rents.key.path[-1], DATE, NEIGHBORHOOD),
        "leftover.parquet",
    )
    pd.DataFrame({"a": [1]}).to_parquet(stale)

    run_rents(store, cache, monkeypatch)

    assert not Path(stale).exists()
