"""Offline tests for the RFU snapshot and the year it picks.

Nothing here touches the network: the CKAN payload and the two CSVs are
canned, and the asset runs against a temp directory through `dagster.materialize`.
The fixtures keep the publisher's real quirks - the case of the filename drifts
between years, the companion is named two different ways, and the dataset lists
XLSX beside every CSV - because those are what the picking code is for.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest
from dagster import materialize

from asset_helpers import materialization_metadata

from urban_rag.open_data import CkanClient
from urban_rag.resources import ParquetStore, RfuResource
from urban_rag.rfu import (
    COMPARATIVE_FACTOR_COLUMN,
    RFU_YEAR_VAR,
    RfuError,
    default_rfu_year,
    pick_data_file,
    pick_postes_file,
    published_years,
)
from urban_rag.rfu_assets import POSTES_FILE, RFU_FILE, uniformized_property_wealth

DOWNLOAD_BASE = "https://www.donneesquebec.ca/recherche/dataset/abc/resource/def/download"

#: The filenames the dataset really publishes, across four years. Two spellings
#: of the data file and two of the companion.
FILENAMES = [
    "RFU-2025.csv",
    "RFU-2025.xlsx",
    "RFU-2025-DescriptionPoste.csv",
    "RFU-2024.csv",
    "RFU-2024-DescriptionPoste.csv",
    "rfu-2023.csv",
    "rfu-2023-postes.csv",
    "rfu-2023-postes.xlsx",
    "rfu-2022.csv",
]

PACKAGE_PAYLOAD = {
    "success": True,
    "result": {
        "name": "richesse-fonciere-uniformisee",
        "title": "Richesse foncière uniformisée",
        "license_title": "Attribution (CC-BY 4.0)",
        "resources": [
            {
                "id": f"id-{index}",
                "name": name,
                "format": name.rsplit(".", 1)[-1].upper(),
                "url": f"{DOWNLOAD_BASE}/{name}",
                "last_modified": "2026-04-01T09:00:00.000000",
            }
            for index, name in enumerate(FILENAMES)
        ],
    },
}

#: Montreal plus one off-island organisme, with the columns the asset reads.
#: `cod_geo` is zero-padded on purpose - 01023 has to survive as text.
RFU_CSV = (
    "cod_geo,nom_organisme,population,CIALX02140,CSALX02163\n"
    "01023,Les Îles-de-la-Madeleine,12521,2588486733,1.67\n"
    "66023,Montréal,1948747,440953403170,1.08\n"
    "66032,Westmount,20276,15000000000,1.08\n"
).encode("utf-8")

POSTES_CSV = (
    "Code de postes,Sujet,Équation\n"
    "CIALX02140,Richesse foncière uniformisée,#CIALX02149 + #CIALX02151\n"
    "CSALX02163,Facteur comparatif,\n"
).encode("cp1252")


class FakeResponse:
    def __init__(self, content: bytes, *, content_type="application/json"):
        self.content = content
        self.headers = {"Content-Type": content_type}
        self.status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return json.loads(self.content)


class FakeSession:
    """Replays the canned payloads, keyed by the tail of the URL."""

    def __init__(self, files: dict[str, bytes] | None = None, payload=None):
        self.files = {
            "RFU-2025.csv": RFU_CSV,
            "RFU-2025-DescriptionPoste.csv": POSTES_CSV,
            **(files or {}),
        }
        self.payload = payload or PACKAGE_PAYLOAD
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params or {}))
        if url.endswith("package_show"):
            return FakeResponse(json.dumps(self.payload).encode("utf-8"))
        filename = url.rsplit("/", 1)[-1]
        if filename not in self.files:
            raise AssertionError(f"unexpected download: {url}")
        return FakeResponse(
            self.files[filename], content_type="application/octet-stream"
        )


def run_asset(tmp_path, monkeypatch, session: FakeSession | None = None, **config):
    """Run the asset against a temp directory, with the portal stubbed out.

    Patched on the class rather than on an instance: Dagster rebuilds the
    resource from its config before the run, so an instance attribute would
    not survive into the asset.
    """
    session = session or FakeSession()
    monkeypatch.setattr(
        RfuResource,
        "client",
        lambda self: CkanClient(
            "https://portal", request_delay_seconds=0, session=session
        ),
    )
    result = materialize(
        [uniformized_property_wealth],
        partition_key="2026-08-30",
        resources={
            "rfu": RfuResource(**config),
            "store": ParquetStore(root_dir=str(tmp_path)),
        },
    )
    return result, session


# -- picking the year ------------------------------------------------------


def test_published_years_reads_both_spellings_of_the_filename():
    # The publisher upper-cased the stem between 2023 and 2024; both are the
    # same file and both have to be found.
    assert published_years(FILENAMES) == {
        2025: "RFU-2025.csv",
        2024: "RFU-2024.csv",
        2023: "rfu-2023.csv",
        2022: "rfu-2022.csv",
    }


def test_the_companion_and_the_xlsx_are_not_mistaken_for_the_data_file():
    # `RFU-2025-DescriptionPoste.csv` and `RFU-2025.xlsx` both start the same
    # way as the data file; neither is it.
    assert 2025 in published_years(["RFU-2025.csv"])
    assert published_years(["RFU-2025-DescriptionPoste.csv", "RFU-2025.xlsx"]) == {}


def test_no_year_asked_for_takes_the_latest_published():
    assert pick_data_file(FILENAMES) == (2025, "RFU-2025.csv")


def test_an_explicit_year_is_honoured_whatever_its_case():
    assert pick_data_file(FILENAMES, 2023) == (2023, "rfu-2023.csv")


def test_an_unpublished_year_names_the_ones_that_are():
    with pytest.raises(RfuError, match="2019.*2022, 2023, 2024, 2025"):
        pick_data_file(FILENAMES, 2019)


def test_a_dataset_with_no_rfu_file_at_all_is_an_error():
    with pytest.raises(RfuError, match="publishes no rfu-<year>.csv"):
        pick_data_file(["something-else.csv"])


def test_the_companion_is_found_under_either_of_its_names():
    assert pick_postes_file(FILENAMES, 2025) == "RFU-2025-DescriptionPoste.csv"
    assert pick_postes_file(FILENAMES, 2023) == "rfu-2023-postes.csv"


def test_a_year_without_descriptions_is_none_rather_than_an_error():
    # 2022 publishes data and no companion. That is documentation missing, not
    # a factor missing, so it must not cost the partition.
    assert pick_postes_file(FILENAMES, 2022) is None


def test_the_year_variable_is_read_per_call(monkeypatch):
    monkeypatch.delenv(RFU_YEAR_VAR, raising=False)
    assert default_rfu_year() is None
    monkeypatch.setenv(RFU_YEAR_VAR, "2024")
    assert default_rfu_year() == 2024


def test_a_year_variable_that_is_not_a_year_is_refused(monkeypatch):
    monkeypatch.setenv(RFU_YEAR_VAR, "latest")
    with pytest.raises(RfuError, match="is not a year"):
        default_rfu_year()


# -- the asset -------------------------------------------------------------


def test_the_snapshot_lands_with_both_files(tmp_path, monkeypatch):
    result, _ = run_asset(tmp_path, monkeypatch)

    assert result.success
    partition = tmp_path / "bronze" / "uniformized_property_wealth" / "2026-08-30"
    assert (partition / RFU_FILE).exists()
    assert (partition / POSTES_FILE).exists()


def test_the_montreal_factor_is_reported_as_metadata(tmp_path, monkeypatch):
    result, _ = run_asset(tmp_path, monkeypatch)

    metadata = materialization_metadata(result, uniformized_property_wealth)
    # The number the whole asset exists for: what a roll value is multiplied
    # by to read as a market one.
    assert metadata["montreal_comparative_factor"].value == pytest.approx(1.08)
    assert metadata["rfu_year"].value == 2025
    assert metadata["num_organismes"].value == 3
    assert metadata["license"].value == "Attribution (CC-BY 4.0)"


def test_the_geographic_code_keeps_its_leading_zero(tmp_path, monkeypatch):
    run_asset(tmp_path, monkeypatch)

    frame = pd.read_parquet(
        tmp_path / "bronze" / "uniformized_property_wealth" / "2026-08-30" / RFU_FILE
    )
    # It joins against the roll's `code_mun`, which is text. Inferred as an
    # integer, 01023 becomes 1023 and matches nothing.
    assert set(frame["cod_geo"]) == {"01023", "66023", "66032"}


def test_the_publishers_column_names_survive(tmp_path, monkeypatch):
    run_asset(tmp_path, monkeypatch)

    frame = pd.read_parquet(
        tmp_path / "bronze" / "uniformized_property_wealth" / "2026-08-30" / RFU_FILE
    )
    # Mixed case as MAMH spells it: the codes are what the companion table is
    # keyed on, so lower-casing them here would break that join.
    assert COMPARATIVE_FACTOR_COLUMN in frame.columns
    assert "cod_geo" in frame.columns
    assert {"source_file", "rfu_year", "scrape_date", "scraped_at"} <= set(
        frame.columns
    )
    assert set(frame["scrape_date"]) == {"2026-08-30"}


def test_a_cp1252_companion_is_decoded(tmp_path, monkeypatch):
    # MAMH publishes the descriptions in the Windows codepage, not UTF-8.
    run_asset(tmp_path, monkeypatch)

    postes = pd.read_parquet(
        tmp_path / "bronze" / "uniformized_property_wealth" / "2026-08-30" / POSTES_FILE
    )
    assert "Facteur comparatif" in set(postes["Sujet"])


def test_an_explicit_year_fetches_that_years_file(tmp_path, monkeypatch):
    session = FakeSession(
        files={"rfu-2023.csv": RFU_CSV, "rfu-2023-postes.csv": POSTES_CSV}
    )
    result, session = run_asset(tmp_path, monkeypatch, session=session, rfu_year=2023)

    metadata = materialization_metadata(result, uniformized_property_wealth)
    assert metadata["rfu_year"].value == 2023
    downloaded = [url.rsplit("/", 1)[-1] for url, _ in session.calls[1:]]
    assert downloaded == ["rfu-2023.csv", "rfu-2023-postes.csv"]


def test_a_file_without_the_factor_column_fails_the_partition(tmp_path, monkeypatch):
    # A valid RFU minus the one column the asset is read for. Writing the
    # snapshot anyway would leave a partition that looks materialized and
    # answers nothing.
    session = FakeSession(
        files={"RFU-2025.csv": b"cod_geo,nom_organisme\n66023,Montreal\n"}
    )
    with pytest.raises(Exception, match=COMPARATIVE_FACTOR_COLUMN):
        run_asset(tmp_path, monkeypatch, session=session)


def test_montreal_missing_is_reported_rather_than_raised(tmp_path, monkeypatch):
    # The province publishing a file Montreal is not in is a fact about the
    # publication; the snapshot is still faithful, so the run stands.
    session = FakeSession(
        files={
            "RFU-2025.csv": (
                "cod_geo,nom_organisme,CSALX02163\n01023,Les Îles,1.67\n"
            ).encode("utf-8")
        }
    )
    result, _ = run_asset(tmp_path, monkeypatch, session=session)

    assert result.success
    metadata = materialization_metadata(result, uniformized_property_wealth)
    assert metadata["montreal_comparative_factor"].value == "absent"


def test_a_year_without_descriptions_still_writes_the_data(tmp_path, monkeypatch):
    payload = json.loads(json.dumps(PACKAGE_PAYLOAD))
    payload["result"]["resources"] = [
        item
        for item in payload["result"]["resources"]
        if item["name"] == "RFU-2025.csv"
    ]
    result, _ = run_asset(tmp_path, monkeypatch, session=FakeSession(payload=payload))

    assert result.success
    partition = tmp_path / "bronze" / "uniformized_property_wealth" / "2026-08-30"
    assert (partition / RFU_FILE).exists()
    assert not (partition / POSTES_FILE).exists()
    metadata = materialization_metadata(result, uniformized_property_wealth)
    assert "no descriptions published" in metadata["postes_error"].value
