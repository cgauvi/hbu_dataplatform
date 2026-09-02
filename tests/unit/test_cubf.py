"""Offline tests for the MEFQ codebook client and the bronze asset over it.

Nothing here touches the network. The workbook is stubbed by writing a real
xlsx with openpyxl - the sheet laid out the way the ministry lays it out, free
notice above the header and signature block below it - so what is under test is
the header search, the hierarchy filter and the code normalisation, not a
mocked `read_excel`.

The fixture sheet carries one of each shape the real one does: a category, a
rubric and a subgroup above the codes; a four-digit code with a SCIAN and a
remark; one numbered but left undescribed, the way ``9800`` is; and the
``2-3`` row, which is the reason `cubf` is read as text at all - manufacturing
spans two leading digits and the manual writes it on one line.
"""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import openpyxl
import pandas as pd
import pytest
from dagster import Failure, materialize

from asset_helpers import materialization_metadata

from urban_rag.cubf import (
    LISTE_SHEET,
    CubfError,
    CubfFetcher,
    describe,
    edition_of,
    read_liste,
    sheet_names,
    use_code_descriptions,
    use_code_key,
)
from urban_rag.cubf_assets import CUBF_FILE, cubf_use_codes
from urban_rag.resources import CubfResource, ParquetStore

DATE = "2026-08-01"

GARAGE = "Garage de stationnement pour automobiles (infrastructure)"

#: The sheet, as the ministry lays it out. Row 0 is the free-text notice the
#: header search has to step over; the four columns start on row 1.
SHEET_ROWS: list[tuple] = [
    (
        "En cas de divergence avec le MEFQ (édition 2025), ce dernier a "
        "préséance sur la liste numérique",
        None,
        None,
        None,
    ),
    ("CUBF", "SCIAN", "DESCRIPTION", "REMARQUE"),
    (1, None, "RÉSIDENTIELLE", None),
    (10, None, "LOGEMENT", None),
    (100, None, "Logement", None),
    (1000, "000999", "Logement", "Un logement est une maison, un appartement…"),
    (1010, None, "Logements sociaux et abordables", None),
    # Manufacturing is 2000-3999 and the manual writes it on one row, which is
    # why the column cannot be a number.
    ("2-3", None, "INDUSTRIES MANUFACTURIÈRES", None),
    (46, None, "TERRAIN ET GARAGE DE STATIONNEMENT POUR VÉHICULES", None),
    (461, None, GARAGE, None),
    (4611, 812930, GARAGE, None),
    (98, None, "RUBRIQUE TEMPORAIRE POUR NOUVEAUX USAGES", None),
    # Numbered, and deliberately undescribed: a slot held open for a use the
    # manual has not named yet.
    (9800, None, None, None),
    (None, None, None, None),
    (
        "Direction générale de la fiscalité et de l'évaluation foncière",
        None,
        None,
        None,
    ),
    ("Ministère des Affaires municipales et de l'Habitation", None, None, None),
    (datetime(2025, 7, 14), None, None, None),
]


def workbook_bytes(rows=SHEET_ROWS, *, sheet: str = LISTE_SHEET, extra=("MAJ2025",)):
    """A real xlsx holding ``rows`` on ``sheet``, plus the change-log sheets."""
    book = openpyxl.Workbook()
    book.active.title = sheet
    for row in rows:
        book.active.append(list(row))
    for name in extra:
        book.create_sheet(name).append(["CUBF", "MODIFICATION"])
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


# -- the client -------------------------------------------------------------


def test_the_hierarchy_rows_survive_into_bronze_beside_the_codes():
    """Bronze keeps the sheet. Selecting the leaves is silver's job."""
    frame = read_liste(workbook_bytes())

    assert list(frame.columns) == ["cubf", "scian", "description", "remarque"]
    assert "1" in set(frame["cubf"])
    assert "100" in set(frame["cubf"])
    assert "1000" in set(frame["cubf"])


def test_the_signature_block_is_dropped_and_the_undescribed_code_is_not():
    """Both have no description; only one of them is a code."""
    frame = read_liste(workbook_bytes())

    assert "9800" in set(frame["cubf"])
    assert not any(
        str(value).startswith("Ministère") for value in frame["cubf"].dropna()
    )
    assert not any(
        str(value).startswith("Direction") for value in frame["cubf"].dropna()
    )


def test_the_two_three_row_survives_because_the_column_is_text():
    """Manufacturing spans two leading digits, so `cubf` cannot be a number."""
    frame = read_liste(workbook_bytes())

    row = frame[frame["cubf"] == "2-3"]
    assert len(row) == 1
    assert row["description"].iloc[0] == "INDUSTRIES MANUFACTURIÈRES"


def test_the_header_is_found_by_value_and_not_by_index():
    """An edition that adds a line of notice must not shift every column."""
    rows = [("Un avis de plus", None, None, None), *SHEET_ROWS]

    frame = read_liste(workbook_bytes(rows))

    assert use_code_descriptions(frame)["4611"] == GARAGE


def test_a_workbook_with_no_header_row_is_refused():
    rows = [row for row in SHEET_ROWS if row[0] != "CUBF"]

    with pytest.raises(CubfError, match="CUBF"):
        read_liste(workbook_bytes(rows))


def test_a_workbook_without_the_liste_sheet_is_refused():
    with pytest.raises(CubfError, match=LISTE_SHEET):
        read_liste(workbook_bytes(sheet="AUTRE"))


def test_the_change_logs_are_named_and_not_read():
    assert sheet_names(workbook_bytes()) == (LISTE_SHEET, "MAJ2025")


def test_the_edition_is_read_off_the_notice():
    assert edition_of(workbook_bytes()) == "2025"


def test_a_reworded_notice_costs_the_edition_and_not_the_snapshot():
    rows = [("Quelque chose d'autre", None, None, None), *SHEET_ROWS[1:]]

    assert edition_of(workbook_bytes(rows)) is None
    assert use_code_descriptions(read_liste(workbook_bytes(rows)))["4611"] == GARAGE


# -- the codes ---------------------------------------------------------------


def test_only_the_four_character_rows_are_use_codes():
    codes = use_code_descriptions(read_liste(workbook_bytes()))

    assert codes["1000"] == "Logement"
    assert codes["1010"] == "Logements sociaux et abordables"
    assert codes["4611"] == GARAGE
    # The hierarchy above them, and the row that is not a number at all.
    assert not {"1", "10", "100", "46", "461", "2-3"} & set(codes)
    # Numbered but undescribed: the point of the column is the text.
    assert "9800" not in codes


def test_a_heading_is_never_padded_into_a_code():
    """`100` is the *Logement* subgroup, and must not become code 0100.

    No CUBF begins with a zero - the manual's categories run 1 to 9 - so
    left-padding could only ever fabricate a code, and would hand a heading's
    name to whatever unit collided with it. `0100` itself is left alone rather
    than specially rejected: it is four characters, so it is code-shaped, and
    the manual having no such code is what makes it describe nothing.
    """
    assert use_code_key("100") is None
    assert use_code_key("10") is None
    assert use_code_key("1") is None
    assert "0100" not in use_code_descriptions(read_liste(workbook_bytes()))


def test_a_code_survives_a_parquet_round_trip_that_made_it_a_float():
    """`str(4611.0)` matches no code and would look like a use nobody numbers."""
    assert use_code_key(4611.0) == "4611"
    assert use_code_key("4611.0") == "4611"
    assert use_code_key(4611) == "4611"
    assert use_code_key(" 4611 ") == "4611"


def test_what_is_not_a_code_answers_none():
    assert use_code_key(None) is None
    assert use_code_key(float("nan")) is None
    assert use_code_key("") is None
    assert use_code_key("nan") is None
    assert use_code_key("2-3") is None
    assert use_code_key("46110") is None


def test_describe_maps_a_column_and_leaves_the_rest_null():
    codes = use_code_descriptions(read_liste(workbook_bytes()))

    text = describe(pd.Series(["4611", 1000.0, "9800", None, "1234"]), codes)

    assert text.iloc[0] == GARAGE
    assert text.iloc[1] == "Logement"
    # Numbered without text, never stated, and not in the manual at all.
    assert text.iloc[2:].isna().all()


# -- the fetcher and the asset ----------------------------------------------


class FakeResponse:
    def __init__(
        self, content: bytes, *, content_type: str = "application/vnd.ms-excel"
    ):
        self.content = content
        self.headers = {"Content-Type": content_type}

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, content: bytes):
        self.content = content
        self.calls: list[str] = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        return FakeResponse(self.content)


def fetcher_for(content: bytes) -> tuple[CubfFetcher, FakeSession]:
    session = FakeSession(content)
    return (
        CubfFetcher(
            url="https://example/CUBF_MEFQ.xlsx",
            request_delay_seconds=0,
            session=session,
        ),
        session,
    )


def test_a_response_that_is_not_an_xlsx_is_refused():
    """A moved file answers 200 with an error page, not a zip."""
    fetcher, _ = fetcher_for(b"<html>Page introuvable</html>")

    with pytest.raises(CubfError, match="not an xlsx"):
        fetcher.fetch()


@pytest.fixture
def store(tmp_path):
    return ParquetStore(root_dir=str(tmp_path / "store"))


@pytest.fixture
def cubf(monkeypatch):
    """The workbook, stubbed on the resource class - Dagster rebuilds it."""
    fetcher, session = fetcher_for(workbook_bytes())
    monkeypatch.setattr(CubfResource, "fetcher", lambda self: fetcher)
    return session


def run_codebook(store):
    return materialize(
        [cubf_use_codes],
        partition_key=DATE,
        resources={"cubf": CubfResource(), "store": store},
    )


def test_the_asset_writes_the_sheet_with_its_provenance(store, cubf):
    result = run_codebook(store)

    assert result.success
    frame = pd.read_parquet(
        Path(store.partition_dir(cubf_use_codes.key.path[-1], DATE)) / CUBF_FILE
    )
    assert set(frame["scrape_date"]) == {DATE}
    assert set(frame["mefq_edition"]) == {"2025"}
    assert set(frame["source_sheet"]) == {LISTE_SHEET}
    assert use_code_descriptions(frame)["4611"] == GARAGE


def test_the_asset_counts_the_codes_rather_than_the_rows(store, cubf):
    result = run_codebook(store)

    metadata = materialization_metadata(result, cubf_use_codes)
    # Three four-character rows: 1000, 1010, 4611 and 9800 - one of which the
    # manual leaves undescribed.
    assert metadata["num_use_codes"].value == 3
    assert metadata["num_codes_without_a_description"].value == 1
    assert metadata["mefq_edition"].value == "2025"
    assert metadata["sheets_not_read"].value == ["MAJ2025"]
    # The hierarchy rows are in the file and are not codes.
    assert metadata["num_rows"].value > metadata["num_use_codes"].value


def test_the_asset_fails_when_the_download_is_not_a_workbook(store, monkeypatch):
    fetcher, _ = fetcher_for(b"<html>Page introuvable</html>")
    monkeypatch.setattr(CubfResource, "fetcher", lambda self: fetcher)

    with pytest.raises(Failure, match="not an xlsx"):
        run_codebook(store)
