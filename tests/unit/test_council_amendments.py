"""The fold from seats to decisions, and the sheet's in-force line.

The seats are the worked case's documents read by `items.py` exactly as the
asset reads them - the Montcalm minute of 16 June 2025, the sommaire
GT2025-233, the extract CA1-2025-0215, the consultation report of fiche 917 -
with the provenance columns the asset adds. Nine rows in
silver.council_planning_items; one amendment here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hbu_dataplatform.cities.quebec_city.council import assets as council_assets
from hbu_dataplatform.cities.quebec_city.council.amendments import (
    KNOWN_BASE_BYLAWS,
    base_bylaws,
    bylaw_status,
    consolidate,
    link,
)
from hbu_dataplatform.cities.quebec_city.council.items import read_document, read_minutes

FIXTURES = Path(__file__).parent.parent / "fixtures" / "council"


def _seat(item, *, doc_id, source_kind, meeting_date, minutes_doc_ids=(), document_number=None, url=None):
    row = council_assets._item_row(item)
    row.update(
        {
            "doc_id": doc_id,
            "source_kind": source_kind,
            "council_id": 12,
            "council_name": "Montcalm",
            "meeting_date": meeting_date,
            "document_number": document_number,
            "url": url or f"https://example.test/{doc_id}.pdf",
            "minutes_doc_ids": json.dumps(list(minutes_doc_ids)),
            "citations": json.dumps({"chunk_ids": [f"{doc_id}:0001"]}),
            "neighborhood": "CIL",
            "scrape_date": "2026-10-01",
        }
    )
    return row


@pytest.fixture(scope="module")
def seats() -> list[dict]:
    minute = (FIXTURES / "minutes_montcalm_2025-06-16.txt").read_text(encoding="utf-8")
    items, _ = read_minutes(minute)
    rows = [_seat(item, doc_id="minute1", source_kind="minutes", meeting_date="2025-06-16") for item in items]
    for name, kind, number in (
        ("gpd_GT2025-233.txt", "gpd", "GT2025-233"),
        ("gpd_CA1-2025-0215.txt", "gpd", "CA1-2025-0215"),
        ("report_917.txt", "consultation_file", None),
    ):
        item = read_document((FIXTURES / name).read_text(encoding="utf-8"))
        rows.append(
            _seat(
                item, doc_id=name.split(".")[0], source_kind=kind, meeting_date="2025-06-16",
                minutes_doc_ids=["minute1"], document_number=number,
            )
        )
    return rows


# -- the fold -----------------------------------------------------------------


def test_the_worked_case_folds_to_one_decision(seats):
    rows = consolidate(seats, neighborhood="CIL", scrape_date="2026-10-01", current_caps={"14040Hb": 10.0})
    about = [r for r in rows if "14040Hb" in json.loads(r["subject_zone_codes"])]
    assert len(about) == 1
    (row,) = about
    assert row["amendment_id"] == "R.C.A.1V.Q. 549"
    assert row["kind"] == "zoning_amendment"
    assert json.loads(row["subject_zone_codes"]) == ["14040Hb"]
    # The by-law being made, and the ones being amended, kept apart.
    assert json.loads(row["bylaw_numbers"]) == ["R.C.A.1V.Q. 549"]
    assert "R.C.A.1V.Q. 4" in json.loads(row["base_bylaw_numbers"])
    assert set(json.loads(row["gpd_numbers"])) >= {"GT2025-233", "CA1-2025-0215"}
    # 8 to 10: the cap, not the project's count, and it is a raise.
    assert (row["max_dwellings_before"], row["max_dwellings_after"]) == (8, 10)
    assert row["project_dwellings"] == 10
    assert row["raises_cap"] is True
    assert row["current_max_dwellings"] == 10.0
    # The furthest stage is the extract's adoption, dated by its sitting.
    assert row["stage"] == "adopted"
    assert row["outcome"] == "approved"
    assert row["decision_date"] == "2025-07-07"
    assert row["first_seen"] == "2025-06-16"
    # The report's opinion and vote table over the minute's paraphrase.
    assert row["council_opinion"] == "favorable"
    assert json.loads(row["votes"]).get("C") == 9
    # Every seat is cited, with its chunks.
    assert row["seats"] >= 4
    citations = json.loads(row["citations"])
    assert {c["source_kind"] for c in citations} >= {"minutes", "gpd", "consultation_file"}
    assert all(c["chunk_ids"] for c in citations)
    assert row["title"]


def test_discussion_without_a_zone_is_not_a_decision(seats):
    """The minute's other agenda items say "zonage" or "logement" without
    naming a zone; they stay in the items table and are not folded."""
    rows = consolidate(seats, neighborhood="CIL", scrape_date="2026-10-01")
    for row in rows:
        assert json.loads(row["subject_zone_codes"]) or json.loads(row["subject_addresses"]) or json.loads(row["lot_numbers"])


def test_two_bylaws_at_one_assembly_stay_apart():
    """Two sommaires discussed at the same meeting about two zones are two
    decisions, even though both cite the arrondissement's base by-law."""
    def seat(doc_id, kind, zones, bylaws, gpd, decision=None, index=0, minute="m"):
        return {
            "doc_id": doc_id, "item_index": index, "source_kind": kind, "meeting_date": "2025-03-25",
            "minutes_doc_ids": json.dumps([minute]) if kind != "minutes" else "[]",
            "item_kind": "zoning_amendment", "subject_zone_codes": json.dumps(zones),
            "bylaw_numbers": json.dumps(bylaws), "gpd_numbers": json.dumps(gpd), "decision": decision,
            "url": "u", "title": doc_id,
        }
    seats = [
        seat("GT1", "gpd", ["14054Mb"], ["R.C.A.1V.Q. 538", "R.C.A.1V.Q. 4"], ["GT2025-074"], "notice_of_motion"),
        seat("GT2", "gpd", ["14060Hb"], ["R.C.A.1V.Q. 539", "R.C.A.1V.Q. 4"], ["GT2025-076"], "adopted"),
        seat("m", "minutes", ["14054Mb"], [], [], "consultation", index=4),
        seat("m", "minutes", [], [], [], None, index=9),
    ]
    groups = link(seats)
    assert len(groups) == 2
    rows = consolidate(seats, neighborhood="CIL", scrape_date="2026-10-01")
    by_id = {row["amendment_id"]: row for row in rows}
    assert set(by_id) == {"R.C.A.1V.Q. 538", "R.C.A.1V.Q. 539"}
    # The minute's item joined the first through its zone at that assembly.
    assert by_id["R.C.A.1V.Q. 538"]["seats"] == 2
    assert by_id["R.C.A.1V.Q. 538"]["outcome"] == "in_progress"
    assert by_id["R.C.A.1V.Q. 539"]["outcome"] == "approved"


def test_a_bylaw_cited_at_three_assemblies_is_a_base_bylaw():
    seats = [
        {"doc_id": f"d{i}", "item_index": 0, "meeting_date": f"2025-0{i}-01", "bylaw_numbers": json.dumps(["R.C.A.1V.Q. 777"])}
        for i in (1, 2, 3)
    ]
    assert "R.C.A.1V.Q. 777" in base_bylaws(seats)
    assert "R.C.A.1V.Q. 4" in KNOWN_BASE_BYLAWS
    assert "R.C.A.1V.Q. 777" not in base_bylaws(seats[:2])


def test_a_refusal_is_terminal_and_a_cap_raise_is_read_against_the_grid():
    seats = [
        {
            "doc_id": "r", "item_index": 0, "source_kind": "gpd", "meeting_date": "2025-05-05",
            "minutes_doc_ids": "[]", "item_kind": "zoning_amendment",
            "subject_zone_codes": json.dumps(["11016Hb"]), "bylaw_numbers": json.dumps(["R.C.A.1V.Q. 600"]),
            "gpd_numbers": "[]", "decision": "refused", "decision_date": "2025-05-20",
            "max_dwellings_before": None, "max_dwellings_after": 6, "url": "u",
        },
        {
            "doc_id": "r", "item_index": 1, "source_kind": "gpd", "meeting_date": "2025-05-05",
            "minutes_doc_ids": "[]", "item_kind": "zoning_amendment",
            "subject_zone_codes": json.dumps(["11016Hb"]), "bylaw_numbers": json.dumps(["R.C.A.1V.Q. 600"]),
            "gpd_numbers": "[]", "decision": "adopted", "decision_date": "2025-04-01", "url": "u",
        },
    ]
    (row,) = consolidate(seats, neighborhood="CIL", scrape_date="2026-10-01", current_caps={"11016Hb": 3.0})
    assert row["stage"] == "refused"
    assert row["outcome"] == "refused"
    assert row["decision_date"] == "2025-05-20"
    # No before read: 6 against the grid's 3 is a raise.
    assert row["raises_cap"] is True and row["current_max_dwellings"] == 3.0
    (row,) = consolidate(seats, neighborhood="CIL", scrape_date="2026-10-01", current_caps={"11016Hb": 8.0})
    assert row["raises_cap"] is False


# -- the sheet ----------------------------------------------------------------


def test_the_sheet_in_force_line_is_read_per_zone():
    chunks = [
        {
            "doc_id": "s1", "url": "https://carte.ville.quebec.qc.ca/GrillesZonage/HandlerZonage.ashx?14089Mb",
            "source_table": "Zonage__ZONAGE_EN_VIGUEUR", "feature_ids": json.dumps(["14089Mb"]),
            "text": "Locali ation Projet d'en emble\nMaximum\n14089MbR.C.A.1V.Q. 515En vigueur le 2024-12-13\n\nAUTRES",
        },
        # The same line twice on one sheet is one row; a sheet with none is none.
        {"doc_id": "s1", "url": "u", "source_table": "Zonage__ZONAGE_EN_VIGUEUR", "feature_ids": ["14089Mb"],
         "text": "14089Mb R.C.A.1V.Q. 515 En vigueur le 2024-12-13"},
        {"doc_id": "s2", "url": "u", "source_table": "Zonage__ZONAGE_EN_VIGUEUR", "feature_ids": ["14090Hb"],
         "text": "nothing about a by-law"},
        {"doc_id": "s3", "url": "u", "source_table": "Zonage__ZONAGE_EN_VIGUEUR", "feature_ids": ["14040Hb"],
         "text": "14040HbR.C.A.1V.Q. 549En vigueur le 2025-07-10"},
    ]
    rows = bylaw_status(chunks, neighborhood="CIL", scrape_date="2026-09-01")
    assert [(r["feature_id"], r["bylaw_number"], r["in_force_date"]) for r in rows] == [
        ("14089Mb", "R.C.A.1V.Q. 515", "2024-12-13"),
        ("14040Hb", "R.C.A.1V.Q. 549", "2025-07-10"),
    ]
    assert rows[0]["excerpt"] == "R.C.A.1V.Q. 515En vigueur le 2024-12-13"
    assert rows[0]["neighborhood"] == "CIL" and rows[0]["scrape_date"] == "2026-09-01"
