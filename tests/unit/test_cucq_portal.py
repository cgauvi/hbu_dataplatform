"""The decisions portal's index: how a CUCQ minute is listed, filtered and
named - offline, against the shapes the index answers with."""

from __future__ import annotations

from datetime import date

import pytest

from hbu_dataplatform.cities.quebec_city.cucq.portal import (
    INSTANCE_CUCQ,
    SELECT_FIELDS,
    TYPE_MINUTES,
    CucqError,
    CucqPortalClient,
    date_of,
    decode_field,
    minutes_filter,
    odata_literal,
    parse_records,
    series_of,
)

ENCODED_INSTANCE = "Commission%20d%27urbanisme%20et%20de%20conservation%20de%20Qu%C3%A9bec"


def test_a_facet_is_spelled_the_way_the_index_stores_it():
    # The portal filters with encodeURIComponent and %27 for the apostrophe,
    # so the stored value is percent-encoded and the filter has to be too.
    assert odata_literal(INSTANCE_CUCQ) == f"'{ENCODED_INSTANCE}'"
    assert odata_literal(TYPE_MINUTES) == "'Proc%C3%A8s-verbaux'"
    assert decode_field(ENCODED_INSTANCE) == INSTANCE_CUCQ
    assert decode_field("null") is None and decode_field(None) is None


def test_the_filter_is_the_commissions_minutes_and_optionally_a_year_on():
    assert minutes_filter() == (
        f"Instance eq '{ENCODED_INSTANCE}' and Type eq 'Proc%C3%A8s-verbaux'"
    )
    assert minutes_filter(since_year=2024).endswith(" and Annee ge '2024'")


@pytest.mark.parametrize(
    "name, series, when",
    [
        ("PV_CUCQ_OR_2026-06-23.pdf", "OR", date(2026, 6, 23)),
        ("PV_CUCQ_DEM_2025-10-23.pdf", "DEM", date(2025, 10, 23)),
        ("pv_cucq_or_2000-01-11.pdf", "OR", date(2000, 1, 11)),
        ("GT2025-233.pdf", None, None),
    ],
)
def test_the_series_and_the_date_are_read_off_the_file_name(name, series, when):
    assert series_of(name) == series
    assert date_of(name) == when


def test_a_listing_is_decoded_and_pointed_at_the_blob():
    payload = {
        "value": [
            {
                "@search.score": 1.0,
                "metadata_storage_name": "PV_CUCQ_OR_2026-06-23.pdf",
                "Numero": "PV_CUCQ_OR_2026-06-23.pdf",
                "Date": "2026-06-23",
                "Annee": "2026",
                "Objet": "Proc%C3%A8s-verbal%20de%20la%20s%C3%A9ance%20de%20la%20CUCQ%20tenue%20le%2023%20juin%202026",
                "Instance": ENCODED_INSTANCE,
                "Type": "Proc%C3%A8s-verbaux",
                "Uniteadministrative": "Planification%20de%20l%27am%C3%A9nagement%20et%20de%20l%27environnement",
                "content": "\nprocès-verbal \n",
            },
            {"metadata_storage_name": "PV_CUCQ_DEM_2025-10-23.pdf", "Date": None, "Annee": None, "content": None},
            {"metadata_storage_name": ""},
        ]
    }
    records = parse_records(payload)
    assert [r.storage_name for r in records] == ["PV_CUCQ_OR_2026-06-23.pdf", "PV_CUCQ_DEM_2025-10-23.pdf"]
    first, second = records
    assert first.url == "https://gpddocs.ville.quebec.qc.ca/gpdblob/PV_CUCQ_OR_2026-06-23.pdf"
    assert first.series == "OR" and first.meeting_date == date(2026, 6, 23) and first.year == 2026
    assert first.objet == "Procès-verbal de la séance de la CUCQ tenue le 23 juin 2026"
    assert first.instance == INSTANCE_CUCQ
    assert first.administrative_unit == "Planification de l'aménagement et de l'environnement"
    assert first.index_text == "procès-verbal"
    # No Date on the record: the file name's date stands in.
    assert second.series == "DEM" and second.meeting_date == date(2025, 10, 23) and second.year is None
    assert second.index_text == ""


class FakeIndex:
    """Answers the search endpoint page by page and remembers what was asked."""

    def __init__(self, pages):
        self.pages = pages
        self.calls: list[dict] = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        skip = int(params["$skip"])
        top = int(params["$top"])
        page = self.pages[skip // top] if skip // top < len(self.pages) else []
        return _JsonResponse({"@odata.count": sum(len(p) for p in self.pages), "value": page})


class _JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _doc(name, when):
    return {"metadata_storage_name": name, "Numero": name, "Date": when, "Annee": when[:4], "content": ""}


def test_the_listing_pages_through_the_index_with_skip():
    index = FakeIndex(
        [
            [_doc("PV_CUCQ_OR_2026-08-05.pdf", "2026-08-05"), _doc("PV_CUCQ_OR_2026-07-29.pdf", "2026-07-29")],
            [_doc("PV_CUCQ_DEM_2025-12-18.pdf", "2025-12-18")],
        ]
    )
    client = CucqPortalClient(session=index, page_size=2, request_delay_seconds=0, query_key="k")
    records = client.list_minutes(since_year=2025)
    assert [r.storage_name for r in records] == [
        "PV_CUCQ_OR_2026-08-05.pdf", "PV_CUCQ_OR_2026-07-29.pdf", "PV_CUCQ_DEM_2025-12-18.pdf",
    ]
    # Two pages: the second came back short, so there was no third request.
    assert [c["params"]["$skip"] for c in index.calls] == ["0", "2"]
    first = index.calls[0]
    assert first["headers"]["api-key"] == "k"
    assert first["params"]["api-version"] == "2020-06-30"
    assert first["params"]["$filter"] == minutes_filter(since_year=2025)
    assert first["params"]["$select"] == ",".join(SELECT_FIELDS)
    assert first["params"]["$orderby"] == "Date desc"


def test_an_empty_listing_is_an_error_naming_the_filter():
    client = CucqPortalClient(session=FakeIndex([[]]), request_delay_seconds=0, query_key="k")
    with pytest.raises(CucqError, match="lists no CUCQ minutes"):
        client.list_minutes()


def test_the_index_s_own_error_is_surfaced():
    class Erroring:
        def get(self, url, params=None, headers=None, timeout=None):
            return _JsonResponse({"error": {"code": "", "message": "Invalid expression"}})

    client = CucqPortalClient(session=Erroring(), request_delay_seconds=0, query_key="k")
    with pytest.raises(CucqError, match="Invalid expression"):
        client.list_minutes()


def test_without_a_key_the_client_says_where_to_get_one(monkeypatch):
    monkeypatch.delenv("CUCQ_SEARCH_QUERY_KEY", raising=False)
    client = CucqPortalClient(session=FakeIndex([[]]), request_delay_seconds=0)
    with pytest.raises(CucqError, match="CUCQ_SEARCH_QUERY_KEY"):
        client.list_minutes()


def test_the_key_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("CUCQ_SEARCH_QUERY_KEY", "from-env")
    assert CucqPortalClient(session=FakeIndex([[]]), request_delay_seconds=0).query_key == "from-env"


def test_a_minute_s_url_quotes_its_name():
    client = CucqPortalClient(session=FakeIndex([[]]), request_delay_seconds=0, query_key="k")
    assert client.minute_url("PV CUCQ.pdf") == "https://gpddocs.ville.quebec.qc.ca/gpdblob/PV%20CUCQ.pdf"
