"""The reading of a council document into a planning item.

The fixtures are the worked case's four documents - the Montcalm minute of
16 June 2025, the sommaire GT2025-233, the extract CA1-2025-0215 and the
consultation report of fiche 917 - flattened exactly as bronze flattens them
(`read_pdf(keep_hyphens=True)`). Every expectation here is a number or a
phrase a reader can find on the published page.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from hbu_dataplatform.council_items import (
    PLANNING_KINDS,
    addresses,
    agenda_items,
    bylaw_numbers,
    classify_item,
    dwelling_changes,
    read_document,
    read_minutes,
    repair_text,
    subject_zone_codes,
    zone_codes,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "council"


@pytest.fixture(scope="module")
def minute():
    return (FIXTURES / "minutes_montcalm_2025-06-16.txt").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def sommaire():
    return (FIXTURES / "gpd_GT2025-233.txt").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def extract():
    return (FIXTURES / "gpd_CA1-2025-0215.txt").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def report():
    return (FIXTURES / "report_917.txt").read_text(encoding="utf-8")


# -- the agenda ---------------------------------------------------------------


def test_the_body_is_cut_into_its_items_not_the_ordre_du_jour(minute):
    items = agenda_items(repair_text(minute))
    # Thirteen on the agenda; "11 Correspondance" prints with no separator
    # in both the ordre du jour and the body, so twelve are found and 12
    # follows 10 as the one skip the chain allows.
    assert [item.number for item in items] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13]
    assert items[3].title.startswith("Consultation publique et demande d")
    # The body's item, not the one-line entry of the ordre du jour.
    assert "Le propriétaire du bâtiment" in items[3].text
    assert "5. Suivi des résolutions" not in items[3].text
    assert items[-1].title.startswith("Levée")
    # Item 8 opens with a bullet in this minute ("• 8. Période de questions").
    assert items[7].title.startswith("Période de questions")


def test_a_nested_list_does_not_win_over_the_body():
    text = "\n".join(
        [
            "Ordre du jour",
            "1. Ouverture",
            "2. Questions",
            "3. Levée",
            "Procès-verbal",
            "1. Ouverture de l’assemblée à 19h.",
            "2. Questions des citoyens.",
            "1- Première question sur le zonage.",
            "2- Deuxième question.",
            "3- Troisième question.",
            "4- Quatrième question.",
            "3. Levée de l’assemblée.",
            "Fin.",
        ]
    )
    items = agenda_items(text)
    assert [item.title for item in items] == [
        "Ouverture de l’assemblée à 19h",
        "Questions des citoyens",
        "Levée de l’assemblée",
    ]
    assert "Quatrième question" in items[1].text


def test_the_ordre_du_jour_last_item_stops_where_the_body_begins():
    """When the body's chain is too broken to be chosen, the ordre du jour's
    last item must not swallow the whole minute."""
    text = "\n".join(
        [
            "1. Ouverture",
            "2. Consultation publique",
            "3. Trésorerie",
            "4. Levée",
            "Procès-verbal",
            "1. Ouverture de l’assemblée",
            "Beaucoup de texte sur le zonage de la zone 14040Hb.",
            "Sans autre en-tête numéroté.",
        ]
    )
    items = agenda_items(text)
    assert items[-1].title == "Levée"
    assert "14040Hb" not in items[-1].text


def test_a_minute_with_no_agenda_reads_as_one_item():
    items, dropped = read_minutes("Résolution sur la modification réglementaire de la zone 14040Hb.")
    assert dropped == 0
    assert len(items) == 1 and items[0].zone_codes == ["14040Hb"]
    assert read_minutes("Le budget est adopté.") == ([], 0)


# -- the minute ---------------------------------------------------------------


def test_the_minute_keeps_its_planning_items_and_drops_the_rest(minute):
    items, dropped = read_minutes(minute)
    assert dropped == 8
    kinds = {item.item_index: item.item_kind for item in items}
    assert kinds[4] == "zoning_amendment"
    assert set(kinds) <= set(range(1, 14))
    assert all(item.item_kind in PLANNING_KINDS for item in items)


def test_the_consultation_item_is_the_worked_case(minute):
    items, _ = read_minutes(minute)
    item = next(item for item in items if item.item_index == 4)
    assert item.title == "Consultation publique et demande d’opinion"
    assert item.subject_zone_codes == ["14040Hb"]
    assert item.zone_codes == ["14040Hb"]
    assert item.subject_addresses[0] == "355, boulevard René-Lévesque Ouest"
    assert item.max_dwellings_before == 8 and item.max_dwellings_after == 10
    assert item.project_dwellings == 10
    assert item.max_height_m == 13.0
    assert 3 in item.storeys
    assert item.decision == "consultation"
    assert item.council_opinion == "favorable_with_conditions"
    assert "est d’accord avec l’augmentation du nombre de logements à 10" in item.council_opinion_excerpt
    assert item.bylaw_numbers == [] and item.gpd_numbers == []
    # The minute's own misspelling, "140040 Hb", is not read as a zone.
    assert "140040" not in "".join(item.zone_codes)


# -- the trail documents ------------------------------------------------------


def test_the_sommaire(sommaire):
    item = read_document(sommaire)
    assert item.item_kind == "zoning_amendment"
    assert item.title.startswith("Adoption du Règlement modifiant le Règlement de l’Arrondissement")
    assert item.subject_zone_codes == ["14040Hb"]
    # The annexed plan extract labels the neighbours; they are kept apart.
    assert len(item.zone_codes) > 20 and item.zone_codes[0] == "14040Hb"
    assert item.bylaw_numbers[:2] == ["R.C.A.1V.Q. 549", "R.C.A.1V.Q. 4"]
    assert set(item.gpd_numbers) == {"GT2025-233", "CA1-2025-0215", "CA1-2025-0145", "AM1-2025-0146"}
    assert item.file_number == "5809"
    assert item.subject_addresses == ["355, boulevard René-Lévesque Ouest"]
    assert item.usage_groups == ["H1"]
    assert item.max_dwellings_before == 8 and item.max_dwellings_after == 10
    zoning = [c for c in item.dwelling_changes if c.scope == "zoning"]
    assert all((c.before, c.after) == (8, 10) for c in zoning)
    # "augmenter de huit à dix" - number words count.
    assert any("huit à dix" in c.excerpt for c in zoning)
    project = [c for c in item.dwelling_changes if c.scope == "project"]
    assert (2, 10) in {(c.before, c.after) for c in project}
    assert item.decision == "notice_of_motion"
    assert item.decision_date == date(2025, 4, 30)
    assert item.council_opinion is None


def test_the_resolution_extract(extract):
    item = read_document(extract)
    assert item.decision == "adopted"
    assert item.decision_date == date(2025, 7, 7)
    assert item.subject_zone_codes == ["14040Hb"]
    assert item.bylaw_numbers == ["R.C.A.1V.Q. 549"]
    assert "GT2025-233" in item.gpd_numbers and "CA1-2025-0215" in item.gpd_numbers
    assert item.subject_addresses == ["355, boulevard René-Lévesque Ouest"]
    # The council chamber is an address in the text, not the subject.
    assert "500, rue du Pont" in item.addresses
    assert "500, rue du Pont" not in item.subject_addresses
    assert item.max_dwellings_after is None


def test_the_consultation_report(report):
    item = read_document(report)
    assert item.item_kind == "zoning_amendment"
    assert item.decision == "consultation"
    assert item.decision_date == date(2025, 6, 16)
    assert item.council_opinion == "favorable"
    assert item.votes == {"A": 0, "B": 0, "C": 9, "Abstention": 0}
    assert item.max_dwellings_before == 8 and item.max_dwellings_after == 10
    assert item.project_dwellings == 10
    assert item.max_height_m == 13.0
    assert item.bylaw_numbers == ["R.C.A.1V.Q. 549"]
    assert item.subject_addresses == ["355, boulevard René-Lévesque Ouest"]


# -- the patterns, one by one -------------------------------------------------


def test_zone_codes_need_five_digits_and_a_suffix():
    assert zone_codes("zones 14040Hb, 14041Ma et 14040 Hb; pas 140040 Hb ni 1404Hb") == ["14040Hb", "14041Ma"]


def test_subject_zones_come_from_the_title_and_the_naming_phrases():
    text = "Ce règlement s’applique relativement à la zone 14040Hb et à la zone 14041Ma. Le plan montre 14029Hb."
    assert subject_zone_codes(text) == ["14040Hb", "14041Ma"]
    assert subject_zone_codes("rien", title="Zone 12345Xy") == ["12345Xy"]


def test_bylaws_are_respelled_one_way():
    assert bylaw_numbers("R.C.A.1V .Q. 549 et R.C.A.1V.Q. 4, RVQ 978 et R. V. Q. 990") == [
        "R.C.A.1V.Q. 549",
        "R.C.A.1V.Q. 4",
        "R.V.Q. 978",
        "R.V.Q. 990",
    ]


def test_addresses_stop_at_the_line_and_keep_the_cardinal():
    text = "au 355, boulevard René-Lévesque Ouest\nProjet de Règlement ... 265 boulevard René-Lévesque ouest, salle 106; 500,\nrue du Pont."
    assert addresses(text) == [
        "355, boulevard René-Lévesque Ouest",
        "265, boulevard René-Lévesque ouest",
        "500, rue du Pont",
    ]


def test_dwelling_changes_are_scoped():
    text = (
        "Le règlement porte le nombre maximal de logements autorisés de 8 logements à 10 logements. "
        "Le projet fait passer le bâtiment de 2 à 10 logements."
    )
    changes = dwelling_changes(text)
    assert [(c.before, c.after, c.scope) for c in changes] == [(8, 10, "zoning"), (2, 10, "project")]
    # A range about something else is not a dwelling change.
    assert dwelling_changes("de 8 à 10 heures le matin") == []


@pytest.mark.parametrize(
    "text, kind",
    [
        ("Règlement modifiant le Règlement de l’Arrondissement sur l’urbanisme", "zoning_amendment"),
        ("La grille de spécifications de la zone", "zoning_amendment"),
        ("Un PPCMOI est déposé", "ppcmoi"),
        ("Demande de dérogation mineure", "minor_variance"),
        ("Le bâtiment sera démoli", "demolition"),
        ("Le plan d’urbanisme révisé", "planning"),
        ("Bâtiment à valeur patrimoniale", "heritage"),
        ("Six logements abordables", "housing"),
        ("Le solde au compte est de 3 228 $", None),
    ],
)
def test_item_kinds(text, kind):
    assert classify_item(text) == kind
