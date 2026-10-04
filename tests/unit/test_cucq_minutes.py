"""Reading a CUCQ minute into decisions, against eight real minutes
flattened exactly as bronze flattens them (`read_pdf(keep_hyphens=True)`):
the two layouts of the annexed list, a list-only minute, and three
sittings of the comité de démolition."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from hbu_dataplatform.cities.quebec_city.cucq.minutes import (
    DECISION_APPROVED,
    DECISION_CONDITIONAL,
    DECISION_REFUSED,
    civic_of,
    classify_works,
    decision_text,
    decision_title,
    dwellings,
    names_an_entry,
    parse_address_line,
    read_sitting,
    section_decision,
    source_table_for,
)
from hbu_dataplatform.cities.quebec_city.cucq.portal import date_of, series_of

FIXTURES = Path(__file__).parent.parent / "fixtures" / "cucq"


def read(name: str, *, listing_date: bool = True):
    text = (FIXTURES / name).read_text(encoding="utf-8")
    pdf = name[:-4].upper() + ".pdf"
    return read_sitting(text, series=series_of(pdf), meeting_date=date_of(pdf) if listing_date else None)


def by_number(sitting):
    return {d.request_number: d for d in sitting.decisions}


# -- the pieces ---------------------------------------------------------------


@pytest.mark.parametrize(
    "line, tokens, civic, street, block",
    [
        ("467, 469, 471, Rue Arago Ouest", ["467", "469", "471"], 467, "Rue Arago Ouest", False),
        ("116-A, 116, Rue Bertrand", ["116-A", "116"], 116, "Rue Bertrand", False),
        ("649-1/2, 649, Rue Boisseau", ["649-1/2", "649"], 649, "Rue Boisseau", False),
        ("1-389, Rue Sainte-Agnès", ["1-389"], 389, "Rue Sainte-Agnès", False),
        ("243 à 305, Rue Gérard-Morisset (Bloc)", ["243", "305"], 243, "Rue Gérard-Morisset", True),
        ("89, 91 et 93 rue Racine", ["89", "91", "93"], 89, "rue Racine", False),
        ("318, 14e Rue", ["318"], 318, "14e Rue", False),
        ("2909, 1re Avenue", ["2909"], 2909, "1re Avenue", False),
        ("184, Grande Allée Ouest", ["184"], 184, "Grande Allée Ouest", False),
        ("8195, Le Trait-Carré Ouest", ["8195"], 8195, "Le Trait-Carré Ouest", False),
        ("Rue des Dominicaines", [], None, "Rue des Dominicaines", False),
        ("122-", ["122"], 122, None, False),
    ],
)
def test_an_address_line_is_taken_apart(line, tokens, civic, street, block):
    address = parse_address_line(line)
    assert address is not None, line
    assert address.civic_numbers == tokens
    assert address.civic == civic
    assert address.street == street
    assert address.is_block is block


def test_the_addresses_of_a_line_are_one_per_door():
    address = parse_address_line("1050, 1052, 1054, Avenue Royale")
    assert address.addresses == ["1050, Avenue Royale", "1052, Avenue Royale", "1054, Avenue Royale"]
    assert address.key == "1050, Avenue Royale"


@pytest.mark.parametrize(
    "line",
    [
        "Construction d'un nouveau bâtiment d'habitation de 1 à 3 logements",
        "terrasse, escalier ou toute autre construction similaire située à plus de 2 m par",
        "2026-05-28 : Travaux d'aménagement paysager.",
        "Labrosse, Marc",
        "Page 1 de 6",
    ],
)
def test_what_is_not_an_entry_s_address(line):
    assert not names_an_entry(line)


def test_a_bare_street_names_an_entry_only_when_short_and_capitalised():
    assert names_an_entry("Rue des Dominicaines")
    assert names_an_entry("Côte du Palais")
    assert not names_an_entry("rue des remparts et autres travaux de réfection de la maçonnerie en façade")


def test_civic_numbers():
    assert civic_of("116-A") == 116
    assert civic_of("229-1/2") == 229
    assert civic_of("1-389") == 389
    assert civic_of("2211-A") == 2211
    assert civic_of("x") is None


@pytest.mark.parametrize(
    "works, kind",
    [
        ("Démolition d'un bâtiment principal\nDémolition du bâtiment principal pour reconstruction", "demolition_main"),
        ("Démolition d'une partie d'un bâtiment principal", "demolition_main"),
        ("Démolition d'un bâtiment accessoire de 18 m2 et moins", "demolition_accessory"),
        ("Démantèlement de la cheminée sans reconstruction.", "demolition_other"),
        ("Démolition d'une enseigne", "demolition_other"),
        # Demolish and rebuild is a demolition first.
        ("Construction d'un nouveau bâtiment d'habitation de 1 à 3 logements\nDémolition du bâtiment principal", "demolition_main"),
        ("Construction d'un nouveau bâtiment d'habitation multifamilial de 4 à 8\nlogements", "new_construction"),
        ("Agrandissement d'un bâtiment résidentiel de 1 à 3 logements", "enlargement"),
        ("Création, modification ou correction d'un ou plusieurs lots", "subdivision"),
        ("Installation ou modification d'une enseigne d'identification sur bâtiment", "sign"),
        ("Travaux ou constructions nécessitant une autorisation spéciale", "special_authorization"),
        ("Changement ou rénovation de fenêtres sans modification des dimensions", "exterior_renovation"),
        ("Installation d'une piscine hors terre", "exterior_renovation"),
        ("Travaux intérieurs impliquant un changement qui affecte l'intégrité", "other"),
    ],
)
def test_the_works_are_classified_demolition_first(works, kind):
    assert classify_works(works) == kind


def test_dwellings_read_the_band_and_the_project():
    assert dwellings("Agrandissement d'un bâtiment résidentiel de 1 à 3 logements") == (1, 3, None)
    assert dwellings("Agrandissement d'un bâtiment résidentiel de 9 logements et plus") == (9, None, None)
    assert dwellings("comportant plus de 2 étages et 8 logements et moins") == (None, 8, None)
    assert dwellings(
        "Construction d'un nouveau bâtiment d'habitation de 1 à 3 logements\n"
        "Construction d'un bâtiment de 3 logements isolés de 2 étages sur le lot 6 717"
    ) == (1, 3, 3)
    assert dwellings("Remplacement ou rénovation de portes") == (None, None, None)


@pytest.mark.parametrize(
    "heading, decision",
    [
        ("Demande approuvée", DECISION_APPROVED),
        ("Demande approuvée conditionnellement", DECISION_CONDITIONAL),
        ("Demande approuvée conditionnellement  de 6Page 2", DECISION_CONDITIONAL),
        ("Demande refusée", DECISION_REFUSED),
        ("Demandes refusées", DECISION_REFUSED),
    ],
)
def test_a_list_heading_is_a_decision(heading, decision):
    assert section_decision(heading)[0] == decision


def test_source_tables_share_one_prefix():
    assert source_table_for("OR") == "cucq_minutes"
    assert source_table_for("DEM") == "cucq_demolition_committee"
    assert source_table_for(None) == "cucq_other"


# -- a regular sitting, glued layout -----------------------------------------


def test_a_regular_sitting_reads_its_list_and_its_resolutions():
    sitting = read("pv_cucq_or_2026-06-23.txt")
    assert sitting.sitting_kind == "regular"
    assert sitting.sitting_number == "2026-25"
    # The listing's date, not the body's slip of the typist - which is noted.
    assert sitting.meeting_date == date(2026, 6, 23)
    assert any("2026-06-25" in note for note in sitting.parse_notes)
    assert sitting.resolutions == {
        DECISION_APPROVED: "C.U. 2026-098",
        DECISION_CONDITIONAL: "C.U. 2026-099",
        DECISION_REFUSED: "C.U. 2026-100",
    }
    assert sitting.audition_numbers == ["20260309-002", "20260306-024"]

    decisions = sitting.decisions
    assert len(decisions) == 20
    assert all(d.request_number for d in decisions)
    assert [d.item_index for d in decisions] == list(range(1, 21))
    assert sum(d.decision == DECISION_APPROVED for d in decisions) == 3
    assert sum(d.decision == DECISION_CONDITIONAL for d in decisions) == 15
    assert sum(d.decision == DECISION_REFUSED for d in decisions) == 2

    refused = by_number(sitting)["20250428-007"]
    assert refused.applicant == "Devmico Construction Inc."
    assert refused.address_line == "467, 469, 471, Rue Arago Ouest"
    assert refused.addresses == ["467, Rue Arago Ouest", "469, Rue Arago Ouest", "471, Rue Arago Ouest"]
    assert refused.civic == 467 and refused.street == "Rue Arago Ouest"
    assert refused.works == "Démolition d'un bâtiment principal\nDémolition du bâtiment principal pour reconstruction"
    assert refused.works_kind == "demolition_main" and refused.is_demolition
    assert refused.decision == DECISION_REFUSED and refused.outcome == "refused"
    assert refused.resolution_number == "C.U. 2026-100"
    assert refused.parse_notes == []

    heard = by_number(sitting)["20260309-002"]
    assert heard.was_heard and heard.decision == DECISION_CONDITIONAL
    assert heard.address_line == "307-A, 307-B, 307, Rue Saint-Benoît"
    assert heard.dwellings_min == 4 and heard.dwellings_max == 8 and heard.project_dwellings == 8

    # A request on a street with no civic number is kept, unplaceable.
    bare = by_number(sitting)["20260105-022"]
    assert bare.address_line == "Rue des Dominicaines" and bare.civic is None
    # "9 logements et plus" is the band; the description counts rooms, not dwellings.
    assert bare.dwellings_min == 9 and bare.dwellings_max is None and bare.project_dwellings is None


def test_a_decision_reads_as_one_paragraph_and_is_titled_by_its_address():
    sitting = read("pv_cucq_or_2026-06-23.txt")
    refused = by_number(sitting)["20250428-007"]
    assert refused.text == decision_text(refused, sitting)
    assert refused.text == (
        "Commission d'urbanisme et de conservation de Québec, séance du 23 juin 2026 "
        "(procès-verbal 2026-25). Demande 20250428-007 - 467, 469, 471, Rue Arago Ouest. "
        "Requérant : Devmico Construction Inc. Travaux : Démolition d'un bâtiment principal "
        "Démolition du bâtiment principal pour reconstruction. "
        "Décision : demande refusée (résolution C.U. 2026-100)."
    )
    assert decision_title(refused, sitting) == (
        "CUCQ, séance du 23 juin 2026 - 467, 469, 471, Rue Arago Ouest - "
        "Démolition d'un bâtiment principal - refusée"
    )


# -- the column layout --------------------------------------------------------


def test_the_column_layout_pairs_numbers_to_entries_per_page():
    sitting = read("pv_cucq_or_2026-08-05.txt")
    decisions = sitting.decisions
    assert len(decisions) == 35
    assert all(d.request_number and d.applicant and d.civic for d in decisions)
    assert sitting.parse_notes == []
    numbered = by_number(sitting)
    # The first page: numbers listed first, entries after, paired in order.
    assert numbered["20260503-014"].applicant == "Labrosse, Marc"
    assert numbered["20260503-014"].address_line == "3819, Rue de Toulouse"
    assert numbered["20260730-023"].applicant == "VILLE DE QUÉBEC"
    # The second page's split digit: "20260616-04 7".
    assert "20260616-047" in numbered
    # A works line that opens on a street-type word is not a new entry.
    block = numbered["20260608-064"]
    assert block.applicant == "Multiple" and block.is_block
    assert block.civic_numbers == ["82", "84"] and block.street == "Rue Lockwell"
    assert block.works.startswith("Ajout, agrandissement ou remplacement d'une galerie")
    assert "terrasse, escalier" in block.works
    # A number that fell after the page footer still belongs to its page.
    assert numbered["20260616-056"].applicant == "Briand, Alexandre"
    assert numbered["20260727-032"].applicant == "Theoret, Bernard"
    assert numbered["20260724-002"].applicant == "LESSARD.STÉPHANE"
    # A street with no type word, and an address the source cut off.
    (trait_carre,) = [d for d in decisions if d.applicant == "CLOUTIER, PIERRE"]
    assert trait_carre.civic == 8195 and trait_carre.street == "Le Trait-Carré Ouest"
    truncated = [d for d in decisions if d.street is None]
    assert {d.civic for d in truncated} == {3, 805}
    assert all(d.works.startswith(("Changement", "Ajout")) for d in truncated)


def test_a_wrapped_civic_number_is_taken_off_the_applicant():
    sitting = read("pv_cucq_or_2024-03-20.txt")
    assert len(sitting.decisions) == 51
    assert sitting.meeting_date == date(2024, 3, 20)
    assert sitting.resolutions[DECISION_APPROVED] == "C.U. 2024-042"
    richelieu = [d for d in sitting.decisions if d.street == "Chemin des Quatre-Bourgeois"][0]
    assert richelieu.applicant == "MAISON RICHELIEU HÉBERGEMENTJEUNESSE SAINTE-FOY INC."
    assert richelieu.civic == 2808
    assert richelieu.dwellings_min == 9
    block = by_number(sitting)["20240216-007"]
    assert block.is_block and block.civic == 243 and block.civic_numbers == ["243", "305"]
    partial = by_number(sitting)["20240116-006"]
    assert partial.works_kind == "demolition_main" and partial.decision == DECISION_APPROVED


def test_a_mixed_page_keeps_the_glued_numbers_and_pairs_the_rest():
    sitting = read("pv_cucq_or_2020-03-10.txt")
    assert sitting.sitting_number == "2020-10"
    assert len(sitting.decisions) == 63
    assert all(d.request_number for d in sitting.decisions)
    numbered = by_number(sitting)
    assert numbered["20180411-014"].civic_numbers == ["649-1/2", "649"]
    assert numbered["20180411-014"].dwellings_min == 4 and numbered["20180411-014"].dwellings_max == 8
    # "122-" glued to the works line by the hyphen-keeping flatten.
    cut = numbered["20171027-015"]
    assert cut.address_line == "122-" and cut.civic == 122 and cut.street is None
    assert cut.works.startswith("Ajout, changement ou rénovation de fenêtres")
    # A street with no number at all.
    assert numbered["20200117-011"].address_line == "Côte du Palais" and numbered["20200117-011"].civic is None


def test_a_list_only_minute_dates_itself_from_the_list():
    sitting = read("pv_cucq_or_2017-06-13.txt", listing_date=False)
    assert sitting.sitting_number is None
    assert sitting.meeting_date == date(2017, 6, 13)
    assert sitting.resolutions == {}
    assert len(sitting.decisions) == 72
    assert all(d.request_number and d.applicant and d.civic for d in sitting.decisions)
    numbered = by_number(sitting)
    # The other glued form: the number first, the applicant after it.
    assert numbered["20170606-039"].applicant == "FAUCHER, CHRISTIAN"
    assert numbered["20170606-039"].address_line == "37, Rue Christophe-Colomb Ouest"
    # A works line that *cites* a request is not an entry: it stays in the
    # works of the entry it was printed under, which is not the one it cites.
    assert all("voir demande" not in (d.applicant or "") for d in sitting.decisions)
    citing = [d for d in sitting.decisions if "voir demande 20170501-024" in d.works]
    assert citing and all(d.request_number not in (None, "20170501-024") for d in citing)
    assert numbered["20170511-015"].works_kind == "demolition_other"


# -- the comité de démolition -------------------------------------------------


def test_a_committee_sitting_is_one_decision_per_hearing():
    sitting = read("pv_cucq_dem_2024-03-28.txt")
    assert sitting.sitting_kind == "demolition_committee"
    assert sitting.sitting_number == "2024-03"
    assert sitting.meeting_date == date(2024, 3, 28)
    assert sitting.resolutions == {} and sitting.audition_numbers == []
    decisions = sitting.decisions
    assert [d.request_number for d in decisions] == [
        "20230607-010", "20240222-028", "20231019-043", "20240131-034", "20240131-031",
    ]
    assert all(d.works_kind == "demolition_main" and d.is_demolition and d.was_heard for d in decisions)
    numbered = by_number(sitting)
    racine = numbered["20240222-028"]
    assert racine.address_line == "89, 91 et 93 rue Racine"
    assert racine.addresses == ["89, rue Racine", "91, rue Racine", "93, rue Racine"]
    assert racine.decision == DECISION_REFUSED and racine.outcome == "refused"
    assert racine.resolution_number == "CD-2024-014"
    # The heading the typist left its second dot off.
    dorchester = numbered["20240131-034"]
    assert dorchester.civic == 225 and dorchester.street == "rue Dorchester"
    assert dorchester.decision == DECISION_CONDITIONAL and dorchester.outcome == "approved"
    assert dorchester.resolution_number == "CD-2024-016"
    assert "1479325" in dorchester.lot_numbers
    assert "CONSIDÉRANT" in dorchester.text and dorchester.text.startswith("5.4 225, rue Dorchester")


def test_a_committee_item_keeps_its_prose_and_its_lot():
    sitting = read("pv_cucq_dem_2025-10-23.txt")
    assert sitting.sitting_number == "2025-06"
    (plante,) = sitting.decisions
    assert plante.request_number == "20250721-011"
    assert plante.address_line == "289, avenue Plante" and plante.civic == 289
    assert plante.decision == DECISION_CONDITIONAL
    assert plante.decision_label.startswith("il est") and "sous condition" in plante.decision_label
    assert plante.resolution_number == "CD-2025-011"
    assert plante.lot_numbers == ["1941776"]
    # The item's own prose, line breaks and all.
    assert "CONSIDÉRANT que la maison sise au 289, avenue Plante" in plante.text
    assert "patrimonial faible" in plante.text
    assert decision_title(plante, sitting) == (
        "CUCQ, séance du comité de démolition du 23 octobre 2025 - 289, avenue Plante - "
        "Démolition d'un bâtiment principal - approuvée conditionnellement"
    )

    refused = read("pv_cucq_dem_2025-12-18.txt").decisions[0]
    assert refused.request_number == "20250429-104" and refused.decision == DECISION_REFUSED
    assert refused.resolution_number == "CD-2025-015"


def test_a_minute_with_no_list_yields_nothing_and_says_so():
    sitting = read_sitting("procès-verbal\n\n2026-30\n\n1. Ouverture de la séance\n", series="OR", meeting_date=date(2026, 8, 5))
    assert sitting.decisions == []
    assert "no request list found" in sitting.parse_notes
    assert sitting.sitting_number == "2026-30"


# -- the extraction's accidents ------------------------------------------------


@pytest.mark.parametrize(
    "line, garbled",
    [
        ("12-99$YHQXHGHVeUDEOHV", True),   # "12-99 Avenue des Érables", glyph ids as characters
        ("9994XpEHFLQF", True),            # "9994 Québec inc"
        ("5XH6DLQW9DOOLHU2XHVW", True),    # "Rue Saint-Vallier Ouest"
        ("LESSARD.STÉPHANE", False),
        ("HYDRO-QUÉBEC", False),
        ("Multiple", False),
        ("Immovima", False),
        ("1076-420", False),
        ("CIUSSS", False),
    ],
)
def test_a_line_from_a_font_with_no_character_map_is_told_apart(line, garbled):
    from hbu_dataplatform.cities.quebec_city.cucq.minutes import is_garbled

    assert is_garbled(line) is garbled


@pytest.mark.parametrize(
    "line, number",
    [
        ("20260616-04 7", "20260616-047"),
        ("20251217 -010", "20251217-010"),
        ("Page  de 920250226-026", "20250226-026"),
        ("Page 3 de 620240730-061", "20240730-061"),
        ("20250328-020 FAUCHER, CHRISTIAN", None),
        ("Labrosse, Marc", None),
    ],
)
def test_a_request_number_is_read_even_glued_to_the_footer(line, number):
    from hbu_dataplatform.cities.quebec_city.cucq.minutes import request_number_alone

    assert request_number_alone(line) == number


def test_garbled_entries_still_count_as_entries():
    listing = "\n".join(
        [
            "Liste des demandes ayant été présentées à la Commission d'urbanisme et de conservation de Québec",
            "Séance du 16 avril 2025",
            "Demande approuvée",
            "20250301-001",
            "20250301-002",
            "20250301-003",
            "Laforest-Garon, Florence",
            "12-99$YHQXHGHVeUDEOHV",
            "Installation d'un appareil de climatisation ou d'une thermopompe sur le",
            "bâtiment",
            "9994XpEHFLQF",
            "5XH6DLQW9DOOLHU2XHVW",
            "Ajout, agrandissement ou remplacement d'une galerie, perron, balcon,",
            "Pain, Annabelle",
            "329, 14e Rue",
            "Changement ou rénovation de fenêtres sans modification des dimensions",
            "Page  de 9",
        ]
    )
    sitting = read_sitting("procès-verbal\n" + listing, series="OR", meeting_date=date(2025, 4, 16))
    numbers = [d.request_number for d in sitting.decisions]
    # Three entries for three numbers in the column: the garbled address
    # and the garbled applicant-and-address pair are entries all the same.
    assert numbers == ["20250301-001", "20250301-002", "20250301-003"]
    assert sitting.parse_notes == []
    first, second, third = sitting.decisions
    assert first.applicant == "Laforest-Garon, Florence" and first.civic is None
    assert "unreadable" in " ".join(first.parse_notes)
    assert second.applicant == "9994XpEHFLQF" and second.address_line == "5XH6DLQW9DOOLHU2XHVW"
    assert second.works.startswith("Ajout, agrandissement")
    assert third.applicant == "Pain, Annabelle" and third.civic == 329
