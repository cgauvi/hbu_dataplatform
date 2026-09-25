"""Quebec City's conseils de quartier: the registry, the listing, the trail's
link policy, and what a minute's PDF carries."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from hbu_dataplatform.partitions.axes import city_of, known_neighborhoods
from hbu_dataplatform.cities.quebec_city.council.councils import (
    KIND_CONSULTATION_FILE,
    KIND_COUNCIL_FILE,
    KIND_FICHE,
    KIND_GPD,
    NEIGHBORHOOD_COUNCILS,
    CouncilError,
    CouncilFetcher,
    canonical_url,
    classify_link,
    councils_for,
    document_number_of,
    parse_fiche,
    parse_french_date,
    parse_minutes_listing,
    pdf_links,
    urls_in_text,
)
from hbu_dataplatform.rag.documents import PdfFetcher

FIXTURES = Path(__file__).parent.parent / "fixtures" / "council"

# -- the registry -------------------------------------------------------------


def test_montcalm_is_council_12_of_la_cite_limoilou():
    council = NEIGHBORHOOD_COUNCILS[12]
    assert council.name == "Montcalm"
    assert council.neighborhood == "CIL"
    assert council.listing_url.endswith("/conseil-quartier/proces-verbaux/12")


def test_every_council_sits_in_a_known_quebec_city_borough():
    known = set(known_neighborhoods())
    for council in NEIGHBORHOOD_COUNCILS.values():
        assert council.neighborhood in known, council
        assert str(city_of(council.neighborhood)) == "quebec", council


def test_la_cite_limoilou_has_nine_councils_and_montreal_none():
    names = [council.name for council in councils_for("CIL")]
    assert len(names) == 9
    assert "Montcalm" in names and "Saint-Roch" in names
    assert councils_for("VSMPE") == ()
    # Thirty councils, one id each - the host answers ids 1-31 with 16 empty.
    assert len(NEIGHBORHOOD_COUNCILS) == 30
    assert 16 not in NEIGHBORHOOD_COUNCILS


# -- dates --------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Procès verbal du 16 juin 2025", date(2025, 6, 16)),
        ("Procès verbal du 1er juin 2021", date(2021, 6, 1)),
        ("tenue le lundi 7 juillet 2025 à 17 h 30", date(2025, 7, 7)),
        ("24 février 2026", date(2026, 2, 24)),
        ("Date : 30 Avril 2025", date(2025, 4, 30)),
        ("20 décembre 2022", date(2022, 12, 20)),
        ("no date here", None),
        ("31 février 2025", None),
    ],
)
def test_french_dates(text, expected):
    assert parse_french_date(text) == expected


# -- the listing --------------------------------------------------------------


def test_the_listing_yields_every_minute_with_its_year_and_date():
    page = (FIXTURES / "listing_montcalm.html").read_text(encoding="utf-8")
    links = parse_minutes_listing(page)
    assert len(links) == 47
    first, june = links[0], links[6]
    assert first.year == 2026 and first.meeting_date == date(2026, 4, 28)
    assert first.url.startswith("https://affichagesite.villequebec.quebec/fichiers/")
    assert first.size_label == "PDF : 655 Ko"
    assert june.meeting_date == date(2025, 6, 16)
    assert june.url.endswith("/fichiers/5af2da14-d90e-4b3f-95b7-0c2104939eb1")
    assert june.title == "Procès verbal du 16 juin 2025"
    assert links[-1].year == 2021
    # Two files can be filed under the same date - Montcalm's 27 June 2023 -
    # and both are minutes.
    assert sum(1 for link in links if link.meeting_date == date(2023, 6, 27)) == 2


def test_an_empty_listing_is_a_council_error(tmp_path):
    class Session:
        def get(self, url, timeout=None):
            return FakeResponse("<html><body><div id='texte'></div></body></html>", "text/html")

    fetcher = CouncilFetcher(PdfFetcher(cache_dir=tmp_path, session=Session()), session=Session(), request_delay_seconds=0)
    with pytest.raises(CouncilError, match="names no minutes"):
        fetcher.list_minutes(NEIGHBORHOOD_COUNCILS[12])


# -- the trail ----------------------------------------------------------------


@pytest.mark.parametrize(
    "url, kind",
    [
        ("https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917", KIND_FICHE),
        ("https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917#texte", KIND_FICHE),
        ("https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf", KIND_GPD),
        ("https://gpddocs.ville.quebec.qc.ca/gpdblob/CA1-2025-0215.pdf", KIND_GPD),
        ("https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/CPFichierAzure.ashx?Fichier=b6382123-f7a0-4d04-b356-d7cbe2ea2db0.pdf", KIND_CONSULTATION_FILE),
        ("https://affichagesite.villequebec.quebec/fichiers/17e1c171-583a-483f-842f-83208c7a7c45", KIND_COUNCIL_FILE),
        # Not followed: a photo on the fiche, YouTube, the city's other pages.
        ("https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/CPFichierAzure.ashx?Fichier=0d7f8e36.jpg", None),
        ("https://www.youtube.com/watch?v=Enbfsee-cvY", None),
        ("https://www.ville.quebec.qc.ca/citoyens/deneigement/info-deneigement.aspx", None),
        ("https://gpddocs.ville.quebec.qc.ca/other/GT2025-233.pdf", None),
    ],
)
def test_the_link_policy(url, kind):
    assert classify_link(url) == kind


def test_a_fiche_has_one_canonical_spelling():
    plain = "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917"
    assert canonical_url(plain + "#texte") == plain
    assert canonical_url(plain + "#menu") == plain
    assert canonical_url("http://ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917&x=1") == plain
    assert canonical_url("https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf ") == "https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf"
    # The tracking parameter a minute's link carries is not part of the document.
    assert canonical_url("https://gpddocs.ville.quebec.qc.ca/gpdblob/PA2025-002.pdf?_gl=1*1vfinn*_gcl_au*OTEw") == "https://gpddocs.ville.quebec.qc.ca/gpdblob/PA2025-002.pdf"
    azure = "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/CPFichierAzure.ashx?Fichier=b6382123-f7a0-4d04-b356-d7cbe2ea2db0.pdf"
    assert canonical_url(azure + "&utm=x") == azure


def test_the_gpd_number_is_read_off_the_file_name():
    assert document_number_of("https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf") == "GT2025-233"
    assert document_number_of("https://gpddocs.ville.quebec.qc.ca/gpdblob/CA1-2025-0215.pdf") == "CA1-2025-0215"
    assert document_number_of("https://affichagesite.villequebec.quebec/fichiers/abc") is None


def test_urls_spelled_out_in_text_get_their_scheme_back():
    text = (
        "Documentation disponible dans le site Web :\n"
        "www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917.\n"
        "Voir aussi https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf, et rien d'autre."
    )
    assert urls_in_text(text) == [
        "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917",
        "https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf",
    ]


def test_the_fiche_page_reads_as_text_with_its_document_links():
    page = (FIXTURES / "fiche_917.html").read_text(encoding="utf-8")
    url = "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917"
    fiche = parse_fiche(page, url)
    assert fiche.title == "Augmentation du nombre de logements au 355, boulevard René-Lévesque Ouest"
    assert "limite le nombre maximal de logements à 8 alors que le projet en propose 10" in fiche.text
    assert "Adoption du règlement : 7 juillet 2025" in fiche.text
    followed = [classify_link(link) for link in fiche.links if classify_link(link)]
    assert followed.count(KIND_GPD) == 1
    assert followed.count(KIND_CONSULTATION_FILE) == 2
    assert "https://gpddocs.ville.quebec.qc.ca/gpdblob/GT2025-233.pdf" in fiche.links
    # The footer's social links and the page's own scripts are not text.
    assert "googletagmanager" not in fiche.text


# -- the PDF's links ----------------------------------------------------------


def test_link_annotations_are_read_in_page_order():
    pdf = make_pdf(
        ["Voir la fiche du projet."],
        links=[
            "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917",
            "https://www.youtube.com/watch?v=x",
            "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917",
        ],
    )
    assert pdf_links(pdf) == [
        "https://www.ville.quebec.qc.ca/citoyens/participation-citoyenne/activites/fiche.aspx?IdProjet=917",
        "https://www.youtube.com/watch?v=x",
    ]
    assert pdf_links(b"not a pdf") == []


# -- helpers shared with the asset tests ---------------------------------------


class FakeResponse:
    def __init__(self, body, content_type):
        self.content = body if isinstance(body, bytes) else body.encode("utf-8")
        self.headers = {"Content-Type": content_type}
        self.encoding = "utf-8"

    @property
    def text(self):
        return self.content.decode("utf-8")

    def raise_for_status(self):
        return None


def make_pdf(lines: list[str], links: list[str] = ()) -> bytes:
    """A one-page PDF with ``lines`` as Helvetica text and one ``/Link``
    annotation per URL - enough for pypdf to read both back. ASCII only:
    the point is the structure, and the fixtures carry the accents."""

    def pdf_string(value: str) -> str:
        return "(" + value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)") + ")"

    content = "BT /F1 11 Tf 40 780 Td 14 TL " + "".join(
        f"{pdf_string(line)} Tj T* " for line in lines
    ) + "ET"
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        None,  # the page, filled once the annotation ids are known
        f"<< /Length {len(content)} >>\nstream\n{content}\nendstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    annotation_ids = []
    for index, url in enumerate(links):
        annotation_ids.append(len(objects) + 1)
        top = 700 - 20 * index
        objects.append(
            f"<< /Type /Annot /Subtype /Link /Rect [40 {top - 12} 300 {top}] "
            f"/Border [0 0 0] /A << /S /URI /URI {pdf_string(url)} >> >>"
        )
    annots = " ".join(f"{i} 0 R" for i in annotation_ids)
    objects[2] = (
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        "/Resources << /Font << /F1 5 0 R >> >>"
        + (f" /Annots [{annots}]" if annots else "")
        + " >>"
    )
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n{body}\nendobj\n".encode("latin-1")
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)
