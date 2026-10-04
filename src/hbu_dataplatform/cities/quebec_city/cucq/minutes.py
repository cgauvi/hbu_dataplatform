"""What a CUCQ minute says, read as decisions - one per permit request.

A regular sitting's procès-verbal is two documents in one PDF. The body is
short and the same every week: quorum, withdrawals for conflict of
interest, the previous minutes approved, then three blanket resolutions -
*il est résolu d'approuver les demandes de la liste « demandes approuvées »*,
the same for *approuvées conditionnellement*, *il est résolu de refuser* for
*refusées* - an *Auditions* item naming the requests heard, and the close.
The annex is the list those resolutions point at: under each of the three
headings, one entry per request with its number (``20250428-007``, the date
it was filed and a sequence), the applicant, the address and the nature of
the works. **The list is the decision**; the body says only which
resolution carried it.

Since 2024 the Commission also sits as a *comité de démolition* for the
heritage demolitions the 2023 by-law (R.V.Q. 3117) sends to a public
hearing, and those minutes are prose: one numbered item per request with
the address and number in its heading, the *considérants* the
commissioners gave, and *il est résolu d'approuver sous condition* or *de
refuser la demande visant la démolition du bâtiment sis sur le lot ...*.

So two readers, one record shape. `read_sitting` cuts a regular minute's
list into entries and a committee minute's body into items, and returns a
`Decision` per request: what was asked, where, by whom, and what the
Commission decided. Like the council items the reading is regular
expressions with nothing guessed - a field the pattern does not reach is
null, and `parse_notes` says what was expected and not found.

The hard part is the list's layout. The PDF is generated from a table, and
the text layer comes out one of two ways: the request number glued to the
end of the applicant's line (``Devmico Construction Inc.20250428-007``), or
every number of a page listed first as a column and the entries after it
(``20260503-014`` ... then ``Labrosse, Marc`` / ``3819, Rue de Toulouse`` /
...). The second has no marker where an entry begins, so an entry is
recognised by its **address line** - a civic number and a street type -
and the line before it is the applicant; the numbers are paired to the
entries of the same section in order, which is the order both columns
print in. A number can also fall after its entry when a page breaks
inside the table, which is why the pairing is per section and not per
page. A section whose numbers and entries do not come out even is noted
and the surplus is left unpaired rather than guessed.

Deliberately free of Dagster and of pandas.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import date

from hbu_dataplatform.cities.quebec_city.council.councils import parse_french_date
from hbu_dataplatform.cities.quebec_city.council.items import lot_numbers
from hbu_dataplatform.cities.quebec_city.cucq.portal import (
    SERIES_DEMOLITION_COMMITTEE,
    SERIES_REGULAR,
)

# ---------------------------------------------------------------------------
# vocabulary
# ---------------------------------------------------------------------------

#: `sitting_kind`: what the series code on the file name means.
SERIES_KINDS: dict[str, str] = {
    SERIES_REGULAR: "regular",
    SERIES_DEMOLITION_COMMITTEE: "demolition_committee",
}

#: Every CUCQ source table in `rag.chunks` starts with this, the way the
#: council corpus' start with ``council_``, so a reader can ask for "the
#: Commission's decisions" without listing the kinds.
SOURCE_TABLE_PREFIX = "cucq_"


def source_table_for(series: str | None) -> str:
    """``cucq_minutes`` for a regular sitting, ``cucq_demolition_committee``
    for the committee's, ``cucq_other`` for a series the file name does not
    say."""
    kind = SERIES_KINDS.get((series or "").upper())
    if kind == "regular":
        return f"{SOURCE_TABLE_PREFIX}minutes"
    if kind == "demolition_committee":
        return f"{SOURCE_TABLE_PREFIX}demolition_committee"
    return f"{SOURCE_TABLE_PREFIX}other"

#: The `decision` column. A request is approved, approved on conditions the
#: presentation report states, or refused; a committee hearing can also
#: defer, and an item heard without a resolution is `heard`.
DECISION_APPROVED = "approved"
DECISION_CONDITIONAL = "approved_conditionally"
DECISION_REFUSED = "refused"
DECISION_DEFERRED = "deferred"
DECISION_WITHDRAWN = "withdrawn"
DECISION_HEARD = "heard"
DECISION_OTHER = "other"

#: `outcome`: the decision folded to what "approved or rejected" means,
#: the same three words `council_planning_items` uses.
OUTCOMES: dict[str, str | None] = {
    DECISION_APPROVED: "approved",
    DECISION_CONDITIONAL: "approved",
    DECISION_REFUSED: "refused",
    DECISION_DEFERRED: "in_progress",
    DECISION_HEARD: "in_progress",
    DECISION_WITHDRAWN: None,
    DECISION_OTHER: None,
}

#: `works_kind`: what the request is for, decided by the first pattern that
#: matches, in this order. Demolition first on purpose - a request that
#: demolishes and rebuilds is a demolition, which is the question this
#: source exists to answer.
WORKS_KINDS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "demolition_main",
        re.compile(
            r"d[ée]molition\s+(?:compl[èe]te\s+|totale\s+|partielle\s+)?d[’']un(?:e\s+partie\s+d[’']un)?\s+b[âa]timent\s+principal"
            r"|d[ée]molition\s+du\s+b[âa]timent(?:\s+principal|\s+sis)?"
            r"|d[ée]molition\s+(?:de\s+l[’']|d[’']un\s+)immeuble",
            re.I,
        ),
    ),
    ("demolition_accessory", re.compile(r"d[ée]molition\s+d[’']un\s+b[âa]timent\s+accessoire", re.I)),
    ("demolition_other", re.compile(r"\bd[ée]moli(?:tion|r|t|e)\b|d[ée]mant[èe]lement", re.I)),
    ("new_construction", re.compile(r"construction\s+d[’']un\s+(?:nouveau\s+)?b[âa]timent", re.I)),
    ("enlargement", re.compile(r"agrandissement|exhaussement", re.I)),
    ("subdivision", re.compile(r"correction\s+d[’']un\s+ou\s+plusieurs\s+lots|lotissement|\blots?\b", re.I)),
    ("sign", re.compile(r"\benseigne", re.I)),
    ("special_authorization", re.compile(r"autorisation\s+sp[ée]ciale", re.I)),
    (
        "exterior_renovation",
        re.compile(
            r"rev[êe]tement|fen[êe]tre|\bportes?\b|toiture|toit\b|galerie|balcon|perron|escalier"
            r"|fa[çc]ade|ma[çc]onnerie|peindre|peinture|fondation|avant-toit|marquise|cl[ôo]ture"
            r"|piscine|installation\s+[ée]lectrique|climati|thermopompe|stationnement|terrasse"
            r"|chemin[ée]e|cr[ée]pi|am[ée]nagement\s+paysager",
            re.I,
        ),
    ),
)

_REQUEST_NUMBER = re.compile(r"\b(\d{8})-(\d{3})\b")
#: A request number standing alone on its line - the column layout - with
#: room for the one case the extraction splits the last digit off
#: (``20260616-04 7``).
_NUMBER_LINE = re.compile(r"^(\d{8})\s?-\s?(\d{2,3})(?:\s(\d))?$")
#: A request number anywhere in a line, tolerant of a space round the dash.
_REQUEST_NUMBER_LOOSE = re.compile(r"\d{8}\s?-\s?\d{3}")
#: What a page footer is made of, for the line that glues a number to one.
_FOOTER_WORDS = re.compile(r"Page|de|\d|\s", re.I)
_VOWELS = frozenset("aeiouyàâäéèêëîïôöùûüÿ")
#: Characters a real list line never carries and a glyph-id line often does.
_GARBLED_SYMBOLS = re.compile(r"[$%&{}\\\[\]|<>^~`]")
#: The applicant's line with the number glued to its end - no space between,
#: which is what tells it from a works line that *cites* a request ("voir
#: demande 20170501-024"). The other glued form puts the number first.
_APPLICANT_NUMBER_LINE = re.compile(r"^(?P<applicant>.*?\S)(?P<number>\d{8}-\d{3})\s*$")
_NUMBER_APPLICANT_LINE = re.compile(r"^(?P<number>\d{8}-\d{3})\s+(?P<applicant>[^\d\s].*?)\s*$")
#: A page footer, alone or glued to the end of another line: "Page 3 de 6",
#: "Demande approuvée  de 7Page 1", "20260616-056 Page 4 de 6".
_PAGE_FOOTER = re.compile(r"\s*(?:de\s*\d+\s*)?Page\s*\d+(?:\s*de\s*\d+)?\s*$", re.I)
#: An address the source cut off after its civic number, which the
#: hyphen-keeping flatten then glued to the next line: "805-Ajout, ...".
#: The rest has to be a word - "Ajout", "Changement" - so "307-A, 307-B,
#: 307, Rue Saint-Benoît", a suffix and more doors, is not read as one.
_TRUNCATED_ADDRESS = re.compile(r"^(?P<civic>\d{1,5}[A-Za-z]?-)(?P<rest>[A-ZÉÈÀÎÔ][a-zéèàêîôûç]{2,}.*)$")

_SECTION = re.compile(
    r"^Demandes?\s+(?P<verb>approuv[ée]+e?s?|accept[ée]+e?s?|refus[ée]+e?s?|report[ée]+e?s?|retir[ée]+e?s?|suspendue?s?)"
    r"(?P<conditional>\s+conditionnellement)?(?=\s|$|\d)",
    re.I,
)
#: Accent-tolerant throughout, like the council patterns: the fixtures the
#: asset tests build are ASCII, and a PDF has been known to lose one.
_LIST_START = re.compile(r"Liste des demandes ayant [ée]t[ée] pr[ée]sent[ée]es", re.I)
#: The list's own page header - also where one page ends and the next begins.
_LIST_HEADER = re.compile(r"^Liste des demandes ayant [ée]t[ée] pr[ée]sent[ée]es", re.I)
_LIST_NOISE = (
    _LIST_HEADER,
    re.compile(r"^S[ée]ance du\s+\d", re.I),
    re.compile(r"^(?:de\s+\d+\s*)?Page\s+\d+(?:\s+de\s+\d+)?$", re.I),
    re.compile(r"^Ville de Qu[ée]bec\s+[–-]\s+Proc[èe]s-verbal", re.I),
)

_STREET_TYPES = (
    r"rue|avenue|boulevard|chemin|c[ôo]te|route|place|grande[- ]all[ée]e|all[ée]e|mont[ée]e"
    r"|carr[ée]|promenade|terrasse|impasse|autoroute|rang|parc|quai|cours|passage|ruelle"
    r"|sentier|voie|esplanade|croissant|rond-point"
)
_ADDRESS_LINE = re.compile(
    rf"^(?P<numbers>.*?)(?P<street>(?:\d+(?:re|er|e|ème)\s+(?:rue|avenue)|(?:{_STREET_TYPES}))\b.*)$",
    re.I,
)
#: A street with no type word - "8195, Le Trait-Carré Ouest" - accepted only
#: behind a civic number and a comma, and only when short and capitalised.
_ADDRESS_NO_TYPE = re.compile(
    r"^(?P<numbers>\d[\dA-Za-z/½\-]*(?:\s*,\s*\d[\dA-Za-z/½\-]*)*)\s*,\s+(?P<street>[A-ZÉÈÀÎÔ][^,;:]{2,50})$"
)
#: An address cut off after its civic number: "122-".
_CIVIC_ONLY = re.compile(r"^(?P<civic>\d{1,5}[A-Za-z]?)-?$")
#: What may precede the street on an address line: civic numbers, their
#: suffixes ("116-A", "229-1/2", "1-389"), and the separators between them.
_CIVIC_RUN = re.compile(r"^(?:\d[\dA-Za-z/½\-]*\s*(?:,|\bet\b|\bà\b|-)?\s*)*$")
_CIVIC_TOKEN = re.compile(r"\d[\dA-Za-z/½\-]*")
_BLOCK = re.compile(r"\s*\((?:bloc|ensemble)\)\s*", re.I)
_TRAILING_CIVIC = re.compile(r"\s+\d{1,5}[A-Za-z]?,?$")

_DWELLINGS_RANGE = re.compile(r"\bde\s+(\d+)\s+[àa]\s+(\d+)\s+logements", re.I)
_DWELLINGS_MIN = re.compile(r"\b(\d+)\s+logements\s+et\s+plus", re.I)
_DWELLINGS_MAX = re.compile(r"\b(\d+)\s+logements\s+et\s+moins", re.I)
_DWELLINGS_COUNT = re.compile(r"\b(?:de|comportant|comptant|total\s+de|ajout\s+de)\s+(\d+)\s+logements\b", re.I)

_SITTING_NUMBER = re.compile(r"^\s*(\d{4}-\d{2,3})\s*$", re.M)
_COMMITTEE_SITTING_NUMBER = re.compile(r"N[oº°]\s*S[ée]ance\s*:\s*(\d{4}-\d{2,3})", re.I)
#: "tenue le mercredi 5 août 2026", "tenue à Québec le mardi 10 mars 2020",
#: "..., le mercredi 20 mars 2024". No leading word boundary: the second form
#: starts at a comma, which has none.
_HELD_ON = re.compile(
    r"(?:tenue(?:\s+à\s+Québec)?\s*,?\s+le|,\s+le)\s+(?:(?:lundi|mardi|mercredi|jeudi|vendredi)\s+)?"
    r"(\d{1,2}(?:er)?\s+[a-zéû]+\s+\d{4})",
    re.I,
)
#: The list's own header, for a minute whose body is missing or a scan.
_LIST_DATE = re.compile(r"^S[ée]ance du\s+(\d{1,2}(?:er)?\s+[a-zéû]+\s+\d{4})", re.I | re.M)
_RESOLUTION_FOR = re.compile(
    r"Dem\s?andes\s+de\s+permis\s+(?P<verb>approuv[ée]es(?:\s+conditionnellement)?|refus[ée]es)\s+"
    r"R[ée]solution\s+C\.?\s?U\.?\s*(?P<year>\d{4})\s*-\s*(?P<seq>\d{3})",
    re.I,
)
_AUDITIONS = re.compile(r"\bAuditions?\b(?P<body>.*?)(?=\s\d{1,2}\.\s+[A-ZÉ]|$)", re.S)
#: "5.1. 289, avenue Plante, demande no ..." - and "5.4 225, rue Dorchester",
#: the one heading the typist left its second dot off.
_HEARING_ITEM = re.compile(r"^\s*(\d{1,2})\.(\d{1,2})\.?\s+(?P<heading>[\dA-ZÉÈÀ].*)$", re.M)
_TOP_HEADING = re.compile(r"^\s*\d{1,2}\.\s+[A-ZÉÀ]", re.M)
_HEARING_NUMBER = re.compile(r"demande\s+n[oº°]\s*(\d{8}-\d{3})", re.I)
_COMMITTEE_RESOLUTION = re.compile(r"R[ée]solution\s+CD\s*-\s*(\d{4}-\d{3})", re.I)
_RESOLVED = re.compile(
    r"il\s+est\s+r[ée]solu\s+(?P<verb>d[’']approuver\s+sous\s+conditions?|d[’']approuver|d[’']accepter"
    r"|de\s+refuser|de\s+reporter|de\s+surseoir|de\s+suspendre|de\s+prendre\s+acte\s+du\s+retrait)",
    re.I,
)

# ---------------------------------------------------------------------------
# records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Address:
    """An address line taken apart."""

    raw: str
    #: The civic tokens as written - "116-A", "229-1/2", "243 à 305" as two.
    civic_numbers: list[str]
    #: The first token's civic number, the one a door is looked up by.
    civic: int | None
    street: str | None
    #: "(Bloc)": the request is for a block of doors, not one.
    is_block: bool

    @property
    def addresses(self) -> list[str]:
        """One "N, Street" per civic token - the strings a site is matched by."""
        if not self.street:
            return [self.raw] if self.raw else []
        if not self.civic_numbers:
            return [self.street]
        return [f"{token}, {self.street}" for token in self.civic_numbers]

    @property
    def key(self) -> str | None:
        """``"467, Rue Arago Ouest"``: the first door, for placement."""
        if self.civic is None or not self.street:
            return None
        return f"{self.civic}, {self.street}"


@dataclass
class Decision:
    """One request the Commission ruled on, as the minutes state it."""

    item_index: int
    request_number: str | None
    applicant: str | None
    address_line: str | None
    addresses: list[str]
    civic_numbers: list[str]
    civic: int | None
    street: str | None
    is_block: bool
    works: str
    works_kind: str
    is_demolition: bool
    dwellings_min: int | None
    dwellings_max: int | None
    project_dwellings: int | None
    decision: str
    decision_label: str | None
    outcome: str | None
    resolution_number: str | None
    lot_numbers: list[str]
    was_heard: bool
    text: str
    parse_notes: list[str] = field(default_factory=list)

    def as_row(self) -> dict:
        return asdict(self)


@dataclass
class Sitting:
    """One minute, read."""

    series: str | None
    sitting_kind: str
    sitting_number: str | None
    meeting_date: date | None
    #: decision -> "C.U. 2026-118": the blanket resolution each list carried.
    resolutions: dict[str, str]
    audition_numbers: list[str]
    decisions: list[Decision]
    parse_notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# small readers
# ---------------------------------------------------------------------------


def normalize_request_number(text: str) -> str:
    """``20260616-04 7`` -> ``20260616-047``: a split last digit put back."""
    return re.sub(r"\s+", "", text.strip())


def parse_address_line(line: str) -> Address | None:
    """``"467, 469, 471, Rue Arago Ouest"`` taken apart, or None when the
    line is not an address: no street type, or words where the civic
    numbers should be."""
    raw = line.strip().rstrip("|").strip()
    if not raw:
        return None
    is_block = bool(_BLOCK.search(raw))
    cleaned = _BLOCK.sub(" ", raw).strip()
    if civic_only := _CIVIC_ONLY.match(cleaned):
        token = civic_only.group("civic")
        return Address(raw=raw, civic_numbers=[token], civic=civic_of(token), street=None, is_block=is_block)
    match = _ADDRESS_LINE.match(cleaned)
    if match and _CIVIC_RUN.match(match.group("numbers").strip()) or (match := _ADDRESS_NO_TYPE.match(cleaned)):
        numbers, street = match.group("numbers").strip(), match.group("street").strip()
    else:
        return None
    street = street.rstrip(" ,;:|").strip()
    tokens = _CIVIC_TOKEN.findall(numbers)
    civic = civic_of(tokens[0]) if tokens else None
    return Address(raw=raw, civic_numbers=tokens, civic=civic, street=street or None, is_block=is_block)


def names_an_entry(line: str) -> bool:
    """Whether a line of the list's column layout is an entry's address -
    the one thing that marks where an entry begins there. A civic number
    settles it; a bare street is taken only when short and capitalised, so
    a works line that happens to open on "terrasse, escalier ou ..." is not
    one."""
    address = parse_address_line(line)
    if address is None:
        return False
    if address.civic_numbers:
        return True
    return bool(address.street) and len(address.street) <= 50 and address.street[0].isupper()


def civic_of(token: str) -> int | None:
    """The civic number in a token: ``116-A`` is 116, ``229-1/2`` is 229,
    and ``1-389`` - a unit before the civic - is 389."""
    parts = token.split("-")
    if len(parts) > 1 and parts[-1].isdigit():
        return int(parts[-1])
    digits = re.match(r"\d+", parts[0])
    return int(digits.group()) if digits else None


def classify_works(text: str) -> str:
    for kind, pattern in WORKS_KINDS:
        if pattern.search(text):
            return kind
    return "other"


def dwellings(text: str) -> tuple[int | None, int | None, int | None]:
    """``(min, max, project)`` dwellings the works line states: the band the
    permit category names ("de 1 à 3 logements", "9 logements et plus") and
    the project's own count when the description gives one."""
    low = high = None
    band = None
    if band := _DWELLINGS_RANGE.search(text):
        low, high = int(band.group(1)), int(band.group(2))
    elif band := _DWELLINGS_MIN.search(text):
        low = int(band.group(1))
    elif band := _DWELLINGS_MAX.search(text):
        high = int(band.group(1))
    project = None
    for match in _DWELLINGS_COUNT.finditer(text):
        # "de 9 logements et plus" is the band again, not a count.
        if band and match.start() < band.end() and match.end() > band.start():
            continue
        project = int(match.group(1))
        break
    return low, high, project


def section_decision(label: str) -> tuple[str, str]:
    """``(decision, label as printed)`` for a list heading."""
    match = _SECTION.match(label)
    if not match:
        return DECISION_OTHER, label.strip()
    verb = match.group("verb").lower()
    printed = match.group(0).strip()
    if match.group("conditional"):
        return DECISION_CONDITIONAL, printed
    if verb.startswith(("approuv", "accept")):
        return DECISION_APPROVED, printed
    if verb.startswith("refus"):
        return DECISION_REFUSED, printed
    if verb.startswith(("report", "suspend")):
        return DECISION_DEFERRED, printed
    if verb.startswith("retir"):
        return DECISION_WITHDRAWN, printed
    return DECISION_OTHER, printed


def resolved_decision(text: str) -> tuple[str | None, str | None]:
    """``(decision, the operative words)`` of a hearing item's resolution."""
    match = _RESOLVED.search(text)
    if not match:
        return None, None
    verb = re.sub(r"\s+", " ", match.group("verb").lower())
    if "sous condition" in verb:
        return DECISION_CONDITIONAL, match.group(0)
    if "approuver" in verb or "accepter" in verb:
        return DECISION_APPROVED, match.group(0)
    if "refuser" in verb:
        return DECISION_REFUSED, match.group(0)
    if "retrait" in verb:
        return DECISION_WITHDRAWN, match.group(0)
    return DECISION_DEFERRED, match.group(0)


def sitting_number(text: str, *, series: str | None) -> str | None:
    head = text[:3000]
    if series == SERIES_DEMOLITION_COMMITTEE:
        match = _COMMITTEE_SITTING_NUMBER.search(head)
        return match.group(1) if match else None
    match = _SITTING_NUMBER.search(head)
    return match.group(1) if match else None


def held_on(text: str) -> date | None:
    """The sitting's date as the body states it."""
    match = _HELD_ON.search(text[:4000])
    return parse_french_date(match.group(1)) if match else None


def list_date(text: str) -> date | None:
    """The sitting's date as the annexed list's header states it."""
    match = _LIST_DATE.search(text)
    return parse_french_date(match.group(1)) if match else None


def resolutions(body: str) -> dict[str, str]:
    """``{decision: "C.U. 2026-118"}`` from the body's blanket resolutions."""
    flat = re.sub(r"\s+", " ", body)
    found: dict[str, str] = {}
    for match in _RESOLUTION_FOR.finditer(flat):
        verb = match.group("verb").lower()
        if "conditionnellement" in verb:
            decision = DECISION_CONDITIONAL
        elif verb.startswith("approuv"):
            decision = DECISION_APPROVED
        else:
            decision = DECISION_REFUSED
        found.setdefault(decision, f"C.U. {match.group('year')}-{match.group('seq')}")
    return found


def audition_numbers(body: str) -> list[str]:
    """The requests the *Auditions* item says were heard."""
    flat = re.sub(r"\s+", " ", body)
    found: list[str] = []
    for match in _AUDITIONS.finditer(flat):
        for number in _REQUEST_NUMBER.finditer(match.group("body")):
            value = f"{number.group(1)}-{number.group(2)}"
            if value not in found:
                found.append(value)
    return found


def _is_noise(line: str) -> bool:
    return any(pattern.match(line) for pattern in _LIST_NOISE)


# ---------------------------------------------------------------------------
# the request list
# ---------------------------------------------------------------------------


@dataclass
class _Entry:
    decision: str
    label: str
    applicant: str | None
    number: str | None
    address_line: str | None = None
    works_lines: list[str] = field(default_factory=list)
    expect_address: bool = False
    notes: list[str] = field(default_factory=list)


def split_list(text: str) -> tuple[str, str | None]:
    """``(body, list)``: the minute cut where its annexed list begins."""
    match = _LIST_START.search(text)
    if not match:
        return text, None
    return text[: match.start()], text[match.start():]


def read_request_list(listing: str) -> tuple[list[_Entry], list[str]]:
    """The annexed list cut into entries, in order, with the notes.

    The numbers of the column layout are paired to entries **per page** -
    the page being what lies between two of the list's own headers - and
    only when a page's numbers and its unnumbered entries come out even.
    The two columns print in the same order, so an even count pairs them
    exactly; an uneven one means a number fell off the page, and pairing in
    order from there would give every following entry its neighbour's
    number. Those entries are left without one, and noted.
    """
    entries: list[_Entry] = []
    notes: list[str] = []
    section: tuple[str, str | None] | None = None
    page_entries: list[_Entry] = []
    page_numbers: list[str] = []
    # What a page could not pair, in document order, for one more try at
    # the end: a number printed at the top of the next page's column for an
    # entry whose text ended the page before leaves one page a number short
    # and the next a number long, and the two meet here.
    leftover_entries: list[_Entry] = []
    leftover_numbers: list[str] = []
    dangling: list[str] = []
    current: _Entry | None = None

    def close_page() -> None:
        unnumbered = [entry for entry in page_entries if entry.number is None]
        if unnumbered and len(unnumbered) == len(page_numbers):
            for entry, number in zip(unnumbered, page_numbers):
                entry.number = number
        elif unnumbered or page_numbers:
            leftover_entries.extend(unnumbered)
            leftover_numbers.extend(page_numbers)
        if dangling:
            notes.append(f"{len(dangling)} line(s) belonged to no entry: {dangling[0][:60]!r}")
        page_entries.clear()
        page_numbers.clear()
        dangling.clear()

    def close_document() -> None:
        close_page()
        if leftover_entries and len(leftover_entries) == len(leftover_numbers):
            for entry, number in zip(leftover_entries, leftover_numbers):
                entry.number = number
            notes.append(f"{len(leftover_numbers)} request number(s) paired across a page break")
        elif leftover_entries or leftover_numbers:
            notes.append(
                f"{len(leftover_numbers)} request number(s) listed for "
                f"{len(leftover_entries)} entr(y/ies) lacking one; none paired"
            )
            for entry in leftover_entries:
                entry.notes.append("no request number paired: the number column did not line up")

    def open_entry(applicant: str | None, number: str | None, address_line: str | None, *, expect_address: bool = False) -> _Entry:
        assert section is not None
        entry = _Entry(
            decision=section[0],
            label=section[1] or "",
            applicant=applicant.strip() if applicant else None,
            number=number,
            address_line=address_line,
            expect_address=expect_address,
        )
        entries.append(entry)
        page_entries.append(entry)
        return entry

    lines = _list_lines(listing)
    for position, line in enumerate(lines):
        if _LIST_HEADER.match(line):
            close_page()
            continue
        if _is_noise(line):
            continue
        if _SECTION.match(line):
            section = section_decision(line)
            current = None
            continue
        if section is None:
            # Text before the first heading is the list's own header - unless
            # it is already an entry, which happens when a page lost its
            # heading: the entries are kept, with no decision stated.
            if not _starts_an_entry(line):
                continue
            section = (DECISION_OTHER, None)
            notes.append("entries before any list heading: their decision is not stated")
        if number := request_number_alone(line):
            page_numbers.append(number)
            continue
        if current is not None and current.expect_address:
            current.expect_address = False
            if truncated := _TRUNCATED_ADDRESS.match(line):
                current.address_line = truncated.group("civic")
                current.works_lines.append(truncated.group("rest"))
            else:
                current.address_line = line
                if is_garbled(line):
                    current.notes.append("address line unreadable: a font with no character map")
            continue
        if applicant_match := _APPLICANT_NUMBER_LINE.match(line) or _NUMBER_APPLICANT_LINE.match(line):
            current = open_entry(applicant_match.group("applicant"), applicant_match.group("number"), None, expect_address=True)
            continue
        truncated = _TRUNCATED_ADDRESS.match(line)
        garbled = is_garbled(line)
        next_line = lines[position + 1] if position + 1 < len(lines) else ""
        if garbled and (current is None or current.address_line is not None) and is_garbled(next_line):
            # Two unreadable lines in a row: an applicant and its address,
            # both printed in the font without a character map.
            current = open_entry(line, None, None, expect_address=True)
            current.notes.append("applicant unreadable: a font with no character map")
            continue
        if (truncated or garbled or names_an_entry(line)) and (current is None or current.address_line is not None):
            # The column layout: the address names the entry, and the line
            # before it - already taken for the previous entry's works, or
            # waiting with nothing to belong to - is the applicant.
            applicant: str | None = None
            if current is not None and current.works_lines:
                applicant = current.works_lines.pop()
            elif dangling:
                applicant = dangling.pop()
            if truncated:
                current = open_entry(applicant, None, truncated.group("civic"))
                current.works_lines.append(truncated.group("rest"))
            else:
                current = open_entry(applicant, None, line)
                if garbled:
                    current.notes.append("address line unreadable: a font with no character map")
            continue
        if current is None:
            dangling.append(line)
        else:
            current.works_lines.append(line)
    close_document()
    return entries, notes


def _list_lines(listing: str) -> list[str]:
    """The list's lines, stripped of their page footers and of blanks."""
    lines: list[str] = []
    for raw_line in listing.split("\n"):
        line = _PAGE_FOOTER.sub("", raw_line.strip()).strip()
        if line:
            lines.append(line)
    return lines


def request_number_alone(line: str) -> str | None:
    """The request number a line consists of, or None.

    A line of the number column - ``20260616-04 7`` with its split digit
    put back - and the one case the extraction glues the number to the
    page footer: ``Page  de 920250226-026``, ``Page 3 de 620240730-061``.
    """
    if _NUMBER_LINE.match(line):
        return normalize_request_number(line)
    match = _REQUEST_NUMBER_LOOSE.search(line)
    if match is None:
        return None
    rest = line[: match.start()] + line[match.end():]
    if _FOOTER_WORDS.sub("", rest) == "":
        return normalize_request_number(match.group(0))
    return None


def is_garbled(line: str) -> bool:
    """Whether a line came out of a font with no character map.

    Some entries are set in a TrueType subset the PDF carries no ToUnicode
    table for, and their text layer is the glyph ids read as characters:
    ``12-99$YHQXHGHVeUDEOHV`` for an avenue, ``9994XpEHFLQF`` for a numbered
    company. No spaces survive and the case changes inside what should be
    one word, which is what this reads.
    """
    if " " in line or len(line) < 6 or line.isdigit():
        return False
    if _GARBLED_SYMBOLS.search(line):
        return True
    letters = [c for c in line if c.isalpha()]
    if len(letters) < 5:
        return False
    changes = sum(1 for a, b in zip(letters, letters[1:]) if a.isupper() != b.isupper())
    if changes >= 3:
        return True
    # Glyph ids read as characters put every lower-case vowel on a
    # consonant - a→D, e→H, i→L, o→R, u→X - so a word of them has next to
    # none beyond what l and r turn into, where "LESSARD.STÉPHANE" keeps a third.
    vowels = sum(1 for c in letters if c.lower() in _VOWELS)
    return vowels / len(letters) <= 0.25


def _starts_an_entry(line: str) -> bool:
    """Whether a line of the list is where an entry, or its number, begins."""
    return bool(
        _NUMBER_LINE.match(line)
        or _APPLICANT_NUMBER_LINE.match(line)
        or _NUMBER_APPLICANT_LINE.match(line)
        or _TRUNCATED_ADDRESS.match(line)
        or names_an_entry(line)
    )


def _decision_from_entry(entry: _Entry, index: int, heard: set[str]) -> Decision:
    address = parse_address_line(entry.address_line) if entry.address_line else None
    applicant = entry.applicant
    if applicant and address and address.civic is not None:
        # The civic number wrapped onto the applicant's line and was then
        # printed again on its own: "MAISON RICHELIEU ... INC. 2808,".
        applicant = _TRAILING_CIVIC.sub("", applicant).strip() or applicant
    works = "\n".join(entry.works_lines).strip()
    low, high, project = dwellings(works)
    notes = list(entry.notes)
    if entry.address_line and address is None:
        notes.append("address line did not parse")
    if not entry.address_line:
        notes.append("no address line")
    if not works:
        notes.append("no works stated")
    number = entry.number
    return Decision(
        item_index=index,
        request_number=number,
        applicant=applicant or None,
        address_line=entry.address_line,
        addresses=address.addresses if address else ([entry.address_line] if entry.address_line else []),
        civic_numbers=address.civic_numbers if address else [],
        civic=address.civic if address else None,
        street=address.street if address else None,
        is_block=address.is_block if address else False,
        works=works,
        works_kind=classify_works(works),
        is_demolition=classify_works(works).startswith("demolition"),
        dwellings_min=low,
        dwellings_max=high,
        project_dwellings=project,
        decision=entry.decision,
        decision_label=entry.label or None,
        outcome=OUTCOMES.get(entry.decision),
        resolution_number=None,
        lot_numbers=lot_numbers(works),
        was_heard=bool(number and number in heard),
        text="",
        parse_notes=notes,
    )


# ---------------------------------------------------------------------------
# the committee's hearings
# ---------------------------------------------------------------------------


def read_hearing_items(text: str) -> list[tuple[str, str]]:
    """``[(heading, item text)]`` for every ``5.1.``-style item of a
    committee minute, each running to the next item or top-level heading."""
    starts = list(_HEARING_ITEM.finditer(text))
    items: list[tuple[str, str]] = []
    for position, match in enumerate(starts):
        start = match.start()
        end = starts[position + 1].start() if position + 1 < len(starts) else len(text)
        top = _TOP_HEADING.search(text, match.end(), end)
        if top:
            end = top.start()
        body = text[start:end]
        heading = match.group("heading").strip()
        # A heading that wrapped: the number is on the next line.
        if not _HEARING_NUMBER.search(heading):
            following = body[match.end() - start:].strip().split("\n")
            for extra in following[:2]:
                heading = f"{heading} {extra.strip()}"
                if _HEARING_NUMBER.search(heading):
                    break
        items.append((heading, body))
    return items


def _decision_from_hearing(heading: str, body: str, index: int) -> Decision | None:
    number_match = _HEARING_NUMBER.search(heading) or _HEARING_NUMBER.search(body)
    if number_match is None:
        # Not a request: the agenda's own items are numbered the same way.
        return None
    number = number_match.group(1)
    address_text = re.split(r",?\s*demande\s+n[oº°]", heading, maxsplit=1, flags=re.I)[0]
    address = parse_address_line(address_text)
    decision, words = resolved_decision(body)
    resolution = _COMMITTEE_RESOLUTION.search(body)
    notes: list[str] = []
    if address is None:
        notes.append("address in heading did not parse")
    if decision is None:
        notes.append("no resolution in the item; heard only")
    works = "Démolition d'un bâtiment principal"
    if re.search(r"d[ée]molition\s+partielle", body, re.I):
        works = "Démolition partielle d'un bâtiment principal"
    low, high, project = dwellings(body)
    item_text = re.sub(r"[ \t]+", " ", body).strip()
    return Decision(
        item_index=index,
        request_number=number,
        applicant=None,
        address_line=address.raw if address else address_text.strip() or None,
        addresses=address.addresses if address else ([address_text.strip()] if address_text.strip() else []),
        civic_numbers=address.civic_numbers if address else [],
        civic=address.civic if address else None,
        street=address.street if address else None,
        is_block=address.is_block if address else False,
        works=works,
        works_kind="demolition_main",
        is_demolition=True,
        dwellings_min=low,
        dwellings_max=high,
        project_dwellings=project,
        decision=decision or DECISION_HEARD,
        decision_label=words,
        outcome=OUTCOMES.get(decision or DECISION_HEARD),
        resolution_number=f"CD-{resolution.group(1)}" if resolution else None,
        lot_numbers=lot_numbers(body),
        was_heard=True,
        text=item_text,
        parse_notes=notes,
    )


# ---------------------------------------------------------------------------
# the whole minute
# ---------------------------------------------------------------------------


def read_sitting(text: str, *, series: str | None, meeting_date: date | None = None) -> Sitting:
    """A minute read into its decisions.

    ``series`` is the file name's code (`portal.series_of`); ``meeting_date``
    is what the listing states and is kept when the body states nothing.
    """
    kind = SERIES_KINDS.get(series or "", "other")
    body, listing = split_list(text)
    stated = held_on(body)
    listed = list_date(text)
    # The listing's date first: the body of 2026-06-23 says "mercredi 25
    # juin 2026" of a Tuesday sitting the list and the file name both date
    # the 23rd. The typist's slip is noted, not believed - and so is the
    # file of 2026-06-10 whose annexed list is the 23rd's.
    when = meeting_date or stated or listed
    number = sitting_number(text, series=series)
    notes: list[str] = []
    if meeting_date and stated and stated != meeting_date:
        notes.append(f"the body dates the sitting {stated.isoformat()}, the listing {meeting_date.isoformat()}")
    if meeting_date and listed and listed != meeting_date:
        notes.append(f"the annexed list is dated {listed.isoformat()}, the listing {meeting_date.isoformat()}")
    decisions: list[Decision] = []

    if kind == "demolition_committee":
        for heading, item in read_hearing_items(text):
            decision = _decision_from_hearing(heading, item, len(decisions) + 1)
            if decision is not None:
                decisions.append(decision)
        if not decisions:
            notes.append("no hearing item with a request number")
        resolved = {}
    else:
        resolved = resolutions(body)
        heard = set(audition_numbers(body))
        if listing is None:
            notes.append("no request list found")
        else:
            entries, list_notes = read_request_list(listing)
            notes.extend(list_notes)
            for entry in entries:
                decisions.append(_decision_from_entry(entry, len(decisions) + 1, heard))
            if not entries:
                notes.append("the request list has no entry")
        for decision in decisions:
            decision.resolution_number = resolved.get(decision.decision)

    sitting = Sitting(
        series=series,
        sitting_kind=kind,
        sitting_number=number,
        meeting_date=when,
        resolutions=resolved,
        audition_numbers=audition_numbers(body) if kind != "demolition_committee" else [],
        decisions=decisions,
        parse_notes=notes,
    )
    for decision in decisions:
        if not decision.text:
            decision.text = decision_text(decision, sitting)
    return sitting


# ---------------------------------------------------------------------------
# how a decision reads in the corpus
# ---------------------------------------------------------------------------

_DECISION_WORDS: dict[str, str] = {
    DECISION_APPROVED: "demande approuvée",
    DECISION_CONDITIONAL: "demande approuvée conditionnellement",
    DECISION_REFUSED: "demande refusée",
    DECISION_DEFERRED: "demande reportée",
    DECISION_WITHDRAWN: "demande retirée",
    DECISION_HEARD: "demande entendue en audition",
    DECISION_OTHER: "demande traitée",
}

_SITTING_WORDS: dict[str, str] = {
    "regular": "séance",
    "demolition_committee": "séance du comité de démolition",
    "other": "séance",
}


def french_date(value: date | None) -> str | None:
    from hbu_dataplatform.cities.quebec_city.council.corpus import (
        french_date as _french_date,
    )

    return _french_date(value)


def decision_text(decision: Decision, sitting: Sitting) -> str:
    """A list entry as one readable paragraph - what a chunk of the corpus
    says, and what a person reads under a citation. A hearing item keeps
    its own prose and takes only the heading line."""
    when = french_date(sitting.meeting_date)
    header = f"Commission d'urbanisme et de conservation de Québec, {_SITTING_WORDS.get(sitting.sitting_kind, 'séance')}"
    if when:
        header += f" du {when}"
    if sitting.sitting_number:
        header += f" (procès-verbal {sitting.sitting_number})"
    parts = [header + "."]
    subject = f"Demande {decision.request_number}" if decision.request_number else "Demande"
    if decision.address_line:
        subject += f" - {decision.address_line}"
    parts.append(subject + ".")
    if decision.applicant:
        parts.append(f"Requérant : {decision.applicant.rstrip('.')}.")
    if decision.works:
        works = re.sub(r"\s*\n\s*", " ", decision.works).strip()
        parts.append(f"Travaux : {works.rstrip('.')}.")
    verdict = _DECISION_WORDS.get(decision.decision, "demande traitée")
    if decision.resolution_number:
        verdict += f" (résolution {decision.resolution_number})"
    if decision.was_heard and decision.decision != DECISION_HEARD:
        verdict += ", requérant entendu en audition"
    parts.append(f"Décision : {verdict}.")
    return " ".join(parts)


def decision_title(decision: Decision, sitting: Sitting) -> str:
    """``rag.chunks.title``: the sitting, the address and the verdict, so a
    search for a street finds the entry by name."""
    when = french_date(sitting.meeting_date)
    label = "CUCQ, " + (_SITTING_WORDS.get(sitting.sitting_kind, "séance"))
    if when:
        label += f" du {when}"
    pieces = [label]
    if decision.address_line:
        pieces.append(decision.address_line)
    first_works = decision.works.split("\n")[0].strip() if decision.works else ""
    if first_works:
        pieces.append(first_works[:90])
    pieces.append(_DECISION_WORDS.get(decision.decision, "traitée").replace("demande ", ""))
    return " - ".join(pieces)


__all__ = [
    "DECISION_APPROVED",
    "DECISION_CONDITIONAL",
    "DECISION_DEFERRED",
    "DECISION_HEARD",
    "DECISION_OTHER",
    "DECISION_REFUSED",
    "DECISION_WITHDRAWN",
    "OUTCOMES",
    "SERIES_KINDS",
    "SOURCE_TABLE_PREFIX",
    "WORKS_KINDS",
    "Address",
    "Decision",
    "Sitting",
    "audition_numbers",
    "civic_of",
    "classify_works",
    "decision_text",
    "decision_title",
    "dwellings",
    "held_on",
    "is_garbled",
    "list_date",
    "names_an_entry",
    "normalize_request_number",
    "parse_address_line",
    "read_hearing_items",
    "read_request_list",
    "read_sitting",
    "request_number_alone",
    "resolutions",
    "resolved_decision",
    "section_decision",
    "sitting_number",
    "source_table_for",
    "split_list",
]
