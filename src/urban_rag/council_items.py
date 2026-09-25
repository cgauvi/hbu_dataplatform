"""What a council document says about zoning, lots and dwellings, read as
fields.

The minutes of a conseil de quartier, the *sommaire décisionnel* behind an
amendment, the arrondissement council's resolution and the consultation
report all describe the same decision in prose, each from its own seat. This
module reads that prose into one record shape - a *planning item* - so that
"which zones had their dwelling cap raised this year" is a filter rather than
a reading.

The reading is deterministic: regular expressions over the flattened PDF
text, with the sentence each field came from kept beside it. That is the
right amount of interpretation for a silver table - a reader can check every
value against its excerpt - and it is what makes the table reproducible from
the bronze snapshot without a model in the loop. What it does not do is
understand: a count it cannot pattern is left null with a note, never
guessed. The fields are a *harvest*, not a summary.

Two grains meet here:

* a **minute** is cut into its agenda items - the numbered headings the
  procès-verbal repeats from the ordre du jour - and only the items about
  planning are kept: a resolution about a crosswalk is not a planning item;
* every other document on the trail (the sommaire, a resolution extract, the
  consultation report, the fiche) is one item, since each is filed about one
  decision.

Deliberately free of Dagster and of pandas: `read_items` takes text and
returns dataclasses, which is what the tests exercise.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import date

from urban_rag.quebec_council import parse_french_date

# ---------------------------------------------------------------------------
# vocabulary
# ---------------------------------------------------------------------------

#: The `item_kind` column: what the item is about, decided by the first group
#: of patterns that matches, in this order. An amendment that also mentions
#: dwellings is an amendment; a bare mention of housing is `housing`.
ITEM_KINDS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "zoning_amendment",
        re.compile(
            r"r[èe]glement modifiant le r[èe]glement .{0,80}urbanisme"
            r"|modifications? (?:à|a|de) la r[ée]glementation d[’']urbanisme"
            r"|modification r[ée]glementaire"
            r"|grille de sp[ée]cifications"
            r"|\b\d{5}\s?[A-Z][a-z]\b"
            r"|\bzonage\b",
            re.I,
        ),
    ),
    ("ppcmoi", re.compile(r"\bPPCMOI\b|projet particulier", re.I)),
    ("minor_variance", re.compile(r"d[ée]rogation mineure", re.I)),
    ("demolition", re.compile(r"\bd[ée]moli(?:tion|r|s|e|es|t)?\b", re.I)),
    ("conditional_use", re.compile(r"usage conditionnel", re.I)),
    (
        "planning",
        re.compile(
            r"plan d[’']urbanisme|\bPPU\b|programme particulier|vision de l[’']habitation"
            r"|plan de mise en [œo]uvre|densit[ée]|\bam[ée]nagement du territoire",
            re.I,
        ),
    ),
    ("heritage", re.compile(r"patrimoni|patrimoine|\bCUCQ\b", re.I)),
    ("housing", re.compile(r"\blogements?\b|\bhabitations?\b|\bimmeuble", re.I)),
)

#: The kinds worth a row. Everything else an agenda covers - snow removal,
#: the treasury, the pool's hours - is counted and dropped.
PLANNING_KINDS: frozenset[str] = frozenset(kind for kind, _ in ITEM_KINDS)

_ZONE_CODE = re.compile(r"\b(\d{5})\s?([A-Z][a-z])\b")
_BYLAW_RCA = re.compile(r"R\.?\s?C\.?\s?A\.?\s?(\d)\s?V\s?\.?\s?Q\.?\s?(\d+)")
_BYLAW_RVQ = re.compile(r"\bR\.?\s?V\.?\s?Q\.?\s?(\d+)\b")
_GPD_NUMBER = re.compile(r"\b(?:[A-Z]{2}\d{4}-\d{3,4}|[A-Z]{2}\d-\d{4}-\d{4})\b")
_FILE_NUMBER = re.compile(r"N[°o]\s*de\s*dossier\s*:?\s*(\d+)", re.I)
_LOT_NUMBER = re.compile(
    r"\blots?\s+(?:n(?:um[ée]ro)?s?\.?\s*|no\.?\s*)?(\d(?:[  ]?\d{3}){2})\b", re.I
)
_STREET_TYPES = (
    r"rue|avenue|boulevard|chemin|c[ôo]te|place|carr[ée]|mont[ée]e|route|promenade"
    r"|grande all[ée]e|all[ée]e|impasse|terrasse|parc|autoroute|rang"
)
#: A civic address: number, street type, a capitalised name of up to four
#: words, an optional cardinal. Horizontal space only - a line break ends it.
_ADDRESS = re.compile(
    r"\b(\d{1,5}(?:[ ]?[A-Z]\b)?(?:[ ]?(?:à|-|et)[ ]?\d{1,5})?)(?:,[ \n]+|[ ]+)"
    rf"({_STREET_TYPES})[ ]+"
    r"((?:de[ ]|des[ ]|du[ ]|d[’']|de[ ]la[ ]|de[ ]l[’']|Sainte?-|Saint-)?[A-ZÉÈÀÎÔ][\w’'\-\.]+"
    r"(?:[ ](?:de|des|du|d[’']|la|le|les|et)[ ][A-ZÉÈÀ][\w’'\-\.]+|[ ][A-ZÉÈÀÎÔ][\w’'\-\.]+){0,3})"
    r"(?:[ ]+(Ouest|Est|Nord|Sud|ouest|est|nord|sud))?",
)
#: Where the zone an item is *about* is named, as opposed to every zone the
#: annexed plan extract labels: the by-law's title, the fiche's "zone visée",
#: the description's "dans la zone".
_SUBJECT_ZONE = re.compile(
    r"(?:relativement\s+(?:à|aux)\s+(?:la\s+|les\s+)?zones?|zones?\s+vis[ée]es?\s*:?|dans\s+la\s+zone|de\s+la\s+zone"
    r"|(?:à|a)\s+la\s+zone|le\s+secteur|à\s+l[’']égard\s+de\s+la\s+zone)\s+"
    r"((?:\d{5}\s?[A-Z][a-z](?:\s*(?:,|et)\s*)?)+)",
    re.I,
)
_USAGE_GROUPS = re.compile(
    r"groupes?\s+d[’']usages?\s+((?:[HCIPRAM]\d{1,2}(?:\s*[,/]\s*|\s+et\s+|\s+)?)+)", re.I
)
_USAGE_CODE = re.compile(r"\b([HCIPRAM]\d{1,2})\b")

_NUMBER_WORDS = {
    "un": 1,
    "une": 1,
    "deux": 2,
    "trois": 3,
    "quatre": 4,
    "cinq": 5,
    "six": 6,
    "sept": 7,
    "huit": 8,
    "neuf": 9,
    "dix": 10,
    "onze": 11,
    "douze": 12,
    "quinze": 15,
    "vingt": 20,
    "trente": 30,
    "quarante": 40,
    "cinquante": 50,
}
_NUM = r"(\d{1,4}|" + "|".join(_NUMBER_WORDS) + r")"

#: "de 8 logements à 10 logements", "de huit à dix le nombre maximum de
#: logements", "de 2 à 10" after "nombre de logements".
_DWELLING_RANGE = re.compile(
    rf"\bde\s+{_NUM}\s+(?:logements?\s+)?[àa]\s+{_NUM}\b(?:\s+logements?)?", re.I
)
#: "limite le nombre maximal de logements à 8 alors que le projet en propose 10"
_DWELLING_LIMIT_VS_PROJECT = re.compile(
    rf"nombre\s+maxim(?:al|um)\s+de\s+logements\s+[àa]\s+{_NUM}\s+alors\s+que\s+le\s+projet\s+en\s+propose\s+{_NUM}",
    re.I,
)
#: "un maximum de 10 logements par bâtiment"
_DWELLING_MAX = re.compile(
    rf"maxim(?:al|um)\s+de\s+{_NUM}\s+logements(?:\s+par\s+b[âa]timent)?", re.I
)
#: "un total de 10 logements", "y aménager 10 logements", "10 logements (4 X 5½"
_DWELLING_COUNT = re.compile(rf"\b{_NUM}\s+logements?\b", re.I)
_DWELLING_CONTEXT = re.compile(r"maxim|autoris|permis|limite|r[èe]glement|norme", re.I)

_HEIGHT_M = re.compile(
    r"(?:hauteur\s+(?:maximale\s+)?(?:permise\s*,?\s+)?(?:qui\s+est\s+)?(?:de\s+)?(\d{1,3}(?:[,.]\d)?)\s?m(?:[èe]tres?)?\b"
    r"|(\d{1,3}(?:[,.]\d)?)\s?m(?:[èe]tres?)?\s+de\s+hauteur)",
    re.I,
)
_STOREYS = re.compile(rf"\b{_NUM}\s+[ée]tages?\b", re.I)

#: The `decision` column, first match wins: the extract's operative words
#: come before the sommaire's recommendation, which lists every stage.
_DECISIONS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("adopted", re.compile(r"il est r[ée]solu\s+d[’']adopter\s+le\s+r[èe]glement", re.I)),
    ("draft_adopted", re.compile(r"il est r[ée]solu\s+d[’']adopter\s+le\s+projet", re.I)),
    ("notice_of_motion", re.compile(r"avis de motion", re.I)),
    ("adopted", re.compile(r"^\s*adoption du r[èe]glement", re.I | re.M)),
    ("draft_adopted", re.compile(r"adopter le projet du r[èe]glement", re.I)),
    ("consultation", re.compile(r"consultation publique|demande d[’']opinion", re.I)),
)

#: The `council_opinion` column, read off the sentence where the council
#: speaks: "le conseil de quartier ... est d'accord", "recommande ... de ne
#: pas approuver", "avec les modifications suivantes".
_OPINION_SENTENCE = re.compile(
    r"(?:le\s+)?conseil\s+de\s+quartier(?:\s+de)?\s+[\w’'\-–\s]{0,60}?"
    r"(?:est\s+d[’']accord|recommande|s[’']oppose|appuie|refuse|se\s+prononce|"
    r"est\s+(?:dé|de)favorable|n[’']est\s+pas)[^.;]{0,400}",
    re.I,
)
_OPINION_UNFAVORABLE = re.compile(
    r"n[’']est\s+pas\s+d[’']accord|s[’']oppose|d[ée]favorable|de\s+ne\s+pas\s+approuver|refuser\s+la\s+demande|contre\s+le\s+projet",
    re.I,
)
_OPINION_FAVORABLE = re.compile(
    r"est\s+d[’']accord|favorable|d[’']adopter|d[’']approuver|appuie|accepter\s+la\s+demande",
    re.I,
)
_OPINION_CONDITIONAL = re.compile(
    r"avec\s+(?:les\s+)?(?:modifications?|proposition|ajustement|demande\s+particuli[èe]re|r[ée]serves?|conditions?)"
    r"|recommande\s+par\s+ailleurs|conditionnel",
    re.I,
)

#: A vote table on the consultation report: "C 9 Accepter la demande avec
#: proposition d'ajustement".
_VOTE_ROW = re.compile(r"^\s*(A|B|C|Abstention)\s+(\d+)\s*$", re.M)

# ---------------------------------------------------------------------------
# records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DwellingChange:
    """One "from N to M dwellings" the text states, with what it is about."""

    before: int | None
    after: int | None
    scope: str  # 'zoning' - a cap in the by-law; 'project' - a building's count
    excerpt: str


@dataclass
class PlanningItem:
    """One planning matter as one document states it."""

    item_index: int
    item_kind: str
    title: str | None
    text: str
    subject_zone_codes: list[str] = field(default_factory=list)
    zone_codes: list[str] = field(default_factory=list)
    bylaw_numbers: list[str] = field(default_factory=list)
    gpd_numbers: list[str] = field(default_factory=list)
    file_number: str | None = None
    subject_addresses: list[str] = field(default_factory=list)
    addresses: list[str] = field(default_factory=list)
    lot_numbers: list[str] = field(default_factory=list)
    usage_groups: list[str] = field(default_factory=list)
    dwelling_changes: list[DwellingChange] = field(default_factory=list)
    max_dwellings_before: int | None = None
    max_dwellings_after: int | None = None
    project_dwellings: int | None = None
    dwelling_counts: list[int] = field(default_factory=list)
    max_height_m: float | None = None
    storeys: list[int] = field(default_factory=list)
    decision: str | None = None
    decision_date: date | None = None
    council_opinion: str | None = None
    council_opinion_excerpt: str | None = None
    votes: dict[str, int] = field(default_factory=dict)
    parse_notes: list[str] = field(default_factory=list)

    def as_row(self) -> dict:
        row = asdict(self)
        row["dwelling_changes"] = [asdict(change) for change in self.dwelling_changes]
        return row


# ---------------------------------------------------------------------------
# text repair
# ---------------------------------------------------------------------------

_SPLIT_HYPHEN = re.compile(r"(\w) ?[-‑] ?(\w)")
_SPLIT_APOSTROPHE = re.compile(r"(\w) ?[’'] ?(\w)")
_HSPACE = re.compile(r"[ \t ]+")


def repair_text(text: str) -> str:
    """Undo what the PDF export did to words: "René -Lévesque" back to
    "René-Lévesque", "d’ urbanisme" to "d’urbanisme", runs of spaces to one.
    Line breaks are kept - the agenda headings are found by line."""
    # A NUL is an extraction artefact too, and Postgres refuses a text with
    # one in it - the first Montcalm run carried one in a trail document.
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    text = _HSPACE.sub(" ", text)
    text = _SPLIT_HYPHEN.sub(r"\1-\2", text)
    text = _SPLIT_APOSTROPHE.sub(r"\1’\2", text)
    lines = [line.strip() for line in text.split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


# ---------------------------------------------------------------------------
# a minute's agenda
# ---------------------------------------------------------------------------

#: "4. Consultation publique", "3.Suivis", "8- Période", "• 8. Période": a
#: number, a separator, a capitalised title, on a line of its own. A bullet
#: before the number is tolerated because one minute in seven puts one there.
_HEADING = re.compile(r"^[ \t•·\-–]*(\d{1,2})\s*([.)\-–])\s*([A-ZÉÈÀÔÎ][^\n]{2,140})$", re.M)


@dataclass(frozen=True)
class AgendaItem:
    number: int
    title: str
    text: str


def agenda_items(text: str) -> list[AgendaItem]:
    """Cut a minute into its agenda items.

    A procès-verbal prints the ordre du jour first and then the minutes under
    the same numbering, so the headings run 1..n twice. The body is the last
    run that starts at 1; every heading in it whose number is the next one up
    opens an item, and a line that merely starts with a number does not.
    """
    matches = list(_HEADING.finditer(text))
    starts = [i for i, m in enumerate(matches) if int(m.group(1)) == 1]
    if not matches or not starts:
        return []
    # A minute nests numbered lists inside its items - the treasury's five
    # payments, a resolution's three demands - and those restart at 1 too. So
    # every "1." opens a candidate chain, and a chain accepts the next number
    # (or the one after, when a heading was lost to the layout, or the same
    # one again, when the agenda itself repeats a number). The ordre du jour
    # and the body are then two chains of about the same length, and the body
    # is the later one - so among the chains within two headings of the
    # longest, the latest wins. Strictly the longest would pick the ordre du
    # jour whenever the body lost one heading to a bullet or a page break,
    # and file the whole minute under "Levée de l'assemblée".
    chains: list[list[re.Match[str]]] = []
    for start in starts:
        accepted: list[re.Match[str]] = []
        expected = 1
        last_index = -1
        behind = False
        # The agenda numbers with one separator ("4.") and its nested lists
        # mostly with another ("1-", "1)"), so a chain keeps the separator it
        # opened with; a nested "3-" then cannot pass for item 3.
        separator = matches[start].group(2)
        for index, match in enumerate(matches[start:], start=start):
            number = int(match.group(1))
            if match.group(2) != separator:
                continue
            if number == expected or number == expected + 1:
                accepted.append(match)
                expected = number + 1
                last_index = index
            elif number == expected - 1 and accepted and index == last_index + 1:
                # The same number twice in a row is the agenda repeating
                # itself. The same number after a run of skipped headings is
                # the *body's* last item seen from the ordre du jour's chain,
                # and taking it would swallow the body.
                accepted.append(match)
                last_index = index
            elif number > expected:
                # A heading further along than this chain has reached: the
                # chain started inside a nested list and the outer numbering
                # is running ahead of it. It can still pick the outer list up
                # later and grow long, so the mark is what excludes it.
                behind = True
        if not behind:
            chains.append(accepted)
    if not chains:
        return []
    longest = max(len(chain) for chain in chains)
    candidates = [chain for chain in chains if len(chain) >= longest - 2]
    # A nested list that happens to run long - six questions under item 7,
    # then the outer 8..12 - is a candidate too, and it starts *inside*
    # another candidate's span. The ordre du jour and the body do not: one
    # ends before the other begins.
    outermost = [
        chain
        for chain in candidates
        if not any(
            other is not chain and other[0].start() < chain[0].start() < other[-1].start()
            for other in candidates
        )
    ]
    accepted = (outermost or candidates)[-1]
    # The last item runs to the end of the text - unless another chain opens
    # after it, which is the body opening after the ordre du jour when the
    # body's own chain was too broken to be chosen.
    later_starts = [chain[0].start() for chain in chains if chain and chain[0].start() > accepted[-1].start()]
    text_end = min(later_starts) if later_starts else len(text)
    items: list[AgendaItem] = []
    for index, match in enumerate(accepted):
        end = accepted[index + 1].start() if index + 1 < len(accepted) else text_end
        items.append(
            AgendaItem(
                number=int(match.group(1)),
                title=match.group(3).strip().rstrip(".:"),
                text=text[match.start() : end].strip(),
            )
        )
    return items


# ---------------------------------------------------------------------------
# fields
# ---------------------------------------------------------------------------


def classify_item(text: str) -> str | None:
    for kind, pattern in ITEM_KINDS:
        if pattern.search(text):
            return kind
    return None


def zone_codes(text: str) -> list[str]:
    return _unique(f"{digits}{suffix}" for digits, suffix in _ZONE_CODE.findall(text))


def subject_zone_codes(text: str, title: str | None = None) -> list[str]:
    """The zones the item is *about*: those its title names, then those
    named after "relativement à la zone", "zone visée", "dans la zone". The
    annexed plan extract labels every neighbouring zone, and `zone_codes`
    keeps those; this does not."""
    found = zone_codes(title or "")
    for run in _SUBJECT_ZONE.findall(text):
        found.extend(zone_codes(run))
    return _unique(found)


def bylaw_numbers(text: str) -> list[str]:
    found = [f"R.C.A.{arr}V.Q. {num}" for arr, num in _BYLAW_RCA.findall(text)]
    found += [f"R.V.Q. {num}" for num in _BYLAW_RVQ.findall(text)]
    return _unique(found)


def gpd_numbers(text: str) -> list[str]:
    return _unique(_GPD_NUMBER.findall(text))


def lot_numbers(text: str) -> list[str]:
    return _unique(re.sub(r"[  ]", "", digits) for digits in _LOT_NUMBER.findall(text))


def addresses(text: str) -> list[str]:
    found = []
    for match in _ADDRESS.finditer(text):
        number, street_type, name, direction = match.groups()
        name = name.strip().rstrip(".,;:")
        if len(name) < 3:
            continue
        spelled = f"{_HSPACE.sub(' ', number).strip()}, {street_type.lower()} {name}"
        if direction:
            spelled += f" {direction}"
        found.append(spelled)
    return _unique(found)


def usage_groups(text: str) -> list[str]:
    found = []
    for run in _USAGE_GROUPS.findall(text):
        found.extend(_USAGE_CODE.findall(run))
    return _unique(found)


def _number(token: str) -> int | None:
    token = token.lower()
    if token.isdigit():
        return int(token)
    return _NUMBER_WORDS.get(token)


def dwelling_changes(text: str) -> list[DwellingChange]:
    """Every "from N to M" the text states about dwellings, scoped."""
    changes: list[DwellingChange] = []
    for match in _DWELLING_LIMIT_VS_PROJECT.finditer(text):
        changes.append(
            DwellingChange(_number(match.group(1)), _number(match.group(2)), "zoning", _excerpt(text, match))
        )
    for match in _DWELLING_RANGE.finditer(text):
        window = _sentence_around(text, match)
        if not re.search(r"logement", window, re.I):
            continue
        scope = "zoning" if _DWELLING_CONTEXT.search(window) else "project"
        changes.append(
            DwellingChange(_number(match.group(1)), _number(match.group(2)), scope, _excerpt(text, match))
        )
    return changes


def dwelling_counts(text: str) -> list[int]:
    counts = []
    for match in _DWELLING_COUNT.finditer(text):
        value = _number(match.group(1))
        if value is not None:
            counts.append(value)
    return _unique(counts)


def max_height(text: str) -> float | None:
    match = _HEIGHT_M.search(text)
    if not match:
        return None
    value = match.group(1) or match.group(2)
    return float(value.replace(",", "."))


def storeys(text: str) -> list[int]:
    return _unique(v for v in (_number(t) for t in _STOREYS.findall(text)) if v is not None)


def decision(text: str) -> str | None:
    for name, pattern in _DECISIONS:
        if pattern.search(text):
            return name
    return None


def decision_date(text: str) -> date | None:
    """The date the document states for what it records: the sitting on a
    resolution extract ("tenue le lundi 7 juillet 2025"), the assembly on a
    consultation report ("Date et heure / Le 16 juin 2025"), the "Date :"
    line on a sommaire, else nothing."""
    match = re.search(r"tenue\s+le\s+([^\n,]+)", text, re.I)
    if match:
        return parse_french_date(match.group(1))
    match = re.search(r"Date\s+et\s+heure\s*\n?\s*(?:Le\s+)?([^\n]+)", text, re.I)
    if match:
        return parse_french_date(match.group(1))
    # A sommaire's header is a form: the PDF prints the value either after
    # the label ("Date : 30 Avril 2025") or, column order being what it is,
    # before it ("30 Avril 2025Date :").
    # No word boundary before "Date": the two columns are glued
    # ("2025Date :") when the PDF is flattened.
    for match in re.finditer(r"([^\n]*?)Date\s*:\s*([^\n]*)", text):
        found = parse_french_date(match.group(2)) or parse_french_date(match.group(1))
        if found:
            return found
    return None


def council_opinion(text: str) -> tuple[str | None, str | None]:
    """``(opinion, excerpt)``: favorable, favorable_with_conditions,
    unfavorable, or None when the council does not speak in this text."""
    for match in _OPINION_SENTENCE.finditer(text.replace("\n", " ")):
        sentence = _HSPACE.sub(" ", match.group()).strip()
        if _OPINION_UNFAVORABLE.search(sentence):
            return "unfavorable", sentence
        if _OPINION_FAVORABLE.search(sentence):
            if _OPINION_CONDITIONAL.search(sentence):
                return "favorable_with_conditions", sentence
            return "favorable", sentence
    return None, None


def votes(text: str) -> dict[str, int]:
    """The consultation report's vote table, keyed A/B/C/Abstention."""
    return {option: int(count) for option, count in _VOTE_ROW.findall(text)}


def _sentence_around(text: str, match: re.Match[str], limit: int = 240) -> str:
    """The sentence a match sits in: back to the previous full stop or
    semicolon and forward to the next, at most ``limit`` characters each
    way. What scopes a "de 8 à 10" is its own sentence - the cap the
    previous one states is not about it."""
    start = max(0, match.start() - limit)
    before = text[start : match.start()]
    boundary = max(before.rfind(". "), before.rfind(";"), before.rfind(".\n"))
    if boundary >= 0:
        before = before[boundary + 1 :]
    after = text[match.end() : match.end() + limit]
    cut = re.search(r"[.;](?:\s|$)", after)
    if cut:
        after = after[: cut.start()]
    return before + match.group() + after


def _excerpt(text: str, match: re.Match[str], margin: int = 90) -> str:
    start = max(0, match.start() - margin)
    end = min(len(text), match.end() + margin)
    return _HSPACE.sub(" ", text[start:end].replace("\n", " ")).strip()


def _unique(values) -> list:
    return list(dict.fromkeys(values))


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------


def read_item(text: str, *, item_index: int, title: str | None, kind: str | None = None) -> PlanningItem:
    """Every field harvested from one item's text."""
    text = repair_text(text)
    item = PlanningItem(
        item_index=item_index,
        item_kind=kind or classify_item(text) or "other",
        title=title,
        text=text,
    )
    item.zone_codes = zone_codes(text)
    item.subject_zone_codes = subject_zone_codes(text, title)
    item.bylaw_numbers = bylaw_numbers(text)
    item.gpd_numbers = gpd_numbers(text)
    file_match = _FILE_NUMBER.search(text)
    item.file_number = file_match.group(1) if file_match else None
    item.addresses = addresses(text)
    # The address the item is about is the one its title carries, else the
    # first one the text names; the assembly's venue and the council chamber
    # are named too, and an extract names the chamber before the title.
    item.subject_addresses = addresses(title or "") or item.addresses[:1]
    item.lot_numbers = lot_numbers(text)
    item.usage_groups = usage_groups(text)
    item.dwelling_changes = dwelling_changes(text)
    item.dwelling_counts = dwelling_counts(text)
    item.max_height_m = max_height(text)
    item.storeys = storeys(text)
    item.decision = decision(text)
    item.decision_date = decision_date(text)
    item.council_opinion, item.council_opinion_excerpt = council_opinion(text)
    item.votes = votes(text)

    zoning = [c for c in item.dwelling_changes if c.scope == "zoning"]
    if zoning:
        item.max_dwellings_before = zoning[0].before
        item.max_dwellings_after = zoning[0].after
    else:
        cap = _DWELLING_MAX.search(text)
        if cap:
            item.max_dwellings_after = _number(cap.group(1))
    project = [c for c in item.dwelling_changes if c.scope == "project"]
    if project:
        item.project_dwellings = project[0].after
    elif (total := re.search(rf"total\s+de\s+{_NUM}\s+logements", text, re.I)):
        item.project_dwellings = _number(total.group(1))

    if item.zone_codes and item.max_dwellings_after is None and re.search(r"logement", text, re.I):
        item.parse_notes.append("dwellings mentioned but no cap read")
    if not item.zone_codes and item.item_kind == "zoning_amendment":
        item.parse_notes.append("amendment names no zone code")
    return item


def read_minutes(text: str) -> tuple[list[PlanningItem], int]:
    """``(planning items, agenda items dropped)`` for one minute. An item is
    kept when its text matches one of `ITEM_KINDS`; a minute whose agenda
    cannot be cut is read as one item, so nothing is lost to the layout."""
    text = repair_text(text)
    items = agenda_items(text)
    if not items:
        kind = classify_item(text)
        if kind is None:
            return [], 0
        return [read_item(text, item_index=0, title=None, kind=kind)], 0

    kept: list[PlanningItem] = []
    dropped = 0
    for agenda in items:
        kind = classify_item(agenda.text)
        if kind is None:
            dropped += 1
            continue
        kept.append(read_item(agenda.text, item_index=agenda.number, title=agenda.title, kind=kind))
    return kept, dropped


def read_document(text: str, *, title: str | None = None) -> PlanningItem:
    """One trail document - sommaire, extract, report, fiche - as one item."""
    text = repair_text(text)
    if title is None:
        title = _first_title(text)
    return read_item(text, item_index=0, title=title)


def _first_title(text: str) -> str | None:
    """The sommaire's *Objet*, an extract's resolution line, a report's
    subject, a fiche's heading - the first line that reads like a title."""
    # A sommaire prints its *Objet* over several lines, between the
    # responsible unit's line and the word "Objet".
    match = re.search(r"responsable\n((?:[^\n]+\n){1,5}?)Objet\b", text)
    if match:
        return _HSPACE.sub(" ", match.group(1).replace("\n", " ")).strip()
    match = re.search(r"^(CA\d-\d{4}-\d{4}|AM\d-\d{4}-\d{4})\s+([^\n]+(?:\n[^\n]+){0,3})", text, re.M)
    if match:
        return _HSPACE.sub(" ", match.group(2).replace("\n", " ")).strip()
    for line in text.split("\n"):
        line = line.strip()
        if len(line) >= 15 and not re.match(r"^(RAPPORT|ACTIVIT|Page|GPD|\d)", line):
            return line
    return None


__all__ = [
    "AgendaItem",
    "DwellingChange",
    "ITEM_KINDS",
    "PLANNING_KINDS",
    "PlanningItem",
    "agenda_items",
    "classify_item",
    "read_document",
    "read_item",
    "read_minutes",
    "repair_text",
]
