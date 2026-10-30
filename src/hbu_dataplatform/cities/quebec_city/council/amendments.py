"""One row per zoning decision, folded from the planning items; and when each
zone's grid last came into force, read off the grid sheet.

`items.py` reads every *seat* an amendment is described from into one row of
`silver.council_planning_items` - the council's agenda item, the fiche, the
sommaire, the notice of motion, the draft's adoption, the adoption, the
consultation report. The worked case (zone 14040Hb, 8 to 10 dwellings) is
nine rows. "How often has the city raised a cap in a zone like this one"
needs it to be one, and that fold is `consolidate`: the seats that share a
sommaire number, an amending by-law number, or a subject zone at the same
assembly are one decision, and the decision keeps its furthest stage, its
dates, its caps, the council's opinion, and the seats it was folded from.

`bylaw_status` is the other record: Quebec City's grid sheet prints, under
the zone code, the by-law that last amended the grid and its in-force date
("R.C.A.1V.Q. 549 En vigueur le 2025-07-10"). It exists for every zone,
minutes or not, and is what says a zone was touched recently.

Both are pure: rows in, rows out, no database. The asset
(`council.assets.zoning_amendments`) reads the items parquet and the
sheet chunks, calls these, and publishes `gold.zoning_amendments` and
`gold.zone_bylaw_status` (hbu_infra sql/039).
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from typing import Any

from hbu_dataplatform.cities.quebec_city.council.items import OUTCOMES
from hbu_dataplatform.cities.quebec_city.council.items import bylaw_numbers as respell_bylaws

# ---------------------------------------------------------------------------
# vocabulary
# ---------------------------------------------------------------------------

#: The item kinds that are a decision about a zone's or a site's right to
#: build. `planning`, `heritage` and `housing` are discussion, not decisions.
AMENDMENT_KINDS: frozenset[str] = frozenset(
    {"zoning_amendment", "ppcmoi", "conditional_use", "minor_variance"}
)

#: The kinds decided about a *site* rather than a zone: an address is enough
#: to keep one, where a zoning amendment needs a subject zone.
SITE_KINDS: frozenset[str] = frozenset({"ppcmoi", "conditional_use", "minor_variance"})

#: The by-laws being *amended*, which a sommaire cites on every amendment and
#: which must never link two decisions into one: each arrondissement's
#: urbanisme by-law is its R.C.A.nV.Q. 4, R.V.Q. 990 is the city's
#: harmonisation by-law, R.V.Q. 1400 the housing by-law.
KNOWN_BASE_BYLAWS: frozenset[str] = frozenset(
    {"R.V.Q. 990", "R.V.Q. 1400"} | {f"R.C.A.{n}V.Q. 4" for n in range(1, 7)}
)

#: A by-law cited at this many distinct assemblies or more is a base by-law
#: too - an amending by-law is made once, over one spring.
BASE_BYLAW_MEETINGS = 3

#: The decision stages in the order they happen; `refused` ends the trail.
STAGES: Mapping[str, int] = {
    "consultation": 1,
    "notice_of_motion": 2,
    "draft_adopted": 3,
    "approved": 4,
    "adopted": 5,
    "refused": 6,
}

#: Which seat to read a value from when several state one: the consultation
#: report states the proposal plainly, the sommaire states the by-law, the
#: fiche summarises, the minute paraphrases.
SEAT_PREFERENCE: tuple[str, ...] = ("consultation_file", "gpd", "fiche", "council_file", "minutes")

#: The sommaire numbers, as opposed to the resolutions' (CA1-..., AM1-...):
#: a sommaire is the dossier, and the best key a decision has.
_SOMMAIRE = re.compile(r"^[A-Z]{2}\d{4}-\d{3,4}$")

#: "R.C.A.1V.Q. 549 En vigueur le 2025-07-10", as the sheet's text layer
#: prints it - the by-law spaced any way, the date ISO.
_IN_FORCE = re.compile(
    r"(R\.?\s?C\.?\s?A\.?\s?\d\s?V\s?\.?\s?Q\.?\s?\d+|R\.?\s?V\.?\s?Q\.?\s?\d+)"
    r"\s*En vigueur le\s*(\d{4}-\d{2}-\d{2})",
    re.I,
)

LIST_COLUMNS: tuple[str, ...] = (
    "subject_zone_codes",
    "zone_codes",
    "bylaw_numbers",
    "base_bylaw_numbers",
    "gpd_numbers",
    "usage_groups",
    "subject_addresses",
    "addresses",
    "lot_numbers",
    "storeys",
    "source_kinds",
    "citations",
)

AMENDMENT_COLUMNS: tuple[str, ...] = (
    "scrape_date", "neighborhood", "amendment_id", "kind", "title",
    *LIST_COLUMNS[:5], "file_number", *LIST_COLUMNS[5:9],
    "max_dwellings_before", "max_dwellings_after", "project_dwellings",
    "max_height_m", "storeys", "current_max_dwellings", "raises_cap",
    "stage", "outcome", "first_seen", "decision_date", "last_seen",
    "council_opinion", "council_opinion_excerpt", "votes",
    "seats", "source_kinds", "citations", "excerpt",
)

BYLAW_STATUS_COLUMNS: tuple[str, ...] = (
    "scrape_date", "neighborhood", "feature_id", "source_table",
    "bylaw_number", "in_force_date", "excerpt", "doc_id", "url",
)


# ---------------------------------------------------------------------------
# reading a row however it was stored
# ---------------------------------------------------------------------------


def _list(value: Any) -> list:
    """A jsonb array, a parquet JSON string, a list, or nothing - as a list."""
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        try:
            parsed = json.loads(value)
        except ValueError:
            return [value]
        return list(parsed) if isinstance(parsed, list) else [parsed]
    if isinstance(value, dict):
        return [value]
    try:
        return list(value)
    except TypeError:
        return [value]


def _dict(value: Any) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def _date(value: Any) -> date | None:
    """A date, an ISO string, a pandas Timestamp, or nothing."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, date):
        return value
    if hasattr(value, "date") and callable(value.date):
        try:
            return value.date()
        except (TypeError, ValueError):
            return None
    text = str(value).strip()
    if not text or text.lower() in ("nat", "none", "null"):
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _seat(item: Mapping[str, Any]) -> tuple[str, int]:
    return str(item["doc_id"]), int(item["item_index"])


def _minute_ids(item: Mapping[str, Any]) -> list[str]:
    """The minutes this seat belongs to: itself for an agenda item, the
    minutes that led here for a trail document."""
    if _text(item.get("source_kind")) == "minutes":
        return [str(item["doc_id"])]
    return [str(m) for m in _list(item.get("minutes_doc_ids"))]


# ---------------------------------------------------------------------------
# the fold
# ---------------------------------------------------------------------------


def base_bylaws(items: Iterable[Mapping[str, Any]]) -> frozenset[str]:
    """The by-laws being amended rather than made, among those the items cite:
    the known ones, plus any cited at `BASE_BYLAW_MEETINGS` assemblies or more."""
    meetings: dict[str, set] = {}
    for item in items:
        when = _date(item.get("meeting_date"))
        for number in _list(item.get("bylaw_numbers")):
            meetings.setdefault(str(number), set()).add(when)
    spread = {number for number, dates in meetings.items() if len(dates - {None}) >= BASE_BYLAW_MEETINGS}
    return frozenset(KNOWN_BASE_BYLAWS | spread)


class _Union:
    """Union-find over seats, keyed by whatever they share."""

    def __init__(self) -> None:
        self.parent: dict[tuple[str, int], tuple[str, int]] = {}

    def add(self, seat: tuple[str, int]) -> None:
        self.parent.setdefault(seat, seat)

    def find(self, seat: tuple[str, int]) -> tuple[str, int]:
        root = seat
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[seat] != root:
            self.parent[seat], seat = root, self.parent[seat]
        return root

    def union(self, a: tuple[str, int], b: tuple[str, int]) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # The smaller root wins, so the grouping is deterministic.
            self.parent[max(ra, rb)] = min(ra, rb)


def _keys(item: Mapping[str, Any], base: frozenset[str]) -> list[str]:
    """What links this seat to the others of its decision."""
    keys = [f"gpd:{number}" for number in _list(item.get("gpd_numbers"))]
    keys += [f"bylaw:{number}" for number in _list(item.get("bylaw_numbers")) if str(number) not in base]
    zones = [str(z) for z in _list(item.get("subject_zone_codes"))]
    for minute in _minute_ids(item):
        keys += [f"mz:{minute}:{zone}" for zone in zones]
    return keys


def _is_decision(item: Mapping[str, Any]) -> bool:
    kind = _text(item.get("item_kind"))
    if kind not in AMENDMENT_KINDS:
        return False
    if _list(item.get("subject_zone_codes")):
        return True
    return kind in SITE_KINDS and bool(_list(item.get("subject_addresses")) or _list(item.get("lot_numbers")))


def link(items: Sequence[Mapping[str, Any]]) -> list[list[Mapping[str, Any]]]:
    """The decisions among `items`, each as the seats that describe it.

    A seat joins a decision through any of: a sommaire or resolution number
    it cites, an amending by-law number it cites, or a subject zone it names
    at the assembly it belongs to. A set that names no subject zone - and,
    for a site decision, no address or lot - is discussion and is dropped.
    """
    base = base_bylaws(items)
    union = _Union()
    by_key: dict[str, tuple[str, int]] = {}
    seats: dict[tuple[str, int], Mapping[str, Any]] = {}
    for item in items:
        if _text(item.get("item_kind")) not in AMENDMENT_KINDS:
            continue
        seat = _seat(item)
        seats[seat] = item
        union.add(seat)
        for key in _keys(item, base):
            if key in by_key:
                union.union(seat, by_key[key])
            else:
                by_key[key] = seat
    groups: dict[tuple[str, int], list[Mapping[str, Any]]] = {}
    for seat, item in seats.items():
        groups.setdefault(union.find(seat), []).append(item)
    kept = []
    for members in groups.values():
        if any(_is_decision(member) for member in members):
            kept.append(sorted(members, key=_seat))
    return sorted(kept, key=lambda members: _seat(members[0]))


def _preferred(members: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """The seats in the order a value is read from them."""
    rank = {kind: index for index, kind in enumerate(SEAT_PREFERENCE)}
    return sorted(members, key=lambda m: (rank.get(_text(m.get("source_kind")) or "", len(rank)), _seat(m)))


def _union(members: Sequence[Mapping[str, Any]], column: str) -> list:
    found: list = []
    for member in members:
        for value in _list(member.get(column)):
            if value not in found:
                found.append(value)
    return found


def _amendment_id(bylaws: Sequence[str], gpd: Sequence[str], members: Sequence[Mapping[str, Any]]) -> str:
    if bylaws:
        return sorted(bylaws, key=_bylaw_sort)[0]
    sommaires = [n for n in gpd if _SOMMAIRE.match(str(n))]
    if sommaires:
        return sorted(sommaires)[0]
    if gpd:
        return sorted(str(n) for n in gpd)[0]
    doc_id, index = _seat(members[0])
    return f"{doc_id}#{index}"


def _bylaw_sort(number: str) -> tuple[str, int]:
    match = re.search(r"(\d+)$", number)
    return (number.rsplit(" ", 1)[0], int(match.group(1)) if match else 0)


def _cap(members: Sequence[Mapping[str, Any]]) -> tuple[int | None, int | None, int | None]:
    """(before, after, project): the first preferred seat stating an after,
    a seat stating both before and after ahead of one stating after alone."""
    ordered = _preferred(members)
    before = after = project = None
    for member in sorted(ordered, key=lambda m: (_int(m.get("max_dwellings_before")) is None, ordered.index(m))):
        if after is None and _int(member.get("max_dwellings_after")) is not None:
            before = _int(member.get("max_dwellings_before"))
            after = _int(member.get("max_dwellings_after"))
            break
    for member in ordered:
        if _int(member.get("project_dwellings")) is not None:
            project = _int(member.get("project_dwellings"))
            break
    return before, after, project


def _opinion(members: Sequence[Mapping[str, Any]]) -> tuple[str | None, str | None, dict]:
    """The council's opinion and the vote table: the consultation report's
    over the minute's, which paraphrases it."""
    opinion = excerpt = None
    votes: dict = {}
    for member in _preferred(members):
        if opinion is None and _text(member.get("council_opinion")):
            opinion = _text(member.get("council_opinion"))
            excerpt = _text(member.get("council_opinion_excerpt"))
        if not votes and _dict(member.get("votes")):
            votes = _dict(member.get("votes"))
    return opinion, excerpt, votes


def _title(members: Sequence[Mapping[str, Any]]) -> str | None:
    for kind in ("fiche", "consultation_file", "gpd", "minutes", "council_file"):
        for member in members:
            if _text(member.get("source_kind")) == kind and _text(member.get("title")):
                return _text(member.get("title"))
    return next((_text(m.get("title")) for m in members if _text(m.get("title"))), None)


def _stage(members: Sequence[Mapping[str, Any]]) -> tuple[str | None, date | None]:
    """The furthest stage any seat states, and its date - the latest date
    among the seats stating that stage, so an adoption dated by its sitting
    beats the sommaire's draft date."""
    best: str | None = None
    for member in members:
        decision = _text(member.get("decision"))
        if decision in STAGES and (best is None or STAGES[decision] > STAGES[best]):
            best = decision
    if best is None:
        return None, None
    dates = [_date(m.get("decision_date")) for m in members if _text(m.get("decision")) == best]
    dates = [d for d in dates if d is not None]
    if not dates:
        dates = [_date(m.get("meeting_date")) for m in members if _text(m.get("decision")) == best]
        dates = [d for d in dates if d is not None]
    return best, max(dates) if dates else None


def _citation(member: Mapping[str, Any]) -> dict:
    citations = _dict(member.get("citations"))
    when = _date(member.get("meeting_date"))
    return {
        "doc_id": str(member["doc_id"]),
        "item_index": int(member["item_index"]),
        "source_kind": _text(member.get("source_kind")),
        "url": _text(member.get("url")),
        "document_number": _text(member.get("document_number")),
        "meeting_date": when.isoformat() if when else None,
        "decision": _text(member.get("decision")),
        "chunk_ids": [str(c) for c in _list(citations.get("chunk_ids"))],
    }


def consolidate(
    items: Sequence[Mapping[str, Any]],
    *,
    neighborhood: str,
    scrape_date: str,
    current_caps: Mapping[str, float | None] | None = None,
) -> list[dict]:
    """One flat row per decision - the columns of `gold.zoning_amendments`,
    lists and dicts as JSON strings the way the items parquet carries them.

    `current_caps` is ``{zone code: the grid's max_dwellings today}``; it
    decides `raises_cap` where no before was read, and is recorded as
    `current_max_dwellings` beside the answer.
    """
    caps = dict(current_caps or {})
    base = base_bylaws(items)
    rows: list[dict] = []
    for members in link(items):
        zones = [str(z) for z in _union(members, "subject_zone_codes")]
        all_bylaws = [str(b) for b in _union(members, "bylaw_numbers")]
        bylaws = [b for b in all_bylaws if b not in base]
        base_cited = [b for b in all_bylaws if b in base]
        gpd = [str(n) for n in _union(members, "gpd_numbers")]
        before, after, project = _cap(members)
        stage, decided = _stage(members)
        opinion, opinion_excerpt, votes = _opinion(members)
        meetings = [d for d in (_date(m.get("meeting_date")) for m in members) if d]
        all_dates = meetings + [d for d in (_date(m.get("decision_date")) for m in members) if d]
        current = next((_float(caps.get(zone)) for zone in zones if caps.get(zone) is not None), None)
        raises = after is not None and (
            (before is not None and after > before)
            or (before is None and current is not None and after > current)
        )
        kinds = Counter(_text(m.get("item_kind")) for m in members)
        kind = "zoning_amendment" if kinds.get("zoning_amendment") else kinds.most_common(1)[0][0]
        heights = [_float(m.get("max_height_m")) for m in _preferred(members)]
        preferred = _preferred(members)[0]
        rows.append(
            {
                "scrape_date": scrape_date,
                "neighborhood": neighborhood,
                "amendment_id": _amendment_id(bylaws, gpd, members),
                "kind": kind,
                "title": _title(members),
                "subject_zone_codes": zones,
                "zone_codes": [str(z) for z in _union(members, "zone_codes")],
                "bylaw_numbers": bylaws,
                "base_bylaw_numbers": base_cited,
                "gpd_numbers": gpd,
                "file_number": next((_text(m.get("file_number")) for m in members if _text(m.get("file_number"))), None),
                "usage_groups": [str(g) for g in _union(members, "usage_groups")],
                "subject_addresses": [str(a) for a in _union(members, "subject_addresses")],
                "addresses": [str(a) for a in _union(members, "addresses")],
                "lot_numbers": [str(n) for n in _union(members, "lot_numbers")],
                "max_dwellings_before": before,
                "max_dwellings_after": after,
                "project_dwellings": project,
                "max_height_m": next((h for h in heights if h is not None), None),
                "storeys": sorted({int(s) for s in _union(members, "storeys") if _int(s) is not None}),
                "current_max_dwellings": current,
                "raises_cap": bool(raises),
                "stage": stage,
                "outcome": OUTCOMES.get(stage) if stage else None,
                "first_seen": min(meetings).isoformat() if meetings else None,
                "decision_date": decided.isoformat() if decided else None,
                "last_seen": max(all_dates).isoformat() if all_dates else None,
                "council_opinion": opinion,
                "council_opinion_excerpt": opinion_excerpt,
                "votes": votes,
                "seats": len(members),
                "source_kinds": sorted({_text(m.get("source_kind")) or "" for m in members}),
                "citations": [_citation(m) for m in members],
                "excerpt": (_text(preferred.get("excerpt")) or _text(preferred.get("text")) or "")[:1500] or None,
            }
        )
    for row in rows:
        for column in LIST_COLUMNS:
            row[column] = json.dumps(row[column], ensure_ascii=False)
        row["votes"] = json.dumps(row["votes"], ensure_ascii=False)
    return rows


# ---------------------------------------------------------------------------
# the sheet's own record
# ---------------------------------------------------------------------------


def bylaw_status(
    chunks: Iterable[Mapping[str, Any]],
    *,
    neighborhood: str,
    scrape_date: str,
) -> list[dict]:
    """The by-law and in-force date each zone's sheet prints - one row per
    (zone, by-law), the columns of `gold.zone_bylaw_status`.

    A chunk is a row of silver.document_chunks: `feature_ids` (the zone the
    sheet is for), `text`, `doc_id`, `url`, `source_table`. A sheet that
    prints the line twice yields the row once; a zone whose sheet prints no
    such line yields nothing, which is the sheet's own answer.
    """
    seen: set[tuple[str, str]] = set()
    rows: list[dict] = []
    for chunk in chunks:
        zones = [str(z) for z in _list(chunk.get("feature_ids"))]
        text = _text(chunk.get("text")) or ""
        if not zones or not text:
            continue
        for match in _IN_FORCE.finditer(text):
            numbers = respell_bylaws(match.group(1))
            if not numbers:
                continue
            number = numbers[0]
            when = _date(match.group(2))
            for zone in zones:
                if (zone, number) in seen:
                    continue
                seen.add((zone, number))
                rows.append(
                    {
                        "scrape_date": scrape_date,
                        "neighborhood": neighborhood,
                        "feature_id": zone,
                        "source_table": _text(chunk.get("source_table")) or "",
                        "bylaw_number": number,
                        "in_force_date": when.isoformat() if when else None,
                        "excerpt": re.sub(r"\s+", " ", match.group(0)).strip(),
                        "doc_id": _text(chunk.get("doc_id")),
                        "url": _text(chunk.get("url")),
                    }
                )
    return rows


__all__ = [
    "AMENDMENT_COLUMNS",
    "AMENDMENT_KINDS",
    "BYLAW_STATUS_COLUMNS",
    "KNOWN_BASE_BYLAWS",
    "STAGES",
    "base_bylaws",
    "bylaw_status",
    "consolidate",
    "link",
]
