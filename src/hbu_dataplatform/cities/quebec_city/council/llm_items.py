"""A local model's second reading of a planning item the patterns left short.

`items.py` reads a council document by regular expression: every value with
its excerpt, nothing guessed, and a sentence the patterns were not written
for yields nothing. This module is the fallback for that last case. For an
item with a *gap* - an amendment with no cap read where the text speaks of
dwellings, no subject zone, no decision - a local instruct model (Qwen3-14B
as a 4-bit GGUF through llama.cpp, see `hbu_dataplatform.rag.structured`)
is handed the text, the fields the patterns did read, and the gaps, and
answers in JSON constrained to `LlmPlanningItem`'s schema. Every value the
model returns carries the excerpt it was read from, and `validate_against_text`
keeps only what is a verbatim quote of the item: a value whose excerpt is not
in the text is rejected with its reason, not the record.

Nothing here imports Dagster, pandas or the model library: the asset
(`council.assets.council_items_llm`) wires those. `extract` is the whole
loop for one item - gaps, focus, prompt, cache, model, validation, merge -
and the cache key covers the item's text, the schema and the prompt, so a
re-run of a partition costs no model call and a changed schema invalidates
everything.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Callable, Mapping
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hbu_dataplatform.cities.quebec_city.council.items import _ZONE_CODE, repair_text

#: Bumping either invalidates every cached answer: the schema changes what
#: the grammar lets the model say, the prompt changes what it is asked.
SCHEMA_VERSION = "1"
PROMPT_VERSION = "1"

#: The post-trained Qwen3 14B as llama.cpp reads it. There is no
#: "Qwen3-14B-Instruct": Qwen3's released checkpoints are the instruct ones.
DEFAULT_REPO_ID = "Qwen/Qwen3-14B-GGUF"
DEFAULT_FILENAME = "Qwen3-14B-Q4_K_M.gguf"

#: The item kinds worth a second reading. `other`, `heritage` and `planning`
#: are discussion; a cap or a decision is not expected in them.
FALLBACK_KINDS: frozenset[str] = frozenset(
    {"zoning_amendment", "ppcmoi", "conditional_use", "minor_variance", "housing"}
)

#: The words a dwelling-count sentence, a decision or an opinion is built
#: around - the windows `focus_text` keeps.
_FOCUS_WORDS = re.compile(
    r"logement|unit[ée]s? d['’]habitation|zone\b|r[ée]solu|adopt|hauteur|[ée]tage"
    r"|refus|accord|recommand|oppos|favorable|avis de motion|projet de r[èe]glement",
    re.I,
)
_HSPACE = re.compile(r"\s+")


class ChatModel(Protocol):
    def invoke(self, messages: list[dict[str, str]]) -> str: ...


# ---------------------------------------------------------------------------
# the schema
# ---------------------------------------------------------------------------


class ItemKind(str, Enum):
    zoning_amendment = "zoning_amendment"
    ppcmoi = "ppcmoi"
    minor_variance = "minor_variance"
    demolition = "demolition"
    conditional_use = "conditional_use"
    planning = "planning"
    heritage = "heritage"
    housing = "housing"
    other = "other"


class Decision(str, Enum):
    adopted = "adopted"
    draft_adopted = "draft_adopted"
    notice_of_motion = "notice_of_motion"
    consultation = "consultation"
    approved = "approved"
    refused = "refused"


class Opinion(str, Enum):
    favorable = "favorable"
    favorable_with_conditions = "favorable_with_conditions"
    unfavorable = "unfavorable"


class IntFact(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int = Field(ge=0, le=5000)
    excerpt: str = Field(min_length=3, max_length=300)


class FloatFact(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: float = Field(ge=0, le=500)
    excerpt: str = Field(min_length=3, max_length=300)


class DwellingChange(BaseModel):
    model_config = ConfigDict(extra="forbid")
    before: int | None = Field(default=None, ge=0, le=5000)
    after: int | None = Field(default=None, ge=0, le=5000)
    #: 'zoning' - a cap in the by-law; 'project' - a building's own count.
    scope: Literal["zoning", "project"]
    excerpt: str = Field(min_length=3, max_length=300)


class LlmPlanningItem(BaseModel):
    """What the model may say about one item. Small on purpose: addresses,
    lots and by-law numbers are the patterns' to read, and every value here
    is one the patterns are known to miss."""

    model_config = ConfigDict(extra="forbid")

    item_kind: ItemKind | None = None
    subject_zone_codes: list[str] = Field(default_factory=list, max_length=6)
    max_dwellings_before: IntFact | None = None
    max_dwellings_after: IntFact | None = None
    project_dwellings: IntFact | None = None
    dwelling_changes: list[DwellingChange] = Field(default_factory=list, max_length=4)
    max_height_m: FloatFact | None = None
    decision: Decision | None = None
    decision_excerpt: str | None = Field(default=None, max_length=300)
    decision_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    council_opinion: Opinion | None = None
    council_opinion_excerpt: str | None = Field(default=None, max_length=300)


def json_schema() -> dict:
    """The schema the grammar is compiled from."""
    return LlmPlanningItem.model_json_schema()


# ---------------------------------------------------------------------------
# which items, and which part of them
# ---------------------------------------------------------------------------


def _list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value) if value.strip() else []
        except ValueError:
            return [value]
        return list(parsed) if isinstance(parsed, list) else [parsed]
    try:
        return list(value)
    except TypeError:
        return [value]


def _missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return isinstance(value, str) and not value.strip()


def gaps(item: Mapping[str, Any]) -> list[str]:
    """The fields the patterns left empty on an item whose text suggests
    they should not be. Empty means the item needs no second reading."""
    kind = item.get("item_kind")
    if kind not in FALLBACK_KINDS:
        return []
    text = str(item.get("text") or "")
    found: list[str] = []
    if _missing(item.get("max_dwellings_after")) and re.search(r"logement", text, re.I):
        found.append("max_dwellings_after")
    if kind == "zoning_amendment" and not _list(item.get("subject_zone_codes")):
        found.append("subject_zone_codes")
    if _missing(item.get("decision")):
        found.append("decision")
    if _missing(item.get("council_opinion")) and item.get("source_kind") in ("minutes", "consultation_file") \
            and re.search(r"conseil de quartier", text, re.I):
        found.append("council_opinion")
    return found


def focus_text(text: str, *, max_chars: int = 7000, head_chars: int = 2000, window: int = 320) -> str:
    """The part of a long item worth the model's context: its head, then
    windows around the words a cap, a decision or an opinion is stated
    with, in order, marked where text was skipped. A short item is itself."""
    text = repair_text(text)
    if len(text) <= max_chars:
        return text
    spans: list[tuple[int, int]] = [(0, min(head_chars, len(text)))]
    for match in _FOCUS_WORDS.finditer(text):
        start, end = max(0, match.start() - window), min(len(text), match.end() + window)
        if spans and start <= spans[-1][1]:
            spans[-1] = (spans[-1][0], max(spans[-1][1], end))
        else:
            spans.append((start, end))
    pieces: list[str] = []
    used = 0
    for start, end in spans:
        piece = text[start:end]
        if used + len(piece) > max_chars:
            piece = piece[: max(0, max_chars - used)]
        if not piece:
            break
        pieces.append(piece)
        used += len(piece)
        if used >= max_chars:
            break
    return "\n[…]\n".join(pieces)


# ---------------------------------------------------------------------------
# the prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "Tu lis des documents d'urbanisme de la Ville de Québec : procès-verbaux de "
    "conseils de quartier, fiches de consultation, sommaires décisionnels, "
    "résolutions. Tu réponds uniquement par un objet JSON conforme au schéma "
    "imposé. Chaque valeur est accompagnée d'un extrait copié mot pour mot du "
    "texte fourni, sans reformulation. Si le texte ne dit pas une chose, la "
    "valeur est null et la liste est vide. N'invente rien, ne déduis rien.\n"
    "Vocabulaire : max_dwellings_before / max_dwellings_after sont le nombre "
    "maximal de logements par bâtiment que le règlement de zonage autorise, "
    "avant et après la modification ; project_dwellings est le nombre de "
    "logements du projet lui-même. decision : adopted (le règlement est adopté), "
    "draft_adopted (le projet de règlement est adopté), notice_of_motion (avis de "
    "motion), consultation (consultation publique ou demande d'opinion), approved "
    "(une demande est accordée), refused (une demande est refusée). council_opinion "
    "est l'avis du conseil de quartier seulement : favorable, "
    "favorable_with_conditions, unfavorable. Les codes de zone ont la forme "
    "14040Hb."
)

#: The fields of a regex record the model is shown, in the order shown.
KNOWN_FIELDS: tuple[str, ...] = (
    "item_kind", "title", "subject_zone_codes", "bylaw_numbers", "subject_addresses",
    "lot_numbers", "max_dwellings_before", "max_dwellings_after", "project_dwellings",
    "max_height_m", "decision", "decision_date", "council_opinion",
)


def build_messages(item: Mapping[str, Any], gap_fields: list[str], focus: str) -> list[dict[str, str]]:
    known = {}
    for name in KNOWN_FIELDS:
        value = item.get(name)
        if _missing(value):
            continue
        known[name] = _list(value) if name in ("subject_zone_codes", "bylaw_numbers", "subject_addresses", "lot_numbers") else value
    user = (
        "Champs déjà lus par un lecteur automatique (à ne pas contredire) :\n"
        f"{json.dumps(known, ensure_ascii=False, default=str)}\n\n"
        f"Champs à compléter : {', '.join(gap_fields) if gap_fields else 'tous ceux que le texte permet'}\n\n"
        "Texte :\n<<<\n"
        f"{focus}\n"
        ">>>\n/no_think"
    )
    return [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}]


# ---------------------------------------------------------------------------
# reading the answer
# ---------------------------------------------------------------------------


def parse_response(text: str) -> tuple[LlmPlanningItem | None, str | None]:
    """The model's answer as the schema, or why it is not."""
    body = text.strip()
    body = re.sub(r"^<think>.*?</think>\s*", "", body, flags=re.S)
    body = re.sub(r"^```(?:json)?\s*|\s*```$", "", body.strip())
    start, end = body.find("{"), body.rfind("}")
    if start < 0 or end < 0:
        return None, "no JSON object in the answer"
    try:
        return LlmPlanningItem.model_validate_json(body[start : end + 1]), None
    except ValidationError as exc:
        return None, f"schema: {exc.errors()[0].get('msg', 'invalid')} at {'.'.join(str(p) for p in exc.errors()[0].get('loc', ()))}"
    except ValueError as exc:
        return None, f"json: {exc}"


def _norm(text: str) -> str:
    return _HSPACE.sub(" ", text.replace("’", "'").replace("‐", "-").replace("‑", "-")).strip().lower()


def validate_against_text(
    record: LlmPlanningItem, text: str, *, meeting_year: int | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Keep what is a quote of the text; reject the rest with its reason.

    Returns ``(fields, rejected)``: `fields` flat, in the items table's
    vocabulary (`max_dwellings_after` an int, `dwelling_changes` a list of
    dicts), each value's excerpt kept under ``<field>_excerpt``.
    """
    haystack = _norm(repair_text(text))
    fields: dict[str, Any] = {}
    rejected: list[dict[str, Any]] = []

    def quoted(excerpt: str | None) -> bool:
        return bool(excerpt) and _norm(excerpt) in haystack

    def take_fact(name: str, fact: IntFact | FloatFact | None) -> None:
        if fact is None:
            return
        if not quoted(fact.excerpt):
            rejected.append({"field": name, "value": fact.value, "excerpt": fact.excerpt, "reason": "excerpt not in text"})
            return
        fields[name] = fact.value
        fields[f"{name}_excerpt"] = fact.excerpt

    if record.item_kind is not None:
        fields["item_kind"] = record.item_kind.value
    zones = []
    for code in record.subject_zone_codes:
        code = code.strip().replace(" ", "")
        if _ZONE_CODE.fullmatch(code) and code.lower() in haystack.replace(" ", ""):
            zones.append(code)
        else:
            rejected.append({"field": "subject_zone_codes", "value": code, "excerpt": None, "reason": "not a zone code of the text"})
    if zones:
        fields["subject_zone_codes"] = zones

    take_fact("max_dwellings_before", record.max_dwellings_before)
    take_fact("max_dwellings_after", record.max_dwellings_after)
    take_fact("project_dwellings", record.project_dwellings)
    take_fact("max_height_m", record.max_height_m)
    if "max_dwellings_before" in fields and "max_dwellings_after" in fields and fields["max_dwellings_before"] == fields["max_dwellings_after"]:
        rejected.append({"field": "max_dwellings_after", "value": fields["max_dwellings_after"], "excerpt": fields.get("max_dwellings_after_excerpt"), "reason": "before equals after"})
        for name in ("max_dwellings_before", "max_dwellings_after"):
            fields.pop(name, None)
            fields.pop(f"{name}_excerpt", None)

    changes = []
    for change in record.dwelling_changes:
        if not quoted(change.excerpt):
            rejected.append({"field": "dwelling_changes", "value": [change.before, change.after], "excerpt": change.excerpt, "reason": "excerpt not in text"})
        elif change.before is not None and change.before == change.after:
            rejected.append({"field": "dwelling_changes", "value": [change.before, change.after], "excerpt": change.excerpt, "reason": "before equals after"})
        else:
            changes.append({"before": change.before, "after": change.after, "scope": change.scope, "excerpt": change.excerpt})
    if changes:
        fields["dwelling_changes"] = changes

    if record.decision is not None:
        if record.decision_excerpt and not quoted(record.decision_excerpt):
            rejected.append({"field": "decision", "value": record.decision.value, "excerpt": record.decision_excerpt, "reason": "excerpt not in text"})
        else:
            fields["decision"] = record.decision.value
            if record.decision_excerpt:
                fields["decision_excerpt"] = record.decision_excerpt
    if record.decision_date is not None:
        year = int(record.decision_date[:4])
        if meeting_year is not None and abs(year - meeting_year) > 2:
            rejected.append({"field": "decision_date", "value": record.decision_date, "excerpt": None, "reason": f"year {year} is not within 2 of the meeting's {meeting_year}"})
        else:
            fields["decision_date"] = record.decision_date
    if record.council_opinion is not None:
        if record.council_opinion_excerpt and not quoted(record.council_opinion_excerpt):
            rejected.append({"field": "council_opinion", "value": record.council_opinion.value, "excerpt": record.council_opinion_excerpt, "reason": "excerpt not in text"})
        else:
            fields["council_opinion"] = record.council_opinion.value
            if record.council_opinion_excerpt:
                fields["council_opinion_excerpt"] = record.council_opinion_excerpt
    return fields, rejected


#: The items-table columns the model may fill. `*_excerpt` siblings ride
#: along inside `llm_fields` and are not columns.
MERGE_FIELDS: tuple[str, ...] = (
    "item_kind", "subject_zone_codes", "max_dwellings_before", "max_dwellings_after",
    "project_dwellings", "dwelling_changes", "max_height_m", "decision", "decision_date",
    "council_opinion", "council_opinion_excerpt",
)


def merge(item: Mapping[str, Any], fields: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """The item with the model's values where the patterns read nothing.

    Regex values are never overwritten - a pattern's value is a quote the
    reader can check, and the model's is a second opinion. Returns the
    merged row and ``{field: 'regex' | 'llm'}`` for every field in play.
    """
    merged = dict(item)
    provenance: dict[str, str] = {}
    for name in MERGE_FIELDS:
        current = item.get(name)
        empty = _missing(current) or (name in ("subject_zone_codes", "dwelling_changes") and not _list(current))
        if not empty:
            provenance[name] = "regex"
            continue
        if name in fields and fields[name] not in (None, [], ""):
            value = fields[name]
            if name in ("subject_zone_codes", "dwelling_changes"):
                value = json.dumps(value, ensure_ascii=False)
            merged[name] = value
            provenance[name] = "llm"
    if "council_opinion" in provenance and provenance["council_opinion"] == "llm" and fields.get("council_opinion_excerpt"):
        merged["council_opinion_excerpt"] = fields["council_opinion_excerpt"]
        provenance["council_opinion_excerpt"] = "llm"
    return merged, provenance


# ---------------------------------------------------------------------------
# the cache, and the loop
# ---------------------------------------------------------------------------


def text_sha256(text: str) -> str:
    return hashlib.sha256(repair_text(text).encode("utf-8")).hexdigest()


def cache_key(doc_id: str, item_index: int, text: str, model_id: str) -> str:
    parts = [str(doc_id), str(item_index), text_sha256(text), SCHEMA_VERSION, PROMPT_VERSION, model_id]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


class LlmCache:
    """One JSON file per answered item, keyed by `cache_key`, outside the
    partition tree the way the PDF cache is: an answer is to a text, not to
    a scrape date."""

    def __init__(self, directory: str | Path | None) -> None:
        self.directory = Path(directory) if directory else None

    def path(self, key: str) -> Path | None:
        return self.directory / key[:2] / f"{key}.json" if self.directory else None

    def get(self, key: str) -> dict | None:
        path = self.path(key)
        if path is None or not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    def put(self, key: str, payload: Mapping[str, Any]) -> None:
        path = self.path(key)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")


def extract(
    item: Mapping[str, Any],
    *,
    model: ChatModel,
    model_id: str,
    cache: LlmCache | None = None,
    force: bool = False,
    max_chars: int = 7000,
    gap_fields: list[str] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """One item through the whole loop. Returns the row of
    `silver.council_item_llm_fields`: status ('filled' | 'nothing_found' |
    'rejected' | 'failed'), fields, provenance, rejected, raw, elapsed_s,
    plus `cached` for the metadata."""
    text = str(item.get("text") or "")
    wanted = gap_fields if gap_fields is not None else gaps(item)
    key = cache_key(str(item["doc_id"]), int(item["item_index"]), text, model_id)
    cached = cache.get(key) if cache is not None and not force else None
    if cached is not None:
        result = dict(cached)
        result["cached"] = True
        return result

    started = clock()
    messages = build_messages(item, wanted, focus_text(text, max_chars=max_chars))
    try:
        raw = model.invoke(messages)
    except Exception as exc:  # a model that dies mid-item is a row, not a run lost
        result = _result(item, key, model_id, wanted, status="failed", raw=None, fields={}, rejected=[{"field": None, "reason": f"model: {exc}"}], elapsed=clock() - started)
        return result
    record, error = parse_response(raw)
    if record is None:
        result = _result(item, key, model_id, wanted, status="failed", raw=raw, fields={}, rejected=[{"field": None, "reason": error}], elapsed=clock() - started)
    else:
        meeting = item.get("meeting_date")
        year = int(str(meeting)[:4]) if meeting and str(meeting)[:4].isdigit() else None
        fields, rejected = validate_against_text(record, text, meeting_year=year)
        _, provenance = merge(item, fields)
        filled = {name for name, source in provenance.items() if source == "llm"}
        status = "filled" if filled else ("rejected" if rejected else "nothing_found")
        result = _result(item, key, model_id, wanted, status=status, raw=raw, fields=fields, rejected=rejected, elapsed=clock() - started, provenance=provenance)
    if cache is not None:
        cache.put(key, result)
    result["cached"] = False
    return result


def _result(item, key, model_id, wanted, *, status, raw, fields, rejected, elapsed, provenance=None) -> dict[str, Any]:
    return {
        "doc_id": str(item["doc_id"]),
        "item_index": int(item["item_index"]),
        "cache_key": key,
        "text_sha256": text_sha256(str(item.get("text") or "")),
        "model": model_id,
        "schema_version": SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION,
        "gaps": list(wanted),
        "status": status,
        "fields": dict(fields),
        "provenance": dict(provenance or {}),
        "rejected": list(rejected),
        "raw": raw,
        "elapsed_s": round(float(elapsed), 2),
    }


__all__ = [
    "DEFAULT_FILENAME",
    "DEFAULT_REPO_ID",
    "FALLBACK_KINDS",
    "LlmCache",
    "LlmPlanningItem",
    "MERGE_FIELDS",
    "PROMPT_VERSION",
    "SCHEMA_VERSION",
    "build_messages",
    "cache_key",
    "extract",
    "focus_text",
    "gaps",
    "json_schema",
    "merge",
    "parse_response",
    "validate_against_text",
]
