"""The model's second reading of a planning item, with the model faked.

What is checked: which items have a gap and which do not; that the prompt
carries the regex record, the gaps and `/no_think`; that a value whose
excerpt is not in the text is rejected with its reason, and a value whose
excerpt is kept with it; that regex values are never overwritten and the
provenance says who read what; that the cache answers a second call without
the model and a schema bump invalidates it; that a model that dies or
answers nonsense is a row, not a lost run.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hbu_dataplatform.cities.quebec_city.council import llm_items
from hbu_dataplatform.cities.quebec_city.council.items import read_document, read_minutes
from hbu_dataplatform.cities.quebec_city.council.llm_items import (
    LlmCache,
    build_messages,
    cache_key,
    extract,
    focus_text,
    gaps,
    json_schema,
    merge,
    parse_response,
    validate_against_text,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "council"


class FakeChatModel:
    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)
        self.calls: list[list[dict[str, str]]] = []

    def invoke(self, messages):
        self.calls.append(messages)
        if not self._responses:
            raise RuntimeError("out of responses")
        return self._responses.pop(0)


def _item(**over) -> dict:
    base = {
        "doc_id": "m1", "item_index": 4, "source_kind": "minutes", "meeting_date": "2025-06-16",
        "item_kind": "zoning_amendment", "title": "Consultation publique",
        "subject_zone_codes": "[]", "bylaw_numbers": "[]", "subject_addresses": "[]", "lot_numbers": "[]",
        "max_dwellings_before": None, "max_dwellings_after": None, "project_dwellings": None,
        "dwelling_changes": "[]", "max_height_m": None, "decision": None, "decision_date": None,
        "council_opinion": None, "council_opinion_excerpt": None,
        "text": (
            "Consultation publique et demande d'opinion. Le règlement vise la zone 14040Hb. "
            "Le propriétaire souhaite porter le nombre maximal de logements par bâtiment de huit à dix. "
            "Le conseil de quartier est d'accord avec la demande."
        ),
    }
    base.update(over)
    return base


@pytest.fixture(scope="module")
def minute_items():
    minute = (FIXTURES / "minutes_montcalm_2025-06-16.txt").read_text(encoding="utf-8")
    items, _ = read_minutes(minute)
    return items


# -- which items ------------------------------------------------------------------


def test_the_gaps_are_the_fields_the_text_promises_and_the_patterns_missed():
    assert gaps(_item()) == ["max_dwellings_after", "subject_zone_codes", "decision", "council_opinion"]
    # The patterns read everything: nothing to ask.
    assert gaps(_item(max_dwellings_after=10, subject_zone_codes='["14040Hb"]', decision="consultation", council_opinion="favorable")) == []
    # A text that never says "logement" promises no cap.
    assert "max_dwellings_after" not in gaps(_item(text="Avis de motion relatif à la zone 14040Hb."))
    # Discussion kinds are not read twice.
    assert gaps(_item(item_kind="heritage")) == []
    assert gaps(_item(item_kind="other")) == []


def test_the_worked_sommaire_needs_no_second_reading_and_the_minute_item_does(minute_items):
    sommaire = read_document((FIXTURES / "gpd_GT2025-233.txt").read_text(encoding="utf-8"))
    row = {**sommaire.as_row(), "doc_id": "s", "source_kind": "gpd", "meeting_date": "2025-06-16"}
    assert "max_dwellings_after" not in gaps(row)
    assert "subject_zone_codes" not in gaps(row)
    rows = [{**item.as_row(), "doc_id": "m", "source_kind": "minutes", "meeting_date": "2025-06-16"} for item in minute_items]
    with_gaps = [row["item_index"] for row in rows if gaps(row)]
    assert with_gaps, "the minute's other planning items leave something to ask"


# -- the prompt ---------------------------------------------------------------------


def test_the_prompt_carries_the_regex_record_the_gaps_and_no_think():
    item = _item(subject_zone_codes='["14040Hb"]', max_dwellings_before=8)
    messages = build_messages(item, ["max_dwellings_after", "decision"], focus_text(item["text"]))
    assert messages[0]["role"] == "system" and "JSON" in messages[0]["content"]
    user = messages[1]["content"]
    assert '"subject_zone_codes": ["14040Hb"]' in user
    assert '"max_dwellings_before": 8' in user
    assert "Champs à compléter : max_dwellings_after, decision" in user
    assert user.rstrip().endswith("/no_think")
    assert "de huit à dix" in user


def test_a_long_text_is_focused_on_its_head_and_its_keywords():
    filler = "Lorem ipsum dolor sit amet. " * 400
    text = "Titre du point.\n" + filler + "Le nombre maximal de logements passe de 8 à 10. " + filler + "Il est résolu d'adopter le règlement."
    focus = focus_text(text, max_chars=3000, head_chars=600, window=100)
    assert len(focus) <= 3000 + 20
    assert focus.startswith("Titre du point.")
    assert "passe de 8 à 10" in focus
    # repair_text turns the ASCII apostrophe typographic, so match around it.
    assert "adopter le r" in focus
    assert "[…]" in focus
    # A short text is itself.
    assert focus_text("court texte") == "court texte"


def test_the_schema_is_small_and_closed():
    schema = json_schema()
    assert schema.get("additionalProperties") is False
    assert set(schema["properties"]) >= {"max_dwellings_after", "decision", "subject_zone_codes", "council_opinion"}
    assert "addresses" not in schema["properties"]


# -- reading the answer -------------------------------------------------------------


def _answer(**over) -> str:
    base = {
        "item_kind": "zoning_amendment",
        "subject_zone_codes": ["14040Hb"],
        "max_dwellings_before": {"value": 8, "excerpt": "de huit à dix"},
        "max_dwellings_after": {"value": 10, "excerpt": "de huit à dix"},
        "project_dwellings": None,
        "dwelling_changes": [{"before": 8, "after": 10, "scope": "zoning", "excerpt": "nombre maximal de logements par bâtiment de huit à dix"}],
        "max_height_m": None,
        "decision": "consultation",
        "decision_excerpt": "Consultation publique et demande d'opinion",
        "decision_date": None,
        "council_opinion": "favorable",
        "council_opinion_excerpt": "Le conseil de quartier est d'accord avec la demande",
    }
    base.update(over)
    return json.dumps(base, ensure_ascii=False)


def test_a_quoted_value_is_kept_and_an_unquoted_one_is_rejected_with_its_reason():
    record, error = parse_response(_answer(max_height_m={"value": 13, "excerpt": "treize mètres de hauteur"}))
    assert error is None
    fields, rejected = validate_against_text(record, _item()["text"], meeting_year=2025)
    assert fields["max_dwellings_after"] == 10 and fields["max_dwellings_after_excerpt"] == "de huit à dix"
    assert fields["subject_zone_codes"] == ["14040Hb"]
    assert fields["decision"] == "consultation"
    assert fields["council_opinion"] == "favorable"
    assert fields["dwelling_changes"][0]["scope"] == "zoning"
    assert "max_height_m" not in fields
    assert rejected == [{"field": "max_height_m", "value": 13.0, "excerpt": "treize mètres de hauteur", "reason": "excerpt not in text"}]


def test_a_zone_code_the_text_does_not_carry_is_rejected():
    record, _ = parse_response(_answer(subject_zone_codes=["14040Hb", "99999Zz"]))
    fields, rejected = validate_against_text(record, _item()["text"])
    assert fields["subject_zone_codes"] == ["14040Hb"]
    assert any(r["field"] == "subject_zone_codes" and r["value"] == "99999Zz" for r in rejected)


def test_a_date_far_from_the_meeting_and_an_equal_before_after_are_rejected():
    record, _ = parse_response(_answer(decision_date="2015-01-01", max_dwellings_before={"value": 10, "excerpt": "de huit à dix"}))
    fields, rejected = validate_against_text(record, _item()["text"], meeting_year=2025)
    assert "decision_date" not in fields
    assert "max_dwellings_after" not in fields and "max_dwellings_before" not in fields
    reasons = {r["reason"] for r in rejected}
    assert any("within 2" in r for r in reasons)
    assert "before equals after" in reasons


def test_the_answer_may_wear_a_think_block_or_a_code_fence():
    record, error = parse_response("<think>\n\n</think>\n\n```json\n" + _answer() + "\n```")
    assert error is None and record.max_dwellings_after.value == 10
    record, error = parse_response("Désolé, je ne peux pas.")
    assert record is None and "no JSON" in error
    record, error = parse_response('{"decision": "maybe"}')
    assert record is None and error.startswith("schema:")


# -- the merge ------------------------------------------------------------------------


def test_regex_values_win_and_the_provenance_says_who_read_what():
    item = _item(max_dwellings_before=8, max_dwellings_after=9, subject_zone_codes="[]")
    merged, provenance = merge(item, {"max_dwellings_after": 10, "subject_zone_codes": ["14040Hb"], "decision": "consultation",
                                      "council_opinion": "favorable", "council_opinion_excerpt": "est d'accord"})
    assert merged["max_dwellings_after"] == 9
    assert provenance["max_dwellings_after"] == "regex"
    assert json.loads(merged["subject_zone_codes"]) == ["14040Hb"]
    assert provenance["subject_zone_codes"] == "llm"
    assert merged["decision"] == "consultation" and provenance["decision"] == "llm"
    assert merged["council_opinion_excerpt"] == "est d'accord" and provenance["council_opinion_excerpt"] == "llm"


# -- the loop and the cache -------------------------------------------------------------


def test_extract_fills_caches_and_a_second_call_never_reaches_the_model(tmp_path):
    model = FakeChatModel([_answer()])
    cache = LlmCache(tmp_path)
    result = extract(_item(), model=model, model_id="fake", cache=cache)
    assert result["status"] == "filled"
    assert result["cached"] is False
    assert result["fields"]["max_dwellings_after"] == 10
    assert result["provenance"]["max_dwellings_after"] == "llm"
    assert result["gaps"] == ["max_dwellings_after", "subject_zone_codes", "decision", "council_opinion"]
    assert result["elapsed_s"] >= 0
    assert len(model.calls) == 1

    again = extract(_item(), model=model, model_id="fake", cache=cache)
    assert again["cached"] is True and again["fields"] == result["fields"]
    assert len(model.calls) == 1

    # Another text, another model, another schema: another key.
    key = cache_key("m1", 4, _item()["text"], "fake")
    assert cache_key("m1", 4, _item()["text"] + " x", "fake") != key
    assert cache_key("m1", 4, _item()["text"], "other") != key
    try:
        llm_items.SCHEMA_VERSION = "999"
        assert cache_key("m1", 4, _item()["text"], "fake") != key
    finally:
        llm_items.SCHEMA_VERSION = "1"
    # `force` bypasses the cache.
    model = FakeChatModel([_answer()])
    forced = extract(_item(), model=model, model_id="fake", cache=cache, force=True)
    assert forced["cached"] is False and len(model.calls) == 1


def test_a_model_that_dies_or_babbles_is_a_row_not_a_lost_run(tmp_path):
    dead = extract(_item(), model=FakeChatModel([]), model_id="fake", cache=LlmCache(tmp_path))
    assert dead["status"] == "failed" and "out of responses" in dead["rejected"][0]["reason"]
    babble = extract(_item(), model=FakeChatModel(["pas de JSON ici"]), model_id="fake")
    assert babble["status"] == "failed" and babble["raw"] == "pas de JSON ici"
    nulls = extract(_item(), model=FakeChatModel([json.dumps({"decision": None})]), model_id="fake")
    assert nulls["status"] == "nothing_found" and nulls["fields"] == {}
    wrong = extract(_item(), model=FakeChatModel([_answer(max_dwellings_after={"value": 10, "excerpt": "pas dans le texte"},
                                                           max_dwellings_before=None, subject_zone_codes=[], decision=None,
                                                           dwelling_changes=[], council_opinion=None)]), model_id="fake")
    assert wrong["status"] == "rejected" and wrong["rejected"][0]["reason"] == "excerpt not in text"
