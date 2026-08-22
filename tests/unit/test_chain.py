"""Tests for the retrieval-to-answer wiring.

The generation model is stubbed. What is under test is the plumbing - that the
retriever's documents reach the prompt, that they survive onto the result for
the caller to cite, and that the passages are numbered the way the system prompt
tells the model to cite them - none of which needs 3 GB of weights to check.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from urban_rag.rag.chain import build_chain, format_documents


@dataclass
class FakeDocument:
    text: str
    metadata: dict


class StubRetriever:
    def __init__(self, documents: list[FakeDocument]) -> None:
        self.documents = documents
        self.queries: list[str] = []

    def get_relevant_documents(self, query: str) -> list[FakeDocument]:
        self.queries.append(query)
        return self.documents


class FakeChatModel:
    """Returns each of `responses` in turn, one per call."""

    def __init__(self, responses: list[str]) -> None:
        self._responses = iter(responses)

    def invoke(self, messages: list[dict[str, str]]) -> str:
        return next(self._responses)


def make_document(index: int, text: str = "texte") -> FakeDocument:
    return FakeDocument(
        text=text,
        metadata={
            "url": f"http://x/PP_CA{index}.pdf",
            "doc_id": f"doc{index}",
            "chunk_index": index,
            "similarity": 0.5,
        },
    )


def test_format_documents_numbers_passages_from_one():
    formatted = format_documents([make_document(0, "alpha"), make_document(1, "beta")])

    assert formatted.startswith("[1] PP_CA0.pdf")
    assert "[2] PP_CA1.pdf" in formatted
    assert "alpha" in formatted and "beta" in formatted


def test_format_documents_labels_a_passage_by_filename_not_url():
    formatted = format_documents([make_document(3)])

    # The whole city URL in every header would crowd out the passage itself.
    assert "PP_CA3.pdf" in formatted
    assert "http://" not in formatted


def test_format_documents_counts_extracts_from_one_for_a_reader():
    assert "extrait 1" in format_documents([make_document(0)])


def test_chain_returns_the_answer_alongside_its_sources():
    documents = [make_document(0), make_document(1)]
    chain = build_chain(
        StubRetriever(documents=documents), FakeChatModel(responses=["Oui [1]."])
    )

    result = chain.invoke("Quelles conditions ?")

    assert result["answer"] == "Oui [1]."
    assert result["question"] == "Quelles conditions ?"
    # The documents ride along so a caller can print real URLs rather than
    # trusting the citations in the prose.
    assert result["documents"] == documents
    assert "[1] PP_CA0.pdf" in result["context"]


def test_the_question_reaches_the_retriever_verbatim():
    retriever = StubRetriever(documents=[make_document(0)])
    chain = build_chain(retriever, FakeChatModel(responses=["ok"]))

    chain.invoke("Où sont les écoles ?")

    assert retriever.queries == ["Où sont les écoles ?"]


def test_a_question_with_no_matches_still_produces_a_result():
    chain = build_chain(
        StubRetriever(documents=[]),
        FakeChatModel(responses=["Les extraits ne le disent pas."]),
    )

    result = chain.invoke("Quoi ?")

    assert result["documents"] == []
    assert result["context"] == ""
    assert result["answer"] == "Les extraits ne le disent pas."


@pytest.mark.parametrize("field", ["question", "documents", "context", "answer"])
def test_the_result_carries_every_stage(field):
    chain = build_chain(
        StubRetriever(documents=[make_document(0)]),
        FakeChatModel(responses=["a"]),
    )

    assert field in chain.invoke("q")
