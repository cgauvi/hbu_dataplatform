"""Tests for the retrieval-to-answer wiring.

The generation model is stubbed. What is under test is the plumbing - that the
retriever's documents reach the prompt, that they survive onto the result for
the caller to cite, and that the passages are numbered the way the system prompt
tells the model to cite them - none of which needs 3 GB of weights to check.
"""

from __future__ import annotations

import pytest
from langchain_core.documents import Document
from langchain_core.language_models import FakeListChatModel
from langchain_core.retrievers import BaseRetriever

from urban_rag.rag.chain import build_chain, format_documents


class StubRetriever(BaseRetriever):
    documents: list[Document]
    queries: list[str] = []

    def _get_relevant_documents(self, query, *, run_manager=None):
        self.queries.append(query)
        return self.documents


def make_document(index: int, text: str = "texte") -> Document:
    return Document(
        page_content=text,
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
        StubRetriever(documents=documents), FakeListChatModel(responses=["Oui [1]."])
    )

    result = chain.invoke("Quelles conditions ?")

    assert result["answer"] == "Oui [1]."
    assert result["question"] == "Quelles conditions ?"
    # The documents ride along so a caller can print real URLs rather than
    # trusting the citations in the prose.
    assert result["documents"] == documents
    assert "[1] PP_CA0.pdf" in result["context"]


def test_the_question_reaches_the_retriever_verbatim():
    retriever = StubRetriever(documents=[make_document(0)], queries=[])
    chain = build_chain(retriever, FakeListChatModel(responses=["ok"]))

    chain.invoke("Où sont les écoles ?")

    assert retriever.queries == ["Où sont les écoles ?"]


def test_a_question_with_no_matches_still_produces_a_result():
    chain = build_chain(
        StubRetriever(documents=[], queries=[]),
        FakeListChatModel(responses=["Les extraits ne le disent pas."]),
    )

    result = chain.invoke("Quoi ?")

    assert result["documents"] == []
    assert result["context"] == ""
    assert result["answer"] == "Les extraits ne le disent pas."


@pytest.mark.parametrize("field", ["question", "documents", "context", "answer"])
def test_the_result_carries_every_stage(field):
    chain = build_chain(
        StubRetriever(documents=[make_document(0)]),
        FakeListChatModel(responses=["a"]),
    )

    assert field in chain.invoke("q")
