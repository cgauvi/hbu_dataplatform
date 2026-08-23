"""A retriever backed by whichever vector store is configured.

Plain, dependency-free interface: `get_relevant_documents(query)` embeds the
question and asks the store for the nearest chunks. The partition filters are
fields rather than call arguments, so binding a neighborhood or a scrape date
means building a retriever per scope, which is cheap since the store's
connection is opened per search.

The store is duck-typed on `search`, so the DuckDB file (`rag.store`) and the
RDS/pgvector table (`rag.pgvector`) are interchangeable here: both return the
same `Hit`, and neither the chain nor the CLI has to know which is answering.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from urban_rag.rag.results import Hit


class Embedder(Protocol):
    """Whatever turns a query string into a vector."""

    def embed_query(self, text: str) -> list[float]: ...


class Searchable(Protocol):
    """Whatever turns a vector into the nearest passages."""

    def search(
        self,
        query_vector: Sequence[float],
        *,
        k: int = ...,
        neighborhood: str | None = ...,
        scrape_date: str | None = ...,
        source_table: str | None = ...,
    ) -> list[Hit]: ...


@dataclass
class VectorStoreRetriever:
    """Embeds the question, then asks the store for the nearest chunks."""

    store: Searchable
    embeddings: Embedder
    k: int = 5
    neighborhood: str | None = None
    scrape_date: str | None = None
    source_table: str | None = None

    def get_relevant_documents(self, query: str) -> list[Hit]:
        return self.store.search(
            self.embeddings.embed_query(query),
            k=self.k,
            neighborhood=self.neighborhood,
            scrape_date=self.scrape_date,
            source_table=self.source_table,
        )


#: What this class was called when DuckDB was the only backend.
DuckDBVSSRetriever = VectorStoreRetriever
