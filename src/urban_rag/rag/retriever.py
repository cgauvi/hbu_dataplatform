"""A LangChain retriever backed by the DuckDB/VSS store."""

from __future__ import annotations

from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.retrievers import BaseRetriever
from pydantic import ConfigDict

from urban_rag.rag.store import VectorStore


class DuckDBVSSRetriever(BaseRetriever):
    """Embeds the question, then asks DuckDB for the nearest chunks.

    The partition filters are fields rather than call arguments because
    LangChain retrievers take only a query string - binding a neighborhood or a
    scrape date means building a retriever per scope, which is cheap since the
    store connection is opened per search.
    """

    # VectorStore and Embeddings are not pydantic models.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    store: VectorStore
    embeddings: Embeddings
    k: int = 5
    neighborhood: str | None = None
    scrape_date: str | None = None
    source_table: str | None = None

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> list[Document]:
        hits = self.store.search(
            self.embeddings.embed_query(query),
            k=self.k,
            neighborhood=self.neighborhood,
            scrape_date=self.scrape_date,
            source_table=self.source_table,
        )
        return [Document(page_content=hit.text, metadata=hit.metadata) for hit in hits]
