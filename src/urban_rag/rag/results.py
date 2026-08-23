"""What a retrieval returns, and the shape both stores agree to return it in.

`rag.store` (DuckDB) and `rag.pgvector` (Postgres) hold the same corpus and are
queried the same way, so the row they hand back, the columns they select it
from, and the error they raise when a store cannot answer the query live here
rather than in either backend - a Postgres-only deployment should not have to
import duckdb to describe a hit.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Bumped when the schema changes in a way an existing store cannot satisfy.
SCHEMA_VERSION = "2"

#: Selected in this order by every load and every search, so `Hit` can be built
#: positionally from a result row.
COLUMNS = (
    "chunk_id",
    "doc_id",
    "url",
    "title",
    "source_table",
    "neighborhood",
    "scrape_date",
    "chunk_index",
    "num_tokens",
    "feature_ids",
    "model",
    "text",
)


class IndexMismatch(RuntimeError):
    """The store, or the parquet behind it, is not internally consistent."""


@dataclass(frozen=True)
class Hit:
    """One retrieved passage and how close it was to the query."""

    chunk_id: str
    doc_id: str
    url: str
    title: str | None
    source_table: str
    neighborhood: str
    scrape_date: str
    chunk_index: int
    num_tokens: int
    feature_ids: str
    model: str
    text: str
    similarity: float

    @property
    def metadata(self) -> dict:
        """Provenance, shaped for the chain's citation formatting."""
        return {
            "chunk_id": self.chunk_id,
            "doc_id": self.doc_id,
            "url": self.url,
            "title": self.title,
            "source_table": self.source_table,
            "neighborhood": self.neighborhood,
            "scrape_date": self.scrape_date,
            "chunk_index": self.chunk_index,
            "num_tokens": self.num_tokens,
            "feature_ids": self.feature_ids,
            "model": self.model,
            "similarity": self.similarity,
        }
