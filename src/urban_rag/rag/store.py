"""The vector store: a DuckDB file with an HNSW index from the vss extension.

Building it is a load, not a computation. `document_embeddings` has already
done the expensive part and written `embeddings.parquet`, so indexing reads
those files straight into a table and builds the index over them - no model
loaded, no PDF re-fetched, seconds rather than minutes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb

from urban_rag.rag.vss import connect

#: Bumped when the schema changes in a way an existing file cannot satisfy.
SCHEMA_VERSION = "2"

_HNSW_INDEX = "chunks_embedding_hnsw"

#: Selected in this order by both the load and the search, so `Hit` can be
#: built positionally from a result row.
_COLUMNS = (
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
        """Provenance, shaped for a LangChain ``Document``."""
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


class VectorStore:
    """Embedded chunks, searchable by cosine distance over an HNSW index.

    The embedding column is a fixed-width ``FLOAT[dimension]``, which is what
    vss requires to index it - parquet only records that width in its arrow
    metadata, so the load casts back to it explicitly.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def exists(self) -> bool:
        return self.path.exists()

    def check_writable(self) -> None:
        """Take and release the write lock, raising `StoreLocked` if held.

        Called before a long run so a database left open in the editor is
        reported immediately rather than after the work is done.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connect(self.path).close()

    # -- writing -----------------------------------------------------------

    def build(
        self,
        pattern: str,
        *,
        neighborhood: str | None = None,
        scrape_date: str | None = None,
    ) -> dict[str, object]:
        """Load every `embeddings.parquet` matching `pattern` into the store.

        A full rebuild rather than an append, mirroring the scrape: each
        partition is a whole snapshot, so a resolution that stopped being cited
        upstream should stop being retrievable here too.
        """
        clauses, parameters = _filters(neighborhood, scrape_date)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        source = "read_parquet(?, hive_partitioning = true)"

        self.path.parent.mkdir(parents=True, exist_ok=True)
        with connect(self.path) as connection:
            dimension, model = _describe_source(
                connection, source, where, [pattern, *parameters]
            )

            connection.execute(f"DROP INDEX IF EXISTS {_HNSW_INDEX}")
            connection.execute("DROP TABLE IF EXISTS chunks")
            connection.execute("DROP TABLE IF EXISTS index_meta")
            connection.execute(
                f"""
                CREATE TABLE chunks AS
                SELECT chunk_id,
                       doc_id,
                       url,
                       title,
                       source_table,
                       neighborhood,
                       CAST(scrape_date AS VARCHAR)     AS scrape_date,
                       CAST(chunk_index AS INTEGER)     AS chunk_index,
                       CAST(num_tokens  AS INTEGER)     AS num_tokens,
                       feature_ids,
                       model,
                       text,
                       CAST(embedding AS FLOAT[{dimension}]) AS embedding
                FROM {source}
                {where}
                -- The same resolution is re-embedded on every scrape date it is
                -- still cited on. Keep the newest copy so chunk_id stays unique.
                QUALIFY row_number() OVER (
                    PARTITION BY chunk_id ORDER BY scrape_date DESC
                ) = 1
                """,
                [pattern, *parameters],
            )
            # Built after the load, not before: inserting into a live HNSW index
            # costs far more than building it once over a finished table.
            connection.execute(
                f"CREATE INDEX {_HNSW_INDEX} ON chunks "
                "USING HNSW (embedding) WITH (metric = 'cosine')"
            )
            connection.execute(
                "CREATE TABLE index_meta (key VARCHAR PRIMARY KEY, value VARCHAR)"
            )
            connection.executemany(
                "INSERT INTO index_meta VALUES (?, ?)",
                [
                    ("schema_version", SCHEMA_VERSION),
                    ("embedding_model", model),
                    ("dimension", str(dimension)),
                    ("source_pattern", pattern),
                ],
            )
            chunks, documents = connection.execute(
                "SELECT count(*), count(DISTINCT doc_id) FROM chunks"
            ).fetchone()

        return {
            "chunks": chunks,
            "documents": documents,
            "dimension": dimension,
            "embedding_model": model,
        }

    # -- reading -----------------------------------------------------------

    def search(
        self,
        query_vector: Sequence[float],
        *,
        k: int = 5,
        neighborhood: str | None = None,
        scrape_date: str | None = None,
        source_table: str | None = None,
        doc_id: str | None = None,
    ) -> list[Hit]:
        """The ``k`` passages closest to ``query_vector``, nearest first."""
        clauses, parameters = _filters(neighborhood, scrape_date)
        for column, value in (("source_table", source_table), ("doc_id", doc_id)):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with connect(self.path, read_only=True) as connection:
            dimension = self._dimension(connection)
            if len(query_vector) != dimension:
                raise IndexMismatch(
                    f"{self.path} holds {dimension}-dimensional vectors but the "
                    f"query is {len(query_vector)}-dimensional; the store was "
                    f"built with {self._meta(connection).get('embedding_model')!r}. "
                    "Re-index, or query with the model it was built from."
                )
            rows = connection.execute(
                f"""
                SELECT {', '.join(_COLUMNS)},
                       array_cosine_distance(embedding, ?::FLOAT[{dimension}]) AS distance
                FROM chunks
                {where}
                ORDER BY distance
                LIMIT ?
                """,
                [list(query_vector), *parameters, k],
            ).fetchall()

        return [
            Hit(
                *row[: len(_COLUMNS)],
                # Vectors are L2-normalised upstream, so cosine distance is in
                # [0, 2] and this is exactly the cosine similarity.
                similarity=1.0 - row[len(_COLUMNS)],
            )
            for row in rows
        ]

    def stats(self) -> dict[str, object]:
        """Counts and coverage, for ``urban-rag status``."""
        with connect(self.path, read_only=True) as connection:
            # Read first: on a database this tool did not write, it is the
            # difference between a clear message and a catalog error.
            meta = self._meta(connection)
            chunks, documents = connection.execute(
                "SELECT count(*), count(DISTINCT doc_id) FROM chunks"
            ).fetchone()
            tokens = connection.execute(
                "SELECT min(num_tokens), avg(num_tokens), max(num_tokens) FROM chunks"
            ).fetchone()
            per_table = connection.execute(
                "SELECT source_table, count(DISTINCT doc_id), count(*) FROM chunks "
                "GROUP BY source_table ORDER BY source_table"
            ).fetchall()
            partitions = connection.execute(
                "SELECT neighborhood, scrape_date, count(DISTINCT doc_id), count(*) "
                "FROM chunks GROUP BY neighborhood, scrape_date "
                "ORDER BY neighborhood, scrape_date"
            ).fetchall()
        return {
            "chunks": chunks,
            "documents": documents,
            "tokens_min": tokens[0],
            "tokens_mean": tokens[1],
            "tokens_max": tokens[2],
            "per_table": per_table,
            "partitions": partitions,
            **meta,
        }

    @staticmethod
    def _meta(connection: duckdb.DuckDBPyConnection) -> dict[str, str]:
        try:
            return dict(
                connection.execute("SELECT key, value FROM index_meta").fetchall()
            )
        except duckdb.Error as exc:
            raise IndexMismatch(
                "this database has no index_meta table - it was not written by "
                "`urban-rag index`. Re-index, or point --store elsewhere."
            ) from exc

    @classmethod
    def _dimension(cls, connection: duckdb.DuckDBPyConnection) -> int:
        value = cls._meta(connection).get("dimension")
        if value is None:
            raise IndexMismatch("store records no dimension; re-run `urban-rag index`")
        return int(value)


def _filters(
    neighborhood: str | None, scrape_date: str | None
) -> tuple[list[str], list[object]]:
    clauses: list[str] = []
    parameters: list[object] = []
    if neighborhood is not None:
        clauses.append("neighborhood = ?")
        parameters.append(neighborhood)
    if scrape_date is not None:
        clauses.append("CAST(scrape_date AS VARCHAR) = ?")
        parameters.append(scrape_date)
    return clauses, parameters


def _describe_source(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    where: str,
    parameters: list[object],
) -> tuple[int, str]:
    """Vector width and model of the parquet about to be loaded.

    Both are checked rather than assumed: an HNSW index needs one fixed width,
    and vectors from two different models share no space, so a partition
    re-embedded with a new model must not be silently mixed into an old index.
    """
    try:
        rows = connection.execute(
            f"SELECT DISTINCT len(embedding), model FROM {source} {where}", parameters
        ).fetchall()
    except duckdb.Error as exc:
        raise IndexMismatch(
            f"No readable embeddings at {parameters[0]!r}: {str(exc).splitlines()[0]}\n"
            "Materialize `document_embeddings` first (`make corpus`)."
        ) from exc

    if not rows:
        raise IndexMismatch(
            f"No embedded chunks matched {parameters[0]!r} with those filters."
        )
    widths = {row[0] for row in rows}
    models = {row[1] for row in rows}
    if len(widths) > 1 or len(models) > 1:
        raise IndexMismatch(
            f"Mixed embeddings: widths {sorted(widths)}, models {sorted(models)}. "
            "Re-materialize `document_embeddings` so every partition uses one model."
        )
    return int(widths.pop()), str(models.pop())
