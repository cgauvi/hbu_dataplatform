"""Assets that turn the scraped tables' linked PDFs into an embedded corpus.

Three steps, kept apart so each can be re-run on its own: fetch and flatten
the PDFs, cut them into chunks, embed the chunks. Re-chunking is cheap;
re-embedding is not, and neither should force a re-download.

Output lands beside the scrape, under the same hive keys::

    data/rag/neighborhood=VSMPE/scrape_date=2026-08-18/
        documents.parquet   one row per linked PDF, with its text
        chunks.parquet      one row per chunk
        embeddings.parquet  chunks + a 1024-wide float32 bge-m3 vector
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.assets import neighborhood_features
from urban_rag.rag.documents import (
    DOCUMENT_SOURCES,
    Document,
    DocumentError,
    chunk_document,
    document_urls,
    read_pdf,
)
from urban_rag.frames import write_vectors
from urban_rag.partitions import scrape_partitions
from urban_rag.resources import DocumentStore, EmbeddingModel, GeoParquetStore

GROUP = "rag"

DOCUMENTS_FILE = "documents.parquet"
CHUNKS_FILE = "chunks.parquet"
EMBEDDINGS_FILE = "embeddings.parquet"

#: Attribute columns worth carrying alongside a document, when the source
#: table has them: the resolution number is what a user actually cites.
_ID_COLUMN = "ID"
_TITLE_COLUMNS = ("DESCRIPTION", "NOM_CAT")


@asset(
    partitions_def=scrape_partitions,
    deps=[neighborhood_features],
    group_name=GROUP,
    description=(
        "The PDFs linked from a scraped table's URL column (EN_SAVOIR_PLUS and "
        "friends), downloaded and flattened to text. One row per distinct link."
    ),
)
def linked_documents(
    context: AssetExecutionContext,
    geoparquet: GeoParquetStore,
    documents: DocumentStore,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    source_dir = geoparquet.partition_dir(neighborhood, scrape_date)
    fetcher = documents.fetcher()
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    rows: list[dict] = []
    failures: dict[str, str] = {}
    from_cache = 0

    for slug, url_column in DOCUMENT_SOURCES.items():
        path = source_dir / f"{slug}.parquet"
        if not path.exists():
            context.log.warning("%s: not in this partition, skipped", slug)
            continue

        frame = pd.read_parquet(path, columns=_wanted_columns(path, url_column))
        links = document_urls(frame, url_column)
        context.log.info("%s: %d distinct link(s) in %s", slug, len(links), url_column)
        features = _features_by_url(frame, url_column)

        for url in links:
            try:
                content, cached = fetcher.fetch(url)
                document = read_pdf(url, content)
            except DocumentError as exc:
                # A dead link should cost its own document, not the partition.
                failures[url] = str(exc)
                context.log.warning("%s", exc)
                continue

            from_cache += cached
            rows.append(
                {
                    "doc_id": document.doc_id,
                    "source_table": slug,
                    "url": url,
                    "num_pages": document.num_pages,
                    "num_chars": document.num_chars,
                    "num_bytes": document.num_bytes,
                    "content_sha256": document.content_sha256,
                    "fetched_at": fetched_at,
                    "text": document.text,
                    **features.get(url, {"feature_ids": "[]", "title": None}),
                }
            )

    if not rows:
        raise Failure(
            f"No document could be read for {neighborhood} {scrape_date} "
            f"({len(failures)} failed)."
        )

    frame = pd.DataFrame(rows)
    partition_dir = documents.partition_dir(neighborhood, scrape_date)
    path = _write(frame, partition_dir, DOCUMENTS_FILE)

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_documents": len(frame),
            "num_from_cache": from_cache,
            "num_failed": len(failures),
            "num_pages": int(frame["num_pages"].sum()),
            "num_chars": int(frame["num_chars"].sum()),
            "median_chars": int(frame["num_chars"].median()),
            "output_path": MetadataValue.path(str(path)),
            "preview": MetadataValue.md(
                _preview("First document", frame["text"].iloc[0])
            ),
            **({"failures": MetadataValue.json(failures)} if failures else {}),
        }
    )


@asset(
    partitions_def=scrape_partitions,
    deps=[linked_documents],
    group_name=GROUP,
    description=(
        "Documents cut into overlapping, paragraph-aligned chunks, measured "
        "with the embedding model's own tokenizer."
    ),
)
def document_chunks(
    context: AssetExecutionContext,
    documents: DocumentStore,
    embedding_model: EmbeddingModel,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    partition_dir = documents.partition_dir(neighborhood, scrape_date)
    frame = _read(partition_dir, DOCUMENTS_FILE)
    ruler = embedding_model.ruler()

    rows: list[dict] = []
    for document in frame.itertuples(index=False):
        chunks = chunk_document(
            _as_document(document),
            ruler,
            max_tokens=embedding_model.max_tokens,
            overlap_tokens=embedding_model.overlap_tokens,
        )
        rows.extend(
            {
                "chunk_id": chunk.chunk_id,
                "doc_id": chunk.doc_id,
                "chunk_index": chunk.chunk_index,
                "num_tokens": chunk.num_tokens,
                "text": chunk.text,
                "source_table": document.source_table,
                "url": document.url,
                "title": document.title,
                "feature_ids": document.feature_ids,
            }
            for chunk in chunks
        )

    if not rows:
        raise Failure(f"{len(frame)} document(s) produced no chunk.")

    chunks_frame = pd.DataFrame(rows)
    path = _write(chunks_frame, partition_dir, CHUNKS_FILE)
    tokens = chunks_frame["num_tokens"]

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(chunks_frame),
            "num_chunks": len(chunks_frame),
            "num_documents": int(chunks_frame["doc_id"].nunique()),
            "chunks_per_document": round(len(chunks_frame) / len(frame), 2),
            "max_tokens": embedding_model.max_tokens,
            "overlap_tokens": embedding_model.overlap_tokens,
            "tokens_median": int(tokens.median()),
            "tokens_max": int(tokens.max()),
            "output_path": MetadataValue.path(str(path)),
            "preview": MetadataValue.md(
                _preview("First chunk", chunks_frame["text"].iloc[0])
            ),
        }
    )


@asset(
    partitions_def=scrape_partitions,
    deps=[document_chunks],
    group_name=GROUP,
    description=(
        "Dense BAAI/bge-m3 vectors, one per chunk, L2-normalised so retrieval "
        "can score with a dot product. Written as a fixed-size float32 column."
    ),
)
def document_embeddings(
    context: AssetExecutionContext,
    documents: DocumentStore,
    embedding_model: EmbeddingModel,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    partition_dir = documents.partition_dir(neighborhood, scrape_date)
    frame = _read(partition_dir, CHUNKS_FILE)

    encoder = embedding_model.encoder()
    context.log.info(
        "Encoding %d chunk(s) with %s", len(frame), encoder.model_name
    )
    vectors = np.asarray(
        encoder.embed_documents(frame["text"].tolist()), dtype=np.float32
    )

    frame = frame.assign(model=encoder.model_name)
    path = _write_vectors(frame, vectors, partition_dir, EMBEDDINGS_FILE)

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_vectors": len(frame),
            "dimension": int(vectors.shape[1]),
            "model": encoder.model_name,
            "device": str(encoder.model.device),
            "output_path": MetadataValue.path(str(path)),
        }
    )


def _partition(context: AssetExecutionContext) -> tuple[str, str]:
    dimensions = context.partition_key.keys_by_dimension
    return dimensions["neighborhood"], dimensions["date"][:10]


def _wanted_columns(path: Path, url_column: str) -> list[str]:
    """Everything but the geometry, which this pipeline has no use for."""
    import pyarrow.parquet as pq

    available = set(pq.ParquetFile(path).schema.names)
    keep = [url_column, _ID_COLUMN, *_TITLE_COLUMNS]
    return [column for column in dict.fromkeys(keep) if column in available]


def _features_by_url(frame: pd.DataFrame, url_column: str) -> dict[str, dict]:
    """Which map features point at each link, and what they call it."""
    title_column = next((c for c in _TITLE_COLUMNS if c in frame.columns), None)
    index: dict[str, dict] = {}
    for url, group in frame.groupby(url_column, sort=False):
        ids = (
            [_scalar(v) for v in group[_ID_COLUMN].tolist()]
            if _ID_COLUMN in group.columns
            else []
        )
        titles = (
            [t for t in dict.fromkeys(group[title_column].dropna().astype(str)) if t]
            if title_column
            else []
        )
        index[str(url).strip()] = {
            "feature_ids": json.dumps(ids, ensure_ascii=False),
            "title": "; ".join(titles) or None,
        }
    return index


def _as_document(row) -> Document:
    return Document(
        doc_id=row.doc_id,
        url=row.url,
        text=row.text,
        num_pages=int(row.num_pages),
        content_sha256=row.content_sha256,
        num_bytes=int(row.num_bytes),
    )


def _read(partition_dir: Path, name: str) -> pd.DataFrame:
    path = partition_dir / name
    if not path.exists():
        raise Failure(f"{path} is missing; materialize its upstream asset first.")
    return pd.read_parquet(path)


def _write(frame: pd.DataFrame, partition_dir: Path, name: str) -> Path:
    partition_dir.mkdir(parents=True, exist_ok=True)
    path = partition_dir / name
    frame.to_parquet(path, index=False)
    return path


def _write_vectors(frame, vectors, partition_dir: Path, name: str) -> Path:
    return write_vectors(frame, vectors, partition_dir / name)


def _preview(title: str, text: str, limit: int = 600) -> str:
    excerpt = text[:limit].strip()
    return (
        f"### {title}\n\n"
        f"```\n{excerpt}{'...' if len(text) > limit else ''}\n```"
    )


def _scalar(value):
    """numpy scalars are not JSON-serialisable; python ones are."""
    return value.item() if hasattr(value, "item") else value
