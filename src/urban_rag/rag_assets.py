"""Assets that turn the zoning grids linked from a scraped table into a corpus.

Three steps, kept apart so each can be re-run on its own: fetch and flatten
the PDFs, cut them into chunks, embed the chunks. Re-chunking is cheap;
re-embedding is not, and neither should force a re-download.

Each lands under its own asset prefix, keyed the same way as the scrape::

    data/bronze/linked_documents/2026-08-18/VSMPE/documents.parquet
    data/silver/document_chunks/2026-08-18/VSMPE/chunks.parquet
    data/silver/document_embeddings/2026-08-18/VSMPE/embeddings.parquet

so re-running one step replaces one prefix and leaves the other two alone.

The layer boundary falls after the fetch. `linked_documents` is bronze: it is
the publisher's PDF, flattened to text and otherwise untouched, and a dead link
costs its own row rather than the partition. Chunking and embedding are silver -
the same documents cut to this platform's own retrieval grain, measured with
this platform's own tokenizer, which is a choice about how the corpus is used
rather than a property of what was published.

A fourth, `document_index`, is gold: it publishes that partition's vectors into
the Postgres/pgvector store the query side reads. It is the one asset in the
platform that writes no parquet of its own, and deliberately - it is a *load*,
not a computation, and what it loads is `document_embeddings`, which is already
in the tree. Its record is that file.
"""

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.guards import guard_current_scrape_month
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
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.rag.results import IndexMismatch
from urban_rag.resources import (
    EmbeddingModel,
    ParquetStore,
    PdfCache,
    PgVectorResource,
    PostgisResource,
)
from urban_rag.storage import dirname, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

BRONZE_GROUP = "bronze_documents"
SILVER_GROUP = "silver_corpus"
GOLD_GROUP = "gold_corpus"

DOCUMENTS_FILE = "documents.parquet"
CHUNKS_FILE = "chunks.parquet"
EMBEDDINGS_FILE = "embeddings.parquet"

#: Attribute columns worth carrying alongside a document, first match wins.
#: The id is whichever citation number a user would actually reference - for
#: ``VSP_REG_ZONE`` that is ``NUMERO_COMPLET``, the zone number printed on the
#: grid itself ("C01-001"), which is also what `neighborhood_features` joins a
#: parcel to. ``ID`` and the other titles are kept for tables added to
#: `DOCUMENT_SOURCES` later; the zone table carries neither.
_ID_COLUMNS = ("NUMERO_COMPLET", "ID")
_TITLE_COLUMNS = ("USAGE", "DESCRIPTION", "NOM_CAT")


@asset(
    key_prefix=key_prefix("linked_documents"),
    partitions_def=scrape_partitions,
    deps=[neighborhood_features],
    group_name=BRONZE_GROUP,
    kinds={"parquet"},
    description=(
        "The zoning grids linked from a scraped table's URL column - today "
        "Reglement_urbanisme__VSP_REG_ZONE.LIEN_GRILLE, one 'grille des "
        "usages et des normes' PDF per zone - downloaded and flattened to "
        "text. One row per distinct link, so two zones sharing a grid cost "
        "one document."
    ),
)
@guard_current_scrape_month
def linked_documents(
    context: AssetExecutionContext,
    store: ParquetStore,
    pdf_cache: PdfCache,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    source_dir = store.partition_dir(
        neighborhood_features.key.path[-1], scrape_date, neighborhood
    )
    fetcher = pdf_cache.fetcher()
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    rows: list[dict] = []
    failures: dict[str, str] = {}
    from_cache = 0

    for slug, url_column in DOCUMENT_SOURCES.items():
        path = join(source_dir, f"{slug}.parquet")
        if not filesystem(path).exists(path):
            context.log.warning("%s: not in this partition, skipped", slug)
            continue

        frame = pd.read_parquet(
            path,
            columns=_wanted_columns(path, url_column),
            storage_options=storage_options(path),
        )
        try:
            links = document_urls(frame, url_column)
        except DocumentError as exc:
            # The registry names a column this table no longer has. That is a
            # registry bug, so it belongs in `failures` where the metadata
            # shows it - not as a traceback that costs the whole partition.
            failures[f"{slug}.{url_column}"] = str(exc)
            context.log.warning("%s: %s", slug, exc)
            continue
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
                    "neighborhood": neighborhood,
                    "scrape_date": scrape_date,
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
    path = _write(frame, _partition_dir(context, store), DOCUMENTS_FILE)

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
    key_prefix=key_prefix("document_chunks"),
    partitions_def=scrape_partitions,
    deps=[linked_documents],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "Documents cut into overlapping, paragraph-aligned chunks, measured "
        "with the embedding model's own tokenizer. Written to the tree and "
        "upserted into silver.document_chunks on (scrape_date, neighborhood, "
        "chunk_id) - the corpus before it is embedded, which rag.chunks only "
        "ever holds the current scrape of."
    ),
)
def document_chunks(
    context: AssetExecutionContext,
    store: ParquetStore,
    embedding_model: EmbeddingModel,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    frame = _read(
        store.partition_dir(
            linked_documents.key.path[-1], scrape_date, neighborhood
        ),
        DOCUMENTS_FILE,
    )
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
                "neighborhood": document.neighborhood,
                "scrape_date": document.scrape_date,
                "url": document.url,
                "title": document.title,
                "feature_ids": document.feature_ids,
            }
            for chunk in chunks
        )

    if not rows:
        raise Failure(f"{len(frame)} document(s) produced no chunk.")

    chunks_frame = pd.DataFrame(rows)
    path = _write(chunks_frame, _partition_dir(context, store), CHUNKS_FILE)
    # After the file, so a database that is down costs the load rather than
    # the chunking - which re-reads every PDF of the borough through the
    # tokenizer. Same order every parquet-first asset here uses.
    try:
        loaded = publish(
            postgis.connect,
            {"document_chunks": chunks_frame},
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.document_chunks could not be "
            f"updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc
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
            **published_metadata(loaded),
            "preview": MetadataValue.md(
                _preview("First chunk", chunks_frame["text"].iloc[0])
            ),
        }
    )


@asset(
    key_prefix=key_prefix("document_embeddings"),
    partitions_def=scrape_partitions,
    deps=[document_chunks],
    group_name=SILVER_GROUP,
    kinds={"parquet"},
    description=(
        "Dense BAAI/bge-m3 vectors, one per chunk, L2-normalised so retrieval "
        "can score with a dot product. Written as a fixed-size float32 column."
    ),
)
def document_embeddings(
    context: AssetExecutionContext,
    store: ParquetStore,
    embedding_model: EmbeddingModel,
) -> MaterializeResult:
    neighborhood, scrape_date = _partition(context)
    frame = _read(
        store.partition_dir(
            document_chunks.key.path[-1], scrape_date, neighborhood
        ),
        CHUNKS_FILE,
    )

    encoder = embedding_model.encoder()
    context.log.info(
        "Encoding %d chunk(s) with %s", len(frame), encoder.model_name
    )
    vectors = np.asarray(
        encoder.embed_documents(frame["text"].tolist()), dtype=np.float32
    )

    frame = frame.assign(model=encoder.model_name)
    path = _write_vectors(
        frame, vectors, _partition_dir(context, store), EMBEDDINGS_FILE
    )

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


@asset(
    key_prefix=key_prefix("document_index"),
    partitions_def=scrape_partitions,
    deps=[document_embeddings],
    group_name=GOLD_GROUP,
    kinds={"postgres"},
    description=(
        "This partition's vectors published to the Postgres/pgvector store the "
        "query side reads: upserted on chunk_id, newest scrape date wins. "
        "Writes no parquet - it is a load of document_embeddings, which is "
        "already in the tree."
    ),
)
def document_index(
    context: AssetExecutionContext,
    store: ParquetStore,
    pgvector: PgVectorResource,
) -> MaterializeResult:
    """Load `embeddings.parquet` into Postgres. A load, not a computation.

    The only asset in this group that writes outside the parquet tree, and the
    only one whose output another process is reading while it runs - so the
    partition is upserted into the live table in one transaction rather than
    replacing it, and the borough's superseded scrape dates are dropped after
    the new one has landed. See `urban_rag.rag.pgvector`.
    """
    neighborhood, scrape_date = _partition(context)
    path = join(
        store.partition_dir(
            document_embeddings.key.path[-1], scrape_date, neighborhood
        ),
        EMBEDDINGS_FILE,
    )
    if not filesystem(path).exists(path):
        raise Failure(f"{path} is missing; materialize its upstream asset first.")

    vector_store = pgvector.store()
    try:
        # Connect before reading a gigabyte of parquet: a closed security group
        # or an expired password should cost the first second of the run.
        vector_store.check_writable()
        context.log.info("Loading %s into %s", path, vector_store.location)
        result = vector_store.load_partition(
            path,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
            prune=pgvector.prune_superseded,
        )
    except (PostgresUnavailable, IndexMismatch) as exc:
        # Both carry a next step in their message; a Failure keeps it in the UI
        # instead of burying it under a driver traceback.
        raise Failure(str(exc)) from exc

    context.log.info(
        "%s chunk(s) upserted, %s superseded row(s) deleted",
        result["loaded"],
        result["pruned"],
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": result["copied"],
            "num_copied": result["copied"],
            "num_upserted": result["loaded"],
            "num_pruned": result["pruned"],
            "chunks_in_store": result["chunks"],
            "documents_in_store": result["documents"],
            "dimension": result["dimension"],
            "model": result["embedding_model"],
            "table": result["table"],
            "target": result["location"],
        }
    )


def _partition(context: AssetExecutionContext) -> tuple[str, str]:
    dimensions = context.partition_key.keys_by_dimension
    return dimensions["neighborhood"], dimensions["date"][:10]


def _partition_dir(context: AssetExecutionContext, store: ParquetStore) -> str:
    """Where the running asset writes.

    `<root>/<layer>/<asset>/<date>/<neighborhood>/` - the layer comes from
    `urban_rag.layers`, so this is the same call whichever layer the asset is
    in.
    """
    neighborhood, scrape_date = _partition(context)
    return store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )


def _wanted_columns(path: str, url_column: str) -> list[str]:
    """Everything but the geometry, which this pipeline has no use for."""
    import pyarrow.parquet as pq

    with filesystem(path).open(path, "rb") as handle:
        available = set(pq.ParquetFile(handle).schema.names)
    keep = [url_column, *_ID_COLUMNS, *_TITLE_COLUMNS]
    return [column for column in dict.fromkeys(keep) if column in available]


def _features_by_url(frame: pd.DataFrame, url_column: str) -> dict[str, dict]:
    """Which map features point at each link, and what they call it."""
    id_column = next((c for c in _ID_COLUMNS if c in frame.columns), None)
    title_column = next((c for c in _TITLE_COLUMNS if c in frame.columns), None)
    index: dict[str, dict] = {}
    for url, group in frame.groupby(url_column, sort=False):
        ids = [_scalar(v) for v in group[id_column].tolist()] if id_column else []
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


def _read(partition_dir: str, name: str) -> pd.DataFrame:
    path = join(partition_dir, name)
    if not filesystem(path).exists(path):
        raise Failure(f"{path} is missing; materialize its upstream asset first.")
    return pd.read_parquet(path, storage_options=storage_options(path))


def _write(frame: pd.DataFrame, partition_dir: str, name: str) -> str:
    path = join(partition_dir, name)
    filesystem(path).makedirs(dirname(path), exist_ok=True)
    frame.to_parquet(path, index=False, storage_options=storage_options(path))
    return path


def _write_vectors(frame, vectors, partition_dir: str, name: str) -> str:
    return write_vectors(frame, vectors, join(partition_dir, name))


def _preview(title: str, text: str, limit: int = 600) -> str:
    excerpt = text[:limit].strip()
    return (
        f"### {title}\n\n"
        f"```\n{excerpt}{'...' if len(text) > limit else ''}\n```"
    )


def _scalar(value):
    """numpy scalars are not JSON-serialisable; python ones are."""
    return value.item() if hasattr(value, "item") else value
