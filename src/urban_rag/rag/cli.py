"""`urban-rag` - load the vector store, search it, and ask questions of it.

Deliberately separate from the Dagster code location. The expensive half of the
work - fetching PDFs, chunking, embedding - belongs to the `rag` asset group and
its schedules; what is left here is loading those vectors into a store and
querying them, which is interactive and wants a shell rather than a run launcher.

Two stores, one command set. `--backend duckdb` (the default) is the file in
`data/`: no server, no credentials, one writer. `--backend postgres` is the
shared RDS/pgvector store, configured entirely from `URBAN_RAG_PG_*` - see
`rag.pgvector`. Set `URBAN_RAG_BACKEND=postgres` to stop typing it.
"""

from __future__ import annotations

import argparse
import os
import sys
import textwrap
import time
from pathlib import Path

from urban_rag.layers import layer_of
from urban_rag.rag.pgvector import PgSettings, PgVectorStore, PostgresUnavailable
from urban_rag.rag.store import IndexMismatch, VectorStore
from urban_rag.rag.vss import StoreLocked, VSSUnavailable
from urban_rag.storage import DATA_ROOT, join, output_root

#: Every partition `document_embeddings` has written: the glob spans
#: `<date>/<neighborhood>/`, and each file carries those two as columns.
#: `output_root()` is an `s3://` URI when `S3_BUCKET` is set - DuckDB's httpfs
#: reads that glob directly, authenticated through `AWS_PROFILE`.
#:
#: The layer comes from `urban_rag.layers` rather than being spelled `silver`
#: here, for the same reason `ParquetStore.partition_dir` looks it up: this is
#: a reader of an asset's output, and a reader that hard-codes the layer is
#: the thing that breaks when the asset moves between them.
DEFAULT_SOURCE = join(
    output_root(),
    str(layer_of("document_embeddings")),
    "document_embeddings",
    "**",
    "embeddings.parquet",
)
#: Matches the database registered in `.vscode/settings.json`, so the editor's
#: DuckDB panel opens the same file the CLI writes. Always local: DuckDB opens
#: its own database file on disk regardless of where the source parquet lives.
DEFAULT_STORE = DATA_ROOT / "vect_db.duckdb"

#: Which store the commands act on when `--backend` is not given.
BACKENDS = ("duckdb", "postgres")
DEFAULT_BACKEND = os.environ.get("URBAN_RAG_BACKEND", "duckdb")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    arguments = parser.parse_args(argv)
    try:
        return arguments.handler(arguments)
    except (IndexMismatch, StoreLocked, VSSUnavailable, PostgresUnavailable) as exc:
        parser.exit(2, f"error: {exc}\n")
    except KeyboardInterrupt:
        parser.exit(130, "\ninterrupted\n")


# -- commands --------------------------------------------------------------


def _index(arguments: argparse.Namespace) -> int:
    store = _store(arguments)
    # Take the write lock (or open the connection) before anything else, so a
    # database left open in the editor - or an unreachable one - is reported
    # now rather than after the load.
    store.check_writable()

    started = time.monotonic()
    result = store.build(
        arguments.source,
        neighborhood=arguments.neighborhood,
        scrape_date=arguments.scrape_date,
    )
    print(
        f"Indexed {result['chunks']} chunk(s) from {result['documents']} document(s) "
        f"in {time.monotonic() - started:.1f}s"
    )
    print(
        f"  model {result['embedding_model']} ({result['dimension']}d)  "
        f"-> {store.location}"
    )
    return 0


def _search(arguments: argparse.Namespace) -> int:
    store = _require_store(arguments)
    if store is None:
        return 1
    hits = store.search(
        _embeddings(arguments, store).embed_query(arguments.question),
        k=arguments.k,
        neighborhood=arguments.neighborhood,
        scrape_date=arguments.scrape_date,
    )
    if not hits:
        print("No matches.")
        return 0
    for position, hit in enumerate(hits, start=1):
        print(f"\n[{position}] {hit.similarity:.3f}  {_document_name(hit.url)}")
        print(
            f"     {hit.neighborhood} · {hit.scrape_date} · "
            f"extrait {hit.chunk_index + 1} · {hit.num_tokens} tokens"
        )
        if hit.title:
            print(f"     {hit.title}")
        print(f"     {hit.url}")
        print(textwrap.indent(_snippet(hit.text, arguments.width), "     "))
    return 0


def _ask(arguments: argparse.Namespace) -> int:
    store = _require_store(arguments)
    if store is None:
        return 1
    from urban_rag.rag.chain import build_chain, build_llm
    from urban_rag.rag.retriever import VectorStoreRetriever

    retriever = VectorStoreRetriever(
        store=store,
        embeddings=_embeddings(arguments, store),
        k=arguments.k,
        neighborhood=arguments.neighborhood,
        scrape_date=arguments.scrape_date,
    )
    print("Loading the generation model ...", file=sys.stderr)
    llm = build_llm(arguments.llm_model, max_new_tokens=arguments.max_new_tokens)

    started = time.monotonic()
    result = build_chain(retriever, llm).invoke(arguments.question)
    print(f"\n{result['answer'].strip()}\n")
    print(f"--- sources ({time.monotonic() - started:.0f}s) ---")
    for position, document in enumerate(result["documents"], start=1):
        metadata = document.metadata
        print(
            f"[{position}] {_document_name(metadata['url'])} "
            f"extrait {metadata['chunk_index'] + 1} "
            f"(similarité {metadata['similarity']:.3f})"
        )
        print(f"    {metadata['url']}")
    return 0


def _status(arguments: argparse.Namespace) -> int:
    store = _require_store(arguments)
    if store is None:
        return 1
    stats = store.stats()
    print(f"store            {store.location}")
    print(f"embedding model  {stats['embedding_model']} ({stats['dimension']}d)")
    print(f"schema version   {stats['schema_version']}")
    print(f"source           {stats.get('source_pattern', '?')}")
    print(f"documents        {stats['documents']}")
    print(
        f"chunks           {stats['chunks']} "
        f"(tokens min {stats['tokens_min']}, "
        f"mean {float(stats['tokens_mean']):.0f}, max {stats['tokens_max']})"
    )
    print("partitions")
    for neighborhood, scrape_date, documents, chunks in stats["partitions"]:
        print(f"  {neighborhood:10s} {scrape_date}  {documents:4d} doc  {chunks:5d} chunk")
    print("tables")
    for source_table, documents, chunks in stats["per_table"]:
        print(f"  {source_table:44s} {documents:4d} doc  {chunks:5d} chunk")
    return 0


# -- helpers ---------------------------------------------------------------


def _embeddings(arguments: argparse.Namespace, store: VectorStore | PgVectorStore):
    """The encoder the question must be embedded with.

    Defaults to whatever model the store was built from rather than to the
    library default: querying a bge-m3 index with a different encoder returns
    confident nonsense rather than an error, since only the width has to match.
    """
    from urban_rag.rag.embeddings import cached_embeddings

    model = arguments.embedding_model or str(store.stats()["embedding_model"])
    return cached_embeddings(model, device=arguments.device)


def _store(arguments: argparse.Namespace) -> VectorStore | PgVectorStore:
    """The store `--backend` selects. Both answer the same four commands.

    The Postgres one takes its endpoint and credentials from the environment,
    not from flags: they are the same `URBAN_RAG_PG_*` variables the Dagster
    code location reads, and a password does not belong in shell history.
    `--dsn` is the exception, for a throwaway container.
    """
    if arguments.backend == "postgres":
        return PgVectorStore(PgSettings.from_env(dsn=arguments.dsn))
    return VectorStore(arguments.store)


def _require_store(
    arguments: argparse.Namespace,
) -> VectorStore | PgVectorStore | None:
    store = _store(arguments)
    if not store.exists():
        flag = " --backend postgres" if arguments.backend == "postgres" else ""
        print(
            f"No index at {store.location}. Run `urban-rag index{flag}` first.",
            file=sys.stderr,
        )
        return None
    return store


def _document_name(url: str) -> str:
    return url.rsplit("/", 1)[-1] or url


def _snippet(text: str, width: int) -> str:
    return textwrap.fill(" ".join(text.split())[:600], width=width)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="urban-rag",
        description="Load and query the resolutions linked from the scrape.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument(
            "--backend",
            choices=BACKENDS,
            default=DEFAULT_BACKEND,
            help="Which store to act on. Overrides URBAN_RAG_BACKEND.",
        )
        subparser.add_argument(
            "--store",
            type=Path,
            default=DEFAULT_STORE,
            help="The DuckDB file, for --backend duckdb.",
        )
        subparser.add_argument(
            "--dsn",
            default=None,
            help=(
                "libpq connection string, for --backend postgres. Defaults to "
                "the URBAN_RAG_PG_* environment - which is where a password "
                "belongs."
            ),
        )
        subparser.add_argument("--neighborhood", default=None)
        subparser.add_argument("--scrape-date", default=None)

    def add_query(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("question")
        subparser.add_argument("-k", type=int, default=5)
        subparser.add_argument(
            "--embedding-model",
            default=None,
            help="Defaults to the model the store was built with.",
        )
        subparser.add_argument(
            "--device", default=None, help="torch device, e.g. cpu or cuda."
        )

    index = subparsers.add_parser(
        "index", help="(re)load the vector store from embeddings.parquet"
    )
    add_common(index)
    index.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="Glob for the parquet written by `document_embeddings`.",
    )
    index.set_defaults(handler=_index)

    search = subparsers.add_parser("search", help="retrieve passages, no generation")
    add_common(search)
    add_query(search)
    search.add_argument("--width", type=int, default=84)
    search.set_defaults(handler=_search)

    ask = subparsers.add_parser("ask", help="retrieve, then answer with a local LLM")
    add_common(ask)
    add_query(ask)
    ask.add_argument("--llm-model", default=None, help="Overrides URBAN_RAG_LLM_MODEL.")
    ask.add_argument(
        "--max-new-tokens",
        type=int,
        default=256,
        # On CPU this is the wall clock, not a ceiling: greedy decoding runs the
        # full budget unless the model emits a stop token, and the prompt asks
        # for a brief answer anyway.
        help="Generation budget. The dominant cost of `ask` on CPU.",
    )
    ask.set_defaults(handler=_ask)

    status = subparsers.add_parser("status", help="what is in the store")
    add_common(status)
    status.set_defaults(handler=_status)

    return parser


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
