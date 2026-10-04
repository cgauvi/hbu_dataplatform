"""The minutes of Quebec City's conseils de quartier, the decisions they
trail to, and what both say about zoning, lots and dwellings.

Three assets, on the borough axis like every other publisher-bounded read::

    bronze/council_minutes/<date>/CIL/minutes.parquet
    bronze/council_minutes_documents/<date>/CIL/documents.parquet
    silver/council_planning_items/<date>/CIL/items.parquet   (+ silver.council_planning_items)

`council_minutes` is the listing of every council in the borough, each
minute fetched and flattened to text with the links it carries - one row per
PDF, as filed. `council_minutes_documents` walks those links: the
consultation fiche a minute points at, the sommaire décisionnel the fiche
points at, the resolutions the sommaire points at, the consultation report
and presentation beside them - one row per document reached, with the hop
it was reached by. Both are bronze: what the city returned and where it was
found, and a link that dies costs its own row.

`council_planning_items` is the reading: a minute cut into its agenda items
and the items about planning kept, every trail document read as one item,
and each item's zones, by-law and file numbers, addresses, lots, dwelling
counts before and after, height, decision and the council's opinion
harvested into columns by `hbu_dataplatform.cities.quebec_city.council.items`. Silver, because a
harvest is this platform's grain and vocabulary, not the city's. The same
asset puts every item on the ground - the lots it names, the parcels under
the addresses it names, the zones - into `silver.council_item_sites`
(`council.sites`), and writes `citations` on each item: the PDF, and the
chunks of the corpus that cover it.

That corpus is three more assets, the council documents walked through the
same three steps the zoning grids take in `hbu_dataplatform.rag.assets`::

    silver/council_minutes_chunks/<date>/CIL/chunks.parquet       (+ silver.document_chunks)
    silver/council_minutes_embeddings/<date>/CIL/embeddings.parquet
    gold/council_minutes_index                                     (-> rag.chunks)

cut from the *repaired* text the items are read from, so an item and a
chunk meet by character offset (`council.corpus`), titled so a sommaire is
found by its number, and filed in `rag.chunks` under source tables that
start with `council_` so a reader can search the minutes alone or beside
the grids.

The councils of a borough are `quebec_council.NEIGHBORHOOD_COUNCILS`; a
Montreal or Saguenay partition has none and the assets say so rather than
fail, since the institution does not exist there. `CouncilMinutesConfig`
narrows a run to some of a borough's councils - the way the first CIL run
took Montcalm alone.
"""

# No `from __future__ import annotations`: Dagster resolves the `config:`
# annotation at decoration time and cannot see through a string.
import json
from collections import deque
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from hbu_dataplatform.cities.quebec_city.council.corpus import (
    chunk_council_document,
    chunk_ids_covering,
    chunk_spans,
    council_title,
    source_table_for,
)
from hbu_dataplatform.cities.quebec_city.council.items import (
    item_spans,
    read_document,
    read_minutes,
    repair_text,
    zone_codes,
)
from hbu_dataplatform.cities.quebec_city.council.sites import (
    compute_council_item_sites,
    read_council_item_sites,
)
from hbu_dataplatform.partitions.guards import guard_current_scrape_month
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.partitions.axes import borough_partition_of, scrape_partitions
from hbu_dataplatform.cities.quebec_city.council.councils import (
    KIND_FICHE,
    CouncilError,
    NeighborhoodCouncil,
    canonical_url,
    classify_link,
    councils_for,
    document_number_of,
    pdf_links,
    urls_in_text,
)
from hbu_dataplatform.rag.documents import Document, DocumentError, document_id, read_pdf
from hbu_dataplatform.rag.results import IndexMismatch
from hbu_dataplatform.core.frames import write_frame, write_vectors
from hbu_dataplatform.core.pg import PostgresUnavailable
from hbu_dataplatform.cities.quebec_city.resources import CouncilMinutesResource
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.rag.resources import EmbeddingModel, PdfCache, PgVectorResource
from hbu_dataplatform.core.storage import dirname, filesystem, join, storage_options
from hbu_dataplatform.core.warehouse import (
    MissingRelation,
    publish,
    published_metadata,
    upsert_frame,
)

BRONZE_GROUP = "bronze_documents"
SILVER_GROUP = "silver_corpus"
GOLD_GROUP = "gold_corpus"

MINUTES_FILE = "minutes.parquet"
DOCUMENTS_FILE = "documents.parquet"
ITEMS_FILE = "items.parquet"
SITES_FILE = "sites.parquet"
CHUNKS_FILE = "chunks.parquet"
EMBEDDINGS_FILE = "embeddings.parquet"

#: Hops from a minute the trail follows: minute -> fiche -> sommaire ->
#: resolution is three. A fourth would be a resolution's own links, which
#: point back at the sommaire.
DEFAULT_MAX_DEPTH = 3


class CouncilMinutesConfig(Config):
    """Which of the borough's councils to read, and how far to follow.

    ``council_ids`` empty means every council `councils_for` places in the
    borough. It exists because a borough's nine councils file some four
    hundred minutes between them, and a first run - or a re-run after one
    council's listing moved - has a reason to take one.
    """

    council_ids: list[int] = []
    max_depth: int = DEFAULT_MAX_DEPTH
    max_documents: int = 2000


@asset(
    key_prefix=key_prefix("council_minutes"),
    partitions_def=scrape_partitions,
    group_name=BRONZE_GROUP,
    kinds={"parquet"},
    description=(
        "The procès-verbaux of the borough's conseils de quartier - one PDF "
        "per assembly, listed by the city's affichagesite host - downloaded "
        "and flattened to text, with the links each one carries. One row per "
        "minute, as filed."
    ),
)
@guard_current_scrape_month
def council_minutes(
    context: AssetExecutionContext,
    config: CouncilMinutesConfig,
    store: ParquetStore,
    pdf_cache: PdfCache,
    council_minutes_source: CouncilMinutesResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    councils = _selected_councils(neighborhood, config.council_ids)
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, neighborhood)
    if not councils:
        # Not a failure: a Montreal borough has no conseil de quartier, and
        # an empty file is the true answer for it.
        context.log.warning("%s: no conseil de quartier registered, writing an empty file", neighborhood)
        path = _write(_empty_minutes(), output_dir, MINUTES_FILE)
        return MaterializeResult(metadata={"dagster/row_count": 0, "num_councils": 0, "output_path": MetadataValue.path(str(path))})

    fetcher = council_minutes_source.fetcher(pdf_cache.fetcher())
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: list[dict] = []
    failures: dict[str, str] = {}
    from_cache = 0
    per_council: dict[str, int] = {}

    for council in councils:
        try:
            links = fetcher.list_minutes(council)
        except CouncilError as exc:
            failures[fetcher.listing_url(council)] = str(exc)
            context.log.warning("%s", exc)
            continue
        context.log.info("%s (%d): %d minute(s) listed", council.name, council.council_id, len(links))
        per_council[council.name] = len(links)
        for link in links:
            try:
                content, cached = fetcher.fetch_pdf(link.url)
                document = read_pdf(link.url, content, keep_hyphens=True)
            except DocumentError as exc:
                failures[link.url] = str(exc)
                context.log.warning("%s", exc)
                continue
            from_cache += cached
            rows.append(
                {
                    "doc_id": document.doc_id,
                    "council_id": council.council_id,
                    "council_name": council.name,
                    "neighborhood": neighborhood,
                    "scrape_date": scrape_date,
                    "url": link.url,
                    "listing_title": link.title,
                    "listing_year": link.year,
                    "meeting_date": link.meeting_date.isoformat() if link.meeting_date else None,
                    "size_label": link.size_label,
                    "num_pages": document.num_pages,
                    "num_chars": document.num_chars,
                    "num_bytes": document.num_bytes,
                    "content_sha256": document.content_sha256,
                    "fetched_at": fetched_at,
                    "links": json.dumps(_unique(pdf_links(content) + urls_in_text(document.text)), ensure_ascii=False),
                    "text": document.text,
                }
            )

    if not rows:
        raise Failure(
            f"No minute could be read for {neighborhood} {scrape_date} "
            f"({len(failures)} failed): {'; '.join(list(failures.values())[:3])}"
        )

    frame = pd.DataFrame(rows)
    path = _write(frame, output_dir, MINUTES_FILE)
    dates = frame["meeting_date"].dropna()
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_councils": len(per_council),
            "councils": MetadataValue.json(per_council),
            "num_minutes": len(frame),
            "num_from_cache": from_cache,
            "num_failed": len(failures),
            "num_pages": int(frame["num_pages"].sum()),
            "num_chars": int(frame["num_chars"].sum()),
            "earliest_meeting": str(dates.min()) if len(dates) else None,
            "latest_meeting": str(dates.max()) if len(dates) else None,
            "output_path": MetadataValue.path(str(path)),
            "preview": MetadataValue.md(_preview("First minute", frame["text"].iloc[0])),
            **({"failures": MetadataValue.json(failures)} if failures else {}),
        }
    )


@asset(
    key_prefix=key_prefix("council_minutes_documents"),
    partitions_def=scrape_partitions,
    deps=[council_minutes],
    group_name=BRONZE_GROUP,
    kinds={"parquet"},
    description=(
        "The documents the minutes link to, followed hop by hop: the "
        "consultation fiche (HTML, read as text), the sommaire décisionnel "
        "and the resolutions on gpddocs, the consultation report and "
        "presentation. One row per document reached, with the hop it was "
        "reached by and the minutes that led there."
    ),
)
@guard_current_scrape_month
def council_minutes_documents(
    context: AssetExecutionContext,
    config: CouncilMinutesConfig,
    store: ParquetStore,
    pdf_cache: PdfCache,
    council_minutes_source: CouncilMinutesResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    minutes = _read(store.partition_dir(council_minutes.key.path[-1], scrape_date, neighborhood), MINUTES_FILE)
    if config.council_ids:
        minutes = minutes[minutes["council_id"].isin(config.council_ids)]
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, neighborhood)

    fetcher = council_minutes_source.fetcher(pdf_cache.fetcher())
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Breadth-first from every minute's links, one row per canonical URL,
    # remembering every minute that led to it: two assemblies about one
    # amendment both point at its fiche.
    queue: deque[tuple[str, str, str, int]] = deque()  # url, parent_url, parent_doc_id, depth
    led_by: dict[str, list[str]] = {}
    ignored = 0
    for minute in minutes.itertuples(index=False):
        for raw in json.loads(minute.links or "[]"):
            kind = classify_link(raw)
            if kind is None:
                ignored += 1
                continue
            url = canonical_url(raw)
            led_by.setdefault(url, [])
            if minute.doc_id not in led_by[url]:
                led_by[url].append(minute.doc_id)
            queue.append((url, minute.url, minute.doc_id, 1))

    rows: list[dict] = []
    seen: set[str] = set()
    failures: dict[str, str] = {}
    from_cache = 0
    while queue and len(rows) < config.max_documents:
        url, parent_url, parent_doc_id, depth = queue.popleft()
        if url in seen:
            continue
        seen.add(url)
        kind = classify_link(url)
        try:
            row, links = _fetch_trail_document(fetcher, url, kind)
        except (CouncilError, DocumentError) as exc:
            failures[url] = str(exc)
            context.log.warning("%s", exc)
            continue
        from_cache += row.pop("_cached")
        rows.append(
            {
                **row,
                "neighborhood": neighborhood,
                "scrape_date": scrape_date,
                "depth": depth,
                "parent_url": parent_url,
                "parent_doc_id": parent_doc_id,
                "minutes_doc_ids": json.dumps(led_by.get(url, []), ensure_ascii=False),
                "fetched_at": fetched_at,
                "links": json.dumps(links, ensure_ascii=False),
            }
        )
        if depth >= config.max_depth:
            continue
        for raw in links:
            next_kind = classify_link(raw)
            if next_kind is None:
                continue
            next_url = canonical_url(raw)
            led_by.setdefault(next_url, [])
            for minute_id in led_by.get(url, []):
                if minute_id not in led_by[next_url]:
                    led_by[next_url].append(minute_id)
            if next_url not in seen:
                queue.append((next_url, url, row["doc_id"], depth + 1))

    frame = pd.DataFrame(rows) if rows else _empty_documents()
    path = _write(frame, output_dir, DOCUMENTS_FILE)
    kinds = frame["kind"].value_counts().to_dict() if len(frame) else {}
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_documents": len(frame),
            "num_minutes_with_links": int(sum(1 for m in minutes["links"] if json.loads(m or "[]"))),
            "num_links_ignored": ignored,
            "num_from_cache": from_cache,
            "num_failed": len(failures),
            "num_truncated": len(queue),
            "kinds": MetadataValue.json(kinds),
            "output_path": MetadataValue.path(str(path)),
            **({"preview": MetadataValue.md(_preview("First document", frame["text"].iloc[0]))} if len(frame) else {}),
            **({"failures": MetadataValue.json(failures)} if failures else {}),
        }
    )


def _fetch_trail_document(fetcher, url: str, kind: str) -> tuple[dict, list[str]]:
    """One document on the trail as a row plus the links it carries."""
    if kind == KIND_FICHE:
        page = fetcher.fetch_fiche(url)
        text = page.text
        if not text:
            raise CouncilError(f"{url}: the fiche page carries no text")
        return (
            {
                "doc_id": document_id(url),
                "url": url,
                "kind": kind,
                "document_number": None,
                "title": page.title,
                "content_type": "text/html",
                "num_pages": None,
                "num_chars": len(text),
                "num_bytes": None,
                "content_sha256": None,
                "text": text,
                "_cached": 0,
            },
            _unique(list(page.links)),
        )
    content, cached = fetcher.fetch_pdf(url)
    document = read_pdf(url, content, keep_hyphens=True)
    return (
        {
            "doc_id": document.doc_id,
            "url": url,
            "kind": kind,
            "document_number": document_number_of(url),
            "title": None,
            "content_type": "application/pdf",
            "num_pages": document.num_pages,
            "num_chars": document.num_chars,
            "num_bytes": document.num_bytes,
            "content_sha256": document.content_sha256,
            "text": document.text,
            "_cached": int(cached),
        },
        _unique(pdf_links(content) + urls_in_text(document.text)),
    )


@asset(
    key_prefix=key_prefix("council_minutes_chunks"),
    partitions_def=scrape_partitions,
    deps=[council_minutes, council_minutes_documents],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "The minutes and their trail documents cut into overlapping, "
        "paragraph-aligned chunks measured with the encoder's tokenizer - the "
        "same cut the zoning grids get - from the repaired text the planning "
        "items are read from, so an item and a chunk meet by offset. Filed "
        "under source tables council_minutes, council_gpd, council_fiche, "
        "council_consultation_file. Written to the tree and upserted into "
        "silver.document_chunks beside the grids' chunks."
    ),
)
def council_minutes_chunks(
    context: AssetExecutionContext,
    store: ParquetStore,
    embedding_model: EmbeddingModel,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    minutes = _read(store.partition_dir(council_minutes.key.path[-1], scrape_date, neighborhood), MINUTES_FILE)
    documents = _read_documents_if_any(store, scrape_date, neighborhood)
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, neighborhood)

    if minutes.empty and documents.empty:
        # A borough with no conseil de quartier: an empty corpus is the true
        # answer, and the embeddings and the index below read it as such.
        path = _write(_empty_chunks(), output_dir, CHUNKS_FILE)
        return MaterializeResult(metadata={"dagster/row_count": 0, "num_chunks": 0, "output_path": MetadataValue.path(str(path))})

    ruler = embedding_model.ruler()
    rows: list[dict] = []
    for minute in minutes.itertuples(index=False):
        _, chunks = chunk_council_document(
            _as_document(minute), ruler,
            max_tokens=embedding_model.max_tokens, overlap_tokens=embedding_model.overlap_tokens,
        )
        title = council_title("minutes", council_name=minute.council_name, meeting_date=minute.meeting_date)
        rows.extend(_chunk_rows(chunks, minute, source_table_for("minutes"), title, neighborhood, scrape_date))
    for document in documents.itertuples(index=False):
        _, chunks = chunk_council_document(
            _as_document(document), ruler,
            max_tokens=embedding_model.max_tokens, overlap_tokens=embedding_model.overlap_tokens,
        )
        title = council_title(
            document.kind,
            document_number=document.document_number if isinstance(document.document_number, str) else None,
            title=document.title if isinstance(document.title, str) else None,
            text=document.text,
        )
        rows.extend(_chunk_rows(chunks, document, source_table_for(document.kind), title, neighborhood, scrape_date))

    if not rows:
        raise Failure(f"{len(minutes)} minute(s) and {len(documents)} document(s) produced no chunk.")

    frame = pd.DataFrame(rows)
    path = _write(frame, output_dir, CHUNKS_FILE)
    # Into silver.document_chunks beside the grids' chunks of the same
    # partition - and *not* through `publish`, whose prune drops every row of
    # the (borough, date) the frame does not carry, which here would be the
    # zoning corpus. The prune is done by hand, to the council rows alone.
    try:
        with postgis.connect() as connection:
            loaded = upsert_frame(connection, "document_chunks", frame, partition=neighborhood, scrape_date=scrape_date, prune=False)
            cursor = connection.cursor()
            cursor.execute(
                "DELETE FROM silver.document_chunks "
                "WHERE neighborhood = %s AND scrape_date = %s::date "
                "AND source_table LIKE 'council\\_%%' AND NOT (chunk_id = ANY(%s))",
                [neighborhood, scrape_date, frame["chunk_id"].tolist()],
            )
            loaded["pruned"] = max(cursor.rowcount, 0)
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.document_chunks could not be "
            f"updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    tokens = frame["num_tokens"]
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_chunks": len(frame),
            "num_documents": int(frame["doc_id"].nunique()),
            "chunks_by_source": MetadataValue.json(frame["source_table"].value_counts().to_dict()),
            "num_with_zone_codes": int((frame["feature_ids"] != "[]").sum()),
            "max_tokens": embedding_model.max_tokens,
            "overlap_tokens": embedding_model.overlap_tokens,
            "tokens_median": int(tokens.median()),
            "tokens_max": int(tokens.max()),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata({"document_chunks": loaded}),
            "preview": MetadataValue.md(_preview("First chunk", frame["text"].iloc[0])),
        }
    )


@asset(
    key_prefix=key_prefix("council_minutes_embeddings"),
    partitions_def=scrape_partitions,
    deps=[council_minutes_chunks],
    group_name=SILVER_GROUP,
    kinds={"parquet"},
    description=(
        "One bge-m3 vector per council chunk, L2-normalised, as a fixed-size "
        "float32 column - the council corpus' half of document_embeddings."
    ),
)
def council_minutes_embeddings(
    context: AssetExecutionContext,
    store: ParquetStore,
    embedding_model: EmbeddingModel,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    frame = _read(store.partition_dir(council_minutes_chunks.key.path[-1], scrape_date, neighborhood), CHUNKS_FILE)
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, neighborhood)
    if frame.empty:
        path = _write(frame.assign(model=pd.Series(dtype="object")), output_dir, EMBEDDINGS_FILE)
        return MaterializeResult(metadata={"dagster/row_count": 0, "num_vectors": 0, "output_path": MetadataValue.path(str(path))})

    encoder = embedding_model.encoder()
    context.log.info("Encoding %d council chunk(s) with %s", len(frame), encoder.model_name)
    vectors = np.asarray(encoder.embed_documents(frame["text"].tolist()), dtype=np.float32)
    frame = frame.assign(model=encoder.model_name)
    path = write_vectors(frame, vectors, join(output_dir, EMBEDDINGS_FILE))
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
    key_prefix=key_prefix("council_minutes_index"),
    partitions_def=scrape_partitions,
    deps=[council_minutes_embeddings],
    group_name=GOLD_GROUP,
    kinds={"postgres"},
    description=(
        "The council corpus' vectors upserted into rag.chunks beside the "
        "grids', on (neighborhood, chunk_id), newest scrape wins. A load, not "
        "a computation - see document_index."
    ),
)
def council_minutes_index(
    context: AssetExecutionContext,
    store: ParquetStore,
    pgvector: PgVectorResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    path = join(store.partition_dir(council_minutes_embeddings.key.path[-1], scrape_date, neighborhood), EMBEDDINGS_FILE)
    if not filesystem(path).exists(path):
        raise Failure(f"{path} is missing; materialize its upstream asset first.")
    if pd.read_parquet(path, columns=["chunk_id"], storage_options=storage_options(path)).empty:
        context.log.info("%s %s: no council chunk to load", neighborhood, scrape_date)
        return MaterializeResult(metadata={"dagster/row_count": 0, "num_upserted": 0})

    vector_store = pgvector.store()
    try:
        vector_store.check_writable()
        context.log.info("Loading %s into %s", path, vector_store.location)
        # No prune: `load_partition`'s prune drops the borough's *older* scrape
        # dates, which is right when the zoning corpus and this one land on the
        # same date and wrong when they do not - the council run lagging the
        # grids by a month would delete the grids. document_index prunes.
        result = vector_store.load_partition(path, neighborhood=neighborhood, scrape_date=scrape_date, prune=False)
    except (PostgresUnavailable, IndexMismatch) as exc:
        raise Failure(str(exc)) from exc

    return MaterializeResult(
        metadata={
            "dagster/row_count": result["copied"],
            "num_copied": result["copied"],
            "num_upserted": result["loaded"],
            "chunks_in_store": result["chunks"],
            "documents_in_store": result["documents"],
            "dimension": result["dimension"],
            "model": result["embedding_model"],
            "table": result["table"],
            "target": result["location"],
        }
    )


@asset(
    key_prefix=key_prefix("council_planning_items"),
    partitions_def=scrape_partitions,
    deps=[council_minutes, council_minutes_documents, council_minutes_chunks],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "What the minutes and their trail say about zoning, lots and "
        "dwellings, as columns: one row per planning item - an agenda item of "
        "a minute, or one trail document - with its zones, by-law and GPD "
        "numbers, addresses, lots, dwelling caps before and after, height, "
        "decision stage, outcome and the council's opinion, each read by "
        "hbu_dataplatform.cities.quebec_city.council.items with its excerpt, and "
        "`citations`: the PDF and the corpus chunks covering the item. Written "
        "to the tree and upserted into silver.council_planning_items; then "
        "every item's lots, addresses and zones are put on the ground into "
        "silver.council_item_sites (and sites.parquet)."
    ),
)
def council_planning_items(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    minutes = _read(store.partition_dir(council_minutes.key.path[-1], scrape_date, neighborhood), MINUTES_FILE)
    documents = _read_documents_if_any(store, scrape_date, neighborhood)
    if documents.empty:
        context.log.warning("%s %s: no trail documents; reading the minutes alone", neighborhood, scrape_date)
    chunks = _read_chunks_if_any(store, scrape_date, neighborhood)
    if chunks.empty:
        context.log.warning("%s %s: no corpus chunks; citations carry the PDFs alone", neighborhood, scrape_date)
    chunks_by_doc = _chunks_by_doc(chunks)
    # A minute's trail, for its items' citations: every document it led to.
    trail_by_minute: dict[str, list[dict]] = {}
    for document in documents.itertuples(index=False):
        for minute_id in json.loads(document.minutes_doc_ids or "[]"):
            trail_by_minute.setdefault(minute_id, []).append(
                {
                    "doc_id": document.doc_id,
                    "url": document.url,
                    "kind": document.kind,
                    "document_number": document.document_number if isinstance(document.document_number, str) else None,
                }
            )

    rows: list[dict] = []
    dropped_items = 0
    minutes_without_items = 0
    for minute in minutes.itertuples(index=False):
        items, dropped = read_minutes(minute.text)
        dropped_items += dropped
        if not items:
            minutes_without_items += 1
        spans = _item_chunk_ids(minute.text, chunks_by_doc.get(minute.doc_id, []))
        for item in items:
            rows.append(
                {
                    **_item_row(item),
                    "doc_id": minute.doc_id,
                    "source_kind": "minutes",
                    "council_id": int(minute.council_id),
                    "council_name": minute.council_name,
                    "meeting_date": minute.meeting_date,
                    "document_number": None,
                    "url": minute.url,
                    "minutes_doc_ids": json.dumps([minute.doc_id]),
                    "citations": json.dumps(
                        {
                            "url": minute.url,
                            "doc_id": minute.doc_id,
                            "source_kind": "minutes",
                            "chunk_ids": spans.get(item.item_index, []),
                            "trail": trail_by_minute.get(minute.doc_id, []),
                        },
                        ensure_ascii=False,
                    ),
                    "neighborhood": neighborhood,
                    "scrape_date": scrape_date,
                }
            )

    council_by_minute = {m.doc_id: (int(m.council_id), m.council_name, m.meeting_date, m.url) for m in minutes.itertuples(index=False)}
    for document in documents.itertuples(index=False):
        item = read_document(document.text, title=document.title if isinstance(document.title, str) else None)
        leaders = json.loads(document.minutes_doc_ids or "[]")
        council_id, council_name, meeting_date, _ = council_by_minute.get(leaders[0], (None, None, None, None)) if leaders else (None, None, None, None)
        document_number = document.document_number if isinstance(document.document_number, str) else None
        rows.append(
            {
                **_item_row(item),
                "doc_id": document.doc_id,
                "source_kind": document.kind,
                "council_id": council_id,
                "council_name": council_name,
                "meeting_date": meeting_date,
                "document_number": document_number,
                "url": document.url,
                "minutes_doc_ids": document.minutes_doc_ids,
                "citations": json.dumps(
                    {
                        "url": document.url,
                        "doc_id": document.doc_id,
                        "source_kind": document.kind,
                        "document_number": document_number,
                        # The whole document is the item, so every chunk of it.
                        "chunk_ids": [chunk_id for chunk_id, _ in chunks_by_doc.get(document.doc_id, [])],
                        "minutes": [
                            {"doc_id": minute_id, "url": council_by_minute[minute_id][3]}
                            for minute_id in leaders
                            if minute_id in council_by_minute
                        ],
                    },
                    ensure_ascii=False,
                ),
                "neighborhood": neighborhood,
                "scrape_date": scrape_date,
            }
        )

    if not rows:
        raise Failure(f"{len(minutes)} minute(s) and {len(documents)} document(s) yielded no planning item.")

    frame = pd.DataFrame(rows)
    path = _write(frame, store.partition_dir(context.asset_key.path[-1], scrape_date, neighborhood), ITEMS_FILE)
    try:
        loaded = publish(postgis.connect, {"council_planning_items": frame}, partition=neighborhood, scrape_date=scrape_date)
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.council_planning_items could not be "
            f"updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    # The items are in the table; now put them on the ground. Computed in
    # PostGIS against the cadastre, the address points and the zoning layer
    # already loaded there, kept as geoparquet beside items.parquet.
    try:
        with postgis.connect() as connection:
            sites = compute_council_item_sites(connection, neighborhood=neighborhood, scrape_date=scrape_date)
            sites_frame = read_council_item_sites(connection, neighborhood=neighborhood, scrape_date=scrape_date)
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written and silver.council_planning_items updated, but "
            f"silver.council_item_sites could not be computed for {neighborhood} "
            f"{scrape_date}: {exc}"
        ) from exc
    sites_path = write_frame(sites_frame, join(store.partition_dir(context.asset_key.path[-1], scrape_date, neighborhood), SITES_FILE))

    with_zone = int((frame["subject_zone_codes"] != "[]").sum())
    with_cap = int(frame["max_dwellings_after"].notna().sum())
    with_chunks = int(frame["citations"].map(lambda c: bool(json.loads(c).get("chunk_ids"))).sum())
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_items": len(frame),
            "num_from_minutes": int((frame["source_kind"] == "minutes").sum()),
            "num_from_documents": int((frame["source_kind"] != "minutes").sum()),
            "num_agenda_items_dropped": dropped_items,
            "num_minutes_without_items": minutes_without_items,
            "item_kinds": MetadataValue.json(frame["item_kind"].value_counts().to_dict()),
            "outcomes": MetadataValue.json(frame["outcome"].fillna("none").value_counts().to_dict()),
            "num_with_subject_zone": with_zone,
            "num_with_dwelling_cap": with_cap,
            "num_with_council_opinion": int(frame["council_opinion"].notna().sum()),
            "num_with_chunk_citations": with_chunks,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
            # -- the sites --------------------------------------------------
            "num_sites": sites["num_sites"],
            "sites_by_kind": MetadataValue.json(sites["sites_by_kind"]),
            "sites_by_match_basis": MetadataValue.json(sites["sites_by_match_basis"]),
            "num_items_on_a_parcel": sites["num_items_on_a_parcel"],
            "num_items_placed": sites["num_items_placed"],
            "named": MetadataValue.json(sites["named"]),
            "unplaced": MetadataValue.json(sites["unplaced"]),
            **({"unplaced_sample": MetadataValue.json(sites["unplaced_sample"])} if sites["unplaced_sample"] else {}),
            "sites_path": MetadataValue.path(str(sites_path)),
            "preview": MetadataValue.md(_items_preview(frame)),
        }
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _selected_councils(neighborhood: str, council_ids: list[int]) -> tuple[NeighborhoodCouncil, ...]:
    councils = councils_for(neighborhood)
    if not council_ids:
        return councils
    wanted = set(council_ids)
    chosen = tuple(c for c in councils if c.council_id in wanted)
    missing = wanted - {c.council_id for c in chosen}
    if missing:
        raise Failure(
            f"council_ids {sorted(missing)} are not councils of {neighborhood}; "
            f"its councils are {[(c.council_id, c.name) for c in councils]}"
        )
    return chosen


def _as_document(row) -> Document:
    """A bronze minute or trail document as the chunker's `Document`."""
    return Document(
        doc_id=row.doc_id,
        url=row.url,
        text=row.text,
        num_pages=int(row.num_pages) if pd.notna(row.num_pages) else 0,
        content_sha256=row.content_sha256 if isinstance(row.content_sha256, str) else "",
        num_bytes=int(row.num_bytes) if pd.notna(row.num_bytes) else 0,
    )


def _chunk_rows(chunks, row, source_table: str, title: str | None, neighborhood: str, scrape_date: str) -> list[dict]:
    """One `rag.chunks`-shaped row per chunk.

    `feature_ids` carries the zone codes the chunk itself names - the
    corpus' "which zones cite this" column, read here as "which zones this
    passage is about", which is what lets a question naming 14040Hb reach
    the minute that discussed it the way it reaches the zone's grid.
    """
    return [
        {
            "chunk_id": chunk.chunk_id,
            "doc_id": chunk.doc_id,
            "chunk_index": chunk.chunk_index,
            "num_tokens": chunk.num_tokens,
            "text": chunk.text,
            "source_table": source_table,
            "neighborhood": neighborhood,
            "scrape_date": scrape_date,
            "url": row.url,
            "title": title,
            "feature_ids": json.dumps(zone_codes(chunk.text), ensure_ascii=False),
        }
        for chunk in chunks
    ]


def _chunks_by_doc(chunks: pd.DataFrame) -> dict[str, list[tuple[str, str]]]:
    """``{doc_id: [(chunk_id, text), ...]}`` in chunk order."""
    if chunks.empty:
        return {}
    ordered = chunks.sort_values(["doc_id", "chunk_index"])
    grouped: dict[str, list[tuple[str, str]]] = {}
    for chunk in ordered.itertuples(index=False):
        grouped.setdefault(chunk.doc_id, []).append((chunk.chunk_id, chunk.text))
    return grouped


def _item_chunk_ids(text: str, chunks: list[tuple[str, str]]) -> dict[int, list[str]]:
    """``{item_index: [chunk_id, ...]}``: the chunks covering each agenda
    item of a minute, by character offset in the repaired text both were
    cut from. An agenda that repeats a number pools that number's spans."""
    if not chunks:
        return {}
    spans = chunk_spans(repair_text(text), [chunk_text for _, chunk_text in chunks])
    ids = [chunk_id for chunk_id, _ in chunks]
    covering: dict[int, list[str]] = {}
    for item_index, start, end in item_spans(text):
        found = covering.setdefault(item_index, [])
        found.extend(c for c in chunk_ids_covering((start, end), spans, ids) if c not in found)
    return covering


def _item_row(item) -> dict:
    """A `PlanningItem` as flat parquet columns: lists as JSON strings, the
    way `linked_documents` writes `feature_ids`."""
    row = item.as_row()
    for column in (
        "subject_zone_codes",
        "zone_codes",
        "bylaw_numbers",
        "gpd_numbers",
        "subject_addresses",
        "addresses",
        "lot_numbers",
        "usage_groups",
        "dwelling_changes",
        "dwelling_counts",
        "storeys",
        "parse_notes",
    ):
        row[column] = json.dumps(row[column], ensure_ascii=False)
    row["votes"] = json.dumps(row["votes"], ensure_ascii=False)
    row["decision_date"] = row["decision_date"].isoformat() if row["decision_date"] else None
    row["excerpt"] = row["text"][:1500]
    return row


_MINUTES_COLUMNS = [
    "doc_id", "council_id", "council_name", "neighborhood", "scrape_date", "url", "listing_title",
    "listing_year", "meeting_date", "size_label", "num_pages", "num_chars", "num_bytes",
    "content_sha256", "fetched_at", "links", "text",
]
_DOCUMENT_COLUMNS = [
    "doc_id", "url", "kind", "document_number", "title", "content_type", "num_pages", "num_chars",
    "num_bytes", "content_sha256", "text", "neighborhood", "scrape_date", "depth", "parent_url",
    "parent_doc_id", "minutes_doc_ids", "fetched_at", "links",
]


_CHUNK_COLUMNS = [
    "chunk_id", "doc_id", "chunk_index", "num_tokens", "text", "source_table",
    "neighborhood", "scrape_date", "url", "title", "feature_ids",
]


def _empty_minutes() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in _MINUTES_COLUMNS})


def _empty_chunks() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in _CHUNK_COLUMNS})


def _read_documents_if_any(store: ParquetStore, scrape_date: str, neighborhood: str) -> pd.DataFrame:
    documents_dir = store.partition_dir(council_minutes_documents.key.path[-1], scrape_date, neighborhood)
    documents_path = join(documents_dir, DOCUMENTS_FILE)
    if filesystem(documents_path).exists(documents_path):
        return _read(documents_dir, DOCUMENTS_FILE)
    return _empty_documents()


def _read_chunks_if_any(store: ParquetStore, scrape_date: str, neighborhood: str) -> pd.DataFrame:
    chunks_dir = store.partition_dir(council_minutes_chunks.key.path[-1], scrape_date, neighborhood)
    chunks_path = join(chunks_dir, CHUNKS_FILE)
    if filesystem(chunks_path).exists(chunks_path):
        return _read(chunks_dir, CHUNKS_FILE)
    return _empty_chunks()


def _empty_documents() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in _DOCUMENT_COLUMNS})


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


def _preview(title: str, text: str, limit: int = 600) -> str:
    excerpt = str(text)[:limit].strip()
    return f"### {title}\n\n```\n{excerpt}{'...' if len(str(text)) > limit else ''}\n```"


def _items_preview(frame: pd.DataFrame) -> str:
    columns = ["source_kind", "meeting_date", "item_kind", "subject_zone_codes", "max_dwellings_before", "max_dwellings_after", "decision", "outcome", "council_opinion", "title"]
    head = frame[columns].head(12).copy()
    head["title"] = head["title"].astype(str).str.slice(0, 70)
    return head.to_markdown(index=False)


def _unique(values) -> list:
    return list(dict.fromkeys(values))


__all__ = [
    "CouncilMinutesConfig",
    "council_minutes",
    "council_minutes_chunks",
    "council_minutes_documents",
    "council_minutes_embeddings",
    "council_minutes_index",
    "council_planning_items",
]
