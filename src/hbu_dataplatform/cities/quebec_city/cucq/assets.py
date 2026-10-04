"""The minutes of the Commission d'urbanisme et de conservation de Québec,
and the decisions read out of them - one per permit request, placed on a
borough by its address.

Three assets, all on the **date axis**: the Commission is one body for the
whole city and its minutes name no arrondissement, so there is no borough
to fetch them for. The borough comes later, from the ground::

    bronze/cucq_minutes/<date>/minutes.parquet
    silver/cucq_decisions/<date>/decisions.parquet       (+ silver.cucq_decisions, per borough)
    silver/cucq_minutes_chunks/<date>/chunks.parquet     (+ silver.document_chunks, per borough)

`cucq_minutes` lists the minutes through the decisions portal's search
index and fetches every PDF through the shared cache, one row per sitting,
flattened to text with the links it carries - as filed. The minutes before
2013 are scans with no text layer, in the PDF and in the index alike; they
are rows with ``has_text`` false rather than failures, so the OCR backlog is
a count in the table and not a gap in it.

`cucq_decisions` is the reading: a regular sitting's annexed list cut into
its entries and a demolition committee's minute into its hearing items,
each a `Decision` - the request number, the applicant, the address, the
works, the verdict - by `hbu_dataplatform.cities.quebec_city.cucq.minutes`.
Silver, because a harvest is this platform's grain and vocabulary, not the
city's. The same asset puts every decision on the ground the way the
council items are placed: the civic number and the street against
`silver.lot_addresses` in PostGIS (`cucq.placement`), which is what gives a
row the borough every table here is partitioned by. The parquet keeps every
decision, placed or not; the table receives the placed ones, one borough
partition each, through `publish_by_neighborhood` the way the assessment
roll's units are published. What reached no loaded door is counted under
``unplaced`` and waits for that borough's addresses.

`cucq_minutes_chunks` is the corpus: one document per *decision* rather
than per minute, because a sitting is sixty requests on sixty addresses and
a passage about one of them should be retrievable - and placeable - on its
own. Each decision's paragraph (`minutes.decision_text`) is chunked with
the same cut the zoning grids and the council minutes get, titled by the
sitting, the address and the verdict, filed under source tables that start
with ``cucq_``, and upserted into `silver.document_chunks` beside the other
two corpora of each borough without pruning them. The embeddings and the
load into `rag.chunks` are the two steps `council_minutes_embeddings` and
`council_minutes_index` take, and are not taken here yet: the chunks are
where the RAG picks this up.
"""

# No `from __future__ import annotations`: Dagster resolves the `config:`
# annotation at decoration time and cannot see through a string.
import json
from datetime import UTC, date, datetime

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from hbu_dataplatform.cities.quebec_city.council.councils import pdf_links
from hbu_dataplatform.cities.quebec_city.cucq.minutes import (
    Decision,
    decision_title,
    read_sitting,
    source_table_for,
)
from hbu_dataplatform.cities.quebec_city.cucq.placement import place_addresses
from hbu_dataplatform.cities.quebec_city.cucq.portal import CucqError, MinuteRecord
from hbu_dataplatform.cities.quebec_city.resources import CucqMinutesResource
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.core.pg import PostgresUnavailable
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.core.storage import dirname, filesystem, join, storage_options
from hbu_dataplatform.core.warehouse import (
    MissingRelation,
    publish_by_neighborhood,
    published_metadata,
    upsert_frame,
)
from hbu_dataplatform.partitions.axes import date_partitions
from hbu_dataplatform.partitions.guards import guard_current_scrape_month
from hbu_dataplatform.rag.documents import (
    Document,
    DocumentError,
    chunk_document,
    document_id,
    read_pdf,
)
from hbu_dataplatform.rag.resources import EmbeddingModel, PdfCache

BRONZE_GROUP = "bronze_documents"
SILVER_GROUP = "silver_corpus"

MINUTES_FILE = "minutes.parquet"
DECISIONS_FILE = "decisions.parquet"
CHUNKS_FILE = "chunks.parquet"

#: The columns the decisions parquet stores as JSON strings, the way
#: `council_planning_items` writes its lists.
_LIST_COLUMNS = ("addresses", "civic_numbers", "lot_numbers", "parse_notes")


class CucqMinutesConfig(Config):
    """How much of the Commission's record to fetch.

    The index lists some 1,300 sittings back to 2000 and a first run fetches
    every PDF - about a gigabyte, once, into the shared cache. ``since_year``
    narrows a run to the years that have a text layer (2013 on) or to the
    recent ones; ``series`` to the regular sittings (``OR``) or the
    demolition committee's (``DEM``); ``max_minutes`` is a stop.
    """

    since_year: int | None = None
    series: list[str] = []
    max_minutes: int = 5000


@asset(
    key_prefix=key_prefix("cucq_minutes"),
    partitions_def=date_partitions,
    group_name=BRONZE_GROUP,
    kinds={"parquet"},
    description=(
        "The procès-verbaux of the Commission d'urbanisme et de conservation "
        "de Québec - one PDF per sitting, listed by the city's decisions "
        "portal (an Azure Search index) and served from gpddocs - downloaded "
        "and flattened to text, with the links each one carries. One row per "
        "minute, as filed; a scan with no text layer is a row with has_text "
        "false. City-wide: the Commission sits for every arrondissement."
    ),
)
@guard_current_scrape_month
def cucq_minutes(
    context: AssetExecutionContext,
    config: CucqMinutesConfig,
    store: ParquetStore,
    pdf_cache: PdfCache,
    cucq_source: CucqMinutesResource,
) -> MaterializeResult:
    scrape_date = context.partition_key
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)
    client = cucq_source.client()
    try:
        records = client.list_minutes(since_year=config.since_year)
    except CucqError as exc:
        raise Failure(str(exc)) from exc
    if config.series:
        wanted = {code.upper() for code in config.series}
        records = [record for record in records if (record.series or "").upper() in wanted]
    truncated = max(0, len(records) - config.max_minutes)
    records = records[: config.max_minutes]
    context.log.info("%s: %d CUCQ minute(s) listed%s", scrape_date, len(records), f", {truncated} beyond max_minutes" if truncated else "")

    fetcher = pdf_cache.fetcher()
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    rows: list[dict] = []
    failures: dict[str, str] = {}
    from_cache = 0
    for record in records:
        try:
            content, cached = fetcher.fetch(record.url)
        except DocumentError as exc:
            failures[record.url] = str(exc)
            context.log.warning("%s", exc)
            if record.index_text:
                # The portal's own extraction stands in for a PDF that would
                # not come: same text, and the row says where it is from.
                rows.append(_minute_row(record, None, None, record.index_text, "index", str(exc), fetched_at, scrape_date))
            continue
        from_cache += cached
        try:
            document = read_pdf(record.url, content, keep_hyphens=True)
        except DocumentError as exc:
            # A scan: the file came, the text did not. Kept as a row - the
            # listing is still data, and the OCR backlog should be visible.
            text = record.index_text
            rows.append(_minute_row(record, content, None, text, "index" if text else "none", str(exc), fetched_at, scrape_date))
            continue
        rows.append(_minute_row(record, content, document, document.text, "pdf", None, fetched_at, scrape_date))

    if not rows:
        raise Failure(
            f"No CUCQ minute could be read for {scrape_date} "
            f"({len(failures)} failed): {'; '.join(list(failures.values())[:3])}"
        )

    frame = pd.DataFrame(rows)
    path = _write(frame, output_dir, MINUTES_FILE)
    dates = frame["meeting_date"].dropna()
    with_text = frame[frame["has_text"]]
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_minutes": len(frame),
            "by_series": MetadataValue.json(frame["series"].fillna("?").value_counts().to_dict()),
            "num_with_text": int(frame["has_text"].sum()),
            "num_textless": int((~frame["has_text"]).sum()),
            "num_from_cache": from_cache,
            "num_failed": len(failures),
            "num_truncated": truncated,
            "num_pages": int(frame["num_pages"].fillna(0).sum()),
            "num_chars": int(frame["num_chars"].sum()),
            "earliest_sitting": str(dates.min()) if len(dates) else None,
            "latest_sitting": str(dates.max()) if len(dates) else None,
            "output_path": MetadataValue.path(str(path)),
            **({"preview": MetadataValue.md(_preview("First minute with text", with_text["text"].iloc[0]))} if len(with_text) else {}),
            **({"failures": MetadataValue.json(dict(list(failures.items())[:50]))} if failures else {}),
        }
    )


def _minute_row(
    record: MinuteRecord,
    content: bytes | None,
    document: Document | None,
    text: str,
    text_source: str,
    read_error: str | None,
    fetched_at: str,
    scrape_date: str,
) -> dict:
    import hashlib

    return {
        "doc_id": document_id(record.url),
        "url": record.url,
        "storage_name": record.storage_name,
        "series": record.series,
        "meeting_date": record.meeting_date.isoformat() if record.meeting_date else None,
        "year": record.year,
        "number": record.number,
        "objet": record.objet,
        "instance": record.instance,
        "administrative_unit": record.administrative_unit,
        "num_pages": document.num_pages if document else None,
        "num_chars": len(text),
        "num_bytes": len(content) if content is not None else None,
        "content_sha256": hashlib.sha256(content).hexdigest() if content is not None else None,
        "has_text": bool(text),
        "text_source": text_source,
        "read_error": read_error,
        "fetched_at": fetched_at,
        "links": json.dumps(_unique(pdf_links(content)) if content is not None else [], ensure_ascii=False),
        "text": text,
        "scrape_date": scrape_date,
    }


@asset(
    key_prefix=key_prefix("cucq_decisions"),
    partitions_def=date_partitions,
    deps=[cucq_minutes],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "What the Commission decided, one row per permit request: the "
        "request number, applicant, address, works and their kind, the "
        "dwellings the permit category names, and the verdict - approved, "
        "approved conditionally, refused - with the resolution that carried "
        "it; a demolition committee's hearing item with its considérants and "
        "the lot it names. Read by regular expression from the minutes' "
        "annexed lists (hbu_dataplatform.cities.quebec_city.cucq.minutes), "
        "then placed on a borough by its address against silver.lot_addresses. "
        "City-wide parquet; silver.cucq_decisions receives the placed rows, "
        "one borough partition each."
    ),
)
def cucq_decisions(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    scrape_date = context.partition_key
    minutes = _read(store.partition_dir(cucq_minutes.key.path[-1], scrape_date), MINUTES_FILE)
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)

    rows: list[dict] = []
    textless = 0
    without_decisions = 0
    sitting_notes: dict[str, list[str]] = {}
    for minute in minutes.itertuples(index=False):
        if not bool(minute.has_text) or not isinstance(minute.text, str) or not minute.text.strip():
            textless += 1
            continue
        series = minute.series if isinstance(minute.series, str) else None
        when = date.fromisoformat(minute.meeting_date) if isinstance(minute.meeting_date, str) and minute.meeting_date else None
        sitting = read_sitting(minute.text, series=series, meeting_date=when)
        if sitting.parse_notes:
            sitting_notes[minute.storage_name] = sitting.parse_notes
        if not sitting.decisions:
            without_decisions += 1
            continue
        for decision in sitting.decisions:
            rows.append(
                {
                    **_decision_row(decision),
                    "doc_id": minute.doc_id,
                    # The corpus files one document per decision, so each one
                    # gets an id of its own - stable across runs, since it is
                    # the minute's URL and the decision's place in it.
                    "chunk_doc_id": document_id(f"{minute.url}#item-{decision.item_index}"),
                    "url": minute.url,
                    "storage_name": minute.storage_name,
                    "series": series,
                    "sitting_kind": sitting.sitting_kind,
                    "sitting_number": sitting.sitting_number,
                    "meeting_date": sitting.meeting_date.isoformat() if sitting.meeting_date else None,
                    "title": decision_title(decision, sitting),
                    "address_key": _address_key(decision),
                    "neighborhood": None,
                    "lot_number": None,
                    "match_basis": None,
                    "scrape_date": scrape_date,
                }
            )

    if not rows:
        raise Failure(
            f"{len(minutes)} minute(s) for {scrape_date} yielded no decision "
            f"({textless} without a text layer, {without_decisions} with no readable list)."
        )

    frame = pd.DataFrame(rows)
    for column in _LIST_COLUMNS:
        frame[column] = frame[column].map(lambda value: json.dumps(value, ensure_ascii=False))
    # Written before the placement, so a database that is down costs the
    # placement and the publish rather than the reading.
    path = _write(frame, output_dir, DECISIONS_FILE)

    wanted = {
        row.address_key: (int(row.civic), row.street)
        for row in frame.itertuples(index=False)
        if isinstance(row.address_key, str) and pd.notna(row.civic) and isinstance(row.street, str)
    }
    try:
        with postgis.connect() as connection:
            placements = place_addresses(connection, [(key, civic, street) for key, (civic, street) in wanted.items()])
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(f"{path} was written, but the decisions could not be placed on a borough: {exc}") from exc
    frame["neighborhood"] = frame["address_key"].map(lambda key: placements[key].neighborhood if key in placements else None)
    frame["lot_number"] = frame["address_key"].map(lambda key: placements[key].lot_number if key in placements else None)
    frame["match_basis"] = frame["address_key"].map(lambda key: placements[key].match_basis if key in placements else None)
    path = _write(frame, output_dir, DECISIONS_FILE)

    placed = frame[frame["neighborhood"].notna()]
    frames = {str(neighborhood): part for neighborhood, part in placed.groupby("neighborhood")}
    if frames:
        try:
            loaded = publish_by_neighborhood(postgis.connect, "cucq_decisions", frames, scrape_date=scrape_date)
        except (PostgresUnavailable, MissingRelation) as exc:
            raise Failure(f"{path} was written, but silver.cucq_decisions could not be updated for {scrape_date}: {exc}") from exc
    else:
        loaded = {}
        context.log.warning(
            "%s: none of the %d decision(s) reached a loaded Quebec City door; "
            "silver.lot_addresses holds no borough they fall in yet",
            scrape_date, len(frame),
        )

    unplaced = frame[frame["neighborhood"].isna()]
    unplaced_keys = unplaced["address_key"].dropna().unique().tolist()
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_decisions": len(frame),
            "num_sittings": int(frame["doc_id"].nunique()),
            "num_minutes_textless": textless,
            "num_minutes_without_decisions": without_decisions,
            "num_minutes_with_notes": len(sitting_notes),
            "by_decision": MetadataValue.json(frame["decision"].value_counts().to_dict()),
            "by_works_kind": MetadataValue.json(frame["works_kind"].value_counts().to_dict()),
            "num_demolitions": int(frame["is_demolition"].sum()),
            "num_heard": int(frame["was_heard"].sum()),
            "num_with_request_number": int(frame["request_number"].notna().sum()),
            "num_placed": len(placed),
            "num_unplaced": len(unplaced),
            "num_unplaced_without_civic": int(unplaced["address_key"].isna().sum()),
            "placed_by_neighborhood": MetadataValue.json({k: len(v) for k, v in frames.items()}),
            **({"unplaced_sample": MetadataValue.json(unplaced_keys[:12])} if unplaced_keys else {}),
            **({"sitting_notes_sample": MetadataValue.json(dict(list(sitting_notes.items())[:8]))} if sitting_notes else {}),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
            "preview": MetadataValue.md(_decisions_preview(frame)),
        }
    )


@asset(
    key_prefix=key_prefix("cucq_minutes_chunks"),
    partitions_def=date_partitions,
    deps=[cucq_decisions],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "The placed decisions as a retrieval corpus: one document per "
        "decision - its paragraph, or a hearing item's prose - cut with the "
        "encoder's tokenizer the way the grids and the council minutes are, "
        "titled by the sitting, the address and the verdict, filed under "
        "cucq_minutes / cucq_demolition_committee. Written to the tree and "
        "upserted into silver.document_chunks beside each borough's other "
        "chunks, without pruning them. Embeddings and the rag.chunks load "
        "are the next two steps and are not taken here yet."
    ),
)
def cucq_minutes_chunks(
    context: AssetExecutionContext,
    store: ParquetStore,
    embedding_model: EmbeddingModel,
    postgis: PostgisResource,
) -> MaterializeResult:
    scrape_date = context.partition_key
    decisions = _read(store.partition_dir(cucq_decisions.key.path[-1], scrape_date), DECISIONS_FILE)
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)
    placed = decisions[decisions["neighborhood"].notna()]
    if placed.empty:
        context.log.warning("%s: no placed decision; the corpus is empty until a borough's addresses are loaded", scrape_date)
        path = _write(_empty_chunks(), output_dir, CHUNKS_FILE)
        return MaterializeResult(metadata={"dagster/row_count": 0, "num_chunks": 0, "output_path": MetadataValue.path(str(path))})

    ruler = embedding_model.ruler()
    rows: list[dict] = []
    for decision in placed.itertuples(index=False):
        document = Document(
            doc_id=decision.chunk_doc_id, url=decision.url, text=decision.text,
            num_pages=0, content_sha256="", num_bytes=0,
        )
        for chunk in chunk_document(document, ruler, max_tokens=embedding_model.max_tokens, overlap_tokens=embedding_model.overlap_tokens):
            rows.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "doc_id": chunk.doc_id,
                    "chunk_index": chunk.chunk_index,
                    "num_tokens": chunk.num_tokens,
                    "text": chunk.text,
                    "source_table": source_table_for(decision.series if isinstance(decision.series, str) else None),
                    "neighborhood": decision.neighborhood,
                    "scrape_date": scrape_date,
                    "url": decision.url,
                    "title": decision.title,
                    # No zone cites a decision; the ground is the decision's
                    # own address, kept on silver.cucq_decisions.
                    "feature_ids": "[]",
                    # Kept in the parquet for the join back to the decision;
                    # silver.document_chunks has no column for them and drops them.
                    "minute_doc_id": decision.doc_id,
                    "item_index": int(decision.item_index),
                    "request_number": decision.request_number,
                }
            )
    if not rows:
        raise Failure(f"{len(placed)} placed decision(s) produced no chunk.")

    frame = pd.DataFrame(rows)
    path = _write(frame, output_dir, CHUNKS_FILE)
    loaded: dict[str, dict[str, int]] = {}
    try:
        with postgis.connect() as connection:
            for neighborhood, part in frame.groupby("neighborhood"):
                # Beside the grids' and the councils' chunks of the same
                # (borough, date), without `publish`'s prune, which would
                # drop both; the prune is done by hand to the CUCQ rows alone.
                counts = upsert_frame(connection, "document_chunks", part, partition=str(neighborhood), scrape_date=scrape_date, prune=False)
                cursor = connection.cursor()
                cursor.execute(
                    "DELETE FROM silver.document_chunks "
                    "WHERE neighborhood = %s AND scrape_date = %s::date "
                    "AND source_table LIKE 'cucq\\_%%' AND NOT (chunk_id = ANY(%s))",
                    [str(neighborhood), scrape_date, part["chunk_id"].tolist()],
                )
                counts["pruned"] = max(cursor.rowcount, 0)
                loaded[str(neighborhood)] = counts
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(f"{path} was written, but silver.document_chunks could not be updated for {scrape_date}: {exc}") from exc

    tokens = frame["num_tokens"]
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_chunks": len(frame),
            "num_documents": int(frame["doc_id"].nunique()),
            "num_decisions_unplaced": int(decisions["neighborhood"].isna().sum()),
            "chunks_by_source": MetadataValue.json(frame["source_table"].value_counts().to_dict()),
            "chunks_by_neighborhood": MetadataValue.json(frame["neighborhood"].value_counts().to_dict()),
            "max_tokens": embedding_model.max_tokens,
            "overlap_tokens": embedding_model.overlap_tokens,
            "tokens_median": int(tokens.median()),
            "tokens_max": int(tokens.max()),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
            "preview": MetadataValue.md(_preview("First chunk", frame["text"].iloc[0])),
        }
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _decision_row(decision: Decision) -> dict:
    return decision.as_row()


def _address_key(decision: Decision) -> str | None:
    """``"467, Rue Arago Ouest"``: the first door, what the placement is
    looked up by."""
    if decision.civic is None or not decision.street:
        return None
    return f"{decision.civic}, {decision.street}"


_CHUNK_COLUMNS = [
    "chunk_id", "doc_id", "chunk_index", "num_tokens", "text", "source_table",
    "neighborhood", "scrape_date", "url", "title", "feature_ids",
    "minute_doc_id", "item_index", "request_number",
]


def _empty_chunks() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in _CHUNK_COLUMNS})


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


def _decisions_preview(frame: pd.DataFrame) -> str:
    columns = ["meeting_date", "request_number", "address_line", "works_kind", "decision", "neighborhood", "applicant"]
    head = frame[columns].head(12).copy()
    head["applicant"] = head["applicant"].astype(str).str.slice(0, 40)
    head["address_line"] = head["address_line"].astype(str).str.slice(0, 45)
    return head.to_markdown(index=False)


def _unique(values) -> list:
    return list(dict.fromkeys(values))


__all__ = [
    "CucqMinutesConfig",
    "cucq_decisions",
    "cucq_minutes",
    "cucq_minutes_chunks",
]
