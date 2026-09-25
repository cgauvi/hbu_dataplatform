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
harvested into columns by `urban_rag.council_items`. Silver, because a
harvest is this platform's grain and vocabulary, not the city's.

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

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.council_items import read_document, read_minutes
from urban_rag.guards import guard_current_scrape_month
from urban_rag.layers import key_prefix
from urban_rag.partitions import borough_partition_of, scrape_partitions
from urban_rag.quebec_council import (
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
from urban_rag.rag.documents import DocumentError, document_id, read_pdf
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import CouncilMinutesResource, ParquetStore, PdfCache, PostgisResource
from urban_rag.storage import dirname, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

BRONZE_GROUP = "bronze_documents"
SILVER_GROUP = "silver_corpus"

MINUTES_FILE = "minutes.parquet"
DOCUMENTS_FILE = "documents.parquet"
ITEMS_FILE = "items.parquet"

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
    key_prefix=key_prefix("council_planning_items"),
    partitions_def=scrape_partitions,
    deps=[council_minutes, council_minutes_documents],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "What the minutes and their trail say about zoning, lots and "
        "dwellings, as columns: one row per planning item - an agenda item of "
        "a minute, or one trail document - with its zones, by-law and GPD "
        "numbers, addresses, lots, dwelling caps before and after, height, "
        "decision stage and the council's opinion, each read by "
        "urban_rag.council_items with its excerpt. Written to the tree and "
        "upserted into silver.council_planning_items."
    ),
)
def council_planning_items(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = borough_partition_of(context)
    minutes = _read(store.partition_dir(council_minutes.key.path[-1], scrape_date, neighborhood), MINUTES_FILE)
    documents_dir = store.partition_dir(council_minutes_documents.key.path[-1], scrape_date, neighborhood)
    documents_path = join(documents_dir, DOCUMENTS_FILE)
    documents = (
        _read(documents_dir, DOCUMENTS_FILE)
        if filesystem(documents_path).exists(documents_path)
        else _empty_documents()
    )
    if documents.empty:
        context.log.warning("%s %s: no trail documents; reading the minutes alone", neighborhood, scrape_date)

    rows: list[dict] = []
    dropped_items = 0
    minutes_without_items = 0
    for minute in minutes.itertuples(index=False):
        items, dropped = read_minutes(minute.text)
        dropped_items += dropped
        if not items:
            minutes_without_items += 1
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
                    "neighborhood": neighborhood,
                    "scrape_date": scrape_date,
                }
            )

    council_by_minute = {m.doc_id: (int(m.council_id), m.council_name, m.meeting_date) for m in minutes.itertuples(index=False)}
    for document in documents.itertuples(index=False):
        item = read_document(document.text, title=document.title if isinstance(document.title, str) else None)
        leaders = json.loads(document.minutes_doc_ids or "[]")
        council_id, council_name, meeting_date = council_by_minute.get(leaders[0], (None, None, None)) if leaders else (None, None, None)
        rows.append(
            {
                **_item_row(item),
                "doc_id": document.doc_id,
                "source_kind": document.kind,
                "council_id": council_id,
                "council_name": council_name,
                "meeting_date": meeting_date,
                "document_number": document.document_number if isinstance(document.document_number, str) else None,
                "url": document.url,
                "minutes_doc_ids": document.minutes_doc_ids,
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

    with_zone = int((frame["subject_zone_codes"] != "[]").sum())
    with_cap = int(frame["max_dwellings_after"].notna().sum())
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_items": len(frame),
            "num_from_minutes": int((frame["source_kind"] == "minutes").sum()),
            "num_from_documents": int((frame["source_kind"] != "minutes").sum()),
            "num_agenda_items_dropped": dropped_items,
            "num_minutes_without_items": minutes_without_items,
            "item_kinds": MetadataValue.json(frame["item_kind"].value_counts().to_dict()),
            "num_with_subject_zone": with_zone,
            "num_with_dwelling_cap": with_cap,
            "num_with_council_opinion": int(frame["council_opinion"].notna().sum()),
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
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


def _empty_minutes() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="object") for column in _MINUTES_COLUMNS})


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
    columns = ["source_kind", "meeting_date", "item_kind", "subject_zone_codes", "max_dwellings_before", "max_dwellings_after", "decision", "council_opinion", "title"]
    head = frame[columns].head(12).copy()
    head["title"] = head["title"].astype(str).str.slice(0, 70)
    return head.to_markdown(index=False)


def _unique(values) -> list:
    return list(dict.fromkeys(values))


__all__ = [
    "CouncilMinutesConfig",
    "council_minutes",
    "council_minutes_documents",
    "council_planning_items",
]
