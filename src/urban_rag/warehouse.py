"""The Postgres serving copy of every silver and gold asset.

One table per dataset, in the schema its medallion layer is named for -
`silver.neighborhood_streets`, `gold.lot_profiles` - and one way of writing to
it. Before this module the four assets that reached Postgres each carried their
own loader in `urban_rag.postgis`, all of them writing into `rag`, all of them
deleting a partition and re-inserting it; the seven that did not reach Postgres
at all had no table to reach. This is the single write path both halves now go
through.

Three rules hold for every table here, and they are the reason the write can be
generic at all.

**The schema is the layer.** `silver/vacancy_rates` in the tree is
`silver.vacancy_rates` in the database, and `gold/lot_profiles` is
`gold.lot_profiles`. The schema is looked up from `urban_rag.layers` rather
than written down twice, the same way `ParquetStore.partition_dir` gets its
prefix - so moving an asset between layers moves its table with it instead of
leaving the two disagreeing about which layer it is in.

**The grain is (neighborhood, scrape_date, natural key).** Every table is
declaratively partitioned `PARTITION BY LIST (neighborhood)` and then
`PARTITION BY RANGE (scrape_date)` by month, so a borough's month is a leaf a
reader's `WHERE` prunes to and an operator can detach. Postgres requires a
partitioned table's unique constraint to contain its partition keys, which is
not a tax here but the grain restated: the primary key is
``(scrape_date, neighborhood, <key>)`` and that is exactly what a write
conflicts on::

    INSERT INTO silver.neighborhood_streets (...)
    VALUES (...)
    ON CONFLICT (scrape_date, neighborhood, cote_rue_id)
    DO UPDATE SET ...

**A write is an upsert, and a partition is still a snapshot.** `upsert_frame`
COPYs into a staging table shaped `LIKE` the target, upserts the whole
partition in one statement, and then deletes the partition's rows the staging
table does not have. The upsert is what lets a re-run land while readers are
querying - nothing is ever missing mid-load, the way a delete-then-insert
leaves it - and the prune is what keeps snapshot semantics, which the upsert
alone cannot: a lot that disappears from the cadastre has no row to conflict
with and would otherwise sit in the table forever. Both run in the caller's
transaction, so a reader sees the partition as it was or as it now is.

That second half also settles what to do about a key that does not survive a
re-scrape. `silver.building_lot_intersections` conflicts on `building_uid`, a
bigserial `load_buildings` mints again on every load, so a re-run of the same
partition upserts nothing and prunes everything - which is precisely the
delete-then-insert that table needs and used to spell out for itself. The
mechanism is one; what changes per table is only which columns are the key.

The partitions themselves are created on demand by `ensure_partition`, which
calls hbu_infra's `warehouse.ensure_partition` - see that repo's
sql/003_warehouse.sql. Deliberately not a `DEFAULT` partition: a row that lands
in a default cannot be moved by attaching the partition it belongs in, so a
default that quietly catches a borough nobody declared is a table that has to
be rewritten to fix. Failing on an unknown borough is the cheaper error.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

from urban_rag.layers import layer_of

if TYPE_CHECKING:  # pragma: no cover - typing only, psycopg is imported lazily
    from psycopg import Connection, Cursor

#: The two columns every warehouse table is partitioned on, in the order they
#: lead the primary key. `scrape_date` first because that is the axis a reader
#: filters on without naming a borough - "the 26th, everywhere" is a question,
#: "VSMPE, any date" is not.
PARTITION_COLUMNS: tuple[str, str] = ("scrape_date", "neighborhood")


class MissingRelation(RuntimeError):
    """A table this module writes to has not been created yet.

    Distinct from letting psycopg raise `relation "x" does not exist`: every
    table here belongs to hbu_infra, so the message that helps names the .sql
    file to apply rather than the identifier that failed to resolve.
    """


@dataclass(frozen=True)
class Table:
    """One dataset's table: where it lives, and what a row of it is keyed by.

    ``asset`` is the Dagster asset that produces the dataset, and is what the
    schema is derived from - not a schema name written out here, which would
    be the second place a layer is declared and the one that goes stale.

    ``keys`` are the natural-key columns *beyond* the partition ones. They are
    what a re-run conflicts on, so they have to identify a row within one
    (neighborhood, scrape_date) and nothing wider: `cote_rue_id` is unique
    across the island, but two scrape dates legitimately both carry it, which
    is why the date leads the key rather than being left out of it.

    ``columns`` maps a target column to the frame column it comes from, and is
    only written where the two differ - the geobase publishes `NOM_VOIE`, this
    platform calls it `street_name`. Matching is otherwise by name and then
    case-insensitively, so a source that shouts its columns needs no entry.

    ``attributes`` names a jsonb catch-all, when the table has one: every frame
    column that found no target column of its own is packed into it. A layer
    that gains a column upstream then lands in the same table rather than
    needing a migration - the posture `rag.features.attributes` already takes.
    """

    asset: str
    name: str
    keys: tuple[str, ...]
    source: str
    columns: Mapping[str, str] = field(default_factory=dict)
    geometry: str | None = None
    attributes: str | None = None

    @property
    def schema(self) -> str:
        """The medallion layer this table's asset belongs to."""
        return str(layer_of(self.asset))

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def conflict_columns(self) -> tuple[str, ...]:
        """The `ON CONFLICT` target: the partition keys, then the natural key."""
        return (*PARTITION_COLUMNS, *self.keys)


#: Every silver and gold dataset that has a table, keyed by the name it is
#: known by in the tree. An asset that writes two files has two entries -
#: `building_lot_intersections` writes both `building_lots.parquet` and
#: `lot_features.parquet`, and they are two grains, so they are two tables.
#:
#: Two assets are deliberately absent, and neither is an oversight:
#:
#: * `document_embeddings` (silver) - its vectors' home is the pgvector index
#:   `rag.chunks`, which `document_index` writes and every reader of the corpus
#:   queries. A `silver.document_embeddings` would be a second copy of the one
#:   thing in this platform measured in gigabytes, to serve no read.
#: * `document_index` (gold) - it *is* that load. It already upserts on
#:   `chunk_id` into a table `urban_rag.rag.pgvector` creates and hbu_infra's
#:   search functions are written against, with the same COPY-into-staging then
#:   `ON CONFLICT DO UPDATE` this module performs; repartitioning it would cut
#:   one HNSW index into one index per borough-month, which changes what a
#:   recall number from it means. See `PgVectorStore.load_partition`.
TABLES: dict[str, Table] = {
    # -- silver ------------------------------------------------------------
    "vacancy_rates": Table(
        asset="vacancy_rates",
        name="vacancy_rates",
        keys=("dwelling_type", "bedroom_type"),
        source="sql/010_silver_cmhc.sql",
    ),
    "quartier_vacancy_rates": Table(
        asset="vacancy_rates",
        name="quartier_vacancy_rates",
        keys=("quartier", "dwelling_type", "bedroom_type"),
        source="sql/010_silver_cmhc.sql",
    ),
    "average_rents": Table(
        asset="average_rents",
        name="average_rents",
        keys=("bedroom_type",),
        source="sql/010_silver_cmhc.sql",
    ),
    "quartier_average_rents": Table(
        asset="average_rents",
        name="quartier_average_rents",
        keys=("quartier", "bedroom_type"),
        source="sql/010_silver_cmhc.sql",
    ),
    "building_lot_intersections": Table(
        asset="building_lot_intersections",
        name="building_lot_intersections",
        keys=("building_uid", "lot_uid"),
        source="sql/004_silver_building_lots.sql",
        geometry="geom",
    ),
    "lot_features": Table(
        asset="building_lot_intersections",
        name="lot_features",
        keys=("lot_uid", "source_table", "feature_id"),
        source="sql/005_silver_lot_features.sql",
        geometry="geom",
    ),
    "neighborhood_streets": Table(
        asset="neighborhood_streets",
        name="neighborhood_streets",
        keys=("cote_rue_id",),
        source="sql/007_silver_streets.sql",
        columns={"cote_rue_id": "COTE_RUE_ID", "street_name": "NOM_VOIE"},
        geometry="geom",
        attributes="attributes",
    ),
    "lot_frontage": Table(
        asset="lot_frontage",
        name="lot_frontage",
        keys=("lot_uid", "cote_rue_id"),
        source="sql/008_silver_lot_frontage.sql",
        geometry="geom",
    ),
    "document_chunks": Table(
        asset="document_chunks",
        name="document_chunks",
        keys=("chunk_id",),
        source="sql/011_silver_corpus.sql",
    ),
    "zoning_grid_columns": Table(
        asset="zoning_grid_columns",
        name="zoning_grid_columns",
        keys=("source_table", "feature_id", "column_index"),
        source="sql/012_silver_zoning.sql",
    ),
    "lot_zoning_envelopes": Table(
        asset="lot_zoning_envelopes",
        name="lot_zoning_envelopes",
        keys=("lot_uid", "feature_id", "column_index"),
        source="sql/012_silver_zoning.sql",
    ),
    # The same key as the envelopes it subtracts the margins from, because it
    # is the same grain: one candidate envelope per (lot, zone, column), and
    # each column states its own four margins.
    "lot_buildable_setbacks": Table(
        asset="lot_buildable_setbacks",
        name="lot_buildable_setbacks",
        keys=("lot_uid", "feature_id", "column_index"),
        source="sql/015_silver_lot_buildable_setbacks.sql",
        geometry="geom",
    ),
    # The roll is published for the province, so the asset that merges it is
    # partitioned by date alone and its parquet stays province-wide. The
    # borough this table is partitioned on is not in the merge: it is the one
    # whose boundary the unit's point falls inside, assigned by
    # `role_assets.assign_boroughs` on the way here. That is a cut of the same
    # kind `neighborhood_streets` makes on the island-wide geobase - one
    # publication, cut into partitions in silver - and it is why one date
    # partition publishes several borough partitions in one transaction. See
    # `publish_by_neighborhood`.
    #
    # `columns` is longer here than anywhere else because the roll names its
    # fields by MAMH code. Only the ones this platform reads are given a name;
    # the other forty land in `attributes`, where a reader who knows the code
    # can still reach them and a roll that adds one needs no migration.
    "assessment_units": Table(
        asset="assessment_units",
        name="assessment_units",
        keys=("id_provinc",),
        source="sql/014_silver_assessment_units.sql",
        columns={
            "use_code": "rl0105a",
            "frontage_m": "rl0301a",
            "land_area_m2": "rl0302a",
            "num_storeys": "rl0306a",
            "year_built": "rl0307a",
            "floor_area_m2": "rl0308a",
            "num_dwellings": "rl0311a",
            "num_nonresidential_units": "rl0312a",
            "num_rental_rooms": "rl0313a",
            "land_value": "rl0402a",
            "building_value": "rl0403a",
            "assessed_value": "rl0404a",
        },
        geometry="geom",
        attributes="attributes",
    ),
    "lot_assessed_values": Table(
        asset="lot_assessed_values",
        name="lot_assessed_values",
        keys=("lot_number",),
        source="sql/013_silver_lot_assessed_values.sql",
        # Infolot shouts its columns, this platform does not - the same one
        # entry `neighborhood_streets` needs for `COTE_RUE_ID`. Everything
        # else the cadastre publishes about the lot lands in `attributes`.
        columns={"lot_number": "NO_LOT"},
        geometry="geom",
        attributes="attributes",
    ),
    # The same grain and the same key as the table above, because it is the
    # same lot: this one carries what that one does not - the roll's
    # characteristics summed onto the parcel, what they earn a year, and the k
    # lots the borough says are like it. Two tables rather than more columns on
    # one, because the totals there are a sum over `rl0404a` and nothing else,
    # and a reader who wants only "what is this lot assessed at" should not
    # have to read a jsonb of neighbours to get it.
    #
    # `columns` is one entry for the same reason 013's is: Infolot shouts
    # `NO_LOT` and this platform does not. Everything else already carries the
    # name the column has, and the cadastre's own attributes - which arrive on
    # the frame because that table's geometry is read whole - land in the jsonb
    # catch-all beside `comparables`.
    "lot_assessment_comparables": Table(
        asset="lot_assessment_comparables",
        name="lot_assessment_comparables",
        keys=("lot_number",),
        source="sql/016_silver_lot_assessment_comparables.sql",
        columns={"lot_number": "NO_LOT"},
        geometry="geom",
        attributes="attributes",
    ),
    # -- gold --------------------------------------------------------------
    "lot_profiles": Table(
        asset="lot_profiles",
        name="lot_profiles",
        keys=("lot_number",),
        source="sql/009_gold_lot_profiles.sql",
        geometry="geom",
    ),
}


def table_for(dataset: str) -> Table:
    """The warehouse table ``dataset`` is published to.

    Raises rather than defaulting: a dataset with no table has nowhere to be
    written, and inventing `silver.<dataset>` would create a table nothing in
    hbu_infra knows about and nothing reads.
    """
    try:
        return TABLES[dataset]
    except KeyError:
        raise KeyError(
            f"{dataset!r} has no table in urban_rag.warehouse.TABLES; add it "
            f"there and in hbu_infra's sql/. Known: {', '.join(sorted(TABLES))}"
        ) from None


def tables_in(layer: str) -> tuple[Table, ...]:
    """Every declared table in one layer, in declaration order."""
    return tuple(table for table in TABLES.values() if table.schema == str(layer))


# --------------------------------------------------------------------------
# the write path
# --------------------------------------------------------------------------


def upsert_frame(
    connection: "Connection",
    dataset: str,
    frame: Any,
    *,
    neighborhood: str,
    scrape_date: str,
    prune: bool = True,
) -> dict[str, int]:
    """Publish one partition of ``frame`` to ``dataset``'s table.

    The frame is whatever the asset already writes as parquet - a DataFrame or
    a GeoDataFrame - and nothing about its shape has to be declared here: the
    target's own columns are read from the catalog and matched to it by name,
    the geometry column travels as hex EWKB, and anything left over goes to the
    table's jsonb catch-all if it has one and is dropped if it does not.

    ``neighborhood`` and ``scrape_date`` are written onto every row rather than
    taken from the frame. They are the partition being published, and a frame
    that disagrees with its own partition key - a stale file, a mis-set
    dependency - would otherwise write rows into a partition it was not asked
    to write, where the prune below would not see them.

    Runs in ``connection``'s transaction and does not commit: the caller
    decides what else belongs in the same one. Returns the counts the asset
    reports - how many rows were COPYed, upserted and pruned.
    """
    table = table_for(dataset)
    cursor = connection.cursor()
    require_table(cursor, table)
    ensure_partition(cursor, table, neighborhood=neighborhood, scrape_date=scrape_date)

    target = _target_columns(cursor, table)
    staging = _create_staging(cursor, table)
    selected, leftover = _match_columns(table, target, frame)
    _require_key_columns(table, [name for name, _ in selected], frame)
    copied = _copy_frame(
        cursor,
        staging,
        table,
        selected,
        leftover,
        frame,
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )
    return _row_counts(
        copied,
        _merge(
            cursor,
            table,
            staging,
            [name for name, _ in selected],
            neighborhood=neighborhood,
            scrape_date=scrape_date,
            prune=prune,
        ),
    )


def upsert_select(
    cursor: "Cursor",
    dataset: str,
    columns: Sequence[str],
    select: str,
    params: Any = None,
    *,
    neighborhood: str,
    scrape_date: str,
    prune: bool = True,
) -> dict[str, int]:
    """Publish one partition computed *in the database* to ``dataset``'s table.

    The three PostGIS joins are computed where their inputs already sit loaded
    and GiST-indexed, so their rows never pass through Python and there is no
    frame to COPY. ``select`` is the statement that produces them, ``columns``
    the target columns it produces, in the order it produces them.

    It still lands in a staging table first rather than inserting straight into
    the target, and for the reason the module docstring gives: the prune has to
    know which keys this run produced, and a bare `INSERT ... SELECT` leaves
    nothing behind to ask.
    """
    table = table_for(dataset)
    require_table(cursor, table)
    ensure_partition(cursor, table, neighborhood=neighborhood, scrape_date=scrape_date)

    staging = _create_staging(cursor, table)
    _require_key_columns(table, columns)
    cursor.execute(f"INSERT INTO {staging} ({', '.join(columns)}) {select}", params)
    copied = max(cursor.rowcount, 0)
    return _row_counts(
        copied,
        _merge(
            cursor,
            table,
            staging,
            list(columns),
            neighborhood=neighborhood,
            scrape_date=scrape_date,
            prune=prune,
        ),
    )


def publish(
    connect: Any,
    datasets: Mapping[str, Any],
    *,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, dict[str, int]]:
    """Upsert several frames in one transaction, keyed by dataset name.

    What a parquet-first asset calls once it has written its files: the two
    CMHC pairs, the two zoning tables, the chunks. One transaction for all of
    them, so a borough's averages and the quartier rows they were taken over
    are never visible apart.

    ``connect`` is `PostgisResource.connect` - the *method*, not a connection.
    Taking it that way is what keeps this module from importing
    `urban_rag.resources`, which imports `urban_rag.postgis`, which imports
    this one.

    Called after the parquet is written, and it raises rather than warning if
    the database is unreachable. The asset really does publish to Postgres, so
    a run that could not is a failed run - and because the files are already in
    the tree by then, the re-run costs a load rather than a scrape.
    """
    with connect() as connection:
        return {
            name: upsert_frame(
                connection,
                name,
                frame,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
            )
            for name, frame in datasets.items()
        }


def publish_by_neighborhood(
    connect: Any,
    dataset: str,
    frames: Mapping[str, Any],
    *,
    scrape_date: str,
) -> dict[str, dict[str, int]]:
    """Upsert one dataset's boroughs in one transaction, keyed by borough.

    The mirror of `publish`, which writes several datasets into one partition;
    this writes one dataset into several partitions. It is what a silver asset
    computed *once for the province* needs: Quebec publishes one assessment
    roll, `assessment_units` merges it once under one date partition, and the
    borough each unit belongs to is a property of where its point falls rather
    than of which run produced it - so one materialization has seventeen
    borough partitions to publish out of it.

    One transaction for all of them, for the reason `publish` gives: a reader
    querying across boroughs should see the island as it was or as it now is,
    not half of each. Every borough is still pruned against its own frame
    alone, so a unit that has moved out of one borough's boundary between
    scrapes is dropped from that partition and not from the one it moved into.

    ``connect`` is `PostgisResource.connect` - the *method*, not a connection,
    for the import-cycle reason `publish` documents.
    """
    with connect() as connection:
        return {
            neighborhood: upsert_frame(
                connection,
                dataset,
                frame,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
            )
            for neighborhood, frame in frames.items()
        }


def published_metadata(results: Mapping[str, dict[str, int]]) -> dict[str, int]:
    """`publish`'s counts, flattened into the metadata an asset reports.

    `<dataset>_rows_upserted` and, only when it is non-zero,
    `<dataset>_rows_pruned` - a prune of 0 is the steady state and says
    nothing, while a prune of 4 000 on a re-run is the borough's cadastre
    having moved under the partition.
    """
    metadata: dict[str, int] = {}
    for name, counts in results.items():
        metadata[f"{name}_rows_upserted"] = counts["upserted"]
        for key in ("pruned", "duplicates"):
            if counts.get(key):
                metadata[f"{name}_rows_{key}"] = counts[key]
    return metadata


def require_table(cursor: "Cursor", table: Table) -> None:
    """Raise `MissingRelation` naming the file to apply, if it is not there."""
    cursor.execute("SELECT to_regclass(%s)", [table.qualified])
    (resolved,) = cursor.fetchone()
    if resolved is None:
        raise MissingRelation(
            f"hbu_infra has not created {table.qualified} ({table.source}) - "
            "apply it with `./scripts/db.py init` in that repo, then re-run "
            "this partition."
        )


def _require_key_columns(
    table: Table, writing: Sequence[str], frame: Any = None
) -> None:
    """Every column the write conflicts on has to be one the write provides.

    Without this the failure is `null value in column "cote_rue_id" violates
    not-null constraint`, three statements later and naming neither the frame
    nor the column it should have come from - and the usual cause is banal: a
    source that renamed a column, or an entry missing from ``Table.columns``.
    """
    missing = [name for name in table.conflict_columns if name not in set(writing)]
    if not missing:
        return
    available = (
        ""
        if frame is None
        else f" The frame has: {', '.join(str(c) for c in frame.columns)}."
    )
    raise ValueError(
        f"{table.qualified} conflicts on {', '.join(table.conflict_columns)}, "
        f"but nothing supplies {', '.join(missing)}."
        + available
        + " Map it in urban_rag.warehouse.TABLES if the source spells it "
        "differently."
    )


def ensure_partition(
    cursor: "Cursor", table: Table, *, neighborhood: str, scrape_date: str
) -> None:
    """Create this partition's leaf, if the borough or the month is new.

    Cheap enough to call on every load - it is two catalog lookups when the
    leaf is already there - and the alternative is a partition set an operator
    has to remember to extend before each new month.
    """
    cursor.execute(
        "SELECT warehouse.ensure_partition(%s::regclass, %s, %s::date)",
        [table.qualified, neighborhood, scrape_date],
    )


def conflict_clause(table: Table, columns: Iterable[str]) -> str:
    """The `ON CONFLICT ... DO UPDATE` clause for ``table``.

    Every written column except the key is overwritten from `EXCLUDED`, and
    `loaded_at` is set to `now()` rather than to the staging row's own default,
    so the column says when this row was last *published* rather than when the
    staging table happened to be filled.
    """
    conflict = set(table.conflict_columns)
    assignments = [
        f"{name} = EXCLUDED.{name}"
        for name in columns
        if name not in conflict and name != "loaded_at"
    ]
    assignments.append("loaded_at = now()")
    return (
        f"ON CONFLICT ({', '.join(table.conflict_columns)}) DO UPDATE SET "
        + ", ".join(assignments)
    )


# --------------------------------------------------------------------------
# internals
# --------------------------------------------------------------------------


def _merge(
    cursor: "Cursor",
    table: Table,
    staging: str,
    columns: Sequence[str],
    *,
    neighborhood: str,
    scrape_date: str,
    prune: bool,
) -> dict[str, int]:
    """Upsert ``staging`` into ``table``, then drop what it no longer holds.

    `DISTINCT ON` is the one thing here that is not a plain merge. A staging
    table holding the same key twice would fail the upsert outright - `ON
    CONFLICT DO UPDATE command cannot affect row a second time` - where the
    loaders this replaces resolved a repeat with `ON CONFLICT ... DO NOTHING`
    and carried on. Infolot really does answer a boundary query with the same
    lot twice when a borough outline is a multipolygon, so the tolerance has to
    stay; `ORDER BY` the same columns is what makes which copy wins the same on
    every run rather than whatever the scan happened to reach first. The count
    that was dropped is reported rather than swallowed - `copied` minus
    `upserted` - since a duplicate is usually a symptom of something upstream.
    """
    column_list = ", ".join(columns)
    conflict_list = ", ".join(table.conflict_columns)
    cursor.execute(
        f"INSERT INTO {table.qualified} ({column_list}) "
        f"SELECT DISTINCT ON ({conflict_list}) {column_list} FROM {staging} "
        f"ORDER BY {conflict_list} "
        + conflict_clause(table, columns)
    )
    upserted = max(cursor.rowcount, 0)

    pruned = 0
    if prune:
        # IS NOT DISTINCT FROM rather than `=`: a key column is NOT NULL in
        # every table here, but a staging row that somehow carries NULL would
        # match nothing under `=` and silently prune the row it stands for.
        match = " AND ".join(
            f"s.{name} IS NOT DISTINCT FROM t.{name}" for name in table.keys
        )
        cursor.execute(
            f"DELETE FROM {table.qualified} t "
            "WHERE t.neighborhood = %s AND t.scrape_date = %s::date "
            f"AND NOT EXISTS (SELECT 1 FROM {staging} s WHERE {match})",
            [neighborhood, scrape_date],
        )
        pruned = max(cursor.rowcount, 0)

    return {"upserted": upserted, "pruned": pruned}


def _row_counts(copied: int, merged: dict[str, int]) -> dict[str, int]:
    """The counts an asset reports, with the dropped duplicates named."""
    return {"copied": copied, "duplicates": copied - merged["upserted"], **merged}


def _create_staging(cursor: "Cursor", table: Table) -> str:
    """A temp table shaped like ``table``, dropped at the end of the transaction.

    `LIKE` rather than a column list of our own, so the staging table's types
    are the target's types and Postgres does the parsing: a text COPY into it
    turns `2026-08-26` into a date and a hex EWKB string into a geometry
    without this module owning a cast for either.

    Dropped up front rather than left to `ON COMMIT DROP` alone: one asset
    publishes two datasets inside one transaction, and a re-entry would
    otherwise meet the previous call's table still holding its rows.
    """
    staging = f"{table.schema}_{table.name}_load"
    cursor.execute(f"DROP TABLE IF EXISTS {staging}")
    cursor.execute(
        f"CREATE TEMP TABLE {staging} (LIKE {table.qualified} INCLUDING DEFAULTS) "
        "ON COMMIT DROP"
    )
    return staging


def _target_columns(cursor: "Cursor", table: Table) -> list[str]:
    """``table``'s own columns, in catalog order."""
    cursor.execute(
        "SELECT attname FROM pg_attribute "
        "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped "
        "ORDER BY attnum",
        [table.qualified],
    )
    return [name for (name,) in cursor.fetchall()]


def _match_columns(
    table: Table, target: Sequence[str], frame: Any
) -> tuple[list[tuple[str, str | None]], list[str]]:
    """Pair each target column with the frame column that fills it.

    Returns the `(target column, frame column)` pairs and, separately, the
    frame columns none of them claimed - which is what the jsonb catch-all
    carries. The frame column is None for the two partition columns, written
    from the partition key rather than read from the frame, and for the
    geometry and the catch-all, which are assembled rather than copied.

    A target column no frame column matches is left out of the write entirely
    rather than written NULL, so the table's own default still applies and a
    column added in hbu_infra ahead of the code that fills it is not
    overwritten with nothing.
    """
    columns = [str(name) for name in frame.columns]
    lookup = {name.lower(): name for name in columns}
    geometry_source = _geometry_column(frame)
    taken: set[str] = set()
    matched: list[tuple[str, str | None]] = []

    for name in target:
        if name in PARTITION_COLUMNS:
            # Written from the partition key, and the frame's own copy is
            # claimed so it does not also land in the jsonb catch-all - where
            # it would be a second, unpruned answer to which partition this is.
            claimed = lookup.get(name)
            if claimed is not None:
                taken.add(claimed)
            matched.append((name, None))
            continue
        if table.geometry and name == table.geometry:
            if geometry_source is not None:
                matched.append((name, None))
            continue
        if table.attributes and name == table.attributes:
            continue  # appended last, once the rest have claimed their columns
        source = table.columns.get(name) or lookup.get(name.lower())
        if source is None or source not in columns:
            continue
        taken.add(source)
        matched.append((name, source))

    leftover = [
        name for name in columns if name not in taken and name != geometry_source
    ]
    if table.attributes and table.attributes in target:
        matched.append((table.attributes, None))
    return matched, leftover


def _copy_frame(
    cursor: "Cursor",
    staging: str,
    table: Table,
    selected: Sequence[tuple[str, str | None]],
    leftover: Sequence[str],
    frame: Any,
    *,
    neighborhood: str,
    scrape_date: str,
) -> int:
    """COPY ``frame`` into ``staging``, one row per row that has a geometry.

    Text format rather than binary, unlike the loaders this replaced. Binary
    COPY dumps each value by its *Python* type, which means writing this
    generically would mean owning an adapter per column type it might meet - a
    date that arrives as a string fails there with `descriptor 'toordinal' ...
    doesn't apply to a 'str' object`, naming neither the column nor the
    partition. In text format every value is written as text and parsed by the
    column's own input function, so the staging table's types - which are the
    target's, via `LIKE` - decide what each string means. Geometry rides the
    same road: the `geometry` input function accepts hex EWKB, so nothing has
    to call `ST_GeomFromWKB` on the other side.
    """
    if len(frame) == 0:
        return 0

    geometry_source = _geometry_column(frame)
    names = [name for name, _ in selected]
    copied = 0
    statement = f"COPY {staging} ({', '.join(names)}) FROM STDIN"
    with cursor.copy(statement) as copy:
        for record in frame.to_dict(orient="records"):
            geometry = record.get(geometry_source) if geometry_source else None
            if table.geometry and (geometry is None or geometry.is_empty):
                # A row with no shape is not a row of a spatial table: it
                # cannot be joined, clipped or drawn, and writing it would put
                # a hole in every measure taken over the partition.
                continue
            values: list[str | None] = []
            for name, source in selected:
                if name == "neighborhood":
                    values.append(neighborhood)
                elif name == "scrape_date":
                    values.append(str(scrape_date))
                elif table.geometry and name == table.geometry:
                    values.append(_as_ewkb(geometry))
                elif table.attributes and name == table.attributes:
                    values.append(
                        json.dumps(
                            {key: _as_json(record.get(key)) for key in leftover},
                            ensure_ascii=False,
                        )
                    )
                else:
                    values.append(_as_text(record.get(source)))
            copy.write_row(values)
            copied += 1
    return copied


def _geometry_column(frame: Any) -> str | None:
    """The frame's geometry column, or None when it is a plain DataFrame."""
    try:
        return str(frame.geometry.name)
    except (AttributeError, TypeError, ValueError):
        return None


def _as_ewkb(geometry: Any) -> str | None:
    """A shapely geometry as the hex EWKB Postgres' `geometry` type parses."""
    if geometry is None:
        return None
    import shapely.wkb

    return shapely.wkb.dumps(geometry, hex=True, srid=4326)


def _as_text(value: Any) -> str | None:
    """One cell as the text a COPY writes, with a missing value left missing.

    `str(nan)` is `"nan"`, which a double precision column happily accepts as
    NaN and a text column stores as the word - a street called "nan" rather
    than the absent name it is. Every flavour of missing this pipeline produces
    (None, `nan`, `NaT`, pandas' NA) has to become NULL here, and only here.

    The missing-value test runs *before* the date branch and not after it, and
    that ordering is the whole point of putting it in the middle: `pd.NaT` is
    an instance of `datetime`, and `pd.NaT.isoformat()` is the string `"NaT"` -
    which a `timestamptz` column rejects outright, naming neither the row nor
    the column it came from.
    """
    if value is None:
        return None
    if isinstance(value, float):
        # `float(value)` before `repr`, and not for style: `numpy.float64`
        # *is* a Python float by isinstance, and its repr under numpy 2 is
        # `np.float64(1.5)` - which a double precision column rejects. Narrowed
        # first, the repr is the shortest string that round-trips.
        if math.isnan(value):
            return None
        number = float(value)
        # An integral float is written without its `.0`, because the column it
        # is bound for may well be an integer one: a count that pandas widened
        # to float64 on a left join - `num_frontages` for a lot facing no
        # street - reaches an `integer` column as "1.0" and is refused with
        # `invalid input syntax for type integer`. "1" is what both column
        # types accept, and `double precision` parses it to the same value, so
        # narrowing here costs a float column nothing. A non-integral float
        # bound for an integer column is a real disagreement about the schema
        # and still fails, naming the column.
        if number.is_integer():
            return str(int(number))
        return repr(number)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(_as_json(value), ensure_ascii=False)

    import pandas as pd

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):  # arrays, and anything else pandas cannot test
        pass

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item") and getattr(value, "shape", None) == ():
        return _as_text(value.item())
    return str(value)


def _as_json(value: Any) -> Any:
    """One cell as something `json.dumps` accepts.

    The jsonb catch-all takes whatever a layer publishes, which for Spectrum is
    "loosely typed" - numpy scalars, timestamps, and (where `urban_rag.frames`
    JSON-encoded a nested cell on the way to parquet) a JSON *string*. That
    last one is re-parsed rather than stored as a quoted string, so a column
    that was a list upstream is a list in `attributes` too.
    """
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, bool):
        return value
    if hasattr(value, "item") and getattr(value, "shape", None) == ():
        return _as_json(value.item())
    if isinstance(value, (int, float)):
        return None if isinstance(value, float) and math.isnan(value) else value
    text = _as_text(value)
    if text is None:
        return None
    if text[:1] in "[{":
        try:
            return json.loads(text)
        except ValueError:
            return text
    return text
