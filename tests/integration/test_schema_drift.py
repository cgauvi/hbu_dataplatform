"""Every warehouse table's declared columns against the ones it actually has.

This exists because of a failure that took three runs to identify and was not,
at any point, legible as what it was.

`gold.lot_profiles` failed to materialize with ``column
assessed.num_shared_units does not exist``. That reads as a bug in the profile
query. It was not: `silver.lot_assessed_values` on the dev database had nine
columns where sql/013 declares twelve, because those three were added to the
file's ``CREATE TABLE IF NOT EXISTS`` *after* the table's first release - and
that statement is a no-op on a database which already has the table. Every
`db.py init` since had reported "24 file(s) applied" and changed nothing.

**So the drift is silent by construction, and it is silent for as long as
nothing selects the column.** hbu_infra's answer is an explicit
``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` block per widening - sql/009,
sql/013, sql/016, sql/019 and sql/021 all carry one - and the failure mode is
simply forgetting to write it. Nothing checked, which is what this module is.

**It is a check on a deployment, not on the files.** A freshly created database
matches its DDL by definition, so this cannot be made to fail on a throwaway:
point it at one and every table is absent and skipped, which is a pass that
says nothing. Its value is against a database that has been upgraded in place -
the dev RDS through the SSM tunnel, or prod - where it answers "is this
database actually the shape the code expects" before a chain runs, rather than
an hour in, as a query error naming a column nobody thought was optional::

    URBAN_RAG_TEST_PG_URL=postgresql://... uv run pytest tests/integration/test_schema_drift.py

Unlike the other modules here it applies no schema and writes nothing, so it is
the one test in this directory that is safe to point at a database you care
about.
"""

from __future__ import annotations

import os
import pathlib
import re

import pytest

from urban_rag.warehouse import TABLES, Table

psycopg = pytest.importorskip("psycopg")

from conftest import DSN_ENV, INFRA_ENV, _infra_sql_dir  # noqa: E402

#: Words that begin a table constraint rather than a column. A `CREATE TABLE`
#: body is a comma-separated list of both, and only the second kind has a name
#: this module should be looking for.
_CONSTRAINTS = (
    "PRIMARY KEY",
    "UNIQUE",
    "CONSTRAINT",
    "FOREIGN KEY",
    "CHECK",
    "EXCLUDE",
    "LIKE",
)

#: One `CREATE TABLE IF NOT EXISTS <qualified> ( ... )`, captured by name.
#: Non-greedy to the first line that closes the body at column 0, because
#: several of these files declare more than one table and a greedy match would
#: hand the first table every later one's columns - which is exactly the false
#: positive that made the first draft of this check unreadable.
_CREATE = re.compile(
    r"CREATE TABLE IF NOT EXISTS\s+([\w.]+)\s*\((.*?)^\)", re.S | re.M
)

#: One widening. The whole point of the convention this module checks.
_ADD_COLUMN = re.compile(
    r"ALTER TABLE\s+([\w.]+)(.*?);", re.S | re.I
)
_ADDED = re.compile(r"ADD COLUMN IF NOT EXISTS\s+(\w+)", re.I)


def _body_columns(body: str) -> set[str]:
    """The column names in a `CREATE TABLE` body.

    Split on top-level commas rather than on newlines, which is the whole of
    the difficulty: a column may run over several lines -

        lot_uid bigint NOT NULL
            REFERENCES rag.lots (lot_uid) ON DELETE CASCADE,

    - and a line-wise reading calls the second line a column named REFERENCES.
    Depth tracking is what keeps the comma inside `PRIMARY KEY (a, b)` and
    inside `numeric(12, 2)` from splitting an item in half.
    """
    stripped = "\n".join(line.split("--", 1)[0] for line in body.splitlines())

    items: list[str] = []
    depth = 0
    current: list[str] = []
    for char in stripped:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            items.append("".join(current))
            current = []
        else:
            current.append(char)
    items.append("".join(current))

    names = set()
    for item in items:
        item = item.strip()
        if not item or item.upper().startswith(_CONSTRAINTS):
            continue
        names.add(item.split()[0].strip('",'))
    return names


def _declared_columns(infra: pathlib.Path, table: Table) -> set[str]:
    """Every column ``table``'s source file says it has.

    The `CREATE` body plus every `ADD COLUMN IF NOT EXISTS` aimed at the same
    table, which together are the shape a database is supposed to end up in
    however it got there.
    """
    text = (infra / table.source).read_text(encoding="utf-8")
    columns: set[str] = set()

    for name, body in _CREATE.findall(text):
        if name == table.qualified:
            columns.update(_body_columns(body))

    for name, body in _ADD_COLUMN.findall(text):
        if name == table.qualified:
            columns.update(_ADDED.findall(body))

    return columns


@pytest.fixture(scope="module")
def live(request):
    """A connection to the configured database, exactly as it stands.

    Deliberately applies nothing. Every other fixture in this directory brings
    a throwaway up to date first, which is right for a test of a measure and
    wrong for a test of a deployment: applying the schema here would heal the
    drift on the way in and the check could never fail.
    """
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        pytest.skip(f"{DSN_ENV} is not set - see tests/integration/conftest.py")
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="module")
def infra():
    """hbu_infra's root, not its `sql/`.

    `Table.source` already carries the `sql/` prefix - it is written to be read
    against the repository, not against the directory - so joining it to
    `_infra_sql_dir()` would ask for `sql/sql/014_...`.
    """
    directory = _infra_sql_dir()
    if not directory.is_dir():
        pytest.skip(f"hbu_infra sql/ not found at {directory} - set {INFRA_ENV}")
    return directory.parent


def _live_columns(connection, table: Table) -> set[str]:
    cursor = connection.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        [table.schema, table.name],
    )
    return {row[0] for row in cursor.fetchall()}


@pytest.mark.parametrize("key", sorted(TABLES), ids=sorted(TABLES))
def test_the_live_table_has_every_column_its_file_declares(key, live, infra):
    """The check itself, one table at a time so a failure names the table.

    A table that is absent is skipped rather than failed: this same module runs
    against a throwaway that holds only what another test applied, and "the
    database has not been initialised" is a different fact from "the database
    is out of date".
    """
    table = TABLES[key]
    have = _live_columns(live, table)
    if not have:
        pytest.skip(f"{table.qualified} does not exist on this database")

    want = _declared_columns(infra, table)
    assert want, f"no columns parsed out of {table.source} for {table.qualified}"

    missing = sorted(want - have)
    assert not missing, (
        f"{table.qualified} is missing {', '.join(missing)}. "
        f"{table.source} declares them, but CREATE TABLE IF NOT EXISTS does "
        "nothing to a table that already exists - add an "
        "ALTER TABLE ... ADD COLUMN IF NOT EXISTS block to that file and "
        "re-run `make db-init`."
    )


def test_every_widened_column_is_also_declared_on_the_table_it_alters(infra):
    """An `ADD COLUMN` aimed at a table the file does not create.

    Cheap, needs no database, and catches the copy-paste that puts a widening
    block under the wrong table - which would apply cleanly, add the column
    somewhere nothing reads it, and leave the intended table untouched.
    """
    stray: list[str] = []
    for key, table in sorted(TABLES.items()):
        text = (infra / table.source).read_text(encoding="utf-8")
        created = {name for name, _ in _CREATE.findall(text)}
        for name, body in _ADD_COLUMN.findall(text):
            if not _ADDED.findall(body):
                continue
            if name not in created:
                stray.append(f"{table.source}: ALTER TABLE {name}")
    assert not stray, "widening blocks on tables their file never creates: " + (
        "; ".join(sorted(set(stray)))
    )


# -- the parser ------------------------------------------------------------
#
# No database, so these run wherever the suite does. They pin the two readings
# that were wrong in this module's own first drafts, both of which produced
# confident false positives rather than errors - which on a check like this is
# the worst failure available, since it teaches the reader to ignore it.


def test_a_column_that_runs_over_two_lines_is_one_column():
    """The reading that reported three tables missing a column `REFERENCES`."""
    body = """
    lot_uid          bigint NOT NULL
        REFERENCES rag.lots (lot_uid) ON DELETE CASCADE,
    cote_rue_id      text NOT NULL,
    PRIMARY KEY (scrape_date, neighborhood, lot_uid, cote_rue_id)
    """

    assert _body_columns(body) == {"lot_uid", "cote_rue_id"}


def test_a_comma_inside_parentheses_does_not_split_an_item():
    body = """
    total_assessed_value numeric(12, 2),
    geom geometry(Geometry, 4326),
    PRIMARY KEY (scrape_date, neighborhood, lot_number)
    """

    assert _body_columns(body) == {"total_assessed_value", "geom"}


def test_a_trailing_comment_is_not_read_as_a_column():
    body = """
    lot_number text NOT NULL, -- NO_LOT in the published cadastre
    roll_year  integer
    """

    assert _body_columns(body) == {"lot_number", "roll_year"}


def test_a_file_declaring_several_tables_gives_each_only_its_own_columns(infra):
    """The reading that credited the first table in a file with every later
    table's columns, and so reported four CMHC tables as drifted at once."""
    vacancy = _declared_columns(infra, TABLES["vacancy_rates"])
    quartier = _declared_columns(infra, TABLES["quartier_vacancy_rates"])

    assert vacancy and quartier
    # Both live in sql/010 and they are not the same table: the quartier rows
    # are the survey as published, the other is the borough average over them.
    assert "num_quartiers" in vacancy
    assert "num_quartiers" not in quartier


def test_the_widened_columns_count_as_declared(infra):
    """A column that only ever arrives by ALTER is still one the table has -
    and is exactly the kind this check exists to notice the absence of."""
    columns = _declared_columns(infra, TABLES["lot_assessed_values"])

    assert "total_assessed_value_apportioned" in columns
    assert "num_shared_units" in columns
    assert "num_units_by_point" in columns
