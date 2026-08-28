"""The shared vector store: Postgres on RDS, indexed by the `pgvector` extension.

`rag.store` holds the same corpus in a DuckDB file, which is the right shape for
one laptop and the wrong one for a deployment: the file takes an exclusive write
lock, lives on whatever disk the process happens to have, and is rebuilt in full
to change. This module is the alternative for when the pipeline runs on ECS and
something else - an API, the map, a second Dagster run - has to read the vectors
while they are being written.

Loading is still a load, not a computation. `document_embeddings` has already
paid for the fetching, chunking and embedding, so a partition is streamed
straight out of `embeddings.parquet` into Postgres with binary `COPY`, upserted
on `chunk_id`, and searched over an HNSW index.

Two things the code cannot do for itself, both of which need a role this
pipeline should not have:

- **the database.** Create the RDS instance and the `urban_rag` database first;
  every entry point here connects to something that already exists.
- **the extension.** `CREATE EXTENSION vector` requires `rds_superuser`, so an
  admin runs hbu_infra's `make db-bootstrap` and `make db-init` once per
  database - the role, the schemas and the extensions all come from there.
  `ensure_schema` attempts it anyway (harmless, and it is what makes a local
  dev container work unattended), and reports the privilege failure for what
  it is.

Everything else - schema, tables, indexes - is created on first load.

Configured from `URBAN_RAG_PG_*` (see `PgSettings.from_env`) or from the
`PgVectorResource` in the Dagster code location. Credentials are resolved at
connect time, in this order: an explicit password, a Secrets Manager secret, an
IAM auth token, then libpq's own `PGPASSWORD`/`~/.pgpass`.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from urban_rag.rag.results import COLUMNS, SCHEMA_VERSION, Hit, IndexMismatch
from urban_rag.storage import AWS_PROFILE, filesystem, is_s3_uri, storage_options

if TYPE_CHECKING:  # pragma: no cover - typing only, psycopg is imported lazily
    from psycopg import Connection, Cursor

#: Prefix for every environment variable this module reads.
ENV_PREFIX = "URBAN_RAG_PG_"

DEFAULT_PORT = 5432
DEFAULT_DATABASE = "urban_rag"
DEFAULT_USER = "urban_rag"
#: A schema of its own, not `public`: the corpus is one tenant of a database
#: that may well end up holding the borough tables too.
DEFAULT_SCHEMA = "rag"
DEFAULT_TABLE = "chunks"

#: What RDS signs its certificates with. `sslmode=verify-full` needs it on disk;
#: without it libpq falls back to `~/.postgresql/root.crt` and fails there.
RDS_CA_BUNDLE_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"

#: HNSW build parameters. pgvector's defaults (16/64) are a reasonable point on
#: the recall/build-time curve for a corpus this size; raise `ef_construction`
#: before `m` if recall matters more than the minutes a rebuild takes.
DEFAULT_M = 16
DEFAULT_EF_CONSTRUCTION = 64
#: Search-time candidate list. pgvector defaults to 40, which silently caps
#: recall once `k` approaches it - and a filtered search discards candidates
#: *after* the index returns them, so both `k` and the filters push this up.
DEFAULT_EF_SEARCH = 100

#: The COPY column list, and the type psycopg dumps each one as. Binary COPY
#: carries no type information of its own, so the two are declared together and
#: must stay in step with `_create_table`.
_COPY_COLUMNS = (*COLUMNS, "embedding")
_COPY_TYPES = (
    "text",  # chunk_id
    "text",  # doc_id
    "text",  # url
    "text",  # title
    "text",  # source_table
    "text",  # neighborhood
    "date",  # scrape_date
    "int4",  # chunk_index
    "int4",  # num_tokens
    "jsonb",  # feature_ids
    "text",  # model
    "text",  # text
    "vector",  # embedding
)
assert len(_COPY_COLUMNS) == len(_COPY_TYPES)

#: Columns an upsert overwrites: everything but the key it conflicted on.
_UPDATED_COLUMNS = tuple(column for column in _COPY_COLUMNS if column != "chunk_id")

#: The search's select list: `COLUMNS`, with the two that Postgres stores in a
#: richer type than `Hit` takes cast back to the text the parquet carried.
_SELECT_LIST = ", ".join(
    {"scrape_date": "scrape_date::text", "feature_ids": "feature_ids::text"}.get(
        column, column
    )
    for column in COLUMNS
)

#: Schema and table names are interpolated into DDL, which takes no parameters.
#: Validated on the way in rather than quoted, the way `storage.AWS_PROFILE` is.
_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_PASSWORD_IN_URI = re.compile(r"://[^/@]*@")


class PostgresUnavailable(RuntimeError):
    """The store could not be reached, or the driver is not installed."""


@dataclass(frozen=True)
class PgSettings:
    """Where the store is, how to authenticate to it, and how hard to search it.

    ``host`` may be left unset, in which case libpq's own `PGHOST`/`PGPORT`
    environment applies; ``dsn`` short-circuits all of it with a full
    connection string, which is what a local `pgvector/pgvector` container is
    easiest to reach with.
    """

    host: str | None = None
    port: int = DEFAULT_PORT
    database: str = DEFAULT_DATABASE
    user: str = DEFAULT_USER
    password: str | None = None
    #: Secrets Manager secret holding `{"username": ..., "password": ...}` -
    #: the shape RDS writes when it manages the password itself.
    secret_id: str | None = None
    #: Sign the connection with an IAM auth token instead of a password. The
    #: database role needs `GRANT rds_iam`, see hbu_infra sql/000_roles.sql.
    iam_auth: bool = False
    region: str | None = None
    #: `verify-full` is the only mode that authenticates the server it is
    #: talking to; a dev container with no TLS at all needs `disable`.
    sslmode: str = "verify-full"
    sslrootcert: str | None = None
    dsn: str | None = None
    db_schema: str = DEFAULT_SCHEMA
    table: str = DEFAULT_TABLE
    connect_timeout: int = 10
    #: Generous: a rebuild's index build runs inside one statement.
    statement_timeout_seconds: int = 1800
    maintenance_work_mem: str = "512MB"
    ef_search: int = DEFAULT_EF_SEARCH

    def __post_init__(self) -> None:
        for name in ("db_schema", "table"):
            value = getattr(self, name)
            if not _VALID_IDENTIFIER.match(value):
                raise ValueError(f"{name}={value!r} is not a valid SQL identifier")

    @classmethod
    def from_env(cls, **overrides: Any) -> "PgSettings":
        """Settings from `URBAN_RAG_PG_*`, with non-None ``overrides`` on top."""
        environment = {
            "dsn": _env("DSN"),
            "host": _env("HOST"),
            "port": _int(_env("PORT")),
            "database": _env("DATABASE"),
            "user": _env("USER"),
            "password": _env("PASSWORD"),
            "secret_id": _env("SECRET_ID"),
            "iam_auth": _flag(_env("IAM_AUTH")),
            "region": _env("REGION")
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION"),
            "sslmode": _env("SSLMODE"),
            "sslrootcert": _env("SSLROOTCERT") or os.environ.get("PGSSLROOTCERT"),
            "db_schema": _env("SCHEMA"),
            "table": _env("TABLE"),
            "ef_search": _int(_env("EF_SEARCH")),
        }
        settings = {k: v for k, v in environment.items() if v is not None}
        settings.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**settings)

    # -- connecting --------------------------------------------------------

    def connection_kwargs(self) -> dict[str, Any]:
        """What `psycopg.connect` is called with, credentials resolved now."""
        options = f"-c statement_timeout={int(self.statement_timeout_seconds) * 1000}"
        if self.dsn:
            return {
                "conninfo": self.dsn,
                "options": options,
                "application_name": "urban_rag",
            }

        user, password = self.credentials()
        self._check_root_cert()
        keywords = {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": user,
            # None rather than "": libpq then falls back to PGPASSWORD and
            # ~/.pgpass, which is how a psql-configured laptop already works.
            "password": password,
            "sslmode": self.sslmode,
            "sslrootcert": self.sslrootcert,
            "connect_timeout": self.connect_timeout,
            "application_name": "urban_rag",
            "options": options,
            # A load reaching RDS through an SSM port-forward has no way to
            # notice that tunnel dying: Session Manager leaves the local
            # listener bound, so the socket stays open and a COPY of a
            # borough's cadastre blocks on it forever - no error, no timeout,
            # because `statement_timeout` only starts once the server has the
            # query. These make the kernel probe an idle connection and give
            # up after ~1 min, turning a silent overnight hang into a
            # connection error the asset reports and a re-run fixes.
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
        }
        return {k: v for k, v in keywords.items() if v is not None}

    def credentials(self) -> tuple[str, str | None]:
        """The user and password to connect as.

        Resolved per connection rather than cached: an IAM auth token is signed
        for fifteen minutes, and a long-lived resource would hand out an expired
        one on its second run.
        """
        if self.password:
            return self.user, self.password
        if self.secret_id:
            return self._from_secrets_manager()
        if self.iam_auth:
            return self.user, self._auth_token()
        return self.user, None

    def _from_secrets_manager(self) -> tuple[str, str]:
        client = _boto_client("secretsmanager", self.region)
        try:
            payload = client.get_secret_value(SecretId=self.secret_id)["SecretString"]
            secret = json.loads(payload)
            return str(secret.get("username", self.user)), str(secret["password"])
        except Exception as exc:
            raise PostgresUnavailable(
                f"Could not read the database password from Secrets Manager "
                f"({self.secret_id!r}): {exc}"
            ) from exc

    def _auth_token(self) -> str:
        if not self.host:
            raise PostgresUnavailable(
                "IAM authentication needs the instance endpoint; set "
                f"{ENV_PREFIX}HOST (an auth token is signed for one host)."
            )
        client = _boto_client("rds", self.region)
        try:
            return client.generate_db_auth_token(
                DBHostname=self.host,
                Port=self.port,
                DBUsername=self.user,
                Region=client.meta.region_name,
            )
        except Exception as exc:
            raise PostgresUnavailable(
                f"Could not sign an RDS IAM auth token for {self.user}@{self.host}: "
                f"{exc}"
            ) from exc

    def _check_root_cert(self) -> None:
        """Fail before connecting when `verify-full` has nothing to verify with.

        libpq's own message for this names a file the reader has never heard of
        (`~/.postgresql/root.crt`), so it is worth pre-empting.
        """
        if self.sslmode not in ("verify-ca", "verify-full"):
            return
        candidate = self.sslrootcert or os.path.expanduser("~/.postgresql/root.crt")
        if os.path.exists(candidate):
            return
        raise PostgresUnavailable(
            f"sslmode={self.sslmode} needs the CA that signed the server's "
            f"certificate, and {candidate} does not exist.\n"
            f"  curl -o ~/.postgresql/root.crt --create-dirs {RDS_CA_BUNDLE_URL}\n"
            f"or point {ENV_PREFIX}SSLROOTCERT at the bundle. Set "
            f"{ENV_PREFIX}SSLMODE=require to encrypt without authenticating the "
            "server (weaker: it does not stop a redirected endpoint)."
        )

    # -- naming ------------------------------------------------------------

    @property
    def qualified_table(self) -> str:
        return f"{self.db_schema}.{self.table}"

    @property
    def safe_target(self) -> str:
        """The endpoint, with any password in a DSN blanked out."""
        if self.dsn:
            return _PASSWORD_IN_URI.sub("://***@", self.dsn)
        return f"{self.host or os.environ.get('PGHOST', 'localhost')}:{self.port}"


class PgVectorStore:
    """Embedded chunks in Postgres, searched by cosine distance over HNSW.

    The same surface as `rag.store.VectorStore` - `exists`, `build`, `search`,
    `stats` - so the CLI, the retriever and the chain take either one without
    knowing which. What differs is the write path: DuckDB is rebuilt wholesale,
    while a partition here is upserted into a table other readers are using,
    so `load_partition` is the Dagster asset's entry point and `build` is the
    from-scratch reload behind `urban-rag index`.
    """

    def __init__(self, settings: PgSettings | None = None) -> None:
        self.settings = settings or PgSettings.from_env()

    # -- identity ----------------------------------------------------------

    @property
    def table(self) -> str:
        return self.settings.qualified_table

    @property
    def meta_table(self) -> str:
        return f"{self.settings.qualified_table}_meta"

    @property
    def location(self) -> str:
        """Where this store is, for messages that name it. Never a password."""
        return f"{self.settings.safe_target}/{self.settings.database}#{self.table}"

    # -- connecting --------------------------------------------------------

    @contextmanager
    def connect(
        self, *, register: bool = True, autocommit: bool = False
    ) -> Iterator["Connection"]:
        """One connection, committed on a clean exit and rolled back otherwise.

        ``register`` is what teaches psycopg the `vector` type, and it looks the
        type up in the database - so a caller that might be creating the
        extension itself opens with ``register=False`` and registers once it is
        there. A caller that only reads can register straight away.
        """
        psycopg = _psycopg()
        try:
            connection = psycopg.connect(
                **self.settings.connection_kwargs(), autocommit=autocommit
            )
        except psycopg.OperationalError as exc:
            raise PostgresUnavailable(self._diagnose(exc)) from exc
        with connection:
            if register:
                _register_vector(connection)
            yield connection

    def exists(self) -> bool:
        """Whether the chunks table is there to be queried."""
        with self.connect(register=False) as connection:
            found = connection.execute(
                "SELECT to_regclass(%s)", [self.table]
            ).fetchone()
        return found is not None and found[0] is not None

    def check_writable(self) -> None:
        """Connect and prove the role can write, before any work is done.

        The counterpart of the DuckDB store's write-lock check: a wrong
        password, a closed security group or a read-only role should cost the
        first second of a run rather than the last.
        """
        with self.connect(register=False) as connection:
            # Whichever level already exists is the one that decides: the table
            # if it is there, else the schema, else the database the schema
            # would be created in. `has_schema_privilege` raises rather than
            # answering for a schema that does not exist, so it is never asked.
            allowed = connection.execute(
                """
                SELECT CASE
                    WHEN to_regclass(%s) IS NOT NULL
                        THEN has_table_privilege(current_user, to_regclass(%s), 'INSERT')
                    WHEN to_regnamespace(%s) IS NOT NULL
                        THEN has_schema_privilege(current_user, to_regnamespace(%s)::oid, 'CREATE')
                    ELSE has_database_privilege(current_user, current_database(), 'CREATE')
                END
                """,
                [self.table, self.table, self.settings.db_schema, self.settings.db_schema],
            ).fetchone()
        if allowed is not None and allowed[0] is False:
            raise PostgresUnavailable(
                f"{self.settings.user} may connect to {self.location} but may "
                f"neither create in schema {self.settings.db_schema} nor insert "
                "into the chunks table. Grant it, see hbu_infra's "
                "sql/000_roles.sql."
            )

    # -- schema ------------------------------------------------------------

    def ensure_schema(self, dimension: int, model: str) -> None:
        """Create everything the loads need, once, and record what they hold."""
        with self.connect(register=False) as connection:
            cursor = connection.cursor()
            self._prepare(cursor, connection, dimension, model)
            self._create_indexes(cursor)
            self._write_meta(cursor, dimension=dimension, embedding_model=model)

    def _prepare(
        self,
        cursor: "Cursor",
        connection: "Connection",
        dimension: int,
        model: str,
    ) -> None:
        """Extension, schema, table - and a refusal to mix vector spaces."""
        self._create_extension(cursor)
        _register_vector(connection)
        self._check_compatible(cursor, dimension, model)
        self._ensure_schema(cursor)
        self._create_table(cursor, dimension)
        self._create_meta_table(cursor)

    def _ensure_schema(self, cursor: "Cursor") -> None:
        """Create the schema only when it is genuinely absent.

        `CREATE SCHEMA IF NOT EXISTS` is not the no-op its name suggests:
        unlike `CREATE EXTENSION IF NOT EXISTS`, Postgres checks `CREATE` on
        the *database* before it checks whether the schema is already there,
        so a role without that privilege is refused with `permission denied
        for database` even though there is nothing to do. `rag` belongs to
        hbu_infra - sql/000_roles.sql creates it `AUTHORIZATION urban_rag` -
        so on a deployed database this asks for a privilege the pipeline is
        deliberately not granted, to do work already done. Asking first keeps
        the create for the one case that needs it: a scratch database where
        the schema really is missing.
        """
        schema = self.settings.db_schema
        found = cursor.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = %s", (schema,)
        ).fetchone()
        if found:
            return
        try:
            cursor.execute(f"CREATE SCHEMA {schema}")
        except Exception as exc:
            raise PostgresUnavailable(
                f"Schema {schema!r} does not exist in {self.settings.database} "
                f"and {self.settings.user} may not create it "
                f"({_first_line(exc)}).\n"
                "hbu_infra owns it: run `db.py init` in that repo, which "
                "applies sql/000_roles.sql."
            ) from exc

    def _create_extension(self, cursor: "Cursor") -> None:
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
        except Exception as exc:
            # `IF NOT EXISTS` returns before the privilege check when the
            # extension is already installed, so reaching here means it is not
            # and this role cannot install it - which is the normal RDS setup.
            raise PostgresUnavailable(
                "The `vector` extension is not installed in "
                f"{self.settings.database} and {self.settings.user} may not "
                f"install it ({_first_line(exc)}).\n"
                "An rds_superuser runs this once, per database:\n"
                "  CREATE EXTENSION IF NOT EXISTS vector;\n"
                "hbu_infra does both: sql/001_extensions.sql for the "
                "extension, sql/000_roles.sql for the grants."
            ) from exc

    def _create_table(self, cursor: "Cursor", dimension: int) -> None:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                chunk_id     text        PRIMARY KEY,
                doc_id       text        NOT NULL,
                url          text        NOT NULL,
                title        text,
                source_table text        NOT NULL,
                neighborhood text        NOT NULL,
                scrape_date  date        NOT NULL,
                chunk_index  integer     NOT NULL,
                num_tokens   integer     NOT NULL,
                -- jsonb rather than the parquet's JSON string: in a shared
                -- database "which zones cite this document" is a query someone
                -- will want, and it reads back as text for `Hit` either way.
                feature_ids  jsonb       NOT NULL DEFAULT '[]'::jsonb,
                model        text        NOT NULL,
                text         text        NOT NULL,
                embedding    vector({int(dimension)}) NOT NULL,
                indexed_at   timestamptz NOT NULL DEFAULT now()
            )
            """
        )

    def _create_meta_table(self, cursor: "Cursor") -> None:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.meta_table} (
                key        text        PRIMARY KEY,
                value      text        NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )

    def _create_indexes(self, cursor: "Cursor") -> None:
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {self.settings.table}_embedding_hnsw "
            f"ON {self.table} USING hnsw (embedding vector_cosine_ops) "
            f"WITH (m = {DEFAULT_M}, ef_construction = {DEFAULT_EF_CONSTRUCTION})"
        )
        # Every filtered search and every prune is by partition, and both are
        # otherwise a sequential scan of the whole corpus.
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {self.settings.table}_partition "
            f"ON {self.table} (neighborhood, scrape_date)"
        )

    def _drop(self, cursor: "Cursor") -> None:
        cursor.execute(f"DROP TABLE IF EXISTS {self.table}")
        cursor.execute(f"DROP TABLE IF EXISTS {self.meta_table}")

    def _check_compatible(self, cursor: "Cursor", dimension: int, model: str) -> None:
        """Refuse to mix vector spaces in one index.

        Vectors from two encoders share no space, and only the *width* has to
        match for an insert to succeed - so a partition re-embedded with a new
        model would otherwise land silently and be retrieved as noise.
        """
        existing = self._column_dimension(cursor)
        if existing is not None and existing != dimension:
            raise IndexMismatch(
                f"{self.table} holds {existing}-dimensional vectors and this "
                f"load is {dimension}-dimensional ({model}). Rebuild the table "
                "(`urban-rag index --backend postgres`), which drops it first."
            )
        if existing is None:
            return
        indexed_model = self._meta(cursor, required=False).get("embedding_model")
        if indexed_model and indexed_model != model:
            raise IndexMismatch(
                f"{self.table} was built with {indexed_model!r} and this load "
                f"was embedded with {model!r}. Same width, different space: "
                "rebuild rather than mix them."
            )

    def _column_dimension(self, cursor: "Cursor") -> int | None:
        """The declared width of the embedding column, or None if there is none.

        Read from the catalog rather than from `index_meta`, so a table someone
        else created is described accurately instead of trusted.
        """
        cursor.execute(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) AND attname = 'embedding' "
            "AND NOT attisdropped",
            [self.table],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        width = re.search(r"\((\d+)\)", str(row[0]))
        return int(width.group(1)) if width else None

    def _write_meta(self, cursor: "Cursor", **values: object) -> None:
        rows = [
            ("schema_version", SCHEMA_VERSION),
            *((key, str(value)) for key, value in values.items() if value is not None),
        ]
        cursor.executemany(
            f"INSERT INTO {self.meta_table} (key, value) VALUES (%s, %s) "
            "ON CONFLICT (key) DO UPDATE "
            "SET value = EXCLUDED.value, updated_at = now()",
            rows,
        )

    # -- writing -----------------------------------------------------------

    def load_partition(
        self,
        path: str,
        *,
        neighborhood: str,
        scrape_date: str,
        prune: bool = True,
    ) -> dict[str, object]:
        """Upsert one partition's `embeddings.parquet` into the store.

        The write path the Dagster asset uses, and the one place this store
        differs in kind from the DuckDB one: a partition is loaded *into* a
        live table rather than replacing it, because other readers are querying
        it while the pipeline runs. Snapshot semantics are kept by two rules
        instead - the newest copy of a chunk wins, and (with ``prune``) the
        borough's older scrape dates are dropped once the new one has landed.

        The whole thing is one transaction: a reader sees the partition either
        as it was or as it now is, never half-loaded.
        """
        frame, vectors, model = _read_embeddings(path)
        if frame.empty:
            raise IndexMismatch(f"{path} holds no embedded chunk to load.")
        dimension = int(vectors.shape[1])
        staging = f"{self.settings.table}_load"

        with self.connect(register=False) as connection:
            cursor = connection.cursor()
            self._prepare(cursor, connection, dimension, model)
            # A fresh cursor, and not for tidiness: registering the `vector`
            # type above only reaches cursors created afterwards. psycopg builds
            # a cursor's transformer the first time that cursor is executed and
            # the transformer keeps the type registry it saw then - so the very
            # cursor that just ran `CREATE EXTENSION` cannot resolve "vector" in
            # a binary COPY. It fails with `couldn't find the type 'vector' in
            # the types registry`, naming neither the column nor the cause.
            cursor = connection.cursor()
            self._create_indexes(cursor)
            cursor.execute(
                f"CREATE TEMP TABLE {staging} "
                f"(LIKE {self.table} INCLUDING DEFAULTS) ON COMMIT DROP"
            )
            copied = _copy_into(cursor, staging, frame, vectors)
            loaded = self._insert_from(cursor, staging, upsert=True)
            pruned = self._prune(cursor, neighborhood, scrape_date) if prune else 0
            self._write_meta(cursor, dimension=dimension, embedding_model=model)
            chunks, documents = self._counts(cursor)

        return {
            "copied": copied,
            "loaded": loaded,
            "pruned": pruned,
            "chunks": chunks,
            "documents": documents,
            "dimension": dimension,
            "embedding_model": model,
            "table": self.table,
            "location": self.location,
        }

    def build(
        self,
        pattern: str,
        *,
        neighborhood: str | None = None,
        scrape_date: str | None = None,
    ) -> dict[str, object]:
        """Reload the store from every `embeddings.parquet` matching ``pattern``.

        The same full rebuild `rag.store.VectorStore.build` performs, and the
        way to change encoder: the table is dropped, so a new vector width or a
        new model is a reload rather than a migration. Prefer `load_partition`
        for the steady state - this one leaves the corpus unqueryable for the
        length of the load.
        """
        paths = _source_files(pattern)
        if not paths:
            raise IndexMismatch(
                f"No readable embeddings at {pattern!r}.\n"
                "Materialize `document_embeddings` first (`make corpus`)."
            )

        staging = f"{self.settings.table}_load"
        dimension: int | None = None
        model: str | None = None
        copied = 0

        with self.connect(register=False) as connection:
            cursor = connection.cursor()
            # An HNSW build that spills to disk takes multiples of the time it
            # takes in memory. Session-scoped, so it cannot starve the instance.
            cursor.execute(
                "SELECT set_config('maintenance_work_mem', %s, false)",
                [self.settings.maintenance_work_mem],
            )

            for source in paths:
                frame, vectors, file_model = _read_embeddings(
                    source, neighborhood=neighborhood, scrape_date=scrape_date
                )
                if frame.empty:
                    continue
                width = int(vectors.shape[1])
                if dimension is None:
                    dimension, model = width, file_model
                    self._create_extension(cursor)
                    _register_vector(connection)
                    # A fresh cursor, and not for tidiness: registering the `vector`
                    # type above only reaches cursors created afterwards. psycopg builds
                    # a cursor's transformer the first time that cursor is executed and
                    # the transformer keeps the type registry it saw then - so the very
                    # cursor that just ran `CREATE EXTENSION` cannot resolve "vector" in
                    # a binary COPY. It fails with `couldn't find the type 'vector' in
                    # the types registry`, naming neither the column nor the cause.
                    cursor = connection.cursor()
                    # A rebuild, so the old table goes before the new one is
                    # described - that is what lets the width change.
                    self._drop(cursor)
                    self._ensure_schema(cursor)
                    self._create_table(cursor, dimension)
                    self._create_meta_table(cursor)
                    cursor.execute(
                        f"CREATE TEMP TABLE {staging} "
                        f"(LIKE {self.table} INCLUDING DEFAULTS) ON COMMIT DROP"
                    )
                elif (width, file_model) != (dimension, model):
                    raise IndexMismatch(
                        f"Mixed embeddings: {model} ({dimension}d) and "
                        f"{file_model} ({width}d) in {pattern!r}. Re-materialize "
                        "`document_embeddings` so every partition uses one model."
                    )
                copied += _copy_into(cursor, staging, frame, vectors)

            if dimension is None:
                raise IndexMismatch(
                    f"No embedded chunks matched {pattern!r} with those filters."
                )

            self._insert_from(cursor, staging, upsert=False)
            # Built after the load, not before: maintaining an HNSW index over
            # every insert costs far more than building it once at the end.
            self._create_indexes(cursor)
            self._write_meta(
                cursor,
                dimension=dimension,
                embedding_model=model,
                source_pattern=pattern,
            )
            chunks, documents = self._counts(cursor)
            # The planner has no statistics for a table this new, and the first
            # searches are the ones a person is watching.
            cursor.execute(f"ANALYZE {self.table}")

        return {
            "copied": copied,
            "chunks": chunks,
            "documents": documents,
            "dimension": dimension,
            "embedding_model": model,
            "table": self.table,
            "location": self.location,
        }

    def _insert_from(self, cursor: "Cursor", staging: str, *, upsert: bool) -> int:
        columns = ", ".join(_COPY_COLUMNS)
        if upsert:
            assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATED_COLUMNS)
            conflict = (
                f"ON CONFLICT (chunk_id) DO UPDATE SET {assignments}, "
                "indexed_at = now() "
                # The same resolution is re-embedded on every scrape date it is
                # still cited on. Newest wins, as in the DuckDB store.
                "WHERE EXCLUDED.scrape_date >= target.scrape_date"
            )
        else:
            conflict = "ON CONFLICT (chunk_id) DO NOTHING"
        cursor.execute(
            f"""
            INSERT INTO {self.table} AS target ({columns})
            -- Two source tables can link the same PDF, so one partition can
            -- carry a chunk_id twice; ON CONFLICT refuses to touch a row twice
            -- in one statement, so the duplicate is resolved here instead.
            SELECT DISTINCT ON (chunk_id) {columns}
            FROM {staging}
            ORDER BY chunk_id, scrape_date DESC
            {conflict}
            """
        )
        return max(cursor.rowcount, 0)

    def _prune(self, cursor: "Cursor", neighborhood: str, scrape_date: str) -> int:
        """Drop what an older scrape of this borough left behind.

        A chunk still cited today was just overwritten with today's date by the
        upsert; what keeps an older date is a document the borough has stopped
        linking to, and the pipeline's snapshot semantics say it should stop
        being retrievable.
        """
        cursor.execute(
            f"DELETE FROM {self.table} "
            "WHERE neighborhood = %s AND scrape_date < %s::date",
            [neighborhood, scrape_date],
        )
        return max(cursor.rowcount, 0)

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
        clauses: list[str] = []
        parameters: list[object] = []
        for column, value in (
            ("neighborhood", neighborhood),
            ("scrape_date", scrape_date),
            ("source_table", source_table),
            ("doc_id", doc_id),
        ):
            if value is None:
                continue
            # `::date` because the column is one and the parameter is a string.
            clauses.append(f"{column} = %s" + ("::date" if column == "scrape_date" else ""))
            parameters.append(value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        vector = np.asarray(query_vector, dtype=np.float32)
        with self.connect() as connection:
            cursor = connection.cursor()
            dimension = self._require_dimension(cursor)
            if vector.size != dimension:
                raise IndexMismatch(
                    f"{self.location} holds {dimension}-dimensional vectors but "
                    f"the query is {vector.size}-dimensional; the store was built "
                    f"with {self._meta(cursor, required=False).get('embedding_model')!r}. "
                    "Re-index, or query with the model it was built from."
                )
            # HNSW returns its candidate list and *then* the filters are applied
            # to it, so a filtered search can come back short. Both the filters
            # and a large k are reasons to widen the list.
            cursor.execute(
                "SELECT set_config('hnsw.ef_search', %s, true)",
                [str(max(self.settings.ef_search, k * 4))],
            )
            cursor.execute(
                f"""
                SELECT {_SELECT_LIST}, embedding <=> %s AS distance
                FROM {self.table}
                {where}
                ORDER BY embedding <=> %s
                LIMIT %s
                """,
                [vector, *parameters, vector, k],
            )
            rows = cursor.fetchall()

        return [
            Hit(
                *row[: len(COLUMNS)],
                # Vectors are L2-normalised upstream, so `<=>` is in [0, 2] and
                # this is exactly the cosine similarity.
                similarity=1.0 - float(row[len(COLUMNS)]),
            )
            for row in rows
        ]

    def stats(self) -> dict[str, object]:
        """Counts and coverage, for ``urban-rag status``."""
        with self.connect(register=False) as connection:
            cursor = connection.cursor()
            # Read first: on a database this tool did not write, it is the
            # difference between a clear message and a catalog error.
            meta = self._meta(cursor)
            self._require_dimension(cursor)
            chunks, documents = self._counts(cursor)
            cursor.execute(
                f"SELECT min(num_tokens), avg(num_tokens), max(num_tokens) "
                f"FROM {self.table}"
            )
            tokens = cursor.fetchone() or (None, None, None)
            cursor.execute(
                f"SELECT source_table, count(DISTINCT doc_id), count(*) "
                f"FROM {self.table} GROUP BY source_table ORDER BY source_table"
            )
            per_table = cursor.fetchall()
            cursor.execute(
                f"SELECT neighborhood, scrape_date::text, count(DISTINCT doc_id), "
                f"count(*) FROM {self.table} GROUP BY neighborhood, scrape_date "
                "ORDER BY neighborhood, scrape_date"
            )
            partitions = cursor.fetchall()
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

    def _counts(self, cursor: "Cursor") -> tuple[int, int]:
        cursor.execute(f"SELECT count(*), count(DISTINCT doc_id) FROM {self.table}")
        chunks, documents = cursor.fetchone()
        return int(chunks), int(documents)

    def _require_dimension(self, cursor: "Cursor") -> int:
        dimension = self._column_dimension(cursor)
        if dimension is None:
            raise IndexMismatch(
                f"There is no {self.table} table in {self.location}. "
                "Run `urban-rag index --backend postgres`, or materialize "
                "`document_index` for a partition."
            )
        return dimension

    def _meta(self, cursor: "Cursor", *, required: bool = True) -> dict[str, str]:
        # Existence is checked rather than caught: a failed statement aborts the
        # transaction, taking every query after it with it.
        cursor.execute("SELECT to_regclass(%s)", [self.meta_table])
        found = cursor.fetchone()
        if found is None or found[0] is None:
            if not required:
                return {}
            raise IndexMismatch(
                f"{self.location} has no {self.meta_table} table - it was not "
                "written by `urban-rag index`. Re-index, or point --backend "
                "somewhere else."
            )
        cursor.execute(f"SELECT key, value FROM {self.meta_table}")
        return {str(key): str(value) for key, value in cursor.fetchall()}

    def _diagnose(self, exc: Exception) -> str:
        """Turn libpq's one-liner into something with a next step in it."""
        message = _first_line(exc)
        lowered = str(exc).lower()
        hints: list[str] = []
        if "timeout" in lowered or "no route" in lowered or "connection refused" in lowered:
            hints.append(
                "An RDS instance is only reachable from inside its VPC: check the "
                "security group's inbound 5432 rule, and whether this host is in "
                "the VPC (ECS task, VPN, bastion) at all."
            )
        if "password authentication failed" in lowered or "no password supplied" in lowered:
            hints.append(
                f"Set {ENV_PREFIX}PASSWORD, or {ENV_PREFIX}SECRET_ID for a Secrets "
                f"Manager secret, or {ENV_PREFIX}IAM_AUTH=1 for an IAM auth token "
                "(which also needs `GRANT rds_iam TO <role>` in the database)."
            )
        if "certificate" in lowered or "ssl" in lowered:
            hints.append(f"TLS: the RDS CA bundle is at {RDS_CA_BUNDLE_URL}.")
        if "does not exist" in lowered:
            hints.append(
                f"This code never creates the database itself; create "
                f"{self.settings.database} on the instance first."
            )
        return f"Cannot connect to {self.location}: {message}" + "".join(
            f"\n  {hint}" for hint in hints
        )


# -- the driver, imported lazily -------------------------------------------
# `resources.py` imports this module at code-location load time, and a Dagster
# process that only scrapes should not fail to start because the Postgres
# driver is missing - the same reason sentence-transformers is imported inside
# `rag.embeddings` methods rather than at its top.


def _psycopg():
    try:
        import psycopg
    except ModuleNotFoundError as exc:  # pragma: no cover - install-time path
        raise PostgresUnavailable(
            "psycopg is not installed. `make sync`, or "
            "`pip install 'psycopg[binary]' pgvector`."
        ) from exc
    return psycopg


def _register_vector(connection: "Connection") -> None:
    """Teach psycopg the `vector` type of this database.

    The adapter looks the type up by name at registration time, so this is also
    the check that the extension is actually installed here.
    """
    try:
        from pgvector.psycopg import register_vector
    except ModuleNotFoundError as exc:  # pragma: no cover - install-time path
        raise PostgresUnavailable(
            "The pgvector python package is not installed. `make sync`, or "
            "`pip install pgvector`."
        ) from exc
    try:
        register_vector(connection)
    except Exception as exc:
        raise PostgresUnavailable(
            "This database has no `vector` type, so the extension has not been "
            f"created in it ({_first_line(exc)}).\n"
            "An rds_superuser creates it once per database - hbu_infra's "
            "`make db-init`."
        ) from exc


def _boto_client(service: str, region: str | None):
    import boto3
    from botocore.exceptions import ProfileNotFound

    try:
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=region)
    except ProfileNotFound:
        # In a task there is no ~/.aws at all, only the role the task assumes.
        session = boto3.Session(region_name=region)
    return session.client(service)


# -- reading the parquet ---------------------------------------------------


def _source_files(pattern: str) -> list[str]:
    """Every `embeddings.parquet` matching ``pattern``, local or on S3."""
    matches = sorted(filesystem(pattern).glob(pattern))
    if is_s3_uri(pattern):
        # s3fs answers with bucket/key, without the scheme it was asked with.
        return [str(m) if is_s3_uri(str(m)) else f"s3://{m}" for m in matches]
    return [str(m) for m in matches]


def _read_embeddings(
    path: str,
    *,
    neighborhood: str | None = None,
    scrape_date: str | None = None,
) -> tuple[pd.DataFrame, np.ndarray, str]:
    """One `embeddings.parquet` as (rows, vectors, model).

    The vectors come out as one contiguous ``(n, dimension)`` float32 block
    rather than a column of arrays, which is the shape both the width check and
    the binary COPY want.
    """
    frame = pd.read_parquet(path, storage_options=storage_options(path))
    missing = [c for c in (*COLUMNS, "embedding") if c not in frame.columns]
    if missing:
        raise IndexMismatch(
            f"{path} has no {', '.join(missing)} column - it was not written by "
            "`document_embeddings`."
        )

    if neighborhood is not None:
        frame = frame[frame["neighborhood"].astype(str) == neighborhood]
    if scrape_date is not None:
        frame = frame[frame["scrape_date"].astype(str).str[:10] == scrape_date]
    if frame.empty:
        return frame, np.empty((0, 0), dtype=np.float32), ""

    models = {str(model) for model in frame["model"]}
    if len(models) > 1:
        raise IndexMismatch(
            f"{path} mixes embeddings from {sorted(models)}. Vectors from two "
            "encoders share no space; re-materialize `document_embeddings`."
        )
    return frame, _stack(frame["embedding"], path), models.pop()


def _stack(column: pd.Series, path: str) -> np.ndarray:
    try:
        vectors = np.stack([np.asarray(value, dtype=np.float32) for value in column])
    except ValueError as exc:
        raise IndexMismatch(
            f"{path} holds vectors of differing width ({exc}); one index needs "
            "one width."
        ) from exc
    if vectors.ndim != 2:
        raise IndexMismatch(f"{path} does not hold one vector per row.")
    return vectors


# -- writing the rows ------------------------------------------------------


def _copy_into(
    cursor: "Cursor", target: str, frame: pd.DataFrame, vectors: np.ndarray
) -> int:
    """Stream ``frame`` into ``target`` with binary COPY.

    Binary rather than text: the chunks are French prose full of newlines and
    quotes, and the vectors are 1024 floats apiece - text COPY would escape the
    first and stringify the second, for no benefit.
    """
    from psycopg.types.json import Jsonb

    statement = (
        f"COPY {target} ({', '.join(_COPY_COLUMNS)}) FROM STDIN (FORMAT BINARY)"
    )
    with cursor.copy(statement) as copy:
        # Binary COPY carries no types of its own; these are `_COPY_COLUMNS`'.
        copy.set_types(list(_COPY_TYPES))
        for position, row in enumerate(frame.itertuples(index=False)):
            copy.write_row(
                (
                    str(row.chunk_id),
                    str(row.doc_id),
                    str(row.url),
                    _optional(row.title),
                    str(row.source_table),
                    str(row.neighborhood),
                    _as_date(row.scrape_date),
                    int(row.chunk_index),
                    int(row.num_tokens),
                    Jsonb(_json_array(row.feature_ids)),
                    str(row.model),
                    str(row.text),
                    vectors[position],
                )
            )
    return len(frame)


def _optional(value: object) -> str | None:
    """`None` for NULL, for pandas' several kinds of missing, and for "".

    `title` is genuinely absent for most source tables, and pandas spells that
    absence differently depending on how the column was typed on the way in.
    """
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass  # pd.isna on an array-like answers element-wise
    text = str(value)
    return text or None


def _as_date(value: object) -> date:
    """The partition's date, as a `date`.

    `datetime` first: `pandas.Timestamp` is a `date` subclass, and a midnight
    timestamp is not what a `date` column should be handed.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _json_array(value: object) -> list:
    """`feature_ids` as a list; the parquet carries it as a JSON string."""
    if isinstance(value, list):
        return value
    if isinstance(value, np.ndarray):
        return value.tolist()
    if value is None or not str(value).strip():
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        # Not worth failing a partition over: the ids are provenance, not keys.
        return []
    return parsed if isinstance(parsed, list) else [parsed]


# -- environment -----------------------------------------------------------


def _env(name: str) -> str | None:
    """`URBAN_RAG_PG_<name>`, treating an empty value as unset.

    docker-compose writes `${VAR:-}` as an empty string rather than leaving the
    variable out, and an empty host is worse than no host at all.
    """
    value = os.environ.get(f"{ENV_PREFIX}{name}")
    return value.strip() or None if value else None


def _int(value: str | None) -> int | None:
    return int(value) if value is not None else None


def _flag(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.strip().lower() in ("1", "true", "yes", "on")


def _first_line(exc: BaseException) -> str:
    return str(exc).splitlines()[0] if str(exc) else type(exc).__name__
