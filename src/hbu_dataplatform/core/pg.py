"""Where the platform's Postgres is, and how to authenticate to it.

One `PgSettings` for everything that opens the database: the vector store
(`hbu_dataplatform.rag.pgvector`), the PostGIS working set and the published
tables (`hbu_dataplatform.core.postgis`, `.warehouse`), the tile CLI and the
scripts. Configured from `URBAN_RAG_PG_*` (see `PgSettings.from_env`) or from
the `PgVectorResource` / `PostgisResource` in the Dagster code location.
Credentials are resolved at connect time, in this order: an explicit
password, a Secrets Manager secret, an IAM auth token, then libpq's own
`PGPASSWORD`/`~/.pgpass`.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any

from hbu_dataplatform.core.storage import AWS_PROFILE

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


#: Search-time candidate list. pgvector defaults to 40, which silently caps
#: recall once `k` approaches it - and a filtered search discards candidates
#: *after* the index returns them, so both `k` and the filters push this up.
DEFAULT_EF_SEARCH = 100


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
    environment applies. ``hostaddr`` can point at a tunnel while ``host`` stays
    the RDS endpoint used for `verify-full` certificate checks. ``dsn``
    short-circuits all of it with a full connection string, which is what a
    local `pgvector/pgvector` container is easiest to reach with.
    """

    host: str | None = None
    hostaddr: str | None = None
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
    #:
    #: Raise it with ``URBAN_RAG_PG_STATEMENT_TIMEOUT_SECONDS`` for the spatial
    #: assets, whose heavy step is one server-side statement rather than a bulk
    #: write: `compute_lot_buildable_setbacks` classifies every boundary
    #: segment of every lot in a borough in a single ``CREATE TEMP TABLE AS``,
    #: and on a small instance a borough of 25,000 lots takes longer than the
    #: half hour below. The cap is a guard against a runaway query, not a
    #: statement about how long the work legitimately takes.
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
            "hostaddr": _env("HOSTADDR") or os.environ.get("PGHOSTADDR"),
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
            "statement_timeout_seconds": _int(
                _env("STATEMENT_TIMEOUT_SECONDS")
            ),
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
            "hostaddr": self.hostaddr,
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


def _boto_client(service: str, region: str | None):
    import boto3
    from botocore.exceptions import ProfileNotFound

    try:
        session = boto3.Session(profile_name=AWS_PROFILE, region_name=region)
    except ProfileNotFound:
        # In a task there is no ~/.aws at all, only the role the task assumes.
        session = boto3.Session(region_name=region)
    return session.client(service)


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
