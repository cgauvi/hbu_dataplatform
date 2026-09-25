"""Dagster resources for the corpus: the PDF cache, the encoder, the vector store."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.core.pg import PgSettings
from hbu_dataplatform.rag.documents import (
    DEFAULT_MAX_TOKENS,
    DEFAULT_OVERLAP_TOKENS,
    PdfFetcher,
)
from hbu_dataplatform.rag.embeddings import (
    DEFAULT_MODEL,
    ModelTokenRuler,
    SentenceTransformerEmbeddings,
    cached_embeddings,
)
from hbu_dataplatform.rag.pgvector import PgVectorStore


class PdfCache(ConfigurableResource):
    """Where the linked PDFs are downloaded to, and how politely.

    Deliberately not part of `ParquetStore`: a published resolution is
    immutable, so the cache is keyed by URL and shared across every scrape
    date, while everything in the parquet tree is a dated snapshot. It is also
    always local - it is a download cache, not pipeline output.
    """

    cache_dir: str
    timeout_seconds: float = 60.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every download, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def fetcher(self) -> PdfFetcher:
        return PdfFetcher(
            cache_dir=self.cache_dir,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class EmbeddingModel(ConfigurableResource):
    """The encoder, and the chunk geometry measured in its tokens."""

    model_name: str = DEFAULT_MODEL
    device: str | None = Field(
        default=None, description="'cuda', 'cpu', ... Defaults to cuda when available."
    )
    cache_dir: str | None = Field(
        default=None, description="Model cache. Defaults to the HF_HOME cache."
    )
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle for huggingface.co. Defaults to certifi, which is what "
            "the hub is signed against - not the ambient corporate root."
        ),
    )
    batch_size: int = 16
    max_tokens: int = Field(
        default=DEFAULT_MAX_TOKENS, description="Token budget per chunk."
    )
    overlap_tokens: int = Field(
        default=DEFAULT_OVERLAP_TOKENS,
        description="Tokens repeated between consecutive chunks.",
    )

    def encoder(self) -> SentenceTransformerEmbeddings:
        return cached_embeddings(
            self.model_name,
            device=self.device,
            batch_size=self.batch_size,
            cache_dir=self.cache_dir,
            ca_bundle=self.ca_bundle,
        )

    def ruler(self) -> ModelTokenRuler:
        """Tokenizer only: chunking does not need the weights loaded."""
        return self.encoder().ruler


class PgVectorResource(ConfigurableResource):
    """The shared vector store: Postgres on RDS with the pgvector extension.

    Every field defaults to `None`, which means "whatever the corresponding
    `URBAN_RAG_PG_*` variable says" - see `core.pg.PgSettings.from_env`.
    Configuring it that way rather than in `definitions.py` keeps one endpoint
    for the code location, the `urban-rag` CLI and anything else that opens the
    store, and keeps the endpoint out of the repository.

    Never put a password in the code location. Either name a Secrets Manager
    secret, use IAM authentication, or pass `EnvVar`::

        PgVectorResource(password=EnvVar("URBAN_RAG_PG_PASSWORD"))
    """

    host: str | None = Field(
        default=None, description="RDS endpoint, e.g. <name>.<id>.<region>.rds.amazonaws.com."
    )
    hostaddr: str | None = Field(
        default=None,
        description="Optional tunnel address; host is still used for TLS verification.",
    )
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = Field(
        default=None,
        description="Prefer secret_id or iam_auth; pass EnvVar(...), never a literal.",
    )
    secret_id: str | None = Field(
        default=None,
        description="Secrets Manager secret holding {'username', 'password'}.",
    )
    iam_auth: bool | None = Field(
        default=None,
        description="Sign each connection with an RDS IAM auth token instead.",
    )
    region: str | None = None
    sslmode: str | None = Field(
        default=None,
        description="Defaults to verify-full, which needs the RDS CA bundle.",
    )
    sslrootcert: str | None = None
    dsn: str | None = Field(
        default=None,
        description="Full libpq connection string; overrides the fields above.",
    )
    db_schema: str | None = None
    table: str | None = None
    ef_search: int | None = Field(
        default=None, description="HNSW candidate list at search time."
    )
    prune_superseded: bool = Field(
        default=True,
        description=(
            "After loading a partition, delete that borough's older scrape "
            "dates - the snapshot semantics the parquet tree already has."
        ),
    )

    def settings(self) -> PgSettings:
        return PgSettings.from_env(
            host=self.host,
            hostaddr=self.hostaddr,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            secret_id=self.secret_id,
            iam_auth=self.iam_auth,
            region=self.region,
            sslmode=self.sslmode,
            sslrootcert=self.sslrootcert,
            dsn=self.dsn,
            db_schema=self.db_schema,
            table=self.table,
            ef_search=self.ef_search,
        )

    def store(self) -> PgVectorStore:
        return PgVectorStore(self.settings())
