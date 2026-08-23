"""Dagster resources: the source connections, the output tree, the PDF cache."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from urban_rag.rag.documents import (
    DEFAULT_MAX_TOKENS,
    DEFAULT_OVERLAP_TOKENS,
    PdfFetcher,
)
from urban_rag.rag.embeddings import (
    DEFAULT_MODEL,
    SentenceTransformerEmbeddings,
    ModelTokenRuler,
    cached_embeddings,
)
from urban_rag.rag.pgvector import PgSettings, PgVectorStore
from urban_rag.postgis import connect as postgis_connect
from urban_rag.bdoi import DEFAULT_BASE_URL as BDOI_BASE_URL, BdoiFetcher
from urban_rag.cmhc import (
    DEFAULT_BASE_URL as CMHC_BASE_URL,
    SURVEY_YEAR_VAR,
    CmhcFetcher,
    default_survey_year,
)
from urban_rag.open_data import (
    DEFAULT_BASE_URL as OPEN_DATA_BASE_URL,
    CkanClient,
)
from urban_rag.infolot import (
    DEFAULT_BASE_URL as INFOLOT_BASE_URL,
    DEFAULT_BATCH_SIZE,
    LOT_LAYER,
    InfolotClient,
)
from urban_rag.spectrum import DEFAULT_BASE_URL, SpectrumClient
from urban_rag.storage import join


class SpectrumResource(ConfigurableResource):
    """Connection settings for the Feature Service.

    Defaults are tuned to be gentle on a live municipal server rather than to
    finish quickly.
    """

    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: float = 60.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every request, in seconds."
    )
    max_retries: int = 3
    page_length: int = Field(
        default=500, description="Rows per `features.json` page."
    )
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def client(self) -> SpectrumClient:
        return SpectrumClient(
            self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class ParquetStore(ConfigurableResource):
    """The output tree: one prefix per asset, then scrape date, then borough.

        <root>/spectrum_table_catalog/2026-08-20/
        <root>/neighborhood_features/2026-08-20/VSMPE/
        <root>/reference_neighborhoods/2026-08-20/

    ``root_dir`` is `output_root()` in the real code location, so the same
    keys address a directory on disk or an ``s3://<S3_BUCKET>/`` prefix.

    Keyed by asset name rather than by source system so that every asset owns
    one prefix: a partition can be listed, copied or dropped without touching
    what another asset wrote for the same day. The keys are bare values rather
    than hive ``key=value`` pairs, so `neighborhood` and `scrape_date` are
    written as columns instead of being recovered from the path.
    """

    root_dir: str

    def partition_dir(
        self, asset: str, scrape_date: str, neighborhood: str | None = None
    ) -> str:
        parts = [asset, scrape_date]
        if neighborhood is not None:
            parts.append(neighborhood)
        return join(self.root_dir, *parts)


class OpenDataResource(ConfigurableResource):
    """Connection settings for donnees.montreal.ca, the city's CKAN portal.

    Same posture as `SpectrumResource`: paced and patient rather than fast,
    since this is a public municipal server with no quota to spend.
    """

    base_url: str = OPEN_DATA_BASE_URL
    timeout_seconds: float = 60.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every request, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def client(self) -> CkanClient:
        return CkanClient(
            self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class InfolotResource(ConfigurableResource):
    """Connection settings for Infolot, the Registre foncier's lot service.

    Same posture as `SpectrumResource`: paced and patient rather than fast.
    One borough is a few hundred batched requests, and the server is a live
    government service with no quota to spend.
    """

    base_url: str = INFOLOT_BASE_URL
    layer: int = Field(
        default=LOT_LAYER, description="Layer id of the cadastral lot polygons."
    )
    timeout_seconds: float = 60.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every request, in seconds."
    )
    max_retries: int = 3
    batch_size: int = Field(
        default=DEFAULT_BATCH_SIZE,
        description="Lots per `objectIds` batch; the service caps a response at 1000.",
    )
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def client(self) -> InfolotClient:
        return InfolotClient(
            self.base_url,
            layer=self.layer,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            batch_size=self.batch_size,
            ca_bundle=self.ca_bundle,
        )


class BdoiResource(ConfigurableResource):
    """Cache settings for StatCan's Open Database of Buildings.

    Deliberately not part of `ParquetStore`: a published BDOI extract does
    not change, so the cache is keyed by filename and shared across every
    scrape date, while everything in the parquet tree is a dated snapshot. It
    is also always local - it is a download cache, not pipeline output.
    """

    cache_dir: str
    base_url: str = BDOI_BASE_URL
    timeout_seconds: float = 300.0
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

    def fetcher(self) -> BdoiFetcher:
        return BdoiFetcher(
            cache_dir=self.cache_dir,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class CmhcResource(ConfigurableResource):
    """Which CMHC Rental Market Survey to read, and where to cache it.

    Same posture as `BdoiResource`: a published survey year is final, so the
    workbook is cached by filename outside the partition tree and shared
    across every scrape date, and the cache is always local - it is a download
    cache, not pipeline output.

    `survey_year` is config rather than a partition dimension because the
    survey is annual and the pipeline's date axis is the scrape date. It
    defaults to $URBAN_RAG_CMHC_SURVEY_YEAR, so a run can be pointed at
    another year without restating `cache_dir` - which `--config-json` would
    otherwise replace, since it overwrites a resource's config wholesale.
    """

    cache_dir: str
    survey_year: int = Field(
        default_factory=default_survey_year,
        description=(
            f"Survey year to read. Defaults to ${SURVEY_YEAR_VAR}, else the "
            "latest published one. An unpublished year answers 404."
        ),
    )
    base_url: str = CMHC_BASE_URL
    timeout_seconds: float = 120.0
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

    def fetcher(self) -> CmhcFetcher:
        return CmhcFetcher(
            cache_dir=self.cache_dir,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )

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
    `URBAN_RAG_PG_*` variable says" - see `rag.pgvector.PgSettings.from_env`.
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


class PostgisResource(ConfigurableResource):
    """The same Postgres database as `PgVectorResource`, for the plain
    PostGIS tables (`rag.lots`, `rag.buildings`, `rag.building_lots`) rather
    than the vector store.

    A separate resource rather than reusing `PgVectorResource` because that
    class's `table`/`ef_search`/`prune_superseded` fields are about the vector
    index and mean nothing here - but every connection field means the same
    thing, resolved the same way: an explicit value, else the corresponding
    `URBAN_RAG_PG_*` variable. See `rag.pgvector.PgSettings.from_env`.
    """

    host: str | None = None
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
    iam_auth: bool | None = None
    region: str | None = None
    sslmode: str | None = None
    sslrootcert: str | None = None
    dsn: str | None = None

    def settings(self) -> PgSettings:
        return PgSettings.from_env(
            host=self.host,
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
        )

    def connect(self):
        return postgis_connect(self.settings())
