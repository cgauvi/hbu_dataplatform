"""Dagster resources: the Spectrum connection and the local parquet store."""

from __future__ import annotations

from pathlib import Path

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
from urban_rag.spectrum import DEFAULT_BASE_URL, SpectrumClient


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


class GeoParquetStore(ConfigurableResource):
    """Hive-partitioned output tree: one directory per (neighborhood, date)."""

    root_dir: str

    def partition_dir(self, neighborhood: str, scrape_date: str) -> Path:
        return (
            Path(self.root_dir)
            / f"neighborhood={neighborhood}"
            / f"scrape_date={scrape_date}"
        )


class DocumentStore(ConfigurableResource):
    """Where linked PDFs are cached, and where the RAG corpus is written.

    The cache is deliberately outside the scrape partitions: a resolution PDF
    is immutable once published, so re-materializing a later scrape date
    re-reads it from disk instead of from the city's web server.
    """

    root_dir: str
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

    def partition_dir(self, neighborhood: str, scrape_date: str) -> Path:
        return (
            Path(self.root_dir)
            / f"neighborhood={neighborhood}"
            / f"scrape_date={scrape_date}"
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
