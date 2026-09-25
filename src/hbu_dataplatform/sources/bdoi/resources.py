"""Dagster resource for StatCan's Open Database of Buildings."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.sources.bdoi.client import (
    DEFAULT_BASE_URL as BDOI_BASE_URL,
    BdoiFetcher,
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
