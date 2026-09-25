"""Dagster resource for the CMHC Rental Market Survey."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.sources.cmhc.client import (
    AVERAGE_RENTS_READING_MODE_URL,
    DEFAULT_BASE_URL as CMHC_BASE_URL,
    SURVEY_YEAR_VAR,
    CmhcFetcher,
    CmhcReadingModeFetcher,
    default_survey_year,
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
    average_rents_url: str = AVERAGE_RENTS_READING_MODE_URL
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

    def reading_mode_fetcher(self) -> CmhcReadingModeFetcher:
        return CmhcReadingModeFetcher(
            average_rents_url=self.average_rents_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )
