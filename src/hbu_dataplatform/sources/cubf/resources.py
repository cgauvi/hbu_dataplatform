"""Dagster resource for the MEFQ's use-code list."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.sources.cubf.client import DEFAULT_URL as CUBF_URL, CubfFetcher


class CubfResource(ConfigurableResource):
    """Where to fetch the MEFQ's use-code list from.

    No `cache_dir`, unlike `RoleResource` beside it, and the contrast is the
    point: a roll year is final, so 572 MB is cached by filename forever, while
    this is 185 kB at a URL with no year in it that the ministry reissues
    whenever the manual is amended. What can be revised is re-read; what is
    published once and never changed is cached - the rule `CrspiResource` and
    `EstimatorResource` already follow.

    `url` is the CDN path rather than the landing page, and is config so a
    ministry that moves the file can be pointed at the new address without a
    release.
    """

    url: str = CUBF_URL
    timeout_seconds: float = 120.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before the download, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def fetcher(self) -> CubfFetcher:
        return CubfFetcher(
            url=self.url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )
