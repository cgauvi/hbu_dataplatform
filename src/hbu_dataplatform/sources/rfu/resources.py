"""Dagster resource for the richesse foncière uniformisée."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.core.open_data import CkanClient
from hbu_dataplatform.sources.rfu.client import (
    DEFAULT_BASE_URL as RFU_BASE_URL,
    RFU_YEAR_VAR,
    default_rfu_year,
)


class RfuResource(ConfigurableResource):
    """Which year's *richesse fonciere uniformisee* to read, and from where.

    A second CKAN portal rather than a second client: Donnees Quebec runs the
    same API as the city's, so this differs from `OpenDataResource` only in its
    base URL and in carrying a year. Kept apart from it because they are two
    publishers with two licences and two release cadences, and a run pointed at
    one should not be able to silently read the other.

    `rfu_year` is config rather than a partition dimension, for the reason
    `RoleResource.roll_year` and `CmhcResource.survey_year` are: the RFU is
    annual and this pipeline's date axis is the scrape date. It defaults to
    $URBAN_RAG_RFU_YEAR and, unset, to whatever year the dataset publishes
    last - see `hbu_dataplatform.sources.rfu.client.default_rfu_year` for why that is resolved from
    the catalogue instead of pinned here.

    No cache_dir, unlike `RoleResource`: this is a 275 kB CSV, not a 572 MB
    archive, and re-fetching it per scrape date costs less than reasoning about
    when a cached copy went stale.
    """

    base_url: str = RFU_BASE_URL
    rfu_year: int | None = Field(
        default_factory=default_rfu_year,
        description=(
            f"Fiscal year of the RFU to read. Defaults to ${RFU_YEAR_VAR}, "
            "else the latest year the dataset publishes."
        ),
    )
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
