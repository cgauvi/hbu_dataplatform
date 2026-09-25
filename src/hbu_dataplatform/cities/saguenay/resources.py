"""Dagster resource for Saguenay's zone lookup and grid documents."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.cities.saguenay.zoning import (
    GRID_PDF_URL as SAGUENAY_GRID_PDF_URL,
    ZONE_LOOKUP_URL as SAGUENAY_ZONE_LOOKUP_URL,
    SaguenayZoningClient,
)


class SaguenayZoningResource(ConfigurableResource):
    """Connection settings for Saguenay's zone lookup and its grid documents.

    Two plain HTTP endpoints rather than a feature service - see
    `hbu_dataplatform.cities.saguenay.zoning`. The default pause is shorter than the other
    publishers' because the index costs one request per zone and there are
    2,837 of them: at a quarter of a second that is twelve minutes of waiting
    on its own, and the endpoint is a cache in front of a read-only API rather
    than a live query service. Raise it if the city asks.
    """

    lookup_url: str = SAGUENAY_ZONE_LOOKUP_URL
    grid_url: str = SAGUENAY_GRID_PDF_URL
    timeout_seconds: float = 120.0
    request_delay_seconds: float = Field(
        default=0.1, description="Pause before every request, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def client(self) -> SaguenayZoningClient:
        return SaguenayZoningClient(
            lookup_url=self.lookup_url,
            grid_url=self.grid_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )
