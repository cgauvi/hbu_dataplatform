"""Dagster resource for Adresses Québec, the province's civic addresses."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.sources.addresses.client import (
    DEFAULT_SERVICE_URL as ADDRESSES_SERVICE_URL,
    DEFAULT_WMS_URL as ADDRESSES_WMS_URL,
    MAX_RECORD_COUNT as ADDRESSES_MAX_RECORD_COUNT,
    AdressesQuebecClient,
)


class AdressesQuebecResource(ConfigurableResource):
    """Connection settings for Adresses Quebec, the province's civic addresses.

    An ArcGIS MapServer at the MRNF - see `hbu_dataplatform.sources.addresses.client`, which
    also says why the REST layer is read rather than the WMS endpoint the
    product is advertised under. Same posture as `QuebecZoningResource` and
    `InfolotResource`: paced and patient against a live provincial server with
    no quota to spend.

    The page size is the one setting worth touching, and only downwards. The
    service caps a response at `ADDRESSES_MAX_RECORD_COUNT` and a larger value
    is silently truncated rather than refused, so the client clamps it; a
    *smaller* one is the lever for a flaky link, at the cost of more round
    trips over a borough of a hundred thousand addresses.
    """

    service_url: str = ADDRESSES_SERVICE_URL
    #: Not read. Recorded so the published entry point travels with the
    #: configuration of the thing that stands in for it, and lands in the
    #: bronze asset's metadata as `source_wms_url`.
    wms_url: str = ADDRESSES_WMS_URL
    timeout_seconds: float = 120.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every request, in seconds."
    )
    max_retries: int = 3
    page_size: int = Field(
        default=ADDRESSES_MAX_RECORD_COUNT,
        description=(
            "Addresses per page; the service caps a response at 1000 and "
            "truncates a larger request rather than refusing it."
        ),
    )
    max_pages: int = Field(
        default=5000,
        description=(
            "Stop after this many full pages. A guard against a service that "
            "ignores resultOffset, which would otherwise page for ever."
        ),
    )
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def client(self) -> AdressesQuebecClient:
        return AdressesQuebecClient(
            self.service_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            page_size=self.page_size,
            max_pages=self.max_pages,
            ca_bundle=self.ca_bundle,
        )
