"""Dagster resource for Infolot, the Registre foncier's lot service."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.sources.infolot.client import (
    DEFAULT_BASE_URL as INFOLOT_BASE_URL,
    DEFAULT_BATCH_SIZE,
    LOT_LAYER,
    InfolotClient,
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
