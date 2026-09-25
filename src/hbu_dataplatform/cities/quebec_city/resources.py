"""Dagster resources for Quebec City's zoning service and its conseils de quartier."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.cities.quebec_city.council.councils import (
    DEFAULT_LISTING_URL_TEMPLATE as COUNCIL_LISTING_URL_TEMPLATE,
    CouncilFetcher,
)
from hbu_dataplatform.cities.quebec_city.zoning import (
    DEFAULT_BATCH_SIZE as QUEBEC_BATCH_SIZE,
    DEFAULT_GRID_URL as QUEBEC_GRID_URL,
    DEFAULT_HERITAGE_SERVICE_URL as QUEBEC_HERITAGE_SERVICE_URL,
    DEFAULT_SHEET_URL_TEMPLATE as QUEBEC_SHEET_URL_TEMPLATE,
    DEFAULT_ZONING_LAYER_URL as QUEBEC_ZONING_LAYER_URL,
    QuebecZoningClient,
)
from hbu_dataplatform.rag.documents import PdfFetcher


class QuebecZoningResource(ConfigurableResource):
    """Connection settings for Quebec City's zoning layer and its grid.

    The layer is an ArcGIS Online feature service and the grid a workbook on
    the city's map server - see `hbu_dataplatform.cities.quebec_city.zoning`. Same posture as
    `InfolotResource`, which reads the same kind of service: paced and
    patient, since one borough is a few batched requests against a live
    municipal server with no quota to spend.
    """

    layer_url: str = QUEBEC_ZONING_LAYER_URL
    grid_url: str = QUEBEC_GRID_URL
    heritage_service_url: str = Field(
        default=QUEBEC_HERITAGE_SERVICE_URL,
        description=(
            "The heritage feature service; each layer in "
            "`quebec.HERITAGE_LAYERS` is read as `<url>/<layer id>`."
        ),
    )
    sheet_url_template: str = Field(
        default=QUEBEC_SHEET_URL_TEMPLATE,
        description=(
            "Template for one zone's grid sheet, with a `{zone}` placeholder. "
            "The URL it builds is written to each zone row as LIEN_GRILLE and "
            "is what the corpus assets download."
        ),
    )
    timeout_seconds: float = 120.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every request, in seconds."
    )
    max_retries: int = 3
    batch_size: int = Field(
        default=QUEBEC_BATCH_SIZE,
        description="Zones per `objectIds` batch; the service caps a response at 2000.",
    )
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def client(self) -> QuebecZoningClient:
        return QuebecZoningClient(
            self.layer_url,
            grid_url=self.grid_url,
            sheet_url_template=self.sheet_url_template,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            batch_size=self.batch_size,
            ca_bundle=self.ca_bundle,
        )


class CouncilMinutesResource(ConfigurableResource):
    """Quebec City's conseils de quartier: the host that lists their minutes
    and the pages on the trail from a minute to its decision.

    The PDFs themselves go through `PdfCache`, whose fetcher this one wraps -
    a filed minute or a sommaire never changes, so it is cached by URL and
    shared across scrape dates like every other published document. The
    listing and the consultation fiches are HTML and are read live every
    time: a fiche gains its report and its adoption date after the assembly.
    """

    listing_url_template: str = COUNCIL_LISTING_URL_TEMPLATE
    timeout_seconds: float = 60.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every page or download, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def fetcher(self, pdf_fetcher: PdfFetcher) -> CouncilFetcher:
        return CouncilFetcher(
            pdf_fetcher,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
            listing_url_template=self.listing_url_template,
        )
