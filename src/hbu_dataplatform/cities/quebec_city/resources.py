"""Dagster resources for Quebec City's zoning service, its conseils de
quartier and its Commission d'urbanisme et de conservation."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.cities.quebec_city.council.councils import (
    DEFAULT_LISTING_URL_TEMPLATE as COUNCIL_LISTING_URL_TEMPLATE,
    CouncilFetcher,
)
from hbu_dataplatform.cities.quebec_city.cucq.portal import (
    DEFAULT_API_VERSION as CUCQ_API_VERSION,
    DEFAULT_BLOB_URL as CUCQ_BLOB_URL,
    QUERY_KEY_ENV as CUCQ_QUERY_KEY_ENV,
    DEFAULT_SEARCH_ENDPOINT as CUCQ_SEARCH_ENDPOINT,
    PAGE_SIZE as CUCQ_PAGE_SIZE,
    CucqPortalClient,
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


class CucqMinutesResource(ConfigurableResource):
    """The decisions portal's search index, where the Commission d'urbanisme
    et de conservation de Québec's minutes are listed.

    The portal is a page over an Azure Cognitive Search index, and the page
    ships the index's query key to every browser - see
    `hbu_dataplatform.cities.quebec_city.cucq.portal`. The endpoint and the
    blob host here are what that page carries as of 2026-10. The key is a
    credential, public or not, so it is not a default: pass ``query_key``
    (``EnvVar`` is the usual way) or set ``CUCQ_SEARCH_QUERY_KEY``, copying
    the ``queryKey`` off the page's ``js/scripts.js``; a rotation by the
    city is a new value there and not a new client.

    The PDFs the index lists go through `PdfCache`, whose fetcher the asset
    hands over - a filed minute never changes, so it is cached by URL and
    shared across scrape dates like every other published document.
    """

    search_endpoint: str = CUCQ_SEARCH_ENDPOINT
    api_version: str = CUCQ_API_VERSION
    query_key: str | None = Field(
        default=None,
        description=(
            "The index's query-only key - the one decisions.ville.quebec.qc.ca "
            f"hands every browser in js/scripts.js. Read from {CUCQ_QUERY_KEY_ENV} "
            "when left unset; pass EnvVar(...), never a literal."
        ),
    )
    blob_url: str = Field(
        default=CUCQ_BLOB_URL,
        description="Where a listed document is served from, by its storage name.",
    )
    page_size: int = Field(
        default=CUCQ_PAGE_SIZE,
        description="Documents per request; the index caps it at 1000.",
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

    def client(self) -> CucqPortalClient:
        return CucqPortalClient(
            endpoint=self.search_endpoint,
            api_version=self.api_version,
            query_key=self.query_key,
            blob_url=self.blob_url,
            page_size=self.page_size,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )
