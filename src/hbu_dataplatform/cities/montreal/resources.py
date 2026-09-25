"""Dagster resources for Montreal's own publishers: Spectrum, the city's CKAN
portal, the ZEF cost guide, Cushman & Wakefield and StatCan's rent index."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.cities.montreal.costs.estimator import (
    DEFAULT_BASE_URL as ESTIMATOR_BASE_URL,
    MONTREAL_CITY_ID,
    EstimatorClient,
)
from hbu_dataplatform.cities.montreal.registry import PORTAL_URL
from hbu_dataplatform.cities.montreal.rents.crspi import (
    DEFAULT_BASE_URL as CRSPI_BASE_URL,
    DEFAULT_TABLE_ID as CRSPI_TABLE_ID,
    CrspiFetcher,
)
from hbu_dataplatform.cities.montreal.rents.marketbeat import (
    DEFAULT_LANDING_URL as MARKETBEAT_LANDING_URL,
    MarketBeatFetcher,
)
from hbu_dataplatform.cities.montreal.spectrum import DEFAULT_BASE_URL, SpectrumClient
from hbu_dataplatform.core.resources import CkanResource


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


class OpenDataResource(CkanResource):
    """Connection settings for donnees.montreal.ca, the city's CKAN portal.

    Same posture as `SpectrumResource`: paced and patient rather than fast,
    since this is a public municipal server with no quota to spend.
    """

    base_url: str = PORTAL_URL


class EstimatorResource(ConfigurableResource):
    """Connection settings for the ZEF construction cost estimator.

    Same posture as `SpectrumResource`: paced and patient rather than fast.
    Nothing here needs the speed - the cost table is one 16 kB script on
    GitHub Pages, which is also why it has no download cache of its own the
    way `BdoiResource` and `CmhcResource` do. Their sources are a published
    extract and a published survey year, both final; this one is a live page
    that its publisher can revise on any day, so every scrape date fetches it
    again and keeps what it got.

    `city` is config rather than a partition dimension because the guide's
    city axis is not this pipeline's: nine markets are priced and one island
    is modelled. Both assets that read this are named for Montreal, so
    pointing it elsewhere is a thing to do deliberately - to diff Montreal
    against Toronto in a notebook - and not a thing to leave set.
    """

    base_url: str = ESTIMATOR_BASE_URL
    city: str = Field(
        default=MONTREAL_CITY_ID,
        description="`CITIES` id to take the rate column from: mtl, tor, van, ...",
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

    def client(self) -> EstimatorClient:
        return EstimatorClient(
            self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class MarketBeatResource(ConfigurableResource):
    """Connection settings for Cushman & Wakefield's Montreal MarketBeats.

    A download cache like `BdoiResource` and `RoleResource` have, and for the
    same reason: a published quarter is final, so only the first scrape date of
    a quarter pays for the two PDFs. The *landing page* is never cached - it is
    what says which quarter is current, and caching it would pin this pipeline
    to whichever quarter it first saw.

    `landing_url` is config rather than a constant so a borough outside Montreal
    could point at another city's MarketBeat page without a code change, the
    same latitude `EstimatorResource.city` gives. Both assets that read this are
    named for Montreal, so pointing it elsewhere is a deliberate act.
    """

    landing_url: str = MARKETBEAT_LANDING_URL
    cache_dir: str
    timeout_seconds: float = 120.0
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

    def fetcher(self) -> MarketBeatFetcher:
        return MarketBeatFetcher(
            cache_dir=self.cache_dir,
            landing_url=self.landing_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class CrspiResource(ConfigurableResource):
    """Connection settings for Statistics Canada's commercial rent index.

    No `cache_dir`, unlike the MarketBeat resource above and for the reason
    `EstimatorResource` has none: the table is 14 kB and is *revised*, so every
    scrape date fetches it again and keeps what it got. What can be revised is
    re-read; what is published once and never changed is cached.
    """

    table_id: str = CRSPI_TABLE_ID
    base_url: str = CRSPI_BASE_URL
    timeout_seconds: float = 120.0
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

    def fetcher(self) -> CrspiFetcher:
        return CrspiFetcher(
            table_id=self.table_id,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )
