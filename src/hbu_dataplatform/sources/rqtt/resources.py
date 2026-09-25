"""Dagster resource for the RQTT, Quebec's province-wide road network."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.sources.rqtt.client import (
    DEFAULT_BASE_URL as RQTT_BASE_URL,
    RqttFetcher,
)


class RqttResource(ConfigurableResource):
    """Where to cache the RQTT, Quebec's province-wide road network.

    The same posture as `RoleResource`, and for one of the same two reasons:
    the archive is 390 MB and the GeoPackage unpacked beside it is 1.27 GB, so
    it is cached outside the partition tree and shared across every scrape
    date, and the cache is always local, since a GeoPackage has to be on a
    filesystem to be read at all.

    Where it *differs* from the roll is the reason there is no `version` field
    to match `RoleResource.roll_year`. A roll year is in the URL, so a year can
    be asked for; the RQTT's URL has no version in it at all and always serves
    whatever is current. So the vintage is discovered rather than requested -
    `RqttFetcher` reads it from `Last-Modified` and names the cache entry after
    it - and it travels back out of `fetch` so the asset can record which one a
    partition was built from. Pinning a vintage here would be a promise this
    source cannot keep: once the MRNF rotates the file, the one before it has
    no URL any more.

    The file is reissued three times a year (April, July, December) while this
    pipeline's date axis is monthly, so most months find the cache already
    holding the vintage the `HEAD` names and move no bytes at all.
    """

    cache_dir: str
    base_url: str = RQTT_BASE_URL
    # 30 minutes, as `RoleResource`: this is a 390 MB download on a link the
    # rest of the pipeline never stresses, and a retry costs the whole file.
    timeout_seconds: float = 1800.0
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

    def fetcher(self) -> RqttFetcher:
        return RqttFetcher(
            cache_dir=self.cache_dir,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )
