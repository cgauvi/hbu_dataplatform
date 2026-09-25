"""Dagster resource for the property assessment roll."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.sources.roll.client import (
    DEFAULT_BASE_URL as ROLE_BASE_URL,
    ROLL_YEAR_VAR,
    RoleFetcher,
    default_roll_year,
)


class RoleResource(ConfigurableResource):
    """Which property assessment roll to read, and where to cache it.

    Same posture as `BdoiResource` and `CmhcResource`: a published roll year is
    final - each municipality files once, between 15 August and 15 September of
    the preceding year - so the archive is cached by filename outside the
    partition tree and shared across every scrape date, and the cache is always
    local, since it is a download cache rather than pipeline output.

    It is a larger cache than either of theirs. The archive is 572 MB and the
    GeoPackage unpacked beside it is 2.8 GB, because a GeoPackage has to be on
    disk to be read at all - see `hbu_dataplatform.sources.roll.client`.

    `roll_year` is config rather than a partition dimension for the same reason
    `CmhcResource.survey_year` is: the roll is annual and this pipeline's date
    axis is the scrape date. It defaults to $URBAN_RAG_ROLL_YEAR, so a run can
    be pointed at another year without restating `cache_dir` - which
    `--config-json` would otherwise replace, since it overwrites a resource's
    config wholesale.
    """

    cache_dir: str
    roll_year: int = Field(
        default_factory=default_roll_year,
        description=(
            f"Fiscal year of the roll to read. Defaults to ${ROLL_YEAR_VAR}, "
            "else the latest published one. An unpublished year answers 404."
        ),
    )
    base_url: str = ROLE_BASE_URL
    # 30 minutes: this is a 572 MB download on a link the rest of the pipeline
    # never stresses, and a retry costs the whole file again.
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

    def fetcher(self) -> RoleFetcher:
        return RoleFetcher(
            cache_dir=self.cache_dir,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )
