"""Dagster resources: the source connections, the output tree, the PDF cache."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from urban_rag.rag.documents import (
    DEFAULT_MAX_TOKENS,
    DEFAULT_OVERLAP_TOKENS,
    PdfFetcher,
)
from urban_rag.rag.embeddings import (
    DEFAULT_MODEL,
    SentenceTransformerEmbeddings,
    ModelTokenRuler,
    cached_embeddings,
)
from urban_rag.rag.pgvector import PgSettings, PgVectorStore
from urban_rag.postgis import connect as postgis_connect
from urban_rag.bdoi import DEFAULT_BASE_URL as BDOI_BASE_URL, BdoiFetcher
from urban_rag.cmhc import (
    AVERAGE_RENTS_READING_MODE_URL,
    DEFAULT_BASE_URL as CMHC_BASE_URL,
    SURVEY_YEAR_VAR,
    CmhcFetcher,
    CmhcReadingModeFetcher,
    default_survey_year,
)
from urban_rag.estimator import (
    DEFAULT_BASE_URL as ESTIMATOR_BASE_URL,
    MONTREAL_CITY_ID,
    EstimatorClient,
)
from urban_rag.crspi import (
    DEFAULT_BASE_URL as CRSPI_BASE_URL,
    DEFAULT_TABLE_ID as CRSPI_TABLE_ID,
    CrspiFetcher,
)
from urban_rag.cubf import (
    DEFAULT_URL as CUBF_URL,
    CubfFetcher,
)
from urban_rag.marketbeat import (
    DEFAULT_LANDING_URL as MARKETBEAT_LANDING_URL,
    MarketBeatFetcher,
)
from urban_rag.open_data import (
    DEFAULT_BASE_URL as OPEN_DATA_BASE_URL,
    CkanClient,
)
from urban_rag.adresses_quebec import (
    DEFAULT_SERVICE_URL as ADDRESSES_SERVICE_URL,
    DEFAULT_WMS_URL as ADDRESSES_WMS_URL,
    MAX_RECORD_COUNT as ADDRESSES_MAX_RECORD_COUNT,
    AdressesQuebecClient,
)
from urban_rag.quebec import (
    DEFAULT_BATCH_SIZE as QUEBEC_BATCH_SIZE,
    DEFAULT_GRID_URL as QUEBEC_GRID_URL,
    DEFAULT_SHEET_URL_TEMPLATE as QUEBEC_SHEET_URL_TEMPLATE,
    DEFAULT_ZONING_LAYER_URL as QUEBEC_ZONING_LAYER_URL,
    QuebecZoningClient,
)
from urban_rag.saguenay import (
    GRID_PDF_URL as SAGUENAY_GRID_PDF_URL,
    ZONE_LOOKUP_URL as SAGUENAY_ZONE_LOOKUP_URL,
    SaguenayZoningClient,
)
from urban_rag.rfu import (
    DEFAULT_BASE_URL as RFU_BASE_URL,
    RFU_YEAR_VAR,
    default_rfu_year,
)
from urban_rag.role_foncier import (
    DEFAULT_BASE_URL as ROLE_BASE_URL,
    ROLL_YEAR_VAR,
    RoleFetcher,
    default_roll_year,
)
from urban_rag.rqtt import (
    DEFAULT_BASE_URL as RQTT_BASE_URL,
    RqttFetcher,
)
from urban_rag.infolot import (
    DEFAULT_BASE_URL as INFOLOT_BASE_URL,
    DEFAULT_BATCH_SIZE,
    LOT_LAYER,
    InfolotClient,
)
from urban_rag.layers import Layer, layer_of
from urban_rag.spectrum import DEFAULT_BASE_URL, SpectrumClient
from urban_rag.storage import join


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


class ParquetStore(ConfigurableResource):
    """The output tree: medallion layer, then asset, then scrape date, then
    borough.

        <root>/bronze/spectrum_table_catalog/2026-08-20/
        <root>/bronze/neighborhood_features/2026-08-20/VSMPE/
        <root>/silver/building_lot_intersections/2026-08-20/VSMPE/
        <root>/gold/lot_profiles/2026-08-20/VSMPE/

    ``root_dir`` is `output_root()` in the real code location, so the same
    keys address a directory on disk or an ``s3://<S3_BUCKET>/`` prefix.

    The layer is looked up from `urban_rag.layers` rather than passed in, so a
    caller reading an upstream asset's output does not have to know which layer
    that asset lives in: `partition_dir(neighborhood_lots...)` finds `bronze/`
    on its own, and moving an asset between layers is one edit in one table.
    That table also supplies the asset's Dagster key prefix, which is what
    keeps the key and the path from drifting apart.

    Below the layer, keyed by asset name rather than by source system so that
    every asset owns one prefix: a partition can be listed, copied or dropped
    without touching what another asset wrote for the same day. The keys are
    bare values rather than hive ``key=value`` pairs, so `neighborhood` and
    `scrape_date` are written as columns instead of being recovered from the
    path.
    """

    root_dir: str

    def partition_dir(
        self, asset: str, scrape_date: str, neighborhood: str | None = None
    ) -> str:
        parts = [str(layer_of(asset)), asset, scrape_date]
        if neighborhood is not None:
            parts.append(neighborhood)
        return join(self.root_dir, *parts)

    def layer_dir(self, layer: Layer) -> str:
        """Everything one layer holds - the prefix to list, copy or drop."""
        return join(self.root_dir, str(layer))


class OpenDataResource(ConfigurableResource):
    """Connection settings for donnees.montreal.ca, the city's CKAN portal.

    Same posture as `SpectrumResource`: paced and patient rather than fast,
    since this is a public municipal server with no quota to spend.
    """

    base_url: str = OPEN_DATA_BASE_URL
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


class QuebecOpenDataResource(OpenDataResource):
    """Connection settings for Données Québec, where Quebec City publishes.

    The same CKAN API as the city of Montreal's portal - `OpenDataResource`
    with another base URL - and kept as its own resource for the reason
    `RfuResource` is: two publishers, two licences, two release cadences, and
    a run pointed at one should not be able to silently read the other. It is
    what `reference_neighborhoods` reads the arrondissement layer from and
    `street_network` the public ways.
    """

    base_url: str = RFU_BASE_URL


class QuebecZoningResource(ConfigurableResource):
    """Connection settings for Quebec City's zoning layer and its grid.

    The layer is an ArcGIS Online feature service and the grid a workbook on
    the city's map server - see `urban_rag.quebec`. Same posture as
    `InfolotResource`, which reads the same kind of service: paced and
    patient, since one borough is a few batched requests against a live
    municipal server with no quota to spend.
    """

    layer_url: str = QUEBEC_ZONING_LAYER_URL
    grid_url: str = QUEBEC_GRID_URL
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


class SaguenayZoningResource(ConfigurableResource):
    """Connection settings for Saguenay's zone lookup and its grid documents.

    Two plain HTTP endpoints rather than a feature service - see
    `urban_rag.saguenay`. The default pause is shorter than the other
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


class AdressesQuebecResource(ConfigurableResource):
    """Connection settings for Adresses Quebec, the province's civic addresses.

    An ArcGIS MapServer at the MRNF - see `urban_rag.adresses_quebec`, which
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
    last - see `urban_rag.rfu.default_rfu_year` for why that is resolved from
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


class BdoiResource(ConfigurableResource):
    """Cache settings for StatCan's Open Database of Buildings.

    Deliberately not part of `ParquetStore`: a published BDOI extract does
    not change, so the cache is keyed by filename and shared across every
    scrape date, while everything in the parquet tree is a dated snapshot. It
    is also always local - it is a download cache, not pipeline output.
    """

    cache_dir: str
    base_url: str = BDOI_BASE_URL
    timeout_seconds: float = 300.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every download, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def fetcher(self) -> BdoiFetcher:
        return BdoiFetcher(
            cache_dir=self.cache_dir,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class CmhcResource(ConfigurableResource):
    """Which CMHC Rental Market Survey to read, and where to cache it.

    Same posture as `BdoiResource`: a published survey year is final, so the
    workbook is cached by filename outside the partition tree and shared
    across every scrape date, and the cache is always local - it is a download
    cache, not pipeline output.

    `survey_year` is config rather than a partition dimension because the
    survey is annual and the pipeline's date axis is the scrape date. It
    defaults to $URBAN_RAG_CMHC_SURVEY_YEAR, so a run can be pointed at
    another year without restating `cache_dir` - which `--config-json` would
    otherwise replace, since it overwrites a resource's config wholesale.
    """

    cache_dir: str
    survey_year: int = Field(
        default_factory=default_survey_year,
        description=(
            f"Survey year to read. Defaults to ${SURVEY_YEAR_VAR}, else the "
            "latest published one. An unpublished year answers 404."
        ),
    )
    base_url: str = CMHC_BASE_URL
    average_rents_url: str = AVERAGE_RENTS_READING_MODE_URL
    timeout_seconds: float = 120.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every download, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def fetcher(self) -> CmhcFetcher:
        return CmhcFetcher(
            cache_dir=self.cache_dir,
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )

    def reading_mode_fetcher(self) -> CmhcReadingModeFetcher:
        return CmhcReadingModeFetcher(
            average_rents_url=self.average_rents_url,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
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
    disk to be read at all - see `urban_rag.role_foncier`.

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


class PdfCache(ConfigurableResource):
    """Where the linked PDFs are downloaded to, and how politely.

    Deliberately not part of `ParquetStore`: a published resolution is
    immutable, so the cache is keyed by URL and shared across every scrape
    date, while everything in the parquet tree is a dated snapshot. It is also
    always local - it is a download cache, not pipeline output.
    """

    cache_dir: str
    timeout_seconds: float = 60.0
    request_delay_seconds: float = Field(
        default=0.25, description="Pause before every download, in seconds."
    )
    max_retries: int = 3
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle to verify TLS against. Defaults to REQUESTS_CA_BUNDLE, "
            "CURL_CA_BUNDLE or SSL_CERT_FILE, whichever is set."
        ),
    )

    def fetcher(self) -> PdfFetcher:
        return PdfFetcher(
            cache_dir=self.cache_dir,
            timeout_seconds=self.timeout_seconds,
            request_delay_seconds=self.request_delay_seconds,
            max_retries=self.max_retries,
            ca_bundle=self.ca_bundle,
        )


class EmbeddingModel(ConfigurableResource):
    """The encoder, and the chunk geometry measured in its tokens."""

    model_name: str = DEFAULT_MODEL
    device: str | None = Field(
        default=None, description="'cuda', 'cpu', ... Defaults to cuda when available."
    )
    cache_dir: str | None = Field(
        default=None, description="Model cache. Defaults to the HF_HOME cache."
    )
    ca_bundle: str | None = Field(
        default=None,
        description=(
            "PEM bundle for huggingface.co. Defaults to certifi, which is what "
            "the hub is signed against - not the ambient corporate root."
        ),
    )
    batch_size: int = 16
    max_tokens: int = Field(
        default=DEFAULT_MAX_TOKENS, description="Token budget per chunk."
    )
    overlap_tokens: int = Field(
        default=DEFAULT_OVERLAP_TOKENS,
        description="Tokens repeated between consecutive chunks.",
    )

    def encoder(self) -> SentenceTransformerEmbeddings:
        return cached_embeddings(
            self.model_name,
            device=self.device,
            batch_size=self.batch_size,
            cache_dir=self.cache_dir,
            ca_bundle=self.ca_bundle,
        )

    def ruler(self) -> ModelTokenRuler:
        """Tokenizer only: chunking does not need the weights loaded."""
        return self.encoder().ruler


class PgVectorResource(ConfigurableResource):
    """The shared vector store: Postgres on RDS with the pgvector extension.

    Every field defaults to `None`, which means "whatever the corresponding
    `URBAN_RAG_PG_*` variable says" - see `rag.pgvector.PgSettings.from_env`.
    Configuring it that way rather than in `definitions.py` keeps one endpoint
    for the code location, the `urban-rag` CLI and anything else that opens the
    store, and keeps the endpoint out of the repository.

    Never put a password in the code location. Either name a Secrets Manager
    secret, use IAM authentication, or pass `EnvVar`::

        PgVectorResource(password=EnvVar("URBAN_RAG_PG_PASSWORD"))
    """

    host: str | None = Field(
        default=None, description="RDS endpoint, e.g. <name>.<id>.<region>.rds.amazonaws.com."
    )
    hostaddr: str | None = Field(
        default=None,
        description="Optional tunnel address; host is still used for TLS verification.",
    )
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = Field(
        default=None,
        description="Prefer secret_id or iam_auth; pass EnvVar(...), never a literal.",
    )
    secret_id: str | None = Field(
        default=None,
        description="Secrets Manager secret holding {'username', 'password'}.",
    )
    iam_auth: bool | None = Field(
        default=None,
        description="Sign each connection with an RDS IAM auth token instead.",
    )
    region: str | None = None
    sslmode: str | None = Field(
        default=None,
        description="Defaults to verify-full, which needs the RDS CA bundle.",
    )
    sslrootcert: str | None = None
    dsn: str | None = Field(
        default=None,
        description="Full libpq connection string; overrides the fields above.",
    )
    db_schema: str | None = None
    table: str | None = None
    ef_search: int | None = Field(
        default=None, description="HNSW candidate list at search time."
    )
    prune_superseded: bool = Field(
        default=True,
        description=(
            "After loading a partition, delete that borough's older scrape "
            "dates - the snapshot semantics the parquet tree already has."
        ),
    )

    def settings(self) -> PgSettings:
        return PgSettings.from_env(
            host=self.host,
            hostaddr=self.hostaddr,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            secret_id=self.secret_id,
            iam_auth=self.iam_auth,
            region=self.region,
            sslmode=self.sslmode,
            sslrootcert=self.sslrootcert,
            dsn=self.dsn,
            db_schema=self.db_schema,
            table=self.table,
            ef_search=self.ef_search,
        )

    def store(self) -> PgVectorStore:
        return PgVectorStore(self.settings())


class PostgisResource(ConfigurableResource):
    """The same Postgres database as `PgVectorResource`, for everything that is
    not the vector store: the PostGIS working set (`rag.lots`,
    `rag.buildings`, `rag.features`) and every `silver.*`/`gold.*` table the
    assets publish through `urban_rag.warehouse`.

    A separate resource rather than reusing `PgVectorResource` because that
    class's `table`/`ef_search`/`prune_superseded` fields are about the vector
    index and mean nothing here - but every connection field means the same
    thing, resolved the same way: an explicit value, else the corresponding
    `URBAN_RAG_PG_*` variable. See `rag.pgvector.PgSettings.from_env`.
    """

    host: str | None = None
    hostaddr: str | None = None
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = Field(
        default=None,
        description="Prefer secret_id or iam_auth; pass EnvVar(...), never a literal.",
    )
    secret_id: str | None = Field(
        default=None,
        description="Secrets Manager secret holding {'username', 'password'}.",
    )
    iam_auth: bool | None = None
    region: str | None = None
    sslmode: str | None = None
    sslrootcert: str | None = None
    dsn: str | None = None

    def settings(self) -> PgSettings:
        return PgSettings.from_env(
            host=self.host,
            hostaddr=self.hostaddr,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            secret_id=self.secret_id,
            iam_auth=self.iam_auth,
            region=self.region,
            sslmode=self.sslmode,
            sslrootcert=self.sslrootcert,
            dsn=self.dsn,
        )

    def connect(self):
        return postgis_connect(self.settings())
