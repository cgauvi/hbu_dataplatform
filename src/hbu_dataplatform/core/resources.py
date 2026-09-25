"""Dagster resources that belong to no publisher: the output tree, the
warehouse connection, and the generic CKAN client every portal shares."""

from __future__ import annotations

from dagster import ConfigurableResource
from pydantic import Field

from hbu_dataplatform.core.layers import Layer, layer_of
from hbu_dataplatform.core.open_data import CkanClient
from hbu_dataplatform.core.pg import PgSettings
from hbu_dataplatform.core.postgis import connect as postgis_connect
from hbu_dataplatform.core.storage import join


class ParquetStore(ConfigurableResource):
    """The output tree: medallion layer, then asset, then scrape date, then
    the spatial partition - a borough on the borough axis, a cut cell on the
    tile axis (see `hbu_dataplatform.partitions.axes`).

        <root>/bronze/spectrum_table_catalog/2026-08-20/
        <root>/bronze/neighborhood_features/2026-08-20/VSMPE/
        <root>/silver/building_lot_intersections/2026-08-20/0302303330102/
        <root>/gold/lot_profiles/2026-08-20/0302303330102/

    ``root_dir`` is `output_root()` in the real code location, so the same
    keys address a directory on disk or an ``s3://<S3_BUCKET>/`` prefix.

    The layer is looked up from `hbu_dataplatform.core.layers` rather than passed in, so a
    caller reading an upstream asset's output does not have to know which layer
    that asset lives in: `partition_dir(neighborhood_lots...)` finds `bronze/`
    on its own, and moving an asset between layers is one edit in one table.
    That table also supplies the asset's Dagster key prefix, which is what
    keeps the key and the path from drifting apart.

    Below the layer, keyed by asset name rather than by source system so that
    every asset owns one prefix: a partition can be listed, copied or dropped
    without touching what another asset wrote for the same day. The keys are
    bare values rather than hive ``key=value`` pairs, so the partition value
    and `scrape_date` are written as columns instead of being recovered from
    the path - which is also why a borough key and a cut cell can share the
    slot: the file says which it is.
    """

    root_dir: str

    def partition_dir(
        self, asset: str, scrape_date: str, partition: str | None = None
    ) -> str:
        """``asset``'s directory for ``scrape_date``, and ``partition`` when
        the asset has a spatial axis - the borough key or the tile."""
        parts = [str(layer_of(asset)), asset, scrape_date]
        if partition is not None:
            parts.append(partition)
        return join(self.root_dir, *parts)

    def layer_dir(self, layer: Layer) -> str:
        """Everything one layer holds - the prefix to list, copy or drop."""
        return join(self.root_dir, str(layer))


class CkanResource(ConfigurableResource):
    """Connection settings for a CKAN portal.

    Paced and patient rather than fast, since every portal this pipeline reads
    is a public server with no quota to spend. ``base_url`` has no default:
    each publisher is its own subclass - `OpenDataResource` for Montreal,
    `QuebecOpenDataResource` for Données Québec - so a run pointed at one
    cannot silently read the other.
    """

    base_url: str
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


class PostgisResource(ConfigurableResource):
    """The same Postgres database as `PgVectorResource`, for everything that is
    not the vector store: the PostGIS working set (`rag.lots`,
    `rag.buildings`, `rag.features`) and every `silver.*`/`gold.*` table the
    assets publish through `hbu_dataplatform.core.warehouse`.

    A separate resource rather than reusing `PgVectorResource` because that
    class's `table`/`ef_search`/`prune_superseded` fields are about the vector
    index and mean nothing here - but every connection field means the same
    thing, resolved the same way: an explicit value, else the corresponding
    `URBAN_RAG_PG_*` variable. See `core.pg.PgSettings.from_env`.
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
