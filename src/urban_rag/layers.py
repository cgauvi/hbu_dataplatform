"""Which medallion layer each asset belongs to, declared once.

Two things have to agree about a layer and they are set in different places:
the ``s3://<bucket>/<layer>/<asset>/...`` prefix an asset writes to, and the
``["<layer>", "<asset>"]`` key Dagster shows it under. Both are derived from
`ASSET_LAYERS` here rather than spelled out at each site, because the failure
mode of spelling them twice is an asset whose key says `silver` and whose
parquet lands under `bronze/`, which nothing would catch.

The three layers, and what a reader is entitled to assume of each:

**bronze** - what a publisher returned, plus the columns that say which
snapshot it is (`scrape_date`, `scraped_at`, `source_*`). Nothing is repaired,
renamed or reconciled: self-intersecting rings survive, CMHC's respellings
survive, a column the source types as text stays text. A bronze asset fails
only when the *fetch* fails. Scoping a query - one borough's outline handed to
Infolot, the Montreal slice of a nation-wide survey - is a bound on what was
asked for, not an interpretation of what came back, so it stays bronze.

**silver** - the same facts at this platform's own grain and vocabulary:
EPSG:4326, valid geometry, the crosswalks in `urban_rag.partitions` applied,
one row per declared grain. This is the layer that is allowed to refuse: a
CMHC quartier the crosswalk names but the workbook does not publish fails a
silver partition, and does not cost the bronze snapshot it was computed from.
Everything here is (geo)parquet in the tree, including the joins that are
*computed* in PostGIS - see `urban_rag.postgis`.

**gold** - one question, answered at the grain whoever asks it reads. Named
for the question rather than for the join behind it.

Postgres is a serving copy of silver and gold, never the only copy of either,
and **the schema a table is in is the layer its asset is in**: `silver/
vacancy_rates` in the tree is `silver.vacancy_rates` in the database, and
`gold/lot_profiles` is `gold.lot_profiles`. `urban_rag.warehouse` derives that
from this table rather than writing a schema name down twice, the same way
`ParquetStore.partition_dir` derives its prefix - so an asset moved between
layers moves its table with it.

Two things in Postgres are outside that rule and are not exceptions to it.
`rag.lots`, `rag.buildings` and `rag.features` are *bronze* snapshots loaded
into PostGIS because the silver joins are computed over them there, and
`rag.chunks` is the vector index `document_index` publishes; neither is a
silver or gold dataset's own table.

The tree is the record - a table can be rebuilt from it, and losing the
database costs a reload rather than a re-scrape, which for a live municipal
source is not something a later date can undo.
"""

from __future__ import annotations

from enum import Enum


class Layer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

    def __str__(self) -> str:  # so f"{layer}" is "bronze", not "Layer.BRONZE"
        return self.value


#: Every asset in the code location, and the layer it belongs to. Keyed by the
#: bare asset name - the last element of its `AssetKey`, which is also the
#: directory it owns in the tree.
#:
#: `definitions.py` asserts this covers exactly the assets it registers, so an
#: asset added without a layer is a load error rather than a `KeyError` on the
#: first materialization.
ASSET_LAYERS: dict[str, Layer] = {
    # -- bronze: one publisher each, as published ---------------------------
    "spectrum_table_catalog": Layer.BRONZE,
    "neighborhood_features": Layer.BRONZE,
    "reference_neighborhoods": Layer.BRONZE,
    "neighborhood_lots": Layer.BRONZE,
    "neighborhood_buildings": Layer.BRONZE,
    "cmhc_vacancy_survey": Layer.BRONZE,
    "cmhc_rent_survey": Layer.BRONZE,
    "street_network": Layer.BRONZE,
    "linked_documents": Layer.BRONZE,
    "montreal_residential_costs": Layer.BRONZE,
    "montreal_nonresidential_costs": Layer.BRONZE,
    "property_assessment_roll": Layer.BRONZE,
    "uniformized_property_wealth": Layer.BRONZE,
    "montreal_commercial_rents": Layer.BRONZE,
    "commercial_rent_index": Layer.BRONZE,
    # -- silver: this platform's grain and vocabulary -----------------------
    "assessment_units": Layer.SILVER,
    "lot_assessed_values": Layer.SILVER,
    "lot_assessment_comparables": Layer.SILVER,
    "commercial_rents": Layer.SILVER,
    "vacancy_rates": Layer.SILVER,
    "average_rents": Layer.SILVER,
    "building_lot_intersections": Layer.SILVER,
    "neighborhood_streets": Layer.SILVER,
    "lot_frontage": Layer.SILVER,
    "document_chunks": Layer.SILVER,
    "document_embeddings": Layer.SILVER,
    "zoning_grid_columns": Layer.SILVER,
    "lot_zoning_envelopes": Layer.SILVER,
    "lot_buildable_setbacks": Layer.SILVER,
    "lot_development_programs": Layer.SILVER,
    # -- gold: a question, answered -----------------------------------------
    "lot_profiles": Layer.GOLD,
    "lot_highest_best_use": Layer.GOLD,
    "lot_redevelopment_gap": Layer.GOLD,
    "lot_building_massing": Layer.GOLD,
    "lot_investment_opportunities": Layer.GOLD,
    "document_index": Layer.GOLD,
}


def layer_of(asset_name: str) -> Layer:
    """The layer ``asset_name`` belongs to.

    Raises rather than defaulting: an asset with no declared layer has no
    prefix to write under, and guessing one would put its parquet somewhere
    nothing else looks.
    """
    try:
        return ASSET_LAYERS[asset_name]
    except KeyError:
        raise KeyError(
            f"{asset_name!r} has no layer in urban_rag.layers.ASSET_LAYERS; "
            f"add it there. Known assets: {', '.join(sorted(ASSET_LAYERS))}"
        ) from None


def key_prefix(asset_name: str) -> list[str]:
    """The `@asset(key_prefix=...)` value for ``asset_name``.

    Only the prefix, so `context.asset_key.path[-1]` stays the bare name that
    `ParquetStore.partition_dir` and every cross-asset read are keyed on.
    """
    return [str(layer_of(asset_name))]


def assets_in(layer: Layer) -> tuple[str, ...]:
    """Every declared asset in ``layer``, in declaration order."""
    return tuple(name for name, value in ASSET_LAYERS.items() if value is layer)
