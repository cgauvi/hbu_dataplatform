"""The code location's two import-time assertions, exercised on purpose.

Both are the same shape: something that has to be spelled at each asset, and a
check that turns forgetting it into a load error rather than into a wrong
answer noticed much later. `_assert_layers_declared` does it for the medallion
layer; `_assert_bronze_assets_guarded` does it for the scrape-month guard.

Testing them means calling them against a doctored asset list, since by
construction the real one passes - that is what a load error means.
"""

# See the note in `test_guards.py`: assets and PEP 563 annotations do not mix.

import pytest
from dagster import AssetExecutionContext, MaterializeResult, asset

from urban_rag import definitions
from urban_rag.guards import guard_current_scrape_month, guards_scrape_month
from urban_rag.layers import ASSET_LAYERS, Layer, layer_of
from urban_rag.partitions import date_partitions


def test_every_registered_bronze_asset_carries_the_guard():
    """The real code location, as it stands."""
    unguarded = sorted(
        definition.key.path[-1]
        for definition in definitions.ASSETS
        if layer_of(definition.key.path[-1]) is Layer.BRONZE
        and not guards_scrape_month(definition)
    )
    assert unguarded == []


def test_no_silver_or_gold_asset_carries_the_guard():
    """Backfilling a recomputation is a feature, so the guard must not spread.

    Silver and gold read bronze parquet that is already on disk; re-deriving
    them for a past month after a fixed crosswalk is exactly what they are for.
    """
    guarded = sorted(
        definition.key.path[-1]
        for definition in definitions.ASSETS
        if layer_of(definition.key.path[-1]) is not Layer.BRONZE
        and guards_scrape_month(definition)
    )
    assert guarded == []


def test_the_bronze_layer_is_not_accidentally_empty():
    """Both assertions above pass trivially if nothing is bronze."""
    bronze = [name for name, layer in ASSET_LAYERS.items() if layer is Layer.BRONZE]
    assert len(bronze) == 16


def test_an_unguarded_bronze_asset_is_a_load_error(monkeypatch):
    @asset(key_prefix=["bronze"], partitions_def=date_partitions)
    def forgetful_source(context: AssetExecutionContext) -> MaterializeResult:
        return MaterializeResult()

    monkeypatch.setitem(ASSET_LAYERS, "forgetful_source", Layer.BRONZE)
    monkeypatch.setattr(
        definitions, "ASSETS", [*definitions.ASSETS, forgetful_source]
    )

    with pytest.raises(ValueError) as raised:
        definitions._assert_bronze_assets_guarded()
    assert "forgetful_source" in str(raised.value)
    assert "guard_current_scrape_month" in str(raised.value)


def test_a_guarded_bronze_asset_passes_the_check(monkeypatch):
    """The same doctored list, with the decorator put back."""

    @asset(key_prefix=["bronze"], partitions_def=date_partitions)
    @guard_current_scrape_month
    def diligent_source(context: AssetExecutionContext) -> MaterializeResult:
        return MaterializeResult()

    monkeypatch.setitem(ASSET_LAYERS, "diligent_source", Layer.BRONZE)
    monkeypatch.setattr(
        definitions, "ASSETS", [*definitions.ASSETS, diligent_source]
    )

    definitions._assert_bronze_assets_guarded()


def test_a_silver_asset_needs_no_guard(monkeypatch):
    """The check is bronze-only, so an unguarded silver asset is fine."""

    @asset(key_prefix=["silver"], partitions_def=date_partitions)
    def derived(context: AssetExecutionContext) -> MaterializeResult:
        return MaterializeResult()

    monkeypatch.setitem(ASSET_LAYERS, "derived", Layer.SILVER)
    monkeypatch.setattr(definitions, "ASSETS", [*definitions.ASSETS, derived])

    definitions._assert_bronze_assets_guarded()
