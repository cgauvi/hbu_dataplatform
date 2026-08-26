"""The medallion declaration, and the two things that must not drift from it.

`urban_rag.layers.ASSET_LAYERS` is the single place a layer is named. Two
consumers read it - `ParquetStore.partition_dir` for the S3 prefix, and each
asset's `key_prefix` for the Dagster key - and the failure mode of them
disagreeing is an asset whose key says `silver` and whose parquet lands under
`bronze/`, which nothing else would catch.
"""

from __future__ import annotations

import pytest

from urban_rag.definitions import ASSETS, _assert_layers_declared
from urban_rag.layers import ASSET_LAYERS, Layer, key_prefix, layer_of
from urban_rag.resources import ParquetStore


def test_every_registered_asset_has_a_layer_and_vice_versa():
    # The same check the code location runs at import time; asserted here too
    # so the failure names the drift rather than only breaking `dagster dev`.
    _assert_layers_declared()


@pytest.mark.parametrize("asset_def", ASSETS, ids=lambda a: a.key.to_user_string())
def test_the_asset_key_prefix_is_the_declared_layer(asset_def):
    name = asset_def.key.path[-1]
    assert asset_def.key.path == [str(layer_of(name)), name]


@pytest.mark.parametrize("asset_def", ASSETS, ids=lambda a: a.key.to_user_string())
def test_the_output_prefix_matches_the_asset_key(asset_def):
    """The one that matters: key and path derived from the same declaration."""
    store = ParquetStore(root_dir="s3://bucket")
    name = asset_def.key.path[-1]

    assert store.partition_dir(name, "2026-08-20") == (
        f"s3://bucket/{'/'.join(asset_def.key.path)}/2026-08-20"
    )
    assert store.partition_dir(name, "2026-08-20", "VSMPE") == (
        f"s3://bucket/{'/'.join(asset_def.key.path)}/2026-08-20/VSMPE"
    )


def test_an_asset_with_no_declared_layer_is_refused():
    """Rather than defaulting: a guessed prefix is a file nothing looks for."""
    with pytest.raises(KeyError, match="has no layer"):
        layer_of("something_nobody_declared")
    with pytest.raises(KeyError, match="has no layer"):
        ParquetStore(root_dir="data").partition_dir("something_nobody_declared", "d")


def test_a_layer_renders_as_its_own_name():
    # `Layer` is a str enum, so an f-string must give "bronze" and not
    # "Layer.BRONZE" - the prefix is built by interpolation.
    assert f"{Layer.BRONZE}" == "bronze"
    assert key_prefix("neighborhood_lots") == ["bronze"]
    assert key_prefix("building_lot_intersections") == ["silver"]
    assert key_prefix("lot_profiles") == ["gold"]


def test_the_layers_are_only_the_three():
    assert set(ASSET_LAYERS.values()) == {Layer.BRONZE, Layer.SILVER, Layer.GOLD}
