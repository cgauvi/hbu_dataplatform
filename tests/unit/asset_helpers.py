"""Helpers shared by the asset tests.

A plain module rather than a `conftest.py` because these are functions the
tests call, not fixtures pytest injects.
"""

from __future__ import annotations


def node_name(asset_def) -> str:
    """The op name Dagster gives ``asset_def``, for looking its events up by.

    Every asset in this project carries a medallion key prefix (see
    `urban_rag.layers`), so `building_lot_intersections` is the node
    `silver__building_lot_intersections`. Derived from the definition rather than
    written out, so moving an asset between layers does not mean editing the
    same string in every test that asserts on its metadata.
    """
    return "__".join(asset_def.key.path)


def materialization_metadata(result, asset_def) -> dict:
    """The metadata of the one materialization ``asset_def`` emitted."""
    events = result.asset_materializations_for_node(node_name(asset_def))
    assert events, f"{node_name(asset_def)} materialized nothing"
    return events[0].metadata
