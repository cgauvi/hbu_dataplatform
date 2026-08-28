"""Helpers shared by the asset tests.

A plain module rather than a `conftest.py` because these are functions the
tests call, not fixtures pytest injects.
"""

from __future__ import annotations

from contextlib import contextmanager

from urban_rag.resources import PostgisResource


def stub_publish(monkeypatch, module, *, pruned: int = 0) -> dict[str, object]:
    """Patch out one asset module's `urban_rag.warehouse.publish`.

    Every silver and gold asset writes its parquet and then upserts the same
    frames into `silver.*`/`gold.*`, which needs a database. What is worth
    checking offline is which datasets it publishes and what it hands over, so
    the call is recorded and the counts are made up.

    Returns the record: `{"datasets": {name: frame}, "partition": (nb, date)}`,
    filled in when the asset calls through. `PostgisResource.connect` is
    patched on the class rather than on an instance because Dagster rebuilds
    the resource before the run.
    """
    seen: dict[str, object] = {"datasets": {}, "partition": None, "calls": 0}

    @contextmanager
    def connect(self):
        yield object()

    def publish(connect_fn, datasets, *, neighborhood, scrape_date):
        seen["calls"] += 1
        seen["datasets"] = dict(datasets)
        seen["partition"] = (neighborhood, scrape_date)
        return {
            name: {
                "copied": len(frame),
                "duplicates": 0,
                "upserted": len(frame),
                "pruned": pruned,
            }
            for name, frame in datasets.items()
        }

    monkeypatch.setattr(PostgisResource, "connect", connect)
    monkeypatch.setattr(module, "publish", publish)
    return seen


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
