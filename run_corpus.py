"""Run rag_corpus_job for one (date, borough), with the stale-scrape waiver.

Why this exists rather than `make corpus`:

* `dagster asset materialize` builds a proper MultiPartitionKey from
  `--partition`, but has no `--tag`, so it cannot carry the waiver the guard
  wants for a month that is not the current one.
* `dagster job execute --tags` can carry the waiver, but the partition then
  arrives as a bare string and `partitions.axes.borough_partition_of` wants a
  MultiPartitionKey - `'str' object has no attribute 'keys_by_dimension'`.

`execute_in_process` takes both: it resolves the partition key against the
job's own partitions_def, and passes tags through to the run the guard reads.
This is the Launchpad's path, scripted.

Usage:
    python run_corpus.py <YYYY-MM-01> <BOROUGH> [--allow-stale]
"""

from __future__ import annotations

import sys

from dagster import DagsterInstance
from dagster._core.definitions.partitions.context import partition_loading_context

from hbu_dataplatform.definitions import defs
from hbu_dataplatform.partitions.guards import ALLOW_STALE_SCRAPE_TAG


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2

    date, borough = argv[0], argv[1]
    tags = {}
    if "--allow-stale" in argv:
        # Named, not silent: the run carries the waiver in its tags, which is
        # how anyone looking at it afterwards can see the month was asked for
        # deliberately rather than by accident.
        tags[ALLOW_STALE_SCRAPE_TAG] = "true"

    job = defs.get_job_def("rag_corpus_job")
    key = f"{date}|{borough}"
    print(f"rag_corpus_job  partition={key}  tags={tags or '{}'}")

    # The borough axis is a DynamicPartitionsDefinition whose keys live in the
    # instance, and `execute_in_process` validates the partition key BEFORE it
    # builds one - so without a loading context it cannot tell whether VSMPE
    # exists and fails with "The instance is not available to load
    # partitions". Same seam `tests/unit/conftest.py` uses, pointed at the
    # real instance instead of a seeded ephemeral one.
    instance = DagsterInstance.get()
    with partition_loading_context(dynamic_partitions_store=instance):
        result = job.execute_in_process(
            partition_key=key,
            tags=tags,
            instance=instance,
            raise_on_error=False,
        )
    print(f"success={result.success}")
    for event in result.all_events:
        if event.is_step_failure:
            print(f"FAILED {event.step_key}: {event.event_specific_data.error}")
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
