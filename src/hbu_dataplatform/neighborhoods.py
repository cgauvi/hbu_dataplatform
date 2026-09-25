"""Register, list and retire neighborhood partition keys in the instance.

The neighborhood axis is dynamic - see `hbu_dataplatform.partitions` - so the set of
boroughs the pipeline scrapes lives in Dagster's instance storage rather than
in code. This is the command that edits it::

    python -m hbu_dataplatform.dagster_home python -m hbu_dataplatform.neighborhoods list
    python -m hbu_dataplatform.dagster_home python -m hbu_dataplatform.neighborhoods add CIL
    python -m hbu_dataplatform.dagster_home python -m hbu_dataplatform.neighborhoods remove CIL

Run through `hbu_dataplatform.dagster_home` (or `make neighborhood-add`) so the
instance it edits is the one the UI and the schedules read - the Postgres one
when `URBAN_RAG_PG_HOST` is set, the local home otherwise. `DagsterInstance.get()`
resolves `DAGSTER_HOME`, and refuses to run without one rather than quietly
editing an ephemeral instance that nothing else will ever see.

`add` refuses a key `partitions.known_neighborhoods` cannot resolve. `remove`
takes the key off the axis and leaves every partition it produced on disk and
in Postgres; registering it again makes them visible again.
"""

from __future__ import annotations

import argparse
import sys

from hbu_dataplatform.partitions import (
    DEFAULT_NEIGHBORHOODS,
    NEIGHBORHOOD_PARTITIONS_NAME,
    city_of,
    enabled_neighborhoods,
    known_neighborhoods,
    register_neighborhoods,
    unregister_neighborhoods,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hbu_dataplatform.neighborhoods",
        description=(
            "Edit the neighborhood partition axis held in the Dagster instance."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="registered keys, and every key that could be")
    add = commands.add_parser("add", help="register keys (seeds the defaults first)")
    add.add_argument("keys", nargs="+", metavar="KEY")
    remove = commands.add_parser("remove", help="take keys off the axis")
    remove.add_argument("keys", nargs="+", metavar="KEY")
    args = parser.parse_args(argv)

    from dagster import DagsterInstance

    instance = DagsterInstance.get()
    if args.command == "list":
        registered = enabled_neighborhoods(instance)
        print(f"registered under {NEIGHBORHOOD_PARTITIONS_NAME!r}:")
        for key in registered:
            print(f"  {key}  ({city_of(key)})")
        others = [key for key in known_neighborhoods() if key not in registered]
        if others:
            print("known but not registered:")
            for key in others:
                print(f"  {key}  ({city_of(key)})")
        return 0

    if args.command == "add":
        # Seed before adding, so a fresh instance ends up with the defaults
        # plus the request rather than the request alone.
        enabled_neighborhoods(instance)
        try:
            added = register_neighborhoods(instance, args.keys)
        except KeyError as exc:
            print(str(exc.args[0]), file=sys.stderr)
            return 2
        for key in args.keys:
            state = "added" if key in added else "already registered"
            print(f"{key}: {state} ({city_of(key)})")
        return 0

    removed = unregister_neighborhoods(instance, args.keys)
    for key in args.keys:
        print(f"{key}: {'removed' if key in removed else 'was not registered'}")
    if not enabled_neighborhoods(instance):  # pragma: no cover - reseeds
        print(f"axis is empty; defaults {DEFAULT_NEIGHBORHOODS} will be reseeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
