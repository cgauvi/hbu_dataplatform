"""Where pipeline output lives: local disk by default, S3 when `S3_BUCKET` is set.

Every writer/reader of (geo)parquet in the pipeline goes through this module
instead of building paths or opening files directly, so the same asset code
runs against a laptop's disk or against S3 without a branch of its own.

Loads `.env` (via `python-dotenv`) so `S3_BUCKET` / `AWS_PROFILE` can live
there instead of the shell, matching how the rest of the project is run.
"""

from __future__ import annotations

import os
import posixpath
import re
from pathlib import Path

import fsspec
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

#: Local root for state that never moves to S3: the PDF cache, the Dagster IO
#: manager's own bookkeeping, and the DuckDB vector store file (DuckDB opens
#: its database file locally; only the parquet it *reads* can live on S3).
DATA_ROOT = Path(os.environ.get("URBAN_RAG_DATA_DIR", PROJECT_ROOT / "data"))

#: When set, (geo)parquet output goes to s3://<S3_BUCKET>/... instead of disk.
S3_BUCKET = os.environ.get("S3_BUCKET")
#: boto3/fsspec/DuckDB profile used for all S3 access - see ~/.aws/credentials
#: and ~/.aws/config for the credentials and region behind it.
AWS_PROFILE = os.environ.get("AWS_PROFILE", "charles_gauvin_east_1")

_VALID_PROFILE = re.compile(r"^[A-Za-z0-9_.-]+$")
if not _VALID_PROFILE.match(AWS_PROFILE):
    raise ValueError(f"AWS_PROFILE={AWS_PROFILE!r} is not a valid AWS profile name")


def is_s3_enabled() -> bool:
    """Whether pipeline output is configured to go to S3 rather than disk."""
    return bool(S3_BUCKET)


def is_s3_uri(path: str) -> bool:
    return str(path).startswith("s3://")


def output_root() -> str:
    """Root for pipeline (geo)parquet output.

    An `s3://<S3_BUCKET>` URI when `S3_BUCKET` is set, `DATA_ROOT` otherwise.
    """
    return f"s3://{S3_BUCKET}" if S3_BUCKET else str(DATA_ROOT)


def storage_options(path: str | None = None) -> dict[str, str]:
    """kwargs pandas/pyarrow forward to fsspec for S3 access.

    Detected from ``path``'s own scheme rather than from `S3_BUCKET`, so a
    plain local path (e.g. in tests) never gets S3 credentials attached just
    because `S3_BUCKET` happens to be set elsewhere.
    """
    target = output_root() if path is None else path
    return {"profile": AWS_PROFILE} if is_s3_uri(target) else {}


def filesystem(path: str | None = None) -> fsspec.AbstractFileSystem:
    """The fsspec filesystem for ``path`` (or for `output_root()` if omitted).

    Detected from ``path``'s own scheme, not from `S3_BUCKET` - see
    `storage_options`.
    """
    target = output_root() if path is None else path
    if is_s3_uri(target):
        return fsspec.filesystem("s3", profile=AWS_PROFILE)
    return fsspec.filesystem("file")


def join(root: str, *parts: str) -> str:
    """Path join that works for both local paths and s3:// URIs."""
    result = str(root).rstrip("/")
    for part in parts:
        result = f"{result}/{str(part).strip('/')}"
    return result


def dirname(path: str) -> str:
    return posixpath.dirname(str(path))


def basename(path: str) -> str:
    return posixpath.basename(str(path))


def configure_duckdb_s3(connection) -> None:
    """Make `read_parquet('s3://...')` work on `connection`.

    Authenticates with `AWS_PROFILE` through DuckDB's own credential chain
    provider rather than passing keys, so nothing secret passes through SQL.
    """
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(
        "CREATE OR REPLACE SECRET s3_profile ("
        "TYPE s3, PROVIDER credential_chain, CHAIN 'profile', "
        f"PROFILE '{AWS_PROFILE}')"
    )


def clear_parquet(output_dir: str) -> list[str]:
    """Delete the parquet already in ``output_dir``, returning what was removed.

    Every partition written by this pipeline is a full snapshot, so a re-run
    replaces the directory's contents instead of adding to them - otherwise a
    table that disappears upstream lingers in the partition forever.
    """
    fs = filesystem(output_dir)
    if not fs.exists(output_dir):
        return []
    stale = fs.glob(join(output_dir, "*.parquet"))
    for path in stale:
        fs.rm(path)
    return stale
