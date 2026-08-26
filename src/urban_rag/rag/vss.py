"""Opening a DuckDB connection with the `vss` extension available.

`INSTALL vss` normally just works. Behind the TLS-inspecting proxy this project
already accommodates elsewhere, it does not, and not the way this module's
first version assumed: `extensions.duckdb.org` answers `200` and then truncates
the body at a few hundred KB of ~11 MB - for DuckDB's own downloader, `curl`
and `requests` alike - while answering `403` to the Range request that would
let a client resume. There is no client-side route around that.

So there are three routes, tried in this order:

1. **The wheel.** `duckdb-extension-vss` ships the extension binary that
   DuckDB publishes, and PyPI is not inspected the way the extension
   repository is, so this is the one route that works from behind that proxy.
   It is in the `dev` extra (see `pyproject.toml`) because it is the test
   suite that needs vss present without a network.
2. **`INSTALL vss`**, DuckDB's own downloader, for a machine with clean egress
   and no dev extra.
3. **A download through `requests`**, which honours the corporate CA bundle
   (see `spectrum.default_ca_bundle`), cached on disk and handed to DuckDB as a
   local file. This is what covers a proxy that inspects TLS but does *not*
   truncate.

`URBAN_RAG_VSS_EXTENSION` short-circuits the third for a machine with no egress
at all.
"""

from __future__ import annotations

import gzip
import os
from pathlib import Path

import duckdb
import requests

from urban_rag.spectrum import default_ca_bundle

#: DuckDB will not persist an HNSW index to disk unless this is set: a crash
#: partway through a write can leave the index inconsistent with the table it
#: indexes. Every connection that *writes* needs it, not only the one that
#: builds the index, so it is set on open rather than at index-creation time.
_PERSISTENCE_PRAGMA = "SET hnsw_enable_experimental_persistence = true"

#: Point this at an already-downloaded `vss.duckdb_extension` to skip the
#: download entirely - useful on a machine with no egress at all.
EXTENSION_PATH_ENV = "URBAN_RAG_VSS_EXTENSION"

_REPOSITORY = "http://extensions.duckdb.org"


class VSSUnavailable(RuntimeError):
    """The vss extension could not be installed by any route."""


class StoreLocked(RuntimeError):
    """Another process holds the database file."""


def connect(database: str | Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open `database` with vss loaded and HNSW persistence enabled."""
    try:
        connection = duckdb.connect(str(database), read_only=read_only)
    except duckdb.IOException as exc:
        if "another process" not in str(exc) and "already open" not in str(exc):
            raise
        # DuckDB takes an exclusive lock for writes, and the editor's DuckDB
        # extension holds one open for the database registered in
        # `.vscode/settings.json` - which is this one.
        raise StoreLocked(
            f"{database} is open in another process.\n"
            f"{str(exc).split('File is already open in')[-1].strip()}\n"
            "Close it there (in VS Code: the DuckDB panel's disconnect button, "
            "or reload the window) and try again."
        ) from exc
    load_vss(connection)
    if not read_only:
        connection.execute(_PERSISTENCE_PRAGMA)
    return connection


def load_vss(connection: duckdb.DuckDBPyConnection) -> None:
    """Make `vss` available on `connection`, by whichever of the three routes works."""
    try:
        connection.execute("LOAD vss")
        return
    except duckdb.Error:
        pass  # not installed yet

    # The wheel first: it is already on disk, so it costs no network at all.
    try:
        _install_packaged(connection)
        connection.execute("LOAD vss")
        return
    except Exception as exc:
        packaged_failure = exc

    try:
        connection.execute("INSTALL vss")
        connection.execute("LOAD vss")
        return
    except duckdb.Error as exc:
        install_failure = exc

    try:
        path = _local_extension(connection)
        connection.execute(f"INSTALL '{path.as_posix()}'")
        connection.execute("LOAD vss")
    except Exception as exc:
        raise VSSUnavailable(
            "Could not install DuckDB's vss extension.\n"
            f"  packaged wheel -> {_first_line(packaged_failure)}\n"
            f"  INSTALL vss    -> {_first_line(install_failure)}\n"
            f"  local install  -> {_first_line(exc)}\n"
            "`uv sync --extra dev` installs the wheel, which is the route that "
            "works from behind a proxy that truncates the extension "
            "repository.\n"
            "On a machine with no egress at all, download "
            f"{_extension_url(_platform(connection))} elsewhere, gunzip it, and "
            f"set {EXTENSION_PATH_ENV} to the resulting .duckdb_extension file."
        ) from exc


def _install_packaged(connection: duckdb.DuckDBPyConnection) -> None:
    """Install the binary shipped by `duckdb-extension-vss`, if it is installed.

    `import_extension` picks the build matching this connection's DuckDB
    version and runs `INSTALL '<path>'` against the connection, so a plain
    `LOAD vss` works afterwards - and keeps working for every later connection,
    since installing writes into DuckDB's own extension directory.

    Imported here rather than at module scope: both packages live in the `dev`
    extra, and a base install has to fall through to the two download routes
    rather than fail to import.
    """
    from duckdb_extensions import import_extension

    import_extension("vss", con=connection)


def _local_extension(connection: duckdb.DuckDBPyConnection) -> Path:
    """Path to a `vss.duckdb_extension` on disk, downloading it if absent."""
    override = os.environ.get(EXTENSION_PATH_ENV)
    if override:
        path = Path(override)
        if not path.exists():
            raise FileNotFoundError(f"{EXTENSION_PATH_ENV}={override!r} does not exist")
        return path

    platform = _platform(connection)
    cache = Path.home() / ".cache" / "urban_rag" / "duckdb" / duckdb.__version__ / platform
    path = cache / "vss.duckdb_extension"
    if path.exists():
        return path

    cache.mkdir(parents=True, exist_ok=True)
    response = requests.get(
        _extension_url(platform), timeout=120, verify=default_ca_bundle() or True
    )
    response.raise_for_status()
    # Written via a temporary name so an interrupted download is not mistaken
    # for a usable extension on the next run.
    scratch = path.with_suffix(".partial")
    scratch.write_bytes(gzip.decompress(response.content))
    scratch.replace(path)
    return path


def _extension_url(platform: str) -> str:
    return f"{_REPOSITORY}/v{duckdb.__version__}/{platform}/vss.duckdb_extension.gz"


def _platform(connection: duckdb.DuckDBPyConnection) -> str:
    """DuckDB's own name for this build's platform, e.g. `windows_amd64`."""
    return connection.execute("PRAGMA platform").fetchone()[0]


def _first_line(exc: BaseException) -> str:
    return str(exc).splitlines()[0] if str(exc) else type(exc).__name__
