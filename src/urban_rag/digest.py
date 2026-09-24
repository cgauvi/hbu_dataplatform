"""What a dataset's content hashes to, and why every rule here is load-bearing.

Free of Dagster and of psycopg, like `tile_grid` and `program`: a digest is a
pure function of a frame.

---------------------------------------------------------------------------
What this is for
---------------------------------------------------------------------------

Almost nothing this pipeline reads changes monthly. The assessment roll is
published once a year, the CUBF codebook is effectively static, CMHC is annual,
Marketbeat quarterly, StatCan's footprints annual, and a zoning by-law is
amended a handful of times a year. The cron fires every month regardless, and
every month the whole chain is recomputed and rewritten - 26 tables, twelve
times a year, for data that changed two to four times.

A digest is how a tick decides it has nothing to do. Same digest, no write, no
downstream run: the month costs a hash instead of 2h23m.

---------------------------------------------------------------------------
The failure mode, which is silent
---------------------------------------------------------------------------

Get the canonicalisation wrong in one direction and the digest changes every
time, the feature does nothing, and somebody eventually deletes it. Get it
wrong in the other and the pipeline **serves stale data and never says so** -
no error, no metric, nothing to notice. That asymmetry is why the rules below
are rules rather than preferences, and why `tests/unit/test_digest.py` spends
most of its length on the second direction.

Two habits keep it honest, and neither is optional:

* **hash a superset of what you write** - a column left out because nothing
  reads it today is a column whose change is invisible tomorrow;
* **force a rematerialisation periodically** and assert the digest predicted
  it. A canonicalisation bug has no symptom; this is the only thing that finds
  one.

---------------------------------------------------------------------------
The rules
---------------------------------------------------------------------------

**Order-independent, by sorting the row digests.** A publisher is free to
return the same rows in a different order - ArcGIS does it routinely - and a
digest that changed when it did would be a digest of the ordering. Sorting the
per-row digests costs an O(n log n) over 32-byte strings and is exact.

*Not* XOR, which is the tempting incremental-friendly alternative: `A XOR A` is
zero, so a pair of identical rows vanishes. That is not hypothetical here -
`warehouse._merge` carries a `DISTINCT ON` precisely because Infolot answers a
boundary query with the same lot twice when a borough outline is a multipolygon.
Under XOR, a duplicated lot and no lot at all would hash the same.

**Provenance is excluded.** `scraped_at` is the wall clock at fetch and
`loaded_at` defaults to `now()`; both change on every run by construction, so
including them makes the digest a very expensive timestamp.

**Geometry is normalised before it is hashed.** The same shape can serialise
two ways - a different start vertex, a ring wound the other way - and a
publisher that re-exports its layer will produce both. `shapely.normalize`
fixes the ordering and `set_precision` snaps coordinates onto a grid, so a
re-export that moves a vertex by a float's last bit is not a change. The grid
is `GEOMETRY_PRECISION` below, about a centimetre, which is finer than any
cadastral survey this reads and coarser than any float noise.

**Floats are formatted, not repr'd.** `repr(0.1 + 0.2)` is a platform's
business; `f"{x:.12g}"` is not.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Sequence
from typing import Any

import pandas as pd

#: Columns the pipeline stamps rather than the publisher supplying. Excluded
#: everywhere, because each one changes on every run by construction.
PROVENANCE_COLUMNS: frozenset[str] = frozenset(
    {"scraped_at", "loaded_at", "content_digest", "first_observed", "last_observed"}
)

#: Degrees a coordinate is snapped to before hashing. 1e-7 is about 1.1 cm at
#: this latitude - finer than the surveys behind any layer here, and coarse
#: enough that a re-export which perturbs the last bit of a double is not a
#: change. Lowering it makes the digest jumpy; raising it makes it blind.
GEOMETRY_PRECISION = 1e-7

#: Significant digits kept when a float is turned into bytes. Twelve is inside
#: a double's ~15-17 and outside anything a source actually publishes.
FLOAT_DIGITS = 12

#: What an absent value hashes as. A byte no formatted value can begin with, so
#: NULL cannot collide with the string "None" that a source might legitimately
#: carry in a text column.
_NULL = b"\x00"
_SEPARATOR = b"\x1f"  # ASCII unit separator: not in any value here.


def _geometry_bytes(value: Any) -> bytes:
    """Canonical WKB for a shapely geometry, or the NULL sentinel."""
    if value is None:
        return _NULL
    import shapely

    try:
        if shapely.is_empty(value):
            return _NULL
    except TypeError:  # not a geometry at all
        return _scalar_bytes(value)

    snapped = shapely.set_precision(value, GEOMETRY_PRECISION)
    # `normalize` puts rings and their vertices in a canonical order, which is
    # what makes two serialisations of one shape hash alike.
    return shapely.to_wkb(shapely.normalize(snapped), include_srid=False)


def _scalar_bytes(value: Any) -> bytes:
    """Canonical bytes for one non-geometry cell."""
    if value is None or value is pd.NaT:
        return _NULL
    if isinstance(value, float):
        if math.isnan(value):
            return _NULL
        # -0.0 and 0.0 are the same number and must hash alike; `+ 0.0`
        # normalises the sign of zero without touching anything else.
        return f"{value + 0.0:.{FLOAT_DIGITS}g}".encode()
    if isinstance(value, bool):
        return b"true" if value else b"false"
    if isinstance(value, (int,)):
        return str(value).encode()
    if isinstance(value, bytes):
        return value
    try:
        if pd.isna(value):
            return _NULL
    except (TypeError, ValueError):
        pass
    return str(value).encode()


def row_digest(values: Iterable[bytes]) -> bytes:
    """One row's 32 bytes, from its cells already canonicalised."""
    hasher = hashlib.sha256()
    for cell in values:
        hasher.update(cell)
        hasher.update(_SEPARATOR)
    return hasher.digest()


def frame_digest(
    frame: pd.DataFrame,
    *,
    exclude: Sequence[str] = (),
    geometry_column: str | None = None,
) -> bytes:
    """The 32-byte content digest of ``frame``.

    Independent of row order and of column order. ``exclude`` is on top of
    `PROVENANCE_COLUMNS`, for the rare column a caller knows is noise -
    everything else is hashed, deliberately, so that a field nobody reads today
    is still a field whose change is visible tomorrow.

    An empty frame has a digest too, and it is not the digest of a frame with
    one empty row: a publisher that went from two rows to none has changed, and
    this has to say so.
    """
    dropped = set(exclude) | PROVENANCE_COLUMNS
    columns = sorted(c for c in frame.columns if c not in dropped)

    if geometry_column is None:
        geometry_column = getattr(getattr(frame, "geometry", None), "name", None)

    header = hashlib.sha256()
    header.update(b"urban_rag.digest.v1")
    for name in columns:
        header.update(_SEPARATOR)
        header.update(str(name).encode())

    if not columns or frame.empty:
        header.update(_SEPARATOR)
        header.update(str(len(frame)).encode())
        return header.digest()

    # Column-major, because the geometry branch is decided per column rather
    # than per cell - 19M cells is where a per-cell isinstance shows up.
    encoded: list[list[bytes]] = []
    for name in columns:
        series = frame[name]
        if name == geometry_column:
            encoded.append([_geometry_bytes(v) for v in series])
        else:
            encoded.append([_scalar_bytes(v) for v in series])

    rows = sorted(row_digest(cells) for cells in zip(*encoded))

    hasher = hashlib.sha256()
    hasher.update(header.digest())
    for digest in rows:
        hasher.update(digest)
    return hasher.digest()


def digest_hex(value: bytes) -> str:
    """The digest as the 64 characters a log line or a metadata value shows."""
    return value.hex()


def short(value: bytes, length: int = 12) -> str:
    """The first characters of the hex, for a run label or a partition name."""
    return value.hex()[:length]
