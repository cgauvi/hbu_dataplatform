"""What `_copy_into` writes, without a database.

Binary COPY takes the column list, the type list and the row tuple from three
different places in `pgvector.py`, and only two of them are tied together by an
assert. When `page_from`/`page_to` were added, `_COPY_COLUMNS` and
`_COPY_TYPES` both grew, that assert stayed true, and the writer went on
sending 13 values into 15 columns - which nothing noticed until a real COPY
raised `expected 15 values in row, got 13` from inside psycopg's Cython
formatter, naming neither the column nor this file.

So the writer is exercised here against a cursor that only records, which is
enough to pin the arity, the order and the NULL handling that a database would
otherwise be needed for.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date

import numpy as np
import pandas as pd

from hbu_dataplatform.rag.pgvector import _COPY_COLUMNS, _COPY_TYPES, _copy_into


class _RecordingCopy:
    def __init__(self) -> None:
        self.rows: list[tuple] = []
        self.types: list[str] | None = None

    def set_types(self, types: list[str]) -> None:
        self.types = types

    def write_row(self, row: tuple) -> None:
        self.rows.append(row)


class _RecordingCursor:
    def __init__(self) -> None:
        self.copy_object = _RecordingCopy()
        self.statement: str | None = None

    @contextmanager
    def copy(self, statement: str):
        self.statement = statement
        yield self.copy_object


def _frame(**overrides) -> pd.DataFrame:
    row = {
        "chunk_id": "c1",
        "doc_id": "d1",
        "url": "https://example.test/grid.pdf",
        "title": "Grille H04-072",
        "source_table": "Reglement_urbanisme__VSP_REG_ZONE",
        "neighborhood": "VSMPE",
        "scrape_date": date(2026, 9, 1),
        "chunk_index": 0,
        "num_tokens": 120,
        "feature_ids": '["H04-072"]',
        "model": "BAAI/bge-m3",
        "text": "Taux d'implantation maximal",
        "page_from": 1.0,
        "page_to": 2.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _write(frame: pd.DataFrame) -> tuple:
    cursor = _RecordingCursor()
    vectors = np.zeros((len(frame), 4), dtype=np.float32)
    _copy_into(cursor, "rag.chunks", frame, vectors)
    return cursor.copy_object.rows[0]


def test_a_row_carries_one_value_per_copied_column():
    """The invariant the three lists have to agree on."""
    assert len(_write(_frame())) == len(_COPY_COLUMNS) == len(_COPY_TYPES)


def test_the_page_columns_are_written_in_their_declared_position():
    """Binary COPY carries no names, so position is the whole contract."""
    row = _write(_frame())
    assert row[_COPY_COLUMNS.index("page_from")] == 1
    assert row[_COPY_COLUMNS.index("page_to")] == 2


def test_a_chunk_with_no_page_is_written_as_null_not_zero():
    """88 of VSMPE's 1,615 chunks could not be placed in their page map.

    Page 0 does not exist, and a citation to it would be a confident lie - so
    the column has to arrive NULL and read as "page unknown".
    """
    row = _write(_frame(page_from=float("nan"), page_to=None))
    assert row[_COPY_COLUMNS.index("page_from")] is None
    assert row[_COPY_COLUMNS.index("page_to")] is None


def test_a_frame_written_before_page_columns_existed_still_publishes():
    """Re-publishing an older partition leaves the page unknown, not failing."""
    frame = _frame().drop(columns=["page_from", "page_to"])

    row = _write(frame)

    assert len(row) == len(_COPY_COLUMNS)
    assert row[_COPY_COLUMNS.index("page_from")] is None
    assert row[_COPY_COLUMNS.index("page_to")] is None
