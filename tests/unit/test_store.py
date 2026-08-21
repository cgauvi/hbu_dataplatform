"""Round-trip tests for the DuckDB/VSS store.

These build a real store from real parquet on a temp path - the vss extension
and the fixed-width cast are as much under test as the SQL is - but with
hand-written unit vectors, so nothing here loads an embedding model.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from urban_rag.frames import write_vectors
from urban_rag.rag.store import IndexMismatch, VectorStore


def unit(*components: float) -> list[float]:
    norm = math.sqrt(sum(c * c for c in components))
    return [c / norm for c in components]


def write_embeddings(
    root,
    rows,
    *,
    neighborhood="VSMPE",
    scrape_date="2026-08-18",
    model="BAAI/bge-m3",
):
    """Write one `embeddings.parquet` under the hive layout the loader expects."""
    directory = root / f"neighborhood={neighborhood}" / f"scrape_date={scrape_date}"
    directory.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        [
            {
                "chunk_id": chunk_id,
                "doc_id": chunk_id.split(":")[0],
                "chunk_index": int(chunk_id.split(":")[1]),
                "num_tokens": tokens,
                "text": text,
                "source_table": "Reglement_urbanisme__VSP_REG_PPCMOI",
                "url": f"http://x/{chunk_id.split(':')[0]}.pdf",
                "title": "Resolution",
                "feature_ids": "[1]",
                "model": model,
            }
            for chunk_id, text, tokens, _ in rows
        ]
    )
    vectors = np.asarray([vector for *_, vector in rows], dtype=np.float32)
    write_vectors(frame, vectors, directory / "embeddings.parquet")
    return str(root / "**" / "embeddings.parquet")


@pytest.fixture
def store(tmp_path):
    return VectorStore(tmp_path / "vect_db.duckdb")


@pytest.fixture
def source(tmp_path):
    return write_embeddings(
        tmp_path / "rag",
        [
            ("doc0:0000", "premier extrait", 100, unit(1, 0, 0)),
            ("doc1:0000", "deuxieme extrait", 101, unit(0, 1, 0)),
            ("doc2:0000", "troisieme extrait", 102, unit(0.95, 0.05, 0)),
        ],
    )


def test_build_reports_what_it_loaded(store, source):
    result = store.build(source)

    assert result["chunks"] == 3
    assert result["documents"] == 3
    assert result["dimension"] == 3
    assert result["embedding_model"] == "BAAI/bge-m3"


def test_search_returns_the_nearest_chunk_first(store, source):
    store.build(source)

    hits = store.search(unit(1, 0, 0), k=3)

    assert [hit.doc_id for hit in hits] == ["doc0", "doc2", "doc1"]
    assert hits[0].similarity == pytest.approx(1.0, abs=1e-6)
    assert hits[0].num_tokens == 100


def test_search_carries_provenance_into_langchain_metadata(store, source):
    store.build(source)

    metadata = store.search(unit(1, 0, 0), k=1)[0].metadata

    assert metadata["doc_id"] == "doc0"
    assert metadata["url"] == "http://x/doc0.pdf"
    assert metadata["neighborhood"] == "VSMPE"
    assert metadata["scrape_date"] == "2026-08-18"
    assert metadata["model"] == "BAAI/bge-m3"
    assert metadata["chunk_index"] == 0


def test_hive_keys_become_columns(store, source):
    store.build(source)

    stats = store.stats()

    assert stats["partitions"] == [("VSMPE", "2026-08-18", 3, 3)]


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"neighborhood": "VSMPE"}, 3),
        ({"neighborhood": "PMR"}, 0),
        ({"scrape_date": "2026-08-18"}, 3),
        ({"doc_id": "doc0"}, 1),
        ({"source_table": "nope"}, 0),
    ],
)
def test_search_filters_narrow_the_candidates(store, source, filters, expected):
    store.build(source)

    assert len(store.search(unit(1, 0, 0), k=10, **filters)) == expected


def test_build_can_load_a_single_partition(store, tmp_path):
    root = tmp_path / "rag"
    write_embeddings(root, [("doc0:0000", "a", 10, unit(1, 0, 0))])
    pattern = write_embeddings(
        root, [("doc1:0000", "b", 10, unit(0, 1, 0))], neighborhood="PMR"
    )

    assert store.build(pattern, neighborhood="PMR")["chunks"] == 1
    assert store.stats()["partitions"] == [("PMR", "2026-08-18", 1, 1)]


def test_the_newest_scrape_date_wins_for_a_repeated_chunk(store, tmp_path):
    root = tmp_path / "rag"
    write_embeddings(
        root, [("doc0:0000", "ancienne version", 10, unit(1, 0, 0))],
        scrape_date="2026-08-01",
    )
    pattern = write_embeddings(
        root, [("doc0:0000", "nouvelle version", 10, unit(1, 0, 0))],
        scrape_date="2026-08-18",
    )

    store.build(pattern)
    hits = store.search(unit(1, 0, 0), k=5)

    assert len(hits) == 1
    assert hits[0].text == "nouvelle version"
    assert hits[0].scrape_date == "2026-08-18"


def test_rebuilding_replaces_rather_than_appends(store, source, tmp_path):
    store.build(source)
    replacement = write_embeddings(
        tmp_path / "other", [("doc9:0000", "seul", 10, unit(1, 0, 0))]
    )

    store.build(replacement)

    assert store.stats()["chunks"] == 1


def test_mixing_two_embedding_models_is_refused(store, tmp_path):
    root = tmp_path / "rag"
    write_embeddings(root, [("doc0:0000", "a", 10, unit(1, 0, 0))], model="bge-m3")
    pattern = write_embeddings(
        root, [("doc1:0000", "b", 10, unit(0, 1, 0))],
        scrape_date="2026-08-19", model="e5-small",
    )

    with pytest.raises(IndexMismatch, match="Mixed embeddings"):
        store.build(pattern)


def test_a_query_of_the_wrong_width_is_a_mismatch_not_a_sql_error(store, source):
    store.build(source)

    with pytest.raises(IndexMismatch, match="bge-m3"):
        store.search(unit(1, 0), k=1)


def test_building_from_a_pattern_that_matches_nothing_says_so(store, tmp_path):
    with pytest.raises(IndexMismatch, match="No readable embeddings"):
        store.build(str(tmp_path / "absent" / "**" / "embeddings.parquet"))


def test_filters_that_match_nothing_say_so(store, source):
    with pytest.raises(IndexMismatch, match="No embedded chunks matched"):
        store.build(source, neighborhood="Outremont")


def test_stats_report_the_model_the_store_was_built_with(store, source):
    store.build(source)

    stats = store.stats()

    assert stats["embedding_model"] == "BAAI/bge-m3"
    assert stats["dimension"] == "3"
    assert stats["documents"] == 3
    assert stats["tokens_min"] == 100
    assert stats["tokens_max"] == 102
    assert stats["per_table"] == [("Reglement_urbanisme__VSP_REG_PPCMOI", 3, 3)]


def test_the_index_survives_reopening_the_file(store, source):
    store.build(source)

    # A second VectorStore over the same path opens a fresh connection, which is
    # where a non-persisted HNSW index would be missing.
    reopened = VectorStore(store.path)

    assert [hit.doc_id for hit in reopened.search(unit(0, 1, 0), k=1)] == ["doc1"]


def test_querying_a_database_we_did_not_write_is_a_clear_error(store, source, tmp_path):
    import duckdb

    foreign = tmp_path / "foreign.duckdb"
    duckdb.connect(str(foreign)).close()

    with pytest.raises(IndexMismatch, match="not written by"):
        VectorStore(foreign).stats()
