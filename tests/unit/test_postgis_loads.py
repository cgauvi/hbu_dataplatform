"""The three `rag` working-set loaders, and the statistics they leave behind.

`rag.lots`, `rag.buildings` and `rag.features` are loaded by the raw
DELETE/COPY/INSERT in `urban_rag.postgis` rather than through
`urban_rag.warehouse`, so the ANALYZE that closes a warehouse write had to be
added to them separately - and separately is exactly how it would go missing
again. These tests hold it.

Why it matters is a reader's problem rather than a writer's. hbu_rag_map draws
the cadastre, the footprints and the zoning layer as vector tiles: one query
per tile, several dozen per pan, each a GiST lookup narrowed by
`(neighborhood, scrape_date)`. A load rewrites most of a partition and leaves
the planner's statistics describing what was there before it, and on stale
counts the planner mis-estimates that filter and drops the index scan. The map
does not fail - it stops answering, on the partition that was just loaded,
which is the one somebody is about to look at.

Nothing here touches a database. The cursor is a stub that records statements,
the same shape `test_warehouse` uses.
"""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Point, box

from urban_rag import postgis

NEIGHBORHOOD = "VSMPE"
DATE = "2026-08-26"


class FakeCopy:
    def __init__(self, rows: list[list[object]]):
        self.rows = rows
        self.types: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def set_types(self, types):
        self.types = list(types)

    def write_row(self, values):
        self.rows.append(list(values))


class FakeCursor:
    def __init__(self):
        self.statements: list[tuple[str, object]] = []
        self.copied: list[list[object]] = []
        self.rowcount = 0

    def execute(self, statement: str, params=None):
        self.statements.append((statement, params))
        return self

    def copy(self, statement: str):
        self.statements.append((statement, None))
        return FakeCopy(self.copied)

    def issued(self) -> list[str]:
        return [" ".join(statement.split()) for statement, _ in self.statements]

    def analyzed(self) -> list[str]:
        return [t for t in self.issued() if t.startswith("ANALYZE")]


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def lots() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"NO_LOT": ["2 170 935", "2 170 936"], "CO_STATT_LOT": ["AC", "AC"]},
        geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1)],
        crs="EPSG:4326",
    )


def zones() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"NUMERO_COMPLET": ["C01-001"], "LIEN_GRILLE": ["http://example/1.pdf"]},
        geometry=[Point(0.5, 0.5).buffer(0.1)],
        crs="EPSG:4326",
    )


@pytest.fixture
def cursor():
    return FakeCursor()


# ---------------------------------------------------------------------------


def test_loading_lots_refreshes_their_statistics(cursor):
    postgis.load_lots(
        FakeConnection(cursor), lots(),
        neighborhood=NEIGHBORHOOD, scrape_date=DATE,
    )
    assert cursor.analyzed() == ["ANALYZE rag.lots"]


def test_loading_buildings_refreshes_their_statistics(cursor):
    postgis.load_buildings(
        FakeConnection(cursor), lots().drop(columns=["NO_LOT"]),
        neighborhood=NEIGHBORHOOD, scrape_date=DATE,
    )
    assert cursor.analyzed() == ["ANALYZE rag.buildings"]


def test_loading_features_refreshes_their_statistics(cursor):
    postgis.load_features(
        FakeConnection(cursor), zones(),
        neighborhood=NEIGHBORHOOD, scrape_date=DATE,
        source_table="Reglement_urbanisme__VSP_REG_ZONE",
        feature_id_column="NUMERO_COMPLET",
    )
    assert cursor.analyzed() == ["ANALYZE rag.features"]


def test_the_analyze_comes_after_the_rows_are_in(cursor):
    """Statistics taken between the DELETE and the INSERT describe an empty
    partition, which is the worst estimate of the three available."""
    postgis.load_lots(
        FakeConnection(cursor), lots(),
        neighborhood=NEIGHBORHOOD, scrape_date=DATE,
    )
    issued = cursor.issued()
    inserted = max(
        i for i, t in enumerate(issued) if t.startswith("INSERT INTO rag.lots")
    )
    analyzed = next(i for i, t in enumerate(issued) if t.startswith("ANALYZE"))
    assert inserted < analyzed


def test_an_emptied_partition_is_analyzed_too(cursor):
    """A borough that loaded nothing this time is as much a change of shape as
    one that loaded everything, and the planner is as wrong about it."""
    empty = gpd.GeoDataFrame(
        {"NO_LOT": []}, geometry=[], crs="EPSG:4326"
    )
    written = postgis.load_lots(
        FakeConnection(cursor), empty,
        neighborhood=NEIGHBORHOOD, scrape_date=DATE,
    )
    assert written == 0
    assert cursor.analyzed() == ["ANALYZE rag.lots"]


def test_an_empty_feature_layer_is_analyzed_too(cursor):
    empty = gpd.GeoDataFrame(
        {"NUMERO_COMPLET": []}, geometry=[], crs="EPSG:4326"
    )
    written = postgis.load_features(
        FakeConnection(cursor), empty,
        neighborhood=NEIGHBORHOOD, scrape_date=DATE,
        source_table="Reglement_urbanisme__VSP_REG_ZONE",
        feature_id_column="NUMERO_COMPLET",
    )
    assert written == 0
    assert cursor.analyzed() == ["ANALYZE rag.features"]
