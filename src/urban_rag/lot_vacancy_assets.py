"""Cadastral lots enriched with CMHC vacancy rates before spatial joins.

`vacancy_rates` is one row per dwelling type x bedroom class, while
`neighborhood_lots` is one row per cadastral lot. This asset joins them by the
partition's borough/neighborhood key, not by geometry: CMHC publishes rates for
named survey neighborhoods and no boundaries to intersect. The rate grid is
pivoted into columns first so every lot stays one row, which keeps the later
building x lot spatial join working against true lot geometries rather than a
lot duplicated once per CMHC cell.
"""

import geopandas as gpd
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.cmhc_assets import VACANCY_FILE, vacancy_rates
from urban_rag.frames import count_invalid_geometries, write_frame
from urban_rag.infolot_assets import LOTS_FILE, neighborhood_lots
from urban_rag.partitions import scrape_partitions
from urban_rag.resources import ParquetStore
from urban_rag.storage import clear_parquet, join, storage_options

GROUP = "lot_vacancy"

#: The one enriched lot file written under
#: `lots_with_vacancy_rates/<YYYY-MM-DD>/<neighborhood>/`.
LOTS_WITH_VACANCY_FILE = "lots_with_vacancy_rates.parquet"

_REQUIRED_RATE_COLUMNS = {
    "neighborhood",
    "dwelling_type",
    "bedroom_type",
    "vacancy_rate_pct",
    "min_vacancy_rate_pct",
    "max_vacancy_rate_pct",
    "num_quartiers",
    "num_quartiers_mapped",
    "averaged_quartiers",
    "survey_year",
    "survey_period",
    "scrape_date",
}

_PIVOTED_RATE_COLUMNS = (
    "vacancy_rate_pct",
    "min_vacancy_rate_pct",
    "max_vacancy_rate_pct",
    "num_quartiers",
    "averaged_quartiers",
)


@asset(
    partitions_def=scrape_partitions,
    deps=[neighborhood_lots, vacancy_rates],
    group_name=GROUP,
    description=(
        "Every cadastral lot intersecting one borough, with that borough's "
        "CMHC Rental Market Survey vacancy-rate grid joined by neighborhood "
        "name, as lots_with_vacancy_rates/<YYYY-MM-DD>/<neighborhood>/"
        f"{LOTS_WITH_VACANCY_FILE}. This is a name/partition join, not a "
        "spatial join, and it runs before building_lot_intersections so the "
        "PostGIS lot attributes already carry the CMHC context."
    ),
)
def lots_with_vacancy_rates(
    context: AssetExecutionContext,
    store: ParquetStore,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    lots_path = join(
        store.partition_dir(neighborhood_lots.key.path[-1], scrape_date, neighborhood),
        LOTS_FILE,
    )
    rates_path = join(
        store.partition_dir(vacancy_rates.key.path[-1], scrape_date, neighborhood),
        VACANCY_FILE,
    )

    lots = _read_lots(lots_path, neighborhood=neighborhood, scrape_date=scrape_date)
    rates = _read_rates(rates_path, neighborhood=neighborhood, scrape_date=scrape_date)

    if lots.empty:
        raise Failure(f"{lots_path} holds no lot to enrich.")
    if rates.empty:
        raise Failure(f"{rates_path} holds no CMHC rate cells to join.")
    _require_lot_neighborhood(lots, lots_path)
    _require_lot_partition(lots, neighborhood=neighborhood, scrape_date=scrape_date)
    _require_rate_columns(rates, rates_path)
    _require_partition(rates, neighborhood=neighborhood, scrape_date=scrape_date)

    wide = _wide_rates(rates, neighborhood=neighborhood)
    enriched = lots.merge(wide, on="neighborhood", how="left", validate="many_to_one")

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    path = write_frame(enriched, join(output_dir, LOTS_WITH_VACANCY_FILE))

    invalid = count_invalid_geometries(enriched)
    if invalid:
        context.log.warning("%s: %d invalid geometr(ies)", path, invalid)

    published = rates["vacancy_rate_pct"].notna()
    overall = _overall_rate(rates)
    context.log.info(
        "%s %s: %d lot(s) enriched with %d CMHC rate cell(s) -> %s",
        neighborhood,
        scrape_date,
        len(enriched),
        len(rates),
        path,
    )

    metadata = {
        "dagster/row_count": len(enriched),
        "num_lots": len(enriched),
        "num_cmhc_rate_cells": len(rates),
        "num_cmhc_published_cells": int(published.sum()),
        "num_cmhc_columns": len(wide.columns) - 1,
        "num_invalid_geometries": invalid,
        "output_path": MetadataValue.path(str(path)),
    }
    if overall is None:
        metadata["overall_vacancy_rate_pct"] = MetadataValue.text("suppressed")
    else:
        metadata["overall_vacancy_rate_pct"] = MetadataValue.float(float(overall))
    return MaterializeResult(metadata=metadata)


def _read_lots(
    path: str, *, neighborhood: str, scrape_date: str
) -> gpd.GeoDataFrame:
    try:
        return gpd.read_parquet(path, storage_options=storage_options(path))
    except FileNotFoundError as exc:
        raise Failure(
            f"{path} does not exist - materialize neighborhood_lots for "
            f"{neighborhood} {scrape_date} first."
        ) from exc


def _read_rates(path: str, *, neighborhood: str, scrape_date: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, storage_options=storage_options(path))
    except FileNotFoundError as exc:
        raise Failure(
            f"{path} does not exist - materialize vacancy_rates for "
            f"{neighborhood} {scrape_date} first."
        ) from exc


def _require_lot_neighborhood(lots: gpd.GeoDataFrame, path: str) -> None:
    if "neighborhood" not in lots.columns:
        raise Failure(
            f"{path} has no neighborhood column - it was not written by "
            "neighborhood_lots."
        )


def _require_lot_partition(
    lots: gpd.GeoDataFrame, *, neighborhood: str, scrape_date: str
) -> None:
    neighborhoods = set(lots["neighborhood"].dropna().astype(str))
    if neighborhoods != {neighborhood}:
        raise Failure(
            f"Lots for {neighborhood} {scrape_date} carry neighborhood values "
            f"{sorted(neighborhoods)!r}."
        )
    if "scrape_date" not in lots.columns:
        return
    dates = set(lots["scrape_date"].dropna().astype(str))
    if dates and dates != {scrape_date}:
        raise Failure(
            f"Lots for {neighborhood} {scrape_date} carry scrape_date values "
            f"{sorted(dates)!r}."
        )


def _require_rate_columns(rates: pd.DataFrame, path: str) -> None:
    missing = sorted(_REQUIRED_RATE_COLUMNS - set(rates.columns))
    if missing:
        raise Failure(f"{path} has no {', '.join(missing)} column(s).")


def _require_partition(
    rates: pd.DataFrame, *, neighborhood: str, scrape_date: str
) -> None:
    neighborhoods = set(rates["neighborhood"].dropna().astype(str))
    if neighborhoods != {neighborhood}:
        raise Failure(
            f"CMHC rates for {neighborhood} {scrape_date} carry neighborhood "
            f"values {sorted(neighborhoods)!r}."
        )
    dates = set(rates["scrape_date"].dropna().astype(str))
    if dates and dates != {scrape_date}:
        raise Failure(
            f"CMHC rates for {neighborhood} {scrape_date} carry scrape_date "
            f"values {sorted(dates)!r}."
        )


def _wide_rates(rates: pd.DataFrame, *, neighborhood: str) -> pd.DataFrame:
    duplicates = rates.duplicated(["dwelling_type", "bedroom_type"], keep=False)
    if duplicates.any():
        repeated = rates.loc[duplicates, ["dwelling_type", "bedroom_type"]]
        pairs = sorted(
            {tuple(row) for row in repeated.itertuples(index=False, name=None)}
        )
        raise Failure(f"CMHC rates contain duplicate cells: {pairs!r}")

    row: dict[str, object] = {
        "neighborhood": neighborhood,
        "cmhc_survey_year": _only_value(rates, "survey_year"),
        "cmhc_survey_period": _only_value(rates, "survey_period"),
        "cmhc_num_quartiers_mapped": _only_value(rates, "num_quartiers_mapped"),
    }
    for rate in rates.itertuples(index=False):
        stem = f"cmhc_{_slug(rate.dwelling_type)}_{_slug(rate.bedroom_type)}"
        for column in _PIVOTED_RATE_COLUMNS:
            row[f"{stem}_{column}"] = _scalar(getattr(rate, column))
    return pd.DataFrame([row])


def _overall_rate(rates: pd.DataFrame) -> float | None:
    overall = rates[
        (rates["dwelling_type"] == "all") & (rates["bedroom_type"] == "all")
    ]["vacancy_rate_pct"]
    if overall.empty or pd.isna(overall.iloc[0]):
        return None
    return float(overall.iloc[0])


def _only_value(frame: pd.DataFrame, column: str) -> object:
    values = frame[column].dropna().unique()
    if len(values) == 0:
        return None
    if len(values) > 1:
        raise Failure(f"CMHC rates carry multiple {column} values: {values!r}")
    return _scalar(values[0])


def _scalar(value) -> object:
    return None if pd.isna(value) else value


def _slug(value: object) -> str:
    return str(value).strip().lower().replace("-", "_")
