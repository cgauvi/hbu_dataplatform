"""CMHC's Rental Market Survey, in two layers.

CMHC surveys the Montreal *census metropolitan area* and cuts it into its own
neighborhoods, which do not line up with the boroughs this pipeline is
partitioned on: `VSMPE` is three CMHC quartiers, `Outremont` is one, and `PR`
is the borough plus a neighbouring municipality CMHC will not split out. The
crosswalk is `urban_rag.partitions.CMHC_QUARTIERS`.

That crosswalk is exactly where the bronze/silver line falls here.

**Bronze** - `cmhc_vacancy_survey` and `cmhc_rent_survey` - snapshot the
Montreal slice as published: every quartier the survey prints for this centre,
under whatever names it prints them, subtotal rows included. Neither knows the
crosswalk exists, so neither can be broken by it. They are partitioned by
scrape date *alone*, because there is nothing borough-shaped about them: one
workbook read per day rather than the same workbook re-read and re-parsed once
per enabled borough.

**Silver** - `vacancy_rates` and `average_rents` - apply the crosswalk to that
snapshot and average each borough's quartiers into one figure. These are the
assets that are allowed to refuse: a quartier the crosswalk names but the
survey does not publish fails a *silver* partition, and the bronze snapshot it
was computed from is still on disk to diagnose it with. Before the split, a
CMHC respelling cost the day's scrape; now it costs a re-run of one silver
asset against parquet already landed.

Unlike the lots and the buildings, the borough cut is a name lookup rather than
a spatial join: the survey publishes rates, not geometry, so there is nothing
to intersect a boundary with. That also makes these the one borough-partitioned
assets with no dependency on `reference_neighborhoods`.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    asset,
)

from urban_rag.cmhc import (
    BEDROOM_TYPES,
    DWELLING_TYPES,
    AVERAGE_RENTS_READING_MODE_URL,
    STATUS_NO_UNITS,
    STATUS_PUBLISHED,
    STATUS_SUPPRESSED,
    TOTAL_LABEL,
    CmhcError,
    normalize_quartier,
    read_average_rents_reading_mode,
    read_quartier_sheet,
    survey_period,
)
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.partitions import date_partitions, quartiers_for, scrape_partitions
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import CmhcResource, ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

BRONZE_GROUP = "bronze_cmhc"
SILVER_GROUP = "silver_cmhc"

#: The borough averages, one row per dwelling type x bedroom class.
VACANCY_FILE = "vacancy_rates.parquet"

#: The borough average rents, one row per bedroom class.
AVERAGE_RENTS_FILE = "average_rents.parquet"

#: The per-quartier rows those averages were taken over. Written by *both*
#: layers, and they are not the same file: bronze holds every quartier the
#: survey publishes for Montreal under the survey's own spelling, silver holds
#: only this borough's, relabelled to the crosswalk's canonical name. Silver
#: keeps its copy because with three quartiers behind a borough figure and most
#: cells suppressed, the average is only readable next to what went into it.
QUARTIERS_FILE = "quartier_vacancy_rates.parquet"

#: The per-quartier rows behind `AVERAGE_RENTS_FILE`, same split as above.
QUARTIER_AVERAGE_RENTS_FILE = "quartier_average_rents.parquet"

#: The slice of the survey this pipeline reads. A scope on what is asked for,
#: not an interpretation of what came back, so it is applied in bronze - the
#: same posture as bounding the Infolot query with a borough outline.
PROVINCE = "Qc"
CENTRE = "Montréal"

#: https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data
SOURCE_URL = (
    "https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/"
    "housing-data/data-tables/rental-market/urban-rental-market-survey-data-vacancy-rates"
)

AVERAGE_RENTS_SOURCE_URL = AVERAGE_RENTS_READING_MODE_URL

# --------------------------------------------------------------------------
# bronze
# --------------------------------------------------------------------------


@asset(
    key_prefix=key_prefix("cmhc_vacancy_survey"),
    partitions_def=date_partitions,
    group_name=BRONZE_GROUP,
    kinds={"parquet"},
    description=(
        "CMHC Rental Market Survey vacancy rates for the Montreal CMA as "
        "published, as bronze/cmhc_vacancy_survey/<YYYY-MM-DD>/"
        f"{QUARTIERS_FILE}: every quartier the survey prints for "
        f"Province={PROVINCE}/Centre={CENTRE}, under the survey's own "
        "spellings, subtotal rows included. Partitioned by date alone - there "
        "is nothing borough-shaped about the workbook, and the crosswalk that "
        f"cuts it into boroughs is applied in silver. Source: {SOURCE_URL}"
    ),
)
def cmhc_vacancy_survey(
    context: AssetExecutionContext,
    cmhc: CmhcResource,
    store: ParquetStore,
) -> MaterializeResult:
    scrape_date = context.partition_key
    survey_year = cmhc.survey_year
    try:
        workbook = cmhc.fetcher().fetch(survey_year)
        survey = read_quartier_sheet(workbook)
        period = survey_period(workbook)
    except CmhcError as exc:
        raise Failure(f"CMHC {survey_year} survey read failed: {exc}")

    montreal = survey[
        (survey["province"] == PROVINCE) & (survey["centre"] == CENTRE)
    ].copy()
    if montreal.empty:
        raise Failure(
            f"The survey publishes no rows for Province={PROVINCE!r}, "
            f"Centre={CENTRE!r}; the workbook's geography labels may have changed."
        )

    montreal = _with_provenance(
        montreal, survey_year=survey_year, period=period, scrape_date=scrape_date
    )
    output_dir = _partition_dir(context, store, scrape_date)
    _clear(context, output_dir)
    path = write_frame(montreal, join(output_dir, QUARTIERS_FILE))

    quartiers = sorted(set(montreal["quartier"]) - {TOTAL_LABEL})
    context.log.info(
        "%d row(s) across %d quartier(s) -> %s", len(montreal), len(quartiers), path
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(montreal),
            "num_quartiers": len(quartiers),
            "quartiers": MetadataValue.text(", ".join(quartiers)),
            "num_published_cells": int(
                (montreal["status"] == STATUS_PUBLISHED).sum()
            ),
            "num_suppressed_cells": int(
                (montreal["status"] == STATUS_SUPPRESSED).sum()
            ),
            "num_no_unit_cells": int((montreal["status"] == STATUS_NO_UNITS).sum()),
            "survey_year": survey_year,
            "survey_period": period or "unknown",
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(SOURCE_URL),
        }
    )


@asset(
    key_prefix=key_prefix("cmhc_rent_survey"),
    partitions_def=date_partitions,
    group_name=BRONZE_GROUP,
    kinds={"parquet"},
    description=(
        "CMHC HMIP reading-mode average rents for the Montreal CMA as "
        "published, as bronze/cmhc_rent_survey/<YYYY-MM-DD>/"
        f"{QUARTIER_AVERAGE_RENTS_FILE}: every quartier the page prints for "
        f"Centre={CENTRE}, under the page's own spellings. Partitioned by "
        "date alone, same as cmhc_vacancy_survey. Source: "
        f"{AVERAGE_RENTS_SOURCE_URL}"
    ),
)
def cmhc_rent_survey(
    context: AssetExecutionContext,
    cmhc: CmhcResource,
    store: ParquetStore,
) -> MaterializeResult:
    scrape_date = context.partition_key
    try:
        table = read_average_rents_reading_mode(
            cmhc.reading_mode_fetcher().fetch_average_rents()
        )
    except CmhcError as exc:
        raise Failure(f"CMHC average-rent reading-mode page read failed: {exc}")

    montreal = table.frame[table.frame["centre"] == CENTRE].copy()
    if montreal.empty:
        raise Failure(
            f"The average-rent page publishes no rows for Centre={CENTRE!r}; "
            "the page geography may have changed."
        )

    montreal = _with_provenance(
        montreal,
        survey_year=table.survey_year,
        period=table.survey_period,
        scrape_date=scrape_date,
    )
    output_dir = _partition_dir(context, store, scrape_date)
    _clear(context, output_dir)
    path = write_frame(montreal, join(output_dir, QUARTIER_AVERAGE_RENTS_FILE))

    quartiers = sorted(set(montreal["quartier"]) - {TOTAL_LABEL})
    context.log.info(
        "%d rent row(s) across %d quartier(s) -> %s",
        len(montreal),
        len(quartiers),
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(montreal),
            "num_quartiers": len(quartiers),
            "quartiers": MetadataValue.text(", ".join(quartiers)),
            "num_published_cells": int(
                (montreal["status"] == STATUS_PUBLISHED).sum()
            ),
            "num_suppressed_cells": int(
                (montreal["status"] == STATUS_SUPPRESSED).sum()
            ),
            "survey_year": table.survey_year,
            "survey_period": table.survey_period,
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(AVERAGE_RENTS_SOURCE_URL),
        }
    )


# --------------------------------------------------------------------------
# silver
# --------------------------------------------------------------------------


@asset(
    key_prefix=key_prefix("vacancy_rates"),
    partitions_def=scrape_partitions,
    deps=[
        AssetDep(
            cmhc_vacancy_survey,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        )
    ],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "One borough's CMHC vacancy rates, as silver/vacancy_rates/"
        "<YYYY-MM-DD>/<neighborhood>/vacancy_rates.parquet: the survey's "
        "Montreal-CMA quartiers that make up the borough (see "
        "partitions.CMHC_QUARTIERS) taken out of that day's "
        "cmhc_vacancy_survey snapshot, relabelled to the crosswalk's "
        "spelling and averaged into one rate per dwelling type x bedroom "
        f"class, with the borough's own rows kept alongside in {QUARTIERS_FILE}. "
        "Both are upserted into silver.vacancy_rates and "
        "silver.quartier_vacancy_rates. Reads parquet, not CMHC: a respelling "
        "upstream fails this asset and leaves the bronze snapshot intact."
    ),
)
def vacancy_rates(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _borough_partition(context)
    quartiers = quartiers_for(neighborhood)

    survey = _read_bronze(
        store, cmhc_vacancy_survey.key.path[-1], scrape_date, QUARTIERS_FILE
    )
    borough = _borough_rows(
        survey,
        neighborhood,
        quartiers,
        sort_by=["quartier", "dwelling_type", "bedroom_type"],
    )
    averages = _average_over_quartiers(borough, quartiers)

    provenance = {
        "neighborhood": neighborhood,
        "survey_year": _only_value(borough, "survey_year"),
        "survey_period": _only_value(borough, "survey_period"),
        "scrape_date": scrape_date,
        "conformed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    for name, value in provenance.items():
        averages[name] = value
        borough[name] = value

    output_dir = _partition_dir(context, store, scrape_date, neighborhood)
    _clear(context, output_dir)
    write_frame(borough, join(output_dir, QUARTIERS_FILE))
    path = write_frame(averages, join(output_dir, VACANCY_FILE))
    loaded = _publish(
        context,
        postgis,
        {"vacancy_rates": averages, "quartier_vacancy_rates": borough},
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    published = averages[averages["num_quartiers"] > 0]
    context.log.info(
        "%s: %d quartier(s) -> %d averaged cell(s), %d with a published rate -> %s",
        neighborhood,
        len(quartiers),
        len(averages),
        len(published),
        path,
    )
    if published.empty:
        # Not a failure: CMHC really does suppress every cell for some
        # boroughs, and an empty-but-correct partition should not block the
        # ones around it.
        context.log.warning(
            "%s: every rate is suppressed or has no units in the %s survey",
            neighborhood,
            provenance["survey_year"],
        )

    overall = published[
        (published["dwelling_type"] == "all") & (published["bedroom_type"] == "all")
    ]["vacancy_rate_pct"]

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(averages),
            "num_quartiers": len(quartiers),
            "quartiers": MetadataValue.text(", ".join(quartiers)),
            "num_quartier_rows": len(borough),
            "num_published_cells": len(published),
            "num_suppressed_cells": int((borough["status"] == STATUS_SUPPRESSED).sum()),
            "num_no_unit_cells": int((borough["status"] == STATUS_NO_UNITS).sum()),
            "overall_vacancy_rate_pct": MetadataValue.float(float(overall.iloc[0]))
            if not overall.empty
            else MetadataValue.text("suppressed"),
            "survey_year": provenance["survey_year"],
            "survey_period": provenance["survey_period"] or "unknown",
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(SOURCE_URL),
            **published_metadata(loaded),
        }
    )


@asset(
    key_prefix=key_prefix("average_rents"),
    partitions_def=scrape_partitions,
    deps=[
        AssetDep(
            cmhc_rent_survey,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        )
    ],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "One borough's CMHC average rents, as silver/average_rents/"
        "<YYYY-MM-DD>/<neighborhood>/average_rents.parquet: the crosswalk "
        "applied to that day's cmhc_rent_survey snapshot and averaged into "
        "one rent per bedroom class, with the borough's own rows kept "
        f"alongside in {QUARTIER_AVERAGE_RENTS_FILE}. Both are upserted into "
        "silver.average_rents and silver.quartier_average_rents."
    ),
)
def average_rents(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    neighborhood, scrape_date = _borough_partition(context)
    quartiers = quartiers_for(neighborhood)

    survey = _read_bronze(
        store,
        cmhc_rent_survey.key.path[-1],
        scrape_date,
        QUARTIER_AVERAGE_RENTS_FILE,
    )
    borough = _borough_rows(
        survey, neighborhood, quartiers, sort_by=["quartier", "bedroom_type"]
    )
    averages = _average_rents_over_quartiers(borough, quartiers)

    provenance = {
        "neighborhood": neighborhood,
        "survey_year": _only_value(borough, "survey_year"),
        "survey_period": _only_value(borough, "survey_period"),
        "scrape_date": scrape_date,
        "conformed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    for name, value in provenance.items():
        averages[name] = value
        borough[name] = value

    output_dir = _partition_dir(context, store, scrape_date, neighborhood)
    _clear(context, output_dir)
    write_frame(borough, join(output_dir, QUARTIER_AVERAGE_RENTS_FILE))
    path = write_frame(averages, join(output_dir, AVERAGE_RENTS_FILE))
    loaded = _publish(
        context,
        postgis,
        {"average_rents": averages, "quartier_average_rents": borough},
        neighborhood=neighborhood,
        scrape_date=scrape_date,
    )

    published = averages[averages["num_quartiers"] > 0]
    context.log.info(
        "%s: %d quartier(s) -> %d averaged rent cell(s), "
        "%d with a published rent -> %s",
        neighborhood,
        len(quartiers),
        len(averages),
        len(published),
        path,
    )
    if published.empty:
        context.log.warning(
            "%s: every average-rent cell is suppressed in the %s survey",
            neighborhood,
            provenance["survey_year"],
        )

    overall = published[published["bedroom_type"] == "all"]["average_rent_cad"]

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(averages),
            "num_quartiers": len(quartiers),
            "quartiers": MetadataValue.text(", ".join(quartiers)),
            "num_quartier_rows": len(borough),
            "num_published_cells": len(published),
            "num_suppressed_cells": int((borough["status"] == STATUS_SUPPRESSED).sum()),
            "overall_average_rent_cad": MetadataValue.float(float(overall.iloc[0]))
            if not overall.empty
            else MetadataValue.text("suppressed"),
            "survey_year": provenance["survey_year"],
            "survey_period": provenance["survey_period"],
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(AVERAGE_RENTS_SOURCE_URL),
            **published_metadata(loaded),
        }
    )


# --------------------------------------------------------------------------
# shared
# --------------------------------------------------------------------------


def _borough_partition(context: AssetExecutionContext) -> tuple[str, str]:
    dimensions = context.partition_key.keys_by_dimension
    return dimensions["neighborhood"], dimensions["date"][:10]


def _partition_dir(
    context: AssetExecutionContext,
    store: ParquetStore,
    scrape_date: str,
    neighborhood: str | None = None,
) -> str:
    return store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )


def _clear(context: AssetExecutionContext, output_dir: str) -> None:
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))


def _publish(
    context: AssetExecutionContext,
    postgis: PostgisResource,
    datasets: dict[str, pd.DataFrame],
    *,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, dict[str, int]]:
    """The borough average and the quartier rows it was taken over, upserted.

    One transaction for both, so a reader never sees a borough figure without
    the rows behind it - which for a table where most cells are suppressed is
    the difference between a number and a number that can be checked.

    Called after the parquet is written: the crosswalk is the expensive part
    and the survey is a live publication no later run can re-read, so a
    database that is down should cost the load and not the conforming.
    """
    try:
        return publish(
            postgis.connect,
            datasets,
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{', '.join(datasets)} for {neighborhood} {scrape_date} were "
            f"written to the tree but could not be published: {exc}"
        ) from exc


def _with_provenance(
    frame: pd.DataFrame, *, survey_year, period, scrape_date: str
) -> pd.DataFrame:
    frame["survey_year"] = survey_year
    frame["survey_period"] = period
    frame["scrape_date"] = scrape_date
    frame["scraped_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return frame


def _read_bronze(
    store: ParquetStore, asset_name: str, scrape_date: str, filename: str
) -> pd.DataFrame:
    """That day's bronze survey snapshot, or a `Failure` naming what to run."""
    path = join(store.partition_dir(asset_name, scrape_date), filename)
    if not filesystem(path).exists(path):
        raise Failure(
            f"{path} is missing; materialize {asset_name} for {scrape_date} first."
        )
    frame = pd.read_parquet(path, storage_options=storage_options(path))
    if frame.empty:
        raise Failure(f"{path} holds no survey row to conform.")
    return frame


def _borough_rows(
    survey: pd.DataFrame,
    neighborhood: str,
    quartiers: tuple[str, ...],
    *,
    sort_by: list[str],
) -> pd.DataFrame:
    """The bronze rows for one borough's quartiers, in the crosswalk's order.

    Matched through `normalize_quartier`, so the crosswalk survives the
    respellings CMHC varies between publications; the `quartier` column is
    then relabelled to the crosswalk's own spelling, so two survey years of
    this asset stack into one frame without the punctuation drifting.

    A quartier the crosswalk names but the snapshot does not publish is a
    `Failure` rather than a silently shorter average: a rename that went
    unnoticed would quietly change what the borough figure means. It fails
    here, in silver, so the bronze snapshot it was read from survives the
    failure and can be looked at.
    """
    # The zone subtotals repeat their quartiers' rows and would double-count.
    # Bronze keeps them because the survey prints them; the guard belongs
    # here. None of the mapped names is "Total" anyway, so this only guards
    # against a future one that is.
    published_rows = survey[survey["quartier"] != TOTAL_LABEL]

    keys = {normalize_quartier(q): q for q in quartiers}
    published_keys = published_rows["quartier"].map(normalize_quartier)
    missing = [name for key, name in keys.items() if key not in set(published_keys)]
    if missing:
        available = sorted(set(published_rows["quartier"]))
        raise Failure(
            f"{neighborhood}: the {CENTRE} survey snapshot publishes no quartier "
            f"named {', '.join(repr(q) for q in missing)}. It has: "
            f"{', '.join(available)}"
        )

    rows = published_rows[published_keys.isin(keys)].copy()
    rows["quartier"] = pd.Categorical(
        rows["quartier"].map(lambda name: keys[normalize_quartier(name)]),
        categories=quartiers,
    )
    return rows.sort_values(sort_by, ignore_index=True).astype({"quartier": "string"})


def _average_over_quartiers(
    borough: pd.DataFrame, quartiers: tuple[str, ...]
) -> pd.DataFrame:
    """Mean vacancy rate per dwelling type x bedroom class, over the quartiers.

    Unweighted, because this table publishes rates and nothing to weight them
    by: the universe counts that would turn this into the borough's true
    vacancy rate are in a different CMHC table. A borough figure here is the
    mean of its quartiers, each counting once - which `num_quartiers` on every
    row makes checkable, since suppressed cells drop out of the mean and the
    denominator is rarely the full set.

    Every dwelling type x bedroom class is emitted whether or not anything was
    published for it, so the grid is the same shape for every borough and a
    suppressed cell is visible as a row rather than as an absence.
    """
    published = borough[borough["status"] == STATUS_PUBLISHED]
    grouped = published.groupby(["dwelling_type", "bedroom_type"], observed=True)
    stats = grouped.agg(
        vacancy_rate_pct=("vacancy_rate_pct", "mean"),
        min_vacancy_rate_pct=("vacancy_rate_pct", "min"),
        max_vacancy_rate_pct=("vacancy_rate_pct", "max"),
        num_quartiers=("vacancy_rate_pct", "count"),
        averaged_quartiers=("quartier", lambda values: ", ".join(values)),
    )

    grid = pd.MultiIndex.from_product(
        [list(DWELLING_TYPES.values()), list(BEDROOM_TYPES.values())],
        names=["dwelling_type", "bedroom_type"],
    )
    frame = stats.reindex(grid).reset_index()
    frame["num_quartiers"] = frame["num_quartiers"].fillna(0).astype("int64")
    frame["averaged_quartiers"] = frame["averaged_quartiers"].fillna("")
    frame["num_quartiers_mapped"] = len(quartiers)
    return frame


def _average_rents_over_quartiers(
    borough: pd.DataFrame, quartiers: tuple[str, ...]
) -> pd.DataFrame:
    published = borough[borough["status"] == STATUS_PUBLISHED]
    grouped = published.groupby("bedroom_type", observed=True)
    stats = grouped.agg(
        average_rent_cad=("average_rent_cad", "mean"),
        min_average_rent_cad=("average_rent_cad", "min"),
        max_average_rent_cad=("average_rent_cad", "max"),
        num_quartiers=("average_rent_cad", "count"),
        averaged_quartiers=("quartier", lambda values: ", ".join(values)),
    )

    grid = pd.Index(
        list(dict.fromkeys(BEDROOM_TYPES.values())),
        name="bedroom_type",
    )
    frame = stats.reindex(grid).reset_index()
    frame["num_quartiers"] = frame["num_quartiers"].fillna(0).astype("int64")
    frame["averaged_quartiers"] = frame["averaged_quartiers"].fillna("")
    frame["num_quartiers_mapped"] = len(quartiers)
    return frame


def _only_value(frame: pd.DataFrame, column: str):
    """The one value ``column`` carries, or a `Failure` if it carries several.

    The survey year and period travel down from bronze as columns rather than
    from `CmhcResource`, because a silver partition describes the snapshot it
    actually read - which may not be the year the resource is configured for
    today.
    """
    values = frame[column].dropna().unique()
    if len(values) == 0:
        return None
    if len(values) > 1:
        raise Failure(f"The survey snapshot carries multiple {column}: {values!r}")
    value = values[0]
    return value.item() if hasattr(value, "item") else value
