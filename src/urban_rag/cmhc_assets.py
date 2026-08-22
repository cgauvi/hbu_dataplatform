"""Asset sourced from CMHC's Rental Market Survey, rather than from Spectrum
or donnees.montreal.ca.

CMHC surveys the Montreal *census metropolitan area* and cuts it into its own
neighborhoods, which do not line up with the boroughs this pipeline is
partitioned on: `VSMPE` is three CMHC quartiers, `Outremont` is one, and `PR`
is the borough plus a neighbouring municipality CMHC will not split out. The
crosswalk is `urban_rag.partitions.CMHC_QUARTIERS`, and this asset averages
each borough's quartiers into one set of rates per bedroom class.

Unlike the lots and the buildings, the cut is a name lookup rather than a
spatial join: the survey publishes rates, not geometry, so there is nothing to
intersect a borough boundary with. That also makes this the one
borough-partitioned asset with no dependency on `reference_neighborhoods`.
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from urban_rag.cmhc import (
    BEDROOM_TYPES,
    DWELLING_TYPES,
    STATUS_NO_UNITS,
    STATUS_PUBLISHED,
    STATUS_SUPPRESSED,
    TOTAL_LABEL,
    CmhcError,
    normalize_quartier,
    read_quartier_sheet,
    survey_period,
)
from urban_rag.frames import write_frame
from urban_rag.partitions import quartiers_for, scrape_partitions
from urban_rag.resources import CmhcResource, ParquetStore
from urban_rag.storage import clear_parquet, join

GROUP = "cmhc"

#: The borough averages, one row per dwelling type x bedroom class.
VACANCY_FILE = "vacancy_rates.parquet"

#: The per-quartier rows those averages were taken over, kept alongside them:
#: with three quartiers behind a borough figure and most cells suppressed, the
#: average is only readable next to what went into it.
QUARTIERS_FILE = "quartier_vacancy_rates.parquet"

#: The slice of the survey this pipeline reads.
PROVINCE = "Qc"
CENTRE = "Montréal"

#: https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data
SOURCE_URL = (
    "https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/"
    "housing-data/data-tables/rental-market/urban-rental-market-survey-data-vacancy-rates"
)


@asset(
    partitions_def=scrape_partitions,
    group_name=GROUP,
    description=(
        "CMHC Rental Market Survey vacancy rates for one borough, as "
        "vacancy_rates/<YYYY-MM-DD>/<neighborhood>/vacancy_rates.parquet: the "
        "survey's Montreal-CMA neighborhoods that make up the borough "
        "(see partitions.CMHC_QUARTIERS), averaged into one rate per dwelling "
        "type x bedroom class, with the per-quartier rows kept alongside in "
        f"{QUARTIERS_FILE}. Source: {SOURCE_URL}"
    ),
)
def vacancy_rates(
    context: AssetExecutionContext,
    cmhc: CmhcResource,
    store: ParquetStore,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    quartiers = quartiers_for(neighborhood)
    survey_year = cmhc.survey_year
    fetcher = cmhc.fetcher()
    try:
        workbook = fetcher.fetch(survey_year)
        survey = read_quartier_sheet(workbook)
        period = survey_period(workbook)
    except CmhcError as exc:
        raise Failure(f"CMHC {survey_year} survey read failed: {exc}")

    borough = _borough_rows(survey, neighborhood, quartiers)

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # Written as columns because the output path holds bare keys rather than
    # hive `key=value` pairs, so a reader that opens one file still knows
    # which snapshot it belongs to.
    provenance = {
        "neighborhood": neighborhood,
        "survey_year": survey_year,
        "survey_period": period,
        "scrape_date": scrape_date,
        "scraped_at": scraped_at,
    }

    averages = _average_over_quartiers(borough, quartiers)
    for name, value in provenance.items():
        averages[name] = value
        borough[name] = value

    write_frame(borough, join(output_dir, QUARTIERS_FILE))
    path = write_frame(averages, join(output_dir, VACANCY_FILE))

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
            "%s: every rate is suppressed or has no units in the %d survey",
            neighborhood,
            survey_year,
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
            "survey_year": survey_year,
            "survey_period": period or "unknown",
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(SOURCE_URL),
        }
    )


def _borough_rows(
    survey: pd.DataFrame, neighborhood: str, quartiers: tuple[str, ...]
) -> pd.DataFrame:
    """The survey rows for one borough's quartiers, in the map's own order.

    Matched through `normalize_quartier`, so the crosswalk survives the
    respellings CMHC varies between publications; the `quartier` column keeps
    the sheet's own label either way.

    A quartier the map names but the workbook does not publish is a `Failure`
    rather than a silently shorter average: a rename that went unnoticed would
    quietly change what the borough figure means.
    """
    montreal = survey[
        (survey["province"] == PROVINCE)
        & (survey["centre"] == CENTRE)
        # The zone subtotals repeat their quartiers' rows and would
        # double-count; none of the mapped names is "Total" anyway, so this
        # only guards against a future one that is.
        & (survey["quartier"] != TOTAL_LABEL)
    ]
    if montreal.empty:
        raise Failure(
            f"The survey publishes no rows for Province={PROVINCE!r}, "
            f"Centre={CENTRE!r}; the workbook's geography labels may have changed."
        )

    keys = {normalize_quartier(q): q for q in quartiers}
    published_keys = montreal["quartier"].map(normalize_quartier)
    missing = [name for key, name in keys.items() if key not in set(published_keys)]
    if missing:
        published = sorted(set(montreal["quartier"]))
        raise Failure(
            f"{neighborhood}: the {CENTRE} survey publishes no quartier named "
            f"{', '.join(repr(q) for q in missing)}. It has: "
            f"{', '.join(published)}"
        )

    rows = montreal[published_keys.isin(keys)].copy()
    # Relabelled to the crosswalk's spelling and ordering, so two survey years
    # of this asset stack into one frame without the punctuation drifting.
    rows["quartier"] = pd.Categorical(
        rows["quartier"].map(lambda name: keys[normalize_quartier(name)]),
        categories=quartiers,
    )
    return rows.sort_values(
        ["quartier", "dwelling_type", "bedroom_type"], ignore_index=True
    ).astype({"quartier": "string"})


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
