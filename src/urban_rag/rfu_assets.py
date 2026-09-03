"""The RFU snapshot: Quebec's standardized property wealth, and its factor.

One bronze asset over MAMH's *richesse foncière uniformisée*, catalogued on
Données Québec. Partitioned by scrape date alone, like `property_assessment_roll`
and the two CMHC surveys: the publication is one row per Quebec *organisme
municipal* and has no borough axis to slice on.

Why this file is read at all, and what `CSALX02163` is, is in `urban_rag.rfu`.
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

from urban_rag.guards import guard_current_scrape_month
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.open_data import OpenDataError, decode_csv
from urban_rag.partitions import date_partitions
from urban_rag.resources import ParquetStore, RfuResource
from urban_rag.rfu import (
    COMPARATIVE_FACTOR_COLUMN,
    GEO_CODE_COLUMN,
    MONTREAL_GEO_CODE,
    ORGANISM_NAME_COLUMN,
    RFU_DATASET,
    RFU_TOTAL_COLUMN,
    RfuError,
    pick_data_file,
    pick_postes_file,
)
from urban_rag.storage import clear_parquet, join

GROUP = "bronze_open_data"

#: The one file the RFU is written to, under
#: `bronze/uniformized_property_wealth/<YYYY-MM-DD>/`.
RFU_FILE = "rfu.parquet"

#: The field descriptions, written beside it. A reader who meets `CSALX02163`
#: in the data needs this to learn it is the *facteur comparatif*, so the two
#: travel together rather than the codes being renamed on the way in - bronze
#: keeps its publisher's vocabulary.
POSTES_FILE = "rfu_postes.parquet"


@asset(
    key_prefix=key_prefix("uniformized_property_wealth"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"parquet"},
    description=(
        "Quebec's richesse fonciere uniformisee (MAMH), one row per organisme "
        "municipal, snapshot per scrape date under "
        f"bronze/uniformized_property_wealth/<YYYY-MM-DD>/{RFU_FILE}. Carries "
        f"{COMPARATIVE_FACTOR_COLUMN}, the facteur comparatif - the "
        "sales-derived multiplier that carries a roll value to a market one, "
        "and the only such number published openly per municipality. Source: "
        "https://www.donneesquebec.ca/recherche/dataset/richesse-fonciere-uniformisee"
    ),
)
@guard_current_scrape_month
def uniformized_property_wealth(
    context: AssetExecutionContext,
    rfu: RfuResource,
    store: ParquetStore,
) -> MaterializeResult:
    scrape_date = context.partition_key
    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)

    client = rfu.client()
    package = client.package(RFU_DATASET)
    filenames = [resource.filename for resource in package.resources]

    # Which year to read is resolved against the catalogue rather than built
    # into a URL, so a year the dataset has not published yet is named here
    # with the years it does have, instead of 404ing on a guessed filename.
    try:
        year, data_file = pick_data_file(filenames, rfu.rfu_year)
    except RfuError as exc:
        raise Failure(str(exc)) from exc
    resource = package.resource(data_file)

    scraped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))

    # The publication is the asset; a failure here is worth the whole partition.
    frame = _rfu_to_frame(
        client.download(resource),
        source_file=resource.filename,
        rfu_year=year,
        scrape_date=scrape_date,
        scraped_at=scraped_at,
    )
    if COMPARATIVE_FACTOR_COLUMN not in frame.columns:
        # The one column this asset is read for. Absent, the file is still a
        # valid RFU and every downstream use of it is gone, so it fails here
        # rather than writing a snapshot nothing can use.
        raise Failure(
            f"{resource.filename} has no {COMPARATIVE_FACTOR_COLUMN} "
            f"(facteur comparatif) column; it publishes "
            f"{', '.join(sorted(frame.columns))}."
        )

    path = write_frame(frame, join(output_dir, RFU_FILE))
    context.log.info(
        "%s: %d organisme(s) -> %s", resource.filename, len(frame), path
    )

    factor = _montreal_factor(frame)
    if factor is None:
        # Reported, not raised: the province publishes the file, and a run that
        # fetched it correctly has done its job. Montreal missing from it is a
        # fact about the publication that the next asset to read the column has
        # to handle, and it is on the record here.
        context.log.warning(
            "%s carries no row for cod_geo=%s", resource.filename, MONTREAL_GEO_CODE
        )

    metadata = {
        "dagster/row_count": len(frame),
        "rfu_year": year,
        "num_organismes": int(frame[GEO_CODE_COLUMN].nunique())
        if GEO_CODE_COLUMN in frame.columns
        else len(frame),
        # The headline number: what a 2026-roll value has to be multiplied by
        # to read as a market one. `make comparables MARKET_FACTOR=<this>`.
        "montreal_comparative_factor": factor
        if factor is not None
        else MetadataValue.text("absent"),
        "output_path": MetadataValue.path(str(path)),
        "source_url": MetadataValue.url(
            f"https://www.donneesquebec.ca/recherche/dataset/{RFU_DATASET}"
        ),
        "license": package.license_title or "unknown",
        "rfu_last_modified": resource.last_modified or "unknown",
    }

    # The descriptions are a companion table, so a bad one costs its own file
    # only - the same posture `reference_neighborhoods` takes with its counts.
    postes_file = pick_postes_file(filenames, year)
    if postes_file is None:
        context.log.warning("no field descriptions published for RFU %d", year)
        metadata["postes_error"] = f"no descriptions published for {year}"
    else:
        try:
            postes = package.resource(postes_file)
            descriptions = _postes_to_frame(
                client.download(postes),
                source_file=postes.filename,
                rfu_year=year,
                scrape_date=scrape_date,
                scraped_at=scraped_at,
            )
            write_frame(descriptions, join(output_dir, POSTES_FILE))
            context.log.info("%s: %d row(s)", postes.filename, len(descriptions))
            metadata["num_postes"] = len(descriptions)
        except (OpenDataError, ValueError) as exc:
            context.log.warning("%s: skipped (%s)", postes_file, exc)
            metadata["postes_error"] = str(exc)

    return MaterializeResult(metadata=metadata)


def _rfu_to_frame(
    content: bytes,
    *,
    source_file: str,
    rfu_year: int,
    scrape_date: str,
    scraped_at: str,
) -> pd.DataFrame:
    """RFU CSV bytes -> DataFrame, with the provenance columns attached.

    Column names are left exactly as MAMH spells them - `cod_geo` lower-case
    beside `CSALX02163` upper - for the reason `street_network` keeps the
    geobase's: this is one file with one spelling, and the codes *are* the
    vocabulary the companion descriptions are keyed on. Lower-casing them here
    would break the join to that table.

    Every column stays text, as `decode_csv` leaves it: `cod_geo` is a
    zero-padded five-digit code that has to keep its leading zero to join
    against the roll's `code_mun`, and the factor is read by the one caller
    that wants it as a number rather than cast for all of them.
    """
    frame = decode_csv(content, filename=source_file)
    if frame.empty:
        raise Failure(f"{source_file}: the portal returned no rows")
    frame["source_file"] = source_file
    frame["rfu_year"] = rfu_year
    # `scrape_date` is a column because the output path holds a bare date
    # rather than a hive `scrape_date=` key.
    frame["scrape_date"] = scrape_date
    frame["scraped_at"] = scraped_at
    return frame


def _postes_to_frame(
    content: bytes,
    *,
    source_file: str,
    rfu_year: int,
    scrape_date: str,
    scraped_at: str,
) -> pd.DataFrame:
    """Field-description CSV -> DataFrame, keyed the same way as the data."""
    frame = decode_csv(content, filename=source_file)
    frame["source_file"] = source_file
    frame["rfu_year"] = rfu_year
    frame["scrape_date"] = scrape_date
    frame["scraped_at"] = scraped_at
    return frame


def _montreal_factor(frame: pd.DataFrame) -> float | None:
    """Ville de Montréal's *facteur comparatif*, or ``None`` if it has no row.

    The agglomeration files one roll, so every on-island municipality carries
    the same factor and reading the city's row is reading all sixteen.
    """
    if GEO_CODE_COLUMN not in frame.columns:
        return None
    rows = frame.loc[frame[GEO_CODE_COLUMN] == MONTREAL_GEO_CODE]
    if rows.empty:
        return None
    value = pd.to_numeric(rows[COMPARATIVE_FACTOR_COLUMN], errors="coerce").iloc[0]
    return None if pd.isna(value) else float(value)


__all__ = [
    "GROUP",
    "POSTES_FILE",
    "RFU_FILE",
    "RFU_TOTAL_COLUMN",
    "ORGANISM_NAME_COLUMN",
    "uniformized_property_wealth",
]
