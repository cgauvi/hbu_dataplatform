"""What a square foot of commercial floor earns, and where that number is from.

Three assets over two publishers, filling the last gap in the income side of a
cap rate. CMHC prices a dwelling; the Altus cost guide prices a building to put
up; neither says what a square foot of retail, office or warehouse *rents for*.
Until these assets that figure was two constants in `urban_rag.program` -
``COMMERCIAL_REVENUE_PER_SQFT_CAD = 80.0`` and
``INDUSTRIAL_REVENUE_PER_SQFT_CAD = 30.0`` - stated rather than surveyed, and
both materially above what Montreal actually pays. The measured figures are
$36.59 gross for office and $18.74 gross for industrial in 2026 Q2.

`montreal_commercial_rents` snapshots Cushman & Wakefield's Montreal
MarketBeats - one report per sector per quarter, discovered off the landing
page because the filenames cannot be constructed. See `urban_rag.marketbeat`.

`commercial_rent_index` snapshots Statistics Canada's Commercial Rents Services
Price Index for the Montreal CMA, quarterly, by building type. See
`urban_rag.crspi`.

`commercial_rents` puts them together at one rate per rent class for one
borough, which is the grain `lot_assessment_comparables` prices floor area at.

----------------------------------------------------------------------------
Why it takes two publishers
----------------------------------------------------------------------------

**C&W measures levels but not retail.** They publish a Montreal office
MarketBeat and a Montreal industrial one, and no retail one at all - so retail,
which is most of the commercial floor in a borough of triplexes and corner
shops, has no free survey of the level anywhere.

**Statistics Canada covers retail but publishes no level.** CRSPI is an index,
`2019=100` per series, and the only honest thing to do with an index is move
one series through time. So retail is a *stated* base carried forward by the
retail index, and it is the one rate in this chain with no survey behind it.
`RentsConfig.retail_base_gross_rent_psf_cad` is where that judgement lives and
every row records it.

**The index also fixes a timing problem nobody would otherwise notice.** A
MarketBeat lands weeks after the quarter it describes, so a scrape date in
between has a rent measured for a quarter that is not the one being scraped.
`escalate` moves it, and `rent_basis` on every row says whether the number is
``measured`` (the survey quarter is the index's latest), ``escalated`` (moved),
``unescalated`` (the index does not reach) or ``stated`` (the retail base).

**What it must never do is cross two series.** ``Retail / Office`` at one
quarter is how retail has moved *relative to* office since 2019, not the ratio
of their rents; using it to turn an office level into a retail one would
silently assume the two were equal in 2019. `crspi.escalate` takes a single
building type for exactly that reason, and this module never asks it for two.

----------------------------------------------------------------------------
The borough gets its own rent
----------------------------------------------------------------------------

The MarketBeats carry a submarket table, and `partitions.MARKETBEAT_SUBMARKETS`
says which submarket each borough sits in - Villeray-Saint-Michel-Parc-
Extension is Midtown North on both maps. That matters: Midtown North office is
$22.39 gross against $36.59 island-wide, and pricing a Villeray dépanneur at
the island average would overstate it by 60 per cent.

A borough with no submarket mapped falls back to the whole-market row and says
so in `is_submarket_rate`. That is the same fallback shape `vacancy_rates`
takes when CMHC suppresses a quartier: a worse answer, reported as such, rather
than no answer.
"""

import json
from datetime import datetime, timezone

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    AssetDep,
    asset,
)
from pydantic import Field

from urban_rag.crspi import (
    BASE_PERIOD,
    PRICED_TYPES,
    CrspiError,
    escalate,
    latest_period,
    read_montreal,
)
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.marketbeat import (
    INDUSTRIAL,
    OFFICE,
    RENT_BASIS,
    SECTORS,
    MarketBeatError,
    discover_reports,
    latest_by_sector,
    market_total,
    parse_submarkets,
)
from urban_rag.partitions import date_partitions, scrape_partitions, submarket_for
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import (
    CrspiResource,
    MarketBeatResource,
    ParquetStore,
    PostgisResource,
)
from urban_rag.storage import clear_parquet, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

#: Bronze, and its own group rather than `bronze_open_data`: these are two
#: commercial-property publishers rather than government open-data portals, and
#: the group is what the Dagster UI sorts an asset into.
GROUP = "bronze_commercial_rents"

#: The silver asset below. `silver_assessment` is the roll's lineage and this
#: is not the roll - it is what prices the floor the roll counts - so it gets
#: its own group for the same reason `silver_streets` has one.
SILVER_GROUP = "silver_commercial_rents"

#: The file each bronze partition writes, under
#: `bronze/<asset>/<YYYY-MM-DD>/`.
MARKETBEAT_FILE = "marketbeat_submarkets.parquet"
RENT_INDEX_FILE = "commercial_rent_index.parquet"

#: The one file the silver asset writes, under
#: `silver/commercial_rents/<YYYY-MM-DD>/<neighborhood>/`.
COMMERCIAL_RENTS_FILE = "commercial_rents.parquet"

#: The rent classes a lot's floor area is priced under, and the CRSPI building
#: type each is carried through time by. `residential` is absent on purpose:
#: CMHC measures a dwelling directly and per borough, so it needs neither a
#: level from here nor an index to move it.
#:
#: `office` and `industrial` are measured by C&W. `retail` is the stated base -
#: see the module docstring - and is the reason this mapping is a table rather
#: than two hard-coded lookups.
RENT_CLASSES: dict[str, str] = {
    "office": "office",
    "retail": "retail",
    "industrial": "industrial",
}

#: Which MarketBeat sector supplies each rent class's *level*, where one does.
#: Retail is absent, and that absence is the whole reason `RentsConfig` carries
#: a base rent.
SECTOR_FOR_CLASS: dict[str, str] = {"office": OFFICE, "industrial": INDUSTRIAL}


class RentsConfig(Config):
    """The one rent nobody publishes, and the quarter it is stated for.

    Config rather than a constant because it is the single judgement in this
    chain that no survey stands behind. C&W measure office and industrial
    levels and Statistics Canada measures how all three have moved; nobody
    publishes a free Montreal *retail* level, so this is it.

    **It is a gross rent**, matching what the office MarketBeat publishes and
    what `urban_rag.comparables` charges a tenant - not the net figure a
    brokerage quotes a landlord. Montreal neighbourhood-strip retail asks
    roughly $14-18 per square foot net, and the additional rent on that kind of
    space runs to about the same again; 26.0 is the middle of what that comes
    to, and it is a number to move when a better one turns up rather than a
    measurement.

    ``retail_base_period`` is the quarter it is stated *for*, in the
    publisher's own `YYYY-MM` spelling, because a rent with no date on it
    cannot be escalated and a base escalated from the wrong quarter is wrong by
    however much the index moved in between. Every row records both, so a rate
    can always be read back against the assumption that produced it - the rule
    `max_built_area_m2` and `frontage_buffer_m` follow.
    """

    retail_base_gross_rent_psf_cad: float = Field(
        default=26.0,
        gt=0,
        description=(
            "Stated Montreal neighbourhood retail rent, gross, per square "
            "foot per year. The one rate here with no survey behind it."
        ),
    )
    retail_base_period: str = Field(
        default="2025-01",
        description=(
            "The quarter the retail base is stated for, as YYYY-MM (the month "
            "the quarter starts in, which is how CRSPI writes one)."
        ),
    )


@asset(
    key_prefix=key_prefix("montreal_commercial_rents"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"pdf", "parquet"},
    description=(
        "Cushman & Wakefield's Montreal office and industrial MarketBeats, "
        "snapshot per scrape date as one row per (sector, submarket) under "
        f"bronze/montreal_commercial_rents/<YYYY-MM-DD>/{MARKETBEAT_FILE}. "
        "Carries the net, additional and gross rent per square foot per year "
        "on one footing across both sectors - office publishes a full-service "
        "gross rent, industrial a direct net rent with the operating costs in "
        "a column of their own. The reports are discovered off the landing "
        "page rather than named: the filename changes shape between sectors "
        "and quarters, and only the /<year>/q<n>/ path is stable."
    ),
)
def montreal_commercial_rents(
    context: AssetExecutionContext,
    store: ParquetStore,
    marketbeat: MarketBeatResource,
) -> MaterializeResult:
    scrape_date = context.partition_key
    fetcher = marketbeat.fetcher()
    try:
        reports = discover_reports(fetcher.landing_html())
        latest = latest_by_sector(reports)
    except MarketBeatError as exc:
        raise Failure(f"{marketbeat.landing_url}: {exc}") from exc

    frames = []
    for sector in SECTORS:
        report = latest[sector]
        try:
            parsed = parse_submarkets(fetcher.report_pdf(report), sector=sector)
        except MarketBeatError as exc:
            raise Failure(f"{report.sector} {report.period}: {exc}") from exc
        parsed["report_period"] = report.period
        parsed["report_period_start"] = report.period_start
        parsed["report_year"] = report.year
        parsed["report_quarter"] = report.quarter
        parsed["published_rent_basis"] = RENT_BASIS[sector]
        parsed["source_url"] = report.url
        frames.append(parsed)
        context.log.info(
            "%s %s: %d submarket(s), market total $%.2f gross",
            sector,
            report.period,
            len(parsed),
            float(market_total(parsed)["gross_rent_psf_cad"]),
        )

    merged = pd.concat(frames, ignore_index=True)
    merged["scrape_date"] = scrape_date
    merged["scraped_at"] = datetime.now(timezone.utc).isoformat()

    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)
    clear_parquet(output_dir)
    path = write_frame(merged, join(output_dir, MARKETBEAT_FILE))

    totals = {
        sector: float(
            market_total(merged[merged["sector"] == sector])["gross_rent_psf_cad"]
        )
        for sector in SECTORS
    }
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(merged),
            "num_submarkets": len(merged),
            **{
                f"num_{sector}_submarkets": int((merged["sector"] == sector).sum())
                for sector in SECTORS
            },
            **{
                f"{sector}_period": latest[sector].period for sector in SECTORS
            },
            **{
                f"{sector}_market_gross_rent_psf_cad": round(value, 2)
                for sector, value in totals.items()
            },
            # The two publications need not be the same quarter - one sector's
            # report lands before the other's - and a partition where they are
            # far apart is one whose office and industrial rents describe
            # different markets.
            "reports_in_step": len({latest[s].period for s in SECTORS}) == 1,
            "num_reports_listed": len(reports),
            "output_path": MetadataValue.path(str(path)),
            **{
                f"{sector}_source_url": MetadataValue.url(latest[sector].url)
                for sector in SECTORS
            },
        }
    )


@asset(
    key_prefix=key_prefix("commercial_rent_index"),
    partitions_def=date_partitions,
    group_name=GROUP,
    kinds={"parquet"},
    description=(
        "Statistics Canada's Commercial Rents Services Price Index for the "
        "Montreal CMA (table 18-10-0260-01), snapshot per scrape date as one "
        "row per (quarter, building type) under bronze/commercial_rent_index/"
        f"<YYYY-MM-DD>/{RENT_INDEX_FILE}. Quarterly, 2019=100, back to 2006 "
        "for the total and 2019 for the rest. An index and not a level: it is "
        "what carries a measured rent from the quarter it was surveyed to the "
        "quarter being scraped, and what carries the stated retail base "
        "forward. Re-fetched every scrape date because the table is revised."
    ),
)
def commercial_rent_index(
    context: AssetExecutionContext, store: ParquetStore, crspi: CrspiResource
) -> MaterializeResult:
    scrape_date = context.partition_key
    fetcher = crspi.fetcher()
    try:
        frame = read_montreal(fetcher.fetch())
    except CrspiError as exc:
        raise Failure(f"{fetcher.url}: {exc}") from exc

    missing = [name for name in PRICED_TYPES if name not in set(frame["building_type"])]
    if missing:
        # Every one of them prices a rent class downstream, so a table that
        # dropped one is a table this platform cannot use - and the message
        # that helps names the series rather than leaving a null rate to be
        # discovered in a cap rate two assets later.
        raise Failure(
            f"{fetcher.url} carries no Montreal series for: {', '.join(missing)}. "
            f"It published {sorted(frame['building_type'].unique())}."
        )

    frame["scrape_date"] = scrape_date
    frame["scraped_at"] = datetime.now(timezone.utc).isoformat()
    frame["source_url"] = fetcher.url

    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date)
    clear_parquet(output_dir)
    path = write_frame(frame, join(output_dir, RENT_INDEX_FILE))

    latest = {name: latest_period(frame, name) for name in PRICED_TYPES}
    context.log.info(
        "%s: %d Montreal index row(s), latest %s",
        scrape_date,
        len(frame),
        ", ".join(f"{name} {period}" for name, period in latest.items()),
    )
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "num_index_rows": len(frame),
            "base_period": BASE_PERIOD,
            "first_period": str(frame["period"].min()),
            **{f"latest_{name}_period": period for name, period in latest.items()},
            "table_id": crspi.table_id,
            "output_path": MetadataValue.path(str(path)),
            "source_url": MetadataValue.url(fetcher.url),
        }
    )


@asset(
    key_prefix=key_prefix("commercial_rents"),
    partitions_def=scrape_partitions,
    deps=[
        AssetDep(
            upstream,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        )
        for upstream in (montreal_commercial_rents, commercial_rent_index)
    ],
    group_name=SILVER_GROUP,
    kinds={"postgres", "parquet"},
    description=(
        "One rent per rent class for one borough: office and industrial from "
        "the Cushman & Wakefield submarket the borough sits in, retail from a "
        "stated base, all three carried to the latest quarter Statistics "
        "Canada's index publishes. rent_psf_cad is gross - what a tenant pays "
        "- on all three, whichever way the publisher quotes it. rent_basis "
        "says whether the figure was measured, escalated, left unescalated or "
        "stated, and is_submarket_rate whether the borough got its own "
        "submarket or the island-wide fallback. Written to silver/"
        f"commercial_rents/<YYYY-MM-DD>/<neighborhood>/{COMMERCIAL_RENTS_FILE} "
        "and upserted into silver.commercial_rents on (scrape_date, "
        "neighborhood, rent_class)."
    ),
)
def commercial_rents(
    context: AssetExecutionContext,
    config: RentsConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    submarkets = _read(
        store.partition_dir(montreal_commercial_rents.key.path[-1], scrape_date),
        MARKETBEAT_FILE,
        asset_name=montreal_commercial_rents.key.path[-1],
        partition=scrape_date,
    )
    index = _read(
        store.partition_dir(commercial_rent_index.key.path[-1], scrape_date),
        RENT_INDEX_FILE,
        asset_name=commercial_rent_index.key.path[-1],
        partition=scrape_date,
    )

    wanted = submarket_for(neighborhood)
    rows = [
        _measured_rate(
            submarkets, index, rent_class=name, neighborhood=neighborhood,
            wanted=wanted,
        )
        if name in SECTOR_FOR_CLASS
        else _stated_rate(index, config)
        for name in RENT_CLASSES
    ]
    frame = pd.DataFrame(rows)
    frame["neighborhood"] = neighborhood
    frame["scrape_date"] = scrape_date
    frame["index_base_period"] = BASE_PERIOD
    frame["conformed_at"] = datetime.now(timezone.utc).isoformat()

    unpriced = frame[frame["rent_psf_cad"].isna()]
    if not unpriced.empty:
        # Silver is the layer that is allowed to refuse. A null rate here is
        # not a class that earns nothing - it is a class nothing could price,
        # and it would reach a cap rate as a silently missing income term.
        raise Failure(
            f"{neighborhood} {scrape_date}: no rent could be resolved for "
            f"{', '.join(unpriced['rent_class'])}. The MarketBeat submarket "
            "table or the index may not have landed for this date."
        )

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    clear_parquet(output_dir)
    path = write_frame(frame, join(output_dir, COMMERCIAL_RENTS_FILE))

    try:
        loaded = publish(
            postgis.connect,
            {"commercial_rents": frame},
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.commercial_rents could not be "
            f"updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    by_class = frame.set_index("rent_class")
    context.log.info(
        "%s %s: %s -> %s",
        neighborhood,
        scrape_date,
        wanted or "no submarket (island-wide)",
        ", ".join(
            f"{name} ${by_class.loc[name, 'rent_psf_cad']:.2f} "
            f"({by_class.loc[name, 'rent_basis']})"
            for name in RENT_CLASSES
        ),
    )
    return MaterializeResult(
        metadata={
            "dagster/row_count": len(frame),
            "submarket": wanted or "none (island-wide)",
            "num_submarket_rates": int(frame["is_submarket_rate"].sum()),
            **{
                f"{name}_rent_psf_cad": round(
                    float(by_class.loc[name, "rent_psf_cad"]), 2
                )
                for name in RENT_CLASSES
            },
            **{
                f"{name}_rent_basis": str(by_class.loc[name, "rent_basis"])
                for name in RENT_CLASSES
            },
            "index_period": str(frame["index_period"].max()),
            "retail_base_gross_rent_psf_cad": config.retail_base_gross_rent_psf_cad,
            "retail_base_period": config.retail_base_period,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


def _measured_rate(
    submarkets: pd.DataFrame,
    index: pd.DataFrame,
    *,
    rent_class: str,
    neighborhood: str,
    wanted: str | None,
) -> dict:
    """One class's rate, off the borough's submarket and moved to the index.

    The submarket is matched with any leading ``Montréal`` taken off, because
    the office report writes `Midtown North` and the industrial one writes
    `Montréal Midtown North` for the same ground. A borough with no submarket
    mapped, or one whose submarket this sector does not publish, falls back to
    the whole-market row - reported as `is_submarket_rate = False` rather than
    passed off as a local figure.
    """
    sector = SECTOR_FOR_CLASS[rent_class]
    rows = submarkets[submarkets["sector"] == sector]
    if rows.empty:
        return _empty_rate(rent_class, note=f"no {sector} report in the snapshot")

    matched = pd.DataFrame()
    if wanted:
        keys = rows["submarket"].map(_submarket_key)
        matched = rows[keys == _submarket_key(wanted)]
    is_submarket = not matched.empty
    row = matched.iloc[0] if is_submarket else market_total(rows)

    period = str(row["report_period_start"])[:7]
    level, index_period, basis = escalate(
        float(row["gross_rent_psf_cad"]),
        index,
        building_type=RENT_CLASSES[rent_class],
        from_period=period,
    )
    return {
        "rent_class": rent_class,
        "rent_psf_cad": round(level, 4),
        "published_rent_psf_cad": float(row["gross_rent_psf_cad"]),
        "published_net_rent_psf_cad": _optional(row.get("net_rent_psf_cad")),
        "published_additional_rent_psf_cad": _optional(
            row.get("additional_rent_psf_cad")
        ),
        "submarket": str(row["submarket"]),
        "is_submarket_rate": is_submarket,
        "source": "cushman_wakefield_marketbeat",
        "source_period": str(row["report_period"]),
        "source_url": str(row.get("source_url") or ""),
        "index_building_type": RENT_CLASSES[rent_class],
        "index_period": index_period,
        "rent_basis": basis,
        "note": ""
        if is_submarket
        else f"no MarketBeat submarket mapped for {neighborhood}",
    }


def _stated_rate(index: pd.DataFrame, config: RentsConfig) -> dict:
    """The retail rate: a stated base, carried forward by the retail index.

    `rent_basis` comes back ``stated`` when the base period is already the
    index's latest quarter and ``escalated`` otherwise, so the column never
    claims a survey stands behind this number - because none does. See
    `RentsConfig`.
    """
    level, index_period, basis = escalate(
        config.retail_base_gross_rent_psf_cad,
        index,
        building_type="retail",
        from_period=config.retail_base_period,
    )
    return {
        "rent_class": "retail",
        "rent_psf_cad": round(level, 4),
        "published_rent_psf_cad": config.retail_base_gross_rent_psf_cad,
        "published_net_rent_psf_cad": None,
        "published_additional_rent_psf_cad": None,
        "submarket": None,
        "is_submarket_rate": False,
        "source": "stated_base",
        "source_period": config.retail_base_period,
        "source_url": "",
        "index_building_type": "retail",
        "index_period": index_period,
        # `measured` would be a lie for a base nobody surveyed, so the one
        # basis `escalate` cannot return for this row is renamed on the way
        # out. Escalated and unescalated stay as they are: both say the number
        # moved, or did not, and neither claims it was measured.
        "rent_basis": "stated" if basis == "measured" else basis,
        "note": "no free Montreal retail survey publishes a level",
    }


def _empty_rate(rent_class: str, *, note: str) -> dict:
    """A rate that could not be resolved. Fails the partition - see the asset."""
    return {
        "rent_class": rent_class,
        "rent_psf_cad": None,
        "published_rent_psf_cad": None,
        "published_net_rent_psf_cad": None,
        "published_additional_rent_psf_cad": None,
        "submarket": None,
        "is_submarket_rate": False,
        "source": "",
        "source_period": "",
        "source_url": "",
        "index_building_type": RENT_CLASSES[rent_class],
        "index_period": "",
        "rent_basis": "unresolved",
        "note": note,
    }


def _submarket_key(name) -> str:
    """A submarket label as the two sectors can be compared on.

    The industrial report prefixes the city and the office one does not, and
    both use accents the crosswalk should not have to reproduce exactly. The
    same idea as `role_assets.lot_key`: normalise only what stands between two
    spellings of one thing.
    """
    text = str(name or "").strip().lower()
    for accented, plain in (("é", "e"), ("è", "e"), ("ô", "o"), ("î", "i")):
        text = text.replace(accented, plain)
    for prefix in ("montreal ", "montreal-"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
    return " ".join(text.split())


def _optional(value):
    """A pandas cell as a plain float, or None where it is missing."""
    if value is None or pd.isna(value):
        return None
    return float(value)


def _read(
    partition_dir: str, name: str, *, asset_name: str, partition: str
) -> pd.DataFrame:
    """One upstream partition's parquet, or a `Failure` naming what to run."""
    path = join(partition_dir, name)
    if not filesystem(path).exists(path):
        raise Failure(
            f"{path} is missing - materialize {asset_name} for {partition} first."
        )
    return pd.read_parquet(path, storage_options=storage_options(path))
