"""What each lot yields, and which lots the roll says are like it.

One silver asset over four upstreams, sitting between `lot_assessed_values`
and `lot_profiles`. `lot_assessed_values` answers *what is the property on this
lot assessed at* and stops there, because a total is what it is for. This asset
is the two questions a reader asks next:

* **Is that a lot of money for this lot?** A cap rate answers it - net
  operating income over value - and the roll supplies only the denominator.
  The numerator is built from the dwellings and the floor the roll says stand
  on the parcel, priced against CMHC's borough rents and, for everything that
  is not a dwelling, against `urban_rag.program`'s stated per-square-foot
  rates.

* **What is this lot worth, if not what the roll says?** The ``k`` most similar
  lots in the borough, chosen on use, size, dwellings and ground distance at
  once, and the dollars per dwelling / per square metre of floor / per square
  metre of ground they imply. `estimated_value_cad` is that applied back to the
  subject, and `assessed_to_estimated_ratio` is the two put side by side -
  which is the screen a highest-and-best-use question starts from, because a
  parcel assessed well under what its neighbours imply is either mispriced or
  under-built.

The arithmetic is all in `urban_rag.comparables`, which has no Dagster imports
and is where the judgements are written down. This module is the partition
handling: read four parquet files, place the roll's units on the borough's
lots, hand the frame over, write the answer and publish it.

**The unit-to-lot placement is `role_assets.place_units_on_lots`, not a second
copy of it.** `lot_assessed_values` sums `rl0404a` over exactly the same
(unit, lot) pairs this sums dwellings and floor area over, and two
implementations of that join would be two answers to which lot a property is
on. The pairs are not persisted by that asset - it keeps only the totals - so
they are re-derived here from the same three inputs, by the same function, with
the same `place_unmatched_by_point` choice available.

**The value columns are `lot_assessed_values`', carried and not recomputed.**
`total_assessed_value` and its apportioned twin arrive by a join on lot number
and travel through untouched, so the two silver tables cannot disagree about
what a lot is worth. What this asset computes is only what that one does not
carry. The unit counts *are* recomputed, as a cross-check rather than as an
answer: `num_units_disagreeing` is 0 on a healthy partition and is what a stale
upstream parquet looks like from here.

**Every lot keeps its row.** A lane, a park, a city parcel - the lots
`lot_assessed_values` gives a null total - are subjects of the comparable
search even though they can never be a comparable *for* anything, and that
asymmetry is the point: it is what values a vacant parcel off the built ones
around it. Their income and cap rate are null rather than zero, and their
estimated value comes from the ground-area basis, which is the only one a
parcel carrying nothing can be valued on.

**The pool is the borough.** `lot_assessed_values` is partitioned by borough,
so the valued lots available to be comparables are the ones in this partition
and a parcel on the boundary draws its neighbours from its own side of it.
`num_candidates` is reported per run so a thin pool is visible rather than
inferred.

**Two rates, because the denominator is a choice.** `cap_rate_pct` is the yield
on the roll's own assessed value, scaled by `market_value_factor` if the reader
sets one; `comparable_cap_rate_pct` is the same income over what the
comparables say the lot is worth. They differ by exactly
`assessed_to_estimated_ratio`, and a lot where they differ a lot is a lot whose
assessment and whose neighbourhood are telling different stories.

**It owns `silver.lot_assessment_comparables`** (hbu_infra's sql/016), upserted
on (scrape_date, neighborhood, lot_number) like every other borough-scoped
silver table, and the parquet is written first - so a database that is down
costs a re-run of the load rather than of the join and the neighbour search.
`lot_profiles` joins the table on `lot_number`, the same way it joins
`lot_assessed_values`.
"""

import json
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    asset,
)
from pydantic import Field

from urban_rag.cmhc_assets import (
    AVERAGE_RENTS_FILE,
    VACANCY_FILE,
    average_rents,
    vacancy_rates,
)
from urban_rag.comparables import (
    METRIC_CRS,
    ComparableWeights,
    IncomeAssumptions,
    aggregate_units_by_lot,
    annual_income,
    cap_rate_pct,
    estimate_value,
    nearest_comparables,
    summarise_comparables,
)
from urban_rag.frames import write_frame
from urban_rag.layers import key_prefix
from urban_rag.partitions import scrape_partitions
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.role_assets import (
    ASSESSMENT_UNITS_FILE,
    CADASTRE_FILE,
    LOT_NUMBER_COLUMN,
    LOT_VALUES_FILE,
    SILVER_GROUP,
    assessment_units,
    lot_assessed_values,
    lot_key,
    place_units_on_lots,
    property_assessment_roll,
)
from urban_rag.role_foncier import JOIN_KEY
from urban_rag.storage import clear_parquet, filesystem, join, storage_options
from urban_rag.warehouse import MissingRelation, publish, published_metadata

#: The one file a partition writes, under
#: `silver/lot_assessment_comparables/<YYYY-MM-DD>/<neighborhood>/`.
LOT_COMPARABLES_FILE = "lot_assessment_comparables.parquet"

#: The CMHC cell every reader means by "the rent here" / "the vacancy rate
#: here". The same pair `lot_profiles` flattens out of its own jsonb, named
#: here because this is where the two numbers become an income.
_OVERALL_BEDROOM = "all"
_OVERALL_DWELLING = "all"


class ComparablesConfig(Config):
    """The judgements this asset makes, all of them recorded on every row.

    Three groups, and none of them is a property of the data.

    **What counts as a comparable.** ``k`` is how many neighbours a lot is
    given; 8 is the size an appraisal actually reasons over, and a larger one
    buys a steadier median at the price of reaching further for it.
    ``max_distance_m`` is the radius past which a lot stops being a comparable
    however similar it looks - 2 km is most of a borough. ``distance_scale_m``
    and ``size_ratio_scale`` are what one *unit* of unlikeness is: 500 m of
    ground, and a factor of two in size. ``use_weight`` is how much the CUBF
    class counts against those, and leads at 1.5 because a comparable of the
    wrong kind is not a comparable at any distance. The rest of
    `ComparableWeights` - the two class penalties, the missing-feature penalty
    and the three remaining weights - stay at their defaults, which are
    documented there.

    **What the income is.** ``operating_expense_ratio`` is the share of gross
    income that never reaches the owner: taxes, insurance, management,
    maintenance. 0.35 is conventional for a Montreal walk-up and is the single
    largest lever on every cap rate this asset produces. Vacancy is *not* in
    it - that is netted per class from the surveyed rate - so the two are never
    applied to each other twice.

    **What the value is.** ``market_value_factor`` scales the assessed value
    before it becomes the cap rate's denominator. 1.0 reports the yield on the
    roll and is the honest default, because Quebec's *facteur comparatif* -
    the published ratio between a roll figure and a market one - is not in this
    publication. A reader who knows the year's factor sets it here and gets a
    market cap rate on every row.

    ``place_unmatched_by_point`` mirrors `LotValuesConfig`'s field of the same
    name and must be set the same way, for the same reason: it decides which
    units reach a lot at all. Left different from the run that produced this
    partition's `lot_assessed_values`, the characteristics here would be summed
    over a different set of units than the totals there.
    """

    k: int = Field(
        default=8,
        ge=1,
        description="How many comparable lots each lot is given.",
    )
    max_distance_m: float = Field(
        default=2000.0,
        gt=0,
        description="Ground radius past which a lot is not a comparable.",
    )
    distance_scale_m: float = Field(
        default=500.0,
        gt=0,
        description="Ground distance worth one unit of unlikeness.",
    )
    size_ratio_scale: float = Field(
        default=2.0,
        gt=1.0,
        description="Size factor worth one unit of unlikeness, e.g. 2.0.",
    )
    use_weight: float = Field(
        default=1.5,
        ge=0,
        description="How much the CUBF use code counts against the rest.",
    )
    operating_expense_ratio: float = Field(
        default=0.35,
        ge=0.0,
        lt=1.0,
        description=(
            "Share of gross income that never reaches the owner. Vacancy is "
            "netted separately and is not in this."
        ),
    )
    market_value_factor: float = Field(
        default=1.0,
        gt=0,
        description=(
            "Scales the assessed value into the cap rate's denominator. 1.0 "
            "reports the yield on the roll."
        ),
    )
    place_unmatched_by_point: bool = Field(
        default=True,
        description=(
            "Place units the lot-number crosswalk cannot resolve by where "
            "their point falls. Set it as lot_assessed_values was run."
        ),
    )

    def weights(self) -> ComparableWeights:
        """The metric this run scores neighbours with."""
        return ComparableWeights(
            distance_scale_m=self.distance_scale_m,
            size_ratio_scale=self.size_ratio_scale,
            use_weight=self.use_weight,
        )


@asset(
    key_prefix=key_prefix("lot_assessment_comparables"),
    partitions_def=scrape_partitions,
    deps=[
        # Partitioned by (neighborhood, date) like this asset, so neither needs
        # a mapping. The first is where the geometry and the two totals come
        # from; the two CMHC assets are the borough's rent and vacancy, which
        # is the whole of the measured half of the income.
        lot_assessed_values,
        vacancy_rates,
        average_rents,
        # Partitioned by date alone: the roll is one publication for the
        # province. Mapped onto this asset's `date` dimension the same way
        # `lot_assessed_values` maps the same two.
        *(
            AssetDep(
                upstream,
                partition_mapping=MultiToSingleDimensionPartitionMapping(
                    partition_dimension_name="date"
                ),
            )
            for upstream in (assessment_units, property_assessment_roll)
        ),
    ],
    group_name=SILVER_GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "What every lot in one borough yields, and which lots are like it. "
        "One row per NO_LOT with the roll's characteristics summed over the "
        "units standing on it - dwellings, floor area split into residential, "
        "commercial and industrial by each unit's own CUBF use code, and the "
        "use code, year and storeys of the unit carrying most of the value. "
        "Priced against CMHC's borough rent and vacancy for the dwellings and "
        "urban_rag.program's stated per-square-foot rates for the rest, that "
        "gives gross_income_cad and net_operating_income_cad, and over the "
        "assessed value cap_rate_pct. comparables is the k most similar lots "
        "in the borough - scored on use code, lot area, floor area, dwellings "
        "and ground distance at once - with the dollars per dwelling and per "
        "square metre they imply flattened beside it; estimated_value_cad is "
        "that applied back to this lot and assessed_to_estimated_ratio is the "
        "two side by side. A lot no assessment unit stands on keeps its row "
        "with null income and a value estimated from ground area alone. "
        "Written to silver/lot_assessment_comparables/<YYYY-MM-DD>/"
        f"<neighborhood>/{LOT_COMPARABLES_FILE} and upserted into "
        "silver.lot_assessment_comparables on (scrape_date, neighborhood, "
        "lot_number)."
    ),
)
def lot_assessment_comparables(
    context: AssetExecutionContext,
    config: ComparablesConfig,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    lots = _read_geoparquet(
        store.partition_dir(
            lot_assessed_values.key.path[-1], scrape_date, neighborhood
        ),
        LOT_VALUES_FILE,
        asset_name=lot_assessed_values.key.path[-1],
        partition=f"{neighborhood} {scrape_date}",
    )
    units = _read_geoparquet(
        store.partition_dir(assessment_units.key.path[-1], scrape_date),
        ASSESSMENT_UNITS_FILE,
        asset_name=assessment_units.key.path[-1],
        partition=scrape_date,
    )
    crosswalk = _read_parquet(
        store.partition_dir(property_assessment_roll.key.path[-1], scrape_date),
        CADASTRE_FILE,
        asset_name=property_assessment_roll.key.path[-1],
        partition=scrape_date,
    )
    if lots.empty:
        raise Failure(
            f"{lot_assessed_values.key.path[-1]} holds no lot for "
            f"{neighborhood} {scrape_date}; there is nothing to compare."
        )
    if LOT_NUMBER_COLUMN not in lots.columns:
        raise Failure(
            f"The {lot_assessed_values.key.path[-1]} partition for "
            f"{neighborhood} {scrape_date} has no {LOT_NUMBER_COLUMN} column - "
            "it was not written by that asset."
        )

    assumptions = _income_assumptions(
        context, store, neighborhood=neighborhood, scrape_date=scrape_date,
        config=config,
    )

    # The same placement `lot_assessed_values` totalled over, re-derived
    # because that asset keeps the totals and not the pairs. `lot_key` is
    # added here for the same reason it is added there: it is the only thing
    # standing between the roll's "1243415" and Infolot's "1 243 415".
    keyed = lots.assign(lot_key=lots[LOT_NUMBER_COLUMN].map(lot_key))
    pairs = place_units_on_lots(
        crosswalk,
        units.to_crs(keyed.crs) if units.crs != keyed.crs else units,
        keyed,
        place_unmatched_by_point=config.place_unmatched_by_point,
    )
    characteristics = aggregate_units_by_lot(
        pairs, units, lot_column=LOT_NUMBER_COLUMN, join_key=JOIN_KEY
    )

    frame = _assemble(lots, characteristics)
    subjects = _subject_frame(frame)
    neighbours = nearest_comparables(
        subjects,
        k=config.k,
        weights=config.weights(),
        max_distance_m=config.max_distance_m,
    )
    frame = pd.concat(
        [frame, summarise_comparables(neighbours, index=frame.index)], axis=1
    )
    frame = pd.concat([frame, estimate_value(frame)], axis=1)
    frame = _with_income(frame, assumptions, config)
    # `comparables` and `income_assumptions` go into the parquet as JSON
    # *strings* rather than as nested objects, the same posture
    # `lot_zoning_envelopes` takes with `usages` and `levels`: Arrow would
    # otherwise have to infer a schema for a list of dicts whose keys differ
    # between a valued and an unvalued lot, and a string keeps the file
    # readable by anything. `urban_rag.warehouse._as_text` writes them straight
    # into a jsonb column, where Postgres parses them back.
    #
    # The metric travels *with* the neighbour list rather than beside it,
    # because a neighbour list means nothing without the weights that chose it -
    # the rule every stated assumption in this platform follows.
    frame["comparables"] = [
        json.dumps(
            {
                "k": config.k,
                "max_distance_m": config.max_distance_m,
                "num_candidates": int(subjects["total_assessed_value"].notna().sum()),
                **config.weights().as_metadata(),
                "neighbors": entries,
            },
            ensure_ascii=False,
        )
        for entries in neighbours
    ]
    frame["income_assumptions"] = json.dumps(
        assumptions.as_metadata(), ensure_ascii=False
    )
    # Overwritten rather than trusted, the posture `lot_assessed_values` takes
    # with the same two: this partition's borough and date are the ones that
    # were asked for, not whatever a stale upstream file carried.
    frame["neighborhood"] = neighborhood
    frame["scrape_date"] = scrape_date
    frame["computed_at"] = datetime.now(timezone.utc).isoformat()

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(frame, join(output_dir, LOT_COMPARABLES_FILE))

    # After the parquet, for the reason every silver asset here writes first:
    # the file is the record, and a database that is down should cost a re-run
    # of the load rather than of the borough-wide neighbour search.
    try:
        loaded = publish(
            postgis.connect,
            {"lot_assessment_comparables": frame},
            neighborhood=neighborhood,
            scrape_date=scrape_date,
        )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.lot_assessment_comparables could "
            f"not be updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    return _result(
        context, frame, path, assumptions, config, pairs=pairs, loaded=loaded
    )


def _assemble(
    lots: gpd.GeoDataFrame, characteristics: pd.DataFrame
) -> gpd.GeoDataFrame:
    """The lot frame, plus its geometry measured and the roll summed onto it.

    ``lot_area_m2`` and the centroid come from the *polygon*, projected to
    `METRIC_CRS`, and not from the roll's own `rl0302a`. Two reasons, and the
    second is why it matters: the polygon has an area for every lot including
    the ones no assessment unit stands on, which is what lets a vacant parcel
    be valued off the ground its neighbours sit on - and a divided
    co-ownership states the whole parcel's superficie on every one of its
    apartments, so summing the roll's column over a 402-unit tower measures the
    same ground four hundred times. `roll_land_area_m2` is carried anyway,
    because on an ordinary single-unit lot the two agreeing is worth being able
    to check.
    """
    projected = lots.to_crs(METRIC_CRS)
    centroids = projected.geometry.centroid
    frame = lots.copy()
    frame["lot_area_m2"] = projected.geometry.area.astype("float64")
    frame["x_m"] = centroids.x.astype("float64")
    frame["y_m"] = centroids.y.astype("float64")

    joined = frame.join(
        characteristics.rename(
            columns={"num_assessment_units": "num_units_recomputed"}
        ),
        on=LOT_NUMBER_COLUMN,
    )
    # A lot the roll never reached has no characteristics row and comes back
    # all-null from the join, which is the right answer for every measure on
    # it - but not for a count, where "no unit stands here" is a measurement.
    joined["num_units_recomputed"] = (
        joined["num_units_recomputed"].fillna(0).astype("int64")
    )
    return joined


def _subject_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """The six columns the neighbour metric reads, under the names it wants.

    Narrowed rather than passed whole, so `nearest_comparables` cannot quietly
    start depending on a column this asset happens to carry: its contract is
    the six features and the value, and that contract is stated here.
    """
    return pd.DataFrame(
        {
            "lot_number": frame[LOT_NUMBER_COLUMN],
            "x_m": frame["x_m"],
            "y_m": frame["y_m"],
            "lot_area_m2": frame["lot_area_m2"],
            "floor_area_m2": frame.get("floor_area_m2"),
            "num_dwellings": frame.get("num_dwellings"),
            "num_assessment_units": frame["num_units_recomputed"],
            "use_code": frame.get("dominant_use_code"),
            "total_assessed_value": frame.get("total_assessed_value"),
        }
    )


def _with_income(
    frame: pd.DataFrame, assumptions: IncomeAssumptions, config: ComparablesConfig
) -> pd.DataFrame:
    """The five income columns, the two cap rates, and the ratio between them.

    `comparable_cap_rate_pct` takes the estimate at face value and passes
    ``market_value_factor=1.0``: the factor exists to carry a *roll* figure to
    a market one, and the comparables are already quoted off the same roll -
    applying it to both would scale the numerator's denominator twice and leave
    the two rates differing by something other than the ratio below.
    """
    income = annual_income(frame, assumptions)
    frame = pd.concat([frame, income], axis=1)
    frame["cap_rate_pct"] = cap_rate_pct(
        income["net_operating_income_cad"],
        frame["total_assessed_value"],
        market_value_factor=config.market_value_factor,
    )
    frame["comparable_cap_rate_pct"] = cap_rate_pct(
        income["net_operating_income_cad"], frame["estimated_value_cad"]
    )
    assessed = pd.to_numeric(frame["total_assessed_value"], errors="coerce")
    estimated = pd.to_numeric(frame["estimated_value_cad"], errors="coerce")
    # The screen this asset exists for: under 1 is a parcel the roll values
    # below what its neighbours imply, which is either a mispricing or a lot
    # doing less with its ground than the ones around it. Null where either
    # side is missing, and where the estimate is zero - a ratio against nothing
    # is not a low ratio.
    frame["assessed_to_estimated_ratio"] = assessed / estimated.where(estimated > 0)
    return frame


def _income_assumptions(
    context: AssetExecutionContext,
    store: ParquetStore,
    *,
    neighborhood: str,
    scrape_date: str,
    config: ComparablesConfig,
) -> IncomeAssumptions:
    """CMHC's two measured figures, and this run's five stated ones.

    The rent and the vacancy are the borough's all-dwellings, all-bedrooms
    cells - the ones every reader means - read out of the same two silver
    files `lot_profiles` builds its own jsonb from, so the cap rate here and
    the `overall_average_rent_cad` there cannot come from different surveys.

    A suppressed cell reaches `IncomeAssumptions` as None and is logged rather
    than raised on: CMHC publishing nothing for a borough is a fact about the
    survey, and a partition that failed over it would be a partition with no
    comparables either - which are computable without a rent and are half of
    what this asset is for.
    """
    rents = _read_parquet(
        store.partition_dir(average_rents.key.path[-1], scrape_date, neighborhood),
        AVERAGE_RENTS_FILE,
        asset_name=average_rents.key.path[-1],
        partition=f"{neighborhood} {scrape_date}",
    )
    vacancy = _read_parquet(
        store.partition_dir(vacancy_rates.key.path[-1], scrape_date, neighborhood),
        VACANCY_FILE,
        asset_name=vacancy_rates.key.path[-1],
        partition=f"{neighborhood} {scrape_date}",
    )
    rent = _overall(
        rents, "average_rent_cad", {"bedroom_type": _OVERALL_BEDROOM}
    )
    rate = _overall(
        vacancy,
        "vacancy_rate_pct",
        {"dwelling_type": _OVERALL_DWELLING, "bedroom_type": _OVERALL_BEDROOM},
    )
    if rent is None:
        context.log.warning(
            "%s %s: CMHC published no overall rent for this borough - every "
            "residential income and both cap rates will be null on lots with "
            "no commercial or industrial floor",
            neighborhood,
            scrape_date,
        )
    return IncomeAssumptions(
        average_rent_cad=rent,
        vacancy_rate_pct=rate,
        operating_expense_ratio=config.operating_expense_ratio,
        market_value_factor=config.market_value_factor,
        survey_year=_first(rents, "survey_year"),
        survey_period=_first(rents, "survey_period"),
    )


def _overall(frame: pd.DataFrame, column: str, where: dict[str, str]) -> float | None:
    """One cell of a CMHC grid, or None where it was suppressed or is absent."""
    if frame.empty or column not in frame.columns:
        return None
    mask = pd.Series(True, index=frame.index)
    for name, value in where.items():
        if name not in frame.columns:
            return None
        mask &= frame[name] == value
    cells = pd.to_numeric(frame.loc[mask, column], errors="coerce").dropna()
    return float(cells.iloc[0]) if len(cells) else None


def _first(frame: pd.DataFrame, column: str):
    """The partition-wide value of a column identical on every row."""
    if frame.empty or column not in frame.columns:
        return None
    values = frame[column].dropna()
    if not len(values):
        return None
    value = values.iloc[0]
    return value.item() if hasattr(value, "item") else value


def _result(
    context: AssetExecutionContext,
    frame: pd.DataFrame,
    path: str,
    assumptions: IncomeAssumptions,
    config: ComparablesConfig,
    *,
    pairs: pd.DataFrame,
    loaded: dict,
) -> MaterializeResult:
    """The run's log line and its metadata, off the frame that was written."""
    num_lots = len(frame)
    with_comparables = int((frame["num_comparables"] > 0).sum())
    with_cap_rate = int(frame["cap_rate_pct"].notna().sum())
    with_estimate = int(frame["estimated_value_cad"].notna().sum())
    # The cross-check the module docstring describes: this asset re-derives
    # the placement and must land on the same unit counts the totals it carries
    # were summed over. Anything but 0 is a stale parquet on one side or a
    # `place_unmatched_by_point` set differently from the run that produced it.
    disagreeing = int(
        (
            pd.to_numeric(frame["num_assessment_units"], errors="coerce").fillna(0)
            != frame["num_units_recomputed"]
        ).sum()
    )
    if disagreeing:
        context.log.warning(
            "%d lot(s) carry a different unit count than this run placed - "
            "re-materialize lot_assessed_values for this partition, and check "
            "that place_unmatched_by_point matches the run that produced it",
            disagreeing,
        )

    noi = pd.to_numeric(frame["net_operating_income_cad"], errors="coerce")
    rates = pd.to_numeric(frame["cap_rate_pct"], errors="coerce").dropna()
    context.log.info(
        "%s %s: %d lot(s) - %d with a comparable set, %d with a cap rate "
        "(median %.2f pct), %d with an estimated value; $%.1fM of net "
        "operating income across the borough -> %s",
        frame["neighborhood"].iloc[0],
        frame["scrape_date"].iloc[0],
        num_lots,
        with_comparables,
        with_cap_rate,
        float(rates.median()) if len(rates) else float("nan"),
        with_estimate,
        float(noi.sum(skipna=True)) / 1e6,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": num_lots,
            "num_lots": num_lots,
            # The pool every neighbour list was drawn from. A lot only gets to
            # be a comparable if the roll gave it a value, so this is well
            # under num_lots and a thin one is what a partition whose roll did
            # not land looks like from here.
            "num_candidates": int(frame["total_assessed_value"].notna().sum()),
            "num_with_comparables": with_comparables,
            # A lot with none is one with nothing inside max_distance_m. In a
            # borough that is a handful on the edge of an industrial strip; a
            # large number means the radius is tighter than the parcels are
            # spread.
            "num_without_comparables": num_lots - with_comparables,
            "mean_comparables_per_lot": round(
                float(frame["num_comparables"].mean()), 2
            ),
            "num_units_placed": int(pairs[JOIN_KEY].nunique()),
            "num_units_disagreeing": disagreeing,
            # What the roll says stands on the borough, which is the whole of
            # the income's measured side.
            "num_dwellings": _total(frame, "num_dwellings"),
            "num_rental_rooms": _total(frame, "num_rental_rooms"),
            "num_nonresidential_units": _total(frame, "num_nonresidential_units"),
            **{
                f"{name}_floor_area_ha": round(
                    (_float_total(frame, f"{name}_floor_area_m2") or 0.0) / 10_000, 2
                )
                for name in ("residential", "commercial", "industrial")
            },
            # Floor the CUBF could not be placed for: a blank `rl0105a` or a
            # code outside the manual's eight classes. It earns nothing here,
            # so this is the number that says how much income the classifier
            # left on the table.
            "unclassified_floor_area_ha": round(
                max(
                    (_float_total(frame, "floor_area_m2") or 0.0)
                    - sum(
                        _float_total(frame, f"{name}_floor_area_m2") or 0.0
                        for name in ("residential", "commercial", "industrial")
                    ),
                    0.0,
                )
                / 10_000,
                2,
            ),
            "num_lots_with_unknown_use": int(
                (frame["dominant_use_class"] == "unknown").sum()
            )
            if "dominant_use_class" in frame.columns
            else 0,
            "gross_income_millions": _millions(frame, "gross_income_cad"),
            "net_operating_income_millions": _millions(
                frame, "net_operating_income_cad"
            ),
            "num_with_cap_rate": with_cap_rate,
            # Null on a lot with no income *or* no value, and both are ordinary:
            # a lane has neither, a vacant lot has a value and no income.
            "num_without_cap_rate": num_lots - with_cap_rate,
            # Median rather than mean, and for the reason the comparables take
            # medians: one $258M common-parts lot would otherwise decide it.
            "median_cap_rate_pct": _rounded(rates.median() if len(rates) else None),
            "p10_cap_rate_pct": _rounded(rates.quantile(0.10) if len(rates) else None),
            "p90_cap_rate_pct": _rounded(rates.quantile(0.90) if len(rates) else None),
            "num_with_estimated_value": with_estimate,
            **{
                f"num_estimated_{basis}": int(
                    (frame["estimated_value_basis"] == basis).sum()
                )
                for basis in ("per_dwelling", "per_floor_area", "per_land_area", "none")
            },
            "estimated_value_billions": round(
                float(
                    pd.to_numeric(
                        frame["estimated_value_cad"], errors="coerce"
                    ).sum(skipna=True)
                )
                / 1e9,
                2,
            ),
            # The screen, as one number for the partition. Well under 1 means
            # the borough's roll sits below what its own comparables imply,
            # which for a triennial roll in a rising market is what it should
            # look like.
            "median_assessed_to_estimated_ratio": _rounded(
                pd.to_numeric(frame["assessed_to_estimated_ratio"], errors="coerce")
                .dropna()
                .median(),
                digits=3,
            ),
            # The borough's measured inputs, reported once rather than left to
            # be read out of one lot's jsonb. "suppressed" is CMHC publishing
            # nothing, which is a fact about the survey and not a gap here.
            "cmhc_survey_year": assumptions.survey_year or "unknown",
            "average_rent_cad": assumptions.average_rent_cad or "suppressed",
            "vacancy_rate_pct": (
                assumptions.vacancy_rate_pct
                if assumptions.vacancy_rate_pct is not None
                else "suppressed"
            ),
            # And this run's stated ones. Every row carries them too, in
            # income_assumptions and comparables; they are here so a run can be
            # read at a glance against the one before it.
            "operating_expense_ratio": config.operating_expense_ratio,
            "market_value_factor": config.market_value_factor,
            "k": config.k,
            "max_distance_m": config.max_distance_m,
            "distance_scale_m": config.distance_scale_m,
            "size_ratio_scale": config.size_ratio_scale,
            "use_weight": config.use_weight,
            "placed_unmatched_by_point": config.place_unmatched_by_point,
            "roll_year": _first(frame, "roll_year") or "unknown",
            "output_path": MetadataValue.path(str(path)),
            **published_metadata(loaded),
        }
    )


def _total(frame: pd.DataFrame, column: str) -> int:
    value = _float_total(frame, column)
    return int(value) if value is not None else 0


def _float_total(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns:
        return None
    return float(pd.to_numeric(frame[column], errors="coerce").sum(skipna=True))


def _millions(frame: pd.DataFrame, column: str) -> float:
    return round((_float_total(frame, column) or 0.0) / 1e6, 2)


def _rounded(value, *, digits: int = 2):
    if value is None or pd.isna(value):
        return "not computed"
    return round(float(value), digits)


def _read_geoparquet(
    partition_dir: str, name: str, *, asset_name: str, partition: str
) -> gpd.GeoDataFrame:
    path = _require(partition_dir, name, asset_name=asset_name, partition=partition)
    return gpd.read_parquet(path, storage_options=storage_options(path))


def _read_parquet(
    partition_dir: str, name: str, *, asset_name: str, partition: str
) -> pd.DataFrame:
    path = _require(partition_dir, name, asset_name=asset_name, partition=partition)
    return pd.read_parquet(path, storage_options=storage_options(path))


def _require(
    partition_dir: str, name: str, *, asset_name: str, partition: str
) -> str:
    """The upstream file, or a `Failure` naming the asset to materialize.

    All five inputs are declared deps, so a missing file means the partition
    was never materialized rather than that this asset is reaching for
    something optional - and the message that helps says which asset to run,
    the posture every reader in this platform takes.
    """
    path = join(partition_dir, name)
    if not filesystem(path).exists(path):
        raise Failure(
            f"{path} is missing - materialize {asset_name} for {partition} first."
        )
    return path
