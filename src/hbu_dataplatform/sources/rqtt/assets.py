"""The street network at this platform's grain: the street sides one tile
of the cut owns, kept whole, measured in metres.

`street_network` snapshots the RQTT - Quebec's province-wide road network -
bounded to the cities this pipeline has boroughs in, because that is how the
MRNF publishes it: one file for the province, with no borough column and no
municipality column either. Everything downstream of it runs on the tile
axis, so this is where that file is cut into partitions, the same hinge
`neighborhood_buildings` turns on for BDOI and `neighborhood_lots` for Infolot.
The difference is that those two cut in *bronze*, at the query, because the
source is fetched per borough; this one cuts in silver, from a file already on
disk, because one download serves every tile and re-fetching 390 MB per
partition would be work done for nothing.

One source for every city, where there were three. Montreal's geobase double,
Quebec City's `vque_18` and Saguenay's `sag-reseau-routier` each had their own
id and name columns and their own branch here, and the dispatch that chose
between them is gone: the RQTT has one schema, so `_as_street_sides` renames
one pair of columns for every city and a fourth city needs no street code at
all.

**Selected, not clipped.** A side belongs to the tile its *midpoint* falls in
- `interpolate(0.5)` along the line, taken to a zoom-19 quadkey - and it is
stored whole, wherever its ends reach. The cell is a write-ownership claim and
nothing more: no geometry is cut at a cell edge, and every reader downstream
takes the snapshot's sides regardless of tile, so a lot on the edge of this
tile measures its frontage on the whole side across the line. That is what
this replaced. The borough clip that stood here `ST_Intersection`-ed every
side against the borough outline, and a lot straddling the line - which the
cadastre query reaches past to include whole - measured its frontage on the
surviving piece only, and was under-reported for it. There is no line to
clip at any more, so there is no piece to under-report against.

`neighborhood` survives as an attribute, because the map and its aggregates
still read one: the enabled borough whose outline holds the midpoint, or NULL
when none does - a side out on the island beyond every registered borough
is still a side of this tile, and a borough whose outline cannot be read is
skipped rather than fatal. It bounds nothing. A side on the line between two
boroughs is claimed by whichever comes first in the registered order, which
is arbitrary and harmless: nothing is measured by it.

Lengths are computed in the tile's city's MTM zone - EPSG:32188 (NAD83 / MTM
zone 8) for a Montreal cell - not in the 4326 the geometry is stored and
written in: a degree is not a metre, and `GeoSeries.length` on lon/lat would
report a number in degrees that reads like one in metres. PostGIS gets the
same answer downstream through `geography`; GeoPandas has no such type, so the
projection is explicit here. A tile has exactly one city by construction
(`hbu_dataplatform.core.tile_cut`), which is what lets it answer for the zone.
"""

import geopandas as gpd
import pandas as pd
import shapely
from dagster import (
    AssetDep,
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    MultiToSingleDimensionPartitionMapping,
    asset,
)

from hbu_dataplatform.core import tile_grid
from hbu_dataplatform.core.frames import count_invalid_geometries, write_frame
from hbu_dataplatform.core.layers import key_prefix
from hbu_dataplatform.boundaries.assets import (
    STREET_SEGMENTS_FILE,
    STREET_ID_COLUMN,
    STREET_NAME_COLUMN,
    borough_boundary,
    reference_neighborhoods,
    street_network,
)
from hbu_dataplatform.partitions.axes import (
    City,
    city_of,
    city_of_tile,
    enabled_neighborhoods,
    metric_crs_for_city,
    tile_partition_of,
    tile_scrape_partitions,
)
from hbu_dataplatform.sources.rqtt.client import STREET_ID_FIELD, STREET_NAME_FIELD
from hbu_dataplatform.core.postgis import load_streets
from hbu_dataplatform.rag.pgvector import PostgresUnavailable
from hbu_dataplatform.core.resources import ParquetStore, PostgisResource
from hbu_dataplatform.core.storage import clear_parquet, join, storage_options
from hbu_dataplatform.core.warehouse import MissingRelation, published_metadata

GROUP = "silver_streets"

#: The one file a partition is written to, under
#: `silver/neighborhood_streets/<YYYY-MM-DD>/<tile>/`.
STREETS_FILE_OUT = "neighborhood_streets.parquet"

#: Where lengths are measured for a Montreal tile. NAD83 / MTM zone 8 is the
#: projected system the island is surveyed in; metres in it are metres on the
#: ground. A Quebec City tile is measured in zone 7 -
#: `partitions.metric_crs_for_city` over the tile's city is what the asset
#: actually consults.
METRIC_CRS = "EPSG:32188"


@asset(
    key_prefix=key_prefix("neighborhood_streets"),
    partitions_def=tile_scrape_partitions,
    deps=[
        AssetDep(
            street_network,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        ),
        AssetDep(
            reference_neighborhoods,
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="date"
            ),
        ),
    ],
    group_name=GROUP,
    kinds={"postgres", "geoparquet"},
    description=(
        "One tile's street segments, selected out of that day's RQTT snapshot "
        "by the cell their midpoint falls in and kept whole - nothing is "
        "clipped at a cell edge. One row per COTE_RUE_ID, valid geometry, "
        "its length in metres, its cell_key, and the borough whose outline "
        "from reference_neighborhoods holds the midpoint (NULL when none "
        "does), as silver/"
        f"neighborhood_streets/<YYYY-MM-DD>/<tile>/{STREETS_FILE_OUT} "
        "and upserted into silver.neighborhood_streets on (scrape_date, "
        "cell_partition, cote_rue_id)."
    ),
)
def neighborhood_streets(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    tile, scrape_date = tile_partition_of(context)

    city = city_of_tile(tile)
    metric_crs = metric_crs_for_city(city)
    streets_path = join(
        store.partition_dir(street_network.key.path[-1], scrape_date),
        STREET_SEGMENTS_FILE,
    )
    streets = _read_streets(streets_path, scrape_date=scrape_date)
    streets = _as_street_sides(streets, STREET_ID_FIELD, STREET_NAME_FIELD)
    if streets.empty:
        raise Failure(f"{streets_path} holds no street side to select from.")
    if STREET_ID_COLUMN not in streets.columns:
        raise Failure(
            f"{streets_path} has no {STREET_ID_COLUMN} column - it was not "
            "written by street_network."
        )

    # Ownership by midpoint, as a string range over the whole snapshot. The
    # cut covers the ground that has lots on it and nothing else - most of
    # the province is outside it - so resolving each side's key to its cut
    # cell would raise on the first rural road. A `[lo, hi)` over the keys
    # asks the only question this run has: is the midpoint under my cell.
    #
    # Half way along the line *in degrees*, on purpose: it is an address, not
    # a measure, and it has to be the same point `ST_LineInterpolatePoint`
    # gives on the 4326 geometry in the table. Called on shapely directly
    # because `GeoSeries.interpolate` warns about the geographic CRS, and the
    # warning is about lengths, which this does not take.
    midpoints = gpd.GeoSeries(
        shapely.line_interpolate_point(streets.geometry.values, 0.5, normalized=True),
        index=streets.index,
        crs=streets.crs,
    )
    cell_keys = pd.Series(
        [tile_grid.quadkey_of(x, y) for x, y in zip(midpoints.x, midpoints.y)],
        index=streets.index,
        dtype=object,
    )
    lo, hi = tile_grid.quadkey_bounds(tile)
    owned = streets[(cell_keys >= lo) & (cell_keys < hi)].copy()
    if owned.empty:
        raise Failure(
            f"No street side has its midpoint in tile {tile}; the cut cell "
            f"holds lots, so the snapshot for {scrape_date} is missing the "
            "ground under them."
        )

    owned["cell_key"] = cell_keys.loc[owned.index]
    owned["segment_length_m"] = _length_m(owned.geometry, metric_crs)
    owned["neighborhood"] = _borough_of(
        context,
        store,
        scrape_date,
        midpoints.loc[owned.index],
        city=city,
    )
    # The path carries bare keys rather than hive `key=value` pairs, so the
    # partition has to travel as columns. `scrape_date` is already one, from
    # bronze, and is overwritten rather than trusted: this partition's date is
    # the one that was asked for.
    owned["cell_partition"] = tile
    owned["scrape_date"] = scrape_date

    _require_unique_streets(owned, tile=tile, scrape_date=scrape_date)
    # Lines cannot self-intersect their way to invalidity the way the polygon
    # layers can, so this is a guard rather than a repair - but silver owes its
    # readers geometry `ST_Intersection` can be run over either way.
    still_invalid = count_invalid_geometries(owned)
    if still_invalid:
        raise Failure(
            f"{still_invalid} street side(s) are invalid; the partition "
            "cannot be joined against."
        )

    output_dir = store.partition_dir(context.asset_key.path[-1], scrape_date, tile)
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(owned, join(output_dir, STREETS_FILE_OUT))

    # Published after the file is written, so a database that is down costs a
    # re-run of the load rather than of the selection. This asset owns
    # `silver.neighborhood_streets`: `lot_frontage` used to load it on its way
    # past, which left a table whose writer was not the asset it is named for.
    try:
        with postgis.connect() as connection:
            published = load_streets(
                connection, owned, tile=tile, scrape_date=scrape_date
            )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.neighborhood_streets could not be "
            f"updated for {tile} {scrape_date}: {exc}"
        ) from exc

    without_borough = int(owned["neighborhood"].isna().sum())
    neighborhoods = sorted(owned["neighborhood"].dropna().unique())
    total_km = float(owned["segment_length_m"].sum()) / 1000.0
    context.log.info(
        "%s %s: %d of %d street side(s) have their midpoint in the tile, "
        "%.1f km (%d in no registered borough) -> %s",
        tile,
        scrape_date,
        len(owned),
        len(streets),
        total_km,
        without_borough,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(owned),
            "tile": tile,
            "num_sides": len(owned),
            "num_segments_in_snapshot": len(streets),
            "num_streets_named": int(owned[STREET_NAME_COLUMN].nunique())
            if STREET_NAME_COLUMN in owned.columns
            else 0,
            "total_length_km": round(total_km, 2),
            # A side out on the island past every registered borough is still
            # this tile's; the count says how much of the tile that is.
            "num_without_borough": without_borough,
            "neighborhoods": ", ".join(neighborhoods),
            "num_invalid_geometries": still_invalid,
            "output_path": MetadataValue.path(str(path)),
            **published_metadata({"neighborhood_streets": published}),
        }
    )


def _read_streets(path: str, *, scrape_date: str) -> gpd.GeoDataFrame:
    try:
        return gpd.read_parquet(path, storage_options=storage_options(path))
    except FileNotFoundError as exc:
        raise Failure(
            f"{path} does not exist - materialize street_network for "
            f"{scrape_date} first."
        ) from exc


def _length_m(geometry: gpd.GeoSeries, metric_crs: str = METRIC_CRS):
    """``geometry``'s length in metres, measured in ``metric_crs``."""
    return geometry.to_crs(metric_crs).length


def _borough_of(
    context: AssetExecutionContext,
    store: ParquetStore,
    scrape_date: str,
    midpoints: gpd.GeoSeries,
    *,
    city: City,
) -> pd.Series:
    """The registered borough whose outline holds each midpoint, else None.

    Walked over the registered boroughs of the tile's own ``city`` - a cell
    has exactly one, so another city's outlines cannot hold its sides and
    are not read - in registered order, first outline to hold a point
    claiming it. An outline that cannot be read - a Quebec City key on a
    snapshot taken before its arrondissements were written, a borough code
    the reference layer does not carry - is skipped and logged, because the
    column is an attribute and not a bound: the tile's sides are the tile's
    whether or not a borough can be named for them. What is not skipped is
    *every* outline failing at once, which is not a borough missing but the
    reference layer missing, and the message says which asset to run.
    """
    resolved = pd.Series([None] * len(midpoints), index=midpoints.index, dtype=object)
    outlines_read = 0
    last_failure: Failure | None = None
    for neighborhood in enabled_neighborhoods(context.instance):
        if city_of(neighborhood) is not city:
            continue
        try:
            outline = borough_boundary(store, scrape_date, neighborhood)
        except Failure as exc:
            last_failure = exc
            context.log.info(
                "%s: no outline, its sides will carry no borough - %s",
                neighborhood,
                exc.description,
            )
            continue
        outlines_read += 1
        unresolved = resolved.isna()
        if not unresolved.any():
            break
        resolved[unresolved & midpoints.intersects(outline)] = neighborhood
    if not outlines_read and last_failure is not None:
        raise Failure(
            "No registered borough has a readable outline for "
            f"{scrape_date}: {last_failure.description}"
        ) from last_failure
    return resolved


def _as_street_sides(
    segments: gpd.GeoDataFrame, id_column: str, name_column: str
) -> gpd.GeoDataFrame:
    """A centre-line network, under this platform's column names.

    The RQTT publishes centre lines - one per segment of roadway - and not the
    two sides the geobase double drew for Montreal. Silver is the layer that
    speaks this platform's vocabulary, so the segment's key becomes
    `COTE_RUE_ID` and its name `NOM_VOIE` here, and the publisher's own columns
    travel alongside untouched.

    **`COTE_RUE_ID` is now a historical name.** It means a *côté de rue*, and
    nothing in this table is a side of a street any more. It is kept because it
    is in the primary key of `silver.neighborhood_streets` and
    `silver.lot_frontage`, denormalised into `gold.lot_profiles` and read by the
    map's tile queries; the word is wrong, and re-keying that lineage to fix a
    word would be the more expensive mistake.

    **What a centre line changes downstream is the *fallback* frontage only.**
    `lot_frontage`'s exact measure is the edge a lot shares with a road parcel
    and needs no line at all, and a centre line still runs inside the road
    parcel - more reliably than a curb side, which hugs the parcel's boundary -
    which is how a road lot is recognised. The reach the fallback spends is
    measured from the line, so a centre line sits half a roadway further from
    the lot than a curb side would; see
    `postgis.DEFAULT_FRONTAGE_FALLBACK_BUFFERS_M`, which is calibrated for it.

    The names stay arguments rather than constants because this is the one
    place a publisher's vocabulary is translated, and a second source arriving
    should have somewhere to be translated from.
    """
    renamed = segments.rename(
        columns={id_column: STREET_ID_COLUMN, name_column: STREET_NAME_COLUMN}
    )
    if STREET_ID_COLUMN in renamed.columns:
        renamed[STREET_ID_COLUMN] = renamed[STREET_ID_COLUMN].astype(str)
    return renamed


def _require_unique_streets(
    streets: gpd.GeoDataFrame, *, tile: str, scrape_date: str
) -> None:
    """One row per street segment - the grain this asset declares.

    `COTE_RUE_ID` is the RQTT's `AQRP_UUID`, which is one per segment across
    the province - and is why the key is that column and not `IdRte`, which
    has both nulls and duplicates (see `rqtt.STREET_ID_FIELD`). So a duplicate
    here means the same segment arrived twice rather than that the publisher
    reuses the key. Left unchecked it would multiply every frontage pair the
    join downstream produces, which shows up as a plausible-looking number
    rather than as a crash.
    """
    numbers = streets[STREET_ID_COLUMN]
    duplicated = numbers[numbers.duplicated(keep=False)]
    if not duplicated.empty:
        repeated = sorted(set(duplicated.astype(str)))
        raise Failure(
            f"{tile} {scrape_date}: {len(repeated)} {STREET_ID_COLUMN} "
            f"value(s) appear more than once, e.g. {', '.join(repeated[:5])}. "
            f"One row per {STREET_ID_COLUMN} is the grain this asset declares."
        )
