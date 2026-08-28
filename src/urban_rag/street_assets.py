"""The street network at this platform's grain: one borough's street sides,
clipped to its boundary, measured in metres.

`street_network` snapshots the geobase double island-wide, because that is how
the city publishes it - 91,546 sides of street with no borough column of their
own. Everything downstream of it is borough-scoped, so this is where the
island is cut into partitions, the same hinge `neighborhood_buildings` turns
on for BDOI and `neighborhood_lots` for Infolot. The difference is that those
two cut in *bronze*, at the query, because the source is fetched per borough;
this one cuts in silver, from a file already on disk, because one download
serves every borough and re-downloading 91 MB per partition would be work done
for nothing.

**Clipped, not selected.** A street side that crosses the borough line is
`ST_Intersection`-ed against the boundary rather than kept whole, so the
geometry in a `VSMPE` partition is inside VSMPE. The full published length
travels alongside as `segment_length_m`, and `pct_in_borough` says how much of
it survived, so a segment cut in half is visible as one rather than silently
becoming a shorter street.

That cut has one edge effect worth naming. The cadastre in
`neighborhood_lots` comes from a boundary *query*, so it reaches a little
past the borough line - a lot straddling the line is in the partition whole.
Its frontage on a street side that was clipped at that line is measured
against the surviving piece only, and is therefore under-reported. The lots
this touches are the ones on the boundary itself; `num_boundary_clipped` counts
the street sides involved so the size of the effect is readable rather than
assumed.

Lengths are computed in EPSG:32188 (NAD83 / MTM zone 8), the projected system
Montreal is surveyed in, not in the 4326 the geometry is stored and written in:
a degree is not a metre, and `GeoSeries.length` on lon/lat would report a
number in degrees that reads like one in metres. PostGIS gets the same answer
downstream through `geography`; GeoPandas has no such type, so the projection
is explicit here.
"""

import geopandas as gpd
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

from urban_rag.frames import count_invalid_geometries, write_frame
from urban_rag.layers import key_prefix
from urban_rag.open_data_assets import (
    STREETS_FILE,
    STREET_ID_COLUMN,
    STREET_NAME_COLUMN,
    borough_boundary,
    reference_neighborhoods,
    street_network,
)
from urban_rag.partitions import scrape_partitions
from urban_rag.postgis import load_streets
from urban_rag.rag.pgvector import PostgresUnavailable
from urban_rag.resources import ParquetStore, PostgisResource
from urban_rag.storage import clear_parquet, join, storage_options
from urban_rag.warehouse import MissingRelation, published_metadata

GROUP = "silver_streets"

#: The one file a partition is written to, under
#: `silver/neighborhood_streets/<YYYY-MM-DD>/<neighborhood>/`.
STREETS_FILE_OUT = "neighborhood_streets.parquet"

#: Where lengths are measured. NAD83 / MTM zone 8 is the projected system the
#: island is surveyed in; metres in it are metres on the ground.
METRIC_CRS = "EPSG:32188"

#: The 1-dimensional geometry types a clipped street side may legitimately be.
#: Anything else - a bare Point where a side only grazes the boundary - is a
#: touch rather than a piece of street inside the borough.
_LINE_TYPES = ("LineString", "MultiLineString", "LinearRing")


@asset(
    key_prefix=key_prefix("neighborhood_streets"),
    partitions_def=scrape_partitions,
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
        "One borough's sides of street, cut out of that day's island-wide "
        "geobase double against its boundary from reference_neighborhoods. "
        "One row per COTE_RUE_ID, geometry clipped to the borough and valid, "
        "with the published length, the length inside the borough and the "
        "share of the segment that survived the cut, as silver/"
        f"neighborhood_streets/<YYYY-MM-DD>/<neighborhood>/{STREETS_FILE_OUT} "
        "and upserted into silver.neighborhood_streets on (scrape_date, "
        "neighborhood, cote_rue_id)."
    ),
)
def neighborhood_streets(
    context: AssetExecutionContext,
    store: ParquetStore,
    postgis: PostgisResource,
) -> MaterializeResult:
    dimensions = context.partition_key.keys_by_dimension
    neighborhood = dimensions["neighborhood"]
    scrape_date = dimensions["date"][:10]

    streets_path = join(
        store.partition_dir(street_network.key.path[-1], scrape_date),
        STREETS_FILE,
    )
    streets = _read_streets(streets_path, scrape_date=scrape_date)
    if streets.empty:
        raise Failure(f"{streets_path} holds no street side to cut.")
    if STREET_ID_COLUMN not in streets.columns:
        raise Failure(
            f"{streets_path} has no {STREET_ID_COLUMN} column - it was not "
            "written by street_network."
        )

    boundary = borough_boundary(store, scrape_date, neighborhood)

    # Selected before clipped: `intersects` rides the spatial index, while
    # `intersection` over 91,546 island-wide sides would clip every one of
    # them against a borough most of them are nowhere near.
    touching = streets[streets.intersects(boundary)].copy()
    if touching.empty:
        raise Failure(
            f"No street side intersects {neighborhood}; its boundary in "
            f"reference_neighborhoods for {scrape_date} may be empty."
        )

    published_length_m = _length_m(touching.geometry)
    clipped = touching.set_geometry(
        gpd.GeoSeries(
            [_lines_only(geometry) for geometry in touching.geometry.intersection(boundary)],
            index=touching.index,
            crs=touching.crs,
        )
    )
    # A side that clipped to a point only touched the boundary; it is not a
    # street inside this borough, and a zero-length row would be one.
    clipped = clipped[clipped.geometry.notna()].copy()
    published_length_m = published_length_m.loc[clipped.index]
    if clipped.empty:
        raise Failure(
            f"Every street side touching {neighborhood} clips to a point; the "
            f"boundary in reference_neighborhoods for {scrape_date} is degenerate."
        )

    clipped["segment_length_m"] = published_length_m
    clipped["length_in_borough_m"] = _length_m(clipped.geometry)
    clipped["pct_in_borough"] = (
        100.0 * clipped["length_in_borough_m"] / clipped["segment_length_m"]
    ).where(clipped["segment_length_m"] > 0, 0.0)
    # The path carries bare keys rather than hive `key=value` pairs, so the
    # partition has to travel as columns. `scrape_date` is already one, from
    # bronze, and is overwritten rather than trusted: this partition's date is
    # the one that was asked for.
    clipped["neighborhood"] = neighborhood
    clipped["scrape_date"] = scrape_date

    _require_unique_streets(clipped, neighborhood=neighborhood, scrape_date=scrape_date)
    # Lines cannot self-intersect their way to invalidity the way the polygon
    # layers can, so this is a guard rather than a repair - but silver owes its
    # readers geometry `ST_Intersection` can be run over either way.
    still_invalid = count_invalid_geometries(clipped)
    if still_invalid:
        raise Failure(
            f"{still_invalid} clipped street side(s) are invalid; the partition "
            "cannot be joined against."
        )

    output_dir = store.partition_dir(
        context.asset_key.path[-1], scrape_date, neighborhood
    )
    removed = clear_parquet(output_dir)
    if removed:
        context.log.info("Removed %d file(s) from a previous run", len(removed))
    path = write_frame(clipped, join(output_dir, STREETS_FILE_OUT))

    # Published after the file is written, so a database that is down costs a
    # re-run of the load rather than of the cut. This asset owns
    # `silver.neighborhood_streets`: `lot_frontage` used to load it on its way
    # past, which left a table whose writer was not the asset it is named for.
    try:
        with postgis.connect() as connection:
            published = load_streets(
                connection,
                clipped,
                neighborhood=neighborhood,
                scrape_date=scrape_date,
            )
    except (PostgresUnavailable, MissingRelation) as exc:
        raise Failure(
            f"{path} was written, but silver.neighborhood_streets could not be "
            f"updated for {neighborhood} {scrape_date}: {exc}"
        ) from exc

    # A side whose published length did not survive the cut whole is one that
    # straddles the borough line - the edge effect the module docstring names.
    boundary_clipped = int((clipped["pct_in_borough"] < 99.9).sum())
    total_km = float(clipped["length_in_borough_m"].sum()) / 1000.0
    context.log.info(
        "%s %s: %d of %d street side(s) inside the borough, %.1f km (%d clipped "
        "at the boundary) -> %s",
        neighborhood,
        scrape_date,
        len(clipped),
        len(streets),
        total_km,
        boundary_clipped,
        path,
    )

    return MaterializeResult(
        metadata={
            "dagster/row_count": len(clipped),
            "num_street_sides": len(clipped),
            "num_street_sides_island_wide": len(streets),
            "num_streets_named": int(clipped[STREET_NAME_COLUMN].nunique())
            if STREET_NAME_COLUMN in clipped.columns
            else 0,
            "total_length_km": round(total_km, 2),
            "num_boundary_clipped": boundary_clipped,
            "num_touching_only": len(touching) - len(clipped),
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


def _length_m(geometry: gpd.GeoSeries):
    """``geometry``'s length in metres, measured in `METRIC_CRS`."""
    return geometry.to_crs(METRIC_CRS).length


def _lines_only(geometry):
    """The 1-dimensional parts of ``geometry``, or None when it has none.

    Clipping a line against a polygon usually gives back lines, but a side that
    meets the boundary at a single point gives a Point, and one that does both
    gives a GeometryCollection holding each. Only the linework is street inside
    the borough; the points are where it stops being.
    """
    if geometry is None or geometry.is_empty:
        return None
    if geometry.geom_type == "GeometryCollection":
        parts = [part for part in geometry.geoms if part.geom_type in _LINE_TYPES]
        if not parts:
            return None
        geometry = shapely.union_all(parts)
    if geometry.geom_type not in _LINE_TYPES or geometry.is_empty:
        return None
    return geometry


def _require_unique_streets(
    streets: gpd.GeoDataFrame, *, neighborhood: str, scrape_date: str
) -> None:
    """One row per street side - the grain this asset declares.

    `COTE_RUE_ID` is unique across the island in the published layer, so a
    duplicate here means the same side arrived twice rather than that the city
    reuses the key. Left unchecked it would multiply every frontage pair the
    join downstream produces, which shows up as a plausible-looking number
    rather than as a crash.
    """
    numbers = streets[STREET_ID_COLUMN]
    duplicated = numbers[numbers.duplicated(keep=False)]
    if not duplicated.empty:
        repeated = sorted(set(duplicated.astype(str)))
        raise Failure(
            f"{neighborhood} {scrape_date}: {len(repeated)} {STREET_ID_COLUMN} "
            f"value(s) appear more than once, e.g. {', '.join(repeated[:5])}. "
            f"One row per {STREET_ID_COLUMN} is the grain this asset declares."
        )
