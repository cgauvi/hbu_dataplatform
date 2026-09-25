"""The three cities, and the switch from a partition key to the one it is in.

What *may* be registered on the neighborhood axis is declared here: a key has
to be one this module knows how to resolve into its sources. The per-city
crosswalks live with each city - `hbu_dataplatform.cities.montreal.registry`,
`.quebec_city.registry`, `.saguenay.registry` - and this module composes them
into `NEIGHBORHOOD_CITIES` and `CMHC_QUARTIERS`, so a borough cannot be in one
registry and not the other. `city_of` is the switch, and every asset that
reads a city-specific source consults it rather than assuming Montreal.
"""

from __future__ import annotations

from enum import Enum

from hbu_dataplatform.cities.montreal import registry as montreal
from hbu_dataplatform.cities.quebec_city import registry as quebec_city
from hbu_dataplatform.cities.saguenay import registry as saguenay
from hbu_dataplatform.core import tile_cut


class City(str, Enum):
    """The three publishers a neighborhood key can belong to.

    A `str` enum so the value reads as the plain word in a parquet column, a
    log line or a partition directory, and compares equal to it.
    """

    MONTREAL = "montreal"
    QUEBEC = "quebec"
    SAGUENAY = "saguenay"

    def __str__(self) -> str:  # so f"{city}" is "quebec", not "City.QUEBEC"
        return self.value


#: Which city each known key belongs to. Derived from the three registries
#: rather than written a fourth time, so a borough cannot be in one and
#: not the other.
NEIGHBORHOOD_CITIES: dict[str, City] = {
    **{key: City.MONTREAL for key in montreal.NEIGHBORHOOD_NAMESPACES},
    **{key: City.QUEBEC for key in quebec_city.QUEBEC_BOROUGH_ABBREVIATIONS},
    **{key: City.SAGUENAY for key in saguenay.SAGUENAY_NEIGHBORHOODS},
}

#: The projected CRS each city is surveyed in - NAD83 / MTM zone 8 for the
#: island, zone 7 for Quebec City and for Saguenay. Metres in it are metres on
#: the ground, which is what every length and area this platform states is
#: measured in; a zone used three degrees off its meridian would be off by a
#: tenth of a per cent, which is small and is still not the surveyed answer.
#:
#: Saguenay shares Quebec City's zone rather than borrowing it: MTM 7 runs from
#: 72°W to 69°W on the 70°30'W meridian, and the city spans 71°34'W to 70°42'W,
#: so it sits inside the zone with its centre about half a degree off the
#: meridian - closer to it than Quebec City is.
METRIC_CRS_BY_CITY: dict[City, str] = {
    City.MONTREAL: "EPSG:32188",
    City.QUEBEC: "EPSG:32187",
    City.SAGUENAY: "EPSG:32187",
}

#: The five-digit *code geographique* each city files its assessment roll and
#: its *richesse fonciere uniformisee* under. Montreal's is the one the roll
#: was always filtered to; Quebec City's and Saguenay's are the further rows
#: the same province-wide sources keep. Saguenay's 94068 is the amalgamated
#: city, and it is the code its own layers stamp every feature with
#: (``municipalite``), which is how a layer can be checked against the key it
#: was fetched for.
MUNICIPALITY_CODES: dict[City, str] = {
    City.MONTREAL: "66023",
    City.QUEBEC: "23027",
    City.SAGUENAY: "94068",
}

#: The CMHC Rental Market Survey *centre* each city is surveyed as. The survey
#: is national and one workbook carries all three; this is the label the bronze
#: snapshot keeps rows for.
CMHC_CENTRES: dict[City, str] = {
    City.MONTREAL: "Montréal",
    City.QUEBEC: "Québec",
    City.SAGUENAY: "Saguenay",
}

#: CMHC survey quartiers per partition key, all three cities, for
#: `hbu_dataplatform.sources.cmhc.assets`. Each city's registry says how its
#: keys were matched to the survey's own neighborhoods.
CMHC_QUARTIERS: dict[str, tuple[str, ...]] = {
    **montreal.CMHC_QUARTIERS,
    **quebec_city.CMHC_QUARTIERS,
    **saguenay.CMHC_QUARTIERS,
}


def city_of_tile(tile: str) -> City:
    """Which city's publishers a cut cell's ground belongs to."""
    return City(tile_cut.city_value_of(tile))


def city_of(neighborhood: str) -> City:
    """Which city's publishers a neighborhood key resolves through."""
    try:
        return NEIGHBORHOOD_CITIES[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(NEIGHBORHOOD_CITIES)}"
        ) from None


def metric_crs_for_city(city: City) -> str:
    """The projected CRS a city's lengths and areas are measured in."""
    return METRIC_CRS_BY_CITY[city]


def metric_srid_for_city(city: City) -> int:
    """`metric_crs_for_city`, as the integer SRID PostGIS takes."""
    return int(metric_crs_for_city(city).split(":")[1])


def metric_crs_for(neighborhood: str) -> str:
    """The projected CRS a neighborhood's lengths and areas are measured in."""
    return metric_crs_for_city(city_of(neighborhood))


def metric_srid_for(neighborhood: str) -> int:
    """`metric_crs_for`, as the integer SRID PostGIS takes."""
    return metric_srid_for_city(city_of(neighborhood))


def municipality_code_for(city: City) -> str:
    """The *code geographique* a city's roll and RFU rows carry."""
    return MUNICIPALITY_CODES[city]


def cmhc_centre_for_city(city: City) -> str:
    """The CMHC survey centre a city's quartiers are printed under."""
    return CMHC_CENTRES[city]


def cmhc_centre_for(neighborhood: str) -> str:
    """The CMHC survey centre a neighborhood's quartiers are printed under."""
    return cmhc_centre_for_city(city_of(neighborhood))


def source_namespace_for(neighborhood: str) -> str:
    """The unit the *publisher* files this key's layers under.

    Not a spatial fact, and that is the point. `rag.features` is unique on
    ``(source_table, feature_id, ...)`` plus something, because `source_table`
    is the file slug - `Reglement_urbanisme__VSP_REG_ZONE` - and Montreal
    restarts its zone numbers at C01-001 in every borough. The something has
    been `neighborhood` since 005_silver_lot_features.sql widened the
    constraint, and `neighborhood` happens to work only because it is 1:1 with
    the Spectrum namespace that actually distinguishes the two rows. This names
    the real qualifier, so the constraint stops depending on that coincidence.

    Montreal's is its Spectrum namespace, `19_VSMPE`. Quebec City and Saguenay
    publish **one** zoning layer for the whole municipality and no namespace at
    all, so theirs is the city: their zone codes are already unique city-wide -
    Quebec's leading digit *is* the arrondissement and Saguenay is one key by
    construction (see its registry). Returning ``""`` for them would be a
    qualifier that qualifies nothing, and a constraint that is strong for one
    city and vacuous for the other two is the asymmetry this avoids.
    """
    city = city_of(neighborhood)
    if city is City.MONTREAL:
        return montreal.namespace_for(neighborhood)
    return str(city)


def quartiers_for(neighborhood: str) -> tuple[str, ...]:
    """CMHC survey neighborhoods covered by a borough partition key."""
    try:
        return CMHC_QUARTIERS[neighborhood]
    except KeyError:
        raise KeyError(
            f"No CMHC quartier mapping for {neighborhood!r}; "
            f"known keys: {sorted(CMHC_QUARTIERS)}"
        ) from None
