"""Partition definitions: one axis per neighborhood, one per scrape month.

The neighborhood axis is a `DynamicPartitionsDefinition`: which boroughs the
pipeline scrapes is a fact recorded in Dagster's own instance storage - the
`dagster` schema on Postgres, or the local SQLite instance - rather than a
tuple frozen into this module. Adding a borough is therefore a registration
(`make neighborhood-add NEIGHBORHOOD=CIL`, or `register_neighborhoods`) and
not a deploy, and the UI, the schedules and every `materialize` call read the
same list. What *may* be registered is still declared here: a key has to be
one this module knows how to resolve into its sources, which is what the
crosswalks below are for.

Three cities publish through those crosswalks. Montreal's boroughs resolve into
Spectrum namespaces, `no_arr` codes on the city's reference-neighborhood layer,
CMHC's Montreal quartiers and Cushman & Wakefield's submarkets. Quebec City's
resolve into the arrondissement layer on Données Québec, the city's ArcGIS
zoning service and its published specification grid, and CMHC's Québec
quartiers. Saguenay resolves into its own Données Québec layers - the zoning
polygons, the administrative limit and the road network - and the grid PDF its
planning counter serves per zone. `city_of` is the switch, and every asset that
reads a city-specific source consults it rather than assuming Montreal.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from enum import Enum

from dagster import (
    DynamicPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
)


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


#: Every borough namespace published by the service, keyed by the partition key
#: used here. The number prefix is Montreal's own borough ordering; two
#: boroughs (L'Ile-Bizard-Sainte-Genevieve, Saint-Laurent) publish nothing.
#: ``VdeM`` and ``Patrimoine`` are city-wide collections rather than boroughs.
NEIGHBORHOOD_NAMESPACES: dict[str, str] = {
    "AC": "01_AC",  # Ahuntsic-Cartierville
    "Anjou": "02_Anjou",
    "CDNNDG": "03_CDNNDG",  # Cote-des-Neiges-Notre-Dame-de-Grace
    "Lachine": "04_Lachine",
    "LaSalle": "05_Las",
    "PMR": "06_PMR",  # Le Plateau-Mont-Royal
    "SO": "07_SO",  # Le Sud-Ouest
    "MHM": "09_MHM",  # Mercier-Hochelaga-Maisonneuve
    "MN": "10_MN",  # Montreal-Nord
    "Outremont": "11_Outremont",
    "PR": "12_PR",  # Pierrefonds-Roxboro
    "RDPPAT": "13_RDPPAT",  # Riviere-des-Prairies-Pointe-aux-Trembles
    "RPP": "14_RPP",  # Rosemont-La Petite-Patrie
    "StLeonard": "16_StLeonard",
    "Verdun": "17_Verdun",
    "VM": "18_VM",  # Ville-Marie
    "VSMPE": "19_VSMPE",  # Villeray-Saint-Michel-Parc-Extension
}

#: Borough code (``no_arr``) carried by the open-data reference-neighborhood
#: layer, for each partition key. The Spectrum namespace prefixes above are
#: Montreal's own borough ordering and do *not* match these, so the two have
#: to be listed separately: `AC` is namespace ``01_AC`` but borough ``23``.
#: Used to cut the borough boundary out of `reference_neighborhoods`, which is
#: what bounds the cadastre query - see `urban_rag.infolot_assets`.
NEIGHBORHOOD_BOROUGH_CODES: dict[str, str] = {
    "AC": "23",  # Ahuntsic-Cartierville
    "Anjou": "09",
    "CDNNDG": "34",  # Cote-des-Neiges-Notre-Dame-de-Grace
    "Lachine": "27",
    "LaSalle": "17",
    "PMR": "21",  # Le Plateau-Mont-Royal
    "SO": "20",  # Le Sud-Ouest
    "MHM": "22",  # Mercier-Hochelaga-Maisonneuve
    "MN": "16",  # Montreal-Nord
    "Outremont": "05",
    "PR": "31",  # Pierrefonds-Roxboro
    "RDPPAT": "33",  # Riviere-des-Prairies-Pointe-aux-Trembles
    "RPP": "24",  # Rosemont-La Petite-Patrie
    "StLeonard": "14",
    "Verdun": "12",
    "VM": "19",  # Ville-Marie
    "VSMPE": "25",  # Villeray-Saint-Michel-Parc-Extension
}

#: Quebec City's six arrondissements, keyed by partition key, with the
#: ``ABREVIATION`` each carries in the city's arrondissement layer on Données
#: Québec (https://www.donneesquebec.ca/recherche/dataset/vque_2). The key
#: *is* the abbreviation - the city already publishes a three-letter code, so
#: inventing a second one would be a crosswalk for its own sake - and the map
#: is kept anyway so the boundary lookup reads the layer's column rather than
#: the partition key. Cut out of `reference_neighborhoods` the way
#: `NEIGHBORHOOD_BOROUGH_CODES` cuts a Montreal borough.
QUEBEC_BOROUGH_ABBREVIATIONS: dict[str, str] = {
    "CIL": "CIL",  # La Cite-Limoilou
    "RIV": "RIV",  # Les Rivieres
    "SSC": "SSC",  # Sainte-Foy-Sillery-Cap-Rouge
    "CHA": "CHA",  # Charlesbourg
    "BEA": "BEA",  # Beauport
    "HSC": "HSC",  # La Haute-Saint-Charles
}

#: Saguenay, as one key for the whole city, with the ``nom`` its administrative
#: limit layer carries the city-wide polygon under
#: (https://www.donneesquebec.ca/recherche/dataset/sag_limite_administrative).
#:
#: **One key, not three.** Saguenay has three arrondissements - Chicoutimi,
#: Jonquière and La Baie - and the same layer draws them, so a per-arrondissement
#: axis was available and is deliberately not taken: the city publishes one
#: zoning layer, one road network and one by-law (VS-R-2012-3) over the whole
#: territory, and its zone numbers do not encode the arrondissement the way
#: Quebec City's leading digit does. Splitting it would put three partitions
#: through the same city-wide sources to no end. What that costs is size - the
#: partition is the whole municipality rather than a borough of it - and the
#: `type` filter below is what keeps the outline the city and not one of its
#: pieces, since the layer holds all three kinds of polygon in one file.
SAGUENAY_NEIGHBORHOODS: dict[str, str] = {
    "SAG": "Saguenay",
}

#: The ``type`` the administrative-limit layer gives the city-wide polygon.
#: The same file also holds ``arrondissement`` and ``secteur`` rows - the
#: former municipalities amalgamated in 2002 - and a union of everything would
#: be the city counted three times over.
SAGUENAY_OUTLINE_TYPE = "ville"

#: Which city each known key belongs to. Derived from the three registries
#: above rather than written a fourth time, so a borough cannot be in one and
#: not the other.
NEIGHBORHOOD_CITIES: dict[str, City] = {
    **{key: City.MONTREAL for key in NEIGHBORHOOD_NAMESPACES},
    **{key: City.QUEBEC for key in QUEBEC_BOROUGH_ABBREVIATIONS},
    **{key: City.SAGUENAY for key in SAGUENAY_NEIGHBORHOODS},
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

#: CMHC Rental Market Survey `Quartier` names covered by each borough
#: partition, for `urban_rag.cmhc_assets`. A third geography keyed the same
#: way as the two maps above, and it lines up with neither: CMHC surveys the
#: Montreal *census metropolitan area* and cuts it into its own neighborhoods,
#: which are finer than a borough in most cases (`VSMPE` is three of them) and
#: coarser in one (`PR`, below).
#:
#: Boroughs absent here are absent upstream too - `Saint-Laurent` and
#: `L'Ile-Bizard-Sainte-Genevieve` publish nothing in Spectrum, so they have
#: no partition key to map even though CMHC surveys them. The CMA's other
#: quartiers are off-island municipalities (Laval, Longueuil, the South Shore)
#: or on-island ones that are not boroughs (Westmount, Mont-Royal,
#: Cote-Saint-Luc, Dorval, Pointe-Claire), and are dropped for the same reason.
#:
#: Quebec City's quartiers come from the same workbook under the `Québec`
#: centre. La Cité-Limoilou is the survey's Haute-Ville and Basse-Ville zones:
#: Vieux-Québec and Saint-Jean-Baptiste, Montcalm, Saint-Sacrement, the
#: Vieux-Port, Saint-Roch, Saint-Sauveur and Limoilou. Vanier, Duberger and
#: Les Saules sit in the same two zones and belong to Les Rivières.
CMHC_QUARTIERS: dict[str, tuple[str, ...]] = {
    "AC": ("Ahuntsic", "Cartierville"),
    "Anjou": ("Anjou",),
    "CDNNDG": ("Côte-des-Neiges", "Notre-Dame-de-Grâce"),
    "Lachine": ("Lachine",),
    "LaSalle": ("LaSalle",),
    "PMR": ("Plateau-Mont-Royal",),
    "SO": ("Sud-Ouest",),
    "MHM": ("Hochelaga-Maisonneuve", "Mercier"),
    "MN": ("Montréal-Nord",),
    "Outremont": ("Outremont",),
    # Wider than the borough: CMHC surveys Pierrefonds and Roxboro together
    # with Senneville, a separate municipality, and publishes no split.
    "PR": ("Senneville-Roxboro-Pierrefonds",),
    "RDPPAT": ("Pointe-aux-Trembles", "Rivière-des-Prairies"),
    "RPP": ("Rosemont/La Petite-Patrie",),
    "StLeonard": ("Saint-Léonard",),
    # Ile-des-Soeurs is part of the Verdun borough, but CMHC files it under
    # its downtown zone rather than with the rest of Verdun.
    "Verdun": ("Verdun", "Île-des-Soeurs"),
    "VM": ("Ville-Marie", "Ville-Marie Est"),
    "VSMPE": ("Parc-Extension", "Saint-Michel", "Villeray"),
    "CIL": (
        "Cap-Blanc/Vieux-Québec/St-Jean-Baptiste",
        "Vieux-Port",
        "Montcalm (Plateau)",
        "Saint-Sacrement",
        "Saint-Roch",
        "Saint-Sauveur",
        "Limoilou",
    ),
    # Saguenay is one partition, so it takes every quartier the `Saguenay`
    # centre publishes - the survey's four zones (Secteur Nord, Chicoutimi-Sud,
    # Jonquière, La Baie) broken into these eight. `Total` rows are the
    # workbook's own subtotals and are deliberately absent: including one
    # beside the quartiers it sums would count the city's stock twice.
    "SAG": (
        "Chicoutimi-Nord",
        "Périphérie (Nord)",
        "Chicoutimi (centre-ville)",
        "Chicoutimi-Sud",
        "Jonquière (centre-ville)",
        "Jonquière",
        "Périphérie (Sud)",
        "La Baie",
    ),
}

#: Cushman & Wakefield's MarketBeat submarket covering each borough, keyed by
#: partition key. The same kind of crosswalk `CMHC_QUARTIERS` is and applied
#: for the same reason: the publisher carves the island its own way, and a
#: borough-level rent is a much better answer than an island-level one.
#:
#: **One name for both sectors, matched loosely.** The office report writes
#: `Midtown North` and the industrial one writes `Montréal Midtown North` for
#: what is the same territory, so `submarket_for` matches on the name with any
#: leading `Montréal` taken off - see `urban_rag.marketbeat`.
#:
#: A borough with no entry falls back to the whole-market row, which is a real
#: answer rather than a gap: C&W's submarkets are drawn around industrial and
#: office concentrations, and several residential boroughs sit inside none of
#: them. `num_lots_priced_by_submarket` says which happened.
#:
#: Only the boroughs whose mapping has actually been checked against a report
#: are here. An unlisted one is not an error; it is the island-wide rent.
#: Neither Quebec City nor Saguenay has a MarketBeat at all - C&W publishes
#: none for either - so their keys are deliberately absent and
#: `urban_rag.rent_assets` says so when it prices one off the Montreal market.
MARKETBEAT_SUBMARKETS: dict[str, str] = {
    # Villeray-Saint-Michel-Parc-Extension sits in Midtown North on both the
    # office and the industrial maps - the belt north of the Metropolitain,
    # which is where the borough's industrial stock actually is.
    "VSMPE": "Midtown North",
    "AC": "Midtown North",
    "RPP": "Midtown Central",
    "CDNNDG": "Décarie CDN",
    "VM": "Downtown South",
    "SO": "Downtown Southwest",
    "Verdun": "Île-Des-Soeurs",
    "Lachine": "Lachine",
    "StLeonard": "Montréal East",
    "MHM": "Montréal East",
    "MN": "Montréal East",
    "RDPPAT": "Montréal East",
    "Anjou": "Montréal East",
    "PR": "West Island",
    "LaSalle": "Lachine",
}

#: The name the neighborhood axis is registered under in the Dagster instance.
#: It is what `instance.get_dynamic_partitions` and the UI's partition dialog
#: know the axis by, so it is spelled once.
NEIGHBORHOOD_PARTITIONS_NAME = "neighborhood"

#: The keys registered when the instance has none yet - a fresh SQLite home,
#: or a Postgres schema Dagster has just created. Not "the enabled
#: neighborhoods": that list lives in the instance and is read back with
#: `enabled_neighborhoods`. Widen the running set with
#: `make neighborhood-add`, not by editing this.
DEFAULT_NEIGHBORHOODS: tuple[str, ...] = ("VSMPE", "CIL")

#: First month the pipeline may scrape. Must be the first of a month: a
#: `MonthlyPartitionsDefinition` cuts its windows on month boundaries and
#: rejects a start date that does not sit on one.
#:
#: `end_offset=1` makes the *current* month a valid partition, which is what
#: "month of scrape" means here - unlike the usual event-time reading, where
#: the latest complete partition would be last month. Partition keys are still
#: `YYYY-MM-DD`, always the first of the month, so nothing downstream that
#: reads a scrape date as a plain ISO date has to change.
SCRAPE_START_DATE = "2026-08-01"

#: Where a scrape month begins and ends. Declared once because two things have
#: to agree about it: the windows `date_partitions` cuts, and the month
#: `urban_rag.guards` compares a partition key against. If they disagreed, the
#: guard would be wrong for the hours between this zone's midnight and UTC's.
SCRAPE_TIMEZONE = "America/Toronto"

date_partitions = MonthlyPartitionsDefinition(
    start_date=SCRAPE_START_DATE,
    timezone=SCRAPE_TIMEZONE,
    end_offset=1,
)

#: The borough axis. Its keys are whatever the instance holds under
#: `NEIGHBORHOOD_PARTITIONS_NAME`; `register_neighborhoods` is the one way in
#: and it refuses a key `known_neighborhoods` cannot resolve.
neighborhood_partitions = DynamicPartitionsDefinition(
    name=NEIGHBORHOOD_PARTITIONS_NAME
)

scrape_partitions = MultiPartitionsDefinition(
    {"date": date_partitions, "neighborhood": neighborhood_partitions}
)


def known_neighborhoods() -> tuple[str, ...]:
    """Every key this module can resolve into its sources, both cities."""
    return tuple(NEIGHBORHOOD_CITIES)


def city_of(neighborhood: str) -> City:
    """Which city's publishers a neighborhood key resolves through."""
    try:
        return NEIGHBORHOOD_CITIES[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(NEIGHBORHOOD_CITIES)}"
        ) from None


def metric_crs_for(neighborhood: str) -> str:
    """The projected CRS a neighborhood's lengths and areas are measured in."""
    return METRIC_CRS_BY_CITY[city_of(neighborhood)]


def metric_srid_for(neighborhood: str) -> int:
    """`metric_crs_for`, as the integer SRID PostGIS takes."""
    return int(metric_crs_for(neighborhood).split(":")[1])


def municipality_code_for(city: City) -> str:
    """The *code geographique* a city's roll and RFU rows carry."""
    return MUNICIPALITY_CODES[city]


def cmhc_centre_for(neighborhood: str) -> str:
    """The CMHC survey centre a neighborhood's quartiers are printed under."""
    return CMHC_CENTRES[city_of(neighborhood)]


def enabled_neighborhoods(instance=None) -> tuple[str, ...]:
    """The keys registered in the Dagster instance, seeding it when empty.

    Reads `instance.get_dynamic_partitions`, which is the same store the UI,
    the schedules and `dagster asset materialize` validate a partition key
    against. An instance that holds no key at all - a fresh home - is seeded
    with `DEFAULT_NEIGHBORHOODS` first, so the very first run has a borough to
    run for and the seeding is visible in the instance afterwards rather than
    being a default nothing recorded.

    ``instance`` is the `DagsterInstance` in hand: `context.instance` inside
    an asset or a schedule, or `DagsterInstance.get()` from a CLI. With none,
    the defaults are returned and nothing is written, which is what a caller
    with no instance - a docstring, a test of the crosswalks - can honestly
    be told.
    """
    if instance is None:
        return DEFAULT_NEIGHBORHOODS
    keys = tuple(instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME))
    if keys:
        return keys
    instance.add_dynamic_partitions(
        NEIGHBORHOOD_PARTITIONS_NAME, list(DEFAULT_NEIGHBORHOODS)
    )
    return DEFAULT_NEIGHBORHOODS


def register_neighborhoods(instance, keys: Iterable[str]) -> tuple[str, ...]:
    """Add ``keys`` to the neighborhood axis; returns the ones newly added.

    Refuses a key `known_neighborhoods` does not list, before touching the
    instance: a registered key with no crosswalk would be offered by the UI
    and fail at `namespace_for` or `borough_boundary` on its first run, which
    is the wrong place to learn that a borough was misspelled.
    """
    wanted = tuple(
        dict.fromkeys(str(key).strip() for key in keys if str(key).strip())
    )
    unknown = [key for key in wanted if key not in NEIGHBORHOOD_CITIES]
    if unknown:
        raise KeyError(
            f"Unknown neighborhood key(s) {unknown}; known keys: "
            f"{', '.join(known_neighborhoods())}"
        )
    existing = set(instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME))
    added = tuple(key for key in wanted if key not in existing)
    if added:
        instance.add_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME, list(added))
    return added


def unregister_neighborhoods(instance, keys: Iterable[str]) -> tuple[str, ...]:
    """Take ``keys`` off the axis; returns the ones that were registered.

    The parquet and the Postgres rows a borough has already produced are left
    exactly where they are - this only stops Dagster offering the key. Put it
    back with `register_neighborhoods` and every existing partition is visible
    again.
    """
    existing = set(instance.get_dynamic_partitions(NEIGHBORHOOD_PARTITIONS_NAME))
    removed = tuple(key for key in dict.fromkeys(keys) if key in existing)
    for key in removed:
        instance.delete_dynamic_partition(NEIGHBORHOOD_PARTITIONS_NAME, key)
    return removed


def namespace_for(neighborhood: str) -> str:
    """Spectrum namespace backing a Montreal neighborhood partition key."""
    try:
        return NEIGHBORHOOD_NAMESPACES[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(NEIGHBORHOOD_NAMESPACES)}"
        ) from None


def borough_code_for(neighborhood: str) -> str:
    """Reference-layer borough code (``no_arr``) for a Montreal key."""
    try:
        return NEIGHBORHOOD_BOROUGH_CODES[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(NEIGHBORHOOD_BOROUGH_CODES)}"
        ) from None


def quebec_abbreviation_for(neighborhood: str) -> str:
    """The ``ABREVIATION`` a Quebec City key carries in the arrondissement layer."""
    try:
        return QUEBEC_BOROUGH_ABBREVIATIONS[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(QUEBEC_BOROUGH_ABBREVIATIONS)}"
        ) from None


def saguenay_outline_name_for(neighborhood: str) -> str:
    """The ``nom`` a Saguenay key carries in the administrative-limit layer.

    Paired with `SAGUENAY_OUTLINE_TYPE`, which is what selects the city-wide
    polygon rather than the arrondissement of the same name: the layer holds a
    ``ville`` row called *Saguenay* and three ``arrondissement`` rows, and
    *Chicoutimi* is both an arrondissement and a secteur in it.
    """
    try:
        return SAGUENAY_NEIGHBORHOODS[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(SAGUENAY_NEIGHBORHOODS)}"
        ) from None


def submarket_for(neighborhood: str) -> str | None:
    """The MarketBeat submarket a borough sits in, or None for none.

    None rather than a raise, unlike `quartiers_for`: a borough C&W draws no
    submarket around is priced at the island-wide rent, which is a worse answer
    but a real one. A missing CMHC quartier is different - that crosswalk is
    the only route from the survey to the borough, and a gap there means a
    partition that cannot be computed at all.
    """
    return MARKETBEAT_SUBMARKETS.get(neighborhood)


def quartiers_for(neighborhood: str) -> tuple[str, ...]:
    """CMHC survey neighborhoods covered by a borough partition key."""
    try:
        return CMHC_QUARTIERS[neighborhood]
    except KeyError:
        raise KeyError(
            f"No CMHC quartier mapping for {neighborhood!r}; "
            f"known keys: {sorted(CMHC_QUARTIERS)}"
        ) from None


def partition_keys_for(
    scrape_date: str, neighborhoods: Sequence[str]
) -> list[str]:
    """``date|neighborhood`` keys, in the order `MultiPartitionKey` prints them."""
    return [f"{scrape_date}|{neighborhood}" for neighborhood in neighborhoods]
