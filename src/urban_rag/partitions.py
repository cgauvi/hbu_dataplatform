"""Partition definitions: one axis per neighborhood, one per scrape date."""

from __future__ import annotations

from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

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
}

#: Only these are scraped. Add keys from NEIGHBORHOOD_NAMESPACES to widen the
#: partition set; existing partitions are untouched by the addition.
ENABLED_NEIGHBORHOODS: tuple[str, ...] = ("VSMPE",)

#: First date the pipeline may scrape. `end_offset=1` makes *today* a valid
#: partition, which is what "date of scrape" means here - unlike the usual
#: event-time reading, where the latest complete partition is yesterday.
SCRAPE_START_DATE = "2026-08-01"

date_partitions = DailyPartitionsDefinition(
    start_date=SCRAPE_START_DATE,
    timezone="America/Toronto",
    end_offset=1,
)

neighborhood_partitions = StaticPartitionsDefinition(list(ENABLED_NEIGHBORHOODS))

scrape_partitions = MultiPartitionsDefinition(
    {"date": date_partitions, "neighborhood": neighborhood_partitions}
)


def namespace_for(neighborhood: str) -> str:
    """Spectrum namespace backing a neighborhood partition key."""
    try:
        return NEIGHBORHOOD_NAMESPACES[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(NEIGHBORHOOD_NAMESPACES)}"
        ) from None


def borough_code_for(neighborhood: str) -> str:
    """Reference-layer borough code (``no_arr``) for a neighborhood key."""
    try:
        return NEIGHBORHOOD_BOROUGH_CODES[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(NEIGHBORHOOD_BOROUGH_CODES)}"
        ) from None


def quartiers_for(neighborhood: str) -> tuple[str, ...]:
    """CMHC survey neighborhoods covered by a borough partition key."""
    try:
        return CMHC_QUARTIERS[neighborhood]
    except KeyError:
        raise KeyError(
            f"No CMHC quartier mapping for {neighborhood!r}; "
            f"known keys: {sorted(CMHC_QUARTIERS)}"
        ) from None
