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
