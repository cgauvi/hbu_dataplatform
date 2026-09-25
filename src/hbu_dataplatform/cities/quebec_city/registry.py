"""Quebec City's arrondissements, and how a partition key resolves into the
city's publishers.

A key here is one of the six arrondissements. It resolves into the
arrondissement layer on Données Québec (by its ``ABREVIATION``), the city's
ArcGIS zoning service and its published specification grid, and CMHC's
Québec quartiers. `hbu_dataplatform.partitions.axes.cities` composes this with the
other cities' registries.
"""

from __future__ import annotations

#: Quebec City's six arrondissements, keyed by partition key, with the
#: ``ABREVIATION`` each carries in the city's arrondissement layer on Données
#: Québec (https://www.donneesquebec.ca/recherche/dataset/vque_2). The key
#: *is* the abbreviation - the city already publishes a three-letter code, so
#: inventing a second one would be a crosswalk for its own sake - and the map
#: is kept anyway so the boundary lookup reads the layer's column rather than
#: the partition key. Cut out of `reference_neighborhoods` the way
#: Montreal's `NEIGHBORHOOD_BOROUGH_CODES` cuts a borough.
QUEBEC_BOROUGH_ABBREVIATIONS: dict[str, str] = {
    "CIL": "CIL",  # La Cite-Limoilou
    "RIV": "RIV",  # Les Rivieres
    "SSC": "SSC",  # Sainte-Foy-Sillery-Cap-Rouge
    "CHA": "CHA",  # Charlesbourg
    "BEA": "BEA",  # Beauport
    "HSC": "HSC",  # La Haute-Saint-Charles
}

#: CMHC Rental Market Survey `Quartier` names covered by each arrondissement,
#: from the same workbook Montreal's come from, under the `Québec` centre.
#: La Cité-Limoilou is the survey's Haute-Ville and Basse-Ville zones:
#: Vieux-Québec and Saint-Jean-Baptiste, Montcalm, Saint-Sacrement, the
#: Vieux-Port, Saint-Roch, Saint-Sauveur and Limoilou. Vanier, Duberger and
#: Les Saules sit in the same two zones and belong to Les Rivières.
#: Sainte-Foy-Sillery-Cap-Rouge is the survey's `Sainte-Foy-Sillery` zone
#: whole, plus one half of `Saint-Augustin-Cap-Rouge`.
CMHC_QUARTIERS: dict[str, tuple[str, ...]] = {
    "CIL": (
        "Cap-Blanc/Vieux-Québec/St-Jean-Baptiste",
        "Vieux-Port",
        "Montcalm (Plateau)",
        "Saint-Sacrement",
        "Saint-Roch",
        "Saint-Sauveur",
        "Limoilou",
    ),
    # Sainte-Foy-Sillery-Cap-Rouge, from two of the survey's Québec zones.
    # `Sainte-Foy-Sillery` is the borough's and nothing else's, so all three
    # of its quartiers are taken. `Saint-Augustin-Cap-Rouge` is not: it pairs
    # Cap-Rouge, which is in the borough, with Saint-Augustin-de-Desmaures,
    # which is a separate municipality - and unlike Senneville in Montreal's
    # `PR` the survey publishes the two apart, so only Cap-Rouge is claimed.
    "SSC": (
        "Haut de Sainte-Foy",
        "Pointe-de-Sainte-Foy",
        "Sillery",
        "Cap-Rouge",
    ),
}


def quebec_abbreviation_for(neighborhood: str) -> str:
    """The ``ABREVIATION`` a Quebec City key carries in the arrondissement layer."""
    try:
        return QUEBEC_BOROUGH_ABBREVIATIONS[neighborhood]
    except KeyError:
        raise KeyError(
            f"Unknown neighborhood {neighborhood!r}; "
            f"known keys: {sorted(QUEBEC_BOROUGH_ABBREVIATIONS)}"
        ) from None
