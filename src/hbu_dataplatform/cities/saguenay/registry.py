"""Saguenay as one partition key, and how it resolves into the city's
publishers: the administrative-limit layer on Données Québec and CMHC's
Saguenay quartiers. The zoning layer and the grid PDFs are in
`hbu_dataplatform.cities.saguenay.zoning`; `hbu_dataplatform.partitions.axes.cities`
composes this with the other cities' registries.
"""

from __future__ import annotations

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

#: The administrative limit - the city, its three arrondissements and the
#: former municipalities amalgamated into it - read for Saguenay the way
#: `vque_2` is read for Quebec City.
#:
#: The road network that used to sit beside it here (`sag-reseau-routier`) is
#: gone: every city's streets now come from the province-wide RQTT, so there is
#: no Saguenay-specific street feed to name. See `hbu_dataplatform.sources.rqtt`.
LIMITS_DATASET = "sag_limite_administrative"
LIMITS_GEOJSON = "sag_limiteadministrative.geojson"

#: Columns the limit layer names its polygons by - see
#: `SAGUENAY_OUTLINE_TYPE` for why both are needed.
LIMIT_NAME_FIELD = "nom"
LIMIT_TYPE_FIELD = "type"

#: CMHC Rental Market Survey `Quartier` names for the one Saguenay key: every
#: quartier the `Saguenay` centre publishes - the survey's four zones (Secteur
#: Nord, Chicoutimi-Sud, Jonquière, La Baie) broken into these eight. `Total`
#: rows are the workbook's own subtotals and are deliberately absent:
#: including one beside the quartiers it sums would count the city's stock
#: twice.
CMHC_QUARTIERS: dict[str, tuple[str, ...]] = {
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
