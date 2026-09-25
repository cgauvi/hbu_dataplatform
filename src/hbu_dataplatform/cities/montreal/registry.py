"""Montreal's boroughs, and the crosswalks from a partition key into what
each of the city's publishers files that borough under.

A key here is a Montreal borough. It resolves into a Spectrum namespace, a
``no_arr`` code on the city's reference-neighborhood layer, CMHC's Montreal
quartiers and Cushman & Wakefield's submarkets. `hbu_dataplatform.partitions.axes.cities`
composes these with the other cities' registries into the one map every
asset switches on.
"""

from __future__ import annotations

#: The city's CKAN portal, where the reference neighborhoods and the
#: dwelling counts are published. Every other portal this pipeline reads is
#: Données Québec; this is the one that is not.
PORTAL_URL = "https://donnees.montreal.ca"

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
#: what bounds the cadastre query - see `hbu_dataplatform.sources.infolot.assets`.
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
#: partition, for `hbu_dataplatform.sources.cmhc.assets`. A third geography
#: keyed the same way as the two maps above, and it lines up with neither:
#: CMHC surveys the Montreal *census metropolitan area* and cuts it into its
#: own neighborhoods, which are finer than a borough in most cases (`VSMPE` is
#: three of them) and coarser in one (`PR`, below).
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

#: Cushman & Wakefield's MarketBeat submarket covering each borough, keyed by
#: partition key. The same kind of crosswalk `CMHC_QUARTIERS` is and applied
#: for the same reason: the publisher carves the island its own way, and a
#: borough-level rent is a much better answer than an island-level one.
#:
#: **One name for both sectors, matched loosely.** The office report writes
#: `Midtown North` and the industrial one writes `Montréal Midtown North` for
#: what is the same territory, so `submarket_for` matches on the name with any
#: leading `Montréal` taken off - see `hbu_dataplatform.cities.montreal.rents.marketbeat`.
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
#: `hbu_dataplatform.cities.montreal.rents.assets` says so when it prices one
#: off the Montreal market.
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


def submarket_for(neighborhood: str) -> str | None:
    """The MarketBeat submarket a borough sits in, or None for none.

    None rather than a raise, unlike `quartiers_for`: a borough C&W draws no
    submarket around is priced at the island-wide rent, which is a worse answer
    but a real one. A missing CMHC quartier is different - that crosswalk is
    the only route from the survey to the borough, and a gap there means a
    partition that cannot be computed at all.
    """
    return MARKETBEAT_SUBMARKETS.get(neighborhood)
