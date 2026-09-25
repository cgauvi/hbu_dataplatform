"""Données Québec, the provincial CKAN portal, as a Dagster resource."""

from __future__ import annotations

from hbu_dataplatform.core.resources import CkanResource
from hbu_dataplatform.sources.rfu.client import DEFAULT_BASE_URL as BASE_URL


class QuebecOpenDataResource(CkanResource):
    """Connection settings for Données Québec, where Quebec City publishes.

    The same CKAN API as the city of Montreal's portal - `CkanResource` with
    another base URL - and kept as its own resource for the reason
    `RfuResource` is: two publishers, two licences, two release cadences, and
    a run pointed at one should not be able to silently read the other. It is
    what `reference_neighborhoods` reads the arrondissement layer from and
    `street_network` the public ways.
    """

    base_url: str = BASE_URL
