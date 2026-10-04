"""Which borough a CUCQ decision belongs to.

The Commission is one body for the whole city and its minutes name no
arrondissement: a request is "467, 469, 471, Rue Arago Ouest" and nothing
more. Every silver table here is partitioned by borough, so a decision has
to be put on the ground before it can be published, and the only thing it
offers to be placed by is its address.

That is the join `council.sites` already makes for a planning item - the
civic number against `silver.lot_addresses`, the street folded on both
sides by `silver.street_core`, a dropped cardinal matched to the first door
on Est or Ouest - and it is made here the same way, in the same SQL
functions, so an address that reaches a parcel from a council minute
reaches the same parcel from a CUCQ minute. What comes back is the door's
borough and the lot it stands on; what matches no loaded door stays
unplaced, is written to the parquet with no borough, and is counted. A
borough whose addresses have not been loaded (`lot_addresses` has not run
for it) therefore holds no CUCQ rows yet, and the asset's `unplaced`
metadata says how many are waiting.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hbu_dataplatform.cities.quebec_city.council.sites import municipality_key
from hbu_dataplatform.core.postgis import _require_relations

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Connection

#: Every relation the lookup reads, with the file that creates it.
PLACEMENT_RELATIONS: tuple[tuple[str, str], ...] = (
    ("silver.lot_addresses", "hbu_infra sql/028_silver_lot_addresses.sql"),
)

#: Quebec City's key in `partitions.cities`, for the municipality fold.
QUEBEC_CITY_KEY = "CIL"


@dataclass(frozen=True)
class Placement:
    neighborhood: str
    lot_number: str | None
    #: 'address' for the street as written, 'address_cardinal' when the
    #: minute dropped Est/Ouest/Nord/Sud and the first such door was taken.
    match_basis: str


_PLACE_SELECT = """
WITH named AS (
    SELECT key, civic, silver.street_core(street) AS street
      FROM unnest(%(keys)s::text[], %(civics)s::integer[], %(streets)s::text[])
           AS n(key, civic, street)
),
doors AS (
    SELECT DISTINCT ON (p.address_id)
           p.civic_number,
           silver.street_core(p.street_name) AS street,
           p.lot_number, p.neighborhood
      FROM silver.lot_addresses p
     WHERE p.civic_number IS NOT NULL
       AND p.street_name IS NOT NULL
       AND silver.place_key(p.municipality) = %(municipality_key)s
     ORDER BY p.address_id, p.scrape_date DESC
)
SELECT DISTINCT ON (n.key)
       n.key, d.neighborhood, d.lot_number,
       CASE WHEN d.street = n.street THEN 'address' ELSE 'address_cardinal' END
  FROM named n
  JOIN doors d
    ON d.civic_number = n.civic
   AND (d.street = n.street
        OR d.street IN (n.street || ' est', n.street || ' ouest',
                        n.street || ' nord', n.street || ' sud'))
 WHERE n.street <> ''
 ORDER BY n.key, (d.street = n.street) DESC, d.lot_number
"""


def place_addresses(
    connection: Connection,
    addresses: Iterable[tuple[str, int, str]],
) -> dict[str, Placement]:
    """``{key: Placement}`` for every ``(key, civic, street)`` that reaches a
    loaded Quebec City door. Keys that reach none are absent."""
    rows = [(key, int(civic), street) for key, civic, street in addresses if street]
    if not rows:
        return {}
    cursor = connection.cursor()
    _require_relations(cursor, PLACEMENT_RELATIONS)
    cursor.execute(
        _PLACE_SELECT,
        {
            "keys": [key for key, _, _ in rows],
            "civics": [civic for _, civic, _ in rows],
            "streets": [street for _, _, street in rows],
            "municipality_key": municipality_key(QUEBEC_CITY_KEY),
        },
    )
    return {
        key: Placement(neighborhood=neighborhood, lot_number=lot_number, match_basis=basis)
        for key, neighborhood, lot_number, basis in cursor.fetchall()
    }


__all__ = ["PLACEMENT_RELATIONS", "QUEBEC_CITY_KEY", "Placement", "place_addresses"]
