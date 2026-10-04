"""Where each council planning item is on the ground.

`items.py` reads a minute into text columns - the lot numbers it names, the
addresses, the zones - and `silver.council_planning_items` holds them as
jsonb arrays of strings. A string is not a place. This module is the join
from those strings to the three layers that have geometry, computed in
PostGIS where all of them already sit loaded and indexed, and written to
`silver.council_item_sites` (hbu_infra sql/032), one row per (item, site):

* a **lot number** against `rag.lots`. The cadastre writes ``1 303 691``
  and the parser strips the spaces, so the key is the digits; the newest
  snapshot carrying the number is the current cadastre.
* an **address** against `silver.lot_addresses`, on the civic number and
  the street folded by `silver.street_key` - the same fold hbu_rag_map
  applies to what a person types, so "chemin Ste-Foy" reaches "Chemin
  Sainte-Foy". A minute that drops the cardinal ("boulevard René-Lévesque"
  for a street the layer prints as Est and Ouest) is matched to the first
  door with that number on either, and the row says so. The parcel's
  polygon is kept rather than the point, so a distance is to the lot.
  Addresses are looked for in the borough's own city only: the municipality
  Adresses Québec prints, folded by `silver.place_key`, has to equal the
  city the borough resolves through (`partitions.cities.city_of`).
* a **zone code** against `rag.features`, in the namespace the borough's
  zoning layer was filed under, so a neighbouring arrondissement's zone
  that a sommaire names still resolves.

`compute_council_item_sites` runs the three joins as one `INSERT ... SELECT`
through `warehouse.upsert_select` - staging, merge, prune, the same path the
other PostGIS joins take - and returns what it found and what it did not.
`read_council_item_sites` reads the partition back as a GeoDataFrame, for
the parquet the asset keeps beside the table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from hbu_dataplatform.core.postgis import _fetch_partition, _require_relations
from hbu_dataplatform.core.warehouse import upsert_select
from hbu_dataplatform.partitions.cities import city_of
from hbu_dataplatform.rag.documents import ZONING_SOURCES

if TYPE_CHECKING:  # pragma: no cover
    import geopandas as gpd
    from psycopg import Connection

#: Every relation the join reads, with the file that creates it.
_SITE_RELATIONS: tuple[tuple[str, str], ...] = (
    ("silver.council_planning_items", "hbu_infra sql/032_silver_council_planning_items.sql"),
    ("silver.council_item_sites", "hbu_infra sql/035_silver_council_item_sites.sql"),
    ("rag.lots", "hbu_infra sql/002_spatial.sql"),
    ("rag.features", "hbu_infra sql/002_spatial.sql"),
    ("silver.lot_addresses", "hbu_infra sql/028_silver_lot_addresses.sql"),
)

#: The columns `_SITES_SELECT` produces, in its order - the target's, less
#: `loaded_at`, which defaults.
SITE_COLUMNS: tuple[str, ...] = (
    "scrape_date", "neighborhood", "doc_id", "item_index",
    "site_kind", "site_key", "is_subject", "match_basis",
    "lot_number", "lot_uid", "feature_id", "source_table", "geom",
)

#: The municipality each city's address points print, folded as
#: `silver.place_key` folds it. Adresses Québec prints the amalgamated city.
_MUNICIPALITY_KEYS: dict[str, str] = {
    "montreal": "montreal",
    "quebec": "quebec",
    "saguenay": "saguenay",
}

_SITES_SELECT = """
WITH items AS (
    SELECT i.doc_id, i.item_index,
           i.lot_numbers, i.subject_addresses, i.addresses,
           i.subject_zone_codes, i.zone_codes
      FROM silver.council_planning_items i
     WHERE i.neighborhood = %(neighborhood)s
       AND i.scrape_date = %(scrape_date)s::date
),
-- The current cadastre: the newest snapshot carrying each lot number,
-- whichever borough loaded it, keyed on the digits.
lots AS (
    SELECT DISTINCT ON (replace(l.lot_number, ' ', ''))
           replace(l.lot_number, ' ', '') AS digits,
           l.lot_number, l.lot_uid, l.geom
      FROM rag.lots l
     WHERE l.geom IS NOT NULL
     ORDER BY replace(l.lot_number, ' ', ''), l.scrape_date DESC
),
lot_sites AS (
    SELECT i.doc_id, i.item_index,
           'lot'::text AS site_kind,
           ln.value AS site_key,
           true AS is_subject,
           'lot_number'::text AS match_basis,
           l.lot_number, l.lot_uid,
           NULL::text AS feature_id, NULL::text AS source_table,
           l.geom
      FROM items i
     CROSS JOIN LATERAL jsonb_array_elements_text(i.lot_numbers) AS ln(value)
      JOIN lots l ON l.digits = replace(ln.value, ' ', '')
),
-- Every address an item names, with the civic number and the folded street
-- lifted out of "355, boulevard René-Lévesque Ouest". A range or a list
-- ("189-193", "189, 191 et 193") takes its first number.
named AS (
    SELECT i.doc_id, i.item_index,
           a.value AS site_key,
           (i.subject_addresses ? a.value) AS is_subject,
           nullif(substring(a.value from '^\\s*(\\d{1,5})'), '')::integer AS civic,
           silver.street_core(regexp_replace(a.value, '^[^,]*,\\s*', '')) AS street
      FROM items i
     CROSS JOIN LATERAL jsonb_array_elements_text(i.addresses) AS a(value)
),
-- The newest address layer of the borough's city, one row per door.
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
),
address_sites AS (
    SELECT DISTINCT ON (n.doc_id, n.item_index, n.site_key)
           n.doc_id, n.item_index,
           'address'::text AS site_kind,
           n.site_key,
           n.is_subject,
           CASE WHEN d.street = n.street THEN 'address' ELSE 'address_cardinal' END
               AS match_basis,
           l.lot_number, l.lot_uid,
           NULL::text AS feature_id, NULL::text AS source_table,
           l.geom
      FROM named n
      JOIN doors d
        ON d.civic_number = n.civic
       AND (d.street = n.street
            OR d.street IN (n.street || ' est', n.street || ' ouest',
                            n.street || ' nord', n.street || ' sud'))
      JOIN lots l ON l.digits = replace(d.lot_number, ' ', '')
     WHERE n.civic IS NOT NULL
       AND n.street <> ''
     -- The exact street over the cardinal-dropped one, the borough's own
     -- door over a neighbour's with the same number.
     ORDER BY n.doc_id, n.item_index, n.site_key,
              (d.street = n.street) DESC,
              (d.neighborhood = %(neighborhood)s) DESC,
              l.lot_number
),
-- The borough's zoning layer, by the namespace it was filed under, so a
-- zone the neighbouring arrondissement loaded still resolves.
namespaces AS (
    SELECT DISTINCT f.source_namespace, f.source_table
      FROM rag.features f
     WHERE f.neighborhood = %(neighborhood)s
       AND f.source_table = ANY (%(zoning_sources)s::text[])
),
zones AS (
    SELECT DISTINCT ON (f.feature_id, f.source_table)
           f.feature_id, f.source_table, f.geom
      FROM rag.features f
      JOIN namespaces ns
        ON ns.source_namespace = f.source_namespace
       AND ns.source_table = f.source_table
     WHERE f.geom IS NOT NULL
     ORDER BY f.feature_id, f.source_table, f.scrape_date DESC
),
zone_sites AS (
    SELECT DISTINCT ON (i.doc_id, i.item_index, z.value)
           i.doc_id, i.item_index,
           'zone'::text AS site_kind,
           z.value AS site_key,
           (i.subject_zone_codes ? z.value) AS is_subject,
           'zone'::text AS match_basis,
           NULL::text AS lot_number, NULL::bigint AS lot_uid,
           f.feature_id, f.source_table,
           f.geom
      FROM items i
     CROSS JOIN LATERAL jsonb_array_elements_text(i.zone_codes) AS z(value)
      JOIN zones f ON f.feature_id = z.value
     ORDER BY i.doc_id, i.item_index, z.value, f.source_table
),
sites AS (
    SELECT * FROM lot_sites
    UNION ALL
    SELECT * FROM address_sites
    UNION ALL
    SELECT * FROM zone_sites
)
SELECT %(scrape_date)s::date, %(neighborhood)s::text,
       s.doc_id, s.item_index, s.site_kind, s.site_key, s.is_subject,
       s.match_basis, s.lot_number, s.lot_uid, s.feature_id, s.source_table,
       s.geom
  FROM sites s
"""


def municipality_key(neighborhood: str) -> str:
    """The folded municipality the borough's address points print."""
    return _MUNICIPALITY_KEYS[str(city_of(neighborhood))]


def compute_council_item_sites(
    connection: "Connection",
    *,
    neighborhood: str,
    scrape_date: str,
) -> dict[str, Any]:
    """(Re)compute `silver.council_item_sites` for one (borough, date).

    Returns the counts worth reading in the asset's metadata: rows by site
    kind and by match basis, how many items have at least one site, and -
    the numbers to watch - how many lot numbers, addresses and zones the
    items name that reached nothing.
    """
    cursor = connection.cursor()
    _require_relations(cursor, _SITE_RELATIONS)
    params = {
        "neighborhood": neighborhood,
        "scrape_date": scrape_date,
        "municipality_key": municipality_key(neighborhood),
        "zoning_sources": list(ZONING_SOURCES),
    }
    loaded = upsert_select(
        cursor,
        "council_item_sites",
        SITE_COLUMNS,
        _SITES_SELECT,
        params,
        partition=neighborhood,
        scrape_date=scrape_date,
    )

    cursor.execute(
        """
        SELECT site_kind, match_basis, count(*)
          FROM silver.council_item_sites
         WHERE neighborhood = %(neighborhood)s
           AND scrape_date = %(scrape_date)s::date
         GROUP BY site_kind, match_basis
        """,
        params,
    )
    by_kind: dict[str, int] = {}
    by_basis: dict[str, int] = {}
    for kind, basis, count in cursor.fetchall():
        by_kind[kind] = by_kind.get(kind, 0) + int(count)
        by_basis[basis] = by_basis.get(basis, 0) + int(count)

    # What the items name against what reached the ground: distinct strings,
    # so one unloaded lot cited by five documents is one miss.
    cursor.execute(
        """
        WITH items AS (
            SELECT i.doc_id, i.item_index, i.lot_numbers, i.addresses, i.zone_codes
              FROM silver.council_planning_items i
             WHERE i.neighborhood = %(neighborhood)s
               AND i.scrape_date = %(scrape_date)s::date
        ),
        named AS (
            SELECT 'lot' AS kind, v.value AS key FROM items, jsonb_array_elements_text(lot_numbers) v
            UNION
            SELECT 'address', v.value FROM items, jsonb_array_elements_text(addresses) v
            UNION
            SELECT 'zone', v.value FROM items, jsonb_array_elements_text(zone_codes) v
        ),
        placed AS (
            SELECT DISTINCT s.site_kind AS kind, s.site_key AS key
              FROM silver.council_item_sites s
             WHERE s.neighborhood = %(neighborhood)s
               AND s.scrape_date = %(scrape_date)s::date
        )
        SELECT n.kind,
               count(*) AS named,
               count(*) FILTER (WHERE p.key IS NULL) AS unplaced,
               (array_agg(n.key ORDER BY n.key) FILTER (WHERE p.key IS NULL))[1:12] AS sample
          FROM named n
          LEFT JOIN placed p ON p.kind = n.kind AND p.key = n.key
         GROUP BY n.kind
        """,
        params,
    )
    named: dict[str, int] = {}
    unplaced: dict[str, int] = {}
    unplaced_sample: dict[str, list[str]] = {}
    for kind, count_named, count_unplaced, sample in cursor.fetchall():
        named[kind] = int(count_named)
        unplaced[kind] = int(count_unplaced)
        if sample:
            unplaced_sample[kind] = list(sample)

    cursor.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM silver.council_item_sites s
                    WHERE s.neighborhood = i.neighborhood AND s.scrape_date = i.scrape_date
                      AND s.doc_id = i.doc_id AND s.item_index = i.item_index
                      AND s.site_kind IN ('lot', 'address'))),
               count(*) FILTER (WHERE EXISTS (
                   SELECT 1 FROM silver.council_item_sites s
                    WHERE s.neighborhood = i.neighborhood AND s.scrape_date = i.scrape_date
                      AND s.doc_id = i.doc_id AND s.item_index = i.item_index))
          FROM silver.council_planning_items i
         WHERE i.neighborhood = %(neighborhood)s
           AND i.scrape_date = %(scrape_date)s::date
        """,
        params,
    )
    num_items, num_items_on_a_parcel, num_items_placed = cursor.fetchone()

    return {
        "loaded": loaded,
        "num_sites": sum(by_kind.values()),
        "sites_by_kind": by_kind,
        "sites_by_match_basis": by_basis,
        "num_items": int(num_items),
        "num_items_on_a_parcel": int(num_items_on_a_parcel),
        "num_items_placed": int(num_items_placed),
        "named": named,
        "unplaced": unplaced,
        "unplaced_sample": unplaced_sample,
    }


def read_council_item_sites(
    connection: "Connection", *, neighborhood: str, scrape_date: str
) -> "gpd.GeoDataFrame":
    """The partition as a GeoDataFrame, for the parquet beside the table."""
    return _fetch_partition(
        connection,
        (
            "s.neighborhood", "s.doc_id", "s.item_index", "s.site_kind", "s.site_key",
            "s.is_subject", "s.match_basis", "s.lot_number", "s.lot_uid",
            "s.feature_id", "s.source_table",
        ),
        "FROM silver.council_item_sites s "
        "WHERE s.neighborhood = %s AND s.scrape_date = %s::date "
        "ORDER BY s.doc_id, s.item_index, s.site_kind, s.site_key",
        [neighborhood, scrape_date],
        date_column="s.scrape_date",
        geometry_column="s.geom",
    )


__all__ = [
    "SITE_COLUMNS",
    "compute_council_item_sites",
    "municipality_key",
    "read_council_item_sites",
]
