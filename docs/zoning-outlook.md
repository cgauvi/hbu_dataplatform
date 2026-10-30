# The zoning-change outlook

"How likely is it that this lot can be rezoned for ten dwellings?" has no
probability in any table. What the platform holds is *precedent*: what the
city decided when asked the same thing about this zone and about the zones
like it, and how recently. Since 2026-10-26 four pieces turn that record into
an answer the chat can give, for Quebec City (`CIL`, `SSC`), where the
conseils de quartier minutes are read ([council-minutes.md](council-minutes.md)).

| Piece | Where | What it is |
| --- | --- | --- |
| `zone_dominance` | `silver.zoning_grid_columns` (hbu_infra `sql/014`) | What the publisher says a zone is *for*: Montreal's H/C/I/E prefix, Quebec City's two-letter *Dominante* (`Hb`, `Mc`), NULL for Saguenay. Zone-level, repeated on every column |
| `rag.similar_zones(zone, borough, n)` | `sql/038` | The zones like a given one: same city, same dominance, ranked by a distance over the dwelling groups, the cap, the density, the height, the implantation mode and the borough. `rag.zone_profiles()` is the shared one-row-per-zone reading both use |
| `gold.zoning_amendments`, `gold.zone_bylaw_status`, `gold.zone_site_activity` | `sql/039`, asset `gold/zoning_amendments` | One row per zoning decision folded from the planning items; the by-law in force on every zone's sheet; the CUCQ's decisions per zone |
| `rag.zoning_change_outlook(lot, target, years, peers)` | `sql/041` | One JSON document: the lot's zones and caps, the amendments in them, the peers and theirs, the activity, and the precedent rate with its nudges named |

hbu_rag_map's `zoning_change_outlook` tool is that function with a lot
resolved in front of it; see its README.

```
make council-minutes NEIGHBORHOOD=CIL        # the items the fold reads (hours)
make council-publish NEIGHBORHOOD=CIL        # the corpus, for the passages
make zoning-amendments NEIGHBORHOOD=CIL      # the fold (seconds)
```

## From seats to decisions

`silver.council_planning_items` is one row per *seat* an amendment was
described from - the council's agenda item, the fiche, the sommaire, the
notice of motion, the draft's adoption, the adoption, the consultation report.
The worked case (zone 14040Hb, 355 boulevard René-Lévesque Ouest, 8 to 10
dwellings) is nine rows. `cities.quebec_city.council.amendments.consolidate`
folds them:

* a seat is a candidate when its `item_kind` is `zoning_amendment`,
  `ppcmoi`, `conditional_use` or `minor_variance`;
* seats are linked when they share a sommaire or resolution number
  (`gpd_numbers`), an *amending* by-law number, or a subject zone named at
  the same assembly (a minute's agenda item and the trail documents that
  minute led to). Union-find; the links are keys, not similarity;
* a set is kept when it names a subject zone - or, for a site decision, an
  address or a lot. "Période de questions" saying *zonage* is left in the
  items table;
* **the by-law being amended is never a key.** A sommaire cites it on every
  amendment - `R.C.A.1V.Q. 4` is La Cité-Limoilou's urbanisme by-law,
  `R.V.Q. 990` the harmonisation by-law - and linking on it would fold the
  borough into one decision. `KNOWN_BASE_BYLAWS` lists them and any by-law
  cited at three or more assemblies joins them (`BASE_BYLAW_MEETINGS`);
  they are kept as `base_bylaw_numbers`;
* one row per set: `stage` is the furthest stage any seat states
  (consultation < notice of motion < draft adopted < adopted; refused is
  terminal) and `outcome` its fold to approved / refused / in_progress;
  `decision_date` is the latest date among the seats stating that stage;
  the caps come from the first seat stating an *after*, the consultation
  report preferred over the sommaire over the fiche over the minute
  (`SEAT_PREFERENCE`), and the council's opinion and vote table the same
  way; `citations` lists every seat with its chunks;
* `raises_cap` is the outlook's numerator: the cap after is above the cap
  before, or above the zone's current grid cap where no before was read
  (`current_max_dwellings`, from `rag.zone_profiles()` at fold time).

`amendment_id` is the smallest amending by-law number, else the smallest
sommaire number, else the first seat - so a re-fold upserts in place. On the
borough axis with the items.

What the first fold of Montcalm (2026-10-26) found: 9 decisions out of 380
items, the worked case exactly right, and one wrong number - the tourist
accommodation by-law `R.C.A.1V.Q. 218` reads as a 30 → 40 cap raise over 60
zones, where the sentence was about something else. The regex reader has no
model of what a sentence is about; the LLM fallback
([council-minutes.md](council-minutes.md)) is where that gets fixed.

## The sheet's own record

Quebec City's grid sheet prints, under the zone code, the by-law that last
amended the grid and its in-force date - `14040HbR.C.A.1V.Q. 549En vigueur le
2025-07-10`, as the text layer runs them together. `amendments.bylaw_status`
reads that line out of the sheet's chunks in `silver.document_chunks` into
`gold.zone_bylaw_status`: 543 of La Cité-Limoilou's zones carry one, dating
from 2010 to 2026. It is the one amendment record that exists for every zone,
minutes or not, and the outlook's "grid amended recently" nudge reads it.

## Zones like this one

`rag.similar_zones` is the one definition of "like it", so the outlook and a
reader checking it by hand rank the same peers. Candidates are the zones of
the same city with the same `zone_dominance` - a hard key, because the by-law
reads an `Hb` and an `Mc` zone under different chapters whatever their norms.
Within the class the distance is: +1 when the dwelling groups differ, up to +1
on the cap's relative gap (+0.5 on a one-sided NULL), up to +0.5 on the
density's, up to +0.5 on the height's, +0.25 when the implantation mode
differs, +0.25 for another arrondissement. The weights are constants at the
top of `sql/038`. For 14040Hb (H1, cap 10, 13 m) the nearest peers are the
other `Hb` zones with cap 10 and 13 m at distance 0, then those capped at 8
or 12 at 0.2.

## The score

`rag.zoning_change_outlook` returns a *precedent rate*: among the `peers`
nearest zones, the share that had a cap-raising amendment approved inside the
window, each weighted `0.5 ^ (age in years / 3)`. Then the nudges, each listed
in `score.modifiers` by name and effect: a raise approved in the lot's own
zone (+0.15), one in progress (+0.20), one refused (−0.15), the target at or
under the median cap the precedents reached (+0.10) or beyond it (−0.10), a
heritage or PIIA screen on the zone (−0.10), a grid amended in the last two
years (+0.05). Clamped to [0, 1]; low below 0.10, moderate below 0.30, high
from there. When the primary zone's cap already reaches the target the tier
is `already_allowed` and there is nothing to amend.

It is a frequency over a small set and the document says how small
(`score.peer_count`, `score.peers_with_approved_raise`). The tool prints it
that way, with every amendment it rests on numbered for citation, and says
that a zone with no amendment on record may simply have no minute loaded.

## What it does not yet do

* **Montreal.** No borough council minutes are scraped, and the PPCMOI
  resolutions and the grids' *MISES À JOUR* lists are in the corpus as prose
  only. Readers for those would feed the same `gold.zoning_amendments`.
* **Plans.** PPU, PDUES and the plan d'urbanisme are not a signal yet; the
  VSMPE layers are scraped and their PDFs are not fetched.
* **The map.** The outlook is a chat tool; the Lot pane does not show it.
