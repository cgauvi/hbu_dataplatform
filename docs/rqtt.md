# The RQTT — Quebec's road network

The *Référentiel québécois du transport terrestre*, the MRNF's province-wide
land transport network and the renamed AQréseau+. Since 2026-09-16 it is the
only street source this platform reads, and it replaced three municipal layers
at once:

| City | Was | Now |
|---|---|---|
| Montreal | [`geobase-double`](https://donnees.montreal.ca/dataset/geobase-double) — one line per *side* of street, 91,546 features | the RQTT |
| Quebec City | [`vque_18`](https://www.donneesquebec.ca/recherche/dataset/vque_18) — 28,691 centre lines | the RQTT |
| Saguenay | [`sag-reseau-routier`](https://www.donneesquebec.ca/recherche/dataset/sag-reseau-routier) — 7,683 centre lines | the RQTT |

Three publishers agreeing on nothing but the fact of being roads meant three
slugs, three pairs of id and name columns and three branches in
`street_network`. One publication with one schema means a fourth city needs a
bounding box and no street code at all.

## The archive

```
https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Vectoriel/Theme/Local/RQTT/OGC(GPKG)/RQTT_GPKG.zip
```

| | |
|---|---|
| Zip | 408,695,811 B (390 MB), one member `OGC(GPKG)/RQTT.gpkg` |
| Unpacked | **1.27 GB** |
| Published CRS | **EPSG:3798** — NAD83 / MTQ Lambert |
| Licence | CC BY 4.0 |
| Reissued | three times a year — April, July, December |

Nine layers. Only **`Reseau_routier`** (MULTILINESTRING) is read; `Route_Verte`,
`Sentiers_motoneige_FCMQ`, `Sentiers_quad_FQCQ`, `Route_Blanche`,
`Reseau_ferroviaire`, `Reseau_ferroviaire_PN`, `Ponts` and `Transport_aerien`
are not. A bike path or a snowmobile trail through a borough would otherwise
read as a roadway.

The GeoPackage is **unpacked before it is read**, never opened through `zip://`
— SQLite reads by seeking, and a seek inside a deflate stream decompresses from
the start of the member every time. `urban_rag.role_foncier` makes the same
choice for the same reason, and `urban_rag.rqtt` is deliberately its twin.

## The vintage problem

The URL carries **no version**. It always serves whatever is current, unlike
the roll's `ROLE2026_GEOPACKAGE.zip`. So the cache cannot be keyed by the
published filename — every vintage would collide on one name and a stale copy
would be reused forever.

It is keyed on the `Last-Modified` the server reports instead, resolved by a
`HEAD` before any body is pulled, and cached as `RQTT_GPKG_20260703.zip`. A new
vintage lands beside the old one under its own name, and the vintage travels
onto every row as `rqtt_version` and into the asset's metadata.

That is the most this source allows: **an old vintage cannot be re-fetched**
once the MRNF rotates it, because there is no URL for it. What a partition can
say is which one it was built from.

The date axis stays monthly. A source reissued three times a year does not
change what a partition key means — "the month this was scraped" is still true
when two consecutive months scrape the same vintage — and most months find the
cache already holding what the `HEAD` names and move no bytes at all.

## No municipality column

The roll can push `code_mun IN (...)` into OGR as an attribute filter. The road
network has nothing equivalent: `Gestion` names the managing authority in prose
(`Municipal` for 86,671 of the Montreal box, `Transports Québec` for 5,280,
`Privé` for 917), not as a code.

So the cut is **spatial**. The GeoPackage ships an R-tree on the geometry, and
`street_network` reads one bounding box per city — taken from that city's whole
outline in `reference_neighborhoods`, so adding a borough never invalidates a
snapshot taken before it. `rqtt.bbox_in_source_crs` is the only way that box is
built, because OGR reads a spatial filter in the *dataset's* CRS: handing it
lon/lat does not raise, it quietly matches nothing.

## The columns

```
fid, geom, IdRte, Version, NomRte, NoRte, ClsRte, CaractRte, Cls_CheFor,
Cls_RAT, Cls_Sepaq, An_Classi, No_Chefor, CarRte, Che_Multi, Gestion,
Source, Notes, Dossi_Pont, Cap_Port, Stat_Pont, AQRP_UUID, Shape_Length
```

**The key is `AQRP_UUID`, not `IdRte`.** `IdRte` is the obvious-looking choice
and is wrong: over the 93,521 segments in the Montreal box of the 2026-07-03
vintage it has 47 nulls and 93,474 distinct values, so it is neither complete
nor unique and fails `street_assets._require_unique_streets` outright.
`AQRP_UUID` is 93,521 distinct, exactly one per row. `IdRte` still travels, in
`attributes`.

`NomRte` is the street name — 12,312 distinct in the box, with 231 segments
unnamed.

`ClsRte` is a road hierarchy this platform has never had before. Nothing reads
it yet; it is carried into `silver.neighborhood_streets.attributes` because an
arterial is a different development frontage from a local street, and it is
free to keep.

| `ClsRte` | Montreal box |
|---|---|
| Locale | 61,602 |
| Collectrice municipale | 13,260 |
| Artère | 11,369 |
| Autoroute | 3,722 |
| Nationale | 2,045 |
| Régionale | 856 |
| *(none)* | 236 |
| Sans classe | 205 |
| Collectrice | 203 |
| Rue piétonne | 19 |
| Liaison maritime | 4 |

**There is no `Ruelle` class**, and only 50 segments in the box carry "Ruelle"
in their name — the named laneways, not Montreal's thousands of back lanes. So
the lane exclusion that `postgis.DEFAULT_ROAD_LOT_MIN_STREET_M` got for free
from the géobase double's coverage survives the change of source. It survives
by *coverage* rather than by a rule, so it is worth re-checking each vintage.

## What is filtered out, and what deliberately is not

`rqtt.roadway_only` drops `Liaison maritime` by class and `Traverse`,
`Passerelle piétonnière`, `Passerelle piétonnière et cycliste`,
`Pont ferroviaire` and `Passage pour pipeline` by `CaractRte`. Each of those
would otherwise run a line through a parcel that carries no roadway and make it
a road lot.

**Highways and ramps stay**, and that is the part worth stating. They are the
tempting things to drop — nobody fronts on an autoroute — and dropping them
would be a bug. The street line's first job is to identify which parcels are
*roadway*, and `hbu.cadastral_road_lots` is what stops the solver being handed
one to develop. A highway with no line inside it is not a road lot, and its
parcel becomes a development site. Whether a lot abutting it earns frontage is
a question the exact shared-edge measure answers on its own.

A missing class keeps the row: `Sans classe` and the 236 with no class at all
are still streets somebody drives on, and the filter's job is to remove what is
positively known not to be a road, not to demand a label before believing one.

## What it produced, measured

The 2026-09-01 snapshot of the 2026-07-03 vintage, in 44 seconds:

```
RQTT 20260703: 125177 segment(s) across montreal 86017, quebec 26789,
               saguenay 13418 (1047 not roadway, 0 duplicate)
```

Cut into VSMPE and compared against the géobase double's first snapshot of the
same borough:

| | géobase double | RQTT |
| --- | --- | --- |
| rows in the borough | 4,262 sides | **2,190 segments** |
| length | 445.8 km | **247.0 km** |
| cut at the boundary | 170 | **159** |

Both roughly halve, which is the whole of the change: two lines per street
became one. The length ratio (0.55) sits a little above the row ratio (0.51)
because a curb side runs the longer way around a corner. No unnamed segments,
no duplicate ids, no invalid geometry, and **no segment named *Ruelle*** in the
borough at all. Its classes: Locale 1,280, Artère 551, Collectrice municipale
187, Autoroute 95, Nationale 75, Sans classe 2.

## What the change of geometry costs

The exact frontage measure is the boundary a lot *shares* with a road parcel
and uses no street line at all, so it is untouched. See
[street-frontage.md](street-frontage.md). What the line does is identify road
lots and name the street, and a centre line does both — more reliably for the
first, since a curb side hugs the boundary the road parcel shares with its
neighbours while an axis cannot.

The one thing still outstanding is the **fallback reach**. 8 m and 16 m were
chosen against curb lines: eight to clear the road-widening strips while
staying under half a Villeray roadway, sixteen for parcels set back further. A
centre line sits half a carriageway further from the lot, which inverts both
halves of that argument at once. Those two numbers are still the géobase
double's and want measuring against a cut partition —
`DEFAULT_FRONTAGE_FALLBACK_BUFFERS_M` says so, and
`FrontageConfig.fallback_buffers_m` is how a run tries other values.

## Downloading it

`make streets DATE=<YYYY-MM-01>` after `make quartiers` — the read is bounded by
each city's outline, so the outlines have to exist first.

On a machine behind TLS interception a single streaming GET may stall under a
megabyte. Ranged requests are not affected; `scripts/fetch_rqtt.sh` pulls the
archive in 16 MB pieces, six at a time, and checks each one's length before
concatenating.
