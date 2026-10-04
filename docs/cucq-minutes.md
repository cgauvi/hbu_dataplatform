# CUCQ minutes

The *conseils de quartier* are consulted; the **Commission d'urbanisme et de
conservation de Québec** decides. It is the body that rules on a permit in
Quebec City's heritage sectors — the *secteurs assujettis* of Vieux-Québec,
Saint-Roch, Saint-Jean-Baptiste, Montcalm, Sillery, the Trait-Carré, the
Vieux-Bourg and the rest — and, since the 2023 demolition by-law
(R.V.Q. 3117), on every demolition of a building in them. The council
corpus ([council-minutes.md](council-minutes.md)) holds Montcalm's
discussion of that by-law's consultation and no per-building demolition
decision, because the decisions are the Commission's and it files its own
minutes. Since 2026-10-04 the pipeline reads them: one row per permit
request the Commission ruled on, with its address, its works and its
verdict, placed on a borough and cut into the corpus.

Three assets on the **date axis**, one job, one make target:

```
export CUCQ_SEARCH_QUERY_KEY=...                    # the portal's queryKey, see below
make cucq-minutes DATE=2026-10-01                   # everything the portal lists, about a gigabyte once
make cucq-minutes DATE=2026-10-01 SINCE_YEAR=2024   # the by-law era
```

| Asset | Output |
| --- | --- |
| `bronze/cucq_minutes` | One row per sitting: the PDF fetched and flattened to text, what the portal's index says about it, the links it carries. A scan is a row with `has_text` false |
| `silver/cucq_decisions` | One row per decision — a list entry or a hearing item — with its request number, applicant, address, works, dwellings, verdict and resolution; placed on a borough by its address and published per borough into `silver.cucq_decisions` (hbu_infra `sql/037`) |
| `silver/cucq_minutes_chunks` | Each placed decision as one document of the corpus, chunked, filed under `cucq_*` source tables into `silver.document_chunks` beside the grids' and the councils' |

The axis is the date and not the borough because the Commission is one body
for the city and its minutes name no arrondissement: a request is "467, 469,
471, Rue Arago Ouest" and nothing more. The borough is found afterwards,
from the ground, and one date partition publishes every borough it placed a
decision in — the posture `assessment_units` takes with the province-wide
roll.

## Where the minutes are

The CUCQ's page on the city's site links *Procès-verbaux* to the decisions
portal:

```
https://decisions.ville.quebec.qc.ca/index.html?inst=cucq&type=proces-verbaux
```

The portal is a page over an **Azure Cognitive Search** index. Its
`js/scripts.js` names the service, the index and a *query-only* key and
hands all three to every browser; every search the page runs is a GET on

```
https://srch-gpd-p.search.windows.net/indexes/stgpdprod01-index/docs
```

with that key in an `api-key` header. `cucq.portal.CucqPortalClient` makes
the same requests. The key is the portal's public read credential and the
index is read-only to it, but it is a credential all the same, so it is not
in the code: set **`CUCQ_SEARCH_QUERY_KEY`** in the environment (or `.env`)
to the `queryKey` the page's `js/scripts.js` carries — the production
`AzSearch.Automagic({...})` line — or pass `query_key` to
`CucqMinutesResource`. A rotation by the city is a new value there. The
index holds
some 208,000 of the city's decision documents; the Commission's are the
`Instance` facet *Commission d'urbanisme et de conservation de Québec* and
the `Type` *Procès-verbaux*: **1,309 sittings from 2000-01 to 2026-08**,
about fifty a year, as of 2026-10. The string facets are stored
percent-encoded (`Proc%C3%A8s-verbaux`, `Commission%20d%27urbanisme...`)
because the page decodes them for display, so a filter spells them that way
too (`portal.odata_literal`) and a listing decodes them back. A listing is
`$orderby=Date desc`, `$top=1000`, paged with `$skip`.

A listed document is `metadata_storage_name` on the blob host
`gpddocs.ville.quebec.qc.ca/gpdblob/` — the host the council trail already
fetches its *sommaires* from — and goes through the same URL-keyed cache: a
filed minute never changes. Two series: `PV_CUCQ_OR_<date>.pdf` is a
regular sitting (1,291 of them) and `PV_CUCQ_DEM_<date>.pdf` a sitting of
the *comité de démolition* the Commission has held monthly since 2024-01
for the heritage demolitions the by-law sends to a public hearing (18).

**The minutes before 2013 are scans.** No text layer in the PDF, and none in
the index's own `content` field either, which is the same extraction. Bronze
keeps them as rows — the listing is data, and the OCR backlog should be a
count in the table rather than a gap in it — with `has_text` false and the
reader's reason in `read_error`; silver reads past them and counts them
under `num_minutes_textless`. The text is read with
`read_pdf(keep_hyphens=True)`, as a council minute is, which has one cost
below.

## What a sitting's minutes say

A regular sitting's procès-verbal is two documents in one PDF. The body is
the same every week: quorum, the members who withdraw for a conflict of
interest (naming the requests), the previous minutes approved, then three
blanket resolutions —

> 6. Demandes de permis approuvées — Résolution C.U. 2026-098 — il est
> unanimement résolu d'approuver, telles que présentées, les demandes de
> permis apparaissant sur la liste identifiée « demandes approuvées » …

the same for *approuvées conditionnellement* (C.U. 2026-099) and *il est
résolu de refuser* for *refusées* (C.U. 2026-100) — an *Auditions* item
naming the requests whose applicants were heard, a *Conseil local du
patrimoine* item, and the close. The annex is the list those resolutions
point at: *Liste des demandes ayant été présentées à la Commission*, under
each of the three headings one entry per request — its number
(`20250428-007`: the date it was filed and a sequence), the applicant, the
address, the permit category and sometimes a description:

```
Demande refusée
Devmico Construction Inc.20250428-007
467, 469, 471, Rue Arago Ouest
Démolition d'un bâtiment principal
Démolition du bâtiment principal pour reconstruction
```

**The list is the decision**; the body says only which resolution carried
it. A sitting lists twenty to seventy requests, most of them windows, doors,
cladding and signs in the heritage sectors — and among them the demolitions,
the new buildings and the enlargements that change what stands on a lot.

A committee sitting's minutes are prose: one numbered item per request —
`5.1. 289, avenue Plante, demande no 20250721-011` — with the publication
period and the comments received, the objectives of R.V.Q. 3117 and
R.V.Q. 1324 recited, the *considérants* the commissioners gave (the
building's heritage interest, the condition of its structure, the cost of
keeping it, the replacement project), and the resolution: *il est résolu
d'approuver sous condition la demande visant la démolition du bâtiment sis
sur le lot 1 941 776 … correspondant au 289, avenue Plante*, or *de
refuser*. The lot number is there, which a list entry never states.

## The reading

`cucq.minutes.read_sitting` returns one `Decision` per request, by regular
expression with nothing guessed — a field the pattern does not reach is
null and `parse_notes` says what was expected and not found. The fields:

| Column | Read from |
| --- | --- |
| `request_number` | `20250428-007`, glued to the applicant's line or listed in a column (below); on a hearing item, "demande no" in its heading |
| `applicant` | The line the number is glued to, or the line before the address |
| `address_line`, `addresses`, `civic_numbers`, `civic`, `street`, `is_block` | The line after the applicant, taken apart: "467, 469, 471, Rue Arago Ouest" is three doors on one street; "116-A, 116" two tokens and civic 116; "229-1/2" is 229; "1-389" is unit 1 at 389; "243 à 305, Rue Gérard-Morisset (Bloc)" is a block. A street without a type word ("Le Trait-Carré Ouest") is accepted behind a number; a bare street ("Rue des Dominicaines") has no civic and is kept, unplaceable |
| `works`, `works_kind`, `is_demolition` | The lines after the address, classified demolition first — `demolition_main` (a main building, whole or part), `demolition_accessory`, `demolition_other` (a sign, a chimney), then `new_construction`, `enlargement`, `subdivision`, `sign`, `special_authorization`, `exterior_renovation`, `other`. A request that demolishes and rebuilds is a demolition, which is the question this source exists to answer |
| `dwellings_min`, `dwellings_max`, `project_dwellings` | The band the permit category names ("de 1 à 3 logements", "9 logements et plus") and the project's own count when the description gives one |
| `decision`, `decision_label`, `outcome` | The list heading the entry sits under — `approved`, `approved_conditionally`, `refused` — or a hearing's operative words, which can also `deferred` or leave an item `heard`. `outcome` folds to the council table's three: approved / refused / in_progress |
| `resolution_number` | "C.U. 2026-100", the blanket resolution of the entry's list, read off the body; "CD-2025-011" on a hearing item |
| `lot_numbers` | Seven-digit lot numbers the text names — a hearing item always does |
| `was_heard` | The *Auditions* item named the request |
| `text` | A list entry as one paragraph (`decision_text`: the sitting, the request, the applicant, the works, the verdict), a hearing item's own prose |

The sitting's `sitting_number` ("2026-25", "No Séance : 2025-06") and date
come off the body; the listing's date wins over the body's, because the
minute of 2026-06-23 says *mercredi 25 juin 2026* of a Tuesday sitting the
list and the file name both date the 23rd. The slip is noted, not believed.

### The two layouts of the list

The list is a table, and its text layer comes out one of two ways. Most
pages glue the request number to the end of the applicant's line
(`Devmico Construction Inc.20250428-007`) — or, in 2017, to its start
(`20170606-039 FAUCHER, CHRISTIAN`) — and an entry is simply the lines
from one such line to the next. Other pages list every number of the page
first, as a column, and the entries after it:

```
20260503-014
20260507-052
...
Labrosse, Marc
3819, Rue de Toulouse
Agrandissement d'un bâtiment résidentiel de 1 à 3 logements
Gestion GP inc
569, Rue Dollard
...
```

There is no marker where an entry begins, so an entry is recognised by its
**address line** — a civic number and a street type — and the line before
it is the applicant. A bare street is taken as an address only when short
and capitalised, so a works line that happens to open on *terrasse,
escalier ou toute autre construction similaire* is not an entry. The
numbers are then paired to the entries **per page**, in order, and only when
the page's numbers and its unnumbered entries come out even: the two
columns print in the same order, so an even count pairs them exactly, and
an uneven one means a number fell off the page — pairing in order from
there would give every following entry its neighbour's number. Those
entries are left without one, and noted. A number can also land *after*
its entries, in the page footer (`20260616-056 Page 4 de 6`), which is why
a page is what lies between two of the list's own headers and not what
lies before its footer.

Four more things the extraction does, each met in the 2025–2026 minutes.
An applicant's line can end in the civic number that then prints again on
the address line (`MAISON RICHELIEU … INC. 2808,`), which is taken off. The
source occasionally cuts an address at its civic number — `122-`, `805-` —
which `keep_hyphens` then glues to the works line (`805-Ajout,
agrandissement …`); the civic is kept and the street is null. A number can
be glued to the page footer (`Page 3 de 620240730-061`), and is read out of
it. And some entries are set in a TrueType subset the PDF carries no
character map for, so their text layer is the glyph ids read as characters
— `12-99$YHQXHGHVeUDEOHV` for an avenue, `9994XpEHFLQF` for a numbered
company. `minutes.is_garbled` tells such a line by its symbols, its case
changes or its want of vowels (every lower-case vowel lands on a consonant
in that shift), and the entry is kept — its number paired, its address
unreadable and said so in `parse_notes` — rather than lost, which would
have thrown the page's whole number column off by one. A page whose
heading was lost yields entries with `decision` `other` and a note; a page
that lost its line breaks altogether (2025-08-06) yields nothing and is
counted.

Measured over the 86 sittings of 2025 and 2026 at the time of writing:
85 have a text layer, 11 of those are the body alone with no annexed list
(the city filed several minutes twice, the second copy under the next
sitting's name and without its list), and the other 74 yield 2,666
decisions — 470 approved, 2,061 approved conditionally, 87 refused — of
which 124 (under 5%) are left without a request number by an uneven page
and 113 (4%) name a street with no civic number and cannot be placed.

## From a decision to a borough, and to the corpus

**The placement.** `cucq.placement.place_addresses` is the join
`council.sites` makes for a planning item's address, made once for every
distinct door the decisions name: the civic number against
`silver.lot_addresses`, the street folded on both sides by
`silver.street_core`, a dropped cardinal matched to the first door on Est
or Ouest and said so in `match_basis`. What comes back is the door's
borough and the lot it stands on. The parquet keeps every decision, placed
or not; `publish_by_neighborhood` loads the placed ones, one borough
partition each; the asset's `unplaced` metadata counts the rest with a
sample. **A borough whose addresses are not loaded holds no CUCQ rows
yet** — the Commission's sectors are mostly in La Cité-Limoilou, Sainte-Foy–
Sillery–Cap-Rouge, Charlesbourg and Beauport, and `lot_addresses` has to
have run for a borough before its decisions can land. A decision on a
street with no civic number is unplaceable by construction and is counted
apart.

**The corpus.** A sitting is sixty requests on sixty addresses, so the
corpus document is the *decision*, not the minute: each placed decision's
paragraph is chunked with the same cut the grids and the council minutes
get, under a `chunk_doc_id` of its own (the minute's URL and the decision's
index, so it is stable across runs), titled so the lexical arm of the
search finds it by its street — *CUCQ, séance du 23 juin 2026 — 467, 469,
471, Rue Arago Ouest — Démolition d'un bâtiment principal — refusée* — and
filed under `source_table` `cucq_minutes` or `cucq_demolition_committee`,
so a reader can ask for the Commission's decisions by the `cucq_` prefix
the way `council_` finds the councils'. A decision's passages are the chunks
whose `doc_id` is its `chunk_doc_id`; no citations column is needed. The
upsert into `silver.document_chunks` is `upsert_frame` with the prune off
and a prune of the `cucq_%` rows alone done by hand, for the reason the
council chunks do the same: `publish`'s prune would drop the borough's
other two corpora.

**What is not done yet.** The embeddings and the load into `rag.chunks` —
the two steps `council_minutes_embeddings` and `council_minutes_index`
take — are not taken here. The chunks in `silver.document_chunks` are where
the RAG picks this source up; the two assets to add are the council pair
with `cucq_minutes_chunks` as their upstream, and `rag.search_council_chunks`
has a sibling to write over `silver.cucq_decisions` instead of the items.

## What it is not

The reading covers the minutes with a text layer: 2013 on for the regular
sittings, every committee sitting. The thirteen earlier years are scans and
wait for OCR. Within a sitting the reading is a harvest, not an
understanding: the works are the permit category as printed and a kind
decided by vocabulary, the dwellings are what the category says, and a
hearing item's *considérants* are kept as text rather than read into
columns. The place to grow it is `cucq/minutes.py`'s patterns, with a
fixture in `tests/fixtures/cucq/` for each new shape — the eight there are
real minutes flattened exactly as bronze flattens them: both list layouts,
a list-only sitting of 2017, a 2020 sitting mixing the two, and three
committee sittings.
