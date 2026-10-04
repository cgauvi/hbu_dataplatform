# Council minutes

Quebec City has a *conseil de quartier* per neighbourhood — thirty of them,
three to nine per arrondissement — and each files the minutes of its monthly
assembly as a PDF. When the city amends a zoning by-law it asks the council
for an opinion and holds the public consultation at the council's assembly, so
the minutes are where an amendment first surfaces in prose: the zone, the
address, what is asked and what the neighbourhood answered. Since 2026-09-25
the pipeline reads them, follows them to the decision behind each one, and
harvests what all of it says about zoning, lots and dwellings into one silver
table. Montcalm, in La Cité-Limoilou (`CIL`), was the first council run.

Six assets on the borough axis, two jobs, two make targets:

```
make council-minutes NEIGHBORHOOD=CIL COUNCILS=12      # Montcalm alone: fetch, chunk, embed, read, place
make council-minutes NEIGHBORHOOD=CIL                  # every CIL council
make council-publish NEIGHBORHOOD=CIL                  # load the corpus into rag.chunks
```

| Asset | Output |
| --- | --- |
| `bronze/council_minutes` | One row per minute: the PDF fetched and flattened to text, the meeting date the listing states, the links the PDF carries |
| `bronze/council_minutes_documents` | One row per document the minutes trail to — fiche, sommaire, resolution, report — with the hop it was reached by |
| `silver/council_minutes_chunks` | Both cut into retrieval chunks, filed under `council_*` source tables — and into `silver.document_chunks` beside the grids' |
| `silver/council_planning_items` | One row per planning item read out of either, as columns, with `citations` and `outcome` — and `silver.council_planning_items` (hbu_infra `sql/030`, `032`); plus every item put on the ground — `sites.parquet` and `silver.council_item_sites` (`sql/032`) |
| `silver/council_minutes_embeddings` | One bge-m3 vector per council chunk |
| `gold/council_minutes_index` | Those vectors upserted into `rag.chunks` beside the zoning grids' (`council_minutes_index_job`) |

## Where the minutes are

The city's page for a council's minutes
(`.../conseils_quartier/montcalm/proces-verbaux.aspx`) is a shell whose list is
an iframe served by a second host:

```
https://affichagesite.villequebec.quebec/conseil-quartier/proces-verbaux/12
```

One `<h2>` per year, one `<a href="/fichiers/<guid>">` per assembly, the date in
the link's `title` ("Procès verbal du 16 juin 2025"). Montcalm's lists 47
minutes from January 2021 to April 2026, 165 kB to 2 MB each. The `12` is the
council's id on that host and is published nowhere the page shows; the
registry, `quebec_council.NEIGHBORHOOD_COUNCILS`, was built by probing ids 1–35
and reading the one line each answer prints ("... du Conseil de quartier de
Montcalm"). Ids 16 and 32 up answer an empty page.

**La Cité-Limoilou's nine councils are placed from the city's quartier map and
Montcalm has been run; the other twenty-one boroughs' councils are placed from
the same map and not yet fetched.** A council in the wrong borough would fetch
cleanly and file under a partition it does not belong to, so a borough's first
run is worth a look at its `councils` metadata.

A minute is a PDF exported from a word processor. Its hyperlinks survive as
`/Link` annotations carrying a `/URI` action while the visible text says only
"voir la fiche", so `links` is read off the annotations
(`quebec_council.pdf_links`) plus any city URL spelled out in the text, which
is how the consultation report cites its fiche. The text is flattened with
`read_pdf(keep_hyphens=True)`: the zoning grids' flattening drops a line-end
hyphen as a syllable break, which for a minute turns *Cité-Limoilou* into
*CitéLimoilou* and `GT2025-\n233` into `GT2025233`; prose keeps it.

## The trail

From a minute to the decision it reports on is three hops, each on a different
host of the same city:

```
minute  ──▶  fiche.aspx?IdProjet=917          (www.ville.quebec.qc.ca, HTML)
                 ├─▶  gpdblob/GT2025-233.pdf   (gpddocs, the sommaire décisionnel)
                 │        ├─▶  gpdblob/CA1-2025-0215.pdf   (the resolution adopting the by-law)
                 │        ├─▶  gpdblob/CA1-2025-0145.pdf   (the draft's adoption)
                 │        └─▶  gpdblob/AM1-2025-0146.pdf   (the notice of motion)
                 ├─▶  CPFichierAzure.ashx?Fichier=<guid>.pdf   (the consultation report)
                 └─▶  CPFichierAzure.ashx?Fichier=<guid>.pdf   (the presentation)
```

`council_minutes_documents` walks that breadth-first, three hops deep, one row
per canonical URL, remembering every minute that led to it — two assemblies
about one amendment both point at its fiche. `quebec_council.classify_link` is
the whole policy of what is followed: those hosts and paths and nothing else,
because a minute also links YouTube, the snow-removal page and the borough's
newsletter (`num_links_ignored`). The fiche is fetched live every time — it
gains its report and its adoption date after the assembly — and is kept as a
row of its own (`kind` `fiche`) because its *Démarche de participation
publique* states the adoption date before any resolution is filed. The PDFs go
through the same URL-keyed cache as the zoning grids: a filed minute or a
sommaire never changes.

What the sommaire bundles is worth knowing: the *fiche de modification
réglementaire* (the zone's description, the request, the proposed change, the
file number), the by-law's text and notes, the **amended grille de
spécifications**, a plan extract labelling the zone and its neighbours, and
the preliminary conformity opinion. Its last page lists the resolutions taken
on it with their dates.

## The reading

`hbu_dataplatform.cities.quebec_city.council.items` turns text into a `PlanningItem`, and
`council_planning_items` writes one row per item. Two grains meet:

* **a minute is cut into its agenda items** — the numbered headings the
  procès-verbal repeats from the ordre du jour — and only the items whose text
  matches a planning vocabulary are kept: `zoning_amendment`, `ppcmoi`,
  `minor_variance`, `demolition`, `conditional_use`, `planning`, `heritage`,
  `housing`, decided in that order. The treasury and the crosswalk resolution
  are counted under `num_agenda_items_dropped`;
* **every trail document is one item**, since each is filed about one decision.

The agenda cut is the fragile part. A minute prints the ordre du jour and then
the body under the same numbering, nests numbered lists inside items (the
treasury's five payments, a resolution's three demands), loses a heading to a
bullet or a page break, and occasionally repeats a number. So every "1." opens
a candidate chain; a chain accepts the next number, the one after, or the same
one again; a chain that starts inside another's span is a nested list and is
dropped; and among what remains within two headings of the longest, the latest
wins — the body follows the ordre du jour. A minute whose agenda cannot be cut
is read as one item rather than lost.

The fields, every one a regular expression with its excerpt kept:

| Column | Read from |
| --- | --- |
| `subject_zone_codes` / `zone_codes` | The zones the item is *about* — its title, "relativement à la zone", "zone visée", "dans la zone" — against every `\d{5}[A-Z][a-z]` it carries. The sommaire's plan extract labels 25 zones; one is the subject |
| `bylaw_numbers` | `R.C.A.1V.Q. 549`, `R.V.Q. 978`, respelled one way however the PDF spaced them |
| `gpd_numbers` | `GT2025-233`, `CA1-2025-0215`, `AM1-2025-0146` |
| `file_number` | The fiche de modification's *N° de dossier* |
| `subject_addresses` / `addresses` | Civic addresses in the title and the first 400 characters, against every one in the text — the assembly's venue and the council chamber are in the second and not the first |
| `lot_numbers` | Seven-digit lot numbers after "lot" |
| `usage_groups` | `H1`, `C2`: the groups named after "groupe d'usages" |
| `dwelling_changes` | Every "de N à M" about dwellings, scoped `zoning` when "maximal", "autorisé", "limite" or "règlement" is within reach and `project` otherwise; `max_dwellings_before` / `_after` are the first zoning-scoped one, `project_dwellings` the first project-scoped one or "un total de N logements" |
| `max_height_m`, `storeys` | "13 m de hauteur", "hauteur permise, qui est de 13 m"; "trois étages" |
| `decision` | `adopted` ("il est résolu d'adopter le Règlement"), `draft_adopted`, `approved` ("il est résolu d'accorder", "la demande de démolition est acceptée", "autorise la démolition"), `refused` ("il est résolu de refuser", "la demande est refusée"), `notice_of_motion`, `consultation`, first match wins so an extract's operative words beat a sommaire's recommendation listing every stage. A by-law is adopted; a request — a demolition, a dérogation mineure, a PPCMOI — is granted or refused |
| `outcome` | The decision folded to what "approved or rejected" means: `adopted` and `approved` → `approved`, `refused` → `refused`, the three stages short of a decision → `in_progress`, nothing stated → null. The council's opinion is **not** folded in — it advises, the arrondissement decides |
| `decision_date` | The sitting on an extract ("tenue le lundi 7 juillet 2025"), the assembly on a report, "Date :" on a sommaire |
| `council_opinion` | `favorable`, `favorable_with_conditions`, `unfavorable`, read off the sentence where the council speaks — "est d'accord", "recommande", "s'oppose" — and kept in `council_opinion_excerpt` |
| `votes` | The consultation report's table: A accept, B refuse, C accept with adjustments, abstention |

Nothing is guessed. A count the pattern does not reach is null, and
`parse_notes` says when a zone was named and no cap read.

### The worked case

Zone 14040Hb, 355 boulevard René-Lévesque Ouest, Montcalm — the owner of a
two-dwelling house asks to enlarge it to ten, the zone caps a building at
eight. Five documents describe it and the table holds all five:

| Row | `source_kind` | `decision` | `council_opinion` | `max_dwellings` |
| --- | --- | --- | --- | --- |
| Minute of 2025-06-16, item 4 *Consultation publique et demande d'opinion* | `minutes` | `consultation` | `favorable_with_conditions` — "est d'accord ... et recommande par ailleurs les modifications suivantes" | 8 → 10, project 10, 13 m |
| Fiche 917 | `fiche` | `consultation` | — | 8 → 10 |
| Consultation report | `consultation_file` | `consultation` (2025-06-16) | `favorable`, votes A 0 / B 0 / C 9 | 8 → 10, project 10, 13 m |
| GT2025-233, the sommaire | `gpd` | `notice_of_motion` | — | 8 → 10, H1, file 5809, by-law R.C.A.1V.Q. 549, resolutions CA1-2025-0215 / CA1-2025-0145 / AM1-2025-0146 |
| CA1-2025-0215, the extract | `gpd` | `adopted` (2025-07-07) | — | — |

Two things the case shows about the reading. The minute's opinion is
*conditional* and the report's is *favorable*: the minute's sentence carries
"recommande par ailleurs les modifications suivantes" and the report's, 400
characters long, ends before its "avec les modifications suivantes" — the
report's vote table (C 9, *accepter avec proposition d'ajustement*) is the
truer signal, and it is kept. And the minute misspells the zone once as
"140040 Hb", which no pattern reads as a zone; the correct spelling two
paragraphs up is what `subject_zone_codes` carries.

## From an item to the ground, and to the corpus

A zoning grid states what a zone permits *today*. This table is the record of
what is being asked to change and what was decided — the one the grids are
silent about, and the one that says where the by-law is moving before the
grid workbook does. Two joins make it askable, and the same asset makes both.

**The sites.** `silver.council_item_sites` (hbu_infra `sql/032`,
`council.sites`) is one row per (item, site), computed in PostGIS once the
items are in their table: every lot number an item names against `rag.lots`
— the cadastre writes `1 303 691` and the parser strips the spaces, so the
key is the digits — every address against `silver.lot_addresses` on the
civic number and the street folded by `silver.street_key` (the fold
hbu_rag_map applies to what a person types, so the minute's "chemin
Ste-Foy" reaches the layer's "Chemin Sainte-Foy"; a dropped cardinal takes
the first door with that number on Est or Ouest and says so), every zone
code against `rag.features` in the borough's namespace. A lot or an address
site carries the *parcel's* polygon, so a distance is to the lot; a zone
site carries the zone's, and is kept apart by `site_kind` because a zone is
within 500 m of most of itself. `is_subject` says whether the item is about
the site or merely names it — the assembly's venue is an address too.

What reaches nothing is counted, not written: the asset's `unplaced`
metadata says how many of the lots, addresses and zones the items name
found no ground, with a sample. On Montcalm that is mostly addresses outside
Quebec City (a mémoire about 7665 boulevard Lacordaire) and lots in a
borough whose cadastre is not loaded.

**The citations.** Each item carries `citations`, a jsonb of what states it:
the PDF's url and `doc_id`, `source_kind`, `document_number`, and
`chunk_ids` — the rows of `rag.chunks` whose span covers the agenda item. A
minute's items also carry `trail`, every document the minute led to; a trail
document carries `minutes`, the minutes that led to it. The chunk ids are
the bridge to the corpus below, and `rag.search_council_chunks` (`sql/033`)
walks it from the other side.

Together they answer the question the grids cannot:

```sql
-- Demolitions decided within 500 m of 439 rue Jeanne-d'Arc, last year
SELECT item_date, outcome, council_opinion, title, distance_m, url
  FROM rag.council_items_near(-71.23612, 46.80413, 500,
                              ARRAY['demolition'], ARRAY['approved', 'refused'],
                              since => current_date - interval '1 year');
```

hbu_rag_map's `council_decisions_near` tool is that call with an address
resolved in front of it; see its README.

## The corpus

The minutes and their trail are chunked and embedded into `rag.chunks`
beside the zoning grids, by the same three steps `hbu_dataplatform.rag`
takes — `council_minutes_chunks`, `council_minutes_embeddings`,
`council_minutes_index` — with three differences, all in `council.corpus`:

* **the text is repaired first.** `read_minutes` reads a span of
  `repair_text(text)`, so the chunks are cut from that repaired text too and
  an item and a chunk meet by character offset. `chunk_spans` finds each
  chunk in the text it came from on its first and last twelve words, any
  whitespace between — because a paragraph the tokenizer had to split comes
  back with its line breaks as spaces — and a chunk found neither way takes
  the previous one's end and is marked as a guess.
* **the title names the document.** "Procès-verbal du conseil de quartier
  de Montcalm, 16 juin 2025", "Sommaire décisionnel GT2025-233 — …",
  "Résolution CA1-2025-0215". `rag.chunks.title` is what the lexical arm of
  the search weights highest, so a sommaire is found by its number.
* **`source_table` is the document kind** — `council_minutes`,
  `council_gpd`, `council_fiche`, `council_consultation_file` — so a reader
  can search the minutes alone, and `rag.search_council_chunks` finds the
  council corpus by the prefix. `feature_ids` carries the zone codes the
  chunk itself names: the grids' "which zones cite this" column read as
  "which zones this passage is about", which is what lets a question
  naming 14040Hb reach the minute that discussed it the way it reaches the
  zone's sheet.

Two things the loads do differently from the grids', and both on purpose.
The chunks go into `silver.document_chunks` through `upsert_frame` with the
prune off and a prune of the council rows alone done by hand, because
`publish`'s prune drops every row of the (borough, date) the frame does not
carry — which would be the zoning corpus of the same partition. And
`council_minutes_index` loads with `prune=False`: `load_partition`'s prune
drops the borough's *older* scrape dates, which is right when the two
corpora land on the same date and would delete the grids when the council
run lags them by a month. `document_index` prunes; this one does not.

## What it is not yet

The reading is regular expressions, chosen so a reader can check every value
against its excerpt and so the table rebuilds from bronze without a model in
the loop. It is a harvest rather than an understanding: a minute that
discusses a project without numbers yields a row with nulls, and a sentence
the patterns were not written for yields nothing. The place to grow it is
`council_items.py`'s patterns, with a fixture in `tests/fixtures/council/`
for each new shape — the four there are the worked case's minute, sommaire,
extract and report, flattened exactly as bronze flattens them.
