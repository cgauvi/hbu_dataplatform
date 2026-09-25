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

Three assets on the borough axis, one job (`council_minutes_job`), one make
target:

```
make council-minutes NEIGHBORHOOD=CIL COUNCILS=12      # Montcalm alone
make council-minutes NEIGHBORHOOD=CIL                  # every CIL council
```

| Asset | Output |
| --- | --- |
| `bronze/council_minutes` | One row per minute: the PDF fetched and flattened to text, the meeting date the listing states, the links the PDF carries |
| `bronze/council_minutes_documents` | One row per document the minutes trail to — fiche, sommaire, resolution, report — with the hop it was reached by |
| `silver/council_planning_items` | One row per planning item read out of either, as columns — and `silver.council_planning_items` (hbu_infra `sql/030`) |

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
| `decision` | `adopted` ("il est résolu d'adopter le Règlement"), `draft_adopted`, `notice_of_motion`, `consultation`, first match wins so an extract's operative words beat a sommaire's recommendation listing every stage |
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

## What it is for, and what it is not yet

A zoning grid states what a zone permits *today*. This table is the record of
what is being asked to change and what was decided — the one the grids are
silent about, and the one that says where the by-law is moving before the
grid workbook does. Joining `subject_zone_codes` onto
`silver.zoning_grid_columns.feature_id` puts an amendment's history on the
zone a lot sits in; joining `subject_addresses` onto `silver.lot_addresses`
puts it on the parcel. Neither join is made yet, and nothing downstream reads
the table.

The reading is regular expressions, chosen so a reader can check every value
against its excerpt and so the table rebuilds from bronze without a model in
the loop. It is a harvest rather than an understanding: a minute that
discusses a project without numbers yields a row with nulls, and a sentence
the patterns were not written for yields nothing. The place to grow it is
`council_items.py`'s patterns, with a fixture in `tests/fixtures/council/`
for each new shape — the four there are the worked case's minute, sommaire,
extract and report, flattened exactly as bronze flattens them.
