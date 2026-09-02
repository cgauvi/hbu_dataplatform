# What a lot is assessed at

Infolot draws the lot. It does not say what stands on it, who owns it, or what
it is worth — those are the *rôle d'évaluation foncière*, the assessment roll
every Quebec municipality files and the MAMH republishes as open data:

- [donneesouvertes.affmunqc.net/role/ROLE2026_GEOPACKAGE.zip](https://donneesouvertes.affmunqc.net/role/ROLE2026_GEOPACKAGE.zip),
  572 MB zipped, one 2.8 GB GeoPackage for the province.

Three assets over it. `property_assessment_roll` snapshots it,
`assessment_units` makes it readable, and `lot_assessed_values` carries it onto
the cadastre.

## Five layers, two of them read

The GeoPackage holds one feature class and four tables, all keyed on
`id_provinc` — the municipality's five-digit code followed by the unit's
18-character matricule:

| Layer | Rows (province) | What it is | Read |
| --- | --- | --- | --- |
| `rol_unite_p_2026` | 3 747 008 | One point per *unité d'évaluation*, at the unit's visual centre. EPSG:4269 | ✅ |
| `b05v_unite_evaln_2026` | 3 747 176 | The characteristics: values, area, storeys, year built, dwellings, use code | ✅ |
| `b05v_lot_cadst_2026` | 5 595 995 | The cadastre lot numbers the unit covers, one to many | ✅ |
| `b05v_adr_unite_evaln_2026` | 3 819 572 | Civic addresses, one to many | — |
| `b05v_repar_fisc_2026` | 1 626 132 | Fiscal breakdowns and exemptions, one to many | — |

The two that are not read are one-to-many against the unit and answer no
question this pipeline asks. `UNREAD_LAYERS` names them and the asset reports
them in its `layers_not_read` metadata, so what was dropped is on the record
rather than inferred from what is absent.

`b05v_lot_cadst` is the one that makes the lot join possible at all, and it
keeps its own grain — one row per (unit, lot) — in its own file. It is not
merged into `assessment_units`, which is one row per unit; the two are put
together where the lot join happens.

The layer names carry the roll year, so they are resolved by prefix rather than
written out: next year's archive is the same five layers ending `_2027`, and a
hard-coded name would fail on the one line guaranteed to change. The year
itself is `RoleResource.roll_year`, defaulting to `$URBAN_RAG_ROLL_YEAR`.

## Two things the archive forces

**It is unpacked to disk.** `urban_rag.bdoi` hands its zipped shapefiles
straight to GDAL through `zip://` and never writes them out. That cannot work
here: a GeoPackage is a SQLite database, and SQLite reads it by *seeking* —
inside a deflate stream, every seek means decompressing from the start of the
member again. The unpacked 2.8 GB copy lands beside the archive in
`data/cache/role/`, and both are keyed by filename and shared across every
scrape date, the same posture as the BDOI extracts and the CMHC workbook. A
published roll year is final; only the first run of a year pays for it.

**It is filtered in OGR, not in memory.** The province is 3.7 million
assessment units against Montreal's 437 thousand. `municipality_codes` becomes
an OGR attribute filter (`code_mun IN ('66023')`) rather than a mask applied to
a frame afterwards, so the rows outside it are never built — the difference
between ~300 MB of memory and a few gigabytes. Scoping a province-wide source
to the territory being modelled is a bound on what was asked for rather than an
interpretation of what came back, which is what keeps this in bronze; it is the
same move `cmhc_vacancy_survey` makes when it keeps the Montreal CMA out of a
national survey.

Set it to `[]` for the whole province, or add codes for the other on-island
municipalities — Westmount, Mont-Royal, Côte-Saint-Luc and the rest file their
own rolls and are not boroughs:

```bash
make roll DATE=2026-08-26                    # Ville de Montréal, the default
make roll DATE=2026-08-26 CODE_MUN='[]'      # the province
```

Geometry is reprojected to EPSG:4326 on the way in, the way BDOI's is. NAD83 is
close enough to WGS 84 to look right on a map and about a metre and a half
away — enough to move a point across a lot line, which is exactly what the
third asset joins on.

## Putting the two layers back together

`assessment_units` merges them on `id_provinc`, which is 1:1 on both sides
(437 192 rows each for Montreal in the 2026 roll). The grain is checked rather
than assumed on both files, because a duplicate would multiply the merge and
then double a lot's total downstream — the kind of error that reads as a
plausible number rather than as a crash.

`code_mun` and `mat18` are published in *both* layers — `id_provinc` taken
apart — so they are dropped from the characteristics side rather than suffixed.
Checked first: two spellings of one municipality code is a thing for silver to
refuse, not to pick a winner for. `identifiant` is in both and the two do *not*
agree, so it is kept as `identifiant_unite`; `arrond` and its characteristics-
side name `rl0102a` both survive, since bronze vocabulary is not reconciled
away.

The result is as wide as the bronze snapshot — the roll has no borough axis, so
this asset is partitioned by **date alone**, like `street_network` and the two
CMHC surveys, and the parquet it writes stays province-wide.

There are then *two* borough cuts downstream of it, and they answer different
questions. `lot_assessed_values`, one asset later, cuts against the **cadastre**
— which lot is this unit's value counted on. `silver.assessment_units` cuts
against the **borough outline** — which borough is this unit in — and that one
happens inside this asset, on the way to Postgres, because a table partitioned
by borough needs a borough and the roll supplies none. See
[Both have a table](#both-have-a-table-and-one-of-them-fills-several-partitions-at-once)
below.

## The use code, and what it says

`rl0105a` is the CUBF — four digits classifying what the property is *for*, and
the single most useful thing the roll says about a parcel after its value. All
437 192 Montreal units state one, and 598 distinct codes are in use.

The roll does not say what any of them mean. A unit reads `4611` and stops
there. The list that says `4611` is *Garage de stationnement pour automobiles
(infrastructure)* is Annexe 2C.1 of the **Manuel d'évaluation foncière du
Québec**, published on its own as a single spreadsheet:

- [CUBF_MEFQ.xlsx](https://cdn-contenu.quebec.ca/cdn-contenu/adm/min/affaires-municipales/publications/evaluation_fonciere/manuel_evaluation_fonciere/CUBF_MEFQ.xlsx),
  185 kB, linked from
  [quebec.ca](https://www.quebec.ca/habitation-territoire/information-fonciere/evaluation-fonciere/manuel/codes-utilisation-biens-fonds).

`cubf_use_codes` snapshots it and `assessment_units` looks it up onto every
unit as `use_description`.

**The sheet is a hierarchy in one column.** `CUBF` holds one, two, three and
four characters on different rows, and the width is the level — `1`
(*RÉSIDENTIELLE*), `10` (*LOGEMENT*), `100` (*Logement*), then `1000`, the code
an assessor actually writes. Only the 1 260 four-character rows are use codes.
Bronze keeps all 1 705 rows because the headings are the classification the
codes hang off; silver selects the leaves.

Nothing is left-padded on the way. No CUBF begins with a zero — the categories
run 1 to 9 — so padding `100` to `0100` could only ever fabricate a code, and
would hand the *Logement* heading's name to whatever unit collided with it.

**The category is not always one digit.** `2-3` is a single row spanning both
leading digits: manufacturing is numbered 2000 through 3999. That is why the
column is read as text and why this pipeline maps a leading digit to an income
class in `urban_rag.comparables` rather than here — see the note on
`CUBF_CLASSES` in that module.

**The lookup is a left join, and eight rows say why.** The 2025 edition
describes 437 184 of Montreal's 437 192 units. The eight it misses are five
codes — 3190, 3410, 3860, 4815, 6394 — in force on a roll filed against an
earlier edition. The roll and the manual are amended on their own cadences, so
a retired code is an ordinary state: a null description and a kept property.
The run reports `num_use_codes_not_in_the_manual` and names each one, so the
gap is on the record rather than inferred. `9800` is the one code the manual
numbers and leaves undescribed — a slot held open for a use not yet named.

**Only the description is carried.** The sheet also publishes a SCIAN
correspondence and the manual's remarks, several of which run to a paragraph of
assessment instruction. Repeating those onto 437 thousand units would be
carrying the manual rather than reading it; a reader who wants them joins the
bronze file on the code.

**The roll archive ships the same file.** `ROLE2026_GEOPACKAGE.zip` carries
`CUBF_MEFQ.xlsx` beside the GeoPackage, byte-identical to the standalone
download (189 379 bytes, same SHA-256). The standalone one is read for what it
costs rather than for what it says: the codebook is then a 185 kB fetch that
can be re-run on its own instead of something recoverable only by unpacking
2.8 GB.

`use_description` travels from here into `silver.lot_assessment_comparables` as
`dominant_use_description` — the words for the unit carrying most of the lot's
value — and from there into `gold.lot_profiles`,
`gold.lot_redevelopment_gap` and `gold.lot_investment_opportunities`. Nothing
on that path looks a code up twice. It is for **reading, not filtering**: the
code stays the key, and two editions of the manual can word one code
differently.

## The lot join is by lot number, with the point as a fallback

`lot_assessed_values` places each unit on the lots the roll itself says it
covers — `b05v_lot_cadst` gives (unit, lot number), and `lot_key` is all that
stands between the roll's `"1243415"` and Infolot's `"1 243 415"` — then groups
by `NO_LOT` and sums `rl0404a` (VALEUR IMMEUBLE: land plus buildings, which the
roll also splits as `rl0402a` and `rl0403a`).

`rl0103b`, the lot-number suffix, is **not** part of the key: it distinguishes
rows of the *non-renewed* cadastre naming one renewed lot, and leaving it in
would count a unit twice on the same lot — 1 758 of Montreal's crosswalk rows
are exactly that shape.

**The crosswalk cannot place everything, and what it misses is not random.** A
condominium unit names its **private** lot numbers, and Infolot does not draw
those — the polygon there is the `PC-*` common parts. So the crosswalk alone
loses every divided co-ownership in the borough. On VSMPE that is 5 211 units
and **$2.92 B**, none of which name a lot that exists in the cadastre:

```
a PC-29987 unit names: 5346252, 5346253, 5346256, 5332344, …
in the VSMPE cadastre: (none)
```

Their point still falls squarely on the `PC-*` lot the tower stands on, so the
point places them. `place_unmatched_by_point` decides whether it may (default
on), and `num_units_by_point` says how many rows came that way — so a table can
be read back against the choice that produced it. Turned off, every row comes
from the roll's own statement and the towers are absent rather than
approximated: 21 862 lots valued instead of 22 443, and $24.26 B instead of
$27.24 B.

```bash
make lot-values DATE=2026-08-26                  # crosswalk + point fallback
make lot-values DATE=2026-08-26 BY_POINT=false   # crosswalk only
```

The two routes are genuinely complementary on real data — neither finds what
the other does, and together they beat both:

| VSMPE, 2026 roll | lot number only | point only | **both (default)** |
| --- | --- | --- | --- |
| lots valued | 21 862 | 21 676 | **22 443** |
| of which `PC-*` | 0 | 582 | 582 |
| lots only that route finds | 778 | 592 | — |
| units placed | 21 204 | 26 484 | **26 489** |
| total (apportioned) | $24.26 B | — | **$27.24 B** |

## Two totals, because one number cannot be both

43% of Montreal's units name two lots or more, so a group-by-sum has to decide
what a shared unit contributes. It reports **both**:

| Column | What it is | Sum it across lots? |
| --- | --- | --- |
| `total_assessed_value` | Every unit's whole value, on each lot it covers | **No** — over-counts |
| `total_assessed_value_apportioned` | Each unit's value ÷ the lots it covers | Yes |

The first answers "what is the property standing on this lot assessed at"; the
second is the one that adds up. On VSMPE they are $32.37 B and $27.24 B — a
$5.1 B gap, and one unit's $150 476 700 landing on four lots is $37 619 175
apportioned to each. 691 of the borough's units are shared this way.
`num_shared_units` counts the ones on a given lot, so the gap is attributable
per row; it is 0 for most lots, and the two totals are then equal.

Apportionment divides by the lots the unit covers **in the snapshot**, not by
the ones in this borough — a unit straddling a borough line contributes its
share here and the rest there.

**The sum is over units, not over buildings.** A divided-co-ownership building
is one unit per apartment, all on the one `PC-*` common-parts lot: `PC-29987`
carries 402 units and $257 660 500. That is the number a highest-and-best-use
question wants — what the ground is currently worth in aggregate — and it is
only readable as such because the count sits next to it.

A lot nothing is assessed on **keeps its row**, with `num_assessment_units = 0`
and **null** totals: a sum over nothing is not a value of zero. A few percent is
the honest reading of `num_lots_unvalued`; a third of the borough would mean the
cadastre and the roll disagree about where the ground is.

The cadastre's self-intersecting rings are repaired with `make_valid` before the
point fallback runs, and the count reported as `num_geometries_repaired` — the
same repair `building_lot_intersections` makes on the way into PostGIS, for the
same reason and made visible the same way.

> `rl0404a` is an **assessed value for taxation**, not a market appraisal, and
> Montreal's roll is triennial: every unit in a 2026 roll is valued as of the
> same reference date. The totals compare across lots; they do not track a
> market between rolls.

## Both have a table, and one of them fills several partitions at once

`lot_assessed_values` owns `silver.lot_assessed_values`
(`sql/013_silver_lot_assessed_values.sql`), like every other borough-scoped
silver asset — one row per `(scrape_date, neighborhood, lot_number)`, the
cadastre's other columns in the jsonb catch-all, upserted then pruned by
`urban_rag.warehouse` like all the rest. That file has no `-- requires:`
header, so it lands on the first `db.py init` and the asset is scheduled
normally.

`assessment_units` owns `silver.assessment_units`
(`sql/014_silver_assessment_units.sql`) and is the one asset in the platform
that does **not** publish the partition it was asked for. The roll has no
borough axis — it is one publication for the province, merged once — so the
asset stays partitioned by date and its parquet stays province-wide, and the
borough is read off the map instead: `assign_boroughs` puts every unit in the
borough whose `reference_neighborhoods` outline its point falls inside, and
`warehouse.publish_by_neighborhood` upserts all of them in one transaction.

That is the same cut `neighborhood_streets` makes on the island-wide geobase,
made against points rather than lines — and it is why **the tree and the table
do not hold the same rows**. The parquet carries every municipality
`municipality_codes` kept (the whole province with `CODE_MUN='[]'`); the table
carries the boroughs. `num_units_outside_every_borough` is the difference, and
it is a count rather than an error — Westmount and Laval file rolls too and are
not boroughs. A run in which *no* unit fell in any enabled borough fails
instead, the same refusal `neighborhood_streets` makes: that is a boundary that
did not load, not a province with no properties in it.

The roll does state a borough of its own — `arrond`, which is `REM` plus the
`no_arr` the reference layer carries. It travels as an ordinary column and is
never partitioned on: geometry is what the rest of this platform cuts on, so
geometry decides here too, and `num_units_arrond_disagrees` counts where the
two publishers part company so the choice stays visible.

The roll names its fields by MAMH code, so `warehouse.TABLES` carries by far
its longest column map — `rl0404a` → `assessed_value`, `rl0308a` →
`floor_area_m2`, and a dozen more. The forty-odd codes nothing reads land in
`attributes`, where a reader who knows the code can still reach them and a roll
that gains a field needs no migration.

The only remaining **documented absences** in `warehouse.TABLES` are
`document_embeddings` and `document_index`, both of which publish to
`rag.chunks`. `test_warehouse.py` asserts those two and only those two, so a
third absence cannot arrive quietly.


## What comes next

A total is half an answer. Whether $900,000 is a lot of money for a given lot
depends on what stands on it and what the lots around it are assessed at, and
neither survives a sum over `rl0404a` — so `lot_assessment_comparables` sums the
roll again, this time for the dwellings, the floor area and the use code, prices
them into a cap rate, and finds each lot its k nearest comparables. That is
[comparables.md](comparables.md), and it is the last silver asset before
`gold.lot_profiles`.
