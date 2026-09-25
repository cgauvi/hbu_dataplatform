# Quebec City

The pipeline was written around Montreal's publishers. Since 2026-09-16 it
also scrapes Quebec City, one arrondissement at a time, through the same
assets and the same partition key - a borough is a borough whichever city
files it. This page is what is different about the second city: where each
input comes from, what is translated on the way in, and which of its inputs
are another city's.

## The key, and the two registries

A neighborhood partition key resolves through `hbu_dataplatform.partitions`, which
now carries a *city* for every key it knows: `city_of("VSMPE")` is
`City.MONTREAL`, `city_of("CIL")` is `City.QUEBEC`. Quebec City's six
arrondissements are keyed by the three-letter abbreviation the city itself
publishes on its arrondissement layer - `CIL` (La Cité-Limoilou), `RIV`
(Les Rivières), `SSC` (Sainte-Foy–Sillery–Cap-Rouge), `CHA` (Charlesbourg),
`BEA` (Beauport), `HSC` (La Haute-Saint-Charles) - so there is no crosswalk to
maintain between the key and the layer.

Every asset that reads a city-specific source consults `city_of` rather than
assuming Montreal. The ones that read a province-wide source - Infolot's
cadastre, StatCan's building footprints, the MEFQ assessment roll and its
CUBF codebook - work unchanged, bounded by the borough outline the way they
always were; only the outline's source moves.

## Sources

| What | Montreal | Quebec City |
|---|---|---|
| Borough outline | `donnees.montreal.ca` reference neighborhoods, dissolved on `no_arr` | Données Québec [Arrondissements](https://www.donneesquebec.ca/recherche/dataset/vque_2), one polygon per arrondissement, cut on `ABREVIATION` |
| Zoning geometry | Spectrum, `<ns>/Reglement_urbanisme/VSP_REG_ZONE` | The city's ArcGIS Online feature service, layer 2 *Zonage en vigueur* of `CI_AMENAGEMENT_ENVIRONNEMENT` - the layer the city's own [interactive map](https://experience.arcgis.com/experience/aadd65187ef64037bd13170afb450e15) draws |
| Zoning norms | One PDF *grille des usages et des normes* per zone, linked from the zone table and parsed by `hbu_dataplatform.zoning_grid` | One workbook for the city, [Grille de spécifications du zonage](https://www.donneesquebec.ca/recherche/dataset/grille-de-specifications-du-zonage), one row per zone, read by `hbu_dataplatform.quebec` |
| Zoning prose | The same PDF, chunked and embedded into the corpus | The city's map server generates a *grille de spécifications* sheet per zone at `GrillesZonage/HandlerZonage.ashx?<zone>`; the URL is built from the zone code rather than published, and the corpus is that sheet |
| Council minutes | None — a Montreal borough has no *conseil de quartier* | The minutes of each of the arrondissement's councils, listed per council by `affichagesite.villequebec.quebec`, and the consultation fiches, *sommaires décisionnels* and resolutions they link to — read into `silver.council_planning_items`, see [council-minutes.md](council-minutes.md) |
| Heritage | Whatever heritage tables the borough's Spectrum namespace publishes | Layers 5-11 of the city's `CI_COMMUNAUTE_CULTURE_PATRIMOINE` feature service - what the [patrimoine bâti](https://www.ville.quebec.qc.ca/citoyens/patrimoine/bati/index.aspx) search and its fiches draw - see [Heritage](#heritage) |
| Streets | The RQTT, bounded to the island | The same RQTT, bounded to Quebec City — one source for both since it replaced `geobase-double` and `vque_18` together ([rqtt.md](rqtt.md)) |
| Lots, buildings, roll, CUBF | Province-wide | The same, unchanged |
| CMHC vacancy and rents | The Montréal centre of the national survey and the Montréal CMA's HMIP page | The Québec centre of the same workbook and the Québec CMA's page (HMIP geography 1400) |
| Commercial rents | Cushman & Wakefield's Montreal MarketBeats | None published: the Montreal whole-market row, flagged as a proxy on every row |
| Construction costs | Montreal's column of the cost guide | The guide prices no Quebec City market: Montreal's column, unflagged |
| RFU factor | Montreal's row, reported in metadata | Quebec City's row, reported beside it |

Both open-data snapshots are written *beside* the Montreal ones, under the
same date partition: `reference_neighborhoods` writes
`arrondissements_quebec.parquet` next to `quartiers.parquet`. The streets are
no longer written per city at all: `street_network` writes one
`street_segments.parquet` covering every city, cut out of the province-wide
RQTT. A snapshot taken before 2026-09-16 has no arrondissement file, and a
Quebec City partition asked to cut against it fails naming
`reference_neighborhoods` - re-materialize that month's `quartiers` and
`streets` first.

The zoning lands where Montreal's does, as two tables under
`bronze/neighborhood_features/<date>/CIL/`: `Zonage__ZONAGE_EN_VIGUEUR`
(the polygons, keyed by `IGDS_TEXT_STRING`, the zone code) and
`Zonage__GRILLE_SPECIFICATIONS` (the workbook rows for those codes, with the
workbook's own column names). The zone code's leading digit is the
arrondissement, which the run's metadata tabulates - a borough fetch bounded
by its outline picks up the neighbours' zones along the line, as every
borough-bounded read here does.

### The two readings of the same norms

The workbook and the per-zone sheet are not alternatives, and the pipeline
uses both. The workbook is the machine-readable one: `grid_columns` turns a
row into the `GridColumn`s the solver needs, and `envelopes` reads nothing
else. The sheet is where the by-law's *prose* is - the notes at the foot of a
grid, the conditional usages, the PIIA and heritage mentions - none of which
the workbook has a column for, and all of which is what retrieval answers
from. Until 2026-09-16 only the workbook was read and `rag.chunks` held no
Quebec City row at all, so "Retrieved passages" was empty for every CIL lot.

The sheet's URL is built, not published: the polygons carry no link column, so
`assets._quebec_features` formats
`quebec.DEFAULT_SHEET_URL_TEMPLATE` against each zone code and writes the
result as `LIEN_GRILLE` - the column name Montreal's zone table uses and
Saguenay's asset already borrows. From there the corpus assets are the same
three for all three cities. **A zone code the handler does not know answers 200
with an 847-byte blank PDF rather than a 404** (a real sheet is 110-125 KB),
which is why the template is only ever applied to a code that came off the
zoning layer itself. A blank one costs nothing anyway: it has no text layer, so
`read_pdf` refuses it and `linked_documents` files it under `failures` like any
dead link.

### Heritage

The *patrimoine bâti* search and its fiches are a front end over an open
ArcGIS service on the same organisation as the zoning,
`CI_COMMUNAUTE_CULTURE_PATRIMOINE`. `_quebec_features` reads seven of its
layers, bounded by the borough outline like the zones, one parquet each beside
them:

| Layer | Slug | City-wide rows | Id |
|---|---|---|---|
| 11 *Bâtiment étudié* | `Patrimoine__BATIMENT_ETUDIE` | 15,801 building footprints | `NO_SEQ`, the fiche number |
| 5 *Immeuble cité (municipal)* | `Patrimoine__IMMEUBLE_CITE` | 12 | `OBJECTID` |
| 6 *Immeuble classé (provincial)* | `Patrimoine__IMMEUBLE_CLASSE` | 103 | `OBJECTID` |
| 7 *Lieu, bâtiment ou monument désigné (fédéral)* | `Patrimoine__DESIGNE_FEDERAL` | 96 | `OBJECTID` |
| 8 *Site patrimonial cité* | `Patrimoine__SITE_CITE` | 4 | `OBJECTID` |
| 9 *Site patrimonial déclaré ou classé* | `Patrimoine__SITE_DECLARE_CLASSE` | 14 | `OBJECTID` |
| 10 *Aire de protection* | `Patrimoine__AIRE_PROTECTION` | 7 | `OBJECTID` |

The id is copied to `ID`, one of `FEATURE_ID_COLUMNS`, which is what carries
the layers into `rag.features` and `silver.lot_features`. The status layers'
register link (`LIEN_URL_GOUVERNEMENTALE`) would be the better key but is blank
on 16 of their 236 rows. Each coded field gets a `<field>_LIBELLE` column with
the domain's label beside the code, and a studied building gets its fiche as
`LIEN_FICHE` - not `LIEN_GRILLE`, which the corpus would download.

**The grade is not an ordinal.** `EVALUATION_VALEUR_PATRIMO_NO` is 1
exceptionnel, 2 supérieur, 3 bon, 4 faible - and then 5 *présumé* and 6
*confirmé*, an interest presumed or confirmed on a building never graded: 62%
of the layer city-wide. `quebec.UNGRADED_CODES` names them. `DEMOLI`,
`FICHE_ETAT` and `STATUT_ACTIF` hold one value on every row. The construction
period, style and photos are on the fiche page only and are not read.

Nothing downstream reads these tables yet: the site-thesis screen still takes
its heritage signal from the grid's *Secteur d'intérêt patrimonial* row alone.
A layer that fails to read is skipped and listed under `heritage_failures`
rather than failing the borough's zoning.

## What the grid translation does

`hbu_dataplatform.quebec.grid_columns` turns one workbook row into the `GridColumn`
objects `zoning_grid_columns` has always emitted, so everything downstream -
`lot_zoning_envelopes`, the setbacks, the CP-SAT programme, the map - reads
a Quebec City zone exactly as it reads a Montreal one. The translation is
faithful where the two by-laws say the same thing and approximate where they
do not, and every approximation is written on the column's `parse_notes`:

* **One column per usage family.** A Montreal grid prints a mixed zone as
  several columns (commerce at the RDC in one, housing above in the next);
  a Quebec City row states every authorised usage group with its own
  *Localisation*. So the row becomes up to four columns - `H`, `C`, `I`, `E`
  - each carrying the zone's one set of building norms and the floors that
  family may occupy. The groups behind each family (`H1`, `C2, C20`, ...)
  travel in the `usage_*` columns.
* **Usage codes are the bare family letters.** Quebec's `H1`..`H4` are not
  Montreal's `H.1`..`H.7` - they are kinds of housing, not dwelling counts -
  so no class ceiling applies and `max_dwellings` is the grid's own
  *nb max. logement par bâtiment*, the largest of the isolé / jumelé /
  rangée figures.
* **Storeys.** *Nombre d'étages max.* where stated, and **left blank where it
  is not** — which is most of the borough: 693 of La Cité-Limoilou's 761
  zones print no storey count, 607 of them stating a height in metres
  instead. `floors_max` is NULL on those columns, exactly as the published
  sheet leaves the row empty. The envelope is bounded by the height, which is
  carried as printed: `GridColumn.to_zone_column` turns it into the storey
  *domain* CP-SAT needs, at the shortest storey the platform builds, and
  `solve_program` then enforces the metre cap itself — charging four metres
  to a commercial plate against three to a dwelling, which is the ranking a
  single storey count cannot express.

  Until 2026-09-22 this filled the blank with `floor(height / 3.5)`. That
  printed a ceiling against *Nombre d'étages max.* on 91% of the borough
  where the by-law states none, and it was also tight: 20 m became five
  storeys where the by-law allows six of housing.
* **Levels.** *Localisation* codes map onto `BuildingLevel`: `S` below
  ground, `R` the ground floor, `R+` everything, `1` the floor above the
  ground floor, `1+` that and up. `2+` and higher have no exact row and are
  read as *tous sauf le RDC*, which overstates a mixed zone's housing floors
  by one at most; the note says so.
* **Site coverage.** *POS min.* is the minimum. The grid states no maximum,
  so `100 - aire verte min.` is carried as one where a green area is required.
* **Margins and lot width** as printed; the implantation mode is read off
  which H1 building types are given a dwelling count, in the `I-J-C`
  letters `postgis` already parses for VSMPE.
* **Dwelling density.** *Nb de log. à l'hectare min/max*, in
  `dwelling_density_min_per_ha` and `dwelling_density_max_per_ha`. Kept apart
  from `density_min`/`density_max`, which are a floor-area ratio: one bounds
  floor area and the other bounds a unit count, so they are multiplied by the
  lot to reach different variables and folding either into the other would be
  a category error. 2 592 of the city's zones state the minimum and 985 the
  maximum; 1 702 state one bound and not the other, which is why they are
  carried as two fields rather than a range. A stated `0` is a stated zero —
  883 zones print `0/0` and all but four of them authorise no dwelling group
  at all — and is **not** to be read as "unstated".
* **Commercial floor area.** *Superficie maximale de plancher*, which the
  grid prints once for *Vente au détail* and once for *Administration*, both
  per building. The platform prices one undifferentiated `commerce`, so the
  **tighter of the two** becomes `commercial_floor_max_m2` and no split of
  that one quantity can breach either stated cap. It is the conservative
  reading and it costs something: the two differ on 2 845 zones city-wide
  (316 in CIL, typically 2 200 against 1 100), and where they do, this
  refuses the all-retail building the grid would allow. *Par bâtiment* and
  not *par établissement*, because the solver sizes a building and a building
  may hold several establishments — a zone printing only the
  per-establishment figure states no building cap here at all. The grid
  states **no minimum** commercial floor area, so there is no field for one.
* **Not carried:** the particular dimensions and norms the grid states per
  building type, and the PDAD code.

All three of the norms above **bind the solve**, not just the table:
`solve_program` bounds the dwelling count by `lot_ha × the log/ha range` (the
minimum owed only where housing is actually built, the way `density_min` is)
and the commerce floor by the lumped ceiling. A lot stopped by either says so
in `binding`, as `dwelling_density_max_per_ha` and `commercial_floor_max_m2`.

On La Cité-Limoilou's 761 zones this yields 539 residential columns, 538 of
them solver-ready (the one that is not states neither storeys nor height).

## Streets and frontage

Quebec City publishes centre lines, not sides. `neighborhood_streets` renames
the segment's `ID` and `NOM_TOPOGRAPHIE` to `COTE_RUE_ID` and `NOM_VOIE` and
carries the rest; nothing else in silver knows the difference. What a centre
line changes is the *fallback* frontage only: the exact measure is the edge a
lot shares with a road parcel and needs no line at all, and a centre line
still runs inside the road parcel, which is how a road lot is recognised.
The 8 m / 16 m reach the fallback spends is measured from the line, and a
centre line sits half a roadway further from the lot than a curb side would.

## Metres

Lengths and areas are measured in the projection each city is surveyed in:
NAD83 / MTM zone 8 for the island, zone 7 for Quebec City
(`partitions.metric_crs_for`). The frontage, setback, zone-piece, comparables
and massing computations all take it from the key. Two things still say
32188 in SQL owned by `hbu_infra` - the polygon area in
`silver.lot_assessment_comparables` and the massing's comment - and are off
by about a tenth of a per cent on Quebec City ground until they are
parameterised.

## What is priced off another city, and what is not priced

Every Quebec City partition can run the whole chain, but three of its inputs
are Montreal's, and each says so where it lands:

* **CMHC vacancy and rents** are the city's own. The vacancy workbook is
  national and the bronze snapshot now keeps its `Québec` centre beside
  `Montréal`; the average-rent page is fetched once per centre (HMIP
  geography 1400 for the Québec CMA, 1060 for Montréal), and
  `CMHC_QUARTIERS` names the quartiers that make up each borough: seven
  for La Cité-Limoilou, and four for Sainte-Foy–Sillery–Cap-Rouge - the
  survey's whole `Sainte-Foy-Sillery` zone plus Cap-Rouge, whose zone pairs
  it with Saint-Augustin-de-Desmaures, a separate municipality. Silver picks
  the borough's centre before matching quartiers.
* **Commercial rents are a proxy.** Cushman & Wakefield publishes no Quebec
  City MarketBeat, so `commercial_rents` prices a Quebec City borough at the
  Montreal whole-market row. The row's `submarket` reads `... (Montreal
  proxy)`, its `note` says which city it stands in for, and the run's
  metadata carries `priced_by_proxy_city = true`. Replace it with a stated
  Quebec City rate through the asset's config when one is in hand.
* **Construction costs are Montreal's column.** The Altus-derived guide
  prices nine markets - Vancouver to St. John's - and Quebec City is not one
  of them, so `lot_profiles` and the programme read the same Montreal rates
  for both cities. There is no flag on the row for this; it is a fact about
  the publisher recorded here and in `docs/construction-costs.md`.
* **The RFU factor** is read for both cities and reported in
  `uniformized_property_wealth`'s metadata (`quebec_comparative_factor`);
  `MARKET_FACTOR` is still what a `comparables` run is told.

What is deliberately not translated from the grid is listed under "What the
grid translation does" above.

## Running it

```bash
make neighborhood-add NEIGHBORHOOD=CIL      # once; the axis lives in the instance
make quartiers streets                      # both cities' outlines and networks
make features lots buildings NEIGHBORHOOD=CIL
make cadastre NEIGHBORHOOD=CIL              # lands them in rag.*; prints the cells CIL fell in
make tile-streets building-lots frontage zone-pieces TILE=0302312101   # per cell - see `make tiles`
make grid-columns NEIGHBORHOOD=CIL          # reads the workbook, not PDFs
make envelopes setbacks TILE=0302312101
make roll                                   # CODE_MUN now defaults to both cities
make lot-values TILE=0302312101
make corpus publish NEIGHBORHOOD=CIL        # the per-zone sheets: fetch, chunk, embed, load
```

CIL spans several cells of the cut — `make tiles-of NEIGHBORHOOD=CIL` lists
them — so every `TILE=` line above runs once per cell;
`scripts/materialize_borough.sh CIL 2026-09-01` does the loop. See [The tile axis](running.md#the-tile-axis).

`features` has to run before `corpus` on a partition scraped before
2026-09-16: `LIEN_GRILLE` is minted by that asset, and a zones parquet written
without it fails `corpus` naming the missing column.

`scripts/materialize_borough.sh CIL 2026-09-01` runs the same steps in order
from WSL, where `uv` is not on the path, and logs each under
`.dagster_home/logs/materialize/`.
