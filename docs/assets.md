# Assets

All 35 assets, their partition axes and what each one writes. The layer
contracts they answer to are in [architecture.md](architecture.md).

| Layer | Asset | Partitions | Output |
| --- | --- | --- | --- |
| bronze | `spectrum_table_catalog` | date | The service's full table list for that day (577 tables across 19 namespaces), as one `tables.parquet` |
| bronze | `neighborhood_features` | date × neighborhood | One parquet file per source table |
| bronze | `reference_neighborhoods` | date | Montreal's 91 housing reference neighborhoods from donnees.montreal.ca, with their dwelling counts |
| bronze | `neighborhood_lots` | date × neighborhood | Every cadastral lot inside that borough, from Quebec's Infolot service, as one `lots.parquet` |
| bronze | `neighborhood_buildings` | date × neighborhood | BDOI building footprints inside that borough, as one `buildings.parquet` |
| bronze | `cmhc_vacancy_survey` | date | The Montreal-CMA slice of the CMHC Rental Market Survey, as published |
| bronze | `cmhc_rent_survey` | date | The Montreal-CMA slice of the CMHC HMIP average-rent page, as published |
| bronze | `street_network` | date | Montreal's *géobase double* — one line per side of street — as published, island-wide |
| bronze | `montreal_residential_costs` | date | The Montreal column of the Altus construction cost guide's residential types — condo/apartment by storey band, townhouses, single family, seniors, student residences — in $/sf |
| bronze | `montreal_nonresidential_costs` | date | The same column's commercial and industrial types in $/sf, plus the three parking types in **$/stall** |
| bronze | `property_assessment_roll` | date | Quebec's *rôle d'évaluation foncière* — one point per assessment unit, the characteristics table describing it, and the crosswalk naming every lot it covers, out of a province-wide GeoPackage, scoped to Ville de Montréal |
| bronze | `montreal_commercial_rents` | date | Cushman & Wakefield's Montreal office and industrial MarketBeats, one row per (sector, submarket), with the net, additional and gross rent per square foot on one footing across both. The reports are discovered off the landing page: the filename changes shape every quarter and only the `/<year>/q<n>/` path is stable |
| bronze | `commercial_rent_index` | date | Statistics Canada's Commercial Rents Services Price Index for the Montreal CMA (table 18-10-0260-01), quarterly, by building type. An **index** (2019=100), not a level — it carries a measured rent to the quarter being scraped, and carries the stated retail base forward |
| bronze | `linked_documents` | date × neighborhood | The PDFs those tables link to, fetched and flattened to text |
| silver | `vacancy_rates` | date × neighborhood | That borough's quartiers taken out of the snapshot and averaged into one rate per dwelling type × bedroom class — as parquet and as `silver.vacancy_rates`, with the quartier rows behind it in `silver.quartier_vacancy_rates` |
| silver | `average_rents` | date × neighborhood | The same, per bedroom class, for rents — `silver.average_rents` and `silver.quartier_average_rents` |
| silver | `building_lot_intersections` | date × neighborhood | Both spatial joins, computed against one load of the cadastre — repaired on the way in, which is where `make_valid` runs: building footprints clipped to the lots they intersect (`silver.building_lot_intersections`) and map features clipped to the lots they cover (`silver.lot_features`, the hop from a lot to its documents), as two geoparquet files |
| silver | `assessment_units` | date | The roll's two layers put back together on `id_provinc` — one row per assessment unit, its point and everything the roll says about it. Province-wide as parquet, and as `silver.assessment_units` cut into **one partition per borough** by where each unit's point falls: the one asset that publishes partitions it was not asked for, because the roll has no borough axis of its own |
| silver | `lot_assessed_values` | date × neighborhood | What every lot in the borough is assessed at: the units the roll's own cadastre crosswalk puts on it (and, for the condos it cannot place, the ones whose point falls in it), summed on `rl0404a` both whole and apportioned — as geoparquet and as `silver.lot_assessed_values` |
| silver | `commercial_rents` | date × neighborhood | One gross rent per rent class for the borough — office and industrial off the C&W submarket it sits in (VSMPE is Midtown North), retail from a stated base, all three carried to the latest quarter the index publishes. `rent_basis` says measured, escalated, unescalated or stated — as parquet and as `silver.commercial_rents` |
| silver | `lot_assessment_comparables` | date × neighborhood | What each lot yields on that assessment, and which lots are like it: the roll's dwellings and floor area summed onto the parcel and split by each unit's own CUBF use code, priced at CMHC's borough rent and `urban_rag.program`'s stated non-residential rates into `cap_rate_pct` — plus the k nearest comparable lots, scored on use, size, dwellings and ground distance at once, and the `estimated_value_cad` their median ratios imply. `assessed_to_estimated_ratio` is the screen. As geoparquet and as `silver.lot_assessment_comparables`
| silver | `neighborhood_streets` | date × neighborhood | That day's street sides clipped to one borough, with the published length, the length inside it, and the share that survived the cut — as geoparquet and as `silver.neighborhood_streets` |
| silver | `lot_frontage` | date × neighborhood | How much of each lot's boundary faces each street side, in metres, longest first — as geoparquet and as `silver.lot_frontage`. **Blocked**, see below |
| silver | `zoning_grid_columns` | date × neighborhood | Those PDFs read as the tables they are — one row per column of each *grille des usages et des normes*, with its usages, authorised levels and every norm of its CADRE BÂTI block as columns — as parquet and as `silver.zoning_grid_columns` |
| silver | `lot_zoning_envelopes` | date × neighborhood | Every lot's zoning envelope, denormalised to the grain `urban_rag.program` reads — one row per (lot, grid column), with the lot's area, its primary and secondary frontage, and the norms that bound what may be built on it — as parquet and as `silver.lot_zoning_envelopes` |
| silver | `lot_buildable_setbacks` | date × neighborhood | What is left of each lot once its zone's four margins are subtracted — one row per (lot, zone, grid column), with the boundary sorted into front, sides and rear and each buffered by the margin that governs it. `footprint_cap_m2` is that envelope or *Taux d'implantation au sol max* × lot area, whichever is smaller — as geoparquet and as `silver.lot_buildable_setbacks`. **Blocked**, see below |
| silver | `document_chunks` | date × neighborhood | Those documents cut into retrieval-sized chunks — as parquet and as `silver.document_chunks` |
| silver | `document_embeddings` | date × neighborhood | A bge-m3 vector per chunk. The one silver asset with no table of its own: its vectors' home is the pgvector index `document_index` writes |
| silver | `lot_development_programs` | date × neighborhood | One `urban_rag.program.solve_program` CP-SAT run per candidate row of `lot_zoning_envelopes` that authorises dwellings, commerce or industry and parses — the mix of dwellings, commerce, industry and parking that maximises discounted net profit (`npv_cad`) under that envelope, with the storey split, footprint, stalls, build cost, the legacy monthly NOI and `binding` caps — as parquet and as `silver.lot_development_programs`. **Blocked**, see below |
| gold | `lot_profiles` | date × neighborhood | Every lot in the borough, one row each — whether a building stands on it and how many, its primary and secondary street frontage in metres, the zoning PDF that covers most of it, the zoning envelopes that govern it, the borough's CMHC vacancy and rent grids, and what the ground earns on what it is assessed at (`cap_rate_pct`, `estimated_value_cad`, `assessed_to_estimated_ratio` and each lot's comparables) — as geoparquet and as `gold.lot_profiles`. **Blocked**, see below |
| gold | `lot_highest_best_use` | date × neighborhood | One row per lot: the `lot_development_programs` candidate of the *governing* envelope — the grid's own pick within a zone, the zone covering most of the lot across zones — with `hbu_status` naming why a lot without one has none. As parquet and as `gold.lot_highest_best_use`. **Blocked**, see below |
| gold | `lot_redevelopment_gap` | date × neighborhood | One row per lot: the floor area standing on it today (`lot_assessment_comparables`, by residential/commercial/industrial class) against what its envelope could hold, in m² and sqft, and the two incomes on one stated NOI definition — `annual_stabilised_noi_gap_cad` and, separately, the solver's own `hbu_annual_noi_after_construction_cad`. `is_underbuilt` is the screen. As parquet and as `gold.lot_redevelopment_gap`. **Blocked**, see below |
| gold | `lot_investment_opportunities` | date × neighborhood | The under-built lots worth looking at first, one row per lot. `investment_thesis` is read off the *proposed* program — so a warehouse whose best use is flats is a residential opportunity, and the existing use beside it makes a conversion play one predicate. `yield_on_cost_pct` is the proposed building's stabilised NOI over construction plus the land at its assessed value; `thesis_rank` orders the under-built lots within each thesis on it, breaking ties on the annual NOI gap, and `is_top_opportunity` marks the first `top_n`. Unranked lots keep their row and say why — as parquet and as `gold.lot_investment_opportunities` |
| gold | `lot_building_massing` | date × neighborhood | The proposed building of every lot, drawn: one rectangle per lot fitted inside that lot's setback envelope so the four margins are respected by construction, in EPSG:4326 and ready to put on a map. A few aspect ratios are tried at the parcel's own axis and its perpendicular, squarest first. `footprint_fit_pct` is the check the table exists for — `solve_program` caps a footprint on the lesser of two *areas* and never asks whether the shape fits, so a fit under 100 is a solved footprint the ground cannot take. Every lot keeps a row in the tree; only the drawn ones reach `gold.lot_building_massing`. **Blocked**, see below |
| gold | `document_index` | date × neighborhood | Those vectors upserted into the Postgres/pgvector store the query side reads |

The corpus assets are described under [The document
corpus](corpus.md#the-document-corpus) and [The shared vector
store](corpus.md#the-shared-vector-store).

The catalog is a separate asset because the published table list drifts —
boroughs add and retire layers without notice, and a scrape is only
reproducible if you know what existed on that date. `neighborhood_features`
reads that day's `tables.parquet` through a
`MultiToSingleDimensionPartitionMapping`, so the `2026-08-18|VSMPE` partition
consumes exactly the `2026-08-18` catalog.

`document_index` is the one asset that writes no parquet of its own, and
deliberately: it is a *load* of `document_embeddings`, which is already in the
tree. Its record is that file.

`lot_profiles` is registered and has a job, but **no schedule**, because it
reads two relations hbu_infra has to create first. `sql/009_gold_lot_profiles.sql`
creates the table it writes into, and `sql/006_lot_documents.sql` creates the
`rag.lot_documents` view it takes the document columns from — and that second
file carries a `-- requires: rag.chunks` header, so `db.py init` skips it on a
database that has never held a corpus and it only lands on the *next* init,
after `document_index` has run. Both files exist; what is outstanding is
`db.py init` against the target database, twice. `compute_lot_profiles` checks
for both up front and fails naming the file to apply rather than letting
psycopg raise `relation "gold.lot_profiles" does not exist`. Add the schedule
then. Run it by hand with `make lot-profiles`.

Five of its inputs come from the tree rather than from Postgres —
`lot_zoning_envelopes`, `vacancy_rates`, `average_rents` and the two
`montreal_*_costs` snapshots — and the envelope pair has no schedule of its own
either, so a scheduled `lot_profiles` would need `zoning_envelopes_job` ahead
of it. A partition missing any of the five fails naming the asset to
materialize, before the rows it was going to replace are deleted. Run the pair
by hand with `make envelopes`, and the cost snapshots with `make costs` — those
are partitioned by date alone, so one run of them serves every borough of that
day.

`lot_frontage` is blocked the same way, one step further along: hbu_infra
*has* `sql/007_silver_streets.sql` and `sql/008_silver_lot_frontage.sql`, but a database
they have not been applied to yet answers `relation "silver.neighborhood_streets" does not
exist`. So it too is registered, given a job, and left off the schedules
until `db.py init` has run against the target database. Run it by hand with
`make frontage`. Its upstream `neighborhood_streets` now owns
`silver.neighborhood_streets` itself rather than being loaded by this asset on
the way past, so it needs `sql/007_silver_streets.sql` applied too — and it is
still scheduled normally, since that file has no `-- requires:` header and lands
on the first `db.py init`.

`lot_buildable_setbacks` is blocked for the same reason again —
`sql/015_silver_lot_buildable_setbacks.sql` has to be applied first — and has
the same shape of fix: registered, given a job, run by hand with `make
setbacks`. It sits behind both `lot_frontage` and the envelope pair, since one
supplies the street edge it sorts a boundary against and the other the margins
it subtracts, so whoever schedules the three schedules them in that order.

`lot_development_programs`, `lot_highest_best_use` and `lot_redevelopment_gap`
are blocked the same way, stacked one behind another: `sql/017`, `sql/018` and
`sql/019` each have to be applied before their asset's upsert will land, and
none has been yet. Registered, given jobs, run by hand with `make programs`
and `make hbu`. `lot_development_programs` reads `lot_zoning_envelopes` (and,
optionally, `lot_buildable_setbacks` — without it every footprint is capped on
*Taux d'implantation* alone) and is where the CP-SAT solve happens, so it sits
behind the envelope pair the same way `lot_profiles` does. `lot_highest_best_use`
is a sort over that asset's own output and needs nothing else.
`lot_redevelopment_gap` additionally reads `lot_assessment_comparables` — the
assessment side of the comparison — which *is* scheduled, so once the first
two land only the envelope lineage is what a schedule for this trio would wait
behind.

`lot_building_massing` is blocked behind all of them and on `sql/022`. It reads
`lot_highest_best_use` for the footprint and `lot_buildable_setbacks` for the
envelope to fit it into, and without the second it draws nothing at all rather
than a rectangle with the margins ignored — which would look entirely plausible
on a map, and is the one failure mode a sanity-check asset must not have. Run
it by hand with `make massing`; it is also the asset most often re-run on its
own, since a different set of aspect ratios redraws a borough without
re-solving it. See [massing.md](massing.md).

## What `lot_buildable_setbacks` measures, and why it is not a rectangle

The zoning grid states four margins — *Avant principale*, *Avant secondaire*,
*Latérale*, *Arrière* — and `lot_zoning_envelopes` has carried all four since
they were first parsed with nothing subtracting them. `urban_rag.program` caps
a footprint on *Taux d'implantation au sol* alone, so a deep mid-block lot and
a shallow one of the same area have been solving identically. They are not the
same site.

Two shortcuts suggest themselves and both are wrong. `ST_Buffer(lot, -d)`
shrinks every edge by the same `d`, and margins are four distances at four
edges. Estimating a depth as `area / frontage` and multiplying out
`(width − 2·side) × (depth − front − rear)` is exact for a rectangle and wrong
for the wedges, dog-legs and skewed rear lines a real cadastre is full of —
and both inputs to the honest version are already here: the polygon in
`rag.lots`, the street edge in `silver.lot_frontage`.

So the subtraction is directional. The boundary is sorted into four classes,
each is buffered by the margin that governs it, and the union is differenced
out of the parcel. The front is what `lot_frontage` *measured*, not an edge
guessed at from the parcel's shape; of what is left, a piece running within 45°
of parallel to that front is rear and everything else is side. That test is
`compute_lot_frontage`'s own parallel test pointed at a different reference
line, which is why the two share one constant.

**The mode moves the answer more than any single margin does.** *Mode
d'implantation* decides whether the side margin applies at all — a contiguous
building is built to the party line and has none — and VSMPE's grids print
`I-J` and `I-J-C`, where the `C` is exactly that permission. The most
permissive mode a column allows is the one applied, because the table answers
what *may* be built:

| mode permitted | `side_setback_rule` | side setback |
|---|---|---|
| contigu (`C`) | `contigu` | 0 — both lines are party lines |
| jumelé (`J`) | `jumele` | half the margin off each side, which removes what a whole margin off one side does |
| otherwise | `isole` / `unknown` | the full margin, both sides |

Subtracting the printed *Latérale* from both sides of every lot in a borough of
plexes would understate most of the stock — on the 476 m² test parcel it is the
difference between 309 m² and 385 m² of buildable area. `side_setback_rule`
records which reading produced a row and `side_margin_min_m` carries what the
grid printed, so a number can always be read back against the rule behind it.

`footprint_cap_m2` is the lesser of that envelope and *Taux d'implantation au
sol max* × lot area, and `footprint_cap_binding` names which of the two bound.
They are independent caps — one says where on the lot, the other how much of it
— and a building satisfies both. A borough whose rows mostly read `setbacks` is
one shaped by its margins rather than by its coverage.

A lot with no frontage row gets no row here: there is no edge to call the front,
so the angle test has no reference. The asset reports the count rather than
leaving it to be noticed.

