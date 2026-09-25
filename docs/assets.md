# Assets

All 46 assets, their partition axes and what each one writes. Two spatial axes:
`date × neighborhood` is the publisher's unit, `date × tile` a cell of the
quadkey cut the lot chain is computed per — see [Two spatial
axes](architecture.md#two-spatial-axes). The layer
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
| bronze | `street_network` | date | The **RQTT**, Quebec's province-wide road network — one centre line per segment — as published, bounded to the three cities' bounding boxes. One file where Montreal's *géobase double*, Quebec City's `vque_18` and Saguenay's `sag-reseau-routier` used to be three |
| bronze | `montreal_residential_costs` | date | The Montreal column of the Altus construction cost guide's residential types — condo/apartment by storey band, townhouses, single family, seniors, student residences — in $/sf |
| bronze | `montreal_nonresidential_costs` | date | The same column's commercial and industrial types in $/sf, plus the three parking types in **$/stall** |
| bronze | `property_assessment_roll` | date | Quebec's *rôle d'évaluation foncière* — one point per assessment unit, the characteristics table describing it, and the crosswalk naming every lot it covers, out of a province-wide GeoPackage, scoped to Ville de Montréal |
| bronze | `cubf_use_codes` | date | The MEFQ's *codes d'utilisation des biens-fonds* (Annexe 2C.1) — the list that says `rl0105a` 4611 is a parking garage, which the roll itself never states. The published sheet as it stands, hierarchy headings and all: 1,260 four-digit codes under the categories and rubrics they hang off. 185 kB, uncached — the file has no year in its URL and is reissued whenever the manual is amended |
| bronze | `uniformized_property_wealth` | date | MAMH's *richesse foncière uniformisée*, as published — one row per Quebec *organisme municipal*, so no borough axis. Read for one column inside it: `CSALX02163`, the *facteur comparatif* that carries a roll figure to a market one. That factor is the only sales-derived number about Montreal's market published openly, per municipality, under a usable licence, and it is in no other publication — `hbu_dataplatform.comparables` defaults `MARKET_FACTOR` to 1.0 without it. Bronze keeps the whole file rather than the column |
| bronze | `montreal_commercial_rents` | date | Cushman & Wakefield's Montreal office and industrial MarketBeats, one row per (sector, submarket), with the net, additional and gross rent per square foot on one footing across both. The reports are discovered off the landing page: the filename changes shape every quarter and only the `/<year>/q<n>/` path is stable |
| bronze | `commercial_rent_index` | date | Statistics Canada's Commercial Rents Services Price Index for the Montreal CMA (table 18-10-0260-01), quarterly, by building type. An **index** (2019=100), not a level — it carries a measured rent to the quarter being scraped, and carries the stated retail base forward |
| bronze | `neighborhood_addresses` | date × neighborhood | Adresses Québec's official civic address points inside that borough, as one `addresses.parquet`. The MRNF's province-wide layer, queried with the borough outline as a spatial filter and paged 1,000 rows at a time. Ten published fields, unrepaired and unparsed — including `AdresseFormatee`, the only place the street name exists, and `Version`, the publisher's own freshness stamp. **A row is an addressable unit rather than a front door**: roughly half the points in a dense borough carry a unit prefix. The product is advertised under a WMS endpoint, which is a picture protocol and cannot answer this — the REST face of the same ArcGIS MapServer is what is read. See [addresses.md](addresses.md) |
| bronze | `linked_documents` | date × neighborhood | The PDFs those tables link to, fetched and flattened to text |
| bronze | `council_minutes` | date × neighborhood | **Quebec City only.** The procès-verbaux of the borough's *conseils de quartier* — one PDF per assembly, listed per council by the city's affichagesite host — fetched and flattened to text, one row per minute with the meeting date the listing states and the links the PDF carries (`links`, a JSON array of its URI annotations plus the city URLs spelled out in its text). A Montreal or Saguenay key has no council and writes an empty file. `council_ids` config narrows a run to some of the borough's councils. See [council-minutes.md](council-minutes.md) |
| bronze | `council_minutes_documents` | date × neighborhood | The documents those minutes link to, followed breadth-first for three hops and one row per document reached: the consultation *fiche* (`kind` `fiche`, an HTML page read as text), the *sommaire décisionnel* and the resolutions on the city's decision system (`gpd`, with the `document_number` its file name states — `GT2025-233`, `CA1-2025-0215`), the consultation report and presentation (`consultation_file`), and a file another minute links (`council_file`). `depth`, `parent_doc_id` and `minutes_doc_ids` keep the hop and the minutes that led there; anything off those hosts is counted under `num_links_ignored` |
| silver | `vacancy_rates` | date × neighborhood | That borough's quartiers taken out of the snapshot and averaged into one rate per dwelling type × bedroom class — as parquet and as `silver.vacancy_rates`, with the quartier rows behind it in `silver.quartier_vacancy_rates` |
| silver | `average_rents` | date × neighborhood | The same, per bedroom class, for rents — `silver.average_rents` and `silver.quartier_average_rents` |
| silver | `neighborhood_cadastre` | date × neighborhood | The hop from the borough axis to the tile axis: that borough's `neighborhood_lots`, `neighborhood_buildings` and `neighborhood_features` landed in `rag.lots`, `rag.buildings` and `rag.features` — rings repaired on the way in, which is where `make_valid` runs — with each row's `cell_key` (its zoom-19 quadkey) and `cell_partition` (the cut cell that owns it). Fails, naming the ground, when the cut has no cell for a row; reports `tiles_touched`, the cells whose runs have to follow, because a reload remints `lot_uid` and cascades into every one of them. Writes a `cadastre.json` manifest rather than parquet: its product is the working set |
| silver | `building_lot_intersections` | date × tile | Both spatial joins, for the lots this cell owns against every building and feature in the snapshot: building footprints clipped to the lots they intersect (`silver.building_lot_intersections`) — a building straddling a cell edge is clipped by each side's run, and the *whole* building is what the 10 % threshold is measured against — and map features clipped to the lots they cover (`silver.lot_features`, the hop from a lot to its documents), as two geoparquet files |
| silver | `assessment_units` | date | The roll's two layers put back together on `id_provinc` — one row per assessment unit, its point and everything the roll says about it. Province-wide as parquet, and as `silver.assessment_units` cut into **one partition per borough** by where each unit's point falls: the one asset that publishes partitions it was not asked for, because the roll has no borough axis of its own. `use_description` is the MEFQ's own words for each unit's use code, looked up from `cubf_use_codes` and carried from here into the comparables, the lot profiles and the redevelopment gap |
| silver | `lot_assessed_values` | date × tile | What every lot in the borough is assessed at: the units the roll's own cadastre crosswalk puts on it (and, for the condos it cannot place, the ones whose point falls in it), summed on `rl0404a` both whole and apportioned — as geoparquet and as `silver.lot_assessed_values` |
| silver | `commercial_rents` | date × neighborhood | One gross rent per rent class for the borough — office and industrial off the C&W submarket it sits in (VSMPE is Midtown North), retail from a stated base, all three carried to the latest quarter the index publishes. `rent_basis` says measured, escalated, unescalated or stated — as parquet and as `silver.commercial_rents` |
| silver | `lot_assessment_comparables` | date × tile | What each lot yields on that assessment, and which lots are like it: the roll's dwellings and floor area summed onto the parcel and split by each unit's own CUBF use code, priced at CMHC's borough rent and `hbu_dataplatform.program`'s stated non-residential rates into `cap_rate_pct` — plus the k nearest comparable lots, scored on use, size, dwellings and ground distance at once, and the `estimated_value_cad` their median ratios imply. `assessed_to_estimated_ratio` is the screen. As geoparquet and as `silver.lot_assessment_comparables`
| silver | `neighborhood_streets` | date × tile | That day's RQTT centre lines whose midpoint falls in the cell, stored **whole** — no side is clipped, so a lot's frontage on a street that crosses a boundary is measured against the whole side. `neighborhood` is the borough outline the midpoint falls in, or null. As geoparquet and as `silver.neighborhood_streets` |
| silver | `lot_frontage` | date × tile | How much of each lot's boundary faces each street, in metres, longest first — the boundary it shares with the street's own cadastral parcel, so an exact edge with no buffer. As geoparquet and as `silver.lot_frontage`, with the parcels that *are* the street beside it as `road_lots.parquet` — the set nothing else on the platform holds, since the roll never reached Montreal's roadways, and what keeps them out of the highest-and-best-use inventory. **Blocked**, see below |
| silver | `lot_zone_pieces` | date × tile | The piece of each lot that one zone governs, as a site in its own right — one row per (lot, zone), carrying the clipped polygon, its area, the street *that piece* faces (`lot_frontage`'s edges cut to it and re-ranked within it) and its share of what already stands on the parcel. A zoning boundary does not have to follow a lot line, and on a large parcel it usually does not: lot 1 740 794 is 27 044 m² with 24 596 in H04-072 (eight storeys) and 2 440 in C04-083 (C.4, six) — two development sites, facing two different streets, with two answers. `is_primary_zone` marks the largest piece and `footprint_share` is how the roll, which describes a *lot*, is divided between them. As geoparquet and as `silver.lot_zone_pieces`. **Blocked**, see below |
| silver | `lot_addresses` | date × tile | Every civic address standing on a parcel, at the **(lot, zone)** grain the gold tables are keyed on — so an address joins straight onto `lot_highest_best_use`, `lot_redevelopment_gap` and `lot_investment_opportunities`, and a map can label a site *7430 Rue Lajeunesse* instead of *2 784 705*. The publisher has no lot number, so this join is the only thing that puts an address on a parcel. `AdresseFormatee` is split into unit, civic number, suffix, street, municipality and postal code on the way in. A point outside every parcel but within `max_snap_m` (2 m) of one is given it and says `match_basis = 'snapped'` — two publishers' surveys of the same ground disagree at the edges; a point on nobody's parcel is counted as `num_unmatched` and **not** written, since the grain is an address *on a lot*. `address_rank` orders a site's addresses by civic number, so `is_primary_address` is the one a label takes, and `num_piece_addresses` (units) is reported beside `num_piece_civic_addresses` (doors). As geoparquet and as `silver.lot_addresses`. **Blocked**, see below |
| silver | `zoning_grid_columns` | date × neighborhood | Those PDFs read as the tables they are — one row per column of each *grille des usages et des normes*, with its usages, authorised levels and every norm of its CADRE BÂTI block as columns — as parquet and as `silver.zoning_grid_columns` |
| silver | `lot_zoning_envelopes` | date × tile | Every zoning envelope in the borough, denormalised to the grain `hbu_dataplatform.program` reads — one row per (lot, zone, grid column), with the **piece** that zone governs (`piece_area_m2`), the parcel it belongs to (`lot_area_m2`), that piece's own primary and secondary frontage, and the norms that bound what may be built on it — as parquet and as `silver.lot_zoning_envelopes` |
| silver | `lot_buildable_setbacks` | date × tile | What is left of each lot once its zone's four margins are subtracted — one row per (lot, zone, grid column), with the boundary sorted into front, sides and rear and each buffered by the margin that governs it, and the result clipped to the piece that zone governs — a margin comes off a *lot line*, but a zone's rules apply only on its own ground. `footprint_cap_m2` is that envelope or *Taux d'implantation au sol max* × piece area, whichever is smaller — as geoparquet and as `silver.lot_buildable_setbacks`. **Blocked**, see below |
| silver | `document_chunks` | date × neighborhood | Those documents cut into retrieval-sized chunks — as parquet and as `silver.document_chunks` |
| silver | `council_planning_items` | date × neighborhood | What the minutes and their trail say about zoning, lots and dwellings, as columns: one row per *planning item* — an agenda item of a minute about planning (a crosswalk resolution is dropped and counted), or one trail document — with the zones it is about and every zone it names, its by-law and GPD numbers, file number, addresses, lot numbers, usage groups, every "from N to M dwellings" it states scoped as the by-law's cap or the project's count, height and storeys, the decision stage, the council's opinion with the sentence it was read from, and the consultation report's vote table. Regular expressions over the text, each value beside its excerpt, nothing guessed — `hbu_dataplatform.council_items`. As parquet and as `silver.council_planning_items`. See [council-minutes.md](council-minutes.md) |
| silver | `document_embeddings` | date × neighborhood | A bge-m3 vector per chunk. The one silver asset with no table of its own: its vectors' home is the pgvector index `document_index` writes |
| silver | `lot_development_programs` | date × tile | One `hbu_dataplatform.program.solve_program` CP-SAT run per candidate row of `lot_zoning_envelopes` that authorises dwellings, commerce or industry and parses — the mix of dwellings, commerce, industry and parking that maximises discounted net profit (`npv_cad`) under that envelope, with the storey split, footprint, stalls, build cost, the legacy monthly NOI and `binding` caps — as parquet and as `silver.lot_development_programs`. **Blocked**, see below |
| gold | `lot_profiles` | date × tile | Every lot in the borough, one row each — whether a building stands on it and how many, its primary and secondary street frontage in metres, the zoning PDF that covers most of it, the zoning envelopes that govern it, the borough's CMHC vacancy and rent grids, and what the ground earns on what it is assessed at (`cap_rate_pct`, `estimated_value_cad`, `assessed_to_estimated_ratio` and each lot's comparables) — as geoparquet and as `gold.lot_profiles`. **Blocked**, see below |
| gold | `lot_highest_best_use` | date × tile | One row per **(lot, zone)** — one per piece of ground: the `lot_development_programs` candidate of that zone's *governing* envelope, which is the grid's own pick among its columns. It used to be one row per lot, with the best-covered zone answering for the whole parcel; a zoning boundary crossing a large lot makes two sites of it and both are now solved, each over its own area and against its own street. `lot_number` groups them, `is_primary_zone` marks the largest, and a borough total sums every row while a per-lot join filters or aggregates. `hbu_status` names why a piece without a program has none. Two kinds of parcel are not development sites and get no program whatever their zoning permits: `road_parcel` and `equipment_zone`. A third is a site but not a rental one: `single_family_zone`, a piece zoned for one dwelling and nothing else priced, which `lot_development_programs` does not solve at all — see [single-family.md](single-family.md). A parcel is a road when *either* the assessment roll files it under a CUBF road code (4510–4599) *or* a RQTT street line runs down the inside of it, which is how Montreal's own street lots are found — the roll does not record them, so the roll alone reached 48 of VSMPE's roadways where the two predicates together reach some 1,400. `equipment_zone` is a zone authorising only *Équipements collectifs* — a park, a school, a cemetery — and is decided **per piece**, so a parcel half park and half housing reports both rather than having to pick one. As parquet and as `gold.lot_highest_best_use`. **Blocked**, see below |
| gold | `lot_redevelopment_gap` | date × tile | One row per (lot, zone), following the table above: the floor area standing on that piece today (`lot_assessment_comparables`, by residential/commercial/industrial class) against what its envelope could hold, in m² and sqft, and the two incomes on one stated NOI definition — `annual_stabilised_noi_gap_cad` and, separately, the solver's own `hbu_annual_noi_after_construction_cad`. `is_underbuilt` is the screen. **The roll describes a *lot*, so it is divided between that lot's pieces** by `footprint_share`, measured off the building footprints actually standing on each — which is what makes a corner commercial strip carry the whole of the block on it and the yard behind it read as vacant land. Since 2026-09-07 it also prices the lot's three futures on one footing — hold, enhance (a second CP-SAT solve with the standing building retained, `enhance_*`) and rebuild, the last two with their income starting after the build and the lease-up — and names the owner's `best_future`; see [site-theses.md](site-theses.md#three-futures-not-one). As parquet and as `gold.lot_redevelopment_gap`. **Blocked**, see below |
| gold | `lot_investment_opportunities` | date × tile | The under-built sites worth looking at first, one row per (lot, zone) — a parcel a zoning boundary crosses can carry two theses, and does. `investment_thesis` is read off the *proposed* program — so a warehouse whose best use is flats is a residential opportunity, and the existing use beside it makes a conversion play one predicate. `yield_on_cost_pct` is the proposed building's stabilised NOI over construction plus the land at its assessed value; `thesis_rank` orders the under-built lots within each thesis on it, breaking ties on the annual NOI gap, and `is_top_opportunity` marks the first `top_n`. `site_thesis` is the second axis — why the parcel is acquirable: `brownfield`, `teardown`, `infill` or `improvement`, each costing its own demolition, remediation or addition into `site_yield_on_cost_pct` and ranked within itself, with a heritage sector read off the grid keeping a lot out of the two that demolish. Reads `lot_highest_best_use`, `lot_assessment_comparables` and `zoning_grid_columns` beside the gap. Unranked lots keep their row and say why — as parquet and as `gold.lot_investment_opportunities`. See [site-theses.md](site-theses.md). **Blocked**, see below |
| gold | `lot_building_massing` | date × tile | The proposed building of every piece of ground, drawn: one rectangle per (lot, zone) fitted inside that piece's setback envelope so the four margins are respected by construction, in EPSG:4326 and ready to put on a map. A few aspect ratios are tried at the parcel's own axis and its perpendicular, squarest first. `footprint_fit_pct` is the check the table exists for — `solve_program` caps a footprint on the lesser of two *areas* and never asks whether the shape fits, so a fit under 100 is a solved footprint the ground cannot take. Every lot keeps a row in the tree; only the drawn ones reach `gold.lot_building_massing`. Draws a **second** polygon beside it - the surface parking, on the yard the building leaves, published to `gold.lot_surface_parking`: a surface stall is not a building, so it is fitted into the *piece* rather than the setback envelope — the ground that zone governs, so neither program on a split lot is offered a yard the other has built on, with 5.5 m clear in every direction, and `surface_parking_fit_pct` is the same check applied to the ground. **Blocked**, see below |
| gold | `map_cell_aggregates` | date × neighborhood | Every gated map layer dissolved onto the tile grid, so a borough-wide view has something true to draw. One row per (layer, cell), a cell being a Web Mercator tile `ZOOM_OFFSET` (4) zooms finer than the one being looked at — so a tile carries at most 256 cells at every zoom, by construction. Five layers: `capacity`, `streets`, `lots`, `buildings`, `massing`. Precomputed rather than aggregated at query time, because a `GROUP BY` over the borough on every tile request is most expensive at exactly the zoom a borough-wide view sits at. A source not materialized for the partition contributes no cells rather than failing the run, and the metadata reports the per-layer counts so an empty layer is visible. The grid arithmetic is `hbu_dataplatform.tile_grid`, the rollup `hbu_dataplatform.postgis.compute_map_cell_aggregates`. As parquet and as `gold.map_cell_aggregates`. **Blocked**, see below |
| gold | `map_tiles` | date × neighborhood | Every map layer rendered as Mapbox Vector Tiles and packed into one **PMTiles** archive per layer, so hbu_rag_map draws a borough straight off S3 with HTTP range requests and no database in the request path. The SQL is the map's former per-request `ST_AsMVT` query, run once per band of tiles at zooms 6–19; below a layer's detail zoom the tiles hold `map_cell_aggregates`' cells at zoom + 4, and below 12 their outlines. The screens the map's sidebar toggles — lot area, under-built, one site thesis, the shortlist, the use side — travel as tile properties and are applied in the browser. A source not materialized for the partition yields no archive rather than a failure, and `map_tiles.json` beside the archives says which layers exist. The specs are `hbu_dataplatform.map_tiles`, the statements `hbu_dataplatform.tile_render`, the file format `hbu_dataplatform.pmtiles_archive`. **Not** in Postgres: the archives are the product. |
| gold | `document_index` | date × neighborhood | Those vectors upserted into the Postgres/pgvector store the query side reads |

The corpus assets are described under [The document
corpus](corpus.md#the-document-corpus) and [The shared vector
store](corpus.md#the-shared-vector-store).

The catalog is a separate asset because the published table list drifts —
boroughs add and retire layers without notice, and a scrape is only
reproducible if you know what existed on that date. `neighborhood_features`
reads that day's `tables.parquet` through a
`MultiToSingleDimensionPartitionMapping`, so the `2026-09-01|VSMPE` partition
consumes exactly the `2026-09-01` catalog.

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

`lot_zone_pieces` is blocked for the same reason —
`sql/025_silver_lot_zone_pieces.sql` has to be applied first — and has the same
shape of fix: registered, given a job, run by hand with `make zone-pieces`. It
sits behind `building_lot_intersections` (the clip and the buildings) and
`lot_frontage` (the street edges it cuts to each piece), and **ahead of the
whole zoning chain**: the envelopes join it rather than the raw overlaps, so it
is the first step of that chain and not an addition to the end of it.

`lot_addresses` is blocked for the same reason once more —
`sql/026_silver_lot_addresses.sql` has to be applied first, and it creates both
`rag.addresses` (the working set the join is computed over) and the published
table. Registered, given a job, run by hand with `make addresses`. It sits
behind `building_lot_intersections` for the cadastre and `lot_zone_pieces` for
the grain: an address is placed on the **(lot, zone)** key the gold tables use,
so without the pieces there is nothing to key it to. Its own bronze half,
`neighborhood_addresses`, needs no table at all — it writes only parquet — but
it has no schedule either. It has its own job, so the fetch and the join can
be re-run separately: `make addresses` materializes both, and re-running the
join alone after a change to the snap tolerance is
`--select silver/lot_addresses` rather than a second scrape of a hundred
thousand points.

`lot_buildable_setbacks` is blocked for the same reason again —
`sql/015_silver_lot_buildable_setbacks.sql` has to be applied first — and has
the same shape of fix: registered, given a job, run by hand with `make
setbacks`. It sits behind `lot_frontage`, `lot_zone_pieces` and the envelope
pair: the first supplies the street edge it sorts a boundary against, the
second the ground it clips the carve to, the third the margins it subtracts, so
whoever schedules them schedules them in that order.

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
on a map, and is the one failure mode a sanity-check asset must not have. It also reads the outlines its
surface parking is fitted onto — `silver.lot_zone_pieces` where the pieces
exist, `rag.lots` as the fallback for a partition without them, which on a
split lot lets each program be offered a yard the other has built on — and
treats them as optional in the same way
round: without either, every row is `no_lot_geometry`, no asphalt is drawn, and
the buildings are unaffected. Run
it by hand with `make massing`; it is also the asset most often re-run on its
own, since a different set of aspect ratios redraws a borough without
re-solving it. See [massing.md](massing.md).

`lot_investment_opportunities` and `map_cell_aggregates` close the list, on
`sql/021` and `sql/023`. The first reads `lot_redevelopment_gap`,
`lot_highest_best_use`, `lot_assessment_comparables` and `zoning_grid_columns`,
so it sits behind the whole chain above; run it with `make opportunities`. The
second sits behind *everything* — the working set, one silver table and two
gold ones — and re-runs whenever any of them does, which is why it is last;
run it with `make map_cells`. Unlike the rest, it treats a missing source as
no cells rather than as a failure, so a borough without `lot_building_massing`
still gets a utilisation surface and the metadata says which layers came back
empty.

`map_tiles` comes after even that, because below a layer's detail zoom its
tiles *are* those cells. It renders the nine map layers into one PMTiles
archive each under `gold/map_tiles/<date>/<neighborhood>/` — on S3 when
`S3_BUCKET` is set, which is where the deployed map reads them — and writes a
`map_tiles.json` naming what was built; run it with `make map_tiles`, or
`make map_tiles TILE_LAYERS='["lots"]'` to redo one layer after its source
re-ran. It is the only gold asset with no table: the archives are what the map
reads, and the tables it renders from are already published.

## What `lot_buildable_setbacks` measures, and why it is not a rectangle

The zoning grid states four margins — *Avant principale*, *Avant secondaire*,
*Latérale*, *Arrière* — and `lot_zoning_envelopes` has carried all four since
they were first parsed with nothing subtracting them. `hbu_dataplatform.program` caps
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
of parallel to that front is rear and everything else is side. That test used
to be `compute_lot_frontage`'s own, aliased from it — frontage no longer
classifies anything by angle, since a road lot's shared edge *is* the frontage,
so `SETBACK_SEGMENT_M` and `SETBACK_MAX_SIN` now live beside the only function
that reads them.

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

**Whose ground the margins come off, and whose ground the answer is on.**
Two questions, two answers, one row. A margin is a setback from a *lot line*,
so it is subtracted from the parcel's own boundary — a zoning boundary running
through the middle of a large parcel is not a lot line and takes nothing off.
What the zone decides is where its rules apply at all, and that is an
intersection with `silver.lot_zone_pieces` applied after the carve. So lot
1 740 794 comes back as two envelopes that do not overlap — 24 596 m² carved
under H04-072's margins and 2 440 m² under C04-083's — rather than as two
copies of the whole 27 044 m², which is what it was before the piece grain and
what double-counted the buildable area of every split parcel in the borough.
A column whose zone governs no piece of a lot gets no row at all: that is the
sliver cutoff arriving from the pieces table.

`footprint_cap_m2` is the lesser of that envelope and *Taux d'implantation au
sol max* × piece area, and `footprint_cap_binding` names which of the two bound.
They are independent caps — one says where on the ground, the other how much of it
— and a building satisfies both. A borough whose rows mostly read `setbacks` is
one shaped by its margins rather than by its coverage.

A lot with no frontage row gets no row here: there is no edge to call the front,
so the angle test has no reference. The asset reports the count rather than
leaving it to be noticed.

