# The assumptions, in one place

Every number in the HBU chain is one of two things: a **measurement** — a
norm the grid prints, a floor area the roll states, a rent CMHC surveyed — or
a **stated assumption**, a judgement the platform makes because no source
publishes the figure. The rule for the second kind is that the row records the
value that produced it: `program_assumptions` on every program and HBU row,
`income_assumptions` on every comparables row, `enhance_assumptions` on every
gap row and `screen_assumptions` on every opportunities row. This page is the
same list read the other way — every stated assumption, its default, where it
is set and where it lands — so a mandate can be read back against the
assumptions behind it, and a reader who wants to move one knows which lever to
pull.

Where each is set: a **make** variable is on `make programs`, `make hbu`,
`make comparables` or `make opportunities`, defaults in the Makefile and is
listed by `make help`; a **config** field is on the asset's Dagster `Config`
and reachable only through `--config-json`; a **constant** is a module default
with no run-time override; **design** is a rule the code states rather than a
number it takes. The pages that argue each default are linked from every
section; this one states them. Read it with
[development-program.md](development-program.md) for the solve and
[site-theses.md](site-theses.md) for everything after it.

## The ground: which pieces exist, and what a plate can be

| assumption | default | set by | why |
| --- | --- | --- | --- |
| a zone gives a lot a piece when it covers ≥ 1% of the lot **and** ≥ 1 m², **or** ≥ 500 m² whatever the share | `ZonePieceConfig` | config | two publishers drew two lines, and the sliver is small absolutely *and* proportionally; the `or` keeps a city block off a park |
| a clipped edge is a frontage at ≥ 0.5 m of street; the clip tolerance is 0.25 m | `MIN_ZONE_PIECE_FRONTAGE_M`, `DEFAULT_ZONE_PIECE_EDGE_TOLERANCE_M` | constant | the edges are ranked, and a 4 cm smear of the cross street would rank second |
| a parcel is the roadway when ≥ 1 m of *géobase double* street line runs inside it; a ruelle is not a street | `FrontageConfig.min_street_m` | config | the geobase draws no lanes, so a lot backing onto one gets no frontage from it — [street-frontage.md](street-frontage.md) |
| a lot sharing no boundary with a road lot reaches 8 m, then 16 m, for a street | `FrontageConfig.fallback_buffers_m` | config | road-widening strips; past 16 m the reach credits interior lots with the street beyond their neighbours |
| the lot's width for *Largeur du terrain min* is its longest street frontage | `Lot.frontage_m` | design | the two differ on a wedge-shaped parcel |
| the setback edge tolerance | 0.05 m | make `TOLERANCE_M` | absorbs the EPSG:4326 round trip the street edge has been through |
| a lot is *effectively empty* at ≤ 30 m² of building footprint | `LotProfilesConfig.max_built_area_m2` | config | a shed in a borough of triplexes; moves `category` only, never `has_building` |
| the plate is capped at the largest rectangle the setback margins hold, tried at 1:1, 1.5:1, 2:1 and 3:1 | `DEFAULT_ASPECT_RATIOS` | make `RATIOS` | past 3:1 a building is a wall — [massing.md](massing.md) |
| a footprint under 10 m² is not a building | `MIN_FOOTPRINT_M2` | constant | a dot on a map that reads as a massing |
| a surface stall needs 5.5 m of clear yard in every direction; a program's asphalt is at most 3 patches | `MIN_PARKING_DEPTH_M`, `PARKING_MAX_BAYS` | constant | article 566 of 01-283; a ribbon of yard holds no car, so those programs dig, bay or shrink |
| the column governing a family is the widest *Largeur min* the lot meets, and among ties the most permissive dwelling ceiling | `select_governing_column` | design | on print order the duplex column won 14,937 of Villeray's lot × zone pairs |
| a program is called by a family's name at ≥ 70% of its usage floor, `mixed` below | `DOMINANT_USE_SHARE` | constant | reporting only; nothing in the solve reads it |
| a `road_parcel` (a road CUBF on the roll, or the cadastre's road lots) and an `equipment_zone` piece (only *E* authorised) get no program | `hbu_status` | design | a street and a park are not sites |
| the roll's building quantities are split across a lot's pieces by footprint share, its ground by area share, and unit counts are rounded | `_allocate_existing` | design | a borough's totals are unchanged by the split; the rounding is why `existing_num_assessment_units` and not `has_assessment` says whether the roll reached a piece |

## What stands today: the roll's side and its income

Recorded in `income_assumptions` on `silver.lot_assessment_comparables`, and
carried onto the gap as the `existing_*` columns.

| assumption | default | set by | why |
| --- | --- | --- | --- |
| a CUBF leading digit is an income class: 1 residential; 2, 3, 4 and 8 industrial; 5, 6 and 7 commercial (5 charged as retail, 6 and 7 as office); 9 nothing | `CUBF_CLASSES` | constant | manufacturing is `2-3` on one row of the manual, which is the off-by-one to avoid — [comparables.md](comparables.md) |
| a dwelling earns the borough's CMHC average rent for its bedroom class, less the surveyed vacancy | CMHC RMS, survey year `URBAN_RAG_CMHC_SURVEY_YEAR` (2023 by default) | env | measured; a suppressed class has no key, which is neither zero nor free — [cmhc-surveys.md](cmhc-surveys.md) |
| retail earns $26 per sq ft per year gross at 2025-Q1, carried to the current quarter by Statistics Canada's retail index | `RETAIL_BASE`, `RETAIL_BASE_PERIOD` | make | the one rate in the chain with no survey behind it — [commercial-rents.md](commercial-rents.md) |
| office and industrial earn Cushman & Wakefield's level for the submarket the borough sits in, carried the same way | `silver.commercial_rents` | measured | `rent_provenance` names the publisher, quarter and submarket per class |
| commerce and industry run 7% vacancy each | `COMMERCIAL_VACANCY_PCT`, `INDUSTRIAL_VACANCY_PCT` | constant | the MarketBeat vacancy is a *space* vacancy for a submarket, not a credit-loss allowance |
| a new building spends 35% of gross on taxes, insurance, management and maintenance; vacancy is netted separately, per class | `OPEX` 0.35 | make | the single largest lever on every cap rate; shared with the solve so both sides of the gap are netted with one number |
| age adds 0.0012 of gross a year to that ratio, capped at 0.10; an unstated year is charged 50 years | `MAINTENANCE_PER_YEAR`, `MAX_MAINTENANCE`, `ASSUMED_AGE` | make | only maintenance among the four depends on age; the flat ratio flattered standing stock — [maintenance.md](maintenance.md) |
| the roll's value is the market value | `MARKET_FACTOR` 1.0 | make | the *facteur comparatif* is not in the published roll; the 2026 roll values everything as of July 2024, and 1.1–1.3 is the honest range |
| a lot's comparables are its 8 most similar within 2 km, on distance (500 m is one unit), floor and lot area (a factor of 2 is one unit), dwellings, and use (weight 1.5; 0.35 for a different code inside the class, 1.0 across classes; 1.0 where a side cannot state a feature) | `K_COMPARABLES`, `MAX_DISTANCE_M`, `ComparableWeights` | make / constant | an appraisal reasons over about eight |
| the standing building is worth its NOI — netted at the age-adjusted ratio — over the hold at the discount rate, sold at the terminal cap, **starting today** | `existing_present_value_cad` | design | it is earning now, so no build or lease-up pushes it out |

## What would be built: rents, areas and build rates

Recorded in `program_assumptions` on `silver.lot_development_programs` and
`gold.lot_highest_best_use`.

| assumption | default | set by | why |
| --- | --- | --- | --- |
| a new dwelling leases at CMHC's stock average plus 30% | `RENT_PREMIUM_PCT` | make | new-build over stock; dwellings only, the commercial rents being market quotes already. Conservative against what new one-bedrooms in Villeray ask |
| space below grade leases at 15% under the floor above it, and costs 15% more to build | `BELOW_GRADE_RENT_DISCOUNT_PCT`, `BELOW_GRADE_COST_PREMIUM` | constant | both families; a market quote for retail is a quote for a shop at grade |
| a studio is 500 sq ft, a one-bedroom 600, a two-bedroom 900, a three-plus 1,200 | `UNIT_AREAS_SQFT` | constant | the schedule the residential cost and the density are charged on |
| a dwelling costs $257.50 per sq ft of unit — the Altus wood-frame condo midpoint — for every building | `RES_COST_SQFT` | make | under-costs a tower; corridors, lobbies and shafts are not priced at all — [construction-costs.md](construction-costs.md) |
| commerce costs $300 and industry $200 per sq ft of gross floor | `COMMERCIAL_COST_PER_SQFT_CAD`, `INDUSTRIAL_COST_PER_SQFT_CAD` | constant | stated; charged on gross, so they do pay for the corridors |
| commerce asks $80 and industry $30 per sq ft per year where no survey resolved a rate | `NonResidentialEconomics` | constant | overridden by `silver.commercial_rents` on every partition that has it |
| a dwelling storey is 3 m, a commercial or industrial one 4 m, an underground level 0 m | `StoreyHeights` | constant | the metric half of the storey cap; digging is free of *Hauteur* as article 38 1° makes it free of *Densité* |
| one level of usage may go below grade, where the level rows authorise one | `BASEMENT_LEVELS_ALLOWED` | config | a modelling bound rather than a norm |

## Parking

| assumption | default | set by | why |
| --- | --- | --- | --- |
| a dwelling owes 0.5 stall and 1,000 sq ft of commerce or industry owes 3.0, rounded up together as one number | `STALLS_PER_DWELLING`, `STALLS_PER_1000_SQFT` | make / constant | Villeray abolished residential minima, so this is what the building offers rather than what a by-law demands — [development-program.md](development-program.md#parking-three-provisions-no-dominance) |
| a stall takes 400 sq ft underground, 300 on the yard and 300 as a ground-floor bay, aisles and ramps included | the three `*_STALL_AREA_SQFT` | constant | the same car needs more structure around it below grade |
| a stall costs $60,300 dug and $6,105 on asphalt (both Altus Montreal midpoints, per stall), and $15,450 as a bay — 20% of the residential rate over 300 sq ft | the three `*_STALL_COST_CAD`, `GARAGE_SHELL_FRACTION` | constant | the bay is the one rate the guide does not price; it sits dearer than asphalt and a quarter of a dug stall |
| at most 6 levels are dug, and the hole may run out under 100% of the parcel | `MAX_UNDERGROUND_LEVELS`, `UNDERGROUND_LOT_SHARE` | constant | articles 38 1° and 43: a dug stall is neither floor area nor coverage, and a parkade runs out under the yard |
| a stall rents for $120 a month at 85% occupancy, and at most 1.0 per dwelling and 3.0 per 1,000 sq ft are rented | `PARKING_STALL_RENT_CAD_MONTH`, `PARKING_STALL_OCCUPANCY_PCT`, `MARKET_STALLS_*` | constant | the market ceiling is why the yard is not paved for rent |
| a full stall per dwelling leases the building 2 months faster, concave in coverage, cut into 4 tranches | `PARKING_ABSORPTION_SAVING_MONTHS`, `PARKING_ABSORPTION_TRANCHES` | constant | about 0.4% of a dwelling's present value at 5% |
| a program empty or infeasible with its stalls is solved again without them, and the row says so | `waive_parking_if_empty` | config | "this program, short this many stalls" is a better answer than "no program"; recorded as `parking_waived` and `waived_stalls` |

## The objective, and when the money moves

| assumption | default | set by | why |
| --- | --- | --- | --- |
| the discount rate | 5% | make `DISCOUNT_PCT` | an unlevered developer's rate |
| the hold | 25 years | make `HOLD_YEARS` | |
| the sale that ends it | a 4.5% terminal cap | make `TERMINAL_CAP_PCT` | what stabilised Montreal multifamily has sold at; 4–5.5 in recent memory. Also the residential market cap the returns are screened against unless `MARKET_CAP_RATE` says otherwise |
| the rebuild takes 18 months to build and 6 to fill, and the whole income stream is pushed out by the build plus half the lease-up | make `CONSTRUCTION_MONTHS`, `LEASE_UP_MONTHS` | make | a linear fill loses half the lease-up on average; about 9% off the present value at 5% |
| capital is spent on day one, undiscounted | — | design | the conservative side |
| the land is outside the objective | — | design | the owner holds it in every future, so it cancels; the deal puts it back — [opportunities.md](opportunities.md) |
| the highest and best use is the largest discounted net profit across the governing column of each family, the mix a decision of the solve; ties break on the grid's print order | `select_highest_best_use` | design | a zone that prints housing and commerce as two columns permits a mix, and a mix is not the maximum of two pure programs |
| a candidate gets 10 seconds of CP-SAT | `max_seconds` | config | `FEASIBLE` and `UNKNOWN` are counted rather than hidden |
| the legacy monthly NOI amortises capital straight-line over 300 months | `AMORTIZATION_MONTHS` | constant | no longer the objective; kept so older tables read unchanged |
| a piece is under-built when the solver's floor exceeds the roll's, a missing floor reading as 0 | `is_underbuilt` | design | a vacant parcel is exactly the case; a piece with no program is not under-built |

## The enhancement: the building stays and grows

Recorded in `enhance_assumptions` on `gold.lot_redevelopment_gap`, and set on
`make hbu`.

| assumption | default | set by | why |
| --- | --- | --- | --- |
| the addition takes 9 months to build and 3 to fill | `ENHANCE_MONTHS`, `ENHANCE_LEASE_UP` | make | the shell is there |
| 25% of the standing NOI is lost while the works are on | `DISRUPTION_SHARE` | make | tenants decanted from the top floor, the yard a site, a shop behind hoarding |
| the addition costs 1.5× the new-build rate | `ADDITION_PREMIUM` | make, on both `make hbu` and `make opportunities` | storey additions $175–400 per sq ft against $210–275 new — [site-theses.md](site-theses.md#the-enhancement-solve). At 1.2 additions start to clear in VSMPE; at 1.5 almost none do |
| at most one storey goes on top; the grid's own ceiling still applies | `MAX_ADDED_STOREYS` | make | a wood-frame plex's limit before the frame is replaced |
| the standing plate is the roll's floor over its storey count | `RetainedBuilding.footprint_m2` | design | exact for a plex, generous for a setback storey; the BDOI footprint on `lot_profiles` is the better input and the next one |
| the addition parks on the yard only — nothing dug, no bay — and is solved again with the stalls waived where the yard cannot hold them | `RetainedBuilding`, `waive_parking_if_empty` | design | recorded as `enhance_parking_waived` and `enhance_waived_stalls` |
| an addition that adds nothing is the hold: no works, so no disruption, and the gain is 0 | `nothing_pencils` in `enhance_binding` | design | the map says "nothing to add" and points at Keep |
| where the solve is not there — an older partition, or a row it came back `INFEASIBLE` or `ERROR` on — the improvement thesis uses the closed-form estimate: the standing footprint repeated up to `MAX_ADDED_STOREYS` times, plus an annex on the ground the solver's footprint covers and the standing one does not, capped at the floor gap and priced at the solver's rates times the premium | `improvement_program` | design | `improvement_source` says which; on VSMPE the estimate is most of the thesis — see the [open items](site-theses.md#open-items) |
| 5 seconds of CP-SAT per enhancement | `enhance_max_seconds` | config | |

## The site theses, and the deal

Recorded in `screen_assumptions` on `gold.lot_investment_opportunities`; set
on `make opportunities`. Every default is argued in
[site-theses.md](site-theses.md).

| assumption | default | set by |
| --- | --- | --- |
| a program is one family's at ≥ 85% of proposed floor, and mixed-use when the smaller of residential and commercial is ≥ 15% | `DOMINANT_SHARE`, `MIXED_MIN_SHARE` | make |
| a teardown is a building of 1960 or older, filling ≤ 40% of the envelope, under ≥ 2 storeys of headroom — and not one whose enhancement pays its owner more than its rebuild | `TEARDOWN_MAX_YEAR`, `TEARDOWN_MAX_BUILT_SHARE`, `MIN_STOREY_HEADROOM` | make |
| a brownfield use is a CUBF starting `2`, `3`, `42`, `487`, `55`, `6231`, `63` or `64` | `brownfield_use_prefixes` | config |
| a *secteur d'intérêt patrimonial* and a PIIA sector each keep a piece with a standing building out of the two theses that demolish; a pre-1940 building is flagged for review, not screened | `EXCLUDE_HERITAGE_SECTORS`, `EXCLUDE_PIIA_SECTORS`, `demolition_review_year`, `exclude_demolition_review` | make / config |
| demolition costs $150 per m² of a residential building's floor and $250 of anything else's | `DEMOLITION_COST_M2`, `DEMOLITION_COST_NONRES_M2` | make |
| characterisation costs $12,000 once per brownfield use; remediation $150 per m² of lot where the program includes housing and $75 otherwise | `SITE_ASSESSMENT_COST`, `REMEDIATION_RES_M2`, `REMEDIATION_NONRES_M2` | make |
| an improvement is filed at ≥ 40 m² of addition, at most 1 storey up, on an under-built piece | `improvement_min_floor_m2`, `improvement_max_added_storeys` | config / make |
| ground the roll never listed, with under 5% of it under a measured building, is a lane and not an infill | `UNASSESSED_VACANT_MAX_COVERAGE` | make |
| a thesis is ranked only where its own future pays the owner; the shortlist is 25 per thesis | `REQUIRE_POSITIVE_NPV`, `SITE_TOP_N`, `TOP_N` | make |
| a buyer pays the larger of the assessed value × `MARKET_FACTOR` and what the standing income is worth to its holder; a buyer whose every future loses at that price walks (`none`) | `acquisition_cost_cad`, `buyer_best_future` | design |
| the owner's three futures cancel the land; a rebuild carries the site's costs, the other two do not | `futures_economics` | design |

## The returns: what the solve leaves out of the budget, and the two screens

Also in `screen_assumptions`; the arithmetic is in
[site-theses.md](site-theses.md#yield-on-cost-and-irr).

| assumption | default | set by | why |
| --- | --- | --- | --- |
| soft costs 18%, contingency 7% and builder's-risk insurance 1% of hard cost | `SOFT_COST_PCT`, `CONTINGENCY_PCT`, `BUILDERS_RISK_PCT` | make | 15–25, 5–10 and under 1 on a Montreal wood-frame mid-rise; operating insurance is inside the 35% |
| selling costs 2.5% of the sale | `SELLING_COST_PCT` | make | brokerage and legal on the exit, 2–3 |
| absorption: 4 dwellings a month, 1,500 sq ft of commerce, 5,000 of industry; the lease-up is the *longest* of the three and the solve's stated months, not the sum | `ABSORPTION_PER_MONTH`, `COMMERCIAL_ABSORPTION_SQFT`, `INDUSTRIAL_ABSORPTION_SQFT` | make | the families fill in parallel; commerce is measured on the plate plus its cellar |
| the market cap for residential income is the terminal cap (4.5) unless stated; commerce trades 175 bps over it and industry 100 | `MARKET_CAP_RATE`, `COMMERCIAL_CAP_SPREAD_BPS`, `INDUSTRIAL_CAP_SPREAD_BPS` | make | retail and suburban office at about 6.25, industrial at 5.5 |
| a mixed building's cap is the harmonic mean of its families' caps weighted by NOI — never by floor | `blended_cap_rate` | design | the only blend for which total NOI over the cap equals the sum of the parts |
| a development has to yield 100 bps over the lot's blended cap, and its unlevered IRR has to clear that cap plus 100 bps; either bar, plus paying against holding, is a good candidate | `MIN_YOC_SPREAD_BPS`, `HURDLE_SPREAD_BPS` | make | in a flat-NOI model the cap is the indifference point, so a hurdle at it prices risk at zero; a flat 12 is a levered convention — `HURDLE_IRR` restores it |
| the IRR is unlevered and annual: price and site costs at day one, the budget spent evenly over the build, income filling linearly over the lease-up, the hold, the sale at the exit cap less selling costs | `proforma.returns` | design | financing, taxes on the gain and rent growth are not in it |

## What the model is, and is not

Structural assumptions — the shape of the model rather than a number in it.
Each is stated in the page it belongs to; they are listed here because a
reader deciding whether to trust a figure needs all of them at once.

- **Flat NOI, no growth, no financing, no tax on the gain**, on every future
  and in both chairs. The NPV and the IRR disagree only by what the returns
  section adds. A 12% IRR is unreachable here by construction, and that is a
  statement about the model, not the borough.
- **One operating expense ratio for all three families.** A triple-net retail
  lease leaves its landlord a far lighter load than an apartment does, so the
  single ratio most flatters the dwellings.
- **The roll's value is the price**, scaled by one factor a reader sets. There
  is no transaction data in the chain.
- **The roll's floor over its storeys is the footprint**, for the standing
  building and for the plate the enhancement grows. The measured footprint on
  the piece is used to tell a lane from a lot, not yet to size the building.
- **One footprint for every floor above grade and for the cellar of usage**;
  only the dug parking has a plate of its own, and it may reach the lot line.
- **A storey is one use, and storeys are interchangeable within a column**
  except where the level rows say otherwise: since 2026-09-10 the ground floor
  goes to a usage authorised on it, but nothing says the shop is *at* grade
  beyond that.
- **The residential build rate is charged on the unit schedule**, so
  corridors, lobbies, stairs and shafts are unpriced; the non-residential
  rates are charged on gross and do pay for them. `condo_wood` prices every
  building, tower or plex.
- **`Largeur du terrain min` is tested against the frontage**, not a width.
- **Remediation is priced per square metre of lot**, and demolition per square
  metre of floor at two rates; no per-lot survey exists in the chain. A real
  site costs what its Phase II finds.
- **The contamination presumption is a CUBF prefix list**, not Annexe III's
  SCIAN codes; the codebook carries the correspondence and a join can replace
  the list.
- **The pre-1940 year is a proxy** for the borough's inventory of 4,612
  heritage immovables, which is the screen the statute actually names.
- **A stated year is charged as a stated year**, whether the roll's `rl0307b`
  says it was measured or estimated; effective age is not modelled.
- **`has_assessment` is true on every row** of the gap and says nothing;
  `existing_num_assessment_units` and its four companions are what say the
  roll reached a piece.
- **`lot_uid` is reminted on every reload of the cadastre.** The joins from
  `rag` to gold are on `lot_number`; the gold-to-gold joins deliberately are
  not, so a reload of the lots orphans every gold table until the chain is
  re-run.

## Where each is recorded

| jsonb | on | carries |
| --- | --- | --- |
| `program_assumptions` | `silver.lot_development_programs`, `gold.lot_highest_best_use` | parking, construction, non-residential rents, storey heights, investment, basement levels, the timing, `waive_parking_if_empty` |
| `income_assumptions` | `silver.lot_assessment_comparables` | CMHC rent and vacancy, OPEX and the maintenance curve, the three commercial rents with `rent_provenance`, their vacancies, the market value factor |
| `enhance_assumptions` | `gold.lot_redevelopment_gap` | the addition's months, disruption share, premium, storey limit |
| `screen_assumptions` | `gold.lot_investment_opportunities` | both theses' thresholds, the heritage switches, the site costs, the proforma lines, the two spreads, and the rebuild's and enhancement's timing read back off the rows |

A run's metadata carries the counts that say how far an assumption reached:
`num_lots_age_assumed` for the 50-year default, `num_parking_waived` and
`num_enhancements_parking_waived` for the waiver, `num_lots_storeys_unknown`
for the improvement estimate, `num_lots_heritage_unknown` for a partition
whose grids were parsed before the *Patrimoine* rows were read.
