# Linux-only: run this from WSL or from the devcontainer. Git Bash is not
# supported - it reports MSYS paths that Dagster and uv both mishandle, which
# is what the cygpath translation this file used to carry was working around.
# The image itself has no make; there, `dagster` and `urban-rag` are on PATH.
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

UNAME_S := $(shell uname -s 2>/dev/null)
ifneq (,$(filter MINGW% MSYS% CYGWIN%,$(UNAME_S)))
$(error This Makefile targets Linux. Run it from WSL or from the devcontainer, \
not from Git Bash - see docs/setup.md.)
endif

export DAGSTER_HOME := $(CURDIR)/.dagster_home

# The pipeline's AWS resources live behind this profile by default. Read the
# same .env that the Python entrypoints load, then export the resolved value so
# Dagster's subprocesses, boto3, fsspec and DuckDB agree on one credential
# source. Command-line overrides still win: `make dagster_run AWS_PROFILE=...`.
DEFAULT_AWS_PROFILE := charles_gauvin_east_1
DOTENV_AWS_PROFILE := $(shell awk -F= '/^[[:space:]]*AWS_PROFILE[[:space:]]*=/ {gsub(/^[[:space:]]+|[[:space:]]+$$/, "", $$2); print $$2; exit}' .env 2>/dev/null)
ifeq (,$(strip $(AWS_PROFILE)))
AWS_PROFILE := $(if $(strip $(DOTENV_AWS_PROFILE)),$(strip $(DOTENV_AWS_PROFILE)),$(DEFAULT_AWS_PROFILE))
endif
export AWS_PROFILE

# botocore checks raw key environment variables before AWS_PROFILE. Keep a
# stale shell from silently outranking the profile pin; CI that intentionally
# uses environment credentials can opt back in with AWS_USE_ENV_CREDS=1.
ifneq (1,$(AWS_USE_ENV_CREDS))
unexport AWS_ACCESS_KEY_ID
unexport AWS_SECRET_ACCESS_KEY
unexport AWS_SESSION_TOKEN
endif

# Touched after a successful `uv sync` (see the environment section). Every
# target that shells out to `uv` takes it as an order-only prerequisite, so a
# fresh checkout, or an edit to pyproject.toml / uv.lock, re-syncs before the
# target runs; an already-current env costs only a stat. Kept under .venv so
# `rm -rf .venv` also arms the next re-sync.
UV_SYNC_STAMP := $(CURDIR)/.venv/.uv-sync-stamp

# Every dagster invocation goes through urban_rag.dagster_home, which writes
# $(DAGSTER_HOME)/dagster.yaml from the environment and then execs the command
# it was handed - the same entrypoint the image uses, so a laptop run and a
# container run configure the instance identically.
#
# With URBAN_RAG_PG_HOST (or an explicit DAGSTER_POSTGRES_URL) in the
# environment, that config puts run, event and schedule storage in Postgres
# under the `dagster` schema; with neither, it leaves the local SQLite default
# alone. `eval "$(../hbu_infra/scripts/db.py env)"` is what sets the former.
#
# A recipe prefix rather than a prerequisite: the config has to be rebuilt on
# every invocation, and a target - directory or stamp - would only fire once
# and then ignore an environment that had changed underneath it. It also does
# its own mkdir -p, which is why these targets no longer take $(DAGSTER_HOME)
# as a prerequisite - only the docker ones, which bind-mount it, still do.
DAGSTER := uv run python -m urban_rag.dagster_home dagster
DAGSTER_DAEMON := uv run python -m urban_rag.dagster_home dagster-daemon

# Assets are selected by their full `<layer>/<asset>` key, which is what
# `key_prefix` in urban_rag.layers gives them. A bare name resolves to no
# AssetsDefinition and dagster answers DagsterInvalidSubsetError, so adding a
# target means looking the asset's layer up rather than copying its name.
MODULE := urban_rag.definitions
# The scrape partition is monthly (see urban_rag.partitions), so the key is
# the first of the current month rather than today. Override to re-derive an
# earlier month from bronze already on disk: `make hbu DATE=2026-08-01`.
#
# That override is for the silver and gold targets only. The targets that
# reach a bronze asset - `catalog`, `cmhc`, `corpus`, `costs`, `features`,
# `quartiers`, `rent-sources`, `roll`, `streets` - fetch from a live publisher,
# so an earlier DATE would write today's data under an earlier month's key.
# They refuse it; see urban_rag.guards and docs/running.md.
DATE ?= $(shell date +%Y-%m-01)
NEIGHBORHOOD ?= VSMPE
PORT ?= 2500
K ?= 5
# Which vector store the retrieval targets act on: the local DuckDB file, or
# the shared Postgres/pgvector one (configured from URBAN_RAG_PG_*, see
# docs/corpus.md). `make index BACKEND=postgres` reloads the latter from parquet.
BACKEND ?= duckdb
# How much geobase street line must run inside a parcel before `make frontage`
# reads that parcel as the roadway itself. It decides which parcels are street,
# not what any lot then measures - the frontage is the boundary a lot shares
# with a street parcel, taken exactly, and has no setting of its own. Replaces
# BUFFER_M, which did decide what every lot measured. The road lots in the test
# slice carry 105-325 m of street line and every other parcel carries none, so
# anything from a decimetre to a hundred metres gives the same table.
# See docs/street-frontage.md.
MIN_STREET_M ?= 1.0
# How far off a lot boundary a measured street edge still counts as lying on
# it, for `make setbacks`. Note this is a tolerance and MIN_STREET_M above is
# not: frontage needs none, because abutting parcels in this cadastre share
# their vertices exactly, while the street edge reaching `setbacks` has been
# through EPSG:4326 and back and this absorbs that round trip. See
# docs/assets.md, under silver/lot_buildable_setbacks.
TOLERANCE_M ?= 0.05
# How many lots `make setbacks` carves and commits per transaction, and whether
# a re-run skips what a previous one already committed. Durability rather than
# speed: the work is identical either way, and what this decides is how much of
# it a dropped connection costs. A borough over an SSM tunnel is the case it
# exists for - the session is rebuilt periodically, a transaction spanning that
# rolls back whole, and unbatched runs could therefore fail to finish at all.
# SETBACK_BATCH=0 does the partition in one transaction, which is only safe on
# a stable link. See urban_rag.postgis.DEFAULT_SETBACK_BATCH_LOTS.
SETBACK_BATCH ?= 2000
SETBACK_RESUME ?= true
# Which municipalities' assessment rolls `make roll` keeps out of the
# province-wide archive, as a JSON list of five-digit `code_mun` values.
# Defaults to Ville de Montréal, which is every borough this pipeline has; `[]`
# keeps the province. See docs/assessment-roll.md.
CODE_MUN ?= ["66023"]
# Whether `make lot-values` falls back to the assessment point for the units
# the roll's own lot-number crosswalk cannot place - which is every divided
# co-ownership, since those name private lots Infolot does not draw. `false`
# leaves them unplaced and every row then comes from the crosswalk alone.
BY_POINT ?= true
# How many comparable lots `make comparables` gives each lot, and the ground
# radius past which one stops being a comparable however alike it looks. 8 is
# the size an appraisal actually reasons over; 2 km is most of a borough.
K_COMPARABLES ?= 8
MAX_DISTANCE_M ?= 2000.0
# Share of gross income that never reaches the owner - taxes, insurance,
# management, maintenance - for `make comparables`, for a building that is
# NEW. Vacancy is netted per class from the surveyed rate and is *not* in this.
# The single largest lever on every cap rate the asset produces. Age is added
# on top of it by the three settings below. See docs/comparables.md.
OPEX ?= 0.35
# What age adds to OPEX, per year of the building's age, capped. Only
# maintenance among the four things OPEX covers depends on how old a building
# is, and charging a 1910 triplex and one finished this year the same share of
# rent for the roof is what most flatters standing stock against redeveloping
# it. MAINTENANCE_PER_YEAR=0 charges every building OPEX and reproduces the
# behaviour the asset had before the curve. See docs/maintenance.md.
MAINTENANCE_PER_YEAR ?= 0.0012
MAX_MAINTENANCE ?= 0.10
# Age charged where the roll states no year built. Not zero - an unstated year
# is likelier to be old stock than new, and the run reports how many lots took
# it as `num_lots_age_assumed`.
ASSUMED_AGE ?= 50
# Scales the assessed value into the cap rate's denominator. 1.0 reports the
# yield on the roll, which is the honest default: Quebec's *facteur comparatif*
# is not in the published roll. Set it to the year's factor for a market rate.
MARKET_FACTOR ?= 1.0
# The Montreal retail rent `make commercial-rents` states, gross per square
# foot per year, and the quarter it is stated for. The one rate in the chain
# with no survey behind it: C&W publish a Montreal office and industrial
# MarketBeat and no retail one, so this is a judgement carried forward by
# Statistics Canada's retail index. See docs/commercial-rents.md.
RETAIL_BASE ?= 26.0
RETAIL_BASE_PERIOD ?= 2025-01
# Stalls each dwelling owes, for `make programs`. Villeray abolished
# residential parking minima, so this is what the building offers rather than
# what a by-law demands - urban_rag.program's own default. 0 removes the
# residential half of the parking demand.
STALLS_PER_DWELLING ?= 0.5
# Dollars per square foot of dwelling, for `make programs`. The default is the
# Altus wood-frame condo midpoint, which under-costs a lot zoned for a tower -
# see urban_rag.hbu_assets.ProgramConfig.
RES_COST_SQFT ?= 257.5
# The proforma `make programs` optimises: discounted net profit. DISCOUNT_PCT
# discounts each year of stabilised NOI, TERMINAL_CAP_PCT prices the sale that
# ends the HOLD_YEARS hold, and RENT_PREMIUM_PCT is what a new dwelling leases
# for over the stock average CMHC surveys - dwellings only, the commercial
# rents are already market quotes. OPEX above is shared with `comparables` so
# both sides of the redevelopment gap stay netted with one number. All stated
# assumptions - see urban_rag.program.InvestmentAssumptions.
DISCOUNT_PCT ?= 5.0
HOLD_YEARS ?= 25
TERMINAL_CAP_PCT ?= 4.5
RENT_PREMIUM_PCT ?= 30.0
# Width-to-depth ratios `make massing` tries when drawing a building, squarest
# first, as a JSON list; the first that fits the lot's setback envelope at the
# solved footprint wins. Each is tried at the parcel's own axis and at the
# perpendicular, so 2.0 covers 1:2 as well. Past 3:1 a "building" is a wall,
# which is why the default stops there. See docs/massing.md.
RATIOS ?= [1.0,1.5,2.0,3.0]

# Which map layers `map_cells` dissolves, as a JSON list. All five is
# the default and the only setting a scheduled run should use; naming
# fewer is for rebuilding one layer's cells by hand.
LAYERS ?= ["capacity","streets","lots","buildings","massing"]
# Where the investment-thesis lines fall for `make opportunities`: the share
# of proposed floor one class needs to own a lot outright, and what the
# smaller of residential and commercial needs for it to be mixed-use instead.
# LAND_FACTOR scales the assessed value into the yield's denominator - 1.0
# costs the land at the roll. TOP_N is the shortlist length per thesis.
DOMINANT_SHARE ?= 0.85
MIXED_MIN_SHARE ?= 0.15
LAND_FACTOR ?= 1.0
TOP_N ?= 25

IMAGE ?= urban-rag
TAG ?= latest
TUNNEL_PORT ?= 5433
TUNNEL_DB_HOST ?= $(shell awk -F= '/^[[:space:]]*URBAN_RAG_PG_HOST[[:space:]]*=/ {gsub(/^[[:space:]]+|[[:space:]]+$$/, "", $$2); gsub(/\r/, "", $$2); print $$2; exit}' .env 2>/dev/null)
DOCKER_RUN := docker run --rm -it \
	-v $(CURDIR)/data:/data \
	-v $(CURDIR)/.dagster_home:/dagster_home \
	-p $(PORT):2500

.PHONY: help sync dagster_run daemon test materialize catalog features \
	quartiers cmhc costs vacancy rents envelopes setbacks lot-profiles \
	programs hbu massing map_cells \
	streets borough-streets roll lot-values comparables \
	rent-sources commercial-rents \
	frontage corpus publish index search ask status \
	require-q validate_defs clean clean-data clean-silver \
	docker-build docker-build-slim docker-run docker-shell docker-test up up-tunnel down logs

help: ## Show this help
	@echo "Targets:"
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "Vars: DATE=$(DATE) NEIGHBORHOOD=$(NEIGHBORHOOD) PORT=$(PORT) K=$(K)"
	@echo "      BACKEND=$(BACKEND) MIN_STREET_M=$(MIN_STREET_M)"
	@echo "      K_COMPARABLES=$(K_COMPARABLES) MAX_DISTANCE_M=$(MAX_DISTANCE_M) OPEX=$(OPEX) MARKET_FACTOR=$(MARKET_FACTOR)"
	@echo "      RETAIL_BASE=$(RETAIL_BASE) RETAIL_BASE_PERIOD=$(RETAIL_BASE_PERIOD)"
	@echo "      IMAGE=$(IMAGE):$(TAG)"
	@echo "      DAGSTER_HOME=$(DAGSTER_HOME)"

# -- environment -----------------------------------------------------------

# `--extra rag` pulls in the retrieval stack (sentence-transformers, torch,
# transformers) for `ask`/`search`/`index`; the code location loads fine
# without it too (see docker-build-slim). The unset works around
# SSL_CERT_FILE breaking uv's resolver on a managed laptop; it is a no-op
# everywhere else.
UV_SYNC := unset SSL_CERT_FILE; UV_SYSTEM_CERTS=1 uv sync --python 3.12 --extra dev --extra rag

sync: ## Install deps, including the retrieval stack
	$(UV_SYNC)
	@mkdir -p $(dir $(UV_SYNC_STAMP)) && touch $(UV_SYNC_STAMP)

# The same sync as `sync`, run only when pyproject.toml or uv.lock is newer
# than the last one, and an order-only prerequisite of every `uv run` target.
# `sync` refreshes the stamp so an explicit run also satisfies them.
$(UV_SYNC_STAMP): pyproject.toml uv.lock
	$(UV_SYNC)
	@mkdir -p $(dir $@) && touch $@

test: | $(UV_SYNC_STAMP) ## Run the test suite (offline)
	uv run pytest

# -- pipeline --------------------------------------------------------------

# Binds 0.0.0.0, not localhost: in a container the UI is only reachable from
# the host if it listens on the container's external interface.
dagster_run: | $(UV_SYNC_STAMP) ## Launch the Dagster UI (PORT=2500)
	$(DAGSTER) dev -h 0.0.0.0 -p $(PORT)

daemon: | $(UV_SYNC_STAMP) ## Run the daemon alone (what the schedules need)
	$(DAGSTER_DAEMON) run -m $(MODULE)

validate_defs: | $(UV_SYNC_STAMP)
	$(DAGSTER) definitions validate -m urban_rag.definitions

catalog: | $(UV_SYNC_STAMP) ## Materialize spectrum_table_catalog for DATE
	$(DAGSTER) asset materialize --select bronze/spectrum_table_catalog --partition $(DATE) -m $(MODULE)

features: | $(UV_SYNC_STAMP) ## Materialize neighborhood_features for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select bronze/neighborhood_features --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

quartiers: | $(UV_SYNC_STAMP) ## Materialize reference_neighborhoods for DATE
	$(DAGSTER) asset materialize --select bronze/reference_neighborhoods --partition $(DATE) -m $(MODULE)

# Bronze: one workbook read for the whole island, so DATE only.
cmhc: | $(UV_SYNC_STAMP) ## Snapshot both CMHC surveys for DATE
	$(DAGSTER) asset materialize --select bronze/cmhc_vacancy_survey,bronze/cmhc_rent_survey --partition $(DATE) -m $(MODULE)

# Bronze: one script read for the whole guide, so DATE only.
costs: | $(UV_SYNC_STAMP) ## Snapshot the Montreal construction cost rates for DATE
	$(DAGSTER) asset materialize --select bronze/montreal_residential_costs,bronze/montreal_nonresidential_costs --partition $(DATE) -m $(MODULE)

# Both write parquet and then upsert into silver.<name> and
# silver.quartier_<name>, so hbu_infra's sql/010 has to have been applied. The
# files land first, so a database that is down costs a re-run of the load
# rather than of the crosswalk.
vacancy: | $(UV_SYNC_STAMP) ## Materialize vacancy_rates for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select silver/vacancy_rates --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

rents: | $(UV_SYNC_STAMP) ## Materialize average_rents for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select silver/average_rents --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# The two envelope assets, which lot_profiles now reads: the grids are
# parsed from the PDFs the corpus already downloaded. Both also upsert into
# silver.zoning_grid_columns / silver.lot_zoning_envelopes (hbu_infra sql/012).
envelopes: | $(UV_SYNC_STAMP) ## Materialize the zoning envelopes for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select silver/zoning_grid_columns,silver/lot_zoning_envelopes --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# Subtracts the four margins those envelopes carry from the parcels `frontage`
# measured, leaving what may actually be built on. Needs
# silver.lot_buildable_setbacks (hbu_infra sql/015) applied, and both
# `frontage` and `envelopes` run first for the same partition - the first
# supplies the street edge a boundary is sorted against, the second the margins
# to subtract. TOLERANCE_M overrides the 0.05 m default.
setbacks: | $(UV_SYNC_STAMP) ## Materialize lot_buildable_setbacks for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select silver/lot_buildable_setbacks --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"silver__lot_buildable_setbacks":{"config":{"edge_tolerance_m":$(TOLERANCE_M),"batch_lots":$(SETBACK_BATCH),"resume":$(SETBACK_RESUME)}}}}'

# Needs gold.lot_profiles and the rag.lot_documents view (hbu_infra sql/009,
# sql/006) applied, and 006 only lands on a db.py init run *after* a corpus has
# been indexed - see urban_rag.lot_profiles_assets. Also reads three silver
# parquet partitions the database knows nothing about: `envelopes`, `vacancy`
# and `rents` for the same DATE x NEIGHBORHOOD.
lot-profiles: | $(UV_SYNC_STAMP) ## Materialize lot_profiles for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select gold/lot_profiles --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# Needs silver.lot_development_programs (hbu_infra sql/017) applied, and
# `envelopes` run first for the same partition - the CP-SAT model in
# urban_rag.program is run once per candidate row it wrote. `setbacks` is
# read too if it has run, and is optional: without it the footprint is capped
# on Taux d'implantation alone. `commercial-rents` likewise: with it the
# commerce and industry are priced at the borough's surveyed rents, without
# it at urban_rag.program's stated constants, which flatter retail badly.
# The objective is discounted net profit - DISCOUNT_PCT / HOLD_YEARS /
# TERMINAL_CAP_PCT / OPEX / RENT_PREMIUM_PCT above are its levers, and
# STALLS_PER_DWELLING and RES_COST_SQFT remain the heaviest two on the cost
# side; see urban_rag.hbu_assets.ProgramConfig for the rest.
programs: | $(UV_SYNC_STAMP) ## Materialize lot_development_programs for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select silver/lot_development_programs --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"silver__lot_development_programs":{"config":{"stalls_per_dwelling":$(STALLS_PER_DWELLING),"residential_cost_per_sqft_cad":$(RES_COST_SQFT),"operating_expense_ratio":$(OPEX),"discount_rate_pct":$(DISCOUNT_PCT),"hold_years":$(HOLD_YEARS),"terminal_cap_rate_pct":$(TERMINAL_CAP_PCT),"new_build_rent_premium_pct":$(RENT_PREMIUM_PCT)}}}}'

# Needs gold.lot_highest_best_use and gold.lot_redevelopment_gap (hbu_infra
# sql/018, sql/019) applied, and `programs` run first for the same partition.
# lot_redevelopment_gap also needs `comparables` for the same partition - the
# assessment side it compares against.
hbu: | $(UV_SYNC_STAMP) ## Materialize lot_highest_best_use and lot_redevelopment_gap for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select gold/lot_highest_best_use,gold/lot_redevelopment_gap --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# Needs gold.lot_investment_opportunities (hbu_infra sql/021) applied and
# `hbu` run first for the same partition. Ranks the under-built lots within
# each investment thesis on yield on cost - a classification and two sorts
# over one parquet, so it is cheap to re-run at a different threshold.
# DOMINANT_SHARE / MIXED_MIN_SHARE move the facet lines, LAND_FACTOR costs
# the land at something other than the roll, TOP_N sets the shortlist length.
opportunities: | $(UV_SYNC_STAMP) ## Rank DATE x NEIGHBORHOOD's under-built lots by thesis
	$(DAGSTER) asset materialize --select gold/lot_investment_opportunities --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"gold__lot_investment_opportunities":{"config":{"dominant_share":$(DOMINANT_SHARE),"mixed_min_share":$(MIXED_MIN_SHARE),"land_value_factor":$(LAND_FACTOR),"top_n":$(TOP_N)}}}}'

# Needs gold.lot_building_massing (hbu_infra sql/022) and gold.lot_surface_parking
# (sql/024) applied - this one asset writes both, the building and the asphalt
# on the yard it leaves - and both `hbu`
# and `setbacks` run first for the same partition: the first supplies the
# footprint to draw, the second the envelope to draw it inside. Without the
# setbacks every row comes back no_buildable_geometry rather than a rectangle
# with the margins ignored. The output is a geoparquet of polygons in
# EPSG:4326 - open it in QGIS beside silver/lot_buildable_setbacks and the
# cadastre. rag.lots is read too, for the parcel outlines the surface parking is
# fitted onto; without it every row is no_lot_geometry and the buildings are
# drawn regardless. RATIOS is the aspect ratios to try, squarest first, as a JSON
# list; footprint_fit_pct below 100 is the column to sort on, and
# surface_parking_fit_pct is the same question about the yard.
massing: | $(UV_SYNC_STAMP) ## Draw DATE x NEIGHBORHOOD's HBU buildings as map polygons
	$(DAGSTER) asset materialize --select gold/lot_building_massing --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"gold__lot_building_massing":{"config":{"aspect_ratios":$(RATIOS)}}}}'

# Needs gold.map_cell_aggregates (hbu_infra sql/023) applied, and runs last:
# it reads rag.lots, rag.buildings, silver.neighborhood_streets,
# gold.lot_redevelopment_gap and gold.lot_building_massing, all of which have
# to hold this partition already. A source that has not been run contributes
# no cells rather than failing the run - the asset's `layers_empty` metadata
# says which - so a borough without a massing still gets a utilisation
# surface.
#
# This is what makes hbu_rag_map draw anything below zoom 15. Its output is
# not read by anything else in this platform: it is one row per tile-grid cell
# per layer, and the map serves display zoom Z from the cells at Z + 4. LAYERS
# narrows it to one layer's cells after that layer's own asset re-ran - but
# note a narrowed run still prunes the partition, so the layers left out are
# removed rather than kept.
map_cells: | $(UV_SYNC_STAMP) ## Dissolve DATE x NEIGHBORHOOD's map layers onto the tile grid
	$(DAGSTER) asset materialize --select gold/map_cell_aggregates --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"gold__map_cell_aggregates":{"config":{"layers":$(LAYERS)}}}}'

# Bronze: one 91 MB download for the whole island, so DATE only.
streets: | $(UV_SYNC_STAMP) ## Snapshot the island-wide geobase double for DATE
	$(DAGSTER) asset materialize --select bronze/street_network --partition $(DATE) -m $(MODULE)

# Owns silver.neighborhood_streets (hbu_infra sql/007), which `frontage` below
# measures against - it used to load that table on its way past, which left it
# with a writer that was not the asset it is named for.
borough-streets: | $(UV_SYNC_STAMP) ## Materialize neighborhood_streets for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select silver/neighborhood_streets --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# Bronze plus the merge that makes it readable, in one run: the roll has no
# borough axis, so DATE only. The first run of a roll year downloads 572 MB and
# unpacks a 2.8 GB GeoPackage into data/cache/role/; every later scrape date
# reuses both. CODE_MUN picks the municipalities out of the province-wide
# archive - `CODE_MUN='[]'` keeps all of them, at ten times the rows.
#
# `cubf_use_codes` rides along because `assessment_units` looks every unit's
# rl0105a up in it - the MEFQ's use-code list, which is what says 4611 is a
# parking garage. 185 kB and uncached, against the roll's 572 MB, so it costs
# nothing to re-fetch; a DATE without it is a partition whose units carry a
# code and no words, and the merge fails naming the asset to run.
#
# Needs `quartiers` for the same DATE, and hbu_infra's sql/014 applied: the
# parquet stays province-wide, but the merge is also cut into borough
# partitions of silver.assessment_units by where each unit's point falls, and
# one run publishes every enabled borough. The file lands first, so a database
# that is down costs a re-run of the load rather than of the merge.
roll: | $(UV_SYNC_STAMP) ## Snapshot the property assessment roll for DATE, its codebook, and merge them
	$(DAGSTER) asset materialize --select bronze/property_assessment_roll,bronze/cubf_use_codes,silver/assessment_units --partition $(DATE) -m $(MODULE) \
		--config-json '{"ops":{"bronze__property_assessment_roll":{"config":{"municipality_codes":$(CODE_MUN)}}}}'

# Needs `roll` for the same DATE and `lots` for the same DATE x NEIGHBORHOOD:
# it puts every assessment unit on the lot its point falls inside, then sums
# rl0404a per lot. Also upserts into silver.lot_assessed_values (hbu_infra
# sql/013), which lands on the first `db.py init` - the file has no
# `-- requires:` header. The parquet is written first, so a database that is
# down costs a re-run of the load rather than of the join.
lot-values: | $(UV_SYNC_STAMP) ## Total DATE's assessment roll onto NEIGHBORHOOD's lots
	$(DAGSTER) asset materialize --select silver/lot_assessed_values --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"silver__lot_assessed_values":{"config":{"place_unmatched_by_point":$(BY_POINT)}}}}'

# The two commercial-rent publishers, both DATE-only: Cushman & Wakefield's
# Montreal office and industrial MarketBeats (two PDFs, discovered off the
# landing page because their filenames change shape every quarter) and
# Statistics Canada's rent index (one 14 kB zipped CSV). Cheap, and the first
# run of a quarter is the only one that pays for the PDFs.
rent-sources: | $(UV_SYNC_STAMP) ## Snapshot the MarketBeats and the rent index for DATE
	$(DAGSTER) asset materialize --select bronze/montreal_commercial_rents,bronze/commercial_rent_index --partition $(DATE) -m $(MODULE)

# Needs `rent-sources` for the same DATE. Resolves one gross rent per rent
# class for the borough: office and industrial off the C&W submarket the
# borough sits in (VSMPE is Midtown North), retail from RETAIL_BASE - the one
# rate with no free survey behind it - and all three carried to the latest
# quarter Statistics Canada publishes. Also upserts into
# silver.commercial_rents (hbu_infra sql/020). See docs/commercial-rents.md.
commercial-rents: | $(UV_SYNC_STAMP) ## Resolve NEIGHBORHOOD's retail, office and industrial rent for DATE
	$(DAGSTER) asset materialize --select silver/commercial_rents --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"silver__commercial_rents":{"config":{"retail_base_gross_rent_psf_cad":$(RETAIL_BASE),"retail_base_period":"$(RETAIL_BASE_PERIOD)"}}}}'

# Needs `lot-values` and both CMHC silver assets (`vacancy`, `rents`) for the
# same DATE x NEIGHBORHOOD: it prices the roll's dwellings and floor area at the
# borough's surveyed rent, then finds each lot's K nearest comparables over the
# same partition. Also upserts into silver.lot_assessment_comparables (hbu_infra
# sql/016), which lands on the first `db.py init` - the file has no
# `-- requires:` header. The parquet is written first, so a database that is
# down costs a re-run of the load rather than of the borough-wide search.
#
# BY_POINT must match the `lot-values` run that produced the partition: it
# decides which units reach a lot at all, and the characteristics summed here
# would otherwise be over a different set of units than the totals there.
# OPEX is the single largest lever on every cap rate; MARKET_FACTOR is 1.0 by
# default, which reports the yield on the roll. See docs/comparables.md.
comparables: | $(UV_SYNC_STAMP) ## Price DATE's roll onto NEIGHBORHOOD's lots and find each lot's comparables
	$(DAGSTER) asset materialize --select silver/lot_assessment_comparables --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"silver__lot_assessment_comparables":{"config":{"k":$(K_COMPARABLES),"max_distance_m":$(MAX_DISTANCE_M),"operating_expense_ratio":$(OPEX),"maintenance_premium_per_year":$(MAINTENANCE_PER_YEAR),"max_maintenance_premium":$(MAX_MAINTENANCE),"assumed_building_age_years":$(ASSUMED_AGE),"market_value_factor":$(MARKET_FACTOR),"place_unmatched_by_point":$(BY_POINT)}}}}'

# Needs silver.neighborhood_streets and silver.lot_frontage (hbu_infra sql/007, sql/008) applied,
# and building_lot_intersections run first for the same partition - that is what
# puts this borough's cadastre in rag.lots. MIN_STREET_M overrides the 1 m
# default, and moving it does not move any frontage: it only changes which
# parcels are read as roadway, and the fixture separates those from every other
# parcel by two orders of magnitude.
frontage: | $(UV_SYNC_STAMP) ## Materialize lot_frontage for DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select silver/lot_frontage --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"silver__lot_frontage":{"config":{"min_street_m":$(MIN_STREET_M)}}}}'

# document_chunks also upserts into silver.document_chunks (hbu_infra sql/011);
# the vectors stay out of the silver schema and go to rag.chunks, which
# `publish` below writes.
corpus: | $(UV_SYNC_STAMP) ## Fetch, chunk and embed the PDFs linked from DATE x NEIGHBORHOOD
	$(DAGSTER) asset materialize --select "bronze/linked_documents,silver/document_chunks,silver/document_embeddings" --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

materialize: catalog features ## Full scrape for DATE x NEIGHBORHOOD

# -- retrieval -------------------------------------------------------------

# The Dagster half of publishing: upserts one partition into the Postgres store
# without touching the others. `make index BACKEND=postgres` is the other half -
# a full reload of every partition, which is what a new encoder needs.
publish: | $(UV_SYNC_STAMP) ## Load DATE x NEIGHBORHOOD into Postgres/pgvector
	$(DAGSTER) asset materialize --select gold/document_index --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

index: | $(UV_SYNC_STAMP) ## (Re)build the vector store from the latest snapshot (BACKEND=...)
	uv run urban-rag index --backend $(BACKEND) --neighborhood $(NEIGHBORHOOD)

# Q is required by both; an empty one would otherwise reach argparse as a
# perfectly valid empty question and embed it.
require-q:
	@[ -n "$(Q)" ] || { echo 'usage: make $(MAKECMDGOALS) Q="your question"' >&2; exit 2; }

search: require-q | $(UV_SYNC_STAMP) ## Retrieve passages, no generation: make search Q="..."
	uv run urban-rag search "$(Q)" -k $(K) --backend $(BACKEND)

ask: require-q | $(UV_SYNC_STAMP) ## Retrieve then answer with a local LLM: make ask Q="..."
	uv run urban-rag ask "$(Q)" -k $(K) --backend $(BACKEND)

status: | $(UV_SYNC_STAMP) ## What is in the vector store
	uv run urban-rag status --backend $(BACKEND)

# -- docker ----------------------------------------------------------------

# Only the two bind-mounting targets below need this: docker creates a missing
# mount source itself, but as root, and then nothing on the host can write to
# it. The targets that run dagster directly get the directory from
# urban_rag.dagster_home instead.
$(DAGSTER_HOME):
	@mkdir -p $(DAGSTER_HOME)

docker-build: ## Build the deployable image
	docker build --target runtime -t $(IMAGE):$(TAG) .

# ~510 MB against several GB, and it still loads every asset - it just can't
# run `ask`/`search`/`index`. See the header of the Dockerfile.
docker-build-slim: ## Build without the retrieval stack (see Dockerfile header)
	docker build --target runtime --build-arg EXTRAS="" -t $(IMAGE):slim .

docker-run: docker-build | $(DAGSTER_HOME) ## Run the Dagster UI in the image
	@mkdir -p data
	$(DOCKER_RUN) $(IMAGE):$(TAG)

docker-shell: docker-build | $(DAGSTER_HOME) ## Shell into the image
	@mkdir -p data
	$(DOCKER_RUN) $(IMAGE):$(TAG) bash

docker-test: ## Run the test suite inside the image
	docker build --target dev -t $(IMAGE):dev .
	docker run --rm -v $(CURDIR):/app -w /app $(IMAGE):dev \
		bash -c "uv sync --locked --extra dev --extra rag && uv run pytest"

up: ## Start webserver + daemon via compose
	docker compose up --build -d

up-tunnel: ## Start compose through an open hbu_infra db-tunnel
	@test -n "$(TUNNEL_DB_HOST)" || { \
	  echo "TUNNEL_DB_HOST is empty; set it to the RDS endpoint, not localhost"; \
	  exit 1; \
	}
	@case "$(TUNNEL_DB_HOST)" in \
	  localhost|127.*|::1) \
	    echo "TUNNEL_DB_HOST must be the RDS endpoint, not $(TUNNEL_DB_HOST), when sslmode=verify-full"; \
	    exit 1 ;; \
	esac
	URBAN_RAG_PG_HOST="$(TUNNEL_DB_HOST)" \
	URBAN_RAG_PG_HOSTADDR= \
	URBAN_RAG_PG_PORT="$(TUNNEL_PORT)" \
	URBAN_RAG_PG_SSLMODE=verify-full \
	URBAN_RAG_PG_SSLROOTCERT=/home/app/.postgresql/root.crt \
	URBAN_RAG_PG_DSN= \
	DAGSTER_POSTGRES_URL= \
	docker compose up --build -d

down: ## Stop the compose stack
	docker compose down

logs: ## Follow the compose logs
	docker compose logs -f

# -- housekeeping ----------------------------------------------------------

clean: ## Remove Python and pytest caches
	rm -rf .pytest_cache
	find . -path ./.venv -prune -o -name __pycache__ -type d -print0 | xargs -0 -r rm -rf

# The PDF/BDOI/CMHC caches and vect_db.duckdb stay: they are keyed by URL or
# filename rather than by scrape date, and the store is rebuilt by `make index`
# from whatever is left. Three directories, one per medallion layer, each
# holding one directory per asset - see docs/architecture.md. Naming
# the layers rather than the assets is the point: an asset added later is
# covered without editing this line.
clean-data: ## Delete every parquet snapshot under data/
	rm -rf data/bronze data/silver data/gold

clean-silver: ## Delete silver and gold, keeping the bronze snapshots
	rm -rf data/silver data/gold
