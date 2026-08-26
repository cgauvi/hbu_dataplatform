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
not from Git Bash - see the Containers section of the README.)
endif

export DAGSTER_HOME := $(CURDIR)/.dagster_home

# Assets are selected by their full `<layer>/<asset>` key, which is what
# `key_prefix` in urban_rag.layers gives them. A bare name resolves to no
# AssetsDefinition and dagster answers DagsterInvalidSubsetError, so adding a
# target means looking the asset's layer up rather than copying its name.
MODULE := urban_rag.definitions
DATE ?= $(shell date +%F)
NEIGHBORHOOD ?= VSMPE
PORT ?= 2500
K ?= 5
# Which vector store the retrieval targets act on: the local DuckDB file, or
# the shared Postgres/pgvector one (configured from URBAN_RAG_PG_*, see the
# README). `make index BACKEND=postgres` reloads the latter from parquet.
BACKEND ?= duckdb
# How far behind the curb line a lot boundary still counts as facing it, for
# `make frontage`. See silver/lot_frontage in the README.
BUFFER_M ?= 3.0

IMAGE ?= urban-rag
TAG ?= latest
DOCKER_RUN := docker run --rm -it \
	-v $(CURDIR)/data:/data \
	-v $(CURDIR)/.dagster_home:/dagster_home \
	-p $(PORT):2500

.PHONY: help sync dagster_run daemon test materialize catalog features \
	quartiers cmhc costs vacancy rents envelopes lot-profiles \
	streets borough-streets frontage corpus publish index search ask status \
	require-q clean clean-data clean-silver \
	docker-build docker-build-slim docker-run docker-shell docker-test up down logs

help: ## Show this help
	@echo "Targets:"
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "Vars: DATE=$(DATE) NEIGHBORHOOD=$(NEIGHBORHOOD) PORT=$(PORT) K=$(K)"
	@echo "      BACKEND=$(BACKEND) BUFFER_M=$(BUFFER_M)"
	@echo "      IMAGE=$(IMAGE):$(TAG)"
	@echo "      DAGSTER_HOME=$(DAGSTER_HOME)"

# -- environment -----------------------------------------------------------

# `--extra rag` pulls in the retrieval stack (sentence-transformers, torch,
# transformers) for `ask`/`search`/`index`; the code location loads fine
# without it too (see docker-build-slim). The unset works around
# SSL_CERT_FILE breaking uv's resolver on a managed laptop; it is a no-op
# everywhere else.
sync: ## Install deps, including the retrieval stack
	unset SSL_CERT_FILE; UV_SYSTEM_CERTS=1 uv sync --python 3.12 --extra dev --extra rag

test: ## Run the test suite (offline)
	uv run pytest

# -- pipeline --------------------------------------------------------------

$(DAGSTER_HOME):
	@mkdir -p $(DAGSTER_HOME)

# Binds 0.0.0.0, not localhost: in a container the UI is only reachable from
# the host if it listens on the container's external interface.
dagster_run: | $(DAGSTER_HOME) ## Launch the Dagster UI (PORT=2500)
	uv run dagster dev -h 0.0.0.0 -p $(PORT)

daemon: | $(DAGSTER_HOME) ## Run the daemon alone (what the schedules need)
	uv run dagster-daemon run -m $(MODULE)

catalog: | $(DAGSTER_HOME) ## Materialize spectrum_table_catalog for DATE
	uv run dagster asset materialize --select bronze/spectrum_table_catalog --partition $(DATE) -m $(MODULE)

features: | $(DAGSTER_HOME) ## Materialize neighborhood_features for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select bronze/neighborhood_features --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

quartiers: | $(DAGSTER_HOME) ## Materialize reference_neighborhoods for DATE
	uv run dagster asset materialize --select bronze/reference_neighborhoods --partition $(DATE) -m $(MODULE)

# Bronze: one workbook read for the whole island, so DATE only.
cmhc: | $(DAGSTER_HOME) ## Snapshot both CMHC surveys for DATE
	uv run dagster asset materialize --select bronze/cmhc_vacancy_survey,bronze/cmhc_rent_survey --partition $(DATE) -m $(MODULE)

# Bronze: one script read for the whole guide, so DATE only.
costs: | $(DAGSTER_HOME) ## Snapshot the Montreal construction cost rates for DATE
	uv run dagster asset materialize --select bronze/montreal_residential_costs,bronze/montreal_nonresidential_costs --partition $(DATE) -m $(MODULE)

vacancy: | $(DAGSTER_HOME) ## Materialize vacancy_rates for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select silver/vacancy_rates --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

rents: | $(DAGSTER_HOME) ## Materialize average_rents for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select silver/average_rents --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# The two envelope assets, which lot_profiles now reads: the grids are
# parsed from the PDFs the corpus already downloaded.
envelopes: | $(DAGSTER_HOME) ## Materialize the zoning envelopes for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select silver/zoning_grid_columns,silver/lot_zoning_envelopes --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# Needs rag.lot_profiles and the rag.lot_documents view (hbu_infra sql/009,
# sql/006) applied, and 006 only lands on a db.py init run *after* a corpus has
# been indexed - see urban_rag.lot_profiles_assets. Also reads three silver
# parquet partitions the database knows nothing about: `envelopes`, `vacancy`
# and `rents` for the same DATE x NEIGHBORHOOD.
lot-profiles: | $(DAGSTER_HOME) ## Materialize lot_profiles for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select gold/lot_profiles --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# Bronze: one 91 MB download for the whole island, so DATE only.
streets: | $(DAGSTER_HOME) ## Snapshot the island-wide geobase double for DATE
	uv run dagster asset materialize --select bronze/street_network --partition $(DATE) -m $(MODULE)

borough-streets: | $(DAGSTER_HOME) ## Materialize neighborhood_streets for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select silver/neighborhood_streets --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

# Needs rag.streets and rag.lot_frontage (hbu_infra sql/007, sql/008) applied,
# and building_lot_intersections run first for the same partition - that is what
# puts this borough's cadastre in rag.lots. BUFFER_M overrides the 3 m default.
frontage: | $(DAGSTER_HOME) ## Materialize lot_frontage for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select silver/lot_frontage --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE) \
		--config-json '{"ops":{"silver__lot_frontage":{"config":{"buffer_m":$(BUFFER_M)}}}}'

corpus: | $(DAGSTER_HOME) ## Fetch, chunk and embed the PDFs linked from DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select "bronze/linked_documents,silver/document_chunks,silver/document_embeddings" --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

materialize: catalog features ## Full scrape for DATE x NEIGHBORHOOD

# -- retrieval -------------------------------------------------------------

# The Dagster half of publishing: upserts one partition into the Postgres store
# without touching the others. `make index BACKEND=postgres` is the other half -
# a full reload of every partition, which is what a new encoder needs.
publish: | $(DAGSTER_HOME) ## Load DATE x NEIGHBORHOOD into Postgres/pgvector
	uv run dagster asset materialize --select gold/document_index --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

index: ## (Re)build the vector store from the latest snapshot (BACKEND=...)
	uv run urban-rag index --backend $(BACKEND) --neighborhood $(NEIGHBORHOOD)

# Q is required by both; an empty one would otherwise reach argparse as a
# perfectly valid empty question and embed it.
require-q:
	@[ -n "$(Q)" ] || { echo 'usage: make $(MAKECMDGOALS) Q="your question"' >&2; exit 2; }

search: require-q ## Retrieve passages, no generation: make search Q="..."
	uv run urban-rag search "$(Q)" -k $(K) --backend $(BACKEND)

ask: require-q ## Retrieve then answer with a local LLM: make ask Q="..."
	uv run urban-rag ask "$(Q)" -k $(K) --backend $(BACKEND)

status: ## What is in the vector store
	uv run urban-rag status --backend $(BACKEND)

# -- docker ----------------------------------------------------------------

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
# holding one directory per asset - see "Output layout" in the README. Naming
# the layers rather than the assets is the point: an asset added later is
# covered without editing this line.
clean-data: ## Delete every parquet snapshot under data/
	rm -rf data/bronze data/silver data/gold

clean-silver: ## Delete silver and gold, keeping the bronze snapshots
	rm -rf data/silver data/gold
