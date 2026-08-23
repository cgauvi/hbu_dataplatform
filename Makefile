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

MODULE := urban_rag.definitions
DATE ?= $(shell date +%F)
NEIGHBORHOOD ?= VSMPE
PORT ?= 2500
K ?= 5
# Which vector store the retrieval targets act on: the local DuckDB file, or
# the shared Postgres/pgvector one (configured from URBAN_RAG_PG_*, see the
# README). `make index BACKEND=postgres` reloads the latter from parquet.
BACKEND ?= duckdb

IMAGE ?= urban-rag
TAG ?= latest
DOCKER_RUN := docker run --rm -it \
	-v $(CURDIR)/data:/data \
	-v $(CURDIR)/.dagster_home:/dagster_home \
	-p $(PORT):2500

.PHONY: help sync dagster_run daemon test materialize catalog features \
	quartiers vacancy corpus publish index search ask status require-q clean clean-data \
	pg-bootstrap docker-build docker-build-slim docker-run docker-shell \
	docker-test up down logs

help: ## Show this help
	@echo "Targets:"
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "Vars: DATE=$(DATE) NEIGHBORHOOD=$(NEIGHBORHOOD) PORT=$(PORT) K=$(K)"
	@echo "      BACKEND=$(BACKEND)"
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
	uv run dagster asset materialize --select spectrum_table_catalog --partition $(DATE) -m $(MODULE)

features: | $(DAGSTER_HOME) ## Materialize neighborhood_features for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select neighborhood_features --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

quartiers: | $(DAGSTER_HOME) ## Materialize reference_neighborhoods for DATE
	uv run dagster asset materialize --select reference_neighborhoods --partition $(DATE) -m $(MODULE)

vacancy: | $(DAGSTER_HOME) ## Materialize vacancy_rates for DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select vacancy_rates --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

corpus: | $(DAGSTER_HOME) ## Fetch, chunk and embed the PDFs linked from DATE x NEIGHBORHOOD
	uv run dagster asset materialize --select "linked_documents,document_chunks,document_embeddings" --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

materialize: catalog features ## Full scrape for DATE x NEIGHBORHOOD

# -- retrieval -------------------------------------------------------------

# The Dagster half of publishing: upserts one partition into the Postgres store
# without touching the others. `make index BACKEND=postgres` is the other half -
# a full reload of every partition, which is what a new encoder needs.
publish: | $(DAGSTER_HOME) ## Load DATE x NEIGHBORHOOD into Postgres/pgvector
	uv run dagster asset materialize --select document_index --partition "$(DATE)|$(NEIGHBORHOOD)" -m $(MODULE)

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

# Run once per database, as the master user - it creates what the pipeline's own
# role may not (the role, the schema, the extension). Everything else the first
# load creates for itself. Needs psql on PATH.
#
# The file itself lives in hbu_infra, which is where the master credentials are
# and which owns the rest of the schema. For the shared RDS instance use that
# repo's `make db-bootstrap ENV=<env>`: it resolves the credentials from Secrets
# Manager and writes the generated password back there. This target is the
# manual escape hatch for any other Postgres - a local container, a scratch
# database - where you have a superuser DSN and a password you picked yourself.
#
#   make pg-bootstrap PG_ADMIN_DSN="host=... dbname=urban_rag user=postgres" \
#                     PG_APP_PASSWORD='...'
INFRA ?= ../hbu_infra
pg-bootstrap: ## Create the role, schema and extension (master user; see hbu_infra)
	@[ -n "$(PG_ADMIN_DSN)" ] || { echo 'usage: make pg-bootstrap PG_ADMIN_DSN="host=... dbname=urban_rag user=<master>" PG_APP_PASSWORD=...' >&2; exit 2; }
	@[ -n "$(PG_APP_PASSWORD)" ] || { echo 'PG_APP_PASSWORD is required - the role is created without one otherwise' >&2; exit 2; }
	@[ -f "$(INFRA)/sql/000_roles.sql" ] || { echo '$(INFRA)/sql/000_roles.sql not found - set INFRA to your hbu_infra checkout' >&2; exit 2; }
	psql "$(PG_ADMIN_DSN)" -v ON_ERROR_STOP=1 \
		-c "CREATE EXTENSION IF NOT EXISTS vector" \
		-f $(INFRA)/sql/000_roles.sql \
		-c "ALTER ROLE urban_rag WITH PASSWORD '$(PG_APP_PASSWORD)'"

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

# The PDF cache and vect_db.duckdb stay: the cache is keyed by URL rather than
# by scrape date, and the store is rebuilt by `make index` from whatever is
# left. One directory per asset - see "Output layout" in the README.
clean-data: ## Delete every parquet snapshot under data/
	rm -rf data/spectrum_table_catalog data/neighborhood_features \
		data/reference_neighborhoods data/vacancy_rates data/linked_documents \
		data/document_chunks data/document_embeddings
