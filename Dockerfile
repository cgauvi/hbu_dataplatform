# syntax=docker/dockerfile:1.9
#
# Multi-stage image for urban_rag.
#
#   docker build --target runtime -t urban-rag .
#   docker build --target runtime --build-arg EXTRAS="" -t urban-rag:slim .
#   docker build --target dev -t urban-rag:dev .        # what .devcontainer/ uses
#
# EXTRAS defaults to `--extra rag` because the code location does not currently
# load without it: `resources.py` imports `rag.embeddings`, which subclasses
# `langchain_core.embeddings.Embeddings` at module scope. That drags in torch
# and, on Linux, the CUDA runtime behind it - several gigabytes against the
# ~510 MB of the scrape alone.
#
# EXTRAS="" builds that ~510 MB image. It needs one change first: move
# `langchain-core` out of the `rag` extra and into the base dependencies in
# pyproject.toml, then re-lock. Nothing else in the import chain is heavy -
# sentence-transformers and torch are already loaded lazily - so that single
# pure-Python package is the whole difference.
#
# The virtualenv lives at /opt/venv, deliberately outside the project directory:
# a bind-mounted workspace (devcontainer, `docker run -v $PWD:/app`) would
# otherwise shadow a venv installed under /app, and the container would start
# with no dependencies at all.

ARG PYTHON_VERSION=3.12

# -- base ------------------------------------------------------------------

FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS base

ARG UID=1000
ARG GID=1000

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    URBAN_RAG_DATA_DIR=/data \
    DAGSTER_HOME=/dagster_home \
    HF_HOME=/home/app/.cache/huggingface

RUN groupadd --gid ${GID} app \
 && useradd --uid ${UID} --gid ${GID} --create-home --shell /bin/bash app \
 && mkdir -p /app /data /dagster_home /opt/venv \
 # A login shell (`bash -l`, and ECS Exec) sources /etc/profile, which
 # rebuilds PATH from scratch and drops the ENV above, taking the venv with
 # it. Re-export it there so every shell into this image finds the same one.
 && printf 'export PATH=/opt/venv/bin:$PATH\n' > /etc/profile.d/10-venv.sh \
 && printf 'telemetry:\n  enabled: false\n' > /dagster_home/dagster.yaml \
 && chown -R app:app /app /data /dagster_home /opt/venv

WORKDIR /app

# -- dependencies ----------------------------------------------------------
# Only the lock files, so the (slow) dependency layer is reused whenever src/
# changes.

FROM base AS deps

ARG EXTRAS="--extra rag"

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project ${EXTRAS}

# -- runtime ---------------------------------------------------------------

FROM deps AS runtime

ARG EXTRAS="--extra rag"

COPY src ./src
COPY README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked ${EXTRAS} \
 && chown -R app:app /app /opt/venv

USER app

# DuckDB's extension downloader is answered with HTTP 403 behind some
# TLS-inspecting proxies, so this uses the project's own installer, which falls
# back to fetching the binary through requests. Baked in at build time to keep
# the running container off the network; non-fatal, since load_vss() retries on
# first use.
RUN python -c "import duckdb; from urban_rag.rag.vss import load_vss; load_vss(duckdb.connect())" \
 || echo "vss not baked in; it will be installed on first use"

VOLUME ["/data", "/dagster_home"]
EXPOSE 2500

# `dagster dev` runs the webserver and the daemon in one process, which is fine
# for a single container but not for a real deployment - see docker-compose.yml
# for the split the schedules actually need.
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "2500", "-m", "urban_rag.definitions"]

# -- dev -------------------------------------------------------------------
# The devcontainer target. The project itself is NOT installed here: the
# workspace is bind-mounted at run time, and postCreateCommand runs `uv sync`
# against it, which is fast because these dependency layers are already in
# /opt/venv.

FROM base AS dev

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      curl \
      git \
      less \
      make \
      openssh-client \
      procps \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --extra dev --extra rag \
 && rm -f pyproject.toml uv.lock \
 && chown -R app:app /opt/venv

USER app

# Same bake-in as the runtime stage, minus the project's own downloader, which
# is not installed here. Non-fatal: the extension is fetched on first use if
# this could not get it, and `make docker-test` is what needs it.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL vss')" \
 || echo "vss not baked in; it will be installed on first use"

CMD ["sleep", "infinity"]
