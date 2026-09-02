#!/usr/bin/env bash
# Drives the 2026-09-01 rebuild from WSL. Each step prints only its verdict.
#
# An argument is either a Makefile target, or `asset:<selection>` for the few
# assets the Makefile has no target for (building_lot_intersections).
set -u
cd /mnt/c/Users/cgauvin/Documents/Dev/urban/hbu_dataplatform
export AWS_PROFILE=charles_gauvin_east_1 AWS_DEFAULT_REGION=us-east-1
export AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
# `bash <script>` is not a login shell, so uv is not on PATH by default.
export PATH="$HOME/.local/bin:$PATH"
# The Makefile exports this; the raw dagster CLI path needs it too, or it
# defaults to /dagster_home and dies on mkdir.
export DAGSTER_HOME="$PWD/.dagster_home"
DATE=2026-09-01
NEIGHBORHOOD=VSMPE
for step in "$@"; do
  name="${step#asset:}"; name="${name//\//_}"
  echo "########## $step  ($(date +%H:%M:%S))"
  start=$(date +%s)
  if [ "$step" = "${step#asset:}" ]; then
    timeout 5400 make "$step" DATE="$DATE" > "/tmp/${name}.log" 2>&1
  else
    timeout 5400 uv run python -m urban_rag.dagster_home dagster asset materialize \
      --select "${step#asset:}" --partition "$DATE|$NEIGHBORHOOD" \
      -m urban_rag.definitions > "/tmp/${name}.log" 2>&1
  fi
  code=$?
  elapsed=$(( $(date +%s) - start ))
  [ $code -eq 0 ] && verdict=OK || verdict="FAILED(exit $code)"
  echo "  $verdict in $((elapsed / 60))m$((elapsed % 60))s"
  grep -E "RUN_FAILURE|STEP_FAILURE|dagster - ERROR|Failure:" "/tmp/${name}.log" | tail -3
  grep -E "dagster - INFO - __ASSET_JOB.* - (silver|gold|bronze)__" "/tmp/${name}.log" | tail -4
  [ $code -eq 0 ] || { echo "  stopping: downstream steps depend on this one"; exit 1; }
done
