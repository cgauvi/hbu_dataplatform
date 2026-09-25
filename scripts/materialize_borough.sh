#!/usr/bin/env bash
# Materialize one borough's chain for one scrape month, step by step, from WSL.
#
#   scripts/materialize_borough.sh CIL 2026-09-01 [step ...]
#
# Runs the dagster CLI directly rather than through `make`, because uv is not
# on the WSL side and the Makefile's targets all go through `uv run`. Each step
# is one `dagster asset materialize`, logged to $LOG_DIR/<step>.log, and the
# script stops at the first failure so a re-run can start from the step that
# broke: name the steps to run as extra arguments (`... CIL 2026-09-01 lots
# buildings`), or none for the whole list below.
#
# Three kinds of step. The date-only steps (`quartiers`, `streets`, `roll`)
# are shared by every borough and re-snapshot the month. The borough steps are
# the DATE x NEIGHBORHOOD partition - the publisher's unit, which is what the
# bronze fetches, the grids and the corpus are bounded by. The tile steps are
# the lot chain, DATE x TILE, one run per cell of the cut the borough's lots
# fall in: `cadastre` lands the borough in rag.lots with each lot's cell, and
# every tile step after it asks Postgres which cells those were
# (`python -m urban_rag.tiles of`) and runs once per cell. That is also why a
# reload of a borough re-runs the whole chain for every cell it touches - a
# reload remints lot_uid and cascades into all of them.
#
# Order matters and mirrors docs/running.md: every silver step before any
# gold one, and `cadastre` before any tile step.
#
# Environment: reads .env like the Makefile does (AWS profile, S3 bucket, the
# Postgres settings for the tunnel on 127.0.0.1:5433), and expects the tunnel
# to be up - see hbu_infra `make db-tunnel ENV=dev`.
set -euo pipefail

NEIGHBORHOOD="${1:?neighborhood key, e.g. CIL}"
DATE="${2:?scrape month, e.g. 2026-09-01}"
shift 2

cd "$(dirname "$0")/.."
export DAGSTER_HOME="$PWD/.dagster_home"
export PYTHONPATH=src
export PATH="$PWD/.venv/bin:$PATH"
export AWS_PROFILE="${AWS_PROFILE:-charles_gauvin_east_1}"
# The CA bundles come from .env (urban_rag.dagster_home loads it): the
# combined Zscaler-plus-certifi file, which is what every publisher here needs.
LOG_DIR="${LOG_DIR:-$PWD/.dagster_home/logs/materialize/$NEIGHBORHOOD/$DATE}"
mkdir -p "$LOG_DIR"

PY=".venv/bin/python -m urban_rag.dagster_home"
MODULE=urban_rag.definitions
CODE_MUN="${CODE_MUN:-[\"66023\",\"23027\",\"94068\"]}"

# step name -> "<partition kind> <asset selection> [extra dagster args]"
declare -A STEPS=(
  [register]="- -"
  [quartiers]="date bronze/reference_neighborhoods"
  [features]="borough bronze/neighborhood_features"
  [lots]="borough bronze/neighborhood_lots"
  [buildings]="borough bronze/neighborhood_buildings"
  [cadastre]="borough silver/neighborhood_cadastre"
  [streets]="date bronze/street_network"
  [tile-streets]="tile silver/neighborhood_streets"
  [building-lots]="tile silver/building_lot_intersections"
  [frontage]="tile silver/lot_frontage"
  [zone-pieces]="tile silver/lot_zone_pieces"
  [grid-columns]="borough silver/zoning_grid_columns"
  [envelopes]="tile silver/lot_zoning_envelopes"
  [setbacks]="tile silver/lot_buildable_setbacks"
  [roll]="date bronze/property_assessment_roll,bronze/cubf_use_codes,silver/assessment_units --config-json {\"ops\":{\"bronze__property_assessment_roll\":{\"config\":{\"municipality_codes\":${CODE_MUN}}}}}"
  [lot-values]="tile silver/lot_assessed_values"
  [cmhc]="date bronze/cmhc_vacancy_survey,bronze/cmhc_rent_survey"
  [vacancy]="borough silver/vacancy_rates"
  [rents]="borough silver/average_rents"
  [rent-sources]="date bronze/montreal_commercial_rents,bronze/commercial_rent_index"
  [commercial-rents]="borough silver/commercial_rents"
  [costs]="date bronze/montreal_residential_costs,bronze/montreal_nonresidential_costs"
  [rfu]="date bronze/uniformized_property_wealth"
  [addresses]="borough bronze/neighborhood_addresses"
  [lot-addresses]="tile silver/lot_addresses"
  [comparables]="tile silver/lot_assessment_comparables"
  [programs]="tile silver/lot_development_programs"
  [lot-profiles]="tile gold/lot_profiles"
  [hbu]="tile gold/lot_highest_best_use,gold/lot_redevelopment_gap"
  [opportunities]="tile gold/lot_investment_opportunities"
  [massing]="tile gold/lot_building_massing"
  [map-cells]="borough gold/map_cell_aggregates"
  [map-tiles]="borough gold/map_tiles"
  [corpus]="borough bronze/linked_documents,silver/document_chunks,silver/document_embeddings"
  [publish]="borough gold/document_index"
  # Quebec City only: the conseils de quartier minutes and their trail. Not in
  # DEFAULT_ORDER - a Montreal borough has none - so it is asked for by name.
  [council-minutes]="borough bronze/council_minutes,bronze/council_minutes_documents,silver/council_planning_items"
)
# `corpus` needs only `features` - it reads the zoning parquet's LIEN_GRILLE and
# nothing else - but it is last because it is the one step that downloads a
# document per zone and embeds it, and a chain that breaks earlier should break
# before spending that. `publish` is what puts the vectors in rag.chunks, which
# is what makes retrieval and the map's "Retrieved passages" answer at all.
DEFAULT_ORDER=(register quartiers features lots buildings cadastre streets
  tile-streets building-lots frontage zone-pieces grid-columns envelopes setbacks
  roll lot-values corpus publish)

ORDER=("$@")
if [ ${#ORDER[@]} -eq 0 ]; then ORDER=("${DEFAULT_ORDER[@]}"); fi

# The cells this borough's loaded lots fall in - asked once, the first time a
# tile step comes up, so `cadastre` has had its chance to run first.
TILES=()
tiles_of_borough() {
  if [ ${#TILES[@]} -eq 0 ]; then
    mapfile -t TILES < <($PY python -m urban_rag.tiles of "$NEIGHBORHOOD" "$DATE")
    echo "$(date '+%H:%M:%S')  $NEIGHBORHOOD holds ${#TILES[@]} cell(s): ${TILES[*]}"
  fi
}

run_step() {  # <log> <partition> <selection> <extra...>
  local log="$1" partition="$2" selection="$3"; shift 3
  # shellcheck disable=SC2086
  $PY dagster asset materialize --select "$selection" --partition "$partition" -m $MODULE "$@" > "$log" 2>&1
}

for step in "${ORDER[@]}"; do
  spec="${STEPS[$step]:?unknown step $step}"
  kind="${spec%% *}"; rest="${spec#* }"
  selection="${rest%% *}"; extra="${rest#"$selection"}"
  log="$LOG_DIR/$step.log"
  started=$(date +%s)
  echo "$(date '+%H:%M:%S')  $step  -> $log"
  status=0
  case "$kind" in
    -)  $PY python -m urban_rag.neighborhoods add "$NEIGHBORHOOD" > "$log" 2>&1 || status=$? ;;
    date)
        # shellcheck disable=SC2086
        run_step "$log" "$DATE" "$selection" $extra || status=$? ;;
    borough)
        # shellcheck disable=SC2086
        run_step "$log" "$DATE|$NEIGHBORHOOD" "$selection" $extra || status=$? ;;
    tile)
        tiles_of_borough || { status=$?; }
        if [ $status -eq 0 ]; then
          for tile in "${TILES[@]}"; do
            log="$LOG_DIR/$step.$tile.log"
            echo "$(date '+%H:%M:%S')    $tile  -> $log"
            # shellcheck disable=SC2086
            run_step "$log" "$DATE|$tile" "$selection" $extra || { status=$?; break; }
          done
        fi ;;
  esac
  elapsed=$(( $(date +%s) - started ))
  if [ $status -ne 0 ]; then
    echo "$(date '+%H:%M:%S')  $step  FAILED after ${elapsed}s (exit $status); see $log"
    tail -n 30 "$log"
    exit $status
  fi
  echo "$(date '+%H:%M:%S')  $step  done in ${elapsed}s"
done
echo "all steps done for $NEIGHBORHOOD $DATE"
