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
# The date-only steps (`quartiers`, `streets`, `roll`) are shared by every
# borough and re-snapshot the month; the borough steps are the DATE x
# NEIGHBORHOOD partition. Order matters and mirrors docs/running.md: every
# silver step before any gold one, since `building-lots` reloads rag.lots.
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
  [streets]="date bronze/street_network"
  [borough-streets]="borough silver/neighborhood_streets"
  [building-lots]="borough silver/building_lot_intersections"
  [frontage]="borough silver/lot_frontage"
  [zone-pieces]="borough silver/lot_zone_pieces"
  [grid-columns]="borough silver/zoning_grid_columns"
  [envelopes]="borough silver/lot_zoning_envelopes"
  [setbacks]="borough silver/lot_buildable_setbacks"
  [roll]="date bronze/property_assessment_roll,bronze/cubf_use_codes,silver/assessment_units --config-json {\"ops\":{\"bronze__property_assessment_roll\":{\"config\":{\"municipality_codes\":${CODE_MUN}}}}}"
  [lot-values]="borough silver/lot_assessed_values"
  [cmhc]="date bronze/cmhc_vacancy_survey,bronze/cmhc_rent_survey"
  [vacancy]="borough silver/vacancy_rates"
  [rents]="borough silver/average_rents"
  [rent-sources]="date bronze/montreal_commercial_rents,bronze/commercial_rent_index"
  [commercial-rents]="borough silver/commercial_rents"
  [costs]="date bronze/montreal_residential_costs,bronze/montreal_nonresidential_costs"
  [rfu]="date bronze/uniformized_property_wealth"
  [comparables]="borough silver/lot_assessment_comparables"
  [programs]="borough silver/lot_development_programs"
  [lot-profiles]="borough gold/lot_profiles"
  [hbu]="borough gold/lot_highest_best_use,gold/lot_redevelopment_gap"
  [opportunities]="borough gold/lot_investment_opportunities"
  [massing]="borough gold/lot_building_massing"
  [map-cells]="borough gold/map_cell_aggregates"
  [corpus]="borough bronze/linked_documents,silver/document_chunks,silver/document_embeddings"
  [publish]="borough gold/document_index"
)
# `corpus` needs only `features` - it reads the zoning parquet's LIEN_GRILLE and
# nothing else - but it is last because it is the one step that downloads a
# document per zone and embeds it, and a chain that breaks earlier should break
# before spending that. `publish` is what puts the vectors in rag.chunks, which
# is what makes retrieval and the map's "Retrieved passages" answer at all.
DEFAULT_ORDER=(register quartiers features lots buildings streets borough-streets
  building-lots frontage zone-pieces grid-columns envelopes setbacks
  roll lot-values corpus publish)

ORDER=("$@")
if [ ${#ORDER[@]} -eq 0 ]; then ORDER=("${DEFAULT_ORDER[@]}"); fi

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
        $PY dagster asset materialize --select "$selection" --partition "$DATE" -m $MODULE $extra > "$log" 2>&1 || status=$? ;;
    borough)
        # shellcheck disable=SC2086
        $PY dagster asset materialize --select "$selection" --partition "$DATE|$NEIGHBORHOOD" -m $MODULE $extra > "$log" 2>&1 || status=$? ;;
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
