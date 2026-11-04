#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 PLAN_TSV CAMPAIGN_DIR [REPETITIONS=30] [NODE_COUNT=8]" >&2
  exit 2
fi

plan_file=$1
campaign_dir=$2
repetitions=${3:-30}
node_count=${4:-8}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
lab_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
root_dir=$(CDPATH= cd -- "$lab_dir/.." && pwd)
matrix_runner="$script_dir/run-product-udp-pre-ec2-matrix.sh"
analyzer="$lab_dir/scripts/analyze-udp-pre-ec2.py"
validator="$lab_dir/scripts/validate-product-campaign.py"
docker_bin=${DOCKER_BIN:-docker}
attach_fault=${FD_CAMPAIGN_ATTACH_FAULT:-}
poll_seconds=${FD_CAMPAIGN_POLL_SECONDS:-30}
max_stagnant_polls=${FD_CAMPAIGN_MAX_STAGNANT_POLLS:-20}
max_attempts=${FD_CAMPAIGN_MAX_ATTEMPTS:-3}
final_report="$lab_dir/results-product-pre-ec2-final-20261101/report"
stop_requested=0

case "$plan_file" in
  /*) ;;
  *) plan_file="$root_dir/$plan_file" ;;
esac
case "$campaign_dir" in
  /*) ;;
  *) campaign_dir="$root_dir/$campaign_dir" ;;
esac

request_stop() {
  stop_requested=1
}
trap request_stop INT TERM

mkdir -p "$campaign_dir/validations"
log_file="$campaign_dir/campaign.log"
completed_stages="$campaign_dir/completed-stages.tsv"
if [[ ! -f "$completed_stages" ]]; then
  printf 'completed_at\tfault\toutput_dir\n' >"$completed_stages"
fi

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$log_file"
}

resolve_output() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$root_dir" "$1" ;;
  esac
}

passed_count() {
  python3 - "$1/execution-status.csv" <<'PY'
import csv
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print(0)
else:
    with path.open(encoding="utf-8", newline="") as source:
        print(len({int(row["sequence"]) for row in csv.DictReader(source) if row.get("status") == "passed"}))
PY
}

wait_for_attached_stage() {
  local fault=$1
  local output=$2
  local last=-1
  local stagnant=0
  local current
  log "ATTACH fault=$fault output=$output"
  while [[ ! -f "$output/COMPLETED" ]]; do
    if (( stop_requested )); then
      log "PAUSED while waiting for attached fault=$fault"
      exit 130
    fi
    current=$(passed_count "$output")
    if [[ "$current" != "$last" ]]; then
      log "PROGRESS fault=$fault passed=$current expected=$((repetitions * 5))"
      last=$current
      stagnant=0
    else
      stagnant=$((stagnant + 1))
    fi
    if (( stagnant >= max_stagnant_polls )); then
      log "STOP attached stage made no checkpoint progress for $((poll_seconds * stagnant)) seconds"
      exit 1
    fi
    sleep "$poll_seconds"
  done
  log "ATTACHED stage completed fault=$fault"
}

check_host() {
  local available_kb
  "$docker_bin" version --format '{{.Server.Version}}' >/dev/null
  available_kb=$(df -Pk "$root_dir" | awk 'NR == 2 { print $4 }')
  if (( available_kb < 10 * 1024 * 1024 )); then
    log "STOP less than 10 GiB free space"
    exit 1
  fi
}

run_stage() {
  local fault=$1
  local output=$2
  local attempt=0
  local resume
  local rc
  while [[ ! -f "$output/COMPLETED" ]]; do
    if (( stop_requested )); then
      log "PAUSED before fault=$fault"
      exit 130
    fi
    attempt=$((attempt + 1))
    if (( attempt > max_attempts )); then
      log "STOP fault=$fault exceeded max attempts=$max_attempts"
      exit 1
    fi
    check_host
    resume=0
    [[ -e "$output" ]] && resume=1
    log "START fault=$fault attempt=$attempt resume=$resume"
    set +e
    FD_MATRIX_FAULTS="$fault" \
      FD_MATRIX_RESUME="$resume" \
      DOCKER_BIN="$docker_bin" \
      "$matrix_runner" "$output" "$repetitions" "$node_count"
    rc=$?
    set -e
    if (( rc == 130 || stop_requested )); then
      log "PAUSED fault=$fault rc=$rc"
      exit 130
    fi
    if (( rc != 0 )); then
      log "RETRY fault=$fault rc=$rc"
    fi
  done
  log "EXECUTION completed fault=$fault"
}

analyze_and_validate_stage() {
  local fault=$1
  local output=$2
  local expected=$((repetitions * 5))
  python3 "$analyzer" --input "$output" --output "$output/report"
  python3 "$validator" \
    --stage-input "$output" \
    --report "$output/report" \
    --expected-total "$expected" \
    --expected-profiles 5 \
    --expected-faults 1 \
    --expected-runs-per-profile "$repetitions" \
    --output "$campaign_dir/validations/$fault.json"
  log "VALIDATION passed fault=$fault"
}

merge_completed_stages() {
  local current_fault=$1
  local fault
  local configured_output
  local resolved_output
  local -a command=(python3 "$analyzer")
  command+=(--input "$lab_dir/results-product-pre-ec2-validation-20261030-v2")
  command+=(--input "$lab_dir/results-product-pre-ec2-invalid-rerun-20261031")
  command+=(--input "$lab_dir/results-product-redis-all-racefix-20261101")
  while IFS=$'\t' read -r fault configured_output; do
    [[ "$fault" == "fault" ]] && continue
    resolved_output=$(resolve_output "$configured_output")
    if [[ -f "$resolved_output/COMPLETED" ]]; then
      command+=(--input "$resolved_output")
    fi
    [[ "$fault" == "$current_fault" ]] && break
  done <"$plan_file"
  command+=(--output "$final_report")
  "${command[@]}"
  log "MERGE completed through fault=$current_fault"
}

log "CAMPAIGN start plan=$plan_file repetitions=$repetitions nodes=$node_count"
# Read the campaign plan from a dedicated descriptor so commands executed inside
# the loop cannot consume the remaining plan rows through inherited stdin.
while IFS=$'\t' read -r -u 9 fault configured_output; do
  [[ "$fault" == "fault" ]] && continue
  output=$(resolve_output "$configured_output")
  if [[ ! -f "$output/COMPLETED" && "$fault" == "$attach_fault" ]]; then
    wait_for_attached_stage "$fault" "$output"
    attach_fault=
  fi
  if [[ ! -f "$output/COMPLETED" ]]; then
    run_stage "$fault" "$output"
  else
    log "SKIP execution fault=$fault status=completed"
  fi
  analyze_and_validate_stage "$fault" "$output"
  merge_completed_stages "$fault"
  if ! awk -F'\t' -v wanted="$fault" '$2 == wanted { found = 1 } END { exit found ? 0 : 1 }' "$completed_stages"; then
    printf '%s\t%s\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$fault" "$configured_output" \
      >>"$completed_stages"
  fi
done 9<"$plan_file"

expected_total=$((repetitions * 5 * 11))
python3 "$validator" \
  --report "$final_report" \
  --expected-total "$expected_total" \
  --expected-profiles 5 \
  --expected-faults 11 \
  --expected-runs-per-profile "$((repetitions * 11))" \
  --output "$campaign_dir/final-validation.json"
"$root_dir/gradlew" -p "$lab_dir" test selfTest
printf 'completed_at=%s\nruns=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$expected_total" \
  >"$campaign_dir/COMPLETED"
printf 'completed_at=%s\nnotion_page=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  'https://app.notion.com/p/409792e9ac6a817ba821f5f050dfa2ee?pvs=204' \
  >"$campaign_dir/NOTION_UPDATE_PENDING"
log "CAMPAIGN complete runs=$expected_total notion_update=pending"
