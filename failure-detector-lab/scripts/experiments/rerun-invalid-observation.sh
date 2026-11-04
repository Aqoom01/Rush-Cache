#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 SOURCE_REPORT_DIR OUTPUT_DIR" >&2
  echo "resume: FD_MATRIX_RESUME=1 $0 SOURCE_REPORT_DIR EXISTING_OUTPUT_DIR" >&2
  exit 2
fi

source_report=$1
output_dir=$2
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
lab_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
root_dir=$(CDPATH= cd -- "$lab_dir/.." && pwd)
single_runner="$script_dir/run-udp-netem.sh"
docker_bin=${DOCKER_BIN:-docker}
resume=${FD_MATRIX_RESUME:-0}

if [[ ! -f "$source_report/runs.csv" ]]; then
  echo "missing source report: $source_report/runs.csv" >&2
  exit 1
fi

if [[ -e "$output_dir" ]]; then
  if [[ "$resume" != "1" ]]; then
    echo "output directory already exists: $output_dir" >&2
    exit 1
  fi
  for required in plan.tsv execution-status.csv; do
    [[ -f "$output_dir/$required" ]] || {
      echo "cannot resume: missing $output_dir/$required" >&2
      exit 1
    }
  done
else
  [[ "$resume" != "1" ]] || {
    echo "cannot resume: output directory does not exist: $output_dir" >&2
    exit 1
  }
  mkdir -p "$output_dir"
  python3 - "$source_report/runs.csv" "$output_dir/plan.tsv" <<'PY'
import csv
import sys

mapping = {
    "swim-lifeguard": "product-swim-lifeguard",
    "redis-cluster": "product-redis-cluster",
    "phi-gossip": "product-phi-gossip",
    "ignite-tcp-ring": "product-ignite-tcp-ring",
    "aerospike-mesh": "product-aerospike-mesh",
}
with open(sys.argv[1], encoding="utf-8", newline="") as source:
    invalid = [
        row for row in csv.DictReader(source)
        if row["valid_observation_window"].lower() != "true"
    ]
invalid.sort(key=lambda row: (row["profile"], row["fault"], int(row["seed"])))
with open(sys.argv[2], "w", encoding="utf-8", newline="") as output:
    writer = csv.writer(output, delimiter="\t", lineterminator="\n")
    writer.writerow(["sequence", "seed", "fault", "profile", "requested_profile", "nodes"])
    for sequence, row in enumerate(invalid, 1):
        writer.writerow([
            sequence,
            row["seed"],
            row["fault"],
            row["profile"],
            mapping[row["profile"]],
            row["node_count"],
        ])
print(f"planned_invalid_runs={len(invalid)}")
PY
  printf 'sequence,seed,fault,profile,status,run_dir\n' >"$output_dir/execution-status.csv"
  printf 'source_report=%s\nsource=real-socket-observed\nreason=minimum-post-fault-observation-window\n' \
    "$source_report" >"$output_dir/rerun.properties"
fi

"$root_dir/gradlew" -p "$lab_dir" jar
"$docker_bin" build \
  --file "$lab_dir/docker/Dockerfile.udp" \
  --tag openkache-fd-udp:local \
  "$lab_dir"

failures=0
while IFS=$'\t' read -r sequence seed fault profile requested_profile nodes <&3; do
  [[ "$sequence" == "sequence" ]] && continue
  if awk -F, -v wanted="$sequence" \
      '$1 == wanted && $5 == "passed" { found = 1 } END { exit found ? 0 : 1 }' \
      "$output_dir/execution-status.csv"; then
    echo "SKIP sequence=$sequence status=passed"
    continue
  fi
  base_run_dir=$(printf '%s/runs/%s/%s/seed-%03d' \
    "$output_dir" "$profile" "$fault" "$seed")
  run_dir=$base_run_dir
  retry=0
  while [[ -e "$run_dir" || -e "$run_dir.runner.log" ]]; do
    retry=$((retry + 1))
    run_dir="$base_run_dir-retry-$retry"
  done
  mkdir -p "$(dirname -- "$run_dir")"
  echo "START sequence=$sequence profile=$profile fault=$fault seed=$seed"
  if FD_UDP_SKIP_BUILD=1 \
      FD_WARMUP_SECONDS=15 \
      FD_FAULT_DURATION_SECONDS=10 \
      FD_RECOVERY_SECONDS=15 \
      DOCKER_BIN="$docker_bin" \
      "$single_runner" "$requested_profile" "$fault" "$run_dir" "$nodes" "$seed" \
      >"$run_dir.runner.log" 2>&1; then
    status=passed
  else
    status=failed
    failures=$((failures + 1))
  fi
  printf '%s,%s,%s,%s,%s,%s\n' \
    "$sequence" "$seed" "$fault" "$profile" "$status" "$run_dir" \
    >>"$output_dir/execution-status.csv"
  echo "DONE sequence=$sequence status=$status"
done 3<"$output_dir/plan.tsv"

remaining=$(python3 - "$output_dir/plan.tsv" "$output_dir/execution-status.csv" <<'PY'
import csv
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    planned = {int(row["sequence"]) for row in csv.DictReader(source, delimiter="\t")}
with open(sys.argv[2], encoding="utf-8") as source:
    passed = {
        int(row["sequence"])
        for row in csv.DictReader(source)
        if row["status"] == "passed"
    }
print(len(planned - passed))
PY
)
if (( remaining > 0 )); then
  echo "rerun stopped with $remaining incomplete runs ($failures failed this attempt)" >&2
  exit 1
fi
printf 'completed_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$output_dir/COMPLETED"
echo "invalid-observation rerun complete: $output_dir"
