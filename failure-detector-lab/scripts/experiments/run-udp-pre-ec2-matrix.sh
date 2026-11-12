#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 OUTPUT_DIR [REPETITIONS=30] [NODE_COUNT=8]" >&2
  echo "resume: FD_MATRIX_RESUME=1 $0 EXISTING_OUTPUT_DIR [REPETITIONS] [NODE_COUNT]" >&2
  exit 2
fi

output_dir=$1
repetitions=${2:-30}
node_count=${3:-8}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
lab_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
root_dir=$(CDPATH= cd -- "$lab_dir/.." && pwd)
single_runner="$script_dir/run-udp-netem.sh"
docker_bin=${DOCKER_BIN:-docker}
profiles_csv=${FD_MATRIX_PROFILES:-fixed-deadline,phi-gossip,swim-lifeguard,adaptive-deadline,ring-verify-h3-q3-r3}
faults_csv=${FD_MATRIX_FAULTS:-crash,loss15,loss45,delay,cpu-pause-target,cpu-pause-observer,cpu-throttle-target,cpu-throttle-observer,cpu-contention-target,io-contention-target}
warmup_seconds=${FD_WARMUP_SECONDS:-15}
fault_duration=${FD_FAULT_DURATION_SECONDS:-10}
recovery_seconds=${FD_RECOVERY_SECONDS:-15}
resume=${FD_MATRIX_RESUME:-0}
stop_requested=0

request_stop() {
  stop_requested=1
}

trap request_stop INT TERM

if (( repetitions < 1 )); then
  echo "repetitions must be positive" >&2
  exit 2
fi
case "$node_count" in
  5|8) ;;
  *) echo "pre-EC2 matrix node count must be 5 or 8" >&2; exit 2 ;;
esac

if [[ -e "$output_dir" ]]; then
  if [[ "$resume" != "1" ]]; then
    echo "output directory already exists: $output_dir" >&2
    echo "set FD_MATRIX_RESUME=1 to continue passed-checkpoint aware" >&2
    exit 1
  fi
  for required in matrix.properties plan.tsv execution-status.csv; do
    if [[ ! -f "$output_dir/$required" ]]; then
      echo "cannot resume: missing $output_dir/$required" >&2
      exit 1
    fi
  done
  for expected in \
      "profiles=$profiles_csv" \
      "faults=$faults_csv" \
      "repetitions=$repetitions" \
      "nodes=$node_count"; do
    if ! grep -Fqx "$expected" "$output_dir/matrix.properties"; then
      echo "cannot resume: configuration mismatch ($expected)" >&2
      exit 1
    fi
  done
else
  if [[ "$resume" == "1" ]]; then
    echo "cannot resume: output directory does not exist: $output_dir" >&2
    exit 1
  fi
  mkdir -p "$output_dir"
  printf 'profiles=%s\nfaults=%s\nrepetitions=%s\nnodes=%s\nparallelism=1\norder_seed=20261030\nwarmup_seconds=%s\nfault_duration_seconds=%s\nrecovery_seconds=%s\nsource=real-socket-observed\nenvironment=docker-single-host\n' \
    "$profiles_csv" "$faults_csv" "$repetitions" "$node_count" \
    "$warmup_seconds" "$fault_duration" "$recovery_seconds" \
    >"$output_dir/matrix.properties"

  python3 - "$profiles_csv" "$faults_csv" "$repetitions" "$output_dir/plan.tsv" <<'PY'
import itertools
import random
import sys

profiles = [item for item in sys.argv[1].split(",") if item]
faults = [item for item in sys.argv[2].split(",") if item]
repetitions = int(sys.argv[3])
rows = list(itertools.product(range(1, repetitions + 1), faults, profiles))
random.Random(20261030).shuffle(rows)
with open(sys.argv[4], "w", encoding="utf-8") as output:
    output.write("sequence\tseed\tfault\tprofile\n")
    for sequence, (seed, fault, profile) in enumerate(rows, 1):
        output.write(f"{sequence}\t{seed}\t{fault}\t{profile}\n")
PY
  printf 'sequence,seed,fault,profile,status,run_dir\n' >"$output_dir/execution-status.csv"
fi

"$root_dir/gradlew" -p "$lab_dir" jar
"$docker_bin" build \
  --file "$lab_dir/docker/Dockerfile.udp" \
  --tag openkache-fd-udp:local \
  "$lab_dir"

failures=0
while IFS=$'\t' read -r sequence seed fault profile <&3; do
  [[ "$sequence" == "sequence" ]] && continue
  if awk -F, -v wanted="$sequence" \
      '$1 == wanted && $5 == "passed" { found = 1 } END { exit found ? 0 : 1 }' \
      "$output_dir/execution-status.csv"; then
    echo "SKIP sequence=$sequence status=passed"
    continue
  fi
  safe_profile=${profile//[^a-zA-Z0-9_-]/_}
  base_run_dir=$(printf '%s/runs/%s/%s/seed-%03d' \
    "$output_dir" "$safe_profile" "$fault" "$seed")
  run_dir=$base_run_dir
  retry=0
  while [[ -e "$run_dir" || -e "$run_dir.runner.log" ]]; do
    retry=$((retry + 1))
    run_dir="$base_run_dir-retry-$retry"
  done
  mkdir -p "$(dirname -- "$run_dir")"
  echo "START sequence=$sequence profile=$profile fault=$fault seed=$seed"
  if FD_UDP_SKIP_BUILD=1 \
      FD_WARMUP_SECONDS="$warmup_seconds" \
      FD_FAULT_DURATION_SECONDS="$fault_duration" \
      FD_RECOVERY_SECONDS="$recovery_seconds" \
      DOCKER_BIN="$docker_bin" \
      "$single_runner" "$profile" "$fault" "$run_dir" "$node_count" "$seed" \
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
  if (( stop_requested )); then
    echo "matrix paused after sequence=$sequence: $output_dir" >&2
    exit 130
  fi
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
  echo "matrix stopped with $remaining incomplete runs ($failures failed this attempt): $output_dir" >&2
  exit 1
fi
planned_runs=$(awk 'NR > 1 { count += 1 } END { print count + 0 }' \
  "$output_dir/plan.tsv")
printf 'completed_at=%s\nruns=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$planned_runs" >"$output_dir/COMPLETED"
echo "matrix complete: $output_dir"
