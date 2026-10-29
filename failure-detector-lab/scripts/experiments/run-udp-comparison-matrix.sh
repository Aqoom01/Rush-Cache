#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 OUTPUT_DIR [REPETITIONS=30] [PARALLELISM=2] [NODE_COUNT=5]" >&2
  exit 2
fi

output_dir=$1
repetitions=${2:-30}
parallelism=${3:-2}
node_count=${4:-5}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
lab_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
root_dir=$(CDPATH= cd -- "$lab_dir/.." && pwd)
single_runner="$script_dir/run-udp-netem.sh"
docker_bin=${DOCKER_BIN:-docker}

if [[ -e "$output_dir" ]]; then
  echo "output directory already exists: $output_dir" >&2
  exit 1
fi
if (( repetitions < 1 || parallelism < 1 )); then
  echo "repetitions and parallelism must be positive" >&2
  exit 2
fi
case "$node_count" in
  5|8) ;;
  *) echo "comparison node count must be 5 or 8" >&2; exit 2 ;;
esac

mkdir -p "$output_dir"
printf 'methods=ring-verify-h3-q3-r3,swim-lifeguard\nfaults=crash,loss15,loss45\nrepetitions=%s\nparallelism=%s\nnodes=%s\n' \
  "$repetitions" "$parallelism" "$node_count" >"$output_dir/matrix.properties"

"$root_dir/gradlew" -p "$lab_dir" jar
"$docker_bin" build \
  --file "$lab_dir/docker/Dockerfile.udp" \
  --tag openkache-fd-udp:local \
  "$lab_dir"

run_one() {
  local method=$1
  local fault=$2
  local seed=$3
  local run_dir
  run_dir=$(printf '%s/%s/%s/seed-%03d' "$output_dir" "$method" "$fault" "$seed")
  mkdir -p "$(dirname -- "$run_dir")"
  echo "START method=$method fault=$fault seed=$seed"
  FD_UDP_SKIP_BUILD=1 DOCKER_BIN="$docker_bin" \
    "$single_runner" "$method" "$fault" "$run_dir" "$node_count" "$seed" \
    >"$run_dir.runner.log" 2>&1
  echo "DONE method=$method fault=$fault seed=$seed"
}

wait_for_slot() {
  while (( $(jobs -pr | wc -l) >= parallelism )); do
    wait -n
  done
}

for fault in crash loss15 loss45; do
  for seed in $(seq 1 "$repetitions"); do
    for method in ring-verify-h3-q3-r3 swim-lifeguard; do
      wait_for_slot
      run_one "$method" "$fault" "$seed" &
    done
  done
done
wait

echo "matrix complete: $output_dir"
