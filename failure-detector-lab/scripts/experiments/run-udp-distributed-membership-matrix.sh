#!/usr/bin/env sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
  echo "usage: $0 OUTPUT_DIR [CONTROL_FAULT=none] [REPETITIONS=10]" >&2
  exit 2
fi

output_dir=$1
control_fault=none
repetitions=10
if [ "$#" -ge 2 ]; then
  case "$2" in
    ''|*[!0-9]*) control_fault=$2; repetitions=${3:-10} ;;
    *) repetitions=$2 ;;
  esac
fi
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
single_runner="$script_dir/run-udp-distributed-membership.sh"

case "$repetitions" in
  ''|*[!0-9]*|0) echo "repetitions must be a positive integer" >&2; exit 2 ;;
esac
if [ -e "$output_dir" ]; then
  echo "output directory already exists: $output_dir" >&2
  exit 1
fi
mkdir -p "$output_dir"
printf '%s\n' \
  "repetitions=$repetitions" \
  "nodes=8" \
  "crash_targets=1,2,3" \
  "fault_injection=udp-process-halt" \
  "control_fault=$control_fault" \
  "source=real-udp-observed" >"$output_dir/matrix.properties"

repetition=1
while [ "$repetition" -le "$repetitions" ]; do
  padded=$(printf '%03d' "$repetition")
  run_dir="$output_dir/run-$padded"
  log="$output_dir/run-$padded.runner.log"
  echo "[$repetition/$repetitions] $run_dir"
  if [ "$repetition" -eq 1 ]; then
    "$single_runner" "$run_dir" "$control_fault" "$repetition" >"$log" 2>&1
  else
    FD_UDP_SKIP_BUILD=1 "$single_runner" \
      "$run_dir" "$control_fault" "$repetition" >"$log" 2>&1
  fi
  repetition=$((repetition + 1))
done

echo "UDP distributed membership matrix: $output_dir"
