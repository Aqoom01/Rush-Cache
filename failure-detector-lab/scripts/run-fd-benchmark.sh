#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="$(cd "${LAB_DIR}/.." && pwd)"

algorithm="all"
nodes="8"
scenario="all"
seeds="1-30"
mode="native"
output="${LAB_DIR}/results/failure-detector"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --algorithm) algorithm="$2"; shift 2 ;;
    --nodes) nodes="$2"; shift 2 ;;
    --scenario) scenario="$2"; shift 2 ;;
    --seeds) seeds="$2"; shift 2 ;;
    --mode) mode="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 [--algorithm all] [--nodes 8] [--scenario all] [--seeds 1-30] [--mode native] [--output PATH]"
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ "${output}" != /* ]]; then
  output="${REPO_DIR}/${output}"
fi

cd "${REPO_DIR}"
exec ./gradlew -p failure-detector-lab fdBenchmark \
  --args="--algorithm ${algorithm} --nodes ${nodes} --scenario ${scenario} --seeds ${seeds} --mode ${mode} --output ${output}"
