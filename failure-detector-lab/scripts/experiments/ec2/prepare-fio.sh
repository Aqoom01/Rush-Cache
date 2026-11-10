#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 RUN_ID" >&2
  exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=ec2-common.sh
source "$script_dir/ec2-common.sh"

run_id=$1
load_manifest "$run_id"
if [[ "$FAULT" != "io-contention-target" || "$FD_NODE_ID" != "1" ]]; then
  echo "fio preparation must run on the I/O fault target" >&2
  exit 2
fi

run_dir="$FD_RUNS/$run_id"
fio_file="$FD_DATA_MOUNT/fio-work/$run_id.fio"
evidence="$run_dir/fio-prepare.json"
rm -f "$fio_file"

started_nanos=$(date +%s%N)
fio --name=openkache-fd-prepare --filename="$fio_file" \
  --size=1G --create_only=1 --fallocate=native --eta=never \
  --output=/dev/null
finished_nanos=$(date +%s%N)
duration_ms=$(((finished_nanos - started_nanos) / 1000000))

jq -n \
  --arg runId "$run_id" \
  --arg fioPath "$fio_file" \
  --argjson durationMs "$duration_ms" \
  --arg preparedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{runId:$runId,fioPath:$fioPath,durationMs:$durationMs,preparedAt:$preparedAt}' \
  >"$evidence"
aws s3 cp --region "$AWS_REGION" "$evidence" \
  "$FD_S3_PREFIX/runs/$run_id/node-$FD_NODE_ID/fio-prepare.json" >/dev/null

echo "fio file prepared run=$run_id duration_ms=$duration_ms"
