#!/usr/bin/env bash
# Read-only AWS collection. Does not start, stop or delete any infrastructure.
set -euo pipefail
script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
lab_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
aws_bin="${AWS_BIN:-/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe}"
profile="${AWS_PROFILE:-openkache}"
region="${AWS_REGION:-ap-northeast-2}"
destination="${1:-$lab_dir/results-ec2-m6gd-final-20261107/raw}"
prefix=s3://openkache-fd-ec2-m6gd-20261106-761018879783-bbb5ba9e/openkache-fd-ec2-m6gd-20261106
mkdir -p "$destination"
"$aws_bin" s3 cp "$prefix/COMPLETED" "$destination/COMPLETED" \
  --profile "$profile" --region "$region" --only-show-errors
"$aws_bin" s3 cp "$prefix/" "$destination/" --recursive \
  --exclude '*' --include 'final-validation.json' --include 'campaign/*' \
  --include 'runs/*/node-0/result.json' --include 'runs/*/node-0/events.log' \
  --include 'runs/*/node-0/run.properties' --include 'runs/*/node-0/run.env' \
  --include 'runs/*/node-1/fault-events.jsonl' --include 'runs/*/node-1/fio-validation.json' \
  --include 'runs/*/node-1/fio-prepare.json' --include 'experiment.properties' \
  --profile "$profile" --region "$region" --only-show-errors
printf 'Final report data collected: %s\n' "$destination"
