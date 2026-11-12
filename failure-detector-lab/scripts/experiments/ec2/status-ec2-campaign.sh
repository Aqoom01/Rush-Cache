#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
infra_dir="${EC2_INFRA_DIR:-$script_dir/../../../infra/ec2}"
aws_bin="${AWS_BIN:-aws}"
terraform_bin="${TERRAFORM_BIN:-terraform}"
aws_profile="${AWS_PROFILE:-openkache}"
aws_region="${AWS_REGION:-ap-northeast-2}"
watch_seconds=0

usage() {
  cat <<'EOF'
Show the current EC2 failure-detector campaign status.

Usage:
  status-ec2-campaign.sh [--watch [SECONDS]]

Environment:
  AWS_PROFILE       AWS CLI profile (default: openkache)
  AWS_REGION        AWS region (default: ap-northeast-2)
  AWS_BIN           AWS CLI executable
  TERRAFORM_BIN     Terraform executable
  EC2_INFRA_DIR     Terraform stack directory
EOF
}

while (($# > 0)); do
  case "$1" in
    --watch)
      watch_seconds=30
      if [[ $# -ge 2 && "$2" =~ ^[1-9][0-9]*$ ]]; then
        watch_seconds=$2
        shift
      fi
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${AWS_BIN:-}" ]] && ! command -v aws >/dev/null 2>&1; then
  windows_aws_bin="/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe"
  if [[ -x "$windows_aws_bin" ]]; then
    aws_bin=$windows_aws_bin
  fi
fi
if [[ -z "${TERRAFORM_BIN:-}" ]] && ! command -v terraform >/dev/null 2>&1; then
  if [[ -x /tmp/terraform-bin/terraform ]]; then
    terraform_bin=/tmp/terraform-bin/terraform
  fi
fi
command -v "$aws_bin" >/dev/null 2>&1 || { echo "AWS CLI not found: $aws_bin" >&2; exit 1; }
command -v "$terraform_bin" >/dev/null 2>&1 || { echo "Terraform not found: $terraform_bin" >&2; exit 1; }

experiment_id=$("$terraform_bin" -chdir="$infra_dir" output -raw experiment_id)
bucket=$("$terraform_bin" -chdir="$infra_dir" output -raw results_bucket)
prefix="$experiment_id"
aws_args=(--profile "$aws_profile" --region "$aws_region")

render() {
  local tmp_dir status_file log_file plan_file now passed failed rows latest current marker eta
  local start_timestamp start_epoch now_epoch elapsed remaining estimate_seconds hours minutes
  tmp_dir=$(mktemp -d)
  status_file="$tmp_dir/execution-status.csv"
  log_file="$tmp_dir/campaign.log"
  plan_file="$tmp_dir/plan.tsv"

  "$aws_bin" s3 cp "${aws_args[@]}" \
    "s3://$bucket/$prefix/campaign/execution-status.csv" "$status_file" \
    --only-show-errors 2>/dev/null || true
  "$aws_bin" s3 cp "${aws_args[@]}" \
    "s3://$bucket/$prefix/campaign/campaign.log" "$log_file" \
    --only-show-errors 2>/dev/null || true
  "$aws_bin" s3 cp "${aws_args[@]}" \
    "s3://$bucket/$prefix/campaign/plan.tsv" "$plan_file" \
    --only-show-errors 2>/dev/null || true

  passed=0
  failed=0
  rows=0
  latest="none"
  if [[ -s "$status_file" ]]; then
    passed=$(awk -F, 'NR > 1 && $6 == "passed" {seen[$1]=1} END {print length(seen)+0}' "$status_file")
    failed=$(awk -F, 'NR > 1 && ($6 == "failed" || $6 == "infra-failed" || $6 == "semantic-failed") {count++} END {print count+0}' "$status_file")
    rows=$(awk 'END {print (NR > 0 ? NR-1 : 0)}' "$status_file")
    if ((rows > 0)); then
      latest=$(tail -n 1 "$status_file" | tr -d '\r')
    fi
  fi

  current="waiting for campaign plan"
  if [[ -s "$plan_file" ]] && ((passed < 550)); then
    current=$(awk -F'\t' -v wanted="$((passed + 1))" \
      '$1 == wanted {printf "sequence=%s seed=%s fault=%s profile=%s", $1, $2, $3, $4; exit}' \
      "$plan_file" | tr -d '\r')
    [[ -n "$current" ]] || current="campaign initialized"
  elif ((passed == 550)); then
    current="all planned sequences passed"
  fi

  marker=RUNNING
  if "$aws_bin" s3api head-object "${aws_args[@]}" --bucket "$bucket" \
      --key "$prefix/COMPLETED" >/dev/null 2>&1; then
    marker=COMPLETED
  elif "$aws_bin" s3api head-object "${aws_args[@]}" --bucket "$bucket" \
      --key "$prefix/FAILED" >/dev/null 2>&1; then
    marker=FAILED
  fi

  eta="initial estimate 13-16h; observed ETA after 5 passed runs"
  if ((passed >= 5)) && [[ -s "$log_file" ]]; then
    start_timestamp=$(grep 'campaign start experiment=' "$log_file" | tail -n 1 | cut -d' ' -f1)
    start_epoch=$(date -u -d "$start_timestamp" +%s 2>/dev/null || true)
    now_epoch=$(date -u +%s)
    if [[ "$start_epoch" =~ ^[0-9]+$ ]] && ((now_epoch > start_epoch)); then
      elapsed=$((now_epoch - start_epoch))
      remaining=$((550 - passed))
      estimate_seconds=$((elapsed * remaining / passed))
      hours=$((estimate_seconds / 3600))
      minutes=$(((estimate_seconds % 3600) / 60))
      eta="about ${hours}h ${minutes}m at the observed average"
    fi
  fi

  now=$(date '+%Y-%m-%d %H:%M:%S %Z')
  if ((watch_seconds > 0)) && [[ -t 1 ]]; then
    clear
  fi
  printf 'OpenKache EC2 campaign  %s\n' "$now"
  printf 'experiment: %s\n' "$experiment_id"
  printf 'status:     %s | unique passed: %s/550 | failed attempts: %s | rows: %s\n' \
    "$marker" "$passed" "$failed" "$rows"
  printf 'instances:  '
  "$aws_bin" ec2 describe-instances "${aws_args[@]}" \
    --filters "Name=tag:ExperimentId,Values=$experiment_id" \
    --query 'Reservations[].Instances[].State.Name' --output text 2>/dev/null \
    | tr '\t' ' ' | tr -d '\r'
  printf 'current:    %s\n' "$current"
  printf 'latest row: %s\n' "$latest"
  printf 'ETA:        %s\n' "$eta"
  if [[ -s "$log_file" ]]; then
    printf '\nlatest campaign log:\n'
    tail -n 6 "$log_file" | tr -d '\r'
  fi
  rm -rf -- "$tmp_dir"
}

while :; do
  render
  ((watch_seconds > 0)) || break
  sleep "$watch_seconds"
done
