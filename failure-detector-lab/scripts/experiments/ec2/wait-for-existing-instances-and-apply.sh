#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
infra_dir="${EC2_INFRA_DIR:-${script_dir}/../../../infra/ec2}"
aws_bin="${AWS_BIN:-aws}"
terraform_bin="${TERRAFORM_BIN:-terraform}"
aws_region="${AWS_REGION:-${TF_VAR_aws_region:-ap-northeast-2}}"
aws_profile="${AWS_PROFILE:-}"
blocking_instance_ids="${BLOCKING_INSTANCE_IDS:-}"
blocking_tag_key="${BLOCKING_TAG_KEY:-}"
blocking_tag_value="${BLOCKING_TAG_VALUE:-}"
expected_count="${BLOCKING_EXPECTED_COUNT:-}"
poll_seconds="${WAIT_POLL_SECONDS:-60}"
timeout_seconds="${WAIT_TIMEOUT_SECONDS:-0}"
plan_file="${TERRAFORM_PLAN_FILE:-openkache-fd.tfplan}"
apply_after_wait=0
allow_missing_at_start=0
allow_coexist=0

usage() {
  cat <<'EOF'
Wait for explicitly selected EC2 instances to terminate, then create a fresh Terraform plan.

Usage:
  wait-for-existing-instances-and-apply.sh [options]

Selectors (choose exactly one):
  --instance-ids IDS       Comma/space-separated instance IDs
  --tag KEY=VALUE          Resolve the currently active instances with this exact tag
  --allow-coexist          Skip termination waiting and enforce a regional vCPU capacity gate

Options:
  --expected-count N       Abort if the selector initially resolves to a different count
  --region REGION          AWS region (default: AWS_REGION/TF_VAR_aws_region/ap-northeast-2)
  --profile PROFILE        AWS CLI profile (default: AWS_PROFILE or CLI default)
  --poll-seconds N         Seconds between checks (default: 60)
  --timeout-seconds N      Stop waiting after N seconds; 0 means no timeout (default: 0)
  --plan-file PATH         Saved plan path relative to infra/ec2 (default: openkache-fd.tfplan)
  --allow-missing-at-start Allow an ID that AWS cannot find during the first check
  --apply                  Apply the newly saved plan after the gate passes
  -h, --help               Show this help

Equivalent environment variables:
  BLOCKING_INSTANCE_IDS
  BLOCKING_TAG_KEY / BLOCKING_TAG_VALUE
  BLOCKING_EXPECTED_COUNT
  WAIT_POLL_SECONDS / WAIT_TIMEOUT_SECONDS

The script never stops or terminates existing instances. A stopped instance remains blocking;
only terminated or no-longer-found instances pass the gate.
EOF
}

while (($# > 0)); do
  case "$1" in
    --instance-ids)
      [[ $# -ge 2 ]] || { echo "--instance-ids requires a value" >&2; exit 2; }
      blocking_instance_ids="$2"
      shift 2
      ;;
    --tag)
      [[ $# -ge 2 && "$2" == *=* ]] || { echo "--tag requires KEY=VALUE" >&2; exit 2; }
      blocking_tag_key="${2%%=*}"
      blocking_tag_value="${2#*=}"
      shift 2
      ;;
    --expected-count)
      [[ $# -ge 2 ]] || { echo "--expected-count requires a value" >&2; exit 2; }
      expected_count="$2"
      shift 2
      ;;
    --region)
      [[ $# -ge 2 ]] || { echo "--region requires a value" >&2; exit 2; }
      aws_region="$2"
      shift 2
      ;;
    --profile)
      [[ $# -ge 2 ]] || { echo "--profile requires a value" >&2; exit 2; }
      aws_profile="$2"
      shift 2
      ;;
    --poll-seconds)
      [[ $# -ge 2 ]] || { echo "--poll-seconds requires a value" >&2; exit 2; }
      poll_seconds="$2"
      shift 2
      ;;
    --timeout-seconds)
      [[ $# -ge 2 ]] || { echo "--timeout-seconds requires a value" >&2; exit 2; }
      timeout_seconds="$2"
      shift 2
      ;;
    --plan-file)
      [[ $# -ge 2 ]] || { echo "--plan-file requires a value" >&2; exit 2; }
      plan_file="$2"
      shift 2
      ;;
    --allow-missing-at-start)
      allow_missing_at_start=1
      shift
      ;;
    --allow-coexist)
      allow_coexist=1
      shift
      ;;
    --apply)
      apply_after_wait=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ((allow_coexist == 1)) && [[ -n "$blocking_instance_ids" || -n "$blocking_tag_key" || -n "$blocking_tag_value" ]]; then
  echo "--allow-coexist cannot be combined with an instance-ID or tag termination gate." >&2
  exit 2
fi
if [[ -n "$blocking_instance_ids" && ( -n "$blocking_tag_key" || -n "$blocking_tag_value" ) ]]; then
  echo "Choose either instance IDs or a tag, not both." >&2
  exit 2
fi
if ((allow_coexist == 0)) && [[ -z "$blocking_instance_ids" && ( -z "$blocking_tag_key" || -z "$blocking_tag_value" ) ]]; then
  echo "Provide --instance-ids or --tag (or the equivalent environment variables)." >&2
  exit 2
fi
if [[ ! "$poll_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "poll seconds must be a positive integer" >&2
  exit 2
fi
if [[ ! "$timeout_seconds" =~ ^[0-9]+$ ]]; then
  echo "timeout seconds must be zero or a positive integer" >&2
  exit 2
fi
if [[ -n "$expected_count" && ! "$expected_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "expected count must be a positive integer" >&2
  exit 2
fi
if [[ "$plan_file" == /* || "$plan_file" == *".."* ]]; then
  echo "plan file must be a path inside the Terraform directory" >&2
  exit 2
fi
if [[ -z "${AWS_BIN+x}" ]] && ! command -v aws >/dev/null 2>&1; then
  windows_aws_bin="/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe"
  if [[ -x "$windows_aws_bin" ]]; then
    aws_bin="$windows_aws_bin"
  fi
fi
if ! command -v "$aws_bin" >/dev/null 2>&1; then
  echo "AWS CLI not found: $aws_bin" >&2
  exit 1
fi
if ! command -v "$terraform_bin" >/dev/null 2>&1; then
  echo "Terraform not found: $terraform_bin" >&2
  exit 1
fi
if [[ ! -d "$infra_dir" ]]; then
  echo "Terraform directory not found: $infra_dir" >&2
  exit 1
fi

aws_args=(--region "$aws_region")
if [[ -n "$aws_profile" ]]; then
  aws_args+=(--profile "$aws_profile")
fi

identity_arn="$("$aws_bin" "${aws_args[@]}" sts get-caller-identity --query Arn --output text)"
echo "AWS identity: $identity_arn"
echo "AWS region:   $aws_region"

declare -a selected_ids=()
if ((allow_coexist == 1)); then
  candidate_ids=()
  echo "Coexistence mode: existing instances will not be stopped or modified."
elif [[ -n "$blocking_instance_ids" ]]; then
  normalized_ids="${blocking_instance_ids//,/ }"
  read -r -a candidate_ids <<<"$normalized_ids"
else
  echo "Resolving active instances tagged ${blocking_tag_key}=${blocking_tag_value} ..."
  tag_output="$("$aws_bin" "${aws_args[@]}" ec2 describe-instances \
    --filters \
      "Name=tag:${blocking_tag_key},Values=${blocking_tag_value}" \
      "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down" \
    --query 'Reservations[].Instances[].InstanceId' \
    --output text)"
  candidate_ids=()
  if [[ -n "$tag_output" && "$tag_output" != "None" ]]; then
    read -r -a candidate_ids <<<"$tag_output"
    # These IDs were just returned by EC2, so a first-poll disappearance is not an input typo.
    allow_missing_at_start=1
  fi
fi

declare -A seen_ids=()
for instance_id in "${candidate_ids[@]}"; do
  if [[ ! "$instance_id" =~ ^i-[0-9a-fA-F]{8,17}$ ]]; then
    echo "Invalid EC2 instance ID: $instance_id" >&2
    exit 2
  fi
  if [[ -z "${seen_ids[$instance_id]+present}" ]]; then
    selected_ids+=("$instance_id")
    seen_ids["$instance_id"]=1
  fi
done

if ((allow_coexist == 0)) && [[ -n "$expected_count" && ${#selected_ids[@]} -ne $expected_count ]]; then
  echo "Selector resolved ${#selected_ids[@]} instance(s), expected $expected_count; refusing to continue." >&2
  exit 1
fi

if ((allow_coexist == 1)); then
  echo "Termination gate skipped; a fresh-plan regional vCPU capacity gate will run before apply."
elif ((${#selected_ids[@]} == 0)); then
  echo "No active instances matched the selector; the gate is already clear."
else
  echo "Termination gate (${#selected_ids[@]} instance(s)): ${selected_ids[*]}"
fi

instance_state() {
  local instance_id="$1"
  local output rc

  set +e
  output="$("$aws_bin" "${aws_args[@]}" ec2 describe-instances \
    --instance-ids "$instance_id" \
    --query 'Reservations[0].Instances[0].State.Name' \
    --output text 2>&1)"
  rc=$?
  set -e

  if ((rc == 0)); then
    if [[ "$output" == "terminated" ]]; then
      printf '%s\n' "cleared:terminated"
    elif [[ -z "$output" || "$output" == "None" ]]; then
      printf '%s\n' "cleared:not-found"
    else
      printf '%s\n' "$output"
    fi
    return 0
  fi

  if [[ "$output" == *"InvalidInstanceID.NotFound"* ]]; then
    printf '%s\n' "cleared:not-found"
    return 0
  fi

  echo "Failed to read $instance_id: $output" >&2
  return "$rc"
}

export_terraform_aws_credentials() {
  local -a export_args=(--region "$aws_region" configure export-credentials)
  local credential_exports line assignment key value
  local found_access_key=0
  local found_secret_key=0

  if [[ -n "$aws_profile" ]]; then
    export_args+=(--profile "$aws_profile")
  fi
  export_args+=(--format env)

  credential_exports="$("$aws_bin" "${export_args[@]}")"
  while IFS= read -r line; do
    line="${line%$'\r'}"
    [[ -z "$line" ]] && continue
    if [[ "$line" != "export "* || "$line" != *=* ]]; then
      echo "Unexpected line from AWS credential export; refusing to source it." >&2
      return 1
    fi

    assignment="${line#export }"
    key="${assignment%%=*}"
    value="${assignment#*=}"
    case "$key" in
      AWS_ACCESS_KEY_ID)
        found_access_key=1
        ;;
      AWS_SECRET_ACCESS_KEY)
        found_secret_key=1
        ;;
      AWS_SESSION_TOKEN|AWS_CREDENTIAL_EXPIRATION)
        ;;
      *)
        echo "Unexpected key from AWS credential export: $key" >&2
        return 1
        ;;
    esac
    export "$key=$value"
  done <<<"$credential_exports"
  unset credential_exports line assignment key value

  if ((found_access_key == 0 || found_secret_key == 0)); then
    echo "AWS credential export did not contain the required keys." >&2
    return 1
  fi

  # Terraform now uses the freshly exported environment credentials. Avoid resolving a WSL-local
  # profile with the same name as the Windows AWS CLI profile.
  unset AWS_PROFILE AWS_DEFAULT_PROFILE
  export AWS_REGION="$aws_region"
  export AWS_DEFAULT_REGION="$aws_region"
}

wait_started_at="$(date +%s)"
first_check=1
while :; do
  remaining=0
  states=()
  for instance_id in "${selected_ids[@]}"; do
    state="$(instance_state "$instance_id")"
    states+=("${instance_id}=${state}")
    if ((first_check == 1 && allow_missing_at_start == 0)) && [[ "$state" == "cleared:not-found" ]]; then
      echo "AWS could not find $instance_id during the first check; refusing to treat a possible typo as terminated." >&2
      echo "If the ID was already terminated and aged out of EC2, rerun with --allow-missing-at-start." >&2
      exit 1
    fi
    if [[ "$state" != cleared:* ]]; then
      ((remaining += 1))
    fi
  done

  now_epoch="$(date +%s)"
  elapsed_seconds=$((now_epoch - wait_started_at))
  if ((${#states[@]} > 0)); then
    printf '[%s] remaining=%d/%d elapsed=%ds | %s\n' \
      "$(date --iso-8601=seconds)" "$remaining" "${#selected_ids[@]}" "$elapsed_seconds" "${states[*]}"
  fi

  if ((remaining == 0)); then
    break
  fi
  first_check=0
  if ((timeout_seconds > 0 && elapsed_seconds >= timeout_seconds)); then
    echo "Timed out before every selected instance terminated. Terraform was not run." >&2
    exit 124
  fi
  sleep "$poll_seconds"
done

if ((allow_coexist == 1)); then
  echo "Coexistence selector phase passed. Creating a fresh Terraform plan."
else
  echo "Termination gate passed. Creating a fresh Terraform plan."
fi
export_terraform_aws_credentials
echo "Exported fresh AWS credentials to the Terraform process without writing them to disk."
cd "$infra_dir"
"$terraform_bin" init -input=false
"$terraform_bin" fmt -check -recursive
"$terraform_bin" validate
"$terraform_bin" plan -input=false -out="$plan_file"

if ((allow_coexist == 1)); then
  capacity_output="${plan_file}.capacity.json"
  capacity_args=(
    --plan "$plan_file"
    --terraform-bin "$terraform_bin"
    --aws-bin "$aws_bin"
    --region "$aws_region"
    --output "$capacity_output"
    --minimum-remaining-vcpus "${COEXIST_MIN_REMAINING_VCPUS:-1}"
  )
  if [[ -n "$aws_profile" ]]; then
    capacity_args+=(--profile "$aws_profile")
  fi
  python3 "$script_dir/validate-coexist-capacity.py" "${capacity_args[@]}"
  echo "Coexistence capacity evidence saved to ${infra_dir}/${capacity_output}."
fi

if ((apply_after_wait == 0)); then
  echo "Plan saved to ${infra_dir}/${plan_file}; no resources were created."
  echo "Review it, then run: terraform -chdir=${infra_dir} apply ${plan_file}"
  exit 0
fi

echo "Applying the freshly generated plan because --apply was explicitly supplied."
"$terraform_bin" apply -input=false "$plan_file"
