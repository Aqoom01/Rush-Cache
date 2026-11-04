#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=ec2-common.sh
source "$script_dir/ec2-common.sh"

exec 9>"$FD_CAMPAIGN/campaign.lock"
if ! flock -n 9; then
  echo "another EC2 campaign process already owns the local lock" >&2
  exit 0
fi

exec > >(tee -a "$FD_CAMPAIGN/campaign.log") 2>&1

profiles=(
  product-swim-lifeguard
  product-redis-cluster
  product-phi-gossip
  product-ignite-tcp-ring
  product-aerospike-mesh
)
faults=(
  normal
  crash
  loss45
  delay
  cpu-pause-target
  cpu-pause-observer
  loss15
  cpu-throttle-target
  cpu-throttle-observer
  cpu-contention-target
  io-contention-target
)
warmup_seconds=15
fault_duration_seconds=10
recovery_seconds=15
expected_runs=$((FD_REPETITIONS * ${#profiles[@]} * ${#faults[@]}))
plan="$FD_CAMPAIGN/plan.tsv"
status="$FD_CAMPAIGN/execution-status.csv"
nodes="$FD_CAMPAIGN/nodes.tsv"

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

sync_campaign_state() {
  aws s3 cp --region "$AWS_REGION" "$plan" "$FD_S3_PREFIX/campaign/plan.tsv" >/dev/null
  aws s3 cp --region "$AWS_REGION" "$status" "$FD_S3_PREFIX/campaign/execution-status.csv" >/dev/null
  aws s3 cp --region "$AWS_REGION" "$FD_CAMPAIGN/campaign.log" \
    "$FD_S3_PREFIX/campaign/campaign.log" >/dev/null || true
}

if aws s3api head-object --region "$AWS_REGION" --bucket "$FD_BUCKET" \
    --key "$FD_EXPERIMENT_ID/COMPLETED" >/dev/null 2>&1; then
  log "campaign already completed"
  exit 0
fi

if [[ ! -f "$plan" ]]; then
  if ! aws s3 cp --region "$AWS_REGION" "$FD_S3_PREFIX/campaign/plan.tsv" "$plan" \
      >/dev/null 2>&1; then
    profiles_csv=$(IFS=,; echo "${profiles[*]}")
    faults_csv=$(IFS=,; echo "${faults[*]}")
    python3 - "$profiles_csv" "$faults_csv" "$FD_REPETITIONS" "$plan" <<'PY'
import itertools
import random
import sys

profiles = sys.argv[1].split(",")
faults = sys.argv[2].split(",")
repetitions = int(sys.argv[3])
rows = list(itertools.product(range(1, repetitions + 1), faults, profiles))
random.Random(20261030).shuffle(rows)
with open(sys.argv[4], "w", encoding="utf-8") as output:
    output.write("sequence\tseed\tfault\tprofile\n")
    for sequence, (seed, fault, profile) in enumerate(rows, 1):
        output.write(f"{sequence}\t{seed}\t{fault}\t{profile}\n")
PY
  fi
fi

if [[ ! -f "$status" ]]; then
  if ! aws s3 cp --region "$AWS_REGION" "$FD_S3_PREFIX/campaign/execution-status.csv" "$status" \
      >/dev/null 2>&1; then
    printf 'sequence,seed,fault,profile,attempt,status,run_id\n' >"$status"
  fi
fi

wait_for_bootstrap() {
  local count
  for _ in $(seq 1 120); do
    count=$(aws s3 ls --region "$AWS_REGION" "$FD_S3_PREFIX/ready/" 2>/dev/null | wc -l || true)
    count=${count:-0}
    if [[ "$count" -eq "$FD_EXPECTED_NODES" ]]; then
      return 0
    fi
    log "waiting for node bootstrap ready=$count/$FD_EXPECTED_NODES"
    sleep 10
  done
  return 1
}

discover_nodes() {
  aws ec2 describe-instances --region "$AWS_REGION" \
    --filters \
      "Name=tag:ExperimentId,Values=$FD_EXPERIMENT_ID" \
      'Name=instance-state-name,Values=running' \
    --query 'Reservations[].Instances[].[Tags[?Key==`NodeId`]|[0].Value,InstanceId,PrivateIpAddress]' \
    --output text | sort -n >"$nodes"
  [[ "$(wc -l <"$nodes")" -eq "$FD_EXPECTED_NODES" ]]
  for node_id in $(seq 0 7); do
    awk -v wanted="$node_id" '$1 == wanted { found=1 } END { exit found ? 0 : 1 }' "$nodes"
  done
}

wait_for_bootstrap
for _ in $(seq 1 30); do
  if discover_nodes; then
    break
  fi
  sleep 5
done
discover_nodes || { log "failed to discover all eight running nodes"; exit 1; }

mapfile -t instance_ids < <(awk '{print $2}' "$nodes")
node0_instance=$(awk '$1 == 0 {print $2}' "$nodes")
node1_instance=$(awk '$1 == 1 {print $2}' "$nodes")
node0_ip=$(awk '$1 == 0 {print $3}' "$nodes")
peers=$(awk 'BEGIN { first=1 } { if (!first) printf ","; printf "%s=%s:19000", $1, $3; first=0 } END { print "" }' "$nodes")

last_command_id=
send_command() {
  local command=$1
  shift
  local -a targets=("$@")
  local parameters
  parameters=$(jq -cn --arg command "$command" '{commands:[$command],executionTimeout:["600"]}')
  for _ in $(seq 1 30); do
    if last_command_id=$(aws ssm send-command --region "$AWS_REGION" \
        --document-name AWS-RunShellScript \
        --instance-ids "${targets[@]}" \
        --parameters "$parameters" \
        --query 'Command.CommandId' --output text 2>/dev/null); then
      return 0
    fi
    sleep 10
  done
  return 1
}

wait_command() {
  local command_id=$1 expected=$2
  local statuses status_count done_count
  for _ in $(seq 1 300); do
    statuses=$(aws ssm list-command-invocations --region "$AWS_REGION" \
      --command-id "$command_id" --details \
      --query 'CommandInvocations[].Status' --output text 2>/dev/null || true)
    status_count=$(wc -w <<<"$statuses")
    if grep -Eq '(Failed|Timed ?Out|Cancelled|Cancelling|Undeliverable|Terminated)' <<<"$statuses"; then
      log "SSM command failed id=$command_id statuses=$statuses"
      return 1
    fi
    done_count=$(tr '[:space:]' '\n' <<<"$statuses" | grep -c '^Success$' || true)
    if [[ "$status_count" -eq "$expected" && "$done_count" -eq "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  log "SSM command timed out id=$command_id statuses=$statuses"
  return 1
}

wait_run_ready() {
  local run_id=$1 count
  for _ in $(seq 1 150); do
    count=$(aws s3 ls --region "$AWS_REGION" "$FD_S3_PREFIX/runs/$run_id/ready/" \
      2>/dev/null | wc -l || true)
    count=${count:-0}
    [[ "$count" -eq "$FD_EXPECTED_NODES" ]] && return 0
    sleep 2
  done
  log "run readiness timed out run=$run_id"
  return 1
}

manifest_value() {
  printf '%s=%q\n' "$1" "$2"
}

write_manifest() {
  local run_id=$1 sequence=$2 seed=$3 fault=$4 profile=$5
  local profile_value product_algorithm main_class observer_duration peer_duration
  case "$profile" in
    product-swim-lifeguard)
      profile_value=swim-lifeguard
      product_algorithm=swim-lifeguard
      main_class=com.openkache.failuredetector.transport.UdpExperimentNodeMain
      ;;
    product-redis-cluster)
      profile_value=redis-cluster
      product_algorithm=redis-cluster
      main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
      ;;
    product-phi-gossip)
      profile_value=phi-gossip
      product_algorithm=phi-gossip
      main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
      ;;
    product-ignite-tcp-ring)
      profile_value=ignite-tcp-ring
      product_algorithm=ignite-tcp-ring
      main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
      ;;
    product-aerospike-mesh)
      profile_value=aerospike-mesh
      product_algorithm=aerospike-mesh
      main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
      ;;
    *) return 2 ;;
  esac
  case "$fault" in
    normal|crash|loss15|loss45|delay)
      observer_duration=60
      peer_duration=65
      ;;
    *)
      observer_duration=$((15 + warmup_seconds + fault_duration_seconds + recovery_seconds))
      peer_duration=$((observer_duration + 5))
      ;;
  esac

  local manifest="$FD_CAMPAIGN/$run_id.env"
  {
    manifest_value RUN_ID "$run_id"
    manifest_value SEQUENCE "$sequence"
    manifest_value SEED "$seed"
    manifest_value FAULT "$fault"
    manifest_value REQUESTED_PROFILE "$profile"
    manifest_value PROFILE_VALUE "$profile_value"
    manifest_value PRODUCT_ALGORITHM "$product_algorithm"
    manifest_value MAIN_CLASS "$main_class"
    manifest_value PEERS "$peers"
    manifest_value NODE0_IP "$node0_ip"
    manifest_value OBSERVER_DURATION_SECONDS "$observer_duration"
    manifest_value PEER_DURATION_SECONDS "$peer_duration"
    manifest_value WARMUP_SECONDS "$warmup_seconds"
    manifest_value FAULT_DURATION_SECONDS "$fault_duration_seconds"
    manifest_value RECOVERY_SECONDS "$recovery_seconds"
    manifest_value THROTTLE_PERCENT "10"
    manifest_value CPU_WORKERS "4"
  } >"$manifest"
  aws s3 cp --region "$AWS_REGION" "$manifest" \
    "$FD_S3_PREFIX/manifests/$run_id.env" >/dev/null
}

run_one_attempt() {
  local run_id=$1 sequence=$2 seed=$3 fault=$4 profile=$5
  local command_id target_instance run_started peer_duration end_at now
  write_manifest "$run_id" "$sequence" "$seed" "$fault" "$profile" || return 1
  # shellcheck disable=SC1090
  source "$FD_CAMPAIGN/$run_id.env"
  peer_duration=$PEER_DURATION_SECONDS

  send_command "/opt/openkache-fd/bin/run-node.sh '$run_id'" "${instance_ids[@]}" || return 1
  command_id=$last_command_id
  wait_command "$command_id" "$FD_EXPECTED_NODES" || return 1
  wait_run_ready "$run_id" || return 1
  run_started=$(date +%s)

  if [[ "$fault" != "normal" ]]; then
    sleep "$warmup_seconds"
    if [[ "$fault" == *-observer ]]; then
      target_instance=$node0_instance
    else
      target_instance=$node1_instance
    fi
    send_command "/opt/openkache-fd/bin/apply-fault.sh '$run_id'" "$target_instance" || return 1
    command_id=$last_command_id
    wait_command "$command_id" 1 || return 1
  fi

  end_at=$((run_started + peer_duration + 10))
  while (( (now=$(date +%s)) < end_at )); do
    sleep "$(( end_at - now > 10 ? 10 : end_at - now ))"
  done

  send_command "/opt/openkache-fd/bin/collect-run.sh '$run_id'" "${instance_ids[@]}" || return 1
  command_id=$last_command_id
  wait_command "$command_id" "$FD_EXPECTED_NODES" || return 1

  local result="$FD_CAMPAIGN/$run_id-result.json"
  aws s3 cp --region "$AWS_REGION" \
    "$FD_S3_PREFIX/runs/$run_id/node-0/result.json" "$result" >/dev/null || return 1
  python3 -m json.tool "$result" >/dev/null || return 1
}

log "campaign start experiment=$FD_EXPERIMENT_ID expected_runs=$expected_runs repetitions=$FD_REPETITIONS"
sync_campaign_state

while IFS=$'\t' read -r sequence seed fault profile <&8; do
  [[ "$sequence" == "sequence" ]] && continue
  if awk -F, -v wanted="$sequence" \
      '$1 == wanted && $6 == "passed" { found=1 } END { exit found ? 0 : 1 }' "$status"; then
    log "skip sequence=$sequence status=passed"
    continue
  fi

  prior_failures=$(awk -F, -v wanted="$sequence" '$1 == wanted && $6 == "failed" { count++ } END { print count+0 }' "$status")
  if (( prior_failures >= 3 )); then
    log "campaign blocked sequence=$sequence after $prior_failures failed attempts"
    printf 'blocked_at=%s\nsequence=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$sequence" \
      >"$FD_CAMPAIGN/FAILED"
    aws s3 cp --region "$AWS_REGION" "$FD_CAMPAIGN/FAILED" "$FD_S3_PREFIX/FAILED" >/dev/null
    exit 1
  fi

  passed=0
  for attempt in $(seq $((prior_failures + 1)) 3); do
    run_id=$(printf 'sequence-%04d-attempt-%d' "$sequence" "$attempt")
    log "start sequence=$sequence attempt=$attempt profile=$profile fault=$fault seed=$seed run=$run_id"
    if run_one_attempt "$run_id" "$sequence" "$seed" "$fault" "$profile"; then
      run_status=passed
      passed=1
    else
      run_status=failed
    fi
    printf '%s,%s,%s,%s,%s,%s,%s\n' \
      "$sequence" "$seed" "$fault" "$profile" "$attempt" "$run_status" "$run_id" >>"$status"
    sync_campaign_state
    log "done sequence=$sequence attempt=$attempt status=$run_status"
    [[ "$passed" == "1" ]] && break
  done

  if [[ "$passed" != "1" ]]; then
    printf 'blocked_at=%s\nsequence=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$sequence" \
      >"$FD_CAMPAIGN/FAILED"
    aws s3 cp --region "$AWS_REGION" "$FD_CAMPAIGN/FAILED" "$FD_S3_PREFIX/FAILED" >/dev/null
    exit 1
  fi
done 8<"$plan"

python3 "$script_dir/validate-ec2-campaign.py" \
  --plan "$plan" \
  --status "$status" \
  --expected "$expected_runs" \
  --output "$FD_CAMPAIGN/final-validation.json"

cat >"$FD_CAMPAIGN/COMPLETED" <<EOF
completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
runs=$expected_runs
environment=ec2-multi-host
EOF
sync_campaign_state
aws s3 cp --region "$AWS_REGION" "$FD_CAMPAIGN/final-validation.json" \
  "$FD_S3_PREFIX/final-validation.json" >/dev/null
aws s3 cp --region "$AWS_REGION" "$FD_CAMPAIGN/COMPLETED" \
  "$FD_S3_PREFIX/COMPLETED" >/dev/null
log "campaign complete runs=$expected_runs"

if [[ "$FD_AUTO_STOP" == "1" ]]; then
  log "stopping all experiment instances after successful upload"
  aws ec2 stop-instances --region "$AWS_REGION" --instance-ids "${instance_ids[@]}" >/dev/null
fi
