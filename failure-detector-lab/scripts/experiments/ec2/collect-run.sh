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
run_dir="$FD_RUNS/$run_id"

for _ in $(seq 1 150); do
  systemctl is-active --quiet "$FD_UNIT" || break
  sleep 0.2
done

systemctl show "$FD_UNIT" >"$run_dir/systemd-show.txt" 2>&1 || true
systemctl status "$FD_UNIT" --no-pager >"$run_dir/systemd-status.txt" 2>&1 || true

cgroup=/sys/fs/cgroup/system.slice/openkache-fd-node.service
if [[ -d "$cgroup" ]]; then
  for name in cpu.stat memory.current memory.events io.stat; do
    [[ -r "$cgroup/$name" ]] && cp "$cgroup/$name" "$run_dir/cgroup-$name.txt"
  done
fi

iface=$(ip route get "$NODE0_IP" | awk '/dev/ {for (i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
[[ -n "$iface" ]] && tc qdisc del dev "$iface" root >/dev/null 2>&1 || true
systemctl set-property --runtime "$FD_UNIT" CPUQuota=100% >/dev/null 2>&1 || true
pid=$(systemctl show "$FD_UNIT" --property MainPID --value 2>/dev/null || true)
[[ -n "$pid" && "$pid" != "0" ]] && kill -CONT "$pid" >/dev/null 2>&1 || true

cat >"$run_dir/run.properties" <<EOF
run_id=$run_id
sequence=$SEQUENCE
seed=$SEED
profile=$REQUESTED_PROFILE
fault=$FAULT
nodes=$FD_EXPECTED_NODES
node_id=$FD_NODE_ID
environment=ec2-multi-host
instance_id=$(metadata instance-id)
instance_type=$(metadata instance-type)
ami_id=$(metadata ami-id)
private_ip=$(metadata local-ipv4)
observer_duration_seconds=$OBSERVER_DURATION_SECONDS
peer_duration_seconds=$PEER_DURATION_SECONDS
warmup_seconds=$WARMUP_SECONDS
fault_duration_seconds=$FAULT_DURATION_SECONDS
recovery_seconds=$RECOVERY_SECONDS
jar_sha256=$FD_JAR_SHA256
data_mount=$FD_DATA_MOUNT
state_root=$FD_STATE
state_device=$(findmnt -n -o SOURCE --target "$FD_STATE")
storage_medium=$FD_STORAGE_MEDIUM
storage_model=$FD_STORAGE_MODEL
storage_fault_contract=$FD_STORAGE_CLAIM
EOF

if [[ "$FD_NODE_ID" == "0" ]]; then
  result=$(sed -n 's/^RESULT_JSON=//p' "$run_dir/events.log" | tail -n 1)
  if [[ -z "$result" ]]; then
    echo "observer did not produce RESULT_JSON for $run_id" >&2
    aws s3 cp --region "$AWS_REGION" --recursive "$run_dir" \
      "$FD_S3_PREFIX/runs/$run_id/node-$FD_NODE_ID/" >/dev/null || true
    exit 1
  fi
  printf '%s\n' "$result" >"$run_dir/result.json"
  python3 -m json.tool "$run_dir/result.json" >/dev/null
fi

aws s3 cp --region "$AWS_REGION" --recursive "$run_dir" \
  "$FD_S3_PREFIX/runs/$run_id/node-$FD_NODE_ID/" >/dev/null

systemctl stop "$FD_UNIT" >/dev/null 2>&1 || true
echo "run collected run=$run_id node=$FD_NODE_ID"
