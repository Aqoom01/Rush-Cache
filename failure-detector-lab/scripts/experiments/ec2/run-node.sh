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

if [[ "$FD_NODE_ID" == "0" ]]; then
  duration=$OBSERVER_DURATION_SECONDS
else
  duration=$PEER_DURATION_SECONDS
fi

systemctl stop "$FD_UNIT" >/dev/null 2>&1 || true
systemctl reset-failed "$FD_UNIT" >/dev/null 2>&1 || true

iface=$(ip route get "$NODE0_IP" | awk '/dev/ {for (i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
if [[ -n "$iface" ]]; then
  tc qdisc del dev "$iface" root >/dev/null 2>&1 || true
fi

rm -f "$run_dir/events.log" "$run_dir/result.json" "$run_dir/gc.log"
touch "$run_dir/events.log"
chown -R fdlab:fdlab "$run_dir"

java_tool_options="-Xms128m -Xmx128m -XX:+UseG1GC -Xlog:gc*:file=$run_dir/gc.log:time,uptime,level,tags"
systemd-run \
  --unit "$FD_UNIT" \
  --uid fdlab \
  --working-directory "$run_dir" \
  --property CPUAccounting=yes \
  --property IOAccounting=yes \
  --property MemoryAccounting=yes \
  --property CPUQuota=100% \
  --setenv "FD_NODE_ID=$FD_NODE_ID" \
  --setenv "FD_PROFILE=$PROFILE_VALUE" \
  --setenv "FD_PRODUCT_ALGORITHM=$PRODUCT_ALGORITHM" \
  --setenv "FD_PORT=19000" \
  --setenv "FD_TCP_PORT=19001" \
  --setenv "FD_DURATION_SECONDS=$duration" \
  --setenv "FD_PEERS=$PEERS" \
  --setenv "JAVA_TOOL_OPTIONS=$java_tool_options" \
  --setenv "FD_RUN_DIR=$run_dir" \
  /bin/bash -c \
  "exec java -cp '$FD_ROOT/lab.jar' '$MAIN_CLASS' >>'$run_dir/events.log' 2>&1"

printf '%s\n' "$run_id" >/run/openkache-fd-current-run

ready=0
for _ in $(seq 1 100); do
  if ss -H -lun | awk '{print $5}' | grep -Eq '(^|:)19000$'; then
    ready=1
    break
  fi
  if ! systemctl is-active --quiet "$FD_UNIT"; then
    break
  fi
  sleep 0.2
done
if [[ "$ready" != "1" ]]; then
  echo "node $FD_NODE_ID did not bind UDP 19000" >&2
  systemctl status "$FD_UNIT" --no-pager >&2 || true
  exit 1
fi

instance_id=$(metadata instance-id)
private_ip=$(metadata local-ipv4)
ready_json="$run_dir/ready.json"
jq -n \
  --arg runId "$run_id" \
  --arg nodeId "$FD_NODE_ID" \
  --arg instanceId "$instance_id" \
  --arg privateIp "$private_ip" \
  --arg readyAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{runId:$runId,nodeId:$nodeId,instanceId:$instanceId,privateIp:$privateIp,readyAt:$readyAt}' \
  >"$ready_json"
aws s3 cp --region "$AWS_REGION" "$ready_json" \
  "$FD_S3_PREFIX/runs/$run_id/ready/node-$FD_NODE_ID.json" >/dev/null

echo "node ready run=$run_id node=$FD_NODE_ID duration=$duration"
