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
events="$run_dir/fault-events.jsonl"
pid=$(systemctl show "$FD_UNIT" --property MainPID --value)
if [[ -z "$pid" || "$pid" == "0" ]]; then
  echo "detector process is not active for $run_id" >&2
  exit 1
fi

send_control() {
  local phase=$1
  java -cp "$FD_ROOT/lab.jar" "$MAIN_CLASS" control "$NODE0_IP" 19000 "$phase"
}

subject=node1
[[ "$FAULT" == *-observer ]] && subject=node0
send_control start
write_json_event "$events" requested "$subject"

case "$FAULT" in
  crash)
    systemctl kill --signal=KILL "$FD_UNIT"
    write_json_event "$events" applied node1
    ;;
  loss15|loss45|delay)
    iface=$(ip route get "$NODE0_IP" | awk '/dev/ {for (i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
    tc qdisc del dev "$iface" root >/dev/null 2>&1 || true
    tc qdisc add dev "$iface" root handle 1: prio bands 3
    if [[ "$FAULT" == "delay" ]]; then
      tc qdisc add dev "$iface" parent 1:3 handle 30: netem delay 250ms 50ms
    else
      rate=${FAULT#loss}
      tc qdisc add dev "$iface" parent 1:3 handle 30: netem loss "$rate%" seed "$SEED"
    fi
    tc filter add dev "$iface" protocol ip parent 1:0 prio 3 u32 \
      match ip protocol 17 0xff match ip dport 19000 0xffff flowid 1:3
    tc filter add dev "$iface" protocol ip parent 1:0 prio 4 u32 \
      match ip protocol 6 0xff match ip dport 19001 0xffff flowid 1:3
    tc -s qdisc show dev "$iface" >"$run_dir/netem.txt"
    write_json_event "$events" applied node1
    ;;
  cpu-pause-target|cpu-pause-observer)
    kill -STOP "$pid"
    write_json_event "$events" applied "$subject"
    sleep "$FAULT_DURATION_SECONDS"
    send_control end
    kill -CONT "$pid"
    write_json_event "$events" released "$subject"
    ;;
  cpu-throttle-target|cpu-throttle-observer)
    systemctl set-property --runtime "$FD_UNIT" "CPUQuota=$THROTTLE_PERCENT%"
    write_json_event "$events" applied "$subject"
    sleep "$FAULT_DURATION_SECONDS"
    send_control end
    systemctl set-property --runtime "$FD_UNIT" CPUQuota=100%
    write_json_event "$events" released "$subject"
    ;;
  cpu-contention-target)
    write_json_event "$events" applied node1
    stress-ng --cpu "$CPU_WORKERS" --cpu-method matrixprod --cpu-load 100 \
      --timeout "${FAULT_DURATION_SECONDS}s" --metrics-brief \
      >"$run_dir/cpu-stress.log" 2>&1
    send_control end
    write_json_event "$events" released node1
    ;;
  io-contention-target)
    write_json_event "$events" applied node1
    fio --name=openkache-fd --filename="/mnt/openkache-fd-data/$run_id.fio" \
      --rw=randrw --rwmixread=50 --bs=4k --iodepth=32 --size=1G \
      --runtime="$FAULT_DURATION_SECONDS" --time_based=1 --direct=1 \
      --output="$run_dir/fio.json" --output-format=json
    rm -f "/mnt/openkache-fd-data/$run_id.fio"
    send_control end
    write_json_event "$events" released node1
    ;;
  *)
    echo "unsupported fault for remote application: $FAULT" >&2
    exit 2
    ;;
esac

aws s3 cp --region "$AWS_REGION" "$events" \
  "$FD_S3_PREFIX/runs/$run_id/node-$FD_NODE_ID/fault-events.jsonl" >/dev/null
echo "fault applied run=$run_id fault=$FAULT node=$FD_NODE_ID"
