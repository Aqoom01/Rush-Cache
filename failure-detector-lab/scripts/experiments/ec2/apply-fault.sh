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
fault_status=0
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
    sleep "$FAULT_DURATION_SECONDS"
    tc qdisc del dev "$iface" root
    # Send the phase marker only after restoring the path: the target's egress
    # netem filter also affects this UDP control frame.
    send_control end
    write_json_event "$events" released node1
    tc -s qdisc show dev "$iface" >"$run_dir/netem-released.txt"
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
    fio_file="$FD_DATA_MOUNT/fio-work/$run_id.fio"
    fio_output="$run_dir/fio.json"
    fio_validation="$run_dir/fio-validation.json"
    cleanup_fio_file() {
      rm -f "$fio_file"
    }
    trap cleanup_fio_file EXIT
    fio_exit_code=0
    fio_wall_started_nanos=$(date +%s%N)
    fio --name=openkache-fd --filename="$fio_file" \
      --allow_file_create=0 --ioengine=libaio --rw=randrw --rwmixread=50 \
      --bs=4k --iodepth=32 --size=1G \
      --runtime="$FAULT_DURATION_SECONDS" --time_based=1 --direct=1 \
      --group_reporting=1 --eta=never \
      --output="$fio_output" --output-format=json || fio_exit_code=$?
    fio_wall_finished_nanos=$(date +%s%N)
    fio_wall_duration_ms=$(((fio_wall_finished_nanos - fio_wall_started_nanos) / 1000000))
    if ! python3 "$script_dir/validate-fio-evidence.py" \
      --input "$fio_output" \
      --output "$fio_validation" \
      --fio-exit-code "$fio_exit_code" \
      --state-path "$run_dir/events.log" \
      --fio-path "$fio_file" \
      --storage-device "$FD_STORAGE_DEVICE" \
      --mounted-device "$(findmnt -n -o SOURCE --target "$FD_STATE")" \
      --storage-model "$FD_STORAGE_MODEL" \
      --expected-storage-model "Amazon EC2 NVMe Instance Storage" \
      --expected-engine libaio \
      --expected-iodepth 32 \
      --min-iops "${FD_FIO_MIN_IOPS:-1500}" \
      --min-deep-queue-percent "${FD_FIO_MIN_DEEP_QUEUE_PERCENT:-80}" \
      --min-runtime-ms "$((FAULT_DURATION_SECONDS * 900))" \
      --wall-duration-ms "$fio_wall_duration_ms" \
      --max-wall-duration-ms "$((FAULT_DURATION_SECONDS * 1500))"; then
      fault_status=1
    fi
    cleanup_fio_file
    trap - EXIT
    send_control end
    write_json_event "$events" released node1
    aws s3 cp --region "$AWS_REGION" "$fio_output" \
      "$FD_S3_PREFIX/runs/$run_id/node-$FD_NODE_ID/fio.json" >/dev/null || true
    aws s3 cp --region "$AWS_REGION" "$fio_validation" \
      "$FD_S3_PREFIX/runs/$run_id/node-$FD_NODE_ID/fio-validation.json" >/dev/null || true
    ;;
  *)
    echo "unsupported fault for remote application: $FAULT" >&2
    exit 2
    ;;
esac

aws s3 cp --region "$AWS_REGION" "$events" \
  "$FD_S3_PREFIX/runs/$run_id/node-$FD_NODE_ID/fault-events.jsonl" >/dev/null
if ((fault_status != 0)); then
  echo "fault evidence validation failed run=$run_id fault=$FAULT" >&2
  exit "$fault_status"
fi
echo "fault applied run=$run_id fault=$FAULT node=$FD_NODE_ID"
