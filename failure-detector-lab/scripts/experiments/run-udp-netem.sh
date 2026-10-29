#!/usr/bin/env sh
set -eu

if [ "$#" -lt 3 ]; then
  echo "usage: $0 PROFILE crash|loss|loss15|loss45|delay OUTPUT_DIR [3|5|8] [NETEM_SEED]" >&2
  exit 2
fi

profile=$1
fault=$2
output_dir=$3
node_count=${4:-3}
netem_seed=${5:-}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
lab_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
root_dir=$(CDPATH= cd -- "$lab_dir/.." && pwd)
compose_file="$lab_dir/docker/compose.udp.yml"
docker_bin=${DOCKER_BIN:-docker}
project="openkache-fd-udp-$$"
verify_helper_count=1
verify_failure_quorum=1
verify_failed_rounds=2

case "$profile" in
  F0|f0) profile=fixed-deadline ;;
  F1|f1) profile=phi-gossip ;;
  F2|f2) profile=swim-base ;;
  F3|f3) profile=swim-lifeguard ;;
  F4|f4) profile=adaptive-deadline ;;
  F5|f5) profile=fixed-deadline-ring ;;
  F6|f6) profile=fixed-deadline-ring-verify ;;
  F7a|f7a) profile=swim-lha-probe ;;
  F7b|f7b) profile=swim-lha-suspicion ;;
  ring-verify-h2-q2-r4)
    profile=fixed-deadline-ring-verify
    verify_helper_count=2
    verify_failure_quorum=2
    verify_failed_rounds=4
    ;;
  ring-verify-h3-q3-r3)
    profile=fixed-deadline-ring-verify
    verify_helper_count=3
    verify_failure_quorum=3
    verify_failed_rounds=3
    ;;
esac
case "$profile" in
  fixed-deadline|adaptive-deadline|swim-base|swim-lifeguard|phi-gossip|\
  fixed-deadline-ring|fixed-deadline-ring-verify|swim-lha-probe|swim-lha-suspicion) ;;
  *) echo "unknown profile: $profile" >&2; exit 2 ;;
esac
case "$fault" in
  crash|loss|loss15|loss45|delay) ;;
  *) echo "unknown fault: $fault" >&2; exit 2 ;;
esac
case "$node_count" in
  3)
    services="node0 node1 node2"
    peers="0=node0:19000,1=node1:19000,2=node2:19000"
    ;;
  5)
    services="node0 node1 node2 node3 node4"
    peers="0=node0:19000,1=node1:19000,2=node2:19000,3=node3:19000,4=node4:19000"
    ;;
  8)
    services="node0 node1 node2 node3 node4 node5 node6 node7"
    peers="0=node0:19000,1=node1:19000,2=node2:19000,3=node3:19000,4=node4:19000,5=node5:19000,6=node6:19000,7=node7:19000"
    ;;
  *) echo "node count must be 3, 5, or 8" >&2; exit 2 ;;
esac
if [ -e "$output_dir" ]; then
  echo "output directory already exists: $output_dir" >&2
  exit 1
fi
mkdir -p "$output_dir"

cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

env_file="$output_dir/compose.env"
printf 'FD_PROFILE=%s\nFD_OBSERVER_DURATION_SECONDS=15\nFD_PEER_DURATION_SECONDS=20\nFD_PEERS=%s\nFD_VERIFY_HELPER_COUNT=%s\nFD_VERIFY_FAILURE_QUORUM=%s\nFD_VERIFY_FAILED_ROUNDS=%s\n' \
  "$profile" "$peers" "$verify_helper_count" "$verify_failure_quorum" \
  "$verify_failed_rounds" >"$env_file"
compose() {
  "$docker_bin" compose --env-file "$env_file" -f "$compose_file" -p "$project" "$@"
}
if [ "${FD_UDP_SKIP_BUILD:-0}" = "1" ]; then
  compose up --no-build --detach $services
else
  "$root_dir/gradlew" -p "$lab_dir" jar
  compose up --build --detach $services
fi

observer=$(compose ps -q node0)
target=$(compose ps -q node1)
sleep 5

"$docker_bin" exec "$observer" java -cp /app/lab.jar \
  com.openkache.failuredetector.transport.UdpExperimentNodeMain control 127.0.0.1 19000

apply_loss() {
  rate=$1
  if [ -n "$netem_seed" ]; then
    "$docker_bin" exec "$target" tc qdisc replace dev eth0 root netem \
      loss "$rate" seed "$netem_seed"
  else
    "$docker_bin" exec "$target" tc qdisc replace dev eth0 root netem loss "$rate"
  fi
}

case "$fault" in
  crash)
    "$docker_bin" stop --time 0 "$target" >/dev/null
    ;;
  loss)
    apply_loss 30%
    ;;
  loss15)
    apply_loss 15%
    ;;
  loss45)
    apply_loss 45%
    ;;
  delay)
    "$docker_bin" exec "$target" tc qdisc replace dev eth0 root netem delay 250ms 50ms
    ;;
esac

if [ "$fault" != "crash" ]; then
  "$docker_bin" exec "$target" tc qdisc show dev eth0 >"$output_dir/netem.txt"
fi

"$docker_bin" wait "$observer" >/dev/null
"$docker_bin" logs "$observer" >"$output_dir/events.log" 2>&1
result=$(sed -n 's/^RESULT_JSON=//p' "$output_dir/events.log" | tail -n 1)
if [ -z "$result" ]; then
  echo "observer did not produce RESULT_JSON" >&2
  exit 1
fi
printf '%s\n' "$result" >"$output_dir/result.json"
printf 'profile=%s\nfault=%s\nnodes=%s\nnetem_seed=%s\nsource=real-udp-observed\n' \
  "$profile" "$fault" "$node_count" "$netem_seed" >"$output_dir/run.properties"
echo "UDP/netem result: $output_dir/result.json"
