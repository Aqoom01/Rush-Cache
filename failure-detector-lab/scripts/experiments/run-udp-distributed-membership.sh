#!/usr/bin/env sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 OUTPUT_DIR" >&2
  exit 2
fi

output_dir=$1
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
lab_dir=$(CDPATH= cd -- "$script_dir/../.." && pwd)
root_dir=$(CDPATH= cd -- "$lab_dir/.." && pwd)
compose_file="$lab_dir/docker/compose.udp.yml"
docker_bin=${DOCKER_BIN:-docker}
project="openkache-fd-membership-$$"
services="node0 node1 node2 node3 node4 node5 node6 node7"
survivors="0 4 5 6 7"
peers="0=node0:19000,1=node1:19000,2=node2:19000,3=node3:19000,4=node4:19000,5=node5:19000,6=node6:19000,7=node7:19000"
main_class="com.openkache.failuredetector.transport.UdpDistributedMembershipNodeMain"

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
printf '%s\n' \
  "FD_MAIN_CLASS=$main_class" \
  "FD_PROFILE=fixed-deadline-ring-verify" \
  "FD_OBSERVER_DURATION_SECONDS=0" \
  "FD_PEER_DURATION_SECONDS=0" \
  "FD_PEERS=$peers" \
  "FD_RING_NEIGHBOR_COUNT=2" \
  "FD_VERIFY_HELPER_COUNT=3" \
  "FD_VERIFY_FAILURE_QUORUM=3" \
  "FD_VERIFY_FAILED_ROUNDS=3" >"$env_file"

compose() {
  "$docker_bin" compose --env-file "$env_file" -f "$compose_file" -p "$project" "$@"
}

if [ "${FD_UDP_SKIP_BUILD:-0}" = "1" ]; then
  compose up --no-build --detach $services
else
  "$root_dir/gradlew" -p "$lab_dir" jar
  compose up --build --detach $services
fi

controller=$(compose ps -q node0)
sleep 5

"$docker_bin" exec "$controller" java -cp /app/lab.jar "$main_class" \
  inject "node0,node1,node2,node3,node4,node5,node6,node7" 19000 \
  "node1,node2,node3"

sleep 12

"$docker_bin" exec "$controller" java -cp /app/lab.jar "$main_class" \
  control "node0,node4,node5,node6,node7" 19000 finish

for node in $survivors; do
  container=$(compose ps -aq "node$node")
  "$docker_bin" wait "$container" >/dev/null
done

for node in 0 1 2 3 4 5 6 7; do
  container=$(compose ps -aq "node$node")
  "$docker_bin" logs "$container" >"$output_dir/node-$node.events.log" 2>&1
done

for node in $survivors; do
  result=$(sed -n 's/^RESULT_JSON=//p' \
    "$output_dir/node-$node.events.log" | tail -n 1)
  if [ -z "$result" ]; then
    echo "survivor node-$node did not produce RESULT_JSON" >&2
    exit 1
  fi
  printf '%s\n' "$result" >"$output_dir/node-$node.result.json"
done

printf '%s\n' \
  "profile=ring2-distributed-h3-q3-r3" \
  "fault=consecutive-three-crash" \
  "nodes=8" \
  "crash_targets=1,2,3" \
  "fault_injection=udp-process-halt" \
  "fault_start_after_seconds=5" \
  "observation_after_fault_seconds=12" \
  "source=real-udp-observed" >"$output_dir/run.properties"

echo "UDP distributed membership result: $output_dir"
