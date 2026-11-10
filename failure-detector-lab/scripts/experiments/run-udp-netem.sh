#!/usr/bin/env sh
set -eu

if [ "$#" -lt 3 ]; then
  echo "usage: $0 PROFILE FAULT OUTPUT_DIR [3|5|8] [NETEM_SEED]" >&2
  echo "fault: normal|crash|loss|loss15|loss45|delay|cpu-pause-target|cpu-pause-observer|cpu-throttle-target|cpu-throttle-observer|cpu-contention-target|io-contention-target" >&2
  exit 2
fi

requested_profile=$1
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
fault_duration=${FD_FAULT_DURATION_SECONDS:-5}
warmup_seconds=${FD_WARMUP_SECONDS:-5}
recovery_seconds=${FD_RECOVERY_SECONDS:-10}
node_cpus=${FD_NODE_CPUS:-1.0}
node_memory=${FD_NODE_MEMORY:-512m}
node_pids_limit=${FD_NODE_PIDS_LIMIT:-256}
throttle_cpus=${FD_THROTTLE_CPUS:-0.10}
cpu_workers=${FD_CPU_STRESS_WORKERS:-4}
io_workers=${FD_IO_STRESS_WORKERS:-2}
io_bytes=${FD_IO_STRESS_BYTES:-512M}
stats_interval=${FD_STATS_INTERVAL_SECONDS:-1}
stats_pid=
observer=
target=
main_class=com.openkache.failuredetector.transport.UdpExperimentNodeMain
product_algorithm=
implementation_type=lab-mechanism

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
  product-swim-lifeguard)
    profile=swim-lifeguard
    product_algorithm=swim-lifeguard
    implementation_type=inspired-real-socket-adapter
    ;;
  product-redis-cluster)
    profile=redis-cluster
    product_algorithm=redis-cluster
    main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
    implementation_type=inspired-real-socket-adapter
    ;;
  product-phi-gossip)
    profile=phi-gossip
    product_algorithm=phi-gossip
    main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
    implementation_type=inspired-real-socket-adapter
    ;;
  product-ignite-tcp-ring)
    profile=ignite-tcp-ring
    product_algorithm=ignite-tcp-ring
    main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
    implementation_type=inspired-real-socket-adapter
    ;;
  product-aerospike-mesh)
    profile=aerospike-mesh
    product_algorithm=aerospike-mesh
    main_class=com.openkache.failuredetector.transport.ProductUdpExperimentNodeMain
    implementation_type=inspired-real-socket-adapter
    ;;
esac
case "$profile" in
  fixed-deadline|adaptive-deadline|swim-base|swim-lifeguard|phi-gossip|\
  fixed-deadline-ring|fixed-deadline-ring-verify|swim-lha-probe|swim-lha-suspicion|\
  redis-cluster|ignite-tcp-ring|aerospike-mesh) ;;
  *) echo "unknown profile: $profile" >&2; exit 2 ;;
esac
case "$fault" in
  normal|crash|loss|loss15|loss45|delay|cpu-pause-target|cpu-pause-observer|\
  cpu-throttle-target|cpu-throttle-observer|cpu-contention-target|\
  io-contention-target) ;;
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
case "$fault_duration:$warmup_seconds:$recovery_seconds:$cpu_workers:$io_workers" in
  *[!0-9:]*) echo "duration and worker settings must be non-negative integers" >&2; exit 2 ;;
esac
case "$node_cpus:$throttle_cpus" in
  *[!0-9.:]*) echo "CPU settings must be positive decimal numbers" >&2; exit 2 ;;
esac
case "$io_bytes" in
  *[!0-9KkMmGg]*) echo "FD_IO_STRESS_BYTES must be a byte count such as 512M" >&2; exit 2 ;;
esac

case "$fault" in
  normal)
    observer_duration=${FD_OBSERVER_DURATION_SECONDS:-60}
    peer_duration=${FD_PEER_DURATION_SECONDS:-65}
    ;;
  cpu-*|io-*)
    observer_duration=${FD_OBSERVER_DURATION_SECONDS:-$((15 + warmup_seconds + fault_duration + recovery_seconds))}
    peer_duration=${FD_PEER_DURATION_SECONDS:-$((observer_duration + 5))}
    ;;
  *)
    observer_duration=${FD_OBSERVER_DURATION_SECONDS:-15}
    peer_duration=${FD_PEER_DURATION_SECONDS:-20}
    ;;
esac
if [ -n "$product_algorithm" ]; then
  case "$fault" in
    crash|loss|loss15|loss45|delay)
      observer_duration=${FD_OBSERVER_DURATION_SECONDS:-60}
      peer_duration=${FD_PEER_DURATION_SECONDS:-65}
      ;;
  esac
fi

if [ -e "$output_dir" ]; then
  echo "output directory already exists: $output_dir" >&2
  exit 1
fi
mkdir -p "$output_dir/logs" "$output_dir/gc" "$output_dir/cgroup"

env_file="$output_dir/compose.env"
printf 'FD_PROFILE=%s\nFD_PRODUCT_ALGORITHM=%s\nFD_MAIN_CLASS=%s\nFD_OBSERVER_DURATION_SECONDS=%s\nFD_PEER_DURATION_SECONDS=%s\nFD_PEERS=%s\nFD_VERIFY_HELPER_COUNT=%s\nFD_VERIFY_FAILURE_QUORUM=%s\nFD_VERIFY_FAILED_ROUNDS=%s\nFD_NODE_CPUS=%s\nFD_NODE_MEMORY=%s\nFD_NODE_PIDS_LIMIT=%s\n' \
  "$profile" "${product_algorithm:-swim-lifeguard}" "$main_class" \
  "$observer_duration" "$peer_duration" "$peers" \
  "$verify_helper_count" "$verify_failure_quorum" "$verify_failed_rounds" \
  "$node_cpus" "$node_memory" "$node_pids_limit" >"$env_file"

compose() {
  "$docker_bin" compose --env-file "$env_file" -f "$compose_file" -p "$project" "$@"
}

cleanup() {
  if [ -n "$stats_pid" ]; then
    kill "$stats_pid" >/dev/null 2>&1 || true
    wait "$stats_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$observer" ]; then
    "$docker_bin" unpause "$observer" >/dev/null 2>&1 || true
    "$docker_bin" update --cpus "$node_cpus" "$observer" >/dev/null 2>&1 || true
  fi
  if [ -n "$target" ]; then
    "$docker_bin" unpause "$target" >/dev/null 2>&1 || true
    "$docker_bin" update --cpus "$node_cpus" "$target" >/dev/null 2>&1 || true
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

record_fault_event() {
  event=$1
  subject=$2
  printf '{"event":"%s","fault":"%s","subject":"%s","wallTime":"%s","wallEpochNanos":%s}\n' \
    "$event" "$fault" "$subject" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" "$(date +%s%N)" \
    >>"$output_dir/fault-events.jsonl"
}

send_control() {
  phase=$1
  "$docker_bin" exec "$observer" java -cp /app/lab.jar \
    "$main_class" \
    control 127.0.0.1 19000 "$phase"
}

send_control_from_target() {
  phase=$1
  "$docker_bin" exec "$target" java -cp /app/lab.jar \
    "$main_class" \
    control node0 19000 "$phase"
}

capture_cgroup() {
  phase=$1
  for service in $services; do
    container=$(compose ps -q "$service" 2>/dev/null || true)
    [ -n "$container" ] || continue
    if [ "$("$docker_bin" inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" = "true" ]; then
      "$docker_bin" exec "$container" sh -c \
        'for f in /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.events /sys/fs/cgroup/io.stat; do if [ -r "$f" ]; then echo "### $f"; cat "$f"; fi; done' \
        >"$output_dir/cgroup/$service-$phase.txt" 2>&1 || true
    fi
  done
}

sample_resources() {
  while :; do
    sampled_at=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
    containers=
    for service in $services; do
      container=$(compose ps -q "$service" 2>/dev/null || true)
      [ -n "$container" ] || continue
      containers="$containers $container"
    done
    if [ -n "$containers" ]; then
      "$docker_bin" stats --no-stream --format '{{.Name}}	{{json .}}' $containers \
        2>/dev/null | while IFS="$(printf '\t')" read -r name stats; do
          service=$(printf '%s\n' "$name" | sed -n 's/.*-\(node[0-9][0-9]*\)-[0-9][0-9]*$/\1/p')
          [ -n "$service" ] || continue
          printf '{"sampledAt":"%s","service":"%s","stats":%s}\n' \
            "$sampled_at" "$service" "$stats"
        done
    fi
    sleep "$stats_interval"
  done
}

wait_for_container_file() {
  container=$1
  file=$2
  attempts=$3
  current=0
  while [ "$current" -lt "$attempts" ]; do
    if "$docker_bin" exec "$container" test -f "$file" >/dev/null 2>&1; then
      return 0
    fi
    current=$((current + 1))
    sleep 0.2
  done
  return 1
}

run_stress_fault() {
  kind=$1
  if [ "$kind" = "cpu" ]; then
    command="stress-ng --cpu $cpu_workers --cpu-method matrixprod --cpu-load 100 --timeout ${fault_duration}s --metrics-brief"
  else
    command="stress-ng --temp-path /var/tmp --hdd $io_workers --hdd-bytes $io_bytes --hdd-opts sync --timeout ${fault_duration}s --metrics-brief"
  fi
  "$docker_bin" exec -d "$target" sh -c \
    "touch /tmp/fd-fault.started; $command >/tmp/fd-stress.log 2>&1; rc=\$?; java -cp /app/lab.jar $main_class control node0 19000 end; echo \$rc >/tmp/fd-fault.rc; touch /tmp/fd-fault.finished"
  if ! wait_for_container_file "$target" /tmp/fd-fault.started 50; then
    echo "$kind stressor did not start" >&2
    return 1
  fi
  record_fault_event applied node1
  if ! wait_for_container_file "$target" /tmp/fd-fault.finished $((fault_duration * 10 + 100)); then
    echo "$kind stressor did not finish" >&2
    return 1
  fi
  "$docker_bin" exec "$target" cat /tmp/fd-stress.log >"$output_dir/$kind-stress.log" 2>&1 || true
  stress_rc=$("$docker_bin" exec "$target" cat /tmp/fd-fault.rc 2>/dev/null || true)
  if [ "$stress_rc" != "0" ]; then
    echo "$kind stressor failed with exit code ${stress_rc:-unknown}" >&2
    return 1
  fi
  record_fault_event released node1
}

if [ "${FD_UDP_SKIP_BUILD:-0}" = "1" ]; then
  compose up --no-build --detach $services
else
  "$root_dir/gradlew" -p "$lab_dir" jar
  "$docker_bin" build \
    --file "$lab_dir/docker/Dockerfile.udp" \
    --tag openkache-fd-udp:local \
    "$lab_dir"
  compose up --no-build --detach $services
fi

observer=$(compose ps -q node0)
target=$(compose ps -q node1)
compose ps >"$output_dir/compose-ps.txt"
compose config >"$output_dir/compose-resolved.yml"
"$docker_bin" version >"$output_dir/docker-version.txt" 2>&1 || true
git -C "$root_dir" rev-parse HEAD >"$output_dir/git-commit.txt" 2>&1 || true
capture_cgroup before-fault
sample_resources >"$output_dir/resource-stats.jsonl" &
stats_pid=$!

sleep "$warmup_seconds"
if [ "$fault" != "normal" ]; then
  send_control start
  case "$fault" in
    *-observer) fault_subject=node0 ;;
    *) fault_subject=node1 ;;
  esac
  record_fault_event requested "$fault_subject"
fi

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
  normal)
    # Observe the complete healthy run without injecting a fault or fault marker.
    normal_remaining=$((observer_duration - warmup_seconds - 1))
    if [ "$normal_remaining" -gt 0 ]; then
      sleep "$normal_remaining"
    fi
    ;;
  crash)
    "$docker_bin" stop --time 0 "$target" >/dev/null
    record_fault_event applied node1
    ;;
  loss)
    apply_loss 30%
    record_fault_event applied node1
    ;;
  loss15)
    apply_loss 15%
    record_fault_event applied node1
    ;;
  loss45)
    apply_loss 45%
    record_fault_event applied node1
    ;;
  delay)
    "$docker_bin" exec "$target" tc qdisc replace dev eth0 root netem delay 250ms 50ms
    record_fault_event applied node1
    ;;
  cpu-pause-target)
    "$docker_bin" pause "$target" >/dev/null
    record_fault_event applied node1
    sleep "$fault_duration"
    send_control end
    "$docker_bin" unpause "$target" >/dev/null
    record_fault_event released node1
    ;;
  cpu-pause-observer)
    "$docker_bin" pause "$observer" >/dev/null
    record_fault_event applied node0
    sleep "$fault_duration"
    send_control_from_target end
    "$docker_bin" unpause "$observer" >/dev/null
    record_fault_event released node0
    ;;
  cpu-throttle-target)
    "$docker_bin" update --cpus "$throttle_cpus" "$target" >/dev/null
    record_fault_event applied node1
    sleep "$fault_duration"
    send_control end
    "$docker_bin" update --cpus "$node_cpus" "$target" >/dev/null
    record_fault_event released node1
    ;;
  cpu-throttle-observer)
    "$docker_bin" update --cpus "$throttle_cpus" "$observer" >/dev/null
    record_fault_event applied node0
    sleep "$fault_duration"
    send_control_from_target end
    "$docker_bin" update --cpus "$node_cpus" "$observer" >/dev/null
    record_fault_event released node0
    ;;
  cpu-contention-target)
    run_stress_fault cpu
    ;;
  io-contention-target)
    run_stress_fault io
    ;;
esac

case "$fault" in
  loss|loss15|loss45|delay)
    "$docker_bin" exec "$target" tc qdisc show dev eth0 >"$output_dir/netem.txt"
    sleep "$fault_duration"
    "$docker_bin" exec "$target" tc qdisc del dev eth0 root
    send_control end
    record_fault_event released node1
    "$docker_bin" exec "$target" tc qdisc show dev eth0 \
      >"$output_dir/netem-released.txt"
    ;;
esac

capture_cgroup after-fault
"$docker_bin" wait "$observer" >/dev/null
kill "$stats_pid" >/dev/null 2>&1 || true
wait "$stats_pid" >/dev/null 2>&1 || true
stats_pid=

for service in $services; do
  container=$(compose ps -aq "$service" 2>/dev/null || true)
  [ -n "$container" ] || continue
  "$docker_bin" logs "$container" >"$output_dir/logs/$service.log" 2>&1 || true
  "$docker_bin" inspect "$container" >"$output_dir/logs/$service.inspect.json" 2>&1 || true
  ("$docker_bin" cp "$container:/tmp/gc.log" - 2>/dev/null | tar -xO) \
    >"$output_dir/gc/$service.log" 2>&1 || true
done
cp "$output_dir/logs/node0.log" "$output_dir/events.log"

result=$(sed -n 's/^RESULT_JSON=//p' "$output_dir/events.log" | tail -n 1)
if [ -z "$result" ]; then
  echo "observer did not produce RESULT_JSON" >&2
  exit 1
fi
printf '%s\n' "$result" >"$output_dir/result.json"
printf 'profile=%s\nrequested_profile=%s\nimplementation_type=%s\nmain_class=%s\nfault=%s\nnodes=%s\nnetem_seed=%s\nsource=real-socket-observed\nenvironment=docker-single-host\nobserver_duration_seconds=%s\npeer_duration_seconds=%s\nwarmup_seconds=%s\nfault_duration_seconds=%s\nrecovery_seconds=%s\nnode_cpus=%s\nnode_memory=%s\nthrottle_cpus=%s\ncpu_stress_workers=%s\nio_stress_workers=%s\nio_stress_bytes=%s\n' \
  "$profile" "$requested_profile" "$implementation_type" "$main_class" \
  "$fault" "$node_count" "$netem_seed" "$observer_duration" "$peer_duration" "$warmup_seconds" \
  "$fault_duration" "$recovery_seconds" "$node_cpus" "$node_memory" \
  "$throttle_cpus" "$cpu_workers" "$io_workers" "$io_bytes" \
  >"$output_dir/run.properties"
echo "UDP/Docker fault result: $output_dir/result.json"
