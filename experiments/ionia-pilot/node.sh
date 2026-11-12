#!/usr/bin/env bash
# Usage: bash tools/node.sh start|stop RUN_DIRECTORY NODE_ID
# Own stop files only; never kill a PID based on an untrusted/stale PID file.
set -euo pipefail
TOOLS="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
ACTION="${1:?start|stop}"; RUN="$(cd -- "${2:?run directory}" && pwd -P)"; ID="${3:?node id}"
[[ "$ID" =~ ^[1-5]$ ]] || { echo 'node ID must be 1..5' >&2; exit 2; }
case "$ACTION" in
  start)
    [[ ! -e "$RUN/state/node-$ID.started" && ! -e "$RUN/state/node-$ID.stop" ]] || { echo 'fresh run required' >&2; exit 2; }
    nohup bash "$TOOLS/pilot.sh" node "$(dirname -- "$TOOLS")" "$RUN" "$ID" > "$RUN/state/node-$ID.stdout.log" 2> "$RUN/state/node-$ID.stderr.log" < /dev/null &
    echo "node $ID launcher PID=$!; readiness: $RUN/state/node-$ID.ready.properties"
    ;;
  stop)
    (set -o noclobber; : > "$RUN/state/node-$ID.stop")
    for ((i=0;i<200;i++)); do
      if [[ -f "$RUN/state/node-$ID.stopped" ]]; then echo "node $ID closed"; exit 0; fi
      sleep 0.1
    done
    echo 'graceful stop not confirmed: inspect retained node logs; no unrelated process was killed' >&2; exit 2
    ;;
  *) echo 'start|stop required' >&2; exit 2 ;;
esac
