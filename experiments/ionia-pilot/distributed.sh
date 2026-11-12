#!/usr/bin/env bash
set -euo pipefail
BUNDLE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -cp "$BUNDLE/lib/*" io.openkache.server.benchmark.pilot.DistributedPilotMain "$@"
