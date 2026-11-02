#!/usr/bin/env bash
# Linux + Java 21. Local commands only: never SSH or change firewall/system settings.
set -euo pipefail
BUNDLE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -cp "$BUNDLE/lib/*" io.openkache.server.benchmark.pilot.PilotMain "$@"
