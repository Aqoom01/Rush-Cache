#!/usr/bin/env bash
set -euo pipefail
# JDK 표준 API만으로 backend를 빌드한다. Gradle/네트워크/외부 라이브러리가 필요 없다.
if [[ $# -ne 2 ]]; then
  echo 'usage: experiments/system-integration/run.sh <config.properties> <new-output-dir>' >&2
  exit 2
fi
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"
compile_dir=$(mktemp -d /tmp/openkache-system-integration.XXXXXX)
trap 'rm -rf -- "$compile_dir"' EXIT
mapfile -t sources < <(rg --files cache-core/src/main/java -g '*.java' | rg -v '/src/test/' | sort)
sources+=(cache-core/src/test/java/io/openkache/core/replication/DirectMailbox.java)
sources+=(cache-core/src/test/java/io/openkache/core/integration/SystemVariant.java)
sources+=(cache-core/src/test/java/io/openkache/core/integration/IntegratedCluster.java)
sources+=(cache-core/src/test/java/io/openkache/core/integration/SystemIntegrationMain.java)
javac --release 21 -Xlint:all -Werror -d "$compile_dir" "${sources[@]}"
java -Xmx768m -cp "$compile_dir" io.openkache.core.integration.SystemIntegrationMain "$1" "$2"
