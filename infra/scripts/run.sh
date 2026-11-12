#!/usr/bin/env bash
# 부하 생성 노드에서 experiment-runner를 실행하고 결과를 S3에 올린다.
# 사용법: infra/scripts/run.sh <실험 config.properties, 저장소 루트 기준 경로>
# config는 노드가 빌드한 commit에 들어 있어야 한다. target.host·target.hosts·results.dir·server.storage.note는 이 스크립트가 바꾼다.
# 요청은 config의 routing.boundaries에 따라 cache-server 노드들로 나뉜다(없으면 1번 노드로만).
# 실행 전에 모든 cache-server를 재시작해 빈 저장소에서 시작한다(앞 실험의 데이터가 남지 않게).
# config의 deploy.* key는 이 스크립트만 읽고 Runner에는 넘기지 않는다.
#   deploy.server=kache(기본, 기존 cache-server) | b1(B1ServerMain, node 0이 Controller)
#   deploy.members=<앞에서부터 쓸 노드 수, 기본 전부>. 나머지 노드는 서버를 멈춰 둔다.
#   deploy.b1.tablets.per.node=<B1 초기 표의 노드당 Tablet 수, 기본 2>
source "$(dirname "$0")/common.sh"

config="${1:?실험 config 경로가 필요하다(예: experiments/baseline-single-node/config.properties)}"
commit=$(git -C "$REPO_ROOT" rev-parse HEAD)
git -C "$REPO_ROOT" cat-file -e "$commit:$config" 2>/dev/null \
  || { echo "commit ${commit:0:8}에 $config가 없다. 커밋한 config만 실행할 수 있다." >&2; exit 2; }

prop() { git -C "$REPO_ROOT" show "$commit:$config" | sed -n "s/^$1=//p" | tail -n 1 | tr -d '\r'; }
mode=$(prop execution.mode)
if [ -n "$mode" ] && [ "$mode" != tcp ]; then
  echo "TCP 실험(execution.mode=tcp)만 클러스터에서 실행한다: $mode" >&2
  exit 2
fi
study=$(prop study)
variant=$(prop variant)
exec_ms=$(prop execution.timeout.ms)
exec_ms=${exec_ms:-3600000}

server=$(prop deploy.server)
server=${server:-kache}
case "$server" in
  kache | b1) ;;
  *) echo "deploy.server는 kache 또는 b1이다: $server" >&2; exit 2 ;;
esac
tablets_per_node=$(prop deploy.b1.tablets.per.node)
tablets_per_node=${tablets_per_node:-2}

loadgen=$(tf output -raw loadgen_instance_id)
node_ids=$(tf output -json node_instance_ids | tr -d '[]" ' | tr ',' ' ')
all_ips=$(tf output -json node_private_ips | tr -d '[]" ' | tr ',' ' ')
total_nodes=$(printf '%s\n' $all_ips | wc -l)
nodes=$(prop deploy.members)
nodes=${nodes:-$total_nodes}
if ! [ "$nodes" -ge 1 ] 2>/dev/null || [ "$nodes" -gt "$total_nodes" ]; then
  echo "deploy.members=$nodes: 1 이상 띄운 노드 수($total_nodes) 이하여야 한다." >&2
  exit 2
fi
# 멤버는 Terraform 노드 순서의 앞 nodes대다. B1은 이 순서가 NodeId(0부터)다.
node_ips=$(printf '%s\n' $all_ips | head -n "$nodes" | tr '\n' ' ')
node_ips=${node_ips% }
target=${node_ips%% *}
target_hosts=$(printf '%s:7000,' $node_ips)
target_hosts=${target_hosts%,}
# Runner는 서버 저장소를 관측하지 않으므로 결과의 storageLayer에 적을 값을 넘긴다.
storage=$(tf output -raw storage_engine)
case "$server:$storage" in
  b1:*) storage="memory"; storage_note="in-memory (B1Server KvEngine, STORAGE 입력과 무관)" ;;
  *:nvme) storage_note="nvme (instance store, O_DIRECT, docs/storage-nvme.md)" ;;
  *) storage_note="in-memory (io.openkache.core.kv.KvEngine)" ;;
esac

# 자동 정지 전에 끝나지 못할 실험은 시작하지 않는다(5분 여유).
remaining=$(( $(date -d "$(tf output -raw stops_at_utc)" +%s) - $(date +%s) ))
if [ "$remaining" -lt $(( exec_ms / 1000 + 300 )) ]; then
  echo "자동 정지까지 ${remaining}초 남아 execution.timeout.ms=${exec_ms}를 채울 수 없다." >&2
  echo "max_lifetime_minutes를 늘려 다시 띄운다." >&2
  exit 1
fi

# SSM으로 인스턴스들(공백 구분)에서 같은 bash 스크립트를 동시에 실행하고 모두 끝날 때까지 기다린다. 출력은 표준 출력으로, 모두
# 성공이면 0.
ssm_run() {
  local instances=$1 script=$2 timeout=$3 cmd status id failed=0
  # shellcheck disable=SC2086
  cmd=$(aws ssm send-command --instance-ids $instances --document-name AWS-RunShellScript \
    --parameters "{\"commands\":[\"echo $(printf '%s' "$script" | base64 -w0) | base64 -d | bash\"],\"executionTimeout\":[\"$timeout\"]}" \
    --query Command.CommandId --output text)
  for id in $instances; do
    while :; do
      status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
        --query Status --output text 2>/dev/null || echo Pending)
      case "$status" in
        Pending | InProgress | Delayed) sleep 3 ;;
        *) break ;;
      esac
    done
    aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
      --query '[StandardOutputContent, StandardErrorContent]' --output text
    [ "$status" = Success ] || { echo "$id: $status" >&2; failed=1; }
  done
  [ "$failed" -eq 0 ]
}

# 노드마다 같은 스크립트를 돌리고, 노드가 자기 사설 IP로 멤버 여부와 NodeId를 찾는다.
restart=$(cat <<EOF
set -uo pipefail
members="$node_ips"
ip=\$(hostname -I | awk '{print \$1}')
idx=-1; i=0
for m in \$members; do [ "\$m" = "\$ip" ] && idx=\$i; i=\$((i + 1)); done
docker rm -f openkache-b1 > /dev/null 2>&1
if [ "\$idx" -lt 0 ]; then
  docker stop openkache-server > /dev/null 2>&1
  echo "멤버 아님(\$ip): 서버 정지"
  exit 0
fi
if [ "$server" = b1 ]; then
  docker stop openkache-server > /dev/null 2>&1
  printf 'node.id=%s\nhosts=%s\nclient.port=7000\npeer.port=7001\ntablets.per.node=%s\n' \
    "\$idx" "\$(echo \$members | tr ' ' ',')" "$tablets_per_node" > /opt/openkache/b1.properties
  # B1Server는 hosts의 자기 IP에 bind하므로 host network로 띄운다. heap은 기존 cache-server와 같게 둔다.
  # GC 정지를 지연 급등과 맞춰 보려고 GC 로그를 남긴다(실험 뒤 결과 폴더로 올린다). 컨테이너는 root가 아니다.
  mkdir -p /opt/openkache/logs && chmod 777 /opt/openkache/logs && rm -f /opt/openkache/logs/gc.log
  docker run -d --name openkache-b1 --network host \
    -v /opt/openkache/b1.properties:/app/config/b1.properties:ro -v /opt/openkache/logs:/logs \
    --entrypoint java openkache-server -Xms2g -Xmx2g -Xlog:gc:file=/logs/gc.log:utctime,uptime -cp '/app/lib/*' \
    io.openkache.server.routing.B1ServerMain /app/config/b1.properties > /dev/null || exit 1
  for _ in \$(seq 1 60); do
    timeout 1 bash -c "</dev/tcp/\$ip/7000" 2> /dev/null && { echo "b1 node \$idx 시작(\$ip)"; exit 0; }
    sleep 2
  done
  docker logs --tail 20 openkache-b1 >&2
else
  docker restart openkache-server > /dev/null
  for _ in \$(seq 1 60); do
    curl -fsS http://127.0.0.1:9091/metrics > /dev/null 2>&1 && { echo "restarted(\$ip)"; exit 0; }
    sleep 2
  done
fi
echo "서버가 다시 뜨지 않았다(\$ip)" >&2
exit 1
EOF
)
echo "서버 재시작(동시, $server, 멤버 $nodes/$total_nodes대): $node_ids"
ssm_run "$node_ids" "$restart" 300 || { echo "서버 재시작 실패" >&2; exit 1; }

# 폴더 이름: <KST 날짜>/<KST 시각>_<저장소>_n<cache-server 수>_<variant>_<실행자>_<commit>. 컨테이너에 tzdata가 없어 +9시간으로 계산한다.
kst_date=$(date -u -d '+9 hours' +%Y-%m-%d)
kst_time=$(date -u -d '+9 hours' +%H%M%S)
safe() { printf '%s' "$1" | tr -c 'A-Za-z0-9.-' '-'; }
run_id="${kst_time}_${storage}_n${nodes}_$(safe "${variant:-none}")_$(safe "${GITLAB_USER_LOGIN:-local}")_${commit:0:8}"
dest="s3://$RESULTS_BUCKET/results/$study/$kst_date/$run_id/"

# 노드에서 실행할 스크립트. 결과 업로드는 실험이 실패해도 한다.
remote=$(cat <<EOF
set -uo pipefail
cd /opt/openkache/src
out=/opt/openkache/results/$run_id
mkdir -p "\$out"
sed -e "s|^target.host=.*|target.host=$target|" -e "s|^results.dir=.*|results.dir=\$out|" \
  -e "/^server.storage.note=/d" -e "/^target.hosts=/d" -e "/^deploy\./d" "$config" > "\$out/config.properties"
echo "server.storage.note=$storage_note" >> "\$out/config.properties"
echo "target.hosts=$target_hosts" >> "\$out/config.properties"
experiment-runner/build/install/openkache-experiment-runner/bin/openkache-experiment-runner \
  "\$out/config.properties" > "\$out/runner.log" 2>&1
rc=\$?
# 서버 내부 지표(저장소 읽기·쓰기, JVM 등)는 클러스터와 함께 사라지므로 실험 직후 노드마다 받아 둔다. B1 서버는 /metrics가 없다.
n=1
for ip in \$([ "$server" = b1 ] || echo "$node_ips"); do
  curl -fsS --max-time 10 "http://\$ip:9091/metrics" > "\$out/metrics-node-\$n.txt" \
    || echo "metrics 수집 실패: node \$n (\$ip)" >> "\$out/runner.log"
  n=\$((n + 1))
done
aws s3 cp --region $AWS_REGION --only-show-errors --recursive "\$out" "$dest" || rc=3
tail -n 20 "\$out/runner.log"
exit \$rc
EOF
)

echo "실험 시작: $config → $target_hosts (부하 생성 $loadgen)"
rc=0
ssm_run "$loadgen" "$remote" $(( exec_ms / 1000 + 600 )) || rc=1

# B1 서버 GC 로그는 실험 성공 여부와 관계없이 노드에서 결과 폴더로 올린다(NodeId 순서로 gc-node-<id>.log).
if [ "$server" = b1 ]; then
  gc_upload=$(cat <<EOF
members="$node_ips"
ip=\$(hostname -I | awk '{print \$1}')
i=0
for m in \$members; do
  [ "\$m" = "\$ip" ] && aws s3 cp --region $AWS_REGION --only-show-errors /opt/openkache/logs/gc.log "${dest}gc-node-\$i.log"
  i=\$((i + 1))
done
exit 0
EOF
)
  ssm_run "$node_ids" "$gc_upload" 120 > /dev/null || echo "GC 로그 업로드 일부 실패" >&2
fi

if [ "$rc" -ne 0 ]; then
  echo "실험 실패. 올라간 결과·로그: $dest" >&2
  exit 1
fi
echo "결과: $dest"
