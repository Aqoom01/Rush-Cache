#!/usr/bin/env bash
# 부하 생성 노드에서 experiment-runner를 실행하고 결과를 S3에 올린다. 요청 대상은 cache-server 1번 노드다.
# 사용법: infra/scripts/run.sh <실험 config.properties, 저장소 루트 기준 경로>
# config는 노드가 빌드한 commit에 들어 있어야 한다. target.host와 results.dir은 이 스크립트가 바꾼다.
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
exec_ms=$(prop execution.timeout.ms)
exec_ms=${exec_ms:-3600000}

loadgen=$(tf output -raw loadgen_instance_id)
target=$(tf output -json node_private_ips | tr -d '[]" ' | cut -d, -f1)

# 자동 정지 전에 끝나지 못할 실험은 시작하지 않는다(5분 여유).
remaining=$(( $(date -d "$(tf output -raw stops_at_utc)" +%s) - $(date +%s) ))
if [ "$remaining" -lt $(( exec_ms / 1000 + 300 )) ]; then
  echo "자동 정지까지 ${remaining}초 남아 execution.timeout.ms=${exec_ms}를 채울 수 없다." >&2
  echo "max_lifetime_minutes를 늘려 다시 띄운다." >&2
  exit 1
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)-${commit:0:8}"
dest="s3://$RESULTS_BUCKET/results/$study/$run_id/"

# 노드에서 실행할 스크립트. 결과 업로드는 실험이 실패해도 한다.
remote=$(cat <<EOF
set -uo pipefail
cd /opt/openkache/src
out=/opt/openkache/results/$run_id
mkdir -p "\$out"
sed -e "s|^target.host=.*|target.host=$target|" -e "s|^results.dir=.*|results.dir=\$out|" \
  "$config" > "\$out/config.properties"
experiment-runner/build/install/openkache-experiment-runner/bin/openkache-experiment-runner \
  "\$out/config.properties" > "\$out/runner.log" 2>&1
rc=\$?
aws s3 cp --region $AWS_REGION --only-show-errors --recursive "\$out" "$dest" || rc=3
tail -n 20 "\$out/runner.log"
exit \$rc
EOF
)

echo "실험 시작: $config → $target:7000 (부하 생성 $loadgen)"
cmd=$(aws ssm send-command --instance-ids "$loadgen" --document-name AWS-RunShellScript \
  --parameters "{\"commands\":[\"echo $(printf '%s' "$remote" | base64 -w0) | base64 -d | bash\"],\"executionTimeout\":[\"$(( exec_ms / 1000 + 600 ))\"]}" \
  --query Command.CommandId --output text)

while :; do
  status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$loadgen" \
    --query Status --output text 2>/dev/null || echo Pending)
  case "$status" in
    Pending | InProgress | Delayed) sleep 15 ;;
    *) break ;;
  esac
done

aws ssm get-command-invocation --command-id "$cmd" --instance-id "$loadgen" \
  --query '[StandardOutputContent, StandardErrorContent]' --output text
if [ "$status" != Success ]; then
  echo "실험 실패($status). 올라간 결과·로그: $dest" >&2
  exit 1
fi
echo "결과: $dest"
