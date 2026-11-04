#!/usr/bin/env bash
# 모든 노드(cache-server와 부하 생성)의 STATUS가 ready가 될 때까지 기다린다. 하나라도 failed이거나 시간이 지나면 실패한다.
# 사용법: infra/scripts/wait.sh [제한 초, 기본 900]
source "$(dirname "$0")/common.sh"

limit="${1:-900}"
ids=$(tf output -json instance_ids | tr -d '[]" ' | tr ',' ' ')
end=$((SECONDS + limit))

status_of() {
  local cmd
  cmd=$(aws ssm send-command --instance-ids "$1" --document-name AWS-RunShellScript \
    --parameters 'commands=["cat /opt/openkache/STATUS 2>/dev/null || echo none"]' \
    --query Command.CommandId --output text 2>/dev/null) || { echo offline; return; }
  aws ssm wait command-executed --command-id "$cmd" --instance-id "$1" 2>/dev/null || true
  aws ssm get-command-invocation --command-id "$cmd" --instance-id "$1" \
    --query StandardOutputContent --output text 2>/dev/null | tr -d '[:space:]'
}

while [ $SECONDS -lt $end ]; do
  pending=0
  for id in $ids; do
    s=$(status_of "$id")
    echo "$(date -u +%H:%M:%SZ) $id $s"
    case "$s" in
      ready) ;;
      failed) echo "$id 기동 실패. /var/log/cloud-init-output.log 확인" >&2; exit 1 ;;
      *) pending=1 ;;
    esac
  done
  [ $pending -eq 0 ] && { echo "모든 노드 ready"; exit 0; }
  sleep 30
done
echo "${limit}초 안에 ready가 되지 않았다" >&2
exit 1
