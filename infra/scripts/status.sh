#!/usr/bin/env bash
# 프로젝트 인스턴스 대수·상태와, 실행 중인 노드의 기동 단계(STATUS)를 보여 준다.
source "$(dirname "$0")/common.sh"

aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=openkache" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId, Tags[?Key==`Name`]|[0].Value, State.Name, InstanceType, LaunchTime]' \
  --output table

running=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=openkache" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].InstanceId' --output text)
[ -z "$running" ] && exit 0

for id in $running; do
  ping=$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$id" \
    --query 'InstanceInformationList[0].PingStatus' --output text)
  if [ "$ping" != "Online" ]; then
    echo "$id: SSM $ping (부팅 직후면 1-2분 기다린다)"
    continue
  fi
  cmd=$(aws ssm send-command --instance-ids "$id" --document-name AWS-RunShellScript \
    --parameters 'commands=["cat /opt/openkache/STATUS 2>/dev/null || echo none","tail -n 3 /var/log/cloud-init-output.log"]' \
    --query Command.CommandId --output text)
  aws ssm wait command-executed --command-id "$cmd" --instance-id "$id" 2>/dev/null || true
  echo "== $id"
  aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
    --query StandardOutputContent --output text
done
