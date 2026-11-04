#!/usr/bin/env bash
# 사용법:
#   infra/scripts/up.sh [--wait 초] [노드 수]   현재 HEAD를 bundle로 올리고 plan을 만든다(기본 1대)
#   infra/scripts/up.sh --apply                 만든 plan을 그대로 적용한다
#
# 이미 클러스터가 있으면 덮어쓰지 않는다. --wait가 있으면 비워질 때까지 기다리고, 없으면 바로 실패한다.
# 단, 모든 노드가 정지된(방치된) 클러스터는 지우고 진행한다.
source "$(dirname "$0")/common.sh"

if [ "${1:-}" = "--apply" ]; then
  tf apply -input=false up.tfplan
  rm -f "$TF_DIR/up.tfplan"
  exit 0
fi

wait_limit=0
if [ "${1:-}" = "--wait" ]; then
  wait_limit="$2"
  shift 2
fi
nodes="${1:-1}"
tf init -input=false > /dev/null

cluster_exists() { tf state list 2>/dev/null | grep -q '^aws_instance\.'; }

# 모든 노드가 stopped면 자동 정지로 끝나 방치된 클러스터다. 정지된 노드에서는 실험이 돌 수 없다.
cluster_abandoned() {
  local ids states s
  ids=$(tf output -json instance_ids 2>/dev/null | tr -d '[]" ' | tr ',' ' ')
  [ -n "$ids" ] || return 1
  # shellcheck disable=SC2086
  states=$(aws ec2 describe-instances --instance-ids $ids \
    --query 'Reservations[].Instances[].State.Name' --output text 2>/dev/null) || return 1
  [ -n "$states" ] || return 1
  for s in $states; do
    case "$s" in stopped | terminated) ;; *) return 1 ;; esac
  done
}

end=$((SECONDS + wait_limit))
while cluster_exists; do
  if cluster_abandoned; then
    echo "모든 노드가 정지된 클러스터를 정리한다(자동 정지 후 destroy되지 않은 것)"
    { "$(dirname "$0")/down.sh" && "$(dirname "$0")/down.sh" --apply; } \
      || { echo "방치된 클러스터 정리에 실패했다" >&2; exit 1; }
    continue
  fi
  if [ $SECONDS -ge $end ]; then
    echo "이미 실행 중인 클러스터가 있다(진행 중인 실험을 지우지 않는다). 끝난 뒤 다시 실행한다." >&2
    exit 1
  fi
  echo "$(date -u +%H:%M:%SZ) 기존 클러스터가 끝나기를 기다린다"
  sleep 60
done

commit=$(git -C "$REPO_ROOT" rev-parse HEAD)
if [ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]; then
  echo "주의: 커밋되지 않은 변경은 노드에 반영되지 않는다(빌드 대상 $commit)." >&2
fi

bundle=$(mktemp --suffix=.bundle)
trap 'rm -f "$bundle"' EXIT
git -C "$REPO_ROOT" bundle create "$bundle" HEAD
aws s3 cp --only-show-errors "$bundle" "s3://$RESULTS_BUCKET/bundles/$commit.bundle"

tf plan -input=false -var "node_count=$nodes" -var "source_commit=$commit" -out=up.tfplan
echo "확인 후 적용: infra/scripts/up.sh --apply"
