#!/usr/bin/env bash
# 사용법:
#   infra/scripts/down.sh           destroy plan을 만든다
#   infra/scripts/down.sh --apply   만든 plan을 그대로 적용한다
source "$(dirname "$0")/common.sh"

if [ "${1:-}" = "--apply" ]; then
  tf apply -input=false down.tfplan
  rm -f "$TF_DIR/down.tfplan"
  exit 0
fi

# destroy에는 source_commit 값이 쓰이지 않지만 변수 검증을 통과해야 한다.
tf init -input=false > /dev/null
tf plan -destroy -input=false -var "source_commit=$(printf '0%.0s' {1..40})" -out=down.tfplan
echo "확인 후 적용: infra/scripts/down.sh --apply"
