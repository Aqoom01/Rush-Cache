# scripts/*.sh가 source한다. 실행 사용자를 고정한다.
set -euo pipefail

# CI는 보호 변수로 받은 Access Key를 쓰고, 로컬은 openstd 프로필을 쓴다.
if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
  export AWS_PROFILE=openstd
fi
export AWS_REGION=ap-northeast-2
RESULTS_BUCKET=openstd-s15p31d111-results
REPO_ROOT=$(git rev-parse --show-toplevel)
TF_DIR="$REPO_ROOT/infra/experiment"

arn=$(aws sts get-caller-identity --query Arn --output text)
case "$arn" in
  *:user/OpenStd) ;;
  *) echo "OpenStd가 아닌 사용자로 실행 중이다: $arn" >&2; exit 1 ;;
esac

tf() { terraform -chdir="$TF_DIR" "$@"; }
