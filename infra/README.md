# infra: AWS 실험 클러스터

실험할 때만 EC2를 만들고, 결과를 S3에 올린 뒤 지운다(apply → 실험 → destroy). Terraform으로 관리한다.

상태: `bootstrap/` 코드 작성(apply 전). `experiment/`, `scripts/`는 아직 없다.

## 1. 절대 규칙: m6gd.medium, 최대 8대

교보재 신청 조건이다. 실행 중·정지 상태를 합쳐 프로젝트 인스턴스는 **8대를 넘으면 안 된다.** 다른 인스턴스 타입은 쓰지 않는다.

AWS IAM에는 "N대까지"를 막는 조건이 없으므로 여러 겹으로 막는다.

| 단계 | 방법 | 막는 것 |
|---|---|---|
| plan | 변수 검증: `node_count + 1(runner) <= 8`, `instance_type == "m6gd.medium"` | 설정 실수 |
| apply | precondition: 태그 `Project=openkache`인 기존 인스턴스(종료되지 않은 것) + 새로 만들 대수 <= 8 | 다른 state·수동 생성분과 합쳐 넘는 경우 |
| apply | state 하나(S3, `use_lockfile`로 S3 자체 lock) | 두 사람이 동시에 apply |
| IAM | Terraform 사용자 정책: `m6gd.medium` 외 타입, `Project` 태그 없는 `RunInstances` 거부 | 다른 타입, 태그 없는 생성 |
| 감시 | 인스턴스 시작 이벤트에서 대수를 세어 7대 이상이면 SNS 메일 | 위를 모두 우회한 경우 |

## 2. 구성

```
infra/
  bootstrap/      # 최초 1회. destroy하지 않는다
                  #   state·결과 버킷, 인스턴스·Scheduler Role, 실험용 최소 권한 정책, Budgets 알림
  experiment/     # 실험마다 apply/destroy
                  #   VPC·서브넷·보안그룹, EC2(노드 N + runner 1), instance profile,
                  #   최대 수명 정지 스케줄, 유휴 정지 알람
  scripts/        # up / run / down / status
```

| 역할 | 대수 | 내용 |
|---|---|---|
| cache-server 노드 | 1-6 | Docker로 `deploy/docker/Dockerfile` 이미지 실행. 지금은 단일 노드 baseline만 의미가 있다(peer 프로토콜 미구현) |
| runner | 1 | `experiment-runner`, Prometheus/Grafana. 노드와 같은 서브넷에 두어 네트워크 잡음을 줄인다 |

- 리전: `ap-northeast-2` (m6gd 제공 여부와 가격은 계정에서 확인 필요)
- 접속: SSH 대신 SSM Session Manager. 보안그룹은 외부 inbound를 열지 않고, 노드 포트(7000-7001, 9091)는 서브넷 안에서만 연다.

## 3. 실행 흐름

```
scripts/up.sh N         1. 로컬 HEAD를 git bundle로 묶어 결과 버킷에 업로드
                        2. terraform apply (노드 N + runner 1)
                        3. user_data: bundle clone → 이미지 빌드(arm64) → cache-server 기동
scripts/run.sh CONFIG   runner에서 experiment-runner 실행 → results.dir을 S3에 업로드
scripts/down.sh         terraform destroy
```

- 소스는 GitLab이 아니라 bundle로 받는다. EC2에 GitLab 인증 정보를 두지 않으면서 `gitCommit`이 결과에 남는다.
- 이미지는 인스턴스에서 직접 빌드한다. ECR은 빌드 시간이 문제가 되면 검토한다.

## 4. 결과 저장 (S3)

- 경로: `s3://<결과 버킷>/<study>/<UTC 시각>-<commit 앞 8자리>/` 아래에 `results.dir` 전체
- 버킷은 `bootstrap/`에 두어 실험 destroy와 분리하고 `prevent_destroy`, 버전 관리, 공개 접근 차단, SSE-S3를 켠다.
- EC2는 IAM Role로 결과 버킷에 쓰기만 한다. Access Key를 인스턴스에 두지 않는다.
- m6gd의 NVMe instance store는 정지하면 지워진다. 결과는 루트 EBS의 `results.dir`에 쓰고 S3에 올린다.
- 보고서용 최종 요약은 지금처럼 `experiments/` 아래 Git에 둔다. S3는 원본 보관용이다.

## 5. destroy를 잊었을 때

| 장치 | 동작 |
|---|---|
| 최대 수명 | apply 때 EventBridge Scheduler에 "생성 1시간 뒤 StopInstances"를 만든다. 기본 1시간, 변수로 조정 |
| 실행 전 확인 | `run.sh`는 남은 수명이 실험 설정의 `execution.timeout.ms`보다 짧으면 실행하지 않는다 |
| 유휴 정지 | CPU가 낮은 상태가 이어지면 CloudWatch 알람이 stop |

terminate가 아니라 stop이므로 루트 EBS의 결과는 남는다. 정지된 인스턴스도 8대에 포함되므로 다음 apply의 precondition에서 걸린다.

## 6. 월 예산: 27만 원 상한

예산은 1시간 자동 정지와 8대 상한으로 지킨다. NAT Gateway, VPC Endpoint처럼 켜 두기만 해도 시간 과금되는 자원은 만들지 않는다(퍼블릭 서브넷 + inbound 전부 차단 + SSM).

자동 정지로 막을 수 없는 사고(Access Key 유출, destroy 실패로 남은 자원, 콘솔에서 만든 자원)를 알아채기 위해 AWS Budgets 알림 하나만 둔다.

- 실제 사용 $90(27만 원의 약 절반, 1,500원/USD 환산)에 도달하면 메일
- 차단하지 않고 알리기만 한다. 계정당 예산 2개까지 무료다.

## 7. 진행 순서

1. `bootstrap/`: state·결과 버킷, IAM 정책, Budgets 알림
2. `experiment/`: 노드 1 + runner 1로 arm64 이미지 빌드·실행 확인 (`linux/arm64`는 아직 확인한 적 없다)
3. `scripts/`, 결과 S3 업로드
4. 자동 정지 장치

## 8. 권한

- AWS에서 Terraform을 실행하는 IAM 사용자는 프로젝트용 **1개**뿐이다. 그 Access Key를 가진 인프라 담당자만 apply/destroy를 실행한다.
- 다른 연구자는 실험 설정(`config.properties`)을 담당자에게 넘기고, 담당자가 up → run → down을 실행한 뒤 S3 결과 경로를 알려 준다.
- 루트 계정은 일상 작업에 쓰지 않는다. Access Key는 저장소·EC2에 두지 않는다.
- AWS CLI·Terraform은 named profile `openstd`만 쓴다. `default` 프로필은 다른 계정이므로 provider의 `allowed_account_ids`로 막는다.

**bootstrap 절차 (최초 1회)**

1. 루트 콘솔에서 IAM 사용자에 `AdministratorAccess`를 잠깐 붙인다.
2. `infra/bootstrap/`에서 `terraform.tfvars.example`을 `terraform.tfvars`로 복사해 값을 넣고 `terraform init` → `plan` → `apply`.
3. 루트 콘솔에서 `AdministratorAccess`와 기존 EC2 정책을 뗀다. 이후 남는 권한은 bootstrap이 붙인 `openkache-experiment` 하나다.
4. bootstrap state(`terraform.tfstate`)는 로컬에 생기고 커밋하지 않는다. 잃어버리면 bootstrap 자원을 import로 되살려야 하므로 백업해 둔다.

`openkache-experiment`를 고쳐야 하면 1-3을 다시 한다(사용자는 자기 정책을 바꿀 권한이 없다).

## 9. 정해지지 않은 것

- m6gd.medium 서울 제공 여부·단가 확인
- Budgets 알림 받을 이메일
