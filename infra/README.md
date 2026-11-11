# infra: AWS 실험 클러스터

실험할 때만 EC2를 만들고, 결과를 S3에 올린 뒤 지운다(apply → 실험 → destroy). Terraform으로 관리한다.

상태: `bootstrap/` 적용 완료. `experiment/`(노드 N대, 1시간 자동 정지)와 `scripts/up·down·status·wait.sh`로 m6gd.medium arm64에서 이미지 빌드·cache-server 기동까지 확인했다(2026-11-03, 생성부터 기동까지 약 3분). 부하 생성 노드·`run.sh`를 포함한 GitLab CI 실행(§10)은 2026-11-04 첫 실행(NODES=1, baseline config)에 성공했다.

## 1. 절대 규칙: m6gd.medium, cache-server 8대 + 부하 생성 1대

교보재 신청 조건이다(신청은 8대, 부하 생성용 1대 추가는 2026-11-04 연구팀 확인). 실행 중·정지 상태를 합쳐 프로젝트 인스턴스는 **9대를 넘으면 안 되고, 그중 cache-server는 8대 이하**다. 다른 인스턴스 타입은 쓰지 않는다.

인스턴스는 이 Terraform(CI)으로만 만든다. 아래 검사는 `Project=openkache` 태그가 있는 인스턴스만 세므로, 콘솔에서 태그 없이 만든 인스턴스는 대수에 잡히지 않는다. 테스트로 콘솔에서 띄워야 하면 CI를 돌리기 전에 `infra/scripts/status.sh`(태그 무관 전체 표시)로 계정 전체 대수를 확인한다.

AWS IAM에는 "N대까지"를 막는 조건이 없으므로 여러 겹으로 막는다.

| 단계 | 방법 | 막는 것 |
|---|---|---|
| plan | 변수 검증: `1 <= node_count <= 8`, `instance_type == "m6gd.medium"` | 설정 실수 |
| apply | precondition: 태그 `Project=openkache`인 기존 인스턴스(종료되지 않은 것) + 새로 만들 대수 <= 9 | 다른 state와 합쳐 넘는 경우 |
| apply | state 하나(S3, `use_lockfile`로 S3 자체 lock) | 두 사람이 동시에 apply |
| IAM | Terraform 사용자 정책: `m6gd.medium` 외 타입, `Project` 태그 없는 `RunInstances` 거부 | 다른 타입, 태그 없는 생성 |
| 감시 | 인스턴스 시작 이벤트에서 대수를 세어 8대 이상이면 SNS 메일 | 위를 모두 우회한 경우 |

## 2. 구성

```
infra/
  bootstrap/      # 최초 1회. destroy하지 않는다
                  #   state·결과 버킷, 인스턴스·Scheduler Role, 실험용 최소 권한 정책, Budgets 알림
  experiment/     # 실험마다 apply/destroy
                  #   VPC·서브넷·보안그룹, EC2(cache-server N + 부하 생성 1),
                  #   최대 수명 정지 스케줄
  scripts/        # up / wait / run / down / status
```

| 역할 | 대수 | 내용 |
|---|---|---|
| cache-server 노드 | 1-8 | Docker로 `deploy/docker/Dockerfile` 이미지 실행. 지금은 단일 노드 baseline만 의미가 있다(peer 프로토콜 미구현) |
| 부하 생성 노드 | 1 | `experiment-runner` 배포본만 빌드(Corretto 21). cache-server를 두지 않아 CPU를 나눠 쓰지 않는다 |

- `experiment-runner`는 `target.host` 하나로만 요청한다. `NODES`가 2 이상이어도 요청은 cache-server 1번 노드로만 간다(peer 프로토콜과 다중 노드 Runner가 생기기 전까지).

- GitLab Runner는 교보재 계정 밖의 SSAFY 제공 EC2에 둔다(§10). terraform 실행과 SSM 명령만 보내고 실험 요청은 보내지 않는다.
- `experiment-runner`는 부하 생성 노드에서 실행해 같은 서브넷의 cache-server로 요청한다. 측정 지연에 인터넷 구간이 들어가지 않고, 노드 포트를 서브넷 밖에 열지 않는다.

- 리전: `ap-northeast-2` (m6gd 제공 여부와 가격은 계정에서 확인 필요)
- 접속: SSH 대신 SSM Session Manager. 보안그룹은 외부 inbound를 열지 않고, 노드 포트(7000-7001, 9091)는 서브넷 안에서만 연다.

## 3. 실행 흐름

```
scripts/up.sh N         1. 로컬 HEAD를 git bundle로 묶어 결과 버킷에 업로드
                        2. terraform apply (cache-server N + 부하 생성 1)
                        3. user_data: bundle clone → commit 확인
                           node: 이미지 빌드(arm64) → cache-server 기동
                           loadgen: experiment-runner installDist
scripts/wait.sh         모든 노드 STATUS=ready 대기
scripts/run.sh CONFIG   서버 전부 동시 재시작(빈 저장소, config의 deploy.*에 따라 종류·멤버 결정) → SSM으로 부하 생성 노드에 지시:
                        target.hosts=멤버 노드, results.dir 치환(요청 분배는 config의 routing.boundaries)
                        → experiment-runner 실행 → 결과·config·로그를 S3에 업로드(실패해도)
scripts/down.sh         terraform destroy
```

- 소스는 GitLab이 아니라 bundle로 받는다. EC2에 GitLab 인증 정보를 두지 않으면서 `gitCommit`이 결과에 남는다.
- 이미지는 인스턴스에서 직접 빌드한다. ECR은 빌드 시간이 문제가 되면 검토한다.

## 4. 결과 저장 (S3)

- 경로: `s3://openstd-s15p31d111-results/results/<study>/<KST 날짜>/<KST 시각>_<저장소>_n<cache-server 수>_<variant>_<실행자>_<commit 8자리>/` 아래에 `results.dir` 전체, 실제 사용한 `config.properties`, `runner.log`, 실험 직후 각 cache-server의 `/metrics`(`metrics-node-<N>.txt`, 저장소 읽기·쓰기·JVM 지표)
  - 예: `results/baseline-single-node/2026-11-04/134752_memory_n1_uniform-70-20-10_hyunaeri_e9475041/`
  - 실행자는 CI면 GitLab 사용자명, 로컬이면 `local`이다. 2026-11-04 13:48 이전 결과는 `<UTC 시각>-<commit>` 형식으로 남아 있다.
- 받기: `aws s3 cp --recursive --profile openstd s3://openstd-s15p31d111-results/results/<study>/<실행>/ <로컬 경로>`
- config는 노드가 빌드한 commit에 들어 있어야 한다(커밋하지 않은 config는 `run.sh`가 거절). `execution.mode=tcp`만 실행한다.
- 버킷은 `bootstrap/`에 두어 실험 destroy와 분리하고 `prevent_destroy`, 버전 관리, 공개 접근 차단, SSE-S3를 켠다.
- EC2는 IAM Role로 결과 버킷에 쓰기만 한다. Access Key를 인스턴스에 두지 않는다.
- m6gd의 NVMe instance store는 정지하면 지워진다. 결과는 루트 EBS의 `results.dir`에 쓰고 S3에 올린다.
- 보고서용 최종 요약은 지금처럼 `experiments/` 아래 Git에 둔다. S3는 원본 보관용이다.

## 5. destroy를 잊었을 때

| 장치 | 동작 |
|---|---|
| 최대 수명 | apply 때 EventBridge Scheduler에 "생성 N분 뒤 StopInstances"를 만든다. 로컬 기본 60분, CI는 `execution.timeout.ms` + 30분 |
| 실행 전 확인 | `run.sh`는 자동 정지까지 남은 시간이 `execution.timeout.ms` + 5분보다 짧으면 실행하지 않는다 |
| 유휴 정지 | (미구현) CPU가 낮은 상태가 이어지면 CloudWatch 알람이 stop |

terminate가 아니라 stop이므로 다음 실행 전까지 루트 EBS는 남는다. 정지된 인스턴스도 9대에 포함된다. 이 state의 클러스터가 모두 정지돼 있으면 다음 `up.sh`가 destroy하고 진행한다(§10). 다른 곳에서 만든 정지 인스턴스는 지우지 않고 precondition에서 걸린다.

## 6. 월 예산: 27만 원 상한

예산은 1시간 자동 정지와 9대 상한으로 지킨다. NAT Gateway, VPC Endpoint처럼 켜 두기만 해도 시간 과금되는 자원은 만들지 않는다(퍼블릭 서브넷 + inbound 전부 차단 + SSM).

자동 정지로 막을 수 없는 사고(Access Key 유출, destroy 실패로 남은 자원, 콘솔에서 만든 자원)를 알아채기 위해 AWS Budgets 알림 하나만 둔다.

- 실제 사용 $90(27만 원의 약 절반, 1,500원/USD 환산)에 도달하면 메일
- 차단하지 않고 알리기만 한다. 계정당 예산 2개까지 무료다.

## 7. 진행 순서

1. `bootstrap/`: state·결과 버킷, IAM 정책, Budgets 알림
2. `experiment/`: 노드 1대로 arm64 이미지 빌드·실행 확인
3. `scripts/`, 결과 S3 업로드
4. 자동 정지 장치

## 8. 권한

- AWS에서 Terraform을 실행하는 IAM 사용자는 프로젝트용 **1개**뿐이다. 그 Access Key를 가진 인프라 담당자만 apply/destroy를 실행한다.
- 팀원은 키 없이 GitLab CI 파이프라인(§10)으로 실행한다. 같은 키를 CI 보호 변수에 둔다(로컬 키와 구분해 두 번째 Access Key를 쓴다).
- 루트 계정은 일상 작업에 쓰지 않는다. Access Key는 저장소·EC2에 두지 않는다.
- 로컬 실행은 named profile `openstd`만 쓴다(`scripts/common.sh`가 설정). 실행 사용자가 `OpenStd`가 아니면 스크립트와 Terraform(`identity.tf`)이 모두 멈춘다.

**bootstrap 절차 (최초 1회)**

1. 루트 콘솔에서 IAM 사용자에 `AdministratorAccess`를 잠깐 붙인다.
2. `infra/bootstrap/`에서 `terraform.tfvars.example`을 `terraform.tfvars`로 복사해 값을 넣고 `terraform init` → `plan` → `apply`.
3. 루트 콘솔에서 `AdministratorAccess`와 기존 EC2 정책을 뗀다. 이후 남는 권한은 bootstrap이 붙인 `openkache-experiment` 하나다.
4. bootstrap state(`terraform.tfstate`)는 로컬에 생기고 커밋하지 않는다. 잃어버리면 bootstrap 자원을 import로 되살려야 하므로 백업해 둔다.

`openkache-experiment`를 고쳐야 하면 1-3을 다시 한다(사용자는 자기 정책을 바꿀 권한이 없다).

## 9. 정해지지 않은 것

- 유휴 정지 알람
- 노드 준비 시간 단축(로컬 빌드 배포본 업로드) 적용 여부
- 다중 cache-server 대상 실험(experiment-runner가 여러 `target.host`를 다루는 방식)
- `execution.mode=research` 실험의 클러스터 실행 여부(지금은 cache-server가 필요 없어 로컬 실행)

## 10. GitLab CI로 실행

팀원은 GitLab 웹의 Build → Pipelines → Run pipeline에서 보호 브랜치(`develop`)를 고르고 실행한다. job `aws-experiment` 하나가 up → 기동 대기 → run → down을 한다.

| 입력 | 기본값 | 뜻 |
|---|---|---|
| `NODES` | 1 | cache-server 대수(1-8). 부하 생성 1대는 항상 추가된다 |
| `CONFIG` | `experiments/baseline-single-node/config.properties` | 실행할 TCP 실험 config. 고른 브랜치에 커밋돼 있어야 한다. 공백으로 여러 개를, 폴더를 넣으면 그 안의 `*.properties` 전부(이름순)를 같은 클러스터에서 차례로 실행하고, 자동 정지 시각은 제한 시간의 합 + 30분(최대 180분)이다 |
| `STORAGE` | `memory` | cache-server KV 저장소. `nvme`면 노드가 instance store를 포맷해 쓴다(`docs/storage-nvme.md`). 결과의 `storageLayer`에 기록된다 |

결과 위치는 job 로그 마지막의 `결과: s3://...` 줄에 나온다. 받는 방법은 §4.

**새 실험 config**

1. `experiments/baseline-single-node/config.properties`를 연구 폴더(예: `experiments/b2-load-balancing/<이름>/config.properties`)로 복사한다.
2. `study`·`variant`·`seed`, 부하 모양(`key.count`, `value.size`, `mix.*`, `distribution`=`UNIFORM`/`FIXED_HOT_KEY`, `hot.key.fraction`, `concurrency`, `*.requests`), `repeat.count`, `execution.timeout.ms`(최대 1시간)를 바꾼다. 항목 설명은 `RunnerConfig` Javadoc.
3. `target.host`·`results.dir`은 `run.sh`가 바꾸므로 로컬용 값을 그대로 둬도 된다. `execution.mode`는 없거나 `tcp`여야 한다.
4. 로컬에서 한 번 실행해 설정 오류를 확인한 뒤(`experiments/README.md`) 커밋·push하고, Run pipeline의 `CONFIG`에 경로를 넣는다.

cache-server는 단일 노드 인메모리 KV이므로 지금은 부하 모양만 바꿀 수 있다. 복제·장애·Tablet 같은 연구 알고리즘은 peer 프로토콜이 생기기 전까지 이 경로로 실험할 수 없다.

**서버 종류와 멤버 수 (`deploy.*`)**

config에 아래 key를 넣으면 `run.sh`가 실행 전 재시작 때 반영한다. Runner에는 넘기지 않는다. 노드 수가 다른 조건도 `NODES`를 가장 큰 값으로 띄운 클러스터 하나에서 config를 바꿔 가며 이어 실행할 수 있다.

| key | 기본값 | 뜻 |
|---|---|---|
| `deploy.server` | `kache` | `kache`는 기존 cache-server, `b1`은 `B1ServerMain`(B1 라우팅 서버, node 0이 Controller) |
| `deploy.members` | 띄운 노드 전부 | Terraform 노드 순서의 앞 N대만 쓴다. 나머지는 서버를 멈춘다. `target.hosts`와 결과 폴더의 `n<대수>`도 N대 기준 |
| `deploy.b1.tablets.per.node` | 2 | B1 초기 표의 노드당 Tablet 수 |

- `b1`은 서버 GC 로그(`-Xlog:gc`, UTC 시각)를 남기고 실험 뒤 결과 폴더에 `gc-node-<NodeId>.log`로 올린다. Runner `cluster-<id>.json`의 `measuredStartEpochMs`로 timeline과 맞춘다.
- `b1`은 노드마다 `node.id`(멤버 순서, 0부터)·`hosts`(멤버 사설 IP)를 담은 설정을 만들어 같은 이미지에서 host network로 띄운다. B1Server가 `hosts`의 자기 IP에 bind하기 때문이다.
- `b1`은 메모리 저장소(worker thread 하나가 소유하는 `NodeStore`)만 쓰고 `/metrics`가 없다. `STORAGE` 입력과 무관하게 결과의 저장소는 `memory`로 기록되고, 노드 `/metrics` 수집은 건너뛰고 window별 서버 CPU는 `NaN`으로 남는다(실험은 계속된다).
- Runner 기본 경로는 MOVED를 따라가지 않는다. 기본 경로로 B1을 쓰려면 B1 초기 표와 같은 `routing.boundaries`로 보내야 한다. 표를 따라가고 이동까지 지시하려면 아래 `cluster.mode=b1`을 쓴다.

**B1 클러스터 실행 (`cluster.mode=b1`, Runner)**

`deploy.server=b1`과 함께 쓴다. Runner가 node 0의 peer 포트에서 표를 받아(TABLE_DUMP) 주인에게 바로 보내고, MOVED면 redirect를 따라가며, 측정 중 정한 시각에 Tablet 이동(protocol-v1.md §6-5)을 지시한다. `target.hosts` 순서가 NodeId이고 `run.sh`가 이 순서로 채운다. `routing.boundaries`·`schedule.file`과 함께 쓸 수 없다.

| key | 기본값 | 뜻 |
|---|---|---|
| `cluster.mode` | 없음 | `b1`이면 아래 실행을 쓴다 |
| `load.rate` | 필수 | open-loop 초당 요청 수. 측정 구간 = `measured.requests ÷ load.rate`초(warm-up도 같은 비율). 지연은 예정 시각부터 잰다 |
| `cluster.peer.port` | 7001 | 노드 peer 포트 |
| `cluster.prefill` | true | 측정 전에 모든 key를 한 번 SET |
| `redirect.max` | 3 | MOVED를 따라갈 최대 횟수 |
| `retry.busy.max`, `retry.busy.interval.ms`, `retry.busy.max.interval.ms` | 1000, 1, 첫 간격과 같음 | BUSY(이동 중 freeze) 재시도 횟수와 간격. 간격은 재시도마다 두 배로 늘려 상한까지 간다(기본은 고정 간격) |
| `timeline.bucket.ms` | 100 | timeline 구간 폭 |
| `consistency.check` | true | 끝난 뒤 모든 key를 GET해 oracle과 대조 |
| `migration.at.ms`, `migration.tablet`, `migration.to` | 없음 | 쉼표 목록(개수 같게). 측정 시작 뒤 그 시각에 Tablet을 node로 옮기라고 그때의 주인에게 지시한다. 앞 이동이 끝난 뒤 다음을 보낸다 |
| `migration.method` | `pre-copy` | `stop-and-copy`, `pre-copy`, `cutover`(복사 없음 참고선) |
| `migration.rate.bytes.per.sec` | 0 | 이동 대역 상한(0 = 없음) |
| `migration.batch.bytes`, `migration.stop.threshold.bytes`, `migration.max.rounds` | 65536, 262144, 10 | batch 크기, pre-copy 종료 임계·라운드 상한 |

**균형 조정 (`balance.*`, B1+B2+B3 통합)**: `balance.split`을 주면 측정 중 window마다 Runner가 관찰한 key별 부하로 B2 정책을 부른다. 분할 시작 조건을 넘은 Tablet은 B2 Split 정책으로 나눠 Controller에 CONTROL SPLIT을 보내고, 나눈 뒤 B2 Placement 정책의 이동 계획을 `migration.method`(보통 pre-copy)로 하나씩 실행한다. 분할 시작 조건은 B2 design.md §4.1·Benchmark C 규칙(Tablet window 요청 > 노드 용량/4가 연속 `sustain` window, 분할 후 `cooldown` window 유예, 노드 용량 = window 요청 ÷ (노드 수 × 목표 이용률))이다. 이동 시각·대상을 정하는 `migration.at.ms` 등과 함께 쓸 수 있다.

| key | 기본값 | 뜻 |
|---|---|---|
| `balance.split` | 없음 | `heat`(split-for-heat), `midpoint`, `estore`, `none` |
| `balance.split.theta` | 0.8 | split-for-heat θ |
| `balance.placement` | `hysteresis` | `hysteresis`, `greedy`, `cost-aware`, `none` |
| `balance.window.ms`, `balance.start.ms` | 2000, 0 | 관찰 window 길이, 측정 시작 뒤 첫 window 시작. 이동이 끝날 때까지 그 window의 결정 단계가 이어지고, 그동안의 요청은 다음 window에 넣지 않는다 |
| `balance.sustain.windows`, `balance.cooldown.windows`, `balance.tablet.limit`, `balance.target.utilization` | 3, 3, 256, 0.7 | 분할 시작 조건 |

결과 폴더에는 기본 JSON·`summary.csv` 외에 `cluster-<id>.json`(hop별 지연, 송신 지연, BUSY·redirect 수, 이동 기록과 서버 보고, 정합성: lostSet·wrongValue·staleHit)과 `timeline-<id>.csv`(예정 시각 기준 구간별 요청·결과·p50/p99/max)가 생긴다. 이동 시각은 측정 시작 기준 ms라 timeline과 맞춰 볼 수 있다.

- open-loop은 thread `k mod concurrency`가 요청 k를 예정 시각에 보낸다. 모든 thread가 freeze로 막히면 실제 송신률이 떨어지므로 `sendLag`(예정 → 실제 송신)로 부하 생성기가 따라갔는지 확인한다. `concurrency`는 `load.rate × 예상 최대 지연`보다 크게 잡는다.
- 정합성 oracle을 위해 같은 key의 쓰기는 Runner 안에서 하나씩 보낸다(다른 key는 동시에). 응답을 못 받은 쓰기 뒤의 key는 검사에서 뺀다(`uncertainExcluded`).

**기존 실험을 지우지 않는 규칙**

| 상황 | 동작 |
|---|---|
| CI 실험 진행 중에 다른 CI 요청 | `resource_group: aws-experiment`로 대기하다가 앞 job이 끝나면 시작한다(대기 시간은 job timeout에 들어가지 않는다) |
| 로컬에서 띄운 클러스터가 있음 | `up.sh --wait 3600`이 1분마다 확인하며 최대 60분 기다린다. 그래도 남아 있으면 **지우지 않고** 실패한다 |
| job 실패·취소 | 이 job이 만든 클러스터만 `after_script`가 destroy한다(`.ci-owns-cluster` 표시, 실패 시 3번 재시도, 제한 15분). 남의 클러스터는 건드리지 않는다 |
| 모든 노드가 정지된 클러스터 | 자동 정지 후 destroy되지 않고 남은 것으로 보고 `up.sh`가 지운 뒤 진행한다. 하나라도 실행 중이면 지우지 않고 기다린다 |

- 로컬 실행은 디버깅용으로만 쓰고 끝나면 `down.sh`를 한다. 잊어도 자동 정지 이후의 다음 CI 실행이 정리한다.
- 정지된 노드의 루트 EBS도 함께 지워진다. 결과는 S3에 올린 것만 남는다.
- 대기 순서를 요청 순서로 하려면 resource group의 process mode를 `oldest_first`로 바꾼다(기본 `unordered`). GitLab API로만 바꿀 수 있다: `PUT /projects/:id/resource_groups/aws-experiment` (`process_mode=oldest_first`). resource group은 첫 실행 때 생긴다.

**Runner (SSAFY 제공 t3.xlarge, Ubuntu 24.04)**

- 교보재 계정 밖의 서버라 9대·예산 제한과 무관하다. 다른 서비스는 두지 않는다.
- UFW 필수(SSAFY 안내). Runner는 GitLab에 outbound로만 붙으므로 새 inbound 포트를 열지 않는다(22·443·8989만 유지).
- Docker로 포트를 공개(`-p`)하면 UFW를 우회해 외부에 열린다. 이 서버에서는 컨테이너 포트를 공개하지 않는다.
- Docker executor, 태그 `openstd-aws`, **Protected**(보호 브랜치 job만), Run untagged jobs 끔.
- AWS 키는 서버에 두지 않는다. CI 변수 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`(Protected·Masked), `TF_VAR_account_id`(Protected)로만 넘긴다.
