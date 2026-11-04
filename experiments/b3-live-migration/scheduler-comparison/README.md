# 선행 Scheduler 방법 비교

2026-11-04. 기존 [M1~M5](../movement-scaling-comparison/README.md)는 FIFO 하나의 조건 변화 실험이다. 이 폴더는 **선행 방법의 실행 순서·경합 회피·자원 배분을 같은 입력에서 비교**한다. 실제 Java Stop/Pre/Post-copy를 호출하며, 원 논문 시스템이나 MongoDB/TiDB 제품의 완전한 재현은 아니다. 공식 실행·핵심 수치는 [결과](results.md)를 따른다.

## 원 논문의 copy와 이번 실험의 관계

확인한 VM/컨테이너 다중 이동 논문 5편은 모두 **Pre-copy를 기본 모델**로 사용한다. 이번 연구는 그 핵심을 Tablet 이동 환경에 옮긴 Pre-copy 비교에 더해, 같은 Scheduler를 Stop-copy·Post-copy에도 연결한 **확장 교차 비교**를 수행했다. 원 논문이 세 방식을 모두 비교했다는 의미는 아니다.

| 원 논문·사례 | 원래 데이터 이동 방식 | 근거 |
|---|---|---|
| iter-GWIN·single-MCs | Pre-copy 컨테이너 이동 | [원문 §III·§IV-A](https://clouds.cis.unimelb.edu.au/papers/MigPlan-SDCEdge-TMC.pdf): CRIU pre-dump·incremental dump와 dirty 반복 전송 |
| SLAMIG | Pre-copy VM/VNF 이동 | [원문 §2.2](https://arxiv.org/html/2101.09716v1): Pre-copy를 기본 모델로 명시 |
| CAMIG | Pre-copy VM/컨테이너 이동 | [원문 §III-B](https://arxiv.org/html/2111.08942v1): dirty rate·반복 횟수에 기반한 Pre-copy 비용 모델 |
| FPTA | Pre-copy VM 이동 | [원문 §II-C·Fig.4](https://arxiv.org/html/1412.4980v1): 전체 메모리 복사 뒤 변경된 page를 반복 전송 |
| VMbuddies | Pre-copy와 관련 VM의 전환 동기화 | [원문 §3.1](https://www.comp.nus.edu.sg/~hebs/pub/VMbuddies-tpds.pdf): Pre-copy 뒤 wait-and-copy를 거쳐 함께 마지막 stop 단계에 진입 |
| MongoDB Balancer | 초기 문서 복사 → 변경분 동기화 → metadata 전환 | [공식 Range Migration 절차](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/#range-migration-procedure). 개념적으로 Pre-copy와 유사하며 VM 프로토콜과 동일한 구현은 아님 |
| TiDB PD/TiKV | replica의 Raft snapshot·로그 복제 | [공식 scheduling 설명](https://www.pingcap.com/blog/tidb-internal-scheduling/), [TiKV replication 설계](https://github.com/tikv/rfcs/blob/master/text/0093-rocksdb-per-region.md). PD의 속도 제어와 실제 replica 동기화를 구분 |

Pre-copy 내부의 마지막 **stop-and-copy 단계**는 이번 실험에서 독립 비교한 **처음부터 쓰기를 멈추는 Stop-copy 전체 프로토콜**과 다르다. FIFO는 자체 기준선이고 `tidb-feedback`은 PD v2에서 착안해 만든 AIMD 변형이다. Stop/Post-copy 결과는 원 논문 재현 결과가 아니라 이 프로젝트에서의 적용 가능성과 영향을 탐색한 결과로 읽는다.

## 비교 대상과 원문 대비 범위

| ID | 선행 방법·출처 | 옮긴 부분 | 원문과 다른 부분 |
|---|---|---|---|
| fifo | 기존 B3 기준선 | 실행 가능한 가장 오래된 job, 회전 균등 byte 할당 | 이번 비교의 자체 기준선 |
| mongodb | [MongoDB Balancer](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/) | 노드가 source/destination 어느 역할이든 동시 이동 1개, 독립 노드 쌍의 병렬 이동 | Balancer의 이동 대상·zone·threshold 선택은 제외. fixture cleanup 완료까지 slot 유지 |
| tidb-token | [TiDB PD Store Limit](https://docs.pingcap.com/tidb/stable/configure-store-limit/) | 노드별 token 소비·보충으로 이동 시작 속도 제한 | job당 1 token, 용량 4, 초기 1, 1/12 token/tick. 원문의 Region 크기별 비용·add/remove 별도 bucket 대신 공유 bucket |
| tidb-feedback | 같은 문서의 v2 아이디어 | pending 작업이 많으면 시작 속도 감소, 적으면 증가 | **PD v2 구현 재현이 아닌 AIMD 변형**. 4 tick마다 pending≥2면 rate×0.5, 그 외 +1/32, 범위 1/64~1/2 |
| iter-gwin | [Efficient Large-Scale Multiple Migration Planning and Scheduling, IEEE TMC 2024](https://clouds.cis.unimelb.edu.au/papers/MigPlan-SDCEdge-TMC.pdf), Algorithm 3 | 경합 graph에서 `W/(degree+1)`이 큰 정점을 고르고 이웃을 제외해 독립 그룹 반복 생성 | 경로 전체 대신 endpoint 공유를 경합으로 정의. 비긴급 W=1, deadline 조건은 원문 Eq.(7)의 slack 가중치 β=γ=1 |
| single-mcs | 같은 논문 Algorithm 4/5 | maximal clique를 한 번 열거하고, 남은 최대 clique를 순차 제거해 cover 구성. clique별로 가능한 고득점 이동을 골라 독립 그룹 생성 | 12 job graph에 Bron–Kerbosch pivoting 사용. 원문의 degeneracy 최적화는 생략하며 결과 집합은 작은 graph 완전 탐색 oracle로 검증 |
| slamig | [SLA-Aware Multiple Migration Planning and Scheduling, JSS 2021](https://arxiv.org/html/2101.09716v1) | 경합 clique 안에서 비용 정렬, 실행 가능한 그룹 생성·그룹 비용 정렬, 자원 해제 시 다음 가능한 job 시작 | 비용 `αT+β(T-D)+γI`의 α=β=1, **γ=0**. 네트워크 영향 항 비활성화, clique cover는 정확한 최대 clique 기반. 원문 전체 SLA 환경 재현 아님 |
| vmbuddies | [VMbuddies, TPDS 2015](https://www.comp.nus.edu.sg/~hebs/pub/VMbuddies-tpds.pdf), Eq.(3)~(5), (9)/(10) | dirty·남은 데이터 추정으로 공통 예상 완료 시간을 찾고 bandwidth 재배분 | 원문의 quasi-secant/Newton 대신 이분 탐색(32×28). 10 live round+final의 고정 예측 horizon, WWS 상한. 4 tick/active 변경마다 재배분. **관련 VM의 동시 cutover는 구현하지 않음** |
| fpta | [Virtual Machine Migration Planning in SDNs, INFOCOM 2015](https://arxiv.org/html/1412.4980v1), Algorithm 1 | dual edge length·phase별 flow 증분·최종 scale로 maximum multicommodity flow 근사 | ε=0.2, 경로는 전체 예산+source+destination의 단일 경로. 다중 경로 선택 제외, FIFO 4개 slot에 들어온 active job의 할당만 결정. float 오차에 대한 feasibility 정규화와 정수 floor 적용 |
| camig-selector | [CAMIG, TPDS, 저자 원문](https://arxiv.org/html/2111.08942v1), Eq.(17)~(21) | src–dst pair graph, maximal clique/MIS의 MIGC, 이미 고른 이동과의 간섭으로 후보 목적지 선택. 사라진 후보 pair를 제거하고 집합 갱신 | **별도 candidate-plan 비교**. 목표는 모든 나열된 Tablet 이동, 후보 목적지는 같은 목표 개선을 준다고 가정. single cost=초기 bytes/node 예산, κ=1. CPU 부하 재배치·원 논문 전체 목적함수 제외 |

논문 이름은 원문 출처를 가리킨다. 위 변경점이 있는 실행을 논문 구현체 자체의 성능이라고 해석하지 않는다. 특히 TiDB 피드백 변형과 CAMIG의 제한된 목적함수, VMbuddies의 비동기 Tablet 전환을 원 시스템과 구분한다.

## 두 가지 비교

**고정 계획:** 6가지 workload × 9개 방법 × 세 복사 방식 × seed 5개 = **810회**. `List<Change>`·초기 데이터·외부 요청 생성 규격은 같은 scenario/seed에 공통이다. physical 상한은 전체 동시 4, 노드 동시 4이며 MongoDB/GWIN/MCs/SLAMIG는 자신의 정책으로 노드당 1개를 선택한다. 완료·제어 단계도 slot을 차지한다.

**계획 선택:** 같은 12 Tablet과 같은 목적지 후보 집합에서 `candidate-first`·`candidate-balanced`·`camig-selector` × 세 복사 방식 × seed 5개 = **45회**. 세 계획 모두 동일한 MongoDB식 실행기로 실행한다. `candidate-first`는 첫 목적지, `candidate-balanced`는 현재까지 계획한 destination bytes가 가장 적은 후보를 고른다. CAMIG를 첫 후보만 고르는 약한 기준선과만 비교하지 않도록 balanced 기준도 둔다. `paired.csv`의 CAMIG 기준선은 candidate-balanced다. 이 비교에서는 목적지가 달라지며 고정 계획 Scheduler 순위에 합치지 않는다.

총 정식 **855회**, quick **342회 × 두 반복 = 684회**다.

| copy 방식 | 고정 계획 비교 | CAMIG 계획 선택 비교 | 정식 합계 |
|---|---:|---:|---:|
| Stop-copy | 270회 | 15회 | 285회 |
| Pre-copy | 270회 | 15회 | 285회 |
| Post-copy | 270회 | 15회 | 285회 |
| 전체 | 810회 | 45회 | 855회 |

## 입력과 관측

| scenario | 조건 |
|---|---|
| heterogeneous | 독립 endpoint, Tablet 크기 8/4/2/1 배 반복, rho=0.1 |
| conflict | 4 source·4 destination 사이 교차 경합. 첫 이동이 다음 두 이동을 막지만 그 둘끼리는 병렬 가능. 서로 다른 크기, rho=0.1 |
| shared-source | 모든 이동이 source 하나 공유, 크기 다름, rho=0.5 |
| shared-destination | 모든 이동이 destination 하나 공유, 크기 다름, rho=0.5 |
| dirty-skew | 경합 계획, job별 rho=1.2/0.05/0.5/0 반복 |
| deadline | 경합 계획, 작은 Tablet은 촉박한 기한, 다른 Tablet은 더 긴 기한. rho=0.1 |
| candidate-plan | 4 source, job별 공통 목적지와 대체 목적지 두 후보, 크기 다름, rho=0.5 |

full의 기본 key 수는 512(실제 Tablet당 512~4,096), node 예산은 4,096 bytes/tick, 전체는 16,384 bytes/tick이다. quick은 기본 64 key, node 1,024, 전체 4,096이다. batch 목표=node 예산, Pre-copy 임계=node 예산/4, 최대 live round=10. 각 job의 reference 예산은 node 예산으로 고정하며 실제 할당에 따라 요청 생성률을 낮추지 않는다.

key 16·value 128 bytes, record 152 bytes. 쓰기는 `rho×reference/152`회/tick, SET 70%·DELETE 30%, 읽기는 job당 2회/tick이다. seed·job·tick으로 시도를 결정하며 대기·완료 job에도 suite 종료까지 요청을 보낸다. 성공한 요청만 oracle에 반영한다. 완료 시각·BUSY·최종 데이터량이 서로 다르므로 같은 수의 성공 요청을 처리했다고 가정하지 않는다. 이전 M 실험의 생성기를 재사용하며 이 비교의 shape/기한/후보 집합을 inputHash에 추가했다. workloadHash는 trace 파일 전체가 아닌 생성 규격 hash다.

deadline은 작은 Tablet에 `10 + ceil(initialBytes/nodeBudget)×2`, 나머지에 `10 + ceil(initialBytes/nodeBudget)×5` tick이다. 나머지 scenario의 기한 10000은 비긴급 표시다. 입력은 모두 tick 0에 도착한다. 지속 도착·실제 장애·네트워크 경로·CPU/SSD 처리는 이번 비교 대상이 아니다.

VMbuddies 변형은 Pre-copy 초기 dirty를 사전 관측 쓰기율로 가정하고, 실행 중 성공 write bytes의 EWMA(α=0.125)로 갱신한다. 성공 DELETE의 no-op와 중복 key까지 포함하는 보수적 추정이며 실제 dirty bytes의 완벽한 예측이 아니다. 남은 snapshot은 초기 bytes−전송 bytes, 다음 dirty round는 직전 경과시간×추정률로 예측한다. Post-copy는 실제 remainingKeys를 사용한다. Stop/Post-copy에서는 dirty 재전송률을 0으로 둔다. 예측 오차가 포함된 결과를 보고하며 future trace나 정합성 oracle을 정책에 제공하지 않는다.

지표는 makespan·평균 job 완료 시간·시작 대기 p99·전송 bytes/초기 대비 비율·write 차단 tick/BUSY·read BUSY·기한 초과 job이다. 모든 시간은 논리 tick이다. 전송 1 byte는 전체 예산에 한 번, 양 endpoint에 각각 한 번 부과한다. protocol payload/header를 세고 ACK/control/TCP 비용은 제외한다. source와 같은 노드의 비이동 guard, owner/epoch, 이전 source MOVED, 최종 데이터·source 정리도 검증한다.

Planner의 실제 관측 nanos는 `timings.csv`에 분리한다. fixture 적재는 제외하지만 JIT warm-up·CPU 경합을 통제하지 않은 한 JVM의 참고값이며 논리 시간 결과와 합치지 않는다. 결정적 요약과 evidenceHash에서는 제외한다.

## 코드·검증·실행

production `MigrationScheduler.Policy`가 시작 후보와 예산을 제안하는 경계를 제공한다. Scheduler는 잘못된 job·중복 후보·전체/노드 예산 초과를 실제 driver 호출 전에 거절한다. 기존 constructor와 FIFO 동작은 유지한다. 연구 알고리즘은 test의 `SchedulerAlgorithms`·`SchedulerComparisonExperiment`에 두며 공통 api/model/spi/runtime·production 외부 의존성은 변경하지 않는다.

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/b3-live-migration/scheduler-comparison/scheduler.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/b3-live-migration/scheduler-comparison/scheduler-quick.properties
```

schema=`b3-scheduler-json-1`. 공통 Runner는 반복·프로세스 제한 시간·로그·실행 manifest를 소유하고 `SchedulerResearchMain`이 결과 의미를 검증한다. raw JSON에는 알고리즘 설정·선택한 계획·입력/계획/증거 hash·job별 정합성 hash·tick별 grant/실소비·phase/round를 보관한다. 설정과 source/backend/runner hash로 실행 코드를 식별한다.

원본을 가진 반복 폴더에서 다음 명령으로 실험 재실행 없이 재생성한다.

```powershell
java -Xmx1g -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.migration.experiment.SchedulerResearchMain --rebuild experiments/b3-live-migration/scheduler-comparison/runs/<run-id>/repeat-001
```

누락·중복 원본, 입력 불일치, 변경된 계획, budget/lifecycle/token 초과, 집계 불일치를 거절한다. `summary.csv`·`results.md`·`jobs.csv`·`budgets.csv`·`rounds.csv`·`paired.csv`는 결정적 파생 자료이고 `timings.csv`는 같은 JSON의 실제 시간 참고값이다. 전체 개수와 모든 조합을 검증한 뒤에만 completion을 쓴다. Git에는 full 반복의 summary.csv/results.md만 보관하고 JSON·manifest·상세 CSV는 실행 PC에 남긴다.
