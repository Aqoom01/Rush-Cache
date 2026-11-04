# B3 Live Migration + Elastic Scaling

B3는 cluster metric을 보고 **노드를 언제 몇 개 늘리거나 줄일지 결정**하고, B1·B2가 만든 이동 계획(`Change`)을 받아 Tablet 데이터를 실제로 옮기고 소유권을 넘긴다. 비교 대상은 **노드 증감 판단 정책**, **데이터를 옮기는 프로토콜**, **이동 job이 많아질 때의 확장 특성**이다.

**현재 구현(2026-11-02):** stop-and-copy·pre-copy·post-copy의 in-process 프로토콜과 정합성 테스트가 있다. lifecycle·adapter 계약·실패 처리는 [구현 안내](protocols.md)를 따른다. S1~S5·S3 민감도·S6 비교에 이어 다중 이동 Scheduler와 M1~M5 backend를 구현하고 정식 210회·축소 반복 168회를 완료했다. [다중 이동 결과](movement-scaling-comparison/results.md)를 따른다. 이전 자체 노드 증감 비교는 제거했고 선행 메커니즘 8개의 연구 prototype과 가상 비교를 구현했다. 실제 서버·원 논문 제품 재현과 구분하며 [범위와 결과](scaling-decision/README.md)를 따른다. 실제 KV/peer 통합과 노드 증감 controller의 이동 연결은 후속이다.

2026-11-04 후속으로 [선행 Scheduler 비교](scheduler-comparison/results.md)도 완료했다. 고정 계획의 9개 정책과 CAMIG의 별도 목적지 선택을 정식 855회·축소 두 반복 684회로 검증했다. 원 논문에서 옮긴 부분과 프로젝트에 맞춘 변형을 [방법별 범위](scheduler-comparison/README.md)에 명시했다.

- 노드 증감 판단: 선행 시스템의 실제 알고리즘·전제·평가 방법을 조사하고 재현 범위를 선정
- 단일 이동: downtime, 총 이동 시간, 전송량 증폭, 정합성
- 이동 scaling: job 수·동시 실행 수·공유 대역폭에 따른 전체 완료 시간, aggregate throughput, 요청 영향

B3는 desired node count와 scale-in 후보를 정한다. 실제 노드 생성·종료는 외부 provisioner·membership, Tablet별 source·destination 계획은 B1, load-aware placement·rebalance는 B2가 담당한다. Tablet split·merge와 일반 운영 자동화는 다루지 않는다.

## 문서

| 파일 | 내용 |
|---|---|
| [design.md](design.md) | B3 전체 설계: 범위, 경계, 공통 모델·인터페이스, 불변식, 지표, 실험 계획 |
| [protocols.md](protocols.md) | 구현된 pre-copy·post-copy 계약, commit·요청·실패 처리, 테스트 실행 |
| [references/](references/) | 참고 논문·시스템별 정리. 읽은 내용과 프로젝트 적용 방식을 분리해 적는다 |
| [migration-comparison/README.md](migration-comparison/README.md) | 단일 이동: SC1~SC6 Baseline·S1~S5 비교·S3 민감도·S6 취소/장애 비교 실행 |
| [scaling-decision/README.md](scaling-decision/README.md) | 선행 메커니즘 8개 가상 모형 실행·결과·재현 범위 |
| [movement-scaling-comparison/README.md](movement-scaling-comparison/README.md) | M1~M5 다중 이동 Scheduler·실행 조건·정식 결과 |
| [scheduler-comparison/README.md](scheduler-comparison/README.md) | 선행 Scheduler 9개 정책 비교, CAMIG 계획 선택 별도 평가·원문 대비 변경 범위 |

## 참고자료와 설계 결정

| 참고자료 | 이 문서에서 가져오는 것 | 가져오지 않는 것 |
|---|---|---|
| [선행 노드 증감 알고리즘](references/elastic-scaling-algorithms.md) | SCADS·ElastMan·E-Store·Accordion·P-Store·TIRAMOLA·PAX·Slicer의 실제 절차와 전제 | 원문과 다른 자체 정책을 논문 재현으로 취급하는 것 |
| [Elastic Scaling Policy](references/elastic-scaling-policy.md) | desired node count 비율식, target tolerance, scale-in stabilization, 새 노드 warm-up, scale-out/in 비대칭 | Kubernetes·AWS 제어면과 서비스별 고정 threshold |
| [VM Live Migration](references/vm-live-migration.md) | `stop-and-copy`·`pre-copy`, dirty 반복 전송, 수렴 조건, downtime과 총 이동 시간의 분리 | VM의 CPU·장치 상태와 네트워크 주소 이전 |
| [MongoDB Chunk Migration](references/mongodb-chunk-migration.md) | source가 이동 중 요청 처리, 복사 → 변경분 동기화 → metadata commit → 정리, 참여 노드별 동시 이동 제한 | chunk 분할 조건과 balancer의 재배치 판단 |
| [Redis Cluster Resharding](references/redis-cluster-resharding.md) | 이동 완료 전후의 요청 처리와 `MOVED`, 데이터 이동과 소유권 확정의 분리 | hash slot 설계, B1 결정과 맞지 않는 `ASK` 경로 |
| [Rocksteady](references/rocksteady.md) | 소유권을 먼저 넘기는 `post-copy`, background/priority pull, aggregate throughput과 요청 지연의 동시 측정 | RAMCloud lineage·side log 복구와 client 주도 split |

논문의 실측값은 해당 시스템과 환경의 결과다. OpenKache의 성능 목표가 아니라 실험 지표와 비교 질문을 고르는 근거로만 사용한다.

## 복사 방식

각 variant는 표에 명시한 선행 연구나 제품 사례에 근거한다. 실험 결과의 `variant` 값으로 이 이름을 쓴다.

| variant | 요청 처리 | 데이터 복사 | 소유권 전환 | 근거 | 상태 |
|---|---|---|---|---|---|
| `stop-and-copy` | source가 끝까지 처리 | write를 멈추고 전체를 한 번 복사 | 복사·검증 뒤 | Baseline: [VM Live Migration](references/vm-live-migration.md) (NSDI 2005) §3.1의 pure stop-and-copy | **1단계 in-process 구현** |
| `pre-copy` | source가 끝까지 처리 | 서비스 중 snapshot → 바뀐 key만 반복 전송 → 짧게 멈추고 남은 변경분 전송 | 따라잡기·검증 뒤 | [VM Live Migration](references/vm-live-migration.md) (NSDI 2005) §3.3·§5.3. 같은 계열의 KV 사례 [MongoDB Chunk Migration](references/mongodb-chunk-migration.md) | **in-process 구현·테스트** |
| `post-copy` | destination이 처음부터 처리. write는 바로, 아직 안 온 key의 read는 pull·재시도 필요 | 요청이 기다리는 key를 먼저 pull, 나머지는 background pull | 시작할 때 | [Rocksteady](references/rocksteady.md) (SOSP 2017) §3·§3.3 | **in-process 구현·별도 계약 테스트** |

- `stop-and-copy`는 전체 복사 전에 write를 멈추는 baseline이다. 전송량은 작지만 downtime이 Tablet 크기에 비례한다.
- `pre-copy`는 서비스 중 snapshot과 dirty key를 반복 전송하고 마지막 변경분만 멈춘 상태에서 보낸다. 자주 바뀌는 key를 재전송하므로 전송량이 늘고, dirty 발생 속도가 전송 속도보다 빠르면 수렴하지 않는다.
- `post-copy`는 bulk-copy를 위한 freeze 없이 소유권부터 destination으로 넘긴다. 대신 아직 도착하지 않은 key의 첫 read가 pull·재시도를 겪고, 이동 완료 전 source 장애 처리 규칙이 필요하다. Rocksteady의 무중단·성능 수치는 해당 논문의 결과이며 OpenKache에서 별도로 측정한다.

## Elastic Scaling

### 노드 증감 판단

현재 선행 메커니즘 8개의 연구 prototype·실험 backend와 가상 모형 결과가 있다. 실제 OpenKache 노드 증감 controller는 없다. [선행 시스템별 알고리즘](references/elastic-scaling-algorithms.md)에 실제 입력·결정 절차·평가 환경을 정리했다. [전체 조사](references/tablet-aware-elasticity-survey.md)는 관련 논문·운영 사례와 지표 근거를 제공한다.

SCADS의 성능 모델·best-fit, ElastMan의 feedforward/feedback, E-Store의 hot/cold 배치, Accordion의 서버 수 후보별 MILP, P-Store의 예측·동적 계획법, TIRAMOLA의 MDP, PAX의 partition-aware 노드 활성화가 조사 대상이다. Slicer는 split/merge·배치 참고이며 서버 수 결정은 외부 담당이다.

연구 prototype에서 생략·변경한 원문 기능을 기록했으며 B1/B2 배치·실제 migration 실행과의 통합은 후속이다. 특정 비율식이나 자체 비용식을 비교 알고리즘으로 확정하지 않는다. 실제 노드 생성·종료는 외부 provisioner·membership의 책임이며 축소는 drain 완료 후 수행한다.

### 여러 job의 이동 Scaling

노드 증감 결정 뒤 B1·B2가 여러 `Change`를 만들면 B3는 이를 고정된 입력으로 받아 실행한다. 이 단계에서는 계획의 품질이 아니라 job 묶음을 실제로 옮기는 능력을 측정한다.

변수:

- migration job 수
- 동시에 실행할 job 수
- node별·cluster 전체 전송 예산
- source 또는 destination을 공유하는 job 비율
- 이동 중 write 비율과 hot-key 쏠림

측정값:

- 모든 job이 끝날 때까지 걸린 시간(makespan)
- aggregate migration throughput과 전송량 증폭
- job별 대기 시간·downtime과 완료율
- 이동 중 요청 지연·거절·재시도
- 특정 source·destination에 job이 몰렸을 때의 공정성과 starvation

MongoDB의 참여 shard당 동시 이동 1개는 첫 baseline 후보로 쓴다. Rocksteady의 aggregate throughput과 tail latency 측정 방식은 지표 선택에 사용한다. Redis의 `ASK` 방식은 비교 자료로만 남긴다. 1단계 `stop-and-copy`는 B1 계약에 따라 source가 cutover 전까지 처리하고 완료 후 `MOVED`만 반환한다. `pre-copy`는 이 계약을 재사용하는 후속 비교 구현이다.

## 코드 위치

알고리즘 코드는 `cache-core`의 `io.openkache.core.migration`에 둔다. 이 디렉터리에는 설계, 참고 문헌 정리, 실험 설정과 결과 요약만 둔다. 상세는 [design.md](design.md) §7을 따른다.

## 실험 진입점과 결과 형식

현재 Baseline 실험의 정식 진입점은 config를 읽는 공통 `experiment-runner`다. 공통 Runner는 실험 선택·반복·실제 시간 제한·결과 수집을, B3 backend는 tick 진행·연구별 관측·정합성 검사를 담당한다. **현재 `execution.mode=research`의 config 지정 JVM backend 실행이 구현됐다.** `backend.main`·classpath·인자가 실행 대상을 선택하고 `study`·`experiment`는 연구·실험 식별자다. [실행 안내](migration-comparison/README.md)와 [전체 설정](migration-comparison/baseline.properties)을 따른다.

새 Baseline 실행은 JSON 원본·CSV 요약·Markdown 해석을 실제 생성한다(`b3-baseline-json-1`). 2026-10-22 전체 설정 실행의 [보고서](migration-comparison/runs/run-2026-10-22T04-48-53.944252700Z-a4354ce4/repeat-001/results.md)와 [조건별 요약](migration-comparison/runs/run-2026-10-22T04-48-53.944252700Z-a4354ce4/repeat-001/summary.csv)을 Git에 보관한다. JSON 원본과 manifest·로그는 실행한 PC에 남긴다. S1~S5는 b3-comparison-json-1, S6는 b3-s6-json-1, 선행 메커니즘은 b3-literature-algorithms-json-1, M1~M5는 b3-movement-json-1 backend로 실행한다. v2 dirty 상세 형식과 실제 노드 증감 통합은 후속이다. 상세 호출·결과 계약은 [design.md §6.6](design.md)에 있다.

## Baseline 실행과 결과

단위·계약 테스트는 `./gradlew build`에 포함된다. SC1~SC6 전체 실행은 공통 Runner에 Baseline config를 전달한다.
[migration-comparison/README.md](migration-comparison/README.md)에 재현 명령·모델·지표 정의를 기록한다.
실행하면 `migration-comparison/runs/<execution_id>/`에 실행별 결과가 생성된다. 현재 보관된 Baseline 실행은 26개 조건 × seed 5개, 총 130회이며 정합성 규칙 위반은 0건이다. S1~S5 세 방식 비교는 2026-10-23 generator v2 full 240회를 참조 run으로 선정했다. [단일 이동 결과](migration-comparison/results.md)와 [B3 통합 결과](results.md)에 핵심 수치·재현·제한을 정리했다. SC 결과는 별도 Baseline으로 유지한다.
SC 결과는 Baseline 단독 검증이다. S1~S5·S3 민감도·S6·선행 메커니즘·M1~M5 비교 backend를 구현하고 각각 별도 실행했다. 실제 노드 증감 통합은 후속이다. 공통 입력·adapter·관측·출력과 구현 순서는 [design.md §6.6](design.md)에 정의한다. 실제 서버의 downtime·latency는 아직 측정하지 않는다.
