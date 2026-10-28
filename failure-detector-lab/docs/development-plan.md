# OpenKache Failure Detector 개발 및 실험 계획서

- 문서 버전: 1.0
- 작성일: 2026-10-23
- 개발 형태: 1인 독립 개발
- 적용 범위: OpenKache와 분리된 실험 모듈 구현, 비교 실험, 최종 후보 선정, 선정안의 OpenKache 연동 검증

## 1. 프로젝트 목표

OpenKache에 적합한 Failure Detector를 감이나 단일 벤치마크 결과가 아니라 반복 가능한 실험 결과로 선정한다.

비교 대상은 다음 두 가지다.

| ID | 후보 | 역할 |
|---|---|---|
| A | SWIM + Lifeguard | 감지자 CPU 지연과 오탐 완화 후보 |
| B | Phi Accrual + Gossip | 네트워크 변동에 적응하는 통계 기반 후보 |

Valkey와 Couchbase의 기능은 위 세 후보와 같은 종류의 알고리즘이 아니므로 별도 계층으로 다룬다.

| 참고 시스템 | 적용 계층 | OpenKache 적용 목적 |
|---|---|---|
| Valkey Cluster | Failover 정책 | 다수 관측, 복제 최신성, 투표, Epoch를 이용한 안전한 승격 |
| Couchbase | Service/Disk Health | 멤버십 Heartbeat만으로 찾기 어려운 Data Service 및 SSD 부분 장애 감지 |

최종 결과물은 다음 질문에 답해야 한다.

1. 정상 노드를 장애로 잘못 판단하는 빈도가 가장 낮은 방법은 무엇인가?
2. 실제 장애를 가장 빠르고 빠짐없이 감지하는 방법은 무엇인가?
3. 노드 수가 증가할 때 메시지, 네트워크, CPU 비용이 어떻게 증가하는가?
4. 감지자 CPU Stall과 네트워크 지연·손실에서 어떤 방법이 가장 안정적인가?
5. 감지 결과를 실제 Failover로 연결했을 때 OpenKache의 GET/SET 복구 시간이 얼마나 되는가?
6. Heartbeat는 정상이지만 SSD 또는 Data Service가 멈춘 부분 장애를 어떻게 보완할 것인가?

## 2. 범위와 원칙

### 2.1 포함 범위

- SWIM + Lifeguard와 Phi Accrual + Gossip의 독립 구현
- 결정 로직 단독 실험과 프로토콜 전체 실험
- 장애 주입, Ground Truth 기록, 지표 수집, 결과 집계 자동화
- CPU, 프로세스 Pause, 네트워크 지연·손실·단절, Disk/Service Stall 실험
- 선정된 Membership Detector에 Service/Disk Health 보완
- Valkey 원칙을 참고한 Failover Gate와 Epoch 검증
- OpenKache GET/SET 워크로드를 이용한 End-to-End 복구 실험
- 표, 그래프, 최종 선정 보고서 생성

### 2.2 제외 범위

- 팀 공통 모듈 수정 또는 의존
- 초기 단계에서 OpenKache 운영 코드에 직접 병합
- 세 후보 모두에 대한 운영 수준 Failover 통합
- Valkey Cluster 또는 Couchbase 기능의 완전 재구현
- 서로 다른 장애 감지 결과를 단순 다수결로 합치는 복합 Detector

### 2.3 개발 원칙

1. **독립성**: 모든 코드는 OpenKache 공통 모듈과 분리된 개인 실험 영역에서 개발한다.
2. **공정성**: 두 후보는 같은 장애 시나리오, 워크로드, Seed, 실행 시간, 자원 제한으로 비교한다.
3. **분리 측정**: 판단 로직의 품질과 통신 프로토콜의 비용을 별도 실험으로 측정한다.
4. **재현성**: 한 명령으로 동일 실험을 다시 실행할 수 있어야 하며 수동 조작은 결과 데이터로 인정하지 않는다.
5. **사전 등록**: 실험 전 지표, 성공 기준, 파라미터 탐색 범위, 제외 조건을 설정 파일과 문서에 기록한다.
6. **원본 보존**: 원시 로그를 수정하지 않고, 가공 데이터와 그래프는 원시 로그로부터 자동 생성한다.

## 3. 핵심 연구 가설

| ID | 가설 | 주요 검증 실험 |
|---|---|---|
| H1 | SWIM + Lifeguard는 Local Health 보정을 통해 감지자 CPU Stall 상황에서 Phi Accrual + Gossip보다 낮은 False Positive를 보인다. | 감지자 CPU 부하·프로세스 Pause |
| H2 | Phi Accrual은 지연과 Jitter가 지속적으로 변하는 환경에서 고정 Timeout 계열보다 안정적이다. | 지연·Jitter·손실 단계 변화 |
| H3 | SWIM 계열은 노드 수 증가 시 노드당 Probe 비용을 제한하며 예측 가능한 확장성을 보인다. | 노드 수별 메시지·Byte·CPU |
| H4 | Membership Heartbeat만으로는 SSD 및 Data Service Stall을 충분히 찾을 수 없다. | Disk I/O·Service Thread Stall |
| H5 | 장애 감지와 Replica 승격을 분리하고 Epoch·복제 최신성·권한 검증을 두면 잘못된 승격을 줄일 수 있다. | Partition·Stale Replica·중복 승격 |

## 4. 전체 구조

실험 코드는 OpenKache 공통 코드와 분리하되, 후보 간 공정한 비교를 위한 실험 전용 인터페이스와 실행기는 공유한다.

```text
failure-detector-lab/
├── README.md
├── detector-contract/       # 실험 전용 입력/출력 계약
├── detectors/
│   ├── swim-lifeguard/
│   └── phi-gossip/
├── health/
│   ├── service-health/
│   └── disk-health/
├── failover-policy/
├── fault-injector/
├── workload-runner/
├── experiment-runner/
├── metrics/
├── scenarios/
├── analysis/
└── results/
    ├── raw/
    ├── processed/
    └── charts/
```

`detector-contract`는 팀 공통 모듈이 아니라 실험실 내부에서만 사용하는 비교 계약이다. 각 구현은 다음과 같은 동일 입력과 출력을 가진다.

### 4.1 공통 입력 이벤트

- `PROBE_SENT`, `ACK_RECEIVED`, `INDIRECT_ACK_RECEIVED`
- `HEARTBEAT_RECEIVED`
- `LOCAL_PROCESSING_DELAY`
- `SERVICE_PROBE_RESULT`, `DISK_PROBE_RESULT`
- `NODE_JOINED`, `NODE_LEFT`, `NODE_RESTARTED`
- Monotonic timestamp, 관측자 Node ID, 대상 Node ID, Incarnation/Epoch, Payload byte

### 4.2 공통 출력 이벤트

- `ALIVE`
- `SUSPECT`
- `DEAD`
- `RECOVERING`
- `HEALTH_DEGRADED`
- `FAILOVER_CANDIDATE`

Detector는 관측 결과만 출력하고 실제 Replica 승격 권한은 가지지 않는다. 승격은 `failover-policy`가 복제 최신성, 다수 관측, Epoch를 추가 검증한 뒤 수행한다.

### 4.3 상태 전이

```text
ALIVE -> SUSPECT -> DEAD -> RECOVERING -> ALIVE
   \-------- 정상 응답/반박 --------/

ALIVE -> HEALTH_DEGRADED -> ALIVE 또는 FAILOVER_CANDIDATE
```

Ground Truth 상태는 Detector 상태와 완전히 분리한다. Detector가 기록한 `DEAD`를 실제 장애로 간주하면 정확도 계산이 순환 논리가 되므로, 장애 주입기가 기록한 상태만 정답으로 사용한다.

## 5. 후보별 구현 계획

### 5.1 A: SWIM + Lifeguard

SWIM의 Direct/Indirect Probe, Suspicion, Incarnation, Gossip을 포함하고 그 위에 Lifeguard의 Local Health Awareness를 결합해 구현한다.

- 주기적으로 무작위 대상에 Direct Probe
- Direct Probe 실패 시 K개 중계 노드에 Indirect Probe 요청
- 실패한 대상을 즉시 DEAD로 만들지 않고 SUSPECT 상태로 전환
- Incarnation Number를 이용한 본인 반박
- 멤버십 변경 사항 Piggyback 전파
- SUSPECT 만료 후 DEAD 확정
- 모든 Timeout, Probe 주기, Indirect Node 수를 외부 설정으로 분리

- Local Health/Awareness Score
- 로컬 처리 지연 또는 Probe 실패 누적에 따른 Probe 관련 시간 보정
- 성공 관측 후 Awareness Score 회복
- Independent Suspicion confirmation에 따른 Suspicion 시간 단축
- 의심 대상에 상태를 빠르게 전달해 반박 기회를 주는 경로
- Lifeguard 세부 기능의 동작을 추적할 수 있는 관측 로그

완료 조건:

- 감지자 CPU 지연이 커질 때 Local Health 값과 Timeout 보정이 로그에 나타남
- 정상, Crash-stop, Packet loss, 지연, 감지자 CPU Stall 상황의 상태 전이 테스트 통과
- 동일 Seed에서 Probe 대상과 장애 주입 순서를 재현
- 메시지 수와 Payload byte를 종류별로 기록
- 원 논문과 `memberlist`에서 채택한 공식·상수·상한을 코드 주석과 설계 문서에 출처와 함께 기록

### 5.2 B: Phi Accrual + Gossip

Phi 계산과 Gossip 전파를 별도 컴포넌트로 구현한다.

- Peer별 Heartbeat 도착 간격 Sliding Window
- 평균, 분산 또는 선택한 도착 분포에 따른 Phi 계산
- 최소 표준편차 또는 최소 변동 폭 보호값
- 초기 표본 부족 시 Warm-up 정책
- Phi Threshold 외부 설정
- Heartbeat generation/version과 멤버십 상태 Gossip
- 오래된 Heartbeat 및 중복 Gossip 제거
- Peer별 독립 상태 판단

주의할 점은 Cassandra 변형과 Hazelcast/Akka 계열 구현 공식을 섞지 않는 것이다. 1차 구현은 Cassandra 소스의 변형을 기준으로 고정하고, 다른 분포 모델은 별도 Variant ID로만 실험한다.

완료 조건:

- 고정 간격, 점진적 지연, 급격한 지연, 표본 부족에 대한 Phi 단위 테스트 통과
- Threshold 증가에 따라 오탐과 감지 지연의 Trade-off가 일관되게 관찰됨
- Gossip fan-out과 interval을 외부 설정으로 변경 가능

### 5.3 Service/Disk Health

Membership Detector 선정 후 다음 신호를 추가한다.

- Data Service 내부 Loopback GET/SET Probe
- Event loop 또는 Worker queue 지연
- SSD read/write Probe 지연과 오류율
- I/O queue 또는 fsync 지연
- 일정 기간 연속 실패를 요구하는 Debounce
- Membership 상태와 Service 상태를 분리한 사유 코드

Health Probe는 사용자 데이터와 분리된 전용 Key/File 영역을 사용한다. Health Probe 자체가 장애를 악화시키지 않도록 빈도와 동시 실행 수를 제한한다.

### 5.4 Failover Policy

Valkey 원칙을 참고하되 OpenKache의 Primary-Backup 구조에 맞게 축소한다.

- `PFAIL`에 해당하는 로컬 의심과 `FAIL`에 해당하는 승격 가능 상태 분리
- 여러 관측자의 최근 장애 보고 수집
- 오래된 보고의 유효기간 제한
- Replica replication offset 또는 freshness 확인
- Epoch 증가 후 승격 요청
- Epoch당 1회 투표
- 더 낮은 Epoch의 승격 및 오래된 Replica 거부
- Minority partition의 승격 금지 또는 Write 차단
- 기존 Primary 복귀 시 자동 Primary 재획득 금지

Failover Policy는 특정 Detector에 유리하게 만들지 않는다. 모든 후보가 같은 Evidence 형식으로 연결되며, 최종 End-to-End 실험은 선정된 Detector에 대해서만 수행한다.

## 6. 실험을 세 단계로 분리하는 방법

### 6.1 단계 1: 판단 로직 단독 비교

동일한 Heartbeat/Probe 도착 Trace를 SWIM + Lifeguard와 Phi Accrual + Gossip에 Replay한다.

목적:

- 통신 방식 차이를 제거하고 장애 판단 품질만 비교
- Threshold와 Timeout 민감도 파악
- 구현 오류를 빠르게 발견

제한:

- Lifeguard의 실제 Probe 경로 효과와 Gossip 비용은 평가할 수 없음
- SWIM + Lifeguard에 Heartbeat Trace를 입력할 때는 Trace를 Probe 관측 이벤트로 변환하는 규칙을 문서화해야 함

### 6.2 단계 2: 프로토콜 전체 비교

각 후보의 실제 Probe 또는 Gossip을 실행한다.

목적:

- 메시지 수, Byte, CPU, 메모리, 수렴 시간 측정
- 노드 수 증가에 따른 확장성 확인
- 통신 방식과 판단 로직이 결합된 최종 동작 평가

### 6.3 단계 3: OpenKache End-to-End 검증

단계 1과 2에서 선정한 Detector에 Service/Disk Health와 Failover Policy를 연결한다.

목적:

- 실제 GET/SET 오류 시간과 복구 시간 측정
- Replica 승격 안전성 확인
- Partial failure에서 Membership-only 방식의 한계와 보완 효과 측정

## 7. 평가 지표

### 7.1 정확성 지표

| 지표 | 정의 | 해석 |
|---|---|---|
| False Positive Event Rate | 정상 노드를 SUSPECT/DEAD로 잘못 선언한 독립 사건 수 ÷ 정상 Node-hour | 낮을수록 좋음 |
| False Suspect Duration Ratio | 정상 시간 중 잘못 SUSPECT/DEAD였던 시간 비율 | 낮을수록 좋음 |
| False Negative Rate | 관찰 종료 시점까지 감지하지 못한 실제 장애 ÷ 전체 실제 장애 | 낮을수록 좋음 |
| Precision | TP ÷ (TP + FP) | 높을수록 좋음 |
| Recall | TP ÷ (TP + FN) | 높을수록 좋음 |
| Flap Count | 일정 시간 내 ALIVE/SUSPECT 반복 전이 횟수 | 낮을수록 좋음 |

연속 시계열에서 TN 개수는 Sampling 주기에 따라 임의로 커질 수 있으므로 Accuracy 하나만 주요 지표로 사용하지 않는다. False Positive는 연속된 잘못된 의심 구간을 하나의 사건으로 합쳐 계산하고, 시간 비율을 함께 제시한다.

### 7.2 시간 지표

- **Detection Latency** = `t(DEAD 또는 정책상 FAIL) - t(실제 장애 효력 발생)`
- **Suspicion Latency** = `t(SUSPECT) - t(실제 장애 효력 발생)`
- **Recovery Detection Latency** = `t(ALIVE 복귀) - t(실제 복구 완료)`
- **Membership Convergence Time** = 장애 발생 후 정상 관측자들이 동일 상태에 도달한 시각의 최댓값
- **Failover Decision Time** = `t(승격 승인) - t(실제 Primary 장애)`
- **Service Recovery Time** = `t(정상 GET/SET 재개) - t(실제 Primary 장애)`

모든 시간은 Wall clock이 아니라 동일 호스트의 Monotonic clock 또는 동기화 오차가 검증된 시계를 기준으로 계산한다.

### 7.3 비용과 확장성 지표

- 노드당 초당 Protocol message 수
- 노드당 초당 송수신 Byte
- 장애 1건당 추가 메시지와 Byte
- CPU 평균, p95, p99
- RSS/Heap, Allocation rate, GC Pause
- Scheduler/Event-loop delay
- 노드 수 N에 따른 전체 비용과 노드당 비용
- 장애 정보 전파 완료까지의 메시지 수

### 7.4 OpenKache 서비스 지표

- GET/SET 처리량
- GET/SET p50, p95, p99 Latency
- 오류율과 Timeout 비율
- 장애 중 Unavailable interval
- Failover 전후 데이터 유실 또는 Stale read 수
- 중복 Primary 또는 Split-brain 발생 횟수
- Stale Replica 승격 횟수
- 복구 노드 Rejoin 후 정상화 시간

### 7.5 Ground Truth 규칙

- Crash: 프로세스 종료 명령이 적용된 시각
- Network fault: Traffic control 규칙이 커널에 적용된 시각
- CPU Stall: 부하 또는 Pause가 목표 수준에 도달한 시각
- Disk Stall: I/O 제한 또는 장애 규칙이 적용된 시각
- Recovery: 해당 규칙 제거와 Health check 성공이 확인된 시각

장애 주입기는 `fault_id`, `target`, `fault_type`, `start_at`, `end_at`, `actual_applied_at`을 기록한다. Detector 로그를 Ground Truth로 사용하지 않는다.

## 8. 실험 변수 설계

아래 값은 최초 권장 범위이며, 사전 Pilot에서 장비 한계를 확인한 뒤 최종 고정한다.

| 변수 | 권장 수준 |
|---|---|
| 노드 수 | 3, 5, 10, 20, 50 |
| 장애 대상 수 | 1, 전체의 10%, 전체의 30% |
| 고정 지연 | 0, 20, 50, 100, 300 ms |
| Jitter | 0, 10, 50, 100 ms |
| Packet loss | 0, 0.5, 1, 5, 10% |
| Partition 지속 시간 | 1, 3, 10, 30 s |
| CPU 사용률 | 정상, 50, 80, 95% 이상 |
| Process Pause | 100, 500, 1,000, 3,000 ms |
| Disk 지연 | 정상, 20, 100, 500, 2,000 ms |
| 워크로드 | 최대 처리량의 30, 60, 90% |
| Read:Write | 90:10, 50:50 |

전체 조합을 한 번에 수행하면 실험 수가 폭증한다. 다음 순서로 진행한다.

1. 한 번에 한 변수만 바꾸는 단일 요인 실험
2. 영향이 큰 변수만 선택
3. `감지자 CPU + 네트워크 Jitter`, `Target CPU + Disk Stall` 등 핵심 상호작용 실험
4. 선정 후보에 대해서만 대규모 End-to-End 실험

### 8.1 매 실험에서 고정할 항목

- OpenKache 및 Detector commit SHA
- OS, Kernel, Runtime 버전
- CPU core와 Memory 제한
- Node topology와 역할
- Dataset 크기와 Key 분포
- 워크로드 도착률과 Read/Write 비율
- Warm-up, 장애 전 정상 구간, 장애 구간, Cool-down 시간
- 장애 대상, 시작 시각, 지속 시간
- Random seed
- 로그 수준
- Metrics scrape interval
- Detector 파라미터 세트 ID

## 9. 필수 실험 시나리오

| ID | 시나리오 | 변화 변수 | 핵심 결과 |
|---|---|---|---|
| FD-00 | 정상 장시간 실행 | 없음 | 기본 오탐률, 기본 비용 |
| FD-01 | 단일 노드 Crash-stop | 노드 수 | 감지 지연, 수렴 시간 |
| FD-02 | 무작위 Packet loss | Loss | 오탐, 감지 지연 |
| FD-03 | 지속 지연 및 Jitter | Delay/Jitter | Phi 적응성, SWIM Timeout 민감도 |
| FD-04 | 감지자 CPU 부하 | CPU | Lifeguard 오탐 개선 |
| FD-05 | 감지 대상 CPU 부하 | CPU | 느린 노드와 장애 구분 능력 |
| FD-06 | 감지자 Process Pause | Pause 시간 | Local Health 효과 |
| FD-07 | Minority/Majority Partition | Partition 형태 | 잘못된 DEAD와 수렴 |
| FD-08 | 노드 수 증가 | N | 메시지·Byte·CPU 확장성 |
| FD-09 | 장애 후 빠른 복구 | 장애 지속 시간 | Flapping, 복구 감지 |
| HD-01 | Heartbeat 정상 + Disk Stall | Disk 지연 | Membership-only 누락 여부 |
| HD-02 | Heartbeat 정상 + Data Service Stall | 서비스 지연 | Service probe 효과 |
| FO-01 | Primary Crash + 최신 Replica | 없음 | 정상 Failover 시간 |
| FO-02 | Primary Minority Partition | Partition | Split-brain 방지 |
| FO-03 | Stale Replica만 도달 가능 | Replication lag | 승격 거부 여부 |
| FO-04 | 장애 Primary 재합류 | 재합류 시각 | Epoch 충돌 방지 |

FD-00부터 FD-09까지는 두 후보를 모두 비교한다. HD와 FO 시나리오는 Membership Detector 선정 후 보완 계층을 검증한다.

## 10. 파라미터 튜닝과 공정한 비교

### 10.1 데이터 분리

- Calibration Seed: 파라미터 선택에만 사용
- Evaluation Seed: 최종 비교에만 사용
- Evaluation 결과를 본 뒤 파라미터를 바꾸면 새 버전과 새 Evaluation Seed를 사용

### 10.2 두 가지 비교 기준

1. **동일 오탐 예산 비교**: 후보의 False Positive Event Rate를 같은 수준으로 맞춘 뒤 Detection Latency 비교
2. **동일 감지 기한 비교**: 같은 Detection deadline을 만족하도록 맞춘 뒤 False Positive와 비용 비교

한 후보의 기본값과 다른 후보의 최적값을 비교하지 않는다. 각 후보에 동일한 튜닝 예산과 탐색 횟수를 제공한다.

### 10.3 반복과 통계

- Pilot: 조건당 3~5회
- 본 실험: 조건당 독립 Seed 최소 30회
- 실행 순서는 알고리즘별 몰아서 수행하지 않고 무작위 Block 순서 사용
- 중앙값, p95, p99와 95% Bootstrap Confidence Interval 보고
- 평균만으로 결론을 내리지 않음
- 실패하거나 누락된 Run도 삭제하지 않고 제외 사유와 함께 기록
- 비교 시 가능한 경우 동일 Seed끼리 Paired difference를 계산

## 11. 데이터 형식과 자동화

### 11.1 실행 메타데이터

각 Run은 다음 필드를 반드시 가진다.

```text
run_id, started_at, git_sha, algorithm, algorithm_version,
parameter_set_id, seed, node_count, topology, workload_id,
fault_id, fault_type, fault_target, environment_id, status
```

### 11.2 이벤트 로그

```text
run_id, monotonic_ns, node_id, peer_id, event_type,
detector_state, ground_truth_state, incarnation, epoch,
phi, local_health_score, message_type, payload_bytes, reason
```

### 11.3 자동 실행 흐름

```text
환경 초기화
-> 노드 시작 및 Ready 확인
-> Warm-up
-> 정상 기준 측정
-> 장애 주입 및 Ground Truth 기록
-> 관찰
-> 장애 복구
-> Cool-down
-> 로그/메트릭 수집
-> Run 유효성 검사
-> 요약 지표 생성
-> 환경 완전 초기화
```

Runner는 중간 실패 시 다음 Run으로 넘어가기 전에 남은 프로세스, 네트워크 규칙, CPU/Disk 제한을 검증하고 초기화한다.

## 12. 결과 표와 그래프

최종 보고서에는 최소 다음 시각화를 포함한다.

1. 알고리즘별 Detection Latency ECDF
2. 알고리즘별 False Positive events/node-hour와 95% CI
3. False Positive 대 Detection Latency Pareto curve
4. 노드 수에 따른 노드당 message/s 및 byte/s 선 그래프
5. 지연·손실 수준별 결과 Heatmap
6. 감지자 CPU 사용률에 따른 False Positive 선 그래프
7. Phi Threshold 및 SWIM Timeout 민감도 그래프
8. 장애 발생부터 SUSPECT, DEAD, 승격, GET/SET 복구까지의 Timeline
9. Disk/Service Stall에서 Membership-only와 Health 보완안 비교 막대그래프
10. OpenKache 처리량 및 p99 Latency Before/During/After 비교

최종 요약표 예시는 다음과 같다.

| 후보 | FP/node-hour | Detection p50/p95 | FN | Byte/s/node | CPU p95 | Convergence p95 | 비고 |
|---|---:|---:|---:|---:|---:|---:|---|
| SWIM + Lifeguard | 측정값 | 측정값 | 측정값 | 측정값 | 측정값 | 측정값 |  |
| Phi Accrual + Gossip | 측정값 | 측정값 | 측정값 | 측정값 | 측정값 | 측정값 |  |

## 13. 최종 선정 방법

### 13.1 먼저 통과해야 하는 안전 조건

- 모든 Crash-stop 평가에서 관찰 제한 시간 내 감지
- Partition 실험에서 중복 Primary 및 안전하지 않은 승격 0건
- Stale Replica 승격 0건
- 장시간 정상 실험에서 사전에 정한 오탐 예산 충족
- 90% 부하에서도 Detector 때문에 OpenKache p99가 사전 한도를 초과하지 않음

절대 기준값은 임의로 정하지 않고 OpenKache 목표에서 역산한다.

- 허용 오탐률 = `하루 허용 가능한 잘못된 Failover 횟수 ÷ 노드 수 ÷ 24`
- 허용 Detection deadline = `서비스 RTO - 승격·라우팅·Warm-up 소요시간`
- 허용 Protocol bandwidth = `노드별 운영 네트워크 예산의 일정 비율`

### 13.2 안전 조건 통과 후 비교 점수

| 항목 | 권장 가중치 |
|---|---:|
| False Positive 및 상태 Flapping | 30 |
| Detection Latency 및 False Negative | 25 |
| 네트워크 비용과 확장성 | 15 |
| CPU·메모리 비용 | 10 |
| 환경 변화에 대한 강건성 | 10 |
| 구현·운영 복잡도 | 10 |

가중합만으로 바로 선정하지 않는다. 먼저 Pareto frontier를 제시하고, 안전 조건을 통과한 후보가 둘 이상일 때만 사전에 정한 가중치를 적용한다. 신뢰구간이 겹쳐 우열을 말하기 어려우면 구현 복잡도가 낮고 파라미터 민감도가 작은 후보를 선택한다.

## 14. 개발 일정

1인 개발 기준 8주를 권장한다.

| 주차 | 작업 | 완료 기준 |
|---|---|---|
| 1주 | 요구사항 고정, 실험 계약, Ground Truth, Runner 골격 | 정상 Run 1건을 자동 수집·집계 |
| 2주 | SWIM + Lifeguard의 SWIM Probe·Suspicion·Gossip 구현 | 단위 테스트와 FD-00/01 통과 |
| 3주 | Lifeguard Local Health와 Suspicion 보정 구현 | CPU Stall 및 Process Pause 실험 가능 |
| 4주 | B Phi + Gossip | Trace Replay와 Protocol Run 가능 |
| 5주 | Network/CPU Fault Injector, 전체 FD 시나리오 | FD-00~09 자동 실행 가능 |
| 6주 | Calibration, Pilot, 파라미터 고정 | 최종 Parameter set과 제외 규칙 확정 |
| 7주 | 본 실험 30회 반복, 결과 분석, 후보 선정 | 원시 데이터·CI·그래프 생성 |
| 8주 | Service/Disk Health, Failover Gate, OpenKache E2E | HD/FO 결과와 최종 선정 보고서 |

8주 안에 End-to-End까지 어렵다면 1차 종료점을 7주차의 Membership Detector 선정으로 두고, Health/Failover 통합을 2차 과제로 분리한다. 두 Detector를 모두 OpenKache 운영 경로에 붙이지 않고 선정된 하나만 통합한다.

## 15. 단계별 산출물

### M1. 실험 기반 완성

- 실험 계약 및 상태 모델 문서
- Ground Truth/Fault Injector
- Run metadata 및 Event schema
- 1-command 실행 스크립트

### M2. 두 후보 구현 완성

- SWIM + Lifeguard와 Phi Accrual + Gossip 소스 코드
- 알고리즘별 단위·통합 테스트
- 파라미터 설명과 출처
- 결정 Trace 출력 기능

### M3. 비교 실험 완성

- Calibration 결과
- 고정 Parameter set
- FD-00~09 원시 로그
- 집계 CSV/Parquet
- 표와 그래프 생성 코드

### M4. OpenKache 적용안 완성

- 최종 Detector 선정 기록(ADR)
- Service/Disk Health 설계
- Failover Gate/Epoch 설계
- OpenKache End-to-End 결과
- 한계, 미해결 위험, 운영 권장값

## 16. 주요 위험과 대응

| 위험 | 영향 | 대응 |
|---|---|---|
| 구현마다 기능 범위가 다름 | 불공정 비교 | 공통 상태 모델과 필수 기능 Checklist 사용 |
| 알고리즘 기본값 편향 | 특정 후보에 유리 | 동일 튜닝 예산, Calibration/Evaluation 분리 |
| Ground Truth 시각 오차 | 감지 지연 왜곡 | Monotonic timestamp와 적용 확인 이벤트 기록 |
| 같은 호스트 자원 경합 | 가짜 CPU/Network 결과 | CPU pinning·자원 제한·실행 순서 무작위화 |
| Container Disk 실험이 실제 SSD를 반영하지 못함 | 결론 과장 | Logic 실험과 실제 SSD 실험을 구분해 보고 |
| 전체 조합 폭증 | 일정 지연 | 단일 요인 후 핵심 상호작용만 실행 |
| 로그 수집이 Detector를 방해 | 성능 왜곡 | 비동기·Batch 로그, 로그 비용 별도 측정 |
| Partition에서 잘못된 승격 | 데이터 불일치 | Detector와 승격 권한 분리, Epoch/Freshness Gate |
| 결과 차이가 통계적으로 작음 | 과도한 결론 | CI와 효과 크기 보고, 복잡도 기준 Tie-break |

## 17. 완료 정의(Definition of Done)

다음 항목을 모두 만족하면 프로젝트를 완료한 것으로 본다.

- [ ] SWIM + Lifeguard와 Phi Accrual + Gossip 구현 및 단위 테스트가 존재한다.
- [ ] 같은 Trace를 두 후보에 Replay할 수 있다.
- [ ] 각 후보의 실제 Probe/Gossip을 포함한 전체 실험이 가능하다.
- [ ] FD-00~09를 같은 조건과 Seed로 최소 30회 반복했다.
- [ ] False Positive, False Negative, Detection Latency, 비용, 수렴 시간을 자동 계산한다.
- [ ] 모든 결과가 원시 로그에서 재생성된다.
- [ ] 평균뿐 아니라 p95/p99, CI, 효과 크기를 보고한다.
- [ ] Pareto curve와 최종 선정 근거가 문서화되어 있다.
- [ ] 선정 후보에 HD 및 FO 시나리오를 수행했다.
- [ ] OpenKache GET/SET 복구 시간과 안전성을 검증했다.
- [ ] 실패한 실험과 알려진 한계도 최종 보고서에 포함했다.

## 18. 참고 기준

- [HashiCorp memberlist](https://github.com/hashicorp/memberlist)
- [Lifeguard paper](https://arxiv.org/abs/1707.00788)
- [Apache Cassandra architecture: Failure Detection](https://github.com/apache/cassandra/blob/trunk/doc/modules/cassandra/pages/architecture/dynamo.adoc)
- [Valkey Cluster specification](https://valkey.io/topics/cluster-spec/)
- [Couchbase Automatic Failover](https://docs.couchbase.com/server/7.6/learn/clusters-and-availability/automatic-failover.html)

이 문서의 최종 핵심은 `가장 빠른 Detector`를 찾는 것이 아니라, OpenKache가 허용할 수 있는 오탐 예산 안에서 가장 빠르고, 확장 가능하며, 실제 서비스 복구까지 안전하게 연결되는 Detector를 찾는 것이다.
