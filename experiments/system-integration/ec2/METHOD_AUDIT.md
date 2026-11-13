# 전체 방법론 구현·통합 누락 감사

코드에 있는 이름, 실행 가능한 알고리즘, 원 논문/제품의 미구현 기능을 구분했다. source별 전수 목록과 선택 키는 [method-inventory.csv](method-inventory.csv)에 있다. 기존의 “전체 다 교체 가능” 설명에는 A3 연결과 B3 고정 배치 선택 누락이 있었다.

## 모듈별 결론

| 모듈 | 소스와 통합 실행 경로 대조 | 보완 |
|---|---|---|
| A1 | PB/PB_FENCED/RAFT/VR/CHAIN/CRAQ/ROSE 7개. VR 전송3모드와 Rosé lag 설정도 존재 | 주요 엔진 누락 없음. 느린 외부 FD보다 native watchdog이 먼저 선거하지 않도록 watchdog과 bootstrap 타이머 분리 |
| A2 | O00/V01~V07 8개. V06은 ADAPTIVE/ROUND_ROBIN 두 읽기 정책 | Round-robin을 CLI·matrix에 명시. 읽기6개 설정, MetaBatch bytes/pending/inflight, V05 storage.shards 설정의 하드코딩 해소 |
| A3 | Lab 모듈 프로필 F0~F6/F7a~c 10개, 주요 제품 방식5개. 서로 겹치는 항목 있음 | 원본 상태 기계를 서버에 포함하고 전체 멤버를 위한 실제 UDP/TCP adapter 추가. F7c Buddy는 실제 notification/refutation 경로를 새로 이식 |
| B1 | 실제 DistributionScheme 6개 class, Ring RANDOM/BALANCED 구분으로7개 선택 | 누락 없음. Factory와 source tree 자동 대조 검사 추가 |
| B2 | SplitPolicy3개, PlacementPolicy7개 | 누락 없음. Factory와 source tree 자동 대조 검사 추가. 실통합에서 드러난 Slicer의 단일 노드 축소 무한 반복 수정. 독립 MergePolicy 구현은 없음 |
| B3 | NodeCountPolicy8개+fixed 대조군, scheduler9개, copy3개, drain2개 | 기존8개 scaler 등 누락 없음. 독립 실험의 fixed-tablet-lpt adapter는 통합 선택에서 빠져 있어 FIXED_LPT로 연결 |

A1은 통합 구조에서 **제어/요청 승인/소유권 복제와 승계**, A2는 **실제 캐시 데이터 복제·읽기**를 담당한다. A1의 예전 데이터 복제 단독 실험과 같은 배치라고 해석하지 않는다. 모든 구현은 해당 저장소의 연구 prototype 범위다. 원 논문이나 Redis/Cassandra/Ignite/Aerospike 제품 전체를 재현했다고 표시하지 않는다.

## A3 선택과 실제 통신

기존3개 대조군과 새14개 Lab 프로필을 합쳐 실행 선택지는17개다. 이는 서로 다른 알고리즘이17개라는 뜻이 아니다. 주요5개 제품 계열의 실행 선택은 `F3`, `REDIS_CLUSTER`, `PHI_GOSSIP`, `IGNITE_TCP_RING`, `AEROSPIKE_MESH`다.

| 선택 | 실제 판단·관측 | 주의 |
|---|---|---|
| FIXED_TIMEOUT | 기존 각 native 엔진의 timer | 공통 F0 heartbeat baseline과 다른 대조군 |
| SWIM / SWIM_LIFEGUARD | 기존 core SWIM runtime | Lab F2/F3와 구분해 과거 재현 경로 유지 |
| F0 | FixedDeadline + UDP all-to-all heartbeat | restart incarnation 조건 유지 |
| F1 | Phi + versioned UDP gossip relay, controlled Lab 설정 | 새 version만 원 관측자의 로컬 도착 시각으로 입력. DOWN을 quorum grant로 바꾸지 않음 |
| F2 / F3 | 실제 UDP direct/indirect probe, seed 기반 무작위 대상 | F3의 LHA/confirmation은 원 Lab 설정 적용 |
| F4 | EWMA adaptive deadline + UDP heartbeat | 고정 timeout 알고리즘으로 대체하지 않음 |
| F5 | deterministic ring heartbeat | 기본 H=2. 현재3멤버에서는 전체 두 peer를 관측하는 퇴화 경우도 명시 |
| F6 | ring + 독립 helper probe 및 명시적인 ACK/NACK | helper 응답 부재는 inconclusive. 실패 증거로 계산하지 않음 |
| F7A / F7B / F7C | LHA probe / LHA suspicion / Buddy 각각 | Buddy 대상이 실제 메시지를 받아 incarnation을 올려 ALIVE를 보냄 |
| REDIS_CLUSTER | UDP PING/PONG + 유효시간 있는 master report 및 FAIL | 실제 보고 과반수 전에는 DEAD를 만들지 않음. 제품 replica election은 A1/A2 책임 |
| PHI_GOSSIP | 같은 Phi 기계, 제품 비교 경로의 기본1초 heartbeat/threshold8 | F1의 controlled 설정과 별도 프로필. relay 장애의 다른 live member DOWN도 결과에 보존 |
| IGNITE_TCP_RING | persistent TCP ring heartbeat + UDP report/독립 검증/topology | coordinator 고정. coordinator 재선출 없음 |
| AEROSPIKE_MESH | persistent TCP mesh + UDP proposal/ACK/commit | 원 Lab의 epoch+majority 모델. Paxos 재현 아님. 실제 ACK 증거만 사용 |

판단 상태는 단일 owner thread에서 변경한다. 소켓 수신/쓰기와 정책 thread를 분리했고 프레임은 run·계층·그룹·송신자와 요청 correlation을 검증한다. 메트릭에는 UDP와 TCP byte를 구분한다. `fdRuntimeValid`는 양쪽 계층의 실제 상태, runtime 오류 및 영구 crash의 DEAD 관측을 확인한다. `fdLiveProcessDownReports`는 종료되지 않은 프로세스에 대한 DOWN 보고를 보존한다. 이 값은 “원격 노드가 전역 합의로 제거됨”을 뜻하지 않는다.

Lab의 CLI·시뮬레이터·observer/target 고정 harness는 서버 배포물에 넣지 않는다. JDK-only 판단 class를 원본 source에서 별도 source set으로 컴파일해 서버 JAR에 포함한다. 연구별 Gradle 모듈이나 core/server 역방향 의존성을 추가하지 않았다.

## 현재 구현으로 그대로 지원할 수 없는 것

- **Ignite coordinator의 crash/재선출:** 원 class의 coordinator가 고정이며 선출 구현이 없다. 해당 replica를 종료하는 입력은 주입 전에 UNSUPPORTED로 거절한다. 정상과 coordinator가 살아 있는 peer 장애 비교는 실행 가능하다.
- **F6 H3/Q3 등 큰 helper 수:** replica group이3멤버라 감시자·대상 이외 helper는1개다. helper 또는 quorum≥2는 시작 전에 거절한다. 이 조건에는 별도의 FD topology 확장이 필요하다.
- **Ring + 임의 배치/개별 Split:** 기존 B1의 소유권 계약과 충돌한다. membership에 따른 Ring 이동은 가능하다. SLOT/RESIZABLE의 개별 Split도 지원하지 않는다.
- **FIXED_LPT + Split/RESIZABLE/Ring:** 원 B3 adapter가 단위를 고정한다. 동적 분할·병합/토큰 재배치를 덧붙여 같은 방법론이라고 표시하지 않고 충돌 조합을 거절한다. 빈 Tablet의 metadata drain도 실제로 수행한다.
- **B2 독립 Merge 방법론:** 현재 source에 구현이 없다. RESIZABLE의 merge를 B2 정책으로 중복 집계하지 않는다.
- **B3 CAMIG/과거 목적지 선택 변형:** 현재 scheduler 소스와 실행 registry에서 삭제됐다. 옛 문서나 결과만으로 실행 가능한 후보에 넣지 않는다.
- **PAX의 partition/hint/휴면 replica 경로:** 현재 구현은 count-only 정책이다. 원 제품 기능은 없다. 다른 B3 정책의 구현 범위도 각 source Javadoc을 따른다.
- **불인증 IONIA read의 일반 장애 실험:** 기존 `certifyReads=false`는 fixed-view simulation 대조군이며 view 변경 안전성을 보장하지 않는다. 일반 캐시 실험에 조용히 섞지 않는다.
- **물리 EC2 프로비저닝/중앙 gateway 자동 복구:** 준비된 그룹의 논리 증감과 구분한다. 이 기능은 현재 구현이 없으며 원문 기능으로 주장하지 않는다.

P-Store는 실제 과거 관찰 history가 있어야 실행되며 없으면 TRAINING이다. 임의의 가짜 history를 실측인 것처럼 넣지 않는다. 실제 최적 조합 판정에는 조합별 calibration과 동일한 부하/장애 조건의 반복 측정이 필요하다.

## 실행

`plan --a1 ... --a2 ... --fd ... --read-selection ...`로 선택한다. `--override fd.heartbeat.ms=...`, `--override storage.shards=...` 등으로 수치 설정을 전달한다. 제품 기본 timeout과 단축한 기능 검사 설정을 같은 성능 표로 합치지 않는다. `fd.native.watchdog.ms`의 Lab 기본은300초라 느린 FD보다 native watchdog이 먼저 승계하는 혼동을 피한다.

`matrix`는 A1/A2/A3와 A2 읽기 정책을 별도 축으로 만들고 호환되지 않는 조합을 제외한다. `batch`는 CSV를 실제 실행한다. 기본은 dry-run이며 `--execute`가 있어야 JVM/SSH를 시작한다. 입력·JAR hash, 원본 요청·audit, 양쪽 상태/FD와 PID cleanup을 행별로 남기고 PASS / FAILED_VALIDATION / REJECTED_CONFIG / UNSUPPORTED / RUN_ERROR를 나눈다. cleanup이 불완전하면 다음 실행을 시작하지 않는다.

```bash
python3 tools/integration.py matrix --stage coverage --output coverage.csv
python3 tools/integration.py batch --matrix coverage.csv --bundle /path/to/bundle --output /path/to/results --limit 10
python3 tools/integration.py batch --matrix coverage.csv --bundle /path/to/bundle --output /path/to/results --limit 10 --execute
```

대규모 전체 Cartesian을 실측 완료했다고 말하지 않는다. [검증 기록](AUDIT_VALIDATION.md)에서 소스 감사, JVM 내 소켓 검사와 독립 JVM 실행을 구분한다.
