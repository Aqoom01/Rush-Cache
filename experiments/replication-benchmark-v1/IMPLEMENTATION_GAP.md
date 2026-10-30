# 현재 구현과 공통 벤치마크 명세의 차이

기준일 2026-10-30. 읽기 전용 코드 대조 결과다. 이번 작업은 명세·설정·로컬 형식 검사이며 Java/Gradle/기존 테스트는 변경하지 않는다. 과거 기능검증 결과를 새 성능·멀티호스트 실험 결과로 합산하지 않는다.

## 1. 확인한 차이

| 영역 | 현재 코드에서 확인 | 공통 driver에 필요한 일 |
| --- | --- | --- |
| 실행 대상 | 기존 ExperimentRunner는 단일 targetHost/targetPort, closed-loop | 3/5개 replica endpoint와 장기 client adapter, 독립 load/controller 역할 |
| 부하 | uniform/fixed-hot, thread별 stream, warm-up과 측정의 같은 seed 재시작 | 고정 PRNG/stream 분할, 분리된 warm-up, trace hash, Zipf/moving-hot 별도 구현 |
| key | key 문자열을 지정 길이로 복사/절단 가능 | 충돌 없는 24B 인코딩과 생성기 자가검사 |
| 처리량 | configured measuredRequests / elapsed; 실패 포함 | logical admitted/success/error/no-response/pending 구분, goodput와 completion throughput 분리 |
| 지연 | 연결 생성 후 send 직전부터; 응답 없는 예외는 집계 제외 | connect/retry를 포함한 logical 종단 지연, attempt 지연 별도, 모든 outcome 기록 |
| 분포 | 공유 8192개 Algorithm R reservoir, p50/p99 | 연산·outcome·phase별 전수 histogram, p95/p99.9/mean/min/max·표본수·오차 공개 |
| 결과 | 단일 실행 요약과 제한된 metadata | manifest/environment/history/events/histogram/window/raw counters·hash 계약 |
| 자원 | protocol backlog/실제 I/O·message 수 계측 없음 | endpoint와 노드 계측의 관측 범위·상태·오버헤드 검증 |
| 장애 | 기존 Runner 결과의 faultScenario=none | 독립된 주입/관측/검증·원복 절차, ACK ledger와 recovery evidence |
| API outcome | Outcome 설명은 SET WRITE_OK이나 실제 비GET OK 분기는 DELETE도 WRITE_OK | GET/SET/DELETE·MISS·오류별 집계; 기존 result를 새 의미로 소급 해석하지 않음 |
| 저장소 | IoniaFileStorage는 append file+RAM index, GET FileChannel read, 선택적 force(false) | OS cache·실제 storage I/O 구분, live bytes와 과거 append bytes·남은 공간 측정 |

확인 위치: experiment-runner의 ExperimentRunner/ResultAccumulator/ExperimentResult/LatencyReservoir/WorkloadGenerator/RunnerConfig/Outcome, cache-core의 IoniaClient, cache-server의 IoniaFileStorage, 기존 experiments 문서. 위 항목은 해당 코드 경로의 관찰이며 모든 클래스에 기능이 없음을 전수 증명한 것은 아니다.

## 2. 보존·재사용

- 현재 O00=ionia, V01–V07=ionia-second부터 ionia-eighth까지 보존한다. 기존 생산 코드·공통 API·sourceSet·의존성·설정·테스트는 이 단계에서 변경하지 않는다.
- 기존 IoniaClient와 Transport의 실제 경로를 adapter로 재사용하되, per-request CLI를 장기 client 성능 driver로 간주하지 않는다. endpoint/View 변경·timeout/cancel·raw completion semantics를 먼저 검증한다.
- 기존 유한 history checker/fixture는 재사용 후보다. 성공 history뿐 아니라 실패 대조군·pending completion·timeout 처리로 새 checker 입력과의 호환성을 검증해야 한다.
- 독립 replica RAM log를 SSD WAL로 바꾸지 않는다. stable-view force와 선택적 KV force는 별도 필드다.
- Hermes는 API·membership·persistence profile을 별도로 검증한 뒤 편입한다. v1 schema의 ready 대상은 IONIA O00/V01–V07이다.
- 시작 시 보호 대상 cache-core/src·cache-server/src·experiment-runner/src의 파일214개 SHA-256을 보관했다. 완료 시 동일 목록·hash 대조를 별도로 기록한다.

## 3. 후속 구현 순서

| 순서 | 작업 | 완료 증거 |
| --- | --- | --- |
| B1 | 공통 logical request/attempt/outcome·trace generator·충돌 없는 key/value·PRNG 계약 구현 | 같은 seed/partition의 trace hash 일치, key 충돌/CRUD oracle·오류 대조군 |
| B2 | O00와 각 변형의 장기 client adapter·총deadline·retry·View 수명주기 | API별 성공/오류/timeout/cancel/late completion, 기존 wire 계약 유지 |
| B3 | phase·closed/open-loop·bounded admission·histogram·결과 export | local controlled endpoint로 보존식·schedule lag·no-response·quantile 검사 |
| B4 | 노드/host 자원·message·backlog·path 계측 | 관측 범위 문서, 집계 대조, on/off 오버헤드와 누락 상태 |
| B5 | history/ACK ledger·장애 관측/controller 구현 | 알려진 위반 검출, overwrite-safe audit, fault 미주입/불충분 관측 대조군 |
| B6 | 사용자가 재개할 때 EC2 inventory·배치·pilot | 실제 장비/clock/storage/disk budget 확인; 현재 보류 |
| B7 | 3노드 작은 correctness gate →5노드 →공통 성능 →민감도/장기·장애 | 각 gate의 artifact와 실패 결과 보존; 성능 수치는 해당 실행 이후만 보고 |
| B8 | Hermes 구현 후 동일 외부 계약으로 비교 | 기능·membership/persistence 적합성 후 paired 동일 trace/자원 비교 |

B1–B5도 이번 문서 작성으로 구현 완료된 것이 아니다. 장애 주입·EC2 자동 접속 도구는 이 디렉터리에 없다. fault 시나리오는 미래 실행의 절차 정의다.

## 4. 이번 로컬 검증의 의미

Validate-Spec.ps1은 JSON schema·카탈로그 교차 참조·설정 산술·결과 보존식·잘못된 예시 거부만 검사한다. 합성 ready fixture는 메모리 안에서만 구성하고 모든 host 주소를 .invalid로 둔다. 서버·AWS·SSH·네트워크·Java/Gradle을 호출하지 않는다. draft template과 NOT_RUN 예시는 그대로 유지한다.

검사 통과는 실제 환경 적합성, driver 구현, 선형성, 복구 성공, 성능 우수성을 뜻하지 않는다. 특히 schema는 adapter capability evidence의 진실성·clock 품질·독립 장애 조건을 판별하지 못한다.

## 5. 구현 전에 보완할 실행별 상세

manifest의 variant_parameters와 capability_evidence에는 stream mapping/PRNG 버전, retry 횟수·backoff, detector timeout, fault 관측/회복 안정화 기준, cache/OS/CPU/heap·disk budget 등 실행별 상세를 hash된 외부 설정으로 연결해야 한다. 문서의 시작값을 운영 최적값으로 채우지 않는다. 필수 상세가 없으면 unresolved에 사유를 남기고 draft로 유지한다.

## 6. 단계1 구현 반영 (2026-10-30)

기존 표는 명세 작성 당시의 기존 Runner 관찰로 보존한다. 별도 benchmark sourceSet에 logical GET/SET/DELETE·LOAD/WARMUP/MEASURE 분리·충돌 없는 key·식별/CRC32C value·indexed PRNG·고정 stream 분할·canonical SHA-256/hash-only/export를 추가했다. UNIFORM/FIXED_HOT만 지원하며 Zipf/moving-hot은 미구현이다.

B1 중 trace/key/value/논리ID 부분을 구현했다. 실제 attempt/outcome·client adapter·scheduler·histogram/collector·history/ACK audit는 후속이다. 상세는 TRACE_GENERATOR.md, 실제 검사 결과와 첫 회귀 실패를 포함한 제한은 B1_VALIDATION.json을 따른다. 기존 알고리즘·Runner 생산 코드/테스트는 변경하지 않았다. EC2·성능 실행 및 commit/push는 하지 않았다.

## 7. 단계2 구현 반영 (2026-10-30)

별도 server benchmark sourceSet에서 기존 IoniaClient/TCP를 재사용하는 O00 adapter와 공통 BenchmarkClient/ClientOutcome/ClientAccounting을 추가했다. 장기 client/View·같은ID/총deadline retry·발견/실행/최종결과 계수·raw stage 관찰 및 상한·취소/close·publication 대기/늦은 결과 처리를 구현했다. 기존 application과 소스227파일은 보존한다.

추가33검사·최종전체562검사 통과 및 로컬TCP3/5노드 각각29요청을 확인했다. 상세·한계는 ADAPTER_O00.md와 B2_VALIDATION.json 참조. B2의 O00 경로만 구현됐으며 V01–V07/Hermes adapter, phase별 부하 실행·histogram·result export·apply/drain·분산 checker/ACK audit는 후속이다. 원본 TCP의 RPC별 새 socket 정책과 과거 V07 TCP 간헐 실패 기록을 변경/해결한 것으로 보고하지 않는다. EC2는 계속 보류한다.

