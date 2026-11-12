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

## 8. 단계3-A 구현 반영 (2026-10-30)

별도 benchmark execution 영역에 fixed-N closed-loop·phase coordinator/결과·관찰 SPI를 추가했다. S를 고정한 C개 worker의 생성/송신·admission bounded retry·phase별 cohort와 backend delta 대조·최소시간 미달/잘못된 outcome 검출·취소/종료 및 unresolved 보존을 구현했다. O00 로컬 관찰기는 기존 공개 engine/client snapshot만 읽고 모든 replica의 기록/순서/commit/apply와 client raw/publication 정리를 확인한다.

추가23검사 및 최종 전체585검사 통과. 로컬3/5노드의 읽기100%/GET80·SET15·DELETE5에서 LOAD8/WARMUP7/MEASURE19 분리·동시성2/streams4·apply target8/15를 확인했다. 기존 src238개 hash 동일·새Java8파일. 상세는 CLOSED_LOOP.md/B3_VALIDATION.json 참조.

closed-loop 코어와 로컬 관찰만 완료됐다. 원격 progress/EC2 launcher·open-loop·histogram/표준 export·모든 변형 adapter·분산 checker/장애 시험은 후속이며 v1 template은 draft/disabled다.


## 9. open-loop 구현 반영 (2026-10-30)

별도 benchmark execution 영역에 정수 fixed-rate·고정 예정열·응답과 독립적인 발행·bounded admission을 추가했다. 상한 drop/발행 timing miss/client 거절/오류/무응답/미확정과 중단 시 due/future 미발행을 보존한다. 수집 적합성·발행 적합성·서비스 정상 여부를 구분하며 불확실한 쓰기는 적용 drain 적합성을 통과시키지 않는다. 상세는 OPEN_LOOP.md를 따른다.

추가22검사 및 최종 전체607검사 통과. 과부하 검사는 제어된 응답/driver 지연 모델이며 실제 서버 처리 한계 시험이 아니다. 실제 로컬TCP3/5노드의 읽기100%/혼합 부하에서 LOAD4/WARMUP3/MEASURE10·rates20/20/30ops/s·상한8 및 전 replica 적용4/6을 확인했다. 기존 src246개 SHA256 동일·새Java7파일이며 기존 Java/Gradle 변경은 없다. B4_VALIDATION.json/B4_SOURCE_PRESERVATION.json은 이번 검증 기록의 차수이며 위 표의 자원 계측 B4 완료를 뜻하지 않는다.

B3 실행 코어 중 closed/open-loop가 로컬에서 검증됐다. 전수 지연 histogram·표준 결과 export·변형 adapter·원격 관찰/분산 checker·자원 계측은 후속이다. EC2 접속/진단·공통 성능 실측·commit/push는 하지 않았으며 template은 draft/disabled를 유지한다.

## 10. 전수 지연·로컬 표준 결과 저장 반영 (2026-10-30)

benchmark measurement에 ns 1% sparse histogram·정확 sum/min/max·bucket percentile·overflow/표본 정책, thread-safe collector, JDK JSON/결과 writer를 추가했다. open-loop Observer를 재사용하고 closed-loop에는 선택적 outcome Observer만 연결했다. 기존 알고리즘·공통 API·Gradle·wire는 보존한다. 기존 src253개 중252개 hash 동일, ClosedLoopRunner1개에 호환 생성자/observer만 추가, 새Java9개다.

추가28/전체635검사 통과(실패·오류·skip0). 로컬TCP3/5노드 × closed/open-loop에서 LOAD4/WARMUP3/MEASURE30, mix40/30/30, 모든 CRUD 전수 population과 전 replica 적용22를 확인했다. 표준 schema/hash·계수·percentile 대조 산출물13개 및 변조/완료누락/잘못된 population/percentile 음성 대조4개도 통과했다. LATENCY_EXPORT.md/MEASUREMENT_VALIDATION.json 참조.

B3의 실행·지연·로컬 결과 저장 코어가 연결됐다. 완전한 ready manifest/trace·source hash·1초 창·자원/프로토콜 계측·variant adapters·history/ACK audit·remote/분산 launcher·overhead pilot은 후속이다. 로컬 subset validity가 시나리오 전체 성능 적합성이나 correctness PASS를 뜻하지 않는다. EC2 접속/진단·정식 성능 비교·commit/push/merge는 하지 않았다.

## 11. V01 공통 adapter 연결 (2026-10-30)

실제 IoniaLeaderReadClient를 별도 server benchmark/ionia_second adapter에서 연결했다. 기존 O00BenchmarkClient의 수명주기는 IoniaBenchmarkSession factory 경계만 추가해 공유하며 기본 생성자는 기존 IoniaClient/Options를 유지한다. V01의 GET 현재 Leader 추종·View probe·SET/DELETE 의미는 실제 client가 담당한다. LocalV01DrainProbe는 공유 O00 엔진의 기존 적용/정리 관찰을 재사용한다. 기존262src 중261동일·실험 O00 adapter1수정·새Java7개이며 알고리즘/공통API/Gradle/생산 의존성/wire는 보존한다.

추가26검사(단위18/TCP8), 최종 전체661검사 통과. 로컬3/5노드 × closed/open-loop × 읽기100/혼합40·30·30에서 적재4/워밍업3/측정30, replica 적용4/22, histogram/표준 V01 export 및 별도 CRUD 값·DELETE OK/MISS·재삽입을 확인했다. V01 READ/META0·probe 유지와 별도 O00 READ/META1 대조를 확인했으며 새로운 원격 장애/성능 비교를 수행한 것은 아니다. 산출물8개 schema/hash/계수·percentile 검증. ADAPTER_V01.md와 V01_ADAPTER_VALIDATION.json을 따른다.

V02–V07 adapter와 공통 정확성/자원/remote 관찰은 후속이다. 현재 local manifest에는 adapter Settings/socketTimeout/membership 전체를 자동 캡처하지 않으므로 정식 manifest에서 반드시 보완해야 한다. 처음60초 포트 gate 대기 초과로 테스트 미실행했고, 이후 첫 전체는 새 TCP7조건 실패·직후 약15107/16384포트 관찰을 보존했다. 새 V01 TCP 그룹 앞에만 동일10% 회복 gate를 추가해 기존 검사 조건을 유지한다. 개별 socket 예외 원문이 없으므로 소급 원인 확정은 하지 않는다. EC2·성능·commit/push/merge는 보류한다.

## 12. V02–V07 adapter 연결 (2026-10-30)

남은6방식을 기존 policy/engine/client와 typed TCP profile로 연결했다. V02–V05는 원본 Follower+Meta client와 해당 파생 서버, V06/V07은 실제 파생 client를 재사용한다. V07 실제 raw batch는 별도 cap/수명주기로 관찰해 timeout/close 뒤 미완료를 유지한다. 공통 ID/deadline/retry/collector를 공유하고 로컬 probe는 client 전체 membership·apply frontier·선택한 내부/raw 대기를 확인한다. 기존269src 중268개 동일·Metadata의 선택적 불변 adapterConfiguration1개 수정·새Java20개. 기존7인자 metadata 생성자는 유지하며 새 Gradle task는 신규 검사만 원래 benchmark 뒤에 실행한다.

추가99/전체760검사 통과: 모델44, 실제 TCP48조건, wire/profile 거절5, 실제 V07 burst2. 각 방식3/5노드·두 실행 방식·읽기/혼합을 검증하고 결과48개 schema/hash·계수/histogram/설정을 대조했다. 실제 V07 TCP3/5노드에서4logical/fresh Meta4→raw frame1. 기존661검사 범위와 병렬/timeout·부하/합격 조건은 유지한다. 증거는 ADAPTERS_V02_V07.md 및 REMAINING_ADAPTERS_VALIDATION.json을 따른다.

O00/V01–V07 연결의 로컬 기능 단계는 완료됐다. V06 WARMUP의 selector 학습이 MEASURE 새 client로 자동 이전되지 않고 V07 coalescer도 phase/client별이다. formal performance 전에 cold/steady 학습·워밍업 정책을 확정하고, 공통 정확성/ACK audit·필수 자원/통신/저장소·1초 창·ready manifest/서버 inventory·clock/overhead/remote 관찰을 보강한다. 이후 본시험 데이터/요청열 후보·사용자 EC2 재개 시 inventory/pilot 순서다. EC2/정식 성능/commit/push/merge는 미실행, template draft/disabled다.

## 13. correctness 전수 이력·예약 ACK 감사 (2026-11-02)

실제 공통 adapter를 decorator로 감싸 전수 payload·결과·submit/callback·미완료/거절/미응답 이력을 수집하고, 명시 초기 상태에서 제한된 전역 선형성 검사를 수행한다. 덮어쓰기가 없는 별도 배타적 audit key의 ACK ledger와 READY 이후 GET을 비교한다. 기존 Java 소스/알고리즘·core API·기존 두 실행기와 writer는 수정하지 않았다. 새 sourceSet 파일과 correctness task만 추가했다. 실제 보존/검증 결과는 CORRECTNESS_SOURCE_PRESERVATION.json과 CORRECTNESS_VALIDATION.json을 따른다.

새 TCP 검사는 전체8방식 ×3/5노드 ×closed/open-loop의32개 혼합 이력과, 전체8방식 ×3/5노드의16개 정상 follower 재기동 감사를 수행한다. 감사는 모든 replica 적용 후와 재합류 후 각각 별도 증거로 저장한다. 재기동 준비 중 RECOVERING을 정상 상태 probe로 바로 평가했던 초기16실패를 보존하고, 테스트 준비 gate만 바로잡았다. 기존 READY/timeout/부하·알고리즘 판정을 완화하지 않았다.

제공 범위는 소규모 단일 JVM 이력이며 checker 한도는 최대24operation이다. 미확정 완료 확장은 PASS가 아니고 초기 상태/탐색 한도가 불명확하면 INCONCLUSIVE다. full payload 수집의 성능 overhead는 미측정이다. ACK 원장은 일반 workload의 overwrite를 손실로 오인하지 않게 별도 영역을 쓰며 service GET 감사가 특정 replica 직접 읽기나 전원 손실 보장을 뜻하지 않는다.

후속: 표준 성능 result M28/M29 연결·대규모/분산 correctness, host/JVM·통신·저장소·1초 창/내부 지표, clock/overhead와 V06 cold/steady 학습·워밍업 정책. 이후 본시험 데이터/요청열 후보·EC2 재개 시 inventory/pilot이다. 이번에도 EC2/정식 성능/Git 작업은 하지 않으며 template draft/disabled를 유지한다.

## 14. P02 JVM/GC·1초 요청 창 (2026-11-02)

JDK source·bounded collector·독립 writer/reader를 benchmark 영역에 추가하고 기존 두 실행기에 호환 생성자·선택적 stage/finish lifecycle만 연결했다. 실제 source interval과 run-anchor terminal/cohort 요청 창을 구분하고 원래 client 계수와 대조한다. 같은 JVM의 replica와 부하는 한 process의 shared-fixture로 기록한다. baseline/미지원은 null+사유, reset/누락/역전/overflow/미종료는 invalid로 남기며 callback에서 management/file I/O를 수행하지 않는다. source/client provider는 비차단 계약이고 비협력 native 호출 강제 종료는 보장하지 않는다.

추가59/전체899검사 통과(실패·오류·skip0): 단위/모델27, 실제 O00/V01–V07×3/5노드×두 실행 방식32. 정상34/명시 invalid2개의 JVM 결과를 독립 검증하고 완료누락·byte 변조·hash 재계산 후 잘못된 CPU delta/요청 수/프로세스 중복/거짓 valid 등6개 대조를 거부했다. 초기 새TCP6실패는 증거를 보존했고 기존10% 포트 준비 gate를 새 시나리오마다 적용했다. 원 socket 예외가 없어 초기 원인을 소급 확정하지 않는다. 기존 회귀 대상·protocol timeout·정상 판정은 유지한다.

마지막 동시 창 생성 한도는 CHM 원자적 생성으로 바로잡고 cap1/4생산자/2000결과를 확인했다. 전체 명령은 수정 때문에 JVM 그룹에서 중단했으며, 변경 없는719개 증거와 최종 영향180개 재검증을 합쳐899개로 기록한다. 중단한 failed manifest는 보존하고 단일 전체 명령 통과로 표시하지 않는다.

표준 M18/M23의 전체 분산 coverage/정규화 비용, RSS/allocation/개별 pause, 통신·파일/host·내부 진단·V06 학습 워밍업·M28/M29 통합·관찰 비용/clock·ready manifest·원격 실행은 후속이다. 본시험 데이터 후보는 P08에서 trace/hash·초기 상태/stream mapping으로 고정하고 EC2 재개 후 pilot으로 규모를 확정한다. Jira164는 사용자 MR/병합 확인 전 진행 중이다. EC2/정식 성능/Git 미실행, template draft/disabled와 RAM replica log·stable View/선택적 KV force 구분을 유지한다.


## 15. TCP 메시지·바이트·연결 관측 (2026-11-02)

공통 transport2개와 wire write1곳에 기본 비활성 관찰을 추가하고 별도 TrafficRun/TrafficWriter를 두 실행기에 연결했다. logical/attempt와 outgoing exchange·sender frame을 분리하며 client 요청·server 응답·peer 송신의 실제 stream bytes를 센다. payload/ID 전수 label·RPC 수를 frame 수로 대체·수신 복사본 중복 합산은 하지 않는다. 원본/파생 알고리즘과 core/public API·bytes·timeout·socket 정책은 보존했다.

최종 전체949검사(추가50)와 정상33/invalid2/burst2·변조6의 독립43건이 통과했다. 기존 source310중307동일·공통 I/O3개 관찰 hook·새Java6개다. 새 source의 process/configuration 경계·부분 write/연결 종료/phase·callback 누락을 확인했다. 첫 strict try-lint와 복합 논리 키 JSON 직렬화 문제는 신규 테스트의 의도적 close 설명 및 cohort 목록 encoding으로 바로잡았다. 전역 compiler 기준은 유지했다. 정확한 명령·XML·source hash는 NETWORK_OBSERVATION_* 보고서를 따른다.

표준 M15/M16/M17 통합·원격 coverage·관찰 비용 적합성, 저장소/내부 진단·V06 학습/워밍업·M28/M29·ready manifest는 남아 있다. 다음은 file I/O/force 계층 분리이며 이후 공통 dataset/trace 후보·통합/overhead·EC2 재개 시 pilot 순서다. 사용자 요청으로 외부 작업 관리 서비스는 조회·변경하지 않는다. Git/EC2도 조작하지 않았다.

