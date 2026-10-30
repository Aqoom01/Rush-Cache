# 단계2 — O00 장기 client adapter·요청 결과 집계

기준일: 2026-10-30. **O00 adapter 구현·로컬 기능 검증 완료.** EC2 접속·사전진단·공통 성능 실측·commit/push는 수행하지 않았다. 기존 회귀와 새 fixture에는 로컬 TCP 요청이 포함된다.

## 배치와 책임

- 공통 계약/집계: experiment-runner/src/benchmark/java/io/openkache/runner/benchmark/client/
- O00 연결: cache-server/src/benchmark/java/io/openkache/server/benchmark/ionia/
- 대응 검사는 각 모듈 src/benchmarkTest에 둔다.
- 기존 IoniaClient, IoniaClientTransport, IoniaTcpTransport, IoniaNode, Command/Result/Bytes/RequestContext를 직접 재사용했다. O00·각 변형·기존 Runner/생성기·공통 API·production 의존성·wire/서버 구현은 수정하지 않는다.
- experiment-runner는 독립 benchmark JAR/benchmarkElements만 내보내고 서버의 실험 sourceSet이 이를 참조한다. 공통 생성기에 IONIA/네트워크 코드를 넣거나 runner의 production에 server 의존성을 추가하지 않는다.
- 기존 application JAR에는 benchmark class가 포함되지 않는다. server의 새 TCP fixture만 기존 회귀 뒤에 실행하도록 순서를 지정했으며 기존 회귀의 병렬 설정·timeout·판정은 유지한다.

**장기 client는 socket pooling과 다르다.** logical stream/phase별 IoniaClient와 View를 유지한다. 기존 TCP 구현은 RPC마다 새 socket을 열며 이를 바꾸지 않았다. 원논문의 eRPC/RDMA나 지속 socket의 성능을 재현했다고 주장하지 않는다.

## 사용 경계

O00TcpClients.open(settings, addresses, socketTimeoutMillis, time)이 소유권을 가진 세션을 만든다. open은 주소·구성을 검증하고, 실제 발견/연결은 submit 때 수행한다.

BenchmarkClient.submit(LogicalRequest)는 읽기 전용 CompletionStage를 반환한다. 완료된 ClientOutcome과 snapshot/diagnostics를 소비하면 된다. 실제 부하 실행 CLI·phase coordinator는 아직 없다. 로컬 사용 예제는 O00BenchmarkTcpTest가 생성기 → 초기 적재 → warm-up/측정용 쓰기 → GET audit·DELETE/MISS·재삽입을 연결한다.

| 설정 | 의미/제약 |
| --- | --- |
| runId/loadSlot | 비어 있지 않은 runtime namespace |
| streamCount | 1..1024 logical streams. B1보다 작은 실험 adapter 한도 |
| wireClientBase | coordinator가 이 run/slot에 배타적으로 예약해야 하는 nonnegative numeric base |
| replicas/leader/readReplica | 3/5개 고정 membership. readReplica는 구성원. 실제 fixture는 follower 사용 |
| totalTimeoutNanos | submit 진입부터 1ns..60초. 발견·retry·결과 전달 대기도 포함 |
| maxRounds | 발견/시도 round 상한1..100. 실제 execute 호출 수와 다름 |
| retryBackoffNanos | 0..totalTimeout. round가 끝난 뒤 고정 backoff, deadline 연장 없음 |
| maxInFlight | 1..100,000. 처리/결과 publication slot의 합계 상한 |
| maxOutstandingRpc | 양의 수. 미완료 raw delegate stage occurrence 상한 |
| timerPeriodNanos | 1ms..1초. watchdog 관찰 주기이며 실제 deadline 정확도의 보장은 아님 |

이 한도/값은 자원 방어용 실험 범위이며 최적 성능 설정이 아니다. 숫자 namespace의 JVM 간 중복 예약을 이 adapter 혼자 검출할 수 없으므로 실제 실행 전 coordinator/manifest에서 확인해야 한다.

## ID·재시도·View

- B1의 stream/globalIndex/sequence/phase 관계를 submit 전에 검사한다.
- runtime ID=(runId, loadSlot, logical clientId, sequence).
- wire ClientId=wireClientBase+logical clientId, OperationId=sequence+1. overflow는 전송 전에 거절한다. 예약할 client 범위는 base..base+3×streamCount−1이다.
- 같은 active ID와 동일 요청은 같은 logical 결과를 공유한다. 다른 command로 ID를 재사용하면 거절한다.
- 각 logical client의 admission sequence는 엄격하게 증가해야 한다. 완료 ID 재제출/역순은 거절한다. gap은 허용하며 생성 stream의 누락 검사는 향후 coordinator의 책임이다. admission 상한으로 거절한 sequence는 소비하지 않는다.
- 처음 사용하는 client는 View를 발견한다. 이후 정상 요청은 유지한 View를 사용하고, retry round에서는 다시 발견한다. 발견은 목적지 선택이며 read certificate를 대체하지 않는다.
- 원본 certifyReads=true와 fallback10ms를 유지한다. SET은 원본 fast ACK, DELETE는 ordered/executed 결과, GET은 원본 인증/fallback을 그대로 따른다.
- UNAVAILABLE/NOT_PRIMARY/BUSY Result와 발견/실행 예외만 bounded round로 재시도한다. TIMEOUT과 영구 오류는 같은 deadline 안에서도 무조건 반복하지 않는다.
- 재시도는 동일 command/clientId/operationId와 같은 절대 deadline을 사용한다. 각 wire request에는 원본 client가 계산한 상대 잔여 시간만 전달한다.

## 결과·계수·시간

ClientOutcome은 runtime/wire ID, 연산, 최종 범주·사유·실제 client Result, 시각, round와 attempt 목록을 담는 불변 관찰이다. 기록은 반환만 하고 전체 요청 이력을 내부에 누적 저장하지 않는다.

| 범주 | 정의 |
| --- | --- |
| SUCCESS | GET HIT/MISS, SET OK, DELETE OK/MISS를 API 모양에 맞게 완료 |
| ERROR_RESPONSE | 원본 client API의 오류 Result 또는 잘못된 성공 모양. GET OK에 값이 없으면 성공으로 세지 않음 |
| NO_RESPONSE | adapter deadline·취소·close·미완료/실패한 발견·실행·client TIMEOUT |

ERROR_RESPONSE는 **client API 관찰 수준**이다. 원본 client는 socket 예외를 UNAVAILABLE로 바꿀 수 있으므로 이 범주만으로 서버 오류 frame 수신을 확정하지 않는다. MISS가 시나리오상 예상한 최신 결과인지는 별도 checker 책임이다. timeout/취소 후 실제 반영 가능성도 배제하지 않는다.

ClientAccounting은 GET/SET/DELETE별 lifetime counts와 불변 snapshot을 제공한다.
- admitted = success + errorResponse + noResponse + pending.
- pending에는 결과 publication 대기도 포함된다. active map 크기와 같은 뜻이 아니다.
- client attempt는 원본 IoniaClient.execute 호출/호출 예약 한 번이다. 초기 발견 실패만 있으면0이다.
- retriedLogical은 실제 client attempt가2회 이상인 logical 요청 수다. discovery round/실패·중복 submit·admission 거절은 별도다.
- protocol 내부 RECORD/EXECUTE/READ/META/PROBE는 ObservedRpc의 호출·delegate dispatch·완료·예외·상한 거절로 따로 센다. SET1회가 RECORD3/5회로 늘어도 logical 요청이나 client attempt를3/5개로 세지 않는다.
- 이 counts는 phase별 성능 cohort 결과가 아니다. 다음 coordinator/collector가 원래 요청 phase와 outcome을 연결하고 phase별 집계를 분리해야 한다. 현재 결과는 result.schema.json의 완성된 benchmark export가 아니다.

시각은 같은 JVM의 monotonic ns다.
- invokedNanos: submit 메서드 진입 시점, lock/초기 client 준비·발견도 포함.
- decidedNanos: 원본 client 결과 또는 timeout/cancel을 adapter가 관찰한 시점.
- terminalNanos: 읽기 전용 결과 future를 완료하기 직전의 publication 시점.
- elapsedNanos는 invoked→terminal, observedElapsedNanos는 invoked→decided이다. publication queue 대기를 종단 지연에서 빼지 않는다.
- publication에 도달할 때 deadline을 넘긴 성공/오류 후보는 NO_RESPONSE/DEADLINE으로 처음 확정하고 latePublications에 센다. 이미 공개한 terminal 결과를 나중 응답으로 성공 재분류하지 않는다.
- attempt 목록의 end/status는 logical 관찰 시 아직 미완료면 null이다. 늦은 client completion과 raw RPC completion은 서로 다른 경계다.

## 취소·close·자원 상한

공개 stage의 toCompletableFuture는 copy다. caller가 그 copy를 cancel/complete해도 원본 계수나 요청을 바꾸지 않는다. 명시적 cancel(runtimeId)을 사용해야 한다.

취소·timeout·close는 **원격 abort가 아니다**. 원본 client의 이미 보낸 RPC/fast-write fallback이 이후 진행할 수 있고, raw stage는 logical 종료 뒤에도 남을 수 있다. raw permit은 실제 delegate stage의 terminal 때만 반환한다. 이 상한은 전체 socket/프로세스 메모리/네트워크 bytes 상한이 아니다.

한 adapter의 session 수는 최대3×streamCount, 미완료 logical/publication 상태는 maxInFlight, raw stage occurrence는 maxOutstandingRpc로 제한된다. 완료 ID 전체를 저장하지 않고 client별 sequence high-water를 유지한다. 반납을 기다리는 publication slot 때문에 완료 future 관찰 직후에도 일시적인 admission 거절이 가능하다. 거절은 logical admitted에 넣지 않는다.

사용자 callback은 내부 lock/control timer 밖의 publisher 두 thread에서 실행한다. callback은 비차단이어야 한다. publisher가 막히면 publication slot으로 admission을 제한하고 queue 지연을 측정한다. 실제 정시 처리를 보장하는 realtime executor는 아니다.

close는 새 admission을 막고 아직 관찰이 끝나지 않은 요청을 CLOSED로 처리한다. 이미 관찰해 publication 대기 중인 결과는 버리지 않고 전달하되 deadline을 다시 검사한다. 원본 client와 소유한 AutoCloseable delegate를 한 번만 닫는다. raw 완료를 관찰하지 못하면 diagnostics에 outstanding을 남기며 drain 완료로 꾸미지 않는다.

## 검증

프로젝트 최상위에서 JAVA_HOME을 Java21로 설정한다.

```powershell
.\gradlew.bat :experiment-runner:benchmarkTest :cache-server:benchmarkTest --offline --console=plain
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1
```

Windows TCP fixture는 기존 검증 helper의 로컬 포트 예산 gate를 유지해 실행했다. EC2 진단이 아니다.

이번 단계 추가33검사(공통 계수2 + O00/관측/TCP31). 최종 전체562개(core392/server90/기존runner16/runner benchmark33/server benchmark31), 실패·오류·skip0. 상세 명령·XML/log 경로·보존 hash는 B2_VALIDATION.json/B2_SOURCE_PRESERVATION.json에 기록한다.

- 제어 응답: 동일ID retry·View 변경·영구 오류·round 한도·발견/실행 timeout·취소·close·중복/역순·namespace overflow·backpressure·늦은 결과/미응답 보존.
- 경합: 취소/응답 동시16회와 publication queue deadline 음성 대조. 유한 fixture이며 전체 interleaving 증명은 아니다.
- 실제 로컬TCP: 3/5노드 각각29 logical 요청. 생성 LOAD4/WARMUP4/MEASURE12 쓰기와 순차 audit·DELETE/MISS·재삽입, 인증된 follower GET 및6개 장기 client 확인.
- 실제 무응답 loopback: socket timeout5초인 probe가 남아 있어도 logical deadline300ms의 watchdog 종료를 관찰. raw outstanding을 숨기지 않음.
- 기존 src227개 SHA-256 동일, 새 Java11파일. 기존 application JAR에는 benchmark class0개.

## 다음 단계

공통 phase coordinator·LOAD_DRAIN/apply frontier 확인·closed/open-loop scheduler·phase/op별 histogram 및 manifest/result export를 구현한다. 아직 phase별 성능 집계, 모든36지표 수집, 분산 history/ACK audit, V01–V07 adapter, 실제 EC2 pilot/성능 비교가 완료된 것은 아니다. 기존 v1 template은 draft/disabled로 유지한다.

