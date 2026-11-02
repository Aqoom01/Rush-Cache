# V01 Leader-read 공통 benchmark adapter

기준일: 2026-10-30. V01의 실제 IoniaLeaderReadClient를 공통 GET/SET/DELETE 생성기·closed/open-loop·전수 지연·로컬 표준 결과 저장에 연결한다. 새 알고리즘 구현이나 정식 성능 비교가 아니다.

## 구현·재사용 경계

- 기존 V01 알고리즘은 `cache-core/src/main/java/io/openkache/core/replication/ionia-second/`에 그대로 유지한다. 기존 O00와 다른 파생 디렉터리도 유지한다.
- 새 adapter는 `cache-server/src/benchmark/java/io/openkache/server/benchmark/ionia_second/`의 V01BenchmarkClient/V01TcpClients/LocalV01DrainProbe다. 기존 application JAR/배포 의존성과 분리된 실험 sourceSet이다.
- V01BenchmarkClient는 실제 IoniaLeaderReadClient의 execute/refreshView/onTimer/close를 IoniaBenchmarkSession에 연결한다. 고정 Leader 주소의 O00 readReplica 옵션으로 대체하지 않는다.
- O00BenchmarkClient에 선택적인 session factory만 추가해 장기 세션·동일 ID/command·총deadline·재탐색/재시도·raw RPC 관찰/상한·logical admission·취소/late completion·publication·close를 공유한다. 기존 생성자는 기존 IoniaClient/Options를 생성하므로 원본 읽기 정책은 유지한다. 클래스/Settings/Diagnostics 이름의 O00는 기존 실험 API 호환성 때문에 유지한다.
- LocalV01DrainProbe는 동일 O00 엔진의 공개 snapshot을 확인하는 기존 LocalO00DrainProbe에 위임한다. trace, scheduler, histogram, writer, 원본 TCP/wire/파일 KV/View 저장소를 재사용한다. 공통 core API·production 의존성·Gradle·알고리즘은 수정하지 않는다.

## V01의 의미

GET 데이터는 현재 View의 Leader로 보낸다. 정상 View를 확인하기 위한 majority probe는 모든 replica에 남는다. Follower 데이터 READ와 MetaQuery가0이라는 것이며 모든 통신이 Leader 한 곳뿐이라는 의미는 아니다. View가 바뀌면 같은 logical ID/command와 남은 총deadline 안에서 재탐색하고 새 Leader로 재시도한다.

SET fanout·Leader를 포함한 Fast ACK·fallback, DELETE OK/MISS의 기존 결과 확정 계약은 유지한다. 독립 replica RAM log를 SSD WAL로 바꾸지 않으며 안정 View metadata force/선택적 KV force를 Fast ACK 영속화와 혼동하지 않는다. TCP는 RPC마다 새 socket을 열고 장기 client 객체가 socket pooling을 뜻하지 않는다.

Settings에는 readReplica나 읽기 인증 해제 옵션이 없다. 초기 leader와 정적 membership, run/slot·stream 수·wireClientBase·총timeout·maxRounds/backoff·logical/raw 상한·timer 주기를 명시한다. 공유 제한 검증에 사용하는 내부 Settings의 readReplica 자리 값은 V01 데이터 라우팅에 사용하지 않는다. wireClientBase는 동시에 실행하는 run/slot 간에 겹치지 않게 별도로 예약해야 한다.

## Java 연결

다음은 이미 준비한 generator/plan/settings/addresses/engines를 연결하는 예다. 같은 JVM의 빈 격리 그룹·고정 정상 View에 대한 관찰이며 원격 엔진을 자동으로 찾아 연결하지 않는다.

```java
var client = V01TcpClients.open(settings, addresses, socketTimeoutMillis, TimeSource.system());
var probe = new LocalV01DrainProbe(client, engines, settings.leader(), 0);
var collector = new LatencyCollector(plan.runId(), plan.loadSlot(), clockId, histogramRangeNanos);
var result = new OpenLoopRunner(generator, plan, client, probe, collector).run();
var metadata = new BenchmarkRunWriter.Metadata("V01", scenario, settings.replicas().size(),
        sourceCommit, sourceDirty, clockId, requiredMetrics);
var exported = BenchmarkRunWriter.write(newRunDirectory, metadata, generator.config(),
        plan, result, collector.freeze());
```

ClosedLoopRunner도 같은 client/probe/collector를 받는다. 실제 호출/설정 예제는 V01BenchmarkTcpTest다. benchmark metadata의 variant를 V01로 기록해야 한다. 현재 local manifest는 trace/plan/seed와 공통 metadata를 저장하지만 adapter Settings/socketTimeout/초기 membership 전체를 자동으로 포함하지 않는다. 정식 비교용 manifest를 만들 때 이 설정과 관측 capability·실제 라우팅/재시도·source/trace hash를 함께 고정해야 하며 local subset validity만으로 성능 비교 준비를 완료 처리하지 않는다.

## 검증 범위

- 제어된 transport: 3/5개 replica의 Leader GET·계속 유지되는 probe·READ/META0, 세션 재사용, View 변경 후 새 Leader 추종, 같은 ID/command와 줄어드는 남은 deadline, SET fanout/DELETE 결과, permanent/transient 오류, discovery/실행 deadline, 늦은 응답·취소·close·admission/중복·잘못된 reply/trace·주소 거절을 확인한다.
- O00 대조군에서는 실제 Follower READ/MetaQuery 경로가 유지되는지 별도로 확인한다. 새 session factory가 원본을 Leader-read로 바꾸지 않았다는 회귀다.
- 실제 로컬TCP3/5노드 × closed/open-loop × GET100/GET40·SET30·DELETE30, 총8조건. LOAD4/WARMUP3/MEASURE30·streams4, open rates20/20/50·상한8, closed 동시성2로 모든 요청 결과·histogram population·표준 결과 저장을 확인한다.
- benchmark cohort는 각 조건37logical, replica 적용 목표는 읽기4/혼합22다. 보조 감사7logical의 SET/GET 값·DELETE OK/MISS·재삽입과 O00 Follower+Meta1logical은 별도 client/ID namespace이며 이37개 계수에 섞지 않는다.
- READ/META0과 probe 존재는 adapter의 raw RPC 호출 관찰이다. 전체 peer 메시지·bytes·연결·물리 SSD 비용 측정을 완료한 것은 아니다. leader routing의 View 전환은 결정적인 제어 transport로 확인하고 원본 V01 장애/복구 회귀는 전체 build에 포함한다. 새 원격 장애 실험을 수행한 것으로 표시하지 않는다.

Java21을 설정하고 저장소 최상위에서 실행한다.

```powershell
.\gradlew.bat :cache-server:benchmarkTest --tests "*V01BenchmarkClientTest" --tests "*O00BenchmarkClientTest" --offline --console=plain
# TCP 실행 전 로컬 포트 예산을 확인한다. 전체 helper는 기존 10% gate를 포함한다.
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1
pwsh -NoProfile -File .\experiments\replication-benchmark-v1\Validate-Run.ps1 -RunDirectory <로컬-V01-run-경로>
```

실제 최종 검사 수·포트 gate/초기 오류를 포함한 실행 기록·source 보존·artifact 대조는 V01_ADAPTER_VALIDATION.json, V01_ADAPTER_SOURCE_PRESERVATION.json, V01_ADAPTER_ARTIFACT_CHECK.json을 따른다. LATENCY_EXPORT.md의 histogram/표본/결과 무효 판정 계약을 적용한다.

## 전체 회귀의 포트 자원과 실패 보존

처음 전체 시작 gate는60초 회복 대기를 초과해 테스트를 실행하지 않았다. 이후88/16384로 회복한 상태에서 시작한 첫 전체 회귀는 기존 benchmark까지 통과했으나 새 V01 TCP8조건 중7조건에서 UNAVAILABLE/discovery 실패 등을 기록했다. 직후 별도 OS 관찰에서 동적 포트 약15107/16384·TIME_WAIT21107을 확인했다. 당시 개별 socket exception 원문은 수집하지 않았으므로 각 실패의 원인을 소급 확정하지 않는다. 실패 XML/log는 보존한다.

새 V01 TCP 그룹의 BeforeAll에만 Wait-LocalTcpBudget.ps1을 연결했다. 기존 회귀가 사용한 포트가10% 이하로 회복한 뒤 이 새 그룹을 시작하며, 최대600초 회복하지 않으면 검사 실패다. Windows에서만 실제 로컬 상태를 읽고 JSON/log를 남기며 기존 결과 검사와 같이 PowerShell7의 pwsh.exe가 PATH에 있어야 한다. Linux에는 이 Windows gate를 적용하지 않는다. 기존 전체 gate·병렬 설정·테스트 및 client timeout·요청 수/rate·성공 조건을 낮추지 않으며 검사 skip/retry로 성공을 만들지 않는다. 회복 대기는 측정 구간 밖의 그룹 준비 단계(최대620초 lifecycle 한도)이며 각 테스트 본문은60초다. 이는 원본 TCP의 연결 비용/성능 문제를 해결하거나 과거 V07 간헐 실패를 고친 변경이 아니다.

최종 전체 build/installDist661개(추가26개: 단위18/TCP8)가 실패·오류·skip0으로 통과했다. 새 그룹 gate는22번 관찰 후1304/16384에서 통과했고 V01 결과8개 schema/hash·계수·histogram/percentile도 확인했다. 전체 회귀의 초기 실패는 그대로 기록하며 최종 통과로 지우지 않는다.

## 남은 작업

V02–V07 adapter, 공통 정확성 이력/ACK audit·자원/프로토콜 계측·1초 창·원격 apply/분산 launcher·manifest/clock/observer overhead/pilot 검증은 후속이다. 기존 V01 알고리즘과 공통 벤치마크에 V01을 연결한 이번 작업을 구분한다. EC2 접속/진단·성능 비교·commit/push/merge는 수행하지 않으며 plan.template.json은 draft/disabled를 유지한다.

근거: docs/IONIA_V01.md, https://app.notion.com/p/4074ed90243781819932d9b6449b55c3. V01 알고리즘의 원래 기능/장애 검증은 해당 문서의 당시 기록을 따른다.

## 후속 연결 진척 (2026-10-30)

남은 V02–V07의 공통 adapter도 연결·로컬 검증을 완료했다. 위 V01 당시의 검사/실패 기록은 보존한다. ADAPTERS_V02_V07.md/REMAINING_ADAPTERS_VALIDATION.json 참조. 다음은 공통 정확성·필수 계측·학습 워밍업 정책 보강 뒤 본시험 데이터/요청열 후보와 pilot이며, 정식 성능/EC2 작업은 아직 미실행이다.
