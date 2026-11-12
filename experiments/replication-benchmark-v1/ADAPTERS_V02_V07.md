# V02–V07 공통 benchmark 연결

기준일: 2026-10-30. 기존 파생 알고리즘을 그대로 사용해 공통 GET/SET/DELETE·closed/open-loop·전수 지연·로컬 표준 결과 저장에 연결한다. 단일 JVM의 기능 검증 범위이며 정식 분산 성능 비교 준비 완료를 의미하지 않는다.

## 방식별 실제 연결

| 표시 ID | 원래 보관 폴더 | 실제 client/server | profile·주요 축 |
| --- | --- | --- | --- |
| V02 | replication/ionia-third | 원본 IoniaClient + IoniaThirdNode/Engine | IoniaHistorySettings, History 목표 N. wire의 과거 표기 V02a 보존 |
| V03 | replication/ionia-fourth | 원본 IoniaClient + IoniaFourthNode/Engine | IoniaBatchSettings, Prepare count/bytes/delay |
| V04 | replication/ionia-fifth | 원본 IoniaClient + IoniaFifthNode/Engine | IoniaApplySettings, 적용 key 허가 K·고정 저장 배치 S. 과거 V04a profile 보존 |
| V05 | replication/ionia-sixth | 원본 IoniaClient + IoniaSixthNode/WorkerFileStorage | IoniaWorkerSettings, 고정 shard S·실제 I/O worker W. 과거 V04b profile 보존 |
| V06 | replication/ionia-seventh | 실제 IoniaAdaptiveReadClient + 원본 IoniaNode | RR/ADAPTIVE client 정책. 서버 wire는 O00와 동일하며 client 정책 hash는 별도 |
| V07 | replication/ionia-eighth | 실제 IoniaMetaBatchClient + IoniaEighthNode/MetaBatchTcpTransport | Meta E/B/D/P/W는 client 정책, ionia-meta-batch-v1은 별도 wire/stable protocol |

표시 ID와 원래 Java package/legacy profile 문자열을 바꾸지 않는다. 원본/이전 파생을 누적하거나 V01 Leader-read로 대체하지 않는다. 실제 알고리즘 소스는 cache-core의 기존 replication 하위 독립 소스 루트에 유지한다. 새 adapter는 `cache-server/src/benchmark/java/io/openkache/server/benchmark/ionia_third`부터 `ionia_eighth`에 분리한다.

## 재사용과 변경 범위

O00BenchmarkClient의 기존 session factory를 사용해 장기 세션·logical ID/wire ID·총deadline·발견/재시도·admission·시도/결과 집계·취소/late publication·종료를 공유한다. 공통 trace/scheduler/histogram/result writer와 기존 파일 KV/노드·TCP를 재사용한다. V02–V05는 원본 Follower+Meta·View 인증, V06은 실제 선택 client, V07은 실제 배치 client를 사용한다.

`benchmark/variants`에는 좁은 세션 연결·TCP profile 연결·공통 수명주기 façade·로컬 drain과 raw batch 관찰만 둔다. 공통 Workload Generator에 IONIA 코드를 넣지 않는다. 원본/파생 알고리즘·공통 core API·production 의존성·wire/서버·기존 Java 테스트는 변경하지 않는다.

기존 Java에서 수정한 곳은 BenchmarkRunWriter.Metadata의 선택적 `adapterConfiguration`뿐이다. 기존7인자 생성자는 빈 map으로 위임해 호환되며 새8인자 생성자는 정렬된 불변 map을 보관한다. 새 검사만 `:cache-server:variantBenchmarkTest`로 분리해 원래 benchmark 검사 뒤에 배치하고 check/build에 포함한다. 기존 검사 대상/병렬/timeout·부하는 유지한다.

## 생성·실행 예

각 adapter는 typed 정책과 공통 O00BenchmarkClient.Settings를 받는다. `open`은 정책의 정확한 configuration으로 기존 TCP를 열며 실제 socket timeout을 metadata에 기록한다. raw transport를 직접 주입하면 socket timeout은 `unobserved_external_transport`로 남긴다.

```java
var client = V02BenchmarkClient.open(runtimeSettings, historySettings,
        addresses, socketTimeoutMillis, TimeSource.system());
var probe = client.localProbe(thirdEngines);
var collector = new LatencyCollector(plan.runId(), plan.loadSlot(), clockId, histogramRangeNanos);
var result = new OpenLoopRunner(generator, plan, client, probe, collector).run();
var metadata = client.metadata(scenario, clockId, sourceCommit, sourceDirty, requiredMetrics);
var artifact = BenchmarkRunWriter.write(newRunDirectory, metadata,
        generator.config(), plan, result, collector.freeze());
```

V03/V04도 해당 typed 엔진 map, V05는 storage 진단까지 확인하는 IoniaSixthNode map, V06/V07은 해당 서버에서 얻은 IoniaEngine map을 localProbe에 전달한다. ClosedLoopRunner도 같은 client/probe/collector를 받는다. 실제6방식의 서버 조립과 호출은 RemainingTcpCluster/RemainingTcpChecks에서 확인한다. runtime wireClientBase는 동시에 사용하는 다른 run/slot과 겹치지 않게 예약해야 한다.

V06/V07의 공통 Settings.readReplica는 실제 파생 client가 사용하지 않으며 metadata에 명시한다. V06은 정책 selector가 Follower를 고르고, V07은 원래 client가 초기 Leader 외 첫 Follower를 고른다. 원본을 임의의 고정 Leader 읽기로 치환하지 않는다. V06의 write fallback50ms·V07의 write fallback50ms와 원본 기본10ms는 기존 client 동작이며 같은 값으로 바꾸지 않았다.

## 설정·재현 정보

`client.metadata(...)`를 사용하면 manifest.metadata.adapterConfiguration에 표시 ID·wire configuration·정책 hash/각 정책값·초기 membership/Leader·runtime readReplica/실제 읽기 정책·stream 수·wire namespace·총timeout·retry/상한/timer·socket timeout을 남긴다. V04 K/S와 V05 S/W는 별도 필드다. V06 client 정책은 서버 configuration hash와 구분하고 V07 client tuning은 wire protocol과 구분한다.

이 map은 client 연결 설정이며 완전한 host/서버 환경 inventory가 아니다. 예를 들어 V02/V03 서버 파일 worker 수·failure detector timeout, source tree/trace hash·heap/CPU/storage·clock uncertainty는 별도 환경/ready manifest에서 보완해야 한다. O00/V01의 기존7인자 metadata 호출은 빈 map으로 유지되므로 정식 비교 전 이들도 설정을 명시해야 한다. local manifest는 ready 분산 manifest.schema가 아니다.

현재 logical ID namespace와 세션은 LOAD/WARMUP/MEASURE별로 분리된다. 따라서 **V06 selector의 WARMUP RTT 학습 상태는 MEASURE의 새 client로 자동 이전되지 않는다.** JIT/서버 파일 cache 워밍업과 client selector 학습은 별개다. 정식 성능 시험 전 client 학습의 cold/steady 조건과 warm-up 정책을 명세로 확정해야 한다. 원래 private 상태를 복사하거나 알고리즘에 학습 이식 API를 추가하지 않았다.

V07 coalescer도 phase/stream client별이다. stream 수·동시성·E/D에 따라 batch 크기가 달라진다. 전체 load slot을 하나의 global batch queue로 표현하지 않는다. 소규모 고정률 fixture에서 모든 frame이 E개로 채워질 것을 요구하지 않으며, 별도 단일-session TCP burst로4개 요청→1개 frame을 확인한다.

## drain·오류·종료 계약

LocalVariantDrainProbe는 client 설정의 전체 membership과 명시적으로 전달한 같은 JVM 엔진을 대조한다. 모든 node의 정상 고정 View·건강 상태·recorded/ordered/commit/applied=알려진 성공 쓰기 목표를 요구하며, 초과 frontier·View 변경·누락 node는 실패다. 예상보다 작으면 WAIT다.

공통 client raw/publication/논리 대기와 파생 내부 대기도 확인한다. V03은 관찰된 Prepare send stage, V04는 apply gate active/ready key, V05는 관찰된 storage active/queue 추정, V06은 실제 adaptive active/raw READ, V07은 실제 coalescer queued/outstanding/inflight와 raw batch frame을 함께 본다. 모든 네트워크/OS/SSD가 완전히 quiescent하다는 증거는 아니다.

V07의 raw batch frame은 ObservedMetaBatchFrames에서 별도 전역 상한으로 관찰한다. 일반 RPC 상한과 batch frame 상한은 각각 runtime.maxOutstandingRpc이며 합산 단일 상한이 아니다. 실제 stage가 끝나야 permit을 반환한다. logical timeout/취소/close로 raw 미완료를 지우지 않는다. 외부에 반환한 stage 사본의 취소도 원본 raw 작업을 취소하지 않는다. 같은 ID의 중복 Meta 응답·누락 item·잘못된 envelope는 원래 client가 검증하며 성공 prefix를 먼저 게시하지 않는다.

오류/미응답을 정상 처리량·성공 latency로 바꾸지 않고 기존 histogram/표준 결과 계약을 따른다. write 반영이 불확실하면 apply 증명을 통과시키지 않는다. 독립 replica RAM log·stable View 영속화·선택적 KV force를 구분하며 이번 작업으로 SSD WAL/fsync나 동시 전원 손실 내구성을 추가하지 않는다. TCP는 원본대로 RPC별 새 socket이다.

## 로컬 기능 검증

- 새 비네트워크44검사: 여섯 client의 읽기/쓰기·ID/deadline retry·잘못된 reply·hung discovery·취소/close, 실제 V06 RR/ADAPTIVE/새 View, V07 batch4→1·잘못된 envelope·미완료 raw 및 drain·상한·stage 사본 취소를 검증한다.
- 실제 TCP48조건: 각 V02–V07 ×3/5노드 ×closed/open-loop ×GET100/GET40·SET30·DELETE30. LOAD4/WARMUP3/MEASURE12·streams4, open rates20/20/50·상한8, closed 동시성2다. 이전 fixture의37요청과 이번19요청을 성능 비교하지 않는다.
- 각 방식의 한 조건에서 별도7logical namespace의 SET/GET 값·DELETE OK/MISS·재삽입을 감사한다. 이42개 감사 요청은 앞선48개 run cohort에 포함하지 않는다.
- V02–V05의 다른 정책 profile4개 및 V07의 원본 wire 혼용1개를 거절하고, 적용되지 않은 write frontier0을 확인한다. V06 RR/ADAPTIVE와 V07 E/B/D/P/W 변경은 원래 허용된 client tuning이며 서버 wire와 혼동하지 않는다.
- 실제 TCP3/5노드의 V07 별도 burst2개에서 distinct logical4·fresh Meta4→raw frame1을 확인한다. batching의 일반 성능 이득이나 낮은 부하의 batch occupancy 보장은 아니다.
- 결과48개를 기존 schema·hash·완료 marker·cohort·histogram/percentile과 adapter 설정 필드로 대조한다. fixture의 p99.9 표본은10만개보다 작아 주요 비교 적합성은 false다.

처음 모델의 배치 delay1초가 총deadline1초와 같아 원래 긴급 flush가 발생해4개 frame으로 발송된 검사를 보존했다. fixture delay만100ms로 고쳐 동일한4→1 인수 조건을 검증했다. 알고리즘이나 성공 조건을 바꾸지 않았다.

새 TCP는 server-policy(V02–V05)와 read-policy(V06/V07) 두 그룹으로 실행하며 Windows의 기존10% 포트 회복 gate를 각 그룹 준비 단계에서 적용한다. PowerShell7 pwsh.exe가 PATH에 있어야 한다. 최대600초 회복 대기와620초 lifecycle 한도는 측정 밖의 준비 단계이며 각 test body는60초다. 미회복이면 실패하고 skip/PASS하지 않는다. Linux에서 Windows gate만 생략하고 테스트는 실행한다. 기존 V01/과거 V07 실패 이력은 이번 연결로 해결했다고 보고하지 않는다.

```powershell
# 비네트워크 검사
.\gradlew.bat :cache-server:variantBenchmarkTest --tests "*RemainingAdaptersTest" --tests "*RemainingBatchFramesTest" --tests "*RemainingDrainProbeTest" --offline --console=plain
# 새 TCP 포함 전체 연결 검사
.\gradlew.bat :cache-server:variantBenchmarkTest --offline --console=plain
# 기존 회귀까지 포함한 build/installDist
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1 -PortWaitSeconds 600
# 개별 결과 검증
pwsh -NoProfile -File .\experiments\replication-benchmark-v1\Validate-Run.ps1 -RunDirectory <새로운-run-경로>
```

최종 전체 수·실제 실행 이력/증거는 REMAINING_ADAPTERS_VALIDATION.json, 보존 목록은 REMAINING_ADAPTERS_SOURCE_PRESERVATION.json, 결과48개 대조는 REMAINING_ADAPTERS_ARTIFACT_CHECK.json을 따른다.

## 남은 작업

공통 정확성 이력/ACK audit·필수 자원/통신/저장소 지표·1초 창·변형 내부 지표의 표준 수집·실제 observer overhead·ready manifest/clock/분산 관찰을 보강해야 한다. 이후 공통 본시험 데이터/요청열 후보를 만들고, EC2 재개 시 inventory/pilot으로 크기·시간·rate·deadline을 확정한다. Hermes는 별도 구현/보장 조건 검증 뒤 편입한다. EC2 접속/진단·정식 성능 비교·commit/push/merge를 하지 않으며 기존 draft/execution_enabled=false를 유지한다.
