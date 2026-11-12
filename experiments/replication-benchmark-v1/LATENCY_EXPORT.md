# 전수 지연 histogram·로컬 결과 저장

기준일: 2026-10-30. 기존 benchmark 실행기에 전수 latency collector와 `replication-result-v1` 결과 저장기를 연결했다. 단일 JVM의 로컬 기능 검증 범위이며, 정식 분산 성능 비교·36개 지표군 전체 계측 완료를 의미하지 않는다.

## 실제 코드와 연결

`experiment-runner/src/benchmark/java/io/openkache/runner/benchmark/measurement/`에 LatencyHistogram, LatencyCollector, BenchmarkRunWriter와 JDK 전용 JSON 인코더를 둔다. 기존 production Runner의 reservoir/ResultWriter와 독립적이며 외부 production 의존성을 추가하지 않는다.

OpenLoopRunner의 기존 Observer를 재사용한다. ClosedLoopRunner에는 선택적인 Observer 생성자와 검증된 outcome 전달을 추가했다. 기존 생성자는 기본 비활성 observer로 위임하며 기존 실행·재시도·기본 테스트 조건은 유지한다. 원본/파생 알고리즘·공통 API·wire·TCP·Gradle은 변경하지 않는다.

예를 들어 이미 준비한 `generator`, `plan`, `client`, `probe`로 다음 Java API를 호출한다. `clockId`는 실행 JVM의 monotonic clock을 식별하는 고유한 값이어야 한다. collector를 생성해 실제 runner에 연결하고 run이 반환된 뒤에만 freeze/export한다.

```java
var collector = new LatencyCollector(plan.runId(), plan.loadSlot(), clockId,
        java.util.concurrent.TimeUnit.HOURS.toNanos(1));
var result = new OpenLoopRunner(generator, plan, client, probe, collector).run();
var metadata = new BenchmarkRunWriter.Metadata("O00", "local-functional", 3,
        sourceCommit, sourceDirty, clockId,
        java.util.Set.of("M01", "M02", "M03", "M04", "M10", "M11"));
var artifact = BenchmarkRunWriter.write(newRunDirectory, metadata,
        generator.config(), plan, result, collector.freeze());
```

ClosedLoopRunner도 같은 collector를 선택적 다섯 번째 인자로 받으며 writer의 해당 overload를 사용한다. 실행 예제/검사는 `HistogramO00TcpTest`와 `BenchmarkRunWriterTest`다. 새 CLI·원격 launcher·ready manifest loader는 제공하지 않는다.

## population과 시간

- LOAD/WARMUP/MEASURE × GET/SET/DELETE × SUCCESS/ERROR_RESPONSE/NO_RESPONSE를 분리한다. 오류/미응답은 reason과 response status도 분리한다. 성공 GET HIT/MISS는 동일 semantic-success population에 포함한다.
- SERVICE: terminal−첫 client invocation. 발견·재시도·adapter 대기와 publication 경과를 포함한다.
- SCHEDULED: terminal−due. open-loop의 고정 예정 시각 또는 closed-loop의 생성 시작 시각부터 잰다.
- DISPATCH_LAG: 첫 client invocation−due. 결과를 확인한 수락 요청의 lag이며 drop/거절을 latency0으로 넣지 않는다. 모든 후보의 decision/submission lag와 drop·미발행은 execution.json의 기존 open-loop 결과에서 별도로 보존한다.
- NO_RESPONSE는 `censored=true`인 관찰 경과 분포다. 성공 latency와 섞거나 실제 서버 완료 시간으로 표현하지 않는다. 재시도2회는 logical latency1개와 attempts2개다.
- 모든 duration은 같은 System.nanoTime domain의 ns다. UTC·다른 host timestamp를 빼지 않는다. ID/op/clock이 맞지 않거나 collector가 예외를 내면 실패로 남긴다.

callback에서 disk I/O나 요청 전수 이력 보관을 하지 않는다. 한 collector는 한 run/slot 전용이며 synchronized 집계 후 불변 snapshot으로 마감한다. 완료 ID 전수 dedup/전체 이력 검사기는 아니고 실행기의 exactly-once callback 계약 및 phase/op별 outcome·attempt 계수 대조를 사용한다. 관찰 누락·추가·불일치는 유효 결과로 만들지 않는다. 기본 callback 비용과 lock 경합은 향후 observer on/off pilot에서 별도 검증해야 한다.

## Histogram·percentile

METRICS.md의 `ns-ceil-1pct-v1` recurrence를 사용한다: 0, 1ns부터 `u + max(1, ceil(u/100))`. 관찰값 이상의 최소 upper bound에 넣으며, 설정 range 이하의 실제 마지막 finite 경계를 함께 기록한다.

- 모든 관찰값을 sparse upper_bound_ns/count에 넣고 count·정확한 sum/min/max·평균을 보존한다. sum은 BigInteger로 long overflow를 피한다. 요청 수에 비례하는 raw latency 배열/reservoir를 사용하지 않는다.
- p50/p95/p99/p99.9는 `ceil(p×n)`의 nearest-rank bucket 상한이다. raw percentile이나 신뢰구간이 아니다. 빈 histogram은 통계 null/no_samples이며, exporter에 없는 population도 0ns 표본이 아니라 관찰 표본 없음이다.
- range 초과는 overflow_count로 보존한다. target rank가 overflow에 있으면 percentile은 null/out_of_range다. 관찰 count/sum/min/max에는 range 초과도 포함한다.
- p99.9는 n<100000이면 raw 상한 추정치를 보존하되 comparisonEligible=false/insufficient_samples로 표시한다. 다른 percentile도 n과 bin 오차를 반드시 함께 본다. eligible=true가 통계적 신뢰도나 정식 성능 적합성 인증은 아니다.
- histogram count=bucket counts+overflow가 되어야 한다. phase/op/outcome·시간 의미가 다른 분포를 합치거나 run/창 percentile을 평균해 pooled percentile로 만들지 않는다. 분산 merge·1초 창 분포·paired 반복 분석은 후속이다.

## 저장 파일과 실패 보존

writer는 종료된 결과를 **존재하지 않는 새 디렉터리**에 저장한다. 부모 폴더는 준비되어 있어야 한다. 기존 결과를 덮어쓰지 않으며 IOException을 성공으로 변환하지 않는다. 각 파일을 CREATE_NEW `.part`에 쓰고 atomic move로 게시한다. 종료/전원 손실에 대한 fsync 내구성 보장은 제공하지 않는다.

| 파일 | 실제 내용 |
| --- | --- |
| manifest.json | benchmark-local-manifest-v1, run/slot·trace 전체 설정/seed·실행 계획/limits·mode·variant/scenario/node 수·source commit/dirty·required metrics·제약 |
| environment.json | 실제 client Java/VM/OS/arch·clock ID/방법. 서버 환경·storage kind·host 간 clock error는 미관찰 null |
| execution.json | 실행기의 phase/op·예정/미발행/손실·client snapshot·정리/적용 관찰·발행/서비스 상태를 포함한 원래 종료 결과 |
| histograms.jsonl | population·clock·bin version/range·count/sum/min/max/mean·sparse buckets·overflow·각 percentile/표본 적합성 |
| counters.csv | phase/op별 considered/admitted·성공/오류/미응답·미수락/미확정·attempt/retried logical |
| events.jsonl | stage/drain/cleanup 사건, run/clock ID와 monotonic 시각 |
| result.json | 기존 result.schema.json을 따르는 envelope. MEASURE admitted cohort·36개 지표 상태·실행/수집 상태·manifest/artifact SHA-256 |
| integrity.json | result를 포함한7파일 hash·collector 대조 여부·invalid 사유·clock/overhead 한계. correctness verdict는 not_evaluated |
| STARTED / COMPLETE | 시작/완료 marker. COMPLETE는 integrity.json hash를 담는다. COMPLETE가 없으면 부분 결과다 |

COMPLETE는 파일 저장 완료 표시다. INVALID run도 실패 증거를 온전히 저장하면 COMPLETE가 있으며, validity나 서비스 정상 여부가 PASS가 되는 것은 아니다. result.elapsed_ns는 run 전체 관찰 시간이고 성능 throughput의 분모로 사용하지 않는다. cohort는 MEASURE만 포함하고 LOAD/WARMUP과 drop/거절은 별도 기록이다.

현재 M01/M02/M03/M04/M07/M08/M10/M11의 로컬 관측 부분을 제공한다. M02는 client execute 시도이며 protocol RPC/message와 다르다. M07/M08은 MEASURE admitted를 분모로 하며 admission 거절은 밖의 계수다. 분모0이면 null+사유다. M09는 unresolved/lifetime late 일부만 execution.json에 있으므로 완전한 phase별 ledger로 표시하지 않는다. 나머지 지표는 unsupported/null+사유다. 설정한 required metric이 measured가 아니면 INVALID다. 정식 비교에는 시나리오의 전체 필수 지표와 수집 범위·clock·overhead 적합성을 다시 요구해야 한다.

manifest는 **로컬 관찰 설정 snapshot**이며 ready 분산 manifest.schema.json과 다르다. trace hash·source tree hash는 현재 미수집 null이다. source commit/dirty는 호출자가 실제 확인값을 제공하며, 모르면 null이다. plan.template.json의 draft/execution_enabled=false를 변경하지 않는다. 완전 correctness history/ACK ledger·requests/history/window 파일을 생성한 것으로 주장하지 않는다.

## 실행·검사

프로젝트 루트에서 Java21을 설정하고 실행한다.

```powershell
.\gradlew.bat :experiment-runner:benchmarkTest --tests "*measurement*" --offline --console=plain
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1 -PortWaitSeconds 60
pwsh -NoProfile -File .\experiments\replication-benchmark-v1\Validate-Run.ps1 -RunDirectory <새로운-로컬-run-경로>
```

Validate-Run.ps1은 파일 schema·hash·완료 marker·지표 단위/필수 지표·cohort·histogram population/bin/percentile·clock 참조를 확인한다. INVALID 결과를 검증해도 출력 result_validity는 그대로 invalid이며 성능/정확성 합격을 뜻하지 않는다. 형식/해시가 자체 일관된 악의적 원시 관찰의 진실성이나 payload의 선형성을 검증하는 도구는 아니다.

이번 실제 검사 수·증거·보존 목록과 독립 artifact/음성 대조 결과는 MEASUREMENT_VALIDATION.json, MEASUREMENT_SOURCE_PRESERVATION.json, MEASUREMENT_ARTIFACT_CHECK.json을 따른다. EC2 접속/진단·공통 성능 비교·commit/push/merge는 수행하지 않았다.

## V02–V07 연결 metadata 후속 (2026-10-30)

Metadata의 선택적8번째 인자 `Map<String,String> adapterConfiguration`을 추가해 정책/profile·client 제한/socket timeout을 정렬된 불변 map으로 보존한다. 기존7인자 생성자는 빈 map으로 호환되며 기존 API 호출은 수정하지 않는다. 여섯 adapter의 client.metadata(...)가 이 필드를 채운다. 전체 서버 inventory·ready manifest를 대신하지 않으며 O00/V01의 기존 빈 metadata도 정식 비교 전에 보완해야 한다.

공통 연결은 O00/V01–V07까지 로컬 검증됐다. V06 phase별 학습·V07 client별 coalescer 범위와 새 검사 task/증거는 ADAPTERS_V02_V07.md를 따른다. histogram/p99.9·미응답·실패/저장 완전성 계약은 그대로 유지한다.
