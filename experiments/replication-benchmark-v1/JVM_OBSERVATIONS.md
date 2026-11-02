# P02 JVM 자원·요청 창 관찰

2026-11-02 · Jira [S15P31D111-164](https://ssafy.atlassian.net/browse/S15P31D111-164), 5 SP.

Java21의 process CPU 누계, heap/non-heap 사용·예약·최대 bytes, GC 누계 count/time을 독립 benchmark 영역에서 수집한다. 기존 closed/open 실행기에 선택적 lifecycle을 추가했으며 기존 생성자는 수집기를 생성하지 않는다. 원본·파생 replication 알고리즘, core API, wire, 기본 TCP 설정, 생산 main 영역은 변경하지 않는다.

## 관측의 의미

| 관찰 | 의미·제약 |
| --- | --- |
| CPU | JDK process CPU ns 누계/차이값. elapsed time이나 CPU utilization이 아니다. |
| Memory | heap/non-heap used/committed/max bytes. JVM이 max를 제공하지 않으면 null+reason. RSS가 아니다. |
| GC | 모든 GC MXBean의 count/time 누계. 하나라도 미지원이면 해당 합계도 미지원. ms 시간 출처를 ns로 변환하며 개별 pause 분포/p99가 아니다. |
| RSS·allocation·개별 GC pause | unsupported, value=null과 개별 reason. 거짓0을 저장하지 않는다. |
| Client | GET/SET/DELETE의 admitted/success/error/no-response/attempt 누계와 pending endpoint gauge. source 수집과 client snapshot은 원자적 동시 snapshot이 아니다. |
| Peak pending | 관측 endpoint 중 최댓값인 하한. true peak는 null. runner의 별도 driver peak와 혼동하지 않는다. |

process_instance_id는 JVM 전체에서 하나를 공유한다. 한 JVM에 3/5 replica와 부하를 배치한 로컬 fixture는 SHARED_FIXTURE이며 CPU/heap/GC를 replica 수로 곱하거나 노드별 비용으로 표시하지 않는다. LOAD_PROCESS/SERVER_PROCESS 역할도 명시할 수 있으나 이번 source는 현재 JVM만 관측하며 원격 에이전트는 없다. 다른 프로세스의 nanoTime을 직접 빼지 않는다.

## 시각·창·수명주기

1. `RunLifecycle.stage`는 PREFLIGHT/LOAD/LOAD_DRAIN/WARMUP/WARMUP_DRAIN/MEASURE/FINAL_DRAIN/CLEANUP의 요청 시각을 받는다. 이전 segment의 실제 snapshot을 닫고 새 stage로 전환한다. 전환 요청/관측 완료 시각을 모두 남기므로 경계 관찰 비용·지연을 숨기지 않는다.
2. 자원은 1초 **목표** 주기의 ticker와 stage 경계·최종 snapshot에서 수집한다. 실제 collection start/end, source start/end, 연속 source-end interval을 보존한다. 지연된 ticker를 정확히 1초 실측으로 보간하지 않는다. 첫 baseline delta는 null이며 마지막 짧은 구간도 남긴다. 경계 관찰 중 전환이 이루어지므로 stage는 관찰 segment의 라벨이며 무오차 CPU phase 귀속을 보장하지 않는다.
3. 요청 창은 전역 run anchor 기준 1초 `[start,end)`에 **원본 terminal timestamp**로 배치한다. LOAD/WARMUP/MEASURE cohort는 별도 필드다. 늦게 호출된 callback도 원래 terminal 창·cohort를 유지한다. 종료 창만 짧게 자르며 빈 sparse bucket을0으로 해석하는 것은 전체 관찰이 valid일 때뿐이다.
4. 요청 창과 실제 자원 interval은 다른 시계열이다. 요청 cohort 성공 수로 임의 자원 interval을 나누거나 시간 비례로 CPU/op를 만들어내지 않는다. P01의 phase 구분은 이 두 시계열과 경계 증거로 보존한다.
5. source/client snapshot/clock provider는 비차단 계약이다. callback은 management API/파일 I/O를 호출하지 않으며 짧은 수집 종료 gate와 bounded bucket만 사용한다. 비협력 native source를 강제 종료하는 장치는 아니다. endpoint sampling/실험 observer 자체 비용은 포함되며 overhead 적합성은 P09에서 확인한다.
6. 최대 sample/bucket 수가 있으며 넘치면 eviction하지 않고 invalid로 실패한다. process 교체·counter reset·clock 역전·필수 source 누락도 invalid다. reset을0으로 보정하지 않는다. 종료 시 ticker 취소·executor bounded await·최종 snapshot·callback freeze를 수행한다. 이미 반환된 snapshot은 불변이고 종료 후 callback은 새 snapshot에서 late 문제로 드러난다.

## 연결 방법

`experiment-runner/src/benchmark/java/io/openkache/runner/benchmark` 아래 `execution/RunLifecycle.java`, `measurement/JvmSnapshotSource.java`, `JvmWindowCollector.java`, `JvmRunWriter.java`를 사용한다. 실제 예시는 runner의 `JvmRunnerIntegrationTest`와 server의 `JvmBenchmarkTcpTest`에 있다.

```java
var resources = new JvmWindowCollector(
    JvmWindowCollector.Settings.local(runId, slot, clockId,
        JvmWindowCollector.Role.LOAD_PROCESS),
    JvmSnapshotSource.system(), client::snapshot);
// 기존 latency observer를 함께 호출한다. 둘 중 하나를 생략하지 않는다.
var result = new ClosedLoopRunner(generator, plan, client, probe,
    (request, due, outcome) -> {
        latency.outcome(request, due, outcome);
        resources.outcome(request, due, outcome);
    }, resources).run();
var export = JvmRunWriter.write(newDirectory, generator.config(), plan,
    result, resources.snapshot());
```

open-loop도 해당 observer overload와 같은 lifecycle을 받는다. 수집 실패 시 runner가 실패를 기록하면서 client cleanup을 수행한다. 실행 결과와 관찰 snapshot을 함께 writer로 넘겨 cohort·최종 client·필수 stage·process/clock·derived delta를 대조한다. outcome observer를 연결하지 않은 실행은 정상 처리되더라도 sidecar가 invalid다.

## 저장·독립 검사

새 디렉터리에만 STARTED, samples.jsonl, request-windows.jsonl, execution.json, jvm-result.json, integrity.json, COMPLETE의7파일을 저장한다. integrity의4개 데이터 파일 SHA256과 COMPLETE의 integrity SHA256을 대조한다. 완료 marker는 **저장 완료**이며 서비스 정상/관찰 valid/정식 성능 적합성과 독립이다. 원자적 rename을 사용하지만 fsync나 power-loss 보장은 주장하지 않는다. 기존 성능 bundle과 이름·디렉터리를 공유하지 않는다.

`validate_jvm.py`는 Python 표준 라이브러리만 사용하는 독립 reader다. hash/파일 목록·run/clock·실제 interval·CPU/GC delta·client 보존식·phase별 요청 창·최종 계수·중복 프로세스를 대조한다. invalid 진단 bundle은 저장과 명시 invalid를 확인하며 qualified로 승격하지 않는다. hash는 인증 서명이 아니다.

```powershell
$env:JAVA_HOME='C:\Users\SSAFY\.jdks\ms-21.0.11'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
.\gradlew.bat :experiment-runner:benchmarkTest --tests '*Jvm*' :cache-server:jvmBenchmarkTest --offline --console=plain
python .\experiments\replication-benchmark-v1\validate_jvm.py <bundle-directory>
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1 -PortWaitSeconds 600
```

Windows TCP fixture는 시나리오 준비 단계에서 기존10% 동적 포트 예산이 회복되기를 기다린다. 테스트를 skip하거나 protocol timeout·정상 판정을 완화하지 않는다. 새 JVM task만 기존 correctness 그룹 뒤에 배치한다. 최종 실행·산출물·보존 수치는 JVM_OBSERVATION_VALIDATION.json, JVM_OBSERVATION_ARTIFACT_CHECK.json, JVM_OBSERVATION_SOURCE_PRESERVATION.json을 따른다. 초기 실패는 별도 증거로 보존하며 재시도 성공이 과거 socket 예외의 원인 확정은 아니다.

## 완료 경계와 다음 단계

P02는 현재 JVM의 원시 관찰과 두 실행기 연결이다. 표준 result의 M18/M23은 필수 replica/역할/clock/coverage·분모와 프로필 통합이 끝나기 전 unsupported를 유지한다. 부분 sidecar를 all-replica CPU/op나 RSS 포함 memory 측정 완료로 표시하지 않는다. 기존 표준 writer의7파일 계약·schema·생산 JAR/배포 분리를 유지한다.

다음은 P03 통신, P04 파일 I/O/force, P05 내부 진단, P06 V06 cold/steady·워밍업, P07 correctness 통합이다. P08에서 공통 dataset/request trace 후보를 키 수·키/값 크기·비율·분포·seed·요청수로 정의하고 내용/순서 hash를 고정한다. P09 통합/overhead 검사와 로컬 정확성 검사를 통과한 뒤 사용자가 EC2 작업을 재개하면 inventory·용량/시간 pilot으로 본시험 규모를 확정한다. 각 방식은 같은 초기 상태·요청 trace·환경·반복 seed를 쓰며 실행마다 저장소/학습 상태를 명시적으로 준비한다. 현재19요청 fixture는 기능 검사용이며 본시험 데이터셋이 아니다.

독립 replica RAM 복제 log의 durability, stable View force, 선택적 KV force는 그대로 구분한다. EC2/SSH·정식 성능 비교·Hermes 구현·commit/push/merge는 이 단계에서 수행하지 않는다. Jira 완료는 팀 기준상 MR/병합 확인 후이며 구현·검증이 끝나도 그 전에는 진행 중으로 기록한다.

## 최종 검증 증거 (2026-11-02)

최종 집계는899개 검사 통과(추가59개: 단위·모델27/TCP32), 실패·오류·skip0이다. 전체 명령은 마지막 JVM 그룹에서 동시 창 생성 한도를 수정하기 위해 중단했다. 변경 없는6그룹719개 XML과, 수정 후 다시 검사한 runner148/JVM_TCP32 및 installDist를 합쳐 기록한다. 중단한 failed manifest는 그대로 보존하며 단일 전체 명령 통과로 표시하지 않는다. 원자적 per-key 생성은 CHM을 사용하고 종료 후 출력만 정렬한다. cap1·동시4생산자·2000결과 검사를 통과했다.

JVM 정상 bundle34개·명시 invalid2개 및 변조6개, 총42건을 독립 검증했다. 기존 source baseline301개 중299개 동일·실험 실행기2개 수정·새Java9개다. 마지막 수정 전후에는 새 collector/test2개만 달라졌다. 생산 application JAR의 benchmark class0·배포 benchmark JAR0도 확인했다. 실제 XML·실패 이력·실행 명령·정확한 hash는 위3개 JSON 보고서를 따른다.

최종 영향 검사 재현 명령은 `gradlew :experiment-runner:benchmarkTest :cache-server:jvmBenchmarkTest :cache-server:installDist --offline --console=plain`이다. 전체 회귀를 처음부터 재실행하려면 앞 절의 Run-Validation 명령을 사용한다. Windows의 기존 포트 준비 gate 대기를 포함하므로 검증 소요 시간을 알고리즘 처리량/지연으로 사용하지 않는다.

데이터 생성 CLI/형식은 [TRACE_GENERATOR.md](TRACE_GENERATOR.md)를 따른다. hash-only/파일 생성은 서버 요청 실행이 아니며, 실행 시 같은 설정/seed/index·stream mapping과 canonical trace hash를 대조해야 한다. closed-loop의 완료 기반 실제 발행 시각/interleaving까지 같다고 주장하지 않는다.

