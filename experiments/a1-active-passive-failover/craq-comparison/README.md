# CRAQ 읽기 분산 비교

Jira: [S15P31D111-181](https://ssafy.atlassian.net/browse/S15P31D111-181)

[Terrace & Freedman, Object Storage on CRAQ, USENIX ATC 2009](https://www.cs.princeton.edu/courses/archive/fall10/cos597B/papers/craq-usenix09.pdf)의 강한 일관성 읽기 경로를 기존 Chain 엔진의 선택 모드로 구현했다. 논문의 전체 시스템 재현은 아니다.

## 구현

- 기본 생성자는 기존 `TAIL_ONLY` 모드다. 쓰기·결과 중계·Sent·외부 master의 crash 복구 경로는 유지한다.
- `ReadMode.CRAQ`는 모든 생존 노드에서 GET/EXISTS를 받는다. COUNT는 전체 key snapshot을 정의하지 않으므로 기존처럼 tail 전용이다.
- SET/DELETE의 KV 적용 완료 후 `CraqVersions`에 key별 버전을 기록한다. 프로토콜 버전은 전역 로그 위치이며 KV의 `valueVersion`과 구분한다. DELETE tombstone과 쓰기 이전의 부재(version 0)도 보존한다.
- tail 또는 최신 key 버전이 누적 tail Ack 이하면 로컬 응답한다. dirty면 `VersionQuery(epoch, queryId, key)`를 tail에 보내 확정 버전을 확인하고, 그 버전의 로컬 값을 반환한다. 값 자체를 tail에서 가져오지 않는다.
- tail 버전 조회는 읽기를 tail 처리 시점에 순서화한다. Ack 전의 dirty 상태도 이미 tail에 도달했다면 새 확정 값을 반환한다. 조회 직후 동시 쓰기가 확정될 수 있으므로 조회 당시의 이전 값을 반환하는 것은 허용된다.
- Ack 이후 최신 clean 이전의 읽기 버전을 정리한다. 진행 중인 조회가 있는 key는 해당 조회 완료/timeout까지 과거 버전을 보존하여 Ack와 늦은 조회 응답의 경합을 처리한다. 기존 진단 로그·쓰기 중복 응답 캐시는 원래처럼 보존하므로 전체 메모리 사용을 제한하는 checkpoint는 아니다.
- query ID·발신 tail·key·epoch·보존 버전을 확인한다. 1초 재전송, deadline 이후 TIMEOUT, 중복/늦은 응답 무시. 재구성 시작 시 대기 읽기를 UNAVAILABLE로 끝내고, freeze/repair 중에는 새 읽기를 받지 않는다.
- owner mailbox에서 상태를 변경한다. 빈 KV에서 시작하고 모든 쓰기가 이 엔진을 경유해야 한다. 미리 채운 KV/import, TTL/eviction은 지원하지 않는다. 공통 KV/API는 변경하지 않았다.

## 검증

`CraqTest`는 clean 로컬 읽기, 미전달 쓰기의 이전 값, Ack 지연 중 확정 값, 최초 삽입의 MISS, 삭제/재생성, 조회 중 버전 보존, 조회 재시도/timeout, 잘못된 응답, 세 위치 crash, 재구성 중 읽기 차단, 분단 후 복구, COUNT/쓰기 라우팅을 검증한다.

`CraqComparisonTest`는 full 32조건을 두 번 재생하여 결과 일치와 원본 round-trip을 확인한다. summary는 canonical JSON 검증과 조건 누락/중복 확인 후 재생성한다. 원본의 schema/결과 변조도 거절한다.

## 실험 조건

|축|조건|
|---|---|
|모드|Chain TAIL_ONLY / CRAQ|
|시나리오|CLEAN / IN_FLIGHT / ACK_DELAY / TAIL_CRASH|
|값 크기|64B / 8KiB|
|방향별 대역폭|1 / 10MiB/s|
|노드|고정 3개, tail crash 이후 생존 2개|
|읽기|같은 hot key에 GET 90개 동시 제출. Chain은 tail, CRAQ는 생존 노드에 round-robin|
|시간/실행|방향별 전파 1ms, 가상 시간, full 32조건 × 반복 2회|

- 정상 쓰기 1건 뒤 측정한다. IN_FLIGHT는 같은 key의 두 번째 쓰기를 middle에서 막아 tail까지 전달하지 않는다. ACK_DELAY는 두 번째 쓰기는 tail에 도달시키되 역방향 Ack만 막는다. TAIL_CRASH는 정확한 영구 crash 감지 200ms 및 freeze/복구/open 이후 측정한다.
- 버전 조회/응답의 논리 크기는 각각 `44 + key B`, `48 + key B`. 실제 wire codec은 아니다. 각 조건은 모든 읽기 응답의 값까지 검증한다.
- 기록 지표: 노드별 client 읽기, 로컬/dirty 처리 수, 버전 조회 수, 측정 중 peer bytes/messages, 성공 p50/p99.
- CPU/GC/디스크, client 링크와 값 응답 직렬화는 없다. **0ns는 모델상 즉시 로컬 응답**이며 실제 지연이나 처리량 결과로 해석하지 않는다. 이 실험은 경로·추가 메시지 비용 검증이며 처리량 벤치마크가 아니다.
- 전원 clean이면 tail의 client 읽기를 90→30으로 분산한다. 전원 dirty면 tail의 client 읽기 30 + 버전 조회 60이므로 tail 전체 작업 수가 줄었다고 주장하지 않는다.
- master 합의·분단 오판·살아 있는 노드 강제 제외·신규 가입·영속화/재시작·weak consistency 모드·쓰기 pipeline·실제 TCP는 범위 밖이다.

## 실행 및 재생성

저장소 루트, JDK 21, Windows PowerShell:

```powershell
.\gradlew.bat build :experiment-runner:installDist
& 'experiment-runner/build/install/openkache-experiment-runner/bin/openkache-experiment-runner.bat' 'experiments/a1-active-passive-failover/craq-comparison/comparison.properties'
```

빠른 실행은 `comparison-quick.properties`(8조건 × 2회)다. 원본과 manifest는 runs 아래에 남기며 summary/results만 버전 관리한다.

```powershell
java -cp 'cache-core/build/classes/java/main;cache-core/build/classes/java/test' io.openkache.core.replication.experiment.CraqResearchMain --rebuild '<repeat-001 폴더>'
```
