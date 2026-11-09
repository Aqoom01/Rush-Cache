# CRAQ 읽기·쓰기 혼합 부하

[S15P31D111-181](https://ssafy.atlassian.net/browse/S15P31D111-181)의 추가 실험이다. 기존 [clean/dirty 읽기 경로 32조건](../craq-comparison/README.md)은 그대로 두고, 동일한 요청열에서 실제 로컬 읽기 비율과 Tail 조회 비용이 어떻게 바뀌는지 측정한다.

## 질문과 방법

쓰기 비율만 높이면 CRAQ의 읽기 이점이 항상 같은 정도로 줄어드는가? 단일 hot key와 64개 key의 균등 접근, 값 크기, 확인 지연을 구분한다. Chain의 tail 읽기와 CRAQ의 전 노드 읽기를 비교한다. 프로토콜과 공통 API를 바꾸지 않고 테스트 fixture에 실제 KV 적용 관찰만 추가했다.

|항목|설정|
|---|---|
|노드/링크|3노드, 방향별 1MiB/s FIFO, 전파 1ms|
|입력|3초간 2ms마다 1건, 1,500건, open-loop, 재시도 없음|
|설정 쓰기 비율|0 / 1 / 5 / 10 / 25 / 50%|
|key 선택|동일 key 1개 / 64개 중 균등 선택. 읽기와 쓰기가 같은 key 집합 사용|
|값 크기|64B / 8KiB, payload에 고유 operation ID 포함|
|확인 지연|Tail에서 보내는 Ack·Completed 추가 0 / 20ms|
|seed|1 / 2 / 3, 모드별 같은 연산·key·값·도착 시각|
|라우팅|쓰기는 head, 읽기는 Chain tail / CRAQ 순환 배치|
|deadline / drain|요청 1초, 입력 후 미완료·Sent를 최대 10초 관측|
|총 조건|모드 2 × 쓰기 비율 6 × key 수 2 × 값 크기 2 × 확인 지연 2 × seed 3 = 288, 2회 반복|

초기 모든 key를 순서대로 복제하고 warmup 비용을 제외한다. 쓰기는 기존 단일 in-flight 엔진을 사용하며 **제출 비율과 성공 비율은 다르다**. 각 요청은 OK/BUSY/TIMEOUT/기타로 집계한다. 서로 다른 key 수는 충돌 범위를 바꾸기 위한 탐색 조건이며 실제 서비스의 분포라고 가정하지 않는다. 원 논문의 쓰기 pipeline 처리량을 재현하는 실험도 아니다.

## 검증과 지표

- 읽기: 로컬/dirty 처리 수, Tail 조회 시도 수, 노드별 client 읽기 수, 성공 p50/p99.
- 쓰기: 제출/OK/BUSY/TIMEOUT/기타, 입장한 로그 수와 Tail 적용 수, 성공 p50/p99.
- 통신: warmup 이후 drain까지 논리 peer bytes/messages. 초기·최종 관측 비용의 포함 범위를 고정했다.
- 읽기 안전성: 테스트 전용 관찰자로 실제 Tail KV 적용, GET 호출, 응답의 순서를 기록한다. 반환 값은 호출 시점의 최신 확정 값 또는 호출~응답 사이에 확정된 값이어야 한다. 호출 전에 이미 대체된 값이나 미확정 값은 거절한다. 단일 key SET/GET 실행 기록에 대한 검사이며 다중 key transaction·모든 가능한 실행에 대한 증명은 아니다.
- 모든 성공 쓰기는 Tail 적용 기록에서 명령·값을 확인한다. 최종 생존 로그 prefix와 적용 위치·Sent drain도 확인한다.
- canonical JSON에서 필드·값·조건 중복/누락을 검증한 뒤 CSV를 재생성한다. 잘못된 원본이면 completion 표시를 지운다.

CPU·실제 thread/GC·client 링크·값 응답 직렬화·디스크는 없다. 로컬 0ns와 모델 p99를 실제 지연·처리량으로 주장하지 않는다. 장애는 이 추가 실험에 주입하지 않았으며 기존 crash 검증과 분리한다.

## 실행

저장소 루트, JDK 21:

```powershell
.\gradlew.bat build :experiment-runner:installDist
& 'experiment-runner/build/install/openkache-experiment-runner/bin/openkache-experiment-runner.bat' 'experiments/a1-active-passive-failover/craq-mixed/comparison.properties'
```

quick 설정은 `comparison-quick.properties`(24조건 × 2회)다. full과 동일한 입력 기간·요청 수를 사용하고 조건 축만 줄인다.

```powershell
java -cp 'cache-core/build/classes/java/main;cache-core/build/classes/java/test' io.openkache.core.replication.experiment.CraqMixedResearchMain --rebuild '<repeat-001 폴더>'
python experiments/a1-active-passive-failover/craq-mixed/plot_results.py '<repeat-001 폴더>'
```

그래프 생성은 matplotlib가 필요하며 core production 의존성에는 추가하지 않는다. [결과 분석](results.md), [대표 조건 표](representative-results.md).
