# 고정 3노드 동기 SET 복제 비교

PrimaryBackupEngine과 실제 KvEngine 구현을 in-process로 실행한다. 동시 진행 요청은 1개이며 실제 서버·thread 스케줄링·SSD·TCP 성능은 측정하지 않는다. 승격·분단·VR 비교는 후속이다.

검증된 실행과 주요 관측은 [결과 분석](results.md)에 정리했다.

## 실행

저장소 루트에서 JDK 21로 실행한다.

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/replication-comparison/baseline-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/replication-comparison/baseline.properties
```

- quick: 27조건 × 조건당 10요청, 전체 묶음 2회 반복. build/a1-replication-quick에 저장한다.
- full: 27조건 × 조건당 100요청, 전체 묶음 1회. 이 폴더의 runs/에 저장한다.
- JUnit은 대표 조건·deadline 경계·재현·보고서 재생성·원본 누락/변조 검출을 확인한다. 전체 실험은 Runner로 별도 실행한다.
- 공통 Runner의 source/class hash 및 완료 계약을 그대로 사용한다. [연결 계약](../../research-backends.md).

## 실험 조건

|변수|값(가상 ms)|
|---|---|
|기본 단방향 메시지 지연|1, 5, 20|
|Backup 2의 추가 ACK 지연|0, 20, 100|
|클라이언트 deadline|50, 200, 500|

조건마다 새 3노드와 독립 저장소를 만든다. 고유 키 SET을 순차 제출하고 요청마다 timer와 늦은 메시지를 모두 배출한 다음 다음 요청을 시작한다. timeout 직후 재시도나 지속 부하를 모델링하지 않는다. 키/값은 key-i/value-i UTF-8 문자열이며 바이트 크기에 따른 전송 비용은 없다.

단일 이벤트 큐가 시각과 삽입 순서로 실행을 결정한다. 같은 시각에서는 삽입 순서이며 deadline을 먼저 예약한다. 엔진도 deadline 이상이면 TIMEOUT으로 판단한다. DirectMailbox로 로컬 적용 및 callback은 즉시 처리하므로 모든 실행은 한 thread에서 재현된다. 난수가 없으므로 seed 반복을 통계 표본으로 만들지 않는다.

## 측정 및 검증

- 응답 지연은 요청 제출부터 실제 응답 callback까지 측정한다. 이벤트 배출 종료 시각은 쓰기 지연에 포함하지 않는다.
- 성공 수와 TIMEOUT 수를 분리한다. 다른 응답은 실험 실패다.
- 성공 응답한 쓰기는 세 저장소 모두에서 값이 일치해야 한다.
- TIMEOUT 후에도 값이 남은 요청을 별도 기록한다. TIMEOUT은 롤백이 아니다.
- 메시지 수는 Replicate와 ACK를 포함한다. client 통신은 모델에 없다.
- 고정 모델에서 성공 지연은 2 × 기본 지연 + 추가 ACK 지연이며 deadline보다 엄격히 작을 때만 성공한다. 측정값을 이 예상식과 독립 대조한다.
- 동일 조건 내 요청 지연은 같으므로 p50/p99를 별도 성능 근거로 제시하지 않는다. 처리량도 계산하지 않는다.

## 결과

반복 폴더에 results/*.json, summary.csv, results.md, completion.properties를 생성한다. 완료 표식은 27개 원본의 조건·개수·지표·보존 검사 후 마지막에 쓴다. source hash, 실행 명령, 설정과 실행 식별자는 Runner의 launch.properties 및 manifest.json에 기록된다. 원본 JSON은 고정 schema만 지원하며 일반 JSON 편집기로 재포맷한 파일은 재생성에서 거절할 수 있다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.ReplicationResearchMain --rebuild "<repeat-001 폴더>"
```

Git에는 기존 공통 정책에 따라 반복별 summary.csv/results.md만 포함한다. 원본·실행 metadata는 로컬에 남으므로 공유 시 run 폴더 전체를 별도 보관한다. full/quick은 부하 규모가 다른 성능 비교가 아니라 동일 모델의 실행·재현 검증이다.
