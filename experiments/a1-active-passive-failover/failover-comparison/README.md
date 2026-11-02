# A1 crash-only 기준형

## 구현 범위

`CrashOnlyPrimaryBackupEngine`이 기존 `PrimaryBackupEngine`의 동기 SET 복제를 조립한다.
공통 CacheEngine/Transport/TimeSource 계약은 변경하지 않았다. 실제 cache-server TCP 연결은 아직 없다.

1. 초기 Primary가 heartbeat를 보내고, Backup 중 NodeId가 가장 작은 노드가 timeout을 감지한다.
2. 후보는 옛 복제 엔진을 닫고 KV worker의 선행 적용 완료를 COUNT barrier로 기다린다.
3. 후보가 남은 Backup에 INSTALL(epoch + 1)을 보낸다. Backup도 선행 적용을 마치고 새 구성을 설치한다.
4. Backup의 READY와 후보 자신의 barrier가 모두 끝나야 새 Primary가 쓰기를 받는다. READY가 없으면 INSTALL을 재전송한다.
5. 새 구성은 Primary 1대 + Backup 1대다. 남은 Backup의 ACK 뒤에만 OK를 보낸다.
6. 클라이언트는 기존 노드 접속 실패 후 알려진 Backup을 조회하고, STALE_EPOCH에서 epoch를 갱신하며 Redirect를 따라 재시도한다.

외부가 1ms마다 TICK을 전달한다. 엔진은 스스로 thread/timer를 생성하지 않는다.
로컬 barrier 실패나 mailbox 포화로 전환이 끝나지 않으면 쓰기를 열지 않는다.

## 보장 범위와 전제

- 초기 3노드 중 최초 Primary 한 대의 영구 crash만 지원한다. 생존 Backup 둘은 동작하며 통신 가능해야 한다.
- heartbeat timeout은 장애 의심이다. 이 구현에는 quorum 선출, lease, 외부 fencing이 없어 분단·오판 안전성을 주장하지 않는다.
- 새 epoch를 설치한 노드는 옛 복제 메시지를 무시하고 stale 클라이언트 요청을 거절한다. 옛 Primary 자체를 외부에서 차단하지 않는다.
- 이전 OK 쓰기는 전원 ACK를 거쳤다. 하지만 진행 중/TIMEOUT 쓰기는 일부 복제본에만 적용될 수 있고 로그 합의·롤백·상태 동기화는 없다.
- 따라서 임의의 덮어쓰기 이력에 대한 선형화 가능성이나 완전한 crash recovery 구현이 아니다. 현재 실험의 보존 검사는 완료된 고유 키 쓰기를 대상으로 한다.
- 기존 엔진의 순서 있는 전달 전제를 유지한다. 데이터 메시지 재전송·순서 역전·중복 제거·두 번째 장애·재시작은 미지원이다.

## 27조건

| 변수 | 값 (가상 ms) |
|---|---|
| 메시지 단방향 지연 | 1, 5, 10 |
| heartbeat timeout | 50, 100, 200 |
| 클라이언트 재시도 간격 | 1, 10, 25 |

heartbeat 간격 10ms, timer tick 1ms. 각 조건마다 독립 KV 3개를 만들고 고유 키 SET 10건의 OK를 확인한 후 Primary를 종료한다.
crash 노드로 들어가거나 그 노드에서 나오는 미전달 메시지는 폐기한다. 종료 사실은 클라이언트에 즉시 UNAVAILABLE로 전달한다.
실제 소켓 접속 timeout과 클라이언트 통신 지연은 모델링하지 않는다. 프로토콜 노드 간 메시지만 편도 지연을 부여한다.

클라이언트는 순차 재시도하며 첫 OK callback 시각을 기록한다. 이미 완료된 callback을 다음 재시도 tick에서 관측하더라도 완료 시각은 늘리지 않는다.
원래 성공한 10개 키와 복구 후 새 키를 생존 저장소 둘에서 검사한다.

| 지표 | 구간 |
|---|---|
| detection | crash → 후보의 heartbeat timeout 감지 |
| transition | 감지 → 로컬 barrier와 생존 Backup READY 완료 |
| clientAndWrite | 준비 완료 → 클라이언트의 첫 OK callback |
| total | crash → 첫 OK callback (위 세 구간의 합) |

고정 지연 모델에서 전환은 INSTALL/READY 왕복인 `2 × 편도 지연`이다. 복구 시간 차이는 감지 위상과 재시도 정책도 포함한다.
처리량·실제 서버 지연·SSD 비용을 측정한 결과가 아니다. VR과의 비교는 후속이다.

## 실행과 산출물

저장소 루트에서 JDK 21로 실행한다.

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/failover-comparison/baseline-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/failover-comparison/baseline.properties
```

quick은 같은 27조건을 두 번 반복해 CSV 결정성을 확인하며, full은 한 번 실행해 runs/에 보관한다. 반복은 독립적인 성능 표본이 아니다.
원본 JSON의 조건·구간 합·보존 개수를 검증하고 CSV/Markdown, 완료 표식을 생성한다. 원본 누락·중복·schema 오류·보존 개수 위반 시 완료 표식을 제거한다.
일반 JSON 편집 형식이 아닌 backend 고정 schema만 읽는다. 다음 명령으로 기존 원본에서 재생성할 수 있다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.FailoverResearchMain --rebuild "<repeat-001 폴더>"
```

Runner가 실행 명령·설정·source/class hash를 기록한다. JSON과 manifest는 로컬 보관하고 CSV와 보고서는 기존 저장소 정책에 따라 Git 보관 대상이다.

실행 결과: [27조건 결과 분석](results.md).
