# Chain Replication 비교

[Chain Replication, OSDI 2004](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)의 head→middle→tail 쓰기, tail 읽기·확정, 역방향 Ack와 장애 후 사슬 연결을 구현했다. CRAQ는 포함하지 않았다.

## 구현한 경로

- SET/DELETE는 head만 받는다. 각 노드는 로컬 KV 적용을 완료한 뒤 successor로 Forward한다.
- tail KV 적용이 확정이다. tail은 Ack를 사슬 역방향으로 보내고, 각 노드는 Sent(적용했지만 아직 tail Ack가 없는 suffix)를 유지한다.
- 공통 CacheEngine의 요청 future는 요청을 받은 head에 있다. 따라서 tail은 작은 Completed 결과를 **head에 한 번 중계**하여 future를 완료한다. 실제 tail→client 연결은 없다. 이 추가 peer 홉을 비용에 포함한다.
- 읽기(GET/EXISTS/COUNT)는 tail만 처리한다. 잘못된 노드의 쓰기는 head, 읽기는 tail로 Redirect한다.
- client/operation별 쓰기 중복 응답을 제공한다. 같은 ID의 다른 명령은 BAD_REQUEST다. TIMEOUT 뒤 쓰기가 적용·확정될 수 있다.
- 데이터는 index 순서대로 한 항목씩 전송한다. 중복은 재적용하지 않고, 구멍이 있으면 적용 위치를 알린다. Progress는 successor 적용 확인이며 **tail 확정 Ack로 사용하지 않는다**.
- 단일 in-flight 기준이다. head가 tail의 직접 결과를 받았더라도 역방향 Ack가 돌아오기 전 다음 새 쓰기는 BUSY일 수 있다. 이때 공통 클라이언트의 재시도 비용도 집계한다.
- 로그와 결과는 메모리 보관, 재전송 주기는 1초다. 모든 상태 변경은 owner mailbox를 따른다.

## 외부 master의 전제와 복구

**신뢰하는 단일 실험 코디네이터**가 실제 영구 crash를 200ms 뒤 통지받는다. 이 감지는 정확한 장애 oracle 모델이다. 실제 heartbeat 오판·master 합의·장애를 구현한 것이 아니다. 살아 있는 분단 노드를 제거하지 않는다.

복구는 아래 순서를 따른다.

1. 생존자 전부에서 입장을 닫고 이미 수락한 KV 적용을 끝낸다. 새 epoch와 생존 순서를 설치하고 FROZEN barrier를 기다린다.
2. 모든 생존자가 freeze한 뒤 REPAIRING으로 전환한다. successor의 적용 위치를 받고, 앞쪽 노드의 Sent suffix를 **실제 peer 경로로** 재전송한다. master가 데이터를 복사하지 않는다.
3. 새 tail은 자기 적용 항목을 확정한다. Ack를 역전파한다. head/middle/tail 제거 모두 같은 경로를 사용한다.
4. 전 생존자의 log/applied/ack 길이가 일치하고 prefix가 같은 것을 확인한 뒤 입장을 연다.

master freeze/open 조정은 각각 2ms로 모델링하며 데이터 링크 통계에서 제외한다. 따라서 failover에는 200ms 감지와 이 조정 시간이 포함되지만 master의 네트워크 bytes·합의 비용은 포함되지 않는다. master 중단 중에도 기존 사슬의 정상 처리 자체는 가능하지만 **재구성 진행은 master에 의존**한다.

제거 대상이 실제로 살아 있다면 epoch만으로 격리된 옛 사슬을 차단하지 못한다. 이 구현의 구성 설치 API를 임의의 partition 제거 절차로 사용하면 안 된다. 재시작·새 노드 추가·master 합의·CRAQ·영속화·checkpoint·pipeline 확대는 범위 밖이다.

## 실험

### 공통 비교 288조건

PB/VR/Raft/Chain × 6시나리오 × seed 3 × 값 64B/8KiB × 1/10MiB/s. 기존 [Raft 비교](../election-comparison/README.md)의 PB·VR·Raft 조건과 클라이언트 정책은 그대로 실행한다. Chain은 난수를 사용하지 않으므로 Chain의 seed 세 개는 동일 조건의 반복이며 독립 난수 표본이 아니다.

|시나리오|Chain 주입·가정|
|---|---|
|NORMAL|정상 사슬|
|SLOW_BACKUP|tail(node 2)의 Ack 및 완료 중계에 +200ms|
|CRASH|head 영구 종료 후 master 재구성|
|PARTITION|head 격리. master는 재구성하지 않음. 새 쓰기 시도 후 repair|
|INFLIGHT_CRASH|Ack·완료 중계를 막고 SET 제출 40ms 뒤 head 종료|
|FOLLOWER_REJOIN|middle 격리 후 논리 쓰기 4건 시도, repair 및 2초 대기|

공통 네트워크는 방향별 FIFO 직렬화 + 1ms 전파 지연이다. 준비 SET 64건 이후 새 논리 SET 16건을 순서대로 보낸다. 시도 deadline 최대 1초, 실패 후 재탐색 10ms, 논리 쓰기당 5초 예산이다. Chain PARTITION에서 새 쓰기가 성공하지 않아도 실험 실패로 숨기지 않고 0건으로 기록한다.

첫 OK는 새 요청 성공 callback의 정확한 가상 시각이다. `successP50Ns/p99Ns`는 성공한 **마지막 시도** RTT라 BUSY/재시도 대기를 포함하지 않는다. attempts·상태별 count와 첫 OK 지연을 함께 본다. 전체 관측 시간은 방식마다 달라 enqueuedBytes/messages를 무조건 동일 시간당 비용으로 비교하면 안 된다.

PB는 생존 KV, VR/Raft/Chain은 생존 로그의 성공 ID·명령·값을 검사한다. KV 일치와 성공 로그 보존은 별개다. 분단 repair 뒤 최종 6초를 더 관측한다.

### Chain 위치별 crash 24조건

head/middle/tail × 미완료 쓰기 유무 × 값 크기 2 × 링크 속도 2다. 정상 SET 16건 뒤 위치별 노드를 종료하고 첫 새 쓰기까지 측정한다. pending=1이면 Forward를 모두 막아 SET 17이 head에만 적용된 시점에 종료한다.

- head 종료: 아직 전달하지 못한 쓰기는 사라질 수 있다. 성공 응답한 16건은 생존한다.
- middle/tail 종료: 살아 있는 head의 미확정 suffix가 복구·확정될 수 있다.
- 검사: 성공 16건의 생존 로그, 생존 KV 복사본 32개, 최종 Sent 0개, 감지/복구/첫 OK 시각.

이 24조건은 `position-results/*.json` 원본 및 `position-summary.csv`, `position-results.md`로 별도 집계한다. 공통 Runner의 `cases=288`은 주 비교만 세며 **위치별 24조건을 포함한 합계는 312조건**이다. quick도 위치별 24조건은 전부 실행한다.

### 논리 bytes 모델

기존 모델은 그대로 두고 Chain 메시지만 추가했다. Forward는 `52B + 공통 command 크기`, Progress/Ack는 36B, Completed는 `65B + 반환값 크기`다. Completed에 SET payload를 다시 싣지 않는다. actual TCP codec이 아니며 CPU·실제 KV/SSD 비용은 없다.

## 실행·원본 재생성

JDK 21, 저장소 루트:

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/chain-comparison/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/chain-comparison/comparison.properties
```

주 비교 schema는 기존 `a1-election-json-1`, 위치별 schema는 `a1-chain-position-json-1`이다. 원본 누락·중복·잘못된 조건·보존/응답 count 위반이면 전체 완료 표식을 제거한다. 소스/실행 클래스 hash와 설정·실행 명령은 공통 Runner가 기록한다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.ChainResearchMain --rebuild "<repeat-001 폴더>"
```

기존 [세 멘토 질문](../sustained-load/mentoring-questions.md)의 동시 쓰기 폭·느린 replica 정책·checkpoint 범위는 그대로다. 다음 variant는 Rosé이며, CRAQ는 Chain의 별도 읽기 확장으로 아직 구현하지 않았다.
