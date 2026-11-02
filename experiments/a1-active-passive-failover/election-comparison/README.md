# PB / VR / Raft 선출 비교

기존 PB·VR에서 Raft를 추가한 고정 3노드 비교다. Raft는 논문 [Figure 2 및 §5](https://raft.github.io/raft.pdf)를 기반으로 별도 엔진을 구현했다. 기존 결과의 variant 목록과 설정은 수정하지 않았다.

## 구현

- seed별 `[200,400)ms` election timeout, term당 한 표, 후보의 마지막 로그 term/index 최신성 검사.
- RequestVote 및 AppendEntries, 높은 term 응답을 받은 리더의 하야.
- prevLogIndex/term 불일치 시 nextIndex 후퇴와 미확정 충돌 suffix 교체.
- 현재 term 항목만 과반수 복제 수로 직접 commit. 새 리더는 no-op을 추가해 이전 term의 항목도 간접 확정한다.
- 읽기도 로그·과반수를 경유한다. 단일 in-flight, client/operation 중복 응답과 명령 재사용 거부.
- follower별 데이터 전송 창 하나, 1→2→4초 재전송 간격. 빈 heartbeat는 별도이며 응답은 RPC ID와 검증한 인덱스에 대응시킨다.
- 모든 상태 변경은 owner mailbox, KV 완료 callback도 owner로 복귀한다.

`currentTerm`, `votedFor`, 로그는 메모리에만 유지한다. **영구 crash와 메모리 유지 분단·복구**의 연구 구현이다. 재시작 영속성, snapshot/checkpoint, membership 변경, PreVote, ReadIndex, 실제 peer codec은 포함하지 않았다. 재시작 시 같은 NodeId를 빈 상태로 재합류시키면 안 된다.

## 조건

`PB / VR_INCREMENTAL / RAFT` × 시나리오 6개 × seed 1·2·3 × 값 64B·8KiB × 방향별 1·10MiB/s = **216조건**. 결과에서 VR은 INCREMENTAL 구현이다. quick은 seed 1·1MiB/s의 36조건을 두 번 실행한다.

|시나리오|주입|
|---|---|
|NORMAL|장애 없음|
|SLOW_BACKUP|Backup 한 대의 ACK 전달 +200ms|
|CRASH|선출/준비 완료한 실제 리더 영구 종료|
|PARTITION|리더 격리, 옛 리더에도 요청, 다수 측 쓰기 시도 후 repair|
|INFLIGHT_CRASH|ACK 차단 후 쓰기 제출, 40ms 뒤 리더 종료|
|FOLLOWER_REJOIN|Follower 한 대 격리 후 새 논리 쓰기 4건 시도, repair 및 2초 대기|

시작 시 Raft는 실제 투표로 리더를 선출한다. PB/VR은 초기 Primary가 정해져 있으므로 startup 시간을 정상 쓰기 지연에 섞지 않는다. 정상 SET 64건을 완료하고 30ms 관측한 뒤 주입한다. 이후 새 논리 쓰기 16건을 순서대로 보낸다. 기존 연속 부하 48조건을 확장한 결과가 아니라 **별도 closed-loop 실험**이다.

공통 네트워크는 방향별 FIFO 직렬화 + 1ms 전파 지연이다. 실제 wire 대신 논리 bytes를 센다. Raft는 공통 frame 32B에 Vote 12B, VoteReply 1B, AppendReply 13B, Append header 28B를 더한다. 로그 entry는 term 8B + request 유무 1B + 요청이 있으면 client/operation 16B와 공통 command 크기다. PB·VR 기존 모델은 변경하지 않았다. 모든 전송 통계는 startup·warmup·최종 대기까지 포함한다.

SLOW 지연 대상은 PB/VR에서 node 2, Raft에서는 초기 리더가 아닌 `(leader+1)%3`이다. 클라이언트 논리 요청은 client/operation/key/value를 유지한 채 재시도한다. 시도 deadline 최대 1000ms(논리 예산 끝에서는 남은 시간), 실패 후 10ms 대기, 논리 쓰기당 최대 5초다. TIMEOUT·BUSY·UNAVAILABLE·STALE_EPOCH이면 다음 노드를 찾고, NOT_PRIMARY면 Redirect를 따른다. client epoch는 응답에서 최댓값으로만 갱신한다. 실행기에서 알고 있는 새 리더를 클라이언트에 직접 알려주지 않는다.

## 지표와 해석

- `firstOkNs - faultAtNs`: 주입 후 새 요청의 첫 성공 callback. **CRASH/INFLIGHT_CRASH/PARTITION에서만** 장애 후 쓰기 재개 지연이다. 다른 시나리오는 정상/지연/재합류 관측 구간의 첫 OK다.
- `startupNs`: 초기 리더가 자신의 로그를 commit/apply한 시점까지의 준비 시간. PB/VR은 사전 지정, Raft는 투표이므로 조건 차이를 명시한다.
- `successP50Ns/successP99Ns`: 성공한 마지막 시도 RTT의 nearest-rank. 실패·재시도 비용은 attempts와 상태별 count 및 첫 OK 지연에서 따로 본다.
- `preOk/retainedPre`, `postOk/retainedPost`: 성공 ID·명령·값의 생존 근거. PB는 KV, VR/Raft는 로그. 죽은 노드의 메모리는 제외한다.
- `converged`: 최종 생존 KV의 대상 key별 값 일치. 전체 로그 동일성이나 실패 요청의 취소를 뜻하지 않는다. 모든 조건에서 지연을 없애고 6초 더 관측한 값이다.
- `inflightRetained`: INFLIGHT_CRASH에서 성공 응답하지 못했던 요청의 생존 여부. TIMEOUT/UNAVAILABLE는 취소가 아니다.
- `elections`: Raft에서 세 노드의 입후보 횟수 합계(초기 선거 포함). PB/VR은 -1.

PB/VR 감지 timeout 200ms와 Raft `[200,400)ms`는 **같은 분포가 아니다.** 이 실험만으로 선출 프로토콜 자체가 더 빠르다고 결론내리지 않는다. 감지/전환 분해와 timeout 민감도는 다음 비교에서 맞출 수 있다. 이번에는 프로토콜 구현과 공통 클라이언트를 통한 장애별 동작을 확인한다.

## 실행과 재현

JDK 21, 저장소 루트:

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/election-comparison/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/election-comparison/comparison.properties
```

공통 Runner는 source/backend hash·설정·명령·반복·제한 시간을 기록한다. `results/*.json`이 원본이고, schema `a1-election-json-1`의 고정 형식만 지원한다. 누락·중복·잘못된 조건·보존/응답 count 위반이면 완료 표시를 제거한다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.ElectionResearchMain --rebuild "<repeat-001 폴더>"
```

동시 쓰기 폭·느린 replica 정책·checkpoint의 세 [멘토 질문](../sustained-load/mentoring-questions.md)은 그대로 남긴다. 후속으로 [Chain 구현·비교](../chain-comparison/results.md)를 추가했으며 Rosé는 구현 대기다.
