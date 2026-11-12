# A1과 A3 로컬 펜싱 검증

`protocol=PB_FENCED`, `failure.detector=SWIM_LIFEGUARD`는 고정 3노드 A1·A3 로컬 결합 실험 모드다. Node 0이 최초 Primary이고 Node 1만 최초 승계 후보가 된다. baseline은 detector만 `SWIM`으로 바꾼다. 기존 `protocol=PB`와 EC2 결과는 crash-only 조건으로 재현한다.

## 장애 감지부터 쓰기 재개까지

1. A3 lab의 UDP 어댑터를 재사용한다. `SUSPECT`와 `DEAD`는 첫 이벤트를 보관하고 다음 PB owner tick에서 처리하므로 일시적인 mailbox 포화로 유실되지 않는다. `SUSPECT`는 시각만 기록하고 `DEAD`는 Node 1의 승계 검토를 시작한다.
2. Node 0은 자기 자신과 Backup 하나의 유효한 grant로 lease를 유지한다. `heartbeat.ms`마다 갱신을 요청하고 `timeout.ms` 동안만 자기 권한을 사용할 수 있다. 적용 ACK는 두 Backup 모두에서 받는다.
3. Node 1과 Node 2는 각자 이전에 준 grant가 만료된 뒤 epoch 2의 유일한 후보 Node 1에 투표한다. Node 2는 Node 1의 요청을 받고 자기 grant를 확인한다. gossip 재전달 횟수는 투표에 쓰지 않는다.
4. 두 Backup은 투표한 뒤 이전 epoch의 새 복제를 차단하고 진행 중인 로컬 적용을 끝낸다. 각각 적용한 쓰기 기록의 SHA-256이 같을 때 Node 1이 승격을 승인한다. Node 2의 `INSTALL/READY`와 epoch 2의 새 lease까지 확인하고 쓰기를 연다.
5. Node 0의 lease가 한번 만료되면, 링크가 돌아와도 이전 epoch의 권한을 다시 얻지 않는다. `GET`과 `SET`은 `UNAVAILABLE`로 막히며, 큐에 남은 요청도 성공 결정 전에 권한을 확인한다. epoch 2를 설치한 노드는 epoch 1 요청을 `STALE_EPOCH`로 거절하고, 현재 epoch에서 Backup으로 온 요청은 `NOT_PRIMARY`와 redirect로 응답한다.

## 시계와 데이터 전제

- Primary는 요청 전송 시점부터 자기 시계로 `T`만큼 권한을 사용한다. Backup은 grant 수신 시점부터 자기 시계로 `2T` 동안 다른 후보에게 투표하지 않는다. 메시지에는 round와 공통 설정 `T`만 넣고 JVM 시각을 비교하지 않는다. 기한이 지난 round의 응답은 권한을 되살리지 못한다.
- 같은 설정과 연속적인 monotonic clock을 전제로 한다. 로컬 모델에서는 실제 시간 대비 시계 속도 오차를 ±1% 이내로 둔다. 느린 Primary의 최대 권한 구간 `T / 0.99`가 빠른 Backup의 최소 grant 구간 `2T / 1.01`보다 짧다. JVM 일시정지 중에도 시계가 진행해야 한다. 시스템 suspend 또는 clock discontinuity는 범위 밖이다. 성공 시각은 서버의 응답 결정 기준이다. [Nerio, bounded-drift lease](https://arxiv.org/abs/1109.5111)
- 새 메모리 KV에서 시작하고 모든 쓰기를 PB 경로로 전달한다. `OK`는 활성 구성의 모든 Backup에서 적용을 확인한 뒤 반환한다. 두 Backup에 같은 미확정 쓰기가 적용됐다면 그 상태가 이어질 수 있다. 이력이 다르거나 적용 실패·미완료가 남으면 서비스 재개를 막는다. common-prefix 복구를 구현하려면 별도의 확정 위치와 재적용·롤백 정책이 필요하다.
- 재시도 시 같은 `ClientId`와 `OperationId`를 보낸다. 이번 PB는 동일 key/value `SET` 재실행만 전제로 하며, 결과나 버전을 exactly-once로 보장하지 않는다.
- lease를 잃은 Node 0은 복귀해도 이 프로세스에서 다시 serving하지 않는다. 이때 `DEAD`가 오지 않거나 Backup 둘이 합의하지 못하면 계속 중단된다. CLI는 동일 작업 디렉터리의 기동 표식으로 프로세스 재시작도 차단한다. Lifeguard local-health 보정은 포함되며, 영속 epoch와 재가입·두 번째 승계·SSD health 신호는 후속이다.

## 로컬 실행

```powershell
.\gradlew.bat :cache-core:test --tests '*PrimaryBackupFencingTest' :cache-server:test --tests '*PbWireTest' :cache-server:installDist
python experiments/a1-active-passive-failover/real-server/local_fencing_validation.py --lib cache-server/build/install/openkache-server/lib --out .local/fencing-validation
```

가상 시계 테스트는 분단, 지연된 grant, 투표·READY 유실, 중복 DEAD, 복제 이력 불일치, 일시정지 후 늦은 응답, mailbox 포화 중 이벤트 및 적용 완료 통지 보존, 승격 후 지연 복제 거절을 검증한다. 로컬 실행은 실제 TCP·UDP를 쓰는 JVM 세 개를 띄워 Node 0을 강제 종료한 다음, SWIM 자동 감지·Node 1 쓰기 재개·이전 성공값·stale epoch·redirect·동일 디렉터리 재시작 차단을 확인한다. POSIX에서는 SIGKILL, Windows에서는 TerminateProcess가 사용된다.

상태 JSON은 로컬 JVM 기준 `fdSuspectNs`, `fdDeadNs`, `suspectNs`, `detectedNs`, `quorumNs`, `promotionAuthorizedNs`, `installedNs`, `peerReadyNs`, `readyNs`, `leaseDeadlineNs`와 Lifeguard awareness/실효 timeout, UDP frame/byte를 기록한다. 전체 장애→첫 성공 SET은 테스트 프로세스의 한 시계로 측정한다.
