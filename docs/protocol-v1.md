# Wire 프로토콜 v1

client·peer listener가 쓰는 TCP 프레임 형식입니다. 코드상의 기준은 cache-server/.../netty/Protocol.java입니다.

## 1. 프레임 헤더

모든 정수는 network byte order(big-endian)입니다.

| offset | bytes | 필드 | 값 |
|---|---|---|---|
| 0 | 4 | length | 첫 4 bytes를 제외한 길이. 최소 24 |
| 4 | 2 | magic | 0x4F4B |
| 6 | 1 | version | 1 |
| 7 | 1 | messageClass | 1 client request, 2 response, 3 peer |
| 8 | 1 | opcode | 아래 표 |
| 9 | 1 | flags | v1에서는 0 |
| 10 | 2 | status | 요청은 0, 응답은 아래 상태 코드 |
| 12 | 8 | requestId | 연결 안에서 요청과 응답을 짝짓는 값. 응답은 요청 값을 그대로 돌려준다 |
| 20 | 8 | routingVersion | 라우팅 버전 (현재 서버는 0으로 응답) |
| 28 | N | body | opcode별 내용 |

크기 제한: length 필드를 포함한 전체 frame 최대 크기는 2 MiB(2,097,152 bytes)입니다.

requestId와 operationId: 둘은 다른 값입니다.
- requestId는 연결 수준의 응답 매칭에만 쓰고, core로 전달하지 않습니다.
- 재시도된 논리 연산은 core의 ClientId와 OperationId 쌍으로 식별합니다. clientId, operationId는 GET/SET/DELETE body(§2)에 있습니다.

## 2. GET/SET/DELETE 요청 body

opcode 1(GET), 2(SET), 3(DELETE)의 body입니다. GET·DELETE는 key까지만, SET은 value까지 포함합니다.

| bytes | 필드 |
|---|---|
| 8 | clientId (u64) |
| 8 | operationId (u64) |
| 8 | ownershipEpoch (u64, 클라이언트가 아는 값) |
| 4 | timeoutMs (u32). 0이면 서버의 request.timeout.ms 기본값을 쓴다 |
| 4 | keyLength (u32) |
| keyLength | key |
| 4 | valueLength (u32). SET에만 있다 |
| valueLength | value. SET에만 있다 |

routingVersion은 이 body가 아니라 frame 헤더의 routingVersion 필드로 전달한다(§1).
timeoutMs는 상대 timeout이다. 서버는 받은 시점에 자기 JVM 기준 Deadline으로 바꾼다.
body 길이가 짧거나 길이 필드가 남은 바이트 수를 넘으면 BAD_REQUEST(2)로 응답하고 연결은 유지한다.

## 3. 응답 body

모든 응답의 body는 다음 형식입니다.

| bytes | 필드 |
|---|---|
| 8 | ownershipEpoch (u64) |
| 8 | valueVersion (u64) |
| 4 | detailLength (u32) |
| detailLength | detail (UTF-8, 오류 사유) |
| 4 | valueLength (u32) |
| valueLength | value. GET 성공 응답에만 내용이 있고, 그 밖에는 길이 0이다 |

MOVED, NOT_PRIMARY 응답의 redirect는 두 가지다. B1 라우팅 서버는 §6-1의 주소 필드를 body 뒤에 덧붙이고, A1 PB 전용 서버는 기존 detail 문자열에 redirect-node=<십진 NodeId>로 인코딩한다(§7).

## 4. 구현된 범위

| 항목 | 동작 |
|---|---|
| PING (opcode 4), client listener | body가 빈 client request class 요청에 OK로 응답합니다. |
| PING (opcode 4), peer listener | body가 빈 peer class 요청에 OK로 응답합니다. |
| GET/SET/DELETE (opcode 1-3), client listener | §2의 body로 단일 노드 인메모리 KV를 호출합니다. 복제·Tablet·라우팅이 없으므로 routingVersion·ownershipEpoch는 항상 0이고, MOVED/NOT_PRIMARY/STALE_EPOCH는 발생하지 않습니다. |
| 그 밖의 peer 메시지 | NOT_IMPLEMENTED(10)로 응답합니다. |
| 알 수 없는 opcode, flags나 요청 status가 0이 아님 | BAD_REQUEST(2)로 응답하고 연결을 유지합니다. |
| magic, version이 잘못됨, length가 24 미만 | BAD_REQUEST(2) 뒤 연결을 닫습니다. |
| frame이 2 MiB 초과 | BAD_REQUEST(2), requestId 0으로 응답하고 연결을 닫습니다. |

## 5. 상태 코드

한 번 정한 값은 바꾸지 않습니다. core의 ResultStatus와 1:1로 대응합니다.

| 코드 | 상태 | 코드 | 상태 |
|---|---|---|---|
| 0 | OK | 6 | BUSY |
| 1 | MISS | 7 | TIMEOUT |
| 2 | BAD_REQUEST | 8 | UNAVAILABLE |
| 3 | MOVED | 9 | CAPACITY_EXCEEDED |
| 4 | NOT_PRIMARY | 10 | NOT_IMPLEMENTED |
| 5 | STALE_EPOCH | 11 | INTERNAL_ERROR |

## 6. B1 라우팅 표 wire 형식

### 6-1. MOVED / NOT_PRIMARY 응답 body의 redirect 필드

§3 응답 body(valueLength + value) 뒤에 아래 필드를 덧붙인다.

| bytes | 필드 |
|---|---|
| 4 | hostLength (u32, big-endian) |
| hostLength | host (UTF-8) |
| 2 | port (u16, big-endian, 1-65535) |

redirect 주소를 모를 때(표가 낡아 새 주인을 모를 때)는 hostLength = 0으로 보내고 port 필드는 생략한다.
클라이언트는 hostLength = 0이면 힌트 없이 표를 갱신한다.
Java 구현: TableMessageCodec.encodeRedirect / decodeRedirect

### 6-2. peer opcode: 라우팅 표 메시지 (B1)

messageClass = 3 (peer), flags = 0. frame body는 공통 prefix `from(u64), to(u64)` 뒤에 아래 opcode별 내용이다.
표 직렬화(§6-3)는 payload의 마지막 필드여야 한다. Java 구현: `server.routing.B1Wire`, 서버는 `B1ServerMain`.

| opcode | 이름 | 방향 | 설명 |
|---|---|---|---|
| 5 | TABLE_PUSH | Controller → 노드 | 표가 바뀌면 한 번 보낸다 |
| 6 | GET_TABLE | 노드 → Controller | 내 버전보다 새 표가 있으면 달라는 요청 |
| 7 | TABLE_REPLY | Controller → 노드 | GET_TABLE의 응답 |

#### TABLE_PUSH body (opcode 5)

body = 라우팅 표 직렬화 (§6-3).

#### GET_TABLE body (opcode 6)

| bytes | 필드 |
|---|---|
| 8 | sinceVersion (u64) - 이 버전보다 새 표가 있으면 달라는 뜻 |

#### TABLE_REPLY body (opcode 7)

| bytes | 필드 |
|---|---|
| 8 | currentVersion (u64) - 응답자가 아는 최신 버전 |
| 1 | hasTable (0 = 변경 없음, 1 = 표 포함) |
| - | 라우팅 표 직렬화 (§6-3), hasTable = 1일 때만 |

### 6-3. 라우팅 표 직렬화

모든 정수는 big-endian. 순서는 TabletRange.start 오름차순(부호 없는 비교).

| bytes | 필드 |
|---|---|
| 8 | version (u64) |
| 8 | nextTabletId (u64) |
| 4 | tabletCount (u32) |
| tabletCount × 40 | Tablet 목록 |

Tablet 하나 (40 bytes):

| bytes | 필드 |
|---|---|
| 8 | tabletId (u64) |
| 8 | rangeStart (u64) |
| 8 | rangeEnd (u64, 0 = 2^64) |
| 8 | ownerId (u64) |
| 8 | epoch (u64) |

### 6-4. B1 실험 제어 opcode (peer listener)

실험 스크립트 전용이고 인증이 없다. prefix 없이 아래 body만 갖는다.

| opcode | 이름 | body | 설명 |
|---|---|---|---|
| 8 | CONTROL | kind(u8), expectedVersion(u64), kind별 필드 | controller 노드 전용. kind 0 = MOVE(tabletId u64, from u64, to u64), 1 = SPLIT(tabletId u64, at u64). CAS 실패는 UNAVAILABLE |
| 9 | TABLE_DUMP | 없음 | 이 노드의 표를 §6-3 형식으로 value에 돌려준다 |

CONTROL MOVE든 이동 commit이든 표가 바뀌어 어떤 노드가 Tablet을 잃으면, 그 노드는 새 표를 받은 직후 그 범위의 값을 지운다(같은 Tablet이 돌아올 때 옛 값이 되살아나지 않게).

### 6-5. B1+B3 Tablet 이동 opcode (peer listener)

`B1ServerMain`에서만 쓴다. B3 `core.migration`의 stop-and-copy·pre-copy protocol을 실제 노드 간 TCP로 실행한다. 이동은 source 노드가 진행하고 commit은 Controller의 MOVE CAS(§6-4와 같은 규칙)로 한다. 구현: `server.routing.MigrationWire`, `MigrationDriver`, `MigrationReceiver`.

| opcode | 이름 | 방향 | body | 응답 |
|---|---|---|---|---|
| 10 | MIGRATE | 실험 스크립트 → source 노드 | method(u8: 0 stop-and-copy, 1 pre-copy, 2 cutover), tabletId(u64), to(u64), rateBytesPerSec(u64, 0 = 상한 없음), batchBytes(u32), stopThresholdBytes(u64), maxRounds(u32) | OK + value = jobId(u64). 이 노드가 주인이 아니면 BAD_REQUEST, 같은 Tablet이 이미 이동 중이면 UNAVAILABLE |
| 11 | MIGRATE_STATUS | 실험 스크립트 → source 노드 | jobId(u64) | OK + value = phase(u8: 0 RUNNING, 1 COMMITTING, 2 AWAIT_TABLE, 3 CLEANING, 4 DONE, 5 FAILED), detail = 결과 JSON |
| 12 | TRANSFER | source → destination | 아래 | OK = 적용 완료(ACK). 검증 실패는 BAD_REQUEST + 이유 |

stopThresholdBytes·maxRounds는 pre-copy만 쓴다(B3 `PreCopyProtocol.Settings`). cutover는 복사 없이 소유권만 넘기고 source 범위를 지운다(복사 없음 참고선).

TRANSFER body는 공통 머리 `from(u64), to(u64), jobId(u64), tabletId(u64), epoch(u64), kind(u8)` 뒤에 kind별 내용이다.

| kind | 이름 | 내용 |
|---|---|---|
| 0 | PREPARE | rangeStart(u64), rangeEnd(u64). destination은 이 범위를 비우고 session을 연다 |
| 1 | SNAPSHOT | batchId(u64), last(u8), checksum(u64), count(u32), record × count. record = keyLength(u32), key, valueLength(u32), value |
| 2 | DELTA | batchId(u64), last(u8), finalRound(u8), checksum(u64), count(u32), delta × count. delta = seq(u64), present(u8, 0 = DELETE tombstone), keyLength(u32), key, valueLength(u32), value(tombstone이면 길이 0) |
| 3 | DISCARD | 없음. commit 전 취소 시 session과 적재한 범위를 지운다 |

checksum은 B3 `TransferBatch`·`DeltaBatch`의 CRC32C이고 destination이 다시 계산해 비교한다. batchId는 job 안에서 snapshot·delta가 함께 쓰는 하나의 증가 순서이며, 이미 적용한 id는 다시 적용하지 않고 OK로 답한다. destination은 commit 전에는 Tablet 주인이 아니므로 적재한 값이 클라이언트에 보이지 않는다.

commit 순서: protocol이 commit 준비(마지막 batch ACK + freeze 유지)가 되면 source는 (1) 그 Tablet의 모든 요청을 destination으로 MOVED 시키고 (2) Controller에 MOVE(expectedVersion = source의 현재 표 버전)를 보내고 (3) 새 표를 받은 뒤 (4) 자기 범위를 지우고 freeze를 푼다. 이동 중 freeze된 Tablet의 SET·DELETE는 BUSY(6)로 응답한다. MOVE 응답이 오지 않으면 commit 여부를 알 수 없으므로 거절과 freeze를 유지한 채 FAILED로 끝낸다.

결과 JSON 시각은 job 시작 기준 ms(source 노드 시계)다: freezeStartMs, commitStartMs, commitAckMs, tableSeenMs, doneMs, writeBlockedMs(freeze 시작 → 새 표 수신). pre-copy는 rounds, stopReason, roundEvents(라운드별 dirtyKeys·tombstoneKeys·dirtyBytes·roundSentBytes)를 더한다. commitRetries는 최초 원격 CONTROL 요청 이후 표 버전 불일치로 다시 보낸 커밋 요청 수다. 재시도 중 쓰기 차단 시간은 writeBlockedMs에 포함된다.

## 7. A1 PB 전용 peer 확장

기본 단일 노드 진입점은 변경하지 않는다. `io.openkache.server.replication.PbServerMain` 진입점에서만 아래 opcode를 활성화한다. 공통 Frame length/class/flags/status 규칙은 그대로 사용한다. 실험용 보안 그룹 안의 고정 3노드만 신뢰하며 TLS·peer 인증은 없다.

|opcode|body|의미|
|---:|---|---|
|32|from(u64), to(u64), epoch(u64), opNumber(u64), keyLength(u32), key, valueLength(u32), value|동기 SET 복제|
|33|from(u64), to(u64), epoch(u64), opNumber(u64)|실제 Backup KV 적용 후 ACK|
|34|from(u64), to(u64), epoch(u64), kind(u8: HEARTBEAT=0, INSTALL=1, READY=2)|crash-only 감지·구성 전환|
|35|빈 body|read-only 상태 진단. 일반 응답 value에 JSON|
|36|key raw bytes|read-only 로컬 KV 진단. Backup에서도 저장 값 보존 검사에만 사용|

32~34는 엔진이 발신자/epoch/op-number를 확인하며, 일반 응답 OK를 복제 ACK로 취급하지 않는다. 유효 peer 메시지는 단방향으로 전달하고 실제 복제 ACK/READY는 별도 peer 메시지로 돌아온다. 알 수 없는 송·수신 노드·길이·kind·trailing bytes는 BAD_REQUEST다. Transport는 연결을 재사용하며 전송 실패한 SET을 자동 재전송하지 않는다.

35~36은 실험 검증 경로다. 일반 client listener의 Backup 읽기/쓰기 거절을 우회하는 제품 API로 사용하지 않는다. 상태의 nanoTime/detectedNs/readyNs는 해당 JVM에서만 비교할 수 있다. 다른 호스트의 monotonic 값을 직접 차감하지 않는다.

### 7-1. 로컬 PB 펜싱 모드

`protocol=PB_FENCED`는 peer opcode 39를 추가하고 로컬 KV 진단 opcode 36을 비활성화한다. opcode 39의 body는 `from(u64), to(u64), epoch(u64), kind(u8), round(u64), durationNanos(u64), digest` 순서다.

| kind | 이름 | 추가 조건 |
|---:|---|---|
| 0 | ACQUIRE | 양수 round와 lease 기간. digest 없음 |
| 1 | GRANT | 요청과 같은 round와 lease 기간. digest 없음 |
| 2 | REQUEST_VOTE | round와 기간은 0. digest 없음 |
| 3 | VOTE | round와 기간은 0. 적용 이력의 SHA-256 32바이트 |

복제 TCP와 분리된 `failure.port`의 UDP plane에서 SWIM opcode 16(PING), 17(ACK), 18(PING_REQ)을 사용한다. `PB_FENCED`는 `failure.detector=SWIM|SWIM_LIFEGUARD`를 요구하며, DEAD를 과반수 투표와 lease fencing의 입력으로만 사용한다. 설정과 실행 방법은 [로컬 펜싱 검증](../experiments/a1-active-passive-failover/real-server/fencing-local.md)에 있다.

## 8. A1 VR 전용 peer 확장

같은 실험 진입점의 `protocol=VR`에서만 48~56을 활성화한다. 생략 시 `protocol=PB`로 기존 설정을 유지한다. PB와 VR peer 메시지를 혼용하지 않는다. 공통 prefix는 `from(u64), to(u64), view(u64)`이며 정수는 big-endian이다. 고정 세 노드 이외의 ID·오배달·잘못된 길이/명령 tag·trailing bytes를 거절한다.

|opcode|prefix 다음 body|의미|
|---:|---|---|
|48|commit(u32), log|PREPARE 전체 로그|
|49|firstOp(u32), commit(u32), log|DELTA_PREPARE, 1-based suffix|
|50|commit(u32)|COMMIT/heartbeat|
|51|length(u32)|PREPARE_OK|
|52|없음|START_VIEW_CHANGE|
|53|lastNormal(u64), commit(u32), log|DO_VIEW_CHANGE|
|54|commit(u32), log|START_VIEW|
|55|없음|GET_STATE|
|56|없음|VIEW_CHANGE_ACK, 전송 확인이며 commit ACK와 구분|

`log = count(u32), Request[count]`. Request는 `client(u64), operation(u64), commandTag(u8), commandBody`다. tag1=SET(keyLength/key/valueLength/value), 2=GET(keyLength/key), 3=DELETE(keyLength/key), 4=EXISTS(keyLength/key), 5=COUNT(빈 body). 길이는 u32이며 Java 구현은 음수·잔여 byte 초과를 거절한다. count도 잔여 byte로 상한을 검사한다. 이는 **내부 로그 명령 tag**로, client opcode 추가를 뜻하지 않는다.

기존 length 포함 2MiB frame 한도를 유지한다. encode 시 초과하는 로그·필드를 거절하며 체크포인트·분할 전송은 없다. VR은 기본 INCREMENTAL 모드지만 view change/상태 전달에는 전체 로그가 포함되므로 큰 로그를 처리하는 장기 실행에는 추가 구현이 필요하다. 진단35/36과 client wire는 PB와 공유한다. 상태 JSON은 protocol과 VR state/logLength/commit/applied를 추가하고 epoch에는 view를 넣는다(PB 초기1, VR 초기0). 최초 유효한 원래 Primary의 heartbeat/prepare/start-view를 받은 뒤 Backup timer를 시작한다.

## 9. A1 Raft 전용 peer 확장

동일 실험 진입점의 `protocol=RAFT`에서만 64~67을 활성화한다. 공통 prefix는 `from(u64), to(u64), term(u64)`다. 기존 PB/VR 메시지와 혼용하지 않는다. ID·수신 노드·길이·잔여 byte·잘못된 boolean/tag를 거절한다. length 포함 2MiB 한도를 유지하고 큰 suffix를 분할하는 기능은 없다.

|opcode|prefix 다음 body|의미|
|---:|---|---|
|64|lastIndex(u32), lastTerm(u64)|RequestVote|
|65|granted(u8:0/1)|VoteReply|
|66|rpc(u64), prevIndex(u32), prevTerm(u64), commit(u32), entryCount(u32), entries|AppendEntries/heartbeat|
|67|rpc(u64), success(u8:0/1), matched(u32)|AppendReply|

각 entry는 `entryTerm(u64), hasRequest(u8:0/1), [Request]`다. hasRequest=0은 현재 term commit을 확립하는 no-op이다. Request 형식은 §8의 client/operation/명령 tag와 동일하다. entryTerm은1 이상·RPC term 이하이며 각 메시지의 core 유효성 검사를 함께 적용한다. entries가 빈 Append는 heartbeat가 될 수 있다. 내부 명령 tag가 client opcode 지원을 확장하는 것은 아니다.

Raft는 세 노드 모두 처음부터 timer를 실행해 리더를 선출한다. `timeout.ms`는 최소 선거 timeout으로 실제 값은 `[timeout.ms, 2*timeout.ms)`다. `election.seed`(기본1)에 노드ID×104729를 더해 노드별 난수를 초기화한다. seed는 실험 반복 조건이지 실제 실행의 네트워크 순서를 결정론적으로 만들지는 않는다.

진단35/36은 공유한다. epoch는 term이며, 상태 JSON에 role/leader(-1=미확인)/elections/logLength/commit/applied/electionTerm/electionSeed를 추가한다. Raft의 detectedNs/readyNs는 최근 **로컬 선거 하나**의 후보 시작과 준비 완료다. electionTerm이 현재 leader의 epoch와 같은 경우에만 차감한다. 초기 선거의 누적 관측을 장애 후 전환 시간으로 사용하지 않는다. term/vote/log의 영속화·프로세스 재시작·snapshot·동적 구성은 구현하지 않았다.

`raftElectionEvents`는 Raft owner에서 기록한 최대 2048개 불변 진단 이력이다. 각 항목의 `term`, `kind`(`STARTED` 후보 시작 / `BECAME_LEADER` 투표 quorum 확보 후 당선), `atNs`(해당 JVM monotonic), `origin`을 제공한다. `raftElectionEventsDropped`가 0이 아니면 이력 일부가 빠졌으므로 실험의 선거/리더 교체 횟수 근거로 사용할 수 없다. 같은 노드의 새 term 당선과 다른 노드로의 리더 교체는 구분한다. 이 진단은 선거·commit 결정을 변경하지 않는다.

## 10. A1 전송량 진단 (opcode35 JSON 확장)

모든 실험 모드에서 `peerTx={totalFrames,totalBytes,opcodes:{"49":{frames,bytes},...}}`를 제공한다. Netty write가 성공한 peer frame의 length+header+body 크기이며 원격 수신 확인이나 TCP/IP overhead는 아니다. 실패 전송은 집계하지 않는다. opcode별·합계는 같은 lock 안에서 복사하므로 한 노드 snapshot 내 합계가 일치한다. 고정 opcode256개 카운터만 유지하며 key/client/request별 항목을 만들지 않는다. 기존 sentBytes는 별도 시점의 카운터라 peerTx와 동시에 읽었다고 가정하지 않는다.

VR의 `vrTraffic`은 고정 원인별 누적 횟수다. GET_STATE_COMMIT_GAP/DELTA_PREFIX_GAP/DELTA_COMMIT_GAP/VIEW_STATE는 상태 요청을 보낸 이유, GET_STATE_RECEIVED는 수신 횟수다. SNAPSHOT_GET_STATE_REQUEST/STALE_VIEW_REQUEST/VIEW_INSTALL_REQUEST/LATE_CHANGE_REQUEST/RETRY_REQUEST는 전체 로그 전달 **요청 원인**이며 실제 송신 횟수와 구분한다. SNAPSHOT_THROTTLED는 중복 요청 억제, SNAPSHOT_UPGRADE는 증분에서 전체 전송으로 전환, TRANSFER_RESEND는 프로토콜 재전송 시도다. GET_STATE_SEND/SNAPSHOT_SEND/DELTA_SEND/COMMIT_SEND는 core 송신 시도다. 실제 송신 성공 byte·frame과 대조할 때 transport 실패와 아직 남은 전송을 확인한다. `vrMode`는 설정 `vr.mode`(INCREMENTAL 기본, INCREMENTAL_CATCHUP)를 그대로 보고한다. COMMIT_BOUNDED/DELTA_CATCHUP_REQUEST/DELTA_CATCHUP_THROTTLED는 INCREMENTAL_CATCHUP 전용이며 각각 Backup별 전송 범위로 낮춰 보낸 COMMIT 수, 같은 view 정상 Backup의 상태 요청을 증분으로 처리한 수, 전송 창 대기로 미룬 수다. 다른 모드에서는 0이다.

core 원인 카운터는 항목별 원자적 관측이고 노드 간 동시 snapshot은 아니다. 실험은 before/부하 직후/값 검증·데이터 안정화 후를 남겨 측정 경계의 잔여 전송을 구분한다. heartbeat가 계속 증가하므로 COMMIT 송신 시도를 wire 완료 횟수와 정확한 동시 관측으로 취급하지 않는다. OS TCP 재전송은 계측하지 않는다.

## 11. A1 Chain/CRAQ·Rosé peer 확장과 master 제어

같은 실험 진입점의 `protocol=CHAIN`(읽기 모드 `chain.read.mode=TAIL_ONLY|CRAQ`)과 `protocol=ROSE`(`rose.limit`=-1/0/양수)에서만 활성화한다. `retry.ms`는 두 엔진의 데이터/제어 재전송 간격이다. 공통 prefix는 `from(u64), to(u64), epoch(u64)`이며 Request/명령 tag는 §8과 같다. 두 엔진 모두 heartbeat 기반 failure detector가 없으므로 세 노드가 처음부터 timer를 실행한다.

|opcode|prefix 다음 body|의미|
|---:|---|---|
|80|index(u32), Request|Chain FORWARD(사슬 다음 노드로 전달)|
|81|applied(u32)|Chain PROGRESS(successor 적용 위치, 전송 창 전진용)|
|82|committed(u32)|Chain ACK(tail→head 역방향 누적 확정)|
|83|index(u32), client(u64), operation(u64), Result|Chain COMPLETED(tail→head 결과 중계)|
|84|queryId(u64), keyLength(u32), key|CRAQ VERSION_QUERY(dirty 읽기의 tail 버전 조회)|
|85|queryId(u64), keyLength(u32), key, version(u32)|CRAQ VERSION_REPLY(0은 미기록 key)|
|96|index(u32), Request|Rosé APPEND|
|97|index(u32)|Rosé APPLIED(Backup 적용 ACK)|

Result는 `status(u8 ordinal), hasValue(u8), [valueLength(u32), value], valueVersion(u64), routingVersion(u64), ownershipEpoch(u64), hasRedirect(u8), [node(u64)]`다. 알 수 없는 status/명령 tag/boolean·잔여 byte·길이 초과는 BAD_REQUEST다. 2MiB frame 한도를 유지하며 분할 전송은 없다.

진단 37(LOG_DIGEST, 빈 body)은 Chain/Rosé 전용으로 `length(u32) + SHA-256(Request 직렬화 연접)`을 value에 돌려준다. 생존자 로그 일치 검사에만 쓴다.

제어 38(MASTER_CONTROL)은 **정확한 영구 crash를 아는 신뢰 master(실험 controller)** 전용이며 제품 API·분단 fencing이 아니다. body는 `kind(u8), epoch(u64)` 뒤에 kind별 필드다. Chain: 0=FREEZE(`count(u32), survivors[u64]`, 생존 사슬 순서) → `installConfiguration`, 1=REPAIR → `beginRepair`, 2=OPEN(`length(u32)`) → `openConfiguration`. Rosé: 0=FREEZE → `freezeAfterCrash`, 1=REPAIR(`newPrimary(u64), count(u32), survivors[u64]`) → `beginRepair`, 2=OPEN(`length(u32)`) → `openAfterRepair`. 엔진 future가 끝난 뒤 OK(detail `applied=N`/`repairing`/`open`) 또는 UNAVAILABLE(거절 사유)로 응답하고 EventLoop를 막지 않는다. FREEZE 수신 시각이 상태 JSON의 `detectedNs`, 엔진 readyAt이 `readyNs`다. controller의 감지 대기는 고정값이며 실제 failure detection이 아니다.

상태 JSON은 Chain에 `readMode/state/chain/logLength/applied/acknowledged/sent/localReads/queryAttempts/dirtyReads/retainedVersions/pendingReads`, Rosé에 `limit/state/primary/logLength/applied/minAck/peakLag/backpressureCount/deferred`를 추가한다. epoch는 master가 설치한 구성 번호(초기1)다.

## 12. A3 SWIM UDP failure-detection plane과 A1 연동

`PbServerMain`의 `protocol=PB|PB_FENCED|RAFT`, `failure.detector=SWIM|SWIM_LIFEGUARD`에서 사용한다. 복제 TCP와 분리된 UDP port(기본 7002)에 datagram 하나당 protocol v1 frame 하나를 보낸다. `length(u32)` 뒤 공통 24-byte header를 사용하고 message class는 PEER(3), opcode는 PING=16, ACK=17, PING_REQ=18이다. body 공통 prefix는 `from(u64), to(u64), probeId(u64)`이고 PING은 `origin(u64)`, ACK은 `target(u64), origin(u64)`, PING_REQ는 `origin(u64), target(u64)`를 잇는다.

그 뒤 `updateCount(u32)`와 update 목록이 온다. membership update 하나는 `member(u64), incarnation(u64), status(u8: ALIVE=0, SUSPECT=1, DEAD=2), reporter(u64)` 25 bytes다. detector는 한 datagram에 최대 64개를 싣는다. 잘못된 length/magic/version/opcode/노드 ID/status/count/trailing bytes는 폐기하고 `fdInvalidFrames`에 집계한다.

SWIM의 DEAD 이벤트는 A1 owner tick으로 전달된다. SUSPECT는 시각만 기록하고 승격하지 않는다. `protocol=PB`에서는 기존 INSTALL/READY crash-only 경로를 시작하므로 partition 안전성을 제공하지 않는다. `protocol=PB_FENCED`에서는 Node 1의 DEAD 관측이 승계 검토를 시작하되 Node 2 투표, 옛 lease 만료, 두 Backup의 적용 이력 digest 일치, INSTALL/READY와 새 lease를 모두 통과해야 쓰기를 연다. `SWIM_LIFEGUARD`는 local-health awareness에 따라 probe 주기·timeout과 suspicion 시간을 확대하고 독립 reporter의 SUSPECT confirmation으로 최대 suspicion 시간을 최소값까지 단축한다. `SWIM`은 같은 wire에서 이 보정을 끈 baseline이다. Buddy System은 구현하지 않는다.

상태 진단은 `failureDetector`, `primaryFailureStatus`, `fdSuspectNs`, `fdDeadNs`, `fdAwarenessScore`, 실효 probe 시간, UDP 송수신 frame/byte 및 failure/invalid 카운터를 제공한다. `PB_FENCED`는 추가로 `quorumNs`, `promotionVotes`, `leaseDeadlineNs`, `fenceState`를 제공한다. 현재 펜싱 구현도 고정 3노드의 최초 승계 한 번만 지원하며 epoch 영속화·재가입·두 번째 승계는 범위 밖이다.

`RAFT`는 현재 리더·term의 DEAD와 최소 `timeout.ms`의 Raft heartbeat 단절을 확인한 뒤 정상 투표 선거를 시작한다. 초기 선거·candidate 재시도는 원래 타이머를, 신호 누락에는 `[10T,11T)` watchdog을 사용한다. `raftElectionOrigin`, `raftElectionTrigger`, `raftFailedLeader`, `raftFailureSignalTerm`, `raftFdSuspectNs`, `raftFdDeadNs`, `raftFallbackExtraNs`로 근거를 구분한다. 유효한 Append는 이전 FD 신호를 무효화한다. FD가 직접 리더를 지정하거나 quorum을 우회하지 않는다. [Raft FD 실험](../experiments/a1-active-passive-failover/real-server/raft-fd.md)에 실행 방법과 범위를 기록한다.
