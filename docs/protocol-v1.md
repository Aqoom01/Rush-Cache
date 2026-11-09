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

MOVED, NOT_PRIMARY 응답의 redirect 주소 인코딩은 §6-1을 참조한다.

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

messageClass = 3 (peer), flags = 0.

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

## 7. 예약된 확장 (미구현)

한 번 정한 값은 바꾸지 않습니다.

- 연구별 peer 메시지: 복제(A1, A2), 장애 감지(A3), 마이그레이션(B3)의 opcode(8 이상)와 body
- EXISTS/COUNT: opcode 번호, body를 먼저 이 문서에 정의합니다.
- flags: 압축이나 단방향 메시지 등. v1에서는 0이 아니면 거절합니다.
- version 2 이상: 현재는 거절합니다.
