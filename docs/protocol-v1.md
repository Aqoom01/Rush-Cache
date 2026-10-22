# Wire 프로토콜 v1

client·peer listener가 쓰는 TCP 프레임 형식입니다. 코드상의 기준은 `cache-server/.../netty/Protocol.java`입니다.

## 1. 프레임 헤더

모든 정수는 network byte order(big-endian)입니다.

| offset | bytes | 필드 | 값 |
|---|---|---|---|
| 0 | 4 | length | 첫 4 bytes를 제외한 길이. 최소 24 |
| 4 | 2 | magic | `0x4F4B` |
| 6 | 1 | version | `1` |
| 7 | 1 | messageClass | 1 client request, 2 response, 3 peer |
| 8 | 1 | opcode | 아래 표 |
| 9 | 1 | flags | v1에서는 `0` |
| 10 | 2 | status | 요청은 `0`, 응답은 아래 상태 코드 |
| 12 | 8 | requestId | 연결 안에서 요청과 응답을 짝짓는 값. 응답은 요청 값을 그대로 돌려준다 |
| 20 | 8 | routingVersion | 라우팅 버전 (현재 서버는 0으로 응답) |
| 28 | N | body | opcode별 내용 |

- **크기 제한:** length 필드를 포함한 전체 frame 최대 크기는 2 MiB(2,097,152 bytes)입니다.
- **requestId와 operationId:** 둘은 다른 값입니다.
  - `requestId`는 연결 수준의 응답 매칭에만 쓰고, core로 전달하지 않습니다.
  - 재시도된 논리 연산은 core의 `ClientId`와 `OperationId` 쌍으로 식별합니다. `clientId`, `operationId`는 GET/SET/DELETE
    body(§2)에 있습니다.

## 2. GET/SET/DELETE 요청 body

opcode 1(GET)·2(SET)·3(DELETE)의 body입니다. GET·DELETE는 key까지만, SET은 value까지 포함합니다.

| bytes | 필드 |
|---|---|
| 8 | clientId (u64) |
| 8 | operationId (u64) |
| 8 | ownershipEpoch (u64, 클라이언트가 아는 값) |
| 4 | timeoutMs (u32). 0이면 서버의 `request.timeout.ms` 기본값을 쓴다 |
| 4 | keyLength (u32) |
| keyLength | key |
| 4 | valueLength (u32). **SET에만 있다** |
| valueLength | value. **SET에만 있다** |

- `routingVersion`은 이 body가 아니라 frame 헤더의 `routingVersion` 필드로 전달한다(§1).
- `timeoutMs`는 상대 timeout이다. 서버는 받은 시점에 자기 JVM 기준 `Deadline`으로 바꾼다. 절대 시각을 보내지 않는다.
- body 길이가 짧거나 길이 필드가 남은 바이트 수를 넘으면 `BAD_REQUEST`(2)로 응답하고 연결은 유지한다(헤더는 정상이므로 frame 경계가 깨지지 않는다).

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

- `MOVED`/`NOT_PRIMARY`는 redirect 대상 노드를 body에 실어야 하지만 그 인코딩은 아직 정의되지 않았다(§6). 지금 엔진은 이 상태를 만들지 않는다.

## 4. 구현된 범위

| 항목 | 동작 |
|---|---|
| PING (opcode 4), client listener | body가 빈 client request class 요청에 OK로 응답합니다. epoch, valueVersion, detailLength, valueLength는 모두 0입니다. 서버 연결 확인용이며 SWIM 장애 감지가 아닙니다. |
| PING (opcode 4), peer listener | body가 빈 peer class 요청에 OK로 응답합니다. |
| GET/SET/DELETE (opcode 1-3), client listener | §2의 body로 단일 노드 인메모리 KV(`io.openkache.core.kv.KvEngine`)를 호출합니다. 복제·Tablet·라우팅이 없으므로 응답의 routingVersion·ownershipEpoch는 항상 0이고, `MOVED`/`NOT_PRIMARY`/`STALE_EPOCH`는 발생하지 않습니다. |
| 그 밖의 peer 메시지 | `NOT_IMPLEMENTED`(10)로 응답합니다. peer listener의 GET/SET/DELETE 등 연구별 peer 프로토콜은 아직 구현되지 않았습니다. |
| 알 수 없는 opcode, flags나 요청 status가 0이 아님, PING body가 있음, GET/SET/DELETE body가 잘못됨 | `BAD_REQUEST`(2)로 응답하고 연결을 유지합니다. |
| client listener에 response나 peer class를 보냄 | `BAD_REQUEST`(2)로 응답합니다. |
| peer listener에 client request나 response class를 보냄 | `BAD_REQUEST`(2)로 응답합니다. |
| magic, version이 잘못됨 / length가 24 미만 | `BAD_REQUEST`(2)와 사유를 보낸 뒤 **연결을 닫습니다**. 헤더를 읽을 수 있으면 requestId를 돌려주고, 아니면 0을 씁니다. |
| frame이 2 MiB 초과 | `BAD_REQUEST`(2), requestId 0으로 응답하고 연결을 닫습니다. |

헤더가 깨지면 이후 byte stream의 frame 경계를 믿을 수 없으므로 연결을 닫습니다. body 내용만 잘못된 경우(§2)는 헤더가 멀쩡하므로 연결을 유지합니다.

## 5. 상태 코드

한 번 정한 값은 바꾸지 않습니다. core의 `ResultStatus`와 1:1로 대응합니다.

| 코드 | 상태 | 코드 | 상태 |
|---|---|---|---|
| 0 | OK | 6 | BUSY |
| 1 | MISS | 7 | TIMEOUT |
| 2 | BAD_REQUEST | 8 | UNAVAILABLE |
| 3 | MOVED | 9 | CAPACITY_EXCEEDED |
| 4 | NOT_PRIMARY | 10 | NOT_IMPLEMENTED |
| 5 | STALE_EPOCH | 11 | INTERNAL_ERROR |

## 6. 예약된 확장 (미구현)

아래 항목은 형식이 정해지지 않았습니다. 구현할 때 이 문서에 먼저 정의합니다.

- **MOVED/NOT_PRIMARY:** redirect 노드 정보를 응답 body에 추가하는 방식. Tablet 라우팅이 없는 지금은 이 상태가 나오지 않습니다.
- **연구별 peer 메시지:** 복제(A1, A2), 장애 감지(A3), 마이그레이션(B3)의 opcode와 body
- **EXISTS/COUNT:** `core.api.Command`에 `Exists`, `Count`가 있고 `KvEngine`이 처리하지만, wire opcode는 아직 없습니다.
  추가할 때는 opcode 번호, COUNT 요청 body(Key 없음), COUNT 응답 body(개수 8-byte big-endian)를 이 문서에 먼저 정의합니다.
- **flags:** 압축이나 단방향 메시지 등. v1에서는 0이 아니면 거절합니다.
- **version 2 이상:** 현재는 거절합니다.
