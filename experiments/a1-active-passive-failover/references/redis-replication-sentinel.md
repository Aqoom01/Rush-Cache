# Redis Replication과 Sentinel (Primary-Backup의 실제 사례)

Redis 공식 문서 *Replication* — https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Redis 공식 문서 *High availability with Redis Sentinel* — https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/

## Redis Replication + Sentinel이란?

Redis는 **master 1대가 쓰기를 받고 replica에 비동기로 흘려보내는** Primary-Backup 복제를 쓴다. 복제 자체에는 "master가 죽으면 누가 이어받나"가 없다.

그 빈칸을 **Sentinel**이라는 별도 프로세스 묶음이 채운다. Sentinel들이 master를 감시하다가 죽었다고 합의하면 replica 하나를 골라 승격시킨다.

> **논문이 아니라 산업계 방법론?**
>
> 정식 논문은 없다. Redis 공식 문서와 소스(`replication.c`, `sentinel.c`)가 근거다. 명세서 카드의 "Primary-Backup" 기준형을 실제 시스템이 어떻게 채웠는지 보여주는 비교 대상이다.

> **Primary-Backup 기본형과의 관계?**
>
> Primary-Backup = Redis Replication(복제만). 거기에 감지·승격을 붙인 것 = Sentinel. 둘을 합쳐야 Active-Passive Failover가 된다.

## 기본 개념

| 용어 | 의미 |
| --- | --- |
| master / replica | Primary / Backup. replica는 기본 읽기 전용(`replica-read-only yes`) |
| replication offset | master가 지금까지 흘려보낸 바이트 수. replica도 자기가 받은 offset을 가진다 |
| replication ID | master의 "시대" 식별자. failover로 master가 바뀌면 새 ID가 생긴다 |
| replication backlog | master가 최근 보낸 스트림을 담아두는 링 버퍼 (`repl-backlog-size`, 기본 1MB). 끊겼다 돌아온 replica가 여기서 이어받는다 |
| PSYNC | replica가 "ID + offset"을 보내 부분 동기화를 요청하는 명령. 불가능하면 전체 동기화(RDB 전송) |
| SDOWN | Subjectively Down. Sentinel 하나가 "내가 보기엔 죽음" |
| ODOWN | Objectively Down. quorum 개수 이상의 Sentinel이 SDOWN에 동의 |
| quorum | ODOWN 판정에 필요한 Sentinel 수. `sentinel monitor <name> <ip> <port> <quorum>` |
| config epoch | failover마다 1 올라가는 번호. 더 큰 epoch의 설정이 이긴다 (= 우리의 `OwnershipEpoch`) |

```
Sentinel 3대, master 1 + replica 2

Sentinel 1 ─┐
Sentinel 2 ─┼── 감시 ──► master ──비동기 복제──► replica A
Sentinel 3 ─┘                     └──────────────► replica B
```

## 복제 — 비동기가 기본이다

master는 쓰기를 자기 메모리에 적용하고 **즉시 클라이언트에 OK**를 보낸다. replica로의 전송은 그 뒤에 소켓 버퍼로 흘러간다.

```
클라이언트 ──SET──► master ──OK──► 클라이언트      ← 여기서 이미 끝
                     │
                     └──(나중에)──► replica A, B
```

replica는 1초마다 `REPLCONF ACK <offset>`으로 "여기까지 받았다"를 master에 알린다. master는 이걸로 각 replica가 얼마나 뒤처졌는지(lag) 안다.

**동기에 가깝게 조이는 장치가 두 개 있다.**

| 장치 | 동작 | 한계 |
| --- | --- | --- |
| `min-replicas-to-write N` + `min-replicas-max-lag M` | lag이 M초 이하인 replica가 N대 미만이면 master가 **쓰기를 거부**한다 | 쓰기 전 검사일 뿐, 그 쓰기가 복제됐다는 보장은 아니다 |
| `WAIT numreplicas timeout` | 클라이언트가 호출하면 **직전 쓰기가 numreplicas대에 도달할 때까지** 블록한다 | 도달 확인일 뿐이다. failover가 그 replica를 안 고르면 여전히 유실될 수 있다 (문서가 명시) |

> Redis 복제는 "동기 복제"를 제공하지 않는다. WAIT는 "기다리기"지 "확정"이 아니다. 명세서 카드의 "동기 복제 변형"은 Redis에 없는 것을 우리가 만드는 셈이다.


## 장애 감지 — SDOWN에서 ODOWN으로

각 Sentinel은 master에 1초마다 PING을 보낸다. `down-after-milliseconds`(기본 30000ms) 동안 유효한 응답이 없으면 **SDOWN**으로 표시한다.

SDOWN은 혼자의 의견이다. Sentinel은 다른 Sentinel들에게 `SENTINEL is-master-down-by-addr`로 "너도 죽은 것 같아?"를 묻고, **quorum 개 이상**이 동의하면 **ODOWN**이 된다.

```
Sentinel 1: 30초 무응답 → SDOWN
Sentinel 1 → 2, 3: "master 죽었어?"
Sentinel 2: "응"  Sentinel 3: "응"
quorum=2 충족 → ODOWN → failover 시작
```

> **quorum과 과반수는 다르다.**
>
> ODOWN 판정은 quorum(설정값)으로 하지만, 실제 failover를 **실행할 리더 Sentinel 선출**은 전체 Sentinel의 **과반수**가 필요하다. Sentinel 5대에 quorum=2면, 2대가 ODOWN을 선언해도 3대가 투표해야 failover가 진행된다.

## 승격 — failover 6단계

| 단계 | 처리 |
| --- | --- |
| 1 | ODOWN을 본 Sentinel이 **config epoch를 1 올리고** 자기를 failover 리더로 뽑아달라고 투표를 요청한다. 과반수 득표 시 리더 |
| 2 | 리더가 replica 중 승격 대상을 고른다. 필터: 연결 끊김·`replica-priority 0` 제외 → `replica-priority` 낮은 순 → **replication offset 큰 순** → run ID 사전순 |
| 3 | 고른 replica에 `REPLICAOF NO ONE`을 보내 master로 바꾼다 |
| 4 | `INFO`로 그 노드가 master가 됐는지 확인한다 (`failover-timeout`, 기본 180000ms 안에) |
| 5 | 나머지 replica에 `REPLICAOF <새 master>`를 보낸다. 동시에 몇 대씩 할지는 `parallel-syncs` |
| 6 | 새 설정(master 주소 + config epoch)을 Pub/Sub `__sentinel__:hello`로 2초마다 퍼뜨린다. 더 큰 epoch를 받은 Sentinel은 자기 설정을 덮어쓴다 |

2단계의 "offset 큰 순"이 **덜 받은 replica를 안 고르는** 장치다. VR의 "가장 최신 로그 선택"과 같은 역할이지만, 비동기 복제라 **가장 많이 받은 replica조차 master보다 뒤처져 있을 수 있다**.

## 옛 master가 돌아오면

Sentinel들은 옛 master가 살아나면 `REPLICAOF <새 master>`를 보내 **replica로 강등**시킨다. 옛 master는 새 master에 PSYNC를 시도하는데, replication ID가 바뀌었으므로 자기가 혼자 받았던 쓰기는 **버리고** 새 master 기준으로 맞춘다.

```
옛 master: 분할 동안 클라이언트 쓰기를 계속 받음 (OK 응답까지 함)
         → 복귀 → replica로 강등 → 그 쓰기들 전부 소멸
```

> 이것이 Redis failover의 대표적 데이터 유실 시나리오다. `min-replicas-to-write`가 분할된 master의 쓰기를 막아 유실 창을 줄이지만, `min-replicas-max-lag`(기본 10초) 동안은 막지 못한다.


Sentinel이 Pub/Sub으로 퍼뜨리는 config epoch가 fencing 역할을 한다. 다만 **클라이언트 요청 자체에는 epoch가 안 붙는다**. 옛 master는 "자기가 master가 아님"을 Sentinel이 알려줄 때까지 모른 채 쓰기를 받는다. VR이 모든 메시지에 view를 붙여 즉시 거부하는 것과 결정적으로 다르다.

## 효과

1. **평소가 빠르다.** 비동기라 복제 대기가 0이다. SET 지연은 단일 노드와 같다.
2. **복제가 끊겨도 싸게 복구된다.** backlog 안에 있으면 PSYNC로 부분 동기화. 전체 RDB를 다시 보내는 일이 드물다.
3. **감지 주체가 분리돼 있다.** Sentinel이 외부에서 감시하므로 복제 코드가 단순하다. 우리 구조에서 A3(감지)와 A1(승격)을 나눈 것과 같은 모양이다.
4. **클라이언트가 Sentinel에 물어본다.** `SENTINEL get-master-addr-by-name`으로 현재 master를 조회해 재연결한다. redirect를 서버가 안 해도 된다.

## 한계

1. **OK 받은 쓰기가 사라질 수 있다.** 비동기의 대가다. 분할 시나리오와 "offset 가장 큰 replica도 뒤처진" 시나리오 두 가지에서 발생한다.
2. **감지가 느리다.** 기본 `down-after-milliseconds`가 30초다. 줄이면 오판이 늘어 failover가 폭주한다.
3. **failover 자체가 느리다.** ODOWN 합의 → 리더 투표 → REPLICAOF → INFO 확인 → 나머지 재구성. 각 단계가 초 단위다.
4. **요청 단위 fencing이 없다.** 옛 master는 Sentinel 명령을 받기 전까지 쓰기를 받는다. split brain 창이 존재한다.
5. **Sentinel도 분산 시스템이다.** Sentinel 3대 이상을 따로 운영해야 하고, Sentinel끼리의 분할 문제가 또 생긴다.

## 실험을 위해 직접 정해야 할 것

Redis 값을 그대로 쓰면 failover가 수십 초라 비교가 안 된다. 우리 규모에 맞게 줄인다.

```
- 복제 모드:            비동기(Redis 그대로) / 동기(backup 전부 ack, Redis엔 없음) 둘 다 구현
- 감지:                 Sentinel 대신 backup 자체 heartbeat (A1 초기) → A3 detector (5주 차)
- down-after:           30초 → 200 / 500 / 1000ms 로 축소해 비교
- 승격 대상 선택:        offset(우리는 op-number) 큰 순 → NodeId 순. VR의 최신 로그 선택과 같은 규칙으로 통일해 공정 비교
- min-replicas-to-write: 0 / 1 두 설정으로 유실 창 차이 측정
- 옛 primary 강등:       Sentinel식(외부 명령 받을 때까지 모름) vs VR식(요청 epoch로 즉시 거부) — fencing 유무가 핵심 비교
- 클라이언트 재연결:      Sentinel 조회 대신 NOT_PRIMARY + Redirect
```

## 우리 코드와의 대응

| Redis / Sentinel | 우리 저장소 |
| --- | --- |
| config epoch | `OwnershipEpoch` |
| replication offset | A1 복제 로그의 op-number |
| REPLICAOF NO ONE | A1 엔진의 승격 처리 |
| SENTINEL get-master-addr-by-name | `NOT_PRIMARY` + `Redirect` |
| ODOWN | A3 failure detector의 "의심" 신호 (5주 차 연결) |
| 요청 단위 fencing | 없음 → 우리는 `STALE_EPOCH`로 추가 (Redis와의 차이점으로 보고) |

## 참고

- Redis *Replication*: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
- Redis *Sentinel*: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
- Redis 소스 `replication.c` (PSYNC, backlog), `sentinel.c` (`sentinelSelectSlave()` 승격 대상 선택): https://github.com/redis/redis
- Kyle Kingsbury, *Jepsen: Redis* (2013) — Sentinel failover 중 쓰기 유실을 실험으로 보인 글: https://aphyr.com/posts/283-jepsen-redis
