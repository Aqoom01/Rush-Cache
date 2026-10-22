# Primary-Backup — Active-Passive Fast Failover 기준형 학습 정리

## Active-Passive Fast Failover란?

노드 여러 대 중 **한 대(Primary)만 쓰기를 받고**, 나머지(Backup)는 복사본만 들고 대기하다가, Primary가 죽으면 Backup 중 하나가 **빠르게 Primary 자리를 이어받는** 방식이다.

Active = 요청을 처리하는 Primary 1대, Passive = 대기하는 Backup N대.

> **Failover?**
>
> 장애(fail)가 났을 때 다른 노드로 넘긴다(over)는 뜻. "대표가 죽으면 부대표가 대표가 된다."

A1은 이 목표를 이루는 방법 중 기준형(Primary-Backup)과 VR 두 가지를 먼저 구현해 비교하고, Raft·Chain·Rosé는 그 뒤에 붙인다. 전체 목록은 [README](../README.md)를 따른다.

| 방법 | 성격 |
| --- | --- |
| Primary-Backup | 가장 단순한 기준형(baseline). "Primary가 받아서 Backup에 복사한다"만 정해져 있고, 감지·승격 규칙은 비어 있어 직접 채워야 한다 |
| Viewstamped Replication (VR) | 감지·승격·fencing·중복 방지까지 프로토콜 안에 들어 있는 완성형 |

Redis Replication + Sentinel은 "Primary-Backup의 빈칸을 채운 실제 사례"로 비교 대상이 된다.

## 기본 개념

| 용어 | 의미 |
| --- | --- |
| Primary | 클라이언트 쓰기를 받는 유일한 노드 |
| Backup | Primary가 보내주는 복사본만 받는 노드 |
| Replication | 같은 데이터를 여러 노드에 복사해 두는 것 |
| Heartbeat | "나 살아있어"를 주기적으로 보내는 신호 |
| Timeout | heartbeat가 이 시간 넘게 안 오면 죽은 것으로 판단하는 기준 |
| Promotion (승격) | Backup이 자기를 Primary로 바꾸는 것 |
| Epoch (= VR의 view) | "지금 몇 번째 Primary 시대인가"를 나타내는 번호. Primary가 바뀔 때마다 1 증가 |
| Fencing | 낮은 epoch가 붙은 메시지를 거부해 옛 Primary를 차단하는 것 |
| Split brain | Primary가 동시에 2대가 되는 사고 |

`NodeId`와 `epoch`는 다른 번호다. NodeId는 노드의 이름표(고정), epoch는 Primary 교체 횟수(변함).

```
epoch 1: Primary A, Backup B·C
epoch 2: Primary B, Backup A·C   ← A가 멈춰서 B로 교체
epoch 3: Primary C, Backup A·B   ← B도 죽어서 C로 교체
```

## Primary-Backup의 동작

### 1. 정상 상태 — 복제

```
클라이언트
    │ SET user:1 Bob
    ▼
Primary ──복사──► Backup 1
        ──복사──► Backup 2
```

Primary는 자기 저장소에 쓰고 Backup에 복사 메시지를 보낸다. 여기서 **언제 클라이언트에게 OK를 보내느냐**가 첫 번째 갈림길이다.

| 방식 | OK 시점 | 장점 | 단점 |
| --- | --- | --- | --- |
| 동기 복제 | Backup이 "저장했다" 응답한 뒤 | Primary가 바로 죽어도 데이터가 남는다 | Backup 왕복만큼 매번 느리다 |
| 비동기 복제 | Primary가 자기한테 저장한 즉시 | 빠르다 | OK 직후 Primary가 죽으면 그 데이터는 사라진다 |

Redis는 기본이 비동기다. 그래서 빠른 대신 failover 때 일부 쓰기가 유실될 수 있다.

### 2. 장애 감지 — heartbeat와 timeout

Primary는 주기적으로(예: 100ms) Backup에 heartbeat를 보낸다. Backup은 마지막 heartbeat 이후 경과 시간을 재다가 timeout(예: 500ms)을 넘기면 "Primary 죽음"으로 판단한다.

문제는 **진짜 죽은 것, 네트워크가 잠깐 끊긴 것, GC로 잠깐 멈춘 것**이 Backup 입장에서 전부 똑같이 "heartbeat 안 옴"으로 보인다는 점이다.

| timeout | 장점 | 단점 |
| --- | --- | --- |
| 짧게 (200ms) | 진짜 장애를 빨리 감지 | 잠깐 느려진 것도 장애로 오판 |
| 길게 (3초) | 오판이 줄어듦 | 진짜 장애 때 3초 동안 서비스 중단 |

### 3. 승격 — 누가 새 Primary가 되나

Backup 둘이 동시에 "내가 Primary"라고 하면 split brain이다. 그래서 모두가 똑같이 아는 규칙이 필요하다.

| 규칙 | 설명 | 위험 |
| --- | --- | --- |
| NodeId 순서 | 살아있는 노드 중 번호가 작은 쪽 | 비동기 복제였으면 덜 받은 노드가 올라가 데이터 유실 가능 |
| 가장 최신 데이터 | 받은 쓰기가 가장 많은 노드 | 유실 최소화. VR이 쓰는 규칙 |

Primary-Backup 기본형에는 이 규칙이 **없다**. 밖에서 따로 붙여야 한다. VR은 프로토콜 안에 포함돼 있다.

### 4. 옛 Primary가 돌아오면 — epoch와 fencing

```
A: epoch 1 Primary → GC로 2초 멈춤
B: timeout 지남 → epoch 2로 승격, 모두에게 "지금은 epoch 2" 알림
A: 깨어남 → 자기가 아직 Primary인 줄 알고 epoch 1로 메시지 보냄
받는 쪽: "지금 epoch 2인데 1이네?" → 거부 (STALE_EPOCH)
A: "아, 2구나" → epoch 2의 Backup으로 강등
```

모든 메시지에 epoch를 붙이고, 낮은 epoch는 거부한다. 이것이 fencing이다.

> **epoch를 올리는 것과 fencing은 다른 문제다.**
>
> 번호만 올리고 아무도 거부하지 않으면 소용없다. "이전 Primary가 자기가 더 이상 Primary가 아님을 어떻게 알고 거절하는가"를 별도로 정의해야 한다.

강등된 A는 멈춰 있는 동안 놓친 쓰기를 따라잡아야(recovery) 다음 승격 후보가 될 수 있다.

### 5. 클라이언트 — redirect와 retry

클라이언트는 아직 "Primary = A"로 알고 있다.

| 상황 | 처리 |
| --- | --- |
| A가 완전히 죽음 | 연결 실패 → 다른 노드에 현재 Primary를 묻고 재시도 |
| A가 Backup으로 강등됨 | A가 `NOT_PRIMARY` + "B로 가라"(redirect) 응답 → B에 재시도 |

**중복 실행 문제**: A가 처리는 했는데 응답만 못 보내고 죽으면 클라이언트는 B에 같은 요청을 또 보낸다. SET은 두 번 해도 결과가 같지만, 카운터 증가 같은 연산은 틀어진다. VR은 **client table**(클라이언트별 마지막 요청 번호)로 같은 요청을 두 번 실행하지 않는다. 우리 코드에서는 `ClientId` + `OperationId`가 그 식별자다.

## Viewstamped Replication이 다르게 하는 것

```
            Prepare #7
Primary ──────────────► Backup 1 ──OK──► Primary
(view 3)  ─ ─ ─ ─ ─ ─ ► Backup 2 (느림)

Primary + Backup 1 = 3대 중 2대(과반수) → commit
```

| 항목 | Primary-Backup | VR |
| --- | --- | --- |
| 쓰기 확정 조건 | 직접 정함 (전부 / 즉시) | **과반수**가 PrepareOK를 보내면 commit |
| 느린 Backup 1대 | 동기면 같이 느려짐 | 과반수만 채우면 되므로 영향 없음 |
| Primary 교체 | 외부 규칙 필요 | **view change** 프로토콜 내장. 가장 최신 로그를 가진 노드가 새 Primary |
| 옛 Primary 차단 | 직접 구현 | view 번호가 낮은 메시지 무시 |
| 재시도 중복 | 없음 | client table로 방지 |

VR Revisited 논문 중 4.1(정상 동작), 4.2(view change)가 A1 범위다. 4.3(recovery)과 reconfiguration은 범위 밖이다.

## 측정할 것

| 항목 | 측정 방법 |
| --- | --- |
| 정상 상태 replication 비용 | 복제 없음 / 비동기 / 동기 / VR 각각 SET p50·p99, throughput |
| end-to-end failover 시간 | Primary kill 시점 → 클라이언트가 새 Primary에 첫 쓰기 성공 시점. 감지 + 승격 + 클라이언트 재연결로 구간 분리 |
| failover 후 데이터 유실 | OK를 받은 쓰기 중 failover 뒤 사라진 개수 |
| fencing 검증 | 옛 Primary의 쓰기가 거절되는지 |
| Backup 1대 지연 시 | 쓰기 지연 변화 (Primary-Backup 동기 vs VR) |

## 우리 구현에서 직접 정해야 할 것

DynamoDB처럼 실제 시스템도 내부 임계값은 공개하지 않으므로 다음 규칙을 우리가 정의하고 검증한다.

```
- heartbeat 간격과 timeout은 얼마로 할 것인가? (감지 속도 vs 오판 트레이드오프 곡선으로 결정)
- SET을 OK로 응답하는 조건은? (Backup 전부 / 과반수 / 즉시)
- Backup에서 GET을 허용할 것인가? (stale 값 위험)
- 재시도 중복 제거는 엔진과 복제 계층 중 어디서 하는가?
- 옛 Primary는 어떤 메시지를 받고 자기 강등을 알게 되는가?
```

## 참고 레퍼런스

- Liskov, Cowling. *Viewstamped Replication Revisited*. MIT-CSAIL-TR-2012-021. https://pmg.csail.mit.edu/papers/vr-revisited.pdf
- Budhiraja, Marzullo, Schneider, Toueg. *The Primary-Backup Approach*. Distributed Systems (2nd ed.), 1993.
- Redis 공식 문서, *Replication*. https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
- Redis 공식 문서, *High availability with Redis Sentinel*. https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
- Kleppmann. 『데이터 중심 애플리케이션 설계』 5장 복제, 8장 분산 시스템의 골칫거리, 9장 펜싱 토큰.
- OpenKache 프로젝트 요구명세서, 공통 모듈 개발 명세서 v1.0 (A1 범위·측정 항목 근거)
