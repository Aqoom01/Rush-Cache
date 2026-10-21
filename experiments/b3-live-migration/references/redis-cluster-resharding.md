# Redis Cluster Resharding

## 출처

### 스펙 문서

[Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)

- Redis 공식 문서
- 읽을 부분: **Hash slots**, **MOVED Redirection**, **Live reconfiguration**, **ASK redirection**
- slot을 옮기는 동안 source·destination의 상태와 client redirect 규칙을 확인한다.

### 명령어 문서

[CLUSTER SETSLOT](https://redis.io/docs/latest/commands/cluster-setslot/)

- `MIGRATING`, `IMPORTING`, `NODE` 하위 명령으로 이동 시작·완료를 표시하는 절차.

### 프로젝트 안의 관련 문서

- [B1 Redis Cluster 고정 해시 슬롯](../../b1-tablet-distribution/references/redis-cluster-hash-slots.md): 같은 스펙을 슬롯 배치 관점에서 정리했다. `cluster.c`의 `getNodeByQuery()`, Valkey 9.0 atomic slot migration까지 다룬다. 이 문서는 이동(resharding) 관점만 다룬다.

## 핵심 아이디어

key 공간을 16,384개 hash slot으로 고정해 두고, slot을 노드 사이로 옮긴다. 옮기는 동안의 중간 상태를 숨기지 않고 **상태와 redirect 종류로 드러낸다**.

| 이름 | 위치 | 의미 |
|---|---|---|
| `MIGRATING` | source | 이 slot의 key를 destination으로 옮기는 중이다 |
| `IMPORTING` | destination | source에서 넘어오는 key를 받는 중이다 |
| `ASK` | source → client | 이번 요청만 destination으로 다시 보내라. slot의 주인은 아직 안 바뀌었다 |
| `MOVED` | 아무 노드 → client | slot의 정식 주인이 바뀌었다. client는 slot map을 갱신한다 |

```text
SETSLOT source MIGRATING / destination IMPORTING
    ↓
source에서 key를 하나씩 destination으로 이동 (MIGRATE)
    ↓  이동 중 요청: source에 key가 있으면 source가 처리, 없으면 ASK
모든 key 이동 완료
    ↓
SETSLOT NODE로 slot 주인을 destination으로 확정
    ↓
이후 source로 온 요청은 MOVED
```

## 효과와 한계

### 기대 효과

- 이동 중에도 요청이 끊기지 않는다. key 단위로 옮기므로 Tablet 전체를 멈출 필요가 없다.
- "이번 요청만 다른 곳"(`ASK`)과 "앞으로 계속 다른 곳"(`MOVED`)을 나눠, client가 이동 중의 일시적 상태로 slot map을 잘못 바꾸지 않는다.

### 한계

- **client가 `ASK`를 이해해야 한다:** `ASKING` 명령을 붙여 다시 보내야 하므로 client 구현이 복잡해진다.
- **multi-key 연산 제약:** 이동 중에는 한 명령의 key들이 source와 destination에 흩어질 수 있어 일부 multi-key 명령이 실패한다.
- **key 단위 이동은 느리다:** key마다 직렬화·전송·삭제를 반복한다.
- **같은 slot을 두 노드가 주장하는 충돌:** config epoch로 풀지만, 이동 완료 표시가 노드마다 늦게 퍼질 수 있다.

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. 이동 중인 slot의 key를 source에 요청했는데 이미 옮겨졌다면?

**답**

source가 `ASK`와 destination 주소를 돌려준다. client는 그 요청 하나만 `ASKING`을 붙여 destination에 다시 보낸다. slot map 캐시는 바꾸지 않는다. destination은 `IMPORTING` 상태에서 `ASKING`이 붙은 요청만 처리한다.

source에 key가 아직 있으면 source가 그대로 처리한다.

**근거**

[Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)의 **ASK redirection** 절.

### 2. `ASK`와 `MOVED`를 왜 나누는가?

**답**

이동 중에는 같은 slot 안에서도 일부 key만 destination에 있다. `ASK`를 받고 client가 slot map을 바꾸면, 아직 source에 남은 key를 destination에서 찾게 되어 없는 것처럼 보인다. `MOVED`는 slot 전체의 주인이 확정된 뒤에만 나간다.

| | `ASK` | `MOVED` |
|---|---|---|
| 언제 | 이동 중, source에 그 key가 없을 때 | 이동 완료 후 |
| client 캐시 | 바꾸지 않음 | 갱신 |
| 의미 | 이번 요청만 | 앞으로 계속 |

**근거**

[Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)의 **MOVED Redirection**, **ASK redirection** 절.

### 3. 이동 완료는 어떻게 확정되고 퍼지는가?

**답**

모든 key를 옮긴 뒤 `CLUSTER SETSLOT <slot> NODE <destination>`으로 slot 주인을 destination으로 바꾼다. 이 정보는 cluster bus(노드 간 gossip)로 퍼지고, 충돌하면 config epoch가 큰 쪽이 이긴다. 그 뒤 source에 온 요청은 `MOVED`를 받는다.

**근거**

[CLUSTER SETSLOT](https://redis.io/docs/latest/commands/cluster-setslot/) 문서의 resharding 절차, [Redis Cluster Specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)의 **Live reconfiguration** 절.

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| key 이동 단위 | `MIGRATE`로 key(또는 key 묶음)를 옮기고 source에서 지운다 | 이동 중 같은 key에 write가 오면 어느 쪽이 처리하는지의 정확한 경계 |
| 완료 전파 | `SETSLOT NODE`와 gossip | 모든 노드에 퍼지기까지 걸리는 시간과 그 사이의 요청 처리 |
| 원자적 slot 이동 | 최신 Valkey에는 slot 단위로 원자적으로 옮기는 기능이 있다 | 기존 key 단위 방식과의 차이 (B1 design.md §4.1이 이 모양을 택했다) |

## 프로젝트 적용

Redis의 상태와 redirect 구분을 B3 job 상태와 응답 규칙의 기준으로 쓴다.

### 적용할 내용

- 이동 중인 Tablet의 상태를 source·destination에 명시적으로 둔다 (B3 `JobState`).
- 이동 완료 후 source에 온 요청은 `MOVED`(새 주인, 새 `RoutingVersion`)로 응답한다.
- 이동 완료 확정(주인 변경)을 데이터 이동과 분리된 별도 단계로 둔다.

### 적용하지 않는 내용

- **`ASK` 임시 redirect:** B1이 "이동 중 라우팅은 `ASK` 없음, 표상 주인이 끝까지 처리하고 핸드오프 순간에만 `MOVED`"로 정했다(B1 design.md §4.1). B3의 `stop-and-copy`, `pre-copy`는 cutover 전까지 source가 Tablet 전체를 처리하므로 `ASK`가 필요 없다.
- key 하나씩 옮기는 방식. B3는 Tablet 단위 snapshot·delta로 옮긴다.

### 직접 설계하고 기록할 내용

- cutover 직후, 새 표가 모든 노드·client에 퍼지기 전의 요청 처리
- source가 `MOVED`를 낼 때 응답에 실을 정보(새 owner, version)
- `post-copy`처럼 이동 중 일부 key만 destination에 있는 variant를 넣을 때 `ASK`류 경로가 다시 필요한지

비교 실험과 불변식은 프로젝트의 `design.md` §5·§6을 따른다.
