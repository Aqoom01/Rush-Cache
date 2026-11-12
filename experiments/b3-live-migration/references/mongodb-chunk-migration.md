# MongoDB Chunk Migration

## 출처

### 공식 문서

[Sharded Cluster Balancer](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/)

- MongoDB Manual. 읽을 부분: **Range Migration Procedure**, **Asynchronous Range Migration Cleanup**, **Balancer Internals**, **Migration Thresholds**

[Data Partitioning with Chunks](https://www.mongodb.com/docs/manual/core/sharding-data-partitioning/)

- chunk(range)의 개념, jumbo chunk, range 이동의 배경

## 핵심 아이디어

shard key 범위인 chunk(range)를 다른 shard로 옮긴다. **이동 중에는 원래 shard(source, donor)가 계속 요청을 처리**하고, destination(recipient)이 복사 → 변경분 동기화를 끝낸 뒤 **metadata를 한 번 갱신해 소유권을 넘긴다**. source의 남은 데이터는 나중에 지운다.

```text
1. balancer → source에 moveRange
2. source가 이동 시작. 그 범위의 요청은 계속 source로 가고 source가 write를 책임진다
3. destination이 필요한 index 생성
4. destination이 범위의 문서를 요청해 복사
5. 마지막 문서까지 받으면 이동 중 바뀐 내용을 동기화
6. 완전히 따라잡으면 source가 config database의 metadata를 새 위치로 갱신  ← commit
7. 열린 cursor가 없어지면 source의 복사본 삭제 (비동기)
```

Pre-copy, 변경 로그 따라잡기(CDC), metadata 기반 소유권 commit의 조합이다.

## 효과와 한계

### 기대 효과

- write를 막는 구간을 "마지막 변경 적용 + metadata 갱신"으로만 좁힌다.
- metadata commit 전까지는 언제든 취소할 수 있다. source가 여전히 정답이다.
- 삭제를 늦춰 commit 직후 문제가 생겨도 데이터가 남아 있고, 삭제 부하도 분산된다.

### 한계

- shard 하나는 한 번에 이동 하나에만 참여한다. shard가 n개면 동시 이동은 최대 ⌊n/2⌋개다.
- 너무 큰 chunk(jumbo)는 먼저 split해야 옮길 수 있다.
- 낡은 라우팅을 가진 router(mongos)는 source의 거절을 받고 metadata를 다시 읽어야 한다.

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. 이동 중 요청은 누가 처리하는가?

**답**

source shard다. 이동 중 그 범위의 요청은 source로 가고, source가 들어오는 write를 책임진다. destination은 metadata가 바뀌기 전까지 요청을 받지 않는다.

**근거**

[Range Migration Procedure](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/) 2단계: "During the migration process, operations to the range are sent to the source shard. The source shard is responsible for incoming write operations for the range."

### 2. commit point는 무엇이고, 그 전에 실패하면?

**답**

source가 config database의 metadata에 범위의 새 위치를 기록하는 순간이다. 그 전에 실패하면 metadata가 안 바뀌었으므로 source가 여전히 정답이고 이동을 취소하면 된다.

**근거**

[Range Migration Procedure](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/) 5·6단계(완전 동기화 후 metadata 갱신). "그 전 실패는 취소" 부분은 절차에서 따라 나오는 해석이며 실패 처리 자체는 이 문서에 **자료에 없음**.

metadata 갱신 직전에 write를 잠깐 막는 구간(critical section)도 이 공식 절차에는 **자료에 없음**. 서버 내부 설명에서 확인할 것.

### 3. source의 남은 데이터를 바로 지우지 않는 이유는?

**답**

metadata 갱신 뒤에도 source에 열린 cursor가 남아 있을 수 있고, 삭제 작업을 기다리지 않고 다음 이동을 시작할 수 있어서다. 삭제는 range deleter가 비동기로 한다.

**근거**

[Range Migration Procedure](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/) 7단계와 **Asynchronous Range Migration Cleanup**.

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| 동시 이동 수 | shard당 1개, 전체 최대 ⌊n/2⌋ (Balancer Internals) | B3 노드당 동시 job 수 기본값 |
| 이동 시작 조건 | shard 간 데이터 차이가 range 크기의 3배 이상 (Migration Thresholds) | 계획은 B2 담당. 참고만 |
| 낡은 router | source가 낡은 버전 요청을 거절 → router가 metadata 재조회 | B1 `MOVED` 판정과의 대응 |
| critical section | 공식 절차 문서에 없음 | 서버 내부 문서 |

## 프로젝트 적용

`pre-copy`(Source based)를 실제 KV 제품이 어떻게 단계로 나누는지 보여 주는 가장 가까운 모델이다.

### 적용할 내용

- 이동 중에는 source가 Tablet 전체를 처리한다. B1의 "표상 주인이 끝까지 처리"와 같다.
- 복사 → 따라잡기 → metadata 갱신(commit) → 지연 정리의 순서. B3 상태로는 `COPYING → CATCHING_UP → SWITCHING → CLEANING_UP`.
- commit 전 실패는 취소, commit 후 문제는 역방향 job.

### 적용하지 않는 내용

- index 생성 단계 (KV에 보조 index 없음).
- 비동기 지연 삭제는 1단계에서 넣지 않는다. in-process라 이득이 없고, 후속 실제 노드 연계에서 다시 본다 (B3 design.md §4.1).

### 직접 설계하고 기록할 내용

- freeze(critical section) 구간의 정확한 범위와 그동안의 read·write 처리
- 노드당 동시 job 수 (MongoDB의 shard당 1개를 기본 후보로)
