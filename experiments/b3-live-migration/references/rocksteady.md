# Rocksteady

## 출처

### 관련 논문

[Rocksteady: Fast Migration for Low-latency In-memory Storage](https://www.flux.utah.edu/download?uid=257)

- ACM SOSP 2017, Chinmay Kulkarni, Aniraj Kesavan, Tian Zhang, Robert Ricci, Ryan Stutsman (University of Utah). [ACM DL](https://dl.acm.org/doi/10.1145/3132747.3132784)
- 핵심 절: **§3 Rocksteady Design**, **§3.3 Priority Pulls**, **§3.4 Lineage for Safe, Lazy Re-replication**
- 관련 절: **§2.3 Barriers to Fast Migration**(기존 RAMCloud 방식의 병목), **§3.1 Task Scheduling, Parallelism, and QoS**
- 평가: **§4.2 Migration Impact and Ownership**, **§4.4 Asynchronous Batched Priority Pulls**
- RAMCloud(인메모리 KV) 위에 구현. 읽는 순서: **§2 동기 → §3 설계 → §3.3·§3.4 → §4.2**

## 핵심 아이디어

이동을 시작하는 순간 **소유권을 destination(target)으로 넘기고**, target이 필요한 데이터를 source에서 **당겨 온다(pull)**.

```text
1. client가 tablet을 split한 뒤 target에 MigrateTablet 요청
2. 소유권이 즉시 target으로 넘어감. source의 이동 대상 tablet은 이후 변경되지 않음(immutable)
3. source로 온 요청 → "더 이상 주인 아님" 응답 → client가 coordinator에서 표를 다시 받아 target으로
4. target: write는 바로 처리, read는 그 record가 도착한 뒤에만 처리
5. background Pull: target이 source의 key hash 공간을 겹치지 않는 구간으로 나눠 병렬로 당김
6. PriorityPull: 아직 안 온 record를 client가 읽으면 그 key를 먼저 당김
7. 모두 받고 side log를 commit하면 완료
```

| 경로 | 동작 | 우선순위 |
|---|---|---|
| background Pull | target이 key hash 구간마다 고정 크기(평가에서 20 KB)씩 요청. 구간이 겹치지 않아 source는 동기화가 필요 없다. target worker가 비었을 때만 새 Pull을 보낸다(흐름 제어) | source에서 가장 낮음 |
| PriorityPull | client read가 안 온 record를 찾으면 비동기로 요청하고 client에게는 잠시 후 재시도하라고 응답. 진행 중인 PriorityPull이 있으면 key를 모아 묶어 보내고 중복은 버린다(평가에서 한 번에 최대 16개) | source에서 가장 높음 |

**source는 migration 상태를 갖지 않는다.** "Sources keep no migration state, and their migrating tablets are immutable"(§3). 받은 record의 재복제는 이동 중에 하지 않고, target의 side log를 나중에 commit할 때 한꺼번에 한다(lineage, §3.4).

## 효과와 한계

### 기대 효과

- 정지가 없다. 높은 부하에서 약 30초 동안 758 MB/s로 옮기면서 동시 요청의 99.9번째 백분위 지연을 250 µs 안팎으로 유지했다(이동 전 45 µs, §4.2·초록).
- 기존 RAMCloud 이동은 정상 상태에서 약 130 MB/s였다(§2.3). 약 5~6배 빠르다.
- target이 뜨거운 record를 먼저 가져가므로 source의 서비스 부하가 바로 줄어든다(§3.1.1).

### 한계

- **첫 read 지연:** 아직 안 온 record를 읽으면 PriorityPull을 기다려야 한다. PriorityPull을 끄면 904 MB/s로 더 빠르지만 요청이 record가 올 때까지 멈춰 timeout이 난다(§4.2).
- **source 의존:** 이동이 끝날 때까지 source가 살아 있어야 한다. source나 target이 죽으면 소유권을 source로 되돌리고, target log의 꼬리와 source log를 함께 replay하는 복구가 필요하다. 보통 복구의 약 2배 비용이다(§3.4).
- **RAMCloud 구조 의존:** lineage 복구는 RAMCloud의 log 구조와 backup에 기댄다. 여러 log에 걸친 복구 구현은 논문 시점에 "ongoing"이었다(§3.4).

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. 이동이 시작되면 요청은 누가 받고, write와 read는 어떻게 다른가?

**답**

target이 받는다. 소유권이 시작과 동시에 넘어가기 때문이다. write는 바로 처리하고, read는 그 record가 도착한 뒤에만 처리한다. 안 온 record의 read는 PriorityPull을 보내고 client에게 잠시 후 재시도하라고 답한다. source에 늦게 온 요청은 "더 이상 주인 아님"을 받고 client가 표를 다시 받는다.

**근거**

§3: "Writes can be serviced immediately; reads can be serviced only after the records requested have been migrated." §2.4: Rocksteady는 "immediately and safely shifts ownership from the source to the target". PriorityPull의 비동기 처리는 §3.3.

### 2. 소유권을 처음에 넘기면 무엇이 좋아지고, 그 대가로 무엇이 필요한가?

**답**

좋아지는 것:
- source의 이동 대상 데이터가 바뀌지 않으므로(immutable) dirty 추적과 재전송이 없다.
- source는 migration 상태 없이 Pull에만 답하면 된다.
- 뜨거운 record가 PriorityPull로 먼저 와서 source 부하가 빨리 준다.

필요한 것:
- target이 어떤 record가 왔는지 알고, 안 온 record의 read를 미뤄야 한다.
- 이동 중 장애에 대비한 복구 경로. Rocksteady는 target log 꼬리에 대한 의존(어느 master의 log인지, 시작 offset, 정수 두 개)을 coordinator에 등록하고, 장애 시 소유권을 source로 되돌려 두 log를 함께 replay한다.

소유권을 source에 둔 채 동기적으로 재복제하는 변형은 549 MB/s로 27.7% 느렸다.

**근거**

source immutable과 상태 없음은 §3. 복구 절차는 §3.4. 동기 재복제 비교는 §4.2.

### 3. 캐시인 OpenKache에서는 무엇을 단순화할 수 있나?

**답**

- **복구:** 캐시는 원본 DB가 있으므로 이동 중 source가 죽으면 "아직 안 온 key는 MISS"로 끝낼 수 있다. lineage 같은 log 기반 복구가 필요 없다.
- **read 대기:** PriorityPull을 기다리는 대신 MISS로 응답하는 선택지도 있다. 대신 hit ratio가 떨어지고 원본 DB 부하가 는다.
- **삭제는 단순화할 수 없다:** target에서 지운 key를 늦게 도착한 Pull이 되살리면 stale HIT가 된다. 이번에 확인한 범위에는 이 경우의 규칙이 나오지 않아 원문에서 확인해야 한다.

**근거**

**자료에 없음**. 프로젝트의 해석이다(B3 design.md §2 "캐시라서 달라지는 점").

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| 병렬 Pull 단위 | key hash 공간을 worker 수보다 조금 많은 구간으로 나눔(평가 8개, §4.1) | B1 Tablet이 이미 hash 구간이므로 Tablet 안을 다시 나눌지 |
| target 측 replay | 빈 worker 누구나 replay, worker별 side log로 경합 회피, 128 B record에서 3 GB/s 이상(§3.1.3, §4.5) | 우리 worker 모델에서 Tablet이 한 worker에 묶이는 것과의 충돌 |
| 이동 중 지연 | 99.9번째 백분위가 250 µs 근처에서 시작해 hot record가 오면서 약 183 µs로 감소(§4.2) | |
| 늦게 도착한 Pull과 새 write의 순서 | 확인 못 함 | 원문 §3에서 version 처리 확인 |

## 프로젝트 적용

`post-copy`(Destination based) variant의 기준 논문이다. 1단계 `stop-and-copy` Baseline과 후속 `pre-copy` 비교 뒤에 확장 후보로 추가한다 (B3 design.md §1).

### 적용할 내용

- 시작 시 라우팅 표의 주인을 destination으로 바꾸고(commit이 처음에 일어남), destination이 없는 key를 source에서 pull, 나머지는 background 복사.
- B1의 "`ASK` 없음, 표상 주인이 처리"와 맞는다. 표상 주인(destination)이 처음부터 처리하고, source에는 내부적으로만 묻는다.
- source로 늦게 온 요청은 `MOVED`(B1).
- background pull은 destination이 비었을 때만 보내는 흐름 제어, 요청이 기다리는 key의 우선 pull.

### 적용하지 않는 내용

- lineage 기반 복구와 side log. 캐시라서 "안 온 key는 MISS"로 대신할 수 있다.
- client 주도 split과 MigrateTablet. 계획은 B2·B1, 실행은 B3 job이 한다.

### 직접 설계하고 기록할 내용

- destination 쪽 진행 상태(받은 구간)와 안 온 key read의 처리: PriorityPull을 기다릴지, MISS로 답할지
- 늦게 도착한 pull 결과가 destination의 새 write나 DELETE를 덮지 않게 하는 규칙
- source 장애 시 job의 결말 (남은 key는 MISS로 두고 `DONE` 처리할지)
- post-copy 실험 지표에 "첫 read 지연"과 "pull 대기 중인 read 수" 추가
