# Raft — 투표 기반 리더 선출

Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, USENIX ATC 2014 — https://raft.github.io/raft.pdf

## Raft란?

VR과 같은 **리더 1대 + 번호(term) + 과반수** 구조의 복제 프로토콜이다. 다른 점은 리더를 **투표로 뽑는다**는 것과, 리더가 죽으면 **랜덤 timeout**으로 후보가 먼저 나서는 쪽이 이긴다는 것이다.

etcd, TiKV, CockroachDB, Consul 등 실제 시스템에서 가장 널리 쓰이는 합의 알고리즘이다.

> **VR이 있는데 왜 Raft도 보나?**
>
> 같은 문제를 다른 방식으로 푼다. VR은 "다음 Primary는 view mod N"으로 **정해져 있고**, Raft는 "먼저 손 든 노드가 **투표로** 리더가 된다". failover 시간과 split vote(동시 입후보) 위험이 다르게 나온다. 둘을 같은 조건에서 재면 "선출 방식"이 failover 시간에 주는 영향을 분리해 볼 수 있다.

## 기본 개념

| 용어 | 의미 |
| --- | --- |
| Leader / Follower / Candidate | Primary / Backup / 입후보 중인 Backup |
| term | 리더 시대 번호. 선거마다 1 증가 (= VR의 view = 우리의 `OwnershipEpoch`) |
| currentTerm, votedFor | 노드가 **디스크에** 저장해야 하는 두 값. "지금 term"과 "이 term에서 누구에게 투표했나" |
| log[] | (term, 명령) 항목 배열. 역시 디스크에 저장 |
| commitIndex | 과반수에 복제돼 실행해도 되는 마지막 인덱스 |
| election timeout | follower가 리더 소식 없이 기다리는 시간. **노드마다 랜덤** (논문 예시 150~300ms) |
| heartbeat | 리더가 빈 AppendEntries를 보내는 주기. election timeout보다 훨씬 짧게 |
| split vote | 두 후보가 동시에 나서 둘 다 과반수를 못 채우는 상황 |

노드는 RPC 두 개만 쓴다.

```
RequestVote(term, candidateId, lastLogIndex, lastLogTerm)  → (term, voteGranted)
AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit) → (term, success)
```

## 리더 선출 — 랜덤 timeout과 투표

```
Follower: election timeout 만료
   │ term++, 자기에게 투표, status = Candidate
   ▼
모두에게 RequestVote(term, 내 마지막 로그 위치)
   │
   ├─ 과반수 득표 → Leader. 즉시 heartbeat 전송
   ├─ 더 큰 term의 AppendEntries 받음 → Follower로 복귀
   └─ timeout 안에 결판 안 남(split vote) → 새 랜덤 timeout 후 다시 term++ 재시도
```

투표 규칙 두 가지가 안전성을 만든다.

| 규칙 | 내용 | 막는 사고 |
| --- | --- | --- |
| term당 1표 | 같은 term에서는 한 노드에게만 투표한다 (`votedFor`를 디스크에 저장) | 리더 2명 |
| 로그 최신성 | 후보의 마지막 로그가 내 것보다 **같거나 최신**(term이 크거나, 같으면 길이가 길거나)일 때만 투표한다 | 덜 받은 노드가 리더가 돼 commit된 쓰기 유실 |

두 번째 규칙이 VR의 "DOVIEWCHANGE 중 최신 로그 선택"에 해당한다. VR은 **새 Primary가 모아서 고르고**, Raft는 **투표자가 거부해서 거른다**.

> **왜 timeout을 랜덤으로 하나?**
>
> 모두 같은 timeout이면 리더가 죽는 순간 전부 동시에 입후보해 표가 갈린다. 랜덤이면 보통 한 노드가 먼저 깨어나 혼자 과반수를 가져간다. 논문은 150~300ms 범위에서 split vote가 거의 안 난다고 보고한다.

## 정상 동작 — AppendEntries

```
클라이언트 ──명령──► Leader: log에 append
Leader ──AppendEntries(entries, prevLogIndex/Term)──► Follower들
Follower: prevLogIndex/Term이 내 로그와 일치하면 append, success=true
Leader: 과반수 success → commitIndex 전진 → 실행 → 클라이언트 응답
다음 AppendEntries의 leaderCommit으로 Follower도 실행
```

VR의 PREPARE/PREPAREOK/COMMIT와 같은 흐름이다. 차이는 **로그 불일치 처리**다. Follower의 로그가 리더와 다르면(옛 리더 시절 쌓인 것) 리더가 `nextIndex`를 하나씩 뒤로 물리며 일치점을 찾고, **그 뒤를 리더 것으로 덮어쓴다**. 리더는 자기 로그를 절대 지우지 않는다.

> commit 규칙 주의: 리더는 **현재 term의 항목**이 과반수에 복제됐을 때만 commitIndex를 올린다. 이전 term 항목은 그때 같이 commit된다 (논문 5.4.2, Figure 8의 함정).


## fencing — term이 모든 RPC에 붙는다

```
받은 term < 내 term  →  거부 (success=false / voteGranted=false)
받은 term > 내 term  →  내 term을 갱신하고 Follower로 내려간다 (리더였더라도)
```

GC로 멈췄다 깨어난 옛 리더가 옛 term으로 AppendEntries를 보내면 전부 거부당한다. 응답에 담긴 더 큰 term을 보고 즉시 Follower로 내려간다. VR과 같은 규칙이다.

## 효과

1. **선출이 빠르고 단순하다.** 랜덤 timeout 하나로 리더 1명이 자연스럽게 나온다. VR처럼 STARTVIEWCHANGE → DOVIEWCHANGE → STARTVIEW 3단계가 아니라 RequestVote 1왕복이다.
2. **노드가 재시작해도 돌아온다.** currentTerm·votedFor·log를 디스크에 쓰므로 recovery 프로토콜 없이 복귀한다. (VR Revisited는 이를 recovery로 대체)
3. **검증된 구현이 많다.** etcd/raft 등 레퍼런스 구현과 TLA+ 증명이 있어 "우리가 맞게 만들었나"를 비교 확인할 수 있다.
4. **읽기 최적화가 정리돼 있다.** ReadIndex, lease read 등 리더에서 로그 없이 읽는 방법이 논문 §8과 etcd에 있다.

## 한계

1. **split vote가 나면 failover가 길어진다.** 랜덤이 겹치면 timeout 한 주기를 통째로 더 쓴다. timeout을 줄이면 겹칠 확률이 오른다.
2. **디스크 쓰기가 필수다.** 투표할 때마다 votedFor를 fsync해야 안전하다. 캐시에는 부담이고, 우리처럼 메모리만 쓰면 Raft의 안전성 전제가 깨진다 → `EpochStore`로 term/votedFor만 저장하는 절충이 필요하다.
3. **Follower가 뒤처지면 리더가 로그를 밀어 넣는 비용이 크다.** nextIndex 후퇴가 한 칸씩이라 큰 불일치는 느리다.
4. **리더 병목은 VR과 같다.** 모든 쓰기가 리더를 거친다.
5. **Pre-Vote 없이는 분할 복귀 노드가 선거를 흔든다.** 분할됐던 노드가 term을 혼자 올리다 돌아오면 멀쩡한 리더를 끌어내린다 (논문 §9.6, etcd의 PreVote로 해결).

## 실험을 위해 직접 정해야 할 것

```
- 노드 수:               3대. 과반수 2
- election timeout 범위:  150~300ms (논문) / 50~100ms / 500~1000ms 비교 → 감지 속도 vs split vote 횟수
- heartbeat 간격:         election timeout 최솟값의 1/3 이하
- 영속화:                 currentTerm·votedFor만 EpochStore에 저장, log는 메모리 (안전성 약화를 보고서에 명시)
- Pre-Vote:               미구현 (한계로 기록)
- 읽기:                   리더 로그 경유만 (ReadIndex는 범위 밖)
- VR과 비교 지점:          "리더 죽음 → 새 리더 첫 heartbeat"까지 시간. 선출 방식만 다르고 나머지 조건은 동일하게
```

## 우리 코드와의 대응

| Raft | 우리 저장소 |
| --- | --- |
| term | `OwnershipEpoch` |
| currentTerm, votedFor 영속화 | `EpochStore` + `EpochRecord(epoch, votedFor)` — **이미 votedFor 필드가 있다** |
| 낮은 term 거부 | `STALE_EPOCH` |
| Follower가 클라이언트 요청 받음 | `NOT_PRIMARY` + `Redirect(leaderId)` |
| RequestVote / AppendEntries | `PeerMessage` record 2개 |

## 참고

- Ongaro, Ousterhout. *In Search of an Understandable Consensus Algorithm* (2014): https://raft.github.io/raft.pdf — §5 선출·복제·안전성, §9.6 Pre-Vote
- Raft 공식 사이트 시각화: https://raft.github.io/
- etcd/raft 라이브러리 (Go, 레퍼런스 구현): https://github.com/etcd-io/raft
- Liskov, Cowling. *VR Revisited* — 비교 대상. 같은 구조, 다른 선출
