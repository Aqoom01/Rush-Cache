# Chain Replication과 CRAQ — 사슬형 복제

van Renesse & Schneider, *Chain Replication for Supporting High Throughput and Availability*, OSDI 2004 — https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf
Terrace & Freedman, *Object Storage on CRAQ: High-throughput chain replication for read-mostly workloads*, USENIX ATC 2009 — https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf

## Chain Replication이란?

노드를 **일렬 사슬**로 세우고, 쓰기는 **머리(head)** 로 들어가 사슬을 따라 **꼬리(tail)** 까지 흐른 뒤 꼬리가 응답한다. 읽기는 **꼬리에서만** 한다.

Primary가 모두에게 뿌리는 VR·Raft와 달리, 각 노드는 **바로 다음 노드 하나**에게만 전달한다.

> **왜 A1 후보인가?**
>
> "Primary 1대가 쓰기를 받는다"는 점에서 Active-Passive다. 그런데 과반수가 아니라 **전원 통과**로 확정하고, 읽기는 쓰기 노드가 아닌 꼬리가 맡는다. 같은 3노드에서 VR·Raft와 완전히 다른 지연·처리량·failover 특성이 나온다. 멘토가 말한 "기존 캐시 시스템에 채택되지 않은 논문 아이디어"에 해당한다.

> **과반수가 아니라 전원?**
>
> VR·Raft는 2f+1대 중 f+1대만 받으면 확정이다. Chain은 **f+1대 전부** 통과해야 확정이다. 대신 f대 장애를 견디는 데 2f+1이 아니라 **f+1대면 된다**. 3대면 2대까지 죽어도 된다.

## 기본 개념

| 용어 | 의미 |
| --- | --- |
| head | 사슬의 첫 노드. 모든 쓰기를 받는다 |
| tail | 사슬의 마지막 노드. 쓰기를 확정하고 응답하며, 모든 읽기를 처리한다 |
| 중간 노드 | 앞에서 받아 뒤로 넘긴다 |
| Sent 목록 | 각 노드가 "뒤로 보냈지만 아직 tail이 확정 안 한" 쓰기를 들고 있는 목록 |
| ack | tail이 확정하면 사슬을 **거꾸로** 타고 올라가는 확인. 받은 노드는 Sent에서 지운다 |
| master | 사슬 구성(누가 head·tail인지)을 관리하는 **외부** 서비스. 논문은 Paxos 기반 서비스를 가정 (실제로는 ZooKeeper 등) |
| clean / dirty (CRAQ) | 객체 버전이 tail까지 확정됐는지(clean) 아직 흐르는 중인지(dirty) |

```
쓰기:  클라이언트 ──► [head] ──► [중간] ──► [tail] ──응답──► 클라이언트
읽기:  클라이언트 ──────────────────────► [tail] ──응답──► 클라이언트
ack:                [head] ◄── [중간] ◄── [tail]
```

## 정상 동작

**쓰기**는 head에서 시작해 순서대로 흐른다. 각 노드는 받은 순서대로 적용하고 뒤로 넘긴다. tail이 적용하는 순간이 **확정(commit)** 이고, tail이 클라이언트에 응답한다.

**읽기**는 tail에서만 처리한다. tail에 있는 값은 사슬 전체를 통과한 값이므로 항상 확정된 최신 값이다. 그래서 **강한 일관성**이 공짜로 나온다. 중간 노드는 읽기를 받지 않는다.

> 쓰기 지연 = 사슬 길이만큼의 홉. 3노드면 head→중간→tail 2홉 + 응답. VR은 Primary→Backup 1홉 + PREPAREOK 1홉이지만 **가장 느린 노드 1대는 무시**할 수 있다. Chain은 **가장 느린 노드에 그대로 끌려간다**.


## failover — 어느 노드가 죽느냐에 따라 다르다

master가 장애를 감지하고 사슬을 다시 잇는다. 노드 위치에 따라 처리가 다르다.

| 죽은 노드 | 처리 | 유실 위험 |
| --- | --- | --- |
| head | 다음 노드가 새 head. head가 받았지만 아직 못 넘긴 쓰기는 클라이언트가 응답을 못 받았으므로 재시도하면 된다 | 없음 (미확정 쓰기만 사라짐) |
| tail | 바로 앞 노드가 새 tail. 앞 노드는 tail보다 **같거나 더 많은** 쓰기를 갖고 있다 (사슬 순서상) | 없음 |
| 중간 | 앞 노드가 뒤 노드에 직접 연결하고, **Sent 목록**에 있는 쓰기를 뒤 노드에 다시 보낸다 | 없음 (Sent가 보장) |

```
[head] ──► [중간 X] ──► [tail]        중간 X 사망
[head] ───────────────► [tail]        head가 Sent 목록을 tail에 재전송
```

**핵심 불변식**: 사슬 앞쪽 노드는 뒤쪽 노드가 가진 모든 쓰기를 갖고 있다 (앞 ⊇ 뒤). 이 덕분에 어느 노드가 죽어도 "최신 로그 고르기"가 필요 없다. 위치가 곧 최신성이다.

새 노드 추가는 **tail 뒤에** 붙인다. 기존 tail이 전체 상태를 새 노드에 복사한 뒤 새 노드가 tail이 된다.

## fencing — master가 한다

Chain 자체에는 epoch가 없다. 사슬 구성을 바꾸는 주체가 **master 하나**이고, master가 각 노드에게 "네 앞·뒤는 누구"를 알려준다. 옛 head가 돌아와도 master가 사슬에 안 넣으면 아무 쓰기도 못 받는다.

> 즉 "누가 Primary(head)인가"와 "옛 Primary 차단"을 **프로토콜 밖의 master에 전부 위임**한다. Redis Sentinel과 같은 구조다. 우리 실험에서는 master 역할을 설정 변경 또는 간단한 코디네이터로 흉내 내야 하고, 그 master가 단일 장애점이라는 점을 보고서에 적어야 한다.


## CRAQ — 읽기를 모든 노드로

Chain의 약점은 읽기가 tail 한 대에 몰린다는 것이다. CRAQ는 **모든 노드가 읽기를 받되** 일관성을 유지하는 방법이다.

각 노드가 객체마다 **여러 버전**을 들고 있고, 버전마다 clean/dirty를 표시한다.

```
쓰기 v5가 흐르는 중:
  head:  v4 clean, v5 dirty
  중간:  v4 clean, v5 dirty
  tail:  v4 clean          ← 아직 v5 안 옴

읽기가 중간에 오면:
  최신이 clean(v4)  → 바로 응답
  최신이 dirty(v5)  → tail에 "v5 확정됐어?" 물어서(버전 번호만, 값은 안 보냄) 답한다
```

tail이 v5를 적용하면 ack가 올라가면서 각 노드가 v5를 clean으로 바꾸고 v4를 지운다.

효과: 읽기 처리량이 노드 수에 비례해 늘어난다. 대가: 쓰기 중인 객체를 읽으면 tail 왕복이 추가되고, 노드마다 버전을 여러 개 들고 있어야 한다.

## 효과

1. **확정된 쓰기 유실이 구조적으로 없다.** 앞 ⊇ 뒤 불변식 덕분에 failover 때 로그 비교·선택이 필요 없다.
2. **f+1대로 f 장애를 견딘다.** VR·Raft의 2f+1보다 노드가 적다. 3대로 2대 장애까지.
3. **읽기가 쓰기를 방해하지 않는다.** 읽기는 tail(또는 CRAQ로 전부), 쓰기는 head. 역할이 갈린다.
4. **각 노드의 전송 부하가 작다.** Primary가 N-1대에 뿌리는 대신 1대에만 보낸다. 네트워크 amplification이 낮다.

## 한계

1. **쓰기 지연이 사슬 길이에 비례한다.** 3노드면 VR보다 홉이 하나 더 많고, 가장 느린 노드에 끌려간다 (명세서 카드의 "Backup 한 대가 느릴 때" 항목에서 가장 불리).
2. **감지·구성 변경이 전부 외부 master 몫이다.** Chain 논문은 master를 "있다고 치고" 넘어간다. 실제로는 ZooKeeper 같은 또 다른 합의 시스템이 필요하다.
3. **failover 중 쓰기가 멈춘다.** master가 사슬을 다시 이을 때까지 head는 쓰기를 받아도 흘릴 곳이 없다.
4. **새 노드 합류가 비싸다.** tail의 전체 상태를 복사해야 한다. (fast recovery 과제와 연결)
5. **캐시의 TTL·eviction과 맞추기 어렵다.** 노드마다 독립적으로 만료·축출하면 앞 ⊇ 뒤 불변식이 깨진다. 캐시 시스템이 Chain을 잘 안 쓰는 이유 중 하나로 보고서에 적을 수 있다.

## 실험을 위해 직접 정해야 할 것

```
- 사슬 길이:        3 (head–중간–tail). VR·Raft 3노드와 동일 조건
- master 역할:     별도 프로세스 없이 테스트 코디네이터가 사슬 구성을 바꿔 줌. 감지는 heartbeat → 코디네이터 통보
- 감지 timeout:     VR·Raft와 같은 값 (200 / 500 / 1000ms)
- 읽기:            Chain 모드(tail만) / CRAQ 모드(전 노드) 두 가지. stale read 0건 확인
- Sent 목록:       메모리. 중간 노드 장애 시 재전송 테스트 필수
- 비교 지점:        쓰기 p99 (느린 노드 1대 주입 시), 읽기 throughput (CRAQ vs VR Primary 읽기), failover 시간 (head / tail / 중간 각각)
- 범위 밖:         노드 추가(상태 복사), TTL 연동
```

## 우리 코드와의 대응

| Chain / CRAQ | 우리 저장소 |
| --- | --- |
| 사슬 구성 버전 | `OwnershipEpoch` (master가 구성 바꿀 때 증가) |
| head가 아닌 노드가 쓰기 받음 | `NOT_PRIMARY` + `Redirect(head)` |
| tail이 아닌 노드가 읽기 받음 (Chain 모드) | `NOT_PRIMARY` + `Redirect(tail)` — 읽기와 쓰기의 redirect 대상이 다름 (현재 `Redirect`는 노드 하나만 가리키므로 확인 필요) |
| 쓰기 전달 / ack | `PeerMessage` record 2개 (Forward, Ack) |
| clean/dirty 버전 (CRAQ) | `KvStore.Entry.version` 확장 필요 |

## 참고

- van Renesse, Schneider. *Chain Replication* (OSDI 2004): https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf — §2 프로토콜, §3 failover
- Terrace, Freedman. *CRAQ* (USENIX ATC 2009): https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf
- MIT 6.824 Lecture 9 "CRAQ" 강의 노트: https://pdos.csail.mit.edu/6.824/notes/l-craq.txt
- 실제 채택 사례: Microsoft Azure Storage (stream layer), HDFS pipeline write (유사 구조), CORFU
