# Viewstamped Replication (VR Revisited)

Liskov & Cowling, *Viewstamped Replication Revisited*, MIT-CSAIL-TR-2012-021 — https://pmg.csail.mit.edu/papers/vr-revisited.pdf

## Viewstamped Replication이란?

Primary 1대가 모든 요청의 **순서를 정하고**, Backup들에게 그 순서대로 복제한 뒤, **과반수가 받았다고 답하면** 실행·응답하는 복제 프로토콜이다.

Primary가 죽으면 Backup들이 **view 번호를 올려** 새 Primary를 뽑는다. 옛 view 번호가 붙은 메시지는 버린다.

> **Viewstamp?**
>
> view 번호 + 연산 번호(op-number)의 쌍이다. "몇 번째 Primary 시대의 몇 번째 연산"을 뜻하며, 모든 노드가 같은 순서로 연산을 쌓게 만드는 도장이다.

> **Paxos·Raft와의 관계?**
>
> 셋 다 "리더 + 번호 + 과반수" 구조로 같은 문제를 푼다. VR(1988)이 Paxos(1989)보다 먼저 나왔고, Raft(2014)는 VR과 거의 같은 구조를 이해하기 쉽게 다시 쓴 것이다. VR의 view = Raft의 term.

## 기본 개념

| 용어 | 의미 |
| --- | --- |
| Replica | 노드. 총 2f+1대이며 f대까지 죽어도 동작한다. 3대면 f=1 |
| Primary | 현재 view에서 요청 순서를 정하는 유일한 노드 |
| Backup | Primary가 정한 순서대로 로그를 받아 쌓는 노드 |
| view-number | 현재 Primary 시대의 번호. 교체마다 1 증가 (= 우리의 `OwnershipEpoch`) |
| op-number | 이 노드가 로그에 쌓은 마지막 연산 번호 |
| commit-number | 과반수가 받았다고 확인돼 실행해도 되는 마지막 연산 번호 |
| log | op-number 순으로 쌓인 연산 목록 |
| client-table | 클라이언트별 마지막 요청 번호와 그 응답. 재시도 중복 실행 방지 |
| status | `normal` / `view-change` / `recovering` 셋 중 하나 |
| 과반수 (quorum) | 2f+1 중 f+1대. 3대면 2대 |

Primary는 **view-number mod 노드 수**로 정해진다. 투표가 없다. view 3이면 3 mod 3 = 노드 0, view 4면 노드 1. 모두가 같은 식을 알고 있어 "누가 Primary냐"를 두고 싸울 일이 없다.

```
노드 3대 (f=1)

view 0: Primary 노드 0, Backup 노드 1·2
view 1: Primary 노드 1, Backup 노드 0·2
view 2: Primary 노드 2, Backup 노드 0·1
view 3: Primary 노드 0 ...
```

## 정상 동작 — 요청 하나가 처리되는 6단계

```
클라이언트 ──REQUEST──► Primary ──PREPARE──► Backup 1
                                 ──PREPARE──► Backup 2
                        Primary ◄─PREPAREOK── Backup 1      ← f=1개 오면 과반수
                        Primary: 실행, commit-number++
클라이언트 ◄──REPLY──── Primary
                        (다음 PREPARE에 commit-number를 실어 Backup도 실행)
```

| 단계 | 메시지 | 처리 |
| --- | --- | --- |
| 1 | `REQUEST(op, client-id, request-number)` | 클라이언트가 Primary에 보낸다 |
| 2 | — | Primary가 client-table을 본다. 요청 번호가 마지막보다 작으면 버리고, 같으면 저장된 응답을 재전송하고, 크면 계속 |
| 3 | `PREPARE(view, msg, op-number, commit-number)` | Primary가 op-number를 1 올려 로그에 쌓고, 모든 Backup에 보낸다 |
| 4 | `PREPAREOK(view, op-number, replica-id)` | Backup은 **op-number 순서대로만** 로그에 쌓고(앞 번호가 비면 기다림) Primary에 답한다 |
| 5 | — | Primary가 PREPAREOK를 f개 받으면(자기 포함 f+1 = 과반수) 연산을 실행하고 commit-number를 올린다 |
| 6 | `REPLY(view, request-number, result)` | 클라이언트에 응답하고 client-table에 기록한다 |

Backup은 PREPARE 안의 commit-number를 보고 "여기까지는 확정됐구나" 하고 자기도 실행한다. 새 요청이 한동안 없으면 Primary가 `COMMIT(view, commit-number)`만 따로 보낸다. 이 COMMIT/PREPARE가 **heartbeat 역할**도 한다.

> **왜 과반수면 되나?**
>
> 3대 중 2대가 로그를 갖고 있으면, 나중에 어떤 2대가 살아남아도 그중 최소 1대는 그 로그를 갖고 있다. 그래서 view change 때 "과반수한테 로그를 모아 가장 긴 것을 고르면" 확정된 연산은 절대 안 사라진다.

## Primary 교체 — view change 4단계

Backup이 timeout 동안 PREPARE도 COMMIT도 못 받으면 Primary가 죽었다고 보고 시작한다.

```
Backup 1: timeout ──STARTVIEWCHANGE(v+1)──► 모두
Backup 2: 받음    ──STARTVIEWCHANGE(v+1)──► 모두
각자 f개 받으면 ──DOVIEWCHANGE(v+1, log, ...)──► 새 Primary (= (v+1) mod N)
새 Primary: f+1개 모이면 가장 최신 log 선택 ──STARTVIEW(v+1, log)──► 모두
```

| 단계 | 메시지 | 처리 |
| --- | --- | --- |
| 1 | `STARTVIEWCHANGE(new-view, replica-id)` | view-number를 올리고 status를 `view-change`로 바꾼 뒤 모두에게 보낸다. 다른 노드의 STARTVIEWCHANGE를 받아도 같이 올린다 |
| 2 | `DOVIEWCHANGE(new-view, log, last-normal-view, op-number, commit-number, replica-id)` | 같은 view의 STARTVIEWCHANGE를 f개 받으면, **새 Primary 한 대에게만** 자기 로그 전체를 보낸다 |
| 3 | — | 새 Primary는 자기 것 포함 f+1개의 DOVIEWCHANGE가 모이면 그중 **last-normal-view가 가장 큰 로그**, 같으면 **op-number가 가장 큰 로그**를 채택한다. commit-number는 받은 것 중 최댓값 |
| 4 | `STARTVIEW(new-view, log, op-number, commit-number)` | 새 Primary가 status를 `normal`로 바꾸고 모두에게 채택한 로그를 보낸다. 받은 노드는 로그를 교체하고, 아직 commit 안 된 연산에 대해 PREPAREOK를 보낸다 |

3단계의 "가장 최신 로그 고르기"가 **데이터 유실을 막는 핵심**이다. Primary-Backup 단순형은 이 규칙이 없어서 덜 받은 노드가 올라갈 수 있다.

## 옛 Primary는 어떻게 차단되나 (fencing)

모든 메시지에 view 번호가 붙어 있다. 규칙은 두 줄이다.

```
받은 view < 내 view  →  무시한다
받은 view > 내 view  →  내가 뒤처진 것. 상대에게 상태를 받아 따라잡는다
```

GC로 멈췄다 깨어난 옛 Primary가 view 3으로 PREPARE를 보내면, 이미 view 4인 Backup들이 전부 무시한다. 옛 Primary는 PREPAREOK를 하나도 못 받으니 아무것도 commit할 수 없다. 그러다 view 4 메시지를 받으면 자기가 뒤처졌음을 알고 Backup으로 내려간다.

> view 번호를 올리는 것(STARTVIEWCHANGE)과 낮은 view를 무시하는 것(fencing)은 별개의 규칙이다. 우리 코드의 `OwnershipEpoch`는 전자, `STALE_EPOCH` 응답은 후자에 해당한다.


## 클라이언트 재시도와 중복 방지

클라이언트는 요청마다 번호를 1씩 올려 붙인다. 응답이 timeout 안에 안 오면 **모든 노드에 재전송**한다. Backup은 무시하고 Primary만 처리한다.

Primary는 client-table로 같은 번호를 두 번 실행하지 않는다. 옛 Primary가 실행은 했는데 응답만 못 보냈어도, client-table이 로그와 함께 새 Primary에 넘어가므로 새 Primary가 저장된 응답을 그대로 돌려준다.

> 우리 코드의 `ClientId` + `OperationId`가 (client-id, request-number)에 해당한다.


## 효과

1. **확정된 쓰기는 절대 안 사라진다.** 과반수 + 최신 로그 선택 규칙 덕분에 Primary가 몇 번 바뀌어도 REPLY를 받은 연산은 보존된다.
2. **느린 Backup 1대에 안 끌려간다.** f+1만 채우면 되므로 3대 중 1대가 느려도 지연이 안 튄다.
3. **Primary 선출에 투표가 없다.** view mod N으로 정해지니 선출 자체는 즉시고, 시간이 드는 건 로그 모으기뿐이다.
4. **디스크를 안 쓴다.** 원래 VR은 로그를 디스크에 쓰지만, Revisited는 recovery 프로토콜로 대체해 메모리만으로 동작한다. 캐시에 잘 맞는다.

## 한계

1. **읽기도 Primary를 거친다.** 논문 기본 설계는 GET도 로그에 넣는다. Backup에서 읽으면 stale 값이 나올 수 있어서다. (논문 6.1절에 lease로 Primary가 바로 읽는 최적화가 있다.)
2. **view change 동안 서비스가 멈춘다.** STARTVIEWCHANGE → DOVIEWCHANGE → STARTVIEW 세 왕복이 끝나야 요청을 받는다. 로그가 길수록 DOVIEWCHANGE가 커진다.
3. **오판으로도 view change가 난다.** 네트워크가 잠깐 끊겨도 timeout이 지나면 멀쩡한 Primary를 갈아치운다. timeout이 짧을수록 심하다.
4. **재시작하면 못 돌아온다.** 메모리만 쓰므로 죽었다 켜진 노드는 recovery 프로토콜(4.3절)로 다른 노드에게 상태를 받아야 한다. 이번 범위 밖이라, 우리 구현에서는 죽은 노드가 복귀하지 않는다고 가정한다.
5. **Primary가 처리량 병목이다.** 모든 쓰기가 한 대를 지난다. (A2 Active-Active가 이걸 푸는 과제다.)

## 실험을 위해 직접 정해야 할 것

논문은 규칙만 주고 값은 안 준다. 실험으로 정한다.

```
- 노드 수:            3대 (f=1). 과반수 = 2
- Primary 선택:       view mod 3 (논문 그대로). NodeId 순 승격과 비교할 수 있음
- heartbeat 간격:     COMMIT 메시지 주기. 100ms 부터 시작
- view change timeout: PREPARE/COMMIT 없이 기다리는 시간. 200 / 500 / 1000ms 비교 → 감지 속도 vs 오판 곡선
- 클라이언트 timeout:  REPLY 없이 기다리는 시간. 재전송 횟수
- GET 처리:           논문대로 Primary 로그 경유 / Backup 로컬 읽기 허용 두 가지 측정 (stale read 개수 비교)
- 로그 길이:          checkpoint 없이 메모리에 전부 유지 (실험 길이 제한)
- 복귀:               죽은 노드는 안 돌아온다고 가정 (recovery 미구현)
```

## 우리 코드와의 대응

| VR | 우리 저장소 |
| --- | --- |
| view-number | `core.model.OwnershipEpoch` |
| 낮은 view 무시 | `ResultStatus.STALE_EPOCH` |
| Backup이 REQUEST 받음 | `ResultStatus.NOT_PRIMARY` + `Redirect` |
| (client-id, request-number) | `ClientId`, `OperationId` |
| PREPARE / PREPAREOK / STARTVIEWCHANGE … | `core.spi.PeerMessage`를 구현한 record들 (A1이 정의) |
| 메시지 전송 | `core.spi.Transport` (테스트는 메모리 큐 구현) |
| view-number 영속화 | `core.spi.EpochStore` (recovery 안 하므로 이번엔 선택) |

## 참고

- Liskov, Cowling. *Viewstamped Replication Revisited* (2012). 4.1 Normal Operation, 4.2 View Changes, 6.1 Optimizations: https://pmg.csail.mit.edu/papers/vr-revisited.pdf
- Oki, Liskov. *Viewstamped Replication: A New Primary Copy Method* (PODC 1988) — 원 논문
- Ongaro, Ousterhout. *In Search of an Understandable Consensus Algorithm* (Raft, 2014) — VR과 비교용. term = view
- MIT 6.824 Lecture 4 "Primary-Backup Replication" — 같은 문제를 VMware FT로 푸는 사례
