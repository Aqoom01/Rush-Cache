# Live Migration of Virtual Machines

## 출처

### 관련 논문

[Live Migration of Virtual Machines](https://www.usenix.org/legacy/events/nsdi05/tech/full_papers/clark/clark.pdf)

- USENIX NSDI 2005, Christopher Clark, Keir Fraser, Steven Hand, Jacob Gorm Hansen, Eric Jul, Christian Limpach, Ian Pratt, Andrew Warfield (Xen)
- 핵심 절: **§3.1 Migrating Memory**, **§3.3 Design Overview**
- 관련 절: **§4 Writable Working Sets**, **§5.3 Dynamic Rate-Limiting**, **§5.4 Rapid Page Dirtying**
- 평가: **§6.3 SPECweb99**, **§6.4 Quake 3**, **§6.5 A Diabolical Workload: MMuncher**
- 읽는 순서: **motivation → pre-copy 알고리즘 → 수렴 조건 → stop 단계 → 평가**

## 핵심 아이디어

실행 중인 VM을 다른 호스트로 옮기면서 메모리가 계속 바뀌는 문제(dirty page)를 다룬다. 서비스를 계속하며 메모리를 **미리(pre) 복사**하고, 그동안 바뀐 page만 여러 라운드 다시 보낸 뒤, **아주 짧게 멈추고** 남은 page와 CPU 상태를 옮긴다.

논문은 메모리 이동을 세 단계로 나눈다. 서비스 중 보내는 **push**, 멈추고 보내는 **stop-and-copy**, 새 위치에서 실행하다 없는 page를 가져오는 **pull**이다.

```text
Stage 0 Pre-Migration → Stage 1 Reservation
    ↓
Stage 2 Iterative Pre-Copy   서비스 중 전체 복사, 이후 dirty page만 재전송
    ↓
Stage 3 Stop-and-Copy        짧게 멈추고 남은 dirty page·CPU 상태 전송
    ↓
Stage 4 Commitment → Stage 5 Activation   이동 확정 후 새 위치에서 재개
```

KV 이동으로 옮기면 snapshot = 첫 메모리 복사, write delta = dirty page, cutover = stop-and-copy, destination miss 시 source에서 가져오기 = post-copy의 page fault다.

## 효과와 한계

### 기대 효과

정지 시간이 "전체 데이터 전송 시간"에서 "마지막에 남은 변경분 전송 시간"으로 줄어든다. 논문 실측은 SPECweb99 정지 210 ms(§6.3), Quake 3 서버 정지 60 ms(§6.4)다.

### 한계

- **쓰기가 많으면 수렴하지 않는다:** 바뀌는 속도가 전송 속도보다 빠르면 dirty가 줄지 않는다. 메모리를 매우 빠르게 쓰는 MMuncher는 정지가 3.5초까지 늘었다(§6.5).
- **전송량이 stop-and-copy보다 많다:** 자주 바뀌는 page를 라운드마다 다시 보낸다.
- **정합성 단위가 다르다:** VM은 page를 덮어쓰면 끝이지만 KV는 삭제·요청 라우팅·소유권 의미가 따로 있다. 논문은 이것을 다루지 않는다.

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. stop-and-copy, pre-copy, demand-migration은 정지 시간과 총 이동 시간을 어떻게 나눠 갖는가?

**답**

| 방식 | 단계 | 정지 시간 | 총 이동 시간 |
|---|---|---|---|
| pure stop-and-copy | stop-and-copy만 | 길다 (메모리 크기에 비례) | 짧다 (각 page 한 번) |
| pure demand-migration | 짧은 stop-and-copy + pull | 짧다 | 길다, 그동안 성능 저하 |
| pre-copy | 반복 push + 짧은 stop-and-copy | 짧다 | 중간 (재전송만큼 늘어남) |

정지 시간은 실행 중인 인스턴스가 없어 서비스를 못 하는 시간이고, 총 이동 시간은 이동을 시작해서 원래 VM을 버려도 되는 시점까지다.

**근거**

[Clark et al.](https://www.usenix.org/legacy/events/nsdi05/tech/full_papers/clark/clark.pdf) **§3.1 Migrating Memory**. 세 단계의 정의, 세 방식의 비교, 두 시간의 정의가 모두 이 절에 있다.

### 2. 반복을 언제 멈추고 stop 단계로 넘어가는가?

**답**

두 조건 중 하나가 되면 멈춘다.

- **남은 양이 충분히 작다:** 보낼 것이 256 KB 미만 남았을 때.
- **더 돌아도 이득이 없다:** 라운드마다 전송률을 "dirty가 생기는 속도 + 50 Mbit/s"로 올리는데, 이렇게 계산한 전송률이 관리자가 정한 최대치를 넘으면 멈춘다.

마지막 라운드는 허용된 최대 전송률로 보내 정지 시간을 줄인다. 계속 바뀌는 page 묶음(**writable working set**)은 미리 보내 봐야 다시 바뀌므로 마지막 stop 단계에서 보내는 편이 낫다.

**근거**

[Clark et al.](https://www.usenix.org/legacy/events/nsdi05/tech/full_papers/clark/clark.pdf) **§5.3 Dynamic Rate-Limiting**(종료 조건, 50 Mbit/s 증분, 최소·최대 전송률), **§4 Writable Working Sets**. 라운드 상한 횟수는 **자료에 없음**(본문에 고정 상한이 없다).

### 3. VM의 dirty page를 KV의 변경분(delta)으로 옮기면 무엇이 더 필요한가?

**답**

| 필요한 것 | 이유 |
|---|---|
| 삭제 표시(tombstone) | 삭제된 key는 "보낼 값"이 없어 그냥 빠진다. snapshot에 들어간 옛 값이 destination에 남는다 |
| 적용 순서 규칙 | 재전송·중복이 오면 옛 값이 새 값을 덮을 수 있다 |
| 요청 라우팅과 소유권 전환 | 이동 중 요청을 누가 받고 언제부터 새 위치로 보낼지 정해야 한다 |
| 캐시 고유 변경 | TTL 만료와 eviction도 destination에 반영해야 한다 |

**근거**

**자료에 없음**. 논문은 메모리 page만 다룬다. 위 표는 프로젝트의 설계 과제다(B3 design.md §4.2). 라우팅과 소유권은 [MongoDB Chunk Migration](mongodb-chunk-migration.md) 정리를 함께 본다.

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| 다시 바뀐 page 건너뛰기 | 보내기 직전 dirty bitmap을 다시 보고, 이번 라운드에 또 바뀐 page는 보내지 않는다 (§5.4) | KV dirty 집합에 적용하면 전송량이 얼마나 주는가 |
| 쓰기 많은 프로세스 늦추기 | 쓰기가 많은 프로세스를 잠시 멈춰 수렴시킨다 (§5.5) | KV의 write throttling과 서비스 p99 영향 |
| commit | Stage 4 Commitment 뒤에 원래 VM을 버린다 (§3.3) | B3 commit(라우팅 표 갱신)과 source 정리 사이의 대기 |

## 프로젝트 적용

`pre-copy` variant의 기준 논문이고, 논문이 양 극단의 하나로 든 pure stop-and-copy를 `stop-and-copy`(Baseline)로 구현한다.

### 적용할 내용

- snapshot → dirty만 재전송하는 라운드 → 짧은 stop 단계 → commit의 순서
- "남은 양 256 KB 미만이면 멈춘다"는 종료 기준 (B3 design.md §4.2)
- 정지 시간과 총 이동 시간을 따로 재는 지표 정의 (B3 design.md §6.1)

### 후속 `pre-copy`의 초기 구현에서 적용하지 않는 내용

- 동적 전송률 제한(§5.3). 초기 비교는 전송률을 고정하고 실험 변수로 둔다.
- 다시 바뀐 page 건너뛰기(§5.4), 쓰기 많은 프로세스 늦추기(§5.5). 비교 결과를 보고 개선 후보로 검토한다.
- CPU·장치 상태 이동, 네트워크 주소 전환 (VM 전용).

### 직접 설계하고 기록할 내용

- 변경 추적 방식(dirty key 집합)과 추적 시작 시점
- tombstone과 적용 순서(`seq`) 규칙
- 라운드 상한과 "더 줄지 않으면 멈춘다"는 조건. 논문의 "계산된 전송률이 최대치를 넘으면"을 고정 전송률 실험에 맞게 옮긴 것이다
