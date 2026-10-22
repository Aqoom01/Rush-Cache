# 방법론 후보 조사와 선정

> Active-Passive(Primary 1대 + Backup) 구조에서 "복제 확정 조건"과 "Primary 교체 방식"을 다루는 논문·산업계 방법론을 조사하고, 같은 3노드 조건에서 구현·비교할 4개를 골랐다.

## 선정 기준

| 기준 | 설명 |
| --- | --- |
| Active-Passive인가 | Primary 1대만 쓰기를 받는 구조. 다중 쓰기(Active-Active)는 A2 범위라 제외 |
| 7주 안에 구현 가능한가 | 핵심 메커니즘이 메시지 몇 종류로 설명되고, 3노드로 재현 가능한 것 |
| 비교 축이 다른가 | 이미 고른 것과 "확정 조건" 또는 "교체 방식"이 달라서 비교 표에 새 열을 만드는 것 |
| 근거가 분명한가 | 논문 또는 공식 스펙·소스가 있는 것 |

## 조사한 후보

| # | 방법론 | 종류 | 한 줄 설명 | 선정 | 이유 |
| --- | --- | --- | --- | --- | --- |
| 1 | **Primary-Backup** (Budhiraja, Marzullo, Schneider, Toueg, 1993) | 논문 | Primary가 받아 Backup에 전달하는 가장 단순한 복제. 감지·교체는 범위 밖 | ✅ baseline | 명세서 지정. 모든 비교의 기준선 |
| 2 | **Redis Replication + Sentinel** | 산업계 | 비동기 Primary-Backup 복제 + 외부 Sentinel이 quorum으로 감지·승격 | ✅ | 명세서 지정. 1번을 실제 제품이 어떻게 채웠는지 보여주는 비교 대상 |
| 3 | **Viewstamped Replication Revisited** (Liskov & Cowling, 2012) | 논문 | view 번호 + 과반수 commit + view change. 교체 규칙이 프로토콜 안에 포함 | ✅ | 명세서 지정. 완성형 기준 |
| 4 | **Raft** (Ongaro & Ousterhout, 2014) | 논문 | VR과 같은 구조이나 리더를 랜덤 timeout + 투표로 선출. etcd·TiKV가 사용 | ✅ | VR과 "교체 방식"만 달라서 선출 방식의 영향을 분리 측정 가능. 레퍼런스 구현이 많아 검증 용이 |
| 5 | **Chain Replication** (van Renesse & Schneider, 2004) / **CRAQ** (Terrace & Freedman, 2009) | 논문 | 노드를 사슬로 세워 head→tail로 쓰기 전달, tail이 확정·응답. CRAQ는 읽기를 전 노드로 확장 | ✅ | 확정 조건(과반수→전원)과 구조(방사형→사슬)가 모두 달라 비교 폭이 가장 넓음. 캐시 시스템에 채택되지 않은 아이디어 |
| 6 | **Paxos Made Simple** (Lamport, 2001) | 논문 | 합의 알고리즘의 원형. 제안자·수락자·학습자 역할과 2단계 투표 | ❌ | VR·Raft가 Paxos를 리더 기반으로 정리한 것이라 중복. 단독 구현 난이도 높음 |
| 7 | **Paxos Made Live** (Chandra, Griesemer, Redstone, 2007) | 논문 | Google이 Chubby에 Paxos를 실제 구현하며 겪은 문제(디스크 손상, 마스터 lease, 스냅샷) | ❌ 참고 | 구현 교훈은 보고서 "한계·향후" 절에 인용. 알고리즘 자체는 6번과 같음 |
| 8 | **Zab** (Junqueira, Reed, Serafini, 2011) | 논문 | ZooKeeper의 복제 프로토콜. Primary-Backup 원자 브로드캐스트, epoch + 과반수 | ❌ | VR과 거의 동일한 구조(epoch·순서 보장·리더 복구). VR 하나로 대표 가능 |
| 9 | **Flexible Paxos** (Howard, Malkhi, Spiegelman, 2016) | 논문 | "복제 quorum과 선출 quorum이 교차하기만 하면 된다". 과반수가 필수가 아님을 증명 | ❌ 참고 | 3노드에서는 효과가 작음. 노드 수를 늘리는 실측 단계에서 "복제 1대 + 선출 전원" 변형으로 VR에 얹을 수 있어 향후 과제로 기록 |
| 10 | **Leases** (Gray & Cheriton, 1989) / **Chubby** (Burrows, 2006) | 논문 | Primary 권한을 "임대 기간"으로 부여. 기간이 끝나면 자동 소멸해 옛 Primary가 스스로 멈춤 | ❌ 참고 | epoch 메시지 없이 fencing하는 다른 접근. 시간 남으면 VR의 fencing 변형으로 추가 (시계 오차 가정이 필요해 1순위 아님) |
| 11 | **Harp** (Liskov et al., 1991) | 논문 | VR을 파일 시스템에 적용. **witness** 노드(데이터 없이 투표만)로 2대 + 증인 구성 | ❌ 참고 | "Backup 2대 대신 Backup 1대 + witness"가 비용 절감 변형. 노드 수가 제한될 때 대안으로 기록 |
| 12 | **VMware vSphere FT** (Scales, Nelson, Venkitachalam, 2010) | 논문 | VM 전체를 명령 수준으로 복제하는 Primary-Backup. 공유 디스크의 atomic test-and-set으로 split brain 방지 | ❌ 참고 | 캐시 수준이 아닌 VM 수준이라 범위 밖. split brain을 "공유 저장소 lock"으로 푸는 발상은 비교 설명에 인용 |
| 13 | **Kafka ISR 복제** | 산업계 | 리더 + In-Sync Replica 집합. `acks=all`이면 ISR 전원 확인, `min.insync.replicas`로 하한 설정 | ❌ 참고 | 동기(전원)와 과반수 사이의 중간 형태. "ISR 집합을 동적으로 줄인다"는 발상은 느린 Backup 처리 비교에 인용 |
| 14 | **MySQL Semi-synchronous Replication** | 산업계 | Primary가 replica 1대의 수신 확인만 기다리고 commit. `rpl_semi_sync_master_timeout` 지나면 비동기로 자동 전환 | ❌ 참고 | "동기 → 비동기 자동 강등"이라는 운영 타협. Backup 1대 지연 시나리오의 비교 설명에 인용 |
| 15 | **EPaxos** (Moraru, Andersen, Kaminsky, 2013) | 논문 | 리더 없이 모든 노드가 제안. 충돌 없는 명령은 1왕복에 commit | ❌ | 리더가 없어 Active-Passive가 아님. A2(Active-Active) 범위 |

## 선정 결과

```
구현 (5)
 ├─ Primary-Backup (동기 / 비동기)     — baseline. Redis 복제 방식을 따름
 ├─ Viewstamped Replication           — 과반수 + view change
 ├─ Raft                              — 과반수 + 투표 선출
 ├─ Chain Replication (+ CRAQ)        — 전원 통과 + 외부 master 교체
 └─ Rosé bounded-lag (2026)           — 비동기 + 복제 지연 상한 (최근 논문 절 참고)

비교·인용 (6)
 ├─ Redis Sentinel      — 감지·승격 실제 사례 (Primary-Backup 변형의 감지 규칙으로 차용)
 ├─ Flexible Paxos      — quorum 완화, 향후 과제
 ├─ Leases / Chubby     — lease 기반 fencing, 선택 변형
 ├─ Harp                — witness 구성
 ├─ Kafka ISR / MySQL semi-sync — 동기·비동기 중간 형태
 └─ Paxos Made Live / VMware FT — 구현 교훈, split brain 대응 사례

제외 (3)
 └─ Paxos Made Simple, Zab, EPaxos — 선정된 것과 중복이거나 Active-Active
```

## 비교 축

4개 구현을 같은 표에 놓기 위한 축이다.

| 축 | Primary-Backup | Rosé bounded-lag | VR | Raft | Chain |
| --- | --- | --- | --- | --- | --- |
| 쓰기 확정 조건 | 전원 ack (동기) / 즉시 (비동기) | 즉시. 단 Backup 지연이 L 초과면 대기 | 과반수 | 과반수 | 전원 통과 (tail 도달) |
| 느린 Backup 1대 영향 | 동기면 전부 지연 | 지연이 L 안이면 없음, 넘으면 대기 | 없음 | 없음 | 전부 지연 |
| 유실 상한 | 비동기: 없음 | **L개 이하** | 0 | 0 | 0 |
| Primary 교체 주체 | 외부 (heartbeat + NodeId 순) | 외부 (Primary-Backup과 동일) | 프로토콜 내 (view mod N) | 프로토콜 내 (투표) | 외부 master |
| 교체 시 로그 선택 | 없음 (유실 가능) | 불필요 (Backup이 항상 일관 prefix) | 최신 로그 채택 | 투표로 거름 | 불필요 (앞 ⊇ 뒤) |
| fencing | epoch 수동 부착 | epoch 수동 부착 | view 번호 | term | master 구성 |
| f 장애 견디는 노드 수 | f+1 | f+1 | 2f+1 | 2f+1 | f+1 |
| 읽기 처리 | Primary | Primary (Backup은 snapshot 읽기 가능) | Primary | Leader | tail (CRAQ: 전원) |

## 참고 링크

| 방법론 | 링크 |
| --- | --- |
| Primary-Backup (1993) | Distributed Systems 2nd ed., ch. 8. 검색어: "The Primary-Backup Approach" Budhiraja |
| Redis Replication / Sentinel | https://redis.io/docs/latest/operate/oss_and_stack/management/replication/ , https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/ |
| VR Revisited (2012) | https://pmg.csail.mit.edu/papers/vr-revisited.pdf |
| Raft (2014) | https://raft.github.io/raft.pdf |
| Chain Replication (2004) | https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf |
| CRAQ (2009) | https://www.usenix.org/legacy/event/usenix09/tech/full_papers/terrace/terrace.pdf |
| Paxos Made Simple (2001) | https://lamport.azurewebsites.net/pubs/paxos-simple.pdf |
| Paxos Made Live (2007) | https://research.google/pubs/paxos-made-live-an-engineering-perspective/ |
| Zab (2011) | https://www.cs.cornell.edu/courses/cs6452/2012sp/papers/zab-ieee.pdf |
| Flexible Paxos (2016) | https://arxiv.org/abs/1608.06696 |
| Leases (1989) | https://web.stanford.edu/class/cs240/readings/89-leases.pdf |
| Chubby (2006) | https://research.google/pubs/the-chubby-lock-service-for-loosely-coupled-distributed-systems/ |
| Harp (1991) | https://pmg.csail.mit.edu/papers/harp.pdf |
| VMware FT (2010) | https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf |
| Kafka Replication | https://kafka.apache.org/documentation/#replication |
| MySQL Semi-sync | https://dev.mysql.com/doc/refman/8.0/en/replication-semisync.html |
| EPaxos (2013) | https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf |

## 최근 논문 (2020~2026)

고전 논문이 "규칙"을 정했다면, 최근 논문은 그 규칙을 **더 빠르게(µs), 더 싸게(비동기에 가깝게), 더 안전하게(검증)** 만드는 쪽이다. A1과 직접 닿는 것만 골랐다.

| # | 논문 | 학회·연도 | 한 줄 설명 | 선정 | A1과의 연결 |
| --- | --- | --- | --- | --- | --- |
| 16 | **Mu: A Highly Available Replication Protocol** (Aguilera, Ben-David, Guerraoui, Marathe, Xygkis, Zablotchi) | OSDI 2020 | RDMA 단방향 쓰기로 복제 1.3µs, **권한(permission) 전환으로 failover 1ms 미만**. Primary가 Backup 메모리에 직접 쓰고, 새 Primary는 옛 Primary의 쓰기 권한을 회수해 fencing | ❌ 참고 | "fencing = 권한 회수"라는 발상. RDMA 하드웨어가 필요해 구현은 불가, 비교 설명에 인용 |
| 17 | **Rowan: Replicating Persistent Memory KV Stores with One-Sided RDMA** (Wang, Lu, Shu et al., Tsinghua) | OSDI 2023 | PM 기반 KV 저장소의 Primary-Backup 복제를 단방향 RDMA로 수행. Backup CPU 개입 없이 복제 | ❌ 참고 | Backup 쪽 CPU 비용이 0인 복제. SSD 기반 OpenKache에 옮길 때 "Backup의 쓰기 비용"을 재는 관점 제공 |
| 18 | **Aceso: Efficient Fault Tolerance in Memory-Disaggregated KV Stores** (Tsinghua) | SOSP 2024 | 메모리 분리 구조 KV에서 복제·복구 비용을 줄이는 방식 | ❌ 참고 | 구조가 달라 범위 밖. "복구 비용" 측정 지표 참고 |
| 19 | **FiDe: Reliable and Fast Crash Failure Detection** (Rovelli et al.) | USENIX ATC 2025 | timeout 기반 감지는 네트워크·CPU 경합 때문에 느리고 불안정하다는 문제 제기. 전용 저지터 처리 + 예약 네트워크로 **30µs 안에 crash 감지**, Redis·ZooKeeper에 적용해 처리량 2.23배 | ✅ 참고(핵심) | A1의 "감지 시간 vs 오판" 트레이드오프를 정면으로 다룬다. 우리 heartbeat timeout 실험의 비교 기준·동기 부여로 인용. A3와 공유 |
| 20 | **LeaseGuard: Raft Leases Done Right** (Davis, Demirbas, Deng) | arXiv 2025.12 | Raft에서 읽기를 리더가 quorum 없이 처리하기 위한 lease 알고리즘을 TLA+로 명세. **새 리더가 lease 획득 전에도 99% 읽기를 즉시 처리**하는 failover 최적화 포함 | ✅ 선택 변형 | Raft 변형의 "읽기 처리" 축을 채운다. Raft 구현 후 시간이 남으면 리더 lease 읽기를 얹는다 |
| 21 | **Rosé: Flexible Replication with Strong Semantics for Partitioned Databases** (Zarkadas et al., Columbia / MS Research) | CIDR 2026 | 비동기 Primary-Backup에서 **복제 지연에 상한(bounded lag)** 을 두고, Backup이 항상 일관된 스냅샷을 노출. failover 후 처리량·p99 악화 없음 | ✅ 구현 | 동기와 비동기 사이의 **세 번째 확정 조건**("Backup 지연이 L 이하일 때만 쓰기 수락"). 하드웨어 의존이 없어 재현 가능하고, 선정 방법론 중 유일한 최근 논문. 다섯 번째 구현 대상 |
| 22 | **FlexiRaft: Flexible Quorums with Raft** (Meta) | CIDR 2023 | Meta의 MySQL Raft에서 데이터센터 구성에 맞춰 quorum을 유연하게 조정 | ❌ 참고 | Flexible Paxos(9번)의 실제 적용 사례. 노드 수 늘리는 실측 단계에서 인용 |

### 최근 논문 반영 후 선정 결과

```
구현 (5)
 ├─ Primary-Backup (동기 / 비동기)
 ├─ Viewstamped Replication
 ├─ Raft (+ LeaseGuard 읽기 lease, 선택)
 ├─ Chain Replication (+ CRAQ)
 └─ Rosé bounded-lag (CIDR 2026)   ← 최근 논문. 비동기에 지연 상한 L을 둬 유실량을 L개 이하로 제한

핵심 인용 (1)
 └─ FiDe (ATC 2025) — 감지 timeout 실험의 문제 정의와 비교 기준

참고 (4)
 └─ Mu, Rowan, Aceso, FlexiRaft — 하드웨어·구조가 달라 구현 불가, 보고서 관련 연구 절에 인용
```

### 최근 논문 링크

| 논문 | 링크 |
| --- | --- |
| Mu (OSDI 2020) | https://arxiv.org/abs/2010.06288 |
| Rowan (OSDI 2023) | https://www.usenix.org/system/files/osdi23-wang-qing.pdf |
| Aceso (SOSP 2024) | https://dl.acm.org/doi/10.1145/3694715.3695951 |
| FiDe (ATC 2025) | https://www.usenix.org/system/files/atc25-rovelli.pdf |
| LeaseGuard (arXiv 2025) | https://arxiv.org/abs/2512.15659 |
| Rosé (CIDR 2026) | https://vldb.org/cidrdb/papers/2026/p8-zarkadas.pdf |
| FlexiRaft (CIDR 2023) | 검색어: "FlexiRaft: Flexible Quorums with Raft" CIDR 2023 |
