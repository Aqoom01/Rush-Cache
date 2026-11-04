# Rosé — 복제 지연에 상한을 둔 비동기 Primary-Backup

> **2026-10-29 구현 기준 정정:** 아래는 초기 논문 조사·적용 초안이다. 실제 구현은 단일 파티션·op 개수 상한의 `ROSE_ADAPTATION`이다. “L개 이하 유실”은 이 모델의 단일 Primary crash에서 검증한 명제이며 논문 전체의 지역/다중 파티션 장애 보장을 재현한 결과가 아니다. 실제 승격은 정확한 crash oracle과 최장 생존 prefix 선택을 사용한다. “PB와 감지·승격·fencing을 완전히 동일하게 유지”는 실현된 전제가 아니다. 다중 파티션 coordinated apply·snapshot·영속 WAL trim·제품의 failover 후 성능 유지 주장은 구현/검증하지 않았다. [실제 범위](../rose-comparison/README.md)와 [240조건 결과](../rose-comparison/results.md)를 기준으로 본다.

Zarkadas, Kostopoulou, Graham, Yang, Cidon, Eldeeb (Columbia), Bernstein (Microsoft Research), *Rosé: Flexible Replication With Strong Semantics For Partitioned Databases*, CIDR 2026 — https://vldb.org/cidrdb/papers/2026/p8-zarkadas.pdf

## Rosé란?

비동기 Primary-Backup 복제에 **"Backup이 뒤처질 수 있는 양의 상한(L)"** 을 두는 방식이다. Backup이 L 넘게 밀리면 Primary가 쓰기를 **잠시 멈춰** 따라잡게 한다(backpressure).

동시에 Backup은 "반쯤 받은 상태"를 절대 노출하지 않고, 항상 **Primary의 어느 시점과 정확히 같은 상태(일관된 prefix)** 만 보여준다. 그래서 failover 때 Backup을 정리할 게 거의 없다.

> **왜 최근 논문 중 이걸 골랐나?**
>
> 2020년 이후 failover 논문은 대부분 RDMA·전용 코어로 µs를 노려서(Mu, FiDe, Rowan) JVM + TCP로는 재현이 안 된다. Rosé는 하드웨어 가정이 없고, 기존 비동기 복제에 "지연 세기 + 상한 넘으면 대기" 두 가지만 더하면 돼서 우리 환경에 그대로 옮길 수 있다.

> **동기도 비동기도 아닌 제3의 확정 조건**
>
> | 방식 | OK 조건 | 유실 상한 | 평소 지연 |
> | --- | --- | --- | --- |
> | 비동기 | 즉시 | 없음 | 0 |
> | 동기 | Backup 전부 ack | 0 | 매번 왕복 |
> | **Rosé** | 즉시. 단 Backup 지연 ≤ L일 때만 | **L개** | 평소 0, 밀릴 때만 대기 |

## 기본 개념

| 용어 | 의미 |
| --- | --- |
| Partition | 논문의 복제 단위. DB가 여러 조각으로 나뉘어 각 조각이 독립적으로 복제된다. 우리는 조각 1개(= 노드 전체)로 단순화한다 |
| Epoch (Rosé) | 논문에서 "몇 ms마다 전역으로 올라가는 시각 번호". 쓰기들을 epoch 단위로 묶어 순서를 정한다. **우리의 `OwnershipEpoch`(Primary 세대)와는 다른 뜻**이다 — 혼동을 피하려고 아래에서는 "batch 번호"라 부른다 |
| replication lag | Primary가 보낸 batch 번호 − Backup이 적용한 batch 번호 |
| L (queue bound) | 허용하는 최대 lag. 논문은 기본값을 주지 않고 "네트워크 대역폭을 채우되 허용 유실량 안에서" 사용자가 정하라고 한다 |
| backpressure | lag이 L에 닿으면 Primary가 해당 partition의 쓰기를 멈추고 timeout θ까지 기다리는 것 |
| coordinated apply | Backup이 받은 로그를 바로 적용하지 않고, 모든 partition이 도달한 **최소 batch 번호까지만** 적용하는 것 |
| snapshot epoch | 그 최소 batch 번호. Backup이 외부에 노출하는 상태의 시점. 단조 증가한다 |
| monotonic prefix consistency | Backup 상태가 항상 Primary 이력의 "앞부분 그대로"이고, 읽을 때마다 그 앞부분이 길어지기만 한다는 보장 (Helt et al. 정의) |

```
Primary                      Backup
 batch 1..100 전송 ──────► 받아서 WAL에 저장 (적용은 아직)
 lag = 100 − 적용 번호
 lag < L  → 계속 쓰기 수락
 lag ≥ L  → 쓰기 멈춤, Backup 따라잡을 때까지 대기 (θ 초과 시 오류)
```

## 정상 동작

**1. Primary → Backup 전송 (push).** Primary가 쓰기를 batch 번호 순으로 Backup에 밀어 보낸다. Backup이 가져가는(pull) 방식이 아니라 Primary가 진행 상황을 직접 감시한다.

**2. 클라이언트 응답.** Primary는 자기 저장 즉시 OK를 보낸다. 비동기와 같다. **단, 보내기 전에 lag < L인지 확인**한다. L 이상이면 이 쓰기는 대기열에 들어가고 Backup이 따라잡으면 처리된다.

**3. Backup의 두 단계 저장.** Backup은 받은 쓰기를 먼저 **로그(WAL)에만** 쌓는다. 실제 KV 저장소에 적용하는 것은 "모든 partition이 도달한 최소 batch 번호"까지다. partition이 하나인 우리 설정에서는 "받은 것을 순서대로 적용하되, batch 경계에서만 적용"으로 단순화된다.

**4. 지연 측정.** Backup은 적용한 batch 번호를 주기적으로 Primary에 알린다(논문은 health check에 piggyback). Primary는 이걸로 lag을 계산한다.

> 핵심은 **"Backup의 KV 저장소에는 항상 완결된 batch만 있다"** 는 것이다. 받다 만 쓰기는 WAL에만 있고 저장소에는 없다. 그래서 Backup을 읽으면 언제나 Primary의 어떤 과거 시점과 정확히 같다.


## failover

논문은 failover 절차를 단계로 나열하지 않는다. 설계에서 따라 나오는 동작은 이렇다.

| 단계 | 처리 |
| --- | --- |
| 1. 감지 | 범위 밖. 외부 감시(cluster management)가 한다 — Primary-Backup baseline과 같이 heartbeat로 대체 |
| 2. 정리 | Backup의 WAL에서 **snapshot epoch 이후 부분을 잘라낸다**. 순서대로 쌓인 로그라 끝을 자르는 것뿐이므로 매우 빠르다 |
| 3. 승격 | Backup이 Primary가 된다. KV 저장소는 이미 일관된 상태라 재구성 작업이 없다 |
| 4. 유실 | 잘라낸 WAL + Primary가 미처 못 보낸 쓰기. **합쳐서 L개 이하**. 비동기(상한 없음)와 동기(0)의 중간 |

논문 실험: Yugabyte(비교 대상)는 failover 뒤 처리량 22% 하락, p99 15% 상승이 남았는데 Rosé는 둘 다 0%였다. Backup이 "정리할 것 없는 상태"를 항상 유지한 덕분이다. (단일 머신에서 다중 노드를 시뮬레이션한 실험이고, 저자들도 프로토타입 vs 제품 비교라 동등하지 않다고 명시했다.)

## fencing

Rosé 자체에는 fencing이 없다. 비동기 Primary-Backup 위에 얹는 방식이라 "옛 Primary 차단"은 바탕이 되는 Primary-Backup 구현의 몫이다. 우리 구현에서는 Primary-Backup baseline과 같은 `OwnershipEpoch` + `STALE_EPOCH` 방식을 그대로 쓴다.

> 비교 표에서 Rosé 행의 "Primary 교체 주체"와 "fencing"은 Primary-Backup과 동일하고, **"쓰기 확정 조건"과 "유실 상한"만 다르다.** 이게 이 변형을 넣는 이유다 — 다른 축을 고정하고 확정 조건 하나만 바꿔 비교할 수 있다.


## 효과

1. **유실량이 L개 이하로 묶인다.** 비동기의 "얼마나 잃을지 모름"이 "최대 L개"가 된다.
2. **평소엔 비동기만큼 빠르다.** Backup이 잘 따라오는 동안은 대기가 없다. 동기처럼 매번 왕복을 기다리지 않는다.
3. **failover 뒤 성능 절벽이 없다.** Backup 저장소가 항상 깨끗해서 승격 직후부터 정상 처리량이 나온다.
4. **느린 Backup만 느려진다.** 논문의 partition 단위 backpressure는 느린 partition만 멈춘다. 우리처럼 partition이 1개면 이 장점은 사라지지만, "느린 Backup 1대" 시나리오에서 동기(전부 대기)와의 차이는 남는다.

## 한계

1. **L을 정하는 기준이 없다.** 논문은 "대역폭을 채우되 허용 유실량 안에서"라고만 한다. 우리 실험에서 L을 바꿔가며 "p99 vs 유실량" 곡선을 그려야 한다.
2. **Backup이 완전히 멈추면 Primary도 멈춘다.** lag이 L에 닿은 채 Backup이 안 돌아오면 쓰기가 영영 대기한다. 논문은 θ 타임아웃과 운영자 개입으로 푼다. 가용성 면에서 비동기보다 나쁘다.
3. **여전히 유실이 있다.** L개는 0이 아니다. VR·Raft·Chain과 달리 "ack된 쓰기가 사라질 수 있다".
4. **Backup 읽기는 옛 값이다.** 일관되지만 최신은 아니다(snapshot epoch 시점). 캐시에서 stale HIT로 보일 수 있어 "최신성을 증명 못 하면 MISS" 규칙과 충돌한다.
5. **평가가 제한적이다.** 단일 머신 시뮬레이션, 프로토타입. 우리가 3노드에서 재현하는 것 자체가 검증이 된다.

## 실험을 위해 직접 정해야 할 것

```
- partition 수:       1 (노드 전체). 논문의 "partition별 독립 backpressure"는 단순화
- lag 단위:           batch 번호 대신 op-number 차이 (요청 개수). 논문의 epoch 서비스 없음
- L:                  0(=동기) / 10 / 100 / 1000 / ∞(=비동기) 비교 → p99 vs 유실량 곡선
- θ (대기 timeout):   클라이언트 deadline과 동일 (TIMEOUT 응답)
- Backup 적용 경계:    op-number N개 단위 batch. N = 1이면 즉시 적용 (단순화 기본값)
- lag 보고:           Backup → Primary ack에 "적용한 op-number" 포함 (REPLCONF ACK와 같은 역할)
- 감지·승격·fencing:    Primary-Backup baseline과 완전히 동일하게 고정 (확정 조건만 다르게)
- 측정:               L별 SET p50/p99, failover 후 유실 개수(≤ L 검증), failover 직후 1초간 처리량 (성능 절벽 유무)
```

## 우리 코드와의 대응

| Rosé | 우리 저장소 |
| --- | --- |
| lag = 보낸 op − 적용 op | Primary가 들고 있는 `lastSentOp − lastAckedOp` |
| backpressure (lag ≥ L) | `KvEngine.execute` 앞에서 검사 → 대기열 또는 `BUSY` |
| WAL과 KV 저장소 분리 | Backup의 복제 로그(`List`) + `KvStore` 적용 분리 |
| snapshot epoch | Backup의 `appliedOp` |
| failover 시 WAL trim | `appliedOp` 이후 로그 버림 |
| epoch (논문) | 사용 안 함. `OwnershipEpoch`는 Primary 세대 번호로만 사용 |

## 참고

- Zarkadas et al. *Rosé* (CIDR 2026): https://vldb.org/cidrdb/papers/2026/p8-zarkadas.pdf — §3 설계, §4 평가
- Helt et al. *Regular Sequential Serializability and Regular Sequential Consistency* (SOSP 2021) — monotonic prefix consistency 정의, C5 알고리즘
- Redis *Replication* 문서 — `min-replicas-to-write` / `min-replicas-max-lag`: 같은 발상의 느슨한 버전 (초 단위, 쓰기 거부)
- Kafka *Replication* 문서 — ISR 집합과 `replica.lag.time.max.ms`: lag 기준으로 Backup을 "동기 집합"에서 빼는 다른 접근
