# B2 정책 벤치마크 결과 (Split 3종, Placement 7종)

2026-11-05 실행. 설계는 [design.md](design.md) §9, 원본·자동 요약은 아래 파일이다. 모든 값은 **in-process 계획 계산값**이다. 서버·네트워크·실제 데이터 이동이 없으므로 QPS·SSD IOPS 개선으로 읽지 않는다. 결과는 같은 seed로 다시 돌리면 결정 시간 외에는 똑같이 나온다(2회 실행 CSV 비교로 확인).

| Benchmark | Baseline | 원본(CSV) / 집계(JSON) / 표(MD) |
|---|---|---|
| A. Split | `midpoint-split` | `split-comparison/benchmark-a.*` |
| B. Placement | `greedy-load-placement` | `placement-comparison/benchmark-b.*` |
| C. Split + Placement | `midpoint-split + greedy-load-placement` | `end-to-end/benchmark-c.*` |

개선율은 seed 10개를 짝지어 계산한 평균이고, [ ]는 95% 신뢰구간(t 분포)이다. 낮을수록 좋은 지표는 (Baseline − 후보) / Baseline × 100이라 **양수가 개선**이다.

## 요약

1. **Split: 부하를 보는 정책이 몰린 부하에서 확실히 낫다.** `split-for-heat`는 S2(Hot Range)에서 다음 window 큰 쪽 비율이 0.875에서 0.509로 줄었다(+41.9% [+41.2, +42.5]). 하지만 burst 한 번에 반응해 자르면 다음 window에서 오히려 나빠진다(BURSTY −59.2%).
2. **Placement: Baseline(greedy)이 균형은 가장 좋지만 데이터를 가장 많이 옮긴다.** 20 window 동안 약 240-310MB를 옮겼다. 부하 기반 정책(`estore-inspired`, `power-of-two-choices`, `cost-aware`, `slicer-inspired`)은 정적 부하에서 **균형은 0-4.7% 나쁘고 이동량은 96-100% 적었다**.
3. **어떤 정책도 모든 부하에서 이기지 못한다.** 균형만 놓고 보면 이동 hotspot에서는 greedy가 가장 좋고, burst에서는 `weighted-rendezvous`가 가장 좋았다.
4. **Split과 Placement는 함께 골라야 한다.** `slicer-inspired`(이동 예산 9%)는 분할로 Tablet이 늘어나는 C에서 예산 때문에 따라가지 못해 HOT_RANGE −21-28%, SINGLE_HOT_KEY −18-40%로 가장 나빴다. 같은 Hot Range에서 `split-for-heat + hysteresis`/`cost-aware`는 Baseline과 같은 균형을 이동량 4-7%로 냈다.

## Benchmark A: Split (Tablet 하나, key 1,000개, 요청 4,000건, θ 0.8)

결정 window로 자르고 **다음 window**로 잰다. 보류한 실행은 큰 쪽 비율을 0으로 넣지 않고 따로 센다.

**회귀 검증 (기존 split-comparison과 같은 입력)**

| 시나리오 | 지표 | 기존 결과 | 이번 측정 |
|---|---|---|---|
| S1 | midpoint / heat 큰 쪽 비율 | 0.514 / 0.509 | 0.514 / 0.509 |
| S2 | midpoint / heat | 0.875 / 0.509 | 0.875 / 0.509 |
| S3 | midpoint 큰 쪽 비율, 분할 | 0.952, 10/10 | 0.952, 10/10 |
| S3 | heat 보류 | 10/10 `SINGLE_HOT_KEY` | 10/10 `SINGLE_HOT_KEY` |

**다음 window 큰 쪽 비율 (분할한 실행만, 평균)과 midpoint 대비 개선율**

| 시나리오 | midpoint-split | split-for-heat | estore-inspired |
|---|---|---|---|
| S1 균등 | 0.514 | 0.509, +0.8% [−1.0, +2.7] | 0.744, **−45.0%** [−67.9, −22.2], 효과 없는 분할 4/10 |
| S2 Hot Range | 0.875, 효과 없음 10/10 | **0.509, +41.9%** [+41.2, +42.5] | 0.679, +22.4% [+12.3, +32.5] |
| S3 단일 Hot Key(90%) | 0.952, 효과 없음 10/10 | 보류 10/10 | 0.950, 효과 없음 10/10 |
| ZIPF | 0.575 | **0.518, +9.0%** [+2.3, +15.8] | 0.665, −16.5% [−36.0, +3.0] |
| MOVING_HOTSPOT | 0.878 | 0.688, +21.7% [+20.7, +22.6] | 0.692, +21.2% [+9.5, +32.8] |
| BURSTY (burst window로 결정) | **0.515** | 0.819, **−59.2%** [−80.8, −37.6], 효과 없음 7/10 | 0.799, −55.2%, 효과 없음 7/10 |

- **결정 시간(중앙값, 참고):** midpoint 0.03-0.07μs, heat 약 1.2μs, estore 2-11μs다. S1 heat의 17μs는 첫 실행의 JIT 영향이다.
- **MOVING_HOTSPOT:** heat는 결정 window에서는 0.501로 정확히 나눴다. 하지만 hotspot이 5% 이동한 다음 window에서는 0.688이 됐다. 관찰한 부하가 그대로 이어진다는 가정이 깨지면 효과가 줄어든다.
- **estore-inspired의 약점:** key당 요청이 4건뿐인 균등 부하에서는 우연히 평균의 2배를 넘는 key 묶음을 hot으로 잘못 잡아 엉뚱한 곳을 자른다(S1·ZIPF). 이 정책은 hot/cold 경계를 자를 뿐 반반으로 나누지 않으므로, 한 번 자르는 지표에서는 불리하다. 효과는 반복 분할(C)에서 따로 본다.

## Benchmark B: Placement (key 2만 개, Tablet 64개 고정, 노드 4대, 20 window)

노드 용량은 평균 이용률이 0.7이 되게 잡았다. window t의 부하로 결정하고 t+1의 부하로 잰다. `no-rebalance(참고)`는 빠지는 노드만 비우는 비교용 기준이고 Baseline이 아니다.

**평균 최대/평균 이용률 (1.0이 완전 균형)과 20 window 이동량(MB)**

| 시나리오 | greedy (Baseline) | slicer | estore | p2c | rendezvous | cost-aware | hysteresis | no-rebalance |
|---|---|---|---|---|---|---|---|---|
| UNIFORM | 1.013 / 306 | 1.018 / 0.6 | 1.018 / 0.1 | 1.018 / 0.1 | 1.136 / 16 | 1.018 / 0 | 1.018 / 0 | 1.018 / 0 |
| HOT_RANGE | 1.017 / 300 | 1.065 / 10.9 | 1.026 / 3.1 | 1.033 / 5.3 | 1.679 / 16 | 1.033 / 3.0 | 1.028 / 2.9 | 3.249 / 0 |
| SINGLE_HOT_KEY(30%) | 1.242 / 271 | 1.275 / 4.9 | 1.242 / 4.9 | 1.242 / 4.9 | 1.881 / 16 | 1.242 / 5.1 | 1.242 / 4.9 | 1.905 / 0 |
| ZIPF | 1.018 / 276 | **1.017** / 6.2 | 1.032 / 1.3 | 1.033 / 1.7 | 1.278 / 16 | 1.035 / 1.1 | 1.094 / 0.5 | 1.306 / 0 |
| MOVING_HOTSPOT | **1.292** / 305 | 1.506 / 32 | 1.464 / 28 | 1.457 / 38 | 1.490 / 16 | 1.515 / 24 | 1.562 / 18 | 2.651 / 0 |
| BURSTY | 1.520 / 309 | 1.405 / 11 | 1.559 / 57 | 1.477 / 58 | **1.271** / 16 | 1.545 / 53 | 1.591 / 38 | 1.435 / 0 |
| HETEROGENEOUS(1:1:2:4) | 1.020 / 243 | 1.039 / 7.1 | 1.031 / 4.5 | 1.033 / 4.8 | 1.386 / 16 | 1.036 / 3.1 | 1.068 / 3.6 | 2.254 / 0 |
| SCALE_OUT(4→5) | 1.033 / 284 | 1.046 / 10 | 1.052 / 3.9 | 1.051 / 5.8 | 1.311 / 20 | 1.057 / 3.0 | 1.109 / 3.3 | 1.486 / 0 |
| SCALE_IN(5→4) | 1.008 / 289 | 1.012 / 11 | 1.017 / 5.9 | 1.015 / 6.3 | 1.401 / 21 | 1.020 / 5.4 | 1.060 / 4.9 | 1.234 / 4.1 |

**greedy 대비 평균 최대/평균 개선율**(음수 = greedy보다 덜 균형):

- **정적 부하** (UNIFORM, HOT_RANGE, ZIPF, HETEROGENEOUS, SCALE_*): estore·p2c·cost-aware·slicer는 +0.2 ~ −4.7%(ZIPF의 slicer만 +0.2%), hysteresis는 −0.5 ~ −7.4%, rendezvous는 −12 ~ −65%
- **MOVING_HOTSPOT:** 모든 후보가 −12.7 ~ −20.9%로 greedy보다 나쁨
- **BURSTY:** rendezvous **+16.3%** [+13.7, +18.9], slicer +7.5% [+3.7, +11.3]. hysteresis는 −4.7%
- **SINGLE_HOT_KEY:** 대부분 0.0%로 같음. Tablet 하나가 30%를 받아 재배치만으로는 1.24 아래로 내려갈 수 없다

**같은 개선에 든 이동 비용.** 정적 부하에서 greedy는 균형을 1-3% 더 얻는 대신, 다른 부하 기반 정책보다 이동량이 수십-수백 배 많았다(HOT_RANGE에서 slicer 대비 27배, hysteresis 대비 100배). UNIFORM에서는 다른 정책이 거의 옮기지 않았다. 부하가 거의 그대로여도 greedy는 Tablet 배치를 처음부터 다시 정하기 때문이다(UNIFORM에서도 20 window에 958개를 옮김).

**결정 시간**(중앙값, 참고): greedy와 p2c가 약 30μs, slicer·estore·rendezvous가 10-20μs, cost-aware·hysteresis가 5-10μs다.

## Benchmark C: Split + Placement (처음 Tablet 8개, 노드 4대, 20 window)

분할 시작 조건은 Tablet 부하가 노드 용량의 1/4을 3 window 연속으로 넘는 것이다. 분할 후 3 window는 다시 자르지 않고, Tablet은 많아야 256개다. 이어서 같은 window에서 배치를 정한다.

**평균 최대/평균, 이동 MB, 분할 수(효과 없는 분할)**

| 시나리오 | midpoint + greedy (Baseline) | heat + greedy | heat + slicer | estore + estore | heat + cost-aware | heat + hysteresis |
|---|---|---|---|---|---|---|
| HOT_RANGE | 1.131 / 245 / 11.0 (0.4) | 1.156 / 271 / 10.0 (0) | 1.442 / 6.8 | 1.232 / 23 / 15.1 (5.6) | **1.127 / 17** / 10.0 (0) | **1.125 / 11** / 10.0 (0) |
| SINGLE_HOT_KEY(30%) | 1.286 / 251 / 7.8 (**6.0**) | 1.243 / 269 / 2.8 (0) | 1.804 / 2.0 | **1.236 / 5.2** / 3.8 (1.7) | 1.243 / 5.2 / 2.8 (0) | 1.243 / 5.1 / 2.8 (0) |
| ZIPF | **1.047** / 171 / 13.2 (5.2) | 1.064 / 196 / 8.2 (0) | 1.095 / 3.8 | 1.059 / 6.5 / 9.2 (1.2) | 1.094 / 3.9 | 1.143 / 2.8 |
| MOVING_HOTSPOT | **1.748** / 198 | 1.805 / 198 | 2.097 / 24 | 1.899 / 73 | 1.812 / 78 | 1.866 / 85 |
| BURSTY | 1.507 / 296 | 1.487 / 307 | **1.454 / 8.9** | 1.490 / 38 | 1.588 / 35 | 1.479 / 27 |
| HETEROGENEOUS | **1.038** / 140 | 1.053 / 178 | 1.257 / 5.6 | 1.172 / 11 | 1.100 / 6.9 | 1.148 / 8.4 |

`midpoint + slicer`는 heat + slicer와 비슷하게 나빴다(HOT_RANGE 1.369, SINGLE_HOT_KEY 1.523). UNIFORM·SCALE_OUT·SCALE_IN에서는 모든 조합이 1.01-1.14 범위였다. 전체 수치는 `end-to-end/benchmark-c.md`.

**Baseline 대비 개선율(평균 최대/평균)**

- **SINGLE_HOT_KEY:** estore + estore **+3.9%** [+3.8, +4.0], heat + greedy·cost-aware·hysteresis는 각각 +3.4%다. midpoint는 hot key가 든 Tablet을 가운데에서 반복해 잘라 7.8회 중 6.0회가 효과 없는 분할이었다.
- **HOT_RANGE:** heat + hysteresis +0.6% [−0.4, +1.6], heat + cost-aware +0.4% [−0.8, +1.6]로 Baseline과 차이가 없다. 그런데 이동량은 각각 Baseline의 4%, 7%다.
- **나머지 시나리오:** 대부분 Baseline이 가장 균형이 좋고(−1 ~ −28%), 그 대가로 이동량이 가장 크다.

## 시나리오별 Trade-off와 실패하는 상황

| 정책 | 강한 곳 | 약한 곳 (원인) |
|---|---|---|
| `split-for-heat` | 몰린 부하(S2·ZIPF), 효과 없는 분할 회피(S3) | burst·이동 hotspot: 결정 window에 맞춰 자르므로 부하가 바뀌면 효과가 사라지거나 반대가 된다 |
| `estore-inspired` (split) | 단일 hot key를 두 번에 걸쳐 떼어 냄(C SINGLE_HOT_KEY 1위) | key당 요청이 적은 균등 부하에서 hot 오판(A S1 −45%), 한 번의 분할로는 반반이 아님 |
| `greedy-load-placement` | 거의 모든 시나리오에서 가장 균형, 이동 hotspot 1위 | 이동량 최대(UNIFORM에서도 매 window 재배치), cooldown 없음 |
| `slicer-inspired` | 정적 부하에서 적은 이동, BURSTY(B +7.5%) | 이동 예산 9%로 큰 변화(분할로 Tablet이 늘 때, 이동 hotspot)를 따라가지 못함(C −18 ~ −40%) |
| `estore-inspired` (placement) | 정적 부하에서 이동량 최소 수준으로 greedy와 1-1.3% 차이 | burst에 반응해 이동이 늘고 균형은 안 좋아짐(B BURSTY) |
| `power-of-two-choices` | 전체를 보지 않고도 estore와 거의 같은 결과 | 후보 2개만 보므로 노드가 많을 때 결과 편차가 커질 수 있음(이 실험은 4-5대라 확인 못 함) |
| `weighted-rendezvous` | 노드 변화 시 그 노드의 Tablet만 이동, burst가 인접 Tablet에 몰릴 때 흩어져 있어 유리(B BURSTY 1위) | 부하를 보지 않아 hot range·단일 hot key·이종 노드에서 가장 나쁨(−12 ~ −65%) |
| `cost-aware-rebalancing` | 이득이 비용보다 클 때만 이동, C HOT_RANGE에서 Baseline과 같은 균형을 이동 7%로 | 비용 단가(0.001 요청/byte)와 기간(5 window)이 가정값이라 결과가 이 값에 달림, BURSTY에서 burst를 지속 부하로 보고 옮겨 −5.4% |
| `hysteresis-rebalancing` | 작은 흔들림 무시, 이동 최소 | 1.25 아래의 불균형을 그대로 둠(ZIPF −7.4%, SCALE_* −5 ~ −7%), 이동 hotspot에서 가장 늦게 반응(B −20.9%) |

## 한계

- **계획 계산값:** 데이터 이동 시간·이동 중 성능 저하·캐시 miss를 넣지 않았다. 이동 MB는 "옮겨야 할 양"이지 "걸리는 시간"이 아니다. 실제 영향은 B3 이동과 연결한 뒤 cluster-split 같은 EC2 실험으로 본다.
- **자체 결정 파라미터:** 노드 4-5대, Tablet 64개, 20 window, 평균 이용률 0.7, 여러 정책 기준값(hot 배수 2, tolerance 1.05, 비용 단가 등)은 모두 프로젝트에서 정했다. 다른 값에서는 순위가 바뀔 수 있다(민감도 실험은 하지 않았다).
- **논문 재현이 아님:** Slicer·E-Store 정책은 각 논문의 핵심 아이디어 하나만 가져온 경량 구현이다. 세부 알고리즘은 자체 설계다([references/slicer.md](references/slicer.md), [references/estore.md](references/estore.md)).
- **결정 시간:** JIT·GC 영향을 받는 단일 JVM 측정이라 순서 비교용 참고값이다.
