# cluster-placement 결과 (EC2 실측)

2026-11-05 실행. GitLab CI 한 번으로 48회(시나리오 5 × 정책 4-6 × seed 2)를 연달아 돌렸다. 조건과 방법은 [README.md](README.md)를 따른다.

- **원본:** `s3://openstd-s15p31d111-results/results/b2-cluster-placement/2026-11-05/` (실행당 폴더 1개, commit `d642c37c`)
- **표:** [results-table.md](results-table.md) (`ClusterPlacementReport` 자동 생성, 정책별 seed 범위와 greedy 대비 개선율)
- **그래프:** [figures/](figures/) (`make_charts.py`로 생성, 값은 `figures/chart-data.csv`)

## 집계 기준

| 지표 | 범위 | 뜻 |
|---|---|---|
| 처리량 | window 10-19 | 측정 요청/초. miss 뒤 재적재 SET은 세지 않는다 |
| 불균형 | window 10-19 | 가장 바쁜 노드 요청 ÷ 클러스터 노드 평균(요청 0인 노드 포함). 1.0이 완전 균등 |
| GET miss 비율 | window 10-19 | 이동 비용. Tablet을 옮기면 새 주인이 비어 있어 miss가 난다 |
| 이동 Tablet, 재적재 SET | window 0-19 합 | |

앞쪽 window를 뺀 이유가 있다. 실행마다 cache-server를 재시작하므로 앞쪽 3-5개 window는 JIT 예열로 정책과 무관하게 느리다(window 0 처리량 6-15k). SCALE_OUT은 window 10이 노드 추가 시점이라, 이 범위가 추가 직후와 그 뒤를 모두 포함한다.

## 확인한 전제

1. **계획대로 재생했다.** 48회 모두 실측 이동 Tablet 수가 계획(`bundle*/plan.md`)과 같았고, 불균형 차이는 최대 0.000이었다.
2. **처리량의 상한은 부하 생성 노드였다.** 측정 요청과 재적재 SET을 합친 서버 연산은 아래 예외를 빼면 정책과 관계없이 **초당 약 2.3-2.4만 건**으로 같았다(`fig4-ops-ceiling.svg`). 이때 가장 바쁜 서버의 CPU도 55-65%에 그쳤다. 따라서 이 실험의 처리량 차이는 대부분 **재적재 SET이 클라이언트 용량을 얼마나 먹었는지**를 보여 준다. 균형이 좋아져 서버 용량이 늘어나는 효과는 서버가 포화된 경우에만 보인다. 예외는 둘이다.
   - **HOT_RANGE, 재배치 없음(no-rebalance, cost-aware):** hot 노드 CPU가 0.94-0.96으로 포화돼 약 1.6만에서 멈췄다.
   - **SCALE_OUT, 4번째 노드를 쓴 정책:** 1.8-2.0만이었다. 새 노드는 데이터도 없고 JIT 예열도 안 된 상태로 투입된다.
3. **cost-aware는 이 규모에서 한 번도 옮기지 않았다.** 비용 단가가 0.001 요청/byte로 고정이라 Tablet 하나(약 8MB)의 이동 비용이 요청 약 8,000건으로 잡힌다. window당 2만 건 규모에서는 부하 감소 × 5 window가 이를 넘지 못했다. 그래서 결과가 no-rebalance와 같다. 이는 정책의 효과가 아니라 **비용 단가를 데이터 크기·요청률에 맞추지 않은 결과**다. 계산 실험에서 적은 약점("비용 단가가 가정값이라 결과가 이 값에 달림")이 실제로 드러났다.

## 결과 요약

seed 2개 평균이다. 개선율은 같은 seed의 greedy 대비 값이다(`results-table.md`).

| 시나리오 | 정책 | 처리량 | 불균형 | miss | 이동 Tablet | 최악 노드 p99 |
|---|---|---|---|---|---|---|
| HOT_RANGE | greedy | 16.2k | 1.016 | 47.9% | 913 | 6.2ms |
| | **estore** | **22.4k** | **1.013** | 6.6% | 9 | 6.2ms |
| | hysteresis | 21.3k | 1.014 | 6.6% | 9 | 6.8ms |
| | slicer | 21.2k | 1.020 | 11.7% | 37 | 6.8ms |
| | cost-aware / no-rebalance | 15.9-16.0k | 3.244 | 0% | 0 | 9.5-11.0ms |
| ZIPF | greedy | 19.9k | 1.026 | 20.7% | 853 | 5.7ms |
| | **estore** | **23.7k** | 1.032 | 1.4% | 7 | 5.7ms |
| | slicer | 22.8k | **1.020** | 4.3% | 19 | 5.6ms |
| | hysteresis | 22.8k | 1.054 | 0.5% | 2 | 7.4ms |
| | cost-aware / no-rebalance | 23.3-23.5k | 1.270 | 0% | 0 | 6.2-6.7ms |
| MOVING_HOTSPOT | greedy | 14.6k | **1.282** | 63.7% | 897 | 5.4ms |
| | rendezvous | **16.3k** | 1.463 | 49.8% | 50 | 6.5ms |
| | hysteresis | 16.2k | 1.544 | 43.8% | 61 | 5.7ms |
| | slicer | 15.9k | 1.552 | 52.9% | 94 | 6.6ms |
| BURSTY | greedy | 16.4k | 1.470 | 48.8% | 913 | 8.0ms |
| | **slicer** | **19.5k** | 1.393 | 12.5% | 22 | 5.8ms |
| | rendezvous | 17.9k | **1.312** | 31.9% | 50 | 5.7ms |
| | hysteresis | 17.6k | 1.429 | 38.0% | 78 | 5.6ms |
| SCALE_OUT | greedy | 15.6k | **1.027** | 22.5% | 577 | 14.2ms |
| | **slicer** | 18.2k | 1.066 | 8.2% | 22 | 15.9ms |
| | rendezvous | 15.2k | 1.331 | 18.0% | 50 | 14.4ms |
| | cost-aware | 23.8k | 1.439 | 0% | 0 | 5.7ms |

## 질문별 답

### 1. 고정 편중: greedy의 균형 이득은 이동 비용을 감수할 만한가? → 아니다

- **균형 차이가 사라졌다.** 계산 실험에서 greedy는 1-3% 더 균등했다. 실측에서는 estore·hysteresis·slicer가 greedy와 0.01 이내였다(HOT_RANGE 1.013-1.020 vs 1.016). 처음 몇 window에 hot Tablet을 옮기고 나면 부하가 그대로라 더 옮길 필요가 없기 때문이다.
- **이동 비용은 그대로 남았다.** greedy는 window마다 배치를 처음부터 다시 정해 20 window에 Tablet 850-910번을 옮겼다(64개 중 매 window 약 70%). 그래서 후반에도 GET의 21-48%가 miss였다. 다른 정책은 0.5-12%였다.
- **처리량은 estore가 greedy보다 HOT_RANGE +38.6%, ZIPF +19.6% 높았다.** 위 전제 2에 따라, 이는 서버 용량 차이가 아니라 재적재 SET이 줄어든 효과다.
- **재배치를 아예 안 하면 HOT_RANGE에서 hot 노드가 포화된다.** 처리량은 약 1.6만으로 다른 정책보다 25-29% 낮았고, 최악 노드 p99는 9.5-11.0ms로 다른 정책(6.2-6.8ms)보다 50% 이상 높았다. ZIPF는 불균형이 1.27로 hot 노드가 포화되지 않아 재배치 없이도 처리량이 높았다.

### 2. 동적 부하: 빨리 따라가는 것과 억제하는 것 중 무엇이 유리한가? → 양 극단이 아니라 이동 예산

- **MOVING_HOTSPOT:** 균형은 계산 실험과 같이 greedy가 가장 좋았다(1.28 vs 1.46-1.55). 하지만 miss가 64%로 가장 높아 처리량이 가장 낮았다. 나머지 셋은 처리량 15.9-16.3k로 차이가 작다. hotspot이 window마다 움직이면 어떤 정책도 miss를 40% 아래로 줄이지 못했다.
- **BURSTY:** 계산 실험에서 1위였던 rendezvous가 실측에서도 가장 균등했다(1.31). 처리량은 이동 예산으로 burst에 덜 끌려다닌 slicer가 가장 높았다(miss 12.5%, greedy 대비 +18.4%). hysteresis는 burst 때마다 임계를 넘어 반응해 miss가 38%였다.

### 3. Scale-out: 이동을 줄이면서 얼마나 빨리 나눌 수 있는가? → slicer가 균형점

`fig6-scale-out.svg`에서 새 노드가 받는 요청 비율의 변화를 본다(목표 25%).

| 정책 | window 10 | 11 | 12 | 13 이후 | window 10 처리량 |
|---|---|---|---|---|---|
| greedy | 25% | 25% | 26% | 25-26% | 약 9.5k(급락) |
| slicer | 9% | 15% | 23% | 25% | 약 17k |
| rendezvous | 30% | 30% | 30% | 29-30% | 약 9k(급락) |
| cost-aware | 0% | 0% | 0% | 0% | 변화 없음 |

- **greedy:** 바로 25%를 맞췄지만 전체 배치를 다시 섞어 추가 직후 처리량이 절반 가까이 떨어졌다.
- **rendezvous:** 원리대로 새 노드 몫만 옮겼다(50개). 다만 부하를 보지 않아 새 노드에 30%가 갔고 불균형이 1.33이었다.
- **slicer:** 3 window에 나눠 옮겨 급락 없이 25%에 도달했다. miss 8%, 불균형 1.07이다.
- **cost-aware:** 새 노드를 쓰지 않았다(전제 3). 처리량이 가장 높은 건 서버가 포화되지 않은 상태에서 miss가 0이기 때문이다. 노드를 늘린 효과가 아니다.

## 계산 실험과 비교

| 항목 | 계산 실험 (`policy-benchmark.md`) | EC2 실측 |
|---|---|---|
| 정적 부하의 균형 순위 | greedy가 1-3% 앞섬 | 차이 0.01 이내. 여러 window를 지나면 다른 정책도 따라잡음 |
| 이동량 | greedy가 27-100배 | 이동 Tablet 수 20-100배, miss 2-30배 |
| MOVING 균형 | greedy 1위 | 같음 |
| BURSTY 균형 | rendezvous 1위 | 같음 |
| SCALE_OUT | rendezvous 이동 20MB > cost-aware 3MB | cost-aware는 단가 문제로 이동 0, rendezvous는 새 노드 몫만 이동 |

**균형은 계획으로 정해지므로 계산과 실측이 같다(전제 1). 실측이 새로 보여 준 것은 이동 비용의 크기다.**

## 한계

- **부하 생성기 상한:** 서버가 포화되지 않아, 균형 개선으로 서버 용량이 늘어나는 효과는 측정하지 못했다(전제 2). 다음 실행에서 동시 요청을 64-128로 올려 서버 포화 지점을 먼저 찾아야 한다.
- **seed 2개:** 신뢰구간이 없다. "두 seed가 같은 방향인가"까지만 말할 수 있다. 위 결론은 모두 두 seed가 같은 방향이었다(`results-table.md`의 범위).
- **miss 비용이 과소평가됐다:** 재적재는 SET 한 번뿐이다. 실제 cache-aside라면 원본 DB 조회가 더해져 miss 비용이 더 크다.
- **miss가 오래 남는 조건이다:** key 50만 개에 window당 요청 2만 건이라, 옮긴 Tablet의 key가 다시 요청돼 채워지기까지 오래 걸린다. key당 요청이 많은 부하라면 miss가 더 빨리 줄어든다.
- **정합성은 재지 않았다:** greedy처럼 Tablet이 예전 주인에게 돌아가면, 그 노드에 남아 있던 이전 값을 읽을 수 있다. 이 실험은 정합성을 재지 않는다(데이터 이동·무효화는 B3 영역).
- **cost-aware 미검증:** 비용 단가를 맞추지 않아 이동 정책으로서는 검증되지 않았다.
- **CPU 측정 오차:** CPU는 `/metrics`의 window 간 CPU 시간 차이라 1.0을 조금 넘는 값(1.02)이 나올 수 있다.

## 재현

```bash
aws s3 cp --recursive --profile openstd \
    s3://openstd-s15p31d111-results/results/b2-cluster-placement/2026-11-05/ ./cp-results/
./gradlew :experiment-runner:testClasses
java -cp experiment-runner/build/classes/java/main:experiment-runner/build/classes/java/test:cache-core/build/classes/java/main \
    io.openkache.runner.ClusterPlacementReport ./cp-results experiments/b2-load-balancing/cluster-placement/results-table.md
python3 -I experiments/b2-load-balancing/cluster-placement/make_charts.py ./cp-results experiments/b2-load-balancing/cluster-placement/figures
```
