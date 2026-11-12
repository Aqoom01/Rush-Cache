# cluster-split

`split-comparison`(in-process)에서 본 분할 지점 차이가 실제 cache-server 2대의 부하·지연 차이로 이어지는지 EC2에서 잰다. 한계에 적었던 "분할 후 p99·처리량"을 채우는 실험이다.

## 구성

- cache-server 2대(m6gd.medium, vCPU 1개씩) + 부하 생성 1대. 저장소는 `STORAGE`(memory 또는 nvme)로 고른다.
- 부하: `HOT_RANGE`, 해시값이 작은 쪽 key 20%에 요청 80%(split-comparison S2와 같은 모양). key 10만 개 x 1KiB, GET 70 / SET 30, 동시성 16.
- 라우팅: 공통 Runner가 key 해시를 `routing.boundaries`로 나눠 노드로 보낸다(클라이언트 쪽 라우팅). 데이터 이동은 없고, 각 실행은 재시작한 빈 서버에서 시작한다.

| config | 분할 | 노드 1 / 노드 2 요청 비율(측정 요청열) |
|---|---|---|
| `no-split.properties` | 없음 | 1.000 / 0 |
| `midpoint-split.properties` | 해시 공간 가운데(2^63) | 0.875 / 0.125 |
| `split-for-heat.properties` | 관찰 부하가 반반이 되는 key | 0.502 / 0.498 |

비율은 `split-plan.txt`에 있다.

## 분할 결정

`ClusterSplitPlanner`(experiment-runner test)가 `base.properties`의 부하로 관찰 window(예열과 같은 40만 건)를 만들어 Tablet 하나의 `TabletLoad`를 구성하고, `MidpointSplitPolicy`·`HeatSplitPolicy`(θ 0.8)의 결정을 `routing.boundaries`로 넣은 config를 쓴다.

- 관찰 window는 기준 seed + 1000003으로 만든다. Runner의 예열은 측정과 같은 요청열이라, 그것으로 결정하면 측정 데이터를 보고 결정하는 셈이 되기 때문이다.
- 결정은 seed로 정해지므로 로컬에서 만들어 커밋한다. `base.properties`를 고치면 다시 만든다.

```bash
./gradlew :experiment-runner:testClasses
java -cp experiment-runner/build/classes/java/main:experiment-runner/build/classes/java/test:cache-core/build/classes/java/main \
    io.openkache.runner.ClusterSplitPlanner \
    experiments/b2-load-balancing/cluster-split/base.properties experiments/b2-load-balancing/cluster-split
```

## 실행

Run pipeline에서 한 번에 세 config를 돌린다(약 30분).

| 입력 | 값 |
|---|---|
| `NODES` | `2` |
| `STORAGE` | `nvme` (메모리 비교가 필요하면 `memory`로 한 번 더) |
| `CONFIG` | `experiments/b2-load-balancing/cluster-split/no-split.properties experiments/b2-load-balancing/cluster-split/midpoint-split.properties experiments/b2-load-balancing/cluster-split/split-for-heat.properties` |

## 볼 것

- 결과 JSON의 `targets[]`: 노드별 요청 수·p50·p99. midpoint는 노드 1이 포화돼 p99가 크고, heat는 두 노드가 비슷해야 한다.
- 전체 처리량: no-split 대비 heat가 늘었는지. no-split과 midpoint·heat의 처리량이 같다면 부하 생성 노드가 먼저 포화된 것이다(그때는 결론을 내지 않는다).
- `metrics-node-<N>.txt`: 노드별 `openkache_storage_read_ops_total`(nvme), `openkache_requests_total`.

## 한계

- Tablet 하나를 한 번만 나눈다. 분할 시점·데이터 이동(B3)·연쇄 분할은 없다.
- 분할 후 노드 2는 빈 상태에서 시작하지 않고, 실행마다 두 노드 모두 빈 상태에서 시작한다(이동 비용·cold miss는 측정 대상이 아니다).
- 노드마다 vCPU 1개라 CPU 포화가 지연을 좌우한다. SSD 대역폭이 병목이 되는 크기는 아니다.
