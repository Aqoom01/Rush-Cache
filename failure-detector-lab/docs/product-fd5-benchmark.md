# 운영 제품 기반 Failure Detector 5종 비교

## 목적과 분류

이 경로는 OpenKache의 기존 SWIM/Lifeguard, Phi, Ring 실험 코드를 보존하면서 다음 5개
완성형 구조를 같은 deterministic fault schedule에서 비교한다.

| 프로필 | 구현 범위 | 분류 |
|---|---|---|
| `swim-lifeguard` | direct/indirect probe, suspicion, incarnation, gossip confirmation, LHA timeout | Inspired, 기존 코드 재사용 |
| `redis-cluster` | PING/PONG, local PFAIL, 유효기간이 있는 master report, weak-majority FAIL, FAIL 전파 수신 | Inspired |
| `phi-gossip` | heartbeat arrival history, Phi conviction, versioned gossip heartbeat, local UP/DOWN | Inspired, 기존 코드 재사용 |
| `ignite-tcp-ring` | logical-ring neighbor timeout, coordinator 검증, next-alive neighbor, versioned topology | Inspired |
| `aerospike-mesh` | all-to-all mesh heartbeat와 별도 membership proposal/ack/commit | Inspired |

어느 결과도 Redis, Cassandra, Ignite, Aerospike 제품 자체의 native benchmark가 아니다.
`native` mode 이름은 각 inspired 프로필의 기본 설정을 사용하는 Mode A를 뜻한다.

## 공식 동작과 구현 경계

- Redis Cluster spec의 PFAIL/FAIL 의미, `NODE_TIMEOUT`, 시간창 안 master 과반수 failure
  reports, FAIL message를 반영했다. FAIL 이후 replica election과 승격은 포함하지 않는다.
- Cassandra 문서대로 heartbeat state는 gossip하지만 UP/DOWN은 observer-local 판단이며,
  DOWN을 gossip하거나 자동 membership removal로 바꾸지 않는다.
- Ignite TCP Discovery의 logical ring, coordinator topology verification, failed neighbor 이후
  다음 생존 이웃 연결을 모델링했다. 실소켓 adapter는 successor가 predecessor로 보내는
  persistent TCP heartbeat와 별도 helper 경유 UDP 검증을 사용한다. Ignite native handshake와
  wire format은 재현하지 않는다.
- Aerospike heartbeat 탐지와 membership 변경을 분리했다. 모든 peer 사이에 TCP heartbeat를
  유지하고, removal ACK는 각 helper도 target heartbeat timeout을 관측한 경우에만 보낸다.
  agreement adapter는 명시적인 epoch+majority 모델이며 Aerospike의 Paxos 기반 clustering을
  재현했다고 주장하지 않는다.
- SWIM/Lifeguard 실소켓 adapter는 LHA awareness에 따라 probe 주기와 timeout을 동적으로
  조정하고 scheduler 지연을 local-health 입력으로 반영한다. memberlist wire compatibility는
  아니다.

Docker adapter도 모두 제품 binary가 아닌 inspired 구현이다. Redis와 Phi는 실제 UDP datagram,
Ignite는 ring TCP heartbeat와 별도 검증 UDP, Aerospike는 all-to-all TCP mesh heartbeat와
evidence-gated UDP agreement message, SWIM/Lifeguard는 실제 UDP probe/gossip 경로를 사용한다.
Phi heartbeat는 제3 노드 relay를 거쳐 observer에 전달되며 Redis FAIL은 master peer에
broadcast된다. 따라서 `real-socket-observed`는 OS
socket을 실제 통과했다는 뜻이지 제품 wire compatibility를 뜻하지 않는다.

참고 자료:

- [HashiCorp memberlist](https://github.com/hashicorp/memberlist)
- [Lifeguard paper](https://arxiv.org/abs/1707.00788)
- [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Apache Cassandra Dynamo architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)
- [Apache Ignite TCP/IP discovery](https://ignite.apache.org/docs/ignite2/latest/clustering/tcp-ip-discovery)
- [Ignite TCP Discovery internals](https://cwiki.apache.org/confluence/spaces/IGNITE/pages/109445242/TCP%2BDiscovery%2BSPI%2Bunder%2Bthe%2Bhood)
- [Aerospike heartbeat](https://aerospike.com/docs/database/manage/network/heartbeat/)
- [Aerospike clustering architecture](https://aerospike.com/docs/database/learn/architecture/clustering/)

## 공통 실험 계약

- 기본 8노드, fault start 5초, run horizon 30초, monotonic virtual nanoseconds.
- 같은 algorithm/scenario/seed는 재현 가능한 packet draw를 사용한다.
- 시나리오: normal, crash, loss15, loss45, delay-jitter, target-stall,
  observer-stall, consecutive-3, consecutive-4, partition.
- 정규화 이벤트: `FAULT_INJECTED`, `LOCAL_SUSPECTED`, `LOCAL_UNAVAILABLE`,
  `CLUSTER_CONFIRMED`, `MEMBERSHIP_CHANGED`, `MEMBERSHIP_CONVERGED`, `RECOVERED`.
- 프로토콜에 없는 이벤트는 만들지 않는다. 예를 들어 Phi의 cluster confirmation과
  membership removal, Redis FAIL의 membership removal은 N/A다.
- 감지와 상태 전이는 `simulation-observed`, frame/byte는 `model-estimated`다.
  기존 Docker UDP 결과(`real-udp-observed`)와 합치지 않는다.
- Mode A 주요 값: Redis node timeout 15,000ms/report validity 30,000ms, Ignite neighbor
  network timeout 5,000ms, Aerospike heartbeat interval 150ms/timeout 10 intervals,
  Cassandra heartbeat 1,000ms/Phi threshold 8, SWIM은 기존 Lab 기본 설정.

## 실행

```bash
./gradlew -p failure-detector-lab test selfTest

failure-detector-lab/scripts/run-fd-benchmark.sh \
  --algorithm all \
  --nodes 8 \
  --scenario all \
  --seeds 1-30 \
  --mode native \
  --output failure-detector-lab/results-fd5-new

python3 failure-detector-lab/scripts/analyze-fd-results.py \
  --input failure-detector-lab/results-fd5-new \
  --output failure-detector-lab/results-fd5-new/report
```

EC2 이전 실제 socket/cgroup 행렬은 별도 runner로 실행한다.

```bash
DOCKER_BIN=docker \
  failure-detector-lab/scripts/experiments/run-product-udp-pre-ec2-matrix.sh \
  failure-detector-lab/results-product-pre-ec2-full 30 8

python3 failure-detector-lab/scripts/analyze-udp-pre-ec2.py \
  --input failure-detector-lab/results-product-pre-ec2-full \
  --output failure-detector-lab/results-product-pre-ec2-full/report
```

분석기는 detector별 최소 fault 이후 관찰창을 검사한다. 관찰창 미달 실행은
`invalid_observation_runs`로 분리하며 성공률과 지연 분위수에서 제외한다.

algorithm/scenario/seed를 하나만 지정할 수도 있다. writer는 기존 결과가 있는 디렉터리를
덮어쓰지 않는다. 분석기는 외부 Python 패키지 없이 CSV에서 PNG와 Markdown을 재생성한다.

## 결과 구조

```text
results-fd5-new/
  raw/run-*.jsonl
  summary/runs.csv
  summary/detection_latency.csv
  summary/false_positive.csv
  summary/membership_convergence.csv
  summary/network_overhead.csv
  summary/detection_success.csv
  charts/*.png
  report/algorithm_comparison.csv
  report/experiment_config.json
  report/benchmark_report.md
```

## 현재 한계

- Docker adapter는 실제 UDP/TCP socket, CPU/RSS/cgroup/GC를 관측하지만 제품 binary나 제품
  wire protocol이 아니다.
- Mode B calibration dataset과 calibrated configuration은 미수행이다.
- 형식 simulation의 stall은 event-loop 모델이고, Docker 계층은 freezer/cgroup throttling을
  실제 적용한다. 두 계층의 수치는 합치지 않는다.
- 실제 OpenKache `cache-server` GET/SET routing과 replica failover는 연결하지 않았다.
- 따라서 현재 순위는 구현 검증과 다음 실험 후보 선정에만 사용하고 최종 제품 선택 근거로
  단독 사용하지 않는다.
- 위 transport/합의/LHA 수정 이전에 생성한 결과 디렉터리는 새 구현 결과와 합치지 않는다.
  11개 시나리오는 동일 seed로 전부 재실행해야 한다.
