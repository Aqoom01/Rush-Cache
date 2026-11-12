# EC2 이전 실제성 강화 Docker 실험

## 목적과 증거 수준

이 계층은 단일 JVM deterministic simulation과 EC2 다중 호스트 실험 사이의 중간 단계다.
노드마다 독립 container/JVM을 실행하고 실제 UDP/TCP socket, Linux cgroup, `tc netem`,
process freezer와 실제 I/O workload를 사용한다. 감지 시각과 wire frame은 profile에 따라
`real-udp-observed` 또는 `real-tcp-heartbeat-and-udp-control-observed`다.

다만 모든 container가 하나의 Docker Desktop Linux VM과 물리 host를 공유한다. 따라서
결과는 “통제된 단일 host에서 detector 구현이 보인 동작”이며 EC2의 ENA, host noisy
neighbor, AZ network, EBS latency 분포를 대표하지 않는다.

## 실행 구조

| 항목 | 기본값 | 증거 파일 |
|---|---:|---|
| node | 8 container / 8 JVM | `compose-ps.txt`, container inspect |
| CPU | node당 1.0 CPU | resolved Compose, `cpu.stat` |
| memory | node당 512MiB | Docker stats, `memory.current` |
| JVM | G1GC, Xms/Xmx 128MiB | `gc/nodeN.log` |
| transport | 실제 UDP 또는 TCP+UDP | 결과의 `wireSource` |
| detector clock | observer `System.nanoTime()` | `timeSource=observer-monotonic` |
| fault controller | host Docker CLI | `fault-events.jsonl` |
| resource sampling | Docker stats + cgroup v2 | `resource-stats.jsonl`, `cgroup/` |

기본 full matrix는 실제 host 자원 경합을 섞지 않기 위해 병렬도 1로 실행한다. 실행 순서는
고정 seed `20261030`으로 섞어 profile/fault 순서에 따른 열·캐시 편향을 줄인다.
자원 장애 실행 시간에는 Docker/Compose 준비 지연이 recovery 관찰창을 잠식하지 않도록
15초 setup budget을 추가한다. 기존 결과의 실제 관찰창은 분석기가 별도로 검증한다.

## 장애 의미

| Fault | 주입 | 의미 | 주장하지 않는 것 |
|---|---|---|---|
| `normal` | 장애·fault marker 없음, 60초 관찰 | 정상 상태 false positive baseline | 장시간 운영 안정성 |
| `crash` | target 즉시 stop | fail-stop | graceful shutdown |
| `loss15`, `loss45` | target egress netem, 기본 10초 뒤 해제 | bounded asymmetric packet-loss episode | host 간 실제 WAN loss |
| `delay` | 250±50ms target egress netem, 기본 10초 뒤 해제 | bounded network delay/jitter episode | EC2 latency 분포 |
| `cpu-pause-target` | freezer cgroup | target process 전체 pause | 실제 GC 원인 |
| `cpu-pause-observer` | freezer cgroup | observer local stall | target 장애 |
| `cpu-throttle-*` | runtime CPU quota | scheduler throttling | 완전 정지 |
| `cpu-contention-target` | 같은 cgroup CPU stress | application noisy workload | 다른 VM noisy neighbor |
| `io-contention-target` | sync HDD stress | Docker overlay I/O 경합 | SSD 고정 latency, EBS 성능 |

`io-contention-target`이 detector에 영향을 주지 않는 결과도 유효하다. 현재 UDP detector는
heartbeat 처리 경로에서 storage를 사용하지 않으므로 I/O가 CPU/scheduler를 함께 압박하지
않으면 직접 영향이 작아야 한다. SSD가 OpenKache 요청 경로에 미치는 영향을 평가하려면
실제 `cache-server`와 GET/SET workload를 연결해야 한다.

모든 11개 시나리오는 같은 판정 계약을 사용한다. `normal`은 fault marker가 없어야 하고,
`crash`는 다섯 profile 모두에서 fault 이후 탐지되어야 한다. 나머지 9개 시나리오는 target
process가 살아 있으므로 DEAD 비율을 crash 성공률로 해석하지 않는다. 네트워크·일시 자원
장애는 설정된 fault duration 뒤 반드시 해제하고 `FAULT_END`를 기록해 recovery 관찰창을
실제 주입 구간과 분리한다. Ignite/Aerospike TCP profile은 observer가 모든 peer의 첫
heartbeat를 받은 뒤에만 준비 완료로 인정한다.

## 실행

한 조건 스모크:

```bash
FD_WARMUP_SECONDS=15 \
FD_FAULT_DURATION_SECONDS=10 \
FD_RECOVERY_SECONDS=15 \
DOCKER_BIN=docker \
failure-detector-lab/scripts/experiments/run-udp-netem.sh \
  swim-lifeguard cpu-pause-target \
  failure-detector-lab/results-pre-ec2-one 8 1
```

5개 networked mechanism profile × 10 faults × 30 seeds:

```bash
DOCKER_BIN=docker \
failure-detector-lab/scripts/experiments/run-udp-pre-ec2-matrix.sh \
  failure-detector-lab/results-pre-ec2-full 30 8
```

기본 profile은 fixed deadline, Phi gossip, SWIM+Lifeguard, adaptive deadline,
Ring+multi-helper verify다. 이는 운영 제품 5개의 native binary benchmark가 아니다. Redis,
Cassandra, Ignite, Aerospike 제품 자체 결과로 표기해서는 안 된다.

운영 제품 inspired 5종의 실제 socket 행렬은 다음 wrapper를 사용한다.

```bash
DOCKER_BIN=docker \
failure-detector-lab/scripts/experiments/run-product-udp-pre-ec2-matrix.sh \
  failure-detector-lab/results-product-pre-ec2-full 30 8
```

호스트 재부팅 등으로 중단되면 `FD_MATRIX_RESUME=1`을 추가해 같은 명령을 실행한다. 이미
통과한 sequence는 CSV checkpoint를 기준으로 건너뛰고, 불완전한 실행은 `-retry-N` 경로에서
재실행하므로 기존 원시 증거를 덮어쓰지 않는다. 전체 완료 시 `COMPLETED` marker가 생성된다.

이 경로는 SWIM UDP, Redis UDP, relay 기반 Phi UDP, Ignite ring TCP+검증 UDP,
Aerospike all-to-all TCP+evidence-gated UDP를 사용한다.
제품 native binary나 wire-compatible 구현은 아니다.

분석과 SVG 생성:

```bash
python3 failure-detector-lab/scripts/analyze-udp-pre-ec2.py \
  --input failure-detector-lab/results-pre-ec2-full \
  --output failure-detector-lab/results-pre-ec2-full/report
```

## 통계 해석

- 각 조건 30회와 동일 seed 집합을 사용한다.
- crash 지연 분위수는 성공 실행만 계산하므로 성공률과 missing을 반드시 함께 표기한다.
- crash 탐지율에는 양측 Wilson 95% 신뢰구간을, p50·p95에는 10,000회 고정-seed
  비모수 bootstrap percentile 95% 신뢰구간을 함께 표기한다. bootstrap 구간은 표본 추출
  불확실성만 나타내며 단일 host의 체계적 편향을 포함하지 않는다.
- profile별 최소 fault 이후 관찰창을 충족하지 못한 실행은 `invalid_observation_runs`로
  제외한다. 느린 detector를 짧은 실험 종료로 미검출 처리하지 않기 위한 장치다.
- 기존 보고서에서 관찰창 미달 실행만 동일 seed로 재실행할 때는
  `scripts/experiments/rerun-invalid-observation.sh SOURCE_REPORT OUTPUT_DIR`를 사용한다.
- crash 외 fault의 DEAD 비율은 탐지 성공률이 아니다. 살아 있는 process에 대한 강한 장애
  판단으로 별도 해석한다.
- pause 동안 `LOCAL_UNAVAILABLE`은 서비스 관점에서 맞을 수 있지만 process death를 뜻하지
  않는다.
- 단일 smoke 값으로 profile 우열이나 p95를 주장하지 않는다.
- simulation, Docker real UDP, 이후 EC2 결과를 같은 표본으로 합치지 않는다.

## EC2 전 남은 두 단계

1. 실제 OpenKache `cache-server`에 detector adapter를 연결하고 GET/SET 부하, 요청 성공률,
   failover와 회복 시간을 함께 측정한다.
2. `m6gd` 로컬 NVMe instance store에서 detector 상태와 fio가 같은 filesystem을 사용하도록
   검증하고 read/write/flush 지연 실험이 필요하면 별도의 `dm-delay` 계층으로 측정한다.
   Docker Desktop overlay 및 gp3 EBS 결과와 합치지 않는다.

이 두 단계에서 경향이 재현되고 30회 반복 결과와 원시 로그가 완성되면 EC2에서 동일
manifest를 사용해 외적 타당성을 확인한다.
