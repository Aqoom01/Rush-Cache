# OpenKache Modular Failure Detector Lab

OpenKache 본체와 분리된 Java 17 기반 Failure Detector 비교 실험 프로젝트다. v2 명세에 따라
판단(A), 관측·전파(B), 보정(C)을 구분하고, 구현 가능한 조합만 프로필로 선언한다.

운영 제품 구조 5종(SWIM+Lifeguard, Redis Cluster, Cassandra Phi, Ignite TCP Ring,
Aerospike Mesh)의 별도 비교 경로도 제공한다. 구현 범위와 원본 제품과의 차이는
`docs/product-fd5-benchmark.md`를 따른다.

## 비교 프로필

| 코드 | ID | A: 판단 | B: 관측·전파 | C: 보정 |
|---|---|---|---|---|
| F0 | `fixed-deadline` | 고정 deadline | all-to-all heartbeat | 없음 |
| F1 | `phi-gossip` | Phi Accrual | gossip heartbeat | 없음 |
| F2 | `swim-base` | SWIM suspicion | direct/indirect probe | 없음 |
| F3 | `swim-lifeguard` | SWIM suspicion | direct/indirect probe | 기존 Lifeguard 묶음 |
| F4 | `adaptive-deadline` | EWMA 기반 adaptive deadline | F0과 같은 heartbeat | 없음 |
| F5 | `fixed-deadline-ring` | F0과 같은 고정 deadline | deterministic ring heartbeat | 없음 |
| F6 | `fixed-deadline-ring-verify` | F5와 같음 | ring + 독립 helper 경로 | Verify Suspect |
| F7a | `swim-lha-probe` | F2와 같음 | F2와 같음 | LHA-Probe만 |
| F7b | `swim-lha-suspicion` | F2와 같음 | F2와 같음 | LHA-Suspicion만 |
| F7c | `swim-buddy` | F2와 같음 | F2와 같음 | Buddy refutation만 |

F4는 Chen/Bertier 계열에서 영감을 받은 통제 baseline이고 특정 논문의 완전 재현이 아니다.
F7a-c도 각 보정 기여를 분리하기 위한 실험 프로필이다. Rapid식 multi-observer cut,
JGroups 전체 stack 복제, Hybrid detector, 자동 replica 승격은 이번 구현 범위가 아니다.

## 실행

저장소 루트의 Gradle Wrapper를 사용한다.

```bash
./gradlew -p failure-detector-lab test selfTest
./gradlew -p failure-detector-lab run --args="--help"
./gradlew -p failure-detector-lab run --args="--seeds 30 --nodes 5,25,100 \
  --profiles F0,F1,F2,F3,F4,F5,F6,F7a,F7b,F7c \
  --scenario normal,crash,crash-under-jitter,packet-loss,packet-loss-burst,latency,latency-gradual,latency-bursty,latency-regime,observer-pause,target-pause,partition \
  --traffic-mode realistic --output-dir results-modular-v2-new"
```

`--config`는 Java properties 파일을 읽으며 `seeds`, `nodes`, `profiles`, `scenarios`,
`trafficMode`, `outputDir`을 지원한다. 출력 writer는 기존 manifest/CSV/JSONL/chart가 있는
디렉터리를 거부하므로 결과를 덮어쓰지 않는다.

`detailedExperiment`와 `parameterSweep`는 기존 SWIM/Phi 집중 분석 흐름을 보존한 legacy
도구다. v2 전체 프로필 비교는 위 `run` 명령을 사용한다.

## 결과 계약

- `run-manifest.json`: Git/JDK/OS와 run별 profile code, A/B/C 모듈, scenario/seed/node.
- `runs.csv`, `runs.jsonl`: run 단위 nullable 지연, false state, 비용과 모듈 메타데이터.
- `events.jsonl`: ground truth, simulation, detector 전이 이벤트.
- `summary.csv`: metric별 n/missing/mean/p50/p95/p99/95% CI와 측정 기원.
- `module-contributions.csv`: F0↔F1/F4, F0↔F5, F5↔F6, F2↔F3/F7a-c의 paired delta.
- `compatibility.csv`: A/B/C 전체 조합의 호환 여부, 선언 프로필, 거부 이유.

감지 시각과 오판 상태는 `simulation-observed`, frame/byte는 선언한 크기를 합산한
`model-estimated`다. 실제 UDP 결과는 별도 `result.json`에 `real-udp-observed`로 기록한다.
시뮬레이션 수치와 실제 UDP 수치를 하나의 통계로 합치지 않는다.

## 실제 UDP + Docker/netem

Lab 전용 harness는 3·5·8노드를 지원한다. F0~F6, F7a, F7b는 실행 가능하고 F7c는 현재
시뮬레이션 전용이다. target `node1`에만 `CAP_NET_ADMIN`을 주며 runner 종료 시 전용
Compose 자원을 정리한다.

```bash
./gradlew -p failure-detector-lab jar
DOCKER_BIN='/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe' \
  failure-detector-lab/scripts/experiments/run-udp-netem.sh \
  F6 crash failure-detector-lab/results-udp-f6-new 8
```

실제 harness는 detector의 UDP 감지 경로를 확인한다. 제품 `cache-server` 통합,
GET/SET workload와 자동 failover는 측정하지 않는다. 자세한 해석과 제한은
`docs/experiment-methodology.md`, 검증 이력은 `docs/verification.md`를 따른다.

### EC2 이전 자원 장애 행렬

동일한 실제 UDP 경로에서 target/observer CPU pause, CPU quota 제한, CPU contention,
Docker 가상 디스크 I/O contention을 주입할 수 있다. 모든 노드는 기본 1 CPU·512MiB,
고정 128MiB heap으로 실행하며 Docker stats, cgroup cpu/io, GC 로그와 fault 시각을 보존한다.

```bash
DOCKER_BIN=docker \
  failure-detector-lab/scripts/experiments/run-udp-pre-ec2-matrix.sh \
  failure-detector-lab/results-pre-ec2-full 30 8

python3 failure-detector-lab/scripts/analyze-udp-pre-ec2.py \
  --input failure-detector-lab/results-pre-ec2-full \
  --output failure-detector-lab/results-pre-ec2-full/report
```

자원 경합 실험은 host 간 네트워크나 실제 SSD 고정 지연을 재현하지 않는다. 전체 범위와
EC2 승격 기준은 `docs/pre-ec2-realistic-experiment.md`에 기록한다.

운영 제품 inspired 5종을 동일한 Docker 계층에서 실행할 때는 다음 wrapper를 사용한다.
SWIM/Redis/Phi는 실제 UDP, Ignite는 persistent TCP, Aerospike는 TCP heartbeat+UDP agreement를
사용한다.

```bash
DOCKER_BIN=docker \
  failure-detector-lab/scripts/experiments/run-product-udp-pre-ec2-matrix.sh \
  failure-detector-lab/results-product-pre-ec2-full 30 8
```

중단된 행렬은 같은 설정과 출력 디렉터리로 재개할 수 있다. `passed` sequence는 건너뛰고,
중단·실패 sequence는 기존 증거를 덮어쓰지 않는 `-retry-N` 디렉터리에서 다시 실행한다.

```bash
FD_MATRIX_RESUME=1 DOCKER_BIN=docker \
  failure-detector-lab/scripts/experiments/run-product-udp-pre-ec2-matrix.sh \
  failure-detector-lab/results-product-pre-ec2-full 30 8
```

모든 결과는 `inspired-real-socket-adapter`이며 제품 native benchmark로 표기하지 않는다.

## 운영 제품 기반 5종 벤치마크

```bash
failure-detector-lab/scripts/run-fd-benchmark.sh \
  --algorithm all --nodes 8 --scenario all --seeds 1-30 \
  --output failure-detector-lab/results-fd5-new

python3 failure-detector-lab/scripts/analyze-fd-results.py \
  --input failure-detector-lab/results-fd5-new \
  --output failure-detector-lab/results-fd5-new/report
```

출력은 5개 프로필 모두 `implementation_type=inspired`로 기록한다. 감지 지연은
`simulation-observed`, 통신 비용은 `model-estimated`이며 실제 UDP 결과와 합치지 않는다.
