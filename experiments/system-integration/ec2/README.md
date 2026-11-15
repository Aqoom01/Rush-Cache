# OpenKache 캐시 동시 통합 실행

노트북 없이 실행하고 S3를 재검증한 뒤 EC2를 정리하는 새 CI 경로는 [HEADLESS_EXECUTION.md](HEADLESS_EXECUTION.md)를 따른다. 현재는 코드·모의 검증 단계이며 실제 계정에서의 end-to-end 실행은 아직 하지 않았다.

기본 실행은 **A1 + A2 + A3 + B1 + B2 + B3**를 함께 사용한다. A1은 복제된 요청 승인·소유권 기록과 승계, A2는 SSD 데이터 복제·읽기를 맡는다. [전체 방법론 감사](METHOD_AUDIT.md), [구조와 범위](../FULL_SYSTEM_SCOPE.md), [실험 설계](EXPERIMENT_PLAN.md), [현재 검증](AUDIT_VALIDATION.md).

## 빌드와 로컬 실행

```bash
./gradlew build :cache-server:integrationZip --offline --console=plain
unzip cache-server/build/distributions/openkache-integration.zip -d /tmp/openkache-joint
python3 /tmp/openkache-joint/tools/integration.py verify-bundle --bundle /tmp/openkache-joint
python3 /tmp/openkache-joint/tools/integration.py plan   --a1 VR --a2 V07 --fd SWIM_LIFEGUARD   --distribution TAG --split HEAT --placement GREEDY   --scaler scads --scheduler tidb-feedback --copy POST --drain incremental-drain   --scenario AUTO --key-mode TAGGED --requests 1000 --rate 50   --output /tmp/openkache-joint-input
python3 /tmp/openkache-joint/tools/integration.py local   --bundle /tmp/openkache-joint --plan /tmp/openkache-joint-input   --output /tmp/openkache-joint-run
```

`plan` 입력의 기본값은 A1=RAFT, A2=O00이다. `--a1`과 `--a2`는 별도로 교체한다. `--protocol`은 과거 독립 엔진 대조군 전용이며 한 A 역할이 빠지는 legacy 모드다. 전체 통합 결과로 집계하지 않는다.

## 교체 가능한 축

| 옵션 | 후보 |
|---|---|
| `--a1` | PB, PB_FENCED, RAFT, VR, CHAIN, CRAQ, ROSE |
| `--a2` | O00, V01, V02, V03, V04, V05, V06, V07 |
| `--fd` | 기존 FIXED_TIMEOUT/SWIM/SWIM_LIFEGUARD와 Lab F0/F1/F2/F3/F4/F5/F6/F7A/F7B/F7C/REDIS_CLUSTER/PHI_GOSSIP/IGNITE_TCP_RING/AEROSPIKE_MESH |
| `--read-selection` | A2 V06의 ADAPTIVE / ROUND_ROBIN |
| `--distribution` | RANGE, SLOT, RING, RING_ALLOC, TAG, RESIZABLE, ORDER |
| `--split` | NONE, MIDPOINT, HEAT, ESTORE |
| `--placement` | NONE, GREEDY, HYSTERESIS, COST, ESTORE, P2C, SLICER, RENDEZVOUS, FIXED_LPT(B3 원본 고정 배치 대조군) |
| `--scaler` | fixed, scads, elastman, estore, accordion, pstore, tiramola, pax, aws-warmup |
| `--scheduler` | fifo, mongodb, tidb-token, tidb-feedback, iter-gwin, single-mcs, slamig, vmbuddies, fpta |
| `--copy` | STOP, PRE, POST |
| `--drain` | preflight-all, incremental-drain |

A3는 주요5개 방식에 대해 `F3`(Lab SWIM/Lifeguard), `REDIS_CLUSTER`, `PHI_GOSSIP`, `IGNITE_TCP_RING`, `AEROSPIKE_MESH`를 선택한다. F0~F7c 프로필 및 기존 대조군이 겹치므로17개 선택지를17개 독립 알고리즘으로 세지 않는다. Ignite coordinator 장애/재선출과 큰 F6 helper 수 등의 제약은 [감사 결과](METHOD_AUDIT.md)에 있다.

`--vr-mode`와 `--rose-limit`은 A1 세부 설정이다. A2 세부 옵션은 `--override prepare.max.entries=...`처럼 전달한다. topology·방법론 선택은 named option으로만 지정한다. `plan --help`에 workload·budget 전체 옵션이 있다. TAG/ORDER에서는 `--key-mode TAGGED/ORDERED`를 사용한다.

## 실제 프로세스와 EC2

기본은 준비된 3그룹 × replica JVM 3개 + gateway + load = **11 JVM**이다. 각 replica JVM 안에 두 native 엔진과 계층별 FD가 있다. `--groups 2`면 8 JVM이다. 데이터 그룹은 2~8개를 준비할 수 있고 `--initial-groups`, `--min-groups`로 active 범위를 정한다. SCALE_CYCLE은 scale-out 후 drain/scale-in, AUTO는 반복 관찰/판단/이동을 실행한다. 예비 replica도 실행하므로 물리 서버 비용이 절감되는 실험은 아니다.

포트 기준 `P=replica.port`: A2 TCP=P, A2 FD UDP=P+1, A2 HTTP=P+4. A1 TCP=P+8, A1 peer TCP=P+9, A1 FD UDP=P+10, A1 HTTP=P+12. Ignite/Aerospike FD는 각각 해당 FD UDP 포트와 같은 번호의 TCP listener도 사용한다. gateway는 별도 gateway.port다. 해당 실험 노드 사이에서만 이 포트를 열어야 한다. `resource-samples.json`은 `native`에 A2, `a1_authority.native`에 A1 진단을 담는다. 요청 승인/데이터 호출 계수는 gateway `components`에 있다. 각 replica의 `a1-authority/`와 `a2-data/`에 이벤트·ready·KV가 분리된다.

`topology.example.json`은 프로젝트 호스트 한도에 맞춘 2그룹 예시다. 이 파일로 증감하려면 `plan --topology topology.example.json --initial-groups 1 --scenario SCALE_CYCLE --a1 VR --a2 V07 ...`로 입력을 만든다.

기존 전용 EC2 topology에는 `replica_hosts`(3의 배수), `gateway_host`, `load_host`, `ssh_user`, `remote_root`, `environment`를 제공한다. 기본은 dry-run이며 SSH/AWS 호출을 하지 않는다.

```bash
python3 tools/integration.py ec2 --bundle /path/to/unpacked-bundle   --plan /path/to/input --output /path/to/results
# 기존 전용 호스트에서 실행할 때만 다음 옵션을 추가한다.
# --execute --identity /path/to/key --known-hosts /path/to/known_hosts
```

인스턴스를 만들거나 삭제하지 않는다. 프로젝트의 AWS 사용 한도·실제 가용 호스트 수 안에서 topology를 정해야 한다. replica 그룹 3개는 9 replica host가 필요하므로 프로젝트 기본 교보재 한도(서버8대+load1대)에서 EC2 topology를 그대로 사용하면 안 된다. 그룹2개(6 replica host)+gateway1+load1로 실행하거나 사전에 승인된 다른 배치를 쓴다. 로컬 리허설 loopback 주소는 같은 호스트의 독립 JVM이다.

## 장애·정합성·결과

CRASH_MOVE/CRASH_SCALE은 진행 중 이동을 확인한 뒤 소유 JVM을 종료한다. `--fault-layer A1/A2`는 어느 계층 leader를 기준으로 **종료할 co-located JVM**을 고를지 정한다. JVM 종료/정지는 두 계층 모두에 영향을 준다. 계층 단독 실패는 Java 통합 테스트에서 따로 검증한다. Chain/Rosé master는 실제 종료와 A3 DEAD 증거를 확인한 뒤 실행한다. PAUSE_LEADER/OBSERVER는 측정 중 1초 SIGSTOP/SIGCONT다. fault 창을 놓친 실행은 유효한 장애 결과가 아니다.

`result.json`의 correctnessValid, migrationValid, coverageValid, jointComponentsValid, fdRuntimeValid와 원본 audit/requests, fault, cleanup을 함께 확인한다. RUN_COMPLETE는 수집 완료 표시다. 불확정 metadata는 routingUncertain/SAFE_HOLD로 전체 요청을 차단한다. P-Store history 부재나 CPU 첫 표본을 성공한 정책 판단으로 집계하지 않는다. `--calibration` CSV는 nodes,reads,writes,p99,completed를 초 단위로 받고 `--history`는 실제 관찰 rate 한 줄씩을 받는다. 측정 프로세스 간 monotonic 시각을 직접 빼지 않는다.

## 실험 입력

```bash
python3 tools/integration.py matrix --stage smoke --output smoke.csv
python3 tools/integration.py matrix --stage coverage --output coverage.csv
python3 tools/integration.py matrix --stage exhaustive --count-only
python3 tools/integration.py matrix --stage exhaustive --a1s RAFT,VR --a2s O00,V07   --distributions RANGE --splits NONE --placements GREEDY --count-only
```

Smoke는 A1/A2 56쌍에 V06의 두 읽기 정책을 구분한 63변형 × 3 copy =189행이다. coverage는 A1/A2/A3 및 V06 읽기 정책을 별도 축으로 모든 호환 가능한 전략 쌍을 덮는다. 현재 입력은275전략×2부하×3seed=1,650행이다. exhaustive 기본은 368,518,248행(2 workload×3 seed 포함)으로 `--allow-large` 없이는 파일 생성을 막는다. matrix 생성은 실험 실행이 아니다. 고차 상호작용 전수 실측이나 우승 방법론 판정을 마쳤다는 뜻이 아니다. shortlist는 `--a1s`, `--a2s`로 각각 줄인다.

## CSV의 조합을 실제로 순차 실행

`matrix`는 입력만 만들며 `batch`가 실행을 맡는다. 기본은 dry-run이다. 출력 디렉터리는 새 이름을 사용한다.

```bash
python3 tools/integration.py matrix --stage smoke --output smoke.csv
python3 tools/integration.py batch --matrix smoke.csv --bundle /path/to/bundle --output /path/to/runs --limit 10
python3 tools/integration.py batch --matrix smoke.csv --bundle /path/to/bundle --output /path/to/runs --limit 10 --execute
```

`--start 10 --limit 10`으로 다음 구간을 실행한다. `--plan-arg=--requests --plan-arg=1000`처럼 공통 부하 인수를 전달할 수 있다. FD 조정도 `--plan-arg=--override --plan-arg=fd.heartbeat.ms=200`으로 전달한다. 행에 기록된 방법론 선택을 공통 인수로 덮어쓰는 것은 거절한다. `batch-results.json`과 CSV는 실패·미지원도 남기며 cleanup이 불완전하면 다음 행으로 넘어가지 않는다. P-Store에는 실제 history 입력을 제공해야 한다.

`--mode ec2`의 실제 일괄 실행에는 `--execute --topology ... --identity ... --known-hosts ...`가 필요하다. 기존 전용 호스트만 사용하며 인스턴스를 생성하거나 삭제하지 않는다. 기본 matrix는 로컬3그룹 기준이므로 프로젝트 한도의 EC22그룹에서는 `--plan-arg=--initial-groups --plan-arg=1` 등 가용 inventory에 맞는 입력을 사용한다.

## 동일한 전략을 운영 시나리오 7개에서 검증

`operating_suite.py`는 CSV의 각 전략을 균등 부하, 특정 해시 구간 쏠림, 느린 A2 복제본, 부하 급증에 따른 자동 증설, 부하 감소에 따른 자동 축소·drain, 일시적 네트워크 지연·손실, 실제 리더 JVM 종료에서 실행한다. TCP 답신 80ms 지연은 느린 복제본에, 250ms 지연과 30% 손실은 리더에서 같은 그룹의 peer로 나가는 실험용 TCP/UDP에 적용한다. 리더 종료는 A1 리더를 기준으로 하며 CSV의 `fault_layer=A2`로 바꿀 수 있다.

전략 CSV에는 다음과 같이 후보를 넣는다. 전수 목록에서 일부를 뽑으려면 `matrix --stage exhaustive --workloads UNIFORM --seeds 1 --key-mode DEFAULT --offset 0 --limit 10 --output strategies.csv`를 사용한다. 시나리오와 시드는 실행 단계에서 전략에 교차한다.

```csv
a1,a2,fd,distribution,split,placement,scaler,scheduler,copy,drain
RAFT,V06,F3,RANGE,HEAT,ESTORE,pax,slamig,PRE,incremental-drain
```

```bash
python3 /path/to/bundle/tools/operating_suite.py --matrix strategies.csv --seeds 1 2 3
python3 /path/to/bundle/tools/operating_suite.py --matrix strategies.csv --seeds 1 2 3 \
  --bundle /path/to/bundle --topology /path/to/dedicated-topology.json \
  --identity /path/to/key --known-hosts /path/to/known_hosts --output /path/to/suite-runs --execute
```

- 전체 시나리오와 지정한 모든 시드에서 정합성, 이동, 통합 구성, 장애 감지 검증과 성공률 기준을 만족해야 전략에 `PASS`가 붙는다. 성공률 기본값은 95%이며 `--min-success`로 조정한다. 지연 기준이 있으면 `--max-p99-ms`를 지정한다.
- 증감은 두 부하 구간으로 진행한다. 부하 전환 뒤 scaler가 판단하고 목표 그룹 수에 도달해 소유권 이전을 마쳐야 통과한다. 고정 그룹 수 전략은 자동 증감 기준을 통과할 수 없다. 실제 환경에 맞춰 `--low-rate`, `--base-rate`, `--high-rate`, `--phase-seconds`를 보정한다. P-Store처럼 학습 입력이 필요한 전략에는 `--history`와 `--calibration`을 제공할 수 있다.
- 실제 실행에는 두 그룹의 Linux 복제본 6대, gateway와 load 각 1대가 필요하다. 복제본에 `ip`, `tc netem`, 비대화식 `sudo -n tc` 권한이 있어야 한다. 호스트 세트는 한 실행에 전용으로 쓴다. 실험 실행기가 네트워크 설정이나 프로세스의 정리를 확인할 수 없으면 다음 실행을 멈춘다.
- `suite-results.json`은 전략별 판정, `cases.json`은 시나리오별 판정과 실행 시간을 담는다. `--stop-on-failed-case`를 사용하면 탈락한 전략의 남은 실험을 생략한다.
- 61,419,708가지 전체 전략을 7개 시나리오에서 실행하면 429,937,956회다. [기존 추정](CORE_FOCUS_PLAN.md)의 155회당 8~12시간을 적용하면 순차 실행 약 2,531~3,797년이다. 이 환산은 운영 시나리오 7개의 EC2 실측 시간이 아니므로 첫 실행의 `wall_seconds`로 갱신한다.
