# PB·VR·Raft 실제 서버 복제·리더 crash 실험

[최종 EC2 250회 결과](final-results.md) · [재현 계획](final-validation-plan.md)

[A1·A3 로컬 펜싱 검증](fencing-local.md)

[Raft + SWIM/Lifeguard 결합·로컬 검증·EC2 실행](raft-fd.md)

[Raft 비교 실험: 타임아웃 고정·14조건·발표용 252회·그래프/S3 보관](raft-full-campaign.md)

[정식 EC2 실행: 노트북을 꺼도 계속 진행하는 coordinator 서비스](raft-ec2-coordinator.md)

[2026-11-10 실제 EC2 시작 기록·결과 경로](raft-execution-20261110.md)

[전체 campaign 로컬 검증 기록](raft-local-validation.md)

## Chain·CRAQ·Rosé 실서버와 종합 비교 (9~12단계)

`protocol=CHAIN`(`chain.read.mode=TAIL_ONLY|CRAQ`)과 `protocol=ROSE`(`rose.limit`)를 같은 `PbServerMain` 조립부에 추가했다. 두 엔진은 자체 failure detector가 없으므로 실험 controller가 **정확한 영구 crash를 아는 외부 master** 역할을 맡아 peer opcode38(FREEZE/REPAIR/OPEN)과 37(로그 digest)로 복구를 조정한다. 감지는 controller의 고정 대기(timeout.ms)이며 실제 failure detection이 아니다. wire 형식은 [protocol-v1 §11](../../../docs/protocol-v1.md)이다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_chain_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새-chain-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_chain.py <chain-결과폴더>
python experiments/a1-active-passive-failover/real-server/run_craq_ec2.py ... --out <새-craq-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_craq.py <craq-결과폴더>
python experiments/a1-active-passive-failover/real-server/run_rose_ec2.py ... --out <새-rose-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_rose.py <rose-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_overall.py
```

- [Chain 위치별 crash 36회](chain-results.md): head/middle/tail × 값64B/8KiB × 대기500/1000ms × 3회. 정상 쓰기는 head, 읽기는 tail(비tail GET은 redirect). head는 직전 쓰기의 역방향 Ack 전 새 쓰기에 BUSY를 돌려주므로 client가 재시도한다.
- [CRAQ 읽기 경로 12회](craq-results.md): TAIL_ONLY vs CRAQ를 조건마다 번갈아 측정. 노드별 clean GET, hot key 동시 쓰기 중 middle GET의 dirty 비율·단조성.
- [Rosé 변형 30회](rose-results.md): L ∈ {0,1,4,16,∞} × 값 × 3회. client 8개 동시 쓰기와 쓰기 진행 중 Primary crash 후 OK 유실 수(상한 L) 검증.
- [종합 비교](overall-results.md): 위 cohort와 PB/VR/Raft 선출 비교·VR 전송 비교를 같은 축으로 모은 표와 보장·전제 비교, 재현 절차. 조건이 다르므로 순위로 읽지 않는다.

## VR 전송 경로 개선 전후 비교

S15P31D111-187 후속으로 `VrEngine.Mode.INCREMENTAL_CATCHUP`을 추가했다. Primary가 Backup별로 이미 보낸 로그 범위를 넘지 않는 COMMIT만 통지하고, 같은 view에서 PREPARE_OK를 보낸 정상 Backup의 GET_STATE를 전체 로그 대신 확인 위치 이후 DeltaPrepare로 처리한다. 과반수 확정·view change·로그 선택·전송 창·재전송 backoff는 그대로다. 기존 `INCREMENTAL`(설정 `vr.mode` 생략 시 기본)과 이전 결과는 보존한다. [전후 비교 결과](catchup-results.md)는 같은 배포본·부하·계측으로 조건마다 두 모드를 번갈아 측정한 24회다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_catchup_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새-catchup-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_catchup.py <catchup-결과폴더>
```

모드2 × 값64B/8KiB × 감지500/1000ms × 3회다. `--quick`은64B/500ms에서 두 모드 각1회다. `traffic_bench.py --mode`는 서버 상태의 `vrMode`가 요청 모드와 같은지 확인만 하며 결과 schema는 `a1-real-vr-traffic-2`다. 유실 환경의 복구 지연(재전송 timer 의존)은 실서버에서 측정하지 않았다.

## VR 정상 전송량 원인 계측

S15P31D111-187에서 opcode별 송신 성공 frame/byte와 VR GET_STATE·전체 START_VIEW 발생 원인을 추가했다. 알고리즘 동작·재전송 정책은 유지한다. [VR 전송량 분해](traffic-results.md)는 장애 주입 없이 값64B/8KiB × FD500/1000 ×3회=12회다. warm30건을 세 노드가 적용하고 데이터 전송이 안정된 뒤 정상128건을 측정하며, 즉시/안정화 후 snapshot을 모두 저장한다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_traffic_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새-traffic-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_traffic.py <traffic-결과폴더>
```

`--quick`은64B/500ms/1회다. 실제 TCP frame 비용과 프로토콜 원인 카운터를 대조하며 OS TCP 재전송이나 원격 수신 완료는 측정하지 않는다. 카운터·warmup 안정화 절차가 달라 이전 cohort와 전후 성능 개선 비교를 하지 않는다. wire 진단은 [protocol-v1 §10](../../../docs/protocol-v1.md)에 정의했다.

## PB·VR·Raft 선출 비교

S15P31D111-186에서 `protocol=RAFT`와 4종 bounded RPC codec을 추가했다. 기본 진입점/공통 계약은 유지하며 기존 Raft 엔진의 알고리즘은 바꾸지 않았다. 초기 선거와 장애 후 선거를 혼동하지 않도록 최근 로컬 선거의 별도 불변 진단만 추가했다. [세 방식 공통 결과](election-results.md)는 같은 배포본으로 새로 측정한36회다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_election_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새-election-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_election.py <election-결과폴더>
```

PB/VR/RAFT × 값64B/8KiB × timeout기준500/1000ms × 반복3 =36회다. `--quick`은64B/500ms 각1회다. 실제 선출 리더의 호스트에서 controller를 실행하고 같은 호스트의 전용 JVM을 종료한다. 초기 리더ID·term은 관측해 기록한다. Raft는 `[기준,2×기준)`의 난수 선거 timeout을 사용하므로 PB/VR과 감지 지연을 동일하게 고정한 비교는 아니다. 나머지 부하·값검증·재시도 조건은 공통이다. 이전 PB42회·PB/VR24회는 별도 결과로 보존한다.

## PB·VR 동일 조건 비교

S15P31D111-185에서 기존 PB 전용 조립부에 `protocol=VR` 선택과 bounded VR codec을 추가했다. core 엔진·공통 계약은 변경하지 않고 동일 Netty transport를 사용한다. 별도 [비교 결과](compare-results.md)는 기존 PB 실험과 분리된 새 cohort다. 설정에서 protocol을 생략하면 PB를 유지한다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_compare_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새-compare-결과폴더>
python experiments/a1-active-passive-failover/real-server/analyze_compare.py <compare-결과폴더>
```

PB/VR × 값64B/8KiB × 감지500/1000ms × 3회=24회, warmup30건·정상128건이다. `--quick`은 두 방식의64B/500ms 각1회이며 본 집계에서 제외한다. 양쪽 Backup 값을 확인한 뒤 종료하는 settled idle crash를 비교하며, 재시도 정책·서버 배치는 공통이다. VR 전체 로그가 기존2MiB frame 안에 들어오도록 두 방식 모두128건으로 제한했다. 기존 PB1000건 결과와 요청 수가 다르므로 해당 수치를 직접 대조하지 않는다. 체크포인트·분할 전송·최대 처리량·SSD 성능·partition·재시작은 비교 범위에 없다.

아래는 이전 PB 실험 조건과 재현 설명이다.

관련 이슈: S15P31D111-160(peer 연결), S15P31D111-179(crash 승격·재시도·실측).

기존 `PrimaryBackupEngine` 및 `CrashOnlyPrimaryBackupEngine`을 server 모듈의 실제 Netty TCP Transport·owner worker·system monotonic 시계로 조립한다. 별도 `PbServerMain` 진입점이며 기본 단일 노드 실행은 유지한다. core production 의존성과 공통 API는 변경하지 않는다.

## 범위

- 고정 3노드: Primary 0, Backup 1/2. 전원 적용 ACK 후 Primary KV 적용 및 OK.
- 최초 Primary JVM의 **단일 영구 crash**만 지원한다. Backup 1이 승계하고 Backup 2의 INSTALL/READY 확인 및 KV barrier 이후 쓰기를 재개한다.
- `failure.detector=FIXED_TIMEOUT`은 처음 유효한 Primary heartbeat를 받은 뒤 기존 고정 timeout 감지를 시작한다. `SWIM`과 `SWIM_LIFEGUARD`는 별도 UDP plane에서 DEAD를 확정한 뒤 같은 INSTALL/READY 승격 경로를 실행한다.
- 전용 보안 그룹 내 TCP 7000(client), 7001(peer), SWIM 사용 시 UDP 7002. 프로토콜은 [protocol-v1 §7·§12](../../../docs/protocol-v1.md)이다. 주소를 고정 설정하며 peer 인증/TLS는 없다.
- 노드별 연결 재사용, 비동기 connect/write, 대기 전송 상한 1024, 연결 timeout 500ms. 전송 완료와 실제 복제 ACK를 구분한다. SET 자동 재전송·로그 복구·client table은 추가하지 않았다.
- Client 재시도는 같은 operation/key/value를 사용한다. 현재 PB는 client ID 중복 제거가 없어 exactly-once 보장을 하지 않는다. 이 실험의 SET은 같은 값 재적용이 가능한 형태이며 버전 증가까지 중복 제거한다고 주장하지 않는다.
- 기본 `protocol=PB`는 partition 안전성·프로세스 재시작/재가입·두 번째 crash·동적 구성·DELETE 복제·영속화를 제외한다. 로컬 `PB_FENCED` 결합 모드는 partition 중 quorum과 lease fencing을 검증하지만 재시작/재가입·두 번째 crash는 여전히 제외한다. TIMEOUT은 취소가 아니며 서로 다른 미완료 쓰기 이력은 서비스 재개를 막는다.

## 실행 조건

|항목|값|
|---|---|
|서버|같은 AZ의 m6gd.medium 3대, Ubuntu 24.04 arm64, OpenJDK 21|
|저장|노드별 독립 KvEngine 인메모리. NVMe/SSD를 이 실험에서 측정하지 않음|
|네트워크|프라이빗 IPv4, 실제 TCP, 인위적 지연/손실 없음|
|부하|원래 Primary EC2에서 Python client 실행, persistent client socket, 동시성 1, closed-loop|
|warmup|조건별 고유 key SET 30건. 정상 상태 측정은 별도 1000건|
|값|64B / 8KiB, 고정 seed1 payload, 고유 key|
|감지|heartbeat 50ms, timeout 500/1000ms, tick 10ms|
|반복|값 2 × timeout 2 × 반복 3 = 12회|
|장애|정상 측정·Backup 값 검증이 끝난 뒤 최초 Primary JVM에 SIGKILL. 장애 주입 시 진행 중 쓰기는 없음|
|재시도|시도 deadline 1000ms, socket timeout 1500ms, 재시도 간격 10ms, 전체 복구 예산 15초|

Python controller와 최초 Primary JVM은 같은 EC2에 공존하므로 정상 비용에는 해당 CPU 경합이 들어간다. 최대 처리량 벤치마크가 아니라 단일 client 관측값이다. 30건 warmup과 1000개 sample의 예비 측정이며 운영 p99 추정으로 일반화하지 않는다.

### 시계와 안전성 확인

- controller가 SIGKILL syscall을 호출하기 직전부터 새 Primary(epoch 2)의 첫 SET OK를 받기까지 `perf_counter_ns`로 측정한다. kill 호출 전후 시각을 모두 기록한다. EC2 인스턴스 자체는 종료하지 않는다.
- 서버 1의 `readyNs - detectedNs`만 같은 JVM의 전환 시간으로 계산한다. 서로 다른 호스트의 nanoTime을 차감해 감지 시간을 계산하지 않는다.
- 장애 전 OK를 받은 1000개 key/value를 양쪽 Backup에서 진단 경로로 확인한 뒤 종료한다. 새 Primary 쓰기 성공 뒤 같은 1000개와 새 key를 생존 두 노드에서 다시 확인한다.
- epoch1 쓰기를 새 Primary에 보내 STALE_EPOCH, Backup 직접 쓰기에 NOT_PRIMARY/Redirect(1)를 확인한다.
- **ACK 유실/단절과 미완료 쓰기 중 crash의 실서버 검증은 후속**이다. 가상/로컬 테스트 결과를 이 측정의 보장으로 섞지 않는다.

## 재현

JDK21에서 `gradlew build :cache-server:distTar`로 배포본을 만든다. 서버에는 Java21/Python3, SSH 접속 및 전용 보안 그룹이 준비되어 있어야 한다.

각 서버에는 아래 형식의 `node.properties`를 사용한다(실제 값은 로컬 파일로만 관리).

```properties
node.id=0
peer.0=<서버0 프라이빗IP>
peer.1=<서버1 프라이빗IP>
peer.2=<서버2 프라이빗IP>
client.port=7000
peer.port=7001
admin.port=9091
heartbeat.ms=50
timeout.ms=500
tick.ms=10
```

SWIM+Lifeguard로 최초 Primary의 영구 crash를 자동 감지하려면 아래를 추가한다. 모든 프로세스가 뜨기 전 오판하지 않도록 기본 3초의 startup grace를 둔다. baseline 비교군은 `failure.detector=SWIM`으로 바꿔 같은 UDP·A1 경로에서 Lifeguard 보정만 끈다.

```properties
failure.detector=SWIM_LIFEGUARD
failure.port=7002
failure.startup.grace.ms=3000
failure.probe.interval.ms=200
failure.direct.timeout.ms=50
failure.indirect.timeout.ms=50
failure.suspicion.ms=500
failure.lifeguard.awareness.max.multiplier=8
failure.lifeguard.suspicion.max.multiplier=2
failure.lifeguard.max.confirmations=1
```

Lifeguard는 최종 probe 실패와 로컬 scheduler 지연에 따라 awareness score를 높이고, probe 주기·direct/indirect timeout과 suspicion 최소 시간을 `score+1`배로 늘린다. 성공 probe마다 score를 1 낮춘다. 최초 SUSPECT의 deadline은 `failure.suspicion.ms × awareness 배수 × suspicion.max.multiplier`이며, 3노드에서 다른 한 노드의 독립 SUSPECT가 확인되면 `failure.suspicion.ms × awareness 배수`까지 단축한다. Buddy System은 이 프로필에 포함하지 않는다.

`protocol=PB`와 결합하면 SWIM 자동 승격은 기존 crash-only 안전 모델을 그대로 따르므로 Primary JVM의 단일 영구 crash 실험에만 사용한다. A1의 quorum·lease fencing까지 결합한 검증은 `protocol=PB_FENCED`와 같은 detector 설정을 사용해 [A1·A3 로컬 펜싱 검증](fencing-local.md)으로 실행한다. 이 모드는 SUSPECT만으로 승격하지 않고, DEAD 이후 Node 2 투표와 옛 lease 만료를 통과해야 Node 1이 epoch 2 쓰기를 연다.

host 파일은 public/private/instance 항목을 가진 객체 3개의 JSON 배열이다. 키·호스트 정보·원본 접속 로그는 Git 제외 로컬 폴더에 둔다. 자동화는 이 서버들의 `/home/ubuntu/a1-real` 전용 PB 프로세스만 관리한다. 프로세스 cmdline을 확인하고 다른 실행 파일은 중단하지 않는다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_ec2.py --hosts <로컬-hosts.json> --key <키파일.pem> --known-hosts <known-hosts> --dist cache-server/build/distributions/openkache-server-0.1.0-SNAPSHOT.tar --out <새-결과폴더>
```

같은 A1 복구 경로에서 고정 timeout, SWIM, SWIM+Lifeguard를 비교하려면 `run_compare_ec2.py`를 detector별 새 결과 폴더로 실행한다. SWIM 계열의 `t500/t1000`은 fixed timeout이 아니라 suspicion 최소 시간이며 결과 JSON의 `failureDetectorConfig`에 실제 설정이 남는다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_compare_ec2.py ... --detector FIXED_TIMEOUT --out <새-fixed-결과폴더>
python experiments/a1-active-passive-failover/real-server/run_compare_ec2.py ... --detector SWIM --out <새-swim-결과폴더>
python experiments/a1-active-passive-failover/real-server/run_compare_ec2.py ... --detector SWIM_LIFEGUARD --out <새-lifeguard-결과폴더>
```

`--quick`은 64B/500ms/1회, 100건의 예비 실행이다. full과 섞어 집계하지 않는다. 자동화는 JVM 기동/중단·배포·JSON 다운로드·노드별 로그 수집을 수행하며 종료 시 전용 PB JVM을 정리한다. EC2는 유지한다.

## 연속 쓰기 중 crash

`run_ec2.py`에 `--load-crash`를 추가하면 별도 `pb_load.py`를 실행한다. 정상 warmup 30건 후 고유 key SET을 closed-loop·동시성1로 연속 제출한다. 300건 OK 다음 요청의 `sendall` 직후, 응답을 읽기 전에 원래 Primary JVM에 SIGKILL을 보낸다. 장애 후 같은 operation/key/value로 재시도하고 새 Primary(epoch2)에서 300건 OK를 더 받는다. 이 결과는 기본 유휴 crash 결과와 별도 폴더/schema로 저장한다.

**client 요청 진행 중이라는 시점은 보장하지만 서버의 특정 로그/ACK 단계까지 강제하지는 않는다.** 첫 시도의 연결 오류와 후보 key의 재시도 전 복사본을 따로 기록하며, 재시도 성공을 미완료 로그 복구나 exactly-once로 해석하지 않는다. 반환된 OK 전부를 생존 두 KV의 실제 값과 대조한다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새-load-결과폴더> --load-crash
python -m unittest discover -s experiments/a1-active-passive-failover/real-server -p test_pb_load.py -v
python experiments/a1-active-passive-failover/real-server/analyze_load.py <load-결과폴더>
```

값 64B/8KiB × timeout 500/1000ms × 3회이며, 결과는 [연속 쓰기 중 crash 결과](load-results.md)에 있다. 기본 실행/기존 유휴 crash 결과는 유지한다. 이후의 Backup fault 조건은 아래 별도 절에서 설명하며 crash 실행과 섞어 집계하지 않는다.

## 느린 Backup과 ACK 경로 단절

`run_faults_ec2.py`는 위 crash 실행과 분리된 세 조건을 수행한다. 최초 Primary는 종료하지 않는다.

|조건|주입|요청 deadline|기대|
|---|---|---:|---|
|BASELINE|없음|1000ms|전원 ACK 후 OK|
|SLOW_BACKUP|Backup2 전용 JVM SIGSTOP200ms 후 SIGCONT|1000ms|일시정지 후 ACK를 받고 OK|
|ACK_BLOCK|Backup2의 OUTPUT 중 Primary private IP:7001 TCP만800ms DROP|200ms|TIMEOUT, 복구 후 동일 SET 재시도 OK|

PC 조정기는 정상 warmup30건과 heartbeat 준비 확인 후, Backup fault controller의 FAULT_APPLIED 신호를 받고 client에 GO를 보낸다. 따라서 request가 시작되기 전에 주입을 적용한다. 정지/차단 duration은 Backup2 시계, 요청 duration은 원래 Primary 호스트의 client 시계를 사용한다. GO 전달 시간 때문에 200ms JVM 정지와 요청 지연이 정확히200ms로 같지는 않다.

임시 방화벽 규칙은 UUID comment를 붙이고 그 정확한 규칙만 finally에서 제거한다. SSH/다른 목적지/다른 서버의 규칙은 변경하지 않는다. 실제 DROP packet counter, 복구 신호와 잔여 규칙 부재, PB JVM 종료를 확인한다. SIGSTOP 대상도 pb_control.py의 전용 PID/cmdline과 node.id=2를 확인한다.

ACK_BLOCK의 첫 응답 후 Primary MISS·두 Backup 값 존재를 확인하고, 복구 후2초 관측과 같은 client 연결의 PING2회로 잘못된 추가 응답이 없는지 확인한다. 동일 SET 재시도·별도 후속 SET 이후 노드별 key2개의 실제 값과 valueVersion을 기록한다. 값 일치와 버전 일치를 혼동하지 않는다.

```powershell
python experiments/a1-active-passive-failover/real-server/run_faults_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새-fault-결과폴더>
python -m unittest discover -s experiments/a1-active-passive-failover/real-server -p 'test_pb_*.py' -v
python experiments/a1-active-passive-failover/real-server/analyze_faults.py <fault-결과폴더>
```

full은 값2 × 조건3 × 반복3 =18회, `--quick`은64B·반복1의3회다. [결과](backup-fault-results.md), [CSV](backup-fault-results/summary.csv). 일반 network partition·멤버십 변경·다른 프로토콜의 실행은 포함하지 않는다.
