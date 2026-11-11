# Raft + SWIM / Lifeguard 결합 실험

전체 장애 조건을 한 번에 실행하려면 [전체 campaign](raft-full-campaign.md)을 사용한다. 아래 단일 crash 경로와 별도로 13개 시나리오의 주입·복원·감사·재개를 제공한다.

`protocol=RAFT`, `failure.detector=FIXED_TIMEOUT|SWIM|SWIM_LIFEGUARD`를 선택한다. Raft의 term·로그 최신성 검사·RequestVote·과반수 commit을 유지한다. FD가 직접 리더를 지정하거나 membership을 제거하지 않는다.

## 선거 정책

T는 `timeout.ms`다. 기본 Raft는 마지막 리더 Append 이후 `[T,2T)` 타이머로 선거를 시작한다. SWIM 계열은 현재 리더가 알려진 follower에 다음 정책을 적용한다.

- SUSPECT만으로 선거하지 않는다.
- 현재 리더·현재 term에 해당하는 DEAD와 최소 T의 Raft heartbeat 단절을 함께 확인하면 정상 Raft 선거를 시작한다.
- 유효한 현재/상위 term Append는 이전 FD 신호를 무효화한다. UDP만 손실되고 TCP heartbeat가 정상일 때 불필요한 선거를 억제한다.
- DEAD 누락에 대비한 `[10T,11T)` watchdog을 유지한다. 초기 리더 선출과 candidate 재시도는 원래 `[T,2T)` 타이머를 쓴다.
- 리더는 Node0에 고정하지 않고 동적으로 추적한다. candidate 재시도와 quorum 검사는 항상 Raft가 수행한다.

기본 Raft와 결합 방식의 선거 정책이 다르므로 복구 속도 개선을 전제하지 않는다. 탐지 지연·오탐·전체 복구의 trade-off를 측정하고 fallback은 SWIM 탐지 성공과 구분한다.

## 설정

노드별 `node.id=0|1|2`, `peer.0`~`peer.2` 사설 IP, client/peer 포트를 지정하고 다음 설정을 추가한다.

```properties
protocol=RAFT
failure.detector=SWIM_LIFEGUARD
heartbeat.ms=50
timeout.ms=500
tick.ms=10
election.seed=1
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

`SWIM`은 같은 UDP 경로에서 Lifeguard 보정만 끈다. `FIXED_TIMEOUT`은 UDP detector를 실행하지 않는다. 이 설정 이름과 달리 기본 Raft의 실제 선거 시간은 랜덤 범위다.

## 로컬 검증

```bash
./gradlew :cache-core:test :cache-server:test :cache-server:installDist :cache-server:distTar
python3 experiments/a1-active-passive-failover/real-server/local_raft_fd_validation.py \
  --lib cache-server/build/install/openkache-server/lib \
  --out /tmp/raft-fd-new-run --repeat 2
python3 experiments/a1-active-passive-failover/real-server/analyze_raft_fd.py /tmp/raft-fd-new-run
```

detector별 별도 JVM 3개를 시작한다. 실제 선출 리더와 startup grace 종료를 확인하고, 정상 SET과 생존 두 노드의 값을 검증한다. 리더만 SIGKILL하고 같은 ClientId/OperationId로 재시도한다. 새 term·기존 데이터 보존·stale epoch·follower 직접 쓰기 거절을 검사한다. SWIM smoke가 fallback으로만 복구되면 실패 처리한다.

## EC2 실행

SSM 경로는 기존 실행 중인 m6gd.medium 전용 EC2 3대와 결과 S3 버킷을 사용한다. 인스턴스 생성·기동·정지·삭제는 수행하지 않는다. 사설망 TCP7000/7001·UDP7002, Java21+, Python3, AWS CLI, Online SSM agent가 필요하다. 노드 IAM role은 해당 S3 경로를 읽고 쓸 수 있어야 한다.

```bash
python3 experiments/a1-active-passive-failover/real-server/run_raft_fd_ssm.py \
  --profile <유효한-AWS-프로필> \
  --instances <node0-instance-id>,<node1-instance-id>,<node2-instance-id> \
  --bucket <기존-결과-버킷> \
  --dist cache-server/build/distributions/openkache-server-0.1.0-SNAPSHOT.tar \
  --out /tmp/raft-fd-ec2-new-run --repeat 3
```

Windows AWS CLI를 WSL에서 쓸 때 `--aws-bin '/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe'`를 지정한다. 기본 프로필이면 `--profile`을 생략한다. 프로필명과 IAM 사용자명은 별개다.

기본 campaign은 detector 3종 × 3회, 값64B/T500ms다. `--value 8192 --timeout 1000`으로 조건을 바꾸고 output 디렉터리를 분리한다. 사전 검증은 `--detectors SWIM_LIFEGUARD --repeat 1`로 실행한다. 사용 중인 포트가 있으면 사전 검사에서 중단하고, 각 반복의 새 디렉터리와 정확한 PID/config 경로로 자신의 JVM만 관리한다. SSM 기동 지연을 위해 startup grace는 15초다. 배포본·script SHA-256, 실제 인스턴스/IP, 원본 결과와 JVM 로그를 남긴다.

SSH가 가능한 기존 클러스터는 `run_election_ec2.py --protocols RAFT --detector SWIM_LIFEGUARD`와 기존 hosts/key/known-hosts/dist/out 인자를 사용한다. 다른 detector는 별도 output에 실행한다.

## 측정과 범위

- `failoverNs`: 리더 호스트 controller의 SIGKILL 호출 직전 → 새 리더 첫 SET OK.
- `promotionNs`: 승리한 노드의 최근 선거 시작 → 현재 term 로그 적용 준비 완료. 실패한 선행 선거는 전체 failover에 포함된다.
- `deadToElectionNs`: 같은 JVM의 DEAD → 최근 선거 시작. 재시도면 그 대기까지 포함한다.
- `fdDetectionLatencyBoundsNs`: 장애 전 status 왕복으로 만든 clock offset 구간에 DEAD를 매핑한 추정치다. 서로 다른 JVM nanoTime을 직접 빼지 않으며, 상대 clock drift까지 보장하는 구간은 아니다.
- `raftElectionOrigin`: BOOTSTRAP/TIMEOUT/SWIM_DEAD/FALLBACK. `raftElectionTrigger`는 최근 시도로, 재선거이면 RETRY다.
- 정상 p50/p99, replication TCP 바이트, FD UDP 바이트를 별도로 기록한다.

`analyze_raft_fd.py`는 새 Raft 실행만 검증·집계하고 fallback 횟수를 보존한다. 기존 36회 PB/VR/Raft 전용 `analyze_election.py`와 혼합하지 않는다.

현재 자동 campaign은 **정상 쓰기가 두 follower에 반영된 후의 리더 process crash** 범위다. loss/delay/CPU/SSD stall 전체 A3 실험을 완료한 것은 아니다. 메모리 KV를 사용하며 term/vote/log 영속화, 프로세스 restart, snapshot, membership 변경, Lifeguard Buddy System은 지원하지 않는다.
