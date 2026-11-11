# 한 번의 실행으로 끝내는 Raft + Failure Detector 실험

실험 본체는 `run_raft_campaign.py`이며 준비 → 리더 탐색 → 정상 부하 → 장애 주입 → 복원 → 서비스 회복 → 데이터/안전성 감사 → 결과 수집 → 다음 조건을 수행한다. 정식 EC2 진입점은 `raft_ec2_campaign.py prepare/start/status/collect`로, 본체를 EC2의 독립 서비스에서 실행한다. [노트북 분리 실행 안내](raft-ec2-coordinator.md)를 따른다.

## 기본 실험 행렬

비교 대상은 기본 Raft / Raft+SWIM / Raft+SWIM+Lifeguard다. 공통 Raft timeout **500ms**, heartbeat **50ms**, tick **10ms**를 고정한다. SWIM 두 방식은 probe **200ms**, direct/indirect **50ms**, 기본 suspicion **500ms**를 공유하고 Lifeguard의 적응 기능만 켜고 끈다. 외부 FD 구성은 DEAD+heartbeat 무응답 조건과 `[10T,11T)` watchdog을 쓰므로 같은 숫자가 동일한 실효 감지 시간을 뜻하지 않는다. timeout 최적화 실험은 수행하지 않는다.

여기서 고정하는 것은 **기준값 T=500ms**다. 기본 Raft 및 bootstrap/candidate retry의 실제 선거 대기는 `[T,2T)`에서 seed 기반으로 선택되며, Raft의 randomized election timer 자체를 제거하지 않는다.

- `--study validation`(기본): 아래 **14조건 × 3방식 × 3회 = 126회**. `--repeat 1`은 42회 로컬 통합 검증이다.
- `--study presentation`: 사전 지정 핵심 6조건을 방식별 **10회**, 나머지 8조건을 방식별 **3회**, 총 **252회** 실행한다. 핵심은 `healthy`, `crash`, `cpu-observer`, `cpu-contention-observer`, `delay-udp`, `delay-both`다. `--core-repeat`과 `--repeat`로 횟수를 명시할 수 있으며 결과에 맞춰 사후 선택하지 않는다.
- 각 조건의 detector 순서는 반복마다 ABC/BCA/CAB로 순환한다. 같은 반복은 같은 election seed를 사용한다. 실제 초기 리더/대상은 원본 결과에 남기며 같은 seed가 같은 물리 리더 배치를 보장하지는 않는다.
- 매 실행에 새 JVM·설정·저장소·디렉터리를 사용한다. detector를 사용하지 않는 기본 Raft도 동일한 startup settling 시간을 기다린다.

|조건|대상과 주입|확인할 것|
|---|---|---|
|healthy|8초간 정상 부하|정상 지연·오탐·통신 비용|
|crash|실제 리더 JVM SIGKILL|재선출·전체 복구 시간·기존 데이터|
|loss-udp|리더 SWIM 송신 경로 30% 손실|TCP heartbeat가 정상일 때 오탐/불필요한 선거|
|loss-both|리더의 SWIM·Raft 송신 경로 30% 손실|탐지·로그 전송·클라이언트 복구|
|delay-udp|리더 SWIM 송신 250ms ±50ms 지연|느린 ACK에 대한 오탐|
|delay-both|리더 SWIM·Raft 송신에 같은 지연|느린 복제·선거·요청 지연|
|cpu-leader|리더 JVM 2초 STOP/CONT|일시 정지와 복귀, 재선출 후 일관성|
|cpu-observer|follower JVM 2초 STOP/CONT|로컬 detector 지연·awareness·정상 리더 오탐|
|cpu-contention-observer|follower JVM과 busy worker 8개를 같은 CPU에 배치해 8초 경합|실제 CPU 소비·awareness·정상 리더 오탐·선거·요청 영향|
|ssd-stall-leader|NVMe KV 리더의 I/O 작업 완료를 2초 지연|KV 지연과 FD/요청 복구의 관계|
|ssd-stall-observer|NVMe KV follower의 I/O 작업 완료를 2초 지연|느린 복제본·quorum·오탐|
|ssd-contention|NVMe 저장소와 같은 파일시스템에서 별도 파일 I/O 부하|실제 I/O 작업 수·저장소 지연·가용성|
|partition|리더와 두 follower 사이의 복제/FD 통신 양방향 차단 후 복원|다수 측 선출·소수 측 성공 금지·복원 후 값 수렴|
|inflight-crash|첫 작업 요청 송신 후 응답을 읽기 전에 리더 종료|불확실한 작업의 같은 identity 재시도·성공 쓰기 보존|

`inflight-crash`는 클라이언트 요청이 완료되지 않은 시점을 보장한다. 특정 Raft 로그/commit 단계에서 죽었다는 보장은 하지 않는다.

I/O stall은 **실제 NVMe KV를 소유한 worker에 주입하는 결정론적 소프트웨어 지연**이다. 하드웨어 SSD가 물리적으로 멈췄다고 주장하지 않는다. `ssd-contention`은 별도 fio 부하이며 단순 지연 주입과 구분한다. 저장소가 느려도 독립된 TCP/UDP plane이 정상이라면 DEAD가 발생하지 않을 수 있으며, 이것도 유효한 결과다. 재시작 durability는 이 campaign의 범위가 아니다.

CPU 경합은 검증된 대상 JVM의 thread affinity만 제한하고 원래 mask를 복원한다. 새로 생긴 thread도 복원하며 worker는 종료 시 정리하고 자체 실행 시간도 제한한다. worker `/proc` CPU tick을 확인해 실제 부하를 증명한다. 로컬에서는 다른 JVM과 호스트를 공유하므로 EC2의 노드별 독립 부하와 동일한 성능 증거가 아니다. STOP/CONT와 CPU 경합은 구분해서 보고한다.

## 세 가지 선택 근거

1. **외부 FD 결합:** `crash`의 장애→선거 시작 및 장애→첫 성공 SET 분포와 살아 있는 원래 리더에 대한 선거 시작/교체를 함께 비교한다. FD 탐지가 없거나 watchdog으로 선거가 시작되면 그 사실을 그대로 남긴다.
2. **Lifeguard 추가:** 관측자 pause/CPU 경합과 UDP/복제 지연에서 원래 정상 리더에 대한 DEAD, candidate 시작, 실제 리더 변경, 요청 성공률, 관측 awareness/실효 probe 간격을 함께 본다. TCP heartbeat가 정상 리더의 선거를 억제할 수도 있으므로 FD 오탐 감소와 리더 교체 감소를 동일시하지 않는다. 관측 최대 awareness는 polling의 하한적 증거이며 짧은 peak를 놓칠 수 있다.
3. **평상시 비용:** presentation의 `healthy`는 기존 30 SET 초기 준비 후 **10초 추가 warmup → 60초 측정**, 단일 client **50 SET/s**를 사용한다. 처리 지연 시 밀린 요청을 burst로 쏟지 않고 missed slot, 실제 성공률/초, 실패 응답을 기록한다. 요청 지연 p50/p99, Raft·FD의 **초당 application bytes와 성공 SET당 bytes**를 함께 보고한다. 최대 처리량 벤치마크가 아니다. 성공 표본 1000개 미만인 실행은 p99 해석에 경고를 붙인다.

모든 실행의 candidate 시작과 실제 리더 획득은 Raft owner가 발행한 bounded journal에서 수집한다. 같은 노드가 새 term 리더가 된 경우는 **재당선**, 다른 노드로 바뀐 경우는 **리더 identity 변경**이다. polling 사이에 발생한 사건도 journal로 복원하며 overflow는 실패로 처리한다. replication 지연으로 quorum 연락이 끊긴 살아 있는 리더의 교체가 반드시 불필요한 것은 아니다.

## 로컬 검증: 실제 JVM + 통신 장애 모델

```bash
./gradlew :cache-core:test :cache-server:test :cache-server:installDist :cache-server:distTar
python3 -m unittest discover -s experiments/a1-active-passive-failover/real-server -p 'test_raft*.py' -v
python3 experiments/a1-active-passive-failover/real-server/run_raft_campaign.py \
  --local --lib cache-server/build/install/openkache-server/lib \
  --out /tmp/raft-campaign-new --repeat 1
```

로컬에서는 다음 구현을 사용한다.

- JVM 3개를 실제로 시작하고 리더 crash와 CPU STOP/CONT는 OS signal로 수행한다.
- 통신 장애는 실험 전용 transport의 비동기 메시지 drop/delay와 수신 거절로 재현한다. 커널 packet loss나 TCP 재전송 측정과는 구분한다.
- NVMe 저장소 구현을 로컬 파일에서 실행하고, I/O 지연과 별도 파일의 동기 쓰기 부하를 검증한다. EC2 NVMe 성능 결과로 해석하지 않는다.

로컬 실행은 호스트 전체의 qdisc/iptables를 변경하지 않는다. 정상·네트워크·CPU·I/O·partition·쓰기 중 장애를 같은 controller에서 검증할 수 있고, 실제 EC2 성능 결과와 섞지 않는다.

## 준비된 EC2 3대에서 전체 실행

EC2를 생성·기동·삭제하지 않으며 정식 서비스는 성공·S3 보관 검증 후 3대를 자동 정지한다. **현재 사용자 승인 범위는 구현·로컬 검증이며 실제 EC2 실행 전 별도 안내가 필요하다.** 같은 VPC·AZ에서 이미 실행 중인 전용 m6gd.medium 3대, SSM, Java21+, Python3, AWS CLI, iproute2/tc, iptables, fio가 필요하다. 사설망 TCP7000/7001·UDP7002를 허용해야 한다. CPU 경합은 affinity 권한, network fault는 root/CAP_NET_ADMIN이 필요하다. manifest에 노드 AZ/사설 IP, uname/lscpu/Java/Python 환경과 실행 controller 환경을 남긴다.

정식 EC2 경로는 [분리된 coordinator 서비스](raft-ec2-coordinator.md)다. 실험용 3대 중 한 대가 전체 순서를 제어하므로 노트북은 패키지 업로드·시작 요청이 끝난 뒤 꺼도 된다. coordinator EC2의 Python에 그래프 의존성을 설치해야 한다. 시스템 Python 대신 별도 venv 사용을 권장한다.

```bash
python3 -m pip install -r experiments/a1-active-passive-failover/real-server/requirements-raft-plots.txt
```

SSD 조건은 `--data-root`가 이미 마운트된 **Amazon EC2 NVMe Instance Storage** 위에 있어야 한다. 확인에 실패하면 전체 실행 전에 중단하며 임의 디스크 포맷은 하지 않는다. 각 case의 새 디렉터리만 만들고, 기존 NVMe KV 파일이 있으면 덮어쓰지 않는다.

```bash
python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py prepare \
  --study presentation --account-id <AWS_ACCOUNT_ID> \
  --instances <node0-id>,<node1-id>,<node2-id> \
  --bucket <기존-결과-버킷> --data-root /mnt/openkache-data \
  --dist cache-server/build/distributions/openkache-server-0.1.0-SNAPSHOT.tar \
  --remote-python /opt/openkache-python/bin/python --out /tmp/raft-detached-job

python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py start \
  --job /tmp/raft-detached-job --profile openkache
```

WSL에서 Windows AWS CLI를 사용할 경우 `--aws-bin '/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe'`를 추가한다. 로컬 프로필과 노드 IAM role에 해당 S3 경로와 SSM 권한이 필요하다.

coordinator의 실행·그래프용 Python은 `prepare --remote-python`으로 고정한다. 로컬 그래프 검증용 `/tmp/openkache-raft-presentation-venv/bin/python`과 EC2의 Python은 별개다. `/tmp` 환경과 원본은 장기 보관 대상이 아니다.

전체 제어는 systemd가 소유한 EC2 coordinator 프로세스에서 진행한다. 진행 상태·체크포인트·최종 보고서는 S3에 보관하며 노트북에서 `status`와 `collect`로 조회·복사한다. 성공 시 archive를 확인한 뒤 보조 2대를 정지하고 coordinator를 마지막으로 정지한다. 실패 시 자동 정지하지 않는다. `prepare --keep-running`으로 성공 후 정지를 비활성화할 수 있다. 이전 터미널 기반 직접 실행은 디버깅용 `run_raft_campaign.py --foreground`로만 허용하며 이 경로에는 자동 정지를 적용하지 않는다. coordinator 시간 제한은 실패 시까지 포함한 EC2 과금 상한이 아니다.

과금 자원 없이 발표용 계획만 확정하려면 다음 명령을 사용한다. AWS를 호출하거나 JVM을 시작하지 않는다.

```bash
python3 experiments/a1-active-passive-failover/real-server/run_raft_campaign.py \
  --local --lib cache-server/build/install/openkache-server/lib \
  --study presentation --plan-only --out /tmp/raft-presentation-plan-new
```

252회와 정상 60초 측정으로 확장했으므로 기존 117회 기준의 2~3시간 추정은 사용하지 않는다. EC2 orchestration 시간은 미검증이며 준비·재실행을 포함해 우선 **4~7시간**을 예산 가정으로 둔다. 앞서 확인한 서울 m6gd.medium 3대 및 IPv4/root EBS 요금, 환율 1500원/$·VAT 10% 가정에서는 약 **1,300~2,200원 + 소량의 S3/요청/전송 비용** 수준이다. 여유 예산은 3천~5천원으로 잡고 EC2 준비 전 최신 요금·현재 자원·시간 계획을 다시 확인한다. 시간·비용 상한 보장은 아니다. 종료/볼륨 정리는 별도이며 정지만 한 디스크에는 비용이 남는다.

EC2에서는 network fault를 tc netem/iptables로 주입하고 client·SSM 포트는 매칭하지 않는다. 기존에 관리자가 설치한 root qdisc가 있으면 덮어쓰지 않는다. CPU 정지는 finally에서 CONT로 복구하며, I/O·통신 fault는 최대 시간과 자동 복원을 둔다.

## 재개와 결과

- `manifest.json`: 고정 설정, 사전 지정 핵심 조건·반복 계획, 바이너리·스크립트 SHA, backend와 장애 모델.
- `summary.json`: passed/failed/pending과 전체 완료 여부. 실패는 성공으로 집계하지 않는다.
- 각 case의 `result.json`: 요청 시도, 성공 응답한 key, 노드 상태·FD 이벤트, fault ground truth, clock 보정 구간, 데이터 보존 감사.
- `runs.csv`, `conditions.csv`, `report.md`: 실행·조건별 결과와 실패·미실행 목록. 누락을 0으로 바꾸지 않는다.
- 기본 Raft에는 SWIM 감지기가 없으므로 DEAD 오탐·awareness는 해당 없음(N/A)이다. 선거·리더 교체·FD 추가 통신량 비교와 구분한다.
- `paired-comparisons.csv`, `comparison-summary.csv`: 같은 조건·repeat seed의 인접 비교군 차이(right minus left). 완전한 쌍의 수를 표시하고 5쌍 이상에서 paired-run bootstrap median의 탐색적 95% 구간을 계산한다. 다중 비교 보정은 하지 않았으며 인과·유의성의 확증으로 해석하지 않는다. 요청 개별 표본을 독립 실행 횟수로 취급하지 않는다.
- presentation에서는 `figures/recovery`, `stability`, `healthy-cost`의 PNG/SVG와 안내를 생성한다. 개별 실행 점·중앙값·실측/예정 수와 실패 표시를 포함하며 로컬 그림에는 LOCAL IMPLEMENTATION VALIDATION을 표기한다. 결과를 보고 자동으로 승자를 선택하지 않는다.
- `analysis-provenance.json`, `figures/provenance.json`: 보고서를 다시 분석하더라도 측정 당시 manifest와 후속 분석/plot 코드·입력 CSV의 SHA 및 Matplotlib 버전을 구분해 남긴다.
- 실패한 시도는 `attempt-N`에 보존한다. 정식 EC2 서비스는 `raft_ec2_campaign.py resume --job <패키지폴더>`를 사용한다. 성공한 조건은 건너뛰고 정상 정리가 확인된 실패/미완료 조건을 재실행한다. 바이너리·스크립트·행렬 변경 또는 중단된 활성 case의 정리 미확인 상태에서는 재개하지 않는다. 직접 로컬 실행에는 기존 `--resume`을 사용한다.
- 분석만 다시 하려면 `python3 experiments/a1-active-passive-failover/real-server/summarize_raft_campaign.py <결과폴더>`를 실행한다.

`falseDeadPerLiveNodeMinute`는 살아 있는 노드의 관측 시간당 DEAD 이벤트 수다. CPU 정지 중인 노드·crash·partition을 정상 노드 오탐으로 집계하지 않는다. 탐지가 없으면 `fdDetectionObserved=false`와 빈 `detections`를 남긴다. 탐지/회복의 clock RTT 구간은 상대 시계 drift까지 보장하는 구간이 아니다.

`raftJournal`, `raftElections`, `firstRaftElectionLatencyBoundsNs`, `fallbackElections`는 기본 Raft 타이머와 외부 FD 신호를 구분한다. 사건은 normal/pre-fault, definite post-fault, clock-boundary uncertain으로 분류한다. 부하 중에는 `faultWindowAttemptSuccessRate`와 성공 응답한 key 이력을 남긴다. S3 장애 기록 polling은 별도 thread에서 수행해 측정 요청의 재시도를 막지 않는다. validation 및 presentation의 비정상 조건은 짧은 정상 32 SET을 기록하지만 정상 비용의 주 근거는 충분한 표본을 확보한 presentation `healthy`다.

EC2에서는 실행별 원본 `result.json`, `fault-events.json`, 노드 로그를 `s3://<bucket>/<campaign>/<case>-attempt-N/`에 저장한다. 로컬에는 원본 결과를 내려받고, 전체 manifest/진행 요약도 S3에 복사한다. 실행 종료 시 CSV·보고서·PNG/SVG를 같은 campaign prefix에 추가 보관한다. **S3 원본을 삭제하는 코드는 없다.** 로컬 파일 수집과 S3 보관은 별개다.

집계 결과 업로드가 모두 성공하면 마지막에 `archive-status.json`을 올린다. 실험 pass/fail과 아카이브 업로드 성공은 구분한다. 업로드 실패는 로컬 상태에 남기고 오류로 종료한다. 완료된 EC2 campaign에 동일한 `--resume`을 실행하면 JVM을 재시작하거나 SSM 배포를 하지 않고 보고서를 재생성·재업로드한다.

각 계획의 모든 case가 passed일 때만 `complete=true`다. 252회도 모든 환경에 대한 보장이나 장기 무오탐 증명은 아니다. 로컬 42회는 구현 검증이며 최종 EC2 비교 결과를 대신하지 않는다.
