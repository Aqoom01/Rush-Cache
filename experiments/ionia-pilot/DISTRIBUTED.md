# 2개 부하 서버와 별도 제어 서버의 O00 기능 pilot

최종 배치는 replica5 + load2 + controller1 = EC2 8대다. replica3 비교에서는 같은 load2/controller1을 사용하고 replica2대를 예비로 둔다. 이 문서의 도구는 **기능 pilot**이며 정식 성능 계측·모든 변형의 원격 실행을 완료한 것은 아니다.

## 재사용과 변경 범위

기존 IoniaNode, IoniaTcpTransport, O00BenchmarkClient, TraceGenerator, LatencyCollector/Histogram을 재사용한다. 공통 생성기·ClosedLoopRunner·복제 알고리즘·wire/API·의존성은 수정하지 않는다. 기존 단일 load PilotMain은 감사/ready helper만 같은 package에서 재사용하도록 열고, distributed.properties가 있는 run을 잘못 단일 load로 실행하는 경우 거절한다. 기존 단일 load CLI·정상 동작은 유지한다.

새 DistributedPilotMain은 별도 TCP 제어 경계를 가진다. 최초 버전은 logical streams=2, 전체 concurrency=2에 한정하며 stream0/1을 load0/1에 하나씩 배정한다. 고정 request/command/sequence를 바꾸지 않고 wire ID 범위와 loadSlot을 분리한다. controller는 replica가 아니며 workload 프록시도 아니다.

## 실행 계약

- 공통 prepare는 한 번만 실행한다. 기존 INPUT_SHA256SUMS에 추가로 DISTRIBUTED_SHA256SUMS가 제어 설정·manifest·base input 해시를 묶는다. 각 호스트에는 **기동 전** 같은 새 run 입력을 복사한다. 절대 경로는 달라도 된다.
- 데이터셋3,200은 load0/1 각각 LOAD500 + WARMUP100 + MEASURE1000 = 1,600요청씩 실행한다. 요청 중복/누락은 canonical record의 stream별 SHA-256과 phase/op 개수로 대조한다. 재시도는 새 요청으로 세지 않는다.
- controller가 두 load의 run/code/input/contract/slot/instance/Java21을 확인하기 전 workload를 시작하지 않는다. 서버 ready 파일도 같은 code/base input이어야 하며 replica 전원 view0/NORMAL/applied0이 필요하다.
- LOAD → 전체 load 보고/idle 확인 → replica 전원 global applied frontier → WARMUP → 같은 barrier → MEASURE → 같은 barrier → 두 client CLOSE/idle 확인 → 별도 CRUD 감사8개 → 두 load 완료 ACK → global export/COMPLETE 순서다.
- 한쪽 load의 EOF·오류 또는 전체 work deadline 초과는 실패다. 제어 socket을 닫아 다른 load의 관찰 thread가 in-flight client를 종료하도록 한다. 누락 요청을 자동 재분배하지 않는다. 같은 role/run/data는 재실행하지 않는다.
- `state/distributed.abort` 파일은 로컬 role의 종료 요청이다. 호스트별 로컬 파일이므로 원격 중단 시 각 참여 호스트에 전달해야 한다. EC2 프로세스를 자동으로 찾아 종료하는 기능은 없다.
- 로컬 wrapper는 자신이 시작한 PID·시작 시각·명령의 class/run 경로를 재검증한 프로세스만 최후의 cleanup에서 종료한다. 정상 node 종료는 기존 stop 파일 방식이며 데이터/로그 자동 삭제는 없다.

제어 protocol은 크기 제한된 properties frame이며 replication wire와 별개다. 입력 manifest에 들어가는 임시 token으로 잘못된 run의 load 연결을 거절하지만 TLS나 사용자 인증 서비스가 아니다. 신뢰된 private 실험 네트워크에서만 사용하고 token/키를 공개하지 않는다. controller 포트와 replica 포트는 달라야 한다. 방화벽·AWS·SSH 설정은 이 도구에서 변경하지 않는다.

## 데이터셋 보관

`datasets/pilot-v1-seed101/`에 기존3,200요청 dataset의 canonical 파일8개를 보존한다. build/clean과 독립적이며 파일 해시는 이전 생성물과 동일하다. `.trace`는 binary이고 메타데이터/설정은 text다. 복제 수나 load 수를 곱해 요청 수를 늘리지 않는다. 이 작은 데이터셋은 본시험의 working-set/SSD I/O 적합성·지속시간을 충족했다는 증거가 아니다.

## Windows 로컬 예행연습

```powershell
$env:JAVA_HOME='C:\path\to\jdk21'
.\gradlew.bat :cache-server:benchmarkTest --tests '*Distributed*Test' --tests '*PilotInputsTest' --tests '*RemoteO00DrainProbeTest' :cache-server:pilotZip --offline --console=plain
Expand-Archive cache-server/build/distributions/ionia-pilot.zip C:/pilot/bundle-NEW
& C:/pilot/bundle-NEW/tools/rehearse-distributed.ps1 -Bundle C:/pilot/bundle-NEW -NewOutput C:/pilot/three-NEW -Replicas 3 -Java "$env:JAVA_HOME/bin/java.exe"
& C:/pilot/bundle-NEW/tools/rehearse-distributed.ps1 -Bundle C:/pilot/bundle-NEW -NewOutput C:/pilot/five-NEW -Replicas 5 -Java "$env:JAVA_HOME/bin/java.exe"
python C:/pilot/bundle-NEW/tools/verify_distributed.py C:/pilot/bundle-NEW C:/pilot/five-NEW/run --local-stopped
```

PowerShell 7과 Java 21을 사용한다. 기본은 72요청 smoke profile이다. 3,200요청은 `-Dataset C:/pilot/bundle-NEW/tools/datasets/pilot-v1-seed101`을 지정한다. 한쪽 중단 음성 대조는 `-Fault StopLoadOne`, 한쪽 미기동은 `-Fault MissingLoadOne`을 지정해 controller 실패·다른 load 중단·global COMPLETE 없음을 확인한다. 미기동 대기는 profile의 barrier timeout이 적용된다. 큰 profile 사용도 명시한다.

Windows에서도 원본의 RPC마다 새 socket을 여는 정책을 유지한다. 큰 실행/연속 실행은 TIME_WAIT와 동적 포트 고갈로 BindException이 날 수 있다. 로컬 wrapper는 기존 Windows TCP 여유 확인기를 그대로 재사용한 tools/Wait-LocalTcpBudget.ps1을 호출해 시작 전 관측 동적 포트 점유가 10% 이하인지 확인한다(최대60초, 표본 보존). 이 확인은 실행 중 고갈 방지를 보장하지 않는다. OS 포트 범위·TIME_WAIT·timeout을 바꿔 통과시키지 않는다. 실패를 보존하고 3,200요청 완주와 작은 smoke 통과를 구분한다. Linux/EC2 실행은 별도로 확인한다.

## Linux/EC2 실행 순서

Java 21을 사전 확인하고 동일 ZIP을 C01/L01/L02/R01–R0N에 푼다. 아래 B/R/DS는 호스트별 절대 경로, TOPOLOGY는 실제 replica private IP를 적은 properties, CTRL은 실제 C01 private IP와 별도 포트다. SSH/AWS 접속·인스턴스 설정은 별도 작업이며 이 코드에서 수행하지 않는다.

```bash
# C01: prepare는 한 번만. DS는 고정 dataset, R은 존재하지 않는 새 경로.
bash "$B/tools/distributed.sh" prepare "$TOPOLOGY" "$DS" "$R" "$CTRL"
bash "$B/tools/distributed.sh" verify "$B" "$R"

# 같은 미사용 R 입력을 모든 replica/load로 복사한 뒤 replica마다 자기 N을 기동.
bash "$B/tools/node.sh" start "$R" "$N"

# replica의 state/node-N.ready.properties를 C01의 R/ready에 모은 뒤 C01에서 실행.
bash "$B/tools/distributed.sh" controller "$B" "$R"

# controller.ready 확인 뒤 별도 L01/L02 호스트에서 각각 하나씩 실행.
bash "$B/tools/distributed.sh" load "$B" "$R" 0
bash "$B/tools/distributed.sh" load "$B" "$R" 1

# 성공·실패 모두 각 replica에서 stop. 호스트별 로그/결과도 수집.
bash "$B/tools/node.sh" stop "$R" "$N"

# C01에서 global 결과 독립 검사. 로컬 재현에만 --local-stopped를 붙임.
python "$B/tools/verify_distributed.py" "$B" "$R"
```

controller 명령은 foreground에서 실행된다. controller를 켜 둔 상태로 다른 터미널/호스트에서 load를 기동한다. `state/controller.ready.properties`는 bind 완료 표시이며 모든 load의 준비 성공이 아니다. 접속 대기는 profile의 barrier.timeout.ms로 제한한다. 실제 EC2의 디렉터리 권한·private 통신·프로세스 관리는 미검증이다. node 준비와 정상 종료 증거도 반드시 별도로 수집한다.

## 결과와 측정의 의미

load별 phase 결과·결과 JSON·LOAD_0/1_COMPLETE와 함께 C01에는 distributed-phase JSON 3개, collected-load JSON 2개, distributed-summary.json, DISTRIBUTED_RESULT_SHA256SUMS와 DISTRIBUTED_COMPLETE를 저장한다. 실패 시 DISTRIBUTED_FAILED/LOAD_N_FAILED 및 workload 실패의 phase/index/status/attempts/제한된 RPC 오류를 남긴다. 미완료를 complete로 숨기지 않는다.

히스토그램은 동일 phase/op/kind/bin의 bucket count와 sum/min/max/overflow를 합쳐 quantile을 다시 계산한다. p99를 평균하지 않는다. 시간은 각 load JVM의 monotonic duration으로 측정하고 JVM 간 nanoTime 차이를 사용하지 않는다. p99.9 비교 적합성은 기존 10만 표본 기준을 따른다.

controller_completion_window_ops_per_second는 controller의 dispatch 시작부터 두 검증된 report 수신까지 같은 clock의 구간으로 계산한다. 제어 통신·보고 처리 비용을 포함한 기능 관찰이며, 정식 throughput이나 엄밀한 동시 시작을 주장하지 않는다. 정식 고정률 부하·clock/overhead 평가·전체 지표 통합은 후속이다.

독립 Python 검사기는 생성된 canonical trace에서 partition hash/요청 수를 계산하고 전체 load/phase 보고·global frontier·전체 bucket 합계와 quantile·파일 hash/완료 marker·CRUD 증거를 대조한다. 로컬 검사에서는 서로 다른 JVM의 PID와 node 정상 종료도 확인한다.

원본의 독립 Replica RAM Durability Log, stable View 저장, 선택적 KV force(off)를 구분한다. remote CPU/SSD/fsync·전체 peer transport drain·전수 이력 선형화·전원 손실·전체 변형의 분산 pilot을 이 결과만으로 완료 처리하지 않는다. Jira·EC2/SSH·Git commit/push/merge는 실행하지 않는다.
