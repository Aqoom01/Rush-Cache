# A1 최종 보강 실험

이 문서는 실험 계획과 재현 방법이다. 결과는 EC2에서 실행한 뒤 별도 기록한다.
기존 216회 결과와 새 cohort는 합산하지 않는다. 개인 발표 자료가 아니다.

## 범위와 완료 조건

| 그룹 | 조건 | 정식 실행 수 | 통과/관측 기준 |
|---|---|---:|---|
| crash | PB / VR INCREMENTAL / VR CATCHUP / Raft × 64B/8KiB × 5회 | 40 | client sendall 직후 리더 crash, 새 epoch 생존 리더의 첫 OK, 이전 성공값과 재시도 완료값을 두 생존자에서 전부 확인 |
| write | 위 4방식 × 2크기 × 동시 client 1/4/8 × 5회 | 120 | 값 보존, 논리 요청 지연(재시도 포함), OK/s, BUSY·TIMEOUT·통신오류 횟수, 노드간 전송량 |
| read | Chain tail 직접 / Chain 분산진입 후 redirect / CRAQ 분산진입 × 2크기 × client 1/4/8 × 5회 | 90 | 30초 clean read, 값 일치, reader 전원 참여, 처리량·p50/p95/p99·redirect 수 |

총 **250회**, 예비는 18회(정식과 분리). 고정 seed로 반복 블록 안의 순서를 섞는다.
timeout 기준 500ms, heartbeat 50ms, tick 10ms. Raft 실제 선거 timeout은 [500,1000)ms.
각 run의 실행 설정/순서/seed/원본 표본을 저장한다. 순서 섞기는 환경 잡음을 제거한다는 뜻이 아니다.

## 중요한 제약

- 코어 프로토콜/공통 API/메시지 한도를 변경하지 않는다. 연구 종료를 위한 검증 범위다.
- retained log의 checkpoint/분할 전송이 없고 wire frame은 2MiB 한도다.
  쓰기는 64B에서 1,024건, 8KiB에서 128건으로 구간을 제한한다(별도 warmup SET 16건).
  매 run 새 JVM으로 시작하며, 시간 기반 장시간 steady-state write benchmark라고 부르지 않는다.
- 기존 128건·3회에 비해 64B 표본과 동시성/반복을 확대한다. 8KiB의 실행별 p99 표본은 여전히 128건이므로 한계가 남는다.
- warmup: SET 16건 뒤 5초 local read. read 그룹은 추가로 같은 reader 부하로 5초 예열한다.
  쓰기 JIT가 충분히 안정화됐다는 보장은 없으므로 cold/fresh-process 조건을 명시한다.
- 쓰기 지연은 BUSY 등의 재시도 대기를 포함한 논리 요청 기준이다. 재시도 간격 10ms, 요청 deadline 1초, 논리 요청 예산 15초.
- client별 ID와 operation ID를 분리한다. 재시도에는 같은 ID/key/value를 사용한다.
- crash는 클라이언트 송신 완료 후 응답을 읽기 전에 종료한다. 서버 내부의 commit/ACK 경계에 정확히 장애를 강제한 실험이 아니다.
- 성공한 고유 key를 검증한다. 불확실했던 요청의 재시도 성공을 이전 성공값 보존으로 둔갑시키지 않는다.
- clean CRAQ는 읽기 확장만 검증하며 기존 mixed 부하 및 전체 이력 선형화가능성 검사를 대체하지 않는다.
- write/read 발생기는 node2 서버와 동거한다. crash controller는 최초 리더 호스트에 둬 같은 시계로 종료/응답을 잰다.
  1 vCPU 발생기/서버 경합 때문에 분리된 전용 발생기 기준의 최대 성능이라고 해석하지 않는다.
- p99는 실행별 nearest-rank 통계, 최종 비교는 반복의 중앙값/min/max다. 신뢰구간이나 운영 p99로 부르지 않는다.
- Chain 위치별 crash 및 Rosé lag 실험은 기존 결과를 유지한다. 영속화/NVMe/분단/서비스 통합은 추가하지 않는다.

## 서버 준비

기존 A1 전용 서버 세 대가 중지 상태로 남아 있으면 같은 세 대를 재시작해도 된다.
기존 cohort와 하드웨어 조건을 유지하려면 Ubuntu 24.04 ARM64 / m6gd.medium 세 대를 사용한다.
같은 VPC/서브넷, 각 노드에 별도 프로세스. 루트 EBS 20GiB면 이 실험용 코드·로그에 충분한지 `df -h`로 점검한다.
NVMe SSD 성능을 측정하는 실험이 아니므로 디스크 포맷/마운트를 하지 않는다.

- Java 21, Python 3, tar, SSH 필요. 기존 서버는 설치 상태부터 확인한다.
- 세 대에 같은 A1 전용 보안 그룹 적용: SSH 22는 접속 PC IP, TCP7000-7001은 그 보안 그룹 자체만 허용.
- 부하 발생기도 서버에서 실행하므로 7000-7001을 인터넷 전체에 열 필요가 없다. S3/공유 파일시스템 불필요.
- 새 인스턴스는 SSH 호스트 키 지문을 별도로 확인해서 known_hosts에 등록한다. 실행기는 StrictHostKeyChecking=yes를 유지한다.
- 중지/재시작 시 public IP가 바뀔 수 있으므로 실제 IP로 private hosts 파일을 갱신한다. 키/호스트는 Git에 넣지 않는다.
- 팀원의 서버와 기존 실험 디렉터리는 조작하지 않는다. 포트가 사용 중이면 실행기가 실패하고 중단한다.

Ubuntu 신규 서버에서만 필요한 설치 명령:

```sh
sudo apt-get update
sudo apt-get install -y openjdk-21-jre-headless python3
java -version
python3 --version
```

## 실행 절차

아래 명령은 실서버 worktree 루트에서 실행한다. OUT은 항상 새로운 private 디렉터리다.
`--execute`가 없으면 AWS/SSH 연결 없이 계획·실행파일 사본·해시만 만든다.

```powershell
$scripts = 'experiments/a1-active-passive-failover/real-server'
$dist = 'cache-server/build/distributions/openkache-server-0.1.0-SNAPSHOT.tar'
$privateRoot = '.local/a1-final-prep'
./gradlew.bat :cache-server:distTar
python "$scripts/run_final_validation.py" --dist $dist --out "$privateRoot/prepared"
```

서버 준비 후 `public`과 `private` IPv4 필드를 가진 객체 세 개를 JSON 배열로 만들어 `$hostsFile`에 저장하고 예비부터 실행한다.

```powershell
$hostsFile = "$privateRoot/ec2-hosts.json"
$keyFile = '.local/keys/experiment-key.pem'
$knownHosts = "$privateRoot/known-hosts"
python "$scripts/run_final_validation.py" --execute --quick --hosts $hostsFile --key $keyFile --known-hosts $knownHosts --dist $dist --out "$privateRoot/ec2-quick"
python "$scripts/analyze_final_validation.py" --source "$privateRoot/ec2-quick" --out "$privateRoot/ec2-quick-analysis"
```

예비 통과 후 정식은 우선순위 순서로 세 묶음을 별도 저장한다. 중간에 실패하면 이후 묶음 진행 전 원인을 해결한다.

```powershell
python "$scripts/run_final_validation.py" --execute --group crash --hosts $hostsFile --key $keyFile --known-hosts $knownHosts --dist $dist --out "$privateRoot/ec2-crash"
python "$scripts/analyze_final_validation.py" --source "$privateRoot/ec2-crash" --out "$privateRoot/ec2-crash-analysis"
Start-Sleep -Seconds 65 # 직전 JVM의 TCP 연결 정리 대기
python "$scripts/run_final_validation.py" --execute --group write --hosts $hostsFile --key $keyFile --known-hosts $knownHosts --dist $dist --out "$privateRoot/ec2-write"
python "$scripts/analyze_final_validation.py" --source "$privateRoot/ec2-write" --out "$privateRoot/ec2-write-analysis"
Start-Sleep -Seconds 65
python "$scripts/run_final_validation.py" --execute --group read --hosts $hostsFile --key $keyFile --known-hosts $knownHosts --dist $dist --out "$privateRoot/ec2-read"
python "$scripts/analyze_final_validation.py" --source "$privateRoot/ec2-read" --out "$privateRoot/ec2-read-analysis"
```

새 run마다 원격 전용 디렉터리가 생성된다. 실행기는 PC로 각 원본/로그를 즉시 회수하고, 배포 tar와 실행 스크립트를 해시와 함께 보존한다.
실패도 원본에 ok=false로 기록하며 누락/실패/잘못된 설정/해시 불일치/정리 실패가 있으면 분석이 중단된다.
세 서버의 전용 JVM 정리는 finally에서 수행한다. EC2 자체를 자동 중지하거나 종료하지 않는다.
명령이 강제 중단돼 finally가 실행되지 않은 경우 manifest.remoteDirectory의 pb_control.py stop으로 해당 JVM만 정리한다.

## 최종 종료 체크

1. 예비 결과를 정식과 분리하고 정식 세 묶음의 manifest.complete 및 cleanup 세 노드 OK 확인.
2. hash/config/성공값 audit 검증을 거친 runs.csv와 summary.csv 생성.
3. 별도 결과 보고서에 기존 대비 조건 차이, 실패/재시도, 반복 편차를 함께 기록.
4. crash → 전환 시간/성공값 보존, write → 동시성별 처리량·지연·BUSY, read → 동시성별 처리량·지연 그래프 작성.
5. 원본/배포본/스크립트/로그 로컬 보존을 확인한 뒤 사용자가 EC2 세 대를 중지.
