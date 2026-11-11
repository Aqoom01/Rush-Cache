# 노트북과 분리된 EC2 Raft 실험

정식 EC2 실행 경로는 `raft_ec2_campaign.py prepare/start/status/collect`다. **`start`가 `service-accepted`로 반환되면 노트북을 끄거나 터미널·SSM 연결을 종료해도 전체 실험은 계속된다.** 파일 업로드와 시작 요청이 끝나기 전에는 로컬 프로세스를 유지한다. 서비스 승인과 최종 실험 성공은 별개이며 진행은 `status`로 확인한다.

현재 작업은 구현·로컬 검증만 수행했다. 실제 EC2 생성·권한 변경·서비스 시작·실험은 실행 전에 사용자에게 먼저 안내한다.

## 실행 구조

- 기존 실험용 m6gd.medium **3대 중 마지막 노드**를 coordinator로 사용한다. `--coordinator`로 같은 세 노드 중 하나를 명시할 수도 있다. 추가 EC2는 만들지 않는다.
- systemd가 Python coordinator를 별도 서비스로 실행한다. `--scope`, `--pipe`, `--pty`, `--wait`를 사용하지 않아 시작한 SSM 세션이나 노트북 프로세스에 매달리지 않는다.
- coordinator가 기존 campaign 실행기를 호출하여 3대의 SSM 명령·장애 주입·결과 수집·보고서 생성을 제어한다. JVM 하나의 종료/정지는 해당 JVM PID만 대상으로 하므로 coordinator Python을 종료하지 않는다.
- coordinator 데이터는 `/opt/openkache-raft-coordinator/<campaign>/`의 **root EBS**에 저장한다. 임시 NVMe의 KV 데이터와 구분한다. 실제 archive의 기준은 S3다.
- coordinator는 EC2의 IAM 역할을 사용한다. 노트북의 프로필·SSO 세션·키를 복사하지 않는다. 노트북 로그인 세션이 만료되어도 이미 시작한 coordinator의 인증에는 영향을 주지 않는다. [AWS 인스턴스 역할 인증 설명](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-metadata.html)
- 같은 노드의 CPU 경합 조건에서는 coordinator도 CPU를 공유할 수 있다. coordinator 위치를 고정하고 manifest의 `executionController`와 실제 초기 리더/장애 대상을 함께 기록한다. 과부하 중 제어·heartbeat 지연과 FD 성능을 동일시하지 않는다.

## 준비 — 로컬 파일 생성만 수행

동일 VPC·AZ의 전용 m6gd.medium 3대와 기존 S3 버킷이 필요하다. 세 노드에는 SSM, Java 21+, Python, AWS CLI, `tc`, `iptables`, `fio` 및 마운트된 instance NVMe가 있어야 한다. 사설망 TCP7000/7001·UDP7002를 허용한다.

coordinator에는 systemd, `systemd-run`, `flock`가 필요하다. presentation 보고서용 **Matplotlib도 coordinator의 Python에 설치**되어 있어야 한다. `/opt/.../venv/bin/python` 등 준비된 환경을 `--remote-python`으로 지정할 수 있다. 로컬에만 Matplotlib이 설치되어 있는 것으로 충분하지 않다. 런처가 의존성 누락을 확인하면 실험 시작 전 오류로 종료한다.

다음 명령은 실제 인스턴스 ID와 계정 ID로 채워 실행한다. 이 단계에는 AWS 호출이 없다.

```bash
python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py prepare \
  --out /tmp/raft-detached-job \
  --dist cache-server/build/distributions/openkache-server-0.1.0-SNAPSHOT.tar \
  --account-id <AWS_ACCOUNT_ID> --instances <node0-id>,<node1-id>,<node2-id> \
  --bucket <RESULT_BUCKET> --study presentation \
  --remote-python /opt/openkache-python/bin/python
```

생성물은 다음과 같다.

- `job.json`, `plan.json`: coordinator 위치, 고정 설정에 따른 252회 조건, 실행 시간 제한, `stopOnSuccess=true`. 기본 핵심 조건 10회/나머지 3회 계획을 유지한다.
- `source.tar`, `dist.tar`: 필요한 소스와 서버 배포물만 포함한다. `.aws`, 로컬 프로필, 전체 저장소나 테스트 fixture를 전송하지 않는다.
- `package-manifest.json`: job 설정과 아티팩트 SHA를 고정한다. 설정을 바꾸려면 새 패키지를 만든다.
- `start-script.sh`: 실제 서비스 시작에 사용할 검토 가능한 스크립트. 내용 생성만으로 원격 실행되지 않는다.
- `coordinator-policy.json`: coordinator 역할에 필요한 **검토용 IAM 정책**. 해당 campaign S3 prefix 읽기/쓰기, EC2 조회, 세 노드에 대한 SSM 실행/결과 조회와 **정확히 해당 3개 instance ARN의 StopInstances**를 포함한다. 시작·삭제·강제 정지 권한은 추가하지 않는다. 이 파일 생성은 IAM 변경을 수행하지 않는다.

일반 노드 역할에는 해당 prefix의 S3 읽기/쓰기가, coordinator 역할에는 위 정책과 SSM 관리 연결 권한이 필요하다. IAM 설정과 실제 권한 검증은 EC2 준비 단계에서 수행한다.

## 시작 — EC2 실행 안내 후 수행할 단계

```bash
python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py start \
  --job /tmp/raft-detached-job --profile openkache
```

WSL에서 Windows CLI를 쓰면 `--aws-bin '/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe'`를 추가한다. 노트북 프로필은 패키지 업로드와 짧은 시작 요청에만 사용한다.

런처는 계정·세 인스턴스·VPC/AZ·실험 태그·coordinator 역할 연결을 확인하고 S3에 패키지를 올린다. 짧은 SSM 명령은 패키지 SHA를 검증하고 root EBS에 풀어 systemd 서비스를 시작한 뒤 반환한다. `launch-receipt.json`에 service 이름·coordinator ID·S3 위치를 남긴다. 응답을 받기 전에 연결이 끊기면 무조건 다시 시작하지 말고 `status --live`로 확인한다.

활성 서비스에 같은 시작 요청을 보내면 새 서비스를 만들지 않는다. worker 잠금도 중복 실행을 막는다. 서비스는 자동 재시작하지 않는다. 기본 `--max-hours 10`은 coordinator 서비스 실행 시간 제한이며, 실패 시까지 포함한 EC2 비용 상한은 아니다. 인스턴스 생성·기동·삭제는 수행하지 않는다.

## 성공 후 EC2 자동 정지

새 패키지는 **성공 후 자동 정지가 기본값**이다. `prepare --keep-running`을 명시하면 비활성화하고 StopInstances 권한도 생성 정책에서 제외한다. 설정은 패키지에 고정되므로 변경하려면 새 패키지를 만든다.

1. 실험 시작 전에 `StopInstances --dry-run`으로 정지 권한을 확인한다. 이 확인은 실제 인스턴스를 정지시키지 않는다. 권한이 없으면 본실험을 시작하지 않는다.
2. 계획의 모든 case가 passed이고, manifest·인스턴스 목록·계획이 해당 job과 일치하는지 확인한다.
3. 최종 S3 archive marker와 manifest/summary를 다시 내려받아 로컬 원본과 SHA를 대조한다. archive에 포함된 보고서·CSV·그림은 S3 존재 여부와 크기를 확인한다.
4. 보조 노드 2대에 정상 정지를 요청하고, 실제 `stopped` 상태를 확인한다. 확인이 시간 내 끝나지 않으면 coordinator를 유지하고 오류를 남긴다.
5. 정지 의도를 S3 `control/auto-stop.json`과 최종 상태에 기록한 뒤 coordinator를 마지막으로 정지 요청한다. 자기 자신이 꺼질 수 있어 최종 `stopped` 확인은 `status --live`의 읽기 전용 EC2 조회에서 수행한다.

실험 실패·보관 실패·검증 불일치 시에는 자동 정지하지 않는다. 정지 요청 실패도 실험 결과와 별도로 기록한다. 이 기능은 강제 종료, OS shutdown 생략, 인스턴스 삭제를 사용하지 않는다. 정지 후 instance NVMe 데이터는 사라지므로 S3 결과 보관을 먼저 검증한다. EBS와 S3 데이터는 유지되며 보관 비용은 남는다. [AWS 정지 동작](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Stop_Start.html), [정지 API 및 EBS 비용](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_StopInstances.html)

## 노트북을 다시 켰을 때 조회와 수집

```bash
python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py status \
  --job /tmp/raft-detached-job --profile openkache

python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py status \
  --job /tmp/raft-detached-job --profile openkache --live

python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py collect \
  --job /tmp/raft-detached-job --profile openkache --out /tmp/raft-collected-results
```

`status`는 S3만 읽는다. `--live`는 EC2 3대의 실제 상태를 추가 조회하고, coordinator가 실행 중일 때만 SSM으로 service 상태와 시작 로그를 읽는다. 정지된 coordinator에 SSM 명령을 보내지 않는다. `autoStopObservedComplete=true`는 세 대가 실제 stopped로 조회됐다는 뜻이다. 두 조회 경로 모두 실험을 시작하지 않는다. `collect`는 S3 데이터를 로컬로 복사하며 S3 원본을 삭제하지 않는다. 로컬 다운로드는 실행·완료의 필수 조건이 아니다.

S3 구조:

```text
s3://<bucket>/<campaign>/
  control/job.json, source.tar, dist.tar
  control/status.json, coordinator.log, auto-stop.json
  checkpoints/<case>/result.json, outcome.json
  <case>-attempt-N/result.json, fault-events.json, node-0.log ...
  manifest.json, summary.json, runs.csv, conditions.csv
  paired-comparisons.csv, comparison-summary.csv, report.md
  figures/*.png, *.svg, provenance.json
  archive-status.json
```

heartbeat는 기본 30초마다 기록하며 상태에는 현재 조건·완료/실패/미실행 수가 있다. 90초 이상 지난 비종료 상태는 stale로 표시한다. stale/미게시를 완료·중지로 단정하지 않는다. `completed`는 계획 검증과 최종 S3 archive가 모두 성공했을 때만 기록한다. 실험 실패와 업로드 실패는 구분한다.

## 중단과 재개 범위

노트북 종료에는 재개 작업이 필요 없다. EC2 서비스 자체가 실패한 경우에만 원인을 확인한 뒤 다음을 사용한다.

```bash
python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py resume \
  --job /tmp/raft-detached-job --profile openkache
```

같은 coordinator의 root EBS 체크포인트와 같은 패키지를 사용한다. 정상 정리된 실패/미완료 조건만 새 attempt로 실행한다. 완료된 조건은 건너뛰며 전체가 완료됐다면 보고서 재생성·보관 후 자동 정지만 재시도한다. 보조 노드가 이미 정지됐더라도 S3의 완료/보관 상태가 확인되고 coordinator가 실행 중이면 이 재시도를 허용한다. 이미 정지된 보조 노드를 다시 기동하지 않는다.

EC2 강제 종료·서비스 SIGKILL처럼 **진행 중 case의 정리가 확인되지 않은 상태는 자동 재개하지 않는다.** 이전 SSM 명령과 장애 복원을 점검해야 한다. 노드 재부팅·coordinator 손실에 대한 자동 failover/자동 재시작은 제공하지 않는다. 서비스 중단과 EC2 과금 중단도 구분한다.

## 로컬 검증

```bash
python3 -m unittest discover -s experiments/a1-active-passive-failover/real-server -p 'test_raft*.py' -v

python3 experiments/a1-active-passive-failover/real-server/validate_detached_raft.py \
  --out /tmp/raft-detached-validation-new \
  --lib cache-server/build/install/openkache-server/lib \
  --dist cache-server/build/distributions/openkache-server-0.1.0-SNAPSHOT.tar
```

오프라인 adapter는 파일 기반 `s3 cp`만 허용하고 실제 AWS 작업은 거절한다. 실제 로컬 JVM으로 정상/crash × 3방식=6회를 수행한다. 시작 부모가 먼저 종료된 뒤에도 worker가 완료·보관하고, 완료 재개가 중복 측정을 만들지 않는지 확인한다. 로컬 검증에서는 자동 정지 API를 호출하지 않는다. 별도 Mock 테스트로 S3 검증, 정지 순서, 권한 오류·보조 노드 정지 지연·coordinator 정지 거절·이미 정지된 노드의 재시도를 검증한다. 실제 EC2 systemd·IAM·SSM·S3 및 정지 동작 검증을 대신하지 않는다.
