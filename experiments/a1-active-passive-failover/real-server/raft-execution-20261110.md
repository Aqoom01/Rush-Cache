# 실제 EC2 실험 시작 기록

시작: **2026-11-10 23:48:01 KST**. campaign: `raft-ec2-528f18e97c71`.

사용자가 실험 시작을 승인한 뒤 서울 리전의 기존 자원을 조회했고, 실행·정지 인스턴스가 없어 전용 m6gd.medium 3대를 Terraform으로 생성했다. 같은 `ap-northeast-2a`의 기존 기본 퍼블릭 서브넷을 재사용했다. 생성은 13개이며 기존 자원 변경·삭제는 없었다. 새 NAT·로드밸런서·별도 데이터 EBS는 만들지 않았다.

|역할|인스턴스|사설 IP|
|---|---|---|
|node 0|`i-0cac41144e30c49a6`|`172.31.7.74`|
|node 1|`i-0135c3b84a5a7fb1c`|`172.31.13.69`|
|node 2 / coordinator|`i-0d998ae4f0df79d86`|`172.31.10.122`|

SG는 내부 TCP7000-7001·UDP7002만 허용하며 외부 inbound는 없다. 루트 gp3는 각 20GB, 암호화, `DeleteOnTermination=true`다. 정지 후에는 EBS가 유지된다.

## 사전 확인과 시작

- SSM 3대 Online, cloud-init 완료·S3 ready marker 3개를 확인했다.
- Java Corretto 21.0.13, Python 3.11.18, Matplotlib 3.11.2, iptables/tc를 확인했다.
- 3대 모두 `Amazon EC2 NVMe Instance Storage`의 XFS mount를 확인했다. 각 노드에서 별도 준비 파일을 사용하는 2초 직접 I/O 검사가 41,000회 이상의 I/O로 성공했다. 본실험 표본과 분리된 준비 검사다.
- coordinator 서비스 `openkache-raft-ec2-528f18e97c71.service`가 active/running으로 시작했고, S3 heartbeat와 실제 case 실행을 확인했다. 시작 요청 프로세스가 끝나도 EC2 서비스가 전체 순서를 제어한다.
- 기준 timeout T=500ms, 3방식 × 사전 지정 반복 계획 총 **252회**, 성공 후 자동 정지 활성 상태다. 실험 시작 전 정지 권한 DryRun도 통과했다.

## 초기 확인 스냅샷

이 문서는 시작 기록이며 최종 결과가 아니다. 마지막 초기 조회 시 **10/252 passed, failed 0**, heartbeat 정상, `loss-both-swim-r1` 실행 중이었다.

- 첫 정상 상태 baseline: 3000/3000 SET 성공, missed slot 0, 표본 기준 충족, 데이터 보존 감사 통과.
- 실제 리더 종료 baseline: 선거 원인 `TIMEOUT`, 데이터 보존 감사 통과.
- 실제 리더 종료 SWIM 및 SWIM+Lifeguard: 선거 원인 `SWIM_DEAD`, fallback 0, 데이터 보존 감사 통과.

위 초기 결과만으로 통계적 우열이나 전체 성공을 주장하지 않는다. 모든 조건과 보고서 보관 검증이 성공해야 자동 정지한다. 실패 시 상태를 남기고 노드를 유지한다.

## 결과와 재접속

- 결과: `s3://openstd-s15p31d111-results/raft-ec2-528f18e97c71/`
- 실시간 상태: 위 prefix의 `control/status.json`, `control/coordinator.log`
- Terraform 원격 상태: `s3://openstd-s15p31d111-tfstate/raft-campaign/raft-ec2-528f18e97c71.tfstate`
- 노트북 재시작에도 남는 로컬 실행 기록: `experiments/a1-active-passive-failover/real-server/runs/raft-ec2-528f18e97c71/`. 패키지·실행 영수증·계획·사전 검사·초기 결과와 마지막 상태를 보존했다. raw 파일은 Git에서 제외된다.

```bash
python3 experiments/a1-active-passive-failover/real-server/raft_ec2_campaign.py status \
  --job experiments/a1-active-passive-failover/real-server/runs/raft-ec2-528f18e97c71/job \
  --profile openkache --aws-bin '/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe'
```

`--live`를 추가하면 3대의 실제 상태도 읽는다. 완료 뒤 `collect` 명령으로 S3를 로컬에 복사할 수 있으며 S3 원본은 삭제하지 않는다. 실행·자동 정지와 조회 방법은 [coordinator 안내](raft-ec2-coordinator.md)에 기록했다.
