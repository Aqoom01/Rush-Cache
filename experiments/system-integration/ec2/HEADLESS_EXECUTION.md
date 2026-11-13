# 노트북과 독립적인 실행·S3 검증·EC2 정리

기존 SSAFY GitLab Runner가 Terraform과 AWS SSM을 제어하고, gateway EC2의 systemd 서비스가 통합 실험을 실행한다. 노트북의 Python·SSH 세션을 유지할 필요가 없다. 실제 계정에서의 end-to-end 실행은 아직 검증하지 않았다.

## 정상 흐름

1. 보호 브랜치의 웹 pipeline에서 `EXPERIMENT_SUITE=integrated`를 선택한다. 기본 입력은1행 `cloud-preflight.csv`다. 이 작은 실행으로 부팅→실험→S3 재다운로드→삭제를 먼저 확인한다.
2. CI에서 배포 ZIP을 빌드하고 공개된 SHA-256으로 묶음을 식별한다. S3의 `bundles/integration/<sha>.zip`에 올린다. EC2 bootstrap은 같은 SHA를 검증한 뒤 사용한다.
3. 전용 m6gd.medium8대를 만들고, 이 CI job의 고유 `ExperimentRun=ci-<pipeline>-<job>` 태그와 별도 Terraform state로 소유권을 고정한다. 기존 인스턴스를 채택하거나 삭제하지 않는다. 계정의 미종료 인스턴스 전체와 합쳐9대 상한을 검사한다.
4. private SSH 키는 gateway에서 새로 만들며 노트북·S3·SSM 명령에 개인키를 넣지 않는다. 공개 host key는 인증된 SSM으로 읽어 known_hosts를 구성한다. SSH와 통합 TCP/UDP 포트는 실험 subnet 내부에서만 연다.
5. gateway의 systemd 서비스가 CSV 묶음을 순차 실행한다. 부하 생성기는 별도 EC2다. 한 묶음은 최대12행, worker는130분 제한이고 EC2에는180분 자동 정지 제한을 유지한다. 여러 묶음은 같은 CI resource_group으로 직렬 실행한다.
   pipeline 시작 후21시간이 지나면 새3시간 묶음을 시작하지 않는다. 이미 만든 자원의 검증·정리 경로는 이 시각 제한 이후에도 허용한다.
6. 결과·로그·입력·PID 상태는 EBS에 저장하고, 재생성 가능한 native 캐시 데이터만 `native.data.root=/mnt/nvme/openkache`에 쓴다. 따라서 업로드 문제가 나도 결과를 EBS에서 복구할 수 있다. KV 저장소 자체의 전체 파일을 결과로 자동 수집하지는 않는다.
   리소스 표본은 측정 중 JSONL에 추가하고 최종 JSON 배열은 수집 후 한 번 생성한다. single-vCPU gateway에서 전체 표본을 매번 다시 쓰는 수집 비용을 줄인다.
7. 결과 전체를 tar.gz와 파일별 SHA-256/크기 inventory로 만들고, S3 SHA256 checksum을 지정해 버전 관리 버킷에 올린다. archive·manifest·완료 pointer 순으로 올린다. 실패한 실험의 원본도 보존한다.
8. 외부 CI가 S3에서 다시 읽어, 현재 객체의 VersionId가 업로드 버전과 같은지 확인하고 압축 파일과 내부 모든 파일의 실제 SHA-256을 검사한다. 요청한 행 수·selectors·입력/로그·collection/cleanup·PASS 결과 파일도 확인한다. ETag를 SHA-256으로 취급하지 않는다. 백업 검증과 실험 정합성 통과는 별도 결과다.
9. 검증된 binding의 계정·OpenStd identity·8개 instance ID·type·Project/ManagedBy/ExperimentRun 태그가 모두 맞을 때만 해당 EC2를 terminate한다. 종료를 기다린 뒤 이 job의 별도 Terraform state에 있는 허용된 실험 자원만 제거한다. 결과/S3/bootstrap 자원은 destroy 대상이 아니다.

## 실패 시 동작

- 누락·손상·다른 실행의 backup·미완료 수집/cleanup이면 자동 삭제를 차단한다. 로컬 성공 marker만으로 삭제하지 않고 항상 S3를 다시 검증한다.
- 수집이 완전한 실패 실험은 FAILED 상태와 원본을 보존한 뒤 정리할 수 있다. 이를 성공한 실험으로 바꾸지 않는다.
- 검증할 수 없으면 소유 태그를 재확인한 인스턴스만 정지하고 EBS·Terraform state를 보존한다. 정지된 인스턴스와 EBS에는 잔여 저장 비용이 발생하며, 복구 후 별도 정리가 필요하다. 자동으로 불완전한 백업을 성공 처리하지 않는다.
- CI/worker 장애나 취소에서도 무조건 destroy하는 기존 legacy after_script를 사용하지 않는다. 계정의 다른 실험은 건드리지 않는다. GitLab Runner까지 중단되면 EventBridge의 최대 수명 정지가 계산 비용을 제한한다. bootstrap 초기에170분 guest shutdown timer도 설치해 provisioning 중 Runner가 중단된 경우를 보완한다.

### 기존 실패 실행의 읽기 전용 진단

`EXPERIMENT_SUITE=integrated-diagnose`와 기존 `INTEGRATION_SOURCE_PIPELINE`·`INTEGRATION_SOURCE_JOB`을 지정하면, 기존 CI AWS 권한으로 해당 실행의 결과만 읽는다. S3 pointer/manifest의 버전과 해시, archive 전체 및 파일별 해시를 확인하고 원본 `collection.json`·`cleanup.json`·실행 로그 끝부분과 해당8대의 현재 상태를 출력한다. 수집 완료나 삭제 승인을 의미하지 않는다. EC2 생성·시작·정지·삭제와 Terraform apply는 호출하지 않는다.

첫 사전 실행 `295003 / 952587`은 `remote-running` 이후 `uncollected or mismatched evidence`로 삭제가 차단됐다. 코드 점검에서 gateway 디렉터리를 replica로 취급해 존재하지 않는 `gateway/a1-authority/*`와 `gateway/a2-data/*`를 요청하는 결함을 재현·수정했다. 실제 해당 실행의 상세 누락 항목과 인스턴스 상태는 진단 job으로 확인해야 한다. 원본 증거를 확인하기 전에는 수집 완료 값을 바꾸거나 보호 장치를 해제하지 않는다.

보존된 인스턴스도9대 상한에 포함된다. 따라서 이 상태에서 `integrated`/`integrated-core`를 다시 시작하지 않는다. 기존 실패 job의 Retry는 이전 커밋을 실행하므로 수정 반영 후의 새 진단 pipeline을 사용한다.

## 입력과 결과 위치

- `INTEGRATION_MATRIX`: 저장소 CSV. 이 CI 경로는 최대156행을12행씩 나눈다. 빈 구간은 AWS 호출 없이 종료한다.
- `INTEGRATION_PLAN_ARGS`: 공통 workload 인수의 JSON 문자열 배열. 기본은 기능 preflight용100req/s·600요청이다. EC2에서 보정한 본실험 부하는 따로 지정한다.
- `integrated`는 확정된 CSV의 모든 구간을 자동 실행한다. CSV의 방법론 선택을 공통 인수로 바꾸는 것은 거절한다. `integrated-core`는 아래의 자동 보정·후보 선정·후속 실험 경로를 사용한다.
- S3: `s3://openstd-s15p31d111-results/results/system-integration-core/ci-<pipeline>/ci-<pipeline>-<job>/`
- 각 경로에 `UPLOAD_COMPLETE.json`, `attempt-<id>/manifest.json`, `attempt-<id>/results.tar.gz`, 검증·정리 성공 시 `CLEANUP_COMPLETE.json`이 생긴다.
- CI artifact에는 binding·검증 receipt·삭제/보존 상태를30일 남긴다. 실제 원본은 S3에서 보관한다.

## 전체 핵심 캠페인의 자동 진행

`EXPERIMENT_SUITE=integrated-core`는 `core_pipeline.py`와 `infra/core-pipeline-ci.yml`로 다음 순서를 서버에서 자동 진행한다.

1. RAFT+O00의4개 요청률 pilot을 EC2에서 실행하고, 검증된 결과로 중간/높은 부하를 선택한다. 최대 요청률에서도 포화가 없으면 포화가 확인되지 않았다고 기록한다.
2. A1×A2 63회 선별 후 후보를 고르고, 확대 작업 집합에서 A2 18회를 실행한다.
3. 확대 결과로 후보를 다시 정해 B1/B2 24회, 복사12회, 장애20회를 진행한다.
4. B1/B2·복사·FD의 검증된 반복 결과로 후보별 구성 요소를 선택해 결합한다. FD는 장애 중 읽기 실패도 가용성 지표로 계산하며, 오판정이 있는 선택은 제외한다. 실제 이동이 확인된 copy만 고른다. 기준선과 결합 후보2개를 부하2종×새 seed3개로18회 SCALE_CYCLE 재검증한다.
5. 기본155회와 pilot4회를 구분한 REPORT.json을 S3에 올리고 다시 읽어 checksum을 확인한다. 일부 실패는 실패 상태로 남기며, 유효한 새 반복3개가 없는 조합의 최종 순위를 만들지 않는다.

단계 전환은 S3에서 독립 검증된 receipt와 해당 묶음의 정리 완료 기록이 모두 있을 때만 한다. 노트북이나 대화형 AI가 다음 후보를 골라 줄 필요가 없다. 단계별로 입력 CSV·workload 인수·선정 근거도 CI artifact와 결과 archive에 남는다.

실제 pipeline의 부팅·S3·삭제 경로는 아직 검증하지 않았다. 보호 브랜치 반영과 CI 설정 확인, 1행 preflight의 실제 S3·삭제 확인 전에는 노트북을 꺼도 되는 상태로 완료됐다고 안내하지 않는다. 과거 로컬/방법론 감사의 JAR 해시는 그때의 버전 근거이며, 이번 headless 배포 JAR과 결과는 별도 해시로 기록한다. 배포·실행 진행 상태는 별도 실행 기록으로 확인한다.
