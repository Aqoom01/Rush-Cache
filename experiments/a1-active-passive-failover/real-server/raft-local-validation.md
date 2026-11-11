# Raft + SWIM/Lifeguard 전체 campaign 로컬 검증

검증일: 2026-11-10. 요청 범위는 구현과 로컬 검증이며 EC2 생성·배포·실험은 수행하지 않았다.

## 실행 결과

- 기본 Raft / Raft+SWIM / Raft+SWIM+Lifeguard × 13조건 × 1회 = **39/39 passed**.
- 정상 통신 비용·선거 원인 집계 경로를 별도로 **6/6 passed**로 확인했다.
- UDP/TCP 양방향 partition 및 I/O 정지 계측 보정 후 **6/6 passed**로 재검증했다. 완료된 campaign에 `--resume`을 실행해 새 attempt나 JVM 없이 종료되는 것도 확인했다.
- 전체 Java 테스트 결과: 1035개 중 실패/오류 0, 12개 skipped.
- campaign/SSM 도구의 오프라인 Python 테스트 12개 통과. PID 소유권 검사, CPU 정지 후 CONT 복원, partition 규칙의 정확한 삭제, 완료된 campaign 재개의 중복 실행 방지, preflight 실패 보고서를 포함한다.
- 39회에서 성공 응답한 key 이력 총 6341건을 생존 복제본의 값과 대조했다. 이는 성능 표본 수나 독립 실험 횟수를 뜻하지 않는다.

13조건은 정상, crash, UDP/양 plane loss, UDP/양 plane delay, 리더/관측자 CPU pause, 리더/관측자 I/O stall, 파일 I/O contention, partition, client 요청 중 crash다.

## 근거 파일

- 전체 39회 원본: `/tmp/openkache-all-scenarios-Kn44O6/full-local/`
- 최종 통신 비용·선거 집계 6회: `/tmp/openkache-all-scenarios-Kn44O6/final-metrics/`
- 양방향 partition 및 정지 계측 보정 후 검증: `/tmp/openkache-all-scenarios-Kn44O6/final-partition-io/`
- 각 디렉터리의 `manifest.json`, `summary.json`, `report.md`, `runs.csv`, case별 `result.json`과 `attempt-N` 아래 JVM 로그에 원본을 보존한다. `/tmp` 원본은 시스템 청소 시 사라질 수 있으므로 장기 보관하려면 별도 결과 저장소로 복사해야 한다.

전체 39회 campaign ID는 `raft-all-4005c8064eb0`이다. 해당 실행의 JAR 묶음 SHA-256은 `70081cc90aaecac3520c61d2143ee5dbc523f025de3198df7aaa331bcc785a65`다. 이후 계측 보정 실행은 각자의 manifest로 바이너리와 스크립트를 식별한다.

## 해석 범위

로컬에서는 실제 JVM 3개, TCP/UDP, OS SIGKILL/STOP/CONT와 파일 저장소를 사용했다. 통신 장애는 application-message drop/delay 모델이고, I/O contention은 로컬 파일시스템의 동기 쓰기 부하다. EC2의 kernel netem, 실제 NVMe 장치 성능이나 fio 결과와 동일하게 해석하지 않는다.

I/O stall은 저장소 worker의 완료를 지연시키는 소프트웨어 주입이다. 물리 SSD 고장을 재현했다고 주장하지 않는다. 짧은 로컬 실행은 동작·안전성·자동화 검증이며, detector의 통계적 우열이나 최종 운영 p99를 주장하지 않는다.

위 39회/117회는 발표용 보강 전 기록이다. 현재 실행 계획과 준비 조건은 [전체 campaign 문서](raft-full-campaign.md)를 따른다.

## 발표용 비교 보강 후 검증 — 2026-11-10

사용자 요청에 따라 Raft 기준 timeout T=500ms를 고정하고, CPU 경합·장시간 정상 부하·선거/리더 교체 구분·비교 보고서를 추가했다. EC2 생성·접근·실험 및 S3 실제 업로드는 이번 검증에서 수행하지 않았다.

- **14조건 × 3방식 × 1회 = 42/42 passed**. 새 CPU 경합 조건에서 worker의 실제 CPU 소비와 JVM thread affinity 복원을 확인했다. 성공 응답한 key 이력 8482건을 생존 복제본과 대조했다.
- **정상 presentation 경로 3/3 passed**. 방식마다 10초 추가 warmup·60초·50 SET/s를 수행했고 각각 **3000/3000 성공, missed slot 0**, 정상 표본 기준 1000개를 충족했다. 장시간 경로의 성공 응답 key 이력 11277건을 검증했다.
- **정상/crash 최종 경로 6/6 passed**. 추가 완료 재개 검증 **1/1 passed** 후 `--resume`이 새 attempt를 만들지 않고 종료되는 것을 실제 확인했다. 모든 검증 후 해당 디렉터리 소유 JVM은 남지 않았다.
- **Java check 성공:** test XML 1035개, 실패 0, 오류 0, skipped 12. candidate 시작과 실제 당선 journal의 순서·원인·불변성 및 status 노출을 확인했다.
- **Python 오프라인 테스트 21개 통과:** fault PID 검사, CPU pause/경합 취소 복원, 정확한 partition 규칙 삭제, journal polling 누락 복원·중복 제거·동일 term 복수 리더 검출, 실패/미실행 및 N/A 보존, 요청 실패·missed slot 계측, S3 업로드 완료/실패 구분, 완료 EC2 재개의 SSM/JVM 미실행 경로를 포함한다. S3 검증은 mock이며 실제 AWS 권한/업로드 성공 검증이 아니다.
- **발표용 최종 계획 252회 생성:** 핵심 6조건×3방식×10회=180회, 나머지 8조건×3방식×3회=72회. 정상 장시간 측정은 30회다. 계획 생성은 AWS/JVM을 사용하지 않았다.
- `runs.csv`, `conditions.csv`, paired 비교 CSV, 보고서 및 복구/안정성/정상 비용 **PNG·SVG 3종**을 생성하고 그림을 확인했다. 모든 로컬 그림에는 LOCAL IMPLEMENTATION VALIDATION 표기를 넣었다. 후속 재분석은 analysis/plot provenance에 실제 코드·입력 SHA를 별도로 남긴다.

근거 위치:

|검증|원본 경로|campaign ID|
|---|---|---|
|전체 42회|`/tmp/openkache-raft-study-SIDo0z/all-local/`|`raft-all-152c39f1a0d5`|
|60초 정상 측정 3회|`/tmp/openkache-raft-study-SIDo0z/healthy-presentation/`|`raft-all-d4c0bf98145b`|
|최종 정상/crash 6회|`/tmp/openkache-raft-study-SIDo0z/final-smoke/`|`raft-all-d064026c82ff`|
|완료 재개 1회|`/tmp/openkache-raft-study-SIDo0z/resume-smoke/`|`raft-all-af22bb366360`|
|최종 252회 계획|`/tmp/openkache-raft-study-SIDo0z/final-presentation-plan/`|manifest 참조|

로컬 측정의 JAR 묶음 SHA-256은 `3a97350ee645e48c0e76e1af196b2cbc3464ec042c749c3dea92f7280a1fd22f`다. EC2용 dist tar SHA-256은 `74b09be2a5069a7baed058ebe642764074f03ea07f5ad5d904ea95077d772c61`다. 그래프 검증에는 `/tmp/openkache-raft-presentation-venv/bin/python`과 Matplotlib 3.11.2를 사용했으며 시스템 Python 패키지는 변경하지 않았다.

이는 구현·측정·분석 경로의 검증이다. Lifeguard 우월성, EC2 복구 성능, 장기 오탐률을 확정한 결과는 아니다. EC2에서는 실제 kernel netem, 전용 노드 CPU 경합, instance NVMe/fio, SSM·S3 권한 및 전체 소요 시간을 별도로 확인해야 한다. 준비·실험 전에 사용자에게 먼저 안내하며 현재는 로컬 검증에서 멈춘다.

## 노트북과 분리된 coordinator 보강 검증 — 2026-11-10

정식 실행을 [EC2 coordinator 서비스](raft-ec2-coordinator.md)로 변경했다. 실험용 3대 중 한 대가 systemd로 전체 순서를 제어하고, 노트북에는 시작·상태 조회·결과 복사만 남긴다. 기존 직접 EC2 CLI 실행은 디버깅용 `--foreground`를 명시해야 한다.

- 오프라인 Python 테스트 **38개 통과**. 원래 21개와 checkpoint callback 오류 처리, source/job 무결성, source bundle import, 명령 구문, 중복 서비스 시작 방지, worker 잠금, 정리 미확인 case 재개 거절, 역할 인증 격리, IAM 정책 범위, S3 상태·수집의 비실행성, 초기 heartbeat 이전 진단, 최종 상태 업로드 실패 구분 및 foreground 진입점 검사를 포함한다.
- 실제 분리 프로세스 + 로컬 JVM의 **정상/crash × 3방식 = 6/6 passed**. 시작 부모 프로세스가 먼저 종료된 상태에서 별도 session의 worker가 끝까지 실행되는 것을 확인했다.
- 파일 기반 S3에 **case별 checkpoint 6개**, manifest/요약/CSV/보고서와 archive 완료 표시 및 coordinator 최종 `completed` 상태가 남았다.
- 완료된 worker를 다시 실행해 **새 attempt 없이** 보고서 보관만 수행하는 것을 확인했다. source/dist/job은 고정 SHA로 검증했다. 기준 Raft timeout은 **500ms**다.
- 새 CLI `prepare`로 **252회 presentation 검토용 패키지와 시작 스크립트·IAM 정책**을 생성했다. 검토용 가상 계정/인스턴스 값을 사용했고 AWS 호출은 하지 않았다.

최종 근거는 `/tmp/openkache-detached-final-20261110/validation-summary.json`이며 campaign은 `raft-ec2-d72057a5ce86`이다. coordinator 원본은 `/tmp/openkache-detached-final-20261110/coordinator/`, 모사 archive는 `/tmp/openkache-detached-final-20261110/s3/offline-validation/raft-ec2-d72057a5ce86/`에 있다. 검토용 252회 패키지는 `/tmp/openkache-detached-review-20261110/`다. 이 경로들은 실제 S3가 아닌 로컬 파일이다.

파일 adapter는 `s3 cp` 외 AWS 동작을 거절한다. 호스트 systemd 서비스도 변경하지 않았다. 실제 EC2 systemd·IAM·SSM·S3 검증 및 실험 시작은 수행하지 않았으며, 실행 전에 사용자에게 먼저 알린다. coordinator 서비스 시간 제한은 EC2 자동 정지나 과금 상한을 뜻하지 않는다.

## 성공 후 EC2 자동 정지 보강 검증 — 2026-11-10

사용자 요청에 따라 새 EC2 패키지는 `stopOnSuccess=true`가 기본값이다. 전체 계획 통과·해당 job 일치·S3 archive 읽기 재검증과 artifact 존재/크기를 확인한 뒤 보조 2대의 stopped 상태를 확인하고 coordinator를 마지막으로 정지 요청한다. `prepare --keep-running`은 이를 비활성화한다. 실패 시에는 노드를 유지하며, 실험 완료와 정지 성공 여부를 별도로 보고한다.

- **오프라인 Python 테스트 56개 통과.** DryRun 권한 확인, 정확한 3개 ARN의 StopInstances 권한, 로컬 결과·미완료 계획·보관 실패·S3 불일치·다른 계정/노드의 정지 차단, 보조 노드 정지 순서와 timeout, 자기 정지 전 S3 의도 기록, 정지 거절, 보조 노드가 이미 stopped인 재시도, 정지된 coordinator에 SSM을 보내지 않는 상태 조회를 포함한다. 정지 API는 Mock으로만 검증했다.
- **분리 프로세스·실제 로컬 JVM 정상/crash × 3방식 6/6 passed.** 부모 종료 후 완료, checkpoint 6개와 최종 archive 보관, 완료 재개 시 중복 attempt 없음이 확인됐다. 로컬 모드에서는 자동 정지를 명시적으로 건너뛰었고 파일 기반 adapter는 EC2 API를 허용하지 않는다.
- 검증 원본: `/tmp/openkache-auto-stop-validation-20261110/validation-summary.json`. campaign: `raft-ec2-06a1a516262e`. worker 및 모사 archive는 같은 디렉터리의 `coordinator/`, `s3/offline-validation/raft-ec2-06a1a516262e/`에 있다.
- 검토용 가상 계정/인스턴스로 새 252회 자동 정지 패키지와 IAM 정책을 `/tmp/openkache-auto-stop-review-20261110/`에 생성했다. 실제 IAM 변경·DryRun·EC2 정지·실험은 수행하지 않았다.

실제 EC2에서는 준비 단계에서 역할 권한과 정지 가능 여부를 확인하고 사용자에게 먼저 알린 뒤 실행한다. 성공 후 정지는 EBS/S3 삭제나 실패 시 과금 상한을 뜻하지 않는다. S3의 자기 정지 요청 기록과 실제 stopped 확인을 구분하며, 실제 3대 상태는 `status --live`로 읽는다.
