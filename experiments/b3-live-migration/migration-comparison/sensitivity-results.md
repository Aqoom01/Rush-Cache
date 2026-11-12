# S3 종료 설정 민감도 결과

2026-10-26. pre-copy의 종료 임계·최대 live 라운드가 쓰기 차단·전체 이동 시간·전송량에 주는 영향을 비교했다.

공식 run은 [full 보고서](runs/run-2026-10-26T05-27-59.618483800Z-9396af1b/repeat-001/results.md)이며, [조건별 CSV](runs/run-2026-10-26T05-27-59.618483800Z-9396af1b/repeat-001/summary.csv)와 [실행 설정](sensitivity.properties)을 함께 보관한다.

- N=100,000, key 16 bytes·value 128 bytes, B=64 KiB/tick, 균등 분포, DELETE 30%
- rho 0.01/0.1/0.5/0.9/1.2 × 임계 64/256/1,024 KiB × 상한 3/10/30 = 45조건
- 조건별 seed 1~5와 stop-and-copy/pre-copy/post-copy, 총 675/675회 완료
- 정합성 위반 0건, 최종 destination 데이터 675/675 일치

## 라운드 상한의 영향

종료 임계를 256 KiB로 고정했다. 표는 seed 5개의 최솟값~최댓값이며 평균·신뢰구간이 아니다. 전송량의 분모는 이동 전 N×152 bytes다.

| rho | 라운드 상한 | 쓰기 차단 tick | 전체 이동 tick | 전송량/초기 크기 | 실제 live 라운드 | 종료 사유 |
|---|---:|---:|---:|---:|---:|---|
| 0.5 | 3 | 6 | 353~354 | 1.500962~1.504062 | 3 | ROUND_LIMIT 5건 |
| 0.5 | 10 | 3~4 | 355~356 | 1.509580~1.512649 | 4 | THRESHOLD 5건 |
| 0.5 | 30 | 3~4 | 355~356 | 1.509580~1.512649 | 4 | THRESHOLD 5건 |
| 0.9 | 3 | 28~29 | 479~483 | 2.048893~2.060164 | 3 | ROUND_LIMIT 5건 |
| 0.9 | 10 | 5 | 539~542 | 2.295024~2.306595 | 9~10 | THRESHOLD 5건 |
| 0.9 | 30 | 5 | 539~542 | 2.295024~2.306595 | 9~10 | THRESHOLD 5건 |
| 1.2 | 3 | 56~57 | 581~583 | 2.484690~2.494006 | 3 | ROUND_LIMIT 5건 |
| 1.2 | 10 | 22~23 | 797~805 | 3.406831~3.435388 | 10 | ROUND_LIMIT 5건 |
| 1.2 | 30 | 11~12 | 954~1,021 | 4.058685~4.322708 | 21~26 | NOT_SHRINKING 5건 |

rho=0.5와 0.9에서는 상한 10회 안에 임계에 도달하므로 30회로 늘려도 결과가 달라지지 않았다. rho=1.2에서는 상한을 늘리면 쓰기 차단이 줄지만 전송량과 전체 이동 시간이 늘었다. 상한 30회 조건은 21~26회 뒤 dirty bytes가 줄지 않아 종료했다. 이 결과는 설정에 따른 비용 교환이며 보편적인 수렴 임계값이나 최적 상한을 확정하지 않는다.

## 종료 임계의 영향

rho=0.9, 라운드 상한 10회에서 종료 임계만 바꿨다.

| 종료 임계 | 쓰기 차단 tick | 전체 이동 tick | 전송량/초기 크기 | 실제 live 라운드 | 종료 사유 |
|---|---:|---:|---:|---:|---|
| 64 KiB | 4~5 | 542~545 | 2.303539~2.318685 | 10 | ROUND_LIMIT 5건 |
| 256 KiB | 5 | 539~542 | 2.295024~2.306595 | 9~10 | THRESHOLD 5건 |
| 1 MiB | 14~15 | 511~516 | 2.183170~2.197985 | 5 | THRESHOLD 5건 |

큰 임계는 더 많은 변경분을 마지막 쓰기 차단 구간에서 보내므로 전체 이동과 전송량은 줄지만 차단은 길어진다. 작은 임계는 차단을 줄이는 대신 live 복사를 더 진행하고, 상한 때문에 임계에 도달하기 전에 멈출 수도 있다.

## 재현·검증

[실행 안내](README.md)의 sensitivity.properties와 sensitivity-quick.properties를 사용한다. 기존 S1~S5 결과와 별도 run으로 유지한다.

- full의 25개 rho/seed 묶음마다 9설정 × 3방식, 27회가 동일 workloadHash를 사용함을 확인했다.
- stop-and-copy·post-copy는 pre-copy 종료 설정을 바꿔도 모든 숫자 지표가 동일했다. 두 방식의 candidateConfig는 비어 있으며 설정 칸은 적용 대상 아님으로 표시한다.
- 전체 빌드·테스트 328개 통과. 설정에 따른 실제 종료 사유 변화, 요청열·Baseline 불변, 원본 재생성, 설정·workloadHash 변조와 결과 누락 검출을 검증했다.
- full CSV와 Markdown을 JSON 원본에서 재생성한 뒤 SHA-256이 이전 파일과 같음을 확인했다.
- full summary.csv SHA-256: 7514CBD9EB6FA960E14BCF7FB5C24CFF1ED2604B69A7FFC556E55905F1E17F1D
- full results.md SHA-256: 493340F18CD2599AF2DD0EC2DD32EC42ABC09BD00952106643E6BB15029B133B
- quick run-2026-10-26T05-27-59.618483800Z-ffdaa7ed는 675회 묶음을 두 번 실행해 1,350/1,350회 성공했다. 두 summary.csv SHA-256은 D5DCBE4F8B4E87FACECB3B564FC0ECA5061913EA36DF20A34D4A701D0346225C로 일치한다.
- 실행 기준 commit은 698726b이며 미커밋 S3 민감도 backend 변경을 포함한다. source hash는 dd18d33cfecc78102a1784d4d3dcec379302863ef93f39dd73c06e1e4c6301eb, backend class hash는 8a99aeaaac6f9f3184b24f90322c18fca3db6e4cdbe7dcf1a3270d25726ad147이다.
- JSON 원본 675개와 manifest·실행 로그는 해당 run 폴더에 로컬 보관하며 Git에서는 제외한다. Git 보관 대상은 반복별 summary.csv·results.md다. quick 원본은 cache-core/build/b3-sensitivity-quick 아래에 있다.

## 해석 범위

같은 rho/seed의 tick별 요청 시도는 같지만 설정·방식마다 완료 시점과 성공 쓰기 수가 다르다. 따라서 동일 성공 작업량을 처리한 성능 우열로 해석하지 않는다. workloadHash는 알고리즘 설정을 제외한 생성 규격 hash이며 실제 요청 trace 전체 hash가 아니다. 기존 inputHash는 알고리즘 설정까지 포함한 규격 hash다.

rho는 쓰기 시도당 152 bytes 기준이며 DELETE·같은 key의 반복 쓰기로 실제 dirty byte 유입량과 다를 수 있다. 전송량에는 데이터 응답 framing을 포함하지만 ACK·pull 요청과 일반 client 통신 bytes는 제외한다. quick은 N과 예산을 줄인 재현 검증용으로 full 결과와 합산하지 않는다.

이 결과는 가상 시간·인메모리 모델이다. 실제 서버 downtime·p99, source 장애 비용은 측정하지 않았다. phaseEvents는 상태·라운드 경계이며 v2 dirty generation별 상세 bytes를 대체하지 않는다. S6 취소·장애 비교는 이후 별도 suite로 구현·실행했다([S6 결과](interruption-results.md)). E/M scaling·실제 KV/peer 실험은 후속이다.
