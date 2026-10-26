# S3 pre-copy 설정 민감도 실험

실행 정보: `run-2026-10-26T05-27-59.618483800Z-9396af1b` · 반복 1/1 · 설정 `sensitivity-full` · commit `698726b6e2fd12a5ae4486a6c58fccb09a7c3aef` · source hash `dd18d33cfecc`

쓰기율 5종 × 종료 임계 3종 × 라운드 상한 3종 = 45조건 × seed 5개 × 세 방식, 총 675회. 정합성 위반 0건, 최종 데이터 675/675 일치.

표는 seed 5개의 최솟값~최댓값이다. 종료 사유는 THRESHOLD/ROUND_LIMIT/NOT_SHRINKING 순서의 실행 건수다. 종료 임계와 라운드 상한은 pre-copy에만 적용하며 다른 방식은 —로 표시한다. 조건 ID에는 비교 상대의 설정이 포함된다.

같은 rho/seed는 모든 설정과 방식에 동일한 초기 데이터·tick별 요청 **시도**를 재생한다. workloadHash는 알고리즘 설정을 제외한 생성 규격 hash이며 원본 trace 전체 hash가 아니다. 기존 inputHash는 설정을 포함한 규격 hash로 유지한다. 설정별 완료 시점·성공 쓰기 수가 달라 같은 성공 작업량의 성능 비교로 해석하지 않는다.

| 조건 | N | rho | 방식 | 임계 bytes | 라운드 상한 | 쓰기 차단 tick | 전체 tick | 전송량/초기 크기 | live 라운드 | 종료 사유 T/R/N |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
| S3-sensitivity-rho0.01-threshold65536-rounds3 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds3 | 100,000 | 0.01 | pre-copy | 65536 | 3 | 2 | 238 | 1.0083~1.0087 | 1 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold65536-rounds3 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds10 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds10 | 100,000 | 0.01 | pre-copy | 65536 | 10 | 2 | 238 | 1.0083~1.0087 | 1 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold65536-rounds10 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds30 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds30 | 100,000 | 0.01 | pre-copy | 65536 | 30 | 2 | 238 | 1.0083~1.0087 | 1 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold65536-rounds30 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold262144-rounds3 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold262144-rounds3 | 100,000 | 0.01 | pre-copy | 262144 | 3 | 3 | 237 | 1.0083~1.0086 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold262144-rounds3 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold262144-rounds10 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold262144-rounds10 | 100,000 | 0.01 | pre-copy | 262144 | 10 | 3 | 237 | 1.0083~1.0086 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold262144-rounds10 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold262144-rounds30 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold262144-rounds30 | 100,000 | 0.01 | pre-copy | 262144 | 30 | 3 | 237 | 1.0083~1.0086 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold262144-rounds30 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold1048576-rounds3 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold1048576-rounds3 | 100,000 | 0.01 | pre-copy | 1048576 | 3 | 3 | 237 | 1.0083~1.0086 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold1048576-rounds3 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold1048576-rounds10 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold1048576-rounds10 | 100,000 | 0.01 | pre-copy | 1048576 | 10 | 3 | 237 | 1.0083~1.0086 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold1048576-rounds10 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.01-threshold1048576-rounds30 | 100,000 | 0.01 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.01-threshold1048576-rounds30 | 100,000 | 0.01 | pre-copy | 1048576 | 30 | 3 | 237 | 1.0083~1.0086 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold1048576-rounds30 | 100,000 | 0.01 | post-copy | — | — | 1 | 271 | 1.1576~1.1580 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds3 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds3 | 100,000 | 0.10 | pre-copy | 65536 | 3 | 2 | 256~257 | 1.0843~1.0855 | 2 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold65536-rounds3 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds10 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds10 | 100,000 | 0.10 | pre-copy | 65536 | 10 | 2 | 256~257 | 1.0843~1.0855 | 2 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold65536-rounds10 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds30 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds30 | 100,000 | 0.10 | pre-copy | 65536 | 30 | 2 | 256~257 | 1.0843~1.0855 | 2 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold65536-rounds30 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold262144-rounds3 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold262144-rounds3 | 100,000 | 0.10 | pre-copy | 262144 | 3 | 3 | 255~256 | 1.0836~1.0849 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold262144-rounds3 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold262144-rounds10 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold262144-rounds10 | 100,000 | 0.10 | pre-copy | 262144 | 10 | 3 | 255~256 | 1.0836~1.0849 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold262144-rounds10 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold262144-rounds30 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold262144-rounds30 | 100,000 | 0.10 | pre-copy | 262144 | 30 | 3 | 255~256 | 1.0836~1.0849 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold262144-rounds30 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold1048576-rounds3 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold1048576-rounds3 | 100,000 | 0.10 | pre-copy | 1048576 | 3 | 3 | 255~256 | 1.0836~1.0849 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold1048576-rounds3 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold1048576-rounds10 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold1048576-rounds10 | 100,000 | 0.10 | pre-copy | 1048576 | 10 | 3 | 255~256 | 1.0836~1.0849 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold1048576-rounds10 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.1-threshold1048576-rounds30 | 100,000 | 0.10 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.1-threshold1048576-rounds30 | 100,000 | 0.10 | pre-copy | 1048576 | 30 | 3 | 255~256 | 1.0836~1.0849 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold1048576-rounds30 | 100,000 | 0.10 | post-copy | — | — | 1 | 258 | 1.1010~1.1025 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds3 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds3 | 100,000 | 0.50 | pre-copy | 65536 | 3 | 6 | 353~354 | 1.5010~1.5041 | 3 | 0/5/0 |
| S3-sensitivity-rho0.5-threshold65536-rounds3 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds10 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds10 | 100,000 | 0.50 | pre-copy | 65536 | 10 | 2 | 356~359 | 1.5132~1.5183 | 5~6 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold65536-rounds10 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds30 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds30 | 100,000 | 0.50 | pre-copy | 65536 | 30 | 2 | 356~359 | 1.5132~1.5183 | 5~6 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold65536-rounds30 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold262144-rounds3 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold262144-rounds3 | 100,000 | 0.50 | pre-copy | 262144 | 3 | 6 | 353~354 | 1.5010~1.5041 | 3 | 0/5/0 |
| S3-sensitivity-rho0.5-threshold262144-rounds3 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold262144-rounds10 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold262144-rounds10 | 100,000 | 0.50 | pre-copy | 262144 | 10 | 3~4 | 355~356 | 1.5096~1.5126 | 4 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold262144-rounds10 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold262144-rounds30 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold262144-rounds30 | 100,000 | 0.50 | pre-copy | 262144 | 30 | 3~4 | 355~356 | 1.5096~1.5126 | 4 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold262144-rounds30 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold1048576-rounds3 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold1048576-rounds3 | 100,000 | 0.50 | pre-copy | 1048576 | 3 | 12 | 348~349 | 1.4822~1.4855 | 2 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold1048576-rounds3 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold1048576-rounds10 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold1048576-rounds10 | 100,000 | 0.50 | pre-copy | 1048576 | 10 | 12 | 348~349 | 1.4822~1.4855 | 2 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold1048576-rounds10 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.5-threshold1048576-rounds30 | 100,000 | 0.50 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.5-threshold1048576-rounds30 | 100,000 | 0.50 | pre-copy | 1048576 | 30 | 12 | 348~349 | 1.4822~1.4855 | 2 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold1048576-rounds30 | 100,000 | 0.50 | post-copy | — | — | 1 | 215 | 0.9159~0.9180 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds3 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds3 | 100,000 | 0.90 | pre-copy | 65536 | 3 | 28~29 | 479~483 | 2.0489~2.0602 | 3 | 0/5/0 |
| S3-sensitivity-rho0.9-threshold65536-rounds3 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds10 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds10 | 100,000 | 0.90 | pre-copy | 65536 | 10 | 4~5 | 542~545 | 2.3035~2.3187 | 10 | 0/5/0 |
| S3-sensitivity-rho0.9-threshold65536-rounds10 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds30 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds30 | 100,000 | 0.90 | pre-copy | 65536 | 30 | 4 | 548~555 | 2.3260~2.3461 | 12~14 | 0/0/5 |
| S3-sensitivity-rho0.9-threshold65536-rounds30 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold262144-rounds3 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold262144-rounds3 | 100,000 | 0.90 | pre-copy | 262144 | 3 | 28~29 | 479~483 | 2.0489~2.0602 | 3 | 0/5/0 |
| S3-sensitivity-rho0.9-threshold262144-rounds3 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold262144-rounds10 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold262144-rounds10 | 100,000 | 0.90 | pre-copy | 262144 | 10 | 5 | 539~542 | 2.2950~2.3066 | 9~10 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold262144-rounds10 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold262144-rounds30 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold262144-rounds30 | 100,000 | 0.90 | pre-copy | 262144 | 30 | 5 | 539~542 | 2.2950~2.3066 | 9~10 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold262144-rounds30 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold1048576-rounds3 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold1048576-rounds3 | 100,000 | 0.90 | pre-copy | 1048576 | 3 | 28~29 | 479~483 | 2.0489~2.0602 | 3 | 0/5/0 |
| S3-sensitivity-rho0.9-threshold1048576-rounds3 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold1048576-rounds10 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold1048576-rounds10 | 100,000 | 0.90 | pre-copy | 1048576 | 10 | 14~15 | 511~516 | 2.1832~2.1980 | 5 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold1048576-rounds10 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho0.9-threshold1048576-rounds30 | 100,000 | 0.90 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho0.9-threshold1048576-rounds30 | 100,000 | 0.90 | pre-copy | 1048576 | 30 | 14~15 | 511~516 | 2.1832~2.1980 | 5 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold1048576-rounds30 | 100,000 | 0.90 | post-copy | — | — | 1 | 187 | 0.7942~0.7967 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds3 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds3 | 100,000 | 1.20 | pre-copy | 65536 | 3 | 56~57 | 581~583 | 2.4847~2.4940 | 3 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold65536-rounds3 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds10 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds10 | 100,000 | 1.20 | pre-copy | 65536 | 10 | 22~23 | 797~805 | 3.4068~3.4354 | 10 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold65536-rounds10 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds30 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds30 | 100,000 | 1.20 | pre-copy | 65536 | 30 | 11~12 | 954~1021 | 4.0587~4.3227 | 21~26 | 0/0/5 |
| S3-sensitivity-rho1.2-threshold65536-rounds30 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold262144-rounds3 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold262144-rounds3 | 100,000 | 1.20 | pre-copy | 262144 | 3 | 56~57 | 581~583 | 2.4847~2.4940 | 3 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold262144-rounds3 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold262144-rounds10 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold262144-rounds10 | 100,000 | 1.20 | pre-copy | 262144 | 10 | 22~23 | 797~805 | 3.4068~3.4354 | 10 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold262144-rounds10 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold262144-rounds30 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold262144-rounds30 | 100,000 | 1.20 | pre-copy | 262144 | 30 | 11~12 | 954~1021 | 4.0587~4.3227 | 21~26 | 0/0/5 |
| S3-sensitivity-rho1.2-threshold262144-rounds30 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold1048576-rounds3 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold1048576-rounds3 | 100,000 | 1.20 | pre-copy | 1048576 | 3 | 56~57 | 581~583 | 2.4847~2.4940 | 3 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold1048576-rounds3 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold1048576-rounds10 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold1048576-rounds10 | 100,000 | 1.20 | pre-copy | 1048576 | 10 | 22~23 | 797~805 | 3.4068~3.4354 | 10 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold1048576-rounds10 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |
| S3-sensitivity-rho1.2-threshold1048576-rounds30 | 100,000 | 1.20 | stop-and-copy | — | — | 234 | 235 | 1.0004 | — | — |
| S3-sensitivity-rho1.2-threshold1048576-rounds30 | 100,000 | 1.20 | pre-copy | 1048576 | 30 | 17 | 867~893 | 3.6979~3.7941 | 14~15 | 5/0/0 |
| S3-sensitivity-rho1.2-threshold1048576-rounds30 | 100,000 | 1.20 | post-copy | — | — | 1 | 171~172 | 0.7266~0.7287 | — | — |

## 범위와 제한

가상 시간·인메모리 모델이며 실제 서버 latency·throughput은 측정하지 않았다. 쓰기 차단은 누락 key DELETE 제어 요청의 BUSY tick 수다. 전송량은 migration 데이터 응답 framing을 포함하고 ACK·pull 요청과 client 통신 bytes는 제외한다. rho는 쓰기 시도당 152 bytes 기준이며 DELETE·중복 쓰기로 실제 dirty bytes와 다를 수 있다. NOT_SHRINKING은 이 모델의 관측 결과이며 보편적인 수렴 한계가 아니다.

quick은 N=1,000, B=4 KiB/tick과 임계 4/16/64 KiB를 쓰는 재현 검증용이다. full은 N=100,000, B=64 KiB/tick과 임계 64/256/1,024 KiB다. 두 결과를 합산하지 않는다. phaseEvents는 상태·라운드 경계이며 dirty generation별 상세 byte 원본을 대체하지 않는다. S6·E/M scaling·실제 KV/peer 통합은 포함하지 않는다.
