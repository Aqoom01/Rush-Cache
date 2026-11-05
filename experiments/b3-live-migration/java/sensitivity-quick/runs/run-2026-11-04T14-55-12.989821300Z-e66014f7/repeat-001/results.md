# S3 pre-copy 설정 민감도 실험

실행 정보: `run-2026-11-04T14-55-12.989821300Z-e66014f7` · 반복 1/2 · 설정 `sensitivity-quick` · commit `55d3c220bfcfdf5caeb384e573af0991277b0b49` · source hash `3b848270f14c`

쓰기율 5종 × 종료 임계 3종 × 라운드 상한 3종 = 45조건 × seed 5개 × 세 방식, 총 675회. 정합성 위반 0건, 최종 데이터 675/675 일치.

표는 seed 5개의 최솟값~최댓값이다. 종료 사유는 THRESHOLD/ROUND_LIMIT/NOT_SHRINKING 순서의 실행 건수다. 종료 임계와 라운드 상한은 pre-copy에만 적용하며 다른 방식은 —로 표시한다. 조건 ID에는 비교 상대의 설정이 포함된다.

같은 rho/seed는 모든 설정과 방식에 동일한 초기 데이터·tick별 요청 **시도**를 재생한다. workloadHash는 알고리즘 설정을 제외한 생성 규격 hash이며 원본 trace 전체 hash가 아니다. 기존 inputHash는 설정을 포함한 규격 hash로 유지한다. 설정별 완료 시점·성공 쓰기 수가 달라 같은 성공 작업량의 성능 비교로 해석하지 않는다.

| 조건 | N | rho | 방식 | 임계 bytes | 라운드 상한 | 쓰기 차단 tick | 전체 tick | 전송량/초기 크기 | live 라운드 | 종료 사유 T/R/N |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|
| S3-sensitivity-rho0.01-threshold4096-rounds3 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold4096-rounds3 | 1,000 | 0.01 | pre-copy | 4096 | 3 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold4096-rounds3 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold4096-rounds10 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold4096-rounds10 | 1,000 | 0.01 | pre-copy | 4096 | 10 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold4096-rounds10 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold4096-rounds30 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold4096-rounds30 | 1,000 | 0.01 | pre-copy | 4096 | 30 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold4096-rounds30 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold16384-rounds3 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold16384-rounds3 | 1,000 | 0.01 | pre-copy | 16384 | 3 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold16384-rounds3 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold16384-rounds10 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold16384-rounds10 | 1,000 | 0.01 | pre-copy | 16384 | 10 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold16384-rounds10 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold16384-rounds30 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold16384-rounds30 | 1,000 | 0.01 | pre-copy | 16384 | 30 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold16384-rounds30 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds3 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds3 | 1,000 | 0.01 | pre-copy | 65536 | 3 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold65536-rounds3 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds10 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds10 | 1,000 | 0.01 | pre-copy | 65536 | 10 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold65536-rounds10 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds30 | 1,000 | 0.01 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.01-threshold65536-rounds30 | 1,000 | 0.01 | pre-copy | 65536 | 30 | 2 | 41 | 1.0080~1.0105 | 0 | 5/0/0 |
| S3-sensitivity-rho0.01-threshold65536-rounds30 | 1,000 | 0.01 | post-copy | — | — | 1 | 45~46 | 1.1552~1.1621 | — | — |
| S3-sensitivity-rho0.1-threshold4096-rounds3 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold4096-rounds3 | 1,000 | 0.10 | pre-copy | 4096 | 3 | 2 | 44~45 | 1.0846~1.0941 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold4096-rounds3 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold4096-rounds10 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold4096-rounds10 | 1,000 | 0.10 | pre-copy | 4096 | 10 | 2 | 44~45 | 1.0846~1.0941 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold4096-rounds10 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold4096-rounds30 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold4096-rounds30 | 1,000 | 0.10 | pre-copy | 4096 | 30 | 2 | 44~45 | 1.0846~1.0941 | 1 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold4096-rounds30 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold16384-rounds3 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold16384-rounds3 | 1,000 | 0.10 | pre-copy | 16384 | 3 | 4~5 | 43~44 | 1.0759~1.0822 | 0 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold16384-rounds3 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold16384-rounds10 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold16384-rounds10 | 1,000 | 0.10 | pre-copy | 16384 | 10 | 4~5 | 43~44 | 1.0759~1.0822 | 0 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold16384-rounds10 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold16384-rounds30 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold16384-rounds30 | 1,000 | 0.10 | pre-copy | 16384 | 30 | 4~5 | 43~44 | 1.0759~1.0822 | 0 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold16384-rounds30 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds3 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds3 | 1,000 | 0.10 | pre-copy | 65536 | 3 | 4~5 | 43~44 | 1.0759~1.0822 | 0 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold65536-rounds3 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds10 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds10 | 1,000 | 0.10 | pre-copy | 65536 | 10 | 4~5 | 43~44 | 1.0759~1.0822 | 0 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold65536-rounds10 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds30 | 1,000 | 0.10 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.1-threshold65536-rounds30 | 1,000 | 0.10 | pre-copy | 65536 | 30 | 4~5 | 43~44 | 1.0759~1.0822 | 0 | 5/0/0 |
| S3-sensitivity-rho0.1-threshold65536-rounds30 | 1,000 | 0.10 | post-copy | — | — | 1 | 43~44 | 1.0946~1.1063 | — | — |
| S3-sensitivity-rho0.5-threshold4096-rounds3 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold4096-rounds3 | 1,000 | 0.50 | pre-copy | 4096 | 3 | 2~3 | 60~63 | 1.5201~1.5433 | 3 | 3/2/0 |
| S3-sensitivity-rho0.5-threshold4096-rounds3 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold4096-rounds10 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold4096-rounds10 | 1,000 | 0.50 | pre-copy | 4096 | 10 | 2 | 60~64 | 1.5298~1.5560 | 3~4 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold4096-rounds10 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold4096-rounds30 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold4096-rounds30 | 1,000 | 0.50 | pre-copy | 4096 | 30 | 2 | 60~64 | 1.5298~1.5560 | 3~4 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold4096-rounds30 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold16384-rounds3 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold16384-rounds3 | 1,000 | 0.50 | pre-copy | 16384 | 3 | 3~4 | 59~61 | 1.4862~1.5213 | 2 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold16384-rounds3 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold16384-rounds10 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold16384-rounds10 | 1,000 | 0.50 | pre-copy | 16384 | 10 | 3~4 | 59~61 | 1.4862~1.5213 | 2 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold16384-rounds10 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold16384-rounds30 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold16384-rounds30 | 1,000 | 0.50 | pre-copy | 16384 | 30 | 3~4 | 59~61 | 1.4862~1.5213 | 2 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold16384-rounds30 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds3 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds3 | 1,000 | 0.50 | pre-copy | 65536 | 3 | 13~14 | 52~53 | 1.3095~1.3363 | 0 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold65536-rounds3 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds10 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds10 | 1,000 | 0.50 | pre-copy | 65536 | 10 | 13~14 | 52~53 | 1.3095~1.3363 | 0 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold65536-rounds10 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds30 | 1,000 | 0.50 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.5-threshold65536-rounds30 | 1,000 | 0.50 | pre-copy | 65536 | 30 | 13~14 | 52~53 | 1.3095~1.3363 | 0 | 5/0/0 |
| S3-sensitivity-rho0.5-threshold65536-rounds30 | 1,000 | 0.50 | post-copy | — | — | 1 | 36~37 | 0.8966~0.9199 | — | — |
| S3-sensitivity-rho0.9-threshold4096-rounds3 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold4096-rounds3 | 1,000 | 0.90 | pre-copy | 4096 | 3 | 6~7 | 81~85 | 2.0806~2.1873 | 3 | 0/5/0 |
| S3-sensitivity-rho0.9-threshold4096-rounds3 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold4096-rounds10 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold4096-rounds10 | 1,000 | 0.90 | pre-copy | 4096 | 10 | 3~4 | 94~100 | 2.3614~2.5175 | 7~9 | 0/0/5 |
| S3-sensitivity-rho0.9-threshold4096-rounds10 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold4096-rounds30 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold4096-rounds30 | 1,000 | 0.90 | pre-copy | 4096 | 30 | 3~4 | 94~100 | 2.3614~2.5175 | 7~9 | 0/0/5 |
| S3-sensitivity-rho0.9-threshold4096-rounds30 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold16384-rounds3 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold16384-rounds3 | 1,000 | 0.90 | pre-copy | 16384 | 3 | 6~7 | 81~85 | 2.0806~2.1873 | 3 | 0/5/0 |
| S3-sensitivity-rho0.9-threshold16384-rounds3 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold16384-rounds10 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold16384-rounds10 | 1,000 | 0.90 | pre-copy | 16384 | 10 | 5 | 85~89 | 2.1698~2.2930 | 4 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold16384-rounds10 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold16384-rounds30 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold16384-rounds30 | 1,000 | 0.90 | pre-copy | 16384 | 30 | 5 | 85~89 | 2.1698~2.2930 | 4 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold16384-rounds30 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds3 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds3 | 1,000 | 0.90 | pre-copy | 65536 | 3 | 12~13 | 69~71 | 1.7662~1.8270 | 1 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold65536-rounds3 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds10 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds10 | 1,000 | 0.90 | pre-copy | 65536 | 10 | 12~13 | 69~71 | 1.7662~1.8270 | 1 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold65536-rounds10 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds30 | 1,000 | 0.90 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho0.9-threshold65536-rounds30 | 1,000 | 0.90 | pre-copy | 65536 | 30 | 12~13 | 69~71 | 1.7662~1.8270 | 1 | 5/0/0 |
| S3-sensitivity-rho0.9-threshold65536-rounds30 | 1,000 | 0.90 | post-copy | — | — | 1 | 31~33 | 0.7790~0.8093 | — | — |
| S3-sensitivity-rho1.2-threshold4096-rounds3 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold4096-rounds3 | 1,000 | 1.20 | pre-copy | 4096 | 3 | 10~11 | 97~103 | 2.4960~2.6392 | 3 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold4096-rounds3 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold4096-rounds10 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold4096-rounds10 | 1,000 | 1.20 | pre-copy | 4096 | 10 | 6~7 | 130~149 | 3.3186~3.7996 | 8~10 | 0/2/3 |
| S3-sensitivity-rho1.2-threshold4096-rounds10 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold4096-rounds30 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold4096-rounds30 | 1,000 | 1.20 | pre-copy | 4096 | 30 | 6~7 | 130~163 | 3.3186~4.1007 | 8~13 | 0/0/5 |
| S3-sensitivity-rho1.2-threshold4096-rounds30 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold16384-rounds3 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold16384-rounds3 | 1,000 | 1.20 | pre-copy | 16384 | 3 | 10~11 | 97~103 | 2.4960~2.6392 | 3 | 0/5/0 |
| S3-sensitivity-rho1.2-threshold16384-rounds3 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold16384-rounds10 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold16384-rounds10 | 1,000 | 1.20 | pre-copy | 16384 | 10 | 6~7 | 130~149 | 3.3186~3.7996 | 8~10 | 0/2/3 |
| S3-sensitivity-rho1.2-threshold16384-rounds10 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold16384-rounds30 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold16384-rounds30 | 1,000 | 1.20 | pre-copy | 16384 | 30 | 6~7 | 130~163 | 3.3186~4.1007 | 8~13 | 0/0/5 |
| S3-sensitivity-rho1.2-threshold16384-rounds30 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds3 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds3 | 1,000 | 1.20 | pre-copy | 65536 | 3 | 14~17 | 76~93 | 1.9441~2.3883 | 1~2 | 5/0/0 |
| S3-sensitivity-rho1.2-threshold65536-rounds3 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds10 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds10 | 1,000 | 1.20 | pre-copy | 65536 | 10 | 14~17 | 76~93 | 1.9441~2.3883 | 1~2 | 5/0/0 |
| S3-sensitivity-rho1.2-threshold65536-rounds10 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds30 | 1,000 | 1.20 | stop-and-copy | — | — | 39 | 40 | 1.0006 | — | — |
| S3-sensitivity-rho1.2-threshold65536-rounds30 | 1,000 | 1.20 | pre-copy | 65536 | 30 | 14~17 | 76~93 | 1.9441~2.3883 | 1~2 | 5/0/0 |
| S3-sensitivity-rho1.2-threshold65536-rounds30 | 1,000 | 1.20 | post-copy | — | — | 1 | 29~30 | 0.7150~0.7278 | — | — |

## 범위와 제한

가상 시간·인메모리 모델이며 실제 서버 latency·throughput은 측정하지 않았다. 쓰기 차단은 누락 key DELETE 제어 요청의 BUSY tick 수다. 전송량은 migration 데이터 응답 framing을 포함하고 ACK·pull 요청과 client 통신 bytes는 제외한다. rho는 쓰기 시도당 152 bytes 기준이며 DELETE·중복 쓰기로 실제 dirty bytes와 다를 수 있다. NOT_SHRINKING은 이 모델의 관측 결과이며 보편적인 수렴 한계가 아니다.

quick은 N=1,000, B=4 KiB/tick과 임계 4/16/64 KiB를 쓰는 재현 검증용이다. full은 N=100,000, B=64 KiB/tick과 임계 64/256/1,024 KiB다. 두 결과를 합산하지 않는다. phaseEvents는 상태·라운드 경계이며 dirty generation별 상세 byte 원본을 대체하지 않는다. S6·E/M scaling·실제 KV/peer 통합은 포함하지 않는다.

라운드별 dirty 원본·예상/실전송량·ACK·종료 판단은 [dirty 상세](dirty-rounds.md)와 [이벤트 CSV](dirty-rounds.csv)를 따른다.
