# 여러 job의 이동 · M1~M5

**상태: 설계와 결과 저장 위치만 준비됨. 다중 노드 fixture·scheduler·runner·JSON 원본·파생 요약 생성기는 미구현이며 결과는 아직 없다.**

[전체 설계 §6.4·§6.6](../design.md) · [단일 이동 비교](../migration-comparison/README.md) · [B3 안내](../README.md)

B1·B2가 만든 고정 `Change` 목록을 받아 job 수·동시성·공유 예산·endpoint 경합이 migration 실행에 주는 영향을 비교한다. placement나 이동 계획의 품질을 평가하는 실험은 별도다.

| 시나리오 | 바꾸는 조건 |
|---|---|
| M1 | job 수 |
| M2 | 고정 총 전송 예산에서 동시 실행 상한 |
| M3 | 서로 다른 endpoint / source 공유 / destination 공유 |
| M4 | 이동 중 쓰기 부하 |
| M5 | 동일 job·workload·scheduler 조건의 복사 variant |

## 실행 계약

정식 진입점은 config를 읽는 공통 `experiment-runner`로 하고 test의 `experiment/movement/MovementScalingExperiment`를 연구 backend로 호출할 계획이다. 현재 공통 Runner는 B3 Baseline의 연구 모드 호출을 지원하며 이 실험의 backend는 아직 미구현이다. 여러 노드·Tablet을 가진 fixture와 FIFO scheduler를 구현하고 node별·cluster 전체 byte 예산을 활성 job에 나눈다. 같은 입력·seed에서는 job 순서·요청열·경합 배치가 고정돼야 한다.

각 tick에서 node·cluster 예산 합계를 검증하고 job별 정합성 oracle·owner·epoch·진행도를 수집한다. makespan·throughput·대기 시간·공정성은 suite가 집계한다. 노드 2개짜리 현재 Baseline fixture를 여러 job 환경으로 간주하지 않는다.

M5는 단일 이동 비교의 variant·candidate 식별과 동일 입력 규칙을 사용한다. 정합성·실패 계약은 각 variant에 맞게 적용하고 미구현 variant를 임의 수치로 채우지 않는다.

## 생성할 결과

JSON이 지표·세부 관측의 원본이며 CSV·Markdown은 그 원본에서 생성한다. Git에는 각 반복의 `summary.csv`·`results.md`만 보관하고, 원본 JSON·manifest·실행 기록과 그 밖의 파생 CSV는 `.gitignore`로 제외한다. 원본 별도 보관 위치·hash는 manifest에 기록한다. 현재 SC v1 결과에는 사후 추정한 metadata를 덧붙이지 않는다.

새 `runs/<execution_id>/`에 `manifest.json`, `results/<run_id>.json`, `run-summary-<suite_id>.json`과 파생 `cases.csv`, `runs.csv`, `summary.csv`, `jobs.csv`, `budgets.csv`, `results.md`를 만들고 pre-copy 라운드가 있으면 `rounds.csv`를 추가할 계획이다. 원본·요약 계약은 [설계 §6.6](../design.md)을 따른다.

`cases.csv`에는 jobs·max concurrency·endpoint 형태·node/cluster 예산·총 데이터 bytes·rho·고정 Change 목록 참조를 기록한다. `jobs.csv`는 run/job ID·source/destination/Tablet·대기/시작/commit/종료 tick·보낸 bytes·쓰기 차단·결과·정합성 상태를 보존한다. `budgets.csv`는 run ID·tick·node·할당 bytes·소비 bytes를 남겨 예산과 공정성 집계를 검증하게 한다. 라운드 원본에도 job ID를 추가한다.

run 폴더의 `results.md`는 M1~M5별 makespan·처리량·대기 시간·요청 영향·공정성과 검증 결과를 표시한다. 같은 run 폴더에서만 집계하고, 누락된 job을 제외해 전체 완료한 것처럼 보고하지 않는다. 실행 명령과 실제 결과 링크는 runner 구현·실행 후 추가한다.

첫 공식 run이 생기면 이 폴더 최상위에 입구 `results.md`(공식 run 지정과 핵심 결과)를 만들고 B3 통합 `../results.md`를 작성하거나 갱신한다. 결과 문서 계층은 [design.md §6.6](../design.md)을 따른다.
