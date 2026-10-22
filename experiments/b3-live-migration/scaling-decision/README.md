# 노드 증감 판단 · E1~E7

**상태: 설계와 결과 저장 위치만 준비됨. 정책·runner·JSON 원본·파생 요약 생성기는 미구현이며 결과는 아직 없다.**

[전체 설계 §6.3·§6.6](../design.md) · [단일 이동 비교](../migration-comparison/README.md) · [B3 안내](../README.md)

이 실험은 같은 metric stream과 초기 노드 상태에서 정책·설정에 따른 증감 결정을 비교한다. 첫 정책은 `target tracking`이고 tolerance·연속 관측 수·stabilization·warm-up 등을 바꾼다. 실제 VM·컨테이너를 만들거나 종료하지 않는다.

| 시나리오 | 확인할 내용 |
|---|---|
| E1 | 지속 부하 증가의 desired node 수·결정 지연 |
| E2 | 짧은 spike에 대한 불필요한 action 억제 |
| E3 | target 주변 진동과 방향 전환 |
| E4 | 지속 저부하의 scale-in 시점·과축소 |
| E5 | warm-up·진행 중 migration의 중복 action 억제 |
| E6 | min/max 범위 보존 |
| E7 | candidate 선택·동률 처리의 결정론 |

## 실행 계약

정식 진입점은 config를 읽는 공통 `experiment-runner`로 하고 test의 `experiment/decision/ScalingDecisionExperiment`를 연구 backend로 호출할 계획이다. 현재 공통 Runner는 B3 Baseline의 연구 모드 호출을 지원하며 이 실험의 backend는 아직 미구현이다. observation window마다 고정 metric을 공급하고 fake provisioner/membership이 READY·DRAINING·migration 완료를 재생한다. policy는 실제 시계를 읽지 않는다. metric stream·외부 이벤트·초기 상태의 hash와 generator 버전을 남긴다. 같은 정책의 다른 설정은 candidate ID로 구별한다.

공통 입력 재생·출력 규칙을 재사용하되 단일 이동 fixture나 source-owned commit 조건을 요구하지 않는다. 별도의 fake controller 환경에서 desired node 오차·결정 지연·불필요한 action·방향 전환·범위 위반을 검증한다. 임의 threshold를 실험 결과처럼 기록하지 않는다.

## 생성할 결과

JSON이 지표·세부 관측의 원본이며 CSV·Markdown은 그 원본에서 생성한다. Git에는 각 반복의 `summary.csv`·`results.md`만 보관하고, 원본 JSON·manifest·실행 기록과 그 밖의 파생 CSV는 `.gitignore`로 제외한다. 원본 별도 보관 위치·hash는 manifest에 기록한다. 현재 SC v1 결과에는 사후 추정한 metadata를 덧붙이지 않는다.

새 `runs/<execution_id>/`에 `manifest.json`, `results/<run_id>.json`, `run-summary-<suite_id>.json`과 파생 `cases.csv`, `runs.csv`, `summary.csv`, `decisions.csv`, `results.md`를 생성할 계획이다. 공통 metadata·상태와 파생 long metric 규칙은 [설계 §6.6](../design.md)을 따른다.

`cases.csv`에는 metric·target·tolerance·연속 관측 수·stabilization/warm-up·min/max·초기 노드 수·stream 참조를 기록한다. `decisions.csv`는 run ID·window/tick·입력 metric·현재/원하는 노드 수·KEEP/SCALE_OUT/SCALE_IN·candidate node·이유를 보존한다. 이동 라운드가 없으므로 `rounds.csv`는 생성하지 않는다.

run 폴더의 `results.md`는 동일 입력의 정책·candidate 설정별 지표를 나란히 보여주고 E1~E7별 해석과 검증 실패·누락을 표시한다. 기존 migration Baseline 원본을 이 실험의 결과로 재사용하지 않는다. 실행 명령과 실제 결과 링크는 runner 구현·실행 후 추가한다.

첫 공식 run이 생기면 이 폴더 최상위에 입구 `results.md`(공식 run 지정과 핵심 결과)를 만들고 B3 통합 `../results.md`를 작성하거나 갱신한다. 결과 문서 계층은 [design.md §6.6](../design.md)을 따른다.
