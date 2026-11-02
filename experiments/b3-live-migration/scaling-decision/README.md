# 노드 증감·재배치 선행 방법 8개 실험

**상태: 논문 핵심 계산의 연구용 prototype 8개를 구현했다.** 실제 OpenKache 서버·원 논문 제품을 구동한 실험이 아니라, 공통 가상 요청·대기열·준비·전송 모형에서 실행한다. 결과와 정식 실행 ID는 [실험 결과](results.md)를 따른다.

[원문별 알고리즘](../references/elastic-scaling-algorithms.md) · [구현](backend/algorithms.py) · [실행 모형](backend/cluster.py) · [설계](../design.md)

## 비교 대상과 구현 범위

| 방법 | 실제로 구현한 계산 | 원문과 다른 부분 |
|---|---|---|
| SCADS Director | get/put logistic 성능 경계, hottest-bin·best-fit, 과대 bin 전용 복제, 저부하 서버 통합 | 원 SCADS·EC2 대신 가상 대기열 calibration과 window 단위 scheduler |
| ElastMan | logistic feedforward, 식 (2)의 PI feedback, deadband·평활화, 목표 서버당 처리량으로 규모 변환 | 가상 읽기 p99, 모형용 PI 이득·정수화, LPT 배치 어댑터 |
| E-Store | CPU 상·하한의 한 노드 증감, hot key 승격·강등, Greedy 후 cold block을 옮기는 Greedy Extended | 32 key에 top-k=4, window CPU 관측, H-Store/Squall 대신 메타데이터·전송 모형 |
| Accordion | 서버 수 후보별 실제 MILP, 부하·메모리·단일 owner 제약, 이동 byte 최소화 | null-affinity·replica 1 조건만 사용. SciPy/HiGHS solver, 실측 대신 calibration 용량 |
| P-Store | 식 (8)의 SPAR, horizon별 최소제곱 적합, 시간×노드 상태의 동적 계획법·첫 move 실행 | 균등 데이터 이동 시간·유효 용량은 공통 모형으로 대체. n=2·m=3·period=48, LPT 실행 배치 |
| TIRAMOLA | 노드 수 상태, load-slice의 최대 cluster centroid 보상, 식 (1)의 Q backup, 온라인 관측 추가 | 가상 별도 학습 sweep, 결정론적 모델 전이의 20회 backup. 실제 VM 탐색·원 학습 일정은 재현하지 않음 |
| PAX | partition 점수에 따른 노드 선택, ARIMA(1,1,1), 목표 CPU 범위 평균 전략, CL 유효성, hint 동기화 | 기존 RF=3·CL=1 가상 휴면 노드. ARIMA 차수 고정·조건부 최소제곱, 현재 부하로 서비스 수요 추정 |
| Slicer | benefit/key-churn 가중치 탐욕 이동, 인접 cold merge, assignment를 유지하는 hot split | 노드 4개 고정·redundancy 1. 32 key 이산 모형, 전체 장애·assignment 프로토콜 제외 |

원문 출처와 절은 [선행 알고리즘 설명](../references/elastic-scaling-algorithms.md)에 연결했다. 방법 이름은 원문의 핵심 메커니즘 출처이며 원 시스템의 완전한 재현을 뜻하지 않는다. 삭제한 자체 세 정책의 이름을 바꿔 사용하지 않는다.

## 공통 모형과 세 비교군

- A: SCADS·ElastMan·E-Store·Accordion·P-Store·TIRAMOLA는 같은 단일 복사 초기 배치에서 시작한다. SCADS만 동적 복제를 사용할 수 있고 E-Store는 key 단위로 세분화한다. 가능한 행동과 입도가 다르다.
- B: PAX와 고정 복제 대조군은 같은 RF=3 데이터·active={0,2,4,6}·CL=1에서 시작한다. inactive 노드에도 원 데이터가 있고 writes의 hint byte를 누적한다.
- C: Slicer와 고정 slice 대조군은 같은 4개 노드에서 시작한다. 노드 증감은 수행하지 않는다.

PAX의 휴면 복귀와 신규 노드 생성, Slicer의 고정 노드 재배치를 같은 증감 순위로 합치지 않는다. 세 대조군은 각각 증감·재배치 없이 초기 상태를 유지한다.

| 항목 | 값·의미 |
|---|---|
| 정식 실행 | 8개 방법+3개 대조군 × 7조건 × seed 1~5 = 385회 |
| 반복 검증 | quick 154회 × 2반복. 같은 seed 반복은 독립 표본에 추가하지 않음 |
| 기간 | full 96, quick 48 virtual window |
| 초기 데이터 | 32 keys × 256 model bytes, 4 keys씩 8개 초기 unit |
| 노드 | 초기 4, 최소 2, 최대 8, 노드당 8 KiB 모형 메모리 |
| 요청 | Poisson 개수·window 안 uniform 도착, 90% read·10% write, 고정 seed |
| 처리 | FCFS 가상 대기열, read 0.01·write 0.02 가상 시간. replica 읽기는 가장 짧은 대기열, 쓰기는 active copies에 부과하고 CL=1 응답 |
| 가상 지연 목표 | 요청 응답 0.12 가상 시간. 실제 ms·p99 SLO가 아님 |
| 제어 | 4 window마다 판단, 완료 후 2 window cooldown, 진행 중 중복 action 보류 |
| 준비·전송 | 신규/휴면 활성 준비 2 window, 일반 이동은 cluster 전체 2 KiB/window, PAX hint는 신규 노드별 2 KiB/window, 전송 중 서비스 비용 15% 증가 |
| 완료 | 필요한 모형 byte 전송 뒤 새 요청을 새 assignment로 전환하고 기존 in-flight drain 뒤 제거 노드를 해제. 가상 KV payload 정합성 검증은 하지 않음 |

계획 생성 중인 새 노드를 즉시 처리 용량으로 사용하지 않는다. 종료 시 pending action과 outstanding request는 실패 없이 완료된 것으로 바꾸지 않고 별도 집계한다. PAX는 수락 이후 새로 발생한 hint도 동기화하므로 실제 model 전송량이 수락 시 계획 byte를 넘을 수 있다.

## 7개 부하 조건

| 조건 | 외부 요청률과 분포 |
|---|---|
| step | 180 → 420 → 85 requests/window, 1/4·3/4 시점 변경 |
| spike | 기본 180, 1/3 시점부터 2 window만 600 |
| periodic | 230+140×sin(2πt/48) |
| low | 180에서 1/4 시점 뒤 65로 유지 |
| hot_range | 총 210, 연속 4 keys에 요청의 70% |
| moving_hotspot | 총 210, 24 window마다 hot 범위를 이동 |
| hot_key | 총 210, 단일 key에 요청의 70% |

모든 방법에 동일 외부 요청열을 주고, 배치·복제·준비·이동에 따라 각자의 관측을 생성한다. 이 요청열은 YCSB 도구로 생성한 benchmark나 실제 운영 trace가 아니다.

## calibration·학습과 미래 정보

`Models`는 별도 seed에서 단일 대기열 부하·read/write mix를 바꾼 165행으로 logistic 경계를 적합한다. TIRAMOLA는 별도 uniform sweep 420행에서 시작하고 실행 중 관측을 추가한다. 모두 모형 calibration이며 실제 서버 성능 측정이 아니다.

SPAR는 평가 시점 이전 6개 주기의 이력으로 horizon별 계수를 적합한다. ARIMA도 과거 64개 관측만 받는다. 평가 미래 요청열은 predictor·정책에 전달하지 않는다. 설정은 이번 모형에 명시한 한 세트이며 원문 기본값이나 실환경 최적값이 아니다.

## 실행

현재 검증한 Python 환경은 Python 3.13.15, NumPy 2.5.3, SciPy 1.18.1, scikit-learn 1.9.0이다. `python` 명령에서 import 가능해야 한다. 이 라이브러리는 연구 backend에서만 사용하며 Java production 의존성은 추가하지 않는다.

```powershell
python -B experiments/b3-live-migration/scaling-decision/backend/test_algorithms.py
.\gradlew.bat build :experiment-runner:installDist
& '.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat' '.\experiments\b3-live-migration\scaling-decision\literature.properties'
& '.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat' '.\experiments\b3-live-migration\scaling-decision\literature-quick.properties'
```

공통 Runner가 반복·제한 시간·소스/클래스 hash를 관리하고 `LiteratureResearchMain`이 Python backend를 시작한다. 실행 중 backend 소스·classpath를 변경하지 않는다. 15개 Python 알고리즘 테스트는 별도 명령이며 Gradle의 480개 Java 테스트에 포함되지 않는다.

## 결과와 재생성

schema는 `b3-literature-algorithms-json-1`이다. 각 `repeat-*`에 다음을 보관한다.

- `inputs/`: 조건·seed별 실제 외부 요청열과 과거 이력
- `calibration.json`: 독립 calibration·학습 관측과 model 계수
- `results/*.json`: 방법·조건·seed별 관측 window·결정·assignment·완료·집계·provenance
- `summary.csv`, `results.md`: 원본에서 재생성한 seed 평균·표준편차와 제한

```powershell
python -B experiments/b3-live-migration/scaling-decision/backend/experiment.py rebuild '<repeat 폴더>' full
```

원본 검증은 조합 누락·중복, 요청열/과거 이력 hash, evidence checksum, 설정·메타데이터, 주요 집계 일치를 확인한다. 매 실행 메타데이터 범위·메모리·단일 range 보존·노드 준비와 drain을 검사한다. 이는 실제 KV 값의 정합성·네트워크 장애 검증이 아니며 모든 모델 지표의 독립 재실행 검증도 아니다. Git에는 반복별 요약과 보고서를 보관하고, JSON·입력·로그는 실행한 PC에 남긴다.
