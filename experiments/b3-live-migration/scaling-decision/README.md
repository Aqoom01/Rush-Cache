# 노드 수 판단 7개: 배치를 고정한 B3 비교

2026-11-05. 기존 실험은 증감 판단과 시스템별 배치·분할·복제를 함께 비교했다. 그 결과를 삭제하고, **배치를 하나로 고정한 노드 수 판단 비교**로 교체한다. Slicer는 노드 수를 정하지 않으므로 제외한다. CAMIG 목적지 선택, Accordion MILP 재배치, E-Store 세분화, SCADS 복제, PAX partition 선택·hint도 실행하지 않는다.

## 무엇만 바꾸는가

| 방법 | 남긴 수 판단 | 원문에서 제외·변경한 부분 |
|---|---|---|
| SCADS | logistic 성능 경계 위반 시 증설, 용량이 허용하면 통합할 노드 수 탐색 | hottest-bin/best-fit·읽기 복제를 없애고 공통 배치로 각 후보의 성능 경계를 검사 |
| ElastMan | logistic feedforward, PI feedback·deadband·정수 노드 수 | 실행 배치는 공통 adapter |
| E-Store | 평균 CPU 상·하한에 따른 한 노드 추가/제거 | hot-key 승격·세분화·Greedy Extended 제외 |
| Accordion | 증가/감소 추세에 따른 서버 수 후보 선택, 용량 조건 | 후보 판정에 공통 배치 사용. MILP 최적 배치·최소 이동량 목적 제외 |
| P-Store | 과거 이력 SPAR, 시간×노드 수 DP, 준비·이동 비용 | DP 내부 이동 시간은 균등 이동 추정이며 실행은 공통 배치. 실제 완료 시간과 차이 가능 |
| TIRAMOLA | 노드 수 상태, load-slice centroid 보상, Q backup | 가상 별도 학습·결정론적 전이의 연구 변형, 실제 VM 탐색 제외 |
| PAX | 과거 ARIMA(1,1,1), 예측 평균 CPU 범위로 수 계산 | 단일 owner의 수 판단 변형. 기존 RF=3·휴면 replica·partition 점수·CL·hint 동기화 제외 |

원문 출처는 [선행 시스템 조사](../references/elastic-scaling-algorithms.md)를 따른다. **각 시스템의 수 판단을 공통 실행 조건에 적용한 연구 변형**이며 원 시스템 전체 재현이 아니다. 특히 SCADS·Accordion의 판단은 배치 가능성과 결합돼 있어 원문의 배치 해법을 제거하면 원형과 동일한 알고리즘으로 주장할 수 없다. PAX도 휴면 replica 활성화 시스템 자체의 성능을 평가하지 않는다.

## 모든 방법에 고정한 것

- 같은 외부 요청열·과거 이력·독립 calibration을 사용한다. 정책에는 평가 미래 요청열을 주지 않는다.
- 32 keys, key당 256 모형 bytes, **4 keys씩 8개 Tablet**, Tablet당 owner 1개. 분할·병합·복제는 없다.
- 초기 4·최소 2·최대 8노드, 노드당 모형 메모리 8 KiB. 기한 0.12 가상 시간, read/write service 0.01/0.02, write 10%.
- 배치는 fixed-tablet-lpt-v1 하나다. 수가 바뀔 때 Tablet 작업량 내림차순으로 메모리 여유가 있는 최소 부하 노드에 배치한다. 동률이면 기존 owner·작은 ID 순이다. 노드 ID 집합은 0부터 목표 수 미만이다.
- **수가 같으면 현재 배치를 유지한다.** hotspot을 이유로 별도 재배치하지 않는다. 같은 배치 규칙이 같은 배치 결과를 항상 뜻하지는 않는다. 정책마다 선택한 노드 수와 시점은 다를 수 있다.
- 4 window마다 판단, 완료 후 2 window cooldown, 진행 중 재판단 보류. 준비 2 window·전송 2 KiB/window·전송 중 service penalty 15%. 전송 뒤 새 요청을 새 배치로 전환하고 제거 노드의 기존 요청을 drain한다.
- 동일 규칙으로 배치하지 못하면 거부·유지한다. CPU 과부하를 split으로 숨기지 않는다. 분할 불가능한 hotspot은 노드를 늘려도 해소하지 못할 수 있다.

이 adapter는 실험 조건을 맞추는 도구다. B2의 배치 알고리즘을 경쟁 평가하거나 B2 기능을 완료한 결과가 아니다. 실제 B2 controller·공통 서비스 통합도 필요하지 않다.

## 조건·실행 수

step, spike, periodic, low, hot_range, moving_hotspot, hot_key의 7조건을 사용한다. 요청은 Java의 고정 seed Poisson·window 내 도착 생성기다. 실제 YCSB·운영 trace가 아니다.

| 프로필 | 한 반복 | 두 반복 |
|---|---:|---:|
| Full: 7판단+고정 4노드 대조군 × 7조건 × 5seed | 280 | 560 |
| Quick: 같은 8방법 × 7조건 × 2seed | 112 | 224 |

Full 96·Quick 48 window. 같은 seed 반복은 재현 확인이며 독립 표본으로 추가하지 않는다. [현재 결과](results.md)와 [감사 기록](../java/results.md)을 따른다.

    .\gradlew.bat :cache-core:b3Research "-Pb3.args=literature --profile full --repeat 2 --output experiments/b3-live-migration/scaling-decision/runs"

schema=b3-node-count-fixed-placement-java-1. 원본에는 공통 배치 ID·입력 hash·결정·실제 계획·window·미완료 action을 저장한다. 재생성 시 모든 계획이 같은 adapter의 결과인지 다시 계산하고, split/merge/복제·같은 수의 재배치가 섞이면 거절한다. 이전 결합형 schema는 새 결과로 재생성할 수 없다.
