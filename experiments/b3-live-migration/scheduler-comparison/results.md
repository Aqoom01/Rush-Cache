# 목적지가 고정된 Scheduler 9개 결과

2026-11-05. CAMIG 목적지 선택과 후보 선택 대조군을 제거하고 **6조건×9실행 정책×3복사×5seed=810회/반복**으로 새로 실행했다. Full 두 반복 **1,620회·19,440 jobs**, Quick 324회×2=648회다. 반복을 추가 독립 표본으로 합치지 않는다.

[Full 반복 1](runs/run-2026-11-04T15-35-13.088917500Z-4ead3854/repeat-001/results.md) · [조건별 평균·표준편차 CSV](runs/run-2026-11-04T15-35-13.088917500Z-4ead3854/repeat-001/summary.csv) · [반복 2](runs/run-2026-11-04T15-35-13.088917500Z-4ead3854/repeat-002/results.md) · [방법·한계](README.md)

source/destination·Tablet·쓰기·기한 입력을 고정하고 실행 순서·동시 제한·byte 배분만 비교했다. 실제 B3 Stop/Pre/Post-copy로 정합성·owner/epoch·source cleanup·guard·매 tick byte/동시 상한 검사를 통과했다. 목적지 선택 성능을 B3 결과에 포함하지 않는다.

각 조건은 다른 경합을 만들므로 전체 평균 하나로 우승 정책을 정하지 않는다. makespan·job 완료·시작 대기·전송 증폭·BUSY·기한 초과를 full 보고서에서 복사 방식·조건별로 비교한다. 모든 시간은 논리 tick이며 실제 서버 지연·p99가 아니다.

원본 재생성·두 반복 scientific evidence 검증은 [현재 감사](../java/results.md)를 따른다. 이전 CAMIG 포함 855회/342회 run은 삭제했고 현재 결과로 재사용하지 않는다. 실제 노드 N/U/R 시나리오는 실행하지 않았다.
