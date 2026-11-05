# 실제 노드 N/U/R 전체 흐름: 계획만 작성

**PLANNED_NOT_RUN. 서버·부하·장애·실험은 실행하지 않았다.** 사용자 실행 지시 전에는 이 계획으로 실험하지 않는다. 아래 3흐름만 사용한다.

공통 흐름: observe→decide→prepare→plan→schedule→copy→publish→settle→scale_in→drain→remove→observe_again.

- N: 기본 정상 흐름. 전체 처리 단계, 강제 장애 없음.
- U: 자주 발생하는 불안정. U01~U07을 매 실행에 모두 포함.
- R: 가끔 발생하는 불안정. U01~U07과 R01~R07을 매 실행에 모두 포함.

문제의 부분집합을 따로 열거하지 않는다. 모든 조건을 한 순간에 몰아넣지 않고 사전 정의한 구간·순서로 발생시킨다. 예방/해당 없음·미발생도 명시하고 안전 보류를 성공적인 노드 제거로 세지 않는다.

| U 문제 | 내용 |
|---|---|
| U01 | 부하 spike·증감 진동 |
| U02 | 이동하는 부하 편향 |
| U03 | dirty 감소 뒤 재증가 |
| U04 | 노드 READY 지연 |
| U05 | 전송 지연·변동 |
| U06 | source/destination 경합 |
| U07 | 축소 시 용량 단편화·배치 거부 |

| R 문제 | 내용 |
|---|---|
| R01 | publish 전 source crash |
| R02 | publish 전 destination crash |
| R03 | publish 후 source partition |
| R04 | publish 후 destination crash |
| R05 | controller 재시작 |
| R06 | 부분 cleanup 실패·재시도 |
| R07 | 중복·오래된 메시지 |

## B3만의 조합

배치는 fixed-tablet-lpt-v1 하나로 고정한다. Slicer·CAMIG·Accordion 재배치·split/merge·복제·native hint를 비교 변수에서 제거했다. 판단 7개는 SCADS/ElastMan/E-Store/Accordion/P-Store/TIRAMOLA/PAX의 count-only 변형이다. 원형 변경점은 [판단 비교 명세](../scaling-decision/README.md)를 따른다.

7판단 × 9실행 Scheduler × 3복사 × 2drain = **378설정/흐름**. N/U/R 합계 1,134설정 × workload seed5 = **5,670회/환경의 계획**이다. 기존 21,870/25,380 계획 수는 폐기한다. AWS warm-up은 별도 준비 지연 참고 비교이며 이 7판단 계획 수에 더하지 않는다.

[설정표](configurations.csv) · [예정 실행표](runs-per-environment.csv) · [문제 배정](stories.csv) · [계획 집계](plan-summary.json)

    .\gradlew.bat :cache-core:b3 "-Pb3.args=--plan experiments/b3-live-migration/real-node-plan"

이 명령은 CSV/JSON만 생성한다. 실제 TCP/SSD/EBS, RTT·경합, 실제 p95/p99·downtime·처리량, 프로세스 재시작·metadata 영속성의 adapter와 계측은 별도 준비가 필요하다. 현재 가상 실험이 이 실서버 흐름을 검증한 결과는 아니다. 공통 서비스 통합을 개인 B3의 필수 과제로 추가하지 않는다.
