# 동시 통합 캐시 실험 계획

목표는 각 모듈 단독의 최선이 아니라 **A1+A2+A3+B1+B2+B3를 함께 실행했을 때**의 방법론 선택과 상호작용을 찾는 것이다. A1×A2를 제외 관계로 취급하지 않는다. 기본 실행 구조와 측정 한계는 [범위](../FULL_SYSTEM_SCOPE.md)를 따른다.

## 실행 순서

1. `smoke`: A1 7개 × A2의 읽기 모드 포함9변형 × copy 3개=189개 조합에서 실제 캐시 저장/조회/삭제·증감·이동·최종 audit을 확인한다. Java TCP fixture의 189개 조합 검사는 독립 JVM/EC2 189회 성능 실험과 구분한다.
2. `coverage`: A1, A2, A3 전체 프로필, V06 읽기 정책, B1, B2 split/placement, B3 scaler/scheduler/copy/drain을 독립 축으로 모든 호환 가능한 **쌍**이 등장하는 기능 입력을 생성한다. workload UNIFORM/HOT_RANGE, seed 1/2/3을 반복한다. P-Store 등 필요한 학습 자료는 실제 history로 제공한다.
3. 성능 후보 선정: 동일 workload·key/value 크기·rate·seed·topology에서 p50/p99, 성공/실패/미수락률, 복구 시간, 실제 CPU/RSS/IO/TCP bytes, 이동 시간·bytes·dirty rounds를 비교한다. A1 승인 RPC와 A2 데이터 RPC의 비용을 포함한다. 낮은 부하부터 포화 부하까지 rate/concurrency를 올리고 값 크기·읽기/쓰기 비율·hot-key 비율을 바꾼다. calibration도 A1+A2 조합별로 수집한다. 실패/SAFE_HOLD 실행은 빠른 성공으로 제외하지 말고 실패율과 함께 보고한다.
4. 상호작용 재실험: 상위 후보들을 `--a1s`, `--a2s`와 B shortlist로 고정하고 조합을 전수 실행한다. A1×A2, A2 batch×B3 copy, A3×A1 failover, B1/B2×B3 이동의 순위 역전을 확인한다. 다른 축을 고정한 paired seed 비교와 반복 분산을 제공한다.
5. 장애: 같은 조합에서 정상, leader/observer pause, 이동 중 영구 crash를 비교한다. `--fault-layer`로 선택 기준을 기록하고 실제 종료는 co-located 두 계층을 함께 멈춘다는 점을 명시한다. 계층 단독 장애는 별도 검사다.
6. 결론: workload별 유효 조합과 선택 이유를 보고한다. 보편적인 단일 우승자를 미리 가정하지 않는다. 데이터/로그 안전성의 native 한계, reference calibration 사용 여부와 논리 scale 비용을 함께 제시한다.

## 판정 기준

- 필수 정합성: 성공한 SET 값과 tombstone 보존, stale read/유실/잘못된 ownership 공개 없음. A1에 초기 및 최종 B1 snapshot이 실제로 기록되어야 한다.
- 동시 실행 증거: gateway의 A1 admissions/dataCalls 모두 양수, 각 프로세스 양쪽 native status/FD 기록. 한 A 계층이 빠진 legacy 결과는 제외한다.
- B3 증거: 목표 소유권/active 수, 완료/미해결 이동, target read barrier, drain 상태를 함께 검사한다. SAFE_HOLD를 완료로 집계하지 않는다.
- 정책 증거: non-fixed scaler의 실제 scalingDecisions, 실제 관찰 창과 선택 이유를 남긴다. TRAINING/OBSERVING은 정책 판단 성공이 아니다.
- 수집 무결성: JAR/tool/input SHA, 요청 CSV terminal count·ID·p99 재계산, audit 해시, fault 확인, 소유 PID 종료 확인을 남긴다.
- EC2 결과는 실제 host 환경과 calibration으로만 설명한다. 로컬 공유 호스트의 수치를 EC2 성능으로 보고하지 않는다.

전체 호환 기본 Cartesian은 전략 61,419,708개이며 workload2×seed3을 곱하면368,518,248개다. `matrix --count-only`로 먼저 확인한다. coverage는 전수 Cartesian이나 모든 조합의 안전성 증명이 아니다.

`batch`의 성공 판정은 두 계층 실행과 FD runtime 증거도 포함한다. FD 기능 smoke의 단축 timeout을 제품 기본 설정 성능과 섞지 않는다. 현재 source 범위·퇴역 항목·지원 불가 조건은 [METHOD_AUDIT.md](METHOD_AUDIT.md)에 있다.
