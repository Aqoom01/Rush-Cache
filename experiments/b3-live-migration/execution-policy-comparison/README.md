# 고정 배치의 증감 준비 지연 비교

이전 Accordion 최적 재배치/CA 목적지 선택 비교는 B2와 겹치므로 결과와 실행 항목을 삭제했다. 현재는 **AWS warm-up·ElastMan·P-Store의 수 판단**을 동일 fixed-tablet-lpt-v1 실행기에 적용해 준비·전송 지연만 비교한다. 분할·병합·복제·목적지 선택 알고리즘을 추가 변수로 곱하지 않는다.

Full은 4부하(step/spike/periodic/low) × 3준비(0/4/12) × 2전송(256/2,048 bytes/window) × 2penalty(0/0.30) × 3판단 × 5seed = **720회/반복**, 두 반복 1,440회다. Quick은 96회/반복, 두 반복 192회다. 같은 seed 반복은 재현 검증이다.

실행은 실제 노드가 아닌 Java FCFS·준비·전송 모형이며 가상 목표 기한 0.12를 사용한다. 미완료 action·준비·drain 중 노드도 기록한다. 원문 AWS 전체 집계·동시 scale-out, ElastMan 재배치 전용 FFR 제어기·실제 P-Store 제품을 재현하지 않는다.

    .\gradlew.bat :cache-core:b3Research "-Pb3.args=policy --profile full --repeat 2 --output experiments/b3-live-migration/execution-policy-comparison/runs"

schema=b3-count-delay-fixed-placement-java-1. 원본에서 입력·설정·집계뿐 아니라 모든 action의 배치가 공통 adapter의 결과인지 검증한다. 과거 archive의 배치 oracle reader는 호환 회귀 fixture에만 쓰며 실행 가능한 비교군으로 노출하지 않는다.

[현재 수치·보고서](../execution-validation.md) · [전체 재실행과 원본 감사](../java/results.md)
