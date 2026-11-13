# 통합 가상 클러스터 screening 결과

실제 알고리즘을 단일 thread·메모리 fixture에서 실행했다. 시간은 virtual, byte는 model-estimated다. 실서버 성능 순위가 아니다.

screening 1440회, 독립 seed 재검증 170회.

선정 기준은 모든 screening seed에서 유실·stale read·오판 DEAD가 0이고 계획한 이동이 완료된 조합 중 평균 goodput 최대다. crash는 쓰기 재개 관측도 필요하다. 동률은 ID 순서다. 지연·비용은 별도로 공개한다. 다중 비교 보정 없는 탐색이므로 전역 최적을 증명하지 않는다.

| 시나리오 | screening 선택 | 독립 seed safe | goodput (virtual req/s) | 해석 |
|---|---|---:|---:|---|
|uniform|PB-SWIM-SLOT-NONE-HYSTERESIS-STOP|5/5|315.233333|후보 유지; 원인·CI는 confirmation-effects.csv 확인|
|hotspot|PB-SWIM-RANGE-HEAT-HYSTERESIS-STOP|5/5|309.500000|독립 seed에서 더 나은 이웃 있음: 선정 불안정|
|crash-overlap|PB-SWIM-RANGE-HEAT-HYSTERESIS-STOP|5/5|305.533333|독립 seed에서 더 나은 이웃 있음: 선정 불안정|
|jitter|PB-LIFEGUARD-RANGE-HEAT-COST-STOP|5/5|301.300000|후보 유지; 원인·CI는 confirmation-effects.csv 확인|

전체 screening safe 1260/1440회. seed별 원본 수치는 summary.csv, 반복 집계는 aggregate.csv, 다른 축을 고정한 효과는 conditional-effects.csv에 있다.

A2 IONIA, VR/Chain/Rosé의 A3 어댑터, FD Lab의 다른 detector, post-copy, 증감 판단·병렬 scheduler는 이 실행 경로에 연결되지 않았다. A1/A2는 동일 복제 계층의 대안이며 동시에 활성화하지 않는다.

2개 논리 shard 그룹 × 고정 replica 3개, GET/SET, 96 key, 240~256 byte value, 6초 측정, 3ms마다 요청, 1000ms에 한 번 관측·분할·배치, 이동은 직렬이다. 요청 충돌은 BUSY로 센다. physical node별 egress 1024 byte/ms를 복제·FD·이동이 공유한다. 목적지 staging은 복제 엔진으로 순차 SET 확정 후 게시하며 이 비용도 포함한다. source 이전 값은 실행 종료까지 보관한다. GC/reclaim, SSD, 실제 CPU·네트워크, 재시작·분단·멤버십 합의는 측정하지 않았다.

노드의 로컬 KV 연산은 각각 1000μs의 같은 직렬 처리 큐를 사용한다. 이 처리 시간은 실측 CPU 비용이 아닌 실험 가정이다. cutover 경계에서 수락된 source 요청을 배출한 뒤 final delta를 만든다.

안전성 검사는 마지막 OK 값보다 오래된 값을 유실/stale로 센다. TIMEOUT 후 적용될 수 있는 더 최신 발행 값은 허용한다. 전 연산 이력의 선형화 가능성 증명은 아니다.

추가 검증: 더 긴 부하·key/value/네트워크 민감도, 독립 seed 확대, SLO별 Pareto 분석, A2 연결, 실제 TCP/SSD 실험. 현재 결과로 여섯 모듈 전체의 최선 조합을 확정하지 않는다.
