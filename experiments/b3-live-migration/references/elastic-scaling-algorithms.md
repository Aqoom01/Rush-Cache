# 노드 증감·데이터 재배치: 선행 시스템의 실제 알고리즘

정리일: 2026-11-02. 아래는 논문에 제시된 방법의 설명이다. 이번에는 아래 핵심 계산을 연구용 가상 모형에 구현·실행했다. 원 논문 시스템·실제 OpenKache 서버의 완전한 재현은 아니다. 구현별 생략·변경 범위는 [실험 안내](../scaling-decision/README.md), 실행 결과는 [보고서](../scaling-decision/results.md)를 따른다. 논문의 설정·성능 수치는 해당 환경의 값이다.

## 1. 선행 사례 목록

| 시스템 | 실제 사용한 방법 | 결정 결과 | 주요 전제 |
|---|---|---|---|
| SCADS Director | 성능 모델 + best-fit 탐욕 재배치 | bin 이동·복제와 서버 증감 | 범위별 통계, 복제, 대기 서버 |
| ElastMan | feedforward 모델 + 지연 feedback 제어 | 목표 서버당 처리량과 서버 수 | 실제 p99 지연·처리량, 성능 모델 |
| E-Store | CPU 상·하한 + hot/cold 2단계 배치 | 매 재구성의 노드 증감과 tuple/block 이동 | tuple 접근 통계, 세밀한 배치 |
| Accordion | 서버 수 후보별 MILP | 제약을 만족하는 서버 수와 최소 이동 배치 | 용량·메모리·partition 관계 모델 |
| P-Store | SPAR 부하 예측 + 동적 계획법 | 증감 시점·규모의 연속 일정 | 예측 가능한 부하, 재구성 시간·용량 모델 |
| TIRAMOLA | MDP 기반 보상 평가 | 노드 추가·제거·유지 | 관측 이력, 상태·행동·보상 설계 |
| PAX | partition-aware 노드 선택 + 반응형/ARIMA 제어 | 기존 VM의 활성·휴면 구성 | 데이터를 가진 휴면 복제 노드, hints 동기화 |
| Slicer | 비용 편익 기반 탐욕 이동 + split/merge | slice 담당 task·복제 수·분할 단위 | 서버 수 결정은 외부 담당 |

각 행의 알고리즘은 뒤의 원문 절에 근거한다. 일반적인 부하 지표나 비용식을 추가했다는 이유만으로 특정 논문을 재현했다고 표시하지 않는다.

## 2. SCADS Director — 성능 모델과 best-fit 재배치

**입력:** bin별 get/put 요청률, 현재 서버 배치, 성능 목표 만족 여부를 예측하는 모델, 복사 시간 모델.

**알고리즘:**

1. 주기적으로 모델을 사용해 과부하·저부하 서버를 식별한다.
2. 과부하 서버의 가장 뜨거운 bin을 선택한다.
3. 한 서버가 감당할 수 있으면 성능 목표를 지키면서 받을 수 있는 서버 중 이미 가장 많이 채워진 서버로 이동한다(best-fit).
4. bin 하나의 부하가 한 서버 용량을 넘으면 별도 서버로 옮기고 복제한다.
5. 저부하 서버의 bin을 다른 서버에 모으고, 계획에 따라 서버를 추가·제거한다.
6. scheduler가 복사를 실행한다. 긴 복사는 나눠 실행하고 이전 복사 완료를 기다리는 제약을 둔다.

**평가:** EC2의 SCADS에서 hotspot·일중 부하 변화, 상위 백분위 지연과 사용 서버 수를 관측했다.

**재현 시 구분:** bin은 분할 불가능한 이동 단위다. 복제 없는 단일 owner 구조에서는 과대 bin을 복제해 해결하는 경로를 그대로 사용할 수 없다.

출처: [FAST 2011 원문 §5.3 Algorithm 1, §5.4–5.6, §6](https://www.usenix.org/legacy/events/fast11/tech/full_papers/Trushkowsky.pdf).

## 3. ElastMan — 성능 예측 후 p99 지연으로 보정

**입력:** 서버당 읽기·쓰기 처리량, 실제 읽기 p99 지연, 목표 지연, 사전 벤치마크 모델.

**알고리즘:**

1. 읽기 p99를 평활화하고 목표와의 오차를 구한다. 허용 오차 안이면 유지한다.
2. 처리량 변화가 크면 logistic regression으로 만든 성능 목표 만족/위반 경계를 사용한다(feedforward).
3. 변화가 작으면 지연 오차에 기반한 feedback 제어로 목표 서버당 처리량을 보정한다.
4. 전체 처리량을 목표 서버당 처리량으로 나눠 서버 수로 변환하고 정수화·범위 제한을 적용한다.
5. 저장소 증감·rebalance API를 호출한다. 논문 평가에서는 rebalance 중 제어를 비활성화해 이동 부하를 수요 증가로 오인하지 않도록 한다.

**평가:** OpenStack의 Voldemort, YCSB 기반 client, 일중 변화·spike에서 처리량과 읽기 p99를 측정했다.

**재현 시 구분:** 미래 요청량을 시계열로 예측하는 방식과 다르다. rebalance 중 동작하는 별도 feedforward 모델은 논문에서 후속 과제로 구분한다. 실제 지연 관측 없이 가상 부하를 p99로 대신할 수 없다.

출처: [CAC 2013 원문 §4.1–4.3 Figure 5, §5, §7](https://dcatkth.github.io/papers/a7-al-shishtawy.pdf).

## 4. E-Store — hot tuple 우선, cold block 후속 배치

**입력:** partition CPU 이동 평균, cluster 평균 CPU, tuple별 접근 횟수, global top-k hot tuple, cold block별 부하, 현재 배치.

**알고리즘:**

1. 가벼운 CPU 관측에서 상·하한을 벗어나면 짧은 tuple 관측을 켠다.
2. cluster 평균 CPU가 상한을 넘으면 노드를 추가하고 하한 아래면 제거한다. 논문 구현은 재구성당 노드 한 대씩 변경한다.
3. 새 hot tuple을 블록에서 분리하고, 더 이상 hot하지 않은 tuple은 블록 방식으로 되돌린다.
4. **Greedy:** hot tuple을 접근 빈도순으로 보고, 현재 partition이 평균 부하+허용 폭을 넘으면 가장 낮은 부하의 partition으로 이동한다.
5. **Greedy Extended:** hot tuple 처리 뒤에도 불균형이 남으면 과부하 partition의 부하가 큰 cold block도 저부하 partition으로 보낸다.
6. **First Fit:** 전체 재배치를 구성하는 대안이다. 논문은 정수계획 기반 one-tier/two-tier bin packing도 최적 배치 비교 기준으로 사용한다.

**평가:** H-Store에서 YCSB·Voter·TPC-C, skew와 증감 조건을 바꿔 처리량·평균 지연·계획 계산 시간을 비교했다.

**재현 시 구분:** hot tuple 분리는 Tablet midpoint split과 다르다. 당시 배치 알고리즘은 main memory 제약 반영을 후속으로 남겼다.

출처: [PVLDB 8(3), 2014 원문 §4, §5.1–5.3, §6](https://www.vldb.org/pvldb/vol8/p245-taft.pdf).

## 5. Accordion — 서버 수 후보별 혼합 정수선형계획(MILP)

**입력:** partition 요청률·크기·현재 배치, 서버 메모리, 복제 수, 함께 접근되는 partition의 affinity와 서버 용량 추정 모델.

**알고리즘:**

1. 주기적 또는 큰 workload 변화 시점에 재구성을 검토한다.
2. 부하 변화 방향에 맞춰 현재 규모 주변의 서버 수 후보들을 만든다.
3. 각 후보의 서버 수를 고정하고, 처리 용량·메모리·복제 수 제약 아래 이동 데이터 양을 최소화하는 MILP를 푼다.
4. 가능한 해 중 가장 적은 서버 수를 선택한다. 해가 없으면 다른 규모의 후보 묶음을 검토한다.
5. 분산 트랜잭션이 있으면 배치에 따른 용량 변화를 선형화해 제약에 반영한다.

**평가:** H-Store의 TPC-C·YCSB에서 서버 수·이동량·서비스 성능을 평가했다.

**재현 시 구분:** 임의 가중합으로 서버 비용과 이동 비용을 한 번에 비교하는 방식과 다르다. 서버 수 후보 선택과 고정 서버 수에서의 최소 이동 최적화를 구분한다. partition 재정의는 자동 split/merge 해결로 간주하지 않는다.

출처: [PVLDB 2014 원문 §4, §5.1 General Algorithm·식 (4), §5.2, §6](https://www.vldb.org/pvldb/vol7/p1035-serafini.pdf).

## 6. P-Store — SPAR 예측과 동적 계획법으로 증감 일정 계산

**입력:** 과거 부하, SPAR 예측 부하열, 현재 서버 수, 정상/재구성 중 처리 용량, 재구성 소요 시간.

**알고리즘:**

1. 미래 일정 구간의 요청 부하를 예측한다.
2. 서버 수를 바꾸거나 유지하는 동작을 시작·종료 시점이 있는 move로 표현한다.
3. move 진행 중의 유효 용량도 예측 수요를 감당하는지 검사한다.
4. 동적 계획법으로 전체 예측 구간에서 수요를 만족하면서 할당 서버 비용을 줄이는 move 연속열을 구한다.
5. 충분한 재구성 시간을 확보해 실행하고, 이후 새 예측으로 계획을 갱신한다.

**평가:** B2W cart·checkout 부하 기반 H-Store benchmark에서 반응형 방식·정적 할당과 지연 목표 위반·서버 사용을 비교했다. 장기 simulation은 별도 평가다.

**재현 시 구분:** 예측 가능한 패턴과 대체로 균등한 partition 부하를 전제한다. hotspot 분할·배치를 자동으로 해결하는 방식이 아니다. 평가 미래값을 예측 입력으로 제공해서는 안 된다.

출처: [SIGMOD 2018 원문 §4 Algorithm 1–3, §5 SPAR, §7–8](https://marcoserafini.github.io/assets/pdf/pstore.pdf).

## 7. TIRAMOLA — MDP 상태·행동의 기대 보상 평가

**입력:** 부하·서버 및 client 관측, 현재 cluster 상태, 허용 resize 행동, 사용자가 정의한 보상 함수.

**알고리즘 개요:**

1. 성능과 자원 관측을 현재 상태로 표현한다.
2. 노드 추가·제거·유지의 가능한 행동을 MDP로 표현한다.
3. 관측된 상태·행동의 보상을 바탕으로 유리한 행동을 평가한다.
4. 선택한 resize를 Cloud Management에 전달한다.
5. Cluster Coordinator가 NoSQL 노드 가입·제거를 수행하고 다음 관측으로 상태를 갱신한다.

**평가:** HBase·OpenStack·YCSB 기반으로 보상 함수·허용 행동·학습 정보 조건을 바꿔 평가했다.

**추가 확인:** 원문 §III-A/B는 노드 수를 상태로 두고 결정론적 resize 전이와 식 (1)의 Q-learning 갱신을 제시한다. 현재 load 근방 관측을 cluster별로 묶어 가장 큰 cluster의 centroid로 보상을 추정한다. 이번 prototype은 그 계산을 사용하되 실제 VM 탐색 대신 별도 모형 학습과 결정론적 Q backup을 사용한다. 현대 deep RL 구현은 아니다.

출처: [CCGrid 2013 저자 원문 §III](https://www.cslab.ece.ntua.gr/~dtsouma/index_files/tiramola_CCGrid13.pdf), [저자 연구실 서지](https://www.cslab.ntua.gr/research/publications/).

## 8. PAX — hot partition에 맞는 휴면 노드 활성화

**입력:** CPU·요청률, 샘플링한 key/partition 접근, 데이터 복제 배치, 활성·휴면 노드 목록, 일관성 수준.

**알고리즘:**

1. 평균 CPU를 목표 범위와 비교한다. 예측형은 ARIMA 요청률과 서비스 수요 모델로 준비 시간 뒤의 CPU를 추정한다.
2. hot partition이 어느 노드에 있는지와 유효한 복제 수를 보며 활성화·비활성화할 노드를 선택한다.
3. 부족하면 휴면 VM을 켜고, 여유가 있으면 유효성을 보존하면서 VM을 끈다.
4. 재활성화 노드에는 shared hints를 적용해 휴면 중 변경을 동기화한다.

**평가:** Azure Cassandra에서 YCSB A·B·C와 논문 정의 G, 변화 부하·접근 쏠림을 사용해 반응형·예측형을 비교했다.

**재현 시 구분:** 고정 노드 집합의 active/dormant 상태를 고른다. 빈 신규 노드 생성·전체 데이터 이동과 같지 않으며, 복제 없는 OpenKache에 동일한 복귀 비용을 가정할 수 없다.

출처: [NOMS 2018 원문 §IV–VI](https://clouds.cis.unimelb.edu.au/papers/PAX-Cassandra-NOMS2018.pdf).

## 9. Slicer — 비용 편익 기반 이동과 hot split·cold merge

**입력:** slice·task 부하, 현재 assignment, 허용 복제 수, key churn 예산.

**알고리즘 핵심:**

1. 인접한 cold slice를 병합해 관리 단위를 정리한다.
2. 가장 뜨거운 task의 slice에 대해 이동·중복 할당·중복 제거 후보를 검토한다. 설정상 허용되지 않는 후보는 제외한다.
3. 부하 불균형 감소량을 key churn으로 나눈 가중치가 높은 후보부터 탐욕적으로 적용한다.
4. churn 예산을 소진하면 이동을 멈춘다.
5. hot slice를 담당 task 변경 없이 분할한다. 세밀한 관측과 다음 판단의 이동 선택지를 늘린다.

**평가:** 운영 workload와 assignment 평가에서 부하 균형·key churn·운영 overhead를 관측했다.

**재현 시 구분:** 서버 수는 외부가 결정한다. 위는 §4.4의 핵심 단계 요약이며 전체 장애·assignment 프로토콜은 아니다. key churn은 담당 범위 변경량이며 전송 byte와 다르다. split 자체로 다른 서버에 부하가 분산되지는 않는다.

출처: [OSDI 2016 원문 §4.4.1–4.4.3, §5](https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf).

## 10. 구현·평가 범위와 후속 확인

각 논문에 대해 관측·결정 알고리즘·실행 지원 기능 중 어디까지 재현할지 명시한다. 생략하거나 OpenKache에 맞게 바꾸는 부분은 원문과 구분해 기록한다.

- 원문 모델에 필요한 실측·이력·용량 데이터가 현재 존재하는가?
- tuple/bin/partition/slice를 Tablet으로 대체하면 어떤 의미가 달라지는가?
- 복제·휴면 노드·트랜잭션 등 현재 없는 전제는 무엇인가?
- 노드 판단과 배치의 출력이 실제 B1/B2 및 migration 실행 계약으로 연결되는가?
- 원문 평가 조건과 OpenKache 평가 조건의 차이를 설명할 수 있는가?

8개 핵심 알고리즘의 가상 모형 실험을 진행했다. 원문의 제품·운영 환경·프로토콜 전체를 재현한 것이 아니며, 실제 서버 통합과 모델 민감도 평가는 후속이다. 구체 변경점과 원본 검증 범위는 [실험 안내](../scaling-decision/README.md)에 명시한다.
