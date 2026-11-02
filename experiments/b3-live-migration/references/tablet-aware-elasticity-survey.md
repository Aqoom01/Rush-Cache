# Tablet을 고려한 노드 증감: 방법론·논문·운영 사례 조사

조사 기준일: 2026-10-30, 알고리즘 정리 갱신: 2026-11-02. 이 문서는 선행연구 조사와 OpenKache 적용 제안이다. E/M 정책 구현, 논문 재현 또는 실험 완료를 의미하지 않는다. 논문의 수치와 제품 기본값을 OpenKache의 목표값으로 사용하지 않는다.

실제 시스템별 입력·알고리즘 단계·평가·전제는 [선행 알고리즘 설명](elastic-scaling-algorithms.md)에 정리한다. 8개 핵심 메커니즘의 가상 prototype·비교 결과는 [실험 안내](../scaling-decision/README.md)를 따른다. 원 논문 시스템 재현과 실환경 검증은 아니다.

## 1. 조사 결론과 판단 범위

Tablet이 있는 저장소의 탄력성은 노드 수, 데이터 배치, 분할 단위, 이동 중 성능을 함께 다룬다. 평균 부하로 계산한 필요 노드 수는 후보값이다. 그 개수의 노드에 실제 Tablet을 배치할 수 있는지, 준비·이동이 끝날 때까지 부하를 견딜 수 있는지 별도로 확인해야 한다.

다음 세 문제는 구분한다.

- **총용량 부족:** 기존 노드에 잘 배치해도 전체 부하·데이터를 수용할 수 없다.
- **배치 불균형:** 전체 용량은 있지만 일부 노드에 Tablet 부하가 몰린다.
- **분할 한계:** 하나의 Tablet이 너무 크거나 뜨겁다. split 후 이동이 유효한지 확인한다. 현재 단일 owner 모델에서 단일 key 자체의 부하는 split만으로 분산되지 않는다.

split은 배치 선택지를 늘리며, 동일 노드에 두 자식이 남으면 하드웨어 용량은 늘지 않는다. merge는 조각 수를 줄이는 동작이며 노드 제거와 같지 않다. 병합을 위해 먼저 같은 owner로 모으는 이동이 필요할 수도 있다. 모든 축소에 merge가 선행해야 하는 것은 아니다.

## 2. 실제 알고리즘 설명의 위치

[선행 시스템별 알고리즘](elastic-scaling-algorithms.md)은 SCADS Director·ElastMan·E-Store·Accordion·P-Store·TIRAMOLA·PAX·Slicer를 각각 설명한다. 아래는 관련 문헌과 평가 근거의 조사 기록이다. 서로 다른 논문의 일부를 조합한 정책을 별도의 논문 알고리즘처럼 명명하거나 재현 결과로 사용하지 않는다.

## 3. 노드 증감까지 다룬 논문

### 3.1 Automated Control for Elastic Storage — ICAC 2010

- 저자: Harold C. Lim, Shivnath Babu, Jeffrey S. Chase.
- **논문에서 확인한 내용:** 정수 단위 자원 증감을 위한 proportional thresholding과 저장소 재균형에 따른 지연·간섭·제어 요소 간 조정을 다룬다.
- **평가:** HDFS 기반 저장소, Cloudstone의 동적 Web 2.0 부하. 성능 목표 유지와 자원 사용 효율, 부하 변화에 대한 반응을 평가한다.
- **B3 적용 해석:** 증감 제어와 migration 제어를 독립적으로 반복 실행하면 서로의 관측값을 흔들 수 있다는 근거다. 준비·재균형 중 상태와 비대칭 안정화 규칙을 명시해야 한다.
- **한계·확인 범위:** HDFS 블록과 Tablet은 같은 구현 단위가 아니다. 이번 조사에서는 원문 검색 본문의 초록·서두·제어 개요와 대학 서지를 확인했으며, 전체 실험 절의 세부 설정은 추가 정독 대상이다.
- 출처: [저자 원문](https://users.cs.duke.edu/~shivnath/papers/icac2010.pdf), [Duke 서지·초록](https://scholars.duke.edu/publication/758485). DOI: 10.1145/1809049.1809051.

### 3.2 The SCADS Director: Scaling a Distributed Storage System Under Stringent Performance Requirements — FAST 2011

- 저자: Beth Trushkowsky 외.
- **논문:** §5의 제어기가 bin별 요청 분포·현재 배치·성능 모델을 보고 서버 증감과 데이터 이동·복제를 결정하고, scheduler가 실행한다. §5.5는 증가·감소에 다른 smoothing을, §5.6은 동시 복사의 서비스 영향을 고려한다.
- **평가:** §6의 EC2 기반 SCADS. 갑작스러운 데이터 hotspot과 일중 부하 변화에서 상위 백분위 응답시간, 안정화와 사용 서버 수의 관계를 평가한다.
- **B3 적용 해석:** B2 계획을 평가할 때 현재 배치와 예상 노드별 부하를 함께 받아야 한다. 계획 생성과 실행 스케줄을 분리한다.
- **한계:** 복제·대기 서버를 활용한다. 복제 없는 단일 owner B3에 그 가용성 보장을 그대로 옮길 수 없다. 논문도 온라인 판단에서 복사 영향을 모두 직접 모델링하는 것은 아니라고 명시한다.
- 출처: [원문 §5–6](https://www.usenix.org/legacy/events/fast11/tech/full_papers/Trushkowsky.pdf).

### 3.3 ElastMan: Elasticity Manager for Elastic Key-Value Stores in the Cloud — CAC 2013

- 저자: Ahmad Al-Shishtawy, Vladimir Vlassov.
- **논문:** feedforward로 큰 부하 변화를 처리하고 feedback으로 오차를 보정한다. 읽기 p99 지연을 목표 성능으로 사용한다.
- **평가:** §5, OpenStack의 Voldemort와 YCSB 기반 client. 균등 key 분포에서 일중 변화·spike를 주고 처리량과 읽기 p99 지연을 관측한다.
- **B3 적용 해석:** 평균 처리량뿐 아니라 서비스 품질을 제어 목표로 사용하는 후속 후보다. 실측한 노드당 용량과 모델 오차 보정의 필요성을 보여 준다.
- **한계:** 실제 latency와 용량 식별이 필요하므로 현재 tick 모델에 실제 p99를 만들어 넣을 수 없다. 이 논문을 split/merge 정책의 근거로 사용하지 않는다.
- 출처: [원문 §4–5](https://dcatkth.github.io/papers/a7-al-shishtawy.pdf).

### 3.4 E-Store: Fine-Grained Elastic Partitioning for Distributed Transaction Processing Systems — PVLDB 8(3), 2014

- 저자: Rebecca Taft 외. 2014년 발행, VLDB 2015 발표 대상 논문이다.
- **논문:** §4에서 가벼운 CPU 관측으로 문제를 감지한 뒤 세밀한 tuple 접근 관측을 켠다. §5는 노드 증감과 세밀한 배치 계획을 다룬다. cold block과 hot tuple을 다른 입도로 배치한다.
- **평가:** §6, H-Store에서 Voter·YCSB·TPC-C, 무쏠림·저쏠림·고쏠림. 배치 방법과 관측 설정을 비교하고 재구성 구간을 포함한 처리량·평균 지연을 관측한다.
- **B3 적용 해석:** 전체 부하와 국소 hotspot을 구분하고, 세밀한 계획이 노드 사용량과 요청 영향에 주는 효과를 비교한다.
- **한계:** hot tuple 배치는 Tablet split/merge와 일대일로 같지 않다. 논문의 계획 알고리즘은 당시 main memory 용량 반영을 후속으로 두었다. 모든 자원 제약을 이미 해결한 시스템으로 설명하지 않는다.
- 출처: [원문 §4–6](https://www.vldb.org/pvldb/vol8/p245-taft.pdf).

### 3.5 Accordion: Elastic Scalability for Database Systems Supporting Distributed Transactions — PVLDB 7(12), 2014

- 저자: Marco Serafini 외.
- **논문:** 서버 용량·메모리·partition 배치를 제약으로 사용한다. 데이터 이동량과 서버 수를 고려하고, 분산 트랜잭션의 affinity에 따른 용량 변화를 모델링한다. §5는 정수 최적화 기반 배치 정식화를 다룬다.
- **평가:** H-Store, TPC-C·YCSB에서 대체 heuristic과 비교. 사용 서버 수와 이동 데이터 양, 서비스 성능을 평가한다.
- **B3 적용 해석:** 후보 노드 수에 실제 배치가 가능한지 검사하고, 가능한 계획 간 이동 비용을 비교하는 근거다. 작은 문제의 최적화 결과를 heuristic 검증 기준으로 둘 수 있다.
- **한계:** 고정 partition의 배치 문제이며 임의 Tablet split/merge를 자동으로 모두 최적화하는 논문은 아니다. 현재 KV에는 분산 트랜잭션 affinity가 없으므로 해당 모델은 제외한다.
- 출처: [원문](https://www.vldb.org/pvldb/vol7/p1035-serafini.pdf).

### 3.6 P-Store: An Elastic Database System with Predictive Provisioning — SIGMOD 2018

- 저자: Rebecca Taft 외.
- **논문:** §3–5에서 미래 부하와 이동 시간·이동 중 유효 용량을 고려해 재구성을 계획한다. 동적 계획법으로 시점과 목표 규모를 정하고 SPAR로 부하를 예측한다.
- **평가:** §7–8, B2W의 cart·checkout 이력으로 만든 H-Store benchmark. 정적 소규모·정적 peak 용량·반응형 E-Store 방식과 비교한다. 백분위 지연 목표 위반 시간과 평균 할당 서버 수를 보고한다. 별도의 장기 simulation은 실측과 구별한다.
- **B3 적용 해석:** VM READY와 부하 해소 완료는 다른 시점이다. provisioning·migration·정상화 시간을 분리하고 예측 horizon에 포함한다.
- **한계:** 평가 workload는 비교적 균등하고 예측 가능한 패턴이다. 논문도 E-Store처럼 skew를 관리하지는 않는다고 설명한다. 예상 못 한 spike 대응과 hotspot 계획을 별도로 둬야 한다.
- 출처: [원문 §3–8](https://marcoserafini.github.io/assets/pdf/pstore.pdf). DOI: 10.1145/3183713.3190650.

### 3.7 PAX: Partition-Aware Autoscaling for the Cassandra NoSQL Database — NOMS 2018

- 저자: Salvatore Dipietro, Rajkumar Buyya, Giuliano Casale.
- **논문:** query sampling과 partition 배치를 보고 활성화할 노드를 선택한다. 반응형·ARIMA 예측형 controller를 비교하며, dormant VM 재활성화와 shared hints 기반 동기화를 활용한다.
- **평가:** Azure의 Cassandra, §VI의 YCSB 부하. A·B·C 및 논문 정의 G로 요청 비율·접근 분포를 바꾸고, 변화 부하·hot partition 이동을 시험한다. 처리량·노드 수·CPU 부족 시간/정도·비용을 비교한다.
- **B3 적용 해석:** 평균 부하가 같아도 어느 데이터가 뜨거운지에 따라 유리한 노드·배치가 다르다는 근거다.
- **한계:** 이미 데이터를 가진 dormant replica와 Cassandra hinted handoff에 의존한다. 빈 노드를 새로 만들고 전체 데이터를 옮기는 B3와 같은 실험이 아니다. 논문 G를 YCSB의 보편적인 표준 workload라고 부르지 않는다.
- 출처: [원문](https://clouds.cis.unimelb.edu.au/papers/PAX-Cassandra-NOMS2018.pdf), [저자 연구실 서지](https://qore.doc.ic.ac.uk/publications/). DOI: 10.1109/NOMS.2018.8406271.

### 3.8 Automated, Elastic Resource Provisioning for NoSQL Clusters Using TIRAMOLA — CCGrid 2013

- 저자: Dimitrios Tsoumakos 외. SIGMOD 2012 demo 논문과 구별한다.
- **논문:** add/remove/no-op을 MDP 행동으로 표현하고 사용자 보상에 따라 cluster 규모를 선택한다. 관측·학습으로 상태의 이득을 추정한다.
- **평가:** HBase, OpenStack, YCSB 기반 부하. 서로 다른 reward·허용 증감량·학습 부하를 비교하며 처리량·latency·VM 비용과 의사결정을 평가한다.
- **B3 적용 해석:** 노드 비용·과부하·이동 비용의 trade-off를 보상으로 표현하는 학습 기반 후속 후보다.
- **한계·확인 범위:** 학습과 reward 설계가 필요하며, MDP에 Tablet 배치나 split 행동이 자동으로 들어가는 것은 아니다. 이번 조사에서는 저자 공개 PDF의 검색 본문과 저자 서지를 확인했다. 전체 실험 절의 세부값은 추가 정독 대상이다. 현대 deep RL을 구현한 연구라고 설명하지 않는다.
- 출처: [저자 원문](https://www.cslab.ntua.gr/~ikons/assets/files/tiramola_ccgrid_camera_ready.pdf), [저자 서지](https://www.cslab.ece.ntua.gr/~dtsouma/pubs.html). DOI: 10.1109/CCGrid.2013.45.

### 3.9 Chameleon: A Hybrid, Proactive Auto-Scaling Mechanism on a Level-Playing Field — IEEE TPDS 30(4), 2019

- 저자: André Bauer, Nikolas Herbst, Simon Spinner, Ahmed Ali-Eldin, Samuel Kounev.
- **논문·공식 안내:** 여러 proactive 방법과 reactive fallback을 결합한다. 다른 auto-scaler와 동일 조건에서 비교하는 일반 탄력성 연구다.
- **B3 적용 해석:** 예측이 부정확하거나 workload가 급변할 때 fallback을 두는 원칙을 참고한다.
- **한계·확인 범위:** 이번 조사에서는 저자 초록·도구 안내·서지를 확인했다. 세부 benchmark와 설정은 원문 평가 절 추가 정독 대상이다. Tablet split/merge와 stateful migration을 직접 해결한 논문으로 인용하지 않는다.
- 출처: [저자 공식 안내·서지](https://se.informatik.uni-wuerzburg.de/en/tools/chameleon/). DOI: 10.1109/TPDS.2018.2870389.

## 4. 분할·병합·배치와 평가 방법의 논문

### 4.1 Slicer: Auto-Sharding for Datacenter Applications — OSDI 2016

- **논문:** §4.4에서 hot slice 분할, 인접 cold slice 병합, load imbalance 감소와 key churn 사이의 비용 편익을 다룬다. 분할 자체는 기존 task 배치를 유지해 다음 이동의 선택지를 만든다.
- **평가:** §5의 production 관측과 평가. 최대 task CPU / 평균 task CPU로 불균형을 보고, static sharding과 비교한다. assignment 전파 시간·관리 overhead도 관측한다.
- **B3 적용 해석:** 같은 노드 수에서 split/merge와 이동만으로 개선되는 양, 이동에 따른 비용, 조각 수 증가를 측정한다.
- **범위:** 노드/task 증감은 외부 메커니즘 담당이라고 명시한다. Slicer를 노드 수 결정 알고리즘으로 재현하지 않는다. key churn은 keyspace 재할당 비율이며, 실제 byte 이동량과 같은 값이 아니다.
- 출처: [원문 §4.4·§5](https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf).

### 4.2 Amazon DynamoDB: A Scalable, Predictably Performant, and Fully Managed NoSQL Database Service — USENIX ATC 2022

- **논문:** §4.3은 소비한 용량·데이터 크기에 따른 replica 이동, §4.4는 관측 접근 분포를 이용한 split을 설명한다. 단일 item이나 순차 접근처럼 효과 없는 경우의 분할을 피한다.
- **평가 해석:** 운영 경험 논문으로, 공개 설명만으로 정확히 동일한 split controller·threshold를 재현할 수 있다고 보지 않는다.
- **B3 적용 해석:** 총용량 부족과 국소 hotspot을 구분한다. 불가능한 split을 반복하거나 무의미한 증설을 계속하는 경우를 별도 결과로 기록한다.
- **범위:** 고객의 provisioned capacity와 물리 노드 수는 같은 제어 대상이 아니다. 이 논문에서 보편적인 merge 정책을 확인한 것은 아니다.
- 출처: [원문 §4.3–4.4](https://www.usenix.org/system/files/atc22-elhemali.pdf).

### 4.3 Optimizing Resource Allocation in Hyperscale Datacenters: Scalability, Usability, and Experiences — OSDI 2024

- 저자: Neeraj Kumar 외, Meta. Rebalancer 연구다.
- **논문·공식 설명:** 다양한 자원 배치 문제의 제약·목표를 표현하고 큰 문제를 효율적으로 풀기 위한 공통 framework를 다룬다.
- **공개 구현:** local search와 mixed-integer solver를 구분한다. 전자는 유효한 이동·교환으로 목적을 개선하지만 전역 최적 보장은 없다. 후자는 계산 시간이 충분하면 최적해를 구할 수 있으나 규모에 제약이 있다.
- **B3 적용 해석:** 먼저 용량 제약을 지키고 이후 이동량·불균형을 줄이는 작은 탐욕/지역 탐색을 만들 수 있다. 비교에는 계획 계산 시간·해의 품질도 넣는다.
- **범위:** 일반 배치 최적화가 곧 노드 증감 정책은 아니다. active node 선택과 split 후보를 문제에 명시해야 한다. OpenKache core에 외부 solver를 추가하기로 결정한 것이 아니다.
- 출처: [논문 공식 페이지](https://www.usenix.org/conference/osdi24/presentation/kumar), [공식 구현](https://github.com/facebook/rebalancer), [2026-09-21 공개 안내](https://engineering.fb.com/2026/09/21/open-source/rebalancer-generic-high-performance-library-assignment-problems/).

### 4.4 BUNGEE: An Elasticity Benchmark for Self-Adaptive IaaS Cloud Environments — SEAMS 2015

- 저자: Nikolas Herbst, Samuel Kounev, Andreas Weber, Henning Groenda.
- **논문·공식 안내:** 처리 능력 분석, 부하 calibration, 변화 부하 주입, 탄력성 평가를 구분한다. CloudStack·AWS 기반 IaaS에서 사용했다.
- **지표 근거:** 같은 계열의 SPEC-RG-2016-01 §2.6은 수요·공급 곡선으로 부족/초과 자원 면적, 부족/초과 상태 시간 비율, adaptation jitter를 정의한다.
- **B3 적용 해석:** 특정 정책이 출력한 노드 수를 정답으로 삼지 않고 별도 수요 모델과 실제 공급을 비교한다. 모델 기반 부족과 실제 SLO 위반은 구분한다.
- 출처: [논문·공식 도구 안내](https://se.informatik.uni-wuerzburg.de/tools/bungee/), [SPEC 지표 정의 §2.6](https://research.spec.org/fileadmin/user_upload/documents/rg_cloud/endorsed_publications/SPEC-RG-2016-01_CloudMetrics.pdf).

## 5. 실제 운영 시스템의 선행 사례

| 사례 | 공식 자료에서 확인한 범위 | B3에 가져올 원칙 | 동일시하면 안 되는 것 |
|---|---|---|---|
| Kubernetes HPA / EC2 Auto Scaling | 관측 metric의 목표 추적, tolerance·stabilization 또는 준비 시간과 관련한 안정화 | 단순하고 재현 가능한 count baseline과 실행 gate | HPA의 Pod 수와 VM/스토리지 노드 수, stateless 준비와 Tablet 이동 완료 |
| ScyllaDB Tablets | Tablet 이동, 데이터 크기와 shard·tablet 설정에 따른 개수 재평가, split/merge로 table resize | node 수와 Tablet 수를 따로 기록하고, 노드 추가 후 실제 분산 완료까지 측정 | 현재 공식 문서를 구버전의 단일 2배/절반 규칙으로 일반화하거나, 자동 재배치를 자동 서버 구매로 설명 |
| CockroachDB Ranges | 크기에 따른 split과 작은 range의 merge, 메타데이터·Raft 경계 관리 | split/merge도 owner·routing 갱신과 실행 정합성이 필요 | range 관리 기능만으로 최상위 compute autoscaling 결정도 모두 설명했다고 보기 |
| Meta Shard Manager / Twine | shard 배치와 container 자원 배분의 계층 분리, 제약·부하에 따른 shard 이동·replica 조정 | B1·B2·B3·외부 provisioner 경계를 분리하고 실행 계약을 명시 | shard replica 증가와 cluster node 증가, 복제 기반 가용성과 현재 단일 owner 모델 |

출처: [HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/), [EC2 동적 정책](https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scale-based-on-demand.html), [ScyllaDB 공식 문서](https://docs.scylladb.com/manual/stable/architecture/tablets.html), [CockroachDB distribution layer](https://docs.cockroachlabs.com/docs/stable/architecture/distribution-layer), [Shard Manager 운영 설명](https://engineering.fb.com/2020/08/24/production-engineering/scaling-services-with-shard-manager/).

제품 문서는 조사일의 stable 내용을 기준으로 읽었다. 향후 재현 실험은 제품 버전과 문서 snapshot/commit을 고정해야 한다. ScyllaDB의 현재 문서에는 tablet 수 제한과 `pow2_count` 설정 등이 있으므로, 과거 정리의 모든 table은 항상 2의 거듭제곱이라는 표현을 현재 모든 설정에 그대로 적용하지 않는다.

## 6. OpenKache 적용 제안

이 절은 조사 결과에 따른 설계 제안이며, 기존 설계를 변경하거나 구현한 내용이 아니다.

### 6.1 선행 방법 재현 범위 선정

자체 기준선 조합을 첫 비교 대상으로 확정하지 않는다. 실제 시스템별 알고리즘을 검토하고 필요한 실측·이력·복제·배치 기능을 확인한 뒤 대상을 선정한다. 원문을 단순화하면 바뀐 관측·결정·행동을 명시한다. 현재 핵심 메커니즘의 가상 prototype 8개를 실행했으며 변경점은 실험 안내에 기록했다. 원 논문 제품·실환경 재현은 후속이다.

### 6.2 판단·실행 계약

1. B3는 후보 노드 수와 준비 상태를 제시한다. B1/B2 경계의 planner는 현재 Tablet 목록·범위·owner·부하·크기를 기준으로 배치/분할 후보와 실행 가능 여부를 돌려준다. 이 계약은 아직 구현되지 않았다.
2. B3는 현재 노드 유지, 증가, 축소 후보를 예상 최대 노드 부하·이동량·효과 발생 시간으로 비교한다. 배치 불가능과 heuristic 탐색 실패를 구별한다.
3. 후보가 한 노드 한도를 넘는 단일 key를 포함하면 현 모델의 해결 불가로 기록한다. 이를 이유로 무한 증설하지 않는다.
4. cluster 전체 용량이 부족하면 불필요하게 재배치 완료를 기다리지 않고 provisioning을 시작할 수 있다. split 준비와 노드 준비의 병행 가능성은 실행 계약으로 정한다.
5. split/merge와 이동이 동일 Tablet에 겹치지 않도록 parent/child ID·routing version·진행 중 작업을 검증한다. 낡은 계획은 재검증하거나 폐기한다. 새 노드는 실제로 부하를 맡기 전까지 전체 처리 용량으로 계산하지 않는다.
6. 축소는 대상 노드의 Tablet을 남은 노드에 수용할 계획과 drain 완료를 확인한 뒤 종료한다. 낮은 CPU만 보고 대상을 고르지 않는다. merge는 필요할 때만 수행한다.

### 6.3 부하와 비교의 공정성

- 모든 정책에 같은 외부 요청 시도열·초기 데이터·seed·측정 구간을 준다. 정책이 달라지면 관측 사용률도 달라지므로 동일한 고정 사용률 stream을 주는 검사는 단독 계약 테스트로 한정한다.
- 균등 부하 증가, hot range, 이동 hotspot, 단일 hot key, 지속 저부하, 순차 쓰기를 구분한다. key-order와 hash-space Tablet에서 순차 쓰기가 가지는 의미는 다르다.
- split 효과와 노드 증감 효과를 분리하기 위해 노드 수 고정/자동 × split/merge 꺼짐/켜짐을 교차한다. 호환되지 않는 B1 variant는 unsupported로 기록한다.
- 부하·메모리·저장 크기를 함께 제한한다. 평균 부하 공식에서 나온 수는 균등·동질 모델의 참고 하한일 뿐, 모든 workload의 정답 노드 수가 아니다. 작은 사례에서는 열거/최적화로 실행 가능성을 검증한다.
- 설정을 고르는 부하와 최종 평가 부하를 분리한다. 예측기는 과거 관측만 사용한다. 미래를 아는 oracle은 별도 비교군으로 표시한다.
- 같은 seed의 정책 간 차이를 비교한다. 동일 입력의 재실행은 재현성 검사이며 독립 표본을 늘렸다고 계산하지 않는다.

## 7. 지표와 출처의 대응

| 평가 질문 | 지표 | 근거·구분 |
|---|---|---|
| 자원을 충분히 공급했나 | 부족/초과 자원 면적과 시간 정규화 값, 부족 시간 비율 | BUNGEE·SPEC. 수요 모델을 별도로 고정 |
| 실제 서비스 품질을 지켰나 | 처리량, 읽기/쓰기 latency, SLO 위반 window 비율 | ElastMan·SCADS·P-Store. 실패·timeout은 성공 latency와 별도 보고 |
| 국소 hotspot을 해소했나 | 최대 노드 부하, 최대/평균 부하, 과부하 해소 시간 | Slicer·E-Store. 평균이 0인 window의 비율은 미정의로 구분 |
| 자원 비용은 얼마인가 | provision 시작~종료의 node-tick 또는 node-second, 최대 노드 수 | P-Store 등의 자원 비용 관점. 가격표가 없으면 실제 금액이라고 부르지 않음 |
| 용량이 언제 늘었나 | 결정 지연, provision 시간, migration 시간, 실제 부하 분담까지의 시간 | P-Store·PAX·Lim |
| 이동 부담이 큰가 | 총 이동 bytes, 재전송 증폭, 이동 중 거절/실패, 계획별 완료 시간 | Accordion·SCADS·기존 B3 migration |
| 구조 변화가 반복되는가 | split/merge 횟수, 되돌리는 resize 횟수, Tablet 수, routing 갱신량 | Slicer·제품 사례를 참고한 프로젝트 추가 지표 |
| 결정 계산이 가능한가 | planner 소요 시간, timeout, heuristic 실패, 작은 사례의 최적해 대비 차이 | Accordion·Rebalancer |
| 안전한가 | 범위 위반·중복 action·미배치/중복 owner·낡은 계획 실행·drain 전 종료 수 | 프로젝트 불변식. 모두 0이어야 함 |

SPEC의 adaptation jitter와 단순 action 수는 같은 정의가 아니다. key churn, 이동한 key 수, bytes도 구분한다. 세 가지를 서로 대신하지 않는다.

현재 in-process 결과는 `timeDomain=virtual`, 시간 tick, 처리량 operations/tick·bytes/tick으로 표시한다. CPU·물리 latency·실제 SLO를 관측한 것으로 설명하지 않는다. 물리 서버 비교는 별도로 수행한다. 가상 결과의 용량 부족이 실제 latency 위반과 같다는 가정을 두지 않는다.

## 8. 정독·구현 우선순위

1. **SCADS Director §5–6:** node count·배치·scheduler를 연결하는 구조.
2. **E-Store §4–6 + Slicer §4.4:** hotspot 관측과 세밀한 배치/split/merge.
3. **Accordion §5:** 실행 가능한 축소와 이동 비용의 제약 표현.
4. **P-Store §3–5·§8:** migration 중 유효 용량, 실측과 simulation을 나누는 평가.
5. **PAX:** partition을 고려한 node 선택의 효과와 replication 의존성.
6. **Lim·ElastMan:** 관측 지연·제어 진동. Lim 원문 평가 절 추가 정독.
7. **TIRAMOLA·Chameleon:** 학습/예측 후보. 전체 실험 설정 추가 정독.
8. **Rebalancer:** planner 규모가 커질 때 계산 시간·품질 비교. solver 도입 자체는 별도 결정.

구현 우선순위는 아직 확정하지 않았다. [알고리즘 설명](elastic-scaling-algorithms.md)의 원문 절과 필요한 전제를 확인한 뒤, 재현 범위와 평가 조건을 먼저 기록한다.
