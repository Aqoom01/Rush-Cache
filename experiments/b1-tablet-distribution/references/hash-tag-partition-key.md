# Hash tag와 partition key: 태그로 묶어 분배하기

B1-B(해시 범위 Tablet)의 변형 `hash-tag-tablet`(B1-D)의 근거. Redis Cluster의 hash tag, DynamoDB·Cassandra의 partition key가 같은 생각이다.

## 출처

- Redis Cluster Specification, "Hash tags" 절 (redis.io/docs/reference/cluster-spec)
- Amazon DynamoDB Developer Guide, "Partitions and data distribution", "Designing partition keys to distribute your workload"
- Apache Cassandra 문서, "Data partitioning" (Murmur3Partitioner, partition key → token)

## 핵심 아이디어

세 시스템 모두 **key 전체가 아니라 key의 일부를 해시**해서 자리를 정한다.

- Redis Cluster: key에 `{...}`가 있으면 첫 `{`와 그 뒤 첫 `}` 사이만 CRC16에 넣는다. `{user1}:profile`과 `{user1}:cart`는 같은 슬롯이다. 규칙은 "첫 쌍, 비어 있지 않을 때만"이고, 아니면 key 전체를 해시한다. 목적은 multi-key 명령(MGET, 트랜잭션, Lua)을 한 노드에서 끝내는 것이다. Redis는 다른 슬롯에 걸친 multi-key 명령을 거부(`CROSSSLOT`)하므로 hash tag 없이는 쓸 수 없다.
- DynamoDB: 테이블의 partition key를 해시해 파티션을 정한다. 같은 partition key의 항목(sort key가 다른)은 한 파티션에 있고, 그 범위 안에서 정렬 조회(Query)가 된다.
- Cassandra: primary key의 앞부분(partition key)을 Murmur3로 토큰에 바꾼다. 한 partition의 row는 전부 한 노드(복제 집합)에 있다.

즉 **분배 단위가 key에서 "태그 묶음"으로 커진다**. 표(슬롯·Tablet·토큰 구간)는 그대로이고 바뀌는 것은 key → 해시값 함수뿐이다.

## 효과와 한계

얻는 것
- 같은 묶음의 key를 한 요청으로 한 노드에서 처리(multi-key, 트랜잭션, 범위 조회).
- 묶음 단위 이동: 노드 증감 때 묶음이 통째로 움직이므로 "묶음 일부만 다른 노드"인 상태가 없다.

잃는 것
- 묶음은 split으로도 못 나눈다. 묶음 하나가 크거나 뜨거우면 그 노드의 불균형은 어떤 재배치로도 못 푼다. DynamoDB 문서가 "hot partition"을 피하라고 partition key 설계 지침(쓰기 샤딩: key 뒤에 난수 접미사)을 두는 이유다. Cassandra도 큰 partition(수백 MB 이상)을 피하라고 한다.
- 묶음 크기가 제각각이면(테넌트·사용자별 데이터 양) key 수 균형이 묶음 크기 분포에 묶인다. 가장 큰 묶음의 비율 × 노드 수가 key 균형(max/mean)의 하한이다.
- 균형을 위해 태그를 잘게 쪼개면(접미사 샤딩) 묶는 이점이 줄어든다. 응용이 태그 설계를 책임진다.

## 질문

1. 묶음 크기가 Zipf로 치우친 workload에서 key 균형은 얼마나 나빠지는가? 묶음 수(100 ~ 10만)에 따라 어디서부터 "표 방식과 같은 균형"이 되는가?
2. multi-key 요청이 한 노드에서 끝날 확률은 key 전체 해시(약 1/n)와 비교해 얼마나 오르는가? (이 값이 해시 태그 방식을 택하는 유일한 이유다)
3. 요청 쏠림(Zipf θ=0.99)에서 Tablet 이동만으로 도달할 수 있는 균형의 한계가 묶음 때문에 얼마나 올라가는가?

## 프로젝트 적용

- `core.tablet.tag.HashTagScheme`. 표·조회·노드 증감·split은 `HashRangeScheme`에 그대로 위임하고, `DistributionScheme.keyHash(key)`만 Redis 규칙(첫 `{...}`)으로 바꾼다. tag가 없는 key는 key 전체를 해시하므로 `hash-range-tablet`과 결과가 같다.
- tag 문법은 Redis와 같은 `{...}`로 둔다. OpenKache의 key는 바이트열이라 구분자 규칙이 따로 없고, Redis 규칙이 가장 널리 쓰이며 명세가 분명하다.
- 비교 실험 S7(태그 묶음): 묶음 수 × 묶음 크기 쏠림 × seed. key 균형, 묶음이 흩어진 비율, 같은 묶음 두 key가 한 노드에 있을 확률, 요청 쏠림 한계. 결과는 `distribution-comparison/results.md` 3차.
