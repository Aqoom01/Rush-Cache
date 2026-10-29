# Bigtable·HBase·TiKV의 키 순서 범위 Tablet

표 계열 variant 확장 세 번째 `key-order-tablet`(B1-F)의 근거. 지금까지의 variant는 전부 key를 해시해 순서를 없앴다. 이 계열은 key를 **정렬된 그대로** 두고 연속 구간을 tablet(region)으로 묶는다.

## 출처

- Chang et al., "Bigtable: A Distributed Storage System for Structured Data", OSDI 2006. §2 Data Model(row key 사전순), §5.3 Tablet Assignment, §6(locality group), 그리고 §2의 "URL을 뒤집어 저장"하는 예
- Apache HBase Reference Guide, "Regions", "Pre-splitting" 절과 "Rowkey design: Hotspotting" 절(salting, hashing, reversing timestamps)
- TiKV 문서, "Region" (key range 기반 split, 기본 96 MB)
- 비교: Slicer §2.1이 key 순서 대신 해시를 택한 이유 → [slicer-scylla-tablets.md](slicer-scylla-tablets.md) 질문 1

## 핵심 아이디어

row key를 바이트 사전순으로 정렬하고, 그 순서에서 **연속한 구간**을 tablet으로 만든다. tablet은 크기가 커지면 가운데 key에서 둘로 쪼개지고, 작아지면 이웃과 합쳐진다. tablet 경계는 실제 key(가변 길이 바이트열)다.

```text
key 순서:   a… b… c… … m… … z…
            [  T0   ][  T1  ][   T2    ]      ← 구간 = key 범위 [start_key, end_key)
             서버A     서버B    서버A
scan("ca".."cz")  →  T0 하나만 읽는다
```

해시 범위(B1-B)와의 차이는 **key → 점 변환이 순서를 보존한다**는 것뿐이다. 표·조회·split·이동은 같은 구조다. 그래서 Bigtable은 "해시 대신 순서"를 택한 대가와 이득을 가장 순수하게 보여 주는 비교 대상이다.

## 효과와 한계

얻는 것
- **범위 조회(scan)**: 이어진 key들이 한 tablet, 한 서버에 있다. 해시 방식은 key 100개를 읽으면 서버 전부를 거친다.
- **접두사 묶음이 공짜다**: `user1:…` 같은 key는 자연히 붙어 있다. B1-D가 hash tag로 만든 "같은 노드에 모으기"를 정렬이 준다. Bigtable 논문의 "URL을 역순(com.google.maps/…)으로 저장해 같은 도메인을 붙여 둔다"가 이 용법이다.
- 경계를 데이터를 보고 정할 수 있다(pre-split).

잃는 것
- **순차 key hot spot**: 시간·자동 증가 ID처럼 단조 증가하는 key를 넣으면 새 쓰기가 전부 **맨 끝 tablet 하나**로 간다. split해도 다음 쓰기는 또 새 끝 tablet으로 간다. HBase 가이드가 salting(접두사 난수)·hashing·timestamp 역순을 권하는 이유이고, 그러면 범위 조회 이득이 줄어든다.
- **처음 경계를 모른다**: 데이터를 보기 전에는 key가 어디에 몰릴지 모르므로 균등 분할이 쓸모없다(순차 key면 전부 한 구간). HBase는 pre-splitting(표본으로 경계 지정)을 운영자에게 맡긴다.
- **표 크기가 key 길이에 비례**: 경계가 실제 key다. 해시 방식은 경계가 8바이트 수다.
- 해시 방식의 균등 분포 보장이 없다. key 분포가 곧 부하 분포다.

## 질문

1. 순차 key를 절반 넣어 둔 상태에서 나머지 절반을 순서대로 삽입하면, 삽입이 한 Tablet·한 노드에 얼마나 몰리는가? 해시 variant들과 비교.
2. 기존 데이터로 pre-split하면 기존 데이터의 균형은 잡히는가, 그래도 새 삽입은 끝으로 몰리는가?
3. 가장 뜨거운 Tablet을 중앙값에서 쪼개기를 반복하면(B2의 split 정책 단순판) 이미 들어온 데이터는 고르게 옮길 수 있는가, 그다음 삽입은 어디로 가는가?
4. 이어진 key 100개를 읽을 때 거치는 Tablet·노드 수는? (범위 조회의 이득을 숫자로)

## 프로젝트 적용

- `core.tablet.order.KeyOrderScheme`. 표·조회·노드 증감·split은 `HashRangeScheme`에 위임하고 `keyHash(key)`만 **순서 보존 변환**(앞 8바이트 big-endian, 짧으면 0 채움)으로 바꾼다. 모든 variant가 같은 64비트 `RoutingTable`을 쓰기 위한 타협이다: 앞 8바이트가 같은 key는 같은 점이 되어 더 쪼갤 수 없다. 응용은 구분력이 앞 8바이트 안에 오도록 key를 설계해야 한다(길이 8의 big-endian 번호처럼). 순서는 B2 `core.balance.KeyRange.ORDER`(부호 없는 바이트 사전순)와 앞 8바이트 안에서 일치한다.
- `initial(nodes, sampleKeys)`: 표본의 분위수에 경계를 두는 pre-split. HBase pre-splitting에 해당한다. 서로 다른 경계가 모자라면(점이 겹치면) 구간 수가 준다.
- 비교 실험 S8(순차 key): key 100만 개를 앞 8바이트 big-endian 번호로 만들어(뒤 8바이트는 난수) 앞 절반을 기존 데이터, 뒤 절반을 삽입으로 본다. 기본 표·pre-split 표·split 64회 뒤의 균형과 hot spot, 범위 조회 창 100개가 거치는 Tablet·노드 수. 결과는 `distribution-comparison/results.md` 6차.
- B2 조율: B2의 `KeyRange`(key 바이트 범위)는 이 variant에서만 뜻이 있다. 해시 variant에서 `KeyRange`는 성립하지 않으므로(비슷한 key가 흩어짐), B2가 순차 쓰기 시나리오를 보려면 이 variant 위에서 해야 한다. 경계 타입은 `TabletRange`(64비트 점) 하나로 두고, key 경계가 필요하면 점을 8바이트 key로 되돌린다.
