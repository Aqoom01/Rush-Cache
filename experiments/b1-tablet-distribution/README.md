# B1 Tablet-based Data Distribution

key를 Tablet 단위로 노드에 분배하는 방식 여러 개를 같은 인터페이스로 구현하고, 같은 workload와 노드 증감 시나리오에서 분배 균형·이동량·메타데이터 크기·조회 비용을 비교한다. B2(재배치·split/merge)와 B3(이동)가 바꿔 끼우며 실험할 분배 계층을 제공한다.

## 문서

| 파일 | 내용 |
|---|---|
| [design.md](design.md) | B1 전체 설계: 범위, 경계, 공통 인터페이스, 불변식, 지표, 실험 계획 |
| [references/](references/) | 참고 논문·시스템별 정리. 읽은 내용과 프로젝트 적용 방식을 분리해 적는다 |

## 분배 방식(variant) 목록

variant 하나는 참고 문헌 하나에 대응한다. 실험 결과의 `variant` 값으로 이 이름을 쓴다.

| variant | 단위 | split | 임의 이동 | 근거 | 상태 |
|---|---|---|---|---|---|
| `fixed-slot` | 고정 개수 슬롯 | 불가 | 가능 | [Redis Cluster 고정 해시 슬롯](references/redis-cluster-hash-slots.md) | 구현·1차 실험 완료 |
| `hash-range-tablet` | 가변 해시 구간 | 가능 | 가능 | [Google Slicer와 ScyllaDB Tablets](references/slicer-scylla-tablets.md) | 구현 |
| `ring-vnode` | 링 위 토큰 구간 | 불가 | 불가 | [Dynamo와 Cassandra vnode](references/dynamo-cassandra-vnode.md) | 구현 (배치 규칙 `ring-vnode` / `ring-vnode-alloc`) |

- `fixed-slot`은 가장 단순한 Tablet이다. 공통 인터페이스와 라우팅 표를 이것으로 먼저 검증한다.
- `hash-range-tablet`은 명세의 "Tablet" 본안이다. split/merge가 되는 유일한 variant라 B2 실험의 본 무대다.
- `ring-vnode`는 표 없이 계산으로 분배하는 기준선이다. "표를 두는 것이 실제로 이득인가"를 수치로 답하기 위해 둔다.

## 코드 위치

알고리즘 코드는 `cache-core`의 `io.openkache.core.tablet`에 둔다. 이 디렉터리에는 설계, 참고 문헌 정리, 실험 설정과 결과 요약만 둔다. 상세는 [design.md](design.md) §7을 따른다.
