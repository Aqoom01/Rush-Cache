# B2 Load-aware Balancing + Split/Merge

부하(QPS, 이후 CPU·IOPS)를 기준으로 Tablet을 나누고(Hot Split), 합치고(Cold Merge), 재배치하는 정책을 구현하고 비교한다.

## 문서

| 파일 | 내용 |
|---|---|
| [design.md](design.md) | B2 전체 설계: 범위, 경계, 공통 입력·출력 모델, 불변식, 지표, 실험 계획 |
| [references/](references/) | 참고 논문·시스템별 정리. 읽은 내용과 프로젝트 적용 방식을 분리해 적는다 |

## 정책(variant) 목록

정책 하나는 참고 문헌 하나에 대응한다. 실험 결과의 `variant` 값으로 이 이름을 쓴다.

| variant | 기능 | 근거 | 상태 |
|---|---|---|---|
| `midpoint-split` | Split | Baseline (key 범위의 가운데에서 분할. 논문 §4.4의 비교 대상) | 구현 (`MidpointSplitPolicy`) |
| `split-for-heat` | Split | [DynamoDB Split-for-Heat](references/dynamodb-split-for-heat.md) | 설계 |
| 미정 | Merge | 미정 | - |
| 미정 | 재배치 | 미정 | - |

## 코드 위치

알고리즘 코드는 `cache-core`의 `io.openkache.core.balance`에 둔다. 이 디렉터리에는 설계, 참고 문헌 정리, 실험 설정과 결과 요약만 둔다. 상세는 [design.md](design.md) §7을 따른다.
