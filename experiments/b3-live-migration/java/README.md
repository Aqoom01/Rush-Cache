# Java B3 모듈과 고정 배치

Java21/JDK로 B3를 실행한다. 기존 작업은 063c817에 커밋했고, 이후 B2 범위가 섞인 증감 비교를 수정했다. 공통 KV/Network/B1/B2/Runner 서비스 연결과 새 Gradle 모듈·외부 production 의존성은 추가하지 않았다.

| 경계 | 현재 역할 |
|---|---|
| NodeCountPolicy.Decision | 노드 수·판단 사유·상세만 반환. 배치를 전달할 필드가 없음 |
| 7개 선행 판단 | SCADS/ElastMan/E-Store/Accordion/P-Store/TIRAMOLA/PAX의 수 판단 변형 |
| AWS warm-up | 준비 지연 별도 비교의 추가 제어 정책 |
| FixedTabletPlacement | 모든 정책에 fixed-tablet-lpt-v1 하나만 적용 |
| SchedulerAlgorithms | 입력 계획을 바꾸지 않는 실행 정책 9개. CAMIG 삭제 |
| CopyDrivers | 실제 B3 Stop/Pre/Post protocol 선택·publish·install·cleanup |
| DrainStrategies | preflight-all / incremental-drain의 용량·잔여 작업·metadata guard |
| B3Workflow | 자체 Environment/Binding으로 관측→판단→READY→고정 계획→복사→drain→제거 연결 |

Slicer·E-Store 세분화·SCADS 복제·Accordion MILP 재배치·CAMIG 목적지 선택은 활성 코드·실험에서 제거했다. PAX는 replica/hint 전제를 제거한 count-only 변형이다. 각 수 판단의 원문 대비 변경은 [비교 명세](../scaling-decision/README.md)에 있다.

## 설정과 실행

[strategies.properties](strategies.properties)에서 판단·Scheduler·복사·drain을 선택한다. planner는 fixed-tablet-lpt만 허용한다. 모르는 이름이나 예전 Slicer/CAMIG/native planner를 조용히 대체하지 않는다. 판단의 PI/Q/이력 상태는 실행마다 새로 만든다.

    .\gradlew.bat :cache-core:b3
    .\gradlew.bat :cache-core:b3 "-Pb3.args=--validate experiments/b3-live-migration/java/strategies.properties"
    .\gradlew.bat :cache-core:b3 "-Pb3.args=--plan experiments/b3-live-migration/real-node-plan"

기본은 목록만 출력한다. validate와 plan은 실험을 실행하지 않는다. B3ResearchLauncher를 명시 호출하면 가상 실험의 Java 자식 JVM을 실행하고 실제 인자·HEAD/dirty·source/class/launcher hash·완료 수·산출물 hash를 저장한다. build/check는 연구 실험을 자동 실행하지 않는다.

    .\gradlew.bat :cache-core:b3Research "-Pb3.args=literature --profile full --repeat 2 --output experiments/b3-live-migration/scaling-decision/runs"
    .\gradlew.bat :cache-core:b3Research "-Pb3.args=policy --profile full --repeat 2 --output experiments/b3-live-migration/execution-policy-comparison/runs"
    .\gradlew.bat :cache-core:b3Research "-Pb3.args=scheduler --profile full --repeat 2 --output experiments/b3-live-migration/scheduler-comparison/runs"

[현재 결과·실행 근거](results.md)를 사용한다. 기존 결합형 증감·배치 비교와 CAMIG 포함 실행 결과는 삭제했다. 순수 복사·안전성·M 실험은 영향을 받지 않은 기존 기록으로 별도 보관한다. 과거 Python JSON 몇 개는 호환 checksum·calibration 회귀 테스트 fixture이며 활성 실험 결과가 아니다.

원본이 있는 실행 PC에서 다음 명령은 실험 재실행 없이 파일 hash·파생 재생성·반복 evidence를 검사한다.

    java -Xmx2g -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" experiments/b3-live-migration/java/B3RunAudit.java

JDK Newton logistic·Jacobi least-squares·SPAR/DP·ARIMA conditional least-squares·결정적 2-means를 사용한다. 기존 Python RNG/solver와 같은 수치를 주장하지 않는다. planning 실제 벽시각은 별도 timings.csv이며 과학적 evidence에서 제외한다.

ComparisonPlotMain은 기존 순수 Java full S1~S5 CSV의 다섯 그림을 Java2D로 생성한다. 표시 범위는 seed min/max, 중심은 (min+max)/2다. [그림 근거](../migration-comparison/figures/sources.json)를 따른다.

**실제 N/U/R 시나리오는 실행하지 않았다.** B3Workflow의 단위 계약과 가상 실험이 실제 TCP/SSD/EBS·프로세스 재시작·metadata 영속성·실제 p99 검증을 대신하지 않는다.
