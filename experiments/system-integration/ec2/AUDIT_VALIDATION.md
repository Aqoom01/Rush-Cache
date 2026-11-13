# 전체 방법론 통합 감사의 검증 기록

보고 기준일: 2026-11-13 (Asia/Seoul). [구현·지원 범위](METHOD_AUDIT.md), [선택 키와 원본 source](method-inventory.csv), [빌드·실행 근거 JSON](method-audit-build-validation.json), [독립 JVM 결과 CSV](method-audit-native-validation.csv).

## 무엇을 보완했는가

- A3의 기존3개 실행 선택 외에 Lab14개 프로필을 원본 판단 class와 실제 UDP/TCP 통신으로 연결했다. 총17개 선택은 중복 계열·대조군을 포함하며17개 독립 알고리즘을 뜻하지 않는다. F7C Buddy도 실제 대상 notification/refutation을 수행한다.
- A2 V06의 ADAPTIVE/ROUND_ROBIN을 별도 실험 축으로 추가하고, 읽기 제한·RTT·cooldown·탐색 주기, MetaBatch budget과 V05 storage shard 설정을 전달한다.
- B3 원본 fixed-tablet-lpt 배치 대조군을 FIXED_LPT로 연결했다. B1/B2/B3 factory와 현재 구현 class를 대조하는 검사도 추가했다.
- 실제 실행에서 발견한 B2 Slicer의 단일 노드 축소 무한 반복을 수정했다. 같은 노드로의 가상 이동을 부하 감소로 오인하던 공용 Assignment 계산이 원인이었다. 모든7개 배치 정책의 단일 노드·orphan drain 경계 검사를 추가했다.
- CSV를 실제 순차 실행하는 batch 명령, 방법론별 설정 검증, 양쪽 A 계층의 FD 상태 확인, 실패·미지원·cleanup 구분을 추가했다.

A1은 통합 캐시의 복제된 승인·소유권·승계 계층이고 A2는 실제 SSD/file KV 복제·읽기 계층이다. 각 replica JVM에 두 엔진이 함께 실행된다. A1의 단독 데이터 복제 실험과 동일한 역할 배치라는 뜻은 아니다.

## 빌드와 JVM 내부 검사

`./gradlew build --offline --console=plain`은 성공했다. 총2,326개 중2,314개 통과, 기존 skip12개, 실패·오류0개다. 이와 별도로 standalone Lab의 `test selfTest`가112개 테스트와 자체 검사를 통과했다. Python controller 검사는16개다. 정확한 task별 개수와 XML timestamp는 JSON에 있다.

| 통합 검사 | 개수 | 확인한 범위 |
|---|---:|---|
| JointCacheTest | 198 | A1 7개 × A2 읽기 정책 포함9변형 × copy3개=189조합과 경계·실패 검사9개. 실제 TCP native 엔진 사용 |
| LabFailureRuntimeTest | 17 | 새14개 FD 프로필의 실제 소켓 교환·정지 peer 판정, registry 완전성, helper 증거 부재 처리 |
| LabJointCacheTest | 14 | 각 Lab FD를 A1 VR과 A2 O00 양쪽에 적용. 기존 leader 종료·승계·값 보존·POST 이동 |
| FixedLptIntegrationTest | 3 | 원본 고정 배치 planner 연결·copy별 증감·빈 단위 drain. backend 대역을 쓰는 정책 검사 |
| MethodInventoryTest | 2 | 현재 B1/B2/scaler source와 실행 factory 대조, A2 설정 전달 |

같은 JVM의 TCP fixture와 독립 프로세스 실험을 구분한다. 이 테스트 수는 EC2 성능 실험 횟수가 아니다. FullIntegratedCacheTest 등 기존 검사도 전체 빌드에 포함된다.

## 독립 JVM 실행

모든 실행은 로컬 공유 호스트의 독립 JVM과 native TCP/file KV를 사용했다. EC2를 실행하거나 인스턴스를 생성하지 않았다. 기능 검사 목적의 단축 FD 설정이며 실제 제품 기본 timeout의 성능 순위로 해석하지 않는다. 동일 호스트의 다른 작업 영향도 통제한 성능 실험이 아니다.

| 캠페인 | 시도 / PASS | 구성·목적 |
|---|---:|---|
| method-audit-r1 | 17 / 16 | 실행당8 JVM. 새14개 Lab FD 모두, A1 7개·A2 8개, B1 7개, scheduler9개를 분산 선택. 추가3행은 V06 ROUND_ROBIN + FIXED_LPT의 STOP/PRE/POST |
| method-audit-r2 | 1 / 1 | 위 실패 행13을 Slicer 수정 후 같은 선택·부하로 재실행 |
| method-audit-fault-r1 | 5 / 5 | 실행당11 JVM. CHAIN + V06 ROUND_ROBIN + POST 이동 중 실제 JVM 종료. 주요5개 FD의 양쪽 계층 판정·승계·복구 |
| method-audit-buddy-r1 | 1 / 1 | 8 JVM. VR + V07 + F7C. 실제 leader JVM 1초 SIGSTOP/SIGCONT와 값 검사 |

합계24회 시도 중23회 PASS, 과거 RUN_ERROR1회다. PASS 실행의 측정 요청3,360개 중3,278개 성공,82개 미수락, 요청 실패·miss·stale read·최종 audit 오류는 모두0이다. 미수락을 성공 요청으로 세지 않는다. PASS 실행은 correctnessValid / migrationValid / coverageValid / jointComponentsValid / fdRuntimeValid가 모두 true다. 모든24회 cleanup이 완료됐으며, 별도 PID·시작 시각·명령줄 대조에서 소유 프로세스207개가 모두 비활성임을 확인했다.

정상 캠페인은 행마다120요청, rate20, warmup16, concurrency8,16keys와2그룹 중 초기1그룹을 사용했다. FD heartbeat100ms, deadline800ms, probe100ms, confirm200ms, Phi threshold2로 단축했다. 장애 캠페인은240요청,8keys,1,024bytes,3그룹 중 초기2그룹과 읽기100%다. 구체 입력·seed·설정 SHA는 각 행의 manifest/plan에 보존한다.

### 실패는 성공으로 덮어쓰지 않았다

`method-audit-r1/row-000013`은 ROSE + V05 + AEROSPIKE_MESH + ORDER + HEAT + SLICER + iter-gwin + PRE + incremental-drain이다. 노드1개로 축소할 때 Slicer가 종료되지 않아 부하 실행 완료·최종 audit에 도달하지 못했다. 해당 결과는 RUN_ERROR이며 정합성 통과로 취급하지 않는다. 실패 console·gateway·요청·cleanup을 그대로 보존했다.

Assignment 수정 후 `method-audit-r2/row-000013`이 같은 조합으로120요청 성공, audit 오류0, 필수 검증 모두 true를 기록했다. 통과16행을 최종 수정 JAR로 재실행했다고 주장하지 않는다. 최종 JAR은 이 회귀 실행, 장애5회, Buddy1회에서 사용됐고 전체189조합 Java fixture도 통과했다. 실행별 bundle SHA로 구분한다.

### 장애 검사와 FD 관측

| FD | 성공 / 미수락 / 요청 실패 | 종료되지 않은 프로세스에 대한 DOWN 보고 |
|---|---:|---:|
| F3 | 215 / 25 / 0 | 0 |
| REDIS_CLUSTER | 227 / 13 / 0 | 0 |
| PHI_GOSSIP | 235 / 5 / 0 | 2 |
| IGNITE_TCP_RING | 224 / 16 / 0 | 0 |
| AEROSPIKE_MESH | 229 / 11 / 0 | 0 |

모두 실제 종료된 peer의 DEAD를 양쪽 A 계층에서 관측했고 최종 audit을 통과했다. Phi의2건은 종료된 gossip relay의 영향으로 다른 살아 있는 프로세스가 DOWN으로 보인 관측이다. 데이터를 보존했더라도 오판정이 없었다고 결론 내리지 않는다. 이 작은 기능 검사의 성공 수로 FD 우승자를 정하지 않는다. Ignite는 coordinator가 살아 있는 장애만 검사했으며 coordinator 재선출 기능은 없다.

Buddy 검사에는 heartbeat200ms, deadline1,200ms, probe100ms, confirm400ms를 썼다. 정지했다 복귀한 replica의 최종 누적 buddyNotifications는 A1=1, A2=2로 실제 메시지 경로를 확인했다.120요청 중108성공·12미수락·실패0, 최종 audit 오류0이다. 표본별 누적 계수를 합산해 이벤트 수를 부풀리지 않는다.

## 조합 입력과 남은 범위

`matrices/smoke.csv`는189행, `matrices/coverage.csv`는1,650행(275전략 × 2부하 × 3seed)이다. exhaustive는 호환 전략61,419,708개, 부하·seed를 포함해368,518,248행으로 count-only만 확인한다. 생성한1,650행이나 전체 Cartesian을 실험 완료한 것은 아니다. 이번 독립 JVM 입력은 `matrices/audit-native-{smoke,fault,buddy}.csv`에 있다.

현재 구현의 핵심 제약은 고정 Ignite coordinator,3멤버에서 F6 helper1개, B1 Ring의 소유권 제약, 고정 LPT와 동적 split의 충돌이다. 구현이 없는 독립 B2 Merge·퇴역 CAMIG·PAX의 원제품 부가기능·gateway HA·물리 EC2 증감을 실행 가능한 방법론으로 세지 않는다. 자세한 거절 조건은 [METHOD_AUDIT.md](METHOD_AUDIT.md)를 따른다.

모든 원본은 `runs/<캠페인>/row-XXXXXX/`에 있다. runs는 대용량·PID 등 로컬 실행 정보이므로 Git과 배포 ZIP에서 제외한다. JSON/CSV 요약에는 원본 경로·SHA와 실행 당시 library/controller 해시가 남는다. ZIP은 최종 r2 JAR과 실행 Python을 유지하면서 이 문서를 포함하도록 다시 묶으며, 새 문서 때문에 tools receipt와 전체 bundle manifest 해시는 달라질 수 있다.
