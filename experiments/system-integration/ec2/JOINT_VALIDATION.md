> 이 파일은 A1+A2 동시 연결 단계의 과거 검증 기록이다. 전체 방법론 감사·보완 후 현재 결과는 [AUDIT_VALIDATION.md](AUDIT_VALIDATION.md)를 따른다.

# A1+A2 동시 통합 검증

A1 요청 승인·소유권 복제와 A2 SSD 데이터 복제를 **동시에 실행한** 새 경로의 검증이다. A1은 기존 데이터 복제 비교와 달리 제어/승인 계층에 적용했다. 기존 A1-or-A2 단일 엔진 16회와 이 결과를 합산하지 않는다. EC2 실험은 실행하지 않았다.

## Java 및 실행 도구

- 전체 `./gradlew build --offline --console=plain` 검증: 최종 결과는 [joint-build-validation.json](joint-build-validation.json)에 기록한다.
- `JointCacheTest`: **177개 통과**. A1 7개 × A2 8개 × STOP/PRE/POST =168개 실제 TCP/파일 KV 조합 검사와 나머지9개 경계 검사다.
- 168개는 저장·조회·삭제 → B3 이동 → A1의 실제 최종 B1 snapshot과 승인 fingerprint를 검사한다. A1에 사용자 value가 들어가지 않는 것도 확인한다. 같은 JVM 안의 native replica fixture이며 독립 JVM 168회 성능 실험은 아니다.
- A1 VR/PB/PB_FENCED leader만 중단한3개 검사, A2 O00 leader만 중단한1개 검사에서 다른 계층을 계속 실행하고 승인·쓰기·읽기 재개를 확인했다. SWIM을 실제 UDP로 실행했다.
- A1 승인 불가 시 A2 미전송, 모호한 routing 기록 시 전체 요청 차단/기존 local owner 유지, legacy/joint 혼합 설정 거절, 내부 RPC의 일시적 승인 실패 재시도, A2 불확정 쓰기 미재실행을 각각 검사했다.
- 기존 CRAQ 증감 fixture가 tail 성공 직후 head ACK 처리 중 BUSY를 즉시 실패로 처리하던 조건을 수정했다. 같은 요청 ID로 제한 시간 내 재시도하며 native 알고리즘을 변경하지 않았다.
- Python 실행 도구11개 검사를 통과했다. 기본 smoke168행과 coverage744행은 A1/A2를 **각각 별도 축**으로 담는다. 이 CSV는 생성된 입력이며744개 독립 JVM 실험을 실행한 것은 아니다.

## 독립 JVM 실행

각 실행은 3그룹 × replica JVM3개 + gateway + load =11 JVM이다. 각 replica JVM에 양쪽 엔진과 계층별 FD가 공존한다. 한 loopback 호스트에서 실제 TCP/UDP/파일 KV를 사용했다. 최종 재검증4회와 수정 전4회를 모두 남겼다. 합계8회/측정 요청1,920개다. 로컬 기능 리허설이며 성능 순위나 EC2 처리량의 근거가 아니다. 최종 재검증은 전체 테스트와 호스트를 공유했다.

최종 `runs/joint-r2`:

| 조합 | 실제 제어/복사 | 측정 결과 | 이동/판단 | 최종 audit·이동 |
|---|---|---|---|---|
| VR + V07 + Lifeguard | TAG/HEAT/GREEDY, SCADS, tidb-feedback, POST, incremental-drain | OK200/MISS40, 실패0/미수락0 | split3회, 이동4개, 판단10회, active2→1 | 통과 |
| PB + O00 | ORDER, PRE, SCALE_CYCLE, DELETE20% 포함 | OK144/MISS96, 실패0/미수락0 | 이동8개, active2→3→1 | 통과 |
| RAFT + V03 + SWIM | SLOT/HYSTERESIS, STOP, SCALE_CYCLE | OK240, 실패0/미수락0 | 이동11개, active2→3→1 | 통과 |
| CHAIN + V06 + SWIM | POST, CRASH_SCALE, A1 head 기준 소유 JVM SIGKILL | OK226, 실패0/미수락14 | A1 master prefix 복구 및 A2 view recovery 후 이동2개 완료 | 통과 |

모든 최종 실행에서 `jointComponentsValid`, `coverageValid`, `correctnessValid`, `migrationValid`가 true다. gateway `components`에 양쪽 method와 승인/data 호출/소유권 기록 계수가 있다. native resource snapshot은 양쪽 HTTP 진단의 PID가 같은 것을 보여 준다. 자동 제어 실행에는 실제 관찰 창·split·placement·scaler·scheduler·migration 이벤트가 남아 있다.

**수정 전 실패를 보존했다.** `runs/joint-r1/chain-v06-crash-run`에서는 이동 중 A1 승계의 BUSY를 B3가 즉시 source 장애로 처리해 Post-copy가 FAILED_AFTER_PUBLICATION/SAFE_HOLD로 멈췄다. audit8개 중1개가 UNAVAILABLE였고 나머지7개 값 해시는 일치했다. correctness/migration을 false로 기록했다. A1의 안전한 미전송 BUSY만 원래 deadline·동일 A2 요청 ID로 기다리도록 고친 뒤 같은 시나리오를 재실행해 통과했다. A2 기록 후의 모호한 쓰기는 TIMEOUT으로 유지해 재실행하지 않는다.

PB 계열은 실패한 쓰기를 자동 재전송하지 않는 기존 계약을 지키면서 **다음** 요청이 승계 노드를 탐색하도록 adapter를 보완했다. 이 경로를 PB/PB_FENCED 단독 계층 장애 검사로 확인했다.

## 증거와 해석 범위

- [joint-local-validation.csv](joint-local-validation.csv): 수정 전후8행 모두, 요청·audit·이동·정합성 판정 및 원본 SHA.
- [joint-build-validation.json](joint-build-validation.json): 전체 Gradle task별 집계와 각 실행 요약.
- 원본: `runs/joint-r1/`, `runs/joint-r2/`. plan/input SHA, JAR/tool SHA, 요청 CSV·audit, 양쪽 native 상태/FD, CPU/RSS/IO, gateway journal, crash 확인과 master repair를 로컬에 보관한다. 저장소의 runs ignore 규칙은 유지한다.
- 각 회차 `post-run-process-check.json`은 실행한 PID44개가 모두 inactive임을 OS의 PID·시작 식별자로 다시 확인했다. 총88개 프로세스가 남지 않았으며 cleanup/collection은 모두 완료됐다.
- 최종 코드로 사용한 배포물은 `/tmp/openkache-joint-r2`다. 최종 ZIP을 다시 만들 때 library receipt와 실행 Python 파일이 이 배포물과 같음을 확인한다. 문서/검증 결과의 추가로 전체 tools receipt는 달라질 수 있다.

A1×A2×copy의 기능 조합 검사와 모든 B 정책 고차 조합의 실제 성능/장애 실측을 구분한다. native PB/Rosé/Chain의 보장, 단일 gateway와 준비된 그룹의 논리 증감, calibration/history 조건은 [구현 범위](../FULL_SYSTEM_SCOPE.md)를 따른다. 최적 조합 판정은 [실험 계획](EXPERIMENT_PLAN.md)에 따른 EC2 반복 측정에서 수행한다.
