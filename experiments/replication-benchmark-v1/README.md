# 공통 복제 벤치마크 명세 v1

기준일: 2026-10-30. **시나리오·계측 명세 v1 + 단계1 논리 trace + 단계2 O00 장기 client·결과 집계 구현. 공통 부하 스케줄러·계측/export·성능 실측은 미완료.**

사용자 요청에 따라 EC2 접속·사전진단은 보류한다. 이 디렉터리에는 원격 접속·프로세스 종료·네트워크 변경·벤치마크 실행 도구가 없다. `execution_enabled=false`인 template을 기존 Runner에 넣어 실행하지 않는다.

## 산출물

- [ADAPTER_O00.md](ADAPTER_O00.md): 단계2 O00 client·retry/deadline·결과/시도/RPC 집계·로컬 연결.
- [B2_VALIDATION.json](B2_VALIDATION.json), [B2_SOURCE_PRESERVATION.json](B2_SOURCE_PRESERVATION.json): 단계2 검사·기존 코드 보존 증거.

- [TRACE_GENERATOR.md](TRACE_GENERATOR.md): 단계1 생성기·ID/분할·binary/hash 계약·로컬 실행.
- [trace-small.properties](trace-small.properties): 작은 생성기 인수 예제. 최종 성능 workload가 아님.
- [B1_VALIDATION.json](B1_VALIDATION.json): 생성기 검사·CLI hash 대조·기존 회귀와 보호 파일 확인 기록.

- [SCENARIOS.md](SCENARIOS.md): 공통 절차, API·3/5노드·데이터·부하·27개 시나리오와 판정.
- [METRICS.md](METRICS.md): 36개 지표군, 시간/분모/단위·histogram·반복 통계·결과 파일 계약.
- [IMPLEMENTATION_GAP.md](IMPLEMENTATION_GAP.md): 현재 Java 코드와 계측 명세의 차이·후속 구현 순서.
- [scenario-catalog.json](scenario-catalog.json), [metric-catalog.json](metric-catalog.json): machine-readable 목록.
- [manifest.schema.json](manifest.schema.json), [plan.template.json](plan.template.json): Draft-07 run 설정 schema와 **실행 불가 초안**.
- [result.schema.json](result.schema.json), [result-shape.example.json](result-shape.example.json): 결과 envelope schema와 **측정값 없는 형식 예시**.
- [Validate-Spec.ps1](Validate-Spec.ps1): PowerShell 7 `Test-Json`으로 로컬 문서/JSON 일관성만 검사. AWS/네트워크/Java 서버를 호출하지 않는다.
- `validation.json`: 마지막 명세 형식·교차 참조 검사 결과이며, benchmark/correctness 실행 결과가 아니다.
- `source-preservation.json`: 이번 작업 전후 보호 대상 src 파일214개의 목록·SHA-256 동일성 확인. 별도 디렉터리의 모든 파일을 전수 hash한 결과는 아니다.

## 확정된 계약 / 실제 실행 전에 정할 것

| 명세로 고정 | 실행 전 결정·검증 |
| --- | --- |
| 각 EC2에 replica1개, 기본3/5복제본; 기존 O00/V01–V07 독립 비교 | 실제 instance/주소/AZ/storage·용량·메모리·배타적 사용 |
| GET value/MISS, SET blind ACK, DELETE 결과 반환 경로 구분 | 정확한 서버·client adapter와 capability 증거 |
| 성공/오류/미응답·logical op/attempt/message 구분 | 워밍업·요청수·시간·동시성·deadline·disk budget pilot 적합성 |
| client 종단 지연·노드별 비용·복구·정확성·변형 내부 지표 | clock 품질·계측 오버헤드·history checker·장애 주입 구현 |
| null+사유, 원시집계·manifest hash·paired 반복 | SLO 값과 재현 가능한 데이터/요청열 생성 |

100만/1000만키, 100B/1024B값, 1000만요청×5seed, warm-up 최소120초·측정 최소300초는 **계획용 시작값**이다. pilot 없이 optimal/ready로 확정하지 않는다. 3/5노드에서 같은 logical dataset을 각 replica가 보유하므로 메모리·디스크 예산은 복제·로그·과거 append까지 고려한다.

## 보존

기존 `cache-core`, `cache-server`, `experiment-runner` 소스·API·의존성·테스트·설정은 보존한다. 단계1에서 `experiment-runner`에 독립 `benchmark`/`benchmarkTest` 영역을, 단계2에서 공통 client 계수와 `cache-server`의 별도 O00 실험 adapter를 추가했다. 기존 application JAR/배포 의존성과 서버 wire는 보존한다. 이 명세가 기존 모든 변형이 완성됐거나 Hermes가 구현됐다는 증거가 아니다. 과거 V04a/b·V05·V06 파일명은 보존하고 현재 표시 ID V01–V07과 매핑한다.

## 검증 명령

```powershell
pwsh -NoProfile -File ./experiments/replication-benchmark-v1/Validate-Spec.ps1
```

명세 검사에 통과해도 `plan.template.json`은 `draft`이다. 이 검사는 실제 EC2 환경·driver 동작·논문 안전성·성능을 검증하지 않는다.

## 근거

문헌 조사/원문 확인 범위는 Notion 04-2(81건 대조·2건 추가확인)를 따른다.

- https://app.notion.com/p/4084ed9024378144bf1ce5eca63972e4
- O00 계약: https://app.notion.com/p/4064ed9024378122a673e1b4a7467ca1
- 복구 계약: https://app.notion.com/p/4064ed9024378169a3eac3697244c9c3
- IONIA §4.5/§5: https://www.usenix.org/system/files/fast24-xu.pdf
- Hermes §5–6/§8: https://users.cs.utah.edu/~vijay/papers/asplos20a.pdf
- YCSB §3–5: https://cgi.di.uoa.gr/~ad/M155/Papers/p143-cooper.pdf
