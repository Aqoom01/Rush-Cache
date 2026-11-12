# Pre-copy 라운드별 dirty 기록

2026-11-04, S15P31D111-155. 종료 판단에 실제 사용한 dirty 관측과 각 generation의 전송·ACK 경계를 기록한다. 기존 종료 알고리즘과 공통 api/model/spi/runtime 계약은 바꾸지 않는다.

## 독립 실행

B3 자체 메모리 저장소·라우팅·프로토콜 fixture를 직접 실행한다. 공통 Experiment Runner, 실제 KvEngine, Network 서비스나 B1/B2 controller를 기동하지 않는다. 기존 Command·Tablet·Change 등의 자료형은 실험 모델로 재사용한다. 공통 서비스 통합은 현재 개인 작업의 필수 범위에 포함하지 않는다.

저장소 루트에서 아래 명령을 실행한다. Java 21과 Gradle Wrapper만 사용한다. 명시적인 b3Research 작업은 testClasses 빌드 후 실행하며 build/check에서 연구 실행을 자동 호출하지 않는다. copy·민감도·M quick은 기존 Java 결과를 유지하고 Scheduler quick만 목적지가 고정된 324회×2 결과로 교체했다.

    .\gradlew.bat :cache-core:b3Research "-Pb3.args=comparison --profile quick --repeat 2 --output cache-core/build/b3-standalone/comparison"
    .\gradlew.bat :cache-core:b3Research "-Pb3.args=sensitivity --profile quick --repeat 2 --output cache-core/build/b3-standalone/sensitivity"
    .\gradlew.bat :cache-core:b3Research "-Pb3.args=movement --profile quick --repeat 2 --output cache-core/build/b3-standalone/movement"
    .\gradlew.bat :cache-core:b3Research "-Pb3.args=scheduler --profile quick --repeat 2 --output cache-core/build/b3-standalone/scheduler"

B3ResearchLauncher는 기존 backend와 새 literature/policy Java backend를 직접 호출한다. launch.properties·원본·로그·manifest에 실제 Java 인자, 소스/클래스/launcher hash, HEAD·dirty, 반복 상태와 산출물 hash를 남긴다. 새 executionLauncher는 b3-standalone-java다. 과거 Python 보조 실행기의 결과는 삭제하고 새 Java 결과로 교체했다. 상세는 [Java 안내](../java/README.md)를 따른다.

공통 Runner용 기존 config도 호환 경로로 유지하지만 독립 실행의 의존성은 아니다. backend의 --rebuild 진입점은 실행 방식과 무관하게 같은 JSON을 검증한다.

## 관측 경계

PreCopyProtocol.roundEvents()는 값이나 key payload를 보관하지 않는 불변 관측 목록을 반환한다.

- STARTED: snapshot 또는 직전 delta의 마지막 ACK 뒤, collecting 집합을 sending generation으로 교체한 시점.
- ACKNOWLEDGED: 그 generation의 마지막 delta ACK를 실제 확인한 시점. 전송 예산을 모두 소비했어도 ACK가 없으면 기록하지 않는다.
- ABORTED: 진행 중 generation의 부분 전송과 마지막 확인 seq를 남기고 중단한다. 이미 ACK된 generation을 취소로 덮어쓰지 않는다.

프로토콜은 시계를 읽지 않는다. 실험 adapter가 step 뒤 같은 controller 작업 안에서 가상 tick을 붙인다. 한 tick에 이전 generation ACK와 다음 generation 시작이 함께 발생할 수 있다. 각 protocol은 단일 controller thread 계약을 유지한다.

## JSON 필드

새 단일 이동 결과에는 dirtyRounds, 다중 이동·Scheduler 결과에는 jobs[].dirtyRounds를 둔다. Stop/Post-copy는 빈 목록으로 해당 없음을 표시한다.

| 필드 | 의미 |
|---|---|
| generation | final을 포함한 delta generation, 1부터 증가. snapshot은 제외 |
| liveRound / finalRound | 쓰기를 허용하는 라운드 번호 / freeze 뒤 최종 라운드 여부 |
| event / tick | STARTED·ACKNOWLEDGED·ABORTED / caller의 가상 시각 |
| dirtyKeys / putKeys / tombstoneKeys | 시작 경계의 서로 다른 dirty key 수와 당시 값 존재·삭제 수 |
| dirtyBytes | 시작 경계에서 계산한 예상 직렬화 전송량. batch 헤더 포함 |
| previousDirtyBytes | 직전 live generation의 예상량. 첫 generation은 null |
| stopReason | live는 null, 최종은 THRESHOLD·ROUND_LIMIT·NOT_SHRINKING |
| roundSentBytes / sentBytes | 해당 generation에 실제 소비한 bytes / snapshot을 포함한 누적 bytes |
| sequenceBefore / lastSequence / acknowledgedSequence | 시작 전 seq / 생성한 마지막 seq / 실제 확인한 seq |

같은 key에 여러 번 쓰면 해당 collecting generation에서는 한 번만 센다. sending 중 같은 key가 다시 바뀌면 다음 generation에서 다시 관측된다. tombstone과 PUT은 시작 경계의 분류이고 live 전송 직전 값이 바뀌면 실제 전송 레코드와 다를 수 있다.

dirtyBytes는 요청 시도량이나 실제 전송량의 별칭이 아니다. live 중 값 크기·삭제 상태가 바뀌면 실제 전송량과 달라진다. final은 freeze 뒤이므로 예상량과 실제량을 교차 검증한다. 빈 generation도 final barrier를 위해 delta 헤더 30 bytes를 보낸다.

종료 우선순위는 기존대로 임계 이하 → 라운드 상한 → 직전 live 예상량 이상이다. NOT_SHRINKING은 증가뿐 아니라 같은 양도 포함한다.

## 원본·파생 자료와 호환성

| backend | 새 실행 schema | 이전 원본 읽기 |
|---|---|---|
| S1~S5·S3 민감도 | b3-comparison-json-2 | b3-comparison-json-1 |
| M1~M5 | b3-movement-json-2 | b3-movement-json-1 |
| 선행 Scheduler | b3-scheduler-json-2 | b3-scheduler-json-1 |

새 schema는 dirtyRounds 누락, 잘못된 generation·시각·종료 판단·seq·전송량을 거절한다. M/Scheduler에서는 기존 evidenceHash의 jobs에 상세가 포함되며 checksum을 다시 계산해도 논리 검증을 생략하지 않는다. 이전 schema는 원래 지표를 다시 생성할 수 있지만 없었던 dirty 상세를 추정해 만들지 않는다. 다른 schema나 provenance를 한 반복에 섞지 않는다.

검증된 JSON에서 dirty-rounds.csv(이벤트별)와 dirty-rounds.md(generation별 시작→ACK)를 생성한다. M/Scheduler의 기존 rounds.csv는 단계 기록으로 유지한다. 기존 공식 run의 JSON·요약을 수정하지 않으며 재실행은 새 runs 디렉터리에 저장한다. S6 JSON 형식과 가상 증감 모형은 이번 변경에 포함하지 않는다.

## 검증

- dirty key 3 → 1 → 2, 예상량 126 → 52 → 74 bytes를 직접 재현한다. 두 번째 감소 뒤 다시 증가한 generation에서 NOT_SHRINKING으로 멈추고 final ACK·최종 데이터를 검증한다.
- live 도중 값 크기가 바뀌어 예상량과 실전송량이 다른 경우, 삭제, 빈 final, 부분 전송 취소와 final ACK 보류를 검증한다.
- 원본 누락·메타데이터 변조·종료 사유·seq·시각·전송량 변조를 거절하고 재생성 결과의 결정성을 확인한다.
- 비교·민감도·M1~M5·Scheduler의 기존 quick config가 Java 독립 실행기로 새 schema를 생성한다. 전체 구현 테스트와 별도로 새 실행의 원본·상세 자료를 재검증한다.

실행 결과와 정확한 검증 범위는 아래 실행 기록을 따른다. 이 기록은 상세 관측 검증이며 기존 정식 성능 비교를 대체하거나 실환경 성능으로 환산하지 않는다.


## Java 독립 실행 검증 기록

| 묶음 | 반복당 실행 | 반복 | 합계 | 결과 |
|---|---:|---:|---:|---|
| comparison | 240 | 2 | 480 | [결과](../java/comparison-quick/runs/run-2026-11-04T14-55-06.926518900Z-557730f9/repeat-001/results.md) |
| sensitivity | 675 | 2 | 1350 | [결과](../java/sensitivity-quick/runs/run-2026-11-04T14-55-12.989821300Z-e66014f7/repeat-001/results.md) |
| movement | 84 | 2 | 168 | [결과](../java/movement-quick/runs/run-2026-11-04T14-55-22.792700700Z-2040a744/repeat-001/results.md) |
| scheduler | 324 | 2 | 648 | [결과](../java/scheduler-quick/runs/run-2026-11-04T15-38-18.319241800Z-1fd9258e/repeat-001/results.md) |

합계 2,646회. 각 묶음의 두 반복 summary.csv·dirty-rounds.csv·dirty-rounds.md가 일치했다. copy·민감도·M은 이전 검증 버전, Scheduler는 현재 수정 버전이다. 원본 재생성·manifest 검증은 [전체 Java 감사](../java/results.md)에 포함했다. reports의 반복 번호는 별도 provenance이며 scientific 결과를 바꾸지 않는다. 이전 Python 보조 실행 결과와 중간 실행 폴더는 삭제했다.

dirty 3→1→2와 bytes 126→52→74의 단위 검증에서 감소 뒤 증가하면 NOT_SHRINKING으로 전환하는 것을 확인했다. 세부 이벤트는 각 run의 dirty-rounds.csv/md에 남는다. 실제 시각·실제 서버 성능으로 환산하지 않는다.

B3 Java 테스트 138개가 모두 통과했다. 전체 760개 중 성공 755개·기존 Windows NVMe 실패 2개·조건부 skip 3개다. 실패는 NvmeKvStoreTest의 scanReadsValuesFromStorage, recordsInOlderSegmentsRemainReadableAfterRotation이며 변경 전 기준에서도 재현했다. 전체 build는 실패 상태이며 core 운영 의존성 검사는 통과했다.
