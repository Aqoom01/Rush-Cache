# B3 범위 수정 후 Java 재실행 결과

2026-11-05. 이전의 증감·배치 결합 실험과 CAMIG 포함 Scheduler 결과를 삭제했다. **노드 수 판단만 바꾸고 배치는 fixed-tablet-lpt-v1 하나로 고정**해 실행했다. Slicer·split/merge·동적 복제·목적지 선택 경쟁은 제외했다. 원문의 판단을 공통 실행 조건에 적용한 연구 변형이며 원 시스템 전체의 성능 재현이 아니다.

## 이번 변경으로 새로 실행한 것

| 묶음 | 프로필 | 반복당 | 반복 | 실행 합계 | 첫 반복 |
|---|---|---:|---:|---:|---|
| literature | full | 280 | 2 | 560 | [보고서](../scaling-decision/runs/run-2026-11-04T15-35-13.070058Z-9d35b480/repeat-001/results.md) |
| literature | quick | 112 | 2 | 224 | [보고서](../java/literature-quick/runs/run-2026-11-04T15-35-57.870091200Z-e1d32bf5/repeat-001/results.md) |
| policy | full | 720 | 2 | 1440 | [보고서](../execution-policy-comparison/runs/run-2026-11-04T15-35-13.124853700Z-40147861/repeat-001/results.md) |
| policy | quick | 96 | 2 | 192 | [보고서](../java/policy-quick/runs/run-2026-11-04T15-35-51.511237100Z-676fe48c/repeat-001/results.md) |
| scheduler | full | 810 | 2 | 1620 | [보고서](../scheduler-comparison/runs/run-2026-11-04T15-35-13.088917500Z-4ead3854/repeat-001/results.md) |
| scheduler | quick | 324 | 2 | 648 | [보고서](../java/scheduler-quick/runs/run-2026-11-04T15-38-18.319241800Z-1fd9258e/repeat-001/results.md) |

총 **4,684회**다. 판단 비교는 7판단+고정 노드 대조군, 지연 비교는 AWS/ElastMan/P-Store, Scheduler는 같은 이동 계획의 9실행 정책이다. 같은 seed의 두 반복은 재현 검증이며 독립 표본으로 합치지 않는다. Full과 Quick도 다른 크기의 별도 모형이다.

- 원본에서 파생 CSV/Markdown 60개 재생성·hash 일치.
- 반복 간 결정적 파생 24개·scientific evidence 2342개 일치. 실제 planning 시간은 별도 보존·비교 제외, 보고서 반복 번호는 정규화.
- 모든 입력·원본·manifest 파일 hash·누락·중복·완료 수 검사. 증감 원본의 모든 배치를 공통 adapter로 다시 계산해 같음을 확인했고 split/merge/복제/같은 수의 재배치를 거부했다.
- source SHA-256: ec4b8c88e7b4c0ea0de49c3ff6052e8800716dde6a90796e4add8eb90a867ce7
- class SHA-256: b2776c8206d25978391ffd6e739ff8edb5624d18f194641778269af50f25695d
- 실행 기준 HEAD는 063c817, 미커밋 수정 포함. [감사 기록](run-audit.json)이 각 run 경로와 manifest hash를 기록한다. 원본·manifest는 로컬, 요약 CSV/보고서는 Git 보관 대상이다.

B3 테스트 138개 모두 통과. 전체 760개 중 성공 755개·기존 NVMe 실패 2개·조건부 skip 3개다. 실패는 NvmeKvStoreTest의 scanReadsValuesFromStorage와 recordsInOlderSegmentsRemainReadableAfterRotation이며 변경 전에도 재현한 항목이다. 전체 build는 실패 상태이며 production 외부 의존성 검사는 통과했다.

## 변경 영향 없이 유지한 순수 Java 기록

| 묶음 | 기존 실행 수 | 기록 |
|---|---:|---|
| safety | 870 | [기존 결과](../execution-safety-comparison/runs/run-2026-11-04T14-54-57.666215300Z-2bdc9f56/repeat-001/results.md) |
| comparison | 480 | [기존 결과](../java/comparison-quick/runs/run-2026-11-04T14-55-06.926518900Z-557730f9/repeat-001/results.md) |
| sensitivity | 1350 | [기존 결과](../java/sensitivity-quick/runs/run-2026-11-04T14-55-12.989821300Z-e66014f7/repeat-001/results.md) |
| movement | 168 | [기존 결과](../java/movement-quick/runs/run-2026-11-04T14-55-22.792700700Z-2040a744/repeat-001/results.md) |

위 네 묶음은 이전 source/class hash에서 검증한 별도 기록이며 이번 수정으로 재실행했다고 세지 않는다. [보존 명세](retained-runs.json)를 따른다. 기존 S1~S6·M 정식 결과도 각 문서에 보존한다. 이전 6,157회를 현재 범위의 단일 성과 수로 사용하지 않는다.

[7판단 결과](../scaling-decision/results.md) · [준비 지연·안전성](../execution-validation.md) · [고정 계획 Scheduler](../scheduler-comparison/results.md)

**실제 N/U/R 시나리오는 0회다.** 수정된 [실서버 계획](../real-node-plan/README.md)은 7판단×고정 배치×9 Scheduler×3복사×2drain×3흐름×5seed = 환경당 5,670회이며 계획만 생성했다. 실제 I/O·프로세스 재시작·metadata 영속성·실제 p99는 검증하지 않았다.
