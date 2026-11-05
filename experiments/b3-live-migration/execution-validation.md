# B3 문제·방법·지표·현재 검증

2026-11-05. 증감 판단과 실행을 B3 범위로 정리했다. B2의 split/merge·일반 부하 균등화·목적지 선택 경쟁을 제외한다. 배치 가능성은 모든 판단에 동일한 adapter와 제거 안전 guard로 검사하며 Accordion MILP/CA 배치 알고리즘 경쟁을 다시 실행하지 않는다.

| B3 문제 | 적용 방법 | 지표·현재 근거 |
|---|---|---|
| 언제·몇 노드 증감할지 | 선행 수 판단 7개 + 고정 대조군 | [고정 배치 결과](scaling-decision/results.md), 모형 기한·노드 사용량·반전·미완료 |
| 준비·전송 동안 늦거나 과잉 반응 | AWS warm-up / ElastMan / P-Store | 같은 배치에서 full 720×2, quick 96×2 |
| 여러 이동의 endpoint 경합 | 고정 계획 Scheduler 9개 | [실행 비교](scheduler-comparison/results.md), 시간·예산·BUSY·기한 |
| dirty가 줄지 않음 | Pre-copy 임계·상한·NOT_SHRINKING | [라운드 상세](migration-comparison/dirty-rounds.md) |
| 소유권 전환·취소·source 장애 | 실제 B3 Stop/Pre/Post protocol | [S6](migration-comparison/interruption-results.md), 단일 owner·데이터·FAILED 구분 |
| 일부 job 실패·중복·timeout | PD식 job 격리·재시도, FDB식 seed 장애 재생 | 기존 safety 390회, event/evidence·영향받지 않은 작업 |
| 잔여 데이터·관리 metadata 상태에서 제거 | Scylla식 사전 검사, MongoDB식 부분 drain | 기존 safety 480회, 제거/거부/보류·잔여 Tablet·ACK |

## 새 준비 지연 결과

[Full 반복 1](execution-policy-comparison/runs/run-2026-11-04T15-35-13.124853700Z-40147861/repeat-001/results.md) · [CSV](execution-policy-comparison/runs/run-2026-11-04T15-35-13.124853700Z-40147861/repeat-001/summary.csv) · [반복 2](execution-policy-comparison/runs/run-2026-11-04T15-35-13.124853700Z-40147861/repeat-002/results.md)

아래는 full 한 반복의 방법당 240조건·seed 결과에 대한 산술 평균이다. 운영 가중 평균이 아니며 높은 초과율을 목표 충족으로 표시하지 않는다. 모든 방법의 배치는 fixed-tablet-lpt-v1이다.

| 판단 | 모형 기한 초과율 | allocated node-windows | 종료 미완료 action 합계 |
|---|---:|---:|---:|
| aws-warmup | 27.367% | 310.542 | 86 |
| elastman | 36.610% | 280.033 | 105 |
| pstore | 31.540% | 278.217 | 123 |

## 기존 안전성 기록

현재 판단 수정이 실제 copy/drain/실패 fixture의 동작을 바꾸지는 않았다. [기존 safety 결과](execution-safety-comparison/runs/run-2026-11-04T14-54-57.666215300Z-2bdc9f56/repeat-001/results.md) 435×2=870회를 이전 검증 버전의 별도 기록으로 유지한다. 새로 실행했다고 세지 않는다.

반복당 REMOVED150·COMPLETED180·REJECTED45·BLOCKED45·HELD15. 검사한 데이터·단일 owner·예산·상태 경계 위반은 0이며 거부/보류/미복구를 이동 성공으로 세지 않았다. Scylla의 복제는 노드 수 하한, MongoDB의 db-primary/reload는 관리 metadata 모형이다. 실제 replica repair·DB 이동·프로세스 영속 복구를 증명하지 않는다. PD Raft·FDB simulation engine을 사용하지 않는다.

실행별 현재/보존 버전과 hash는 [전체 감사](java/results.md)에 구분했다. B3 테스트 138개 모두 통과. 전체 760개 중 성공 755개·기존 NVMe 실패 2개·조건부 skip 3개다. 실패는 NvmeKvStoreTest의 scanReadsValuesFromStorage와 recordsInOlderSegmentsRemainReadableAfterRotation이며 변경 전에도 재현한 항목이다. 전체 build는 실패 상태이며 production 외부 의존성 검사는 통과했다.

실제 N/U/R 전체 폐루프는 실행하지 않았다. B3Workflow의 단위 계약에 더해 자체 실제 I/O·네트워크·디스크·프로세스 복구와 성능 측정이 남아 있다. 공통 서비스 통합을 필수로 추가하지 않는다.
