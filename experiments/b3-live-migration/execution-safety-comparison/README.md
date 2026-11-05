# 안전한 축소·다중 이동 장애 연구 실험

실제 B3 Stop-copy/Pre-copy/Post-copy와 MigrationScheduler를 자체 인메모리 fixture에서 실행한다. 논리적 장애와 복구를 주입해 소유권·데이터·제거 경계를 확인한다. 실제 DB 제품이나 물리 서버 장애 실험은 아니다. [문제·선행 방법·지표·결과](../execution-validation.md)를 함께 본다.

## 선행 방법과 적용 범위

| 선행 사례 | 이번 적용 | 제한 |
|---|---|---|
| [ScyllaDB decommission](https://docs.scylladb.com/manual/stable/operating-scylla/nodetool-commands/decommission.html) | 남은 용량·노드 수/복제 하한·peer 상태 사전 검사, 모든 Tablet 이동/정리 후 제거 | 복제 수는 노드 수 하한만 검사. 실제 replica placement·repair·제품 decommission 없음 |
| [MongoDB 8.0 shard draining](https://www.mongodb.com/docs/v8.0/tutorial/remove-shards-from-cluster/) | 이동 가능한 Tablet부터 drain, balancer/jumbo/용량/관리 metadata 때문에 제거 보류 | db-primary는 관리 플래그와 명시적 완료 ACK 모형. 실제 DB/unsharded collection 이전 없음 |
| [TiKV PD operator](https://github.com/tikv/pd/blob/master/pkg/schedule/operator/operator.go) | job별 실패·timeout·취소·재시도, 관련 작업과 무관한 작업 분리 | Raft operator/snapshot 재현이 아니라 B3 복사 방식에 상태·실패 격리 원칙 적용 |
| [FoundationDB SIGMOD 2021](https://www.foundationdb.org/files/fdb-paper.pdf) | 같은 seed로 같은 논리 장애·event timeline을 재생하는 실제 B3 코드 검사 | 제어 정책 비교 대상이 아닌 검증 방법. FDB simulation engine·실제 network/disk/process crash 탐색은 없음 |

원 시스템이 세 복사 방식을 모두 사용했다는 뜻이 아니다. 선행 사례의 안전 원칙·검증 방법을 B3의 세 복사 방식 각각에 적용했다.

## 실행과 조건

저장소 루트에서 JDK 21과 Gradle Wrapper만으로 실행한다. Java 독립 실행기로 full 두 반복 870회를 재실행했다. 실제 N/U/R 시나리오는 실행하지 않았다. 공통 Experiment Runner는 필요 없다.

    .\gradlew.bat :cache-core:b3Research "-Pb3.args=safety --profile full --repeat 2 --output experiments/b3-live-migration/execution-safety-comparison/runs"

b3Research 작업이 testClasses를 먼저 준비한다. main 직접 실행도 가능하며 [Java 안내](../java/README.md)를 따른다. 원본에서 재생성하려면 Windows classpath 구분자 ; 를 사용한다:

    java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.migration.experiment.ExecutionSafetyResearchMain --rebuild <run-folder>/repeat-001

Full은 Scylla 7조건 + MongoDB 9조건 + PD 9조건을 세 복사 방식·각 seed 5개로 실행하고, FDB식 장애 주입은 세 방식·seed 20개로 실행한다. 반복당 435회, 두 반복 870회다. Quick은 각 seed 2개와 FDB seed 4개로 반복당 162회다.

각 실행은 4 jobs, Full Tablet당 128 key(Quick 48), global 동시 4, cluster 4,096 bytes/tick, node 1,024 bytes/tick이다. MongoDB 변형은 endpoint 동시 1, 다른 변형은 endpoint 동시 2다. 따라서 방법 간 raw makespan을 동일 정책의 제품 성능 순위로 비교하지 않는다. job timeout 200 tick, 전체 관측 상한 5,000 tick이다.

- Scylla: 정상/용량 부족/복제 하한/peer 불가/전환 전 source 불가/전환 후 장애/부분 cleanup 재시도.
- MongoDB: 정상/용량 부족/balancer 중지/jumbo/관리 metadata 잔존/전환 전 source 불가/전환 후 장애/cleanup 재시도/drain metadata reload.
- PD: 전환 전 source·destination 불가/전환 후 장애/timeout/cleanup 재시도/중복 취소/shared source/shared destination/source 영구 미복구.
- FDB식: seed로 source·destination 불가/전환 후 장애/timeout/cleanup 재시도/중복 취소 중 하나를 결정한다. 모든 가능한 장애 순서를 탐색한 것은 아니다.

## 장애 의미와 검증

전환 전에는 실제 protocol abort로 source 소유권을 유지하고 새 protocol로 재시도한다. 전환 후에는 destination 소유권을 되돌리지 않고 기존 protocol의 복구·정리를 진행한다. 논리적 노드 불가·driver 정지와 검출 후 fail/abort를 주입하며 TCP·SSD·운영체제 프로세스를 끊지는 않는다. controller-reload는 drain 관리 metadata의 JSON roundtrip/reload이며 실행 중 protocol 객체는 유지한다. durable process recovery 검증이 아니다.

cleanup은 source 일부를 지운 뒤 false를 세 번 반환하고 네 번째에 성공하도록 주입한다. 전환 후 부분 정리 중단을 소유권 rollback으로 처리하지 않는지 확인한다. source 영구 미복구에서는 Stop/Pre는 source 소유권을 유지하고 Post는 이미 공개한 destination을 유지하되 미도착 read는 UNAVAILABLE로 남긴다. 완료·노드 제거로 표시하지 않는다. 복구 성공을 증명한 경우와 구분한다.

검사는 성공한 GET/SET/DELETE의 독립 oracle, destination 최종 데이터, 무관한 guard Tablet, Tablet당 단일 owner/epoch 전환, 매 tick byte·동시 상한, 미완료 Tablet이 있는 노드의 제거 금지를 포함한다. 실패한 시도까지 포함해 전송 bytes를 누적한다. 핵심 지표는 제거 여부·잔여 Tablet·초기 실패 jobs·복구 전 완료한 무관한 jobs·소유권 commit·BUSY/UNAVAILABLE·재시도 bytes·event 순서다. 실제 지연 ms나 원 제품 처리량으로 해석하지 않는다.

## 결과와 원본

정식 run: run-2026-11-04T14-54-57.666215300Z-2bdc9f56. 반복당 435회에서 REMOVED 150, COMPLETED 180, REJECTED 45, BLOCKED 45, HELD 15다. 모든 예상 상태·정합성·예산 검사를 통과했으며 위반 0이다. 거부·보류는 의도한 안전 동작이지 이동 완료가 아니다.

manifest/launch.properties에 HEAD·dirty 상태·source/backend/launcher hash·실제 호출을 보관한다. results/case-*.json에는 입력·plan·preflight·jobs·event journal·evidence hash가 있다. JSON에서 summary.csv·results.md·events.csv를 재생성한다. 누락/추가 원본·다른 실행 혼합·입력/증거 변경·완료/제거 경계 위반은 거절한다. 원본·manifest·상세 events는 실행 PC, 요약 CSV/Markdown은 Git 보관 대상이다.

- [반복 1 결과](runs/run-2026-11-04T14-54-57.666215300Z-2bdc9f56/repeat-001/results.md) · [CSV](runs/run-2026-11-04T14-54-57.666215300Z-2bdc9f56/repeat-001/summary.csv)
- [반복 2 결과](runs/run-2026-11-04T14-54-57.666215300Z-2bdc9f56/repeat-002/results.md) · [CSV](runs/run-2026-11-04T14-54-57.666215300Z-2bdc9f56/repeat-002/summary.csv)
