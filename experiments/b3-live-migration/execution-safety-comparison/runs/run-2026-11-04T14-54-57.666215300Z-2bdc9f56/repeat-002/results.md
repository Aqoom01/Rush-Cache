# 안전한 축소·다중 이동 장애 검증

profile=full; 435회. 실제 세 protocol을 자체 메모리 fixture에서 실행했다. 실서버·원 제품 재현이 아니다.

| 방법 | 복사 방식 | 실행 | 완료/제거 | 거부 | 보류 | 미복구 유지 | 위반 |
|---|---|---:|---:|---:|---:|---:|---:|
| scylla-drain | stop-and-copy | 35 | 20 | 15 | 0 | 0 | 0 |
| scylla-drain | pre-copy | 35 | 20 | 15 | 0 | 0 | 0 |
| scylla-drain | post-copy | 35 | 20 | 15 | 0 | 0 | 0 |
| mongodb-drain | stop-and-copy | 45 | 30 | 0 | 15 | 0 | 0 |
| mongodb-drain | pre-copy | 45 | 30 | 0 | 15 | 0 | 0 |
| mongodb-drain | post-copy | 45 | 30 | 0 | 15 | 0 | 0 |
| pd-operator | stop-and-copy | 45 | 40 | 0 | 0 | 5 | 0 |
| pd-operator | pre-copy | 45 | 40 | 0 | 0 | 5 | 0 |
| pd-operator | post-copy | 45 | 40 | 0 | 0 | 5 | 0 |
| foundationdb-fault | stop-and-copy | 20 | 20 | 0 | 0 | 0 | 0 |
| foundationdb-fault | pre-copy | 20 | 20 | 0 | 0 | 0 | 0 |
| foundationdb-fault | post-copy | 20 | 20 | 0 | 0 | 0 | 0 |

진행 중 상태를 완료로 바꾸지 않는다. 노드 제거는 모든 Tablet 이동·정리와 관리 metadata 확인 뒤에만 수행한다. 실패 전 작업의 bytes도 재시도 전송량에 포함한다. 논리적 노드 불가·정지와 검출 후 abort/fail을 주입하며 실제 TCP·디스크 crash는 재현하지 않는다.
