# E5·E6 다중 이동 스케줄러와 drain

같은 노드의 Tablet을 모두 옮길 때 이동 시작 정책이 전체 완료 시간, 이동별 평균 완료 시간, 서비스 p99에 어떤 영향을 주는지 비교한다. 이동이 끝나면 소스의 잔여 Tablet과 정합성 oracle을 확인한다.

## 구성

| 항목 | 설정 |
|---|---|
| B1 서버 | 4대, 노드당 Tablet 8개, 총 32개 |
| 데이터 | key 100만, key 16 B, value 1 KiB, 사전 채움 |
| 부하 | Zipf 0.99, seed 42, open-loop 5,000 req/s, GET/SET/DELETE 40/30/30 |
| 측정 | 600,000 요청, 예정 시각 기준 120초 |
| Drain | 측정 10초 후 node 3(일반 노드)의 Tablet 8개를 B1과 같은 `HashRangeScheme.onNodeRemoved`로 배치 |
| 이동 | E4와 같은 pre-copy, job별 대역 상한 0, batch 64 KiB, 종료 임계 256 KiB, 최대 10 라운드 |

사전 채움 기준 Tablet 하나는 약 32.5 MB이고 소스 전체는 약 260 MB다. E4에서 260 MB Tablet의 pre-copy 전송이 약 8초 걸렸다. 여기서는 순차 8개 각각에 그 8초를 잡아 64초로 예산을 추정하고, 10초에 시작하는 120초 측정에서 약 46초를 추가 여유로 둔다. 실제 완료 시각이 측정 안에 드는지는 결과에서 확인한다.

네 조건 모두 node 3을 drain 소스로 쓴다. 현재 B1에서 Controller는 고정이므로, 일반 노드 하나를 비우는 운영 상황을 비교한다.

| 설정 | 시작 정책 | 같은 소스의 동시 이동 상한 |
|---|---|---:|
| `0-sequential.properties` | 기존 순차 이동 | 1 |
| `1-fifo.properties` | 실행 가능한 오래된 계획부터 시작 | 4 |
| `2-mongodb.properties` | 송신과 수신을 합쳐 노드당 1개 | 1 |
| `3-slamig.properties` | SLAMIG의 기한 기반 순서와 endpoint 충돌 제한 | 1 |

## 실행

GitLab CI에서 실험을 실행할 때 `NODES=4`, `CONFIG=experiments/cluster-integration/e5-e6-drain`을 지정한다. 폴더의 네 설정이 순서대로 실행된다. 실행 절차와 결과 다운로드는 [인프라 README §10](../../../infra/README.md#10-gitlab-ci로-실행)을 따른다. 현재 Runner는 한 실행에서 같은 drain을 초기화해 반복하는 기능이 없으므로 폴더 전체를 독립적으로 3회 실행한다. 조건별 평균·최소·최대를 기록한다. [통합 실험 설계](../design.md)는 한 런 안에서 초기화 후 3회 반복하는 방식을 제안한다.

## 결과 읽기

- `cluster-<id>.json`의 `migrations[]`에서 `queuedMs`, `sentMs`, `finishedMs`를 비교한다. 모두 측정 시작 기준이다. `sentMs`가 시작 요청 시각이며, 재시도 시 마지막 요청 시각이다. `admissionRetries`는 재대기 횟수다. 완료는 `phase: 4`로 확인한다.
- `scheduler`에서 정책·동시 상한과 함께 `admissionOrder`, `maxObservedConcurrent`, `makespanMs`(첫 수락된 시작 요청부터 마지막 성공 완료까지), `meanJobCompletionMs`(성공한 이동의 대기열 진입부터 완료까지의 평균)를 읽는다. 완료가 없으면 두 시간 지표는 0이다. 실패가 섞인 실행은 성공한 drain과 구분한다. 순차 drain도 같은 요약을 기록한다. `policy`는 `sequential`, 동시 상한은 전체·노드별 각각 1, `tickUnit`은 `not-applied`다.
- `drain`에서 `node`, `atMs`, `planner`, `plannedMoves`, `completedMs`, `remainingTablets`, `nodeRemoved`를 확인한다. 계획된 이동이 모두 완료되고 잔여 Tablet이 0일 때 `completedMs`가 기록된다. `consistency`의 `lostSet`, `wrongValue`, `staleHit`가 모두 0인지 보고 `unverified`와 `uncertainExcluded`도 함께 확인한다.
- `timeline-<id>.csv`에서 drain 이전, 진행 중, 완료 후 p99를 비교한다. `sendLag`와 함께 읽어 단일 부하 생성기가 예정 요청률을 따라갔는지 확인한다.

## 해석 범위

- Runner는 `SchedulerAlgorithms.order()`로 시작 시점만 제어한다. byte 할당은 서버가 맡으며 `scheduler.allocation`은 `not-applied`다. `maxObservedConcurrent`는 Runner가 수락 후 종결 상태를 확인하기 전까지 집계한 동시 이동 수다.
- 정책 tick은 폴링 한 회다. 논리 tick으로 순서를 정하는 기존 정책과 맞추며, 실제 주기는 5ms 대기와 wire 왕복을 합친 값이다. 기한은 5ms 단위로 올림한다. SLAMIG의 설정값 110초는 drain 시작부터 예정 측정 종료까지의 여유를 모든 이동에 동일하게 적용한 순위 입력이다.
- 이동량은 해시 구간 폭 비율 × key 수 × (key 길이 + value 크기)로 추정한다. dirty 입력은 관측 근거가 없어 0이다. 순위 계산의 노드당 byte 예산은 job별 대역 상한을 5ms로 환산하고, 상한이 0이면 기존 full 시뮬레이션의 4,096 B/tick을 사용한다.
- 이동 시작이 BUSY면 최대 100번 다음 poll로 재대기한다. 전체 제한은 마지막 예정 시각부터 계획 건당 120초다. 수락된 이동의 상태가 120초 안에 확인되지 않으면 후속 시작을 중단한다. 시작 응답을 잃어 수락 여부를 모를 때도 후속 시작을 중단한다. 이때 서버가 이미 수락한 이동은 서버에서 계속될 수 있다.
- 다른 Tablet의 커밋이 먼저 라우팅 표 버전을 올린 경우, 서버는 최신 표에서 대상 Tablet의 id·범위·소유자·epoch가 그대로인지 확인하고 커밋을 다시 보낸다. Controller push가 없으면 50ms 간격으로 GetTable을 최대 16번 보낸다. 대기와 커밋 재시도 동안 freeze를 유지한다. 커밋 재시도는 이동당 최대 16번이며, 새 표를 받지 못하거나 대상 Tablet이 달라지면 실패한다. 커밋 응답이 유실되면 적용 여부가 불명확하므로 쓰기 차단을 유지하고 실패 처리한다.
- `migrations[].server.commitRetries`는 최초 시도를 제외한 실제 재시도 횟수다. 재시도로 늘어난 freeze 시간은 성공한 이동의 `migrations[].server.writeBlockedMs`와 서비스 지연에 반영되므로 FIFO 비용에 포함한다. `remainingTablets`와 이동별 `phase`도 함께 확인한다.
- Drain 뒤에도 서버와 membership은 유지되고 `nodeRemoved`는 항상 `false`다. 1 vCPU 서버에서 동시 이동과 서비스 요청이 CPU를 공유한다. 단일 부하 생성기의 처리량과 반복 간 편차를 함께 고려한다.
