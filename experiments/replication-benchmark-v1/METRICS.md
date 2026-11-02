# 계측 정의 v1

기준일2026-10-30. **지표 정의와 O00/V01–V07 로컬 전수 지연/결과 저장 연결. 분산36개 지표군 전체 계측·정식 성능 실측은 미완료.** metric-catalog.json의 M01–M36은 지표군이다. 개별 scalar/분포에는 op/phase/node/path 차원과 유효 sample count·분자·분모·계측 계층이 붙는다. 모든 scalar를 항상 수집 가능하다고 가정하지 않는다.

## 1. 요청·완료·결과 분류

- logical op: (run_id, load_slot, client_id, sequence) 전역 유일 ID. 재시도는 같은 ID와 같은 command/value hash를 보존한다. adapter 내부 message는 logical op 수가 아니다.
- semantic success: GET HIT/MISS, SET OK, DELETE OK/MISS를 API 계약에 맞게 완료한 응답. MISS가 예상 값인지의 correctness는 별도. BUSY/TIMEOUT/UNAVAILABLE/NOT_PRIMARY 및 exception은 성공이 아니다.
- 무응답/timeout은 미확정 반영 가능성을 가지므로 history에서 “실행 안 됨”으로 단정하지 않는다. 확실한 미전송 admission rejection은 별도 구분한다.
- 재시도 진행 중 중간 응답은 attempt outcome이고 logical final은 하나다. deadline 후 late response는 별도 계수하고 이미 마감한 logical outcome을 성공으로 소급 변경하지 않는다.
- cohort: 측정 구간에 admitted된 logical ops 집합. 종료 시 admitted = success + error_response + no_response + pending. 정상 completed export는 pending=0이어야 한다. scheduled-but-not-admitted/drop은 이 등식 밖의 별도 counter다.
- wire/phase별 counter reset을 숨기지 않는다. process 재시작은 새 process_epoch를 발급하고 delta 계산을 재설정한다.

## 2. 시간·처리량

t_due=예정 발행, t_invoke=처음 실제 logical client API 호출 직전, t_attempt_i=각 transport attempt 시작, t_final=최종 응답/terminal 시각. 같은 load process의 monotonic ns를 사용한다.

- service end-to-end latency = t_final−t_invoke. 최초 연결·발견·retry·client/adapter 대기를 포함한다.
- scheduled latency = t_final−t_due; dispatch lag = t_invoke−t_due. closed-loop는 op 생성 시 t_due를 기록한다. open-loop는 이전 응답과 무관한 schedule을 따른다.
- no-response/timeout은 관찰 경과와 censor reason을 저장한다. success histogram에 끼우지 않으며 버리지도 않는다.
- 1초 창 goodput = 해당 창에서 최종 성공한 logical ops/창길이초. completion throughput은 오류 응답까지 포함한 별도 값이다.
- fixed-N cohort goodput = cohort success 수 / controller가 release부터 마지막 terminal 관측까지 잰 makespan. fixed-duration은 admission window와 terminal grace를 구분하고 **window completion rate**와 **cohort deadline success fraction**을 따로 낸다. drain/grace까지 걸린 cohort 성공수를 measure_seconds로 나눠 saturation throughput을 부풀리지 않는다.
- 다중 load host rate를 합칠 때는 동일 controller 관측 구간 또는 offset/error가 검증된 창만 사용한다. 검증하지 못하면 per-host 비정렬 시간열로 저장하고 하나의 정렬된 cluster 시간열이라 하지 않는다.
- 모든 latency/lag는 원본 ns, size는 byte, rate는 ops/s 또는 byte/s, ratio는0..1로 저장한다. 화면에 ms/MiB를 표시할 때 변환한다. wall UTC는 실행 상관·보고용이며 duration clock으로 쓰지 않는다.

## 3. Histogram·percentile 계약

기존8192개 reservoir 기반 혼합 p50/p99를 새로운 전수 계측으로 부르지 않는다. 새 collector는 **모든 관찰 latency**를 op/phase/outcome별 sparse histogram에 넣는다.

- 경계: u0=0, u1=1ns, 다음 u = 이전 u + max(1, ceil(이전 u/100)). 정수식은 이전 u + max(1,(이전 u+99)//100)이다.
- latency x를 x 이하가 아닌 가장 작은 upper bound에 넣는다. 저장은 upper_bound_ns/count, 관찰 n/sum/min/max를 함께 기록한다.
- quantile p는 rank ceil(p×n)의 누적 count가 처음 도달하는 bucket upper bound. 이는 정확한 raw percentile이 아닌 bounded-bucket 상향 근사다. p50/p95/p99/p99.9와 bin 방식·범위·overflow를 함께 내보낸다.
- n=0은 percentile=null(no_samples). overflow가 target rank를 포함하면 finite percentile을 만들지 않고 null(out_of_range)로 둔다. timeout/범위 초과를0으로 넣지 않는다.
- p99.9는 op/outcome별 per-run n≥100000일 때 주요 비교에 사용한다(상위0.1%에 기대100개라는 보고 정책; 신뢰도 증명이 아님). 나머지 percentiles도 n과 표본 적합성 flag를 표시한다.
- histogram count 합은 해당 population observed 수와 같아야 한다. 합치기는 같은 bin version·op·phase·outcome·측정 의미에서만 가능하다. 호환되지 않는 histogram을 임의 보간하지 않는다.
- 1초 창 p99도 n을 표시하고 적은 표본은 LOW_SAMPLE로 표시한다. 전체 run p99는 창 p99의 평균으로 만들지 않는다.
- correctness run은 완전 invocation/response 이력을 저장한다. performance run의 event trace sampling은 별도 설정으로 표시하고 latency histogram sampling과 혼동하지 않는다.

## 4. 집계·반복·비율

- paired5seed 각각의 결과·실행 순서를 보존. 정상 실패 run을 삭제하거나 원하는 결과까지 반복하지 않는다.
- scalar mean·sample SD와95%tCI: n=5에서 mean ±2.776×s/√5. 독립성 가정과 seed/운영잡음의 혼합을 명시. n이 다르면 df에 맞는 상수 또는 계산기를 사용하고 분석 버전을 저장.
- run별 p99, run p99의 평균, 합친 histogram의 p99는 별개 이름으로 저장한다. 평균 p99를 전체 요청의 p99로 부르지 않는다.
- throughput improvement = Tv/To−1, latency reduction =1−Lv/Lo. 같은 seed/block·부하·단위·보장 조건의 O00를 분모로 사용. 분모0/누락이면 null. 각각의 peak load 비교는 공통부하 latency 비교와 따로 표시.
- failure fraction의 분모는 admitted cohort. retry fraction도 logical cohort 기준. 메시지/성공op는 실패·control traffic 포함 여부와 관측창을 명시하고 total counter도 보존한다.
- host write amplification은 logical payload 대비 host block bytes이며 RF와 metadata를 포함한다. SSD NAND WAF가 아니다. SSD SMART의 media bytes를 얻지 못하면 device WAF는 unsupported로 둔다.
- phase간 gauge는 1초 창 end/maximum/time-weighted mean을 구분한다. 단순 sampling mean을 time-weighted라고 부르지 않는다.

## 5. 정확성·장애 시각

- 별도 checker는 GET value/MISS, SET ACK, DELETE 결과, pending completion을 sequential specification과 비교한다. 시간정보 부족이면 인위적인 host간 순서를 만들지 않고 causal barrier/clock bound 또는 불명확 판정을 사용한다.
- original TLA+/모델 검증·기존fixture 결과는 새 Java benchmark correctness 결과가 아니다.
- ack ledger audit는 overwrite 없는 keys 또는 versioned audit namespace를 사용한다. 중간 ACK 값이 나중 합법 overwrite로 사라진 것을 손실로 세지 않는다.
- t_fault_command, t_fault_observed, t_detect, t_view_ready, t_first_success, t_steady_recovery, t_catchup를 분리한다. single controller 관측 또는 clock error bound를 표기한다.
- last-success→first-success gap은 부하 간격이 포함된 client 관측 gap이며 실제 완전 서비스 중단 시간과 다르다.
- steady recovery는 manifest에 고정한 baseline fraction·연속 window수·오류 조건을 충족한 최초 관측으로 정의한다. threshold 미확정 시 raw 시간열만 보고하고 recovery time=null(unqualified_threshold).
- IONIA fast ACK 뒤 적용 지연, Hermes INVALID/read stall/replay는 서로 다른 상태다. ack→all_apply와 ack→all_replicas_local_read_ready를 같은 지표로 치환하지 않는다.

## 6. 누락·결과 파일 계약

metric record: metric_id/status/unit/value/reason/dimensions/numerator/denominator. 정상실측0과 null을 구분한다.

| status | 값 |
| --- | --- |
| measured | 실제 관측 값과 단위·분모, reason=null |
| unsupported | collector/엔진 미지원: value=null + 이유 |
| not_applicable | 해당 protocol/scenario에 개념 없음: null + 이유 |
| not_run | 미실행: null + 이유 |
| insufficient_samples | 수집은 했으나 보고 정책 미달. 원시 count 보존·해석 제한 |
| invalid | clock/drop/reset/수집오류로 무효: null + 이유 |

result envelope의 execution_status/validity와 correctness verdict를 구분한다. required_metrics 누락은 해당 비교를 INVALID 처리한다. optional metric 누락은 명시해 보존하며 다른 방식이 같은 관측범위를 갖는 표에서만 비교한다.

run directory(향후 driver 산출물):

| 파일 | 계약 |
| --- | --- |
| manifest.json | ready 설정·trace/config/source hash·자원/옵션·known limitations |
| environment.json | 실제 관측 환경·storage kind·roles·clock uncertainty, 미확인은 null |
| result.json | result.schema.json envelope, cohort accounting·필수 metric records |
| requests.csv / history.jsonl | correctness는 완전 이력, performance는 지정한 sampled events와 sampling 비율. 민감 payload 대신 value hash 가능 |
| counters.csv / windows.csv | phase/node/op/outcome별 monotonic counter·창 경계·1초 gauge 및 histogram reference |
| histograms.jsonl | bin version/population/count/sum/min/max/sparse buckets |
| events.jsonl | view/fault/replay/drain 사건과 clock domain·process epoch |
| integrity.json | 파일 SHA256·clock/observer 검증·counter 보존식·완전성 결과 |

CSV timestamps는 해당 clock_id와ns 단위를 동반한다. request/key/frame ID는 event에 저장하며 Prometheus label로 추가하지 않는다. Java 제어·업무코드나 기업 소스를 Notion에 복사하지 않고 이 명세와 공개논문 근거만 기록한다.

## 7. 36개 지표군 목록

### M01 — logical_requests

- 단위: count
- 정의: 각 logical_request_id를 scheduled/admitted/dispatched/terminal_response/no_response/pending로 구분한 횟수
- 수집원/차원: client counter / op, phase, outcome
- 구현 상태: PENDING; retry attempt는 새logical op 아님

### M02 — transport_attempts

- 단위: count
- 정의: 동일 logical ID의 첫 송신+재송신 횟수; protocol 내부 message는 M15
- 수집원/차원: adapter counter / op, phase, reason
- 구현 상태: PENDING

### M03 — semantic_successes

- 단위: count
- 정의: GET HIT/MISS, SET OK, DELETE OK/MISS 중 계약에 맞는 완료 응답 수
- 수집원/차원: client response classifier / op, phase, outcome
- 구현 상태: PENDING; 성능 success는 정합성 검사 합격을 대신하지 않음

### M04 — dispatch_lag

- 단위: ns
- 정의: actual_invocation_ns - planned_invocation_ns, 같은 client clock
- 수집원/차원: client scheduler / op, phase
- 구현 상태: PENDING; closed-loop planned=logical 생성 시각

### M05 — goodput

- 단위: logical_ops/s
- 정의: M03 final 응답 시각이 관측창 안에 있는 건수 / 관측창 초
- 수집원/차원: client counter+monotonic window / op, phase
- 구현 상태: PENDING; fixed-N 전체값은 별도 cohort 정의

### M06 — completion_throughput

- 단위: logical_ops/s
- 정의: 관측창 내 final response 건수(오류 포함)/초
- 수집원/차원: client counter / op, phase
- 구현 상태: PENDING; M05와 혼용 금지

### M07 — failure_fraction

- 단위: ratio
- 정의: 해당 cohort의 no-response+오류·거절·deadline종료 logical 수 / admitted logical 수
- 수집원/차원: client terminal ledger / op, phase, reason
- 구현 상태: PENDING; 정상 MISS 제외, 분모0=null

### M08 — retry_fraction

- 단위: ratio
- 정의: attempt_count>1인 logical 수/admitted logical 수; retry attempt 총수도 별도
- 수집원/차원: client terminal ledger / op, phase, reason
- 구현 상태: PENDING

### M09 — late_or_unresolved

- 단위: count
- 정의: logical deadline 이후 응답·run 종료 시 불명확 상태를 구분. 늦은 성공은 원래 deadline 성공으로 재분류하지 않음
- 수집원/차원: client ledger / op, phase, reason
- 구현 상태: PENDING

### M10 — latency_service

- 단위: ns
- 정의: 최초 logical invocation부터 최종 response/terminal까지. 자동 retry·재발견·대기를 포함; success/failure별 분리
- 수집원/차원: client monotonic timestamps+histogram / op, phase, outcome
- 구현 상태: PENDING; no-response 경과는 censored outcome

### M11 — latency_scheduled

- 단위: ns
- 정의: planned invocation부터 terminal까지; scheduler 지연 포함
- 수집원/차원: client scheduler+histogram / op, phase, outcome
- 구현 상태: PENDING; client queue 지연 누락 금지

### M12 — client_inflight

- 단위: requests
- 정의: admitted - logical terminal, 시각별 gauge; queue/rejected 별도
- 수집원/차원: client gauge / client, phase
- 구현 상태: PENDING; 1초 최대/평균/끝값

### M13 — ack_to_apply

- 단위: ns
- 정의: 같은 노드에서 logical ACK 관측과 해당 request apply 사이의 시간 또는 coordinator 관측 round-trip 상한, 방법 라벨 필수
- 수집원/차원: protocol/storage events / replica, op, phase, clock_method
- 구현 상태: PENDING; cross-host 무보정 timestamp 차이 금지

### M14 — apply_backlog

- 단위: requests,bytes,ns
- 정의: accepted/ordered/committed-not-applied 별도 counts/bytes·oldest age·쓰기중단 후 drain latency
- 수집원/차원: protocol gauges+drain probe / replica, phase, state
- 구현 상태: PENDING; ACK/commit/apply 서로 다른단계

### M15 — messages_per_op

- 단위: messages/logical_op
- 정의: 동일 phase/time-window의 client/peer/control 메시지 delta / 성공 op수, 송신 기준
- 수집원/차원: transport counter / node, direction, flow, phase
- 구현 상태: PENDING; control idle 비용과failure수도 병기

### M16 — bytes_per_op

- 단위: bytes/logical_op
- 정의: client/peer wire byte delta/성공 op수, send/receive 분리·계측계층 기록
- 수집원/차원: transport/OS counter / node, direction, layer, phase
- 구현 상태: PENDING; 양끝송수신 합을 단방향비용으로 오인금지

### M17 — connections

- 단위: count,connections/s
- 정의: open/close/error·active·peak·TIME_WAIT, 지속연결과per-message연결 분리
- 수집원/차원: transport/OS / node, phase
- 구현 상태: PENDING

### M18 — cpu_cost

- 단위: cpu_ns/logical_op
- 정의: 모든 replica의 process user+system CPU delta / 성공op수; load hosts 별도
- 수집원/차원: process OS counter / node, role, phase
- 구현 상태: PENDING; CPU 사용률 분모는할당vCPU×wall시간

### M19 — storage_io

- 단위: bytes,ops,ns
- 정의: KV/log별 file API·host block·device 계층의 read/write bytes/IOPS/latency/queue depth
- 수집원/차원: storage hooks+OS block counter / node, device, layer, operation, phase
- 구현 상태: PENDING; kernelcache hit read를물리SSD I/O로세지않음

### M20 — host_write_amplification

- 단위: ratio
- 정의: 같은 phase host block written bytes/logical SET+DELETE payload bytes; KV record logical bytes도 별도
- 수집원/차원: storage+client counters / node, replica_total, phase
- 구현 상태: PENDING; RF·metadata 포함, NAND WAF와 다름; 분모0=null

### M21 — fsync_cost

- 단위: count,ns
- 정의: KV force 호출/소요시간과 stable-view force를 구분
- 수집원/차원: storage hooks / node, path, phase
- 구현 상태: PENDING; replicated RAM durability log와다름

### M22 — cache_behavior

- 단위: count,ratio,bytes
- 정의: engine/OS cache hit·miss·cache size를 계층별 수집
- 수집원/차원: engine/OS supported observer / node, layer, phase
- 구현 상태: PENDING; 미지원이면unsupported,null

### M23 — memory_gc

- 단위: bytes,ns,count
- 정의: heap/RSS/off-heap·allocation·GC pause·history/log 메모리·시간추세
- 수집원/차원: JVM+process+protocol gauges / node, component, phase
- 구현 상태: PENDING; 메모리 추정과실측표시

### M24 — queue_worker

- 단위: requests,ns,ratio
- 정의: owner/I/O worker queue size·wait·busy·active 및 같은키 실행중 수
- 수집원/차원: worker hooks / node, worker, phase
- 구현 상태: PENDING

### M25 — ionia_path

- 단위: count,ns,bytes,ratio
- 정의: Meta INDEX/DATA·view probe·fast/slow/fallback·stale rejection·History size/trim·1RTT-candidate vs measured RTT 경로 분리
- 수집원/차원: IONIA adapter/hooks / node, path, phase
- 구현 상태: PENDING; 각비율 eligible 분모를 result에기록

### M26 — hermes_path

- 단위: count,ns,ratio
- 정의: INV/ACK/VAL·INVALID age·read block·same-key wait·replay·membership epoch
- 수집원/차원: future Hermes hooks / node, path, phase
- 구현 상태: PENDING; Hermes미구현이면not_applicable,null

### M27 — availability

- 단위: ns,ratio
- 정의: fault trigger/detect/view/first success/steady recovery/catch-up 사건,1초창성공률·마지막성공부터다음성공gap
- 수집원/차원: controller+client+protocol / node, fault_id, phase, clock_method
- 구현 상태: PENDING; 관측gap과실제blackout구분

### M28 — correctness

- 단위: count,status
- 정의: history VIOLATION/PASS/INCONCLUSIVE·bad values·duplicate effects·checker controls
- 수집원/차원: offline history checker / scenario, seed, op
- 구현 상태: PENDING; 검사기 timeout은PASS아님

### M29 — acked_survival

- 단위: count,status
- 정의: overwrite없는audit key의 ACK ledger와recovery후검증 결과,missing/duplicate 분리
- 수집원/차원: audit ledger+post-recovery reads / fault_id, op
- 구현 상태: PENDING; snapshot equality만으로충분하지않음

### M30 — recovery_work

- 단위: bytes,count,ns
- 정의: state transfer/log replay bytes·records·복귀중service성능·catch-up target frontier
- 수집원/차원: recovery hooks+controller / node, fault_id, phase
- 구현 상태: PENDING

### M31 — batch_behavior

- 단위: items,bytes,ns,ratio
- 정의: 실효batch items/bytes·fill trigger·enqueue→send 대기·inflight·batch실패
- 수집원/차원: batch hooks / variant, flow, phase
- 구현 상태: PENDING; logical op 수와frame 수분리

### M32 — apply_parallelism

- 단위: keys,workers,ratio
- 정의: 실제동시applyingkeys K·actual I/Oworkers W·고정stripe S·동일키순서
- 수집원/차원: variant hooks / variant, node, phase
- 구현 상태: PENDING

### M33 — selection_adaptation

- 단위: count,ns,ratio
- 정의: replica별선택/회피/탐색·관측RTT·관측age·outstanding·외부변화후적응시간
- 수집원/차원: selector hooks / variant, replica, phase
- 구현 상태: PENDING

### M34 — batch_coupling

- 단위: requests,ns,count
- 정의: frame하나의 지연/실패로영향받은개별요청수·HOL·cancel후점유
- 수집원/차원: V07 hooks+fault fixture / frame_id, fault_id, phase
- 구현 상태: PENDING; frame_id는event에만,Prometheus label금지

### M35 — repeat_uncertainty

- 단위: ratio,logical_ops/s,ns
- 정의: 5개paired run별 통계·mean/sample SD/95%tCI; throughputratio와latencyreduction 분모명시
- 수집원/차원: offline aggregator / variant, scenario, seed
- 구현 상태: PENDING; percentile 평균은pooled percentile아님

### M36 — instrumentation_overhead

- 단위: ratio
- 정의: 동일입력에서 observer on/off의 goodput/latency차이·observerCPU/bytes
- 수집원/차원: pilot paired experiment / observer, scenario
- 구현 상태: PENDING; 정확성/필수안전계측off 금지



## 8. 로컬 전수 지연·결과 export 구현 반영 (2026-10-30)

위 PENDING 목록은 최초 명세 시점의 상태를 보존한다. 현재 O00의 closed/open-loop에서 phase/op/outcome별 전수 service/scheduled/dispatch-lag histogram과 표준 result.schema envelope를 구현했다. M01/02/03/04/07/08/10/11의 단일 JVM 관찰을 제공하고, 나머지 및 M09의 불완전한 phase별 late ledger는 unsupported/null+사유로 둔다. 1초 창·자원/프로토콜 지표·전체 history·ACK audit·remote/분산 실행은 후속이다.

추가28/전체635검사, 실제 로컬3/5노드 두 실행 방식의 모든 CRUD population, 산출물13개 및 음성 대조4개를 확인했다. percentile은 bucket 상향 근사이며 p99.9는 n<100000이면 주요 비교 부적합이다. 설정 snapshot은 local manifest로서 ready 분산 manifest가 아니고 template은 draft/disabled다. LATENCY_EXPORT.md와 MEASUREMENT_VALIDATION.json을 따른다.

## 9. V02–V07 공통 연결 계측 범위 (2026-10-30)

추가99/전체760검사와 결과48개 대조를 통해 O00/V01–V07 전체8개 adapter의 공통 지연/계수/로컬 export 연결을 확인했다. 새 Metadata.adapterConfiguration은 client/정책/profile·초기 membership·제한/socket timeout을 저장하며 기존7인자 호출과 histogram 계약을 유지한다. ADAPTERS_V02_V07.md/REMAINING_ADAPTERS_VALIDATION.json 참조.

V07 raw batch frame은 logical Meta/일반 RPC와 구분하며 전체 peer message/bytes를 계측한 것으로 표시하지 않는다. 기존36개 지표군의 미지원/미관찰·원격/1초 창·history/ACK audit·clock/overhead 한계는 유지한다. V06 phase간 학습 상태의 자동 이전이 없어 cold/steady 학습과 워밍업 정책을 정식 비교 전에 확정한다. 공통 정확성·필수 계측을 보강한 후 본시험 데이터/요청열 후보·EC2 재개 시 inventory/pilot을 진행한다.

## 10. 제한된 correctness history·ACK 감사 구현 (2026-11-02)

M28/M29의 **단일 JVM·소규모 기능 검증 부분**을 별도 correctness 산출물로 추가했다. 이전 PENDING/후속 표시는 당시 기록이며 위 전체36개 지표의 분산 수집 완료를 뜻하지 않는다. CORRECTNESS_HISTORY.md와 CORRECTNESS_VALIDATION.json을 따른다.

- 전수 key/value/결과·submit/callback·pending/거절/미응답 이력을 보존한다. 제한된 전역 GET/SET/DELETE 선형성 검사는 알려진 초기 상태를 요구하고 uncertain·한도 초과를 PASS로 바꾸지 않는다. checker의 최대24operation 한도를 본시험 천만 요청에 확장해 적용했다고 보고하지 않는다.
- 별도 예약·배타적·덮어쓰기 없는 key의 SET/DELETE ACK와 READY 이후 실제 GET을 감사한다. O00/V01–V07·3/5노드에서 모든 복제본 적용 후와 follower 정상 close/재기동·생존 다수 복구 후 서비스 수준의 값/부재를 확인한다. ACK 직후 apply 전 crash·leader kill·전원 손실·원격 장애 검증은 아니다.
- correctness.json/history.jsonl/ack-ledger.json/ack-audit.json·integrity/marker를 저장한다. 독립 Python 검증기로 hash·payload·계수·witness replay·ACK 증거를 대조하며 VIOLATION/INCONCLUSIVE도 유효한 저장 결과일 수 있다.
- 기존 표준 성능 result의 M28/M29 `not_evaluated`는 그대로다. 별도 correctness artifact의 run/slot/clock/history token/hash로 증거를 구분하며 공통 envelope 자동 통합은 후속이다. 전수 payload·탐색 비용이 성능 mode와 같다고 가정하지 않는다.

필수 host/JVM·protocol/storage·1초 창·clock/observer overhead·분산 관찰과 V06 학습/워밍업 정책은 남아 있다. RAM durability log·stable View·선택적 KV fsync는 변경하지 않았다.

## P02 JVM 원시 관찰의 제공 범위 (2026-11-02)

별도 `benchmark-jvm-observation-v1` sidecar가 process CPU·heap/non-heap·GC count/time과 client endpoint pending을 저장한다. CPU/GC는 누계와 실제 snapshot interval 차이값이며 개별 GC pause가 아니다. 1초 ticker는 목표 주기이며 jitter·stage 경계·마지막 짧은 구간과 실제 source/collection 시각을 그대로 보존한다. terminal 요청 창은 run anchor 기준1초와 원래 LOAD/WARMUP/MEASURE cohort를 별도 보존한다. 자원 interval과 요청 창을 임의로 나누어 CPU/op를 만들지 않는다.

RSS/allocation/개별 pause는 null+reason, peak pending은 sampled lower bound이고 true peak는 null이다. 같은 JVM의 replica 비용을 중복 합산하지 않는다. 초기 baseline·counter reset·필수 source 누락·clock 오류·late callback·수집 cap/종료 오류·실행 cohort 불일치를 대조하며, 정상34/명시 invalid2 bundle과 변조6개를 독립 reader로 검증했다. 상세는 JVM_OBSERVATIONS.md 및 JVM_OBSERVATION_ARTIFACT_CHECK.json을 따른다.

기존 성능 result와 기본7파일 무결성 계약은 그대로다. M18/M23은 전체 역할/replica/source coverage·clock·분모/프로필 통합 전 unsupported를 유지한다. shared-fixture local subset valid는 정식 CPU/op·RSS·SSD 성능 적합성 완료가 아니다. GC 시간 출처는 MXBean의 누적ms→ns이며 ns 정밀도의 개별 pause를 관측했다는 뜻이 아니다. 원격 clock·host/device 계측·계측 오버헤드 적합성은 후속이다.


## TCP raw 계측 연결 (2026-11-02)

P03은 sender frame·initiator exchange·logical outcome을 분리해 기록한다. sender에 request/reply/peer를 각1회 합하고 receive/exchange 복사본은 더하지 않는다. JDK stream의4B prefix·protocol header/body 포함 확인된 bytes이며 TCP/IP packet·전달 ACK·host 전체 TIME_WAIT는 아니다. 불확실 부분 write는 exact bytes를 null로 두고 invalid로 남긴다. local socket 상태·event 처리 순서 peak를 OS 전체 연결/진짜 peak로 표시하지 않는다. 구독 이전 연결은 null이다.

같은 monotonic source의 subscription anchor·terminal 창·시작/완료 stage를 보존하고 frame event와 exchange admission cohort의 경계 차이를 명시한다. 실패가 없는 흐름만 reply send↔read를 정확히 대조하고 실패 시에는 진단으로 보존한다. 표준 result M15/M16/M17은 전체 process/role coverage·clock·분모/프로필 통합 전 unsupported를 유지한다. 기존 schema/default7파일은 변경하지 않았다. 전체949검사·독립 산출물43건의 실제 범위/근거는 NETWORK_OBSERVATIONS.md와 관련3개 JSON에 있다.

