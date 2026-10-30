# 계측 정의 v1

기준일2026-10-30. **지표 정의는 작성, 3/5노드 구현·실측은 미완료.** metric-catalog.json의 M01–M36은 지표군이다. 개별 scalar/분포에는 op/phase/node/path 차원과 유효 sample count·분자·분모·계측 계층이 붙는다. 모든 scalar를 항상 수집 가능하다고 가정하지 않는다.

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


