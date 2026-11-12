# A1 실서버 종합 비교 (PB·VR·Raft·Chain·CRAQ·Rosé 변형)

12단계. 새 측정 없이 각 cohort의 요약 CSV를 같은 축으로 모았다. **cohort마다 부하·장애 모델·감지 모델이 다르므로 한 표의 숫자를 순위로 읽지 않는다.** 각 행의 조건 열을 함께 본다. 모든 cohort는 전용 EC2 m6gd.medium 3대(같은 AZ)·인메모리 KV·private TCP·heartbeat50ms·tick10ms이며 아래 값은 FD/대기 500ms 조건의 3회 중앙값이다.

## 같은 축으로 모은 표 (값64B / 8KiB, 500ms 조건)

|방식|값 B|정상 SET p50/p99 ms|peer B/SET|failover ms (최소~최대)|감지 이후 전환 ms|감지 모델|장애·보장 조건|cohort|
|---|---:|---|---:|---|---:|---|---|---|
|PB (crash-only 고정 승계)|64|3.09/7.99|402|512 (489~520)|13.6|heartbeat timeout 500ms|settled idle 리더 SIGKILL|election 36회|
|VR INCREMENTAL|64|4.31/8.97|930|518 (513~553)|51.7|heartbeat timeout 500ms|settled idle 리더 SIGKILL|election 36회|
|Raft|64|3.01/7.29|514|570 (533~622)|7.5|선거 timeout [500,1000)ms 난수|settled idle 리더 SIGKILL|election 36회|
|VR INCREMENTAL_CATCHUP|64|3.37/7.81|664|미측정|미측정|heartbeat timeout 500ms(동일 엔진)|장애 미주입|catchup 24회(전송 비교)|
|Chain TAIL_ONLY (head crash)|64|5.50/10.51 (BUSY 재시도 포함)|641|564 (553~569)|38.3|controller 고정 대기 500ms(감지기 없음)|settled idle, 외부 master FREEZE/REPAIR/OPEN|chain 36회|
|Chain TAIL_ONLY (middle crash)|64|5.00/11.82 (BUSY 재시도 포함)|641|546 (541~553)|33.1|controller 고정 대기 500ms(감지기 없음)|settled idle, 외부 master FREEZE/REPAIR/OPEN|chain 36회|
|Chain TAIL_ONLY (tail crash)|64|5.25/10.48 (BUSY 재시도 포함)|641|542 (541~546)|33.6|controller 고정 대기 500ms(감지기 없음)|settled idle, 외부 master FREEZE/REPAIR/OPEN|chain 36회|
|Rosé 변형 L=0|64|22.18/189.37 (client 8개 동시)|미집계|596 (594~598)|64.9|controller 고정 대기 500ms(감지기 없음)|쓰기 진행 중 Primary SIGKILL, OK 유실 최대 0건(상한 0)|rose 30회|
|Rosé 변형 L=4|64|20.80/149.83 (client 8개 동시)|미집계|597 (596~597)|68.3|controller 고정 대기 500ms(감지기 없음)|쓰기 진행 중 Primary SIGKILL, OK 유실 최대 2건(상한 4)|rose 30회|
|Rosé 변형 L=∞|64|13.08/89.89 (client 8개 동시)|미집계|602 (594~610)|78.6|controller 고정 대기 500ms(감지기 없음)|쓰기 진행 중 Primary SIGKILL, OK 유실 최대 168건(상한 ∞)|rose 30회|
|PB (crash-only 고정 승계)|8192|3.18/6.32|16659|489 (473~537)|16.4|heartbeat timeout 500ms|settled idle 리더 SIGKILL|election 36회|
|VR INCREMENTAL|8192|4.06/14.63|58720|595 (578~608)|131.2|heartbeat timeout 500ms|settled idle 리더 SIGKILL|election 36회|
|Raft|8192|2.98/5.96|16772|616 (550~786)|8.9|선거 timeout [500,1000)ms 난수|settled idle 리더 SIGKILL|election 36회|
|VR INCREMENTAL_CATCHUP|8192|3.80/7.39|16947|미측정|미측정|heartbeat timeout 500ms(동일 엔진)|장애 미주입|catchup 24회(전송 비교)|
|Chain TAIL_ONLY (head crash)|8192|5.50/12.86 (BUSY 재시도 포함)|16901|562 (560~563)|38.4|controller 고정 대기 500ms(감지기 없음)|settled idle, 외부 master FREEZE/REPAIR/OPEN|chain 36회|
|Chain TAIL_ONLY (middle crash)|8192|5.55/12.02 (BUSY 재시도 포함)|16902|549 (544~559)|36.7|controller 고정 대기 500ms(감지기 없음)|settled idle, 외부 master FREEZE/REPAIR/OPEN|chain 36회|
|Chain TAIL_ONLY (tail crash)|8192|5.35/10.46 (BUSY 재시도 포함)|16901|544 (535~544)|35.8|controller 고정 대기 500ms(감지기 없음)|settled idle, 외부 master FREEZE/REPAIR/OPEN|chain 36회|
|Rosé 변형 L=0|8192|23.62/215.58 (client 8개 동시)|미집계|615 (604~618)|83.4|controller 고정 대기 500ms(감지기 없음)|쓰기 진행 중 Primary SIGKILL, OK 유실 최대 0건(상한 0)|rose 30회|
|Rosé 변형 L=4|8192|20.74/167.96 (client 8개 동시)|미집계|609 (609~619)|82.8|controller 고정 대기 500ms(감지기 없음)|쓰기 진행 중 Primary SIGKILL, OK 유실 최대 1건(상한 4)|rose 30회|
|Rosé 변형 L=∞|8192|12.41/104.27 (client 8개 동시)|미집계|605 (590~615)|72.3|controller 고정 대기 500ms(감지기 없음)|쓰기 진행 중 Primary SIGKILL, OK 유실 최대 188건(상한 ∞)|rose 30회|

## CRAQ 읽기 경로 (별도 cohort, 장애 없음)

|값 B|모드|clean GET p99 ms head/middle/tail|혼합 GET p99 ms|dirty 읽기 합|
|---:|---|---|---:|---:|
|64|TAIL_ONLY|5.35/4.56/2.34|4.34|0|
|64|CRAQ|2.55/2.97/3.09|6.17|424|
|8192|TAIL_ONLY|4.87/4.25/2.65|5.15|0|
|8192|CRAQ|2.77/3.54/3.21|6.53|424|

## 보장·전제 비교

|방식|쓰기 응답 조건|리더/구성 crash 시 성공 쓰기|감지·승계 주체|이번 실측에서 맞춘 축|맞추지 않은 축|
|---|---|---|---|---|---|
|PB|두 Backup 적용 ACK 후|보존(단일 영구 crash 전제)|Backup heartbeat timeout, 고정 승계|부하·값·FD·settled idle crash|분단 안전성·재시작·exactly-once 없음|
|VR|과반수 PREPARE_OK 후|보존|heartbeat timeout, view change|부하·값·FD·settled idle crash|checkpoint·2MiB 로그 한도|
|Raft|과반수 복제 후 현재 term commit|보존|난수 선거 timeout, 선출|부하·값·FD 기준값|선거 timeout 범위가 PB/VR과 다름, 영속화 없음|
|Chain|tail 적용·결과 중계 후(역방향 Ack 전에는 다음 쓰기 BUSY)|보존(정확한 영구 crash·외부 master 전제)|controller가 master: 고정 대기 후 FREEZE/REPAIR/OPEN|부하·값·대기값·settled idle crash·위치별|실제 감지기·master 합의 없음, 파이프라인 쓰기 없음|
|CRAQ|Chain과 동일, 읽기는 clean 로컬/dirty tail 버전 조회|Chain과 동일|Chain과 동일|읽기 경로만 TAIL_ONLY와 번갈아 비교|crash 중 읽기 가용성 미측정|
|Rosé 변형|L=0 전원 적용 ACK, L>0 로컬 적용 후(lag<L), L=∞ 무제한|L=0 보존, L>0 최대 L건 유실 허용|controller가 master: 고정 대기 후 최장 prefix 승격|쓰기 진행 중 crash·client 8개 동시|단일 파티션·op 상한 변형, 원 논문 재현 아님|

## 재현 절차

각 cohort는 `run_<cohort>_ec2.py --hosts <hosts.json> --key <key.pem> --known-hosts <known-hosts> --dist <배포.tar> --out <새 폴더>`로 실행하고 `analyze_<cohort>.py <폴더>`로 CSV·그래프·문서를 다시 만든다. 배포본 SHA256은 각 `*-results/sources.json`에 있다. 이 문서는 `analyze_overall.py`가 요약 CSV에서 생성한다.

|cohort|실행|분석|배포본 SHA256|
|---|---|---|---|
|election-results|run_election_ec2.py|analyze_election.py|656ff2bf51637d5d50fb6c707a3b59f18d9d01e24664c2d4e3be5f5d515ccce5|
|catchup-results|run_catchup_ec2.py|analyze_catchup.py|9c5300c727b5087d0094b0aa6c3a110270f7e27b5122804171cd668fcd350f25|
|chain-results|run_chain_ec2.py|analyze_chain.py|97eba2a91062b57c66c20e3b30092335a393a3180ee37d38369393ded4c71068|
|craq-results|run_craq_ec2.py|analyze_craq.py|97eba2a91062b57c66c20e3b30092335a393a3180ee37d38369393ded4c71068|
|rose-results|run_rose_ec2.py|analyze_rose.py|b480d145b3cfa1b64867e78c16a796b86b20ae6e7df7abc7e07946eb33e4f038|

## 한계

- 단일 client(Rosé는 8개) closed-loop·128~512표본·조건별 3회다. 최대 처리량·운영 p99·SSD·장기 실행은 측정하지 않았다.
- PB/VR/Raft는 자체 heartbeat/선거 감지, Chain/Rosé는 controller의 고정 대기이므로 failover 절대값을 섞어 순위를 매기지 않는다. 감지 이후 전환 비용도 측정 주체(서버 자체 vs controller 제어 왕복)가 다르다.
- crash 모델이 다르다: PB/VR/Raft/Chain은 양쪽 복제본 값 확인 후 idle crash, Rosé는 쓰기 진행 중 crash다. Rosé의 유실 수는 L의 상한 검증이지 다른 방식의 유실 0과 같은 조건이 아니다.
- 계측·warmup 절차가 cohort마다 조금씩 달라(예: VR traffic/catchup의 안정화 대기) 정상 지연의 소수점 차이를 방식 차이로 해석하지 않는다.
- CRAQ의 짧은 clean 지연은 head/middle에서 redirect를 따라 tail에 재요청하는 경로와 비교한 결과다. TAIL_ONLY 직접 tail p50 범위는0.43~0.45ms, CRAQ head/middle은0.90~0.97ms다. 전체 읽기 처리량 우위는 이번 실험으로 확인하지 않았다.
- CRAQ 실서버 검증은 순번 역행 검사이며 호출/완료 history 기반 선형화 가능성 검증은 아니다. 복사본 검사는 warm key에 적용됐고 혼합 hot key의 최종 값은 tail에서만 확인했다.
- 모든 엔진은 연구 prototype이다. 영속화·재시작·membership 변경·분단 fencing·checkpoint는 구현하지 않았다.
