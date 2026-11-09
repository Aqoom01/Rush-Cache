# CRAQ 읽기 경로 구현·실험 결과

2026-11-03 · [S15P31D111-181](https://ssafy.atlassian.net/browse/S15P31D111-181) · 브랜치 `feat/craq-reads`

Chain의 쓰기·crash 복구 경로를 유지한 상태에서 CRAQ의 clean/dirty GET·EXISTS 읽기 경로를 추가했다. 논문 전체 시스템이나 실제 처리량의 재현은 아니다.

## 실행 증거

- 공통 Runner: 모드 2 × 시나리오 4 × 값 크기 2 × 대역폭 2 = **32조건, 2회 반복**.
- [반복 1 원본 요약](runs/run-2026-11-03T01-17-16.348627900Z-c3ef1e97/repeat-001/summary.csv) · [조건별 결과](runs/run-2026-11-03T01-17-16.348627900Z-c3ef1e97/repeat-001/results.md) · [반복 2 요약](runs/run-2026-11-03T01-17-16.348627900Z-c3ef1e97/repeat-002/summary.csv)
- 반복별 CSV SHA-256 동일: `66581B7BCC4B0BB247EC1DCD631AA700565C38DF4AEACE9AD645CA2EBBA7CCFD`.
- 각 조건 GET 90개: 반복당 **2,880건**, 2회 **5,760건** 모두 기대한 확정 값과 일치. 주입한 조건의 검증이며 모든 가능한 실행에 대한 증명은 아니다.
- 새 CRAQ 테스트 15개 포함 전체 프로젝트 **655개 테스트, 실패/오류 0**. 기존 Chain 테스트 유지. `gradlew build :experiment-runner:installDist` 통과.
- 실행 기준 HEAD `5d0176f6dfcfee50735f4b11ab54176a6d18318e`. CRAQ 작업은 미커밋 상태이며 이 commit 자체에 구현이 포함된 것은 아니다. 실행 sourceHash `caf68d2c036ce771774e4dc42e43a8d741721db7654908354c588181536a5f3b`, backendHash `a1b7f8213e164c6f16ebb28bb4d7cd6d0d56c3b4fb04880a8f1d651fc0956577`로 실행 소스와 클래스를 구분한다.

## 핵심 결과

대표 조건: 8KiB 값, 방향별 1MiB/s, 전파 1ms. client 읽기는 Chain에서 tail, CRAQ에서 생존 노드에 균등 배치한다.

|시나리오|Chain client 읽기 분포 (0/1/2)|CRAQ client 읽기 분포|CRAQ tail 버전 조회|CRAQ 추가 peer bytes|
|---|---|---|---:|---:|
|CLEAN|0 / 0 / 90|30 / 30 / 30|0|0|
|IN_FLIGHT|0 / 0 / 90|30 / 30 / 30|60|5,880|
|ACK_DELAY|0 / 0 / 90|30 / 30 / 30|60|5,880|
|TAIL_CRASH 복구 이후|0 / 90 / 0|45 / 45 / 0|0|0|

- **미전달 쓰기**: head/middle의 로컬 KV에는 새 값이 있어도 dirty 읽기는 tail이 보유한 이전 확정 값을 반환했다.
- **역방향 ACK 차단**: tail이 쓰기를 적용했으면 head/middle은 dirty 상태여도 버전 조회를 통해 새 확정 값을 반환했다.
- **tail crash**: 신뢰 master가 생존자 freeze→suffix 복구→Ack 일치→open을 완료한 뒤 두 노드에서 확정 값을 읽었다. 이 표는 failover 시간이나 재구성 중 가용성을 측정한 표가 아니다.
- **clean**이면 tail의 값 읽기를 분산한다. **dirty**이면 tail의 client 읽기 30개에 버전 조회 60개가 추가되므로 tail 전체 작업 수 감소라고 해석하지 않는다.

## 해석 범위와 남은 작업

dirty 대표 조건 p99는 3.504ms, clean 로컬 읽기는 0ns다. CPU·GC·client 연결/값 응답 직렬화를 모델링하지 않아서 발생하는 값이며 실제 지연·처리량 우위로 주장하지 않는다. 현재 확인한 것은 안전한 버전 선택, 요청 분산 경로, 추가 peer 조회 비용이다.

실제 읽기 처리량 비교는 CPU와 client 값 응답 비용을 포함한 환경에서 후속으로 수행해야 한다. 빈 KV·엔진 전용 쓰기 경로, 단일 in-flight 쓰기, 영구 crash oracle과 기존 외부 master 전제를 유지한다. master 합의·동적 가입·재시작/영속화·약한 일관성 읽기는 이번 범위에 포함하지 않았다.
