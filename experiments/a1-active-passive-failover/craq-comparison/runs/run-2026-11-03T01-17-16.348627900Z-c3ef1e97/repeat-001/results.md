# Chain / CRAQ 읽기 경로 비교

가상 시간, 정상/미전달 쓰기/역방향 ACK 차단/tail crash 복구 후 읽기. 조건별 GET 90개를 동시에 제출한다.

|모드|시나리오|값 B|MiB/s|정확한 읽기|노드별 client 읽기|dirty 조회|peer B|p99 ms|
|---|---|---:|---:|---:|---|---:|---:|---:|
|CRAQ|ACK_DELAY|64|1|90|30/30/30|60|5880|3.504|
|CRAQ|ACK_DELAY|64|10|90|30/30/30|60|5880|2.150|
|CRAQ|ACK_DELAY|8192|1|90|30/30/30|60|5880|3.504|
|CRAQ|ACK_DELAY|8192|10|90|30/30/30|60|5880|2.150|
|CRAQ|CLEAN|64|1|90|30/30/30|0|0|0.000|
|CRAQ|CLEAN|64|10|90|30/30/30|0|0|0.000|
|CRAQ|CLEAN|8192|1|90|30/30/30|0|0|0.000|
|CRAQ|CLEAN|8192|10|90|30/30/30|0|0|0.000|
|CRAQ|IN_FLIGHT|64|1|90|30/30/30|60|5880|3.504|
|CRAQ|IN_FLIGHT|64|10|90|30/30/30|60|5880|2.150|
|CRAQ|IN_FLIGHT|8192|1|90|30/30/30|60|5880|3.504|
|CRAQ|IN_FLIGHT|8192|10|90|30/30/30|60|5880|2.150|
|CRAQ|TAIL_CRASH|64|1|90|45/45/0|0|0|0.000|
|CRAQ|TAIL_CRASH|64|10|90|45/45/0|0|0|0.000|
|CRAQ|TAIL_CRASH|8192|1|90|45/45/0|0|0|0.000|
|CRAQ|TAIL_CRASH|8192|10|90|45/45/0|0|0|0.000|
|TAIL_ONLY|ACK_DELAY|64|1|90|0/0/90|0|0|0.000|
|TAIL_ONLY|ACK_DELAY|64|10|90|0/0/90|0|0|0.000|
|TAIL_ONLY|ACK_DELAY|8192|1|90|0/0/90|0|0|0.000|
|TAIL_ONLY|ACK_DELAY|8192|10|90|0/0/90|0|0|0.000|
|TAIL_ONLY|CLEAN|64|1|90|0/0/90|0|0|0.000|
|TAIL_ONLY|CLEAN|64|10|90|0/0/90|0|0|0.000|
|TAIL_ONLY|CLEAN|8192|1|90|0/0/90|0|0|0.000|
|TAIL_ONLY|CLEAN|8192|10|90|0/0/90|0|0|0.000|
|TAIL_ONLY|IN_FLIGHT|64|1|90|0/0/90|0|0|0.000|
|TAIL_ONLY|IN_FLIGHT|64|10|90|0/0/90|0|0|0.000|
|TAIL_ONLY|IN_FLIGHT|8192|1|90|0/0/90|0|0|0.000|
|TAIL_ONLY|IN_FLIGHT|8192|10|90|0/0/90|0|0|0.000|
|TAIL_ONLY|TAIL_CRASH|64|1|90|0/90/0|0|0|0.000|
|TAIL_ONLY|TAIL_CRASH|64|10|90|0/90/0|0|0|0.000|
|TAIL_ONLY|TAIL_CRASH|8192|1|90|0/90/0|0|0|0.000|
|TAIL_ONLY|TAIL_CRASH|8192|10|90|0/90/0|0|0|0.000|

- clean CRAQ는 각 생존 노드에서 로컬 응답한다. dirty CRAQ는 tail의 확정 key 버전만 조회하고 값은 로컬 보존 버전에서 반환한다.
- IN_FLIGHT는 middle→tail Forward 차단: tail의 이전 값 반환. ACK_DELAY는 역방향 Ack만 차단: tail의 새 확정 값 반환.
- TAIL_CRASH는 정확한 영구 crash oracle 200ms와 전 생존자 freeze/복구/open 후 읽는다. 재구성 중 가용성 측정은 이 표의 범위가 아니다.
- 방향별 1ms 전파와 명시적 논리 메시지 크기, 1/10MiB/s를 사용한다. 클라이언트 링크·값 응답 직렬화·CPU·GC·디스크는 모델링하지 않았다.
- 따라서 0ns는 로컬 CPU 비용을 두지 않은 모델값이며 처리량/실제 지연 우위를 뜻하지 않는다. peer 비용과 읽기 경로 분산만 비교한다.
- dirty 조건에서는 CRAQ의 tail client 읽기는 30개로 줄지만 버전 조회 60개가 추가된다. 총 tail 작업량 감소라고 해석하면 안 된다.
- 논문의 일부 핵심 경로를 구현한 연구 prototype이다. weak consistency 모드·pipelining·master 합의·영속화·실제 TCP는 범위 밖이다.
