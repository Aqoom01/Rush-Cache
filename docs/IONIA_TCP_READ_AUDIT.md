# V02a TCP 읽기 간헐 실패 감사

2026-10-28. 시작 HEAD `3998af206d7b2363f9d130b0c185b5f99b0c4d3e`, 직전 V04b의 미커밋 변경을 사본/hash로 보존한 상태에서 진행했다. commit/push/merge는 실행하지 않았다.

## 확인된 현상과 원인 범위

이전 전체 병렬 실행에서 V02a `IoniaThirdTcpConcurrencyTest.java:39`가 기대 valueVersion32/실제0으로 실패했다. 그 당시 로그에는 Result status나 개별 RPC 오류가 없어 그 한 건의 원인을 소급해서 확정할 수는 없다.

이번에는 원본 코드/부하를 유지하고 읽기·MetaQuery·View probe의 응답을 관측했다. 분리 반복30회는 통과했다. 전체 회귀와 추가 반복30회를 연이어 실행한 진단에서는22번째 GET이 같은 32/0 형태로 실패했다. 정확한 결과는 `UNAVAILABLE`, 빈 값, valueVersion0이었다. READ/META와 probe2개가 모두 `java.net.BindException: Address already in use: getsockopt`에 실패했다. 세 엔진은 View0/NORMAL, failureReason 없음이었으며 모든 replica에32개 쓰기가 기록돼 있었다. 당시 leader commit/applied는2였으므로 fast ACK를 완료했다고 이미 모두 KV에 적용됐다는 의미도 아니다.

동시 관측한 Windows IPv4 동적 포트 범위는49152부터16384개였다. TIME_WAIT 연결19730개, 고유 TIME_WAIT 로컬 포트15064개를 확인했다. TIME_WAIT 연결 수는 고유 포트 수 또는 연결4-tuple 여유와 같지 않지만, 실제 BindException 및 다른 기존 테스트의 connect/bind 실패가 함께 발생해 이 재현의 포트 할당 실패를 확인했다. 추가 반복30회 중9회가 실패했고, 포트가 풀린 뒤 검사는 다시 통과했다. 추가 반복 자체가 자원 압박을 늘렸으므로 일반 실행의 실패 확률로 해석하지 않는다.

값이 성공적으로 읽혔는데 버전이0으로 손상된 재현이 아니다. 원본 IoniaClient는 MetaQuery 실패 또는 현재 View 과반 인증 부족을 UNAVAILABLE로 반환한다. Result.of의 version0을 성공 값의 버전과 혼동하지 않아야 한다. 운영체제 설정을 바꾸거나 실패를 성공으로 변환하지 않았다.

## 보완한 범위

- 기존 V02a TCP 테스트는 먼저 GET/replica ResultStatus.OK를 확인하고, 실패하면 RPC 종류·대상·오류·엔진 progress/View/failureReason을 보고한다. TraceTransport는 시험 전용 관측 wrapper이며 원래 응답/오류를 전달한다. 새로운 자동 재시도를 넣지 않았다.
- 첫 사전진단 도구 검증에서 전체1회는 통과했고 다음 실행은 기존 V01의 관측용 followerRead가 Connection refused로 실패했다. 이 문제는 포트 BindException과 구분한다. V01에도 다른 변형 테스트에서 이미 사용하던 관측용 접속 거절 재시도 경계를 적용했다. 같은 요청 ID와 최초 총5초 deadline을 유지하고 전송 상대 timeout은 남은 시간으로 줄인다. 매 시도 전후 원본 View0/NORMAL·failureReason 없음을 확인하며, ConnectException 외 오류·UNAVAILABLE/MISS·값 불일치는 재시도로 덮지 않는다. 적용 prefix가 완료됐어도 다음 관측 연결의 성공을 보장하지는 않는다. 지속 연결/accept 경로의 거절 원인을 모두 해결한 변경으로 주장하지 않는다.
- 새 IoniaReadTransportFailureTest 3개는 실제 관측한 BindException을 RPC 경계에 주입한다. Meta 실패 시 신선한 follower 값도 성공으로 반환하지 않음, probe2개 실패 시 읽기 인증 거절, follower만 실패하면 기존 인증된 leader fallback은 작동함을 확인한다. OS 포트 할당 내부를 이 테스트로 재현했다고 주장하지 않는다.
- Windows `experiments/ionia-validation/Run-Validation.ps1`은 실행 전 관측 포트 점유가10% 이하가 될 때까지 한도를 두고 기다린다. 반복마다 확인하며 로그/XML/포트 표본을 보존한다. 테스트를 skip/pass로 바꾸거나 실패한 실행을 자동 재시도하지 않는다. 포트 범위·TIME_WAIT·프로세스·테스트 deadline을 변경하지 않는다.
- 원본/파생 엔진·Client·Transport·wire·저장소·설정·공통 API는 그대로다. 기존 파일 수정은 위 두 TCP 테스트의 진단/관측 경계와 개발 기록뿐이며, 시작 시점 보호 파일 226개의 hash가 같다. 직전 V04b 소스·문서도 유지했다.

10%는 전체 주소의 보수적 사전진단 기준이다. TIME_WAIT의 모든 포트가 무조건 사용할 수 없다는 뜻도, 실행 중 포트가 절대 고갈되지 않는다는 뜻도 아니다. 이 도구는 Windows 전용이며 공통 성능 비교 driver와 별개다.

## 실행 증거

```powershell
& .\experiments\ionia-validation\Run-Validation.ps1 -Mode V02aTcp -Repeat 3
& .\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 2
```

Java21.0.11에서 V02a 대상4개를3회 통과했다. 전체는 기존 병렬 설정 그대로 build/installDist를2회 실행해 **각416개(core321/server79/runner16), 실패·오류·skip0**으로 통과했다. 반복 결과를 별개 테스트832개로 부풀리지 않는다. 신규는 위 오류 회귀3개다. 두 번째 전체 실행 전 실제 포트 대기 분기를 통과했다. 과부하 재현에서의 실패를 최종 회귀 통과 결과와 구분해 `IONIA_TCP_READ_VALIDATION.json`에 기록했다.

## 남은 범위

프로토타입 Transport는 여전히 메시지마다 새 TCP 연결을 연다. 운영 트래픽에서의 포트 고갈이나 네트워크 장애를 근본적으로 제거한 변경은 아니다. 지속 연결·pool·전송 admission은 별도 공통 전송 설계/검증 과제이며, 이번에 원본 및 모든 변형의 동작/비교 조건을 바꾸지 않았다. OS 포트/연결 여유를 확인한 기능 회귀와 성능·실제 SSD·multi-host·power-loss 검증을 구분한다.

다음 알고리즘 작업은 V05 후보의 문헌 근거·읽기 대상 선택과 View/Meta 검증의 안전 조건 검토다. V05/V06은 아직 구현하지 않았다.
