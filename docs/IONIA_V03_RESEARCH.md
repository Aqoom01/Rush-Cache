# V03 구현 전 추가 문헌 검토 — 판본·배치·파이프라인·TCP 대조

검토일: 2026-10-28. 기준 코드 revision: `9abfcb68cf493c74b7d7281155aa5f3336e6bc4d`.

**판정: 기존 v1.0을 성능 설계 확정으로 부른 표현을 철회한다. 기능 대조군 설계 v1.1로 수정하며, V03 Java 구현과 성능 측정은 이번 작업에 포함하지 않는다.** R=열람 원문 근거, C=실제 코드, D=프로젝트 판단, U=미확인 사항이다.

## 1. 실제 읽은 범위와 판본

| 문헌 | 실제 열람 범위 | 증거 수준·제한 |
| --- | --- | --- |
| P001 IONIA, FAST 2024 | §4.1–4.6, §5 평가 환경 재대조 | 공식 PDF 18쪽 확보. 논문 전체 신규 정독·실험 재현으로 올리지 않음 |
| P003 Skyros, SOSP 2021 | §4.3–4.5, §4.6 복구 관련 부분, §4.8, §5 환경 | 공식 연구실 PDF 17쪽. 기존 복구 감사와 구분 |
| P004 VR Revisited, 2012 | §5.3, §6.2–6.3 재대조 | 대학 공개 PDF 14쪽 |
| P076 Optimizing Paxos with batching and pipelining, TCS 496 (2013), 170–183 | 출판사 서지·초록 | DOI 10.1016/j.tcs.2012.10.002. 최종 원문 미확보; 상세 분석·식 검증 미완료 |
| P077 Tuning Paxos for High-Throughput with Batching and Pipelining, ICDCN 2012 서지 | 저자 공개 선행 원고 §II–III, §IV-A/B/C/D/F/G/H 관련 본문, §V | DOI 10.1007/978-3-642-25959-3_11. 열람 본문에는 DSN 2011 제출 표기. 출판된 학회판과 동일하다고 단정하지 않음. 수식·그림의 HTML 전사 한계 |
| P078 JPaxos: State machine replication based on the Paxos protocol, EPFL-REPORT-167765, 2011-07-31 | §6.1–6.4, §9 전체, §5.3.2 관련 부분 | 공동저자 대학 공개 PDF 38쪽. Algorithm 2, PDF p.24는 렌더 이미지로 대조. 보고서 전체 정독·재현 아님 |
| P079 Adaptive Batching for Replicated Servers, SRDS 2006 | 저자 공개 본문 §1–4, §6 | DOI 10.1109/SRDS.2006.8. 저자 업로드 공개 HTML 본문 사용; PDF 다운로드는 403. 그래프 수치를 정밀 추출한 결과 아님 |

P076/P077은 관련 연구의 저널·학회 계열이며, P078에도 같은 저자진의 요약이 있다. 네 신규 서지 행을 네 독립적인 재현 실험이나 네 신규 알고리즘으로 세지 않는다. 확인한 기존 78행(정본 P001–P075 + DUP 3)에는 신규 제목·DOI에 해당하는 행이 없었다. 기존 P번호는 유지한다.

**접근 한계 U:** P076의 EPFL 공개 링크는 제한/사람 확인 화면, 출판사 PDF는 접근 불가, 다른 공개 미러는 오류 또는 빈 응답이었다. 유효 PDF 헤더·파싱 실패 자료는 확보로 세지 않았다. 초록은 LAN에서도 파이프라인의 추가 이득이 가능한 경우를 설명한다. 선행 원고의 특정 LAN 결론을 2013년 최종판의 보편 결론으로 대체하지 않는다. 저자 연락·로그인·유료 접근은 하지 않았다. 최종판의 모델·실험 확장은 공개 원문 또는 제공받은 PDF 확보 후 보완한다. [출판사](https://www.sciencedirect.com/science/article/pii/S0304397512009097) · [EPFL 서지](https://infoscience.epfl.ch/entities/publication/41fb5228-7cae-41f9-b9f4-ea3375d4b233)

## 2. 원문에서 확인한 근거 R

### IONIA·Skyros·VR

IONIA는 배경 Prepare 배치와 pending 읽기의 동기 진행을 구분한다. §4.6·§5의 SplinterDB/eRPC/SSD 환경은 현재 Java 파일 KV/TCP와 다르다. 모든 비교군에서 가능한 batching을 사용하므로 O00를 무배치로 표기할 근거가 없다. [IONIA §4–5](https://www.usenix.org/system/files/fast24-xu.pdf)

Skyros §4.8은 입력 검증과 실행 시 자원 오류를 구별한다. 배경 복제에서 전체 요청을 보내는 구현과 순서 정보만 보내는 향후 최적화도 구별한다. 복제 RAM 로그를 매 요청 디스크 fsync와 동일시하지 않는다. [Skyros §4.3–4.8](https://research.cs.wisc.edu/adsl/Publications/nilext.pdf)

VR §6.2의 저부하 즉시 처리 제안은 지연 절충의 근거다. §6.3의 옛 Primary 읽기 위험을 고려해야 하므로, 배치 때문에 quorum/View 검사를 생략할 수 없다. [VR §6](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf)

### Paxos 선행 원고·JPaxos

선행 원고 §III의 모델은 무손실·무장애, 전이중 링크, 단순화한 처리 지연 등 전제하의 네트워크 이용률 분석이다. §IV는 실제 평균 batch/window와 설정 상한을 구별한다. 10ms는 다수 고부하 실험에서 만료되지 않았던 설정이며 보편 최적값이 아니다. §IV-G/H의 지연 정책·TCP/UDP 결과도 구현과 부하에 의존한다. [저자 공개 선행 원고](https://www.researchgate.net/publication/49459755_Tuning_Paxos_for_High-Throughput_with_Batching_and_Pipelining)

JPaxos §6의 WND는 동시에 미결정인 합의 인스턴스 수다. 과도한 창은 자원 비용과 순서상 빈 구간 문제를 만든다. Algorithm 2는 요청 도착 시각을 사용한다. §9는 상태 없는 서비스·지속 TCP 연결·고부하·단일 코어 조건의 실험이다. 이는 디스크 KV/혼합 CRUD/장애 복구의 성능 증거가 아니다. [JPaxos §6, §9](https://www.cs.put.poznan.pl/pawelw/pub/EPFL-REPORT-167765.pdf)

Algorithm 2의 인쇄본 p.24에는 `age(Batch)+ΔB >= Ct()`가 있다. 본문이 설명한 충분히 오래 기다린 요청의 발송 조건과 부등호 방향이 맞지 않아 기계적으로 복사하지 않는다. 빈 batch·초과 단일 요청의 분기 역시 명시적인 자체 명세로 다룬다. 이를 저자의 정오표 확인으로 주장하지 않는다.

### 적응형 배치

AB는 겹치는 요청을 처리하는 여러 스레드의 doorway를, TAB는 마지막 도착 후 리셋되는 타이머를 사용한다. §6은 무제한 연속 도착에서 진행·지연 경계가 없을 수 있음을 명시한다. §3–4의 CORBA/Ensemble·구형 JVM 실험은 현재 owner 모델과 다르다. [저자 공개 본문 §2–4, §6](https://www.researchgate.net/publication/224678710_Adaptive_Batching_for_Replicated_Servers)

## 3. 실제 코드 대조 C

아래는 원논문의 주장과 분리한 코드 관찰이다. 저장소 기준 경로이며 이번에 모두 읽기 전용으로 확인했다.

| 파일·위치 | 관찰 | 영향 |
| --- | --- | --- |
| cache-core/src/main/java/io/openkache/core/replication/ionia/IoniaEngine.java:303 | Follower의 matched+1 이후 전체 suffix 전송 | 기존 O00도 요청을 합침. E=1 비교군으로 부르지 않음 |
| 같은 파일:278,315 | explicit ACK로 matched 갱신; send Future는 로컬 결과 | 전송 성공을 복제 완료로 세지 않음 |
| cache-core/src/main/java/io/openkache/core/replication/ionia/IoniaLog.java:65 | 누락 구간·충돌·요청 중복 위치를 거절 | 여러 새 TCP 연결의 도착 순서를 가정한 speculative pipeline을 바로 넣을 수 없음 |
| cache-server/src/main/java/io/openkache/server/ionia/IoniaTcpTransport.java:62 | call마다 가상 스레드 안에서 새 Socket 연결/닫기, TCP_NODELAY | 지속 연결을 쓴 논문의 전송 모델과 다름. 같은 peer의 여러 call은 전송 순서가 보장되지 않음 |
| cache-server/src/main/java/io/openkache/server/ionia/IoniaNode.java:51 | 20ms timer | 배치 목표 시각과 실제 owner 실행시각 분리 필요 |

## 4. 설계 수정 D — v1.1에서 우선 적용

| 항목 | 변경된 판단 | 구현/비교 시 확인 |
| --- | --- | --- |
| 창 의미 | Follower별 한 논리 확인 대기 구간 Wf=1은 기능 대조군. 논문의 global WND와 다름 | 진단 이름을 구별. 최적 파이프라인 설계 확정으로 표기하지 않음 |
| 전송 자원 | 한 논리 flight라도 재전송 socket 여러 개가 겹칠 수 있음 | Wf=1을 socket·RAM·실제 전송량 상한으로 주장하지 않음. 재시도/실패/연결 생성 관측 |
| 대기 | 신규 도착·중복·재전송이 첫 요청 시각을 바꾸지 않음 | due = now−firstOrderedAt >= D. 빈 batch 발송 금지. 복구 때만 새 generation 시각 |
| 시간 의미 | ordering 이후 age는 slot/owner/전송 대기와 겹칠 수 있음 | D는 선택 정책의 대기 목표. ACK·commit·apply 완료의 hard deadline 아님 |
| 동기 barrier | urgent는 추가 모으기 대기만 우회 | 기존 flight/ACK 대기는 남음. 여러 chunk 뒤 GET을 일정 RTT 안에 끝낸다고 약속하지 않음 |
| 저부하 | D=0 기능 대조와 D>0 기능 대조를 구별 | 즉시 처리/적응 정책이 항상 우월하다는 결론 금지. 희소·burst·고부하 각각 평가 |
| 20ms | timer·retry는 기존 조립을 재사용하는 기능 fixture 값 | 최적/재현 설정 아님. 더 정밀한 timer는 별도 통제 변경 |
| 비교 해석 | O00 대 V03은 배치/전송 창/연결 생성 패턴 및 admission 차이가 있는 전체 구성 비교 | V03 내부 E/B/D만 바꾼 통제 실험과 구분. 순수 batching 효과라고 단정 금지 |
| 내구성 | 독립 Replica 복제 로그, stable View 저장, 선택적 KV force를 구별 | 배치 group-fsync 효과·전원 손실 내구성은 미구현 실험으로 남김 |

E=128/B=65536/D=20ms는 예제에만 남긴다. 공개 원문과 다르다는 이유로 O00/V01/V02a의 TCP·timer·로그·공통 API를 수정하지 않는다. V03도 이번에는 Java를 작성하지 않는다.

## 5. 새 반례 확인 D와 테스트 증분

연구 작업용 `v03-research-prepare.py`의 작은 모형은 아래 다섯 개의 결정적 작은 시나리오를 실행한다. 실제 Java/분산 프로토콜 검증이 아니며, 기존 6,912조합 모형이나 257개 회귀 테스트와 합산하지 않는다.

1. 연속 도착이 inactivity timer를 계속 미루는 경우와 첫 요청의 절대 대기 조건 비교.
2. 이미 지나간 목표 시각에도 slot이 없어 첫 발송이 늦어지는 경우.
3. urgent barrier가 여러 chunk와 ACK 대기를 통과해야 하는 경우.
4. 별도 연결로 뒤 chunk가 먼저 도착해 연속 prefix 검사에서 거절되는 경우.
5. logical flight=1이어도 재시도 작업이 겹쳐 physical attempts>1인 경우.

| 추가 인수 ID | 필수 확인 |
| --- | --- |
| V03-13 | 지속 도착·동일 ID 재시도로 age/deadline 리셋 금지; 빈 batch 없음 |
| V03-14 | slot wait·owner lateness·첫 전송·retry age를 분리; 실제 지연이 D를 넘을 수 있는 fixture |
| V03-15 | flight가 있는 동안 도착한 GET/Meta/DELETE/Slow fallback과 유실 ACK; 재시도 후 barrier drain |
| V03-16 | 별도 TCP 연결의 역순·중복·늦은 ACK; 아직 없는 prefix를 건너뛰지 않음 |
| V03-17 | 논리 flight 수와 겹친 전송 시도/연결 수 구분; 실패 뒤 resource 정리 |
| V03-18 | 기능 fixture 결과를 성능 최적값으로 승격하지 않으며, profile/byte 정의/실측 평균 batch를 산출물에 남김 |

기존 V03-01–12는 계획이며 여전히 미실행이다. ‘D 이내 전체 응답 완료’를 인수 조건으로 바꾸지 않는다. 공정성·eventual 응답 조건 없이 진행성을 증명했다고 하지 않는다.

## 6. 이후 순서와 남은 검토 U

1. 기능 구현은 `replication/ionia-fourth`에서 v1.1을 기준으로 진행한다. 기존 디렉터리를 보존하고 기능 확인용 Wf=1과 E/B/D를 명시한다. 검증 후 사용자가 commit/push한다.
2. V03 기능 검증은 가상 시계·ACK/유실·View·이력·실제 TCP/JVM 및 기존 회귀 순으로 수행한다. 이번 문헌 검토를 테스트 통과로 세지 않는다.
3. 성능 파라미터를 정하기 전에 P076 최종 원문의 모델/실험 확장을 확인한다. 현재 선행 원고와 다르면 판본별 근거를 수정한다. 공개 원문이 계속 불가하면 해당 주장·재현을 미검증으로 남긴다.
4. 성능 단계는 공통 데이터/driver 구축 후 실행한다. CPU·RTT·bandwidth·연결 생성·입력 크기·Fast/Slow·읽기 경로·KV 조건을 고정/기록하고, 저부하/포화/과부하를 구별한다. 평균만 아니라 p50/p95/p99, timeout, backlog 시간 추이와 종료 drain을 관측한다.
5. 최소 상호작용 실험은 E/B를 고정한 D 변화와 D를 고정한 E/B 변화다. 같은 workload trace/seed를 사용한다. Wf/연결/timer를 바꾸는 연구는 별도 profile과 통제 비교가 필요하다. 최적 W 값은 현재 미정이다.

**완료 범위:** 원문 관련 절·판본 대조, 실제 TCP/로그 차이 확인, 설계 v1.1/검증 계획 보완, 문헌 DB 증거 갱신. **미완료:** P076 최종 원문 상세 분석, V03 Java 구현·통합 테스트·최적 설정·성능/SSD/fsync 재현.

