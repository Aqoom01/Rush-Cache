# 실제 통합 경로 로컬 검증 결과

2026-11-12. **EC2 접속·업로드·실험은 수행하지 않았다.** 아래는 Linux/WSL의 한 물리 호스트에서 수행한 검증이다. 가상 fixture 결과와 합치지 않는다. 성능 순위·SSD 처리량·EC2 장애 복구 수치는 아직 없다.

## 빌드와 검사

- `./gradlew build :cache-server:integrationZip --offline --console=plain`: **BUILD SUCCESSFUL**. JDK21.0.12.1, `-Xlint:all -Werror`.
- Java XML 결과: 총2,032개, 실패/오류0개, 기존 설정의 skip12개, 실행 통과2,020개. 세부 계수는 [build-validation.json](build-validation.json).
- 새 통합 검사20개: 11종 native TCP/파일 KV와 이동·dedup/DELETE·stale epoch, 목적지 적용 barrier, 불확정 쓰기/실패 bootstrap의 보류·quarantine, 설정 거절·ledger 축출, O00/V03×SWIM/Lifeguard의 실제 UDP DEAD→native view-change와 값 보존.
- 배포 도구 `python3 -m unittest discover -s experiments/system-integration/ec2 -p 'test_*.py'`: **8개 통과**. JAR/도구 변조, 임의 role/PID 보호, 불완전 control frame, 게시 뒤 fault를 주입했다고 꾸미는 경우, 지원 불가능 조합과 B1/B2 행렬을 검사한다.

## 독립 JVM 리허설

최종 Java 배포물의 `local-final`32회와 FD startup gate를 보강한 `local-fdgate`5회, 합계**37회**를 수집했다. 매 실행은 replica6개·gateway·load의 서로 다른 JVM8개이며 TCP/UDP와 실제 파일 KV를 사용한다. process state의 PID8개 유일성·정확한 main/config 경로, 원본 요청 CSV의 계수/ID/p99, 입력 hash, 최종 결과, 소유 JVM 종료를 별도로 확인했다. 원본은 `runs/`에 보존하고 [local-validation.csv](local-validation.csv)에 판정/실행 hash/원본 경로를 남겼다.

| 검사 | 결과 |
|---|---|
| PB/RAFT/VR/O00/V01~V07 × STOP/PRE | **22/22** 이동 DONE·게시, 각120 측정 요청 성공, stale/audit/uncertain=0 |
| O00·RAFT + HOT_RANGE + HEAT/GREEDY BALANCE | 각1회 split, 각4회 실제 이동 DONE, 측정 요청 실패0 |
| O00 + SLOT MOVE / RING NORMAL | 모두 값 검사 통과. SLOT 이동 DONE, Ring은 토큰 소유권대로 라우팅하며 임의 이동 없음 |
| O00·V07 + GET/SET/DELETE 중 MOVE | 모두 이동 DONE, MISS를 포함한 응답과 최종 전수 감사 일치 |
| startup grace 안의 초기 pause 리허설 | 2회 값 검사 통과. FD 활성 상태의 비교 근거로 쓰지 않고 아래 gate 이후 실행으로 보강 |
| 실제 UDP probe 이후 O00/Lifeguard leader·observer pause, V07/Lifeguard observer pause, O00/SWIM observer pause | **4/4** 값 검사 통과. 부하 admission 탈락은 별도 계수이며 성능 성공으로 숨기지 않음 |
| 이동 중 leader SIGKILL: O00/Lifeguard·RAFT/fixed와 gate 이후 O00/Lifeguard | **3회 모두 이동 ABORTED, 미게시**. 각3/3/5 key의 쓰기 결과가 불확정하여 correctnessValid=false, migrationValid=false. 검증 가능한 key의 값 불일치는0이지만, 불확정 key의 안전성을 확정하지 않음 |

37회 중34회는 correctnessValid/migrationValid가 모두 true다. **3회 crash 결과를 안전성 통과로 집계하지 않는다.** `COMPLETE`는 결과 수집 완료 표시다. crash에서 정상 가용성 또는 자동 이동 재개를 달성했다고 주장하지 않는다. 쓰기 결과가 불확정해도 성공으로 간주하며 소유권을 넘기지 않는 경계를 확인했다.

총5,760개 측정 요청과 prefill/warmup·감사 요청이 실행됐다. 22개 정상 이동의 요청 tape SHA는 모두 `cfa04d578d206188a9eec329976400ab365faa088459ca37541577281b140350`으로 같았다. 모든 실행에서 정확한8개 JVM state와 종료/수집 성공을 확인했고, native data·GC/role 로그·fault·gateway 이벤트를 남겼다. 작은 key 수의 최종 감사는 전수이며 더 큰 실행의 기본 감사는 최대1,000 key 표본이다.

초기 `local-v2`에서는 동시 native 요청 때문에 A1의 bootstrap 인증 GET이 BUSY로 거절되어 이동이 중단되는 문제가 드러났다. administrative 호출이 원래 client처럼 같은 ID로 BUSY를 재시도하도록 고쳤고 `local-v3` 및 최종 배포물에서 Raft/VR 이동을 재검증했다. 초기 실패 원본도 삭제하지 않았다.

최종 native JAR는 전체 빌드·37회 실행에서 동일하다. 마지막 단계에서 제어 도구에 FD startup grace 이후 **여섯 노드의 실제 UDP probe 증거 확인**을 추가했으며, 그 버전으로5회를 다시 실행했다. 문서·검증표를 ZIP에 추가하면 전체 도구 receipt hash가 바뀌므로 CSV에는 **실제 실행에 사용한 번들 hash**를 보존한다. 최종 ZIP은 JAR receipt와 실행용 Python 소스가 마지막 검증 버전과 일치하는지 다시 검사한다.

## EC2 준비 상태와 남은 경계

- ZIP에 server/core/Runner JAR와 library/tool SHA receipt, 실행 도구·topology 예제·실험 계획 포함.
- `plan`, `matrix`, `verify-bundle`, `verify`와 `ec2` 기본 dry-run 확인. 원격 실행용으로 역할별 Java21/Linux 검증, 확인된 known_hosts, input/hash 검증, 소유 PID 확인, 수집/정리 실패 기록을 구현했다. 실제 SSH/SCP 동작과 EC2 환경 적합성은 첫 원격 pilot에서 검증해야 한다.
- EC2 예제 입력: replica6/gateway1/load1의 서로 다른8호스트, 1,000 key·1KiB·80/20·200req/s·warmup2,000·측정12,000. 실제 주소와 SSH 설정은 예제 값을 교체해야 한다. 인스턴스를 생성하거나 켜고 끄지 않는다.
- 중앙 gateway metadata/ledger는 RAM이며 HA/재시작을 지원하지 않는다. B3는 gateway의 native GET/SET adapter를 이용한 직렬 이동이다. Post-copy·병렬 scheduler·node-count/증감/drain은 미연결이다.
- FD pause는 프로세스 전체 정지다. network jitter/SSD 단독 stall/partition injector는 없다. 파일 KV의 fsync를 끄므로 전원 손실 내구성/SSD force 완료를 검증한 것이 아니다.
- A1 client TCP 계측과 A2 stream 계측의 범위가 달라 전체 네트워크 비용 순위는 동일 OS 계측을 추가해야 한다. gateway 포화와 staged bootstrap 비용을 native 알고리즘 성능으로 일반화하지 않는다.

전체 EC2 비교·탈락/선정 규칙은 [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md), 배포 명령은 [README.md](README.md)를 따른다.
