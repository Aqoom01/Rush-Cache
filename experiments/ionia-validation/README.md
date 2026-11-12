# Windows IONIA 기능 회귀 실행

프로젝트 최상위 폴더에서 Java 21의 JAVA_HOME을 설정한 PowerShell로 실행한다. 공통 성능 부하 driver가 아니라 전체 기능 회귀의 환경 사전진단 도구다.

```powershell
$env:JAVA_HOME='C:\Users\SSAFY\.jdks\ms-21.0.11'
& .\experiments\ionia-validation\Run-Validation.ps1 -Mode V02aTcp -Repeat 3
& .\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 2
```

Full은 `gradlew.bat build :cache-server:installDist --rerun-tasks --console=plain`을 실행한다. 저장소의 기존 병렬 설정을 유지한다. V02aTcp는 기존 TCP 검사 1개와 오류 회귀 3개를 실행한다. Repeat는1..20이다.

각 실행 전에 Windows IPv4 동적 포트 범위를 읽고 전체 TCP 연결/리스너의 로컬 포트를 집계한다. 범위 안에서 관측한 고유 로컬 포트가 전체의10% 이하일 때만 시작한다. 높은 사용량이면 기본5초 간격으로 최대600초 기다린다. `-PortWaitSeconds`(30..1800)와 `-PollSeconds`(1..30)만 조정할 수 있다. 10%는 보수적 시험 전제이며 처리량 권장값·운영 용량 또는 실행 중 고갈 방지의 보장이 아니다. 소스 포트 하나와 연결4-tuple 사용률은 같지 않다.

기다림 한도·포트 조회·Gradle 실행 실패는 오류로 종료한다. 실패를 자동 재실행하거나 skip/pass로 바꾸지 않는다. 테스트 입력·deadline·인수 조건·OS 포트 범위·TIME_WAIT 설정·프로세스 상태를 바꾸지 않는다. OS 연결 정보 조회가 거절되면 환경 권한을 확인해야 하며 검증을 통과 처리하지 않는다.

증거는 `build/ionia-validation/<timestamp-id>/`의 manifest.json, port-samples.json, run-N.log, run-N/<module>/TEST-*.xml에 저장한다. build/는 임시 산출물이며 영구 요약은 docs/IONIA_TCP_READ_VALIDATION.json이다. 최종 manifest status와 completedRuns를 함께 확인한다. 대상 검사에서는 실행한 server XML만 보존하며 전체 실행은 세 모듈을 보존한다.

Linux/WSL용 실행 도구는 아니다. 해당 환경에서는 Gradle을 직접 실행하고 그 OS의 포트 자원 조건을 별도로 확인한다. 성능 측정에 이 대기 시간을 포함하지 않는다.
