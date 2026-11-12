# V01 IONIA-LR

Leader 전용 데이터 읽기 클라이언트와 해당 테스트를 보관한다. 정석 `../ionia/`의 소스·테스트는 수정하지 않는다.

- Java package: `io.openkache.core.replication.ionia_second`
- Production source root: `src/main/java/`
- Test source root: `src/test/java/`
- 두 소스 루트는 `cache-core/build.gradle.kts`에서 등록한다. 기존 기본 main 루트에서는 이 컨테이너 디렉터리를 제외하므로 테스트가 production에 포함되지 않는다.
- CLI: `cache-server`의 `io.openkache.server.ionia_second.IoniaClientMain`
- 서버·합의·복구·저장 어댑터: 고정된 정석 O00 구현 재사용

읽기 View 확인은 항상 켜져 있다. GET 대상은 최초 Leader가 아닌 매 요청 시점에 관측한 현재 Leader다. 라이브러리 사용자는 timer를 공급하고, View 변경 시 refresh 후 같은 논리 요청을 재시도해야 한다. CLI는 이를 조립한다.

실행 방법·검증 범위: 저장소의 `docs/IONIA_V01.md`.
