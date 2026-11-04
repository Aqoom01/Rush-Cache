# OpenKache Adapter Snapshot

이 디렉터리는 `failure-detector-lab`을 만들기 전에 OpenKache 공용 모듈에
staged되어 있던 19개 파일을 원래 저장소 경로 그대로 보존한 스냅샷이다.

## 목적

- 기존 UDP 전송, 메시지 codec, 서버 lifecycle 연결 코드를 잃지 않는다.
- 개인 Failure Detector 연구 코드를 공용 `cache-core`, `cache-server`,
  `experiment-runner`, 루트 문서와 분리한다.
- 다른 팀원의 `develop` 변경과 충돌하지 않은 상태에서 실험을 계속한다.

파일은 `files/` 아래에서 원래 경로를 유지한다. 예를 들어 기존
`cache-server/src/main/.../SwimRuntime.java`는 다음 위치에 있다.

```text
files/cache-server/src/main/.../SwimRuntime.java
```

## 빌드 관계

이 스냅샷은 참고 및 향후 통합용이며, 독립 Lab의 Gradle/Maven source set에
포함되지 않는다. 따라서 `failure-detector-lab/src`의 실험 구현과 클래스가
충돌하지 않는다.

## 향후 사용 시 주의

스냅샷에는 기존 SWIM baseline 중심의 서버 wiring도 포함되어 있다. 최종
OpenKache 통합 시 파일 전체를 그대로 되돌려 넣지 말고 다음 작업을 거쳐야 한다.

1. detector 선택을 `SWIM + Lifeguard`와 `Phi Accrual + Gossip` 인터페이스로 분리한다.
2. Lab 실험으로 선정된 detector만 서버 lifecycle에 연결한다.
3. `develop`의 최신 설정, protocol, lifecycle 코드에 작은 단위로 적용한다.
4. 공용 모듈 테스트와 실제 다중 노드 통합 테스트를 다시 실행한다.
