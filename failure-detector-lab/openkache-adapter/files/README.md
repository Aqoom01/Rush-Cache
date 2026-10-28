# OpenKache

고성능 분산 캐시의 가용성과 확장성에 관한 선행연구 알고리즘을 공통 서버 뼈대 위에서 구현하고 비교하는 Java 21 프로젝트입니다.

현재 서버에는 TCP client/peer listener, 단일 노드 KV, `/metrics`, SWIM baseline과 UDP transport가 연결되어 있습니다.
복제·Tablet·마이그레이션과 Lifeguard 확장은 아직 구현되지 않았습니다.

구조와 규칙은 아래 문서에 있습니다.
- [docs/architecture.md](docs/architecture.md): 구조, thread 경계, 기동·종료
- [docs/module-boundaries.md](docs/module-boundaries.md): 모듈 의존성, 패키지 책임
- [docs/protocol-v1.md](docs/protocol-v1.md): TCP frame과 SWIM UDP datagram 형식
- [docs/git-convention.md](docs/git-convention.md), [docs/mr-convention.md](docs/mr-convention.md): 협업 규칙

## 요구 사항

- JDK 21. `java -version`으로 확인합니다.
- Gradle은 설치하지 않아도 됩니다. `./gradlew`가 Gradle 9.8.0을 자동으로 받습니다.
- Docker는 compose 실행 시에만 필요합니다.

## 빌드와 테스트

```bash
./gradlew build                         # 컴파일 + 전체 테스트 + core 의존성 검사
./gradlew test                          # 테스트만
./gradlew :cache-server:test --tests '*FrameCodecTest'   # 특정 테스트
```

테스트 결과는 `*/build/reports/tests/test/index.html`에서 볼 수 있습니다.

## 실행

```bash
./gradlew :cache-server:installDist
cache-server/build/install/openkache-server/bin/openkache-server                 # 기본 설정
cache-server/build/install/openkache-server/bin/openkache-server my.properties   # 설정 파일 지정
```

**기본 설정** (`cache-server/src/main/resources/application.properties`)

| 항목 | 값 |
|---|---|
| client listener | `127.0.0.1:7000` |
| peer TCP listener | `127.0.0.1:7001` |
| SWIM UDP listener | `127.0.0.1:7001/udp` |
| `/metrics` | `127.0.0.1:9091` |
| 정적 클러스터 멤버 | `cluster.members=1@127.0.0.1:7001` |
| SWIM probe 주기 | `500ms` |

`cluster.members`는 `nodeId@host:peerPort`를 쉼표로 구분하며 SWIM UDP 목적지로 사용합니다. TCP와 UDP는 프로토콜이
달라 같은 `peer.port` 번호를 함께 사용할 수 있습니다. 모든 노드에 같은 멤버 목록을 설정하고 각 노드의 `node.id`,
`peer.port`, client/admin port만 해당 노드 값으로 바꿉니다.

**설정 오류:** 포트 범위 밖, 포트 중복, `workers`가 1-256 밖, 로컬 노드 누락, 중복 NodeId·peer 주소, 잘못된 SWIM
timeout, 알 수 없는 key가 있으면 서버를 띄우지 않고 종료 코드 2로 끝납니다.

## PING 확인

서버를 띄운 상태에서 다른 터미널에서 실행합니다. bash의 `/dev/tcp`를 쓰므로 별도 도구가 필요 없습니다.

```bash
exec 3<>/dev/tcp/127.0.0.1/7000
# length=24, magic=0x4F4B, version=1, class=1(client), opcode=4(PING), flags=0, status=0,
# requestId=42, routingVersion=0
printf '\x00\x00\x00\x18\x4f\x4b\x01\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x2a\x00\x00\x00\x00\x00\x00\x00\x00' >&3
head -c 48 <&3 | od -An -tx1
exec 3<&-
```

정상 응답 (48 bytes):

```
 00 00 00 2c 4f 4b 01 02 04 00 00 00 00 00 00 00
 00 00 00 2a 00 00 00 00 00 00 00 00 00 00 00 00
 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
```

응답 필드는 다음과 같습니다.
- length 44
- class 2 (response)
- opcode 4
- status 0 (OK)
- requestId 42
- 이후 routingVersion, ownershipEpoch, valueVersion, detailLength는 모두 0

**지표 확인**

```bash
curl -s http://127.0.0.1:9091/metrics | grep openkache_requests_total
```

## 종료

실행 중인 터미널에서 `Ctrl+C`를 누르거나 `kill <pid>`(SIGTERM)를 보냅니다. shutdown hook이 아래 순서로 닫고 종료합니다.
1. TCP listener
2. SWIM scheduler와 UDP listener
3. Netty EventLoop
4. core worker
5. metrics endpoint

로그에 `shutdown complete`가 나오면 정상 종료입니다.

## Docker Compose (server + Prometheus + Grafana)

```bash
docker compose -f deploy/compose.yml up --build
docker compose -f deploy/compose.yml down
```

| 서비스 | 주소 |
|---|---|
| server | `127.0.0.1:7000` (client), `127.0.0.1:7001` (peer) |
| Prometheus | http://127.0.0.1:9090 |
| Grafana | http://127.0.0.1:3000 (익명 읽기 전용, "OpenKache Overview" 대시보드) |

server의 `/metrics`는 호스트에 공개하지 않고 compose 내부에서 Prometheus만 수집합니다.

## 디렉터리

| 경로 | 내용 |
|---|---|
| `cache-core/` | 공통 계약과 연구 패키지 (순수 Java) |
| `cache-server/` | Netty 서버 실행 애플리케이션 |
| `monitoring/` | Prometheus, Grafana 설정 |
| `deploy/` | Dockerfile, compose |
| `experiments/` | 실험 시나리오와 결과 요약 |
