# cache-server 이미지. 저장소 루트를 build context로 사용한다.
#   docker build -f deploy/docker/Dockerfile -t openkache-server .
# 기반 이미지는 linux/amd64, linux/arm64를 모두 지원한다.

FROM eclipse-temurin:21.0.12_8-jdk-noble AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle
COPY cache-core ./cache-core
COPY cache-server ./cache-server

# settings.gradle.kts가 include하므로 이미지에는 넣지 않더라도 빌드 단계에 디렉터리가 있어야 한다.
COPY experiment-runner ./experiment-runner
RUN ./gradlew --no-daemon :cache-server:installDist

FROM eclipse-temurin:21.0.12_8-jre-noble
RUN groupadd --system openkache && useradd --system --gid openkache --no-create-home openkache
WORKDIR /app
COPY --from=build /src/cache-server/build/install/openkache-server ./
COPY deploy/docker/server.properties ./config/server.properties
USER openkache

# 7000 client, 7001 peer, 9091 /metrics
EXPOSE 7000 7001 9091
ENTRYPOINT ["/app/bin/openkache-server"]
CMD ["/app/config/server.properties"]
