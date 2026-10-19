package io.openkache.server;

import io.openkache.server.config.ConfigException;
import io.openkache.server.config.ServerConfig;
import io.openkache.server.lifecycle.ServerApplication;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 서버 진입점. 설정 로딩 → 검증 → 기동 → 종료 대기 순서로 명시적으로 조립한다.
 *
 * <p>사용법: {@code openkache-server [설정 파일 경로]}. 경로가 없으면 classpath의 {@code application.properties}를 쓴다.
 * SIGTERM이나 Ctrl+C를 받으면 shutdown hook이 listener, EventLoop, worker, metrics를 닫는다.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final int EXIT_CONFIG_ERROR = 2;
    private static final int EXIT_START_ERROR = 1;

    private Main() {}

    public static void main(String[] args) throws InterruptedException {
        if (args.length > 1) {
            System.err.println("usage: openkache-server [config.properties]");
            System.exit(EXIT_CONFIG_ERROR);
        }

        ServerConfig config;
        try {
            config = ServerConfig.load(args.length == 1 ? Path.of(args[0]) : null);
        } catch (ConfigException e) {
            LOG.error("{}", e.getMessage());
            System.exit(EXIT_CONFIG_ERROR);
            return;
        }

        ServerApplication app;
        try {
            app = ServerApplication.start(config);
        } catch (RuntimeException e) {
            LOG.error("failed to start server", e);
            System.exit(EXIT_START_ERROR);
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(app::close, "openkache-shutdown-hook"));
        app.awaitTermination();
    }
}
