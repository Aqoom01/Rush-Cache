package io.openkache.server.routing;

import java.nio.file.Path;

/**
 * B1 라우팅 실서버 진입점. 사용법: {@code B1ServerMain <config.properties>}.
 * 설정 key: node.id, hosts(콤마 구분), client.port, peer.port, tablets.per.node.
 */
public final class B1ServerMain {

    private B1ServerMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: B1ServerMain <config.properties>");
            System.exit(2);
        }
        B1NodeConfig config = B1NodeConfig.load(Path.of(args[0]));
        B1Server server = B1Server.start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "b1-shutdown"));
        System.out.println("b1-server node=" + config.node() + " client=" + config.clientPort()
                + " peer=" + config.peerPort() + " tablets=" + server.table().size());
        server.awaitTermination();
    }
}
