package io.openkache.server.replication;

import java.nio.file.Path;

/** java -cp 'lib/*' io.openkache.server.replication.PbServerMain node.properties 로 실행한다. */
public final class PbServerMain {
    private PbServerMain() {}
    public static void main(String[] args) throws Exception {
        if(args.length!=1) throw new IllegalArgumentException("usage: PbServerMain <node.properties>");
        var server=PbServer.start(PbNodeConfig.load(Path.of(args[0])));
        Runtime.getRuntime().addShutdownHook(new Thread(server::close,"pb-shutdown"));
        System.out.println("PB_READY "+server.statusJson());
        server.awaitTermination();
    }
}
