package io.openkache.server.replication;

import java.nio.file.Path;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;

/** java -cp 'lib/*' io.openkache.server.replication.PbServerMain node.properties 로 실행한다. */
public final class PbServerMain {
    private PbServerMain() {}
    public static void main(String[] args) throws Exception {
        if(args.length!=1) throw new IllegalArgumentException("usage: PbServerMain <node.properties>");
        Path path = Path.of(args[0]);
        PbNodeConfig config = PbNodeConfig.load(path);
        if (config.protocol().equals("PB_FENCED")) {
            // Grants and votes are volatile. Refuse process restart within the same cluster directory.
            Path guard = path.toAbsolutePath().getParent().resolve("pb-fenced-node-" + config.node() + ".started");
            try { Files.createFile(guard); }
            catch (FileAlreadyExistsException e) {
                throw new IllegalStateException("PB_FENCED restart requires state recovery; use a new directory for a fresh cluster", e);
            }
        }
        var server=PbServer.start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close,"pb-shutdown"));
        System.out.println("PB_READY "+server.statusJson());
        server.awaitTermination();
    }
}
