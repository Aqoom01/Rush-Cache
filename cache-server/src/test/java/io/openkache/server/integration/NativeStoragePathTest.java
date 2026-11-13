package io.openkache.server.integration;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class NativeStoragePathTest {
    private Properties settings() {
        var p = new Properties(); p.setProperty("run.id", "storage-path-test");
        p.setProperty("group.0.hosts", "127.0.0.11,127.0.0.12,127.0.0.13");
        p.setProperty("group.1.hosts", "127.0.0.14,127.0.0.15,127.0.0.16");
        return p;
    }
    @Test void retainsDefaultAndSeparatesCoLocatedNativeFilesFromEvidence() {
        var p = settings(); var evidence = Path.of("/ebs/evidence/a1-authority");
        assertEquals(evidence.resolve("data"), IntegrationReplicaMain.nativeStoragePath(new IntegrationConfig(p).layer(true), 0, 1, evidence));
        p.setProperty("native.data.root", "/mnt/nvme/cache"); var c = new IntegrationConfig(p);
        var a1 = IntegrationReplicaMain.nativeStoragePath(c.layer(true), 0, 1, evidence);
        var a2 = IntegrationReplicaMain.nativeStoragePath(c.layer(false), 0, 1, evidence);
        assertEquals(Path.of("/mnt/nvme/cache/storage-path-test/g0-n1/RAFT"), a1);
        assertEquals(Path.of("/mnt/nvme/cache/storage-path-test/g0-n1/O00"), a2);
        assertNotEquals(a1, a2);
    }
    @Test void rejectsRelativeTraversalAndShellPaths() {
        for (String root : new String[] {"data", "/mnt/../ebs", "/mnt/$(whoami)", "/mnt/cache space"}) {
            var p = settings(); p.setProperty("native.data.root", root);
            assertThrows(IllegalArgumentException.class, () -> new IntegrationConfig(p));
        }
    }
}
