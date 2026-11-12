package io.openkache.server.lifecycle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.kv.KvStorage;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** {@link ServerApplicationTest}의 모든 테스트를 NVMe 저장소(일반 I/O)로 다시 돌리고, 저장소 지표 노출을 확인한다. */
class NvmeServerApplicationTest extends ServerApplicationTest {

    @TempDir
    static Path root;

    @Override
    KvStorage storage() throws IOException {
        return new KvStorage.Nvme(Files.createTempDirectory(root, "server"), 16L * 1024 * 1024, 1024L * 1024, 64 * 1024, false);
    }

    @Test
    void storageMetricsAreExposed() throws IOException {
        try (Socket socket = connect(config.clientPort())) {
            sendSet(socket, 1, bytes("k"), bytes("v"));
            read(socket);
            sendGet(socket, 2, bytes("k"));
            assertArrayEquals(bytes("v"), read(socket).value());
        }

        HttpURLConnection http = (HttpURLConnection)
                URI.create("http://127.0.0.1:" + config.adminPort() + "/metrics").toURL().openConnection();
        String body = new String(http.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertEquals(200, http.getResponseCode());
        assertTrue(body.contains("openkache_storage_buffer_hits_total 1"), body);
    }
}
