package io.openkache.core.kv;

import io.openkache.core.model.NodeId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.spi.MetricsSink;
import io.openkache.core.spi.TimeSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;

/** {@link KvEngineTest}의 모든 테스트를 NVMe 저장소(일반 I/O)로 다시 돌린다. 용량이 넉넉해 축출은 일어나지 않는다. */
class NvmeKvEngineTest extends KvEngineTest {

    @TempDir
    static Path root;

    @Override
    KvEngine newEngine(List<OwnerWorker> workers) {
        try {
            Path dir = Files.createTempDirectory(root, "kv");
            KvStorage storage = new KvStorage.Nvme(dir, 64L * 1024 * 1024, 4L * 1024 * 1024, 64 * 1024, false);
            return new KvEngine(new NodeId(0), workers, TimeSource.system(), storage, MetricsSink.noop());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
