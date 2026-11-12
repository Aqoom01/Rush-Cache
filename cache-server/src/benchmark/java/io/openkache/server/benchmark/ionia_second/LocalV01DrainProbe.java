package io.openkache.server.benchmark.ionia_second;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.runner.benchmark.execution.DrainProbe;
import io.openkache.server.benchmark.ionia.LocalO00DrainProbe;
import java.util.Map;

/** 같은 JVM의 V01 공유 O00 엔진만 관찰한다. 원격 apply/SSD force 증거로 사용하지 않는다. */
public final class LocalV01DrainProbe implements DrainProbe {
    private final LocalO00DrainProbe delegate;
    public LocalV01DrainProbe(V01BenchmarkClient client, Map<NodeId, IoniaEngine> engines, NodeId leader, long view) {
        delegate = new LocalO00DrainProbe(client.runtime(), engines, leader, view);
    }
    @Override public Observation observe(long expectedWrites) { return delegate.observe(expectedWrites); }
    @Override public Observation afterClose(long expectedWrites) { return delegate.afterClose(expectedWrites); }
}
