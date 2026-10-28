package io.openkache.core.replication.ionia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.api.Command;
import io.openkache.core.api.Deadline;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.ClientId;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.WorkerId;
import io.openkache.core.runtime.OwnerWorker;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.spi.Transport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class IoniaWorkerIntegrationTest {
    @Test
    @Timeout(20)
    void concurrentClientsReplicateThroughRealOwnerWorkersAndTheCommonKvEngine() throws Exception {
        List<NodeId> nodes = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
        NodeId leader = nodes.getFirst();
        ClientId clientId = new ClientId(1000);
        TimeSource time = TimeSource.system();
        Map<NodeId, IoniaEngine> engines = new ConcurrentHashMap<>();
        List<OwnerWorker> workers = new ArrayList<>();
        List<KvEngine> kvs = new ArrayList<>();
        Transport peers = envelope -> {
            engines.get(envelope.to()).onPeerMessage(envelope.from(), envelope.message());
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        };
        IoniaClientTransport clients = new IoniaClientTransport() {
            @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) {
                return java.util.concurrent.CompletableFuture.completedFuture(engines.get(to).viewInfo());
            }
            @Override public CompletionStage<WriteAck> record(NodeId to, Request request) {
                return engines.get(to).recordWrite(request);
            }
            @Override public CompletionStage<Reply> execute(NodeId to, Request request) {
                return engines.get(to).execute(request);
            }
        };
        try {
            for (NodeId node : nodes) {
                OwnerWorker owner = new OwnerWorker(new WorkerId(Math.toIntExact(node.value())), 10000);
                workers.add(owner);
                KvEngine kv = new KvEngine(node, List.of(owner), time);
                kvs.add(kv);
                engines.put(node, new IoniaEngine(new IoniaConfig(node, leader, nodes, 0), kv, owner, peers, time));
            }
            try (IoniaClient client = new IoniaClient(clientId, nodes, leader, 0, clients, time);
                    var callers = Executors.newFixedThreadPool(4)) {
                List<Future<Result>> requests = new ArrayList<>();
                for (int i = 0; i < 32; i++) {
                    int index = i;
                    requests.add(callers.submit(() -> client.execute(
                            new Command.Set(Bytes.utf8("key-" + index), Bytes.utf8("value-" + index)),
                            context(clientId, index + 1, time)).toCompletableFuture().get(5, TimeUnit.SECONDS)));
                }
                for (Future<Result> request : requests) {
                    assertEquals(ResultStatus.OK, request.get(5, TimeUnit.SECONDS).status());
                }
                for (int i = 0; i < 32; i++) {
                    Result result = client.execute(new Command.Get(Bytes.utf8("key-" + i)),
                            context(clientId, 100 + i, time)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                    assertEquals(Bytes.utf8("value-" + i), result.value().orElseThrow());
                }
                long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (engines.values().stream().anyMatch(engine -> engine.progress().appliedIndex() != 32)
                        && System.nanoTime() - waitUntil < 0) {
                    Thread.sleep(1);
                }
                for (IoniaEngine engine : engines.values()) {
                    assertEquals(32, engine.progress().recordedCount());
                    assertEquals(32, engine.progress().appliedIndex());
                }
                for (KvEngine kv : kvs) {
                    Result value = kv.execute(new Command.Get(Bytes.utf8("key-0")),
                            context(clientId, 999, time)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                    assertTrue(value.value().isPresent());
                    assertEquals(1, value.valueVersion());
                }
            }
        } finally {
            engines.values().forEach(IoniaEngine::close);
            kvs.forEach(KvEngine::close);
            workers.forEach(OwnerWorker::close);
        }
    }

    private static RequestContext context(ClientId client, long operation, TimeSource time) {
        return new RequestContext(client, new OperationId(operation), RoutingVersion.ZERO, OwnershipEpoch.ZERO,
                Deadline.after(time, Duration.ofSeconds(5)));
    }
}
