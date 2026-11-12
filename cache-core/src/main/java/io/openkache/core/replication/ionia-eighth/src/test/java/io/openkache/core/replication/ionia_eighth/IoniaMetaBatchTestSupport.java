package io.openkache.core.replication.ionia_eighth;
import io.openkache.core.replication.ionia.*;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaMessage.LogEntry;
import io.openkache.core.runtime.*;
import io.openkache.core.spi.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import org.junit.jupiter.api.*;

/** Deterministic acceptance fixtures. Failures report the delivery trace; no sleeps or wall-clock races. */
@Timeout(20)
class IoniaMetaBatchTestSupport {
    static final NodeId A = new NodeId(1), B = new NodeId(2), C = new NodeId(3);
    static final ClientId CLIENT = new ClientId(7);
    static final Bytes K = Bytes.utf8("key");
    static Command.Set set(String value) { return new Command.Set(K, Bytes.utf8(value)); }
    static LogEntry entry(long id) { return new LogEntry(id, CLIENT, new OperationId(id), set("v" + id)); }
    static RequestContext context(long id) { return new RequestContext(CLIENT, new OperationId(id), RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(1_000_000_000L)); }
    static IoniaClientTransport.Request request(long id, Command command, long view) {
        return new IoniaClientTransport.Request(view, command, CLIENT, new OperationId(id), RoutingVersion.ZERO, OwnershipEpoch.ZERO, 1_000_000_000L);
    }
    static Result result(CompletionStage<Result> stage) { assertTrue(stage.toCompletableFuture().isDone(), "fixture did not drive completion"); return stage.toCompletableFuture().join(); }

    static final class Clock implements TimeSource {
        long now; public long monotonicNanos() { return now; } public long epochMillis() { return 0; }
    }
    static final class MemoryStorage implements IoniaStorage {
        final Queue<Runnable> tasks; final Map<Bytes, Bytes> values = new HashMap<>(); final Map<Bytes, Long> versions = new HashMap<>();
        final Set<Bytes> held = new HashSet<>(); final List<Runnable> blocked = new ArrayList<>();
        boolean holdReadReplies; final List<Runnable> delayedReadReplies = new ArrayList<>();
        MemoryStorage(Queue<Runnable> tasks) { this.tasks = tasks; }
        public CompletionStage<Result> execute(Command command, RequestContext context) {
            CompletableFuture<Result> future = new CompletableFuture<>();
            Runnable apply = () -> {
                if (command instanceof Command.Get get) {
                    Result value = values.containsKey(get.key()) ? Result.ok(values.get(get.key()), versions.get(get.key()), RoutingVersion.ZERO, OwnershipEpoch.ZERO) : plain(ResultStatus.MISS);
                    if (holdReadReplies) delayedReadReplies.add(() -> future.complete(value)); else future.complete(value);
                }
                else if (command instanceof Command.Set set) { values.put(set.key(), set.value()); long version = versions.merge(set.key(), 1L, Long::sum);
                    future.complete(new Result(ResultStatus.OK, Optional.empty(), version, RoutingVersion.ZERO, OwnershipEpoch.ZERO, Optional.empty())); }
                else { boolean existed = values.remove(IoniaCommands.key(command)) != null; versions.remove(IoniaCommands.key(command)); future.complete(plain(existed ? ResultStatus.OK : ResultStatus.MISS)); }
            };
            if (!(command instanceof Command.Get) && held.contains(IoniaCommands.key(command))) blocked.add(apply); else tasks.add(apply);
            return future;
        }
        void release(Bytes key) { held.remove(key); tasks.addAll(blocked); blocked.clear(); }
        public CompletionStage<Void> reset() { values.clear(); versions.clear(); return CompletableFuture.completedFuture(null); }
        public EngineSnapshot snapshot() { return new EngineSnapshot(A, 0, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
        public void onPeerMessage(NodeId from, PeerMessage message) { } public void onTimer(TimerEvent timer) { } public void close() { }
    }
    static Result plain(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
    static final class Cluster implements IoniaMetaBatchTransport, AutoCloseable {
        final Clock clock = new Clock(); final Queue<Runnable> tasks = new ArrayDeque<>();
        final Mailbox mailbox = new Mailbox() { public boolean offer(Runnable task) { return tasks.offer(task); } public int capacity() { return 100000; } };
        final List<NodeId> nodes = new ArrayList<>(); final Map<NodeId, IoniaEngine> engines = new HashMap<>();
        final Map<NodeId, MemoryStorage> stores = new HashMap<>(); final List<PeerEnvelope> messages = new ArrayList<>();
        final Set<NodeId> offline = new HashSet<>(), hiddenAcks = new HashSet<>(); final List<NodeId> executed = new ArrayList<>();
        final List<String> trace = new ArrayList<>(); Predicate<PeerEnvelope> drop = envelope -> false; final IoniaMetaBatchClient client;
        Cluster(int count) {
            for (int i = 1; i <= count; i++) nodes.add(new NodeId(i));
            for (NodeId node : nodes) create(node, false, 0);
            client = new IoniaMetaBatchClient(CLIENT, nodes, A, 0, this, clock, new IoniaMetaBatchSettings(4,65536,2,256,4));
        }
        void create(NodeId node, boolean recovering, long view) {
            MemoryStorage storage = new MemoryStorage(tasks); stores.put(node, storage);
            engines.put(node, new IoniaEngine(new IoniaConfig(node, A, nodes, 0), storage, mailbox,
                    envelope -> { messages.add(envelope); return CompletableFuture.completedFuture(null); }, clock,
                    IoniaViewStore.simulation(), recovering, view, Long.MAX_VALUE));
        }
        void crash(NodeId node) { offline.add(node); engines.get(node).close(); }
        void restart(NodeId node, long view) { engines.get(node).close(); offline.remove(node); create(node, true, view); }
        public CompletionStage<WriteAck> record(NodeId to, Request request) {
            if (offline.contains(to)) return CompletableFuture.failedFuture(new IllegalStateException("offline"));
            CompletionStage<WriteAck> response = engines.get(to).recordWrite(request);
            return hiddenAcks.contains(to) ? new CompletableFuture<>() : response;
        }
        public CompletionStage<Reply> execute(NodeId to, Request request) {
            executed.add(to); return offline.contains(to) ? CompletableFuture.failedFuture(new IllegalStateException("offline")) : engines.get(to).execute(request);
        }
        public CompletionStage<ReadReply> followerRead(NodeId to, Request request) { return engines.get(to).followerRead(request); }
        public CompletionStage<MetaReply> metaQuery(NodeId to, Request request) { return engines.get(to).metaQuery(request); }
        public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) { return offline.contains(to) ? CompletableFuture.failedFuture(new IllegalStateException("offline")) : CompletableFuture.completedFuture(engines.get(to).viewInfo()); }
        public CompletionStage<List<Answer>> metaBatch(NodeId to,List<Item> items){
            var values=items.stream().map(i->engines.get(to).metaQuery(i.request()).thenApply(reply->new Answer(i.token(),reply)).toCompletableFuture()).toList();
            return CompletableFuture.allOf(values.toArray(CompletableFuture<?>[]::new)).thenApply(ignored->values.stream().map(CompletableFuture::join).toList());
        }
        void runOne() { if (!tasks.isEmpty()) tasks.remove().run(); }
        void runOwners() { int limit = 100000; while (!tasks.isEmpty()) { assertTrue(limit-- > 0, trace.toString()); runOne(); } }
        void pump() {
            int limit = 100000;
            while (!tasks.isEmpty() || !messages.isEmpty() || client.diagnostics().queued()>0) {
                clock.now+=3;client.onTimer(new TimerEvent(IoniaEngine.TICK,clock.now));
                assertTrue(limit-- > 0, trace.toString()); runOwners();
                if (!messages.isEmpty()) {
                    PeerEnvelope message = messages.removeFirst(); trace.add(message.from() + "->" + message.to() + ":" + message.message());
                    if (!offline.contains(message.from()) && !offline.contains(message.to()) && !drop.test(message)) engines.get(message.to()).onPeerMessage(message.from(), message.message());
                }
            }
            for (IoniaEngine engine : engines.values()) assertTrue(engine.failureReason().isEmpty(), engine.failureReason().toString() + trace);
        }
        void tick() { client.onTimer(new TimerEvent(IoniaEngine.TICK, clock.now)); engines.values().forEach(engine -> engine.onTimer(new TimerEvent(IoniaEngine.TICK, clock.now))); }
        public void close() { client.close(); engines.values().forEach(IoniaEngine::close); }
    }
}
