package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.DirectMailbox;
import io.openkache.core.replication.VirtualTimeSource;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.Transport;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/** 고정 3노드의 동기 SET 복제 실험. 실제 시간·thread·네트워크 비용은 모델링하지 않는다. */
public final class ReplicationExperiment {
    private ReplicationExperiment() {}

    /** 모든 시간은 가상 밀리초. 추가 지연은 두 번째 Backup의 ACK에만 적용한다. */
    public record Condition(int delayMs, int extraAckMs, int deadlineMs) {
        public Condition {
            if (delayMs < 0 || extraAckMs < 0 || deadlineMs <= 0) throw new IllegalArgumentException("invalid timing");
        }
        public String id() { return "d" + delayMs + "-a" + extraAckMs + "-t" + deadlineMs; }
    }

    /** 지연은 성공/timeout을 포함한 응답 시간이며 성공 응답 지연은 별도로 기록한다(-1은 성공 없음). */
    public record Measurement(Condition condition, int requests, int ok, int timeouts,
            long responseMs, long successMs, int verifiedCopies, int timeoutPartialWrites, int messages) {}

    record Event(long at, long sequence, Runnable action) {}

    /** 같은 시각은 삽입 순서. 요청 deadline은 해당 요청의 전송 이벤트보다 먼저 예약한다. */
    static final class Scheduler {
        final VirtualTimeSource time = new VirtualTimeSource();
        final PriorityQueue<Event> events = new PriorityQueue<>(Comparator.comparingLong(Event::at)
                .thenComparingLong(Event::sequence));
        long sequence;
        void schedule(long at, Runnable action) { events.add(new Event(at, sequence++, action)); }
        void drain() {
            int steps = 0;
            while (!events.isEmpty()) {
                if (++steps > 1000) throw new IllegalStateException("event limit exceeded");
                Event event = events.remove();
                time.advanceTo(event.at());
                event.action().run();
            }
        }
    }

    public static List<Condition> conditions() {
        List<Condition> result = new ArrayList<>();
        for (int delay : new int[] {1, 5, 20})
            for (int extra : new int[] {0, 20, 100})
                for (int deadline : new int[] {50, 200, 500}) result.add(new Condition(delay, extra, deadline));
        return List.copyOf(result);
    }

    public static Measurement run(Condition condition, int requests) {
        if (requests < 1) throw new IllegalArgumentException("requests must be positive");
        Scheduler scheduler = new Scheduler();
        List<NodeId> nodes = List.of(new NodeId(0), new NodeId(1), new NodeId(2));
        OwnershipEpoch epoch = new OwnershipEpoch(1);
        Map<NodeId, KvEngine> stores = new LinkedHashMap<>();
        Map<NodeId, PrimaryBackupEngine> engines = new LinkedHashMap<>();
        int[] messages = {0};
        Transport transport = envelope -> {
            messages[0]++;
            int extra = envelope.from().equals(nodes.get(2))
                    && envelope.message() instanceof PrimaryBackupMessage.ReplicateAck ? condition.extraAckMs() : 0;
            long at = scheduler.time.monotonicNanos() + Duration.ofMillis(condition.delayMs() + extra).toNanos();
            scheduler.schedule(at, () -> engines.get(envelope.to()).onPeerMessage(envelope.from(), envelope.message()));
            return CompletableFuture.completedFuture(null);
        };
        int ok = 0, timeouts = 0, verified = 0, partial = 0;
        long responseMs = -1, successMs = -1;
        try {
            for (NodeId node : nodes) {
                KvEngine store = new KvEngine(node, List.of(new DirectMailbox()), scheduler.time);
                stores.put(node, store);
                engines.put(node, new PrimaryBackupEngine(new PrimaryBackupConfig(node, nodes.get(0),
                        nodes.subList(1, 3), epoch), store, new DirectMailbox(), transport, scheduler.time));
            }
            for (int i = 0; i < requests; i++) {
                Bytes key = Bytes.utf8("key-" + i), value = Bytes.utf8("value-" + i);
                long start = scheduler.time.monotonicNanos();
                long deadline = start + Duration.ofMillis(condition.deadlineMs()).toNanos();
                PrimaryBackupEngine primary = engines.get(nodes.get(0));
                scheduler.schedule(deadline, () -> primary.onTimer(new TimerEvent(
                        PrimaryBackupEngine.REPLICATE_DEADLINE_TIMER, scheduler.time.monotonicNanos())));
                CompletableFuture<Result> future = primary.execute(new Command.Set(key, value),
                        context(i + 1L, epoch, deadline)).toCompletableFuture();
                long[] completedAt = {-1};
                future.whenComplete((result, error) -> completedAt[0] = scheduler.time.monotonicNanos());
                // timeout 이후의 늦은 메시지도 배출하고 다음 요청과 분리한다.
                scheduler.drain();
                require(future.isDone() && !future.isCompletedExceptionally(), "request did not finish");
                Result result = future.getNow(null);
                long latency = Duration.ofNanos(completedAt[0] - start).toMillis();
                require(responseMs == -1 || responseMs == latency, "fixed model latency changed");
                responseMs = latency;
                int copies = 0;
                for (KvEngine store : stores.values()) {
                    Result read = store.execute(new Command.Get(key), context(i + 1L, epoch,
                            scheduler.time.monotonicNanos() + Duration.ofSeconds(1).toNanos())).toCompletableFuture().getNow(null);
                    require(read != null, "store did not finish synchronously");
                    if (read.status() == ResultStatus.OK) {
                        require(read.value().orElseThrow().equals(value), "value mismatch");
                        copies++;
                    } else require(read.status() == ResultStatus.MISS, "unexpected read status");
                }
                if (result.status() == ResultStatus.OK) {
                    require(copies == 3, "acknowledged write missing");
                    ok++; verified += copies; successMs = latency;
                } else {
                    require(result.status() == ResultStatus.TIMEOUT, "unexpected result " + result.status());
                    timeouts++;
                    if (copies > 0) partial++;
                }
            }
        } finally {
            engines.values().forEach(PrimaryBackupEngine::close);
            stores.values().forEach(KvEngine::close);
        }
        return new Measurement(condition, requests, ok, timeouts, responseMs, successMs, verified, partial, messages[0]);
    }

    private static RequestContext context(long operation, OwnershipEpoch epoch, long deadline) {
        return new RequestContext(new ClientId(1), new OperationId(operation), RoutingVersion.ZERO, epoch, new Deadline(deadline));
    }

    static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
