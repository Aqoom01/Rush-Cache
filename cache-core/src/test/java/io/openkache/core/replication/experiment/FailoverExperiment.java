package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 단일 Primary crash와 순차 클라이언트 재시도 모델. 시간 단위 ms, 로컬 호출 비용은 0. */
public final class FailoverExperiment {
    private FailoverExperiment() {}
    public record Measurement(int delay, int timeout, int retry, long detection, long transition,
            long clientAndWrite, long total, int preserved, int redirects) {}

    static final class Cluster implements AutoCloseable {
        final ReplicationExperiment.Scheduler scheduler = new ReplicationExperiment.Scheduler();
        final Map<NodeId, KvEngine> stores = new LinkedHashMap<>();
        final Map<NodeId, CrashOnlyPrimaryBackupEngine> engines = new LinkedHashMap<>();
        final Set<NodeId> dead = new HashSet<>();
        boolean dropReady;
        Cluster(int delay, int timeout) {
            List<NodeId> ids = List.of(new NodeId(0), new NodeId(1), new NodeId(2));
            Transport transport = e -> {
                if (!dead.contains(e.from())) scheduler.schedule(now() + ms(delay), () -> {
                    if (dead.contains(e.from()) || dead.contains(e.to())) return;
                    if (dropReady && e.message() instanceof CrashOnlyPrimaryBackupEngine.Control c
                            && c.kind() == CrashOnlyPrimaryBackupEngine.Kind.READY) return;
                    engines.get(e.to()).onPeerMessage(e.from(), e.message());
                });
                return CompletableFuture.completedFuture(null);
            };
            for (NodeId id : ids) {
                var store = new KvEngine(id, List.of(new DirectMailbox()), scheduler.time);
                stores.put(id, store);
                engines.put(id, new CrashOnlyPrimaryBackupEngine(new PrimaryBackupConfig(id, ids.get(0),
                        ids.subList(1, 3), new OwnershipEpoch(1)), store, new DirectMailbox(), transport,
                        scheduler.time, Duration.ofMillis(10), Duration.ofMillis(timeout)));
            }
            tick();
        }
        long now() { return scheduler.time.monotonicNanos(); }
        void tick() {
            engines.forEach((id, engine) -> { if (!dead.contains(id)) engine.onTimer(new TimerEvent(CrashOnlyPrimaryBackupEngine.TICK, now())); });
            scheduler.schedule(now() + ms(1), this::tick);
        }
        void advance(long until) {
            // Scheduler drain은 유한 복제용이다. 주기 timer가 있는 실험에서는 종료 시각까지 진행한다.
            while (!scheduler.events.isEmpty() && scheduler.events.peek().at() <= until) {
                var event = scheduler.events.remove(); scheduler.time.advanceTo(event.at()); event.action().run();
            }
            scheduler.time.advanceTo(until);
        }
        RequestContext context(long epoch) {
            return new RequestContext(new ClientId(1), new OperationId(1), RoutingVersion.ZERO,
                    new OwnershipEpoch(epoch), new Deadline(now() + ms(1000)));
        }
        CompletableFuture<Result> set(int node, String key, long epoch) {
            return engines.get(new NodeId(node)).execute(new Command.Set(Bytes.utf8(key), Bytes.utf8(key)), context(epoch)).toCompletableFuture();
        }
        void crash() { dead.add(new NodeId(0)); engines.get(new NodeId(0)).close(); }
        void verify(String key) {
            for (int id : new int[] {1, 2}) {
                Result r = stores.get(new NodeId(id)).execute(new Command.Get(Bytes.utf8(key)), context(2)).toCompletableFuture().join();
                require(r.status() == ResultStatus.OK && r.value().orElseThrow().equals(Bytes.utf8(key)), "lost acknowledged write");
            }
        }
        @Override public void close() { engines.values().forEach(CrashOnlyPrimaryBackupEngine::close); stores.values().forEach(KvEngine::close); }
    }
    static long ms(long n) { return Duration.ofMillis(n).toNanos(); }
    static long millis(long n) { return Duration.ofNanos(n).toMillis(); }

    public static Measurement run(int delay, int timeout, int retry) {
        try (var c = new Cluster(delay, timeout)) {
            for (int i = 0; i < 10; i++) {
                var write = c.set(0, "before-" + i, 1); c.advance(c.now() + ms(2L * delay + 1));
                require(write.join().status() == ResultStatus.OK, "warmup failed");
            }
            long crash = c.now(); c.crash();
            int target = 0, redirects = 0; long epoch = 1, success = -1;
            CompletableFuture<Result> pending = null;
            long[] completed = {-1};
            for (int attempt = 0; attempt < 2000 && success < 0; attempt++) {
                if (pending == null) {
                    pending = c.set(target, "after", epoch);
                    pending.thenAccept(r -> { if (r.status() == ResultStatus.OK) completed[0] = c.now(); });
                }
                if (pending.isDone()) {
                    Result r = pending.join();
                    if (r.status() == ResultStatus.OK) { success = completed[0]; break; }
                    if (r.status() == ResultStatus.STALE_EPOCH) epoch = r.ownershipEpoch().value();
                    else if (r.status() == ResultStatus.NOT_PRIMARY) {
                        target = Math.toIntExact(r.redirect().orElseThrow().target().value()); redirects++;
                    } else if (target == 0) target = 2; // 접속 실패 후 알려진 Backup을 조회한다.
                    pending = null;
                }
                c.advance(c.now() + ms(retry));
            }
            require(success >= 0, "failover did not finish");
            var primary = c.engines.get(new NodeId(1));
            for (int i = 0; i < 10; i++) c.verify("before-" + i);
            c.verify("after");
            long detection = primary.detectedAtNanos(), ready = primary.readyAtNanos();
            require(detection >= crash && ready >= detection && success >= ready, "invalid phase order");
            return new Measurement(delay, timeout, retry, millis(detection-crash), millis(ready-detection),
                    millis(success-ready), millis(success-crash), 10, redirects);
        }
    }
}
