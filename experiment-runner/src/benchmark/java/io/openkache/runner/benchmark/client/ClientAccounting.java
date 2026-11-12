package io.openkache.runner.benchmark.client;

import java.util.EnumMap;
import java.util.Map;

/** 요청 결과와 시도 계수. 모든 메서드는 synchronized이며 network/사용자 callback을 호출하지 않는다. */
public final class ClientAccounting {
    private final EnumMap<ClientOutcome.Operation, Mutable> counts =
            new EnumMap<>(ClientOutcome.Operation.class);
    private long rejected;
    private long duplicateSubmissions;

    public ClientAccounting() {
        for (var op : ClientOutcome.Operation.values()) { counts.put(op, new Mutable()); }
    }

    public synchronized void admitted(ClientOutcome.Operation op) { counts.get(op).admitted++; }
    public synchronized void attempt(ClientOutcome.Operation op) { counts.get(op).attempts++; }
    public synchronized void discovery(ClientOutcome.Operation op) { counts.get(op).discoveries++; }
    public synchronized void lateClient(ClientOutcome.Operation op) { counts.get(op).lateClient++; }
    public synchronized void lateDiscovery(ClientOutcome.Operation op) { counts.get(op).lateDiscovery++; }
    public synchronized void rejected() { rejected++; }
    public synchronized void duplicateSubmission() { duplicateSubmissions++; }

    public synchronized void terminal(ClientOutcome.Operation op, ClientOutcome.Category category) {
        terminal(op, category, 0);
    }

    /** attemptCount는 해당 logical 요청의 execute 호출 수다. */
    public synchronized void terminal(ClientOutcome.Operation op, ClientOutcome.Category category, int attemptCount) {
        Mutable c = counts.get(op);
        if (c.pending() <= 0 || attemptCount < 0) { throw new IllegalStateException("invalid terminal accounting"); }
        if (attemptCount > 1) { c.retried++; }
        switch (category) {
            case SUCCESS -> c.success++;
            case ERROR_RESPONSE -> c.error++;
            case NO_RESPONSE -> c.noResponse++;
        }
    }

    /** 한 시점의 불변 집계. admitted=success+errorResponse+noResponse+pending이다. */
    public synchronized Snapshot snapshot() {
        EnumMap<ClientOutcome.Operation, Counts> copy = new EnumMap<>(ClientOutcome.Operation.class);
        for (var entry : counts.entrySet()) {
            Mutable c = entry.getValue();
            copy.put(entry.getKey(), new Counts(c.admitted, c.success, c.error, c.noResponse,
                    c.pending(), c.attempts, c.retried, c.discoveries, c.lateClient, c.lateDiscovery));
        }
        return new Snapshot(copy, rejected, duplicateSubmissions);
    }

    public record Counts(long admitted, long success, long errorResponse, long noResponse,
            long pending, long attempts, long retriedLogical, long discoveries, long lateClientCompletions,
            long lateDiscoveryCompletions) {}

    public record Snapshot(Map<ClientOutcome.Operation, Counts> byOperation,
            long rejected, long duplicateSubmissions) {
        public Snapshot { byOperation = Map.copyOf(byOperation); }
        public long admitted() { return byOperation.values().stream().mapToLong(Counts::admitted).sum(); }
        public long success() { return byOperation.values().stream().mapToLong(Counts::success).sum(); }
        public long errorResponse() { return byOperation.values().stream().mapToLong(Counts::errorResponse).sum(); }
        public long noResponse() { return byOperation.values().stream().mapToLong(Counts::noResponse).sum(); }
        public long pending() { return byOperation.values().stream().mapToLong(Counts::pending).sum(); }
        public long attempts() { return byOperation.values().stream().mapToLong(Counts::attempts).sum(); }
        public long retriedLogical() { return byOperation.values().stream().mapToLong(Counts::retriedLogical).sum(); }
    }

    private static final class Mutable {
        long admitted, success, error, noResponse, attempts, retried, discoveries, lateClient, lateDiscovery;
        long pending() { return admitted - success - error - noResponse; }
    }
}

