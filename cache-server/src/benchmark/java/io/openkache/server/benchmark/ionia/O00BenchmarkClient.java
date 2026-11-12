package io.openkache.server.benchmark.ionia;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * O00의 인증/fast ACK/DELETE 경로를 그대로 사용하는 장기 client 세션.
 * public 메서드는 thread-safe. delegate는 비차단이어야 하며 소유권을 이 객체에 넘긴다.
 * 사용자 completion은 내부 lock/control timer 밖의 별도 publisher에서 실행한다.
 */
public final class O00BenchmarkClient implements BenchmarkClient {
    public record Settings(String runId, String loadSlot, int streamCount, long wireClientBase,
            List<NodeId> replicas, NodeId leader, NodeId readReplica, long totalTimeoutNanos,
            int maxRounds, long retryBackoffNanos, int maxInFlight, int maxOutstandingRpc,
            long timerPeriodNanos) {
        public Settings {
            if (Objects.requireNonNull(runId).isBlank() || Objects.requireNonNull(loadSlot).isBlank()
                    || streamCount < 1 || streamCount > 1024 || wireClientBase < 0) {
                throw new IllegalArgumentException("invalid runtime namespace/stream count");
            }
            replicas = List.copyOf(replicas);
            new IoniaConfig(leader, leader, replicas, 0);
            if ((replicas.size() != 3 && replicas.size() != 5) || !replicas.contains(readReplica)) {
                throw new IllegalArgumentException("requires 3/5 replicas and a member readReplica");
            }
            try { Math.addExact(wireClientBase, 3L * streamCount - 1); }
            catch (ArithmeticException error) { throw new IllegalArgumentException("wire namespace overflow", error); }
            if (totalTimeoutNanos < 1 || totalTimeoutNanos > TimeUnit.SECONDS.toNanos(60)
                    || maxRounds < 1 || maxRounds > 100
                    || retryBackoffNanos < 0 || retryBackoffNanos > totalTimeoutNanos
                    || maxInFlight < 1 || maxInFlight > 100_000 || maxOutstandingRpc < 1
                    || timerPeriodNanos < TimeUnit.MILLISECONDS.toNanos(1)
                    || timerPeriodNanos > TimeUnit.SECONDS.toNanos(1)) {
                throw new IllegalArgumentException("invalid deadline/retry/resource limits");
            }
        }
    }

    public record Diagnostics(ClientAccounting.Snapshot logical, ObservedRpc.Snapshot rpc,
            int clientSessions, int active, int occupiedSlots, long latePublications,
            boolean closed, String closeError) {}

    private static final class Session {
        final IoniaBenchmarkSession client;
        long highestSequence = -1;
        volatile boolean discovered;
        Session(IoniaBenchmarkSession client) { this.client = client; }
    }
    private static final class AttemptState {
        final int number;
        final long start;
        Long end;
        ResultStatus status;
        String error;
        AttemptState(int number, long start) { this.number = number; this.start = start; }
        ClientOutcome.Attempt snapshot() {
            return new ClientOutcome.Attempt(number, start, end, status, error);
        }
    }
    private static final class Pending {
        final LogicalRequest request;
        final LogicalRequest.RequestId id;
        final ClientOutcome.Operation operation;
        final RequestContext context;
        final Session session;
        final long invoked;
        final List<AttemptState> attempts = new ArrayList<>();
        final CompletableFuture<ClientOutcome> completion = new CompletableFuture<>();
        int rounds;
        boolean done;
        ScheduledFuture<?> retry;
        Pending(LogicalRequest request, LogicalRequest.RequestId id, RequestContext context,
                Session session, long invoked) {
            this.request = request; this.id = id; this.context = context; this.session = session;
            this.invoked = invoked; operation = ClientOutcome.Operation.of(request.command());
        }
    }

    private final Object lock = new Object();
    private final Settings settings;
    private final TimeSource time;
    private final IoniaBenchmarkSession.Factory sessionFactory;
    private final IoniaClientTransport raw;
    private final ObservedRpc transport;
    private final ClientAccounting accounting = new ClientAccounting();
    private final Map<Long, Session> sessions = new HashMap<>();
    private final Map<LogicalRequest.RequestId, Pending> active = new HashMap<>();
    private final ScheduledThreadPoolExecutor control = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("o00-benchmark-control-", 0).factory());
    private final ExecutorService publisher = Executors.newFixedThreadPool(2,
            Thread.ofPlatform().daemon().name("o00-benchmark-publish-", 0).factory());
    private final AtomicBoolean resourcesClosed = new AtomicBoolean();
    private boolean closed;
    private int occupiedSlots;
    private long latePublications;
    private String closeError;

    public O00BenchmarkClient(Settings settings, IoniaClientTransport raw, TimeSource time) {
        this(settings, raw, time, true);
    }

    // 가상 시계 fixture에서는 자동 timer를 끄고 tick을 명시 호출한다.
    O00BenchmarkClient(Settings settings, IoniaClientTransport raw, TimeSource time, boolean autoTick) {
        this(settings, raw, time, autoTick, originalFactory(settings));
    }

    /** 실제 client만 교체하고 기존 실행/계측 수명주기를 재사용한다. factory는 전달된 transport/time을 사용해야 한다. */
    public O00BenchmarkClient(Settings settings, IoniaClientTransport raw, TimeSource time,
            IoniaBenchmarkSession.Factory factory) {
        this(settings, raw, time, true, factory);
    }

    private O00BenchmarkClient(Settings settings, IoniaClientTransport raw, TimeSource time,
            boolean autoTick, IoniaBenchmarkSession.Factory factory) {
        this.sessionFactory = Objects.requireNonNull(factory);
        this.settings = Objects.requireNonNull(settings);
        this.raw = Objects.requireNonNull(raw);
        this.time = Objects.requireNonNull(time);
        transport = new ObservedRpc(raw, settings.maxOutstandingRpc());
        control.setRemoveOnCancelPolicy(true);
        if (autoTick) {
            control.scheduleAtFixedRate(this::tick, settings.timerPeriodNanos(),
                    settings.timerPeriodNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private static IoniaBenchmarkSession.Factory originalFactory(Settings settings) {
        Objects.requireNonNull(settings);
        return (id, transport, time) -> {
            var client = new IoniaClient(id, settings.replicas(), settings.leader(), 0, transport, time,
                    new IoniaClient.Options(settings.readReplica(), true, 10_000_000L));
            return new IoniaBenchmarkSession() {
                @Override public CompletionStage<IoniaEngine.ViewInfo> refreshView() { return client.refreshView(); }
                @Override public CompletionStage<Result> execute(Command command, RequestContext context) { return client.execute(command, context); }
                @Override public void onTimer(TimerEvent event) { client.onTimer(event); }
                @Override public void close() { client.close(); }
            };
        };
    }

    @Override public CompletionStage<ClientOutcome> submit(LogicalRequest request) {
        long invoked = time.monotonicNanos();
        Objects.requireNonNull(request);
        int streams = settings.streamCount();
        if (request.streamId() != request.globalIndex() % streams
                || request.sequence() != request.globalIndex() / streams
                || request.clientId() != (long) request.phase().tag() * streams + request.streamId()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("trace stream identity mismatch"));
        }
        long wireClient;
        long wireOperation;
        try {
            wireClient = Math.addExact(settings.wireClientBase(), request.clientId());
            wireOperation = Math.addExact(request.sequence(), 1);
        } catch (ArithmeticException error) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("wire identity overflow", error));
        }
        var id = request.id(settings.runId(), settings.loadSlot());
        Pending pending;
        synchronized (lock) {
            if (closed) { return reject("client closed"); }
            Pending duplicate = active.get(id);
            if (duplicate != null) {
                if (!duplicate.request.equals(request)) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException("ID reused with a different request"));
                }
                accounting.duplicateSubmission();
                return duplicate.completion.minimalCompletionStage();
            }
            Session session = sessions.get(request.clientId());
            if (session != null && request.sequence() <= session.highestSequence) {
                return reject("completed/out-of-order sequence; retries belong inside the adapter");
            }
            if (occupiedSlots >= settings.maxInFlight()) { return reject("logical admission limit"); }
            if (session == null) {
                session = new Session(Objects.requireNonNull(sessionFactory.open(new ClientId(wireClient), transport, time)));
                sessions.put(request.clientId(), session);
            }
            var context = new RequestContext(new ClientId(wireClient), new OperationId(wireOperation),
                    RoutingVersion.ZERO, OwnershipEpoch.ZERO,
                    new Deadline(invoked + settings.totalTimeoutNanos()));
            pending = new Pending(request, id, context, session, invoked);
            session.highestSequence = request.sequence();
            active.put(id, pending); occupiedSlots++;
            accounting.admitted(pending.operation);
        }
        round(pending);
        return pending.completion.minimalCompletionStage();
    }

    private CompletionStage<ClientOutcome> reject(String reason) {
        accounting.rejected();
        return CompletableFuture.failedFuture(new RejectedExecutionException(reason));
    }

    private void round(Pending pending) {
        boolean discover;
        synchronized (lock) {
            if (pending.done) { return; }
            pending.rounds++;
            discover = pending.rounds > 1 || !pending.session.discovered;
            if (discover) { accounting.discovery(pending.operation); }
        }
        if (expired(pending)) {
            finish(pending, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.DEADLINE, null);
            return;
        }
        if (!discover) { dispatch(pending); return; }
        CompletionStage<IoniaEngine.ViewInfo> stage;
        try { stage = pending.session.client.refreshView(); }
        catch (RuntimeException error) {
            retryOrFinish(pending, null, ClientOutcome.Reason.DISCOVERY_FAILED); return;
        }
        stage.whenComplete((info, error) -> {
            synchronized (lock) {
                if (pending.done) { accounting.lateDiscovery(pending.operation); return; }
            }
            if (expired(pending)) {
                finish(pending, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.DEADLINE, null);
            } else if (error != null || info == null) {
                retryOrFinish(pending, null, ClientOutcome.Reason.DISCOVERY_FAILED);
            } else {
                pending.session.discovered = true;
                dispatch(pending);
            }
        });
    }

    private void dispatch(Pending pending) {
        AttemptState attempt;
        synchronized (lock) {
            if (pending.done) { return; }
            attempt = new AttemptState(pending.attempts.size() + 1, time.monotonicNanos());
            // 예약 후 취소와 경합할 수 있으므로 해당 시도는 실행 불명확으로 취급한다.
            if (pending.context.deadline().isExpired(time)) { attempt = null; }
            else { pending.attempts.add(attempt); accounting.attempt(pending.operation); }
        }
        if (attempt == null) {
            finish(pending, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.DEADLINE, null);
            return;
        }
        AttemptState observed = attempt;
        try {
            pending.session.client.execute(pending.request.command(), pending.context)
                    .whenComplete((result, error) -> completed(pending, observed, result, error));
        } catch (RuntimeException error) { completed(pending, observed, null, error); }
    }

    private void completed(Pending pending, AttemptState attempt, Result result, Throwable error) {
        synchronized (lock) {
            if (attempt.end != null) { return; }
            attempt.end = time.monotonicNanos();
            attempt.status = result == null ? null : result.status();
            attempt.error = error == null ? null : error.getClass().getSimpleName();
            if (pending.done) { accounting.lateClient(pending.operation); return; }
        }
        if (expired(pending)) {
            accounting.lateClient(pending.operation);
            finish(pending, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.DEADLINE, null);
        } else if (error != null || result == null) {
            retryOrFinish(pending, null, ClientOutcome.Reason.CLIENT_EXCEPTION);
        } else if (result.status() == ResultStatus.TIMEOUT) {
            finish(pending, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CLIENT_TIMEOUT, result);
        } else if (result.status() == ResultStatus.UNAVAILABLE || result.status() == ResultStatus.NOT_PRIMARY
                || result.status() == ResultStatus.BUSY) {
            retryOrFinish(pending, result, ClientOutcome.Reason.CLIENT_ERROR);
        } else if (validSuccess(pending.request.command(), result)) {
            finish(pending, ClientOutcome.Category.SUCCESS, ClientOutcome.Reason.SUCCESS, result);
        } else {
            boolean malformed = result.status() == ResultStatus.OK || result.status() == ResultStatus.MISS;
            finish(pending, ClientOutcome.Category.ERROR_RESPONSE,
                    malformed ? ClientOutcome.Reason.MALFORMED_RESULT : ClientOutcome.Reason.CLIENT_ERROR, result);
        }
    }

    private static boolean validSuccess(Command command, Result result) {
        if (command instanceof Command.Get) {
            return result.status() == ResultStatus.OK && result.value().isPresent()
                    || result.status() == ResultStatus.MISS && result.value().isEmpty();
        }
        if (result.value().isPresent()) { return false; }
        return result.status() == ResultStatus.OK
                || command instanceof Command.Delete && result.status() == ResultStatus.MISS;
    }

    private void retryOrFinish(Pending pending, Result result, ClientOutcome.Reason reason) {
        boolean retry;
        synchronized (lock) {
            if (pending.done) { return; }
            retry = pending.rounds < settings.maxRounds() && !pending.context.deadline().isExpired(time);
            if (retry) {
                try {
                    pending.retry = control.schedule(() -> round(pending),
                            settings.retryBackoffNanos(), TimeUnit.NANOSECONDS);
                    return;
                } catch (RejectedExecutionException ignored) { /* close/deadline completion below */ }
            }
        }
        if (expired(pending)) {
            finish(pending, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.DEADLINE, null);
        } else {
            finish(pending, result == null ? ClientOutcome.Category.NO_RESPONSE
                    : ClientOutcome.Category.ERROR_RESPONSE, reason, result);
        }
    }

    private boolean expired(Pending pending) { return pending.context.deadline().isExpired(time); }

    private boolean finish(Pending pending, ClientOutcome.Category category, ClientOutcome.Reason reason, Result result) {
        long decided;
        List<ClientOutcome.Attempt> attempts;
        synchronized (lock) {
            if (pending.done) { return false; }
            pending.done = true;
            active.remove(pending.id);
            if (pending.retry != null) { pending.retry.cancel(false); }
            decided = time.monotonicNanos();
            attempts = pending.attempts.stream().map(AttemptState::snapshot).toList();
        }
        Runnable publish = () -> {
            try {
                ClientOutcome outcome;
                synchronized (lock) {
                    long terminal = time.monotonicNanos();
                    boolean late = category != ClientOutcome.Category.NO_RESPONSE
                            && pending.context.deadline().monotonicNanos() - terminal <= 0;
                    if (late) { latePublications++; }
                    var finalCategory = late ? ClientOutcome.Category.NO_RESPONSE : category;
                    accounting.terminal(pending.operation, finalCategory, attempts.size());
                    outcome = new ClientOutcome(pending.id, pending.operation, finalCategory,
                            late ? ClientOutcome.Reason.DEADLINE : reason, late ? null : result,
                            pending.invoked, decided, terminal, pending.context.clientId().value(),
                            pending.context.operationId().value(), pending.rounds, attempts);
                }
                pending.completion.complete(outcome);
            }
            finally { synchronized (lock) { occupiedSlots--; } }
        };
        try { publisher.execute(publish); }
        catch (RejectedExecutionException closedPublisher) { publish.run(); }
        return true;
    }

    /** watchdog 관찰. raw stage를 cancel하거나 permit을 반환하지 않는다. */
    void tick() {
        List<Pending> pending;
        List<Session> clients;
        synchronized (lock) {
            if (closed) { return; }
            pending = List.copyOf(active.values());
            clients = List.copyOf(sessions.values());
        }
        for (Pending item : pending) {
            if (expired(item)) { finish(item, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.DEADLINE, null); }
        }
        for (Session client : clients) {
            try { client.client.onTimer(new TimerEvent(IoniaEngine.TICK, time.monotonicNanos())); }
            catch (RuntimeException error) {
                for (Pending item : pending) {
                    if (item.session == client) { finish(item, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CLIENT_EXCEPTION, null); }
                }
            }
        }
    }

    @Override public boolean cancel(LogicalRequest.RequestId id) {
        Pending pending;
        synchronized (lock) { pending = active.get(Objects.requireNonNull(id)); }
        return pending != null && finish(pending, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CANCELLED, null);
    }

    @Override public ClientAccounting.Snapshot snapshot() { return accounting.snapshot(); }

    public Diagnostics diagnostics() {
        synchronized (lock) {
            return new Diagnostics(accounting.snapshot(), transport.snapshot(), sessions.size(),
                    active.size(), occupiedSlots, latePublications, closed, closeError);
        }
    }

    @Override public void close() {
        if (!resourcesClosed.compareAndSet(false, true)) { return; }
        List<Pending> pending;
        List<Session> clients;
        synchronized (lock) {
            closed = true; pending = List.copyOf(active.values()); clients = List.copyOf(sessions.values());
        }
        for (Pending item : pending) {
            finish(item, ClientOutcome.Category.NO_RESPONSE, ClientOutcome.Reason.CLOSED, null);
        }
        transport.stopAdmission();
        control.shutdownNow();
        clients.forEach(client -> client.client.close());
        if (raw instanceof AutoCloseable owner) {
            try { owner.close(); }
            catch (Exception error) {
                if (error instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                synchronized (lock) { closeError = error.getClass().getSimpleName(); }
            }
        }
        // terminal publication은 버리지 않고 drain한다. 사용자 callback은 비차단이어야 한다.
        publisher.shutdown();
    }
}

