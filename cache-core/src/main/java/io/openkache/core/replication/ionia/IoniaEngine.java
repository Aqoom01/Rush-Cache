package io.openkache.core.replication.ionia;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaLog.RequestId;
import io.openkache.core.replication.ionia.IoniaMessage.LogEntry;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** IONIA O00 control state machine. Mutable protocol state is confined to the injected owner.
 * Log ACKs protect replicated RAM records; SSD KV storage and stable view metadata are separate.
 * All network calls are nonblocking. A restarted replica MUST use the recovering constructor.
 */
public final class IoniaEngine implements CacheEngine {
    public static final String TICK = "ionia.tick";
    public enum Status { NORMAL, VIEW_CHANGE, RECOVERING }
    public record Progress(NodeId nodeId, int recordedCount, long lastLogIndex, long commitIndex, long appliedIndex) { }
    public record ViewInfo(long view, NodeId leaderId, Status status) { }
    private record Waiting(RequestContext context, CompletableFuture<Result> future) { }
    private record ReadWait(Command.Get command, Waiting waiting, long barrier, long view) { }
    private record RecordCall(IoniaClientTransport.Request request, Deadline deadline) { }
    private final IoniaConfig config;
    private IoniaLog log = new IoniaLog();
    private final CacheEngine localKv;
    private final Mailbox owner;
    private final Transport transport;
    private final TimeSource time;
    private final IoniaViewStore stableViews;
    private final long failureTimeoutNanos;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<CompletableFuture<Result>, Deadline> pending = new ConcurrentHashMap<>();
    private final Map<CompletableFuture<IoniaClientTransport.WriteAck>, RecordCall> recordCalls = new ConcurrentHashMap<>();
    private final Set<CompletableFuture<?>> auxiliary = ConcurrentHashMap.newKeySet();
    private final Map<NodeId, Long> matched = new HashMap<>();
    private final Map<RequestId, Result> appliedResults = new HashMap<>();
    private final Map<RequestId, List<Waiting>> writes = new HashMap<>();
    private final List<ReadWait> reads = new ArrayList<>();
    private final Map<Bytes, Long> keyLast = new HashMap<>(), keyApplied = new HashMap<>();
    private final Map<Bytes, ArrayDeque<LogEntry>> queues = new HashMap<>();
    private final Map<Long, CompletableFuture<Void>> inFlight = new HashMap<>();
    private final Set<Long> completed = new HashSet<>();
    private final IoniaHistory history = new IoniaHistory();
    private final Map<NodeId, Long> followerContact = new HashMap<>();
    private final Set<NodeId> viewVotes = new HashSet<>();
    private final Map<NodeId, IoniaRecovery.State> viewStates = new HashMap<>();
    private final Map<NodeId, IoniaMessage.RecoveryResponse> recoveryResponses = new HashMap<>();
    private Status status;
    private long view, lastNormalView, announcedCommit, scheduledIndex, generation;
    private long lastLeaderContact, lastViewProgress, lastTrim, deferredView;
    private NodeId leader;
    private boolean flushQueued, dvcSent, installing;
    private long installCommit;
    private Runnable installed;
    private UUID recoveryNonce;
    private IoniaMessage.StartView latestStart;
    private volatile Progress progress;
    private volatile ViewInfo observedView;
    private volatile String failureReason;

    /** Fresh, fixed-view simulation compatibility constructor; use the stable-store overload in production. */
    public IoniaEngine(IoniaConfig config, CacheEngine localKv, Mailbox owner, Transport transport, TimeSource time) {
        this(config, localKv, owner, transport, time, IoniaViewStore.simulation(), false, config.view(), Long.MAX_VALUE);
    }

    /** Persisted view is loaded by assembly; recovering=true excludes this replica from every normal quorum. */
    public IoniaEngine(IoniaConfig config, CacheEngine localKv, Mailbox owner, Transport transport, TimeSource time,
                       IoniaViewStore stableViews, boolean recovering, long persistedView, long failureTimeoutNanos) {
        this.config = Objects.requireNonNull(config);
        this.localKv = Objects.requireNonNull(localKv);
        this.owner = Objects.requireNonNull(owner);
        this.transport = Objects.requireNonNull(transport);
        this.time = Objects.requireNonNull(time);
        this.stableViews = Objects.requireNonNull(stableViews);
        if (persistedView < config.view() || failureTimeoutNanos <= 0) throw new IllegalArgumentException("invalid view/timeout");
        this.failureTimeoutNanos = failureTimeoutNanos;
        view = persistedView;
        lastNormalView = config.view();
        leader = config.leaderFor(view);
        status = recovering ? Status.RECOVERING : Status.NORMAL;
        lastLeaderContact = lastViewProgress = lastTrim = time.monotonicNanos();
        config.replicas().forEach(node -> matched.put(node, 0L));
        if (!recovering) lastNormalView = view;
        publish();
    }

    public Progress progress() { return progress; }
    public ViewInfo viewInfo() { ViewInfo current = observedView; return closed.get() ? new ViewInfo(current.view(), current.leaderId(), Status.RECOVERING) : current; }
    public Optional<String> failureReason() { return Optional.ofNullable(failureReason); }
    private boolean isLeader() { return config.nodeId().equals(leader); }
    private boolean normal() { return !closed.get() && status == Status.NORMAL && !installing; }

    public CompletionStage<IoniaClientTransport.WriteAck> recordWrite(IoniaClientTransport.Request request) {
        Objects.requireNonNull(request);
        RequestContext context = request.contextAt(time);
        CompletableFuture<IoniaClientTransport.WriteAck> future = new CompletableFuture<>();
        recordCalls.put(future, new RecordCall(request, context.deadline()));
        future.whenComplete((reply, error) -> recordCalls.remove(future));
        if (closed.get()) future.complete(ack(request, ResultStatus.UNAVAILABLE));
        else if (!offer(() -> {
            if (!normal()) future.complete(ack(request, ResultStatus.UNAVAILABLE));
            else if (context.deadline().isExpired(time)) future.complete(ack(request, ResultStatus.TIMEOUT));
            else if (request.view() != view) future.complete(ack(request, ResultStatus.NOT_PRIMARY));
            else if (!(request.command() instanceof Command.Set) || !supported(context))
                future.complete(ack(request, ResultStatus.BAD_REQUEST));
            else {
                try { log.record(request.clientId(), request.operationId(), request.command()); }
                catch (IllegalArgumentException invalid) { future.complete(ack(request, ResultStatus.BAD_REQUEST)); return; }
                future.complete(ack(request, ResultStatus.OK));
                if (isLeader()) order(request.command(), context);
            }
        })) future.complete(ack(request, ResultStatus.BUSY));
        return future;
    }

    public CompletionStage<IoniaClientTransport.Reply> execute(IoniaClientTransport.Request request) {
        return executeChecked(request).thenApply(result -> new IoniaClientTransport.Reply(config.nodeId(), request.view(),
                request.clientId(), request.operationId(), result));
    }

    private CompletionStage<Result> executeChecked(IoniaClientTransport.Request request) {
        RequestContext context = request.contextAt(time);
        CompletableFuture<Result> future = register(context);
        if (!offer(() -> {
            if (request.view() != view) future.complete(resultOf(ResultStatus.NOT_PRIMARY));
            else executeOwner(request.command(), context, future);
        })) future.complete(resultOf(ResultStatus.BUSY));
        return future;
    }

    @Override public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        CompletableFuture<Result> future = register(context);
        if (!offer(() -> executeOwner(command, context, future))) future.complete(resultOf(ResultStatus.BUSY));
        return future;
    }

    private CompletableFuture<Result> register(RequestContext context) {
        CompletableFuture<Result> future = new CompletableFuture<>();
        pending.put(future, context.deadline());
        future.whenComplete((result, error) -> pending.remove(future));
        if (closed.get()) future.complete(resultOf(ResultStatus.UNAVAILABLE));
        return future;
    }

    private void executeOwner(Command command, RequestContext context, CompletableFuture<Result> future) {
        if (future.isDone()) return;
        if (!normal()) future.complete(resultOf(ResultStatus.UNAVAILABLE));
        else if (context.deadline().isExpired(time)) future.complete(resultOf(ResultStatus.TIMEOUT));
        else if (!supported(context)) future.complete(resultOf(ResultStatus.BAD_REQUEST));
        else if (!isLeader()) future.complete(resultOf(ResultStatus.NOT_PRIMARY));
        else if (command instanceof Command.Get get) {
            reads.add(new ReadWait(get, new Waiting(context, future), keyLast.getOrDefault(get.key(), 0L), view));
            flush();
            drainReads();
        } else {
            try { log.record(context.clientId(), context.operationId(), command); }
            catch (IllegalArgumentException invalid) { future.complete(resultOf(ResultStatus.BAD_REQUEST)); return; }
            RequestId id = new RequestId(context.clientId(), context.operationId());
            Result existing = appliedResults.get(id);
            if (existing != null) { finish(new Waiting(context, future), existing, view); return; }
            writes.computeIfAbsent(id, ignored -> new ArrayList<>()).add(new Waiting(context, future));
            order(command, context);
        }
    }

    private void order(Command command, RequestContext context) {
        LogEntry entry = log.order(context.clientId(), context.operationId(), command);
        keyLast.merge(IoniaCommands.key(command), entry.index(), Math::max);
        history.modified(IoniaCommands.key(command), entry.index());
        matched.put(config.nodeId(), log.lastLogIndex());
        if (!flushQueued) {
            flushQueued = true;
            if (!offer(() -> { flushQueued = false; if (normal() && isLeader()) flush(); })) close();
        }
    }

    /** Capture the contiguous applied prefix BEFORE dispatching the physical read, including MISS. */
    public CompletionStage<IoniaClientTransport.ReadReply> followerRead(IoniaClientTransport.Request request) {
        RequestContext context = request.contextAt(time);
        CompletableFuture<IoniaClientTransport.ReadReply> future = auxiliary(new CompletableFuture<>());
        if (!offer(() -> {
            if (context.deadline().isExpired(time)) { future.complete(readReply(request, 0, resultOf(ResultStatus.TIMEOUT))); return; }
            if (!normal() || view != request.view() || !(request.command() instanceof Command.Get get)
                    || !supported(context)) {
                future.complete(readReply(request, 0, resultOf(ResultStatus.UNAVAILABLE))); return;
            }
            long captured = log.appliedIndex(), atView = view;
            localKv.execute(get, context).whenComplete((result, error) -> offerCompletion(() -> {
                Result checked = error == null && result != null && normal() && view == atView
                        ? result : resultOf(ResultStatus.UNAVAILABLE);
                future.complete(readReply(request, captured, checked));
            }));
        })) future.complete(readReply(request, 0, resultOf(ResultStatus.BUSY)));
        return future;
    }

    public CompletionStage<IoniaClientTransport.MetaReply> metaQuery(IoniaClientTransport.Request request) {
        RequestContext context = request.contextAt(time);
        CompletableFuture<IoniaClientTransport.MetaReply> future = auxiliary(new CompletableFuture<>());
        if (!offer(() -> {
            if (context.deadline().isExpired(time)) { future.complete(metaReply(request, 0, resultOf(ResultStatus.TIMEOUT))); return; }
            if (!normal() || !isLeader() || view != request.view() || !(request.command() instanceof Command.Get get)
                    || !supported(context)) {
                future.complete(metaReply(request, 0, resultOf(ResultStatus.UNAVAILABLE))); return;
            }
            long barrier = keyLast.getOrDefault(get.key(), 0L);
            if (keyApplied.getOrDefault(get.key(), 0L) < barrier) {
                CompletableFuture<Result> data = register(context);
                reads.add(new ReadWait(get, new Waiting(context, data), barrier, view));
                data.whenComplete((result, error) -> future.complete(metaReply(request, 0,
                        error == null && result != null ? result : resultOf(ResultStatus.UNAVAILABLE))));
                flush(); drainReads();
            } else future.complete(metaReply(request, history.index(get.key()), null));
        })) future.complete(metaReply(request, 0, resultOf(ResultStatus.BUSY)));
        return future;
    }

    private <T> CompletableFuture<T> auxiliary(CompletableFuture<T> future) {
        auxiliary.add(future);
        future.whenComplete((result, error) -> auxiliary.remove(future));
        if (closed.get()) future.completeExceptionally(new IllegalStateException("engine closed"));
        return future;
    }
    private IoniaClientTransport.ReadReply readReply(IoniaClientTransport.Request request, long index, Result result) {
        return new IoniaClientTransport.ReadReply(config.nodeId(), request.view(), request.clientId(), request.operationId(), index, result);
    }
    private IoniaClientTransport.MetaReply metaReply(IoniaClientTransport.Request request, long index, Result data) {
        return new IoniaClientTransport.MetaReply(config.nodeId(), request.view(), request.clientId(), request.operationId(), index, data);
    }

    @Override public void onPeerMessage(NodeId from, PeerMessage message) {
        if (closed.get() || !config.replicas().contains(from) || from.equals(config.nodeId()) || !(message instanceof IoniaMessage ionia)) return;
        offer(() -> receive(from, ionia));
    }
    private void receive(NodeId from, IoniaMessage message) {
        if (closed.get()) return;
        switch (message) {
            case IoniaMessage.RecoveryRequest recovery -> {
                if (recovery.view() > view && status != Status.RECOVERING && !installing) transition(recovery.view());
                else if (normal()) send(from, new IoniaMessage.RecoveryResponse(view, recovery.nonce(), isLeader() ? state() : null));
            }
            case IoniaMessage.RecoveryResponse response -> acceptRecovery(from, response);
            case IoniaMessage.StartViewChange change -> {
                if (status == Status.RECOVERING) return;
                if (installing) { deferredView = Math.max(deferredView, change.view()); return; }
                if (change.view() > view) transition(change.view());
                if (change.view() == view && status == Status.VIEW_CHANGE) { viewVotes.add(from); maybeDoViewChange(); }
            }
            case IoniaMessage.DoViewChange change -> {
                if (status == Status.RECOVERING) return;
                if (installing) { deferredView = Math.max(deferredView, change.view()); return; }
                if (change.view() > view) transition(change.view());
                if (change.view() == view && status == Status.VIEW_CHANGE && isLeader()) acceptViewState(from, change.state());
            }
            case IoniaMessage.StartView start -> {
                if (status == Status.RECOVERING || installing || start.view() < view
                        || start.state().lastNormalView() != start.view()
                        || !from.equals(config.leaderFor(start.view())) || (normal() && start.view() == view)) return;
                view = start.view(); leader = from; stableViews.save(view); fence();
                install(start.state(), () -> { lastLeaderContact = time.monotonicNanos(); reportApplied(); });
            }
            default -> {
                if (!normal() || message.view() != view) return;
                switch (message) {
                    case IoniaMessage.Prepare prepare -> {
                        if (isLeader() || !from.equals(leader)) return;
                        try { log.accept(prepare); }
                        catch (IllegalArgumentException invalid) { return; }
                        for (LogEntry entry : prepare.entries()) keyLast.merge(IoniaCommands.key(entry.command()), entry.index(), Math::max);
                        lastLeaderContact = time.monotonicNanos();
                        log.commitThrough(Math.min(announcedCommit, log.lastLogIndex()));
                        send(leader, new IoniaMessage.PrepareAck(view, log.lastLogIndex())); scheduleApplies();
                    }
                    case IoniaMessage.PrepareAck ack -> {
                        if (!isLeader() || ack.lastLogIndex() > log.lastLogIndex()) return;
                        matched.merge(from, ack.lastLogIndex(), Math::max);
                        followerContact.put(from, time.monotonicNanos());
                        List<Long> indices = matched.values().stream().sorted().toList();
                        long commit = indices.get(indices.size() - config.orderingQuorumSize());
                        if (commit > log.commitIndex()) { log.commitThrough(commit); broadcastCommit(); scheduleApplies(); }
                    }
                    case IoniaMessage.Commit commit -> {
                        if (isLeader() || !from.equals(leader)) return;
                        lastLeaderContact = time.monotonicNanos();
                        announcedCommit = Math.max(announcedCommit, commit.commitIndex());
                        log.commitThrough(Math.min(announcedCommit, log.lastLogIndex())); scheduleApplies();
                    }
                    case IoniaMessage.Applied applied -> {
                        if (isLeader() && applied.appliedIndex() <= log.lastLogIndex()) {
                            history.report(from, applied.appliedIndex()); followerContact.put(from, time.monotonicNanos());
                        }
                    }
                    default -> { }
                }
            }
        }
    }

    private void flush() {
        if (!normal() || !isLeader()) return;
        for (NodeId node : config.replicas()) if (!node.equals(config.nodeId())) {
            List<LogEntry> entries = log.entriesFrom(matched.getOrDefault(node, 0L) + 1);
            if (!entries.isEmpty()) send(node, new IoniaMessage.Prepare(view, entries));
        }
    }
    private void broadcastCommit() { broadcast(new IoniaMessage.Commit(view, log.commitIndex())); }
    private void broadcast(IoniaMessage message) {
        for (NodeId node : config.replicas()) if (!node.equals(config.nodeId())) send(node, message);
    }
    private void send(NodeId to, IoniaMessage message) {
        try { transport.send(new PeerEnvelope(config.nodeId(), to, message)).exceptionally(error -> null); }
        catch (RuntimeException ignored) { /* TICK retransmits; a local send never counts as an ACK. */ }
    }

    /** Per-key FIFO execution; completion may be out of order across keys. */
    private void scheduleApplies() {
        while (scheduledIndex < log.commitIndex()) {
            LogEntry entry = log.entry(++scheduledIndex).orElseThrow();
            ArrayDeque<LogEntry> queue = queues.computeIfAbsent(IoniaCommands.key(entry.command()), ignored -> new ArrayDeque<>());
            queue.add(entry);
            if (queue.size() == 1) apply(entry);
        }
        finishInstall();
    }
    private void apply(LogEntry entry) {
        long atGeneration = generation;
        CompletableFuture<Void> done = new CompletableFuture<>();
        inFlight.put(entry.index(), done);
        RequestContext internal = new RequestContext(entry.clientId(), entry.operationId(), RoutingVersion.ZERO,
                OwnershipEpoch.ZERO, new Deadline(time.monotonicNanos() + Long.MAX_VALUE));
        try {
            localKv.execute(entry.command(), internal).whenComplete((result, error) -> {
                done.complete(null);
                offerCompletion(() -> {
                    if (closed.get() || atGeneration != generation) return;
                    inFlight.remove(entry.index());
                    if (error != null || result == null || !(result.status() == ResultStatus.OK
                            || entry.command() instanceof Command.Delete && result.status() == ResultStatus.MISS)) {
                        fail("KV apply failed at index " + entry.index()); return;
                    }
                    completed.add(entry.index());
                    while (completed.remove(log.appliedIndex() + 1)) log.markApplied(log.appliedIndex() + 1);
                    keyApplied.put(IoniaCommands.key(entry.command()), entry.index());
                    RequestId id = new RequestId(entry.clientId(), entry.operationId());
                    appliedResults.put(id, result);
                    List<Waiting> waiters = writes.remove(id);
                    if (waiters != null) waiters.forEach(waiting -> finish(waiting, result, view));
                    ArrayDeque<LogEntry> queue = queues.get(IoniaCommands.key(entry.command()));
                    queue.removeFirst();
                    if (queue.isEmpty()) queues.remove(IoniaCommands.key(entry.command())); else apply(queue.getFirst());
                    drainReads(); reportApplied(); finishInstall();
                });
            });
        } catch (RuntimeException error) { done.complete(null); fail("storage rejected apply: " + error); }
    }
    private void reportApplied() {
        if (normal() && !isLeader()) send(leader, new IoniaMessage.Applied(view, log.appliedIndex()));
    }
    private void drainReads() {
        Iterator<ReadWait> iterator = reads.iterator();
        while (iterator.hasNext()) {
            ReadWait read = iterator.next();
            Waiting waiting = read.waiting();
            if (waiting.future().isDone()) { iterator.remove(); continue; }
            if (!normal() || read.view() != view) { iterator.remove(); finish(waiting, resultOf(ResultStatus.UNAVAILABLE), read.view()); }
            else if (keyApplied.getOrDefault(read.command().key(), 0L) >= read.barrier()) {
                iterator.remove();
                try {
                    localKv.execute(read.command(), waiting.context()).whenComplete((result, error) -> offerCompletion(() ->
                            finish(waiting, error == null && result != null ? result : resultOf(ResultStatus.INTERNAL_ERROR), read.view())));
                } catch (RuntimeException error) { finish(waiting, resultOf(ResultStatus.INTERNAL_ERROR), read.view()); }
            }
        }
    }
    private void finish(Waiting waiting, Result result, long atView) {
        waiting.future().complete(!normal() || view != atView ? resultOf(ResultStatus.UNAVAILABLE)
                : waiting.context().deadline().isExpired(time) ? resultOf(ResultStatus.TIMEOUT) : result);
    }

    public void startViewChange(long targetView) {
        offer(() -> { if (status != Status.RECOVERING && targetView > view) {
            if (installing) deferredView = Math.max(deferredView, targetView); else transition(targetView);
        } });
    }
    private void transition(long targetView) {
        if (targetView <= view) return;
        view = targetView; leader = config.leaderFor(view); stableViews.save(view);
        status = Status.VIEW_CHANGE; fence();
        latestStart = null; viewVotes.clear(); viewStates.clear(); dvcSent = false;
        lastViewProgress = time.monotonicNanos();
        viewVotes.add(config.nodeId()); broadcast(new IoniaMessage.StartViewChange(view)); maybeDoViewChange();
    }
    private void fence() {
        pending.keySet().forEach(future -> future.complete(resultOf(ResultStatus.UNAVAILABLE)));
        recordCalls.forEach((future, call) -> future.complete(ack(call.request(), ResultStatus.UNAVAILABLE)));
        auxiliary.forEach(future -> future.completeExceptionally(new IllegalStateException("view fenced")));
        writes.clear(); reads.clear();
    }
    private void maybeDoViewChange() {
        if (viewVotes.size() < config.orderingQuorumSize() || dvcSent) return;
        dvcSent = true;
        if (isLeader()) acceptViewState(config.nodeId(), state());
        else send(leader, new IoniaMessage.DoViewChange(view, state()));
    }
    private void acceptViewState(NodeId from, IoniaRecovery.State state) {
        if (installing || state.lastNormalView() >= view) return;
        viewStates.putIfAbsent(from, state);
        if (viewStates.size() < config.orderingQuorumSize() || !viewStates.containsKey(config.nodeId())) return;
        Map<NodeId, IoniaRecovery.State> participants = new LinkedHashMap<>();
        participants.put(config.nodeId(), viewStates.get(config.nodeId()));
        for (NodeId node : config.replicas()) {
            if (participants.size() == config.orderingQuorumSize()) break;
            if (!node.equals(config.nodeId()) && viewStates.containsKey(node)) participants.put(node, viewStates.get(node));
        }
        IoniaRecovery.State recovered = IoniaRecovery.recover(config, participants);
        install(recovered, () -> {
            latestStart = new IoniaMessage.StartView(view, state());
            broadcast(latestStart); flush(); broadcastCommit();
        });
    }
    private IoniaRecovery.State state() {
        return new IoniaRecovery.State(lastNormalView, log.entriesFrom(1), log.durabilityEntries(), log.commitIndex());
    }

    /** A restarted node only installs after one nonce's majority includes the highest observed view's leader. */
    public void startRecovery(UUID nonce) {
        Objects.requireNonNull(nonce);
        offer(() -> {
            if (installing) return;
            status = Status.RECOVERING; fence();
            recoveryNonce = nonce; recoveryResponses.clear();
            broadcast(new IoniaMessage.RecoveryRequest(view, nonce));
        });
    }
    private void acceptRecovery(NodeId from, IoniaMessage.RecoveryResponse response) {
        if (status != Status.RECOVERING || installing || recoveryNonce == null || !recoveryNonce.equals(response.nonce())) return;
        if (response.view() < view) return;
        recoveryResponses.put(from, response);
        if (recoveryResponses.size() < config.orderingQuorumSize()) return;
        long highest = recoveryResponses.values().stream().mapToLong(IoniaMessage.RecoveryResponse::view).max().orElseThrow();
        NodeId primary = config.leaderFor(highest);
        IoniaMessage.RecoveryResponse authoritative = recoveryResponses.get(primary);
        if (authoritative == null || authoritative.view() != highest || authoritative.state() == null) return;
        view = highest; leader = primary; stableViews.save(view);
        install(authoritative.state(), () -> { recoveryNonce = null; lastLeaderContact = time.monotonicNanos(); reportApplied(); });
    }

    private void install(IoniaRecovery.State state, Runnable after) {
        if (!(localKv instanceof IoniaStorage storage)) {
            fail("recovery requires IoniaStorage.reset; legacy CacheEngine cannot safely replace KV state"); return;
        }
        installing = true; installCommit = state.commitIndex(); installed = after;
        generation++;
        CompletableFuture<?>[] oldApplies = inFlight.values().toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(oldApplies).thenCompose(ignored -> storage.reset()).whenComplete((ignored, error) -> offerCompletion(() -> {
            if (error != null) { fail("storage reset failed: " + error); return; }
            log = new IoniaLog();
            if (!state.consensus().isEmpty()) log.accept(new IoniaMessage.Prepare(view, state.consensus()));
            state.durability().forEach(entry -> log.record(entry.clientId(), entry.operationId(), entry.command()));
            log.commitThrough(state.commitIndex()); announcedCommit = state.commitIndex();
            scheduledIndex = 0; inFlight.clear(); queues.clear(); completed.clear(); appliedResults.clear();
            keyLast.clear(); keyApplied.clear();
            state.consensus().forEach(entry -> keyLast.put(IoniaCommands.key(entry.command()), entry.index()));
            history.rebuild(state.consensus()); followerContact.clear();
            config.replicas().forEach(node -> matched.put(node, node.equals(config.nodeId()) ? log.lastLogIndex() : 0L));
            scheduleApplies();
        }));
    }
    private void finishInstall() {
        if (!installing || log.appliedIndex() < installCommit) return;
        installing = false;
        if (deferredView > view) { long target = deferredView; deferredView = 0; transition(target); return; }
        status = Status.NORMAL; lastNormalView = view;
        lastLeaderContact = lastViewProgress = time.monotonicNanos();
        Runnable callback = installed; installed = null;
        if (callback != null) callback.run();
    }

    @Override public void onTimer(TimerEvent event) {
        if (!TICK.equals(event.name()) || closed.get()) return;
        pending.forEach((future, deadline) -> { if (deadline.isExpired(time)) future.complete(resultOf(ResultStatus.TIMEOUT)); });
        recordCalls.forEach((future, call) -> { if (call.deadline().isExpired(time)) future.complete(ack(call.request(), ResultStatus.TIMEOUT)); });
        offer(() -> {
            if (closed.get() || installing) return;
            long now = time.monotonicNanos();
            if (status == Status.RECOVERING) {
                if (recoveryNonce != null) broadcast(new IoniaMessage.RecoveryRequest(view, recoveryNonce));
            } else if (status == Status.VIEW_CHANGE) {
                if (now - lastViewProgress >= failureTimeoutNanos) transition(Math.incrementExact(view));
                else { broadcast(new IoniaMessage.StartViewChange(view));
                    if (dvcSent && !isLeader()) send(leader, new IoniaMessage.DoViewChange(view, state())); }
            } else if (!isLeader() && now - lastLeaderContact >= failureTimeoutNanos) transition(Math.incrementExact(view));
            else if (isLeader()) {
                if (latestStart != null) for (NodeId node : config.replicas())
                    if (!node.equals(config.nodeId()) && matched.getOrDefault(node, 0L) == 0) send(node, latestStart);
                flush(); broadcastCommit();
                if (now - lastTrim >= 1_000_000_000L) {
                    Set<NodeId> active = new HashSet<>();
                    followerContact.forEach((node, seen) -> { if (now - seen < failureTimeoutNanos) active.add(node); });
                    history.trim(active, log.appliedIndex()); lastTrim = now;
                }
            } else reportApplied();
            writes.values().forEach(waiters -> waiters.removeIf(waiting -> waiting.future().isDone()));
            writes.values().removeIf(List::isEmpty); drainReads();
        });
    }
    @Override public EngineSnapshot snapshot() {
        return new EngineSnapshot(config.nodeId(), time.monotonicNanos(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }
    @Override public void close() {
        closed.set(true);
        pending.keySet().forEach(future -> future.complete(resultOf(ResultStatus.UNAVAILABLE)));
        recordCalls.forEach((future, call) -> future.complete(ack(call.request(), ResultStatus.UNAVAILABLE)));
        auxiliary.forEach(future -> future.completeExceptionally(new IllegalStateException("engine closed")));
    }
    private void fail(String reason) { failureReason = reason; close(); }
    private boolean offer(Runnable task) {
        return owner.offer(() -> {
            try { task.run(); }
            catch (RuntimeException error) { fail(error.toString()); }
            finally { publish(); }
        });
    }
    private void offerCompletion(Runnable task) { if (!offer(task)) fail("completion mailbox rejected"); }
    private void publish() {
        progress = new Progress(config.nodeId(), log.recordedCount(), log.lastLogIndex(), log.commitIndex(), log.appliedIndex());
        observedView = new ViewInfo(view, leader, closed.get() || installing ? Status.RECOVERING : status);
    }
    private IoniaClientTransport.WriteAck ack(IoniaClientTransport.Request request, ResultStatus result) {
        return new IoniaClientTransport.WriteAck(config.nodeId(), request.view(), request.clientId(), request.operationId(), result);
    }
    private static boolean supported(RequestContext context) {
        return RoutingVersion.ZERO.equals(context.routingVersion()) && OwnershipEpoch.ZERO.equals(context.ownershipEpoch());
    }
    private static Result resultOf(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
}
