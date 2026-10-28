package io.openkache.core.replication.ionia;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Nonblocking IONIA client. Same logical identity is retained across fast/slow paths and retries.
 * The default constructor certifies reads; disabling certificates is only for fixed-view simulations.
 * View probes run in parallel with reads to fence an isolated old primary without a clock lease.
 */
public final class IoniaClient implements AutoCloseable {
    public record Options(NodeId readReplica, boolean certifyReads, long fallbackAfterNanos) {
        public Options { Objects.requireNonNull(readReplica); if (fallbackAfterNanos < 0) throw new IllegalArgumentException("negative fallback delay"); }
    }
    private record Endpoint(long view, NodeId leader) { }
    private record FastWrite(Command.Set command, RequestContext context, long started, Endpoint endpoint, AtomicBoolean fallback) { }
    private final ClientId clientId;
    private final List<NodeId> replicas;
    private final IoniaConfig membership;
    private final IoniaClientTransport transport;
    private final TimeSource time;
    private final Options options;
    private final Map<CompletableFuture<Result>, Deadline> pending = new ConcurrentHashMap<>();
    private final Map<CompletableFuture<Result>, FastWrite> fastWrites = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Endpoint endpoint;

    public IoniaClient(ClientId clientId, List<NodeId> replicas, NodeId leaderId, long view,
                       IoniaClientTransport transport, TimeSource time) {
        this(clientId, replicas, leaderId, view, transport, time, new Options(leaderId, true, 10_000_000L));
    }
    /** For real clusters use a follower readReplica and certifyReads=true. Membership's base view is zero. */
    public IoniaClient(ClientId clientId, List<NodeId> replicas, NodeId leaderId, long view,
                       IoniaClientTransport transport, TimeSource time, Options options) {
        this.clientId = Objects.requireNonNull(clientId);
        this.replicas = List.copyOf(replicas);
        membership = new IoniaConfig(leaderId, leaderId, replicas, view);
        this.transport = Objects.requireNonNull(transport);
        this.time = Objects.requireNonNull(time);
        this.options = Objects.requireNonNull(options);
        if (!replicas.contains(options.readReplica())) throw new IllegalArgumentException("read replica is not a member");
        endpoint = new Endpoint(view, leaderId);
    }
    public synchronized void updateView(long view, NodeId leader) {
        if (view < endpoint.view() || !replicas.contains(leader)) throw new IllegalArgumentException("invalid endpoint");
        if (view == endpoint.view() && !leader.equals(endpoint.leader())) throw new IllegalArgumentException("two leaders in one view");
        endpoint = new Endpoint(view, leader);
    }

    /** Best-effort discovery only chooses a destination; writes still need ACK quorums and reads need view certificates. */
    public CompletionStage<IoniaEngine.ViewInfo> refreshView() {
        List<CompletableFuture<IoniaEngine.ViewInfo>> observations = replicas.stream().map(node -> {
            try { return transport.probe(node).exceptionally(error -> null).toCompletableFuture(); }
            catch (RuntimeException error) { return CompletableFuture.<IoniaEngine.ViewInfo>completedFuture(null); }
        }).toList();
        return CompletableFuture.allOf(observations.toArray(CompletableFuture<?>[]::new)).thenApply(ignored -> {
            IoniaEngine.ViewInfo newest = observations.stream().map(CompletableFuture::join).filter(Objects::nonNull)
                    .filter(info -> info.status() == IoniaEngine.Status.NORMAL)
                    .filter(info -> membership.leaderFor(info.view()).equals(info.leaderId()))
                    .max(Comparator.comparingLong(IoniaEngine.ViewInfo::view)).orElseThrow(() -> new IllegalStateException("no normal primary"));
            updateView(newest.view(), newest.leaderId());
            return newest;
        });
    }

    public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        CompletableFuture<Result> result = register(context);
        if (result.isDone()) return result;
        Endpoint target = endpoint;
        switch (command) {
            case Command.Set set -> set(set, context, target, result);
            case Command.Delete delete -> atLeader(delete, context, target, result);
            case Command.Get get -> {
                CompletableFuture<Result> data = new CompletableFuture<>();
                if (options.readReplica().equals(target.leader())) atLeader(get, context, target, data);
                else follower(get, context, target, options.readReplica(), data);
                finishCertifiedRead(data, context, target, result);
            }
        }
        return result;
    }
    public CompletionStage<Result> readAtFollower(NodeId follower, Command.Get get, RequestContext context) {
        if (!replicas.contains(follower)) throw new IllegalArgumentException("not a replica");
        CompletableFuture<Result> result = register(context);
        if (!result.isDone()) {
            Endpoint target = endpoint;
            CompletableFuture<Result> data = new CompletableFuture<>();
            follower(get, context, target, follower, data);
            finishCertifiedRead(data, context, target, result);
        }
        return result;
    }
    private CompletableFuture<Result> register(RequestContext context) {
        CompletableFuture<Result> future = new CompletableFuture<>();
        if (closed.get()) { future.complete(resultOf(ResultStatus.UNAVAILABLE)); return future; }
        if (!clientId.equals(context.clientId())) { future.complete(resultOf(ResultStatus.BAD_REQUEST)); return future; }
        if (context.deadline().isExpired(time)) { future.complete(resultOf(ResultStatus.TIMEOUT)); return future; }
        pending.put(future, context.deadline());
        future.whenComplete((result, error) -> { pending.remove(future); fastWrites.remove(future); });
        if (closed.get()) future.complete(resultOf(ResultStatus.UNAVAILABLE));
        return future;
    }
    private void set(Command.Set command, RequestContext context, Endpoint target, CompletableFuture<Result> result) {
        Set<NodeId> acks = ConcurrentHashMap.newKeySet();
        AtomicInteger replies = new AtomicInteger();
        FastWrite state = new FastWrite(command, context, time.monotonicNanos(), target, new AtomicBoolean());
        fastWrites.put(result, state);
        for (NodeId node : replicas) {
            try {
                transport.record(node, request(command, context, target)).whenComplete((ack, error) -> {
                    if (result.isDone()) return;
                    int received = replies.incrementAndGet();
                    if (context.deadline().isExpired(time)) result.complete(resultOf(ResultStatus.TIMEOUT));
                    else if (ack != null && !identity(node, target, context, ack.nodeId(), ack.view(), ack.clientId(), ack.operationId()))
                        result.complete(resultOf(ResultStatus.BAD_REQUEST));
                    else if (ack != null && ack.status() == ResultStatus.BAD_REQUEST) result.complete(resultOf(ResultStatus.BAD_REQUEST));
                    else {
                        if (error == null && ack != null && ack.status() == ResultStatus.OK) acks.add(node);
                        if (acks.size() >= membership.fastWriteQuorumSize() && acks.contains(target.leader())) result.complete(resultOf(ResultStatus.OK));
                        else if (acks.size() + replicas.size() - received < membership.fastWriteQuorumSize()
                                || node.equals(target.leader()) && !acks.contains(node)) fallback(state, result);
                    }
                });
            } catch (RuntimeException error) { replies.incrementAndGet(); fallback(state, result); }
        }
    }
    private void fallback(FastWrite state, CompletableFuture<Result> result) {
        if (!result.isDone() && !state.context().deadline().isExpired(time) && state.fallback().compareAndSet(false, true))
            atLeader(state.command(), state.context(), state.endpoint(), result);
    }
    private void atLeader(Command command, RequestContext context, Endpoint target, CompletableFuture<Result> result) {
        try {
            transport.execute(target.leader(), request(command, context, target)).whenComplete((reply, error) -> {
                if (context.deadline().isExpired(time)) result.complete(resultOf(ResultStatus.TIMEOUT));
                else if (error != null || reply == null) result.complete(resultOf(ResultStatus.UNAVAILABLE));
                else if (!identity(target.leader(), target, context, reply.nodeId(), reply.view(), reply.clientId(), reply.operationId()))
                    result.complete(resultOf(ResultStatus.BAD_REQUEST));
                else result.complete(reply.result());
            });
        } catch (RuntimeException error) { result.complete(resultOf(ResultStatus.UNAVAILABLE)); }
    }
    private void follower(Command.Get get, RequestContext context, Endpoint target, NodeId node, CompletableFuture<Result> result) {
        CompletableFuture<IoniaClientTransport.ReadReply> read;
        try { read = transport.followerRead(node, request(get, context, target)).toCompletableFuture(); }
        catch (RuntimeException error) { read = CompletableFuture.failedFuture(error); }
        CompletableFuture<IoniaClientTransport.ReadReply> follower = read;
        try {
            transport.metaQuery(target.leader(), request(get, context, target)).whenComplete((meta, error) -> {
                if (error != null || meta == null) { result.complete(resultOf(ResultStatus.UNAVAILABLE)); return; }
                if (!identity(target.leader(), target, context, meta.nodeId(), meta.view(), meta.clientId(), meta.operationId())) {
                    result.complete(resultOf(ResultStatus.UNAVAILABLE)); return;
                }
                if (meta.data() != null) { result.complete(meta.data()); return; }
                follower.whenComplete((reply, readError) -> {
                    if (readError == null && reply != null
                            && identity(node, target, context, reply.nodeId(), reply.view(), reply.clientId(), reply.operationId())
                            && (reply.result().status() == ResultStatus.OK || reply.result().status() == ResultStatus.MISS)
                            && reply.appliedIndex() >= meta.index()) result.complete(reply.result());
                    else atLeader(get, context, target, result);
                });
            });
        } catch (RuntimeException error) { result.complete(resultOf(ResultStatus.UNAVAILABLE)); }
    }
    private void finishCertifiedRead(CompletableFuture<Result> data, RequestContext context, Endpoint target, CompletableFuture<Result> result) {
        CompletableFuture<Boolean> certificate = options.certifyReads() ? certify(target) : CompletableFuture.completedFuture(true);
        data.thenCombine(certificate, (value, valid) -> valid && endpoint.equals(target) ? value : resultOf(ResultStatus.UNAVAILABLE))
                .whenComplete((value, error) -> result.complete(context.deadline().isExpired(time) ? resultOf(ResultStatus.TIMEOUT)
                        : error == null && value != null ? value : resultOf(ResultStatus.UNAVAILABLE)));
    }
    private CompletableFuture<Boolean> certify(Endpoint target) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        Set<NodeId> confirmations = ConcurrentHashMap.newKeySet();
        AtomicInteger finished = new AtomicInteger();
        for (NodeId node : replicas) {
            try {
                transport.probe(node).whenComplete((info, error) -> {
                    if (error == null && info != null && info.status() == IoniaEngine.Status.NORMAL
                            && info.view() == target.view() && info.leaderId().equals(target.leader())) confirmations.add(node);
                    if (confirmations.size() >= membership.orderingQuorumSize()) result.complete(true);
                    if (finished.incrementAndGet() == replicas.size()) result.complete(false);
                });
            } catch (RuntimeException error) { if (finished.incrementAndGet() == replicas.size()) result.complete(false); }
        }
        return result;
    }
    private boolean identity(NodeId expected, Endpoint target, RequestContext context, NodeId node, long view, ClientId client, OperationId operation) {
        return expected.equals(node) && target.view() == view && clientId.equals(client) && context.operationId().equals(operation);
    }
    private IoniaClientTransport.Request request(Command command, RequestContext context, Endpoint target) {
        return new IoniaClientTransport.Request(target.view(), command, clientId, context.operationId(), context.routingVersion(),
                context.ownershipEpoch(), Math.max(0, context.deadline().monotonicNanos() - time.monotonicNanos()));
    }
    public void onTimer(TimerEvent event) {
        Objects.requireNonNull(event);
        pending.forEach((future, deadline) -> { if (deadline.isExpired(time)) future.complete(resultOf(ResultStatus.TIMEOUT)); });
        fastWrites.forEach((future, state) -> { if (time.monotonicNanos() - state.started() >= options.fallbackAfterNanos()) fallback(state, future); });
    }
    @Override public void close() {
        closed.set(true); pending.keySet().forEach(future -> future.complete(resultOf(ResultStatus.UNAVAILABLE)));
    }
    private static Result resultOf(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
}
