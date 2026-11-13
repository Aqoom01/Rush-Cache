package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.ionia.IoniaTcpTransport;
import io.openkache.server.ionia_eighth.IoniaMetaBatchTcpTransport;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Gateway와 실제 replica 그룹 사이의 adapter. A2 Fast ACK를 적용 완료로 바꾸지 않는다. */
interface ReplicatedBackend extends AutoCloseable {
    Result execute(Command command, long client, long operation, long deadline) throws IOException, InterruptedException;
    default Result administrative(Command command) throws IOException, InterruptedException { throw new UnsupportedOperationException(); }
    default Result administrative(Command command, long deadline) throws IOException, InterruptedException { return administrative(command); }
    boolean supportsDelete();
    /** 실제 replica CPU 누계 차분. 누락/첫 표본은 값 없음이며 모형 값으로 대체하지 않는다. */
    default OptionalDouble cpu() { return OptionalDouble.empty(); }
    /** 신뢰하는 실행기가 소유 JVM의 영구 종료를 확인한 뒤 호출한다. FD 관측만으로는 master 승격하지 않는다. */
    default void confirmedCrash(int replica) throws IOException, InterruptedException { }
    /** Single gateway controller publishes the B1 snapshot through A1 before exposing it. */
    default void publishRouting(io.openkache.core.tablet.RoutingTable before, io.openkache.core.tablet.RoutingTable after) throws IOException, InterruptedException { }
    default String compositionStatus() { return "null"; }
    static ReplicatedBackend connect(IntegrationConfig config, int group) {
        return config.composed() ? new JointBackend(config, group) : new Native(config, group);
    }

    @Override void close();

    final class Native implements ReplicatedBackend {
        private final IntegrationConfig config;
        private final int group;
        private final long namespace;
        private final IoniaClientTransport transport;
        private final AutoCloseable transportClose;
        private final Map<Long, IoniaProfile.Session> sessions = new ConcurrentHashMap<>();
        private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        private final AtomicLong adminOperation = new AtomicLong();
        private final AtomicInteger primary = new AtomicInteger();
        private final AtomicInteger readNode;
        private final long[] cpuPrevious = {-1, -1, -1}, pidPrevious = {-1, -1, -1};
        private long cpuAt;
        private final AtomicBoolean repaired = new AtomicBoolean();
        private final AtomicLongArray epochs = new AtomicLongArray(3);
        private final List<InetSocketAddress> addresses;
        Native(IntegrationConfig config, int group) {
            this.config = config; this.group = group; readNode = new AtomicInteger(config.protocol().equals("CHAIN") ? 2 : config.protocol().equals("CRAQ") ? 1 : 0); addresses = List.copyOf(config.addresses(group, 0).values());
            try { namespace = (ByteBuffer.wrap(MessageDigest.getInstance("SHA-256").digest(config.runId().getBytes(java.nio.charset.StandardCharsets.UTF_8))).getLong() & ((1L << 40) - 1)) << 20; }
            catch (NoSuchAlgorithmException e) { throw new AssertionError(e); }
            if (config.ionia()) {
                int timeout = config.number("request.timeout.ms", 3000);
                if (config.protocol().equals("V07")) {
                    var t = new IoniaMetaBatchTcpTransport(new NodeId(0), IoniaProfile.configuration(config, group), config.addresses(group, 0), timeout);
                    transport = t; transportClose = t;
                } else {
                    var t = new IoniaTcpTransport(new NodeId(0), IoniaProfile.configuration(config, group), config.addresses(group, 0), timeout);
                    transport = t; transportClose = t;
                }
                timer.scheduleAtFixedRate(() -> sessions.values().forEach(s -> s.timer().accept(new TimerEvent(IoniaEngine.TICK, System.nanoTime()))), 5, 5, TimeUnit.MILLISECONDS);
            } else { transport = null; transportClose = null; }
        }
        @Override public boolean supportsDelete() { return !config.protocol().startsWith("PB"); }
        @Override public Result execute(Command command, long client, long operation, long deadline) throws IOException, InterruptedException {
            if (command instanceof Command.Delete && !supportsDelete()) return status(ResultStatus.NOT_IMPLEMENTED);
            if (client < 0 || client > 1048575) return status(ResultStatus.BAD_REQUEST);
            if (!config.ionia()) return a1(command, namespace + client, operation, deadline);
            IoniaProfile.Session session = sessions.get(client);
            if (session == null) {
                synchronized (sessions) {
                    session = sessions.get(client);
                    if (session == null) {
                        if (sessions.size() >= 1024) return status(ResultStatus.CAPACITY_EXCEEDED);
                        session = IoniaProfile.session(config, group, new ClientId(namespace + client), transport); sessions.put(client, session);
                    }
                }
            }
            RequestContext context = new RequestContext(new ClientId(namespace + client), new OperationId(operation), RoutingVersion.ZERO, OwnershipEpoch.ZERO, new Deadline(deadline));
            for (int attempt = 0; attempt < 3 && System.nanoTime() < deadline; attempt++) {
                Result result = await(session.execute().apply(command, context), deadline);
                if (result.status() != ResultStatus.NOT_PRIMARY && result.status() != ResultStatus.UNAVAILABLE) return result;
                try { await(session.refresh().get(), deadline); }
                catch (IOException e) { if (System.nanoTime() >= deadline) return status(ResultStatus.TIMEOUT); Thread.sleep(10); }
            }
            return status(ResultStatus.UNAVAILABLE);
        }
        private Result a1(Command c, long client, long operation, long deadline) throws InterruptedException {
            for (int attempt = 0; attempt < 9 && System.nanoTime() < deadline; attempt++) {
                AtomicInteger target = c instanceof Command.Get && (config.protocol().equals("CHAIN") || config.protocol().equals("CRAQ")) ? readNode : primary;
                int node = target.get();
                try {
                    int left = (int) Math.max(1, Math.min(config.number("request.timeout.ms", 3000), TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
                    Result r = IntegrationWire.call(addresses.get(node), c, client, operation, epochs.get(node), left);
                    epochs.accumulateAndGet(node, r.ownershipEpoch().value(), Math::max);
                    if (r.status() == ResultStatus.STALE_EPOCH) continue;
                    if (r.status() == ResultStatus.NOT_PRIMARY) { target.set(r.redirect().map(x -> (int) x.target().value()).filter(x -> x >= 0 && x < 3).orElse((node + 1) % 3)); continue; }
                    if (r.status() != ResultStatus.UNAVAILABLE) return r;
                } catch (IOException e) {
                    // PB의 모호한 쓰기는 자동 재실행하지 않는다. GET과 dedup 엔진만 같은 ID로 다시 시도한다.
                    if (config.protocol().startsWith("PB") && !(c instanceof Command.Get)) {
                        // Keep this write ambiguous, but let the NEXT operation discover
                        // the successor. The joint authority admits via SET, so a failed
                        // primary cannot otherwise be discovered by a later GET.
                        target.compareAndSet(node, (node + 1) % 3);
                        return status(ResultStatus.UNAVAILABLE);
                    }
                }
                target.compareAndSet(node, (node + 1) % 3); Thread.sleep(10);
            }
            return status(System.nanoTime() >= deadline ? ResultStatus.TIMEOUT : ResultStatus.UNAVAILABLE);
        }
        @Override public Result administrative(Command command) throws IOException, InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.number("request.timeout.ms", 3000));
            return administrative(command, deadline);
        }
        @Override public Result administrative(Command command, long deadline) throws IOException, InterruptedException {
            long operation = adminOperation.incrementAndGet();
            while (true) {
                Result result = execute(command, 1048575, operation, deadline);
                // A1은 적용 중 다른 요청에 BUSY를 반환한다. 원래 client와 같은 ID로 재시도한다.
                // A2 쓰기의 BUSY는 미기록을 보장하지 않으므로 bootstrap 실패로 보존한다.
                if (result.status() != ResultStatus.BUSY || config.ionia() && !(command instanceof Command.Get) || System.nanoTime() >= deadline) return result;
                Thread.sleep(1);
            }
        }
        private static <T> T await(CompletionStage<T> stage, long deadline) throws IOException, InterruptedException {
            try { return stage.toCompletableFuture().get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
            catch (ExecutionException | TimeoutException e) { throw new IOException("native RPC failed/expired", e); }
        }
        @Override public synchronized OptionalDouble cpu() {
            long now = System.nanoTime(); double used = 0; boolean valid = cpuAt != 0;
            for (int n = 0; n < 3; n++) {
                try {
                    String status = ReplicaDiagnostics.request(config, group, n, null);
                    long cpu = ReplicaDiagnostics.number(status, "processCpuNanos"), pid = ReplicaDiagnostics.number(status, "pid");
                    int processors = (int) ReplicaDiagnostics.number(status, "processors");
                    if (cpu < 0 || processors < 1 || cpuPrevious[n] < 0 || cpu < cpuPrevious[n] || pid != pidPrevious[n]) valid = false;
                    else used += (double) (cpu - cpuPrevious[n]) / processors;
                    cpuPrevious[n] = cpu; pidPrevious[n] = pid;
                } catch (IOException | RuntimeException failure) { valid = false; cpuPrevious[n] = -1; }
            }
            long elapsed = now - cpuAt; cpuAt = now;
            return valid && elapsed > 0 ? OptionalDouble.of(Math.max(0, Math.min(1, used / elapsed / 3))) : OptionalDouble.empty();
        }
        @Override public void confirmedCrash(int failed) throws IOException, InterruptedException {
            if (failed < 0 || failed >= 3) throw new IllegalArgumentException("replica outside group");
            if (!config.masterRequired()) return;
            if (!repaired.compareAndSet(false, true)) throw new IOException("Chain/Rose master supports one permanent crash per group");
            try { ReplicaDiagnostics.request(config, group, failed, null); throw new IOException("confirmed-crash target still responds"); }
            catch (java.net.ConnectException | java.net.SocketTimeoutException expected) { /* 외부 실행기의 종료 확인에 더해 live endpoint도 거절한다. */ }
            List<Integer> survivors = java.util.stream.IntStream.range(0, 3).filter(i -> i != failed).boxed().toList();
            long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.number("failure.grace.ms", 3000) + config.number("fd.repair.timeout.ms", FailureMethods.legacy(config.failureMethod()) ? config.number("failure.timeout.ms", 1000) * 15 : 60000));
            if (!config.externalFd()) Thread.sleep(config.number("failure.timeout.ms", 1000));
            else {
                while (true) {
                    int dead = 0;
                    for (int n : survivors) {
                        String status = ReplicaDiagnostics.request(config, group, n, null);
                        var matcher = java.util.regex.Pattern.compile("\\\"deadMembers\\\":\\[([^]]*)\\]").matcher(status);
                        if (matcher.find() && Arrays.stream(matcher.group(1).split(",")).anyMatch(x -> x.trim().equals("" + failed))) dead++;
                    }
                    if (dead == 2) break;
                    if (System.nanoTime() > until) throw new IOException("no matching A3 DEAD evidence on both survivors"); Thread.sleep(20);
                }
            }
            String first = ReplicaDiagnostics.request(config, group, survivors.getFirst(), null);
            long epoch = ReplicaDiagnostics.number(first, "epoch") + 1;
            String nodes = survivors.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(","));
            var lengths = new HashMap<Integer, Integer>();
            for (int n : survivors) lengths.put(n, (int) ReplicaDiagnostics.number(ReplicaDiagnostics.request(config, group, n, Map.of("op", "freeze", "epoch", "" + epoch, "nodes", nodes)), "length"));
            int leader = config.protocol().equals("ROSE") ? survivors.stream().max(Comparator.<Integer>comparingInt(lengths::get).thenComparingInt(i -> -i)).orElseThrow() : survivors.getFirst();
            int length = lengths.values().stream().mapToInt(Integer::intValue).max().orElseThrow();
            for (int n : survivors) ReplicaDiagnostics.request(config, group, n, Map.of("op", "repair", "epoch", "" + epoch, "nodes", nodes, "primary", "" + leader));
            while (true) {
                boolean ready = true;
                for (int n : survivors) {
                    String status = ReplicaDiagnostics.request(config, group, n, null);
                    ready &= ReplicaDiagnostics.number(status, "length") == length && ReplicaDiagnostics.number(status, "applied") == length;
                    if (!config.protocol().equals("ROSE")) ready &= ReplicaDiagnostics.number(status, "acknowledged") == length;
                    else if (n == leader) ready &= ReplicaDiagnostics.number(status, "minAck") == length;
                }
                if (ready) break;
                if (System.nanoTime() > until) throw new IOException("master prefix/apply/ACK repair barrier timeout"); Thread.sleep(10);
            }
            // 쓰기 입구(head/primary)를 마지막에 연다. 먼저 열면 cleanup/새 쓰기가 아직 FROZEN인 peer의 길이를 바꿀 수 있다.
            List<Integer> openOrder = new ArrayList<>(survivors.stream().filter(n -> n != leader).toList()); openOrder.add(leader);
            for (int n : openOrder) ReplicaDiagnostics.request(config, group, n, Map.of("op", "open", "epoch", "" + epoch, "length", "" + length));
            primary.set(leader); readNode.set(config.protocol().equals("CHAIN") ? survivors.getLast() : config.protocol().equals("CRAQ") ? survivors.getFirst() : leader);
            for (int n : survivors) epochs.set(n, epoch);
        }
        @Override public void close() {
            timer.shutdownNow(); sessions.values().forEach(IoniaProfile.Session::close);
            if (transportClose != null) try { transportClose.close(); } catch (Exception e) { throw new IllegalStateException(e); }
        }
    }
    static Result status(ResultStatus s) { return Result.of(s, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
}
