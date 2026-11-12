package io.openkache.server.ionia_eighth;


import io.openkache.core.replication.ionia_eighth.*;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** O00 command-line client. Discovery and retries retain one logical request ID and one total deadline. */
public final class IoniaMetaBatchClientMain {
    private IoniaMetaBatchClientMain() { }
    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) throw new IllegalArgumentException("usage: IoniaMetaBatchClientMain node.properties get|set|delete key [value]");
        Command command = switch (args[1]) {
            case "get" -> new Command.Get(Bytes.utf8(args[2]));
            case "delete" -> new Command.Delete(Bytes.utf8(args[2]));
            case "set" -> { if (args.length != 4) throw new IllegalArgumentException("set needs a value"); yield new Command.Set(Bytes.utf8(args[2]), Bytes.utf8(args[3])); }
            default -> throw new IllegalArgumentException("unknown operation");
        };
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(Path.of(args[0]))) { properties.load(reader); }
        Set<String> allowed=new HashSet<>(IoniaMetaBatchSettings.KEYS);allowed.addAll(Set.of("replicas","leader.id"));
        for(String key:properties.stringPropertyNames())if(!allowed.contains(key))throw new IllegalArgumentException("unknown setting: "+key);
        IoniaMetaBatchSettings settings=IoniaMetaBatchSettings.parse(properties);
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        for (String item : properties.getProperty("replicas").split(",")) {
            String[] parts = item.trim().split("@", -1); int separator = parts[1].lastIndexOf(':');
            NodeId node = new NodeId(Long.parseLong(parts[0]));
            if (addresses.put(node, new InetSocketAddress(parts[1].substring(0, separator), Integer.parseInt(parts[1].substring(separator + 1)))) != null)
                throw new IllegalArgumentException("duplicate replica");
        }
        List<NodeId> replicas = new ArrayList<>(addresses.keySet());
        NodeId leader = new NodeId(Long.parseLong(properties.getProperty("leader.id", "1")));
        IoniaConfig membership = new IoniaConfig(leader, leader, replicas, 0);
        ClientId id = new ClientId(Math.max(1, UUID.randomUUID().getLeastSignificantBits() & Long.MAX_VALUE));
        TimeSource time = TimeSource.system();
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        try (IoniaMetaBatchTcpTransport transport = new IoniaMetaBatchTcpTransport(new NodeId(0), IoniaMetaBatchSettings.configuration(membership.configurationId()), addresses, 1500);
                IoniaMetaBatchClient client = new IoniaMetaBatchClient(id, replicas, leader, 0, transport, time, settings)) {
            timer.scheduleAtFixedRate(() -> client.onTimer(new TimerEvent(IoniaEngine.TICK, time.monotonicNanos())), 10, 10, TimeUnit.MILLISECONDS);
            RequestContext context = new RequestContext(id, new OperationId(1), RoutingVersion.ZERO, OwnershipEpoch.ZERO,
                    Deadline.after(time, Duration.ofSeconds(10)));
            Result result = null;
            while (!context.deadline().isExpired(time)) {
                try {
                    client.refreshView().toCompletableFuture().get(remaining(context, time, TimeUnit.SECONDS.toNanos(5)), TimeUnit.NANOSECONDS);
                    if (context.deadline().isExpired(time)) break;
                    result = client.execute(command, context).toCompletableFuture()
                            .get(remaining(context, time, Long.MAX_VALUE), TimeUnit.NANOSECONDS);
                    if (result.status() != ResultStatus.UNAVAILABLE && result.status() != ResultStatus.NOT_PRIMARY && result.status() != ResultStatus.BUSY) break;
                } catch (ExecutionException | TimeoutException transientFailure) {
                    result = Result.of(ResultStatus.UNAVAILABLE, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
                }
                if (!context.deadline().isExpired(time))
                    TimeUnit.NANOSECONDS.sleep(remaining(context, time, TimeUnit.MILLISECONDS.toNanos(100)));
            }
            if (result == null || context.deadline().isExpired(time))
                result = Result.of(ResultStatus.TIMEOUT, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
            System.out.println(result.status() + result.value().map(value -> " " + new String(value.toByteArray(), StandardCharsets.UTF_8)).orElse(""));
        } finally { timer.shutdownNow(); }
    }
    private static long remaining(RequestContext context, TimeSource time, long cap) {
        return Math.max(1, Math.min(cap, context.deadline().monotonicNanos() - time.monotonicNanos()));
    }
}
