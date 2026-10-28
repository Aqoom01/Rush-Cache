package io.openkache.server.ionia;

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
public final class IoniaClientMain {
    private IoniaClientMain() { }
    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) throw new IllegalArgumentException("usage: IoniaClientMain node.properties get|set|delete key [value]");
        Command command = switch (args[1]) {
            case "get" -> new Command.Get(Bytes.utf8(args[2]));
            case "delete" -> new Command.Delete(Bytes.utf8(args[2]));
            case "set" -> { if (args.length != 4) throw new IllegalArgumentException("set needs a value"); yield new Command.Set(Bytes.utf8(args[2]), Bytes.utf8(args[3])); }
            default -> throw new IllegalArgumentException("unknown operation");
        };
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(Path.of(args[0]))) { properties.load(reader); }
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
        try (IoniaTcpTransport transport = new IoniaTcpTransport(new NodeId(0), membership.configurationId(), addresses, 1500);
                IoniaClient client = new IoniaClient(id, replicas, leader, 0, transport, time,
                        new IoniaClient.Options(replicas.stream().filter(node -> !node.equals(leader)).findFirst().orElseThrow(), true, 50_000_000))) {
            timer.scheduleAtFixedRate(() -> client.onTimer(new TimerEvent(IoniaEngine.TICK, time.monotonicNanos())), 10, 10, TimeUnit.MILLISECONDS);
            RequestContext context = new RequestContext(id, new OperationId(1), RoutingVersion.ZERO, OwnershipEpoch.ZERO,
                    Deadline.after(time, Duration.ofSeconds(10)));
            Result result = null;
            for (int attempt = 0; attempt < 3 && !context.deadline().isExpired(time); attempt++) {
                client.refreshView().toCompletableFuture().get(5, TimeUnit.SECONDS);
                result = client.execute(command, context).toCompletableFuture().get(10, TimeUnit.SECONDS);
                if (result.status() != ResultStatus.UNAVAILABLE && result.status() != ResultStatus.NOT_PRIMARY) break;
                Thread.sleep(100);
            }
            if (result == null) throw new TimeoutException("no result; request outcome is unknown");
            System.out.println(result.status() + result.value().map(value -> " " + new String(value.toByteArray(), StandardCharsets.UTF_8)).orElse(""));
        } finally { timer.shutdownNow(); }
    }
}
