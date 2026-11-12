package io.openkache.server.ionia_fifth;

import io.openkache.core.replication.ionia_fifth.IoniaApplySettings;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaConfig;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;

/** Independent-process V04a entry point: IoniaFifthMain path/to/node.properties. */
public final class IoniaFifthMain {
    private IoniaFifthMain() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("usage: IoniaFifthMain node.properties");
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(Path.of(args[0]))) { properties.load(reader); }
        Set<String> allowed = Set.of("node.id", "leader.id", "replicas", "data.directory", "storage.workers", "failure.timeout.ms", IoniaApplySettings.LIMIT);
        for (String key : properties.stringPropertyNames()) if (!allowed.contains(key)) throw new IllegalArgumentException("unknown setting: " + key);
        IoniaApplySettings settings = IoniaApplySettings.parse(properties);
        Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
        for (String spec : properties.getProperty("replicas", "").split(",")) {
            String[] parts = spec.trim().split("@", -1);
            if (parts.length != 2) throw new IllegalArgumentException("replicas must be id@host:port,...");
            int separator = parts[1].lastIndexOf(':');
            if (separator <= 0) throw new IllegalArgumentException("missing host/port");
            NodeId id = new NodeId(Long.parseLong(parts[0]));
            InetSocketAddress address = new InetSocketAddress(parts[1].substring(0, separator), Integer.parseInt(parts[1].substring(separator + 1)));
            if (addresses.put(id, address) != null) throw new IllegalArgumentException("duplicate replica");
        }
        if (new HashSet<>(addresses.values()).size() != addresses.size()) throw new IllegalArgumentException("replica addresses must be distinct");
        NodeId node = new NodeId(Long.parseLong(properties.getProperty("node.id")));
        NodeId leader = new NodeId(Long.parseLong(properties.getProperty("leader.id", "1")));
        IoniaConfig config = new IoniaConfig(node, leader, new ArrayList<>(addresses.keySet()), 0);
        try (IoniaFifthNode running = new IoniaFifthNode(config, addresses, Path.of(properties.getProperty("data.directory")),
                Integer.parseInt(properties.getProperty("storage.workers", "2")), Long.parseLong(properties.getProperty("failure.timeout.ms", "1500")), settings)) {
            Runtime.getRuntime().addShutdownHook(new Thread(running::close, "ionia-shutdown"));
            System.out.println("IONIA V04a node=" + node.value() + " address=" + addresses.get(node) + " mode=replicated-memory-log keys=" + settings.maxInFlightKeys() + " storageShards=" + settings.storageShards() + " profile=" + settings.profileId());
            running.awaitTermination();
        }
    }
}
