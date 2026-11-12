package io.openkache.server.ionia_eighth;

import io.openkache.server.ionia.*;
import io.openkache.core.replication.ionia_eighth.*;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Three actual JVMs, loopback TCP, disk KV, forced leader termination and state-transfer rejoin. */
@Timeout(60)
class IoniaMetaBatchProcessTest {
    @TempDir Path directory;
    private final Map<NodeId, Process> processes = new HashMap<>();
    private final Map<NodeId, Path> configs = new HashMap<>();
    private final List<NodeId> nodes = List.of(new NodeId(1), new NodeId(2), new NodeId(3));
    private final Map<NodeId, InetSocketAddress> addresses = new LinkedHashMap<>();
    private final TimeSource time = TimeSource.system();
    private long operation;

    @AfterEach void stop() throws InterruptedException {
        for (Process process : processes.values()) if (process.isAlive()) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS); }
    }
    private void start(NodeId node) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = Path.of(IoniaEighthMain.class.getProtectionDomain().getCodeSource().getLocation().toURI()) + File.pathSeparator
                + Path.of(IoniaEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Process process = new ProcessBuilder(java, "-Xmx96m", "-cp", classpath, IoniaEighthMain.class.getName(), configs.get(node).toString())
                .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(directory.resolve("node-" + node.value() + ".log").toFile())).start();
        processes.put(node, process);
    }
    private String cli(String operationName, String key, String value) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = Path.of(IoniaEighthMain.class.getProtectionDomain().getCodeSource().getLocation().toURI()) + File.pathSeparator
                + Path.of(IoniaEngine.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> arguments = new ArrayList<>(List.of(java, "-Xmx64m", "-cp", classpath, IoniaMetaBatchClientMain.class.getName(),
                directory.resolve("client.properties").toString(), operationName, key));
        if (value != null) arguments.add(value);
        Process clientProcess = new ProcessBuilder(arguments).redirectErrorStream(true).start();
        try {
            assertTrue(clientProcess.waitFor(12, TimeUnit.SECONDS), "CLI did not finish");
            String output = new String(clientProcess.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
            assertEquals(0, clientProcess.exitValue(), output);
            return output;
        } finally { if (clientProcess.isAlive()) clientProcess.destroyForcibly(); }
    }
    private RequestContext context() { return new RequestContext(new ClientId(90), new OperationId(++operation), RoutingVersion.ZERO, OwnershipEpoch.ZERO,
            Deadline.after(time, Duration.ofSeconds(5))); }
    private Result execute(IoniaMetaBatchClient client, Command command) throws Exception {
        CompletionStage<Result> result = client.execute(command, context()); long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!result.toCompletableFuture().isDone() && System.nanoTime() - end < 0) { client.onTimer(new TimerEvent(IoniaEngine.TICK, time.monotonicNanos())); Thread.sleep(10); }
        return result.toCompletableFuture().get(1, TimeUnit.SECONDS);
    }
    private IoniaEngine.ViewInfo awaitNormal(IoniaMetaBatchTcpTransport transport, NodeId node, long minimum) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15); Throwable last = null;
        while (System.nanoTime() - end < 0) {
            try {
                IoniaEngine.ViewInfo view = transport.probe(node).toCompletableFuture().get(2, TimeUnit.SECONDS);
                if (view.view() >= minimum && view.status() == IoniaEngine.Status.NORMAL) return view;
            } catch (ExecutionException | TimeoutException error) { last = error; }
            Thread.sleep(20);
        }
        StringBuilder logs = new StringBuilder();
        for (NodeId id : nodes) { Path path = directory.resolve("node-" + id.value() + ".log"); if (Files.exists(path)) logs.append(Files.readString(path)); }
        throw new AssertionError("node did not become normal: " + node + " " + logs, last);
    }
    @Test void V06_17_threeProcessesRecoverLeaderFailureAndRestartedReplica() throws Exception {
        for (NodeId node : nodes) {
            try (ServerSocket reservation = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
                addresses.put(node, new InetSocketAddress("127.0.0.1", reservation.getLocalPort()));
            }
        }
        String members = String.join(",", nodes.stream().map(node -> node.value() + "@127.0.0.1:" + addresses.get(node).getPort()).toList());
        for (NodeId node : nodes) {
            Properties properties = new Properties(); properties.setProperty("node.id", Long.toString(node.value())); properties.setProperty("leader.id", "1");
            properties.setProperty("replicas", members); properties.setProperty("data.directory", directory.resolve("data-" + node.value()).toString());
            properties.setProperty("failure.timeout.ms", "1500"); properties.setProperty("storage.workers","2");
            Path config = directory.resolve("node-" + node.value() + ".properties"); configs.put(node, config);
            try (Writer writer = Files.newBufferedWriter(config)) { properties.store(writer, "test configuration"); }
            start(node);
        }
        Files.writeString(directory.resolve("client.properties"),"leader.id=1\nreplicas="+members+"\nmeta.batch.entries=8\nmeta.batch.bytes=65536\nmeta.batch.delay.ms=2\nmeta.pending.limit=256\nmeta.inflight.limit=4\n");
        IoniaConfig configuration = new IoniaConfig(nodes.getFirst(), nodes.getFirst(), nodes, 0);
        try (IoniaMetaBatchTcpTransport transport = new IoniaMetaBatchTcpTransport(new NodeId(0), IoniaMetaBatchSettings.configuration(configuration.configurationId()), addresses, 1500);
                IoniaMetaBatchClient client = new IoniaMetaBatchClient(new ClientId(90), nodes, nodes.getFirst(), 0, transport, time, new IoniaMetaBatchSettings(8,65536,2_000_000,256,4))) {
            for (NodeId node : nodes) awaitNormal(transport, node, 0);
            Bytes key = Bytes.utf8("process-key");
            assertEquals(ResultStatus.OK, execute(client, new Command.Set(key, Bytes.utf8("before-crash"))).status());
            assertEquals(Bytes.utf8("before-crash"), execute(client, new Command.Get(key)).value().orElseThrow());
            Process oldLeader = processes.get(nodes.getFirst()); oldLeader.destroyForcibly(); assertTrue(oldLeader.waitFor(5, TimeUnit.SECONDS));
            assertEquals("OK before-crash", cli("get", "process-key", null));
            IoniaEngine.ViewInfo changed = awaitNormal(transport, nodes.get(1), 1);
            awaitNormal(transport, nodes.getLast(), changed.view());
            client.refreshView().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(Bytes.utf8("before-crash"), execute(client, new Command.Get(key)).value().orElseThrow());
            assertEquals(ResultStatus.OK, execute(client, new Command.Set(key, Bytes.utf8("after-crash"))).status());
            start(nodes.getFirst()); awaitNormal(transport, nodes.getFirst(), changed.view());
            IoniaClientTransport.Request read = new IoniaClientTransport.Request(changed.view(), new Command.Get(key), new ClientId(90), new OperationId(++operation),
                    RoutingVersion.ZERO, OwnershipEpoch.ZERO, TimeUnit.SECONDS.toNanos(5));
            IoniaClientTransport.ReadReply restored = transport.followerRead(nodes.getFirst(), read).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(Bytes.utf8("after-crash"), restored.result().value().orElseThrow()); assertTrue(restored.appliedIndex() >= 2);
            assertEquals("OK after-crash", cli("get", "process-key", null));
            Process follower=processes.get(nodes.getLast());follower.destroyForcibly();assertTrue(follower.waitFor(5,TimeUnit.SECONDS));
            assertEquals(Bytes.utf8("after-crash"),execute(client,new Command.Get(key)).value().orElseThrow());
            assertEquals(ResultStatus.OK,execute(client,new Command.Set(key,Bytes.utf8("after-follower-crash"))).status());
            start(nodes.getLast());awaitNormal(transport,nodes.getLast(),changed.view());
            var followerRead=new IoniaClientTransport.Request(changed.view(),new Command.Get(key),new ClientId(90),new OperationId(++operation),RoutingVersion.ZERO,OwnershipEpoch.ZERO,TimeUnit.SECONDS.toNanos(5));
            assertEquals(Bytes.utf8("after-follower-crash"),transport.followerRead(nodes.getLast(),followerRead).toCompletableFuture().get(5,TimeUnit.SECONDS).result().value().orElseThrow());

            assertEquals("OK", cli("set", "cli-key", "cli-value"));
            assertEquals("OK cli-value", cli("get", "cli-key", null));
            assertEquals("OK", cli("delete", "cli-key", null));
            assertEquals("MISS", cli("get", "cli-key", null));
            assertEquals(ResultStatus.OK, execute(client, new Command.Delete(key)).status());
            assertEquals(ResultStatus.MISS, execute(client, new Command.Delete(key)).status());
            assertEquals(ResultStatus.MISS, execute(client, new Command.Get(key)).status());
            System.out.println("V06 process fixture: JVMs=3, forced leader crash, new-view=" + changed.view()
                    + ", majority fallback, leader+follower crash/restart, disk replay/rejoin, DELETE OK/MISS verified; profile="+client.diagnostics().profile()+"; loopback host only");
        }
    }
}
