package io.openkache.core.replication.ionia;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.spi.PeerEnvelope;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;

/** Bounded exhaustive history checker with real-time precedence and GET/SET/DELETE return semantics. */
@Timeout(30)
class IoniaLinearizabilityTest {
    private record Operation(Command command, long start, long end, Result result) { }

    @Test void T29_seededConcurrentHistoriesHaveALegalLinearization() {
        for (long seed = 0; seed < 64; seed++) {
            Random random = new Random(seed);
            AtomicLong events = new AtomicLong();
            List<Operation> history = new ArrayList<>();
            try (IoniaO00Test.Cluster cluster = new IoniaO00Test.Cluster(3)) {
                for (int i = 0; i < 10; i++) {
                    Bytes key = Bytes.utf8("k" + random.nextInt(2));
                    Command command = switch (random.nextInt(3)) {
                        case 0 -> new Command.Set(key, Bytes.utf8("value" + i)); case 1 -> new Command.Get(key); default -> new Command.Delete(key);
                    };
                    long start = events.incrementAndGet();
                    cluster.client.execute(command, IoniaO00Test.context(i + 1)).whenComplete((value, error) -> {
                        assertNull(error); history.add(new Operation(command, start, events.incrementAndGet(), value));
                    });
                    for (int step = 0; step < random.nextInt(12); step++) drive(cluster, random);
                }
                int limit = 100000;
                while (!cluster.tasks.isEmpty() || !cluster.messages.isEmpty()) { assertTrue(limit-- > 0); drive(cluster, random); }
                assertEquals(10, history.size(), "seed=" + seed + " " + cluster.trace);
                assertTrue(check(history, 0, new HashMap<>(), new HashSet<>()), "seed=" + seed + " history=" + history + " trace=" + cluster.trace);
            }
        }
        System.out.println("T29: seeds=0..63, 10 completed operations/history, GET/SET/DELETE, all histories linearizable; bounded check only");
    }
    private static void drive(IoniaO00Test.Cluster cluster, Random random) {
        if (!cluster.tasks.isEmpty() && (cluster.messages.isEmpty() || random.nextBoolean())) cluster.runOne();
        else if (!cluster.messages.isEmpty()) {
            PeerEnvelope envelope = cluster.messages.remove(random.nextInt(cluster.messages.size()));
            cluster.trace.add(envelope.toString());
            cluster.engines.get(envelope.to()).onPeerMessage(envelope.from(), envelope.message());
        }
    }
    private static boolean check(List<Operation> history, int used, Map<Bytes, Bytes> state, Set<String> visited) {
        if (Integer.bitCount(used) == history.size()) return true;
        String fingerprint = used + ":" + state.entrySet().stream().sorted(Comparator.comparing(entry -> entry.getKey().toString())).toList();
        if (!visited.add(fingerprint)) return false;
        for (int i = 0; i < history.size(); i++) {
            if ((used & (1 << i)) != 0) continue;
            Operation operation = history.get(i); boolean eligible = true;
            for (int j = 0; j < history.size(); j++) if ((used & (1 << j)) == 0 && history.get(j).end() < operation.start()) eligible = false;
            if (!eligible) continue;
            Map<Bytes, Bytes> next = new HashMap<>(state);
            Bytes old = next.get(IoniaCommands.key(operation.command()));
            boolean valid;
            if (operation.command() instanceof Command.Get) valid = old == null
                    ? operation.result().status() == ResultStatus.MISS
                    : operation.result().status() == ResultStatus.OK && operation.result().value().equals(Optional.of(old));
            else if (operation.command() instanceof Command.Set set) { valid = operation.result().status() == ResultStatus.OK; next.put(set.key(), set.value()); }
            else { valid = operation.result().status() == (old == null ? ResultStatus.MISS : ResultStatus.OK); next.remove(IoniaCommands.key(operation.command())); }
            if (valid && check(history, used | (1 << i), next, visited)) return true;
        }
        return false;
    }
    @Test void checkerRejectsStaleMissAfterACompletedSet() {
        List<Operation> invalid = List.of(new Operation(IoniaO00Test.set("v"), 1, 2, IoniaO00Test.plain(ResultStatus.OK)),
                new Operation(new Command.Get(IoniaO00Test.K), 3, 4, IoniaO00Test.plain(ResultStatus.MISS)));
        assertFalse(check(invalid, 0, new HashMap<>(), new HashSet<>()));
    }
}
