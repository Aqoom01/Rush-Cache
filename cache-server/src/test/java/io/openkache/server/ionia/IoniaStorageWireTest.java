package io.openkache.server.ionia;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.spi.TimeSource;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Timeout(20)
class IoniaStorageWireTest {
    @TempDir Path directory;
    static final NodeId A = new NodeId(1), B = new NodeId(2);
    static final Bytes K = Bytes.utf8("disk-key");
    static RequestContext context(long id) { return new RequestContext(new ClientId(1), new OperationId(id), RoutingVersion.ZERO, OwnershipEpoch.ZERO,
            Deadline.after(TimeSource.system(), Duration.ofSeconds(5))); }
    static Result run(IoniaFileStorage store, Command command, long id) throws Exception { return store.execute(command, context(id)).toCompletableFuture().get(5, TimeUnit.SECONDS); }
    @Test void T25_T27_diskValuesSurviveReopenAndResetClearsAuthoritativeState() throws Exception {
        try (IoniaFileStorage store = new IoniaFileStorage(A, directory, 2, TimeSource.system(), true)) {
            assertEquals(ResultStatus.OK, run(store, new Command.Set(K, Bytes.utf8("ssd-value")), 1).status());
        }
        try (IoniaFileStorage store = new IoniaFileStorage(A, directory, 2, TimeSource.system(), false)) {
            assertEquals(Bytes.utf8("ssd-value"), run(store, new Command.Get(K), 2).value().orElseThrow());
            assertEquals(ResultStatus.OK, run(store, new Command.Delete(K), 3).status());
            assertEquals(ResultStatus.MISS, run(store, new Command.Delete(K), 4).status());
            run(store, new Command.Set(K, Bytes.utf8("stale")), 5); store.reset().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertEquals(ResultStatus.MISS, run(store, new Command.Get(K), 6).status());
        }
    }
    @Test void stableViewMonotonicityAndRestartMarkerAreIndependentOfDurabilityLogFsync() throws Exception {
        IoniaFileViewStore fresh = new IoniaFileViewStore(directory); assertFalse(fresh.restarted()); fresh.save(3);
        assertThrows(IllegalArgumentException.class, () -> fresh.save(2));
        IoniaFileViewStore reopened = new IoniaFileViewStore(directory); assertTrue(reopened.restarted()); assertEquals(3, reopened.view());
    }
    @Test void staticConfigurationChangesAreRejectedBeforeServingOldData() throws Exception {
        new IoniaFileViewStore(directory, "members=1,2,3;node=1");
        assertThrows(IOException.class, () -> new IoniaFileViewStore(directory, "members=1,2,4;node=1"));
        assertThrows(IOException.class, () -> new IoniaFileViewStore(directory, "members=1,2,3;node=2"));
    }
    @Test void partialTrailingKvRecordIsDiscardedOnOpen() throws Exception {
        try (IoniaFileStorage store = new IoniaFileStorage(A, directory, 1, TimeSource.system(), false)) { run(store, new Command.Set(K, Bytes.utf8("good")), 1); }
        Files.write(directory.resolve("kv-0.data"), new byte[]{0, 0}, StandardOpenOption.APPEND);
        try (IoniaFileStorage store = new IoniaFileStorage(A, directory, 1, TimeSource.system(), false)) {
            assertEquals(Bytes.utf8("good"), run(store, new Command.Get(K), 2).value().orElseThrow());
        }
    }
    @Test void exclusiveDirectoryLockRejectsTwoNodesSharingOneKvState() throws Exception {
        try (IoniaFileStorage first = new IoniaFileStorage(A, directory, 1, TimeSource.system(), false)) {
            assertNotNull(first.snapshot());
            assertThrows(java.nio.channels.OverlappingFileLockException.class,
                    () -> new IoniaFileStorage(B, directory, 1, TimeSource.system(), false));
        }
    }
    @Test void allPeerMessagesRoundTripWithoutJavaSerialization() throws Exception {
        IoniaMessage.LogEntry entry = new IoniaMessage.LogEntry(1, new ClientId(1), new OperationId(2), new Command.Set(K, Bytes.utf8("v")));
        IoniaRecovery.State state = new IoniaRecovery.State(0, List.of(entry), List.of(entry), 0);
        UUID nonce = UUID.randomUUID();
        List<IoniaMessage> messages = List.of(new IoniaMessage.Prepare(0, List.of(entry)), new IoniaMessage.PrepareAck(0, 1),
                new IoniaMessage.Commit(0, 1), new IoniaMessage.Applied(0, 1), new IoniaMessage.StartViewChange(1),
                new IoniaMessage.DoViewChange(1, state), new IoniaMessage.StartView(1, state), new IoniaMessage.RecoveryRequest(0, nonce),
                new IoniaMessage.RecoveryResponse(0, nonce, state), new IoniaMessage.RecoveryResponse(0, nonce, null));
        for (IoniaMessage message : messages) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            IoniaWire.write(bytes, 99, A, B, "config", IoniaWire.PEER, out -> IoniaWire.peer(out, message));
            IoniaWire.Packet packet = IoniaWire.read(new ByteArrayInputStream(bytes.toByteArray()));
            assertEquals(99, packet.correlation()); assertEquals(message, IoniaWire.peer(packet.body())); IoniaWire.exhausted(packet.body());
        }
    }
    @Test void invalidFrameLengthsVersionsAndTruncationAreRejected() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        IoniaWire.write(bytes, 1, A, B, "config", IoniaWire.PROBE, out -> { }); byte[] valid = bytes.toByteArray();
        assertThrows(IOException.class, () -> IoniaWire.read(new ByteArrayInputStream(Arrays.copyOf(valid, valid.length - 1))));
        byte[] badMagic = valid.clone(); badMagic[4] = 0;
        assertThrows(IOException.class, () -> IoniaWire.read(new ByteArrayInputStream(badMagic)));
        assertThrows(IOException.class, () -> IoniaWire.read(new ByteArrayInputStream(new byte[]{127, -1, -1, -1})));
    }
}
