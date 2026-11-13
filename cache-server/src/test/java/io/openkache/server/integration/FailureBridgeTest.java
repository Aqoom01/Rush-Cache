package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.failure.MemberStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.replication.ionia.IoniaEngine;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class FailureBridgeTest {
    @TempDir Path directory;
    @ParameterizedTest @ValueSource(strings = {"SWIM", "SWIM_LIFEGUARD"})
    void realUdpDeadTriggersVrViewChangeBeforeWatchdog(String detector) throws Exception {
        var p = IntegratedCacheTest.properties("VR"); p.setProperty("failure.detector", detector); p.setProperty("failure.grace.ms", "0");
        var c = new IntegrationConfig(p); var nodes = new ArrayList<A1Replica>(); var detectors = new ArrayList<FailureBridge>();
        try (var journal = new JsonLine(directory.resolve("vr-fd.jsonl")); var backend = new ReplicatedBackend.Native(c, 0)) {
            try {
                for (int i = 0; i < 3; i++) nodes.add(new A1Replica(c, 0, i, directory.resolve("vr" + i)));
                for (int i = 0; i < 3; i++) detectors.add(new FailureBridge(c, 0, i, nodes.get(i)::membership, journal));
                assertEquals(ResultStatus.OK, backend.administrative(new Command.Set(Bytes.utf8("key"), Bytes.utf8("value"))).status());
                nodes.get(0).close(); detectors.get(0).close(); long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(6); Result read = null;
                while (System.nanoTime() < until) { read = backend.administrative(new Command.Get(Bytes.utf8("key"))); if (read.status() == ResultStatus.OK) break; Thread.sleep(20); }
                assertNotNull(read); assertEquals(Optional.of(Bytes.utf8("value")), read.value());
                assertTrue(Files.readString(directory.resolve("vr-fd.jsonl")).contains("DEAD"));
            } finally { detectors.forEach(FailureBridge::close); nodes.forEach(A1Replica::close); }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"O00:SWIM", "O00:SWIM_LIFEGUARD", "V03:SWIM", "V03:SWIM_LIFEGUARD"})
    void realUdpDeadTriggersIoniaViewChangeBeforeWatchdog(String variant) throws Exception {
        String[] parts = variant.split(":"); var p = IntegratedCacheTest.properties(parts[0]);
        p.setProperty("failure.detector", parts[1]); p.setProperty("failure.grace.ms", "0");
        IntegrationConfig c = new IntegrationConfig(p); List<IoniaProfile.Replica> nodes = new ArrayList<>(); List<FailureBridge> detectors = new ArrayList<>();
        AtomicInteger signals = new AtomicInteger();
        try (var journal = new JsonLine(directory.resolve("fd.jsonl")); var backend = new ReplicatedBackend.Native(c, 0)) {
            try {
                for (int i = 0; i < 3; i++) nodes.add(IoniaProfile.replica(c, 0, i, directory.resolve("node" + i)));
                for (int i = 0; i < 3; i++) {
                    var node = nodes.get(i);
                    detectors.add(new FailureBridge(c, 0, i, event -> {
                        var view = node.view().get();
                        if (event.current() == MemberStatus.DEAD && event.member().equals(view.leaderId()) && view.status() == IoniaEngine.Status.NORMAL) {
                            signals.incrementAndGet(); node.startViewChange().accept(view.view() + 1);
                        }
                    }, journal));
                }
                assertEquals(ResultStatus.OK, backend.administrative(new Command.Set(Bytes.utf8("survive"), Bytes.utf8("v"))).status());
                assertEquals(Optional.of(Bytes.utf8("v")), backend.administrative(new Command.Get(Bytes.utf8("survive"))).value());
                nodes.get(0).close(); detectors.get(0).close();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
                while (signals.get() == 0 && System.nanoTime() < until) Thread.sleep(20);
                assertTrue(signals.get() > 0, "external detector must signal before the 10 second watchdog");
                Result read = null;
                while (System.nanoTime() < until) {
                    read = backend.administrative(new Command.Get(Bytes.utf8("survive")));
                    if (read.status() == ResultStatus.OK) break;
                    Thread.sleep(20);
                }
                assertNotNull(read); assertEquals(ResultStatus.OK, read.status()); assertEquals(Optional.of(Bytes.utf8("v")), read.value());
                assertTrue(nodes.get(1).view().get().view() > 0 || nodes.get(2).view().get().view() > 0);
            } finally {
                for (var detector : detectors) detector.close();
                for (var node : nodes) node.close();
            }
        }
    }
}
