package io.openkache.server.integration;

import io.openkache.core.failure.*;
import io.openkache.core.model.*;
import io.openkache.core.spi.*;
import io.openkache.server.failure.SwimRuntime;
import io.openkache.server.failure.codec.SwimMessageCodec;
import java.io.IOException;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** 기존 SWIM/Lifeguard를 실제 UDP로 구동한다. DEAD는 엔진의 합의 시작 신호이며 승격 승인이 아니다. */
final class FailureBridge implements Transport, FailureRuntime {
    private final Map<NodeId, InetSocketAddress> addresses;
    private final NodeId self;
    private final DatagramSocket socket;
    private final SwimRuntime runtime;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong sent = new AtomicLong(), bytes = new AtomicLong(), received = new AtomicLong(), failures = new AtomicLong();
    private final Thread receiver;
    private final ConcurrentMap<NodeId, MemberStatus> members = new ConcurrentHashMap<>();
    FailureBridge(IntegrationConfig c, int group, int index, MembershipListener listener, JsonLine journal) throws IOException {
        addresses = c.addresses(group, c.ionia() ? 1 : 2); self = new ArrayList<>(addresses.keySet()).get(index);
        socket = new DatagramSocket(addresses.get(self));
        boolean lha = c.text("failure.detector", "FIXED_TIMEOUT").equals("SWIM_LIFEGUARD");
        var settings = new SwimConfig(Duration.ofMillis(200), Duration.ofMillis(50), Duration.ofMillis(50), Duration.ofMillis(500),
                1, 3, 8, lha, lha ? 8 : 0, lha ? 2 : 1, lha ? 1 : 0);
        runtime = new SwimRuntime(self, addresses.keySet(), settings, this, event -> {
            members.put(event.member(), event.current());
            journal.event("membership", "\"member\":" + event.member().value() + ",\"state\":" + JsonLine.quote(event.current().name()));
            listener.onMembershipChanged(event);
        }, 900, Long.parseLong(c.text("failure.seed", "1")) + self.value() * 7919);
        runtime.start(Duration.ofMillis(c.number("failure.grace.ms", 3000)));
        receiver = Thread.ofVirtual().name("integration-fd-udp").start(() -> {
            byte[] buffer = new byte[65507];
            while (!closed.get()) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length); socket.receive(packet);
                    var envelope = SwimMessageCodec.decode(Arrays.copyOf(packet.getData(), packet.getLength()));
                    if (envelope.to().equals(self) && addresses.containsKey(envelope.from()) && !self.equals(envelope.from())) {
                        received.incrementAndGet(); runtime.onMessage(envelope.from(), (SwimMessage) envelope.message());
                    }
                } catch (IOException | RuntimeException e) { if (!closed.get()) failures.incrementAndGet(); }
            }
        });
    }
    @Override public CompletionStage<Void> send(PeerEnvelope envelope) {
        try {
            if (closed.get() || !envelope.from().equals(self) || !addresses.containsKey(envelope.to())) throw new IOException("invalid FD destination/lifecycle");
            byte[] data = SwimMessageCodec.encode(envelope);
            socket.send(new DatagramPacket(data, data.length, addresses.get(envelope.to()))); sent.incrementAndGet(); bytes.addAndGet(data.length);
            return CompletableFuture.completedFuture(null);
        } catch (IOException e) { failures.incrementAndGet(); return CompletableFuture.failedFuture(e); }
    }
    public String json() { return "{\"sentFrames\":" + sent.get() + ",\"sentDatagramBytes\":" + bytes.get() + ",\"receivedFrames\":" + received.get()
            + ",\"failures\":" + failures.get() + ",\"awareness\":" + runtime.awarenessScore() + ",\"deadMembers\":"
            + members.entrySet().stream().filter(e -> e.getValue() == MemberStatus.DEAD).map(e -> "" + e.getKey().value()).sorted().collect(java.util.stream.Collectors.joining(",", "[", "]")) + "}"; }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        socket.close(); runtime.close(); receiver.interrupt();
    }
}
