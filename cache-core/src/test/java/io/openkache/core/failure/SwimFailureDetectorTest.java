package io.openkache.core.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.spi.Transport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class SwimFailureDetectorTest {

    private static final NodeId NODE_1 = new NodeId(1);
    private static final NodeId NODE_2 = new NodeId(2);
    private static final NodeId NODE_3 = new NodeId(3);
    private static final List<NodeId> THREE_NODES = List.of(NODE_1, NODE_2, NODE_3);

    // 테스트에서는 실제로 기다리지 않고 ManualTime을 전진시켜 각 타임아웃 경계를 재현한다.
    // 순서: 프로브 주기 500ms, 직접 PING 100ms, 간접 PING 100ms, 의심 상태 300ms.
    private static final SwimConfig CONFIG = new SwimConfig(
            Duration.ofMillis(500),
            Duration.ofMillis(100),
            Duration.ofMillis(100),
            Duration.ofMillis(300),
            3,
            3,
            8);
    private static final SwimConfig LIFEGUARD_CONFIG = new SwimConfig(
            Duration.ofMillis(500),Duration.ofMillis(100),Duration.ofMillis(100),Duration.ofMillis(300),
            3,3,8,true,8,2,1);

    /**
     * 직접 PING에 ACK가 오면 프로브가 성공한 것으로 끝나며, 시간이 더 지나도 대상은 ALIVE이고
     * 멤버십 변경 이벤트가 발생하지 않는지 검증한다.
     */
    @Test
    void directAckCompletesProbeWithoutSuspicion() {
        Fixture fixture = fixture(NODE_1);
        fixture.detector.start();

        fixture.detector.tick();
        PeerEnvelope sent = fixture.transport.removeFirst();
        SwimMessage.Ping ping = assertInstanceOf(SwimMessage.Ping.class, sent.message());

        fixture.detector.onMessage(
                sent.to(), new SwimMessage.Ack(ping.probeId(), sent.to(), NODE_1, List.of()));
        fixture.time.advance(Duration.ofMillis(250));
        fixture.detector.tick();

        assertEquals(MemberStatus.ALIVE, fixture.detector.member(sent.to()).orElseThrow().status());
        assertTrue(fixture.events.isEmpty());
    }

    /**
     * 직접 PING 제한 시간 안에 응답이 없어 간접 프로브(PING_REQ)로 전환된 뒤, 헬퍼를 통해 ACK를
     * 받으면 대상을 의심하지 않고 ALIVE로 유지하는지 검증한다.
     */
    @Test
    void indirectAckCompletesProbeWhenDirectProbeTimesOut() {
        Fixture fixture = fixture(NODE_1);
        fixture.detector.start();
        fixture.detector.tick();
        PeerEnvelope direct = fixture.transport.removeFirst();
        SwimMessage.Ping ping = assertInstanceOf(SwimMessage.Ping.class, direct.message());

        fixture.time.advance(Duration.ofMillis(100));
        fixture.detector.tick();
        PeerEnvelope indirect = fixture.transport.removeFirst();
        SwimMessage.PingRequest request =
                assertInstanceOf(SwimMessage.PingRequest.class, indirect.message());
        assertEquals(direct.to(), request.target());

        fixture.detector.onMessage(
                indirect.to(),
                new SwimMessage.Ack(ping.probeId(), direct.to(), NODE_1, List.of()));
        fixture.time.advance(Duration.ofMillis(100));
        fixture.detector.tick();

        assertEquals(MemberStatus.ALIVE, fixture.detector.member(direct.to()).orElseThrow().status());
        assertTrue(fixture.events.isEmpty());
    }

    /**
     * 직접 프로브와 간접 프로브가 모두 실패하면 대상이 ALIVE에서 SUSPECT로 바뀌고, 의심 제한
     * 시간까지 새 생존 정보가 없으면 DEAD로 바뀌며 두 변경 이벤트가 순서대로 발행되는지 검증한다.
     */
    @Test
    void failedDirectAndIndirectProbeBecomeSuspectThenDead() {
        Fixture fixture = fixture(NODE_1);
        fixture.detector.start();
        fixture.detector.tick();
        NodeId target = fixture.transport.removeFirst().to();

        fixture.time.advance(Duration.ofMillis(100));
        fixture.detector.tick();
        assertInstanceOf(SwimMessage.PingRequest.class, fixture.transport.removeFirst().message());

        fixture.time.advance(Duration.ofMillis(100));
        fixture.detector.tick();
        assertEquals(MemberStatus.SUSPECT, fixture.detector.member(target).orElseThrow().status());
        assertEquals(MemberStatus.SUSPECT, fixture.events.getFirst().current());

        fixture.time.advance(Duration.ofMillis(300));
        fixture.detector.tick();
        assertEquals(MemberStatus.DEAD, fixture.detector.member(target).orElseThrow().status());
        assertEquals(MemberStatus.DEAD, fixture.events.get(1).current());
    }

    /**
     * 로컬 노드 자신을 SUSPECT로 표시한 gossip을 받으면 incarnation을 높여 해당 정보를 반박하고,
     * 증가한 incarnation의 ALIVE 정보를 ACK에 실어 응답하는지 검증한다.
     */
    @Test
    void suspectedLocalNodeRefutesWithHigherIncarnationInAck() {
        Fixture fixture = fixture(NODE_1);
        fixture.detector.start();
        MembershipUpdate suspect = new MembershipUpdate(NODE_1, 0, MemberStatus.SUSPECT, NODE_2);

        fixture.detector.onMessage(
                NODE_2, new SwimMessage.Ping(7, NODE_2, List.of(suspect)));

        MemberRecord local = fixture.detector.member(NODE_1).orElseThrow();
        assertEquals(MemberStatus.ALIVE, local.status());
        assertEquals(1, local.incarnation());
        SwimMessage.Ack ack =
                assertInstanceOf(SwimMessage.Ack.class, fixture.transport.removeFirst().message());
        assertEquals(
                new MembershipUpdate(NODE_1, 1, MemberStatus.ALIVE, NODE_1),
                ack.updates().getFirst());
    }

    @Test
    void resumedLocalNodeRefutesDeadGossipRatherThanRemainingDead() {
        Fixture fixture=fixture(NODE_1);fixture.detector.start();
        var dead=new MembershipUpdate(NODE_1,3,MemberStatus.DEAD,NODE_2);
        fixture.detector.onMessage(NODE_2,new SwimMessage.Ping(8,NODE_2,List.of(dead)));
        var local=fixture.detector.member(NODE_1).orElseThrow();
        assertEquals(MemberStatus.ALIVE,local.status());assertEquals(4,local.incarnation());
        var ack=assertInstanceOf(SwimMessage.Ack.class,fixture.transport.removeFirst().message());
        assertEquals(new MembershipUpdate(NODE_1,4,MemberStatus.ALIVE,NODE_1),ack.updates().getFirst());
    }

    /**
     * SUSPECT인 노드에 대해 더 높은 incarnation의 ALIVE gossip을 받으면 의심 타이머를 취소하고,
     * 기존 의심 제한 시간이 지나도 ALIVE 상태를 유지하는지 검증한다.
     */
    @Test
    void higherIncarnationAliveCancelsSuspicion() {
        Fixture fixture = fixture(NODE_1);
        fixture.detector.start();
        fixture.detector.tick();
        NodeId target = fixture.transport.removeFirst().to();
        NodeId sender = target.equals(NODE_2) ? NODE_3 : NODE_2;

        fixture.time.advance(Duration.ofMillis(100));
        fixture.detector.tick();
        fixture.transport.removeFirst();
        fixture.time.advance(Duration.ofMillis(100));
        fixture.detector.tick();
        assertEquals(MemberStatus.SUSPECT, fixture.detector.member(target).orElseThrow().status());

        MembershipUpdate alive = new MembershipUpdate(target, 1, MemberStatus.ALIVE, target);
        fixture.detector.onMessage(sender, new SwimMessage.Ping(9, sender, List.of(alive)));
        fixture.time.advance(Duration.ofSeconds(1));
        fixture.detector.tick();

        MemberRecord recovered = fixture.detector.member(target).orElseThrow();
        assertEquals(MemberStatus.ALIVE, recovered.status());
        assertEquals(1, recovered.incarnation());
        assertEquals(MemberStatus.ALIVE, fixture.events.get(1).current());
    }

    /**
     * PING_REQ를 받은 헬퍼가 실제 대상에게 PING을 보내고, 대상의 ACK를 원래 프로브를 시작한
     * 노드에 그대로 중계하는지 검증한다.
     */
    @Test
    void helperRelaysIndirectAckToOrigin() {
        Fixture helper = fixture(NODE_2);
        helper.detector.start();
        SwimMessage.PingRequest request =
                new SwimMessage.PingRequest(12, NODE_1, NODE_3, List.of());

        helper.detector.onMessage(NODE_1, request);
        PeerEnvelope pingEnvelope = helper.transport.removeFirst();
        assertEquals(NODE_3, pingEnvelope.to());
        SwimMessage.Ping ping = assertInstanceOf(SwimMessage.Ping.class, pingEnvelope.message());
        assertEquals(NODE_1, ping.origin());

        helper.detector.onMessage(
                NODE_3, new SwimMessage.Ack(12, NODE_3, NODE_1, List.of()));
        PeerEnvelope relayed = helper.transport.removeFirst();
        assertEquals(NODE_1, relayed.to());
        SwimMessage.Ack ack = assertInstanceOf(SwimMessage.Ack.class, relayed.message());
        assertEquals(NODE_3, ack.target());
    }

    /**
     * 이미 incarnation 2의 ALIVE 정보를 알고 있을 때 뒤늦게 도착한 incarnation 1의 SUSPECT
     * 정보가 최신 상태를 덮어쓰지 못하는지 검증한다.
     */
    @Test
    void staleSuspicionDoesNotOverrideNewerAliveIncarnation() {
        Fixture fixture = fixture(NODE_1);
        fixture.detector.start();
        fixture.detector.onMessage(
                NODE_2,
                new SwimMessage.Ping(
                        1,
                        NODE_2,
                        List.of(new MembershipUpdate(NODE_3, 2, MemberStatus.ALIVE, NODE_3))));
        fixture.transport.removeFirst();

        fixture.detector.onMessage(
                NODE_2,
                new SwimMessage.Ping(
                        2,
                        NODE_2,
                        List.of(new MembershipUpdate(NODE_3, 1, MemberStatus.SUSPECT, NODE_2))));

        MemberRecord node = fixture.detector.member(NODE_3).orElseThrow();
        assertEquals(MemberStatus.ALIVE, node.status());
        assertEquals(2, node.incarnation());
    }

    /**
     * start() 전에 tick()이나 onMessage()를 호출하면 탐지기가 잘못 사용된 것으로 보고
     * IllegalStateException을 던지는지 검증한다.
     */
    @Test
    void useBeforeStartIsRejected() {
        Fixture fixture = fixture(NODE_1);

        assertThrows(IllegalStateException.class, fixture.detector::tick);
        assertThrows(
                IllegalStateException.class,
                () -> fixture.detector.onMessage(
                        NODE_2, new SwimMessage.Ping(1, NODE_2, List.of())));
    }

    @Test
    void lifeguardLocalDelayScalesProbeDeadlinesAndSuccessRecoversHealth() {
        Fixture fixture = fixture(NODE_1,LIFEGUARD_CONFIG);
        fixture.detector.start();
        fixture.detector.observeLocalDelay(Duration.ofSeconds(2).toNanos());
        assertEquals(3,fixture.detector.awarenessScore());
        assertEquals(Duration.ofMillis(400).toNanos(),fixture.detector.effectiveDirectProbeTimeoutNanos());

        fixture.detector.tick();
        PeerEnvelope direct=fixture.transport.removeFirst();
        SwimMessage.Ping ping=assertInstanceOf(SwimMessage.Ping.class,direct.message());
        fixture.time.advance(Duration.ofMillis(100)); fixture.detector.tick();
        assertTrue(fixture.transport.sent.isEmpty());

        fixture.detector.onMessage(direct.to(),new SwimMessage.Ack(ping.probeId(),direct.to(),NODE_1,List.of()));
        assertEquals(2,fixture.detector.awarenessScore());
    }

    @Test
    void lifeguardFailedProbeDegradesHealthAndIndependentConfirmationShortensSuspicion() {
        Fixture fixture=fixture(NODE_1,LIFEGUARD_CONFIG);
        fixture.detector.start(); fixture.detector.tick();
        NodeId target=fixture.transport.removeFirst().to();
        fixture.time.advance(Duration.ofMillis(100)); fixture.detector.tick(); fixture.transport.removeFirst();
        fixture.time.advance(Duration.ofMillis(100)); fixture.detector.tick();

        assertEquals(1,fixture.detector.awarenessScore());
        long initialDeadline=fixture.detector.suspicionDeadlineNanos(target);
        NodeId independentReporter=target.equals(NODE_2)?NODE_3:NODE_2;
        fixture.detector.onMessage(independentReporter,new SwimMessage.Ping(99,independentReporter,
                List.of(new MembershipUpdate(target,0,MemberStatus.SUSPECT,independentReporter))));
        long confirmedDeadline=fixture.detector.suspicionDeadlineNanos(target);
        assertTrue(confirmedDeadline<initialDeadline);
        assertEquals(fixture.time.monotonicNanos()+Duration.ofMillis(600).toNanos(),confirmedDeadline);
    }

    @Test
    void swimBaselineIgnoresLifeguardLocalDelay() {
        Fixture fixture=fixture(NODE_1,CONFIG);
        fixture.detector.start(); fixture.detector.observeLocalDelay(Duration.ofSeconds(10).toNanos());
        assertEquals(0,fixture.detector.awarenessScore());
        assertEquals(Duration.ofMillis(100).toNanos(),fixture.detector.effectiveDirectProbeTimeoutNanos());
    }

    private static Fixture fixture(NodeId localNode) {
        return fixture(localNode,CONFIG);
    }

    private static Fixture fixture(NodeId localNode,SwimConfig config) {
        ManualTime time = new ManualTime();
        RecordingTransport transport = new RecordingTransport();
        List<MembershipEvent> events = new ArrayList<>();
        SwimFailureDetector detector = new SwimFailureDetector(
                localNode, THREE_NODES, config, transport, time, events::add, 42);
        return new Fixture(detector, time, transport, events);
    }

    private record Fixture(
            SwimFailureDetector detector,
            ManualTime time,
            RecordingTransport transport,
            List<MembershipEvent> events) {}

    // 실시간 sleep 없이 타임아웃 경계를 정확하고 빠르게 테스트하기 위한 가짜 시계다.
    private static final class ManualTime implements TimeSource {
        private long nanos;

        void advance(Duration duration) {
            nanos += duration.toNanos();
        }

        @Override
        public long monotonicNanos() {
            return nanos;
        }

        @Override
        public long epochMillis() {
            return 0;
        }
    }

    // 네트워크를 사용하지 않고 발신 메시지를 순서대로 기록해 테스트에서 검사할 수 있게 한다.
    private static final class RecordingTransport implements Transport {
        private final List<PeerEnvelope> sent = new ArrayList<>();

        @Override
        public CompletionStage<Void> send(PeerEnvelope envelope) {
            sent.add(envelope);
            return CompletableFuture.completedFuture(null);
        }

        PeerEnvelope removeFirst() {
            return sent.removeFirst();
        }
    }
}
