package io.openkache.core.failure;

import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.TimeSource;
import io.openkache.core.spi.Transport;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * SWIM baseline failure detector의 순수 상태 머신.
 *
 * <p>caller는 {@link #start()} 후 다음 timer 시점마다 {@link #tick()}을 호출하고, 수신 메시지는
 * {@link #onMessage(NodeId, SwimMessage)}로 전달한다. 이 클래스는 thread-safe하지 않으며 모든 메서드는 하나의 owner
 * thread에서 호출해야 한다. 네트워크 전송은 block하지 않는 {@link Transport}에 위임한다.
 *
 * <p>구성원 목록은 기동 시 고정한다. 동적 join/leave, UDP adapter와 Lifeguard local-health awareness는 이 baseline의 범위가
 * 아니다.
 */
public final class SwimFailureDetector {

    private enum ProbeStage {
        DIRECT,
        INDIRECT
    }

    // ACK을 기다리고 있는 정보
    private record ProbeState(long id, NodeId target, ProbeStage stage, long deadlineNanos) {}

    private static final class GossipEntry {
        // 전파할 정보
        private final MembershipUpdate update;
        // 앞으로 몇 번 더 전파할지
        private int remaining;

        GossipEntry(MembershipUpdate update, int remaining) {
            this.update = update;
            this.remaining = remaining;
        }
    }

    // SwimFailureDetector가 실행되고 있는 노드
    private final NodeId localNode;
    private final SwimConfig config;
    private final Transport transport;
    private final TimeSource timeSource;
    // 상태 변경 이벤트를 기능 1,2에 넘겨주는 객체
    private final MembershipListener listener;
    private final Random random;
    // 전체 노드의 멤버쉽 정보
    private final Map<NodeId, MemberRecord> membershipInfos = new HashMap<>();
    // suspect -> dead 타이머
    private final Map<NodeId, Long> suspicionDeadlines = new HashMap<>();
    // 다른 멤버들에게 gossip으로 전파해야할 정보들
    private final Map<NodeId, GossipEntry> gossip = new LinkedHashMap<>();
    // 앞으로 probe 할 노드의 순서 (한번 probe한 건 targetOrder에서 제외됨)
    private final ArrayDeque<NodeId> targetOrder = new ArrayDeque<>();

    private boolean started;
    // 다음 주기 예정 시작 시각
    private long nextProbeAtNanos;
    // 다음 probe에 부여할 ID
    private long nextProbeId;

    // 다음 pending probe에 부여할 ID
    private ProbeState probeState;

    /**
     * 고정 membership으로 detector를 만든다.
     *
     * @param clusterMembers 자기 자신을 포함한 구성원 목록
     * @param randomSeed probe 대상과 helper 선택을 재현하기 위한 seed
     */
    public SwimFailureDetector(
            NodeId localNode,
            Collection<NodeId> clusterMembers,
            SwimConfig config,
            Transport transport,
            TimeSource timeSource,
            MembershipListener listener,
            long randomSeed) {
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.config = Objects.requireNonNull(config, "config");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
        this.listener = Objects.requireNonNull(listener, "listener");
        this.random = new Random(randomSeed);

        LinkedHashSet<NodeId> uniqueMembers =
                new LinkedHashSet<>(Objects.requireNonNull(clusterMembers, "clusterMembers"));
        if (!uniqueMembers.contains(localNode)) {
            throw new IllegalArgumentException("clusterMembers must contain localNode " + localNode);
        }
        long now = timeSource.monotonicNanos();
        for (NodeId member : uniqueMembers) {
            membershipInfos.put(Objects.requireNonNull(member, "cluster member"), new MemberRecord(member, 0, MemberStatus.ALIVE, now));
        }
    }

    /** detector를 활성화한다.*/
    public void start() {
        if (started) {
            return;
        }
        started = true;
        nextProbeAtNanos = timeSource.monotonicNanos();
    }

    /**
     * 현재 monotonic 시각까지 만료된 probe와 suspicion을 처리하고, 필요하면 다음 probe를 시작한다.
     *
     * <p>실제 runtime은 고정 주기로 호출하거나 다음 deadline에 맞춰 호출할 수 있다.
     */
    public void tick() {
        requireStarted();
        long now = timeSource.monotonicNanos();
        expireSuspicions(now);
        checkProbeTimeout(now);

        // 만약에 다음 주기를 실행할 차례가 되었다면?
        if (probeState == null && isTimedOut(now, nextProbeAtNanos)) {
            // probing 시작 및 다음 주기까지의 시간 갱신
            startDirectProbe(now);
            nextProbeAtNanos = now + config.probeInterval().toNanos();
        }
    }

    /** 수신한 SWIM 메시지를 처리한다.*/
    public void onMessage(NodeId from, SwimMessage message) {
        requireStarted();
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(message, "message");
        if (!membershipInfos.containsKey(from)) {
            return;
        }

        for (MembershipUpdate update : message.updates()) {
            processMembershipUpdate(update);
        }

        switch (message) {
            case SwimMessage.Ping ping -> onPing(from, ping);
            case SwimMessage.Ack ack -> onAck(ack);
            case SwimMessage.PingRequest request -> onPingRequest(request);
        }
    }

    /** 어느 thread에서나 읽어 전달할 수 있는 불변 membership 복사본. */
    public Map<NodeId, MemberRecord> snapshot() {
        return Map.copyOf(membershipInfos);
    }

    /** 한 구성원의 현재 로컬 관측. 고정 membership에 없는 노드는 empty다. */
    public Optional<MemberRecord> member(NodeId nodeId) {
        return Optional.ofNullable(membershipInfos.get(Objects.requireNonNull(nodeId, "nodeId")));
    }

    /** localNode가 ping을 받았을 때 처리하는 메서드 */
    private void onPing(NodeId from, SwimMessage.Ping ping) {
        // ping을 받은 노드에게 ACK을 보냄
        send(from, new SwimMessage.Ack(ping.probeId(), localNode, ping.origin(), chooseUpdates()));
    }

    /** localNode가 ACK을 받았을 때 처리하는 메서드 */
    private void onAck(SwimMessage.Ack ack) {
        // 만약에 이 ack의 근원이 본인이 아니다 -> 간접 probe이다.
        if (!ack.origin().equals(localNode)) {
            // 간접 probe를 요청한 origin에게도 ack을 보내줌
            send(ack.origin(), new SwimMessage.Ack(ack.probeId(), ack.target(), ack.origin(), chooseUpdates()));
            return;
        }
        // 만약에 이 ACK의 origin이 본인이다 -> 정상적인 ACK인 경우 다음 probe period가 시작될 때까지 아무것도 안함 (의심할 게 없으니까)
        if (probeState == null
                || probeState.id() != ack.probeId()
                || !probeState.target().equals(ack.target())) {
            return;
        }
        probeState = null;
    }
    /** localNode가 간접 probe를 요청 받았을 때 처리하는 메서드 */
    private void onPingRequest(SwimMessage.PingRequest request) {
        // 간접 probe의 대상이 본인이거나, 간접 probe를 요청한 게 본인이라면 그냥 pass
        if (request.target().equals(localNode) || request.origin().equals(localNode)) {
            return;
        }
        // 간접 probe를 요청 받은 target에게 ping을 보냄
        send(request.target(), new SwimMessage.Ping(request.probeId(), request.origin(), chooseUpdates()));
    }
    /** probe timed out에 따라서 probeState를 처리하는 메서드 */
    private void checkProbeTimeout(long now) {
        // 의심할 게 없거나 timed out이 아직 안됐으면 pass
        if (probeState == null || !isTimedOut(now, probeState.deadlineNanos())) {
            return;
        }
        // timed out이 됐는데 현재 상태가 직접 probing이었다면 간접 probing 시작
        if (probeState.stage() == ProbeStage.DIRECT) {
            startIndirectProbe(now);
        }
        // 간접 probing이라면 의심단계로 설정
        else {
            NodeId target = probeState.target();
            probeState = null;
            markSuspect(target);
        }
    }

    /** 직접 probe 시작하는 메서드 */
    private void startDirectProbe(long now) {
        Optional<NodeId> target = nextTarget();
        if (target.isEmpty()) {
            return;
        }
        long probeId = nextProbeId;
        nextProbeId = Math.incrementExact(nextProbeId);
        probeState = new ProbeState(
                probeId,
                target.orElseThrow(),
                ProbeStage.DIRECT,
                now + config.directProbeTimeout().toNanos());
        send(target.orElseThrow(), new SwimMessage.Ping(probeId, localNode, chooseUpdates()));
    }
    /** 간접 probe 시작하는 메서드 */
    private void startIndirectProbe(long now) {
        ProbeState direct = probeState;
        List<NodeId> helpers = selectHelpers(direct.target());
        if (helpers.isEmpty()) {
            probeState = null;
            markSuspect(direct.target());
            return;
        }

        probeState = new ProbeState(
                direct.id(),
                direct.target(),
                ProbeStage.INDIRECT,
                now + config.indirectProbeTimeout().toNanos());
        for (NodeId helper : helpers) {
            send(helper, new SwimMessage.PingRequest(direct.id(), localNode, direct.target(), chooseUpdates()));
        }
    }

    /** 이번에 직접 probe할 노드를 반환해주는 메서드 */
    private Optional<NodeId> nextTarget() {
        while (true) {
            while (!targetOrder.isEmpty()) {
                // 만약에 다음 후보가 남아있고 해당 후보가 죽은 노드가 아니라면 직접 probing으로 선정
                NodeId candidate = targetOrder.removeFirst();
                MemberRecord record = membershipInfos.get(candidate);
                if (record != null && record.status() != MemberStatus.DEAD) {
                    return Optional.of(candidate);
                }
            }
            // 만약에 직접 probing할 대상을 못 찾은 경우라면? -> 앞전에 선정했었던 후보들을 다 한번씩 순회한 것으로 판단 -> 다시 리셋
            List<NodeId> candidates = membershipInfos.values().stream()
                    .filter(record -> !record.nodeId().equals(localNode))
                    .filter(record -> record.status() != MemberStatus.DEAD)
                    .map(MemberRecord::nodeId)
                    .collect(Collectors.toCollection(ArrayList::new));
            if (candidates.isEmpty()) {
                return Optional.empty();
            }
            // 후보군을 리셋해서 다시 섞음
            Collections.shuffle(candidates, random);
            targetOrder.addAll(candidates);
        }
    }

    /** 간접 probing을 요청할 helper들을 고르는 메서드*/
    private List<NodeId> selectHelpers(NodeId target) {
        List<NodeId> candidates = membershipInfos.values().stream()
                .filter(record -> !record.nodeId().equals(localNode))
                .filter(record -> !record.nodeId().equals(target))
                .filter(record -> record.status() != MemberStatus.DEAD)
                .map(MemberRecord::nodeId)
                .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(candidates, random);
        int count = Math.min(config.indirectChecks(), candidates.size());
        return List.copyOf(candidates.subList(0, count));
    }
    /** suspect로 멤버쉽을 업데이트하는 메서드*/
    private void markSuspect(NodeId target) {
        MemberRecord current = membershipInfos.get(target);
        if (current == null || current.status() != MemberStatus.ALIVE) {
            return;
        }
        processMembershipUpdate(new MembershipUpdate(target, current.incarnation(), MemberStatus.SUSPECT, localNode));
    }
    /** suspect에서 timed out이 된 게 있는지 확인하는 메서드*/
    private void expireSuspicions(long now) {
        List<NodeId> expired = suspicionDeadlines.entrySet().stream()
                .filter(entry -> isTimedOut(now, entry.getValue()))
                .map(Map.Entry::getKey)
                .toList();
        // suspect 단계에서도 timed out이 되었기 때문에 이 노드들을 DEAD 처리함
        for (NodeId node : expired) {
            MemberRecord current = membershipInfos.get(node);
            if (current != null && current.status() == MemberStatus.SUSPECT) {
                processMembershipUpdate(new MembershipUpdate(node, current.incarnation(), MemberStatus.DEAD, localNode));
            } else {
                suspicionDeadlines.remove(node);
            }
        }
    }

    // 받은 메세지에 따라서 membership을 업데이트함
    private void processMembershipUpdate(MembershipUpdate update) {
        // localNode가 알고 있는 이번 변경 대상의 상태를 가져옴
        MemberRecord current = membershipInfos.get(update.member());
        if (current == null || !membershipInfos.containsKey(update.reporter())) {
            return;
        }
        // 만약에 이번 변경 대상이 본인이고, 본인이 의심받는 상황이라면 ALIVE 전파
        if (update.member().equals(localNode) && update.status() == MemberStatus.SUSPECT) {
            refuteIfCurrent(update, current);
            return;
        }
        // 새로운 정보와 기존 정보를 우선순위에 따라 비교해서, 기존 정보를 유지해야할지 결정
        if (!supersedes(update, current)) {
            return;
        }

        long now = timeSource.monotonicNanos();
        MemberRecord replacement =
                new MemberRecord(update.member(), update.incarnation(), update.status(), now);
        membershipInfos.put(update.member(), replacement);
        addUpdate(update);

        if (replacement.status() == MemberStatus.SUSPECT) {
            suspicionDeadlines.put(
                    replacement.nodeId(), now + config.suspicionTimeout().toNanos());
        } else {
            suspicionDeadlines.remove(replacement.nodeId());
        }
        if (current.status() != replacement.status()) {
            listener.onMembershipChanged(new MembershipEvent(
                    replacement.nodeId(),
                    current.status(),
                    replacement.status(),
                    replacement.incarnation(),
                    now));
        }
    }

    // 본인이 의심받고 있는 상황에서 incarnation을 1 증가시키고 ALIVE를 전파함
    private void refuteIfCurrent(MembershipUpdate update, MemberRecord current) {
        if (update.incarnation() < current.incarnation()) {
            return;
        }
        // incarnation 1 증가
        long newIncarnation = Math.incrementExact(update.incarnation());
        long now = timeSource.monotonicNanos();
        // 본인의 정보를 최신화
        membershipInfos.put(localNode, new MemberRecord(localNode, newIncarnation, MemberStatus.ALIVE, now));
        suspicionDeadlines.remove(localNode);
        addUpdate(new MembershipUpdate(localNode, newIncarnation, MemberStatus.ALIVE, localNode));
    }

    // 우선순위를 따져서, 만약에 이번에 업데이트를 반영해야한다면 true를 리턴
    private static boolean supersedes(MembershipUpdate update, MemberRecord current) {
        if (update.incarnation() != current.incarnation()) {
            return update.incarnation() > current.incarnation();
        }
        // 만약 버전이 같으면 ALIVE -> SUSPECT -> DEAD
        return precedence(update.status()) > precedence(current.status());
    }

    // 만약 버전이 같으면 ALIVE -> SUSPECT -> DEAD
    private static int precedence(MemberStatus status) {
        return switch (status) {
            case ALIVE -> 0;
            case SUSPECT -> 1;
            case DEAD -> 2;
        };
    }

    // 새로운 메세지를 updates에 추가
    private void addUpdate(MembershipUpdate update) {
        // 재전파할 횟수를 결정
        int respread = Math.max(
                1,
                (int) Math.ceil(config.gossipRetransmitMultiplier() * Math.log(membershipInfos.size() + 1.0)));
        // 옛날 버전의 정보 제거
        gossip.remove(update.member());
        // 새로운 정보를 추가
        gossip.put(update.member(), new GossipEntry(update, respread));
    }

    // remaining과 maxPiggybackUpdates를 고려하여 메세지에 추가할 updates를 선정함
    private List<MembershipUpdate> chooseUpdates() {
        if (gossip.isEmpty()) {
            return List.of();
        }

        List<GossipEntry> candidates =
                new ArrayList<>(gossip.values());

        // 남은 전파 횟수가 많은 업데이트 우선
        candidates.sort(
                (a, b) -> Integer.compare(b.remaining, a.remaining)
        );

        List<MembershipUpdate> selected = new ArrayList<>();

        for (GossipEntry entry : candidates) {
            if (selected.size() >= config.maxPiggybackUpdates()) {
                break;
            }

            selected.add(entry.update);
            entry.remaining--;
        }

        gossip.values().removeIf(entry -> entry.remaining == 0);

        return List.copyOf(selected);
    }

    private void send(NodeId to, SwimMessage message) {
        transport.send(new PeerEnvelope(localNode, to, message));
    }

    // 탐지기가 시작됐는지 확인
    private void requireStarted() {
        if (!started) {
            throw new IllegalStateException("실패 감지기가 아직 시작되지 않았습니다.");
        }
    }

    // 타임아웃이 됐는지 확인
    private static boolean isTimedOut(long now, long deadline) {
        return now - deadline >= 0;
    }
}
