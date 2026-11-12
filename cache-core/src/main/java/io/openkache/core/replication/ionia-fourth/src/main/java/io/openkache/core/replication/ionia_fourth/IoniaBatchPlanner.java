package io.openkache.core.replication.ionia_fourth;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia.IoniaMessage.LogEntry;
import java.util.*;

/** V03 owner 전용 선택기. matched/commit은 소유하지 않으며 local send를 ACK로 취급하지 않는다. */
public final class IoniaBatchPlanner {
    /** 겹친 조건은 RETRY > URGENT > FULL > ZERO_DELAY > AGE 순서로 한 번만 집계한다. */
    public enum Cause { RETRY, URGENT, FULL, AGE, ZERO_DELAY }
    public record Flight(long start, long end, long firstSendNanos, long lastSendNanos) { }
    public record Batch(List<LogEntry> entries, long encodedBytes, long oldestAgeNanos, boolean oversized, Cause cause) {
        public Batch { entries = List.copyOf(entries); }
    }
    public record PeerState(long matched, Optional<Flight> flight, OptionalLong oldestOrderedAgeNanos) { }
    /** 누적 시도 통계는 engine 시작 이후, flight/age는 현재 installation 기준. 불변 snapshot이다. */
    public record Snapshot(long attempts, long entries, long encodedBytes, long retries, long oversized,
                           long urgentThrough, Map<Cause, Long> causes, Map<NodeId, PeerState> peers) {
        public Snapshot { causes = Map.copyOf(causes); peers = Map.copyOf(peers); }
    }
    private final IoniaBatchSettings settings;
    private final IoniaPrepareSizer sizer;
    private final Map<Long, Long> orderedAt = new HashMap<>();
    private final Map<NodeId, Flight> flights = new HashMap<>();
    private final Map<NodeId, NavigableMap<Long, Long>> firstSends = new HashMap<>();
    private final EnumMap<Cause, Long> causes = new EnumMap<>(Cause.class);
    private long urgentThrough, attempts, entries, encodedBytes, retries, oversized;
    public IoniaBatchPlanner(IoniaBatchSettings settings, IoniaPrepareSizer sizer) {
        this.settings = Objects.requireNonNull(settings); this.sizer = Objects.requireNonNull(sizer);
    }
    /** owner에서 새 ordering 때만 호출한다. 재시도/중복은 첫 시각을 보존한다. */
    public void ordered(long index, long now) { orderedAt.putIfAbsent(index, now); }
    public void urgent(long through) { urgentThrough = Math.max(urgentThrough, through); }
    public void clear() { flights.clear(); firstSends.clear(); orderedAt.clear(); urgentThrough = 0; }
    /** authoritative log 설치 후 사용한다. 복구된 tail은 age 대기 없이 진행한다. */
    public void installed(IoniaLog log, long now) {
        clear();
        for (long i = 1; i <= log.lastLogIndex(); i++) ordered(i, now);
        urgent(log.lastLogIndex());
    }
    public void acknowledged(NodeId peer, long through) {
        NavigableMap<Long, Long> sent = firstSends.get(peer);
        if (sent != null) { sent.headMap(through, true).clear(); if (sent.isEmpty()) firstSends.remove(peer); }
        Flight flight = flights.get(peer);
        if (flight != null && through >= flight.end()) flights.remove(peer);
    }
    /** matched+1부터만 선택한다. 논리 flight 하나와 재전송 주기를 지키며 전체 suffix를 복사하지 않는다. */
    public Optional<Batch> select(NodeId peer, long matched, IoniaLog log, long now) {
        if (matched >= log.lastLogIndex()) return Optional.empty();
        Flight flight = flights.get(peer);
        if (flight != null && now - flight.lastSendNanos() < IoniaBatchSettings.RETRY_NANOS) return Optional.empty();
        long start = matched + 1, bytes = sizer.headerBytes();
        List<LogEntry> chunk = new ArrayList<>(); boolean full = false;
        for (long i = start; i <= log.lastLogIndex() && chunk.size() < settings.maxEntries(); i++) {
            LogEntry entry = log.entry(i).orElseThrow(); long candidate = bytes + sizer.entryBytes(entry.command());
            if (candidate > IoniaBatchSettings.MAX_PACKET_BYTES) {
                if (chunk.isEmpty()) throw new IllegalStateException("inadmissible recorded entry");
                full = true; break;
            }
            if (!chunk.isEmpty() && candidate > settings.maxEncodedBytes()) { full = true; break; }
            chunk.add(entry); bytes = candidate;
            if (bytes >= settings.maxEncodedBytes()) { full = true; break; }
        }
        if (chunk.isEmpty()) return Optional.empty();
        full |= chunk.size() == settings.maxEntries();
        Long first = orderedAt.get(start);
        if (first == null) throw new IllegalStateException("missing ordering timestamp");
        long age = now - first;
        Cause cause = flight != null ? Cause.RETRY : start <= urgentThrough ? Cause.URGENT : full ? Cause.FULL
                : settings.maxDelayNanos() == 0 ? Cause.ZERO_DELAY : age >= settings.maxDelayNanos() ? Cause.AGE : null;
        if (cause == null) return Optional.empty();
        boolean above = bytes > settings.maxEncodedBytes();
        NavigableMap<Long, Long> sent = firstSends.computeIfAbsent(peer, ignored -> new TreeMap<>());
        for (LogEntry entry : chunk) sent.putIfAbsent(entry.index(), now);
        flights.put(peer, new Flight(start, chunk.getLast().index(), sent.get(start), now));
        attempts++; entries += chunk.size(); encodedBytes += bytes; if (cause == Cause.RETRY) retries++; if (above) oversized++;
        causes.merge(cause, 1L, Long::sum);
        return Optional.of(new Batch(chunk, bytes, age, above, cause));
    }
    public Snapshot snapshot(Map<NodeId, Long> peerMatched, long lastIndex, long now) {
        Map<NodeId, PeerState> peers = new HashMap<>();
        peerMatched.forEach((node, match) -> {
            Long first = match < lastIndex ? orderedAt.get(match + 1) : null;
            peers.put(node, new PeerState(match, Optional.ofNullable(flights.get(node)),
                    first == null ? OptionalLong.empty() : OptionalLong.of(now - first)));
        });
        return new Snapshot(attempts, entries, encodedBytes, retries, oversized, urgentThrough, causes, peers);
    }
}
