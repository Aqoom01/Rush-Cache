package io.openkache.core.replication.ionia;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaLog.RequestId;
import io.openkache.core.replication.ionia.IoniaMessage.LogEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/** VR consensus selection plus Skyros Figure 6; no network or local storage side effects. */
public final class IoniaRecovery {
    private IoniaRecovery() { }
    public record State(long lastNormalView, List<LogEntry> consensus,
                        List<LogEntry> durability, long commitIndex) {
        public State {
            consensus = List.copyOf(consensus);
            durability = List.copyOf(durability);
            if (lastNormalView < 0 || commitIndex < 0 || commitIndex > consensus.size())
                throw new IllegalArgumentException("invalid recovery bounds");
            IoniaLog validator = new IoniaLog();
            if (!consensus.isEmpty()) validator.accept(new IoniaMessage.Prepare(0, consensus));
            Set<RequestId> unique = new HashSet<>();
            for (LogEntry entry : durability) {
                if (!unique.add(id(entry))) throw new IllegalArgumentException("duplicate durable request");
                validator.record(entry.clientId(), entry.operationId(), entry.command());
            }
        }
    }

    public static State recover(IoniaConfig config, Map<NodeId, State> responses) {
        if (responses.size() != config.orderingQuorumSize()
                || !config.replicas().containsAll(responses.keySet()))
            throw new IllegalArgumentException("a distinct membership majority is required");
        long normal = responses.values().stream().mapToLong(State::lastNormalView).max().orElseThrow();
        State selected = responses.values().stream().filter(state -> state.lastNormalView() == normal)
                .max(Comparator.comparingInt(state -> state.consensus().size())).orElseThrow();
        long committed = responses.values().stream().mapToLong(State::commitIndex).max().orElseThrow();
        if (committed > selected.consensus().size()) throw new IllegalArgumentException("missing committed suffix");
        for (State state : responses.values()) {
            for (int i = 0; i < state.commitIndex(); i++)
                if (!selected.consensus().get(i).equals(state.consensus().get(i)))
                    throw new IllegalArgumentException("conflicting committed prefix");
        }
        List<List<LogEntry>> logs = responses.values().stream()
                .filter(state -> state.lastNormalView() == normal).map(State::durability).toList();
        List<LogEntry> durable = recoverDurability(config.recoveryQuorumSize(), logs);
        List<LogEntry> consensus = new ArrayList<>(selected.consensus());
        Map<RequestId, LogEntry> present = new HashMap<>();
        consensus.forEach(entry -> present.put(id(entry), entry));
        for (LogEntry entry : durable) {
            LogEntry existing = present.get(id(entry));
            if (existing != null) {
                if (!existing.command().equals(entry.command()))
                    throw new IllegalArgumentException("conflicting recovery payload");
            } else {
                LogEntry appended = new LogEntry(consensus.size() + 1L,
                        entry.clientId(), entry.operationId(), entry.command());
                consensus.add(appended);
                present.put(id(entry), appended);
            }
        }
        return new State(normal, consensus, durable, committed);
    }

    public static List<LogEntry> recoverDurability(int quorum, List<List<LogEntry>> logs) {
        if (quorum < 1) throw new IllegalArgumentException("invalid recovery quorum");
        Map<RequestId, LogEntry> entries = new LinkedHashMap<>();
        Map<RequestId, Integer> counts = new HashMap<>();
        List<Map<RequestId, Integer>> positions = new ArrayList<>();
        for (List<LogEntry> log : logs) {
            Map<RequestId, Integer> position = new HashMap<>();
            for (int i = 0; i < log.size(); i++) {
                LogEntry entry = log.get(i);
                RequestId request = id(entry);
                if (position.put(request, i) != null) throw new IllegalArgumentException("duplicate in durable log");
                LogEntry prior = entries.putIfAbsent(request, entry);
                if (prior != null && !prior.command().equals(entry.command()))
                    throw new IllegalArgumentException("request payload differs across replicas");
                counts.merge(request, 1, Integer::sum);
            }
            positions.add(position);
        }
        Set<RequestId> included = new HashSet<>();
        counts.forEach((id, count) -> { if (count >= quorum) included.add(id); });
        Map<RequestId, Set<RequestId>> edges = new HashMap<>();
        Map<RequestId, Integer> incoming = new HashMap<>();
        included.forEach(id -> { edges.put(id, new HashSet<>()); incoming.put(id, 0); });
        for (RequestId a : included) for (RequestId b : included) {
            if (a.equals(b)) continue;
            int beforeOrAbsent = 0;
            for (Map<RequestId, Integer> position : positions) {
                Integer pa = position.get(a), pb = position.get(b);
                if (pa != null && (pb == null || pa < pb)) beforeOrAbsent++;
            }
            if (beforeOrAbsent >= quorum) { edges.get(a).add(b); incoming.merge(b, 1, Integer::sum); }
        }
        Comparator<RequestId> stable = Comparator.comparingLong((RequestId id) -> id.clientId().value())
                .thenComparingLong(id -> id.operationId().value());
        PriorityQueue<RequestId> ready = new PriorityQueue<>(stable);
        incoming.forEach((id, degree) -> { if (degree == 0) ready.add(id); });
        List<LogEntry> result = new ArrayList<>();
        while (!ready.isEmpty()) {
            RequestId next = ready.remove();
            LogEntry entry = entries.get(next);
            result.add(new LogEntry(result.size() + 1L, entry.clientId(), entry.operationId(), entry.command()));
            for (RequestId child : edges.get(next)) if (incoming.merge(child, -1, Integer::sum) == 0) ready.add(child);
        }
        if (result.size() != included.size()) throw new IllegalArgumentException("durability recovery graph has a cycle");
        return List.copyOf(result);
    }

    private static RequestId id(LogEntry entry) { return new RequestId(entry.clientId(), entry.operationId()); }
}
