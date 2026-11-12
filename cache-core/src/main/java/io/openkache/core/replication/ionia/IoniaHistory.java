package io.openkache.core.replication.ionia;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Owner-confined IONIA history. Absent keys return the conservative last-trimmed index. */
public final class IoniaHistory {
    private final Map<Bytes, Long> modified = new HashMap<>();
    private final Map<NodeId, Long> followers = new HashMap<>();
    private long lti;

    public void modified(Bytes key, long index) {
        if (index < 1) throw new IllegalArgumentException("index must be positive");
        modified.merge(key, index, Math::max);
    }

    public long index(Bytes key) { return modified.getOrDefault(key, lti); }
    public long lastTrimmedIndex() { return lti; }
    public int size() { return modified.size(); }
    public void report(NodeId follower, long applied) {
        if (applied < 0) throw new IllegalArgumentException("negative applied index");
        followers.merge(follower, applied, Math::max);
    }

    /** Called lazily, using only active followers; local contiguous apply also bounds trimming. */
    public void trim(Set<NodeId> active, long localApplied) {
        long through = localApplied;
        for (NodeId follower : active) through = Math.min(through, followers.getOrDefault(follower, 0L));
        if (through <= lti) return;
        lti = through;
        long limit = through;
        modified.values().removeIf(index -> index <= limit);
    }

    public void rebuild(java.util.List<IoniaMessage.LogEntry> entries) {
        modified.clear();
        followers.clear();
        lti = entries.isEmpty() ? 0 : entries.getFirst().index();
        entries.forEach(entry -> modified(IoniaCommands.key(entry.command()), entry.index()));
    }
}
