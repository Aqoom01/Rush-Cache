package io.openkache.core.replication.ionia_third;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.IoniaMessage;
import java.util.*;

/** V02a History. 모든 수정과 snapshot 생성은 owner에서만 한다. 안전 prefix 밖 항목은 보존한다. */
public final class IoniaBudgetHistory {
    public enum Reason { NO_PROGRESS, BELOW_TARGET, NO_SAFE_PREFIX, TRIMMED }
    /** safeThrough는 판단 당시 값이다. eligible이 비어 있으면 계산하지 않은 값이다. */
    public record Decision(long safeThrough, long timeNanos, OptionalLong cutoff, OptionalInt eligible,
                           int removed, Reason reason) { }
    /** 불변 통계. N=0의 overshoot는 목표 초과라는 의미가 없어 0으로 표시한다. */
    public record Snapshot(int targetEntries, int entries, int overshoot, long lastTrimmedIndex,
                           long trimCalls, long removedEntries, Optional<Decision> lastDecision) { }
    private final int target;
    private final Map<Bytes, Long> modified = new HashMap<>();
    private final Map<NodeId, Long> followers = new HashMap<>();
    private long lti, calls, removedEntries;
    private Decision lastDecision;

    public IoniaBudgetHistory(IoniaHistorySettings settings) { target = Objects.requireNonNull(settings).targetEntries(); }
    public void modified(Bytes key, long index) {
        if (index < 1) throw new IllegalArgumentException("index must be positive");
        modified.merge(Objects.requireNonNull(key), index, Math::max);
    }
    public long index(Bytes key) { return modified.getOrDefault(key, lti); }
    public long lastTrimmedIndex() { return lti; }
    public int size() { return modified.size(); }
    public void report(NodeId follower, long applied) {
        if (applied < 0) throw new IllegalArgumentException("negative applied index");
        followers.merge(Objects.requireNonNull(follower), applied, Math::max);
    }

    /** 1초 owner tick에서 호출한다. N=0은 원본 trim, N>0은 필요한 최소 prefix만 정리한다. */
    public void trim(Set<NodeId> active, long localApplied, long nowNanos) {
        if (localApplied < 0) throw new IllegalArgumentException("negative local applied index");
        long safe = localApplied;
        for (NodeId follower : active) safe = Math.min(safe, followers.getOrDefault(follower, 0L));
        calls++;
        if (safe <= lti) { decide(safe, nowNanos, OptionalLong.empty(), OptionalInt.empty(), 0, Reason.NO_PROGRESS); return; }
        if (target > 0 && modified.size() <= target) {
            decide(safe, nowNanos, OptionalLong.empty(), OptionalInt.empty(), 0, Reason.BELOW_TARGET); return;
        }
        long cutoff = safe;
        OptionalInt eligible = OptionalInt.empty();
        if (target > 0) {
            long bound = safe;
            long[] indices = modified.values().stream().mapToLong(Long::longValue).filter(i -> i <= bound).sorted().toArray();
            eligible = OptionalInt.of(indices.length);
            int count = Math.min(modified.size() - target, indices.length);
            if (count == 0) { decide(safe, nowNanos, OptionalLong.empty(), eligible, 0, Reason.NO_SAFE_PREFIX); return; }
            cutoff = indices[count - 1];
        }
        int before = modified.size();
        lti = Math.max(lti, cutoff);
        long through = cutoff;
        modified.values().removeIf(index -> index <= through);
        int removed = before - modified.size();
        removedEntries += removed;
        decide(safe, nowNanos, OptionalLong.of(cutoff), eligible, removed, Reason.TRIMMED);
    }
    private void decide(long safe, long now, OptionalLong cutoff, OptionalInt eligible, int removed, Reason reason) {
        lastDecision = new Decision(safe, now, cutoff, eligible, removed, reason);
    }
    /** 새 generation은 원본처럼 재구성하며 이전 follower 보고와 진단을 초기화한다. */
    public void rebuild(List<IoniaMessage.LogEntry> entries) {
        modified.clear(); followers.clear();
        lti = entries.isEmpty() ? 0 : entries.getFirst().index();
        entries.forEach(entry -> modified(entry.command().key(), entry.index()));
        calls = removedEntries = 0; lastDecision = null;
    }
    public Snapshot snapshot() {
        return new Snapshot(target, modified.size(), target == 0 ? 0 : Math.max(0, modified.size() - target),
                lti, calls, removedEntries, Optional.ofNullable(lastDecision));
    }
}
