package com.openkache.failuredetector.experiment;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Per-node remove-only membership view with deterministic version ordering. */
public final class VersionedMembershipView {
    private final List<String> originalMembers;
    private final Map<String, VersionStamp> removalTombstones = new LinkedHashMap<>();
    private int viewVersion = 1;

    public VersionedMembershipView(Collection<String> members) {
        if (members == null || members.size() < 2) {
            throw new IllegalArgumentException("membership view needs at least two members");
        }
        List<String> sorted = members.stream()
                .peek(member -> {
                    if (member == null || member.isBlank()) {
                        throw new IllegalArgumentException("member must not be blank");
                    }
                })
                .distinct()
                .sorted(Comparator.naturalOrder())
                .toList();
        if (sorted.size() != members.size()) {
            throw new IllegalArgumentException("membership view must not contain duplicates");
        }
        originalMembers = List.copyOf(sorted);
    }

    public ApplyOutcome applyRemoval(String target, VersionStamp stamp) {
        if (!originalMembers.contains(target)) {
            throw new IllegalArgumentException("target is outside the original membership");
        }
        if (stamp == null) {
            throw new IllegalArgumentException("version stamp is required");
        }
        VersionStamp previous = removalTombstones.get(target);
        if (previous != null && stamp.compareTo(previous) <= 0) {
            return ApplyOutcome.STALE_OR_DUPLICATE;
        }
        removalTombstones.put(target, stamp);
        if (previous == null) {
            viewVersion++;
            return ApplyOutcome.MEMBER_REMOVED;
        }
        return ApplyOutcome.TOMBSTONE_ADVANCED;
    }

    public List<String> members() {
        return originalMembers.stream()
                .filter(member -> !removalTombstones.containsKey(member))
                .toList();
    }

    public boolean contains(String member) {
        return originalMembers.contains(member) && !removalTombstones.containsKey(member);
    }

    public int viewVersion() {
        return viewVersion;
    }

    public VersionStamp removalStamp(String target) {
        return removalTombstones.get(target);
    }

    public enum ApplyOutcome {
        MEMBER_REMOVED,
        TOMBSTONE_ADVANCED,
        STALE_OR_DUPLICATE
    }

    public record VersionStamp(long logicalTime, String origin) implements Comparable<VersionStamp> {
        public VersionStamp {
            if (logicalTime < 1 || origin == null || origin.isBlank()) {
                throw new IllegalArgumentException("version stamp must be positive and named");
            }
        }

        @Override
        public int compareTo(VersionStamp other) {
            int timeOrder = Long.compare(logicalTime, other.logicalTime);
            return timeOrder != 0 ? timeOrder : origin.compareTo(other.origin);
        }
    }
}
