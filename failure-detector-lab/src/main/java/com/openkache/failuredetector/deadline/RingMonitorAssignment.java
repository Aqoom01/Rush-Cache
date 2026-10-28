package com.openkache.failuredetector.deadline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Deterministic membership-view to successor-monitor mapping used by F5/F6. */
public final class RingMonitorAssignment {
    private final int neighborCount;
    private List<String> members;

    public RingMonitorAssignment(Collection<String> members, int neighborCount) {
        if (neighborCount < 1) {
            throw new IllegalArgumentException("neighborCount must be positive");
        }
        this.neighborCount = neighborCount;
        updateView(members);
    }

    public void updateView(Collection<String> nextMembers) {
        if (nextMembers == null || nextMembers.size() < 2) {
            throw new IllegalArgumentException("ring view needs at least two members");
        }
        List<String> sorted = nextMembers.stream()
                .peek(member -> {
                    if (member == null || member.isBlank()) {
                        throw new IllegalArgumentException("member must not be blank");
                    }
                })
                .distinct()
                .sorted(Comparator.naturalOrder())
                .toList();
        if (sorted.size() != nextMembers.size()) {
            throw new IllegalArgumentException("ring view must not contain duplicate members");
        }
        members = List.copyOf(sorted);
    }

    public List<String> monitoredBy(String observer) {
        int observerIndex = members.indexOf(observer);
        if (observerIndex < 0) {
            throw new IllegalArgumentException("observer is not in the membership view: " + observer);
        }
        int count = Math.min(neighborCount, members.size() - 1);
        List<String> monitored = new ArrayList<>(count);
        for (int offset = 1; offset <= count; offset++) {
            monitored.add(members.get((observerIndex + offset) % members.size()));
        }
        return List.copyOf(monitored);
    }

    public Map<String, List<String>> assignments() {
        Map<String, List<String>> result = new LinkedHashMap<>();
        for (String member : members) {
            result.put(member, monitoredBy(member));
        }
        return Map.copyOf(result);
    }

    public List<String> members() {
        return members;
    }
}
