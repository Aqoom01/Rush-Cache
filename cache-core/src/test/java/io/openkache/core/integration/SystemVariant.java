package io.openkache.core.integration;

import io.openkache.core.balance.placement.*;
import io.openkache.core.balance.split.*;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.ArrayList;
import java.util.List;

/** 통합 연구 fixture에서 실제 연결된 조합만 열거한다. 모든 설정은 불변이다. */
public record SystemVariant(String replication, String detector, String distribution,
                            String split, String placement, String copy) {
    public SystemVariant {
        if (!List.of("PB", "RAFT").contains(replication)
                || !List.of("SWIM", "LIFEGUARD").contains(detector)
                || !List.of("RANGE", "SLOT").contains(distribution)
                || !List.of("NONE", "MIDPOINT", "HEAT", "ESTORE").contains(split)
                || !List.of("GREEDY", "HYSTERESIS", "COST").contains(placement)
                || !List.of("STOP", "PRE").contains(copy)) {
            throw new IllegalArgumentException("미지원 통합 전략");
        }
        if (distribution.equals("SLOT") && !split.equals("NONE")) {
            throw new IllegalArgumentException("fixed-slot은 split을 지원하지 않는다");
        }
    }

    public String id() { return String.join("-", replication, detector, distribution, split, placement, copy); }
    DistributionScheme scheme() {
        return distribution.equals("RANGE") ? new HashRangeScheme(4) : new FixedSlotScheme(8);
    }
    SplitPolicy splitter() {
        return switch (split) {
            case "MIDPOINT" -> new MidpointSplitPolicy();
            case "HEAT" -> new HeatSplitPolicy();
            case "ESTORE" -> new EStoreSplitPolicy();
            default -> throw new IllegalStateException("split disabled");
        };
    }
    PlacementPolicy placer() {
        return switch (placement) {
            case "GREEDY" -> new GreedyLoadPlacement();
            case "HYSTERESIS" -> new HysteresisRebalancing();
            case "COST" -> new CostAwareRebalancing();
            default -> throw new IllegalStateException();
        };
    }
    public static List<SystemVariant> all() {
        List<SystemVariant> result = new ArrayList<>();
        for (String r : List.of("PB", "RAFT"))
            for (String d : List.of("SWIM", "LIFEGUARD"))
                for (String b : List.of("RANGE", "SLOT"))
                    for (String s : b.equals("RANGE") ? List.of("NONE", "MIDPOINT", "HEAT", "ESTORE") : List.of("NONE"))
                        for (String p : List.of("GREEDY", "HYSTERESIS", "COST"))
                            for (String c : List.of("STOP", "PRE")) result.add(new SystemVariant(r, d, b, s, p, c));
        return List.copyOf(result);
    }
}
