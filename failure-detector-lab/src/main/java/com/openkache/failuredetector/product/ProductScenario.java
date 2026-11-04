package com.openkache.failuredetector.product;

import java.util.Arrays;

public enum ProductScenario {
    NORMAL("normal", 0),
    CRASH("crash", 1),
    LOSS_15("loss15", 0),
    LOSS_45("loss45", 0),
    DELAY_JITTER("delay-jitter", 0),
    TARGET_STALL("target-stall", 0),
    OBSERVER_STALL("observer-stall", 0),
    CONSECUTIVE_3("consecutive-3", 3),
    CONSECUTIVE_4("consecutive-4", 4),
    PARTITION("partition", 0);

    private final String id;
    private final int crashedTargets;

    ProductScenario(String id, int crashedTargets) {
        this.id = id;
        this.crashedTargets = crashedTargets;
    }

    public String id() {
        return id;
    }

    public int crashedTargets() {
        return crashedTargets;
    }

    public boolean expectsCrashDetection() {
        return crashedTargets > 0;
    }

    public static ProductScenario fromId(String value) {
        return Arrays.stream(values())
                .filter(scenario -> scenario.id.equalsIgnoreCase(value.trim()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown scenario: " + value));
    }
}
