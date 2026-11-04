package com.openkache.failuredetector.experiment;

public enum ScenarioType {
    NORMAL("normal"),
    CRASH("crash"),
    CRASH_UNDER_JITTER("crash-under-jitter"),
    PACKET_LOSS("packet-loss"),
    PACKET_LOSS_BURST("packet-loss-burst"),
    LATENCY("latency"),
    LATENCY_GRADUAL("latency-gradual"),
    LATENCY_BURSTY("latency-bursty"),
    LATENCY_REGIME("latency-regime"),
    OBSERVER_PAUSE("observer-pause"),
    TARGET_PAUSE("target-pause"),
    PARTITION("partition");

    private final String id;

    ScenarioType(String id) {
        this.id = id;
    }

    public String id() {
        return id;
    }

    public static ScenarioType fromId(String value) {
        for (ScenarioType type : values()) {
            if (type.id.equalsIgnoreCase(value.trim())) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown scenario: " + value);
    }
}
