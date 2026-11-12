package com.openkache.failuredetector.swim;

import java.time.Duration;

public record SwimLifeguardConfig(
        Duration probeInterval,
        Duration probeTimeout,
        int indirectChecks,
        double suspicionMultiplier,
        double suspicionMaxTimeoutMultiplier,
        int maxConfirmations,
        int awarenessMaxMultiplier,
        boolean lhaProbeEnabled,
        boolean lhaSuspicionEnabled,
        boolean suspicionConfirmationsEnabled,
        boolean buddySystemEnabled
) {
    public SwimLifeguardConfig(
            Duration probeInterval,
            Duration probeTimeout,
            int indirectChecks,
            double suspicionMultiplier,
            double suspicionMaxTimeoutMultiplier,
            int maxConfirmations,
            int awarenessMaxMultiplier
    ) {
        this(
                probeInterval,
                probeTimeout,
                indirectChecks,
                suspicionMultiplier,
                suspicionMaxTimeoutMultiplier,
                maxConfirmations,
                awarenessMaxMultiplier,
                true,
                true,
                true,
                false
        );
    }

    public SwimLifeguardConfig(
            Duration probeInterval,
            Duration probeTimeout,
            int indirectChecks,
            double suspicionMultiplier,
            double suspicionMaxTimeoutMultiplier,
            int maxConfirmations,
            int awarenessMaxMultiplier,
            boolean localHealthAwarenessEnabled,
            boolean suspicionConfirmationsEnabled
    ) {
        this(
                probeInterval,
                probeTimeout,
                indirectChecks,
                suspicionMultiplier,
                suspicionMaxTimeoutMultiplier,
                maxConfirmations,
                awarenessMaxMultiplier,
                localHealthAwarenessEnabled,
                localHealthAwarenessEnabled,
                suspicionConfirmationsEnabled,
                false
        );
    }

    public SwimLifeguardConfig {
        if (probeInterval == null || probeInterval.isZero() || probeInterval.isNegative()) {
            throw new IllegalArgumentException("probeInterval must be positive");
        }
        if (probeTimeout == null || probeTimeout.isZero() || probeTimeout.isNegative()) {
            throw new IllegalArgumentException("probeTimeout must be positive");
        }
        if (indirectChecks < 0 || maxConfirmations < 0 || awarenessMaxMultiplier < 0) {
            throw new IllegalArgumentException("counts cannot be negative");
        }
        if (suspicionMultiplier <= 0 || suspicionMaxTimeoutMultiplier < 1) {
            throw new IllegalArgumentException("invalid suspicion multipliers");
        }
    }

    public static SwimLifeguardConfig defaults() {
        return new SwimLifeguardConfig(
                Duration.ofSeconds(1),
                Duration.ofMillis(500),
                3,
                4.0,
                6.0,
                3,
                8,
                true,
                true,
                true,
                false
        );
    }

    public static SwimLifeguardConfig swimBase() {
        SwimLifeguardConfig defaults = defaults();
        return new SwimLifeguardConfig(
                defaults.probeInterval(),
                defaults.probeTimeout(),
                defaults.indirectChecks(),
                defaults.suspicionMultiplier(),
                defaults.suspicionMaxTimeoutMultiplier(),
                defaults.maxConfirmations(),
                defaults.awarenessMaxMultiplier(),
                false,
                false,
                false,
                false
        );
    }

    public static SwimLifeguardConfig lhaProbeOnly() {
        return withCorrections(true, false, false, false);
    }

    public static SwimLifeguardConfig lhaSuspicionOnly() {
        return withCorrections(false, true, true, false);
    }

    public static SwimLifeguardConfig buddyOnly() {
        return withCorrections(false, false, false, true);
    }

    public boolean localHealthAwarenessEnabled() {
        return lhaProbeEnabled || lhaSuspicionEnabled;
    }

    private static SwimLifeguardConfig withCorrections(
            boolean lhaProbe,
            boolean lhaSuspicion,
            boolean confirmations,
            boolean buddy
    ) {
        SwimLifeguardConfig defaults = defaults();
        return new SwimLifeguardConfig(
                defaults.probeInterval(),
                defaults.probeTimeout(),
                defaults.indirectChecks(),
                defaults.suspicionMultiplier(),
                defaults.suspicionMaxTimeoutMultiplier(),
                defaults.maxConfirmations(),
                defaults.awarenessMaxMultiplier(),
                lhaProbe,
                lhaSuspicion,
                confirmations,
                buddy
        );
    }
}
