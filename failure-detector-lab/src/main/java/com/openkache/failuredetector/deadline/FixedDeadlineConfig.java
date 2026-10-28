package com.openkache.failuredetector.deadline;

import java.time.Duration;

public record FixedDeadlineConfig(
        Duration heartbeatInterval,
        Duration deadline,
        Duration confirm,
        Duration warmup,
        PeerSelectionMode peerSelectionMode,
        int payloadBytes,
        boolean verifySuspectEnabled,
        Duration verificationInterval,
        int failedVerificationsToDead
) {
    public FixedDeadlineConfig(
            Duration heartbeatInterval,
            Duration deadline,
            Duration confirm,
            Duration warmup,
            PeerSelectionMode peerSelectionMode,
            int payloadBytes
    ) {
        this(
                heartbeatInterval,
                deadline,
                confirm,
                warmup,
                peerSelectionMode,
                payloadBytes,
                false,
                Duration.ofMillis(500),
                2
        );
    }

    public FixedDeadlineConfig {
        if (heartbeatInterval == null || heartbeatInterval.isZero()
                || heartbeatInterval.isNegative()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        if (deadline == null || deadline.compareTo(heartbeatInterval) <= 0) {
            throw new IllegalArgumentException("deadline must be greater than heartbeatInterval");
        }
        if (confirm == null || confirm.isNegative() || warmup == null || warmup.isNegative()) {
            throw new IllegalArgumentException("confirm and warmup cannot be negative");
        }
        if (peerSelectionMode == null || payloadBytes < 1) {
            throw new IllegalArgumentException("peer selection and payload must be valid");
        }
        if (verificationInterval == null || verificationInterval.isZero()
                || verificationInterval.isNegative() || failedVerificationsToDead < 1) {
            throw new IllegalArgumentException("verification settings must be valid");
        }
    }

    public static FixedDeadlineConfig defaults() {
        return new FixedDeadlineConfig(
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                Duration.ofSeconds(2),
                Duration.ofSeconds(3),
                PeerSelectionMode.FULL_MESH,
                64,
                false,
                Duration.ofMillis(500),
                2
        );
    }

    public static FixedDeadlineConfig ring() {
        FixedDeadlineConfig defaults = defaults();
        return new FixedDeadlineConfig(
                defaults.heartbeatInterval(),
                defaults.deadline(),
                defaults.confirm(),
                defaults.warmup(),
                PeerSelectionMode.DETERMINISTIC_RING,
                defaults.payloadBytes(),
                false,
                defaults.verificationInterval(),
                defaults.failedVerificationsToDead()
        );
    }

    public static FixedDeadlineConfig ringVerified() {
        FixedDeadlineConfig ring = ring();
        return new FixedDeadlineConfig(
                ring.heartbeatInterval(),
                ring.deadline(),
                ring.confirm(),
                ring.warmup(),
                ring.peerSelectionMode(),
                ring.payloadBytes(),
                true,
                ring.verificationInterval(),
                ring.failedVerificationsToDead()
        );
    }

    public enum PeerSelectionMode {
        FULL_MESH,
        DETERMINISTIC_RING
    }
}
