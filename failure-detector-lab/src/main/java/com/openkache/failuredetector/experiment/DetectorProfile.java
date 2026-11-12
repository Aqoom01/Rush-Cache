package com.openkache.failuredetector.experiment;

import com.openkache.failuredetector.adaptive.AdaptiveDeadlineFailureDetector;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.modules.CorrectionKind;
import com.openkache.failuredetector.modules.DecisionKind;
import com.openkache.failuredetector.modules.ObservationKind;
import com.openkache.failuredetector.modules.ProfileModules;
import com.openkache.failuredetector.phi.PhiAccrualFailureDetector;
import com.openkache.failuredetector.swim.SwimLifeguardDetector;

import java.util.Optional;

public enum DetectorProfile {
    F0_FIXED_DEADLINE(
            "F0", FixedDeadlineFailureDetector.ALGORITHM,
            new ProfileModules(
                    DecisionKind.FIXED_DEADLINE,
                    ObservationKind.ALL_TO_ALL_HEARTBEAT,
                    CorrectionKind.NONE)),
    F1_PHI_GOSSIP(
            "F1", PhiAccrualFailureDetector.ALGORITHM,
            new ProfileModules(
                    DecisionKind.PHI_ACCRUAL,
                    ObservationKind.GOSSIP_HEARTBEAT,
                    CorrectionKind.NONE)),
    F2_SWIM_BASE(
            "F2", "swim-base",
            new ProfileModules(
                    DecisionKind.SWIM_SUSPICION,
                    ObservationKind.SWIM_RANDOM_PROBE,
                    CorrectionKind.NONE)),
    F3_SWIM_LIFEGUARD(
            "F3", SwimLifeguardDetector.ALGORITHM,
            new ProfileModules(
                    DecisionKind.SWIM_SUSPICION,
                    ObservationKind.SWIM_RANDOM_PROBE,
                    CorrectionKind.LIFEGUARD_ALL)),
    F4_ADAPTIVE_DEADLINE(
            "F4", AdaptiveDeadlineFailureDetector.ALGORITHM,
            new ProfileModules(
                    DecisionKind.ADAPTIVE_INTERARRIVAL,
                    ObservationKind.ALL_TO_ALL_HEARTBEAT,
                    CorrectionKind.NONE)),
    F5_FIXED_DEADLINE_RING(
            "F5", "fixed-deadline-ring",
            new ProfileModules(
                    DecisionKind.FIXED_DEADLINE,
                    ObservationKind.RING_NEIGHBOR_HEARTBEAT,
                    CorrectionKind.NONE)),
    F6_FIXED_DEADLINE_RING_VERIFY(
            "F6", "fixed-deadline-ring-verify",
            new ProfileModules(
                    DecisionKind.FIXED_DEADLINE,
                    ObservationKind.RING_NEIGHBOR_HEARTBEAT,
                    CorrectionKind.VERIFY_SUSPECT)),
    F7A_SWIM_LHA_PROBE(
            "F7a", "swim-lha-probe",
            new ProfileModules(
                    DecisionKind.SWIM_SUSPICION,
                    ObservationKind.SWIM_RANDOM_PROBE,
                    CorrectionKind.LHA_PROBE)),
    F7B_SWIM_LHA_SUSPICION(
            "F7b", "swim-lha-suspicion",
            new ProfileModules(
                    DecisionKind.SWIM_SUSPICION,
                    ObservationKind.SWIM_RANDOM_PROBE,
                    CorrectionKind.LHA_SUSPICION)),
    F7C_SWIM_BUDDY(
            "F7c", "swim-buddy",
            new ProfileModules(
                    DecisionKind.SWIM_SUSPICION,
                    ObservationKind.SWIM_RANDOM_PROBE,
                    CorrectionKind.BUDDY_SYSTEM));

    private final String code;
    private final String id;
    private final ProfileModules modules;

    DetectorProfile(String code, String id, ProfileModules modules) {
        this.code = code;
        this.id = id;
        this.modules = modules;
        modules.validate();
    }

    public String code() {
        return code;
    }

    public String id() {
        return id;
    }

    public ProfileModules modules() {
        return modules;
    }

    public static DetectorProfile fromId(String value) {
        Optional<DetectorProfile> profile = findById(value);
        if (profile.isPresent()) {
            return profile.get();
        }
        throw new IllegalArgumentException("Unknown profile: " + value);
    }

    public static Optional<DetectorProfile> findById(String value) {
        if (value == null) {
            return Optional.empty();
        }
        for (DetectorProfile profile : values()) {
            if (profile.id.equalsIgnoreCase(value.trim())
                    || profile.code.equalsIgnoreCase(value.trim())) {
                return Optional.of(profile);
            }
        }
        return Optional.empty();
    }
}
