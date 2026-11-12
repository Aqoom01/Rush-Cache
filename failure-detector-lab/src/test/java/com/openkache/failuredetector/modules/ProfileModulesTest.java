package com.openkache.failuredetector.modules;

import com.openkache.failuredetector.experiment.DetectorProfile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProfileModulesTest {
    @Test
    void everyDeclaredF0ToF7ProfileIsCompatible() {
        for (DetectorProfile profile : DetectorProfile.values()) {
            assertDoesNotThrow(profile.modules()::validate, profile.id());
        }
    }

    @Test
    void phiCannotSilentlyConsumeRandomSwimProbes() {
        ProfileModules invalid = new ProfileModules(
                DecisionKind.PHI_ACCRUAL,
                ObservationKind.SWIM_RANDOM_PROBE,
                CorrectionKind.NONE
        );

        assertThrows(IllegalArgumentException.class, invalid::validate);
    }

    @Test
    void lifeguardCannotAttachToDeadlineHeartbeat() {
        ProfileModules invalid = new ProfileModules(
                DecisionKind.FIXED_DEADLINE,
                ObservationKind.ALL_TO_ALL_HEARTBEAT,
                CorrectionKind.LHA_PROBE
        );

        assertThrows(IllegalArgumentException.class, invalid::validate);
    }
}
