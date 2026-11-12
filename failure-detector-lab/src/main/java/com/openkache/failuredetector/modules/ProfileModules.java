package com.openkache.failuredetector.modules;

import java.util.ArrayList;
import java.util.List;

public record ProfileModules(
        DecisionKind decision,
        ObservationKind observation,
        CorrectionKind correction
) {
    public ProfileModules {
        if (decision == null || observation == null || correction == null) {
            throw new IllegalArgumentException("profile modules must not be null");
        }
    }

    public List<String> validationErrors() {
        List<String> errors = new ArrayList<>();
        for (Capability required : decision.requiredCapabilities()) {
            if (!observation.capabilities().contains(required)) {
                errors.add(decision + " requires " + required + " but " + observation
                        + " does not provide it");
            }
        }
        boolean lifeguard = correction == CorrectionKind.LIFEGUARD_ALL
                || correction == CorrectionKind.LHA_PROBE
                || correction == CorrectionKind.LHA_SUSPICION
                || correction == CorrectionKind.BUDDY_SYSTEM;
        if (lifeguard && (decision != DecisionKind.SWIM_SUSPICION
                || observation != ObservationKind.SWIM_RANDOM_PROBE)) {
            errors.add(correction + " is only compatible with SWIM suspicion and SWIM probes");
        }
        if (correction == CorrectionKind.VERIFY_SUSPECT
                && !observation.capabilities().contains(Capability.INDEPENDENT_VERIFY_PATH)) {
            errors.add("VERIFY_SUSPECT requires an independent verification path");
        }
        return List.copyOf(errors);
    }

    public void validate() {
        List<String> errors = validationErrors();
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", errors));
        }
    }
}
