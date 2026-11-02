package io.openkache.core.replication.experiment;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolComparisonTest {
    @Test void matrixValidatesAvailabilityAndIndeterminateWrites() {
        for(String variant:new String[]{"PB","VR"}) for(var scenario:ProtocolComparison.Scenario.values())
            for(int delay:new int[]{1,5}) for(int seed=1;seed<=5;seed++) {
                var result=ProtocolComparison.run(variant,scenario,delay,seed);
                assertEquals(result,ProtocolComparison.run(variant,scenario,delay,seed));
            }
    }
}
