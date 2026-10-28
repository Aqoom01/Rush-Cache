package com.openkache.failuredetector.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpDistributedMembershipReportMainTest {
    @TempDir
    Path tempDir;

    @Test
    void reportsOnlyWhenEverySurvivorRemovedEveryCrashedMember() throws Exception {
        Files.writeString(tempDir.resolve("run.properties"),
                "nodes=8\ncrash_targets=1,2,3\nsource=real-udp-observed\n");
        for (int node : new int[] {0, 4, 5, 6, 7}) {
            Files.writeString(
                    tempDir.resolve("node-" + node + ".result.json"),
                    resultJson(node));
        }

        UdpDistributedMembershipReportMain.generate(tempDir);

        String summary = Files.readString(tempDir.resolve("summary.md"));
        assertTrue(summary.contains("| 생존 노드 View 수렴 | 성공 |"));
        assertTrue(summary.contains("| 마지막 장애 최초 DEAD | 2400.000 ms |"));
        assertTrue(summary.contains("| 모든 생존 View 수렴 | 2700.000 ms |"));
        assertTrue(summary.contains("| node-3 | 1500.000 ms | 2400.000 ms |"));
        assertTrue(Files.readString(tempDir.resolve("manifest.json"))
                .contains("\"converged\": true"));
    }

    private static String resultJson(int node) {
        return "{"
                + "\"nodeId\":" + node + ","
                + "\"preFaultDeadEvents\":0,"
                + "\"viewVersion\":4,"
                + "\"finalMembers\":\"0;4;5;6;7\","
                + "\"monitoredAtFault\":\"1;2\","
                + "\"firstAssignedAfterFault\":\"3:1500000000\","
                + "\"removalLatenciesNanos\":\"1:1200000000;2:1300000000;3:2700000000\","
                + "\"localDeadLatenciesNanos\":\"1:1100000000;2:1200000000;3:2400000000\","
                + "\"localRemovals\":3,"
                + "\"remoteRemovals\":0,"
                + "\"membershipControlFrames\":21,"
                + "\"sentFrames\":100,"
                + "\"sentBytes\":2600,"
                + "\"wireSource\":\"real-udp-observed\""
                + "}";
    }
}
