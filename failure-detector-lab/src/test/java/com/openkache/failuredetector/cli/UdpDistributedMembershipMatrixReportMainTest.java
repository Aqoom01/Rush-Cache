package com.openkache.failuredetector.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpDistributedMembershipMatrixReportMainTest {
    @TempDir
    Path tempDir;

    @Test
    void aggregatesRepeatedConvergedRunsAndPercentiles() throws Exception {
        createRun(1, 1_500_000_000L, 2_400_000_000L, 2_700_000_000L);
        createRun(2, 1_600_000_000L, 2_500_000_000L, 2_800_000_000L);

        UdpDistributedMembershipMatrixReportMain.generate(tempDir);

        String summary = Files.readString(tempDir.resolve("summary.md"));
        assertTrue(summary.contains("| 전체 View 수렴 | 2/2 |"));
        assertTrue(summary.contains("| 마지막 장애 DEAD p50 / p95 / p99 | "
                + "2400.000 ms / 2500.000 ms / 2500.000 ms |"));
        assertTrue(summary.contains("| 전체 View 수렴 p50 / p95 / p99 | "
                + "2700.000 ms / 2800.000 ms / 2800.000 ms |"));
        assertTrue(Files.readString(tempDir.resolve("manifest.json"))
                .contains("\"successful\": 2"));
    }

    private void createRun(
            int repetition,
            long observed,
            long lastDead,
            long convergence
    ) throws Exception {
        Path run = tempDir.resolve(String.format("run-%03d", repetition));
        Files.createDirectory(run);
        Files.writeString(run.resolve("run.properties"),
                "nodes=8\ncrash_targets=1,2,3\nsource=real-udp-observed\n");
        for (int node : new int[] {0, 4, 5, 6, 7}) {
            Files.writeString(
                    run.resolve("node-" + node + ".result.json"),
                    resultJson(node, observed, lastDead, convergence));
        }
    }

    private static String resultJson(
            int node,
            long observed,
            long lastDead,
            long convergence
    ) {
        return "{"
                + "\"nodeId\":" + node + ","
                + "\"preFaultDeadEvents\":0,"
                + "\"viewVersion\":4,"
                + "\"finalMembers\":\"0;4;5;6;7\","
                + "\"monitoredAtFault\":\"1;2\","
                + "\"firstAssignedAfterFault\":\"3:" + observed + "\","
                + "\"removalLatenciesNanos\":\"1:800000000;2:900000000;3:"
                + convergence + "\","
                + "\"localDeadLatenciesNanos\":\"1:800000000;2:900000000;3:"
                + lastDead + "\","
                + "\"localRemovals\":3,"
                + "\"remoteRemovals\":0,"
                + "\"membershipControlFrames\":21,"
                + "\"sentFrames\":100,"
                + "\"sentBytes\":2600,"
                + "\"wireSource\":\"real-udp-observed\""
                + "}";
    }
}
