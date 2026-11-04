package com.openkache.failuredetector.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UdpComparisonReportMainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void writesCsvJsonlAndSummaryFromRealUdpResults() throws IOException {
        writeRun("ring-verify-h3-q3-r3", "crash", 1, 0, 700_000_000L);
        writeRun("ring-verify-h3-q3-r3", "loss45", 1, 0, null);
        writeRun("swim-lifeguard", "crash", 1, 0, 900_000_000L);
        writeRun("swim-lifeguard", "loss45", 1, 0, null);

        UdpComparisonReportMain.generate(temporaryDirectory);

        String csv = Files.readString(temporaryDirectory.resolve("runs.csv"));
        String jsonl = Files.readString(temporaryDirectory.resolve("runs.jsonl"));
        String summary = Files.readString(temporaryDirectory.resolve("summary.md"));
        assertTrue(csv.contains("ring-verify-h3-q3-r3"));
        assertTrue(csv.contains("swim-lifeguard"));
        assertTrue(jsonl.contains("\"wireSource\":\"real-udp-observed\""));
        assertTrue(summary.contains("700.000 ms"));
        assertTrue(summary.contains("1/1"));
        assertTrue(summary.contains("20.7%–100.0%"));
        assertTrue(summary.contains("-200.000 ms"));
    }

    private void writeRun(
            String method,
            String fault,
            int seed,
            long preFaultDeadEvents,
            Long deadLatencyNanos
    ) throws IOException {
        Path run = temporaryDirectory.resolve(method).resolve(fault)
                .resolve(String.format("seed-%03d", seed));
        Files.createDirectories(run);
        Files.writeString(run.resolve("run.properties"),
                "profile=" + (method.equals("swim-lifeguard")
                        ? "swim-lifeguard" : "fixed-deadline-ring-verify") + "\n"
                        + "fault=" + fault + "\n"
                        + "nodes=5\n"
                        + "netem_seed=" + seed + "\n"
                        + "source=real-udp-observed\n");
        Files.writeString(run.resolve("result.json"), "{"
                + "\"profile\":\"" + (method.equals("swim-lifeguard")
                ? "swim-lifeguard" : "fixed-deadline-ring-verify") + "\","
                + "\"nodeId\":0,"
                + "\"verificationHelperCount\":3,"
                + "\"verificationFailureQuorum\":3,"
                + "\"failedVerificationRoundsToDead\":3,"
                + "\"verificationRoundSuccesses\":2,"
                + "\"verificationRoundFailures\":1,"
                + "\"verificationProbeSuccesses\":7,"
                + "\"verificationProbeFailures\":2,"
                + "\"timeSource\":\"observer-monotonic\","
                + "\"wireSource\":\"real-udp-observed\","
                + "\"faultAtNanos\":5000000000,"
                + "\"preFaultDeadEvents\":" + preFaultDeadEvents + ","
                + "\"suspectLatencyNanos\":600000000,"
                + "\"deadLatencyNanos\":"
                + (deadLatencyNanos == null ? "null" : deadLatencyNanos) + ","
                + "\"sentFrames\":100,"
                + "\"sentBytes\":2600,"
                + "\"receivedFrames\":90,"
                + "\"receivedBytes\":2340"
                + "}\n");
    }
}
