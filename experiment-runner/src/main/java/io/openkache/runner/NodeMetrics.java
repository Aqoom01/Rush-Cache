package io.openkache.runner;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * cache-server의 {@code /metrics}에서 프로세스 CPU 누적 시간을 읽는다. window 앞뒤로 읽어 차이를 window 시간으로 나누면 그 window의
 * 서버 CPU 이용률이 된다(vCPU 1개 = 1.0).
 *
 * <p>Runner가 서버를 관측하는 유일한 경로다. 읽지 못하면 {@code NaN}을 돌려주고 실험은 계속한다.
 */
final class NodeMetrics {

    private static final String CPU_LINE = "process_cpu_seconds_total ";
    private static final int TIMEOUT_MS = 3000;

    private NodeMetrics() {}

    static double cpuSeconds(String host, int port) {
        try {
            HttpURLConnection http = (HttpURLConnection) URI.create("http://" + host + ":" + port + "/metrics").toURL().openConnection();
            http.setConnectTimeout(TIMEOUT_MS);
            http.setReadTimeout(TIMEOUT_MS);
            try (InputStream in = http.getInputStream()) {
                for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                    if (line.startsWith(CPU_LINE)) {
                        return Double.parseDouble(line.substring(CPU_LINE.length()).trim());
                    }
                }
            } finally {
                http.disconnect();
            }
        } catch (IOException | NumberFormatException e) {
            return Double.NaN;
        }
        return Double.NaN;
    }
}
