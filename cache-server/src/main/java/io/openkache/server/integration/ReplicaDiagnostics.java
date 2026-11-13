package io.openkache.server.integration;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

/** 이 실행이 소유한 replica의 작은 진단 JSON 전용 reader. 일반 JSON parser나 제품 API가 아니다. */
final class ReplicaDiagnostics {
    private ReplicaDiagnostics() { }
    static long number(String json, String name) {
        var m = Pattern.compile("\\\"" + Pattern.quote(name) + "\\\"\\s*:\\s*(-?[0-9]+)").matcher(json);
        if (!m.find()) throw new IllegalArgumentException("diagnostic field missing: " + name); return Long.parseLong(m.group(1));
    }
    static String request(IntegrationConfig c, int group, int index, Map<String, String> operation) throws IOException {
        String query = operation == null ? "" : "?" + operation.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8)).collect(java.util.stream.Collectors.joining("&"));
        URL url = URI.create("http://" + c.hosts(group).get(index) + ":" + (c.number("replica.port", 17101) + 4) + (operation == null ? "/status" : "/master") + query).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(500); connection.setReadTimeout(c.number("request.timeout.ms", 3000));
        if (operation != null) { connection.setRequestMethod("POST"); connection.setRequestProperty("X-Openkache-Run", c.runId()); }
        try {
            if (connection.getResponseCode() != 200) {
                String detail = ""; if (connection.getErrorStream() != null) try (var error = connection.getErrorStream()) { detail = new String(error.readNBytes(1500), StandardCharsets.UTF_8); }
                throw new IOException("replica diagnostic/master rejected: " + connection.getResponseCode() + ":" + detail);
            }
            try (var in = connection.getInputStream()) {
                byte[] bytes = in.readNBytes(262145); if (bytes.length > 262144) throw new IOException("oversize replica status");
                String json = new String(bytes, StandardCharsets.UTF_8);
                if (!json.contains("\"runId\":\"" + c.runId() + "\"")) throw new IOException("replica run ID mismatch"); return json;
            }
        } finally { connection.disconnect(); }
    }
}
