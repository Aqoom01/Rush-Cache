package io.openkache.server.integration;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;

/** payload와 비밀 값 없이 동일 JVM clock의 이벤트를 기록한다. */
final class JsonLine implements AutoCloseable {
    private final BufferedWriter out;
    JsonLine(Path path) throws IOException { out = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW); }
    static String quote(String value) {
        StringBuilder b = new StringBuilder("\"");
        for (char c : value.toCharArray()) switch (c) {
            case '\\' -> b.append("\\\\"); case '"' -> b.append("\\\""); case '\n' -> b.append("\\n"); case '\r' -> b.append("\\r"); case '\t' -> b.append("\\t");
            default -> { if (c < 32) b.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c)); else b.append(c); }
        }
        return b.append('"').toString();
    }
    synchronized void event(String type, String fields) {
        try { out.write("{\"event\":" + quote(type) + ",\"atNanos\":" + System.nanoTime() + ",\"atUtc\":" + quote(Instant.now().toString()) + (fields.isEmpty() ? "" : "," + fields) + "}\n"); out.flush(); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }
    @Override public synchronized void close() throws IOException { out.close(); }
}
