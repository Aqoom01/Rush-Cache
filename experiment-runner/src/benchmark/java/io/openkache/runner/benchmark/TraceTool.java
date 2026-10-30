package io.openkache.runner.benchmark;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 논리 trace 생성/해시 CLI. 네트워크 송신이나 성능 시험을 하지 않는다.
 * 단일 thread로 실행하며 기존 출력 디렉터리/파일을 덮어쓰지 않는다.
 */
public final class TraceTool {
    private TraceTool() {}

    /** args: config.properties new-output-directory [--emit-traces]. 기본은 hash만 계산한다. */
    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3
                || (args.length == 3 && !"--emit-traces".equals(args[2]))) {
            throw new IllegalArgumentException(
                    "usage: TraceTool config.properties NEW_OUTPUT_DIRECTORY [--emit-traces]");
        }
        TracePlan plan = TracePlan.read(Path.of(args[0]));
        TraceGenerator generator = new TraceGenerator(plan.config());
        Path output = Path.of(args[1]).toAbsolutePath().normalize();
        // createDirectory fails for any existing path; no automatic cleanup on partial failure.
        Files.createDirectory(output);
        boolean emit = args.length == 3;
        List<TraceWriter.Summary> summaries = new ArrayList<>();
        for (TracePhase phase : TracePhase.values()) {
            long count = switch (phase) {
                case LOAD -> plan.config().keyCount();
                case WARMUP -> plan.warmupRequests();
                case MEASURE -> plan.measuredRequests();
            };
            String filename = phase.name().toLowerCase(java.util.Locale.ROOT) + ".trace";
            try (OutputStream stream = emit
                    ? Files.newOutputStream(output.resolve(filename),
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                    : OutputStream.nullOutputStream()) {
                summaries.add(TraceWriter.write(generator, phase, count, stream));
            }
        }
        String metadata = metadata(plan, summaries, emit);
        Files.writeString(output.resolve("trace-metadata.json"), metadata, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        System.out.print(metadata);
    }

    private static String metadata(TracePlan plan, List<TraceWriter.Summary> summaries, boolean emit) {
        TraceConfig c = plan.config();
        StringBuilder json = new StringBuilder("""
                {
                  "schema_version": "openkache-logical-trace-v1",
                  "status": "generated_only",
                  "request_execution": "not_run",
                  "generator": "indexed-splitmix64-v1",
                  "key_encoding": "okkey-v1",
                  "value_encoding": "okv1-crc32c",
                  "trace_encoding": "oktrace1-big-endian-v1",
                """);
        json.append("  \"seed\": ").append(c.seed()).append(",\n");
        json.append("  \"key_count\": ").append(c.keyCount()).append(",\n");
        json.append("  \"key_bytes\": ").append(c.keyBytes()).append(",\n");
        json.append("  \"value_bytes\": ").append(c.valueBytes()).append(",\n");
        json.append("  \"mix\": {\"get\": ").append(c.getPercent())
                .append(", \"set\": ").append(c.setPercent())
                .append(", \"delete\": ").append(c.deletePercent()).append("},\n");
        json.append("  \"stream_count\": ").append(c.streamCount()).append(",\n");
        json.append("  \"distribution\": \"").append(c.distribution().name()).append("\",\n");
        json.append("  \"hot_probability\": ").append(c.hotProbability()).append(",\n");
        json.append("  \"trace_files_emitted\": ").append(emit).append(",\n  \"phases\": [\n");
        for (int i = 0; i < summaries.size(); i++) {
            TraceWriter.Summary s = summaries.get(i);
            if (i != 0) { json.append(",\n"); }
            json.append("    {\"phase\": \"").append(s.phase().name())
                    .append("\", \"count\": ").append(s.count())
                    .append(", \"get\": ").append(s.gets())
                    .append(", \"set\": ").append(s.sets())
                    .append(", \"delete\": ").append(s.deletes())
                    .append(", \"canonical_bytes\": ").append(s.canonicalBytes())
                    .append(", \"sha256\": \"").append(s.sha256())
                    .append("\", \"file\": ");
            if (emit) {
                json.append('\"').append(s.phase().name().toLowerCase(java.util.Locale.ROOT))
                        .append(".trace\"");
            } else {
                json.append("null");
            }
            json.append("}");
        }
        return json.append("\n  ]\n}\n").toString();
    }
}

