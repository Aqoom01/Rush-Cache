package io.openkache.core.migration.experiment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Baseline 실행 스냅샷의 JSON 원본 codec. 외부 JSON 의존성을 production에 추가하지 않는다. */
final class BaselineJson {
    private BaselineJson() {}

    static void write(Path file, BaselineResults.Run run, Map<String, Object> context, String caseId) throws IOException {
        var c = run.condition();
        Map<String, Object> root = new LinkedHashMap<>(context);
        root.put("resultSchemaVersion", "b3-baseline-json-1");
        String localId = file.getFileName().toString().replaceFirst("\\.json$", "");
        String suiteId = String.valueOf(context.getOrDefault("suiteExecutionId", "standalone"));
        root.put("executionId", suiteId + "-" + localId);
        root.put("resultFile", "results/" + file.getFileName());
        root.put("caseId", caseId);
        root.put("study", "b3-live-migration");
        root.put("variant", "stop-and-copy");
        root.put("scenario", c.scenario());
        root.put("seed", run.seed());
        root.put("startedAtEpochMillis", run.startedAtEpochMillis() == 0 ? null : run.startedAtEpochMillis());
        root.put("timeDomain", "virtual");
        root.put("outcome", run.outcome());
        root.put("validationStatus", run.metrics().get(BaselineResults.VIOLATIONS).longValue() == 0 ? "valid" : "invalid");
        root.put("terminationReason", switch (run.outcome()) {
            case "완료" -> "completed";
            case "취소(timeout)" -> "model-timeout";
            case "취소(요청)" -> "requested-abort";
            case "취소 거부" -> "completed-after-refused-abort";
            default -> throw new IllegalArgumentException("알 수 없는 결과");
        });
        root.put("stopPhaseReason", null);
        Map<String, Object> condition = new LinkedHashMap<>();
        condition.put("scenario", c.scenario()); condition.put("keys", c.keys()); condition.put("budgetBytesPerTick", c.budget());
        condition.put("rho", c.rho()); condition.put("deletePercent", c.deletePercent()); condition.put("fault", c.fault().name());
        condition.put("stop", c.stop()); condition.put("otherWritesPerTick", c.otherWrites());
        root.put("condition", condition);
        var metrics = new LinkedHashMap<String, Object>();
        run.metrics().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            String metric = entry.getKey();
            String unit = metric.contains("(tick)") ? "tick" : metric.contains("(바이트)") ? "bytes"
                    : metric.equals("전송량 비율") ? "ratio" : metric.contains("여부") ? "boolean" : "count";
            metrics.put(metric, Map.of("value", entry.getValue(), "unit", unit));
        });
        root.put("metrics", metrics);
        Files.writeString(file, encode(root) + "\n", StandardCharsets.UTF_8);
    }

    static BaselineResults.Run read(Path file) throws IOException {
        Map<String, Object> root = object(new Parser(Files.readString(file, StandardCharsets.UTF_8)).parse());
        if (!"b3-baseline-json-1".equals(root.get("resultSchemaVersion")) || !"stop-and-copy".equals(root.get("variant"))) {
            throw new IllegalArgumentException("지원하지 않는 Baseline JSON 형식: " + file);
        }
        Map<String, Object> c = object(root.get("condition"));
        var condition = new BaselineResults.Condition(string(c, "scenario"), Math.toIntExact(number(c, "keys").longValue()),
                number(c, "budgetBytesPerTick").longValue(), number(c, "rho").doubleValue(), Math.toIntExact(number(c, "deletePercent").longValue()),
                MigrationFixture.Fault.valueOf(string(c, "fault")), string(c, "stop"), Math.toIntExact(number(c, "otherWritesPerTick").longValue()));
        Map<String, Number> metrics = new LinkedHashMap<>();
        object(root.get("metrics")).forEach((name, value) -> metrics.put(name, number(object(value), "value")));
        long started = root.get("startedAtEpochMillis") instanceof Number value ? value.longValue() : 0;
        return new BaselineResults.Run(condition, number(root, "seed").longValue(), string(root, "outcome"), metrics, started);
    }

    static Object decode(String json) { return new Parser(json).parse(); }

    static Map<String, Object> object(Object value) {
        if (!(value instanceof Map<?, ?> map)) throw new IllegalArgumentException("JSON object가 필요합니다");
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, item) -> { if (!(key instanceof String)) throw new IllegalArgumentException("JSON key 오류"); result.put((String) key, item); });
        return result;
    }
    private static String string(Map<String, Object> map, String key) {
        if (map.get(key) instanceof String value) return value;
        throw new IllegalArgumentException("문자열 필드 누락: " + key);
    }
    private static Number number(Map<String, Object> map, String key) {
        if (map.get(key) instanceof Number value) return value;
        throw new IllegalArgumentException("숫자 필드 누락: " + key);
    }

    static String encode(Object value) {
        if (value == null) return "null";
        if (value instanceof String text) {
            StringBuilder out = new StringBuilder("\"");
            for (char c : text.toCharArray()) {
                switch (c) {
                    case '"' -> out.append("\\\""); case '\\' -> out.append("\\\\");
                    case '\n' -> out.append("\\n"); case '\r' -> out.append("\\r"); case '\t' -> out.append("\\t");
                    default -> { if (c < 32) out.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c)); else out.append(c); }
                }
            }
            return out.append('"').toString();
        }
        if (value instanceof Number number) {
            if (!Double.isFinite(number.doubleValue())) throw new IllegalArgumentException("유한한 숫자만 JSON에 기록합니다");
            return number.toString();
        }
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> map) return map.entrySet().stream().map(e -> encode(e.getKey().toString()) + ":" + encode(e.getValue()))
                .collect(java.util.stream.Collectors.joining(",", "{", "}"));
        if (value instanceof Iterable<?> list) {
            List<String> items = new ArrayList<>(); for (Object item : list) items.add(encode(item));
            return "[" + String.join(",", items) + "]";
        }
        throw new IllegalArgumentException("지원하지 않는 JSON 값");
    }

    /** 이 backend가 쓰는 JSON을 다시 읽는다. 중복 key·후행 데이터·잘못된 escape는 거절한다. */
    private static final class Parser {
        private final String input;
        private int at;
        private int depth;
        Parser(String input) { this.input = input; }
        Object parse() { Object result = value(); space(); if (at != input.length()) throw bad(); return result; }
        private void space() { while (at < input.length() && " \n\r\t".indexOf(input.charAt(at)) >= 0) at++; }
        private boolean take(char c) { space(); if (at < input.length() && input.charAt(at) == c) { at++; return true; } return false; }
        private void need(char c) { if (!take(c)) throw bad(); }
        private IllegalArgumentException bad() { return new IllegalArgumentException("잘못된 JSON 위치: " + at); }
        private Object value() {
            space(); if (at == input.length() || ++depth > 64) throw bad();
            try {
                char token = input.charAt(at);
                if (token == '"') return text();
                if (take('{')) {
                    Map<String, Object> object = new LinkedHashMap<>();
                    if (take('}')) return object;
                    do { space(); String key = text(); need(':'); if (object.containsKey(key)) throw bad(); object.put(key, value()); } while (take(','));
                    need('}'); return object;
                }
                if (take('[')) {
                    List<Object> list = new ArrayList<>();
                    if (take(']')) return list;
                    do { list.add(value()); } while (take(',')); need(']'); return list;
                }
                for (String literal : new String[] {"null", "true", "false"}) {
                    if (input.startsWith(literal, at)) { at += literal.length(); return literal.equals("null") ? null : Boolean.valueOf(literal); }
                }
                int start = at;
                while (at < input.length() && "-+0123456789.eE".indexOf(input.charAt(at)) >= 0) at++;
                String number = input.substring(start, at);
                if (!number.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?")) throw bad();
                if (number.contains(".") || number.contains("e") || number.contains("E")) {
                    double result = Double.parseDouble(number); if (!Double.isFinite(result)) throw bad(); return result;
                }
                return Long.valueOf(number);
            } finally { depth--; }
        }
        private String text() {
            if (at >= input.length() || input.charAt(at++) != '"') throw bad();
            StringBuilder out = new StringBuilder();
            while (at < input.length()) {
                char c = input.charAt(at++);
                if (c == '"') return out.toString();
                if (c < 32) throw bad();
                if (c != '\\') { out.append(c); continue; }
                if (at == input.length()) throw bad();
                char escape = input.charAt(at++);
                switch (escape) {
                    case '"', '\\', '/' -> out.append(escape);
                    case 'n' -> out.append('\n'); case 'r' -> out.append('\r'); case 't' -> out.append('\t');
                    case 'b' -> out.append('\b'); case 'f' -> out.append('\f');
                    case 'u' -> { if (at + 4 > input.length()) throw bad(); out.append((char) Integer.parseInt(input.substring(at, at + 4), 16)); at += 4; }
                    default -> throw bad();
                }
            }
            throw bad();
        }
    }
}
