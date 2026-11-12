package io.openkache.runner.benchmark.measurement;

import java.lang.reflect.RecordComponent;
import java.util.*;

/** JDK 전용 결과 인코더. 지원하지 않는 타입/비유한 수는 문자열로 숨기지 않고 거절한다. */
final class BenchmarkJson {
    private BenchmarkJson() {}
    static String encode(Object value) {
        StringBuilder out = new StringBuilder(); append(out, value); return out.toString();
    }
    private static void append(StringBuilder out, Object value) {
        if (value == null) { out.append("null"); }
        else if (value instanceof String || value instanceof Enum<?>) { quote(out, value.toString()); }
        else if (value instanceof Boolean) { out.append(value); }
        else if (value instanceof Number number) {
            if ((number instanceof Double d && !Double.isFinite(d))
                    || (number instanceof Float f && !Float.isFinite(f))) {
                throw new IllegalArgumentException("nonfinite JSON number");
            }
            out.append(number);
        } else if (value instanceof Map<?, ?> map) {
            TreeMap<String, Object> ordered = new TreeMap<>();
            map.forEach((key, entry) -> {
                if (!(key instanceof String || key instanceof Enum<?> || key instanceof Number)) {
                    throw new IllegalArgumentException("unsupported JSON key");
                }
                if (ordered.containsKey(key.toString())) { throw new IllegalArgumentException("duplicate JSON key"); }
                ordered.put(key.toString(), entry);
            });
            out.append('{'); boolean first = true;
            for (var entry : ordered.entrySet()) {
                if (!first) { out.append(','); } first = false;
                quote(out, entry.getKey()); out.append(':'); append(out, entry.getValue());
            }
            out.append('}');
        } else if (value instanceof Iterable<?> items) {
            out.append('['); boolean first = true;
            for (Object item : items) { if (!first) { out.append(','); } first = false; append(out, item); }
            out.append(']');
        } else if (value.getClass().isRecord()) {
            Map<String, Object> fields = new LinkedHashMap<>();
            try {
                for (RecordComponent field : value.getClass().getRecordComponents()) {
                    fields.put(field.getName(), field.getAccessor().invoke(value));
                }
            } catch (ReflectiveOperationException error) { throw new IllegalArgumentException("record inaccessible", error); }
            append(out, fields);
        } else { throw new IllegalArgumentException("unsupported JSON type " + value.getClass().getName()); }
    }
    private static void quote(StringBuilder out, String text) {
        out.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\""); case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n"); case '\r' -> out.append("\\r"); case '\t' -> out.append("\\t");
                default -> {
                    if (c < 32 || Character.isSurrogate(c)) {
                        out.append("\\u");
                        String hex = Integer.toHexString(c); out.append("0".repeat(4 - hex.length())).append(hex);
                    } else { out.append(c); }
                }
            }
        }
        out.append('"');
    }
}
