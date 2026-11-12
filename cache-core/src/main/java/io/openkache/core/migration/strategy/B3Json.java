package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

/** B3 전용 JDK JSON codec. 중복 key·후행 데이터·비유한 숫자를 거절한다. */
public final class B3Json {
    private B3Json() {}
    public static Object decode(String json) { return new Parser(json).parse(); }
    public static Map<String,Object> object(Object value) {
        if (!(value instanceof Map<?,?> map)) throw new IllegalArgumentException("JSON object required");
        Map<String,Object> out = new LinkedHashMap<>();
        map.forEach((k,v)->{if (!(k instanceof String s)) throw new IllegalArgumentException("string key required"); out.put(s,v);});
        return out;
    }
    public static String hash(Object value) {
        try { return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(encode(canonical(value)).getBytes(java.nio.charset.StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    }
    private static Object canonical(Object value) {
        if (value != null && value.getClass().isRecord()) return canonical(record(value));
        if (value != null && value.getClass().isArray()) { List<Object> out=new ArrayList<>(); for(int i=0;i<java.lang.reflect.Array.getLength(value);i++) out.add(canonical(java.lang.reflect.Array.get(value,i))); return out; }
        if (value instanceof Map<?,?> map) { Map<String,Object> out = new java.util.TreeMap<>(); map.forEach((k,v)->out.put(k.toString(),canonical(v))); return out; }
        if (value instanceof Iterable<?> iterable) { List<Object> out=new ArrayList<>(); iterable.forEach(v->out.add(canonical(v))); return out; }
        return value;
    }
    public static String encode(Object value) {
        if (value == null) return "null";
        if (value instanceof Enum<?> e) return encode(e.name());
        if (value.getClass().isRecord()) return encode(record(value));
        if (value.getClass().isArray()) { List<Object> out=new ArrayList<>(); for(int i=0;i<java.lang.reflect.Array.getLength(value);i++)out.add(java.lang.reflect.Array.get(value,i));return encode(out); }
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
        if (value instanceof Map<?, ?> map) return map.entrySet().stream().sorted(java.util.Comparator.comparing(e -> e.getKey().toString())).map(e -> encode(e.getKey().toString()) + ":" + encode(e.getValue()))
                .collect(java.util.stream.Collectors.joining(",", "{", "}"));
        if (value instanceof Iterable<?> list) {
            List<String> items = new ArrayList<>(); for (Object item : list) items.add(encode(item));
            return "[" + String.join(",", items) + "]";
        }
        throw new IllegalArgumentException("지원하지 않는 JSON 값");
    }

    private static Map<String,Object> record(Object value) {
        Map<String,Object> out=new LinkedHashMap<>();
        try { for(var component:value.getClass().getRecordComponents())out.put(component.getName(),component.getAccessor().invoke(value)); }
        catch(ReflectiveOperationException error){throw new IllegalArgumentException("public B3 record required",error);}
        return out;
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
