package io.openkache.core.migration.experiment;

import io.openkache.core.migration.precopy.DeltaBatch;
import io.openkache.core.migration.precopy.PreCopyProtocol.RoundEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Protocol의 실제 generation 경계에 caller의 tick을 붙이고 JSON 원본에서 상세 파일을 재생성한다. */
final class PreCopyRoundTrace {
    private final List<Map<String, Object>> events = new ArrayList<>();

    void capture(List<RoundEvent> observations, long tick) {
        require(observations.size() >= events.size(), "round observation history shrank");
        for (int i = events.size(); i < observations.size(); i++) {
            var e = observations.get(i);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("generation", e.generation()); row.put("liveRound", e.liveRound());
            row.put("finalRound", e.finalRound()); row.put("event", e.event().name()); row.put("tick", tick);
            row.put("dirtyKeys", e.dirtyKeys()); row.put("putKeys", e.dirtyKeys() - e.tombstoneKeys());
            row.put("tombstoneKeys", e.tombstoneKeys()); row.put("dirtyBytes", e.dirtyBytes());
            row.put("previousDirtyBytes", e.previousDirtyBytes());
            row.put("stopReason", e.stopReason() == null ? null : e.stopReason().name());
            row.put("roundSentBytes", e.roundSentBytes()); row.put("sentBytes", e.sentBytes());
            row.put("sequenceBefore", e.sequenceBefore()); row.put("lastSequence", e.lastSequence());
            row.put("acknowledgedSequence", e.acknowledgedSequence());
            events.add(Collections.unmodifiableMap(row));
        }
    }

    List<Map<String, Object>> events() { return List.copyOf(events); }

    /** 완료 run의 generation 연결·종료 우선순위·ACK/seq·전송량을 원본에서 교차 검증한다. */
    static void validate(Object value, boolean preCopy, long threshold, int maxRounds, long rounds, String reason,
            long sentBytes, long startTick, long commitTick) {
        var rows = objects(value);
        if (!preCopy) { require(rows.isEmpty(), "unexpected non-precopy dirty trace"); return; }
        require(rounds >= 0 && rounds <= maxRounds && startTick >= 0 && commitTick >= startTick && sentBytes > 0,
                "invalid dirty run bounds");
        require(!rows.isEmpty() && rows.size() % 2 == 0 && rows.size() == 2 * (rounds + 1), "missing dirty generation");
        Long previousDirty = null;
        long previousSent = -1, previousSequence = 0, previousTick = startTick;
        for (int i = 0; i < rows.size(); i += 2) {
            var begin = rows.get(i); var end = rows.get(i + 1);
            for (var row : List.of(begin, end)) {
                require(row.keySet().containsAll(List.of("generation", "liveRound", "finalRound", "event", "tick",
                        "dirtyKeys", "putKeys", "tombstoneKeys", "dirtyBytes", "previousDirtyBytes", "stopReason",
                        "roundSentBytes", "sentBytes", "sequenceBefore", "lastSequence", "acknowledgedSequence")), "missing dirty observation field");
            }
            long generation = i / 2L + 1;
            boolean last = i == rows.size() - 2;
            require("STARTED".equals(begin.get("event")) && "ACKNOWLEDGED".equals(end.get("event")), "incomplete dirty round");
            require(n(begin, "generation") == generation && Boolean.valueOf(last).equals(begin.get("finalRound"))
                    && n(begin, "liveRound") == (last ? rounds : generation), "dirty round identity");
            for (String key : List.of("generation", "liveRound", "finalRound", "dirtyKeys", "putKeys", "tombstoneKeys",
                    "dirtyBytes", "previousDirtyBytes", "stopReason", "sequenceBefore")) {
                require(Objects.equals(begin.get(key), end.get(key)), "dirty round metadata changed: " + key);
            }
            long keys = n(begin, "dirtyKeys"), tombstones = n(begin, "tombstoneKeys"), dirty = n(begin, "dirtyBytes");
            require(keys >= 0 && keys <= Integer.MAX_VALUE && tombstones >= 0 && tombstones <= keys && n(begin, "putKeys") == keys - tombstones
                    && dirty >= DeltaBatch.HEADER_BYTES + keys * 17, "invalid dirty size/count");
            require(begin.get("previousDirtyBytes") == null ? previousDirty == null
                    : previousDirty != null && n(begin, "previousDirtyBytes") == previousDirty, "wrong previous dirty generation");
            String expected = dirty <= threshold ? "THRESHOLD" : generation - 1 >= maxRounds ? "ROUND_LIMIT"
                    : previousDirty != null && dirty >= previousDirty ? "NOT_SHRINKING" : null;
            require(Objects.equals(expected, begin.get("stopReason")) && (expected != null) == last,
                    "dirty stop decision disagrees with observations");
            long firstTick = n(begin, "tick"), endTick = n(end, "tick"), base = n(begin, "sentBytes");
            require(firstTick >= previousTick && endTick >= firstTick && endTick <= commitTick, "dirty tick outside lifetime");
            require(n(begin, "roundSentBytes") == 0 && base > 0 && (previousSent < 0 || base == previousSent), "dirty byte boundary");
            require(n(begin, "sequenceBefore") == previousSequence && n(begin, "lastSequence") == previousSequence
                    && n(begin, "acknowledgedSequence") == previousSequence, "dirty sequence boundary");
            long actual = n(end, "roundSentBytes");
            require(actual >= DeltaBatch.HEADER_BYTES + keys * 17 && n(end, "sentBytes") == Math.addExact(base, actual)
                    && n(end, "lastSequence") == Math.addExact(previousSequence, keys)
                    && n(end, "acknowledgedSequence") == n(end, "lastSequence"), "dirty ACK/transfer accounting");
            if (last) require(dirty == actual && Objects.equals(reason, expected) && n(end, "sentBytes") == sentBytes,
                    "final dirty transfer mismatch");
            previousDirty = dirty; previousSent = n(end, "sentBytes");
            previousSequence = n(end, "lastSequence"); previousTick = endTick;
        }
    }

    /** CSV는 모든 이벤트, 보고서는 generation별 시작·완료를 한 행으로 보존한다. */
    static void write(Path folder, List<Map<String, Object>> results) throws IOException {
        StringBuilder csv = new StringBuilder("\uFEFF조건 ID,방법,방식,seed,job,generation,live 라운드,최종 라운드,이벤트,tick,dirty keys,PUT keys,tombstone keys,dirty 예상 bytes,직전 live dirty bytes,종료 사유,라운드 실제 전송 bytes,누적 전송 bytes,시작 전 seq,생성한 seq,ACK seq\n");
        StringBuilder md = new StringBuilder("# Pre-copy 라운드별 dirty 상세\n\n")
                .append("JSON 원본의 dirtyRounds에서 재생성한다. 시각은 caller의 가상 tick이고 byte는 framing을 포함한다. ")
                .append("dirty 예상량은 generation 시작 경계의 관측이다. live 중 값이 바뀌면 실제 전송량과 다를 수 있다. ")
                .append("최종 라운드는 freeze 뒤라 예상량과 실제량이 같다. STARTED/ACKNOWLEDGED는 같은 tick에 발생할 수 있다.\n\n")
                .append("| 조건 / 방법 | seed | job | generation | live / final | 시작→ACK tick | dirty keys (삭제) | 예상 bytes → 실제 bytes | 직전 live bytes | 종료 사유 |\n")
                .append("|---|---:|---:|---:|---|---|---:|---|---:|---|\n");
        int generations = 0;
        for (var result : results) {
            String id = Objects.toString(result.getOrDefault("caseId", result.get("scenario")), "");
            String method = Objects.toString(result.get("method"), "");
            String variant = Objects.toString(result.get("variant"), "");
            Object seed = result.get("seed");
            var jobs = result.containsKey("jobs") ? objects(result.get("jobs")) : List.of(result);
            for (var job : jobs) {
                var trace = objects(job.get("dirtyRounds"));
                for (var e : trace) {
                    var values = new ArrayList<Object>();
                    Collections.addAll(values, id, method, variant, seed, job.getOrDefault("job", ""));
                    for (String key : List.of("generation", "liveRound", "finalRound", "event", "tick", "dirtyKeys",
                            "putKeys", "tombstoneKeys", "dirtyBytes", "previousDirtyBytes", "stopReason",
                            "roundSentBytes", "sentBytes", "sequenceBefore", "lastSequence", "acknowledgedSequence")) values.add(e.get(key));
                    csv.append(String.join(",", values.stream().map(PreCopyRoundTrace::csv).toList())).append('\n');
                }
                for (int i = 0; i < trace.size(); i += 2) {
                    var begin = trace.get(i); var end = trace.get(i + 1); generations++;
                    md.append("| ").append(id).append(method.isEmpty() ? "" : " / " + method).append(" | ").append(seed)
                            .append(" | ").append(job.getOrDefault("job", "—")).append(" | ").append(begin.get("generation"))
                            .append(" | ").append(Boolean.TRUE.equals(begin.get("finalRound")) ? "final" : "live")
                            .append(" | ").append(begin.get("tick")).append("→").append(end.get("tick"))
                            .append(" | ").append(begin.get("dirtyKeys")).append(" (").append(begin.get("tombstoneKeys"))
                            .append(") | ").append(begin.get("dirtyBytes")).append(" → ").append(end.get("roundSentBytes"))
                            .append(" | ").append(Objects.toString(begin.get("previousDirtyBytes"), "—"))
                            .append(" | ").append(Objects.toString(begin.get("stopReason"), "—")).append(" |\n");
                }
            }
        }
        md.append("\n검증된 generation 합계: ").append(generations).append(". 이전 형식에 없는 관측을 소급 생성하지 않는다.\n");
        Files.writeString(folder.resolve("dirty-rounds.csv"), csv, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("dirty-rounds.md"), md, StandardCharsets.UTF_8);
    }

    private static String csv(Object value) {
        String text = Objects.toString(value, "");
        return text.contains(",") || text.contains("\"") || text.contains("\n") ? "\"" + text.replace("\"", "\"\"") + "\"" : text;
    }
    private static List<Map<String, Object>> objects(Object value) {
        require(value instanceof List<?>, "missing dirty trace");
        return ((List<?>) value).stream().map(BaselineJson::object).toList();
    }
    private static long n(Map<String, Object> value, String key) {
        Object number = value.get(key);
        require(number instanceof Number && Double.isFinite(((Number) number).doubleValue())
                && ((Number) number).doubleValue() == ((Number) number).longValue(), "invalid dirty integer: " + key);
        return ((Number) number).longValue();
    }
    private static void require(boolean condition, String message) { ComparisonExperiment.require(condition, message); }
}
