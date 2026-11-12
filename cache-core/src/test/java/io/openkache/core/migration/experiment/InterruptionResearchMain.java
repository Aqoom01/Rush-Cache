package io.openkache.core.migration.experiment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/** S6 전용 연구 backend. 미지원과 예상 FAILED를 정상 이동 완료와 구별해 기록한다. */
public final class InterruptionResearchMain {
    static final String SCHEMA = "b3-s6-json-1";
    static int expected(String profile) { return InterruptionExperiment.boundaries(profile).size() * 5 * ComparisonExperiment.VARIANTS.size(); }

    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) {
            Path folder = Path.of(args[1]).toAbsolutePath().normalize();
            Properties complete = properties(folder.resolve("completion.properties"));
            ComparisonExperiment.require(SCHEMA.equals(complete.getProperty("resultSchemaVersion")), "S6 schema mismatch");
            regenerate(folder, complete.getProperty("profile"));
            return;
        }
        if (args.length != 4) throw new IllegalArgumentException("<출력 폴더> <full|quick|extended-full|extended-quick> <반복 번호> <전체 반복 수>");
        var condition = InterruptionExperiment.condition(args[1]);
        Path folder = Path.of(args[0]).toAbsolutePath().normalize();
        int repeat = Integer.parseInt(args[2]), repeats = Integer.parseInt(args[3]);
        if (repeat < 1 || repeat > repeats) throw new IllegalArgumentException("invalid repeat");
        Map<String, Object> context = BaselineResearchMain.context(properties(folder.resolve("launch.properties")), repeat, repeats);
        context.put("modelConfig", Map.of("keyBytes", 16, "valueBytes", 128, "batchTargetBytes", 65_536,
                "generatorVersion", InterruptionExperiment.GENERATOR, "traceGeneratorVersion", ComparisonExperiment.GENERATOR,
                "timeUnit", "tick", "outageTicks", InterruptionExperiment.OUTAGE_TICKS, "maxTicks", InterruptionExperiment.MAX_TICKS));
        Path originals = Files.createDirectory(folder.resolve("results"));
        for (var boundary : InterruptionExperiment.boundaries(args[1])) {
            for (int seed = 1; seed <= 5; seed++) for (String variant : ComparisonExperiment.VARIANTS) {
                var root = new LinkedHashMap<>(context);
                String id = "S6-" + boundary + "-seed" + seed + "-" + variant;
                root.put("resultSchemaVersion", SCHEMA);
                root.put("executionId", context.get("suiteExecutionId") + "-repeat" + repeat + "-" + id);
                root.put("caseId", "S6-" + boundary); root.put("boundary", boundary.name());
                root.put("study", "b3-live-migration"); root.put("scenario", "S6"); root.put("variant", variant);
                root.put("seed", seed); root.put("timeDomain", "virtual");
                root.put("condition", InterruptionExperiment.config(condition, boundary));
                var model = new LinkedHashMap<>(BaselineJson.object(root.get("modelConfig")));
                model.put("batchTargetBytes", InterruptionExperiment.batchBytes(condition, boundary));
                root.put("modelConfig", model);
                root.put("workloadHash", InterruptionExperiment.workloadHash(condition, seed));
                root.put("inputHash", InterruptionExperiment.inputHash(condition, boundary, seed));
                root.put("inputHashKind", "boundary-and-generator-specification-sha256");
                root.put("candidateConfig", variant.equals("pre-copy") ? Map.of("stopThresholdBytes", condition.threshold(), "maxRounds", condition.maxRounds()) : Map.of());
                root.put("supported", boundary.supported(variant));
                if (boundary.supported(variant)) {
                    var run = InterruptionExperiment.run(condition, boundary, seed, variant);
                    root.put("outcome", "completed"); root.put("validationStatus", "valid");
                    root.put("protocolOutcome", run.terminal()); root.put("injectionState", run.injectionState());
                    root.put("terminationReason", run.terminal().equals("FAILED") ? "expected-source-unavailable" : "expected-" + run.terminal());
                    root.put("metrics", run.metrics()); root.put("phaseEvents", run.events());
                } else {
                    root.put("outcome", "unsupported"); root.put("validationStatus", "not-applicable");
                    root.put("unsupportedReason", boundary.reason()); root.put("metrics", Map.of()); root.put("phaseEvents", List.of());
                }
                Files.writeString(originals.resolve(id + ".json"), BaselineJson.encode(root) + "\n", StandardCharsets.UTF_8);
            }
            System.out.println(boundary + ": seed 5개 × 세 방식(미지원 명시) 완료");
        }
        regenerate(folder, args[1]);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion=" + SCHEMA
                + "\ncases=" + expected(args[1]) + "\nprofile=" + args[1] + "\nviolations=0\n", StandardCharsets.UTF_8);
    }

    private static Properties properties(Path file) throws IOException {
        var result = new Properties();
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) { result.load(reader); }
        return result;
    }

    static void regenerate(Path folder, String profile) throws IOException {
        var condition = InterruptionExperiment.condition(profile);
        var unique = new HashSet<String>();
        var executionIds = new HashSet<String>();
        var rows = new ArrayList<Map<String, Object>>();
        String provenance = null;
        try (var paths = Files.list(folder.resolve("results"))) {
            for (Path file : paths.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()) {
                var root = BaselineJson.object(BaselineJson.decode(Files.readString(file, StandardCharsets.UTF_8)));
                ComparisonExperiment.require(SCHEMA.equals(root.get("resultSchemaVersion")) && "virtual".equals(root.get("timeDomain")), "invalid S6 schema/time");
                var boundary = InterruptionExperiment.Boundary.valueOf((String) root.get("boundary"));
                ComparisonExperiment.require(InterruptionExperiment.boundaries(profile).contains(boundary), "S6 boundary is not part of this profile");
                String variant = (String) root.get("variant");
                ComparisonExperiment.require(ComparisonExperiment.VARIANTS.contains(variant) && ("S6-" + boundary).equals(root.get("caseId")), "invalid S6 case/variant");
                Object seedValue = root.get("seed");
                ComparisonExperiment.require(seedValue instanceof Number, "missing seed");
                long seed = ((Number) seedValue).longValue();
                ComparisonExperiment.require(seed >= 1 && seed <= 5 && ((Number) seedValue).doubleValue() == seed, "invalid seed");
                ComparisonExperiment.require(unique.add(boundary + ":" + variant + ":" + seed)
                        && root.get("executionId") instanceof String && executionIds.add((String) root.get("executionId")), "duplicate S6 result");
                ComparisonExperiment.require(BaselineJson.decode(BaselineJson.encode(InterruptionExperiment.config(condition, boundary))).equals(root.get("condition")), "S6 condition mismatch");
                var model = BaselineJson.object(root.get("modelConfig"));
                ComparisonExperiment.require(model.get("batchTargetBytes") instanceof Number bytes
                        && bytes.doubleValue() == InterruptionExperiment.batchBytes(condition, boundary), "S6 batch model mismatch");
                var expectedCandidate = variant.equals("pre-copy") ? Map.of("stopThresholdBytes", condition.threshold(), "maxRounds", condition.maxRounds()) : Map.of();
                ComparisonExperiment.require(BaselineJson.decode(BaselineJson.encode(expectedCandidate)).equals(root.get("candidateConfig")), "S6 candidate mismatch");
                ComparisonExperiment.require(InterruptionExperiment.workloadHash(condition, seed).equals(root.get("workloadHash"))
                        && InterruptionExperiment.inputHash(condition, boundary, seed).equals(root.get("inputHash")), "S6 input mismatch");
                String signature = root.get("sourceHash") + ":" + root.get("backendHash") + ":" + root.get("suiteExecutionId") + ":" + root.get("runNumber");
                if (provenance == null) provenance = signature;
                ComparisonExperiment.require(provenance.equals(signature), "mixed S6 provenance");
                ComparisonExperiment.require(Boolean.valueOf(boundary.supported(variant)).equals(root.get("supported")), "unsupported S6 marked supported");
                if (boundary.supported(variant)) validateSupported(root, boundary, variant);
                else ComparisonExperiment.require("unsupported".equals(root.get("outcome")) && "not-applicable".equals(root.get("validationStatus"))
                        && boundary.reason().equals(root.get("unsupportedReason")) && Map.of().equals(root.get("metrics"))
                        && List.of().equals(root.get("phaseEvents")) && !root.containsKey("protocolOutcome")
                        && !root.containsKey("injectionState") && !root.containsKey("terminationReason"), "fabricated unsupported outcome/metrics");
                rows.add(root);
            }
        }
        ComparisonExperiment.require(rows.size() == expected(profile), "missing S6 results: " + rows.size());
        writeReport(folder, rows, profile);
    }

    private static void validateSupported(Map<String, Object> root, InterruptionExperiment.Boundary boundary, String variant) {
        ComparisonExperiment.require("completed".equals(root.get("outcome")) && "valid".equals(root.get("validationStatus"))
                && boundary.terminal().equals(root.get("protocolOutcome")), "invalid S6 terminal outcome");
        String termination = boundary.terminal().equals("FAILED") ? "expected-source-unavailable" : "expected-" + boundary.terminal();
        ComparisonExperiment.require(termination.equals(root.get("terminationReason")), "S6 termination reason mismatch");
        int expectedCommits = boundary.cancelsBeforeCommit() ? 0 : 1;
        ComparisonExperiment.require(metric(root, "invariant_violations") == 0 && metric(root, "ownership_commits") == expectedCommits,
                "S6 ownership/invariant result");
        ComparisonExperiment.require(metric(root, "source_exact_comparisons") == (boundary.cancelsBeforeCommit() ? 1 : 0)
                && metric(root, "destination_exact_comparisons") == (boundary.terminal().equals("DONE") ? 1 : 0)
                && metric(root, "retained_state_exact_comparisons") == (boundary.terminal().equals("FAILED") ? 1 : 0), "S6 oracle result mismatch");
        ComparisonExperiment.require(metric(root, "abort_attempts") == 1
                && metric(root, "abort_refused") == expectedCommits, "S6 abort contract missing");
        if (boundary == InterruptionExperiment.Boundary.PARTIAL_CLEANUP_ABORT) ComparisonExperiment.require(metric(root, "cleanup_calls") == 3, "S6 cleanup not exercised");
        if (boundary.sourceFailure()) ComparisonExperiment.require(metric(root, "reads_unavailable") > 0
                && metric(root, "failed_observation_ticks") >= InterruptionExperiment.OUTAGE_TICKS, "S6 source failure not observed");
        if (boundary == InterruptionExperiment.Boundary.PULL_TIMEOUT_RESUME) ComparisonExperiment.require(metric(root, "timeout_wait_ticks") == InterruptionExperiment.OUTAGE_TICKS, "S6 timeout not observed");
        for (var value : BaselineJson.object(root.get("metrics")).values()) {
            var observation = BaselineJson.object(value);
            ComparisonExperiment.require(observation.get("value") instanceof Number number && Double.isFinite(number.doubleValue()) && number.doubleValue() >= 0,
                    "invalid S6 metric");
        }
        String state = switch (boundary) {
            case COPYING_ABORT -> "COPYING";
            case BEFORE_COMMIT_ABORT -> "SWITCHING";
            case FINAL_ACK_WAIT_ABORT -> variant.equals("pre-copy") ? "SWITCHING" : "COPYING";
            case AFTER_COMMIT_ABORT -> variant.equals("post-copy") ? "PULLING" : "CLEANING_UP";
            case PARTIAL_CLEANUP_ABORT -> "CLEANING_UP";
            case CATCHING_UP_ABORT, CATCHING_UP_PARTIAL_ABORT -> "CATCHING_UP";
            case SOURCE_DOWN_RESUME, PULL_TIMEOUT_RESUME, SOURCE_DOWN_UNRECOVERED -> "PULLING";
        };
        ComparisonExperiment.require(state.equals(root.get("injectionState")), "S6 injection state mismatch");
        ComparisonExperiment.require(root.get("phaseEvents") instanceof List<?>, "missing S6 events");
        var events = (List<?>) root.get("phaseEvents");
        long injections = events.stream().map(BaselineJson::object).filter(e -> ("inject:" + boundary).equals(e.get("action"))
                && state.equals(e.get("state")) && ((Number) e.get("tick")).doubleValue() == metric(root, "injection_tick")).count();
        ComparisonExperiment.require(injections == 1 && !events.isEmpty(), "S6 injection not reached");
        if (boundary == InterruptionExperiment.Boundary.CATCHING_UP_PARTIAL_ABORT) {
            ComparisonExperiment.require(metric(root, "delta_bytes_before_abort") > 0
                    && metric(root, "delta_records_acknowledged_before_abort") > 0
                    && metric(root, "snapshot_bytes_before_delta") + metric(root, "delta_bytes_before_abort") == metric(root, "sent_bytes"),
                    "S6 partial delta was not transferred");
            var injection = events.stream().map(BaselineJson::object).filter(e -> ("inject:" + boundary).equals(e.get("action"))).findFirst().orElseThrow();
            ComparisonExperiment.require(Boolean.TRUE.equals(injection.get("partialLiveDelta"))
                    && injection.get("acknowledgedSequence") instanceof Number acknowledged
                    && acknowledged.doubleValue() == metric(root, "delta_records_acknowledged_before_abort"), "S6 partial delta ACK evidence mismatch");
            long starts = events.stream().map(BaselineJson::object).filter(e -> "live-delta-start".equals(e.get("action"))
                    && e.get("sentBytes") instanceof Number value && value.doubleValue() == metric(root, "snapshot_bytes_before_delta")
                    && ((Number) e.get("tick")).doubleValue() < metric(root, "injection_tick")).count();
            ComparisonExperiment.require(starts == 1, "S6 partial delta start evidence missing");
        }
        var terminal = BaselineJson.object(events.getLast());
        ComparisonExperiment.require("expected-terminal".equals(terminal.get("action")) && boundary.terminal().equals(terminal.get("state"))
                && ((Number) terminal.get("tick")).doubleValue() + 1 == metric(root, "total_ticks")
                && ((Number) terminal.get("sentBytes")).doubleValue() == metric(root, "sent_bytes")
                && metric(root, "action_to_terminal_ticks") == metric(root, "total_ticks") - 1 - metric(root, "injection_tick"), "S6 terminal evidence mismatch");
    }

    private static void writeReport(Path folder, List<Map<String, Object>> rows, String profile) throws IOException {
        long supported = rows.stream().filter(r -> Boolean.TRUE.equals(r.get("supported"))).count();
        long aborted = rows.stream().filter(r -> "ABORTED".equals(r.get("protocolOutcome"))).count();
        long done = rows.stream().filter(r -> "DONE".equals(r.get("protocolOutcome"))).count();
        long failed = rows.stream().filter(r -> "FAILED".equals(r.get("protocolOutcome"))).count();
        var csv = new StringBuilder("\uFEFF경계,방식,지원 여부,seed 수,종료 상태,쓰기 차단 최소 tick,쓰기 차단 최대 tick,주입 후 종료 최소 tick,주입 후 종료 최대 tick,전송 최소 bytes,전송 최대 bytes,취소 거부 수,미도착 조회 UNAVAILABLE 수,정합성 위반,미지원 이유\n");
        var md = new StringBuilder("# S6 취소·장애 경계 비교\n\n");
        md.append(BaselineResearchMain.provenance(folder)).append("\n\n");
        md.append(InterruptionExperiment.boundaries(profile).size()).append("개 경계 × seed 5개 × 세 방식 = ").append(rows.size()).append("개 결과 행. 실제 실행 ").append(supported)
                .append("회, 미지원 ").append(rows.size() - supported).append("행. 실행된 시나리오의 정합성 위반 0건.\n\n");
        md.append("예상 종결: 취소 ABORTED ").append(aborted).append("회, 이동 DONE ").append(done).append("회, source 미복구 FAILED ")
                .append(failed).append("회. FAILED는 이동 성공이 아니라 source 의존 데이터가 남아 정상 완료를 거부한 결과다. ")
                .append("ABORTED는 source 전체, DONE은 destination 전체, FAILED는 보존된 source 전체와 destination의 가용 데이터 전체를 각각 검사한다.\n\n");
        md.append("| 경계 | 방식 | 결과 | 쓰기 차단 tick | 주입 후 종료 tick | 전송 bytes | 취소 거부 | 미도착 UNAVAILABLE |\n")
                .append("|---|---|---|---:|---:|---:|---:|---:|\n");
        for (var boundary : InterruptionExperiment.boundaries(profile)) for (String variant : ComparisonExperiment.VARIANTS) {
            var group = rows.stream().filter(r -> boundary.name().equals(r.get("boundary")) && variant.equals(r.get("variant"))).toList();
            ComparisonExperiment.require(group.size() == 5, "missing S6 paired seed");
            boolean applicable = boundary.supported(variant);
            csv.append(boundary).append(',').append(variant).append(',').append(applicable).append(",5,");
            if (applicable) {
                csv.append(boundary.terminal()).append(',');
                for (String name : List.of("writes_blocked_ticks", "action_to_terminal_ticks", "sent_bytes")) csv.append(min(group, name)).append(',').append(max(group, name)).append(',');
                csv.append(sum(group, "abort_refused")).append(',').append(sum(group, "reads_unavailable")).append(",0,\n");
                md.append(String.format(Locale.ROOT, "| %s | %s | %s | %s | %s | %s | %d | %d |\n", boundary, variant, boundary.terminal(),
                        display(group, "writes_blocked_ticks"), display(group, "action_to_terminal_ticks"), display(group, "sent_bytes"),
                        sum(group, "abort_refused"), sum(group, "reads_unavailable")));
            } else {
                csv.append("unsupported,,,,,,,,,,").append(boundary.reason()).append('\n');
                md.append("| ").append(boundary).append(" | ").append(variant).append(" | 미지원 | — | — | — | — | — |\n");
            }
        }
        md.append("\n## 경계와 해석\n\n")
                .append("COPYING_ABORT는 복사에 byte를 소비한 뒤 취소한다. BEFORE_COMMIT_ABORT는 readyToCommit에서 취소한다. ")
                .append("FINAL_ACK_WAIT_ABORT는 마지막 ACK를 보류해 commit을 막은 뒤 취소한다. 이때 stop-and-copy의 실제 상태는 COPYING, pre-copy는 SWITCHING이다. ")
                .append("AFTER_COMMIT_ABORT는 소유권 전환 직후 취소를 거부하고 마무리한다. PARTIAL_CLEANUP_ABORT는 source 일부를 지운 정리 실패 뒤 취소 거부와 두 번의 실패·세 번째 성공을 검증한다. ")
                .append("CATCHING_UP_ABORT는 pre-copy의 실제 delta 라운드 진입 뒤 취소한다. ")
                .append("extended profile의 CATCHING_UP_PARTIAL_ABORT는 live delta의 마지막이 아닌 batch가 적용·ACK된 뒤, 같은 라운드가 끝나기 전에 취소한다. ")
                .append("이 조건의 batch 목표는 tick 예산의 절반(full 32 KiB, quick 2 KiB)이며 원본 condition/modelConfig에 기록한다.\n\n")
                .append("SOURCE_DOWN_RESUME는 post-copy source를 3 tick 사용할 수 없게 한 뒤 동일 데이터·fence로 복구한다. ")
                .append("PULL_TIMEOUT_RESUME는 pull 응답을 막고 3 tick 뒤 controller가 fail을 호출하며 FAILED를 3 tick 관찰한 뒤 복구한다. ")
                .append("장애 중에도 destination 쓰기·삭제를 성공시키고 미도착 GET의 UNAVAILABLE와 취소 거부를 확인한다. ")
                .append("SOURCE_DOWN_UNRECOVERED는 source를 복구하지 않은 채 FAILED를 관찰하고 source·소유권·성공 쓰기가 보존됨을 검사한다. source의 물리적 영구 삭제와 자동 데이터 복구를 구현한 실험은 아니다.\n\n")
                .append("가상 시간·단일 controller·인메모리 모델이다. 모든 경계/방식에 같은 seed의 초기 데이터·tick별 요청 시도를 재생하지만 경계에 도달하는 시점·성공 요청 집합은 다르다. ")
                .append("주입 후 종료 tick은 시나리오 동작을 포함하며 실제 장애 감지 latency나 동일 작업량의 속도 비교가 아니다. 미지원에는 0이나 차이값을 채우지 않는다. ")
                .append("workloadHash와 inputHash는 요청 생성 및 경계 규격 hash이며 materialized trace 전체 hash가 아니다. ")
                .append("S6 전용 b3-s6-json-1이며 S1~S5/S3 결과 및 계획된 dirty 라운드 상세 v2와 합산하지 않는다.\n\n")
                .append("## 미지원 이유\n\n");
        for (var boundary : InterruptionExperiment.boundaries(profile)) if (ComparisonExperiment.VARIANTS.stream().anyMatch(v -> !boundary.supported(v))) {
            md.append("- ").append(boundary).append(": ").append(boundary.reason()).append('\n');
        }
        Files.writeString(folder.resolve("summary.csv"), csv, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("results.md"), md, StandardCharsets.UTF_8);
    }

    static double metric(Map<String, Object> row, String name) { return ComparisonResearchMain.metric(row, name); }
    private static long min(List<Map<String, Object>> rows, String name) { return (long) rows.stream().mapToDouble(r -> metric(r, name)).min().orElseThrow(); }
    private static long max(List<Map<String, Object>> rows, String name) { return (long) rows.stream().mapToDouble(r -> metric(r, name)).max().orElseThrow(); }
    private static long sum(List<Map<String, Object>> rows, String name) { return (long) rows.stream().mapToDouble(r -> metric(r, name)).sum(); }
    private static String display(List<Map<String, Object>> rows, String name) { return min(rows, name) == max(rows, name) ? Long.toString(min(rows, name)) : min(rows, name) + "~" + max(rows, name); }
    private InterruptionResearchMain() {}
}
