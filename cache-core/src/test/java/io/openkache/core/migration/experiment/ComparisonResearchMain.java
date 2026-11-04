package io.openkache.core.migration.experiment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/** 공통 Runner 연결. 비교 JSON 원본을 먼저 저장하고 검증 후 CSV·Markdown을 다시 생성한다. */
public final class ComparisonResearchMain {
    static final String SCHEMA = "b3-comparison-json-1";

    public static void main(String[] args) throws IOException {
        if (args.length == 2 && args[0].equals("--rebuild")) {
            Path folder = Path.of(args[1]).toAbsolutePath().normalize();
            Properties completion = readProperties(folder.resolve("completion.properties"));
            if (!SCHEMA.equals(completion.getProperty("resultSchemaVersion"))) throw new IllegalArgumentException("schema mismatch");
            regenerate(folder, completion.getProperty("profile"));
            return;
        }
        if (args.length != 4) {
            throw new IllegalArgumentException("<출력 폴더> <full|quick|sensitivity-full|sensitivity-quick> <반복 번호> <전체 반복 수>");
        }
        var cases = ComparisonExperiment.cases(args[1]);
        Path folder = Path.of(args[0]).toAbsolutePath().normalize();
        int repeat = Integer.parseInt(args[2]), repeats = Integer.parseInt(args[3]);
        if (repeat < 1 || repeat > repeats) throw new IllegalArgumentException("invalid repeat");
        Properties launch = readProperties(folder.resolve("launch.properties"));
        Map<String, Object> context = BaselineResearchMain.context(launch, repeat, repeats);
        context.put("modelConfig", Map.of("keyBytes", 16, "valueBytes", 128, "recordBytes", 152,
                "batchTargetBytes", 65_536, "generatorVersion", ComparisonExperiment.GENERATOR,
                "postCopyReplyHeaderBytes", 33, "timeUnit", "tick", "controlTrafficCounted", false));
        Path originals = Files.createDirectory(folder.resolve("results"));
        for (var c : cases) {
            for (int seed = 1; seed <= 5; seed++) {
                for (String variant : ComparisonExperiment.VARIANTS) {
                    var run = ComparisonExperiment.run(c, seed, variant);
                    String id = c.id() + "-seed" + seed + "-" + variant;
                    Map<String, Object> root = new LinkedHashMap<>(context);
                    root.put("resultSchemaVersion", SCHEMA);
                    root.put("executionId", context.get("suiteExecutionId") + "-repeat" + repeat + "-" + id);
                    root.put("resultFile", "results/" + id + ".json");
                    root.put("caseId", c.id()); root.put("candidateId", variant); root.put("variant", variant);
                    root.put("study", "b3-live-migration"); root.put("scenario", c.scenario()); root.put("seed", seed);
                    root.put("condition", c.config());
                    root.put("candidateConfig", variant.equals("pre-copy")
                            ? Map.of("stopThresholdBytes", c.threshold(), "maxRounds", c.maxRounds()) : Map.of());
                    root.put("inputHash", run.inputHash()); root.put("inputHashKind", "generator-specification-sha256");
                    root.put("workloadHash", ComparisonExperiment.workloadHash(c, seed));
                    root.put("workloadHashKind", "generator-specification-without-protocol-settings-sha256");
                    root.put("timeDomain", "virtual"); root.put("outcome", "completed");
                    root.put("validationStatus", "valid"); root.put("terminationReason", "completed");
                    root.put("stopPhaseReason", run.stopReason()); root.put("metrics", run.metrics());
                    root.put("phaseEvents", run.phases());
                    Files.writeString(originals.resolve(id + ".json"), BaselineJson.encode(root) + "\n", StandardCharsets.UTF_8);
                }
            }
            System.out.println(c.id() + ": 5 seeds × 3 variants 완료");
        }
        regenerate(folder, args[1]);
        Files.writeString(folder.resolve("completion.properties"), "status=complete\nresultSchemaVersion=" + SCHEMA
                + "\ncases=" + cases.size() * 15 + "\nprofile=" + args[1] + "\nviolations=0\n", StandardCharsets.UTF_8);
    }

    private static Properties readProperties(Path path) throws IOException {
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) { properties.load(reader); }
        return properties;
    }

    static void regenerate(Path folder, String profile) throws IOException {
        var cases = ComparisonExperiment.cases(profile);
        boolean sensitivity = ComparisonExperiment.sensitivity(profile);
        Map<String, ComparisonExperiment.Case> expectedCases = new HashMap<>();
        cases.forEach(c -> expectedCases.put(c.id(), c));
        var unique = new HashSet<String>();
        var executionIds = new HashSet<String>();
        var rows = new ArrayList<Map<String, Object>>();
        Map<String, String> inputs = new HashMap<>();
        String provenance = null;
        try (var files = Files.list(folder.resolve("results"))) {
            for (Path file : files.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()) {
                var root = BaselineJson.object(BaselineJson.decode(Files.readString(file, StandardCharsets.UTF_8)));
                ComparisonExperiment.require(SCHEMA.equals(root.get("resultSchemaVersion")), "unexpected schema");
                ComparisonExperiment.require("virtual".equals(root.get("timeDomain"))
                        && "valid".equals(root.get("validationStatus")) && "completed".equals(root.get("outcome")), "invalid run");
                String caseId = (String) root.get("caseId"), variant = (String) root.get("variant");
                ComparisonExperiment.require(expectedCases.containsKey(caseId) && ComparisonExperiment.VARIANTS.contains(variant), "unknown case/variant");
                int seed = ((Number) root.get("seed")).intValue();
                ComparisonExperiment.require(seed >= 1 && seed <= 5, "unexpected seed");
                ComparisonExperiment.require(unique.add(caseId + ":" + seed + ":" + variant)
                        && executionIds.add((String) root.get("executionId")), "duplicate result");
                var actual = BaselineJson.object(root.get("condition"));
                expectedCases.get(caseId).config().forEach((name, value) -> ComparisonExperiment.require(
                        actual.get(name) instanceof Number number && Double.compare(number.doubleValue(), ((Number) value).doubleValue()) == 0,
                        "condition mismatch " + name));
                var condition = expectedCases.get(caseId);
                var candidate = BaselineJson.object(root.get("candidateConfig"));
                ComparisonExperiment.require(candidate.equals(variant.equals("pre-copy")
                        ? BaselineJson.decode(BaselineJson.encode(Map.of("stopThresholdBytes", condition.threshold(),
                                "maxRounds", condition.maxRounds()))) : Map.of()), "candidate settings mismatch");
                if (sensitivity) {
                    ComparisonExperiment.require(ComparisonExperiment.workloadHash(condition, seed).equals(root.get("workloadHash")),
                            "workload specification hash mismatch");
                    if (variant.equals("pre-copy")) {
                        double rounds = metric(root, "live_delta_rounds");
                        ComparisonExperiment.require(rounds >= 0 && rounds <= condition.maxRounds() && rounds == Math.rint(rounds), "invalid rounds");
                        ComparisonExperiment.require(List.of("THRESHOLD", "ROUND_LIMIT", "NOT_SHRINKING").contains(root.get("stopPhaseReason")),
                                "invalid stop reason");
                    } else ComparisonExperiment.require(root.get("stopPhaseReason") == null, "unexpected stop reason");
                }
                ComparisonExperiment.require(metric(root, "invariant_violations") == 0
                        && metric(root, "exact_comparisons") == 1, "invariant result");
                String signature = root.get("sourceHash") + ":" + root.get("backendHash") + ":" + root.get("suiteExecutionId")
                        + ":" + root.get("runNumber");
                if (provenance == null) provenance = signature;
                ComparisonExperiment.require(provenance.equals(signature), "mixed provenance");
                String hash = (String) root.get("inputHash");
                String previous = inputs.putIfAbsent(caseId + ":" + seed, hash);
                ComparisonExperiment.require(previous == null || previous.equals(hash), "unpaired input");
                String computedInput = ComparisonExperiment.GENERATOR + ":seed=" + seed + ":"
                        + BaselineJson.encode(new java.util.TreeMap<>(expectedCases.get(caseId).config()));
                ComparisonExperiment.require(ComparisonExperiment.sha256(computedInput).equals(hash), "input specification hash mismatch");
                rows.add(root);
            }
        }
        ComparisonExperiment.require(rows.size() == cases.size() * 15, "missing results " + rows.size());
        if (sensitivity) {
            writeSensitivityReport(folder, cases, rows);
            return;
        }
        StringBuilder csv = new StringBuilder("\uFEFF조건 ID,시나리오,초기 키 수,쓰기율 rho,삭제 비율,Zipf theta,방식,seed 수,쓰기 차단 최소 tick,쓰기 차단 최대 tick,총 시간 최소 tick,총 시간 최대 tick,전송량 비율 최소,전송량 비율 최대,조회 BUSY 비율 최소,조회 BUSY 비율 최대,정합성 위반\n");
        StringBuilder md = new StringBuilder("# B3 복사 방식 비교 실험\n\n");
        md.append(BaselineResearchMain.provenance(folder)).append("\n\n");
        md.append("S1~S5의 16조건 × seed 5개 × 3방식, 총 ").append(rows.size()).append("회. 정합성 위반 0건, destination 전체 비교 ")
                .append(rows.size()).append('/').append(rows.size()).append("건 일치.\n\n");
        md.append("동일 case/seed의 초기 데이터와 tick별 GET/SET/DELETE **시도**는 같다. BUSY인 쓰기는 적용하지 않는다. 실행 길이가 다르므로 각 방식이 소비한 요청열 길이·성공 쓰기 수는 다르다. ")
                .append("inputHash는 generator·설정·seed의 규격 hash이며 실제 materialized trace hash가 아니다.\n\n");
        md.append("가상 tick·인메모리 모델이다. 전체 시간은 begin~cleanup 완료, 쓰기 차단은 누락 key DELETE 제어 요청의 BUSY tick 수다. ")
                .append("제어 요청은 데이터/dirty/workload에 영향을 주지 않는다. 단계/라운드 경계는 각 원본의 phaseEvents에 기록한다.\n\n");
        md.append("전송량은 직렬화된 데이터 응답 bytes다. snapshot 헤더 29, delta 헤더 30, post-copy key별 응답 헤더 33 bytes를 포함하고 ACK/pull 요청의 제어 bytes는 제외한다. ")
                .append("전송량 비율 분모는 이동 전 N×152 bytes다. 삭제로 줄어든 최종 크기도 JSON에 별도로 기록한다. post-copy의 미도착 GET은 BUSY이며 false MISS로 간주하지 않는다.\n\n");
        md.append("표는 seed 5개의 최솟값~최댓값이다. 같은 seed 반복의 신뢰구간이나 실제 지연 시간으로 해석하지 않는다. [조건별 CSV](summary.csv), 원본 results/*.json(로컬 보관).\n\n");
        md.append("| 조건 | N | rho | 삭제 % | θ | 방식 | 쓰기 차단 tick | 전체 tick | 전송량/초기 크기 | 조회 BUSY 비율 |\n")
                .append("|---|---:|---:|---:|---:|---|---:|---:|---:|---:|\n");
        for (var c : cases) for (String variant : ComparisonExperiment.VARIANTS) {
            List<Map<String, Object>> group = rows.stream().filter(r -> c.id().equals(r.get("caseId")) && variant.equals(r.get("variant"))).toList();
            ComparisonExperiment.require(group.size() == 5, "missing paired seed");
            csv.append(String.format(Locale.ROOT, "%s,%s,%d,%.2f,%d,%.2f,%s,5,", c.id(), c.scenario(), c.keys(), c.rho(), c.deletePercent(), c.theta(), variant));
            for (String metric : List.of("writes_blocked_ticks", "total_ticks", "transfer_ratio", "read_busy_ratio")) {
                double[] range = range(group, metric);
                csv.append(String.format(Locale.ROOT, "%.6f,%.6f,", range[0], range[1]));
            }
            csv.append("0\n");
            md.append(String.format(Locale.ROOT, "| %s | %,d | %.2f | %d | %.2f | %s | %s | %s | %s | %s |\n", c.id(), c.keys(), c.rho(), c.deletePercent(), c.theta(), variant,
                    display(group, "writes_blocked_ticks", true), display(group, "total_ticks", true), display(group, "transfer_ratio", false), display(group, "read_busy_ratio", false)));
        }
        md.append("\n## 범위와 제한\n\n");
        md.append("균등 분포와 finite Zipf CDF(역변환, 고정 rank→key permutation)를 이 backend에서 구현했다. 공통 Workload Generator를 수정하지 않는다. ")
                .append("쓰기율 rho는 시도당 152 bytes 기준이며 DELETE의 tombstone 크기·같은 key 반복으로 실제 dirty 전송량과 같지 않다. ")
                .append("초기 값은 모두 같은 128-byte payload이고 이후 SET은 tick/연산 번호를 담는다. post-copy는 key manifest를 수집한 뒤 commit하며 이 준비/CAS의 실제 비용은 모델에서 측정하지 않는다.\n\n");
        md.append("S6의 취소·장애 경계는 기존 프로토콜 테스트로 별도 검증한다. 이번 suite는 S6 성능 비교·S3 임계/라운드 민감도·E/M scaling·실제 서버 실험을 포함하지 않는다. ")
                .append("기존 SC 결과와 이번 결과는 별도 schema이며 자동 합산하지 않는다.\n");
        Files.writeString(folder.resolve("summary.csv"), csv, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("results.md"), md, StandardCharsets.UTF_8);
    }

    private static void writeSensitivityReport(Path folder, List<ComparisonExperiment.Case> cases,
            List<Map<String, Object>> rows) throws IOException {
        StringBuilder csv = new StringBuilder("\uFEFF조건 ID,시나리오,초기 키 수,쓰기율 rho,방식,종료 임계 bytes,라운드 상한,seed 수,쓰기 차단 최소 tick,쓰기 차단 최대 tick,총 시간 최소 tick,총 시간 최대 tick,전송량 비율 최소,전송량 비율 최대,live 라운드 최소,live 라운드 최대,THRESHOLD 종료 수,ROUND_LIMIT 종료 수,NOT_SHRINKING 종료 수,정합성 위반\n");
        StringBuilder md = new StringBuilder("# S3 pre-copy 설정 민감도 실험\n\n");
        md.append(BaselineResearchMain.provenance(folder)).append("\n\n");
        md.append("쓰기율 5종 × 종료 임계 3종 × 라운드 상한 3종 = 45조건 × seed 5개 × 세 방식, 총 ")
                .append(rows.size()).append("회. 정합성 위반 0건, 최종 데이터 ").append(rows.size()).append('/').append(rows.size()).append(" 일치.\n\n");
        md.append("표는 seed 5개의 최솟값~최댓값이다. 종료 사유는 THRESHOLD/ROUND_LIMIT/NOT_SHRINKING 순서의 실행 건수다. ")
                .append("종료 임계와 라운드 상한은 pre-copy에만 적용하며 다른 방식은 —로 표시한다. 조건 ID에는 비교 상대의 설정이 포함된다.\n\n");
        md.append("같은 rho/seed는 모든 설정과 방식에 동일한 초기 데이터·tick별 요청 **시도**를 재생한다. ")
                .append("workloadHash는 알고리즘 설정을 제외한 생성 규격 hash이며 원본 trace 전체 hash가 아니다. 기존 inputHash는 설정을 포함한 규격 hash로 유지한다. ")
                .append("설정별 완료 시점·성공 쓰기 수가 달라 같은 성공 작업량의 성능 비교로 해석하지 않는다.\n\n");
        md.append("| 조건 | N | rho | 방식 | 임계 bytes | 라운드 상한 | 쓰기 차단 tick | 전체 tick | 전송량/초기 크기 | live 라운드 | 종료 사유 T/R/N |\n")
                .append("|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|\n");
        for (var c : cases) for (String variant : ComparisonExperiment.VARIANTS) {
            var group = rows.stream().filter(r -> c.id().equals(r.get("caseId")) && variant.equals(r.get("variant"))).toList();
            ComparisonExperiment.require(group.size() == 5, "missing paired seed");
            boolean pre = variant.equals("pre-copy");
            csv.append(String.format(Locale.ROOT, "%s,%s,%d,%.2f,%s,%s,%s,5,", c.id(), c.scenario(), c.keys(), c.rho(), variant,
                    pre ? Long.toString(c.threshold()) : "", pre ? Integer.toString(c.maxRounds()) : ""));
            for (String name : List.of("writes_blocked_ticks", "total_ticks", "transfer_ratio")) {
                double[] values = range(group, name);
                csv.append(String.format(Locale.ROOT, "%.6f,%.6f,", values[0], values[1]));
            }
            long threshold = reasonCount(group, "THRESHOLD"), limit = reasonCount(group, "ROUND_LIMIT"), shrinking = reasonCount(group, "NOT_SHRINKING");
            if (pre) {
                double[] rounds = range(group, "live_delta_rounds");
                csv.append(String.format(Locale.ROOT, "%.0f,%.0f,%d,%d,%d,", rounds[0], rounds[1], threshold, limit, shrinking));
            } else csv.append(",,,,,");
            csv.append("0\n");
            md.append(String.format(Locale.ROOT, "| %s | %,d | %.2f | %s | %s | %s | %s | %s | %s | %s | %s |\n",
                    c.id(), c.keys(), c.rho(), variant, pre ? Long.toString(c.threshold()) : "—", pre ? Integer.toString(c.maxRounds()) : "—",
                    display(group, "writes_blocked_ticks", true), display(group, "total_ticks", true), display(group, "transfer_ratio", false),
                    pre ? display(group, "live_delta_rounds", true) : "—", pre ? threshold + "/" + limit + "/" + shrinking : "—"));
        }
        md.append("\n## 범위와 제한\n\n가상 시간·인메모리 모델이며 실제 서버 latency·throughput은 측정하지 않았다. ")
                .append("쓰기 차단은 누락 key DELETE 제어 요청의 BUSY tick 수다. 전송량은 migration 데이터 응답 framing을 포함하고 ACK·pull 요청과 client 통신 bytes는 제외한다. ")
                .append("rho는 쓰기 시도당 152 bytes 기준이며 DELETE·중복 쓰기로 실제 dirty bytes와 다를 수 있다. NOT_SHRINKING은 이 모델의 관측 결과이며 보편적인 수렴 한계가 아니다.\n\n")
                .append("quick은 N=1,000, B=4 KiB/tick과 임계 4/16/64 KiB를 쓰는 재현 검증용이다. full은 N=100,000, B=64 KiB/tick과 임계 64/256/1,024 KiB다. 두 결과를 합산하지 않는다. ")
                .append("phaseEvents는 상태·라운드 경계이며 dirty generation별 상세 byte 원본을 대체하지 않는다. S6·E/M scaling·실제 KV/peer 통합은 포함하지 않는다.\n");
        Files.writeString(folder.resolve("summary.csv"), csv, StandardCharsets.UTF_8);
        Files.writeString(folder.resolve("results.md"), md, StandardCharsets.UTF_8);
    }

    private static long reasonCount(List<Map<String, Object>> rows, String reason) {
        return rows.stream().filter(r -> reason.equals(r.get("stopPhaseReason"))).count();
    }

    static double metric(Map<String, Object> root, String name) {
        var metrics = BaselineJson.object(root.get("metrics"));
        return ((Number) BaselineJson.object(metrics.get(name)).get("value")).doubleValue();
    }
    private static double[] range(List<Map<String, Object>> group, String name) {
        return new double[] {group.stream().mapToDouble(r -> metric(r, name)).min().orElseThrow(),
                group.stream().mapToDouble(r -> metric(r, name)).max().orElseThrow()};
    }
    private static String display(List<Map<String, Object>> group, String name, boolean integer) {
        double[] range = range(group, name);
        String min = String.format(Locale.ROOT, integer ? "%.0f" : "%.4f", range[0]);
        return range[0] == range[1] ? min : min + "~" + String.format(Locale.ROOT, integer ? "%.0f" : "%.4f", range[1]);
    }
    private ComparisonResearchMain() {}
}
