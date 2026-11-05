package io.openkache.runner;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * B2 cluster-placement EC2 결과 요약기. 공통 Runner 코드가 아니라 B2 실험용 도구다(test 소스에 둔다).
 *
 * <p>S3에서 받은 실행 폴더들(각 폴더에 {@code config.properties}와 {@code b2-cluster-placement/windows-*.csv})을 읽어 (시나리오,
 * 정책)별로 seed를 모으고 {@code greedy-load-placement} 대비 개선율(seed 짝 평균)을 계산해 Markdown 표를 쓴다.
 *
 * <p>사용법: {@code ClusterPlacementReport <받은 결과 루트> <출력 md>}
 */
final class ClusterPlacementReport {

    static final String BASELINE = "greedy-load-placement";

    /** 실행 하나의 지표. 이용률·지연은 window 평균이다. */
    record Run(String scenario, int seed, String variant, Map<String, Double> metrics) {}

    /** 지표 이름, 표 제목, 낮을수록 좋은가. */
    static final String[][] METRICS = {
        {"throughput", "처리량(req/s)", "false"},
        {"mean_imbalance", "평균 최대/평균 요청", "true"},
        {"mean_max_cpu", "평균 최대 노드 CPU", "true"},
        {"mean_worst_p99_us", "평균 최악 노드 p99(μs)", "true"},
        {"miss_rate", "GET miss 비율", "true"},
        {"moved_tablets", "이동 Tablet", "true"},
        {"refills", "다시 채운 SET", "true"},
    };

    private ClusterPlacementReport() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: ClusterPlacementReport <downloaded results root> <out.md>");
            System.exit(2);
        }
        List<Run> runs = new ArrayList<>();
        try (Stream<Path> configs = Files.walk(Path.of(args[0]))) {
            for (Path config : configs.filter(p -> p.getFileName().toString().equals("config.properties")).sorted().toList()) {
                Run run = read(config.getParent());
                if (run != null) {
                    runs.add(run);
                }
            }
        }
        Files.write(Path.of(args[1]), markdown(runs), StandardCharsets.UTF_8);
        System.err.println("runs=" + runs.size() + " wrote " + args[1]);
    }

    static Run read(Path dir) throws IOException {
        Properties p = new Properties();
        try (Reader reader = Files.newBufferedReader(dir.resolve("config.properties"), StandardCharsets.UTF_8)) {
            p.load(reader);
        }
        if (!"b2-cluster-placement".equals(p.getProperty("study"))) {
            return null;
        }
        Path csv;
        try (Stream<Path> files = Files.walk(dir)) {
            csv = files.filter(f -> f.getFileName().toString().startsWith("windows-")).findFirst().orElse(null);
        }
        if (csv == null) {
            return null;
        }
        // window → 서버별 행
        Map<Integer, List<String[]>> byWindow = new TreeMap<>();
        List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
        List<String> header = Arrays.asList(lines.getFirst().split(","));
        for (String line : lines.subList(1, lines.size())) {
            String[] f = line.split(",");
            byWindow.computeIfAbsent(Integer.parseInt(f[header.indexOf("window")]), k -> new ArrayList<>()).add(f);
        }
        double requests = 0;
        double elapsed = 0;
        double imbalance = 0;
        double maxCpu = 0;
        double worstP99 = 0;
        double hits = 0;
        double misses = 0;
        double moved = 0;
        double refills = 0;
        for (List<String[]> rows : byWindow.values()) {
            double[] perNode = rows.stream().mapToDouble(r -> num(r, header, "requests")).filter(x -> x > 0).toArray();
            double total = Arrays.stream(perNode).sum();
            requests += total;
            elapsed += num(rows.getFirst(), header, "elapsedNanos") / 1e9;
            imbalance += perNode.length == 0 ? 1 : Arrays.stream(perNode).max().orElse(0) / (total / perNode.length);
            maxCpu += rows.stream().mapToDouble(r -> num(r, header, "cpuUtilization")).filter(x -> !Double.isNaN(x)).max().orElse(Double.NaN);
            worstP99 += rows.stream().filter(r -> num(r, header, "requests") > 0).mapToDouble(r -> num(r, header, "p99Nanos")).max().orElse(0) / 1e3;
            hits += rows.stream().mapToDouble(r -> num(r, header, "hit")).sum();
            misses += rows.stream().mapToDouble(r -> num(r, header, "miss")).sum();
            moved += num(rows.getFirst(), header, "movedTablets");
            refills += rows.stream().mapToDouble(r -> num(r, header, "refills")).sum();
        }
        int windows = byWindow.size();
        Map<String, Double> m = new LinkedHashMap<>();
        m.put("throughput", requests / elapsed);
        m.put("mean_imbalance", imbalance / windows);
        m.put("mean_max_cpu", maxCpu / windows);
        m.put("mean_worst_p99_us", worstP99 / windows);
        m.put("miss_rate", hits + misses == 0 ? 0 : misses / (hits + misses));
        m.put("moved_tablets", moved);
        m.put("refills", refills);
        String scenario = p.getProperty("scenario");
        int dash = scenario.lastIndexOf("-s");
        return new Run(scenario.substring(0, dash), Integer.parseInt(scenario.substring(dash + 2)), p.getProperty("variant"), m);
    }

    static List<String> markdown(List<Run> runs) {
        List<String> md = new ArrayList<>();
        md.add("# cluster-placement EC2 결과 요약 (ClusterPlacementReport 자동 생성)");
        md.add("");
        md.add("값은 seed 평균 (최소-최대). 괄호 아래 줄은 `" + BASELINE + "` 대비 개선율(같은 seed끼리 짝지은 평균, 양수가 개선). "
                + "실행 수: " + runs.size());
        Map<String, Map<String, List<Run>>> grouped = new TreeMap<>();
        for (Run r : runs) {
            grouped.computeIfAbsent(r.scenario(), k -> new TreeMap<>()).computeIfAbsent(r.variant(), k -> new ArrayList<>()).add(r);
        }
        grouped.forEach((scenario, byVariant) -> {
            md.add("");
            md.add("## " + scenario);
            md.add("");
            StringBuilder head = new StringBuilder("| policy | seeds |");
            StringBuilder line = new StringBuilder("|---|---|");
            for (String[] metric : METRICS) {
                head.append(' ').append(metric[1]).append(" |");
                line.append("---|");
            }
            md.add(head.toString());
            md.add(line.toString());
            List<Run> baseline = byVariant.getOrDefault(BASELINE, List.of());
            byVariant.forEach((variant, list) -> {
                StringBuilder row = new StringBuilder("| `" + variant + "` | " + list.size() + " |");
                for (String[] metric : METRICS) {
                    double[] v = list.stream().mapToDouble(r -> r.metrics().get(metric[0])).filter(x -> !Double.isNaN(x)).toArray();
                    String cell = v.length == 0 ? "-" : String.format(Locale.ROOT, "%.3f (%.3f-%.3f)",
                            Arrays.stream(v).average().orElseThrow(), Arrays.stream(v).min().orElseThrow(), Arrays.stream(v).max().orElseThrow());
                    if (!variant.equals(BASELINE)) {
                        cell += "<br>" + improvement(baseline, list, metric[0], Boolean.parseBoolean(metric[2]));
                    }
                    row.append(' ').append(cell).append(" |");
                }
                md.add(row.toString());
            });
        });
        return md;
    }

    private static String improvement(List<Run> baseline, List<Run> candidate, String metric, boolean lowerIsBetter) {
        List<Double> pct = new ArrayList<>();
        for (Run c : candidate) {
            for (Run b : baseline) {
                double bv = b.metrics().get(metric);
                double cv = c.metrics().get(metric);
                if (b.seed() == c.seed() && bv != 0 && !Double.isNaN(bv) && !Double.isNaN(cv)) {
                    pct.add((lowerIsBetter ? bv - cv : cv - bv) / Math.abs(bv) * 100);
                }
            }
        }
        return pct.isEmpty() ? "NA" : String.format(Locale.ROOT, "%+.1f%% (n=%d)", pct.stream().mapToDouble(Double::doubleValue).average().orElseThrow(), pct.size());
    }

    private static double num(String[] row, List<String> header, String column) {
        String v = row[header.indexOf(column)];
        return v.equals("NA") ? Double.NaN : Double.parseDouble(v);
    }
}
