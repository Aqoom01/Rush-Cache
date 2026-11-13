package io.openkache.core.integration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.stream.Collectors;

/** 통합 조합 screening, seed별 짝비교, 독립 seed 재검증을 수행하는 명시 호출 진입점. */
public final class SystemIntegrationMain {
    private SystemIntegrationMain() { }
    record Row(SystemVariant variant, IntegratedCluster.Measurement m) { }
    public static void main(String[] args) throws Exception {
        if (args.length == 0 || Arrays.equals(args, new String[]{"--list"})) {
            SystemVariant.all().forEach(v -> System.out.println(v.id()));
            return;
        }
        if (args.length != 2) throw new IllegalArgumentException("usage: SystemIntegrationMain <config.properties> <new-output-dir>");
        Properties p = new Properties();
        try (var reader = Files.newBufferedReader(Path.of(args[0]), StandardCharsets.UTF_8)) { p.load(reader); }
        Set<String> allowed = Set.of("seeds", "confirmation.seeds", "scenarios", "variants", "node.service.micros");
        for (String key : p.stringPropertyNames()) if (!allowed.contains(key)) throw new IllegalArgumentException("unknown key: " + key);
        List<Long> seeds = numbers(p.getProperty("seeds", "1,2,3"));
        List<Long> confirmation = numbers(p.getProperty("confirmation.seeds", "11,12,13,14,15"));
        if (seeds.size() < 2 || confirmation.size() < 2 || confirmation.stream().anyMatch(seeds::contains))
            throw new IllegalArgumentException("screening/confirmation은 각각 독립 seed 2개 이상이어야 한다");
        List<String> scenarios = values(p.getProperty("scenarios", "uniform,hotspot,crash-overlap,jitter"));
        if (!Set.of("uniform", "hotspot", "crash-overlap", "jitter").containsAll(scenarios)) throw new IllegalArgumentException("unknown scenario");
        List<SystemVariant> variants = SystemVariant.all();
        if (p.containsKey("variants")) {
            Set<String> ids = new HashSet<>(values(p.getProperty("variants")));
            variants = variants.stream().filter(v -> ids.remove(v.id())).toList();
            if (!ids.isEmpty()) throw new IllegalArgumentException("unsupported variants: " + ids);
        }
        Path output = Path.of(args[1]);
        Files.createDirectories(output.toAbsolutePath().getParent());
        Files.createDirectory(output); // 기존 증거를 덮어쓰지 않는다.
        Files.copy(Path.of(args[0]), output.resolve("config.properties"));
        Files.writeString(output.resolve("manifest.json"), manifest(args[0], seeds, confirmation, variants.size(), scenarios));
        int serviceMicros = Integer.parseInt(p.getProperty("node.service.micros", "4000"));
        if (serviceMicros < 0 || serviceMicros > 10000) throw new IllegalArgumentException("node.service.micros must be 0..10000");
        List<Row> rows = new ArrayList<>();
        int total = variants.size() * seeds.size() * scenarios.size();
        try (var writer = Files.newBufferedWriter(output.resolve("summary.csv"))) {
            writer.write(header());
            for (String scenario : scenarios) for (long seed : seeds) for (SystemVariant variant : shuffled(variants, seed)) {
                var row = run(variant, scenario, seed, serviceMicros); rows.add(row); writer.write(csv(row)); writer.flush();
                if (rows.size() % 60 == 0) System.out.println("screening " + rows.size() + "/" + total);
            }
        }
        Files.writeString(output.resolve("screening.json"), raw(rows));
        writeAggregate(output.resolve("aggregate.csv"), rows);
        writePairs(output.resolve("conditional-effects.csv"), rows);
        List<Row> heldout = new ArrayList<>();
        Map<String, SystemVariant> selected = new LinkedHashMap<>();
        for (String scenario : scenarios) {
            SystemVariant winner = variants.stream().filter(v -> eligible(rows, v, scenario, seeds.size()))
                    .max(Comparator.comparingDouble((SystemVariant v) -> mean(rows, v, scenario)).thenComparing(SystemVariant::id)).orElse(null);
            if (winner == null) continue;
            selected.put(scenario, winner);
            // 선택된 조합과 모든 구현 축의 단일 변경 이웃을 독립 seed에서 재검증한다.
            for (SystemVariant v : variants) if (distance(v, winner) <= 1)
                for (long seed : confirmation) heldout.add(run(v, scenario, seed, serviceMicros));
        }
        Files.writeString(output.resolve("confirmation.csv"), header() + heldout.stream().map(SystemIntegrationMain::csv).collect(Collectors.joining()));
        Files.writeString(output.resolve("confirmation.json"), raw(heldout));
        writeAggregate(output.resolve("confirmation-aggregate.csv"), heldout);
        writePairs(output.resolve("confirmation-effects.csv"), heldout);
        Files.writeString(output.resolve("results.md"), report(rows, heldout, scenarios, selected, seeds.size(), confirmation.size(), serviceMicros));
        Files.writeString(output.resolve("COMPLETE"), "screening=" + rows.size() + " confirmation=" + heldout.size() + "\n");
        System.out.println("complete " + output + " screening=" + rows.size() + " confirmation=" + heldout.size());
    }
    static List<SystemVariant> shuffled(List<SystemVariant> variants, long seed) {
        List<SystemVariant> copy = new ArrayList<>(variants); Collections.shuffle(copy, new Random(seed)); return copy;
    }
    static Row run(SystemVariant variant, String scenario, long seed, int serviceMicros) {
        try (var cluster = new IntegratedCluster(variant, scenario, seed, serviceMicros)) { return new Row(variant, cluster.run()); }
    }
    static List<String> values(String s) {
        List<String> v = Arrays.stream(s.split(",", -1)).map(String::trim).toList();
        if (v.isEmpty() || v.stream().anyMatch(String::isEmpty) || new HashSet<>(v).size() != v.size())
            throw new IllegalArgumentException("비어 있거나 중복된 설정 값: " + s);
        return v;
    }
    static List<Long> numbers(String s) { return values(s).stream().map(Long::parseLong).toList(); }
    static String header() {
        return "variant,scenario,seed,offered,ok,rejected,pending,goodput_virtual_rps,p99_virtual_ms,lost,stale_reads,false_dead,replication_model_bytes,detector_model_bytes,migration_model_bytes,freeze_virtual_ms,splits,planned_moves,completed_moves,recovery_virtual_ms,tape_hash,safe\n";
    }
    static String csv(Row row) {
        var m = row.m();
        return String.join(",", m.variant(), m.scenario(), "" + m.seed(), "" + m.offered(), "" + m.ok(), "" + m.rejected(),
                "" + m.pending(), fmt(m.goodput()), fmt(m.p99Ms()), "" + m.lost(), "" + m.staleReads(), "" + m.falseDead(),
                "" + m.replicationBytes(), "" + m.detectorBytes(), "" + m.migrationBytes(), "" + m.freezeMs(), "" + m.splits(),
                "" + m.plannedMoves(), "" + m.completedMoves(), m.recoveryMs() < 0 ? "" : "" + m.recoveryMs(), "" + m.tapeHash(), "" + m.safe()) + "\n";
    }
    static String raw(List<Row> rows) {
        String[] names = header().strip().split(",");
        return "[\n" + rows.stream().map(row -> {
            String[] fields = csv(row).strip().split(",", -1);
            StringBuilder json = new StringBuilder("{");
            for (int i = 0; i < names.length; i++) {
                if (i > 0) json.append(',');
                json.append(quote(names[i])).append(':').append(i < 2 ? quote(fields[i]) : fields[i].isEmpty() ? "null" : fields[i]);
            }
            json.append(",\"events\":[").append(row.m().events().stream().map(SystemIntegrationMain::quote).collect(Collectors.joining(","))).append("]}");
            return json.toString();
        }).collect(Collectors.joining(",\n")) + "\n]\n";
    }
    static String quote(String s) { return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r") + "\""; }
    static String fmt(double n) { return Double.isFinite(n) ? String.format(Locale.ROOT, "%.6f", n) : ""; }
    static List<Row> matching(List<Row> rows, SystemVariant v, String scenario) {
        return rows.stream().filter(r -> r.variant().equals(v) && r.m().scenario().equals(scenario)).toList();
    }
    static boolean eligible(List<Row> rows, SystemVariant v, String scenario, int n) {
        List<Row> found = matching(rows, v, scenario);
        return found.size() == n && found.stream().allMatch(r -> r.m().safe()
                && (!scenario.equals("crash-overlap") || r.m().recoveryMs() >= 0));
    }
    static double mean(List<Row> rows, SystemVariant v, String scenario) {
        return matching(rows, v, scenario).stream().mapToDouble(r -> r.m().goodput()).average().orElse(Double.NaN);
    }
    static void writeAggregate(Path path, List<Row> rows) throws IOException {
        Map<String, List<Row>> groups = new TreeMap<>();
        for (Row r : rows) groups.computeIfAbsent(r.m().scenario() + "," + r.variant().id(), ignored -> new ArrayList<>()).add(r);
        StringBuilder out = new StringBuilder("scenario,variant,n,safe_n,mean_goodput_virtual_rps,goodput_ci95_halfwidth,mean_run_p99_virtual_ms,mean_migration_model_bytes,mean_freeze_virtual_ms\n");
        groups.forEach((key, list) -> {
            double[] goodput = list.stream().mapToDouble(r -> r.m().goodput()).toArray();
            out.append(key).append(',').append(list.size()).append(',').append(list.stream().filter(r -> r.m().safe()).count())
                    .append(',').append(fmt(Arrays.stream(goodput).average().orElseThrow())).append(',').append(fmt(ci(goodput)))
                    .append(',').append(fmt(list.stream().mapToDouble(r -> r.m().p99Ms()).average().orElse(Double.NaN)))
                    .append(',').append(fmt(list.stream().mapToLong(r -> r.m().migrationBytes()).average().orElseThrow()))
                    .append(',').append(fmt(list.stream().mapToLong(r -> r.m().freezeMs()).average().orElseThrow())).append('\n');
        });
        Files.writeString(path, out);
    }
    static List<String> axes(SystemVariant v) { return List.of(v.replication(), v.detector(), v.distribution(), v.split(), v.placement(), v.copy()); }
    static int distance(SystemVariant a, SystemVariant b) {
        int d = 0; for (int i = 0; i < 6; i++) if (!axes(a).get(i).equals(axes(b).get(i))) d++; return d;
    }
    static void writePairs(Path path, List<Row> rows) throws IOException {
        Map<String, Row> index = new HashMap<>();
        rows.forEach(r -> index.put(r.variant().id() + "," + r.m().scenario() + "," + r.m().seed(), r));
        Set<SystemVariant> variants = new LinkedHashSet<>(); rows.forEach(r -> variants.add(r.variant()));
        String[] baseline = {"PB", "SWIM", "RANGE", "NONE", "GREEDY", "STOP"};
        String[] axisNames = {"replication", "detector", "distribution", "split", "placement", "copy"};
        StringBuilder out = new StringBuilder("scenario,axis,baseline,candidate,n,mean_delta_goodput,delta_ci95_halfwidth,mean_delta_p99_ms,mean_delta_migration_bytes,mean_delta_freeze_ms,baseline_safe_n,candidate_safe_n\n");
        for (SystemVariant v : variants) for (int axis = 0; axis < 6; axis++) {
            if (axes(v).get(axis).equals(baseline[axis])) continue;
            for (SystemVariant b : variants) {
                if (!axes(b).get(axis).equals(baseline[axis]) || distance(b, v) != 1) continue;
                for (String scenario : rows.stream().map(r -> r.m().scenario()).distinct().toList()) {
                    List<Row> candidates = matching(rows, v, scenario);
                    List<Double> delta = new ArrayList<>(); double dp99 = 0, dm = 0, df = 0; int bs = 0, vs = 0;
                    for (Row r : candidates) {
                        Row ref = index.get(b.id() + "," + scenario + "," + r.m().seed());
                        if (ref == null) continue;
                        if (ref.m().tapeHash() != r.m().tapeHash()) throw new IllegalStateException("paired workload differs");
                        delta.add(r.m().goodput() - ref.m().goodput()); dp99 += r.m().p99Ms() - ref.m().p99Ms();
                        dm += r.m().migrationBytes() - ref.m().migrationBytes(); df += r.m().freezeMs() - ref.m().freezeMs();
                        if (ref.m().safe()) bs++; if (r.m().safe()) vs++;
                    }
                    if (delta.isEmpty()) continue;
                    double[] ds = delta.stream().mapToDouble(Double::doubleValue).toArray(); int n = ds.length;
                    out.append(scenario).append(',').append(axisNames[axis]).append(',').append(b.id()).append(',').append(v.id())
                            .append(',').append(n).append(',').append(fmt(Arrays.stream(ds).average().orElseThrow())).append(',').append(fmt(ci(ds)))
                            .append(',').append(fmt(dp99 / n)).append(',').append(fmt(dm / n)).append(',').append(fmt(df / n))
                            .append(',').append(bs).append(',').append(vs).append('\n');
                }
            }
        }
        Files.writeString(path, out);
    }
    static double ci(double[] x) {
        if (x.length < 2) return Double.NaN;
        double mean = Arrays.stream(x).average().orElseThrow(), sum = 0;
        for (double value : x) sum += (value - mean) * (value - mean);
        double[] t = {0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262,
                2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093,
                2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042};
        double critical = x.length <= 31 ? t[x.length - 1] : x.length <= 61 ? 2.042 : 2.000;
        return critical * Math.sqrt(sum / (x.length - 1) / x.length);
    }
    static String report(List<Row> rows, List<Row> confirmation, List<String> scenarios,
                         Map<String, SystemVariant> selected, int seedCount, int confirmationCount, int serviceMicros) {
        StringBuilder out = new StringBuilder("# 통합 가상 클러스터 screening 결과\n\n");
        out.append("실제 알고리즘을 단일 thread·메모리 fixture에서 실행했다. 시간은 virtual, byte는 model-estimated다. 실서버 성능 순위가 아니다.\n\n")
                .append("screening ").append(rows.size()).append("회, 독립 seed 재검증 ").append(confirmation.size()).append("회.\n\n")
                .append("선정 기준은 모든 screening seed에서 유실·stale read·오판 DEAD가 0이고 계획한 이동이 완료된 조합 중 평균 goodput 최대다. crash는 쓰기 재개 관측도 필요하다. 동률은 ID 순서다. 지연·비용은 별도로 공개한다. 다중 비교 보정 없는 탐색이므로 전역 최적을 증명하지 않는다.\n\n")
                .append("| 시나리오 | screening 선택 | 독립 seed safe | goodput (virtual req/s) | 해석 |\n|---|---|---:|---:|---|\n");
        for (String scenario : scenarios) {
            SystemVariant v = selected.get(scenario);
            if (v == null) { out.append('|').append(scenario).append("|없음|—|—|안전성·완료 조건 통과 조합 없음|\n"); continue; }
            long safe = matching(confirmation, v, scenario).stream().filter(r -> r.m().safe()).count();
            double base = mean(confirmation, v, scenario);
            boolean betterNeighbor = confirmation.stream().map(Row::variant).distinct().filter(n -> !n.equals(v))
                    .anyMatch(n -> eligible(confirmation, n, scenario, confirmationCount) && mean(confirmation, n, scenario) > base);
            out.append('|').append(scenario).append('|').append(v.id()).append('|').append(safe).append('/').append(confirmationCount)
                    .append('|').append(fmt(base)).append('|').append(safe < confirmationCount ? "재검증 실패" : betterNeighbor ? "독립 seed에서 더 나은 이웃 있음: 선정 불안정" : "후보 유지; 원인·CI는 confirmation-effects.csv 확인").append("|\n");
        }
        out.append("\n전체 screening safe ").append(rows.stream().filter(r -> r.m().safe()).count()).append('/').append(rows.size())
                .append("회. seed별 원본 수치는 summary.csv, 반복 집계는 aggregate.csv, 다른 축을 고정한 효과는 conditional-effects.csv에 있다.\n\n")
                .append("A2 IONIA, VR/Chain/Rosé의 A3 어댑터, FD Lab의 다른 detector, post-copy, 증감 판단·병렬 scheduler는 이 실행 경로에 연결되지 않았다. A1/A2는 동일 복제 계층의 대안이며 동시에 활성화하지 않는다.\n\n")
                .append("2개 논리 shard 그룹 × 고정 replica 3개, GET/SET, 96 key, 240~256 byte value, 6초 측정, 3ms마다 요청, 1000ms에 한 번 관측·분할·배치, 이동은 직렬이다. 요청 충돌은 BUSY로 센다. physical node별 egress 1024 byte/ms를 복제·FD·이동이 공유한다. 목적지 staging은 복제 엔진으로 순차 SET 확정 후 게시하며 이 비용도 포함한다. source 이전 값은 실행 종료까지 보관한다. GC/reclaim, SSD, 실제 CPU·네트워크, 재시작·분단·멤버십 합의는 측정하지 않았다.\n\n")
                .append("노드의 로컬 KV 연산은 각각 ").append(serviceMicros).append("μs의 같은 직렬 처리 큐를 사용한다. 이 처리 시간은 실측 CPU 비용이 아닌 실험 가정이다. cutover 경계에서 수락된 source 요청을 배출한 뒤 final delta를 만든다.\n\n")
                .append("안전성 검사는 마지막 OK 값보다 오래된 값을 유실/stale로 센다. TIMEOUT 후 적용될 수 있는 더 최신 발행 값은 허용한다. 전 연산 이력의 선형화 가능성 증명은 아니다.\n\n")
                .append("추가 검증: 더 긴 부하·key/value/네트워크 민감도, 독립 seed 확대, SLO별 Pareto 분석, A2 연결, 실제 TCP/SSD 실험. 현재 결과로 여섯 모듈 전체의 최선 조합을 확정하지 않는다.\n");
        return out.toString();
    }
    static String manifest(String config, List<Long> seeds, List<Long> confirmation, int count, List<String> scenarios) throws Exception {
        String git = process("git", "rev-parse", "HEAD").trim();
        String status = process("git", "status", "--porcelain");
        String diff = process("git", "diff", "--binary");
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (var files = Files.walk(Path.of("cache-core/src"))) {
            for (Path path : files.filter(f -> f.toString().endsWith(".java")).sorted().toList()) {
                digest.update(path.toString().getBytes(StandardCharsets.UTF_8)); digest.update(Files.readAllBytes(path));
            }
        }
        return "{\n  \"schema\":1,\n  \"origin\":\"integrated-in-process-virtual\",\n  \"git\":\"" + git
                + "\",\n  \"dirty\":" + !status.isBlank() + ",\n  \"java\":\"" + System.getProperty("java.version")
                + "\",\n  \"coreSourceSha256\":\"" + HexFormat.of().formatHex(digest.digest())
                + "\",\n  \"gitDiffSha256\":\"" + hash(diff.getBytes(StandardCharsets.UTF_8))
                + "\",\n  \"configSha256\":\"" + hash(Files.readAllBytes(Path.of(config)))
                + "\",\n  \"screeningSeeds\":" + seeds + ",\n  \"confirmationSeeds\":" + confirmation
                + ",\n  \"variants\":" + count + ",\n  \"scenarios\":\"" + String.join(",", scenarios) + "\"\n}\n";
    }
    static String hash(byte[] bytes) throws NoSuchAlgorithmException { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
    static String process(String... command) throws Exception {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String text = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) throw new IOException("command failed " + Arrays.toString(command));
        return text;
    }
}
