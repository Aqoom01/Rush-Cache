package io.openkache.core.migration.experiment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** 같은 실행 스냅샷을 CSV와 사람이 읽는 Markdown 표에 사용한다. */
final class BaselineResults {
    static final String BLOCKED = "쓰기 차단 지속 시간(tick)";
    static final String TOTAL = "전체 이동 시간(tick)";
    static final String SENT = "보낸 데이터 크기(바이트)";
    static final String REJECTED = "거절한 쓰기 요청 수";
    static final String OTHER_REJECTED = "다른 Tablet에서 거절한 쓰기 요청 수";
    static final String VALIDATION = "전송 검증 실패 수";
    static final String DUPLICATES = "중복 전달 수";
    static final String VIOLATIONS = "정합성 규칙 위반 수";
    /** CSV 지표 순서. 실행별 long CSV와 조건별 요약 CSV가 같은 순서를 쓴다. */
    static final List<String> METRICS = List.of("이동 대상 항목 수", "이동 대상 데이터 크기(바이트)", SENT,
            BLOCKED, TOTAL, "전송량 비율", REJECTED, OTHER_REJECTED, VALIDATION, DUPLICATES,
            "이전 노드 데이터 일치 여부", "새 노드 데이터 일치 여부", "잘못된 소유권 전환 수", VIOLATIONS);

    record Condition(String scenario, int keys, long budget, double rho, int deletePercent,
            MigrationFixture.Fault fault, String stop, int otherWrites) { }

    record Run(Condition condition, long seed, String outcome, Map<String, Number> metrics, long startedAtEpochMillis) {
        Run { metrics = Map.copyOf(metrics); }
        Run(Condition condition, long seed, String outcome, Map<String, Number> metrics) { this(condition, seed, outcome, metrics, 0); }
    }

    private final List<Run> runs = new ArrayList<>();

    void add(Run run) { runs.add(run); }

    List<Run> snapshots() { return List.copyOf(runs); }

    static boolean include(Condition condition, String metric) {
        return switch (metric) {
            case REJECTED -> List.of("SC2", "SC3", "SC6").contains(condition.scenario());
            case OTHER_REJECTED -> condition.scenario().equals("SC6");
            case VALIDATION -> condition.scenario().equals("SC5");
            case DUPLICATES -> condition.scenario().equals("SC5") && condition.fault() == MigrationFixture.Fault.DUPLICATE;
            case "잘못된 소유권 전환 수" -> List.of("SC4", "SC5").contains(condition.scenario());
            // 다른 시나리오라도 예상하지 않은 오류는 숨기지 않는다(호출부에서 nonzero 보존).
            default -> true;
        };
    }

    /** 저장소 루트 기준 실험 안내 문서. 보고서 위치(runs/&lt;id&gt;/, build/)와 관계없이 이 문서로 연결한다. */
    static final Path GUIDE = Path.of("experiments", "b3-live-migration", "migration-comparison", "README.md");

    void write(Path path, String csvName) throws IOException {
        Files.writeString(path, markdown(csvName, guideLink(path)), StandardCharsets.UTF_8);
    }

    /** 보고서 폴더에서 실험 안내 문서까지의 상대 링크. 다른 드라이브 등 상대화할 수 없으면 절대 경로를 쓴다. */
    static String guideLink(Path report) {
        Path guide = GUIDE.toAbsolutePath().normalize();
        Path from = report.toAbsolutePath().normalize().getParent();
        try {
            return from.relativize(guide).toString().replace('\\', '/');
        } catch (IllegalArgumentException differentRoot) {
            return guide.toString().replace('\\', '/');
        }
    }

    String markdown(String csvName) { return markdown(csvName, "README.md"); }

    String markdown(String csvName, String guideLink) {
        Map<Condition, Group> grouped = new LinkedHashMap<>();
        for (Run run : runs) grouped.computeIfAbsent(run.condition(), Group::new).runs.add(run);
        List<Group> groups = new ArrayList<>(grouped.values());
        boolean same = groups.stream().allMatch(Group::identical);
        long violations = runs.stream().mapToLong(run -> run.metrics().get(VIOLATIONS).longValue()).sum();
        long matches = runs.stream().mapToLong(run -> run.metrics().get("이전 노드 데이터 일치 여부").longValue()
                + run.metrics().get("새 노드 데이터 일치 여부").longValue()).sum();
        String seeds = runs.stream().map(Run::seed).distinct().sorted().map(String::valueOf).collect(Collectors.joining(", "));
        StringBuilder out = new StringBuilder("# stop-and-copy Baseline 결과\n\n");
        out.append("[재현 방법과 지표 정의](").append(guideLink).append(") · [분석용 원본](").append(csvName).append(")\n\n")
                .append("`StopAndCopyBaseline` 실행 값으로 자동 생성한 문서다. SC1~SC6는 B3 연구 안의 시나리오 ID다.\n\n")
                .append("## 핵심 결과\n\n");
        appendInterpretations(out, groups);
        out.append("- ").append(groups.size()).append("개 조건 × seed ").append(seeds).append(": 총 ")
                .append(runs.size()).append("회 실행, 정합성 규칙 위반 ").append(violations).append("건. source/destination 전체 데이터 비교 ")
                .append(matches).append("/").append(runs.size() * 2L).append("건 일치. 결과: ")
                .append(outcomeCounts()).append(". `취소 거부`도 이동은 완료됐다.\n");
        List<Group> faults = groups.stream().filter(g -> g.c.scenario().equals("SC5")).toList();
        if (!faults.isEmpty()) {
            Group baseline = groups.stream().filter(g -> g.c.scenario().equals("SC1")
                    && g.c.keys() == faults.getFirst().c.keys() && g.c.budget() == faults.getFirst().c.budget()).findFirst().orElse(null);
            Group timeout = faults.stream().filter(g -> g.runs.stream().allMatch(r -> r.outcome().equals("취소(timeout)"))).findFirst().orElse(null);
            if (baseline != null && timeout != null) {
                out.append("- SC5의 timeout 시 쓰기 차단은 ").append(timeout.value(BLOCKED)).append(" tick, 같은 크기 정상 이동은 ")
                        .append(baseline.value(BLOCKED)).append(" tick이다. 실패 후 freeze 해제까지의 가용성 비용도 포함한다.\n");
            }
        }
        out.append("\n전송량·전송 예산에 따른 virtual tick 모델의 결과다. 실제 서버의 초 단위 downtime·latency 또는 네트워크 성능으로 환산하지 않는다.\n\n");
        if (same) out.append("모든 조건에서 seed별 지표와 결과가 같다. 표는 조건당 한 행으로 모았다. 고정 record 크기·전송 예산·이상 주입 시점과 credit 기반 요청 수, freeze 중 쓰기 거절 때문에 seed는 key 선택과 내용을 바꾸지만 집계값은 바꾸지 않는다. 반복 실행은 입력별 정합성 확인이며 성능 분산·신뢰구간 추정용 표본이 아니다.\n\n");
        else out.append("seed별 값이 다른 조건은 표에 최솟값~최댓값과 관측된 결과들을 표시한다. CSV에는 seed별 원본을 모두 보존한다.\n\n");

        section(out, "SC1 · 데이터 크기와 전송 예산", "초기 항목 수 | 예산 (KiB/tick) | 결과 | 쓰기 차단 (tick) | 전체 시간 (tick) | 보낸 바이트 | 전송량 비율", 0, 1, 3, 4, 5, 6);
        for (Group g : select(groups, c -> c.scenario().equals("SC1") || c.scenario().equals("SC1-rate"))) {
            row(out, integer(g.c.keys()), integer(g.c.budget() / 1024), g.outcome(), g.value(BLOCKED), g.value(TOTAL), g.value(SENT), g.value("전송량 비율"));
        }
        out.append("\n쓰기 차단은 전송 완료와 commit까지, 전체 시간은 cleanup까지 포함한다. 정상 경로에서는 `ceil(보낸 바이트 / 예산) + 1` tick에 쓰기를 재개한다.\n\n");

        section(out, "SC2 · 이동 중 쓰기", "쓰기 부하 비율 (rho) | 결과 | 쓰기 차단 (tick) | 거절한 쓰기 요청", 0, 2, 3);
        for (Group g : select(groups, c -> c.scenario().equals("SC2"))) row(out, decimal(g.c.rho(), 2), g.outcome(), g.value(BLOCKED), g.value(REJECTED));
        out.append("\nfreeze 중 이동 Tablet의 모든 SET·DELETE는 BUSY다. 부하 증가는 이 모델에서 거절 수를 늘리며 전송 예산을 소비하지 않는다.\n\n");

        section(out, "SC3 · freeze 전 삭제", "삭제 비율 (%) | 남은 항목 수 | 결과 | 쓰기 차단 (tick) | 보낸 바이트 | 거절한 쓰기 요청", 0, 1, 3, 4, 5);
        for (Group g : select(groups, c -> c.scenario().equals("SC3"))) row(out, integer(g.c.deletePercent()), g.value("이동 대상 항목 수"), g.outcome(), g.value(BLOCKED), g.value(SENT), g.value(REJECTED));
        out.append("\n삭제 0%는 SC2의 rho=0.10과 같은 조건인 대조군이다. 삭제한 key의 부활 여부는 destination 전체 데이터 비교로 검증한다.\n\n");

        section(out, "SC4 · 취소 시점과 cleanup", "취소 요청 시점 | 결과 | 쓰기 차단 (tick) | 전체 시간 (tick)", 2, 3);
        for (Group g : select(groups, c -> c.scenario().equals("SC4"))) row(out, StopAndCopyBaseline.stopLabel(g.c.stop()), g.outcome(), g.value(BLOCKED), g.value(TOTAL));
        out.append("\ncommit 전 취소는 source 원본과 빈 destination으로 원복한다. commit 뒤에는 소유권을 되돌리지 않는다. cleanup 조건은 부분 정리 실패 2회와 재시도를 포함한다.\n\n");

        section(out, "SC5 · 전송 이상", "주입한 이상 | 결과 | 쓰기 차단 (tick) | 보낸 바이트 | 검출 | 중복 전달", 2, 3, 4, 5);
        if (!faults.isEmpty()) {
            for (Group g : select(groups, c -> c.scenario().equals("SC1") && c.keys() == faults.getFirst().c.keys() && c.budget() == faults.getFirst().c.budget())) {
                row(out, "없음 (SC1 기준)", g.outcome(), g.value(BLOCKED), g.value(SENT), "–", "–");
            }
        }
        for (Group g : faults) row(out, StopAndCopyBaseline.faultLabel(g.c.fault()), g.outcome(), g.value(BLOCKED), g.value(SENT), g.value(VALIDATION), g.c.fault() == MigrationFixture.Fault.DUPLICATE ? g.value(DUPLICATES) : "–");
        out.append("\n손상·잘못된 ACK는 검출 후 commit 없이 timeout으로 취소한다. 누락의 검출 0은 무결성 검출 이벤트가 없다는 뜻이며 timeout은 실제 TickRunner 경로에서 판정한다. 마지막 ACK는 3 tick 지연한다. 중복은 수신 adapter의 추가 전달 이벤트이며 source 전송량에는 중복 네트워크 비용을 더하지 않는다.\n\n");

        section(out, "SC6 · Tablet 격리", "다른 Tablet 쓰기 (회/tick) | 결과 | 이동 Tablet 쓰기 차단 (tick) | 다른 Tablet 쓰기 거절", 0, 2, 3);
        for (Group g : select(groups, c -> c.scenario().equals("SC6"))) row(out, integer(g.c.otherWrites()), g.outcome(), g.value(BLOCKED), g.value(OTHER_REJECTED));
        out.append("\n다른 Tablet의 쓰기는 이동 중에도 처리한다. N은 이동 Tablet에 채운 항목 수이며 별도 Tablet의 항목은 포함하지 않는다.\n");
        return out.toString();
    }

    /**
     * 조건당 한 행의 요약 CSV. seed별 값이 모두 같으면 그 값을, 다르면 {@code 최솟값~최댓값}을 쓴다.
     * 해당 조건의 표시 대상이 아니고 모든 seed에서 0인 지표는 {@code –}로 둔다. seed별 원본은 JSON과 실행별 CSV에 있다.
     */
    void writeConditionSummary(java.io.PrintWriter writer) {
        Map<Condition, Group> grouped = new LinkedHashMap<>();
        for (Run run : runs) grouped.computeIfAbsent(run.condition(), Group::new).runs.add(run);
        writer.print('﻿');
        writer.print("복사 방식,실험 구분,초기 항목 수,전송 예산(바이트/tick),쓰기 부하 비율,이동 전 삭제 비율(%),주입한 전송 이상,취소 시점,"
                + "다른 Tablet 쓰기 수(회/tick),seed 수,seed별 값 동일,시간 기준,결과," + String.join(",", METRICS) + "\n");
        for (Group g : grouped.values()) {
            List<String> cells = new ArrayList<>(List.of("stop-and-copy", StopAndCopyBaseline.scenarioLabel(g.c.scenario()),
                    Integer.toString(g.c.keys()), Long.toString(g.c.budget()), String.format(Locale.ROOT, "%.2f", g.c.rho()),
                    Integer.toString(g.c.deletePercent()), StopAndCopyBaseline.faultLabel(g.c.fault()), StopAndCopyBaseline.stopLabel(g.c.stop()),
                    Integer.toString(g.c.otherWrites()), Integer.toString(g.runs.size()), g.identical() ? "1" : "0", "tick", g.outcome()));
            for (String metric : METRICS) {
                if (!include(g.c, metric) && g.min(metric) == 0 && g.max(metric) == 0) cells.add("–");
                else cells.add(g.csvValue(metric));
            }
            writer.print(String.join(",", cells) + "\n");
        }
    }

    private String outcomeCounts() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Run run : runs) counts.merge(run.outcome(), 1L, Long::sum);
        return counts.entrySet().stream().map(e -> e.getKey() + " " + e.getValue() + "회").collect(Collectors.joining(", "));
    }

    private static void appendInterpretations(StringBuilder out, List<Group> groups) {
        List<Group> sizes = select(groups, c -> c.scenario().equals("SC1")).stream()
                .sorted(Comparator.comparingInt(g -> g.c.keys())).toList();
        if (sizes.size() > 1) {
            out.append("- SC1에서 N=").append(sizes.stream().map(g -> integer(g.c.keys())).collect(Collectors.joining("/")))
                    .append("의 쓰기 차단은 ").append(values(sizes, BLOCKED)).append(" tick이다.");
            boolean increasing = true;
            for (int i = 1; i < sizes.size(); i++) {
                if (sizes.get(i - 1).max(BLOCKED) >= sizes.get(i).min(BLOCKED)) increasing = false;
            }
            if (increasing) out.append(" 고정 전송 예산에서 데이터 크기에 따라 쓰기 차단이 증가했다.");
            out.append('\n');
        }
        List<Group> extraBudgets = select(groups, c -> c.scenario().equals("SC1-rate"));
        if (!extraBudgets.isEmpty()) {
            int keys = extraBudgets.getFirst().c.keys();
            List<Group> budgets = select(groups, c -> (c.scenario().equals("SC1") || c.scenario().equals("SC1-rate"))
                    && c.keys() == keys).stream().sorted(Comparator.comparingLong(g -> g.c.budget())).toList();
            List<Run> samples = budgets.stream().flatMap(g -> g.runs.stream()).toList();
            long matches = samples.stream().filter(r -> {
                long sent = r.metrics().get(SENT).longValue();
                long budget = r.condition().budget();
                return (sent + budget - 1) / budget + 1 == r.metrics().get(BLOCKED).longValue();
            }).count();
            out.append("- SC1의 N=").append(integer(keys)).append("에서 예산 ")
                    .append(budgets.stream().map(g -> integer(g.c.budget() / 1024)).collect(Collectors.joining("/")))
                    .append(" KiB/tick의 쓰기 차단은 ").append(values(budgets, BLOCKED)).append(" tick이다. ")
                    .append(matches).append('/').append(samples.size())
                    .append("회가 예측식 `ceil(보낸 바이트 / 예산) + 1`과 일치했다.\n");
        }
        List<Group> loads = select(groups, c -> c.scenario().equals("SC2")).stream()
                .sorted(Comparator.comparingDouble(g -> g.c.rho())).toList();
        if (loads.size() > 1) {
            boolean sameBlocked = loads.stream().allMatch(g -> g.min(BLOCKED) == loads.getFirst().min(BLOCKED)
                    && g.max(BLOCKED) == loads.getFirst().min(BLOCKED));
            out.append("- SC2의 rho=").append(loads.stream().map(g -> decimal(g.c.rho(), 2)).collect(Collectors.joining("/")))
                    .append("에서 쓰기 차단은 ").append(sameBlocked ? "모두 " + loads.getFirst().value(BLOCKED) : values(loads, BLOCKED))
                    .append(" tick, 거절 수는 ").append(values(loads, REJECTED)).append("건이다.");
            if (sameBlocked) out.append(" 이 모델에서는 rho가 거절 수를 바꾸고 쓰기 차단 시간은 바꾸지 않았다.");
            out.append('\n');
        }
    }

    private static String values(List<Group> groups, String metric) {
        return groups.stream().map(g -> g.value(metric)).collect(Collectors.joining("/"));
    }

    private static List<Group> select(List<Group> groups, Predicate<Condition> test) {
        return groups.stream().filter(g -> test.test(g.c)).toList();
    }

    private static void section(StringBuilder out, String title, String columns, int... numericColumns) {
        out.append("## ").append(title).append("\n\n| ").append(columns).append(" |\n|");
        for (int column = 0; column < columns.split("\\|").length; column++) {
            boolean numeric = false;
            for (int numericColumn : numericColumns) if (column == numericColumn) numeric = true;
            out.append(numeric ? "---:|" : "---|");
        }
        out.append('\n');
    }

    private static void row(StringBuilder out, String... cells) { out.append("| ").append(String.join(" | ", cells)).append(" |\n"); }
    private static String integer(long number) { return String.format(Locale.ROOT, "%,d", number); }
    private static String decimal(double number, int places) { return String.format(Locale.ROOT, "% ." + places + "f", number).strip(); }

    private static final class Group {
        private final Condition c;
        private final List<Run> runs = new ArrayList<>();
        private Group(Condition condition) { c = condition; }
        private boolean identical() {
            Run first = runs.getFirst();
            return runs.stream().allMatch(r -> r.outcome().equals(first.outcome()) && r.metrics().equals(first.metrics()));
        }
        private String outcome() {
            return runs.stream().map(Run::outcome).collect(Collectors.toCollection(LinkedHashSet::new)).stream().collect(Collectors.joining(" / "));
        }
        private double min(String name) {
            return runs.stream().mapToDouble(r -> r.metrics().get(name).doubleValue()).min().orElseThrow();
        }
        private double max(String name) {
            return runs.stream().mapToDouble(r -> r.metrics().get(name).doubleValue()).max().orElseThrow();
        }
        private String value(String name) {
            double min = min(name);
            double max = max(name);
            boolean ratio = name.equals("전송량 비율");
            String low = ratio ? decimal(min, 6) : integer((long) min);
            String high = ratio ? decimal(max, 6) : integer((long) max);
            return min == max ? low : low + "~" + high;
        }
        /** CSV용 값. 천 단위 구분 기호 없이 정수, 비율은 소수 6자리로 쓴다. */
        private String csvValue(String name) {
            boolean ratio = name.equals("전송량 비율");
            double min = min(name);
            double max = max(name);
            String low = ratio ? String.format(Locale.ROOT, "%.6f", min) : Long.toString((long) min);
            String high = ratio ? String.format(Locale.ROOT, "%.6f", max) : Long.toString((long) max);
            return min == max ? low : low + "~" + high;
        }
    }
}
