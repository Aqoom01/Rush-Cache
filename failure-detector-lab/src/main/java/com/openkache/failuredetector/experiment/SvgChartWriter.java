package com.openkache.failuredetector.experiment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;

final class SvgChartWriter {
    void write(Path outputDirectory, List<ExperimentResult> results) throws IOException {
        Files.createDirectories(outputDirectory);
        List<Integer> nodeCounts = results.stream()
                .map(ExperimentResult::nodeCount)
                .distinct()
                .sorted()
                .toList();
        for (int nodeCount : nodeCounts) {
            Path nodeDirectory = outputDirectory.resolve("nodes-" + nodeCount);
            Files.createDirectories(nodeDirectory);
            List<ExperimentResult> nodeResults = results.stream()
                    .filter(result -> result.nodeCount() == nodeCount)
                    .toList();
            for (String comparisonMode : nodeResults.stream()
                    .map(ExperimentResult::comparisonMode)
                    .distinct()
                    .sorted()
                    .toList()) {
                Path modeDirectory = nodeDirectory.resolve(comparisonMode);
                Files.createDirectories(modeDirectory);
                List<ExperimentResult> modeResults = nodeResults.stream()
                        .filter(result -> result.comparisonMode().equals(comparisonMode))
                        .toList();
                writeModeCharts(modeDirectory, nodeCount, comparisonMode, modeResults);
            }
        }
    }

    private void writeModeCharts(
            Path outputDirectory,
            int nodeCount,
            String comparisonMode,
            List<ExperimentResult> results
    ) throws IOException {
            writeGroupedBars(
                    outputDirectory.resolve("false-positive-events.svg"),
                    "False Positive Events (nodes=" + nodeCount + ", mode=" + comparisonMode + ")",
                    "events/run",
                    "simulation-observed",
                    results,
                    result -> result.metrics().falsePositiveEvents()
            );
            writeGroupedBars(
                    outputDirectory.resolve("false-suspect-duration.svg"),
                    "False Suspect Duration (nodes=" + nodeCount + ", mode=" + comparisonMode + ")",
                    "seconds/run",
                    "simulation-observed",
                    results,
                    result -> result.metrics().falseSuspectSeconds()
            );
            writeGroupedBars(
                    outputDirectory.resolve("false-dead-events.svg"),
                    "False DEAD Decisions (nodes=" + nodeCount + ", mode=" + comparisonMode + ")",
                    "events/run",
                    "simulation-observed",
                    results,
                    result -> result.metrics().falseDeadEvents()
            );
            writeGroupedBars(
                    outputDirectory.resolve("protocol-messages.svg"),
                    "Protocol Messages (nodes=" + nodeCount + ", mode=" + comparisonMode + ")",
                    "messages/run",
                    "model-estimated",
                    results,
                    result -> result.metrics().protocolMessages()
            );
            List<ExperimentResult> detected = results.stream()
                    .filter(result -> result.metrics().detectionLatencyMillis() != null)
                    .toList();
            if (!detected.isEmpty()) {
                writeGroupedBars(
                        outputDirectory.resolve("detection-latency.svg"),
                        "Crash Detection Latency (nodes=" + nodeCount + ", mode="
                                + comparisonMode + ")",
                        "milliseconds/run",
                        "simulation-observed",
                        detected,
                        result -> result.metrics().detectionLatencyMillis()
                );
            }
    }

    private void writeGroupedBars(
            Path path,
            String title,
            String unit,
            String source,
            List<ExperimentResult> results,
            ToDoubleFunction<ExperimentResult> value
    ) throws IOException {
        Map<Key, List<Double>> grouped = new HashMap<>();
        Set<ScenarioType> scenarios = new LinkedHashSet<>();
        Set<String> algorithms = new LinkedHashSet<>();
        for (ExperimentResult result : results) {
            scenarios.add(result.scenario());
            algorithms.add(result.algorithm());
            grouped.computeIfAbsent(
                    new Key(result.scenario(), result.algorithm()),
                    ignored -> new ArrayList<>()
            ).add(value.applyAsDouble(result));
        }
        List<ScenarioType> scenarioList = scenarios.stream()
                .sorted(java.util.Comparator.comparing(ScenarioType::id))
                .toList();
        List<String> algorithmList = algorithms.stream().sorted().toList();
        double max = grouped.values().stream()
                .flatMap(List::stream)
                .mapToDouble(Double::doubleValue)
                .max()
                .orElse(1.0);
        if (max == 0) {
            max = 1.0;
        }

        int width = 1_100;
        int height = 620;
        int left = 90;
        int top = 90;
        int chartWidth = width - left - 40;
        int chartHeight = height - top - 120;
        double groupWidth = scenarioList.isEmpty() ? chartWidth : (double) chartWidth / scenarioList.size();
        double barWidth = groupWidth / Math.max(algorithmList.size() + 1, 3);
        String[] colors = {"#2563eb", "#ea580c", "#16a34a", "#9333ea"};

        StringBuilder svg = new StringBuilder();
        svg.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"")
                .append(width).append("\" height=\"").append(height).append("\">");
        svg.append("<rect width=\"100%\" height=\"100%\" fill=\"white\"/>");
        svg.append(text(width / 2, 32, title, 22, "middle", "#111827"));
        svg.append(text(
                width / 2,
                57,
                "source=" + source + "; samples=" + results.size() + "; bars=mean",
                12,
                "middle",
                "#4b5563"
        ));
        svg.append(text(left, 76, unit, 13, "start", "#4b5563"));
        svg.append("<line x1=\"").append(left).append("\" y1=\"").append(top + chartHeight)
                .append("\" x2=\"").append(left + chartWidth).append("\" y2=\"")
                .append(top + chartHeight).append("\" stroke=\"#374151\"/>");
        svg.append("<line x1=\"").append(left).append("\" y1=\"").append(top)
                .append("\" x2=\"").append(left).append("\" y2=\"")
                .append(top + chartHeight).append("\" stroke=\"#374151\"/>");

        for (int tick = 0; tick <= 5; tick++) {
            double ratio = tick / 5.0;
            double y = top + chartHeight * (1 - ratio);
            svg.append("<line x1=\"").append(left).append("\" y1=\"").append(y)
                    .append("\" x2=\"").append(left + chartWidth).append("\" y2=\"")
                    .append(y).append("\" stroke=\"#e5e7eb\"/>");
            svg.append(text(left - 8, y + 4, format(max * ratio), 11, "end", "#6b7280"));
        }

        for (int scenarioIndex = 0; scenarioIndex < scenarioList.size(); scenarioIndex++) {
            ScenarioType scenario = scenarioList.get(scenarioIndex);
            double groupX = left + scenarioIndex * groupWidth;
            for (int algorithmIndex = 0; algorithmIndex < algorithmList.size(); algorithmIndex++) {
                String algorithm = algorithmList.get(algorithmIndex);
                List<Double> values = grouped.getOrDefault(new Key(scenario, algorithm), List.of());
                double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                double barHeight = chartHeight * mean / max;
                double x = groupX + barWidth * (algorithmIndex + 0.5);
                double y = top + chartHeight - barHeight;
                svg.append("<rect x=\"").append(x).append("\" y=\"").append(y)
                        .append("\" width=\"").append(barWidth * 0.85).append("\" height=\"")
                        .append(barHeight).append("\" fill=\"")
                        .append(colors[algorithmIndex % colors.length]).append("\"/>");
            }
            svg.append(text(
                    groupX + groupWidth / 2,
                    top + chartHeight + 24,
                    scenario.id(),
                    11,
                    "middle",
                    "#374151"
            ));
        }

        for (int index = 0; index < algorithmList.size(); index++) {
            int x = left + index * 230;
            int y = height - 35;
            svg.append("<rect x=\"").append(x).append("\" y=\"").append(y - 12)
                    .append("\" width=\"16\" height=\"16\" fill=\"")
                    .append(colors[index % colors.length]).append("\"/>");
            svg.append(text(x + 24, y, algorithmList.get(index), 13, "start", "#111827"));
        }
        svg.append("</svg>");
        Files.writeString(path, svg.toString(), StandardCharsets.UTF_8);
    }

    private static String text(
            double x,
            double y,
            String value,
            int size,
            String anchor,
            String color
    ) {
        return "<text x=\"" + x + "\" y=\"" + y + "\" font-family=\"sans-serif\" font-size=\""
                + size + "\" text-anchor=\"" + anchor + "\" fill=\"" + color + "\">"
                + escape(value) + "</text>";
    }

    private static String escape(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static String format(double value) {
        if (value >= 1_000) {
            return String.format(Locale.ROOT, "%.1fk", value / 1_000);
        }
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private record Key(ScenarioType scenario, String algorithm) {
    }
}
