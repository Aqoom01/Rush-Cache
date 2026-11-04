package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.experiment.DetectorProfile;
import com.openkache.failuredetector.experiment.ExperimentResult;
import com.openkache.failuredetector.experiment.ExperimentRunner;
import com.openkache.failuredetector.experiment.ResultWriter;
import com.openkache.failuredetector.experiment.ScenarioConfig;
import com.openkache.failuredetector.experiment.ScenarioType;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        if (options == null) {
            return;
        }
        long planned = (long) options.nodeCounts.size()
                * options.scenarios.size()
                * options.seeds
                * options.profiles.size()
                * options.comparisonModes.size();
        if (options.dryRun) {
            System.out.printf(
                    "Dry run: %,d runs; profiles=%s; scenarios=%s; nodes=%s; modes=%s; output=%s%n",
                    planned,
                    options.profiles,
                    options.scenarios,
                    options.nodeCounts,
                    options.comparisonModes,
                    options.output.toAbsolutePath()
            );
            return;
        }

        ExperimentRunner runner = new ExperimentRunner();
        List<ExperimentResult> results = new ArrayList<>();
        for (int nodeCount : options.nodeCounts) {
            for (ScenarioType type : options.scenarios) {
                ScenarioConfig scenario = withNodeCount(ScenarioConfig.defaults(type), nodeCount);
                for (long seed = 1; seed <= options.seeds; seed++) {
                    for (String comparisonMode : options.comparisonModes) {
                        results.addAll(runner.runProfiles(
                                scenario,
                                seed,
                                options.profiles,
                                comparisonMode
                        ));
                    }
                }
            }
        }

        new ResultWriter().write(options.output, results);
        System.out.printf("Completed %,d runs. Results: %s%n", results.size(), options.output.toAbsolutePath());
    }

    private static ScenarioConfig withNodeCount(ScenarioConfig source, int nodeCount) {
        return new ScenarioConfig(
                source.type(),
                nodeCount,
                source.duration(),
                source.tick(),
                source.faultStart(),
                source.faultEnd(),
                source.packetLoss(),
                source.fixedLatency(),
                source.jitter(),
                Math.min(source.gossipFanout(), nodeCount - 1)
        );
    }

    private static final class Options {
        private int seeds = 30;
        private List<Integer> nodeCounts = List.of(5);
        private Path output = Path.of("results");
        private List<DetectorProfile> profiles = List.of(DetectorProfile.values());
        private Set<ScenarioType> scenarios = new LinkedHashSet<>(Arrays.asList(ScenarioType.values()));
        private List<String> comparisonModes = List.of(ExperimentRunner.REALISTIC_DEFAULT);
        private boolean dryRun;

        private static Options parse(String[] args) throws IOException {
            Options result = new Options();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--seeds" -> result.seeds = positiveInt(value(args, ++i), "seeds");
                    case "--nodes" -> result.nodeCounts = nodeCounts(value(args, ++i));
                    case "--output", "--output-dir" -> result.output = Path.of(value(args, ++i));
                    case "--profiles", "--algorithms" -> result.profiles = profiles(value(args, ++i));
                    case "--scenario", "--scenarios" -> result.scenarios = scenarios(value(args, ++i));
                    case "--traffic-mode" -> result.comparisonModes = comparisonModes(value(args, ++i));
                    case "--config" -> result.applyProperties(Path.of(value(args, ++i)));
                    case "--dry-run" -> result.dryRun = true;
                    case "--help", "-h" -> {
                        usage();
                        return null;
                    }
                    default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
                }
            }
            return result;
        }

        private void applyProperties(Path path) throws IOException {
            Properties properties = new Properties();
            try (Reader reader = Files.newBufferedReader(path)) {
                properties.load(reader);
            }
            if (properties.containsKey("seeds")) {
                seeds = positiveInt(properties.getProperty("seeds"), "seeds");
            }
            if (properties.containsKey("nodes")) {
                nodeCounts = nodeCounts(properties.getProperty("nodes"));
            }
            if (properties.containsKey("profiles")) {
                profiles = profiles(properties.getProperty("profiles"));
            }
            if (properties.containsKey("scenarios")) {
                scenarios = scenarios(properties.getProperty("scenarios"));
            }
            if (properties.containsKey("trafficMode")) {
                comparisonModes = comparisonModes(properties.getProperty("trafficMode"));
            }
            if (properties.containsKey("outputDir")) {
                output = Path.of(properties.getProperty("outputDir"));
            }
        }

        private static List<Integer> nodeCounts(String value) {
            List<Integer> result = Arrays.stream(value.split(","))
                    .map(String::trim)
                    .map(item -> positiveInt(item, "nodes"))
                    .peek(item -> {
                        if (item < 3) {
                            throw new IllegalArgumentException("nodes must be at least three");
                        }
                    })
                    .distinct()
                    .toList();
            if (result.isEmpty()) {
                throw new IllegalArgumentException("at least one node count is required");
            }
            return result;
        }

        private static List<DetectorProfile> profiles(String value) {
            List<DetectorProfile> result = Arrays.stream(value.split(","))
                    .map(DetectorProfile::fromId)
                    .distinct()
                    .toList();
            if (result.isEmpty()) {
                throw new IllegalArgumentException("at least one profile is required");
            }
            return result;
        }

        private static Set<ScenarioType> scenarios(String value) {
            Set<ScenarioType> result = new LinkedHashSet<>();
            for (String item : value.split(",")) {
                result.add(ScenarioType.fromId(item));
            }
            if (result.isEmpty()) {
                throw new IllegalArgumentException("at least one scenario is required");
            }
            return result;
        }

        private static List<String> comparisonModes(String value) {
            if (value.equalsIgnoreCase("both")) {
                return List.of(ExperimentRunner.REALISTIC_DEFAULT, ExperimentRunner.MATCHED_ACTIVITY);
            }
            if (value.equalsIgnoreCase("default")
                    || value.equalsIgnoreCase(ExperimentRunner.REALISTIC_DEFAULT)) {
                return List.of(ExperimentRunner.REALISTIC_DEFAULT);
            }
            if (value.equalsIgnoreCase("matched")
                    || value.equalsIgnoreCase(ExperimentRunner.MATCHED_ACTIVITY)) {
                return List.of(ExperimentRunner.MATCHED_ACTIVITY);
            }
            throw new IllegalArgumentException("traffic-mode must be default, matched, or both");
        }

        private static String value(String[] args, int index) {
            if (index >= args.length) {
                throw new IllegalArgumentException("missing option value");
            }
            return args[index];
        }

        private static int positiveInt(String value, String name) {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        }

        private static void usage() {
            System.out.println("Failure detector comparison experiment");
            System.out.println("  --seeds N");
            System.out.println("  --nodes 5,25,100");
            System.out.println("  --profiles F0,F1,F2,F3,F4,F5,F6,F7a,F7b,F7c");
            System.out.println("             or fixed-deadline,phi-gossip,swim-base,swim-lifeguard,");
            System.out.println("             adaptive-deadline,fixed-deadline-ring,");
            System.out.println("             fixed-deadline-ring-verify,swim-lha-probe,");
            System.out.println("             swim-lha-suspicion,swim-buddy");
            System.out.println("  --scenario ID,ID");
            System.out.println("  --traffic-mode default|matched|both");
            System.out.println("  --output-dir PATH");
            System.out.println("  --config PATH.properties");
            System.out.println("  --dry-run");
        }
    }
}
