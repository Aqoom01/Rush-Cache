package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.experiment.ExperimentResult;
import com.openkache.failuredetector.experiment.ExperimentRunner;
import com.openkache.failuredetector.experiment.DetectorProfile;
import com.openkache.failuredetector.experiment.ResultWriter;
import com.openkache.failuredetector.experiment.ScenarioConfig;
import com.openkache.failuredetector.experiment.ScenarioType;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Runs the reproducible comparison matrix used for detector selection.
 *
 * <p>Every algorithm receives the same scenario, cluster size and random seed.
 * Results are grouped by algorithm, scenario and node count so protocol costs
 * are not accidentally compared at different scales.</p>
 */
public final class DetailedExperimentMain {
    private DetailedExperimentMain() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        ExperimentRunner runner = new ExperimentRunner();
        List<ExperimentResult> results = new ArrayList<>();

        for (int nodeCount : options.nodeCounts) {
            for (ScenarioType type : options.scenarios) {
                ScenarioConfig scenario = withNodeCount(ScenarioConfig.defaults(type), nodeCount);
                for (long seed = 1; seed <= options.seeds; seed++) {
                    results.addAll(runner.runAllProfiles(scenario, seed));
                }
            }
        }

        new ResultWriter().write(options.output, results);
        System.out.printf(
                "Completed %,d detailed runs (%d scenarios x %d node sizes x %d seeds x %d profiles).%n",
                results.size(),
                options.scenarios.size(),
                options.nodeCounts.size(),
                options.seeds,
                DetectorProfile.values().length
        );
        System.out.println("Results: " + options.output.toAbsolutePath());
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
        private int seeds = 50;
        private List<Integer> nodeCounts = List.of(5, 25, 100);
        private Path output = Path.of("results-detailed");
        private Set<ScenarioType> scenarios = new LinkedHashSet<>(Arrays.asList(ScenarioType.values()));

        private static Options parse(String[] args) {
            Options result = new Options();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--seeds" -> result.seeds = positiveInt(value(args, ++i), "seeds");
                    case "--nodes" -> result.nodeCounts = nodeCounts(value(args, ++i));
                    case "--output" -> result.output = Path.of(value(args, ++i));
                    case "--scenarios" -> result.scenarios = scenarios(value(args, ++i));
                    case "--help", "-h" -> {
                        usage();
                        System.exit(0);
                    }
                    default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
                }
            }
            return result;
        }

        private static List<Integer> nodeCounts(String value) {
            List<Integer> result = new ArrayList<>();
            for (String item : value.split(",")) {
                int count = positiveInt(item.trim(), "nodes");
                if (count < 3) {
                    throw new IllegalArgumentException("nodes must be at least three");
                }
                result.add(count);
            }
            return List.copyOf(new LinkedHashSet<>(result));
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
            System.out.println("Detailed failure detector experiment");
            System.out.println("  --seeds N                 default: 50");
            System.out.println("  --nodes 5,25,100          comma-separated cluster sizes");
            System.out.println("  --output PATH             default: results-detailed");
            System.out.println("  --scenarios ID,ID         default: all scenarios");
        }
    }
}
