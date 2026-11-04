package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.experiment.ExperimentResult;
import com.openkache.failuredetector.experiment.ExperimentRunner;
import com.openkache.failuredetector.experiment.ResultWriter;
import com.openkache.failuredetector.experiment.ScenarioConfig;
import com.openkache.failuredetector.experiment.ScenarioType;
import com.openkache.failuredetector.phi.PhiAccrualConfig;
import com.openkache.failuredetector.swim.SwimLifeguardConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Runs multiple operating points so algorithms can be compared at similar error budgets. */
public final class ParameterSweepMain {
    private ParameterSweepMain() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        ExperimentRunner runner = new ExperimentRunner();
        List<ExperimentResult> results = new ArrayList<>();

        for (int nodeCount : options.nodeCounts) {
            for (ScenarioType type : ScenarioType.values()) {
                ScenarioConfig scenario = withNodeCount(ScenarioConfig.defaults(type), nodeCount);
                for (long seed = 1; seed <= options.seeds; seed++) {
                    for (SwimProfile profile : swimProfiles()) {
                        results.add(runner.runSwimLifeguard(
                                scenario,
                                seed,
                                profile.config(),
                                profile.label()
                        ));
                    }
                    for (PhiProfile profile : phiProfiles()) {
                        results.add(runner.runPhiGossip(
                                scenario,
                                seed,
                                profile.config(),
                                profile.label()
                        ));
                    }
                }
            }
        }

        new ResultWriter().write(options.output, results);
        System.out.printf(
                "Completed %,d parameter-sweep runs (%d SWIM profiles, %d Phi profiles).%n",
                results.size(),
                swimProfiles().size(),
                phiProfiles().size()
        );
        System.out.println("Results: " + options.output.toAbsolutePath());
    }

    private static List<SwimProfile> swimProfiles() {
        return List.of(
                new SwimProfile("swim-lifeguard-fast", new SwimLifeguardConfig(
                        Duration.ofSeconds(1), Duration.ofMillis(500), 3,
                        2.0, 3.0, 3, 8
                )),
                new SwimProfile("swim-lifeguard-balanced", SwimLifeguardConfig.defaults()),
                new SwimProfile("swim-lifeguard-conservative", new SwimLifeguardConfig(
                        Duration.ofSeconds(1), Duration.ofMillis(500), 3,
                        6.0, 8.0, 3, 8
                ))
        );
    }

    private static List<PhiProfile> phiProfiles() {
        return List.of(4.0, 6.0, 8.0, 10.0).stream()
                .map(threshold -> new PhiProfile(
                        "phi-gossip-t" + threshold.intValue(),
                        new PhiAccrualConfig(
                                Duration.ofSeconds(1),
                                threshold,
                                1_000,
                                Duration.ofSeconds(2),
                                Duration.ofSeconds(2),
                                Duration.ofSeconds(5)
                        )
                ))
                .toList();
    }

    private static ScenarioConfig withNodeCount(ScenarioConfig source, int nodeCount) {
        return new ScenarioConfig(
                source.type(), nodeCount, source.duration(), source.tick(),
                source.faultStart(), source.faultEnd(), source.packetLoss(),
                source.fixedLatency(), source.jitter(),
                Math.min(source.gossipFanout(), nodeCount - 1)
        );
    }

    private record SwimProfile(String label, SwimLifeguardConfig config) {
    }

    private record PhiProfile(String label, PhiAccrualConfig config) {
    }

    private static final class Options {
        private int seeds = 30;
        private List<Integer> nodeCounts = List.of(5, 25, 100);
        private Path output = Path.of("results-sweep");

        private static Options parse(String[] args) {
            Options result = new Options();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--seeds" -> result.seeds = positiveInt(value(args, ++i), "seeds");
                    case "--nodes" -> result.nodeCounts = Arrays.stream(value(args, ++i).split(","))
                            .map(String::trim)
                            .map(value -> positiveInt(value, "nodes"))
                            .peek(value -> {
                                if (value < 3) {
                                    throw new IllegalArgumentException("nodes must be at least three");
                                }
                            })
                            .distinct()
                            .toList();
                    case "--output" -> result.output = Path.of(value(args, ++i));
                    default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
                }
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
    }
}
