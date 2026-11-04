package com.openkache.failuredetector.cli;

import com.openkache.failuredetector.product.ProductAlgorithm;
import com.openkache.failuredetector.product.ProductBenchmarkRunner;
import com.openkache.failuredetector.product.ProductResultWriter;
import com.openkache.failuredetector.product.ProductRunResult;
import com.openkache.failuredetector.product.ProductScenario;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class FailureDetectorBenchmarkMain {
    private FailureDetectorBenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        if (options.containsKey("help")) {
            usage();
            return;
        }
        List<ProductAlgorithm> algorithms = algorithms(options.getOrDefault("algorithm", "all"));
        List<ProductScenario> scenarios = scenarios(options.getOrDefault("scenario", "all"));
        List<Long> seeds = seeds(options.getOrDefault("seeds", "1-30"));
        int nodes = Integer.parseInt(options.getOrDefault("nodes", "8"));
        String mode = options.getOrDefault("mode", "native");
        Path output = Path.of(options.getOrDefault("output", "results/failure-detector"));

        ProductBenchmarkRunner runner = new ProductBenchmarkRunner();
        List<ProductRunResult> results = new ArrayList<>();
        for (ProductAlgorithm algorithm : algorithms) {
            for (ProductScenario scenario : scenarios) {
                for (long seed : seeds) {
                    results.add(runner.run(algorithm, scenario, seed, nodes, mode));
                }
            }
        }
        new ProductResultWriter().write(output, results);
        long failed = results.stream().filter(result -> !result.executionSuccess()).count();
        System.out.println("wrote " + results.size() + " runs to " + output.toAbsolutePath());
        System.out.println("execution failures: " + failed);
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new TreeMap<>();
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if (arg.equals("--help")) {
                options.put("help", "true");
                continue;
            }
            if (!arg.startsWith("--") || index + 1 >= args.length) {
                throw new IllegalArgumentException("Expected --key value, got: " + arg);
            }
            options.put(arg.substring(2), args[++index]);
        }
        return options;
    }

    private static List<ProductAlgorithm> algorithms(String value) {
        if (value.equalsIgnoreCase("all")) {
            return List.of(ProductAlgorithm.values());
        }
        return Arrays.stream(value.split(",")).map(ProductAlgorithm::fromId).toList();
    }

    private static List<ProductScenario> scenarios(String value) {
        if (value.equalsIgnoreCase("all")) {
            return List.of(ProductScenario.values());
        }
        return Arrays.stream(value.split(",")).map(ProductScenario::fromId).toList();
    }

    private static List<Long> seeds(String value) {
        if (value.contains("-")) {
            String[] bounds = value.split("-", 2);
            long first = Long.parseLong(bounds[0]);
            long last = Long.parseLong(bounds[1]);
            if (first > last) {
                throw new IllegalArgumentException("invalid seed range: " + value);
            }
            List<Long> seeds = new ArrayList<>();
            for (long seed = first; seed <= last; seed++) {
                seeds.add(seed);
            }
            return List.copyOf(seeds);
        }
        return Arrays.stream(value.split(",")).map(String::trim)
                .map(Long::parseLong).toList();
    }

    private static void usage() {
        System.out.println("Usage: fdBenchmark [options]\n"
                + "  --algorithm all|swim-lifeguard,redis-cluster,phi-gossip,ignite-tcp-ring,aerospike-mesh\n"
                + "  --scenario all|normal,crash,loss15,loss45,delay-jitter,target-stall,observer-stall,consecutive-3,consecutive-4,partition\n"
                + "  --seeds 1-30|1,2,3 --nodes 8 --mode native --output PATH");
    }
}
