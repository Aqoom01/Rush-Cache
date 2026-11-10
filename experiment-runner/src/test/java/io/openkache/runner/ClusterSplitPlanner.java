package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.balance.split.HeatSplitPolicy;
import io.openkache.core.balance.split.MidpointSplitPolicy;
import io.openkache.core.balance.split.SplitDecision;
import io.openkache.core.balance.split.SplitPolicy;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.TabletRange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * B2 클러스터 분할 실험의 config 생성기. 공통 Runner 코드가 아니라 B2 실험용 도구다(test 소스에 둔다).
 *
 * <p>기준 config의 부하로 관찰 window 하나를 만들어 Tablet 하나(해시 공간 전체)의 {@link TabletLoad}를 구성하고,
 * {@code midpoint-split}·{@code split-for-heat}가 정한 분할 지점을 {@code routing.boundaries}로 넣은 config를 쓴다. 분할하지
 * 않는 기준선 {@code no-split}도 같이 쓴다. 관찰 window는 측정 요청열과 겹치지 않도록 같은 분포에서 다른 seed로 만든다
 * (Runner의 warm-up은 측정과 같은 요청열이므로 그것으로 결정하면 측정 데이터로 결정하는 셈이 된다).
 *
 * <p>사용법: {@code ClusterSplitPlanner <기준 config> <출력 디렉터리>}. 출력에 config 3개와 {@code split-plan.txt}를 쓴다.
 */
final class ClusterSplitPlanner {

    /** 관찰 window의 seed = 기준 seed + 이 값. */
    static final long OBSERVATION_SEED_OFFSET = 1_000_003;

    private ClusterSplitPlanner() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: ClusterSplitPlanner <base.properties> <output-dir>");
            System.exit(2);
        }
        Path base = Path.of(args[0]);
        Path outDir = Path.of(args[1]);
        RunnerConfig config = RunnerConfig.load(base);
        List<String> baseLines = Files.readAllLines(base, StandardCharsets.UTF_8);

        TabletLoad observed = observe(config, config.seed() + OBSERVATION_SEED_OFFSET);
        List<String> plan = new ArrayList<>();
        plan.add("base=" + base);
        plan.add("observation: seed=" + (config.seed() + OBSERVATION_SEED_OFFSET) + " requests=" + config.warmupRequests()
                + " keys=" + observed.keys().size());

        Files.createDirectories(outDir);
        write(outDir.resolve("no-split.properties"), baseLines, "no-split", null);
        plan.add("no-split: boundary=none");
        for (SplitPolicy policy : List.of(new MidpointSplitPolicy(), new HeatSplitPolicy())) {
            SplitDecision decision = policy.decide(observed);
            Long boundary = decision instanceof SplitDecision.Split split ? split.at() : null;
            write(outDir.resolve(policy.variant() + ".properties"), baseLines, policy.variant(), boundary);
            plan.add(String.format(
                    Locale.ROOT,
                    "%s: decision=%s boundary=%s observed_left_share=%.4f measured_left_share=%.4f",
                    policy.variant(),
                    decision,
                    boundary == null ? "none" : Long.toUnsignedString(boundary),
                    leftShare(observed, boundary),
                    boundary == null ? 1.0 : measuredLeftShare(config, boundary)));
        }
        Files.write(outDir.resolve("split-plan.txt"), plan, StandardCharsets.UTF_8);
        plan.forEach(System.out::println);
    }

    /** {@code seed}로 만든 요청 {@code warmupRequests}건을 key별로 센 Tablet 부하. key는 해시값 오름차순이다. */
    static TabletLoad observe(RunnerConfig config, long seed) {
        long[] reads = new long[config.keyCount()];
        long[] writes = new long[config.keyCount()];
        forEachRequest(config, config.warmupRequests(), seed, (index, command) -> {
            if (command instanceof Command.Get) {
                reads[index]++;
            } else {
                writes[index]++;
            }
        });
        List<KeyLoad> keys = new ArrayList<>(config.keyCount());
        for (int index : WorkloadGenerator.hashOrder(config)) {
            keys.add(new KeyLoad(KeyHash.of(WorkloadGenerator.key(index, config.keyLength())), reads[index], writes[index]));
        }
        return new TabletLoad(TabletRange.FULL, 0, keys);
    }

    /** 실제 측정 요청열(기준 seed)에서 해시값이 {@code boundary} 미만인 쪽으로 갈 요청 비율. */
    static double measuredLeftShare(RunnerConfig config, long boundary) {
        long[] left = new long[1];
        forEachRequest(config, config.measuredRequests(), config.seed(), (index, command) -> {
            if (Long.compareUnsigned(KeyHash.of(WorkloadGenerator.key(index, config.keyLength())), boundary) < 0) {
                left[0]++;
            }
        });
        return (double) left[0] / config.measuredRequests();
    }

    private static double leftShare(TabletLoad load, Long boundary) {
        if (boundary == null) {
            return 1.0;
        }
        long left = 0;
        for (KeyLoad key : load.keys()) {
            if (Long.compareUnsigned(key.keyHash(), boundary) < 0) {
                left += key.requests();
            }
        }
        return (double) left / load.totalRequests();
    }

    private interface RequestVisitor {
        void visit(int keyIndex, Command command);
    }

    /** {@link ExperimentRunner}와 같은 방식(thread i는 seed+i, 요청 수를 thread에 나눔)으로 요청열을 만든다. */
    private static void forEachRequest(RunnerConfig config, long total, long seed, RequestVisitor visitor) {
        Map<Bytes, Integer> index = new HashMap<>();
        for (int i = 0; i < config.keyCount(); i++) {
            index.put(WorkloadGenerator.key(i, config.keyLength()), i);
        }
        int threads = config.concurrency();
        for (int t = 0; t < threads; t++) {
            long count = total / threads + (t < total % threads ? 1 : 0);
            WorkloadGenerator generator = new WorkloadGenerator(config, new Random(seed + t));
            for (long i = 0; i < count; i++) {
                Command command = generator.next();
                Bytes key = switch (command) {
                    case Command.Get get -> get.key();
                    case Command.Set set -> set.key();
                    case Command.Delete delete -> delete.key();
                    default -> throw new IllegalStateException("unexpected command " + command);
                };
                visitor.visit(index.get(key), command);
            }
        }
    }

    /** 기준 config에서 variant와 routing.boundaries만 바꿔 쓴다. 로컬 실행용 target.hosts도 넣는다(CI는 노드 주소로 바꾼다). */
    private static void write(Path file, List<String> baseLines, String variant, Long boundary) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("# ClusterSplitPlanner가 만든 파일. 직접 고치지 말고 기준 config를 고친 뒤 다시 만든다.");
        for (String line : baseLines) {
            if (line.startsWith("variant=") || line.startsWith("routing.boundaries=") || line.startsWith("target.hosts=")) {
                continue;
            }
            lines.add(line);
        }
        lines.add("variant=" + variant);
        lines.add("target.hosts=127.0.0.1:7000,127.0.0.1:7010");
        if (boundary != null) {
            lines.add("routing.boundaries=" + Long.toUnsignedString(boundary));
        }
        Files.write(file, lines, StandardCharsets.UTF_8);
    }
}
