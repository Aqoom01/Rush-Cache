package io.openkache.core.migration.experiment;

import io.openkache.core.api.Command;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.migration.JobState;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import io.openkache.core.tablet.Tablet;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * 설계 §6.1 SC1~SC6를 실행하는 별도 main. JUnit에서는 실행하지 않는다.
 * 모든 시간은 virtual tick이며 실제 성능을 측정하지 않는다.
 * CSV와 같은 실행 값으로 사람이 읽는 Markdown 결과도 생성한다.
 * 인수: summary.csv [--quick]. quick은 key 수만 줄이고 조건·seed는 그대로 유지한다.
 */
public final class StopAndCopyBaseline {
    private static final long RECORD_BYTES = 8 + 16 + 128;
    private static final long BUDGET = 64 * 1024;
    private final PrintWriter writer;
    private final BaselineResults results = new BaselineResults();
    private final java.util.function.Consumer<BaselineResults.Run> observer;
    private long cases;
    private long violations;

    StopAndCopyBaseline(PrintWriter writer) { this(writer, ignored -> { }); }

    StopAndCopyBaseline(PrintWriter writer, java.util.function.Consumer<BaselineResults.Run> observer) {
        this.writer = writer;
        this.observer = observer;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 2 || (args.length == 2 && !args[1].equals("--quick"))) {
            throw new IllegalArgumentException("사용법: StopAndCopyBaseline <결과.csv> [--quick]");
        }
        boolean quick = args.length == 2;
        Path output = Path.of(args[0]).toAbsolutePath();
        Files.createDirectories(output.getParent());
        long errors;
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(output, StandardCharsets.UTF_8))) {
            writeHeader(writer);
            StopAndCopyBaseline runner = new StopAndCopyBaseline(writer);
            runner.execute(quick);
            writer.flush();
            if (writer.checkError()) throw new IOException("결과 CSV 저장 실패");
            errors = runner.violations;
            runner.writeReport(reportPath(output), output.getFileName().toString());
            System.out.println("실험 횟수=" + runner.cases + ", 정합성 규칙 위반=" + errors + ", 결과 파일=" + output);
        }
        if (errors != 0) throw new IllegalStateException("기준 구현의 정합성 규칙 위반 수: " + errors);
    }

    void execute(boolean quick) {
        int[] sizes = quick ? new int[] {100, 1_000} : new int[] {10_000, 100_000, 1_000_000};
        int normal = quick ? 1_000 : 100_000;
        int faultSize = quick ? 100 : 10_000;
        for (int seed = 1; seed <= 5; seed++) {
            for (int size : sizes) run("SC1", size, BUDGET, 0, 0, MigrationFixture.Fault.NONE, "none", 0, seed);
            for (long budget : new long[] {16 * 1024, 256 * 1024}) {
                run("SC1-rate", normal, budget, 0, 0, MigrationFixture.Fault.NONE, "none", 0, seed);
            }
            for (double rho : new double[] {0, 0.1, 0.5}) {
                run("SC2", normal, BUDGET, rho, 0, MigrationFixture.Fault.NONE, "none", 0, seed);
            }
            for (int delete : new int[] {0, 30}) {
                run("SC3", normal, BUDGET, 0.1, delete, MigrationFixture.Fault.NONE, "none", 0, seed);
            }
            for (String stop : new String[] {"copying", "before-final-ack", "switching", "after-commit", "cleanup"}) {
                MigrationFixture.Fault fault = stop.equals("before-final-ack") ? MigrationFixture.Fault.ACK_DELAY : MigrationFixture.Fault.NONE;
                run("SC4", faultSize, BUDGET, 0, 0, fault, stop, 0, seed);
            }
            for (MigrationFixture.Fault fault : MigrationFixture.Fault.values()) {
                if (fault != MigrationFixture.Fault.NONE) run("SC5", faultSize, BUDGET, 0, 0, fault, "none", 0, seed);
            }
            for (int otherWrites : new int[] {0, 1, 16}) {
                run("SC6", normal, BUDGET, 0.1, 0, MigrationFixture.Fault.NONE, "none", otherWrites, seed);
            }
            System.err.println("난수 초깃값 " + seed + " 실험 완료");
        }
    }

    long violations() { return violations; }

    static void writeHeader(PrintWriter writer) {
        // Windows의 Excel에서도 한국어를 바로 인식하도록 UTF-8 BOM을 기록한다.
        writer.print('\uFEFF');
        writer.print("복사 방식,실험 구분,초기 항목 수,전송 예산(바이트/tick),쓰기 부하 비율,이동 전 삭제 비율(%),주입한 전송 이상,취소 시점,다른 Tablet 쓰기 수(회/tick),난수 초깃값,시간 기준,결과,측정 항목,측정값\n");
    }

    void run(String scenario, int keys, long budget, double rho, int deletePercent,
            MigrationFixture.Fault fault, String stop, int otherWrites, long seed) {
        long startedAtEpochMillis = System.currentTimeMillis();
        boolean isolated = scenario.equals("SC6");
        MigrationFixture f = new MigrationFixture(isolated, fault);
        StopAndCopyProtocol p = f.protocol;
        Tablet movingTablet = p.expectedTable().get(MigrationFixture.MOVING);
        List<Bytes> movingKeys = new ArrayList<>();
        Bytes otherKey = null;
        for (long index = 0; movingKeys.size() < keys; index++) {
            Bytes key = MigrationFixture.key(seed, index);
            if (movingTablet.range().contains(KeyHash.of(key))) {
                require(f.request(MigrationFixture.SOURCE, new Command.Set(key, MigrationFixture.value(index))).status() == ResultStatus.OK);
                movingKeys.add(key);
            } else if (otherKey == null) {
                otherKey = key;
                require(f.request(MigrationFixture.SOURCE, new Command.Set(key, MigrationFixture.value(index))).status() == ResultStatus.OK);
            }
        }
        if (isolated && otherKey == null) throw new IllegalStateException("다른 Tablet에 해당하는 키가 없습니다");
        int deleted = keys * deletePercent / 100;
        for (int i = 0; i < deleted; i++) {
            require(f.request(MigrationFixture.SOURCE, new Command.Delete(movingKeys.get(i))).status() == ResultStatus.OK);
        }
        Bytes probe = movingKeys.get(keys - 1);
        Bytes probeValue = MigrationFixture.value(seed + 10_000_000);
        require(f.request(MigrationFixture.SOURCE, new Command.Set(probe, probeValue)).status() == ResultStatus.OK);
        Map<Bytes, Bytes> original = new LinkedHashMap<>(f.source.data);
        Map<Bytes, Bytes> expected = f.movingData();
        long logicalBytes = expected.size() * RECORD_BYTES;
        long timeout = (logicalBytes + budget - 1) / budget + 30;
        TickRunner runner = new TickRunner(f, timeout);
        p.begin(f.initialJob);
        // 제어용 write probe로 tick 0의 freeze를 관측한다. workload 거절 수에는 포함하지 않는다.
        require(f.request(MigrationFixture.SOURCE, new Command.Set(probe, probeValue)).status() == ResultStatus.BUSY);
        SplittableRandom random = new SplittableRandom(seed);
        long tick = 0;
        long commitTick = -1;
        long writeUnblockedTick = -1;
        long busy = 0;
        long otherRejected = 0;
        long invariantErrors = 0;
        long wrongCommits = 0;
        double credit = 0;
        boolean stopped = false;
        Bytes otherFinalValue = otherKey == null ? null : original.get(otherKey);
        while (p.state() != JobState.DONE && p.state() != JobState.ABORTED && tick < timeout + 20) {
            if (!stopped && shouldStop(stop, f)) {
                if (stop.equals("cleanup")) {
                    f.source.cleanupFailures = 2;
                    f.source.partialCleanup = true;
                }
                p.abort();
                stopped = true;
                if (p.state() == JobState.ABORTED) {
                    if (probeWrite(f, probe, probeValue) == ResultStatus.OK) writeUnblockedTick = tick;
                    else invariantErrors++;
                    break;
                }
            }
            tick++;
            if (commitTick < 0) {
                credit += rho * budget / RECORD_BYTES;
                while (credit >= 1) {
                    credit--;
                    Bytes key = movingKeys.get(random.nextInt(movingKeys.size()));
                    Command write = random.nextInt(100) < 30 ? new Command.Delete(key)
                            : new Command.Set(key, MigrationFixture.value(tick));
                    if (f.request(MigrationFixture.SOURCE, write).status() == ResultStatus.BUSY) busy++;
                    else invariantErrors++;
                }
                if (f.request(MigrationFixture.SOURCE, new Command.Get(probe)).status() != ResultStatus.OK) invariantErrors++;
                if (f.request(MigrationFixture.DESTINATION, new Command.Get(probe)).status() != ResultStatus.MOVED) invariantErrors++;
            }
            for (int i = 0; i < otherWrites; i++) {
                otherFinalValue = MigrationFixture.value(tick * 100 + i);
                if (f.request(MigrationFixture.SOURCE, new Command.Set(otherKey, otherFinalValue)).status() != ResultStatus.OK) otherRejected++;
            }
            boolean readyBefore = p.readyToCommit();
            long previousVersion = f.coordinator.current().version().value();
            runner.step(stop.equals("copying") && tick == 1 ? 1 : budget);
            if (f.coordinator.current().version().value() != previousVersion && !readyBefore) wrongCommits++;
            if (commitTick < 0 && f.coordinator.current() != p.expectedTable()) {
                commitTick = tick;
            }
            // 현재 소유자에게 같은 값을 쓰는 제어 요청으로 실제 쓰기 재개를 관측한다.
            // 성공 시 새 노드, 취소 시 이전 노드에서 처리된다. workload 거절 수에는 포함하지 않는다.
            if (writeUnblockedTick < 0) {
                ResultStatus writeStatus = probeWrite(f, probe, probeValue);
                if (writeStatus == ResultStatus.OK) writeUnblockedTick = tick;
                else if (writeStatus != ResultStatus.BUSY) invariantErrors++;
            }
        }
        int validationFailures = f.link.validationFailures;
        boolean expectedAbort = stop.equals("copying") || stop.equals("before-final-ack") || stop.equals("switching")
                || fault == MigrationFixture.Fault.CHECKSUM || fault == MigrationFixture.Fault.COUNT
                || fault == MigrationFixture.Fault.BYTES || fault == MigrationFixture.Fault.DROP
                || fault == MigrationFixture.Fault.BAD_ACK || fault == MigrationFixture.Fault.LOST_ACK;
        boolean sourceMatch;
        boolean destinationMatch;
        if (expectedAbort) {
            sourceMatch = original.equals(f.source.data);
            destinationMatch = f.destination.isEmpty();
            if (p.state() != JobState.ABORTED || f.coordinator.current() != p.expectedTable()
                    || !sourceMatch || !destinationMatch || f.source.frozen(movingTablet)) invariantErrors++;
        } else {
            destinationMatch = expected.equals(f.destination);
            Map<Bytes, Bytes> expectedSource = new LinkedHashMap<>(original);
            expectedSource.keySet().removeAll(expected.keySet());
            if (otherKey != null) expectedSource.put(otherKey, otherFinalValue);
            sourceMatch = expectedSource.equals(f.source.data);
            if (p.state() != JobState.DONE || commitTick < 0 || !destinationMatch || !sourceMatch
                    || f.coordinator.successfulCommits != 1
                    || f.coordinator.current().version().value() != p.expectedTable().version().value() + 1
                    || f.coordinator.current().get(MigrationFixture.MOVING).epoch().value() != movingTablet.epoch().value() + 1) invariantErrors++;
            if (f.link.deliver(f.initialJob, movingTablet.epoch(), f.link.lastBatch)) invariantErrors++;
            for (Command command : new Command[] {new Command.Get(probe), new Command.Set(probe, probeValue), new Command.Delete(probe)}) {
                var response = f.request(MigrationFixture.SOURCE, command);
                if (response.status() != ResultStatus.MOVED || !response.redirect().orElseThrow().target().equals(MigrationFixture.DESTINATION)
                        || !response.routingVersion().equals(f.coordinator.current().version())) invariantErrors++;
            }
            if (isolated && !f.coordinator.current().get(MigrationFixture.OTHER).equals(p.expectedTable().get(MigrationFixture.OTHER))) invariantErrors++;
        }
        if (writeUnblockedTick < 0 || (commitTick >= 0 && writeUnblockedTick != commitTick)) invariantErrors++;
        invariantErrors += otherRejected + wrongCommits;
        cases++;
        violations += invariantErrors;
        Map<String, Number> metrics = new LinkedHashMap<>();
        metrics.put("이동 대상 항목 수", expected.size());
        metrics.put("이동 대상 데이터 크기(바이트)", logicalBytes);
        metrics.put("보낸 데이터 크기(바이트)", p.job().progress().sentBytes());
        metrics.put(BaselineResults.BLOCKED, writeUnblockedTick);
        metrics.put(BaselineResults.TOTAL, tick);
        metrics.put("전송량 비율", logicalBytes == 0 ? 0 : (double) p.job().progress().sentBytes() / logicalBytes);
        metrics.put("거절한 쓰기 요청 수", busy);
        metrics.put(BaselineResults.OTHER_REJECTED, otherRejected);
        metrics.put("전송 검증 실패 수", validationFailures);
        metrics.put("중복 전달 수", f.link.duplicateDeliveries);
        metrics.put("이전 노드 데이터 일치 여부", sourceMatch ? 1 : 0);
        metrics.put("새 노드 데이터 일치 여부", destinationMatch ? 1 : 0);
        metrics.put("잘못된 소유권 전환 수", wrongCommits);
        metrics.put("정합성 규칙 위반 수", invariantErrors);
        String outcome = outcome(p.state(), runner.timedOut(), stopped);
        BaselineResults.Condition condition = new BaselineResults.Condition(scenario, keys, budget, rho,
                deletePercent, fault, stop, otherWrites);
        var snapshot = new BaselineResults.Run(condition, seed, outcome, metrics, startedAtEpochMillis);
        results.add(snapshot);
        observer.accept(snapshot);
        writeRun(writer, snapshot);
    }

    static void writeRun(PrintWriter writer, BaselineResults.Run snapshot) {
        var condition = snapshot.condition();
        for (String name : BaselineResults.METRICS) {
            var metric = Map.entry(name, snapshot.metrics().get(name));
            if (!BaselineResults.include(condition, metric.getKey()) && metric.getValue().doubleValue() == 0) continue;
            String value = metric.getValue() instanceof Double
                    ? String.format(Locale.ROOT, "%.6f", metric.getValue().doubleValue()) : metric.getValue().toString();
            // CSV 줄바꿈은 운영체제에 관계없이 LF로 고정한다.
            writer.printf(Locale.ROOT, "stop-and-copy,%s,%d,%d,%.2f,%d,%s,%s,%d,%d,tick,%s,%s,%s\n",
                    scenarioLabel(condition.scenario()), condition.keys(), condition.budget(), condition.rho(), condition.deletePercent(),
                    faultLabel(condition.fault()), stopLabel(condition.stop()), condition.otherWrites(), snapshot.seed(), snapshot.outcome(), metric.getKey(), value);
        }
    }

    static Path reportPath(Path csv) {
        String name = csv.getFileName().toString();
        String report = name.equals("summary.csv") ? "results.md" : name.replaceFirst("\\.csv$", "") + "-results.md";
        return csv.resolveSibling(report);
    }

    void writeReport(Path path, String csvName) throws IOException { results.write(path, csvName); }

    private static String outcome(JobState state, boolean timedOut, boolean stopped) {
        if (state == JobState.DONE) return stopped ? "취소 거부" : "완료";
        if (state == JobState.ABORTED && timedOut) return "취소(timeout)";
        if (state == JobState.ABORTED && stopped) return "취소(요청)";
        throw new IllegalStateException("예상하지 못한 실험 종료: " + state);
    }

    private static ResultStatus probeWrite(MigrationFixture fixture, Bytes key, Bytes value) {
        var owner = fixture.coordinator.current().get(MigrationFixture.MOVING).owner();
        return fixture.request(owner, new Command.Set(key, value)).status();
    }

    private static boolean shouldStop(String point, MigrationFixture f) {
        return switch (point) {
            case "copying" -> f.protocol.state() == JobState.COPYING && f.protocol.job().progress().sentBytes() > 0;
            case "before-final-ack" -> f.protocol.state() == JobState.COPYING && f.link.lastBatch != null && f.link.lastBatch.last();
            case "switching" -> f.protocol.state() == JobState.SWITCHING;
            case "after-commit", "cleanup" -> f.protocol.state() == JobState.CLEANING_UP;
            default -> false;
        };
    }

    static String scenarioLabel(String scenario) {
        return switch (scenario) {
            case "SC1" -> "SC1 저장 항목 수";
            case "SC1-rate" -> "SC1 전송 예산";
            case "SC2" -> "SC2 이동 중 쓰기";
            case "SC3" -> "SC3 이동 전 삭제";
            case "SC4" -> "SC4 이동 중 취소";
            case "SC5" -> "SC5 전송 이상";
            case "SC6" -> "SC6 Tablet 격리";
            default -> throw new IllegalArgumentException("알 수 없는 실험 구분: " + scenario);
        };
    }

    static String faultLabel(MigrationFixture.Fault fault) {
        return switch (fault) {
            case NONE -> "없음";
            case CHECKSUM -> "checksum 손상";
            case COUNT -> "항목 수 불일치";
            case BYTES -> "데이터 크기 불일치";
            case DUPLICATE -> "중복 전달";
            case DROP -> "batch 누락";
            case ACK_DELAY -> "마지막 ACK 3 tick 지연";
            case BAD_ACK -> "잘못된 ACK";
            case LOST_ACK -> "ACK 누락";
        };
    }

    static String stopLabel(String stop) {
        return switch (stop) {
            case "none" -> "없음";
            case "copying" -> "데이터 복사 중";
            case "before-final-ack" -> "마지막 ACK 전";
            case "switching" -> "소유권 전환 직전";
            case "after-commit" -> "소유권 전환 직후";
            case "cleanup" -> "이전 노드 데이터 정리 중 (정리 실패 2회)";
            default -> throw new IllegalArgumentException("알 수 없는 취소 시점: " + stop);
        };
    }

    private static void require(boolean success) {
        if (!success) throw new IllegalStateException("실험 초기 데이터 준비 실패");
    }
}
