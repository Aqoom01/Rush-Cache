package io.openkache.server.integration;

import io.openkache.core.balance.placement.*;
import io.openkache.core.balance.split.*;
import io.openkache.core.model.NodeId;
import io.openkache.core.migration.MigrationScheduler;
import io.openkache.core.migration.experiment.SchedulerAlgorithms;
import io.openkache.core.migration.strategy.*;
import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import io.openkache.core.tablet.ring.RingScheme;
import io.openkache.core.tablet.tag.HashTagScheme;
import io.openkache.core.tablet.order.KeyOrderScheme;
import io.openkache.core.tablet.resize.ResizableSlotScheme;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;

/** 전체 후보·준비된 복제 그룹·제어 예산의 불변 설정. 단위는 별도 표시 외 ms/bytes다. */
public record IntegrationConfig(Properties values) {
    public static final Set<String> A1 = Set.of("PB", "PB_FENCED", "RAFT", "VR", "CHAIN", "CRAQ", "ROSE");
    public static final Set<String> A2 = Set.of("O00", "V01", "V02", "V03", "V04", "V05", "V06", "V07");
    public static final Set<String> PROTOCOLS;
    public static final Set<String> DISTRIBUTIONS = Set.of("RANGE", "SLOT", "RING", "RING_ALLOC", "TAG", "RESIZABLE", "ORDER");
    public static final Set<String> PLACEMENTS = Set.of("NONE", "GREEDY", "HYSTERESIS", "COST", "ESTORE", "P2C", "SLICER", "RENDEZVOUS", "FIXED_LPT");
    public static final Set<String> KEYS = Set.of("run.id", "protocol", "a1.method", "a2.method", "failure.detector", "gateway.host", "gateway.port",
            "groups.count", "groups.initial", "groups.min", "replica.port", "failure.timeout.ms", "failure.grace.ms", "failure.seed",
            "distribution", "tablets.per.group", "split", "placement", "copy", "migration.bytes.per.tick", "migration.tick.ms",
            "migration.timeout.ms", "request.timeout.ms", "storage.workers", "history.target.entries", "prepare.max.entries",
            "prepare.max.encoded.bytes", "prepare.max.delay.ms", "apply.max.in.flight.keys", "storage.io.workers",
            "read.selection", "meta.batch.entries", "meta.batch.delay.ms", "migration.max.records", "ledger.capacity",
            "vr.mode", "rose.limit", "replication.retry.ms", "scaler", "scheduler", "drain", "control.interval.ms",
            "control.cooldown.windows", "control.max.plans", "scheduler.concurrent.jobs", "scheduler.jobs.per.node",
            "scheduler.node.bytes.per.tick", "copy.batch.bytes", "copy.dirty.threshold.bytes", "copy.max.live.rounds",
            "scaling.node.bytes", "scaling.key.bytes", "scaling.read.service", "scaling.write.service", "scaling.slo",
            "scaling.low", "scaling.high", "scaling.target", "scaling.provision.windows", "scaling.period",
            "scaling.training.periods", "scaling.forecast.horizon", "scaling.calibration.file", "scaling.history.file",
            "scaling.transfer.bytes", "scaling.migration.penalty", "scaling.write.ratio", "scaling.placement.search.limit", "native.data.root");
    static { var all = new HashSet<>(A1); all.addAll(A2); PROTOCOLS = Set.copyOf(all); }
    public IntegrationConfig {
        Properties p = new Properties(); p.putAll(values); values = p;
        int groups = integer(p, "groups.count", 2, 2, 8);
        int initial = integer(p, "groups.initial", 2, 1, groups);
        integer(p, "groups.min", 1, 1, initial);
        for (String k : p.stringPropertyNames()) {
            if (KEYS.contains(k) || FailureMethods.KEYS.contains(k) || IoniaProfile.OPTION_KEYS.contains(k)) continue;
            if (k.matches("group\\.[0-7]\\.hosts") && Integer.parseInt(k.split("\\.")[1]) < groups) continue;
            throw new IllegalArgumentException("unknown integration setting: " + k);
        }
        if (!PROTOCOLS.contains(p.getProperty("protocol", "O00"))) throw new IllegalArgumentException("unsupported replication engine");
        if (p.containsKey("protocol") && (p.containsKey("a1.method") || p.containsKey("a2.method"))) throw new IllegalArgumentException("protocol is a legacy isolated engine; use a1.method AND a2.method for the joint cache");
        if (!A1.contains(p.getProperty("a1.method", "RAFT")) || !A2.contains(p.getProperty("a2.method", "O00"))) throw new IllegalArgumentException("unsupported joint A1/A2 method");
        if (!p.containsKey("protocol")) integer(p, "replica.port", 17101, 1024, 65522);
        if (!p.getProperty("run.id", "").matches("[A-Za-z0-9_-]{1,64}")) throw new IllegalArgumentException("run.id required");
        String dataRoot = p.getProperty("native.data.root", "");
        if (!dataRoot.isEmpty() && (!dataRoot.matches("/[A-Za-z0-9_./-]+") || !Path.of(dataRoot).isAbsolute()
                || java.util.stream.StreamSupport.stream(Path.of(dataRoot).spliterator(), false).anyMatch(x -> x.toString().equals(".."))))
            throw new IllegalArgumentException("native.data.root must be an absolute literal path without traversal");
        p.setProperty("failure.detector", FailureMethods.canonical(p.getProperty("failure.detector", "FIXED_TIMEOUT")));
        FailureMethods.validate(p);
        var allHosts = new HashSet<String>();
        for (int g = 0; g < groups; g++) {
            String[] hosts = p.getProperty("group." + g + ".hosts", "").split(",", -1);
            if (hosts.length != 3) throw new IllegalArgumentException("three replica hosts per group required");
            for (String host : hosts) if (!host.trim().matches("[A-Za-z0-9._-]+") || !allHosts.add(host.trim())) throw new IllegalArgumentException("distinct replica hostnames/IPv4 required");
        }
        if (!p.getProperty("gateway.host", "127.0.0.1").matches("[A-Za-z0-9._-]+")) throw new IllegalArgumentException("invalid gateway host");
        for (String key : List.of("gateway.port", "replica.port")) integer(p, key, key.startsWith("gateway") ? 7300 : 17101, 1024, 65530);
        integer(p, "tablets.per.group", 4, 1, 4096); integer(p, "storage.workers", 2, 1, 256);
        integer(p, "request.timeout.ms", 3000, 50, 60000); integer(p, "failure.timeout.ms", 1000, 200, 60000);
        integer(p, "failure.grace.ms", 3000, 0, 120000); integer(p, "migration.tick.ms", 10, 1, 1000);
        integer(p, "migration.bytes.per.tick", 65536, 256, 16777216); integer(p, "migration.timeout.ms", 300000, 1000, 3600000);
        integer(p, "migration.max.records", 200000, 1, 1000000); integer(p, "ledger.capacity", 65536, 16, 1000000);
        integer(p, "control.interval.ms", 0, 0, 60000); integer(p, "control.cooldown.windows", 2, 0, 10000);
        integer(p, "control.max.plans", 1000, 1, 100000); integer(p, "replication.retry.ms", 50, 1, 60000);
        integer(p, "rose.limit", 4, -1, 1000000);
        if (!Set.of("FULL_LOG", "INCREMENTAL", "INCREMENTAL_CATCHUP").contains(p.getProperty("vr.mode", "INCREMENTAL_CATCHUP"))) throw new IllegalArgumentException("unsupported VR mode");
        String d = p.getProperty("distribution", "RANGE");
        if (!DISTRIBUTIONS.contains(d) || !Set.of("NONE", "MIDPOINT", "HEAT", "ESTORE").contains(p.getProperty("split", "NONE"))
                || !PLACEMENTS.contains(p.getProperty("placement", "NONE"))
                || !Set.of("STOP", "PRE", "POST").contains(p.getProperty("copy", "PRE"))) throw new IllegalArgumentException("unsupported B strategy");
        if (!Set.of("RANGE", "TAG", "ORDER").contains(d) && !p.getProperty("split", "NONE").equals("NONE")) throw new IllegalArgumentException("distribution does not support individual split");
        if (d.startsWith("RING") && !p.getProperty("placement", "NONE").equals("NONE")) throw new IllegalArgumentException("token ring does not allow arbitrary placement");
        if (p.getProperty("placement", "NONE").equals("FIXED_LPT") && (!p.getProperty("split", "NONE").equals("NONE") || d.equals("RESIZABLE"))) throw new IllegalArgumentException("fixed-tablet LPT does not split or merge units");
        if (!p.getProperty("scaler", "fixed").equals("fixed") && !ScalingPolicies.METHODS.contains(p.getProperty("scaler"))) throw new IllegalArgumentException("unsupported scaler");
        if (!SchedulerAlgorithms.METHODS.contains(p.getProperty("scheduler", "fifo")) || !DrainStrategies.METHODS.contains(p.getProperty("drain", "preflight-all"))) throw new IllegalArgumentException("unsupported scheduler/drain");
        String readMode=p.getProperty("read.selection","ADAPTIVE").toUpperCase(Locale.ROOT).replace('-', '_'); p.setProperty("read.selection",readMode);
        String dataMethod=p.containsKey("protocol")?p.getProperty("protocol"):p.getProperty("a2.method","O00");
        if (A2.contains(dataMethod) && !dataMethod.equals("V06") && !readMode.equals("ADAPTIVE")) throw new IllegalArgumentException("ROUND_ROBIN read selection requires A2 V06");
        IoniaProfile.validate(p);
        schedulerSettings(p); copySettings(p); scalingSettings(p, 32);
    }
    @Override public Properties values() { Properties p = new Properties(); p.putAll(values); return p; }
    public static void main(String[] args) throws IOException {
        if (args.length == 1 && args[0].equals("--catalog")) { System.out.println(catalog()); return; }
        if (args.length != 1) throw new IllegalArgumentException("usage: IntegrationConfig config.properties");
        var c = load(Path.of(args[0])); System.out.println("CONFIG_VALID protocol=" + c.protocol() + " groups=" + c.groups() + " run=" + c.runId());
    }
    /** 실행 adapter가 선택하는 전체 후보 목록. 문자열 등록과 실행 검증은 별도 상태로 보고한다. */
    public static String catalog() {
        return "{\"a1\":" + array(A1) + ",\"a2\":" + array(A2) + ",\"failureDetector\":" + array(FailureMethods.ALL)
                + ",\"readSelection\":" + array(Set.of("ADAPTIVE","ROUND_ROBIN")) + ",\"distribution\":" + array(DISTRIBUTIONS) + ",\"split\":" + array(Set.of("NONE", "MIDPOINT", "HEAT", "ESTORE"))
                + ",\"placement\":" + array(PLACEMENTS)
                + ",\"scaler\":" + array(java.util.stream.Stream.concat(java.util.stream.Stream.of("fixed"), ScalingPolicies.METHODS.stream()).toList())
                + ",\"scheduler\":" + array(SchedulerAlgorithms.METHODS) + ",\"copy\":" + array(CopyDrivers.METHODS) + ",\"drain\":" + array(DrainStrategies.METHODS)
                + ",\"engineRelationship\":\"A1 replicated admission and routing authority + A2 SSD data replication; independently selectable, co-located on each replica\"}";
    }
    private static String array(Collection<String> items) { return items.stream().sorted().map(JsonLine::quote).collect(java.util.stream.Collectors.joining(",", "[", "]")); }
    public static IntegrationConfig load(Path path) throws IOException {
        Properties p = new Properties(); try (var in = Files.newBufferedReader(path)) { p.load(in); }
        for (String k : List.of("scaling.calibration.file", "scaling.history.file")) if (p.containsKey(k)) p.setProperty(k, path.toAbsolutePath().getParent().resolve(p.getProperty(k)).normalize().toString());
        return new IntegrationConfig(p);
    }
    static int integer(Properties p, String k, int fallback, int min, int max) {
        int n = Integer.parseInt(p.getProperty(k, "" + fallback)); if (n < min || n > max) throw new IllegalArgumentException(k + " outside " + min + ".." + max); return n;
    }
    public String text(String k, String fallback) { return values.getProperty(k, fallback); }
    public int number(String k, int fallback) { return Integer.parseInt(text(k, "" + fallback)); }
    public int groups() { return number("groups.count", 2); }
    public int initialGroups() { return number("groups.initial", 2); }
    public int minGroups() { return number("groups.min", 1); }
    /** Explicit protocol is retained only for historical isolated-engine tests. */
    public boolean composed() { return !values.containsKey("protocol"); }
    public String a1Method() { return text("a1.method", "RAFT"); }
    public String a2Method() { return text("a2.method", "O00"); }
    public String protocol() { return composed() ? a1Method() + "+" + a2Method() : text("protocol", "O00"); }
    /** Separate native endpoints and storage; both run on the same three physical hosts. */
    public IntegrationConfig layer(boolean authority) {
        if (!composed()) throw new IllegalStateException("not a joint configuration");
        Properties p = values(); p.remove("a1.method"); p.remove("a2.method");
        p.setProperty("protocol", authority ? a1Method() : a2Method());
        p.setProperty("replica.port", "" + (number("replica.port", 17101) + (authority ? 8 : 0)));
        return new IntegrationConfig(p);
    }
    public boolean ionia() { return A2.contains(protocol()); }
    public boolean masterRequired() { return Set.of("CHAIN", "CRAQ", "ROSE").contains(protocol()); }
    public boolean crashOnly() { return protocol().equals("PB") || masterRequired(); }
    public String runId() { return values.getProperty("run.id"); }
    public String failureMethod() { return text("failure.detector", "FIXED_TIMEOUT"); }
    public boolean externalFd() { return !failureMethod().equals("FIXED_TIMEOUT"); }
    public int nativeWatchdogMillis() { return number("fd.native.watchdog.ms", FailureMethods.legacy(failureMethod()) ? number("failure.timeout.ms", 1000) * 10 : 300000); }
    public String copyName() { return switch (text("copy", "PRE")) { case "STOP" -> "stop-and-copy"; case "POST" -> "post-copy"; default -> "pre-copy"; }; }
    public List<String> hosts(int group) { if (group < 0 || group >= groups()) throw new IllegalArgumentException("group outside inventory"); return Arrays.stream(values.getProperty("group." + group + ".hosts").split(",")).map(String::trim).toList(); }
    public Map<NodeId, InetSocketAddress> addresses(int group, int offset) {
        Map<NodeId, InetSocketAddress> out = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) out.put(new NodeId(ionia() ? i + 1 : i), new InetSocketAddress(hosts(group).get(i), number("replica.port", 17101) + offset));
        if (out.values().stream().anyMatch(InetSocketAddress::isUnresolved)) throw new IllegalArgumentException("unresolved replica host"); return Collections.unmodifiableMap(out);
    }
    public DistributionScheme scheme() {
        int n = number("tablets.per.group", 4);
        return switch (text("distribution", "RANGE")) {
            case "SLOT" -> new FixedSlotScheme(n * initialGroups()); case "RING" -> new RingScheme(n); case "RING_ALLOC" -> new RingScheme(n, RingScheme.Placement.BALANCED);
            case "TAG" -> new HashTagScheme(n); case "ORDER" -> new KeyOrderScheme(n); case "RESIZABLE" -> new ResizableSlotScheme(n); default -> new HashRangeScheme(n);
        };
    }
    public SplitPolicy splitter() { return switch (text("split", "NONE")) { case "MIDPOINT" -> new MidpointSplitPolicy(); case "HEAT" -> new HeatSplitPolicy(); case "ESTORE" -> new EStoreSplitPolicy(); default -> throw new IllegalStateException("split disabled"); }; }
    public PlacementPolicy placer() { return switch (text("placement", "NONE")) { case "GREEDY" -> new GreedyLoadPlacement(); case "HYSTERESIS" -> new HysteresisRebalancing(); case "COST" -> new CostAwareRebalancing(); case "ESTORE" -> new EStoreInspiredPlacement(); case "P2C" -> new PowerOfTwoChoicesPlacement(); case "SLICER" -> new SlicerInspiredPlacement(); case "RENDEZVOUS" -> new WeightedRendezvousPlacement(); default -> throw new IllegalStateException("placement disabled"); }; }
    public MigrationScheduler.Settings schedulerSettings() { return schedulerSettings(values); }
    private static MigrationScheduler.Settings schedulerSettings(Properties p) {
        int tick = integer(p, "migration.tick.ms", 10, 1, 1000);
        long timeout = integer(p, "migration.timeout.ms", 300000, 1000, 3600000);
        return new MigrationScheduler.Settings(integer(p, "scheduler.concurrent.jobs", 4, 1, 60), integer(p, "scheduler.jobs.per.node", 4, 1, 60),
                integer(p, "migration.bytes.per.tick", 65536, 256, 16777216), integer(p, "scheduler.node.bytes.per.tick", 65536, 1, 16777216), Math.max(1, timeout / tick));
    }
    public CopyDrivers.Settings copySettings() { return copySettings(values); }
    private static CopyDrivers.Settings copySettings(Properties p) { return new CopyDrivers.Settings(integer(p, "copy.batch.bytes", 65536, 1, 16777216), integer(p, "copy.dirty.threshold.bytes", 16384, 1, 16777216), integer(p, "copy.max.live.rounds", 10, 1, 10000)); }
    public ScalingConfig scalingSettings(int keys) { return scalingSettings(values, keys); }
    private static ScalingConfig scalingSettings(Properties p, int keys) {
        Properties s = new Properties(); s.putAll(p); s.setProperty("scaling.keys", "" + Math.max(4, ((keys + 3) / 4) * 4));
        s.setProperty("scaling.initial.nodes", p.getProperty("groups.initial", "2")); s.setProperty("scaling.min.nodes", p.getProperty("groups.min", "1")); s.setProperty("scaling.max.nodes", p.getProperty("groups.count", "2"));
        s.setProperty("scaling.node.bytes", p.getProperty("scaling.node.bytes", "67108864")); s.setProperty("scaling.key.bytes", p.getProperty("scaling.key.bytes", "256"));
        s.setProperty("scaling.cooldown.windows", p.getProperty("control.cooldown.windows", "2")); s.setProperty("scaling.control.windows", "1");
        return ScalingConfig.from(s);
    }
}
