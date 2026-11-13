package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_second.IoniaLeaderReadClient;
import io.openkache.core.replication.ionia_third.IoniaHistorySettings;
import io.openkache.core.replication.ionia_fourth.IoniaBatchSettings;
import io.openkache.core.replication.ionia_fifth.IoniaApplySettings;
import io.openkache.core.replication.ionia_sixth.IoniaWorkerSettings;
import io.openkache.core.replication.ionia_seventh.*;
import io.openkache.core.replication.ionia_eighth.*;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.TimeSource;
import io.openkache.server.ionia.*;
import io.openkache.server.ionia_third.IoniaThirdNode;
import io.openkache.server.ionia_fourth.IoniaFourthNode;
import io.openkache.server.ionia_fifth.IoniaFifthNode;
import io.openkache.server.ionia_sixth.IoniaSixthNode;
import io.openkache.server.ionia_eighth.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** O00/V01–V07의 기존 서버/client를 그대로 조립한다. 표시 ID와 과거 폴더 이름을 여기서 매핑한다. */
final class IoniaProfile {
    record Replica(AutoCloseable lifecycle, Supplier<IoniaEngine.ViewInfo> view, LongConsumer startViewChange,
                   Supplier<IoniaEngine.Progress> progress) implements AutoCloseable {
        @Override public void close() { try { lifecycle.close(); } catch (Exception e) { throw new IllegalStateException(e); } }
    }
    record Session(Supplier<CompletionStage<IoniaEngine.ViewInfo>> refresh,
                   BiFunction<Command, RequestContext, CompletionStage<Result>> execute,
                   Consumer<TimerEvent> timer, Runnable stop) implements AutoCloseable {
        @Override public void close() { stop.run(); }
    }
    static final Set<String> OPTION_KEYS;
    static { var keys = new HashSet<>(IoniaReadSelectionSettings.KEYS); keys.addAll(IoniaMetaBatchSettings.KEYS); keys.add("storage.shards"); OPTION_KEYS=Set.copyOf(keys); }
    private IoniaProfile() { }
    static IoniaHistorySettings history(Properties p) { return new IoniaHistorySettings(Integer.parseInt(p.getProperty("history.target.entries", "4096"))); }
    static IoniaBatchSettings batch(Properties p) { return new IoniaBatchSettings(Integer.parseInt(p.getProperty("prepare.max.entries", "16")),
            Integer.parseInt(p.getProperty("prepare.max.encoded.bytes", "65536")), Long.parseLong(p.getProperty("prepare.max.delay.ms", "20")) * 1000000L); }
    static IoniaApplySettings apply(Properties p) { return new IoniaApplySettings(Integer.parseInt(p.getProperty("apply.max.in.flight.keys", "16")), Integer.parseInt(p.getProperty("storage.workers", "2"))); }
    static IoniaWorkerSettings worker(Properties p) { return new IoniaWorkerSettings(Integer.parseInt(p.getProperty("storage.shards", p.getProperty("storage.workers", "2"))), Integer.parseInt(p.getProperty("storage.io.workers", "2"))); }
    static IoniaReadSelectionSettings selection(Properties p) { return new IoniaReadSelectionSettings(IoniaReadSelectionSettings.Mode.valueOf(p.getProperty("read.selection", "ADAPTIVE").toUpperCase(Locale.ROOT).replace('-', '_')),
            Integer.parseInt(p.getProperty("read.logical.limit","128")), Integer.parseInt(p.getProperty("read.rpc.limit.per.replica","256")),
            Integer.parseInt(p.getProperty("read.rtt.initial.ms","5")), Integer.parseInt(p.getProperty("read.sample.ttl.ms","1000")),
            Integer.parseInt(p.getProperty("read.failure.cooldown.ms","200")), Integer.parseInt(p.getProperty("read.explore.every","16"))); }
    static IoniaMetaBatchSettings meta(Properties p) { return new IoniaMetaBatchSettings(Integer.parseInt(p.getProperty("meta.batch.entries", "16")), Integer.parseInt(p.getProperty("meta.batch.bytes","65536")),
            Long.parseLong(p.getProperty("meta.batch.delay.ms", "1")) * 1000000L, Integer.parseInt(p.getProperty("meta.pending.limit","1024")), Integer.parseInt(p.getProperty("meta.inflight.limit","32"))); }
    static void validate(Properties p) { history(p); batch(p); apply(p); worker(p); selection(p); meta(p); }
    static IoniaConfig membership(IntegrationConfig c, int group, int replica) {
        return new IoniaConfig(new NodeId(replica + 1), new NodeId(1), new ArrayList<>(c.addresses(group, 0).keySet()), 0);
    }
    static String configuration(IntegrationConfig c, int group) {
        IoniaConfig m = membership(c, group, 0); Properties p = c.values();
        return switch (c.protocol()) {
            case "V02" -> history(p).configurationId(m); case "V03" -> batch(p).configurationId(m);
            case "V04" -> apply(p).configurationId(m); case "V05" -> worker(p).configurationId(m);
            case "V07" -> IoniaMetaBatchSettings.configuration(m.configurationId()); default -> m.configurationId();
        };
    }
    static Replica replica(IntegrationConfig c, int group, int index, Path directory) throws IOException {
        IoniaConfig m = membership(c, group, index); var addresses = c.addresses(group, 0);
        Properties p = c.values(); int shards = c.number("storage.workers", 2);
        // 외부 FD는 정상 리더 손실을 신호로 전달한다. 내부 타이머는 10배 watchdog/재시도다.
        long timeout = c.externalFd() ? c.nativeWatchdogMillis() : c.number("failure.timeout.ms", 1000);
        return switch (c.protocol()) {
            case "V02" -> { var n = new IoniaThirdNode(m, addresses, directory, shards, timeout, history(p)); yield new Replica(n, n.engine()::viewInfo, n.engine()::startViewChange, n.engine()::progress); }
            case "V03" -> { var n = new IoniaFourthNode(m, addresses, directory, shards, timeout, batch(p)); yield new Replica(n, n.engine()::viewInfo, n.engine()::startViewChange, n.engine()::progress); }
            case "V04" -> { var n = new IoniaFifthNode(m, addresses, directory, shards, timeout, apply(p)); yield new Replica(n, n.engine()::viewInfo, n.engine()::startViewChange, n.engine()::progress); }
            case "V05" -> { var n = new IoniaSixthNode(m, addresses, directory, timeout, worker(p)); yield new Replica(n, n.engine()::viewInfo, n.engine()::startViewChange, n.engine()::progress); }
            case "V07" -> { var n = new IoniaEighthNode(m, addresses, directory, shards, timeout); yield new Replica(n, n.engine()::viewInfo, n.engine()::startViewChange, n.engine()::progress); }
            default -> { var n = new IoniaNode(m, addresses, directory, shards, timeout); yield new Replica(n, n.engine()::viewInfo, n.engine()::startViewChange, n.engine()::progress); }
        };
    }
    static Session session(IntegrationConfig c, int group, ClientId id, IoniaClientTransport transport) {
        List<NodeId> members = new ArrayList<>(c.addresses(group, 0).keySet()); var leader = members.getFirst(); TimeSource clock = TimeSource.system();
        return switch (c.protocol()) {
            case "V01" -> { var x = new IoniaLeaderReadClient(id, members, leader, 0, transport, clock); yield new Session(x::refreshView, x::execute, x::onTimer, x::close); }
            case "V06" -> { var x = new IoniaAdaptiveReadClient(id, members, leader, 0, transport, clock, selection(c.values())); yield new Session(x::refreshView, x::execute, x::onTimer, x::close); }
            case "V07" -> { var x = new IoniaMetaBatchClient(id, members, leader, 0, (IoniaMetaBatchTransport) transport, clock, meta(c.values())); yield new Session(x::refreshView, x::execute, x::onTimer, x::close); }
            default -> { var x = new IoniaClient(id, members, leader, 0, transport, clock, new IoniaClient.Options(members.get(1), true, 50_000_000)); yield new Session(x::refreshView, x::execute, x::onTimer, x::close); }
        };
    }
}
