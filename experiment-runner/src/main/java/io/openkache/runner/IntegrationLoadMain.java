package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/** 실제 통합 gateway용 고정률·bounded admission 부하. 기존 요청 생성기와 client wire를 재사용한다. */
public final class IntegrationLoadMain {
    private static final int STATUS = 96, BALANCE = 97, MOVE = 98, RESET = 99, READY = 101, SCALE = 102, STOP_CONTROL = 103;
    private static final Set<String> EXTRA = Set.of("integration.run.id", "integration.rate", "integration.rate.change.at", "integration.rate.after", "integration.balance.at", "integration.move.tablet",
            "integration.move.to", "integration.audit.keys", "integration.drain.ms", "integration.window.requests", "integration.scale.schedule", "integration.key.mode", "integration.tag.count", "integration.require.scaler.decision", "integration.require.joint");
    private record Expected(String hash, boolean missing) { }
    private final RunnerConfig config;
    private final Properties extra;
    private final Path output;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService actions = Executors.newSingleThreadExecutor();
    private final Map<Bytes, Bytes> keyTransform = new HashMap<>();
    private final Map<Long, Integer> scaleSchedule = new TreeMap<>();
    private final ConcurrentLinkedQueue<WireClient> pool = new ConcurrentLinkedQueue<>();
    private final Set<WireClient> connections = ConcurrentHashMap.newKeySet();
    private final Map<Bytes, Expected> expected = new ConcurrentHashMap<>();
    private final Set<Bytes> uncertain = ConcurrentHashMap.newKeySet();
    private final ReentrantLock[] locks = new ReentrantLock[1024];
    private final AtomicLong operations = new AtomicLong(), success = new AtomicLong(), miss = new AtomicLong(), failed = new AtomicLong(), dropped = new AtomicLong(), violations = new AtomicLong();
    private final AtomicReference<Throwable> asyncError = new AtomicReference<>();
    private final List<Long> successLatency = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> terminalLatency = Collections.synchronizedList(new ArrayList<>());
    private final BufferedWriter raw;
    private final MessageDigest tape;
    private final long rate, rateChangeAt, rateAfter;
    private long measurementStart, measurementEnd;
    private IntegrationLoadMain(Path path, Path output) throws Exception {
        Properties all = new Properties(); try (var in = Files.newBufferedReader(path)) { all.load(in); }
        extra = new Properties(); Properties base = new Properties();
        for (String key : all.stringPropertyNames()) {
            if (key.startsWith("integration.")) {
                if (!EXTRA.contains(key)) throw new IllegalArgumentException("unknown integration load setting: " + key); extra.setProperty(key, all.getProperty(key));
            } else base.setProperty(key, all.getProperty(key));
        }
        config = RunnerConfig.from(base);
        if (config.keyLength() < ("key-" + (config.keyCount() - 1)).length()) throw new IllegalArgumentException("key.length truncates unique keys");
        if (config.concurrency() > 1024 || config.measuredRequests() > 10000000 || config.measuredRequests() < 1) throw new IllegalArgumentException("unsupported population");
        if (!extra.getProperty("integration.run.id", "").matches("[A-Za-z0-9_-]{1,64}")) throw new IllegalArgumentException("integration.run.id required");
        rate = number("integration.rate", 100, 1, 100000);
        rateChangeAt = number("integration.rate.change.at", config.measuredRequests(), 1, config.measuredRequests());
        rateAfter = number("integration.rate.after", rate, 1, 100000);
        if (extra.containsKey("integration.rate.change.at") != extra.containsKey("integration.rate.after")
                || extra.containsKey("integration.rate.after") && rateChangeAt == config.measuredRequests()) {
            throw new IllegalArgumentException("rate change requires a new rate and a request index inside measurement");
        }
        number("integration.balance.at", -1, -1, config.measuredRequests() - 1);
        number("integration.move.tablet", -1, -1, Long.MAX_VALUE);
        number("integration.move.to", 1, 0, 1);
        number("integration.audit.keys", Math.min(1000, config.keyCount()), 1, config.keyCount());
        number("integration.drain.ms", 300000, 1000, 3600000);
        number("integration.window.requests", config.measuredRequests(), 1, config.measuredRequests());
        if (!Set.of("DEFAULT", "TAGGED", "ORDERED").contains(extra.getProperty("integration.key.mode", "DEFAULT"))) throw new IllegalArgumentException("unknown integration key mode");
        number("integration.tag.count", Math.min(8, config.keyCount()), 1, config.keyCount());
        if (extra.getProperty("integration.key.mode", "DEFAULT").equals("ORDERED") && config.keyLength() < 8) throw new IllegalArgumentException("ordered key mode needs at least eight bytes");
        if (!extra.getProperty("integration.scale.schedule", "").isEmpty()) for (String item : extra.getProperty("integration.scale.schedule").split(",")) {
            String[] pair = item.split(":", -1); if (pair.length != 2) throw new IllegalArgumentException("scale schedule: request_index:group_count");
            long at = Long.parseLong(pair[0]); int count = Integer.parseInt(pair[1]);
            if (at < 0 || at >= config.measuredRequests() || count < 1 || count > 8 || scaleSchedule.put(at, count) != null) throw new IllegalArgumentException("invalid/duplicate scale schedule index");
        }
        if (!Set.of("true", "false").contains(extra.getProperty("integration.require.scaler.decision", "false"))) throw new IllegalArgumentException("invalid required scaler decision flag");
        canonicalKey(config.keyCount() - 1);
        if (output == null) { this.output = null; raw = null; tape = null; return; }
        this.output = output.toAbsolutePath(); Files.createDirectories(this.output.getParent()); Files.createDirectory(this.output);
        Files.copy(path, this.output.resolve("config.properties"));
        raw = Files.newBufferedWriter(this.output.resolve("requests.csv"));
        raw.write("phase,index,client,operation,command,key_sha256,planned_nanos,start_nanos,end_nanos,status,attempts,latency_from_schedule_ns\n");
        tape = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < locks.length; i++) locks[i] = new ReentrantLock();
    }
    private long number(String key, long fallback, long min, long max) {
        long n = Long.parseLong(extra.getProperty(key, "" + fallback)); if (n < min || n > max) throw new IllegalArgumentException("invalid " + key); return n;
    }
    private WireClient borrow() throws IOException {
        WireClient wire = pool.poll();
        if (wire == null) { wire = new WireClient(config.targetHost(), config.targetPort(), config.requestTimeoutMs() + 1000); connections.add(wire); }
        return wire;
    }
    private WireClient.Response control(int opcode, byte[] body) throws IOException {
        try (WireClient client = new WireClient(config.targetHost(), config.targetPort(), Math.max(config.requestTimeoutMs() + 1000, 10000))) { return client.peer(opcode, body); }
    }
    private void ready() throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < until) {
            try {
                var r = control(READY, new byte[0]);
                if (r.status() == 0) {
                    String body = new String(r.value(), StandardCharsets.UTF_8);
                    if (!body.contains("\"runId\":\"" + extra.getProperty("integration.run.id") + "\"")) throw new IllegalArgumentException("gateway run ID differs");
                    return;
                }
            } catch (IOException e) { /* readiness 실패는 기록 ACK가 아니다. */ }
            Thread.sleep(200);
        }
        throw new IOException("gateway/native read quorum not ready");
    }
    private static Bytes key(Command c) { return c instanceof Command.Get g ? g.key() : c instanceof Command.Set s ? s.key() : ((Command.Delete) c).key(); }
    private Bytes canonicalKey(int index) {
        String mode = extra.getProperty("integration.key.mode", "DEFAULT");
        if (mode.equals("ORDERED")) return Bytes.copyOf(ByteBuffer.allocate(config.keyLength()).putLong(Long.divideUnsigned(-1L, config.keyCount()) * index).array());
        if (mode.equals("TAGGED")) {
            byte[] name = ("{t" + (index % number("integration.tag.count", Math.min(8, config.keyCount()), 1, config.keyCount())) + "}:" + index).getBytes(StandardCharsets.UTF_8);
            if (name.length > config.keyLength()) throw new IllegalArgumentException("tagged key length truncates identity"); return Bytes.copyOf(Arrays.copyOf(name, config.keyLength()));
        }
        return WorkloadGenerator.key(index, config.keyLength());
    }
    private Command transform(Command command) {
        Bytes k = keyTransform.get(key(command)); if (k == null) throw new IllegalStateException("generator key not in prefilled universe");
        return command instanceof Command.Get ? new Command.Get(k) : command instanceof Command.Set set ? new Command.Set(k, set.value()) : new Command.Delete(k);
    }
    private void prefill() throws Exception {
        Random random = new Random(config.seed() ^ 0x193ca71L);
        for (int i = 0; i < config.keyCount(); i++) {
            byte[] value = new byte[config.valueSize()]; random.nextBytes(value);
            Bytes canonical = canonicalKey(i); keyTransform.put(WorkloadGenerator.key(i, config.keyLength()), canonical);
            Command c = new Command.Set(canonical, Bytes.copyOf(value));
            int status = request("prefill", i, c, operations.incrementAndGet(), System.nanoTime(), false);
            if (status != 0) throw new IOException("prefill failed key=" + i + " status=" + status);
        }
    }
    private synchronized void record(String phase, long index, long op, Command c, long scheduled, long start, long end, int status, int attempts) throws IOException {
        raw.write(phase + "," + index + ",1," + op + "," + c.getClass().getSimpleName() + "," + hash(key(c).toByteArray()) + "," + scheduled + "," + start + "," + end + "," + status + "," + attempts + "," + Math.max(0, end - scheduled) + "\n");
    }
    private int request(String phase, long index, Command c, long op, long scheduled, boolean measured) throws IOException, InterruptedException {
        Bytes key = key(c); ReentrantLock lock = locks[(key.hashCode() & Integer.MAX_VALUE) % locks.length];
        long deadline = scheduled + TimeUnit.MILLISECONDS.toNanos(config.requestTimeoutMs());
        long start = System.nanoTime(); int attempts = 0, status = 7; byte[] value = null;
        try {
            if (lock.tryLock(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                try {
                    while (System.nanoTime() < deadline) {
                        WireClient client = borrow(); boolean healthy = false;
                        try {
                            var r = client.send(c, 1, op, 0, Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
                            healthy = true; attempts++; status = r.status(); value = r.value();
                        } catch (IOException e) { attempts++; status = -2; break; }
                        finally { if (healthy) pool.offer(client); else { client.close(); connections.remove(client); } }
                        if (status != 6) break;
                        LockSupport.parkNanos(1_000_000);
                    }
                    if (c instanceof Command.Set set && status == 0) expected.put(key, new Expected(hash(set.value().toByteArray()), false));
                    else if (c instanceof Command.Delete && (status == 0 || status == 1)) expected.put(key, new Expected("", true));
                    else if (!(c instanceof Command.Get) && status != 2 && status != 6 && status != 10) uncertain.add(key);
                    if (c instanceof Command.Get && !uncertain.contains(key)) {
                        Expected wanted = expected.get(key);
                        if (wanted != null && (status == 0 || status == 1) && (wanted.missing() ? status != 1 : status != 0 || !wanted.hash().equals(hash(value)))) violations.incrementAndGet();
                    }
                } finally { lock.unlock(); }
            }
        } finally {
            long end = System.nanoTime();
            if (measured) {
                terminalLatency.add(Math.max(0, end - scheduled));
                if (status == 0 || status == 1) { if (status == 0) success.incrementAndGet(); else miss.incrementAndGet(); successLatency.add(Math.max(0, end - scheduled)); }
                else failed.incrementAndGet();
            }
            record(phase, index, op, c, scheduled, start, end, status, attempts);
        }
        return status;
    }
    private void updateTape(Command c) {
        tape.update((byte) (c instanceof Command.Get ? 1 : c instanceof Command.Set ? 2 : 3));
        byte[] key = key(c).toByteArray(); tape.update(ByteBuffer.allocate(4).putInt(key.length).array()); tape.update(key);
        if (c instanceof Command.Set s) { byte[] value = s.value().toByteArray(); tape.update(ByteBuffer.allocate(4).putInt(value.length).array()); tape.update(value); }
    }
    private void phase(String phase, long count, boolean measured) throws Exception {
        CountDownLatch done = new CountDownLatch((int) count); Semaphore capacity = new Semaphore(config.concurrency());
        List<Future<?>> interventions = new ArrayList<>();
        Random random = new Random(config.seed()); KeySpace space = KeySpace.of(config);
        WorkloadGenerator generator = new WorkloadGenerator(config, space, random, 0);
        long epoch = System.nanoTime(); if (measured) { measurementStart = epoch; Files.writeString(output.resolve("MEASURE_STARTED"), Instant.now().toString(), StandardOpenOption.CREATE_NEW); }
        long windowRequests = number("integration.window.requests", config.measuredRequests(), 1, config.measuredRequests());
        boolean trackCrash = measured && config.scenario().equals("CRASH_LEADER");
        for (long i = 0; i < count; i++) {
            if (i > 0 && i % windowRequests == 0) generator = new WorkloadGenerator(config, space, random, (int) (i / windowRequests));
            Command command = transform(generator.next()); long op = operations.incrementAndGet(); long index = i;
            if (measured) updateTape(command);
            long planned = epoch + (measured && i >= rateChangeAt
                    ? rateChangeAt * 1_000_000_000L / rate + (i - rateChangeAt) * 1_000_000_000L / rateAfter
                    : i * 1_000_000_000L / rate);
            while (System.nanoTime() < planned) LockSupport.parkNanos(Math.min(1_000_000, planned - System.nanoTime()));
            if (measured && i == rateChangeAt) {
                var before = control(STATUS, new byte[0]);
                if (before.status() != 0) throw new IOException("rate transition status failed");
                Files.write(output.resolve("before-rate-change.json"), before.value(), StandardOpenOption.CREATE_NEW);
            }
            if (trackCrash && (i & 15) == 0 && Files.exists(output.resolve("FAULT_CONFIRMED"))) {
                Files.writeString(output.resolve("first-post-crash-index"), Long.toString(i), StandardOpenOption.CREATE_NEW);
                trackCrash = false;
            }
            if (measured && i == number("integration.balance.at", -1, -1, config.measuredRequests() - 1)) {
                long tablet = number("integration.move.tablet", -1, -1, Long.MAX_VALUE);
                interventions.add(actions.submit(() -> {
                    var result = tablet < 0 ? control(BALANCE, new byte[0]) : control(MOVE, ByteBuffer.allocate(12).putLong(tablet).putInt((int) number("integration.move.to", 1, 0, 1)).array());
                    if (result.status() != 0) throw new IOException("balance/move admission failed: " + result.status());
                    Files.write(output.resolve("move-admission.json"), result.value(), StandardOpenOption.CREATE_NEW);
                    return null;
                }));
            }
            if (measured && scaleSchedule.containsKey(i)) {
                int target = scaleSchedule.get(i); long offeredIndex = i;
                interventions.add(actions.submit(() -> {
                    migrationDrain(); var result = control(SCALE, ByteBuffer.allocate(4).putInt(target).array());
                    if (result.status() != 0) throw new IOException("scale admission failed: " + result.status());
                    Files.write(output.resolve("scale-" + offeredIndex + "-admission.json"), result.value(), StandardOpenOption.CREATE_NEW); return null;
                }));
            }
            if (!capacity.tryAcquire()) {
                if (measured) dropped.incrementAndGet();
                record(phase, i, op, command, planned, System.nanoTime(), System.nanoTime(), -3, 0); done.countDown(); continue;
            }
            workers.execute(() -> {
                try { request(phase, index, command, op, planned, measured); }
                catch (Exception e) { asyncError.compareAndSet(null, e); if (e instanceof InterruptedException) Thread.currentThread().interrupt(); }
                finally { capacity.release(); done.countDown(); }
            });
        }
        if (!done.await(number("integration.drain.ms", 300000, 1000, 3600000), TimeUnit.MILLISECONDS)) throw new IOException("client drain timeout");
        if (measured) measurementEnd = System.nanoTime();
        for (Future<?> intervention : interventions) intervention.get(number("integration.drain.ms", 300000, 1000, 3600000), TimeUnit.MILLISECONDS);
        if (asyncError.get() != null) throw new IOException("request writer/worker failed", asyncError.get());
        raw.flush();
    }
    private String migrationDrain() throws Exception {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(number("integration.drain.ms", 300000, 1000, 3600000));
        while (System.nanoTime() < until) {
            var r = control(STATUS, new byte[0]); if (r.status() != 0) throw new IOException("gateway status failed");
            String text = new String(r.value(), StandardCharsets.UTF_8);
            if (text.contains("\"pendingJobs\":0") && text.contains("\"pendingRepairs\":0")) return text;
            Thread.sleep(50);
        }
        throw new IOException("migration drain timeout");
    }
    private long audit() throws Exception {
        long errors = 0, n = number("integration.audit.keys", Math.min(1000, config.keyCount()), 1, config.keyCount());
        try (WireClient client = new WireClient(config.targetHost(), config.targetPort(), config.requestTimeoutMs() + 1000);
             BufferedWriter evidence = Files.newBufferedWriter(output.resolve("audit.csv"))) {
            evidence.write("key_index,status,expected_sha256,actual_sha256,uncertain,match\n");
            for (int i = 0; i < n; i++) {
                int index = (int) ((long) i * config.keyCount() / n); Bytes key = canonicalKey(index); Expected wanted = expected.get(key);
                var r = client.send(new Command.Get(key), 1, operations.incrementAndGet(), 0, config.requestTimeoutMs());
                String actual = r.status() == 0 ? hash(r.value()) : "";
                boolean unknown = uncertain.contains(key);
                boolean match = wanted != null && (wanted.missing() ? r.status() == 1 : r.status() == 0 && wanted.hash().equals(actual));
                if (!unknown && !match) errors++;
                evidence.write(index + "," + r.status() + "," + (wanted == null ? "" : wanted.hash()) + "," + actual + "," + unknown + "," + match + "\n");
            }
        } return errors;
    }
    private static long percentile(List<Long> values, double q) {
        synchronized (values) { if (values.isEmpty()) return -1; var copy = new ArrayList<>(values); Collections.sort(copy); return copy.get((int) Math.ceil(copy.size() * q) - 1); }
    }
    private static String hash(byte[] data) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data)); }
        catch (NoSuchAlgorithmException e) { throw new AssertionError(e); }
    }
    private void run(Path input) throws Exception {
        ready(); prefill(); phase("warmup", config.warmupRequests(), false);
        migrationDrain(); if (control(RESET, new byte[0]).status() != 0) throw new IOException("measurement reset failed");
        phase("measure", config.measuredRequests(), true);
        if (control(STOP_CONTROL, new byte[0]).status() != 0) throw new IOException("stop automatic control failed");
        String gateway = migrationDrain(); long auditErrors = audit();
        Files.writeString(output.resolve("gateway-final.json"), gateway, StandardOpenOption.CREATE_NEW);
        long complete = success.get() + miss.get() + failed.get() + dropped.get();
        if (complete != config.measuredRequests()) throw new IOException("terminal operation accounting differs: " + complete);
        boolean migrationValid = !gateway.contains("\"state\":\"ABORTED\"") && !gateway.contains("\"state\":\"HELD\"") && !gateway.contains("FAILED_AFTER_PUBLICATION") && !gateway.contains("\"controlState\":\"SAFE_HOLD\"") && !gateway.contains("\"state\":\"FAILED\"") && gateway.contains("\"unresolvedMigrations\":0");
        var unknown = java.util.regex.Pattern.compile("\\\"uncertainKeys\\\":([0-9]+)").matcher(gateway); if (!unknown.find()) throw new IOException("gateway uncertainty field missing");
        long gatewayUncertain = Long.parseLong(unknown.group(1));
        var decisions = java.util.regex.Pattern.compile("\\\"scalingDecisions\\\":([0-9]+)").matcher(gateway); if (!decisions.find()) throw new IOException("scaler decision field missing");
        boolean coverageValid = !Boolean.parseBoolean(extra.getProperty("integration.require.scaler.decision", "false")) || Long.parseLong(decisions.group(1)) > 0;
        boolean joint = gateway.contains("\"jointCache\":true");
        boolean jointValid = joint && gateway.contains("\"routingInitialized\":true") && gateway.contains("\"routingUncertain\":false");
        for (String field : java.util.List.of("authorityAdmissions", "dataCalls", "routingPublications")) {
            var counts = java.util.regex.Pattern.compile("\\\"" + field + "\\\":([0-9]+)").matcher(gateway); long sum = 0;
            while (counts.find()) sum += Long.parseLong(counts.group(1));
            jointValid &= sum > 0;
        }
        coverageValid &= !Boolean.parseBoolean(extra.getProperty("integration.require.joint", "false")) || jointValid;
        boolean correctness = violations.get() == 0 && auditErrors == 0 && uncertain.isEmpty() && gatewayUncertain == 0;
        String result = "{\"schema\":1,\"origin\":\"integrated-real-tcp-file-kv\",\"runId\":\"" + extra.getProperty("integration.run.id") + "\",\"seed\":" + config.seed()
                + ",\"offered\":" + config.measuredRequests() + ",\"ok\":" + success.get() + ",\"miss\":" + miss.get() + ",\"failed\":" + failed.get() + ",\"notAdmitted\":" + dropped.get()
                + ",\"durationNanosIncludingClientDrain\":" + (measurementEnd - measurementStart) + ",\"configuredRate\":" + rate
                + ",\"terminalGoodputIncludingDrain\":" + ((success.get() + miss.get()) * 1e9 / (measurementEnd - measurementStart))
                + ",\"successP99FromScheduleNanos\":" + percentile(successLatency, .99) + ",\"terminalP99FromScheduleNanos\":" + percentile(terminalLatency, .99)
                + ",\"staleReads\":" + violations.get() + ",\"auditErrors\":" + auditErrors + ",\"uncertainKeys\":" + uncertain.size()
                + ",\"jointCache\":" + joint + ",\"jointComponentsValid\":" + jointValid
                + ",\"coverageValid\":" + coverageValid + ",\"gatewayUncertainKeys\":" + gatewayUncertain + ",\"correctnessValid\":" + correctness + ",\"migrationValid\":" + migrationValid + ",\"requestTapeSha256\":\"" + HexFormat.of().formatHex(tape.digest())
                + "\",\"configSha256\":\"" + hash(Files.readAllBytes(input)) + "\",\"java\":\"" + System.getProperty("java.version") + "\"}\n";
        Files.writeString(output.resolve("result.json"), result, StandardOpenOption.CREATE_NEW);
        Files.writeString(output.resolve("COMPLETE"), "collected; see correctnessValid/migrationValid\n", StandardOpenOption.CREATE_NEW);
    }
    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--validate")) { new IntegrationLoadMain(Path.of(args[1]), null); System.out.println("LOAD_CONFIG_VALID"); return; }
        if (args.length != 2) throw new IllegalArgumentException("usage: IntegrationLoadMain workload.properties new-output-dir");
        IntegrationLoadMain run = new IntegrationLoadMain(Path.of(args[0]), Path.of(args[1]));
        try { run.run(Path.of(args[0])); }
        catch (Exception e) { Files.writeString(run.output.resolve("FAILED"), e.getClass().getName() + ": " + e.getMessage()); throw e; }
        finally { run.actions.shutdownNow(); run.workers.shutdownNow(); run.connections.forEach(WireClient::close); run.raw.close(); }
    }
}
