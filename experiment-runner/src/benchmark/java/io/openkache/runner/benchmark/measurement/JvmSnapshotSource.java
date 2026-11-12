package io.openkache.runner.benchmark.measurement;

import java.lang.management.*;
import java.util.*;
import java.util.function.LongSupplier;

/** 동일 JVM의 management API 관찰. 구현은 비차단이어야 한다. 외부 process/장치 수집기가 아니다. */
@FunctionalInterface
public interface JvmSnapshotSource {
    record Value(Long value, String unit, String status, String reason) {
        public Value {
            Objects.requireNonNull(unit); Objects.requireNonNull(status);
            if (!(status.equals("measured") && value != null && value >= 0 && reason == null
                    || status.equals("unsupported") && value == null && reason != null && !reason.isBlank())) {
                throw new IllegalArgumentException("invalid observation value");
            }
        }
        public static Value measured(long value, String unit) { return new Value(value, unit, "measured", null); }
        public static Value unsupported(String unit, String reason) { return new Value(null, unit, "unsupported", reason); }
    }
    record Snapshot(String processInstanceId, long pid, long startedNanos, long endedNanos,
            Map<String, Value> values) {
        public Snapshot {
            if (Objects.requireNonNull(processInstanceId).isBlank() || pid < 0 || endedNanos-startedNanos < 0) {
                throw new IllegalArgumentException("snapshot identity/clock");
            }
            values = Map.copyOf(values);
        }
    }
    Snapshot sample();

    /** 이 JVM의 모든 source instance가 identity를 공유해 replica별 중복 합산을 막는다. */
    final class Identity {
        private Identity() {}
        static final String ID = UUID.randomUUID().toString();
        static final long PID = ProcessHandle.current().pid();
    }
    static JvmSnapshotSource system() {
        return () -> {
            long start = System.nanoTime(); Map<String, Value> values = new LinkedHashMap<>();
            var os = ManagementFactory.getOperatingSystemMXBean();
            values.put("process_cpu_ns", os instanceof com.sun.management.OperatingSystemMXBean process
                    ? read(process::getProcessCpuTime, "ns") : Value.unsupported("ns", "process_cpu_api_unavailable"));
            try {
                var memory = ManagementFactory.getMemoryMXBean();
                memory(values, "heap", memory.getHeapMemoryUsage());
                memory(values, "non_heap", memory.getNonHeapMemoryUsage());
            } catch (RuntimeException error) {
                for (String area : List.of("heap", "non_heap")) {
                    for (String field : List.of("used", "committed", "max")) {
                        values.put(area+"_"+field+"_bytes", Value.unsupported("bytes", "memory_api_"+error.getClass().getSimpleName()));
                    }
                }
            }
            values.put("gc_count", read(() -> gc(false), "count"));
            values.put("gc_time_ns", read(() -> Math.multiplyExact(gc(true), 1_000_000L), "ns"));
            values.put("rss_bytes", Value.unsupported("bytes", "no_process_rss_source"));
            values.put("allocation_bytes", Value.unsupported("bytes", "allocation_not_collected"));
            values.put("gc_pause_ns", Value.unsupported("ns", "gc_cumulative_time_is_not_pause_distribution"));
            return new Snapshot(Identity.ID, Identity.PID, start, System.nanoTime(), values);
        };
    }
    private static Value read(LongSupplier supplier, String unit) {
        try {
            long value = supplier.getAsLong();
            return value < 0 ? Value.unsupported(unit, "negative_or_unavailable_management_value") : Value.measured(value, unit);
        } catch (RuntimeException error) { return Value.unsupported(unit, "management_api_"+error.getClass().getSimpleName()); }
    }
    private static void memory(Map<String,Value> values, String name, MemoryUsage usage) {
        values.put(name+"_used_bytes", read(usage::getUsed, "bytes"));
        values.put(name+"_committed_bytes", read(usage::getCommitted, "bytes"));
        values.put(name+"_max_bytes", read(usage::getMax, "bytes"));
    }
    private static long gc(boolean time) {
        var beans = ManagementFactory.getGarbageCollectorMXBeans();
        if (beans.isEmpty()) { throw new IllegalStateException("no GC beans"); }
        long sum = 0;
        for (var bean : beans) {
            long value = time ? bean.getCollectionTime() : bean.getCollectionCount();
            if (value < 0) { throw new IllegalStateException("partial GC counter unsupported"); }
            sum = Math.addExact(sum, value);
        }
        return sum;
    }
}
