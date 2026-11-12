package io.openkache.runner.benchmark.measurement;

/** Versioned experimental sidecars share the existing strict JDK encoder. */
public final class ObservationJson {
    private ObservationJson() {}
    public static String encode(Object value) { return BenchmarkJson.encode(value); }
}
