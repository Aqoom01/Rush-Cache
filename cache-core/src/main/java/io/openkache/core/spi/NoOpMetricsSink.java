package io.openkache.core.spi;

enum NoOpMetricsSink implements MetricsSink {
    INSTANCE;

    @Override
    public void increment(String name, long delta) {}

    @Override
    public void recordNanos(String name, long nanos) {}

    @Override
    public void gauge(String name, double value) {}
}
