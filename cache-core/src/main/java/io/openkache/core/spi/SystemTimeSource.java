package io.openkache.core.spi;

enum SystemTimeSource implements TimeSource {
    INSTANCE;

    @Override
    public long monotonicNanos() {
        return System.nanoTime();
    }

    @Override
    public long epochMillis() {
        return System.currentTimeMillis();
    }
}
