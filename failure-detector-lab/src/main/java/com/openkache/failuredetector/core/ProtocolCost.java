package com.openkache.failuredetector.core;

public final class ProtocolCost {
    private long messages;
    private long bytes;

    public void add(long messageCount, long byteCount) {
        if (messageCount < 0 || byteCount < 0) {
            throw new IllegalArgumentException("Protocol costs cannot be negative");
        }
        messages += messageCount;
        bytes += byteCount;
    }

    public long messages() {
        return messages;
    }

    public long bytes() {
        return bytes;
    }
}
