package io.openkache.core.replication.ionia_eighth;

import java.util.*;
import java.nio.charset.StandardCharsets;
import java.security.*;

/** Client-local batching policy. Values are experiment settings, not optimal parameters. */
public record IoniaMetaBatchSettings(int maxEntries, int maxBytes, long maxDelayNanos, int pendingLimit, int inFlightLimit) {
    public static final int WIRE_ENTRIES = 64, WIRE_REQUEST_BYTES = 1024 * 1024;
    public static final String PROTOCOL = "ionia-meta-batch-v1";
    public static final Set<String> KEYS = Set.of("meta.batch.entries", "meta.batch.bytes", "meta.batch.delay.ms", "meta.pending.limit", "meta.inflight.limit");
    public IoniaMetaBatchSettings {
        if (maxEntries < 1 || maxEntries > WIRE_ENTRIES || maxBytes < 65 || maxBytes > WIRE_REQUEST_BYTES
                || maxDelayNanos < 0 || maxDelayNanos > 1_000_000_000L || pendingLimit < maxEntries || pendingLimit > 65536
                || inFlightLimit < 1 || inFlightLimit > 256) throw new IllegalArgumentException("invalid Meta batch limits");
    }
    public static IoniaMetaBatchSettings parse(Properties p) {
        for (String key : KEYS) if (p.getProperty(key) == null) throw new IllegalArgumentException("missing setting: " + key);
        return new IoniaMetaBatchSettings(Integer.parseInt(p.getProperty("meta.batch.entries")), Integer.parseInt(p.getProperty("meta.batch.bytes")),
                Math.multiplyExact(Long.parseLong(p.getProperty("meta.batch.delay.ms")), 1_000_000L),
                Integer.parseInt(p.getProperty("meta.pending.limit")), Integer.parseInt(p.getProperty("meta.inflight.limit")));
    }
    public String profileId() {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(toString().getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    public static String configuration(String membership) { return membership + "|" + PROTOCOL; }
}
