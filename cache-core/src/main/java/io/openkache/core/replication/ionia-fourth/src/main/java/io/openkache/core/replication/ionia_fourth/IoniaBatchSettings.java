package io.openkache.core.replication.ionia_fourth;

import io.openkache.core.replication.ionia.IoniaConfig;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** V03 불변 기능 정책. delay는 합의/적용 완료 시한이나 성능 최적값이 아니다. */
public record IoniaBatchSettings(int maxEntries, int maxEncodedBytes, long maxDelayNanos) {
    public static final String ENTRIES = "prepare.max.entries", BYTES = "prepare.max.encoded.bytes", DELAY = "prepare.max.delay.ms";
    public static final long RETRY_NANOS = 20_000_000L;
    public static final int MAX_PACKET_BYTES = 67_108_868;
    public IoniaBatchSettings {
        if (maxEntries < 1 || maxEntries > 1_000_000 || maxEncodedBytes < 1 || maxEncodedBytes > MAX_PACKET_BYTES
                || maxDelayNanos < 0 || maxDelayNanos > 60_000_000_000L
                || maxDelayNanos != 0 && (maxDelayNanos < RETRY_NANOS || maxDelayNanos % 1_000_000 != 0))
            throw new IllegalArgumentException("invalid V03 entries/bytes/delay");
    }
    /** 시작할 때 필수 설정 3개를 엄격하게 검증한다. */
    public static IoniaBatchSettings parse(Properties properties) {
        Objects.requireNonNull(properties);
        return new IoniaBatchSettings((int) number(properties, ENTRIES, 1_000_000),
                (int) number(properties, BYTES, MAX_PACKET_BYTES), number(properties, DELAY, 60_000) * 1_000_000L);
    }
    private static long number(Properties p, String key, long limit) {
        String value = p.getProperty(key);
        if (value == null || !value.matches("[0-9]+")) throw new IllegalArgumentException("missing/invalid " + key);
        try { long n = Long.parseLong(value); if (n > limit) throw new IllegalArgumentException("out of range " + key); return n; }
        catch (NumberFormatException error) { throw new IllegalArgumentException("out of range " + key, error); }
    }
    /** 모든 thread에서 동일한 정책의 SHA-256 64 hex를 얻는다. */
    public String profileId() {
        String policy = "v03|maxEntries=" + maxEntries + "|maxEncodedBytes=" + maxEncodedBytes
                + "|maxDelayNanos=" + maxDelayNanos + "|retryIntervalNanos=20000000|inFlightPerFollower=1|oversize=singleton|sizeConvention=wireV1IncludingLength";
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(policy.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    /** wire·stable View·client의 정책을 묶고 writeUTF 한도를 파일을 열기 전에 검사한다. */
    public String configurationId(IoniaConfig membership) {
        String value = Objects.requireNonNull(membership).configurationId() + "|algorithm=V03|profile=" + profileId();
        IoniaPrepareSizer.modifiedUtfLength(value);
        return value;
    }
}
