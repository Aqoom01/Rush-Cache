package io.openkache.core.replication.ionia_third;

import io.openkache.core.replication.ionia.IoniaConfig;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** V02a 고정 실행 정책. N은 key 항목 목표이며 heap 상한이 아니다. 모든 thread에서 불변이다. */
public record IoniaHistorySettings(int targetEntries) {
    public static final long TRIM_INTERVAL_NANOS = 1_000_000_000L;
    public static final String PROPERTY = "history.target.entries";

    public IoniaHistorySettings {
        if (targetEntries < 0) throw new IllegalArgumentException("negative history target");
    }

    /** 누락·음수·int 범위 밖 설정을 시작 전에 거절한다. 0은 정석 prefix 정리 대조군이다. */
    public static IoniaHistorySettings parse(String value) {
        if (value == null || !value.matches("[0-9]+"))
            throw new IllegalArgumentException("history.target.entries must be a nonnegative int");
        try { return new IoniaHistorySettings(Integer.parseInt(value)); }
        catch (NumberFormatException error) { throw new IllegalArgumentException("history target exceeds int range", error); }
    }

    /** N 자릿수와 무관하게 UTF-8 정책 문자열의 SHA-256 64자리 hex를 반환한다. */
    public String profileId() {
        String policy = "v02a|targetEntries=" + targetEntries + "|trimIntervalNanos=" + TRIM_INTERVAL_NANOS;
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(policy.getBytes(StandardCharsets.UTF_8))); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    /** 서버·클라이언트·stable View가 같은 정책을 사용하도록 wire 구성에 정책을 결합한다. */
    public String configurationId(IoniaConfig membership) {
        return Objects.requireNonNull(membership).configurationId() + "|algorithm=V02a|profile=" + profileId();
    }
}
