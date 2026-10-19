package io.openkache.core.model;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/**
 * 불변 바이트열. Key와 Value를 표현한다.
 *
 * <p>생성할 때와 {@link #toByteArray()}로 꺼낼 때 모두 배열을 복사하므로, 호출자가 원본이나 반환값을 수정해도
 * 이 객체의 내용은 바뀌지 않는다. {@link #equals(Object)}와 {@link #hashCode()}는 배열 참조가 아닌 내용으로
 * 비교한다.
 *
 * <p>불변 객체이므로 여러 thread에서 동기화 없이 공유할 수 있다.
 */
public final class Bytes {

    private static final Bytes EMPTY = new Bytes(new byte[0]);
    private static final int TO_STRING_MAX_BYTES = 32;

    private final byte[] data;
    private final int hash;

    private Bytes(byte[] ownedData) {
        this.data = ownedData;
        this.hash = Arrays.hashCode(ownedData);
    }

    /** {@code source}를 복사해 만든다. */
    public static Bytes copyOf(byte[] source) {
        Objects.requireNonNull(source, "source");
        return source.length == 0 ? EMPTY : new Bytes(source.clone());
    }

    /** 문자열의 UTF-8 인코딩으로 만든다. */
    public static Bytes utf8(String text) {
        Objects.requireNonNull(text, "text");
        return copyOf(text.getBytes(StandardCharsets.UTF_8));
    }

    /** 길이 0인 바이트열. */
    public static Bytes empty() {
        return EMPTY;
    }

    /** 바이트 수. */
    public int size() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    /** {@code index} 위치의 바이트. */
    public byte get(int index) {
        return data[index];
    }

    /** 내용을 복사한 새 배열을 반환한다. */
    public byte[] toByteArray() {
        return data.clone();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other instanceof Bytes that && Arrays.equals(data, that.data));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    /** 앞부분 최대 32바이트를 16진수로 표시한다. 로그 용도이며 형식은 바뀔 수 있다. */
    @Override
    public String toString() {
        int shown = Math.min(data.length, TO_STRING_MAX_BYTES);
        String hex = HexFormat.of().formatHex(data, 0, shown);
        String suffix = data.length > shown ? "..." : "";
        return "Bytes[size=" + data.length + ", hex=" + hex + suffix + "]";
    }
}
