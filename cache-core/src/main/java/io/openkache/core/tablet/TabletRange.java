package io.openkache.core.tablet;

import java.util.ArrayList;
import java.util.List;

/**
 * 해시 공간의 반열린 구간 {@code [start, end)}. 값은 부호 없는 64비트로 해석한다.
 *
 * <p>{@code end == 0}은 2^64(공간의 끝)를 뜻한다. 공간 전체는 {@link #FULL}이다. 끝을 미포함으로 두면 이웃 구간과 겹치지도
 * 빈틈이 생기지도 않고({@code a.end == b.start}), split은 경계값 하나로 나뉜다.
 *
 * <p>불변이다.
 *
 * @param start 포함하는 시작점
 * @param end 포함하지 않는 끝점. 0이면 2^64
 */
public record TabletRange(long start, long end) {

    /** 해시 공간 전체. */
    public static final TabletRange FULL = new TabletRange(0, 0);

    public TabletRange {
        if (end != 0 && Long.compareUnsigned(start, end) >= 0) {
            throw new IllegalArgumentException("empty or reversed range: " + describe(start, end));
        }
    }

    /** {@code at}에서 나눈 두 조각. */
    public record Split(TabletRange left, TabletRange right) {}

    /** {@code hash}가 이 구간 안에 있으면 {@code true}. */
    public boolean contains(long hash) {
        boolean afterStart = Long.compareUnsigned(hash, start) >= 0;
        boolean beforeEnd = end == 0 || Long.compareUnsigned(hash, end) < 0;
        return afterStart && beforeEnd;
    }

    /** {@code at}에서 둘로 나눈다: {@code [start, at)}, {@code [at, end)}. {@code at}은 구간 안이고 start와 달라야 한다. */
    public Split splitAt(long at) {
        if (!contains(at) || at == start) {
            throw new IllegalArgumentException(
                    "split point must be inside " + this + " and not at start: " + Long.toUnsignedString(at));
        }
        return new Split(new TabletRange(start, at), new TabletRange(at, end));
    }

    /** 이 구간 바로 뒤에 {@code next}가 붙어 있으면 둘을 합친 구간. */
    public TabletRange mergeWith(TabletRange next) {
        if (end == 0 || end != next.start) {
            throw new IllegalArgumentException("ranges are not adjacent: " + this + ", " + next);
        }
        return new TabletRange(start, next.end);
    }

    /**
     * 공간을 {@code n}개로 똑같이 나눈 구간들. 시작점 오름차순이며 전체를 빈틈없이 덮는다.
     *
     * <p>간격은 {@code 2^64 / n}을 부호 없는 나눗셈으로 구한다. 2^63 근처에서 double 계산은 정밀도가 모자라므로 쓰지 않는다.
     */
    public static List<TabletRange> uniform(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be >= 1: " + n);
        }
        long step = Long.divideUnsigned(-1L, n);
        List<TabletRange> ranges = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long s = i * step;
            long e = i == n - 1 ? 0 : (i + 1) * step;
            ranges.add(new TabletRange(s, e));
        }
        return List.copyOf(ranges);
    }

    @Override
    public String toString() {
        return describe(start, end);
    }

    private static String describe(long start, long end) {
        return "[" + Long.toUnsignedString(start) + ", " + (end == 0 ? "2^64" : Long.toUnsignedString(end)) + ")";
    }
}
