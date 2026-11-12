package io.openkache.core.balance.split;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.tablet.TabletRange;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntToLongFunction;

/**
 * 분할 정책 테스트용 Tablet 관찰 결과({@link TabletLoad})를 만든다.
 *
 * <p>key는 해시값으로만 다룬다. key {@code n}개를 해시 구간에 고르게 놓는다: 구간을 {@code n}칸으로 나누고 {@code i}번
 * key를 {@code i}번 칸의 가운데에 둔다. 그래서 {@code i}가 작을수록 해시값도 작고, 전체 공간의 산술적 가운데(2^63)는
 * {@code n / 2 - 1}번과 {@code n / 2}번 key 사이에 온다(n이 짝수일 때).
 *
 * <p>요청은 모두 읽기로 넣는다. 분할 정책은 읽기와 쓰기를 합친 요청 수를 본다.
 */
final class Loads {

    private Loads() {}

    /** 해시 공간 전체에 key {@code n}개를 고르게 놓았을 때 {@code i}번 key의 해시값. */
    static long key(int i, int n) {
        return keyIn(TabletRange.FULL, i, n);
    }

    /** 해시 공간 전체에 key {@code n}개가 있고, {@code i}번 key의 요청 수가 {@code requestsOfKey(i)}인 Tablet. */
    static TabletLoad tablet(int n, IntToLongFunction requestsOfKey) {
        return tablet(TabletRange.FULL, n, requestsOfKey);
    }

    /** {@code range} 안에 key {@code n}개를 고르게 놓은 Tablet. */
    static TabletLoad tablet(TabletRange range, int n, IntToLongFunction requestsOfKey) {
        List<KeyLoad> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(new KeyLoad(keyIn(range, i, n), requestsOfKey.applyAsLong(i), 0));
        }
        return new TabletLoad(range, 0, keys);
    }

    /** 모든 key가 {@code requestsPerKey}건씩 받은 Tablet. */
    static TabletLoad uniform(int n, long requestsPerKey) {
        return tablet(n, i -> requestsPerKey);
    }

    /**
     * key 1,000개 중 해시값이 작은 쪽 200개(20%)가 전체 요청의 80%를 받은 Tablet.
     *
     * <p>앞쪽 key는 16건씩(합 3,200건), 나머지 800개는 1건씩(합 800건)이다. 전체 4,000건 중 3,200건 = 80%.
     */
    static TabletLoad hotRange() {
        return tablet(1000, i -> i < 200 ? 16 : 1);
    }

    /** key 1,000개 중 가운데 key 하나({@code 500}번)에만 요청 {@code requests}건이 몰리고 나머지는 0건인 Tablet. */
    static TabletLoad singleHotKey(long requests) {
        return tablet(1000, i -> i == 500 ? requests : 0);
    }

    private static long keyIn(TabletRange range, int i, int n) {
        long width = range.equals(TabletRange.FULL) ? -1L : range.end() - range.start();
        long step = Long.divideUnsigned(width, n);
        return range.start() + i * step + step / 2;
    }
}
