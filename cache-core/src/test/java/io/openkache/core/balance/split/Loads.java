package io.openkache.core.balance.split;

import io.openkache.core.balance.KeyLoad;
import io.openkache.core.balance.KeyRange;
import io.openkache.core.balance.TabletLoad;
import io.openkache.core.model.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntToLongFunction;

/**
 * 분할 정책 테스트용 Tablet 관찰 결과({@link TabletLoad})를 만든다.
 *
 * <p>key는 {@code key-0000}, {@code key-0001}, ... 형식이다. 숫자를 4자리로 맞췄으므로 숫자 순서와 key 순서가 같다. 예:
 * {@code key(5)}는 {@code key(4)}보다 뒤, {@code key(6)}보다 앞이다.
 *
 * <p>요청은 모두 읽기로 넣는다. 분할 정책은 읽기와 쓰기를 합친 요청 수를 보므로 테스트에서는 구분할 필요가 없다.
 */
final class Loads {

    private Loads() {}

    /** {@code index}번 key. 예: {@code key(5)} = {@code "key-0005"}. */
    static Bytes key(int index) {
        return Bytes.utf8(String.format("key-%04d", index));
    }

    /**
     * key {@code 0}부터 {@code keyCount - 1}까지 있는 Tablet. key {@code i}의 요청 수는 {@code requestsOfKey(i)}다. 범위는
     * 위아래 끝이 없다.
     */
    static TabletLoad tablet(int keyCount, IntToLongFunction requestsOfKey) {
        return tablet(KeyRange.all(), 0, keyCount, requestsOfKey);
    }

    /** {@code range} 안에 key {@code firstKey}부터 {@code keyCount}개가 있는 Tablet. */
    static TabletLoad tablet(KeyRange range, int firstKey, int keyCount, IntToLongFunction requestsOfKey) {
        List<KeyLoad> keys = new ArrayList<>(keyCount);
        for (int i = firstKey; i < firstKey + keyCount; i++) {
            keys.add(new KeyLoad(key(i), requestsOfKey.applyAsLong(i), 0));
        }
        return new TabletLoad(range, 0, keys);
    }

    /** 모든 key가 {@code requestsPerKey}건씩 받은 Tablet. */
    static TabletLoad uniform(int keyCount, long requestsPerKey) {
        return tablet(keyCount, i -> requestsPerKey);
    }

    /**
     * key 1,000개 중 앞쪽 200개(20%)가 전체 요청의 80%를 받은 Tablet.
     *
     * <p>앞쪽 key는 16건씩(합 3,200건), 나머지 800개는 1건씩(합 800건)이다. 전체 4,000건 중 3,200건 = 80%.
     */
    static TabletLoad hotRange() {
        return tablet(1000, i -> i < 200 ? 16 : 1);
    }

    /** key 1,000개 중 가운데 key 하나({@code key(500)})에만 요청 {@code requests}건이 몰리고 나머지는 0건인 Tablet. */
    static TabletLoad singleHotKey(long requests) {
        return tablet(1000, i -> i == 500 ? requests : 0);
    }
}
