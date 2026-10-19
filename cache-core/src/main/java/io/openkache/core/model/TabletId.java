package io.openkache.core.model;

/**
 * Tablet(Key Range 단위의 데이터 분할)을 식별한다.
 *
 * <p>값은 0 이상이다. Tablet의 Key Range, Owner 등 메타데이터 구조는 B1·B2·B3 담당자가 합의한 뒤 정의한다.
 */
public record TabletId(long value) {

    public TabletId {
        Ids.requireNonNegative("TabletId", value);
    }
}
