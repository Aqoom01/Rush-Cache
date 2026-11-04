package io.openkache.core.kv;

import io.openkache.core.api.ResultStatus;
import java.util.List;
import java.util.Objects;

/**
 * {@link KvEngine#scan}·{@link KvEngine#range}의 결과. 불변이다.
 *
 * <p>{@code Command}/{@code Result}는 값 하나를 다루므로 여러 항목을 돌려주는 SCAN/RANGE는 이 타입을 쓴다. 실패는 예외가 아니라
 * {@link #status()}로 표현한다. {@link ResultStatus#OK}가 아니면 {@link #entries()}는 비어 있다.
 *
 * @param status {@link ResultStatus#OK}, {@link ResultStatus#BUSY}, {@link ResultStatus#TIMEOUT},
 *     {@link ResultStatus#UNAVAILABLE} 중 하나
 * @param entries {@link KvEntry#KEY_ORDER}로 정렬된 항목. 조건에 맞는 Key가 없으면 빈 목록이며 그것도 OK다
 * @param truncated {@code limit}에 걸려 조건에 맞는 항목을 다 담지 못했으면 {@code true}. 이어서 읽으려면 마지막 Key 다음부터 다시
 *     요청한다
 */
public record ScanResult(ResultStatus status, List<KvEntry> entries, boolean truncated) {

    public ScanResult {
        Objects.requireNonNull(status, "status");
        entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (status != ResultStatus.OK && !entries.isEmpty()) {
            throw new IllegalArgumentException("entries are only allowed with OK: " + status);
        }
    }

    /** 항목 없는 실패 결과. */
    static ScanResult of(ResultStatus status) {
        return new ScanResult(status, List.of(), false);
    }
}
