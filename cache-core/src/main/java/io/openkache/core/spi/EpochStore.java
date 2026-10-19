package io.openkache.core.spi;

import io.openkache.core.model.TabletId;
import java.util.Optional;

/**
 * epoch, vote 등 장애 후에도 유지되어야 하는 안전성 메타데이터의 저장 계약.
 *
 * <p>이번 단계에서는 계약만 정의하며 영속 구현은 없다. 구현 규칙:
 *
 * <ul>
 *   <li>{@link #save}는 데이터가 내구성 있게 기록된 뒤에 반환한다. 반환 후 프로세스가 죽어도 {@link #load}가 저장한 값을
 *       돌려줘야 한다.
 *   <li>저장된 값보다 작은 epoch를 저장하려 하면 {@link IllegalArgumentException}을 던진다.
 *   <li>disk I/O로 block될 수 있으므로 Netty EventLoop에서 호출하지 않는다.
 * </ul>
 */
public interface EpochStore {

    /** {@code tablet}에 저장된 값. 저장된 적이 없으면 empty. */
    Optional<EpochRecord> load(TabletId tablet);

    /** {@code tablet}의 값을 {@code record}로 교체한다. */
    void save(TabletId tablet, EpochRecord record);
}
