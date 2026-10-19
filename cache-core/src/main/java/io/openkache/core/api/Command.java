package io.openkache.core.api;

import io.openkache.core.model.Bytes;
import java.util.Objects;

/**
 * 클라이언트가 요청하는 캐시 연산.
 *
 * <p>sealed 계층이므로 {@code switch}에서 모든 연산을 빠짐없이 처리했는지 컴파일러가 검사한다. wire frame 타입과는 별개이며,
 * server가 frame을 해석해 이 타입으로 바꿔 core에 넘긴다.
 */
public sealed interface Command {

    /** 연산 대상 Key. */
    Bytes key();

    /** Key의 값을 조회한다. 없으면 {@link ResultStatus#MISS}. */
    record Get(Bytes key) implements Command {
        public Get {
            Objects.requireNonNull(key, "key");
        }
    }

    /** Key에 값을 저장한다. 같은 Key가 있으면 덮어쓴다. */
    record Set(Bytes key, Bytes value) implements Command {
        public Set {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }
    }

    /** Key를 삭제한다. 없으면 {@link ResultStatus#MISS}. */
    record Delete(Bytes key) implements Command {
        public Delete {
            Objects.requireNonNull(key, "key");
        }
    }
}
