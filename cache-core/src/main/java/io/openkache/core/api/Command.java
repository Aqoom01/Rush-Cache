package io.openkache.core.api;

import io.openkache.core.model.Bytes;
import java.util.Objects;

/**
 * 클라이언트가 요청하는 캐시 연산.
 *
 * <p>sealed 계층이므로 {@code switch}에서 모든 연산을 빠짐없이 처리했는지 컴파일러가 검사한다. wire frame 타입과는 별개이며,
 * server가 frame을 해석해 이 타입으로 바꿔 core에 넘긴다.
 *
 * <p>{@link Count}를 제외한 연산은 대상 Key 하나를 가진다. {@link Count}는 Key가 없으므로 이 인터페이스에 공통 {@code key()}를
 * 두지 않는다. Key가 필요하면 {@code switch}로 연산별 record에서 꺼낸다.
 */
public sealed interface Command {

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

    /**
     * Key가 있는지 확인한다. 있으면 값 없이 {@link ResultStatus#OK}와 그 값의 {@code valueVersion}을, 없으면
     * {@link ResultStatus#MISS}를 돌려준다. 값을 복사하지 않으므로 존재 여부만 필요할 때 {@link Get} 대신 쓴다.
     */
    record Exists(Bytes key) implements Command {
        public Exists {
            Objects.requireNonNull(key, "key");
        }
    }

    /**
     * 이 노드에 저장된 Key 개수를 센다. 결과는 {@link ResultStatus#OK}이며 개수는 {@link Result#value()}에 8-byte big-endian
     * {@code long}으로 들어간다({@code valueVersion}은 0). Key가 없어도 MISS가 아니라 개수 0인 OK다.
     *
     * <p>개수는 노드 전체 기준이다. 동시에 처리 중인 {@link Set}·{@link Delete}가 반영되는지는 구현이 정의한다.
     */
    record Count() implements Command {}
}
