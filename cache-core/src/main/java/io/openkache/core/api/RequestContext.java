package io.openkache.core.api;

import io.openkache.core.model.ClientId;
import io.openkache.core.model.OperationId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import java.util.Objects;

/**
 * 요청 하나의 부가 정보. 불변이다.
 *
 * <p>연결 안에서 응답을 짝짓는 wire의 {@code requestId}는 server만 알고 여기에 넣지 않는다. 같은 논리 연산의 재시도를 식별하려면
 * {@code clientId}와 {@code operationId} 쌍을 쓴다.
 *
 * @param clientId 요청한 클라이언트
 * @param operationId 논리 연산 식별자. 재시도해도 같다
 * @param routingVersion 클라이언트가 알고 있는 라우팅 버전
 * @param ownershipEpoch 클라이언트가 알고 있는 소유권 epoch
 * @param deadline 이 JVM 기준 처리 마감 시각
 */
public record RequestContext(
        ClientId clientId,
        OperationId operationId,
        RoutingVersion routingVersion,
        OwnershipEpoch ownershipEpoch,
        Deadline deadline) {

    public RequestContext {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(routingVersion, "routingVersion");
        Objects.requireNonNull(ownershipEpoch, "ownershipEpoch");
        Objects.requireNonNull(deadline, "deadline");
    }
}
