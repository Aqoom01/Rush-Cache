package io.openkache.core.api;

import io.openkache.core.model.Bytes;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import java.util.Objects;
import java.util.Optional;

/**
 * 요청 처리 결과. 불변이다.
 *
 * @param status 처리 상태
 * @param value 조회한 값. {@link ResultStatus#OK}일 때만 있을 수 있다
 * @param valueVersion 값의 버전. 0 이상이며 값이 없으면 0
 * @param routingVersion 응답 시점에 이 노드가 아는 라우팅 버전
 * @param ownershipEpoch 응답 시점에 이 노드가 아는 소유권 epoch
 * @param redirect 다시 요청할 노드. {@link ResultStatus#MOVED}, {@link ResultStatus#NOT_PRIMARY}일 때만 있을 수 있다
 */
public record Result(
        ResultStatus status,
        Optional<Bytes> value,
        long valueVersion,
        RoutingVersion routingVersion,
        OwnershipEpoch ownershipEpoch,
        Optional<Redirect> redirect) {

    public Result {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(routingVersion, "routingVersion");
        Objects.requireNonNull(ownershipEpoch, "ownershipEpoch");
        Objects.requireNonNull(redirect, "redirect");
        if (valueVersion < 0) {
            throw new IllegalArgumentException("valueVersion must be >= 0: " + valueVersion);
        }
        if (value.isPresent() && status != ResultStatus.OK) {
            throw new IllegalArgumentException("value is only allowed with OK: " + status);
        }
        if (redirect.isPresent()
                && status != ResultStatus.MOVED
                && status != ResultStatus.NOT_PRIMARY) {
            throw new IllegalArgumentException(
                    "redirect is only allowed with MOVED or NOT_PRIMARY: " + status);
        }
    }

    /** 값을 반환하는 성공 결과. */
    public static Result ok(
            Bytes value,
            long valueVersion,
            RoutingVersion routingVersion,
            OwnershipEpoch ownershipEpoch) {
        return new Result(
                ResultStatus.OK,
                Optional.of(value),
                valueVersion,
                routingVersion,
                ownershipEpoch,
                Optional.empty());
    }

    /** 값과 redirect가 없는 결과 (값 없는 OK, MISS, 각종 실패). */
    public static Result of(
            ResultStatus status, RoutingVersion routingVersion, OwnershipEpoch ownershipEpoch) {
        return new Result(
                status, Optional.empty(), 0, routingVersion, ownershipEpoch, Optional.empty());
    }

    /** 다른 노드로 다시 요청하라는 결과. */
    public static Result redirect(
            ResultStatus status,
            Redirect redirect,
            RoutingVersion routingVersion,
            OwnershipEpoch ownershipEpoch) {
        return new Result(
                status, Optional.empty(), 0, routingVersion, ownershipEpoch, Optional.of(redirect));
    }
}
