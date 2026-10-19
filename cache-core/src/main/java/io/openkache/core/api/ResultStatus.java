package io.openkache.core.api;

/**
 * 요청 처리 결과의 상태.
 *
 * <p>예상할 수 있는 실패(MISS, 라우팅 불일치, 과부하, 시간 초과 등)는 예외가 아니라 이 상태로 표현한다. 예외는 프로그래밍 오류에만
 * 쓴다. wire 상태 코드와의 대응은 server가 정한다.
 */
public enum ResultStatus {
    /** 성공. GET이면 값이 있다. */
    OK,
    /** Key가 없다. 오류가 아닌 정상 결과다. */
    MISS,
    /** 요청 형식이나 값이 잘못되었다. 같은 요청을 재시도해도 실패한다. */
    BAD_REQUEST,
    /** Key의 소유 노드가 바뀌었다. redirect와 최신 routingVersion으로 다시 요청한다. */
    MOVED,
    /** 이 노드가 해당 Key의 Primary가 아니다. redirect가 있으면 그 노드로 다시 요청한다. */
    NOT_PRIMARY,
    /** 요청의 ownershipEpoch가 이 노드가 아는 값보다 오래되었다. */
    STALE_EPOCH,
    /** 과부하로 처리하지 않았다 (mailbox 가득 참 등). 잠시 후 재시도할 수 있다. */
    BUSY,
    /** deadline까지 처리를 끝내지 못했다. 연산이 실제로 적용되었는지는 알 수 없다. */
    TIMEOUT,
    /** 필요한 노드나 복제본에 접근할 수 없다. */
    UNAVAILABLE,
    /** 저장 용량 한도를 넘었다. */
    CAPACITY_EXCEEDED,
    /** 아직 구현되지 않은 연산이다. 성공으로 처리하지 않았다. */
    NOT_IMPLEMENTED,
    /** 서버 내부 오류. */
    INTERNAL_ERROR
}
