package io.openkache.runner;

/**
 * Runner가 관찰한 요청 한 건의 결과 분류.
 *
 * <p>{@code HIT}/{@code MISS}는 GET·DELETE가 서버의 {@code core.api.ResultStatus}로 {@code OK}/{@code MISS}를
 * 돌려줬을 때다. {@code WRITE_OK}는 SET이 {@code OK}일 때다. {@code CLIENT_FAILURE}는 응답을 전혀 받지 못한 경우
 * (연결 실패, 소켓 read timeout 등)로, 서버가 보낸 wire 상태 코드가 아니다.
 */
enum Outcome {
    HIT,
    MISS,
    WRITE_OK,
    BUSY,
    TIMEOUT,
    UNAVAILABLE,
    SERVER_ERROR,
    CLIENT_FAILURE
}
