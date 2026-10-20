package io.openkache.runner;

/** 반복 실행 한 건의 최종 상태. */
enum RunStatus {
    SUCCESS,
    FAILED,
    TIMEOUT,
    SKIPPED
}
