package io.openkache.runner;

import java.util.Optional;

/** 반복 실행 한 건의 결과. 실패해도 다른 반복은 계속 실행한다. */
record RunAttempt(int runNumber, RunStatus status, Optional<ExperimentResult> result, Optional<String> error) {

    static RunAttempt success(int runNumber, ExperimentResult result) {
        return new RunAttempt(runNumber, RunStatus.SUCCESS, Optional.of(result), Optional.empty());
    }

    static RunAttempt failed(int runNumber, String error) {
        return new RunAttempt(runNumber, RunStatus.FAILED, Optional.empty(), Optional.of(error));
    }

    static RunAttempt timedOut(int runNumber) {
        return new RunAttempt(runNumber, RunStatus.TIMEOUT, Optional.empty(), Optional.empty());
    }

    static RunAttempt skipped(int runNumber, String reason) {
        return new RunAttempt(runNumber, RunStatus.SKIPPED, Optional.empty(), Optional.of(reason));
    }
}
