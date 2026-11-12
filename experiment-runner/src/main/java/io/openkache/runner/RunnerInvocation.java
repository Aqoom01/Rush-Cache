package io.openkache.runner;

import java.util.List;

/** Runner를 실행할 때 전달된 명령과 인자. 결과 재현을 위해 기록한다. */
record RunnerInvocation(String command, String configPath, List<String> arguments) {

    RunnerInvocation {
        arguments = List.copyOf(arguments);
    }

    static RunnerInvocation fromMainArgs(String[] args) {
        return new RunnerInvocation("openkache-experiment-runner", args[0], List.of(args));
    }
}
