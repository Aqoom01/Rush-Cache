package io.openkache.runner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 결과 재현성 metadata용 Git 상태. 이 디렉터리가 Git 저장소가 아니거나 {@code git}을 실행할 수 없으면 조용히
 * {@link Optional#empty()}를 반환한다(실험 자체는 git 정보 없이도 계속된다).
 *
 * @param commit {@code git rev-parse HEAD}
 * @param workingTreeDirty {@code git status --porcelain}의 출력이 비어 있지 않으면 {@code true}
 */
record GitInfo(Optional<String> commit, Optional<Boolean> workingTreeDirty) {

    static GitInfo capture() {
        Optional<String> commit = run("git", "rev-parse", "HEAD").map(String::trim);
        Optional<Boolean> dirty = run("git", "status", "--porcelain").map(output -> !output.isBlank());
        return new GitInfo(commit, dirty);
    }

    private static Optional<String> run(String... command) {
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output;
            try (var processOutput = process.getInputStream()) {
                output = new String(processOutput.readAllBytes(), StandardCharsets.UTF_8);
            }
            boolean finished = process.waitFor(5, TimeUnit.SECONDS);
            if (!finished || process.exitValue() != 0) {
                return Optional.empty();
            }
            return Optional.of(output);
        } catch (IOException e) {
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }
}
