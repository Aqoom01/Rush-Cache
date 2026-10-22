package io.openkache.runner;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchLauncherTest {
    @TempDir Path directory;

    @Test
    void researchConfigResolvesPathsAndRejectsMalformedOptions() throws Exception {
        Path config = config("example-study", "example-experiment", SyntheticBackend.class, "complete");
        Path repository = config.getParent();
        String valid = Files.readString(config) + "repeat.count=2\nbackend.arg.6={config.file}\n";
        Files.writeString(config, valid);
        var parsed = ResearchConfig.load(config);
        assertEquals(repository, parsed.repository());
        assertEquals(repository.resolve("out"), parsed.results());
        assertEquals(2, parsed.repeats());
        assertEquals(SyntheticBackend.class.getName(), parsed.backend().mainClass());
        assertEquals(List.of(repository.resolve("output with spaces").toString(), "complete", "1", "2", "label with spaces", config.toString()),
                parsed.arguments(repository.resolve("output with spaces"), 1));
        assertEquals("600000", parsed.snapshot().get("run.timeout.ms"));
        Files.writeString(config, valid + "research.profle=full\n");
        assertThrows(ConfigException.class, () -> ResearchConfig.load(config));
        Files.writeString(config, valid + "research.profile=full\n");
        assertThrows(ConfigException.class, () -> ResearchConfig.load(config));
        Files.writeString(config, valid + "backend.option.profile=custom-profile\n");
        assertEquals("custom-profile", ResearchConfig.load(config).snapshot().get("backend.option.profile"));
        Files.writeString(config, valid + "backend.arg.8=gap\n");
        assertThrows(ConfigException.class, () -> ResearchConfig.load(config));
        Files.writeString(config, valid.replace("{config.file}", "{unknown}"));
        assertThrows(ConfigException.class, () -> ResearchConfig.load(config));
        Files.writeString(config, valid + "backend.jvm.arg.1=-jar\n");
        assertThrows(ConfigException.class, () -> ResearchConfig.load(config));
        Files.writeString(config, valid.replace("repeat.count=2", "repeat.count=0"));
        assertThrows(ConfigException.class, () -> ResearchConfig.load(config));
        Files.writeString(config, "study=tcp-baseline\n");
        assertEquals("tcp", ResearchConfig.mode(config));
    }

    @Test
    void launchesTwoDifferentResearchBackendsUsingOnlyTheirConfigs() throws Exception {
        for (Class<?> backend : List.of(SyntheticBackend.class, OtherBackend.class)) {
            String name = backend == SyntheticBackend.class ? "distribution" : "balancing";
            Path file = config(name, name + "-experiment", backend, "complete");
            Files.writeString(file, Files.readString(file) + "repeat.count=2\n");
            ResearchConfig config = ResearchConfig.load(file);
            assertEquals(0, ResearchLauncher.run(config));
            Path suite = suite(config.results());
            assertTrue(suite.getFileName().toString().startsWith("run-"));
            String manifest = Files.readString(suite.resolve("manifest.json"));
            assertTrue(manifest.contains("\"study\":\"" + name + "\""));
            assertTrue(manifest.contains("\"status\":\"complete\""));
            assertTrue(manifest.contains("\"resultSchemaVersion\":\"example-json-1\""));
            for (String repeat : List.of("repeat-001", "repeat-002")) {
                String original = Files.readString(suite.resolve(repeat + "/results/case.json"));
                assertTrue(original.contains(backend == SyntheticBackend.class ? "distribution" : "balancing"));
                assertTrue(Files.readString(suite.resolve(repeat + "/backend.log")).contains("label with spaces"));
            }
        }
    }

    @Test
    void zeroExitDoesNotHideMissingOrMismatchedResultArtifacts() throws Exception {
        for (String mode : List.of("incomplete", "schema", "count")) {
            ResearchConfig config = ResearchConfig.load(config("validation", mode, SyntheticBackend.class, mode));
            assertEquals(1, ResearchLauncher.run(config));
            Path suite = suite(config.results());
            assertTrue(Files.readString(suite.resolve("manifest.json")).contains("\"status\":\"partial\""));
            assertTrue(Files.readString(suite.resolve("run-summary.json")).contains("\"status\":\"FAILED\""));
        }
    }

    @Test
    void hashesSourcesSelectedByConfigAndFailsBeforeCreatingOutputWhenBackendIsMissing() throws Exception {
        Path file = config("example", "hash", SyntheticBackend.class, "complete");
        ResearchConfig config = ResearchConfig.load(file);
        String hash = ResearchFiles.sourceHash(config.repository(), config.backend().sources());
        Files.writeString(config.repository().resolve("Research.java"), "changed research code");
        assertNotEquals(hash, ResearchFiles.sourceHash(config.repository(), config.backend().sources()));
        Files.writeString(file, Files.readString(file).replace(SyntheticBackend.class.getName(), "example.MissingMain"));
        assertThrows(ConfigException.class, () -> ResearchLauncher.run(ResearchConfig.load(file)));
        assertFalse(Files.exists(config.results()));
    }

    @Test
    void normalizesSourceLineEndingsAndRejectsStaleSourcesClassesAndMissingBuildReceipt() throws Exception {
        ResearchConfig config = ResearchConfig.load(config("example", "build-check", SyntheticBackend.class, "complete"));
        Path source = config.repository().resolve("Research.java");
        Files.writeString(source, "first\r\nsecond\r\n");
        ResearchBuild.record(config);
        String hash = ResearchFiles.sourceHash(config.repository(), config.backend().sources());
        Files.writeString(source, "first\nsecond\n");
        assertEquals(hash, ResearchBuild.verify(config).get("sourceHash"));
        Files.writeString(source, "first\nchanged\n");
        assertThrows(ConfigException.class, () -> ResearchLauncher.run(config));
        assertFalse(Files.exists(config.results()));
        ResearchBuild.record(config);
        Path binary = config.backend().classpath().getFirst().resolve(SyntheticBackend.class.getName().replace('.', '/') + ".class");
        Files.write(binary, new byte[] {1, 2, 3});
        assertThrows(ConfigException.class, () -> ResearchLauncher.run(config));
        assertFalse(Files.exists(config.results()));
        ResearchBuild.record(config);
        Files.delete(config.backend().buildReceipt());
        assertThrows(ConfigException.class, () -> ResearchLauncher.run(config));
    }

    @Test
    void recordsCommitWithoutWorkingTreeStatusForCleanAndModifiedRepositories() throws Exception {
        for (boolean dirty : List.of(false, true)) {
            Path file = config("git-state", "git-state-" + dirty, SyntheticBackend.class, "complete");
            Files.writeString(file, Files.readString(file).replace("results.dir=out", "results.dir=experiments/example/runs"));
            ResearchConfig config = ResearchConfig.load(file);
            git(config.repository(), "init", "-q");
            git(config.repository(), "config", "core.autocrlf", "false");
            Files.writeString(config.repository().resolve(".gitignore"), "build/\n");
            git(config.repository(), "add", ".");
            git(config.repository(), "-c", "user.name=Research Test", "-c", "user.email=research@example.invalid", "commit", "-qm", "fixture");
            String commit = GitInfo.capture(config.repository()).commit().orElseThrow();
            if (dirty) Files.writeString(config.repository().resolve("Untracked.java"), "untracked source");
            assertEquals(0, ResearchLauncher.run(config));
            Path suite = suite(config.results());
            String manifest = Files.readString(suite.resolve("manifest.json"));
            assertFalse(manifest.contains("workingTreeDirty"));
            assertFalse(manifest.contains("gitStateCapturedAt"));
            assertTrue(manifest.contains("\"gitCommit\":\"" + commit + "\""));
            var launch = ResearchConfig.properties(suite.resolve("repeat-001/launch.properties"));
            assertFalse(launch.containsKey("workingTreeDirty"));
            assertEquals(commit, launch.getProperty("gitCommit"));
            assertEquals(commit, GitInfo.capture(config.repository()).commit().orElseThrow());
        }
    }

    private static void git(Path repository, String... args) throws Exception {
        var command = new ArrayList<>(List.of("git", "-C", repository.toString()));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), output);
    }

    private Path config(String study, String experiment, Class<?> backend, String mode) throws Exception {
        Path repository = Files.createDirectory(directory.resolve(experiment));
        Files.writeString(repository.resolve("settings.gradle.kts"), "");
        Files.createDirectories(repository.resolve("experiment-runner/src/main"));
        Files.writeString(repository.resolve("Research.java"), "research source");
        Path file = repository.resolve("research.properties");
        Path classpath = Files.createDirectories(repository.resolve("build/backend"));
        Path compiled = Path.of(backend.getProtectionDomain().getCodeSource().getLocation().toURI());
        for (Class<?> fixture : List.of(SyntheticBackend.class, OtherBackend.class)) {
            Path relative = Path.of(fixture.getName().replace('.', '/') + ".class");
            Files.createDirectories(classpath.resolve(relative).getParent());
            Files.copy(compiled.resolve(relative), classpath.resolve(relative));
        }
        Files.writeString(file, "execution.mode=research\nstudy=" + study + "\nexperiment=" + experiment + "\nrepository.root=.\nresults.dir=out\n"
                + "backend.main=" + backend.getName() + "\nbackend.classpath=build/backend\nbackend.source.paths=Research.java\nbackend.build.receipt=build/research.properties\n"
                + "backend.result.schema=example-json-1\nbackend.option.mode=" + mode + "\nbackend.option.label=label with spaces\n"
                + "backend.arg.1={output.dir}\nbackend.arg.2={config.backend.option.mode}\nbackend.arg.3={repeat}\nbackend.arg.4={repeat.count}\n"
                + "backend.arg.5={config.backend.option.label}\n");
        ResearchBuild.record(ResearchConfig.load(file));
        return file;
    }

    private static Path suite(Path output) throws Exception {
        try (var paths = Files.list(output)) { return paths.findFirst().orElseThrow(); }
    }

    public static final class SyntheticBackend {
        private SyntheticBackend() {}
        public static void main(String[] args) throws Exception { write(args, "distribution"); }
        static void write(String[] args, String kind) throws Exception {
            Path output = Path.of(args[0]);
            Files.createDirectory(output.resolve("results"));
            Files.writeString(output.resolve("results/case.json"), "{\"kind\":\"" + kind + "\",\"repeat\":" + args[2] + "}");
            Files.writeString(output.resolve("summary.csv"), "kind\n" + kind + "\n");
            Files.writeString(output.resolve("results.md"), "# " + kind + "\n");
            if (!args[1].equals("incomplete")) Files.writeString(output.resolve("completion.properties"), "status=complete\nresultSchemaVersion="
                    + (args[1].equals("schema") ? "wrong-schema" : "example-json-1") + "\ncases=" + (args[1].equals("count") ? 2 : 1) + "\n");
            System.out.println(args[4]);
        }
    }

    public static final class OtherBackend {
        private OtherBackend() {}
        public static void main(String[] args) throws Exception { SyntheticBackend.write(args, "balancing"); }
    }

    @Test
    void capturesSuccessAndNonzeroExitWithoutBlockingOnProcessOutput() throws Exception {
        Path log = directory.resolve("child.log");
        assertEquals(new ResearchLauncher.ProcessResult("SUCCESS", 0), ResearchLauncher.execute(command("ok"), directory, log, 10_000));
        assertTrue(Files.readString(log).contains("완료"));
        assertEquals(new ResearchLauncher.ProcessResult("FAILED", 3), ResearchLauncher.execute(command("fail"), directory, log, 10_000));
    }

    @Test
    void terminatesBackendOnDeadlineInsteadOfLeavingItRunning() throws Exception {
        Path log = directory.resolve("timeout.log");
        assertEquals("TIMEOUT", ResearchLauncher.execute(command("wait"), directory, log, 2_000).status());
        String output = Files.readString(log).strip();
        if (!output.isEmpty()) {
            long pid = Long.parseLong(output.lines().findFirst().orElseThrow());
            assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false));
        }
    }

    private List<String> command(String mode) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(ResearchLauncher.javaExecutable()); command.add("-Dstdout.encoding=UTF-8"); command.add("-cp");
        command.add(Path.of(Child.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString());
        command.add(Child.class.getName()); command.add(mode); return command;
    }

    public static final class Child {
        private Child() {}
        public static void main(String[] args) throws InterruptedException {
            if (args[0].equals("wait")) { System.out.println(ProcessHandle.current().pid()); Thread.sleep(60_000); }
            if (args[0].equals("fail")) System.exit(3);
            System.out.println(new String("완료".getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
        }
    }
}
