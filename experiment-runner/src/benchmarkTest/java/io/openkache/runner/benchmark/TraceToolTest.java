package io.openkache.runner.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TraceToolTest {
    @TempDir Path root;

    private Path config(String change) throws Exception {
        String text = """
                seed=101
                key.count=17
                key.bytes=24
                value.bytes=100
                mix.get=80
                mix.set=15
                mix.delete=5
                stream.count=4
                distribution=UNIFORM
                hot.probability=0
                warmup.requests=37
                measured.requests=101
                """;
        if (!change.isEmpty()) {
            String[] pair = change.split("=", 2);
            text = text.replaceAll("(?m)^" + java.util.regex.Pattern.quote(pair[0]) + "=.*$",
                    java.util.regex.Matcher.quoteReplacement(change));
        }
        return Files.writeString(root.resolve("config.properties"), text);
    }

    @Test
    void hashOnlyWritesDeterministicMetadataAndNoTraceFiles() throws Exception {
        Path config = config("");
        Path a = root.resolve("a");
        Path b = root.resolve("b");
        TraceTool.main(new String[]{config.toString(), a.toString()});
        TraceTool.main(new String[]{config.toString(), b.toString()});
        assertEquals(Files.readString(a.resolve("trace-metadata.json")), Files.readString(b.resolve("trace-metadata.json")));
        try (var files = Files.list(a)) { assertEquals(1, files.count()); }
        assertTrue(Files.readString(a.resolve("trace-metadata.json")).contains("\"request_execution\": \"not_run\""));
    }

    @Test
    void emitModeWritesThreeHashVerifiedTraceFiles() throws Exception {
        Path config = config("");
        Path output = root.resolve("emitted");
        TraceTool.main(new String[]{config.toString(), output.toString(), "--emit-traces"});
        String metadata = Files.readString(output.resolve("trace-metadata.json"));
        for (String name : new String[]{"load", "warmup", "measure"}) {
            byte[] bytes = Files.readAllBytes(output.resolve(name + ".trace"));
            String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            assertTrue(metadata.contains(hash));
        }
        try (var files = Files.list(output)) { assertEquals(4, files.count()); }
    }

    @Test
    void existingOutputIsNeverOverwritten() throws Exception {
        Path config = config("");
        Path output = Files.createDirectory(root.resolve("existing"));
        Path sentinel = Files.writeString(output.resolve("sentinel"), "preserve");
        assertThrows(FileAlreadyExistsException.class,
                () -> TraceTool.main(new String[]{config.toString(), output.toString()}));
        assertEquals("preserve", Files.readString(sentinel));
    }

    @Test
    void unsupportedDistributionAndNegativeCountsFailBeforeCreatingOutput() throws Exception {
        Path output = root.resolve("invalid");
        Path unsupported = config("distribution=ZIPF");
        assertThrows(IllegalArgumentException.class,
                () -> TraceTool.main(new String[]{unsupported.toString(), output.toString()}));
        assertFalse(Files.exists(output));
        Path negative = config("measured.requests=-1");
        assertThrows(IllegalArgumentException.class,
                () -> TraceTool.main(new String[]{negative.toString(), output.toString()}));
        assertFalse(Files.exists(output));
    }

    @Test
    void unknownMissingPropertiesAndInvalidFlagFailBeforeOutput() throws Exception {
        Path output = root.resolve("invalid");
        Path unknown = config("");
        Files.writeString(unknown, Files.readString(unknown) + "target.host=do-not-connect\n");
        assertThrows(IllegalArgumentException.class,
                () -> TraceTool.main(new String[]{unknown.toString(), output.toString()}));
        assertFalse(Files.exists(output));
        Path config = config("");
        Files.writeString(config, Files.readString(config).replace("seed=101\n", ""));
        Path missing = config;
        assertThrows(IllegalArgumentException.class,
                () -> TraceTool.main(new String[]{missing.toString(), output.toString()}));
        assertFalse(Files.exists(output));
        assertThrows(IllegalArgumentException.class, () -> TraceTool.main(new String[]{"a", "b", "--send"}));
    }
}

