package io.openkache.server.ionia;

import io.openkache.core.replication.ionia.IoniaViewStore;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/** Stable view metadata is mandatory even when per-request durability logs use replicated RAM. */
public final class IoniaFileViewStore implements IoniaViewStore {
    private final Path path;
    private long view;
    private final boolean restarted;
    public IoniaFileViewStore(Path directory) throws IOException {
        this(directory, "simulation");
    }
    public IoniaFileViewStore(Path directory, String configuration) throws IOException {
        Files.createDirectories(directory);
        Path configurationFile = directory.resolve("configuration.meta");
        if (Files.exists(configurationFile)) {
            if (!Files.readString(configurationFile).equals(configuration)) throw new IOException("static configuration/node changed; reconfiguration is unsupported");
        } else atomicWrite(configurationFile, java.nio.charset.StandardCharsets.UTF_8.encode(configuration));
        path = directory.resolve("view.meta"); restarted = Files.exists(path);
        if (restarted) {
            byte[] bytes = Files.readAllBytes(path);
            if (bytes.length != 8 || (view = ByteBuffer.wrap(bytes).getLong()) < 0) throw new IOException("invalid stable view");
        }
        save(view);
    }
    public long view() { return view; }
    public boolean restarted() { return restarted; }
    @Override public synchronized void save(long next) {
        if (next < view) throw new IllegalArgumentException("view regression");
        try {
            ByteBuffer bytes = ByteBuffer.allocate(8).putLong(next); bytes.flip(); atomicWrite(path, bytes);
            view = next;
        } catch (IOException error) { throw new UncheckedIOException("cannot persist IONIA view", error); }
    }
    private static void atomicWrite(Path target, ByteBuffer bytes) throws IOException {
        Path temporary = target.resolveSibling(target.getFileName() + ".next");
        try (FileChannel file = FileChannel.open(temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            while (bytes.hasRemaining()) file.write(bytes);
            file.force(true);
        }
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
