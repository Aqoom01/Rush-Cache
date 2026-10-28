package io.openkache.server.ionia;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.ionia.IoniaStorage;
import io.openkache.core.runtime.*;
import io.openkache.core.spi.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32C;

/** JDK-only append-based disk KV adapter. Values are read from disk; RAM retains key/offset metadata.
 * Per-shard owners serialize file I/O. syncKvWrites controls KV force only, NOT fast-ACK durability logs.
 * This functional Java adapter does not claim SplinterDB/LSM performance equivalence.
 */
public final class IoniaFileStorage implements IoniaStorage {
    private record Entry(long offset, int length, long version) { }
    private static final int MAX_RECORD = 32 * 1024 * 1024;
    private final NodeId node;
    private final TimeSource time;
    private final List<OwnerWorker> workers = new ArrayList<>();
    private final List<FileChannel> files = new ArrayList<>();
    private final List<Map<Bytes, Entry>> indexes = new ArrayList<>();
    private final Set<CompletableFuture<?>> pending = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final boolean syncKvWrites;
    private final FileChannel lockChannel;
    private final FileLock lock;

    public IoniaFileStorage(NodeId node, Path directory, int shards, TimeSource time, boolean syncKvWrites) throws IOException {
        if (shards < 1 || shards > 256) throw new IllegalArgumentException("invalid shard count");
        this.node = Objects.requireNonNull(node); this.time = Objects.requireNonNull(time); this.syncKvWrites = syncKvWrites;
        Files.createDirectories(directory);
        lockChannel = FileChannel.open(directory.resolve("storage.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired;
        try { acquired = lockChannel.tryLock(); }
        catch (IOException | RuntimeException error) { lockChannel.close(); throw error; }
        if (acquired == null) { lockChannel.close(); throw new IOException("storage directory is already in use"); }
        lock = acquired;
        try {
            for (int i = 0; i < shards; i++) {
                FileChannel file = FileChannel.open(directory.resolve("kv-" + i + ".data"),
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                files.add(file); indexes.add(new HashMap<>()); scan(i);
                workers.add(new OwnerWorker(new WorkerId(i), 65536));
            }
        } catch (IOException | RuntimeException error) { close(); throw error; }
    }

    @Override public CompletionStage<Result> execute(Command command, RequestContext context) {
        Objects.requireNonNull(command); Objects.requireNonNull(context);
        CompletableFuture<Result> future = track(new CompletableFuture<>());
        if (closed.get()) { future.complete(result(ResultStatus.UNAVAILABLE)); return future; }
        int shard = Math.floorMod(command.key().hashCode(), files.size());
        if (!workers.get(shard).offer(() -> {
            if (closed.get()) future.complete(result(ResultStatus.UNAVAILABLE));
            else if (context.deadline().isExpired(time)) future.complete(result(ResultStatus.TIMEOUT));
            else try { future.complete(run(shard, command)); }
            catch (IOException | RuntimeException error) { future.completeExceptionally(error); }
        })) future.complete(result(ResultStatus.BUSY));
        return future;
    }
    private Result run(int shard, Command command) throws IOException {
        Map<Bytes, Entry> index = indexes.get(shard);
        FileChannel file = files.get(shard);
        if (command instanceof Command.Get get) {
            Entry entry = index.get(get.key());
            if (entry == null) return result(ResultStatus.MISS);
            ByteBuffer bytes = ByteBuffer.allocate(entry.length());
            readFully(file, bytes, entry.offset());
            return Result.ok(Bytes.copyOf(bytes.array()), entry.version(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
        Entry previous = index.get(command.key());
        long version = previous == null ? 1 : Math.incrementExact(previous.version());
        byte[] key = command.key().toByteArray();
        byte[] value = command instanceof Command.Set set ? set.value().toByteArray() : new byte[0];
        long payloadLength = 17L + key.length + value.length;
        if (payloadLength > MAX_RECORD) return result(ResultStatus.CAPACITY_EXCEEDED);
        ByteBuffer payload = ByteBuffer.allocate((int) payloadLength);
        payload.put((byte) (command instanceof Command.Set ? 1 : 2)).putInt(key.length).putInt(value.length).putLong(version).put(key).put(value);
        CRC32C crc = new CRC32C(); crc.update(payload.array());
        ByteBuffer record = ByteBuffer.allocate(payload.capacity() + 8).putInt(payload.capacity()).put(payload.array()).putInt((int) crc.getValue());
        record.flip(); long start = file.size();
        while (record.hasRemaining()) file.write(record, start + record.position());
        if (syncKvWrites) file.force(false);
        if (command instanceof Command.Set) {
            index.put(command.key(), new Entry(start + 21L + key.length, value.length, version));
            return new Result(ResultStatus.OK, Optional.empty(), version, RoutingVersion.ZERO, OwnershipEpoch.ZERO, Optional.empty());
        }
        index.remove(command.key());
        return result(previous == null ? ResultStatus.MISS : ResultStatus.OK);
    }

    private void scan(int shard) throws IOException {
        FileChannel file = files.get(shard); Map<Bytes, Entry> index = indexes.get(shard);
        long position = 0, size = file.size();
        while (position < size) {
            if (size - position < 4) { file.truncate(position); break; }
            ByteBuffer length = ByteBuffer.allocate(4); readFully(file, length, position); length.flip(); int n = length.getInt();
            if (n < 17 || n > MAX_RECORD) throw new IOException("invalid KV record length at " + position);
            if (size - position < n + 8L) { file.truncate(position); break; }
            ByteBuffer body = ByteBuffer.allocate(n + 4); readFully(file, body, position + 4); body.flip();
            byte[] payload = new byte[n]; body.get(payload); int checksum = body.getInt();
            CRC32C crc = new CRC32C(); crc.update(payload);
            if ((int) crc.getValue() != checksum) throw new IOException("KV checksum mismatch at " + position);
            ByteBuffer decoded = ByteBuffer.wrap(payload); byte op = decoded.get();
            int keyLength = decoded.getInt(), valueLength = decoded.getInt(); long version = decoded.getLong();
            if (keyLength < 0 || valueLength < 0 || 17L + keyLength + valueLength != n || version < 1 || (op != 1 && op != 2))
                throw new IOException("invalid KV record at " + position);
            byte[] key = new byte[keyLength]; decoded.get(key); Bytes k = Bytes.copyOf(key);
            if (op == 1) index.put(k, new Entry(position + 21L + keyLength, valueLength, version)); else index.remove(k);
            position += n + 8L;
        }
    }
    private static void readFully(FileChannel file, ByteBuffer buffer, long offset) throws IOException {
        while (buffer.hasRemaining()) if (file.read(buffer, offset + buffer.position()) < 0) throw new EOFException("truncated KV data");
    }
    @Override public CompletionStage<Void> reset() {
        List<CompletableFuture<Void>> resets = new ArrayList<>();
        for (int i = 0; i < workers.size(); i++) {
            int shard = i; CompletableFuture<Void> future = track(new CompletableFuture<>()); resets.add(future);
            if (closed.get() || !workers.get(i).offer(() -> {
                try { files.get(shard).truncate(0); indexes.get(shard).clear(); future.complete(null); }
                catch (IOException error) { future.completeExceptionally(error); }
            })) future.completeExceptionally(new IllegalStateException("storage closed/busy"));
        }
        return CompletableFuture.allOf(resets.toArray(CompletableFuture<?>[]::new));
    }
    private <T> CompletableFuture<T> track(CompletableFuture<T> future) {
        pending.add(future); future.whenComplete((result, error) -> pending.remove(future)); return future;
    }
    @Override public EngineSnapshot snapshot() { return new EngineSnapshot(node, time.monotonicNanos(), RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
    @Override public void onPeerMessage(NodeId from, PeerMessage message) { }
    @Override public void onTimer(TimerEvent event) { }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        pending.forEach(future -> future.completeExceptionally(new IllegalStateException("storage closed")));
        workers.forEach(OwnerWorker::close);
        for (FileChannel file : files) try { file.close(); } catch (IOException ignored) { }
        try { lock.release(); } catch (IOException ignored) { }
        try { lockChannel.close(); } catch (IOException ignored) { }
    }
    private static Result result(ResultStatus status) { return Result.of(status, RoutingVersion.ZERO, OwnershipEpoch.ZERO); }
}
