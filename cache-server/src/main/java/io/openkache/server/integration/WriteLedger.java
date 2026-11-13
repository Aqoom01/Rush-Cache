package io.openkache.server.integration;

import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/** 이동 전후의 동일 논리 쓰기를 중복 실행하지 않는 run-local ledger. 축출된 오래된 ID는 재실행하지 않고 거절한다. */
final class WriteLedger {
    record Key(long client, long operation) { }
    record Entry(String digest, CompletableFuture<Result> result) { }
    record Admission(Entry entry, boolean fresh, Result rejection) { }
    private final int capacity;
    private final LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>();
    private final Map<Long, Long> floor = new HashMap<>();
    WriteLedger(int capacity) { this.capacity = capacity; }
    synchronized Admission admit(long client, long operation, Command command) {
        Key key = new Key(client, operation); String digest = digest(command); Entry old = entries.get(key);
        if (old != null) return old.digest().equals(digest) ? new Admission(old, false, null)
                : new Admission(null, false, ReplicatedBackend.status(ResultStatus.BAD_REQUEST));
        if (operation <= floor.getOrDefault(client, -1L)) return new Admission(null, false, ReplicatedBackend.status(ResultStatus.BAD_REQUEST));
        if (entries.size() >= capacity) {
            Iterator<Map.Entry<Key, Entry>> it = entries.entrySet().iterator();
            while (it.hasNext() && entries.size() >= capacity) {
                var row = it.next(); if (!row.getValue().result().isDone()) continue;
                floor.merge(row.getKey().client(), row.getKey().operation(), Math::max); it.remove();
            }
        }
        if (entries.size() >= capacity) return new Admission(null, false, ReplicatedBackend.status(ResultStatus.BUSY));
        Entry entry = new Entry(digest, new CompletableFuture<>()); entries.put(key, entry); return new Admission(entry, true, null);
    }
    synchronized void releaseRejected(long client, long operation, Entry entry) {
        entries.remove(new Key(client, operation), entry);
    }
    private static String digest(Command c) {
        try {
            MessageDigest d = MessageDigest.getInstance("SHA-256");
            if (c instanceof Command.Set s) { d.update((byte) 2); part(d, s.key()); part(d, s.value()); }
            else if (c instanceof Command.Delete delete) { d.update((byte) 3); part(d, delete.key()); }
            else throw new IllegalArgumentException("write command expected");
            return HexFormat.of().formatHex(d.digest());
        } catch (NoSuchAlgorithmException e) { throw new AssertionError(e); }
    }
    private static void part(MessageDigest digest, Bytes value) { digest.update(ByteBuffer.allocate(4).putInt(value.size()).array()); digest.update(value.toByteArray()); }
}
