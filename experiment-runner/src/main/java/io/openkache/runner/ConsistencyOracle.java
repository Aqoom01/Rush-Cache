package io.openkache.runner;

import io.openkache.core.model.Bytes;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 성공한 쓰기를 key마다 기록하는 정합성 oracle. 같은 key의 쓰기는 key별 lock으로 Runner 안에서 한 번에 하나만 보내므로
 * "마지막으로 성공한 쓰기"가 정해진다(서로 다른 key는 동시에 보낸다). 값 자체 대신 64비트 해시를 보관한다.
 *
 * <p>상태: 기록 없음, 존재(값 해시), 삭제됨, 불확실(응답을 못 받아 적용 여부를 모르는 쓰기 뒤). 불확실한 key는 검사에서 뺀다.
 * 여러 thread에서 호출한다. key의 상태는 그 key의 lock을 쥔 thread만 바꾼다.
 */
final class ConsistencyOracle {

    static final byte UNKNOWN = 0;
    static final byte PRESENT = 1;
    static final byte DELETED = 2;
    static final byte UNCERTAIN = 3;

    private final byte[] state;
    private final long[] valueHash;
    // freeze된 key의 쓰기가 lock을 오래 쥐므로 다른 key와 lock을 나눠 쓰지 않는다(처음 쓸 때 만든다)
    private final AtomicReferenceArray<ReentrantLock> locks;

    ConsistencyOracle(int keyCount) {
        state = new byte[keyCount];
        valueHash = new long[keyCount];
        locks = new AtomicReferenceArray<>(keyCount);
    }

    int keyCount() {
        return state.length;
    }

    ReentrantLock lock(int key) {
        ReentrantLock lock = locks.get(key);
        if (lock != null) return lock;
        locks.compareAndSet(key, null, new ReentrantLock());
        return locks.get(key);
    }

    /** 성공한 SET. 호출자가 그 key의 lock을 쥐고 있어야 한다. */
    void set(int key, Bytes value) {
        valueHash[key] = hash(value.toByteArray());
        state[key] = PRESENT;
    }

    /** 성공한 DELETE(OK 또는 MISS). */
    void delete(int key) {
        state[key] = DELETED;
    }

    /** 응답을 못 받아 적용 여부를 모르는 쓰기. */
    void uncertain(int key) {
        state[key] = UNCERTAIN;
    }

    byte state(int key) {
        return state[key];
    }

    boolean matches(int key, byte[] value) {
        return valueHash[key] == hash(value);
    }

    /** {@code "key-<n>"} 형식(뒤는 0으로 채움)의 key에서 번호를 읽는다. */
    static int indexOf(Bytes key) {
        int n = 0;
        for (int i = 4; i < key.size(); i++) {
            byte b = key.get(i);
            if (b == 0) break;
            n = n * 10 + (b - '0');
        }
        return n;
    }

    /** FNV-1a 64. */
    static long hash(byte[] value) {
        long h = 0xcbf29ce484222325L;
        for (byte b : value) {
            h ^= b & 0xff;
            h *= 0x100000001b3L;
        }
        return h;
    }
}
