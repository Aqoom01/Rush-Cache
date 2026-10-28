package io.openkache.core.replication.ionia_fourth;

import io.openkache.core.api.Command;
import io.openkache.core.replication.ionia.IoniaMessage.LogEntry;
import java.util.*;

/** 기존 server IoniaWire v1의 길이를 계산한다. server 의존성 없이 전체 packet(외부 4바이트 포함)을 센다. */
public final class IoniaPrepareSizer {
    public static final long MAX_KV_PAYLOAD = 32L * 1024 * 1024;
    private final long headerBytes;
    public IoniaPrepareSizer(String configuration) { headerBytes = 52L + modifiedUtfLength(configuration); }
    /** DataOutputStream.writeUTF의 UTF-16 code unit별 modified UTF-8 payload 길이. */
    public static int modifiedUtfLength(String text) {
        Objects.requireNonNull(text); long length = 0;
        for (int i = 0; i < text.length(); i++) {
            int c = text.charAt(i); length += c >= 1 && c <= 127 ? 1 : c <= 2047 ? 2 : 3;
        }
        if (length > 65535) throw new IllegalArgumentException("configuration exceeds writeUTF limit");
        return (int) length;
    }
    public long headerBytes() { return headerBytes; }
    /** index/client/operation/command를 포함한 한 consensus entry의 길이. */
    public long entryBytes(Command command) {
        Objects.requireNonNull(command);
        if (command instanceof Command.Set set) return 33L + set.key().size() + set.value().size();
        if (command instanceof Command.Delete delete) return 29L + delete.key().size();
        throw new IllegalArgumentException("Prepare only contains writes");
    }
    /** 모든 thread에서 사용 가능. 입력 크기 검증이며 실제 남은 disk/RAM 공간 보장은 아니다. */
    public boolean admissible(Command command) {
        long value = command instanceof Command.Set set ? set.value().size() : 0;
        return (command instanceof Command.Set || command instanceof Command.Delete)
                && 17L + command.key().size() + value <= MAX_KV_PAYLOAD
                && headerBytes + entryBytes(command) <= IoniaBatchSettings.MAX_PACKET_BYTES;
    }
    public long packetBytes(List<LogEntry> entries) {
        long bytes = headerBytes;
        for (LogEntry entry : entries) bytes += entryBytes(entry.command());
        return bytes;
    }
}
