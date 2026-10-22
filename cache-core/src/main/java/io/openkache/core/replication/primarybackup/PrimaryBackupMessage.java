package io.openkache.core.replication.primarybackup;

import io.openkache.core.api.Command;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.spi.PeerMessage;
import java.util.Objects;

/**
 * Primary와 Backup 사이에 주고받는 복제 메시지.
 *
 * <p>모든 메시지는 불변이며 thread 간 공유할 수 있다. 보낸 노드와 받을 노드는 공통 {@code PeerEnvelope}가 표현하므로 여기에
 * 넣지 않는다. 이 타입은 형식만 검증한다. 보낸 노드가 지정된 Primary·Backup인지, epoch와 op-number가 진행 중인 요청과
 * 일치하는지는 {@link PrimaryBackupEngine}이 검사한다. wire 인코딩은 아직 없다.
 */
public sealed interface PrimaryBackupMessage extends PeerMessage {

    /** 메시지가 속한 소유권 epoch. 이번 단계에서는 설정으로 고정한 값이다. */
    OwnershipEpoch epoch();

    /** Primary가 부여한 쓰기 순서 번호. 1부터 시작해 쓰기마다 1 증가한다. */
    long opNumber();

    /**
     * Primary가 Backup에 보내는 쓰기 전달. Backup은 이 SET을 자기 KV에 적용한 뒤에만 {@link ReplicateAck}를 보낸다.
     *
     * @param epoch 고정 epoch
     * @param opNumber 1 이상
     * @param command 적용할 SET. 이번 단계에서는 SET만 복제한다
     */
    record Replicate(OwnershipEpoch epoch, long opNumber, Command.Set command) implements PrimaryBackupMessage {
        public Replicate {
            Objects.requireNonNull(epoch, "epoch");
            requireOpNumber(opNumber);
            Objects.requireNonNull(command, "command");
        }
    }

    /**
     * Backup이 같은 epoch·op-number의 {@link Replicate}를 자기 KV에 적용했다는 확인. 적용에 실패하면 보내지 않는다.
     *
     * <p>{@code Transport.send} 완료와 다르다. Primary는 이 메시지를 받아야만 그 Backup의 ACK로 센다.
     *
     * @param epoch 고정 epoch
     * @param opNumber 적용한 쓰기의 번호. 1 이상
     */
    record ReplicateAck(OwnershipEpoch epoch, long opNumber) implements PrimaryBackupMessage {
        public ReplicateAck {
            Objects.requireNonNull(epoch, "epoch");
            requireOpNumber(opNumber);
        }
    }

    private static void requireOpNumber(long opNumber) {
        if (opNumber < 1) {
            throw new IllegalArgumentException("opNumber must be >= 1: " + opNumber);
        }
    }
}
