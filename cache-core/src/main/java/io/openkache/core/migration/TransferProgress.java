package io.openkache.core.migration;

/**
 * 1단계 snapshot 전송 진행도의 불변 스냅샷. 설계 §3을 따른다.
 *
 * <p>실행 주체가 owner/controller thread에서 진행도를 갱신한다. byte 수는 직렬화된 전송량이다.
 * batch id는 0부터 시작하며 아직 전송/ACK한 batch가 없으면 -1이다.
 *
 * @param snapshotComplete 마지막 snapshot batch의 ACK까지 확인했는지
 * @param lastSentBatchId 마지막으로 전송한 batch id
 * @param lastAckBatchId 마지막으로 ACK를 확인한 batch id
 * @param sentBytes 누적 전송량, bytes
 */
public record TransferProgress(
        boolean snapshotComplete,
        long lastSentBatchId,
        long lastAckBatchId,
        long sentBytes) {
    public TransferProgress {
        if (sentBytes < 0 || lastSentBatchId < -1 || lastAckBatchId < -1 || lastAckBatchId > lastSentBatchId) {
            throw new IllegalArgumentException("invalid transfer progress");
        }
        if (snapshotComplete && (lastAckBatchId < 0 || lastAckBatchId != lastSentBatchId)) {
            throw new IllegalArgumentException("snapshot requires final ACK");
        }
    }

    /** 전송 시작 전 상태. */
    public static TransferProgress initial() {
        return new TransferProgress(false, -1, -1, 0);
    }
}
