package io.openkache.core.migration.postcopy;

/**
 * Destination-owned 상태. SWITCHING에서 외부 commit 뒤 PULLING으로 간다.
 * commit 전만 ABORTED로 되돌리며, commit 후 장애는 소유권을 유지한 FAILED이고 복구 뒤 resume한다.
 */
public enum PostCopyState {
    PENDING, SWITCHING, PULLING, CLEANING_UP, DONE, ABORTED, FAILED
}
