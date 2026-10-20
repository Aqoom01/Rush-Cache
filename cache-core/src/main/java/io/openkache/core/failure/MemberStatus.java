package io.openkache.core.failure;

/** SWIM이 관측한 구성원 상태. */
public enum MemberStatus {
    ALIVE,
    SUSPECT,
    DEAD
}
