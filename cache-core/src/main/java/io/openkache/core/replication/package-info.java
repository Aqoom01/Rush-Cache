/**
 * 복제 전략. A1(Active-Passive Fast Failover)과 A2(Active-Active / SSD-aware Replication)가 사용한다.
 *
 * <p>책임: Primary/Backup 또는 다중 쓰기 복제, 승격, epoch/fencing, 복제 확인 메시지 정의. 두 연구의 전략은 이 패키지 안에서
 * 하위 패키지로 나눈다.
 *
 * <p>상태: 미구현. 복제 메시지는 {@link io.openkache.core.spi.PeerMessage}로, epoch 영속화는
 * {@link io.openkache.core.spi.EpochStore}로 다룬다. {@code Transport.send} 완료는 원격 commit이 아님에 유의한다.
 */
package io.openkache.core.replication;
