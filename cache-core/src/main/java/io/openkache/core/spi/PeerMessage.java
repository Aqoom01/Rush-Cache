package io.openkache.core.spi;

/**
 * 노드 사이에 주고받는 메시지의 공통 타입.
 *
 * <p>각 연구(복제, 장애 감지, 마이그레이션 등)가 자기 메시지를 불변 record로 정의하고 이 인터페이스를 구현한다. wire 인코딩은
 * server가 담당하며, 연구별 peer 프로토콜은 아직 정의되지 않았다.
 */
public interface PeerMessage {}
