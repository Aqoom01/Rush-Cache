/**
 * 노드 로컬 Key-Value 저장소.
 *
 * <p>책임: 한 노드 안에서 Key-Value를 저장·조회·삭제한다. 복제·라우팅은 다루지 않는다.
 *
 * <p>상태: {@link io.openkache.core.kv.KvEngine}이 단일 노드 인메모리 GET/SET/DELETE를 구현한다. Key마다 고정 worker로
 * 샤딩하고, 저장소는 그 owner worker thread에서만 변경한다는 {@code runtime} 규칙을 따른다.
 *
 * <p>아직 구현하지 않은 것: TTL 만료, 저장 용량 한도(eviction), {@code EXISTS}·{@code COUNT}({@link
 * io.openkache.core.api.Command}에 해당 연산이 아직 없다), SCAN/RANGE, 영속화. 복제·Tablet 연동 시 필요한 epoch 검사는
 * {@link io.openkache.core.spi.EpochStore}와 함께 그 연구 패키지가 책임진다.
 */
package io.openkache.core.kv;
