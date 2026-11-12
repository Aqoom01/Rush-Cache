/**
 * variant {@code ring-vnode}: consistent hashing 링 + vnode(노드당 토큰 V개). Dynamo Strategy 1, Cassandra vnode 계열.
 *
 * <p>표 없이 토큰 목록만으로 주인을 정하는 방식을 공통 {@link io.openkache.core.tablet.RoutingTable} 위에 재현한 것이다.
 * 임의 이동·split이 없어 B2·B3 실험 대상이 아니고, "표를 두면 무엇을 얻는가"를 재는 기준선이다.
 * 구현은 {@link io.openkache.core.tablet.ring.RingScheme}, 설계는 {@code experiments/b1-tablet-distribution/design.md} §4.4.
 */
package io.openkache.core.tablet.ring;
