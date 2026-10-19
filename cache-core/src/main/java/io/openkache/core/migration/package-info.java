/**
 * Live Migration과 Elastic Scaling. B3가 사용한다.
 *
 * <p>책임: Tablet snapshot 복사, 복사 중 변경분(delta) 동기화, cutover, 노드 추가·제거 시 rebalancing 실행.
 *
 * <p>상태: 미구현. cutover 시 {@link io.openkache.core.model.OwnershipEpoch}와
 * {@link io.openkache.core.model.RoutingVersion}을 올려 이전 소유자의 요청을 거절한다.
 */
package io.openkache.core.migration;
