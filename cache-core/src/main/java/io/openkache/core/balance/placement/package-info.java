/**
 * B2 배치(재배치) 정책: Tablet을 어느 노드에 둘지 정한다. 경계는 바꾸지 않는다(그건 {@code balance.split}).
 *
 * <p>입력은 B1 {@link io.openkache.core.tablet.RoutingTable}과 window 하나의 Tablet 부하·크기, 노드 용량이고, 출력은 B1
 * {@link io.openkache.core.tablet.Change} 목록이다. 계획만 만든다. 데이터 이동은 B3({@code migration})가, 표 반영은 B1
 * {@link io.openkache.core.tablet.RoutingTable#apply}가 한다.
 *
 * <p>Baseline은 {@link io.openkache.core.balance.placement.GreedyLoadPlacement}다. 정책 목록과 근거는
 * {@code experiments/b2-load-balancing/design.md} §9.
 */
package io.openkache.core.balance.placement;
