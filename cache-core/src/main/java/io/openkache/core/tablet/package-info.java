/**
 * Tablet 기반 데이터 분산. B1(Tablet-based Data Distribution)이 사용한다.
 *
 * <p>책임: Key → Tablet → Node 라우팅, Tablet ownership 모델, {@link io.openkache.core.model.RoutingVersion} 갱신.
 *
 * <p>구조: 해시 공간 {@code [0, 2^64)}을 구간({@link io.openkache.core.tablet.TabletRange})으로 빈틈·겹침 없이 덮고,
 * 구간마다 주인 노드와 {@link io.openkache.core.model.OwnershipEpoch}를 둔 표가
 * {@link io.openkache.core.tablet.RoutingTable}이다. 표는 불변이고 모든 변경은 새 표를 돌려주며 버전을 1 올린다.
 * 분배 방식(variant)은 {@link io.openkache.core.tablet.DistributionScheme}으로 바꿔 끼운다. 표 조회와 변경 연산은 variant와
 * 무관하게 {@code RoutingTable}이 맡고, variant는 표를 처음 만드는 규칙과 노드 증감 시 이동 계획만 정한다.
 *
 * <p>상태: 공통 타입과 인터페이스, variant {@code slot}(fixed-slot)·{@code range}(hash-range-tablet)·{@code ring}(ring-vnode),
 * 표 전파·소유권 판정({@code routing}, in-process)까지 구현. wire 인코딩은 미구현. 설계는
 * {@code experiments/b1-tablet-distribution/design.md}를 따른다.
 */
package io.openkache.core.tablet;
