/**
 * Tablet 기반 데이터 분산. B1(Tablet-based Data Distribution)이 사용한다.
 *
 * <p>책임: Key → Tablet → Node 라우팅, Tablet ownership 모델, {@link io.openkache.core.model.RoutingVersion} 갱신.
 *
 * <p>상태: 미구현. Tablet 메타데이터(Owner, Key Range, Version) 구조는 B1·B2·B3 담당자가 합의한 뒤 정의한다. 식별자는
 * {@link io.openkache.core.model.TabletId}를 쓴다.
 */
package io.openkache.core.tablet;
