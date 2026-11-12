/**
 * {@code key-order-tablet} variant: key를 해시하지 않고 바이트 순서 그대로 범위로 나눈다. Bigtable·HBase·TiKV 계열.
 *
 * <p>{@link io.openkache.core.tablet.order.KeyOrderScheme} 하나다. 표는 hash-range-tablet과 같고 key → 점 변환(앞 8바이트)과 표본 기반
 * pre-split만 다르다.
 */
package io.openkache.core.tablet.order;
