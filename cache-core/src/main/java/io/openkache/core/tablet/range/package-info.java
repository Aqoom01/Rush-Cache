/**
 * {@code hash-range-tablet} variant: split·merge로 수와 길이가 바뀌는 해시 구간. Google Slicer·ScyllaDB Tablets 계열.
 *
 * <p>{@link io.openkache.core.tablet.range.HashRangeScheme} 하나다. 경계를 바꾸는 결정은 {@code balance}가 하고, 이 variant는
 * 길이 기준으로 노드 증감 계획만 세운다.
 */
package io.openkache.core.tablet.range;
