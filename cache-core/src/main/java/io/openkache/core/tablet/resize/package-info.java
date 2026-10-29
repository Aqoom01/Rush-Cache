/**
 * {@code resizable-slot} variant: 균등 슬롯이되 슬롯 수를 전체 2배 / 절반으로 바꿀 수 있다. ScyllaDB tablets 계열.
 *
 * <p>{@link io.openkache.core.tablet.resize.ResizableSlotScheme} 하나다. fixed-slot(슬롯 수 고정)과 hash-range-tablet(슬롯 하나씩 split)
 * 사이의 중간 방식이다.
 */
package io.openkache.core.tablet.resize;
