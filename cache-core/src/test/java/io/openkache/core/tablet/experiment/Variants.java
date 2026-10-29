package io.openkache.core.tablet.experiment;

import io.openkache.core.tablet.DistributionScheme;
import io.openkache.core.tablet.order.KeyOrderScheme;
import io.openkache.core.tablet.tag.HashTagScheme;
import io.openkache.core.tablet.range.HashRangeScheme;
import io.openkache.core.tablet.resize.ResizableSlotScheme;
import io.openkache.core.tablet.ring.RingScheme;
import io.openkache.core.tablet.slot.FixedSlotScheme;
import java.util.List;
import java.util.function.IntFunction;

/**
 * 비교 대상 variant와 "전체 단위 수 G"를 variant별 손잡이로 바꾸는 규칙.
 *
 * <p>variant마다 잘게 자르는 손잡이가 다르다(fixed-slot은 전체 슬롯 수 S, hash-range는 노드당 Tablet 수 T, ring은 노드당 토큰 수
 * V). 같은 조건으로 비교하려고 기준 노드 수 {@link #BASE_NODES}에서 표의 줄 수가 G가 되도록 환산한다.
 *
 * <pre>
 * fixed-slot          S = G
 * hash-range-tablet   T = G / BASE_NODES
 * ring-vnode          V = G / BASE_NODES   (무작위 토큰)
 * ring-vnode-alloc    V = G / BASE_NODES   (가장 많이 맡은 노드에서 공정 몫만큼 잘라 오는 토큰)
 * hash-tag-tablet     T = G / BASE_NODES   (표는 hash-range-tablet과 같고 keyHash만 hash tag)
 * resizable-slot      T = G / BASE_NODES   (노드당 목표 슬롯 수. 노드 8대면 S = 8T = G, 노드 수에 따라 2배/절반)
 * key-order-tablet    T = G / BASE_NODES   (표는 hash-range-tablet과 같고 keyHash만 순서 보존: 앞 8바이트)
 * </pre>
 *
 * <p>ring 둘은 {@code supportsMove() = false}라 S5의 그리디 한계는 참고값이고, 표 크기는 토큰 목록 크기다. hash-tag-tablet은
 * tag 없는 key(S1~S5)에서는 hash-range-tablet과 같은 값이 나와야 하고, 차이는 S7(태그 묶음)에서만 난다. resizable-slot은 노드 8대에서
 * fixed-slot과 같은 표라 S1·S5·S7이 같고, 노드 수가 움직이는 S4·S9에서 슬롯 수가 바뀐다.
 */
final class Variants {

    /** 환산 기준 노드 수. 실험의 기본 노드 수와 같다. */
    static final int BASE_NODES = 8;

    record Variant(String name, IntFunction<DistributionScheme> forGranularity) {}

    static final List<Variant> ALL = List.of(
            new Variant("fixed-slot", g -> new FixedSlotScheme(g)),
            new Variant("hash-range-tablet", g -> new HashRangeScheme(Math.max(1, g / BASE_NODES))),
            new Variant("ring-vnode", g -> new RingScheme(Math.max(1, g / BASE_NODES), RingScheme.Placement.RANDOM)),
            new Variant("ring-vnode-alloc",
                    g -> new RingScheme(Math.max(1, g / BASE_NODES), RingScheme.Placement.BALANCED)),
            new Variant("hash-tag-tablet", g -> new HashTagScheme(Math.max(1, g / BASE_NODES))),
            new Variant("resizable-slot", g -> new ResizableSlotScheme(Math.max(1, g / BASE_NODES))),
            new Variant("key-order-tablet", g -> new KeyOrderScheme(Math.max(1, g / BASE_NODES))));

    private Variants() {}
}
