package io.openkache.core.balance.placement;

/**
 * Tablet을 어느 노드에 둘지 정하는 정책. 분할({@code split})과 책임이 다르다: 분할은 Tablet 경계를, 배치는 주인을 바꾼다.
 *
 * <p>구현은 다음을 지켜야 한다(design.md §9, {@code PlacementInvariantTest}).
 *
 * <ul>
 *   <li>{@code nodeCapacity}에 없는 노드로 옮기지 않고, 그 밖의 노드가 가진 Tablet은 모두 허용된 노드로 옮긴다(노드 제거).
 *   <li>Tablet당 이동은 많아야 하나이고, {@code from}은 현재 주인, {@code to}는 그와 다르다.
 *   <li>같은 입력에는 같은 결정을 낸다. 난수 대신 입력에서 만든 해시를 쓰고 실제 시각을 쓰지 않는다.
 *   <li>입력을 변경하지 않는다. 상태를 갖지 않는다(cooldown은 {@link PlacementInput#lastMovedWindow()}로 받는다).
 * </ul>
 */
public interface PlacementPolicy {

    /** 실험 결과의 variant 값으로 쓰는 이름. 예: {@code greedy-load-placement}. */
    String variant();

    PlacementDecision decide(PlacementInput input);
}
