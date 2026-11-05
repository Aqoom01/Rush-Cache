package io.openkache.core.migration.strategy;

/** PD식 작업 단위 격리의 복구 결정. 이 자체가 데이터 영속성·장애 감지 구현은 아니다. */
public final class RecoveryPolicy {
    public enum Action { RETRY_FROM_SOURCE, RESUME_PUBLISHED_OWNER, HOLD }
    public Action decide(boolean published,boolean sourceAvailable,boolean destinationAvailable){
        if(!sourceAvailable||!destinationAvailable)return Action.HOLD;
        return published?Action.RESUME_PUBLISHED_OWNER:Action.RETRY_FROM_SOURCE;
    }
}
