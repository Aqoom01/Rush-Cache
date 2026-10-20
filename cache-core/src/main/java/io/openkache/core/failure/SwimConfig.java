package io.openkache.core.failure;

import java.time.Duration;
import java.util.Objects;

/**
 * SWIM 장애 감지 알고리즘이 사용할 설정
 *
 * @param probeInterval 새 probe를 시작하는 간격
 * @param directProbeTimeout direct PING의 ACK 대기 시간
 * @param indirectProbeTimeout PING_REQ 이후 ACK 대기 시간
 * @param suspicionTimeout SUSPECT를 DEAD로 확정하기 전 대기 시간
 * @param indirectChecks indirect probe helper 최대 수
 * @param gossipRetransmitMultiplier membership 변경 재전파 횟수를 계산하는 배수
 * @param maxPiggybackUpdates 한 메시지에 넣을 membership 변경 최대 수
 */
public record SwimConfig(
        Duration probeInterval,
        Duration directProbeTimeout,
        Duration indirectProbeTimeout,
        Duration suspicionTimeout,
        int indirectChecks,
        int gossipRetransmitMultiplier,
        int maxPiggybackUpdates) {

    public SwimConfig {
        requirePositive("probeInterval", probeInterval);
        requirePositive("directProbeTimeout", directProbeTimeout);
        requirePositive("indirectProbeTimeout", indirectProbeTimeout);
        requirePositive("suspicionTimeout", suspicionTimeout);
        if (probeInterval.compareTo(directProbeTimeout.plus(indirectProbeTimeout)) < 0) {
            throw new IllegalArgumentException(
                    "probe 주기는 direct 와 indirect probe를 진행하는 시간합보다 작으면 안된다.");
        }
        if (indirectChecks < 1) {
            throw new IllegalArgumentException("간접 타임 아웃 >= 1 이어야 한다. " + indirectChecks);
        }
        if (gossipRetransmitMultiplier < 1) {
            throw new IllegalArgumentException(
                    "재전파 횟수 >= 1 이어야 한다. " + gossipRetransmitMultiplier);
        }
        if (maxPiggybackUpdates < 1) {
            throw new IllegalArgumentException("최대 gossip 메세지 횟수 >= 1 이어야 한다. " + maxPiggybackUpdates);
        }
    }

    private static void requirePositive(String name, Duration value) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "은(는) 무조건 양수: " + value);
        }
        value.toNanos();
    }
}
