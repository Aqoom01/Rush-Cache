package io.openkache.core.tablet.routing;

import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 클라이언트 쪽 라우팅: 표 사본으로 보낼 노드를 고르고, 응답을 보고 표를 고친다.
 *
 * <p>규칙(design.md §4.3):
 * <ul>
 *   <li>{@code OK}·{@code MISS} 등: 완료. 응답의 표 버전이 내 것보다 크면 "다음 호출 전에 갱신"으로 표시한다(즉시 받지 않는다).
 *   <li>{@code MOVED}: 표를 받아 온다. 새 표가 왔으면 그 표로, 아니면 응답의 redirect로 바로 재시도한다. 재시도는 최대
 *       {@value #MAX_REDIRECTS}회이고 넘으면 {@link ResultStatus#UNAVAILABLE}.
 *   <li>{@code STALE_EPOCH}: 표를 받아 와서 바뀌었으면 한 번 더, 아니면 그대로 돌려준다.
 *   <li>{@code BUSY}·{@code TIMEOUT} 등: 라우팅 문제가 아니므로 그대로 돌려준다.
 * </ul>
 *
 * <p>표를 받아 오는 방법({@code fetcher})은 호출자가 준다. in-process 실험에서는 Controller의 {@code table()}를, 실제 클라이언트에서는
 * {@link TableMessage.GetTable} 왕복을 넣는다. 네트워크 전송 자체는 {@code sender}에 맡기므로 이 클래스는 thread 하나에서 쓰는 것을
 * 전제로 한다(experiment-runner의 worker 하나가 클라이언트 하나).
 */
public final class RoutingClient {

    /** MOVED를 따라가는 최대 횟수. Redis 클라이언트들의 관례(redis-py 기본 3)와 같다. */
    public static final int MAX_REDIRECTS = 3;

    /**
     * 요청 하나를 어디로 어떤 정보를 붙여 보낼지.
     *
     * @param node 보낼 노드
     * @param tablet key의 Tablet(내 표 기준)
     * @param version 요청 헤더에 넣을 내 표 버전
     * @param epoch 요청에 넣을 Tablet epoch. 모르면 {@link OwnershipEpoch#ZERO}
     */
    public record Route(NodeId node, TabletId tablet, RoutingVersion version, OwnershipEpoch epoch) {}

    private final Supplier<Optional<RoutingTable>> fetcher;
    private RoutingTable table;
    private boolean refreshNeeded;

    /**
     * @param initial 처음 표. 실제 클라이언트는 시작할 때 Controller에서 받는다
     * @param fetcher 최신 표를 받아 오는 방법. 받지 못하면 empty
     */
    public RoutingClient(RoutingTable initial, Supplier<Optional<RoutingTable>> fetcher) {
        this.table = Objects.requireNonNull(initial, "initial");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
    }

    public RoutingTable table() {
        return table;
    }

    public RoutingVersion version() {
        return table.version();
    }

    /** 다음 호출 전에 표를 갱신해야 하는지(피기백으로 더 새 버전을 봤다). */
    public boolean refreshNeeded() {
        return refreshNeeded;
    }

    /** {@code keyHash}를 맡는 노드와 요청에 붙일 버전·epoch. */
    public Route route(long keyHash) {
        Tablet t = table.lookup(keyHash);
        return new Route(t.owner(), t.id(), table.version(), t.epoch());
    }

    /** {@code candidate}가 더 새 버전이면 교체하고 {@code true}. */
    public boolean accept(RoutingTable candidate) {
        if (!candidate.version().isNewerThan(table.version())) {
            return false;
        }
        table = candidate;
        refreshNeeded = false;
        return true;
    }

    /** 응답을 보고 내 표가 낡았는지 기록한다. {@link #call}이 부르므로 직접 보낼 때만 쓴다. */
    public void observe(Result result) {
        if (result.routingVersion().isNewerThan(table.version())) {
            refreshNeeded = true;
        }
    }

    /** 표를 받아 와서 더 새 것이면 교체한다. 받아 온 표가 새것이었으면 {@code true}. */
    public boolean refresh() {
        boolean changed = fetcher.get().map(this::accept).orElse(false);
        refreshNeeded = false;
        return changed;
    }

    /**
     * 요청 하나를 끝까지 보낸다. {@code sender}는 주어진 {@link Route}로 실제 요청을 보내고 응답을 돌려준다.
     *
     * @return 마지막 응답. redirect를 {@value #MAX_REDIRECTS}회 넘게 따라갔으면 {@link ResultStatus#UNAVAILABLE}
     */
    public Result call(long keyHash, Function<Route, Result> sender) {
        if (refreshNeeded) {
            refresh();
        }
        Route route = route(keyHash);
        for (int redirects = 0; ; redirects++) {
            Result result = sender.apply(route);
            observe(result);
            switch (result.status()) {
                case MOVED -> {
                    if (redirects >= MAX_REDIRECTS) {
                        return Result.of(ResultStatus.UNAVAILABLE, table.version(), OwnershipEpoch.ZERO);
                    }
                    if (refresh()) {
                        route = route(keyHash);
                    } else if (result.redirect().isPresent()) {
                        // 새 표는 아직 없지만 노드가 새 주인을 안다: 표를 기다리지 않고 힌트로 바로 간다. epoch는 모르므로 0
                        route = new Route(result.redirect().get().target(), route.tablet(), table.version(), OwnershipEpoch.ZERO);
                    } else {
                        return result;
                    }
                }
                case STALE_EPOCH -> {
                    if (redirects >= MAX_REDIRECTS || !refresh()) {
                        return result;
                    }
                    route = route(keyHash);
                }
                default -> {
                    return result;
                }
            }
        }
    }
}
