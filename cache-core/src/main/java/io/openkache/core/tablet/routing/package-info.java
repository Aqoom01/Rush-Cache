/**
 * 라우팅 표 전파와 소유권 판정. variant와 무관하게 하나로 고정한다(design.md §4.3: Controller push + 응답 피기백 + {@code MOVED}).
 *
 * <p>세 참여자가 각자 표의 사본을 들고 있고 원본은 {@link io.openkache.core.tablet.routing.Controller} 하나다.
 * 노드는 {@link io.openkache.core.tablet.routing.LocalRouting}으로 사본과 거절 플래그를 들고
 * {@link io.openkache.core.tablet.routing.OwnershipCheck}로 요청을 판정하며, 클라이언트는
 * {@link io.openkache.core.tablet.routing.RoutingClient}로 보낼 노드를 고르고 {@code MOVED}를 따라간다. 사본이 낡아도 동작은 멈추지
 * 않고, 낡은 사본으로 온 요청은 {@code MOVED}로 바로잡는다. 메시지는 {@link io.openkache.core.tablet.routing.TableMessage}.
 *
 * <p>상태: in-process 구현. wire 인코딩(MOVED redirect 필드, 표 메시지 opcode)과 Controller 버전 영속화는 미구현.
 */
package io.openkache.core.tablet.routing;
